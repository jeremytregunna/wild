const std = @import("std");
const wild = @import("wild.zig");
const wal = @import("wal.zig");
const flat_hash_storage = @import("flat_hash_storage.zig");
const static_allocator = @import("static_allocator.zig");

// View-based replication system for WILD database
// Maintains primary's exceptional performance while enabling horizontal read scaling

// Replication protocol message types
pub const ReplicationMessage = extern struct {
    message_type: MessageType,
    view_number: u64,
    batch_id: u64,
    entry_count: u16,
    reserved: u16,
    data_length: u32,
    // Variable-length data follows

    pub const MessageType = enum(u32) {
        wal_batch = 1,
        heartbeat = 2,
        bootstrap_request = 3,
        snapshot_data = 4,
        bootstrap_complete = 5,
    };

    pub fn init(msg_type: MessageType, view_number: u64, batch_id: u64, entry_count: u16, data_length: u32) ReplicationMessage {
        return ReplicationMessage{
            .message_type = msg_type,
            .view_number = view_number,
            .batch_id = batch_id,
            .entry_count = entry_count,
            .reserved = 0,
            .data_length = data_length,
        };
    }
};

// Replica connection state
pub const ReplicaConnection = struct {
    socket_fd: std.posix.fd_t,
    last_batch_id: std.atomic.Value(u64),
    last_heartbeat: std.atomic.Value(i64),
    is_connected: std.atomic.Value(bool),
    send_buffer: []u8,
    allocator: std.mem.Allocator,
    
    // Bootstrap state
    is_bootstrapping: std.atomic.Value(bool),
    bootstrap_last_batch_requested: std.atomic.Value(u64),

    const Self = @This();
    const SEND_BUFFER_SIZE = 1024 * 1024; // 1MB send buffer

    // Note: Initialization is handled by PrimaryReplicator pre-allocation
    // Note: deinit is no longer needed since buffers are pre-allocated
    // Connection cleanup is handled by the PrimaryReplicator

    pub fn sendWALBatch(self: *Self, view_number: u64, batch: *const wal.WALBatch) bool {
        if (!self.is_connected.load(.acquire)) return false;

        const message = ReplicationMessage.init(
            .wal_batch,
            view_number,
            batch.batch_id,
            batch.entry_count,
            @sizeOf(wal.WALBatch),
        );

        // Serialize message + batch to send buffer
        if (@sizeOf(ReplicationMessage) + @sizeOf(wal.WALBatch) > self.send_buffer.len) {
            return false; // Buffer too small
        }

        @memcpy(self.send_buffer[0..@sizeOf(ReplicationMessage)], std.mem.asBytes(&message));
        @memcpy(
            self.send_buffer[@sizeOf(ReplicationMessage)..@sizeOf(ReplicationMessage) + @sizeOf(wal.WALBatch)],
            std.mem.asBytes(batch),
        );

        const total_size = @sizeOf(ReplicationMessage) + @sizeOf(wal.WALBatch);
        const bytes_sent = std.posix.send(self.socket_fd, self.send_buffer[0..total_size], 0) catch |err| {
            switch (err) {
                error.BrokenPipe, error.ConnectionResetByPeer, error.NetworkUnreachable => {
                    // Expected network errors - mark as disconnected without logging
                    self.is_connected.store(false, .release);
                },
                else => {
                    std.debug.print("Failed to send WAL batch to replica: {}\n", .{err});
                    self.is_connected.store(false, .release);
                },
            }
            return false;
        };

        if (bytes_sent != total_size) {
            std.debug.print("Incomplete send to replica: {}/{} bytes\n", .{ bytes_sent, total_size });
            self.is_connected.store(false, .release);
            return false;
        }

        self.last_batch_id.store(batch.batch_id, .release);
        return true;
    }

    pub fn sendHeartbeat(self: *Self, view_number: u64) bool {
        if (!self.is_connected.load(.acquire)) return false;

        const message = ReplicationMessage.init(.heartbeat, view_number, 0, 0, 0);
        const bytes_sent = std.posix.send(self.socket_fd, std.mem.asBytes(&message), 0) catch |err| {
            switch (err) {
                error.BrokenPipe, error.ConnectionResetByPeer, error.NetworkUnreachable => {
                    // Expected network errors - mark as disconnected silently
                    self.is_connected.store(false, .release);
                },
                else => {
                    std.debug.print("Failed to send heartbeat to replica: {}\n", .{err});
                    self.is_connected.store(false, .release);
                },
            }
            return false;
        };

        if (bytes_sent != @sizeOf(ReplicationMessage)) {
            self.is_connected.store(false, .release);
            return false;
        }

        self.last_heartbeat.store(std.time.timestamp(), .release);
        return true;
    }
};

// Primary node replication coordinator
pub const PrimaryReplicator = struct {
    view_number: std.atomic.Value(u64),
    replica_connections: []ReplicaConnection,
    replica_send_buffers: [][]u8,
    active_replicas: std.atomic.Value(u32),
    replication_thread: ?std.Thread,
    should_stop: std.atomic.Value(bool),
    wal_manager: *wal.DurabilityManager,
    listen_fd: std.posix.fd_t,
    allocator: std.mem.Allocator,
    
    // Reference to database storage for snapshot creation
    database_storage: *flat_hash_storage.FlatHashStorage,

    // Statistics
    batches_replicated: std.atomic.Value(u64),
    replicas_connected: std.atomic.Value(u32),
    replication_errors: std.atomic.Value(u64),
    
    // Lag detection
    last_lag_check: std.atomic.Value(i64),

    const Self = @This();
    const MAX_REPLICAS = 64;
    const HEARTBEAT_INTERVAL_MS = 1000; // 1 second
    const REPLICA_TIMEOUT_MS = 5000; // 5 seconds
    const LAG_CHECK_INTERVAL_MS = 10000; // 10 seconds
    const MAX_LAG_BATCHES = 100; // Maximum batches a replica can lag behind

    pub fn init(allocator: std.mem.Allocator, wal_manager: *wal.DurabilityManager, database_storage: *flat_hash_storage.FlatHashStorage, listen_port: u16) !Self {
        // Create listening socket for replica connections
        const listen_fd = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
        try std.posix.setsockopt(listen_fd, std.posix.SOL.SOCKET, std.posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));

        const addr = try std.net.Address.parseIp("0.0.0.0", listen_port);
        try std.posix.bind(listen_fd, &addr.any, addr.getOsSockLen());
        try std.posix.listen(listen_fd, 16);

        // Pre-allocate all replica connection memory
        const replica_connections = try allocator.alloc(ReplicaConnection, MAX_REPLICAS);
        const replica_send_buffers = try allocator.alloc([]u8, MAX_REPLICAS);

        // Pre-allocate send buffers for all potential replicas
        for (replica_send_buffers) |*buffer| {
            buffer.* = try allocator.alloc(u8, ReplicaConnection.SEND_BUFFER_SIZE);
        }

        // Initialize all replica connections as unused
        for (replica_connections, 0..) |*conn, i| {
            conn.* = ReplicaConnection{
                .socket_fd = -1, // Invalid fd indicates unused slot
                .last_batch_id = std.atomic.Value(u64).init(0),
                .last_heartbeat = std.atomic.Value(i64).init(0),
                .is_connected = std.atomic.Value(bool).init(false),
                .send_buffer = replica_send_buffers[i],
                .allocator = allocator,
                .is_bootstrapping = std.atomic.Value(bool).init(false),
                .bootstrap_last_batch_requested = std.atomic.Value(u64).init(0),
            };
        }

        return Self{
            .view_number = std.atomic.Value(u64).init(1),
            .replica_connections = replica_connections,
            .replica_send_buffers = replica_send_buffers,
            .active_replicas = std.atomic.Value(u32).init(0),
            .replication_thread = null,
            .should_stop = std.atomic.Value(bool).init(false),
            .wal_manager = wal_manager,
            .listen_fd = listen_fd,
            .allocator = allocator,
            .database_storage = database_storage,
            .batches_replicated = std.atomic.Value(u64).init(0),
            .replicas_connected = std.atomic.Value(u32).init(0),
            .replication_errors = std.atomic.Value(u64).init(0),
            .last_lag_check = std.atomic.Value(i64).init(0),
        };
    }

    pub fn deinit(self: *Self) void {
        self.stop();

        // Clean up active replica connections
        for (self.replica_connections) |*replica| {
            if (replica.socket_fd != -1) {
                std.posix.close(replica.socket_fd);
            }
        }

        // Free pre-allocated send buffers
        for (self.replica_send_buffers) |buffer| {
            self.allocator.free(buffer);
        }
        self.allocator.free(self.replica_send_buffers);
        self.allocator.free(self.replica_connections);

        std.posix.close(self.listen_fd);
    }

    pub fn start(self: *Self) !void {
        std.debug.assert(self.replication_thread == null);
        self.replication_thread = try std.Thread.spawn(.{}, replicationLoop, .{self});
        // Primary replication started
    }

    pub fn stop(self: *Self) void {
        if (self.replication_thread) |thread| {
            self.should_stop.store(true, .release);
            thread.join();
            self.replication_thread = null;
        }
    }

    // Called by WAL durability manager when a batch is written
    pub fn onWALBatchWritten(self: *Self, batch: *const wal.WALBatch) void {
        // This will be processed by the replication thread
        // We don't block the WAL thread here to maintain primary performance
        _ = self;
        _ = batch;
    }

    fn replicationLoop(self: *Self) void {
        var last_heartbeat = std.time.milliTimestamp();
        var last_batch_check = self.wal_manager.batches_written.load(.acquire);

        while (!self.should_stop.load(.acquire)) {
            const current_time = std.time.milliTimestamp();

            // Phase 1: Accept new replica connections (non-blocking)
            self.acceptNewReplicas();

            // Phase 2: Clean up disconnected replicas
            self.cleanupDisconnectedReplicas();

            // Phase 3: Handle bootstrap requests from replicas
            self.processBootstrapRequests();

            // Phase 4: Check for new WAL batches to replicate
            const current_batches = self.wal_manager.batches_written.load(.acquire);
            if (current_batches > last_batch_check) {
                // New batches available - we need to stream them
                // For now, we'll implement a simple approach where we stream completed batches
                // In a production system, we'd want to stream from the WAL file directly
                last_batch_check = current_batches;
                self.streamPendingBatches();
            }

            // Phase 5: Send heartbeats periodically
            if (current_time - last_heartbeat >= HEARTBEAT_INTERVAL_MS) {
                self.sendHeartbeats();
                last_heartbeat = current_time;
            }

            // Phase 6: Check for replica lag and initiate catch-up if needed
            if (current_time - self.last_lag_check.load(.acquire) >= LAG_CHECK_INTERVAL_MS) {
                self.checkReplicaLag(current_batches);
                self.last_lag_check.store(current_time, .release);
            }

            // Phase 7: Brief sleep to avoid busy waiting
            std.time.sleep(1_000_000); // 1ms
        }

        // Replication loop stopped
    }

    fn acceptNewReplicas(self: *Self) void {
        // Non-blocking accept
        var addr: std.posix.sockaddr = undefined;
        var addr_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr);

        const replica_fd = std.posix.accept(self.listen_fd, &addr, &addr_len, std.posix.SOCK.NONBLOCK) catch |err| {
            switch (err) {
                error.WouldBlock => return, // No pending connections
                else => {
                    _ = self.replication_errors.fetchAdd(1, .monotonic);
                    return;
                },
            }
        };

        // Find an available replica slot
        var slot_found = false;
        for (self.replica_connections) |*replica| {
            if (replica.socket_fd == -1) { // Unused slot
                replica.socket_fd = replica_fd;
                replica.last_batch_id.store(0, .release);
                replica.last_heartbeat.store(std.time.timestamp(), .release);
                replica.is_connected.store(true, .release);
                replica.is_bootstrapping.store(false, .release);
                replica.bootstrap_last_batch_requested.store(0, .release);
                
                _ = self.active_replicas.fetchAdd(1, .monotonic);
                _ = self.replicas_connected.fetchAdd(1, .monotonic);
                
                slot_found = true;
                break;
            }
        }

        if (!slot_found) {
            // No available slots, reject connection
            std.posix.close(replica_fd);
            _ = self.replication_errors.fetchAdd(1, .monotonic);
        }
    }

    fn cleanupDisconnectedReplicas(self: *Self) void {
        const current_time = std.time.timestamp();

        for (self.replica_connections) |*replica| {
            if (replica.socket_fd != -1 and
                (!replica.is_connected.load(.acquire) or
                (current_time - replica.last_heartbeat.load(.acquire)) > (REPLICA_TIMEOUT_MS / 1000)))
            {
                // Clean up disconnected replica
                std.posix.close(replica.socket_fd);
                replica.socket_fd = -1; // Mark as unused
                replica.is_connected.store(false, .release);
                
                _ = self.active_replicas.fetchSub(1, .monotonic);
                _ = self.replicas_connected.fetchSub(1, .monotonic);
            }
        }
    }

    // Stream a WAL batch to all connected replicas
    pub fn streamBatchToReplicas(self: *Self, batch: *const wal.WALBatch) void {
        const current_view = self.view_number.load(.acquire);
        var batches_sent: u32 = 0;

        for (self.replica_connections) |*replica| {
            if (replica.socket_fd != -1 and replica.is_connected.load(.acquire)) {
                if (replica.sendWALBatch(current_view, batch)) {
                    batches_sent += 1;
                } else {
                    _ = self.replication_errors.fetchAdd(1, .monotonic);
                }
            }
        }

        if (batches_sent > 0) {
            _ = self.batches_replicated.fetchAdd(1, .monotonic);
        }
    }

    fn processBootstrapRequests(self: *Self) void {
        // Process bootstrap requests from newly connected replicas
        for (self.replica_connections) |*replica| {
            if (replica.socket_fd != -1 and replica.is_connected.load(.acquire)) {
                // Check if we need to process bootstrap requests from this replica
                self.checkForBootstrapRequest(replica);
                
                // If replica is bootstrapping, continue sending snapshot data
                if (replica.is_bootstrapping.load(.acquire)) {
                    self.continueBootstrapProcess(replica);
                }
            }
        }
    }
    
    fn checkForBootstrapRequest(self: *Self, replica: *ReplicaConnection) void {
        // Check for incoming bootstrap request messages
        var temp_buffer: [64]u8 = undefined;
        const bytes_available = std.posix.recv(replica.socket_fd, &temp_buffer, std.posix.MSG.DONTWAIT | std.posix.MSG.PEEK) catch |err| {
            switch (err) {
                error.WouldBlock => return, // No data available
                error.ConnectionResetByPeer => {
                    // Connection lost - mark as disconnected
                    replica.is_connected.store(false, .release);
                    return;
                },
                else => {
                    std.debug.print("Error checking for bootstrap request: {}\n", .{err});
                    replica.is_connected.store(false, .release);
                    return;
                }
            }
        };
        
        if (bytes_available >= @sizeOf(ReplicationMessage)) {
            const message = std.mem.bytesToValue(ReplicationMessage, temp_buffer[0..@sizeOf(ReplicationMessage)]);
            
            if (message.message_type == .bootstrap_request) {
                // Actually consume the message
                _ = std.posix.recv(replica.socket_fd, temp_buffer[0..@sizeOf(ReplicationMessage)], 0) catch return;
                
                // Start bootstrap process
                replica.is_bootstrapping.store(true, .release);
                replica.bootstrap_last_batch_requested.store(message.batch_id, .release);
                
                // Begin sending snapshot data
                self.startBootstrapSnapshot(replica);
            }
        }
    }
    
    fn startBootstrapSnapshot(self: *Self, replica: *ReplicaConnection) void {
        // Create and send snapshot of current database state
        const snapshot_data = self.createDatabaseSnapshot() catch {
            replica.is_bootstrapping.store(false, .release);
            return;
        };
        defer self.allocator.free(snapshot_data);
        
        // Send snapshot in chunks if needed
        const chunk_size = replica.send_buffer.len - @sizeOf(ReplicationMessage);
        var offset: usize = 0;
        var sequence_id: u64 = 0;
        
        while (offset < snapshot_data.len) {
            const chunk_end = @min(offset + chunk_size, snapshot_data.len);
            const chunk = snapshot_data[offset..chunk_end];
            
            const message = ReplicationMessage.init(
                .snapshot_data,
                self.view_number.load(.acquire),
                sequence_id,
                0, // entry_count not used for snapshot
                @intCast(chunk.len),
            );
            
            // Send message + chunk
            @memcpy(replica.send_buffer[0..@sizeOf(ReplicationMessage)], std.mem.asBytes(&message));
            @memcpy(replica.send_buffer[@sizeOf(ReplicationMessage)..@sizeOf(ReplicationMessage) + chunk.len], chunk);
            
            const total_size = @sizeOf(ReplicationMessage) + chunk.len;
            const bytes_sent = std.posix.send(replica.socket_fd, replica.send_buffer[0..total_size], 0) catch {
                replica.is_bootstrapping.store(false, .release);
                replica.is_connected.store(false, .release);
                return;
            };
            
            if (bytes_sent != total_size) {
                replica.is_bootstrapping.store(false, .release);
                replica.is_connected.store(false, .release);
                return;
            }
            
            offset = chunk_end;
            sequence_id += 1;
        }
        
        // Send bootstrap complete message
        const complete_message = ReplicationMessage.init(
            .bootstrap_complete,
            self.view_number.load(.acquire),
            self.wal_manager.batches_written.load(.acquire), // Current batch ID
            0,
            0,
        );
        
        _ = std.posix.send(replica.socket_fd, std.mem.asBytes(&complete_message), 0) catch {
            replica.is_connected.store(false, .release);
            return;
        };
        
        replica.is_bootstrapping.store(false, .release);
        replica.last_batch_id.store(self.wal_manager.batches_written.load(.acquire), .release);
    }
    
    fn continueBootstrapProcess(self: *Self, replica: *ReplicaConnection) void {
        // For now, bootstrap is sent in one shot in startBootstrapSnapshot
        // This function could be used for more sophisticated streaming bootstrap
        _ = self;
        _ = replica;
    }
    
    fn createDatabaseSnapshot(self: *Self) ![]u8 {
        // Create a snapshot of the current database state
        // We'll serialize all valid records in the flat hash storage
        
        const storage_stats = self.database_storage.getStats();
        const max_records = storage_stats.total_capacity;
        
        // Calculate maximum possible snapshot size
        const max_snapshot_size = max_records * @sizeOf(flat_hash_storage.CacheLineRecord);
        const snapshot_data = try self.allocator.alloc(u8, max_snapshot_size);
        var snapshot_offset: usize = 0;
        
        // Iterate through all storage slots and copy valid records
        for (0..max_records) |slot_index| {
            if (self.database_storage.getRecordAtSlot(@intCast(slot_index))) |record| {
                if (record.isValid()) {
                    const record_bytes = std.mem.asBytes(record);
                    @memcpy(snapshot_data[snapshot_offset..snapshot_offset + record_bytes.len], record_bytes);
                    snapshot_offset += record_bytes.len;
                }
            }
        }
        
        // Resize to actual size used
        if (snapshot_offset == 0) {
            self.allocator.free(snapshot_data);
            return try self.allocator.alloc(u8, 0); // Empty snapshot
        }
        
        const final_snapshot = try self.allocator.alloc(u8, snapshot_offset);
        @memcpy(final_snapshot, snapshot_data[0..snapshot_offset]);
        self.allocator.free(snapshot_data);
        
        return final_snapshot;
    }

    fn checkReplicaLag(self: *Self, current_batch_id: u64) void {
        // Check each replica for excessive lag
        for (self.replica_connections) |*replica| {
            if (replica.socket_fd != -1 and replica.is_connected.load(.acquire) and !replica.is_bootstrapping.load(.acquire)) {
                const replica_last_batch = replica.last_batch_id.load(.acquire);
                const lag = current_batch_id - replica_last_batch;
                
                if (lag > MAX_LAG_BATCHES) {
                    // Replica is lagging too much, initiate catch-up
                    self.initiateCatchUp(replica, replica_last_batch, current_batch_id);
                }
            }
        }
    }
    
    fn initiateCatchUp(self: *Self, replica: *ReplicaConnection, replica_batch_id: u64, current_batch_id: u64) void {
        const lag = current_batch_id - replica_batch_id;
        std.debug.print("Replica lag detected: {} batches behind, initiating catch-up\n", .{lag});
        
        // For small lags, try streaming missing WAL batches first
        if (lag <= MAX_LAG_BATCHES / 4) { // Quarter of max threshold
            std.debug.print("Attempting WAL file streaming for catch-up\n", .{});
            self.streamMissingBatches(replica, replica_batch_id, current_batch_id);
        } else {
            // For large lags, use full snapshot bootstrap
            std.debug.print("Large lag detected, using snapshot bootstrap\n", .{});
            replica.is_bootstrapping.store(true, .release);
            replica.bootstrap_last_batch_requested.store(replica_batch_id, .release);
            self.startBootstrapSnapshot(replica);
        }
    }
    
    fn streamMissingBatches(self: *Self, replica: *ReplicaConnection, from_batch: u64, to_batch: u64) void {
        // Stream missing WAL batches from persistent files for efficient catch-up
        if (to_batch <= from_batch) return;
        
        // Read missing batches from WAL files and stream them
        for (from_batch + 1..to_batch + 1) |batch_id| {
            if (self.readWALBatchFromDisk(@intCast(batch_id))) |batch| {
                const current_view = self.view_number.load(.acquire);
                if (!replica.sendWALBatch(current_view, &batch)) {
                    // Failed to send, replica will be marked unhealthy
                    break;
                }
            } else {
                // Batch not found on disk, may have been garbage collected
                // Fall back to bootstrap for this replica
                replica.is_bootstrapping.store(true, .release);
                self.startBootstrapSnapshot(replica);
                break;
            }
        }
    }
    
    fn readWALBatchFromDisk(self: *Self, batch_id: u64) ?wal.WALBatch {
        // Read a specific WAL batch from persistent storage
        // This would integrate with the WAL file management system
        
        if (self.wal_manager.wal_files.items.len == 0) return null;
        
        // Search through WAL files for the requested batch
        for (self.wal_manager.wal_files.items) |*wal_file| {
            if (batch_id >= wal_file.start_batch_id and batch_id <= wal_file.end_batch_id) {
                // Found the right file, read the batch
                const file = std.fs.cwd().openFile(wal_file.file_path, .{}) catch return null;
                defer file.close();
                
                // Calculate offset for this batch in the file
                const batch_offset = (batch_id - wal_file.start_batch_id) * @sizeOf(wal.WALBatch);
                file.seekTo(batch_offset) catch return null;
                
                var batch: wal.WALBatch = undefined;
                const bytes_read = file.readAll(std.mem.asBytes(&batch)) catch return null;
                
                if (bytes_read == @sizeOf(wal.WALBatch) and batch.batch_id == batch_id) {
                    return batch;
                }
            }
        }
        
        return null;
    }

    fn streamPendingBatches(self: *Self) void {
        // This function is now mainly for periodic checks
        // The main streaming happens via the WAL callback
        _ = self;
    }

    fn sendHeartbeats(self: *Self) void {
        const current_view = self.view_number.load(.acquire);

        for (self.replica_connections) |*replica| {
            if (replica.socket_fd != -1 and replica.is_connected.load(.acquire)) {
                if (!replica.sendHeartbeat(current_view)) {
                    _ = self.replication_errors.fetchAdd(1, .monotonic);
                }
            }
        }
    }

    pub fn getStats(self: *const Self) ReplicationStats {
        // Calculate lag statistics
        const current_batch_id = self.wal_manager.batches_written.load(.acquire);
        var max_lag: u64 = 0;
        var lagging_count: u32 = 0;
        
        for (self.replica_connections) |*replica| {
            if (replica.socket_fd != -1 and replica.is_connected.load(.acquire)) {
                const replica_batch = replica.last_batch_id.load(.acquire);
                const lag = if (current_batch_id >= replica_batch) current_batch_id - replica_batch else 0;
                
                if (lag > max_lag) {
                    max_lag = lag;
                }
                
                if (lag > MAX_LAG_BATCHES / 2) { // Consider lagging if more than half the threshold
                    lagging_count += 1;
                }
            }
        }
        
        return ReplicationStats{
            .view_number = self.view_number.load(.acquire),
            .batches_replicated = self.batches_replicated.load(.acquire),
            .replicas_connected = self.active_replicas.load(.acquire),
            .replication_errors = self.replication_errors.load(.acquire),
            .max_replica_lag = max_lag,
            .lagging_replicas = lagging_count,
        };
    }
};

// Replica node that receives and applies WAL streams
pub const ReplicaNode = struct {
    database: *wild.WILD,
    static_alloc: *static_allocator.StaticAllocator,
    primary_connection: ?ReplicaConnection,
    replica_thread: ?std.Thread,
    should_stop: std.atomic.Value(bool),
    last_applied_batch: std.atomic.Value(u64),
    current_view: std.atomic.Value(u64),
    allocator: std.mem.Allocator,

    // Statistics
    batches_applied: std.atomic.Value(u64),
    connection_errors: std.atomic.Value(u64),

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, database: *wild.WILD, static_alloc: *static_allocator.StaticAllocator) !Self {
        return Self{
            .database = database,
            .static_alloc = static_alloc,
            .primary_connection = null,
            .replica_thread = null,
            .should_stop = std.atomic.Value(bool).init(false),
            .last_applied_batch = std.atomic.Value(u64).init(0),
            .current_view = std.atomic.Value(u64).init(0),
            .allocator = allocator,
            .batches_applied = std.atomic.Value(u64).init(0),
            .connection_errors = std.atomic.Value(u64).init(0),
        };
    }

    pub fn deinit(self: *Self) void {
        self.stop();
        if (self.primary_connection) |*conn| {
            if (conn.socket_fd != -1) {
                std.posix.close(conn.socket_fd);
            }
            self.allocator.free(conn.send_buffer);
        }
    }

    pub fn connectToPrimary(self: *Self, primary_address: []const u8, primary_port: u16) !void {
        const sock_fd = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
        const addr = try std.net.Address.parseIp(primary_address, primary_port);
        
        try std.posix.connect(sock_fd, &addr.any, addr.getOsSockLen());
        
        // Create replica connection manually since we're not using the pre-allocated pool
        const send_buffer = try self.allocator.alloc(u8, ReplicaConnection.SEND_BUFFER_SIZE);
        
        self.primary_connection = ReplicaConnection{
            .socket_fd = sock_fd,
            .last_batch_id = std.atomic.Value(u64).init(0),
            .last_heartbeat = std.atomic.Value(i64).init(std.time.timestamp()),
            .is_connected = std.atomic.Value(bool).init(true),
            .send_buffer = send_buffer,
            .allocator = self.allocator,
            .is_bootstrapping = std.atomic.Value(bool).init(false),
            .bootstrap_last_batch_requested = std.atomic.Value(u64).init(0),
        };
        
        // Connected to primary
    }

    pub fn start(self: *Self) !void {
        std.debug.assert(self.replica_thread == null);
        std.debug.assert(self.primary_connection != null);
        
        self.replica_thread = try std.Thread.spawn(.{}, replicaLoop, .{self});
        // Replica started
    }

    pub fn stop(self: *Self) void {
        if (self.replica_thread) |thread| {
            self.should_stop.store(true, .release);
            thread.join();
            self.replica_thread = null;
        }
    }

    fn replicaLoop(self: *Self) void {
        var receive_buffer: [8192]u8 = undefined; // Increased buffer size
        var buffer_used: usize = 0;

        // Send bootstrap request to get initial state
        self.requestBootstrap();

        while (!self.should_stop.load(.acquire)) {
            if (self.primary_connection) |*conn| {
                if (!conn.is_connected.load(.acquire)) {
                    break;
                }

                // Receive data into buffer after existing data
                const bytes_received = std.posix.recv(
                    conn.socket_fd, 
                    receive_buffer[buffer_used..], 
                    0
                ) catch {
                    _ = self.connection_errors.fetchAdd(1, .monotonic);
                    conn.is_connected.store(false, .release);
                    break;
                };

                if (bytes_received == 0) {
                    // Primary disconnected
                    conn.is_connected.store(false, .release);
                    break;
                }

                buffer_used += bytes_received;

                // Process complete messages
                var processed_bytes: usize = 0;
                while (processed_bytes < buffer_used) {
                    const remaining = buffer_used - processed_bytes;
                    
                    // Need at least a message header
                    if (remaining < @sizeOf(ReplicationMessage)) break;
                    
                    // Parse message header
                    const message = std.mem.bytesToValue(
                        ReplicationMessage, 
                        receive_buffer[processed_bytes..processed_bytes + @sizeOf(ReplicationMessage)]
                    );
                    
                    const total_message_size = @sizeOf(ReplicationMessage) + message.data_length;
                    
                    // Check if we have the complete message
                    if (remaining < total_message_size) break;
                    
                    // Process the complete message
                    self.processReceivedData(receive_buffer[processed_bytes..processed_bytes + total_message_size]);
                    processed_bytes += total_message_size;
                }

                // Move remaining partial data to start of buffer
                if (processed_bytes > 0) {
                    if (processed_bytes < buffer_used) {
                        std.mem.copyForwards(u8, receive_buffer[0..], receive_buffer[processed_bytes..buffer_used]);
                    }
                    buffer_used -= processed_bytes;
                }
            } else {
                break;
            }

            std.time.sleep(100_000); // 100μs
        }

        // Replica loop stopped
    }

    fn requestBootstrap(self: *Self) void {
        if (self.primary_connection) |*conn| {
            const last_batch = self.last_applied_batch.load(.acquire);
            const message = ReplicationMessage.init(
                .bootstrap_request,
                0, // view number not needed for bootstrap request
                last_batch,
                0,
                0,
            );

            _ = std.posix.send(conn.socket_fd, std.mem.asBytes(&message), 0) catch {
                conn.is_connected.store(false, .release);
            };
        }
    }

    fn processSnapshotData(self: *Self, snapshot_data: []const u8, sequence_id: u64) void {
        // Process snapshot data chunks
        // For now, implement a simple approach that applies records directly
        
        // Snapshot data format: sequence of CacheLineRecord structures
        var offset: usize = 0;
        while (offset + @sizeOf(flat_hash_storage.CacheLineRecord) <= snapshot_data.len) {
            const record_bytes = snapshot_data[offset..offset + @sizeOf(flat_hash_storage.CacheLineRecord)];
            const record = std.mem.bytesToValue(flat_hash_storage.CacheLineRecord, record_bytes);
            
            if (record.isValid()) {
                const key = record.getKey();
                const data = record.getData();
                
                // Apply the record to our replica database
                self.database.write(key, data) catch {
                    _ = self.connection_errors.fetchAdd(1, .monotonic);
                    continue;
                };
            }
            
            offset += @sizeOf(flat_hash_storage.CacheLineRecord);
        }
        
        // Update our sequence tracking
        _ = sequence_id; // For now, just acknowledge receipt
    }

    fn processReceivedData(self: *Self, data: []const u8) void {
        if (data.len < @sizeOf(ReplicationMessage)) return;

        const message = std.mem.bytesToValue(ReplicationMessage, data[0..@sizeOf(ReplicationMessage)]);
        
        // Production: no debug logging
        
        // Validate message type (1=wal_batch, 2=heartbeat, 3=bootstrap_request, 4=snapshot_data, 5=bootstrap_complete)
        const raw_type = @intFromEnum(message.message_type);
        if (raw_type < 1 or raw_type > 5) return;
        
        switch (message.message_type) {
            .wal_batch => {
                if (data.len < @sizeOf(ReplicationMessage) + message.data_length) return;
                
                const batch_data = data[@sizeOf(ReplicationMessage)..@sizeOf(ReplicationMessage) + message.data_length];
                
                if (batch_data.len >= @sizeOf(wal.WALBatch)) {
                    const batch = std.mem.bytesToValue(wal.WALBatch, batch_data[0..@sizeOf(wal.WALBatch)]);
                    self.applyWALBatch(&batch);
                }
            },
            .heartbeat => {
                self.current_view.store(message.view_number, .release);
            },
            .bootstrap_request => {
                // Request from replica for bootstrap (handled by primary)
                // This message shouldn't be received by replica
            },
            .snapshot_data => {
                // Receive snapshot data from primary during bootstrap
                if (data.len < @sizeOf(ReplicationMessage) + message.data_length) return;
                const snapshot_data = data[@sizeOf(ReplicationMessage)..@sizeOf(ReplicationMessage) + message.data_length];
                self.processSnapshotData(snapshot_data, message.batch_id);
            },
            .bootstrap_complete => {
                // Bootstrap completed, switch to normal WAL streaming
                std.debug.print("Bootstrap complete, switching to WAL streaming\n", .{});
                self.last_applied_batch.store(message.batch_id, .release);
            },
        }
    }

    fn applyWALBatch(self: *Self, batch: *const wal.WALBatch) void {
        // Apply each entry in the batch to our local database
        for (batch.entries[0..batch.entry_count]) |*entry| {
            if (entry.isValid()) {
                const key = entry.getKey();
                const data = entry.getData();
                
                // Apply the write operation to our replica
                self.database.write(key, data) catch {
                    // On write failure, increment error counter and continue
                    _ = self.connection_errors.fetchAdd(1, .monotonic);
                    continue;
                };
            }
        }

        self.last_applied_batch.store(batch.batch_id, .release);
        _ = self.batches_applied.fetchAdd(1, .monotonic);
    }

    pub fn getStats(self: *const Self) ReplicaStats {
        return ReplicaStats{
            .last_applied_batch = self.last_applied_batch.load(.acquire),
            .current_view = self.current_view.load(.acquire),
            .batches_applied = self.batches_applied.load(.acquire),
            .connection_errors = self.connection_errors.load(.acquire),
            .is_connected = if (self.primary_connection) |*conn| conn.is_connected.load(.acquire) else false,
        };
    }
};

// Statistics structures
pub const ReplicationStats = struct {
    view_number: u64,
    batches_replicated: u64,
    replicas_connected: u32,
    replication_errors: u64,
    max_replica_lag: u64,
    lagging_replicas: u32,
};

pub const ReplicaStats = struct {
    last_applied_batch: u64,
    current_view: u64,
    batches_applied: u64,
    connection_errors: u64,
    is_connected: bool,
};