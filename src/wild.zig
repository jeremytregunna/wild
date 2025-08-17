const std = @import("std");
const cache_topology = @import("cache_topology.zig");
const flat_hash_storage = @import("flat_hash_storage.zig");
const static_allocator = @import("static_allocator.zig");
const wal = @import("wal.zig");
const snapshot = @import("snapshot.zig");
const replication = @import("replication.zig");

// WILD Database - Cache-Resident Ultra-High-Performance Key-Value Store
pub const WILD = struct {
    const Self = @This();

    // Core storage engine
    storage: flat_hash_storage.FlatHashStorage,
    cache_topology: *cache_topology.CacheTopology,
    allocator: std.mem.Allocator,

    // Optional durability - null means no WAL
    durability: ?wal.DurabilityManager,

    // Optional snapshots for recovery
    snapshot_manager: ?snapshot.SnapshotManager,

    // Optional replication for read replicas and multi-writer
    primary_replicator: ?replication.PrimaryReplicator,
    replica_node: ?replication.ReplicaNode,

    // Configuration
    target_capacity: u64,

    pub const Config = struct {
        target_capacity: u64,
    };

    pub const Stats = struct {
        load_factor: f32,
        used_capacity: u32,
        total_capacity: u32,
        physical_cores: u32,
        cache_line_size: u32,
        optimal_batch_size: u32,
    };

    // Initialize WILD database
    pub fn init(allocator: std.mem.Allocator, config: Config) !Self {
        // Database receives static allocator from caller
        // Caller is responsible for managing arena + static allocator lifecycle

        // Detect CPU topology
        const topology_ptr = try allocator.create(cache_topology.CacheTopology);
        topology_ptr.* = try cache_topology.analyzeCacheTopology(allocator);

        // Calculate total L3 cache size for validation
        var total_l3_cache_kb: u64 = 0;
        for (topology_ptr.l3_domains) |domain| {
            total_l3_cache_kb += domain.cache_size_kb;
        }

        // Initialize flat hash storage using static allocator
        const storage = try flat_hash_storage.FlatHashStorage.init(allocator, topology_ptr, config.target_capacity);

        // Note: Caller will transition static allocator to static state after init

        return Self{
            .storage = storage,
            .cache_topology = topology_ptr,
            .allocator = allocator,
            .durability = null, // Initially no WAL
            .snapshot_manager = null, // Initially no snapshots
            .primary_replicator = null, // Initially no replication
            .replica_node = null, // Initially not a replica
            .target_capacity = config.target_capacity,
        };
    }

    pub fn deinit(self: *Self) void {
        // CRITICAL SHUTDOWN ORDER - dependencies must be respected for performance and correctness
        
        // 1. Stop replication first - replicators may depend on WAL for final batches
        if (self.primary_replicator) |*replicator| {
            replicator.deinit(); // Stops accepting new connections, flushes pending batches
        }
        if (self.replica_node) |*replica| {
            replica.deinit(); // Disconnects from primary cleanly
        }

        // 2. Stop durability - WAL must be stopped after replication to avoid lost writes
        if (self.durability) |*dur| {
            dur.stop();   // Flush remaining batches to disk
            dur.deinit(); // Close file handles, cleanup threads
        }

        // 3. Clean up snapshot manager - may depend on storage state consistency
        if (self.snapshot_manager) |*snap_mgr| {
            snap_mgr.deinit();
        }

        // 4. Storage cleanup - core data structures, no dependencies
        self.storage.deinit();
        self.cache_topology.deinit();
        self.allocator.destroy(self.cache_topology);
        
        // Note: Caller handles static allocator and arena cleanup for performance
    }

    // Core single operations - no stats overhead
    pub inline fn read(self: *Self, key: u64) !?*const flat_hash_storage.CacheLineRecord {
        return self.storage.read(key);
    }

    pub inline fn write(self: *Self, key: u64, data: []const u8) !void {
        try self.storage.write(key, data);
    }

    pub inline fn delete(self: *Self, key: u64) !bool {
        return self.storage.delete(key);
    }

    pub inline fn clear(self: *Self) void {
        self.storage.clear();
    }

    // Batch operations for maximum performance - caller must provide result array
    pub fn readBatch(self: *Self, keys: []const u64, results: []?*const flat_hash_storage.CacheLineRecord) void {
        std.debug.assert(keys.len == results.len);

        // Simple loop - cache-line access is already optimized
        for (keys, 0..) |key, i| {
            results[i] = self.storage.read(key);
        }
    }

    // Simplified write batch - no complex batching needed with cache-line approach
    pub fn writeBatch(self: *Self, keys: []const u64, data_items: []const []const u8) !void {
        std.debug.assert(keys.len == data_items.len);

        // Simple loop over writes
        for (keys, data_items) |key, data| {
            try self.storage.write(key, data);
        }
    }

    pub fn deleteBatch(self: *Self, keys: []const u64) ![]bool {
        const results = try self.allocator.alloc(bool, keys.len);

        for (keys, 0..) |key, i| {
            results[i] = self.storage.delete(key);
        }

        return results;
    }

    // Convenience methods for string keys - no double hashing
    pub fn readString(self: *Self, key: []const u8) !?*const flat_hash_storage.CacheLineRecord {
        const hash = flat_hash_storage.hashKey(key);
        return self.read(hash);
    }

    pub fn writeString(self: *Self, key: []const u8, data: []const u8) !void {
        const hash = flat_hash_storage.hashKey(key);
        return self.write(hash, data);
    }

    pub fn deleteString(self: *Self, key: []const u8) !bool {
        const hash = flat_hash_storage.hashKey(key);
        return self.delete(hash);
    }

    // Get optimal batch size for this hardware - simplified for cache-line approach
    pub fn getOptimalBatchSize(self: *const Self) u32 {
        // With cache-line storage, batch size can be larger since we don't have bucket limitations
        const base_batch_size = 64;
        const cores_multiplier = @max(1, self.cache_topology.total_physical_cores / 2);
        return base_batch_size * cores_multiplier;
    }

    // Basic statistics - no timing overhead
    pub fn getStats(self: *const Self) Stats {
        const storage_stats = self.storage.getStats();

        return Stats{
            .load_factor = storage_stats.load_factor,
            .used_capacity = storage_stats.total_count,
            .total_capacity = storage_stats.total_capacity,
            .physical_cores = self.cache_topology.total_physical_cores,
            .cache_line_size = self.cache_topology.cache_line_size,
            .optimal_batch_size = self.getOptimalBatchSize(),
        };
    }

    // Print hardware and storage report - no performance timing
    pub fn printHardwareReport(self: *const Self) void {
        const stats = self.getStats();

        // Print storage details first
        self.storage.printDetailedStats();

        std.debug.print("\n=== WILD Hardware Report ===\n", .{});
        std.debug.print("Hardware Configuration:\n", .{});
        std.debug.print("- Physical cores: {}\n", .{stats.physical_cores});
        std.debug.print("- Cache line size: {} bytes\n", .{stats.cache_line_size});
        std.debug.print("- Optimal batch size: {}\n", .{stats.optimal_batch_size});

        std.debug.print("\nStorage Statistics:\n", .{});
        std.debug.print("- Used capacity: {}/{} ({d:.1}%)\n", .{ stats.used_capacity, stats.total_capacity, stats.load_factor * 100 });
    }

    // Hash key utility - unified interface
    pub fn hashKey(key: anytype) u64 {
        return flat_hash_storage.hashKey(key);
    }

    // Enable durability with WAL - must be called during init phase
    pub fn enableDurability(self: *Self, wal_config: wal.DurabilityManager.Config) !void {
        std.debug.assert(self.durability == null); // Should only be called once

        // Initialize durability manager using static allocator
        self.durability = try wal.DurabilityManager.init(self.allocator, wal_config);

        // Associate WAL with storage
        self.storage.enableDurability(&self.durability.?);

        // Start durability thread
        try self.durability.?.start();
    }

    // Convenience method: enable durability with properly sized WAL for this storage
    pub fn enableDurabilityForStorage(self: *Self, wal_path: []const u8) !void {
        const storage_stats = self.storage.getStats();
        const wal_config = wal.DurabilityManager.Config.forStorage(wal_path, storage_stats.total_capacity);
        try self.enableDurability(wal_config);
    }

    // Durability stats without ring buffer details
    pub const DurabilityStatsSimple = struct {
        batches_written: u64,
        entries_dropped: u64,
        total_entries_written: u64,
        wal_size_bytes: u64,
        pending_ios: u32,
        completed_ios: u64,
        io_errors: u64,
    };

    // Get durability statistics if enabled - no allocation version
    pub fn getDurabilityStatsNoAlloc(self: *const Self) ?DurabilityStatsSimple {
        if (self.durability) |*dur| {
            const stats = dur.getStatsNoAlloc();
            return DurabilityStatsSimple{
                .batches_written = stats.batches_written,
                .entries_dropped = stats.entries_dropped,
                .total_entries_written = stats.total_entries_written,
                .wal_size_bytes = stats.wal_size_bytes,
                .pending_ios = stats.pending_ios,
                .completed_ios = stats.completed_ios,
                .io_errors = stats.io_errors,
            };
        }
        return null;
    }

    // Get durability statistics if enabled - with allocation (init phase only)
    pub fn getDurabilityStats(self: *const Self) !?wal.DurabilityManager.Stats {
        if (self.durability) |*dur| {
            return try dur.getStats(self.allocator);
        }
        return null;
    }

    // Enable snapshots for crash recovery - must be called during init phase
    pub fn enableSnapshots(self: *Self, snapshot_config: snapshot.SnapshotManager.Config) !void {
        std.debug.assert(self.snapshot_manager == null); // Should only be called once

        // Initialize snapshot manager using static allocator
        self.snapshot_manager = try snapshot.SnapshotManager.init(self.allocator, snapshot_config);
    }

    // Create a snapshot of current database state
    pub fn createSnapshot(self: *Self) !void {
        if (self.snapshot_manager) |*snap_mgr| {
            const current_wal_position = if (self.durability) |*dur|
                dur.wal_offset.load(.monotonic)
            else
                0;

            try snap_mgr.createSnapshot(&self.storage, current_wal_position);
        } else {
            return error.SnapshotsNotEnabled;
        }
    }

    // Perform crash recovery from snapshots + WAL replay
    pub fn performRecovery(self: *Self, wal_file_path: []const u8) !void {
        if (self.snapshot_manager) |*snap_mgr| {
            var recovery_coordinator = snapshot.RecoveryCoordinator.init(self.allocator, snap_mgr, wal_file_path);

            const final_wal_position = try recovery_coordinator.performRecovery(&self.storage);

            // Update WAL offset if durability is enabled
            if (self.durability) |*dur| {
                dur.wal_offset.store(final_wal_position, .monotonic);
            }
        } else {
            return error.SnapshotsNotEnabled;
        }
    }

    // Check if automatic snapshot should be created
    pub fn shouldCreateSnapshot(self: *const Self) bool {
        if (self.snapshot_manager) |*snap_mgr| {
            return snap_mgr.shouldCreateSnapshot();
        }
        return false;
    }

    // Create snapshot if it's time for one (called periodically)
    pub fn maybeCreateSnapshot(self: *Self) !void {
        if (self.shouldCreateSnapshot()) {
            try self.createSnapshot();
        }
    }

    // Enable replication as primary node
    pub fn enableReplicationAsPrimary(self: *Self, listen_port: u16, auth_secret: []const u8) !void {
        std.debug.assert(self.primary_replicator == null);
        std.debug.assert(self.replica_node == null);
        
        // Must have durability enabled to replicate
        if (self.durability == null) {
            return error.DurabilityRequired;
        }

        // Initialize primary replicator
        self.primary_replicator = try replication.PrimaryReplicator.init(
            self.allocator,
            &self.durability.?,
            &self.storage,
            listen_port,
            auth_secret,
        );

        // Set up WAL replicator for streaming batches
        self.durability.?.setPrimaryReplicator(@ptrCast(&self.primary_replicator.?));

        // Start replication
        try self.primary_replicator.?.start();

        // Replication enabled as primary
    }

    // Enable replication as replica node
    pub fn enableReplicationAsReplica(self: *Self, primary_address: []const u8, primary_port: u16, auth_secret: []const u8) !void {
        std.debug.assert(self.primary_replicator == null);
        std.debug.assert(self.replica_node == null);

        // Initialize replica node
        self.replica_node = try replication.ReplicaNode.init(
            self.allocator,
            self,
            undefined, // static_alloc not needed for replica operations
        );

        // Connect to primary with authentication
        try self.replica_node.?.connectToPrimary(primary_address, primary_port, auth_secret);

        // Start replica
        try self.replica_node.?.start();

        // Replication enabled as replica
    }

    // Disable replication
    pub fn disableReplication(self: *Self) void {
        if (self.primary_replicator) |*replicator| {
            replicator.deinit();
            self.primary_replicator = null;
            
            // Remove WAL replicator
            if (self.durability) |*dur| {
                dur.removePrimaryReplicator();
            }
        }

        if (self.replica_node) |*replica| {
            replica.deinit();
            self.replica_node = null;
        }
    }

    // Get replication statistics
    pub fn getReplicationStats(self: *const Self) ?union(enum) {
        primary: replication.ReplicationStats,
        replica: replication.ReplicaStats,
    } {
        if (self.primary_replicator) |*replicator| {
            return .{ .primary = replicator.getStats() };
        } else if (self.replica_node) |*replica| {
            return .{ .replica = replica.getStats() };
        }
        return null;
    }

    // Check if this node is a primary
    pub fn isPrimary(self: *const Self) bool {
        return self.primary_replicator != null;
    }

    // Check if this node is a replica
    pub fn isReplica(self: *const Self) bool {
        return self.replica_node != null;
    }
};
