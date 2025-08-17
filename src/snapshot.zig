const std = @import("std");
const flat_hash_storage = @import("flat_hash_storage.zig");
const wal = @import("wal.zig");

// Snapshot file format for WILD database recovery
// Design: Memory-mapped file with header + flat array of cache-line records
// Compatible with zero-copy loading and efficient iteration

// Snapshot file header - fixed size for memory mapping alignment
pub const SnapshotHeader = extern struct {
    // Magic bytes for format validation
    magic: [8]u8 = [8]u8{ 'W', 'I', 'L', 'D', 'S', 'N', 'A', 'P' },

    // Version for format evolution
    version: u32 = 1,

    // Snapshot metadata
    snapshot_id: u64,
    creation_timestamp: u64, // Unix timestamp in nanoseconds
    wal_position: u64, // WAL offset at time of snapshot

    // Database state at snapshot time
    total_records: u64,
    total_capacity: u64,
    load_factor_x1000: u32, // Load factor * 1000 for precision

    // File layout information
    records_offset: u64, // Offset to start of records array
    records_size_bytes: u64, // Total size of records section

    // Checksums for integrity
    header_checksum: u32, // CRC32 of header (excluding this field)
    records_checksum: u32, // CRC32 of all records

    // Reserved for future use (adjusted for padding)
    reserved: [40]u8 = std.mem.zeroes([40]u8),

    const Self = @This();

    comptime {
        // Ensure header is cache-line aligned
        std.debug.assert(@sizeOf(SnapshotHeader) == 128);
    }

    pub fn init(snapshot_id: u64, wal_position: u64, storage_stats: flat_hash_storage.StorageStats) Self {
        return Self{
            .snapshot_id = snapshot_id,
            .creation_timestamp = @as(u64, @intCast(std.time.nanoTimestamp())),
            .wal_position = wal_position,
            .total_records = storage_stats.total_count,
            .total_capacity = storage_stats.total_capacity,
            .load_factor_x1000 = @intFromFloat(storage_stats.load_factor * 1000.0),
            .records_offset = @sizeOf(SnapshotHeader),
            .records_size_bytes = storage_stats.total_capacity * @sizeOf(flat_hash_storage.CacheLineRecord),
            .header_checksum = 0, // Will be calculated after creation
            .records_checksum = 0, // Will be calculated after writing records
        };
    }

    pub fn calculateHeaderChecksum(self: *Self) void {
        // Temporarily zero the checksum field
        const old_checksum = self.header_checksum;
        self.header_checksum = 0;

        // Calculate CRC32 of the header
        const header_bytes = std.mem.asBytes(self);
        self.header_checksum = std.hash.Crc32.hash(header_bytes);

        // Restore if calculation failed (shouldn't happen)
        if (self.header_checksum == 0) {
            self.header_checksum = old_checksum;
        }
    }

    pub fn validateHeader(self: *const Self) bool {
        // Check magic bytes
        if (!std.mem.eql(u8, &self.magic, &[8]u8{ 'W', 'I', 'L', 'D', 'S', 'N', 'A', 'P' })) {
            return false;
        }

        // Check version
        if (self.version != 1) {
            return false;
        }

        // Validate checksum
        var header_copy = self.*;
        const stored_checksum = header_copy.header_checksum;
        header_copy.calculateHeaderChecksum();

        return header_copy.header_checksum == stored_checksum;
    }
};

// Memory-mapped snapshot file for fast recovery
pub const SnapshotFile = struct {
    file: std.fs.File,
    mapped_memory: []align(std.heap.page_size_min) u8,
    header: *SnapshotHeader,
    records: []flat_hash_storage.CacheLineRecord,

    const Self = @This();

    // Create a new snapshot from current database state
    pub fn create(file_path: []const u8, storage: *const flat_hash_storage.FlatHashStorage, current_wal_position: u64) !Self {
        const storage_stats = storage.getStats();

        // Calculate total file size (header + records, page-aligned)
        const records_size = storage_stats.total_capacity * @sizeOf(flat_hash_storage.CacheLineRecord);
        const total_size = @sizeOf(SnapshotHeader) + records_size;
        const aligned_size = std.mem.alignForward(usize, total_size, std.heap.page_size_min);

        // Create snapshot file
        const file = try std.fs.cwd().createFile(file_path, .{ .read = true, .truncate = true });
        try file.setEndPos(aligned_size);

        // Memory map the file
        const mapped_memory = try std.posix.mmap(
            null,
            aligned_size,
            std.posix.PROT.READ | std.posix.PROT.WRITE,
            .{ .TYPE = .SHARED },
            file.handle,
            0,
        );

        // Initialize header
        const header = @as(*SnapshotHeader, @ptrCast(@alignCast(mapped_memory.ptr)));
        header.* = SnapshotHeader.init(@as(u64, @intCast(std.time.milliTimestamp())), // Use timestamp as snapshot ID
            current_wal_position, storage_stats);

        // Get records array view
        const records_ptr = @as([*]flat_hash_storage.CacheLineRecord, @ptrCast(@alignCast(mapped_memory.ptr + @sizeOf(SnapshotHeader))));
        const records = records_ptr[0..storage_stats.total_capacity];

        // Copy all records from storage (atomic snapshot)
        storage.copyAllRecords(records);

        // Calculate records checksum
        const records_bytes = std.mem.sliceAsBytes(records);
        header.records_checksum = std.hash.Crc32.hash(records_bytes);

        // Calculate header checksum (must be last)
        header.calculateHeaderChecksum();

        // Ensure data is written to disk
        try std.posix.msync(mapped_memory, std.posix.MSF.SYNC);

        std.debug.print("Created snapshot: {} records, {} bytes, WAL position {}\n", .{ storage_stats.total_count, aligned_size, current_wal_position });

        return Self{
            .file = file,
            .mapped_memory = mapped_memory,
            .header = header,
            .records = records,
        };
    }

    // Load existing snapshot from disk
    pub fn load(file_path: []const u8) !Self {
        const file = try std.fs.cwd().openFile(file_path, .{ .mode = .read_only });
        const file_size = try file.getEndPos();

        // Memory map the file
        const mapped_memory = try std.posix.mmap(
            null,
            file_size,
            std.posix.PROT.READ,
            .{ .TYPE = .SHARED },
            file.handle,
            0,
        );

        // Validate header
        const header = @as(*SnapshotHeader, @ptrCast(@alignCast(mapped_memory.ptr)));
        if (!header.validateHeader()) {
            std.posix.munmap(mapped_memory);
            file.close();
            return error.InvalidSnapshotHeader;
        }

        // Get records array view
        const records_ptr = @as([*]flat_hash_storage.CacheLineRecord, @ptrCast(@alignCast(mapped_memory.ptr + header.records_offset)));
        const records = records_ptr[0..header.total_capacity];

        // Validate records checksum
        const records_bytes = std.mem.sliceAsBytes(records);
        const calculated_checksum = std.hash.Crc32.hash(records_bytes);
        if (calculated_checksum != header.records_checksum) {
            std.posix.munmap(mapped_memory);
            file.close();
            return error.InvalidSnapshotChecksum;
        }

        std.debug.print("Loaded snapshot: {} records, {} bytes, WAL position {}\n", .{ header.total_records, file_size, header.wal_position });

        return Self{
            .file = file,
            .mapped_memory = mapped_memory,
            .header = header,
            .records = records,
        };
    }

    pub fn deinit(self: *Self) void {
        std.posix.munmap(self.mapped_memory);
        self.file.close();
    }

    // Get snapshot metadata
    pub fn getMetadata(self: *const Self) struct {
        snapshot_id: u64,
        creation_timestamp: u64,
        wal_position: u64,
        total_records: u64,
        total_capacity: u64,
        load_factor: f32,
    } {
        return .{
            .snapshot_id = self.header.snapshot_id,
            .creation_timestamp = self.header.creation_timestamp,
            .wal_position = self.header.wal_position,
            .total_records = self.header.total_records,
            .total_capacity = self.header.total_capacity,
            .load_factor = @as(f32, @floatFromInt(self.header.load_factor_x1000)) / 1000.0,
        };
    }

    // Restore database state from this snapshot
    pub fn restoreToStorage(self: *const Self, storage: *flat_hash_storage.FlatHashStorage) !void {
        // Verify capacities match
        const storage_stats = storage.getStats();
        if (storage_stats.total_capacity != self.header.total_capacity) {
            return error.CapacityMismatch;
        }

        // Clear current storage
        storage.clear();

        // Copy all valid records from snapshot
        for (self.records) |*record| {
            if (record.isValid()) {
                try storage.restoreRecord(record);
            }
        }

        std.debug.print("Restored {} records from snapshot\n", .{self.header.total_records});
    }
};

// Snapshot manager for coordinating snapshot creation and WAL integration
pub const SnapshotManager = struct {
    snapshot_dir: []const u8,
    max_snapshots: u32,
    snapshot_interval_seconds: u64,

    // Current snapshot state
    current_snapshot: ?SnapshotFile,
    last_snapshot_time: std.atomic.Value(u64),
    snapshot_counter: std.atomic.Value(u64),

    allocator: std.mem.Allocator,

    const Self = @This();

    pub const Config = struct {
        snapshot_dir: []const u8,
        max_snapshots: u32 = 10,
        snapshot_interval_seconds: u64 = 300, // 5 minutes
    };

    pub fn init(allocator: std.mem.Allocator, config: Config) !Self {
        // Ensure snapshot directory exists
        std.fs.cwd().makeDir(config.snapshot_dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        return Self{
            .snapshot_dir = try allocator.dupe(u8, config.snapshot_dir),
            .max_snapshots = config.max_snapshots,
            .snapshot_interval_seconds = config.snapshot_interval_seconds,
            .current_snapshot = null,
            .last_snapshot_time = std.atomic.Value(u64).init(0),
            .snapshot_counter = std.atomic.Value(u64).init(0),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.current_snapshot) |*snapshot| {
            snapshot.deinit();
        }
        self.allocator.free(self.snapshot_dir);
    }

    // Check if it's time to create a new snapshot
    pub fn shouldCreateSnapshot(self: *const Self) bool {
        const now = @as(u64, @intCast(std.time.timestamp()));
        const last_snapshot = self.last_snapshot_time.load(.monotonic);
        return (now - last_snapshot) >= self.snapshot_interval_seconds;
    }

    // Create a new snapshot of current database state
    pub fn createSnapshot(self: *Self, storage: *const flat_hash_storage.FlatHashStorage, current_wal_position: u64) !void {
        const snapshot_id = self.snapshot_counter.fetchAdd(1, .monotonic);

        // Generate snapshot filename
        var filename_buf: [256]u8 = undefined;
        const filename = try std.fmt.bufPrint(filename_buf[0..], "{s}/wild_snapshot_{:0>10}.snap", .{ self.snapshot_dir, snapshot_id });

        // Create new snapshot
        const new_snapshot = try SnapshotFile.create(filename, storage, current_wal_position);

        // Replace current snapshot
        if (self.current_snapshot) |*old_snapshot| {
            old_snapshot.deinit();
        }
        self.current_snapshot = new_snapshot;

        // Update last snapshot time
        self.last_snapshot_time.store(@as(u64, @intCast(std.time.timestamp())), .monotonic);

        // Clean up old snapshots beyond retention limit
        self.cleanupOldSnapshots() catch |err| {
            std.debug.print("Warning: Failed to clean up old snapshots: {}\n", .{err});
        };
    }

    // Find and load the latest valid snapshot
    pub fn loadLatestSnapshot(self: *Self) !?SnapshotFile {
        var dir = try std.fs.cwd().openDir(self.snapshot_dir, .{ .iterate = true });
        defer dir.close();

        var iterator = dir.iterate();
        var latest_snapshot_id: ?u64 = null; // Use optional to handle ID 0 correctly
        var latest_filename_buf: [256]u8 = undefined; // Stack buffer to avoid allocation
        var latest_filename_len: usize = 0;

        // Find the latest snapshot file
        while (try iterator.next()) |entry| {
            if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".snap")) {
                if (std.mem.startsWith(u8, entry.name, "wild_snapshot_")) {
                    // Extract snapshot ID from filename
                    const id_str = entry.name[14..24]; // "wild_snapshot_XXXXXXXXXX.snap"
                    if (std.fmt.parseInt(u64, id_str, 10)) |snapshot_id| {
                        if (latest_snapshot_id == null or snapshot_id > latest_snapshot_id.?) {
                            latest_snapshot_id = snapshot_id;
                            // Copy filename to stack buffer
                            @memcpy(latest_filename_buf[0..entry.name.len], entry.name);
                            latest_filename_len = entry.name.len;
                        }
                    } else |_| {
                        // Invalid filename format, skip
                        continue;
                    }
                }
            }
        }

        if (latest_snapshot_id != null) {
            const filename = latest_filename_buf[0..latest_filename_len];

            var full_path_buf: [512]u8 = undefined;
            const full_path = try std.fmt.bufPrint(full_path_buf[0..], "{s}/{s}", .{ self.snapshot_dir, filename });

            return SnapshotFile.load(full_path) catch |err| {
                std.debug.print("Failed to load snapshot {s}: {}\n", .{ filename, err });
                return null;
            };
        }

        return null;
    }

    // Clean up old snapshots, keeping only max_snapshots
    fn cleanupOldSnapshots(self: *Self) !void {
        var dir = try std.fs.cwd().openDir(self.snapshot_dir, .{ .iterate = true });
        defer dir.close();

        var snapshot_files = std.ArrayList(struct { name: []u8, id: u64 }).init(self.allocator);
        defer {
            for (snapshot_files.items) |item| {
                self.allocator.free(item.name);
            }
            snapshot_files.deinit();
        }

        // Collect all snapshot files
        var iterator = dir.iterate();
        while (try iterator.next()) |entry| {
            if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".snap")) {
                if (std.mem.startsWith(u8, entry.name, "wild_snapshot_")) {
                    const id_str = entry.name[14..24];
                    if (std.fmt.parseInt(u64, id_str, 10)) |snapshot_id| {
                        try snapshot_files.append(.{
                            .name = try self.allocator.dupe(u8, entry.name),
                            .id = snapshot_id,
                        });
                    } else |_| {
                        continue;
                    }
                }
            }
        }

        // Sort by ID (newest first)
        std.mem.sort(@TypeOf(snapshot_files.items[0]), snapshot_files.items, {}, struct {
            fn lessThan(_: void, a: @TypeOf(snapshot_files.items[0]), b: @TypeOf(snapshot_files.items[0])) bool {
                return a.id > b.id;
            }
        }.lessThan);

        // Delete old snapshots beyond max_snapshots
        if (snapshot_files.items.len > self.max_snapshots) {
            for (snapshot_files.items[self.max_snapshots..]) |item| {
                dir.deleteFile(item.name) catch |err| {
                    std.debug.print("Failed to delete old snapshot {s}: {}\n", .{ item.name, err });
                };
            }
        }
    }
};

// WAL replay functionality for point-in-time recovery
pub const WALReplayer = struct {
    wal_file_path: []const u8,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, wal_file_path: []const u8) Self {
        return Self{
            .wal_file_path = wal_file_path,
            .allocator = allocator,
        };
    }

    // Replay WAL entries from a specific position to recover database state
    pub fn replayFromPosition(self: *Self, storage: *flat_hash_storage.FlatHashStorage, start_position: u64, end_position: ?u64) !u64 {
        const file = std.fs.cwd().openFile(self.wal_file_path, .{ .mode = .read_only }) catch |err| switch (err) {
            error.FileNotFound => {
                std.debug.print("WAL file not found, no replay needed\n", .{});
                return start_position;
            },
            else => return err,
        };
        defer file.close();

        const file_size = try file.getEndPos();
        const actual_end_position = end_position orelse file_size;

        if (start_position >= file_size) {
            std.debug.print("WAL replay: start position {} beyond file size {}\n", .{ start_position, file_size });
            return start_position;
        }

        std.debug.print("Replaying WAL from position {} to {} (file size: {})\n", .{ start_position, actual_end_position, file_size });

        var current_position = start_position;
        var entries_replayed: u64 = 0;

        // Read and replay WAL batches
        while (current_position + @sizeOf(wal.WALBatch) <= actual_end_position) {
            // Read batch header first
            try file.seekTo(current_position);

            var batch: wal.WALBatch = undefined;
            const bytes_read = try file.readAll(std.mem.asBytes(&batch));
            if (bytes_read != @sizeOf(wal.WALBatch)) {
                std.debug.print("WAL replay: incomplete batch at position {}\n", .{current_position});
                break;
            }

            // Validate batch
            if (batch.entry_count == 0 or batch.entry_count > wal.WALBatch.MAX_BATCH_ENTRIES) {
                std.debug.print("WAL replay: invalid batch entry count {} at position {}\n", .{ batch.entry_count, current_position });
                break;
            }

            // Replay entries in this batch
            for (batch.entries[0..batch.entry_count]) |*entry| {
                if (entry.isValid()) {
                    // Convert WAL entry back to storage record format
                    const key = entry.getKey();
                    const data = entry.getData();

                    // Apply the write operation
                    storage.write(key, data) catch |err| {
                        std.debug.print("WAL replay: failed to replay entry key={}: {}\n", .{ key, err });
                        // Continue with other entries
                        continue;
                    };

                    entries_replayed += 1;
                }
            }

            current_position += @sizeOf(wal.WALBatch);
        }

        std.debug.print("WAL replay completed: {} entries replayed\n", .{entries_replayed});
        return current_position;
    }

    // Get the current end position of the WAL file
    pub fn getCurrentWALPosition(self: *Self) !u64 {
        const file = std.fs.cwd().openFile(self.wal_file_path, .{ .mode = .read_only }) catch |err| switch (err) {
            error.FileNotFound => return 0,
            else => return err,
        };
        defer file.close();

        return try file.getEndPos();
    }

    // Validate WAL integrity from a specific position
    pub fn validateWAL(self: *Self, start_position: u64) !bool {
        const file = std.fs.cwd().openFile(self.wal_file_path, .{ .mode = .read_only }) catch |err| switch (err) {
            error.FileNotFound => return true, // No WAL file means nothing to validate
            else => return err,
        };
        defer file.close();

        const file_size = try file.getEndPos();
        if (start_position >= file_size) {
            return true; // Nothing to validate
        }

        var current_position = start_position;
        var batches_validated: u32 = 0;

        while (current_position + @sizeOf(wal.WALBatch) <= file_size) {
            try file.seekTo(current_position);

            var batch: wal.WALBatch = undefined;
            const bytes_read = try file.readAll(std.mem.asBytes(&batch));
            if (bytes_read != @sizeOf(wal.WALBatch)) {
                std.debug.print("WAL validation: incomplete batch at position {}\n", .{current_position});
                return false;
            }

            // Validate batch structure
            if (batch.entry_count > wal.WALBatch.MAX_BATCH_ENTRIES) {
                std.debug.print("WAL validation: invalid entry count {} at position {}\n", .{ batch.entry_count, current_position });
                return false;
            }

            // Basic validation of entries
            for (batch.entries[0..batch.entry_count]) |*entry| {
                if (!entry.isValid()) {
                    std.debug.print("WAL validation: invalid entry at batch position {}\n", .{current_position});
                    return false;
                }
            }

            current_position += @sizeOf(wal.WALBatch);
            batches_validated += 1;
        }

        std.debug.print("WAL validation: {} batches validated successfully\n", .{batches_validated});
        return true;
    }
};

// Recovery coordinator that handles the complete crash recovery process
pub const RecoveryCoordinator = struct {
    snapshot_manager: *SnapshotManager,
    wal_replayer: WALReplayer,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, snapshot_manager: *SnapshotManager, wal_file_path: []const u8) Self {
        return Self{
            .snapshot_manager = snapshot_manager,
            .wal_replayer = WALReplayer.init(allocator, wal_file_path),
            .allocator = allocator,
        };
    }

    // Perform complete crash recovery: load latest snapshot + replay WAL
    pub fn performRecovery(self: *Self, storage: *flat_hash_storage.FlatHashStorage) !u64 {
        std.debug.print("=== Starting WILD Database Recovery ===\n", .{});

        // Step 1: Try to load the latest snapshot
        var recovery_start_position: u64 = 0;

        const maybe_snapshot = self.snapshot_manager.loadLatestSnapshot() catch |err| blk: {
            std.debug.print("Error loading snapshot: {}, starting from empty database\n", .{err});
            break :blk null; // Continue with null snapshot (empty database + full WAL replay)
        };

        if (maybe_snapshot) |snapshot| {
            defer {
                var mut_snapshot = snapshot;
                mut_snapshot.deinit();
            }

            const metadata = snapshot.getMetadata();
            std.debug.print("Found snapshot: ID={}, WAL position={}, {} records\n", .{ metadata.snapshot_id, metadata.wal_position, metadata.total_records });

            // Restore from snapshot
            try snapshot.restoreToStorage(storage);
            recovery_start_position = metadata.wal_position;

            std.debug.print("Restored database from snapshot\n", .{});
        } else {
            std.debug.print("No valid snapshot found, starting from empty database\n", .{});
            storage.clear();
            recovery_start_position = 0;
        }

        // Step 2: Validate WAL integrity
        if (!try self.wal_replayer.validateWAL(recovery_start_position)) {
            std.debug.print("WAL validation failed, recovery may be incomplete\n", .{});
            return error.WALValidationFailed;
        }

        // Step 3: Replay WAL from snapshot position to current
        const final_position = try self.wal_replayer.replayFromPosition(storage, recovery_start_position, null // Replay to end of WAL
        );

        const storage_stats = storage.getStats();
        std.debug.print("=== Recovery Complete ===\n", .{});
        std.debug.print("Final database state: {} records, {:.1}% load factor\n", .{ storage_stats.total_count, storage_stats.load_factor * 100 });
        std.debug.print("WAL position: {}\n", .{final_position});

        return final_position;
    }

    // Check if recovery is needed (detects unclean shutdown)
    pub fn isRecoveryNeeded(self: *Self) !bool {
        // Check if WAL file exists and has data
        const current_wal_position = self.wal_replayer.getCurrentWALPosition() catch 0;
        if (current_wal_position == 0) {
            return false; // No WAL data, clean state
        }

        // Check if there's a snapshot and compare positions
        const maybe_snapshot = self.snapshot_manager.loadLatestSnapshot() catch {
            // Error loading snapshot, assume recovery needed if WAL has data
            return current_wal_position > 0;
        };

        if (maybe_snapshot) |snapshot| {
            defer {
                var mut_snapshot = snapshot;
                mut_snapshot.deinit();
            }

            const metadata = snapshot.getMetadata();
            // Recovery needed if WAL has data beyond snapshot position
            return current_wal_position > metadata.wal_position;
        } else {
            // No snapshot but WAL has data - recovery needed
            return current_wal_position > 0;
        }
    }
};
