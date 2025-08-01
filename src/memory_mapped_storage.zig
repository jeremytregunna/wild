const std = @import("std");
const cache_topology = @import("cache_topology.zig");
const flat_hash_storage = @import("flat_hash_storage.zig");

pub const SyncPolicy = enum {
    none, // No persistence (in-memory only)
    write_through, // Write to both cache and disk immediately
    write_back, // Write to cache, sync to disk periodically
    async_sync, // Async writeback to disk
};

pub const MemoryMappedStorage = struct {
    const Self = @This();

    // Memory-mapped file backing the storage
    mapped_memory: []align(2 * 1024 * 1024) u8, // 2MB huge page alignment
    file: std.fs.File,
    file_size: u64,

    // Hash table using the memory-mapped region
    records: []flat_hash_storage.CacheLineRecord,
    capacity: u32,
    count: u32,
    topology: *const cache_topology.CacheTopology,

    // Durability configuration
    sync_policy: SyncPolicy,

    // NUMA placement hints
    numa_domains: u32,
    records_per_domain: u32,

    pub fn init(allocator: std.mem.Allocator, topology: *const cache_topology.CacheTopology, target_capacity: u64, file_path: ?[]const u8, sync_policy: SyncPolicy) !Self {
        // Enforce power-of-2 capacity for bitwise operations
        const raw_capacity = @as(u32, @intCast(target_capacity));
        const capacity = if (std.math.isPowerOfTwo(raw_capacity))
            raw_capacity
        else
            std.math.floorPowerOfTwo(u32, raw_capacity);

        const storage_size = capacity * @sizeOf(flat_hash_storage.CacheLineRecord);

        if (file_path) |path| {
            // Create or open the backing file
            const file = try std.fs.cwd().createFile(path, .{ .read = true, .truncate = false });

            // Ensure file is large enough
            const current_size = try file.getEndPos();
            if (current_size < storage_size) {
                try file.seekTo(storage_size - 1);
                _ = try file.write(&[_]u8{0});
                try file.seekTo(0);
            }

            // Ensure storage size is huge page aligned
            const huge_page_size = 2 * 1024 * 1024;
            const aligned_size = std.mem.alignForward(u64, storage_size, huge_page_size);

            // Try to memory-map with huge pages first, fall back to regular pages
            const mapped_memory = std.posix.mmap(null, aligned_size, std.posix.PROT.READ | std.posix.PROT.WRITE, .{ .TYPE = .SHARED, .HUGETLB = true }, // Request 2MB huge pages
                file.handle, 0) catch |err| switch (err) {
                error.Unexpected => blk: {
                    // Huge pages likely not available, fall back to regular pages
                    std.debug.print("Warning: File-backed huge pages failed, using regular pages\n", .{});
                    break :blk try std.posix.mmap(null, storage_size, // Use original size for regular pages
                        std.posix.PROT.READ | std.posix.PROT.WRITE, .{ .TYPE = .SHARED }, file.handle, 0);
                },
                else => return err,
            };

            // Note: We don't verify alignment here since we may have fallen back to regular pages

            // Cast mapped memory to records array
            const records: []flat_hash_storage.CacheLineRecord = @alignCast(@ptrCast(mapped_memory.ptr[0..capacity]));

            // Count existing valid records
            var count: u32 = 0;
            for (records) |*record| {
                if (record.isValid()) {
                    count += 1;
                }
            }

            // Calculate NUMA domain distribution
            const numa_domains = @as(u32, @intCast(topology.l3_domains.len));
            const records_per_domain = capacity / numa_domains;

            return Self{
                .mapped_memory = @alignCast(mapped_memory),
                .file = file,
                .file_size = aligned_size,
                .records = records,
                .capacity = capacity,
                .count = count,
                .topology = topology,
                .sync_policy = sync_policy,
                .numa_domains = numa_domains,
                .records_per_domain = records_per_domain,
            };
        } else {
            // In-memory only mode - allocate memory with huge page alignment
            const huge_page_size = 2 * 1024 * 1024;
            const records = try allocator.alignedAlloc(flat_hash_storage.CacheLineRecord, huge_page_size, capacity);

            // Initialize all records as invalid/empty
            for (records) |*record| {
                record.* = flat_hash_storage.CacheLineRecord.initEmpty();
            }

            const numa_domains = @as(u32, @intCast(topology.l3_domains.len));
            const records_per_domain = capacity / numa_domains;

            const storage_bytes = @as([*]u8, @ptrCast(records.ptr))[0 .. capacity * @sizeOf(flat_hash_storage.CacheLineRecord)];

            return Self{
                .mapped_memory = @alignCast(storage_bytes),
                .file = undefined, // Not used in memory-only mode
                .file_size = 0,
                .records = records,
                .capacity = capacity,
                .count = 0,
                .topology = topology,
                .sync_policy = sync_policy,
                .numa_domains = numa_domains,
                .records_per_domain = records_per_domain,
            };
        }
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        if (self.sync_policy != .none) {
            // Sync any pending changes before closing
            self.forceSync() catch {};

            // Unmap the memory
            std.posix.munmap(self.mapped_memory);
            self.file.close();
        } else {
            // Free regular allocated memory
            allocator.free(self.records);
        }
    }

    // Get hash position with linear probing for reads
    fn findRecord(self: *const Self, key: u64) ?u32 {
        if (self.count == 0) return null;

        const start_pos = @as(u32, @intCast(flat_hash_storage.hashKey(key) & (self.capacity - 1)));
        var pos = start_pos;

        while (true) {
            const record = &self.records[pos];

            if (!record.isValid()) {
                return null;
            }

            if (record.key == key) {
                return pos;
            }

            pos = (pos + 1) & (self.capacity - 1);
            if (pos == start_pos) break;
        }

        return null;
    }

    // Find empty slot for insertion with linear probing
    fn findEmptySlot(self: *const Self, key: u64) ?u32 {
        if (self.count >= self.capacity) return null;

        const start_pos = @as(u32, @intCast(flat_hash_storage.hashKey(key) & (self.capacity - 1)));
        var pos = start_pos;

        while (true) {
            const record = &self.records[pos];

            if (!record.isValid() or record.key == key) {
                return pos;
            }

            pos = (pos + 1) & (self.capacity - 1);
            if (pos == start_pos) break;
        }

        return null;
    }

    pub inline fn read(self: *const Self, key: u64) ?*const flat_hash_storage.CacheLineRecord {
        const pos = self.findRecord(key) orelse return null;
        return &self.records[pos];
    }

    pub inline fn write(self: *Self, key: u64, data: []const u8) !void {
        const pos = self.findEmptySlot(key) orelse return error.TableFull;

        const old_record = &self.records[pos];
        const is_update = old_record.isValid() and old_record.key == key;

        // Create new record
        self.records[pos] = try flat_hash_storage.CacheLineRecord.init(key, data);

        // Update count only for new insertions
        if (!is_update) {
            self.count += 1;
        }

        // Handle sync policy
        switch (self.sync_policy) {
            .none => {}, // No sync needed
            .write_through => {
                try self.syncRecord(pos);
            },
            .write_back, .async_sync => {
                // Mark as dirty for later sync (could track dirty bits here)
            },
        }
    }

    pub inline fn delete(self: *Self, key: u64) bool {
        const pos = self.findRecord(key) orelse return false;

        self.records[pos].markInvalid();
        self.count -= 1;

        // Handle sync policy for deletion
        switch (self.sync_policy) {
            .none => {},
            .write_through => {
                self.syncRecord(pos) catch {};
            },
            .write_back, .async_sync => {},
        }

        return true;
    }

    pub fn clear(self: *Self) void {
        @memset(std.mem.sliceAsBytes(self.records), 0);
        self.count = 0;

        // Sync the clear operation if needed
        switch (self.sync_policy) {
            .none => {},
            .write_through => {
                self.forceSync() catch {};
            },
            .write_back, .async_sync => {},
        }
    }

    // Sync a specific record to disk
    fn syncRecord(self: *Self, _: u32) !void {
        if (self.sync_policy == .none) return;

        // For individual record sync, sync the entire mapped memory
        // This is more efficient than trying to align individual records
        try std.posix.msync(self.mapped_memory, std.posix.MSF.SYNC);
    }

    // Async sync to disk
    pub fn asyncSync(self: *Self) !void {
        if (self.sync_policy == .none) return;

        try std.posix.msync(self.mapped_memory, std.posix.MSF.ASYNC);
    }

    // Force synchronous sync to disk
    pub fn forceSync(self: *Self) !void {
        if (self.sync_policy == .none) return;

        try std.posix.msync(self.mapped_memory, std.posix.MSF.SYNC);
    }

    pub fn getStats(self: *const Self) flat_hash_storage.StorageStats {
        return flat_hash_storage.StorageStats{
            .total_count = self.count,
            .total_capacity = self.capacity,
            .load_factor = @as(f32, @floatFromInt(self.count)) / @as(f32, @floatFromInt(self.capacity)),
            .numa_domains = self.numa_domains,
        };
    }

    pub fn printDetailedStats(self: *const Self) void {
        const stats = self.getStats();

        std.debug.print("\n=== Memory-Mapped Storage Statistics ===\n", .{});
        std.debug.print("Capacity: {}/{} records ({d:.1}% full)\n", .{ stats.total_count, stats.total_capacity, stats.load_factor * 100 });
        std.debug.print("NUMA domains: {}\n", .{stats.numa_domains});
        std.debug.print("Sync policy: {}\n", .{self.sync_policy});
        std.debug.print("File size: {} bytes\n", .{self.file_size});
    }
};
