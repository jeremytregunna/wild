const std = @import("std");
const cache_topology = @import("cache_topology.zig");
const flat_hash_storage = @import("flat_hash_storage.zig");
const static_allocator = @import("static_allocator.zig");
const memory_mapped_storage = @import("memory_mapped_storage.zig");

// WILD Database - Cache-Resident Ultra-High-Performance Key-Value Store
pub const WILD = struct {
    const Self = @This();

    // Storage engine (either in-memory or memory-mapped)
    storage: StorageEngine,
    cache_topology: *cache_topology.CacheTopology,
    allocator: std.mem.Allocator,

    // Configuration
    target_capacity: u64,

    const StorageEngine = union(enum) {
        in_memory: flat_hash_storage.FlatHashStorage,
        memory_mapped: memory_mapped_storage.MemoryMappedStorage,
    };

    pub const Config = struct {
        target_capacity: u64,
        file_path: ?[]const u8 = null,
        sync_policy: memory_mapped_storage.SyncPolicy = .none,
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

        // Initialize storage based on configuration
        const storage = if (config.file_path) |_|
            StorageEngine{ .memory_mapped = try memory_mapped_storage.MemoryMappedStorage.init(allocator, topology_ptr, config.target_capacity, config.file_path, config.sync_policy) }
        else
            StorageEngine{ .in_memory = try flat_hash_storage.FlatHashStorage.init(allocator, topology_ptr, config.target_capacity) };

        // Note: Caller will transition static allocator to static state after init

        return Self{
            .storage = storage,
            .cache_topology = topology_ptr,
            .allocator = allocator,
            .target_capacity = config.target_capacity,
        };
    }

    pub fn deinit(self: *Self) void {
        // Note: Caller will transition static allocator to deinit state
        switch (self.storage) {
            .in_memory => |*storage| storage.deinit(),
            .memory_mapped => |*storage| storage.deinit(self.allocator),
        }
        self.cache_topology.deinit();
        self.allocator.destroy(self.cache_topology);
        // Note: Caller handles static allocator and arena cleanup
    }

    // Core single operations - no stats overhead
    pub inline fn read(self: *Self, key: u64) !?*const flat_hash_storage.CacheLineRecord {
        return switch (self.storage) {
            .in_memory => |*storage| storage.read(key),
            .memory_mapped => |*storage| storage.read(key),
        };
    }

    pub inline fn write(self: *Self, key: u64, data: []const u8) !void {
        return switch (self.storage) {
            .in_memory => |*storage| storage.write(key, data),
            .memory_mapped => |*storage| storage.write(key, data),
        };
    }

    pub inline fn delete(self: *Self, key: u64) !bool {
        return switch (self.storage) {
            .in_memory => |*storage| storage.delete(key),
            .memory_mapped => |*storage| storage.delete(key),
        };
    }

    pub inline fn clear(self: *Self) void {
        switch (self.storage) {
            .in_memory => |*storage| storage.clear(),
            .memory_mapped => |*storage| storage.clear(),
        }
    }

    // Durability operations
    pub fn asyncSync(self: *Self) !void {
        switch (self.storage) {
            .in_memory => {}, // No-op for in-memory storage
            .memory_mapped => |*storage| try storage.asyncSync(),
        }
    }

    pub fn forceSync(self: *Self) !void {
        switch (self.storage) {
            .in_memory => {}, // No-op for in-memory storage
            .memory_mapped => |*storage| try storage.forceSync(),
        }
    }

    // Batch operations for maximum performance - caller must provide result array
    pub fn readBatch(self: *Self, keys: []const u64, results: []?*const flat_hash_storage.CacheLineRecord) void {
        std.debug.assert(keys.len == results.len);

        // Simple loop - cache-line access is already optimized
        for (keys, 0..) |key, i| {
            results[i] = switch (self.storage) {
                .in_memory => |*storage| storage.read(key),
                .memory_mapped => |*storage| storage.read(key),
            };
        }
    }

    // Simplified write batch - no complex batching needed with cache-line approach
    pub fn writeBatch(self: *Self, keys: []const u64, data_items: []const []const u8) !void {
        std.debug.assert(keys.len == data_items.len);

        // Simple loop over writes
        for (keys, data_items) |key, data| {
            try switch (self.storage) {
                .in_memory => |*storage| storage.write(key, data),
                .memory_mapped => |*storage| storage.write(key, data),
            };
        }
    }

    pub fn deleteBatch(self: *Self, keys: []const u64) ![]bool {
        const results = try self.allocator.alloc(bool, keys.len);

        for (keys, 0..) |key, i| {
            results[i] = switch (self.storage) {
                .in_memory => |*storage| storage.delete(key),
                .memory_mapped => |*storage| storage.delete(key),
            };
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
        const storage_stats = switch (self.storage) {
            .in_memory => |*storage| storage.getStats(),
            .memory_mapped => |*storage| storage.getStats(),
        };

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
        switch (self.storage) {
            .in_memory => |*storage| storage.printDetailedStats(),
            .memory_mapped => |*storage| storage.printDetailedStats(),
        }

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
};
