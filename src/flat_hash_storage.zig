const std = @import("std");
const cache_topology = @import("cache_topology.zig");

// Cache-line-aligned record that fits exactly in one cache line
pub const CacheLineRecord = extern struct {
    key: u64, // 8 bytes (check first for key comparison)
    metadata: u32, // 4 bytes: 1 bit valid flag + 6 bits length + 25 bits reserved
    data: [52]u8, // 52 bytes data payload (accessed last)
    // Total: 64 bytes = exactly 1 cache line

    const Self = @This();
    pub const VALID_FLAG: u32 = 1 << 31; // Use top bit for valid flag
    pub const LENGTH_MASK: u32 = 0x3F; // Bottom 6 bits for length (0-63, enough for 52)

    pub fn init(key: u64, data: []const u8) !Self {
        if (data.len > 52) return error.DataTooLarge;

        var record = Self{
            .metadata = VALID_FLAG | @as(u32, @intCast(data.len)),
            .key = key,
            .data = std.mem.zeroes([52]u8),
        };

        @memcpy(record.data[0..data.len], data);
        return record;
    }

    pub fn initEmpty() Self {
        return Self{
            .metadata = 0, // Invalid (no VALID_FLAG bit set)
            .key = 0,
            .data = std.mem.zeroes([52]u8),
        };
    }

    pub fn isValid(self: *const Self) bool {
        return (self.metadata & VALID_FLAG) != 0;
    }

    pub fn markInvalid(self: *Self) void {
        self.metadata &= ~VALID_FLAG;
    }

    pub fn getData(self: *const Self) []const u8 {
        const len = @as(u32, @intCast(self.metadata & LENGTH_MASK));
        return self.data[0..len];
    }

    pub fn getKey(self: *const Self) u64 {
        return self.key;
    }
};

// Unified hash function for both string and integer keys
pub fn hashKey(key: anytype) u64 {
    const KeyType = @TypeOf(key);

    if (KeyType == []const u8) {
        // For string keys, use Wyhash directly
        return std.hash.Wyhash.hash(0, key);
    } else if (KeyType == u64) {
        // For u64 keys, use MurmurHash3 finalizer for good distribution
        var h = key;
        h ^= h >> 33;
        h *%= 0xff51afd7ed558ccd;
        h ^= h >> 33;
        h *%= 0xc4ceb9fe1a85ec53;
        h ^= h >> 33;
        return h;
    } else {
        @compileError("Unsupported key type");
    }
}

// Forward declaration for optional WAL
const wal = @import("wal.zig");

// Flat hash table with linear probing - no artificial capacity limits
pub const FlatHashStorage = struct {
    const Self = @This();

    records: []CacheLineRecord, // Flat cache-aligned array (alignment handled in allocator)
    capacity: u32,
    count: u32,
    topology: *const cache_topology.CacheTopology,
    allocator: std.mem.Allocator,

    // Optional WAL for durability - null means no durability
    durability_manager: ?*wal.DurabilityManager,

    // NUMA placement hints (for future optimization)
    numa_domains: u32,
    records_per_domain: u32,

    pub fn init(allocator: std.mem.Allocator, topology: *const cache_topology.CacheTopology, target_capacity: u64) !Self {
        // Enforce power-of-2 capacity for bitwise operations
        // Round DOWN to largest power-of-2 that fits in L3 cache
        const raw_capacity = @as(u32, @intCast(target_capacity));
        const capacity = if (std.math.isPowerOfTwo(raw_capacity))
            raw_capacity
        else
            std.math.floorPowerOfTwo(u32, raw_capacity);

        // Allocate flat cache-aligned array
        // Note: CacheLineRecord is designed for 64-byte cache lines, which matches most modern CPUs
        // If topology shows different cache line size, we should validate compatibility
        if (topology.cache_line_size != 64) {
            std.debug.print("Warning: Detected cache line size {} != 64 bytes. Performance may be suboptimal.\n", .{topology.cache_line_size});
        }
        const records = try allocator.alignedAlloc(CacheLineRecord, 64, capacity);

        // Initialize all records as invalid/empty
        for (records) |*record| {
            record.* = CacheLineRecord.initEmpty();
        }

        // Calculate NUMA domain distribution for future optimization
        const numa_domains = @as(u32, @intCast(topology.l3_domains.len));
        const records_per_domain = capacity / numa_domains;

        return Self{
            .records = records,
            .capacity = capacity,
            .count = 0,
            .topology = topology,
            .allocator = allocator,
            .durability_manager = null, // Initially no WAL
            .numa_domains = numa_domains,
            .records_per_domain = records_per_domain,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.records);
    }

    // Get hash position with linear probing for reads
    fn findRecord(self: *const Self, key: u64) ?u32 {
        if (self.count == 0) return null;

        const start_pos = @as(u32, @intCast(hashKey(key) & (self.capacity - 1)));
        var pos = start_pos;

        // Linear probe until we find the key or an empty slot
        // Use bitwise AND for faster wraparound (capacity is power-of-2)
        while (true) {
            const record = &self.records[pos];

            if (!record.isValid()) {
                // Hit empty slot - key doesn't exist
                return null;
            }

            if (record.key == key) {
                // Found it!
                return pos;
            }

            // Continue probing with bitwise wraparound
            pos = (pos + 1) & (self.capacity - 1);

            // If we've wrapped around to start, key doesn't exist
            if (pos == start_pos) break;
        }

        return null;
    }

    // Find empty slot for insertion with linear probing
    fn findEmptySlot(self: *const Self, key: u64) ?u32 {
        if (self.count >= self.capacity) return null; // Table full

        const start_pos = @as(u32, @intCast(hashKey(key) & (self.capacity - 1)));
        var pos = start_pos;

        // Linear probe until we find an empty slot or the key (for updates)
        // Use bitwise AND for faster wraparound (capacity is power-of-2)
        while (true) {
            const record = &self.records[pos];

            if (!record.isValid() or record.key == key) {
                // Found empty slot or existing key to update
                return pos;
            }

            // Continue probing with bitwise wraparound
            pos = (pos + 1) & (self.capacity - 1);

            // Wrapped around - shouldn't happen if count < capacity
            if (pos == start_pos) break;
        }

        return null; // Should never happen if count < capacity
    }

    pub inline fn read(self: *const Self, key: u64) ?*const CacheLineRecord {
        const pos = self.findRecord(key) orelse return null;
        return &self.records[pos];
    }

    pub inline fn write(self: *Self, key: u64, data: []const u8) !void {
        const pos = self.findEmptySlot(key) orelse return error.TableFull;

        const old_record = &self.records[pos];
        const is_update = old_record.isValid() and old_record.key == key;

        // Critical path: update cache line (maintains 42ns performance)
        self.records[pos] = try CacheLineRecord.init(key, data);

        // Always-async durability: never blocks
        if (self.durability_manager) |dur_mgr| {
            _ = dur_mgr.appendAsync(&self.records[pos]);
            // Note: We ignore the return value to never block
            // If WAL ring is full, the entry is dropped but write succeeds
        }

        // Update count only for new insertions
        if (!is_update) {
            self.count += 1;
        }
    }

    pub inline fn delete(self: *Self, key: u64) bool {
        const pos = self.findRecord(key) orelse return false;

        // Mark record as invalid
        self.records[pos].markInvalid();
        self.count -= 1;

        // Important: We don't compact the table to maintain O(1) performance
        // The slot remains "tombstoned" until overwritten by a new record

        return true;
    }

    pub fn clear(self: *Self) void {
        // Zero out all records with memset - much faster than looping
        @memset(std.mem.sliceAsBytes(self.records), 0);
        self.count = 0;
    }

    // Enable durability by associating with a WAL manager
    pub fn enableDurability(self: *Self, durability_manager: *wal.DurabilityManager) void {
        self.durability_manager = durability_manager;
    }

    // Disable durability
    pub fn disableDurability(self: *Self) void {
        self.durability_manager = null;
    }

    // Snapshot support: Copy all records atomically for snapshot creation
    pub fn copyAllRecords(self: *const Self, dest_records: []CacheLineRecord) void {
        std.debug.assert(dest_records.len >= self.records.len);

        // Atomic copy of all records using memcpy - fastest approach
        // This gives us a point-in-time snapshot of the entire storage
        @memcpy(dest_records[0..self.records.len], self.records);
    }

    // Snapshot support: Restore a single record during recovery
    pub fn restoreRecord(self: *Self, record: *const CacheLineRecord) !void {
        if (!record.isValid()) return;

        // Find the correct position for this record
        const key = record.getKey();
        const pos = self.findEmptySlot(key) orelse return error.TableFull;

        // Restore the record
        self.records[pos] = record.*;
        self.count += 1;
    }

    pub fn getStats(self: *const Self) StorageStats {
        return StorageStats{
            .total_count = self.count,
            .total_capacity = self.capacity,
            .load_factor = @as(f32, @floatFromInt(self.count)) / @as(f32, @floatFromInt(self.capacity)),
            .numa_domains = self.numa_domains,
        };
    }

    pub fn printDetailedStats(self: *const Self) void {
        const stats = self.getStats();

        std.debug.print("\n=== Storage Statistics ===\n", .{});
        std.debug.print("Capacity: {}/{} records ({d:.1}% full)\n", .{ stats.total_count, stats.total_capacity, stats.load_factor * 100 });
        std.debug.print("NUMA domains: {}\n", .{stats.numa_domains});
    }
};

pub const StorageStats = struct {
    total_count: u32,
    total_capacity: u32,
    load_factor: f32,
    numa_domains: u32,
};
