const std = @import("std");
const cache_topology = @import("src/cache_topology.zig");
const simd_hash_table = @import("src/simd_hash_table.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Test cache topology detection with CPUID
    var topology = try cache_topology.analyzeCacheTopology(allocator);
    defer topology.deinit();

    std.debug.print("Cache Topology Detection:\n", .{});
    std.debug.print("- Total CPUs: {}\n", .{topology.total_cpus});
    std.debug.print("- Physical cores: {}\n", .{topology.total_physical_cores});
    std.debug.print("- Cache line size: {} bytes\n", .{topology.cache_line_size});
    std.debug.print("- Number of shards: {}\n", .{topology.shard_mappings.len});

    // Test SIMD hash table with dynamic sizing
    const target_capacity: u64 = 1000;
    var hash_table = try simd_hash_table.SIMDHashTable.init(allocator, &topology, target_capacity);
    defer hash_table.deinit();
    
    const stats = hash_table.getStats();
    std.debug.print("\nSIMD Hash Table:\n", .{});
    std.debug.print("- Bucket count: {}\n", .{stats.bucket_count});
    std.debug.print("- Slots per bucket: {}\n", .{stats.slots_per_bucket});
    std.debug.print("- Total capacity: {}\n", .{stats.total_capacity});
    std.debug.print("- Optimal batch size: {}\n", .{hash_table.getOptimalBatchSize()});

    // Test record creation with dynamic cache line sizing
    const test_data = "Hello, WILD database!";
    const record = try simd_hash_table.Record.createSmall(
        allocator, 
        topology.cache_line_size, 
        12345, 
        @as(u64, @intCast(std.time.milliTimestamp())),
        test_data
    );
    defer {
        var mutable_record = record;
        mutable_record.deinit(allocator);
    }
    
    std.debug.print("\nRecord Test:\n", .{});
    std.debug.print("- Key: {}\n", .{record.getKey()});
    std.debug.print("- Timestamp: {}\n", .{record.timestamp});
    std.debug.print("- Data length: {}\n", .{record.data.len});
    std.debug.print("- Total size: {} bytes\n", .{record.getTotalSize()});
    std.debug.print("- Cache line optimized for: {} bytes\n", .{record.cache_line_size});
}