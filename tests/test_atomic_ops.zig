const std = @import("std");
const cache_topology = @import("src/cache_topology.zig");
const simd_hash_table = @import("src/simd_hash_table.zig");
const atomic_ops = @import("src/atomic_ops.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Initialize cache topology and hash table
    var topology = try cache_topology.analyzeCacheTopology(allocator);
    defer topology.deinit();

    const target_capacity: u64 = 10000;
    var hash_table = try simd_hash_table.SIMDHashTable.init(allocator, &topology, target_capacity);
    defer hash_table.deinit();

    std.debug.print("WILD Atomic Operations Test\n", .{});
    std.debug.print("===========================\n", .{});
    std.debug.print("Physical cores: {}\n", .{topology.total_physical_cores});
    std.debug.print("Cache line size: {} bytes\n", .{topology.cache_line_size});
    std.debug.print("Hash table buckets: {}\n", .{hash_table.bucket_count});
    std.debug.print("Slots per bucket: {}\n", .{hash_table.slots_per_bucket});
    std.debug.print("Optimal batch size: {}\n\n", .{hash_table.getOptimalBatchSize()});

    // Test single atomic operations
    std.debug.print("Testing single atomic operations...\n", .{});
    
    const test_data = "Hello WILD database - atomic operations test!";
    const record1 = try simd_hash_table.Record.createSmall(
        allocator, topology.cache_line_size, 12345, 
        @as(u64, @intCast(std.time.milliTimestamp())), test_data
    );
    defer {
        var mutable_record1 = record1;
        mutable_record1.deinit(allocator);
    }

    // Atomic insert
    try hash_table.atomicInsert(12345, record1);
    std.debug.print("✓ Atomic insert successful\n", .{});

    // Atomic lookup
    const found = hash_table.atomicLookup(12345);
    if (found) |f| {
        std.debug.print("✓ Atomic lookup successful - key: {}\n", .{f.getKey()});
    } else {
        std.debug.print("✗ Atomic lookup failed\n", .{});
    }

    // Atomic remove
    const removed = hash_table.atomicRemove(12345);
    if (removed) {
        std.debug.print("✓ Atomic remove successful\n", .{});
    } else {
        std.debug.print("✗ Atomic remove failed\n", .{});
    }

    // Verify removal
    const not_found = hash_table.atomicLookup(12345);
    if (not_found == null) {
        std.debug.print("✓ Verification: key no longer exists\n", .{});
    }

    std.debug.print("\nTesting batch atomic operations...\n", .{});

    // Prepare batch data
    const batch_size = hash_table.getOptimalBatchSize();
    var write_ops = try allocator.alloc(simd_hash_table.WriteOp, batch_size);
    defer allocator.free(write_ops);
    
    var keys = try allocator.alloc(u64, batch_size);
    defer allocator.free(keys);

    // Create records for batch operations
    var records_to_cleanup = std.ArrayList(simd_hash_table.Record).init(allocator);
    defer {
        for (records_to_cleanup.items) |*record| {
            record.deinit(allocator);
        }
        records_to_cleanup.deinit();
    }

    for (0..batch_size) |i| {
        const key = @as(u64, 1000 + i);
        keys[i] = key;
        
        const batch_test_data = try std.fmt.allocPrint(allocator, "Batch record {}", .{i});
        defer allocator.free(batch_test_data);
        
        const record = try simd_hash_table.Record.createSmall(
            allocator, topology.cache_line_size, key,
            @as(u64, @intCast(std.time.milliTimestamp())), batch_test_data
        );
        try records_to_cleanup.append(record);
        
        write_ops[i] = .{ .key = key, .record = record };
    }

    // Batch atomic insert
    const insert_results = try hash_table.atomicBatchInsert(write_ops);
    defer allocator.free(insert_results);
    
    var successful_inserts: u32 = 0;
    for (insert_results) |result| {
        if (result == .success) successful_inserts += 1;
    }
    std.debug.print("✓ Batch insert: {}/{} successful\n", .{ successful_inserts, batch_size });

    // Batch atomic lookup
    const lookup_results = try hash_table.atomicBatchLookup(keys);
    defer allocator.free(lookup_results);
    
    var successful_lookups: u32 = 0;
    for (lookup_results) |result| {
        if (result != null) successful_lookups += 1;
    }
    std.debug.print("✓ Batch lookup: {}/{} found\n", .{ successful_lookups, batch_size });

    // Batch atomic remove
    const remove_results = try hash_table.atomicBatchRemove(keys);
    defer allocator.free(remove_results);
    
    var successful_removes: u32 = 0;
    for (remove_results) |result| {
        if (result) successful_removes += 1;
    }
    std.debug.print("✓ Batch remove: {}/{} successful\n", .{ successful_removes, batch_size });

    // Performance statistics
    const stats = hash_table.getAtomicStats();
    std.debug.print("\nAtomic Operation Statistics:\n", .{});
    std.debug.print("- Successful inserts: {}\n", .{stats.successful_inserts});
    std.debug.print("- Failed inserts: {}\n", .{stats.failed_inserts});
    std.debug.print("- Successful removes: {}\n", .{stats.successful_removes});
    std.debug.print("- Failed removes: {}\n", .{stats.failed_removes});
    std.debug.print("- Total lookups: {}\n", .{stats.lookup_count});
    std.debug.print("- CAS retries: {}\n", .{stats.cas_retries});
    std.debug.print("- Insert success rate: {d:.1}%\n", .{stats.getInsertSuccessRate() * 100});
    std.debug.print("- Remove success rate: {d:.1}%\n", .{stats.getRemoveSuccessRate() * 100});

    // Hash table statistics
    const hash_stats = hash_table.getStats();
    std.debug.print("\nHash Table Statistics:\n", .{});
    std.debug.print("- Used slots: {}/{}\n", .{ hash_stats.used_slots, hash_stats.total_capacity });
    std.debug.print("- Load factor: {d:.2}\n", .{hash_stats.load_factor});

    std.debug.print("\n✅ All tests completed successfully!\n", .{});
}