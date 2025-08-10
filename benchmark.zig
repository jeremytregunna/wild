const std = @import("std");
const wild = @import("src/wild.zig");
const flat_hash_storage = @import("src/flat_hash_storage.zig");
const static_allocator = @import("src/static_allocator.zig");

// WILD Database Benchmark Suite
pub fn main() !void {
    // Benchmark's own arena allocator for benchmark operations
    const page_allocator = std.heap.page_allocator;
    var benchmark_arena = std.heap.ArenaAllocator.init(page_allocator);
    defer benchmark_arena.deinit();
    const benchmark_allocator = benchmark_arena.allocator();

    // Database's arena allocator for static allocation
    var database_arena = std.heap.ArenaAllocator.init(page_allocator);
    defer database_arena.deinit();

    // Create static allocator for database
    var static_alloc = static_allocator.StaticAllocator.init(database_arena.allocator());

    std.debug.print("WILD Database Benchmark\n", .{});

    // Detect cache topology first to get actual L3 cache size
    const cache_topology = @import("src/cache_topology.zig");
    const topology = try cache_topology.analyzeCacheTopology(static_alloc.allocator());

    // Calculate detected L3 cache size
    var total_l3_cache_kb: u64 = 0;
    for (topology.l3_domains) |domain| {
        total_l3_cache_kb += domain.cache_size_kb;
    }
    const detected_cache_mb = @as(u32, @intCast(total_l3_cache_kb / 1024));

    // Calculate target capacity based on actual cache size and record size
    const cache_line_size = topology.cache_line_size;
    const record_size = cache_line_size; // Each record fits in one cache line
    const available_cache_bytes = detected_cache_mb * 1024 * 1024;
    const target_capacity = @as(u64, @intFromFloat(@as(f64, @floatFromInt(available_cache_bytes)) * 0.75 / @as(f64, @floatFromInt(record_size)))); // 75% load factor

    // Initialize WILD database with calculated capacity
    const config = wild.WILD.Config{
        .target_capacity = target_capacity,
    };

    var database = try wild.WILD.init(static_alloc.allocator(), config);

    // Transition to static state - no more database allocations allowed
    static_alloc.transitionToStatic();

    // Get actual database capacity for benchmark sizing
    const db_stats = database.getStats();
    const safe_capacity = @as(u32, @intFromFloat(@as(f64, @floatFromInt(db_stats.total_capacity)) * 0.6)); // Use 60% of capacity for testing

    std.debug.print("Database: {} MB cache, {} records\n", .{ detected_cache_mb, db_stats.total_capacity });

    // Run benchmark suite using benchmark allocator with safe capacity
    try runSingleOperationBenchmarks(&database, benchmark_allocator, safe_capacity);

    // Clear database between benchmarks to ensure clean state
    database.clear();
    std.debug.print("Database cleared between benchmarks\n", .{});

    try runBatchOperationBenchmarks(&database, benchmark_allocator, safe_capacity);

    // Clear database between benchmarks to ensure clean state
    database.clear();
    std.debug.print("Database cleared between benchmarks\n", .{});

    try runMixedWorkloadBenchmark(&database, benchmark_allocator, safe_capacity);

    // Final hardware report
    database.printHardwareReport();

    // Cleanup in correct order: database first, then static allocator
    static_alloc.transitionToDeinit();
    database.deinit();
    static_alloc.deinit();
}

fn runSingleOperationBenchmarks(database: *wild.WILD, allocator: std.mem.Allocator, max_records: u32) !void {
    std.debug.print("\nSingle Operations\n", .{});

    // Use half of max_records for each phase to stay within capacity
    // const num_operations = max_records / 2;
    var keys = try allocator.alloc(u64, max_records);
    defer allocator.free(keys);

    const n_reads = max_records / 2;
    const n_writes = max_records - n_reads;

    // Pre-allocate all test data strings to avoid allocPrint during benchmarks
    var test_data = try allocator.alloc([]u8, max_records);
    defer {
        for (test_data) |data| allocator.free(data);
        allocator.free(test_data);
    }

    // Prepare test data for both read and write
    for (0..max_records) |i| {
        keys[i] = @as(u64, i + 1);
        if (i < n_reads) {
            test_data[i] = try std.fmt.allocPrint(allocator, "test_data_{}", .{keys[i]});
            try database.write(keys[i], test_data[i]);
        } else {
            test_data[i] = try std.fmt.allocPrint(allocator, "write_test_{}", .{keys[i]});
        }
    }

    std.debug.print("Write test: ", .{});
    const write_start = std.time.nanoTimestamp();

    for (n_reads..max_records) |index| {
        try database.write(keys[index], test_data[index]);
    }

    const write_end = std.time.nanoTimestamp();
    const write_duration = @as(f64, @floatFromInt(write_end - write_start)) / 1_000_000_000.0;
    const write_ops_per_sec = @as(f64, @floatFromInt(n_writes)) / write_duration;

    std.debug.print("{} ops in {d:.3}s = {d:.0} ops/sec\n", .{ n_writes, write_duration, write_ops_per_sec });

    std.debug.print("Read test:  ", .{});
    var successful_reads: u32 = 0;
    const read_start = std.time.nanoTimestamp();
    for (keys[0..n_reads]) |key| {
        const result = try database.read(key);
        if (result != null) successful_reads += 1;
    }

    const read_end = std.time.nanoTimestamp();
    const read_duration = @as(f64, @floatFromInt(read_end - read_start)) / 1_000_000_000.0;
    const read_ops_per_sec = @as(f64, @floatFromInt(n_reads)) / read_duration;

    std.debug.print("{} ops in {d:.3}s = {d:.0} ops/sec ({} found)\n", .{ n_reads, read_duration, read_ops_per_sec, successful_reads });

    // Latency measurements
    const avg_write_latency = (write_duration * 1_000_000_000.0) / @as(f64, @floatFromInt(n_writes));
    const avg_read_latency = (read_duration * 1_000_000_000.0) / @as(f64, @floatFromInt(n_reads));

    std.debug.print("Latency - Write: {d:.0}ns, Read: {d:.0}ns\n", .{ avg_write_latency, avg_read_latency });

    // Compare to Redis typical performance (rough estimates)
    const redis_write_latency_ns = 50_000; // ~50μs for Redis
    const redis_read_latency_ns = 25_000; // ~25μs for Redis

    const write_speedup = @as(f64, @floatFromInt(redis_write_latency_ns)) / avg_write_latency;
    const read_speedup = @as(f64, @floatFromInt(redis_read_latency_ns)) / avg_read_latency;

    std.debug.print("vs Redis: {d:.1}x write, {d:.1}x read\n", .{ write_speedup, read_speedup });
}

fn runBatchOperationBenchmarks(database: *wild.WILD, allocator: std.mem.Allocator, max_records: u32) !void {
    std.debug.print("\nBatch Operations\n", .{});

    const optimal_batch_size = database.getOptimalBatchSize();
    const max_batches = @max(1, max_records / optimal_batch_size); // Calculate based on safe capacity
    const num_batches = max_batches; // Use max_batches directly to respect safe_capacity
    const total_operations = optimal_batch_size * num_batches;

    var batch_keys = try allocator.alloc(u64, optimal_batch_size);
    defer allocator.free(batch_keys);

    // Pre-allocate batch data strings
    var batch_data = try allocator.alloc([]u8, optimal_batch_size);
    defer {
        for (batch_data) |data| allocator.free(data);
        allocator.free(batch_data);
    }

    // Prepare batch data with completely different key space to avoid collisions
    for (0..optimal_batch_size) |i| {
        // Use a completely different key space that won't collide with single ops (keys 1-5000)
        // Start from a large offset and use incremental keys for better distribution
        const key = @as(u64, 1_000_000_000) + @as(u64, i); // Simple incremental keys in high range
        batch_keys[i] = key;
        batch_data[i] = try std.fmt.allocPrint(allocator, "batch_data_{}", .{key});
    }

    std.debug.print("Batch write: ", .{});
    const batch_write_start = std.time.nanoTimestamp();

    for (0..num_batches) |_| {
        for (batch_keys, 0..) |key, i| {
            try database.write(key, batch_data[i]);
        }
    }

    const batch_write_end = std.time.nanoTimestamp();
    const batch_write_duration = @as(f64, @floatFromInt(batch_write_end - batch_write_start)) / 1_000_000_000.0;
    const batch_write_ops_per_sec = @as(f64, @floatFromInt(total_operations)) / batch_write_duration;

    std.debug.print("{} ops in {d:.3}s = {d:.0} ops/sec\n", .{ total_operations, batch_write_duration, batch_write_ops_per_sec });

    std.debug.print("Batch read:  ", .{});
    // Allocate results array once
    const read_results = try allocator.alloc(?*const flat_hash_storage.CacheLineRecord, optimal_batch_size);
    defer allocator.free(read_results);
    var total_found: u32 = 0;

    const batch_read_start = std.time.nanoTimestamp();
    for (0..num_batches) |_| {
        database.readBatch(batch_keys, read_results);
        for (read_results) |result| {
            if (result != null) total_found += 1;
        }
    }
    const batch_read_end = std.time.nanoTimestamp();
    const batch_read_duration = @as(f64, @floatFromInt(batch_read_end - batch_read_start)) / 1_000_000_000.0;
    const batch_read_ops_per_sec = @as(f64, @floatFromInt(total_operations)) / batch_read_duration;

    std.debug.print("{} ops in {d:.3}s = {d:.0} ops/sec ({} found)\n", .{ total_operations, batch_read_duration, batch_read_ops_per_sec, total_found });

    const batch_write_latency = (batch_write_duration * 1_000_000_000.0) / @as(f64, @floatFromInt(total_operations));
    const batch_read_latency = (batch_read_duration * 1_000_000_000.0) / @as(f64, @floatFromInt(total_operations));
    std.debug.print("Batch latency - Write: {d:.0}ns, Read: {d:.0}ns\n", .{ batch_write_latency, batch_read_latency });
}

fn runMixedWorkloadBenchmark(database: *wild.WILD, allocator: std.mem.Allocator, max_records: u32) !void {
    std.debug.print("\nMixed Workload\n", .{});

    const duration_seconds = 10; // Increase to 10 seconds for better measurement
    const batch_size = @min(database.getOptimalBatchSize(), max_records / 4); // Use smaller batch size to fit within capacity

    var keys = try allocator.alloc(u64, batch_size);
    defer allocator.free(keys);

    // Pre-allocate mixed workload data strings
    var prep_data = try allocator.alloc([]u8, batch_size);
    defer {
        for (prep_data) |data| allocator.free(data);
        allocator.free(prep_data);
    }

    var write_data = try allocator.alloc([]u8, batch_size);
    defer {
        for (write_data) |data| allocator.free(data);
        allocator.free(write_data);
    }

    // Prepare workload data with different key space
    for (0..batch_size) |i| {
        // Use yet another key space for mixed workload to avoid all previous keys
        keys[i] = @as(u64, 2_000_000_000) + @as(u64, i);
        prep_data[i] = try std.fmt.allocPrint(allocator, "mixed_workload_{}", .{i});
        write_data[i] = try std.fmt.allocPrint(allocator, "mixed_write_{}", .{keys[i]});

        // Write preparation data to database
        try database.write(keys[i], prep_data[i]);
    }

    // Data is already written to database, just use keys for mixed workload

    // Mixed workload: 50% reads, 50% writes
    const start_time = std.time.nanoTimestamp();
    const end_time = start_time + (duration_seconds * 1_000_000_000);

    var total_operations: u64 = 0;
    var read_count: u64 = 0;
    var write_count: u64 = 0;

    while (std.time.nanoTimestamp() < end_time) {
        // Alternate between batch reads and writes
        if (total_operations & 1 == 0) {
            // Batch read - allocate results array
            const read_results = try allocator.alloc(?*const flat_hash_storage.CacheLineRecord, batch_size);
            defer allocator.free(read_results);

            database.readBatch(keys, read_results);
            read_count += batch_size;
        } else {
            // Individual writes using pre-allocated data
            for (keys, 0..) |key, i| {
                try database.write(key, write_data[i]);
            }
            write_count += batch_size;
        }

        total_operations += batch_size;
    }

    const actual_duration = @as(f64, @floatFromInt(std.time.nanoTimestamp() - start_time)) / 1_000_000_000.0;
    const ops_per_sec = @as(f64, @floatFromInt(total_operations)) / actual_duration;

    std.debug.print("Mixed workload: {} ops in {d:.2}s = {d:.0} ops/sec\n", .{ total_operations, actual_duration, ops_per_sec });

    const stats = database.getStats();
    const ops_per_sec_per_core = ops_per_sec / @as(f64, @floatFromInt(stats.physical_cores));
    std.debug.print("Per-core: {d:.0} ops/sec\n", .{ops_per_sec_per_core});

    const target_per_core = 10_000_000;
    const achievement_percentage = (ops_per_sec_per_core / @as(f64, @floatFromInt(target_per_core))) * 100.0;
    std.debug.print("Target: {d:.1}% of 10M ops/sec/core\n", .{achievement_percentage});

    if (achievement_percentage >= 100.0) {
        std.debug.print("Target achieved!\n", .{});
    } else if (achievement_percentage >= 50.0) {
        std.debug.print("Close to target.\n", .{});
    } else {
        std.debug.print("Room for optimization.\n", .{});
    }
}
