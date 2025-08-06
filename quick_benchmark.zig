const std = @import("std");
const wild = @import("src/wild.zig");
const flat_hash_storage = @import("src/flat_hash_storage.zig");
const static_allocator = @import("src/static_allocator.zig");

pub fn main() !void {
    std.debug.print("=== WILD Quick Performance Test ===\n", .{});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create static allocator setup
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    var static_alloc = static_allocator.StaticAllocator.init(arena.allocator());

    // Initialize database - smaller capacity for testing
    const db_config = wild.WILD.Config{
        .target_capacity = 10_000, // Smaller for focused testing
    };

    var db = try wild.WILD.init(static_alloc.allocator(), db_config);
    defer {
        static_alloc.transitionToDeinit();
        db.deinit();
    }

    static_alloc.transitionToStatic();

    // Test data
    const test_data = "benchmark_test_data_32_bytes_!";
    var prng = std.Random.DefaultPrng.init(@as(u64, @intCast(std.time.nanoTimestamp())));
    const random = prng.random();

    std.debug.print("Testing core write performance (no durability)...\n", .{});

    // Generate unique keys for consistent testing
    const num_tests = 1000;
    var keys: [num_tests]u64 = undefined;
    for (&keys) |*key| {
        key.* = random.int(u64);
    }

    // Benchmark write latency
    var write_times: [num_tests]u64 = undefined;

    for (keys, 0..) |key, i| {
        const start_time = std.time.nanoTimestamp();
        try db.write(key, test_data);
        const end_time = std.time.nanoTimestamp();
        write_times[i] = @as(u64, @intCast(end_time - start_time));
    }

    // Calculate statistics
    var total_time: u64 = 0;
    var min_time: u64 = std.math.maxInt(u64);
    var max_time: u64 = 0;

    for (write_times) |time| {
        total_time += time;
        if (time < min_time) min_time = time;
        if (time > max_time) max_time = time;
    }

    const avg_time_ns = @as(f64, @floatFromInt(total_time)) / @as(f64, @floatFromInt(num_tests));

    std.debug.print("\n=== Core Write Performance Results ===\n", .{});
    std.debug.print("Operations:     {}\n", .{num_tests});
    std.debug.print("Average:        {d:.1} ns/write\n", .{avg_time_ns});
    std.debug.print("Minimum:        {} ns\n", .{min_time});
    std.debug.print("Maximum:        {} ns\n", .{max_time});
    std.debug.print("Target (42ns):  {s}\n", .{if (avg_time_ns <= 42.0) "✓ ACHIEVED" else "❌ MISSED"});

    // Test read performance
    std.debug.print("\nTesting read performance...\n", .{});
    var read_times: [num_tests]u64 = undefined;

    for (keys, 0..) |key, i| {
        const start_time = std.time.nanoTimestamp();
        _ = try db.read(key);
        const end_time = std.time.nanoTimestamp();
        read_times[i] = @as(u64, @intCast(end_time - start_time));
    }

    var total_read_time: u64 = 0;
    var min_read_time: u64 = std.math.maxInt(u64);
    var max_read_time: u64 = 0;

    for (read_times) |time| {
        total_read_time += time;
        if (time < min_read_time) min_read_time = time;
        if (time > max_read_time) max_read_time = time;
    }

    const avg_read_time_ns = @as(f64, @floatFromInt(total_read_time)) / @as(f64, @floatFromInt(num_tests));

    std.debug.print("\n=== Read Performance Results ===\n", .{});
    std.debug.print("Average:        {d:.1} ns/read\n", .{avg_read_time_ns});
    std.debug.print("Minimum:        {} ns\n", .{min_read_time});
    std.debug.print("Maximum:        {} ns\n", .{max_read_time});

    // Database statistics
    const stats = db.getStats();
    std.debug.print("\n=== Database Statistics ===\n", .{});
    std.debug.print("Load Factor:    {d:.1}%\n", .{stats.load_factor * 100});
    std.debug.print("Used/Total:     {}/{}\n", .{ stats.used_capacity, stats.total_capacity });
    std.debug.print("Memory Usage:   {d:.1} MB\n", .{@as(f64, @floatFromInt(stats.total_capacity * @sizeOf(flat_hash_storage.CacheLineRecord))) / (1024.0 * 1024.0)});

    // Hardware information
    std.debug.print("\n=== Hardware Information ===\n", .{});
    std.debug.print("Physical Cores: {}\n", .{stats.physical_cores});
    std.debug.print("Cache Line:     {} bytes\n", .{stats.cache_line_size});
    std.debug.print("Record Size:    {} bytes\n", .{@sizeOf(flat_hash_storage.CacheLineRecord)});

    std.debug.print("\n=== Performance Analysis ===\n", .{});
    if (avg_time_ns <= 42.0) {
        std.debug.print("🎉 SUCCESS: 42ns target ACHIEVED!\n", .{});
        std.debug.print("   Write performance: {d:.1}ns (target: ≤42ns)\n", .{avg_time_ns});
    } else {
        std.debug.print("⚠️  WARNING: 42ns target missed\n", .{});
        std.debug.print("   Write performance: {d:.1}ns (target: ≤42ns)\n", .{avg_time_ns});
        std.debug.print("   Difference: +{d:.1}ns\n", .{avg_time_ns - 42.0});
    }

    std.debug.print("\n=== Quick Benchmark Complete ===\n", .{});
}
