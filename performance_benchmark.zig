const std = @import("std");
const wild = @import("src/wild.zig");
const flat_hash_storage = @import("src/flat_hash_storage.zig");
const wal = @import("src/wal.zig");
const snapshot = @import("src/snapshot.zig");
const static_allocator = @import("src/static_allocator.zig");

// Performance benchmark for WILD database
// Tests core performance claims and durability overhead

const BenchmarkConfig = struct {
    target_capacity: u64 = 100_000,
    warm_up_iterations: u64 = 10_000,
    benchmark_iterations: u64 = 1_000_000,
    batch_size: u32 = 1000,
    test_data_size: u32 = 32, // bytes per record
    enable_wal: bool = true,
    enable_snapshots: bool = true,
};

const BenchmarkResults = struct {
    single_write_ns: f64,
    single_read_ns: f64,
    batch_write_ns_per_op: f64,
    batch_read_ns_per_op: f64,
    memory_usage_mb: f64,
    wal_overhead_ns: f64,
    recovery_time_ms: f64,
    throughput_ops_per_sec: f64,
    load_factor: f32,
};

pub fn main() !void {
    std.debug.print("=== WILD Performance Benchmark Suite ===\n", .{});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Clean up test files
    cleanup() catch {};

    const config = BenchmarkConfig{};

    // Benchmark 1: Core performance without durability
    std.debug.print("\n--- Benchmark 1: Core Performance (No Durability) ---\n", .{});
    const core_results = try runCoreBenchmark(allocator, config, false, false);
    printResults("Core Performance", core_results);

    // Benchmark 2: Performance with WAL durability
    std.debug.print("\n--- Benchmark 2: Performance with WAL Durability ---\n", .{});
    const wal_results = try runCoreBenchmark(allocator, config, true, false);
    printResults("WAL Durability", wal_results);

    // Benchmark 3: Performance with WAL + Snapshots
    std.debug.print("\n--- Benchmark 3: Performance with WAL + Snapshots ---\n", .{});
    const full_results = try runCoreBenchmark(allocator, config, true, true);
    printResults("Full Durability", full_results);

    // Benchmark 4: Recovery performance
    std.debug.print("\n--- Benchmark 4: Recovery Performance ---\n", .{});
    try runRecoveryBenchmark(allocator, config);

    // Benchmark 5: Load factor vs performance
    std.debug.print("\n--- Benchmark 5: Load Factor Impact ---\n", .{});
    try runLoadFactorBenchmark(allocator, config);

    // Summary
    std.debug.print("\n=== Performance Summary ===\n", .{});
    std.debug.print("Core Write Latency:     {d:.1} ns\n", .{core_results.single_write_ns});
    std.debug.print("WAL Overhead:           {d:.1} ns ({d:.1}% impact)\n", .{ wal_results.single_write_ns - core_results.single_write_ns, ((wal_results.single_write_ns - core_results.single_write_ns) / core_results.single_write_ns) * 100 });
    std.debug.print("Target (42ns):          {s} {s}\n", .{ if (core_results.single_write_ns <= 42.0) "✓ ACHIEVED" else "❌ MISSED", if (core_results.single_write_ns <= 42.0) "" else "(Performance regression detected)" });

    std.debug.print("\n=== Benchmark Complete ===\n", .{});
}

fn cleanup() !void {
    const paths = [_][]const u8{
        "/tmp/benchmark_wild.wal",
        "/tmp/benchmark_snapshots",
    };

    for (paths) |path| {
        std.fs.cwd().deleteFile(path) catch {};
        std.fs.cwd().deleteTree(path) catch {};
    }
}

fn runCoreBenchmark(allocator: std.mem.Allocator, config: BenchmarkConfig, enable_wal: bool, enable_snapshots: bool) !BenchmarkResults {
    // Create static allocator setup
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var static_alloc = static_allocator.StaticAllocator.init(arena_allocator);

    // Initialize database
    const db_config = wild.WILD.Config{
        .target_capacity = config.target_capacity,
    };

    var db = try wild.WILD.init(static_alloc.allocator(), db_config);
    defer {
        static_alloc.transitionToDeinit();
        db.deinit();
    }

    // Enable durability if requested
    if (enable_wal) {
        const wal_config = wal.DurabilityManager.Config{
            .wal_path = "/tmp/benchmark_wild.wal",
            .max_threads = 4,
            .ring_buffer_size = 4096,
            .batch_buffer_count = 16,
            .io_uring_entries = 64,
        };
        try db.enableDurability(wal_config);
    }

    // Enable snapshots if requested
    if (enable_snapshots) {
        const snapshot_config = snapshot.SnapshotManager.Config{
            .snapshot_dir = "/tmp/benchmark_snapshots",
            .max_snapshots = 5,
            .snapshot_interval_seconds = 300, // 5 minutes
        };
        try db.enableSnapshots(snapshot_config);
    }

    // Freeze static allocator
    static_alloc.transitionToStatic();

    // Prepare test data
    const test_data = try allocator.alloc(u8, config.test_data_size);
    defer allocator.free(test_data);
    @memset(test_data, 0xAB); // Fill with test pattern

    var prng = std.Random.DefaultPrng.init(@as(u64, @intCast(std.time.nanoTimestamp())));
    const random = prng.random();

    std.debug.print("Warming up with {} iterations...\n", .{config.warm_up_iterations});

    // Warm-up phase
    var i: u64 = 0;
    while (i < config.warm_up_iterations) : (i += 1) {
        const key = random.int(u64);
        db.write(key, test_data) catch |err| switch (err) {
            error.TableFull => break, // Expected at high load factors
            else => return err,
        };
    }

    // Single operation benchmarks
    std.debug.print("Benchmarking single operations...\n", .{});

    const write_times = try allocator.alloc(u64, @intCast(config.benchmark_iterations));
    defer allocator.free(write_times);
    const read_times = try allocator.alloc(u64, @intCast(config.benchmark_iterations));
    defer allocator.free(read_times);

    const keys = try allocator.alloc(u64, @intCast(config.benchmark_iterations));
    defer allocator.free(keys);

    // Generate unique keys
    for (keys) |*key| {
        key.* = random.int(u64);
    }

    // Benchmark writes
    i = 0;
    while (i < config.benchmark_iterations and i < keys.len) : (i += 1) {
        const start_time = std.time.nanoTimestamp();
        db.write(keys[i], test_data) catch |err| switch (err) {
            error.TableFull => break,
            else => return err,
        };
        const end_time = std.time.nanoTimestamp();
        write_times[i] = @as(u64, @intCast(end_time - start_time));
    }
    const actual_writes = i;

    // Wait for WAL to catch up if enabled
    if (enable_wal) {
        std.time.sleep(50_000_000); // 50ms
    }

    // Benchmark reads
    i = 0;
    while (i < actual_writes) : (i += 1) {
        const start_time = std.time.nanoTimestamp();
        _ = try db.read(keys[i]);
        const end_time = std.time.nanoTimestamp();
        read_times[i] = @as(u64, @intCast(end_time - start_time));
    }

    // Calculate statistics
    const write_avg = calculateAverage(write_times[0..actual_writes]);
    const read_avg = calculateAverage(read_times[0..actual_writes]);

    // Batch operation benchmarks
    std.debug.print("Benchmarking batch operations...\n", .{});

    const batch_keys = try allocator.alloc(u64, config.batch_size);
    defer allocator.free(batch_keys);
    const batch_data = try allocator.alloc([]const u8, config.batch_size);
    defer allocator.free(batch_data);

    for (batch_keys) |*key| key.* = random.int(u64);
    for (batch_data) |*data| data.* = test_data;

    const batch_start = std.time.nanoTimestamp();
    try db.writeBatch(batch_keys, batch_data);
    const batch_end = std.time.nanoTimestamp();

    const batch_write_time = @as(f64, @floatFromInt(@as(u64, @intCast(batch_end - batch_start))));
    const batch_write_per_op = batch_write_time / @as(f64, @floatFromInt(config.batch_size));

    // Read batch benchmark
    const read_results = try allocator.alloc(?*const flat_hash_storage.CacheLineRecord, config.batch_size);
    defer allocator.free(read_results);

    const batch_read_start = std.time.nanoTimestamp();
    db.readBatch(batch_keys, read_results);
    const batch_read_end = std.time.nanoTimestamp();

    const batch_read_time = @as(f64, @floatFromInt(@as(u64, @intCast(batch_read_end - batch_read_start))));
    const batch_read_per_op = batch_read_time / @as(f64, @floatFromInt(config.batch_size));

    // Calculate throughput
    const total_time_sec = @as(f64, @floatFromInt(actual_writes)) * write_avg / 1_000_000_000.0;
    const throughput = @as(f64, @floatFromInt(actual_writes)) / total_time_sec;

    // Get final statistics
    const stats = db.getStats();
    const memory_usage = @as(f64, @floatFromInt(stats.total_capacity * @sizeOf(flat_hash_storage.CacheLineRecord))) / (1024.0 * 1024.0);

    return BenchmarkResults{
        .single_write_ns = write_avg,
        .single_read_ns = read_avg,
        .batch_write_ns_per_op = batch_write_per_op,
        .batch_read_ns_per_op = batch_read_per_op,
        .memory_usage_mb = memory_usage,
        .wal_overhead_ns = 0, // Calculated externally
        .recovery_time_ms = 0, // Set by recovery benchmark
        .throughput_ops_per_sec = throughput,
        .load_factor = stats.load_factor,
    };
}

fn runRecoveryBenchmark(allocator: std.mem.Allocator, _: BenchmarkConfig) !void {
    // Create database with data
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        var static_alloc = static_allocator.StaticAllocator.init(arena.allocator());

        var db = try wild.WILD.init(static_alloc.allocator(), wild.WILD.Config{ .target_capacity = 10_000 });
        defer {
            static_alloc.transitionToDeinit();
            db.deinit();
        }

        const wal_config = wal.DurabilityManager.Config{
            .wal_path = "/tmp/benchmark_wild.wal",
            .max_threads = 4,
            .ring_buffer_size = 2048,
            .batch_buffer_count = 8,
            .io_uring_entries = 32,
        };
        try db.enableDurability(wal_config);

        const snapshot_config = snapshot.SnapshotManager.Config{
            .snapshot_dir = "/tmp/benchmark_snapshots",
            .max_snapshots = 3,
            .snapshot_interval_seconds = 1,
        };
        try db.enableSnapshots(snapshot_config);

        static_alloc.transitionToStatic();

        // Fill database with test data
        std.debug.print("Creating test dataset for recovery benchmark...\n", .{});
        var i: u64 = 0;
        while (i < 5000) : (i += 1) {
            try db.write(i, "recovery_test_data_value");
        }

        // Wait for WAL
        std.time.sleep(20_000_000);

        // Create snapshot
        try db.createSnapshot();

        // Add more data after snapshot
        while (i < 7000) : (i += 1) {
            try db.write(i, "post_snapshot_data_value");
        }

        std.time.sleep(20_000_000); // Let WAL catch up
    }

    // Now benchmark recovery
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        var static_alloc = static_allocator.StaticAllocator.init(arena.allocator());

        var db = try wild.WILD.init(static_alloc.allocator(), wild.WILD.Config{ .target_capacity = 10_000 });
        defer {
            static_alloc.transitionToDeinit();
            db.deinit();
        }

        const snapshot_config = snapshot.SnapshotManager.Config{
            .snapshot_dir = "/tmp/benchmark_snapshots",
            .max_snapshots = 3,
            .snapshot_interval_seconds = 1,
        };
        try db.enableSnapshots(snapshot_config);

        static_alloc.transitionToStatic();

        std.debug.print("Benchmarking recovery time...\n", .{});
        const recovery_start = std.time.nanoTimestamp();
        try db.performRecovery("/tmp/benchmark_wild.wal");
        const recovery_end = std.time.nanoTimestamp();

        const recovery_time_ns = @as(u64, @intCast(recovery_end - recovery_start));
        const recovery_time_ms = @as(f64, @floatFromInt(recovery_time_ns)) / 1_000_000.0;

        const stats = db.getStats();
        std.debug.print("Recovery completed in {d:.2} ms\n", .{recovery_time_ms});
        std.debug.print("Recovered {} records ({d:.1}% load factor)\n", .{ stats.used_capacity, stats.load_factor * 100 });
    }
}

fn runLoadFactorBenchmark(allocator: std.mem.Allocator, _: BenchmarkConfig) !void {
    std.debug.print("Testing performance at different load factors...\n", .{});

    const load_factors = [_]f32{ 0.25, 0.5, 0.75, 0.9 };
    const test_data = "load_factor_test_data";

    for (load_factors) |target_load_factor| {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        var static_alloc = static_allocator.StaticAllocator.init(arena.allocator());

        var db = try wild.WILD.init(static_alloc.allocator(), wild.WILD.Config{ .target_capacity = 10_000 });
        defer {
            static_alloc.transitionToDeinit();
            db.deinit();
        }

        static_alloc.transitionToStatic();

        // Fill to target load factor
        const target_count = @as(u64, @intFromFloat(@as(f64, @floatFromInt(10_000)) * target_load_factor));
        var i: u64 = 0;
        while (i < target_count) : (i += 1) {
            try db.write(i, test_data);
        }

        // Benchmark at this load factor
        const times = try allocator.alloc(u64, 1000);
        defer allocator.free(times);

        var prng = std.Random.DefaultPrng.init(@as(u64, @intCast(std.time.nanoTimestamp())));
        const random = prng.random();

        for (times) |*time| {
            const key = random.intRangeAtMost(u64, 0, target_count - 1);
            const start = std.time.nanoTimestamp();
            _ = try db.read(key);
            const end = std.time.nanoTimestamp();
            time.* = @as(u64, @intCast(end - start));
        }

        const avg_time = calculateAverage(times);
        const stats = db.getStats();

        std.debug.print("Load Factor {d:.1}%: {d:.1} ns avg read time ({} records)\n", .{ stats.load_factor * 100, avg_time, stats.used_capacity });
    }
}

fn calculateAverage(times: []const u64) f64 {
    if (times.len == 0) return 0;

    var sum: u64 = 0;
    for (times) |time| {
        sum += time;
    }

    return @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(times.len));
}

fn printResults(label: []const u8, results: BenchmarkResults) void {
    std.debug.print("\n{s} Results:\n", .{label});
    std.debug.print("  Single Write:     {d:.1} ns\n", .{results.single_write_ns});
    std.debug.print("  Single Read:      {d:.1} ns\n", .{results.single_read_ns});
    std.debug.print("  Batch Write:      {d:.1} ns/op\n", .{results.batch_write_ns_per_op});
    std.debug.print("  Batch Read:       {d:.1} ns/op\n", .{results.batch_read_ns_per_op});
    std.debug.print("  Throughput:       {d:.0} ops/sec\n", .{results.throughput_ops_per_sec});
    std.debug.print("  Memory Usage:     {d:.1} MB\n", .{results.memory_usage_mb});
    std.debug.print("  Load Factor:      {d:.1}%\n", .{results.load_factor * 100});
}
