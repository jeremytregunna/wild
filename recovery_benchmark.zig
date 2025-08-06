const std = @import("std");
const wild = @import("src/wild.zig");
const wal = @import("src/wal.zig");
const snapshot = @import("src/snapshot.zig");
const static_allocator = @import("src/static_allocator.zig");

pub fn main() !void {
    std.debug.print("=== WILD Recovery Performance Benchmark ===\n", .{});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Clean up test files
    cleanup() catch {};

    // Benchmark 1: WAL-only recovery
    try benchmarkWALOnlyRecovery(allocator);

    // Benchmark 2: Snapshot-only recovery
    try benchmarkSnapshotOnlyRecovery(allocator);

    // Benchmark 3: Snapshot + WAL recovery
    try benchmarkSnapshotPlusWALRecovery(allocator);

    // Benchmark 4: WAL write verification
    try benchmarkWALWriteVerification(allocator);

    std.debug.print("\n=== Recovery Benchmark Complete ===\n", .{});
}

fn cleanup() !void {
    const paths = [_][]const u8{
        "/tmp/recovery_test.wal",
        "/tmp/recovery_snapshots",
    };

    for (paths) |path| {
        std.fs.cwd().deleteFile(path) catch {};
        std.fs.cwd().deleteTree(path) catch {};
    }
}

fn benchmarkWALOnlyRecovery(allocator: std.mem.Allocator) !void {
    std.debug.print("\n--- Benchmark 1: WAL-Only Recovery ---\n", .{});

    // First, create a database with WAL data
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
            .wal_path = "/tmp/recovery_test.wal",
            .max_threads = 4,
            .ring_buffer_size = 2048,
            .batch_buffer_count = 8,
            .io_uring_entries = 32,
        };
        try db.enableDurability(wal_config);

        static_alloc.transitionToStatic();

        std.debug.print("Creating test data with WAL...\n", .{});

        // Fill database with test data
        var i: u64 = 0;
        while (i < 5000) : (i += 1) {
            try db.write(i, "wal_only_recovery_test_data");
        }

        // Wait for WAL to flush
        std.time.sleep(100_000_000); // 100ms

        if (db.getDurabilityStatsNoAlloc()) |dur_stats| {
            std.debug.print("WAL Status: {} batches, {} entries, {d:.1} MB\n", .{ dur_stats.batches_written, dur_stats.total_entries_written, @as(f64, @floatFromInt(dur_stats.wal_size_bytes)) / (1024.0 * 1024.0) });
        }
    }

    // Check if WAL file exists
    const wal_file = std.fs.cwd().openFile("/tmp/recovery_test.wal", .{}) catch |err| {
        std.debug.print("❌ WAL file not found: {}\n", .{err});
        return;
    };
    defer wal_file.close();

    const wal_size = try wal_file.getEndPos();
    std.debug.print("WAL file size: {} bytes\n", .{wal_size});

    // Now benchmark recovery from WAL only
    const recovery_times = [_]struct { name: []const u8, capacity: u64 }{
        .{ .name = "10K capacity", .capacity = 10_000 },
        .{ .name = "25K capacity", .capacity = 25_000 },
        .{ .name = "50K capacity", .capacity = 50_000 },
    };

    for (recovery_times) |scenario| {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        var static_alloc = static_allocator.StaticAllocator.init(arena.allocator());

        var db = try wild.WILD.init(static_alloc.allocator(), wild.WILD.Config{ .target_capacity = scenario.capacity });
        defer {
            static_alloc.transitionToDeinit();
            db.deinit();
        }

        const snapshot_config = snapshot.SnapshotManager.Config{
            .snapshot_dir = "/tmp/empty_snapshots", // Empty directory
            .max_snapshots = 5,
            .snapshot_interval_seconds = 300,
        };
        try db.enableSnapshots(snapshot_config);

        static_alloc.transitionToStatic();

        const recovery_start = std.time.nanoTimestamp();
        try db.performRecovery("/tmp/recovery_test.wal");
        const recovery_end = std.time.nanoTimestamp();

        const recovery_time_ms = @as(f64, @floatFromInt(@as(u64, @intCast(recovery_end - recovery_start)))) / 1_000_000.0;
        const stats = db.getStats();

        std.debug.print("  {s}: {d:.1} ms ({} records recovered)\n", .{ scenario.name, recovery_time_ms, stats.used_capacity });
    }
}

fn benchmarkSnapshotOnlyRecovery(allocator: std.mem.Allocator) !void {
    std.debug.print("\n--- Benchmark 2: Snapshot-Only Recovery ---\n", .{});

    // Create database with snapshot
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        var static_alloc = static_allocator.StaticAllocator.init(arena.allocator());

        var db = try wild.WILD.init(static_alloc.allocator(), wild.WILD.Config{ .target_capacity = 25_000 });
        defer {
            static_alloc.transitionToDeinit();
            db.deinit();
        }

        const snapshot_config = snapshot.SnapshotManager.Config{
            .snapshot_dir = "/tmp/recovery_snapshots",
            .max_snapshots = 5,
            .snapshot_interval_seconds = 300,
        };
        try db.enableSnapshots(snapshot_config);

        static_alloc.transitionToStatic();

        std.debug.print("Creating snapshot with test data...\n", .{});

        // Fill database
        var i: u64 = 0;
        while (i < 10_000) : (i += 1) {
            try db.write(i, "snapshot_recovery_test_data");
        }

        // Create snapshot
        try db.createSnapshot();
    }

    // Now benchmark snapshot-only recovery
    const snapshot_sizes = [_]u64{ 5_000, 10_000, 15_000, 25_000 };

    for (snapshot_sizes) |capacity| {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        var static_alloc = static_allocator.StaticAllocator.init(arena.allocator());

        var db = try wild.WILD.init(static_alloc.allocator(), wild.WILD.Config{ .target_capacity = capacity });
        defer {
            static_alloc.transitionToDeinit();
            db.deinit();
        }

        const snapshot_config = snapshot.SnapshotManager.Config{
            .snapshot_dir = "/tmp/recovery_snapshots",
            .max_snapshots = 5,
            .snapshot_interval_seconds = 300,
        };
        try db.enableSnapshots(snapshot_config);

        static_alloc.transitionToStatic();

        const recovery_start = std.time.nanoTimestamp();
        db.performRecovery("/tmp/nonexistent.wal") catch {}; // WAL doesn't exist, only snapshot
        const recovery_end = std.time.nanoTimestamp();

        const recovery_time_ms = @as(f64, @floatFromInt(@as(u64, @intCast(recovery_end - recovery_start)))) / 1_000_000.0;
        const stats = db.getStats();

        std.debug.print("  {}K capacity: {d:.1} ms ({} records recovered)\n", .{ capacity / 1000, recovery_time_ms, stats.used_capacity });
    }
}

fn benchmarkSnapshotPlusWALRecovery(allocator: std.mem.Allocator) !void {
    std.debug.print("\n--- Benchmark 3: Snapshot + WAL Recovery ---\n", .{});

    // Create database with snapshot + additional WAL data
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        var static_alloc = static_allocator.StaticAllocator.init(arena.allocator());

        var db = try wild.WILD.init(static_alloc.allocator(), wild.WILD.Config{ .target_capacity = 20_000 });
        defer {
            static_alloc.transitionToDeinit();
            db.deinit();
        }

        const wal_config = wal.DurabilityManager.Config{
            .wal_path = "/tmp/recovery_test.wal",
            .max_threads = 4,
            .ring_buffer_size = 2048,
            .batch_buffer_count = 8,
            .io_uring_entries = 32,
        };
        try db.enableDurability(wal_config);

        const snapshot_config = snapshot.SnapshotManager.Config{
            .snapshot_dir = "/tmp/recovery_snapshots",
            .max_snapshots = 5,
            .snapshot_interval_seconds = 300,
        };
        try db.enableSnapshots(snapshot_config);

        static_alloc.transitionToStatic();

        std.debug.print("Creating snapshot + WAL data scenario...\n", .{});

        // Fill database up to snapshot point
        var i: u64 = 0;
        while (i < 8_000) : (i += 1) {
            try db.write(i, "pre_snapshot_data");
        }

        // Create snapshot
        try db.createSnapshot();

        // Add more data after snapshot (will be in WAL)
        while (i < 12_000) : (i += 1) {
            try db.write(i, "post_snapshot_wal_data");
        }

        // Wait for WAL
        std.time.sleep(100_000_000);
    }

    // Benchmark combined recovery
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    var static_alloc = static_allocator.StaticAllocator.init(arena.allocator());

    var db = try wild.WILD.init(static_alloc.allocator(), wild.WILD.Config{ .target_capacity = 20_000 });
    defer {
        static_alloc.transitionToDeinit();
        db.deinit();
    }

    const snapshot_config = snapshot.SnapshotManager.Config{
        .snapshot_dir = "/tmp/recovery_snapshots",
        .max_snapshots = 5,
        .snapshot_interval_seconds = 300,
    };
    try db.enableSnapshots(snapshot_config);

    static_alloc.transitionToStatic();

    const recovery_start = std.time.nanoTimestamp();
    try db.performRecovery("/tmp/recovery_test.wal");
    const recovery_end = std.time.nanoTimestamp();

    const recovery_time_ms = @as(f64, @floatFromInt(@as(u64, @intCast(recovery_end - recovery_start)))) / 1_000_000.0;
    const stats = db.getStats();

    std.debug.print("  Combined Recovery: {d:.1} ms ({} records total)\n", .{ recovery_time_ms, stats.used_capacity });
}

fn benchmarkWALWriteVerification(allocator: std.mem.Allocator) !void {
    std.debug.print("\n--- Benchmark 4: WAL Write Verification ---\n", .{});

    std.debug.print("Testing WAL write functionality...\n", .{});

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    var static_alloc = static_allocator.StaticAllocator.init(arena.allocator());

    var db = try wild.WILD.init(static_alloc.allocator(), wild.WILD.Config{ .target_capacity = 5_000 });
    defer {
        static_alloc.transitionToDeinit();
        db.deinit();
    }

    const wal_config = wal.DurabilityManager.Config{
        .wal_path = "/tmp/wal_verification.wal",
        .max_threads = 2,
        .ring_buffer_size = 512,
        .batch_buffer_count = 4,
        .io_uring_entries = 16,
    };
    try db.enableDurability(wal_config);

    static_alloc.transitionToStatic();

    // Write some test data
    std.debug.print("Writing test data to trigger WAL writes...\n", .{});

    var i: u64 = 0;
    while (i < 1000) : (i += 1) {
        try db.write(i, "wal_verification_test_data_payload");

        // Check stats every 100 writes
        if (i % 100 == 0) {
            if (db.getDurabilityStatsNoAlloc()) |stats| {
                std.debug.print("  After {} writes: {} batches, {} entries, {} dropped\n", .{ i + 1, stats.batches_written, stats.total_entries_written, stats.entries_dropped });
            }
        }
    }

    // Final wait and stats
    std.debug.print("Waiting for final WAL flush...\n", .{});
    std.time.sleep(200_000_000); // 200ms

    if (db.getDurabilityStatsNoAlloc()) |final_stats| {
        std.debug.print("\nFinal WAL Stats:\n", .{});
        std.debug.print("  Batches Written:     {}\n", .{final_stats.batches_written});
        std.debug.print("  Entries Written:     {}\n", .{final_stats.total_entries_written});
        std.debug.print("  Entries Dropped:     {}\n", .{final_stats.entries_dropped});
        std.debug.print("  WAL Size:            {d:.2} KB\n", .{@as(f64, @floatFromInt(final_stats.wal_size_bytes)) / 1024.0});
        std.debug.print("  I/O Errors:          {}\n", .{final_stats.io_errors});
        std.debug.print("  Completed I/Os:      {}\n", .{final_stats.completed_ios});
    }

    // Check if file actually exists on disk
    const wal_file = std.fs.cwd().openFile("/tmp/wal_verification.wal", .{}) catch |err| {
        std.debug.print("❌ WAL file verification failed: {}\n", .{err});
        return;
    };
    defer wal_file.close();

    const actual_size = try wal_file.getEndPos();
    std.debug.print("✓ WAL file on disk: {} bytes\n", .{actual_size});

    // Read first few bytes to verify content
    var buffer: [64]u8 = undefined;
    try wal_file.seekTo(0);
    const bytes_read = try wal_file.readAll(&buffer);
    std.debug.print("✓ WAL file readable: {} bytes read\n", .{bytes_read});
}
