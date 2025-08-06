const std = @import("std");
const wild = @import("wild.zig");

// Performance metrics collection and monitoring for WILD database
// Provides real-time performance tracking without impacting hot path

pub const MetricsCollector = struct {
    const Self = @This();

    // Core operation metrics
    write_count: std.atomic.Value(u64),
    read_count: std.atomic.Value(u64),
    delete_count: std.atomic.Value(u64),

    // Timing metrics (nanoseconds) - using lock-free circular buffers
    write_times: CircularBuffer,
    read_times: CircularBuffer,

    // Error tracking
    table_full_count: std.atomic.Value(u64),
    key_not_found_count: std.atomic.Value(u64),

    // Cache performance metrics
    cache_hit_count: std.atomic.Value(u64),
    cache_miss_count: std.atomic.Value(u64),

    // System resource metrics
    memory_usage_bytes: std.atomic.Value(u64),
    load_factor_x1000: std.atomic.Value(u32), // Load factor * 1000 for precision

    // Performance snapshot for reporting
    last_snapshot_time: std.atomic.Value(u64),
    snapshot_interval_ns: u64,

    allocator: std.mem.Allocator,

    pub const Config = struct {
        buffer_size: u32 = 1000, // Number of recent timings to track
        snapshot_interval_seconds: u64 = 60, // How often to take performance snapshots
    };

    pub const PerformanceSnapshot = struct {
        // Counts
        total_writes: u64,
        total_reads: u64,
        total_deletes: u64,
        total_errors: u64,

        // Performance (nanoseconds)
        avg_write_latency_ns: f64,
        avg_read_latency_ns: f64,
        min_write_latency_ns: u64,
        max_write_latency_ns: u64,
        p99_write_latency_ns: u64,

        // Throughput (operations per second)
        writes_per_sec: f64,
        reads_per_sec: f64,
        total_ops_per_sec: f64,

        // Resource usage
        memory_usage_mb: f64,
        load_factor: f32,
        cache_hit_rate: f32,

        // Time range
        measurement_duration_sec: f64,
        timestamp: u64,
    };

    pub fn init(allocator: std.mem.Allocator, config: Config) !Self {
        return Self{
            .write_count = std.atomic.Value(u64).init(0),
            .read_count = std.atomic.Value(u64).init(0),
            .delete_count = std.atomic.Value(u64).init(0),

            .write_times = try CircularBuffer.init(allocator, config.buffer_size),
            .read_times = try CircularBuffer.init(allocator, config.buffer_size),

            .table_full_count = std.atomic.Value(u64).init(0),
            .key_not_found_count = std.atomic.Value(u64).init(0),

            .cache_hit_count = std.atomic.Value(u64).init(0),
            .cache_miss_count = std.atomic.Value(u64).init(0),

            .memory_usage_bytes = std.atomic.Value(u64).init(0),
            .load_factor_x1000 = std.atomic.Value(u32).init(0),

            .last_snapshot_time = std.atomic.Value(u64).init(@as(u64, @intCast(std.time.nanoTimestamp()))),
            .snapshot_interval_ns = config.snapshot_interval_seconds * 1_000_000_000,

            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.write_times.deinit();
        self.read_times.deinit();
    }

    // Hot path metrics - designed for minimal overhead
    pub inline fn recordWrite(self: *Self, latency_ns: u64) void {
        _ = self.write_count.fetchAdd(1, .monotonic);
        self.write_times.add(latency_ns);
    }

    pub inline fn recordRead(self: *Self, latency_ns: u64, hit: bool) void {
        _ = self.read_count.fetchAdd(1, .monotonic);
        self.read_times.add(latency_ns);

        if (hit) {
            _ = self.cache_hit_count.fetchAdd(1, .monotonic);
        } else {
            _ = self.cache_miss_count.fetchAdd(1, .monotonic);
        }
    }

    pub inline fn recordDelete(self: *Self) void {
        _ = self.delete_count.fetchAdd(1, .monotonic);
    }

    pub inline fn recordError(self: *Self, error_type: ErrorType) void {
        switch (error_type) {
            .TableFull => _ = self.table_full_count.fetchAdd(1, .monotonic),
            .KeyNotFound => _ = self.key_not_found_count.fetchAdd(1, .monotonic),
        }
    }

    // Update system metrics (called periodically, not on hot path)
    pub fn updateSystemMetrics(self: *Self, db_stats: wild.WILD.Stats) void {
        const memory_bytes = @as(u64, db_stats.total_capacity) * 64; // 64 bytes per record
        self.memory_usage_bytes.store(memory_bytes, .monotonic);

        const load_factor_scaled = @as(u32, @intFromFloat(db_stats.load_factor * 1000.0));
        self.load_factor_x1000.store(load_factor_scaled, .monotonic);
    }

    // Check if it's time to take a performance snapshot
    pub fn shouldTakeSnapshot(self: *const Self) bool {
        const now = @as(u64, @intCast(std.time.nanoTimestamp()));
        const last_snapshot = self.last_snapshot_time.load(.monotonic);
        return (now - last_snapshot) >= self.snapshot_interval_ns;
    }

    // Take a performance snapshot (for reporting/monitoring)
    pub fn takeSnapshot(self: *Self) PerformanceSnapshot {
        const now = @as(u64, @intCast(std.time.nanoTimestamp()));
        const last_snapshot = self.last_snapshot_time.swap(now, .monotonic);
        const duration_ns = now - last_snapshot;
        const duration_sec = @as(f64, @floatFromInt(duration_ns)) / 1_000_000_000.0;

        // Get current counts
        const writes = self.write_count.load(.monotonic);
        const reads = self.read_count.load(.monotonic);
        const deletes = self.delete_count.load(.monotonic);
        const table_full_errors = self.table_full_count.load(.monotonic);
        const key_not_found_errors = self.key_not_found_count.load(.monotonic);

        // Calculate cache hit rate
        const cache_hits = self.cache_hit_count.load(.monotonic);
        const cache_misses = self.cache_miss_count.load(.monotonic);
        const total_cache_ops = cache_hits + cache_misses;
        const cache_hit_rate = if (total_cache_ops > 0)
            @as(f32, @floatFromInt(cache_hits)) / @as(f32, @floatFromInt(total_cache_ops))
        else
            0.0;

        // Analyze timing data
        const write_stats = self.write_times.analyze();
        const read_stats = self.read_times.analyze();

        // Calculate throughput
        const writes_per_sec = if (duration_sec > 0) @as(f64, @floatFromInt(writes)) / duration_sec else 0;
        const reads_per_sec = if (duration_sec > 0) @as(f64, @floatFromInt(reads)) / duration_sec else 0;
        const total_ops_per_sec = writes_per_sec + reads_per_sec;

        // Get resource metrics
        const memory_bytes = self.memory_usage_bytes.load(.monotonic);
        const memory_mb = @as(f64, @floatFromInt(memory_bytes)) / (1024.0 * 1024.0);
        const load_factor = @as(f32, @floatFromInt(self.load_factor_x1000.load(.monotonic))) / 1000.0;

        return PerformanceSnapshot{
            .total_writes = writes,
            .total_reads = reads,
            .total_deletes = deletes,
            .total_errors = table_full_errors + key_not_found_errors,

            .avg_write_latency_ns = write_stats.average,
            .avg_read_latency_ns = read_stats.average,
            .min_write_latency_ns = write_stats.minimum,
            .max_write_latency_ns = write_stats.maximum,
            .p99_write_latency_ns = write_stats.p99,

            .writes_per_sec = writes_per_sec,
            .reads_per_sec = reads_per_sec,
            .total_ops_per_sec = total_ops_per_sec,

            .memory_usage_mb = memory_mb,
            .load_factor = load_factor,
            .cache_hit_rate = cache_hit_rate,

            .measurement_duration_sec = duration_sec,
            .timestamp = now,
        };
    }

    // Print human-readable performance report
    pub fn printReport(snapshot: PerformanceSnapshot) void {
        const timestamp = @as(i64, @intCast(snapshot.timestamp));
        std.debug.print("\n=== WILD Performance Report ===\n", .{});
        std.debug.print("Timestamp: {} (duration: {d:.1}s)\n", .{ timestamp, snapshot.measurement_duration_sec });

        std.debug.print("\nOperation Counts:\n", .{});
        std.debug.print("  Writes:       {}\n", .{snapshot.total_writes});
        std.debug.print("  Reads:        {}\n", .{snapshot.total_reads});
        std.debug.print("  Deletes:      {}\n", .{snapshot.total_deletes});
        std.debug.print("  Errors:       {}\n", .{snapshot.total_errors});

        std.debug.print("\nLatency (nanoseconds):\n", .{});
        std.debug.print("  Avg Write:    {d:.1} ns\n", .{snapshot.avg_write_latency_ns});
        std.debug.print("  Avg Read:     {d:.1} ns\n", .{snapshot.avg_read_latency_ns});
        std.debug.print("  Min Write:    {} ns\n", .{snapshot.min_write_latency_ns});
        std.debug.print("  Max Write:    {} ns\n", .{snapshot.max_write_latency_ns});
        std.debug.print("  P99 Write:    {} ns\n", .{snapshot.p99_write_latency_ns});

        std.debug.print("\nThroughput:\n", .{});
        std.debug.print("  Writes/sec:   {d:.0}\n", .{snapshot.writes_per_sec});
        std.debug.print("  Reads/sec:    {d:.0}\n", .{snapshot.reads_per_sec});
        std.debug.print("  Total ops/sec:{d:.0}\n", .{snapshot.total_ops_per_sec});

        std.debug.print("\nResource Usage:\n", .{});
        std.debug.print("  Memory:       {d:.1} MB\n", .{snapshot.memory_usage_mb});
        std.debug.print("  Load Factor:  {d:.1}%\n", .{snapshot.load_factor * 100});
        std.debug.print("  Cache Hit:    {d:.1}%\n", .{snapshot.cache_hit_rate * 100});

        // Performance assessment
        std.debug.print("\nPerformance Assessment:\n", .{});
        if (snapshot.avg_write_latency_ns <= 42.0) {
            std.debug.print("  Write Latency: ✅ EXCELLENT (≤42ns target)\n", .{});
        } else if (snapshot.avg_write_latency_ns <= 100.0) {
            std.debug.print("  Write Latency: ✅ GOOD (≤100ns)\n", .{});
        } else {
            std.debug.print("  Write Latency: ⚠️ DEGRADED (>100ns)\n", .{});
        }

        if (snapshot.cache_hit_rate >= 0.90) {
            std.debug.print("  Cache Efficiency: ✅ EXCELLENT (≥90%)\n", .{});
        } else if (snapshot.cache_hit_rate >= 0.70) {
            std.debug.print("  Cache Efficiency: ✅ GOOD (≥70%)\n", .{});
        } else {
            std.debug.print("  Cache Efficiency: ⚠️ LOW (<70%)\n", .{});
        }

        std.debug.print("================================\n", .{});
    }

    pub const ErrorType = enum {
        TableFull,
        KeyNotFound,
    };
};

// Lock-free circular buffer for tracking recent latency measurements
const CircularBuffer = struct {
    const Self = @This();

    buffer: []u64,
    write_index: std.atomic.Value(u32),
    size: u32,
    allocator: std.mem.Allocator,

    const Stats = struct {
        average: f64,
        minimum: u64,
        maximum: u64,
        p99: u64,
        count: u32,
    };

    pub fn init(allocator: std.mem.Allocator, size: u32) !Self {
        const buffer = try allocator.alloc(u64, size);
        @memset(buffer, 0);

        return Self{
            .buffer = buffer,
            .write_index = std.atomic.Value(u32).init(0),
            .size = size,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.buffer);
    }

    // Add a new measurement (lock-free)
    pub inline fn add(self: *Self, value: u64) void {
        const index = self.write_index.fetchAdd(1, .monotonic) % self.size;
        self.buffer[index] = value;
    }

    // Analyze current buffer contents (snapshot-based, safe for concurrent access)
    pub fn analyze(self: *const Self) Stats {
        // Create a snapshot of current buffer state
        var snapshot: [1000]u64 = undefined; // Max buffer size
        const actual_size = @min(self.size, 1000);

        // Copy current buffer contents (may have some inconsistency but that's ok for metrics)
        for (0..actual_size) |i| {
            snapshot[i] = self.buffer[i];
        }

        // Filter out uninitialized values (zeros)
        var valid_count: u32 = 0;
        for (0..actual_size) |i| {
            if (snapshot[i] > 0) {
                snapshot[valid_count] = snapshot[i];
                valid_count += 1;
            }
        }

        if (valid_count == 0) {
            return Stats{
                .average = 0,
                .minimum = 0,
                .maximum = 0,
                .p99 = 0,
                .count = 0,
            };
        }

        // Sort for percentiles
        std.mem.sort(u64, snapshot[0..valid_count], {}, std.sort.asc(u64));

        // Calculate statistics
        var sum: u64 = 0;
        for (snapshot[0..valid_count]) |value| {
            sum += value;
        }

        const average = @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(valid_count));
        const minimum = snapshot[0];
        const maximum = snapshot[valid_count - 1];

        // P99 calculation
        const p99_index = (valid_count * 99) / 100;
        const p99 = if (p99_index < valid_count) snapshot[p99_index] else maximum;

        return Stats{
            .average = average,
            .minimum = minimum,
            .maximum = maximum,
            .p99 = p99,
            .count = valid_count,
        };
    }
};
