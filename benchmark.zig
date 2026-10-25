const std = @import("std");
const os_compat = @import("src/os_compat.zig");
const wild = @import("src/wild.zig");
const flat_hash_storage = @import("src/flat_hash_storage.zig");
const static_allocator = @import("src/static_allocator.zig");
const cache_topology = @import("src/cache_topology.zig");
const bench_counters = @import("bench_counters.zig");

// WILD Database Benchmark Suite
//
// The point of this program is to answer one question honestly: does a change
// make the lookup path faster, and where does the time go. Two things used to
// make that impossible, and both are fixed here:
//
//   - Capacity was always derived from L3, so the table never left L3 and a
//     cache-miss optimisation could not show any effect.
//   - The batch and mixed phases re-read the same small key set, which stays
//     resident in L1 or L2. Those phases now sweep the whole key space.
//
// Read phases use a fixed PRNG seed, so two runs are comparable.

/// One record is one cache line: see `CacheLineRecord`.
const record_bytes = 64;

/// How big to make the table relative to L3.
const SizeClass = enum {
    l3_resident,
    dram_resident,

    fn multiplier(self: SizeClass) u64 {
        return switch (self) {
            .l3_resident => 1,
            .dram_resident => 4, // 4x L3, so a probe is a real memory access
        };
    }

    fn label(self: SizeClass) []const u8 {
        return switch (self) {
            .l3_resident => "L3-resident",
            .dram_resident => "DRAM-resident",
        };
    }
};

const Mode = enum { all, write, read, random, batch, pipe, sweep, probes, mixed };

const Options = struct {
    capacity: ?u64 = null,
    keys: ?u64 = null,
    load_factor: f64 = 0.6,
    mode: Mode = .all,
    compare: bool = false,
    counters: bool = true,
    populate: bool = true,
    micro: usize = wild.WILD.default_microbatch,
    shuffle: bool = false,
    mixed_seconds: i128 = 2,
    seed: u64 = 0x9E3779B97F4A7C15,
};

const usage =
    \\benchmark [options]
    \\
    \\  --capacity N       force the table capacity in records (default: from L3)
    \\  --keys N           operations per phase (default: load factor x capacity)
    \\  --load-factor F    fill fraction used to size the key set, 0.05 to 0.95
    \\                     (default 0.6)
    \\  --mode M           all|write|read|random|batch|pipe|sweep|probes|mixed
    \\                     (default all). probes reports the probe-chain length
    \\                     distribution, which says what fill fraction costs.
    \\  --micro N          keys per prefetch group for pipe mode: 1, 4, 8, 16,
    \\                     32, 64, 128, 256 (default: WILD's measured value).
    \\                     sweep mode runs all of them and prints a row per size.
    \\  --shuffle          read the key set in random order, so no phase can
    \\                     benefit from the order it was written in
    \\  --compare          run the suite twice, at L3-resident and 4x L3, then
    \\                     print a comparison table
    \\  --no-counters      skip the IPC and cache-counter lines
    \\  --no-populate      do not write the key set before a read phase
    \\  --seed N           PRNG seed for the random-read phase
    \\  --help             this text
    \\
    \\There is no write-ahead log in this code path. Wild.init leaves durability
    \\unset and the benchmark never calls enableDurability, so write numbers are
    \\table writes only. The daemon path is the one that has a WAL.
    \\
    \\Counters come from perf_event_open. Counting excludes kernel time, which is
    \\what perf_event_paranoid=2 allows. On CPUs where the kernel does not map the
    \\generic LLC event, the miss count is reported as unavailable and IPC is the
    \\usable signal: a memory-bound loop sits near 1, a compute-bound one above 2.
    \\
;

/// Command-line reader. Value flags consume the following item from the same
/// cursor, so a missing value is reported against the flag that wanted it.
const ArgParser = struct {
    argv: []const []const u8,
    i: usize,

    fn next(self: *ArgParser) ?[]const u8 {
        if (self.i >= self.argv.len) return null;
        defer self.i += 1;
        return self.argv[self.i];
    }
};

pub fn main(init: std.process.Init) !void {
    // One arena for everything the benchmark itself needs: arguments, key
    // arrays, results. The database gets its own static allocator below.
    var main_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer main_arena.deinit();
    const alloc = main_arena.allocator();

    var opts = Options{};
    const argv = try init.minimal.args.toSlice(alloc);
    var args = ArgParser{ .argv = argv, .i = 1 };
    while (args.next()) |arg| {
        if (eq(arg, "--help") or eq(arg, "-h")) {
            try std.Io.File.stdout().writeStreamingAll(init.io, usage);
            return;
        } else if (eq(arg, "--compare")) {
            opts.compare = true;
        } else if (eq(arg, "--no-counters")) {
            opts.counters = false;
        } else if (eq(arg, "--no-populate")) {
            opts.populate = false;
        } else if (eq(arg, "--micro")) {
            opts.micro = try parseU64(&args, arg);
        } else if (eq(arg, "--shuffle")) {
            opts.shuffle = true;
        } else if (eq(arg, "--capacity")) {
            opts.capacity = try parseU64(&args, arg);
        } else if (eq(arg, "--keys")) {
            opts.keys = try parseU64(&args, arg);
        } else if (eq(arg, "--seed")) {
            opts.seed = try parseU64(&args, arg);
        } else if (eq(arg, "--mixed-seconds")) {
            opts.mixed_seconds = try parseU64(&args, arg);
        } else if (eq(arg, "--load-factor")) {
            opts.load_factor = try std.fmt.parseFloat(f64, try nextValue(&args, arg));
            if (!(opts.load_factor > 0.05 and opts.load_factor <= 0.95)) {
                std.debug.print("bad --load-factor {d}: use 0.05 to 0.95\n", .{opts.load_factor});
                return error.BadLoadFactor;
            }
        } else if (eq(arg, "--mode")) {
            const mode = try nextValue(&args, arg);
            opts.mode = std.meta.stringToEnum(Mode, mode) orelse {
                std.debug.print("bad --mode {s}\n", .{mode});
                return error.BadMode;
            };
        } else {
            std.debug.print("unknown argument: {s}\n\n", .{arg});
            try std.Io.File.stdout().writeStreamingAll(init.io, usage);
            return error.BadArgument;
        }
    }

    // Cache topology drives the default sizes, so detect it once.
    const topology = try cache_topology.analyzeCacheTopology(alloc);
    var l3_kb: u64 = 0;
    for (topology.l3_domains) |domain| l3_kb += domain.cache_size_kb;
    const l3_bytes = l3_kb * 1024;

    std.debug.print("WILD Database Benchmark\n", .{});
    std.debug.print("L3 {d} MB · record {d} B · load factor {d:.2}\n\n", .{
        l3_bytes / (1024 * 1024),
        record_bytes,
        opts.load_factor,
    });

    var results = std.ArrayList(Result).empty;
    defer results.deinit(alloc);

    if (opts.compare) {
        try runSuite(alloc, &results, opts, l3_bytes, .l3_resident);
        try runSuite(alloc, &results, opts, l3_bytes, .dram_resident);
        printComparison(results.items);
    } else {
        try runSuite(alloc, &results, opts, l3_bytes, .l3_resident);
    }
}

fn eq(a: []const u8, b: []const u8) bool {
    return std.mem.eql(u8, a, b);
}

fn nextValue(iter: *ArgParser, flag: []const u8) ![]const u8 {
    return iter.next() orelse {
        std.debug.print("{s} needs a value\n", .{flag});
        return error.MissingValue;
    };
}

fn parseU64(iter: *ArgParser, flag: []const u8) !u64 {
    const raw = try nextValue(iter, flag);
    return std.fmt.parseInt(u64, raw, 10) catch {
        std.debug.print("{s}: expected a number, got {s}\n", .{ flag, raw });
        return error.BadValue;
    };
}

/// One measured phase.
const Result = struct {
    size: SizeClass,
    phase: []const u8,
    ops: u64,
    ns_per_op: f64,
    ipc: ?f64,
    misses_per_kilo_op: ?f64,
    miss_source: ?[]const u8,
};

fn wanted(opts: Options, mode: Mode) bool {
    // The sweep is only ever asked for. Leaving it in `all` would run every
    // group size on every default run and bury the rows that matter.
    if (mode == .sweep) return opts.mode == .sweep;
    return opts.mode == .all or opts.mode == mode;
}

fn runSuite(
    allocator: std.mem.Allocator,
    results: *std.ArrayList(Result),
    opts: Options,
    l3_bytes: u64,
    size: SizeClass,
) !void {
    std.debug.print("── {s} ──\n", .{size.label()});

    // Database-owned memory is static after the transition, so it gets its own
    // arena. The benchmark's own scratch memory is freed with the arena below.
    var database_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer database_arena.deinit();
    var static_alloc = static_allocator.StaticAllocator.init(database_arena.allocator());

    var bench_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer bench_arena.deinit();
    const scratch = bench_arena.allocator();

    const capacity = opts.capacity orelse capacityFor(l3_bytes, size);
    const config = wild.WILD.Config{ .target_capacity = capacity };
    var database = try wild.WILD.init(static_alloc.allocator(), config);
    // Freeze the database allocator. Any allocation from here on panics, which
    // is the point: it is the only thing that proves the hot path does not
    // allocate. Skipping this line silently removes that guarantee.
    static_alloc.transitionToStatic();
    defer {
        static_alloc.transitionToDeinit();
        database.deinit();
        static_alloc.deinit();
    }

    const total_capacity = database.getStats().total_capacity;
    const key_count: u64 = opts.keys orelse @as(u64, @intFromFloat(@as(f64, @floatFromInt(total_capacity)) * opts.load_factor));
    std.debug.print("capacity {d} records ({d} MB table) · {d} keys\n", .{
        total_capacity,
        @as(u64, @intCast(@as(u64, @intCast(total_capacity)) * record_bytes / (1024 * 1024))),
        key_count,
    });
    if (key_count >= total_capacity) {
        std.debug.print("key count {d} must stay below capacity {d}\n", .{ key_count, total_capacity });
        return error.TooManyKeys;
    }

    const keys = try scratch.alloc(u64, key_count);
    for (keys, 0..) |*key, i| key.* = @as(u64, i) + 1;
    if (opts.shuffle) {
        // Same seed every run, so a before/after comparison changes one thing.
        var rng = std.Random.Xoshiro256.init(opts.seed);
        var i: usize = keys.len;
        while (i > 1) {
            i -= 1;
            const j = @as(usize, @intCast(rng.next() % @as(u64, i + 1)));
            std.mem.swap(u64, &keys[i], &keys[j]);
        }
        std.debug.print("key order: shuffled (seed 0x{x})\n", .{opts.seed});
    }

    // Values come from a small pool. Distinct values cost one allocation and
    // about 40 bytes per key, which for a DRAM-sized table is more scratch
    // memory than the table itself, and the read path does not look at values.
    const value_pool = try scratch.alloc([]const u8, 64);
    for (value_pool, 0..) |*value, i| {
        value.* = try std.fmt.allocPrint(scratch, "bench_value_{d}_{d}", .{ i, i * 7919 });
    }

    const batch = database.getOptimalBatchSize();
    const read_results = try scratch.alloc(?*const flat_hash_storage.CacheLineRecord, batch);

    if (wanted(opts, .write)) {
        try measure(results, allocator, size, "write", opts.counters, 0, struct {
            fn run(ctx: *const Ctx) !Outcome {
                for (ctx.keys, 0..) |key, i| {
                    try ctx.database.write(key, ctx.value(i));
                }
                return .{ .ops = ctx.keys.len };
            }
        }.run, &Ctx{
            .database = &database,
            .keys = keys,
            .value_pool = value_pool,
            .read_results = read_results,
            .seed = opts.seed,
        });
    }

    if (wanted(opts, .read)) {
        // Ascending key order. The hash scatters the addresses, so this is not
        // a linear sweep, but the order is still predictable per key.
        try measure(results, allocator, size, "read", opts.counters, try populateIfNeeded(&database, keys, value_pool, opts), struct {
            fn run(ctx: *const Ctx) !Outcome {
                var found: u64 = 0;
                for (ctx.keys) |key| {
                    if (try ctx.database.read(key) != null) found += 1;
                }
                return .{ .ops = ctx.keys.len, .found = found };
            }
        }.run, &Ctx{
            .database = &database,
            .keys = keys,
            .value_pool = value_pool,
            .read_results = read_results,
            .seed = opts.seed,
        });
    }

    if (wanted(opts, .random)) {
        // Random order over the whole key space. This is the phase that answers
        // the cache-miss question: the hardware prefetcher cannot predict the
        // next address, so the table size decides the cost.
        try measure(results, allocator, size, "read-random", opts.counters, try populateIfNeeded(&database, keys, value_pool, opts), struct {
            fn run(ctx: *const Ctx) !Outcome {
                var rng = std.Random.Xoshiro256.init(ctx.seed);
                var found: u64 = 0;
                for (0..ctx.keys.len) |_| {
                    // Modulo bias is irrelevant for picking a victim key.
                    const i = @as(usize, @intCast(rng.next() % @as(u64, ctx.keys.len)));
                    if (try ctx.database.read(ctx.keys[i]) != null) found += 1;
                }
                return .{ .ops = ctx.keys.len, .found = found };
            }
        }.run, &Ctx{
            .database = &database,
            .keys = keys,
            .value_pool = value_pool,
            .read_results = read_results,
            .seed = opts.seed,
        });
    }

    if (wanted(opts, .batch)) {
        // readBatch across the whole key space, in optimal batch chunks. The
        // old version re-read one chunk over and over, which stayed in L1.
        try measure(results, allocator, size, "read-batch", opts.counters, try populateIfNeeded(&database, keys, value_pool, opts), struct {
            fn run(ctx: *const Ctx) !Outcome {
                var found: u64 = 0;
                var start: usize = 0;
                while (start < ctx.keys.len) : (start += ctx.read_results.len) {
                    const end = @min(start + ctx.read_results.len, ctx.keys.len);
                    const chunk = ctx.keys[start..end];
                    ctx.database.readBatchSimple(chunk, ctx.read_results[0..chunk.len]);
                    for (ctx.read_results[0..chunk.len]) |r| {
                        if (r != null) found += 1;
                    }
                }
                return .{ .ops = ctx.keys.len, .found = found };
            }
        }.run, &Ctx{
            .database = &database,
            .keys = keys,
            .value_pool = value_pool,
            .read_results = read_results,
            .seed = opts.seed,
        });
    }

    if (wanted(opts, .pipe)) {
        const populated = try populateIfNeeded(&database, keys, value_pool, opts);
        try measure(results, allocator, size, "read-pipelined", opts.counters, populated, struct {
            fn run(ctx: *const Ctx) !Outcome {
                var found: u64 = 0;
                var start: usize = 0;
                while (start < ctx.keys.len) : (start += ctx.read_results.len) {
                    const end = @min(start + ctx.read_results.len, ctx.keys.len);
                    const chunk = ctx.keys[start..end];
                    found += pipelinedOnce(ctx.database, chunk, ctx.read_results[0..chunk.len], ctx.micro);
                }
                return .{ .ops = ctx.keys.len, .found = found };
            }
        }.run, &Ctx{
            .database = &database,
            .keys = keys,
            .value_pool = value_pool,
            .read_results = read_results,
            .seed = opts.seed,
            .micro = opts.micro,
        });
    }

    if (wanted(opts, .sweep)) {
        // One row per group size, so the best microbatch comes from the
        // machine and not from a blog post.
        const sizes = [_]usize{ 1, 4, 8, 16, 32, 64, 128, 256 };
        for (sizes) |m| {
            const populated = try populateIfNeeded(&database, keys, value_pool, opts);
            var name_buf: [32]u8 = undefined;
            const name = std.fmt.bufPrint(&name_buf, "read-pipe-{d}", .{m}) catch unreachable;
            try measure(results, allocator, size, name, opts.counters, populated, struct {
                fn run(ctx: *const Ctx) !Outcome {
                    var found: u64 = 0;
                    var start: usize = 0;
                    while (start < ctx.keys.len) : (start += ctx.read_results.len) {
                        const end = @min(start + ctx.read_results.len, ctx.keys.len);
                        const chunk = ctx.keys[start..end];
                        found += pipelinedOnce(ctx.database, chunk, ctx.read_results[0..chunk.len], ctx.micro);
                    }
                    return .{ .ops = ctx.keys.len, .found = found };
                }
            }.run, &Ctx{
                .database = &database,
                .keys = keys,
                .value_pool = value_pool,
                .read_results = read_results,
                .seed = opts.seed,
                .micro = m,
            });
        }
    }

    if (wanted(opts, .probes)) {
        _ = try populateIfNeeded(&database, keys, value_pool, opts);
        reportProbeLengths(&database, keys);
    }

    if (wanted(opts, .mixed)) {
        try runMixed(&database, scratch, allocator, keys, value_pool, batch, opts, size, results);
    }

    database.printHardwareReport();
    std.debug.print("\n", .{});
}

/// Everything a phase closure needs. Kept in one place so the phases above
/// differ only in the loop body.
const Ctx = struct {
    database: *wild.WILD,
    keys: []const u64,
    value_pool: []const []const u8,
    read_results: []?*const flat_hash_storage.CacheLineRecord,
    seed: u64,
    micro: usize = 32,

    fn value(self: Ctx, i: usize) []const u8 {
        return self.value_pool[i % self.value_pool.len];
    }
};

/// Write the key set when it is not there yet, so a read phase can run on its
/// own. Runs outside the timed region. A read phase over an empty table
/// measures nothing, and reports 0 ops while looking like it ran.
fn populateIfNeeded(database: *wild.WILD, keys: []const u64, value_pool: []const []const u8, opts: Options) !u64 {
    if (database.getStats().used_capacity != 0) return 0;
    if (!opts.populate) return 0;
    for (keys, 0..) |key, i| {
        try database.write(key, value_pool[i % value_pool.len]);
    }
    return keys.len;
}

/// How many cache lines a lookup must read, across the whole key set.
///
/// The table has no load-factor limit, so fill fraction decides chain length.
/// Prefetch hides the latency of a long chain; only a load-factor policy removes
/// the chain. This is the number that decides whether that policy is worth
/// building, and `--load-factor` is how you drive it there.
fn reportProbeLengths(database: *wild.WILD, keys: []const u64) void {
    const buckets = 64;
    var hist: [buckets]u64 = undefined;
    @memset(&hist, 0);
    var overflow: u64 = 0;
    var missing: u64 = 0;
    var total_lines: u64 = 0;
    var max_lines: u32 = 0;

    for (keys) |key| {
        const lines = database.storage.probeLength(key) orelse {
            missing += 1;
            continue;
        };
        total_lines += lines;
        max_lines = @max(max_lines, lines);
        if (lines < buckets) hist[lines] += 1 else overflow += 1;
    }

    const present = keys.len - missing;
    if (present == 0) {
        std.debug.print("  {s:<14} no keys present\n", .{"probe-length"});
        return;
    }

    const mean = @as(f64, @floatFromInt(total_lines)) / @as(f64, @floatFromInt(present));
    const multi = present - hist[1];

    // Percentiles from the histogram: walk the cumulative count.
    var percentile_lines: [3]u32 = .{ 0, 0, 0 };
    const targets = [_]f64{ 0.50, 0.90, 0.99 };
    for (targets, 0..) |target, slot| {
        const want: u64 = @intFromFloat(@as(f64, @floatFromInt(present)) * target);
        var running: u64 = 0;
        for (1..buckets) |lines| {
            running += hist[lines];
            if (running >= want) {
                percentile_lines[slot] = @intCast(lines);
                break;
            }
        }
        if (percentile_lines[slot] == 0) percentile_lines[slot] = max_lines;
    }

    std.debug.print("  {s:<14} {d:>10} keys  mean {d:.2} lines  p50 {d}  p90 {d}  p99 {d}  max {d}", .{
        "probe-length", present, mean, percentile_lines[0], percentile_lines[1], percentile_lines[2], max_lines,
    });
    std.debug.print("  multi-line {d:.1}%  missing {d}", .{
        @as(f64, @floatFromInt(multi)) * 100.0 / @as(f64, @floatFromInt(present)),
        missing,
    });
    if (overflow > 0) std.debug.print("  overflow {d}", .{overflow});
    std.debug.print("\n", .{});

    if (mean > 2.0) {
        std.debug.print("    mean above 2 lines: the average lookup touches more than one\n", .{});
        std.debug.print("    cache line. Prefetch hides that; a load-factor cap removes it.\n", .{});
    }
}

/// Run one pipelined batch read at the requested group size. `micro` has to be
/// comptime inside `readBatchPipelined` so the position arrays sit on the
/// stack, so a runtime choice becomes a dispatch over the sizes worth testing.
fn pipelinedOnce(
    database: *wild.WILD,
    keys: []const u64,
    results: []?*const flat_hash_storage.CacheLineRecord,
    micro: usize,
) usize {
    return switch (micro) {
        1 => database.readBatchPipelined(keys, results, 1),
        4 => database.readBatchPipelined(keys, results, 4),
        8 => database.readBatchPipelined(keys, results, 8),
        16 => database.readBatchPipelined(keys, results, 16),
        32 => database.readBatchPipelined(keys, results, 32),
        64 => database.readBatchPipelined(keys, results, 64),
        128 => database.readBatchPipelined(keys, results, 128),
        256 => database.readBatchPipelined(keys, results, 256),
        else => database.readBatchPipelined(keys, results, wild.WILD.default_microbatch),
    };
}

/// What a measured phase reports. `ops` is the work attempted, never the work
/// that succeeded: a read phase over an empty table must not report 0 ops.
const Outcome = struct {
    ops: u64 = 0,
    found: u64 = 0,
};

/// Time one phase, print it, and record it.
fn measure(
    results: *std.ArrayList(Result),
    allocator: std.mem.Allocator,
    size: SizeClass,
    phase: []const u8,
    with_counters: bool,
    populated: u64,
    comptime body: *const fn (*const Ctx) anyerror!Outcome,
    ctx: *const Ctx,
) !void {
    var counters: ?bench_counters.Counters = null;
    if (with_counters) counters = bench_counters.Counters.init();
    defer if (counters) |*c| c.deinit();

    const start = os_compat.nanoTimestamp();
    if (counters) |*c| c.start();
    const outcome = try body(ctx);
    if (counters) |*c| c.stop();
    const ns = @as(f64, @floatFromInt(os_compat.nanoTimestamp() - start));
    const ops = outcome.ops;

    const ns_per_op = if (ops > 0) ns / @as(f64, @floatFromInt(ops)) else 0;
    const ops_per_sec = if (ns > 0) @as(f64, @floatFromInt(ops)) / (ns / 1_000_000_000.0) else 0;

    std.debug.print("  {s:<14} {d:>10} ops  {d:>8.1} ns/op  {d:>13.0} ops/sec", .{ phase, outcome.ops, ns_per_op, ops_per_sec });
    if (outcome.found > 0) std.debug.print("  found {d}", .{outcome.found});
    if (populated > 0) std.debug.print("  populated {d} (unmeasured)", .{populated});
    if (counters) |*c| {
        if (c.ipc()) |ipc| {
            std.debug.print("  IPC {d:.2}", .{ipc});
        } else {
            std.debug.print("  IPC n/a", .{});
        }
        if (c.misses()) |misses| {
            std.debug.print("  {s} {d}", .{ c.missSource() orelse "misses", misses });
        } else {
            std.debug.print("  misses n/a", .{});
        }
    }
    std.debug.print("\n", .{});

    try results.append(allocator, .{
        .size = size,
        .phase = phase,
        .ops = ops,
        .ns_per_op = ns_per_op,
        .ipc = if (counters) |*c| c.ipc() else null,
        .misses_per_kilo_op = if (counters) |*c| c.missesPerKiloOps(ops) else null,
        .miss_source = if (counters) |*c| c.missSource() else null,
    });
}

/// 50/50 read and write for a fixed wall-clock budget. No allocation inside
/// the loop: the previous version allocated the results array per iteration,
/// which measured the allocator instead of the database.
fn runMixed(
    database: *wild.WILD,
    scratch: std.mem.Allocator,
    allocator: std.mem.Allocator,
    keys: []const u64,
    value_pool: []const []const u8,
    batch: usize,
    opts: Options,
    size: SizeClass,
    results: *std.ArrayList(Result),
) !void {
    const chunk_len = @min(batch, @max(1, keys.len / 4));
    const chunk = keys[0..chunk_len];
    const read_results = try scratch.alloc(?*const flat_hash_storage.CacheLineRecord, chunk_len);

    var counters: ?bench_counters.Counters = null;
    if (opts.counters) counters = bench_counters.Counters.init();
    defer if (counters) |*c| c.deinit();

    const budget_ns = opts.mixed_seconds * std.time.ns_per_s;
    const start = os_compat.nanoTimestamp();
    if (counters) |*c| c.start();

    var ops: u64 = 0;
    var flip: bool = false;
    while (os_compat.nanoTimestamp() - start < budget_ns) {
        if (flip) {
            database.readBatch(chunk, read_results);
        } else {
            for (chunk, 0..) |key, i| {
                try database.write(key, value_pool[i % value_pool.len]);
            }
        }
        ops += chunk_len;
        flip = !flip;
    }

    if (counters) |*c| c.stop();
    const ns = @as(f64, @floatFromInt(os_compat.nanoTimestamp() - start));
    const ns_per_op = if (ops > 0) ns / @as(f64, @floatFromInt(ops)) else 0;

    std.debug.print("  {s:<14} {d:>10} ops  {d:>8.1} ns/op  {d:>13.0} ops/sec", .{
        "mixed",
        ops,
        ns_per_op,
        @as(f64, @floatFromInt(ops)) / (ns / 1_000_000_000.0),
    });
    if (counters) |*c| {
        if (c.ipc()) |ipc| std.debug.print("  IPC {d:.2}", .{ipc});
        if (c.misses()) |m| std.debug.print("  {s} {d}", .{ c.missSource() orelse "misses", m });
    }
    std.debug.print("\n", .{});

    try results.append(allocator, .{
        .size = size,
        .phase = "mixed",
        .ops = ops,
        .ns_per_op = ns_per_op,
        .ipc = if (counters) |*c| c.ipc() else null,
        .misses_per_kilo_op = if (counters) |*c| c.missesPerKiloOps(ops) else null,
        .miss_source = if (counters) |*c| c.missSource() else null,
    });
}

/// Capacity for a size class, rounded up to a power of two the way WILD does.
fn capacityFor(l3_bytes: u64, size: SizeClass) u64 {
    const target_bytes = l3_bytes * size.multiplier();
    var cap: u64 = @intFromFloat(@as(f64, @floatFromInt(target_bytes)) * 0.75 / record_bytes);
    cap = @max(1024, cap);
    // Round up to the next power of two, matching Wild.init.
    var p: u64 = 1024;
    while (p < cap) p *= 2;
    return p;
}

fn printComparison(items: []const Result) void {
    std.debug.print("── comparison ──\n", .{});
    std.debug.print("{s:<14} {s:>16} {s:>16} {s:>9}\n", .{ "phase", "L3 ns/op", "DRAM ns/op", "ratio" });

    for (items) |l3| {
        if (l3.size != .l3_resident) continue;
        var dram: ?Result = null;
        for (items) |d| {
            if (d.size == .dram_resident and std.mem.eql(u8, d.phase, l3.phase)) dram = d;
        }
        const ratio = if (dram) |d| d.ns_per_op / @max(l3.ns_per_op, 0.001) else 0;
        std.debug.print("{s:<14} {d:>16.1} {d:>16.1} {d:>8.1}x\n", .{
            l3.phase,
            l3.ns_per_op,
            if (dram) |d| d.ns_per_op else 0,
            ratio,
        });
        if (l3.ipc) |ipc| {
            std.debug.print("{s:<14} {s:>16} {s:>16} IPC L3 {d:.2} / DRAM {d:.2}\n", .{
                "",
                "",
                "",
                ipc,
                if (dram) |d| d.ipc orelse 0 else 0,
            });
        }
    }
}
