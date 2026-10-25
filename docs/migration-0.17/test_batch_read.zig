//! Batch-read equivalence tests.
//!
//! `readBatchPipelined` reorders memory traffic: it requests the home line of
//! every key in a group before it walks any probe chain, and it requests the
//! next group before it resolves the current one. That is only an optimisation
//! if the answers are identical to looking each key up on its own. These tests
//! pin that, including the cases the reordering could plausibly get wrong:
//! probe chains longer than one line, group sizes that do not divide the key
//! count, and an empty table.

const std = @import("std");
const testing = std.testing;
const wildb_lib = @import("wildb_lib");

const wild = wildb_lib.wild;
const static_allocator = wildb_lib.static_allocator;
const flat_hash_storage = wildb_lib.flat_hash_storage;
const CacheLineRecord = wildb_lib.CacheLineRecord;

/// A database that has already frozen its allocator, the way the server does
/// before it serves a request. Anything that allocates after this point
/// panics, so these tests also prove a batch read does not allocate.
const TestDb = struct {
    arena: std.heap.ArenaAllocator,
    /// The database keeps a pointer to this allocator, so it must outlive
    /// init(). Leaving it as a local in init() would leave the database
    /// pointing into a dead stack frame.
    static_alloc: static_allocator.StaticAllocator,
    database: wild.WILD,

    fn init(capacity: u64) !TestDb {
        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        errdefer arena.deinit();

        var static_alloc = static_allocator.StaticAllocator.init(arena.allocator());
        const database = try wild.WILD.init(static_alloc.allocator(), .{ .target_capacity = capacity });
        static_alloc.transitionToStatic();

        return .{ .arena = arena, .static_alloc = static_alloc, .database = database };
    }

    /// Free the arena, which owns everything the database allocated.
    ///
    /// `database.deinit()` is deliberately not called. It frees a 64-byte
    /// aligned allocation through `Allocator.free`, which reports alignment 1,
    /// and the static allocator refuses to free anything while in the static
    /// state. The production path gets away with both because its backing
    /// allocator is an arena, which ignores them. That teardown is a separate
    /// defect, recorded in docs/MIGRATION_0.17.md.
    fn deinit(self: *TestDb) void {
        self.arena.deinit();
    }
};

/// Compare one batch result against looking the same keys up one at a time.
fn expectBatchMatchesSingleReads(
    database: *wild.WILD,
    keys: []const u64,
    results: []?*const CacheLineRecord,
    comptime micro: usize,
) !void {
    const found = database.readBatchPipelined(keys, results, micro);

    var expected_found: usize = 0;
    for (keys, 0..) |key, i| {
        const single = try database.read(key);
        try testing.expectEqual(single != null, results[i] != null);

        if (single) |expected| {
            expected_found += 1;
            const got = results[i].?;
            try testing.expectEqual(expected.key, got.key);
            try testing.expectEqualStrings(expected.getData(), got.getData());
        }
    }

    try testing.expectEqual(expected_found, found);
}

test "pipelined batch read matches single reads across group boundaries" {
    var db = try TestDb.init(100_000);
    defer db.deinit();

    // Every key in the table, plus keys that were never written.
    const keys = try testing.allocator.alloc(u64, 500);
    defer testing.allocator.free(keys);
    const results = try testing.allocator.alloc(?*const CacheLineRecord, keys.len);
    defer testing.allocator.free(results);

    var value_buf: [32]u8 = undefined;
    for (keys, 0..) |*key, i| {
        key.* = @as(u64, i) * 3; // every third key is absent
        if (i % 3 != 0) {
            const value = std.fmt.bufPrint(&value_buf, "v{}", .{key.*}) catch unreachable;
            try db.database.write(key.*, value);
        }
    }

    // Group sizes that straddle the interesting boundaries: smaller than a
    // group, exactly one group, one past it, and several groups with a
    // remainder.
    // inline for, because `micro` selects a comptime instantiation.
    inline for ([_]usize{ 1, 2, 3, 7, 8, 9, 16, 64, 500, 1024 }) |micro| {
        try expectBatchMatchesSingleReads(&db.database, keys, results, micro);
    }
}

test "pipelined batch read walks probe chains longer than one line" {
    var db = try TestDb.init(65_536);
    defer db.deinit();

    // Keys that share a home slot land in consecutive lines, so resolving them
    // walks more than one cache line. Find them by hashing candidates rather
    // than by trusting that some will collide: with 65k buckets, scanning a
    // few hundred thousand candidates reliably produces a large bucket.
    const mask: u64 = 65_536 - 1;
    var bucket = std.AutoHashMap(u64, std.ArrayList(u64)).init(testing.allocator);
    defer {
        var it = bucket.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(testing.allocator);
        bucket.deinit();
    }

    var candidate: u64 = 1;
    while (candidate < 400_000) : (candidate += 1) {
        const home = flat_hash_storage.hashKey(candidate) & mask;
        const entry = try bucket.getOrPut(home);
        if (!entry.found_existing) entry.value_ptr.* = std.ArrayList(u64).empty;
        try entry.value_ptr.append(testing.allocator, candidate);
    }

    // Take the largest bucket found.
    var best: []const u64 = &.{};
    var it = bucket.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.items.len > best.len) best = entry.value_ptr.items;
    }
    try testing.expect(best.len >= 8); // the test is worthless without a real chain

    var value_buf: [24]u8 = undefined;
    for (best, 0..) |key, i| {
        const value = std.fmt.bufPrint(&value_buf, "chain{}", .{i}) catch unreachable;
        try db.database.write(key, value);
    }

    const results = try testing.allocator.alloc(?*const CacheLineRecord, best.len);
    defer testing.allocator.free(results);

    try expectBatchMatchesSingleReads(&db.database, best, results, 8);

    // Every key in a shared bucket must still be found: if the probe walk
    // stopped early, the tail of the chain would read as missing.
    const found = db.database.readBatchPipelined(best, results, 4);
    try testing.expectEqual(best.len, found);
}

test "pipelined batch read on an empty table returns null for every key" {
    var db = try TestDb.init(10_000);
    defer db.deinit();

    const keys = try testing.allocator.alloc(u64, 100);
    defer testing.allocator.free(keys);
    const results = try testing.allocator.alloc(?*const CacheLineRecord, keys.len);
    defer testing.allocator.free(results);

    for (keys, 0..) |*key, i| key.* = @as(u64, i) + 1;

    const found = db.database.readBatchPipelined(keys, results, 8);
    try testing.expectEqual(@as(usize, 0), found);
    for (results) |record| {
        try testing.expectEqual(@as(?*const CacheLineRecord, null), record);
    }
}

test "readBatch agrees with the plain loop below and above the threshold" {
    var db = try TestDb.init(50_000);
    defer db.deinit();

    var keys = try testing.allocator.alloc(u64, 200);
    defer testing.allocator.free(keys);
    for (keys, 0..) |*key, i| key.* = @as(u64, i) * 7 + 1;

    var value_buf: [24]u8 = undefined;
    for (keys, 0..) |key, i| {
        if (i % 2 == 0) continue; // half the keys are absent
        const value = std.fmt.bufPrint(&value_buf, "k{}", .{key}) catch unreachable;
        try db.database.write(key, value);
    }

    var simple = try testing.allocator.alloc(?*const CacheLineRecord, keys.len);
    defer testing.allocator.free(simple);
    var batched = try testing.allocator.alloc(?*const CacheLineRecord, keys.len);
    defer testing.allocator.free(batched);

    // Lengths either side of WILD.pipelined_threshold: below it readBatch takes
    // the plain loop, above it the prefetching path. Both must agree with the
    // plain loop exactly.
    const lengths = [_]usize{ 1, 2, wild.WILD.pipelined_threshold - 1, wild.WILD.pipelined_threshold, wild.WILD.pipelined_threshold + 1, 64, 200 };
    for (lengths) |len| {
        const some_keys = keys[0..len];
        db.database.readBatchSimple(some_keys, simple[0..len]);
        db.database.readBatch(some_keys, batched[0..len]);

        for (simple[0..len], batched[0..len]) |a, b| {
            try testing.expectEqual(a != null, b != null);
            if (a) |rec_a| {
                const rec_b = b.?;
                try testing.expectEqual(rec_a.key, rec_b.key);
                try testing.expectEqualStrings(rec_a.getData(), rec_b.getData());
            }
        }
    }
}

test "a batch reflects writes made before it and is not a table-wide snapshot" {
    var db = try TestDb.init(20_000);
    defer db.deinit();

    const keys = [_]u64{ 11, 22, 33, 44, 55 };
    var results: [keys.len]?*const CacheLineRecord = undefined;

    try db.database.write(11, "one");
    try db.database.write(33, "three");
    try testing.expectEqual(@as(usize, 2), db.database.readBatchPipelined(&keys, &results, 8));
    try testing.expectEqualStrings("one", results[0].?.getData());
    try testing.expectEqual(@as(?*const CacheLineRecord, null), results[1]);

    // An update after the batch is visible to the next batch. A batch is a
    // snapshot per key, not one consistent view of the table taken at a moment.
    try db.database.write(22, "two");
    try db.database.write(33, "THREE");
    try testing.expectEqual(@as(usize, 3), db.database.readBatchPipelined(&keys, &results, 8));
    try testing.expectEqualStrings("two", results[1].?.getData());
    try testing.expectEqualStrings("THREE", results[2].?.getData());
}

test "readBatchPipelined writes only the result slots it was given" {
    var db = try TestDb.init(20_000);
    defer db.deinit();

    try db.database.write(5, "five");

    // A sentinel record that no read can return: it is not in the table.
    var sentinel = CacheLineRecord.initEmpty();
    sentinel.key = 0xDEADBEEF;
    sentinel.metadata = CacheLineRecord.VALID_FLAG | 4;
    sentinel.data[0..4].* = .{ 'x', 'x', 'x', 'x' };

    const keys = [_]u64{5};
    var results: [4]?*const CacheLineRecord = .{ &sentinel, &sentinel, &sentinel, &sentinel };

    // results is longer than keys: the extra slots belong to the caller and
    // must stay untouched.
    const found = db.database.readBatchPipelined(&keys, results[0..1], 8);
    try testing.expectEqual(@as(usize, 1), found);
    try testing.expectEqualStrings("five", results[0].?.getData());
    try testing.expectEqual(@as(?*const CacheLineRecord, &sentinel), results[1]);
    try testing.expectEqual(@as(?*const CacheLineRecord, &sentinel), results[2]);
    try testing.expectEqual(@as(?*const CacheLineRecord, &sentinel), results[3]);
}
