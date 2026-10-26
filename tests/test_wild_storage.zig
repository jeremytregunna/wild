//! Round-trip tests for the database, using the same static allocator the
//! server uses. These run under `zig build test`.

const std = @import("std");
const testing = std.testing;
const wildb_lib = @import("wildb_lib");

const wild = wildb_lib.wild;
const static_allocator = wildb_lib.static_allocator;
const CacheLineRecord = wildb_lib.CacheLineRecord;
const flat_hash_storage = wildb_lib.flat_hash_storage;

test "writeString and readString round trip" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var database_arena = std.heap.ArenaAllocator.init(arena.allocator());
    var static_alloc = static_allocator.StaticAllocator.init(database_arena.allocator());

    // Build the database the way main.zig does, then freeze the allocator the
    // way the server does before it accepts a request.
    var database = try wild.WILD.init(static_alloc.allocator(), .{ .target_capacity = 100_000 });
    static_alloc.transitionToStatic();

    try database.writeString("round-trip-key", "round-trip-value");

    const record = try database.readString("round-trip-key") orelse {
        return error.ValueMissing;
    };
    try testing.expectEqualStrings("round-trip-value", record.getData());
}

test "deleteString removes the record" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var database_arena = std.heap.ArenaAllocator.init(arena.allocator());
    var static_alloc = static_allocator.StaticAllocator.init(database_arena.allocator());

    var database = try wild.WILD.init(static_alloc.allocator(), .{ .target_capacity = 100_000 });
    static_alloc.transitionToStatic();

    try database.writeString("doomed", "temporary");
    try testing.expect(try database.deleteString("doomed"));

    const gone = try database.readString("doomed");
    try testing.expectEqual(@as(?*const CacheLineRecord, null), gone);
}

test "missing key reads as null" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var database_arena = std.heap.ArenaAllocator.init(arena.allocator());
    var static_alloc = static_allocator.StaticAllocator.init(database_arena.allocator());

    var database = try wild.WILD.init(static_alloc.allocator(), .{ .target_capacity = 100_000 });
    static_alloc.transitionToStatic();

    const missing = try database.readString("never-written");
    try testing.expectEqual(@as(?*const CacheLineRecord, null), missing);
}

test "an update replaces the value" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var database_arena = std.heap.ArenaAllocator.init(arena.allocator());
    var static_alloc = static_allocator.StaticAllocator.init(database_arena.allocator());

    var database = try wild.WILD.init(static_alloc.allocator(), .{ .target_capacity = 100_000 });
    static_alloc.transitionToStatic();

    try database.writeString("counter", "one");
    try database.writeString("counter", "two");

    const record = try database.readString("counter") orelse {
        return error.ValueMissing;
    };
    try testing.expectEqualStrings("two", record.getData());
}

// ---------------------------------------------------------------------------
// Delete and probe chains.
//
// Linear probing places a colliding key in the next free slot. A lookup stops
// at the first empty slot it meets, so a delete that leaves a hole in the
// middle of a run makes every key behind that hole unreachable. The three
// tests below cover that failure at the storage level and at the string API
// the server uses.
// ---------------------------------------------------------------------------

/// The home slot a key probes from. Matches what `FlatHashStorage` does when
/// it stores a `u64` key: one application of `hashKey`, masked by capacity.
fn homeSlot(key: u64, capacity: u32) u32 {
    return @as(u32, @intCast(flat_hash_storage.hashKey(key) & (capacity - 1)));
}

/// The home slot a string key probes from, through the double hash the string
/// API performs: `hashKey` on the bytes, then `hashKey` again inside storage.
fn homeSlotOfString(key: []const u8, capacity: u32) u32 {
    return homeSlot(flat_hash_storage.hashKey(key), capacity);
}

/// Return two distinct keys in `0 .. limit` that share a home slot, searching
/// in a fixed order so the pair is the same on every run and every machine.
fn collidingPair(capacity: u32, limit: u64) struct { first: u64, second: u64, slot: u32 } {
    var first: u64 = 0;
    while (first < limit) : (first += 1) {
        const slot = homeSlot(first, capacity);
        var second = first + 1;
        while (second < limit) : (second += 1) {
            if (homeSlot(second, capacity) == slot) {
                return .{ .first = first, .second = second, .slot = slot };
            }
        }
    }
    unreachable; // a 1024-slot table yields a collision well before 200_000 keys
}

/// Build a storage of `capacity` slots, or return null where the machine
/// exposes no cache topology, which is the same skip the root tests use.
fn makeStorage(
    allocator: std.mem.Allocator,
    topology: *wildb_lib.CacheTopology,
    capacity: u32,
) !wildb_lib.FlatHashStorage {
    return wildb_lib.FlatHashStorage.init(allocator, topology, capacity);
}

test "delete does not strand a key that probed past the deleted slot" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var topology = wildb_lib.analyzeCacheTopology(alloc) catch return; // no /sys: skip
    defer topology.deinit();

    const capacity: u32 = 1024;
    var store = try makeStorage(alloc, &topology, capacity);
    defer store.deinit();

    const pair = collidingPair(capacity, 200_000);

    // Write the first key, then the one that collides with it. The second has
    // to land one slot further along, so it depends on the first staying put.
    try store.write(pair.first, "value-one");
    try store.write(pair.second, "value-two");

    try testing.expectEqual(@as(u32, 2), store.probeLength(pair.second));

    try testing.expect(store.delete(pair.first));

    // The deleted key must be gone...
    try testing.expectEqual(@as(?*const CacheLineRecord, null), store.read(pair.first));

    // ...and the key behind it must still be reachable. Before the fix this
    // read returned null while the bytes of the value were still in the table.
    const survivor = store.read(pair.second) orelse return error.KeyStrandedByDelete;
    try testing.expectEqualStrings("value-two", survivor.getData());
}

test "delete keeps every surviving key reachable across a half-full table" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var topology = wildb_lib.analyzeCacheTopology(alloc) catch return; // no /sys: skip
    defer topology.deinit();

    const capacity: u32 = 4096;
    const filled: u64 = 2048; // half full, so collisions and runs are routine
    var store = try makeStorage(alloc, &topology, capacity);
    defer store.deinit();

    var value_buf: [16]u8 = undefined;
    for (0..filled) |i| {
        try store.write(i, std.fmt.bufPrint(&value_buf, "v{d}", .{i}) catch unreachable);
    }

    // Delete every third key. The survivors are the ones a hole can strand.
    var deleted: usize = 0;
    for (0..filled) |i| {
        if (i % 3 == 0) {
            try testing.expect(store.delete(i));
            deleted += 1;
        }
    }

    var expected_live: usize = 0;
    for (0..filled) |i| {
        const record = store.read(i);
        if (i % 3 == 0) {
            try testing.expectEqual(@as(?*const CacheLineRecord, null), record);
        } else {
            expected_live += 1;
            const found = record orelse return error.KeyStrandedByDelete;
            try testing.expectEqualStrings(
                std.fmt.bufPrint(&value_buf, "v{d}", .{i}) catch unreachable,
                found.getData(),
            );
        }
    }

    // A count that drifts would quietly distort load factor and the full check.
    try testing.expectEqual(@as(u32, @intCast(expected_live)), store.getStats().total_count);
    try testing.expectEqual(@as(u64, filled - deleted), expected_live);
}

// Fill a small table to 95 percent, delete a scattered half of it by a fixed
// pseudo-random pattern, and check every survivor.
//
// The table is small and nearly full on purpose: the runs then reach the last
// slot and wrap to the first, which is the branch the two tests above never
// enter. A wrong shift there strands a key on the far side of the wrap.
test "delete survives long runs that wrap the end of the table" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var topology = wildb_lib.analyzeCacheTopology(alloc) catch return; // no /sys: skip
    defer topology.deinit();

    const capacity: u32 = 256;
    const filled: u64 = 244; // 95 percent, so runs reach the end and wrap
    var store = try makeStorage(alloc, &topology, capacity);
    defer store.deinit();

    var buf: [24]u8 = undefined;
    for (0..filled) |i| {
        try store.write(i, std.fmt.bufPrint(&buf, "v{d}", .{i}) catch unreachable);
    }

    // Deterministic scatter: a fixed-seed xorshift decides which keys go.
    var state: u64 = 0x9E37_79B9_7F4A_7C15;
    var gone = std.mem.zeroes([256]bool);
    var removed: usize = 0;
    while (removed < filled / 2) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        const victim = state % filled;
        if (gone[victim]) continue;
        gone[victim] = true;
        try testing.expect(store.delete(victim));
        removed += 1;
    }

    var live: usize = 0;
    for (0..filled) |i| {
        const expected = std.fmt.bufPrint(&buf, "v{d}", .{i}) catch unreachable;
        if (gone[i]) {
            try testing.expectEqual(@as(?*const CacheLineRecord, null), store.read(i));
        } else {
            live += 1;
            const found = store.read(i) orelse return error.KeyStrandedByDelete;
            try testing.expectEqualStrings(expected, found.getData());
        }
    }
    try testing.expectEqual(@as(u32, @intCast(live)), store.getStats().total_count);

    // Refilling the freed slots must work, so the holes stay usable, and it
    // must not disturb the keys that were never deleted.
    for (0..filled) |i| {
        if (gone[i]) try store.write(i, "rewritten");
    }
    for (0..filled) |i| {
        const found = store.read(i) orelse return error.KeyMissingAfterRefill;
        const expected = if (gone[i])
            "rewritten"
        else
            std.fmt.bufPrint(&buf, "v{d}", .{i}) catch unreachable;
        try testing.expectEqualStrings(expected, found.getData());
    }
}

test "deleteString keeps every surviving key reachable through the string API" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var database_arena = std.heap.ArenaAllocator.init(arena.allocator());
    var static_alloc = static_allocator.StaticAllocator.init(database_arena.allocator());

    // Small on purpose: it keeps the collision search below cheap.
    var database = try wild.WILD.init(static_alloc.allocator(), .{ .target_capacity = 1024 });
    static_alloc.transitionToStatic();

    const capacity: u32 = 1024;

    // Find two strings that share a home slot, in a fixed order.
    const limit = 200_000;
    var buf_a: [32]u8 = undefined;
    var buf_b: [32]u8 = undefined;
    var key_a: []const u8 = undefined;
    var key_b: []const u8 = undefined;
    outer: for (0..limit) |a| {
        const ka = std.fmt.bufPrint(&buf_a, "key-{d}", .{a}) catch unreachable;
        const slot = homeSlotOfString(ka, capacity);
        var b = a + 1;
        while (b < limit) : (b += 1) {
            const kb = std.fmt.bufPrint(&buf_b, "key-{d}", .{b}) catch unreachable;
            if (homeSlotOfString(kb, capacity) == slot) {
                key_a = try arena.allocator().dupe(u8, ka);
                key_b = try arena.allocator().dupe(u8, kb);
                break :outer;
            }
        }
    }

    try database.writeString(key_a, "value-one");
    try database.writeString(key_b, "value-two");

    try testing.expect(try database.deleteString(key_a));

    try testing.expectEqual(@as(?*const CacheLineRecord, null), try database.readString(key_a));

    const survivor = try database.readString(key_b) orelse return error.KeyStrandedByDelete;
    try testing.expectEqualStrings("value-two", survivor.getData());
}
