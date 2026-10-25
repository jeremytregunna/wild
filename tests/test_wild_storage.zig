//! Round-trip tests for the database, using the same static allocator the
//! server uses. These run under `zig build test`.

const std = @import("std");
const testing = std.testing;
const wildb_lib = @import("wildb_lib");

const wild = wildb_lib.wild;
const static_allocator = wildb_lib.static_allocator;
const CacheLineRecord = wildb_lib.CacheLineRecord;

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
