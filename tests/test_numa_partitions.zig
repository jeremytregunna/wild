//! The partitioned store has to survive the paths that rebuild it, not just
//! the request path.
//!
//! A key's table is `bucket(key) % table_count`, and only the key is carried by
//! a log entry, a snapshot record, or a replication payload. So replay, restore
//! and replica apply all have to route by key — there is nothing else to route
//! by — and a mistake there is invisible in the obvious direction: a key stored
//! in the wrong table is still counted, still in memory, and never found.
//!
//! These tests build two tables on whatever zones the machine has, so the
//! multi-table paths run where a two-zone box is not available.

const std = @import("std");
const testing = std.testing;
const wildb_lib = @import("wildb_lib");

const CacheLineRecord = wildb_lib.CacheLineRecord;
const TableSet = wildb_lib.TableSet;
const wal = wildb_lib.wal;
const snapshot = wildb_lib.snapshot;
const os_compat = wildb_lib.os_compat;
const partition = wildb_lib.partition;

/// Scratch space inside the build cache, so a failed run leaves nothing in the
/// working tree.
const scratch_dir = ".zig-cache/wild_numa_partitions";

fn newTables(topology: *wildb_lib.CacheTopology, allocator: std.mem.Allocator, capacity: u64) !?TableSet {
    const tables = TableSet.init(allocator, topology, capacity, 2) catch return null;
    return tables;
}

test "a replayed log puts each entry in the table its key routes to" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var topology = wildb_lib.analyzeCacheTopology(alloc) catch return;
    defer topology.deinit();

    var store = (try newTables(&topology, alloc, 1024)) orelse return;
    defer store.deinit();

    // Keys that land in both tables, with one key written, deleted and written
    // again so replay order has to survive the routing.
    const keys = [_]u64{ 111, 222, 333, 444, 555 };
    var write_batch = wal.WALBatch.init(0, 1);
    for (keys) |key| {
        var record = try CacheLineRecord.init(key, "logged");
        try testing.expect(write_batch.addEntry(wal.WALEntry.fromCacheLineRecord(&record)));
    }
    var delete_batch = wal.WALBatch.init(1, 1);
    try testing.expect(delete_batch.addEntry(wal.WALEntry.fromCacheLineRecord(&CacheLineRecord.initDelete(keys[1]))));

    os_compat.makeDir(scratch_dir) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };

    var path_buffer: [256]u8 = undefined;
    const wal_path = try std.fmt.bufPrint(&path_buffer, "{s}/replay.wal", .{scratch_dir});
    {
        const file = try os_compat.createFile(wal_path, .{ .truncate = true });
        defer file.close();
        try file.writeAll(std.mem.asBytes(&write_batch));
        try file.writeAll(std.mem.asBytes(&delete_batch));
    }
    defer os_compat.deleteFile(wal_path) catch {};

    var replayer = snapshot.WALReplayer.init(alloc, wal_path);
    _ = try replayer.replayFromPosition(&store, 0, null);

    // The deleted key is gone, and every survivor is visible through routing.
    try testing.expect(try store.read(keys[1]) == null);
    for (keys, 0..) |key, index| {
        if (index == 1) continue;
        try testing.expect(try store.read(key) != null);
    }

    // No replayed key sits in a table that does not own it.
    for (keys, 0..) |key, index| {
        if (index == 1) continue;
        const owner = store.tables[partition.tableOfKey(key, 2)];
        const stranger = store.tables[1 - partition.tableOfKey(key, 2)];
        try testing.expect(owner.storage.read(key) != null);
        try testing.expect(stranger.storage.read(key) == null);
    }
}

test "a snapshot taken on two tables restores onto three" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var topology = wildb_lib.analyzeCacheTopology(alloc) catch return;
    defer topology.deinit();

    var source = TableSet.init(alloc, &topology, 1024, 2) catch return;
    defer source.deinit();

    var keys: [24]u64 = undefined;
    for (0..keys.len) |index| keys[index] = @intCast(index * 31 + 3);
    for (keys) |key| {
        var buffer: [16]u8 = undefined;
        try source.write(key, try std.fmt.bufPrint(&buffer, "v{d}", .{key}));
    }

    os_compat.makeDir(scratch_dir) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };

    var path_buffer: [256]u8 = undefined;
    const snapshot_path = try std.fmt.bufPrint(&path_buffer, "{s}/two-onto-three.snap", .{scratch_dir});
    defer os_compat.deleteFile(snapshot_path) catch {};

    var file = try snapshot.SnapshotFile.create(snapshot_path, &source, 0);
    defer file.deinit();

    // The destination proves a different zone count, so it splits the same
    // total into different pieces. Every record still has to find its table
    // from its key, and the totals have to match or restore refuses.
    var restored = TableSet.init(alloc, &topology, 1024, 3) catch return;
    defer restored.deinit();

    try testing.expectEqual(@as(u16, 3), restored.count());
    try testing.expectEqual(source.totalCapacity(), restored.totalCapacity());
    try file.restoreToStorage(&restored);

    for (keys) |key| {
        const found = try restored.read(key);
        try testing.expect(found != null);

        var buffer: [16]u8 = undefined;
        const expected = try std.fmt.bufPrint(&buffer, "v{d}", .{key});
        try testing.expectEqualStrings(expected, found.?.getData());

        for (restored.tables, 0..) |table, index| {
            if (index != partition.tableOfKey(key, 3)) {
                try testing.expect(table.storage.read(key) == null);
            }
        }
    }
}

test "a snapshot round-trips two tables" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var topology = wildb_lib.analyzeCacheTopology(alloc) catch return;
    defer topology.deinit();

    var source = (try newTables(&topology, alloc, 1024)) orelse return;
    defer source.deinit();

    var keys: [40]u64 = undefined;
    for (0..keys.len) |index| keys[index] = @intCast(index * 13 + 1);

    for (keys) |key| {
        var buffer: [16]u8 = undefined;
        const data = try std.fmt.bufPrint(&buffer, "v{d}", .{key});
        try source.write(key, data);
    }
    _ = try source.delete(keys[7]);

    os_compat.makeDir(scratch_dir) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };

    var path_buffer: [256]u8 = undefined;
    const snapshot_path = try std.fmt.bufPrint(&path_buffer, "{s}/two-tables.snap", .{scratch_dir});
    defer os_compat.deleteFile(snapshot_path) catch {};

    var file = try snapshot.SnapshotFile.create(snapshot_path, &source, 0);
    defer file.deinit();

    // A fresh set, as a restarted process would have. Its capacities have to
    // line up with the header, and each record has to find its own table
    // again from its key alone.
    var restored = (try newTables(&topology, alloc, 1024)) orelse return;
    defer restored.deinit();

    try testing.expectEqual(source.totalCapacity(), restored.totalCapacity());
    try file.restoreToStorage(&restored);

    for (keys, 0..) |key, index| {
        if (index == 7) {
            try testing.expect(try restored.read(key) == null);
            continue;
        }

        const found = try restored.read(key);
        try testing.expect(found != null);

        var buffer: [16]u8 = undefined;
        const expected = try std.fmt.bufPrint(&buffer, "v{d}", .{key});
        try testing.expectEqualStrings(expected, found.?.getData());

        const stranger = restored.tables[1 - partition.tableOfKey(key, 2)];
        try testing.expect(stranger.storage.read(key) == null);
    }
}
