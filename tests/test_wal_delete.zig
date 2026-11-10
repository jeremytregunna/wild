//! Deletes must survive a WAL replay.
//!
//! A WAL entry is a 64-byte mirror of a cache-line record, and until now the
//! only thing a reader could do with one was write it. A delete produced no
//! entry at all, so replaying the log after a crash brought every deleted key
//! back. These tests pin the marker that fixes it, and pin the three places
//! that read the marker: WAL replay, snapshot restore, and replica apply.
//!
//! The durability thread that flushes batches to disk is deliberately not
//! exercised here. It copies 64-byte entries verbatim and never reads the
//! metadata, so it cannot tell a marker from a write. Every place that does
//! read the metadata goes through `FlatHashStorage.applyEntry`, tested below.

const std = @import("std");
const testing = std.testing;
const wildb_lib = @import("wildb_lib");

const flat_hash_storage = wildb_lib.flat_hash_storage;
const CacheLineRecord = wildb_lib.CacheLineRecord;
const wal = wildb_lib.wal;
const os_compat = wildb_lib.os_compat;
const static_allocator = wildb_lib.static_allocator;

/// Scratch space for WAL files, inside the build cache so a crash mid-test
/// leaves nothing in the working tree. The directory is created by the build.
const scratch_prefix = ".zig-cache/wild_wal_test_";

/// Write `batches` to a WAL file at `path`, as the durability thread would.
fn writeWalFile(path: []const u8, batches: []const wal.WALBatch) !void {
    const file = try os_compat.createFile(path, .{ .truncate = true });
    defer file.close();
    for (batches) |*batch| try file.writeAll(std.mem.asBytes(batch));
}

/// A storage of `capacity` slots, or null where the machine exposes no cache
/// topology, which is the same skip the other storage tests use.
fn newStorage(
    allocator: std.mem.Allocator,
    topology: *wildb_lib.CacheTopology,
    capacity: u32,
) !?wildb_lib.TableSet {
    const store = wildb_lib.TableSet.init(allocator, topology, capacity, null) catch
        return null;
    return store;
}

/// A WAL entry that writes `key`.
fn writeEntry(key: u64, value: []const u8) wal.WALEntry {
    const record = CacheLineRecord.init(key, value) catch unreachable;
    return wal.WALEntry.fromCacheLineRecord(&record);
}

/// A WAL entry that deletes `key`.
fn deleteEntry(key: u64) wal.WALEntry {
    const marker = CacheLineRecord.initDelete(key);
    return wal.WALEntry.fromCacheLineRecord(&marker);
}

test "a delete marker is a distinct state from a record holding an empty value" {
    // Storing an empty value is legal, so length 0 cannot mean "delete". The
    // marker needs its own bit, and that bit must not overlap the length
    // field, or a legal value length would read as a delete.
    const empty = try CacheLineRecord.init(7, "");
    try testing.expect(empty.isValid());
    try testing.expect(!empty.isDelete());
    try testing.expectEqual(@as(usize, 0), empty.getData().len);

    const marker = CacheLineRecord.initDelete(7);
    try testing.expect(marker.isValid()); // a WAL reader must still accept it
    try testing.expect(marker.isDelete());
    try testing.expectEqual(@as(u64, 7), marker.getKey());

    // Every legal length, including 0, must be distinguishable from a marker.
    var buf: [52]u8 = undefined;
    @memset(&buf, 0x5A);
    var len: usize = 0;
    while (len <= 52) : (len += 1) {
        const record = try CacheLineRecord.init(1, buf[0..len]);
        try testing.expect(!record.isDelete());
    }

    try testing.expectEqual(@as(u32, 0), CacheLineRecord.DELETE_FLAG & CacheLineRecord.LENGTH_MASK);
}

test "replaying a WAL that deletes a key leaves that key gone" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var topology = wildb_lib.analyzeCacheTopology(alloc) catch return; // no /sys: skip
    defer topology.deinit();

    const capacity: u32 = 1024;
    var store = (try newStorage(alloc, &topology, capacity)) orelse return;
    defer store.deinit();

    // The log says: write a, write b, delete a.
    var batches: [2]wal.WALBatch = .{
        wal.WALBatch.init(0, 1),
        wal.WALBatch.init(1, 1),
    };
    try testing.expect(batches[0].addEntry(writeEntry(111, "value-a")));
    try testing.expect(batches[0].addEntry(writeEntry(222, "value-b")));
    try testing.expect(batches[1].addEntry(deleteEntry(111)));

    const wal_path = scratch_prefix ++ "replay_delete.wal";
    try writeWalFile(wal_path, &batches);
    defer os_compat.deleteFile(wal_path) catch {};

    var replayer = wildb_lib.snapshot.WALReplayer.init(alloc, wal_path);
    const end_position = try replayer.replayFromPosition(&store, 0, null);
    try testing.expect(end_position > 0);

    // Before the fix, a was replayed as a write of an empty value and came back.
    try testing.expectEqual(@as(?*const CacheLineRecord, null), try store.read(111));

    const survivor = try store.read(222) orelse return error.KeyLostDuringReplay;
    try testing.expectEqualStrings("value-b", survivor.getData());
    try testing.expectEqual(@as(u32, 1), store.getStats().total_count);
}

test "replaying writes after the delete in the same log keeps them" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var topology = wildb_lib.analyzeCacheTopology(alloc) catch return; // no /sys: skip
    defer topology.deinit();

    var store = (try newStorage(alloc, &topology, 1024)) orelse return;
    defer store.deinit();

    // a is written, deleted, then written again. Order in the log is the order
    // that must survive, so a ends up present with the later value.
    var batch = wal.WALBatch.init(0, 1);
    try testing.expect(batch.addEntry(writeEntry(111, "first")));
    try testing.expect(batch.addEntry(deleteEntry(111)));
    try testing.expect(batch.addEntry(writeEntry(111, "second")));

    const wal_path = scratch_prefix ++ "replay_resurrect.wal";
    try writeWalFile(wal_path, &.{batch});
    defer os_compat.deleteFile(wal_path) catch {};

    var replayer = wildb_lib.snapshot.WALReplayer.init(alloc, wal_path);
    _ = try replayer.replayFromPosition(&store, 0, null);

    const record = try store.read(111) orelse return error.KeyLostDuringReplay;
    try testing.expectEqualStrings("second", record.getData());
}

test "delete appends a marker entry to the WAL" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var topology = wildb_lib.analyzeCacheTopology(alloc) catch return; // no /sys: skip
    defer topology.deinit();

    const capacity: u32 = 1024;
    var store = (try newStorage(alloc, &topology, capacity)) orelse return;
    defer store.deinit();

    // The durability thread is never started, so the ring keeps what the
    // storage appends and nothing else drains it.
    const wal_path = scratch_prefix ++ "append_delete.wal";
    defer os_compat.deleteFile(wal_path ++ ".000000") catch {};

    var durability = try wal.DurabilityManager.init(alloc, wal.DurabilityManager.Config.forStorage(wal_path, capacity));
    defer durability.deinit();
    store.enableDurability(&durability);

    try store.write(333, "value-c");
    try testing.expect(try store.delete(333));

    // appendAsync walks the rings round-robin, so the two entries landed in
    // two different rings. Collect every ring, as the durability thread does.
    var batch = wal.WALBatch.init(0, 1);
    var drained: u16 = 0;
    for (durability.tls_wals) |*ring| drained += ring.drainBatch(&batch);

    // The write and the delete are two entries. Before the fix there was one.
    try testing.expectEqual(@as(u16, 2), drained);

    try testing.expect(!batch.entries[0].isDelete());
    try testing.expectEqual(@as(u64, 333), batch.entries[0].getKey());
    try testing.expectEqualStrings("value-c", batch.entries[0].getData());

    try testing.expect(batch.entries[1].isValid());
    try testing.expect(batch.entries[1].isDelete());
    try testing.expectEqual(@as(u64, 333), batch.entries[1].getKey());

    // A marker must not carry a value: applyEntry reads the key and nothing else.
    try testing.expectEqual(@as(usize, 0), batch.entries[1].getData().len);
}

test "applyEntry applies writes, applies deletes, and ignores a delete for a missing key" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var topology = wildb_lib.analyzeCacheTopology(alloc) catch return; // no /sys: skip
    defer topology.deinit();

    var store = (try newStorage(alloc, &topology, 1024)) orelse return;
    defer store.deinit();

    // A delete for a key that is not there is not an error: a replica can
    // legitimately see one after a snapshot it already covers.
    try store.applyEntry(&deleteEntry(999));
    try testing.expectEqual(@as(u32, 0), store.getStats().total_count);

    try store.applyEntry(&writeEntry(999, "nina"));
    try testing.expectEqual(@as(u32, 1), store.getStats().total_count);

    try store.applyEntry(&deleteEntry(999));
    try testing.expectEqual(@as(?*const CacheLineRecord, null), try store.read(999));
    try testing.expectEqual(@as(u32, 0), store.getStats().total_count);

    // Applying a marker must not leave an occupied slot behind, or the table
    // would report a record that no key can reach.
    var occupied: u32 = 0;
    for (0..1024) |slot| {
        if (store.getRecordAtSlot(@intCast(slot))) |record| {
            if (record.isValid()) occupied += 1;
        }
    }
    try testing.expectEqual(@as(u32, 0), occupied);
}

test "restoring a marker is refused and restoring a present key does not inflate the count" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var topology = wildb_lib.analyzeCacheTopology(alloc) catch return; // no /sys: skip
    defer topology.deinit();

    var store = (try newStorage(alloc, &topology, 1024)) orelse return;
    defer store.deinit();

    // A snapshot is a copy of the live table, so a marker in one is corruption.
    // It must never be inserted: a marker reports itself valid, and an occupied
    // slot that no probe can reach is exactly the failure the delete fix removed.
    const marker = CacheLineRecord.initDelete(555);
    try store.restoreRecord(&marker);
    try testing.expectEqual(@as(?*const CacheLineRecord, null), try store.read(555));
    try testing.expectEqual(@as(u32, 0), store.getStats().total_count);

    // Restoring a key the table already holds is an update, not a new record.
    const record = CacheLineRecord.init(555, "restored") catch unreachable;
    try store.restoreRecord(&record);
    try store.restoreRecord(&record);
    try testing.expectEqual(@as(u32, 1), store.getStats().total_count);

    const found = try store.read(555) orelse return error.KeyLostOnRestore;
    try testing.expectEqualStrings("restored", found.getData());
}

test "a deleted key does not come back when the log is replayed into a full table" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var topology = wildb_lib.analyzeCacheTopology(alloc) catch return; // no /sys: skip
    defer topology.deinit();

    // Half full, so replayed entries collide and form runs: this is where a
    // marker applied as a write, or a hole left unclosed, shows up.
    const capacity: u32 = 256;
    var store = (try newStorage(alloc, &topology, capacity)) orelse return;
    defer store.deinit();

    var buf: [24]u8 = undefined;
    for (0..128) |i| {
        try store.write(i, std.fmt.bufPrint(&buf, "v{d}", .{i}) catch unreachable);
    }

    // A batch holds 63 entries, so the 64 deletes go in two batches and the
    // late write gets a third.
    var batches: [3]wal.WALBatch = .{
        wal.WALBatch.init(0, 1),
        wal.WALBatch.init(1, 1),
        wal.WALBatch.init(2, 1),
    };
    var deleted: usize = 0;
    for (0..128) |i| {
        if (i % 2 != 0) continue;
        const below = if (i < 64) @as(usize, 0) else @as(usize, 1);
        try testing.expect(batches[below].addEntry(deleteEntry(i)));
        deleted += 1;
    }
    try testing.expectEqual(@as(usize, 64), deleted);

    // And a fresh key arriving in the same log, so a delete and an insert of a
    // colliding key are settled in one pass.
    try testing.expect(batches[2].addEntry(writeEntry(1000, "late")));

    const wal_path = scratch_prefix ++ "replay_half_full.wal";
    try writeWalFile(wal_path, &batches);
    defer os_compat.deleteFile(wal_path) catch {};

    var replayer = wildb_lib.snapshot.WALReplayer.init(alloc, wal_path);
    _ = try replayer.replayFromPosition(&store, 0, null);

    var live: u32 = 0;
    for (0..128) |i| {
        const expected = std.fmt.bufPrint(&buf, "v{d}", .{i}) catch unreachable;
        if (i % 2 == 0) {
            try testing.expectEqual(@as(?*const CacheLineRecord, null), try store.read(i));
        } else {
            live += 1;
            const found = try store.read(i) orelse return error.KeyStrandedByDelete;
            try testing.expectEqualStrings(expected, found.getData());
        }
    }
    const late = try store.read(1000) orelse return error.KeyLostDuringReplay;
    try testing.expectEqualStrings("late", late.getData());

    live += 1;
    try testing.expectEqual(live, store.getStats().total_count);
}
