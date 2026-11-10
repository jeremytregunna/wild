//! The WAL has to keep three promises that no format test covers:
//!
//! 1. Entries from one thread keep their order. Replay applies them in file
//!    order, so a ring assignment that spreads one key's history across rings
//!    can replay an older operation last.
//! 2. `stop` flushes what is still in the rings. A clean shutdown that drops
//!    the tail loses acknowledged writes just as surely as a crash.
//! 3. An entry being replayed or applied from a replica is not logged again.
//!    Otherwise replay writes the log it is reading, and a replica logs data
//!    the primary already logged.
//!
//! A fourth test covers reopening an existing file: the manager must continue
//! at the end of the log instead of writing over it from offset zero.

const std = @import("std");
const testing = std.testing;
const wildb_lib = @import("wildb_lib");

const flat_hash_storage = wildb_lib.flat_hash_storage;
const CacheLineRecord = wildb_lib.CacheLineRecord;
const wal = wildb_lib.wal;
const os_compat = wildb_lib.os_compat;

/// Scratch space inside the build cache, so a failed run leaves nothing in the
/// working tree. The directory already exists.
const scratch_prefix = ".zig-cache/wild_wal_guarantees_";

fn newStorage(
    allocator: std.mem.Allocator,
    topology: *wildb_lib.CacheTopology,
    capacity: u32,
) !?flat_hash_storage.FlatHashStorage {
    const store = flat_hash_storage.FlatHashStorage.init(allocator, topology, capacity) catch
        return null;
    return store;
}

fn writeEntry(key: u64, value: []const u8) wal.WALEntry {
    const record = CacheLineRecord.init(key, value) catch unreachable;
    return wal.WALEntry.fromCacheLineRecord(&record);
}

fn deleteEntry(key: u64) wal.WALEntry {
    const marker = CacheLineRecord.initDelete(key);
    return wal.WALEntry.fromCacheLineRecord(&marker);
}

/// Drain every ring in the order the durability thread drains them, and append
/// the entries to `out` in that order. The thread is never started in these
/// tests, so nothing else is consuming a ring.
fn drainAll(
    allocator: std.mem.Allocator,
    durability: *wal.DurabilityManager,
    out: *std.ArrayList(wal.WALEntry),
) !void {
    var batch = wal.WALBatch.init(0, 1);
    for (durability.tls_wals) |*ring| {
        while (true) {
            batch.entry_count = 0;
            const n = ring.drainBatch(&batch);
            if (n == 0) break;
            for (batch.entries[0..n]) |entry| try out.append(allocator, entry);
        }
    }
}

test "entries from one thread keep the order they were appended in" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var topology = wildb_lib.analyzeCacheTopology(alloc) catch return; // no /sys: skip
    defer topology.deinit();

    const capacity: u32 = 1024;
    var store = (try newStorage(alloc, &topology, capacity)) orelse return;
    defer store.deinit();

    const wal_path = scratch_prefix ++ "order.wal";
    defer os_compat.deleteFile(wal_path ++ ".000000") catch {};

    var durability = try wal.DurabilityManager.init(alloc, wal.DurabilityManager.Config.forStorage(wal_path, capacity));
    defer durability.deinit();
    store.enableDurability(&durability);
    defer store.disableDurability();

    // Five operations on one key. The last one, a write of "v4", must win.
    // Round-robin ring assignment splits this history across four rings, and
    // the durability thread drains ring 0 first, so "v4" would be replayed
    // before the delete in ring 3 and the key would come back absent.
    try store.write(111, "v1");
    try store.write(111, "v2");
    try store.write(111, "v3");
    try testing.expect(store.delete(111));
    try store.write(111, "v4");

    var entries: std.ArrayList(wal.WALEntry) = .empty;
    defer entries.deinit(alloc);
    try drainAll(alloc, &durability, &entries);

    // Replay in exactly the order the log would yield.
    var replay = (try newStorage(alloc, &topology, capacity)) orelse return;
    defer replay.deinit();
    for (entries.items) |*entry| try replay.applyEntry(entry);

    const record = replay.read(111) orelse return error.KeyLostDuringReplay;
    try testing.expectEqualStrings("v4", record.getData());
}

test "stop flushes entries still waiting in the rings" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var topology = wildb_lib.analyzeCacheTopology(alloc) catch return; // no /sys: skip
    defer topology.deinit();

    const capacity: u32 = 1024;
    var store = (try newStorage(alloc, &topology, capacity)) orelse return;
    defer store.deinit();

    const wal_path = scratch_prefix ++ "flush.wal";
    const first_file = wal_path ++ ".000000";
    defer os_compat.deleteFile(first_file) catch {};

    var durability = try wal.DurabilityManager.init(alloc, wal.DurabilityManager.Config.forStorage(wal_path, capacity));
    defer durability.deinit();
    store.enableDurability(&durability);
    defer store.disableDurability();

    try store.write(4242, "must-reach-disk");

    // No durability thread was started. A clean stop must flush the ring
    // itself; before this, `stop` returned immediately because there was no
    // thread to join, and the entry stayed in memory.
    durability.stop();

    const file = try os_compat.openFile(first_file, .{ .mode = .read_only });
    defer file.close();

    const size = try file.getEndPos();
    try testing.expectEqual(@as(u64, @sizeOf(wal.WALBatch)), size);

    var batch: wal.WALBatch = undefined;
    try testing.expectEqual(@as(usize, @sizeOf(wal.WALBatch)), try file.readAll(std.mem.asBytes(&batch)));
    try testing.expectEqual(@as(u16, 1), batch.entry_count);
    try testing.expectEqual(@as(u64, 4242), batch.entries[0].getKey());
    try testing.expectEqualStrings("must-reach-disk", batch.entries[0].getData());
}

test "applying a log entry does not append it to the log again" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var topology = wildb_lib.analyzeCacheTopology(alloc) catch return; // no /sys: skip
    defer topology.deinit();

    const capacity: u32 = 1024;
    var store = (try newStorage(alloc, &topology, capacity)) orelse return;
    defer store.deinit();

    const wal_path = scratch_prefix ++ "apply.wal";
    defer os_compat.deleteFile(wal_path ++ ".000000") catch {};

    var durability = try wal.DurabilityManager.init(alloc, wal.DurabilityManager.Config.forStorage(wal_path, capacity));
    defer durability.deinit();
    store.enableDurability(&durability);
    defer store.disableDurability();

    // This is what WAL replay and a replica both do.
    const entry = writeEntry(9, "from-the-log");
    try store.applyEntry(&entry);

    // The apply must not create a new entry to be written to disk.
    var entries: std.ArrayList(wal.WALEntry) = .empty;
    defer entries.deinit(alloc);
    try drainAll(alloc, &durability, &entries);

    try testing.expectEqual(@as(usize, 0), entries.items.len);
    const record = store.read(9) orelse return error.EntryNotApplied;
    try testing.expectEqualStrings("from-the-log", record.getData());
}

test "a reopened WAL continues at the end of the existing log" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const wal_path = scratch_prefix ++ "reopen.wal";
    const first_file = wal_path ++ ".000000";
    defer os_compat.deleteFile(first_file) catch {};

    // Put two complete batches on disk, as an earlier run would have.
    var batches: [2]wal.WALBatch = .{
        wal.WALBatch.init(0, 1),
        wal.WALBatch.init(1, 1),
    };
    try testing.expect(batches[0].addEntry(writeEntry(1, "one")));
    try testing.expect(batches[1].addEntry(writeEntry(2, "two")));

    {
        const file = try os_compat.createFile(first_file, .{ .truncate = true });
        defer file.close();
        for (&batches) |*batch| try file.writeAll(std.mem.asBytes(batch));
    }

    var durability = try wal.DurabilityManager.init(alloc, wal.DurabilityManager.Config.forStorage(wal_path, 1024));
    defer durability.deinit();

    // The next batch written must not overwrite batch 0. It goes after the
    // two existing batches, and its id follows on from them.
    try testing.expectEqual(@as(u64, 2 * @sizeOf(wal.WALBatch)), durability.current_wal_offset.load(.acquire));
    try testing.expectEqual(@as(u64, 2), durability.batches_written.load(.acquire));
}

test "rotation records the batch range each file holds" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const wal_path = scratch_prefix ++ "rotate.wal";
    defer os_compat.deleteFile(wal_path ++ ".000000") catch {};
    defer os_compat.deleteFile(wal_path ++ ".000001") catch {};
    defer os_compat.deleteFile(wal_path ++ ".000002") catch {};

    var durability = try wal.DurabilityManager.init(alloc, wal.DurabilityManager.Config.forStorage(wal_path, 1024));
    defer durability.deinit();

    // Batch 0 filled file 0; batch 1 goes to file 1.
    _ = durability.batches_written.fetchAdd(1, .monotonic);
    durability.current_wal_offset.store(@sizeOf(wal.WALBatch), .monotonic);
    try durability.rotateWALFile(1);

    try testing.expectEqual(@as(u64, 0), durability.wal_files.items[0].end_batch_id);
    try testing.expectEqual(@as(u64, 1), durability.wal_files.items[1].start_batch_id);

    // The previous file's end must not claim the batch that lives in the next
    // file, or catch-up reads the wrong slot from the wrong file.
    _ = durability.batches_written.fetchAdd(1, .monotonic);
    durability.current_wal_offset.store(@sizeOf(wal.WALBatch), .monotonic);
    try durability.rotateWALFile(2);

    try testing.expectEqual(@as(u64, 1), durability.wal_files.items[1].end_batch_id);
    try testing.expectEqual(@as(u64, 2), durability.wal_files.items[2].start_batch_id);
}

test "retention never unlinks the file being written" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const wal_path = scratch_prefix ++ "retention.wal";
    defer os_compat.deleteFile(wal_path ++ ".000000") catch {};
    defer os_compat.deleteFile(wal_path ++ ".000001") catch {};
    defer os_compat.deleteFile(wal_path ++ ".000002") catch {};

    var durability = try wal.DurabilityManager.init(alloc, wal.DurabilityManager.Config.forStorage(wal_path, 1024));
    defer durability.deinit();
    durability.config.segment_size_mb = 1;
    durability.config.min_wal_size_mb = 0;
    durability.config.max_wal_size_mb = 1;

    _ = durability.batches_written.fetchAdd(1, .monotonic);
    durability.current_wal_offset.store(@sizeOf(wal.WALBatch), .monotonic);
    try durability.rotateWALFile(1);
    _ = durability.batches_written.fetchAdd(1, .monotonic);
    durability.current_wal_offset.store(@sizeOf(wal.WALBatch), .monotonic);
    try durability.rotateWALFile(2);

    // The active file is the last one. Unlinking it would leave writes going
    // to an inode nothing can open by name.
    const active = durability.wal_files.items[durability.wal_files.items.len - 1].file_path;
    const file = try os_compat.openFile(active, .{ .mode = .read_only });
    file.close();
}

test "the active segment is readable by replica catch-up after a flush" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var topology = wildb_lib.analyzeCacheTopology(alloc) catch return; // no /sys: skip
    defer topology.deinit();

    const capacity: u32 = 1024;
    var store = (try newStorage(alloc, &topology, capacity)) orelse return;
    defer store.deinit();

    const wal_path = scratch_prefix ++ "active-read.wal";
    defer os_compat.deleteFile(wal_path ++ ".000000") catch {};

    var durability = try wal.DurabilityManager.init(alloc, wal.DurabilityManager.Config.forStorage(wal_path, capacity));
    defer durability.deinit();
    store.enableDurability(&durability);
    defer store.disableDurability();

    try store.write(77, "still-in-the-active-segment");
    durability.stop();

    // The segment has never been rotated, so its stored end is start - 1.
    // Catch-up must use the completed-batch watermark instead.
    const batch = durability.readBatch(0) orelse return error.ActiveSegmentNotReadable;
    try testing.expectEqual(@as(u64, 0), batch.batch_id);
    try testing.expectEqual(@as(u16, 1), batch.entry_count);
    try testing.expectEqualStrings("still-in-the-active-segment", batch.entries[0].getData());
}

test "retention keeps the segment a connected replica still needs" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const wal_path = scratch_prefix ++ "pin.wal";
    defer os_compat.deleteFile(wal_path ++ ".000000") catch {};
    defer os_compat.deleteFile(wal_path ++ ".000001") catch {};
    defer os_compat.deleteFile(wal_path ++ ".000002") catch {};

    var durability = try wal.DurabilityManager.init(alloc, wal.DurabilityManager.Config.forStorage(wal_path, 1024));
    defer durability.deinit();
    durability.config.segment_size_mb = 1;
    durability.config.min_wal_size_mb = 0;
    durability.config.max_wal_size_mb = 1024;

    // The replica has consumed batch 0, so the first batch it still needs is
    // batch 1. The segment ending at 0 is safe to remove.
    durability.setRetainFromBatchId(1);
    _ = durability.batches_written.fetchAdd(1, .monotonic);
    durability.current_wal_offset.store(@sizeOf(wal.WALBatch), .monotonic);
    try durability.rotateWALFile(1);

    try testing.expectEqual(@as(usize, 1), durability.wal_files.items.len);

    // Now segment 1 holds batch 1, which the replica still needs. It must
    // survive even though the size floor is zero.
    _ = durability.batches_written.fetchAdd(1, .monotonic);
    durability.current_wal_offset.store(@sizeOf(wal.WALBatch), .monotonic);
    try durability.rotateWALFile(2);

    try testing.expectEqual(@as(usize, 2), durability.wal_files.items.len);
    try testing.expectEqual(@as(u64, 1), durability.wal_files.items[0].end_batch_id);
    try testing.expectEqual(@as(u64, 2), durability.wal_files.items[1].start_batch_id);
}

test "the hard size cap removes a segment even when a replica needs it" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const wal_path = scratch_prefix ++ "cap.wal";
    defer os_compat.deleteFile(wal_path ++ ".000000") catch {};
    defer os_compat.deleteFile(wal_path ++ ".000001") catch {};

    var durability = try wal.DurabilityManager.init(alloc, wal.DurabilityManager.Config.forStorage(wal_path, 1024));
    defer durability.deinit();
    durability.config.segment_size_mb = 1;
    durability.config.min_wal_size_mb = 0;
    durability.config.max_wal_size_mb = 1;

    // The replica pins batch 0, but the old segment is already 2MB and the cap
    // is 1MB. Disk wins; the replica will bootstrap on its next catch-up.
    durability.setRetainFromBatchId(0);
    _ = durability.batches_written.fetchAdd(1, .monotonic);
    durability.current_wal_offset.store(2 * 1024 * 1024, .monotonic);
    try durability.rotateWALFile(1);

    try testing.expectEqual(@as(usize, 1), durability.wal_files.items.len);
    try testing.expectEqual(@as(u64, 1), durability.wal_files.items[0].start_batch_id);
}
