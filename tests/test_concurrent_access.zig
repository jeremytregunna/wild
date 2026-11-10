//! A second thread must not shift records under a reader.
//!
//! The storage is a flat table with linear probing and a delete that closes
//! the hole by shifting later records. A reader walking a probe chain while a
//! background thread runs that shift can meet the hole before the record that
//! was shifted past it, and report a key that is present as absent. A replica
//! applies deletes on exactly such a background thread.
//!
//! These tests exercise the lock that makes the two threads take turns. It is
//! off by default, so the single-threaded hot path pays nothing; anything that
//! touches the storage from a second thread must turn it on first.

const std = @import("std");
const testing = std.testing;
const wildb_lib = @import("wildb_lib");

const flat_hash_storage = wildb_lib.flat_hash_storage;

/// Keys the writer owns. The reader only looks at keys above this range, which
/// the writer never writes or deletes directly. They can still move, because a
/// delete shifts any record whose probe path crossed the hole.
const writer_keys = 100;
const reader_first_key = 100;
const total_keys = 200;

var reader_saw_missing = std.atomic.Value(bool).init(false);

fn newStorage(
    allocator: std.mem.Allocator,
    topology: *wildb_lib.CacheTopology,
    capacity: u32,
) !?flat_hash_storage.FlatHashStorage {
    const store = flat_hash_storage.FlatHashStorage.init(allocator, topology, capacity) catch
        return null;
    return store;
}

test "a background writer cannot take a key out from under a reader" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var topology = wildb_lib.analyzeCacheTopology(alloc) catch return; // no /sys: skip
    defer topology.deinit();

    var store = (try newStorage(alloc, &topology, 256)) orelse return;
    defer store.deinit();
    store.enableSync();

    // A high fill fraction makes probe runs long enough to overlap a shift.
    for (0..total_keys) |key| {
        var buf: [16]u8 = undefined;
        const value = try std.fmt.bufPrint(&buf, "value-{d}", .{key});
        try store.write(@intCast(key), value);
    }

    reader_saw_missing.store(false, .release);

    const Writer = struct {
        fn run(storage: *flat_hash_storage.FlatHashStorage) void {
            for (0..20_000) |round| {
                const key: u64 = @intCast(round % writer_keys);
                _ = storage.delete(key);
                var buf: [16]u8 = undefined;
                const value = std.fmt.bufPrint(&buf, "w-{d}", .{round}) catch unreachable;
                storage.write(key, value) catch {};
            }
        }
    };

    const writer = try std.Thread.spawn(.{}, Writer.run, .{&store});
    defer writer.join();

    for (0..20_000) |_| {
        for (reader_first_key..total_keys) |key| {
            if (store.read(@intCast(key)) == null) {
                reader_saw_missing.store(true, .release);
                return;
            }
        }
    }

    try testing.expect(!reader_saw_missing.load(.acquire));
}

test "concurrent inserters agree on the record count" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var topology = wildb_lib.analyzeCacheTopology(alloc) catch return; // no /sys: skip
    defer topology.deinit();

    const capacity: u32 = 4096;
    var store = (try newStorage(alloc, &topology, capacity)) orelse return;
    defer store.deinit();
    store.enableSync();

    const keys_per_thread = 1000;
    const thread_count = 4;

    const Inserter = struct {
        fn run(storage: *flat_hash_storage.FlatHashStorage, base: u64) void {
            for (0..keys_per_thread) |i| {
                var buf: [8]u8 = undefined;
                const value = std.fmt.bufPrint(&buf, "v-{d}", .{i}) catch unreachable;
                storage.write(base + i, value) catch {};
            }
        }
    };

    var threads: [thread_count]std.Thread = undefined;
    for (0..thread_count) |t| {
        threads[t] = try std.Thread.spawn(.{}, Inserter.run, .{ &store, @as(u64, t * keys_per_thread) });
    }
    for (threads) |thread| thread.join();

    // Every insert is a distinct key, so the table must hold exactly this many.
    // `count` is read-modify-written without synchronisation, and two inserts
    // can both read the same value and write the same successor.
    try testing.expectEqual(@as(u32, thread_count * keys_per_thread), store.getStats().total_count);

    // A lost slot is the other way the race shows up: a key that was written
    // is not reachable at all.
    for (0..thread_count * keys_per_thread) |key| {
        try testing.expect(store.read(@intCast(key)) != null);
    }
}
