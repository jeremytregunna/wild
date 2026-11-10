//! Routing a key to the table that owns it, and the reason routing is a pure
//! function of the key.
//!
//! A machine with two NUMA zones on one socket gets two tables, one per zone,
//! each with a thread pinned to its zone. A write then never crosses a zone
//! boundary: the thread and the memory are on the same side. This module is
//! only the routing rule. It holds no state and takes no locks, because three
//! separate callers must agree on it:
//!
//! 1. the request path, which sends a key to a table,
//! 2. WAL replay, which rebuilds the tables from the log after a crash,
//! 3. replica apply, which rebuilds them from a primary's stream.
//!
//! (2) and (3) see only a key. If routing depended on anything else — the
//! machine, the table count of the *sender*, the order records arrived in —
//! then a replica with a different zone count, or a node that recovered on
//! different hardware, would file a key in a table the lookup path never
//! searches. The key would be present and invisible. So routing is
//! `bucket(key) % table_count`: the bucket depends only on the key, and each
//! node applies its own table count to it. Two nodes with different counts
//! still agree on the bucket, and each is self-consistent.
//!
//! 256 buckets, not `table_count` directly: the bucket is the stable thing,
//! and a table is a bucket group. With two tables a bucket's table is 0 or 1,
//! and adding a third table later changes which table a bucket lands in, but
//! never which bucket a key belongs to.

const std = @import("std");

/// Buckets a key can fall into. Fixed, so the bucket of a key never depends on
/// the hardware it is computed on.
pub const bucket_count: u16 = 256;

/// The bucket a key belongs to: a pure function of the key, stable across
/// processes, machines, and table counts.
///
/// The key is usually already a hash (`WILD.hashKey` is Wyhash), but not
/// always: a caller can pass a plain integer id, and benchmarks pass
/// sequential ids. Taking the low byte of a sequential id would spread fine,
/// but the low byte of an id that is a multiple of 256 would not spread at
/// all, and the last mix costs two multiplications. `fmix64` from MurmurHash3
/// spreads both cases into the high and low bits before the byte is taken.
pub fn bucketOf(key: u64) u16 {
    return @truncate(fmix64(key) & 0xFF);
}

/// The table that owns a bucket, given how many tables this node has.
///
/// `table_count` must be at least 1. A node with one table takes every
/// bucket, which is the single-zone case and the fallback when the machine
/// cannot prove its zones.
pub fn tableOfBucket(bucket: u16, table_count: u16) u16 {
    std.debug.assert(table_count >= 1);
    return bucket % table_count;
}

/// The table that owns a key on this node. The request path and every
/// rebuilding path call this, and nothing else decides placement.
pub fn tableOfKey(key: u64, table_count: u16) u16 {
    return tableOfBucket(bucketOf(key), table_count);
}

/// MurmurHash3's 64-bit finalizer. A bijection on 64-bit values, so distinct
/// keys stay distinct, with no low-bit clustering for sequential input.
fn fmix64(value: u64) u64 {
    var h = value;
    h ^= h >> 33;
    h *%= 0xFF51AFD7ED558CCD;
    h ^= h >> 33;
    h *%= 0xC4CEB9FE1A85EC53;
    h ^= h >> 33;
    return h;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "a key's bucket depends only on the key" {
    const keys = [_]u64{ 0, 1, 2, 255, 256, 257, 1 << 32, ~@as(u64, 0), 0xDEADBEEF };

    for (keys) |key| {
        const bucket = bucketOf(key);
        try testing.expect(bucket < bucket_count);

        // No state, no memo, no counter: the same key asks the same question
        // twice and must get the same answer. Replay and replica apply rely
        // on this, and they have no shared state with the request path to
        // rely on instead.
        try testing.expectEqual(bucket, bucketOf(key));
    }
}

test "one table takes every key" {
    var key: u64 = 0;
    while (key < 4096) : (key += 1) {
        try testing.expectEqual(@as(u16, 0), tableOfKey(key, 1));
    }
}

test "sequential keys fill every bucket" {
    var seen = std.mem.zeroes([bucket_count]bool);
    var key: u64 = 0;
    while (key < 4096) : (key += 1) {
        seen[bucketOf(key)] = true;
    }

    var filled: usize = 0;
    for (seen) |hit| {
        if (hit) filled += 1;
    }
    try testing.expectEqual(@as(usize, bucket_count), filled);
}

test "keys a multiple of 256 apart still spread" {
    // The case a raw low byte would fail: every one of these keys has a zero
    // low byte. The finalizer has to move information down before the byte is
    // taken, or 64 sequential ids in this shape would all land in one bucket.
    var seen = std.mem.zeroes([bucket_count]bool);
    var step: u64 = 0;
    while (step < 256) : (step += 1) {
        seen[bucketOf(step << 8)] = true;
    }

    var filled: usize = 0;
    for (seen) |hit| {
        if (hit) filled += 1;
    }
    // Not a guarantee of a uniform hash, but 256 keys of this shape must not
    // collapse into one bucket, and the test is useless if they collapse into
    // ten.
    try testing.expect(filled > bucket_count / 2);
}

test "every key has exactly one table, whatever the count" {
    const counts = [_]u16{ 1, 2, 3, 4, 7, 16, bucket_count };
    const keys = [_]u64{ 0, 7, 42, 1 << 40, 0xFFFF_FFFF_FFFF_FFFF };

    for (counts) |count| {
        for (keys) |key| {
            const table = tableOfKey(key, count);
            try testing.expect(table < count);
            // Asking again cannot move the key: a second table would be a
            // key present twice, or a key the lookup never finds.
            try testing.expectEqual(table, tableOfKey(key, count));
        }
    }
}

test "keys land in every table, so no table is dead weight" {
    const counts = [_]u16{ 2, 4, 8 };
    for (counts) |count| {
        var seen = std.mem.zeroes([bucket_count]bool);
        var key: u64 = 0;
        while (key < 4096) : (key += 1) {
            seen[tableOfKey(key, count)] = true;
        }
        var filled: usize = 0;
        for (seen[0..count]) |hit| {
            if (hit) filled += 1;
        }
        try testing.expectEqual(@as(usize, count), filled);
    }
}

test "a node with a different table count still agrees on the bucket" {
    // The replica case. The primary has four tables, the replica two. They
    // disagree about which table slot owns the key, which is fine and
    // expected: each routes its own lookups the same way. What they must not
    // disagree about is the bucket, because that is the only thing a record
    // carries besides its key.
    var key: u64 = 0;
    while (key < 1024) : (key += 1) {
        const primary_bucket = bucketOf(key);
        const replica_bucket = bucketOf(key);
        try testing.expectEqual(primary_bucket, replica_bucket);

        // Each side is consistent with itself.
        try testing.expectEqual(tableOfBucket(primary_bucket, 4), tableOfKey(key, 4));
        try testing.expectEqual(tableOfBucket(replica_bucket, 2), tableOfKey(key, 2));
    }
}
