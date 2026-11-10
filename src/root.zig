//! By convention, root.zig is the root source file when making a library. If
//! you are making an executable, the convention is to delete this file and
//! start with main.zig instead.
const std = @import("std");
const testing = std.testing;

// Re-export main WILD database interface
pub const WILD = @import("wild.zig").WILD;

// Re-export cache topology functionality
pub const cache_topology = @import("cache_topology.zig");
pub const CacheTopology = cache_topology.CacheTopology;
pub const CacheDomain = cache_topology.CacheDomain;
pub const ShardMapping = cache_topology.ShardMapping;
pub const CpuCore = cache_topology.CpuCore;
pub const analyzeCacheTopology = cache_topology.analyzeCacheTopology;

// Re-export flat hash storage
pub const flat_hash_storage = @import("flat_hash_storage.zig");
pub const FlatHashStorage = flat_hash_storage.FlatHashStorage;
pub const CacheLineRecord = flat_hash_storage.CacheLineRecord;

// Re-export the routing rule. WAL replay and replica apply have to route a
// key the same way the request path does, so it cannot live inside either.
pub const partition = @import("partition.zig");

// One table per NUMA zone, each with a pinned owner thread.
pub const table_set = @import("table_set.zig");
pub const TableSet = table_set.TableSet;

// Re-export the pieces an end-to-end test needs to start the server and talk
// to it with the real client.
pub const wild = @import("wild.zig");
pub const server = @import("server.zig");
pub const client = @import("client.zig");
pub const static_allocator = @import("static_allocator.zig");
pub const os_compat = @import("os_compat.zig");

// Re-export durability and recovery. A WAL entry is a mirror of a cache-line
// record, so a test that pins the entry format needs both sides of it: the
// writer in wal.zig and the reader in snapshot.zig.
pub const wal = @import("wal.zig");
pub const snapshot = @import("snapshot.zig");

// Nothing else references every module: the router is imported for replay
// and replica apply, and its own guarantees ("one key, one table") are only
// tested if the test runner reaches it.
test {
    testing.refAllDecls(@This());
}

test "cache topology analysis" {
    const allocator = std.testing.allocator;

    var topology = analyzeCacheTopology(allocator) catch {
        // If we can't analyze cache topology (e.g., running in CI without /sys/devices),
        // skip the test
        return;
    };
    defer topology.deinit();

    // Basic validation
    try testing.expect(topology.total_cpus > 0);
    try testing.expect(topology.shard_mappings.len > 0);

    // Test key hashing and shard mapping
    const test_key = "test_key";
    const hash = CacheTopology.hashKey(test_key);
    const shard = topology.getShardForData(hash);
    try testing.expect(shard.shard_id < topology.shard_mappings.len);

    // Test convenience method
    const shard2 = topology.getShardForKey(test_key);
    try testing.expect(shard.shard_id == shard2.shard_id);
}
