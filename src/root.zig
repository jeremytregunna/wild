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



test "cache topology analysis" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

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
