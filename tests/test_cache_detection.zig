const std = @import("std");
const wildb_lib = @import("wildb_lib");
const cache_topology = wildb_lib.cache_topology;

pub fn main() !void {
    // Zig 0.16 removed std.heap.GeneralPurposeAllocator. This program is a
    // manual check, so a plain arena over the page allocator is enough.
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // Test cache topology detection with CPUID
    var topology = try cache_topology.analyzeCacheTopology(allocator);
    defer topology.deinit();

    std.debug.print("Cache Topology Detection (CPUID-based):\n", .{});
    std.debug.print("- Total CPUs: {}\n", .{topology.total_cpus});
    std.debug.print("- Physical cores: {}\n", .{topology.total_physical_cores});
    std.debug.print("- NUMA zones: {}\n", .{topology.numaNodeCount()});
    std.debug.print("- Cache line size: {} bytes\n", .{topology.cache_line_size});
    std.debug.print("- Number of shards: {}\n", .{topology.shard_mappings.len});

    // Print L3 domain information
    for (topology.l3_domains, 0..) |domain, i| {
        std.debug.print("\nL3 Domain {}:\n", .{i});
        std.debug.print("- NUMA zone: {?}\n", .{domain.node_id});
        std.debug.print("- Cache size: {} KB\n", .{domain.cache_size_kb});
        std.debug.print("- CPU count: {}\n", .{domain.cpus.len});
        std.debug.print("- CPU list: {s}\n", .{domain.cpu_list_str});

        // Counted from the accessor, not from the flags: a core whose only
        // allowed thread is a sibling still counts as one physical core.
        const physical_count = domain.getPhysicalCores().len;
        std.debug.print("- Physical cores: {}, SMT siblings: {}\n", .{ physical_count, domain.cpus.len - physical_count });
    }

    // Test shard mapping
    std.debug.print("\nShard Mappings:\n", .{});
    for (topology.shard_mappings, 0..) |mapping, i| {
        std.debug.print("- Shard {}: {} preferred CPUs (zone {?})\n", .{ i, mapping.preferred_cpus.len, mapping.cache_domain.node_id });
    }

    std.debug.print("\nCache topology detection completed successfully!\n", .{});
}
