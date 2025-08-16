const std = @import("std");
const cache_topology = @import("cache_topology.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Test cache topology detection with CPUID
    var topology = try cache_topology.analyzeCacheTopology(allocator);
    defer topology.deinit();

    std.debug.print("Cache Topology Detection (CPUID-based):\n", .{});
    std.debug.print("- Total CPUs: {}\n", .{topology.total_cpus});
    std.debug.print("- Physical cores: {}\n", .{topology.total_physical_cores});
    std.debug.print("- Cache line size: {} bytes\n", .{topology.cache_line_size});
    std.debug.print("- Number of shards: {}\n", .{topology.shard_mappings.len});
    
    // Print L3 domain information
    for (topology.l3_domains, 0..) |domain, i| {
        std.debug.print("\nL3 Domain {}:\n", .{i});
        std.debug.print("- Cache size: {} KB\n", .{domain.cache_size_kb});
        std.debug.print("- CPU count: {}\n", .{domain.cpus.len});
        std.debug.print("- CPU list: {s}\n", .{domain.cpu_list_str});
        
        var physical_count: u32 = 0;
        var smt_count: u32 = 0;
        for (domain.cpus) |cpu| {
            if (cpu.is_smt_sibling) {
                smt_count += 1;
            } else {
                physical_count += 1;
            }
        }
        std.debug.print("- Physical cores: {}, SMT siblings: {}\n", .{physical_count, smt_count});
    }
    
    // Test shard mapping
    std.debug.print("\nShard Mappings:\n", .{});
    for (topology.shard_mappings, 0..) |mapping, i| {
        std.debug.print("- Shard {}: {} preferred CPUs\n", .{i, mapping.preferred_cpus.len});
    }
    
    std.debug.print("\nCache topology detection completed successfully!\n", .{});
}