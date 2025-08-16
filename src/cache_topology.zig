const std = @import("std");

pub const CpuCore = struct {
    cpu_id: u32,
    is_smt_sibling: bool,
    physical_core_id: u32,
};

pub const CacheDomain = struct {
    domain_id: u32,
    cache_size_kb: u64,
    cpus: []CpuCore,
    cpu_list_str: []const u8, // Original string like "0-3,32-35"

    pub fn getPhysicalCores(self: *const CacheDomain) []const CpuCore {
        var count: usize = 0;
        for (self.cpus) |cpu| {
            if (!cpu.is_smt_sibling) count += 1;
        }

        const result = self.cpus[0..count];
        return result;
    }

    pub fn containsCpu(self: *const CacheDomain, cpu_id: u32) bool {
        for (self.cpus) |cpu| {
            if (cpu.cpu_id == cpu_id) return true;
        }
        return false;
    }
};

pub const ShardMapping = struct {
    shard_id: u32,
    cache_domain: *const CacheDomain,
    preferred_cpus: []u32, // Subset of CPUs optimized for this shard

    pub fn getAffinityMask(self: *const ShardMapping, allocator: std.mem.Allocator) ![]u8 {
        // Create CPU affinity mask for sched_setaffinity
        const mask_size = (@as(usize, @intCast(self.cache_domain.cpus[self.cache_domain.cpus.len - 1].cpu_id)) + 8) / 8;
        var mask = try allocator.alloc(u8, mask_size);
        @memset(mask, 0);

        for (self.preferred_cpus) |cpu| {
            const byte_idx = cpu / 8;
            const bit_idx = @as(u3, @intCast(cpu % 8));
            mask[byte_idx] |= (@as(u8, 1) << bit_idx);
        }

        return mask;
    }
};

pub const CacheTopology = struct {
    l3_domains: []CacheDomain,
    l2_domains: []CacheDomain,
    total_cpus: u32,
    total_physical_cores: u32,
    shard_mappings: []ShardMapping,
    cache_line_size: u32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *CacheTopology) void {
        for (self.l3_domains) |domain| {
            self.allocator.free(domain.cpus);
            self.allocator.free(domain.cpu_list_str);
        }
        for (self.l2_domains) |domain| {
            self.allocator.free(domain.cpus);
            self.allocator.free(domain.cpu_list_str);
        }
        for (self.shard_mappings) |mapping| {
            self.allocator.free(mapping.preferred_cpus);
        }
        self.allocator.free(self.l3_domains);
        self.allocator.free(self.l2_domains);
        self.allocator.free(self.shard_mappings);
    }

    pub fn getShardForData(self: *const CacheTopology, data_hash: u64) *const ShardMapping {
        const shard_idx = data_hash % self.shard_mappings.len;
        return &self.shard_mappings[shard_idx];
    }

    pub fn getCpuShard(self: *const CacheTopology, cpu_id: u32) ?*const ShardMapping {
        for (self.shard_mappings) |*mapping| {
            if (mapping.cache_domain.containsCpu(cpu_id)) {
                return mapping;
            }
        }
        return null;
    }

    pub fn getShardCount(self: *const CacheTopology) usize {
        return self.shard_mappings.len;
    }

    pub fn getShardById(self: *const CacheTopology, shard_id: u32) ?*const ShardMapping {
        if (shard_id >= self.shard_mappings.len) return null;
        return &self.shard_mappings[shard_id];
    }

    pub fn hashKey(key: []const u8) u64 {
        return std.hash.Wyhash.hash(0, key);
    }

    pub fn getShardForKey(self: *const CacheTopology, key: []const u8) *const ShardMapping {
        const hash = hashKey(key);
        return self.getShardForData(hash);
    }
};

fn parseCpuList(allocator: std.mem.Allocator, cpu_list_str: []const u8) ![]u32 {
    var cpus = std.ArrayList(u32).init(allocator);
    defer cpus.deinit();

    var ranges = std.mem.tokenizeAny(u8, cpu_list_str, ",");
    while (ranges.next()) |range| {
        if (std.mem.indexOf(u8, range, "-")) |dash_pos| {
            const start = try std.fmt.parseInt(u32, range[0..dash_pos], 10);
            const end = try std.fmt.parseInt(u32, range[dash_pos + 1 ..], 10);
            var i = start;
            while (i <= end) : (i += 1) {
                try cpus.append(i);
            }
        } else {
            const cpu = try std.fmt.parseInt(u32, range, 10);
            try cpus.append(cpu);
        }
    }

    return cpus.toOwnedSlice();
}

const CpuTopologyInfo = struct {
    logical_cpu_bits: u8,
    core_bits: u8,
    package_bits: u8,
    apic_id: u32,
};

fn getCpuTopologyInfo() ?CpuTopologyInfo {
    const builtin = @import("builtin");
    
    if (builtin.cpu.arch != .x86_64) {
        return null;
    }

    var eax_out: u32 = undefined;
    var ebx_out: u32 = undefined;
    var ecx_out: u32 = undefined;
    var edx_out: u32 = undefined;

    // First check if CPUID leaf 0xB is supported
    asm volatile ("cpuid"
        : [eax] "={eax}" (eax_out),
          [ebx] "={ebx}" (ebx_out),
          [ecx] "={ecx}" (ecx_out),
          [edx] "={edx}" (edx_out),
        : [leaf] "{eax}" (@as(u32, 0)),
          [subleaf] "{ecx}" (@as(u32, 0)),
        : "memory"
    );
    
    // Check if we have enough CPUID leaves
    if (eax_out < 0xB) {
        return null;
    }
    
    // Get APIC ID from leaf 1
    asm volatile ("cpuid"
        : [eax] "={eax}" (eax_out),
          [ebx] "={ebx}" (ebx_out),
          [ecx] "={ecx}" (ecx_out),
          [edx] "={edx}" (edx_out),
        : [leaf] "{eax}" (@as(u32, 1)),
          [subleaf] "{ecx}" (@as(u32, 0)),
        : "memory"
    );
    
    const apic_id = (ebx_out >> 24) & 0xFF;
    
    // Use CPUID leaf 0xB for topology
    // ECX=0 for SMT level
    asm volatile ("cpuid"
        : [eax] "={eax}" (eax_out),
          [ebx] "={ebx}" (ebx_out),
          [ecx] "={ecx}" (ecx_out),
          [edx] "={edx}" (edx_out),
        : [leaf] "{eax}" (@as(u32, 0xB)),
          [subleaf] "{ecx}" (@as(u32, 0)),
        : "memory"
    );
    
    if ((ecx_out & 0xFF00) == 0) {
        // No topology information available
        return null;
    }
    
    const logical_cpu_bits = @as(u8, @intCast(eax_out & 0x1F));
    
    // ECX=1 for core level
    asm volatile ("cpuid"
        : [eax] "={eax}" (eax_out),
          [ebx] "={ebx}" (ebx_out),
          [ecx] "={ecx}" (ecx_out),
          [edx] "={edx}" (edx_out),
        : [leaf] "{eax}" (@as(u32, 0xB)),
          [subleaf] "{ecx}" (@as(u32, 1)),
        : "memory"
    );
    
    const core_and_logical_bits = @as(u8, @intCast(eax_out & 0x1F));
    const core_bits = core_and_logical_bits - logical_cpu_bits;
    
    return CpuTopologyInfo{
        .logical_cpu_bits = logical_cpu_bits,
        .core_bits = core_bits,
        .package_bits = 0, // Will be calculated based on actual system topology
        .apic_id = apic_id,
    };
}

fn getPhysicalCoreId(apic_id: u32, logical_cpu_bits: u8) u32 {
    // Remove the logical CPU bits to get the physical core ID
    return apic_id >> @as(u5, @intCast(logical_cpu_bits));
}

fn isSmtSibling(apic_id: u32, logical_cpu_bits: u8) bool {
    // If logical_cpu_bits > 0, then we have SMT
    // A CPU is an SMT sibling if it's not the first logical CPU in its core
    if (logical_cpu_bits == 0) return false;
    
    const logical_cpu_id = apic_id & ((@as(u32, 1) << @as(u5, @intCast(logical_cpu_bits))) - 1);
    return logical_cpu_id != 0;
}

const CacheInfo = struct {
    cache_type: u8, // 0=null, 1=data, 2=instruction, 3=unified
    cache_level: u8,
    cache_size_kb: u64,
    cache_line_size: u32,
    shared_cpu_list: []u32,
    allocator: std.mem.Allocator,
    
    pub fn deinit(self: *CacheInfo) void {
        self.allocator.free(self.shared_cpu_list);
    }
};

fn detectCacheLineSize() u32 {
    const builtin = @import("builtin");

    if (builtin.cpu.arch == .x86_64) {
        // Use CPUID to get cache line size
        // CPUID leaf 1, EBX bits 15:8 contain cache line size in 8-byte units
        var eax_out: u32 = undefined;
        var ebx_out: u32 = undefined;
        var ecx_out: u32 = undefined;
        var edx_out: u32 = undefined;

        // CPUID leaf 1
        asm volatile ("cpuid"
            : [eax] "={eax}" (eax_out),
              [ebx] "={ebx}" (ebx_out),
              [ecx] "={ecx}" (ecx_out),
              [edx] "={edx}" (edx_out),
            : [leaf] "{eax}" (@as(u32, 1)),
              [subleaf] "{ecx}" (@as(u32, 0)),
            : "memory"
        );

        // Extract cache line size from EBX bits 15:8
        const cache_line_units = (ebx_out >> 8) & 0xFF;
        const cache_line_size = cache_line_units * 8;

        // Validate reasonable cache line size
        return switch (cache_line_size) {
            16, 32, 64, 128, 256 => cache_line_size,
            else => 64, // Default to 64 if unexpected value
        };
    } else {
        // For non-x86 architectures, use common default
        return 64;
    }
}

fn detectCacheInfoCpuid(allocator: std.mem.Allocator) ![]CacheInfo {
    const builtin = @import("builtin");
    
    if (builtin.cpu.arch != .x86_64) {
        return &[_]CacheInfo{};
    }
    
    var cache_list = std.ArrayList(CacheInfo).init(allocator);
    defer cache_list.deinit();
    
    const topo_info = getCpuTopologyInfo();
    
    var eax_out: u32 = undefined;
    var ebx_out: u32 = undefined;
    var ecx_out: u32 = undefined;
    var edx_out: u32 = undefined;
    
    // Check CPU vendor to determine which CPUID method to use
    asm volatile ("cpuid"
        : [eax] "={eax}" (eax_out),
          [ebx] "={ebx}" (ebx_out),
          [ecx] "={ecx}" (ecx_out),
          [edx] "={edx}" (edx_out),
        : [leaf] "{eax}" (@as(u32, 0)),
          [subleaf] "{ecx}" (@as(u32, 0)),
        : "memory"
    );
    
    const max_std_function = eax_out;
    
    // Check for AMD by looking at vendor string
    const is_amd = (ebx_out == 0x68747541) and (edx_out == 0x69746E65) and (ecx_out == 0x444D4163); // "AuthenticAMD"
    
    if (is_amd) {
        // Check extended CPUID functions for AMD
        asm volatile ("cpuid"
            : [eax] "={eax}" (eax_out),
              [ebx] "={ebx}" (ebx_out),
              [ecx] "={ecx}" (ecx_out),
              [edx] "={edx}" (edx_out),
            : [leaf] "{eax}" (@as(u32, 0x80000000)),
              [subleaf] "{ecx}" (@as(u32, 0)),
            : "memory"
        );
        
        const max_extended = eax_out;
        
        // Use AMD CPUID 0x8000001D for newer CPUs
        if (max_extended >= 0x8000001D) {
            var cache_index: u32 = 0;
            while (cache_index < 10) : (cache_index += 1) {
                asm volatile ("cpuid"
                    : [eax] "={eax}" (eax_out),
                      [ebx] "={ebx}" (ebx_out),
                      [ecx] "={ecx}" (ecx_out),
                      [edx] "={edx}" (edx_out),
                    : [leaf] "{eax}" (@as(u32, 0x8000001D)),
                      [subleaf] "{ecx}" (cache_index),
                    : "memory"
                );
                
                const cache_type = eax_out & 0x1F;
                if (cache_type == 0) break; // No more cache levels
                
                const cache_level = @as(u8, @intCast((eax_out >> 5) & 0x7));
                const line_size = @as(u32, @intCast((ebx_out & 0xFFF) + 1));
                const ways = @as(u32, @intCast(((ebx_out >> 22) & 0x3FF) + 1));
                const partitions = @as(u32, @intCast(((ebx_out >> 12) & 0x3FF) + 1));
                const sets = @as(u32, @intCast(ecx_out + 1));
                
                const cache_size = (ways * partitions * line_size * sets) / 1024; // Convert to KB
                
                // For AMD, determine CPU sharing based on cache level
                // Get total CPU count from CPUID 0xB
                var total_logical_cpus: u32 = 16; // fallback
                if (topo_info) |_| {
                    var cpuid_eax: u32 = undefined;
                    var cpuid_ebx: u32 = undefined;
                    var cpuid_ecx: u32 = undefined;
                    var cpuid_edx: u32 = undefined;
                    
                    asm volatile ("cpuid"
                        : [eax] "={eax}" (cpuid_eax),
                          [ebx] "={ebx}" (cpuid_ebx),
                          [ecx] "={ecx}" (cpuid_ecx),
                          [edx] "={edx}" (cpuid_edx),
                        : [leaf] "{eax}" (@as(u32, 0xB)),
                          [subleaf] "{ecx}" (@as(u32, 1)),
                        : "memory"
                    );
                    
                    total_logical_cpus = cpuid_ebx & 0xFFFF;
                }
                
                const shared_cpu_count = switch (cache_level) {
                    1, 2 => if (topo_info) |info| @as(u32, 1) << @as(u5, @intCast(info.logical_cpu_bits)) else 2, // Per core
                    3 => total_logical_cpus, // Shared across all CPUs
                    else => 1,
                };
                
                var shared_cpus = try allocator.alloc(u32, shared_cpu_count);
                for (0..shared_cpu_count) |i| {
                    shared_cpus[i] = @as(u32, @intCast(i));
                }
                
                try cache_list.append(CacheInfo{
                    .cache_type = @as(u8, @intCast(cache_type)),
                    .cache_level = cache_level,
                    .cache_size_kb = cache_size,
                    .cache_line_size = line_size,
                    .shared_cpu_list = shared_cpus,
                    .allocator = allocator,
                });
            }
        }
    } else {
        // Intel path - use CPUID leaf 4
        if (max_std_function >= 4) {
            var cache_index: u32 = 0;
            while (cache_index < 10) : (cache_index += 1) {
                asm volatile ("cpuid"
                    : [eax] "={eax}" (eax_out),
                      [ebx] "={ebx}" (ebx_out),
                      [ecx] "={ecx}" (ecx_out),
                      [edx] "={edx}" (edx_out),
                    : [leaf] "{eax}" (@as(u32, 4)),
                      [subleaf] "{ecx}" (cache_index),
                    : "memory"
                );
                
                const cache_type = eax_out & 0x1F;
                if (cache_type == 0) break; // No more cache levels
                
                const cache_level = @as(u8, @intCast((eax_out >> 5) & 0x7));
                const line_size = @as(u32, @intCast((ebx_out & 0xFFF) + 1));
                const ways = @as(u32, @intCast(((ebx_out >> 22) & 0x3FF) + 1));
                const partitions = @as(u32, @intCast(((ebx_out >> 12) & 0x3FF) + 1));
                const sets = @as(u32, @intCast(ecx_out + 1));
                
                const cache_size = (ways * partitions * line_size * sets) / 1024; // Convert to KB
                
                // Determine which CPUs share this cache
                const shared_cores = @as(u32, @intCast(((eax_out >> 14) & 0xFFF) + 1));
                const logical_cpus_per_core = if (topo_info) |info| @as(u32, 1) << @as(u5, @intCast(info.logical_cpu_bits)) else 1;
                const total_logical_cpus = shared_cores * logical_cpus_per_core;
                
                var shared_cpus = try allocator.alloc(u32, total_logical_cpus);
                for (0..total_logical_cpus) |i| {
                    shared_cpus[i] = @as(u32, @intCast(i));
                }
                
                try cache_list.append(CacheInfo{
                    .cache_type = @as(u8, @intCast(cache_type)),
                    .cache_level = cache_level,
                    .cache_size_kb = cache_size,
                    .cache_line_size = line_size,
                    .shared_cpu_list = shared_cpus,
                    .allocator = allocator,
                });
            }
        }
    }
    
    return try cache_list.toOwnedSlice();
}

pub fn analyzeCacheTopology(allocator: std.mem.Allocator) !CacheTopology {
    var l3_domains = std.ArrayList(CacheDomain).init(allocator);
    defer l3_domains.deinit();
    var l2_domains = std.ArrayList(CacheDomain).init(allocator);
    defer l2_domains.deinit();

    // Get CPU topology information using CPUID
    const topo_info = getCpuTopologyInfo();
    
    // Determine number of logical CPUs and physical cores
    var total_logical_cpus: u32 = 1;
    var total_physical_cores: u32 = 1;
    
    if (topo_info) |info| {
        // For single-package systems, just use the total count from CPUID leaf 0xB
        // Get the actual total logical CPU count from CPUID
        var eax_out: u32 = undefined;
        var ebx_out: u32 = undefined;
        var ecx_out: u32 = undefined;
        var edx_out: u32 = undefined;
        
        // Core level (ECX=1) gives us the total logical CPU count
        asm volatile ("cpuid"
            : [eax] "={eax}" (eax_out),
              [ebx] "={ebx}" (ebx_out),
              [ecx] "={ecx}" (ecx_out),
              [edx] "={edx}" (edx_out),
            : [leaf] "{eax}" (@as(u32, 0xB)),
              [subleaf] "{ecx}" (@as(u32, 1)),
            : "memory"
        );
        
        total_logical_cpus = ebx_out & 0xFFFF;
        
        // Calculate physical cores: total logical CPUs / threads per core
        const logical_cpus_per_core = @as(u32, 1) << @as(u5, @intCast(info.logical_cpu_bits));
        total_physical_cores = total_logical_cpus / logical_cpus_per_core;
    } else {
        // Fallback: try to detect CPU count using available processors
        // For now, assume single core system
        total_logical_cpus = 1;
        total_physical_cores = 1;
    }
    
    // Get cache information using CPUID
    const cache_infos = detectCacheInfoCpuid(allocator) catch &[_]CacheInfo{};
    defer {
        for (cache_infos) |*info| {
            var mutable_info = info.*;
            mutable_info.deinit();
        }
        allocator.free(cache_infos);
    }
    
    // Process L3 cache domains
    for (cache_infos) |cache_info| {
        if (cache_info.cache_level == 3 and (cache_info.cache_type == 1 or cache_info.cache_type == 3)) {
            // Create CpuCore structs with SMT detection
            var cpu_cores = try allocator.alloc(CpuCore, cache_info.shared_cpu_list.len);
            
            for (cache_info.shared_cpu_list, 0..) |cpu_id, i| {
                const physical_core_id = if (topo_info) |info| 
                    getPhysicalCoreId(cpu_id, info.logical_cpu_bits) 
                else 
                    cpu_id;
                    
                const is_smt = if (topo_info) |info| 
                    isSmtSibling(cpu_id, info.logical_cpu_bits) 
                else 
                    false;
                
                cpu_cores[i] = .{
                    .cpu_id = cpu_id,
                    .is_smt_sibling = is_smt,
                    .physical_core_id = physical_core_id,
                };
            }
            
            // Create CPU list string for compatibility
            var cpu_list_str = std.ArrayList(u8).init(allocator);
            defer cpu_list_str.deinit();
            
            for (cache_info.shared_cpu_list, 0..) |cpu_id, i| {
                if (i > 0) try cpu_list_str.append(',');
                try std.fmt.format(cpu_list_str.writer(), "{d}", .{cpu_id});
            }
            
            try l3_domains.append(.{
                .domain_id = @intCast(l3_domains.items.len),
                .cache_size_kb = cache_info.cache_size_kb,
                .cpus = cpu_cores,
                .cpu_list_str = try cpu_list_str.toOwnedSlice(),
            });
        }
    }
    
    // If no L3 caches found via CPUID, create a single domain with all CPUs
    if (l3_domains.items.len == 0) {
        var cpu_cores = try allocator.alloc(CpuCore, total_logical_cpus);
        
        for (0..total_logical_cpus) |i| {
            const cpu_id = @as(u32, @intCast(i));
            const physical_core_id = if (topo_info) |info| 
                getPhysicalCoreId(cpu_id, info.logical_cpu_bits) 
            else 
                cpu_id;
                
            const is_smt = if (topo_info) |info| 
                isSmtSibling(cpu_id, info.logical_cpu_bits) 
            else 
                false;
            
            cpu_cores[i] = .{
                .cpu_id = cpu_id,
                .is_smt_sibling = is_smt,
                .physical_core_id = physical_core_id,
            };
        }
        
        // Create CPU list string
        var cpu_list_str = std.ArrayList(u8).init(allocator);
        defer cpu_list_str.deinit();
        
        for (0..total_logical_cpus) |i| {
            if (i > 0) try cpu_list_str.append(',');
            try std.fmt.format(cpu_list_str.writer(), "{d}", .{i});
        }
        
        try l3_domains.append(.{
            .domain_id = 0,
            .cache_size_kb = 8192, // Default 8MB L3 cache assumption
            .cpus = cpu_cores,
            .cpu_list_str = try cpu_list_str.toOwnedSlice(),
        });
    }

    // Convert to owned slices
    const owned_l3_domains = try l3_domains.toOwnedSlice();
    const owned_l2_domains = try l2_domains.toOwnedSlice();

    // Create shard mappings
    var shard_mappings = try allocator.alloc(ShardMapping, owned_l3_domains.len);
    for (owned_l3_domains, 0..) |*domain, i| {
        // Select preferred CPUs (physical cores first, then some SMT if needed)
        var preferred = std.ArrayList(u32).init(allocator);

        // Add all physical cores first
        for (domain.cpus) |cpu| {
            if (!cpu.is_smt_sibling) {
                try preferred.append(cpu.cpu_id);
            }
        }

        // If we have fewer than 4 physical cores, add some SMT siblings
        if (preferred.items.len < 4) {
            for (domain.cpus) |cpu| {
                if (cpu.is_smt_sibling and preferred.items.len < 4) {
                    try preferred.append(cpu.cpu_id);
                }
            }
        }

        shard_mappings[i] = .{
            .shard_id = @intCast(i),
            .cache_domain = domain,
            .preferred_cpus = try preferred.toOwnedSlice(),
        };
    }

    return CacheTopology{
        .l3_domains = owned_l3_domains,
        .l2_domains = owned_l2_domains,
        .total_cpus = total_logical_cpus,
        .total_physical_cores = total_physical_cores,
        .shard_mappings = shard_mappings,
        .cache_line_size = detectCacheLineSize(),
        .allocator = allocator,
    };
}
