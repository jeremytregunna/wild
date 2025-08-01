const std = @import("std");
const fs = std.fs;

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
            const end = try std.fmt.parseInt(u32, range[dash_pos + 1..], 10);
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

fn detectSmtSiblings(allocator: std.mem.Allocator, cpu_id: u32) !u32 {
    const path = try std.fmt.allocPrint(allocator, "/sys/devices/system/cpu/cpu{d}/topology/thread_siblings_list", .{cpu_id});
    defer allocator.free(path);

    const file = fs.openFileAbsolute(path, .{}) catch {
        // If we can't read topology, assume no SMT
        return cpu_id;
    };
    defer file.close();

    var buf: [256]u8 = undefined;
    const bytes = try file.read(&buf);
    const siblings_str = std.mem.trim(u8, buf[0..bytes], " \n");

    // Parse to find the physical core (usually the lower numbered CPU)
    const cpus = try parseCpuList(allocator, siblings_str);
    defer allocator.free(cpus);

    var min_cpu = cpu_id;
    for (cpus) |cpu| {
        if (cpu < min_cpu) min_cpu = cpu;
    }

    return min_cpu;
}

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
              [edx] "={edx}" (edx_out)
            : [leaf] "{eax}" (@as(u32, 1)),
              [subleaf] "{ecx}" (@as(u32, 0))
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

pub fn analyzeCacheTopology(allocator: std.mem.Allocator) !CacheTopology {
    var l3_domains = std.ArrayList(CacheDomain).init(allocator);
    defer l3_domains.deinit();
    var l2_domains = std.ArrayList(CacheDomain).init(allocator);
    defer l2_domains.deinit();

    var max_cpu: u32 = 0;
    var seen_l3_domains = std.StringHashMap(void).init(allocator);
    defer {
        var it = seen_l3_domains.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
        }
        seen_l3_domains.deinit();
    }

    // First pass: discover all cache domains
    var cpu_id: u32 = 0;
    while (cpu_id < 256) : (cpu_id += 1) {
        const cpu_path = try std.fmt.allocPrint(allocator, "/sys/devices/system/cpu/cpu{d}", .{cpu_id});
        defer allocator.free(cpu_path);

        var cpu_dir = fs.openDirAbsolute(cpu_path, .{}) catch break;
        defer cpu_dir.close();

        max_cpu = cpu_id;

        const cache_path = try std.fmt.allocPrint(allocator, "{s}/cache", .{cpu_path});
        defer allocator.free(cache_path);

        var cache_dir = fs.openDirAbsolute(cache_path, .{}) catch continue;
        defer cache_dir.close();

        var index_id: u32 = 0;
        while (index_id < 10) : (index_id += 1) {
            const index_path = try std.fmt.allocPrint(allocator, "index{d}", .{index_id});
            defer allocator.free(index_path);

            var index_dir = cache_dir.openDir(index_path, .{}) catch continue;
            defer index_dir.close();

            // Read cache level
            var level_file = index_dir.openFile("level", .{}) catch continue;
            defer level_file.close();

            var level_buf: [10]u8 = undefined;
            const level_bytes = try level_file.read(&level_buf);
            const level = try std.fmt.parseInt(u8, std.mem.trim(u8, level_buf[0..level_bytes], " \n"), 10);

            if (level != 3) continue; // Only process L3 for now

            // Read cache size
            const size_file = index_dir.openFile("size", .{}) catch continue;
            defer size_file.close();

            var size_buf: [32]u8 = undefined;
            const size_bytes = try size_file.read(&size_buf);
            const size_str = std.mem.trim(u8, size_buf[0..size_bytes], " \n");

            var size_kb: u64 = 0;
            if (std.mem.endsWith(u8, size_str, "K")) {
                size_kb = try std.fmt.parseInt(u64, size_str[0..size_str.len-1], 10);
            } else if (std.mem.endsWith(u8, size_str, "M")) {
                const mb = try std.fmt.parseInt(u64, size_str[0..size_str.len-1], 10);
                size_kb = mb * 1024;
            }

            // Read shared CPU list
            const shared_file = index_dir.openFile("shared_cpu_list", .{}) catch continue;
            defer shared_file.close();

            var shared_buf: [256]u8 = undefined;
            const shared_bytes = try shared_file.read(&shared_buf);
            const shared_cpu_list = std.mem.trim(u8, shared_buf[0..shared_bytes], " \n");

            if (!seen_l3_domains.contains(shared_cpu_list)) {
                try seen_l3_domains.put(try allocator.dupe(u8, shared_cpu_list), {});

                const cpu_ids = try parseCpuList(allocator, shared_cpu_list);
                defer allocator.free(cpu_ids);

                // Create CpuCore structs with SMT detection
                var cpu_cores = try allocator.alloc(CpuCore, cpu_ids.len);
                var physical_cores_seen = std.AutoHashMap(u32, void).init(allocator);
                defer physical_cores_seen.deinit();

                for (cpu_ids, 0..) |id, i| {
                    const physical_core = try detectSmtSiblings(allocator, id);
                    const is_smt = physical_cores_seen.contains(physical_core) or (physical_core != id);
                    try physical_cores_seen.put(physical_core, {});

                    cpu_cores[i] = .{
                        .cpu_id = id,
                        .is_smt_sibling = is_smt,
                        .physical_core_id = physical_core,
                    };
                }

                try l3_domains.append(.{
                    .domain_id = @intCast(l3_domains.items.len),
                    .cache_size_kb = size_kb,
                    .cpus = cpu_cores,
                    .cpu_list_str = try allocator.dupe(u8, shared_cpu_list),
                });
            }
            break;
        }
    }

    // Count physical cores
    var all_physical_cores = std.AutoHashMap(u32, void).init(allocator);
    defer all_physical_cores.deinit();

    for (l3_domains.items) |domain| {
        for (domain.cpus) |cpu| {
            try all_physical_cores.put(cpu.physical_core_id, {});
        }
    }

    // Convert l3_domains to owned slice first
    const owned_l3_domains = try l3_domains.toOwnedSlice();
    const owned_l2_domains = try l2_domains.toOwnedSlice();

    // Create shard mappings with valid pointers
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
        .total_cpus = max_cpu + 1,
        .total_physical_cores = @intCast(all_physical_cores.count()),
        .shard_mappings = shard_mappings,
        .cache_line_size = detectCacheLineSize(),
        .allocator = allocator,
    };
}