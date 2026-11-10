//! CPU topology: which logical CPUs share a core, which share a cache, and
//! which NUMA zone that cache belongs to.
//!
//! Every number below comes from CPUID, run once on every CPU this process may
//! use. No file is read and no sysfs string is parsed. The one OS call it needs
//! is CPU affinity, because a CPU only answers CPUID about itself: the asking
//! thread has to run there first. Only CPUs the process already allows are
//! asked, and the original affinity mask is restored before
//! `analyzeCacheTopology` returns.
//!
//! This reports zones and their caches. It does not place memory on a zone and
//! it does not pin threads; that is the caller's decision, and the data needed
//! to make it is all here.

const std = @import("std");
const builtin = @import("builtin");
const os_compat = @import("os_compat.zig");

/// One logical CPU inside a cache domain.
pub const CpuCore = struct {
    /// The OS's own numbering, which is the bit index in an affinity mask. It
    /// is not the APIC id; on most AMD parts those two disagree.
    cpu_id: u32,
    /// A second thread of a core that runs SMT.
    is_smt_sibling: bool,
    /// The APIC id of the core this thread is part of, shifted past the SMT
    /// bits. Both threads of a core report the same value.
    physical_core_id: u32,
};

/// A set of CPUs that share one cache instance, and the NUMA zone it sits in.
pub const CacheDomain = struct {
    domain_id: u32,
    cache_size_kb: u64,
    /// Members, with one CPU per physical core first. `getPhysicalCores`
    /// relies on that ordering; it is what lets the accessor return a slice
    /// without allocating.
    cpus: []CpuCore,
    cpu_list_str: []const u8, // Original string like "0-3,32-35"
    /// The NUMA zone that owns this cache, as the hardware numbers it. Chips
    /// that report no zone put the whole package in zone 0. A single socket
    /// can hold more than one zone, which is the case this field exists for.
    ///
    /// Null means the zone is not known: the CPUs would not say, or this is
    /// the degraded one-domain fallback and its members disagree. Claiming a
    /// zone there would be a guess, and a wrong zone is worse than none.
    node_id: ?u32,
    /// How many entries at the front of `cpus` stand for one physical core
    /// each: the first thread of the core when this process may run it, and
    /// otherwise whichever thread it does have.
    physical_cores: u32,

    /// One CPU per physical core, taken from the front of the member list.
    pub fn getPhysicalCores(self: *const CacheDomain) []const CpuCore {
        return self.cpus[0..self.physical_cores];
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

    /// A bitmap with one bit per preferred CPU, in `sched_setaffinity` form.
    pub fn getAffinityMask(self: *const ShardMapping, allocator: std.mem.Allocator) ![]u8 {
        // The mask must cover the highest CPU id, not the last one in the
        // list: `preferred_cpus` is not promised to be ascending, and a mask
        // sized from the last entry writes past its end when it is not.
        var highest: u32 = 0;
        for (self.preferred_cpus) |cpu| highest = @max(highest, cpu);
        if (self.preferred_cpus.len == 0) return allocator.alloc(u8, 0);

        const mask_size = (@as(usize, highest) + 8) / 8;
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
    /// CPUs this process may run on, as counted from the affinity mask.
    total_cpus: u32,
    /// Distinct physical cores among those CPUs.
    total_physical_cores: u32,
    shard_mappings: []ShardMapping,
    cache_line_size: u32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *CacheTopology) void {
        freeDomains(self.allocator, self.l3_domains);
        freeDomains(self.allocator, self.l2_domains);
        for (self.shard_mappings) |mapping| {
            self.allocator.free(mapping.preferred_cpus);
        }
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

    /// NUMA zones named by the cache domains, counting each once.
    ///
    /// Zero means no zone could be proven: a machine that reports none, or the
    /// degraded one-domain fallback whose members disagree. Callers must treat
    /// zero as "unknown", never as "one zone".
    pub fn numaNodeCount(self: *const CacheTopology) u32 {
        var count: u32 = 0;
        for (self.l3_domains, 0..) |domain, index| {
            const zone = domain.node_id orelse continue;
            var seen = false;
            for (self.l3_domains[0..index]) |earlier| {
                if (earlier.node_id == zone) seen = true;
            }
            if (!seen) count += 1;
        }
        return count;
    }
};

fn freeDomains(allocator: std.mem.Allocator, domains: []CacheDomain) void {
    for (domains) |domain| {
        allocator.free(domain.cpus);
        allocator.free(domain.cpu_list_str);
    }
    allocator.free(domains);
}

// ---------------------------------------------------------------------------
// CPUID
// ---------------------------------------------------------------------------

const CpuId = struct {
    eax: u32,
    ebx: u32,
    ecx: u32,
    edx: u32,
};

fn cpuid(leaf: u32, subleaf: u32) CpuId {
    var eax_out: u32 = undefined;
    var ebx_out: u32 = undefined;
    var ecx_out: u32 = undefined;
    var edx_out: u32 = undefined;

    asm volatile ("cpuid"
        : [eax] "={eax}" (eax_out),
          [ebx] "={ebx}" (ebx_out),
          [ecx] "={ecx}" (ecx_out),
          [edx] "={edx}" (edx_out),
        : [leaf] "{eax}" (leaf),
          [subleaf] "{ecx}" (subleaf),
        : .{ .memory = true });

    return .{ .eax = eax_out, .ebx = ebx_out, .ecx = ecx_out, .edx = edx_out };
}

const Vendor = enum { intel, amd, other };

const VendorInfo = struct {
    vendor: Vendor,
    max_std_leaf: u32,
    max_ext_leaf: u32,
};

fn vendorInfo() VendorInfo {
    const leaf0 = cpuid(0, 0);
    const ext0 = cpuid(0x80000000, 0);

    // "AuthenticAMD" is the three register halves of the vendor string.
    const is_amd = leaf0.ebx == 0x68747541 and leaf0.edx == 0x69746E65 and leaf0.ecx == 0x444D4163;
    const vendor: Vendor = if (is_amd) .amd else if (leaf0.ebx == 0x756E6547 and leaf0.edx == 0x49656E69) .intel else .other;

    return .{
        .vendor = vendor,
        .max_std_leaf = leaf0.eax,
        .max_ext_leaf = ext0.eax,
    };
}

/// A right shift that is total in the value it can be handed.
fn shrink(value: u32, bits: u8) u32 {
    if (bits >= 32) return 0;
    return value >> @intCast(bits);
}

/// Whether this thread is the first one of its core. Per the x2APIC spec the
/// low `smt_bits` of an APIC id are the thread number, so a non-zero tail is a
/// sibling. This is deliberately not "cpu_id is odd": it holds whatever order
/// the vendor uses, and AMD and Intel do not agree.
fn isPrimaryThread(apic_id: u32, smt_bits: u8) bool {
    return (apic_id & lowMask(smt_bits)) == 0;
}

fn lowMask(bits: u8) u32 {
    if (bits >= 32) return std.math.maxInt(u32);
    return (@as(u32, 1) << @intCast(bits)) - 1;
}

const TopoBits = struct {
    apic_id: u32,
    smt_bits: u8,
    package_bits: u8,
};

fn topologyBits(info: VendorInfo) ?TopoBits {
    var apic_id: u32 = 0;
    var smt_bits: u8 = 0;
    var package_bits: u8 = 8;
    var found = false;

    if (info.max_std_leaf >= 0xB) {
        // Subleaf 0 is the SMT level: EAX[4:0] is the shift from APIC id to
        // core, EDX is the whole 32-bit x2APIC id of this CPU.
        const smt = cpuid(0xB, 0);
        if (((smt.ecx >> 8) & 0xFF) != 0) {
            apic_id = smt.edx;
            smt_bits = @intCast(smt.eax & 0x1F);
            found = true;

            // Subleaf 1 is the core level: EAX[4:0] is the shift from APIC id
            // to package.
            const core = cpuid(0xB, 1);
            if (((core.ecx >> 8) & 0xFF) == 2) {
                package_bits = @intCast(core.eax & 0x1F);
            }
        }
    }

    if (!found) {
        // No leaf 0xB: an 8-bit APIC id from leaf 1, and HTT tells us whether
        // SMT exists at all. Ids above 255 cannot be represented, so a package
        // is one 8-bit space and `package_bits = 8` keeps the shift legal.
        const one = cpuid(1, 0);
        apic_id = (one.ebx >> 24) & 0xFF;
        smt_bits = if ((one.edx & (@as(u32, 1) << 28)) != 0) 1 else 0;
    }

    return .{ .apic_id = apic_id, .smt_bits = smt_bits, .package_bits = package_bits };
}

const NodeInfo = struct {
    node_id: u32,
    /// False when the CPU does not report a zone, in which case the caller
    /// treats the package as one zone.
    known: bool,
};

fn probeNode(info: VendorInfo) NodeInfo {
    // AMD extended topology, leaf 0x8000001E: ECX[7:0] is the node id and
    // ECX[10:8] is nodes per processor. This is the only CPUID field that
    // names a zone, and it is why a single socket can report two of them.
    if (info.vendor == .amd and info.max_ext_leaf >= 0x8000001E) {
        const ext = cpuid(0x8000001E, 0);
        return .{ .node_id = ext.ecx & 0xFF, .known = true };
    }
    return .{ .node_id = 0, .known = false };
}

const CacheGeometry = struct {
    size_kb: u64,
    line_size: u32,
    /// Logical CPUs that share one instance, from the level's sharing count
    /// multiplied by the threads per core.
    sharing_logical: u32,
};

fn probeCache(info: VendorInfo, cpus_per_core: u32, level: u8) ?CacheGeometry {
    const leaf: u32 = if (info.vendor == .amd) 0x8000001D else 4;
    const limit: u32 = if (info.vendor == .amd) info.max_ext_leaf else info.max_std_leaf;
    if (limit < leaf) return null;

    var index: u32 = 0;
    while (index < 16) : (index += 1) {
        const entry = cpuid(leaf, index);
        const cache_type = entry.eax & 0x1F;
        if (cache_type == 0) return null; // end of the list

        const cache_level: u8 = @intCast((entry.eax >> 5) & 0x7);
        // 1 = data, 3 = unified. Instruction caches are irrelevant here.
        if (cache_level != level or (cache_type != 1 and cache_type != 3)) continue;

        const line_size = (entry.ebx & 0xFFF) + 1;
        const partitions = ((entry.ebx >> 12) & 0x3FF) + 1;
        const ways = ((entry.ebx >> 22) & 0x3FF) + 1;
        const sets = @as(u64, entry.ecx) + 1;
        const size_kb = (@as(u64, ways) * partitions * line_size * sets) / 1024;

        const sharing_cores = ((entry.eax >> 14) & 0xFFF) + 1;
        // Saturate instead of wrapping. A sharing count that large cannot
        // match the group sizes below, so the caller degrades rather than
        // mis-grouping.
        const sharing_logical = @as(u64, sharing_cores) * cpus_per_core;
        return .{
            .size_kb = size_kb,
            .line_size = line_size,
            .sharing_logical = @intCast(@min(sharing_logical, std.math.maxInt(u32))),
        };
    }
    return null;
}

fn detectCacheLineSize() u32 {
    if (builtin.cpu.arch != .x86_64) return 64;

    // CPUID leaf 1, EBX[15:8] is the cache line size in 8-byte units.
    const one = cpuid(1, 0);
    const cache_line_size = ((one.ebx >> 8) & 0xFF) * 8;
    return switch (cache_line_size) {
        16, 32, 64, 128, 256 => cache_line_size,
        else => 64,
    };
}

// ---------------------------------------------------------------------------
// Probes
// ---------------------------------------------------------------------------

/// What one CPU says about itself.
const CpuRecord = struct {
    cpu_id: u32,
    apic_id: u32,
    /// APIC id shifted past the SMT bits. Threads of one core agree.
    core_id: u32,
    /// APIC id shifted past the package bits.
    package_id: u32,
    node_id: u32,
    /// Whether `node_id` is something the CPU said, rather than the zero it
    /// was given when it said nothing.
    node_known: bool,
    is_primary_thread: bool,
    l2: ?CacheGeometry,
    l3: ?CacheGeometry,
};

const CpuSelfReport = struct {
    apic_id: u32,
    smt_bits: u8,
    package_bits: u8,
    node_id: u32,
    l2: ?CacheGeometry,
    l3: ?CacheGeometry,
};

fn probeThisCpu() ?CpuSelfReport {
    if (builtin.cpu.arch != .x86_64) return null;

    const info = vendorInfo();
    const topo = topologyBits(info) orelse return null;
    const node = probeNode(info);
    const threads_per_core = @as(u32, 1) << @intCast(@min(topo.smt_bits, 31));
    const package_id = shrink(topo.apic_id, topo.package_bits);

    return .{
        .apic_id = topo.apic_id,
        .smt_bits = topo.smt_bits,
        .package_bits = topo.package_bits,
        .node_id = if (node.known) node.node_id else package_id,
        .l2 = probeCache(info, threads_per_core, 2),
        .l3 = probeCache(info, threads_per_core, 3),
    };
}

/// A record for a CPU that would not answer, or that cannot be asked at all.
/// It claims no cache and no zone, and it is its own core, so every consumer
/// degrades to one domain of physical cores rather than pinning something it
/// cannot name.
fn unknownRecord(cpu_id: u32) CpuRecord {
    return .{
        .cpu_id = cpu_id,
        .apic_id = cpu_id,
        .core_id = cpu_id,
        .package_id = 0,
        .node_id = 0,
        .node_known = false,
        .is_primary_thread = true,
        .l2 = null,
        .l3 = null,
    };
}

fn probeCpus(allocator: std.mem.Allocator) ![]CpuRecord {
    var records: std.ArrayList(CpuRecord) = .empty;
    defer records.deinit(allocator);

    var allowed: os_compat.CpuSet = undefined;
    if (!os_compat.cpuAffinityGet(&allowed)) {
        // No mask means no idea which CPUs exist here. Count them, name them
        // by OS id, and claim nothing else.
        const count = std.Thread.getCpuCount() catch 1;
        for (0..count) |index| {
            try records.append(allocator, unknownRecord(@intCast(index)));
        }
        return records.toOwnedSlice(allocator);
    }

    var cpu_id: u32 = 0;
    while (cpu_id < os_compat.max_cpus) : (cpu_id += 1) {
        if (!os_compat.cpuSetContains(&allowed, cpu_id)) continue;

        var only_this: os_compat.CpuSet = undefined;
        @memset(std.mem.asBytes(&only_this), 0);
        os_compat.cpuSetPut(&only_this, cpu_id);

        if (!os_compat.cpuAffinitySet(&only_this)) {
            try records.append(allocator, unknownRecord(cpu_id));
            continue;
        }

        const report = probeThisCpu() orelse {
            try records.append(allocator, unknownRecord(cpu_id));
            continue;
        };

        try records.append(allocator, .{
            .cpu_id = cpu_id,
            .apic_id = report.apic_id,
            .core_id = shrink(report.apic_id, report.smt_bits),
            .package_id = shrink(report.apic_id, report.package_bits),
            .node_id = report.node_id,
            .node_known = true,
            .is_primary_thread = isPrimaryThread(report.apic_id, report.smt_bits),
            .l2 = report.l2,
            .l3 = report.l3,
        });
    }

    // Put the process back where it was. Everything after this point runs on
    // whatever CPUs the caller allowed, not on the last one probed. A failure
    // here leaves the thread pinned, which is a performance bug, so say so.
    if (!os_compat.cpuAffinitySet(&allowed)) {
        std.debug.print("Warning: could not restore CPU affinity after the topology probe; this thread is stuck on one CPU.\n", .{});
    }

    if (records.items.len == 0) {
        try records.append(allocator, unknownRecord(0));
    }
    return records.toOwnedSlice(allocator);
}

// ---------------------------------------------------------------------------
// Grouping
// ---------------------------------------------------------------------------

const Level = enum { l2, l3 };

fn geometryOf(record: CpuRecord, level: Level) ?CacheGeometry {
    return switch (level) {
        .l2 => record.l2,
        .l3 => record.l3,
    };
}

/// The identity of the cache a CPU belongs to.
///
/// For an L3, a package plus a NUMA zone. A package can hold two zones on one
/// socket, which is exactly the case where one cache id is not enough; and on
/// chips that report no zone at all, one package is one zone by definition. For
/// an L2, a package plus a core, since an L2 is private to a core that may run
/// two threads.
///
/// The package half of the L2 key is redundant today, because `core_id` is
/// shifted out of the APIC id and so already carries the package. It stays for
/// a cheap reason: if `core_id` ever stops encoding the package, the key does
/// not silently merge two sockets.
/// The zone a domain may claim: one, only when every member reported the
/// same one. Anything else is null.
fn domainZone(records: []const CpuRecord, members: []const usize) ?u32 {
    var zone: ?u32 = null;
    for (members) |record_index| {
        const record = records[record_index];
        if (!record.node_known) return null;
        if (zone) |known| {
            if (known != record.node_id) return null;
        } else {
            zone = record.node_id;
        }
    }
    return zone;
}

fn groupKey(record: CpuRecord, level: Level) [2]u32 {
    return switch (level) {
        .l2 => .{ record.package_id, record.core_id },
        .l3 => .{ record.package_id, record.node_id },
    };
}

fn buildDomains(allocator: std.mem.Allocator, records: []const CpuRecord, level: Level) ![]CacheDomain {
    // No geometry means no claim: one domain, every CPU, and the caller can
    // see that it is the whole machine.
    const unverifiable = for (records) |record| {
        if (geometryOf(record, level) == null) break true;
    } else false;

    if (unverifiable) return singleDomain(allocator, records, level);

    const Group = struct {
        key: [2]u32,
        geometry: CacheGeometry,
        members: std.ArrayList(usize),
    };

    var groups: std.ArrayList(Group) = .empty;
    defer {
        for (groups.items) |*group| group.members.deinit(allocator);
        groups.deinit(allocator);
    }

    for (records, 0..) |record, index| {
        const key = groupKey(record, level);
        var found: ?*Group = null;
        for (groups.items) |*group| {
            if (group.key[0] == key[0] and group.key[1] == key[1]) {
                found = group;
                break;
            }
        }
        if (found) |group| {
            try group.members.append(allocator, index);
        } else {
            var members: std.ArrayList(usize) = .empty;
            try members.append(allocator, index);
            try groups.append(allocator, .{
                .key = key,
                .geometry = geometryOf(record, level).?,
                .members = members,
            });
        }
    }

    // A group is only a cache if the hardware agrees about how many logical
    // CPUs share one. Running under `taskset` or in a cgroup hides part of a
    // cache, and the sharing count then describes CPUs this process cannot
    // use. Claiming the zone would be a guess, so claim nothing.
    for (groups.items) |group| {
        if (group.members.items.len != group.geometry.sharing_logical) {
            return singleDomain(allocator, records, level);
        }
    }

    var domains = try allocator.alloc(CacheDomain, groups.items.len);
    var built: usize = 0;
    errdefer {
        freeDomains(allocator, domains[0..built]);
        allocator.free(domains);
    }

    for (groups.items, 0..) |group, domain_index| {
        const ordered = try orderMembers(allocator, records, group.members.items);
        errdefer allocator.free(ordered.cpus);

        domains[domain_index] = .{
            .domain_id = @intCast(domain_index),
            .cache_size_kb = group.geometry.size_kb,
            .cpus = ordered.cpus,
            .cpu_list_str = try formatCpuList(allocator, ordered.cpus),
            .node_id = domainZone(records, group.members.items),
            .physical_cores = ordered.physical,
        };
        built += 1;
    }

    return domains;
}

/// The honest fallback: one domain holding every CPU this process may use.
fn singleDomain(allocator: std.mem.Allocator, records: []const CpuRecord, level: Level) ![]CacheDomain {
    if (records.len == 0) return allocator.alloc(CacheDomain, 0);

    var domains = try allocator.alloc(CacheDomain, 1);
    errdefer allocator.free(domains);

    const members = try allocator.alloc(usize, records.len);
    defer allocator.free(members);
    for (members, 0..) |*slot, index| slot.* = index;

    const ordered = try orderMembers(allocator, records, members);
    errdefer allocator.free(ordered.cpus);

    domains[0] = .{
        .domain_id = 0,
        .cache_size_kb = for (records) |record| {
            if (geometryOf(record, level)) |geometry| break geometry.size_kb;
        } else 0,
        .cpus = ordered.cpus,
        .cpu_list_str = try formatCpuList(allocator, ordered.cpus),
        .node_id = domainZone(records, members),
        .physical_cores = ordered.physical,
    };
    return domains;
}

const OrderedMembers = struct {
    cpus: []CpuCore,
    /// Entries at the front of `cpus` that stand for one physical core each.
    physical: u32,
};

/// Lay members out the way every domain is laid out: one CPU per physical core
/// first, then the remaining threads, each block in CPU id order.
///
/// The representative of a core is its lowest-numbered thread that this
/// process may run. Under a full mask that is the primary thread. Under a mask
/// that holds only second threads — `taskset -c 15` — the representative is a
/// sibling, and the domain must still report one CPU per core: an empty
/// physical-core list would leave a caller sizing its worker set from nothing.
fn orderMembers(allocator: std.mem.Allocator, records: []const CpuRecord, members: []const usize) !OrderedMembers {
    const ordered = try allocator.alloc(CpuCore, members.len);
    errdefer allocator.free(ordered);

    var next: usize = 0;
    var physical: u32 = 0;

    for (0..2) |pass| {
        for (members, 0..) |record_index, member_index| {
            const record = records[record_index];

            var is_representative = true;
            for (members[0..member_index]) |earlier_index| {
                const earlier = records[earlier_index];
                if (earlier.core_id == record.core_id and earlier.package_id == record.package_id) {
                    is_representative = false;
                    break;
                }
            }
            if (is_representative != (pass == 0)) continue;

            ordered[next] = .{
                .cpu_id = record.cpu_id,
                .is_smt_sibling = !record.is_primary_thread,
                .physical_core_id = record.core_id,
            };
            next += 1;
            if (pass == 0) physical += 1;
        }
    }

    return .{ .cpus = ordered, .physical = physical };
}

fn formatCpuList(allocator: std.mem.Allocator, cpus: []const CpuCore) ![]const u8 {
    var list: std.ArrayList(u8) = .empty;
    errdefer list.deinit(allocator);

    var number_buffer: [20]u8 = undefined;
    for (cpus, 0..) |cpu, index| {
        if (index > 0) try list.appendSlice(allocator, ",");
        try list.appendSlice(allocator, try std.fmt.bufPrint(&number_buffer, "{d}", .{cpu.cpu_id}));
    }
    return list.toOwnedSlice(allocator);
}

fn countDistinctCores(records: []const CpuRecord) u32 {
    var count: u32 = 0;
    for (records, 0..) |record, index| {
        var seen = false;
        for (records[0..index]) |earlier| {
            if (earlier.core_id == record.core_id and earlier.package_id == record.package_id) {
                seen = true;
                break;
            }
        }
        if (!seen) count += 1;
    }
    return count;
}

/// Detect this machine's cache and NUMA layout.
///
/// Runs CPUID on every CPU the process is allowed to use, one pin per CPU, and
/// restores the original affinity mask before returning. The result is a
/// report; nothing here places memory or pins anything.
pub fn analyzeCacheTopology(allocator: std.mem.Allocator) !CacheTopology {
    const records = try probeCpus(allocator);
    defer allocator.free(records);
    return buildTopology(allocator, records);
}

/// Turn per-CPU records into domains and shards.
///
/// Separate from the probe so the rules below can be tested against machines
/// this one is not: a second socket, two zones on one socket, a partial mask.
fn buildTopology(allocator: std.mem.Allocator, records: []const CpuRecord) !CacheTopology {
    const l3_domains = try buildDomains(allocator, records, .l3);
    errdefer freeDomains(allocator, l3_domains);

    const l2_domains = try buildDomains(allocator, records, .l2);
    errdefer freeDomains(allocator, l2_domains);

    // One shard per L3 domain: the unit a CPU can read without leaving its
    // cache. Shards carry the zone they sit in, so a caller that pins a thread
    // to a shard also knows which zone that thread's memory should come from.
    const shard_mappings = try allocator.alloc(ShardMapping, l3_domains.len);
    var shards_built: usize = 0;
    errdefer {
        for (shard_mappings[0..shards_built]) |mapping| allocator.free(mapping.preferred_cpus);
        allocator.free(shard_mappings);
    }

    for (l3_domains, 0..) |*domain, index| {
        var preferred: std.ArrayList(u32) = .empty;
        errdefer preferred.deinit(allocator);

        for (domain.getPhysicalCores()) |core| {
            try preferred.append(allocator, core.cpu_id);
        }
        // A machine with fewer than four physical cores still needs workers
        // somewhere to run, so fill the rest from the remaining threads of the
        // domain. A core whose only allowed thread is a sibling is already
        // preferred under that rule, so a CPU can never be added twice.
        if (preferred.items.len < 4) {
            for (domain.cpus) |cpu| {
                if (preferred.items.len >= 4) break;
                var already_chosen = false;
                for (preferred.items) |chosen| {
                    if (chosen == cpu.cpu_id) already_chosen = true;
                }
                if (!already_chosen) try preferred.append(allocator, cpu.cpu_id);
            }
        }

        shard_mappings[index] = .{
            .shard_id = @intCast(index),
            .cache_domain = domain,
            .preferred_cpus = try preferred.toOwnedSlice(allocator),
        };
        shards_built += 1;
    }

    // The cache entries carry the line size, which is what the geometry
    // probes actually read. Leaf 1 is the fallback for a CPU that reported no
    // cache entries at all.
    const line_size = cacheLineSizeOf(records) orelse detectCacheLineSize();

    return CacheTopology{
        .l3_domains = l3_domains,
        .l2_domains = l2_domains,
        .total_cpus = @intCast(records.len),
        .total_physical_cores = countDistinctCores(records),
        .shard_mappings = shard_mappings,
        .cache_line_size = line_size,
        .allocator = allocator,
    };
}

fn cacheLineSizeOf(records: []const CpuRecord) ?u32 {
    for (records) |record| {
        if (record.l3) |geometry| {
            if (geometry.line_size != 0) return geometry.line_size;
        }
        if (record.l2) |geometry| {
            if (geometry.line_size != 0) return geometry.line_size;
        }
    }
    return null;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

/// Records for a machine, built by hand so the grouping rule can be tested
/// without the hardware it is meant for. `cores` is how many physical cores,
/// `zones` how many NUMA zones they split into, and `smt` whether each core
/// runs two threads.
fn syntheticMachine(
    allocator: std.mem.Allocator,
    cores: u32,
    packages: u32,
    zones_per_package: u32,
    smt: bool,
    l3_sharing_cores: u32,
    l3_size_kb: []const u64,
) ![]CpuRecord {
    const threads_per_core: u32 = if (smt) 2 else 1;
    const smt_bits: u8 = if (smt) 1 else 0;
    const cores_per_package = cores / packages;
    const cores_per_zone = cores_per_package / zones_per_package;

    var records: std.ArrayList(CpuRecord) = .empty;
    defer records.deinit(allocator);

    var cpu_id: u32 = 0;
    for (0..packages) |package| {
        for (0..cores_per_package) |core_in_package| {
            // Match the layout on AMD parts: the APIC ids of one core are
            // adjacent, so thread t of core c is c * threads_per_core + t.
            const core_index = @as(u32, @intCast(package)) * cores_per_package + @as(u32, @intCast(core_in_package));
            const zone_in_package = @as(u32, @intCast(core_in_package)) / cores_per_zone;
            const zone_id = @as(u32, @intCast(package)) * zones_per_package + zone_in_package;
            const size = l3_size_kb[@min(zone_id, l3_size_kb.len - 1)];

            for (0..threads_per_core) |thread| {
                const apic_id = core_index * threads_per_core + @as(u32, @intCast(thread));
                try records.append(allocator, .{
                    .cpu_id = cpu_id,
                    .apic_id = apic_id,
                    .core_id = apic_id >> @intCast(smt_bits),
                    .package_id = @intCast(package),
                    .node_id = zone_id,
                    .node_known = true,
                    .is_primary_thread = thread == 0,
                    .l2 = .{ .size_kb = 1024, .line_size = 64, .sharing_logical = threads_per_core },
                    .l3 = .{ .size_kb = size, .line_size = 64, .sharing_logical = l3_sharing_cores * threads_per_core },
                });
                cpu_id += 1;
            }
        }
    }

    return records.toOwnedSlice(allocator);
}

/// Pick the record for one OS CPU, so a domain check can compare a member
/// against what that CPU reported.
fn recordFor(records: []const CpuRecord, cpu_id: u32) ?CpuRecord {
    for (records) |record| {
        if (record.cpu_id == cpu_id) return record;
    }
    return null;
}

/// Distinct physical cores inside one domain. A domain sits inside a single
/// package, so its core ids cannot collide the way ids across packages can.
fn distinctCoresIn(domain: *const CacheDomain) u32 {
    var count: u32 = 0;
    for (domain.cpus, 0..) |cpu, index| {
        var seen = false;
        for (domain.cpus[0..index]) |earlier| {
            if (earlier.physical_core_id == cpu.physical_core_id) seen = true;
        }
        if (!seen) count += 1;
    }
    return count;
}

/// The printable list is the member CPUs, in order, with no trailing comma.
fn expectCpuListMatches(allocator: std.mem.Allocator, domain: *const CacheDomain) !void {
    var expected: std.ArrayList(u8) = .empty;
    defer expected.deinit(allocator);

    var number_buffer: [20]u8 = undefined;
    for (domain.cpus, 0..) |cpu, index| {
        if (index > 0) try expected.append(allocator, ',');
        try expected.appendSlice(allocator, try std.fmt.bufPrint(&number_buffer, "{d}", .{cpu.cpu_id}));
    }
    try testing.expectEqualStrings(expected.items, domain.cpu_list_str);
}

/// Every rule the domain builder promises, checked against whatever machine
/// layout the caller built by hand.
fn expectDomainInvariants(allocator: std.mem.Allocator, domains: []const CacheDomain, records: []const CpuRecord) !void {
    var members: usize = 0;

    for (domains, 0..) |*domain, index| {
        // Ids are dense from zero, so a domain can index its own shard.
        try testing.expectEqual(@as(u32, @intCast(index)), domain.domain_id);
        members += domain.cpus.len;

        for (domain.cpus, 0..) |cpu, member_index| {
            // No CPU appears twice inside one domain.
            for (domain.cpus[0..member_index]) |earlier| {
                try testing.expect(earlier.cpu_id != cpu.cpu_id);
            }

            const record = recordFor(records, cpu.cpu_id) orelse return error.UnknownCpu;
            // A domain that names a zone names the zone its members did.
            if (domain.node_id) |zone| {
                try testing.expect(record.node_known);
                try testing.expectEqual(zone, record.node_id);
            }
            // Classification and core identity come from the record, never
            // from the order the members arrived in.
            try testing.expectEqual(!record.is_primary_thread, cpu.is_smt_sibling);
            try testing.expectEqual(record.core_id, cpu.physical_core_id);
        }

        // The prefix is one CPU per physical core, and its length is the
        // number of distinct cores in the domain.
        const physical = domain.getPhysicalCores();
        try testing.expectEqual(domain.physical_cores, @as(u32, @intCast(physical.len)));
        try testing.expectEqual(distinctCoresIn(domain), domain.physical_cores);
        try testing.expect(@as(usize, domain.physical_cores) <= domain.cpus.len);

        for (physical, 0..) |core, core_index| {
            // One entry per core, no core twice.
            for (physical[core_index + 1 ..]) |other| {
                try testing.expect(core.physical_core_id != other.physical_core_id);
            }
            // Every thread behind the prefix belongs to one of those cores.
            for (domain.cpus[domain.physical_cores..]) |other| {
                if (other.physical_core_id != core.physical_core_id) continue;
                // The representative is the lowest-numbered thread of the
                // core this process may run, so nothing behind it comes first.
                try testing.expect(other.cpu_id > core.cpu_id);
            }
        }

        // A primary thread is always its core's representative, because the
        // primary thread sorts before its siblings on every platform that
        // reports them honestly.
        for (domain.cpus, 0..) |cpu, member_index| {
            if (cpu.is_smt_sibling) continue;
            try testing.expect(member_index < domain.physical_cores);
        }

        // Nothing behind the prefix names a core the prefix does not cover.
        for (domain.cpus[domain.physical_cores..]) |cpu| {
            var covered = false;
            for (physical) |core| {
                if (core.physical_core_id == cpu.physical_core_id) covered = true;
            }
            try testing.expect(covered);
        }

        try expectCpuListMatches(allocator, domain);
    }

    // Every CPU is in exactly one domain, and no CPU is dropped.
    try testing.expectEqual(records.len, members);
    for (records) |record| {
        var found: u32 = 0;
        for (domains) |domain| {
            if (domain.containsCpu(record.cpu_id)) found += 1;
        }
        try testing.expectEqual(@as(u32, 1), found);
    }
}

/// What a shard promises: it points at one real domain, prefers that domain's
/// CPUs, and its bitmap says exactly what `preferred_cpus` says.
fn expectShardInvariants(allocator: std.mem.Allocator, mapping: *const ShardMapping) !void {
    const domain = mapping.cache_domain;
    const physical = domain.getPhysicalCores();

    for (mapping.preferred_cpus, 0..) |cpu, index| {
        try testing.expect(domain.containsCpu(cpu));
        for (mapping.preferred_cpus[0..index]) |earlier| {
            try testing.expect(earlier != cpu);
        }
    }

    // Every physical core of the domain is preferred; siblings join only to
    // reach four CPUs, which is the floor for a usable worker set.
    for (physical) |core| {
        var present = false;
        for (mapping.preferred_cpus) |cpu| {
            if (cpu == core.cpu_id) present = true;
        }
        try testing.expect(present);
    }
    try testing.expectEqual(
        @max(physical.len, @min(4, domain.cpus.len)),
        mapping.preferred_cpus.len,
    );

    const mask = try mapping.getAffinityMask(allocator);
    defer allocator.free(mask);

    if (mapping.preferred_cpus.len == 0) {
        try testing.expectEqual(@as(usize, 0), mask.len);
        return;
    }

    var highest: u32 = 0;
    for (mapping.preferred_cpus) |cpu| highest = @max(highest, cpu);
    try testing.expectEqual((@as(usize, highest) + 8) / 8, mask.len);

    // No bit more than the preferred set, and every preferred CPU is in it.
    var bits: usize = 0;
    for (mask) |byte| bits += @popCount(byte);
    try testing.expectEqual(mapping.preferred_cpus.len, bits);
    for (mapping.preferred_cpus) |cpu| {
        try testing.expect((mask[cpu / 8] & (@as(u8, 1) << @intCast(cpu % 8))) != 0);
    }
}

/// The rules `buildTopology` promises on top of the per-domain ones.
fn expectTopologyInvariants(allocator: std.mem.Allocator, topology: *const CacheTopology, records: []const CpuRecord) !void {
    try testing.expectEqual(@as(u32, @intCast(records.len)), topology.total_cpus);
    try testing.expectEqual(countDistinctCores(records), topology.total_physical_cores);
    try testing.expect(topology.cache_line_size > 0);

    // One shard per L3 domain, and the shard points into the domain slice
    // rather than at a copy that could go stale.
    try testing.expectEqual(topology.l3_domains.len, topology.shard_mappings.len);
    for (topology.shard_mappings, 0..) |*mapping, index| {
        try testing.expectEqual(@as(u32, @intCast(index)), mapping.shard_id);
        try testing.expectEqual(@intFromPtr(&topology.l3_domains[index]), @intFromPtr(mapping.cache_domain));
        try expectShardInvariants(allocator, mapping);
        try testing.expectEqual(index, topology.getShardById(@intCast(index)).?.shard_id);
    }
    try testing.expect(topology.getShardById(@intCast(topology.shard_mappings.len)) == null);

    // Every CPU maps back to the shard that owns it.
    for (topology.l3_domains) |domain| {
        for (domain.cpus) |cpu| {
            try testing.expectEqual(domain.domain_id, topology.getCpuShard(cpu.cpu_id).?.shard_id);
        }
    }

    try testing.expectEqual(topology.shard_mappings.len, topology.getShardCount());

    // The zone count is the number of distinct zones the domains name, and a
    // domain that names no zone contributes nothing.
    var zones: u32 = 0;
    for (topology.l3_domains, 0..) |domain, index| {
        const zone = domain.node_id orelse continue;
        var seen = false;
        for (topology.l3_domains[0..index]) |earlier| {
            if (earlier.node_id == zone) seen = true;
        }
        if (!seen) zones += 1;
    }
    try testing.expectEqual(zones, topology.numaNodeCount());
}

/// What the probe promises about the machine it ran on, before grouping.
fn expectProbeInvariants(records: []const CpuRecord) !void {
    try testing.expect(records.len > 0);

    for (records, 0..) |record, index| {
        // Ascending OS ids, no duplicates: downstream code indexes by them.
        if (index > 0) try testing.expect(record.cpu_id > records[index - 1].cpu_id);

        // Exactly one primary thread per core, and every sibling has one.
        var primaries: u32 = 0;
        var members: u32 = 0;
        for (records) |other| {
            if (other.package_id == record.package_id and other.core_id == record.core_id) {
                members += 1;
                if (other.is_primary_thread) primaries += 1;
            }
        }
        try testing.expectEqual(@as(u32, 1), primaries);
        if (!record.is_primary_thread) try testing.expect(members > 1);

        // A cache entry, when reported, describes something real.
        if (record.l2) |geometry| try expectCacheGeometry(geometry);
        if (record.l3) |geometry| try expectCacheGeometry(geometry);
    }
}

fn expectCacheGeometry(geometry: CacheGeometry) !void {
    try testing.expect(geometry.size_kb > 0);
    try testing.expect(geometry.sharing_logical >= 1);
    try testing.expect(switch (geometry.line_size) {
        16, 32, 64, 128, 256 => true,
        else => false,
    });
}

// ---------------------------------------------------------------------------
// Layouts
// ---------------------------------------------------------------------------

test "one zone, one L3: every CPU lands in one domain" {
    const records = try syntheticMachine(testing.allocator, 8, 1, 1, true, 8, &.{32768});
    defer testing.allocator.free(records);

    const domains = try buildDomains(testing.allocator, records, .l3);
    defer freeDomains(testing.allocator, domains);

    try testing.expectEqual(@as(usize, 1), domains.len);
    try testing.expectEqual(@as(u32, 16), domains[0].cpus.len);
    try testing.expectEqual(@as(u32, 32768), domains[0].cache_size_kb);
    try testing.expectEqual(@as(?u32, 0), domains[0].node_id);
    try expectDomainInvariants(testing.allocator, domains, records);
}

test "two zones on one socket: two domains, no CPU in both" {
    // The Zen 6 case: one package, two NUMA zones, one L3 per zone.
    const records = try syntheticMachine(testing.allocator, 16, 1, 2, true, 8, &.{ 32768, 32768 });
    defer testing.allocator.free(records);

    const domains = try buildDomains(testing.allocator, records, .l3);
    defer freeDomains(testing.allocator, domains);

    try testing.expectEqual(@as(usize, 2), domains.len);
    try testing.expectEqual(@as(u32, 16), domains[0].cpus.len);
    try testing.expectEqual(@as(u32, 16), domains[1].cpus.len);
    try testing.expectEqual(@as(?u32, 0), domains[0].node_id);
    try testing.expectEqual(@as(?u32, 1), domains[1].node_id);
    try expectDomainInvariants(testing.allocator, domains, records);
}

test "two sockets: one domain per package" {
    const records = try syntheticMachine(testing.allocator, 16, 2, 1, true, 8, &.{ 32768, 32768 });
    defer testing.allocator.free(records);

    const domains = try buildDomains(testing.allocator, records, .l3);
    defer freeDomains(testing.allocator, domains);

    try testing.expectEqual(@as(usize, 2), domains.len);
    try testing.expectEqual(@as(?u32, 0), domains[0].node_id);
    try testing.expectEqual(@as(?u32, 1), domains[1].node_id);
    try expectDomainInvariants(testing.allocator, domains, records);
}

test "two sockets with two zones each: four domains" {
    const records = try syntheticMachine(testing.allocator, 32, 2, 2, true, 8, &.{ 32768, 32768 });
    defer testing.allocator.free(records);

    const domains = try buildDomains(testing.allocator, records, .l3);
    defer freeDomains(testing.allocator, domains);

    try testing.expectEqual(@as(usize, 4), domains.len);
    for (domains, 0..) |domain, index| {
        try testing.expectEqual(@as(?u32, @intCast(index)), domain.node_id);
    }
    try expectDomainInvariants(testing.allocator, domains, records);
}

test "zones of different sizes keep their own size" {
    // The 7900X3D shape: one package, two domains, one of them stacked.
    const records = try syntheticMachine(testing.allocator, 16, 1, 2, true, 8, &.{ 32768, 98304 });
    defer testing.allocator.free(records);

    const domains = try buildDomains(testing.allocator, records, .l3);
    defer freeDomains(testing.allocator, domains);

    try testing.expectEqual(@as(u32, 32768), domains[0].cache_size_kb);
    try testing.expectEqual(@as(u32, 98304), domains[1].cache_size_kb);
}

test "a zone id is not an identity on its own: two packages share zone 0" {
    // Both sockets number their zones from zero, which is how real NUMA
    // numbering works. Grouping by zone alone would merge the two sockets.
    var records: [4]CpuRecord = undefined;
    for (0..4) |index| {
        const package_id: u32 = @intCast(index / 2);
        const thread: u32 = @intCast(index % 2);
        const apic_id = package_id * 2 + thread;
        records[index] = .{
            .cpu_id = @intCast(index),
            .apic_id = apic_id,
            .core_id = apic_id >> 1,
            .package_id = package_id,
            .node_id = 0,
            .node_known = true,
            .is_primary_thread = thread == 0,
            .l2 = .{ .size_kb = 1024, .line_size = 64, .sharing_logical = 2 },
            .l3 = .{ .size_kb = 32768, .line_size = 64, .sharing_logical = 2 },
        };
    }

    const l3_domains = try buildDomains(testing.allocator, &records, .l3);
    defer freeDomains(testing.allocator, l3_domains);
    try testing.expectEqual(@as(usize, 2), l3_domains.len);
    try expectDomainInvariants(testing.allocator, l3_domains, &records);

    const l2_domains = try buildDomains(testing.allocator, &records, .l2);
    defer freeDomains(testing.allocator, l2_domains);
    try testing.expectEqual(@as(usize, 2), l2_domains.len);
    try expectDomainInvariants(testing.allocator, l2_domains, &records);
}

test "L2 domains are one per core" {
    const records = try syntheticMachine(testing.allocator, 8, 1, 1, true, 8, &.{32768});
    defer testing.allocator.free(records);

    const domains = try buildDomains(testing.allocator, records, .l2);
    defer freeDomains(testing.allocator, domains);

    try testing.expectEqual(@as(usize, 8), domains.len);
    for (domains) |domain| {
        try testing.expectEqual(@as(u32, 2), domain.cpus.len);
        // Two threads, one core id, so exactly one physical core in the domain.
        try testing.expectEqual(@as(usize, 1), domain.getPhysicalCores().len);
    }
    try expectDomainInvariants(testing.allocator, domains, records);
}

test "a partial mask degrades to one domain instead of guessing zones" {
    // Two zones, four CPUs allowed, two in each zone: `taskset -c 0,1,16,17`
    // on a 32-thread dual-zone part. The hardware still says 16 CPUs share
    // each L3, so neither group is a whole cache and a zone claim for either
    // would be a guess. Taking the CPUs from one zone only would not test
    // this: one group of four is one domain whether or not the check is there.
    const all = try syntheticMachine(testing.allocator, 16, 1, 2, true, 8, &.{ 32768, 32768 });
    defer testing.allocator.free(all);

    var buffer: [4]CpuRecord = undefined;
    buffer[0] = all[0];
    buffer[1] = all[1];
    buffer[2] = all[16];
    buffer[3] = all[17];
    const records = buffer[0..4];

    const domains = try buildDomains(testing.allocator, records, .l3);
    defer freeDomains(testing.allocator, domains);

    try testing.expectEqual(@as(usize, 1), domains.len);
    try testing.expectEqual(@as(u32, 4), domains[0].cpus.len);
    try expectDomainInvariants(testing.allocator, domains, records);
}

/// Every record that is the second thread of its core, in CPU id order.
fn onlySiblings(all: []const CpuRecord, buffer: []CpuRecord) []const CpuRecord {
    var count: usize = 0;
    for (all) |record| {
        if (record.is_primary_thread) continue;
        buffer[count] = record;
        count += 1;
    }
    return buffer[0..count];
}

test "a mask of only second threads still names one CPU per core" {
    // A process pinned to one thread per core, and always the second thread:
    // `taskset` on odd CPU ids. There is no primary thread anywhere in the
    // mask, so the domain must still report one CPU per core rather than an
    // empty physical list.
    const all = try syntheticMachine(testing.allocator, 8, 1, 1, true, 8, &.{32768});
    defer testing.allocator.free(all);

    var buffer: [16]CpuRecord = undefined;
    const records = onlySiblings(all, &buffer);
    try testing.expectEqual(@as(usize, 8), records.len);
    for (records) |record| try testing.expect(!record.is_primary_thread);

    const domains = try buildDomains(testing.allocator, records, .l3);
    defer freeDomains(testing.allocator, domains);

    try testing.expectEqual(@as(usize, 1), domains.len);
    try testing.expectEqual(@as(u32, 8), domains[0].physical_cores);
    try testing.expectEqual(@as(usize, 8), domains[0].getPhysicalCores().len);
    for (domains[0].getPhysicalCores(), 0..) |core, index| {
        try testing.expectEqual(records[index].cpu_id, core.cpu_id);
        try testing.expect(core.is_smt_sibling);
    }
    try expectDomainInvariants(testing.allocator, domains, records);
}

test "a core whose only allowed thread is a sibling still gets a shard CPU" {
    // The same mask through the shard path: preferred CPUs must not be empty,
    // or a caller sizing its workers from it gets nothing to run on.
    const all = try syntheticMachine(testing.allocator, 8, 1, 1, true, 8, &.{32768});
    defer testing.allocator.free(all);

    var buffer: [16]CpuRecord = undefined;
    const records = onlySiblings(all, &buffer);
    var topology = try buildTopology(testing.allocator, records);
    defer topology.deinit();

    try testing.expectEqual(@as(usize, 1), topology.shard_mappings.len);
    try testing.expectEqual(@as(usize, 8), topology.shard_mappings[0].preferred_cpus.len);
    try expectTopologyInvariants(testing.allocator, &topology, records);
}

test "a probe with no cache data claims one domain, not a made-up one" {
    var records = [_]CpuRecord{ unknownRecord(0), unknownRecord(1), unknownRecord(2) };

    const domains = try buildDomains(testing.allocator, &records, .l3);
    defer freeDomains(testing.allocator, domains);

    try testing.expectEqual(@as(usize, 1), domains.len);
    try testing.expectEqual(@as(u32, 3), domains[0].cpus.len);
    try testing.expectEqual(@as(u64, 0), domains[0].cache_size_kb);
    try testing.expectEqual(@as(?u32, null), domains[0].node_id);
    try expectDomainInvariants(testing.allocator, domains, &records);
}

test "primary threads come first and getPhysicalCores finds them" {
    // Every sibling is sorted before its primary thread in CPU id order, so a
    // domain built by appending members would fail this.
    const records = try syntheticMachine(testing.allocator, 4, 1, 1, true, 4, &.{32768});
    defer testing.allocator.free(records);

    const domains = try buildDomains(testing.allocator, records, .l3);
    defer freeDomains(testing.allocator, domains);

    const physical = domains[0].getPhysicalCores();
    try testing.expectEqual(@as(usize, 4), physical.len);
    for (physical) |core| {
        try testing.expect(!core.is_smt_sibling);
        try testing.expectEqual(@as(u32, 0), core.cpu_id % 2);
    }
}

// ---------------------------------------------------------------------------
// Shards and masks
// ---------------------------------------------------------------------------

test "two zones on one socket give two shards that own their own CPUs" {
    const records = try syntheticMachine(testing.allocator, 16, 1, 2, true, 8, &.{ 32768, 32768 });
    defer testing.allocator.free(records);

    var topology = try buildTopology(testing.allocator, records);
    defer topology.deinit();

    try testing.expectEqual(@as(usize, 2), topology.shard_mappings.len);
    try testing.expectEqual(@as(u32, 2), topology.numaNodeCount());
    try testing.expectEqual(@as(u32, 32), topology.total_cpus);
    try testing.expectEqual(@as(u32, 16), topology.total_physical_cores);
    try testing.expectEqual(@as(u32, 64), topology.cache_line_size);

    try expectDomainInvariants(testing.allocator, topology.l3_domains, records);
    try expectDomainInvariants(testing.allocator, topology.l2_domains, records);
    try expectTopologyInvariants(testing.allocator, &topology, records);
}

test "a dual socket with two zones each gives four shards" {
    const records = try syntheticMachine(testing.allocator, 32, 2, 2, true, 8, &.{ 32768, 32768 });
    defer testing.allocator.free(records);

    var topology = try buildTopology(testing.allocator, records);
    defer topology.deinit();

    try testing.expectEqual(@as(usize, 4), topology.shard_mappings.len);
    try testing.expectEqual(@as(u32, 4), topology.numaNodeCount());
    try expectTopologyInvariants(testing.allocator, &topology, records);
}

test "a degraded machine still yields a usable shard" {
    const all = try syntheticMachine(testing.allocator, 16, 1, 2, true, 8, &.{ 32768, 32768 });
    defer testing.allocator.free(all);

    var buffer: [4]CpuRecord = undefined;
    buffer[0] = all[0];
    buffer[1] = all[1];
    buffer[2] = all[16];
    buffer[3] = all[17];
    const records = buffer[0..4];

    var topology = try buildTopology(testing.allocator, records);
    defer topology.deinit();

    try testing.expectEqual(@as(usize, 1), topology.shard_mappings.len);
    // The one domain spans two zones, so it names neither. Zero here means
    // "unknown", which is the only honest answer left.
    try testing.expectEqual(@as(?u32, null), topology.l3_domains[0].node_id);
    try testing.expectEqual(@as(u32, 0), topology.numaNodeCount());
    try testing.expectEqual(@as(usize, 4), topology.shard_mappings[0].preferred_cpus.len);
    try expectTopologyInvariants(testing.allocator, &topology, records);
}

test "sibling classification follows the APIC id, not the CPU id" {
    // APIC ids 0 and 1 are the two threads of core 0. APIC id 8 is the first
    // thread of core 4, so it is a physical core and not a sibling of 0, even
    // though the CPU id 8 sits next to the sibling range.
    try testing.expect(isPrimaryThread(0, 1));
    try testing.expect(!isPrimaryThread(1, 1));
    try testing.expect(isPrimaryThread(8, 1));
    try testing.expect(!isPrimaryThread(9, 1));

    // Low thread bits instead of high ones: 8 & 7 == 0, 4 & 7 == 4.
    try testing.expect(isPrimaryThread(8, 3));
    try testing.expect(!isPrimaryThread(4, 3));

    // No SMT at all: every CPU is a physical core.
    try testing.expect(isPrimaryThread(5, 0));

    // A shift this large is not a real machine, but it must not wrap. Every
    // bit is a thread bit at 32, so only APIC id 0 is a primary thread.
    try testing.expectEqual(@as(u32, 0xFFFF_FFFF), lowMask(32));
    try testing.expectEqual(@as(u32, 0x7FFF_FFFF), lowMask(31));
    try testing.expectEqual(@as(u32, 0), shrink(5, 32));
    try testing.expect(isPrimaryThread(0, 32));
    try testing.expect(!isPrimaryThread(1, 32));
}

/// Compare a shard's bitmap against the bytes it must produce.
fn expectMask(mapping: *const ShardMapping, expected: []const u8) !void {
    const mask = try mapping.getAffinityMask(testing.allocator);
    defer testing.allocator.free(mask);
    try testing.expectEqualSlices(u8, expected, mask);
}

test "the smallest possible mask still gives one preferred CPU" {
    // A single hyperthread (`taskset -c 15`) has no primary thread, and a
    // single primary thread (`taskset -c 0`) has no sibling. Both are one CPU,
    // and one CPU must appear once in the preferred list: the fill that tops a
    // short list up to four used to add a CPU that was already there.
    const all = try syntheticMachine(testing.allocator, 8, 1, 1, true, 8, &.{32768});
    defer testing.allocator.free(all);

    var buffer: [16]CpuRecord = undefined;
    const siblings = onlySiblings(all, &buffer);

    const cases = [_][]const CpuRecord{ siblings[0..1], all[0..1] };
    for (cases) |records| {
        var topology = try buildTopology(testing.allocator, records);
        defer topology.deinit();

        try testing.expectEqual(@as(u32, 1), topology.total_cpus);
        try testing.expectEqual(@as(u32, 1), topology.total_physical_cores);
        try testing.expectEqual(@as(u32, 1), topology.l3_domains[0].physical_cores);
        try testing.expectEqual(@as(usize, 1), topology.shard_mappings[0].preferred_cpus.len);
        try testing.expectEqual(records[0].cpu_id, topology.shard_mappings[0].preferred_cpus[0]);
        try expectTopologyInvariants(testing.allocator, &topology, records);
    }
}

test "affinity mask is sized by the highest CPU, not the last one" {
    var domain = CacheDomain{
        .domain_id = 0,
        .cache_size_kb = 0,
        .cpus = &.{},
        .cpu_list_str = "",
        .node_id = 0,
        .physical_cores = 0,
    };
    var mapping = ShardMapping{
        .shard_id = 0,
        .cache_domain = &domain,
        .preferred_cpus = &.{},
    };

    // A list that ends on a small id after a large one must not undersize.
    var descending = [_]u32{ 9, 1 };
    mapping.preferred_cpus = &descending;
    try expectMask(&mapping, &[_]u8{ 0x02, 0x02 });

    // Byte boundaries: 7 is the last bit of byte 0, 8 the first of byte 1.
    var low = [_]u32{7};
    mapping.preferred_cpus = &low;
    try expectMask(&mapping, &[_]u8{0x80});

    var boundary = [_]u32{ 8, 63 };
    mapping.preferred_cpus = &boundary;
    try expectMask(&mapping, &[_]u8{ 0, 0x01, 0, 0, 0, 0, 0, 0x80 });

    var wide = [_]u32{64};
    mapping.preferred_cpus = &wide;
    try expectMask(&mapping, &[_]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0x01 });

    // Nothing to prefer is an empty mask, not a one-byte mask with no bits.
    mapping.preferred_cpus = &.{};
    try expectMask(&mapping, &[_]u8{});
}

// ---------------------------------------------------------------------------
// This machine
// ---------------------------------------------------------------------------

test "the probe reports a consistent view of this machine" {
    const records = try probeCpus(testing.allocator);
    defer testing.allocator.free(records);

    try expectProbeInvariants(records);
}

test "this machine's topology satisfies every invariant" {
    const records = try probeCpus(testing.allocator);
    defer testing.allocator.free(records);

    var topology = try buildTopology(testing.allocator, records);
    defer topology.deinit();

    try expectDomainInvariants(testing.allocator, topology.l3_domains, records);
    try expectDomainInvariants(testing.allocator, topology.l2_domains, records);
    try expectTopologyInvariants(testing.allocator, &topology, records);

    // Hash-based lookup and hashed-key lookup agree, on any key.
    for ([_][]const u8{ "test_key", "", "a" }) |key| {
        const hash = CacheTopology.hashKey(key);
        try testing.expectEqual(topology.getShardForData(hash).shard_id, topology.getShardForKey(key).shard_id);
    }
}

test "detecting topology leaves the process affinity mask alone" {
    // The probe pins the thread once per CPU to ask it about itself. If it
    // forgets to put the mask back, every thread in the process is stuck on
    // one CPU, which no other test would notice and production would feel.
    //
    // The mask is narrowed to two CPUs first, for two reasons: this test then
    // does not depend on what earlier tests did to it, and a probe that fails
    // to restore leaves a mask of one CPU, which is a visible difference. A
    // one-CPU mask would hide the bug.
    var before: os_compat.CpuSet = undefined;
    if (!os_compat.cpuAffinityGet(&before)) return error.SkipZigTest;
    defer _ = os_compat.cpuAffinitySet(&before);

    var two = std.mem.zeroes(os_compat.CpuSet);
    var picked: u32 = 0;
    var cpu: u32 = 0;
    while (cpu < os_compat.max_cpus and picked < 2) : (cpu += 1) {
        if (os_compat.cpuSetContains(&before, cpu)) {
            os_compat.cpuSetPut(&two, cpu);
            picked += 1;
        }
    }
    // A process already narrowed to one CPU cannot show the difference, and
    // is itself a sign that something upstream went wrong.
    if (picked < 2) return error.SkipZigTest;
    if (!os_compat.cpuAffinitySet(&two)) return error.SkipZigTest;

    var topology = try analyzeCacheTopology(testing.allocator);
    defer topology.deinit();

    var after: os_compat.CpuSet = undefined;
    try testing.expect(os_compat.cpuAffinityGet(&after));
    try testing.expectEqualSlices(u8, std.mem.asBytes(&two), std.mem.asBytes(&after));

    // And the report describes the mask that was in force while probing, not
    // whatever the probe happened to leave behind.
    try testing.expectEqual(@as(u32, 2), topology.total_cpus);
}
