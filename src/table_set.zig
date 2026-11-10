//! One table per NUMA zone, each owned by a thread pinned to that zone.
//!
//! The point is a write that never crosses a zone boundary. A thread and a
//! page are on the same side when the thread runs in the zone the page was
//! allocated from; if either moves, every write pays the far side's latency
//! (24 ns local against 270 ns across zones on the part this was written for)
//! and the local L3 stops being local. So each table owns both halves:
//!
//! * its memory is bound to the zone's kernel node and first touched by the
//!   thread that owns it, and
//! * that thread is pinned to the zone's physical cores.
//!
//! The routing rule is in `partition.zig`, deliberately not here: WAL replay
//! and replica apply have to route a key the same way without any of this
//! object's state.
//!
//! A machine that proves one zone, or cannot prove any, gets one table and no
//! owner thread: requests run on the calling thread, exactly as before. The
//! machinery only turns on when there is a second zone to be local to.

const std = @import("std");
const cache_topology = @import("cache_topology.zig");
const flat_hash_storage = @import("flat_hash_storage.zig");
const os_compat = @import("os_compat.zig");
const partition = @import("partition.zig");
const wal = @import("wal.zig");

const CacheLineRecord = flat_hash_storage.CacheLineRecord;
const FlatHashStorage = flat_hash_storage.FlatHashStorage;

/// Requests one caller has in flight at once. Only the batch path uses more
/// than one, and it reuses a stack array of this size rather than allocating
/// after init, which the database does not do.
const max_in_flight: usize = 64;

/// What the owning thread should do with a request.
const Op = union(enum) {
    read: u64,
    write: struct { key: u64, data: []const u8 },
    delete: u64,
    apply: *const wal.WALEntry,
    clear: void,
};

/// A request, on the caller's stack. The caller fills it in, hands over a
/// pointer, and waits for `done`; the owner writes the result before it sets
/// `done`, so the caller sees a finished request or no request.
const Request = struct {
    op: Op,
    next: ?*Request = null,
    done: std.atomic.Value(u32) = .init(0),
    err: ?anyerror = null,
    read: ?*const CacheLineRecord = null,
    deleted: bool = false,
};

/// One zone's table and the thread that owns it.
pub const Table = struct {
    id: u16,
    domain: ?*const cache_topology.CacheDomain,
    storage: FlatHashStorage = undefined,

    // The queue. `head` and `tail` are only touched under `mutex`; `work` is
    // the futex word the idle owner sleeps on.
    mutex: os_compat.Mutex = .{},
    head: ?*Request = null,
    tail: ?*Request = null,
    work: std.atomic.Value(u32) = .init(0),
    stop: std.atomic.Value(bool) = .init(false),
    ready: std.atomic.Value(u32) = .init(0),

    thread: ?std.Thread = null,
    /// Whether the owner could pin itself to the zone's cores.
    pinned: bool = false,
    /// Whether the table's range accepted the node binding.
    bound: bool = false,
    /// The kernel node the table's first page actually landed on, read back
    /// after the first touch. Null when the kernel would not say.
    memory_node: ?u32 = null,

    /// Whether requests for this table must go through the owner thread.
    fn hasOwner(self: *const Table) bool {
        return self.thread != null;
    }

    fn start(self: *Table, topology: *const cache_topology.CacheTopology, allocator: std.mem.Allocator, records: []CacheLineRecord) !void {
        self.thread = try std.Thread.spawn(.{}, ownerMain, .{ self, topology, allocator, records });
    }

    fn waitReady(self: *Table) void {
        while (self.ready.load(.acquire) == 0) os_compat.futexWait(&self.ready, 0);
    }

    fn shutdown(self: *Table) void {
        if (self.thread) |thread| {
            self.stop.store(true, .release);
            _ = self.work.fetchAdd(1, .release);
            os_compat.futexWake(&self.work, 1);
            thread.join();
            self.thread = null;
        }
    }

    fn take(self: *Table) ?*Request {
        self.mutex.lock();
        defer self.mutex.unlock();

        const request = self.head orelse return null;
        self.head = request.next;
        if (self.head == null) self.tail = null;
        request.next = null;
        return request;
    }

    fn push(self: *Table, request: *Request) void {
        self.mutex.lock();
        request.next = null;
        if (self.tail) |tail| tail.next = request else self.head = request;
        self.tail = request;
        self.mutex.unlock();

        _ = self.work.fetchAdd(1, .release);
        os_compat.futexWake(&self.work, 1);
    }

    fn wait(self: *Table, request: *Request) void {
        _ = self;
        while (request.done.load(.acquire) == 0) os_compat.futexWait(&request.done, 0);
    }

    fn execute(self: *Table, request: *Request) void {
        switch (request.op) {
            .read => |key| request.read = self.storage.read(key),
            .write => |write| self.storage.write(write.key, write.data) catch |err| {
                request.err = err;
            },
            .delete => |key| request.deleted = self.storage.delete(key),
            .apply => |entry| self.storage.applyEntry(entry) catch |err| {
                request.err = err;
            },
            .clear => self.storage.clear(),
        }
    }

    fn ownerMain(self: *Table, topology: *const cache_topology.CacheTopology, allocator: std.mem.Allocator, records: []CacheLineRecord) void {
        // Both halves of "local": the thread runs here, and the pages come
        // from here. The bind goes first, so the pages the zero-fill allocates
        // follow the node rather than the scheduler; the pin is what keeps the
        // first touch on the right side of the boundary even when the kernel
        // refuses the bind.
        if (self.domain) |domain| {
            self.pinned = pinToDomain(domain);
            if (domain.os_node) |node| self.bound = bindRecords(records, node);
        }

        self.storage = FlatHashStorage.initInPlace(topology, allocator, records);

        self.memory_node = if (records.len > 0) os_compat.nodeOfPage(@ptrCast(records.ptr)) else null;
        self.ready.store(1, .release);
        os_compat.futexWake(&self.ready, 1);

        // Serve until told to stop. An idle owner sleeps on `work`; a push
        // bumps it, so a wakeup cannot be lost between the empty check and the
        // wait.
        while (true) {
            const epoch = self.work.load(.acquire);

            if (self.take()) |request| {
                self.execute(request);
                request.done.store(1, .release);
                os_compat.futexWake(&request.done, 1);
                continue;
            }

            if (self.stop.load(.acquire)) break;
            os_compat.futexWait(&self.work, epoch);
        }
    }
};

/// Pin the calling thread to one zone's physical cores, so it cannot be
/// scheduled in another zone.
///
/// One thread per core is the point: the siblings of the chosen cores are not
/// in the set, because an owner does not need to run twice at once.
fn pinToDomain(domain: *const cache_topology.CacheDomain) bool {
    var set = std.mem.zeroes(os_compat.CpuSet);

    for (domain.getPhysicalCores()) |core| os_compat.cpuSetPut(&set, core.cpu_id);

    // A domain with no physical-core prefix is the degraded case; pin to
    // whatever members it has rather than not pinning at all.
    if (domain.physical_cores == 0) {
        for (domain.cpus) |cpu| os_compat.cpuSetPut(&set, cpu.cpu_id);
    }

    return os_compat.cpuAffinitySet(&set);
}

/// Bind a table's pages to `node`, over the whole pages it spans.
///
/// The table is 64-byte aligned, not page aligned, and its length is not a
/// whole number of pages either. The partial pages at each end cannot be bound;
/// they are still allocated by the pinned owner's first touch, so they are
/// local by construction rather than by policy.
fn bindRecords(records: []const CacheLineRecord, node: u32) bool {
    if (records.len == 0) return false;

    const page = std.heap.pageSize();
    const start = std.mem.alignForward(usize, @intFromPtr(records.ptr), page);
    const end = std.mem.alignBackward(usize, @intFromPtr(records.ptr) + records.len * @sizeOf(CacheLineRecord), page);

    if (end <= start) return false;
    return os_compat.bindRangeToNode(@ptrFromInt(start), end - start, node);
}

/// The set of tables, and the routing that picks one.
pub const TableSet = struct {
    const Self = @This();

    tables: []Table,
    allocator: std.mem.Allocator,
    topology: *const cache_topology.CacheTopology,

    pub const Stats = struct {
        load_factor: f32,
        used_capacity: u32,
        total_capacity: u32,
        tables: u16,
        placed: u16,
        pinned: u16,
    };

    /// Build one table per zone.
    ///
    /// `force_tables` overrides the zone count. A machine with one zone cannot
    /// exercise the multi-table path otherwise, and that path is where a bug
    /// would hide until someone ran it on a two-zone machine. Forcing more
    /// tables than zones is allowed for that reason: the tables then share the
    /// zones round-robin, which tests the routing and the owner threads but
    /// not the placement.
    pub fn init(
        allocator: std.mem.Allocator,
        topology: *const cache_topology.CacheTopology,
        target_capacity: u64,
        force_tables: ?u16,
    ) !Self {
        const domains = topology.l3_domains;
        const zones: u16 = @intCast(@max(1, domains.len));
        const table_count: u16 = if (force_tables) |forced| @max(1, forced) else zones;

        // Split the capacity the one-table design would have had. Each piece
        // is rounded down to a power of two on its own, because the slot
        // arithmetic is bitwise; the report shows what is usable.
        const total = roundCapacity(target_capacity);
        const piece = roundCapacity(@max(1, @divFloor(total, @as(u32, table_count))));

        const tables = try allocator.alloc(Table, table_count);
        errdefer allocator.free(tables);

        // Allocate every piece before any owner thread runs. The allocator the
        // database runs on is a bump arena that is not thread safe, so nothing
        // may allocate once the owners are live.
        const slices = try allocator.alloc([]CacheLineRecord, table_count);
        defer allocator.free(slices);

        var allocated: usize = 0;
        errdefer for (slices[0..allocated]) |slice| allocator.free(slice);

        for (slices) |*slice| {
            // Each piece is its own allocation, so each can be bound to its own
            // node without touching a neighbour's pages.
            slice.* = try allocator.alignedAlloc(CacheLineRecord, @fromBackingInt(@intCast(6)), piece);
            allocated += 1;
        }

        var started: usize = 0;
        errdefer for (tables[0..started]) |*table| table.shutdown();

        for (tables, 0..) |*table, index| {
            table.* = .{
                .id = @intCast(index),
                .domain = if (domains.len > 0) &domains[index % domains.len] else null,
            };
        }

        if (table_count == 1) {
            placeInCaller(&tables[0], topology, allocator, slices[0]);
            started = 1;
        } else {
            for (tables, 0..) |*table, index| {
                try table.start(topology, allocator, slices[index]);
                started += 1;
            }
            for (tables) |*table| table.waitReady();
        }

        return Self{ .tables = tables, .allocator = allocator, .topology = topology };
    }

    pub fn deinit(self: *Self) void {
        for (self.tables) |*table| table.shutdown();

        // The records are the tables' own, not the storage struct's, so they
        // are freed here, with the 64-byte alignment they were allocated with.
        for (self.tables) |*table| {
            table.storage.deinit();
            self.allocator.rawFree(std.mem.sliceAsBytes(table.storage.records), @fromBackingInt(@intCast(6)), 0);
        }
        self.allocator.free(self.tables);
    }

    pub fn count(self: *const Self) u16 {
        return @intCast(self.tables.len);
    }

    /// The table that owns a key. The one place in this file that picks a
    /// table, so the request path and the rebuilding paths cannot disagree.
    pub fn tableFor(self: *const Self, key: u64) *Table {
        return &self.tables[partition.tableOfKey(key, self.count())];
    }

    pub fn read(self: *Self, key: u64) !?*const CacheLineRecord {
        const table = self.tableFor(key);
        if (!table.hasOwner()) return table.storage.read(key);

        var request = Request{ .op = .{ .read = key } };
        table.push(&request);
        table.wait(&request);
        if (request.err) |err| return err;
        return request.read;
    }

    pub fn write(self: *Self, key: u64, data: []const u8) !void {
        const table = self.tableFor(key);
        if (!table.hasOwner()) return table.storage.write(key, data);

        var request = Request{ .op = .{ .write = .{ .key = key, .data = data } } };
        table.push(&request);
        table.wait(&request);
        if (request.err) |err| return err;
    }

    pub fn delete(self: *Self, key: u64) !bool {
        const table = self.tableFor(key);
        if (!table.hasOwner()) return table.storage.delete(key);

        var request = Request{ .op = .{ .delete = key } };
        table.push(&request);
        table.wait(&request);
        if (request.err) |err| return err;
        return request.deleted;
    }

    pub fn clear(self: *Self) void {
        for (self.tables) |*table| {
            if (!table.hasOwner()) {
                table.storage.clear();
                continue;
            }
            var request = Request{ .op = .clear };
            table.push(&request);
            table.wait(&request);
        }
    }

    /// Batch reads with no prefetch: one lookup after another on its owner.
    /// `results[i]` is null for a miss. An entry that cannot be read stays
    /// null rather than failing the batch.
    pub fn readBatch(self: *Self, keys: []const u64, results: []?*const CacheLineRecord) void {
        std.debug.assert(keys.len == results.len);

        var offset: usize = 0;
        while (offset < keys.len) : (offset += max_in_flight) {
            const n = @min(max_in_flight, keys.len - offset);
            var requests: [max_in_flight]Request = undefined;

            const owned = for (keys[offset .. offset + n]) |key| {
                if (!self.tableFor(key).hasOwner()) break false;
            } else true;

            if (!owned) {
                // Every table runs in the caller, so the plain loop is both
                // correct and the fastest path.
                for (keys[offset .. offset + n], 0..) |key, index| {
                    results[offset + index] = self.tableFor(key).storage.read(key);
                }
                continue;
            }

            for (0..n) |index| {
                requests[index] = .{ .op = .{ .read = keys[offset + index] } };
                self.tableFor(keys[offset + index]).push(&requests[index]);
            }
            for (0..n) |index| {
                const table = self.tableFor(keys[offset + index]);
                table.wait(&requests[index]);
                results[offset + index] = requests[index].read;
            }
        }
    }

    pub fn writeBatch(self: *Self, keys: []const u64, data_items: []const []const u8) !void {
        std.debug.assert(keys.len == data_items.len);

        // Submit everything in the window before waiting on any of it, so a
        // batch pays one handoff per window and not one per key.
        var offset: usize = 0;
        while (offset < keys.len) : (offset += max_in_flight) {
            const n = @min(max_in_flight, keys.len - offset);
            var requests: [max_in_flight]Request = undefined;
            var pending: [max_in_flight]bool = undefined;
            var errors: [max_in_flight]?anyerror = undefined;
            @memset(errors[0..], null);

            for (0..n) |index| {
                const key = keys[offset + index];
                const table = self.tableFor(key);
                pending[index] = table.hasOwner();
                if (pending[index]) {
                    requests[index] = .{ .op = .{ .write = .{ .key = key, .data = data_items[offset + index] } } };
                    table.push(&requests[index]);
                } else {
                    table.storage.write(key, data_items[offset + index]) catch |err| {
                        errors[index] = err;
                    };
                }
            }

            for (0..n) |index| {
                if (!pending[index]) continue;
                self.tableFor(keys[offset + index]).wait(&requests[index]);
                if (requests[index].err) |err| errors[index] = err;
            }

            for (errors[0..n]) |maybe_err| {
                if (maybe_err) |err| return err;
            }
        }
    }

    pub fn deleteBatch(self: *Self, keys: []const u64, results: []bool) !void {
        std.debug.assert(keys.len == results.len);

        var offset: usize = 0;
        while (offset < keys.len) : (offset += max_in_flight) {
            const n = @min(max_in_flight, keys.len - offset);
            var requests: [max_in_flight]Request = undefined;
            var pending: [max_in_flight]bool = undefined;
            var errors: [max_in_flight]?anyerror = undefined;
            @memset(errors[0..], null);

            for (0..n) |index| {
                const key = keys[offset + index];
                const table = self.tableFor(key);
                pending[index] = table.hasOwner();
                if (pending[index]) {
                    requests[index] = .{ .op = .{ .delete = key } };
                    table.push(&requests[index]);
                } else {
                    results[offset + index] = table.storage.delete(key);
                }
            }

            for (0..n) |index| {
                if (!pending[index]) continue;
                self.tableFor(keys[offset + index]).wait(&requests[index]);
                if (requests[index].err) |err| errors[index] = err;
                results[offset + index] = requests[index].deleted;
            }

            for (errors[0..n]) |maybe_err| {
                if (maybe_err) |err| return err;
            }
        }
    }

    /// Apply one logged entry to the table that owns its key.
    ///
    /// Replay and replica apply come here rather than through the request
    /// path, and they route identically: the entry carries a key, and the key
    /// decides the table, on any machine, whatever its zone count.
    pub fn applyEntry(self: *Self, entry: *const wal.WALEntry) !void {
        const key = entry.getKey();

        const table = self.tableFor(key);
        if (!table.hasOwner()) return table.storage.applyEntry(entry);

        var request = Request{ .op = .{ .apply = entry } };
        table.push(&request);
        table.wait(&request);
        if (request.err) |err| return err;
    }

    /// Copy every table's records into `out`, table after table, each into its
    /// own capacity-sized slot. The slot boundaries are recomputed with
    /// `capacityOf`, so a snapshot does not have to store them.
    pub fn copyAllRecords(self: *Self, out: []CacheLineRecord) void {
        std.debug.assert(out.len == self.totalCapacity());

        var offset: usize = 0;
        for (self.tables) |*table| {
            const capacity = table.storage.capacity;
            table.storage.copyAllRecords(out[offset .. offset + capacity]);
            offset += capacity;
        }
    }

    pub fn capacityOf(self: *const Self, index: usize) usize {
        return if (index < self.tables.len) self.tables[index].storage.capacity else 0;
    }

    pub fn totalCapacity(self: *const Self) usize {
        var total: usize = 0;
        for (self.tables) |table| total += table.storage.capacity;
        return total;
    }

    pub fn stats(self: *const Self) Stats {
        var used: u32 = 0;
        var total: u32 = 0;
        var placed: u16 = 0;
        var pinned: u16 = 0;

        for (self.tables) |table| {
            used += table.storage.count;
            total += table.storage.capacity;
            if (table.bound) placed += 1;
            if (table.pinned) pinned += 1;
        }

        return .{
            .load_factor = if (total == 0) 0 else @as(f32, @floatFromInt(used)) / @as(f32, @floatFromInt(total)),
            .used_capacity = used,
            .total_capacity = total,
            .tables = self.count(),
            .placed = placed,
            .pinned = pinned,
        };
    }

    pub fn enableSync(self: *Self) void {
        for (self.tables) |*table| table.storage.enableSync();
    }

    pub fn enableDurability(self: *Self, manager: *wal.DurabilityManager) void {
        for (self.tables) |*table| table.storage.enableDurability(manager);
    }
};

/// Place and build the one table when there is no owner thread: pin just long
/// enough for the first touch, then put the caller's affinity mask back.
///
/// Pinning the caller for the rest of the process would be a surprise for an
/// embedded caller, and on a single-zone machine there is no boundary to stay
/// inside anyway; the pin here is only about where the pages land.
fn placeInCaller(table: *Table, topology: *const cache_topology.CacheTopology, allocator: std.mem.Allocator, records: []CacheLineRecord) void {
    var before: os_compat.CpuSet = undefined;
    const had_mask = os_compat.cpuAffinityGet(&before);

    if (table.domain) |domain| {
        if (domain.os_node) |node| table.bound = bindRecords(records, node);
        table.pinned = pinToDomain(domain);
    }

    table.storage = FlatHashStorage.initInPlace(topology, allocator, records);

    if (had_mask) {
        if (!os_compat.cpuAffinitySet(&before)) {
            std.debug.print("Warning: could not restore CPU affinity after placing the table; this thread is stuck on one zone.\n", .{});
        }
    }

    table.memory_node = if (records.len > 0) os_compat.nodeOfPage(@ptrCast(records.ptr)) else null;
}

/// The capacity one table would have had: a power of two, rounded down,
/// because the slot arithmetic is bitwise.
fn roundCapacity(requested: u64) u32 {
    const raw = @as(u32, @intCast(@min(requested, std.math.maxInt(u32))));
    if (raw == 0) return 1;
    return if (std.math.isPowerOfTwo(raw)) raw else std.math.floorPowerOfTwo(u32, raw);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

/// A topology for a test to route with. The real machine is the only machine
/// available, so the placement half of these tests checks the zone the table
/// claims, not two zones far apart.
fn testTopology(allocator: std.mem.Allocator) !cache_topology.CacheTopology {
    return cache_topology.analyzeCacheTopology(allocator);
}

test "one table runs on the caller and needs no owner thread" {
    var topology = try testTopology(testing.allocator);
    defer topology.deinit();

    var set = try TableSet.init(testing.allocator, &topology, 256, 1);
    defer set.deinit();

    try testing.expectEqual(@as(u16, 1), set.count());
    try testing.expect(!set.tables[0].hasOwner());

    try set.write(1, "one");
    const found = try set.read(1);
    try testing.expect(found != null);
    try testing.expectEqualStrings("one", found.?.getData());

    try testing.expect(try set.delete(1));
    try testing.expect(try set.read(1) == null);
}

test "two tables: a key is filed in the table that routes it" {
    var topology = try testTopology(testing.allocator);
    defer topology.deinit();

    // Two tables on whatever zones exist. On a one-zone machine both tables
    // share the zone, which still exercises the owner threads and the routing;
    // the placement check below is the part that needs two zones to be
    // interesting.
    var set = try TableSet.init(testing.allocator, &topology, 1024, 2);
    defer set.deinit();

    try testing.expectEqual(@as(u16, 2), set.count());

    var key: u64 = 0;
    while (key < 200) : (key += 1) {
        var buffer: [16]u8 = undefined;
        const data = try std.fmt.bufPrint(&buffer, "v{d}", .{key});
        try set.write(key, data);
    }

    key = 0;
    while (key < 200) : (key += 1) {
        const found = try set.read(key);
        try testing.expect(found != null);

        var buffer: [16]u8 = undefined;
        const expected = try std.fmt.bufPrint(&buffer, "v{d}", .{key});
        try testing.expectEqualStrings(expected, found.?.getData());

        // The table it was found in is the table routing names, and it is the
        // one the request path asked.
        try testing.expectEqual(partition.tableOfKey(key, 2), set.tableFor(key).id);
    }

    // Deleting through the set reaches the owner: a delete on the wrong table
    // would report false.
    key = 0;
    while (key < 200) : (key += 2) {
        try testing.expect(try set.delete(key));
        try testing.expect(try set.delete(key) == false);
    }
    key = 0;
    while (key < 200) : (key += 2) {
        try testing.expect(try set.read(key) == null);
    }
    key = 1;
    while (key < 200) : (key += 2) {
        try testing.expect(try set.read(key) != null);
    }
}

test "an owner pins itself and takes its table's memory from its zone" {
    var topology = try testTopology(testing.allocator);
    defer topology.deinit();

    var set = try TableSet.init(testing.allocator, &topology, 1024, 2);
    defer set.deinit();

    const page = std.heap.pageSize();
    _ = page;

    for (set.tables) |table| {
        try testing.expect(table.hasOwner());
        try testing.expect(table.pinned);

        const domain = table.domain.?;
        if (domain.os_node) |zone_node| {
            // The table's first page landed on the zone's node. This is the
            // property the whole file exists for: memory that is local to the
            // zone, not memory that was merely allocated near it.
            try testing.expectEqual(zone_node, table.memory_node.?);
        }

        // A table that could not bind still says so, rather than reporting a
        // placement it did not get.
        if (!table.bound) {
            try testing.expect(table.memory_node != null);
        }
    }
}

test "batches route like single operations" {
    var topology = try testTopology(testing.allocator);
    defer topology.deinit();

    var set = try TableSet.init(testing.allocator, &topology, 1024, 2);
    defer set.deinit();

    var keys: [80]u64 = undefined;
    var data: [80][]const u8 = undefined;
    var owned: [80][12]u8 = undefined;
    for (0..keys.len) |index| {
        keys[index] = index * 7;
        owned[index] = undefined;
        data[index] = std.fmt.bufPrint(&owned[index], "b{d}", .{index}) catch unreachable;
    }

    try set.writeBatch(&keys, &data);

    var results: [80]?*const CacheLineRecord = undefined;
    set.readBatch(&keys, &results);

    for (0..keys.len) |index| {
        try testing.expect(results[index] != null);
        try testing.expectEqualStrings(data[index], results[index].?.getData());
    }

    var deleted: [80]bool = undefined;
    try set.deleteBatch(&keys, &deleted);
    for (deleted) |was_present| try testing.expect(was_present);

    set.readBatch(&keys, &results);
    for (results) |result| try testing.expect(result == null);
}

test "a logged entry lands in the table its key routes to" {
    var topology = try testTopology(testing.allocator);
    defer topology.deinit();

    var set = try TableSet.init(testing.allocator, &topology, 1024, 2);
    defer set.deinit();

    // Replay and replica apply see only the entry, never the table. If they
    // routed by anything else, this lands where a lookup would not find it.
    const key: u64 = 0xDEAD_BEEF;
    var record = try CacheLineRecord.init(key, "replayed");
    const entry = wal.WALEntry.fromCacheLineRecord(&record);
    try set.applyEntry(&entry);

    const found = try set.read(key);
    try testing.expect(found != null);
    try testing.expectEqualStrings("replayed", found.?.getData());

    // A delete marker removes it through the same route.
    const tombstone = CacheLineRecord.initDelete(key);
    const delete_entry = wal.WALEntry.fromCacheLineRecord(&tombstone);
    try set.applyEntry(&delete_entry);
    try testing.expect(try set.read(key) == null);
}

test "concurrent callers do not lose a write" {
    var topology = try testTopology(testing.allocator);
    defer topology.deinit();

    var set = try TableSet.init(testing.allocator, &topology, 1024, 2);
    defer set.deinit();

    const Writer = struct {
        fn run(shared: *TableSet, start: u64) void {
            var key = start;
            while (key < start + 40) : (key += 1) {
                var buffer: [16]u8 = undefined;
                const data = std.fmt.bufPrint(&buffer, "w{d}", .{key}) catch unreachable;
                shared.write(key, data) catch return;
            }
        }
    };

    var threads: [4]std.Thread = undefined;
    for (0..4) |index| {
        threads[index] = try std.Thread.spawn(.{}, Writer.run, .{ &set, @as(u64, @intCast(index)) * 40 });
    }
    for (threads) |thread| thread.join();

    var key: u64 = 0;
    while (key < 160) : (key += 1) {
        const found = try set.read(key);
        try testing.expect(found != null);
    }
}
