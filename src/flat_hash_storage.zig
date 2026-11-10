const std = @import("std");
const cache_topology = @import("cache_topology.zig");
const os_compat = @import("os_compat.zig");

// Cache-line-aligned record that fits exactly in one cache line
pub const CacheLineRecord = extern struct {
    key: u64, // 8 bytes (check first for key comparison)
    metadata: u32, // 4 bytes: 1 bit valid flag + 6 bits length + 25 bits reserved
    data: [52]u8, // 52 bytes data payload (accessed last)
    // Total: 64 bytes = exactly 1 cache line

    const Self = @This();
    pub const VALID_FLAG: u32 = 1 << 31; // Use top bit for valid flag
    pub const LENGTH_MASK: u32 = 0x3F; // Bottom 6 bits for length (0-63, enough for 52)
    /// Marks a record as a removal rather than a stored value.
    ///
    /// A delete cannot be encoded as a length-0 record, because storing an
    /// empty value is legal and reads back as one. It needs a bit of its own.
    /// Bits 6 to 30 of `metadata` were unused, so this takes bit 6, which is
    /// what keeps WAL files written before it readable: such a record has 0
    /// here and still reads as a write.
    ///
    /// A marker belongs in the log only. A live table never holds one,
    /// because `isValid` reports true for it, and an occupied slot that no
    /// probe can reach is exactly the failure `FlatHashStorage.delete` fixes.
    pub const DELETE_FLAG: u32 = 1 << 6;

    pub fn init(key: u64, data: []const u8) !Self {
        if (data.len > 52) return error.DataTooLarge;

        var record = Self{
            .metadata = VALID_FLAG | @as(u32, @intCast(data.len)),
            .key = key,
            .data = std.mem.zeroes([52]u8),
        };

        @memcpy(record.data[0..data.len], data);
        return record;
    }

    pub fn initEmpty() Self {
        return Self{
            .metadata = 0, // Invalid (no VALID_FLAG bit set)
            .key = 0,
            .data = std.mem.zeroes([52]u8),
        };
    }

    /// The record that says "remove `key`", for the log only.
    ///
    /// The length is 0 and the payload is zeroed: a delete carries a key and
    /// nothing else. `FlatHashStorage.applyEntry` reads the key and ignores
    /// the payload.
    pub fn initDelete(key: u64) Self {
        return Self{
            .metadata = VALID_FLAG | DELETE_FLAG,
            .key = key,
            .data = std.mem.zeroes([52]u8),
        };
    }

    /// Whether this record is a delete marker rather than a stored value.
    ///
    /// Meaningful only alongside `isValid`: a record that is neither valid nor
    /// marked is the gap left between batches in a WAL, not an entry.
    pub fn isDelete(self: *const Self) bool {
        return (self.metadata & DELETE_FLAG) != 0;
    }

    pub fn isValid(self: *const Self) bool {
        return (self.metadata & VALID_FLAG) != 0;
    }

    /// Clear the valid bit on this record, in place.
    ///
    /// This is the record-level primitive only. Clearing one slot of an open
    /// table is not a delete: it leaves a hole that ends every probe chain
    /// running through it, which strands the keys behind it. Call
    /// `FlatHashStorage.delete`, which closes the run.
    pub fn markInvalid(self: *Self) void {
        self.metadata &= ~VALID_FLAG;
    }

    pub fn getData(self: *const Self) []const u8 {
        const len = @as(u32, @intCast(self.metadata & LENGTH_MASK));
        return self.data[0..len];
    }

    pub fn getKey(self: *const Self) u64 {
        return self.key;
    }
};

// Unified hash function for both string and integer keys
pub fn hashKey(key: anytype) u64 {
    const KeyType = @TypeOf(key);

    if (KeyType == []const u8) {
        // For string keys, use Wyhash directly
        return std.hash.Wyhash.hash(0, key);
    } else if (KeyType == u64) {
        // For u64 keys, use MurmurHash3 finalizer for good distribution
        var h = key;
        h ^= h >> 33;
        h *%= 0xff51afd7ed558ccd;
        h ^= h >> 33;
        h *%= 0xc4ceb9fe1a85ec53;
        h ^= h >> 33;
        return h;
    } else {
        @compileError("Unsupported key type");
    }
}

// Forward declaration for optional WAL
const wal = @import("wal.zig");

// Flat hash table with linear probing - no artificial capacity limits
pub const FlatHashStorage = struct {
    const Self = @This();

    records: []CacheLineRecord, // Flat cache-aligned array (alignment handled in allocator)
    capacity: u32,
    count: u32,
    topology: *const cache_topology.CacheTopology,
    allocator: std.mem.Allocator,

    // Mutual exclusion for the table. Off by default: the storage is written
    // to be driven by one thread, and that thread pays nothing for a guard it
    // does not need. Anything that touches the table from a second thread —
    // replica apply, a snapshot walk, a concurrent embedded caller — must call
    // `enableSync` before it starts.
    sync_enabled: bool,
    sync_mutex: os_compat.Mutex,

    // Optional WAL for durability - null means no durability
    durability_manager: ?*wal.DurabilityManager,

    // L3 domains the machine reports. The whole table is one allocation, so
    // this is a report, not a placement decision: nothing here splits records
    // across zones. A caller that wants per-zone placement has to allocate
    // per zone first.
    l3_domains: u32,

    pub fn init(allocator: std.mem.Allocator, topology: *const cache_topology.CacheTopology, target_capacity: u64) !Self {
        // Enforce power-of-2 capacity for bitwise operations
        // Round DOWN to largest power-of-2 that fits in L3 cache
        const raw_capacity = @as(u32, @intCast(target_capacity));
        const capacity = if (std.math.isPowerOfTwo(raw_capacity))
            raw_capacity
        else
            std.math.floorPowerOfTwo(u32, raw_capacity);

        // Allocate flat cache-aligned array
        // Note: CacheLineRecord is designed for 64-byte cache lines, which matches most modern CPUs
        // If topology shows different cache line size, we should validate compatibility
        if (topology.cache_line_size != 64) {
            std.debug.print("Warning: Detected cache line size {} != 64 bytes. Performance may be suboptimal.\n", .{topology.cache_line_size});
        }
        const records = try allocator.alignedAlloc(CacheLineRecord, @fromBackingInt(@intCast(6)), capacity); // 2^6 = 64

        // Initialize all records as invalid/empty
        for (records) |*record| {
            record.* = CacheLineRecord.initEmpty();
        }

        return Self{
            .records = records,
            .capacity = capacity,
            .count = 0,
            .topology = topology,
            .allocator = allocator,
            .sync_enabled = false,
            .sync_mutex = os_compat.Mutex{},
            .durability_manager = null, // Initially no WAL
            .l3_domains = @intCast(topology.l3_domains.len),
        };
    }

    /// Turn on mutual exclusion for the table.
    ///
    /// The hot path is written for one thread and relies on that: `count` is a
    /// plain integer, and a delete shifts records that a concurrent probe may
    /// be walking. Replication and snapshots put a second thread on the table,
    /// so they call this before that thread starts. The guard costs one
    /// predictable branch per operation when it is off, and a mutex when it is
    /// on; there is no third state.
    pub fn enableSync(self: *Self) void {
        self.sync_enabled = true;
    }

    /// Turn the guard off again. Only safe once no second thread can reach the
    /// table.
    pub fn disableSync(self: *Self) void {
        self.sync_enabled = false;
    }

    inline fn lockSync(self: *const Self) void {
        if (self.sync_enabled) @constCast(&self.sync_mutex).lock();
    }

    inline fn unlockSync(self: *const Self) void {
        if (self.sync_enabled) @constCast(&self.sync_mutex).unlock();
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.records);
    }

    // Get hash position with linear probing for reads
    fn findRecord(self: *const Self, key: u64) ?u32 {
        if (self.count == 0) return null;

        const start_pos = @as(u32, @intCast(hashKey(key) & (self.capacity - 1)));
        var pos = start_pos;

        // Linear probe until we find the key or an empty slot
        // Use bitwise AND for faster wraparound (capacity is power-of-2)
        while (true) {
            const record = &self.records[pos];

            if (!record.isValid()) {
                // Hit empty slot - key doesn't exist
                return null;
            }

            if (record.key == key) {
                // Found it!
                return pos;
            }

            // Continue probing with bitwise wraparound
            pos = (pos + 1) & (self.capacity - 1);

            // If we've wrapped around to start, key doesn't exist
            if (pos == start_pos) break;
        }

        return null;
    }

    // Find empty slot for insertion with linear probing
    fn findEmptySlot(self: *const Self, key: u64) ?u32 {
        if (self.count >= self.capacity) return null; // Table full

        const start_pos = @as(u32, @intCast(hashKey(key) & (self.capacity - 1)));
        var pos = start_pos;

        // Linear probe until we find an empty slot or the key (for updates)
        // Use bitwise AND for faster wraparound (capacity is power-of-2)
        while (true) {
            const record = &self.records[pos];

            if (!record.isValid() or record.key == key) {
                // Found empty slot or existing key to update
                return pos;
            }

            // Continue probing with bitwise wraparound
            pos = (pos + 1) & (self.capacity - 1);

            // Wrapped around - shouldn't happen if count < capacity
            if (pos == start_pos) break;
        }

        return null; // Should never happen if count < capacity
    }

    pub inline fn read(self: *const Self, key: u64) ?*const CacheLineRecord {
        self.lockSync();
        defer self.unlockSync();
        return self.readLocked(key);
    }

    inline fn readLocked(self: *const Self, key: u64) ?*const CacheLineRecord {
        const pos = self.findRecord(key) orelse return null;
        return &self.records[pos];
    }

    /// Walk one probe chain whose first line is already on its way in.
    ///
    /// `start_pos` is the home slot from `hashKey`, and the caller prefetched
    /// that line. If the home line does not hold the key, the following two
    /// lines are requested before the walk continues: one line of lookahead
    /// still leaves the next compare waiting on a miss, two overlap it.
    inline fn probeFrom(self: *const Self, key: u64, start_pos: u32) ?*const CacheLineRecord {
        const mask = self.capacity - 1;
        var pos = start_pos;

        while (true) {
            const record = &self.records[pos];
            if (!record.isValid()) return null;
            if (record.key == key) return record;

            const next = (pos + 1) & mask;
            if (next == start_pos) return null; // whole table examined

            @prefetch(&self.records[next], .{ .rw = .read, .locality = 3 });
            @prefetch(&self.records[(next + 1) & mask], .{ .rw = .read, .locality = 3 });
            pos = next;
        }
    }

    /// How many cache lines a lookup for `key` must read. Returns null when the
    /// key is absent.
    ///
    /// This is a diagnostic, not part of the hot path: it re-derives what
    /// `read` already knows, and it does not prefetch. It exists because the
    /// table has no load-factor limit — `findEmptySlot` only refuses at 100%
    /// full — so fill fraction, not hashing, decides how long a chain gets.
    /// Prefetch hides that latency; only a load-factor policy removes it.
    pub fn probeLength(self: *const Self, key: u64) ?u32 {
        self.lockSync();
        defer self.unlockSync();

        if (self.count == 0) return null;

        const mask = self.capacity - 1;
        const start_pos = @as(u32, @intCast(hashKey(key) & (self.capacity - 1)));
        var pos = start_pos;
        var lines: u32 = 1;

        while (true) {
            const record = &self.records[pos];
            if (!record.isValid()) return null;
            if (record.key == key) return lines;

            pos = (pos + 1) & mask;
            if (pos == start_pos) return null;
            lines += 1;
        }
    }

    /// Read many keys with software prefetch, in fixed microbatches.
    ///
    /// A lookup is a dependent chain: the hash gives an address, and the line
    /// at that address decides whether the next line is needed. The hardware
    /// prefetcher cannot predict that jump, so a table larger than L3 spends
    /// its time stalled. Measured here at 512 MB: 32.6 ns per batched read and
    /// IPC 0.27, where IPC above 2 means the cores are doing work.
    ///
    /// Two things recover that, and both need several keys in flight at once:
    ///
    ///   - the home line of every key in the microbatch is requested before
    ///     any chain is walked;
    ///   - the *next* microbatch is requested before the current one is
    ///     resolved, so the requests have lead time instead of being issued
    ///     immediately before first use.
    ///
    /// `micro` is comptime so both position arrays live on the stack. That is
    /// not a style choice: the database allocator refuses to allocate once
    /// startup finishes, so a batch reader that allocated would crash the
    /// process rather than merely run slowly.
    ///
    /// Returns how many keys were found. Each result is a pointer into the
    /// shared array, exactly like `read`, so a batch is a snapshot per key and
    /// not one consistent view of the table.
    pub fn readBatchPipelined(
        self: *const Self,
        keys: []const u64,
        results: []?*const CacheLineRecord,
        comptime micro: usize,
    ) usize {
        comptime {
            if (micro == 0 or micro > 1024) @compileError("micro must be between 1 and 1024");
        }
        std.debug.assert(results.len >= keys.len);
        self.lockSync();
        defer self.unlockSync();
        if (keys.len == 0) return 0;

        if (self.count == 0) {
            @memset(results[0..keys.len], null);
            return 0;
        }

        const mask: u64 = self.capacity - 1;
        var cur: [micro]u32 = undefined;
        var nxt: [micro]u32 = undefined;
        var found: usize = 0;

        // Home slots for the first group, with their lines on the way in.
        var head: usize = 0;
        var cur_len: usize = @min(micro, keys.len);
        for (keys[0..cur_len], 0..) |key, j| {
            cur[j] = @as(u32, @intCast(hashKey(key) & mask));
            @prefetch(&self.records[cur[j]], .{ .rw = .read, .locality = 3 });
        }

        var done: usize = cur_len;
        while (done < keys.len) {
            // Issue the next group before resolving this one.
            const next_len = @min(micro, keys.len - done);
            for (keys[done..][0..next_len], 0..) |key, j| {
                nxt[j] = @as(u32, @intCast(hashKey(key) & mask));
                @prefetch(&self.records[nxt[j]], .{ .rw = .read, .locality = 3 });
            }

            for (keys[head..][0..cur_len], 0..) |key, j| {
                const record = self.probeFrom(key, cur[j]);
                results[head + j] = record;
                if (record != null) found += 1;
            }

            head = done;
            cur = nxt;
            cur_len = next_len;
            done += next_len;
        }

        // Drain the last group, whose lines went out in the previous pass.
        for (keys[head..][0..cur_len], 0..) |key, j| {
            const record = self.probeFrom(key, cur[j]);
            results[head + j] = record;
            if (record != null) found += 1;
        }

        return found;
    }

    pub inline fn write(self: *Self, key: u64, data: []const u8) !void {
        self.lockSync();
        defer self.unlockSync();
        return self.writeLocked(key, data, true);
    }

    /// Store a value. `log` is false when the entry came from the log itself:
    /// replay and replica apply must not write the entry back.
    fn writeLocked(self: *Self, key: u64, data: []const u8, log: bool) !void {
        const pos = self.findEmptySlot(key) orelse return error.TableFull;

        const old_record = &self.records[pos];
        const is_update = old_record.isValid() and old_record.key == key;

        // Critical path: update cache line (maintains 42ns performance)
        self.records[pos] = try CacheLineRecord.init(key, data);

        // PERFORMANCE DESIGN CHOICE: Always-async durability never blocks write path
        // This prioritizes consistent 42ns write latency over guaranteed durability
        if (log) {
            if (self.durability_manager) |dur_mgr| {
                _ = dur_mgr.appendAsync(&self.records[pos]);
                // INTENTIONAL: Ignore return value to maintain lock-free performance
                // Trade-off: WAL ring full = dropped entry, but write still succeeds
                // Rationale: Better to lose some durability than block all writes
                // Alternative: Use blocking WAL for critical data, async for cache-like workloads
            }
        }

        // Update count only for new insertions
        if (!is_update) {
            self.count += 1;
        }
    }

    /// Remove a key, closing the hole it leaves so that later keys in the same
    /// probe run stay reachable.
    ///
    /// Linear probing puts a colliding key in the next free slot, and a lookup
    /// stops at the first empty slot it meets. Clearing one slot in the middle
    /// of a run therefore hides every key behind it: the value bytes stay in
    /// the table, but no probe can reach them. So the records after the hole
    /// are shifted back, one slot at a time, until the run ends.
    ///
    /// This keeps the only two states the table has, occupied or empty, which
    /// is what `copyAllRecords`, the snapshot and replication walkers, and the
    /// WAL entry format already read. A tombstone state would add a third
    /// state to all of those, and nothing here grows or rehashes the table, so
    /// tombstones would never be reclaimed.
    ///
    /// Cost: a delete walks its own run, so it costs O(run length) instead of
    /// O(1). Reads are untouched, and an in-run read still costs one cache
    /// line.
    ///
    /// Returns false when the key is not present.
    pub fn delete(self: *Self, key: u64) bool {
        self.lockSync();
        defer self.unlockSync();
        return self.deleteLocked(key, true);
    }

    /// Remove a key. `log` is false when the removal came from the log itself.
    fn deleteLocked(self: *Self, key: u64, log: bool) bool {
        const pos = self.findRecord(key) orelse return false;
        self.closeRun(pos);
        self.count -= 1;

        // The removal has to reach the log. Without this entry the next replay
        // re-applies the write that put the key there, and the key comes back.
        // Same async policy as `write`: a full ring drops the entry and the
        // delete still stands in memory.
        if (log) {
            if (self.durability_manager) |dur_mgr| {
                const marker = CacheLineRecord.initDelete(key);
                _ = dur_mgr.appendAsync(&marker);
            }
        }

        return true;
    }

    /// Apply one WAL entry: a write stores the value, a marker removes the key.
    ///
    /// This is the only place WAL entry metadata is interpreted. Replay,
    /// snapshot restore and replica apply all route here, so the three cannot
    /// drift apart over what a marker means.
    ///
    /// Removing a key that is not present is not an error. A replica that
    /// bootstrapped from a snapshot legitimately sees deletes for keys that
    /// snapshot already settled, and replay sees a delete for a key that was
    /// never in the table.
    pub fn applyEntry(self: *Self, entry: *const wal.WALEntry) !void {
        self.lockSync();
        defer self.unlockSync();
        return self.applyEntryLocked(entry);
    }

    fn applyEntryLocked(self: *Self, entry: *const wal.WALEntry) !void {
        if (entry.isDelete()) {
            _ = self.deleteLocked(entry.getKey(), false);
            return;
        }
        try self.writeLocked(entry.getKey(), entry.getData(), false);
    }

    /// Apply log entries from outside the owning thread, under one lock.
    ///
    /// A replica drains a batch on its receive thread, so the whole batch is
    /// one critical section: an entry in the middle of a batch must not become
    /// visible to a client reader while the rest of the batch is still being
    /// applied. Invalid entries are the gaps in a partially written batch and
    /// are skipped, as replay skips them.
    ///
    /// Returns how many entries were applied.
    pub fn applyEntries(self: *Self, entries: []const wal.WALEntry) usize {
        self.lockSync();
        defer self.unlockSync();

        var applied: usize = 0;
        for (entries) |*entry| {
            if (!entry.isValid()) continue;
            self.applyEntryLocked(entry) catch continue;
            applied += 1;
        }
        return applied;
    }

    /// Apply a bootstrap snapshot from outside the owning thread, under one
    /// lock. The payload is a packed sequence of 64-byte records, the same
    /// format `createDatabaseSnapshot` sends. Invalid records and delete
    /// markers are not snapshot contents, so they are skipped.
    ///
    /// Returns how many records were applied.
    pub fn applySnapshotBytes(self: *Self, bytes: []const u8) usize {
        self.lockSync();
        defer self.unlockSync();

        var applied: usize = 0;
        var offset: usize = 0;
        while (offset + @sizeOf(CacheLineRecord) <= bytes.len) {
            const record = std.mem.bytesToValue(CacheLineRecord, bytes[offset .. offset + @sizeOf(CacheLineRecord)]);
            offset += @sizeOf(CacheLineRecord);

            if (!record.isValid() or record.isDelete()) continue;
            self.writeLocked(record.getKey(), record.getData(), false) catch continue;
            applied += 1;
        }
        return applied;
    }

    /// Shift the records after `hole` back by one slot, then clear the last one.
    ///
    /// A record moves only when its home slot sits at or before the hole in
    /// probe order, which is what makes the record's placement depend on that
    /// slot being occupied. The first record whose probe started at or after
    /// the hole stays where it is, and the scan continues past it: a later
    /// record in the same run can still depend on the hole.
    ///
    /// The scan is bounded by `capacity` rather than by the first empty slot,
    /// because a table that reached 100 percent has no empty slot to stop at
    /// until this function clears one.
    fn closeRun(self: *Self, hole: u32) void {
        const mask = self.capacity - 1;
        var hole_pos = hole;
        var probe = (hole + 1) & mask;
        var steps: u32 = 0;

        while (self.records[probe].isValid() and steps < self.capacity) : (steps += 1) {
            const home = @as(u32, @intCast(hashKey(self.records[probe].key) & mask));

            if (Self.dependsOnHole(hole_pos, probe, home)) {
                self.records[hole_pos] = self.records[probe];
                hole_pos = probe;
            }
            probe = (probe + 1) & mask;
        }

        self.records[hole_pos] = CacheLineRecord.initEmpty();
    }

    /// Whether the record sitting at `probe` had to probe past `hole` to get
    /// there, given that its home slot is `home`.
    ///
    /// The record did not depend on `hole` exactly when `home` lies in the
    /// circular interval `(hole, probe]`: its probe path then started at or
    /// after the hole and never crossed it. Checked at the boundary that
    /// matters: `home == probe` means the record is already on its home slot
    /// and must not move, and `home == hole` means it belongs in the hole.
    inline fn dependsOnHole(hole: u32, probe: u32, home: u32) bool {
        if (hole <= probe) return !(home > hole and home <= probe);
        return !(home > hole or home <= probe); // the probe path wrapped
    }

    pub fn clear(self: *Self) void {
        self.lockSync();
        defer self.unlockSync();

        // Zero out all records with memset - much faster than looping
        @memset(std.mem.sliceAsBytes(self.records), 0);
        self.count = 0;
    }

    // Enable durability by associating with a WAL manager
    pub fn enableDurability(self: *Self, durability_manager: *wal.DurabilityManager) void {
        self.durability_manager = durability_manager;
    }

    // Disable durability
    pub fn disableDurability(self: *Self) void {
        self.durability_manager = null;
    }

    // Snapshot support: Copy all records atomically for snapshot creation
    pub fn copyAllRecords(self: *const Self, dest_records: []CacheLineRecord) void {
        std.debug.assert(dest_records.len >= self.records.len);

        self.lockSync();
        defer self.unlockSync();

        // Atomic copy of all records using memcpy - fastest approach
        // This gives us a point-in-time snapshot of the entire storage
        @memcpy(dest_records[0..self.records.len], self.records);
    }

    // Snapshot support: Restore a single record during recovery
    pub fn restoreRecord(self: *Self, record: *const CacheLineRecord) !void {
        if (!record.isValid()) return;

        // A snapshot is a copy of the live table, so a marker in one means the
        // snapshot is corrupt. Storing it would occupy a slot that no probe can
        // reach, because isValid reports true for a marker.
        if (record.isDelete()) return;

        self.lockSync();
        defer self.unlockSync();

        // Find the correct position for this record
        const key = record.getKey();
        const pos = self.findEmptySlot(key) orelse return error.TableFull;

        // Restoring a key the table already holds updates that slot, so the
        // count must not move. A replay that covers keys already present would
        // otherwise inflate the count and with it the load factor.
        const is_update = self.records[pos].isValid() and self.records[pos].key == key;

        // Restore the record
        self.records[pos] = record.*;
        if (!is_update) self.count += 1;
    }

    // Get record at specific slot index (for snapshot creation). This is a raw
    // accessor: it takes no lock, because a snapshot walks every slot and a
    // lock per slot would cost more than the copy. The caller either is the
    // owning thread or holds the storage guard across the whole walk.
    pub fn getRecordAtSlot(self: *const Self, slot_index: u32) ?*const CacheLineRecord {
        if (slot_index >= self.capacity) return null;
        return &self.records[slot_index];
    }

    pub fn getStats(self: *const Self) StorageStats {
        self.lockSync();
        defer self.unlockSync();

        return StorageStats{
            .total_count = self.count,
            .total_capacity = self.capacity,
            .load_factor = @as(f32, @floatFromInt(self.count)) / @as(f32, @floatFromInt(self.capacity)),
            .l3_domains = self.l3_domains,
        };
    }

    pub fn printDetailedStats(self: *const Self) void {
        const stats = self.getStats();

        std.debug.print("\n=== Storage Statistics ===\n", .{});
        std.debug.print("Capacity: {}/{} records ({d:.1}% full)\n", .{ stats.total_count, stats.total_capacity, stats.load_factor * 100 });
        std.debug.print("L3 domains: {}\n", .{stats.l3_domains});
    }
};

pub const StorageStats = struct {
    total_count: u32,
    total_capacity: u32,
    load_factor: f32,
    /// L3 domains the machine reports, from the detected topology.
    l3_domains: u32,
};
