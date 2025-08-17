# WILD Database - Write-Ahead Log Design

## Overview

This document outlines the design for adding a Write-Ahead Log (WAL) to WILD Database while preserving its ultra-high performance characteristics. The design prioritizes keeping durability completely off the critical path to maintain sub-microsecond read/write latencies.

## Design Principles

1. **Always Async**: Durability operations never block the hot path
2. **Zero-Copy**: WAL entries use identical format to cache lines  
3. **Cache-Line Batched**: Group operations by cache line boundaries for optimal I/O
4. **Lock-Free**: Use atomic operations and ring buffers to avoid contention
5. **Linux-Optimized**: Leverage io_uring for maximum I/O efficiency

## Architecture Components

### 1. WAL Entry Format

WAL entries maintain identical structure to `CacheLineRecord` for zero-copy operations:

```zig
const WALEntry = extern struct {
    metadata: u32,    // Same format as CacheLineRecord
    key: u64,         // 8 bytes
    data: [52]u8,     // 52 bytes payload
    // Total: 64 bytes = exactly 1 cache line
    
    comptime {
        std.debug.assert(@sizeOf(WALEntry) == 64);
        std.debug.assert(@alignOf(WALEntry) == 64);
        std.debug.assert(@sizeOf(WALEntry) == @sizeOf(CacheLineRecord));
    }
};
```

### 2. Batched WAL Format

Operations are batched into 2KB blocks for efficient disk I/O and network replication:

```zig
const WALBatch = extern struct {
    batch_id: u64,               // Monotonic sequence number
    view_number: u32,            // For viewstamped replication
    entry_count: u16,            // Number of cache lines in batch
    reserved: u16,               // Alignment padding
    entries: [MAX_BATCH_ENTRIES]WALEntry, // Up to 31 cache line entries
    
    const MAX_BATCH_ENTRIES = 31; // 16 + (64 * 31) = 2KB exactly
};
```

### 3. Thread-Local Ring Buffer

Each thread maintains its own lock-free ring buffer to avoid contention:

```zig
const ThreadLocalWAL = struct {
    buffer: []align(64) WALEntry,
    head: std.atomic.Value(u32),
    tail: std.atomic.Value(u32),
    capacity_mask: u32, // Power-of-2 capacity for fast modulo
    
    pub fn appendNonBlocking(self: *Self, entry: WALEntry) bool {
        const current_tail = self.tail.load(.acquire);
        const next_tail = (current_tail + 1) & self.capacity_mask;
        
        if (next_tail != self.head.load(.acquire)) {
            self.buffer[current_tail] = entry;
            self.tail.store(next_tail, .release);
            return true;
        }
        return false; // Ring buffer full
    }
    
    pub fn drainBatch(self: *Self, batch: *WALBatch) u16 {
        const current_head = self.head.load(.acquire);
        const current_tail = self.tail.load(.acquire);
        
        var count: u16 = 0;
        var head = current_head;
        
        while (head != current_tail and count < MAX_BATCH_ENTRIES) {
            batch.entries[count] = self.buffer[head];
            head = (head + 1) & self.capacity_mask;
            count += 1;
        }
        
        if (count > 0) {
            self.head.store(head, .release);
        }
        
        return count;
    }
};
```

### 4. Durability Manager

Central coordinator for all durability operations:

```zig
pub const DurabilityManager = struct {
    // Memory-mapped snapshot regions
    snapshot_mmap: []align(std.mem.page_size) u8,
    shadow_mmap: []align(std.mem.page_size) u8,
    
    // WAL file management
    wal_fd: std.os.fd_t,
    wal_offset: std.atomic.Value(u64),
    
    // io_uring for async I/O
    ring: std.os.linux.io_uring,
    
    // Pre-allocated aligned buffers for O_DIRECT
    wal_buffers: []align(512) WALBatch,
    buffer_pool: std.atomic.Queue([]align(512) WALBatch),
    
    // Thread-local WAL rings
    tls_wals: []ThreadLocalWAL,
    
    // Background thread for durability operations
    durability_thread: std.Thread,
    should_stop: std.atomic.Value(bool),
    
    // Statistics
    batches_written: std.atomic.Value(u64),
    entries_dropped: std.atomic.Value(u64),
    
    pub fn init(allocator: std.mem.Allocator, db_size: usize, num_threads: u32) !Self {
        // Create memory-mapped regions for snapshots
        const total_mmap_size = db_size * 2; // Double-buffered
        const snapshot_region = try std.posix.mmap(
            null, total_mmap_size,
            std.posix.PROT.READ | std.posix.PROT.WRITE,
            std.posix.MAP{ .TYPE = .SHARED, .ANONYMOUS = true },
            -1, 0
        );
        
        // Open WAL file with O_DIRECT for bypass page cache
        const wal_fd = try std.posix.openat(
            std.posix.AT.FDCWD,
            "wild.wal",
            .{ .ACCMODE = .RDWR, .CREAT = true, .DIRECT = true },
            0o644
        );
        
        // Initialize io_uring
        var ring = try std.os.linux.io_uring.init(256, 0);
        
        // Pre-allocate buffer pool
        const buffer_count = 64;
        var wal_buffers = try allocator.alignedAlloc(WALBatch, 512, buffer_count);
        
        return Self{
            .snapshot_mmap = snapshot_region[0..db_size],
            .shadow_mmap = snapshot_region[db_size..],
            .wal_fd = wal_fd,
            .wal_offset = std.atomic.Value(u64).init(0),
            .ring = ring,
            .wal_buffers = wal_buffers,
            // ... initialize other fields
        };
    }
    
    fn durabilityLoop(self: *Self) void {
        while (!self.should_stop.load(.acquire)) {
            // Collect batches from all thread-local rings
            var total_batches: u32 = 0;
            
            for (self.tls_wals) |*tls_wal| {
                var batch = self.getBatchBuffer() orelse continue;
                
                const entry_count = tls_wal.drainBatch(batch);
                if (entry_count > 0) {
                    batch.entry_count = entry_count;
                    batch.batch_id = self.batches_written.fetchAdd(1, .monotonic);
                    
                    try self.submitBatch(batch);
                    total_batches += 1;
                }
            }
            
            if (total_batches > 0) {
                // Wait for completions
                _ = try self.ring.submit_and_wait(total_batches);
                self.processBatchCompletions();
            } else {
                // Sleep briefly when no work
                std.time.sleep(1000); // 1μs
            }
        }
    }
    
    fn submitBatch(self: *Self, batch: *WALBatch) !void {
        // Queue WAL write with io_uring
        const write_sqe = try self.ring.get_sqe();
        const offset = self.wal_offset.fetchAdd(@sizeOf(WALBatch), .monotonic);
        
        std.os.linux.io_uring_prep_write(
            write_sqe,
            self.wal_fd,
            @as([*]const u8, @ptrCast(batch)),
            @sizeOf(WALBatch),
            offset
        );
        
        // Set user data to identify completion
        write_sqe.user_data = @intFromPtr(batch);
    }
    
    pub fn createSnapshot(self: *Self, db_records: []const CacheLineRecord) !void {
        // Copy current database state to shadow region
        const db_size = db_records.len * @sizeOf(CacheLineRecord);
        @memcpy(self.shadow_mmap[0..db_size], std.mem.sliceAsBytes(db_records));
        
        // Async flush to disk
        try std.posix.msync(self.shadow_mmap, std.posix.MSF.ASYNC);
        
        // Atomic swap: shadow becomes active snapshot
        std.mem.swap([]u8, &self.snapshot_mmap, &self.shadow_mmap);
    }
};
```

## Integration with WILD

### Hot Path Integration

The WAL integrates into the existing write path with minimal overhead:

```zig
// In src/flat_hash_storage.zig - extend existing write function
pub inline fn write(self: *Self, key: u64, data: []const u8) !void {
    const pos = self.findEmptySlot(key) orelse return error.TableFull;
    const old_record = &self.records[pos];
    const is_update = old_record.isValid() and old_record.key == key;
    
    // Critical path: update cache line (maintains 42ns performance)
    self.records[pos] = try CacheLineRecord.init(key, data);
    
    // Always-async durability: never blocks
    if (self.durability) |*dur| {
        const success = dur.appendAsync(self.records[pos]);
        if (!success) {
            // Ring buffer full - increment drop counter but continue
            dur.entries_dropped.fetchAdd(1, .relaxed);
        }
    }
    
    if (!is_update) self.count += 1;
}
```

### WILD Database Extension

```zig
// In src/wild.zig - extend the WILD struct
pub const WILD = struct {
    storage: flat_hash_storage.FlatHashStorage,
    cache_topology: *cache_topology.CacheTopology,
    allocator: std.mem.Allocator,
    target_capacity: u64,
    
    // Optional durability - zero impact when disabled
    durability: ?DurabilityManager,
    
    pub fn enableDurability(self: *Self, wal_path: []const u8) !void {
        const db_size = self.storage.capacity * @sizeOf(flat_hash_storage.CacheLineRecord);
        
        self.durability = try DurabilityManager.init(
            self.allocator,
            db_size,
            self.cache_topology.total_physical_cores
        );
        
        // Start background durability thread on dedicated core
        const last_core = self.cache_topology.total_physical_cores - 1;
        self.durability.?.start(last_core);
    }
    
    pub fn createCheckpoint(self: *Self) !void {
        if (self.durability) |*dur| {
            try dur.createSnapshot(self.storage.records);
        }
    }
    
    pub fn getDurabilityStats(self: *const Self) ?DurabilityStats {
        if (self.durability) |*dur| {
            return DurabilityStats{
                .batches_written = dur.batches_written.load(.monotonic),
                .entries_dropped = dur.entries_dropped.load(.monotonic),
                .wal_size_bytes = dur.wal_offset.load(.monotonic),
            };
        }
        return null;
    }
};

pub const DurabilityStats = struct {
    batches_written: u64,
    entries_dropped: u64,
    wal_size_bytes: u64,
};
```

## Recovery Process

Recovery leverages memory-mapped snapshots for instant startup:

```zig
pub fn recover(allocator: std.mem.Allocator, snapshot_path: []const u8, wal_path: []const u8) !WILD {
    // 1. Memory-map the snapshot directly
    const snapshot_file = try std.fs.cwd().openFile(snapshot_path, .{});
    defer snapshot_file.close();
    
    const file_size = try snapshot_file.getEndPos();
    const snapshot_data = try std.posix.mmap(
        null, file_size,
        std.posix.PROT.READ | std.posix.PROT.WRITE,
        std.posix.MAP{ .TYPE = .SHARED },
        snapshot_file.handle, 0
    );
    
    // 2. Cast directly to CacheLineRecord array - zero-copy!
    const capacity = file_size / @sizeOf(CacheLineRecord);
    const records = @as([*]CacheLineRecord, @ptrCast(@alignCast(snapshot_data.ptr)))[0..capacity];
    
    // 3. Create WILD instance with recovered data
    var wild = WILD{
        .storage = FlatHashStorage{
            .records = records,
            .capacity = @intCast(capacity),
            .count = 0, // Will be recalculated
            // ... other fields
        },
        // ... other WILD fields
    };
    
    // 4. Replay WAL tail
    try replayWAL(&wild, wal_path);
    
    // 5. Recalculate record count
    wild.storage.recalculateCount();
    
    return wild;
}

fn replayWAL(wild: *WILD, wal_path: []const u8) !void {
    const wal_file = try std.fs.cwd().openFile(wal_path, .{});
    defer wal_file.close();
    
    var batch_buffer: WALBatch = undefined;
    var offset: u64 = 0;
    
    while (true) {
        const bytes_read = wal_file.preadAll(
            std.mem.asBytes(&batch_buffer), 
            offset
        ) catch break;
        
        if (bytes_read < @sizeOf(WALBatch)) break;
        
        // Apply each entry in the batch
        for (batch_buffer.entries[0..batch_buffer.entry_count]) |entry| {
            if (entry.isValid()) {
                try wild.storage.write(entry.getKey(), entry.getData());
            }
        }
        
        offset += @sizeOf(WALBatch);
    }
}
```

## Future: Viewstamped Replication Integration

The WAL design naturally extends to support viewstamped replication:

### Replication Message Format

```zig
const ReplicationMessage = extern struct {
    view_number: u32,
    op_number: u64,           // Global operation sequence
    batch: WALBatch,          // 2KB batch for efficient network transfer
    
    // Total size: ~2KB - efficient for UDP/TCP transmission
};
```

### Network Integration

```zig
// Extend DurabilityManager for replication
fn submitBatch(self: *Self, batch: *WALBatch) !void {
    // Local WAL write
    const write_sqe = try self.ring.get_sqe();
    std.os.linux.io_uring_prep_write(write_sqe, self.wal_fd, ...);
    
    // Network replication to all replicas (parallel)
    for (self.replica_sockets) |socket| {
        const send_sqe = try self.ring.get_sqe();
        const repl_msg = ReplicationMessage{
            .view_number = self.current_view.load(.acquire),
            .op_number = self.op_counter.fetchAdd(1, .monotonic),
            .batch = batch.*,
        };
        
        std.os.linux.io_uring_prep_send(
            send_sqe, socket, 
            @ptrCast(&repl_msg), @sizeOf(ReplicationMessage), 0
        );
    }
    
    // Submit all I/O operations in single syscall
    _ = try self.ring.submit();
}
```

## Performance Characteristics

### Expected Overhead

- **Hot path impact**: <5ns additional latency per write
- **Memory overhead**: ~1MB per thread for ring buffers
- **Disk I/O**: Batched 2KB writes via io_uring
- **Network replication**: 2KB UDP packets, <100μs latency

### Monitoring

```zig
pub const DurabilityMetrics = struct {
    avg_batch_size: f32,
    batches_per_second: u64,
    ring_buffer_utilization: f32,
    drop_rate: f32,
    wal_write_latency_ns: u64,
    snapshot_frequency: u64,
};
```

## Implementation Plan

### Phase 1: Basic WAL
1. Implement `ThreadLocalWAL` ring buffers
2. Add durability thread with batch collection
3. Basic file-based WAL with O_DIRECT
4. Integration with existing `FlatHashStorage.write()`

### Phase 2: io_uring Optimization
1. Replace basic file I/O with io_uring
2. Add pre-allocated buffer pool
3. Implement async batch submission
4. Performance testing and tuning

### Phase 3: Memory-Mapped Snapshots
1. Implement snapshot creation via mmap
2. Add recovery from snapshots
3. Periodic checkpoint scheduling
4. WAL truncation after snapshots

### Phase 4: Replication Foundation
1. Network message format design
2. Basic replica communication
3. View change protocol
4. Primary election logic

This design maintains WILD's incredible performance while adding enterprise-grade durability. The always-async approach ensures that durability never impacts the critical path, preserving the sub-microsecond latencies that make WILD unique.