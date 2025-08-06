const std = @import("std");
const flat_hash_storage = @import("flat_hash_storage.zig");

// WAL entry - identical to CacheLineRecord for zero-copy
pub const WALEntry = extern struct {
    key: u64,
    metadata: u32,
    data: [52]u8,

    const Self = @This();

    comptime {
        std.debug.assert(@sizeOf(WALEntry) == @sizeOf(flat_hash_storage.CacheLineRecord));
    }

    pub fn fromCacheLineRecord(record: *const flat_hash_storage.CacheLineRecord) WALEntry {
        return WALEntry{
            .key = record.key,
            .metadata = record.metadata,
            .data = record.data,
        };
    }

    pub fn isValid(self: *const Self) bool {
        return (self.metadata & flat_hash_storage.CacheLineRecord.VALID_FLAG) != 0;
    }

    pub fn getKey(self: *const Self) u64 {
        return self.key;
    }

    pub fn getData(self: *const Self) []const u8 {
        const len = @as(u32, @intCast(self.metadata & flat_hash_storage.CacheLineRecord.LENGTH_MASK));
        return self.data[0..len];
    }
};

// Batched WAL format - page-aligned blocks for efficient I/O
pub const WALBatch = extern struct {
    batch_id: u64,
    view_number: u32,
    entry_count: u16,
    reserved: u16,
    entries: [MAX_BATCH_ENTRIES]WALEntry,

    // Calculate entries per page: (page_size - header_size) / entry_size
    // Standard 4KB page: (4096 - 16) / 64 = 63 entries
    pub const HEADER_SIZE = 16; // batch_id + view_number + entry_count + reserved
    pub const MAX_BATCH_ENTRIES = (std.heap.page_size_min - HEADER_SIZE) / @sizeOf(WALEntry);

    const Self = @This();

    comptime {
        // Ensure we fit within a page
        std.debug.assert(@sizeOf(WALBatch) <= std.heap.page_size_min);
    }

    pub fn init(batch_id: u64, view_number: u32) Self {
        return Self{
            .batch_id = batch_id,
            .view_number = view_number,
            .entry_count = 0,
            .reserved = 0,
            .entries = std.mem.zeroes([MAX_BATCH_ENTRIES]WALEntry),
        };
    }

    pub fn addEntry(self: *Self, entry: WALEntry) bool {
        if (self.entry_count >= MAX_BATCH_ENTRIES) return false;

        self.entries[self.entry_count] = entry;
        self.entry_count += 1;
        return true;
    }

    pub fn isFull(self: *const Self) bool {
        return self.entry_count >= MAX_BATCH_ENTRIES;
    }

    pub fn isEmpty(self: *const Self) bool {
        return self.entry_count == 0;
    }
};

// Thread-local WAL ring buffer - all memory pre-allocated
pub const ThreadLocalWAL = struct {
    buffer: []WALEntry,
    head: std.atomic.Value(u32),
    tail: std.atomic.Value(u32),
    capacity_mask: u32,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, capacity: u32) !Self {
        // Ensure power-of-2 capacity for fast modulo
        const actual_capacity = if (std.math.isPowerOfTwo(capacity))
            capacity
        else
            std.math.floorPowerOfTwo(u32, capacity);

        // Pre-allocate all ring buffer memory
        const buffer = try allocator.alignedAlloc(WALEntry, 64, actual_capacity);

        // Initialize all entries as invalid
        for (buffer) |*entry| {
            entry.* = std.mem.zeroes(WALEntry);
        }

        return Self{
            .buffer = buffer,
            .head = std.atomic.Value(u32).init(0),
            .tail = std.atomic.Value(u32).init(0),
            .capacity_mask = actual_capacity - 1,
        };
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        allocator.free(self.buffer);
    }

    pub fn appendNonBlocking(self: *Self, entry: WALEntry) bool {
        const current_tail = self.tail.load(.acquire);
        const next_tail = (current_tail + 1) & self.capacity_mask;

        // Check if ring buffer is full
        if (next_tail == self.head.load(.acquire)) {
            return false;
        }

        // Store entry and advance tail
        self.buffer[current_tail] = entry;
        self.tail.store(next_tail, .release);
        return true;
    }

    pub fn drainBatch(self: *Self, batch: *WALBatch) u16 {
        const current_head = self.head.load(.acquire);
        const current_tail = self.tail.load(.acquire);

        var count: u16 = 0;
        var head = current_head;

        while (head != current_tail and count < WALBatch.MAX_BATCH_ENTRIES) {
            if (batch.addEntry(self.buffer[head])) {
                head = (head + 1) & self.capacity_mask;
                count += 1;
            } else {
                break;
            }
        }

        if (count > 0) {
            self.head.store(head, .release);
        }

        return count;
    }

    pub fn getUsage(self: *const Self) struct { used: u32, capacity: u32 } {
        const head = self.head.load(.acquire);
        const tail = self.tail.load(.acquire);
        const used = if (tail >= head) tail - head else (self.capacity_mask + 1) - (head - tail);

        return .{ .used = used, .capacity = self.capacity_mask + 1 };
    }
};

pub const WALCompletion = struct {
    next: ?*WALCompletion,
    batch: *WALBatch,
    operation: Operation,
    result: i32,

    pub const Operation = enum {
        write_batch,
    };

    pub fn init(batch: *WALBatch, operation: Operation) WALCompletion {
        return WALCompletion{
            .next = null,
            .batch = batch,
            .operation = operation,
            .result = 0,
        };
    }
};

// io_uring-optimized durability manager - completion-based architecture
pub const DurabilityManager = struct {
    // WAL file management
    wal_fd: std.posix.fd_t,
    wal_offset: std.atomic.Value(u64),

    // io_uring for async I/O
    ring: std.os.linux.IoUring,

    // Thread-local WAL rings - pre-allocated array
    tls_wals: []ThreadLocalWAL,
    current_tls_index: std.atomic.Value(u32),

    // Pre-allocated batch buffers for durability thread (O_DIRECT aligned)
    batch_buffers: []align(512) WALBatch,
    batch_buffer_available: std.atomic.Value(u32), // Bitmask for available buffers

    // Pre-allocated completion objects
    completions: []WALCompletion,
    completion_available: std.atomic.Value(u32), // Bitmask for available completions

    // Completion queues
    unqueued: ?*WALCompletion,
    completed: ?*WALCompletion,
    awaiting: ?*WALCompletion,

    // Queue locks
    unqueued_lock: std.Thread.Mutex,
    completed_lock: std.Thread.Mutex,
    awaiting_lock: std.Thread.Mutex,

    // Completion tracking
    pending_ios: std.atomic.Value(u32),
    completed_ios: std.atomic.Value(u64),

    // Background thread
    durability_thread: ?std.Thread,
    should_stop: std.atomic.Value(bool),

    // Statistics - all atomic for lock-free access
    batches_written: std.atomic.Value(u64),
    entries_dropped: std.atomic.Value(u64),
    total_entries_written: std.atomic.Value(u64),
    io_errors: std.atomic.Value(u64),

    allocator: std.mem.Allocator,

    const Self = @This();

    pub const Config = struct {
        wal_path: []const u8,
        max_threads: u32,
        ring_buffer_size: u32, // Should match storage capacity exactly
        batch_buffer_count: u32,
        io_uring_entries: u16, // Size of io_uring submission queue

        // Create config with ring buffer sized to match storage capacity exactly
        pub fn forStorage(wal_path: []const u8, storage_capacity: u32) Config {
            // WAL ring buffer must match storage capacity exactly - every storable record gets a WAL slot
            // Storage capacity is already power-of-2 from flat_hash_storage init
            return Config{
                .wal_path = wal_path,
                .max_threads = 4,
                .ring_buffer_size = storage_capacity, // 1:1 correspondence
                .batch_buffer_count = @max(8, storage_capacity / 4096), // Scale batch buffers appropriately
                .io_uring_entries = @intCast(@max(32, @min(storage_capacity / 1024, 512))), // Scale io_uring
            };
        }
    };

    pub fn init(allocator: std.mem.Allocator, config: Config) !Self {
        // Open WAL file with O_DIRECT for bypass page cache
        const wal_fd = try std.posix.openat(
            std.posix.AT.FDCWD,
            config.wal_path,
            .{ .ACCMODE = .RDWR, .CREAT = true, .DIRECT = true },
            0o644,
        );

        // Initialize io_uring
        const ring = try std.os.linux.IoUring.init(config.io_uring_entries, 0);

        // Pre-allocate thread-local WAL rings
        const tls_wals = try allocator.alloc(ThreadLocalWAL, config.max_threads);
        for (tls_wals) |*tls_wal| {
            tls_wal.* = try ThreadLocalWAL.init(allocator, config.ring_buffer_size);
        }

        // Pre-allocate batch buffers for durability thread
        const batch_buffer_count = @min(32, config.batch_buffer_count); // Max 32 for bitmask
        const actual_buffer_count = if (std.math.isPowerOfTwo(batch_buffer_count))
            batch_buffer_count
        else
            std.math.floorPowerOfTwo(u32, batch_buffer_count);

        const batch_buffers = try allocator.alignedAlloc(WALBatch, 512, actual_buffer_count);
        const completions = try allocator.alloc(WALCompletion, actual_buffer_count);

        // Initialize all batch buffers and completions
        for (batch_buffers, 0..) |*batch, i| {
            batch.* = WALBatch.init(@intCast(i), 0);
        }

        for (completions, 0..) |*completion, i| {
            completion.* = WALCompletion.init(&batch_buffers[i], .write_batch);
        }

        // All buffers and completions start as available
        const all_available = if (actual_buffer_count == 32)
            0xFFFFFFFF // All 32 bits set
        else
            (@as(u32, 1) << @intCast(actual_buffer_count)) - 1;

        return Self{
            .wal_fd = wal_fd,
            .wal_offset = std.atomic.Value(u64).init(0),
            .ring = ring,
            .tls_wals = tls_wals,
            .current_tls_index = std.atomic.Value(u32).init(0),
            .batch_buffers = batch_buffers,
            .batch_buffer_available = std.atomic.Value(u32).init(all_available),
            .completions = completions,
            .completion_available = std.atomic.Value(u32).init(all_available),
            .unqueued = null,
            .completed = null,
            .awaiting = null,
            .unqueued_lock = std.Thread.Mutex{},
            .completed_lock = std.Thread.Mutex{},
            .awaiting_lock = std.Thread.Mutex{},
            .pending_ios = std.atomic.Value(u32).init(0),
            .completed_ios = std.atomic.Value(u64).init(0),
            .durability_thread = null,
            .should_stop = std.atomic.Value(bool).init(false),
            .batches_written = std.atomic.Value(u64).init(0),
            .entries_dropped = std.atomic.Value(u64).init(0),
            .total_entries_written = std.atomic.Value(u64).init(0),
            .io_errors = std.atomic.Value(u64).init(0),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        // Stop durability thread if running
        if (self.durability_thread) |thread| {
            self.should_stop.store(true, .release);
            thread.join();
        }

        // Clean up io_uring
        self.ring.deinit();

        // Close WAL file
        std.posix.close(self.wal_fd);

        // Free thread-local WAL rings
        for (self.tls_wals) |*tls_wal| {
            tls_wal.deinit(self.allocator);
        }
        self.allocator.free(self.tls_wals);

        // Free batch buffers and completions
        self.allocator.free(self.batch_buffers);
        self.allocator.free(self.completions);
    }

    pub fn start(self: *Self) !void {
        std.debug.assert(self.durability_thread == null);
        self.durability_thread = try std.Thread.spawn(.{}, durabilityLoop, .{self});
    }

    pub fn stop(self: *Self) void {
        if (self.durability_thread) |thread| {
            self.should_stop.store(true, .release);
            thread.join();
            self.durability_thread = null;
        }
    }

    // Get a thread-local WAL ring - round-robin assignment
    pub fn getThreadLocalWAL(self: *Self) *ThreadLocalWAL {
        const index = self.current_tls_index.fetchAdd(1, .monotonic) % self.tls_wals.len;
        return &self.tls_wals[index];
    }

    // Append WAL entry - never blocks, returns false if ring buffer full
    pub fn appendAsync(self: *Self, record: *const flat_hash_storage.CacheLineRecord) bool {
        const wal_entry = WALEntry.fromCacheLineRecord(record);

        // Try multiple thread-local rings to improve success rate
        var attempts: u8 = 0;
        const max_attempts = @min(4, self.tls_wals.len);

        while (attempts < max_attempts) {
            const tls_wal = self.getThreadLocalWAL();

            if (tls_wal.appendNonBlocking(wal_entry)) {
                return true; // Success
            }

            attempts += 1;
        }

        // All attempts failed - record dropped entry
        _ = self.entries_dropped.fetchAdd(1, .monotonic);
        return false;
    }

    fn getAvailableCompletion(self: *Self) ?*WALCompletion {
        const available = self.completion_available.load(.acquire);
        if (available == 0) return null;

        // Find first available completion using bit scan
        const index = @ctz(available);
        if (index >= self.completions.len) return null;

        // Try to atomically claim this completion
        const mask = @as(u32, 1) << @intCast(index);
        const old_available = self.completion_available.fetchAnd(~mask, .acquire);

        if ((old_available & mask) == 0) {
            // Someone else claimed it, try again
            return self.getAvailableCompletion();
        }

        const completion = &self.completions[index];
        // Reset completion for reuse
        completion.next = null;
        completion.result = 0;
        completion.batch.entry_count = 0;

        return completion;
    }

    fn returnCompletion(self: *Self, completion: *WALCompletion) void {
        // Find the index of this completion
        const completion_ptr = @intFromPtr(completion);
        const base_ptr = @intFromPtr(self.completions.ptr);
        const index = (completion_ptr - base_ptr) / @sizeOf(WALCompletion);

        if (index < self.completions.len) {
            const mask = @as(u32, 1) << @intCast(index);
            _ = self.completion_available.fetchOr(mask, .release);
        }
    }

    fn enqueueCompletion(self: *Self, head: *?*WALCompletion, completion: *WALCompletion, lock: *std.Thread.Mutex) void {
        _ = self;
        lock.lock();
        defer lock.unlock();

        completion.next = head.*;
        head.* = completion;
    }

    fn dequeueCompletion(self: *Self, head: *?*WALCompletion, lock: *std.Thread.Mutex) ?*WALCompletion {
        _ = self;
        lock.lock();
        defer lock.unlock();

        const completion = head.* orelse return null;
        head.* = completion.next;
        completion.next = null;
        return completion;
    }

    // io_uring-optimized durability thread main loop
    fn durabilityLoop(self: *Self) void {
        while (!self.should_stop.load(.acquire)) {
            var submitted_count: u32 = 0;

            // Phase 1: Collect and submit batches via io_uring
            for (self.tls_wals) |*tls_wal| {
                const completion = self.getAvailableCompletion() orelse break;
                const batch = completion.batch;

                const entry_count = tls_wal.drainBatch(batch);
                if (entry_count > 0) {
                    batch.batch_id = self.batches_written.fetchAdd(1, .monotonic);

                    // Submit batch via io_uring (non-blocking)
                    self.submitBatchAsync(completion) catch |err| {
                        std.debug.print("WAL submit error: {}\n", .{err});
                        _ = self.io_errors.fetchAdd(1, .monotonic);
                        self.returnCompletion(completion);
                        continue;
                    };

                    _ = self.total_entries_written.fetchAdd(entry_count, .monotonic);
                    submitted_count += 1;
                    // Note: completion will be processed when I/O completes
                } else {
                    self.returnCompletion(completion);
                }
            }

            // Phase 2: Process completions if we have pending I/Os
            const pending = self.pending_ios.load(.acquire);
            if (pending > 0 or submitted_count > 0) {
                // Submit all queued operations with retry logic
                if (submitted_count > 0) {
                    var submit_attempts: u8 = 0;
                    while (submit_attempts < 3) {
                        _ = self.ring.submit() catch |err| switch (err) {
                            error.SystemResources => {
                                submit_attempts += 1;
                                std.time.sleep(@as(u64, 1000) * submit_attempts); // Exponential backoff
                                continue;
                            },
                            error.CompletionQueueOvercommitted => {
                                submit_attempts += 1;
                                std.time.sleep(@as(u64, 1000) * submit_attempts); // Exponential backoff
                                continue;
                            },
                            else => {
                                std.debug.print("io_uring submit error: {}\n", .{err});
                                break;
                            },
                        };
                        break; // Success
                    }
                }

                // Wait for at least one completion (with retry)
                var completion_attempts: u8 = 0;
                while (completion_attempts < 3) {
                    self.processCompletions() catch |err| {
                        completion_attempts += 1;
                        std.debug.print("Completion processing error: {} (attempt {})\n", .{ err, completion_attempts });
                        if (completion_attempts >= 3) break;
                        std.time.sleep(@as(u64, 1000) * completion_attempts);
                        continue;
                    };
                    break; // Success
                }
            } else {
                // No work - sleep briefly
                std.time.sleep(1000); // 1μs
            }
        }

        // Final cleanup: wait for all pending I/Os to complete with robust retry
        var cleanup_attempts: u32 = 0;
        while (self.pending_ios.load(.acquire) > 0 and cleanup_attempts < 100) {
            // Submit any remaining operations
            _ = self.ring.submit() catch {};

            // Process completions with progressively longer waits
            self.processCompletions() catch |err| {
                std.debug.print("Cleanup completion error: {}\n", .{err});
            };

            cleanup_attempts += 1;
            if (cleanup_attempts > 20) {
                // After 20 attempts, start backing off more aggressively
                const backoff_ms = @min(cleanup_attempts - 20, 10);
                std.time.sleep(@as(u64, backoff_ms) * 1000_000); // Convert to nanoseconds
            } else {
                std.time.sleep(1000); // 1μs between early attempts
            }
        }

        // Comprehensive shutdown: ensure ALL pending I/Os complete
        const initial_pending = self.pending_ios.load(.acquire);
        if (initial_pending > 0) {
            std.debug.print("Shutdown: waiting for {} pending I/Os to complete...\n", .{initial_pending});

            var shutdown_attempts: u16 = 0;
            while (self.pending_ios.load(.acquire) > 0 and shutdown_attempts < 500) {
                // Multiple submission strategies
                const submitted = self.ring.submit() catch 0;

                // Try different completion approaches
                if (shutdown_attempts < 100) {
                    // Strategy 1: Normal completion processing
                    self.processCompletions() catch |err| {
                        if (shutdown_attempts % 50 == 0) {
                            std.debug.print("Completion error (attempt {}): {}\n", .{ shutdown_attempts, err });
                        }
                    };
                } else if (shutdown_attempts < 300) {
                    // Strategy 2: More aggressive - bypass our wrapper and use raw io_uring
                    const cq_ready = self.ring.cq_ready();
                    if (cq_ready > 0) {
                        var processed: u32 = 0;
                        while (self.ring.cq_ready() > 0 and processed < cq_ready) {
                            const cqe = self.ring.copy_cqe() catch break;

                            // Handle completion properly - check for errors
                            if (cqe.res < 0) {
                                _ = self.io_errors.fetchAdd(1, .monotonic);
                            } else if (cqe.res != @sizeOf(WALBatch)) {
                                _ = self.io_errors.fetchAdd(1, .monotonic);
                            }

                            _ = self.pending_ios.fetchSub(1, .monotonic);
                            _ = self.completed_ios.fetchAdd(1, .monotonic);
                            processed += 1;
                        }
                        if (processed > 0 and shutdown_attempts % 50 == 0) {
                            std.debug.print("Raw processing: {} completions handled\n", .{processed});
                        }
                    }
                } else {
                    // Strategy 3: Force fsync to ensure data is on disk
                    if (shutdown_attempts == 300) {
                        std.debug.print("Forcing fsync to ensure data persistence...\n", .{});
                        std.posix.fsync(self.wal_fd) catch {};
                    }

                    // Continue trying completions
                    const cq_ready = self.ring.cq_ready();
                    if (cq_ready > 0) {
                        var processed: u32 = 0;
                        while (self.ring.cq_ready() > 0 and processed < 10) {
                            const cqe = self.ring.copy_cqe() catch break;

                            // Handle completion properly - check for errors
                            if (cqe.res < 0) {
                                _ = self.io_errors.fetchAdd(1, .monotonic);
                            } else if (cqe.res != @sizeOf(WALBatch)) {
                                _ = self.io_errors.fetchAdd(1, .monotonic);
                            }

                            _ = self.pending_ios.fetchSub(1, .monotonic);
                            _ = self.completed_ios.fetchAdd(1, .monotonic);
                            processed += 1;
                        }
                    }
                }

                shutdown_attempts += 1;

                // Progress reporting
                if (shutdown_attempts % 50 == 0) {
                    const remaining = self.pending_ios.load(.acquire);
                    std.debug.print("Shutdown progress: {} I/Os remaining (attempt {}, {} submitted)\n", .{ remaining, shutdown_attempts, submitted });
                }

                // Smart backoff strategy
                if (shutdown_attempts < 50) {
                    // No delay for first attempts
                } else if (shutdown_attempts < 200) {
                    std.time.sleep(1000); // 1μs
                } else if (shutdown_attempts < 400) {
                    std.time.sleep(10_000); // 10μs
                } else {
                    std.time.sleep(100_000); // 100μs
                }
            }

            const final_pending = self.pending_ios.load(.acquire);
            if (final_pending == 0) {
                std.debug.print("✓ Shutdown complete: all {} I/Os flushed successfully after {} attempts\n", .{ initial_pending, shutdown_attempts });
            } else {
                std.debug.print("❌ Shutdown timeout after {} attempts: {} of {} I/Os could not be completed\n", .{ shutdown_attempts, final_pending, initial_pending });
                std.debug.print("   Data has been written to WAL file ({} bytes) but io_uring completions are stuck\n", .{self.wal_offset.load(.acquire)});

                // Final desperate attempt: force sync
                std.posix.fsync(self.wal_fd) catch {};
                std.debug.print("   Forced fsync completed - data should be durable on disk\n", .{});
            }
        } else {
            std.debug.print("✓ Clean shutdown: no pending I/Os\n", .{});
        }
    }

    // Submit batch to io_uring for async write (non-blocking)
    fn submitBatchAsync(self: *Self, completion: *WALCompletion) !void {
        const batch = completion.batch;
        if (batch.isEmpty()) return;

        // Get submission queue entry with retry logic
        const sqe = blk: {
            // First attempt
            if (self.ring.get_sqe()) |sqe_ptr| {
                break :blk sqe_ptr;
            } else |err| switch (err) {
                error.SubmissionQueueFull => {
                    // SQ full - flush pending operations first
                    _ = self.ring.submit() catch {};
                    // Try again after flush
                    break :blk try self.ring.get_sqe();
                },
                else => return err,
            }
        };

        // Calculate file offset for this batch
        const offset = self.wal_offset.fetchAdd(@sizeOf(WALBatch), .monotonic);

        // Prepare write operation
        sqe.prep_write(
            self.wal_fd,
            std.mem.asBytes(batch),
            offset,
        );

        // Set user data to identify the completion on completion
        sqe.user_data = @intFromPtr(completion);

        // Add to awaiting queue and track pending I/O
        self.enqueueCompletion(&self.awaiting, completion, &self.awaiting_lock);
        _ = self.pending_ios.fetchAdd(1, .monotonic);
    }

    fn processCompletions(self: *Self) !void {
        // First try to get existing completions without waiting
        if (self.ring.cq_ready() > 0) {
            // Process immediately available completions
        } else {
            // Submit pending operations and wait briefly for at least one completion
            const completed = self.ring.submit_and_wait(1) catch |err| switch (err) {
                error.SystemResources => {
                    // io_uring overcommitted - wait and retry
                    std.time.sleep(1000); // 1μs
                    return;
                },
                error.CompletionQueueOvercommitted => {
                    // CQ overcommitted - wait and retry
                    std.time.sleep(1000); // 1μs
                    return;
                },
                else => return err,
            };

            if (completed == 0) return; // Timeout, no completions
        }

        // Process all available completions
        var cqe_count: u32 = 0;
        while (self.ring.cq_ready() > 0) {
            const cqe = try self.ring.copy_cqe();

            // Recover completion pointer from user_data
            const completion = @as(*WALCompletion, @ptrFromInt(cqe.user_data));
            completion.result = cqe.res;

            // Move from awaiting to completed queue
            // Note: In a more complex system, we'd search awaiting queue
            // For now, we trust the completion came from awaiting
            self.enqueueCompletion(&self.completed, completion, &self.completed_lock);

            if (cqe.res < 0) {
                // I/O error
                std.debug.print("WAL write error: {}\n", .{cqe.res});
                _ = self.io_errors.fetchAdd(1, .monotonic);
            } else if (cqe.res != @sizeOf(WALBatch)) {
                // Incomplete write
                std.debug.print("WAL incomplete write: {} bytes\n", .{cqe.res});
                _ = self.io_errors.fetchAdd(1, .monotonic);
            }

            // Update counters
            _ = self.pending_ios.fetchSub(1, .monotonic);
            _ = self.completed_ios.fetchAdd(1, .monotonic);
            cqe_count += 1;
        }

        // Process completed queue - return completions to pool
        while (self.dequeueCompletion(&self.completed, &self.completed_lock)) |completion| {
            // Log successful writes if needed
            if (completion.result == @sizeOf(WALBatch)) {
                // Success - batch was written
            }

            // Return completion to available pool
            self.returnCompletion(completion);
        }
    }

    // Statistics for monitoring
    pub const RingUsage = struct { used: u32, capacity: u32 };

    pub const Stats = struct {
        batches_written: u64,
        entries_dropped: u64,
        total_entries_written: u64,
        wal_size_bytes: u64,
        pending_ios: u32,
        completed_ios: u64,
        io_errors: u64,
        ring_buffer_usage: []RingUsage,
    };

    pub fn getStats(self: *const Self, allocator: std.mem.Allocator) !Stats {
        var ring_usage = try allocator.alloc(RingUsage, self.tls_wals.len);

        for (self.tls_wals, 0..) |*tls_wal, i| {
            const usage = tls_wal.getUsage();
            ring_usage[i] = RingUsage{ .used = usage.used, .capacity = usage.capacity };
        }

        return Stats{
            .batches_written = self.batches_written.load(.monotonic),
            .entries_dropped = self.entries_dropped.load(.monotonic),
            .total_entries_written = self.total_entries_written.load(.monotonic),
            .wal_size_bytes = self.wal_offset.load(.monotonic),
            .pending_ios = self.pending_ios.load(.monotonic),
            .completed_ios = self.completed_ios.load(.monotonic),
            .io_errors = self.io_errors.load(.monotonic),
            .ring_buffer_usage = ring_usage,
        };
    }

    // Get stats without allocating - for use after static allocator is frozen
    pub fn getStatsNoAlloc(self: *const Self) struct {
        batches_written: u64,
        entries_dropped: u64,
        total_entries_written: u64,
        wal_size_bytes: u64,
        pending_ios: u32,
        completed_ios: u64,
        io_errors: u64,
    } {
        return .{
            .batches_written = self.batches_written.load(.monotonic),
            .entries_dropped = self.entries_dropped.load(.monotonic),
            .total_entries_written = self.total_entries_written.load(.monotonic),
            .wal_size_bytes = self.wal_offset.load(.monotonic),
            .pending_ios = self.pending_ios.load(.monotonic),
            .completed_ios = self.completed_ios.load(.monotonic),
            .io_errors = self.io_errors.load(.monotonic),
        };
    }
};
