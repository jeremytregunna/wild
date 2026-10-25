const std = @import("std");
const os_compat = @import("os_compat.zig");
const wild = @import("wild.zig");
const flat_hash_storage = @import("flat_hash_storage.zig");
const static_allocator = @import("static_allocator.zig");

// Simple binary protocol for WILD server
// Each message starts with a 2-byte header:
// - 1 byte: operation type (SET=1, GET=2, DELETE=3)
// - 1 byte: value length (max 52, only used for SET operations)
// Followed by:
// - key data (variable length string, will be hashed to 8 bytes internally)
// - value data (up to 52 bytes, only for SET operations)

const Operation = enum(u8) {
    set = 1,
    get = 2,
    delete = 3,
};

const MessageHeader = packed struct {
    op: Operation,
    value_len: u8, // Only used for SET operations, max 52
};

const Response = enum(u8) {
    ok = 0,
    not_found = 1,
    err = 2,
};

/// Maximum concurrent connections. Every slot carries two message buffers, so
/// this number also sets the memory the server reserves before the allocator
/// freezes: MAX_CLIENTS x 2 x MAX_MESSAGE_SIZE.
const MAX_CLIENTS = 100_000;

/// Bits used by the slot field of `user_data`. 17 bits covers MAX_CLIENTS.
const slot_bits: u6 = 17;
const slot_mask: u64 = (1 << slot_bits) - 1;

const CompletionKind = enum(u1) { read = 0, write = 1 };

/// io_uring `user_data` tags. One completion queue carries accepts, reads, and
/// writes, so every completion has to say which it was.
///
/// Reads and writes used to share one value, so each write completion was
/// handled as a read of `bytes_written` bytes: a one-byte message that never
/// parsed, and the server answered with an error forever.
///
/// Slots used to be compacted on disconnect, which misattributed every read
/// already in flight for the connection that moved. The tag now carries a
/// generation as well as the slot:
///
///     0                        accept
///     1                        the wake-up eventfd
///     generation<<18 | (slot+1)<<1 | kind   client read (0) or write (1)
///
/// A slot is released and later handed to a new connection with a fresh
/// generation, so a late completion is recognised as stale and dropped instead
/// of being delivered to whoever holds the slot next.
inline fn readUserData(slot: usize, generation: u32) u64 {
    return userData(slot, generation, .read);
}

inline fn writeUserData(slot: usize, generation: u32) u64 {
    return userData(slot, generation, .write);
}

inline fn userData(slot: usize, generation: u32, kind: CompletionKind) u64 {
    const slot_field = (@as(u64, slot) + 1) << 1;
    const gen_field = @as(u64, generation) << (slot_bits + 1);
    return slot_field | gen_field | @as(u64, @intFromEnum(kind));
}

const Completion = struct {
    slot: usize,
    generation: u32,
    kind: CompletionKind,
};

/// Null for the accept and wake-up tags, and for anything that cannot be a
/// slot tag.
inline fn decodeCompletion(user_data: u64) ?Completion {
    if (user_data <= 1) return null; // accept, and the wake-up read

    const slot_field = (user_data >> 1) & slot_mask;
    if (slot_field == 0 or slot_field > MAX_CLIENTS) return null;

    return .{
        .slot = @intCast(slot_field - 1),
        .generation = @intCast(user_data >> (slot_bits + 1)),
        .kind = @enumFromInt(@as(u1, @truncate(user_data))),
    };
}

/// The `user_data` of a read posted on the wake-up pipe. Client tags are 2 or
/// higher and accept is 0, so 1 is free.
const wakeUserData: u64 = 1;

const MAX_MESSAGE_SIZE = 2 + 255 + 52; // header + max key string + max value
const MAX_KEY_STRING_SIZE = 255; // String key before hashing
const MAX_VALUE_SIZE = 52; // WILD's record data limit

/// Requests collected from one drain of the completion queue, so the lookups
/// for several connections go out together and their cache misses overlap.
///
/// 64 is a floor of two things: the prefetch group size that measured best
/// (docs/bench/README.md), and enough SQE headroom. Each entry costs one write
/// and one read, so a full batch plus a full drain stays inside the 256-deep
/// submission queue.
const max_batched_reads = 64;

const PendingRead = struct {
    slot: usize,
    generation: u32,
    key_hash: u64,
};

/// Where a connection stands. Kept explicit because the completion handler
/// cannot ask the socket what stage it is at.
pub const ClientState = enum(u8) {
    /// Not in use. The slot can go to the next connection.
    free,
    /// Connected. The first message must be the shared secret, unless the
    /// server started without one.
    awaiting_auth,
    /// Authenticated, or authentication is off. Messages are commands.
    ready,
};

/// One connection.
///
/// The slot never moves while the connection lives. That is the whole point:
/// an io_uring completion carries only the tag handed to it at submission, so
/// a connection whose read is in flight must keep the address and the index it
/// was posted with.
const Client = struct {
    fd: std.posix.fd_t = 0,
    state: ClientState = .free,
    generation: u32 = 0,
    read_buf: [MAX_MESSAGE_SIZE]u8 = std.mem.zeroes([MAX_MESSAGE_SIZE]u8),
    write_buf: [MAX_MESSAGE_SIZE]u8 = std.mem.zeroes([MAX_MESSAGE_SIZE]u8),
};

pub const WildServer = struct {
    const Self = @This();

    database: *wild.WILD,
    static_alloc: *static_allocator.StaticAllocator,
    bind_address: []const u8,
    port: u16,
    auth_secret: []const u8,

    // Fixed slots, allocated before the allocator freezes. Index is stable for
    // the life of the connection, which is what makes a posted read safe to
    // attribute when it lands.
    clients: []Client,
    // LIFO of free slot indices. Push and pop only, so accepting a connection
    // never allocates.
    free_slots: []u32,
    free_count: usize,
    active_clients: usize,

    // One drain's worth of GET requests, held so their lookups can be issued
    // as one batch. Fixed size and reused: the allocator is frozen by the time
    // the first request arrives.
    batch_entries: [max_batched_reads]PendingRead,
    batch_keys: [max_batched_reads]u64,
    batch_results: [max_batched_reads]?*const flat_hash_storage.CacheLineRecord,
    batch_count: usize,

    // Diagnostics. If `batches` equals `batched_reads`, the collection step
    // never grouped anything and the batching is dead weight.
    batched_reads: std.atomic.Value(u64),
    batches: std.atomic.Value(u64),

    // Set by requestShutdown from another thread. The accept loop checks it
    // between batches of completions.
    shutdown: std.atomic.Value(bool),

    // The loop usually waits inside io_uring_enter, where no flag check can
    // reach it. A read on this eventfd is posted in the ring, and writing to
    // the same fd from another thread ends the wait.
    wake_fd: std.posix.fd_t,
    wake_buf: [8]u8,

    pub fn init(database: *wild.WILD, static_alloc: *static_allocator.StaticAllocator, bind_address: []const u8, port: u16, auth_secret: []const u8) !Self {
        const alloc = static_alloc.allocator();

        // One allocation for every slot, and no allocation on the accept path
        // afterwards: the allocator refuses to allocate once it is frozen.
        const clients = try alloc.alloc(Client, MAX_CLIENTS);
        @memset(clients, std.mem.zeroes(Client));
        const free_slots = try alloc.alloc(u32, MAX_CLIENTS);

        // Fill so that slot 0 is handed out first.
        for (0..MAX_CLIENTS) |i| {
            free_slots[i] = @intCast(MAX_CLIENTS - 1 - i);
        }

        const wake_raw = std.os.linux.eventfd(0, std.os.linux.EFD.NONBLOCK | std.os.linux.EFD.CLOEXEC);
        switch (std.posix.errno(wake_raw)) {
            .SUCCESS => {},
            else => return error.EventFdFailed,
        }

        return Self{
            .database = database,
            .static_alloc = static_alloc,
            .bind_address = bind_address,
            .port = port,
            .auth_secret = auth_secret,
            .clients = clients,
            .free_slots = free_slots,
            .free_count = MAX_CLIENTS,
            .active_clients = 0,
            .batch_entries = undefined,
            .batch_keys = undefined,
            .batch_results = undefined,
            .batch_count = 0,
            .batched_reads = .init(0),
            .batches = .init(0),
            .shutdown = .init(false),
            .wake_fd = @intCast(wake_raw),
            .wake_buf = undefined,
        };
    }

    /// Ask the accept loop to return. Safe to call from another thread.
    pub fn requestShutdown(self: *Self) void {
        self.shutdown.store(true, .release);

        // End the io_uring wait. The posted read on wake_fd completes, so the
        // loop runs again even with no client traffic.
        const wake_value = [_]u8{ 1, 1, 1, 1, 1, 1, 1, 1 };
        _ = std.os.linux.write(self.wake_fd, &wake_value, wake_value.len);
    }

    pub fn start(self: *Self) !void {
        // Initialize io_uring for async networking
        var ring = try std.os.linux.IoUring.init(256, 0);
        defer ring.deinit();
        defer os_compat.close(self.wake_fd);

        // Create socket and bind
        const sock_fd = try os_compat.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
        defer os_compat.close(sock_fd);

        // Set socket options
        try std.posix.setsockopt(sock_fd, std.posix.SOL.SOCKET, std.posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));

        // Parse bind address
        const addr = try os_compat.Address.parseIp(self.bind_address, self.port);
        try os_compat.bind(sock_fd, &addr.any, addr.getOsSockLen());
        try os_compat.listen(sock_fd, 128);

        std.debug.print("WILD server listening on {s}:{}\n", .{ self.bind_address, self.port });

        // Submit initial accept
        const accept_sqe = try ring.get_sqe();
        accept_sqe.prep_accept(sock_fd, null, null, 0);
        accept_sqe.user_data = 0; // Accept operations use user_data = 0

        // Arm the wake-up read so requestShutdown can end the wait below.
        const wake_sqe = try ring.get_sqe();
        wake_sqe.prep_read(self.wake_fd, &self.wake_buf, 0);
        wake_sqe.user_data = wakeUserData;

        _ = try ring.submit();

        while (true) {
            // Stop cleanly when the daemon asks.
            if (self.shutdown.load(.acquire)) {
                std.debug.print("WILD server stopping\n", .{});
                return;
            }

            // Wait for completions. A signal interrupts the wait, which is how
            // SIGINT and SIGTERM reach a server that has no traffic.
            _ = ring.submit_and_wait(1) catch |err| switch (err) {
                error.SignalInterrupt => continue,
                else => return err,
            };

            var cq_count: u32 = 0;
            var since_submit: u32 = 0;
            while (ring.cq_ready() > 0) {
                // Submit after every batch's worth of completions. The
                // submission queue is 256 deep and each completion posts a
                // write plus the next read, so draining without submitting can
                // run it dry, and get_sqe would take the server down.
                if (since_submit >= max_batched_reads) {
                    try self.flushReadBatch(&ring);
                    _ = try ring.submit();
                    since_submit = 0;
                }

                const cqe = ring.copy_cqe() catch break;
                cq_count += 1;
                since_submit += 1;

                if (cqe.user_data == 0) { // Accept completion
                    if (cqe.res >= 0) {
                        const client_fd = cqe.res;

                        if (self.free_count > 0) {
                            self.free_count -= 1;
                            const slot: usize = self.free_slots[self.free_count];
                            const client = &self.clients[slot];

                            // A fresh generation invalidates anything still in
                            // flight from this slot's previous owner.
                            client.generation +%= 1;
                            client.fd = client_fd;
                            client.state = if (self.auth_secret.len == 0) .ready else .awaiting_auth;
                            self.active_clients += 1;

                            const read_sqe = try ring.get_sqe();
                            read_sqe.prep_read(client_fd, &client.read_buf, 0);
                            read_sqe.user_data = readUserData(slot, client.generation);
                        } else {
                            // No slot left. Reject without disturbing the
                            // connections already being served.
                            os_compat.close(client_fd);
                        }

                        // Submit next accept
                        const next_accept_sqe = try ring.get_sqe();
                        next_accept_sqe.prep_accept(sock_fd, null, null, 0);
                        next_accept_sqe.user_data = 0;
                    }
                } else if (cqe.user_data == wakeUserData) {
                    // requestShutdown wrote to the wake-up fd. Stop serving.
                    std.debug.print("WILD server stopping\n", .{});
                    self.closeAllClients();
                    return;
                } else {
                    // Client operation completion
                    const completion = decodeCompletion(cqe.user_data) orelse continue;
                    const client = &self.clients[completion.slot];

                    // The connection this tag was posted for is gone, and the
                    // slot may already serve someone else. Drop it.
                    if (client.state == .free or client.generation != completion.generation) continue;

                    const client_fd = client.fd;

                    if (cqe.res <= 0) {
                        // Client disconnected or errored. Any read or write of
                        // this connection that is still in flight becomes
                        // stale, because the slot goes out with a new
                        // generation.
                        self.releaseSlot(completion.slot);
                        continue;
                    }

                    // A successful write is not a request. Only reads carry a
                    // message, so ignore write completions after the error
                    // check above, which also drops clients we could not write
                    // to.
                    if (completion.kind == .write) continue;

                    // A GET is collected rather than answered here, so one
                    // batch can look up the keys for many connections at once.
                    // Nothing runs between collecting and flushing except other
                    // completions on this same thread, so the record cannot
                    // change underneath the batch.
                    {
                        const header_size = @sizeOf(MessageHeader);
                        const len: usize = @intCast(cqe.res);
                        if (len > header_size and self.batch_count < max_batched_reads and
                            client.read_buf[0] == @intFromEnum(Operation.get))
                        {
                            const key_len = len - header_size;
                            if (key_len <= MAX_KEY_STRING_SIZE) {
                                const key: []const u8 = client.read_buf[header_size..][0..key_len];
                                const hash = flat_hash_storage.hashKey(key);
                                self.batch_entries[self.batch_count] = .{
                                    .slot = completion.slot,
                                    .generation = completion.generation,
                                    .key_hash = hash,
                                };
                                self.batch_keys[self.batch_count] = hash;
                                self.batch_count += 1;
                                continue; // flushReadBatch answers it
                            }
                        }
                    }

                    // Process message with authentication check
                    const bytes_read = @as(usize, @intCast(cqe.res));
                    const message = client.read_buf[0..bytes_read];

                    const response_len = self.processMessageWithAuth(client, message, &client.write_buf) catch {
                        // Error processing, send error response
                        client.write_buf[0] = @backingInt(Response.err);
                        const error_write_sqe = try ring.get_sqe();
                        error_write_sqe.prep_write(client_fd, client.write_buf[0..1], 0);
                        error_write_sqe.user_data = writeUserData(completion.slot, completion.generation);
                        continue;
                    };

                    // Submit write response
                    const write_sqe = try ring.get_sqe();
                    write_sqe.prep_write(client_fd, client.write_buf[0..response_len], 0);
                    write_sqe.user_data = writeUserData(completion.slot, completion.generation);

                    // Submit next read for this client
                    const next_read_sqe = try ring.get_sqe();
                    next_read_sqe.prep_read(client_fd, &client.read_buf, 0);
                    next_read_sqe.user_data = readUserData(completion.slot, completion.generation);
                }
            }

            // Nothing left ready to drain, so answer whatever the last chunk
            // collected. Skipping this would leave a GET waiting for a
            // completion that is never coming.
            try self.flushReadBatch(&ring);
            if (since_submit > 0) {
                _ = try ring.submit();
                since_submit = 0;
            }

            if (cq_count == 0) {
                os_compat.sleep(1000); // 1μs
            }
        }
    }

    /// Answer every GET collected from this drain, then arm the next read for
    /// each connection that asked.
    ///
    /// The lookups are issued together, which is the point: the hash of one key
    /// tells you nothing about where the next one lives, so the hardware
    /// prefetcher cannot overlap them on its own.
    fn flushReadBatch(self: *Self, ring: *std.os.linux.IoUring) !void {
        const n = self.batch_count;
        if (n == 0) return;
        self.batch_count = 0;

        _ = self.batches.fetchAdd(1, .monotonic);
        _ = self.batched_reads.fetchAdd(n, .monotonic);

        _ = self.database.storage.readBatchPipelined(
            self.batch_keys[0..n],
            self.batch_results[0..n],
            wild.WILD.default_microbatch,
        );

        for (self.batch_entries[0..n], 0..) |entry, i| {
            const client = &self.clients[entry.slot];

            // The connection may have hung up while its key waited here. Its
            // slot could even belong to a new connection now, which is what the
            // generation check is for.
            if (client.state == .free or client.generation != entry.generation) continue;

            const response_len: usize = if (self.batch_results[i]) |record| blk: {
                const data = record.getData();
                if (data.len > MAX_MESSAGE_SIZE - 2) { // one byte status, one byte length
                    client.write_buf[0] = @backingInt(Response.err);
                    break :blk 1;
                }
                client.write_buf[0] = @backingInt(Response.ok);
                client.write_buf[1] = @intCast(data.len);
                @memcpy(client.write_buf[2 .. 2 + data.len], data);
                break :blk 2 + data.len;
            } else blk: {
                client.write_buf[0] = @backingInt(Response.not_found);
                break :blk 1;
            };

            const write_sqe = try ring.get_sqe();
            write_sqe.prep_write(client.fd, client.write_buf[0..response_len], 0);
            write_sqe.user_data = writeUserData(entry.slot, entry.generation);

            const read_sqe = try ring.get_sqe();
            read_sqe.prep_read(client.fd, &client.read_buf, 0);
            read_sqe.user_data = readUserData(entry.slot, entry.generation);
        }
    }

    /// Give up a slot. It leaves the free list only when the next connection
    /// arrives, and that arrival bumps the generation, so completions still in
    /// flight for the old connection are recognised as stale and dropped.
    fn releaseSlot(self: *Self, slot: usize) void {
        const client = &self.clients[slot];
        if (client.state == .free) return; // already released

        os_compat.close(client.fd);
        client.fd = 0;
        client.state = .free;
        self.active_clients -|= 1;

        self.free_slots[self.free_count] = @intCast(slot);
        self.free_count += 1;
    }

    /// Close every connection still open. Called when the loop stops, so a
    /// shutdown does not leave descriptors behind in a long-running process
    /// that restarts the server.
    fn closeAllClients(self: *Self) void {
        for (self.clients, 0..) |*client, slot| {
            if (client.state == .free) continue;
            os_compat.close(client.fd);
            client.fd = 0;
            client.state = .free;
            self.free_slots[self.free_count] = @intCast(slot);
            self.free_count += 1;
        }
        self.active_clients = 0;
    }

    fn processMessageWithAuth(self: *Self, client: *Client, message: []const u8, response_buffer: []u8) !usize {
        // An empty secret means the server was started without authentication,
        // so every message is a command. An empty secret used to match any
        // message, so the connection was already open; it also swallowed the
        // first command, which made the server unusable without a secret.
        if (self.auth_secret.len == 0) {
            return self.processMessage(message, response_buffer);
        }

        // Check if client is authenticated for non-auth messages
        if (client.state == .awaiting_auth) {
            // First message must be authentication
            return self.processAuthMessage(client, message, response_buffer);
        }

        // Client is authenticated, process normal message
        return self.processMessage(message, response_buffer);
    }

    fn processAuthMessage(self: *Self, client: *Client, message: []const u8, response_buffer: []u8) !usize {
        // This should be an auth request from wire protocol
        // For simplicity, we'll parse it directly here rather than using wire_protocol
        // since the server uses a different protocol format

        if (message.len < self.auth_secret.len) {
            response_buffer[0] = @backingInt(Response.err);
            return 1;
        }

        // Compare auth secret (constant-time comparison for security)
        const received_secret = message[0..self.auth_secret.len];
        var match: u8 = 0;
        for (received_secret, self.auth_secret) |a, b| {
            match |= a ^ b;
        }

        if (match == 0) {
            // Authentication successful
            client.state = .ready;
            response_buffer[0] = @backingInt(Response.ok);
            return 1;
        } else {
            // Authentication failed
            response_buffer[0] = @backingInt(Response.err);
            return 1;
        }
    }

    fn processMessage(self: *Self, message: []const u8, response_buffer: []u8) !usize {
        if (message.len < @sizeOf(MessageHeader)) {
            response_buffer[0] = @backingInt(Response.err);
            return 1;
        }

        const header = std.mem.bytesToValue(MessageHeader, message[0..@sizeOf(MessageHeader)]);
        const header_size = @sizeOf(MessageHeader);

        switch (header.op) {
            .set => {
                if (header.value_len > MAX_VALUE_SIZE or header.value_len == 0) {
                    response_buffer[0] = @backingInt(Response.err);
                    return 1;
                }

                // Calculate expected message length: header + key + value
                const key_len = message.len - header_size - header.value_len;
                if (key_len == 0 or key_len > MAX_KEY_STRING_SIZE) {
                    response_buffer[0] = @backingInt(Response.err);
                    return 1;
                }

                const key = message[header_size .. header_size + key_len];
                const value = message[header_size + key_len ..];

                if (value.len != header.value_len) {
                    response_buffer[0] = @backingInt(Response.err);
                    return 1;
                }

                self.database.writeString(key, value) catch {
                    response_buffer[0] = @backingInt(Response.err);
                    return 1;
                };

                response_buffer[0] = @backingInt(Response.ok);
                return 1;
            },
            .get => {
                const key_len = message.len - header_size;
                if (key_len == 0 or key_len > MAX_KEY_STRING_SIZE) {
                    response_buffer[0] = @backingInt(Response.err);
                    return 1;
                }

                const key = message[header_size..];

                const result = self.database.readString(key) catch {
                    response_buffer[0] = @backingInt(Response.err);
                    return 1;
                };

                if (result) |record| {
                    const data = record.getData();
                    if (data.len > response_buffer.len - 2) { // 1 byte status + 1 byte length
                        response_buffer[0] = @backingInt(Response.err);
                        return 1;
                    }

                    response_buffer[0] = @backingInt(Response.ok);
                    response_buffer[1] = @intCast(data.len);
                    @memcpy(response_buffer[2 .. 2 + data.len], data);
                    return 2 + data.len;
                } else {
                    response_buffer[0] = @backingInt(Response.not_found);
                    return 1;
                }
            },
            .delete => {
                const key_len = message.len - header_size;
                if (key_len == 0 or key_len > MAX_KEY_STRING_SIZE) {
                    response_buffer[0] = @backingInt(Response.err);
                    return 1;
                }

                const key = message[header_size..];

                const deleted = self.database.deleteString(key) catch {
                    response_buffer[0] = @backingInt(Response.err);
                    return 1;
                };

                response_buffer[0] = if (deleted) @backingInt(Response.ok) else @backingInt(Response.not_found);
                return 1;
            },
        }
    }
};
