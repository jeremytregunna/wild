const std = @import("std");
const wild = @import("wild.zig");
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

const MAX_MESSAGE_SIZE = 2 + 255 + 52; // header + max key string + max value
const MAX_KEY_STRING_SIZE = 255; // String key before hashing
const MAX_VALUE_SIZE = 52; // WILD's record data limit

pub const WildServer = struct {
    const Self = @This();

    database: *wild.WILD,
    static_alloc: *static_allocator.StaticAllocator,
    bind_address: []const u8,
    port: u16,

    // Pre-allocated buffers
    client_fds: std.ArrayList(std.posix.fd_t),
    read_buffers: std.ArrayList([MAX_MESSAGE_SIZE]u8),
    write_buffers: std.ArrayList([MAX_MESSAGE_SIZE]u8),

    pub fn init(database: *wild.WILD, static_alloc: *static_allocator.StaticAllocator, bind_address: []const u8, port: u16) !Self {
        const MAX_CLIENTS = 100000; // Support massive concurrent connections

        var client_fds = std.ArrayList(std.posix.fd_t).init(static_alloc.allocator());
        var read_buffers = std.ArrayList([MAX_MESSAGE_SIZE]u8).init(static_alloc.allocator());
        var write_buffers = std.ArrayList([MAX_MESSAGE_SIZE]u8).init(static_alloc.allocator());

        // Pre-allocate all the memory we'll need before static allocator freezes
        try client_fds.ensureTotalCapacity(MAX_CLIENTS);
        try read_buffers.ensureTotalCapacity(MAX_CLIENTS);
        try write_buffers.ensureTotalCapacity(MAX_CLIENTS);

        // Pre-allocate the actual buffer storage
        for (0..MAX_CLIENTS) |_| {
            try read_buffers.append(std.mem.zeroes([MAX_MESSAGE_SIZE]u8));
            try write_buffers.append(std.mem.zeroes([MAX_MESSAGE_SIZE]u8));
        }

        // Clear the arrays but keep capacity
        read_buffers.clearRetainingCapacity();
        write_buffers.clearRetainingCapacity();

        return Self{
            .database = database,
            .static_alloc = static_alloc,
            .bind_address = bind_address,
            .port = port,
            .client_fds = client_fds,
            .read_buffers = read_buffers,
            .write_buffers = write_buffers,
        };
    }

    pub fn start(self: *Self) !void {
        // Initialize io_uring for async networking
        var ring = try std.os.linux.IoUring.init(256, 0);
        defer ring.deinit();

        // Create socket and bind
        const sock_fd = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
        defer std.posix.close(sock_fd);

        // Set socket options
        try std.posix.setsockopt(sock_fd, std.posix.SOL.SOCKET, std.posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));

        // Parse bind address
        const addr = try std.net.Address.parseIp(self.bind_address, self.port);
        try std.posix.bind(sock_fd, &addr.any, addr.getOsSockLen());
        try std.posix.listen(sock_fd, 128);

        std.debug.print("WILD server listening on {s}:{}\n", .{ self.bind_address, self.port });

        const MAX_CLIENTS = 100000;

        // Submit initial accept
        const accept_sqe = try ring.get_sqe();
        accept_sqe.prep_accept(sock_fd, null, null, 0);
        accept_sqe.user_data = 0; // Accept operations use user_data = 0
        _ = try ring.submit();

        while (true) {
            // Wait for completions
            _ = try ring.submit_and_wait(1);

            var cq_count: u32 = 0;
            while (ring.cq_ready() > 0) {
                const cqe = ring.copy_cqe() catch break;
                cq_count += 1;

                if (cqe.user_data == 0) {
                    // Accept completion
                    if (cqe.res >= 0) {
                        const client_fd = cqe.res;

                        if (self.client_fds.items.len < MAX_CLIENTS) {
                            // Add new client
                            try self.client_fds.append(client_fd);
                            try self.read_buffers.append(undefined);
                            try self.write_buffers.append(undefined);

                            const client_idx = self.client_fds.items.len - 1;

                            // Submit read for new client
                            const read_sqe = try ring.get_sqe();
                            read_sqe.prep_read(client_fd, &self.read_buffers.items[client_idx], 0);
                            read_sqe.user_data = @intCast(client_idx + 1);
                        } else {
                            // Too many clients, close immediately
                            std.posix.close(client_fd);
                        }

                        // Submit next accept
                        const next_accept_sqe = try ring.get_sqe();
                        next_accept_sqe.prep_accept(sock_fd, null, null, 0);
                        next_accept_sqe.user_data = 0;
                    }
                } else {
                    // Client operation completion
                    const client_idx = @as(usize, @intCast(cqe.user_data - 1));

                    if (client_idx >= self.client_fds.items.len) continue;

                    const client_fd = self.client_fds.items[client_idx];

                    if (cqe.res <= 0) {
                        // Client disconnected or error
                        std.posix.close(client_fd);

                        // Remove client (swap with last)
                        const last_idx = self.client_fds.items.len - 1;
                        if (client_idx != last_idx) {
                            self.client_fds.items[client_idx] = self.client_fds.items[last_idx];
                            self.read_buffers.items[client_idx] = self.read_buffers.items[last_idx];
                            self.write_buffers.items[client_idx] = self.write_buffers.items[last_idx];
                        }
                        _ = self.client_fds.pop();
                        _ = self.read_buffers.pop();
                        _ = self.write_buffers.pop();
                        continue;
                    }

                    // Process message
                    const bytes_read = @as(usize, @intCast(cqe.res));
                    const message = self.read_buffers.items[client_idx][0..bytes_read];

                    const response_len = self.processMessage(message, &self.write_buffers.items[client_idx]) catch {
                        // Error processing, send error response
                        self.write_buffers.items[client_idx][0] = @intFromEnum(Response.err);
                        const error_write_sqe = try ring.get_sqe();
                        error_write_sqe.prep_write(client_fd, self.write_buffers.items[client_idx][0..1], 0);
                        error_write_sqe.user_data = @intCast(client_idx + 1);
                        continue;
                    };

                    // Submit write response
                    const write_sqe = try ring.get_sqe();
                    write_sqe.prep_write(client_fd, self.write_buffers.items[client_idx][0..response_len], 0);
                    write_sqe.user_data = @intCast(client_idx + 1);

                    // Submit next read for this client
                    const next_read_sqe = try ring.get_sqe();
                    next_read_sqe.prep_read(client_fd, &self.read_buffers.items[client_idx], 0);
                    next_read_sqe.user_data = @intCast(client_idx + 1);
                }
            }

            if (cq_count == 0) {
                std.time.sleep(1000); // 1μs
            }
        }
    }

    fn processMessage(self: *Self, message: []const u8, response_buffer: []u8) !usize {
        if (message.len < @sizeOf(MessageHeader)) {
            response_buffer[0] = @intFromEnum(Response.err);
            return 1;
        }

        const header = std.mem.bytesToValue(MessageHeader, message[0..@sizeOf(MessageHeader)]);
        const header_size = @sizeOf(MessageHeader);

        switch (header.op) {
            .set => {
                if (header.value_len > MAX_VALUE_SIZE or header.value_len == 0) {
                    response_buffer[0] = @intFromEnum(Response.err);
                    return 1;
                }

                // Calculate expected message length: header + key + value
                const key_len = message.len - header_size - header.value_len;
                if (key_len == 0 or key_len > MAX_KEY_STRING_SIZE) {
                    response_buffer[0] = @intFromEnum(Response.err);
                    return 1;
                }

                const key = message[header_size .. header_size + key_len];
                const value = message[header_size + key_len ..];

                if (value.len != header.value_len) {
                    response_buffer[0] = @intFromEnum(Response.err);
                    return 1;
                }

                self.database.writeString(key, value) catch {
                    response_buffer[0] = @intFromEnum(Response.err);
                    return 1;
                };

                response_buffer[0] = @intFromEnum(Response.ok);
                return 1;
            },
            .get => {
                const key_len = message.len - header_size;
                if (key_len == 0 or key_len > MAX_KEY_STRING_SIZE) {
                    response_buffer[0] = @intFromEnum(Response.err);
                    return 1;
                }

                const key = message[header_size..];

                const result = self.database.readString(key) catch {
                    response_buffer[0] = @intFromEnum(Response.err);
                    return 1;
                };

                if (result) |record| {
                    const data = record.getData();
                    if (data.len > response_buffer.len - 2) { // 1 byte status + 1 byte length
                        response_buffer[0] = @intFromEnum(Response.err);
                        return 1;
                    }

                    response_buffer[0] = @intFromEnum(Response.ok);
                    response_buffer[1] = @intCast(data.len);
                    @memcpy(response_buffer[2 .. 2 + data.len], data);
                    return 2 + data.len;
                } else {
                    response_buffer[0] = @intFromEnum(Response.not_found);
                    return 1;
                }
            },
            .delete => {
                const key_len = message.len - header_size;
                if (key_len == 0 or key_len > MAX_KEY_STRING_SIZE) {
                    response_buffer[0] = @intFromEnum(Response.err);
                    return 1;
                }

                const key = message[header_size..];

                const deleted = self.database.deleteString(key) catch {
                    response_buffer[0] = @intFromEnum(Response.err);
                    return 1;
                };

                response_buffer[0] = if (deleted) @intFromEnum(Response.ok) else @intFromEnum(Response.not_found);
                return 1;
            },
        }
    }
};
