const std = @import("std");
const os_compat = @import("os_compat.zig");

// WILD client protocol implementation
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

pub const WildClient = struct {
    const Self = @This();

    socket_fd: std.posix.fd_t,
    server_address: []const u8,
    server_port: u16,

    pub fn init(server_address: []const u8, server_port: u16) !Self {
        return Self{
            .socket_fd = 0, // Will be set when connecting
            .server_address = server_address,
            .server_port = server_port,
        };
    }

    pub fn connect(self: *Self) !void {
        // Create socket
        self.socket_fd = try os_compat.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);

        // Parse server address
        const addr = try os_compat.Address.parseIp(self.server_address, self.server_port);

        // Connect to server
        try os_compat.connect(self.socket_fd, &addr.any, addr.getOsSockLen());
    }

    pub fn disconnect(self: *Self) void {
        if (self.socket_fd != 0) {
            os_compat.close(self.socket_fd);
            self.socket_fd = 0;
        }
    }

    /// Send the auth secret. The server reads the first message on a connection
    /// as the secret, and answers with one byte. An empty secret means the
    /// server runs without authentication, so nothing is sent.
    pub fn authenticate(self: *Self, auth_secret: []const u8) !void {
        if (auth_secret.len == 0) return;

        _ = try os_compat.write(self.socket_fd, auth_secret);

        var reply: [1]u8 = undefined;
        const read = try std.posix.read(self.socket_fd, &reply);
        if (read != 1 or reply[0] != @backingInt(Response.ok)) {
            return error.AuthFailed;
        }
    }

    pub fn set(self: *Self, key: []const u8, value: []const u8) !void {
        if (key.len == 0 or key.len > MAX_KEY_STRING_SIZE) {
            return error.InvalidKeySize;
        }
        if (value.len == 0 or value.len > MAX_VALUE_SIZE) {
            return error.InvalidValueSize;
        }

        var message_buffer: [MAX_MESSAGE_SIZE]u8 = undefined;

        // Build message: header + key + value
        const header = MessageHeader{
            .op = .set,
            .value_len = @intCast(value.len),
        };

        @memcpy(message_buffer[0..@sizeOf(MessageHeader)], std.mem.asBytes(&header));
        const key_start = @sizeOf(MessageHeader);
        @memcpy(message_buffer[key_start .. key_start + key.len], key);
        const value_start = key_start + key.len;
        @memcpy(message_buffer[value_start .. value_start + value.len], value);

        const message_len = @sizeOf(MessageHeader) + key.len + value.len;

        // Send message
        _ = try os_compat.write(self.socket_fd, message_buffer[0..message_len]);

        // Read response
        var response_buffer: [1]u8 = undefined;
        _ = try std.posix.read(self.socket_fd, &response_buffer);

        const response: Response = @fromBackingInt(@intCast(response_buffer[0]));
        switch (response) {
            .ok => return,
            .err => return error.ServerError,
            .not_found => return error.NotFound, // Shouldn't happen for SET
        }
    }

    pub fn get(self: *Self, key: []const u8, allocator: std.mem.Allocator) !?[]u8 {
        if (key.len == 0 or key.len > MAX_KEY_STRING_SIZE) {
            return error.InvalidKeySize;
        }

        var message_buffer: [MAX_MESSAGE_SIZE]u8 = undefined;

        // Build message: header + key
        const header = MessageHeader{
            .op = .get,
            .value_len = 0, // Not used for GET
        };

        @memcpy(message_buffer[0..@sizeOf(MessageHeader)], std.mem.asBytes(&header));
        const key_start = @sizeOf(MessageHeader);
        @memcpy(message_buffer[key_start .. key_start + key.len], key);

        const message_len = @sizeOf(MessageHeader) + key.len;

        // Send message
        _ = try os_compat.write(self.socket_fd, message_buffer[0..message_len]);

        // Read response header
        var response_header: [2]u8 = undefined;
        const bytes_read = try std.posix.read(self.socket_fd, &response_header);

        if (bytes_read < 1) {
            return error.InvalidResponse;
        }

        const response: Response = @fromBackingInt(@intCast(response_header[0]));
        switch (response) {
            .ok => {
                if (bytes_read < 2) {
                    return error.InvalidResponse;
                }
                const value_len = response_header[1];
                if (value_len == 0) {
                    return try allocator.dupe(u8, "");
                }

                // Read value data
                const value = try allocator.alloc(u8, value_len);
                _ = try std.posix.read(self.socket_fd, value);
                return value;
            },
            .not_found => return null,
            .err => return error.ServerError,
        }
    }

    pub fn delete(self: *Self, key: []const u8) !bool {
        if (key.len == 0 or key.len > MAX_KEY_STRING_SIZE) {
            return error.InvalidKeySize;
        }

        var message_buffer: [MAX_MESSAGE_SIZE]u8 = undefined;

        // Build message: header + key
        const header = MessageHeader{
            .op = .delete,
            .value_len = 0, // Not used for DELETE
        };

        @memcpy(message_buffer[0..@sizeOf(MessageHeader)], std.mem.asBytes(&header));
        const key_start = @sizeOf(MessageHeader);
        @memcpy(message_buffer[key_start .. key_start + key.len], key);

        const message_len = @sizeOf(MessageHeader) + key.len;

        // Send message
        _ = try os_compat.write(self.socket_fd, message_buffer[0..message_len]);

        // Read response
        var response_buffer: [1]u8 = undefined;
        _ = try std.posix.read(self.socket_fd, &response_buffer);

        const response: Response = @fromBackingInt(@intCast(response_buffer[0]));
        switch (response) {
            .ok => return true,
            .not_found => return false,
            .err => return error.ServerError,
        }
    }
};

fn printUsage() void {
    std.debug.print("WILD Client\n", .{});
    std.debug.print("Usage: wild-client [--auth-secret SECRET] <server:port> <command> [args...]\n\n", .{});
    std.debug.print("Commands:\n", .{});
    std.debug.print("  set <key> <value>  - Store a key-value pair\n", .{});
    std.debug.print("  get <key>          - Retrieve value for key\n", .{});
    std.debug.print("  delete <key>       - Delete key-value pair\n", .{});
    std.debug.print("\nOptions:\n", .{});
    std.debug.print("  --auth-secret SECRET  Secret the server was started with.\n", .{});
    std.debug.print("                        Reads WILD_AUTH_SECRET if omitted.\n", .{});
    std.debug.print("                        Omit it when the server has no secret.\n", .{});
    std.debug.print("\nOutput:\n", .{});
    std.debug.print("  The value from get goes to stdout. Everything else goes\n", .{});
    std.debug.print("  to stderr, so `wild-client ... get k > file` holds only the value.\n", .{});
    std.debug.print("\nExit status:\n", .{});
    std.debug.print("  0 success   1 request failed   2 bad usage\n", .{});
    std.debug.print("\nExamples:\n", .{});
    std.debug.print("  wild-client 127.0.0.1:7878 set name 'John Doe'\n", .{});
    std.debug.print("  wild-client 127.0.0.1:7878 get name\n", .{});
    std.debug.print("  wild-client --auth-secret s3cret 127.0.0.1:7878 delete name\n", .{});
}

/// Report a failure and leave nonzero, so a script can tell success from error.
fn fail(comptime fmt: []const u8, args: anytype) noreturn {
    std.debug.print(fmt, args);
    std.process.exit(1);
}

/// Write data to stdout. Values from `get` belong there, so they can be piped
/// or redirected. Diagnostics stay on stderr, where std.debug.print sends them.
fn printStdout(io: std.Io, text: []const u8) void {
    std.Io.File.stdout().writeStreamingAll(io, text) catch {
        // A closed pipe, as with `... | head`, is not a client failure.
    };
}

fn usageFail() noreturn {
    printUsage();
    std.process.exit(2);
}

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.page_allocator;

    const args = try init.minimal.args.toSlice(init.arena.allocator());

    // --auth-secret is accepted anywhere. WILD_AUTH_SECRET is the fallback, so
    // the secret need not appear in the process list or shell history.
    var auth_secret: []const u8 = "";
    var positional: std.ArrayList([]const u8) = .empty;

    var i: usize = 1; // args[0] is the program name
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--auth-secret")) {
            if (i + 1 >= args.len) {
                std.debug.print("Error: --auth-secret needs a value\n", .{});
                usageFail();
            }
            auth_secret = args[i + 1];
            i += 1;
            continue;
        }
        try positional.append(allocator, arg);
    }
    defer positional.deinit(allocator);

    if (auth_secret.len == 0) {
        auth_secret = init.environ_map.get("WILD_AUTH_SECRET") orelse "";
    }

    const params = positional.items;
    if (params.len < 2) {
        usageFail();
    }

    // Parse server address
    const server_arg = params[0];
    const colon_pos = std.mem.lastIndexOf(u8, server_arg, ":") orelse {
        std.debug.print("Error: Server address must be in format address:port\n", .{});
        usageFail();
    };

    const server_address = server_arg[0..colon_pos];
    const port_str = server_arg[colon_pos + 1 ..];
    const server_port = std.fmt.parseInt(u16, port_str, 10) catch {
        std.debug.print("Error: Invalid port number: {s}\n", .{port_str});
        usageFail();
    };

    // Parse command
    const command = params[1];

    // Create and connect client
    var client = try WildClient.init(server_address, server_port);
    client.connect() catch |err| {
        fail("Error connecting to server {s}:{}: {}\n", .{ server_address, server_port, err });
    };
    defer client.disconnect();

    client.authenticate(auth_secret) catch |err| {
        fail("Error authenticating with {s}:{}: {}\n", .{ server_address, server_port, err });
    };

    if (std.mem.eql(u8, command, "set")) {
        if (params.len != 4) {
            std.debug.print("Error: set command requires key and value\n", .{});
            std.debug.print("Usage: wild-client <server:port> set <key> <value>\n", .{});
            usageFail();
        }

        const key = params[2];
        const value = params[3];

        client.set(key, value) catch |err| {
            fail("Error setting key: {}\n", .{err});
        };

        std.debug.print("OK\n", .{});
    } else if (std.mem.eql(u8, command, "get")) {
        if (params.len != 3) {
            std.debug.print("Error: get command requires key\n", .{});
            std.debug.print("Usage: wild-client <server:port> get <key>\n", .{});
            usageFail();
        }

        const key = params[2];

        const result = client.get(key, allocator) catch |err| {
            fail("Error getting key: {}\n", .{err});
        };

        if (result) |value| {
            defer allocator.free(value);
            printStdout(init.io, value);
            printStdout(init.io, "\n");
        } else {
            std.debug.print("(nil)\n", .{});
        }
    } else if (std.mem.eql(u8, command, "delete")) {
        if (params.len != 3) {
            std.debug.print("Error: delete command requires key\n", .{});
            std.debug.print("Usage: wild-client <server:port> delete <key>\n", .{});
            usageFail();
        }

        const key = params[2];

        const deleted = client.delete(key) catch |err| {
            fail("Error deleting key: {}\n", .{err});
        };

        if (deleted) {
            std.debug.print("OK\n", .{});
        } else {
            std.debug.print("Key not found\n", .{});
        }
    } else {
        std.debug.print("Error: Unknown command '{s}'\n", .{command});
        usageFail();
    }
}
