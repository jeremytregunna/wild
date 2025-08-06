const std = @import("std");

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
        self.socket_fd = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);

        // Parse server address
        const addr = try std.net.Address.parseIp(self.server_address, self.server_port);

        // Connect to server
        try std.posix.connect(self.socket_fd, &addr.any, addr.getOsSockLen());
    }

    pub fn disconnect(self: *Self) void {
        if (self.socket_fd != 0) {
            std.posix.close(self.socket_fd);
            self.socket_fd = 0;
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
        _ = try std.posix.write(self.socket_fd, message_buffer[0..message_len]);

        // Read response
        var response_buffer: [1]u8 = undefined;
        _ = try std.posix.read(self.socket_fd, &response_buffer);

        const response: Response = @enumFromInt(response_buffer[0]);
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
        _ = try std.posix.write(self.socket_fd, message_buffer[0..message_len]);

        // Read response header
        var response_header: [2]u8 = undefined;
        const bytes_read = try std.posix.read(self.socket_fd, &response_header);

        if (bytes_read < 1) {
            return error.InvalidResponse;
        }

        const response: Response = @enumFromInt(response_header[0]);
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
        _ = try std.posix.write(self.socket_fd, message_buffer[0..message_len]);

        // Read response
        var response_buffer: [1]u8 = undefined;
        _ = try std.posix.read(self.socket_fd, &response_buffer);

        const response: Response = @enumFromInt(response_buffer[0]);
        switch (response) {
            .ok => return true,
            .not_found => return false,
            .err => return error.ServerError,
        }
    }
};

fn printUsage() void {
    std.debug.print("WILD Client\n", .{});
    std.debug.print("Usage: wild-client <server:port> <command> [args...]\n\n", .{});
    std.debug.print("Commands:\n", .{});
    std.debug.print("  set <key> <value>  - Store a key-value pair\n", .{});
    std.debug.print("  get <key>          - Retrieve value for key\n", .{});
    std.debug.print("  delete <key>       - Delete key-value pair\n", .{});
    std.debug.print("\nExamples:\n", .{});
    std.debug.print("  wild-client 127.0.0.1:9999 set name 'John Doe'\n", .{});
    std.debug.print("  wild-client 127.0.0.1:9999 get name\n", .{});
    std.debug.print("  wild-client 127.0.0.1:9999 delete name\n", .{});
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 3) {
        printUsage();
        return;
    }

    // Parse server address
    const server_arg = args[1];
    const colon_pos = std.mem.lastIndexOf(u8, server_arg, ":") orelse {
        std.debug.print("Error: Server address must be in format address:port\n", .{});
        return;
    };

    const server_address = server_arg[0..colon_pos];
    const port_str = server_arg[colon_pos + 1 ..];
    const server_port = std.fmt.parseInt(u16, port_str, 10) catch {
        std.debug.print("Error: Invalid port number: {s}\n", .{port_str});
        return;
    };

    // Parse command
    const command = args[2];

    // Create and connect client
    var client = try WildClient.init(server_address, server_port);
    client.connect() catch |err| {
        std.debug.print("Error connecting to server {s}:{}: {}\n", .{ server_address, server_port, err });
        return;
    };
    defer client.disconnect();

    if (std.mem.eql(u8, command, "set")) {
        if (args.len != 5) {
            std.debug.print("Error: set command requires key and value\n", .{});
            std.debug.print("Usage: wild-client <server:port> set <key> <value>\n", .{});
            return;
        }

        const key = args[3];
        const value = args[4];

        client.set(key, value) catch |err| {
            std.debug.print("Error setting key: {}\n", .{err});
            return;
        };

        std.debug.print("OK\n", .{});
    } else if (std.mem.eql(u8, command, "get")) {
        if (args.len != 4) {
            std.debug.print("Error: get command requires key\n", .{});
            std.debug.print("Usage: wild-client <server:port> get <key>\n", .{});
            return;
        }

        const key = args[3];

        const result = client.get(key, allocator) catch |err| {
            std.debug.print("Error getting key: {}\n", .{err});
            return;
        };

        if (result) |value| {
            defer allocator.free(value);
            std.debug.print("{s}\n", .{value});
        } else {
            std.debug.print("(nil)\n", .{});
        }
    } else if (std.mem.eql(u8, command, "delete")) {
        if (args.len != 4) {
            std.debug.print("Error: delete command requires key\n", .{});
            std.debug.print("Usage: wild-client <server:port> delete <key>\n", .{});
            return;
        }

        const key = args[3];

        const deleted = client.delete(key) catch |err| {
            std.debug.print("Error deleting key: {}\n", .{err});
            return;
        };

        if (deleted) {
            std.debug.print("OK\n", .{});
        } else {
            std.debug.print("Key not found\n", .{});
        }
    } else {
        std.debug.print("Error: Unknown command '{s}'\n", .{command});
        printUsage();
    }
}
