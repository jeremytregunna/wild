const std = @import("std");

// Space-optimized wire protocol for WILD database client-server communication
// Uses bit-packed header for efficient field encoding

pub const WireMessage = extern struct {
    packed_fields: u32, // Bit-packed: message_type(4) + data_length(6) + status(3) + reserved(19)
    key: u64, // Database key

    // Bit field constants
    const MESSAGE_TYPE_MASK: u32 = 0x0000000F;
    const MESSAGE_TYPE_SHIFT: u5 = 0;
    const DATA_LENGTH_MASK: u32 = 0x000003F0;
    const DATA_LENGTH_SHIFT: u5 = 4;
    const STATUS_MASK: u32 = 0x00001C00;
    const STATUS_SHIFT: u5 = 10;
    const RESERVED_MASK: u32 = 0xFFFFE000;

    pub const MessageType = enum(u4) {
        auth_request = 1,
        auth_response = 2,
        read_request = 3,
        read_response = 4,
        write_request = 5,
        write_response = 6,
        delete_request = 7,
        delete_response = 8,
        error_response = 9,
    };

    pub const Status = enum(u3) {
        success = 0,
        not_found = 1,
        error_internal = 2,
        error_invalid_key = 3,
        error_data_too_large = 4,
        error_connection = 5,
        error_auth_required = 6,
        error_auth_failed = 7,
    };

    pub fn init(msg_type: MessageType, key: u64, data_length: u32, status: Status) WireMessage {
        const packed_value = (@as(u32, @intFromEnum(msg_type)) << MESSAGE_TYPE_SHIFT) |
            (@as(u32, data_length) << DATA_LENGTH_SHIFT) |
            (@as(u32, @intFromEnum(status)) << STATUS_SHIFT);

        return WireMessage{
            .packed_fields = packed_value,
            .key = key,
        };
    }

    pub fn getMessageType(self: *const WireMessage) MessageType {
        const value = (self.packed_fields & MESSAGE_TYPE_MASK) >> MESSAGE_TYPE_SHIFT;
        return @enumFromInt(value);
    }

    pub fn getDataLength(self: *const WireMessage) u32 {
        return (self.packed_fields & DATA_LENGTH_MASK) >> DATA_LENGTH_SHIFT;
    }

    pub fn getStatus(self: *const WireMessage) Status {
        const value = (self.packed_fields & STATUS_MASK) >> STATUS_SHIFT;
        return @enumFromInt(value);
    }

    pub fn setStatus(self: *WireMessage, status: Status) void {
        self.packed_fields = (self.packed_fields & ~STATUS_MASK) |
            (@as(u32, @intFromEnum(status)) << STATUS_SHIFT);
    }
};

comptime {
    std.debug.assert(@sizeOf(WireMessage) == 16);
    std.debug.assert(@alignOf(WireMessage) <= 8);
}

pub const WireProtocol = struct {
    const Self = @This();
    const HEADER_SIZE = @sizeOf(WireMessage);
    const MAX_DATA_SIZE = 52;

    // Authentication - first message on any connection
    pub fn sendAuthRequest(socket_fd: std.posix.fd_t, auth_secret: []const u8) !void {
        if (auth_secret.len > MAX_DATA_SIZE) {
            return error.DataTooLarge;
        }

        const message = WireMessage.init(.auth_request, 0, @intCast(auth_secret.len), .success);

        // Send header
        var bytes_sent = try std.posix.send(socket_fd, std.mem.asBytes(&message), 0);
        if (bytes_sent != HEADER_SIZE) {
            return error.IncompleteWrite;
        }

        // Send auth secret
        if (auth_secret.len > 0) {
            bytes_sent = try std.posix.send(socket_fd, auth_secret, 0);
            if (bytes_sent != auth_secret.len) {
                return error.IncompleteWrite;
            }
        }
    }

    pub fn sendAuthResponse(socket_fd: std.posix.fd_t, success: bool) !void {
        const status: WireMessage.Status = if (success) .success else .error_auth_failed;
        const message = WireMessage.init(.auth_response, 0, 0, status);

        const bytes_sent = try std.posix.send(socket_fd, std.mem.asBytes(&message), 0);
        if (bytes_sent != HEADER_SIZE) {
            return error.IncompleteWrite;
        }
    }

    pub fn receiveAuthRequest(socket_fd: std.posix.fd_t, allocator: std.mem.Allocator) ![]u8 {
        // Receive header
        var header_bytes: [HEADER_SIZE]u8 = undefined;
        const header_received = try std.posix.recv(socket_fd, &header_bytes, 0);
        if (header_received != HEADER_SIZE) {
            return error.IncompleteRead;
        }

        const message = std.mem.bytesToValue(WireMessage, &header_bytes);

        if (message.getMessageType() != .auth_request) {
            return error.UnexpectedMessageType;
        }

        const data_length = message.getDataLength();
        if (data_length > MAX_DATA_SIZE) {
            return error.DataTooLarge;
        }

        // Receive auth secret
        if (data_length > 0) {
            const auth_secret = try allocator.alloc(u8, data_length);
            const data_received = try std.posix.recv(socket_fd, auth_secret, 0);
            if (data_received != data_length) {
                allocator.free(auth_secret);
                return error.IncompleteRead;
            }
            return auth_secret;
        } else {
            return allocator.alloc(u8, 0); // Empty secret
        }
    }

    pub fn receiveAuthResponse(socket_fd: std.posix.fd_t) !bool {
        var header_bytes: [HEADER_SIZE]u8 = undefined;
        const header_received = try std.posix.recv(socket_fd, &header_bytes, 0);
        if (header_received != HEADER_SIZE) {
            return error.IncompleteRead;
        }

        const message = std.mem.bytesToValue(WireMessage, &header_bytes);

        if (message.getMessageType() != .auth_response) {
            return error.UnexpectedMessageType;
        }

        return message.getStatus() == .success;
    }

    pub fn sendReadRequest(socket_fd: std.posix.fd_t, key: u64) !void {
        const message = WireMessage.init(.read_request, key, 0, .success);
        const bytes_sent = try std.posix.send(socket_fd, std.mem.asBytes(&message), 0);
        if (bytes_sent != HEADER_SIZE) {
            return error.IncompleteWrite;
        }
    }

    pub fn sendWriteRequest(socket_fd: std.posix.fd_t, key: u64, data: []const u8) !void {
        if (data.len > MAX_DATA_SIZE) {
            return error.DataTooLarge;
        }

        const message = WireMessage.init(.write_request, key, @intCast(data.len), .success);

        // Send header
        var bytes_sent = try std.posix.send(socket_fd, std.mem.asBytes(&message), 0);
        if (bytes_sent != HEADER_SIZE) {
            return error.IncompleteWrite;
        }

        // Send data if any
        if (data.len > 0) {
            bytes_sent = try std.posix.send(socket_fd, data, 0);
            if (bytes_sent != data.len) {
                return error.IncompleteWrite;
            }
        }
    }

    pub fn sendDeleteRequest(socket_fd: std.posix.fd_t, key: u64) !void {
        const message = WireMessage.init(.delete_request, key, 0, .success);
        const bytes_sent = try std.posix.send(socket_fd, std.mem.asBytes(&message), 0);
        if (bytes_sent != HEADER_SIZE) {
            return error.IncompleteWrite;
        }
    }

    pub fn sendReadResponse(socket_fd: std.posix.fd_t, key: u64, data: ?[]const u8) !void {
        const status: WireMessage.Status = if (data != null) .success else .not_found;
        const data_len: u32 = if (data) |d| @intCast(d.len) else 0;

        const message = WireMessage.init(.read_response, key, data_len, status);

        // Send header
        var bytes_sent = try std.posix.send(socket_fd, std.mem.asBytes(&message), 0);
        if (bytes_sent != HEADER_SIZE) {
            return error.IncompleteWrite;
        }

        // Send data if found
        if (data) |d| {
            bytes_sent = try std.posix.send(socket_fd, d, 0);
            if (bytes_sent != d.len) {
                return error.IncompleteWrite;
            }
        }
    }

    pub fn sendWriteResponse(socket_fd: std.posix.fd_t, key: u64, success: bool) !void {
        const status: WireMessage.Status = if (success) .success else .error_internal;
        const message = WireMessage.init(.write_response, key, 0, status);

        const bytes_sent = try std.posix.send(socket_fd, std.mem.asBytes(&message), 0);
        if (bytes_sent != HEADER_SIZE) {
            return error.IncompleteWrite;
        }
    }

    pub fn sendDeleteResponse(socket_fd: std.posix.fd_t, key: u64, deleted: bool) !void {
        const status: WireMessage.Status = if (deleted) .success else .not_found;
        const message = WireMessage.init(.delete_response, key, 0, status);

        const bytes_sent = try std.posix.send(socket_fd, std.mem.asBytes(&message), 0);
        if (bytes_sent != HEADER_SIZE) {
            return error.IncompleteWrite;
        }
    }

    pub fn sendErrorResponse(socket_fd: std.posix.fd_t, key: u64, error_status: WireMessage.Status) !void {
        const message = WireMessage.init(.error_response, key, 0, error_status);

        const bytes_sent = try std.posix.send(socket_fd, std.mem.asBytes(&message), 0);
        if (bytes_sent != HEADER_SIZE) {
            return error.IncompleteWrite;
        }
    }

    pub const ReadResult = struct {
        data: ?[]u8,
        allocator: std.mem.Allocator,

        pub fn deinit(self: *ReadResult) void {
            if (self.data) |data| {
                self.allocator.free(data);
            }
        }
    };

    pub fn receiveReadResponse(socket_fd: std.posix.fd_t, allocator: std.mem.Allocator) !ReadResult {
        // Receive header
        var header_bytes: [HEADER_SIZE]u8 = undefined;
        const header_received = try std.posix.recv(socket_fd, &header_bytes, 0);
        if (header_received != HEADER_SIZE) {
            return error.IncompleteRead;
        }

        const message = std.mem.bytesToValue(WireMessage, &header_bytes);

        if (message.getMessageType() != .read_response) {
            return error.UnexpectedMessageType;
        }

        const status = message.getStatus();
        if (status == .not_found) {
            return ReadResult{ .data = null, .allocator = allocator };
        }

        if (status != .success) {
            return error.ServerError;
        }

        // Receive data if any
        const data_length = message.getDataLength();
        if (data_length > 0) {
            if (data_length > MAX_DATA_SIZE) {
                return error.DataTooLarge;
            }

            const data = try allocator.alloc(u8, data_length);
            const data_received = try std.posix.recv(socket_fd, data, 0);
            if (data_received != data_length) {
                allocator.free(data);
                return error.IncompleteRead;
            }

            return ReadResult{ .data = data, .allocator = allocator };
        }

        return ReadResult{ .data = null, .allocator = allocator };
    }

    pub fn receiveWriteResponse(socket_fd: std.posix.fd_t) !bool {
        var header_bytes: [HEADER_SIZE]u8 = undefined;
        const header_received = try std.posix.recv(socket_fd, &header_bytes, 0);
        if (header_received != HEADER_SIZE) {
            return error.IncompleteRead;
        }

        const message = std.mem.bytesToValue(WireMessage, &header_bytes);

        if (message.getMessageType() != .write_response) {
            return error.UnexpectedMessageType;
        }

        return message.getStatus() == .success;
    }

    pub fn receiveDeleteResponse(socket_fd: std.posix.fd_t) !bool {
        var header_bytes: [HEADER_SIZE]u8 = undefined;
        const header_received = try std.posix.recv(socket_fd, &header_bytes, 0);
        if (header_received != HEADER_SIZE) {
            return error.IncompleteRead;
        }

        const message = std.mem.bytesToValue(WireMessage, &header_bytes);

        if (message.getMessageType() != .delete_response) {
            return error.UnexpectedMessageType;
        }

        return message.getStatus() == .success;
    }

    // Timeout utilities for non-blocking operations
    pub fn setSocketTimeout(socket_fd: std.posix.fd_t, timeout_ms: u32) !void {
        const timeout = std.posix.timeval{
            .sec = @intCast(timeout_ms / 1000),
            .usec = @intCast((timeout_ms % 1000) * 1000),
        };

        try std.posix.setsockopt(socket_fd, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&timeout));
        try std.posix.setsockopt(socket_fd, std.posix.SOL.SOCKET, std.posix.SO.SNDTIMEO, std.mem.asBytes(&timeout));
    }
};
