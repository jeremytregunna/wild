const std = @import("std");

// Simple wire protocol for WILD database client-server communication
// Uses binary messages for efficiency

pub const WireMessage = extern struct {
    message_type: MessageType,
    key: u64,
    data_length: u32,
    status: Status,
    reserved: u32,
    // Variable-length data follows

    pub const MessageType = enum(u32) {
        read_request = 1,
        read_response = 2,
        write_request = 3,
        write_response = 4,
        delete_request = 5,
        delete_response = 6,
        error_response = 7,
    };

    pub const Status = enum(u32) {
        success = 0,
        not_found = 1,
        error_internal = 2,
        error_invalid_key = 3,
        error_data_too_large = 4,
        error_connection = 5,
    };

    pub fn init(msg_type: MessageType, key: u64, data_length: u32, status: Status) WireMessage {
        return WireMessage{
            .message_type = msg_type,
            .key = key,
            .data_length = data_length,
            .status = status,
            .reserved = 0,
        };
    }
};

pub const WireProtocol = struct {
    const Self = @This();
    const HEADER_SIZE = @sizeOf(WireMessage);
    const MAX_DATA_SIZE = 1024 * 1024; // 1MB max payload

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
        
        if (message.message_type != .read_response) {
            return error.UnexpectedMessageType;
        }

        if (message.status == .not_found) {
            return ReadResult{ .data = null, .allocator = allocator };
        }

        if (message.status != .success) {
            return error.ServerError;
        }

        // Receive data if any
        if (message.data_length > 0) {
            if (message.data_length > MAX_DATA_SIZE) {
                return error.DataTooLarge;
            }

            const data = try allocator.alloc(u8, message.data_length);
            const data_received = try std.posix.recv(socket_fd, data, 0);
            if (data_received != message.data_length) {
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
        
        if (message.message_type != .write_response) {
            return error.UnexpectedMessageType;
        }

        return message.status == .success;
    }

    pub fn receiveDeleteResponse(socket_fd: std.posix.fd_t) !bool {
        var header_bytes: [HEADER_SIZE]u8 = undefined;
        const header_received = try std.posix.recv(socket_fd, &header_bytes, 0);
        if (header_received != HEADER_SIZE) {
            return error.IncompleteRead;
        }

        const message = std.mem.bytesToValue(WireMessage, &header_bytes);
        
        if (message.message_type != .delete_response) {
            return error.UnexpectedMessageType;
        }

        return message.status == .success;
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