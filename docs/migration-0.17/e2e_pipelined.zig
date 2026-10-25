//! Concurrent request test for the io_uring accept loop.
//!
//! Many connections each send one GET, all before any response is read. That is
//! the shape the server batches: one drain of the completion queue holds many
//! lookups, and they go to storage together. The test asserts the server really
//! grouped them, through its own counters, so the batching cannot quietly become
//! dead code.
//!
//! Each connection keeps one request outstanding at a time, and that is a
//! protocol limit rather than a choice made here. The header the server parses
//! carries the operation and the value length, but not the key length, so a read
//! that returns two messages glued together cannot be split: the server hashes
//! the tail as part of one key and answers `not found`. A client that pipelines
//! therefore gets wrong answers. docs/MIGRATION_0.17.md records it as an open
//! defect, together with the fact that docs/WIRE_PROTOCOL.md describes a 16-byte
//! header with a length field that the implementation does not use.
//!
//! Run with: zig build && ./zig-out/bin/e2e-pipelined

const std = @import("std");
const wildb_lib = @import("wildb_lib");

const wild = wildb_lib.wild;
const server_mod = wildb_lib.server;
const static_allocator = wildb_lib.static_allocator;
const os_compat = wildb_lib.os_compat;

const secret = "pipelined-secret";
const port: u16 = 7912;
const num_connections = 256;
const keys_per_connection = 4;
const pipelined_gets = 200;
const watchdog_ms = 30_000;

const op_get: u8 = 2; // Operation.get on the wire

fn fail(comptime format: []const u8, args: anytype) noreturn {
    std.debug.print("e2e-pipelined: FAIL " ++ format ++ "\n", args);
    std.process.exit(1);
}

fn serve(srv: *server_mod.WildServer) void {
    srv.start() catch |err| {
        std.debug.print("e2e-pipelined: server stopped: {t}\n", .{err});
    };
}

/// A stuck server would otherwise hang the suite.
fn watchdog() void {
    var waited: u64 = 0;
    while (waited < watchdog_ms) : (waited += 100) {
        os_compat.sleep(100 * std.time.ns_per_ms);
    }
    std.debug.print("e2e-pipelined: FAIL no reply within {}ms\\n", .{watchdog_ms});
    std.process.exit(1);
}

fn connectAndAuthenticate(who: []const u8) os_compat.fd_t {
    const fd = os_compat.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0) catch |err| {
        fail("{s}: socket: {t}", .{ who, err });
    };
    var addr = os_compat.Address.parseIp("127.0.0.1", port) catch |err| {
        fail("{s}: parseIp: {t}", .{ who, err });
    };
    os_compat.connect(fd, &addr.any, addr.getOsSockLen()) catch |err| {
        fail("{s}: connect: {t}", .{ who, err });
    };

    os_compat.sendAllCompat(fd, secret) catch |err| fail("{s}: send secret: {t}", .{ who, err });

    var ack: [1]u8 = undefined;
    readExact(fd, &ack, "{s}: auth reply", .{who}) catch |err| fail("{s}: auth reply: {t}", .{ who, err });
    if (ack[0] != 0) fail("{s}: auth refused", .{who});

    return fd;
}

fn readExact(fd: os_compat.fd_t, buf: []u8, comptime format: []const u8, args: anytype) !void {
    var filled: usize = 0;
    while (filled < buf.len) {
        const n = try os_compat.recv(fd, buf[filled..], 0);
        if (n == 0) {
            fail(format ++ " — connection closed after {d} of {d} bytes", args ++ .{ filled, buf.len });
        }
        filled += n;
    }
}

/// GET on the wire: one operation byte, one value-length byte, then the key.
fn sendGet(fd: os_compat.fd_t, key: []const u8) void {
    var msg: [2 + 255]u8 = undefined;
    if (key.len == 0 or key.len > 255) fail("bad key length {d}", .{key.len});
    msg[0] = op_get;
    msg[1] = 0;
    @memcpy(msg[2 .. 2 + key.len], key);
    os_compat.sendAllCompat(fd, msg[0 .. 2 + key.len]) catch |err| {
        fail("send GET: {t}", .{err});
    };
}

/// Read one response. Status 0 carries a length byte and the value.
fn readResponse(fd: os_compat.fd_t, buf: []u8) !?[]const u8 {
    var status: [1]u8 = undefined;
    try readExact(fd, &status, "response status", .{});

    switch (status[0]) {
        0 => {}, // ok, a value follows
        1 => return null, // not found
        else => fail("server returned error status {d}", .{status[0]}),
    }

    var len_buf: [1]u8 = undefined;
    try readExact(fd, &len_buf, "value length", .{});
    const len: usize = len_buf[0];
    if (len > buf.len) fail("value length {d} exceeds the buffer", .{len});

    try readExact(fd, buf[0..len], "value", .{});
    return buf[0..len];
}

fn keyName(alloc: std.mem.Allocator, connection: usize, index: usize) []const u8 {
    return std.fmt.allocPrint(alloc, "pipe-{d}-{d}", .{ connection, index }) catch fail("out of memory", .{});
}

fn valueOf(alloc: std.mem.Allocator, connection: usize, index: usize) []const u8 {
    return std.fmt.allocPrint(alloc, "v-{d}-{d}", .{ connection, index }) catch fail("out of memory", .{});
}

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var database_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer database_arena.deinit();
    var static_alloc = static_allocator.StaticAllocator.init(database_arena.allocator());

    // Big enough that a lookup does not stay in L1, which is where batching is
    // supposed to matter.
    var database = try wild.WILD.init(static_alloc.allocator(), .{ .target_capacity = 2_000_000 });

    var srv = try server_mod.WildServer.init(&database, &static_alloc, "127.0.0.1", port, secret);
    static_alloc.transitionToStatic();

    const thread = try std.Thread.spawn(.{}, serve, .{&srv});
    defer thread.join();

    _ = std.Thread.spawn(.{}, watchdog, .{}) catch {};

    // Wait for the listener.
    var attempt: usize = 0;
    while (true) : (attempt += 1) {
        const probe = os_compat.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0) catch |err| {
            fail("probe socket: {t}", .{err});
        };
        var addr = try os_compat.Address.parseIp("127.0.0.1", port);
        if (os_compat.connect(probe, &addr.any, addr.getOsSockLen())) {
            os_compat.close(probe);
            break;
        } else |_| {
            os_compat.close(probe);
            if (attempt > 400) fail("the server never listened on {}", .{port});
            os_compat.sleep(5 * std.time.ns_per_ms);
        }
    }

    // Load every key the test will ask for, over one connection.
    {
        const loader = connectAndAuthenticate("loader");
        for (0..num_connections) |c| {
            for (0..keys_per_connection) |k| {
                const key = keyName(alloc, c, k);
                const value = valueOf(alloc, c, k);
                // SET on the wire: op, value length, key, value.
                var msg: [2 + 255 + 52]u8 = undefined;
                msg[0] = 1;
                msg[1] = @intCast(value.len);
                @memcpy(msg[2 .. 2 + key.len], key);
                @memcpy(msg[2 + key.len ..][0..value.len], value);
                os_compat.sendAllCompat(loader, msg[0 .. 2 + key.len + value.len]) catch |err| {
                    fail("load SET: {t}", .{err});
                };
                var ack: [1]u8 = undefined;
                try readExact(loader, &ack, "load ack", .{});
                if (ack[0] != 0) fail("load SET refused", .{});
            }
        }
        os_compat.close(loader);
    }

    // Phase 1: one round per key index. Every connection has a request in
    // flight at the same moment, which is what makes one drain carry many
    // lookups, and no connection has two outstanding, which the protocol cannot
    // express.
    const fds = try alloc.alloc(os_compat.fd_t, num_connections);
    for (fds) |*fd| {
        fd.* = connectAndAuthenticate("client");
    }

    var value_buf: [52]u8 = undefined;
    for (0..keys_per_connection) |k| {
        for (fds, 0..) |fd, c| sendGet(fd, keyName(alloc, c, k));

        for (fds, 0..) |fd, c| {
            const got = try readResponse(fd, &value_buf);
            const expected = valueOf(alloc, c, k);
            const actual = got orelse fail("connection {d} key {d}: not found", .{ c, k });
            if (!std.mem.eql(u8, actual, expected)) {
                fail("connection {d} key {d}: got '{s}' expected '{s}'", .{ c, k, actual, expected });
            }
        }
    }

    const batches = srv.batches.load(.monotonic);
    const batched_reads = srv.batched_reads.load(.monotonic);
    if (batched_reads == 0) fail("the batch path was never used", .{});
    if (batches >= batched_reads) {
        fail("nothing was ever grouped: {d} batches for {d} batched reads", .{ batches, batched_reads });
    }
    std.debug.print("e2e-pipelined: {} GETs answered in {} batches (average {d:.1} per batch)\n", .{
        batched_reads, batches, @as(f64, @floatFromInt(batched_reads)) / @as(f64, @floatFromInt(batches)),
    });

    for (fds) |fd| os_compat.close(fd);

    // Phase 2: one connection, many requests in sequence. A connection only
    // ever has one posted read, so this asks whether a long-lived connection
    // keeps working: every request gets its answer, and the slot stays usable
    // over hundreds of round trips.
    {
        const steady = connectAndAuthenticate("steady");

        const single = keyName(alloc, 0, 0);
        const expected = valueOf(alloc, 0, 0);

        for (0..pipelined_gets) |i| {
            sendGet(steady, single);
            const got = try readResponse(steady, &value_buf);
            const actual = got orelse fail("request {d} on a long-lived connection: not found", .{i});
            if (!std.mem.eql(u8, actual, expected)) {
                fail("request {d} on a long-lived connection: got '{s}' expected '{s}'", .{ i, actual, expected });
            }
        }

        os_compat.close(steady);
    }

    srv.requestShutdown();

    std.debug.print("e2e-pipelined: PASS {} connections x {} keys answered in batches, and {} requests on one long-lived connection\n", .{
        num_connections, keys_per_connection, pipelined_gets,
    });
}
