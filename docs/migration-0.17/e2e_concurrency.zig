//! Concurrency regression test for the io_uring accept loop.
//!
//! Connections used to be stored in arrays that were compacted on disconnect:
//! the last client was copied into the freed slot. The read of that moved
//! client was already posted in the ring, tagged with its old index, so its
//! next completion pointed past the end of the array and was dropped. The
//! visible symptom is a connection that never gets an answer: a peer of the
//! client that just hung up goes quiet forever.
//!
//! Slots are stable now, and the completion tag carries a generation. This test
//! reproduces the exact sequence that used to break it: several authenticated
//! connections sitting idle with a read posted, then one of them disconnects,
//! then every remaining connection has to get its own answer.
//!
//! Run with: zig build && ./zig-out/bin/e2e-concurrency

const std = @import("std");
const wildb_lib = @import("wildb_lib");

const wild = wildb_lib.wild;
const server_mod = wildb_lib.server;
const client_mod = wildb_lib.client;
const static_allocator = wildb_lib.static_allocator;
const os_compat = wildb_lib.os_compat;

const secret = "concurrency-secret";
const survivors = 6;
const port: u16 = 7911;
const watchdog_ms = 20_000;

fn fail(comptime format: []const u8, args: anytype) noreturn {
    std.debug.print("e2e-concurrency: FAIL " ++ format ++ "\n", args);
    std.process.exit(1);
}

fn serve(srv: *server_mod.WildServer) void {
    srv.start() catch |err| {
        std.debug.print("e2e-concurrency: server stopped: {t}\n", .{err});
    };
}

/// If the accept loop has dropped a completion, a client blocks forever. A
/// watchdog turns that hang into a failure with a reason.
fn watchdog() void {
    var waited: u64 = 0;
    while (waited < watchdog_ms) : (waited += 100) {
        os_compat.sleep(100 * std.time.ns_per_ms);
    }
    std.debug.print("e2e-concurrency: FAIL no answer from the server within {}ms\n", .{watchdog_ms});
    std.debug.print("  A dropped io_uring completion is the usual cause: the loop\n", .{});
    std.debug.print("  stopped posting a read for one connection, so it is quiet forever.\n", .{});
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

    // The first message on a connection is the shared secret, and the server
    // answers with one byte: 0 for accepted.
    const sent = os_compat.send(fd, secret, 0) catch |err| {
        fail("{s}: send secret: {t}", .{ who, err });
    };
    if (sent != secret.len) fail("{s}: short secret write", .{who});

    var ack: [1]u8 = undefined;
    const n = os_compat.recv(fd, &ack, 0) catch |err| {
        fail("{s}: auth reply: {t}", .{ who, err });
    };
    if (n != 1 or ack[0] != 0) fail("{s}: auth refused", .{who});

    return fd;
}

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var database_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer database_arena.deinit();
    var static_alloc = static_allocator.StaticAllocator.init(database_arena.allocator());

    var database = try wild.WILD.init(static_alloc.allocator(), .{ .target_capacity = 100_000 });

    var srv = try server_mod.WildServer.init(&database, &static_alloc, "127.0.0.1", port, secret);
    static_alloc.transitionToStatic();

    const thread = try std.Thread.spawn(.{}, serve, .{&srv});
    defer thread.join();

    _ = std.Thread.spawn(.{}, watchdog, .{}) catch {};

    // Wait for the listener.
    var attempt: usize = 0;
    while (true) : (attempt += 1) {
        const probe = os_compat.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0) catch |err| {
            fail("socket for probe: {t}", .{err});
        };
        var addr = try os_compat.Address.parseIp("127.0.0.1", port);
        if (os_compat.connect(probe, &addr.any, addr.getOsSockLen())) {
            os_compat.close(probe);
            break;
        } else |_| {
            os_compat.close(probe);
            if (attempt > 400) fail("server never started listening on {}", .{port});
            os_compat.sleep(5 * std.time.ns_per_ms);
        }
    }

    // The sacrificial connection takes the lowest slot, so it is not the last
    // client. That is the arrangement that used to corrupt the last client's
    // in-flight read when this one went away.
    const sacrificial = connectAndAuthenticate("sacrificial");

    var clients: [survivors]client_mod.WildClient = undefined;
    for (&clients, 0..) |*client, i| {
        client.* = try client_mod.WildClient.init("127.0.0.1", port);
        try client.connect();
        client.authenticate(secret) catch |err| fail("client {d}: authenticate: {t}", .{ i, err });

        const key = try std.fmt.allocPrint(alloc, "ck{}", .{i});
        const value = try std.fmt.allocPrint(alloc, "value-from-{d}", .{i});
        client.set(key, value) catch |err| fail("client {d}: set: {t}", .{ i, err });
    }

    // Every connection now sits idle with a read posted in the ring. Remove one
    // in the middle, twice: the raw connection at the low slot, and one of the
    // clients. Under the old compaction both of these orphaned another
    // connection's pending read.
    os_compat.close(sacrificial);
    os_compat.close(clients[1].socket_fd);

    // Every connection that survived must still answer, with its own value.
    for (&clients, 0..) |*client, i| {
        if (i == 1) continue; // closed above

        // No poll here on purpose: WildClient.get sends the request and then
        // waits, so polling first would wait for a response that has not been
        // asked for. The watchdog is what turns a stuck connection into a
        // failure with a reason.
        const key = try std.fmt.allocPrint(alloc, "ck{}", .{i});
        const got = try client.get(key, alloc);
        const expected = try std.fmt.allocPrint(alloc, "value-from-{d}", .{i});
        const actual = got orelse fail("client {d}: get returned null", .{i});
        if (!std.mem.eql(u8, actual, expected)) {
            fail("client {d}: got '{s}' expected '{s}' — responses crossed connections", .{ i, actual, expected });
        }
    }

    // A new connection lands on a freed slot. It must not inherit anything from
    // the connection that used to own it, and it must see committed data.
    const reused = connectAndAuthenticate("reused");
    var fresh = try client_mod.WildClient.init("127.0.0.1", port);
    try fresh.connect();
    fresh.authenticate(secret) catch |err| fail("reused client: authenticate: {t}", .{err});

    const seen = try fresh.get("ck0", alloc);
    const want = try std.fmt.allocPrint(alloc, "value-from-{d}", .{0});
    if (seen == null or !std.mem.eql(u8, seen.?, want)) {
        fail("reused connection read '{s}' expected '{s}'", .{ seen orelse "<null>", want });
    }

    // A write on the reused slot must work too, so the slot is fully live.
    fresh.set("fresh-key", "fresh-value") catch |err| fail("reused client: set: {t}", .{err});
    const back = try fresh.get("fresh-key", alloc);
    if (back == null or !std.mem.eql(u8, back.?, "fresh-value")) {
        fail("reused connection could not read back its own write", .{});
    }

    os_compat.close(reused);

    srv.requestShutdown();

    std.debug.print("e2e-concurrency: PASS {} connections kept their own responses across two disconnects,\n", .{survivors - 1});
    std.debug.print("  and a freed slot was reused without inheriting stale state\n", .{});
}
