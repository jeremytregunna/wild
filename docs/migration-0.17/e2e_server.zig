//! Starts the real WILD server for end-to-end testing.
//!
//! This is the production server (`src/server.zig`, io_uring accept loop and
//! all) with the production database. It exists because the daemon
//! (`src/daemon.zig`) never starts the client-facing server, so there is
//! otherwise no supported way to exercise the TCP path.
//!
//! Build it with `zig build`, then run the pair:
//!
//!     ./zig-out/bin/e2e-server 7878 testsecret
//!     ./zig-out/bin/e2e-client 127.0.0.1:7878 testsecret
//!
//! The server blocks until the process is killed.

const std = @import("std");
const wildb_lib = @import("wildb_lib");

const wild = wildb_lib.wild;
const server = wildb_lib.server;
const static_allocator = wildb_lib.static_allocator;

pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(init.arena.allocator());

    if (args.len < 3) {
        std.debug.print("Usage: e2e-server <port> <auth-secret> [capacity]\n", .{});
        return error.Usage;
    }

    const port = try std.fmt.parseInt(u16, args[1], 10);
    const auth_secret = args[2];
    const capacity = if (args.len > 3) try std.fmt.parseInt(usize, args[3], 10) else 100_000;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var database_arena = std.heap.ArenaAllocator.init(arena.allocator());
    var static_alloc = static_allocator.StaticAllocator.init(database_arena.allocator());

    var database = try wild.WILD.init(static_alloc.allocator(), .{
        .target_capacity = capacity,
    });

    // WildServer.allocates from the static allocator, so init must run before
    // the transition. start() then blocks and serves forever.
    var wild_server = try server.WildServer.init(&database, &static_alloc, "127.0.0.1", port, auth_secret);
    static_alloc.transitionToStatic();

    std.debug.print("e2e-server: database capacity {} records\n", .{capacity});
    try wild_server.start();
}
