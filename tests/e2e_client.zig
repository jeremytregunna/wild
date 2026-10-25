//! Drives the real WILD client against a running `e2e-server`.
//!
//! It uses the production client (`src/client.zig`) over a real TCP socket and
//! checks one set, one get, and one delete. It exits nonzero on any mismatch,
//! so it can run in CI.
//!
//!     ./zig-out/bin/e2e-server 7878 testsecret &
//!     ./zig-out/bin/e2e-client 127.0.0.1:7878 testsecret
//!
//! The first message on a connection is the auth secret, which the server
//! expects and `wild-client` never sends. This harness sends it. That gap is
//! documented in docs/MIGRATION_0.17.md.

const std = @import("std");
const wildb_lib = @import("wildb_lib");

const client = wildb_lib.client;

pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(init.arena.allocator());

    if (args.len < 3) {
        std.debug.print("Usage: e2e-client <address:port> <auth-secret>\n", .{});
        return error.Usage;
    }

    const colon = std.mem.indexOfScalar(u8, args[1], ':') orelse {
        std.debug.print("e2e: address must be address:port\n", .{});
        return error.BadAddress;
    };
    const address = args[1][0..colon];
    const port = try std.fmt.parseInt(u16, args[1][colon + 1 ..], 10);
    const auth_secret = args[2];

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var wc = try client.WildClient.init(address, port);
    try wc.connect();
    defer wc.disconnect();

    try wc.authenticate(auth_secret);

    try wc.set("e2e-key", "e2e-value");
    std.debug.print("e2e: set e2e-key = e2e-value\n", .{});

    const got = try wc.get("e2e-key", allocator) orelse {
        std.debug.print("e2e: FAIL get returned null right after set\n", .{});
        return error.ValueMissing;
    };
    if (!std.mem.eql(u8, got, "e2e-value")) {
        std.debug.print("e2e: FAIL expected e2e-value, got {s}\n", .{got});
        return error.WrongValue;
    }
    std.debug.print("e2e: get e2e-key = {s}\n", .{got});

    if (!try wc.delete("e2e-key")) {
        std.debug.print("e2e: FAIL delete reported false\n", .{});
        return error.DeleteFailed;
    }
    std.debug.print("e2e: delete e2e-key\n", .{});

    if (try wc.get("e2e-key", allocator) != null) {
        std.debug.print("e2e: FAIL get still returns the value after delete\n", .{});
        return error.DeleteNotVisible;
    }
    std.debug.print("e2e: get e2e-key = null\n", .{});

    std.debug.print("e2e: PASS set, get, and delete over TCP\n", .{});
}
