const std = @import("std");
const compat = @import("src/os_compat.zig");

fn server(port: u16) !void {
    const fd = try compat.socket(compat.AF.INET, compat.SOCK.STREAM, 0);
    try compat.setsockoptReuse(fd);
    const addr = try compat.Address.parseIp("127.0.0.1", port);
    try compat.bind(fd, &addr.any, addr.getOsSockLen());
    try compat.listen(fd, 16);
    var a: compat.sockaddr = undefined;
    var l: compat.socklen_t = @sizeOf(compat.sockaddr);
    const cfd = try compat.accept(fd, &a, &l, 0);
    var buf: [32]u8 = undefined;
    const n = try compat.recv(cfd, &buf, 0);
    var out: [40]u8 = undefined;
    @memcpy(out[0..5], "echo:");
    @memcpy(out[5 .. 5 + n], buf[0..n]);
    try compat.sendAllCompat(cfd, out[0 .. 5 + n]);
    compat.close(cfd);
    compat.close(fd);
}

pub fn main() !void {
    const port = 17878;
    const th = try std.Thread.spawn(.{}, server, .{port});
    compat.sleep(200_000_000); // 200ms
    const fd = try compat.socket(compat.AF.INET, compat.SOCK.STREAM, 0);
    const addr = try compat.Address.parseIp("127.0.0.1", port);
    try compat.connect(fd, &addr.any, addr.getOsSockLen());
    _ = try compat.send(fd, "hello", 0);
    var buf: [32]u8 = undefined;
    const n = try compat.recv(fd, &buf, 0);
    std.debug.print("roundtrip: {s}\n", .{buf[0..n]});
    if (!std.mem.eql(u8, buf[0..n], "echo:hello")) return error.BadRoundtrip;
    // file round trip
    const path = "/tmp/wild_compat_probe.txt";
    var f = try compat.createFile(path, .{ .read = true, .truncate = true });
    try f.writeAll("snapshot-bytes");
    const size = try f.getEndPos();
    if (size != 14) return error.BadSize;
    try f.seekTo(0);
    var fbuf: [14]u8 = undefined;
    const rn = try f.readAll(&fbuf);
    if (rn != 14 or !std.mem.eql(u8, &fbuf, "snapshot-bytes")) return error.BadFile;
    f.close();
    std.debug.print("file roundtrip ok, size={d}\n", .{size});
    // dir iteration
    const dir = try compat.openDir("/tmp", .{ .iterate = true });
    _ = dir;
    std.debug.print("PASS: tcp + file paths verified\n", .{});
    th.join();
}
