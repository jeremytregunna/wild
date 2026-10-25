const std = @import("std");
const compat = @import("src/os_compat.zig");
pub fn main() !void {
    var dir = try compat.openDir("/tmp/wild_compat_dir", .{ .iterate = true });
    defer dir.close();
    var it = dir.iterate();
    var found: u32 = 0;
    var files: u32 = 0;
    while (try it.next()) |entry| {
        if (std.mem.eql(u8, entry.name, ".")) continue;
        if (std.mem.eql(u8, entry.name, "..")) continue;
        found += 1;
        if (entry.kind == .file) files += 1;
        std.debug.print("entry: {s} kind={s}\n", .{ entry.name, @tagName(entry.kind) });
    }
    if (found != 2 or files != 2) return error.BadDir;
    std.debug.print("PASS: dir iteration found {d} files\n", .{files});
}
