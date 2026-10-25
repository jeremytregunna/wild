//! Hardware counters for the benchmark, read straight from the kernel.
//!
//! No external profiling tool is needed. Every counter is optional: if the
//! kernel refuses an event, or `perf_event_paranoid` blocks it, that counter
//! reports unavailable and the run continues. A benchmark that fails because
//! profiling is locked down is a broken benchmark.
//!
//! Counting excludes kernel time, which is what `perf_event_paranoid=2` allows.
//! For a CPU-bound loop over a hash table the difference is small, and the
//! labels in the output say so.
//!
//! Use it around the phase you want to measure:
//!
//!     var counters = bench_counters.Counters.init();
//!     defer counters.deinit();
//!     counters.start();
//!     ... phase ...
//!     counters.stop();
//!     counters.print("read");

const std = @import("std");
const linux = std.os.linux;
const os_compat = @import("src/os_compat.zig");

/// ioctl numbers from <linux/perf_event.h>. std does not export them.
const IOC_ENABLE: u32 = 0x2400;
const IOC_DISABLE: u32 = 0x2401;
const IOC_RESET: u32 = 0x2402;

pub const Counter = struct {
    name: []const u8,
    fd: ?std.posix.fd_t = null,
    start: u64 = 0,
    end: u64 = 0,

    /// False when the kernel refused the event.
    pub fn available(self: *const Counter) bool {
        return self.fd != null;
    }

    /// The count accumulated between start and stop.
    pub fn delta(self: *const Counter) u64 {
        if (self.end < self.start) return 0;
        return self.end - self.start;
    }

    fn open(name: []const u8, kind: linux.PERF.TYPE, config: u64) Counter {
        var counter = Counter{ .name = name };

        var attr = linux.perf_event_attr{
            .type = kind,
            .config = config,
            .flags = .{
                .disabled = true,
                // perf_event_paranoid=2 only permits user-space counting.
                .exclude_kernel = true,
                .exclude_hv = true,
            },
        };

        const res = linux.perf_event_open(&attr, 0, -1, -1, 0);
        switch (linux.errno(res)) {
            .SUCCESS => counter.fd = @intCast(res),
            else => {}, // leave unavailable; the caller reports it
        }
        return counter;
    }

    fn ioctl(self: *Counter, request: u32) void {
        const fd = self.fd orelse return;
        _ = linux.ioctl(fd, request, 0);
    }

    fn readValue(self: *Counter) ?u64 {
        const fd = self.fd orelse return null;
        var buf: [8]u8 = undefined;
        const n = std.posix.read(fd, &buf) catch return null;
        if (n != buf.len) return null;
        return std.mem.readInt(u64, &buf, .little);
    }
};

pub const Counters = struct {
    cycles: Counter,
    instructions: Counter,
    cache_misses: Counter,
    llc_read_misses: Counter,

    /// Hardware cache config encoding: cache | (op << 8) | (result << 16).
    const llc_read_miss_config: u64 = @as(u64, @intFromEnum(linux.PERF.COUNT.HW.CACHE.LL)) |
        (@as(u64, @intFromEnum(linux.PERF.COUNT.HW.CACHE.OP.READ)) << 8) |
        (@as(u64, @intFromEnum(linux.PERF.COUNT.HW.CACHE.RESULT.MISS)) << 16);

    pub fn init() Counters {
        return .{
            .cycles = Counter.open("cycles", .HARDWARE, @intFromEnum(linux.PERF.COUNT.HW.CPU_CYCLES)),
            .instructions = Counter.open("instructions", .HARDWARE, @intFromEnum(linux.PERF.COUNT.HW.INSTRUCTIONS)),
            // The generic cache-miss event. On AMD it counts fewer levels than
            // the LLC counter, so it is a proxy, and it is reported separately.
            .cache_misses = Counter.open("cache misses", .HARDWARE, @intFromEnum(linux.PERF.COUNT.HW.CACHE_MISSES)),
            .llc_read_misses = Counter.open("LLC read misses", .HW_CACHE, llc_read_miss_config),
        };
    }

    pub fn deinit(self: *Counters) void {
        for (self.allMutable()) |c| {
            if (c.fd) |fd| os_compat.close(fd);
            c.fd = null;
        }
    }

    /// Reset and enable every counter that the kernel allowed.
    pub fn start(self: *Counters) void {
        for (self.allMutable()) |c| {
            if (c.fd == null) continue;
            c.ioctl(IOC_RESET);
            c.ioctl(IOC_ENABLE);
        }
    }

    /// Disable every counter and capture its value.
    pub fn stop(self: *Counters) void {
        for (self.allMutable()) |c| {
            if (c.fd == null) continue;
            c.ioctl(IOC_DISABLE);
            if (c.readValue()) |v| {
                c.start = 0; // values were reset at start, so the read is the delta
                c.end = v;
            }
        }
    }

    /// Instructions per cycle, or null when the cycle counter is unavailable.
    pub fn ipc(self: *const Counters) ?f64 {
        const cycles = self.cycles.delta();
        if (!self.cycles.available() or cycles == 0) return null;
        return @as(f64, @floatFromInt(self.instructions.delta())) / @as(f64, @floatFromInt(cycles));
    }

    /// Misses per thousand operations, from whichever miss counter the kernel
    /// allowed. Null when neither is available.
    pub fn missesPerKiloOps(self: *const Counters, ops: u64) ?f64 {
        const count = self.misses() orelse return null;
        if (ops == 0) return null;
        return @as(f64, @floatFromInt(count)) * 1000.0 / @as(f64, @floatFromInt(ops));
    }

    /// The best miss count available: the LLC counter if the kernel maps it,
    /// otherwise the generic cache-miss proxy. Null when neither is available.
    pub fn misses(self: *const Counters) ?u64 {
        if (self.llc_read_misses.available()) return self.llc_read_misses.delta();
        if (self.cache_misses.available()) return self.cache_misses.delta();
        return null;
    }

    /// Which counter `misses` used, for the label in the output.
    pub fn missSource(self: *const Counters) ?[]const u8 {
        if (self.llc_read_misses.available()) return "LLC read misses";
        if (self.cache_misses.available()) return "cache misses (proxy)";
        return null;
    }

    /// Print one line per counter plus IPC, under the given phase label.
    pub fn print(self: *const Counters, phase: []const u8) void {
        std.debug.print("  {s} counters:", .{phase});

        for (self.all()) |c| {
            if (c.available()) {
                std.debug.print(" {s}={d}", .{ c.name, c.delta() });
            } else {
                std.debug.print(" {s}=unavailable", .{c.name});
            }
        }

        if (self.ipc()) |value| {
            std.debug.print(" IPC={d:.2}", .{value});
        } else {
            std.debug.print(" IPC=unavailable", .{});
        }
        std.debug.print("\n", .{});
    }

    fn all(self: *const Counters) [4]*const Counter {
        return .{ &self.cycles, &self.instructions, &self.cache_misses, &self.llc_read_misses };
    }

    fn allMutable(self: *Counters) [4]*Counter {
        return .{ &self.cycles, &self.instructions, &self.cache_misses, &self.llc_read_misses };
    }
};
