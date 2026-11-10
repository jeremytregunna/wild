//! Compatibility layer for the POSIX calls that std removed in Zig 0.16/0.17.
//!
//! Zig 0.16 moved sockets and files behind the `std.Io` interface. WILD calls
//! those syscalls directly on purpose: the server uses io_uring, and the WAL
//! uses O_DIRECT. So this file keeps the raw syscall path. It wraps
//! `std.os.linux`. It adds no allocation and no extra thread.
//!
//! Linux x86-64 only, like the rest of WILD.

const std = @import("std");
const os_compat = @import("os_compat.zig");
const linux = std.os.linux;
const posix = std.posix;

// Raw `linux.openat` and friends take a NUL-terminated path. A runtime-built
// path — `std.fmt.allocPrint`, a WAL file number, a CLI argument — is an
// ordinary slice, so passing `path.ptr` reads past its end. One thread-local
// buffer is enough: these calls do not yield, and each thread has its own.
threadlocal var path_buffer: [std.Io.Dir.max_path_bytes]u8 = undefined;

fn toPathZ(path: []const u8) ?[*:0]const u8 {
    if (path.len >= path_buffer.len) return null;
    @memcpy(path_buffer[0..path.len], path);
    path_buffer[path.len] = 0;
    return @ptrCast(&path_buffer);
}

pub const fd_t = posix.fd_t;
pub const sockaddr = posix.sockaddr;
pub const socklen_t = posix.socklen_t;
pub const AF = posix.AF;
pub const SOCK = posix.SOCK;
pub const SOL = posix.SOL;
pub const SO = posix.SO;
pub const MSG = posix.MSG;
pub const F = posix.F;
pub const AT = posix.AT;

/// Errors that the removed `std.posix` socket functions used to return.
pub const SocketError = error{
    WouldBlock,
    NotSocket,
    PermissionDenied,
    AddressInUse,
    AddressUnavailable,
    NetworkUnreachable,
    ConnectionResetByPeer,
    ConnectionRefused,
    ConnectionAborted,
    BrokenPipe,
    AlreadyConnected,
    FileDescriptorInvalid,
    ProcessFileDescriptorLimitReached,
    SystemResources,
    IsDir,
    FileNotFound,
    NameTooLong,
    NoSpaceLeft,
    InputOutput,
    Unexpected,
};

/// Maps a raw syscall return value to an error, or returns the value.
fn check(rc: usize) SocketError!usize {
    switch (posix.errno(rc)) {
        .SUCCESS => return rc,
        .INTR => return error.WouldBlock,
        .AGAIN => return error.WouldBlock,
        .BADF => return error.FileDescriptorInvalid,
        .INVAL => return error.Unexpected,
        .FAULT => return error.Unexpected,
        .NOMEM => return error.SystemResources,
        .PERM => return error.PermissionDenied,
        .ACCES => return error.PermissionDenied,
        .ADDRINUSE => return error.AddressInUse,
        .ADDRNOTAVAIL => return error.AddressUnavailable,
        .ALREADY => return error.AlreadyConnected,
        .ISCONN => return error.AlreadyConnected,
        .CONNABORTED => return error.ConnectionAborted,
        .CONNREFUSED => return error.ConnectionRefused,
        .CONNRESET => return error.ConnectionResetByPeer,
        .NOTSOCK => return error.NotSocket,
        .PIPE => return error.BrokenPipe,
        .NOBUFS => return error.SystemResources,
        .NFILE => return error.ProcessFileDescriptorLimitReached,
        .MFILE => return error.ProcessFileDescriptorLimitReached,
        .ISDIR => return error.IsDir,
        .NAMETOOLONG => return error.NameTooLong,
        .NOENT => return error.FileNotFound,
        .NOSPC => return error.NoSpaceLeft,
        .IO => return error.InputOutput,
        else => return error.Unexpected,
    }
}

/// `std.posix.sockaddr_in`, which no longer exists. `std.posix.sockaddr` lost
/// its `.inet` union, so the IPv4 fields go into `.data` as raw bytes.
const SockaddrIn = extern struct {
    family: posix.sa_family_t,
    port: u16,
    addr: u32,
    padding: [8]u8,
};

comptime {
    std.debug.assert(@sizeOf(SockaddrIn) == 16);
}

pub const AddrInfoError = error{
    InvalidCharacter,
    NameNotFound,
};

/// Replacement for the removed `std.net.Address`, covering the IPv4 TCP path
/// that WILD uses.
pub const Address = struct {
    any: sockaddr = std.mem.zeroes(sockaddr),

    pub fn getOsSockLen(self: *const Address) socklen_t {
        _ = self;
        return @sizeOf(SockaddrIn);
    }

    /// Parse a dotted-quad IPv4 literal plus a port. Every WILD call site uses
    /// an IPv4 literal ("0.0.0.0", a primary address, an endpoint address).
    pub fn parseIp(text: []const u8, port: u16) AddrInfoError!Address {
        var self = Address{};
        const sin = SockaddrIn{
            .family = posix.AF.INET,
            .port = std.mem.bigToNative(u16, port),
            .addr = try parseIpv4(text),
            .padding = std.mem.zeroes([8]u8),
        };
        self.any.family = posix.AF.INET;
        // sockaddr.data starts where SockaddrIn.port starts, so skip the family.
        @memcpy(self.any.data[0..14], std.mem.asBytes(&sin)[@sizeOf(posix.sa_family_t)..]);
        return self;
    }
};

/// Parse "1.2.3.4" into a network-byte-order IPv4 address.
fn parseIpv4(text: []const u8) AddrInfoError!u32 {
    var octets: [4]u8 = undefined;
    var count: usize = 0;
    var rest: []const u8 = text;
    while (true) {
        if (count == 4) return error.InvalidCharacter;
        const dot = std.mem.indexOfScalar(u8, rest, '.');
        const part = if (dot) |d| rest[0..d] else rest;
        if (part.len == 0 or part.len > 3) return error.InvalidCharacter;
        octets[count] = std.fmt.parseInt(u8, part, 10) catch return error.InvalidCharacter;
        count += 1;
        if (dot) |d| {
            rest = rest[d + 1 ..];
        } else break;
    }
    if (count != 4) return error.InvalidCharacter;
    const host_order: u32 = (@as(u32, octets[0]) << 24) |
        (@as(u32, octets[1]) << 16) |
        (@as(u32, octets[2]) << 8) |
        @as(u32, octets[3]);
    return std.mem.bigToNative(u32, host_order);
}

pub fn socket(domain: u32, socket_type: u32, protocol: u32) SocketError!fd_t {
    const rc = try check(linux.socket(domain, socket_type, protocol));
    return @intCast(rc);
}

pub fn bind(fd: fd_t, addr: *const sockaddr, addrlen: socklen_t) SocketError!void {
    _ = try check(linux.bind(fd, addr, addrlen));
}

pub fn listen(fd: fd_t, backlog: u32) SocketError!void {
    _ = try check(linux.listen(fd, backlog));
}

pub fn connect(fd: fd_t, addr: *const sockaddr, addrlen: socklen_t) SocketError!void {
    _ = try check(linux.connect(fd, addr, addrlen));
}

/// `std.posix.send` was removed. `send(2)` with no address equals `sendto(2)`.
pub fn send(fd: fd_t, buf: []const u8, flags: u32) SocketError!usize {
    if (buf.len == 0) return 0;
    return check(linux.sendto(fd, buf.ptr, buf.len, flags, null, 0));
}

/// `std.posix.recv` was removed. `recv(2)` with no address equals `recvfrom(2)`.
pub fn recv(fd: fd_t, buf: []u8, flags: u32) SocketError!usize {
    if (buf.len == 0) return 0;
    var addr_len: socklen_t = 0;
    return check(linux.recvfrom(fd, buf.ptr, buf.len, flags, null, &addr_len));
}

/// `std.posix.accept` took `SOCK.NONBLOCK` in the flags argument. `accept4(2)`
/// uses the same bits, so the value passes straight through.
pub fn accept(fd: fd_t, addr: ?*sockaddr, addr_len: ?*socklen_t, flags: u32) SocketError!fd_t {
    const rc = try check(linux.accept4(fd, addr, addr_len, flags));
    return @intCast(rc);
}

pub fn setsockoptReuse(fd: fd_t) SocketError!void {
    posix.setsockopt(fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1))) catch
        return error.Unexpected;
}

/// Send every byte, retrying on partial writes.
pub fn sendAllCompat(fd: fd_t, buf: []const u8) SocketError!void {
    var total: usize = 0;
    while (total < buf.len) {
        total += try send(fd, buf[total..], 0);
    }
}

pub fn close(fd: fd_t) void {
    _ = linux.close(fd);
}

pub fn write(fd: fd_t, buf: []const u8) SocketError!usize {
    if (buf.len == 0) return 0;
    return check(linux.write(fd, buf.ptr, buf.len));
}

pub fn read(fd: fd_t, buf: []u8) SocketError!usize {
    if (buf.len == 0) return 0;
    return check(linux.read(fd, buf.ptr, buf.len));
}

pub fn fsync(fd: fd_t) SocketError!void {
    _ = try check(linux.fsync(fd));
}

pub fn fdatasync(fd: fd_t) SocketError!void {
    _ = try check(linux.fdatasync(fd));
}

pub fn fcntl(fd: fd_t, cmd: u32, arg: usize) SocketError!usize {
    return check(linux.fcntl(fd, @intCast(cmd), arg));
}

pub fn unlink(path: []const u8) SocketError!void {
    const c_path = toPathZ(path) orelse return error.NameTooLong;
    _ = try check(linux.unlink(c_path));
}

// ---------------------------------------------------------------------------
// Time and sleep
//
// Zig 0.16 removed `std.time.nanoTimestamp`, `std.time.milliTimestamp`,
// `std.time.timestamp`, and `std.Thread.sleep`. Their replacements are
// `std.Io.Clock.now(io)` and `io.sleep(...)`, which need an `std.Io` instance.
// WILD calls these on the write path and in spin-wait backoff loops, so these
// wrappers keep the direct `clock_gettime`/`nanosleep` syscalls instead.
// ---------------------------------------------------------------------------

/// Same units and sign as the removed `std.time.nanoTimestamp`.
pub fn nanoTimestamp() i128 {
    const ts = now(linux.clockid_t.MONOTONIC);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

/// Same units and sign as the removed `std.time.milliTimestamp`.
pub fn milliTimestamp() i64 {
    const ts = now(linux.clockid_t.MONOTONIC);
    return @as(i64, ts.sec) * std.time.ms_per_s + @divTrunc(@as(i64, ts.nsec), std.time.ns_per_ms);
}

/// Same units and sign as the removed `std.time.timestamp` (wall clock seconds).
pub fn timestamp() i64 {
    return @as(i64, now(linux.clockid_t.REALTIME).sec);
}

fn now(clock: linux.clockid_t) posix.timespec {
    var ts: posix.timespec = undefined;
    const rc = linux.clock_gettime(clock, &ts);
    switch (posix.errno(rc)) {
        .SUCCESS => return ts,
        // A clock reading failure is a kernel bug on the supported target.
        else => @panic("clock_gettime failed"),
    }
}

/// Replacement for the removed `std.Thread.sleep`. Takes nanoseconds, like the
/// removed function.
pub fn sleep(nanoseconds: u64) void {
    var req = posix.timespec{
        .sec = @intCast(nanoseconds / std.time.ns_per_s),
        .nsec = @intCast(nanoseconds % std.time.ns_per_s),
    };
    while (true) {
        var rem: posix.timespec = undefined;
        const rc = linux.nanosleep(&req, &rem);
        switch (posix.errno(rc)) {
            .SUCCESS => return,
            .INTR => req = rem, // interrupted, sleep the remainder
            else => return,
        }
    }
}

// ---------------------------------------------------------------------------
// Mutex
//
// `os_compat.Mutex` was removed; `std.Io.Mutex` is the replacement, but
// `lock`/`unlock` take an `std.Io`. The WAL keeps locks in structs that have no
// `std.Io`, so this mirrors `std.Io.Mutex` with the same futex calls and the
// same no-argument signatures.
// ---------------------------------------------------------------------------

pub const Mutex = struct {
    state: std.atomic.Value(State) = .init(.unlocked),

    pub const State = enum(u32) {
        unlocked,
        locked_once,
        contended,
    };

    pub fn tryLock(m: *Mutex) bool {
        return m.state.cmpxchgStrong(.unlocked, .locked_once, .acquire, .monotonic) == null;
    }

    pub fn lock(m: *Mutex) void {
        if (m.state.cmpxchgStrong(.unlocked, .locked_once, .acquire, .monotonic) == null) {
            @branchHint(.likely);
            return;
        }
        while (true) {
            if (m.state.swap(.contended, .acquire) == .unlocked) return;
            m.futexWait(.contended);
        }
    }

    pub fn unlock(m: *Mutex) void {
        switch (m.state.swap(.unlocked, .release)) {
            .unlocked => unreachable,
            .locked_once => {},
            .contended => m.futexWake(1),
        }
    }

    fn futexWait(m: *Mutex, expected: State) void {
        const ptr: *align(4) const u32 = @ptrCast(&m.state.raw);
        _ = linux.futex_3arg(ptr, .{ .cmd = .WAIT, .private = true }, @backingInt(expected));
    }

    fn futexWake(m: *Mutex, max_waiters: u32) void {
        if (max_waiters == 0) return;
        const ptr: *align(4) const u32 = @ptrCast(&m.state.raw);
        _ = linux.futex_3arg(
            ptr,
            .{ .cmd = .WAKE, .private = true },
            @min(max_waiters, std.math.maxInt(i32)),
        );
    }
};

// ---------------------------------------------------------------------------
// CPU affinity
//
// Topology detection asks every CPU what it is, and a CPU only answers about
// itself, so the asking thread has to run there first. That is what pins are
// for. These wrappers are the whole OS surface of `cache_topology.zig`: it is
// called once at init, and the original mask is restored before it returns.
//
// `cpu_set_t` is a fixed bitmap of `CPU_SETSIZE / @sizeOf(usize) * 8` bits, so
// the kernel call takes a byte count, not a CPU count.
// ---------------------------------------------------------------------------

/// The kernel's CPU bitmap, large enough for 1024 CPUs.
pub const CpuSet = linux.cpu_set_t;

/// CPUs one `CpuSet` can describe.
pub const max_cpus: u32 = @as(u32, @sizeOf(CpuSet)) * 8;

/// Fill `set` with the CPUs this process is allowed to run on.
///
/// Returns false when the kernel will not answer, which is what happens off
/// Linux. The caller then reports no topology rather than guessing one.
pub fn cpuAffinityGet(set: *CpuSet) bool {
    @memset(std.mem.asBytes(set), 0);
    const rc = linux.sched_getaffinity(0, @sizeOf(CpuSet), set);
    return posix.errno(rc) == .SUCCESS;
}

/// Restrict the calling thread to `set`. Returns false if the kernel refuses.
pub fn cpuAffinitySet(set: *const CpuSet) bool {
    linux.sched_setaffinity(0, set) catch return false;
    return true;
}

pub fn cpuSetContains(set: *const CpuSet, cpu: u32) bool {
    if (cpu >= max_cpus) return false;
    const word = cpu / @bitSizeOf(usize);
    const bit = cpu % @bitSizeOf(usize);
    return (set[word] & (@as(usize, 1) << @intCast(bit))) != 0;
}

pub fn cpuSetPut(set: *CpuSet, cpu: u32) void {
    if (cpu >= max_cpus) return;
    const word = cpu / @bitSizeOf(usize);
    const bit = cpu % @bitSizeOf(usize);
    set[word] |= @as(usize, 1) << @intCast(bit);
}

/// The kernel's NUMA node for the CPU the calling thread is on right now, or
/// null when the kernel will not answer.
///
/// This is the only trustworthy translation from a hardware zone id to the
/// number a placement call takes. The CPUID leaf `0x8000001E` reports the
/// hardware's own numbering, and the kernel's numbering is free to differ: NPS
/// modes split a socket differently, and a cpuset can hide nodes. `getcpu`
/// asks the kernel, so its answer is the number `mbind` and `numactl` use.
///
/// A CPU answers only about itself, so the caller must be pinned to the CPU it
/// wants to ask about. `cache_topology.zig` already is, one CPU at a time.
pub fn nodeOfThisCpu() ?u32 {
    var node: usize = 0;
    const rc = linux.getcpu(null, &node);
    if (posix.errno(rc) != .SUCCESS) return null;
    if (node > std.math.maxInt(u32)) return null;
    return @intCast(node);
}

// ---------------------------------------------------------------------------
// Memory policy
//
// A table lives on one zone, and its pages must come from that zone. Two
// mechanisms say so, and the partition owner uses both, in this order: bind
// the range before it is touched, then touch it while pinned. The bind decides
// where untouched pages will be allocated; the touch is what actually
// allocates them. Doing one without the other leaves the answer to the
// scheduler, which is what the table did before.
//
// Node numbers here are the kernel's, never the ones CPUID reports. The
// kernel is free to number nodes differently, and `mbind` with the hardware's
// number puts a table on the wrong side of the boundary it was meant to stay
// inside.
// ---------------------------------------------------------------------------

/// `MPOL_BIND`: allocate only on the nodes in the mask, and fall back nowhere.
const MPOL_BIND: u64 = 2;

/// `MPOL_F_NODE | MPOL_F_ADDR`: ask `get_mempolicy` which node a page is on.
const MPOL_F_NODE: u64 = 1;
const MPOL_F_ADDR: u64 = 2;

/// Node ids the kernel may hand out. `MAX_NUMNODES` is 1024 on x86-64, and a
/// mask has to describe all of them, not just the one in use.
pub const max_nodes: u32 = 1024;

/// One bit per node, as `mbind` wants it.
const NodeMask = [max_nodes / @bitSizeOf(usize)]usize;

/// Bind an already-mapped range to one node, so pages allocated from it from
/// now on come from that node and nowhere else.
///
/// The range must start on a page boundary and its length must be a whole
/// number of pages; `alignedAlloc` with `pageSize()` gives both when the length
/// is rounded up. Returns false when the kernel refuses, which it does on a
/// machine without NUMA or when a policy manager forbids it; the caller then
/// reports the table as unplaced instead of pretending.
///
/// `MPOL_MF_MOVE` is deliberately not passed: moving pages that are already
/// resident would copy a table that is about to be touched anyway, and on an
/// untouched range there is nothing to move.
pub fn bindRangeToNode(ptr: [*]u8, len: usize, node: u32) bool {
    if (node >= max_nodes) return false;
    if (len == 0) return false;

    var mask: NodeMask = std.mem.zeroes(NodeMask);
    mask[node / @bitSizeOf(usize)] |= @as(usize, 1) << @intCast(node % @bitSizeOf(usize));

    const rc = linux.syscall6(
        .mbind,
        @intFromPtr(ptr),
        len,
        MPOL_BIND,
        @intFromPtr(&mask),
        max_nodes,
        0,
    );
    return posix.errno(rc) == .SUCCESS;
}

/// The node a resident page is on, or null when the kernel will not say.
///
/// This is the check that binding and pinning worked, rather than the claim
/// that they were asked for. A page that is not resident yet has no answer.
pub fn nodeOfPage(ptr: [*]const u8) ?u32 {
    var node: u64 = 0;
    const rc = linux.syscall6(
        .get_mempolicy,
        @intFromPtr(&node),
        0,
        0,
        @intFromPtr(ptr),
        MPOL_F_NODE | MPOL_F_ADDR,
        0,
    );
    if (posix.errno(rc) != .SUCCESS) return null;
    if (node > std.math.maxInt(u32)) return null;
    return @intCast(node);
}

/// Wait until `*word` stops being `expected`, or return when it already is
/// not. Wakes only when a writer has changed the word, so a lost wakeup
/// cannot park this thread forever.
pub fn futexWait(word: *const std.atomic.Value(u32), expected: u32) void {
    const ptr: *align(4) const u32 = @ptrCast(&word.raw);
    _ = linux.futex_3arg(ptr, .{ .cmd = .WAIT, .private = true }, expected);
}

/// Wake up to `count` threads waiting on `word`.
pub fn futexWake(word: *const std.atomic.Value(u32), count: u32) void {
    if (count == 0) return;
    const ptr: *align(4) const u32 = @ptrCast(&word.raw);
    _ = linux.futex_3arg(
        ptr,
        .{ .cmd = .WAKE, .private = true },
        @min(count, std.math.maxInt(i32)),
    );
}

// ---------------------------------------------------------------------------
// Files and directories
//
// Zig 0.16 removed `os_compat.File`, `std.fs.Dir`, and `std.fs.cwd()`. The
// replacements are `std.Io.File` and `std.Io.Dir`, and every method takes an
// `std.Io`. Snapshots and WAL recovery have no `std.Io` to pass, so these
// wrappers keep the old signatures over direct syscalls.
//
// Paths are ordinary slices, not NUL-terminated pointers. The syscall helpers
// copy them through `toPathZ` first.
// ---------------------------------------------------------------------------

pub const FileError = error{
    FileNotFound,
    AccessDenied,
    NameTooLong,
    NotDir,
    IsDir,
    DirNotEmpty,
    PathAlreadyExists,
    NoSpaceLeft,
    InputOutput,
    Unexpected,
};

fn fileCheck(rc: usize) FileError!usize {
    switch (posix.errno(rc)) {
        .SUCCESS => return rc,
        .NOENT => return error.FileNotFound,
        .ACCES => return error.AccessDenied,
        .PERM => return error.AccessDenied,
        .NAMETOOLONG => return error.NameTooLong,
        .NOTDIR => return error.NotDir,
        .ISDIR => return error.IsDir,
        .NOTEMPTY => return error.DirNotEmpty,
        .EXIST => return error.PathAlreadyExists,
        .NOSPC => return error.NoSpaceLeft,
        .IO => return error.InputOutput,
        .BADF => return error.FileNotFound,
        .INTR => return error.InputOutput,
        else => return error.Unexpected,
    }
}

pub const Mode = enum { read_only, write_only, read_write };

pub const FileOpenOptions = struct {
    /// For `openFile`.
    mode: Mode = .read_write,
    /// For `createFile`: open for reading as well as writing.
    read: bool = false,
    /// For `createFile`: truncate an existing file to zero length.
    truncate: bool = true,
};

pub const File = struct {
    handle: posix.fd_t,

    pub fn read(self: File, buffer: []u8) FileError!usize {
        if (buffer.len == 0) return 0;
        return fileCheck(linux.read(self.handle, buffer.ptr, buffer.len));
    }

    /// Same contract as the removed `os_compat.File.readAll`: returns bytes read,
    /// fewer than buffer.len only at end of file.
    pub fn readAll(self: File, buffer: []u8) FileError!usize {
        var total: usize = 0;
        while (total < buffer.len) {
            const n = try self.read(buffer[total..]);
            if (n == 0) break; // end of file
            total += n;
        }
        return total;
    }

    pub fn write(self: File, buffer: []const u8) FileError!usize {
        if (buffer.len == 0) return 0;
        return fileCheck(linux.write(self.handle, buffer.ptr, buffer.len));
    }

    pub fn writeAll(self: File, buffer: []const u8) FileError!void {
        var total: usize = 0;
        while (total < buffer.len) {
            total += try self.write(buffer[total..]);
        }
    }

    pub fn seekTo(self: File, offset: u64) FileError!void {
        _ = try fileCheck(linux.lseek(self.handle, @bitCast(@as(isize, @bitCast(offset))), linux.SEEK.SET));
    }

    /// Same contract as the removed `std.fs.File.getEndPos`. Restores the file
    /// offset, so callers can interleave reads.
    pub fn getEndPos(self: File) FileError!u64 {
        const current = try fileCheck(linux.lseek(self.handle, 0, linux.SEEK.CUR));
        const size = try fileCheck(linux.lseek(self.handle, 0, linux.SEEK.END));
        _ = try fileCheck(linux.lseek(
            self.handle,
            @bitCast(@as(isize, @bitCast(current))),
            linux.SEEK.SET,
        ));
        return size;
    }

    pub fn setEndPos(self: File, new_size: u64) FileError!void {
        _ = try fileCheck(linux.ftruncate(self.handle, @bitCast(@as(i64, @bitCast(new_size)))));
    }

    pub fn sync(self: File) FileError!void {
        _ = try fileCheck(linux.fsync(self.handle));
    }

    pub fn datasync(self: File) FileError!void {
        _ = try fileCheck(linux.fdatasync(self.handle));
    }

    pub fn close(self: File) void {
        _ = linux.close(self.handle);
    }
};

fn openFlags(opts: FileOpenOptions, create: bool) linux.O {
    var flags = linux.O{ .ACCMODE = switch (opts.mode) {
        .read_only => .RDONLY,
        .write_only => .WRONLY,
        .read_write => .RDWR,
    } };
    if (create) {
        flags.CREAT = true;
        flags.TRUNC = opts.truncate;
    }
    return flags;
}

/// Replacement for `os_compat.openFile(path, .{ .mode = ... })`.
pub fn openFile(path: []const u8, opts: FileOpenOptions) FileError!File {
    return openFileAt(path, opts, false);
}

/// Replacement for `os_compat.createFile(path, .{ ... })`.
pub fn createFile(path: []const u8, opts: FileOpenOptions) FileError!File {
    return openFileAt(path, opts, true);
}

fn openFileAt(path: []const u8, opts: FileOpenOptions, create: bool) FileError!File {
    const c_path = toPathZ(path) orelse return error.NameTooLong;
    const rc = linux.openat(
        linux.AT.FDCWD,
        c_path,
        openFlags(opts, create),
        0o644,
    );
    return .{ .handle = @intCast(try fileCheck(rc)) };
}

/// Replacement for `os_compat.deleteFile(path)`.
pub fn deleteFile(path: []const u8) FileError!void {
    const c_path = toPathZ(path) orelse return error.NameTooLong;
    _ = try fileCheck(linux.unlinkat(linux.AT.FDCWD, c_path, 0));
}

/// Replacement for `os_compat.makeDir(path)`.
pub fn makeDir(path: []const u8) FileError!void {
    const c_path = toPathZ(path) orelse return error.NameTooLong;
    _ = try fileCheck(linux.mkdirat(linux.AT.FDCWD, c_path, 0o755));
}

pub const Kind = enum { file, directory, unknown };

pub const DirEntry = struct {
    name: []const u8,
    kind: Kind,
};

pub const Dir = struct {
    handle: posix.fd_t,
    buffer: [8192]u8 align(@alignOf(linux.dirent64)) = undefined,
    buffer_len: usize = 0,
    buffer_pos: usize = 0,

    pub fn close(self: *Dir) void {
        _ = linux.close(self.handle);
        self.* = undefined;
    }

    pub fn iterate(self: *Dir) Iterator {
        return .{ .dir = self };
    }

    /// Replacement for `std.fs.Dir.deleteFile`.
    pub fn deleteFile(self: *Dir, path: []const u8) FileError!void {
        const c_path = toPathZ(path) orelse return error.NameTooLong;
        _ = try fileCheck(linux.unlinkat(self.handle, c_path, 0));
    }

    pub const Iterator = struct {
        dir: *Dir,

        pub fn next(it: *Iterator) FileError!?DirEntry {
            if (it.dir.buffer_pos >= it.dir.buffer_len) {
                const rc = linux.getdents64(
                    it.dir.handle,
                    @ptrCast(&it.dir.buffer),
                    it.dir.buffer.len,
                );
                const n = try fileCheck(rc);
                if (n == 0) return null; // end of directory
                it.dir.buffer_len = n;
                it.dir.buffer_pos = 0;
            }
            const base: [*]u8 = @ptrCast(&it.dir.buffer);
            const aligned: [*]align(@alignOf(linux.dirent64)) const u8 = @alignCast(base + it.dir.buffer_pos);
            const dirent: *const linux.dirent64 = @ptrCast(aligned);
            const name: [*:0]const u8 = @ptrCast(&dirent.name);
            const name_slice = std.mem.span(name);
            const entry = DirEntry{
                .name = name_slice,
                .kind = switch (dirent.type) {
                    linux.DT.REG => .file,
                    linux.DT.DIR => .directory,
                    else => .unknown,
                },
            };
            it.dir.buffer_pos += dirent.reclen;
            return entry;
        }
    };
};

pub const OpenDirOptions = struct {
    /// Only `true` is supported: the returned `Dir` can be iterated.
    iterate: bool = false,
};

/// Replacement for `std.fs.cwd().openDir(path, .{ .iterate = true })`.
pub fn openDir(path: []const u8, opts: OpenDirOptions) FileError!Dir {
    if (!opts.iterate) @panic("os_compat.openDir supports only .iterate = true");
    const c_path = toPathZ(path) orelse return error.NameTooLong;
    const rc = linux.openat(linux.AT.FDCWD, c_path, .{ .ACCMODE = .RDONLY, .DIRECTORY = true }, 0);
    return .{ .handle = @intCast(try fileCheck(rc)) };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "a CPU bit lands on the CPU it names" {
    var set = std.mem.zeroes(CpuSet);

    // 0 and 63 share a word, 63 and 64 straddle two, 1023 is the last bit.
    cpuSetPut(&set, 0);
    cpuSetPut(&set, 63);
    cpuSetPut(&set, 64);
    cpuSetPut(&set, 1023);

    try testing.expect(cpuSetContains(&set, 0));
    try testing.expect(cpuSetContains(&set, 63));
    try testing.expect(cpuSetContains(&set, 64));
    try testing.expect(cpuSetContains(&set, 1023));

    // Neighbours stay clear: bit placement is not off by one either way.
    try testing.expect(!cpuSetContains(&set, 1));
    try testing.expect(!cpuSetContains(&set, 62));
    try testing.expect(!cpuSetContains(&set, 65));
    try testing.expect(!cpuSetContains(&set, 1022));

    var bits: usize = 0;
    for (set) |word| bits += @popCount(word);
    try testing.expectEqual(@as(usize, 4), bits);
}

test "a CPU id past the end of the set is dropped, not wrapped" {
    var set = std.mem.zeroes(CpuSet);

    cpuSetPut(&set, max_cpus);
    cpuSetPut(&set, 10_000);

    try testing.expect(!cpuSetContains(&set, max_cpus));
    try testing.expect(!cpuSetContains(&set, 10_000));
    for (set) |word| try testing.expectEqual(@as(usize, 0), word);
}

test "reading the affinity mask twice gives the same mask" {
    // Read only: setting a mask from a test would restrict the whole test
    // process if the test failed halfway through.
    var first: CpuSet = undefined;
    if (!cpuAffinityGet(&first)) return error.SkipZigTest;

    var second: CpuSet = undefined;
    try testing.expect(cpuAffinityGet(&second));
    try testing.expectEqualSlices(u8, std.mem.asBytes(&first), std.mem.asBytes(&second));

    // At least one CPU is allowed, and every bit has an id below the limit.
    var allowed: usize = 0;
    for (first) |word| allowed += @popCount(word);
    try testing.expect(allowed > 0);
}

test "a bound range reports the node it was bound to" {
    const page = std.heap.pageSize();

    // Page-aligned and a whole number of pages, which is what mbind takes.
    const bytes = try testing.allocator.alignedAlloc(u8, std.mem.Alignment.fromByteUnits(page), page);
    defer testing.allocator.free(bytes);

    const node = nodeOfThisCpu() orelse return error.SkipZigTest;

    // No NUMA, or a policy manager that forbids binding: nothing to prove, and
    // the partition code reports the table as unplaced in that case.
    if (!bindRangeToNode(bytes.ptr, bytes.len, node)) return error.SkipZigTest;

    // The touch is what allocates the page; until it happens there is no node
    // to report.
    @memset(bytes, 0xA5);

    try testing.expectEqual(node, nodeOfPage(bytes.ptr).?);

    // A range that is not page-aligned cannot be bound: mbind refuses, and the
    // caller must not treat a refusal as success.
    try testing.expect(!bindRangeToNode(bytes.ptr + 1, bytes.len - 1, node));
}

test "binding to a node that does not exist is refused, not rounded" {
    const page = std.heap.pageSize();
    const bytes = try testing.allocator.alignedAlloc(u8, std.mem.Alignment.fromByteUnits(page), page);
    defer testing.allocator.free(bytes);

    try testing.expect(!bindRangeToNode(bytes.ptr, bytes.len, max_nodes));
    try testing.expect(!bindRangeToNode(bytes.ptr, 0, 0));
}

test "a pinned thread asks about a CPU and gets a stable node" {
    var allowed: CpuSet = undefined;
    if (!cpuAffinityGet(&allowed)) return error.SkipZigTest;

    var first_cpu: ?u32 = null;
    var cpu_id: u32 = 0;
    while (cpu_id < max_cpus) : (cpu_id += 1) {
        if (cpuSetContains(&allowed, cpu_id)) {
            first_cpu = cpu_id;
            break;
        }
    }
    const pinned = first_cpu orelse return error.SkipZigTest;

    var only_this = std.mem.zeroes(CpuSet);
    cpuSetPut(&only_this, pinned);
    if (!cpuAffinitySet(&only_this)) return error.SkipZigTest;
    defer _ = cpuAffinitySet(&allowed);

    const node = nodeOfThisCpu() orelse return error.SkipZigTest;

    // The thread cannot move off the CPU it is pinned to, so the kernel's
    // answer cannot change between two calls.
    try testing.expectEqual(node, nodeOfThisCpu().?);

    // A node id is a small dense number; anything near the cap is a misread
    // return value, not an answer.
    try testing.expect(node < max_cpus);
}
