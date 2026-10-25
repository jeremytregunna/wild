//! Deep reachability check for the WILD source tree.
//!
//! Zig analyses code lazily. The compiler type-checks a function body only when
//! something reachable calls that function. `zig build` therefore passes on
//! functions that cannot compile. This file references every public declaration
//! of every module, so the compiler must analyse each one.
//!
//! Run it with `zig build deep-check`, or directly:
//!
//!     zig build-exe --dep wildb_lib -Mroot=deep_check_main.zig \
//!         -Mwildb_lib=src/root.zig -fno-emit-bin
//!
//! When you add a public type, add it here too. A type missing from this list
//! is a type the compiler never checks.

const std = @import("std");
const deep_check = @import("deep_check.zig");

const cache_topology = @import("src/cache_topology.zig");
const client = @import("src/client.zig");
const client_router = @import("src/client_router.zig");
const daemon = @import("src/daemon.zig");
const flat_hash_storage = @import("src/flat_hash_storage.zig");
const metrics = @import("src/metrics.zig");
const os_compat = @import("src/os_compat.zig");
const replication = @import("src/replication.zig");
const server = @import("src/server.zig");
const slab_allocator = @import("src/slab_allocator.zig");
const snapshot = @import("src/snapshot.zig");
const static_allocator = @import("src/static_allocator.zig");
const wal = @import("src/wal.zig");
const wild = @import("src/wild.zig");
const wire_protocol = @import("src/wire_protocol.zig");

pub fn main() void {}

comptime {
    deep_check.reach(cache_topology.CacheTopology);
    deep_check.reach(cache_topology.CacheDomain);
    deep_check.reach(cache_topology.ShardMapping);
    deep_check.reach(cache_topology.CpuCore);
    deep_check.reachFn(cache_topology.analyzeCacheTopology);

    deep_check.reach(client.WildClient);

    deep_check.reach(client_router.ClientRouter);

    deep_check.reach(daemon.WildDaemon);
    deep_check.reachFn(daemon.runDaemon);

    deep_check.reach(flat_hash_storage.FlatHashStorage);
    deep_check.reach(flat_hash_storage.CacheLineRecord);
    deep_check.reach(flat_hash_storage.StorageStats);
    deep_check.reachFn(flat_hash_storage.hashKey);

    deep_check.reach(metrics.MetricsCollector);

    deep_check.reach(os_compat.Address);
    deep_check.reach(os_compat.File);
    deep_check.reach(os_compat.Dir);
    deep_check.reach(os_compat.Mutex);
    deep_check.reach(os_compat.AF);
    deep_check.reach(os_compat.AT);
    deep_check.reach(os_compat.AddrInfoError);
    deep_check.reach(os_compat.DirEntry);
    deep_check.reach(os_compat.F);
    deep_check.reach(os_compat.FileError);
    deep_check.reach(os_compat.FileOpenOptions);
    deep_check.reach(os_compat.Kind);
    deep_check.reach(os_compat.MSG);
    deep_check.reach(os_compat.Mode);
    deep_check.reach(os_compat.OpenDirOptions);
    deep_check.reach(os_compat.SO);
    deep_check.reach(os_compat.SOCK);
    deep_check.reach(os_compat.SOL);
    deep_check.reach(os_compat.SocketError);
    deep_check.reach(os_compat.fd_t);
    deep_check.reach(os_compat.sockaddr);
    deep_check.reach(os_compat.socklen_t);
    deep_check.reachFn(os_compat.accept);
    deep_check.reachFn(os_compat.bind);
    deep_check.reachFn(os_compat.close);
    deep_check.reachFn(os_compat.connect);
    deep_check.reachFn(os_compat.createFile);
    deep_check.reachFn(os_compat.deleteFile);
    deep_check.reachFn(os_compat.fcntl);
    deep_check.reachFn(os_compat.fdatasync);
    deep_check.reachFn(os_compat.fsync);
    deep_check.reachFn(os_compat.listen);
    deep_check.reachFn(os_compat.makeDir);
    deep_check.reachFn(os_compat.milliTimestamp);
    deep_check.reachFn(os_compat.nanoTimestamp);
    deep_check.reachFn(os_compat.openDir);
    deep_check.reachFn(os_compat.openFile);
    deep_check.reachFn(os_compat.read);
    deep_check.reachFn(os_compat.recv);
    deep_check.reachFn(os_compat.send);
    deep_check.reachFn(os_compat.sendAllCompat);
    deep_check.reachFn(os_compat.setsockoptReuse);
    deep_check.reachFn(os_compat.sleep);
    deep_check.reachFn(os_compat.socket);
    deep_check.reachFn(os_compat.timestamp);
    deep_check.reachFn(os_compat.unlink);
    deep_check.reachFn(os_compat.write);

    deep_check.reach(replication.PrimaryReplicator);
    deep_check.reach(replication.ReplicaNode);
    deep_check.reach(replication.ReplicaConnection);
    deep_check.reach(replication.ReplicationMessage);
    deep_check.reach(replication.ReplicationStats);
    deep_check.reach(replication.ReplicaStats);

    deep_check.reach(server.WildServer);

    deep_check.reach(slab_allocator.SlabAllocator);

    deep_check.reach(snapshot.SnapshotHeader);
    deep_check.reach(snapshot.SnapshotFile);
    deep_check.reach(snapshot.SnapshotManager);
    deep_check.reach(snapshot.WALReplayer);
    deep_check.reach(snapshot.RecoveryCoordinator);

    deep_check.reach(static_allocator.StaticAllocator);

    deep_check.reach(wal.DurabilityManager);
    deep_check.reach(wal.ThreadLocalWAL);
    deep_check.reach(wal.WALEntry);
    deep_check.reach(wal.WALBatch);
    deep_check.reach(wal.WALCompletion);
    deep_check.reach(wal.WALFileInfo);

    deep_check.reach(wild.WILD);

    deep_check.reach(wire_protocol.WireProtocol);
    deep_check.reach(wire_protocol.WireMessage);
}
