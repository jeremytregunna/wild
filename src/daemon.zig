const std = @import("std");
const os_compat = @import("os_compat.zig");
const wild = @import("wild.zig");
const static_allocator = @import("static_allocator.zig");
const replication = @import("replication.zig");
const server = @import("server.zig");

// WILD Database Daemon - Production server for WILD database
pub const WildDaemon = struct {
    database: ?wild.WILD,
    static_alloc: ?static_allocator.StaticAllocator,
    database_arena: ?std.heap.ArenaAllocator,
    server: ?server.WildServer,
    server_thread: ?std.Thread,
    config: DaemonConfig,
    allocator: std.mem.Allocator,

    // Daemon state
    is_running: std.atomic.Value(bool),
    shutdown_signal: std.atomic.Value(bool),

    const Self = @This();

    pub const DaemonConfig = struct {
        // Database configuration
        capacity: u64,

        // Server configuration (for future TCP/HTTP API)
        bind_address: []const u8,
        port: u16,

        // WAL configuration (optional unless replication enabled)
        enable_wal: bool,
        wal_path: ?[]const u8,

        // Replication configuration
        replication_mode: ReplicationMode,
        replication_port: ?u16, // For primary mode
        primary_address: ?[]const u8, // For replica mode
        primary_port: ?u16, // For replica mode

        // Authentication
        auth_secret: ?[]const u8,

        pub const ReplicationMode = enum {
            none, // No replication
            primary, // Act as primary (accept replica connections)
            replica, // Act as replica (connect to primary)
        };

        pub fn default() DaemonConfig {
            return DaemonConfig{
                .capacity = 100000,
                .bind_address = "127.0.0.1",
                .port = 7878,
                .enable_wal = false,
                .wal_path = null,
                .replication_mode = .none,
                .replication_port = null,
                .primary_address = null,
                .primary_port = null,
                .auth_secret = null,
            };
        }

        // Validate configuration and enforce WAL requirement for replication
        pub fn validate(self: *const DaemonConfig) !void {
            // WAL is mandatory when replication is enabled
            if (self.replication_mode != .none and !self.enable_wal) {
                return error.WALRequiredForReplication;
            }

            // WAL path required when WAL is enabled
            if (self.enable_wal and self.wal_path == null) {
                return error.WALPathRequired;
            }

            // Primary mode requires replication port
            if (self.replication_mode == .primary and self.replication_port == null) {
                return error.ReplicationPortRequired;
            }

            // Replica mode requires primary address and port
            if (self.replication_mode == .replica) {
                if (self.primary_address == null or self.primary_port == null) {
                    return error.PrimaryAddressRequired;
                }
            }
        }
    };

    pub fn init(allocator: std.mem.Allocator, config: DaemonConfig) !Self {
        // Validate configuration first
        try config.validate();

        return Self{
            .database = null,
            .static_alloc = null,
            .database_arena = null,
            .server = null,
            .server_thread = null,
            .config = config,
            .allocator = allocator,
            .is_running = std.atomic.Value(bool).init(false),
            .shutdown_signal = std.atomic.Value(bool).init(false),
        };
    }

    pub fn deinit(self: *Self) void {
        self.stop();

        // The accept loop runs on its own thread and touches the database and
        // the static allocator. Stop it and wait before anything is freed.
        if (self.server_thread) |thread| {
            thread.join();
            self.server_thread = null;
        }

        if (self.database) |*db| {
            self.static_alloc.?.transitionToDeinit();
            db.deinit();
        }

        if (self.static_alloc) |*static_alloc| {
            static_alloc.deinit();
        }

        if (self.database_arena) |*arena| {
            arena.deinit();
        }
    }

    pub fn start(self: *Self) !void {
        if (self.is_running.load(.acquire)) {
            return error.AlreadyRunning;
        }

        // The database and the server store raw allocator pointers, so the arena
        // and the static allocator must live in the daemon, at an address that
        // outlives this function. As locals they handed both of them a pointer
        // into a dead stack frame, which the shutdown path then read.
        const database_arena = std.heap.ArenaAllocator.init(self.allocator);
        self.database_arena = database_arena;
        self.static_alloc = static_allocator.StaticAllocator.init(self.database_arena.?.allocator());

        const db_config = wild.WILD.Config{
            .target_capacity = self.config.capacity,
        };

        const database = try wild.WILD.init(self.static_alloc.?.allocator(), db_config);
        // Store the database before anything takes its address.
        self.database = database;

        // Enable WAL if configured
        if (self.config.enable_wal) {
            try self.database.?.enableDurabilityForStorage(self.config.wal_path.?);
        }

        // Enable replication based on mode
        switch (self.config.replication_mode) {
            .none => {
                // No replication setup needed
            },
            .primary => {
                if (!self.config.enable_wal) {
                    return error.WALRequiredForReplication;
                }
                const auth_secret = self.config.auth_secret orelse {
                    std.debug.print("Error: --auth-secret is required for replication mode\n", .{});
                    return error.AuthSecretRequired;
                };
                try self.database.?.enableReplicationAsPrimary(self.config.replication_port.?, auth_secret);
                std.debug.print("WILD daemon started as PRIMARY on replication port {}\n", .{self.config.replication_port.?});
            },
            .replica => {
                if (!self.config.enable_wal) {
                    return error.WALRequiredForReplication;
                }
                const auth_secret = self.config.auth_secret orelse {
                    std.debug.print("Error: --auth-secret is required for replication mode\n", .{});
                    return error.AuthSecretRequired;
                };
                try self.database.?.enableReplicationAsReplica(self.config.primary_address.?, self.config.primary_port.?, auth_secret);
                std.debug.print("WILD daemon started as REPLICA connecting to {s}:{}\n", .{ self.config.primary_address.?, self.config.primary_port.? });
            },
        }

        // Start the client-facing server. WildServer allocates from the static
        // allocator, so it must be initialised before the transition freezes
        // it. An empty secret means the server takes commands without a secret.
        const auth_secret = self.config.auth_secret orelse "";
        const wild_server = try server.WildServer.init(
            &self.database.?,
            &self.static_alloc.?,
            self.config.bind_address,
            self.config.port,
            auth_secret,
        );

        // Transition to static state
        self.static_alloc.?.transitionToStatic();

        self.server = wild_server;
        self.is_running.store(true, .release);

        // The accept loop blocks, so it gets its own thread. The daemon main
        // loop stays free to print stats and to watch for a shutdown signal.
        self.server_thread = try std.Thread.spawn(.{}, serveClientConnections, .{&self.server.?});

        std.debug.print("WILD daemon started successfully\n", .{});
        std.debug.print("  Client server: {s}:{}\n", .{ self.config.bind_address, self.config.port });
        std.debug.print("  Capacity: {} records\n", .{self.config.capacity});
        std.debug.print("  WAL: {s}\n", .{if (self.config.enable_wal) "enabled" else "disabled"});
        std.debug.print("  Replication: {s}\n", .{@tagName(self.config.replication_mode)});
    }

    pub fn stop(self: *Self) void {
        if (!self.is_running.load(.acquire)) return;

        self.shutdown_signal.store(true, .release);

        // Ask the accept loop to return before deinit frees its memory.
        if (self.server) |*srv| {
            srv.requestShutdown();
        }

        // Disable replication gracefully
        if (self.database) |*db| {
            db.disableReplication();
        }

        self.is_running.store(false, .release);
        std.debug.print("WILD daemon stopped\n", .{});
    }

    pub fn isRunning(self: *const Self) bool {
        return self.is_running.load(.acquire);
    }

    /// Thread entry for the blocking accept loop inside WildServer.start.
    fn serveClientConnections(wild_server: *server.WildServer) void {
        wild_server.start() catch |err| {
            std.debug.print("WILD client server stopped: {}\n", .{err});
        };
    }

    pub fn shouldShutdown(self: *const Self) bool {
        return self.shutdown_signal.load(.acquire);
    }

    // Get database handle for operations (only when running)
    pub fn getDatabase(self: *Self) ?*wild.WILD {
        if (!self.is_running.load(.acquire)) return null;
        return if (self.database) |*db| db else null;
    }

    // Get runtime statistics
    pub fn getStats(self: *const Self) ?DaemonStats {
        if (!self.isRunning()) return null;

        const db = &self.database.?;
        const db_stats = db.getStats();

        var replication_stats: ?ReplicationStatsUnion = null;

        if (db.getReplicationStats()) |stats| {
            switch (stats) {
                .primary => |p| replication_stats = .{ .primary = p },
                .replica => |r| replication_stats = .{ .replica = r },
            }
        }

        return DaemonStats{
            .database_stats = db_stats,
            .durability_stats = db.getDurabilityStatsNoAlloc(),
            .replication_stats = replication_stats,
            .replication_mode = self.config.replication_mode,
            .wal_enabled = self.config.enable_wal,
        };
    }

    const ReplicationStatsUnion = union(enum) {
        primary: replication.ReplicationStats,
        replica: replication.ReplicaStats,
    };

    pub const DaemonStats = struct {
        database_stats: wild.WILD.Stats,
        durability_stats: ?wild.WILD.DurabilityStatsSimple,
        replication_stats: ?ReplicationStatsUnion,
        replication_mode: DaemonConfig.ReplicationMode,
        wal_enabled: bool,

        pub fn print(self: *const DaemonStats) void {
            std.debug.print("\n=== WILD Daemon Statistics ===\n", .{});
            std.debug.print("Database:\n", .{});
            std.debug.print("  Capacity: {}/{}\n", .{ self.database_stats.used_capacity, self.database_stats.total_capacity });
            std.debug.print("  Load Factor: {d:.1}%\n", .{self.database_stats.load_factor * 100});

            std.debug.print("Configuration:\n", .{});
            std.debug.print("  WAL: {s}\n", .{if (self.wal_enabled) "enabled" else "disabled"});
            if (self.durability_stats) |durability| {
                std.debug.print("  WAL Batches Written: {}\n", .{durability.batches_written});
                std.debug.print("  WAL Entries Written: {}\n", .{durability.total_entries_written});
                std.debug.print("  WAL Entries Dropped: {} (writes that are in memory only)\n", .{durability.entries_dropped});
                std.debug.print("  WAL I/O Errors: {}\n", .{durability.io_errors});
                std.debug.print("  WAL Pending I/O: {}\n", .{durability.pending_ios});
            }
            std.debug.print("  Replication: {s}\n", .{@tagName(self.replication_mode)});

            if (self.replication_stats) |stats| {
                switch (stats) {
                    .primary => |p| {
                        std.debug.print("Primary Replication:\n", .{});
                        std.debug.print("  Connected Replicas: {}\n", .{p.replicas_connected});
                        std.debug.print("  Batches Replicated: {}\n", .{p.batches_replicated});
                        std.debug.print("  Replication Errors: {}\n", .{p.replication_errors});
                        std.debug.print("  Max Replica Lag: {} batches\n", .{p.max_replica_lag});
                        std.debug.print("  Lagging Replicas: {}\n", .{p.lagging_replicas});
                    },
                    .replica => |r| {
                        std.debug.print("Replica Status:\n", .{});
                        std.debug.print("  Connected: {s}\n", .{if (r.is_connected) "yes" else "no"});
                        std.debug.print("  Last Applied Batch: {}\n", .{r.last_applied_batch});
                        std.debug.print("  Batches Applied: {}\n", .{r.batches_applied});
                        std.debug.print("  Connection Errors: {}\n", .{r.connection_errors});
                    },
                }
            }
            std.debug.print("===============================\n", .{});
        }
    };
};

// Signal handling for graceful shutdown
var global_daemon: ?*WildDaemon = null;

fn signalHandler(sig: std.posix.SIG) callconv(.c) void {
    _ = sig;
    if (global_daemon) |daemon| {
        daemon.stop();
    }
}

// Main daemon entry point
pub fn runDaemon(allocator: std.mem.Allocator, config: WildDaemon.DaemonConfig) !void {
    var daemon = try WildDaemon.init(allocator, config);
    defer daemon.deinit();

    // Set up signal handling for graceful shutdown
    global_daemon = &daemon;
    const act = std.posix.Sigaction{
        .handler = .{ .handler = signalHandler },
        .mask = std.posix.sigset_t{0},
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.INT, &act, null);
    std.posix.sigaction(std.posix.SIG.TERM, &act, null);

    // Start the daemon
    try daemon.start();

    // Main daemon loop. The sleep is sliced: a single 30-second sleep does not
    // notice the shutdown flag, so a quit idle server would wait for the next
    // stats report to arrive before it stopped.
    const slice_ns: u64 = 50_000_000; // 50 ms
    const stats_interval_ms: u64 = 30_000;
    var since_report_ms: u64 = 0;

    while (daemon.isRunning() and !daemon.shouldShutdown()) {
        os_compat.sleep(slice_ns);
        since_report_ms += slice_ns / std.time.ns_per_ms;

        if (since_report_ms >= stats_interval_ms) {
            since_report_ms = 0;
            if (daemon.getStats()) |stats| {
                stats.print();
            }
        }
    }

    std.debug.print("WILD daemon shutting down...\n", .{});
}
