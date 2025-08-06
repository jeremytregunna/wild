//! WILD CLI interface
const std = @import("std");
const wild = @import("wild.zig");
const static_allocator = @import("static_allocator.zig");
const server = @import("server.zig");

const Command = enum {
    set,
    get,
    delete,
    stats,
    help,
    exit,
};

const CliError = error{
    InvalidCommand,
    MissingKey,
    MissingValue,
    DatabaseError,
};

fn parseCommand(input: []const u8) !Command {
    const trimmed = std.mem.trim(u8, input, " \t\n\r");

    if (std.mem.eql(u8, trimmed, "set")) return .set;
    if (std.mem.eql(u8, trimmed, "get")) return .get;
    if (std.mem.eql(u8, trimmed, "delete")) return .delete;
    if (std.mem.eql(u8, trimmed, "stats")) return .stats;
    if (std.mem.eql(u8, trimmed, "help")) return .help;
    if (std.mem.eql(u8, trimmed, "exit")) return .exit;

    return CliError.InvalidCommand;
}

fn parseSetCommand(input: []const u8) !struct { key: []const u8, value: []const u8 } {
    var parts = std.mem.tokenizeAny(u8, input, " \t");

    // Skip "set" command
    _ = parts.next() orelse return CliError.InvalidCommand;

    const key = parts.next() orelse return CliError.MissingKey;
    const value = parts.rest();

    if (value.len == 0) return CliError.MissingValue;

    return .{ .key = key, .value = std.mem.trim(u8, value, " \t") };
}

fn parseGetCommand(input: []const u8) ![]const u8 {
    var parts = std.mem.tokenizeAny(u8, input, " \t");

    // Skip "get" command
    _ = parts.next() orelse return CliError.InvalidCommand;

    const key = parts.next() orelse return CliError.MissingKey;
    return key;
}

fn parseDeleteCommand(input: []const u8) ![]const u8 {
    var parts = std.mem.tokenizeAny(u8, input, " \t");

    // Skip "delete" command
    _ = parts.next() orelse return CliError.InvalidCommand;

    const key = parts.next() orelse return CliError.MissingKey;
    return key;
}

fn printHelp() void {
    std.debug.print("WILD CLI Commands:\n", .{});
    std.debug.print("  set <key> <value>  - Store a key-value pair (value max of 52 bytes)\n", .{});
    std.debug.print("  get <key>          - Retrieve value for key\n", .{});
    std.debug.print("  delete <key>       - Delete key-value pair\n", .{});
    std.debug.print("  stats              - Show database statistics\n", .{});
    std.debug.print("  help               - Show this help message\n", .{});
    std.debug.print("  exit               - Exit the CLI\n", .{});
}

fn runCli(database: *wild.WILD, static_alloc: *static_allocator.StaticAllocator) void {
    const stdin = std.io.getStdIn().reader();
    var buffer: [1024]u8 = undefined;

    while (true) {
        std.debug.print("> ", .{});

        if (stdin.readUntilDelimiterOrEof(buffer[0..], '\n') catch null) |input| {
            const trimmed_input = std.mem.trim(u8, input, " \t\n\r");

            if (trimmed_input.len == 0) continue;

            // Parse first word to determine command
            var first_word_end: usize = 0;
            for (trimmed_input, 0..) |char, i| {
                if (char == ' ' or char == '\t') {
                    first_word_end = i;
                    break;
                }
            } else {
                first_word_end = trimmed_input.len;
            }

            const command_str = trimmed_input[0..first_word_end];
            const command = parseCommand(command_str) catch {
                std.debug.print("Unknown command: {}. Type 'help' for available commands.\n", .{std.zig.fmtEscapes(command_str)});
                continue;
            };

            switch (command) {
                .set => {
                    const parsed = parseSetCommand(trimmed_input) catch |err| {
                        switch (err) {
                            CliError.MissingKey => std.debug.print("Error: Missing key. Usage: set <key> <value>\n", .{}),
                            CliError.MissingValue => std.debug.print("Error: Missing value. Usage: set <key> <value>\n", .{}),
                            else => std.debug.print("Error parsing set command: {}\n", .{err}),
                        }
                        continue;
                    };

                    database.writeString(parsed.key, parsed.value) catch |err| {
                        std.debug.print("Error storing key-value: {}\n", .{err});
                        continue;
                    };

                    std.debug.print("OK\n", .{});
                },
                .get => {
                    const key = parseGetCommand(trimmed_input) catch |err| {
                        switch (err) {
                            CliError.MissingKey => std.debug.print("Error: Missing key. Usage: get <key>\n", .{}),
                            else => std.debug.print("Error parsing get command: {}\n", .{err}),
                        }
                        continue;
                    };

                    const result = database.readString(key) catch |err| {
                        std.debug.print("Error reading key: {}\n", .{err});
                        continue;
                    };

                    if (result) |record| {
                        const data = record.getData();
                        std.debug.print("{}\n", .{std.zig.fmtEscapes(data)});
                    } else {
                        std.debug.print("(nil)\n", .{});
                    }
                },
                .delete => {
                    const key = parseDeleteCommand(trimmed_input) catch |err| {
                        switch (err) {
                            CliError.MissingKey => std.debug.print("Error: Missing key. Usage: delete <key>\n", .{}),
                            else => std.debug.print("Error parsing delete command: {}\n", .{err}),
                        }
                        continue;
                    };

                    const deleted = database.deleteString(key) catch |err| {
                        std.debug.print("Error deleting key: {}\n", .{err});
                        continue;
                    };

                    if (deleted) {
                        std.debug.print("OK\n", .{});
                    } else {
                        std.debug.print("Key not found\n", .{});
                    }
                },
                .stats => {
                    const stats = database.getStats();
                    std.debug.print("Database Statistics:\n", .{});
                    std.debug.print("- Used capacity: {}/{} ({d:.1}%)\n", .{ stats.used_capacity, stats.total_capacity, @as(f64, @floatFromInt(stats.used_capacity)) / @as(f64, @floatFromInt(stats.total_capacity)) * 100 });
                    std.debug.print("- Optimal batch size: {}\n", .{stats.optimal_batch_size});
                    std.debug.print("- Physical cores: {}\n", .{stats.physical_cores});
                    std.debug.print("- Cache line size: {} bytes\n", .{stats.cache_line_size});
                },
                .help => {
                    printHelp();
                },
                .exit => {
                    std.debug.print("Goodbye!\n", .{});
                    break;
                },
            }
        } else {
            // EOF reached (Ctrl+D)
            std.debug.print("\nGoodbye!\n", .{});
            break;
        }
    }

    // Proper cleanup order: transition allocator first, then database
    static_alloc.transitionToDeinit();
    database.deinit();
}

const ServerMode = struct {
    enabled: bool = false,
    bind_address: []const u8 = "",
    port: u16 = 0,
};

const WalConfig = struct {
    enabled: bool = false,
    wal_path: []const u8 = "wild.wal",
};

fn parseArgs(allocator: std.mem.Allocator) !struct { server_mode: ServerMode, wal_config: WalConfig } {
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var server_mode = ServerMode{};
    var wal_config = WalConfig{};

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "-listen")) {
            if (i + 1 >= args.len) {
                std.debug.print("Error: -listen requires an address:port argument\n", .{});
                return error.InvalidArguments;
            }

            const listen_addr = args[i + 1];
            i += 1; // Skip the next argument

            // Parse address:port
            if (std.mem.lastIndexOf(u8, listen_addr, ":")) |colon_pos| {
                // Copy the address string to avoid dangling pointer
                const addr_slice = listen_addr[0..colon_pos];
                const copied_addr = try allocator.dupe(u8, addr_slice);
                server_mode.bind_address = copied_addr;
                const port_str = listen_addr[colon_pos + 1 ..];
                server_mode.port = std.fmt.parseInt(u16, port_str, 10) catch {
                    std.debug.print("Error: Invalid port number: {s}\n", .{port_str});
                    return error.InvalidPort;
                };
                server_mode.enabled = true;
            } else {
                std.debug.print("Error: -listen address must be in format address:port\n", .{});
                return error.InvalidAddress;
            }
        } else if (std.mem.eql(u8, args[i], "-wal")) {
            wal_config.enabled = true;
            // Check if next argument is a custom WAL path
            if (i + 1 < args.len and !std.mem.startsWith(u8, args[i + 1], "-")) {
                i += 1;
                const copied_path = try allocator.dupe(u8, args[i]);
                wal_config.wal_path = copied_path;
            }
        }
    }

    return .{ .server_mode = server_mode, .wal_config = wal_config };
}

pub fn main() !void {
    const page_allocator = std.heap.page_allocator;

    // Parse command line arguments
    const args_result = parseArgs(page_allocator) catch |err| {
        std.debug.print("Usage: wild [-listen address:port] [-wal [path]]\n", .{});
        return err;
    };
    const server_mode = args_result.server_mode;
    const wal_config = args_result.wal_config;

    // Database arena allocator
    var database_arena = std.heap.ArenaAllocator.init(page_allocator);
    defer database_arena.deinit();

    // Create static allocator for database
    var static_alloc = static_allocator.StaticAllocator.init(database_arena.allocator());
    defer static_alloc.deinit();

    // Don't print CLI banner here - will be printed conditionally

    // Detect cache topology to calculate proper capacity
    const cache_topology = @import("cache_topology.zig");
    const topology = cache_topology.analyzeCacheTopology(static_alloc.allocator()) catch |err| {
        std.debug.print("Failed to analyze cache topology: {}\n", .{err});
        std.debug.print("Using default configuration...\n", .{});

        // Fallback to reasonable defaults
        const config = wild.WILD.Config{
            .target_capacity = 100_000,
        };

        var database = wild.WILD.init(static_alloc.allocator(), config) catch |init_err| {
            std.debug.print("Failed to initialize database: {}\n", .{init_err});
            return;
        };

        // Enable WAL if requested (fallback case)
        if (wal_config.enabled) {
            std.debug.print("Enabling write-ahead log: {s}\n", .{wal_config.wal_path});

            // Enable snapshots (required for recovery)
            const snapshot_config = @import("snapshot.zig").SnapshotManager.Config{
                .snapshot_dir = "wild_snapshots",
                .max_snapshots = 5,
                .snapshot_interval_seconds = 300,
            };
            database.enableSnapshots(snapshot_config) catch |snap_err| {
                std.debug.print("Failed to enable snapshots: {}\n", .{snap_err});
                return;
            };

            // Enable WAL
            database.enableDurabilityForStorage(wal_config.wal_path) catch |wal_err| {
                std.debug.print("Failed to enable WAL: {}\n", .{wal_err});
                return;
            };

            // Attempt recovery from existing WAL
            std.debug.print("Attempting recovery from WAL...\n", .{});
            database.performRecovery(wal_config.wal_path) catch |recovery_err| {
                // Recovery failure is not fatal - might be first run or no previous data
                std.debug.print("WAL recovery error: {} (likely first run or no previous data)\n", .{recovery_err});
            };
        }

        if (server_mode.enabled) {
            // Initialize server before transitioning to static state
            var wild_server = try server.WildServer.init(&database, &static_alloc, server_mode.bind_address, server_mode.port);
            static_alloc.transitionToStatic();
            try wild_server.start();
        } else {
            std.debug.print("WILD CLI\n", .{});
            std.debug.print("Type 'help' for commands\n\n", .{});
            static_alloc.transitionToStatic();
            runCli(&database, &static_alloc);
        }
        return;
    };

    // Calculate detected L3 cache size
    var total_l3_cache_kb: u64 = 0;
    for (topology.l3_domains) |domain| {
        total_l3_cache_kb += domain.cache_size_kb;
    }
    const detected_cache_mb = @as(u32, @intCast(total_l3_cache_kb / 1024));

    // Calculate target capacity based on actual cache size and record size
    const cache_line_size = topology.cache_line_size;
    const record_size = cache_line_size; // Each record fits in one cache line
    const available_cache_bytes = detected_cache_mb * 1024 * 1024;
    const target_capacity = @as(u64, @intFromFloat(@as(f64, @floatFromInt(available_cache_bytes)) * 0.75 / @as(f64, @floatFromInt(record_size)))); // 75% load factor

    std.debug.print("Detected: {} MB L3 cache, {} records capacity\n", .{ detected_cache_mb, target_capacity });

    // Initialize database with calculated capacity
    const config = wild.WILD.Config{
        .target_capacity = target_capacity,
    };

    var database = wild.WILD.init(static_alloc.allocator(), config) catch |err| {
        std.debug.print("Failed to initialize database: {}\n", .{err});
        return;
    };

    // Enable WAL if requested
    if (wal_config.enabled) {
        std.debug.print("Enabling write-ahead log: {s}\n", .{wal_config.wal_path});

        // Enable snapshots (required for recovery)
        const snapshot_config = @import("snapshot.zig").SnapshotManager.Config{
            .snapshot_dir = "wild_snapshots",
            .max_snapshots = 5,
            .snapshot_interval_seconds = 300,
        };
        database.enableSnapshots(snapshot_config) catch |snap_err| {
            std.debug.print("Failed to enable snapshots: {}\n", .{snap_err});
            return;
        };

        // Enable WAL
        database.enableDurabilityForStorage(wal_config.wal_path) catch |wal_err| {
            std.debug.print("Failed to enable WAL: {}\n", .{wal_err});
            return;
        };

        // Attempt recovery from existing WAL
        std.debug.print("Attempting recovery from WAL...\n", .{});
        database.performRecovery(wal_config.wal_path) catch |recovery_err| {
            // Recovery failure is not fatal - might be first run or no previous data
            std.debug.print("WAL recovery error: {} (likely first run or no previous data)\n", .{recovery_err});
        };
    }

    if (server_mode.enabled) {
        // Initialize server before transitioning to static state
        var wild_server = try server.WildServer.init(&database, &static_alloc, server_mode.bind_address, server_mode.port);
        static_alloc.transitionToStatic();
        try wild_server.start();
    } else {
        std.debug.print("WILD CLI\n", .{});
        std.debug.print("Type 'help' for commands\n\n", .{});
        static_alloc.transitionToStatic();
        runCli(&database, &static_alloc);
    }
}
