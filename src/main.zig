//! WILD CLI interface
const std = @import("std");
const wild = @import("wild.zig");
const static_allocator = @import("static_allocator.zig");

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

pub fn main() !void {
    const page_allocator = std.heap.page_allocator;

    // Database arena allocator
    var database_arena = std.heap.ArenaAllocator.init(page_allocator);
    defer database_arena.deinit();

    // Create static allocator for database
    var static_alloc = static_allocator.StaticAllocator.init(database_arena.allocator());
    defer static_alloc.deinit();

    std.debug.print("WILD CLI\n", .{});

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

        static_alloc.transitionToStatic();
        std.debug.print("Type 'help' for commands\n\n", .{});
        runCli(&database, &static_alloc);
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
    std.debug.print("Type 'help' for commands\n\n", .{});

    // Initialize database with calculated capacity
    const config = wild.WILD.Config{
        .target_capacity = target_capacity,
    };

    var database = wild.WILD.init(static_alloc.allocator(), config) catch |err| {
        std.debug.print("Failed to initialize database: {}\n", .{err});
        return;
    };

    // Transition to static state
    static_alloc.transitionToStatic();

    runCli(&database, &static_alloc);
}
