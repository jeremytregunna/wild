const std = @import("std");
const daemon = @import("src/daemon.zig");

// WILD Database Production Daemon
// Command-line interface for running WILD in production mode

const VERSION = "1.0.0";

const CommandLineArgs = struct {
    mode: daemon.WildDaemon.DaemonConfig.ReplicationMode = .none,
    capacity: u64 = 100000,
    bind_address: []const u8 = "127.0.0.1",
    port: u16 = 7878,
    enable_wal: bool = false,
    wal_path: ?[]const u8 = null,
    replication_port: ?u16 = null,
    primary_address: ?[]const u8 = null,
    primary_port: ?u16 = null,
    help: bool = false,
    version: bool = false,
};

fn printUsage(program_name: []const u8) void {
    std.debug.print("WILD Database v{s} - Production Daemon\n\n", .{VERSION});
    std.debug.print("Usage: {s} [OPTIONS]\n\n", .{program_name});
    std.debug.print("Options:\n", .{});
    std.debug.print("  --mode MODE              Operation mode: none, primary, replica (default: none)\n", .{});
    std.debug.print("  --capacity NUM           Database capacity in records (default: 100000)\n", .{});
    std.debug.print("  --bind-address ADDR      Server bind address (default: 127.0.0.1)\n", .{});
    std.debug.print("  --port PORT              Server port (default: 7878)\n", .{});
    std.debug.print("  --enable-wal             Enable Write-Ahead Log (required for replication)\n", .{});
    std.debug.print("  --wal-path PATH          Path to WAL file (required if --enable-wal)\n", .{});
    std.debug.print("  --replication-port PORT  Port for replica connections (primary mode)\n", .{});
    std.debug.print("  --primary-address ADDR   Primary server address (replica mode)\n", .{});
    std.debug.print("  --primary-port PORT      Primary server port (replica mode)\n", .{});
    std.debug.print("  --help, -h               Show this help message\n", .{});
    std.debug.print("  --version, -v            Show version information\n", .{});
    std.debug.print("\nExamples:\n", .{});
    std.debug.print("  # Standalone database without replication\n", .{});
    std.debug.print("  {s} --capacity 1000000\n\n", .{program_name});
    std.debug.print("  # Primary with replication enabled\n", .{});
    std.debug.print("  {s} --mode primary --enable-wal --wal-path primary.wal --replication-port 9001\n\n", .{program_name});
    std.debug.print("  # Replica connecting to primary\n", .{});
    std.debug.print("  {s} --mode replica --primary-address 192.168.1.100 --primary-port 9001\n\n", .{program_name});
}

fn parseArguments(allocator: std.mem.Allocator, args: [][:0]u8) !CommandLineArgs {
    var parsed_args = CommandLineArgs{};
    
    var i: usize = 1; // Skip program name
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            parsed_args.help = true;
        } else if (std.mem.eql(u8, arg, "--version") or std.mem.eql(u8, arg, "-v")) {
            parsed_args.version = true;
        } else if (std.mem.eql(u8, arg, "--mode")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            const mode_str = args[i];
            if (std.mem.eql(u8, mode_str, "none")) {
                parsed_args.mode = .none;
            } else if (std.mem.eql(u8, mode_str, "primary")) {
                parsed_args.mode = .primary;
            } else if (std.mem.eql(u8, mode_str, "replica")) {
                parsed_args.mode = .replica;
            } else {
                std.debug.print("Invalid mode: {s}. Use 'none', 'primary', or 'replica'\n", .{mode_str});
                return error.InvalidMode;
            }
        } else if (std.mem.eql(u8, arg, "--capacity")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            parsed_args.capacity = try std.fmt.parseInt(u64, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--bind-address")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            parsed_args.bind_address = try allocator.dupe(u8, args[i]);
        } else if (std.mem.eql(u8, arg, "--port")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            parsed_args.port = try std.fmt.parseInt(u16, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--enable-wal")) {
            parsed_args.enable_wal = true;
        } else if (std.mem.eql(u8, arg, "--wal-path")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            parsed_args.wal_path = try allocator.dupe(u8, args[i]);
        } else if (std.mem.eql(u8, arg, "--replication-port")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            parsed_args.replication_port = try std.fmt.parseInt(u16, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--primary-address")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            parsed_args.primary_address = try allocator.dupe(u8, args[i]);
        } else if (std.mem.eql(u8, arg, "--primary-port")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            parsed_args.primary_port = try std.fmt.parseInt(u16, args[i], 10);
        } else {
            std.debug.print("Unknown argument: {s}\n", .{arg});
            return error.UnknownArgument;
        }
    }
    
    return parsed_args;
}

fn buildDaemonConfig(args: CommandLineArgs) daemon.WildDaemon.DaemonConfig {
    return daemon.WildDaemon.DaemonConfig{
        .capacity = args.capacity,
        .bind_address = args.bind_address,
        .port = args.port,
        .enable_wal = args.enable_wal,
        .wal_path = args.wal_path,
        .replication_mode = args.mode,
        .replication_port = args.replication_port,
        .primary_address = args.primary_address,
        .primary_port = args.primary_port,
    };
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);
    
    if (args.len < 1) {
        printUsage("wild");
        return;
    }
    
    const parsed_args = parseArguments(allocator, args) catch |err| {
        switch (err) {
            error.MissingValue => std.debug.print("Error: Missing value for argument\n", .{}),
            error.InvalidMode => {}, // Already printed
            error.UnknownArgument => {}, // Already printed
            else => std.debug.print("Error parsing arguments: {}\n", .{err}),
        }
        std.debug.print("Use --help for usage information\n", .{});
        std.process.exit(1);
    };
    
    if (parsed_args.help) {
        printUsage(args[0]);
        return;
    }
    
    if (parsed_args.version) {
        std.debug.print("WILD Database v{s}\n", .{VERSION});
        return;
    }
    
    // Build daemon configuration
    const config = buildDaemonConfig(parsed_args);
    
    // Validate configuration
    config.validate() catch |err| {
        switch (err) {
            error.WALRequiredForReplication => {
                std.debug.print("Error: WAL must be enabled for replication. Use --enable-wal --wal-path <path>\n", .{});
            },
            error.WALPathRequired => {
                std.debug.print("Error: WAL path required when WAL is enabled. Use --wal-path <path>\n", .{});
            },
            error.ReplicationPortRequired => {
                std.debug.print("Error: Replication port required for primary mode. Use --replication-port <port>\n", .{});
            },
            error.PrimaryAddressRequired => {
                std.debug.print("Error: Primary address and port required for replica mode. Use --primary-address <addr> --primary-port <port>\n", .{});
            },
        }
        std.process.exit(1);
    };
    
    // Print startup banner
    std.debug.print("🗲 WILD Database v{s} Starting Up\n", .{VERSION});
    std.debug.print("Configuration:\n", .{});
    std.debug.print("  Mode: {s}\n", .{@tagName(config.replication_mode)});
    std.debug.print("  Capacity: {} records\n", .{config.capacity});
    std.debug.print("  Bind: {s}:{}\n", .{ config.bind_address, config.port });
    std.debug.print("  WAL: {s}\n", .{if (config.enable_wal) "enabled" else "disabled"});
    if (config.wal_path) |path| {
        std.debug.print("  WAL Path: {s}\n", .{path});
    }
    if (config.replication_mode == .primary and config.replication_port != null) {
        std.debug.print("  Replication Port: {}\n", .{config.replication_port.?});
    }
    if (config.replication_mode == .replica) {
        std.debug.print("  Primary: {s}:{}\n", .{ config.primary_address.?, config.primary_port.? });
    }
    std.debug.print("\n", .{});
    
    // Run the daemon
    try daemon.runDaemon(allocator, config);
}