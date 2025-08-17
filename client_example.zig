const std = @import("std");
const client_router = @import("src/client_router.zig");

// Example client application using the WILD client router
// Demonstrates load-balanced reads across replicas with failover

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    
    // Parse command line arguments
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);
    
    if (args.len < 2) {
        std.debug.print("Usage: {s} <command> [args...]\n", .{args[0]});
        std.debug.print("Commands:\n", .{});
        std.debug.print("  demo             - Run load balancing demo\n", .{});
        std.debug.print("  health           - Check endpoint health\n", .{});
        std.debug.print("  stats            - Show router statistics\n", .{});
        return;
    }
    
    const command = args[1];
    
    if (std.mem.eql(u8, command, "demo")) {
        try runLoadBalancingDemo(allocator);
    } else if (std.mem.eql(u8, command, "health")) {
        try checkEndpointHealth(allocator);
    } else if (std.mem.eql(u8, command, "stats")) {
        try showRouterStats(allocator);
    } else {
        std.debug.print("Unknown command: {s}\n", .{command});
        return;
    }
}

fn runLoadBalancingDemo(allocator: std.mem.Allocator) !void {
    std.debug.print("🗲 WILD Client Router Demo\n\n", .{});
    
    // Configure router with primary and replicas
    const replica_addresses = [_][]const u8{ "127.0.0.1", "127.0.0.1", "127.0.0.1" };
    const replica_ports = [_]u16{ 9002, 9003, 9004 };
    
    const config = client_router.ClientRouter.Config{
        .primary_address = "127.0.0.1",
        .primary_port = 9001,
        .replica_addresses = replica_addresses[0..],
        .replica_ports = replica_ports[0..],
        .auth_secret = "defaultsecret", // In production, use a secure secret
        .max_connections_per_endpoint = 5,
        .health_check_interval_ms = 10000, // 10 seconds for demo
    };
    
    var router = try client_router.ClientRouter.init(allocator, config);
    defer router.deinit();
    
    std.debug.print("Configured router:\n", .{});
    std.debug.print("  Primary: {s}:{}\n", .{ config.primary_address, config.primary_port });
    std.debug.print("  Replicas: {} endpoints\n", .{replica_addresses.len});
    std.debug.print("\n", .{});
    
    // Simulate load-balanced read operations
    std.debug.print("Simulating load-balanced reads...\n", .{});
    for (1..11) |i| {
        const key = @as(u64, @intCast(i));
        
        std.debug.print("Read #{}: key={} -> ", .{ i, key });
        
        if (router.routeRead(key)) |result| {
            if (result) |data| {
                std.debug.print("found: {s}\n", .{data});
            } else {
                std.debug.print("not found\n", .{});
            }
        } else |err| {
            std.debug.print("error: {}\n", .{err});
        }
        
        // Small delay between operations
        std.time.sleep(100_000_000); // 100ms
    }
    
    std.debug.print("\n", .{});
    
    // Show router statistics
    const stats = router.getStats();
    std.debug.print("Router Statistics:\n", .{});
    std.debug.print("  Primary healthy: {s}\n", .{if (stats.primary_healthy) "yes" else "no"});
    std.debug.print("  Healthy replicas: {}/{}\n", .{ stats.healthy_replicas, stats.total_replicas });
    std.debug.print("  Total failures: {}\n", .{stats.total_failures});
    std.debug.print("  Active connections: {}\n", .{stats.active_connections});
    
    std.debug.print("\n", .{});
    
    // Simulate write operations (always go to primary)
    std.debug.print("Simulating writes to primary...\n", .{});
    for (1..6) |i| {
        const key = @as(u64, @intCast(i + 100));
        const data = try std.fmt.allocPrint(allocator, "value_{}", .{i});
        defer allocator.free(data);
        
        std.debug.print("Write #{}: key={}, data={s} -> ", .{ i, key, data });
        
        router.routeWrite(key, data) catch |err| {
            std.debug.print("error: {}\n", .{err});
            continue;
        };
        
        std.debug.print("success\n", .{});
        std.time.sleep(50_000_000); // 50ms
    }
    
    std.debug.print("\nDemo complete! 🎉\n", .{});
}

fn checkEndpointHealth(allocator: std.mem.Allocator) !void {
    std.debug.print("🏥 WILD Endpoint Health Check\n\n", .{});
    
    const replica_addresses = [_][]const u8{ "127.0.0.1", "127.0.0.1", "127.0.0.1" };
    const replica_ports = [_]u16{ 9002, 9003, 9004 };
    
    const config = client_router.ClientRouter.Config{
        .primary_address = "127.0.0.1",
        .primary_port = 9001,
        .replica_addresses = replica_addresses[0..],
        .replica_ports = replica_ports[0..],
        .auth_secret = "defaultsecret", // In production, use a secure secret
    };
    
    var router = try client_router.ClientRouter.init(allocator, config);
    defer router.deinit();
    
    // Force immediate health check
    router.performHealthChecks();
    
    const stats = router.getStats();
    
    std.debug.print("Health Check Results:\n", .{});
    std.debug.print("  Primary ({s}:{}): {s}\n", .{ 
        config.primary_address, 
        config.primary_port, 
        if (stats.primary_healthy) "✅ HEALTHY" else "❌ UNHEALTHY" 
    });
    
    std.debug.print("  Replicas:\n", .{});
    for (replica_addresses, replica_ports, 0..) |address, port, i| {
        const is_healthy = i < stats.healthy_replicas;
        std.debug.print("    Replica {} ({s}:{}): {s}\n", .{ 
            i + 1, 
            address, 
            port, 
            if (is_healthy) "✅ HEALTHY" else "❌ UNHEALTHY" 
        });
    }
    
    std.debug.print("\nSummary: {}/{} endpoints healthy\n", .{ 
        @as(u32, if (stats.primary_healthy) @as(u32, 1) else @as(u32, 0)) + stats.healthy_replicas,
        stats.total_replicas + 1 
    });
}

fn showRouterStats(allocator: std.mem.Allocator) !void {
    std.debug.print("📊 WILD Router Statistics\n\n", .{});
    
    const replica_addresses = [_][]const u8{ "127.0.0.1", "127.0.0.1" };
    const replica_ports = [_]u16{ 9002, 9003 };
    
    const config = client_router.ClientRouter.Config{
        .primary_address = "127.0.0.1",
        .primary_port = 9001,
        .replica_addresses = replica_addresses[0..],
        .replica_ports = replica_ports[0..],
        .auth_secret = "defaultsecret", // In production, use a secure secret
        .max_connections_per_endpoint = 10,
    };
    
    var router = try client_router.ClientRouter.init(allocator, config);
    defer router.deinit();
    
    const stats = router.getStats();
    
    std.debug.print("Configuration:\n", .{});
    std.debug.print("  Primary endpoint: {s}:{}\n", .{ config.primary_address, config.primary_port });
    std.debug.print("  Replica endpoints: {}\n", .{stats.total_replicas});
    std.debug.print("  Max connections per endpoint: {}\n", .{config.max_connections_per_endpoint});
    std.debug.print("  Health check interval: {}ms\n", .{config.health_check_interval_ms});
    
    std.debug.print("\nCurrent Status:\n", .{});
    std.debug.print("  Primary healthy: {s}\n", .{if (stats.primary_healthy) "yes" else "no"});
    std.debug.print("  Healthy replicas: {}\n", .{stats.healthy_replicas});
    std.debug.print("  Total replicas: {}\n", .{stats.total_replicas});
    std.debug.print("  Connection failures: {}\n", .{stats.total_failures});
    std.debug.print("  Active connections: {}\n", .{stats.active_connections});
    
    const health_percentage = if (stats.total_replicas > 0)
        (@as(f32, @floatFromInt(stats.healthy_replicas)) / @as(f32, @floatFromInt(stats.total_replicas))) * 100.0
    else
        0.0;
    
    std.debug.print("\nHealth Summary:\n", .{});
    std.debug.print("  Overall replica health: {d:.1}%\n", .{health_percentage});
    
    if (stats.primary_healthy and stats.healthy_replicas > 0) {
        std.debug.print("  Status: ✅ Fully operational\n", .{});
    } else if (stats.primary_healthy) {
        std.debug.print("  Status: ⚠️  Primary only (no replicas)\n", .{});
    } else {
        std.debug.print("  Status: ❌ Service degraded\n", .{});
    }
}