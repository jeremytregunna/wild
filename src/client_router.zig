const std = @import("std");
const wild = @import("wild.zig");
const wire_protocol = @import("wire_protocol.zig");

// Client-side router for load balancing reads across replicas
// Provides automatic failover and connection pooling
pub const ClientRouter = struct {
    allocator: std.mem.Allocator,
    primary_endpoint: Endpoint,
    replica_endpoints: []Endpoint,
    active_replicas: std.ArrayList(u32),
    next_replica_index: std.atomic.Value(u32),
    auth_secret: []const u8,
    
    // Connection pools
    primary_pool: ConnectionPool,
    replica_pools: []ConnectionPool,
    
    // Health monitoring
    health_check_interval_ms: u32,
    last_health_check: std.atomic.Value(i64),
    
    const Self = @This();
    
    pub const Endpoint = struct {
        address: []const u8,
        port: u16,
        is_healthy: std.atomic.Value(bool),
        last_error_time: std.atomic.Value(i64),
        consecutive_failures: std.atomic.Value(u32),
    };
    
    pub const ConnectionPool = struct {
        connections: std.ArrayList(Connection),
        max_connections: u32,
        active_connections: std.atomic.Value(u32),
        allocator: std.mem.Allocator,
        
        const Connection = struct {
            socket_fd: std.posix.fd_t,
            is_connected: std.atomic.Value(bool),
            last_used: std.atomic.Value(i64),
        };
        
        pub fn init(allocator: std.mem.Allocator, max_connections: u32) ConnectionPool {
            return ConnectionPool{
                .connections = std.ArrayList(Connection).init(allocator),
                .max_connections = max_connections,
                .active_connections = std.atomic.Value(u32).init(0),
                .allocator = allocator,
            };
        }
        
        pub fn deinit(self: *ConnectionPool) void {
            for (self.connections.items) |*conn| {
                if (conn.is_connected.load(.acquire)) {
                    std.posix.close(conn.socket_fd);
                }
            }
            self.connections.deinit();
        }
        
        pub fn getConnection(self: *ConnectionPool, endpoint: *const Endpoint, auth_secret: []const u8) !std.posix.fd_t {
            // Try to reuse existing connection
            for (self.connections.items) |*conn| {
                if (conn.is_connected.load(.acquire)) {
                    conn.last_used.store(std.time.timestamp(), .release);
                    return conn.socket_fd;
                }
            }
            
            // Create new connection if under limit
            if (self.active_connections.load(.acquire) < self.max_connections) {
                const socket_fd = try self.createConnection(endpoint, auth_secret);
                
                try self.connections.append(Connection{
                    .socket_fd = socket_fd,
                    .is_connected = std.atomic.Value(bool).init(true),
                    .last_used = std.atomic.Value(i64).init(std.time.timestamp()),
                });
                
                _ = self.active_connections.fetchAdd(1, .monotonic);
                return socket_fd;
            }
            
            return error.PoolExhausted;
        }
        
        fn createConnection(self: *ConnectionPool, endpoint: *const Endpoint, auth_secret: []const u8) !std.posix.fd_t {
            _ = self;
            const socket_fd = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
            errdefer std.posix.close(socket_fd);
            
            const addr = try std.net.Address.parseIp(endpoint.address, endpoint.port);
            try std.posix.connect(socket_fd, &addr.any, addr.getOsSockLen());
            
            // Authenticate with server
            try wire_protocol.WireProtocol.sendAuthRequest(socket_fd, auth_secret);
            const auth_success = try wire_protocol.WireProtocol.receiveAuthResponse(socket_fd);
            if (!auth_success) {
                return error.AuthenticationFailed;
            }
            
            return socket_fd;
        }
        
        pub fn releaseConnection(self: *ConnectionPool, socket_fd: std.posix.fd_t) void {
            for (self.connections.items) |*conn| {
                if (conn.socket_fd == socket_fd) {
                    conn.last_used.store(std.time.timestamp(), .release);
                    break;
                }
            }
        }
        
        pub fn closeConnection(self: *ConnectionPool, socket_fd: std.posix.fd_t) void {
            for (self.connections.items) |*conn| {
                if (conn.socket_fd == socket_fd) {
                    // Use CAS to atomically transition from connected to disconnected
                    // Only the first thread to successfully CAS will close the socket
                    if (conn.is_connected.cmpxchgWeak(true, false, .acq_rel, .acquire) == null) {
                        std.posix.close(socket_fd);
                        _ = self.active_connections.fetchSub(1, .release);
                    }
                    break;
                }
            }
        }
    };
    
    pub const Config = struct {
        primary_address: []const u8,
        primary_port: u16,
        replica_addresses: []const []const u8,
        replica_ports: []const u16,
        auth_secret: []const u8,
        max_connections_per_endpoint: u32 = 10,
        health_check_interval_ms: u32 = 30000, // 30 seconds
    };
    
    pub fn init(allocator: std.mem.Allocator, config: Config) !Self {
        std.debug.assert(config.replica_addresses.len == config.replica_ports.len);
        
        // Initialize primary endpoint
        const primary_endpoint = Endpoint{
            .address = try allocator.dupe(u8, config.primary_address),
            .port = config.primary_port,
            .is_healthy = std.atomic.Value(bool).init(true),
            .last_error_time = std.atomic.Value(i64).init(0),
            .consecutive_failures = std.atomic.Value(u32).init(0),
        };
        
        // Initialize replica endpoints
        const replica_endpoints = try allocator.alloc(Endpoint, config.replica_addresses.len);
        for (replica_endpoints, 0..) |*endpoint, i| {
            endpoint.* = Endpoint{
                .address = try allocator.dupe(u8, config.replica_addresses[i]),
                .port = config.replica_ports[i],
                .is_healthy = std.atomic.Value(bool).init(true),
                .last_error_time = std.atomic.Value(i64).init(0),
                .consecutive_failures = std.atomic.Value(u32).init(0),
            };
        }
        
        // Initialize connection pools
        const primary_pool = ConnectionPool.init(allocator, config.max_connections_per_endpoint);
        const replica_pools = try allocator.alloc(ConnectionPool, replica_endpoints.len);
        for (replica_pools) |*pool| {
            pool.* = ConnectionPool.init(allocator, config.max_connections_per_endpoint);
        }
        
        // Initialize active replicas list
        var active_replicas = std.ArrayList(u32).init(allocator);
        for (0..replica_endpoints.len) |i| {
            try active_replicas.append(@intCast(i));
        }
        
        return Self{
            .allocator = allocator,
            .primary_endpoint = primary_endpoint,
            .replica_endpoints = replica_endpoints,
            .active_replicas = active_replicas,
            .next_replica_index = std.atomic.Value(u32).init(0),
            .auth_secret = config.auth_secret,
            .primary_pool = primary_pool,
            .replica_pools = replica_pools,
            .health_check_interval_ms = config.health_check_interval_ms,
            .last_health_check = std.atomic.Value(i64).init(0),
        };
    }
    
    pub fn deinit(self: *Self) void {
        // Clean up connection pools
        self.primary_pool.deinit();
        for (self.replica_pools) |*pool| {
            pool.deinit();
        }
        self.allocator.free(self.replica_pools);
        
        // Clean up endpoints
        self.allocator.free(self.primary_endpoint.address);
        for (self.replica_endpoints) |*endpoint| {
            self.allocator.free(endpoint.address);
        }
        self.allocator.free(self.replica_endpoints);
        
        self.active_replicas.deinit();
    }
    
    // Route read operations to healthy replicas with load balancing
    pub fn routeRead(self: *Self, key: u64) !?[]const u8 {
        // Perform health checks if needed
        self.maybePerformHealthChecks();
        
        // Try replicas first (round-robin load balancing)
        if (self.active_replicas.items.len > 0) {
            const attempts = self.active_replicas.items.len;
            for (0..attempts) |_| {
                const replica_idx = self.getNextReplicaIndex();
                if (replica_idx < self.active_replicas.items.len) {
                    const replica_index = self.active_replicas.items[replica_idx];
                    if (self.tryReadFromReplica(replica_index, key)) |result| {
                        return result;
                    }
                }
            }
        }
        
        // Fallback to primary if all replicas failed
        return self.tryReadFromPrimary(key);
    }
    
    // Route write operations to primary only
    pub fn routeWrite(self: *Self, key: u64, data: []const u8) !void {
        return self.tryWriteToPrimary(key, data);
    }
    
    // Route delete operations to primary only  
    pub fn routeDelete(self: *Self, key: u64) !bool {
        return self.tryDeleteFromPrimary(key);
    }
    
    fn getNextReplicaIndex(self: *Self) u32 {
        if (self.active_replicas.items.len == 0) return 0;
        
        const current = self.next_replica_index.fetchAdd(1, .monotonic);
        return current % @as(u32, @intCast(self.active_replicas.items.len));
    }
    
    fn tryReadFromReplica(self: *Self, replica_index: u32, key: u64) ?[]const u8 {
        if (replica_index >= self.replica_endpoints.len) return null;
        
        const endpoint = &self.replica_endpoints[replica_index];
        if (!endpoint.is_healthy.load(.acquire)) return null;
        
        const pool = &self.replica_pools[replica_index];
        const socket_fd = pool.getConnection(endpoint, self.auth_secret) catch {
            self.markEndpointUnhealthy(endpoint);
            return null;
        };
        defer pool.releaseConnection(socket_fd);
        
        // Set timeout for operations
        wire_protocol.WireProtocol.setSocketTimeout(socket_fd, 5000) catch {
            self.markEndpointUnhealthy(endpoint);
            return null;
        };
        
        // Send read request
        wire_protocol.WireProtocol.sendReadRequest(socket_fd, key) catch {
            self.markEndpointUnhealthy(endpoint);
            return null;
        };
        
        // Receive response
        const result = wire_protocol.WireProtocol.receiveReadResponse(socket_fd, self.allocator) catch {
            self.markEndpointUnhealthy(endpoint);
            return null;
        };
        
        return result.data;
    }
    
    fn tryReadFromPrimary(self: *Self, key: u64) !?[]const u8 {
        const endpoint = &self.primary_endpoint;
        if (!endpoint.is_healthy.load(.acquire)) {
            return error.PrimaryUnhealthy;
        }
        
        const socket_fd = self.primary_pool.getConnection(endpoint, self.auth_secret) catch |err| {
            self.markEndpointUnhealthy(endpoint);
            return err;
        };
        defer self.primary_pool.releaseConnection(socket_fd);
        
        // Set timeout for operations
        try wire_protocol.WireProtocol.setSocketTimeout(socket_fd, 5000);
        
        // Send read request
        wire_protocol.WireProtocol.sendReadRequest(socket_fd, key) catch |err| {
            self.markEndpointUnhealthy(endpoint);
            return err;
        };
        
        // Receive response
        const result = wire_protocol.WireProtocol.receiveReadResponse(socket_fd, self.allocator) catch |err| {
            self.markEndpointUnhealthy(endpoint);
            return err;
        };
        
        return result.data;
    }
    
    fn tryWriteToPrimary(self: *Self, key: u64, data: []const u8) !void {
        const endpoint = &self.primary_endpoint;
        if (!endpoint.is_healthy.load(.acquire)) {
            return error.PrimaryUnhealthy;
        }
        
        const socket_fd = self.primary_pool.getConnection(endpoint, self.auth_secret) catch |err| {
            switch (err) {
                error.ConnectionRefused, error.NetworkUnreachable => {
                    self.markEndpointUnhealthy(endpoint);
                    return error.PrimaryUnhealthy;
                },
                error.PoolExhausted => return error.ConnectionPoolExhausted,
                else => {
                    self.markEndpointUnhealthy(endpoint);
                    return err;
                },
            }
        };
        defer self.primary_pool.releaseConnection(socket_fd);
        
        // Set timeout for operations
        try wire_protocol.WireProtocol.setSocketTimeout(socket_fd, 5000);
        
        // Send write request
        wire_protocol.WireProtocol.sendWriteRequest(socket_fd, key, data) catch |err| {
            self.markEndpointUnhealthy(endpoint);
            return err;
        };
        
        // Receive response
        const success = wire_protocol.WireProtocol.receiveWriteResponse(socket_fd) catch |err| {
            self.markEndpointUnhealthy(endpoint);
            return err;
        };
        
        if (!success) {
            return error.WriteFailed;
        }
    }
    
    fn tryDeleteFromPrimary(self: *Self, key: u64) !bool {
        const endpoint = &self.primary_endpoint;
        if (!endpoint.is_healthy.load(.acquire)) {
            return error.PrimaryUnhealthy;
        }
        
        const socket_fd = self.primary_pool.getConnection(endpoint, self.auth_secret) catch |err| {
            self.markEndpointUnhealthy(endpoint);
            return err;
        };
        defer self.primary_pool.releaseConnection(socket_fd);
        
        // Set timeout for operations
        try wire_protocol.WireProtocol.setSocketTimeout(socket_fd, 5000);
        
        // Send delete request
        wire_protocol.WireProtocol.sendDeleteRequest(socket_fd, key) catch |err| {
            self.markEndpointUnhealthy(endpoint);
            return err;
        };
        
        // Receive response
        const success = wire_protocol.WireProtocol.receiveDeleteResponse(socket_fd) catch |err| {
            self.markEndpointUnhealthy(endpoint);
            return err;
        };
        
        return success;
    }
    
    fn markEndpointUnhealthy(self: *Self, endpoint: *Endpoint) void {
        _ = self;
        endpoint.is_healthy.store(false, .release);
        endpoint.last_error_time.store(std.time.timestamp(), .release);
        _ = endpoint.consecutive_failures.fetchAdd(1, .monotonic);
    }
    
    fn maybePerformHealthChecks(self: *Self) void {
        const current_time = std.time.milliTimestamp();
        const last_check = self.last_health_check.load(.acquire);
        
        if (current_time - last_check >= self.health_check_interval_ms) {
            self.performHealthChecks();
            self.last_health_check.store(current_time, .release);
        }
    }
    
    pub fn performHealthChecks(self: *Self) void {
        // Check primary health
        if (self.checkEndpointHealth(&self.primary_endpoint)) {
            self.primary_endpoint.is_healthy.store(true, .release);
            self.primary_endpoint.consecutive_failures.store(0, .release);
        }
        
        // Check replica health and update active list
        self.active_replicas.clearRetainingCapacity();
        for (self.replica_endpoints, 0..) |*endpoint, i| {
            if (self.checkEndpointHealth(endpoint)) {
                endpoint.is_healthy.store(true, .release);
                endpoint.consecutive_failures.store(0, .release);
                self.active_replicas.append(@intCast(i)) catch {};
            }
        }
    }
    
    fn checkEndpointHealth(self: *Self, endpoint: *const Endpoint) bool {
        _ = self;
        // Simple TCP connection test
        const socket_fd = std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0) catch return false;
        defer std.posix.close(socket_fd);
        
        const addr = std.net.Address.parseIp(endpoint.address, endpoint.port) catch return false;
        
        // Set non-blocking and short timeout
        const flags = std.posix.fcntl(socket_fd, std.posix.F.GETFL, 0) catch return false;
        _ = std.posix.fcntl(socket_fd, std.posix.F.SETFL, flags | 0x800) catch return false; // O_NONBLOCK = 0x800
        
        // Attempt connection
        std.posix.connect(socket_fd, &addr.any, addr.getOsSockLen()) catch |err| {
            switch (err) {
                error.WouldBlock => {
                    // Connection in progress, wait briefly
                    std.time.sleep(100_000_000); // 100ms
                    return true; // Assume healthy if connection is progressing
                },
                else => return false,
            }
        };
        
        return true;
    }
    
    pub fn getStats(self: *const Self) RouterStats {
        var healthy_replicas: u32 = 0;
        var total_failures: u32 = 0;
        
        // Use relaxed ordering for statistics collection - no synchronization needed
        for (self.replica_endpoints) |*endpoint| {
            if (endpoint.is_healthy.load(.monotonic)) {
                healthy_replicas += 1;
            }
            total_failures += endpoint.consecutive_failures.load(.monotonic);
        }
        
        return RouterStats{
            .primary_healthy = self.primary_endpoint.is_healthy.load(.monotonic),
            .healthy_replicas = healthy_replicas,
            .total_replicas = @intCast(self.replica_endpoints.len),
            .total_failures = total_failures,
            .active_connections = self.countActiveConnections(),
        };
    }
    
    fn countActiveConnections(self: *const Self) u32 {
        // Use relaxed ordering for statistics - no synchronization needed
        var total: u32 = self.primary_pool.active_connections.load(.monotonic);
        for (self.replica_pools) |*pool| {
            total += pool.active_connections.load(.monotonic);
        }
        return total;
    }
    
    pub const RouterStats = struct {
        primary_healthy: bool,
        healthy_replicas: u32,
        total_replicas: u32,
        total_failures: u32,
        active_connections: u32,
    };
};