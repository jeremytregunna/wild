# WILD Replication Guide

This document covers WILD's replication system for read scaling and high availability.

## Replication Overview

WILD implements a primary-replica replication model designed for read scaling while maintaining the extreme performance characteristics of the single-node database.

### Architecture
- **Primary Node**: Handles all writes, replicates to replicas
- **Replica Nodes**: Handle reads, receive updates from primary
- **WAL-Based Replication**: Real-time streaming of write-ahead log batches
- **Authenticated Communication**: All replication traffic is authenticated
- **Client-Side Load Balancing**: Automatic replica selection and failover

## Quick Start

### 1. Start Primary Node
```bash
./zig-out/bin/wild \
  --mode primary \
  --auth-secret "mysecret" \
  --enable-wal \
  --wal-path primary.wal \
  --port 7878 \
  --replication-port 9001
```

### 2. Start Replica Node
```bash
./zig-out/bin/wild \
  --mode replica \
  --auth-secret "mysecret" \
  --primary-address 127.0.0.1 \
  --primary-port 9001 \
  --port 7879
```

### 3. Use Client with Load Balancing
```bash
# Client automatically uses primary for writes, replicas for reads
./zig-out/bin/wild-client-example load-test
```

## Replication Components

### Primary Node
**Role**: Write node, replication source
- Accepts all write operations (SET, DELETE)
- Streams WAL batches to connected replicas
- Maintains replica health and connection state
- Handles replica catch-up for lagging nodes

### Replica Node  
**Role**: Read node, replication target
- Accepts read operations (GET)
- Receives and applies WAL batches from primary
- Maintains connection to primary with health monitoring
- Supports automatic reconnection and catch-up

### Client Router
**Role**: Intelligent load balancer
- Routes writes to primary node
- Routes reads to healthy replica nodes (round-robin)
- Monitors replica health with automatic failover
- Falls back to primary if all replicas fail

## Detailed Configuration

### Primary Node Configuration

```bash
./zig-out/bin/wild \
  --mode primary \                    # Enable primary mode
  --auth-secret "production-secret" \ # Authentication secret
  --capacity 2000000 \               # Database capacity
  --enable-wal \                     # Required for replication
  --wal-path /data/primary.wal \     # WAL file location
  --port 7878 \                      # Client connection port
  --bind-address 0.0.0.0 \          # Bind to all interfaces
  --replication-port 9001            # Replica connection port
```

**Primary-Specific Options:**
- `--replication-port`: Port for replica connections (required)
- `--enable-wal`: WAL required for replication (automatic check)

### Replica Node Configuration

```bash
./zig-out/bin/wild \
  --mode replica \                      # Enable replica mode
  --auth-secret "production-secret" \   # Same secret as primary
  --primary-address primary.example.com \  # Primary server address
  --primary-port 9001 \                 # Primary replication port
  --port 7879 \                         # Client connection port (unique)
  --bind-address 0.0.0.0               # Bind to all interfaces
```

**Replica-Specific Options:**
- `--primary-address`: Hostname/IP of primary node (required)
- `--primary-port`: Primary's replication port (required)
- `--port`: Unique port for this replica's client connections

### Client Configuration

Clients use a configuration that includes all database nodes:

```zig
const config = client_router.ClientRouter.Config{
    .primary_address = "primary.example.com",
    .primary_port = 7878,
    .replica_addresses = &[_][]const u8{
        "replica1.example.com",
        "replica2.example.com", 
        "replica3.example.com"
    },
    .replica_ports = &[_]u16{ 7879, 7880, 7881 },
    .auth_secret = "production-secret",
    .max_connections_per_endpoint = 10,
    .health_check_interval_ms = 30000,
};
```

## Production Deployment

### Three-Node Setup Example

#### Primary Node (primary.example.com)
```bash
./zig-out/bin/wild \
  --mode primary \
  --auth-secret "$(cat /etc/wild/secret)" \
  --capacity 4000000 \
  --enable-wal \
  --wal-path /data/wild/primary.wal \
  --port 7878 \
  --bind-address 0.0.0.0 \
  --replication-port 9001
```

#### Replica 1 (replica1.example.com)
```bash
./zig-out/bin/wild \
  --mode replica \
  --auth-secret "$(cat /etc/wild/secret)" \
  --primary-address primary.example.com \
  --primary-port 9001 \
  --port 7879 \
  --bind-address 0.0.0.0
```

#### Replica 2 (replica2.example.com)
```bash
./zig-out/bin/wild \
  --mode replica \
  --auth-secret "$(cat /etc/wild/secret)" \
  --primary-address primary.example.com \
  --primary-port 9001 \
  --port 7880 \
  --bind-address 0.0.0.0
```

### Network Topology

```
                     ┌─────────────────┐
                     │  Load Balancer  │
                     │   (Optional)    │
                     └─────────────────┘
                              │
                ┌─────────────┼─────────────┐
                │             │             │
        ┌───────▼──────┐ ┌────▼────┐ ┌─────▼─────┐
        │   Primary    │ │ Replica │ │ Replica  │
        │ :7878 (RW)   │ │ :7879(R)│ │ :7880(R) │
        └──────────────┘ └─────────┘ └───────────┘
                │              ▲            ▲
                │              │            │
                └──── Replication ────────────
                      :9001
```

### Systemd Services

#### Primary Service (`/etc/systemd/system/wild-primary.service`)
```ini
[Unit]
Description=WILD Database Primary Node
After=network.target

[Service]
Type=simple
User=wild
Group=wild
ExecStart=/usr/local/bin/wild \
  --mode primary \
  --auth-secret-file /etc/wild/secret \
  --capacity 4000000 \
  --enable-wal \
  --wal-path /data/wild/primary.wal \
  --port 7878 \
  --replication-port 9001
Restart=always
RestartSec=5
WorkingDirectory=/data/wild

[Install]
WantedBy=multi-user.target
```

#### Replica Service (`/etc/systemd/system/wild-replica.service`)
```ini
[Unit]
Description=WILD Database Replica Node
After=network.target

[Service]
Type=simple
User=wild
Group=wild
ExecStart=/usr/local/bin/wild \
  --mode replica \
  --auth-secret-file /etc/wild/secret \
  --primary-address primary.example.com \
  --primary-port 9001 \
  --port 7879
Restart=always
RestartSec=5
Environment=WILD_REPLICA_ID=replica1

[Install]
WantedBy=multi-user.target
```

## Replication Features

### Real-Time Synchronization
- **WAL Streaming**: Primary streams write-ahead log batches to replicas
- **Batched Updates**: Multiple operations bundled for efficiency
- **Minimal Lag**: Typically microsecond-level replication delay
- **Ordered Delivery**: Operations applied in same order as primary

### Automatic Replica Management
- **Health Monitoring**: Continuous replica health checks
- **Automatic Reconnection**: Replicas reconnect on connection loss
- **Catch-Up Protocol**: Lagging replicas automatically sync missing data
- **Bootstrap Support**: New replicas can join existing clusters

### Client-Side Intelligence
- **Read Load Balancing**: Round-robin distribution across healthy replicas
- **Automatic Failover**: Seamless failover when replicas become unhealthy
- **Primary Fallback**: Reads fall back to primary if all replicas fail
- **Connection Pooling**: Efficient connection reuse for performance

## Monitoring and Management

### Health Monitoring

#### Server-Side Monitoring
```bash
# Check primary replication status
curl -s http://primary.example.com:7878/stats

# Monitor replica connections
netstat -tn | grep :9001

# Check WAL file growth
ls -lah /data/wild/primary.wal
```

#### Client-Side Monitoring
```bash
# Test replica health
./zig-out/bin/wild-client-example health

# Load test with replication
./zig-out/bin/wild-client-example replication-test
```

### Performance Metrics

#### Replication Lag
- **Measurement**: Difference between primary and replica batch IDs
- **Target**: < 1ms under normal load
- **Monitoring**: Built into client router statistics

#### Throughput Impact
- **Primary overhead**: ~5% performance impact for replication
- **Replica performance**: 95% of primary read performance
- **Network bandwidth**: Proportional to write volume

### Troubleshooting

#### Common Issues

**Replica Connection Failures**
```bash
# Check network connectivity
telnet primary.example.com 9001

# Verify authentication
grep "auth" /var/log/wild/replica.log

# Check primary replication port binding
netstat -ln | grep :9001
```

**Replication Lag**
```bash
# Check network latency
ping primary.example.com

# Monitor WAL batch rates
tail -f /var/log/wild/primary.log | grep "batch"

# Check replica catch-up status
grep "catch-up" /var/log/wild/replica.log
```

**Client Failover Issues**
```bash
# Test individual endpoints
./zig-out/bin/wild-client-example --endpoint replica1.example.com:7879 health
./zig-out/bin/wild-client-example --endpoint replica2.example.com:7880 health

# Check client router configuration
./zig-out/bin/wild-client-example config-test
```

## Operational Procedures

### Adding New Replica

1. **Prepare replica server**
   ```bash
   # Install WILD binary
   # Configure authentication secret
   # Set up systemd service
   ```

2. **Start replica with primary connection**
   ```bash
   ./zig-out/bin/wild \
     --mode replica \
     --auth-secret "$(cat /etc/wild/secret)" \
     --primary-address primary.example.com \
     --primary-port 9001 \
     --port 7881
   ```

3. **Update client configurations**
   ```bash
   # Add new replica to client router config
   # Deploy updated client configurations
   ```

### Removing Replica

1. **Remove from client configurations**
   ```bash
   # Update client router config
   # Deploy updated configurations
   ```

2. **Gracefully stop replica**
   ```bash
   systemctl stop wild-replica
   ```

3. **Clean up replica data** (if desired)
   ```bash
   rm -rf /data/wild/replica/
   ```

### Primary Failover (Manual)

WILD currently requires manual failover procedures:

1. **Stop primary node**
   ```bash
   systemctl stop wild-primary
   ```

2. **Promote replica to primary**
   ```bash
   # Stop replica
   systemctl stop wild-replica
   
   # Reconfigure as primary
   ./zig-out/bin/wild \
     --mode primary \
     --auth-secret "$(cat /etc/wild/secret)" \
     --enable-wal \
     --wal-path /data/wild/new-primary.wal \
     --port 7878 \
     --replication-port 9001
   ```

3. **Update client configurations**
   ```bash
   # Point clients to new primary
   # Update replica configurations
   ```

## Performance Considerations

### Replication Impact
- **Primary overhead**: ~5% performance reduction with replication enabled
- **Network bandwidth**: Proportional to write rate
- **Memory usage**: Additional connection buffers for each replica

### Scaling Guidelines
- **Read scaling**: Add replicas to distribute read load
- **Write scaling**: Single primary limits write throughput
- **Network capacity**: Ensure adequate bandwidth for replication traffic

### Optimal Deployment
- **Same datacenter**: Minimize replication latency
- **Private network**: Use dedicated network for replication traffic
- **SSD storage**: Fast storage for WAL files improves replication performance

## Limitations

### Current Limitations
- **Single primary**: No multi-primary support
- **Manual failover**: No automatic primary promotion
- **No conflict resolution**: Split-brain scenarios require manual intervention
- **Memory-bound**: All nodes must fit data in L3 cache

### Planned Enhancements
- **Automatic failover**: Leader election and automatic promotion
- **Multi-datacenter replication**: Cross-datacenter deployment support
- **Read preferences**: Client-side read preference configuration
- **Replica priorities**: Weighted replica selection

---

This replication system enables WILD to scale read workloads while maintaining its extreme performance characteristics. For production deployments, carefully plan your network topology and monitoring strategy.