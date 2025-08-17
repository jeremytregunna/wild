# WILD Database

**Cache-Resident Ultra-High-Performance Key-Value Store with Authentication & Replication**

WILD (Within-cache Incredibly Lightweight Database) is a production-ready high-performance key-value store designed to reside entirely in CPU L3 cache memory, achieving sub-microsecond latencies with comprehensive security and replication features.

[![Performance](https://img.shields.io/badge/Latency-42ns_writes-brightgreen)](docs/PERFORMANCE.md)
[![Security](https://img.shields.io/badge/Security-Authenticated-blue)](docs/SECURITY.md)
[![Replication](https://img.shields.io/badge/Replication-Primary%2FReplica-orange)](docs/REPLICATION.md)

## 🚀 Quick Start

```bash
# 1. Build WILD
zig build

# 2. Start primary database with authentication
./zig-out/bin/wild --auth-secret mysecret --enable-wal --wal-path primary.wal

# 3. Connect with client
./zig-out/bin/wild-client-example health
```

## 📊 Performance Characteristics

### Benchmark Results (Ryzen 9 7800X3D, 96MB L3 Cache)

| Operation | Latency | Throughput |
|-----------|---------|------------|
| **Single Read** | 20ns/op | 349.0M ops/sec |
| **Single Write** | 42ns/op | 23.6M ops/sec |
| **Batch Operations** | 2-28ns/op | 620M+ ops/sec |

**Key Metrics:**
- **Sub-microsecond latencies** for all operations
- **620M+ ops/sec** total throughput
- **Cache-optimized storage** sized to fit entirely in L3 cache

## 🏗️ Architecture

WILD leverages modern CPU architecture for extreme performance:

### Core Design
- **Cache-Line Aligned Records**: 64-byte records fit exactly in CPU cache lines
- **Flat Hash Storage**: Linear probing with bitwise operations
- **NUMA Awareness**: Automatic CPU topology detection and optimization
- **Zero Runtime Allocation**: Static allocator eliminates allocation overhead
- **Lock-Free Operations**: Atomic primitives for concurrent access
- **Replication**: Primary-replica model for read scaling
- **Durability**: Write-ahead log with async batching
- **Snapshots**: Point-in-time recovery system
- **High Concurrency**: 100,000+ concurrent connections via io_uring (requires Linux)

## 🔐 Security & Authentication

WILD includes a simple authentication system:

```bash
# Start database with authentication required
./zig-out/bin/wild --auth-secret "your-secure-secret-here" --port 7878

# All clients must authenticate
./zig-out/bin/wild-client-example --auth-secret "your-secure-secret-here"
```

**Security Features:**
- ✅ Shared secret authentication for all connections
- ✅ Timing-attack resistant constant-time comparison
- ✅ Replication authentication (primary-replica security)
- ✅ Connection-level access control

See [docs/SECURITY.md](docs/SECURITY.md) for detailed security information.

## 🔄 Replication & High Availability

WILD supports primary-replica replication for read scaling:

### Primary Node
```bash
# Start as primary with replication enabled
./zig-out/bin/wild \
  --mode primary \
  --auth-secret mysecret \
  --enable-wal \
  --wal-path primary.wal \
  --replication-port 9001
```

### Replica Node
```bash
# Start as replica connecting to primary
./zig-out/bin/wild \
  --mode replica \
  --auth-secret mysecret \
  --primary-address 192.168.1.100 \
  --primary-port 9001
```

**Replication Features:**
- 📈 **Read Scaling**: Distribute read load across replicas
- 🔒 **Authenticated Replication**: Secure primary-replica communication
- ⚡ **Real-time Sync**: WAL-based replication with minimal lag
- 🎯 **Automatic Failover**: Client-side replica health monitoring

See [docs/REPLICATION.md](docs/REPLICATION.md) for replication setup and management.

## 📋 Usage Examples

### Standalone Database
```bash
# Basic standalone database
./zig-out/bin/wild --capacity 1000000 --auth-secret mysecret
```

### Production Primary-Replica Setup
```bash
# Primary (write node)
./zig-out/bin/wild \
  --mode primary \
  --capacity 2000000 \
  --auth-secret "production-secret" \
  --enable-wal \
  --wal-path /data/wild-primary.wal \
  --port 7878 \
  --replication-port 9001

# Replica 1 (read node)
./zig-out/bin/wild \
  --mode replica \
  --auth-secret "production-secret" \
  --primary-address primary.example.com \
  --primary-port 9001 \
  --port 7879

# Replica 2 (read node)
./zig-out/bin/wild \
  --mode replica \
  --auth-secret "production-secret" \
  --primary-address primary.example.com \
  --primary-port 9001 \
  --port 7880
```

### Client Usage
```bash
# Health check
./zig-out/bin/wild-client-example health

# Load testing
./zig-out/bin/wild-client-example load-test

# Replication testing
./zig-out/bin/wild-client-example replication-test
```

## 🛠️ Build & Installation

### Prerequisites
- Zig 0.14+
- Linux x86-64 (other platforms untested)
- CPU with L3 cache (more cache = higher capacity)

### Building
```bash
# Build all binaries
zig build

# Build with optimizations
zig build -Doptimize=ReleaseFast

# Available binaries after build:
# - ./zig-out/bin/wild              (main database daemon)
# - ./zig-out/bin/wild-client-example (example client)
```

### Testing
```bash
# Run performance benchmarks
zig run quick_benchmark.zig -O ReleaseFast
zig run performance_benchmark.zig -O ReleaseFast

# Test replication
zig run replication_test.zig -O ReleaseFast

# Test recovery
zig run recovery_benchmark.zig -O ReleaseFast
```

## 📚 Documentation

| Document | Description |
|----------|-------------|
| [docs/WIRE_PROTOCOL.md](docs/WIRE_PROTOCOL.md) | Binary protocol specification |
| [docs/REPLICATION.md](docs/REPLICATION.md) | Replication setup and management |
| [docs/SECURITY.md](docs/SECURITY.md) | Authentication and security features |
| [docs/PERFORMANCE.md](docs/PERFORMANCE.md) | Performance tuning and benchmarks |
| [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) | Production deployment guide |
| [docs/API.md](docs/API.md) | Client API reference |

## 🎯 Use Cases

WILD is optimized for specific high-performance scenarios:

### ✅ Perfect For
- **Cache-like workloads**: Session stores, temporary data
- **Read-heavy applications**: Content delivery, catalog data
- **Ultra-low latency**: Trading systems, real-time analytics
- **High-throughput services**: Ad serving, recommendation engines

### ❌ Not Suitable For
- **Large datasets**: Must fit entirely in L3 cache
- **Complex queries**: Key-value operations only
- **ACID transactions**: Single-operation consistency only
- **Cross-platform**: Linux x86-64 primarily supported

## 📈 Performance Tuning

### Capacity Planning
```bash
# Check L3 cache size
./zig-out/bin/wild --help  # Shows detected cache size

# Set capacity (records must fit in L3 cache)
./zig-out/bin/wild --capacity 500000  # ~32MB for 500k records
```

### Hardware Optimization
- **L3 Cache**: More L3 cache = higher database capacity
- **Memory**: Ensure sufficient RAM for WAL and OS buffers
- **CPU**: Higher core count improves concurrent throughput
- **Storage**: NVMe SSD recommended for WAL files

## ⚖️ License

MIT License - see [LICENSE](LICENSE) for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Run tests: `zig build test`
4. Submit a pull request

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/your-repo/wild/issues)
- **Documentation**: See `docs/` directory
- **Performance**: Run benchmarks on your hardware
- **Security**: Report security issues privately

---

**WILD Database** - Extreme performance meets production reliability.
