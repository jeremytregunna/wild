# WILD Database

**Cache-Resident Ultra-High-Performance Key-Value Store with Authentication & Replication**

WILD (Within-cache Incredibly Lightweight Database) is a production-ready high-performance key-value store designed to reside entirely in CPU L3 cache memory, achieving sub-microsecond latencies with comprehensive security and replication features.

[![Performance](https://img.shields.io/badge/Latency-42ns_writes-brightgreen)](docs/PERFORMANCE.md)
[![Security](https://img.shields.io/badge/Security-Authenticated-blue)](docs/SECURITY.md)
[![Replication](https://img.shields.io/badge/Replication-Primary%2FReplica-orange)](docs/REPLICATION.md)

## Quick Start

```bash
# 1. Build WILD
zig build

# 2. Start the database. It serves clients on --port (default 7878).
./zig-out/bin/wild --port 7878 --capacity 100000

# 3. Store and read a key. The value goes to stdout.
./zig-out/bin/wild-client 127.0.0.1:7878 set greeting hello
./zig-out/bin/wild-client 127.0.0.1:7878 get greeting   # prints: hello

# 4. Stop it. An idle daemon exits within about a second.
kill -TERM <pid>
```

To check the whole thing without starting anything by hand, run
`./scripts/verify`. It builds, runs the unit tests, type-checks code that
nothing calls, and starts a real server to drive a real client over TCP.

## Performance Characteristics

Built in ReleaseFast, the dev machine (Ryzen 7 7800X3D, 8 cores, 96 MB L3, 64-byte lines).

Two table sizes: 128 MB (80 MB actually touched, fits L3) and 512 MB (4.2 GB... no: 512 MB table, 5,033,164 keys, far past L3).

 Storage, per key:

| phase                     | L3-resident       | DRAM-resident     | DRAM IPC |
|---------------------------|-------------------|-------------------|----------|
| write                     | 34.1 ns · 29.3M/s | 45.4 ns · 22.0M/s | 0.44     |
| read (one key at a time)  | 18.0 ns · 55.5M/s | 29.0 ns · 34.5M/s | 0.24     |
| read, random order        | 31.4 ns · 31.9M/s | 51.9 ns · 19.3M/s | 0.22     |
| read-batch (plain loop)   | 21.0 ns · 47.5M/s | 32.1 ns · 31.2M/s | 0.28     |
| read-pipelined (prefetch) | 13.0 ns · 76.7M/s | 20.4 ns · 49.0M/s | 0.57     |
| mixed                     | 6.9 ns · 144M/s   | 10.0 ns · 99.6M/s | 1.40     |


**Key Metrics:**
- **Sub-microsecond latencies** for all operations
- **Cache-optimized storage** sized to fit entirely in L3 cache, while permitting larger capacities

## Architecture

WILD leverages modern CPU architecture for extreme performance:

### Core Design
- **Cache-Line Aligned Records**: 64-byte records fit exactly in CPU cache lines
- **Flat Hash Storage**: Linear probing with bitwise operations
- **Zone-Local Tables** ([docs/NUMA.md](docs/NUMA.md)): CPUID on every CPU the process may use reports cores, SMT siblings, L2/L3 domains, cache line size, and the NUMA zone each cache sits in, including two zones on one socket. Each zone gets its own table, bound to that zone's kernel node and first touched by a thread pinned to its cores, so a write does not cross a zone boundary. One zone means one table and no extra thread.
- **Zero Runtime Allocation**: Static allocator eliminates allocation overhead
- **One Owning Thread per Table**: A table is driven by a single thread. Replication and snapshot walks take a storage guard first, so a second thread cannot shift records under a reader.
- **Replication**: Primary-replica model for read scaling
- **Durability**: Write-ahead log with async batching
- **Snapshots**: Point-in-time recovery system
- **High Concurrency**: 100,000+ concurrent connections via io_uring (requires Linux)

## Security & Authentication

The server reads the first message on every connection as the shared secret and
answers with one byte. `wild-client` sends it for you:

```bash
# Server: authentication required
./zig-out/bin/wild --auth-secret "your-secure-secret-here" --port 7878

# Client: pass the secret, or set WILD_AUTH_SECRET
./zig-out/bin/wild-client --auth-secret "your-secure-secret-here" 127.0.0.1:7878 get name
WILD_AUTH_SECRET="your-secure-secret-here" ./zig-out/bin/wild-client 127.0.0.1:7878 get name
```

**How it behaves:**
- With `--auth-secret`, a client that sends no secret, or the wrong one, is
  refused. The secret comparison is constant-time.
- Without `--auth-secret`, authentication is off and every message is a command.
  That is the mode to use on a loopback interface for local work.
- Replication between nodes always needs a secret, and it needs `--enable-wal`.

See [docs/SECURITY.md](docs/SECURITY.md) for detailed security information.

## Replication & High Availability

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
- **Read Scaling**: Distribute read load across replicas
- **Real-time Sync**: WAL-based replication with minimal lag
- **Automatic Failover**: Client-side replica health monitoring

See [docs/REPLICATION.md](docs/REPLICATION.md) for replication setup and management.

## Usage Examples

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
# Key-value operations. The value goes to stdout, everything else to stderr.
./zig-out/bin/wild-client 127.0.0.1:7878 set name "John Doe"
./zig-out/bin/wild-client 127.0.0.1:7878 get name
./zig-out/bin/wild-client 127.0.0.1:7878 delete name

# Against a server that has a secret
./zig-out/bin/wild-client --auth-secret s3cret 127.0.0.1:7878 get name

# Example and end-to-end programs
./zig-out/bin/wild-client-example health
./zig-out/bin/e2e-server 7878 testsecret &
./zig-out/bin/e2e-client 127.0.0.1:7878 testsecret
```

`wild-client` exit status: `0` success, `1` the request failed, `2` bad usage.
So a shell script can trust it.

## 🛠️ Build & Installation

### Prerequisites
- Zig 0.17.0 — the code needs `std.Io`, so it does not build on 0.14 or 0.15.
  See `docs/MIGRATION_0.17.md`.
- Linux x86-64 (other platforms unsupported, PRs welcome)
- CPU with L3 cache (more cache = higher capacity)
  - Storage space sizing is based on 75% of available L3 cache, divided by one cache line (64 bytes) then rounded down to the largest power of two that fits.
    - e.g., 96MB L3 cache means 72MB / 64 bytes = 1,179,648 records; rounded down to power of 2 is 1,048,576 records (2^20). Means space occupied in memory for data is 64MB.

### Building
```bash
# Build all binaries
zig build

# Build with optimizations
zig build -Doptimize=ReleaseFast

# Available binaries after build:
# - ./zig-out/bin/wild                (database daemon and client server)
# - ./zig-out/bin/wild-client         (command line client)
# - ./zig-out/bin/wild-client-example (example client)
# - ./zig-out/bin/benchmark           (throughput benchmark)
# - ./zig-out/bin/e2e-server          (real server, for the end-to-end test)
# - ./zig-out/bin/e2e-client          (drives e2e-server over TCP)
# - ./zig-out/bin/e2e-concurrency     (disconnects must not steal another
#                                      connection's answer)
# - ./zig-out/bin/e2e-pipelined       (256 connections served from batched
#                                      lookups)
```

### Testing
```bash
# The green light: build, 18 unit tests, deep type-check, four end-to-end tests
./scripts/verify

# Fast loop while you work: the same without the ReleaseFast build
./scripts/test

# Throughput, at a table that fits in L3 and one that does not
zig build -Doptimize=ReleaseFast
./zig-out/bin/benchmark --compare

# How many keys the server groups per drain, and what fill fraction costs
./zig-out/bin/e2e-pipelined
./zig-out/bin/benchmark --mode probes --load-factor 0.95 --capacity 8388608
```

Batch reads prefetch the home cache line of every key in a group, which is
35% faster per key than a plain loop when the table is larger than L3. The
server groups the requests that arrive together and looks them up in one batch.
Measurements, and the limits of both claims: [docs/bench/README.md](bench/README.md).

## Documentation

| Document | Description |
|----------|-------------|
| [docs/WIRE_PROTOCOL.md](docs/WIRE_PROTOCOL.md) | Binary protocol specification |
| [docs/REPLICATION.md](docs/REPLICATION.md) | Replication setup and management |
| [docs/SECURITY.md](docs/SECURITY.md) | Authentication and security features |
| [docs/WAL.md](docs/WAL.md) | Write-ahead log design and guarantees |
| [docs/NUMA.md](docs/NUMA.md) | Zones, tables, and placement |
| [docs/API.md](docs/API.md) | Client API reference |

## Use Cases

WILD is optimized for specific high-performance scenarios:

### Perfect For
- **Cache-like workloads**: Session stores, temporary data
- **Read-heavy applications**: Content delivery, catalog data
- **Ultra-low latency**: Trading systems, real-time analytics
- **High-throughput services**: Ad serving, recommendation engines
- **Small datasets**: You're limited to 52 bytes of data for the data you store in each record

### Not Suitable For
- **Large datasets**: Must fit entirely in L3 cache
- **Complex queries**: Key-value operations only
- **ACID transactions**: Single-operation/batch consistency only
- **Cross-platform**: Linux x86-64 supported

## Performance Tuning

### Capacity Planning
```bash
# Check L3 cache size
./zig-out/bin/wild --help  # Shows detected cache size

# Set capacity
./zig-out/bin/wild --capacity 500000  # ~32MB for 500k records
```

### Hardware Optimization
- **L3 Cache**: More L3 cache = higher database capacity
- **Memory**: Ensure sufficient RAM for WAL and OS buffers
- **CPU**: Higher core count improves concurrent throughput
- **Storage**: NVMe SSD recommended for WAL files

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

See our [contribution guidelines](CONTRIBUTING.md) for more information.

## Support

- **Issues**: [GitHub Issues](https://github.com/jeremytregunna/wild/issues)
- **Documentation**: See `docs/` directory
- **Performance**: Run benchmarks on your hardware
