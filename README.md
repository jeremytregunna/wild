# WILD Database

**Cache-Resident Ultra-High-Performance Key-Value Store**

WILD (Within-cache Incredibly Lightweight Database) is a demonstration of storing an entire database in CPU L3 cache memory to achieve sub-microsecond latencies and extreme throughput. This project showcases how modern CPU cache hierarchies can be leveraged for ultra-low-latency data storage.

## Architecture

WILD is designed for modern x86-64 processors with the following characteristics:

- **Cache-Line Aligned Storage**: All records fit exactly in 64-byte cache lines
- **NUMA Awareness**: Automatically detects and utilizes L3 cache topology
- **Flat Hash Storage**: Linear probing hash table with no artificial capacity limits
- **Zero Runtime Allocation**: Static allocator prevents allocation after initialization
- **CPU Topology Detection**: Uses CPUID and `/sys/devices` to optimize for actual hardware

As a result, works best on Linux, but changing how caches are discovered to use cpuid could support other OSes.

### Supported Architectures

Only tested on an x86-64 system (AMD). Needs L3 cache, and the more you have, the more capacity the database will offer you.

## Goals

1. **Sub-microsecond latencies**: Target <1μs for reads/writes
2. **Cache-resident storage**: Entire database fits in L3 cache
3. **NUMA optimization**: Leverage CPU topology for performance
4. **Zero-copy operations**: Direct cache-line access without serialization
5. **Demonstrate L3 cache potential**: Show what's possible with modern CPUs

## How It Achieves Performance

### 1. Cache-Line Optimization
```
Record Structure (64 bytes = 1 cache line):
├── Metadata (4 bytes: 1 valid bit + 6 length bits + 25 reserved)
├── Key (8 bytes)
└── Data Payload (52 bytes)
```

### 2. Flat Hash Storage

WILD uses a flat hash table with linear probing that eliminates bucket overflow through bitwise operations. The implementation employs a unified hash function with compile-time dispatch, using Wyhash for string keys and MurmurHash3 for integer keys to ensure optimal distribution. By enforcing power-of-2 capacity constraints, the system replaces expensive modulo operations with fast bitwise AND operations during probe sequences. All memory is cache-aligned to ensure optimal CPU access patterns, and the system maintains O(1) performance by avoiding tombstone compaction—deleted slots remain available for immediate reuse without requiring table reorganization.

### 3. NUMA-Aware Memory Placement

WILD automatically detects CPU topology and L3 cache configuration by analyzing `/sys/devices/system/cpu` to understand the underlying hardware architecture. The system places data structures in NUMA-local memory domains and optimizes allocation patterns based on physical cores rather than SMT siblings. This topology-aware approach ensures that memory access patterns align with the CPU's cache hierarchy, minimizing cross-NUMA penalties and maximizing cache locality for optimal performance.

### 4. Static Memory Management

WILD employs a three-state static allocator that transitions through init, static, and deinit states to eliminate runtime memory allocation overhead. During the initialization phase, all required memory is allocated from arena-backed storage pools. Once transitioned to the static phase, no further allocations are permitted, ensuring zero allocation overhead during database operations. This approach prevents memory fragmentation, eliminates garbage collection pauses, and provides predictable cleanup through arena deallocation, making it ideal for latency-sensitive applications.

## Performance Characteristics

### Benchmark Results (Ryzen 9 7800X3D (8-core) x86-64, 96MB L3 Cache)

| Operation Type | Latency | Throughput | vs Redis |
|---|---|---|---|
| **Single Read** | 3ns | 349.0M ops/sec | 8726x faster |
| **Single Write** | 42ns | 23.6M ops/sec | 1180x faster |
| **Batch Read** | 2ns per op | 620.1M ops/sec | - |
| **Batch Write** | 28ns per op | 36.3M ops/sec | - |

### Mixed Workload Performance

| Metric | Value |
|---|---|
| **Total Throughput** | 615.5M ops/sec |
| **Per-Core Performance** | 76.9M ops/sec/core |
| **Target Achievement** | 769.4% of 10M ops/sec/core |
| **Database Utilization** | 19.1% (200,512/1,048,576 records) |

### System Configuration

| Component | Specification |
|---|---|
| **L3 Cache Size** | 96 MB (detected) |
| **Record Capacity** | 1,048,576 records |
| **Physical Cores** | 8 |
| **Cache Line Size** | 64 bytes |
| **NUMA Domains** | 1 |

### Closing

I will fully admit, these numbers are crazy stupid, run the benchmark on your system and see how fast it is to verify. These numbers are for a Ryzen 9 7800X3D (8-core) CPU with 96MB of L3 cache. You may need to lower the size if your system has less L3 cache.

It also must be noted, that in its current state, data is never saved to the disk, and may get be evacuated from L3 cache by the OS. If that happens, there will be a drop in performance.

Again, this is not a **durable database** it is only a **demonstration**.

## Usage

### CLI Interface
```bash
$ ./main
WILD CLI
Detected: 96 MB L3 cache, 1048576 records capacity
Type 'help' for commands

> set name "John Doe"
OK
> get name
"John Doe"
> stats
Database Statistics:
- Used capacity: 1/1048576 (0.0%)
- Optimal batch size: 256
- Physical cores: 8
- Cache line size: 64 bytes
```

### Benchmark Suite
```bash
$ ./benchmark
# Runs comprehensive performance tests including:
# - Single operation latency tests
# - Batch operation throughput tests
# - Mixed workload stress tests
# - Hardware topology analysis
```

## Building

```bash
# Build CLI
zig build-exe src/main.zig -O ReleaseSafe

# Build benchmark (optimized for maximum performance)
zig run benchmark.zig -O ReleaseFast -fstrip

# Run tests
zig build test
```

## License

MIT License

Copyright (c) 2024 Jeremy Tregunna <jeremy@tregunna.ca>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
