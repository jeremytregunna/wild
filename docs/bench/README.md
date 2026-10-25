# Batch read prefetching: what was measured

Date: 2026-10-25. Machine: AMD Ryzen 7 7800X3D, 8 cores, 96 MB L3, 64-byte
cache lines, Linux, `perf_event_paranoid=2`. Every number is from
`./zig-out/bin/benchmark` built with `zig build -Doptimize=ReleaseFast`.

## The answer

`readBatch` now prefetches. On a table larger than L3 it reads 37% faster per
key and moves 58% more keys per second.

| Table | Keys | Plain loop | Prefetching | Change |
|---|---|---|---|---|
| 128 MB (fits L3) | 1,258,291 | 23.1 ns/op, IPC 0.38 | 15.2 ns/op, IPC 0.78 | 34% faster |
| 512 MB (4x L3) | 5,033,164 | 32.5 ns/op, IPC 0.27 | 20.6 ns/op, IPC 0.56 | 37% faster |

Throughput at 512 MB: 30.7M reads/sec became 48.6M reads/sec.

The single-key read path and the write path did not change, which is the point:
a lookup that costs 20 ns cannot pay for a state machine, so nothing was added
to it.

| Phase | Before | After |
|---|---|---|
| `read` (one key at a time) | 20.7 ns/op | 20.8 ns/op |
| `write` | 38.7 ns/op | 38.1 ns/op |

## Why it works, and how much each part contributes

A lookup is a dependent chain: the hash gives an address, and only the line at
that address says whether the next line is needed. The hardware prefetcher
cannot predict that jump. So the fix is to have several independent lookups in
flight at once.

`--mode sweep` separates the two effects. Both phases read the same 5.03M keys
from the 512 MB table:

| Group size | ns/op | IPC | What is prefetched |
|---|---|---|---|
| plain loop | 32.5 | 0.27 | nothing |
| 1 | 29.8 | 0.37 | the next two lines of one chain |
| 4 | 23.3 | 0.53 | 4 home lines |
| **8** | **21.7** | **0.56** | **8 home lines** |
| 16 | 23.3 | 0.46 | 16 home lines |
| 32 | 24.4 | 0.43 | 32 home lines |
| 64 | 24.6 | 0.42 | 64 home lines |
| 128 | 25.0 | 0.41 | 128 home lines |
| 256 | 24.9 | 0.41 | 256 home lines |

Reading the table:

1. Prefetching within one chain is worth about 8% (32.5 to 29.8).
2. Batching home lines across keys is worth the rest (29.8 to 21.7).
3. Past 8 keys per group it gets worse and then flat. The position array stops
   fitting in L1, and the tail of a large group is resolved long after its
   lines arrive.

So `WILD.default_microbatch = 8`, chosen from this table. 4 and 16 are inside
noise of each other, so nothing hangs on the exact value.

## The win is not an access-order artefact

Batch phases read keys in array order, and the keys were written in that same
order. That could flatter the result. `--shuffle` reads them in random order
instead, from a fixed seed:

| Phase, 512 MB table, random order | ns/op | IPC |
|---|---|---|
| plain loop | 33.4 | 0.27 |
| prefetching, group 8 | 21.9 | 0.54 |

Same 34% gap. The effect comes from memory-level parallelism, not from
insertion order.

## What the counters mean, and one limitation

The benchmark opens its own counters with `perf_event_open`. No external
profiling tool is needed, and a locked-down kernel degrades to `n/a` instead of
failing the run. Counting excludes kernel time, which is what
`perf_event_paranoid=2` allows.

Two caveats you should know before quoting these numbers:

- **IPC is the usable signal.** A compute-bound loop on this machine counts
  above 2; the batch read before this change counted 0.27. Low IPC with a high
  cycle count means the cores are waiting on memory.
- **The miss counts are not usable.** The kernel does not map the generic LLC
  event on this CPU, so it reports `unavailable`. The generic `cache misses`
  event is accepted but undercounts by about four orders of magnitude (it
  reported 534 misses for 8M random reads over 128 MB). Accurate LLC counts on
  Zen 4 need raw IBBS events, which is a separate piece of work. Do not repeat
  the miss numbers as if they were LLC misses.

## Fill fraction costs more than prefetch does

The table has no load-factor limit. `findEmptySlot` refuses only at 100% full,
so the operator decides chain length by filling the database, and nothing warns
them when it gets expensive. `--mode probes` measures it:

| Fill | Mean lines per lookup | p90 | p99 | Longest chain | Keys needing >63 lines |
|---|---|---|---|---|---|
| 60% | 1.75 | 3 | 10 | 66 | 2 |
| 80% | 3.00 | 6 | 29 | 339 | 9,483 |
| 95% | 10.47 | 15 | **4,803** | 4,803 | 253,214 |

All from the same 512 MB table. The 1.75 mean at 60% matches the textbook value
for linear probing, `0.5 x (1 + 1/(1 - load))`, which says the measurement is
reading the real structure and not an artefact.

What that costs per key, same table:

| Fill | `read` | `read` prefetching | `write` | Reads/sec |
|---|---|---|---|---|
| 60% | 28.0 ns | 20.3 ns | 45.1 ns | 35.7M |
| 80% | 43.4 ns | 29.9 ns | 61.3 ns | 23.1M |
| 95% | 74.5 ns | 56.3 ns | 86.2 ns | 13.4M |

Two things follow.

**Observation.** At 95% fill, 1% of lookups walk about 4,800 cache lines. That
is roughly 0.1 ms for a single get, and prefetch cannot hide it: 4,800 loads in
a dependent chain are 4,800 waits, however many you have in flight.

**Interpretation.** The prefetch change is worth about 35% at every fill. A
load-factor policy is worth 2.7x at the top end. If the database is meant to be
filled near capacity, the policy matters more than the prefetch. The two are
independent, and both are cheap to build.

**Not decided.** A policy changes what the server does when the database is
nearly full, so it needs a decision from the owner. The two reasonable options:

1. Refuse writes above a threshold, for example 80%, and return a named error
   that says what to do. Simple, no data movement, and the operator sees a
   clear failure instead of a slow database.
2. Grow and rehash at a threshold. Keeps the operator from hitting a wall, but
   rehashing an 8M-record table is a pause worth measuring before it is
   promised, and it must be crash-safe against the WAL.

Recommendation: option 1 first, because it is small, it is reversible, and it
turns an invisible cliff into an error message. Option 2 only if filling the
database near capacity is a supported use.

## The server batches requests too

`WildServer` now collects the GETs it finds in one drain of the io_uring
completion queue and looks them up together, then posts every response. It never
waits for more requests: it batches only what has already arrived, so a lone
request is answered exactly as fast as before.

Measured with `./zig-out/bin/e2e-pipelined` — 256 connections, each with one
request in flight, on a 128 MB table:

    e2e-pipelined: 1024 GETs answered in 69 batches (average 14.8 per batch)

Across runs the average lands between 12 and 15, against a maximum of 64
(`max_batched_reads`). The test fails if the server never groups anything, so
this cannot rot into dead code unnoticed.

Two honest limits:

- **The end-to-end gain is smaller than the 35% at the storage layer.** Each
  request still costs its own `recv` and `send`, and those syscalls dominate a
  20 ns lookup. The batching removes memory stalls, not syscall overhead.
  Anyone claiming a 35% server-wide speedup from this change would be quoting
  the wrong table.
- **A connection can only have one request outstanding.** Each connection has
  one posted read, and the wire header carries no message length, so two
  messages delivered in one read cannot be split: the server hashes the tail as
  part of one key and answers `not found`. `tests/e2e_pipelined.zig` keeps one
  request outstanding per connection because of this, and
  `docs/MIGRATION_0.17.md` records the framing defect.

## Reproduce it

```bash
zig build -Doptimize=ReleaseFast

# the A/B table, at both sizes, in one run
./zig-out/bin/benchmark --compare

# the group-size sweep that picked 8
./zig-out/bin/benchmark --mode sweep --capacity 8388608 --keys 5033164

# the same A/B with the key order randomised
./zig-out/bin/benchmark --mode batch --shuffle --capacity 8388608 --keys 5033164
./zig-out/bin/benchmark --mode pipe --shuffle --capacity 8388608 --keys 5033164

# how many lookups the server groups per drain
./zig-out/bin/e2e-pipelined

# the chain-length distribution at whatever fill you want
./zig-out/bin/benchmark --mode probes --load-factor 0.95 --capacity 8388608
```

Captured output for this report: `default-2026-10-25.txt`,
`micro-sweep-2026-10-25.txt`, `random-order-2026-10-25.txt`,
`probe-chains-2026-10-25.txt`, and
`baseline-before-prefetch-2026-10-25.txt` from before the change.

## What this does not claim

- **The server batches what already arrived; it does not wait for more.** That
  keeps single-request latency unchanged. Getting more requests per drain means
  fixing message framing first, so one connection can pipeline.
- **No load-factor policy exists yet.** The numbers above are at 60% fill,
  which is what the benchmark defaults to. The fill experiment is in the
  previous section, and the decision it needs is spelled out there.
- **Single-key reads were not touched**, on purpose.
- **`readBatch` still falls back to the plain loop below 8 keys**
  (`WILD.pipelined_threshold`), where the setup costs more than it saves.

## Correctness

`tests/test_batch_read.zig` pins that a batch returns exactly what single reads
return: across group boundaries, on probe chains longer than one cache line
(keys that share a home slot, found by hashing), on an empty table, and on a
`results` slice longer than `keys`. 6 tests, run by `zig build test`.
