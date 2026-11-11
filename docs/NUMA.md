# WILD Database — Zones, Tables, and Placement

WILD runs one table per NUMA zone, keeps that table's memory on the zone, and
keeps one thread per table pinned to the zone. This document says how that is
detected, what a report line means, what an operator should see on different
machines, and where the current limits are.

## What is detected, and how

Detection is CPUID only. Nothing reads `/sys` or `/proc`, so the answer comes
from the CPUs themselves and cannot disagree with what a later `lscpu` says
after a re-read of the filesystem.

Every CPU the process may use is visited once:

1. the thread pins itself to that CPU,
2. it reads the topology leaves (`0x8000001E` for the zone, `0x8000001D` for
   cache geometry, `0x80000008` for core count),
3. it records the kernel's node number for that CPU with `getcpu`,
4. it restores the mask.

Visiting each CPU pinned is what makes the answer exact. A package id read
from one thread says nothing about which zone the *other* CPUs sit in.

Three things come out:

| Field | Meaning |
|---|---|
| `physical_cores` | CPUs at the front of the member list, one per physical core |
| `cache_size_kb` | that zone's L3 |
| `node_id` | the zone the **hardware** numbers |
| `os_node` | the node the **kernel** numbers |

The last two are not the same field twice. Hardware numbering comes from
CPUID. Kernel numbering comes from `getcpu`, and the kernel renumbers nodes
when NPS modes or cpusets change what it can see. Placement calls take the
kernel's number, so `os_node` is what the code uses; `node_id` is only for
reporting a human-readable zone.

Both can be null. Null means "not known", never a guess: if the CPUs of one
domain disagree, or the kernel will not say, the field stays empty. A table
that lands on the wrong node is worse than a table that is not placed, because
the wrong placement is invisible and permanent.

## What placement does

For each L3 domain:

1. **Pin.** The thread that builds the table pins itself to the domain's
   physical cores — not the SMT siblings. One owner per core, because an owner
   does not need to run twice at once and a sibling would share the core's
   cache with unrelated work.
2. **Bind.** The table's record array is its own allocation, so it can be bound
   to `os_node` with `MPOL_BIND` without touching a neighbour's pages.
3. **First touch.** The pinned thread then initialises the table in place, so
   the pages are faulted on the node the thread is running on.
4. **Restore.** The calling thread gets its original mask back. It keeps
   running other work afterwards and must not stay stuck on one zone.

If the machine has one zone there is one table and **no owner thread**: the
caller runs the operation directly. That keeps the single-zone path at the cost
it had before partitioning existed.

## How a key finds its table

Routing is a pure function of the key:

```
bucket = fmix64(key) & 0xFF      // 256 buckets, fixed
table  = bucket % table_count    // this node's count
```

The bucket is fixed and the table is derived from it, because three callers
must agree without sharing state: the request path, WAL replay after a crash,
and replica apply. They all see a key and nothing else. Anything hardware-
dependent in that rule would file a recovered key in a table the lookup path
never searches — present but invisible.

Two consequences worth knowing:

- **A node that recovers on different hardware is still correct.** It applies
  its own `table_count` to the same buckets, so it is self-consistent. Logs,
  snapshots, and replica streams need no partition field.
- **Capacity is split by halving.** Each table's capacity must be a power of
  two, because the slot arithmetic is bitwise. One piece becomes two halves, so
  the pieces still add up to the total (1024 over three tables is 512+256+256,
  not 3×256 = 768). Snapshots store the total, and restore refuses a mismatch,
  so this is what lets one snapshot be restored on a machine with a different
  zone count.

## Reading the report

`benchmark` prints this on every run, so that is the quickest way to see what a
machine reports. The daemon does not print it; `printHardwareReport` on the
database object does, and `benchmark` is the only binary that calls it today.
A small capacity is enough to see the report:

    ./zig-out/bin/benchmark --mode write --capacity 4096 --keys 1000

Output on a one-zone machine:

```
Table 0: 1000/4096 records, zone 0, kernel node 0, placed true, pinned true

=== WILD Hardware Report ===
Hardware Configuration:
- Physical cores: 8
- Cache line size: 64 bytes
- Optimal batch size: 256

Storage Statistics:
- Used capacity: 1000/4096 (24.4%)
NUMA zones: 1
  L3 domain 0: 16 CPUs (8 physical), L3 98304 KB, zone 0, kernel node 0, cpus 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
```

Per table:

| Field | Read it as |
|---|---|
| `zone` | the hardware zone of the domain this table belongs to (may be `null`) |
| `kernel node` | the node the pages **actually** landed on, read back from the first record |
| `placed` | `mbind` returned success; the pages are bound |
| `pinned` | the affinity call returned success for this table's owner |

`zone` and `kernel node` disagreeing is normal and not an error. On a
`numactl --cpunodebind=1` run, hardware still numbers the zone 1 while the
kernel sees one node, numbered 0. Placement follows the kernel.

`placed false` with `kernel node null` means the pages are unplaced: either the
machine has one node and there is nothing to bind to, or the zone could not be
proven. `pinned false` means the owner is not confined and may run anywhere.

## The cases you will see

**One zone.** The common case, and this machine: one table, no owner thread,
`placed true`, `pinned true`, `NUMA zones: 1`. Everything above is inert.

**Two zones on one socket.** `NUMA zones: 2`, two `L3 domain` lines with
different `zone` and `kernel node`, and two `Table` lines on different nodes.
This is the case the design exists for.

**One zone, more tables than zones.** `partition_override` forces a table count.
It exists for tests: a one-zone machine cannot reach the multi-table path any
other way, and that path is where a placement bug would hide. Both tables land
on the same node and pin to overlapping CPUs, which is correct but pointless —
it is not a way to gain performance.

**Degraded.** If the CPUs of one domain disagree about grouping (a partial
mask, an unusual APIC layout), the domains collapse to the single-domain
fallback, `node_id`/`os_node` go null, and the report shows one domain with a
null zone. The store keeps working; it just stops saying anything it cannot
prove.

## Operator actions

- **Restricting CPUs is supported and is the normal thing to do.** `taskset`,
  cpusets, and `numactl --cpunodebind` all change what the process may use.
  Detection probes only permitted CPUs, so a restricted process reports the
  zones it can actually reach. Restricting to part of one zone legitimately
  gives one domain.
- **Do not put a pinned owner on a core you also want for other work.** The
  owner takes one core per table for the process's life.
- **NPS mode changes renumber kernel nodes.** A restart after an NPS change
  re-detects, so nothing needs updating by hand; a *snapshot* taken before the
  change restores fine, because routing depends only on the key.
- **A replica may have a different zone count than its primary.** Routing stays
  self-consistent on both sides. The one visible difference: on a replica with
  a different count, a batch from the primary can span two local tables, and
  those two parts are applied in separate critical sections. Reader visibility
  is then per table, not per batch. With matching counts it is per batch.
- **A client batch is one handoff per table.** `writeBatch` with keys in two
  tables costs two handoffs, not one.

## Known limits

1. **The two-zone case has not been run on two zones.** No machine with two
   zones is available, so the multi-table path is exercised by forcing the
   table count on a one-zone machine. Routing, replay, restore, and pinning are
   covered by tests; the payoff — a write that does not cross a zone boundary —
   is not measured. Measure before trusting the speed, and check the report
   first: two `L3 domain` lines with different `kernel node` values.
2. **Pipelined batch reads only apply with one table.** `readBatchPipelined`
   falls back to plain dispatch when there is more than one table, giving up
   the measured microbatch gain. Single-key and batch throughput are otherwise
   unaffected.
3. **No per-zone prefetch and no per-zone critical section for cross-table
   batches.** Both are consequences of (2).
4. **Nothing in the repository proves the invariant checker rejects a bad
   topology.** The checkers are asserted on real and synthetic machines, and a
   deliberately broken topology was checked against them once by hand, outside
   the repository. A mutation test — break one property, require the checker to
   fail — still has to be written.

## How this is checked

```
zig build test            # all suites
scripts/verify            # the gate
```

Tests worth knowing by name, in `src/table_set.zig`,
`src/partition.zig`, `src/cache_topology.zig`, and
`tests/test_numa_partitions.zig`:

- "pinning takes the physical cores of a domain and nothing else"
- "a domain with no physical-core prefix still pins to its members"
- "placing a table leaves the calling thread's mask as it found it"
- "the capacity split keeps every piece a power of two and loses nothing"
- "a snapshot taken on two tables restores onto three"
- "a torn WAL tail recovers the last complete batch, on both tables"
- "a replayed log puts each entry in the table its key routes to"
- the synthetic-machine tests in `cache_topology.zig` — one zone, two zones on
  one socket, two sockets, two zones on each of two sockets, a partial mask —
  each of which builds a topology from a description and holds it to the same
  invariant checker.
