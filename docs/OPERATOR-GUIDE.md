# Operator Guide

Practical reference for running Moon in production: memory accounting,
allocator tuning, and observability. Companion to
[PRODUCTION-CONTRACT.md](PRODUCTION-CONTRACT.md) (SLOs and durability) and
[BENCHMARK.md](../BENCHMARK.md) (throughput methodology).

---

## Memory Accounting

Moon allocates memory in three logically-separable layers: keyspace data
(DashTable + per-subsystem indexes), allocator metadata (arenas, slabs,
spare runs), and operating-system page mappings (VSZ). Each layer can
inflate independently; this section explains how to read each one and
when a number indicates a real problem.

### 1. VSZ vs RSS — What They Mean

Virtual memory (VSZ) is the total address space a process has reserved from
the operating system. It includes pages that the process has mapped but never
written to, shared library segments, and memory-mapped I/O regions that exist
in the virtual address space but are not backed by physical RAM until they are
first accessed. VSZ is a reservation, not consumption. The OS will never
charge your RAM or swap budget for a virtual page that has never been touched.

Resident set size (RSS) is the amount of RAM the process is actually using
right now — pages that are physically in RAM, whether written by the process
or read from disk. RSS is what you pay for on hosted infrastructure and what
the OOM killer tracks. It is the correct number to monitor.

**Reading tools:**

- Linux: `cat /proc/$PID/status` — look for `VmRSS:` (current resident) and
  `VmPeak:` (high-water mark). `ps -o pid,vsz,rss -p $PID` also works.
- macOS: `ps -o pid,vsz,rss -p $(pgrep moon)` — VSZ and RSS in kilobytes.
  Activity Monitor's "Memory" column approximates RSS (compressed memory
  included), not VSZ.

**The 44 GB incident:** On a 16-core Linux host with default jemalloc settings
(64 arenas), Moon at idle can show **VSZ ≈ 438 GB** while **RSS ≈ 228 MB**.
The 438 GB is jemalloc's reserved virtual address space (6.8 GB per arena,
64 arenas). Zero of those pages are backed by RAM. See section 2 for how Moon
caps this by default.

**macOS specifics:** On macOS aarch64 with narenas:8 active (post-PERF-10),
Moon has been measured at **VSZ ≈ 391.61 GB, RSS ≈ 8.31 MB** at idle. The
large VSZ on macOS is dominated by monoio's mmap region allocations (the io_uring
equivalent — monoio uses kqueue on macOS, but its internal buffer rings are
mmap'd at startup). The jemalloc arena cap has a smaller effect on macOS than
on Linux because monoio mmap is the dominant contributor. On Linux, capping
arenas from 64 to 8 reduces VSZ significantly because jemalloc arenas are the
dominant reservation. This platform difference is expected; RSS behavior on
both platforms is identical.

The authoritative check for whether Moon is consuming real RAM is:

```bash
ps -o pid,vsz,rss -p $(pgrep moon)
# Or on Linux:
grep -E 'VmRSS|VmPeak' /proc/$(pgrep moon)/status
```

### 2. Jemalloc Arena Layout

Jemalloc divides its heap into arenas to reduce contention between threads.
By default, jemalloc creates `4 × ncpus` arenas. On first use each arena
reserves a contiguous block of virtual address space — approximately 6.8 GB
per arena on 64-bit systems. The reservation is virtual-only; only pages
actually written are backed by RAM.

On a 16-core Linux host: 64 arenas × 6.8 GB ≈ **438 GB VSZ** at idle. This
is the source of the "44 GB" (or higher) reading operators see in Activity
Monitor or `top`'s VIRT column. The process's actual RAM consumption is in the
hundreds of megabytes.

**Moon's default cap (PERF-10):** Moon exports a static `_rjem_malloc_conf`
symbol that bakes `narenas:8` into the binary. This is read by jemalloc before
any allocation, before `main()` runs. Result on a 16-core host: 8 arenas × 6.8 GB
≈ **54 GB VSZ** — a linear reduction in virtual address reservation with zero
impact on RSS, throughput, or latency.

**CLI override:** Pass `--memory-arenas-cap N` (N in 1–256, default 8) to spawn
Moon with a different arena count. The flag works by re-spawning the process
with `_RJEM_MALLOC_CONF=narenas:N` injected into the environment before
jemalloc initializes. A sentinel environment variable (`MOON_ARENAS_CAP_APPLIED`)
prevents infinite re-spawn loops.

```bash
# Use 4 arenas instead of the default 8
moon --port 6399 --shards 4 --memory-arenas-cap 4
```

**Environment variable precedence:** If `_RJEM_MALLOC_CONF` is already set in
the environment when Moon starts, Moon detects this and logs a warning:

```
WARN: --memory-arenas-cap ignored because _RJEM_MALLOC_CONF is already set
```

The operator-supplied value always wins over `--memory-arenas-cap`. This
allows advanced jemalloc tuning (e.g., `narenas:16,background_thread:true`)
without rebuilding the binary. See https://jemalloc.net/jemalloc.3.html §OPTIONS
for the full jemalloc option reference.

**Non-jemalloc builds:** If Moon is built with `mimalloc-alt` (see section 5),
the `_rjem_malloc_conf` static symbol is absent and `--memory-arenas-cap` is
accepted but logs a no-op warning. The arena concept does not apply to mimalloc.

**Contention note:** Reducing narenas below `ncpus` will cause arena sharing
across threads. Moon is a thread-per-core server — each shard event loop runs
on its own OS thread and allocates predominantly within its own working set.
8 arenas is comfortably above the contention threshold for typical 8-shard
deployments. If you run more than 8 shards, consider `--memory-arenas-cap N`
where N ≥ `--shards`.

### 3. Reading `MEMORY DOCTOR`

`MEMORY DOCTOR` is a Moon admin command that returns a multi-line breakdown of
process memory across all instrumented subsystems. Run it with any Redis client:

```bash
redis-cli -p 6399 MEMORY DOCTOR
```

Sample output from a live Moon instance with 100 keys loaded on macOS aarch64
(jemalloc build, 1 shard):

```text
Sample of Moon memory usage at 2026-04-27T17:23:51Z

Process:
  RSS:                    8.34 MB
  VSZ:                    391.62 GB
  Allocator:              jemalloc
  Arenas:                 8

Per-subsystem (resident):
  DashTable + entries:    24.74 KB  (0.3%)
  HNSW (vector):          0 B  (0.0%)
  CSR (graph):            0 B  (0.0%)
  WAL writers:            0 B  (0.0%)
  Sealed segments:        0 B  (0.0%)
  Replication backlog:    0 B  (0.0%)
  Allocator overhead:     8.32 MB  (99.7%)

Mapped regions:
  File-backed mmap:       n/a
  Anonymous mmap:         n/a

Recommendations:
  - VSZ-vs-RSS ratio is 48061x (high -- consider --memory-arenas-cap 8)
  - Allocator overhead dominates RSS (>50%). Possible fragmentation -- consider MEMORY PURGE or restart.
```

**Interpreting each field:**

- **Process: RSS** — current resident set size. This is your real memory consumption.
- **Process: VSZ** — virtual address space reserved. Large values are expected; see section 1.
- **Process: Allocator** — `jemalloc` (default) or `mimalloc` (mimalloc-alt build).
- **Process: Arenas** — the configured narenas cap, read live via `tikv-jemalloc-ctl`.
  Confirms which of (a) the built-in default 8, (b) `--memory-arenas-cap N`, or
  (c) operator-set `_RJEM_MALLOC_CONF=narenas:N` is active. Shows `n/a` on
  non-jemalloc builds.
- **Per-subsystem (resident)** — bytes attributed to each subsystem, derived from
  `resident_bytes()` accessors added in Phase 190. The percentages are relative to RSS.
  The 7 fixed labels are:
  - `DashTable + entries` — key-value storage (DashTable structural overhead + entry bytes)
  - `HNSW (vector)` — HNSW graph nodes and edges in active vector indexes
  - `CSR (graph)` — property graph adjacency and MemGraph node/edge SlotMaps
  - `WAL writers` — per-shard write-ahead log buffers (note: always 0 — WAL writers
    are stack-owned by shard event loops and not reachable from command dispatch)
  - `Sealed segments` — immutable text-search index segments pending compaction
  - `Replication backlog` — replica replication buffer VecDeques
  - `Allocator overhead` — computed as `max(0, RSS - sum(other six))`. This includes
    jemalloc thread caches, slab metadata, spare runs, and any subsystem not yet
    instrumented. Healthy values are 5–15% of RSS. Sustained >25% on a steady
    workload suggests fragmentation; consider the mimalloc-alt A/B (see section 5).
- **Mapped regions** — `n/a` until a future phase implements `/proc/self/smaps`
  parsing (Linux) or `vmmap` enumeration (macOS). These fields are reserved.
- **Recommendations** — diagnostic hints generated automatically from the observed
  ratios. A high VSZ-vs-RSS ratio on a fresh install is normal and expected.

### 4. Prometheus `moon_memory_bytes{kind=...}`

Moon emits a labeled Prometheus gauge `moon_memory_bytes` with 7 kind labels.
The metric is updated every 15 seconds by a background publisher on the admin-HTTP
thread. All 7 labels are always present regardless of which features are enabled;
disabled subsystems emit zero-valued series (stable label set for Grafana dashboards).

Scrape endpoint: `GET http://<admin-host>:<admin-port>/metrics` (default admin port
is `6380`). The metric appears in standard Prometheus text format.

| kind | Source | Notes |
|------|--------|-------|
| `dashtable` | DashTable structural overhead + per-entry bytes | Scales with key count and average value size |
| `hnsw` | HNSW graph nodes and edges across all mutable vector segments | Grows on HSET into indexed fields; resets to mutable after FT.COMPACT |
| `csr` | Graph adjacency (CSR storage) + MemGraph SlotMaps | Grows with GRAPH.ADDNODE / GRAPH.ADDEDGE |
| `wal` | Per-shard WAL buffer capacity | Always 0 — WAL writers are stack-owned by event loops, not accessible from admin thread |
| `sealed` | Immutable text-search index segment buffers | Grows until FT.COMPACT flushes mutable to immutable |
| `replication_backlog` | Replica backlog VecDeque allocated capacity | Bounded by `repl-backlog-size`; 0 when no replicas connected |
| `allocator_overhead` | `max(0, RSS - sum of other 6 kinds)` | Includes jemalloc thread caches, spare runs, and any uninstrumented subsystem |

**Querying:**

```promql
# Total memory by subsystem (most recent values)
sum by (kind) (moon_memory_bytes)

# Allocator overhead as percentage of RSS
moon_memory_bytes{kind="allocator_overhead"} / moon_rss_bytes * 100

# Dashtable growth rate (bytes per minute)
rate(moon_memory_bytes{kind="dashtable"}[5m]) * 60
```

**Coverage:** The sum of all 7 kind values is expected to equal or slightly
exceed RSS by construction (allocator_overhead absorbs the difference). The
Phase 190 milestone gate (OBS-04) requires that the sum covers ≥95% of RSS.
Lower coverage would indicate a subsystem not yet instrumented; file a bug
if the sum falls below 80% of RSS on a loaded server.

**Scrape interval:** The publisher updates every 15 seconds. Prometheus scrape
intervals faster than 15s will see stale values between publisher ticks. For
real-time memory investigation, use `MEMORY DOCTOR` directly — it reads
live values on each invocation.

### 5. `mimalloc-alt` Opt-In

`mimalloc-alt` is an optional build feature that replaces jemalloc with
Microsoft's mimalloc as the global allocator. It is provided for A/B evaluation
and allocator-specific performance investigation. It is not the supported
production default.

**When to use `mimalloc-alt`:**

- Investigating allocator-bound performance on synthetic microbenchmarks where
  jemalloc's thread-cache warm-up latency is a confounder.
- Evaluating a smaller VSZ profile on macOS development hosts (mimalloc does
  not use the same large-arena reservation model as jemalloc).
- Diagnosing suspected jemalloc-specific fragmentation: if `MEMORY DOCTOR`
  shows `Allocator overhead` climbing steadily over days without keyspace
  growth, an A/B comparison can confirm whether the fragmentation is
  allocator-specific.

**When NOT to use `mimalloc-alt`:**

Production deployments. jemalloc is the validated allocator with stronger
thread-cache and better under-load fragmentation behavior for Moon's
thread-per-core workload pattern. mimalloc is the experimental knob.

**Build command:**

```bash
cargo build --release --no-default-features \
  --features runtime-monoio,mimalloc-alt,graph,text-index
```

For the tokio runtime (CI parity):

```bash
cargo build --release --no-default-features \
  --features runtime-tokio,mimalloc-alt,graph,text-index
```

**Mutual exclusion:** `jemalloc` and `mimalloc-alt` are mutually exclusive.
Enabling both features at once produces a `compile_error!` at build time:

```
error: jemalloc and mimalloc-alt are mutually exclusive -- enable only one
```

**A/B benchmark:** The script `scripts/bench-allocator-ab.sh` builds both
allocator variants and runs identical workloads against each:

```bash
bash scripts/bench-allocator-ab.sh
```

Options:
- `--quick` — reduced request count for fast iteration
- `--requests N` — override default request count (default: 500000)
- `--shards N` — number of shards (default: 8)
- `--clients N` — number of client connections (default: 50)

Output is written to `tmp/allocator-ab-<timestamp>.txt`. The script runs
SET p=64 and GET p=64 against both binaries and prints throughput side-by-side.

**Trade-off summary:**

| Dimension | jemalloc (default) | mimalloc-alt |
|-----------|-------------------|--------------|
| VSZ profile | High (large arena reservations) | Lower (no arena model) |
| Thread-cache | Strong; benefits long-running shards | Per-thread heap pools |
| Fragmentation under load | Better on sustained write workloads | Better on allocation-heavy micro-tasks |
| `--memory-arenas-cap` flag | Supported | No-op (logged warning) |
| MEMORY DOCTOR `Arenas:` | Numeric (e.g., `8`) | `n/a` |
| Production support | Validated | Experimental |

### 6. Troubleshooting

**Q: Activity Monitor shows Moon using 44 GB (or 200+ GB). Is something broken?**

No. The large number is jemalloc's reserved virtual address space — see section 1
(VSZ vs RSS) and section 2 (Jemalloc Arena Layout). Virtual address reservations
cost zero RAM, zero swap, and zero disk. The number the operator is seeing is VSZ,
not RSS.

To confirm: run `ps -o pid,vsz,rss -p $(pgrep moon)` or send `MEMORY DOCTOR` to
the server. The `RSS` line in the MEMORY DOCTOR output is the real memory
consumption in the hundreds of megabytes, not tens of gigabytes.

On macOS, Activity Monitor's "Memory" column approximates RSS (including
compressed memory), not VSZ. If Activity Monitor shows a number much larger than
what `MEMORY DOCTOR` reports for RSS, the discrepancy is almost certainly
compressed memory or macOS's own memory management heuristics — not a Moon leak.

**Q: How do I detect a real memory leak?**

A real leak shows RSS growth over time without a corresponding increase in stored
data. Watch:

```bash
watch -n 60 'echo "RSS=$(ps -o rss= -p $(pgrep moon))kb DBSIZE=$(redis-cli DBSIZE)"'
```

If RSS is climbing >5% per hour while DBSIZE, vector segment count, and graph
node count are flat, that is a leak signal. Cross-check `MEMORY DOCTOR`: if
`Allocator overhead` is climbing while per-subsystem totals are flat, suspect
allocator fragmentation rather than a data-structure leak.

To identify which subsystem is responsible, monitor the Prometheus gauge:

```bash
# One-shot scrape of per-kind values
curl -s http://localhost:6380/metrics | grep 'moon_memory_bytes'
```

If a per-subsystem kind is climbing without corresponding keyspace growth (DBSIZE
or index size unchanged), file a bug with the subsystem name and the rate of growth.

To rule out fragmentation as the root cause, run the allocator A/B comparison:

```bash
bash scripts/bench-allocator-ab.sh --requests 1000000
```

If mimalloc-alt shows meaningfully lower allocator overhead under the same workload,
the issue is jemalloc fragmentation rather than a logic-level leak.

**Q: Can I disable the arena cap entirely and use jemalloc's default (4 × ncpus arenas)?**

The `narenas:8` cap is always active on default jemalloc builds. To use a
different value, pass `--memory-arenas-cap N` where N is in 1–256. For example,
to match jemalloc's default on a 4-core host:

```bash
moon --memory-arenas-cap 16  # 4 × 4 cores
```

To bypass both the static symbol and the CLI flag entirely, set `_RJEM_MALLOC_CONF`
in the environment directly — it takes precedence over both:

```bash
_RJEM_MALLOC_CONF=narenas:32,background_thread:true moon --port 6399
```

To use jemalloc's uncapped default (4 × ncpus, potentially hundreds of GB VSZ),
build Moon with the `mimalloc-alt` feature — that path bypasses the static
`_rjem_malloc_conf` symbol entirely and does not apply any arena limit.

Advanced jemalloc options are documented at
https://jemalloc.net/jemalloc.3.html §OPTIONS.

**Q: What is a healthy `Allocator overhead` percentage?**

- 5–15% of RSS: normal and expected.
- 15–25%: acceptable on bursty write workloads where jemalloc retains thread
  caches between bursts.
- Sustained >25% on a steady workload: investigate. Run the allocator A/B
  (section 5). If mimalloc-alt shows significantly lower overhead, the issue
  is jemalloc fragmentation. If both allocators show the same overhead,
  there may be an uninstrumented subsystem accumulating memory.

**Q: The CI `memory-steady-state` job failed. What do I do?**

The job runs `scripts/bench-memory-steady-state.sh` and compares per-kind
values against the committed baseline in `tests/fixtures/memory-baseline.json`
with a ±5% tolerance.

If the failure is expected (intentional data structure change, new entry overhead):

```bash
# Regenerate the baseline
bash scripts/bench-memory-steady-state.sh \
  --write-baseline tests/fixtures/memory-baseline.json

# Verify the new baseline
jq . tests/fixtures/memory-baseline.json

# Commit with the required tag in the subject
git commit -m "chore(190-04): [memory-baseline-update] reason: <why the change is expected>"
```

The `[memory-baseline-update]` tag in the commit subject is the project
convention for baseline updates and is required for the CI job to recognize
the change as intentional.

If the failure is unexpected, compare the captured values to the baseline to
identify which subsystem regressed, then follow the real-leak detection
procedure above.

---

See also:

- [PRODUCTION-CONTRACT.md](PRODUCTION-CONTRACT.md) for memory-related SLOs and
  platform guarantees.
- [production-guide.md](production-guide.md) for deployment configuration and
  tuning recommendations.
- [Phase 190 plans](../.planning/phases/190-memory-observability/) for
  `MEMORY DOCTOR` and Prometheus internals.
- [Phase 191 plans](../.planning/phases/191-allocator-ux/) for the arena-cap
  and `mimalloc-alt` feature design notes.
