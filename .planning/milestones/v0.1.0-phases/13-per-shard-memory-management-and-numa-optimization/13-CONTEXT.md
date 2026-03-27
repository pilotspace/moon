# Phase 13: Per-Shard Memory Management and NUMA Optimization - Context

**Gathered:** 2026-03-24
**Status:** Ready for planning

<domain>
## Phase Boundary

Implement per-shard memory isolation with mimalloc as global allocator replacing jemalloc, per-connection bumpalo arenas for request-scoped temporaries, NUMA-aware thread pinning and memory placement, and cache-line discipline (64-byte alignment, false-sharing prevention, prefetching) across all shard-local data structures.

</domain>

<decisions>
## Implementation Decisions

### Global allocator: mimalloc
- [auto] Replace `tikv-jemallocator` with `mimalloc` (`#[global_allocator]`) — 5.3x faster average allocation under multi-threaded workloads
- mimalloc's per-thread free lists naturally complement thread-per-core architecture
- NUMA-aware: `MIMALLOC_USE_NUMA_NODES` environment variable for NUMA-local allocation
- If heap profiling needed during development, keep jemalloc behind a feature flag

### Per-connection arenas
- [auto] Each connection maintains a `bumpalo::Bump` arena (building on Phase 8's parser arena)
- Arena scope expanded beyond parsing: also covers command argument assembly, response building, temporary sort buffers
- `bump.reset()` after each pipeline batch — O(1) bulk deallocation of thousands of allocations
- Arena initial capacity: 4KB (grows on demand, rarely exceeds 16KB per batch)

### NUMA-aware thread/memory placement
- [auto] Pin each shard thread to a specific CPU core via `sched_setaffinity()` (Linux) / no-op on macOS
- Bind shard memory to local NUMA node via `mbind()` with `MPOL_BIND`
- Blueprint cites: local DRAM access ~70ns vs remote ~112-119ns — 62% penalty on cross-NUMA access
- Consider `numanji` crate for NUMA-local allocator with jemalloc fallback, OR mimalloc's built-in NUMA support

### Cache-line discipline
- [auto] All per-shard mutable structures aligned to 64 bytes: `#[repr(C, align(64))]`
- Use `crossbeam_utils::CachePadded<T>` for any shared atomic counters (stats, metrics)
- Separate read-heavy fields (hash table metadata, bucket control bytes) from write-heavy fields (stats, expiry timestamps) on different cache lines
- False sharing between cores can cause 300x slowdown at 32 threads — critical to prevent

### Hash bucket prefetching
- [auto] During DashTable lookup, prefetch next bucket's memory via `core::arch::x86_64::_mm_prefetch`
- Prefetch hint: `_MM_HINT_T0` (all cache levels) for sequential bucket access
- Only on x86-64; no-op on other architectures via `#[cfg(target_arch = "x86_64")]`

### Claude's Discretion
- Exact NUMA detection strategy (read `/sys/devices/system/node/` or use libnuma)
- Whether to implement a per-shard memory pool for fixed-size allocations (entry structs, bucket arrays)
- Arena capacity auto-tuning based on observed batch sizes
- Whether to use `core::hint::black_box` for benchmark-accurate memory measurements

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Architecture blueprint
- `.planning/architect-blue-print.md` §Memory management: per-shard allocators and arena patterns — mimalloc, bumpalo, NUMA, cache-line discipline, prefetching

### Phase dependencies
- `.planning/phases/08-resp-parser-simd-acceleration/08-CONTEXT.md` — Initial bumpalo arena introduction for parser
- `.planning/phases/11-thread-per-core-shared-nothing-architecture/11-CONTEXT.md` — Shard architecture that memory management builds on
- `.planning/phases/10-dashtable-segmented-hash-table-with-swiss-table-simd-probing/10-CONTEXT.md` — DashTable segments that need cache-line alignment

### Current implementation
- `src/main.rs` — Current jemalloc `#[global_allocator]` declaration (replacement target)
- `Cargo.toml` — `tikv-jemallocator = "0.6"` dependency to replace with `mimalloc`

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `bumpalo` already introduced in Phase 8 — extend usage from parser to all request-scoped allocations
- `crossbeam_utils` — `CachePadded<T>` available (crossbeam is a transitive dep of parking_lot)

### Established Patterns
- Single `#[global_allocator]` declaration in main.rs — straightforward replacement
- Per-connection state in connection handler — natural place to own the Bump arena

### Integration Points
- `#[global_allocator]` swap affects all allocations — test suite must pass with mimalloc
- Shard thread pinning done during shard bootstrap (Phase 11's thread spawn)
- DashTable segment/bucket structs need `#[repr(C, align(64))]` attributes
- Arena lifecycle tied to connection handler's batch processing loop

</code_context>

<specifics>
## Specific Ideas

- Blueprint: mimalloc delivers 5.3x faster average allocation than glibc under multi-threaded workloads
- NUMA penalty: ~62% slower for remote access compounding across millions of lookups
- False sharing: 300x slowdown at 32 threads — cache-line alignment is not optional, it's correctness

</specifics>

<deferred>
## Deferred Ideas

- Custom slab allocator for fixed-size entry structs — evaluate after profiling shows allocation as bottleneck
- Huge pages (2MB THP) for DashTable segments — evaluate based on TLB miss profiling

</deferred>

---

*Phase: 13-per-shard-memory-management-and-numa-optimization*
*Context gathered: 2026-03-24*
