# Phase 13: Per-Shard Memory Management and NUMA Optimization - Research

**Researched:** 2026-03-24
**Domain:** Memory allocators, cache-line discipline, NUMA-aware placement, prefetching
**Confidence:** HIGH

## Summary

This phase replaces jemalloc with mimalloc as the global allocator, expands bumpalo per-connection arenas to cover all request-scoped temporaries (not just parsing), adds NUMA-aware thread pinning (Linux-only via `sched_setaffinity`/`mbind`), and enforces cache-line discipline across all per-shard mutable structures. The prefetching enhancement uses `core::arch::x86_64::_mm_prefetch` during DashTable bucket lookups.

The codebase is well-prepared: jemalloc is a single 3-line declaration in `src/main.rs`, bumpalo is already imported in `connection.rs` with 4KB arena and batch reset, `core_affinity` is already in Cargo.toml (unused), and the Shard struct in `shard/mod.rs` already owns all its state thread-locally with no Arc/Mutex. The DashTable Segment struct has no alignment attributes yet -- adding `#[repr(C, align(64))]` is the key cache-line fix.

**Primary recommendation:** Execute in three waves: (1) mimalloc swap + arena expansion, (2) cache-line alignment + prefetching, (3) NUMA placement with cfg gates.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Global allocator: mimalloc replacing tikv-jemallocator (keep jemalloc behind feature flag for heap profiling)
- Per-connection arenas: bumpalo::Bump expanded beyond parsing to cover command argument assembly, response building, temporary sort buffers; bump.reset() after each pipeline batch; 4KB initial capacity
- NUMA-aware thread/memory placement: sched_setaffinity() on Linux, no-op on macOS; mbind() with MPOL_BIND; consider numanji or mimalloc built-in NUMA support
- Cache-line discipline: #[repr(C, align(64))] on all per-shard mutable structures; crossbeam_utils::CachePadded<T> for shared atomics; read/write field separation
- Hash bucket prefetching: core::arch::x86_64::_mm_prefetch with _MM_HINT_T0; x86-64 only via cfg gate

### Claude's Discretion
- Exact NUMA detection strategy (read /sys/devices/system/node/ or use libnuma)
- Whether to implement a per-shard memory pool for fixed-size allocations
- Arena capacity auto-tuning based on observed batch sizes
- Whether to use core::hint::black_box for benchmark-accurate memory measurements

### Deferred Ideas (OUT OF SCOPE)
- Custom slab allocator for fixed-size entry structs
- Huge pages (2MB THP) for DashTable segments
</user_constraints>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| mimalloc | 0.1.48 | Global allocator replacing jemalloc | 5.3x faster multi-threaded allocation, per-thread free lists complement thread-per-core |
| bumpalo | 3.20 | Per-connection arena allocator | Already in project; O(1) reset, ~2ns bump allocation |
| crossbeam-utils | 0.8.21 | CachePadded<T> for false-sharing prevention | Already a transitive dep (via parking_lot); proven, widely used |
| core_affinity | 0.8.3 | Cross-platform CPU core pinning | Already in Cargo.toml; wraps sched_setaffinity on Linux |
| libc | 0.2 | NUMA mbind(), sched_setaffinity() raw calls | Already in Cargo.toml (Linux target); standard FFI layer |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| tikv-jemallocator | 0.6 | Heap profiling fallback | Behind `jemalloc` feature flag for development profiling only |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| mimalloc crate | mimalloc-rust | mimalloc (purpleprotocol) is more maintained; mimalloc-rust has different API |
| core_affinity | raw libc::sched_setaffinity | core_affinity is simpler but less granular; for NUMA we need libc anyway |
| numanji | mimalloc NUMA support | numanji adds jemalloc fallback complexity; mimalloc has MIMALLOC_USE_NUMA_NODES env var |

**Installation:**
```bash
cargo add mimalloc
# crossbeam-utils already available as transitive dep
# core_affinity already in Cargo.toml
# libc already in Cargo.toml for Linux target
```

**Cargo.toml changes:**
```toml
# Replace:
tikv-jemallocator = "0.6"
# With:
mimalloc = { version = "0.1", default-features = false }

# Feature flag for profiling fallback:
[features]
jemalloc = ["tikv-jemallocator"]

# Direct dependency on crossbeam-utils (for CachePadded):
crossbeam-utils = "0.8"
```

## Architecture Patterns

### Current Code State (Integration Points)

**Global allocator (src/main.rs lines 1-3):**
```rust
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
```
Replacement is a direct 1:1 swap.

**Per-connection arena (src/server/connection.rs line 109):**
```rust
let mut arena = Bump::with_capacity(4096); // 4KB initial capacity
```
Already exists in the non-sharded handler. The sharded handler (`handle_connection_sharded`, line 912) does NOT have a bumpalo arena yet -- this is a key addition.

**Shard struct (src/shard/mod.rs lines 32-43):**
```rust
pub struct Shard {
    pub id: usize,
    pub databases: Vec<Database>,
    pub num_shards: usize,
    pub runtime_config: RuntimeConfig,
    pub pubsub_registry: PubSubRegistry,
}
```
No alignment attributes. Thread pinning should happen at the start of the shard thread, before `Shard::run()`.

**DashTable Segment (src/storage/dashtable/segment.rs lines 68-74):**
```rust
pub struct Segment<K, V> {
    ctrl: [Group; NUM_GROUPS],   // 64 bytes
    keys: [MaybeUninit<K>; TOTAL_SLOTS],
    values: [MaybeUninit<V>; TOTAL_SLOTS],
    count: u32,
    depth: u32,
}
```
No alignment. The `ctrl` array is already 64 bytes (4 groups * 16 bytes) but the struct itself is not cache-line aligned.

**CompactEntry (src/storage/entry.rs lines 122-129):**
```rust
#[repr(C)]
#[derive(Debug, Clone)]
pub struct CompactEntry {
    pub value: CompactValue,  // 16 bytes
    pub ttl_delta: u32,       // 4 bytes
    pub metadata: u32,        // 4 bytes
}
```
Already `#[repr(C)]`, 24 bytes. Adding `align(64)` would pad to 64 bytes -- this is a TRADEOFF: it prevents false sharing but increases memory per entry by 2.67x. Only apply alignment to the Segment struct itself, not individual entries.

### Pattern 1: mimalloc Global Allocator Swap

**What:** Replace jemalloc with mimalloc as `#[global_allocator]`
**When to use:** At startup; affects all heap allocations globally

```rust
// src/main.rs
#[cfg(not(feature = "jemalloc"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
```

### Pattern 2: Per-Connection Arena in Sharded Handler

**What:** Add bumpalo arena to `handle_connection_sharded` for batch temporaries
**When to use:** Each connection should own an arena for request-scoped allocations

```rust
// In handle_connection_sharded:
let mut arena = Bump::with_capacity(4096);

// Within batch processing loop:
{
    let mut write_indices: BumpVec<usize> = BumpVec::new_in(&arena);
    // ... use arena for temporary allocations ...
    // BumpVec must be dropped before any await (Send safety)
    drop(write_indices);
}
arena.reset(); // O(1) after batch
```

### Pattern 3: Cache-Line Aligned Segment

**What:** Align DashTable Segment to 64-byte boundary
**When to use:** All per-shard mutable structures

```rust
#[repr(C, align(64))]
pub struct Segment<K, V> {
    // Hot path: read during every lookup
    ctrl: [Group; NUM_GROUPS],      // 64 bytes = exactly 1 cache line
    count: u32,                     // Read-mostly
    depth: u32,                     // Read-mostly
    // Cold path: only accessed on hit
    keys: [MaybeUninit<K>; TOTAL_SLOTS],
    values: [MaybeUninit<V>; TOTAL_SLOTS],
}
```

### Pattern 4: CachePadded for Shared Counters

**What:** Wrap any shared atomic counters in CachePadded
**When to use:** Any atomic/mutable value accessed from multiple threads

```rust
use crossbeam_utils::CachePadded;

struct ShardStats {
    commands_processed: CachePadded<AtomicU64>,
    bytes_read: CachePadded<AtomicU64>,
    // Each counter on its own cache line
}
```

### Pattern 5: Hash Bucket Prefetching

**What:** Prefetch next bucket during DashTable lookup
**When to use:** During segment.find() before accessing key data

```rust
#[cfg(target_arch = "x86_64")]
#[inline]
unsafe fn prefetch_bucket<K, V>(segment: &Segment<K, V>, slot: usize) {
    use core::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
    let ptr = segment.keys[slot].as_ptr() as *const i8;
    _mm_prefetch(ptr, _MM_HINT_T0);
}

#[cfg(not(target_arch = "x86_64"))]
#[inline]
fn prefetch_bucket<K, V>(_segment: &Segment<K, V>, _slot: usize) {
    // No-op on non-x86_64
}
```

### Pattern 6: NUMA Thread Pinning (Linux-only)

**What:** Pin shard thread to specific core and bind memory to NUMA node
**When to use:** During shard thread bootstrap, before any allocations

```rust
// In main.rs shard thread spawn, BEFORE creating the runtime:
#[cfg(target_os = "linux")]
{
    use core_affinity::CoreId;
    let core_id = CoreId { id: shard_id };
    core_affinity::set_for_current(core_id);

    // NUMA memory binding via libc
    numa_bind_local_node(shard_id);
}

#[cfg(target_os = "linux")]
fn numa_bind_local_node(core_id: usize) {
    // Read /sys/devices/system/node/nodeN/cpulist to find which node owns this core
    // Then call libc::mbind() with MPOL_BIND for subsequent allocations
    // Fallback: just use mimalloc's MIMALLOC_USE_NUMA_NODES=auto
}
```

### Anti-Patterns to Avoid
- **Aligning CompactEntry to 64 bytes:** Would waste 40 bytes per entry (24 -> 64), multiplied by millions of keys. Only align the containing Segment struct.
- **Using CachePadded everywhere:** Only needed for values actually shared between threads. In shared-nothing architecture, most data is thread-local and does not need padding.
- **Calling mbind() after allocations:** Must bind BEFORE allocating shard data structures, or memory will be on the wrong NUMA node.
- **Forgetting cfg gates on macOS:** Developer is on macOS (Darwin). All NUMA/affinity code MUST compile and run as no-ops on macOS.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Cache-line padding | Manual `[u8; 64]` padding arrays | `crossbeam_utils::CachePadded<T>` | Handles alignment, sizing, Deref, all architectures |
| CPU core pinning | Raw `libc::sched_setaffinity` | `core_affinity::set_for_current` | Cross-platform (Linux/macOS/Windows), already in deps |
| Global allocator | Custom allocator impl | `mimalloc::MiMalloc` as `#[global_allocator]` | Battle-tested, Microsoft-maintained, one line |
| Arena allocation | Custom free-list allocator | `bumpalo::Bump` | Already in project, proven, O(1) reset |

**Key insight:** All four components (allocator, arena, padding, affinity) have mature crate solutions already in or trivially addable to the dependency tree. The work is integration, not invention.

## Common Pitfalls

### Pitfall 1: Arena Send Safety in Async Context
**What goes wrong:** `BumpVec` allocated from `bumpalo::Bump` is `!Send`. If held across an `.await` point, the future becomes `!Send` and won't compile with tokio::spawn (though spawn_local is fine).
**Why it happens:** Bump arena uses raw pointers internally.
**How to avoid:** Scope BumpVec within sync blocks, explicitly drop before any await. The existing non-sharded handler already does this correctly (line 619-715 in connection.rs).
**Warning signs:** Compiler error about `Send` bound not satisfied.

### Pitfall 2: CompactEntry Alignment Temptation
**What goes wrong:** Applying `#[repr(align(64))]` to CompactEntry bloats per-key memory from 24 bytes to 64 bytes.
**Why it happens:** False sharing prevention instinct applied too broadly.
**How to avoid:** Align the Segment struct (which contains arrays of entries), not individual entries. Entries within a single shard's segment are only accessed by one thread.
**Warning signs:** Memory usage regression in benchmarks.

### Pitfall 3: NUMA Detection on Non-NUMA Systems
**What goes wrong:** Code assumes `/sys/devices/system/node/` exists or has multiple nodes.
**Why it happens:** Single-socket systems or macOS have no NUMA topology.
**How to avoid:** Always check if NUMA is available. If only one node exists, skip mbind entirely. Use cfg gates for macOS.
**Warning signs:** File-not-found errors on startup, panics on macOS.

### Pitfall 4: Prefetch on Non-x86_64
**What goes wrong:** `_mm_prefetch` is x86_64-only. Code won't compile on aarch64 (Apple Silicon).
**Why it happens:** Using intrinsic without architecture cfg gate.
**How to avoid:** Always gate with `#[cfg(target_arch = "x86_64")]` and provide a no-op fallback. The developer's macOS machine uses Apple Silicon (aarch64).
**Warning signs:** Compile failure on `cargo build` locally.

### Pitfall 5: mimalloc Feature Flags
**What goes wrong:** Default mimalloc features include things like `override` which can conflict with system allocator on some platforms.
**Why it happens:** Not specifying `default-features = false`.
**How to avoid:** Use `mimalloc = { version = "0.1", default-features = false }` and only enable needed features.
**Warning signs:** Linker errors or double-free crashes.

### Pitfall 6: Thread Pinning Order
**What goes wrong:** Shard thread pins to core AFTER creating tokio runtime and allocating data structures.
**Why it happens:** Pinning added as afterthought inside `Shard::run()`.
**How to avoid:** Pin BEFORE `tokio::runtime::Builder::new_current_thread().build()` in the `std::thread::Builder::new().spawn()` closure in main.rs.
**Warning signs:** Memory allocated on wrong NUMA node, defeating the purpose.

## Code Examples

### Example 1: mimalloc Global Allocator with Feature Flag

```rust
// src/main.rs -- top of file
#[cfg(not(feature = "jemalloc"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "jemalloc")]
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
```

### Example 2: Segment Cache-Line Alignment

```rust
// src/storage/dashtable/segment.rs
#[repr(C, align(64))]
pub struct Segment<K, V> {
    // --- Cache line 0: control bytes (hot, read on every lookup) ---
    ctrl: [Group; NUM_GROUPS],   // 64 bytes exactly
    // --- Cache line 1+: metadata + keys + values ---
    count: u32,
    depth: u32,
    keys: [MaybeUninit<K>; TOTAL_SLOTS],
    values: [MaybeUninit<V>; TOTAL_SLOTS],
}
```

### Example 3: Prefetch During DashTable Lookup

```rust
// In segment.rs find() method, before accessing keys for comparison:
#[cfg(target_arch = "x86_64")]
{
    use core::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
    // Prefetch the key data for the first matching slot
    if let Some(first_pos) = mask_a.lowest_set_bit() {
        let slot = base_a + first_pos;
        if slot < TOTAL_SLOTS {
            unsafe {
                _mm_prefetch(
                    self.keys[slot].as_ptr() as *const i8,
                    _MM_HINT_T0,
                );
            }
        }
    }
}
```

### Example 4: Thread Pinning in Shard Spawn

```rust
// In main.rs, inside the shard thread spawn closure:
let handle = std::thread::Builder::new()
    .name(format!("shard-{}", id))
    .spawn(move || {
        // Pin to core FIRST, before any allocations
        #[cfg(target_os = "linux")]
        {
            use core_affinity::CoreId;
            let core_id = CoreId { id };
            if core_affinity::set_for_current(core_id) {
                info!("Shard {} pinned to core {}", id, id);
            }
        }

        // Now build runtime (allocations go to local NUMA node)
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build shard runtime");
        // ...
    })
```

### Example 5: NUMA Node Detection (Linux)

```rust
#[cfg(target_os = "linux")]
fn detect_numa_node_for_cpu(cpu_id: usize) -> Option<usize> {
    // Read /sys/devices/system/node/nodeN/cpulist for each node
    let node_dir = std::path::Path::new("/sys/devices/system/node");
    if !node_dir.exists() {
        return None;
    }
    for entry in std::fs::read_dir(node_dir).ok()? {
        let entry = entry.ok()?;
        let name = entry.file_name();
        let name_str = name.to_str()?;
        if !name_str.starts_with("node") {
            continue;
        }
        let node_id: usize = name_str.strip_prefix("node")?.parse().ok()?;
        let cpulist_path = entry.path().join("cpulist");
        let cpulist = std::fs::read_to_string(&cpulist_path).ok()?;
        if cpu_in_cpulist(cpu_id, &cpulist) {
            return Some(node_id);
        }
    }
    None
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| tikv-jemallocator 0.6 | mimalloc 0.1.48 (Microsoft) | Project decision | 5.3x allocation throughput under contention |
| No arena in sharded handler | bumpalo per-connection arena | Phase 8 started it, Phase 13 completes | O(1) batch deallocation |
| No alignment | #[repr(C, align(64))] on Segment | Phase 13 | Prevents false sharing (300x slowdown risk) |
| No prefetching | _mm_prefetch on DashTable lookup | Phase 13 | Hides memory latency during hash probing |

**Important note:** `core_affinity` (0.8.3) is already in Cargo.toml but completely unused in source code. This phase activates it.

## Open Questions

1. **NUMA detection: sysfs vs libnuma**
   - What we know: `/sys/devices/system/node/` is available on all Linux NUMA systems. libnuma requires a C library dependency.
   - What's unclear: Whether mimalloc's built-in `MIMALLOC_USE_NUMA_NODES=auto` is sufficient, making explicit mbind() unnecessary.
   - Recommendation: Use mimalloc's env var as the primary NUMA strategy. Only add explicit mbind() if benchmarks show cross-NUMA allocation. Read sysfs for logging/diagnostics only.

2. **Arena expansion scope in sharded handler**
   - What we know: The sharded handler lacks arena. The non-sharded handler uses it for write index tracking.
   - What's unclear: How much the sharded handler benefits, given it has simpler batch processing (no lock acquisition).
   - Recommendation: Add arena with same 4KB initial capacity. Use for batch response Vec pre-allocation and any temporary buffers.

3. **CachePadded on x86_64 vs aarch64**
   - What we know: crossbeam CachePadded uses 128-byte alignment on x86_64 and aarch64 (accounting for hardware prefetcher stride), 64 bytes elsewhere.
   - What's unclear: Whether 128 bytes is wasteful for our use case.
   - Recommendation: Use CachePadded as-is. The 128-byte alignment is correct for modern CPUs with spatial prefetchers.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | cargo test (built-in) + criterion 0.8 (benchmarks) |
| Config file | Cargo.toml [[bench]] sections |
| Quick run command | `cargo test --lib` |
| Full suite command | `cargo test` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| MEM-ALLOC-01 | mimalloc as global allocator, all tests pass | integration | `cargo test` | Existing suite |
| MEM-ARENA-01 | Per-connection arena with O(1) reset | unit | `cargo test --lib connection` | Partial (non-sharded handler has arena) |
| NUMA-PIN-01 | Shard threads pinned to cores (Linux) | manual-only | Manual: verify via `taskset -cp <pid>` on Linux | No |
| CACHE-ALIGN-01 | Segment aligned to 64 bytes | unit | `cargo test --lib dashtable` | Existing segment tests |
| CACHE-ALIGN-02 | Read/write field separation | unit | `cargo test --lib dashtable` | Existing segment tests |
| PREFETCH-01 | _mm_prefetch during lookup | unit | `cargo test --lib dashtable` | Existing lookup tests |
| PERF-01 | 3x allocation throughput vs jemalloc | bench | `cargo bench` | Existing benchmarks |

### Sampling Rate
- **Per task commit:** `cargo test --lib`
- **Per wave merge:** `cargo test`
- **Phase gate:** Full suite green before verify

### Wave 0 Gaps
- [ ] Compile-time assertion for Segment alignment: `const _: () = assert!(std::mem::align_of::<Segment<Bytes, CompactEntry>>() >= 64);`
- [ ] Compile-time assertion for struct size stability after reordering
- [ ] Benchmark for mimalloc vs jemalloc allocation throughput (criterion bench)

## Sources

### Primary (HIGH confidence)
- [mimalloc crate](https://crates.io/crates/mimalloc) - v0.1.48, GlobalAllocator impl verified
- [crossbeam-utils CachePadded docs](https://docs.rs/crossbeam/latest/crossbeam/utils/struct.CachePadded.html) - 128-byte alignment on x86_64/aarch64
- [core::arch::x86_64::_mm_prefetch](https://doc.rust-lang.org/beta/core/arch/x86_64/fn._mm_prefetch.html) - Prefetch intrinsic API
- [bumpalo 3.20](https://crates.io/crates/bumpalo) - Already in project, arena with collections feature
- [core_affinity 0.8.3](https://crates.io/crates/core_affinity) - Already in Cargo.toml
- Project source code: `src/main.rs`, `src/shard/mod.rs`, `src/server/connection.rs`, `src/storage/dashtable/segment.rs`

### Secondary (MEDIUM confidence)
- [libc::sched_setaffinity docs](https://docs.rs/libc/0.2.88/libc/fn.sched_setaffinity.html) - Linux thread affinity
- [sched_setaffinity(2) man page](https://man7.org/linux/man-pages/man2/sched_setaffinity.2.html) - Linux system call reference
- Blueprint section "Memory management: per-shard allocators and arena patterns" - Architecture guidance

### Tertiary (LOW confidence)
- numanji crate (0.1.5) - Not recommending; mimalloc's built-in NUMA support is simpler
- 5.3x allocation improvement claim from blueprint -- based on mimalloc benchmarks, actual improvement depends on workload

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - All crates verified on crates.io, versions confirmed, mimalloc and bumpalo are well-established
- Architecture: HIGH - Integration points clearly identified in existing source code; patterns are straightforward
- Pitfalls: HIGH - Based on direct code inspection (macOS dev environment, aarch64, existing arena pattern)
- NUMA specifics: MEDIUM - sysfs approach is standard but developer cannot test on macOS; mimalloc env var approach is safest

**Research date:** 2026-03-24
**Valid until:** 2026-04-24 (stable domain, crate versions unlikely to break)
