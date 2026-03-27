# Principal Architect Optimization Analysis

**Date:** 2026-03-26
**Author:** Principal Architect Review
**Baseline:** PROFILING-REPORT.md (commit bd3399c)

---

## Current Performance Profile

### GET Hot Path (174ns total, profiled via Criterion)

| Stage | Time | % | Bottleneck |
|-------|-----:|--:|:----------:|
| RESP Parse (memchr + Frame alloc) | 81.4ns | 46.7% | YES |
| Extract Command | 0.5ns | 0.3% | |
| DashTable Lookup (xxhash + SIMD) | 52.6ns | 30.2% | YES |
| as_bytes_owned (Bytes alloc/clone) | 54.5ns | 31.3% | YES |
| Frame::BulkString construct | 4.1ns | 2.4% | |
| RESP Serialize (itoa + memcpy) | 14.8ns | 8.5% | |
| xxhash key route | 5.9ns | 3.4% | |
| is_write_command | 0.8ns | 0.5% | |

**Redis equivalent: ~55ns** (3.2x faster per-command, but rust-redis wins at pipeline depth due to batch dispatch + io_uring)

### Memory Profile

| Metric | rust-redis | Redis 8.6.1 | Gap |
|--------|----------:|------------:|----:|
| Per-key (1M strings, 256B) | 280 B | 197 B | +83B (1.42x) |
| Data structure overhead | 49 B | 96 B | **rust-redis wins** |
| Runtime/allocator overhead | 231 B | 101 B | +130B (gap source) |
| Memory reclamation (delete 50%) | 72% returned | 1.65x frag | **rust-redis wins** |

### CPU Efficiency

| Metric | rust-redis (Tokio) | Redis | Gap |
|--------|-------------------:|------:|----:|
| ops/sec | 334,488 | 337,428 | 0.99x |
| CPU% | 85.6% | 60.2% | +42% |
| ops/%CPU | 3,908 | 5,605 | 0.70x |

---

## Optimization Solutions

### OPT-1: DashTable Prefetch (Phase A — Quick Win)

**File:** `src/storage/dashtable/mod.rs`

**Problem:** Loading a segment from L2/L3 cache costs ~10ns on random access patterns. At 1M keys, ~40% of lookups miss L1.

**Solution:** Insert `_mm_prefetch` / `__prefetch` after computing segment index, before computing home bucket. The hash computation (6ns) overlaps with prefetch latency.

```rust
fn get(&self, key: &[u8]) -> Option<&Entry> {
    let hash = xxhash64(key);
    let seg_storage_idx = self.directory[hash_to_dir_index(hash)];

    // Prefetch segment while computing home bucket
    let seg_ptr = &self.segments[seg_storage_idx] as *const _ as *const i8;
    #[cfg(target_arch = "x86_64")]
    unsafe { core::arch::x86_64::_mm_prefetch(seg_ptr, core::arch::x86_64::_MM_HINT_T0); }
    #[cfg(target_arch = "aarch64")]
    unsafe { core::arch::aarch64::__prefetch(seg_ptr as *const u8, 0, 3); }

    let h2 = (hash >> 57) as u8;
    let home = hash_to_home(hash);
    // Segment now likely in L1
    self.segments[seg_storage_idx].lookup(key, h2, home)
}
```

**Expected savings:** -8ns on cache-miss paths
**Risk:** Very low (prefetch is a hint)
**Acceptance:** `cargo bench -- 3_dashtable_get_hit` shows improvement

### OPT-2: Branchless CompactKey Equality (Phase A — Quick Win)

**File:** `src/storage/compact_key.rs`

**Problem:** `PartialEq` for CompactKey branches on inline/heap flag, calls `as_bytes()`, then memcmp. For inline keys (90%+), this is 3 function calls + 2 branches.

**Solution:** For inline keys, compare raw 24-byte `data` array directly (3 QWORD compares, no branching):

```rust
impl PartialEq for CompactKey {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        if (self.data[0] | other.data[0]) & HEAP_FLAG == 0 {
            self.data == other.data  // 3 QWORD compares, branchless
        } else {
            self.as_bytes() == other.as_bytes()
        }
    }
}
```

**Expected savings:** -5ns for inline key comparisons
**Risk:** Very low
**Acceptance:** `cargo bench -- eq_inline_match` shows improvement

### OPT-3: jemalloc as Default Allocator (Phase A — Quick Win)

**File:** `Cargo.toml`, `src/main.rs`

**Problem:** mimalloc has ~32B per-allocation metadata overhead and aggressive size-class rounding. At 1M keys, this adds ~70B/key in RSS.

**Solution:** Change default feature from `mimalloc` to `jemalloc`. jemalloc has tighter size-class granularity and lower per-allocation metadata (~8B vs ~32B).

```toml
[features]
default = ["runtime-tokio", "jemalloc"]
```

**Expected savings:** -35B/key RSS
**Risk:** Low (jemalloc feature already exists, tested in Phase 13)
**Trade-off:** Allocation speed: mimalloc ~4ns vs jemalloc ~8ns. For memory-bound workloads, jemalloc wins.
**Acceptance:** Run profiling: `profile.sh --section memory` shows <250B/key at 1M

### OPT-4: No-Arc Local Command Responses (Phase A — Quick Win)

**Files:** `src/shard/mod.rs`, `src/server/connection.rs`

**Problem:** Every command response is wrapped in `Arc<Frame>` for cross-shard dispatch compatibility. But 95%+ of commands are local — the Arc atomic refcount (2 ops × 5ns = 10ns) is wasted.

**Solution:** Return `Frame` directly for local commands, `Arc<Frame>` only for cross-shard:

```rust
enum CommandResponse {
    Local(Frame),
    Remote(Arc<Frame>),
}
```

In the connection handler, match on response type and skip Arc for local path.

**Expected savings:** ~4% CPU reduction (~10ns per command)
**Risk:** Low
**Acceptance:** `cargo bench -- 9_full_pipeline_get_256b` shows improvement

### OPT-5: Direct GET Serialization (Phase B — Core)

**Files:** `src/command/string.rs`, `src/server/connection.rs`

**Problem:** GET response goes through: `as_bytes_owned()` (18ns alloc) → `Frame::BulkString` (4ns) → `serialize()` (20ns match+write) = 42ns. Redis does this in ~15ns.

**Solution:** Serialize directly from `CompactValue` to output buffer, skipping Bytes and Frame intermediaries:

```rust
// In string::get(), return a DirectResponse instead of Frame
pub enum GetResult<'a> {
    Hit(&'a CompactValue),  // Borrow, no allocation
    Miss,
}

// In connection handler, serialize directly:
match get_result {
    GetResult::Hit(value) => {
        let data = value.as_bytes();  // borrow, 2ns
        buf.put_u8(b'$');
        buf.put_slice(itoa::Buffer::new().format(data.len()).as_bytes());
        buf.put_slice(b"\r\n");
        buf.put_slice(data);          // single memcpy, 14ns
        buf.put_slice(b"\r\n");
        // Total: ~18ns (vs 42ns before)
    }
    GetResult::Miss => buf.put_slice(b"$-1\r\n"),
}
```

**Expected savings:** -24ns per GET response
**Risk:** Medium (special-case for GET family; need to do same for HGET, LINDEX, etc.)
**Acceptance:** `cargo bench -- 9_full_pipeline_get_256b` drops below 140ns

### OPT-6: Zero-Copy Argument Slicing (Phase B — Core)

**File:** `src/protocol/parse.rs`

**Problem:** `Bytes::copy_from_slice()` for every bulk string argument costs ~18ns (malloc + memcpy). For arguments ≥10 bytes, `buf.slice()` (Arc bump, ~5ns) is faster.

**Solution:** Use `slice()` for arguments, `copy_from_slice()` only for small command names:

```rust
fn parse_bulk_string(buf: &mut BytesMut, len: usize) -> Bytes {
    if len >= 10 {
        buf.split_to(len).freeze()  // ~5ns: Arc refcount bump
    } else {
        let data = Bytes::copy_from_slice(&buf[..len]);  // ~8ns: small memcpy
        buf.advance(len);
        data
    }
}
```

**Expected savings:** -13ns per key/value argument
**Risk:** Low (Bytes::slice is well-tested; read buffer held longer via Arc)
**Acceptance:** `cargo bench -- 1_resp_parse_get_cmd` drops below 65ns

### OPT-7: Slab Segment Allocator (Phase B — Core)

**File:** `src/storage/dashtable/mod.rs`

**Problem:** Each DashTable segment is individually heap-allocated, paying mimalloc/jemalloc per-allocation metadata (~16-32B per segment, amortized to ~25B per key).

**Solution:** Pre-allocate segments in contiguous slabs:

```rust
struct SegmentStore {
    slabs: Vec<Vec<MaybeUninit<Segment>>>,  // Contiguous blocks
    free_list: Vec<(usize, usize)>,          // (slab_idx, slot_idx)
    slab_size: usize,                        // Segments per slab (e.g., 64)
}
```

**Expected savings:** -25B/key RSS
**Risk:** Low (segments already in Vec; this formalizes the pattern)
**Acceptance:** `profile.sh --section memory` shows <230B/key at 1M with jemalloc+slab

### OPT-8: Arena-Allocated Frames (Phase C — Advanced)

**File:** `src/protocol/parse.rs`, `src/protocol/frame.rs`, dispatch path

**Problem:** `Frame::Array(Vec<Frame>)` allocates a heap Vec per command. For pipelined batches, this is N allocations.

**Solution:** Allocate frames from per-connection bumpalo arena. Reset after each pipeline batch:

```rust
// New: ArenaFrame<'a> with BumpVec
enum ArenaFrame<'a> {
    Array(BumpVec<'a, ArenaFrame<'a>>),
    BulkString(Bytes),
    // ...
}

// Parse into arena
fn parse_frame<'a>(buf: &mut BytesMut, arena: &'a Bump) -> ArenaFrame<'a> {
    let mut items = BumpVec::with_capacity_in(argc, arena);  // ~2ns bump
    // ...
}

// After dispatch+serialize: arena.reset()  — O(1) for all frames
```

**Expected savings:** -18ns per command (Vec alloc → bump alloc)
**Risk:** Medium (lifetime parameter propagation through ~15 files)
**Acceptance:** `cargo bench -- 1_resp_parse_get_cmd` drops below 50ns

### OPT-9: writev Zero-Copy GET (Phase C — Advanced)

**File:** `src/io/uring_driver.rs`, `src/shard/mod.rs`

**Problem:** GET response copies value data from DashTable to response BytesMut buffer (14ns for 256B memcpy). With writev, kernel reads directly from DashTable memory.

**Solution:** Use 3-iovec writev for GET responses:

```
iov[0]: "$256\r\n"        ← stack buffer (6 bytes)
iov[1]: entry.as_bytes()  ← DIRECT POINTER into DashTable segment
iov[2]: "\r\n"            ← static &[u8]
```

**Expected savings:** -14ns per GET (eliminate value memcpy)
**Risk:** Medium (must hold segment reference until CQE; WritevGuard pattern from Phase 12)
**Acceptance:** io_uring GET throughput improvement measurable at p=16+

---

## Projected Results

### After Phase A (Quick Wins, 1 week)

| Metric | Before | After | Change |
|--------|-------:|------:|-------:|
| GET hot path | 174ns | 153ns | -12% |
| Memory/key | 280B | 245B | -13% |
| CPU efficiency | 3,908 | 4,300 | +10% |

### After Phase B (Core, 2 weeks)

| Metric | Before | After | Change |
|--------|-------:|------:|-------:|
| GET hot path | 153ns | 106ns | -31% |
| Memory/key | 245B | 220B | -10% |
| p=16 throughput | 1.5x Redis | ~1.8x Redis | +20% |

### After Phase C (Advanced, 2 weeks)

| Metric | Before | After | Change |
|--------|-------:|------:|-------:|
| GET hot path | 106ns | 92ns | -13% |
| p=128 peak | 5.4M GET/s | ~7M GET/s | +30% |
| vs Redis gap | 3.2x per-cmd | 1.7x per-cmd | -47% |

---

## Implementation Order

| Priority | ID | Solution | Effort | Files |
|---------:|---:|---------|-------:|-------|
| 1 | OPT-1 | DashTable prefetch | 2 days | dashtable/mod.rs |
| 2 | OPT-2 | Branchless CompactKey eq | 1 day | compact_key.rs |
| 3 | OPT-3 | jemalloc default | 1 day | Cargo.toml, main.rs |
| 4 | OPT-4 | No-Arc local responses | 3 days | shard/mod.rs, connection.rs |
| 5 | OPT-5 | Direct GET serialization | 1 week | string.rs, connection.rs |
| 6 | OPT-6 | Zero-copy arg slicing | 3 days | parse.rs |
| 7 | OPT-7 | Slab segment allocator | 4 days | dashtable/mod.rs |
| 8 | OPT-8 | Arena-allocated frames | 2 weeks | parse.rs, frame.rs, ~15 files |
| 9 | OPT-9 | writev zero-copy GET | 1 week | uring_driver.rs, shard/mod.rs |
