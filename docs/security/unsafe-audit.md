# Moon Unsafe Code Audit Report

**Date:** 2026-04-09 (Phase 90 of v0.1.3 Production Readiness)
**Total unsafe blocks:** 156
**SAFETY comment coverage:** 100% (CI-enforced by `scripts/audit-unsafe.sh`)
**Reviewer:** Claude Code (automated) + human review pending

## Overview

Moon uses `unsafe` in performance-critical paths where the compiler cannot verify invariants that are guaranteed by the surrounding design. Every `unsafe` block has a `// SAFETY:` comment explaining the relied-upon invariant. New unsafe blocks without SAFETY comments fail CI.

## Categories

### 1. Tagged Pointer Unions (25 blocks)

**Files:** `src/storage/compact_value.rs` (13), `src/storage/compact_key.rs` (1), `src/storage/dashtable/segment.rs` (11 via MaybeUninit)

**Pattern:** CompactValue and CompactKey use the low 3 bits of a pointer as a type tag (SSO vs heap, string vs collection). Dereference is safe because the tag is set during construction and verified before each access.

**Invariant:** Tag bits are always consistent with the allocated type. Construction and destruction are paired — every `Box::into_raw` has a matching `Box::from_raw` in `Drop`.

**Risk:** Medium. A tag corruption would cause type confusion. Mitigated by: the tag is never exposed to user input, only set in constructors. Fuzzing covers the command paths that create/read values.

### 2. Swiss Table SIMD Probing (9 blocks)

**Files:** `src/storage/dashtable/segment.rs` (6), `src/storage/dashtable/simd.rs` (3)

**Pattern:** SIMD H2 control byte comparison + `get_unchecked` / `get_unchecked_mut` on fixed-size arrays after hash probing determines the slot index.

**Invariant:** Slot indices are bounded by `SLOTS_PER_SEGMENT` (60). The hash probing loop's modular arithmetic guarantees indices stay in range. SIMD operations use `#[target_feature]` gates.

**Risk:** Low. The segment size is a compile-time constant. Index out-of-bounds would require a bug in the probing loop, which is exercised by every GET/SET operation (high coverage).

### 3. Atomic State Machine (4 blocks)

**Files:** `src/server/response_slot.rs`

**Pattern:** `UnsafeCell<Option<Vec<Frame>>>` accessed after `AtomicU8` state check. Producer writes data then stores `FILLED` with `Release`. Consumer loads with `Acquire`, sees `FILLED`, reads data.

**Invariant:** Single-producer, single-consumer. `Release/Acquire` ordering provides happens-before. `fill()` always writes `Some(data)`.

**Risk:** Medium-high. Memory ordering bugs are subtle. Mitigated by: loom model tests (Phase 89) verify the state machine under all interleavings. The `atomic-waker` crate handles waker synchronization.

### 4. io_uring Kernel Interface (6 blocks)

**Files:** `src/io/uring_driver.rs` (4), `src/io/buf_ring.rs` (2)

**Pattern:** Submission queue entry (SQE) construction and registered buffer/FD management for multishot accept, recv, send.

**Invariant:** FDs are registered before use. Buffer ring entries point to valid, pinned memory. SQE opcodes match the registered resource types.

**Risk:** Low-medium. Kernel validates most invariants. The risk is double-free of FDs or use-after-free of buffers. Mitigated by: `ConnState` lifecycle tracks FD ownership; buffer ring entries are pre-allocated and pinned.

### 5. File Descriptor Ownership Transfer (11 blocks)

**Files:** `src/shard/conn_accept.rs` (4), `src/shard/event_loop.rs` (3), `src/server/listener.rs` (3), `src/server/conn/handler_sharded.rs` (1)

**Pattern:** `OwnedFd::from_raw_fd(fd)` after `dup()` or `accept()` syscalls, and `libc::close(fd)` for cleanup.

**Invariant:** The raw FD is valid and not owned by any other `OwnedFd`. `dup()` returns a new FD; `accept()` returns a new FD. Transfer to `OwnedFd` happens exactly once per FD.

**Risk:** Low. FD double-close would cause EBADF, not memory corruption. The ownership transfer is auditable by grep for `from_raw_fd`.

### 6. HNSW Pointer Arithmetic (5 blocks)

**Files:** `src/vector/hnsw/search.rs`

**Pattern:** Raw pointer arithmetic for TQ-ADC distance computation — reading quantized codes and LUT entries via `*ptr.add(offset)`.

**Invariant:** Offsets are bounded by the vector dimension and quantization parameters, computed once during index build. Buffer sizes are allocated to match.

**Risk:** Medium. An off-by-one in offset computation would read garbage. Mitigated by: unit tests cover exact distance values; fuzzing of the FT.SEARCH path exercises the search loop.

### 7. Other (6 blocks)

- `src/vector/distance/avx512.rs` (1): AVX-512 intrinsic behind `is_x86_feature_detected!` + test-only
- `src/vector/diskann/uring_search.rs` (1): `libc::open` with validated path
- `src/vector/persistence/sealed_mmap.rs` (1): `mmap` with validated FD and size
- `src/storage/dashtable/mod.rs` (2): `_mm_prefetch` intrinsic (hint only, no soundness risk)
- `src/storage/dashtable/iter.rs` (1): Raw pointer iteration over segment array (bounded by segment count)

## Enforcement

- **CI script:** `scripts/audit-unsafe.sh` checks every `unsafe {` block for a preceding `// SAFETY:` comment. Fails the build on any missing annotation.
- **Policy:** New unsafe blocks require explicit user approval per `UNSAFE_POLICY.md`. The SAFETY comment must explain the invariant; "I know what I'm doing" is not acceptable.
- **Fuzzing:** `cargo-fuzz` targets cover the RESP parser (which feeds data into the command dispatch → storage path), WAL/RDB decoders, and cluster/ACL parsers. Any unsafe-related crash is a P0.
- **Loom:** Model tests verify the `ResponseSlot` atomic state machine under all interleavings.

## Recommendations for v1.0

1. **Reduce DashTable unsafe surface** — consider `MaybeUninit::assume_init_read` replacements with `Option<T>` in non-hot paths. The segment code has 17 unsafe blocks; halving this is feasible.
2. **Eliminate CompactValue tagged union** — Rust enums with `repr(C)` + `niche` optimization may achieve similar size. Measure before changing — the 16-byte layout is load-bearing.
3. **miri coverage** — run `cargo +nightly miri test --lib` on the storage + protocol modules. io_uring code won't work under miri but the core data structures will.
4. **External audit** — a second pair of eyes on the response_slot atomic ordering and the compact_value tag discipline before v1.0 GA.
