---
phase: 27-compactkey-sso-for-key-storage-inline-short-keys-eliminate-bytes-heap-overhead
verified: 2026-03-25T10:00:00Z
status: passed
score: 4/4 must-haves verified
re_verification: false
---

# Phase 27: CompactKey SSO for Key Storage Verification Report

**Phase Goal:** Replace Bytes key representation in DashTable with CompactKey struct that inlines short keys (<=23 bytes) to eliminate heap allocations and improve cache locality.
**Verified:** 2026-03-25T10:00:00Z
**Status:** passed
**Re-verification:** No -- initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | CompactKey struct is exactly 24 bytes with inline path for keys <= 23 bytes and heap path for longer keys | VERIFIED | Compile-time assertion `assert!(size_of::<CompactKey>() == 24)` at line 35. INLINE_MAX = 23 constant. Heap path uses HEAP_FLAG 0x80 discriminant. 26 unit tests all pass covering both paths. |
| 2 | DashTable uses CompactKey instead of Bytes as key type with identical lookup API | VERIFIED | `impl<V> DashTable<CompactKey, V>` at dashtable/mod.rs:63. Zero `DashTable<Bytes` matches in codebase. get/get_mut/contains_key still accept `&[u8]` via `Borrow<[u8]>`. 38 DashTable tests pass. |
| 3 | All existing tests pass with CompactKey integration | VERIFIED | `cargo test --lib` reports 1020 passed, 0 failed, 0 ignored. |
| 4 | No heap allocation for keys <= 23 bytes (verified by unit tests) | VERIFIED | Tests `test_inline_empty`, `test_inline_one_byte`, `test_inline_max_23_bytes` verify `is_inline() == true`. Tests `test_heap_24_bytes`, `test_heap_100_bytes` verify `is_inline() == false` for keys > 23 bytes. |

**Score:** 4/4 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `src/storage/compact_key.rs` | CompactKey struct with SSO | VERIFIED | 457 lines, 24-byte struct with inline/heap paths, full trait suite (Hash, Eq, Ord, Clone, Drop, AsRef, Borrow, Debug, Send, Sync, From<Bytes/&[u8]/&str/String/Vec<u8>>), 26 unit tests |
| `src/storage/dashtable/mod.rs` | DashTable using CompactKey | VERIFIED | Contains `DashTable<CompactKey, V>` impl block, 38 tests all use `DashTable<CompactKey, String>` |
| `src/storage/db.rs` | Database with CompactKey keys | VERIFIED | `data: DashTable<CompactKey, Entry>` at line 267, `data()`, `data_mut()`, `check_expired()` all use `DashTable<CompactKey, Entry>` |
| `src/storage/mod.rs` | Module declaration | VERIFIED | Contains `pub mod compact_key;` |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `src/storage/dashtable/mod.rs` | `src/storage/compact_key.rs` | `use super::compact_key::CompactKey` | WIRED | Import at line 26, used in `impl<V> DashTable<CompactKey, V>` and all test types |
| `src/storage/db.rs` | `src/storage/compact_key.rs` | `use super::compact_key::CompactKey` | WIRED | Import at line 6, used in `DashTable<CompactKey, Entry>` field and method signatures |
| `src/storage/db.rs` | `src/storage/dashtable/mod.rs` | `DashTable<CompactKey, Entry>` | WIRED | Field declaration at line 267, methods at lines 448, 498, 953 |
| `src/persistence/rdb.rs` | `src/storage/compact_key.rs` | `use crate::storage::compact_key::CompactKey` | WIRED | Import at line 13, used in `save_from_snapshot` signature taking `Vec<(CompactKey, Entry)>` |
| `src/persistence/aof.rs` | `src/storage/compact_key.rs` | `use crate::storage::compact_key::CompactKey` | WIRED | Import at line 16, used in snapshot serialization |
| `src/storage/eviction.rs` | `src/storage/compact_key.rs` | `use crate::storage::compact_key::CompactKey` | WIRED | Import at line 6, used in `Vec<CompactKey>` for eviction sampling across all three policies |
| `src/command/key.rs` | `src/storage/compact_key.rs` | `use crate::storage::compact_key::CompactKey` | WIRED | Import at line 4, used for KEYS/SCAN key collection and sorting |
| `src/cluster/migration.rs` | CompactKey (indirect) | `key.as_bytes()` | WIRED | Iterates `db.keys()` (returns `&CompactKey`), calls `.as_bytes()` for slot calculation |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| COMPACTKEY-01 | 27-01 | CompactKey struct with SSO | SATISFIED | `src/storage/compact_key.rs` -- 457 lines, 24-byte struct, compile-time size assertion, 26 tests pass |
| COMPACTKEY-02 | 27-01 | DashTable uses CompactKey | SATISFIED | `DashTable<CompactKey, V>` impl, zero `DashTable<Bytes` matches, 38 DashTable tests pass |
| COMPACTKEY-03 | 27-02 | Full codebase wiring | SATISFIED | Database, persistence, eviction, commands, cluster all use CompactKey; 1020 tests pass |

**Note on requirement IDs:** The user referenced CK-01 through CK-04. The plans and ROADMAP use COMPACTKEY-01 through COMPACTKEY-03 (three requirements total). There is no CK-04/COMPACTKEY-04 defined anywhere. These COMPACTKEY requirements are not present in REQUIREMENTS.md -- they exist only in the ROADMAP and plan frontmatter.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| None | - | - | - | No anti-patterns found in any phase 27 files |

No TODOs, FIXMEs, placeholders, empty implementations, or stub handlers found in `compact_key.rs`, `dashtable/mod.rs`, or any modified files.

### Human Verification Required

### 1. Memory Allocation Verification

**Test:** Run a micro-benchmark or instrumented test with keys of varying lengths to confirm inline keys produce zero heap allocations.
**Expected:** Keys <= 23 bytes show no heap alloc (allocator instrumentation), keys > 23 bytes show exactly one allocation.
**Why human:** Cannot programmatically verify absence of heap allocation without allocator hooks or valgrind/DHAT profiling.

### 2. Performance Regression Check

**Test:** Run the existing benchmark suite (redis-benchmark or custom) comparing before/after CompactKey integration.
**Expected:** Equal or better throughput for typical workloads (most keys < 23 bytes). No regression for long keys.
**Why human:** Performance characteristics depend on runtime conditions and hardware.

### Gaps Summary

No gaps found. All four observable truths are verified. CompactKey is a substantive, well-tested 457-line implementation with proper unsafe discipline following the CompactValue SSO pattern. It is fully wired through DashTable, Database, persistence, eviction, commands, and cluster migration. All 1020 lib tests pass. Zero instances of `DashTable<Bytes` remain in the codebase.

---

_Verified: 2026-03-25T10:00:00Z_
_Verifier: Claude (gsd-verifier)_
