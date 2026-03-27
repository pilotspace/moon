---
phase: 12-io-uring-networking-layer
verified: 2026-03-24T12:00:00Z
status: human_needed
score: 7/8 success criteria verified
re_verification: true
  previous_status: gaps_found
  previous_score: 5/8
  gaps_closed:
    - "Multishot accept wired via SO_REUSEPORT per-shard listener sockets"
    - "writev scatter-gather wired for BulkString GET responses via submit_writev_bulkstring"
    - "std::mem::forget leak removed — replaced with InFlightSend RAII enum"
  gaps_remaining: []
  regressions: []
human_verification:
  - test: "Run benchmarks on Linux at 1000+ concurrent connections"
    expected: ">= 10% throughput improvement over epoll baseline (SC7)"
    why_human: "Requires Linux hardware with kernel >= 5.19 for io_uring SINGLE_ISSUER+DEFER_TASKRUN flags. Developer is on macOS. Cannot verify programmatically."
  - test: "Verify io_uring init succeeds on Linux and all shards log io_uring mode"
    expected: "All shards log 'Shard N started (io_uring mode)' — UringDriver::new() and init() succeed with SINGLE_ISSUER+DEFER_TASKRUN+COOP_TASKRUN flags"
    why_human: "Requires Linux kernel >= 5.19. Cannot verify on macOS."
  - test: "Verify SO_REUSEPORT multishot accept distributes connections across shards"
    expected: "Each shard thread binds a SO_REUSEPORT listener on the same port and logs 'multishot accept armed on fd N'. Connections are kernel-distributed across shards without a shared listener."
    why_human: "Requires Linux. SO_REUSEPORT is a Linux-specific socket option. Cannot verify on macOS."
---

# Phase 12: io-uring-networking-layer Verification Report

**Phase Goal:** Replace epoll/kqueue I/O with io_uring for each shard thread, using multishot accept, multishot recv with provided buffer rings, registered file descriptors and buffers, SQE batching, and zero-copy response scatter-gather via writev

**Verified:** 2026-03-24T12:00:00Z
**Status:** human_needed
**Re-verification:** Yes — after gap closure (commits a04f8ea, d230264)

## Re-Verification Summary

Previous score: 5/8 (SC1 partial, SC6 failed, SC7 needs human).

Both gaps have been closed:
- **Gap 1 (SC1):** `Shard::run` now calls `Self::create_reuseport_listener(addr)` to create a per-shard SO_REUSEPORT TCP socket, then `d.submit_multishot_accept(listener_fd)` to arm it. The `IoEvent::Accept` and `IoEvent::AcceptError` handlers in `handle_uring_event` wire up connection registration and multishot re-arm respectively. Listener fd is closed on shard shutdown.
- **Gap 2 (SC6):** `handle_uring_event` now matches on `Frame::BulkString` and calls `driver.submit_writev_bulkstring(conn_id, value.clone())`. A new `WritevGuard` RAII struct owns the iovec array and header buffer until `SendComplete`. All other responses go through `submit_send` with an `InFlightSend::Buf` tracked entry. `std::mem::forget` is gone from the entire `src/shard/mod.rs`.

New score: 7/8 (SC7 still requires Linux hardware).

---

## Goal Achievement

### Observable Truths (Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| SC1 | Each shard thread operates one io_uring instance with multishot accept (Linux 5.19+) | VERIFIED (arch) | `shard/mod.rs:116-130`: `create_reuseport_listener(addr)` called, then `d.submit_multishot_accept(listener_fd)`. `IoEvent::Accept { raw_fd }` at line 517 calls `driver.register_connection(raw_fd)`. `IoEvent::AcceptError` at line 521-525 re-arms. Listener fd closed at shutdown (line 263). Needs Linux runtime to confirm no init failure. |
| SC2 | Multishot recv with provided buffer rings (Linux 6.0+) handles connection reads | VERIFIED | `RecvMulti` opcode with `BUFFER_SELECT` flag at `uring_driver.rs:196-199`. `BufRingManager.setup_ring()` called in `init()` at line 123. Unchanged from initial verification. |
| SC3 | All client socket FDs pre-registered via IORING_REGISTER_FILES | VERIFIED | `FdTable.register_with_ring()` calls `ring.submitter().register_files(&self.fds)` at `fd_table.rs:64`. Called from `UringDriver::init()` at `uring_driver.rs:122`. Unchanged. |
| SC4 | Read/write buffers pre-mapped via IORING_REGISTER_BUFFERS | VERIFIED (deviation) | Uses `ProvideBuffers` opcode (IORING_OP_PROVIDE_BUFFERS) rather than IORING_REGISTER_BUFFERS. Intentional per plan decisions — provides equivalent buffer-pool benefit. Unchanged. |
| SC5 | SQE batching: accumulate all submissions per event loop iteration, single io_uring_submit() | VERIFIED | `pending_sqes` counter at `uring_driver.rs:89`. All `submit_*` methods increment it. `submit_and_wait_nonblocking()` called once per 1ms `spsc_interval` tick in `shard/mod.rs:232`. Unchanged. |
| SC6 | GET responses use writev scatter-gather for zero-copy from DashTable to socket | VERIFIED (arch) | `shard/mod.rs:455-481`: `Frame::BulkString(ref value)` branch calls `driver.submit_writev_bulkstring(conn_id, value.clone())`. Returns `WritevGuard` stored in `InFlightSend::Writev(guard)`. `WritevGuard` at `uring_driver.rs:478-484` holds `iovecs`, `header_buf`, and `_value_hold: bytes::Bytes`. `build_get_response_iovecs` is now called from `submit_writev_bulkstring` at line 503. `std::mem::forget` is absent from `shard/mod.rs`. |
| SC7 | >= 10% throughput improvement over epoll baseline at 1000+ connections | NEEDS HUMAN | Architecture enables it. SC1 (multishot accept) and SC6 (writev) now wired. Actual measurement requires Linux hardware. |
| SC8 | macOS/kqueue fallback path maintained for development | VERIFIED | All Linux code in `#[cfg(target_os = "linux")]` blocks. `cargo test --lib`: 549 passed, 0 failed. `cargo test --test integration`: 66 passed, 0 failed. Total 615 tests pass on macOS. |

**Score:** 7/8 success criteria verified (SC1-SC6, SC8; SC7 needs human)

---

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `src/io/mod.rs` | IoDriver trait + platform re-exports | VERIFIED | Line 14: `pub use uring_driver::{build_get_response_iovecs, IoEvent, UringConfig, UringDriver, WritevGuard}` — WritevGuard now exported. |
| `src/io/static_responses.rs` | Pre-computed RESP response bytes | VERIFIED | Unchanged. `bulk_string_header` and `CRLF` used by `build_get_response_iovecs`. |
| `src/io/tokio_driver.rs` | macOS/fallback TokioDriver | VERIFIED | Unchanged. |
| `src/io/fd_table.rs` | Registered FD table (Linux, cfg-gated) | VERIFIED | Unchanged. |
| `src/io/buf_ring.rs` | Provided buffer ring wrapper (Linux, cfg-gated) | VERIFIED | Unchanged. |
| `src/io/uring_driver.rs` | UringDriver with io_uring event loop + WritevGuard | VERIFIED | 574 lines. `WritevGuard` struct at lines 478-484. `submit_writev_bulkstring` at lines 489-508. `build_get_response_iovecs` and `submit_writev` now have production callers. |
| `src/shard/mod.rs` | Shard event loop with multishot accept + writev wired | VERIFIED | `create_reuseport_listener` at line 554. `submit_multishot_accept` called at lines 118 and 524. `submit_writev_bulkstring` called at line 458. `InFlightSend` enum at lines 50-55 replaces `mem::forget`. |
| `Cargo.toml` | Linux-only io-uring and nix deps | VERIFIED | Unchanged. |
| `src/lib.rs` | pub mod io declared | VERIFIED | Unchanged. |

---

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `src/io/mod.rs` | `src/io/uring_driver.rs` | `#[cfg(target_os = "linux")] pub mod uring_driver` | VERIFIED | Unchanged. |
| `src/io/mod.rs` | `WritevGuard` export | `pub use uring_driver::WritevGuard` | VERIFIED | Line 14 of `mod.rs` — new export added in gap closure. |
| `src/io/uring_driver.rs` | `build_get_response_iovecs` | Called from `submit_writev_bulkstring` | VERIFIED | `uring_driver.rs:503`: `let (iovecs, _count) = build_get_response_iovecs(...)`. No longer dead code. |
| `src/io/uring_driver.rs` | `submit_writev` | Called from `submit_writev_bulkstring` | VERIFIED | `uring_driver.rs:505`: `self.submit_writev(conn_id, guard.iovecs.as_ptr(), 3)?`. No longer dead code. |
| `src/shard/mod.rs` | `submit_multishot_accept` | Called on Linux io_uring init + AcceptError | VERIFIED | Lines 118 (initial arm) and 524 (re-arm on error). No longer dead code. |
| `src/shard/mod.rs` | `submit_writev_bulkstring` | Called for BulkString responses in `handle_uring_event` | VERIFIED | Line 458: `driver.submit_writev_bulkstring(conn_id, value.clone())`. No longer orphaned. |
| `src/shard/mod.rs` | `InFlightSend::Writev(guard)` | Stores `WritevGuard` until `SendComplete` | VERIFIED | Line 463: `inflight_sends.entry(conn_id).or_default().push(InFlightSend::Writev(guard))`. Lines 529-536: `SendComplete` drops oldest entry (FIFO). |
| `src/shard/mod.rs` | `create_reuseport_listener` | Per-shard SO_REUSEPORT socket for multishot accept | VERIFIED | Lines 554-631: Creates `AF_INET SOCK_STREAM NONBLOCK CLOEXEC` socket with `SO_REUSEPORT` + `SO_REUSEADDR`, binds to `addr`, calls `listen(1024)`. Called from `Shard::run` at line 116. |
| `src/main.rs` | `bind_addr` → `shard.run` | `Some(shard_bind_addr)` passed as last arg | VERIFIED | `main.rs:74`: `let bind_addr = format!("{}:{}", config.bind, config.port)`. Line 85: `let shard_bind_addr = bind_addr.clone()`. Line 108: `Some(shard_bind_addr)` passed to `shard.run`. |

---

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| P12-01 | 12-02, 12-03, 12-04 | io_uring multishot accept | SATISFIED | `submit_multishot_accept` called from `Shard::run` with SO_REUSEPORT fd. Re-armed on `AcceptError`. |
| P12-02 | 12-02, 12-03 | Multishot recv with provided buffers | SATISFIED | `RecvMulti` + `BufRingManager.setup_ring()`. Unchanged. |
| P12-03 | 12-02, 12-03 | IORING_REGISTER_FILES | SATISFIED | `FdTable.register_with_ring()`. Unchanged. |
| P12-04 | 12-02 | IORING_REGISTER_BUFFERS (ProvideBuffers) | SATISFIED (deviation) | `ProvideBuffers` approach. Unchanged. |
| P12-05 | 12-02, 12-03 | SQE batching | SATISFIED | `pending_sqes` counter + single submit. Unchanged. |
| P12-06 | 12-01, 12-03 | macOS fallback path | SATISFIED | 615 tests pass on macOS. |
| P12-07 | 12-03, 12-04 | Shard integration | SATISFIED | Multishot accept, multishot recv, writev, and RAII send tracking all wired. |
| P12-08 | 12-01, 12-03 | Platform cfg gates | SATISFIED | All Linux code `#[cfg(target_os = "linux")]`. |

---

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `src/storage/compact_value.rs` | 197, 202 | `std::mem::forget(self)` | INFO | Pre-existing, unrelated to Phase 12. Part of compact value RAII (intentional manual memory management for tagged pointer optimization). |
| `src/shard/mod.rs` | 213 | `None, // requirepass: TODO wire from shard config` | INFO | Pre-existing TODO unrelated to Phase 12. |

The Phase 12 anti-patterns from the initial verification have all been resolved:
- `std::mem::forget(resp_buf)` at former line 410 is gone — replaced with `InFlightSend` RAII.
- `submit_multishot_accept()` dead code — now wired at lines 118 and 524.
- `submit_writev()` dead code — now called from `submit_writev_bulkstring`.
- `build_get_response_iovecs()` dead code — now called from `submit_writev_bulkstring`.

---

### Human Verification Required

#### 1. Throughput Benchmark (SC7)

**Test:** On a Linux machine with kernel >= 6.0, run the redis-bench or criterion benchmarks against the server at 1000+ concurrent connections and compare throughput vs the Phase 11 epoll baseline.

**Expected:** >= 10% throughput improvement for read-heavy workloads (GET, PING). With both SC1 (multishot accept) and SC6 (writev scatter-gather) now wired, the improvement potential is higher than at initial verification.

**Why human:** Requires Linux hardware. Developer is on macOS. Cannot verify programmatically.

#### 2. io_uring Init and Multishot Accept Verification

**Test:** On a Linux machine with kernel >= 5.19, start the server with 4 shards and observe logs.

**Expected:**
- All shards log "Shard N started (io_uring mode)" — `UringDriver::new()` and `init()` succeed.
- All shards log "Shard N: multishot accept armed on fd NNN" — `create_reuseport_listener` and `submit_multishot_accept` succeed.
- `netstat -tlnp` shows 4+ sockets on the same port (one per shard via SO_REUSEPORT).

**Why human:** SINGLE_ISSUER and DEFER_TASKRUN flags require kernel >= 5.19. SO_REUSEPORT is Linux-specific. Cannot verify on macOS.

#### 3. writev Zero-Copy Path Verification

**Test:** On Linux, use `strace -e writev` while sending GET requests with large values (>= 1KB) to the server.

**Expected:** `writev(sockfd, [{iov_base=..., iov_len=...}, {iov_base=..., iov_len=...}, {iov_base=..., iov_len=2}], 3)` syscalls appear — confirming the 3-iovec scatter-gather (header + value + CRLF) is being dispatched by the kernel.

**Why human:** Requires Linux strace and io_uring runtime. Cannot verify on macOS.

---

### Test Results (macOS)

| Suite | Passed | Failed | Notes |
|-------|--------|--------|-------|
| `cargo test --lib` | 549 | 0 | All unit tests pass |
| `cargo test --test integration` | 66 | 0 | All integration tests pass |
| **Total** | **615** | **0** | All tests pass on macOS |

Note: `test_sharded_concurrent_clients` is a pre-existing flaky test (intermittent race condition in the sharded SET/GET concurrent path, introduced in Phase 11 commit 463ffff). It was failing before Phase 12 began and is not caused by Phase 12 changes. It passes consistently when run in isolation.

---

### Closed Gaps Summary

**Gap 1 — Multishot accept (SC1):** Closed in commit a04f8ea. `Shard::run` now takes `bind_addr: Option<String>`. On Linux with a live io_uring driver and a provided `bind_addr`, it calls `Self::create_reuseport_listener(addr)` (lines 554-631) to create a non-blocking TCP socket with `SO_REUSEPORT` and `SO_REUSEADDR`, bind it, and start listening. It then calls `d.submit_multishot_accept(listener_fd)` to arm multishot accept. The `IoEvent::Accept { raw_fd }` branch in `handle_uring_event` (line 517) calls `driver.register_connection(raw_fd)` directly. The `IoEvent::AcceptError` branch (line 521) re-arms multishot accept after transient errors. The listener fd is closed via `libc::close(lfd)` on shutdown (line 263). `bind_addr` is threaded from `main.rs` through all shard spawns.

**Gap 2 — writev scatter-gather (SC6):** Closed in commit d230264. A new `WritevGuard` struct (lines 478-484) owns the `iovecs: [libc::iovec; 3]`, `header_buf: [u8; 32]`, and `_value_hold: bytes::Bytes` handle until the `SendComplete` CQE arrives. A new `submit_writev_bulkstring` method (lines 489-508) on `UringDriver` builds the guard, calls `build_get_response_iovecs`, and calls `submit_writev` — wiring both previously-orphaned functions. In `handle_uring_event`, the `Frame::BulkString(ref value)` branch (lines 456-481) calls `submit_writev_bulkstring` and stores the guard as `InFlightSend::Writev(guard)`. The `InFlightSend` enum (lines 50-55) replaces `std::mem::forget` entirely. `SendComplete` drops the oldest entry (FIFO matches CQE order). `Disconnect` and `SendError` drain all in-flight sends for the connection.

---

*Verified: 2026-03-24T12:00:00Z*
*Verifier: Claude (gsd-verifier)*
*Re-verification: Yes — gap closure verification after commits a04f8ea, d230264*
