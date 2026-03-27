# Phase 12: io_uring Networking Layer - Research

**Researched:** 2026-03-24
**Domain:** Linux io_uring kernel interface for high-performance async networking
**Confidence:** HIGH

## Summary

This phase replaces the existing Tokio-based epoll/kqueue I/O path with direct io_uring operations per shard thread. The current codebase uses `Framed<TcpStream, RespCodec>` via `tokio_util::codec` for all network I/O, with Tokio `current_thread` runtimes per shard (Phase 11). No io_uring code exists yet.

The recommended approach is to use the low-level `io-uring` crate (v0.7.11) directly rather than adopting a new runtime (Glommio/Monoio/compio). The Phase 11 architecture already runs per-shard `current_thread` Tokio runtimes on dedicated OS threads -- this is the ideal setup for io_uring's `IORING_SETUP_SINGLE_ISSUER` + `IORING_SETUP_DEFER_TASKRUN` flags. Rather than replacing the entire runtime, we build a custom io_uring event loop that integrates with the existing shard infrastructure via a platform abstraction trait gated on `#[cfg(target_os = "linux")]`.

Apache Iggy's recent migration (Feb 2026) to thread-per-core io_uring in Rust validates this approach, achieving 57% p95 and 60% p99 latency improvements. Their key lesson: holding `RefCell` borrows across `.await` points causes runtime panics -- our codebase already uses this pattern in `shard/mod.rs` and must be carefully managed.

**Primary recommendation:** Use `io-uring` 0.7.11 directly with a custom per-shard event loop. Define a `trait IoDriver` abstraction with `UringDriver` (Linux) and `TokioDriver` (macOS fallback) implementations behind `#[cfg(target_os)]` gates. Keep Tokio as the timer/signal infrastructure on all platforms.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Each shard thread operates one io_uring instance (Glommio manages this internally via three rings: main, latency-sensitive, NVMe polling)
- Ring size: 256 entries (tunable) -- sufficient for hundreds of concurrent connections per shard
- Kernel polling mode (IORING_SETUP_SQPOLL) evaluated but NOT enabled by default
- Multishot accept: single SQE on listener socket continuously produces CQEs (Linux 5.19+)
- Multishot recv with provided buffer rings: 256 buffers x 4KB each = 1MB per shard (Linux 6.0+)
- Pre-register all client socket FDs via IORING_REGISTER_FILES
- Pre-map read/write buffers via IORING_REGISTER_BUFFERS
- SQE batching: accumulate all submissions per event loop iteration, single io_uring_submit()
- GET responses use writev scatter-gather: iovec[0]=RESP header, iovec[1]=DashTable value, iovec[2]=trailing CRLF
- Pre-computed static responses for +OK, +PONG, $-1, :0 through :999
- io_uring is Linux-only; macOS/development uses fallback path
- Feature flag #[cfg(target_os = "linux")] gates io_uring-specific optimizations

### Claude's Discretion
- Exact ring size tuning (256 vs 512 vs 1024 entries)
- Whether to use io_uring_prep_send_zc (zero-copy send, Linux 6.1+) for large responses
- Buffer ring sizing per shard (balance memory vs connection count)
- Whether Glommio's internal io_uring management is sufficient or needs custom ring setup

### Deferred Ideas (OUT OF SCOPE)
- io_uring for persistence writes (DMA file I/O) -- Phase 14
- SQPOLL kernel thread mode -- evaluate after baseline io_uring is working
</user_constraints>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| io-uring | 0.7.11 | Low-level io_uring Rust bindings | Maintained by tokio-rs, covers all opcodes (AcceptMulti, RecvMulti, Writev, SendZc), active development |
| io_uring_buf_ring | 0.2.3 | Safe provided buffer ring management | Wraps io-uring's buf_ring API with safe Rust ownership semantics, depends on io-uring 0.7.11 |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| libc | (already dep of io-uring) | iovec struct, socket flags | Writev scatter-gather, MSG_* flags |
| nix | 0.29 | Socket creation, setsockopt (SO_REUSEPORT, TCP_NODELAY) | Listener socket setup before passing fd to io_uring |
| itoa | 1 (already in Cargo.toml) | Fast integer formatting for RESP | Response header assembly ($<len>\r\n) |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| io-uring (direct) | Glommio 0.9.0 | Glommio provides full runtime but requires replacing Tokio entirely; last release Mar 2024; macOS support limited; project already on Tokio current_thread which works well |
| io-uring (direct) | Monoio 0.2.4 | ByteDance production-proven but requires buffer ownership transfer model (GAT traits) incompatible with current Framed codec; last release Aug 2024 |
| io-uring (direct) | tokio-uring 0.5.0 | Limited maintenance, no multishot support (open issue since 2022), last release May 2024 |
| io-uring (direct) | compio 0.18.0 | Apache Iggy uses it; newer but still maturing; adds heap allocation per I/O request |
| Custom event loop | Keep Tokio+epoll | Phase goal is specifically to exploit io_uring; Tokio path remains as macOS fallback |

### Decision: Direct io-uring over runtime replacement

The CONTEXT.md references "Glommio manages this internally" but Phase 11 actually implemented Tokio `current_thread` per shard (not Glommio). The Glommio runtime swap was deferred. Given:
1. The codebase is stable on Tokio current_thread per shard
2. Glommio/Monoio would require rewriting all async I/O (connection handling, SPSC drain, timers)
3. The io-uring crate provides all needed opcodes directly
4. A custom event loop gives maximum control over SQE batching and buffer management

**Recommendation:** Use io-uring 0.7.11 directly with a custom per-shard event loop on Linux. Keep Tokio for timers, signals, and macOS fallback.

**Installation:**
```bash
# Add to Cargo.toml under [target.'cfg(target_os = "linux")'.dependencies]
# io-uring = "0.7"
# io_uring_buf_ring = "0.2"
# nix = { version = "0.29", features = ["net", "socket"] }
```

## Architecture Patterns

### Recommended Project Structure
```
src/
  io/
    mod.rs              # trait IoDriver + #[cfg] re-exports
    uring_driver.rs     # Linux: UringDriver wrapping io_uring::IoUring
    tokio_driver.rs     # macOS/fallback: TokioDriver using Framed<TcpStream>
    buf_ring.rs         # Linux: provided buffer ring management
    fd_table.rs         # Linux: registered file descriptor table
    static_responses.rs # Pre-computed static &'static [u8] responses
  server/
    listener.rs         # Updated: multishot accept on Linux, Tokio accept on macOS
    connection.rs       # Updated: uses IoDriver trait for reads/writes
```

### Pattern 1: Platform-Gated I/O Driver Trait
**What:** Abstract the I/O submission/completion behind a trait so connection handlers are platform-agnostic.
**When to use:** Always -- this is the core abstraction enabling the macOS fallback.

```rust
// src/io/mod.rs

#[cfg(target_os = "linux")]
pub mod uring_driver;
#[cfg(not(target_os = "linux"))]
pub mod tokio_driver;

pub mod static_responses;

/// Represents a received data buffer from the I/O layer.
/// On Linux: borrowed from provided buffer ring, must be returned after processing.
/// On macOS: owned BytesMut from Tokio read.
pub enum RecvBuf<'a> {
    #[cfg(target_os = "linux")]
    Uring { data: &'a [u8], buf_id: u16 },
    Owned(bytes::BytesMut),
}
```

### Pattern 2: Per-Shard io_uring Event Loop
**What:** Each shard thread runs its own io_uring instance with SINGLE_ISSUER + DEFER_TASKRUN flags.
**When to use:** Linux shard threads.

```rust
// Conceptual event loop structure (Linux path)
use io_uring::{IoUring, opcode, types, squeue::Flags};

fn shard_event_loop(ring: &mut IoUring, /* shard state */) {
    loop {
        // 1. Submit all accumulated SQEs from previous iteration
        ring.submit_and_wait(1).unwrap();

        // 2. Process all CQEs
        let cq = ring.completion();
        for cqe in cq {
            match decode_user_data(cqe.user_data()) {
                Event::Accept { fd } => handle_new_connection(fd, ring),
                Event::Recv { conn_id, buf_id, len } => {
                    // Parse RESP from provided buffer
                    // Execute commands
                    // Queue writev SQE for response
                }
                Event::Send { conn_id } => {
                    // Return buffer to ring, re-arm recv
                }
                Event::Timeout => {
                    // Run expiry cycle, drain SPSC
                }
            }
        }

        // 3. Accumulate new SQEs (batched, single submit next iteration)
    }
}
```

### Pattern 3: user_data Encoding for CQE Routing
**What:** Pack event type + connection ID into the 64-bit user_data field of each SQE, so CQE processing can route completions without a lookup table.
**When to use:** All io_uring submissions.

```rust
// Encode: [8-bit event_type][24-bit connection_id][32-bit aux_data]
const EVENT_ACCEPT: u8 = 1;
const EVENT_RECV: u8 = 2;
const EVENT_SEND: u8 = 3;
const EVENT_TIMEOUT: u8 = 4;

fn encode_user_data(event_type: u8, conn_id: u32, aux: u32) -> u64 {
    ((event_type as u64) << 56) | ((conn_id as u64 & 0xFF_FFFF) << 32) | (aux as u64)
}

fn decode_user_data(data: u64) -> (u8, u32, u32) {
    let event_type = (data >> 56) as u8;
    let conn_id = ((data >> 32) & 0xFF_FFFF) as u32;
    let aux = data as u32;
    (event_type, conn_id, aux)
}
```

### Pattern 4: Zero-Copy GET Response via writev
**What:** Scatter-gather response assembly for GET: header + DashTable value slice + CRLF trailer.
**When to use:** GET responses where value is stored in DashTable.

```rust
use libc::iovec;

// For GET key -> value "hello" (5 bytes)
// iovec[0]: "$5\r\n" (header, stack-allocated via itoa)
// iovec[1]: value data directly from DashTable (&[u8] into shard-local storage)
// iovec[2]: "\r\n" (static)
fn build_get_response_iovecs(value: &[u8], header_buf: &mut [u8; 32]) -> ([iovec; 3], usize) {
    let mut pos = 0;
    header_buf[pos] = b'$';
    pos += 1;
    pos += itoa::Buffer::new().format(value.len()).len(); // ... write length
    header_buf[pos] = b'\r';
    header_buf[pos + 1] = b'\n';
    pos += 2;

    let iovecs = [
        iovec { iov_base: header_buf.as_ptr() as *mut _, iov_len: pos },
        iovec { iov_base: value.as_ptr() as *mut _, iov_len: value.len() },
        iovec { iov_base: b"\r\n".as_ptr() as *mut _, iov_len: 2 },
    ];
    (iovecs, 3)
}
```

### Pattern 5: Registered File Descriptor Table
**What:** Maintain a fixed-size fd table registered with io_uring. New connections get a slot; closed connections free it.
**When to use:** All socket operations on Linux path.

```rust
// Fixed fd table management
struct FdTable {
    fds: Vec<i32>,      // -1 = empty slot
    free_slots: Vec<u32>,
}

impl FdTable {
    fn new(capacity: usize) -> Self { /* ... */ }
    fn insert(&mut self, fd: i32) -> Option<u32> { /* returns fixed_fd index */ }
    fn remove(&mut self, idx: u32) -> i32 { /* returns raw fd, marks slot free */ }
}
// After insert/remove, call ring.submitter().register_files_update(idx, &[fd])
```

### Anti-Patterns to Avoid
- **Submitting one SQE at a time:** Each `io_uring_submit()` is a syscall. Always batch all pending SQEs and submit once per event loop iteration.
- **Not returning provided buffers promptly:** Holding provided buffers too long starves the ring. Parse the RESP frame and return the buffer before executing commands.
- **Blocking in the event loop:** io_uring is async but the event loop itself is synchronous. Never call blocking operations (file I/O, lock contention) in the completion handler.
- **Assuming submission order = completion order:** io_uring does NOT guarantee ordering. Use user_data to correlate completions. Use IOSQE_IO_LINK only when ordering is required (e.g., writev then close).
- **Holding RefCell borrows across yield points:** The shard codebase uses `Rc<RefCell<Vec<Database>>>`. If the io_uring event loop yields (via Tokio integration), borrows must be scoped. Apache Iggy hit this exact panic.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Provided buffer ring management | Custom mmap + atomic tail updates | `io_uring_buf_ring` crate 0.2.3 | Subtle memory ordering requirements, unsafe mmap lifecycle |
| Socket creation with options | Raw syscalls | `nix` crate socket + setsockopt | SO_REUSEPORT, TCP_NODELAY, IPPROTO_TCP correctly |
| Integer formatting in RESP headers | format! or manual digit extraction | `itoa` crate (already in deps) | 3x faster than std::fmt, no allocation |
| CRLF scanning in received data | byte-by-byte loop | `memchr` crate (already in deps) | SIMD-accelerated, 6.5x faster |
| io_uring opcode building | Raw SQE field manipulation | `io-uring` crate opcode builders | Type-safe, handles flag combinations correctly |

**Key insight:** The io_uring kernel API is intricate with many subtle flag interactions. The `io-uring` crate's type-safe opcode builders prevent misuse (e.g., forgetting IOSQE_BUFFER_SELECT with RecvMulti, or setting incompatible flags).

## Common Pitfalls

### Pitfall 1: Buffer Lifetime with Provided Buffer Rings
**What goes wrong:** Provided buffer is returned to the ring while the RESP parser still references it, causing use-after-free or data corruption.
**Why it happens:** io_uring hands out buffer references from the ring; once returned, the kernel may overwrite them with new recv data.
**How to avoid:** Parse the complete RESP frame and copy any needed data (or advance the Bytes slice) BEFORE returning the buffer to the ring. For pipeline batching, copy all frames from the buffer, then return it.
**Warning signs:** Corrupted RESP frames appearing intermittently under load.

### Pitfall 2: Registered File Descriptor Table Exhaustion
**What goes wrong:** New connections fail with ENOMEM because the fixed fd table is full.
**Why it happens:** Fixed fd table has a static size set at registration time. Under connection storms, all slots fill up.
**How to avoid:** Size the table to `max_connections_per_shard + headroom`. Implement graceful fallback: when table is full, accept connection but use raw fd (unregistered) with a small performance penalty rather than rejecting.
**Warning signs:** Sporadic connection failures under high concurrency.

### Pitfall 3: CQE Overflow
**What goes wrong:** Completions are lost because the CQ is full when the kernel posts a new CQE.
**Why it happens:** Not draining completions frequently enough, or CQ too small relative to in-flight SQEs.
**How to avoid:** io_uring CQ is typically 2x SQ size. With SQ=256, CQ=512. Drain all CQEs every event loop iteration. Monitor `io_uring_cq_overflow` counter.
**Warning signs:** Missing recv completions, stalled connections.

### Pitfall 4: Multishot Accept Cancellation on Error
**What goes wrong:** A transient error (EMFILE, ENFILE) cancels the multishot accept, and no new connections are accepted.
**Why it happens:** Multishot operations are cancelled on error. The accept SQE must be re-submitted.
**How to avoid:** Check every accept CQE: if result < 0 and not ECANCELED, re-submit the multishot accept SQE. Monitor for repeated errors.
**Warning signs:** Server stops accepting connections after a burst.

### Pitfall 5: send_zc Notification CQE Confusion
**What goes wrong:** Zero-copy send produces TWO CQEs: one immediate (send queued) and one deferred (buffer safe to reuse). Code processes only the first, reuses buffer too early.
**Why it happens:** send_zc has dual-CQE semantics unlike regular send.
**How to avoid:** For this phase, use regular `Send` or `Writev` (not `SendZc`) unless responses exceed ~4KB. The copy overhead for small RESP responses is negligible vs. the complexity. Defer send_zc to later optimization.
**Warning signs:** Corrupted responses under high write load.

### Pitfall 6: macOS Development Cannot Test io_uring Paths
**What goes wrong:** io_uring code paths are never tested during development because all devs are on macOS.
**Why it happens:** io_uring is Linux-only.
**How to avoid:** Integration tests MUST run in Linux CI (Docker or VM). Use `#[cfg(target_os = "linux")]` test modules. The Tokio fallback path covers functional correctness; Linux CI covers io_uring correctness.
**Warning signs:** Tests pass locally but fail in Linux CI.

## Code Examples

### io_uring Ring Creation with Optimal Flags
```rust
// Source: io-uring crate docs + liburing wiki best practices
use io_uring::IoUring;

fn create_shard_ring(ring_size: u32) -> io_uring::IoUring {
    io_uring::IoUring::builder()
        .setup_single_issuer()    // One thread submits (our shard thread)
        .setup_defer_taskrun()    // Defer kernel work to submit_and_wait
        .setup_coop_taskrun()     // Avoid IPIs
        .build(ring_size)         // 256 default, tunable
        .expect("failed to create io_uring instance")
}
```

### Multishot Accept Setup
```rust
use io_uring::{opcode, types};

fn submit_multishot_accept(ring: &mut IoUring, listener_fd: i32) {
    let accept = opcode::AcceptMulti::new(types::Fd(listener_fd))
        .build()
        .user_data(encode_user_data(EVENT_ACCEPT, 0, 0));

    unsafe {
        ring.submission().push(&accept).expect("SQ full");
    }
}
```

### Multishot Recv with Provided Buffer Ring
```rust
use io_uring::{opcode, types, squeue::Flags};

fn submit_multishot_recv(
    ring: &mut IoUring,
    conn_fixed_fd: u32,
    buf_group_id: u16,
    conn_id: u32,
) {
    let recv = opcode::RecvMulti::new(types::Fixed(conn_fixed_fd), buf_group_id)
        .build()
        .user_data(encode_user_data(EVENT_RECV, conn_id, 0))
        .flags(Flags::BUFFER_SELECT);

    unsafe {
        ring.submission().push(&recv).expect("SQ full");
    }
}
```

### Static Response Table
```rust
// src/io/static_responses.rs

pub const OK: &[u8] = b"+OK\r\n";
pub const PONG: &[u8] = b"+PONG\r\n";
pub const NULL_BULK: &[u8] = b"$-1\r\n";
pub const EMPTY_ARRAY: &[u8] = b"*0\r\n";
pub const CRLF: &[u8] = b"\r\n";
pub const QUEUED: &[u8] = b"+QUEUED\r\n";

/// Pre-computed integer responses :0\r\n through :999\r\n
pub static INT_RESPONSES: once_cell::sync::Lazy<Vec<Vec<u8>>> =
    once_cell::sync::Lazy::new(|| {
        (0..1000)
            .map(|i| format!(":{}\r\n", i).into_bytes())
            .collect()
    });

pub fn integer_response(n: i64) -> Option<&'static [u8]> {
    if n >= 0 && n < 1000 {
        Some(&INT_RESPONSES[n as usize])
    } else {
        None
    }
}
```

### Writev Scatter-Gather Response
```rust
use io_uring::{opcode, types};
use libc::iovec;

fn submit_get_response(
    ring: &mut IoUring,
    conn_fixed_fd: u32,
    conn_id: u32,
    header: &[u8],      // "$5\r\n" (stack buffer)
    value: &[u8],       // Direct DashTable reference
) {
    // SAFETY: iovecs and referenced data must live until CQE completion.
    // In single-threaded shard, this is guaranteed because we process
    // CQEs before the next event loop iteration.
    let iovecs = [
        iovec { iov_base: header.as_ptr() as *mut _, iov_len: header.len() },
        iovec { iov_base: value.as_ptr() as *mut _, iov_len: value.len() },
        iovec { iov_base: CRLF.as_ptr() as *mut _, iov_len: 2 },
    ];

    let write = opcode::Writev::new(
        types::Fixed(conn_fixed_fd),
        iovecs.as_ptr(),
        iovecs.len() as u32,
    )
    .build()
    .user_data(encode_user_data(EVENT_SEND, conn_id, 0));

    unsafe {
        ring.submission().push(&write).expect("SQ full");
    }
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Individual accept() per connection | Multishot accept (one SQE, many CQEs) | Linux 5.19 (2022) | Eliminates per-accept syscall overhead |
| Pre-allocated recv buffers per connection | Provided buffer rings (kernel picks buffer on-demand) | Linux 6.0 (2022) | Eliminates buffer waste for idle connections |
| Legacy IORING_REGISTER_BUFFERS API | Ring-mapped provided buffers (io_uring_buf_ring) | Linux 5.19 (2022) | "Vastly more efficient" per liburing wiki |
| Regular send() | send_zc (zero-copy send) | Linux 6.0 (2022) | 2.4x faster for large payloads; negligible for <4KB |
| Default task work processing | DEFER_TASKRUN + SINGLE_ISSUER | Linux 6.1 (2022) | Significant efficiency gains for thread-per-core |
| epoll + readiness notification | io_uring completion-based I/O | Linux 5.1 (2019) | 10%+ throughput at 1000+ connections, 30% CPU reduction |

**Deprecated/outdated:**
- `IORING_OP_PROVIDE_BUFFERS` (legacy provided buffers): Use ring-mapped buf_ring instead (5.19+)
- `tokio-uring` for multishot: Issue #104 open since Aug 2022, no implementation
- `rio` crate: Unmaintained, last meaningful commit years ago

## Recommendations for Discretion Items

### Ring Size: 256 entries (keep default)
256 SQ entries with 512 CQ entries is sufficient for hundreds of concurrent connections per shard. Each connection has at most 1-2 in-flight operations (recv + send). With 256 connections at pipeline depth 4, the worst case is ~512 in-flight which just fits the CQ. If benchmarks show SQ full errors, bump to 512.

### send_zc: Do NOT use for Phase 12
Zero-copy send adds complexity (dual-CQE notification, buffer pinning) with negligible benefit for typical RESP responses (most are under 1KB). Regular `Writev` is zero-copy from DashTable to kernel buffer (single copy to NIC). Defer send_zc evaluation to Phase 23 benchmarking.

### Buffer Ring Sizing: 256 x 4KB = 1MB per shard (keep default)
256 buffers handles the common case well. Under pipeline=16 with 256 connections, peak concurrent recv buffers is ~256 if all connections are active. If `-ENOBUFS` errors appear, increase to 512 buffers. The 4KB buffer size matches typical RESP command frames.

### Glommio vs Custom: Custom ring setup
Phase 11 stayed on Tokio. The io-uring crate provides direct ring access with full control over flags and batching. No runtime replacement needed.

## Open Questions

1. **Integration with Tokio timers**
   - What we know: The shard event loop uses `tokio::time::interval` for expiry and SPSC drain. A pure io_uring event loop would need `IORING_OP_TIMEOUT` or a separate integration.
   - What's unclear: Whether to keep a Tokio runtime alongside io_uring (register io_uring fd with epoll) or replace timers with io_uring timeouts.
   - Recommendation: Use io_uring `IORING_OP_TIMEOUT` for the event loop's periodic tasks (expiry, SPSC drain). This keeps the critical path fully in io_uring without Tokio runtime overhead on Linux.

2. **SPSC channel integration**
   - What we know: Current shard drains SPSC consumers on a 1ms timer tick. In an io_uring event loop, there's no native "check channel" operation.
   - What's unclear: Best mechanism to wake the io_uring event loop when cross-shard messages arrive.
   - Recommendation: Use an eventfd per shard, registered with io_uring via `IORING_OP_POLL_ADD` (multishot). SPSC producer writes 1 to eventfd after push. io_uring CQE wakes the loop to drain SPSC.

3. **Connection state management**
   - What we know: Current code uses `Framed<TcpStream, RespCodec>` which maintains a read buffer per connection. With provided buffer rings, buffers are shared across connections.
   - What's unclear: How to handle partial RESP frames that span multiple recv completions.
   - Recommendation: Each connection needs a small `BytesMut` accumulation buffer for partial frames. When a provided buffer contains an incomplete frame, copy the partial data to the connection's buffer, return the provided buffer, and re-arm recv.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust built-in test + integration tests via redis crate |
| Config file | Cargo.toml (test section) |
| Quick run command | `cargo test --lib -- io` |
| Full suite command | `cargo test` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| P12-01 | Each shard operates one io_uring instance with multishot accept | integration (Linux CI) | `cargo test --test integration -- uring_accept` | No - Wave 0 |
| P12-02 | Multishot recv with provided buffer rings handles reads | integration (Linux CI) | `cargo test --test integration -- uring_recv` | No - Wave 0 |
| P12-03 | Client FDs pre-registered via IORING_REGISTER_FILES | unit (Linux) | `cargo test --lib -- io::fd_table` | No - Wave 0 |
| P12-04 | Read/write buffers pre-mapped via IORING_REGISTER_BUFFERS | unit (Linux) | `cargo test --lib -- io::buf_ring` | No - Wave 0 |
| P12-05 | SQE batching: single io_uring_submit() per loop iteration | unit (Linux) | `cargo test --lib -- io::uring_driver::batch` | No - Wave 0 |
| P12-06 | GET writev scatter-gather zero-copy | unit (Linux) | `cargo test --lib -- io::static_responses` | No - Wave 0 |
| P12-07 | >= 10% throughput improvement at 1000+ connections | manual benchmark (Linux) | `cargo bench` / bench.sh | No - Wave 0 |
| P12-08 | macOS fallback path maintained | integration | `cargo test --test integration` (run on macOS) | Yes (existing) |

### Sampling Rate
- **Per task commit:** `cargo test --lib` (unit tests, fast)
- **Per wave merge:** `cargo test` (full suite including integration)
- **Phase gate:** Full suite green on BOTH macOS and Linux CI

### Wave 0 Gaps
- [ ] `src/io/mod.rs` -- IoDriver trait + platform re-exports
- [ ] `src/io/static_responses.rs` -- pre-computed RESP responses
- [ ] `src/io/uring_driver.rs` -- UringDriver (Linux) with unit tests
- [ ] `src/io/tokio_driver.rs` -- TokioDriver (macOS fallback)
- [ ] `src/io/fd_table.rs` -- registered fd table management
- [ ] `src/io/buf_ring.rs` -- provided buffer ring wrapper
- [ ] Linux CI configuration for integration testing
- [ ] `#[cfg(target_os = "linux")]` test module in integration tests

## Sources

### Primary (HIGH confidence)
- [io-uring crate docs](https://docs.rs/io-uring/latest/io_uring/) - opcode API (AcceptMulti, RecvMulti, Writev, SendZc), Builder, Submitter
- [io-uring crate opcode source](https://docs.rs/io-uring/latest/src/io_uring/opcode.rs.html) - kernel version requirements per opcode
- [io_uring_buf_ring crate](https://docs.rs/io_uring_buf_ring/latest/io_uring_buf_ring/) - provided buffer ring safe API
- [liburing wiki: io_uring and networking in 2023](https://github.com/axboe/liburing/wiki/io_uring-and-networking-in-2023) - multishot, provided buffers, DEFER_TASKRUN
- [crates.io](https://crates.io) - verified versions: io-uring 0.7.11 (Nov 2025), io_uring_buf_ring 0.2.3 (Dec 2025)

### Secondary (MEDIUM confidence)
- [Apache Iggy thread-per-core io_uring blog](https://iggy.apache.org/blogs/2026/02/27/thread-per-core-io_uring/) - real-world Rust migration, 57% p95 improvement, RefCell pitfall
- [Red Hat: Why use io_uring for network I/O](https://developers.redhat.com/articles/2023/04/12/why-you-should-use-iouring-network-io) - registered files, batching benefits
- [LWN: multishot recv](https://lwn.net/Articles/899498/) - kernel design rationale
- [Phoronix: send_zc improvements](https://www.phoronix.com/news/Linux-6.10-IO_uring) - 2.4x over MSG_ZEROCOPY

### Tertiary (LOW confidence)
- [tokio-uring multishot issue #104](https://github.com/tokio-rs/tokio-uring/issues/104) - confirms no multishot in tokio-uring (open since 2022)
- [Rust forum: Status of tokio_uring](https://users.rust-lang.org/t/status-of-tokio-uring/114481) - limited maintenance status

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - io-uring 0.7.11 is the definitive Rust io_uring binding, maintained by tokio-rs, covers all needed opcodes
- Architecture: HIGH - platform abstraction via cfg gates is standard Rust practice; event loop pattern well-documented
- Pitfalls: HIGH - buffer lifetime, CQE overflow, multishot cancellation are well-documented in liburing wiki and real-world reports
- Performance targets: MEDIUM - 10% improvement is conservative per Alibaba benchmarks but actual results depend on workload and kernel version

**Research date:** 2026-03-24
**Valid until:** 2026-04-24 (30 days -- io-uring crate stable, kernel API frozen)
