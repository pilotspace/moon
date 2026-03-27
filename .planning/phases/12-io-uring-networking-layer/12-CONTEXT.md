# Phase 12: io_uring Networking Layer - Context

**Gathered:** 2026-03-24
**Status:** Ready for planning

<domain>
## Phase Boundary

Replace epoll/kqueue-based I/O with io_uring for each shard thread, using multishot accept, multishot recv with provided buffer rings, registered file descriptors, registered buffers, SQE batching, and zero-copy response scatter-gather. This builds on Phase 11's thread-per-core runtime (Glommio/Monoio already provides io_uring integration) and optimizes the I/O path for maximum throughput.

</domain>

<decisions>
## Implementation Decisions

### io_uring configuration per shard
- [auto] Each shard thread operates one io_uring instance (Glommio manages this internally via three rings: main, latency-sensitive, NVMe polling)
- Ring size: 256 entries (tunable) — sufficient for hundreds of concurrent connections per shard
- Kernel polling mode (IORING_SETUP_SQPOLL) evaluated but NOT enabled by default — burns a kernel thread per ring, only beneficial at extreme throughput

### Multishot accept
- [auto] Single SQE on the listener socket continuously produces CQEs for new connections (Linux 5.19+)
- Eliminates per-accept submission overhead — one SQE serves all incoming connections
- Accepted FDs distributed to shard threads via SPSC channels (from Phase 11)

### Multishot recv with provided buffer rings
- [auto] Pre-registered buffer ring per shard (Linux 6.0+) — kernel selects buffers on-demand when data arrives
- Buffer ring size: 256 buffers × 4KB each = 1MB per shard (tunable)
- Eliminates pre-allocation waste for idle connections — buffer only consumed when data actually arrives
- After processing, buffer returned to ring for reuse

### Registered file descriptors and buffers
- [auto] Pre-register all client socket FDs via `IORING_REGISTER_FILES` — avoids per-I/O fd lookup overhead
- Pre-map read/write buffers via `IORING_REGISTER_BUFFERS` — eliminates per-I/O `get_user_pages()`
- Blueprint cites: ~30% CPU reduction for registered FDs, up to 3.5x fewer CPU cycles for registered buffers

### SQE batching
- [auto] Accumulate all submissions during one event loop iteration, flush with single `io_uring_submit()`
- At 32 batched operations: per-op syscall cost drops from ~1-5μs to 30-150ns
- Natural fit with pipeline batching from Phase 7 — parse batch → execute batch → submit all responses at once

### Zero-copy response path
- [auto] For GET responses: scatter-gather via `writev` (or `io_uring_prep_writev`):
  - iovec[0]: RESP header `$<len>\r\n` (small stack buffer)
  - iovec[1]: value data referenced directly from DashTable (zero-copy — safe in single-threaded shard)
  - iovec[2]: trailing `\r\n` (static &[u8])
- Pre-computed static responses for common cases: `+OK\r\n`, `+PONG\r\n`, `$-1\r\n`, `:0\r\n` through `:999\r\n`

### Platform fallback
- [auto] io_uring is Linux-only (kernel 5.19+ for multishot accept, 6.0+ for multishot recv)
- macOS/development: Glommio or Monoio provide kqueue fallback transparently
- Feature flag `#[cfg(target_os = "linux")]` gates io_uring-specific optimizations
- Core logic works on any platform — only the I/O submission path differs

### Claude's Discretion
- Exact ring size tuning (256 vs 512 vs 1024 entries)
- Whether to use `io_uring_prep_send_zc` (zero-copy send, Linux 6.1+) for large responses
- Buffer ring sizing per shard (balance memory vs connection count)
- Whether Glommio's internal io_uring management is sufficient or needs custom ring setup

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Architecture blueprint
- `.planning/architect-blue-print.md` §Networking layer: io_uring from socket to response — Full io_uring feature list, multishot accept/recv, registered buffers, scatter-gather response path
- `.planning/architect-blue-print.md` §The target architecture — Architecture diagram showing io_uring rings per shard

### Phase dependencies
- `.planning/phases/11-thread-per-core-shared-nothing-architecture/11-CONTEXT.md` — Shard architecture that io_uring integrates with
- `src/server/connection.rs` — Connection handler (819 lines) — response writing path is the primary optimization target
- `src/server/listener.rs` — TCP listener (175 lines) — replaced by multishot accept

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `itoa` crate — already used for fast integer formatting in responses
- Static response constants can be defined as `&'static [u8]` slices
- Pipeline batching logic from Phase 7 — natural SQE batching boundary

### Established Patterns
- `Framed<TcpStream, RespCodec>` — will be replaced by direct io_uring recv/send
- Response assembly via `Frame::serialize()` — can be optimized to scatter-gather

### Integration Points
- Glommio/Monoio runtime from Phase 11 provides io_uring ring management
- Shard event loop drives io_uring submission/completion
- DashTable value references (from Phase 10) can be passed directly to writev — zero-copy safe within single-threaded shard

</code_context>

<specifics>
## Specific Ideas

- Blueprint cites: io_uring achieves 5M+ IOPS with lower p99 latency under saturation, 30% less CPU utilization vs epoll
- Alibaba Cloud benchmarks: ~10% higher throughput at 1000 connections, gap widens with Spectre mitigations
- Each syscall costs ~1-5μs with Spectre mitigations — batching is critical

</specifics>

<deferred>
## Deferred Ideas

- io_uring for persistence writes (DMA file I/O) — Phase 14
- SQPOLL kernel thread mode — evaluate after baseline io_uring is working

</deferred>

---

*Phase: 12-io-uring-networking-layer*
*Context gathered: 2026-03-24*
