# Phase 28: Performance Tuning — Context

**Gathered:** 2026-03-25
**Status:** Ready for planning

<domain>
## Phase Boundary

Final performance tuning pass: response buffer pooling to reduce allocator pressure, io_uring registered buffers for zero-copy send path, and adaptive pipeline batching for multi-get workloads.

</domain>

<decisions>
## Implementation Decisions

### Claude's Discretion
All implementation choices are at Claude's discretion — pure infrastructure phase.

Key areas from deep dive analysis:

**1. Response Buffer Pooling:**
- Currently each response allocates new BytesMut
- Pool per-connection reusable response buffers
- Return to pool after write completes
- Expected gain: 5-8% throughput by eliminating allocator pressure

**2. io_uring Registered Buffers (send path):**
- Multishot recv with provided buffer ring already implemented (Phase 12)
- Send path still uses regular write — not registered buffers
- `IORING_REGISTER_BUFFERS` pre-maps buffers into kernel space
- `IORING_OP_WRITE_FIXED` eliminates per-I/O get_user_pages()
- Expected gain: 15-30% fewer CPU cycles per write

**3. Adaptive Pipeline Batching:**
- Current pipelines process commands sequentially
- For MGET-style workloads with independent keys on same shard, batch DashTable lookups
- Expected gain: 10-20% for pipelined MGET workloads

**Key files:**
- `src/io/uring_driver.rs` (574 lines) — io_uring driver
- `src/io/buf_ring.rs` — provided buffer ring (recv side)
- `src/shard/mod.rs` — shard event loop, pipeline processing
- `src/server/connection.rs` — per-connection handler

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- `src/io/buf_ring.rs` — Provided buffer ring pattern (recv side) to adapt for send
- `src/io/fd_table.rs` — Fixed FD table already registered
- `src/io/uring_driver.rs` — InFlightSend RAII tracking for send buffers
- `src/shard/mod.rs` — Pipeline batching via now_or_never() (Phase 7)

### Established Patterns
- WritevGuard RAII wrapper holds Bytes clone for io_uring lifetime
- InFlightSend enum with Buf/Writev variants
- SQE batching: accumulate per tick, single submit_and_wait
- Pipeline batch collects up to 1024 frames per cycle

### Integration Points
- `src/io/uring_driver.rs` — send_response(), queue_writev()
- `src/shard/mod.rs` — handle_uring_event() processes CQEs
- `src/server/connection.rs` — response serialization

</code_context>

<specifics>
## Specific Ideas

No specific requirements — performance optimization phase. Benchmark before/after each change.

</specifics>

<deferred>
## Deferred Ideas

- Connection-level memory budgets (backpressure when budget exceeded)
- NUMA-aware buffer allocation
- Adaptive io_uring ring size based on connection count

</deferred>
