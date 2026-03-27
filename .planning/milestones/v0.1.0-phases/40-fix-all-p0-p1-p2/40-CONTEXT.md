# Phase 40: fix all P0, P1, P2 - Context

**Gathered:** 2026-03-26
**Status:** Ready for planning

<domain>
## Phase Boundary

Fix 3 critical multi-shard performance issues identified via shard-scaling benchmarks and deep code analysis. The Monoio connection handler is missing pipeline batch dispatch (present in Tokio handler), causing negative scaling and pipeline throughput gaps. High client count instability caused by per-read allocations and connection channel overflow.

</domain>

<decisions>
## Implementation Decisions

### Claude's Discretion
All implementation choices are at Claude's discretion — pure infrastructure/bugfix phase. The fixes are precisely identified from code analysis:

**P0 — Port pipeline batch dispatch to Monoio handler:**
- Add `remote_groups: HashMap<usize, Vec<...>>` to defer remote commands
- Send `ShardMessage::PipelineBatch` per shard (not `Execute` per command)
- Parallel await all shard responses
- Change `for frame in &frames` to `for frame in frames` (eliminate deep clone)

**P1 — Fix per-read allocation and connection capacity:**
- Reuse read buffer in Monoio handler (move `vec![0u8; 8192]` outside loop)
- Increase `CONN_CHANNEL_CAPACITY` from 256 to 4096

**P2 — Connection stability improvements:**
- Pool oneshot channels per-connection (reusable reply channel)
- Per-shard connection limit with graceful backpressure

</decisions>

<code_context>
## Existing Code Insights

### Critical Files
- `src/server/connection.rs:3104-3713` — Monoio sharded handler (BROKEN path)
- `src/server/connection.rs:1333-2320` — Tokio sharded handler (CORRECT reference)
- `src/shard/dispatch.rs:34-113` — ShardMessage enum (PipelineBatch variant exists)
- `src/shard/mesh.rs:13,16` — CHANNEL_BUFFER_SIZE=4096, CONN_CHANNEL_CAPACITY=256
- `src/shard/mod.rs:752-834` — drain_spsc_shared (already batches Execute/PipelineBatch)

### Root Causes Identified
1. **Monoio handler line 3639-3667**: Each remote command dispatched individually as `ShardMessage::Execute` with synchronous await — should use deferred `PipelineBatch` like Tokio handler
2. **Monoio handler line 3378**: `for frame in &frames` borrows, forcing `frame.clone()` — should consume iterator
3. **Monoio handler line 3350**: `vec![0u8; 8192]` allocated every loop iteration — should reuse
4. **mesh.rs line 16**: `CONN_CHANNEL_CAPACITY = 256` too small for 5K clients

### Tokio Handler Reference Pattern (lines 2240-2320)
The Tokio handler already implements the correct pattern:
- `remote_groups: HashMap<usize, Vec<(usize, Arc<Frame>, Option<Bytes>, Vec<u8>)>>`
- Deferred collection during frame loop
- Batch `PipelineBatch` per target shard after loop
- Parallel await all reply futures

</code_context>

<specifics>
## Specific Ideas

Port the exact Tokio handler pattern (lines 1436, 2240-2320) to the Monoio handler (lines 3574-3680). The PipelineBatch message type already exists and is already handled by the shard event loop's drain_spsc_shared.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>
