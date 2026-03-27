# Phase 43: Lock-free oneshot channels and timestamp caching (P0 profiling fixes) - Context

**Gathered:** 2026-03-26
**Status:** Ready for planning

<domain>
## Phase Boundary

Fix the two highest-impact bottlenecks identified by CPU profiling of the 8-shard p=16 workload:
1. flume oneshot mutex contention (12% of CPU, 216 samples) — replace with lock-free implementation
2. clock_gettime overhead (4% of CPU, 66 samples) — cache timestamp per event loop tick

</domain>

<decisions>
## Implementation Decisions

### Claude's Discretion

**Fix 1 — Lock-free oneshot channel (12% CPU elimination):**

The current `runtime::channel::oneshot()` wraps `flume::bounded(1)` which uses `Arc<Shared<Mutex<VecDeque>>>`. Every cross-shard command creates one, and both send and recv contend on the mutex between two threads.

Replace with a custom lock-free oneshot using `AtomicPtr<T>` + `AtomicU8` state machine + `Waker`:
```rust
pub struct OneshotSender<T> { inner: Arc<Inner<T>> }
pub struct OneshotReceiver<T> { inner: Arc<Inner<T>> }

struct Inner<T> {
    state: AtomicU8,      // 0=empty, 1=value_ready, 2=closed
    value: UnsafeCell<MaybeUninit<T>>,
    waker: AtomicWaker,   // from futures-util or manual impl
}
```

Send: store value via UnsafeCell, set state to 1, wake waker. O(1), no mutex.
Recv: check state, if 0 register waker and return Pending, if 1 take value. O(1), no mutex.

Alternative: use `tokio::sync::oneshot` which already uses AtomicU8 state machine — but this adds a tokio dependency to the monoio path. Better to implement our own minimal version.

**Fix 2 — Cached timestamp per event loop tick (4% CPU elimination):**

`refresh_now()` calls `SystemTime::now()` → `clock_gettime_nsec_np()` on every batch. Redis calls `updateCachedTime()` once per event loop iteration.

Add a `Cell<u64>` to the shard's event loop that stores the current epoch seconds. Update it once per 1ms tick (the periodic timer arm). All `refresh_now()` calls read from the cached value instead of making a syscall.

Implementation:
- Add `cached_now: Cell<u64>` to shard run() local state
- Update in the periodic tick arm: `cached_now.set(SystemTime::now().duration_since(UNIX_EPOCH).as_secs())`
- Pass to `refresh_now(cached_now)` or `Database::set_cached_now()`
- Connection handler reads cached_now from a shared `Arc<AtomicU64>` updated by the shard tick

</decisions>

<code_context>
## Existing Code Insights

### Current oneshot implementation
- `src/runtime/channel.rs` — wraps flume::bounded(1)
- Used in: `src/server/connection.rs` (every cross-shard dispatch creates one)
- Used in: `src/shard/coordinator.rs` (multi-key commands)
- Used in: `src/shard/dispatch.rs` (ShardMessage::Execute, PipelineBatch, MultiExecute reply_tx)

### Current timestamp path
- `src/storage/db.rs` — `Database::refresh_now()` calls `SystemTime::now()`
- Called from: `src/server/connection.rs:3412` (once per batch, before frame loop)
- Called from: `src/shard/mod.rs` (in drain_spsc_shared for each Execute message)

### Profiling evidence (8-shard p=16)
- pthread_mutex: 216 samples (12%) — ALL from flume oneshot send/recv/drop
- clock_gettime: 66 samples (4%) — from refresh_now → SystemTime::now
- OneshotSender::send: 67 samples
- OneshotReceiver::recv: 57 samples
- Notify::notify_one: 35 samples (also uses flume internally)

</code_context>

<specifics>
## Specific Ideas

For the lock-free oneshot, the simplest correct implementation uses:
- `AtomicU8` for state (EMPTY=0, VALUE=1, CLOSED=2, RECV_WAITING=3)
- `UnsafeCell<Option<T>>` for the value
- `AtomicWaker` from `futures-util` crate (or manual impl with `AtomicPtr<Waker>`)

The critical invariant: only the sender writes the value, only the receiver reads it. The AtomicU8 state transition provides the happens-before guarantee. No mutex needed.

</specifics>

<deferred>
## Deferred Ideas

- Replace flume Notify with AtomicBool + Waker (P1, 2-3% impact)
- Extend inline dispatch to more commands (P1, separate phase)

</deferred>
