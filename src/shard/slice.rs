//! Per-shard owned aggregate — the `ShardSlice` type.
//!
//! # Architecture
//!
//! Replaces the cross-thread `Arc<RwLock<Database>>` pattern with thread-local
//! ownership: each shard thread holds its `ShardSlice` in a `thread_local!`.
//! The borrow checker enforces that no other thread can access it (`_not_send`
//! marker via `PhantomData<Rc<()>>`). This is the Rust expression of
//! Dragonfly's `__thread EngineShard*` — stronger, because it is a compile
//! error to violate, not a runtime assertion.
//!
//! # Migration status
//!
//! Phase 1 introduces this scaffolding **unused**. The type exists, compiles,
//! and has accessors. No call sites in `src/server/` or `src/command/` use it
//! yet. Phases 2a–2f migrate the 213 call sites one wave at a time. Phase 4
//! deletes the `RwLock`/`Mutex` wrappers in `ShardDatabases` and wires
//! `init_shard` at shard fiber startup.
//!
//! # Thread-local contract
//!
//! - `init_shard` MUST be called exactly once per shard thread, before any
//!   commands are handled. Calling it twice panics.
//! - `with_shard` panics if `init_shard` has not been called on the current
//!   thread. Use `try_with_shard` for code that runs on non-shard threads.
//! - The closure passed to `with_shard` or `with_shard_db` MUST NOT re-enter
//!   either function — doing so causes a `RefCell` double-borrow panic.

use std::cell::{Cell, RefCell};
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

#[cfg(feature = "graph")]
use crate::graph::store::GraphStore;
use crate::mq::{DurableQueueRegistry, TriggerRegistry};
use crate::runtime::channel::MpscSender;
use crate::shard::shared_databases::ShardStoreMemory;
use crate::storage::Database;
use crate::temporal::{TemporalKvIndex, TemporalRegistry};
use crate::text::store::TextStore;
use crate::transaction::{DeferredHnswInserts, KvWriteIntents};
use crate::vector::store::VectorStore;

/// Per-shard owned aggregate. `!Send` by construction.
///
/// Each field is directly owned — no `Arc`, no `Mutex`, no `RwLock`. The
/// `_not_send: PhantomData<Rc<()>>` field makes the compiler reject any attempt
/// to move or share `ShardSlice` across threads. This is the compile-time
/// equivalent of Dragonfly's `IsMyThread()` runtime assertion.
///
/// # Panics (via `with_shard`)
///
/// Accessing `ShardSlice` from a thread that never called `init_shard` panics
/// with "ShardSlice not initialized on this thread". Use `try_with_shard` for
/// code that runs on coordinator or background threads.
///
/// Re-entrant access (calling `with_shard` inside a `with_shard` closure)
/// causes a `RefCell` double-borrow panic with the message
/// "with_shard called recursively — closure may not call with_shard or with_shard_db".
pub struct ShardSlice {
    /// Shard index (0..num_shards).
    pub shard_id: usize,
    /// Per-shard databases (SELECT 0-15). Fixed-size box after init — no
    /// reallocation during the shard lifetime. Indexed directly by db_index.
    pub databases: Box<[Database]>,
    /// Per-shard vector store for FT.* commands.
    pub vector_store: VectorStore,
    /// Per-shard text store for full-text search indexes.
    pub text_store: TextStore,
    /// Per-shard graph store for GRAPH.* commands.
    #[cfg(feature = "graph")]
    pub graph_store: GraphStore,
    /// KV write-intent side-table for transactional MVCC visibility checks.
    pub kv_write_intents: KvWriteIntents,
    /// Deferred HNSW insert queue for post-commit processing.
    pub deferred_hnsw_inserts: DeferredHnswInserts,
    /// Temporal registry for wall-clock-to-LSN bindings. Lazy-init: `None`
    /// until the first `TEMPORAL.SNAPSHOT_AT` call on this shard.
    pub temporal_registry: Option<Box<TemporalRegistry>>,
    /// Temporal KV index for versioned KV reads. Lazy-init: `None` until the
    /// first `TemporalUpsert` WAL write on this shard.
    pub temporal_kv_index: Option<Box<TemporalKvIndex>>,
    /// Durable queue registry for MQ.* commands. Lazy-init: `None` until the
    /// first `MQ.CREATE` call on this shard.
    pub durable_queue_registry: Option<Box<DurableQueueRegistry>>,
    /// Trigger registry for MQ.TRIGGER debounced callbacks. Lazy-init: `None`
    /// until the first `MQ.TRIGGER` call on this shard.
    pub trigger_registry: Option<Box<TriggerRegistry>>,
    /// WAL append channel sender. `None` when persistence is disabled.
    pub wal_append_tx: Option<MpscSender<bytes::Bytes>>,
    /// Per-shard estimated memory counter. Published atomically so that
    /// cross-shard readers (maxmemory eviction, metrics) can sum without locks.
    ///
    /// The `Arc` is shared with `ShardDatabases::memory_per_shard[shard_id]`.
    /// Phase 3 will switch the eviction tick to read this atomic instead of
    /// calling `aggregate_memory()`, which acquires per-DB read locks.
    pub estimated_memory: Arc<AtomicUsize>,
    /// Per-shard published store-memory atomics (C5 / M4).
    ///
    /// Shared with `ShardDatabases::store_memory_per_shard[shard_id]`. The
    /// shard refreshes vector/text/graph bytes on its 100ms tick; cross-thread
    /// observers (metrics, MEMORY DOCTOR) read without locks.
    pub store_memory: Arc<ShardStoreMemory>,
    /// Makes `ShardSlice` unconditionally `!Send` and `!Sync`.
    ///
    /// `Rc<()>` is neither `Send` nor `Sync`, so any type containing
    /// `PhantomData<Rc<()>>` inherits those non-implementations.
    /// The field is private so that only `ShardSlice::new` can construct one,
    /// preventing accidental creation outside of `init_shard`.
    _not_send: PhantomData<Rc<()>>,
}

/// Builder-style initializer for `ShardSlice`.
///
/// Accepts all 13 public fields and constructs the private `_not_send` marker
/// internally. This makes `init_shard` call sites readable without exposing
/// the marker field.
pub struct ShardSliceInit {
    pub shard_id: usize,
    pub databases: Box<[Database]>,
    pub vector_store: VectorStore,
    pub text_store: TextStore,
    #[cfg(feature = "graph")]
    pub graph_store: GraphStore,
    pub kv_write_intents: KvWriteIntents,
    pub deferred_hnsw_inserts: DeferredHnswInserts,
    pub temporal_registry: Option<Box<TemporalRegistry>>,
    pub temporal_kv_index: Option<Box<TemporalKvIndex>>,
    pub durable_queue_registry: Option<Box<DurableQueueRegistry>>,
    pub trigger_registry: Option<Box<TriggerRegistry>>,
    pub wal_append_tx: Option<MpscSender<bytes::Bytes>>,
    /// A clone of `ShardDatabases::memory_per_shard[shard_id]`. The master
    /// `Arc<AtomicUsize>` lives in `ShardDatabases`; this is a second owner.
    pub estimated_memory: Arc<AtomicUsize>,
    /// A clone of `ShardDatabases::store_memory_per_shard[shard_id]`.
    pub store_memory: Arc<ShardStoreMemory>,
}

impl ShardSliceInit {
    /// Build one init package per shard from pre-restored databases plus
    /// clones of the shared masters' atomics (shardslice-migration C1).
    ///
    /// Called by `ShardDatabases::new` at boot; each package is handed to its
    /// shard thread's spawn closure BY MOVE and consumed by `init_shard`.
    /// Stores and registries start empty/lazy — recovery has already replayed
    /// into `shard_databases` before this point, and store recovery (vector/
    /// text/graph) happens on the shard thread after `init_shard`.
    pub fn build_all(
        shard_databases: Vec<Vec<Database>>,
        memory_per_shard: &[Arc<AtomicUsize>],
        store_memory_per_shard: &[Arc<ShardStoreMemory>],
    ) -> Vec<ShardSliceInit> {
        shard_databases
            .into_iter()
            .enumerate()
            .map(|(shard_id, dbs)| {
                let databases: Box<[Database]> = dbs.into_boxed_slice();
                ShardSliceInit {
                    shard_id,
                    databases,
                    vector_store: VectorStore::new(),
                    text_store: TextStore::new(),
                    #[cfg(feature = "graph")]
                    graph_store: GraphStore::new(),
                    kv_write_intents: KvWriteIntents::new(),
                    deferred_hnsw_inserts: DeferredHnswInserts::new(),
                    temporal_registry: None,
                    temporal_kv_index: None,
                    durable_queue_registry: None,
                    trigger_registry: None,
                    wal_append_tx: None,
                    estimated_memory: memory_per_shard[shard_id].clone(),
                    store_memory: store_memory_per_shard[shard_id].clone(),
                }
            })
            .collect()
    }
}

impl ShardSlice {
    /// Construct a `ShardSlice` from its initializer.
    ///
    /// The private `_not_send` marker is added here so callers never see it.
    /// Call this only from `init_shard`.
    pub fn new(init: ShardSliceInit) -> Self {
        Self {
            shard_id: init.shard_id,
            databases: init.databases,
            vector_store: init.vector_store,
            text_store: init.text_store,
            #[cfg(feature = "graph")]
            graph_store: init.graph_store,
            kv_write_intents: init.kv_write_intents,
            deferred_hnsw_inserts: init.deferred_hnsw_inserts,
            temporal_registry: init.temporal_registry,
            temporal_kv_index: init.temporal_kv_index,
            durable_queue_registry: init.durable_queue_registry,
            trigger_registry: init.trigger_registry,
            wal_append_tx: init.wal_append_tx,
            estimated_memory: init.estimated_memory,
            store_memory: init.store_memory,
            _not_send: PhantomData,
        }
    }
}

// ── Thread-local storage ──────────────────────────────────────────────────────

thread_local! {
    static SHARD: RefCell<Option<ShardSlice>> = const { RefCell::new(None) };
}

// ── Cross-shard reply-wait idle gate (xshard-read-fastpath C1) ─────────────────
//
// The shard thread is single-threaded, so this counter needs NO atomic and NO
// lock — a plain `Cell<u32>`. It tracks how many connections on THIS shard thread
// are currently blocked in a cross-shard reply-wait. The reply-side adaptive poll
// (C2) consults `xshard_may_spin()` to decide whether to busy-poll the reply
// (skipping one cross-thread wake) or park immediately: it spins ONLY when the
// shard is near-idle, so polling never steals a co-located connection's turn (the
// spike measured a −48% c100 collapse from an unconditional spin).

thread_local! {
    /// Connections on this shard thread currently blocked in a cross-shard
    /// reply-wait. Incremented on entry, decremented on exit (see `XshardWaitGuard`).
    static XSHARD_INFLIGHT: Cell<u32> = const { Cell::new(0) };
}

/// Upper bound (INCLUSIVE) on concurrent cross-shard reply-waiters for which the
/// reply-side spin is still allowed. At or below the gate the requesting
/// connection is effectively alone, so polling its reply steals no co-located
/// turn; above it, the path parks immediately. Tuned on bare metal; NOT a CLI flag.
pub const XSHARD_SPIN_GATE: u32 = 2;

/// Bounded number of poll iterations before the reply-side path falls back to
/// parking. Caps the busy-loop so a slow owner shard cannot starve this thread.
pub const XSHARD_SPIN_BUDGET: u32 = 4_096;

/// Max cross-shard commands in the CURRENT connection batch for which the reply-side
/// spin is allowed — the call site gates on `batch_remote <= this && xshard_may_spin()`.
///
/// The spin is SYNCHRONOUS (it blocks the shard thread, which never yields mid-spin), so
/// it must engage ONLY for a singleton foreign read (the c1 win). When a batch carries
/// multiple cross-shard reads — a pipeline (P>1) or a multi-key fan-out — spinning
/// SERIALIZES them behind per-read thread-blocks and starves pipelined throughput
/// (measured −27% on s4-P16 best-of-7). The `XSHARD_INFLIGHT` gate alone cannot catch
/// this: a synchronous spin never yields, so co-located waiters never register as
/// in-flight for each other and the gate stays falsely open. This is the §3 contract
/// flag-#1 reserved mitigation ("also gate on … other ready work"), realised as the
/// cheap, shard-local batch-depth signal instead of an SPSC-inbox probe.
pub const XSHARD_SPIN_MAX_BATCH_REMOTE: usize = 1;

/// May the calling connection POLL its cross-shard reply instead of parking?
///
/// True when this shard thread has `<= XSHARD_SPIN_GATE` in-flight cross-shard
/// reply-waiters — i.e. it is near-idle and the spin steals no co-located work.
/// Reads a thread-local `Cell<u32>`: no atomic, no lock, no syscall.
#[inline]
pub fn xshard_may_spin() -> bool {
    XSHARD_INFLIGHT.with(|c| c.get() <= XSHARD_SPIN_GATE)
}

/// The full reply-side spin decision for the current batch: spin only when the batch
/// holds at most `XSHARD_SPIN_MAX_BATCH_REMOTE` cross-shard commands (singleton — not a
/// pipeline) AND the shard is near-idle (`xshard_may_spin`). Both reply-wait sites
/// (monoio + tokio) call THIS, so the two regimes the spin must avoid — pipelined
/// fan-out (batch gate) and concurrent waiters (inflight gate) — are enforced in one
/// place. `batch_remote` is the count of cross-shard commands in the connection's
/// current batch (cheap shard-local state; no atomic, no lock).
#[inline]
pub fn xshard_should_spin(batch_remote: usize) -> bool {
    batch_remote <= XSHARD_SPIN_MAX_BATCH_REMOTE && xshard_may_spin()
}

/// Test-only: current in-flight cross-shard reply-waiter count on this thread.
#[cfg(test)]
pub fn xshard_inflight_count() -> u32 {
    XSHARD_INFLIGHT.with(|c| c.get())
}

/// RAII guard that marks the current connection as an in-flight cross-shard
/// reply-waiter for its lifetime: increments `XSHARD_INFLIGHT` on construction,
/// decrements on drop. Wrap every cross-shard reply-wait in one so the idle gate
/// reflects reality even across early returns / cancellation. `!Send` in spirit —
/// the thread-local it guards is per-thread and the guard must not cross threads.
#[must_use = "the guard must be held for the duration of the reply-wait; dropping it early ends the in-flight window"]
pub struct XshardWaitGuard {
    // Private field: construct only via `new()`, and a `PhantomData<Rc<()>>`-free
    // marker is unnecessary because the type holds no data that could be moved
    // across threads; callers keep it on the shard thread's stack.
    _priv: (),
}

impl XshardWaitGuard {
    /// Enter a cross-shard reply-wait: increment this thread's in-flight count.
    #[inline]
    pub fn new() -> Self {
        XSHARD_INFLIGHT.with(|c| c.set(c.get().saturating_add(1)));
        Self { _priv: () }
    }
}

impl Default for XshardWaitGuard {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for XshardWaitGuard {
    #[inline]
    fn drop(&mut self) {
        XSHARD_INFLIGHT.with(|c| c.set(c.get().saturating_sub(1)));
    }
}

// ── Lifecycle API ─────────────────────────────────────────────────────────────

/// Initialize the `ShardSlice` for the current shard thread.
///
/// MUST be called exactly once per shard thread before any command handling
/// begins. Typically called at the start of the shard event-loop fiber
/// (Phase 4 wires this into `event_loop.rs`).
///
/// # Panics
///
/// Panics if called a second time on the same thread — double-initialization
/// indicates a programming error in shard startup.
pub fn init_shard(slice: ShardSlice) {
    let shard_id = slice.shard_id;
    SHARD.with(|cell| {
        let mut guard = cell.borrow_mut();
        if guard.is_some() {
            panic!(
                "init_shard called twice on the same thread (shard {}). \
                 Each shard thread must call init_shard exactly once.",
                slice.shard_id
            );
        }
        *guard = Some(slice);
    });
    // C1 (shardslice-migration): one line per shard, before its first accept.
    // test_ssm1_slice_live_at_startup pins this exact marker + shard id.
    // The shard id is embedded directly in the message (not as a structured
    // tracing field) so that the test can match "shard=N" as a contiguous
    // substring — ANSI colour codes inserted around structured-field `=`
    // separators break plain string matching of `"shard=0"`.
    tracing::info!("ShardSlice initialized shard={}", shard_id);
}

/// Test-only: forcibly replace the thread-local ShardSlice with a fresh one.
///
/// Production code MUST use `init_shard` (which panics on double-init).
/// Tests often run multiple test functions on the same OS thread; this helper
/// lets each test reset to a clean state without triggering the double-init
/// guard.  Only compiled in `#[cfg(test)]` — never available in production.
#[cfg(test)]
pub fn reset_test_shard(slice: ShardSlice) {
    SHARD.with(|cell| {
        *cell.borrow_mut() = Some(slice);
    });
}

// ── Accessor API ──────────────────────────────────────────────────────────────

/// Execute a closure with exclusive access to the current thread's `ShardSlice`.
///
/// The closure receives a `&mut ShardSlice` and may read or write any field.
/// Returns the closure's return value.
///
/// # Panics
///
/// - If `init_shard` has not been called on this thread: "ShardSlice not
///   initialized on this thread — call init_shard before dispatching commands."
/// - If called recursively inside a closure: "with_shard called recursively —
///   closure may not call with_shard or with_shard_db".
///
/// Use `try_with_shard` if the caller may run on a non-shard thread.
#[inline]
pub fn with_shard<R>(f: impl FnOnce(&mut ShardSlice) -> R) -> R {
    SHARD.with(|cell| {
        #[allow(clippy::unwrap_used)] // the error message is the invariant explanation
        let mut guard = cell
            .try_borrow_mut()
            .expect("with_shard called recursively — closure may not call with_shard or with_shard_db");
        #[allow(clippy::unwrap_used)] // caller contract: init_shard must have been called first
        let slice = guard
            .as_mut()
            .expect("ShardSlice not initialized on this thread — call init_shard before dispatching commands");
        f(slice)
    })
}

/// Execute a closure with exclusive access to a specific database in the
/// current thread's `ShardSlice`.
///
/// Equivalent to `with_shard(|s| f(&mut s.databases[db_index]))` but panics
/// with a clearer message on out-of-bounds access.
///
/// # Panics
///
/// - All panics from `with_shard` (uninitialized, reentrant).
/// - If `db_index >= databases.len()`: "db_index N out of bounds (M databases)".
///
/// Use `try_with_shard` to guard against the uninitialized case first.
#[inline]
pub fn with_shard_db<R>(db_index: usize, f: impl FnOnce(&mut Database) -> R) -> R {
    with_shard(|slice| {
        let len = slice.databases.len();
        #[allow(clippy::unwrap_used)] // the expect message identifies the programming error
        let db = slice.databases.get_mut(db_index).unwrap_or_else(|| {
            panic!(
                "db_index {db_index} out of bounds ({len} databases on shard {})",
                slice.shard_id
            )
        });
        f(db)
    })
}

/// Execute a closure with exclusive access to the current thread's `ShardSlice`,
/// returning `None` if `init_shard` has not been called on this thread.
///
/// Does NOT panic on uninitialized thread — returns `None` instead. Panics
/// only on reentrant access (RefCell double-borrow).
///
/// Useful for code that may run on coordinator threads, background timers, or
/// test harnesses that do not go through shard startup.
#[inline]
pub fn try_with_shard<R>(f: impl FnOnce(&mut ShardSlice) -> R) -> Option<R> {
    SHARD.with(|cell| {
        #[allow(clippy::unwrap_used)] // reentrant access is always a programming error
        let mut guard = cell.try_borrow_mut().expect(
            "try_with_shard called recursively — closure may not call with_shard or with_shard_db",
        );
        guard.as_mut().map(f)
    })
}

/// Returns `true` if `init_shard` has been called on the current thread.
///
/// Useful for assertions at Phase 4 startup boundaries and for conditional
/// logic in code paths that run on both shard and non-shard threads.
#[inline]
pub fn is_initialized() -> bool {
    SHARD.with(|cell| {
        #[allow(clippy::unwrap_used)] // not inside any closure — cannot be reentrant here
        cell.try_borrow().map(|g| g.is_some()).unwrap_or(false)
    })
}

/// Assert that `init_shard` has been called on the current thread; abort the
/// process with the shard id if it has not.
///
/// Call this ONCE at the entry of the shard accept/drain loop, after
/// `init_shard(init)` is called and before the first connection is accepted.
/// This is the C1 startup-abort guard: an uninitialized shard fails fast at
/// startup (not per-command), so no command is ever answered on an
/// uninitialized thread.
///
/// Using this helper keeps `is_initialized()` calls out of production
/// dispatch paths (the shape test forbids `is_initialized()` outside
/// slice.rs).
#[inline]
pub fn assert_initialized(shard_id: usize) {
    if !is_initialized() {
        // Startup abort: the shard event loop entered without init_shard.
        // Panic here (before the first accept) so the process terminates
        // before any command can be answered on an uninitialized thread.
        panic!(
            "ShardSlice not initialized on shard thread {} — \
             init_shard must be called before the accept/drain loop. \
             This is a startup-configuration bug, not a runtime error.",
            shard_id
        );
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::storage::Database;
    use crate::text::store::TextStore;
    use crate::transaction::{DeferredHnswInserts, KvWriteIntents};
    use crate::vector::store::VectorStore;

    fn make_init(shard_id: usize, db_count: usize) -> ShardSliceInit {
        let databases: Box<[Database]> = (0..db_count).map(|_| Database::new()).collect();
        ShardSliceInit {
            shard_id,
            databases,
            vector_store: VectorStore::new(),
            text_store: TextStore::new(),
            #[cfg(feature = "graph")]
            graph_store: crate::graph::store::GraphStore::new(),
            kv_write_intents: KvWriteIntents::new(),
            deferred_hnsw_inserts: DeferredHnswInserts::new(),
            temporal_registry: None,
            temporal_kv_index: None,
            durable_queue_registry: None,
            trigger_registry: None,
            wal_append_tx: None,
            estimated_memory: Arc::new(AtomicUsize::new(0)),
            store_memory: Arc::new(crate::shard::shared_databases::ShardStoreMemory {
                vector: AtomicUsize::new(0),
                text: AtomicUsize::new(0),
                graph: AtomicUsize::new(0),
            }),
        }
    }

    /// Run a closure in a fresh OS thread whose thread-local `SHARD` is guaranteed
    /// to be uninitialized. Returns the join result.
    fn on_fresh_thread<F, R>(f: F) -> std::thread::Result<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        std::thread::spawn(f).join()
    }

    /// Assert that a closure panics and that the panic message contains `substr`.
    ///
    /// `std::panic::catch_unwind` captures the panic payload on the CURRENT thread,
    /// so message matching works correctly. We use this instead of spawning +
    /// `#[should_panic]` to avoid the JoinError wrapper that swallows the message.
    fn assert_panics_with(substr: &str, f: impl FnOnce() + std::panic::UnwindSafe) {
        let result = std::panic::catch_unwind(f);
        let err = result.expect_err("expected a panic but closure returned normally");
        let msg = err
            .downcast_ref::<String>()
            .map(|s| s.as_str())
            .or_else(|| err.downcast_ref::<&str>().copied())
            .unwrap_or("<non-string panic payload>");
        assert!(
            msg.contains(substr),
            "panic message did not contain {:?}; got: {:?}",
            substr,
            msg
        );
    }

    // ── Uninitialized-thread tests (safe to run on test thread directly) ──────

    #[test]
    fn test_is_not_initialized_on_fresh_thread() {
        let result = on_fresh_thread(is_initialized).unwrap();
        assert!(!result, "fresh thread must not be initialized");
    }

    #[test]
    fn test_try_with_shard_returns_none_when_uninit() {
        let result = on_fresh_thread(|| try_with_shard(|_| 42u32)).unwrap();
        assert!(result.is_none());
    }

    // `with_shard` panics on an uninitialized thread. We can test this directly
    // on the test thread because it panics before touching any initialized state.
    #[test]
    fn test_with_shard_panics_when_uninit() {
        // Run on a fresh thread so we don't poison the test thread's SHARD if
        // another test happened to initialize it. The panic message is captured
        // inside the spawned thread via catch_unwind before joining.
        let msg = on_fresh_thread(|| {
            std::panic::catch_unwind(|| with_shard(|_| {}))
                .unwrap_err()
                .downcast::<String>()
                .map(|s| *s)
                .unwrap_or_else(|_| String::from("<non-string>"))
        })
        .unwrap();
        assert!(
            msg.contains("not initialized"),
            "expected 'not initialized', got: {msg:?}"
        );
    }

    // ── Tests that need init_shard (always in fresh threads) ─────────────────

    #[test]
    fn test_init_and_with_shard_basic() {
        on_fresh_thread(|| {
            init_shard(ShardSlice::new(make_init(7, 4)));
            assert!(is_initialized());
            assert_eq!(with_shard(|s| s.shard_id), 7);
            assert_eq!(with_shard(|s| s.databases.len()), 4);
        })
        .unwrap();
    }

    #[test]
    fn test_with_shard_db_accesses_correct_db() {
        on_fresh_thread(|| {
            init_shard(ShardSlice::new(make_init(0, 3)));
            // Mutably accessing db index 2 of 3 must not panic.
            with_shard_db(2, |_db| {});
        })
        .unwrap();
    }

    #[test]
    fn test_with_shard_db_oob_panics() {
        let msg = on_fresh_thread(|| {
            init_shard(ShardSlice::new(make_init(0, 2)));
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                with_shard_db(99, |_db| {});
            }))
            .unwrap_err()
            .downcast::<String>()
            .map(|s| *s)
            .unwrap_or_else(|_| String::from("<non-string>"))
        })
        .unwrap();
        assert!(
            msg.contains("out of bounds"),
            "expected 'out of bounds', got: {msg:?}"
        );
    }

    #[test]
    fn test_with_shard_reentrant_panics() {
        let msg = on_fresh_thread(|| {
            init_shard(ShardSlice::new(make_init(0, 1)));
            // `with_shard` closure is `FnOnce`, not `UnwindSafe`, so we must wrap
            // the outer call in AssertUnwindSafe to allow catch_unwind.
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                with_shard(|_| {
                    // Re-entrant: SHARD is already mutably borrowed — RefCell panics.
                    with_shard(|_| {});
                });
            }))
            .unwrap_err()
            .downcast::<String>()
            .map(|s| *s)
            .unwrap_or_else(|_| String::from("<non-string>"))
        })
        .unwrap();
        assert!(
            msg.contains("recursively"),
            "expected 'recursively', got: {msg:?}"
        );
    }

    #[test]
    fn test_init_shard_twice_panics() {
        let msg = on_fresh_thread(|| {
            init_shard(ShardSlice::new(make_init(0, 1)));
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                init_shard(ShardSlice::new(make_init(0, 1)));
            }))
            .unwrap_err()
            .downcast::<String>()
            .map(|s| *s)
            .unwrap_or_else(|_| String::from("<non-string>"))
        })
        .unwrap();
        assert!(msg.contains("twice"), "expected 'twice', got: {msg:?}");
    }

    // ── assert_panics_with helper smoke-test ─────────────────────────────────

    #[test]
    fn test_assert_panics_with_helper() {
        assert_panics_with("sentinel", || panic!("sentinel message"));
    }

    // ── Arc sharing across threads ────────────────────────────────────────────

    #[test]
    fn test_estimated_memory_arc_shared() {
        let arc = Arc::new(AtomicUsize::new(0));
        let arc_clone = arc.clone();
        on_fresh_thread(move || {
            let mut init = make_init(0, 1);
            init.estimated_memory = arc_clone;
            init_shard(ShardSlice::new(init));
            with_shard(|s| s.estimated_memory.store(12345, Ordering::Relaxed));
        })
        .unwrap();
        // The outer arc should see the value written by the shard thread.
        assert_eq!(arc.load(Ordering::Relaxed), 12345);
    }
}
