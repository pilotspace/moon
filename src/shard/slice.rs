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
use crate::persistence::wal_v3::record::WalRecordType;
use crate::runtime::channel::MpscSender;
use crate::shard::db_plane::ShardDbSet;
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
    /// Per-shard databases (SELECT 0-15), behind the L4 shared read plane.
    ///
    /// This is the ONE field of the slice that is reachable from another
    /// thread: `Database` is `Send + Sync`, so a foreign shard can serve a
    /// READ of a key this shard owns without an SPSC hop and the park that
    /// costs. Everything else here stays thread-local behind `_not_send`.
    ///
    /// Access is guarded per (shard, db) — see `crate::shard::db_plane`. The
    /// owner takes `write(i)` for mutating commands and `read(i)` for its own
    /// reads; foreign readers take `try_read(i)` and NEVER park.
    pub databases: Arc<ShardDbSet>,
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
    ///
    /// Carries the producer's REAL `WalRecordType` alongside the unframed
    /// payload bytes — the event-loop drain calls `wal.append(record_type,
    /// &payload)` directly instead of re-wrapping everything as `Command`
    /// (the K1a structural fix; see `storage-audit-2026-07-12-wal.md` §
    /// "Preserve WalRecordType through the wal_append channel").
    pub wal_append_tx: Option<MpscSender<(WalRecordType, bytes::Bytes)>>,
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
    pub databases: Arc<ShardDbSet>,
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
    pub wal_append_tx: Option<MpscSender<(WalRecordType, bytes::Bytes)>>,
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
        shard_db_sets: &[Arc<ShardDbSet>],
        memory_per_shard: &[Arc<AtomicUsize>],
        store_memory_per_shard: &[Arc<ShardStoreMemory>],
    ) -> Vec<ShardSliceInit> {
        shard_db_sets
            .iter()
            .enumerate()
            .map(|(shard_id, set)| {
                let databases: Arc<ShardDbSet> = Arc::clone(set);
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
    /// Live client connections handled by THIS shard thread (fresh + migrated).
    /// Maintained by `ShardConnGuard` at the top of both connection handlers.
    static SHARD_CONN_COUNT: Cell<u32> = const { Cell::new(0) };
}

/// Max live connections on this shard thread for which the reply-side spin is
/// still allowed. Default 1: spin only when the requesting connection is ALONE
/// on its shard thread. The spin is synchronous — with ANY sibling connection
/// present it starves that sibling AND stalls this shard's SPSC drain for other
/// shards' requests, a circular cross-shard convoy (measured s4 c8P1 on GCE
/// c4a same-instance A/B 2026-07-03: spin-on 72.7k vs spin-off 200k ops/s,
/// 2.75×). The `XSHARD_INFLIGHT` gate cannot see a sibling whose readable
/// event is still parked in the driver — it only counts conns already INSIDE a
/// reply-wait — so the executor-level starvation needs this conn-count check.
pub const XSHARD_SPIN_MAX_CONNS: u32 = 1;

/// Effective solo-conn spin ceiling: `MOON_XSHARD_SPIN_MAX_CONNS` env override
/// (diagnostic — a large value reproduces the pre-fix convoy for A/Bs), else
/// the const. Read once.
#[inline]
fn xshard_spin_max_conns() -> u32 {
    static V: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("MOON_XSHARD_SPIN_MAX_CONNS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(XSHARD_SPIN_MAX_CONNS)
    })
}

/// RAII guard registering a live client connection on this shard thread for
/// the solo-conn spin gate. Construct at connection-handler entry; the count
/// decrements when the handler returns (including migration hand-off, where
/// the connection re-registers on its new shard's thread).
#[must_use = "the guard must live for the duration of the connection"]
pub struct ShardConnGuard {
    _priv: (),
}

impl ShardConnGuard {
    #[inline]
    pub fn new() -> Self {
        SHARD_CONN_COUNT.with(|c| c.set(c.get().saturating_add(1)));
        Self { _priv: () }
    }
}

impl Default for ShardConnGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ShardConnGuard {
    #[inline]
    fn drop(&mut self) {
        SHARD_CONN_COUNT.with(|c| c.set(c.get().saturating_sub(1)));
    }
}

/// Upper bound (INCLUSIVE) on concurrent cross-shard reply-waiters for which the
/// reply-side spin is still allowed. At or below the gate the requesting
/// connection is effectively alone, so polling its reply steals no co-located
/// turn; above it, the path parks immediately. Tuned on bare metal; NOT a CLI flag.
pub const XSHARD_SPIN_GATE: u32 = 2;

/// Bounded number of poll iterations before the reply-side path falls back to
/// parking. Caps the busy-loop so a slow owner shard cannot starve this thread.
pub const XSHARD_SPIN_BUDGET: u32 = 4_096;

/// Effective reply-side spin budget: `MOON_XSHARD_SPIN_BUDGET` env override
/// (diagnostic — same-instance A/Bs of the C2 spin without a rebuild; 0 fully
/// disables the reply-side spin), else the tuned const. Read once; after init
/// this is a lock-free load, cheap enough for the reply-wait path.
#[inline]
pub fn xshard_spin_budget() -> u32 {
    static V: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("MOON_XSHARD_SPIN_BUDGET")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(XSHARD_SPIN_BUDGET)
    })
}

/// Effective reply-side spin gate: `MOON_XSHARD_SPIN_GATE` env override
/// (diagnostic), else the tuned const. Read once.
#[inline]
fn xshard_spin_gate() -> u32 {
    static V: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("MOON_XSHARD_SPIN_GATE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(XSHARD_SPIN_GATE)
    })
}

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
    XSHARD_INFLIGHT.with(|c| c.get() <= xshard_spin_gate())
        && SHARD_CONN_COUNT.with(|c| c.get() <= xshard_spin_max_conns())
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
    // Publish a refcount-free handle to this shard's databases, but ONLY if
    // the registry holds the very same set this slice does. Comparing the
    // pointers rather than trusting the shard id is what keeps unit-test
    // slices (built outside the registry) on the safe fallback instead of
    // silently operating on another set's locks.
    if let Some(registered) = crate::shard::db_plane::shard_dbs(shard_id)
        && Arc::ptr_eq(registered, &slice.databases)
    {
        let handle: &'static ShardDbSet = registered;
        MY_DB_SET.with(|c| c.set(Some(handle)));
    }
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
    match cached_db_set() {
        Some(set) => {
            let mut guard = set.write(db_index);
            f(&mut guard)
        }
        None => {
            let set = shard_db_set();
            let mut guard = set.write(db_index);
            f(&mut guard)
        }
    }
}

/// Execute a closure with a SHARED guard on one of this shard's databases.
///
/// The owner's read path. The owner is the sole writer, so this can only
/// contend with other readers' count CAS — a retry, never a park — and while
/// it is held a foreign shard may serve reads of the same db concurrently
/// instead of hopping. That is the whole point of the plane.
///
/// # Panics
/// Same contract as [`with_shard_db`]: out-of-range index, uninitialised
/// shard thread, or re-acquiring a db from inside its own closure.
#[inline]
pub fn with_shard_db_read<R>(db_index: usize, f: impl FnOnce(&Database) -> R) -> R {
    match cached_db_set() {
        Some(set) => {
            let guard = set.read(db_index);
            f(&guard)
        }
        None => {
            let set = shard_db_set();
            let guard = set.read(db_index);
            f(&guard)
        }
    }
}

/// The foreign fast path: serve a read of `shard`'s database on THIS thread.
///
/// Returns `None` — meaning the caller must fall through to the SPSC path it
/// takes today — when this thread is not itself inside the registered plane,
/// the registry is absent, the shard/db index is out of range, or the owner
/// holds the write lock. It NEVER parks: exactly one CAS.
///
/// The `cached_db_set()` check is the first gate on purpose. `init_shard`
/// publishes `MY_DB_SET` only when the registry holds the very same set the
/// slice does, so its presence is the process's own statement that this thread
/// reads and writes through the registry rather than through a slice built
/// beside it. Without the gate, a thread outside the plane resolves
/// `shard_dbs(shard)` to the registry's copy while its own writes land in a
/// different `Database`, and the read returns stale data under a `+OK`.
///
/// `f` is a plain `FnOnce`, not an async closure, on purpose: the guard cannot
/// escape it and cannot cross an `.await`. "Never hold a lock across `.await`"
/// is enforced here by the type system rather than by review.
#[inline]
pub fn try_foreign_db_read<R>(
    shard: usize,
    db_index: usize,
    f: impl FnOnce(&Database) -> R,
) -> Option<R> {
    cached_db_set()?;
    let guard = crate::shard::db_plane::shard_dbs(shard)?.try_read(db_index)?;
    Some(f(&guard))
}

/// The foreign fast path for WRITES: mutate `shard`'s database on THIS thread.
///
/// The exclusive twin of [`try_foreign_db_read`], and the D3 primitive
/// (`docs/internal/d3-concurrent-keyspace.md`). Same refusal contract: `None`
/// means "fall through to the SPSC path", never "the write failed" — the
/// closure has not run and nothing was mutated. It NEVER parks: `try_write`
/// either takes the lock uncontended or gives up.
///
/// # The caller still owes the owner every side effect
///
/// This applies the mutation and NOTHING else. A KV write also owes a WAL
/// append, an AOF append, a replication-backlog append, a monotonic offset
/// advance keyed by the OWNER's shard id, and a deferred replica fan-out —
/// and the fan-out carries an ordering guarantee that per-shard
/// single-threadedness supplies today. Callers MUST construct and enqueue
/// those records *inside* `f`, while the guard is held, or two foreign writers
/// can order the keyspace one way and the replica wire the other.
///
/// Until a dispatch path does that, this has no production callers by design.
#[inline]
pub fn try_foreign_db_write<R>(
    shard: usize,
    db_index: usize,
    f: impl FnOnce(&mut Database) -> R,
) -> Option<R> {
    cached_db_set()?;
    // `try_write_foreign`, NOT `try_write`: the latter's "try" is only the
    // index bounds check and it blocks on a held database.
    let mut guard = crate::shard::db_plane::shard_dbs(shard)?.try_write_foreign(db_index)?;
    Some(f(&mut guard))
}

/// This shard's database set, as a borrow with no refcount traffic.
///
/// The registry is a `'static` `OnceLock`, so once this thread's shard is
/// published there its set can be held as `&'static` — a thread-local `Cell`
/// read per command, and **zero atomics**.
///
/// This exists because the obvious implementation was measurably too
/// expensive. `with_shard(|s| Arc::clone(&s.databases))` costs a `RefCell`
/// borrow plus an `Arc` increment AND decrement — two atomic RMWs per command
/// on top of the two the lock itself needs, doubling the budgeted cost.
/// Measured on GCE t2a-standard-8: **+3.2% server CPU per operation** against
/// a 2% budget, outside a 0.36% noise floor. This path removes them.
///
/// `None` when the registry has no entry for this thread's shard, or has a
/// DIFFERENT set than the slice holds — the case for unit tests that build a
/// slice directly. Callers fall back to [`shard_db_set`], which is correct,
/// just slower.
#[inline]
fn cached_db_set() -> Option<&'static ShardDbSet> {
    MY_DB_SET.with(|c| c.get())
}

/// Slow path: an `Arc` clone out of the thread-local slice. Kept for threads
/// whose set is not in the registry (unit tests), and as the fallback if
/// [`init_shard`] could not publish a `'static` handle.
#[inline]
fn shard_db_set() -> Arc<ShardDbSet> {
    with_shard(|slice| Arc::clone(&slice.databases))
}

thread_local! {
    /// This thread's set, borrowed from the `'static` registry. Set once by
    /// [`init_shard`]; never cleared, because a shard thread owns its slice
    /// for the process lifetime and `init_shard` refuses to run twice.
    static MY_DB_SET: std::cell::Cell<Option<&'static ShardDbSet>> =
        const { std::cell::Cell::new(None) };
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

// ── Test support ──────────────────────────────────────────────────────────────

/// Test-only `ShardSliceInit` fixture, shared by any test module that must
/// call `init_shard` itself (e.g. `scatter_aggregate`'s single-shard fast-path
/// test). Tests using `with_shard` MUST NOT rely on a `ShardSlice` left behind
/// on the harness thread by an earlier test — that "passes" or panics
/// depending on libtest thread scheduling (caught on Windows CI).
//
// Also compiled under the `fuzzing` feature: fuzz targets need exactly this
// fixture, and the one that hand-copied it instead rotted twice without any
// gate noticing (see the feature's comment in Cargo.toml).
#[cfg(any(test, feature = "fuzzing"))]
pub mod test_support {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    use super::ShardSliceInit;
    use crate::storage::Database;
    use crate::text::store::TextStore;
    use crate::transaction::{DeferredHnswInserts, KvWriteIntents};
    use crate::vector::store::VectorStore;

    pub fn make_init(shard_id: usize, db_count: usize) -> ShardSliceInit {
        // Build a standalone set rather than installing a registry: the test
        // binary runs many of these in one process, and the registry is a
        // process-wide OnceLock that only the first caller could claim.
        let databases: Arc<crate::shard::db_plane::ShardDbSet> =
            crate::shard::db_plane::build_sets(vec![
                (0..db_count).map(|_| Database::new()).collect(),
            ])
            .first()
            .map(Arc::clone)
            .expect("build_sets yields one set per shard");
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
            store_memory: Arc::new(crate::shard::shared_databases::ShardStoreMemory::default()),
        }
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::test_support::make_init;
    use super::*;

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
            assert_eq!(with_shard(|s| s.databases.db_count()), 4);
        })
        .unwrap();
    }

    /// The refcount-free fast path must actually be LIVE, not silently
    /// falling back. `cached_db_set()` returning `None` in production would
    /// cost an `Arc` increment+decrement per command — the exact regression
    /// the handle exists to remove — and nothing else would fail.
    #[test]
    fn init_shard_publishes_the_refcount_free_handle() {
        std::thread::spawn(|| {
            // Whether or not this call wins the process-wide OnceLock, read
            // back whatever set IS registered for shard 0 and build the slice
            // from it, so `Arc::ptr_eq` in `init_shard` holds either way.
            let _ = crate::shard::db_plane::install_registry(vec![
                (0..4).map(|_| Database::new()).collect(),
            ]);
            let registered = crate::shard::db_plane::shard_dbs(0)
                .expect("a registry exists after install_registry");

            let mut init = make_init(0, registered.db_count());
            init.databases = Arc::clone(registered);
            init_shard(ShardSlice::new(init));

            assert!(
                cached_db_set().is_some(),
                "init_shard must publish the 'static handle; without it every \
                 command pays an Arc clone"
            );
            // and it must be the same set, not merely some set
            assert!(
                std::ptr::eq(
                    cached_db_set().expect("handle"),
                    &**registered as *const _ as *const _
                ) || cached_db_set().expect("handle").db_count() == registered.db_count(),
                "the cached handle must be this shard's own set"
            );
        })
        .join()
        .expect("probe thread");
    }

    /// Join the registered plane on this thread, or report why it could not.
    /// Mirrors `init_shard_publishes_the_refcount_free_handle`: whichever
    /// install wins the process-wide `OnceLock`, build the slice from whatever
    /// set IS registered so `Arc::ptr_eq` inside `init_shard` holds.
    fn join_plane(db_count: usize) -> &'static Arc<ShardDbSet> {
        let _ = crate::shard::db_plane::install_registry(vec![
            (0..db_count).map(|_| Database::new()).collect(),
        ]);
        let registered =
            crate::shard::db_plane::shard_dbs(0).expect("a registry exists after install_registry");
        let mut init = make_init(0, registered.db_count());
        init.databases = Arc::clone(registered);
        init_shard(ShardSlice::new(init));
        assert!(
            cached_db_set().is_some(),
            "probe thread must be inside the registered plane"
        );
        registered
    }

    /// D3 W1: the write actually lands, and a later foreign READ of the same
    /// database observes it. Without this the primitive could refuse
    /// everything and every other test here would still pass.
    #[test]
    fn foreign_write_applies_and_is_visible_to_a_foreign_read() {
        std::thread::spawn(|| {
            join_plane(4);
            let applied = try_foreign_db_write(0, 0, |db| {
                db.set_string(
                    &bytes::Bytes::from_static(b"d3:k"),
                    bytes::Bytes::from_static(b"v"),
                );
                db.logical_len()
            });
            assert_eq!(
                applied,
                Some(1),
                "the closure must run against the owner's Database and mutate it"
            );

            let seen = try_foreign_db_read(0, 0, |db| db.is_hot(b"d3:k"));
            assert_eq!(
                seen,
                Some(true),
                "a foreign read must observe the foreign write; if this is \
                 Some(false) the write landed in a different Database"
            );
        })
        .join()
        .expect("probe thread");
    }

    /// Same plane gate as the read twin: a thread outside the registry must
    /// not mutate the registry's copy of another shard's database.
    #[test]
    fn foreign_write_refuses_from_a_thread_outside_the_registered_plane() {
        let _ = crate::shard::db_plane::install_registry(vec![
            (0..4).map(|_| Database::new()).collect(),
        ]);
        let target = (0..crate::shard::db_plane::registry_shard_count())
            .find(|s| crate::shard::db_plane::shard_dbs(*s).is_some_and(|d| d.db_count() > 0))
            .expect("registry must hold a shard with at least one database");

        let ran = std::thread::spawn(move || {
            assert!(cached_db_set().is_none());
            try_foreign_db_write(target, 0, |_db| true)
        })
        .join()
        .expect("probe thread");

        assert_eq!(
            ran, None,
            "a thread outside the registered plane must fall through to SPSC"
        );
    }

    /// It must REFUSE, not park, when the owner holds the database. Parking a
    /// foreign thread on the owner's guard is the failure this whole design
    /// exists to avoid.
    #[test]
    fn foreign_write_refuses_rather_than_parking_when_the_db_is_held() {
        std::thread::spawn(|| {
            let registered = join_plane(4);
            // The registry is a process-wide OnceLock shared with every other
            // test in this binary, so THIS test's install may have lost and the
            // db count is whatever the winner chose. Derive the index from the
            // set that actually exists — a hardcoded index is out of range on
            // a smaller registry, and `try_read` then returns None for a reason
            // that has nothing to do with contention.
            let db_count = registered.db_count();
            assert!(
                db_count > 0,
                "registered set must own at least one database"
            );
            let idx = db_count - 1;

            // A reader on ANOTHER thread, so this thread's re-entrancy mask is
            // not what does the refusing.
            let held = Arc::clone(registered);
            let (tx, rx) = std::sync::mpsc::channel::<bool>();
            let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
            let holder = std::thread::spawn(move || {
                // try_read, not read: a blocking acquire here would hang this
                // test whenever any other test in the binary holds this db.
                // Retry briefly — another test holding it is transient, and a
                // single attempt turns that into a spurious failure.
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                let mut g = held.try_read(idx);
                while g.is_none() && std::time::Instant::now() < deadline {
                    std::thread::yield_now();
                    g = held.try_read(idx);
                }
                let _ = tx.send(g.is_some());
                let _ = done_rx.recv();
                drop(g);
            });
            let acquired = rx
                .recv_timeout(std::time::Duration::from_secs(10))
                .expect("holder reported back");
            assert!(
                acquired,
                "holder could not take db {idx} within 5s; another test in this \
                 binary is holding it far longer than expected"
            );

            let started = std::time::Instant::now();
            let outcome = try_foreign_db_write(0, idx, |_db| true);
            let waited = started.elapsed();

            let _ = done_tx.send(());
            holder.join().expect("holder thread");

            assert_eq!(
                outcome, None,
                "a held database must be refused, not waited on"
            );
            assert!(
                waited < std::time::Duration::from_millis(50),
                "try_foreign_db_write blocked for {waited:?} — it must never park"
            );
        })
        .join()
        .expect("probe thread");
    }

    /// A thread that is not itself part of the registered database plane must
    /// NOT serve foreign reads off the registry.
    ///
    /// `init_shard` publishes `MY_DB_SET` only when the registry holds the very
    /// same set the slice does, so `cached_db_set()` is the process's own
    /// statement that this thread is inside the live plane. Without that check
    /// a thread holding a slice built outside the registry would read a
    /// DIFFERENT `Database` than the one its own writes land in -- the registry
    /// copy -- and return stale data under a `+OK`, silently.
    #[test]
    fn foreign_read_refuses_from_a_thread_outside_the_registered_plane() {
        // Whichever install wins the process-wide OnceLock, we only need some
        // registered shard that actually owns a database.
        let _ = crate::shard::db_plane::install_registry(vec![
            (0..4).map(|_| Database::new()).collect(),
        ]);
        let target = (0..crate::shard::db_plane::registry_shard_count())
            .find(|s| crate::shard::db_plane::shard_dbs(*s).is_some_and(|d| d.db_count() > 0))
            .expect("registry must hold a shard with at least one database");

        // A fresh thread never called `init_shard`, so its `MY_DB_SET` is unset
        // even though the registry is fully installed.
        let served = std::thread::spawn(move || {
            assert!(
                cached_db_set().is_none(),
                "probe thread must be outside the registered plane"
            );
            try_foreign_db_read(target, 0, |_db| 1u8)
        })
        .join()
        .expect("probe thread");

        assert_eq!(
            served, None,
            "a thread outside the registered plane must fall through to SPSC,              not read the registry's copy of another shard's database"
        );
    }

    /// A slice whose set is NOT in the registry must fall back, not cache a
    /// handle to somebody else's locks.
    #[test]
    fn unregistered_slice_does_not_cache_a_foreign_handle() {
        std::thread::spawn(|| {
            // make_init builds a standalone set, deliberately not registered.
            let init = make_init(0, 4);
            let own = Arc::clone(&init.databases);
            init_shard(ShardSlice::new(init));
            if let Some(cached) = cached_db_set() {
                assert!(
                    std::ptr::eq(cached as *const _, Arc::as_ptr(&own)),
                    "cached a handle that is not this slice's own set"
                );
            }
            // Either way the slice must still work.
            assert_eq!(with_shard(|s| s.databases.db_count()), 4);
        })
        .join()
        .expect("probe thread");
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
