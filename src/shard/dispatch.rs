use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};

use atomic_waker::AtomicWaker;
use bytes::Bytes;
use xxhash_rust::xxh64::xxh64;

use crate::protocol::Frame;
use crate::runtime::channel;
use crate::server::response_slot::ResponseSlot;

/// Newtype wrapper for `*const ResponseSlot` to isolate the `Send` unsafety.
///
/// Raw pointers are `!Send` by default. This newtype provides a localized
/// `unsafe impl Send` with a clear safety contract, instead of requiring a
/// blanket `unsafe impl Send for ShardMessage`.
#[derive(Debug, Clone, Copy)]
pub struct ResponseSlotPtr(pub *const ResponseSlot);

// SAFETY: The pointed-to ResponseSlot is Send+Sync (enforced by its own unsafe impls).
// The pointer remains valid for the lifetime of the connection's ResponseSlotPool,
// which outlives all dispatched ShardMessage values.
unsafe impl Send for ResponseSlotPtr {}

/// Lock-free response slot for accumulating cross-shard PUBLISH subscriber counts.
///
/// Instead of N-1 oneshot channels (one per target shard), a single `Arc<PubSubResponseSlot>`
/// is shared. Each shard atomically adds its local subscriber count. The connection handler
/// awaits a `PubSubResponseFuture` that wakes properly when the last shard responds,
/// eliminating the previous spin-yield polling.
pub struct PubSubResponseSlot {
    /// Accumulated subscriber count from all remote shards.
    total: AtomicI64,
    /// Number of shards that haven't responded yet.
    remaining: AtomicU32,
    /// Wakes the connection handler when the last shard responds.
    waker: AtomicWaker,
    /// Per-pair subscriber counts for batched PUBLISH.
    /// Written by the SPSC handler, read by the connection handler after `is_ready()`.
    /// Empty for single-PUBLISH slots (no batch).
    pub counts: Vec<AtomicI64>,
}

impl PubSubResponseSlot {
    /// Create a new slot expecting `num_pending` shard responses.
    pub fn new(num_pending: u32) -> Self {
        Self {
            total: AtomicI64::new(0),
            remaining: AtomicU32::new(num_pending),
            waker: AtomicWaker::new(),
            counts: Vec::new(),
        }
    }

    /// Create a new slot with per-pair count tracking for batched PUBLISH.
    pub fn with_counts(num_pending: u32, num_pairs: usize) -> Self {
        Self {
            total: AtomicI64::new(0),
            remaining: AtomicU32::new(num_pending),
            waker: AtomicWaker::new(),
            counts: (0..num_pairs).map(|_| AtomicI64::new(0)).collect(),
        }
    }

    /// Called by SPSC handler: add this shard's subscriber count and decrement remaining.
    /// Wakes the connection handler when the last shard responds.
    #[inline]
    pub fn add(&self, count: i64) {
        self.total.fetch_add(count, Ordering::Relaxed);
        let prev = self.remaining.fetch_sub(1, Ordering::Release);
        if prev == 1 {
            // This was the last shard -- wake the connection handler
            self.waker.wake();
        }
    }

    /// Called by connection handler: check if all shards have responded.
    #[inline]
    pub fn is_ready(&self) -> bool {
        self.remaining.load(Ordering::Acquire) == 0
    }

    /// Called by connection handler after `is_ready()` returns true: get the accumulated total.
    ///
    /// Uses Relaxed ordering because the Acquire load on `remaining` in `is_ready()`/`poll_ready()`
    /// already establishes happens-before with the Release store in `add()`, ensuring all
    /// prior `total.fetch_add(Relaxed)` writes are visible.
    #[inline]
    pub fn get(&self) -> i64 {
        self.total.load(Ordering::Relaxed)
    }

    /// Poll for readiness, registering the waker for notification.
    ///
    /// Uses Relaxed ordering on `total.load()` after the Acquire load on `remaining` confirms
    /// all shards have responded. The Acquire/Release pair on `remaining` provides the
    /// necessary happens-before guarantee for all prior `total.fetch_add(Relaxed)` stores.
    #[inline]
    pub fn poll_ready(&self, cx: &mut std::task::Context<'_>) -> std::task::Poll<i64> {
        // Fast path: already complete
        if self.remaining.load(Ordering::Acquire) == 0 {
            return std::task::Poll::Ready(self.total.load(Ordering::Relaxed));
        }
        // Register waker before re-checking (avoids missed wake race)
        self.waker.register(cx.waker());
        // Re-check after registration
        if self.remaining.load(Ordering::Acquire) == 0 {
            return std::task::Poll::Ready(self.total.load(Ordering::Relaxed));
        }
        std::task::Poll::Pending
    }
}

/// Future that resolves when all shards have responded to a PUBLISH.
/// Returns the accumulated subscriber count.
pub struct PubSubResponseFuture {
    slot: std::sync::Arc<PubSubResponseSlot>,
}

impl PubSubResponseFuture {
    pub fn new(slot: std::sync::Arc<PubSubResponseSlot>) -> Self {
        Self { slot }
    }
}

impl std::future::Future for PubSubResponseFuture {
    type Output = i64;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<i64> {
        self.slot.poll_ready(cx)
    }
}

const HASH_SEED: u64 = 0;

/// Determine which shard owns a key.
///
/// Uses xxhash64 for fast, high-quality hashing. If the key contains a hash
/// tag (content between `{` and `}`), only the tag is hashed, enabling
/// co-location of related keys on the same shard.
#[inline]
pub fn key_to_shard(key: &[u8], num_shards: usize) -> usize {
    let hash_input = extract_hash_tag(key).unwrap_or(key);
    (xxh64(hash_input, HASH_SEED) % num_shards as u64) as usize
}

/// Determine which shard owns a graph by name.
///
/// Uses the same hash-tag extraction as `key_to_shard`: if the graph name
/// contains `{tag}`, only the tag is hashed. This allows co-locating all
/// operations for a graph on a single shard via `{partition_key}` naming.
///
/// When a graph name has a hash tag, all GRAPH.* commands route to the same
/// shard, eliminating cross-shard traversal entirely.
#[cfg(feature = "graph")]
#[inline]
pub fn graph_to_shard(graph_name: &[u8], num_shards: usize) -> usize {
    key_to_shard(graph_name, num_shards)
}

/// Extract hash tag: content between first `{` and first `}` after it.
///
/// Empty tags `{}` are ignored (returns `None`), matching Redis Cluster behavior.
pub fn extract_hash_tag(key: &[u8]) -> Option<&[u8]> {
    let open = key.iter().position(|&b| b == b'{')?;
    let close = key[open + 1..].iter().position(|&b| b == b'}')?;
    if close == 0 {
        return None;
    }
    Some(&key[open + 1..open + 1 + close])
}

/// Boxed payload for `ShardMessage::TextAggregate` (Phase 152 D-05/D-07).
///
/// Kept as a separate struct so the enum variant stays small (single
/// pointer) — the raw payload is ~120 bytes which would push the overall
/// enum past the 256-byte cap asserted at module bottom.
#[cfg(feature = "text-index")]
pub struct TextAggregatePayload {
    pub index_name: Bytes,
    pub query: Bytes,
    pub pipeline: Vec<crate::text::aggregate::AggregateStep>,
    pub reply_tx: channel::OneshotSender<Frame>,
}

/// Boxed payload for `ShardMessage::InvertedSearch` (Phase 152 Plan 06, B-02).
///
/// Transports a `FieldFilter` (TAG today; Plan 07 adds NumericRange) across
/// the SPSC so multi-shard FT.SEARCH with `@field:{value}` carries the
/// field-scoped semantics instead of degrading to a bag-of-words BM25 scatter.
///
/// Unlike `ShardMessage::TextSearch`, this payload carries no `global_df`,
/// `global_n`, or highlight / summarize options — FieldFilter results have
/// score=0.0 and no BM25 pairing. Boxed to keep the overall enum within the
/// 512-byte cap asserted at module bottom (`FieldFilter::Tag` is two Bytes
/// handles ~32 B plus discriminant, but boxing matches the pattern set by
/// `TextAggregatePayload` and `FtHybridPayload`).
///
/// NOT persisted to WAL / NOT gossiped — in-process SPSC only.
#[cfg(feature = "text-index")]
pub struct InvertedSearchPayload {
    pub index_name: Bytes,
    pub filter: crate::command::vector_search::ft_text_search::FieldFilter,
    pub top_k: usize,
    pub offset: usize,
    pub count: usize,
    pub reply_tx: channel::OneshotSender<Frame>,
}

/// Boxed payload for `ShardMessage::FtHybrid` (Phase 152 Plan 05, D-13).
///
/// Carries a pre-computed global IDF (Phase 1 DFS aggregate) so the shard
/// can score BM25 with multi-shard-correct ranking. The shard returns three
/// separate raw per-stream lists (bm25, dense, sparse) UNFUSED — coordinator
/// unions per stream and calls `rrf_fuse_three` exactly once on the unions.
///
/// Boxed to keep the enum discriminant + pointer at ~16 bytes, per the
/// pattern established by `TextAggregatePayload`.
#[cfg(feature = "text-index")]
pub struct FtHybridPayload {
    pub index_name: Bytes,
    pub query_terms: Vec<crate::command::vector_search::ft_text_search::QueryTerm>,
    pub dense_field: Bytes,
    pub dense_blob: Bytes,
    pub sparse_field: Option<Bytes>,
    pub sparse_blob: Option<Bytes>,
    pub weights: [f32; 3],
    pub k_per_stream: usize,
    pub top_k: usize,
    pub global_df: std::collections::HashMap<String, u32>,
    pub global_n: u32,
    /// Coordinator-resolved AS_OF / TXN snapshot LSN (Phase 171, HYB-02 / SCAT-02).
    /// `0` means "no temporal filtering" (default for non-AS_OF hybrid callers).
    /// Non-zero values are forwarded to the dense KNN stream for MVCC filtering.
    /// BM25 stream remains AS_OF-unaware until text-index MVCC ships (v0.2).
    pub as_of_lsn: u64,
    pub reply_tx: channel::OneshotSender<Frame>,
}

/// Boxed payload for `ShardMessage::TextSearch` (Phase 177, hot-path split).
///
/// Before boxing, `TextSearch` packed `HashMap<String, u32>` + `Vec<QueryTerm>`
/// + smallvec fields inline at ~280 bytes, driving the entire `ShardMessage`
///   enum past 288 B and fragmenting every SPSC ring slot across 5 cache lines.
///   Boxing moves the payload to the heap and collapses the variant to a single
///   pointer (16 B incl. discriminant) so the ring slot stays within 2 cache
///   lines when the hot slotted variants are enqueued.
pub struct TextSearchPayload {
    pub index_name: Bytes,
    pub field_idx: Option<usize>,
    pub query_terms: Vec<crate::command::vector_search::ft_text_search::QueryTerm>,
    pub global_df: std::collections::HashMap<String, u32>,
    pub global_n: u32,
    pub top_k: usize,
    pub offset: usize,
    pub count: usize,
    pub highlight_opts: Option<crate::command::vector_search::ft_text_search::HighlightOpts>,
    pub summarize_opts: Option<crate::command::vector_search::ft_text_search::SummarizeOpts>,
    pub reply_tx: channel::OneshotSender<Frame>,
}

/// Boxed payload for `ShardMessage::VectorSearch` (Phase 177, hot-path split).
///
/// The inline variant carried two `Bytes` (32 B each), two `usize`, and a
/// oneshot sender — ~88 B. Boxing reduces its enum slot to 16 B so the
/// hot slotted variants pay no cache-line tax from its presence.
pub struct VectorSearchPayload {
    pub index_name: Bytes,
    pub query_blob: Bytes,
    pub k: usize,
    pub as_of_lsn: u64,
    pub reply_tx: channel::OneshotSender<Frame>,
}

/// Boxed payload for `ShardMessage::BlockRegister` (Phase 177, hot-path split).
///
/// `BlockedCommand::XReadGroup` carries Vec + two Bytes + count options, pushing
/// the inline variant past 160 B. Boxing collapses it to a pointer.
pub struct BlockRegisterPayload {
    pub db_index: usize,
    pub key: Bytes,
    pub wait_id: u64,
    pub cmd: crate::blocking::BlockedCommand,
    pub reply_tx: channel::OneshotSender<Option<crate::protocol::Frame>>,
}

/// Portable raw socket file descriptor type.
///
/// On Unix this is `std::os::unix::io::RawFd` (i32). On Windows (and any other
/// non-unix target) it aliases to `i32` so that `MigrateConnectionPayload` and
/// the pending-migrations Vec compile on all platforms. Actual fd operations are
/// gated by `#[cfg(unix)]` at every call site.
#[cfg(unix)]
pub type RawSocketFd = std::os::unix::io::RawFd;
#[cfg(not(unix))]
pub type RawSocketFd = i32;

/// Boxed payload for `ShardMessage::MigrateConnection` (Phase 177, hot-path split).
///
/// `MigratedConnectionState` already holds heap-backed strings/bytes but still
/// exceeds 120 B inline. Moving it behind a Box keeps the enum slot in the
/// cache-line budget set by the slotted variants.
pub struct MigrateConnectionPayload {
    pub fd: RawSocketFd,
    pub state: crate::server::conn::affinity::MigratedConnectionState,
}

/// Boxed payload for `ShardMessage::PubSubPublish` (Phase 177, hot-path split).
///
/// Two `Bytes` (~64 B) plus an Arc'd slot — worth boxing to keep the enum
/// slot size driven by the slotted variants rather than fan-out traffic.
pub struct PubSubPublishPayload {
    pub channel: Bytes,
    pub message: Bytes,
    pub slot: std::sync::Arc<PubSubResponseSlot>,
}

/// Boxed payload for `ShardMessage::GraphTraverse` (Phase 177, hot-path split).
///
/// Six fields including a Vec<u64> and Bytes — inline variant was ~80 B.
#[cfg(feature = "graph")]
pub struct GraphTraversePayload {
    pub graph_name: Bytes,
    pub node_ids: Vec<u64>,
    pub remaining_hops: u32,
    pub edge_type_filter: Option<u16>,
    pub snapshot_lsn: u64,
    pub reply_tx: channel::OneshotSender<Frame>,
}

/// Boxed payload for `ShardMessage::GraphRollback` (multi-shard TXN.ABORT).
///
/// Carries the graph-undo ops and create-intents whose graph names hash to
/// the receiving shard, so the rollback mutates the store that actually owns
/// those graphs. Boxed: two Vecs + oneshot would blow the 64 B enum ceiling.
#[cfg(feature = "graph")]
pub struct GraphRollbackPayload {
    pub txn_id: u64,
    pub graph_undo: Vec<crate::transaction::GraphUndoOp>,
    pub graph_intents: Vec<crate::transaction::GraphIntent>,
    pub reply_tx: channel::OneshotSender<Frame>,
}

/// Boxed payload for `ShardMessage::CdcSubscribe` (C3b-2).
///
/// Carries the per-subscriber sink and replay start point to the shard
/// fan-out registry. The shard side wires the `tx` into a
/// `CdcSubscriber` whose tail reader uses the shard's own `wal_dir`,
/// so the dispatch layer never has to know that path.
pub struct CdcSubscribePayload {
    /// Bounded channel into which the shard pushes Debezium envelopes.
    /// Backpressure is non-blocking: `try_send` returning `Full` evicts
    /// the subscriber from the registry (slow-consumer disconnect).
    pub tx: channel::MpscSender<bytes::Bytes>,
    /// Inclusive LSN floor — records with `lsn < from_lsn` are skipped.
    pub from_lsn: u64,
}

/// Messages sent to a shard via SPSC channels from the connection layer
/// or from other shards for cross-shard operations.
pub enum ShardMessage {
    /// New TCP connection assigned to this shard by the listener.
    NewConnection(crate::runtime::TcpStream),
    /// Execute a single-key command on this shard, send result back.
    /// The command is wrapped in Arc to avoid deep-cloning the Frame::Array
    /// when dispatching across shards (only an Arc refcount bump instead of
    /// heap-allocating a new Vec + cloning each inner Frame).
    Execute {
        db_index: usize,
        command: std::sync::Arc<Frame>,
        reply_tx: channel::OneshotSender<Frame>,
    },
    /// Execute a multi-key sub-operation on this shard.
    MultiExecute {
        db_index: usize,
        commands: Vec<(Bytes, Frame)>, // (key, command) pairs for this shard
        reply_tx: channel::OneshotSender<Vec<Frame>>,
    },
    /// Execute a batch of pipelined commands on this shard.
    /// Each command is independent (not transactional). Returns one response per command.
    PipelineBatch {
        db_index: usize,
        commands: Vec<std::sync::Arc<Frame>>,
        reply_tx: channel::OneshotSender<Vec<Frame>>,
    },
    /// Begin a cooperative snapshot at the given epoch.
    /// Shard creates SnapshotState and advances one segment per tick.
    /// Sends reply when snapshot is complete.
    SnapshotBegin {
        epoch: u64,
        snapshot_dir: std::path::PathBuf,
        reply_tx: channel::OneshotSender<Result<(), String>>,
    },
    /// Register a blocked client waiting for data on a key (cross-shard).
    ///
    /// Boxed (Phase 177) — `BlockedCommand::XReadGroup` pushes the inline variant
    /// past 160 B.
    BlockRegister(Box<BlockRegisterPayload>),
    /// Cancel a blocked client registration (woken by another shard or timed out).
    BlockCancel { wait_id: u64 },
    /// Register a connected replica's per-shard sender channel with this shard.
    /// Called once per shard per replica when a new replica connection is established.
    /// The shard adds `tx` to its replica_txs list for WAL fan-out.
    RegisterReplica {
        replica_id: u64,
        tx: channel::MpscSender<bytes::Bytes>,
    },
    /// Remove a replica's sender channel from this shard's fan-out list.
    /// Called when a replica disconnects or REPLICAOF NO ONE is executed.
    UnregisterReplica { replica_id: u64 },
    /// Register a CDC subscriber with this shard's fan-out registry (C3b-2).
    ///
    /// The connection handler creates a bounded channel, ships the sender
    /// here, and keeps the receiver for its own write-loop. The shard's
    /// 1ms periodic tick drives `CdcSubscriberRegistry::fanout_tick`,
    /// pumping freshly-flushed WAL records through the channel as
    /// Debezium JSON envelopes.
    ///
    /// Boxed because the payload (sender + u64) plus enum discriminant
    /// stays clear of the 64-byte cap, but boxing is the cheaper choice
    /// when the variant is rare relative to per-write traffic.
    CdcSubscribe(Box<CdcSubscribePayload>),
    /// Return keys in a specific hash slot (for CLUSTER GETKEYSINSLOT).
    GetKeysInSlot {
        db_index: usize,
        slot: u16,
        count: usize,
        reply_tx: channel::OneshotSender<Vec<bytes::Bytes>>,
    },
    /// Notify shard of slot ownership changes (no-op placeholder for future per-shard caching).
    SlotOwnershipUpdate {
        add_slots: Vec<u16>,
        remove_slots: Vec<u16>,
    },
    /// Fan-out a loaded script to all shards so EVALSHA works regardless of which shard receives it.
    /// Sent by the connection handler on SCRIPT LOAD; received by all other shards' SPSC drain loops.
    ScriptLoad { sha1: String, script: bytes::Bytes },
    /// Migrate a connection's file descriptor to this shard.
    /// The source shard has deregistered the FD and extracted connection state.
    /// This shard must reconstruct the TCP stream and spawn a new handler.
    ///
    /// Boxed (Phase 177) — `MigratedConnectionState` exceeds 120 B.
    MigrateConnection(Box<MigrateConnectionPayload>),
    /// Execute a single command with pre-allocated response slot (zero allocation).
    /// Used instead of Execute for cross-shard write dispatch.
    ExecuteSlotted {
        db_index: usize,
        command: std::sync::Arc<Frame>,
        response_slot: ResponseSlotPtr,
    },
    /// Execute multi-key sub-operation with pre-allocated response slot.
    /// Used instead of MultiExecute for cross-shard multi-key dispatch.
    MultiExecuteSlotted {
        db_index: usize,
        commands: Vec<(Bytes, Frame)>,
        response_slot: ResponseSlotPtr,
    },
    /// Execute pipelined batch with pre-allocated response slot.
    /// Used instead of PipelineBatch for cross-shard pipeline dispatch.
    PipelineBatchSlotted {
        db_index: usize,
        commands: Vec<std::sync::Arc<Frame>>,
        response_slot: ResponseSlotPtr,
    },
    /// Execute a vector search query on this shard's VectorStore.
    /// Used for cross-shard scatter-gather: coordinator sends to all shards,
    /// each returns local top-K, coordinator merges.
    ///
    /// `as_of_lsn` carries the coordinator-resolved AS_OF / TXN snapshot LSN
    /// (Phase 171, SCAT-01). `0` means "no temporal filtering" (default
    /// behavior for non-AS_OF callers); non-zero values are the LSN boundary
    /// applied by `search_local_raw` for MVCC filtering on the responder.
    ///
    /// Boxed (Phase 177) — inline variant was ~88 B.
    VectorSearch(Box<VectorSearchPayload>),
    /// DFS Phase 1: collect per-term document frequency from this shard.
    ///
    /// Returns `Frame::Array` with interleaved `[term1, df1, term2, df2, ..., "N", total_docs]`
    /// for each (field_idx, terms) pair in `field_queries`.
    /// The coordinator aggregates these across all shards to compute global IDF.
    DocFreq {
        index_name: Bytes,
        /// (field_idx, terms) pairs -- None field_idx means use field 0 (all-field mode).
        field_queries: Vec<(Option<usize>, Vec<String>)>,
        reply_tx: channel::OneshotSender<Frame>,
    },
    /// DFS Phase 2: execute BM25 text search with injected global IDF.
    ///
    /// Returns `Frame::Array` in the same format as `ft_text_search` response:
    /// `[total, key1, ["__bm25_score", "N.NNNNNN"], key2, [...], ...]`
    ///
    /// `highlight_opts` and `summarize_opts` are passed through for Plan 03
    /// post-processing. In this plan (150-02) they are always `None`.
    ///
    /// Boxed (Phase 177) — the payload's HashMap + Vec<QueryTerm> pushed this
    /// variant past 280 B inline.
    TextSearch(Box<TextSearchPayload>),
    /// Execute an FT.* command on this shard's VectorStore.
    /// For FT.CREATE, FT.DROPINDEX, FT.INFO -- operations that modify/read
    /// VectorStore state rather than search.
    VectorCommand {
        command: std::sync::Arc<Frame>,
        reply_tx: channel::OneshotSender<Frame>,
    },
    /// FT.AGGREGATE phase 1: execute pipeline UP TO post-GROUPBY on this shard
    /// and ship the resulting `ShardPartial` back for coordinator-side merge
    /// (Phase 152 D-05/D-07).
    ///
    /// The variant is **boxed** because the inline payload (index_name,
    /// query, Vec<AggregateStep>, oneshot) pushes `ShardMessage` past the
    /// 256-byte cap asserted at module bottom. Boxing moves the payload to
    /// the heap and keeps the enum discriminant + pointer at ~16 bytes.
    ///
    /// The shard is expected to:
    /// 1. Resolve the index via `text_store.get_index(&index_name)`.
    /// 2. Run `execute_local_partial(...)` from
    ///    `crate::command::vector_search::ft_aggregate`.
    /// 3. Return a Frame encoding `ShardPartial` (via
    ///    `encode_shard_partial`) or `Frame::Error` on any failure.
    ///
    /// Per D-07, SORTBY/LIMIT are **NOT** applied at the shard — they are
    /// coordinator-only global stages.
    #[cfg(feature = "text-index")]
    TextAggregate(Box<TextAggregatePayload>),
    /// Execute a hybrid FT.SEARCH on this shard with pre-computed global IDF (Phase 152 D-13).
    ///
    /// Each shard computes BM25 (with global IDF), dense KNN, and optional sparse in parallel,
    /// then returns THREE separate raw per-stream lists (bm25, dense, sparse) UNFUSED.
    /// The coordinator unions shard lists per stream and calls `rrf_fuse_three` exactly
    /// once on the unions — preserving RRF math correctness (per D-13 + D-14).
    ///
    /// Boxed per the `TextAggregatePayload` pattern to keep the enum discriminant +
    /// pointer at ~16 bytes; the raw payload carries a `HashMap<String, u32>` + Vec<QueryTerm>
    /// + blob bytes which would otherwise push `ShardMessage` past the 512-byte cap.
    #[cfg(feature = "text-index")]
    FtHybrid(Box<FtHybridPayload>),
    /// Cross-shard FieldFilter search (Phase 152 Plan 06, B-02).
    ///
    /// Used for `@field:{value}` (TAG) — Plan 07 extends to `@field:[min max]`
    /// numeric ranges. Bypasses BM25 entirely on the shard side: the remote
    /// shard runs `TextIndex::search_tag` (or Plan 07 `search_numeric_range`)
    /// and returns matching keys with `score=0.0`.
    ///
    /// Boxed for enum-size discipline — see `InvertedSearchPayload` docs.
    #[cfg(feature = "text-index")]
    InvertedSearch(Box<InvertedSearchPayload>),
    /// Execute a GRAPH.* command on this shard's GraphStore.
    #[cfg(feature = "graph")]
    GraphCommand {
        command: std::sync::Arc<Frame>,
        reply_tx: channel::OneshotSender<Frame>,
    },
    /// Cross-shard graph traversal: expand the given nodes locally and return neighbors.
    /// Used by scatter-gather coordinator for multi-shard BFS expansion.
    ///
    /// Boxed (Phase 177) — Vec<u64> + Bytes + oneshot pushed this past 80 B inline.
    #[cfg(feature = "graph")]
    GraphTraverse(Box<GraphTraversePayload>),
    /// Multi-shard TXN.ABORT: apply graph rollback (undo ops + create-intent
    /// removal) for graphs owned by this shard.
    #[cfg(feature = "graph")]
    GraphRollback(Box<GraphRollbackPayload>),
    /// Cross-shard PUBLISH with shared atomic response slot for subscriber count accumulation.
    ///
    /// Boxed (Phase 177) — two Bytes + Arc was 72 B inline.
    PubSubPublish(Box<PubSubPublishPayload>),
    /// Batched cross-shard PUBLISH for pipeline efficiency.
    /// Multiple (channel, message) pairs destined for the same shard, sharing one ResponseSlot.
    /// Each pair's subscriber count is accumulated into the slot's `counts` field, and the
    /// batch total is accumulated into the slot's `total`.
    PubSubPublishBatch {
        pairs: Vec<(Bytes, Bytes)>,
        slot: std::sync::Arc<PubSubResponseSlot>,
    },
    /// Swap two databases within this shard (SWAPDB implementation).
    ///
    /// The SPSC handler emits a per-shard WAL record before performing the swap,
    /// then calls `ShardDatabases::swap_dbs(shard_id, a, b)`.  On completion it
    /// sends `()` on `reply_tx` so the coordinator can await all-shard acks before
    /// returning `+OK` to the client.
    ///
    /// Indices `a` and `b` are pre-validated (0..db_count, a != b) by the
    /// coordinator; the handler may assert rather than re-validate.
    SwapDb {
        a: usize,
        b: usize,
        reply_tx: channel::OneshotSender<()>,
    },
    /// Graceful shutdown signal.
    Shutdown,
}

// ShardMessage is Send because all fields are Send. The raw pointer in
// ResponseSlotPtr is the only non-auto-Send field, and it has its own
// localized unsafe impl Send with documented safety invariants.

/// Compile-time guard: keep `ShardMessage` under 256 bytes to prevent
/// hot-path allocator pressure in the SPSC ring buffer. Large variants
/// MUST be boxed (see `TextAggregatePayload` for Phase 152 D-05).
const _: () = {
    // Keep ShardMessage under a 512-byte cap. The practical ceiling is set
    // by the largest existing variant (TextSearch already packs ~280 B via
    // HashMap + smallvecs). Phase 152's TextAggregate variant is boxed
    // (`Box<TextAggregatePayload>`) so it contributes only a pointer.
    //
    // If a new variant is added that would push size past this cap, box it
    // following the `TextAggregatePayload` pattern.
    // Phase 177 hot-path split: after boxing TextSearch / VectorSearch /
    // BlockRegister / MigrateConnection / PubSubPublish / GraphTraverse, the
    // actual measured enum size on aarch64 / x86_64 is 64 bytes — exactly
    // one cache line. The assertion is tightened to that measured ceiling
    // so any future variant that regresses cache-line residency forces an
    // explicit boxing decision at review time. See
    // `print_shard_message_sizes` for layout diagnostics.
    assert!(
        std::mem::size_of::<ShardMessage>() <= 64,
        "ShardMessage exceeded the 64-byte (1 cache-line) cap -- box the largest variant",
    );
};

/// Backoff between cross-shard SPSC push retries when the target ring is full
/// (F3). Small so genuine transient backpressure adds minimal latency; runtime
/// timers may round it up to their tick granularity.
pub(crate) const CROSS_SHARD_PUSH_BACKOFF: std::time::Duration =
    std::time::Duration::from_micros(100);

/// Max push retries before giving up on a non-draining target ring (F3). With
/// the 100µs backoff the real budget is ~0.5s (timer honoured) to a few
/// seconds (coarse granularity) — wedged-shard detection scale, generous
/// enough not to false-reject a merely saturated shard under heavy load.
pub(crate) const CROSS_SHARD_PUSH_MAX_RETRIES: u32 = 5_000;

/// Outcome of a bounded cross-shard SPSC push ([`push_with_backpressure`], F3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PushOutcome {
    /// The target ring accepted the message.
    Pushed,
    /// The ring stayed full for the whole retry budget — the target shard is
    /// not draining (wedged or saturated). The command was **never executed**,
    /// so the caller returns a clean reject error to the client.
    Backpressure,
    /// Shutdown was requested mid-retry. The command was **never executed**.
    Cancelled,
}

/// Push a message onto a cross-shard SPSC ring with bounded backpressure
/// retry (F3 — design-for-failure).
///
/// `try_push` returns `true` when the ring accepted the message and `false`
/// when the ring is full. On a `false`, the closure MUST retain ownership of
/// the message (stash it) so the next attempt can re-send it. We back off
/// `backoff` and retry up to `max_retries` times, checking `shutdown` before
/// every sleep so a graceful shutdown can never be blocked by a wedged peer.
///
/// This replaces the previous unbounded `loop { try_push; sleep(10µs) }` in
/// the monoio cross-shard dispatch path. That loop parked a connection task
/// forever — holding its read/write buffers and piling wakers into the
/// `pending_wakers` relay — whenever a target shard stopped draining: the
/// multi-shard "zombie connection" RAM-growth vector from the investigation.
///
/// **Hot path:** the happy case is a single `try_push()` returning `true` —
/// no sleep, no allocation. `TimerImpl::sleep` boxes a future, but only on
/// the contended retry path (ring already full), never on the fast path.
///
/// **Wall-clock budget:** the total bound is roughly
/// `max_retries * max(backoff, timer_granularity)`. Callers pick
/// `max_retries` for the *intended* budget, not a literal microsecond count,
/// because runtime timers round small sleeps up to their tick granularity.
pub(crate) async fn push_with_backpressure(
    shutdown: &crate::runtime::cancel::CancellationToken,
    max_retries: u32,
    backoff: std::time::Duration,
    mut try_push: impl FnMut() -> bool,
) -> PushOutcome {
    use crate::runtime::{TimerImpl, traits::RuntimeTimer};

    if try_push() {
        return PushOutcome::Pushed;
    }
    for _ in 0..max_retries {
        if shutdown.is_cancelled() {
            return PushOutcome::Cancelled;
        }
        TimerImpl::sleep(backoff).await;
        if try_push() {
            return PushOutcome::Pushed;
        }
    }
    PushOutcome::Backpressure
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    #[cfg(feature = "runtime-tokio")]
    use std::sync::Arc;

    // ── F3: bounded cross-shard push (push_with_backpressure) ──
    // Tokio-gated: these drive the runtime timer via TimerImpl::sleep.

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn push_backpressure_pushes_on_first_try_without_sleeping() {
        let token = crate::runtime::cancel::CancellationToken::new();
        let mut calls = 0u32;
        let start = std::time::Instant::now();
        let outcome =
            push_with_backpressure(&token, 1000, std::time::Duration::from_millis(10), || {
                calls += 1;
                true // ring accepts immediately
            })
            .await;
        assert_eq!(outcome, PushOutcome::Pushed);
        assert_eq!(calls, 1, "happy path must not retry");
        assert!(
            start.elapsed() < std::time::Duration::from_millis(5),
            "happy path must not sleep"
        );
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn push_backpressure_succeeds_after_transient_full() {
        let token = crate::runtime::cancel::CancellationToken::new();
        let mut calls = 0u32;
        let outcome =
            push_with_backpressure(&token, 1000, std::time::Duration::from_millis(1), || {
                calls += 1;
                calls >= 3 // full twice, then drains
            })
            .await;
        assert_eq!(outcome, PushOutcome::Pushed);
        assert_eq!(calls, 3, "should retry until the ring drains");
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn push_backpressure_gives_up_when_ring_never_drains() {
        // A wedged target shard: ring always full. The bounded retry MUST
        // give up (Backpressure) rather than spin forever — the command was
        // never accepted, so the caller returns a clean reject.
        let token = crate::runtime::cancel::CancellationToken::new();
        let mut calls = 0u32;
        let start = std::time::Instant::now();
        let outcome =
            push_with_backpressure(&token, 5, std::time::Duration::from_millis(1), || {
                calls += 1;
                false // never accepts
            })
            .await;
        let elapsed = start.elapsed();
        assert_eq!(outcome, PushOutcome::Backpressure);
        // initial attempt + 5 retries = 6 try_push calls.
        assert_eq!(calls, 6, "initial try + max_retries attempts");
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "must give up within the budget, not hang (took {:?})",
            elapsed
        );
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn push_backpressure_aborts_on_shutdown() {
        // Shutdown signalled before the call: a full ring must surface
        // Cancelled immediately (checked before the first backoff sleep),
        // never blocking a graceful shutdown on a wedged peer.
        let token = crate::runtime::cancel::CancellationToken::new();
        token.cancel();
        let mut calls = 0u32;
        let outcome = push_with_backpressure(
            &token,
            1_000_000, // huge budget — only the shutdown check can break us out
            std::time::Duration::from_millis(10),
            || {
                calls += 1;
                false
            },
        )
        .await;
        assert_eq!(outcome, PushOutcome::Cancelled);
        assert_eq!(calls, 1, "one attempt, then the shutdown check aborts");
    }

    /// Diagnostic: print ShardMessage total size + representative payload sizes
    /// so we can see which variants cap the enum and target them for hot/cold split.
    #[test]
    fn print_shard_message_sizes() {
        use std::mem::size_of;
        eprintln!("== ShardMessage layout ==");
        eprintln!(
            "ShardMessage total           = {}",
            size_of::<ShardMessage>()
        );
        eprintln!(
            "  TcpStream                  = {}",
            size_of::<crate::runtime::TcpStream>()
        );
        eprintln!(
            "  Arc<Frame>                 = {}",
            size_of::<std::sync::Arc<Frame>>()
        );
        eprintln!(
            "  OneshotSender<Frame>       = {}",
            size_of::<channel::OneshotSender<Frame>>()
        );
        eprintln!(
            "  ResponseSlotPtr            = {}",
            size_of::<ResponseSlotPtr>()
        );
        eprintln!("  Bytes                      = {}", size_of::<Bytes>());
        eprintln!(
            "  PathBuf                    = {}",
            size_of::<std::path::PathBuf>()
        );
        eprintln!(
            "  Vec<(Bytes, Frame)>        = {}",
            size_of::<Vec<(Bytes, Frame)>>()
        );
        eprintln!(
            "  Vec<Arc<Frame>>            = {}",
            size_of::<Vec<std::sync::Arc<Frame>>>()
        );
        eprintln!(
            "  Vec<(Option<usize>, Vec<String>)> = {}",
            size_of::<Vec<(Option<usize>, Vec<String>)>>()
        );
        eprintln!(
            "  Vec<(Bytes, Bytes)>        = {}",
            size_of::<Vec<(Bytes, Bytes)>>()
        );
        eprintln!("  Vec<u16>                   = {}", size_of::<Vec<u16>>());
    }

    #[test]
    fn test_key_to_shard_deterministic() {
        let key = b"user:1234";
        let num_shards = 8;
        let shard1 = key_to_shard(key, num_shards);
        let shard2 = key_to_shard(key, num_shards);
        let shard3 = key_to_shard(key, num_shards);
        assert_eq!(shard1, shard2);
        assert_eq!(shard2, shard3);
    }

    #[test]
    fn test_key_to_shard_distribution() {
        let num_shards = 8;
        let mut seen = HashSet::new();
        // With enough different keys, we should hit multiple shards
        for i in 0..100 {
            let key = format!("key:{}", i);
            seen.insert(key_to_shard(key.as_bytes(), num_shards));
        }
        // With 100 keys across 8 shards, we should see most shards used
        assert!(
            seen.len() >= 4,
            "Expected at least 4 shards used, got {}",
            seen.len()
        );
    }

    #[test]
    fn test_extract_hash_tag_basic() {
        assert_eq!(extract_hash_tag(b"{user}.name"), Some(b"user".as_slice()));
        assert_eq!(
            extract_hash_tag(b"prefix{tag}suffix"),
            Some(b"tag".as_slice())
        );
    }

    #[test]
    fn test_extract_hash_tag_empty() {
        // Empty tag {} should return None
        assert_eq!(extract_hash_tag(b"{}key"), None);
        assert_eq!(extract_hash_tag(b"a{}b"), None);
    }

    #[test]
    fn test_extract_hash_tag_none() {
        // No braces at all
        assert_eq!(extract_hash_tag(b"simplekey"), None);
        // Only opening brace
        assert_eq!(extract_hash_tag(b"key{nope"), None);
    }

    #[test]
    fn test_hash_tag_co_location() {
        let num_shards = 16;
        let shard_name = key_to_shard(b"{user}.name", num_shards);
        let shard_email = key_to_shard(b"{user}.email", num_shards);
        let shard_age = key_to_shard(b"{user}.age", num_shards);
        assert_eq!(shard_name, shard_email);
        assert_eq!(shard_email, shard_age);
    }

    #[test]
    fn test_key_to_shard_single_shard() {
        // With only 1 shard, everything maps to shard 0
        assert_eq!(key_to_shard(b"any_key", 1), 0);
        assert_eq!(key_to_shard(b"another_key", 1), 0);
        assert_eq!(key_to_shard(b"{tag}.key", 1), 0);
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn test_pubsub_slot_waker() {
        let slot = Arc::new(PubSubResponseSlot::new(1));
        let slot2 = slot.clone();

        // Spawn a task that adds to the slot after a short delay
        let handle = tokio::spawn(async move {
            tokio::task::yield_now().await;
            slot2.add(42);
        });

        // Await the future -- should wake properly when add() is called
        let result = PubSubResponseFuture::new(slot).await;
        assert_eq!(result, 42);
        handle.await.unwrap();
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn test_pubsub_slot_multiple_shards() {
        let slot = Arc::new(PubSubResponseSlot::new(3));

        // Spawn 3 tasks that each add a subscriber count
        let handles: Vec<_> = (0..3)
            .map(|i| {
                let slot = slot.clone();
                tokio::spawn(async move {
                    tokio::task::yield_now().await;
                    slot.add(10 + i);
                })
            })
            .collect();

        // Await the future -- should resolve with the sum of all 3 adds
        let result = PubSubResponseFuture::new(slot).await;
        // 10 + 11 + 12 = 33
        assert_eq!(result, 33);

        for h in handles {
            h.await.unwrap();
        }
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn test_pubsub_slot_already_ready() {
        // Slot with 0 pending should resolve immediately
        let slot = Arc::new(PubSubResponseSlot::new(0));
        let result = PubSubResponseFuture::new(slot).await;
        assert_eq!(result, 0);
    }

    #[cfg(feature = "graph")]
    #[test]
    fn test_graph_to_shard_deterministic() {
        let name = b"social_graph";
        let num_shards = 8;
        let s1 = super::graph_to_shard(name, num_shards);
        let s2 = super::graph_to_shard(name, num_shards);
        assert_eq!(s1, s2);
    }

    #[cfg(feature = "graph")]
    #[test]
    fn test_graph_to_shard_hash_tag_co_location() {
        let num_shards = 16;
        // All graph names with the same hash tag route to the same shard.
        let s1 = super::graph_to_shard(b"{social}.friends", num_shards);
        let s2 = super::graph_to_shard(b"{social}.posts", num_shards);
        let s3 = super::graph_to_shard(b"{social}.likes", num_shards);
        assert_eq!(s1, s2);
        assert_eq!(s2, s3);
    }

    #[cfg(feature = "graph")]
    #[test]
    fn test_graph_to_shard_no_tag_uses_full_name() {
        let num_shards = 16;
        // Without hash tag, different names may (likely) route to different shards.
        let s1 = super::graph_to_shard(b"graph_alpha", num_shards);
        let s2 = super::graph_to_shard(b"graph_beta", num_shards);
        // Not guaranteed different, but at least the function works.
        let _ = (s1, s2);
    }

    #[cfg(feature = "graph")]
    #[test]
    fn test_graph_to_shard_single_shard() {
        assert_eq!(super::graph_to_shard(b"any_graph", 1), 0);
        assert_eq!(super::graph_to_shard(b"{tag}.graph", 1), 0);
    }

    /// The 512-byte cap is enforced at compile time by the `const _: ()`
    /// assertion at module bottom. Phase 152 boxes the TextAggregate
    /// payload (`Box<TextAggregatePayload>`) so the variant contributes
    /// only a pointer, matching how `Vec<..>` fields in existing
    /// TextSearch already amortise. This runtime test documents the
    /// current size for PR reviewers and fails loud if it creeps up.
    #[test]
    fn test_shard_message_size_bounded() {
        let sz = std::mem::size_of::<ShardMessage>();
        assert!(
            sz <= 512,
            "ShardMessage size {} exceeds the 512-byte cap",
            sz,
        );
    }

    #[cfg(feature = "text-index")]
    #[test]
    fn test_text_aggregate_roundtrip() {
        // Construct a boxed TextAggregate payload with a realistic pipeline
        // (GroupBy + SortBy + Limit) and a oneshot sender. Send through the
        // oneshot + pattern-match the variant on the receive side to confirm
        // every field survives the message-channel round trip.
        use crate::text::aggregate::{AggregateStep, ReducerFn, ReducerSpec, SortOrder};
        use smallvec::SmallVec;

        let (reply_tx, reply_rx) = channel::oneshot();
        let mut gb_fields: SmallVec<[Bytes; 4]> = SmallVec::new();
        gb_fields.push(Bytes::from_static(b"@status"));
        let reducers = vec![ReducerSpec {
            fn_name: ReducerFn::Count,
            field: None,
            alias: Bytes::from_static(b"cnt"),
        }];

        let mut sort_keys: SmallVec<[(Bytes, SortOrder); 4]> = SmallVec::new();
        sort_keys.push((Bytes::from_static(b"cnt"), SortOrder::Desc));

        let pipeline = vec![
            AggregateStep::GroupBy {
                fields: gb_fields,
                reducers,
            },
            AggregateStep::SortBy {
                keys: sort_keys,
                max: None,
            },
            AggregateStep::Limit {
                offset: 0,
                count: 10,
            },
        ];

        let msg = ShardMessage::TextAggregate(Box::new(TextAggregatePayload {
            index_name: Bytes::from_static(b"myidx"),
            query: Bytes::from_static(b"*"),
            pipeline: pipeline.clone(),
            reply_tx,
        }));

        match msg {
            ShardMessage::TextAggregate(payload) => {
                assert_eq!(payload.index_name.as_ref(), b"myidx");
                assert_eq!(payload.query.as_ref(), b"*");
                assert_eq!(payload.pipeline.len(), 3);
                // Round-trip the reply channel to confirm the oneshot works.
                let _ = payload
                    .reply_tx
                    .send(Frame::SimpleString(bytes::Bytes::from_static(b"OK")));
            }
            _ => panic!("expected TextAggregate variant"),
        }

        // Non-blocking receive to confirm the sender was intact.
        let got = reply_rx.try_recv().expect("reply received");
        match got {
            Frame::SimpleString(s) => assert_eq!(s.as_ref(), b"OK"),
            other => panic!("expected SimpleString, got {other:?}"),
        }
    }

    /// Phase 152 Plan 05 Task 1: verify the boxed `FtHybrid` variant keeps the
    /// overall enum within the 512-byte cap.
    #[cfg(feature = "text-index")]
    #[test]
    fn test_ft_hybrid_variant_size_bounded() {
        // Adding the boxed FtHybridPayload must not push ShardMessage past the cap.
        // The boxed pattern keeps the variant at discriminant + pointer ~= 16 bytes.
        let sz = std::mem::size_of::<ShardMessage>();
        assert!(
            sz <= 512,
            "ShardMessage grew past the 512-byte cap after FtHybrid: {sz}",
        );
        // Sanity: the boxed payload itself can be large — only the outer enum
        // needs to stay small.
        let payload_sz = std::mem::size_of::<FtHybridPayload>();
        assert!(
            payload_sz > 0 && payload_sz < 4096,
            "FtHybridPayload size sanity: {payload_sz}",
        );
    }
}
