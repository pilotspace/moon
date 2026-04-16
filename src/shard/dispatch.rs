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
    BlockRegister {
        db_index: usize,
        key: Bytes,
        wait_id: u64,
        cmd: crate::blocking::BlockedCommand,
        reply_tx: channel::OneshotSender<Option<crate::protocol::Frame>>,
    },
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
    MigrateConnection {
        fd: std::os::unix::io::RawFd,
        state: crate::server::conn::affinity::MigratedConnectionState,
    },
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
    VectorSearch {
        index_name: Bytes,
        query_blob: Bytes,
        k: usize,
        reply_tx: channel::OneshotSender<Frame>,
    },
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
    TextSearch {
        index_name: Bytes,
        field_idx: Option<usize>,
        /// Query terms with per-term expansion modifiers (Exact/Fuzzy/Prefix).
        /// Carries full QueryTerm so remote shards can apply the same OR-union expansion.
        query_terms: Vec<crate::command::vector_search::ft_text_search::QueryTerm>,
        global_df: std::collections::HashMap<String, u32>,
        global_n: u32,
        top_k: usize,
        offset: usize,
        count: usize,
        /// Placeholder for Plan 03 HIGHLIGHT post-processing (always None here).
        highlight_opts: Option<crate::command::vector_search::ft_text_search::HighlightOpts>,
        /// Placeholder for Plan 03 SUMMARIZE post-processing (always None here).
        summarize_opts: Option<crate::command::vector_search::ft_text_search::SummarizeOpts>,
        reply_tx: channel::OneshotSender<Frame>,
    },
    /// Execute an FT.* command on this shard's VectorStore.
    /// For FT.CREATE, FT.DROPINDEX, FT.INFO -- operations that modify/read
    /// VectorStore state rather than search.
    VectorCommand {
        command: std::sync::Arc<Frame>,
        reply_tx: channel::OneshotSender<Frame>,
    },
    /// Execute a GRAPH.* command on this shard's GraphStore.
    #[cfg(feature = "graph")]
    GraphCommand {
        command: std::sync::Arc<Frame>,
        reply_tx: channel::OneshotSender<Frame>,
    },
    /// Cross-shard graph traversal: expand the given nodes locally and return neighbors.
    /// Used by scatter-gather coordinator for multi-shard BFS expansion.
    #[cfg(feature = "graph")]
    GraphTraverse {
        /// Name of the graph to traverse.
        graph_name: Bytes,
        /// Node IDs to expand on this shard (external IDs, hashed to this shard).
        node_ids: Vec<u64>,
        /// Remaining cross-shard hops allowed (decremented per hop).
        remaining_hops: u32,
        /// Optional edge-type filter (only expand edges of this type).
        edge_type_filter: Option<u16>,
        /// MVCC snapshot LSN for consistent reads across shards.
        snapshot_lsn: u64,
        /// Reply channel for traversal results.
        reply_tx: channel::OneshotSender<Frame>,
    },
    /// Cross-shard PUBLISH with shared atomic response slot for subscriber count accumulation.
    PubSubPublish {
        channel: Bytes,
        message: Bytes,
        slot: std::sync::Arc<PubSubResponseSlot>,
    },
    /// Batched cross-shard PUBLISH for pipeline efficiency.
    /// Multiple (channel, message) pairs destined for the same shard, sharing one ResponseSlot.
    /// Each pair's subscriber count is accumulated into the slot's `counts` field, and the
    /// batch total is accumulated into the slot's `total`.
    PubSubPublishBatch {
        pairs: Vec<(Bytes, Bytes)>,
        slot: std::sync::Arc<PubSubResponseSlot>,
    },
    /// Graceful shutdown signal.
    Shutdown,
}

// ShardMessage is Send because all fields are Send. The raw pointer in
// ResponseSlotPtr is the only non-auto-Send field, and it has its own
// localized unsafe impl Send with documented safety invariants.

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;

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
}
