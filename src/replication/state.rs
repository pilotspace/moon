use bytes::Bytes;
use rand::RngExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub use crate::replication::handshake::ReplicaHandshakeState;

use crate::replication::backlog::SharedBacklog;

/// Default per-shard replication backlog capacity (1 MiB), Redis parity.
pub const DEFAULT_REPL_BACKLOG_SIZE: usize = 1024 * 1024;
/// Smallest accepted backlog capacity. Redis clamps `repl-backlog-size` to
/// 16 KiB; the floor also protects `ServerConfig`'s derived `Default` (0)
/// from allocating an evict-everything backlog.
pub const MIN_REPL_BACKLOG_SIZE: usize = 16 * 1024;

pub struct ReplicationState {
    pub role: ReplicationRole,
    /// Primary replication ID (40-char hex). Survives restarts -- persisted to disk.
    pub repl_id: String,
    /// Secondary replication ID (previous master's ID). Used after failover.
    pub repl_id2: String,
    /// Per-shard write offset (monotonic bytes appended, NEVER resets on WAL truncation).
    /// Length = num_shards. Arc'd so `offset_handle()` can hand shards a
    /// lock-free clone — the per-write advance must never take the
    /// surrounding `RwLock` (QW3, 2026-06 review).
    pub shard_offsets: Arc<[AtomicU64]>,
    /// Sum of all shard offsets -- global master replication offset.
    /// Arc'd for the same lock-free `offset_handle()` distribution.
    pub master_repl_offset: Arc<AtomicU64>,
    /// Connected replicas (master mode). Guarded by Arc<RwLock<ReplicationState>> callers.
    pub replicas: Vec<ReplicaInfo>,
    /// Per-shard replication backlogs, shared between the shard event loop
    /// (writer) and PSYNC handlers (reader). Wrapped in `Mutex<Option<...>>`
    /// so allocation is lazy: stays `None` until the first replica handshake
    /// arrives (REPLCONF). When `None`, write-path append is a single branch
    /// with no lock acquisition.
    pub per_shard_backlogs: Vec<SharedBacklog>,
    /// Lock-free mirror of `role == Replica { .. }` so per-command
    /// `try_enforce_readonly` can avoid taking the surrounding RwLock.
    /// Invariant: written by `set_role()` whenever `role` changes.
    pub is_replica_mirror: Arc<AtomicBool>,
    /// Per-shard backlog capacity in bytes (`--repl-backlog-size`), the single
    /// source of truth read by every allocation site and by INFO. Set once at
    /// startup via [`set_backlog_capacity`](Self::set_backlog_capacity); never
    /// resizes already-allocated backlogs.
    pub backlog_capacity: usize,
    /// Per-shard logical-db context of the replication byte stream (HIGH-2,
    /// task #22): the db of the LAST data command recorded into the shard's
    /// backlog, or `-1` = unknown. `record_local_write_db` prepends a
    /// `SELECT <db>` record whenever the writing connection's db differs, so
    /// a replica's drain binds each command to the master's db. Reset to `-1`
    /// in the SAME synchronous stretch as every FULLRESYNC snapshot capture —
    /// the first post-snapshot write then re-establishes the context for the
    /// freshly-attached replica (Redis's `slaveseldb = -1` idiom).
    pub stream_db: Vec<std::sync::atomic::AtomicI64>,
}

pub enum ReplicationRole {
    Master,
    Replica {
        host: String,
        port: u16,
        state: ReplicaHandshakeState,
    },
}

pub struct ReplicaInfo {
    pub id: u64, // monotonic ID for unregister
    pub addr: std::net::SocketAddr,
    /// Per-shard acknowledged offsets from this replica (updated on REPLCONF ACK).
    pub ack_offsets: Vec<AtomicU64>,
    /// Channels to per-shard sender tasks. shard_txs[shard_id] = Sender for that shard.
    pub shard_txs: Vec<crate::runtime::channel::MpscSender<Bytes>>,
    /// Last ACK time as unix seconds (for lag computation in INFO).
    pub last_ack_time: AtomicU64,
}

impl ReplicationState {
    pub fn new(num_shards: usize, repl_id: String, repl_id2: String) -> Self {
        ReplicationState {
            role: ReplicationRole::Master,
            repl_id,
            repl_id2,
            shard_offsets: (0..num_shards).map(|_| AtomicU64::new(0)).collect(),
            master_repl_offset: Arc::new(AtomicU64::new(0)),
            replicas: Vec::new(),
            per_shard_backlogs: (0..num_shards)
                .map(|_| Arc::new(parking_lot::Mutex::new(None)))
                .collect(),
            is_replica_mirror: Arc::new(AtomicBool::new(false)),
            backlog_capacity: DEFAULT_REPL_BACKLOG_SIZE,
            stream_db: (0..num_shards)
                .map(|_| std::sync::atomic::AtomicI64::new(-1))
                .collect(),
        }
    }

    /// Set the per-shard backlog capacity from config, clamped to
    /// [`MIN_REPL_BACKLOG_SIZE`] (Redis clamps `repl-backlog-size` the same
    /// way). Call before any replica handshake; it does not resize backlogs
    /// that were already allocated.
    pub fn set_backlog_capacity(&mut self, capacity: usize) {
        self.backlog_capacity = capacity.max(MIN_REPL_BACKLOG_SIZE);
    }

    /// Set `role` and update the lock-free `is_replica_mirror` atomically.
    ///
    /// Single owner of the invariant that the mirror tracks `role`. All
    /// production sites that transition between Master and Replica MUST go
    /// through this method; otherwise the mirror drifts and
    /// `try_enforce_readonly` will allow writes against a replica.
    #[inline]
    pub fn set_role(&mut self, new_role: ReplicationRole) {
        let is_replica = matches!(new_role, ReplicationRole::Replica { .. });
        self.role = new_role;
        self.is_replica_mirror.store(is_replica, Ordering::Release);
    }

    /// Allocate the per-shard backlog if not already allocated. Idempotent.
    /// Called when a replica handshake begins (REPLCONF or PSYNC arrival) so
    /// subsequent writes on the shard's event loop start being captured for
    /// partial resync. Capacity comes from `self.backlog_capacity`
    /// (`--repl-backlog-size`, default 1 MiB per shard).
    ///
    /// Task #70: allocation is deliberately split from [`mark_fanout_active`]
    /// — a bare `REPLCONF` (sent by any prober, or the first two steps of a
    /// handshake that never reaches `PSYNC`) used to flip the process-global
    /// `FANOUT_HINT` here, and the hint is never cleared, so a single failed
    /// handshake permanently taxed every future write with the replication
    /// serialize+SPSC round trip. Backlog allocation itself stays load-
    /// bearing on `REPLCONF` (a real handshake's `REPLCONF` arrives before
    /// `PSYNC`, so deferring allocation to `PSYNC` would buffer zero bytes
    /// during the handshake window); only the hint activation moved to
    /// `try_handle_psync` (dispatch.rs), which runs on an actual `PSYNC`
    /// arrival — never on a `REPLCONF`-only probe.
    ///
    /// ⚠ Correctness invariant (task #70 follow-up): allocating here seeds
    /// the backlog at the shard's offset AT ALLOCATION TIME, but the gap
    /// between a bare `REPLCONF` and an actual `PSYNC`/replica registration
    /// can be arbitrarily long (or never close, for a probe). During that
    /// gap `FANOUT_HINT` is false, so local writes take the bare-`issue_lsn`
    /// branch and advance the shard offset with NO backlog append — the
    /// allocated backlog silently skews stale. Backlog byte positions are
    /// therefore only trustworthy from the LAST activation-time
    /// [`Self::realign_backlog`] call onward, never from allocation alone.
    /// Every activation site (`try_handle_psync`, and the per-shard
    /// `RegisterReplica`/`PrepareReplicaSync` arms in `spsc_handler.rs`)
    /// MUST call `realign_backlog` before capturing any snapshot/cut offset
    /// from this backlog.
    pub fn ensure_backlogs_allocated(&self) {
        for (shard_id, slot) in self.per_shard_backlogs.iter().enumerate() {
            let mut guard = slot.lock();
            if guard.is_none() {
                // Seed byte positions at the CURRENT shard offset — see
                // `ReplicationBacklog::new_at`.
                let offset = self
                    .shard_offsets
                    .get(shard_id)
                    .map(|o| o.load(Ordering::Relaxed))
                    .unwrap_or(0);
                *guard = Some(crate::replication::backlog::ReplicationBacklog::new_at(
                    self.backlog_capacity,
                    offset,
                ));
            }
        }
    }

    /// Realign an already-allocated backlog for `shard_id` to the shard's
    /// CURRENT real offset, discarding any bytes/positions that skewed away
    /// from the offset counter during the REPLCONF-allocated /
    /// FANOUT_HINT-false window (see [`Self::ensure_backlogs_allocated`]).
    /// No-op if the shard has no backlog allocated yet, or if the backlog
    /// is already aligned (`ReplicationBacklog::realign_to` is itself a
    /// no-op in that case).
    ///
    /// MUST be called at every activation site — an actual PSYNC arrival
    /// (`try_handle_psync`) or a shard's own `RegisterReplica` /
    /// `PrepareReplicaSync` arm (`spsc_handler.rs`) — BEFORE that call site
    /// captures any snapshot/cut/push offset from this backlog. A skewed
    /// buffer's contents are already useless (they cover positions the real
    /// counter never confirmed), so resetting is correct: any partial-resync
    /// request for an offset below the new `start_offset` falls through
    /// `bytes_from` returning `None`, and the caller's existing fail-safe —
    /// full resync — kicks in. Never silently serves wrong bytes.
    ///
    /// Race-free ONLY when called from the shard thread that owns
    /// `shard_id`'s own offset advances — true for every call site above
    /// (the inline single-shard PSYNC leg at shards=1 runs on that shard's
    /// event-loop thread; the SPSC arms run there by construction), so the
    /// offset read below and the realign happen back-to-back with nothing
    /// else able to interleave on that shard's own append/advance sequence.
    pub fn realign_backlog(&self, shard_id: usize) {
        let Some(slot) = self.per_shard_backlogs.get(shard_id) else {
            return;
        };
        let mut guard = slot.lock();
        if let Some(backlog) = guard.as_mut() {
            let offset = self
                .shard_offsets
                .get(shard_id)
                .map(|o| o.load(Ordering::Relaxed))
                .unwrap_or(0);
            backlog.realign_to(offset);
        }
    }

    /// Total resident bytes across all per-shard replication backlogs.
    /// Returns 0 if no backlogs have been allocated (lazy init).
    /// O(num_shards) -- one lock acquire per shard (uncontended on metrics scrape).
    pub fn backlog_resident_bytes(&self) -> usize {
        let mut total: usize = 0;
        for slot in &self.per_shard_backlogs {
            let guard = slot.lock();
            if let Some(ref backlog) = *guard {
                total += backlog.resident_bytes();
            }
        }
        total
    }

    /// Increment the offset for the given shard by delta bytes.
    /// Also adds delta to master_repl_offset.
    ///
    /// Returns the PER-SHARD offset after the advance — the record's
    /// `end_offset` on the live-fanout wire, compared against each replica's
    /// per-shard snapshot cut (`ReplicaFanout::cut`). Deliberately NOT the
    /// master offset: on a multi-shard (`num_shards > 1`) master,
    /// `seed_master_offset` (AOF recovery) only puts its whole seed on shard
    /// 0 (task #67) — shards 1..N-1 boot at 0 while `total_offset()` starts
    /// at the recovered lsn, so those shards' axes still diverge from the
    /// master axis after a restart and must never be mixed with it.
    pub fn increment_shard_offset(&self, shard_id: usize, delta: u64) -> u64 {
        if shard_id >= self.shard_offsets.len() {
            return 0;
        }
        let prev = self.shard_offsets[shard_id].fetch_add(delta, Ordering::Relaxed);
        self.master_repl_offset.fetch_add(delta, Ordering::Relaxed);
        prev + delta
    }

    /// Atomically issue an LSN for a write and advance per-shard +
    /// master replication offsets by `delta`.
    ///
    /// Returns the LSN that uniquely identifies this write — equal to the
    /// value of `master_repl_offset` BEFORE the increment, mirroring Redis's
    /// `+ delta - delta` semantics. The same LSN MUST tag the corresponding
    /// `AofMessage::Append` entry and the replication backlog entry for that
    /// write so per-shard AOF replay can rebuild a globally consistent log
    /// (per-shard AOF RFC § 2 Rule 3).
    ///
    /// Atomicity caveat: the per-shard offset advance and the master offset
    /// advance are TWO separate `fetch_add`s, not one composite op. Concurrent
    /// callers across shards observe a brief window where the master sum
    /// disagrees with the sum of shard offsets. Acceptable today because the
    /// only `total_offset()` consumer is INFO replication, which tolerates
    /// transient skew. Do not promote to a hard invariant without redesign.
    ///
    /// Returns 0 if `shard_id` is out of range (defensive; production callers
    /// must pass a valid id).
    pub fn issue_lsn(&self, shard_id: usize, delta: u64) -> u64 {
        if shard_id >= self.shard_offsets.len() {
            return 0;
        }
        self.shard_offsets[shard_id].fetch_add(delta, Ordering::Relaxed);
        self.master_repl_offset.fetch_add(delta, Ordering::Relaxed)
    }

    /// Returns sum of all per-shard offsets.
    pub fn total_offset(&self) -> u64 {
        self.master_repl_offset.load(Ordering::Relaxed)
    }

    /// Seed `master_repl_offset` to at least `lsn` after AOF recovery, and
    /// seed shard 0's per-shard offset to the same value so the
    /// `Σ shard_offsets == total_offset()` invariant — the one every write
    /// maintains going forward via `increment_shard_offset`/`issue_lsn`,
    /// which bump both axes by the same delta in lockstep — already holds
    /// the instant recovery finishes, instead of only after enough live
    /// traffic has flowed to close the gap (never, if the master idles).
    ///
    /// Per-shard AOF RFC § 2 Rule 3: after recovery reads the per-shard AOFs,
    /// `master_repl_offset` MUST be at least the max LSN observed across all
    /// shards before the server accepts client traffic. Otherwise the next
    /// write would issue an LSN already present on disk, breaking the
    /// `lsn → entry` uniqueness invariant the backlog merge depends on.
    ///
    /// task #67 (P0, v0.7.0 soak): a restarted PerShard-AOF master
    /// (`--shards >= 2`) that skipped the shard-axis seed left
    /// `shard_offsets` at the fresh-boot 0 while `master_repl_offset` jumped
    /// straight to the recovered `lsn`. The multi-shard PSYNC full-resync
    /// handshake (`handle_psync_inline_multi_shard`) advertises
    /// `Σ shard_offset(i)` — not `total_offset()` — as the reconnecting
    /// replica's new baseline, since each shard's PrepareReplicaSync leg
    /// captures its own offset atomically with its RDB body (the exactly-once
    /// invariant `ReplicaFanout::cut` depends on). A replica adopting a
    /// near-zero baseline can never ACK up to `total_offset()`'s old-history
    /// value: `wait_for_replicas` compares against `total_offset()` and
    /// blocks forever, even though the data stream itself is correct and
    /// live (reproduced 2× by `scripts/soak-replication-24h.sh`).
    ///
    /// The exact per-shard SPLIT of the seed does not affect correctness:
    /// each shard's offset is compared only against ITSELF — the live-fanout
    /// `cut` gate and the lazily-allocated backlog are both scoped to one
    /// shard and never cross-reference another shard's counter. Concentrating
    /// the whole seed on shard 0 is therefore sufficient. (Splitting it
    /// proportionally by each shard's own historical byte count is not
    /// possible from the data AOF replay retains — `replay_per_shard` tracks
    /// each shard's max GLOBAL lsn *tag*, not its cumulative local byte
    /// length, and seeding N shards each to a value near the shared global
    /// max would sum to roughly `N × lsn` — a real double-count, which is
    /// what the original single-axis version of this function's doc avoided
    /// by not touching `shard_offsets` at all.)
    ///
    /// Uses `fetch_max` on both counters so a concurrent in-flight increment
    /// (extremely unlikely at boot, but free to guard against) cannot
    /// regress either value.
    pub fn seed_master_offset(&self, lsn: u64) {
        self.master_repl_offset.fetch_max(lsn, Ordering::Relaxed);
        if let Some(shard0) = self.shard_offsets.first() {
            shard0.fetch_max(lsn, Ordering::Relaxed);
        }
    }

    /// Returns the per-shard offset for a specific shard.
    pub fn shard_offset(&self, shard_id: usize) -> u64 {
        self.shard_offsets
            .get(shard_id)
            .map(|o| o.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Number of shards this state tracks (`shard_offsets.len()`) — the
    /// SAME value `ConnectionContext::num_shards` is initialized from, so a
    /// ctx-free caller (e.g. `shard::mq_exec::replicate_mq_record`) can
    /// evaluate the identical "single-shard only" gate the connection layer
    /// uses (`ctx.num_shards == 1`) without a `ConnectionContext` in scope.
    #[inline]
    pub fn num_shards(&self) -> usize {
        self.shard_offsets.len()
    }

    /// Clone out a lock-free handle to the offset atomics.
    ///
    /// Called once per shard at event-loop startup; the handle is what the
    /// per-write path uses, so `RwLock<ReplicationState>` is never read-locked
    /// per write (QW3, 2026-06 review finding 1.4).
    pub fn offset_handle(&self) -> OffsetHandle {
        OffsetHandle {
            shard_offsets: Arc::clone(&self.shard_offsets),
            master_repl_offset: Arc::clone(&self.master_repl_offset),
        }
    }
}

/// Lock-free handle to the replication offset atomics, distributed to each
/// shard at startup via [`ReplicationState::offset_handle`]. Advancing
/// offsets through this handle is equivalent to `ReplicationState::issue_lsn`
/// — both operate on the same `Arc`'d atomics.
#[derive(Clone)]
pub struct OffsetHandle {
    shard_offsets: Arc<[AtomicU64]>,
    master_repl_offset: Arc<AtomicU64>,
}

impl OffsetHandle {
    /// See [`ReplicationState::issue_lsn`] — same semantics, same atomics,
    /// no surrounding lock.
    #[inline]
    pub fn issue_lsn(&self, shard_id: usize, delta: u64) -> u64 {
        if shard_id >= self.shard_offsets.len() {
            return 0;
        }
        self.shard_offsets[shard_id].fetch_add(delta, Ordering::Relaxed);
        self.master_repl_offset.fetch_add(delta, Ordering::Relaxed)
    }

    /// See [`ReplicationState::increment_shard_offset`] — returns the
    /// per-shard offset after the advance (the record's fan-out `end_offset`).
    #[inline]
    pub fn increment_shard_offset(&self, shard_id: usize, delta: u64) -> u64 {
        if shard_id >= self.shard_offsets.len() {
            return 0;
        }
        let prev = self.shard_offsets[shard_id].fetch_add(delta, Ordering::Relaxed);
        self.master_repl_offset.fetch_add(delta, Ordering::Relaxed);
        prev + delta
    }

    /// Current offset of one shard. Used by the `RegisterReplica` reply to
    /// tell the PSYNC task exactly where live fan-out begins, so its backlog
    /// catch-up read covers `[snapshot_offset, this)` with no gap and no
    /// overlap.
    #[inline]
    pub fn shard_offset(&self, shard_id: usize) -> u64 {
        self.shard_offsets
            .get(shard_id)
            .map(|o| o.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Number of shards this handle tracks. R2 (task #20): `> 1` switches the
    /// replica-stream serialization to per-record `SELECT` framing — N shard
    /// threads feed ONE replica wire, so a shared "current db" context cannot
    /// exist and every db-scoped record must carry its own.
    #[inline]
    pub fn num_shards(&self) -> usize {
        self.shard_offsets.len()
    }
}

const ZEROED_ID: &str = "0000000000000000000000000000000000000000";

/// Generate a Redis-compatible 40-char hex replication ID.
pub fn generate_repl_id() -> String {
    let mut rng = rand::rng();
    (0..20)
        .map(|_| format!("{:02x}", rng.random::<u8>()))
        .collect()
}

/// Atomically persist replication IDs to {dir}/replication.state.
/// Uses .tmp + rename pattern (same as WalWriter::truncate_after_snapshot).
pub fn save_replication_state(
    dir: &std::path::Path,
    repl_id: &str,
    repl_id2: &str,
) -> std::io::Result<()> {
    let dst = dir.join("replication.state");
    // Atomic via `atomic_write_durable` (task #49): temp + fsync + rename
    // + dir-fsync. The prior code (comment claimed "same pattern as
    // WalWriter::truncate_after_snapshot") only did write+rename with no
    // fsync -- a kill-9 right after `rename()` returned could still revert
    // replication.state on ext4/xfs, losing the master replication ID
    // across a restart.
    crate::persistence::atomic::atomic_write_durable(
        &dst,
        format!("{}\n{}\n", repl_id, repl_id2).as_bytes(),
    )?;
    Ok(())
}

/// Process-global "a replica has (ever) attached" hint.
///
/// Write hot paths gate their replication-fanout serialization on this ONE
/// Relaxed load instead of taking `repl_state.read()` per command (the same
/// S3.5a rationale that gave READONLY its `is_replica_mirror` AtomicBool).
///
/// Set by an actual `PSYNC` arrival (`try_handle_psync` in
/// `handler_monoio::dispatch`) or multi-shard replica registration — NOT by
/// a bare `REPLCONF` (task #70: `REPLCONF` only allocates the backlog via
/// [`ReplicationState::ensure_backlogs_allocated`], which does not touch this
/// hint, so a prober or a handshake that never reaches `PSYNC` cannot flip it
/// on). Never cleared once set — once a replica has genuinely begun
/// attaching, the master keeps feeding the backlog so partial resync stays
/// possible, exactly like Redis.
static FANOUT_HINT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Mark the fanout hint (idempotent). See [`FANOUT_HINT`].
#[inline]
pub fn mark_fanout_active() {
    FANOUT_HINT.store(true, Ordering::Relaxed);
}

/// Cheap hot-path predicate: has any replica ever begun attaching?
/// False ⇒ skip replication serialization/fan-out entirely.
#[inline]
pub fn fanout_hint_active() -> bool {
    FANOUT_HINT.load(Ordering::Relaxed)
}

/// Ctx-free twin of the connection layer's `record_local_write`
/// (`server::conn::handler_monoio::ft::record_local_write`), for owner-shard
/// execution paths that run on a shard's own OS thread WITHOUT a
/// `ConnectionContext` in scope — today only `shard::mq_exec` (MQ.* effect-
/// record replication, Wave B stage 2b).
///
/// Resolves the SAME per-process `ReplicationState` `Arc` the connection
/// layer holds, via `admin::metrics_setup::get_global_repl_state_arc()` —
/// both are clones of the one `Arc` built at boot (`main.rs` / `listener.rs`
/// / `embedded.rs` all call `set_global_repl_state(repl_state.clone())` right
/// after constructing it), so backlog append + offset advance land in the
/// identical per-shard slot the connection-layer path would have used.
/// No-op if replication was never initialized (embedded/library callers with
/// no listener boot path).
///
/// Caller MUST be running on shard `shard_id`'s own OS thread — this pushes
/// to `shard::self_msg`, which is a `thread_local!` queue (see that module's
/// docs: "Tokio tasks must NOT push here").
pub fn record_local_write_global(shard_id: usize, bytes: Bytes) {
    let Some(repl_state_arc) = crate::admin::metrics_setup::get_global_repl_state_arc() else {
        return;
    };
    let g = repl_state_arc.read();
    if let Some(slot) = g.per_shard_backlogs.get(shard_id) {
        if let Some(backlog) = slot.lock().as_mut() {
            backlog.append(&bytes);
        }
    }
    let end_offset = g.increment_shard_offset(shard_id, bytes.len() as u64);
    drop(g);
    crate::shard::self_msg::push(crate::shard::dispatch::ShardMessage::ReplicaLiveFanout {
        bytes,
        end_offset,
    });
}

/// Db-aware ctx-free twin of `record_local_write_db`, for the same
/// ctx-free callers as [`record_local_write_global`]. Implements only the
/// single-shard "prepend `SELECT` on db change" branch — ctx-free MQ
/// replication is gated `num_shards == 1` by its caller (graph precedent:
/// live streaming is single-shard-only), so the N-shard fused-SELECT branch
/// (which needs `ctx.num_shards`) is never reached and intentionally not
/// implemented here.
pub fn record_local_write_db_global(shard_id: usize, db: usize, bytes: Bytes) {
    let Some(repl_state_arc) = crate::admin::metrics_setup::get_global_repl_state_arc() else {
        return;
    };
    let needs_select = repl_state_arc
        .read()
        .stream_db
        .get(shard_id)
        .is_some_and(|slot| {
            if slot.load(Ordering::Relaxed) != db as i64 {
                slot.store(db as i64, Ordering::Relaxed);
                true
            } else {
                false
            }
        });
    if needs_select {
        record_local_write_global(
            shard_id,
            crate::persistence::aof::serialize_select_record(db),
        );
    }
    record_local_write_global(shard_id, bytes);
}

/// Load replication IDs from {dir}/replication.state.
/// If file missing or malformed, generates new IDs and saves them.
pub fn load_replication_state(dir: &std::path::Path) -> (String, String) {
    let path = dir.join("replication.state");
    if let Ok(content) = std::fs::read_to_string(&path) {
        let mut lines = content.lines();
        let id1 = lines.next().unwrap_or("").to_string();
        let id2 = lines.next().unwrap_or(ZEROED_ID).to_string();
        if id1.len() == 40 {
            let id2 = if id2.len() == 40 {
                id2
            } else {
                ZEROED_ID.to_string()
            };
            return (id1, id2);
        }
    }
    let id1 = generate_repl_id();
    let id2 = ZEROED_ID.to_string();
    let _ = save_replication_state(dir, &id1, &id2);
    (id1, id2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_repl_id_length_and_hex() {
        let id = generate_repl_id();
        assert_eq!(id.len(), 40, "repl_id must be exactly 40 chars");
        assert!(
            id.chars().all(|c| c.is_ascii_hexdigit()),
            "repl_id must be hex"
        );
    }

    #[test]
    fn test_generate_repl_id_uniqueness() {
        let a = generate_repl_id();
        let b = generate_repl_id();
        assert_ne!(a, b, "two generated IDs should differ");
    }

    #[test]
    fn test_replication_state_new_shard_offsets() {
        let state = ReplicationState::new(4, generate_repl_id(), ZEROED_ID.to_string());
        assert_eq!(state.shard_offsets.len(), 4);
        for i in 0..4 {
            assert_eq!(state.shard_offset(i), 0);
        }
    }

    #[test]
    fn test_increment_shard_offset() {
        let state = ReplicationState::new(2, generate_repl_id(), ZEROED_ID.to_string());
        state.increment_shard_offset(0, 100);
        state.increment_shard_offset(1, 200);
        assert_eq!(state.shard_offset(0), 100);
        assert_eq!(state.shard_offset(1), 200);
    }

    #[test]
    fn test_total_offset() {
        let state = ReplicationState::new(3, generate_repl_id(), ZEROED_ID.to_string());
        state.increment_shard_offset(0, 50);
        state.increment_shard_offset(1, 75);
        state.increment_shard_offset(2, 25);
        assert_eq!(state.total_offset(), 150);
    }

    /// task #67: `seed_master_offset` (PerShard AOF recovery) must seed shard
    /// 0's per-shard offset alongside `master_repl_offset`, so
    /// `Σ shard_offsets == total_offset()` holds immediately after boot —
    /// not just after enough live writes have flowed to close the gap.
    /// `handle_psync_inline_multi_shard` advertises `Σ shard_offset(i)` (not
    /// `total_offset()`) as a reconnecting replica's new baseline; leaving
    /// shard_offsets at 0 while total_offset jumps to the recovered lsn wedges
    /// `WAIT` forever on a restarted multi-shard master (reproduced by
    /// `tests/replication_hardening.rs::master_kill_restart_wait_acks`).
    #[test]
    fn test_seed_master_offset_seeds_shard_zero_to_match_total() {
        let state = ReplicationState::new(4, generate_repl_id(), ZEROED_ID.to_string());
        state.seed_master_offset(5_000);

        assert_eq!(state.total_offset(), 5_000);
        let sum: u64 = (0..state.num_shards()).map(|i| state.shard_offset(i)).sum();
        assert_eq!(
            sum,
            state.total_offset(),
            "sum of per-shard offsets must equal total_offset() right after seeding"
        );
        assert_eq!(state.shard_offset(0), 5_000, "seed lands on shard 0");
        for i in 1..state.num_shards() {
            assert_eq!(state.shard_offset(i), 0, "non-zero shards stay at 0");
        }

        // The invariant must survive subsequent live writes on any shard —
        // increment_shard_offset bumps both axes by the same delta, so the
        // sum-equals-total property is preserved, not just true at t=0.
        state.increment_shard_offset(1, 42);
        state.increment_shard_offset(0, 8);
        let sum_after: u64 = (0..state.num_shards()).map(|i| state.shard_offset(i)).sum();
        assert_eq!(sum_after, state.total_offset());
        assert_eq!(state.total_offset(), 5_050);
    }

    /// `fetch_max` semantics: a concurrent in-flight increment before the
    /// seed call must not be clobbered backwards on either axis.
    #[test]
    fn test_seed_master_offset_never_regresses() {
        let state = ReplicationState::new(2, generate_repl_id(), ZEROED_ID.to_string());
        state.increment_shard_offset(0, 9_000);
        assert_eq!(state.total_offset(), 9_000);

        // A seed lower than the current live offset (e.g. a stale/short AOF
        // max-lsn) must not regress either counter.
        state.seed_master_offset(100);
        assert_eq!(state.total_offset(), 9_000);
        assert_eq!(state.shard_offset(0), 9_000);
    }

    #[test]
    fn test_save_load_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let id1 = generate_repl_id();
        let id2 = generate_repl_id();
        save_replication_state(dir.path(), &id1, &id2).unwrap();
        let (loaded1, loaded2) = load_replication_state(dir.path());
        assert_eq!(loaded1, id1);
        assert_eq!(loaded2, id2);
    }

    /// Task #49: `save_replication_state` must go through
    /// `atomic_write_durable`, not a bare write+rename. Regression pin: no
    /// leftover `replication.state.tmp` after a successful save.
    #[test]
    fn test_save_replication_state_leaves_no_leftover_temp_file() {
        let dir = tempfile::tempdir().unwrap();
        let id1 = generate_repl_id();
        let id2 = generate_repl_id();
        save_replication_state(dir.path(), &id1, &id2).unwrap();

        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(entries, vec![std::ffi::OsString::from("replication.state")]);
    }

    #[test]
    fn test_load_generates_fresh_when_missing() {
        let dir = tempfile::tempdir().unwrap();
        let (id1, id2) = load_replication_state(dir.path());
        assert_eq!(id1.len(), 40);
        assert_eq!(id2, ZEROED_ID);
        // Should have been persisted
        let (id1b, id2b) = load_replication_state(dir.path());
        assert_eq!(id1b, id1, "reloading should return same ID");
        assert_eq!(id2b, id2);
    }

    #[test]
    fn test_set_role_updates_is_replica_mirror() {
        // S3.5a: mirror must track role transitions so try_enforce_readonly
        // can read it lock-free on the hot path.
        let mut state = ReplicationState::new(2, generate_repl_id(), ZEROED_ID.to_string());
        let mirror = state.is_replica_mirror.clone();

        // Fresh state starts as Master => mirror false.
        assert!(!mirror.load(Ordering::Acquire));

        // Promote to replica => mirror flips true.
        state.set_role(ReplicationRole::Replica {
            host: "127.0.0.1".to_string(),
            port: 6379,
            state: ReplicaHandshakeState::PingPending,
        });
        assert!(mirror.load(Ordering::Acquire));
        assert!(matches!(state.role, ReplicationRole::Replica { .. }));

        // REPLICAOF NO ONE: back to master => mirror flips false.
        state.set_role(ReplicationRole::Master);
        assert!(!mirror.load(Ordering::Acquire));
        assert!(matches!(state.role, ReplicationRole::Master));
    }

    #[test]
    fn test_backlog_capacity_default_and_clamp() {
        let mut state = ReplicationState::new(1, generate_repl_id(), ZEROED_ID.to_string());
        assert_eq!(state.backlog_capacity, DEFAULT_REPL_BACKLOG_SIZE);

        // Configured capacity is honored…
        state.set_backlog_capacity(64 * 1024);
        assert_eq!(state.backlog_capacity, 64 * 1024);

        // …but clamped to the Redis floor (also guards derived-Default 0).
        state.set_backlog_capacity(1024);
        assert_eq!(state.backlog_capacity, MIN_REPL_BACKLOG_SIZE);
        state.set_backlog_capacity(0);
        assert_eq!(state.backlog_capacity, MIN_REPL_BACKLOG_SIZE);
    }

    #[test]
    fn test_ensure_backlogs_use_configured_capacity() {
        let mut state = ReplicationState::new(2, generate_repl_id(), ZEROED_ID.to_string());
        state.set_backlog_capacity(MIN_REPL_BACKLOG_SIZE);
        state.ensure_backlogs_allocated();
        // Overflow one backlog past the configured capacity: eviction must
        // kick in exactly at the configured size, proving the capacity plumb.
        let slot = &state.per_shard_backlogs[0];
        let mut guard = slot.lock();
        let backlog = guard.as_mut().expect("allocated");
        let data = vec![0xAAu8; MIN_REPL_BACKLOG_SIZE + 100];
        backlog.append(&data);
        assert_eq!(backlog.start_offset(), 100, "evicts past configured cap");
        assert_eq!(backlog.end_offset(), (MIN_REPL_BACKLOG_SIZE + 100) as u64);
    }

    /// Task #70 RED/GREEN: `ensure_backlogs_allocated` — the side effect of a
    /// bare REPLCONF (`try_handle_replconf`, dispatch.rs) — must NOT flip the
    /// sticky, process-global `FANOUT_HINT`. Only an actual PSYNC arrival
    /// (`try_handle_psync`) or replica registration (`RegisterReplica`/
    /// `PrepareReplicaSync` in `spsc_handler.rs`) may.
    ///
    /// RED on the pre-fix code: the old `ensure_backlogs_allocated` called
    /// `mark_fanout_active()` unconditionally as its first line, so this
    /// delta assertion (`before == after`) fails — `after` flips to `true`
    /// regardless of `before` — proving a probe/failed handshake that only
    /// ever sends REPLCONF (never PSYNC) permanently taxed every future
    /// write. GREEN on the fix: `ensure_backlogs_allocated` only allocates
    /// the backlog (still load-bearing, asserted below), the hint is
    /// untouched.
    ///
    /// `FANOUT_HINT` is a process-global `static` shared by every test in
    /// this binary; the delta form (`before == after`) is deliberately
    /// robust to whatever the ambient value already is — no test in this
    /// codebase's unit-test-reachable surface calls `mark_fanout_active`
    /// outside a real `ConnectionContext`/`RegisterReplica` path, so `before`
    /// is `false` in practice, but the assertion does not depend on that.
    #[test]
    fn test_ensure_backlogs_allocated_does_not_activate_fanout_hint() {
        let before = fanout_hint_active();
        let state = ReplicationState::new(2, generate_repl_id(), ZEROED_ID.to_string());
        state.ensure_backlogs_allocated();
        let after = fanout_hint_active();
        assert_eq!(
            before, after,
            "ensure_backlogs_allocated (bare-REPLCONF path) must not change \
             the sticky process-global fanout hint — task #70"
        );
        // Allocation itself is unchanged, still load-bearing: writes between
        // REPLCONF and PSYNC are captured once PSYNC actually flips the hint.
        assert!(
            state.per_shard_backlogs[0].lock().is_some(),
            "backlog allocation must still happen on REPLCONF"
        );
        assert!(state.per_shard_backlogs[1].lock().is_some());
    }

    /// Task #70 companion: the actual activation path (`mark_fanout_active`,
    /// called from `try_handle_psync` on a genuine PSYNC arrival and from the
    /// `RegisterReplica`/`PrepareReplicaSync` shard-message arms) still sets
    /// the hint — monotonic, so this holds regardless of the ambient value.
    #[test]
    fn test_mark_fanout_active_sets_hint() {
        mark_fanout_active();
        assert!(
            fanout_hint_active(),
            "mark_fanout_active (actual PSYNC/replica registration) must set the hint"
        );
    }

    /// Task #70 correctness follow-up (orchestrator-caught regression):
    /// `ensure_backlogs_allocated` (bare REPLCONF) seeds the backlog at the
    /// shard's offset AT THAT INSTANT. If the hint stays false afterward
    /// (no PSYNC yet — exactly the window this task's fix widened), local
    /// writes advance the shard offset via the bare `issue_lsn` path with NO
    /// backlog append, so the backlog silently skews stale relative to the
    /// real offset counter. This test proves the skew is real (asserting it
    /// BEFORE calling the fix), then proves `realign_backlog` — called at
    /// every activation site — repairs it.
    ///
    /// RED without the fix: if a caller captured a snapshot/cut offset from
    /// the backlog at this point (skipping `realign_backlog`), it would read
    /// `backlog.end_offset() == 0` while the shard's real offset is `300` —
    /// a 300-byte skew. `bytes_from(300)` against the unrealigned backlog
    /// returns `None` (300 is treated as a future offset past a backlog
    /// whose `end_offset` is still 0), which silently drops partial-resync
    /// catch-up instead of correctly triggering a full resync at the RIGHT
    /// offset — the corruption the orchestrator flagged.
    /// GREEN with the fix: after `realign_backlog`, `end_offset` matches the
    /// real offset exactly and `bytes_from` at that offset returns the
    /// (empty) live tail, not `None`.
    #[test]
    fn test_realign_backlog_fixes_skew_after_hint_false_window() {
        let state = ReplicationState::new(1, generate_repl_id(), ZEROED_ID.to_string());

        // 1. Bare REPLCONF: allocates + seeds the backlog at offset 0.
        state.ensure_backlogs_allocated();
        assert_eq!(
            state.per_shard_backlogs[0]
                .lock()
                .as_ref()
                .unwrap()
                .end_offset(),
            0
        );

        // 2. The FANOUT_HINT-false window: local writes advance the shard's
        // real offset via the bare-LSN path (no backlog append — this is
        // exactly what `record_local_write` skips while
        // `replication_fanout_active` is false).
        state.issue_lsn(0, 100);
        state.issue_lsn(0, 150);
        state.issue_lsn(0, 50);
        let real_offset = state.shard_offset(0);
        assert_eq!(real_offset, 300);

        // Prove the skew actually exists before the fix runs: the backlog
        // is still stuck at its allocation-time offset while the real
        // counter has moved on.
        let skewed_end = state.per_shard_backlogs[0]
            .lock()
            .as_ref()
            .unwrap()
            .end_offset();
        assert_eq!(skewed_end, 0, "backlog must still be stale pre-realign");
        assert_ne!(
            skewed_end, real_offset,
            "skew must exist: this is the bug the orchestrator caught"
        );
        // A caller that (incorrectly) trusted the skewed backlog now would
        // silently drop catch-up instead of full-resyncing: bytes_from at
        // the real offset returns None because the backlog doesn't know
        // any offset that high exists yet.
        assert_eq!(
            state.per_shard_backlogs[0]
                .lock()
                .as_ref()
                .unwrap()
                .bytes_from(real_offset),
            None,
            "unrealigned backlog treats the real offset as an unreachable future position"
        );

        // 3. Activation (PSYNC arrival / RegisterReplica arm): realign BEFORE
        // any snapshot/cut offset is captured.
        state.realign_backlog(0);

        let realigned_end = state.per_shard_backlogs[0]
            .lock()
            .as_ref()
            .unwrap()
            .end_offset();
        assert_eq!(
            realigned_end, real_offset,
            "realign_backlog must bring the backlog's end_offset back in sync \
             with the shard's real offset counter — task #70 correctness fix"
        );
        assert_eq!(
            state.per_shard_backlogs[0]
                .lock()
                .as_ref()
                .unwrap()
                .bytes_from(real_offset),
            Some(vec![]),
            "post-realign, the real offset is a valid (empty) live-tail position"
        );
    }

    /// Integration-shaped companion: after realign, a record appended via
    /// the normal write path lands at exactly the pre-append real offset —
    /// end-to-end proof that catch-up reads from the correct byte range
    /// once activation has realigned the backlog.
    #[test]
    fn test_realign_backlog_then_append_reads_back_exact_record() {
        let state = ReplicationState::new(1, generate_repl_id(), ZEROED_ID.to_string());
        state.ensure_backlogs_allocated();
        state.issue_lsn(0, 300); // hint-false window skew, as above.
        state.realign_backlog(0); // activation.

        let pre_append_offset = state.shard_offset(0);
        assert_eq!(pre_append_offset, 300);

        // Simulate the write path's post-activation record append
        // (`record_local_write`'s backlog.append + increment_shard_offset
        // pairing) directly against the now-realigned backlog.
        let record = b"*1\r\n$4\r\nPING\r\n";
        {
            let mut guard = state.per_shard_backlogs[0].lock();
            guard.as_mut().unwrap().append(record);
        }
        state.increment_shard_offset(0, record.len() as u64);

        let bytes = state.per_shard_backlogs[0]
            .lock()
            .as_ref()
            .unwrap()
            .bytes_from(pre_append_offset)
            .expect("pre_append_offset must still be live after realign+append");
        assert_eq!(
            bytes, record,
            "catch-up from the pre-append offset must return exactly that record's bytes"
        );
    }

    #[test]
    fn test_is_replica_mirror_default_false() {
        let state = ReplicationState::new(1, generate_repl_id(), ZEROED_ID.to_string());
        assert!(!state.is_replica_mirror.load(Ordering::Acquire));
    }

    #[test]
    fn test_load_zeroed_id2_when_only_id1_present() {
        let dir = tempfile::tempdir().unwrap();
        let id1 = generate_repl_id();
        // Write only id1 (no id2 line)
        std::fs::write(dir.path().join("replication.state"), format!("{}\n", id1)).unwrap();
        let (loaded1, loaded2) = load_replication_state(dir.path());
        assert_eq!(loaded1, id1);
        assert_eq!(loaded2, ZEROED_ID);
    }
}
