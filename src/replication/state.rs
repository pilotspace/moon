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
    pub fn ensure_backlogs_allocated(&self) {
        // Hint FIRST: any write racing this allocation that still sees the
        // hint as false advances the offset without a backlog append, and the
        // seed below (reading the offset AFTER that advance) re-aligns. At
        // shards=1 both run on the same shard thread, so there is no race at
        // all. (Multi-shard masters ride the R2 redesign.)
        mark_fanout_active();
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
    /// master offset: `seed_master_offset` (AOF recovery) advances only the
    /// master counter, so the two axes diverge and must never be mixed.
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

    /// Seed `master_repl_offset` to at least `lsn` after AOF recovery.
    ///
    /// Per-shard AOF RFC § 2 Rule 3: after recovery reads the per-shard AOFs,
    /// `master_repl_offset` MUST be at least the max LSN observed across all
    /// shards before the server accepts client traffic. Otherwise the next
    /// write would issue an LSN already present on disk, breaking the
    /// `lsn → entry` uniqueness invariant the backlog merge depends on.
    ///
    /// Uses `fetch_max` so a concurrent in-flight increment (extremely
    /// unlikely at boot, but free to guard against) cannot regress the value.
    /// Per-shard offsets are intentionally NOT touched here — at boot they
    /// are still 0, and seeding shard offsets to the per-shard AOF max would
    /// double-count once the first write advances them via `issue_lsn`.
    pub fn seed_master_offset(&self, lsn: u64) {
        self.master_repl_offset.fetch_max(lsn, Ordering::Relaxed);
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
/// Set by `ensure_backlogs_allocated` (REPLCONF/PSYNC arrival) and never
/// cleared — once a replica has attached, the master keeps feeding the
/// backlog so partial resync stays possible, exactly like Redis.
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
    let mut end_offset = u64::MAX;
    if let Ok(g) = repl_state_arc.read() {
        if let Some(slot) = g.per_shard_backlogs.get(shard_id) {
            if let Some(backlog) = slot.lock().as_mut() {
                backlog.append(&bytes);
            }
        }
        end_offset = g.increment_shard_offset(shard_id, bytes.len() as u64);
    }
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
    let needs_select = repl_state_arc.read().is_ok_and(|g| {
        g.stream_db.get(shard_id).is_some_and(|slot| {
            if slot.load(Ordering::Relaxed) != db as i64 {
                slot.store(db as i64, Ordering::Relaxed);
                true
            } else {
                false
            }
        })
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
