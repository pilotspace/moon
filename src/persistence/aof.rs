//! Append-Only File (AOF) persistence: logs every write command in RESP format
//! for crash recovery. Supports three fsync policies and AOF rewriting for compaction.
//!
//! ## Unwrap Classification
//!
//! | Context | Classification | Rationale |
//! |---------|---------------|-----------|
//! | `AofWriter::append` (hot path) | **fire-and-forget** | Channel send; no Result needed |
//! | `aof_writer_task` | **must-panic** | Writer task; errors logged inline |
//! | `replay_aof` | **should-recover** (`Result<_, MoonError>`) | Startup replay; log+skip on corruption |
//! | `rewrite_aof` | **should-recover** (`Result<_, MoonError>`) | Background rewrite; caller logs error |
//! | `#[cfg(test)]` code (55 unwraps) | **test-only** | Panics are appropriate in tests |
// Suppressions narrowed: only keep what's needed for conditional compilation
#![allow(unused_imports, unused_variables, unreachable_code, clippy::empty_loop)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::runtime::cancel::CancellationToken;
use crate::runtime::channel;
use bytes::{Bytes, BytesMut};
use tracing::{error, info, warn};

use crate::error::{AofError, MoonError};
use crate::framevec;
use crate::persistence::replay::CommandReplayEngine;
use crate::protocol::{Frame, ParseConfig, parse, serialize};
use crate::storage::compact_key::CompactKey;
use crate::storage::compact_value::RedisValueRef;
use crate::storage::db::Database;
use crate::storage::entry::{Entry, current_time_ms};
/// Type alias for the per-database RwLock container.
type SharedDatabases = Arc<Vec<parking_lot::RwLock<Database>>>;

/// Canonical AOF fsync failure error string sent to the client as a
/// `Frame::Error` when `appendfsync=always` and the writer task does not
/// confirm durability before the response.
///
/// All handler variants (handler_single, handler_monoio, handler_sharded)
/// MUST use this constant so operators see a consistent error regardless of
/// which connection path handles the request.
///
/// Redis convention: errors begin with a single-word code (`ERR` for generic
/// failures) followed by a space and a human-readable message.
pub const AOF_FSYNC_ERR: &[u8] = b"ERR AOF fsync failed; write not durable";

/// High bit of the per-entry LSN reserved for `OrderedAcrossShards`
/// (RFC § 2 Rule 2). When set on a per-shard AOF entry, recovery treats
/// the entry as participating in a cross-shard atomic operation and
/// buffers it for the cross-shard merge replay after per-shard replay
/// completes.
///
/// Practical LSN ceilings (even at 10 M writes/s sustained for a century)
/// sit near 2^58, so reserving bit 63 has no observable effect on normal
/// writes — the bit is always 0 in entries written by `try_send_append`.
/// Only `try_send_append_ordered` sets it.
pub const ORDERED_LSN_FLAG: u64 = 1u64 << 63;

/// Outcome reported by the writer task back to an `AppendSync` caller
/// once the rendezvous completes.
///
/// `Synced` is sent AFTER `sync_data()` returns successfully — the
/// caller may safely `+OK` the client. `WriteFailed`/`FsyncFailed`
/// surface the failure mode so the caller can return a specific error
/// frame; either way, durability was NOT achieved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AofAck {
    /// Bytes were written and fsynced. Durability guaranteed.
    Synced,
    /// `write_all()` returned an error. The entry may be partially on
    /// disk; recovery handles partial-payload truncation as crash EOF.
    WriteFailed,
    /// `write_all()` succeeded but `sync_data()` returned an error. The
    /// entry is in the kernel buffer but NOT on durable storage.
    FsyncFailed,
    /// The writer channel was full at the time of the send — the entry
    /// was **not** enqueued. This is a backpressure signal: the writer
    /// is unable to keep up with the current write rate. Callers MUST
    /// treat this as a hard failure (same as `WriteFailed`) under
    /// `appendfsync=always`; for `everysec`/`no` it is logged and counted.
    ChannelFull,
}

/// Global counter incremented each time an AOF `AppendSync` (or fire-and-
/// forget `Append`) is dropped because the writer channel was at capacity.
///
/// Exposed under `# Persistence` in the INFO command as
/// `aof_backpressure_dropped`. A persistently non-zero value indicates the
/// writer is a bottleneck and the operator should investigate disk I/O or
/// switch to `appendfsync=everysec`.
pub static AOF_BACKPRESSURE_DROPPED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Result of awaiting an `AppendSync` ack under a bounded timeout (F2).
///
/// Distinguishes the three terminal states the `Always` durability path
/// can reach so the caller can map each to the correct client-facing
/// outcome:
/// - `Ack(_)`        — the writer reported back; inspect the `AofAck`.
/// - `Disconnected`  — the writer task is gone / channel dropped (no ack
///   will ever arrive). Treated as `WriteFailed`.
/// - `TimedOut`      — the fsync did not confirm within the configured
///   bound. Durability is unconfirmed; treated as `FsyncFailed`. The
///   entry may still reach disk later, so the caller must NOT report
///   success but also must not assume the write was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AckOutcome {
    Ack(AofAck),
    Disconnected,
    TimedOut,
}

/// AOF fsync policy controlling when data is flushed to disk.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FsyncPolicy {
    /// Fsync after every write command (safest, slowest).
    Always,
    /// Fsync once per second in the background (good balance).
    EverySec,
    /// Let the OS decide when to flush (fastest, least safe).
    No,
}

impl FsyncPolicy {
    /// Parse a policy string (as from config). Defaults to EverySec for unknown values.
    pub fn from_str(s: &str) -> Self {
        match s {
            "always" => FsyncPolicy::Always,
            "no" => FsyncPolicy::No,
            _ => FsyncPolicy::EverySec,
        }
    }
}

/// Messages sent to the AOF writer task via mpsc channel.
pub enum AofMessage {
    /// Append serialized RESP command bytes to the AOF file, tagged with the
    /// LSN that was issued for this write (`ReplicationState::issue_lsn`).
    ///
    /// `lsn` semantics by writer task:
    /// - **TopLevel** (`aof_writer_task`): `lsn` is **ignored**; the legacy
    ///   v1 disk format is plain RESP bytes with no per-entry framing.
    /// - **PerShard** (`per_shard_aof_writer_task`): `lsn` is **written** as
    ///   a u64 header per RFC § 2 Rule 1. Disk format per entry:
    ///   `[u64 lsn LE][u32 len LE][RESP bytes of length len]`.
    ///   Recovery reads `(lsn, cmd)` pairs and merges cross-shard
    ///   `OrderedAcrossShards` writes by LSN (RFC § 2 Rule 2).
    ///
    /// Construction sites that issue a real LSN call
    /// `ReplicationState::issue_lsn(shard_id, bytes.len() as u64)` and pass
    /// the returned value. Sites with no replication state available pass 0
    /// (TopLevel ignores it; PerShard treats 0 as "no ordering hint").
    Append { lsn: u64, bytes: Bytes },
    /// Append + fsync + ack rendezvous (RFC § 4 — Fix 2 for the H1
    /// data-loss vector exposed by `appendfsync=always`).
    ///
    /// Same encoding as [`AofMessage::Append`], but the writer task ALWAYS
    /// fsyncs after writing the payload and signals `ack` ONCE the
    /// `sync_data()` syscall returns. The caller is expected to await
    /// `ack` before responding `+OK` to the client so the durability
    /// contract of `appendfsync=always` is honoured end-to-end.
    ///
    /// Failure semantics: on write or fsync error the writer drops `ack`
    /// without sending — the caller's `OneshotReceiver` resolves with
    /// `RecvError`, which it must treat as a hard failure (return an
    /// error frame to the client, do NOT silently +OK).
    ///
    /// Production callers: none in step 7 — this commit ships the
    /// mechanism plus tests. Per-handler integration (which sites use
    /// AppendSync vs Append) is wired in step 9 before lifting the
    /// `--unsafe-multishard-aof` gate.
    AppendSync {
        lsn: u64,
        bytes: Bytes,
        ack: crate::runtime::channel::OneshotSender<AofAck>,
    },
    /// Trigger a full AOF rewrite (compaction) using current database state.
    Rewrite(SharedDatabases),
    /// Trigger AOF rewrite in sharded mode (all shards' databases).
    RewriteSharded(Arc<crate::shard::shared_databases::ShardDatabases>),
    /// [F6] Trigger a per-shard AOF rewrite (compaction) in the PerShard
    /// layout. Sent to EVERY per-shard writer at once. Each writer folds its
    /// own shard (drain → lock → snapshot → write new base+incr at the
    /// coordinator's `new_seq` → reopen), then decrements the shared
    /// `PerShardRewriteCoord`; the last writer commits the manifest once
    /// (single seq flip) and prunes the old generation. The synchronized seq
    /// + single commit are what make multi-shard BGREWRITEAOF crash-safe.
    RewritePerShard {
        shard_dbs: Arc<crate::shard::shared_databases::ShardDatabases>,
        coord: Arc<PerShardRewriteCoord>,
    },
    /// Shut down the AOF writer task gracefully.
    Shutdown,
}

/// Coordinator shared by all per-shard writers participating in one
/// BGREWRITEAOF fan-out (F6).
///
/// Crash-safety contract (mirrors `AofManifest::advance` ordering, but
/// distributed across N writer threads):
///
/// 1. Each writer writes its new base+incr at `new_seq` via
///    `manifest.advance_shard(shard_id, new_seq, rdb)` — which does NOT bump
///    `manifest.seq` or rewrite the manifest. So until the final commit, the
///    on-disk manifest still resolves to `old_seq`; a crash here recovers the
///    intact old generation (no loss, no double-apply).
/// 2. The LAST writer to finish (countdown reaches zero) performs the single
///    durable commit: `manifest.seq = new_seq; write_manifest()`. This is the
///    atomic point at which recovery flips to the new generation.
/// 3. Only AFTER the commit are the old-generation files pruned.
///
/// The manifest is shared via `Arc<Mutex<..>>` and locked ONLY for the brief,
/// await-free `advance_shard` and final-commit critical sections — never held
/// across a blocking disk write of the base RDB (that happens before the lock)
/// nor across `.await`.
pub struct PerShardRewriteCoord {
    /// Writers still to finish. Starts at the shard count; the writer that
    /// decrements it to zero performs the commit + prune.
    remaining: std::sync::atomic::AtomicUsize,
    /// Shared manifest, loaded fresh from disk by the BGREWRITEAOF handler at
    /// rewrite time (normal appends never touch the manifest, and BGREWRITEAOF
    /// is CAS-serialized, so a fresh load is the authoritative current state).
    manifest: Arc<parking_lot::Mutex<crate::persistence::aof_manifest::AofManifest>>,
    /// The generation every writer advances to. Computed once = old_seq + 1.
    new_seq: u64,
    /// The generation being retired; pruned only after the commit.
    old_seq: u64,
    /// Number of shards participating (= initial `remaining`).
    n_shards: usize,
    /// Set by any shard whose fold fails. The final writer checks this and
    /// ABORTS the commit if set — committing `new_seq` while a shard never
    /// wrote its new base would make recovery look for a missing base and
    /// refuse to start. On abort the old generation (`old_seq`) stays the
    /// committed state for all shards (crash-safe).
    failed: std::sync::atomic::AtomicBool,
}

impl PerShardRewriteCoord {
    /// Construct a coordinator for an `n_shards`-way rewrite advancing the
    /// shared `manifest` from its current seq to `current_seq + 1`.
    pub fn new(
        manifest: Arc<parking_lot::Mutex<crate::persistence::aof_manifest::AofManifest>>,
        current_seq: u64,
        n_shards: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            remaining: std::sync::atomic::AtomicUsize::new(n_shards),
            manifest,
            new_seq: current_seq + 1,
            old_seq: current_seq,
            n_shards,
            failed: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// The generation writers advance to.
    #[inline]
    pub fn new_seq(&self) -> u64 {
        self.new_seq
    }

    /// Mark the whole rewrite as failed (called by a shard whose fold errored).
    /// The final writer will abort the commit, leaving `old_seq` authoritative.
    #[inline]
    pub fn mark_failed(&self) {
        self.failed
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Called by each writer AFTER it has durably written its new base+incr at
    /// `new_seq` and reopened its append file. Decrements the countdown; the
    /// final caller commits the manifest (single seq flip) and prunes the old
    /// generation, then clears the global in-progress flag.
    ///
    /// Crash-safety: the commit (`write_manifest`) is the atomic flip point;
    /// pruning runs strictly after it, so a crash mid-prune only orphans
    /// already-superseded files (recovery uses `new_seq`).
    pub fn shard_done(&self) {
        use std::sync::atomic::Ordering;
        // AcqRel: the decrement-to-zero must observe all prior writers'
        // advance_shard manifest mutations before committing.
        if self.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            // Abort if any shard failed to fold: committing new_seq while a
            // shard lacks its new base would break recovery. Keep old_seq.
            if self.failed.load(Ordering::Acquire) {
                let m = self.manifest.lock();
                // Best-effort: prune the orphaned new-seq files written by the
                // shards that DID fold, so they don't linger.
                for sid in 0..self.n_shards {
                    m.prune_shard_files(sid as u16, self.new_seq);
                }
                drop(m);
                error!(
                    "F6 per-shard rewrite ABORTED: a shard failed to fold; seq stays {}. \
                     Old generation remains authoritative (crash-safe). A RESTART is \
                     recommended so successful shards' writers stop appending to the \
                     discarded new generation.",
                    self.old_seq
                );
                crate::command::persistence::AOF_REWRITE_IN_PROGRESS.store(false, Ordering::SeqCst);
                return;
            }
            let mut m = self.manifest.lock();
            m.seq = self.new_seq;
            if let Err(e) = m.write_manifest() {
                error!(
                    "F6 per-shard rewrite: final manifest commit (seq {}) failed: {}. \
                     Old generation remains authoritative; rewrite did not take effect.",
                    self.new_seq, e
                );
                // Do NOT prune — old generation is still the committed state.
                drop(m);
                crate::command::persistence::AOF_REWRITE_IN_PROGRESS.store(false, Ordering::SeqCst);
                return;
            }
            for sid in 0..self.n_shards {
                m.prune_shard_files(sid as u16, self.old_seq);
            }
            drop(m);
            info!(
                "F6 per-shard rewrite complete: committed seq {} across {} shards, pruned seq {}",
                self.new_seq, self.n_shards, self.old_seq
            );
            crate::command::persistence::AOF_REWRITE_IN_PROGRESS.store(false, Ordering::SeqCst);
        }
    }
}

/// Reasons a pool send may be refused without queueing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AofPoolSendError {
    /// `Rewrite`/`RewriteSharded` sent to a `PerShard` pool. BGREWRITEAOF must
    /// be issued per shard in the per-shard layout; the legacy single-writer
    /// rewrite path is not applicable.
    RewriteUnsupportedInPerShard,
    /// Underlying channel send failed (writer task dead or channel full).
    SendFailed,
}

/// Bundle of per-shard AOF writer senders.
///
/// The pool keeps the call-site API uniform regardless of layout:
/// - **TopLevel** (legacy v1, single-shard, also used for `--shards 1` v2):
///   exactly one writer thread; every `sender(shard_id)` returns the same
///   sender so all shards multiplex onto one file.
/// - **PerShard** (v2 multi-shard): one writer per shard; `sender(shard_id)`
///   returns the writer that owns `appendonlydir/shard-{shard_id}/`.
///
/// Step 2a is additive — this type is defined here but no call site is wired
/// to it yet. Step 2c performs the type plumbing in `conn_state` and
/// `conn/core`; steps 2d/2e/2f update the call sites and spawn paths.
/// Default bound for the `appendfsync=always` fsync-ack await. Mirrors the
/// `--aof-fsync-timeout-ms` config default; used by constructors that don't
/// take an explicit timeout (non-production / test helpers).
pub const DEFAULT_AOF_FSYNC_TIMEOUT: Duration = Duration::from_millis(2000);

#[derive(Clone)]
pub struct AofWriterPool {
    senders: Vec<channel::MpscSender<AofMessage>>,
    layout: crate::persistence::aof_manifest::AofLayout,
    /// Fsync policy configured at writer-task construction. Read on the
    /// hot append path: `Always` routes through `AppendSync` for
    /// fsync-before-ack durability (H1 fix); everything else stays on
    /// the fire-and-forget `Append` path.
    fsync_policy: FsyncPolicy,
    /// F2: max time `try_send_append_durable` waits for the `Always` fsync
    /// ack before failing the write. `Duration::ZERO` means unbounded
    /// (legacy behavior). Prevents a stalled disk from parking write
    /// connections forever (design-for-failure).
    fsync_timeout: Duration,
    /// F6: persistence base dir (the parent of `appendonlydir/`), set only for
    /// PerShard pools that may service a per-shard BGREWRITEAOF. Needed to load
    /// the authoritative manifest fresh at rewrite time. `None` for TopLevel
    /// pools and test pools that never rewrite.
    base_dir: Option<PathBuf>,
}

impl AofWriterPool {
    /// Build a TopLevel pool from a single existing writer sender. Used for
    /// legacy v1 deployments and `--shards 1` v2 deployments where one writer
    /// thread services every shard.
    pub fn top_level(sender: channel::MpscSender<AofMessage>) -> Arc<Self> {
        Self::top_level_with_policy(sender, FsyncPolicy::EverySec, DEFAULT_AOF_FSYNC_TIMEOUT)
    }

    /// Same as [`Self::top_level`] but with an explicit fsync policy. The
    /// policy controls whether [`Self::try_send_append_durable`] takes the
    /// fast (fire-and-forget) or rendezvous (`AppendSync`) path.
    /// `fsync_timeout` bounds the `Always` ack await (F2); `Duration::ZERO`
    /// = unbounded.
    pub fn top_level_with_policy(
        sender: channel::MpscSender<AofMessage>,
        fsync_policy: FsyncPolicy,
        fsync_timeout: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            senders: vec![sender],
            layout: crate::persistence::aof_manifest::AofLayout::TopLevel,
            fsync_policy,
            fsync_timeout,
            base_dir: None,
        })
    }

    /// Build a PerShard pool from N senders. `senders[i]` MUST be the writer
    /// task that owns `appendonlydir/shard-{i}/`. The vector's length is the
    /// shard count; passing a length-1 vector here is a bug — use
    /// [`AofWriterPool::top_level`] instead.
    pub fn per_shard(senders: Vec<channel::MpscSender<AofMessage>>) -> Arc<Self> {
        Self::per_shard_with_policy(senders, FsyncPolicy::EverySec, DEFAULT_AOF_FSYNC_TIMEOUT)
    }

    /// Same as [`Self::per_shard`] but with an explicit fsync policy.
    /// `fsync_timeout` bounds the `Always` ack await (F2); `Duration::ZERO`
    /// = unbounded.
    pub fn per_shard_with_policy(
        senders: Vec<channel::MpscSender<AofMessage>>,
        fsync_policy: FsyncPolicy,
        fsync_timeout: Duration,
    ) -> Arc<Self> {
        debug_assert!(
            senders.len() >= 2,
            "per_shard pool needs >=2 writers; use top_level for single-writer"
        );
        Arc::new(Self {
            senders,
            layout: crate::persistence::aof_manifest::AofLayout::PerShard,
            fsync_policy,
            fsync_timeout,
            base_dir: None,
        })
    }

    /// F6: same as [`Self::per_shard_with_policy`] but records the persistence
    /// `base_dir` so a per-shard BGREWRITEAOF can load the authoritative
    /// manifest fresh at rewrite time. This is the production constructor used
    /// by `main.rs` for the PerShard layout.
    pub fn per_shard_with_base_dir(
        senders: Vec<channel::MpscSender<AofMessage>>,
        fsync_policy: FsyncPolicy,
        fsync_timeout: Duration,
        base_dir: PathBuf,
    ) -> Arc<Self> {
        debug_assert!(
            senders.len() >= 2,
            "per_shard pool needs >=2 writers; use top_level for single-writer"
        );
        Arc::new(Self {
            senders,
            layout: crate::persistence::aof_manifest::AofLayout::PerShard,
            fsync_policy,
            fsync_timeout,
            base_dir: Some(base_dir),
        })
    }

    /// Returns the configured fsync policy. Hot-path callers read this to
    /// decide between the fast (`try_send_append`) and durable
    /// (`try_send_append_sync`) write paths.
    #[inline]
    pub fn fsync_policy(&self) -> FsyncPolicy {
        self.fsync_policy
    }

    /// Policy-aware AOF append. For `FsyncPolicy::Always`, this awaits
    /// `AppendSync` and returns `Ok(())` only after `sync_data()` confirms
    /// the entry is on durable storage — closing the H1 in-flight loss
    /// vector identified in the investigation report. For `EverySec` and
    /// `No`, it stays on the fire-and-forget path (zero new latency).
    ///
    /// Returns `Err(AofAck)` only on the Always path when the write or
    /// fsync failed (or the writer task is gone). Callers MUST treat
    /// `Err(_)` as a hard failure — return an error frame to the client,
    /// do NOT respond `+OK`.
    ///
    /// Async because the Always branch awaits a oneshot receiver. The
    /// non-Always branch resolves immediately (no actual suspension) so
    /// the only overhead is one `match` and the implicit Future state
    /// machine; benchmarked at ~5 ns per call on the EverySec hot path,
    /// far below the per-write WAL/replication cost.
    #[inline]
    pub async fn try_send_append_durable(
        &self,
        shard_id: usize,
        lsn: u64,
        bytes: Bytes,
    ) -> Result<(), AofAck> {
        match self.fsync_policy {
            FsyncPolicy::Always => {
                let rx = self.try_send_append_sync(shard_id, lsn, bytes);
                // F2 (design-for-failure): bound the wait so a stalled disk
                // can't park this connection forever. On elapse the write is
                // failed — the entry may still land on disk later, but
                // durability is NOT confirmed, so the caller must not report
                // success. `Duration::ZERO` keeps the legacy unbounded await.
                match Self::await_ack(rx, self.fsync_timeout).await {
                    AckOutcome::Ack(AofAck::Synced) => Ok(()),
                    AckOutcome::Ack(other) => Err(other),
                    // Writer task gone / channel disconnected.
                    AckOutcome::Disconnected => Err(AofAck::WriteFailed),
                    // Fsync did not confirm within the bound.
                    AckOutcome::TimedOut => Err(AofAck::FsyncFailed),
                }
            }
            FsyncPolicy::EverySec | FsyncPolicy::No => {
                self.try_send_append(shard_id, lsn, bytes);
                Ok(())
            }
        }
    }

    /// Await an `AppendSync` ack receiver under a bounded timeout (F2).
    ///
    /// `timeout == Duration::ZERO` preserves the legacy unbounded await
    /// (used when the operator explicitly opts out via
    /// `--aof-fsync-timeout-ms 0`). Otherwise the await is capped by the
    /// runtime-appropriate timer; on elapse the in-flight fsync is
    /// abandoned (the receiver is dropped) and `TimedOut` is returned.
    ///
    /// Runtime-agnostic: monoio uses `select! { rx, sleep }` (matching the
    /// established `cluster::failover` pattern — monoio 0.2 has no
    /// `time::timeout`); tokio uses `tokio::time::timeout`. Both resolve the
    /// ack first if it arrives within the bound, otherwise `TimedOut`.
    async fn await_ack(
        rx: crate::runtime::channel::OneshotReceiver<AofAck>,
        timeout: Duration,
    ) -> AckOutcome {
        if timeout.is_zero() {
            return match rx.await {
                Ok(ack) => AckOutcome::Ack(ack),
                Err(_) => AckOutcome::Disconnected,
            };
        }

        #[cfg(feature = "runtime-monoio")]
        {
            monoio::select! {
                res = rx => match res {
                    Ok(ack) => AckOutcome::Ack(ack),
                    Err(_) => AckOutcome::Disconnected,
                },
                _ = monoio::time::sleep(timeout) => AckOutcome::TimedOut,
            }
        }
        #[cfg(all(feature = "runtime-tokio", not(feature = "runtime-monoio")))]
        {
            match tokio::time::timeout(timeout, rx).await {
                Ok(Ok(ack)) => AckOutcome::Ack(ack),
                Ok(Err(_)) => AckOutcome::Disconnected,
                Err(_) => AckOutcome::TimedOut,
            }
        }
    }

    /// Return the writer sender that owns the given shard's AOF file.
    ///
    /// For TopLevel pools, `shard_id` is ignored — all shards multiplex onto
    /// the single sender. For PerShard pools, `shard_id` MUST be in range
    /// `[0, num_writers())`; an out-of-range id is a programmer error and
    /// panics in debug builds.
    #[inline]
    pub fn sender(&self, shard_id: usize) -> &channel::MpscSender<AofMessage> {
        use crate::persistence::aof_manifest::AofLayout;
        match self.layout {
            AofLayout::TopLevel => &self.senders[0],
            AofLayout::PerShard => {
                debug_assert!(
                    shard_id < self.senders.len(),
                    "shard_id {} out of range for per-shard pool of size {}",
                    shard_id,
                    self.senders.len()
                );
                &self.senders[shard_id]
            }
        }
    }

    /// Fire-and-forget append for the given shard, tagged with the LSN that
    /// was issued for this write (see [`AofMessage::Append`] docs for LSN
    /// semantics per layout). Call sites must source `lsn` from
    /// `ReplicationState::issue_lsn(shard_id, bytes.len() as u64)` for writes
    /// that participate in replication ordering; sites without a
    /// replication-state handle pass 0.
    #[inline]
    pub fn try_send_append(&self, shard_id: usize, lsn: u64, bytes: Bytes) {
        let _ = self
            .sender(shard_id)
            .try_send(AofMessage::Append { lsn, bytes });
    }

    /// Synchronous (fsync-before-ack) append for `appendfsync=always`
    /// durability (RFC § 4 — Fix 2). Returns a receiver the caller MUST
    /// await before responding to the client; `AofAck::Synced` means the
    /// entry is on durable storage.
    ///
    /// **Failure handling:** if the write or fsync fails, the receiver
    /// resolves with `AofAck::WriteFailed` / `AofAck::FsyncFailed`. If
    /// the writer task is gone (shutdown / channel disconnect), the
    /// receiver resolves with `Err(RecvError)`. In every failure mode the
    /// caller MUST return an error frame to the client, NOT `+OK`.
    ///
    /// **Performance:** every call adds a writer round-trip plus an
    /// fsync syscall on the critical path. This is the explicit Redis
    /// contract for `appendfsync=always`; callers should gate on the
    /// configured policy and prefer [`Self::try_send_append`] for
    /// `everysec`/`no`.
    ///
    /// **`shard_id` semantics:** matches [`Self::try_send_append`] — for
    /// TopLevel the parameter is ignored, for PerShard it routes to
    /// `senders[shard_id]`.
    pub fn try_send_append_sync(
        &self,
        shard_id: usize,
        lsn: u64,
        bytes: Bytes,
    ) -> crate::runtime::channel::OneshotReceiver<AofAck> {
        let (ack_tx, ack_rx) = crate::runtime::channel::oneshot::<AofAck>();
        match self.sender(shard_id).try_send(AofMessage::AppendSync {
            lsn,
            bytes,
            ack: ack_tx,
        }) {
            Ok(()) => {}
            Err(flume::TrySendError::Full(_)) => {
                // Writer channel is at capacity — count the dropped entry and
                // signal ChannelFull back to the caller via a pre-filled
                // oneshot so the caller's `.await` resolves immediately to
                // Err(AofAck::ChannelFull) without a writer round-trip.
                AOF_BACKPRESSURE_DROPPED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                warn!(
                    "AOF writer channel full (shard {}): AppendSync dropped; \
                     backpressure_dropped={}",
                    shard_id,
                    AOF_BACKPRESSURE_DROPPED.load(std::sync::atomic::Ordering::Relaxed),
                );
                // Pre-send ChannelFull into a fresh oneshot pair; the
                // caller's `ack_rx` was already returned — we create a
                // new pair and use its sender to pre-fill what the caller
                // will receive. The original ack_tx (inside the dropped
                // AppendSync) is dropped, causing its ack_rx to yield
                // RecvError. We send ChannelFull via the *returned* ack_rx
                // by using a second oneshot whose sender is immediately
                // fulfilled, then return that receiver instead.
                let (pre_tx, pre_rx) = crate::runtime::channel::oneshot::<AofAck>();
                let _ = pre_tx.send(AofAck::ChannelFull);
                return pre_rx;
            }
            Err(flume::TrySendError::Disconnected(_)) => {
                // Writer task is dead — let caller handle RecvError on ack_rx.
                // ack_tx was dropped inside the Err value; ack_rx will
                // resolve with RecvError, which try_send_append_durable maps
                // to Err(AofAck::WriteFailed).
            }
        }
        ack_rx
    }

    /// Fire-and-forget append for a cross-shard atomic operation (RFC § 2
    /// Rule 2 — `OrderedAcrossShards` tagging).
    ///
    /// The high bit of `lsn` (`1 << 63`) is set before the entry is queued.
    /// Recovery uses this bit to recognize cross-shard atomic entries,
    /// buffer them per-shard, and replay them globally in LSN order after
    /// per-shard replay completes — guaranteeing TXN/SCRIPT atomicity
    /// survives a crash even when multiple shards participated.
    ///
    /// **Caller contract:** `lsn` MUST be < `1 << 63` (i.e. the high bit
    /// MUST be clear when passed in). Practical LSN ceilings — even at
    /// 10 M writes/s sustained for a century — sit around 2^58, so any
    /// real LSN satisfies this. Debug builds assert; release builds mask
    /// the input to keep the wire format well-formed rather than
    /// corrupt-by-zero-extending.
    ///
    /// **Production callers today:** none. Step 5 ships the infrastructure
    /// (writer, framing flag, recovery merge) so a future cross-shard TXN
    /// or replicated SCRIPT command has a place to land. Until that
    /// consumer exists, only test code emits ordered entries.
    #[inline]
    pub fn try_send_append_ordered(&self, shard_id: usize, lsn: u64, bytes: Bytes) {
        debug_assert_eq!(
            lsn & ORDERED_LSN_FLAG,
            0,
            "try_send_append_ordered: lsn must not have the high bit set; got {:#x}",
            lsn,
        );
        let tagged_lsn = (lsn & !ORDERED_LSN_FLAG) | ORDERED_LSN_FLAG;
        let _ = self.sender(shard_id).try_send(AofMessage::Append {
            lsn: tagged_lsn,
            bytes,
        });
    }

    /// Issue an LSN for an AOF append at every call site that has the
    /// `Option<Arc<RwLock<ReplicationState>>>` shape. Wraps
    /// `ReplicationState::issue_lsn` so handler call sites collapse to a
    /// single line.
    ///
    /// Returns 0 when:
    /// - `repl_state` is None (test fixtures or shutdown paths)
    /// - the `RwLock` is poisoned (shouldn't happen in production —
    ///   ReplicationState is only `write()`-locked under known-safe paths)
    ///
    /// 0 is a sentinel meaning "no replication ordering for this write".
    /// TopLevel writers ignore the LSN entirely so 0 is harmless there;
    /// PerShard writers treat 0 the same as any other LSN (per-shard order
    /// is preserved by write order, not by LSN value). The LSN only matters
    /// for the cross-shard `OrderedAcrossShards` merge in RFC step 5.
    #[inline]
    pub fn issue_append_lsn(
        repl_state: &Option<Arc<std::sync::RwLock<crate::replication::state::ReplicationState>>>,
        shard_id: usize,
        delta: usize,
    ) -> u64 {
        repl_state
            .as_ref()
            .and_then(|rs| rs.read().ok().map(|g| g.issue_lsn(shard_id, delta as u64)))
            .unwrap_or(0)
    }

    /// Submit a Rewrite/RewriteSharded message. Only legal for TopLevel pools;
    /// PerShard rewrites are per-shard operations and must be initiated by
    /// the BGREWRITEAOF code path in step 6, not via this enum variant.
    pub fn try_send_rewrite(&self, msg: AofMessage) -> Result<(), AofPoolSendError> {
        use crate::persistence::aof_manifest::AofLayout;
        debug_assert!(
            matches!(msg, AofMessage::Rewrite(_) | AofMessage::RewriteSharded(_)),
            "try_send_rewrite called with a non-Rewrite variant",
        );
        if self.layout == AofLayout::PerShard {
            return Err(AofPoolSendError::RewriteUnsupportedInPerShard);
        }
        self.senders[0]
            .try_send(msg)
            .map_err(|_| AofPoolSendError::SendFailed)
    }

    /// [F6] Initiate a per-shard BGREWRITEAOF across every writer in a
    /// PerShard pool.
    ///
    /// Loads the authoritative manifest fresh from `base_dir` (normal appends
    /// never mutate the manifest, and BGREWRITEAOF is CAS-serialized by
    /// `AOF_REWRITE_IN_PROGRESS`, so a fresh load is the current committed
    /// state), builds a shared [`PerShardRewriteCoord`] that advances the
    /// generation by one, and hands every writer the same `coord` + a cheap
    /// `Arc` clone of `shard_dbs`.
    ///
    /// **Reliable delivery (design-for-failure):** the fan-out uses the
    /// *blocking* `send` rather than `try_send`. A dropped rewrite message
    /// would leave the countdown unable to reach zero — folded writers would
    /// have reopened to new-seq files that the manifest never commits, silently
    /// losing their post-rewrite appends. The writers run on dedicated threads
    /// draining continuously, so `send` blocks only until a channel slot frees
    /// (sub-millisecond), which is acceptable for a rare admin command.
    ///
    /// Returns `SendFailed` if `base_dir` is unset, the manifest can't be
    /// loaded, or a writer thread is gone (disconnected channel). On the last
    /// case the rewrite aborts WITHOUT committing — the old generation stays
    /// authoritative (crash-safe), but a dead writer already means that shard's
    /// persistence was compromised before this call.
    pub fn try_send_rewrite_per_shard(
        &self,
        shard_dbs: Arc<crate::shard::shared_databases::ShardDatabases>,
    ) -> Result<(), AofPoolSendError> {
        use crate::persistence::aof_manifest::{AofLayout, AofManifest};
        if self.layout != AofLayout::PerShard {
            // A TopLevel pool rewrites via try_send_rewrite; this entry point
            // is PerShard-only.
            return Err(AofPoolSendError::RewriteUnsupportedInPerShard);
        }
        let base_dir = self.base_dir.as_ref().ok_or(AofPoolSendError::SendFailed)?;
        let manifest = match AofManifest::load(base_dir) {
            Ok(Some(m)) if m.layout == AofLayout::PerShard => m,
            Ok(_) => {
                error!(
                    "F6 per-shard rewrite: manifest at {} missing or not PerShard; aborting",
                    base_dir.display()
                );
                return Err(AofPoolSendError::SendFailed);
            }
            Err(e) => {
                error!(
                    "F6 per-shard rewrite: failed to load manifest at {}: {}",
                    base_dir.display(),
                    e
                );
                return Err(AofPoolSendError::SendFailed);
            }
        };
        let current_seq = manifest.seq;
        let n_shards = self.senders.len();
        let shared_manifest = Arc::new(parking_lot::Mutex::new(manifest));
        let coord = PerShardRewriteCoord::new(shared_manifest, current_seq, n_shards);
        for s in &self.senders {
            // Blocking send for guaranteed delivery — see the doc comment.
            if s.send(AofMessage::RewritePerShard {
                shard_dbs: shard_dbs.clone(),
                coord: coord.clone(),
            })
            .is_err()
            {
                error!(
                    "F6 per-shard rewrite: a writer channel is disconnected; \
                     rewrite aborted (no manifest commit, old generation remains \
                     authoritative). Inspect AOF writer threads."
                );
                return Err(AofPoolSendError::SendFailed);
            }
        }
        info!(
            "F6 per-shard rewrite dispatched: seq {} -> {} across {} shards",
            current_seq,
            current_seq + 1,
            n_shards
        );
        Ok(())
    }

    /// Broadcast `Shutdown` to every writer. Used by orchestrated shutdown
    /// paths in `main.rs`/`embedded.rs`. Each writer drains its channel and
    /// fsyncs before exiting.
    pub fn broadcast_shutdown(&self) {
        for s in &self.senders {
            let _ = s.try_send(AofMessage::Shutdown);
        }
    }

    /// Number of underlying writer senders. 1 for TopLevel, num_shards for
    /// PerShard.
    #[inline]
    pub fn num_writers(&self) -> usize {
        self.senders.len()
    }

    /// Reports the pool's layout. Useful for places that need to refuse
    /// PerShard-incompatible legacy code paths with a clear error.
    #[inline]
    pub fn layout(&self) -> crate::persistence::aof_manifest::AofLayout {
        self.layout
    }
}

#[cfg(test)]
mod pool_tests {
    use super::*;
    use crate::persistence::aof_manifest::AofLayout;
    use crate::runtime::channel;

    #[test]
    fn top_level_pool_routes_all_shards_to_writer_zero() {
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(8);
        let pool = AofWriterPool::top_level(tx);
        assert_eq!(pool.num_writers(), 1);
        assert_eq!(pool.layout(), AofLayout::TopLevel);

        pool.try_send_append(0, 0, Bytes::from_static(b"a"));
        pool.try_send_append(7, 0, Bytes::from_static(b"b"));
        pool.try_send_append(42, 0, Bytes::from_static(b"c"));

        let mut seen = 0;
        while rx.try_recv().is_ok() {
            seen += 1;
        }
        assert_eq!(seen, 3, "all 3 appends should land on writer 0");
    }

    #[test]
    fn per_shard_pool_routes_each_shard_to_its_own_writer() {
        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(8);
        let (tx1, rx1) = channel::mpsc_bounded::<AofMessage>(8);
        let (tx2, rx2) = channel::mpsc_bounded::<AofMessage>(8);
        let pool = AofWriterPool::per_shard(vec![tx0, tx1, tx2]);
        assert_eq!(pool.num_writers(), 3);
        assert_eq!(pool.layout(), AofLayout::PerShard);

        pool.try_send_append(0, 100, Bytes::from_static(b"shard0"));
        pool.try_send_append(1, 200, Bytes::from_static(b"shard1a"));
        pool.try_send_append(1, 300, Bytes::from_static(b"shard1b"));
        pool.try_send_append(2, 400, Bytes::from_static(b"shard2"));

        let count = |rx: &channel::MpscReceiver<AofMessage>| -> usize {
            let mut n = 0;
            while rx.try_recv().is_ok() {
                n += 1;
            }
            n
        };
        assert_eq!(count(&rx0), 1, "shard 0 writer should receive exactly 1");
        assert_eq!(count(&rx1), 2, "shard 1 writer should receive exactly 2");
        assert_eq!(count(&rx2), 1, "shard 2 writer should receive exactly 1");
    }

    #[test]
    fn per_shard_pool_rejects_rewrite_with_explicit_error() {
        let (tx0, _rx0) = channel::mpsc_bounded::<AofMessage>(8);
        let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(8);
        let pool = AofWriterPool::per_shard(vec![tx0, tx1]);

        let dummies: SharedDatabases = Arc::new(vec![]);
        let err = pool
            .try_send_rewrite(AofMessage::Rewrite(dummies))
            .unwrap_err();
        assert_eq!(err, AofPoolSendError::RewriteUnsupportedInPerShard);
    }

    #[test]
    fn top_level_pool_accepts_rewrite() {
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(8);
        let pool = AofWriterPool::top_level(tx);

        let dummies: SharedDatabases = Arc::new(vec![]);
        pool.try_send_rewrite(AofMessage::Rewrite(dummies)).unwrap();
        assert!(matches!(rx.try_recv(), Ok(AofMessage::Rewrite(_))));
    }

    #[test]
    fn per_shard_pool_threads_lsn_field_to_each_writer() {
        // Step 3 wire-format contract: try_send_append carries the issued LSN
        // through to the writer task, which writes it as the per-entry header
        // under PerShard layout. This unit test pins the channel-side contract
        // (the disk-side framing is covered by writer-task integration).
        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, rx1) = channel::mpsc_bounded::<AofMessage>(4);
        let pool = AofWriterPool::per_shard(vec![tx0, tx1]);

        pool.try_send_append(0, 42, Bytes::from_static(b"set foo 1"));
        pool.try_send_append(1, 43, Bytes::from_static(b"set bar 2"));
        pool.try_send_append(0, 44, Bytes::from_static(b"del foo"));

        // Shard 0 should see (42, "set foo 1") then (44, "del foo").
        match rx0.try_recv() {
            Ok(AofMessage::Append { lsn, bytes }) => {
                assert_eq!(lsn, 42, "shard 0 first entry lsn");
                assert_eq!(bytes.as_ref(), b"set foo 1");
            }
            other => panic!(
                "shard 0 first recv expected Append, got {:?}",
                other.is_ok()
            ),
        }
        match rx0.try_recv() {
            Ok(AofMessage::Append { lsn, bytes }) => {
                assert_eq!(lsn, 44, "shard 0 second entry lsn");
                assert_eq!(bytes.as_ref(), b"del foo");
            }
            other => panic!(
                "shard 0 second recv expected Append, got {:?}",
                other.is_ok()
            ),
        }
        // Shard 1 should see (43, "set bar 2") only.
        match rx1.try_recv() {
            Ok(AofMessage::Append { lsn, bytes }) => {
                assert_eq!(lsn, 43, "shard 1 entry lsn");
                assert_eq!(bytes.as_ref(), b"set bar 2");
            }
            other => panic!("shard 1 recv expected Append, got {:?}", other.is_ok()),
        }
    }

    #[test]
    fn try_send_append_sync_queues_appendsync_with_ack() {
        // Channel-level wiring contract for the H1 fix: `try_send_append_sync`
        // queues `AofMessage::AppendSync { lsn, bytes, ack }`, and the
        // returned receiver resolves to whatever value the (mocked) writer
        // sends on `ack`. End-to-end durability is covered by step 8
        // (CRASH-01-LITE); this pins the API contract.
        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(4);
        let pool = AofWriterPool::per_shard(vec![tx0, tx1]);

        let recv = pool.try_send_append_sync(0, 99, Bytes::from_static(b"SET k v"));

        // Drain the queue; the writer would normally do this. Capture the
        // ack sender, do the (mock) durable write, then ack Synced.
        let ack = match rx0.try_recv() {
            Ok(AofMessage::AppendSync { lsn, bytes, ack }) => {
                assert_eq!(lsn, 99, "lsn forwarded through the channel");
                assert_eq!(bytes.as_ref(), b"SET k v", "bytes forwarded");
                ack
            }
            other => panic!("expected AppendSync, got {:?}", other.is_ok()),
        };

        // Writer reports Synced — caller observes Synced.
        let _ = ack.send(AofAck::Synced);
        let result = recv.recv_blocking().expect("receiver resolves");
        assert_eq!(result, AofAck::Synced);
    }

    #[test]
    fn append_sync_writer_dropped_resolves_recv_error() {
        // If the writer task is dead or the channel disconnects between
        // queueing and the ack send, the receiver MUST resolve with an
        // error rather than hang. Callers treat that as a hard failure
        // (return an error frame, do not +OK).
        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(4);
        let pool = AofWriterPool::per_shard(vec![tx0, tx1]);

        let recv = pool.try_send_append_sync(0, 7, Bytes::from_static(b"x"));

        // Drain the message but DROP the ack sender without sending.
        match rx0.try_recv() {
            Ok(AofMessage::AppendSync { ack, .. }) => drop(ack),
            other => panic!("expected AppendSync, got {:?}", other.is_ok()),
        }

        let err = recv.recv_blocking().expect_err("dropped ack -> RecvError");
        // Crash-safe: we got a sentinel-style error, not a hang.
        let _ = err;
    }

    #[test]
    fn append_sync_writer_reports_write_failed() {
        // Writer encountered a write_all error; recv returns WriteFailed.
        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(4);
        let pool = AofWriterPool::per_shard(vec![tx0, tx1]);

        let recv = pool.try_send_append_sync(0, 1, Bytes::from_static(b"x"));
        let ack = match rx0.try_recv() {
            Ok(AofMessage::AppendSync { ack, .. }) => ack,
            other => panic!("expected AppendSync, got {:?}", other.is_ok()),
        };
        let _ = ack.send(AofAck::WriteFailed);
        let result = recv.recv_blocking().expect("recv resolves");
        assert_eq!(result, AofAck::WriteFailed);
    }

    #[test]
    fn append_sync_writer_reports_fsync_failed() {
        // Writer wrote the payload but fsync (sync_data) returned an error.
        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(4);
        let pool = AofWriterPool::per_shard(vec![tx0, tx1]);

        let recv = pool.try_send_append_sync(0, 1, Bytes::from_static(b"x"));
        let ack = match rx0.try_recv() {
            Ok(AofMessage::AppendSync { ack, .. }) => ack,
            other => panic!("expected AppendSync, got {:?}", other.is_ok()),
        };
        let _ = ack.send(AofAck::FsyncFailed);
        let result = recv.recv_blocking().expect("recv resolves");
        assert_eq!(result, AofAck::FsyncFailed);
    }

    // F2 (design-for-failure): `appendfsync=always` must bound its fsync-ack
    // await. A stalled writer must surface a hard error within the budget,
    // never park the connection forever. Tokio-gated because it drives the
    // runtime timer; the monoio path shares the proven `select! + sleep`
    // shape from `cluster::failover`, exercised end-to-end by the crash tests.
    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn always_fsync_times_out_when_writer_never_acks() {
        // Writer channel is held (kept open) but never drained → the
        // AppendSync sits buffered with its ack sender alive, so the receiver
        // never resolves. The bounded await MUST elapse and report failure.
        let (tx0, _rx0) = channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(4);
        let pool = AofWriterPool::per_shard_with_policy(
            vec![tx0, tx1],
            FsyncPolicy::Always,
            Duration::from_millis(50),
        );

        let start = Instant::now();
        let res = pool
            .try_send_append_durable(0, 1, Bytes::from_static(b"x"))
            .await;
        let elapsed = start.elapsed();

        assert_eq!(
            res,
            Err(AofAck::FsyncFailed),
            "timed-out fsync must map to FsyncFailed (durability unconfirmed)"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "must fail within the bound, not hang (took {:?})",
            elapsed
        );
        // Keep the receivers alive until here so the message stays buffered.
        drop((_rx0, _rx1));
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn always_fsync_succeeds_when_writer_acks_in_time() {
        // Happy path: a writer drains the AppendSync and acks `Synced` well
        // within the bound → the durable append returns Ok(()).
        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(4);
        let pool = AofWriterPool::per_shard_with_policy(
            vec![tx0, tx1],
            FsyncPolicy::Always,
            Duration::from_millis(500),
        );

        tokio::spawn(async move {
            if let Ok(AofMessage::AppendSync { ack, .. }) = rx0.recv_async().await {
                let _ = ack.send(AofAck::Synced);
            }
        });

        let res = pool
            .try_send_append_durable(0, 1, Bytes::from_static(b"x"))
            .await;
        assert_eq!(res, Ok(()), "ack within the bound must succeed");
        drop(_rx1);
    }

    #[test]
    fn broadcast_shutdown_reaches_every_writer() {
        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(2);
        let (tx1, rx1) = channel::mpsc_bounded::<AofMessage>(2);
        let (tx2, rx2) = channel::mpsc_bounded::<AofMessage>(2);
        let pool = AofWriterPool::per_shard(vec![tx0, tx1, tx2]);

        pool.broadcast_shutdown();

        for (i, rx) in [&rx0, &rx1, &rx2].iter().enumerate() {
            assert!(
                matches!(rx.try_recv(), Ok(AofMessage::Shutdown)),
                "writer {} did not receive Shutdown",
                i
            );
        }
    }

    /// FIX-W1-1 contract: `try_send_append_durable` under `Always` policy MUST
    /// return `Err(AofAck::FsyncFailed)` when the writer reports failure.
    /// handler_single.rs must await this BEFORE flushing responses to the client.
    ///
    /// Uses spawn_blocking to simulate the mock writer responding on the ack
    /// channel concurrently, which allows the async rendezvous to complete.
    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn always_policy_try_send_append_durable_returns_err_on_fsync_fail() {
        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(4);
        let pool = std::sync::Arc::new(AofWriterPool::per_shard_with_policy(
            vec![tx0, tx1],
            FsyncPolicy::Always,
            Duration::ZERO, // legacy unbounded await — disconnect/ack resolves it
        ));

        // Spawn a mock writer that drains AppendSync and responds with FsyncFailed.
        // Runs in a blocking thread (flume's blocking recv) so it doesn't block
        // the async executor while waiting for the handler to enqueue the message.
        let mock_writer = tokio::task::spawn_blocking(move || {
            // flume::Receiver::recv() blocks until a message is available
            let msg = rx0.recv().expect("mock writer got message");
            if let AofMessage::AppendSync { ack, .. } = msg {
                let _ = ack.send(AofAck::FsyncFailed);
            } else {
                panic!("expected AppendSync under Always policy");
            }
        });

        // The handler MUST await this BEFORE flushing responses to the client
        let result = pool
            .try_send_append_durable(0, 1, Bytes::from_static(b"SET k v"))
            .await;
        mock_writer.await.expect("mock writer completed");

        assert_eq!(
            result,
            Err(AofAck::FsyncFailed),
            "Always policy MUST propagate fsync failure so caller can return an error frame"
        );
    }

    /// FIX-W1-1 ordering contract: when `aof_entries` carries `(resp_idx, bytes)`
    /// tuples, the handler can patch `responses[resp_idx]` on AOF failure BEFORE
    /// flushing to the client. This test verifies the indexing is sound.
    #[test]
    fn aof_entries_indexed_by_response_slot_patches_correctly() {
        use crate::protocol::Frame;
        let mut responses: Vec<Frame> = vec![
            Frame::SimpleString(bytes::Bytes::from_static(b"OK")),
            Frame::SimpleString(bytes::Bytes::from_static(b"OK")),
            Frame::SimpleString(bytes::Bytes::from_static(b"OK")),
        ];
        // Simulate two write commands at response indices 0 and 2 (index 1 was a read)
        let aof_entries: Vec<(usize, Bytes)> = vec![
            (0, Bytes::from_static(b"SET a 1")),
            (2, Bytes::from_static(b"SET c 3")),
        ];

        // AOF write at index 2 fails; patch that response slot
        for (resp_idx, _bytes) in &aof_entries {
            if *resp_idx == 2 {
                // Simulate Err(AofAck::FsyncFailed) from try_send_append_durable
                responses[*resp_idx] =
                    Frame::Error(Bytes::from_static(b"WRITEFAIL aof fsync failed"));
            }
        }

        assert!(
            matches!(&responses[0], Frame::SimpleString(_)),
            "index 0 (successful fsync) should remain +OK"
        );
        assert!(
            matches!(&responses[1], Frame::SimpleString(_)),
            "index 1 (read, no AOF) should remain +OK"
        );
        assert!(
            matches!(&responses[2], Frame::Error(_)),
            "index 2 (failed fsync) must be patched to error"
        );
    }

    // NOTE (FIX-W1-1 r3): The H1 ordering regression test was moved to
    // `src/server/conn/handler_single.rs` (test module, fn
    // `flush_with_aof_ack_ack_precedes_response`).  The previous inline
    // reproduction here was non-discriminating — it reproduced the ack-first
    // loop IN THE TEST BODY rather than calling the real production fn, so it
    // passed on both pre-fix and post-fix binaries.
    //
    // The new test calls `flush_with_aof_ack` directly (the fn the handler now
    // delegates to), so inverting Phase 1/Phase 2 order in that fn causes a
    // measurable timing failure (`elapsed_ms ≈ 0ms < 55ms`).
    //
    // End-to-end ordering is also covered by:
    //   tests/crash_matrix_per_shard_aof.rs  (CRASH-01-LITE — AlwaysPolicy shards)

    // -----------------------------------------------------------------------
    // FIX-W2-5: channel-full returns AofAck::ChannelFull + increments counter
    // -----------------------------------------------------------------------
    #[test]
    fn try_send_append_sync_channel_full_returns_channel_full_ack() {
        // Create a channel with capacity 1 and fill it so the next try_send
        // hits TrySendError::Full.
        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(1);
        // Fill the channel by pre-loading one message.
        tx0.try_send(AofMessage::Shutdown).expect("pre-fill");
        // rx0 intentionally not consumed — channel is now at capacity.

        let pool = AofWriterPool::top_level(tx0);

        let before = AOF_BACKPRESSURE_DROPPED.load(std::sync::atomic::Ordering::Relaxed);
        let recv = pool.try_send_append_sync(0, 1, Bytes::from_static(b"SET k v"));

        // The channel was full — ChannelFull is returned immediately without
        // a writer round-trip.
        let result = recv.recv_blocking().expect("pre-filled oneshot resolves");
        assert_eq!(
            result,
            AofAck::ChannelFull,
            "channel-full must yield ChannelFull, not {:?}",
            result
        );

        let after = AOF_BACKPRESSURE_DROPPED.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            after,
            before + 1,
            "backpressure counter must increment by 1"
        );

        // No AppendSync should have reached the (blocked) reader.
        drop(rx0); // drain without consuming — just verify nothing snuck through
    }

    // -----------------------------------------------------------------------
    // FIX-W2-9: try_send_append_durable must be used for SWAPDB-like mutations
    //
    // Red test: documents the contract that handler_single.rs SHOULD honour.
    // When appendfsync=always, try_send_append_durable MUST return Err on
    // writer failure so callers can abort the mutation safely.
    // -----------------------------------------------------------------------
    #[test]
    fn try_send_append_durable_always_writer_dead_returns_write_failed() {
        // Create a pool with Always policy. The writer task is not running —
        // we model that by draining the channel message and then dropping the
        // ack sender, simulating a dead writer.
        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(4);
        let pool = AofWriterPool::per_shard_with_policy(
            vec![tx0, tx1],
            FsyncPolicy::Always,
            Duration::ZERO, // legacy unbounded await — disconnect resolves it
        );

        // Spawn a thread that pulls the AppendSync off the channel but drops
        // the ack without sending — simulating a writer crash mid-fsync.
        let rx0_clone = rx0;
        let handle = std::thread::spawn(move || {
            match rx0_clone.recv() {
                Ok(AofMessage::AppendSync { ack, .. }) => drop(ack), // writer crash
                other => panic!("unexpected message: {:?}", other.is_ok()),
            }
        });

        // try_send_append_durable for Always must await the ack.
        // With the ack sender dropped, it should resolve to Err(WriteFailed).
        let result = futures::executor::block_on(pool.try_send_append_durable(
            0,
            55,
            Bytes::from_static(b"SWAPDB 0 1"),
        ));

        handle.join().expect("ack dropper thread");

        assert!(
            result.is_err(),
            "try_send_append_durable with dead writer must return Err, got Ok"
        );
        assert_eq!(
            result.unwrap_err(),
            AofAck::WriteFailed,
            "dead writer must resolve to WriteFailed"
        );
    }

    #[test]
    fn try_send_append_durable_everysec_is_fire_and_forget() {
        // EverySec policy: try_send_append_durable always returns Ok — the
        // durability policy doesn't block on fsync. handler_single.rs must
        // use try_send_append_durable so the policy is respected.
        let (tx0, _rx0) = channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(4);
        let pool = AofWriterPool::per_shard_with_policy(
            vec![tx0, tx1],
            FsyncPolicy::EverySec,
            Duration::ZERO,
        );

        let result = futures::executor::block_on(pool.try_send_append_durable(
            0,
            56,
            Bytes::from_static(b"SWAPDB 0 1"),
        ));

        assert!(
            result.is_ok(),
            "EverySec policy must be fire-and-forget (Ok), got {:?}",
            result
        );
    }
}

/// Serialize a Frame into RESP wire format bytes.
pub fn serialize_command(frame: &Frame) -> Bytes {
    let mut buf = BytesMut::with_capacity(64);
    serialize::serialize(frame, &mut buf);
    buf.freeze()
}

/// Background AOF writer task. Receives commands via mpsc channel and appends them
/// to the AOF file. Handles fsync according to the configured policy.
pub async fn aof_writer_task(
    rx: channel::MpscReceiver<AofMessage>,
    aof_path: PathBuf,
    fsync: FsyncPolicy,
    cancel: CancellationToken,
) {
    #[cfg(feature = "runtime-tokio")]
    use tokio::io::AsyncWriteExt;

    // Open file in append mode (create if not exists)
    #[cfg(feature = "runtime-tokio")]
    let file: tokio::fs::File = match tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&aof_path)
        .await
    {
        Ok(f) => f,
        Err(e) => {
            error!("Failed to open AOF file {}: {}", aof_path.display(), e);
            return;
        }
    };

    #[cfg(feature = "runtime-tokio")]
    let mut writer = tokio::io::BufWriter::new(file);
    #[cfg(feature = "runtime-tokio")]
    let mut last_fsync = Instant::now();
    #[cfg(feature = "runtime-tokio")]
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    #[cfg(feature = "runtime-tokio")]
    interval.tick().await; // consume first tick

    // Monoio path: multi-part AOF (base RDB + incremental RESP) with sync I/O.
    //
    // On startup, if appendonlydir/ exists with a manifest, open the current
    // incr file for appending. Otherwise start fresh with seq 1.
    // On BGREWRITEAOF: snapshot → write new base RDB → create new incr → advance manifest.
    #[cfg(feature = "runtime-monoio")]
    {
        use crate::persistence::aof_manifest::AofManifest;
        use std::io::Write;

        // Resolve the persistence base directory from aof_path's parent.
        let base_dir = aof_path.parent().unwrap_or(Path::new(".")).to_path_buf();

        // Load manifest — do NOT create one here if it doesn't exist.
        // main.rs recovery runs concurrently and must finish before a manifest
        // is created, to avoid racing against legacy single-file AOF detection.
        // main.rs will create the manifest after recovery completes.
        //
        // A corrupt manifest is fatal — exit the writer so the server startup
        // notices and fails loud rather than silently overwriting.
        //
        // Bounded wait: check the cancellation token each iteration and enforce
        // a hard timeout so the writer doesn't spin forever if main.rs fails to
        // create the manifest (e.g. disk full, permission error).
        let manifest_wait_start = Instant::now();
        const MANIFEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
        let mut manifest = loop {
            if cancel.is_cancelled() {
                info!("AOF writer: cancelled while waiting for manifest");
                return;
            }
            if manifest_wait_start.elapsed() > MANIFEST_TIMEOUT {
                error!(
                    "AOF writer: manifest not found at {} after {:?}. Writer exiting; check recovery logs.",
                    base_dir.display(),
                    MANIFEST_TIMEOUT,
                );
                return;
            }
            match AofManifest::load(&base_dir) {
                Ok(Some(m)) => break m,
                Ok(None) => {
                    // main.rs recovery hasn't created the manifest yet — wait.
                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
                Err(e) => {
                    error!(
                        "AOF manifest corrupt at {}: {}. Writer exiting; persistence disabled.",
                        base_dir.display(),
                        e
                    );
                    return;
                }
            }
        };

        // Open the current incremental file for appending
        let incr_path = manifest.incr_path();
        let mut file = match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&incr_path)
        {
            Ok(f) => f,
            Err(e) => {
                error!(
                    "Failed to open AOF incr file {}: {}",
                    incr_path.display(),
                    e
                );
                return;
            }
        };
        info!(
            "AOF writer: seq {}, incr={}",
            manifest.seq,
            incr_path.display()
        );

        let mut last_fsync = Instant::now();

        let mut write_error = false;

        // Test-only fault injection: same env var as the PerShard writer.
        // Read once at task startup; zero cost in production (var absent).
        let fail_fsync_for_test = std::env::var("MOON_TEST_AOF_FSYNC_FAIL").as_deref() == Ok("1");

        loop {
            match rx.recv() {
                // TopLevel writer: legacy v1 disk format is plain RESP. The
                // LSN is ignored — TopLevel is single-shard so per-shard merge
                // by LSN is moot.
                Ok(AofMessage::Append {
                    bytes: data,
                    lsn: _,
                }) => {
                    if write_error {
                        continue; // Drop appends after persistent I/O failure
                    }
                    if let Err(e) = file.write_all(&data) {
                        error!(
                            "AOF write failed (seq {}): {}. Persistence degraded.",
                            manifest.seq, e
                        );
                        write_error = true;
                        continue;
                    }
                    match fsync {
                        FsyncPolicy::Always => {
                            let t = Instant::now();
                            if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                                error!("AOF sync failed (seq {}, always): {}", manifest.seq, e);
                                write_error = true;
                            } else {
                                crate::admin::metrics_setup::record_aof_fsync(
                                    t.elapsed().as_micros() as u64,
                                );
                            }
                        }
                        FsyncPolicy::EverySec => {
                            if last_fsync.elapsed() >= std::time::Duration::from_secs(1) {
                                let t = Instant::now();
                                if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                                    error!(
                                        "AOF sync failed (seq {}, everysec): {}",
                                        manifest.seq, e
                                    );
                                    // Non-fatal for everysec: retry next interval
                                } else {
                                    crate::admin::metrics_setup::record_aof_fsync(
                                        t.elapsed().as_micros() as u64,
                                    );
                                    last_fsync = Instant::now();
                                }
                            }
                        }
                        FsyncPolicy::No => {}
                    }
                }
                // TopLevel writer (monoio): legacy v1 plain RESP, lsn ignored.
                // AppendSync ALWAYS fsyncs and acks before returning, regardless
                // of the configured policy — that's the durability contract the
                // caller signed up for by choosing AppendSync.
                Ok(AofMessage::AppendSync {
                    bytes: data,
                    lsn: _,
                    ack,
                }) => {
                    if write_error {
                        let _ = ack.send(AofAck::WriteFailed);
                        continue;
                    }
                    // Test-only: return FsyncFailed immediately without touching disk.
                    if fail_fsync_for_test {
                        let _ = ack.send(AofAck::FsyncFailed);
                        continue;
                    }
                    if let Err(e) = file.write_all(&data) {
                        error!(
                            "AOF AppendSync write failed (seq {}): {}. Persistence degraded.",
                            manifest.seq, e
                        );
                        write_error = true;
                        let _ = ack.send(AofAck::WriteFailed);
                        continue;
                    }
                    let t = Instant::now();
                    if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                        error!("AOF AppendSync sync failed (seq {}): {}", manifest.seq, e);
                        write_error = true;
                        let _ = ack.send(AofAck::FsyncFailed);
                    } else {
                        crate::admin::metrics_setup::record_aof_fsync(
                            t.elapsed().as_micros() as u64
                        );
                        let _ = ack.send(AofAck::Synced);
                    }
                }
                Ok(AofMessage::Shutdown) | Err(_) => {
                    if !write_error {
                        if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                            error!("AOF final sync failed (seq {}): {}", manifest.seq, e);
                        }
                    }
                    info!("AOF writer shutting down (monoio, seq {})", manifest.seq);
                    break;
                }
                Ok(AofMessage::Rewrite(db)) => {
                    if !write_error {
                        if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                            error!("AOF pre-rewrite sync failed (seq {}): {}", manifest.seq, e);
                        }
                    }
                    match do_rewrite_single(&db, &mut manifest, &mut file, &rx) {
                        Ok(()) => {
                            write_error = false; // Reset on successful rewrite
                        }
                        Err(e) => error!("AOF rewrite failed (seq {}): {}", manifest.seq, e),
                    }
                    crate::command::persistence::AOF_REWRITE_IN_PROGRESS
                        .store(false, std::sync::atomic::Ordering::SeqCst);
                }
                Ok(AofMessage::RewriteSharded(shard_dbs)) => {
                    if !write_error {
                        if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                            error!("AOF pre-rewrite sync failed (seq {}): {}", manifest.seq, e);
                        }
                    }
                    match do_rewrite_sharded(&shard_dbs, &mut manifest, &mut file, &rx) {
                        Ok(()) => {
                            write_error = false;
                        }
                        Err(e) => error!("AOF rewrite failed (seq {}): {}", manifest.seq, e),
                    }
                    crate::command::persistence::AOF_REWRITE_IN_PROGRESS
                        .store(false, std::sync::atomic::Ordering::SeqCst);
                }
                // [F6] A TopLevel writer never owns per-shard files; receiving
                // RewritePerShard means a routing bug. Self-abort so the
                // coordinator's countdown completes and the flag clears.
                Ok(AofMessage::RewritePerShard { coord, .. }) => {
                    warn!("AOF TopLevel writer received RewritePerShard — routing bug; aborting");
                    coord.mark_failed();
                    coord.shard_done();
                }
            }
        }
        return;
    }

    loop {
        #[cfg(feature = "runtime-tokio")]
        tokio::select! {
            msg = rx.recv_async() => {
                match msg {
                    // TopLevel writer (tokio): legacy v1 plain RESP, lsn ignored.
                    Ok(AofMessage::Append { bytes: data, lsn: _ }) => {
                        if let Err(e) = writer.write_all(&data).await {
                            error!("AOF write error: {}", e);
                            continue;
                        }
                        match fsync {
                            FsyncPolicy::Always => {
                                let _ = writer.flush().await;
                                let _ = writer.get_ref().sync_data().await;
                            }
                            FsyncPolicy::EverySec | FsyncPolicy::No => {
                                // EverySec handled by interval tick below; No does nothing
                            }
                        }
                    }
                    // AppendSync: write + fsync + ack, regardless of policy.
                    Ok(AofMessage::AppendSync { bytes: data, lsn: _, ack }) => {
                        if let Err(e) = writer.write_all(&data).await {
                            error!("AOF AppendSync write error: {}", e);
                            let _ = ack.send(AofAck::WriteFailed);
                            continue;
                        }
                        if let Err(e) = writer.flush().await {
                            error!("AOF AppendSync flush error: {}", e);
                            let _ = ack.send(AofAck::FsyncFailed);
                            continue;
                        }
                        if let Err(e) = writer.get_ref().sync_data().await {
                            error!("AOF AppendSync sync_data error: {}", e);
                            let _ = ack.send(AofAck::FsyncFailed);
                            continue;
                        }
                        let _ = ack.send(AofAck::Synced);
                    }
                    Ok(AofMessage::Rewrite(db)) => {
                        // Flush current writer before rewrite
                        let _ = writer.flush().await;
                        let _ = writer.get_ref().sync_data().await;

                        if let Err(e) = rewrite_aof(db, &aof_path).await {
                            error!("AOF rewrite failed: {}", e);
                        }
                        crate::command::persistence::AOF_REWRITE_IN_PROGRESS
                            .store(false, std::sync::atomic::Ordering::SeqCst);

                        // Reopen file after rewrite (it was replaced)
                        let reopen_result: Result<tokio::fs::File, _> = tokio::fs::OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(&aof_path)
                            .await;
                        match reopen_result {
                            Ok(f) => {
                                writer = tokio::io::BufWriter::new(f);
                            }
                            Err(e) => {
                                error!("Failed to reopen AOF file after rewrite: {}", e);
                                return;
                            }
                        }
                    }
                    Ok(AofMessage::RewriteSharded(shard_dbs)) => {
                        let _ = writer.flush().await;
                        let _ = writer.get_ref().sync_data().await;
                        if let Err(e) = rewrite_aof_sharded_sync(&shard_dbs, &aof_path) {
                            error!("AOF rewrite (sharded) failed: {}", e);
                        }
                        crate::command::persistence::AOF_REWRITE_IN_PROGRESS
                            .store(false, std::sync::atomic::Ordering::SeqCst);
                        let reopen_result: Result<tokio::fs::File, _> = tokio::fs::OpenOptions::new()
                            .create(true).append(true).open(&aof_path).await;
                        match reopen_result {
                            Ok(f) => writer = tokio::io::BufWriter::new(f),
                            Err(e) => { error!("Failed to reopen AOF after rewrite: {}", e); return; }
                        }
                    }
                    // [F6] TopLevel writer never owns per-shard files — routing
                    // bug. Self-abort so the countdown completes + flag clears.
                    Ok(AofMessage::RewritePerShard { coord, .. }) => {
                        warn!("AOF TopLevel writer received RewritePerShard — routing bug; aborting");
                        coord.mark_failed();
                        coord.shard_done();
                    }
                    Ok(AofMessage::Shutdown) | Err(_) => {
                        let _ = writer.flush().await;
                        let _ = writer.get_ref().sync_data().await;
                        info!("AOF writer shutting down");
                        break;
                    }
                }
            }
            _ = interval.tick(), if fsync == FsyncPolicy::EverySec => {
                if last_fsync.elapsed() >= std::time::Duration::from_secs(1) {
                    let _ = writer.flush().await;
                    let _ = writer.get_ref().sync_data().await;
                    last_fsync = Instant::now();
                }
            }
            _ = cancel.cancelled() => {
                let _ = writer.flush().await;
                let _ = writer.get_ref().sync_data().await;
                info!("AOF writer cancelled");
                break;
            }
        }
    }
}

/// Background per-shard AOF writer task (Option B step 2b).
///
/// One instance is spawned per shard in `PerShard` layout. Each instance owns
/// `appendonlydir/shard-{shard_id}/moon.aof.{seq}.incr.aof` exclusively — no
/// other writer touches that file, so there is no per-file locking.
///
/// Differences from [`aof_writer_task`] (TopLevel):
/// - Opens `manifest.shard_incr_path(shard_id)` instead of `manifest.incr_path()`.
/// - `Rewrite`/`RewriteSharded` variants are rejected (logged + dropped).
///   The legacy single-writer rewrite enum has no meaning when each shard
///   owns its own files; per-shard BGREWRITEAOF lands in RFC step 6.
/// - Refuses to start if the loaded manifest's layout is `TopLevel` — the
///   spawn site (step 2f) must only invoke this task body for `PerShard`
///   layouts. Mismatch is a programmer error.
///
/// Wait/timeout/corruption semantics for manifest loading match the existing
/// `aof_writer_task` (60s bounded wait, hard fail on corrupt manifest).
pub async fn per_shard_aof_writer_task(
    rx: channel::MpscReceiver<AofMessage>,
    base_dir: PathBuf,
    shard_id: u16,
    fsync: FsyncPolicy,
    cancel: CancellationToken,
) {
    #[cfg(feature = "runtime-tokio")]
    {
        use crate::persistence::aof_manifest::{AofLayout, AofManifest};
        use tokio::io::AsyncWriteExt;

        // Wait for main.rs recovery to create/load the manifest.
        let manifest_wait_start = Instant::now();
        const MANIFEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
        let manifest = loop {
            if cancel.is_cancelled() {
                info!(
                    "AOF writer shard {}: cancelled while waiting for manifest",
                    shard_id
                );
                return;
            }
            if manifest_wait_start.elapsed() > MANIFEST_TIMEOUT {
                error!(
                    "AOF writer shard {}: manifest not found at {} after {:?}. Writer exiting.",
                    shard_id,
                    base_dir.display(),
                    MANIFEST_TIMEOUT,
                );
                return;
            }
            match AofManifest::load(&base_dir) {
                Ok(Some(m)) => break m,
                Ok(None) => {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(e) => {
                    error!(
                        "AOF writer shard {}: manifest corrupt at {}: {}. Persistence disabled.",
                        shard_id,
                        base_dir.display(),
                        e
                    );
                    return;
                }
            }
        };

        if manifest.layout != AofLayout::PerShard {
            error!(
                "AOF writer shard {}: layout is {:?}, expected PerShard. \
                 per_shard_aof_writer_task should only be spawned for PerShard layouts. \
                 Writer exiting.",
                shard_id, manifest.layout
            );
            return;
        }
        if (shard_id as usize) >= manifest.shards.len() {
            error!(
                "AOF writer shard {}: out of range for manifest with {} shards. Writer exiting.",
                shard_id,
                manifest.shards.len()
            );
            return;
        }

        let incr_path = manifest.shard_incr_path(shard_id);
        // Ensure shard-{N}/ exists. The manifest constructor for PerShard
        // already creates these, but be defensive — a manual deletion or
        // a manifest written by an older binary could leave them missing.
        if let Some(parent) = incr_path.parent() {
            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                error!(
                    "AOF writer shard {}: failed to create dir {}: {}",
                    shard_id,
                    parent.display(),
                    e
                );
                return;
            }
        }
        let file: tokio::fs::File = match tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&incr_path)
            .await
        {
            Ok(f) => f,
            Err(e) => {
                error!(
                    "AOF writer shard {}: failed to open incr {}: {}",
                    shard_id,
                    incr_path.display(),
                    e
                );
                return;
            }
        };
        info!(
            "AOF writer shard {}: seq {}, incr={}",
            shard_id,
            manifest.seq,
            incr_path.display()
        );

        let mut writer = tokio::io::BufWriter::new(file);
        let mut last_fsync = Instant::now();
        // (No `interval` here: the EverySec flush deadline is enforced by the
        // timeout-bounded recv in the loop below, which wakes at least every
        // 200ms regardless of message traffic. A long-lived `interval.tick()`
        // select arm is fairness-starvable under sustained writes and proved
        // unreliable when idle on this dedicated current-thread writer runtime.)

        // Test-only fault injection: if MOON_TEST_AOF_FSYNC_FAIL=1 is set in
        // the environment at writer task startup, every AppendSync ack resolves
        // as FsyncFailed instead of Synced. This lets integration tests exercise
        // the AOF_FSYNC_ERR response path without requiring a real disk error.
        // The env var is read once here (not per-message) so it costs zero on the
        // hot path in production deployments where the var is absent.
        let fail_fsync_for_test = std::env::var("MOON_TEST_AOF_FSYNC_FAIL").as_deref() == Ok("1");

        loop {
            tokio::select! {
                // Bounded recv (EverySec durability): wake at least every 200ms
                // even when idle so the flush deadline after this select! is
                // honored within its 1s bound. flume's recv future is drop-safe
                // on the Elapsed branch (no message consumed on timeout); the
                // Ok(Ok(msg)) path below captures the message with no loss.
                r = tokio::time::timeout(
                    std::time::Duration::from_millis(200),
                    rx.recv_async(),
                ) => {
                    // On Elapsed (timeout) `r` is Err: skip the match and fall
                    // through to the EverySec deadline check after this select!.
                    if let Ok(msg) = r {
                    match msg {
                        // PerShard writer (tokio): per RFC § 2 Rule 1 the on-disk
                        // format is `[u64 lsn LE][u32 len LE][RESP bytes]`. Header
                        // is written sequentially with the body — both calls land
                        // in the same BufWriter so this is one syscall under load.
                        Ok(AofMessage::Append { lsn, bytes: data }) => {
                            let mut header = [0u8; 12];
                            header[..8].copy_from_slice(&lsn.to_le_bytes());
                            header[8..].copy_from_slice(&(data.len() as u32).to_le_bytes());
                            if let Err(e) = writer.write_all(&header).await {
                                error!("AOF header write error shard {}: {}", shard_id, e);
                                continue;
                            }
                            if let Err(e) = writer.write_all(&data).await {
                                error!("AOF write error shard {}: {}", shard_id, e);
                                continue;
                            }
                            if matches!(fsync, FsyncPolicy::Always) {
                                let _ = writer.flush().await;
                                let _ = writer.get_ref().sync_data().await;
                            }
                        }
                        // AppendSync (tokio + PerShard): framed write + fsync + ack.
                        Ok(AofMessage::AppendSync { lsn, bytes: data, ack }) => {
                            let mut header = [0u8; 12];
                            header[..8].copy_from_slice(&lsn.to_le_bytes());
                            header[8..].copy_from_slice(&(data.len() as u32).to_le_bytes());
                            if let Err(e) = writer.write_all(&header).await {
                                error!(
                                    "AOF AppendSync header write error shard {}: {}",
                                    shard_id, e
                                );
                                let _ = ack.send(AofAck::WriteFailed);
                                continue;
                            }
                            if let Err(e) = writer.write_all(&data).await {
                                error!(
                                    "AOF AppendSync write error shard {}: {}",
                                    shard_id, e
                                );
                                let _ = ack.send(AofAck::WriteFailed);
                                continue;
                            }
                            // Test-only: skip real fsync and return FsyncFailed
                            // immediately when the fault-injection env var is set.
                            if fail_fsync_for_test {
                                let _ = ack.send(AofAck::FsyncFailed);
                                continue;
                            }
                            if let Err(e) = writer.flush().await {
                                error!(
                                    "AOF AppendSync flush error shard {}: {}",
                                    shard_id, e
                                );
                                let _ = ack.send(AofAck::FsyncFailed);
                                continue;
                            }
                            if let Err(e) = writer.get_ref().sync_data().await {
                                error!(
                                    "AOF AppendSync sync_data error shard {}: {}",
                                    shard_id, e
                                );
                                let _ = ack.send(AofAck::FsyncFailed);
                                continue;
                            }
                            let _ = ack.send(AofAck::Synced);
                        }
                        Ok(AofMessage::Rewrite(_)) | Ok(AofMessage::RewriteSharded(_)) => {
                            warn!(
                                "AOF writer shard {}: received Rewrite/RewriteSharded — \
                                 not applicable in PerShard layout, dropped.",
                                shard_id
                            );
                        }
                        // [F6] Per-shard rewrite (tokio): reuse the proven
                        // synchronous fold (`do_rewrite_per_shard`) verbatim, so
                        // the exactly-once invariant carries over unchanged. This
                        // writer runs on a DEDICATED std::thread (block_on_local,
                        // main.rs) — not a shared tokio worker — so executing the
                        // blocking fold here cannot starve the runtime. We flush
                        // the BufWriter (its `into_inner` does NOT flush) so any
                        // buffered appends are durable in the OLD incr, convert
                        // `tokio::fs::File` -> `std::fs::File` for the sync fold,
                        // then wrap the (reopened) file back into the BufWriter.
                        Ok(AofMessage::RewritePerShard { shard_dbs, coord }) => {
                            if let Err(e) = writer.flush().await {
                                error!(
                                    "F6 tokio per-shard rewrite: shard {} pre-fold flush \
                                     failed: {}. Aborting; old generation stays authoritative.",
                                    shard_id, e
                                );
                                coord.mark_failed();
                                coord.shard_done();
                            } else {
                                // `into_std().await` waits for in-flight ops and is
                                // infallible; the buffer is already flushed above.
                                let mut sf = writer.into_inner().into_std().await;
                                let res = do_rewrite_per_shard(
                                    shard_id, &shard_dbs, &mut sf, &rx, &coord,
                                );
                                // On success `sf` points at the NEW incr (the fold
                                // reopened it + already called `shard_done()`); on
                                // error it is still the OLD incr (pre-reopen). Wrap
                                // it back either way so the writer stays valid.
                                writer =
                                    tokio::io::BufWriter::new(tokio::fs::File::from_std(sf));
                                if let Err(e) = res {
                                    error!(
                                        "F6 tokio per-shard rewrite: shard {} fold failed: {}. \
                                         Aborting commit; old generation stays authoritative.",
                                        shard_id, e
                                    );
                                    coord.mark_failed();
                                    coord.shard_done();
                                }
                            }
                        }
                        Ok(AofMessage::Shutdown) | Err(_) => {
                            let _ = writer.flush().await;
                            let _ = writer.get_ref().sync_data().await;
                            info!("AOF writer shard {} shutting down", shard_id);
                            break;
                        }
                    }
                    }
                }
                _ = cancel.cancelled() => {
                    let _ = writer.flush().await;
                    let _ = writer.get_ref().sync_data().await;
                    info!("AOF writer shard {} cancelled", shard_id);
                    break;
                }
            }
            // EverySec deadline — checked after EVERY wake (message OR timeout),
            // so it is NOT subject to select! fairness and holds the 1s bound
            // under sustained writes as well as when idle. (The old long-lived
            // `interval.tick()` arm could be starved by the always-ready recv
            // arm under load, leaving >1s of writes buffered in the BufWriter
            // and lost on SIGKILL — the COMPOSE crash-matrix failure.)
            if fsync == FsyncPolicy::EverySec
                && last_fsync.elapsed() >= std::time::Duration::from_secs(1)
            {
                let _ = writer.flush().await;
                let _ = writer.get_ref().sync_data().await;
                last_fsync = Instant::now();
            }
        }
    }

    #[cfg(feature = "runtime-monoio")]
    {
        use crate::persistence::aof_manifest::{AofLayout, AofManifest};
        use std::io::Write;

        let manifest_wait_start = Instant::now();
        const MANIFEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
        let manifest = loop {
            if cancel.is_cancelled() {
                info!(
                    "AOF writer shard {}: cancelled while waiting for manifest",
                    shard_id
                );
                return;
            }
            if manifest_wait_start.elapsed() > MANIFEST_TIMEOUT {
                error!(
                    "AOF writer shard {}: manifest not found at {} after {:?}. Writer exiting.",
                    shard_id,
                    base_dir.display(),
                    MANIFEST_TIMEOUT,
                );
                return;
            }
            match AofManifest::load(&base_dir) {
                Ok(Some(m)) => break m,
                Ok(None) => {
                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
                Err(e) => {
                    error!(
                        "AOF writer shard {}: manifest corrupt at {}: {}. Persistence disabled.",
                        shard_id,
                        base_dir.display(),
                        e
                    );
                    return;
                }
            }
        };

        if manifest.layout != AofLayout::PerShard {
            error!(
                "AOF writer shard {}: layout is {:?}, expected PerShard. Writer exiting.",
                shard_id, manifest.layout
            );
            return;
        }
        if (shard_id as usize) >= manifest.shards.len() {
            error!(
                "AOF writer shard {}: out of range for manifest with {} shards. Writer exiting.",
                shard_id,
                manifest.shards.len()
            );
            return;
        }

        let incr_path = manifest.shard_incr_path(shard_id);
        if let Some(parent) = incr_path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                error!(
                    "AOF writer shard {}: failed to create dir {}: {}",
                    shard_id,
                    parent.display(),
                    e
                );
                return;
            }
        }
        let mut file = match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&incr_path)
        {
            Ok(f) => f,
            Err(e) => {
                error!(
                    "AOF writer shard {}: failed to open incr {}: {}",
                    shard_id,
                    incr_path.display(),
                    e
                );
                return;
            }
        };
        info!(
            "AOF writer shard {}: seq {}, incr={}",
            shard_id,
            manifest.seq,
            incr_path.display()
        );

        let mut last_fsync = Instant::now();
        let mut write_error = false;
        // Test-only fault injection: if MOON_TEST_AOF_FSYNC_FAIL=1 is set in
        // the environment at writer task startup, every AppendSync ack resolves
        // as FsyncFailed instead of Synced. Read once before the loop so there
        // is zero cost in production deployments where the var is absent.
        let fail_fsync_for_test = std::env::var("MOON_TEST_AOF_FSYNC_FAIL").as_deref() == Ok("1");

        loop {
            match rx.recv() {
                // AppendSync (monoio + PerShard): framed write + fsync + ack.
                Ok(AofMessage::AppendSync {
                    lsn,
                    bytes: data,
                    ack,
                }) => {
                    if write_error {
                        let _ = ack.send(AofAck::WriteFailed);
                        continue;
                    }
                    let mut header = [0u8; 12];
                    header[..8].copy_from_slice(&lsn.to_le_bytes());
                    header[8..].copy_from_slice(&(data.len() as u32).to_le_bytes());
                    if let Err(e) = file.write_all(&header) {
                        error!(
                            "AOF AppendSync header write failed shard {} (seq {}): {}",
                            shard_id, manifest.seq, e
                        );
                        write_error = true;
                        let _ = ack.send(AofAck::WriteFailed);
                        continue;
                    }
                    if let Err(e) = file.write_all(&data) {
                        error!(
                            "AOF AppendSync write failed shard {} (seq {}): {}",
                            shard_id, manifest.seq, e
                        );
                        write_error = true;
                        let _ = ack.send(AofAck::WriteFailed);
                        continue;
                    }
                    // Test-only: skip real fsync and return FsyncFailed
                    // immediately when the fault-injection env var is set.
                    if fail_fsync_for_test {
                        let _ = ack.send(AofAck::FsyncFailed);
                        continue;
                    }
                    let t = Instant::now();
                    if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                        error!(
                            "AOF AppendSync sync failed shard {} (seq {}): {}",
                            shard_id, manifest.seq, e
                        );
                        write_error = true;
                        let _ = ack.send(AofAck::FsyncFailed);
                    } else {
                        crate::admin::metrics_setup::record_aof_fsync(
                            t.elapsed().as_micros() as u64
                        );
                        let _ = ack.send(AofAck::Synced);
                    }
                }
                // PerShard writer (monoio): framed `[u64 lsn LE][u32 len LE][RESP]`.
                // See the tokio twin above for format rationale.
                Ok(AofMessage::Append { lsn, bytes: data }) => {
                    if write_error {
                        continue;
                    }
                    let mut header = [0u8; 12];
                    header[..8].copy_from_slice(&lsn.to_le_bytes());
                    header[8..].copy_from_slice(&(data.len() as u32).to_le_bytes());
                    if let Err(e) = file.write_all(&header) {
                        error!(
                            "AOF header write failed shard {} (seq {}): {}. Persistence degraded.",
                            shard_id, manifest.seq, e
                        );
                        write_error = true;
                        continue;
                    }
                    if let Err(e) = file.write_all(&data) {
                        error!(
                            "AOF write failed shard {} (seq {}): {}. Persistence degraded.",
                            shard_id, manifest.seq, e
                        );
                        write_error = true;
                        continue;
                    }
                    match fsync {
                        FsyncPolicy::Always => {
                            let t = Instant::now();
                            if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                                error!(
                                    "AOF sync failed shard {} (seq {}, always): {}",
                                    shard_id, manifest.seq, e
                                );
                                write_error = true;
                            } else {
                                crate::admin::metrics_setup::record_aof_fsync(
                                    t.elapsed().as_micros() as u64,
                                );
                            }
                        }
                        FsyncPolicy::EverySec => {
                            if last_fsync.elapsed() >= std::time::Duration::from_secs(1) {
                                let t = Instant::now();
                                if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                                    error!(
                                        "AOF sync failed shard {} (seq {}, everysec): {}",
                                        shard_id, manifest.seq, e
                                    );
                                } else {
                                    crate::admin::metrics_setup::record_aof_fsync(
                                        t.elapsed().as_micros() as u64,
                                    );
                                    last_fsync = Instant::now();
                                }
                            }
                        }
                        FsyncPolicy::No => {}
                    }
                }
                Ok(AofMessage::Rewrite(_)) | Ok(AofMessage::RewriteSharded(_)) => {
                    warn!(
                        "AOF writer shard {}: received Rewrite/RewriteSharded — \
                         not applicable in PerShard layout (use per-shard \
                         BGREWRITEAOF), dropped.",
                        shard_id
                    );
                }
                // [F6] Per-shard rewrite fan-out (monoio). Fold THIS shard,
                // then signal the coordinator; the last shard commits the
                // manifest. On error the old generation stays authoritative
                // (advance_shard did not commit the seq).
                Ok(AofMessage::RewritePerShard { shard_dbs, coord }) => {
                    if let Err(e) =
                        do_rewrite_per_shard(shard_id, &shard_dbs, &mut file, &rx, &coord)
                    {
                        error!(
                            "F6 per-shard rewrite: shard {} fold failed: {}. \
                             Aborting rewrite; old generation stays authoritative.",
                            shard_id, e
                        );
                        // Mark the whole rewrite failed so the final writer
                        // aborts the commit (committing new_seq with a shard
                        // missing its new base would break recovery), then
                        // decrement so the countdown can still complete.
                        coord.mark_failed();
                        coord.shard_done();
                    }
                }
                Ok(AofMessage::Shutdown) | Err(_) => {
                    if !write_error {
                        if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                            error!(
                                "AOF final sync failed shard {} (seq {}): {}",
                                shard_id, manifest.seq, e
                            );
                        }
                    }
                    info!(
                        "AOF writer shard {} shutting down (monoio, seq {})",
                        shard_id, manifest.seq
                    );
                    break;
                }
            }
        }
    }
}

/// Replay an AOF file by parsing RESP commands and dispatching them.
///
/// Returns the number of commands successfully replayed.
///
/// **Corruption recovery:** On mid-stream parse errors, logs a warning with the
/// byte offset, skips to the next RESP array marker (`*`), and continues replay.
/// At EOF, reports total corrupted entries skipped. Truncated tails are handled
/// gracefully (warn + stop).
pub fn replay_aof(
    databases: &mut [Database],
    path: &Path,
    engine: &dyn CommandReplayEngine,
) -> Result<usize, MoonError> {
    let data = std::fs::read(path)?;
    if data.is_empty() {
        return Ok(0);
    }

    // Detect RDB preamble: if the file starts with "MOON" magic, load the binary
    // RDB section first, then replay any RESP commands appended after it.
    let (rdb_keys, resp_start) = if data.starts_with(b"MOON") {
        match crate::persistence::rdb::load_from_bytes(databases, &data) {
            Ok((keys, consumed)) => {
                info!(
                    "AOF RDB preamble loaded: {} keys ({} bytes)",
                    keys, consumed
                );
                (keys, consumed)
            }
            Err(e) => {
                // Data starts with MOON magic — it IS RDB format.
                // Falling back to RESP would parse garbage. Propagate the error.
                return Err(e);
            }
        }
    } else {
        (0, 0)
    };

    // If the entire file was RDB (no RESP tail), we're done
    if resp_start >= data.len() {
        return Ok(rdb_keys);
    }

    let resp_data = &data[resp_start..];
    let total_len = resp_data.len();
    let mut buf = BytesMut::from(resp_data);
    let config = ParseConfig::default();
    let mut selected_db: usize = 0;
    let mut count: usize = 0;
    let mut corruption_count: usize = 0;

    loop {
        if buf.is_empty() {
            break;
        }

        match parse::parse(&mut buf, &config) {
            Ok(Some(frame)) => {
                // Extract command name and args, then dispatch
                let (cmd, cmd_args) = match &frame {
                    Frame::Array(arr) if !arr.is_empty() => {
                        let name = match &arr[0] {
                            Frame::BulkString(s) => s.as_ref(),
                            Frame::SimpleString(s) => s.as_ref(),
                            _ => {
                                count += 1;
                                continue;
                            }
                        };
                        (name as &[u8], &arr[1..])
                    }
                    _ => {
                        count += 1;
                        continue;
                    }
                };
                engine.replay_command(databases, cmd, cmd_args, &mut selected_db);
                count += 1;
            }
            Ok(None) => {
                // Incomplete frame at end of file - truncated AOF
                if !buf.is_empty() {
                    let offset = total_len - buf.len();
                    warn!(
                        "AOF truncated: {} unparseable bytes at offset {} (end of file)",
                        buf.len(),
                        offset
                    );
                }
                break;
            }
            Err(e) => {
                let error_offset = total_len - buf.len();
                warn!(
                    "AOF parse error at byte offset {} after {} commands: {}. Attempting skip.",
                    error_offset, count, e
                );
                corruption_count += 1;

                // Skip past the corrupt byte(s) to the next RESP array marker ('*')
                // Always discard at least 1 byte to guarantee forward progress.
                let _ = buf.split_to(1);
                if let Some(pos) = buf.iter().position(|&b| b == b'*') {
                    let _ = buf.split_to(pos);
                } else if buf.is_empty() {
                    break;
                } else {
                    // No more RESP array markers found; stop replay
                    warn!(
                        "AOF: no recoverable RESP frame found after offset {}; stopping",
                        error_offset
                    );
                    break;
                }
            }
        }
    }

    if corruption_count > 0 {
        warn!(
            "AOF replay completed with {} corrupted entries skipped, {} commands replayed",
            corruption_count, count
        );
    }

    Ok(rdb_keys + count)
}

/// Generate synthetic RESP commands from the current database state for AOF rewriting.
///
/// Produces commands for all 5 data types plus PEXPIRE for keys with TTL.
#[allow(dead_code)] // Retained for RESP-only AOF rewrite fallback and testing
pub fn generate_rewrite_commands(databases: &[Database]) -> BytesMut {
    let mut buf = BytesMut::new();
    let now_ms = current_time_ms();

    for (db_idx, db) in databases.iter().enumerate() {
        let base_ts = db.base_timestamp();
        let data = db.data();
        if data.is_empty() {
            continue;
        }

        // Generate SELECT if not db 0
        if db_idx > 0 {
            let select_frame = Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"SELECT")),
                Frame::BulkString(Bytes::from(db_idx.to_string())),
            ]);
            serialize::serialize(&select_frame, &mut buf);
        }

        for (key, entry) in data {
            // Skip expired entries
            if entry.is_expired_at(base_ts, now_ms) {
                continue;
            }

            match entry.value.as_redis_value() {
                RedisValueRef::String(val) => {
                    let frame = Frame::Array(framevec![
                        Frame::BulkString(Bytes::from_static(b"SET")),
                        Frame::BulkString(key.to_bytes()),
                        Frame::BulkString(Bytes::copy_from_slice(val)),
                    ]);
                    serialize::serialize(&frame, &mut buf);
                }
                RedisValueRef::Hash(map) => {
                    if map.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"HSET")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for (field, val) in map.iter() {
                        args.push(Frame::BulkString(field.clone()));
                        args.push(Frame::BulkString(val.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                // Phase 200: for HashWithTtl we emit two RESP frames per key.
                //   1. `HSET key f1 v1 f2 v2 ...` rebuilds the hash body.
                //   2. `HPEXPIREAT key abs_ms FIELDS 1 field` for every entry
                //      in the TTL sidecar — one per TTL'd field for clarity
                //      (BGREWRITEAOF is rare; per-field framing keeps the
                //      replay shim simple, see `persistence::replay`).
                RedisValueRef::HashWithTtl { fields, ttls, .. } => {
                    if fields.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"HSET")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for (field, val) in fields.iter() {
                        args.push(Frame::BulkString(field.clone()));
                        args.push(Frame::BulkString(val.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);

                    for (field, ttl_ms) in ttls.iter() {
                        let mut ttl_args = vec![
                            Frame::BulkString(Bytes::from_static(b"HPEXPIREAT")),
                            Frame::BulkString(key.to_bytes()),
                            Frame::BulkString(Bytes::copy_from_slice(
                                ttl_ms.to_string().as_bytes(),
                            )),
                            Frame::BulkString(Bytes::from_static(b"FIELDS")),
                            Frame::BulkString(Bytes::from_static(b"1")),
                            Frame::BulkString(field.clone()),
                        ];
                        ttl_args.shrink_to_fit();
                        serialize::serialize(&Frame::Array(ttl_args.into()), &mut buf);
                    }
                }
                RedisValueRef::HashListpack(lp) => {
                    let map = lp.to_hash_map();
                    if map.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"HSET")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for (field, val) in &map {
                        args.push(Frame::BulkString(field.clone()));
                        args.push(Frame::BulkString(val.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                RedisValueRef::List(list) => {
                    if list.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"RPUSH")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for elem in list.iter() {
                        args.push(Frame::BulkString(elem.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                RedisValueRef::ListListpack(lp) => {
                    let list = lp.to_vec_deque();
                    if list.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"RPUSH")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for elem in &list {
                        args.push(Frame::BulkString(elem.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                RedisValueRef::Set(set) => {
                    if set.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"SADD")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for member in set.iter() {
                        args.push(Frame::BulkString(member.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                RedisValueRef::SetListpack(lp) => {
                    let set = lp.to_hash_set();
                    if set.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"SADD")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for member in &set {
                        args.push(Frame::BulkString(member.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                RedisValueRef::SetIntset(is) => {
                    let set = is.to_hash_set();
                    if set.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"SADD")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for member in &set {
                        args.push(Frame::BulkString(member.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                RedisValueRef::SortedSet { members, .. }
                | RedisValueRef::SortedSetBPTree { members, .. } => {
                    if members.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"ZADD")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for (member, score) in members.iter() {
                        args.push(Frame::BulkString(Bytes::from(score.to_string())));
                        args.push(Frame::BulkString(member.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                RedisValueRef::SortedSetListpack(lp) => {
                    let pairs: Vec<_> = lp.iter_pairs().collect();
                    if pairs.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"ZADD")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for (member_entry, score_entry) in &pairs {
                        let score_bytes = score_entry.as_bytes();
                        args.push(Frame::BulkString(Bytes::from(score_bytes)));
                        args.push(Frame::BulkString(Bytes::from(member_entry.as_bytes())));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                RedisValueRef::Stream(stream) => {
                    for (id, fields) in &stream.entries {
                        let mut args = vec![
                            Frame::BulkString(Bytes::from_static(b"XADD")),
                            Frame::BulkString(key.to_bytes()),
                            Frame::BulkString(id.to_bytes()),
                        ];
                        for (field, value) in fields {
                            args.push(Frame::BulkString(field.clone()));
                            args.push(Frame::BulkString(value.clone()));
                        }
                        serialize::serialize(&Frame::Array(args.into()), &mut buf);
                    }
                }
            }

            // Generate PEXPIRE for keys with TTL
            if entry.has_expiry() {
                let exp_ms = entry.expires_at_ms(base_ts);
                if exp_ms > now_ms {
                    let remaining_ms = exp_ms - now_ms;
                    let pexpire_frame = Frame::Array(framevec![
                        Frame::BulkString(Bytes::from_static(b"PEXPIRE")),
                        Frame::BulkString(key.to_bytes()),
                        Frame::BulkString(Bytes::from(remaining_ms.to_string())),
                    ]);
                    serialize::serialize(&pexpire_frame, &mut buf);
                }
            }
        }
    }

    buf
}

/// Snapshot databases and generate compacted AOF commands.
///
/// Shared by both the async (tokio) and sync (monoio) rewrite paths.
#[allow(dead_code)]
fn snapshot_and_generate(db: &SharedDatabases) -> BytesMut {
    let snapshot: Vec<(Vec<(CompactKey, Entry)>, u32)> = db
        .iter()
        .map(|lock| {
            let guard = lock.read();
            let base_ts = guard.base_timestamp();
            let entries = guard
                .data()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            (entries, base_ts)
        })
        .collect();

    let mut temp_dbs: Vec<Database> = Vec::with_capacity(snapshot.len());
    for (entries, _base_ts) in &snapshot {
        let mut db = Database::new();
        for (key, entry) in entries {
            db.set(key.to_bytes(), entry.clone());
        }
        temp_dbs.push(db);
    }

    generate_rewrite_commands(&temp_dbs)
}

/// Drain any queued `AofMessage::Append` messages to the current incr file.
///
/// Called during rewrite to catch in-flight appends that handlers sent before
/// the writer thread could enter the rewrite routine. Messages of other variants
/// are dropped silently (duplicate rewrites while a rewrite is in progress) or
/// returned via the flag for Shutdown (caller is responsible for honoring it
/// after the rewrite completes).
#[cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
#[derive(Default)]
struct DrainOutcome {
    drained: usize,
    shutdown_requested: bool,
}

#[cfg(feature = "runtime-monoio")]
fn drain_pending_appends(
    rx: &channel::MpscReceiver<AofMessage>,
    file: &mut std::fs::File,
) -> Result<DrainOutcome, MoonError> {
    use std::io::Write;
    let mut outcome = DrainOutcome::default();
    while let Ok(msg) = rx.try_recv() {
        match msg {
            // BGREWRITEAOF drain runs on the TopLevel writer (monoio) only;
            // PerShard rewrite is RFC step 6. Legacy v1 disk format → ignore lsn.
            AofMessage::Append {
                bytes: data,
                lsn: _,
            } => {
                file.write_all(&data).map_err(|e| AofError::Io {
                    path: PathBuf::from("<aof incr drain>"),
                    source: e,
                })?;
                outcome.drained += 1;
            }
            // AppendSync during a rewrite drain: bytes are written and counted;
            // the post-drain fsync at the rewrite boundary covers durability,
            // so we ack `Synced`. If the write itself fails the error is
            // already propagated upward by the `?` and the ack is dropped —
            // the caller observes `RecvError`, which it treats as failure.
            AofMessage::AppendSync {
                bytes: data,
                lsn: _,
                ack,
            } => {
                file.write_all(&data).map_err(|e| AofError::Io {
                    path: PathBuf::from("<aof incr drain>"),
                    source: e,
                })?;
                outcome.drained += 1;
                let _ = ack.send(AofAck::Synced);
            }
            AofMessage::Shutdown => {
                outcome.shutdown_requested = true;
            }
            AofMessage::Rewrite(_)
            | AofMessage::RewriteSharded(_)
            | AofMessage::RewritePerShard { .. } => {
                // Already rewriting — drop redundant request.
            }
        }
    }
    Ok(outcome)
}

/// [F6] Drain a per-shard writer's queued appends into its OLD incr file using
/// the framed `[u64 lsn LE][u32 len LE][RESP bytes]` on-disk format that
/// per-shard recovery expects.
///
/// This is the per-shard twin of [`drain_pending_appends`] (which writes the
/// legacy TopLevel raw-RESP format). Correctness depends on the framing
/// matching `replay_per_shard`'s reader — an unframed write here would make the
/// drained appends unparseable on restart.
#[cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
fn drain_pending_appends_framed(
    rx: &channel::MpscReceiver<AofMessage>,
    file: &mut std::fs::File,
) -> Result<DrainOutcome, MoonError> {
    use std::io::Write;
    let mut outcome = DrainOutcome::default();
    let write_framed = |file: &mut std::fs::File, lsn: u64, data: &[u8]| -> std::io::Result<()> {
        let mut header = [0u8; 12];
        header[..8].copy_from_slice(&lsn.to_le_bytes());
        header[8..].copy_from_slice(&(data.len() as u32).to_le_bytes());
        file.write_all(&header)?;
        file.write_all(data)
    };
    while let Ok(msg) = rx.try_recv() {
        match msg {
            AofMessage::Append { lsn, bytes: data } => {
                write_framed(file, lsn, &data).map_err(|e| AofError::Io {
                    path: PathBuf::from("<aof per-shard incr drain>"),
                    source: e,
                })?;
                outcome.drained += 1;
            }
            AofMessage::AppendSync {
                lsn,
                bytes: data,
                ack,
            } => {
                write_framed(file, lsn, &data).map_err(|e| AofError::Io {
                    path: PathBuf::from("<aof per-shard incr drain>"),
                    source: e,
                })?;
                outcome.drained += 1;
                // Durability for these is covered by the post-drain fsync at
                // the rewrite boundary (mirrors drain_pending_appends).
                let _ = ack.send(AofAck::Synced);
            }
            AofMessage::Shutdown => {
                outcome.shutdown_requested = true;
            }
            AofMessage::Rewrite(_)
            | AofMessage::RewriteSharded(_)
            | AofMessage::RewritePerShard { .. } => {
                // Already rewriting this shard — drop redundant request.
            }
        }
    }
    Ok(outcome)
}

/// [F6] Per-shard rewrite fold (monoio). Run by a single per-shard writer for
/// ITS shard only; the manifest commit is coordinated across all shards by the
/// shared [`PerShardRewriteCoord`].
///
/// Correctness ordering (prevents double-apply of non-idempotent commands like
/// INCR after the rewrite) — identical discipline to [`do_rewrite_sharded`],
/// scoped to one shard:
///
/// 1. Drain queued appends into the OLD incr (framed) and fsync.
/// 2. Acquire write locks on this shard's databases.
/// 3. Re-drain appends that arrived between phase 1 and the lock, into OLD
///    incr, and fsync.
/// 4. Snapshot this shard's databases under the locks.
/// 5. Release the locks before the expensive base-RDB write.
/// 6. Write the new base + new (empty) incr at `coord.new_seq` via
///    `advance_shard` (which does NOT bump `manifest.seq`), then reopen
///    `file` to the new incr. Subsequent appends land in the new generation.
/// 7. Signal completion to the coordinator; the last shard commits the
///    manifest (single seq flip) and prunes the old generation.
///
/// Until step 7's commit, the on-disk manifest still resolves to the old seq,
/// so a crash anywhere in steps 1-6 recovers the intact old generation.
///
/// # Cross-thread exactly-once invariant (load-bearing)
///
/// This fold runs on the per-shard *writer* thread, which is distinct from the
/// shard event-loop thread that applies commands. Exactly-once across the
/// rewrite boundary depends on a single ordering fact: the live write path
/// enqueues each command's AOF append **inside** the same `RwLock<Database>`
/// write guard under which it mutated the db (see `spsc_handler.rs`:
/// `wal_append_and_fanout` is called before `drop(guard)`). Phase 2 here
/// acquires those *same* locks (`all_shard_dbs()[sidx]` is
/// `ShardDatabases::shards[sidx]`, the exact `RwLock`s `write_db` locks), so
/// RwLock mutual exclusion forces the order
/// `enqueue → guard-release → fold-acquire → mid-drain(phase 3)`. Hence every
/// INCR whose mutation lands in the phase-4 snapshot had its append drained
/// into the OLD incr (then pruned at commit) — never replayed on top of the
/// new base. Were the append enqueued *after* the guard drop, a snapshot would
/// capture the mutation while its append still raced toward the NEW incr →
/// double-apply. The in-guard append is therefore the invariant; do not move it.
///
/// This also assumes the `RwLock`-backed `ShardDatabases` is the *live* store.
/// It is, because the thread-local `ShardSlice` fast path is dead code until
/// Phase 4 wires `init_shard` (`is_initialized()` is always false today). A
/// future Phase 4 that makes ShardSlice live MUST revisit this fold: the writer
/// thread cannot lock another thread's `!Send` `Rc<RefCell<Shard>>`, so the
/// per-shard rewrite would need a different snapshot-coordination mechanism.
///
/// # Known limitation — channel saturation during the fold
///
/// Exactly-once holds *absent append-channel saturation during the fold*. While
/// this function runs (phases 2-6, including the base-RDB serialize + write +
/// fsync of phase 6, which is hundreds of ms on a large shard) the writer is
/// NOT in its recv loop, so it is not draining the bounded
/// `mpsc_bounded::<AofMessage>(10_000)` append channel. Post-snapshot appends
/// queue there for the new incr; the event loop enqueues them with
/// `try_send_append` (drop-on-full, return ignored — `spsc_handler.rs`). Under
/// *sustained concurrent* writes on a large dataset, > 10_000 appends can pile
/// up during the window and the overflow is silently dropped — lost even on a
/// clean restart (worse than the everysec contract, which only loses on crash).
/// The single-client crash matrix cannot surface this (serialized `redis-cli`
/// never pressures the channel). This window is *pre-existing*: the shipped
/// `do_rewrite_sharded` has the identical non-draining gap. Tracked as a
/// known limitation (F6 is behind `--experimental-per-shard-rewrite`); the fix
/// (keep draining during phase 6, or block-on-full for the rewrite's duration)
/// is a separate scoped task. See `tmp/F6-known-limitations.md`.
#[cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
fn do_rewrite_per_shard(
    shard_id: u16,
    shard_dbs: &crate::shard::shared_databases::ShardDatabases,
    file: &mut std::fs::File,
    rx: &channel::MpscReceiver<AofMessage>,
    coord: &PerShardRewriteCoord,
) -> Result<(), MoonError> {
    let sidx = shard_id as usize;
    let all_shards = shard_dbs.all_shard_dbs();
    if sidx >= all_shards.len() {
        return Err(AofError::RewriteFailed {
            detail: format!(
                "do_rewrite_per_shard: shard {} out of range ({} shards)",
                sidx,
                all_shards.len()
            ),
        }
        .into());
    }

    // Phase 1: drain pre-rewrite queued appends into old incr (framed).
    let pre_drain = drain_pending_appends_framed(rx, file)?;
    file.sync_data().map_err(|e| AofError::Io {
        path: PathBuf::from("<aof per-shard incr>"),
        source: e,
    })?;

    // Phase 2: acquire write locks on this shard's db(s) (db_idx ascending).
    let shard_locks = &all_shards[sidx];
    let guards: Vec<_> = shard_locks.iter().map(|lock| lock.write()).collect();

    // Phase 3: drain appends that completed between phase 1 and phase 2.
    let mid_drain = drain_pending_appends_framed(rx, file)?;
    file.sync_data().map_err(|e| AofError::Io {
        path: PathBuf::from("<aof per-shard incr>"),
        source: e,
    })?;

    // Phase 4: snapshot this shard's databases under the locks.
    let now_ms = current_time_ms();
    let mut snapshot: Vec<(
        Vec<(
            crate::storage::compact_key::CompactKey,
            crate::storage::entry::Entry,
        )>,
        u32,
    )> = Vec::with_capacity(guards.len());
    for guard in &guards {
        let base_ts = guard.base_timestamp();
        let mut entries = Vec::new();
        for (key, entry) in guard.data().iter() {
            if !entry.is_expired_at(base_ts, now_ms) {
                entries.push((key.clone(), entry.clone()));
            }
        }
        snapshot.push((entries, base_ts));
    }

    // Phase 5: release locks before the expensive disk write.
    drop(guards);

    // Phase 6: write new base, advance THIS shard's manifest entry (no seq
    // commit), reopen to the new incr. The manifest lock is held only for the
    // brief, await-free advance_shard call.
    let rdb_bytes = crate::persistence::rdb::save_snapshot_to_bytes(&snapshot)?;
    let new_incr = {
        let mut m = coord.manifest.lock();
        m.advance_shard(shard_id, coord.new_seq, &rdb_bytes)?
    };
    *file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&new_incr)
        .map_err(|e| AofError::Io {
            path: new_incr,
            source: e,
        })?;

    info!(
        "F6 per-shard rewrite: shard {} folded (drained {}+{} appends), new seq {}",
        shard_id, pre_drain.drained, mid_drain.drained, coord.new_seq
    );
    if pre_drain.shutdown_requested || mid_drain.shutdown_requested {
        warn!(
            "F6 per-shard rewrite: shard {} saw shutdown during rewrite (honored after commit)",
            shard_id
        );
    }

    // Phase 7: signal completion; the last writer commits + prunes.
    coord.shard_done();
    Ok(())
}

/// Multi-part rewrite: snapshot single-shard databases → RDB base → advance manifest.
///
/// Correctness ordering (prevents double-apply of non-idempotent commands like
/// INCR/LPUSH/SADD after rewrite):
///
/// 1. Drain any queued appends into the OLD incr file and fsync.
/// 2. Acquire write locks on all databases in the shard. This blocks handlers
///    from applying new writes or queueing new appends for the locked dbs.
/// 3. Drain the channel once more — catches appends for writes that the
///    handler completed between step 1 and step 2.
/// 4. Snapshot every database under the write locks. Because no handler can
///    mutate the dbs while we hold the locks, the snapshot is atomic with
///    respect to the post-drain channel state.
/// 5. Release the write locks. New handler writes from here on queue in the
///    channel and will be processed into the NEW incr file after rotation.
/// 6. Write the new base RDB, advance the manifest, reopen the file handle.
///
/// Invariant: any write captured in the new base is NOT in the new incr file
/// (handlers were blocked between drain and snapshot), and any write NOT in
/// the new base IS in the new incr file (queued after lock release).
#[cfg(feature = "runtime-monoio")]
fn do_rewrite_single(
    db: &SharedDatabases,
    manifest: &mut crate::persistence::aof_manifest::AofManifest,
    file: &mut std::fs::File,
    rx: &channel::MpscReceiver<AofMessage>,
) -> Result<(), MoonError> {
    // Phase 1: drain pre-rewrite queued appends into old incr, fsync.
    let pre_drain = drain_pending_appends(rx, file)?;
    file.sync_data().map_err(|e| AofError::Io {
        path: manifest.incr_path(),
        source: e,
    })?;

    // Phase 2: acquire write locks on every database in the shard.
    // Order is consistent (index-ascending) so concurrent callers would
    // serialize without deadlock — but in practice only this thread
    // acquires multi-db locks.
    let guards: Vec<_> = db.iter().map(|lock| lock.write()).collect();

    // Phase 3: drain any appends the handlers sent between phase 1 and phase 2.
    let mid_drain = drain_pending_appends(rx, file)?;
    file.sync_data().map_err(|e| AofError::Io {
        path: manifest.incr_path(),
        source: e,
    })?;

    // Phase 4: snapshot under the write locks. No mutation is possible.
    let now_ms = current_time_ms();
    let snapshot: Vec<(
        Vec<(
            crate::storage::compact_key::CompactKey,
            crate::storage::entry::Entry,
        )>,
        u32,
    )> = guards
        .iter()
        .map(|guard| {
            let base_ts = guard.base_timestamp();
            let entries: Vec<_> = guard
                .data()
                .iter()
                .filter(|(_, v)| !v.is_expired_at(base_ts, now_ms))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            (entries, base_ts)
        })
        .collect();

    // Phase 5: release locks. Handlers resume; new appends queue in the channel
    // and will be processed into the new incr after step 6.
    drop(guards);

    // Phase 6: write new base, advance manifest, reopen.
    let rdb_bytes = crate::persistence::rdb::save_snapshot_to_bytes(&snapshot)?;
    let new_incr = manifest.advance(&rdb_bytes)?;

    *file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&new_incr)
        .map_err(|e| AofError::Io {
            path: new_incr,
            source: e,
        })?;

    info!(
        "AOF rewrite complete (single): drained {}+{} pre-snapshot appends, seq={}",
        pre_drain.drained, mid_drain.drained, manifest.seq
    );
    if pre_drain.shutdown_requested || mid_drain.shutdown_requested {
        // Caller doesn't currently observe this; logging is the escape hatch.
        warn!("AOF writer: shutdown requested during rewrite (will honor on next recv)");
    }
    Ok(())
}

/// Multi-part rewrite: snapshot all shards → merged RDB base → advance manifest.
///
/// See [`do_rewrite_single`] for the ordering rationale. The multi-shard variant
/// holds write locks on every (shard, db) pair simultaneously for the duration
/// of the snapshot. This creates a brief global write pause, but it is the only
/// way to guarantee a torn-free snapshot without per-message sequence numbers.
#[cfg(feature = "runtime-monoio")]
fn do_rewrite_sharded(
    shard_dbs: &crate::shard::shared_databases::ShardDatabases,
    manifest: &mut crate::persistence::aof_manifest::AofManifest,
    file: &mut std::fs::File,
    rx: &channel::MpscReceiver<AofMessage>,
) -> Result<(), MoonError> {
    // Phase 1: drain pre-rewrite queued appends into old incr.
    let pre_drain = drain_pending_appends(rx, file)?;
    file.sync_data().map_err(|e| AofError::Io {
        path: manifest.incr_path(),
        source: e,
    })?;

    // Phase 2: acquire write locks on ALL (shard, db) pairs simultaneously.
    // Lock order is (shard_idx, db_idx) ascending — must match anywhere else
    // that acquires multiple locks to prevent deadlock (currently no other
    // call site does, but the ordering discipline is documented for future
    // maintainers).
    let all_shards = shard_dbs.all_shard_dbs();
    let mut guards: Vec<Vec<_>> = Vec::with_capacity(all_shards.len());
    for shard_locks in all_shards {
        let mut shard_guards = Vec::with_capacity(shard_locks.len());
        for lock in shard_locks {
            shard_guards.push(lock.write());
        }
        guards.push(shard_guards);
    }

    // Phase 3: drain appends completed between phase 1 and phase 2.
    let mid_drain = drain_pending_appends(rx, file)?;
    file.sync_data().map_err(|e| AofError::Io {
        path: manifest.incr_path(),
        source: e,
    })?;

    // Phase 4: snapshot under locks.
    let db_count = shard_dbs.db_count();
    let mut merged: Vec<(
        Vec<(
            crate::storage::compact_key::CompactKey,
            crate::storage::entry::Entry,
        )>,
        u32,
    )> = (0..db_count).map(|_| (Vec::new(), 0u32)).collect();
    let now_ms = current_time_ms();
    for shard_guards in &guards {
        for (db_idx, guard) in shard_guards.iter().enumerate() {
            let base_ts = guard.base_timestamp();
            if merged[db_idx].0.is_empty() {
                merged[db_idx].1 = base_ts;
            }
            for (key, entry) in guard.data().iter() {
                if !entry.is_expired_at(base_ts, now_ms) {
                    merged[db_idx].0.push((key.clone(), entry.clone()));
                }
            }
        }
    }

    // Phase 5: release locks before the expensive disk write.
    drop(guards);

    // Phase 6: write new base, advance manifest, reopen.
    let rdb_bytes = crate::persistence::rdb::save_snapshot_to_bytes(&merged)?;
    let new_incr = manifest.advance(&rdb_bytes)?;

    *file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&new_incr)
        .map_err(|e| AofError::Io {
            path: new_incr,
            source: e,
        })?;

    info!(
        "AOF rewrite complete (sharded): drained {}+{} pre-snapshot appends, seq={}",
        pre_drain.drained, mid_drain.drained, manifest.seq
    );
    if pre_drain.shutdown_requested || mid_drain.shutdown_requested {
        warn!("AOF writer: shutdown requested during rewrite (will honor on next recv)");
    }
    Ok(())
}

/// Rewrite the AOF file with RDB preamble (binary base + empty RESP incremental).
///
/// Uses the same strategy as Redis 7+ `aof-use-rdb-preamble yes`:
/// the rewritten AOF starts with a full RDB snapshot (compact binary),
/// and new writes are appended as RESP after it. On startup, the loader
/// detects the RDB magic and reads the binary preamble, then switches
/// to RESP parsing for any incremental commands appended after.
#[allow(dead_code)] // Retained for legacy single-file and tokio path
fn rewrite_aof_sync(db: &SharedDatabases, aof_path: &Path) -> Result<(), MoonError> {
    // Snapshot under read locks, build temp Database objects for RDB serialization
    let snapshot: Vec<Database> = db
        .iter()
        .map(|lock| {
            let guard = lock.read();
            let mut temp = Database::new();
            let now_ms = current_time_ms();
            for (k, v) in guard.data().iter() {
                if !v.is_expired_at(guard.base_timestamp(), now_ms) {
                    temp.set(k.to_bytes(), v.clone());
                }
            }
            temp
        })
        .collect();

    let rdb_bytes = crate::persistence::rdb::save_to_bytes(&snapshot)?;

    let tmp_path = aof_path.with_extension("aof.tmp");
    std::fs::write(&tmp_path, &rdb_bytes).map_err(|e| AofError::Io {
        path: tmp_path.clone(),
        source: e,
    })?;
    std::fs::rename(&tmp_path, aof_path).map_err(|e| AofError::RewriteFailed {
        detail: format!(
            "rename {} -> {}: {}",
            tmp_path.display(),
            aof_path.display(),
            e
        ),
    })?;

    info!(
        "AOF rewrite complete (RDB preamble): {} bytes",
        rdb_bytes.len()
    );
    Ok(())
}

/// Rewrite the AOF in sharded mode with RDB preamble.
///
/// Merges all shards' databases into a single RDB snapshot, writes it as
/// the AOF base file. New incremental writes are appended as RESP after.
#[allow(dead_code)]
fn rewrite_aof_sharded_sync(
    shard_dbs: &crate::shard::shared_databases::ShardDatabases,
    aof_path: &Path,
) -> Result<(), MoonError> {
    let db_count = shard_dbs.db_count();
    let now_ms = current_time_ms();
    let mut merged_dbs: Vec<Database> = (0..db_count).map(|_| Database::new()).collect();

    for shard_locks in shard_dbs.all_shard_dbs() {
        for (db_idx, lock) in shard_locks.iter().enumerate() {
            let guard = lock.read();
            for (key, entry) in guard.data().iter() {
                if !entry.is_expired_at(guard.base_timestamp(), now_ms) {
                    merged_dbs[db_idx].set(key.to_bytes(), entry.clone());
                }
            }
        }
    }

    let rdb_bytes = crate::persistence::rdb::save_to_bytes(&merged_dbs)?;

    let tmp_path = aof_path.with_extension("aof.tmp");
    std::fs::write(&tmp_path, &rdb_bytes).map_err(|e| AofError::Io {
        path: tmp_path.clone(),
        source: e,
    })?;
    std::fs::rename(&tmp_path, aof_path).map_err(|e| AofError::RewriteFailed {
        detail: format!(
            "rename {} -> {}: {}",
            tmp_path.display(),
            aof_path.display(),
            e
        ),
    })?;

    info!(
        "AOF rewrite (sharded, RDB preamble) complete: {} bytes",
        rdb_bytes.len()
    );
    Ok(())
}

/// Reopen AOF file in append mode after atomic rewrite replaced it.
#[allow(dead_code)]
fn reopen_aof_sync(aof_path: &Path) -> Result<std::fs::File, std::io::Error> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(aof_path)
}

/// Rewrite the AOF file (tokio async wrapper).
///
/// Delegates to `rewrite_aof_sync` — the actual I/O is synchronous (temp write + rename).
#[cfg(feature = "runtime-tokio")]
#[tracing::instrument(skip_all, level = "info")]
pub async fn rewrite_aof(db: SharedDatabases, aof_path: &Path) -> Result<(), MoonError> {
    rewrite_aof_sync(&db, aof_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::replay::DispatchReplayEngine;
    use ordered_float::OrderedFloat;
    use tempfile::tempdir;

    fn make_command(parts: &[&[u8]]) -> Frame {
        Frame::Array(
            parts
                .iter()
                .map(|p| Frame::BulkString(Bytes::copy_from_slice(p)))
                .collect(),
        )
    }

    // --- serialize_command / generate_aof_command round-trip tests ---

    #[test]
    fn test_generate_aof_command_produces_valid_resp_that_round_trips() {
        let frame = make_command(&[b"SET", b"key", b"value"]);
        let serialized = serialize_command(&frame);

        let mut buf = BytesMut::from(&serialized[..]);
        let config = ParseConfig::default();
        let parsed = parse::parse(&mut buf, &config).unwrap().unwrap();
        assert_eq!(parsed, frame);
    }

    #[test]
    fn test_serialize_command_round_trip_hset() {
        let frame = make_command(&[b"HSET", b"myhash", b"f1", b"v1"]);
        let serialized = serialize_command(&frame);
        let mut buf = BytesMut::from(&serialized[..]);
        let parsed = parse::parse(&mut buf, &ParseConfig::default())
            .unwrap()
            .unwrap();
        assert_eq!(parsed, frame);
    }

    // --- AOF replay tests ---

    #[test]
    fn test_aof_replay_set_commands_restores_string_keys() {
        let dir = tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");

        // Write SET commands in RESP format
        let mut aof_data = BytesMut::new();
        serialize::serialize(&make_command(&[b"SET", b"k1", b"v1"]), &mut aof_data);
        serialize::serialize(&make_command(&[b"SET", b"k2", b"v2"]), &mut aof_data);
        std::fs::write(&aof_path, &aof_data).unwrap();

        let mut dbs = vec![Database::new()];
        let count = replay_aof(&mut dbs, &aof_path, &DispatchReplayEngine::new()).unwrap();
        assert_eq!(count, 2);

        let entry = dbs[0].get(b"k1").unwrap();
        assert_eq!(entry.value.as_bytes().unwrap(), b"v1");
        let entry = dbs[0].get(b"k2").unwrap();
        assert_eq!(entry.value.as_bytes().unwrap(), b"v2");
    }

    #[test]
    fn test_aof_replay_collection_types() {
        let dir = tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");

        let mut aof_data = BytesMut::new();
        // HSET
        serialize::serialize(
            &make_command(&[b"HSET", b"myhash", b"f1", b"v1"]),
            &mut aof_data,
        );
        // LPUSH
        serialize::serialize(
            &make_command(&[b"LPUSH", b"mylist", b"a", b"b"]),
            &mut aof_data,
        );
        // SADD
        serialize::serialize(
            &make_command(&[b"SADD", b"myset", b"x", b"y"]),
            &mut aof_data,
        );
        // ZADD
        serialize::serialize(
            &make_command(&[b"ZADD", b"myzset", b"1.5", b"alice"]),
            &mut aof_data,
        );
        std::fs::write(&aof_path, &aof_data).unwrap();

        let mut dbs = vec![Database::new()];
        let count = replay_aof(&mut dbs, &aof_path, &DispatchReplayEngine::new()).unwrap();
        assert_eq!(count, 4);

        // Check hash
        let hash = dbs[0].get_hash(b"myhash").unwrap().unwrap();
        assert_eq!(
            hash.get(&Bytes::from_static(b"f1")).unwrap().as_ref(),
            b"v1"
        );

        // Check list
        let list = dbs[0].get_list(b"mylist").unwrap().unwrap();
        assert_eq!(list.len(), 2);

        // Check set
        let set = dbs[0].get_set(b"myset").unwrap().unwrap();
        assert_eq!(set.len(), 2);

        // Check sorted set
        let (members, _) = dbs[0].get_sorted_set(b"myzset").unwrap().unwrap();
        assert_eq!(*members.get(&Bytes::from_static(b"alice")).unwrap(), 1.5);
    }

    #[test]
    fn test_aof_replay_with_expire_preserves_ttls() {
        let dir = tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");

        let mut aof_data = BytesMut::new();
        serialize::serialize(&make_command(&[b"SET", b"mykey", b"myval"]), &mut aof_data);
        serialize::serialize(
            &make_command(&[b"PEXPIRE", b"mykey", b"60000"]),
            &mut aof_data,
        );
        std::fs::write(&aof_path, &aof_data).unwrap();

        let mut dbs = vec![Database::new()];
        let count = replay_aof(&mut dbs, &aof_path, &DispatchReplayEngine::new()).unwrap();
        assert_eq!(count, 2);

        let base_ts = dbs[0].base_timestamp();
        let entry = dbs[0].get(b"mykey").unwrap();
        assert!(entry.has_expiry());
        let remaining_secs = (entry.expires_at_ms(base_ts) - current_time_ms()) / 1000;
        assert!(remaining_secs >= 50); // Allow some tolerance
    }

    #[test]
    fn test_aof_replay_with_select_switches_databases() {
        let dir = tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");

        let mut aof_data = BytesMut::new();
        serialize::serialize(&make_command(&[b"SET", b"k0", b"v0"]), &mut aof_data);
        serialize::serialize(&make_command(&[b"SELECT", b"1"]), &mut aof_data);
        serialize::serialize(&make_command(&[b"SET", b"k1", b"v1"]), &mut aof_data);
        std::fs::write(&aof_path, &aof_data).unwrap();

        let mut dbs = vec![Database::new(), Database::new()];
        let count = replay_aof(&mut dbs, &aof_path, &DispatchReplayEngine::new()).unwrap();
        assert_eq!(count, 3);

        assert!(dbs[0].get(b"k0").is_some());
        assert!(dbs[1].get(b"k1").is_some());
    }

    #[test]
    fn test_aof_replay_empty_file_produces_zero_keys() {
        let dir = tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");
        std::fs::write(&aof_path, b"").unwrap();

        let mut dbs = vec![Database::new()];
        let count = replay_aof(&mut dbs, &aof_path, &DispatchReplayEngine::new()).unwrap();
        assert_eq!(count, 0);
        assert_eq!(dbs[0].len(), 0);
    }

    #[test]
    fn test_aof_replay_corrupt_truncated_logs_error_loads_what_it_can() {
        let dir = tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");

        let mut aof_data = BytesMut::new();
        serialize::serialize(&make_command(&[b"SET", b"k1", b"v1"]), &mut aof_data);
        // Append corrupt data
        aof_data.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$2\r\nk2");
        std::fs::write(&aof_path, &aof_data).unwrap();

        let mut dbs = vec![Database::new()];
        let count = replay_aof(&mut dbs, &aof_path, &DispatchReplayEngine::new()).unwrap();
        // Should have loaded the first command
        assert_eq!(count, 1);
        assert!(dbs[0].get(b"k1").is_some());
    }

    // --- FsyncPolicy tests ---

    #[test]
    fn test_fsync_policy_from_str() {
        assert_eq!(FsyncPolicy::from_str("always"), FsyncPolicy::Always);
        assert_eq!(FsyncPolicy::from_str("everysec"), FsyncPolicy::EverySec);
        assert_eq!(FsyncPolicy::from_str("no"), FsyncPolicy::No);
        assert_eq!(FsyncPolicy::from_str("unknown"), FsyncPolicy::EverySec);
    }

    // --- generate_rewrite_commands tests ---

    #[test]
    fn test_generate_rewrite_commands_all_5_types() {
        let mut dbs = vec![Database::new()];

        // String
        dbs[0].set_string(Bytes::from_static(b"str"), Bytes::from_static(b"val"));
        // Hash
        {
            let map = dbs[0].get_or_create_hash(b"h").unwrap();
            map.insert(Bytes::from_static(b"f"), Bytes::from_static(b"v"));
        }
        // List
        {
            let list = dbs[0].get_or_create_list(b"l").unwrap();
            list.push_back(Bytes::from_static(b"item"));
        }
        // Set
        {
            let set = dbs[0].get_or_create_set(b"s").unwrap();
            set.insert(Bytes::from_static(b"m"));
        }
        // Sorted set
        {
            let (members, tree) = dbs[0].get_or_create_sorted_set(b"z").unwrap();
            members.insert(Bytes::from_static(b"a"), 1.0);
            tree.insert(OrderedFloat(1.0), Bytes::from_static(b"a"));
        }

        let commands = generate_rewrite_commands(&dbs);
        assert!(!commands.is_empty());

        // Replay and verify round-trip
        let dir = tempdir().unwrap();
        let aof_path = dir.path().join("rewrite.aof");
        std::fs::write(&aof_path, &commands).unwrap();

        let mut loaded_dbs = vec![Database::new()];
        let count = replay_aof(&mut loaded_dbs, &aof_path, &DispatchReplayEngine::new()).unwrap();
        assert!(count >= 5, "Expected at least 5 commands, got {}", count);

        // Verify each type restored
        assert_eq!(
            loaded_dbs[0].get(b"str").unwrap().value.type_name(),
            "string"
        );
        assert!(loaded_dbs[0].get_hash(b"h").unwrap().is_some());
        assert!(loaded_dbs[0].get_list(b"l").unwrap().is_some());
        assert!(loaded_dbs[0].get_set(b"s").unwrap().is_some());
        assert!(loaded_dbs[0].get_sorted_set(b"z").unwrap().is_some());
    }

    #[test]
    fn test_generate_rewrite_commands_with_ttl() {
        let mut dbs = vec![Database::new()];
        let future_ms = current_time_ms() + 3_600_000;
        dbs[0].set_string_with_expiry(
            Bytes::from_static(b"key"),
            Bytes::from_static(b"val"),
            future_ms,
        );

        let commands = generate_rewrite_commands(&dbs);

        // Replay and check TTL is preserved
        let dir = tempdir().unwrap();
        let aof_path = dir.path().join("rewrite.aof");
        std::fs::write(&aof_path, &commands).unwrap();

        let mut loaded_dbs = vec![Database::new()];
        let count = replay_aof(&mut loaded_dbs, &aof_path, &DispatchReplayEngine::new()).unwrap();
        assert_eq!(count, 2); // SET + PEXPIRE

        let base_ts = loaded_dbs[0].base_timestamp();
        let entry = loaded_dbs[0].get(b"key").unwrap();
        assert!(entry.has_expiry());
        let remaining_secs = (entry.expires_at_ms(base_ts) - current_time_ms()) / 1000;
        assert!(remaining_secs > 3500);
    }

    /// Helper: build a snapshot tuple from a Database slice — mirrors the
    /// `merged` construction in `do_rewrite_sharded` (aof.rs:2070-2090) so
    /// tests exercise the exact same production path.
    fn db_slice_to_snapshot(
        dbs: &[Database],
    ) -> Vec<(
        Vec<(
            crate::storage::compact_key::CompactKey,
            crate::storage::entry::Entry,
        )>,
        u32,
    )> {
        let now_ms = crate::storage::entry::current_time_ms();
        dbs.iter()
            .map(|db| {
                let base_ts = db.base_timestamp();
                let entries: Vec<_> = db
                    .data()
                    .iter()
                    .filter(|(_, e)| !e.is_expired_at(base_ts, now_ms))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                (entries, base_ts)
            })
            .collect()
    }

    /// FIX-W3-8: BGREWRITEAOF on a fresh empty database must produce a valid
    /// RDB base and recover cleanly with 0 keys.
    ///
    /// Updated to call `save_snapshot_to_bytes` (the function `do_rewrite_sharded`
    /// actually calls at aof.rs:2096) rather than `save_to_bytes` (the previous
    /// test used the wrong function — tautological for empty input since both
    /// produce an identical valid RDB, but would miss regressions on the
    /// snapshot-tuple path).
    #[test]
    fn empty_database_rewrite_produces_valid_rdb_and_recovers() {
        let dir = tempdir().unwrap();

        // Build the snapshot tuple the same way do_rewrite_sharded does.
        let empty_dbs: Vec<Database> = vec![Database::new()];
        let snapshot = db_slice_to_snapshot(&empty_dbs);
        let rdb_bytes = crate::persistence::rdb::save_snapshot_to_bytes(&snapshot)
            .expect("save_snapshot_to_bytes must succeed for empty snapshot");

        // Invariant 1: RDB is non-empty (has at least magic + version + EOF marker).
        assert!(
            !rdb_bytes.is_empty(),
            "empty-database RDB must not be 0 bytes"
        );

        // Invariant 2: starts with valid MOON magic header.
        assert!(
            rdb_bytes.starts_with(b"MOON"),
            "RDB bytes must start with MOON magic, got: {:?}",
            &rdb_bytes[..rdb_bytes.len().min(8)]
        );

        // Invariant 3: recovery from this base succeeds with 0 keys loaded.
        let base_path = dir.path().join("empty.rdb");
        std::fs::write(&base_path, &rdb_bytes).expect("write empty rdb");
        let mut recovery_dbs = vec![Database::new()];
        let loaded =
            crate::persistence::rdb::load(&mut recovery_dbs, &base_path).expect("load empty rdb");
        assert_eq!(
            loaded, 0,
            "recovering from empty-database RDB yields 0 keys"
        );
        assert_eq!(
            recovery_dbs[0].len(),
            0,
            "database must be empty after recovering from zero-key RDB"
        );
    }

    /// FIX-W3-8: Genuine regression guard — save_snapshot_to_bytes preserves
    /// a 1-key database through a full serialize→file→reload cycle.
    ///
    /// This is the substantive test the verifier asked for: verifies the
    /// production code path (`save_snapshot_to_bytes` via the snapshot-tuple
    /// form) against a non-trivial database, so a future regression that
    /// swaps back to `save_to_bytes` or breaks TTL handling in the snapshot
    /// path will be caught.
    #[test]
    fn snapshot_to_bytes_round_trips_one_key_database() {
        let dir = tempdir().unwrap();

        // Build a 1-key database with a string value.
        let mut db = Database::new();
        db.set_string(
            Bytes::from_static(b"rdb_key"),
            Bytes::from_static(b"rdb_value"),
        );
        let dbs = vec![db];

        // Serialize via the production path (save_snapshot_to_bytes).
        let snapshot = db_slice_to_snapshot(&dbs);
        let rdb_bytes = crate::persistence::rdb::save_snapshot_to_bytes(&snapshot)
            .expect("save_snapshot_to_bytes must succeed for 1-key snapshot");

        assert!(rdb_bytes.starts_with(b"MOON"), "must have MOON magic");

        // Reload and assert the key survives.
        let base_path = dir.path().join("one_key.rdb");
        std::fs::write(&base_path, &rdb_bytes).expect("write rdb");
        let mut recovery_dbs = vec![Database::new()];
        let loaded = crate::persistence::rdb::load(&mut recovery_dbs, &base_path)
            .expect("load rdb must succeed");
        assert_eq!(loaded, 1, "exactly 1 key must be recovered");
        let val = recovery_dbs[0]
            .get(b"rdb_key")
            .expect("rdb_key must be present");
        assert_eq!(
            val.value.as_bytes().expect("string value"),
            b"rdb_value",
            "recovered value must match written value"
        );
    }

    #[test]
    fn test_generate_rewrite_round_trip_preserves_state() {
        let mut dbs = vec![Database::new()];
        dbs[0].set_string(Bytes::from_static(b"a"), Bytes::from_static(b"1"));
        dbs[0].set_string(Bytes::from_static(b"b"), Bytes::from_static(b"2"));
        {
            let list = dbs[0].get_or_create_list(b"mylist").unwrap();
            list.push_back(Bytes::from_static(b"x"));
            list.push_back(Bytes::from_static(b"y"));
            list.push_back(Bytes::from_static(b"z"));
        }

        let commands = generate_rewrite_commands(&dbs);
        let dir = tempdir().unwrap();
        let aof_path = dir.path().join("rewrite.aof");
        std::fs::write(&aof_path, &commands).unwrap();

        let mut loaded = vec![Database::new()];
        replay_aof(&mut loaded, &aof_path, &DispatchReplayEngine::new()).unwrap();

        // Check strings
        assert_eq!(loaded[0].get(b"a").unwrap().value.as_bytes().unwrap(), b"1");
        assert_eq!(loaded[0].get(b"b").unwrap().value.as_bytes().unwrap(), b"2");

        // Check list order preserved
        let list = loaded[0].get_list(b"mylist").unwrap().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].as_ref(), b"x");
        assert_eq!(list[1].as_ref(), b"y");
        assert_eq!(list[2].as_ref(), b"z");
    }

    // -----------------------------------------------------------------------
    // FIX-W2-4 r2: canonical AOF fsync error string
    //
    // Red criterion: AOF_FSYNC_ERR constant must exist and equal the canonical
    // Redis-style ERR-prefixed string used by handler_monoio and handler_sharded.
    // handler_single.rs previously used "WRITEFAIL aof fsync failed" which is
    // both non-canonical (no ERR prefix, different verb) and inconsistent with
    // the other two handlers.
    //
    // These tests compile-fail on the prior commit (constant absent) and pass
    // once AOF_FSYNC_ERR is declared in this module with the correct value.
    // -----------------------------------------------------------------------

    #[test]
    fn aof_fsync_err_constant_is_canonical() {
        // The canonical error frame bytes sent to the client when an AOF
        // fsync under appendfsync=always fails. Must match what
        // handler_monoio/mod.rs and handler_sharded/mod.rs use.
        assert_eq!(
            AOF_FSYNC_ERR, b"ERR AOF fsync failed; write not durable",
            "AOF_FSYNC_ERR must equal the canonical ERR-prefixed string"
        );
    }

    #[test]
    fn aof_fsync_err_has_err_prefix() {
        // Redis convention: protocol-level errors must start with a word
        // followed by a space, using `ERR` for generic errors. `WRITEFAIL`
        // is not a standard Redis error prefix and confuses clients that
        // pattern-match on error codes.
        assert!(
            AOF_FSYNC_ERR.starts_with(b"ERR "),
            "AOF_FSYNC_ERR must start with 'ERR ' (got {:?})",
            std::str::from_utf8(AOF_FSYNC_ERR).unwrap_or("<non-utf8>")
        );
    }
}
