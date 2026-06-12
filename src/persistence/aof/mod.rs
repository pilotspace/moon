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
    /// own shard (drain → AofFold SPSC → snapshot → write new base+incr at
    /// the coordinator's `new_seq` → reopen), then decrements the shared
    /// `PerShardRewriteCoord`; the last writer commits the manifest once
    /// (single seq flip) and prunes the old generation. The synchronized seq
    /// + single commit are what make multi-shard BGREWRITEAOF crash-safe.
    ///
    /// `fold_producer` / `fold_notifier` are per-shard SPSC handles used for
    /// the C4 cooperative snapshot (ShardMessage::AofFold).  Each writer
    /// receives the producer/notifier for ITS own shard only.
    RewritePerShard {
        shard_dbs: Arc<crate::shard::shared_databases::ShardDatabases>,
        coord: Arc<PerShardRewriteCoord>,
        /// SPSC producer into this shard's event-loop ring buffer.
        /// Wrapped in `Mutex` so the AOF writer thread (not the shard thread)
        /// can push the AofFold message safely.
        fold_producer:
            Arc<parking_lot::Mutex<ringbuf::HeapProd<crate::shard::dispatch::ShardMessage>>>,
        /// Notifier that wakes the shard event loop after an SPSC push.
        fold_notifier: Arc<crate::runtime::channel::Notify>,
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
    /// The committed generation, published exactly once by the terminal writer
    /// (`new_seq` on success, `old_seq` on abort/commit-failure). Every folded
    /// writer blocks on this in `await_outcome` after `shard_done` so it can
    /// reopen its append file onto the COMMITTED generation before resuming
    /// appends — without this barrier a writer that reopened onto `new_seq` in
    /// phase 6 keeps appending there after an abort, into a generation recovery
    /// ignores (silent data loss; the old "RESTART recommended" hazard).
    outcome: parking_lot::Mutex<Option<u64>>,
    outcome_cv: parking_lot::Condvar,
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
            outcome: parking_lot::Mutex::new(None),
            outcome_cv: parking_lot::Condvar::new(),
        })
    }

    /// Publish the committed generation to every writer blocked in
    /// `await_outcome`. Called exactly once, by the terminal `shard_done`.
    #[inline]
    fn publish_outcome(&self, committed_seq: u64) {
        let mut slot = self.outcome.lock();
        *slot = Some(committed_seq);
        self.outcome_cv.notify_all();
    }

    /// Block until the terminal writer publishes the committed generation, then
    /// return it (`new_seq` on success, `old_seq` on abort/commit-failure). Each
    /// folded writer calls this AFTER `shard_done` so it can reopen its append
    /// file onto the committed generation. Safe against missed wake-ups: the
    /// outcome is set under the same lock the condvar waits on, so a writer that
    /// arrives after the publish observes `Some` and skips the wait.
    ///
    /// Deadlock-free: all `n_shards` writers call `shard_done` exactly once
    /// (success via phase 7, fold-error/send-failure via the caller), so the
    /// countdown always reaches zero and the terminal branch always publishes.
    /// Each per-shard writer runs on its own dedicated OS thread, so blocking
    /// one here never starves the thread that must publish.
    #[must_use]
    fn await_outcome(&self) -> u64 {
        let mut slot = self.outcome.lock();
        while slot.is_none() {
            self.outcome_cv.wait(&mut slot);
        }
        slot.expect("outcome is Some after the wait loop")
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
                     Old generation remains authoritative (crash-safe). Folded writers \
                     roll their append files back to the old generation at the \
                     barrier — no restart required.",
                    self.old_seq
                );
                // Publish BEFORE clearing the in-progress flag so every blocked
                // writer wakes and reopens onto the (still-authoritative) old gen.
                self.publish_outcome(self.old_seq);
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
                // Commit failed: old_seq is still authoritative, so folded
                // writers must roll back to it (their phase-6 reopen targeted
                // new_seq, which never committed).
                self.publish_outcome(self.old_seq);
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
            // Success: new_seq is committed. Folded writers already reopened onto
            // new_seq in phase 6, so the barrier is a no-op for them — but it must
            // still unblock them.
            self.publish_outcome(self.new_seq);
            crate::command::persistence::AOF_REWRITE_IN_PROGRESS.store(false, Ordering::SeqCst);
        }
    }
}

/// Panic-safety guard for a per-shard fold's `shard_done` obligation.
///
/// The `await_outcome` barrier in `do_rewrite_per_shard` turns "every shard
/// calls `shard_done` exactly once" from a tidiness rule into a LIVENESS
/// requirement: any folded writer that decrements then blocks in `await_outcome`
/// hangs forever if some other shard never decrements. The one path that would
/// skip `shard_done` is a panic mid-fold (e.g. `save_snapshot_to_bytes`
/// OOM-unwinding — issue #138), which under `panic = "unwind"` would otherwise
/// hang every healthy shard's AOF writer thread.
///
/// This guard makes the decrement unconditional. The fold creates it on entry
/// and calls [`complete`](Self::complete) on the success path (clean
/// `shard_done`). On ANY other exit — `?` error or panic unwind — `Drop` fires
/// `mark_failed` + `shard_done`, so the countdown still reaches zero, the
/// terminal writer publishes `old_seq`, and every barrier waiter wakes and rolls
/// back. Because the guard owns the decrement for ALL exits, callers MUST NOT
/// call `shard_done` again after invoking `do_rewrite_per_shard`.
struct ShardDoneGuard<'a> {
    coord: &'a PerShardRewriteCoord,
    done: bool,
}

impl<'a> ShardDoneGuard<'a> {
    #[inline]
    fn new(coord: &'a PerShardRewriteCoord) -> Self {
        Self { coord, done: false }
    }

    /// Success-path completion: a single clean `shard_done` (no `mark_failed`).
    /// Consumes the guard so the subsequent `Drop` is a no-op.
    #[inline]
    fn complete(mut self) {
        self.done = true;
        self.coord.shard_done();
    }
}

impl Drop for ShardDoneGuard<'_> {
    fn drop(&mut self) {
        if !self.done {
            // Unwind or early-error before completion: abort the rewrite (keep
            // old_seq) and still decrement so the barrier releases every waiter.
            self.coord.mark_failed();
            self.coord.shard_done();
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

// ── Submodule decomposition (refactor: aof.rs 4379 lines -> directory module) ──
// Codec (serialize_command/replay_aof) stays in this parent so children reach it
// via `use super::*`. AofWriterPool, writer tasks, and rewrite paths move out.
mod pool;
mod rewrite;
mod writer_task;

pub use pool::AofWriterPool;
pub use rewrite::generate_rewrite_commands;
// `rewrite_aof` is defined only under the tokio runtime; gate its re-export to match.
#[cfg(feature = "runtime-tokio")]
pub use rewrite::rewrite_aof;
pub use writer_task::{aof_writer_task, per_shard_aof_writer_task};
// Test-only torn-write injection hook, consumed by pool_tests; gated to match its
// definition in writer_task.rs.
#[cfg(all(test, feature = "runtime-tokio"))]
pub(crate) use writer_task::TEST_FAIL_WRITE_AT;

/// Serialize a Frame into RESP wire format bytes.
pub fn serialize_command(frame: &Frame) -> Bytes {
    let mut buf = BytesMut::with_capacity(64);
    serialize::serialize(frame, &mut buf);
    buf.freeze()
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
