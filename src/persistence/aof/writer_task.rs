//! AOF writer tasks: single-file and per-shard background append loops.
#![allow(unused_imports, unused_variables, unreachable_code, clippy::empty_loop)]

use super::rewrite::{
    do_rewrite_per_shard, drain_pending_appends_framed, rewrite_aof_sharded_sync,
    sync_and_fulfill_drain,
};
use super::*;
// `do_rewrite_single` / `do_rewrite_sharded` exist only under the monoio runtime.
#[cfg(feature = "runtime-monoio")]
use super::rewrite::{do_rewrite_sharded, do_rewrite_single};

// Group-commit batching seam (wal-group-commit). All four writer loops drain a
// ready batch via `collect_group_commit_batch` and ack through `group_commit`'s
// shared ordering; the sync monoio TopLevel path commits through a
// `GroupCommitSink`, while the async/framed paths replicate the same invariant
// inline (one fsync per batch, ack AFTER the fsync) — see group_commit.rs.
use super::group_commit::{
    self, AOF_GROUP_COMMIT_MAX_BATCH, AOF_GROUP_COMMIT_MAX_BYTES, BatchAck,
    collect_group_commit_batch,
};
#[cfg(feature = "runtime-monoio")]
use super::group_commit::{GroupCommitSink, commit_group_commit_batch};

/// Idle-adaptive wake cadence for a background AOF writer's channel poll
/// (RSS/CPU wave 5, item B).
///
/// All four steady-state writer loops (PerShard monoio/tokio, TopLevel
/// monoio/tokio) poll their channel with a bounded timeout so the EverySec proactive-fsync
/// deadline check that follows every wake still fires when no new Appends
/// ever arrive. A FIXED cadence forever (previously 50ms monoio / 200ms
/// tokio) means an idle server's AOF writer thread wakes 5-20 times a
/// second doing nothing. Escalating the wait once a poll times out with
/// nothing queued costs nothing: the poll races a message against the
/// deadline, so a real write always wakes the loop immediately regardless
/// of how long the timeout is set — only the "still idle, re-check
/// nothing" cadence relaxes.
///
/// # The one invariant that must never regress
///
/// The EverySec bound ("the oldest unflushed byte reaches disk within ~1s +
/// one wake") must hold exactly as it did under the old fixed cadence.
/// [`IdleWait`] enforces this the same way the ground rules require:
/// escalation is refused (stays pinned at the fast floor) whenever
/// [`Self::mark_pending`] has been called and not yet cleared by
/// [`Self::clear_pending`] — i.e. whenever there is a write buffered under
/// `FsyncPolicy::EverySec` that has not yet been fsynced, or a manually
/// back-dated `last_fsync` (the F6 post-fold drain trick) representing an
/// imminent deadline. `FsyncPolicy::Always` never buffers past its own
/// batch (fsynced same-iteration) and `FsyncPolicy::No` has no deadline at
/// all, so neither ever calls `mark_pending` — both escalate freely once
/// idle, which is correct: there is nothing time-sensitive to protect.
struct IdleWait {
    step: usize,
    pending: bool,
}

/// Escalation ladder: fast floor for responsiveness right after activity,
/// capped at 1s (never longer than the EverySec deadline itself).
const AOF_IDLE_WAIT_STEPS: &[std::time::Duration] = &[
    std::time::Duration::from_millis(50),
    std::time::Duration::from_millis(250),
    std::time::Duration::from_secs(1),
];

/// Park-free bounded receive for the std-thread writer loops under
/// `FsyncPolicy::EverySec`/`No`.
///
/// A parked `recv_timeout` registers this thread as a flume waiter, so every
/// producer `try_send` from a shard thread pays a futex WAKE **on the shard
/// thread** — measured at 149,718 futex calls (63% of shard-thread syscall
/// time) during an 8s p1 SET run under everysec, the mechanism behind the
/// esec p1 SET deficit vs Redis (whose AOF append is a plain memcpy into
/// `aof_buf`). Polling with `try_recv` + short sleeps never registers a
/// waiter, so producer sends stay pure userspace atomics.
///
/// Latency/CPU trade (why this is safe ONLY for EverySec/No): a message may
/// sit unobserved for up to one sleep step. The step scales with the wait
/// (`wait/16`, clamped to 500µs..50ms), so at the 50ms fast floor the
/// EverySec deadline is still checked within ~3ms slack, and at the idle 1s
/// escalated wait the writer wakes ≤20×/s (vs the parked recv's 1×/s —
/// the accepted cost of this fix; still far below the pre-wave-5 fixed
/// 50ms cadence). Under load `try_recv` returns immediately and no sleep
/// happens at all. `Always` keeps the parked recv: its callers block on the
/// per-batch fsync ack, so added receive latency is user-visible RTT.
#[cfg(feature = "runtime-monoio")]
fn poll_recv(
    rx: &channel::MpscReceiver<AofMessage>,
    wait: std::time::Duration,
) -> Result<AofMessage, flume::RecvTimeoutError> {
    let step = (wait / 16).clamp(
        std::time::Duration::from_micros(500),
        std::time::Duration::from_millis(50),
    );
    let deadline = std::time::Instant::now() + wait;
    loop {
        match rx.try_recv() {
            Ok(m) => return Ok(m),
            Err(flume::TryRecvError::Disconnected) => {
                return Err(flume::RecvTimeoutError::Disconnected);
            }
            Err(flume::TryRecvError::Empty) => {
                if std::time::Instant::now() >= deadline {
                    return Err(flume::RecvTimeoutError::Timeout);
                }
                std::thread::sleep(step);
            }
        }
    }
}

/// Bounded receive for the std-thread writer loops: parked under `Always`
/// (ack latency is client-visible), park-free polling otherwise (producer
/// sends must not pay a futex wake — see [`poll_recv`]).
#[cfg(feature = "runtime-monoio")]
fn recv_next(
    rx: &channel::MpscReceiver<AofMessage>,
    wait: std::time::Duration,
    park: bool,
) -> Result<AofMessage, flume::RecvTimeoutError> {
    if park {
        rx.recv_timeout(wait)
    } else {
        poll_recv(rx, wait)
    }
}

impl IdleWait {
    fn new() -> Self {
        Self {
            step: 0,
            pending: false,
        }
    }

    /// Wait duration to use for the next channel poll.
    fn current(&self) -> std::time::Duration {
        AOF_IDLE_WAIT_STEPS[self.step]
    }

    /// A message (data or control) was just received: reset to the fast
    /// floor so the very next poll — which re-checks the EverySec deadline
    /// — happens promptly again, exactly like the old fixed cadence did.
    fn on_message(&mut self) {
        self.step = 0;
    }

    /// The poll timed out with nothing queued. Escalates towards the max
    /// step UNLESS a deadline is still pending (see struct docs) — in that
    /// case the wait stays at its current (already-fast, since
    /// `on_message` just reset it) step so the deadline is re-checked
    /// promptly instead of drifting out to the escalated cadence.
    fn on_timeout(&mut self) {
        if !self.pending {
            self.step = (self.step + 1).min(AOF_IDLE_WAIT_STEPS.len() - 1);
        }
    }

    /// Mark that `last_fsync` now represents an unflushed/imminent deadline
    /// (a batch was buffered under `FsyncPolicy::EverySec` without an
    /// immediate fsync, or `last_fsync` was manually back-dated). Blocks
    /// further escalation until [`Self::clear_pending`].
    fn mark_pending(&mut self) {
        self.pending = true;
    }

    /// The pending deadline was satisfied (a proactive or batch fsync just
    /// succeeded) — escalation may resume from here.
    fn clear_pending(&mut self) {
        self.pending = false;
    }
}

#[cfg(all(test, feature = "runtime-monoio"))]
mod poll_recv_tests {
    use super::*;

    #[test]
    fn returns_queued_message_immediately() {
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(4);
        assert!(
            tx.try_send(AofMessage::Append {
                lsn: 7,
                db: 0,
                bytes: bytes::Bytes::from_static(b"x"),
            })
            .is_ok()
        );
        let start = std::time::Instant::now();
        let Ok(got) = poll_recv(&rx, std::time::Duration::from_secs(1)) else {
            panic!("expected message");
        };
        assert!(matches!(got, AofMessage::Append { lsn: 7, .. }));
        // No sleep step should have been taken for an already-queued message.
        assert!(start.elapsed() < std::time::Duration::from_millis(50));
    }

    #[test]
    fn picks_up_message_sent_mid_wait() {
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(4);
        let sender = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            assert!(
                tx.try_send(AofMessage::Append {
                    lsn: 1,
                    db: 0,
                    bytes: bytes::Bytes::from_static(b"y"),
                })
                .is_ok()
            );
        });
        let Ok(got) = poll_recv(&rx, std::time::Duration::from_secs(5)) else {
            panic!("expected message");
        };
        assert!(matches!(got, AofMessage::Append { lsn: 1, .. }));
        assert!(sender.join().is_ok());
    }

    #[test]
    fn times_out_when_empty() {
        let (_tx, rx) = channel::mpsc_bounded::<AofMessage>(4);
        let start = std::time::Instant::now();
        // AofMessage has no Debug impl — match instead of unwrap_err.
        let Err(err) = poll_recv(&rx, std::time::Duration::from_millis(30)) else {
            panic!("expected timeout");
        };
        assert!(matches!(err, flume::RecvTimeoutError::Timeout));
        assert!(start.elapsed() >= std::time::Duration::from_millis(30));
    }

    #[test]
    fn reports_disconnect() {
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(4);
        drop(tx);
        // AofMessage has no Debug impl — match instead of unwrap_err.
        let Err(err) = poll_recv(&rx, std::time::Duration::from_secs(1)) else {
            panic!("expected disconnect");
        };
        assert!(matches!(err, flume::RecvTimeoutError::Disconnected));
    }
}

/// A sync [`GroupCommitSink`] over a `std::fs::File` for the monoio writer
/// loops. `write_all` appends raw bytes (an empty buffer — a zero-length
/// H1-BARRIER `AppendSync` — is a no-op); `sync` does the single per-batch
/// `flush()+sync_data()` and records the fsync metric on success.
///
/// `fail_sync` mirrors the legacy `MOON_TEST_AOF_FSYNC_FAIL` injection: when set,
/// `sync()` returns an error so the whole batch resolves `FsyncFailed` (the
/// client sees `AOF_FSYNC_ERR`, never `+OK`) without touching durable storage.
#[cfg(feature = "runtime-monoio")]
struct FileGroupSink<'a> {
    file: &'a mut std::fs::File,
    fail_sync: bool,
}

#[cfg(feature = "runtime-monoio")]
impl GroupCommitSink for FileGroupSink<'_> {
    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        use std::io::Write;
        if buf.is_empty() {
            return Ok(()); // zero-length barrier: fsync+ack only, no on-disk record
        }
        self.file.write_all(buf)
    }

    #[inline]
    fn sync(&mut self) -> std::io::Result<()> {
        use std::io::Write;
        if self.fail_sync {
            return Err(std::io::Error::other("MOON_TEST_AOF_FSYNC_FAIL"));
        }
        let t = Instant::now();
        let r = self.file.flush().and_then(|_| self.file.sync_data());
        if r.is_ok() {
            crate::admin::metrics_setup::record_aof_fsync(t.elapsed().as_micros() as u64);
        }
        r
    }
}

/// Background AOF writer task. Receives commands via mpsc channel and appends them
/// to the AOF file. Handles fsync according to the configured policy.
///
/// `fold_channels` — for the TopLevel `--shards 1` path: an optional
/// `(fold_producer, fold_notifier)` pair for shard 0, wired by `main.rs` via
/// `set_fold_channels` + extracted here.  When `Some`, `do_rewrite_sharded`
/// uses the C4 cooperative-snapshot fold; when `None` (legacy, no AOF fold
/// wired), `do_rewrite_sharded` falls back to an error-abort so the old
/// generation stays authoritative rather than deadlocking.
pub async fn aof_writer_task(
    rx: channel::MpscReceiver<AofMessage>,
    aof_path: PathBuf,
    fsync: FsyncPolicy,
    cancel: CancellationToken,
    fold_channels: Option<(
        Arc<parking_lot::Mutex<ringbuf::HeapProd<crate::shard::dispatch::ShardMessage>>>,
        Arc<crate::runtime::channel::Notify>,
    )>,
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
    // Torn-write latch (tokio TopLevel): once a batch write fails partway, the
    // plain-RESP stream may carry a partial record — never append more bytes nor
    // claim durability after the tear. Latched for the writer's lifetime; reset
    // only on a successful rewrite (the rewrite replaces the file with a fresh
    // one). Mirrors the monoio TopLevel and both per-shard writers, which already
    // carry this latch (group_commit::CommitOutcome.write_failed ⇒ "latch must
    // engage").
    #[cfg(feature = "runtime-tokio")]
    let mut write_error = false;
    // Test-only fault injection: when MOON_TEST_AOF_FSYNC_FAIL=1 every AppendSync
    // batch acks FsyncFailed instead of Synced (read once; zero cost in prod).
    // Mirrors the monoio TopLevel + per-shard writers so the AOF_FSYNC_ERR wire
    // path is exercised identically under both runtimes (shards=1 TopLevel).
    #[cfg(feature = "runtime-tokio")]
    let fail_fsync_for_test = std::env::var("MOON_TEST_AOF_FSYNC_FAIL").as_deref() == Ok("1");
    // Idle-adaptive channel-poll wake cadence (RSS/CPU wave 5, item B) — see
    // `IdleWait` docs above. Declared outside the `loop` below (not inside
    // the per-iteration `#[cfg(runtime-tokio)]` block) so its escalation
    // state survives across iterations.
    #[cfg(feature = "runtime-tokio")]
    let mut idle_wait = IdleWait::new();
    // task #35: AOF db-aware writer — see the monoio TopLevel loop above for
    // the full rationale. Resets to 0 on every successful rewrite (fresh
    // file: replay starts a segment at db 0).
    #[cfg(feature = "runtime-tokio")]
    let mut last_db: usize = 0;

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
        //
        // task #27 (SHUTDOWN) fix: `AofManifest::load` runs BEFORE the
        // cancellation/timeout checks, not after. A client can already be
        // connected and writing (queuing `Append` messages into `rx`) while
        // main.rs's recovery is still mid-flight creating this manifest on a
        // separate thread -- that's the documented race this loop exists to
        // wait out. The old ordering checked `cancel.is_cancelled()` FIRST:
        // a graceful shutdown landing in that same window made the writer
        // return immediately without ever loading the now-current manifest,
        // silently discarding every already-queued Append (reproduced via
        // SHUTDOWN arriving <50ms after boot -- `AOF writer: cancelled while
        // waiting for manifest` with zero bytes ever written to the incr
        // file). Trying the load first means a manifest that exists by the
        // time we get here is never missed just because a shutdown signal
        // happened to land in the same instant.
        let manifest_wait_start = Instant::now();
        const MANIFEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
        let mut manifest = loop {
            match AofManifest::load(&base_dir) {
                Ok(Some(m)) => break m,
                Ok(None) => {
                    // main.rs recovery hasn't created the manifest yet — wait,
                    // unless we're being torn down or have waited too long.
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

        // Idle-adaptive channel-poll wake cadence (RSS/CPU wave 5, item B) —
        // see `IdleWait` docs near the top of this file. This loop used to
        // block on an UNTIMED `rx.recv()`, with the EverySec deadline check
        // only inside the batch path: a batch buffered without a per-batch
        // fsync only became durable when the NEXT message happened to
        // arrive, so idle-after-a-burst deferred the fsync indefinitely
        // (host-crash exposure only — the per-batch flush already reaches
        // the kernel page cache, so a plain process kill loses nothing).
        // The bounded recv + end-of-loop proactive fsync below restore the
        // ~1s EverySec bound exactly like the PerShard writers.
        let mut idle_wait = IdleWait::new();
        // task #35: AOF db-aware writer. Tracks the db the last-written
        // record executed in; a non-empty record whose db differs gets a
        // `SELECT <db>` record injected first (see `inject_select_records`).
        // Resets to 0 whenever a NEW incr/base file becomes the append
        // target (fresh manifest segment — replay starts each incr at db 0).
        let mut last_db: usize = 0;

        loop {
            // Group commit: wait (bounded) for one message, then
            // opportunistically drain whatever else is already queued into a
            // bounded batch so a single fsync makes the whole batch durable
            // (TopLevel = plain RESP bytes). On timeout, fall through (None)
            // to the EverySec proactive fsync at the end of the loop.
            // Park-free under EverySec/No so producer try_sends never pay a
            // futex wake on the shard thread — see `poll_recv`.
            let first = match recv_next(
                &rx,
                idle_wait.current(),
                matches!(fsync, FsyncPolicy::Always),
            ) {
                Ok(m) => {
                    idle_wait.on_message();
                    Some(m)
                }
                Err(flume::RecvTimeoutError::Timeout) => {
                    idle_wait.on_timeout();
                    None
                }
                Err(flume::RecvTimeoutError::Disconnected) => {
                    // Channel disconnected — final sync + shut down.
                    if !write_error {
                        if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                            error!("AOF final sync failed (seq {}): {}", manifest.seq, e);
                        }
                    }
                    info!("AOF writer shutting down (monoio, seq {})", manifest.seq);
                    break;
                }
            };

            if let Some(first) = first {
                let mut batch = collect_group_commit_batch(
                    first,
                    || rx.try_recv().ok(),
                    AOF_GROUP_COMMIT_MAX_BATCH,
                    AOF_GROUP_COMMIT_MAX_BYTES,
                );
                // task #35: inject SELECT <db> records ahead of any db switch
                // before committing — rides the same write path as any other
                // record (raw bytes on this TopLevel format).
                batch.data = inject_select_records(std::mem::take(&mut batch.data), &mut last_db);

                // -- commit the data batch (one fsync under Always; deadline under everysec) --
                if !batch.data.is_empty() {
                    if write_error {
                        // Persistent I/O failure latched: drop appends and fail every
                        // AppendSync waiter — never a false durability claim.
                        let _ = group_commit::ack_batch(&mut batch, BatchAck::WriteFailed);
                    } else {
                        let do_fsync = matches!(fsync, FsyncPolicy::Always);
                        let mut sink = FileGroupSink {
                            file: &mut file,
                            fail_sync: fail_fsync_for_test,
                        };
                        let outcome = commit_group_commit_batch(&mut sink, &mut batch, do_fsync);
                        if outcome.write_failed {
                            // A torn write may leave a partial record — latch so no
                            // further bytes are appended after the tear.
                            error!(
                                "AOF batch write failed (seq {}). Persistence degraded.",
                                manifest.seq
                            );
                            write_error = true;
                        }
                        // EverySec: the batch was written but not per-batch-fsynced
                        // (do_fsync=false; there are no AppendSync waiters under
                        // everysec). The end-of-loop proactive fsync makes it
                        // durable within the 1s bound — pin the idle wait at its
                        // fast floor until that fsync clears it.
                        if fsync == FsyncPolicy::EverySec && !write_error {
                            idle_wait.mark_pending();
                        }
                    }
                }

                // -- handle the control message that ended the drain (if any) --
                // A control message is NEVER absorbed into the batch: the batch above
                // is already committed before the control message is handled
                // (batch_straddles_control is structurally impossible).
                match batch.deferred_control {
                    None => {}
                    Some(AofMessage::Shutdown) => {
                        if !write_error {
                            if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                                error!("AOF final sync failed (seq {}): {}", manifest.seq, e);
                            }
                        }
                        info!("AOF writer shutting down (monoio, seq {})", manifest.seq);
                        break;
                    }
                    Some(AofMessage::Rewrite(db, overflow)) => {
                        // #452.1: spill window opens for the whole fold.
                        overflow.arm();
                        if !write_error {
                            if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                                error!("AOF pre-rewrite sync failed (seq {}): {}", manifest.seq, e);
                            }
                        }
                        match do_rewrite_single(&db, &mut manifest, &mut file, &rx, &mut last_db) {
                            Ok(()) => {
                                write_error = false; // Reset on successful rewrite
                            }
                            Err(e) => error!("AOF rewrite failed (seq {}): {}", manifest.seq, e),
                        }
                        // #452.1: channel backlog + spilled overflow → current
                        // (committed) incr, in order, before resuming the loop.
                        if let Err(e) = overflow.finish_raw(&rx, &mut file, &mut last_db) {
                            error!(
                                "AOF rewrite overflow drain failed (seq {}): {}",
                                manifest.seq, e
                            );
                        }
                        crate::command::persistence::AOF_REWRITE_IN_PROGRESS
                            .store(false, std::sync::atomic::Ordering::SeqCst);
                    }
                    Some(AofMessage::RewriteSharded(shard_dbs, overflow)) => {
                        // #452.1: spill window opens for the whole fold.
                        overflow.arm();
                        if !write_error {
                            if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                                error!("AOF pre-rewrite sync failed (seq {}): {}", manifest.seq, e);
                            }
                        }
                        // C4 TopLevel cooperative fold: pass the wired fold channels
                        // (producer + notifier for shard 0) so do_rewrite_sharded can
                        // use the AofFold SPSC protocol instead of the deleted RwLock
                        // path.  `fold_channels` is `None` only if main.rs failed to
                        // wire them at startup (Arc::get_mut race — logged at boot).
                        match do_rewrite_sharded(
                            &shard_dbs,
                            &mut manifest,
                            &mut file,
                            &rx,
                            fold_channels.as_ref(),
                            &mut last_db,
                        ) {
                            Ok(()) => {
                                write_error = false;
                            }
                            Err(e) => error!("AOF rewrite failed (seq {}): {}", manifest.seq, e),
                        }
                        // #452.1: channel backlog + spilled overflow → current
                        // (committed) incr, in order, before resuming the loop.
                        if let Err(e) = overflow.finish_raw(&rx, &mut file, &mut last_db) {
                            error!(
                                "AOF rewrite overflow drain failed (seq {}): {}",
                                manifest.seq, e
                            );
                        }
                        crate::command::persistence::AOF_REWRITE_IN_PROGRESS
                            .store(false, std::sync::atomic::Ordering::SeqCst);
                    }
                    // [F6] A TopLevel writer never owns per-shard files; receiving
                    // RewritePerShard means a routing bug. Self-abort so the
                    // coordinator's countdown completes and the flag clears.
                    Some(AofMessage::RewritePerShard { coord, .. }) => {
                        warn!(
                            "AOF TopLevel writer received RewritePerShard — routing bug; aborting"
                        );
                        coord.mark_failed();
                        coord.shard_done();
                    }
                    // collect_group_commit_batch only ever defers a control message.
                    Some(_) => {}
                }
            }

            // EverySec proactive fsync — runs after every loop iteration
            // (message processed OR timeout); the only path that guarantees
            // the ~1s durability bound when no further messages arrive after
            // a buffered batch (idle-after-a-burst).
            if fsync == FsyncPolicy::EverySec
                && !write_error
                && last_fsync.elapsed() >= std::time::Duration::from_secs(1)
            {
                let t = Instant::now();
                if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                    error!("AOF sync failed (seq {}, everysec): {}", manifest.seq, e);
                    crate::persistence::aof::record_everysec_fsync_result(0, false);
                    // Non-fatal for everysec: retry next interval (status
                    // stays latched "err" in INFO — the failed window's
                    // pages are already gone even if the retry "succeeds").
                } else {
                    crate::admin::metrics_setup::record_aof_fsync(t.elapsed().as_micros() as u64);
                    crate::persistence::aof::record_everysec_fsync_result(0, true);
                    last_fsync = Instant::now();
                    idle_wait.clear_pending();
                }
            }
        }
        return;
    }

    loop {
        #[cfg(feature = "runtime-tokio")]
        {
            // Bounded recv (EverySec durability): wake at least every
            // `idle_wait.current()` (50ms floor, escalates to 1s while
            // truly idle — see `IdleWait` docs; tighter than the old fixed
            // 200ms right after activity, far looser once idle) even when
            // idle so the flush
            // deadline check after this select! is honored within its 1s
            // bound. A long-lived `interval.tick()` select arm is
            // fairness-starvable under sustained writes and unreliable when idle
            // (see the per-shard writer below, which hit exactly that) — the
            // bounded recv cannot starve. flume's recv future is drop-safe on
            // the Elapsed branch (no message consumed on timeout).
            let recv_result = tokio::select! {
                r = tokio::time::timeout(
                    idle_wait.current(),
                    rx.recv_async(),
                ) => r,
                _ = cancel.cancelled() => {
                    // Skip the final sync if the stream is torn — syncing past a
                    // partial record cannot recover it and risks a false durability
                    // signal (mirrors the monoio TopLevel disconnect/shutdown gate).
                    if !write_error {
                        let _ = writer.flush().await;
                        let _ = writer.get_ref().sync_data().await;
                    }
                    info!("AOF writer cancelled");
                    break;
                }
            };
            match recv_result {
                // Timeout (Elapsed): no message — fall through to the EverySec
                // deadline check after this block.
                Err(_) => idle_wait.on_timeout(),
                // Channel disconnected — final sync + shut down.
                Ok(Err(_)) => {
                    if !write_error {
                        let _ = writer.flush().await;
                        let _ = writer.get_ref().sync_data().await;
                    }
                    info!("AOF writer shutting down");
                    break;
                }
                Ok(Ok(first)) => {
                    // A message just arrived (data or control): reset the idle
                    // wait to its fast floor so the deadline check after this
                    // block — and every subsequent poll while there is still
                    // buffered/pending data — happens at the tight cadence.
                    idle_wait.on_message();
                    // Group commit: drain whatever else is queued into a bounded
                    // batch so one fsync covers all (TopLevel = plain RESP bytes).
                    let mut batch = collect_group_commit_batch(
                        first,
                        || rx.try_recv().ok(),
                        AOF_GROUP_COMMIT_MAX_BATCH,
                        AOF_GROUP_COMMIT_MAX_BYTES,
                    );
                    // task #35: inject SELECT <db> records ahead of any db
                    // switch before committing (see the monoio TopLevel loop
                    // above).
                    batch.data =
                        inject_select_records(std::mem::take(&mut batch.data), &mut last_db);

                    // -- write the data batch inline (async), then ONE fsync --
                    if !batch.data.is_empty() {
                        if write_error {
                            // Stream already torn — drop appends and fail every
                            // AppendSync waiter (never ack into a corrupt stream).
                            let _ = group_commit::ack_batch(&mut batch, BatchAck::WriteFailed);
                        } else {
                            let mut write_failed = false;
                            for msg in &batch.data {
                                let body = group_commit::msg_body(msg);
                                if body.is_empty() {
                                    continue; // zero-length barrier: fsync+ack only
                                }
                                if let Err(e) = writer.write_all(body).await {
                                    error!("AOF batch write error: {}", e);
                                    write_failed = true;
                                    break;
                                }
                            }
                            let do_fsync = matches!(fsync, FsyncPolicy::Always);
                            let verdict = if write_failed {
                                // A torn write may leave a partial record — latch so
                                // no further bytes are appended after the tear.
                                write_error = true;
                                BatchAck::WriteFailed
                            } else if fail_fsync_for_test && do_fsync {
                                // Injected: bytes written, the batch fsync "fails"
                                // → every waiter FsyncFailed (no disk error needed).
                                BatchAck::FsyncFailed
                            } else if do_fsync {
                                let mut fsync_failed = false;
                                if let Err(e) = writer.flush().await {
                                    error!("AOF batch flush error: {}", e);
                                    fsync_failed = true;
                                } else if let Err(e) = writer.get_ref().sync_data().await {
                                    error!("AOF batch sync_data error: {}", e);
                                    fsync_failed = true;
                                }
                                if fsync_failed {
                                    BatchAck::FsyncFailed
                                } else {
                                    BatchAck::Synced
                                }
                            } else {
                                // EverySec/No: batch buffered; the deadline check
                                // after this block fsyncs. An AppendSync is enqueued
                                // ONLY under Always (pool::try_send_append_durable /
                                // fsync_barrier), so this branch acks no waiter.
                                debug_assert!(
                                    !batch
                                        .data
                                        .iter()
                                        .any(|m| matches!(m, AofMessage::AppendSync { .. })),
                                    "everysec/no batch must contain no AppendSync"
                                );
                                if fsync == FsyncPolicy::EverySec {
                                    // Bytes are buffered but not yet durable —
                                    // pin the idle wait at its floor until the
                                    // deadline check below (or a future
                                    // iteration's) clears it.
                                    idle_wait.mark_pending();
                                }
                                BatchAck::Synced
                            };
                            let _ = group_commit::ack_batch(&mut batch, verdict);
                        }
                    }

                    // -- handle the control message that ended the drain (if any) --
                    match batch.deferred_control {
                        None => {}
                        Some(AofMessage::Rewrite(db, overflow)) => {
                            // #452.1: spill window opens for the whole rewrite.
                            overflow.arm();
                            // Flush current writer before rewrite (skip if torn).
                            if !write_error {
                                let _ = writer.flush().await;
                                let _ = writer.get_ref().sync_data().await;
                            }

                            match rewrite_aof(db, &aof_path).await {
                                // Rewrite replaced the file with a clean one — the
                                // torn-write latch resets (mirrors monoio TopLevel).
                                // task #35: fresh file — replay starts at db 0. On
                                // Err the old file is untouched, so last_db stays
                                // whatever it already was.
                                Ok(()) => {
                                    write_error = false;
                                    last_db = 0;
                                }
                                Err(e) => error!("AOF rewrite failed: {}", e),
                            }
                            crate::command::persistence::AOF_REWRITE_IN_PROGRESS
                                .store(false, std::sync::atomic::Ordering::SeqCst);

                            // Reopen file after rewrite (it was replaced)
                            let reopen_result: Result<tokio::fs::File, _> =
                                tokio::fs::OpenOptions::new()
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
                                    overflow.disarm_dropping();
                                    return;
                                }
                            }
                            // #452.1: channel backlog + spilled overflow → the
                            // fresh file, in order, before resuming the loop.
                            {
                                let mut sf = writer.into_inner().into_std().await;
                                if let Err(e) = overflow.finish_raw(&rx, &mut sf, &mut last_db) {
                                    error!("AOF rewrite overflow drain failed: {}", e);
                                }
                                writer = tokio::io::BufWriter::new(tokio::fs::File::from_std(sf));
                            }
                            // Back-date so the backlog drained right after the
                            // rewrite reaches disk within ~100ms + wake floor,
                            // not a full second later (mirrors the per-shard
                            // writer's post-rewrite back-dating). This is itself
                            // a pending deadline — pin the idle wait at its
                            // floor so escalation cannot push the next check
                            // past the intended window.
                            last_fsync = Instant::now() - std::time::Duration::from_millis(900);
                            idle_wait.mark_pending();
                        }
                        Some(AofMessage::RewriteSharded(shard_dbs, overflow)) => {
                            // #452.1: spill window opens for the whole fold.
                            overflow.arm();
                            // C4 TopLevel cooperative fold (tokio path):
                            // flush + sync the BufWriter (skip if torn), convert to
                            // std::fs::File for the sync fold (same pattern as tokio
                            // per-shard), then reopen for appending.
                            if !write_error {
                                let _ = writer.flush().await;
                                let _ = writer.get_ref().sync_data().await;
                            }
                            let mut sf = writer.into_inner().into_std().await;
                            match rewrite_aof_sharded_sync(
                                &shard_dbs,
                                &aof_path,
                                &rx,
                                &mut sf,
                                fold_channels.as_ref(),
                                &mut last_db,
                            ) {
                                // Fold rewrote aof_path clean — the latch resets.
                                Ok(()) => write_error = false,
                                Err(e) => error!("AOF rewrite (sharded) failed: {}", e),
                            }
                            // Drop sf — caller will reopen aof_path below.
                            drop(sf);
                            crate::command::persistence::AOF_REWRITE_IN_PROGRESS
                                .store(false, std::sync::atomic::Ordering::SeqCst);
                            let reopen_result: Result<tokio::fs::File, _> =
                                tokio::fs::OpenOptions::new()
                                    .create(true)
                                    .append(true)
                                    .open(&aof_path)
                                    .await;
                            match reopen_result {
                                Ok(f) => writer = tokio::io::BufWriter::new(f),
                                Err(e) => {
                                    error!("Failed to reopen AOF after rewrite: {}", e);
                                    overflow.disarm_dropping();
                                    return;
                                }
                            }
                            // #452.1: channel backlog + spilled overflow → the
                            // fresh file, in order, before resuming the loop.
                            {
                                let mut sf = writer.into_inner().into_std().await;
                                if let Err(e) = overflow.finish_raw(&rx, &mut sf, &mut last_db) {
                                    error!("AOF rewrite overflow drain failed: {}", e);
                                }
                                writer = tokio::io::BufWriter::new(tokio::fs::File::from_std(sf));
                            }
                            // Back-date so the channel backlog that accumulated
                            // during the blocking fold reaches disk within ~100ms
                            // + wake floor — a SIGKILL shortly after rewrite
                            // completion must not take the tail with it. Pin the
                            // idle wait at its floor until this deadline fires.
                            last_fsync = Instant::now() - std::time::Duration::from_millis(900);
                            idle_wait.mark_pending();
                        }
                        // [F6] TopLevel writer never owns per-shard files — routing
                        // bug. Self-abort so the countdown completes + flag clears.
                        Some(AofMessage::RewritePerShard { coord, .. }) => {
                            warn!(
                                "AOF TopLevel writer received RewritePerShard — routing bug; aborting"
                            );
                            coord.mark_failed();
                            coord.shard_done();
                        }
                        Some(AofMessage::Shutdown) => {
                            if !write_error {
                                let _ = writer.flush().await;
                                let _ = writer.get_ref().sync_data().await;
                            }
                            info!("AOF writer shutting down");
                            break;
                        }
                        // collect_group_commit_batch only ever defers a control message.
                        Some(_) => {}
                    }
                }
            }
            // EverySec deadline: the oldest unflushed byte reaches disk at
            // most ~1.2s after it was written (1s deadline + wake floor —
            // the wake floor only, never the escalated idle cadence: see
            // `IdleWait`, which `mark_pending`/`clear_pending` keep pinned
            // at the floor for exactly this check). tokio's BufWriter holds
            // up to 8KB in userspace — a SIGKILL takes that tail with it, so
            // the bound must hold even when the recv arm is saturated with
            // messages. Skip if torn: syncing past a partial record cannot
            // recover it.
            if fsync == FsyncPolicy::EverySec
                && !write_error
                && last_fsync.elapsed() >= std::time::Duration::from_secs(1)
            {
                let t = Instant::now();
                let res = match writer.flush().await {
                    Ok(()) => writer.get_ref().sync_data().await,
                    Err(e) => Err(e),
                };
                match res {
                    Err(e) => {
                        error!("AOF sync failed (everysec, tokio TopLevel): {}", e);
                        crate::persistence::aof::record_everysec_fsync_result(0, false);
                        // Keep last_fsync unadvanced so the deadline stays
                        // armed — a silent success-record here would let the
                        // failed window's loss self-heal invisibly.
                    }
                    Ok(()) => {
                        crate::admin::metrics_setup::record_aof_fsync(
                            t.elapsed().as_micros() as u64
                        );
                        crate::persistence::aof::record_everysec_fsync_result(0, true);
                        last_fsync = Instant::now();
                        idle_wait.clear_pending();
                    }
                }
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
/// Test-only torn-write injection for `per_shard_aof_writer_task`: when set to a
/// nonzero `N`, the `N`-th `Append` received by a tokio per-shard writer writes
/// its header and then simulates a payload write failure, exercising the
/// `write_error` latch. `0` disables. Atomic (not an env var) because
/// `std::env::set_var` is `unsafe` under edition 2024. Compiled out of release.
#[cfg(all(test, feature = "runtime-tokio"))]
pub(crate) static TEST_FAIL_WRITE_AT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

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
        //
        // task #27 fix: load-before-cancel-check, same rationale as the
        // TopLevel loop above — a manifest that now exists must not be
        // missed just because a shutdown signal landed in the same instant
        // (see that loop's comment for the reproduction).
        let manifest_wait_start = Instant::now();
        const MANIFEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
        let manifest = loop {
            match AofManifest::load(&base_dir) {
                Ok(Some(m)) => break m,
                Ok(None) => {
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
        // Idle-adaptive channel-poll wake cadence (RSS/CPU wave 5, item B) —
        // see `IdleWait` docs near the top of this file.
        let mut idle_wait = IdleWait::new();
        // task #35: AOF db-aware writer — see the monoio TopLevel loop's docs
        // near the top of this file for the full rationale.
        let mut last_db: usize = 0;
        // (No `interval` here: the EverySec flush deadline is enforced by the
        // timeout-bounded recv in the loop below, which wakes at least every
        // `idle_wait.current()` (50ms floor, escalates to 1s while idle)
        // regardless of message traffic. A long-lived `interval.tick()`
        // select arm is fairness-starvable under sustained writes and proved
        // unreliable when idle on this dedicated current-thread writer runtime.)

        // Test-only fault injection: if MOON_TEST_AOF_FSYNC_FAIL=1 is set in
        // the environment at writer task startup, every AppendSync ack resolves
        // as FsyncFailed instead of Synced. This lets integration tests exercise
        // the AOF_FSYNC_ERR response path without requiring a real disk error.
        // The env var is read once here (not per-message) so it costs zero on the
        // hot path in production deployments where the var is absent.
        let fail_fsync_for_test = std::env::var("MOON_TEST_AOF_FSYNC_FAIL").as_deref() == Ok("1");

        // Torn-write latch: once any write to this incr file fails partway
        // (e.g. the header landed but the payload did not), we must NEVER write
        // another record — a lone orphaned header makes the framed reader
        // misinterpret the next record's bytes as the orphan's payload,
        // corrupting every record after it on replay. Stay latched for the
        // writer's lifetime; recovery replays the clean prefix and a rewrite
        // starts a fresh file. This mirrors the single-file (line ~1467) and
        // monoio per-shard (line ~2125) writers, which already carry the latch.
        let mut write_error = false;
        // Test-only fault injection (no env var: edition-2024 set_var is unsafe).
        // When `TEST_FAIL_WRITE_AT` is the ordinal of an incoming Append, that
        // append writes its header then simulates a payload failure, exercising
        // the latch. Compiled out of production builds.
        #[cfg(test)]
        let mut test_append_ordinal: usize = 0;

        loop {
            tokio::select! {
                // Bounded recv (EverySec durability): wake at least every
                // `idle_wait.current()` (50ms floor, escalates to 1s while
                // idle — see `IdleWait`) even when idle so the flush deadline
                // after this select! is honored within its 1s bound. flume's
                // recv future is drop-safe on the Elapsed branch (no message
                // consumed on timeout); the Ok(Ok(msg)) path below captures
                // the message with no loss.
                r = tokio::time::timeout(
                    idle_wait.current(),
                    rx.recv_async(),
                ) => {
                    // On Elapsed (timeout) `r` is Err: skip and fall through to
                    // the EverySec deadline check after this select!.
                    match r {
                        Err(_) => idle_wait.on_timeout(),
                        // Channel disconnected — final sync + shut down.
                        Ok(Err(_)) => {
                            let _ = writer.flush().await;
                            let _ = writer.get_ref().sync_data().await;
                            info!("AOF writer shard {} shutting down", shard_id);
                            break;
                        }
                        Ok(Ok(first)) => {
                            // A message just arrived: reset to the fast floor
                            // (see TopLevel writer above for the full rationale).
                            idle_wait.on_message();
                            // Group commit: drain a bounded batch so ONE fsync
                            // makes all framed records (`[u64 lsn][u32 len][RESP]`)
                            // durable.
                            let mut batch = collect_group_commit_batch(
                                first,
                                || rx.try_recv().ok(),
                                AOF_GROUP_COMMIT_MAX_BATCH,
                                AOF_GROUP_COMMIT_MAX_BYTES,
                            );
                            // task #35: inject SELECT <db> records on db-context
                            // changes before this batch's framed writes go out.
                            batch.data = inject_select_records(
                                std::mem::take(&mut batch.data),
                                &mut last_db,
                            );

                            if !batch.data.is_empty() {
                                if write_error {
                                    // Stream already torn — drop appends, fail every
                                    // AppendSync waiter (no ack into a corrupt stream).
                                    let _ = group_commit::ack_batch(
                                        &mut batch,
                                        BatchAck::WriteFailed,
                                    );
                                } else {
                                    // Write each record framed, header + body, in
                                    // order; the single fsync below covers them all.
                                    let mut write_failed = false;
                                    for msg in &batch.data {
                                        let (lsn, data) = match msg {
                                            AofMessage::Append { lsn, bytes, .. }
                                            | AofMessage::AppendSync { lsn, bytes, .. } => {
                                                (*lsn, bytes)
                                            }
                                            _ => continue,
                                        };
                                        // H1-BARRIER: a zero-length AppendSync writes
                                        // NO record (a len=0 framed header would make
                                        // replay reject the file) — fsync+ack only.
                                        if data.is_empty() {
                                            continue;
                                        }
                                        #[cfg(test)]
                                        {
                                            if matches!(msg, AofMessage::Append { .. }) {
                                                test_append_ordinal += 1;
                                                let fail_at = TEST_FAIL_WRITE_AT
                                                    .load(std::sync::atomic::Ordering::Relaxed);
                                                if fail_at != 0 && fail_at == test_append_ordinal {
                                                    // Reproduce a torn write: header
                                                    // lands, payload "fails", latch.
                                                    let mut header = [0u8; 12];
                                                    header[..8]
                                                        .copy_from_slice(&lsn.to_le_bytes());
                                                    header[8..].copy_from_slice(
                                                        &(data.len() as u32).to_le_bytes(),
                                                    );
                                                    let _ = writer.write_all(&header).await;
                                                    let _ = writer.flush().await;
                                                    error!(
                                                        "AOF shard {}: injected torn write after header (test)",
                                                        shard_id
                                                    );
                                                    write_failed = true;
                                                    break;
                                                }
                                            }
                                        }
                                        let mut header = [0u8; 12];
                                        header[..8].copy_from_slice(&lsn.to_le_bytes());
                                        header[8..]
                                            .copy_from_slice(&(data.len() as u32).to_le_bytes());
                                        if let Err(e) = writer.write_all(&header).await {
                                            error!(
                                                "AOF header write error shard {}: {}",
                                                shard_id, e
                                            );
                                            write_failed = true;
                                            break;
                                        }
                                        if let Err(e) = writer.write_all(data).await {
                                            error!("AOF write error shard {}: {}", shard_id, e);
                                            write_failed = true;
                                            break;
                                        }
                                    }

                                    let do_fsync = matches!(fsync, FsyncPolicy::Always);
                                    let verdict = if write_failed {
                                        // A torn write may leave a partial record —
                                        // latch so no further bytes are appended.
                                        write_error = true;
                                        BatchAck::WriteFailed
                                    } else if fail_fsync_for_test && do_fsync {
                                        // Injected: bytes written, the batch fsync
                                        // "fails" → every waiter FsyncFailed.
                                        BatchAck::FsyncFailed
                                    } else if do_fsync {
                                        let mut ff = false;
                                        if let Err(e) = writer.flush().await {
                                            error!(
                                                "AOF batch flush error shard {}: {}",
                                                shard_id, e
                                            );
                                            ff = true;
                                        } else if let Err(e) = writer.get_ref().sync_data().await {
                                            error!(
                                                "AOF batch sync_data error shard {}: {}",
                                                shard_id, e
                                            );
                                            ff = true;
                                        }
                                        if ff {
                                            BatchAck::FsyncFailed
                                        } else {
                                            BatchAck::Synced
                                        }
                                    } else {
                                        // EverySec/No: the deadline check fsyncs; no
                                        // AppendSync waiters under everysec/no.
                                        if fsync == FsyncPolicy::EverySec {
                                            idle_wait.mark_pending();
                                        }
                                        BatchAck::Synced
                                    };
                                    let _ = group_commit::ack_batch(&mut batch, verdict);
                                }
                            }

                            // -- handle the control message that ended the drain --
                            match batch.deferred_control {
                                None => {}
                                Some(AofMessage::Rewrite(..))
                                | Some(AofMessage::RewriteSharded(..)) => {
                                    warn!(
                                        "AOF writer shard {}: received Rewrite/RewriteSharded — \
                                         not applicable in PerShard layout, dropped.",
                                        shard_id
                                    );
                                }
                                // [F6] Per-shard rewrite (tokio): reuse the proven
                                // synchronous fold (`do_rewrite_per_shard`) verbatim.
                                // This writer runs on a DEDICATED std::thread
                                // (block_on_local, main.rs) — not a shared tokio
                                // worker — so the blocking fold cannot starve the
                                // runtime. Flush the BufWriter (its `into_inner` does
                                // NOT flush) so buffered appends are durable in the
                                // OLD incr, convert to `std::fs::File` for the sync
                                // fold, then wrap the (reopened) file back.
                                Some(AofMessage::RewritePerShard {
                                    shard_dbs,
                                    coord,
                                    fold_producer,
                                    fold_notifier,
                                    overflow,
                                }) => {
                                    // #452.1: spill window opens for the fold.
                                    overflow.arm();
                                    if let Err(e) = writer.flush().await {
                                        error!(
                                            "F6 tokio per-shard rewrite: shard {} pre-fold flush \
                                             failed: {}. Aborting; old generation stays authoritative.",
                                            shard_id, e
                                        );
                                        coord.mark_failed();
                                        coord.shard_done();
                                        overflow.disarm_dropping();
                                    } else {
                                        // `into_std().await` waits for in-flight ops
                                        // and is infallible; buffer flushed above.
                                        let mut sf = writer.into_inner().into_std().await;
                                        let res = do_rewrite_per_shard(
                                            shard_id, &shard_dbs, &mut sf, &rx, &coord,
                                            &fold_producer, &fold_notifier, &mut last_db,
                                        );
                                        // `sf` is left on the committed generation by
                                        // the fold's internal barrier: NEW incr on
                                        // success, OLD incr on abort/pre-reopen error.
                                        // The fold's ShardDoneGuard already did
                                        // `shard_done` for every exit, so do NOT
                                        // decrement again. Wrap `sf` back either way.
                                        // #452.1: drain channel backlog + spilled
                                        // overflow into the committed incr first.
                                        if let Err(e) =
                                            overflow.finish_framed(&rx, &mut sf, &mut last_db)
                                        {
                                            error!(
                                                "F6 tokio per-shard rewrite: shard {} overflow \
                                                 drain failed: {}",
                                                shard_id, e
                                            );
                                        }
                                        writer = tokio::io::BufWriter::new(
                                            tokio::fs::File::from_std(sf),
                                        );
                                        if let Err(e) = res {
                                            error!(
                                                "F6 tokio per-shard rewrite: shard {} fold failed: {}. \
                                                 Rewrite aborted by the fold guard; old generation \
                                                 stays authoritative.",
                                                shard_id, e
                                            );
                                        }
                                    }
                                }
                                Some(AofMessage::Shutdown) => {
                                    let _ = writer.flush().await;
                                    let _ = writer.get_ref().sync_data().await;
                                    info!("AOF writer shard {} shutting down", shard_id);
                                    break;
                                }
                                // collect only ever defers a control message.
                                Some(_) => {}
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
                && !write_error
                && last_fsync.elapsed() >= std::time::Duration::from_secs(1)
            {
                let t = Instant::now();
                let res = match writer.flush().await {
                    Ok(()) => writer.get_ref().sync_data().await,
                    Err(e) => Err(e),
                };
                match res {
                    Err(e) => {
                        error!(
                            "AOF sync failed shard {} (everysec, tokio PerShard): {}",
                            shard_id, e
                        );
                        crate::persistence::aof::record_everysec_fsync_result(
                            usize::from(shard_id),
                            false,
                        );
                    }
                    Ok(()) => {
                        crate::admin::metrics_setup::record_aof_fsync(
                            t.elapsed().as_micros() as u64
                        );
                        crate::persistence::aof::record_everysec_fsync_result(
                            usize::from(shard_id),
                            true,
                        );
                        last_fsync = Instant::now();
                        idle_wait.clear_pending();
                    }
                }
            }
        }
    }

    #[cfg(feature = "runtime-monoio")]
    {
        use crate::persistence::aof_manifest::{AofLayout, AofManifest};
        use std::io::Write;

        // task #27 fix: load-before-cancel-check, same rationale as the
        // TopLevel loop above.
        let manifest_wait_start = Instant::now();
        const MANIFEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
        let manifest = loop {
            match AofManifest::load(&base_dir) {
                Ok(Some(m)) => break m,
                Ok(None) => {
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
        let mut _dbg_processed: u64 = 0;
        let _dbg_start = Instant::now();
        // Reusable frame-coalescing buffer: one contiguous write_all per
        // group-commit batch instead of a header+body write pair per record.
        let mut batch_buf: Vec<u8> = Vec::new();
        // Idle-adaptive channel-poll wake cadence (RSS/CPU wave 5, item B) —
        // see `IdleWait` docs near the top of this file.
        let mut idle_wait = IdleWait::new();
        // task #35: AOF db-aware writer — see the monoio TopLevel loop's docs
        // near the top of this file for the full rationale.
        let mut last_db: usize = 0;
        // Test-only fault injection: if MOON_TEST_AOF_FSYNC_FAIL=1 is set in
        // the environment at writer task startup, every AppendSync ack resolves
        // as FsyncFailed instead of Synced. Read once before the loop so there
        // is zero cost in production deployments where the var is absent.
        let fail_fsync_for_test = std::env::var("MOON_TEST_AOF_FSYNC_FAIL").as_deref() == Ok("1");

        loop {
            // Use recv_timeout so the EverySec fsync fires even when no new
            // Appends arrive after a fold (or when the client stops writing).
            // Without a timeout, the writer blocks forever in rx.recv() and
            // the 1s fsync window never fires → data loss on kill.
            // recv_timeout so the EverySec proactive fsync fires even when no new
            // Appends arrive after a fold (or when the client stops writing).
            // The wait starts at `idle_wait`'s fast floor (50ms, matching the
            // old fixed cadence) and escalates while genuinely idle — see
            // `IdleWait` docs. Park-free under EverySec/No so producer
            // try_sends never pay a futex wake on the shard thread — see
            // `poll_recv`.
            let first = match recv_next(
                &rx,
                idle_wait.current(),
                matches!(fsync, FsyncPolicy::Always),
            ) {
                Ok(m) => {
                    idle_wait.on_message();
                    Some(m)
                }
                // Timeout: no message in the window. Fall through (None) to
                // the EverySec proactive fsync below so queued-but-unfsynced
                // appends are durable within the everysec contract even when idle.
                Err(flume::RecvTimeoutError::Timeout) => {
                    idle_wait.on_timeout();
                    None
                }
                Err(flume::RecvTimeoutError::Disconnected) => {
                    if !write_error {
                        if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                            error!(
                                "AOF final sync failed shard {} (seq {}): {}",
                                shard_id, manifest.seq, e
                            );
                        }
                    }
                    info!(
                        "AOF writer shard {} shutting down (monoio, seq {}, processed {} appends in {:.3}s)",
                        shard_id,
                        manifest.seq,
                        _dbg_processed,
                        _dbg_start.elapsed().as_secs_f64()
                    );
                    break;
                }
            };

            if let Some(first) = first {
                // Group commit: drain a bounded batch so ONE fsync makes all
                // framed records (`[u64 lsn][u32 len][RESP]`) durable.
                let mut batch = collect_group_commit_batch(
                    first,
                    || rx.try_recv().ok(),
                    AOF_GROUP_COMMIT_MAX_BATCH,
                    AOF_GROUP_COMMIT_MAX_BYTES,
                );
                // task #35: inject SELECT <db> records on db-context changes
                // before this batch's framed writes go into batch_buf.
                batch.data = inject_select_records(std::mem::take(&mut batch.data), &mut last_db);

                if !batch.data.is_empty() {
                    if write_error {
                        // Stream already torn — drop appends, fail every AppendSync
                        // waiter so callers error instead of acking a corrupt write.
                        let _ = group_commit::ack_batch(&mut batch, BatchAck::WriteFailed);
                    } else {
                        // Frame each record (`[u64 lsn][u32 len]` header + body, in
                        // channel order) into the reusable batch buffer and issue
                        // ONE write_all for the whole batch; the single fsync below
                        // covers it all. The old per-record header+body write_all
                        // pair on the raw File was the dominant writer-thread cost
                        // under always-P16 (measured 127,812 write(2) calls / 1.25s
                        // of an 8s strace window vs 2,144 fdatasyncs / 0.2s — Redis
                        // batches ~120 records per write via aof_buf).
                        batch_buf.clear();
                        for msg in &batch.data {
                            let (lsn, data) = match msg {
                                AofMessage::Append { lsn, bytes, .. }
                                | AofMessage::AppendSync { lsn, bytes, .. } => (*lsn, bytes),
                                _ => continue,
                            };
                            // H1-BARRIER: a zero-length AppendSync writes NO record
                            // (a len=0 framed header would make replay reject the
                            // file) — fsync + ack only.
                            if data.is_empty() {
                                continue;
                            }
                            _dbg_processed += 1;
                            let mut header = [0u8; 12];
                            header[..8].copy_from_slice(&lsn.to_le_bytes());
                            header[8..].copy_from_slice(&(data.len() as u32).to_le_bytes());
                            batch_buf.extend_from_slice(&header);
                            batch_buf.extend_from_slice(data);
                        }
                        let mut write_failed = false;
                        if !batch_buf.is_empty() {
                            if let Err(e) = file.write_all(&batch_buf) {
                                error!(
                                    "AOF batch write failed shard {} (seq {}): {}. Persistence degraded.",
                                    shard_id, manifest.seq, e
                                );
                                write_failed = true;
                            }
                        }
                        // Cap the reusable buffer's high-water mark: a rare burst of
                        // large values (up to AOF_GROUP_COMMIT_MAX_BYTES) must not
                        // pin megabytes on the writer thread forever.
                        if batch_buf.capacity() > 1 << 20 {
                            batch_buf = Vec::new();
                        }

                        let do_fsync = matches!(fsync, FsyncPolicy::Always);
                        let verdict = if write_failed {
                            // A torn write may leave a partial record — latch so no
                            // further bytes are appended after the tear.
                            write_error = true;
                            BatchAck::WriteFailed
                        } else if fail_fsync_for_test && do_fsync {
                            // Injected: bytes written, the batch fsync "fails".
                            BatchAck::FsyncFailed
                        } else if do_fsync {
                            let t = Instant::now();
                            if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                                error!(
                                    "AOF batch sync failed shard {} (seq {}, always): {}",
                                    shard_id, manifest.seq, e
                                );
                                BatchAck::FsyncFailed
                            } else {
                                crate::admin::metrics_setup::record_aof_fsync(
                                    t.elapsed().as_micros() as u64,
                                );
                                BatchAck::Synced
                            }
                        } else {
                            // EverySec/No: the proactive fsync below makes the batch
                            // durable; no AppendSync waiters under everysec/no.
                            if fsync == FsyncPolicy::EverySec {
                                idle_wait.mark_pending();
                            }
                            BatchAck::Synced
                        };
                        let _ = group_commit::ack_batch(&mut batch, verdict);
                    }
                }

                // -- handle the control message that ended the drain (if any) --
                match batch.deferred_control {
                    None => {}
                    Some(AofMessage::Rewrite(..)) | Some(AofMessage::RewriteSharded(..)) => {
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
                    Some(AofMessage::RewritePerShard {
                        shard_dbs,
                        coord,
                        fold_producer,
                        fold_notifier,
                        overflow,
                    }) => {
                        // #452.1: spill window opens for the fold.
                        overflow.arm();
                        if let Err(e) = do_rewrite_per_shard(
                            shard_id,
                            &shard_dbs,
                            &mut file,
                            &rx,
                            &coord,
                            &fold_producer,
                            &fold_notifier,
                            &mut last_db,
                        ) {
                            // The fold's ShardDoneGuard already marked the rewrite
                            // failed and decremented on this error exit (committing
                            // new_seq with a shard missing its new base would break
                            // recovery), so do NOT decrement again here. `file` is left
                            // on the OLD incr (error exits are pre-reopen).
                            error!(
                                "F6 per-shard rewrite: shard {} fold failed: {}. \
                                 Rewrite aborted by the fold guard; old generation \
                                 stays authoritative.",
                                shard_id, e
                            );
                        }
                        // #452.1: drain channel backlog + spilled overflow into
                        // the committed incr, in order, before the EverySec
                        // post-fold drain below picks up anything newer.
                        if let Err(e) = overflow.finish_framed(&rx, &mut file, &mut last_db) {
                            error!(
                                "F6 per-shard rewrite: shard {} overflow drain failed: {}",
                                shard_id, e
                            );
                        }
                        // EverySec post-fold drain+fsync: the fold runs synchronously
                        // and does NOT update `last_fsync`. Appends that arrived during
                        // the fold queue in the bounded AOF channel; they land on the NEW
                        // incr but are NOT fsynced until the EverySec timer fires.
                        // Strategy: drain what's currently in the channel and fsync now
                        // (covers appends that landed before this drain), then set
                        // `last_fsync` 900ms in the past so the proactive check below
                        // fires within the NEXT 100ms window (≤150ms total, since the
                        // recv_timeout is 50ms). That second fsync covers any appends
                        // that arrived between the drain and that window close.
                        // Combined, the two fsyncs bound the post-fold EverySec window
                        // to ≤150ms — well within the test's 1500ms kill margin.
                        if !write_error {
                            if let Ok(mut post_drain) = drain_pending_appends_framed(
                                &rx,
                                &mut file,
                                usize::MAX,
                                &mut last_db,
                            ) {
                                if let Err(e) = sync_and_fulfill_drain(
                                    &mut post_drain,
                                    &mut file,
                                    std::path::PathBuf::from("<aof per-shard new-incr post-fold>"),
                                ) {
                                    error!(
                                        "F6 per-shard rewrite: shard {} post-fold fsync \
                                         failed: {}. EverySec window open until next Append.",
                                        shard_id, e
                                    );
                                } else {
                                    // Back-date last_fsync by 900ms: the proactive check
                                    // (threshold=1s) fires within the next 100ms, covering
                                    // any appends that arrived after the drain above. This
                                    // IS a pending deadline — pin the idle wait at its
                                    // floor (`on_message` already reset it for this
                                    // iteration; `mark_pending` keeps it there) so
                                    // escalation cannot push the next check out past the
                                    // ≤150ms window the comment above promises.
                                    last_fsync =
                                        Instant::now() - std::time::Duration::from_millis(900);
                                    idle_wait.mark_pending();
                                }
                            }
                        }
                    }
                    Some(AofMessage::Shutdown) => {
                        if !write_error {
                            if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                                error!(
                                    "AOF final sync failed shard {} (seq {}): {}",
                                    shard_id, manifest.seq, e
                                );
                            }
                        }
                        info!(
                            "AOF writer shard {} shutting down (monoio, seq {}, processed {} appends in {:.3}s)",
                            shard_id,
                            manifest.seq,
                            _dbg_processed,
                            _dbg_start.elapsed().as_secs_f64()
                        );
                        break;
                    }
                    // collect only ever defers a control message.
                    Some(_) => {}
                }
            }
            // EverySec proactive fsync — runs after every loop iteration
            // (message processed OR timeout). This is the only path that
            // guarantees the 1s fsync bound when no new Appends arrive
            // (e.g. after BGREWRITEAOF completes and the client stops writing).
            if fsync == FsyncPolicy::EverySec
                && !write_error
                && last_fsync.elapsed() >= std::time::Duration::from_secs(1)
            {
                tracing::debug!(
                    "AOF EverySec proactive fsync firing shard {} (elapsed={:.3}s)",
                    shard_id,
                    last_fsync.elapsed().as_secs_f64()
                );
                let t = Instant::now();
                if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                    error!(
                        "AOF EverySec proactive sync failed shard {} (seq {}): {}",
                        shard_id, manifest.seq, e
                    );
                    crate::persistence::aof::record_everysec_fsync_result(
                        usize::from(shard_id),
                        false,
                    );
                } else {
                    crate::admin::metrics_setup::record_aof_fsync(t.elapsed().as_micros() as u64);
                    crate::persistence::aof::record_everysec_fsync_result(
                        usize::from(shard_id),
                        true,
                    );
                    last_fsync = Instant::now();
                    idle_wait.clear_pending();
                }
            }
        }
    }
}

#[cfg(test)]
mod idle_wait_tests {
    use super::*;

    #[test]
    fn starts_at_fast_floor() {
        let w = IdleWait::new();
        assert_eq!(w.current(), AOF_IDLE_WAIT_STEPS[0]);
    }

    #[test]
    fn timeouts_escalate_and_cap_at_max() {
        let mut w = IdleWait::new();
        w.on_timeout();
        assert_eq!(w.current(), AOF_IDLE_WAIT_STEPS[1]);
        w.on_timeout();
        assert_eq!(w.current(), AOF_IDLE_WAIT_STEPS[2]);
        // Capped: further timeouts stay at the max step.
        w.on_timeout();
        assert_eq!(w.current(), AOF_IDLE_WAIT_STEPS[2]);
    }

    #[test]
    fn message_resets_to_floor_from_any_step() {
        let mut w = IdleWait::new();
        w.on_timeout();
        w.on_timeout();
        assert_eq!(w.current(), AOF_IDLE_WAIT_STEPS[2]);
        w.on_message();
        assert_eq!(w.current(), AOF_IDLE_WAIT_STEPS[0]);
    }

    #[test]
    fn pending_deadline_blocks_escalation() {
        let mut w = IdleWait::new();
        w.mark_pending();
        // Never escalates while a deadline is pending, no matter how many
        // consecutive timeouts occur.
        w.on_timeout();
        w.on_timeout();
        w.on_timeout();
        assert_eq!(w.current(), AOF_IDLE_WAIT_STEPS[0]);
    }

    #[test]
    fn clearing_pending_resumes_escalation() {
        let mut w = IdleWait::new();
        w.mark_pending();
        w.on_timeout();
        assert_eq!(w.current(), AOF_IDLE_WAIT_STEPS[0]);
        w.clear_pending();
        w.on_timeout();
        assert_eq!(w.current(), AOF_IDLE_WAIT_STEPS[1]);
    }
}
