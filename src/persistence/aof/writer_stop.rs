//! Stopping the AOF writers at the end of a graceful shutdown (moon#1274).
//!
//! redis's `prepareForShutdown` flushes the AOF buffer and fsyncs before the
//! process exits. moon's writers are threads fed by bounded channels, and the
//! old shutdown lost what they still held: it sent them `Shutdown` with a
//! `try_send` BEFORE the shards stopped, cancelled their token together with
//! the server's (the tokio writers left at once), and joined only the shard
//! threads. The process then exited with acknowledged `appendfsync everysec`
//! appends still queued — a SIGTERM right after 100 SETs lost all 100 at
//! `--shards 1` (monoio), and some at `--shards 4` on both runtimes.
//!
//! Now `main.rs` joins the shards first, so no append can arrive after that,
//! and only then calls [`stop_writers`]: each writer gets `Shutdown` BEHIND
//! everything queued (a blocking send, not a `try_send` that a full channel
//! drops), writes it in FIFO order, fsyncs, and exits, and the process
//! waits for that. A rewrite fold in flight drains its spill buffer
//! (`RewriteOverflow`) into the incr before the writer reads on — that drain
//! can swallow the `Shutdown`, so it is re-sent until the writer is gone.
//! The writers have their own token (not the server's), cancelled only once
//! the wait runs out: [`STOP_BOUND`], or [`HURRY_BOUND`] once `hurry` says so
//! (a second SIGINT, redis's "You insist"). The cancel releases a writer that
//! has not started yet and ends the tokio writers' loops after they write
//! what is queued; they get [`GRACE`] more. A writer still running then is
//! ABANDONED: [`stop_writers`] names it and returns `Err`, and the process
//! exits non-zero (moon#1274 round 3, A1: it used to exit 0).
//!
//! A rewrite dispatched just before the stop no longer wedges a writer here:
//! a fold whose shard has stopped fails at once (`rewrite::fold_reply`).

use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tracing::{error, info, warn};

use super::AofWriterPool;
use crate::runtime::cancel::CancellationToken;

/// How long the exit waits for the writers to drain and fsync.
pub const STOP_BOUND: Duration = Duration::from_secs(60);

/// The wait once the operator insists (a second SIGINT).
pub const HURRY_BOUND: Duration = Duration::from_secs(2);

/// What the writers get after their token is cancelled.
pub const GRACE: Duration = Duration::from_secs(2);

/// How often `Shutdown` is re-sent to a writer still running.
const RESEND_EVERY: Duration = Duration::from_millis(500);

/// Drain, fsync and join every AOF writer. Call once every producer (every
/// shard thread) has stopped; `token` is the writers' own. Waits up to
/// `bound` ([`STOP_BOUND`] in production), or [`HURRY_BOUND`] from the
/// moment `hurry` turns true. `Err` names the writers abandoned.
pub fn stop_writers(
    pool: &AofWriterPool,
    writers: Vec<JoinHandle<()>>,
    token: &CancellationToken,
    bound: Duration,
    hurry: &dyn Fn() -> bool,
) -> Result<(), Vec<String>> {
    let started = Instant::now();
    let mut deadline = started + bound;
    let mut hurried = false;
    let mut cancelled_at: Option<Instant> = None;
    pool.broadcast_shutdown(deadline.min(started + RESEND_EVERY));
    let mut sent = Instant::now();
    loop {
        if writers.iter().all(JoinHandle::is_finished) {
            break;
        }
        let now = Instant::now();
        if !hurried && hurry() {
            hurried = true;
            deadline = deadline.min(now + HURRY_BOUND);
        }
        match cancelled_at {
            None if now >= deadline => {
                warn!(
                    "AOF writers still running {:?} into shutdown: cancelling them (each writes \
                     what is queued, then exits)",
                    now - started
                );
                token.cancel();
                cancelled_at = Some(now);
            }
            Some(at) if now - at >= GRACE.min(bound) => {
                let abandoned: Vec<String> = writers
                    .iter()
                    .filter(|w| !w.is_finished())
                    .map(|w| w.thread().name().unwrap_or("aof-writer").to_string())
                    .collect();
                error!(
                    "AOF writer(s) {abandoned:?} still running {:?} into shutdown; exiting \
                     without them — records they had not written yet are lost",
                    now - started
                );
                return Err(abandoned);
            }
            _ => {}
        }
        if now - sent >= RESEND_EVERY {
            pool.broadcast_shutdown(now);
            sent = now;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    for writer in writers {
        if writer.join().is_err() {
            error!("an AOF writer thread panicked during shutdown");
        }
    }
    info!("AOF writers drained and synced in {:?}", started.elapsed());
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;
    use crate::persistence::aof::AofMessage;
    use crate::runtime::channel;

    /// A stand-in writer: waits for `start` (or the token), then counts the
    /// appends it reads until `Shutdown`. With `wedged`, it never reads.
    fn writer(
        name: &str,
        rx: channel::MpscReceiver<AofMessage>,
        token: CancellationToken,
        start: Arc<AtomicBool>,
        wedged: Arc<AtomicBool>,
        written: Arc<AtomicUsize>,
    ) -> JoinHandle<()> {
        std::thread::Builder::new()
            .name(name.to_string())
            .spawn(move || {
                while !start.load(Ordering::SeqCst) && !token.is_cancelled() {
                    std::thread::sleep(Duration::from_millis(1));
                }
                while wedged.load(Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(1));
                }
                while let Ok(msg) = rx.recv() {
                    match msg {
                        AofMessage::Shutdown => return,
                        _ => {
                            written.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
            })
            .expect("spawn")
    }

    fn append() -> AofMessage {
        AofMessage::Append {
            lsn: 0,
            db: 0,
            bytes: bytes::Bytes::from_static(b"*1\r\n$4\r\nPING\r\n"),
            epoch: crate::persistence::aof::FoldEpoch::INITIAL,
        }
    }

    struct Rig {
        pool: Arc<AofWriterPool>,
        writers: Vec<JoinHandle<()>>,
        token: CancellationToken,
        start: Arc<AtomicBool>,
        wedged: Arc<AtomicBool>,
        written: Arc<AtomicUsize>,
    }

    /// Two writers with 3 appends queued each; writer 1 is `wedged` or not.
    fn rig(wedged: bool) -> Rig {
        let token = CancellationToken::new();
        let (start, wedge) = (
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        );
        let written = Arc::new(AtomicUsize::new(0));
        let mut senders = Vec::new();
        let mut writers = Vec::new();
        for i in 0..2 {
            let (tx, rx) = channel::mpsc_bounded::<AofMessage>(16);
            for _ in 0..3 {
                tx.try_send(append()).expect("room");
            }
            // Writer 1 checks the shared `wedge` flag; writer 0 never wedges.
            let w = if i == 1 {
                wedge.store(wedged, Ordering::SeqCst);
                Arc::clone(&wedge)
            } else {
                Arc::new(AtomicBool::new(false))
            };
            let (t, s, c) = (token.clone(), Arc::clone(&start), Arc::clone(&written));
            writers.push(writer(&format!("test-aof-writer-{i}"), rx, t, s, w, c));
            senders.push(tx);
        }
        Rig {
            pool: AofWriterPool::per_shard(senders),
            writers,
            token,
            start,
            wedged: wedge,
            written,
        }
    }

    /// Every writer drains its queue before the exit.
    #[test]
    fn writers_drain_their_queue_before_the_exit() {
        let r = rig(false);
        r.start.store(true, Ordering::SeqCst);
        let out = stop_writers(&r.pool, r.writers, &r.token, STOP_BOUND, &|| false);
        assert_eq!(out, Ok(()));
        assert_eq!(r.written.load(Ordering::SeqCst), 6);
        assert!(!r.token.is_cancelled());
    }

    /// A writer that has not started (held, late) is released by the cancel
    /// once the bound runs out, and still writes its queue.
    #[test]
    fn a_late_writer_is_released_by_the_cancel_and_drains() {
        let r = rig(false);
        let out = stop_writers(
            &r.pool,
            r.writers,
            &r.token,
            Duration::from_millis(50),
            &|| false,
        );
        assert_eq!(out, Ok(()));
        assert_eq!(r.written.load(Ordering::SeqCst), 6);
        assert!(r.token.is_cancelled());
    }

    /// A1: a writer that never finishes is abandoned BY NAME, so the caller
    /// exits non-zero (it used to return nothing and exit 0).
    #[test]
    fn a_wedged_writer_is_abandoned_by_name() {
        let r = rig(true);
        r.start.store(true, Ordering::SeqCst);
        let bound = Duration::from_millis(50);
        let t = Instant::now();
        let out = stop_writers(&r.pool, r.writers, &r.token, bound, &|| false);
        r.wedged.store(false, Ordering::SeqCst);
        assert_eq!(out, Err(vec!["test-aof-writer-1".to_string()]));
        assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());
    }

    /// A2: `hurry` (a second SIGINT) cuts the 60 s bound to HURRY_BOUND.
    #[test]
    fn hurry_cuts_the_wait() {
        let r = rig(false);
        let t = Instant::now();
        let out = stop_writers(&r.pool, r.writers, &r.token, STOP_BOUND, &|| true);
        assert_eq!(out, Ok(()));
        assert!(t.elapsed() < HURRY_BOUND + GRACE, "{:?}", t.elapsed());
        assert_eq!(r.written.load(Ordering::SeqCst), 6);
    }

    /// Round 3 A7: a full channel (a wedged writer) does not delay the
    /// others' `Shutdown` — they get it before the wait on the full one.
    #[test]
    fn broadcast_shutdown_does_not_wait_on_a_full_channel_first() {
        let (tx0, _rx0) = channel::mpsc_bounded::<AofMessage>(1);
        tx0.try_send(AofMessage::Shutdown).expect("fill writer 0");
        let (tx1, rx1) = channel::mpsc_bounded::<AofMessage>(2);
        let pool = AofWriterPool::per_shard(vec![tx0, tx1]);
        let until = std::time::Instant::now() + std::time::Duration::from_millis(600);
        let sender = std::thread::spawn(move || pool.broadcast_shutdown(until));
        let got = rx1.recv_timeout(std::time::Duration::from_millis(300));
        sender.join().expect("broadcast");
        assert!(
            matches!(got, Ok(AofMessage::Shutdown)),
            "writer 1's Shutdown waited behind writer 0's full channel"
        );
    }
}
