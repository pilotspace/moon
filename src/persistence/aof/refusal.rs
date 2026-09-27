//! moon#1272: an append the AOF writer is too backlogged to take is not an
//! fsync failure, and must not be reported as one.
//!
//! Under `appendfsync everysec`/`no` a durable-path producer
//! (`AofWriterPool::send_append_group` / `try_send_append_durable`) waits up
//! to `--aof-fsync-timeout-ms` for room in its shard's writer channel. When
//! the writer stays behind for that long (a slow disk, a stalled fsync, a
//! frozen runner), or a rewrite fold's overflow buffer is at its cap, the
//! record is refused with [`AofAck::ChannelFull`]. Nothing was fsynced and
//! nothing failed to fsync: the writer was simply backlogged.
//!
//! Every connection path used to answer that refusal with
//! [`AOF_FSYNC_ERR`] (`-ERR AOF fsync failed; write not durable`), the text of
//! a real write/fsync failure (a redis `-MISCONF`-class disk fault). Operators
//! paged on a disk that was fine, and nothing in INFO explained the refusal.
//!
//! This module owns the split:
//!
//! * [`append_refusal_reply`] maps a refused append to its reply:
//!   [`AofAck::ChannelFull`] → [`AOF_BACKLOG_ERR`]; `WriteFailed` /
//!   `FsyncFailed` → [`AOF_FSYNC_ERR`] (unchanged).
//! * [`note_append_backpressure_refusal`] counts every such refusal
//!   (`aof_append_backpressure_refusals` in `INFO persistence`,
//!   `moon_aof_append_backpressure_refusals_total` in `/metrics`) and logs
//!   ONE warning when a stall begins, then at most one summary every
//!   [`STALL_SUMMARY_EVERY`] while it lasts — never one line per refused
//!   write.
//!
//! ## Why `MOONERR AOF backpressure`, not `BUSY` or `ERR`
//!
//! Moon already answers every other AOF writer-backlog condition with that
//! prefix (the SPSC drain's and inline path's 5 ms bound,
//! `spsc_handler::AOF_APPEND_LOST_ERR`; moon#769's routed refusal,
//! `shard::aof_admission::AOF_BACKPRESSURE_REFUSED_ERR`), so one prefix now
//! names one condition. `BUSY` is redis's "a script is running, use
//! SCRIPT KILL" code: Jedis maps it to `JedisBusyException` and Lettuce to
//! `RedisBusyException`, and an operator reading it goes looking for a stuck
//! script. (redis-py 5.x/8.x does not special-case `BUSY` — it maps only
//! `LOADING` to `BusyLoadingError` — so it would not have misfired there.) An
//! `ERR` code would be indistinguishable from any generic error to a client
//! that switches on the code. No client library maps `MOONERR`, so every one
//! surfaces it as its plain server-error type with the full text.
//!
//! ## Why the text says "applied in memory"
//!
//! Every producer that maps through [`append_refusal_reply`] has ALREADY
//! applied the write when its record is refused (the local write legs,
//! MOVE/COPY, graph, MULTI/EXEC bodies, coordinator local legs); the drop is
//! counted in `aof_backpressure_dropped` and reported by
//! `aof_last_write_status:err` until a rewrite folds it back in. A reply
//! claiming "not applied, retry" would invite a client to re-run an `INCR`.
//! Producers that refuse BEFORE applying keep their own texts
//! (`ERR SWAPDB aborted: AOF enqueue failed (persistence backpressure)`,
//! moon#769's `... command not executed ...; retry`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use tracing::warn;

use super::{AOF_FSYNC_ERR, AofAck};
use crate::protocol::Frame;

/// Reply for a write that was applied in memory but whose AOF record the
/// writer could not take: its channel stayed full for `--aof-fsync-timeout-ms`
/// (or a rewrite fold's overflow buffer was at its cap). No fsync ran or
/// failed.
///
/// Shares its leading text with `spsc_handler::AOF_APPEND_LOST_ERR` (the same
/// condition under the SPSC drain's / inline path's 5 ms bound), so a client
/// or harness matching `MOONERR AOF backpressure` catches both, while the
/// suffix tells the two bounds apart.
pub const AOF_BACKLOG_ERR: &[u8] = b"MOONERR AOF backpressure: write applied in memory but not \
queued for persistence; the AOF writer is backlogged";

/// Appends refused because the AOF writer was backlogged (moon#1272): the
/// writer channel stayed full past `--aof-fsync-timeout-ms` for a durable-path
/// producer, or a rewrite fold's overflow buffer was at its cap. Each one is a
/// write command whose client got a backpressure error (normally
/// [`AOF_BACKLOG_ERR`]) — never `+OK`, and never the fsync-failure text.
///
/// Exposed as `aof_append_backpressure_refusals` in `INFO persistence` and as
/// `moon_aof_append_backpressure_refusals_total` in `/metrics`. Disjoint from
/// `aof_backpressure_refused` (moon#769's routed legs, refused before they ran)
/// and from `aof_fsync_failures` (real fsync errors). A refusal of an applied
/// write is ALSO counted in `aof_backpressure_dropped`, which counts records.
pub static AOF_APPEND_BACKPRESSURE_REFUSALS: AtomicU64 = AtomicU64::new(0);

/// A stall "ends" once no append has been refused for this long; the next
/// refusal logs a fresh "stall began" warning.
pub(crate) const STALL_QUIET: Duration = Duration::from_secs(10);

/// While a stall lasts, at most one summary line per this interval.
pub(crate) const STALL_SUMMARY_EVERY: Duration = Duration::from_secs(10);

impl AofAck {
    /// `true` for a refusal caused by writer backlog (the record never
    /// reached a full writer channel), `false` for a write/fsync failure.
    #[inline]
    pub const fn is_backpressure(self) -> bool {
        matches!(self, AofAck::ChannelFull)
    }
}

/// The reply text for a write whose AOF append was refused with `ack`.
///
/// [`AofAck::ChannelFull`] (writer backlog) → [`AOF_BACKLOG_ERR`]; every other
/// ack (`WriteFailed`, `FsyncFailed`; `Synced` never reaches here) →
/// [`AOF_FSYNC_ERR`]. Static bytes: no allocation on the reply path.
#[inline]
pub fn append_refusal_reply(ack: AofAck) -> &'static [u8] {
    if ack.is_backpressure() {
        AOF_BACKLOG_ERR
    } else {
        AOF_FSYNC_ERR
    }
}

/// [`append_refusal_reply`] as an error frame.
#[inline]
pub fn append_refusal_frame(ack: AofAck) -> Frame {
    Frame::Error(Bytes::from_static(append_refusal_reply(ack)))
}

/// Reply for an `appendfsync always` write whose record the writer DID take
/// but whose fsync barrier could not be queued: the writer channel stayed
/// full (moon#1272 review round 2b). The record will be written and fsynced
/// with the backlog, so "not queued" ([`AOF_BACKLOG_ERR`]) would be false;
/// durability was simply not confirmed. Same `MOONERR AOF backpressure`
/// prefix, so one matcher catches every backlog refusal.
pub const AOF_BARRIER_BACKLOG_ERR: &[u8] = b"MOONERR AOF backpressure: write applied in memory \
and queued, but not confirmed durable; the AOF writer is backlogged";

/// The reply text for writes whose `appendfsync always` fsync barrier was
/// refused with `ack`: [`AofAck::ChannelFull`] → [`AOF_BARRIER_BACKLOG_ERR`],
/// anything else → [`AOF_FSYNC_ERR`].
#[inline]
pub fn barrier_refusal_reply(ack: AofAck) -> &'static [u8] {
    if ack.is_backpressure() {
        AOF_BARRIER_BACKLOG_ERR
    } else {
        AOF_FSYNC_ERR
    }
}

/// [`barrier_refusal_reply`] as an error frame.
#[inline]
pub fn barrier_refusal_frame(ack: AofAck) -> Frame {
    Frame::Error(Bytes::from_static(barrier_refusal_reply(ack)))
}

/// What the stall logger does for one refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StallLog {
    /// First refusal after [`STALL_QUIET`] without one: warn that a stall began.
    Began,
    /// Mid-stall, and [`STALL_SUMMARY_EVERY`] passed since the last line.
    Summary,
    /// Mid-stall, logged recently: count only.
    Quiet,
}

/// Pure decision: `prev_refusal_ms` is the previous refusal's time (0 =
/// never), `last_log_ms` the last line's, `now_ms` this refusal's.
pub(crate) fn stall_log_step(prev_refusal_ms: u64, last_log_ms: u64, now_ms: u64) -> StallLog {
    let quiet = STALL_QUIET.as_millis() as u64;
    if prev_refusal_ms == 0 || now_ms.saturating_sub(prev_refusal_ms) >= quiet {
        StallLog::Began
    } else if now_ms.saturating_sub(last_log_ms) >= STALL_SUMMARY_EVERY.as_millis() as u64 {
        StallLog::Summary
    } else {
        StallLog::Quiet
    }
}

static STALL_LAST_REFUSAL_MS: AtomicU64 = AtomicU64::new(0);
static STALL_LAST_LOG_MS: AtomicU64 = AtomicU64::new(0);
/// [`AOF_APPEND_BACKPRESSURE_REFUSALS`] when the current stall began.
static STALL_BASE_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Milliseconds on a process-monotonic clock, never 0 (0 means "never").
/// Cold path only: reached once per refused append, i.e. after a producer
/// already waited `--aof-fsync-timeout-ms`.
fn monotonic_ms() -> u64 {
    static BASE: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    let base = *BASE.get_or_init(std::time::Instant::now);
    u64::try_from(base.elapsed().as_millis())
        .unwrap_or(u64::MAX)
        .saturating_add(1)
}

/// Count one append refused for writer backlog on `shard_id`, after `waited`
/// (the producer's bound; `ZERO` for an immediate overflow-cap refusal), and
/// log it rate-limited: one WARN when a stall begins, then at most one
/// summary every [`STALL_SUMMARY_EVERY`]. `what` names the producer's
/// outcome ("everysec append dropped", ...); the begin line always carries
/// it, so a log grep for a drop still finds every stall.
pub(crate) fn note_append_backpressure_refusal(shard_id: usize, what: &str, waited: Duration) {
    let total = AOF_APPEND_BACKPRESSURE_REFUSALS
        .fetch_add(1, Ordering::Relaxed)
        .saturating_add(1);
    crate::admin::metrics_setup::record_aof_append_backpressure_refusal();

    let now = monotonic_ms();
    let prev = STALL_LAST_REFUSAL_MS.swap(now, Ordering::AcqRel);
    let last_log = STALL_LAST_LOG_MS.load(Ordering::Acquire);
    match stall_log_step(prev, last_log, now) {
        StallLog::Began => {
            STALL_LAST_LOG_MS.store(now, Ordering::Release);
            STALL_BASE_TOTAL.store(total.saturating_sub(1), Ordering::Relaxed);
            warn!(
                "AOF writer backlogged (shard {}): {} after {:?} of backpressure — the writer \
                 channel stayed full (slow disk or stalled fsync?); no fsync failed. Writes are \
                 answered `-MOONERR AOF backpressure` until it drains; further refusals are \
                 counted in INFO aof_append_backpressure_refusals and summarized at most every \
                 {:?}",
                shard_id, what, waited, STALL_SUMMARY_EVERY,
            );
        }
        StallLog::Summary => {
            // One summariser per interval even with many refusing producers.
            if STALL_LAST_LOG_MS
                .compare_exchange(last_log, now, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                let base = STALL_BASE_TOTAL.load(Ordering::Relaxed);
                warn!(
                    "AOF writer still backlogged (shard {}): {} appends refused in this stall \
                     so far (aof_append_backpressure_refusals={})",
                    shard_id,
                    total.saturating_sub(base),
                    total,
                );
            }
        }
        StallLog::Quiet => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::aof::{AofMessage, AofWriterPool, FoldEpoch, FsyncPolicy};
    use crate::runtime::channel;

    /// Runs `fut` on a current-thread runtime of the compiled-in flavour,
    /// timers on (the everysec backpressure bound is a runtime timer).
    fn block_on_with_timer<F: std::future::Future>(fut: F) -> F::Output {
        #[cfg(feature = "runtime-monoio")]
        {
            monoio::RuntimeBuilder::<monoio::LegacyDriver>::new()
                .enable_timer()
                .build()
                .expect("monoio runtime")
                .block_on(fut)
        }
        #[cfg(all(feature = "runtime-tokio", not(feature = "runtime-monoio")))]
        {
            tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("tokio runtime")
                .block_on(fut)
        }
    }

    /// A writer that never drains: a one-slot channel, pre-filled, whose
    /// receiver is kept alive (so the channel is Full, not Disconnected).
    fn held_writer(
        timeout: Duration,
    ) -> (
        std::sync::Arc<AofWriterPool>,
        channel::MpscReceiver<AofMessage>,
    ) {
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(1);
        tx.try_send(AofMessage::Append {
            lsn: 0,
            db: 0,
            bytes: Bytes::from_static(b"filler"),
            epoch: FoldEpoch::INITIAL,
        })
        .expect("pre-fill the only slot");
        (
            AofWriterPool::top_level_with_policy(tx, FsyncPolicy::EverySec, timeout),
            rx,
        )
    }

    /// moon#1272 red/green: an everysec append past `--aof-fsync-timeout-ms`
    /// on a held writer is a BACKPRESSURE refusal — its reply is
    /// `AOF_BACKLOG_ERR`, never `AOF_FSYNC_ERR` — and it is counted.
    /// Before the fix every handler mapped this `Err` to `AOF_FSYNC_ERR`.
    #[test]
    fn everysec_append_past_timeout_maps_to_backlog_error_not_fsync_error() {
        let (pool, _rx) = held_writer(Duration::from_millis(30));
        let before = AOF_APPEND_BACKPRESSURE_REFUSALS.load(Ordering::Relaxed);
        let started = std::time::Instant::now();
        let res = block_on_with_timer(pool.send_append_group(
            0,
            1,
            0,
            Bytes::from_static(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"),
            FoldEpoch::INITIAL,
        ));
        let ack = res.expect_err("a held writer must refuse the append");
        assert_eq!(ack, AofAck::ChannelFull);
        assert!(
            started.elapsed() >= Duration::from_millis(25),
            "the refusal must come only after the backpressure bound, not before"
        );
        assert!(ack.is_backpressure());
        assert_eq!(append_refusal_reply(ack), AOF_BACKLOG_ERR);
        assert_ne!(
            append_refusal_reply(ack),
            AOF_FSYNC_ERR,
            "a writer backlog must never be reported as an fsync failure"
        );
        assert!(
            matches!(append_refusal_frame(ack), Frame::Error(ref e) if e.as_ref() == AOF_BACKLOG_ERR)
        );
        assert!(
            AOF_APPEND_BACKPRESSURE_REFUSALS.load(Ordering::Relaxed) > before,
            "the refusal must be counted for INFO aof_append_backpressure_refusals"
        );
    }

    /// Same contract through the policy-aware entry point the SWAPDB and
    /// legacy callers use.
    #[test]
    fn everysec_durable_append_past_timeout_is_backpressure() {
        let (pool, _rx) = held_writer(Duration::from_millis(20));
        let before = AOF_APPEND_BACKPRESSURE_REFUSALS.load(Ordering::Relaxed);
        let res = block_on_with_timer(pool.try_send_append_durable(
            0,
            1,
            0,
            Bytes::from_static(b"x"),
            FoldEpoch::INITIAL,
        ));
        assert_eq!(res, Err(AofAck::ChannelFull));
        assert_eq!(append_refusal_reply(AofAck::ChannelFull), AOF_BACKLOG_ERR);
        assert!(AOF_APPEND_BACKPRESSURE_REFUSALS.load(Ordering::Relaxed) > before);
    }

    /// A writer that is GONE is a write failure, not backpressure: the
    /// fsync-failure text stays, and nothing is counted as a refusal.
    #[test]
    fn writer_gone_keeps_the_fsync_failure_text() {
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(1);
        drop(rx);
        let pool = AofWriterPool::top_level_with_policy(
            tx,
            FsyncPolicy::EverySec,
            Duration::from_millis(20),
        );
        let res = block_on_with_timer(pool.send_append_group(
            0,
            1,
            0,
            Bytes::from_static(b"x"),
            FoldEpoch::INITIAL,
        ));
        let ack = res.expect_err("a disconnected writer must refuse");
        assert_eq!(ack, AofAck::WriteFailed);
        assert!(!ack.is_backpressure());
        assert_eq!(append_refusal_reply(ack), AOF_FSYNC_ERR);
        assert_eq!(append_refusal_reply(AofAck::FsyncFailed), AOF_FSYNC_ERR);
    }

    #[test]
    fn stall_logger_warns_once_per_stall_then_summarises_sparsely() {
        let quiet = STALL_QUIET.as_millis() as u64;
        let every = STALL_SUMMARY_EVERY.as_millis() as u64;
        // The very first refusal of the process begins a stall.
        assert_eq!(stall_log_step(0, 0, 5), StallLog::Began);
        // Refusals every 2 s (the default bound) stay one stall...
        assert_eq!(stall_log_step(1_000, 1_000, 3_000), StallLog::Quiet);
        // ...with at most one summary per interval.
        assert_eq!(
            stall_log_step(1_000 + every - 1, 1_000, 1_000 + every),
            StallLog::Summary
        );
        assert_eq!(
            stall_log_step(1_000 + every, 1_000 + every, 1_000 + every + 1),
            StallLog::Quiet
        );
        // A quiet gap ends the stall; the next refusal begins a new one.
        assert_eq!(
            stall_log_step(50_000, 50_000, 50_000 + quiet),
            StallLog::Began
        );
    }
}
