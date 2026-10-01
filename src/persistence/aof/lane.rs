//! moon#1266 Option 1A: the shard thread writes its own AOF records, with
//! one `write(2)` per event-loop iteration, before that iteration's replies
//! leave — redis's model (`beforeSleep` writes the AOF buffer, then the
//! replies go out; only the fsync is deferred). A `kill -9` then loses no
//! acknowledged write under `appendfsync everysec`: the bytes are in the
//! kernel page cache before the client sees `+OK`. Only an OS crash or power
//! loss can lose up to ~1 s (the fsync stays on the writer's agent thread,
//! moon#1266 Option 3).
//!
//! One [`AofLane`] per AOF writer. The hand-off of the stream's append
//! position between the writer thread and the producers is
//! [`super::lane_protocol`]; this file adds the I/O, the framing (identical
//! to the writer's: [`RecordCtx::prefix_for`], `[u64 lsn][u32 len]` for a
//! per-shard incr, bare RESP otherwise, the #455 fold-floor drop), the
//! per-thread binding and the flush points:
//!
//! - io_uring monoio (no SQPOLL): the vendored driver calls
//!   [`flush_current`] before every `io_uring_enter` that submits (park, cold
//!   submit, a full SQ), so the shard's whole iteration is ONE `write(2)`
//!   ahead of its replies' SQEs ([`install_for_shard`]);
//! - every other driver (monoio legacy/epoll/kqueue, SQPOLL, tokio) writes a
//!   reply inside the task: the reply macros call
//!   [`flush_before_reply_coalesced`] (tokio: one write per scheduler round;
//!   monoio: one per connection batch), the rarer early flushes (before a
//!   blocking command, SUBSCRIBE) [`flush_before_reply`];
//! - a reply that leaves for ANOTHER thread (`OneshotSender::send`,
//!   `ResponseSlot::fill`) calls [`flush_current`] first: the receiver may
//!   put it on its socket before this thread's next park;
//! - the shard event loop flushes once per iteration (background records:
//!   expiry, eviction), and once on exit.
//!
//! On by default; `MOON_AOF_SHARD_WRITE=0` ([`enabled`]) turns it off: the
//! pool then never touches a lane, and Option 3 runs unchanged (the same-
//! binary A/B, and the escape hatch).

use std::cell::{Cell, RefCell};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tracing::error;

use super::lane_protocol::{Flushed, LaneCore, Mode, TakenBack};
use super::{AofMessage, FoldEpoch, RecordCtx};
use crate::runtime::channel;

/// Direct `write(2)`s issued by lanes, process-wide (diagnostic: one per
/// flushing iteration while 1A runs).
pub static AOF_LANE_WRITES: AtomicU64 = AtomicU64::new(0);

/// Option 1A when `MOON_AOF_SHARD_WRITE` is unset: on (WS46 adopted it —
/// `.add/milestones/v0-9-2-perf-review/plans/WS46-aof-1a/`).
const DEFAULT_ON: bool = true;

/// `MOON_AOF_SHARD_WRITE`: `1`/`on`/`yes`/`true` or `0`/`off`/`no`/`false`;
/// anything else (or unset) is the default.
fn parse_switch(v: Option<&str>) -> bool {
    match v.map(str::trim) {
        Some(v)
            if v == "1"
                || v.eq_ignore_ascii_case("on")
                || v.eq_ignore_ascii_case("yes")
                || v.eq_ignore_ascii_case("true") =>
        {
            true
        }
        Some(v)
            if v == "0"
                || v.eq_ignore_ascii_case("off")
                || v.eq_ignore_ascii_case("no")
                || v.eq_ignore_ascii_case("false") =>
        {
            false
        }
        _ => DEFAULT_ON,
    }
}

/// Whether Option 1A is on (`MOON_AOF_SHARD_WRITE`, [`DEFAULT_ON`] when
/// unset). Read once per process.
///
/// The test hook `MOON_TEST_AOF_FSYNC_STALL_MS` (a writer held at its
/// everysec deadline — the moon#769/#838/#1272 suites fill the writer's
/// channel with it to exercise the backpressure refusals) keeps 1A off:
/// with the shard writing its own records there is no queue to fill — a slow
/// disk stalls the shard's `write(2)` instead, as in redis — and the channel
/// path those suites pin is still live (`always`, folds, before the first
/// hand-over).
pub fn enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        let on = parse_switch(std::env::var("MOON_AOF_SHARD_WRITE").ok().as_deref());
        let stall_hook = std::env::var("MOON_TEST_AOF_FSYNC_STALL_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .is_some_and(|ms| ms > 0);
        if on && stall_hook {
            tracing::warn!(
                "MOON_TEST_AOF_FSYNC_STALL_MS is set: AOF records go through the writer \
                 thread (moon#1266 1A off for this test hook)"
            );
            return false;
        }
        on
    })
}

/// What the writer hands over with the append position: its record context
/// and the fold floor below which a record is already in the base (#455).
#[derive(Debug)]
pub(crate) struct DirectCtx {
    pub(crate) rec: RecordCtx,
    pub(crate) floor: FoldEpoch,
}

/// Outcome of [`AofLane::enqueue`].
pub(crate) enum Sent<'a> {
    /// In the lane buffer or in the channel.
    Ok,
    /// The channel is full. The caller holds `SlowSend` until its slow send
    /// (spill, park, or give up) is over: until then the writer does not hand
    /// the append position to the producers.
    Full(AofMessage, SlowSend<'a>),
    /// The writer is gone (the message comes back unsent).
    Disconnected(#[allow(dead_code)] AofMessage),
}

/// A producer between a full-channel `try_send` and the end of its slow
/// send (see [`super::lane_protocol::LaneCore::enter_slow`]).
#[must_use]
pub(crate) struct SlowSend<'a>(Option<&'a AofLane>);

impl Drop for SlowSend<'_> {
    fn drop(&mut self) {
        if let Some(lane) = self.0 {
            lane.core.lock().leave_slow();
        }
    }
}

/// One AOF writer's lane (see the module doc).
pub struct AofLane {
    /// Per-shard incr (`[u64 lsn][u32 len]` frames) vs bare RESP.
    framed: bool,
    /// [`enabled`] when the lane was made; `false` = Option 3, no locking.
    on: bool,
    core: parking_lot::Mutex<LaneCore<DirectCtx, std::fs::File>>,
    /// Framed bytes are pending (set and cleared under the lock; read
    /// without it on the flush fast path).
    dirty: AtomicBool,
    /// A direct write landed since the writer last looked (its everysec
    /// deadline: [`Self::take_written`]).
    written: AtomicBool,
    /// Direct writes issued (tests).
    #[cfg(test)]
    writes: AtomicU64,
}

impl std::fmt::Debug for AofLane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AofLane")
            .field("framed", &self.framed)
            .field("on", &self.on)
            .field("mode", &self.core.lock().mode())
            .finish()
    }
}

fn write_all(file: &mut std::fs::File, buf: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    file.write_all(buf)
}

/// Append one record (`[u64 lsn][u32 len]`-framed or bare) to `buf`.
#[inline]
fn push_record(buf: &mut Vec<u8>, framed: bool, lsn: u64, bytes: &[u8]) {
    if framed {
        buf.extend_from_slice(&lsn.to_le_bytes());
        buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    }
    buf.extend_from_slice(bytes);
}

impl AofLane {
    /// A lane for a writer whose incr is `framed` (per-shard) or bare RESP.
    pub fn new(framed: bool) -> Arc<Self> {
        Self::with_switch(framed, enabled())
    }

    /// [`Self::new`] with the switch given (tests).
    pub(crate) fn with_switch(framed: bool, on: bool) -> Arc<Self> {
        Arc::new(Self {
            framed,
            on,
            core: parking_lot::Mutex::new(LaneCore::new()),
            dirty: AtomicBool::new(false),
            written: AtomicBool::new(false),
            #[cfg(test)]
            writes: AtomicU64::new(0),
        })
    }

    /// Whether 1A is on for this lane.
    #[inline]
    pub(crate) fn is_on(&self) -> bool {
        self.on
    }

    /// The current mode (tests).
    #[cfg(test)]
    pub(crate) fn mode(&self) -> Mode {
        self.core.lock().mode()
    }

    /// Whether the calling thread is this lane's shard thread.
    fn is_home_thread(&self) -> bool {
        CURRENT
            .try_with(|c| {
                c.borrow()
                    .as_ref()
                    .is_some_and(|l| std::ptr::eq(Arc::as_ptr(l), self))
            })
            .unwrap_or(false)
    }

    /// The producers' one entry point (the pool's sends): DIRECT frames an
    /// `Append` into the buffer — written at the next flush point, or at once
    /// from a thread that is not this lane's shard thread; any other message
    /// flips the lane back to the writer (buffer written first) and goes to
    /// the channel; WRITER/CLOSED send to the channel. The send happens under
    /// the lane lock, so the writer's hand-over check sees it.
    pub(crate) fn enqueue<'a>(
        &'a self,
        msg: AofMessage,
        tx: &channel::MpscSender<AofMessage>,
    ) -> Sent<'a> {
        if !self.on {
            return match tx.try_send(msg) {
                Ok(()) => Sent::Ok,
                Err(flume::TrySendError::Full(m)) => Sent::Full(m, SlowSend(None)),
                Err(flume::TrySendError::Disconnected(m)) => Sent::Disconnected(m),
            };
        }
        let mut core = self.core.lock();
        if core.mode() == Mode::Direct {
            if let AofMessage::Append {
                lsn,
                db,
                bytes,
                epoch,
                clock_ms,
                txn,
            } = &msg
            {
                let framed = self.framed;
                let buffered = core.frame_with(|ctx, buf| {
                    frame_append(ctx, buf, framed, *lsn, *db, bytes, *epoch, *clock_ms, *txn);
                });
                if buffered {
                    if core.has_pending() {
                        self.dirty.store(true, Ordering::Release);
                        if self.is_home_thread() {
                            let _ = HOME_APPENDS.try_with(|n| n.set(n.get().wrapping_add(1)));
                        } else {
                            self.flush_locked(&mut core);
                        }
                    }
                    return Sent::Ok;
                }
            } else {
                // A non-Append message must come after every buffered record.
                self.flip_locked(&mut core);
            }
        }
        match tx.try_send(msg) {
            Ok(()) => Sent::Ok,
            Err(flume::TrySendError::Full(m)) => {
                core.enter_slow();
                Sent::Full(m, SlowSend(Some(self)))
            }
            Err(flume::TrySendError::Disconnected(m)) => Sent::Disconnected(m),
        }
    }

    /// Flip the lane back to the writer before a message is sent around
    /// [`Self::enqueue`] (control messages: `Rewrite*`, `Shutdown`).
    pub(crate) fn flip(&self) {
        if self.on {
            self.flip_locked(&mut self.core.lock());
        }
    }

    /// Write what is buffered (one `write(2)`). Cheap when nothing is.
    #[inline]
    pub fn flush(&self) {
        if !self.dirty.load(Ordering::Acquire) {
            return;
        }
        self.flush_locked(&mut self.core.lock());
    }

    fn flush_locked(&self, core: &mut LaneCore<DirectCtx, std::fs::File>) {
        let flushed = core.flush(write_all);
        self.after_write(flushed);
    }

    fn flip_locked(&self, core: &mut LaneCore<DirectCtx, std::fs::File>) {
        let flushed = core.flip(write_all);
        self.after_write(flushed);
    }

    /// Bookkeeping after a write under the lock: the buffer is empty now.
    fn after_write(&self, flushed: Flushed) {
        self.dirty.store(false, Ordering::Release);
        match flushed {
            Flushed::Nothing => {}
            Flushed::Wrote(_) => {
                AOF_LANE_WRITES.fetch_add(1, Ordering::Relaxed);
                #[cfg(test)]
                self.writes.fetch_add(1, Ordering::Relaxed);
                self.written.store(true, Ordering::Release);
            }
            Flushed::Failed => error!(
                "AOF direct write failed: the stream may be torn; the AOF writer latches \
                 its write error and appends nothing more until a rewrite. Persistence \
                 degraded."
            ),
        }
    }

    /// The writer, on every wake: whether a direct write landed since its
    /// last look (it then owes the everysec fsync).
    #[inline]
    pub(crate) fn take_written(&self) -> bool {
        self.on && self.written.swap(false, Ordering::AcqRel)
    }

    /// Whether the producers hold the append position.
    #[inline]
    pub(crate) fn is_direct(&self) -> bool {
        self.on && self.core.lock().mode() == Mode::Direct
    }

    /// The writer, before it handles any message and before any stop path:
    /// the append position is the writer's again (buffer written first).
    pub(crate) fn take_back(&self) -> TakenBack<DirectCtx> {
        let mut core = self.core.lock();
        let taken = core.take_back(write_all);
        self.after_write(taken.flushed);
        taken
    }

    /// The writer exits (also on unwind): CLOSED for good.
    pub(crate) fn close(&self) -> TakenBack<DirectCtx> {
        let mut core = self.core.lock();
        let taken = core.close(write_all);
        self.after_write(taken.flushed);
        taken
    }

    /// Whether the writer may try to hand the append position over now (a
    /// cheap check before it dups its file).
    pub(crate) fn may_release(&self, rx: &channel::MpscReceiver<AofMessage>) -> bool {
        self.on && {
            let core = self.core.lock();
            core.may_release() && rx.is_empty()
        }
    }

    /// The writer hands its append position to the producers: `ctx` moves
    /// into the lane (left as a fresh context; the writer takes the real one
    /// back with [`Self::take_back`]) and `file` (a dup of the writer's file)
    /// is what they write with. Refused (`false`, `ctx` untouched) unless the
    /// channel is empty and no producer is in a slow send — checked under the
    /// lock every producer sends under.
    pub(crate) fn release(
        &self,
        rx: &channel::MpscReceiver<AofMessage>,
        ctx: &mut RecordCtx,
        floor: FoldEpoch,
        file: std::fs::File,
    ) -> bool {
        if !self.on {
            return false;
        }
        let mut core = self.core.lock();
        let empty = rx.is_empty();
        let direct = DirectCtx {
            rec: std::mem::take(ctx),
            floor,
        };
        match core.release(direct, file, empty) {
            Ok(()) => true,
            Err((back, _file)) => {
                *ctx = back.rec;
                false
            }
        }
    }
}

/// Frame one `Append` exactly as the writer loops do
/// ([`super::inject_record_prefixes`], then the record): a record already in
/// the committed base is dropped (#455) before the context moves; then its
/// prefixes (session stamp, `MOON.TXN RESET`, `MOON.TS`, `MOON.TXN
/// BEGIN|PAUSE`, `SELECT`) with lsn 0, then the record. A zero-length payload
/// writes nothing.
#[allow(clippy::too_many_arguments)]
#[inline]
fn frame_append(
    ctx: &mut DirectCtx,
    buf: &mut Vec<u8>,
    framed: bool,
    lsn: u64,
    db: usize,
    bytes: &[u8],
    epoch: FoldEpoch,
    clock_ms: u64,
    txn: u64,
) {
    if bytes.is_empty() {
        return;
    }
    if epoch.folded_below(ctx.floor) {
        super::AOF_REWRITE_LATE_RECORDS_FOLDED.fetch_add(1, Ordering::Relaxed);
        return;
    }
    for prefix in ctx.rec.prefix_for(db, clock_ms, txn, false) {
        push_record(buf, framed, 0, &prefix);
    }
    push_record(buf, framed, lsn, bytes);
}

/// Closes the writer's lane when the writer task ends, however it ends.
pub(crate) struct CloseOnExit(pub(crate) Arc<AofLane>);

impl Drop for CloseOnExit {
    fn drop(&mut self) {
        if self.0.on {
            let _ = self.0.close();
        }
    }
}

thread_local! {
    /// The lane of the shard this thread runs (see [`install_for_shard`]).
    static CURRENT: RefCell<Option<Arc<AofLane>>> = const { RefCell::new(None) };
    /// Replies are written inside their task (every driver but io_uring
    /// without SQPOLL): the reply macros flush first.
    static INLINE_REPLIES: Cell<bool> = const { Cell::new(true) };
    /// Records this thread buffered into its own lane (wrapping count): tells
    /// [`flush_before_reply_coalesced`] whether its yield let other
    /// connections add theirs.
    static HOME_APPENDS: Cell<u64> = const { Cell::new(0) };
    /// [`flush_before_reply_coalesced`]'s adaptivity (see there).
    static COALESCE: Cell<Coalesce> = const { Cell::new(Coalesce { misses: 0, skip: 0 }) };
}

/// Whether a reply on this thread yields before it flushes.
#[derive(Clone, Copy)]
struct Coalesce {
    /// Consecutive yields after which no other connection had appended.
    misses: u32,
    /// Replies left to flush without yielding (after a run of misses).
    skip: u32,
}

/// Whether a task that wakes itself is queued BEHIND the tasks already
/// runnable (tokio's current-thread scheduler), so a yield lets the round's
/// other connections run first. Not under monoio (see
/// [`flush_before_reply_coalesced`]).
const YIELD_REACHES_THE_ROUND: bool = !cfg!(feature = "runtime-monoio");

/// A run of this many fruitless yields (a lone connection) ...
const COALESCE_MISSES: u32 = 16;
/// ... stops the yielding for this many replies, then it is tried again.
const COALESCE_SKIP: u32 = 256;

/// Return `Pending` once (waking itself), so every task already runnable on
/// this thread runs before the caller resumes.
struct YieldOnce(bool);

impl std::future::Future for YieldOnce {
    type Output = ();
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        if self.0 {
            return std::task::Poll::Ready(());
        }
        self.0 = true;
        cx.waker().wake_by_ref();
        std::task::Poll::Pending
    }
}

/// [`flush_before_reply`] for a connection's reply write, coalesced per
/// scheduler round under tokio: when this thread has records buffered, the
/// reply first yields once, so the other connections that are ready in the
/// same round run their commands too, and ONE `write(2)` then covers all of
/// them (the first of them to resume writes; the rest find the lane clean) —
/// redis's one write per event-loop iteration, not one per connection. A lone
/// connection gains nothing from the yield: after [`COALESCE_MISSES`]
/// fruitless yields in a row the replies stop yielding for [`COALESCE_SKIP`]
/// replies, then try again. The write still precedes the reply in every case.
///
/// monoio re-polls a task that woke itself BEFORE the rest of its queue
/// (`LocalScheduler::yield_now` pushes it to the front), so a yield cannot
/// let the round's other connections in: on monoio's epoll/kqueue driver (and
/// SQPOLL) the reply flushes at once — one write per connection batch. (Its
/// io_uring driver never gets here: the before-submit hook writes once per
/// iteration.)
pub async fn flush_before_reply_coalesced() {
    if !INLINE_REPLIES.try_with(Cell::get).unwrap_or(true) || !current_dirty() {
        return;
    }
    if !YIELD_REACHES_THE_ROUND {
        flush_current();
        return;
    }
    let mut state = COALESCE
        .try_with(Cell::get)
        .unwrap_or(Coalesce { misses: 0, skip: 0 });
    if state.skip > 0 {
        state.skip -= 1;
    } else {
        let before = HOME_APPENDS.try_with(Cell::get).unwrap_or(0);
        YieldOnce(false).await;
        if HOME_APPENDS.try_with(Cell::get).unwrap_or(0) == before {
            state.misses += 1;
            if state.misses >= COALESCE_MISSES {
                state = Coalesce {
                    misses: 0,
                    skip: COALESCE_SKIP,
                };
            }
        } else {
            state.misses = 0;
        }
    }
    let _ = COALESCE.try_with(|c| c.set(state));
    flush_current();
}

/// Whether this thread's lane holds buffered records.
#[inline]
fn current_dirty() -> bool {
    CURRENT
        .try_with(|c| {
            c.borrow()
                .as_ref()
                .is_some_and(|l| l.dirty.load(Ordering::Acquire))
        })
        .unwrap_or(false)
}

/// Bind this shard thread to its writer's lane (1A on and an AOF pool): its
/// appends are then written at its flush points. On monoio with an io_uring
/// driver that submits only on `io_uring_enter`, the driver's before-submit
/// hook is the flush point for replies; otherwise the reply macros are.
pub fn install_for_shard(shard_id: usize, pool: Option<&Arc<super::AofWriterPool>>) {
    let Some(pool) = pool else {
        return;
    };
    if !enabled() {
        return;
    }
    let lane = Arc::clone(pool.lane_for(shard_id));
    let _ = CURRENT.try_with(|c| *c.borrow_mut() = Some(lane));
    #[cfg(feature = "runtime-monoio")]
    {
        let gated = monoio::set_before_submit_hook(Some(flush_current_hook));
        let _ = INLINE_REPLIES.try_with(|c| c.set(!gated));
        tracing::info!(
            "Shard {shard_id}: AOF records are written on the shard thread (moon#1266 1A), \
             before {}",
            if gated {
                "each io_uring submit"
            } else {
                "each reply write"
            }
        );
    }
    #[cfg(not(feature = "runtime-monoio"))]
    tracing::info!(
        "Shard {shard_id}: AOF records are written on the shard thread (moon#1266 1A), before \
         each reply write"
    );
}

/// Unbind (shard thread exit), writing what is buffered.
pub fn uninstall_for_shard() {
    flush_current();
    #[cfg(feature = "runtime-monoio")]
    if CURRENT.try_with(|c| c.borrow().is_some()).unwrap_or(false) {
        let _ = monoio::set_before_submit_hook(None);
    }
    let _ = CURRENT.try_with(|c| c.borrow_mut().take());
}

#[cfg(feature = "runtime-monoio")]
fn flush_current_hook() {
    flush_current();
}

/// Write this thread's buffered AOF records now (one `write(2)`; nothing
/// when clean or unbound). Call before anything that acknowledges a write
/// leaves the thread.
#[inline]
pub fn flush_current() {
    let _ = CURRENT.try_with(|c| {
        if let Some(lane) = c.borrow().as_ref() {
            lane.flush();
        }
    });
}

/// [`flush_current`] for a reply about to be written to a socket by its
/// task: needed unless the io_uring driver's submit hook flushes instead.
#[inline]
pub fn flush_before_reply() {
    if INLINE_REPLIES.try_with(Cell::get).unwrap_or(true) {
        flush_current();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::aof::record_ctx::UNKNOWN_DB;
    use bytes::Bytes;

    fn append(db: usize, clock_ms: u64, txn: u64, body: &'static [u8]) -> AofMessage {
        AofMessage::Append {
            lsn: 9,
            db,
            bytes: Bytes::from_static(body),
            epoch: FoldEpoch(3),
            clock_ms,
            txn,
        }
    }

    /// What the writer loops write for `msgs`: `inject_record_prefixes`, then
    /// each body framed (per-shard) or bare (TopLevel).
    fn writer_bytes(
        ctx: &mut RecordCtx,
        floor: FoldEpoch,
        framed: bool,
        msgs: Vec<AofMessage>,
    ) -> Vec<u8> {
        let mut out = Vec::new();
        for m in crate::persistence::aof::inject_record_prefixes(msgs, floor, ctx) {
            if let AofMessage::Append { lsn, bytes, .. } = m {
                if bytes.is_empty() {
                    continue;
                }
                push_record(&mut out, framed, lsn, &bytes);
            }
        }
        out
    }

    fn lane_bytes(
        ctx: RecordCtx,
        floor: FoldEpoch,
        framed: bool,
        msgs: Vec<AofMessage>,
    ) -> (Vec<u8>, RecordCtx) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("incr");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .expect("open");
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(8);
        let lane = AofLane::with_switch(framed, true);
        let mut ctx = ctx;
        assert!(lane.release(&rx, &mut ctx, floor, file));
        assert_eq!(lane.mode(), Mode::Direct);
        for m in msgs {
            assert!(matches!(lane.enqueue(m, &tx), Sent::Ok));
        }
        assert!(rx.is_empty(), "DIRECT must not use the channel");
        let back = lane.take_back();
        assert!(!back.write_failed);
        (
            std::fs::read(&path).expect("read"),
            back.ctx.expect("ctx").rec,
        )
    }

    fn mixed() -> Vec<AofMessage> {
        vec![
            append(0, 1_000, 0, b"*1\r\n$1\r\na\r\n"),
            append(0, 1_000, 0, b"*1\r\n$1\r\nb\r\n"),
            append(2, 1_000, 0, b"*1\r\n$1\r\nc\r\n"),
            append(2, 1_001, 5, b"*1\r\n$1\r\nd\r\n"),
            append(2, 1_001, 0, b"*1\r\n$1\r\ne\r\n"),
            append(
                2,
                1_001,
                5 | crate::persistence::aof::TXN_END_FLAG,
                b"*1\r\n$1\r\nf\r\n",
            ),
            append(0, 0, 0, b""),
            append(1, 0, 0, b"*1\r\n$1\r\ng\r\n"),
        ]
    }

    #[test]
    fn direct_framing_is_byte_identical_to_the_writer_framed_and_bare() {
        for framed in [true, false] {
            let mut wctx = RecordCtx::new();
            let want = writer_bytes(&mut wctx, FoldEpoch::INITIAL, framed, mixed());
            let (got, lctx) = lane_bytes(RecordCtx::new(), FoldEpoch::INITIAL, framed, mixed());
            assert_eq!(got, want, "framed={framed}");
            // The context comes back where the writer's would be.
            assert_eq!(lctx.db(), wctx.db());
            assert_eq!(lctx.ts_ms(), wctx.ts_ms());
            assert_eq!(lctx.txn(), wctx.txn());
        }
    }

    #[test]
    fn a_reopened_stream_gets_its_select_and_session_stamp_from_the_lane_too() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("moon.aof.1.incr.aof");
        let mut wctx = RecordCtx::appending(&path);
        assert_eq!(wctx.db(), UNKNOWN_DB);
        let want = writer_bytes(&mut wctx, FoldEpoch::INITIAL, false, mixed());
        let (got, _) = lane_bytes(
            RecordCtx::appending(&path),
            FoldEpoch::INITIAL,
            false,
            mixed(),
        );
        // The session stamp carries the writer's clock when the first record
        // has none; the first record here has one, so the bytes match.
        assert_eq!(got, want);
        assert!(
            got.windows(6).any(|w| w == b"SELECT"),
            "the first record selects its db"
        );
    }

    #[test]
    fn records_below_the_fold_floor_are_dropped_like_the_writer_does() {
        let floor = FoldEpoch(4); // the records are stamped 3
        let mut wctx = RecordCtx::new();
        let want = writer_bytes(&mut wctx, floor, true, mixed());
        assert!(want.is_empty());
        let (got, _) = lane_bytes(RecordCtx::new(), floor, true, mixed());
        assert!(got.is_empty());
    }

    #[test]
    fn a_non_append_message_flips_the_lane_after_writing_the_buffer() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("incr");
        let file = std::fs::File::create(&path).expect("create");
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(8);
        let lane = AofLane::with_switch(false, true);
        let mut ctx = RecordCtx::new();
        assert!(lane.release(&rx, &mut ctx, FoldEpoch::INITIAL, file));
        // Buffered, not yet written (this test thread is not the lane's
        // shard thread, so it writes at once: bind it first).
        let _ = CURRENT.try_with(|c| *c.borrow_mut() = Some(Arc::clone(&lane)));
        assert!(matches!(
            lane.enqueue(append(0, 0, 0, b"*1\r\n$1\r\na\r\n"), &tx),
            Sent::Ok
        ));
        assert!(std::fs::read(&path).expect("read").is_empty());
        let (ack, _rx) = channel::oneshot();
        let sync = AofMessage::AppendSync {
            lsn: 0,
            db: 0,
            bytes: Bytes::new(),
            ack,
            epoch: FoldEpoch::INITIAL,
            clock_ms: 0,
            txn: 0,
        };
        assert!(matches!(lane.enqueue(sync, &tx), Sent::Ok));
        assert_eq!(lane.mode(), Mode::Writer);
        assert!(
            !std::fs::read(&path).expect("read").is_empty(),
            "written before the flip"
        );
        assert_eq!(rx.len(), 1);
        // Now WRITER: the next append goes to the channel, behind the barrier.
        assert!(matches!(
            lane.enqueue(append(0, 0, 0, b"*1\r\n$1\r\nb\r\n"), &tx),
            Sent::Ok
        ));
        assert_eq!(rx.len(), 2);
        let _ = CURRENT.try_with(|c| c.borrow_mut().take());
    }

    #[test]
    fn a_full_channel_holds_the_hand_over_until_the_slow_send_ends() {
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(1);
        let lane = AofLane::with_switch(false, true);
        assert!(matches!(lane.enqueue(append(0, 0, 0, b"x"), &tx), Sent::Ok));
        let slow = match lane.enqueue(append(0, 0, 0, b"y"), &tx) {
            Sent::Full(_, slow) => slow,
            _ => panic!("channel of 1 must be full"),
        };
        let _ = rx.try_recv();
        assert!(rx.is_empty());
        assert!(!lane.may_release(&rx), "a slow sender is still out");
        drop(slow);
        assert!(lane.may_release(&rx));
    }

    #[test]
    fn a_foreign_thread_writes_at_once_and_the_writer_learns_of_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("incr");
        let file = std::fs::File::create(&path).expect("create");
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(8);
        let lane = AofLane::with_switch(false, true);
        let mut ctx = RecordCtx::new();
        assert!(lane.release(&rx, &mut ctx, FoldEpoch::INITIAL, file));
        assert!(!lane.take_written());
        assert!(matches!(
            lane.enqueue(append(0, 0, 0, b"*1\r\n$1\r\na\r\n"), &tx),
            Sent::Ok
        ));
        assert!(!std::fs::read(&path).expect("read").is_empty());
        assert!(lane.take_written());
        assert!(!lane.take_written());
    }

    /// A released lane bound to this thread, over a temp file.
    #[cfg(not(feature = "runtime-monoio"))]
    fn bound_direct_lane() -> (
        Arc<AofLane>,
        channel::MpscSender<AofMessage>,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = std::fs::File::create(dir.path().join("incr")).expect("create");
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(8);
        let lane = AofLane::with_switch(false, true);
        let mut ctx = RecordCtx::new();
        assert!(lane.release(&rx, &mut ctx, FoldEpoch::INITIAL, file));
        let _ = CURRENT.try_with(|c| *c.borrow_mut() = Some(Arc::clone(&lane)));
        let _ = INLINE_REPLIES.try_with(|c| c.set(true));
        let _ = COALESCE.try_with(|c| c.set(Coalesce { misses: 0, skip: 0 }));
        drop(rx);
        (lane, tx, dir)
    }

    #[cfg(not(feature = "runtime-monoio"))]
    fn unbind() {
        let _ = CURRENT.try_with(|c| c.borrow_mut().take());
    }

    /// Connections ready in the same scheduler round share ONE write: each
    /// appends, then yields at its reply; the first to resume writes them all.
    /// (The round here is a tokio `LocalSet`; under the monoio feature the
    /// reply does not yield — see `YIELD_REACHES_THE_ROUND`.)
    #[cfg(not(feature = "runtime-monoio"))]
    #[test]
    fn replies_in_one_round_share_one_write() {
        let (lane, tx, dir) = bound_direct_lane();
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("rt");
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async {
            let mut tasks = Vec::new();
            for i in 0..5u8 {
                let lane = Arc::clone(&lane);
                let tx = tx.clone();
                tasks.push(tokio::task::spawn_local(async move {
                    let body: &'static [u8] = match i {
                        0 => b"*1\r\n$1\r\na\r\n",
                        1 => b"*1\r\n$1\r\nb\r\n",
                        2 => b"*1\r\n$1\r\nc\r\n",
                        3 => b"*1\r\n$1\r\nd\r\n",
                        _ => b"*1\r\n$1\r\ne\r\n",
                    };
                    assert!(matches!(lane.enqueue(append(0, 0, 0, body), &tx), Sent::Ok));
                    flush_before_reply_coalesced().await;
                    // The reply may go now: this task's record is written.
                    assert!(!lane.dirty.load(Ordering::Acquire));
                }));
            }
            for t in tasks {
                t.await.expect("task");
            }
        });
        assert_eq!(
            lane.writes.load(Ordering::Relaxed),
            1,
            "one write for the round"
        );
        let got = std::fs::read(dir.path().join("incr")).expect("read");
        assert_eq!(
            got.iter().filter(|&&b| b == b'*').count(),
            5,
            "every record written"
        );
        unbind();
    }

    /// A lone connection: after a run of fruitless yields the replies stop
    /// yielding (and still write before every reply).
    #[cfg(not(feature = "runtime-monoio"))]
    #[test]
    fn a_lone_connection_stops_yielding() {
        let (lane, tx, _dir) = bound_direct_lane();
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("rt");
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async {
            for _ in 0..COALESCE_MISSES {
                assert!(matches!(
                    lane.enqueue(append(0, 0, 0, b"*1\r\n$1\r\nx\r\n"), &tx),
                    Sent::Ok
                ));
                flush_before_reply_coalesced().await;
                assert!(!lane.dirty.load(Ordering::Acquire));
            }
        });
        let state = COALESCE.try_with(Cell::get).expect("tls");
        assert_eq!(
            state.skip, COALESCE_SKIP,
            "the misses switched yielding off"
        );
        assert_eq!(
            lane.writes.load(Ordering::Relaxed),
            u64::from(COALESCE_MISSES),
            "one write per reply when nothing coalesces"
        );
        unbind();
    }

    #[test]
    fn the_switch_parses_both_ways_and_defaults() {
        for on in ["1", "on", "YES", " true "] {
            assert!(parse_switch(Some(on)), "{on}");
        }
        for off in ["0", "off", "No", "false"] {
            assert!(!parse_switch(Some(off)), "{off}");
        }
        assert_eq!(parse_switch(None), DEFAULT_ON);
        assert_eq!(parse_switch(Some("maybe")), DEFAULT_ON);
    }

    #[test]
    fn switched_off_the_lane_is_a_plain_channel() {
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(8);
        let lane = AofLane::with_switch(false, false);
        let mut ctx = RecordCtx::new();
        let file = tempfile::tempfile().expect("tempfile");
        assert!(!lane.release(&rx, &mut ctx, FoldEpoch::INITIAL, file));
        assert!(matches!(lane.enqueue(append(0, 0, 0, b"a"), &tx), Sent::Ok));
        assert_eq!(rx.len(), 1);
        assert!(!lane.take_written());
    }
}
