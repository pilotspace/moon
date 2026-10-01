//! The ownership protocol of an AOF lane (moon#1266 Option 1A): who may
//! append to an AOF stream — the writer thread, or the producers themselves
//! (the shard thread that executed the write, before its replies leave).
//!
//! An AOF writer's stream has ONE append position: the file offset, plus the
//! record context (`SELECT` / `MOON.TS` / `MOON.TXN` state, `RecordCtx`) that
//! decides what the next record must be prefixed with. Option 1A lets the
//! shard thread write its own records with one `write(2)` per event-loop
//! iteration, before that iteration's replies; the writer thread keeps
//! everything else (the everysec fsync, rewrites, generation switches, the
//! clean-close marker). This file is the state machine that hands the append
//! position between them. Every operation runs under the lane's mutex (the
//! caller holds it), so the protocol is a plain struct; the guarantees come
//! from WHEN the callers take the lock:
//!
//! ```text
//!                  writer: release (channel empty, no slow sender)
//!        WRITER ─────────────────────────────────────────────▶ DIRECT
//!          ▲  ▲                                                  │
//!          │  └── anyone: flip (buffer written first) ───────────┘
//!          │        a producer about to send a non-Append message, a failed
//!          │        flush, the writer on any message / stop / policy change
//!          └── writer exits: close (from either mode) ──▶ CLOSED
//! ```
//!
//! * **WRITER**: producers send every record to the writer's channel, the
//!   writer frames and writes them (Option 3). A producer decides "channel"
//!   while holding the lock and sends before releasing it.
//! * **DIRECT**: producers frame `Append`s into [`LaneCore::buf`] with the
//!   context the writer handed over; whoever flushes writes the whole buffer
//!   with ONE write. The channel holds nothing: any non-`Append` message is
//!   sent only after a flip, so every message in the channel comes after every
//!   buffered record, and the writer's [`LaneCore::take_back`] (which flips
//!   first) writes the buffer before the message.
//! * **release** happens only when the channel is empty AND no producer is
//!   between a full-channel `try_send` and its slow send ([`LaneCore::enter_slow`]),
//!   both checked under the lock — so no record can still be on its way to the
//!   channel when records start bypassing it.
//! * **CLOSED**: the writer is gone; producers fall through to the (closed)
//!   channel and get its errors. A buffer is never written after `close`.
//!
//! Exactly one side owns the append position at any instant, and it changes
//! hands only under the lock, so the file holds records in lock order — the
//! order producers appended them.
//!
//! This file is compiled into `tests/loom_aof_lane.rs` through `#[path]`;
//! keep it self-contained (`std` only).

/// The buffer keeps at least this much capacity between writes (the AOF
/// writer's `group_commit::BATCH_BUF_RETAIN_FLOOR`)...
const BUF_RETAIN_FLOOR: usize = 64 * 1024;

/// ...and gives a burst's capacity back only after this many consecutive
/// writes used at most a quarter of it (`BATCH_BUF_SHRINK_AFTER`): a steady
/// load settles to zero allocations per write, a one-off burst does not pin
/// megabytes per shard forever.
const BUF_SHRINK_AFTER: u32 = 64;

/// Who owns the append position (see the module doc).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    /// The writer thread: producers send to its channel.
    Writer,
    /// The producers: they frame into the lane buffer.
    Direct,
    /// The writer exited: producers reach its closed channel.
    Closed,
}

/// What [`LaneCore::flush`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Flushed {
    /// Nothing was buffered (or the lane is not DIRECT).
    Nothing,
    /// The buffer (this many bytes) was written.
    Wrote(usize),
    /// The write failed: the lane flipped to WRITER with its latch set.
    Failed,
}

/// What [`LaneCore::take_back`] hands the writer.
#[derive(Debug)]
pub(crate) struct TakenBack<C> {
    /// The context the writer had handed over, if it had (it is the writer's
    /// again: the append position is back on the writer).
    pub ctx: Option<C>,
    /// A direct write failed since the writer last looked: the stream may be
    /// torn, so the writer must latch its write-error state.
    pub write_failed: bool,
    /// The buffer that was still pending, written by this call.
    pub flushed: Flushed,
}

/// The lane state (see the module doc). `C` is the record context the writer
/// hands over, `F` the file handle the producers write with.
#[derive(Debug)]
pub(crate) struct LaneCore<C, F> {
    mode: Mode,
    /// DIRECT: the writer's context. After a flip: parked here until the
    /// writer takes it back.
    ctx: Option<C>,
    /// DIRECT only: the producers' handle on the writer's file.
    file: Option<F>,
    /// DIRECT only: framed records not yet written.
    buf: Vec<u8>,
    /// A direct write failed and the writer has not taken it back yet.
    write_failed: bool,
    /// Producers between a full-channel `try_send` (decided under the lock)
    /// and the end of their slow send (spill, park, or give up).
    slow_senders: usize,
    /// Consecutive small writes (see [`BUF_SHRINK_AFTER`]).
    small_streak: u32,
}

impl<C, F> Default for LaneCore<C, F> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C, F> LaneCore<C, F> {
    /// A lane in WRITER mode with nothing handed over.
    #[must_use]
    pub(crate) const fn new() -> Self {
        Self {
            mode: Mode::Writer,
            ctx: None,
            file: None,
            buf: Vec::new(),
            write_failed: false,
            slow_senders: 0,
            small_streak: 0,
        }
    }

    /// The current mode.
    #[inline]
    #[must_use]
    pub(crate) fn mode(&self) -> Mode {
        self.mode
    }

    /// Whether framed bytes wait in the buffer.
    #[inline]
    #[must_use]
    pub(crate) fn has_pending(&self) -> bool {
        !self.buf.is_empty()
    }

    /// DIRECT: let `frame` append one record (and its prefixes) to the
    /// buffer with the handed-over context; returns `true`. Otherwise returns
    /// `false` without calling it — the caller sends the record to the
    /// channel BEFORE releasing the lock.
    #[inline]
    pub(crate) fn frame_with(&mut self, frame: impl FnOnce(&mut C, &mut Vec<u8>)) -> bool {
        if self.mode != Mode::Direct {
            return false;
        }
        match self.ctx.as_mut() {
            Some(ctx) => {
                frame(ctx, &mut self.buf);
                true
            }
            // Unreachable: DIRECT always holds the context. Refuse rather
            // than frame without it.
            None => false,
        }
    }

    /// DIRECT: write the buffer with ONE `write` call. On failure the stream
    /// may be torn: the buffer is dropped, the latch set, and the lane flips
    /// to WRITER (the writer latches at its next look and appends nothing
    /// more). The buffer is kept allocated for reuse.
    pub(crate) fn flush(
        &mut self,
        write: impl FnOnce(&mut F, &[u8]) -> std::io::Result<()>,
    ) -> Flushed {
        if self.mode != Mode::Direct || self.buf.is_empty() {
            return Flushed::Nothing;
        }
        let Some(file) = self.file.as_mut() else {
            return Flushed::Nothing;
        };
        let len = self.buf.len();
        let result = write(file, &self.buf);
        self.retain_or_shrink();
        match result {
            Ok(()) => Flushed::Wrote(len),
            Err(_) => {
                self.write_failed = true;
                self.mode = Mode::Writer;
                self.file = None;
                Flushed::Failed
            }
        }
    }

    /// Empty the buffer after a write, keeping its capacity unless a long run
    /// of small writes says a burst is over (shrink hysteresis).
    fn retain_or_shrink(&mut self) {
        let cap = self.buf.capacity();
        if cap > BUF_RETAIN_FLOOR && self.buf.len() <= cap / 4 {
            self.small_streak += 1;
            if self.small_streak >= BUF_SHRINK_AFTER {
                self.buf.clear();
                self.buf.shrink_to(BUF_RETAIN_FLOOR);
                self.small_streak = 0;
            }
        } else {
            self.small_streak = 0;
        }
        self.buf.clear();
    }

    /// DIRECT → WRITER, writing the buffer first. The context stays parked
    /// until the writer takes it back. A no-op in any other mode.
    pub(crate) fn flip(
        &mut self,
        write: impl FnOnce(&mut F, &[u8]) -> std::io::Result<()>,
    ) -> Flushed {
        if self.mode != Mode::Direct {
            return Flushed::Nothing;
        }
        let flushed = self.flush(write);
        self.mode = Mode::Writer;
        self.file = None;
        flushed
    }

    /// The writer, before it handles any message or stops: flip (writing the
    /// buffer) and take the context back.
    pub(crate) fn take_back(
        &mut self,
        write: impl FnOnce(&mut F, &[u8]) -> std::io::Result<()>,
    ) -> TakenBack<C> {
        let flushed = self.flip(write);
        TakenBack {
            ctx: self.ctx.take(),
            write_failed: std::mem::take(&mut self.write_failed),
            flushed,
        }
    }

    /// The writer hands the append position over: WRITER → DIRECT, if
    /// `channel_empty` (read by the caller under the same lock) and no
    /// producer is in a slow send. Refused, the arguments come back.
    pub(crate) fn release(&mut self, ctx: C, file: F, channel_empty: bool) -> Result<(), (C, F)> {
        if self.mode != Mode::Writer
            || !channel_empty
            || self.slow_senders != 0
            || self.write_failed
            || self.ctx.is_some()
        {
            return Err((ctx, file));
        }
        self.ctx = Some(ctx);
        self.file = Some(file);
        self.mode = Mode::Direct;
        Ok(())
    }

    /// Whether [`Self::release`] could succeed now, the channel aside (a
    /// cheap pre-check before the caller dups its file).
    #[inline]
    #[must_use]
    pub(crate) fn may_release(&self) -> bool {
        self.mode == Mode::Writer
            && self.slow_senders == 0
            && !self.write_failed
            && self.ctx.is_none()
    }

    /// The writer exits: write what is buffered (DIRECT), then CLOSED for
    /// good. Returns the parked context, if any (the writer's final records
    /// need it).
    pub(crate) fn close(
        &mut self,
        write: impl FnOnce(&mut F, &[u8]) -> std::io::Result<()>,
    ) -> TakenBack<C> {
        let taken = self.take_back(write);
        self.mode = Mode::Closed;
        taken
    }

    /// A producer found the channel full (under the lock) and is about to
    /// leave it with its record for a slow send.
    #[inline]
    pub(crate) fn enter_slow(&mut self) {
        self.slow_senders += 1;
    }

    /// That producer's slow send ended (sent, spilled, or given up).
    #[inline]
    pub(crate) fn leave_slow(&mut self) {
        self.slow_senders = self.slow_senders.saturating_sub(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Core = LaneCore<u32, Vec<u8>>;

    fn ok(f: &mut Vec<u8>, b: &[u8]) -> std::io::Result<()> {
        f.extend_from_slice(b);
        Ok(())
    }

    fn fail(_: &mut Vec<u8>, _: &[u8]) -> std::io::Result<()> {
        Err(std::io::Error::other("injected"))
    }

    fn direct() -> Core {
        let mut c = Core::new();
        assert!(c.release(7, Vec::new(), true).is_ok());
        c
    }

    #[test]
    fn writer_mode_never_frames() {
        let mut c = Core::new();
        assert!(!c.frame_with(|_, _| panic!("framed in WRITER")));
        assert_eq!(c.flush(ok), Flushed::Nothing);
    }

    #[test]
    fn direct_frames_with_the_handed_over_context_and_flushes_once() {
        let mut c = direct();
        assert!(c.frame_with(|ctx, buf| {
            *ctx += 1;
            buf.extend_from_slice(b"a");
        }));
        assert!(c.frame_with(|_, buf| buf.extend_from_slice(b"b")));
        assert!(c.has_pending());
        assert_eq!(c.flush(ok), Flushed::Wrote(2));
        assert!(!c.has_pending());
        let back = c.take_back(ok);
        assert_eq!(back.ctx, Some(8));
        assert!(!back.write_failed);
        assert_eq!(c.mode(), Mode::Writer);
    }

    #[test]
    fn release_needs_an_empty_channel_and_no_slow_sender() {
        let mut c = Core::new();
        assert!(c.release(1, Vec::new(), false).is_err());
        c.enter_slow();
        assert!(!c.may_release());
        assert!(c.release(1, Vec::new(), true).is_err());
        c.leave_slow();
        assert!(c.may_release());
        assert!(c.release(1, Vec::new(), true).is_ok());
        // Already DIRECT.
        assert!(c.release(2, Vec::new(), true).is_err());
    }

    #[test]
    fn a_flip_writes_the_buffer_and_parks_the_context_for_the_writer() {
        let mut c = direct();
        assert!(c.frame_with(|_, buf| buf.extend_from_slice(b"xy")));
        assert_eq!(c.flip(ok), Flushed::Wrote(2));
        assert_eq!(c.mode(), Mode::Writer);
        assert!(!c.frame_with(|_, _| panic!("framed after a flip")));
        // Not released again while the context is parked.
        assert!(!c.may_release());
        let back = c.take_back(ok);
        assert_eq!(back.ctx, Some(7));
        assert!(c.may_release());
    }

    #[test]
    fn a_failed_write_latches_until_the_writer_takes_it_back() {
        let mut c = direct();
        assert!(c.frame_with(|_, buf| buf.extend_from_slice(b"z")));
        assert_eq!(c.flush(fail), Flushed::Failed);
        assert_eq!(c.mode(), Mode::Writer);
        assert!(!c.may_release());
        let back = c.take_back(ok);
        assert!(back.write_failed);
        assert_eq!(back.ctx, Some(7));
        // Taken back: the latch moved to the writer.
        assert!(c.may_release());
    }

    #[test]
    fn a_burst_capacity_is_kept_then_given_back_after_small_writes() {
        let mut c = direct();
        assert!(c.frame_with(|_, buf| buf.resize(1 << 20, b'x')));
        assert_eq!(c.flush(ok), Flushed::Wrote(1 << 20));
        assert!(c.buf.capacity() >= 1 << 20, "a burst's capacity is kept");
        for _ in 0..BUF_SHRINK_AFTER - 1 {
            assert!(c.frame_with(|_, buf| buf.push(b'y')));
            assert_eq!(c.flush(ok), Flushed::Wrote(1));
        }
        assert!(
            c.buf.capacity() >= 1 << 20,
            "not before the streak completes"
        );
        assert!(c.frame_with(|_, buf| buf.push(b'y')));
        assert_eq!(c.flush(ok), Flushed::Wrote(1));
        assert!(
            c.buf.capacity() <= BUF_RETAIN_FLOOR,
            "given back after the streak"
        );
    }

    #[test]
    fn close_writes_what_is_buffered_and_never_reopens() {
        let mut c = direct();
        assert!(c.frame_with(|_, buf| buf.extend_from_slice(b"q")));
        let mut out = Vec::new();
        let back = c.close(|_, b| {
            out.extend_from_slice(b);
            Ok(())
        });
        assert_eq!(out, b"q");
        assert_eq!(back.ctx, Some(7));
        assert_eq!(c.mode(), Mode::Closed);
        assert!(c.release(1, Vec::new(), true).is_err());
        assert!(!c.frame_with(|_, _| panic!("framed after close")));
    }
}
