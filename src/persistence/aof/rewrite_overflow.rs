//! Rewrite-window append overflow (issue #452.1): the aof_rewrite_buf
//! equivalent that closes the fold-time append-channel saturation drop.
//! See [`RewriteOverflow`] for the ordering invariant.

use super::rewrite::{
    drain_pending_appends_bounded, drain_pending_appends_framed, sync_and_fulfill_drain,
};
use super::*;

/// Total appends spilled into a [`RewriteOverflow`] buffer instead of being
/// dropped at the full channel (issue #452.1). Exposed in `INFO persistence`
/// as `aof_rewrite_overflow_spilled` — a non-zero value means rewrites are
/// racing sustained write load and the overflow saved acked writes.
pub static AOF_REWRITE_OVERFLOW_SPILLED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Default cap on bytes buffered in one shard's [`RewriteOverflow`] during a
/// single rewrite (256 MiB). Beyond the cap the producer falls back to the
/// pre-existing bounded-blocking/drop path — fail-loud, never unbounded RAM.
pub(crate) const AOF_REWRITE_OVERFLOW_DEFAULT_MAX_BYTES: usize = 256 << 20;

/// Rewrite-window append overflow (issue #452.1).
///
/// While a rewrite fold runs, the AOF writer thread is NOT in its recv loop,
/// so the bounded append channel can saturate under sustained write load and
/// `try_send_append` / `send_append_bounded_blocking` used to DROP acked
/// records — lost even on a clean restart. This buffer is the aof_rewrite_buf
/// equivalent: while armed (fold in progress), producers spill overflow
/// appends here instead of dropping them, and the writer drains the buffer
/// into the committed incr file immediately after the fold, before resuming
/// its recv loop.
///
/// # Ordering invariant (load-bearing)
///
/// Replay order must match enqueue order. Three rules enforce it:
///
/// 1. **Once spilling, keep spilling**: a producer checks
///    [`spill_first`](Self::spill_first) BEFORE `try_send` — while the buffer
///    is non-empty every new append goes to the buffer even if the channel
///    has room (phase-1/3 drains free slots mid-fold). Otherwise an older
///    spilled append would be replayed after a newer channel append.
/// 2. **Channel before buffer at drain time**: every buffered append was
///    spilled while the channel was full, so all channel-resident appends are
///    older than all buffered ones. [`finish_framed`](Self::finish_framed) /
///    [`finish_raw`](Self::finish_raw) therefore drain the channel (bounded
///    by its current length) BEFORE writing the buffer.
/// 3. **Drain holds the buffer lock end-to-end**: producers deciding
///    spill-vs-send block on the same mutex, so no append can slip into the
///    channel between "buffer written" and "disarmed" and be reordered
///    against still-buffered items.
///
/// Producers are ordered by the db write guard (appends are enqueued inside
/// the guard — see the fold's exactly-once doc), so the buffer's push order
/// matches mutation order across threads too.
///
/// # Snapshot cut (exactly-once)
///
/// The fold's C4 contract splits appends at the snapshot instant:
/// PRE-snapshot appends have their effects captured by the new base RDB
/// (the shard mutates BEFORE enqueuing/spilling), so they are drained into
/// the OLD incr (deleted at `manifest.advance`) and must never reach the
/// NEW incr — replaying them on top of the base double-applies
/// non-idempotent commands (INCR/APPEND/LPUSH…). The channel side is cut by
/// `pending_aof_count`; the buffer side is cut by [`mark_cut`](Self::mark_cut),
/// called at the same atomic instant (the AofFold handler / under the
/// all-db write guards). On a COMMITTED fold, [`finish_framed`] /
/// [`finish_raw`] discard entries `[0..cut)` (acking their parked
/// `AppendSync`s — the base makes them durable) and write only `[cut..)`
/// into the new incr. On an ABORTED fold everything is written to the
/// rolled-back OLD incr (the old base predates all of it).
pub struct RewriteOverflow {
    /// True from fold start until the post-fold drain completes. Read with
    /// `Acquire` on the producer's cold (channel-Full) path only.
    armed: std::sync::atomic::AtomicBool,
    /// Spilled messages in enqueue order. Only `Append` / `AppendSync` are
    /// ever pushed.
    buf: parking_lot::Mutex<Vec<AofMessage>>,
    /// Payload bytes currently buffered; enforces `max_bytes`.
    bytes: std::sync::atomic::AtomicUsize,
    max_bytes: usize,
    /// Buffer length at the fold's snapshot instant — entries below this
    /// index are pre-snapshot (see "Snapshot cut" above). Written by
    /// [`mark_cut`](Self::mark_cut) under the buffer lock; 0 when no cut
    /// was recorded (fold aborted before its snapshot, or an arm without
    /// an exact snapshot instant — both treat the whole buffer as
    /// post-snapshot/write-everything, which can never lose data).
    cut: std::sync::atomic::AtomicUsize,
}

/// Why [`RewriteOverflow::try_spill`] refused a message — the two reasons
/// require OPPOSITE fallbacks, so they must be distinguishable:
///
/// - [`Disarmed`](SpillReject::Disarmed): no fold in progress (or the drain
///   just completed). The channel is live again; falling through to the
///   normal send path is correct.
/// - [`CapExceeded`](SpillReject::CapExceeded): a fold IS in progress and
///   older records remain buffered. The message is consumed (dropped) — the
///   caller records the loss (a hole, but monotone order); sending it into
///   the channel instead would place a NEWER record ahead of the OLDER
///   buffered ones at the post-fold drain (channel is written before
///   buffer), inverting same-key replay order.
pub(crate) enum SpillReject {
    Disarmed(AofMessage),
    CapExceeded,
}

impl RewriteOverflow {
    pub fn new() -> Self {
        Self::with_cap(AOF_REWRITE_OVERFLOW_DEFAULT_MAX_BYTES)
    }

    /// Explicit-cap constructor (tests exercise the cap-exceeded fallback).
    pub fn with_cap(max_bytes: usize) -> Self {
        Self {
            armed: std::sync::atomic::AtomicBool::new(false),
            buf: parking_lot::Mutex::new(Vec::new()),
            bytes: std::sync::atomic::AtomicUsize::new(0),
            max_bytes,
            cut: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Arm at fold start. Called by the writer task immediately before
    /// entering a `do_rewrite_*` fold. Resets the snapshot cut — a fold
    /// that never reaches its snapshot leaves it 0 (write-everything).
    pub(crate) fn arm(&self) {
        self.cut.store(0, std::sync::atomic::Ordering::Release);
        self.armed.store(true, std::sync::atomic::Ordering::Release);
    }

    /// Record the snapshot cut: everything spilled so far is pre-snapshot.
    /// MUST be called at the fold's snapshot instant, under whatever
    /// exclusion makes that instant exact (the shard's AofFold handler —
    /// same instant as the `pending_aof_count` capture — or the writer
    /// while holding every db write guard). Takes the buffer lock so the
    /// recorded length can't race a concurrent spill.
    pub(crate) fn mark_cut(&self) {
        let buf = self.buf.lock();
        self.cut
            .store(buf.len(), std::sync::atomic::Ordering::Release);
    }

    /// Producer fast-path gate: true when the producer must spill directly
    /// (skip `try_send`) to preserve order — see ordering rule 1.
    #[inline]
    pub(crate) fn spill_first(&self) -> bool {
        self.armed.load(std::sync::atomic::Ordering::Acquire)
            && self.bytes.load(std::sync::atomic::Ordering::Relaxed) > 0
    }

    /// Try to buffer an overflow append. Refusals carry the message back
    /// with the reason — see [`SpillReject`] for why the caller's fallback
    /// MUST differ between the two.
    pub(crate) fn try_spill(&self, msg: AofMessage) -> Result<(), SpillReject> {
        let len = match &msg {
            AofMessage::Append { bytes, .. } => bytes.len(),
            AofMessage::AppendSync { bytes, .. } => bytes.len(),
            _ => return Err(SpillReject::Disarmed(msg)),
        };
        let mut buf = self.buf.lock();
        // Re-check under the lock: a concurrent finish_* may have disarmed
        // between the caller's armed read and our lock acquisition.
        if !self.armed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(SpillReject::Disarmed(msg));
        }
        let cur = self.bytes.load(std::sync::atomic::Ordering::Relaxed);
        if cur.saturating_add(len) > self.max_bytes {
            // Distinguish the cap only while older records are buffered —
            // an empty buffer at the cap (only possible with a pathological
            // tiny test cap) has no ordering hazard.
            return if buf.is_empty() {
                Err(SpillReject::Disarmed(msg))
            } else {
                Err(SpillReject::CapExceeded)
            };
        }
        buf.push(msg);
        self.bytes
            .store(cur + len, std::sync::atomic::Ordering::Relaxed);
        AOF_REWRITE_OVERFLOW_SPILLED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Post-fold drain + disarm, PerShard framed format
    /// (`[u64 lsn][u32 len][RESP]`). Writes (a) the channel's current
    /// contents, then (b) the buffered overflow, fsyncs the boundary,
    /// resolves parked `AppendSync` acks, and disarms. Always disarms and
    /// clears — even on write error (the loss is then counted and logged,
    /// never silent, and producers stop spilling into a dead buffer).
    /// `committed`: whether the fold REPLACED the base (manifest advanced /
    /// file renamed). On `true`, pre-cut entries are discarded — their
    /// effects are in the new base and writing them would double-apply. On
    /// `false` (abort — `file` is the rolled-back OLD incr) everything is
    /// written: the old base predates every buffered entry.
    #[cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
    pub(crate) fn finish_framed(
        &self,
        rx: &channel::MpscReceiver<AofMessage>,
        file: &mut std::fs::File,
        db_ctx: &mut usize,
        committed: bool,
    ) -> Result<(), MoonError> {
        self.finish_inner(rx, file, db_ctx, true, committed)
    }

    /// Post-fold drain + disarm, TopLevel RAW RESP format (no framing).
    /// See [`finish_framed`](Self::finish_framed) for `committed`.
    #[cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
    pub(crate) fn finish_raw(
        &self,
        rx: &channel::MpscReceiver<AofMessage>,
        file: &mut std::fs::File,
        db_ctx: &mut usize,
        committed: bool,
    ) -> Result<(), MoonError> {
        self.finish_inner(rx, file, db_ctx, false, committed)
    }

    #[cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
    fn finish_inner(
        &self,
        rx: &channel::MpscReceiver<AofMessage>,
        file: &mut std::fs::File,
        db_ctx: &mut usize,
        framed: bool,
        committed: bool,
    ) -> Result<(), MoonError> {
        use std::io::Write;
        // Two-phase drain (deep-review P2): the buffer lock is held only for
        // each SWAP, never across the disk writes — a shard thread deciding
        // spill-vs-send must not park behind a multi-second file drain.
        // Ordering stays intact without the long hold:
        // - `bytes` is NOT reset per swap, only at the final disarm, so
        //   `spill_first()` stays true for producers throughout the drain
        //   (no producer can slip a newer record into the channel while
        //   swapped-but-unwritten entries exist — rule 1 keeps holding).
        // - The cap therefore bounds the TOTAL spilled during one armed
        //   window, which also bounds the number of swap rounds.
        // - Disarm happens under the buffer lock in the empty-swap round
        //   (rule 3's "no append slips between written and disarmed").
        //
        // Snapshot cut: on a committed fold, entries below the cut are
        // pre-snapshot — their effects are in the new base RDB. Skip their
        // bytes; park their AppendSync acks with the others (the advance's
        // base fsync made them durable before finish ran). Only the FIRST
        // swap can contain pre-cut entries (nothing drains the buffer
        // between `mark_cut` and this call).
        let cut = if committed {
            self.cut.load(std::sync::atomic::Ordering::Acquire)
        } else {
            0
        };
        let io_err = |e: std::io::Error| {
            MoonError::from(AofError::Io {
                path: PathBuf::from("<aof rewrite overflow drain>"),
                source: e,
            })
        };
        let write_record =
            |file: &mut std::fs::File, lsn: u64, data: &[u8]| -> std::io::Result<()> {
                if framed {
                    let mut header = [0u8; 12];
                    header[..8].copy_from_slice(&lsn.to_le_bytes());
                    header[8..].copy_from_slice(&(data.len() as u32).to_le_bytes());
                    file.write_all(&header)?;
                }
                file.write_all(data)
            };
        let mut drained_overflow = 0usize;
        let mut folded_into_base = 0usize;
        let mut first_swap = true;
        let result = (|| -> Result<(), MoonError> {
            // (a) channel first — everything in it is older than the buffer
            // (ordering rule 2: producers spill while armed, so nothing new
            // enters the channel during this drain). Bounded by the current
            // length so we never consume appends enqueued after this point.
            let chan_bound = rx.len();
            let mut outcome = if framed {
                drain_pending_appends_framed(rx, file, chan_bound, db_ctx)?
            } else {
                drain_pending_appends_bounded(rx, file, chan_bound, db_ctx)?
            };
            // (b) then the buffered overflow: swap-write rounds until a swap
            // finds the buffer empty (that round disarms under the lock).
            loop {
                let spilled = {
                    let mut buf = self.buf.lock();
                    if buf.is_empty() {
                        self.bytes.store(0, std::sync::atomic::Ordering::Relaxed);
                        self.armed
                            .store(false, std::sync::atomic::Ordering::Release);
                        break;
                    }
                    std::mem::take(&mut *buf)
                };
                let batch_cut = if first_swap {
                    cut.min(spilled.len())
                } else {
                    0
                };
                first_swap = false;
                for (idx, msg) in spilled.into_iter().enumerate() {
                    // Pre-cut entries: effect already in the committed base —
                    // writing the record would double-apply on replay. Ack
                    // any parked AppendSync (durable via the base) and skip.
                    if idx < batch_cut {
                        folded_into_base += 1;
                        if let AofMessage::AppendSync { ack, .. } = msg {
                            outcome.pending_acks.push(ack);
                        }
                        continue;
                    }
                    match msg {
                        AofMessage::Append { lsn, db, bytes } => {
                            if let Some(sel) = select_prefix_if_needed(db, bytes.is_empty(), db_ctx)
                            {
                                write_record(file, 0, &sel).map_err(io_err)?;
                            }
                            write_record(file, lsn, &bytes).map_err(io_err)?;
                            drained_overflow += 1;
                        }
                        AofMessage::AppendSync {
                            lsn,
                            db,
                            bytes,
                            ack,
                        } => {
                            // Zero-length AppendSync = fsync barrier: no
                            // on-disk record, ack parks for the boundary
                            // fsync below (H1-BARRIER, mirrors
                            // drain_pending_appends_framed).
                            if !bytes.is_empty() {
                                if let Some(sel) = select_prefix_if_needed(db, false, db_ctx) {
                                    write_record(file, 0, &sel).map_err(io_err)?;
                                }
                                write_record(file, lsn, &bytes).map_err(io_err)?;
                            }
                            drained_overflow += 1;
                            outcome.pending_acks.push(ack);
                        }
                        // try_spill only ever admits Append/AppendSync.
                        _ => {}
                    }
                }
            }
            if outcome.drained > 0 || drained_overflow > 0 || folded_into_base > 0 {
                sync_and_fulfill_drain(
                    &mut outcome,
                    file,
                    PathBuf::from("<aof rewrite overflow drain>"),
                )?;
                info!(
                    "rewrite overflow drained: {} channel + {} spilled appends flushed post-fold \
                     ({} pre-snapshot spills folded into the new base, not re-written)",
                    outcome.drained, drained_overflow, folded_into_base
                );
            }
            if outcome.shutdown_requested {
                // Parity with the fold's own drains: the Shutdown message was
                // consumed by this drain — the writer exits via channel
                // disconnect instead, but the request must not vanish
                // silently (deep-review P2).
                warn!(
                    "AOF writer: shutdown message consumed by rewrite-overflow drain \
                     (writer will exit on channel disconnect)"
                );
            }
            Ok(())
        })();
        if result.is_err() {
            // Failed drain: disarm and clear so producers stop spilling into
            // a buffer nobody will drain; count everything still buffered as
            // lost (fail-loud, never silent).
            let mut buf = self.buf.lock();
            let lost = buf.len() as u64;
            if lost > 0 {
                super::record_append_dropped(lost);
            }
            buf.clear();
            self.bytes.store(0, std::sync::atomic::Ordering::Relaxed);
            self.armed
                .store(false, std::sync::atomic::Ordering::Release);
        }
        if let Err(ref e) = result {
            error!(
                "rewrite overflow drain FAILED — spilled appends may be lost: {}",
                e
            );
        }
        result
    }
}

impl RewriteOverflow {
    /// Fatal-path disarm (writer thread exiting without a usable file, or
    /// an [`ArmGuard`] unwinding past a panicked fold): counts every
    /// still-buffered append as dropped (fail-loud, never silent) and stops
    /// producers from spilling into a dead buffer.
    pub(crate) fn disarm_dropping(&self) {
        let mut buf = self.buf.lock();
        let lost = buf.len() as u64;
        if lost > 0 {
            super::record_append_dropped(lost);
            error!(
                "rewrite overflow discarded {} spilled appends — writer exiting without a usable file",
                lost
            );
        }
        buf.clear();
        self.bytes.store(0, std::sync::atomic::Ordering::Relaxed);
        self.armed
            .store(false, std::sync::atomic::Ordering::Release);
    }
}

impl RewriteOverflow {
    /// [`arm`](Self::arm) with unwind safety (deep-review U1): the returned
    /// guard disarms-with-accounting if the overflow is STILL armed when it
    /// drops — i.e. the fold panicked (release profile unwinds; the writer
    /// thread dies but the pool-shared overflow lives on) or an arm forgot
    /// its `finish_*`. Without this, producers would silently spill up to
    /// the cap into a buffer nobody will ever drain. After a normal
    /// `finish_*` (which disarms) the guard is a no-op.
    pub(crate) fn arm_scoped(&self) -> ArmGuard<'_> {
        self.arm();
        ArmGuard(self)
    }
}

/// See [`RewriteOverflow::arm_scoped`].
pub(crate) struct ArmGuard<'a>(&'a RewriteOverflow);

impl Drop for ArmGuard<'_> {
    fn drop(&mut self) {
        if self.0.armed.load(std::sync::atomic::Ordering::Acquire) {
            error!(
                "rewrite overflow still armed at guard drop — fold panicked or \
                 finish was skipped; disarming with drop accounting"
            );
            self.0.disarm_dropping();
        }
    }
}

impl Default for RewriteOverflow {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::aof::{AofMessage, AofWriterPool};
    use bytes::Bytes;
    use std::io::Read;

    fn append(lsn: u64, payload: &'static [u8]) -> AofMessage {
        AofMessage::Append {
            lsn,
            db: 0,
            bytes: Bytes::from_static(payload),
        }
    }

    /// Parse the PerShard framed format back: `[u64 lsn LE][u32 len LE][bytes]`.
    fn read_framed(path: &std::path::Path) -> Vec<(u64, Vec<u8>)> {
        let mut data = Vec::new();
        std::fs::File::open(path)
            .unwrap()
            .read_to_end(&mut data)
            .unwrap();
        let mut out = Vec::new();
        let mut off = 0;
        while off + 12 <= data.len() {
            let lsn = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
            let len = u32::from_le_bytes(data[off + 8..off + 12].try_into().unwrap()) as usize;
            off += 12;
            out.push((lsn, data[off..off + len].to_vec()));
            off += len;
        }
        assert_eq!(off, data.len(), "trailing garbage in framed file");
        out
    }

    #[test]
    fn arm_guard_disarms_with_accounting_on_unwind() {
        use crate::persistence::aof::AOF_BACKPRESSURE_DROPPED;
        let ovf = RewriteOverflow::new();
        let before = AOF_BACKPRESSURE_DROPPED.load(std::sync::atomic::Ordering::Relaxed);
        {
            let _guard = ovf.arm_scoped();
            assert!(ovf.try_spill(append(1, b"a")).is_ok());
            assert!(ovf.try_spill(append(2, b"b")).is_ok());
            // No finish_* — simulates a fold panic unwinding past the arm.
        }
        assert!(
            !ovf.spill_first(),
            "guard drop must disarm so producers stop spilling into a dead buffer"
        );
        let after = AOF_BACKPRESSURE_DROPPED.load(std::sync::atomic::Ordering::Relaxed);
        // >= not ==: the counter is process-global and other parallel tests
        // may bump it in the window (same convention as the pool.rs tests).
        assert!(
            after - before >= 2,
            "both stranded spills must be counted as dropped, never silent"
        );
        // A later fold must start from a clean buffer.
        let _guard = ovf.arm_scoped();
        assert!(ovf.try_spill(append(3, b"c")).is_ok());
    }

    #[test]
    fn arm_guard_is_noop_after_normal_finish() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("incr.aof");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        let (_tx, rx) = channel::mpsc_bounded::<AofMessage>(1);
        let ovf = RewriteOverflow::new();
        {
            let _guard = ovf.arm_scoped();
            assert!(ovf.try_spill(append(1, b"a")).is_ok());
            let mut db_ctx = 0usize;
            ovf.finish_framed(&rx, &mut file, &mut db_ctx, false)
                .unwrap();
            // finish disarmed the overflow; the guard drop below must be a
            // no-op (armed=false), not a second disarm-with-accounting.
        }
        assert!(!ovf.spill_first());
        assert_eq!(
            read_framed(&path).len(),
            1,
            "spilled entry reached the file"
        );
    }

    #[test]
    fn spill_rejected_when_disarmed() {
        let ovf = RewriteOverflow::new();
        assert!(
            ovf.try_spill(append(1, b"x")).is_err(),
            "no fold in progress → producer must fall through to its normal path"
        );
        assert!(!ovf.spill_first());
    }

    #[test]
    fn cap_rejects_beyond_max_bytes() {
        let ovf = RewriteOverflow::with_cap(10);
        ovf.arm();
        assert!(ovf.try_spill(append(1, b"12345678")).is_ok());
        assert!(
            ovf.try_spill(append(2, b"12345678")).is_err(),
            "second 8-byte payload exceeds the 10-byte cap → fail-loud fallback"
        );
        // A payload that still fits must be accepted.
        assert!(ovf.try_spill(append(3, b"12")).is_ok());
    }

    /// The core #452.1 ordering contract: at drain time the channel's
    /// contents (older) are written before the spilled buffer (newer), and
    /// the buffer preserves push order.
    #[test]
    fn finish_framed_writes_channel_before_buffer_in_order() {
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(2);
        tx.try_send(append(1, b"chan-a")).unwrap();
        tx.try_send(append(2, b"chan-b")).unwrap();

        let ovf = RewriteOverflow::new();
        ovf.arm();
        assert!(ovf.try_spill(append(3, b"spill-c")).is_ok());
        assert!(ovf.try_spill(append(4, b"spill-d")).is_ok());

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("incr.aof");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        let mut db_ctx = 0usize;
        ovf.finish_framed(&rx, &mut file, &mut db_ctx, true)
            .unwrap();

        let records = read_framed(&path);
        assert_eq!(
            records,
            vec![
                (1, b"chan-a".to_vec()),
                (2, b"chan-b".to_vec()),
                (3, b"spill-c".to_vec()),
                (4, b"spill-d".to_vec()),
            ],
            "replay order must be channel (older) then buffer (newer), each FIFO"
        );
        assert!(!ovf.spill_first(), "finish must disarm and clear");
        assert!(
            ovf.try_spill(append(5, b"post")).is_err(),
            "disarmed overflow must reject further spills"
        );
    }

    /// Red/green for the #452.1 drop itself: with the channel full and the
    /// bounded-blocking budget exhausted, the pre-fix path DROPPED the acked
    /// record (returned false). With a rewrite in progress (overflow armed)
    /// the record must be spilled and reported enqueued instead.
    #[test]
    fn pool_bounded_blocking_spills_instead_of_dropping_during_rewrite() {
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(1);
        let pool = AofWriterPool::top_level(tx);
        assert!(pool.try_send_append(0, 1, 0, Bytes::from_static(b"fill")));

        // Baseline (no rewrite): budget-exhausted send on a full channel drops.
        let mut budget = std::time::Duration::ZERO;
        assert!(
            !pool.send_append_bounded_blocking(0, 2, 0, Bytes::from_static(b"lost"), &mut budget),
            "sanity: without a rewrite in progress the old drop path is unchanged"
        );

        // Rewrite in progress: same situation must spill, not drop.
        pool.overflow_for(0).arm();
        let mut budget = std::time::Duration::ZERO;
        assert!(
            pool.send_append_bounded_blocking(0, 3, 0, Bytes::from_static(b"saved"), &mut budget),
            "#452.1: acked append during a fold must be spilled, never dropped"
        );

        // Ordering rule 1: even with channel room, later appends keep
        // spilling while the buffer is non-empty.
        assert!(matches!(
            rx.try_recv(),
            Ok(AofMessage::Append { lsn: 1, .. })
        ));
        assert!(pool.try_send_append(0, 4, 0, Bytes::from_static(b"also-spilled")));
        assert!(
            rx.is_empty(),
            "append with room in the channel must STILL spill while the buffer is non-empty"
        );

        // Drain: file receives the spilled records in order; afterwards the
        // pool goes back to the plain channel path.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("incr.aof");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        let mut db_ctx = 0usize;
        pool.overflow_for(0)
            .finish_framed(&rx, &mut file, &mut db_ctx, true)
            .unwrap();
        let lsns: Vec<u64> = read_framed(&path).into_iter().map(|(l, _)| l).collect();
        assert_eq!(lsns, vec![3, 4]);

        assert!(pool.try_send_append(0, 5, 0, Bytes::from_static(b"back-to-channel")));
        assert!(matches!(
            rx.try_recv(),
            Ok(AofMessage::Append { lsn: 5, .. })
        ));
    }

    /// P0 fix (adversarial review): entries spilled BEFORE the fold's
    /// snapshot instant have their effects captured by the new base RDB — a
    /// COMMITTED fold must not re-write them into the new incr, or
    /// non-idempotent commands (INCR/APPEND/LPUSH…) double-apply on replay.
    /// Their parked AppendSync acks still resolve Synced (durable via the
    /// base).
    #[test]
    fn committed_finish_discards_pre_cut_entries() {
        let (_tx, rx) = channel::mpsc_bounded::<AofMessage>(1);
        let ovf = RewriteOverflow::new();
        ovf.arm();
        assert!(ovf.try_spill(append(1, b"pre-a")).is_ok());
        let (ack_tx, ack_rx) = crate::runtime::channel::oneshot::<AofAck>();
        assert!(
            ovf.try_spill(AofMessage::AppendSync {
                lsn: 2,
                db: 0,
                bytes: Bytes::from_static(b"pre-b"),
                ack: ack_tx,
            })
            .is_ok()
        );
        // Snapshot instant: everything above is pre-snapshot.
        ovf.mark_cut();
        assert!(ovf.try_spill(append(3, b"post-c")).is_ok());

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("incr.aof");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        let mut db_ctx = 0usize;
        ovf.finish_framed(&rx, &mut file, &mut db_ctx, true)
            .unwrap();

        assert_eq!(
            read_framed(&path),
            vec![(3, b"post-c".to_vec())],
            "pre-cut entries are in the committed base and must NOT be re-written"
        );
        assert_eq!(
            ack_rx.try_recv(),
            Ok(AofAck::Synced),
            "a discarded pre-cut AppendSync is durable via the base and must still ack"
        );
    }

    /// Abort mirror of the cut: the rolled-back OLD base predates every
    /// spilled entry, so ALL of them must be written to the old incr.
    #[test]
    fn aborted_finish_writes_all_entries_ignoring_cut() {
        let (_tx, rx) = channel::mpsc_bounded::<AofMessage>(1);
        let ovf = RewriteOverflow::new();
        ovf.arm();
        assert!(ovf.try_spill(append(1, b"pre-a")).is_ok());
        ovf.mark_cut();
        assert!(ovf.try_spill(append(2, b"post-b")).is_ok());

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("incr.aof");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        let mut db_ctx = 0usize;
        ovf.finish_framed(&rx, &mut file, &mut db_ctx, false)
            .unwrap();

        assert_eq!(
            read_framed(&path),
            vec![(1, b"pre-a".to_vec()), (2, b"post-b".to_vec())],
            "an aborted fold keeps the old base — nothing folded, write everything"
        );
    }

    /// `arm()` must reset the previous fold's cut — a stale cut would
    /// silently discard post-snapshot entries of the NEXT fold (data loss).
    #[test]
    fn rearm_resets_stale_cut() {
        let (_tx, rx) = channel::mpsc_bounded::<AofMessage>(1);
        let ovf = RewriteOverflow::new();
        ovf.arm();
        assert!(ovf.try_spill(append(1, b"fold1")).is_ok());
        ovf.mark_cut();
        let tmp = tempfile::tempdir().unwrap();
        let path1 = tmp.path().join("incr1.aof");
        let mut f1 = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path1)
            .unwrap();
        let mut db_ctx = 0usize;
        ovf.finish_framed(&rx, &mut f1, &mut db_ctx, true).unwrap();
        assert!(read_framed(&path1).is_empty());

        // Second fold: no mark_cut this time — nothing may be discarded.
        ovf.arm();
        assert!(ovf.try_spill(append(2, b"fold2")).is_ok());
        let path2 = tmp.path().join("incr2.aof");
        let mut f2 = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path2)
            .unwrap();
        ovf.finish_framed(&rx, &mut f2, &mut db_ctx, true).unwrap();
        assert_eq!(
            read_framed(&path2),
            vec![(2, b"fold2".to_vec())],
            "a stale cut from the previous fold must not discard this fold's entries"
        );
    }

    /// AppendSync entries spilled during a fold must park their acks until
    /// the post-drain boundary fsync (issue #140 discipline) and then
    /// resolve `Synced`.
    #[test]
    fn finish_framed_resolves_spilled_appendsync_acks_after_fsync() {
        let (_tx, rx) = channel::mpsc_bounded::<AofMessage>(1);
        let ovf = RewriteOverflow::new();
        ovf.arm();
        let (ack_tx, ack_rx) = crate::runtime::channel::oneshot::<AofAck>();
        assert!(
            ovf.try_spill(AofMessage::AppendSync {
                lsn: 7,
                db: 0,
                bytes: Bytes::from_static(b"durable"),
                ack: ack_tx,
            })
            .is_ok()
        );

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("incr.aof");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        let mut db_ctx = 0usize;
        ovf.finish_framed(&rx, &mut file, &mut db_ctx, true)
            .unwrap();

        assert_eq!(ack_rx.try_recv(), Ok(AofAck::Synced));
        assert_eq!(read_framed(&path), vec![(7, b"durable".to_vec())]);
    }
}
