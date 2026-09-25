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
/// # Snapshot epoch (exactly-once)
///
/// A cooperative fold splits appends at its snapshot instant.
/// PRE-snapshot appends have their effects captured by the new base RDB (the
/// shard mutates BEFORE enqueuing/spilling), so they must never reach the NEW
/// incr: replaying them on top of the base double-applies non-idempotent
/// commands (INCR/APPEND/LPUSH…).
///
/// The split is by [`FoldEpoch`], not by position. This overflow also owns
/// its writer's epoch counter: producers stamp every record with
/// [`stamp`](Self::stamp) in the same synchronous section as the mutation,
/// and the fold calls [`advance_epoch`](Self::advance_epoch) at the snapshot
/// instant (the AofFold handler, or under the all-db write guards). A position
/// cut (channel length, buffer length at the snapshot) misses a record whose
/// producer was parked or awaiting between mutation and enqueue — the record
/// lands past the cut while its effect is in the base (#455). The epoch
/// classifies it correctly wherever it lands.
///
/// When the fold's generation takes effect, [`finish_framed`] /
/// [`finish_raw`] are given the snapshot epoch as their `floor`. They drop
/// every buffered or channel entry stamped below it, acking parked
/// `AppendSync`s (the base makes them durable), and write the rest. The writer
/// keeps applying the same floor to everything it dequeues later. When the
/// fold aborts, the floor stays where it was: the old base predates all of it,
/// so everything is written into the old incr.
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
    /// This writer's fold epoch (see "Snapshot epoch" above). Advanced only
    /// at a fold's snapshot instant; read by every producer when it stamps
    /// a record.
    epoch: std::sync::atomic::AtomicU64,
    /// The snapshot epoch of the latest fold whose generation this writer
    /// adopted as COMMITTED (`FoldOutcome::adopt`); `INITIAL` until one
    /// commits. Only ever raised. A shard reads it to learn that a fold cut
    /// after some instant of its own (stamped with [`Self::stamp`]) is now the
    /// durable generation — the cold tier's reclaim waits for exactly that
    /// before it lists a compacted spill file (moon#1215, PR #1233 review).
    committed: std::sync::atomic::AtomicU64,
    /// Whether this writer's log is missing an acked append, and since when:
    /// `0` for no hole, otherwise one more than the highest fold epoch
    /// current at any drop not yet folded back in (see "Dropped appends"
    /// on [`Self::note_append_dropped`]). A mutex, not an atomic, so a drop
    /// and a heal on the same writer cannot interleave between reading this
    /// and moving [`AOF_WRITERS_MISSING_APPENDS`]. Taken only on a drop or a
    /// committed fold.
    missing_since: parking_lot::Mutex<u64>,
    /// Test-only probe (moon#1158): the BGREWRITEAOF in-progress flag as the
    /// post-fold drain found it on entry. The drain must run while the
    /// rewrite still holds the flag — otherwise the next rewrite can be
    /// dispatched into the channel being drained.
    #[cfg(test)]
    in_progress_at_finish: parking_lot::Mutex<Option<bool>>,
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
    ///
    /// Every AOF writer owns one of these, created when its pool is built at
    /// boot — before recovery replays the log. So this is also where the
    /// process learns that an AOF rewrite fold exists to consume the cold
    /// tier's dead-slot ledger (moon#1215): without one, the ledger records
    /// nothing (`storage::tiered::dead_slots`).
    pub fn with_cap(max_bytes: usize) -> Self {
        crate::storage::tiered::dead_slots::enable_ledger();
        Self {
            armed: std::sync::atomic::AtomicBool::new(false),
            buf: parking_lot::Mutex::new(Vec::new()),
            bytes: std::sync::atomic::AtomicUsize::new(0),
            max_bytes,
            epoch: std::sync::atomic::AtomicU64::new(FoldEpoch::INITIAL.0),
            committed: std::sync::atomic::AtomicU64::new(FoldEpoch::INITIAL.0),
            missing_since: parking_lot::Mutex::new(0),
            #[cfg(test)]
            in_progress_at_finish: parking_lot::Mutex::new(None),
        }
    }

    /// Test-only: the in-progress flag as the last `finish_*` saw it on
    /// entry (`None` if no drain ran yet). See the field doc.
    #[cfg(test)]
    pub(crate) fn in_progress_at_finish_for_test(&self) -> Option<bool> {
        *self.in_progress_at_finish.lock()
    }

    /// Record that an acked append for this writer was dropped (moon#1094).
    ///
    /// # Dropped appends
    ///
    /// The drop is tagged with the fold epoch current when it happens. The
    /// dropped record's mutation happened before the drop, so before that
    /// epoch's end: any fold whose snapshot epoch is ABOVE it captured the
    /// mutation in its base. That is the same `folded_below` rule the writer
    /// applies to the records it drops after a commit, read at the drop
    /// instead of the stamp, which can only be later, so the rule errs
    /// towards reporting a hole. Only the latest drop matters: a fold that
    /// covers it covers every earlier one.
    pub(crate) fn note_append_dropped(&self) {
        let at = self.stamp().0.saturating_add(1);
        let mut missing = self.missing_since.lock();
        if *missing == 0 {
            AOF_WRITERS_MISSING_APPENDS.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        }
        *missing = (*missing).max(at);
    }

    /// Clear this writer's hole if the fold that just COMMITTED with snapshot
    /// epoch `floor` postdates its latest drop (see
    /// [`Self::note_append_dropped`]). Called by the writer when it adopts a
    /// committed floor, and never for an aborted fold: the old base covers
    /// nothing new. A drop at or after the snapshot keeps the hole, because
    /// the new generation lacks that record too.
    pub(crate) fn heal_on_commit(&self, floor: FoldEpoch) {
        let mut missing = self.missing_since.lock();
        if *missing != 0 && FoldEpoch(*missing - 1).folded_below(floor) {
            *missing = 0;
            AOF_WRITERS_MISSING_APPENDS.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        }
    }

    /// Record that the fold with snapshot epoch `floor` committed (see the
    /// `committed` field). Called by `FoldOutcome::adopt`, never for an
    /// aborted fold.
    pub(crate) fn note_committed(&self, floor: FoldEpoch) {
        self.committed
            .fetch_max(floor.0, std::sync::atomic::Ordering::AcqRel);
    }

    /// The snapshot epoch of the latest committed fold (`INITIAL` if none).
    /// A mutation stamped `e` (by [`Self::stamp`]) is captured by the
    /// committed generation iff `e.folded_below(committed_floor())`.
    #[inline]
    pub fn committed_floor(&self) -> FoldEpoch {
        FoldEpoch(self.committed.load(std::sync::atomic::Ordering::Acquire))
    }

    /// Whether this writer's log is missing an acked append no committed
    /// fold has covered yet.
    #[cfg(test)]
    pub(crate) fn is_missing_appends(&self) -> bool {
        *self.missing_since.lock() != 0
    }

    /// Arm at fold start. Called by the writer task immediately before
    /// entering a `do_rewrite_*` fold.
    pub(crate) fn arm(&self) {
        self.armed.store(true, std::sync::atomic::Ordering::Release);
    }

    /// The fold epoch a record logging a mutation made RIGHT NOW belongs to.
    /// Producers MUST read it in the same synchronous section as the
    /// mutation (see "Snapshot epoch" above): reading it after an `.await`
    /// that followed the mutation can stamp a pre-snapshot record as
    /// post-snapshot.
    #[inline]
    pub fn stamp(&self) -> FoldEpoch {
        FoldEpoch(self.epoch.load(std::sync::atomic::Ordering::Acquire))
    }

    /// Open a new fold epoch and return it: every record stamped before this
    /// call is pre-snapshot, every record stamped after it is post-snapshot.
    /// MUST be called at the fold's snapshot instant, under whatever exclusion
    /// makes that instant exact: the shard's AofFold handler (same instant as
    /// the `pending_aof_count` capture), or the writer while it holds every db
    /// write guard.
    pub(crate) fn advance_epoch(&self) -> FoldEpoch {
        FoldEpoch(
            self.epoch
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
                .saturating_add(1),
        )
    }

    /// Test-only: number of currently buffered spilled messages.
    #[cfg(test)]
    pub(crate) fn buffered_len_for_test(&self) -> usize {
        self.buf.lock().len()
    }

    /// `true` between `arm` and the fold's `finish_*`/disarm — i.e. while a
    /// producer that finds the writer channel full can `try_spill` here
    /// instead of blocking or dropping (moon#838: the inline SET fast path's
    /// `append_would_block` probe). Distinct from [`Self::spill_first`],
    /// which additionally requires the buffer to be non-empty.
    #[inline]
    pub(crate) fn is_armed(&self) -> bool {
        self.armed.load(std::sync::atomic::Ordering::Acquire)
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
    ///
    /// `floor`: the writer's floor AFTER the fold's outcome is known. That is
    /// the fold's snapshot epoch when its generation took effect (`file` is
    /// the new incr), and the unchanged previous floor when the fold aborted
    /// (`file` is still the old incr). Entries stamped below it are already in
    /// the base of `file`'s generation. They are dropped, because writing them
    /// would double-apply. Everything else is written, in order.
    #[cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
    pub(crate) fn finish_framed(
        &self,
        rx: &channel::MpscReceiver<AofMessage>,
        file: &mut std::fs::File,
        db_ctx: &mut usize,
        floor: FoldEpoch,
    ) -> Result<(), MoonError> {
        self.finish_inner(rx, file, db_ctx, true, floor)
    }

    /// Post-fold drain + disarm, TopLevel RAW RESP format (no framing).
    /// See [`finish_framed`](Self::finish_framed) for `floor`.
    #[cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
    pub(crate) fn finish_raw(
        &self,
        rx: &channel::MpscReceiver<AofMessage>,
        file: &mut std::fs::File,
        db_ctx: &mut usize,
        floor: FoldEpoch,
    ) -> Result<(), MoonError> {
        self.finish_inner(rx, file, db_ctx, false, floor)
    }

    #[cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
    fn finish_inner(
        &self,
        rx: &channel::MpscReceiver<AofMessage>,
        file: &mut std::fs::File,
        db_ctx: &mut usize,
        framed: bool,
        floor: FoldEpoch,
    ) -> Result<(), MoonError> {
        use std::io::Write;
        #[cfg(test)]
        {
            *self.in_progress_at_finish.lock() = Some(
                crate::command::persistence::AOF_REWRITE_IN_PROGRESS
                    .load(std::sync::atomic::Ordering::SeqCst),
            );
        }
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
        // Snapshot epoch: entries stamped below `floor` are pre-snapshot —
        // their effects are in the base of the generation `file` belongs to.
        // Skip their bytes; park their AppendSync acks with the others (the
        // base was fsynced before the floor was raised). The channel drains
        // below apply the same filter.
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
        let result = (|| -> Result<(), MoonError> {
            // (a) channel first — everything in it is older than the buffer
            // (ordering rule 2: producers spill while armed, so nothing new
            // enters the channel during this drain). Bounded by the current
            // length so we never consume appends enqueued after this point.
            let chan_bound = rx.len();
            let mut outcome = if framed {
                drain_pending_appends_framed(rx, file, chan_bound, db_ctx, floor)?
            } else {
                drain_pending_appends_bounded(rx, file, chan_bound, db_ctx, floor)?
            };
            // (b) then the buffered overflow: swap-write rounds until a swap
            // finds the buffer empty (that round disarms under the lock).
            loop {
                let mut spilled = {
                    let mut buf = self.buf.lock();
                    if buf.is_empty() {
                        self.bytes.store(0, std::sync::atomic::Ordering::Relaxed);
                        self.armed
                            .store(false, std::sync::atomic::Ordering::Release);
                        break;
                    }
                    std::mem::take(&mut *buf)
                };
                // Re-drain the channel BEFORE this batch (re-verify Q3): the
                // buffer-empty window (finish entry, or before the armed
                // window's first spill) lets a producer try_send into a slot
                // phase (a) freed — such an entry is OLDER than everything
                // buffered afterwards (rule 1 forces spilling from the first
                // buffered byte on), so writing this swap first would invert
                // same-key replay order. Every channel entry present at this
                // instant predates every entry in `spilled`; drain them first.
                let late = rx.len();
                if late > 0 {
                    let o = if framed {
                        drain_pending_appends_framed(rx, file, late, db_ctx, floor)?
                    } else {
                        drain_pending_appends_bounded(rx, file, late, db_ctx, floor)?
                    };
                    outcome.drained += o.drained;
                    outcome.shutdown_requested |= o.shutdown_requested;
                    outcome.pending_acks.extend(o.pending_acks);
                }
                // Drain-by-index so a mid-batch write error can count the
                // un-written remainder as dropped (re-verify: fail-loud) —
                // `?` inside a consuming for-loop silently discards it.
                let mut wrote_err: Option<MoonError> = None;
                let batch_total = spilled.len();
                let mut batch_consumed = 0usize;
                for (idx, msg) in spilled.drain(..).enumerate() {
                    // Pre-snapshot entries: effect already in the base of
                    // `file`'s generation — writing the record would
                    // double-apply on replay. Ack any parked AppendSync
                    // (durable via the base) and skip.
                    if is_folded(&msg, floor) {
                        folded_into_base += 1;
                        batch_consumed += 1;
                        super::AOF_REWRITE_LATE_RECORDS_FOLDED
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if let AofMessage::AppendSync { ack, .. } = msg {
                            outcome.pending_acks.push(ack);
                        }
                        continue;
                    }
                    match msg {
                        AofMessage::Append { lsn, db, bytes, .. } => {
                            let r = select_prefix_if_needed(db, bytes.is_empty(), db_ctx)
                                .map_or(Ok(()), |sel| write_record(file, 0, &sel))
                                .and_then(|()| write_record(file, lsn, &bytes));
                            match r {
                                Ok(()) => {
                                    drained_overflow += 1;
                                    batch_consumed += 1;
                                }
                                Err(e) => wrote_err = Some(io_err(e)),
                            }
                        }
                        AofMessage::AppendSync {
                            lsn,
                            db,
                            bytes,
                            ack,
                            ..
                        } => {
                            // Zero-length AppendSync = fsync barrier: no
                            // on-disk record, ack parks for the boundary
                            // fsync below (H1-BARRIER, mirrors
                            // drain_pending_appends_framed).
                            let r = if bytes.is_empty() {
                                Ok(())
                            } else {
                                select_prefix_if_needed(db, false, db_ctx)
                                    .map_or(Ok(()), |sel| write_record(file, 0, &sel))
                                    .and_then(|()| write_record(file, lsn, &bytes))
                            };
                            match r {
                                Ok(()) => {
                                    drained_overflow += 1;
                                    batch_consumed += 1;
                                    outcome.pending_acks.push(ack);
                                }
                                // Dropping the ack here fails safe: the
                                // parked receiver sees RecvError, never a
                                // false Synced.
                                Err(e) => wrote_err = Some(io_err(e)),
                            }
                        }
                        // try_spill only ever admits Append/AppendSync.
                        _ => {
                            batch_consumed += 1;
                        }
                    }
                    if wrote_err.is_some() {
                        let _ = idx;
                        break;
                    }
                }
                if let Some(e) = wrote_err {
                    // The un-written remainder of this TAKEN batch (the
                    // failed message + everything after it) was consumed by
                    // `drain(..)`'s drop and is not in `buf` — the outer
                    // error path can't see it. Count it here so no acked
                    // append is ever lost silently (re-verify: P2.5 gap).
                    // AppendSync remainders fail safe on their own: the
                    // dropped ack sender surfaces as RecvError, never a
                    // false Synced.
                    let lost = (batch_total - batch_consumed) as u64;
                    if lost > 0 {
                        super::record_append_dropped(self, lost);
                    }
                    return Err(e);
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
                super::record_append_dropped(self, lost);
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
            super::record_append_dropped(self, lost);
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
        append_at(lsn, payload, FoldEpoch::INITIAL)
    }

    fn append_at(lsn: u64, payload: &'static [u8], epoch: FoldEpoch) -> AofMessage {
        AofMessage::Append {
            lsn,
            db: 0,
            bytes: Bytes::from_static(payload),
            epoch,
        }
    }

    fn incr_file(dir: &std::path::Path, name: &str) -> (std::path::PathBuf, std::fs::File) {
        let path = dir.join(name);
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        (path, file)
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
            ovf.finish_framed(&rx, &mut file, &mut db_ctx, FoldEpoch::INITIAL)
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

    /// Re-verify P2.5 gap: a mid-batch write error during the finish drain
    /// loses the TAKEN batch's un-written remainder (cleared by
    /// `drain(..)`'s drop, invisible to the outer buf-count) — it must be
    /// counted as dropped, never silent. Uses a read-only file handle so
    /// every write fails.
    #[test]
    fn finish_write_error_counts_unwritten_batch_remainder() {
        use crate::persistence::aof::AOF_BACKPRESSURE_DROPPED;
        let (_tx, rx) = channel::mpsc_bounded::<AofMessage>(1);
        let ovf = RewriteOverflow::new();
        ovf.arm();
        assert!(ovf.try_spill(append(1, b"a")).is_ok());
        assert!(ovf.try_spill(append(2, b"b")).is_ok());
        assert!(ovf.try_spill(append(3, b"c")).is_ok());

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("readonly.aof");
        std::fs::write(&path, b"").unwrap();
        let mut file = std::fs::OpenOptions::new().read(true).open(&path).unwrap();
        let mut db_ctx = 0usize;
        let before = AOF_BACKPRESSURE_DROPPED.load(std::sync::atomic::Ordering::Relaxed);
        let res = ovf.finish_framed(&rx, &mut file, &mut db_ctx, FoldEpoch::INITIAL);
        assert!(res.is_err(), "writes to a read-only handle must fail");
        let after = AOF_BACKPRESSURE_DROPPED.load(std::sync::atomic::Ordering::Relaxed);
        // >= not ==: process-global counter, parallel tests may bump it.
        assert!(
            after - before >= 3,
            "all 3 un-written spills must be counted (before={before}, after={after})"
        );
        assert!(!ovf.spill_first(), "error path must disarm");
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
        ovf.finish_framed(&rx, &mut file, &mut db_ctx, FoldEpoch::INITIAL)
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
            .finish_framed(&rx, &mut file, &mut db_ctx, FoldEpoch::INITIAL)
            .unwrap();
        let lsns: Vec<u64> = read_framed(&path).into_iter().map(|(l, _)| l).collect();
        assert_eq!(lsns, vec![3, 4]);

        assert!(pool.try_send_append(0, 5, 0, Bytes::from_static(b"back-to-channel")));
        assert!(matches!(
            rx.try_recv(),
            Ok(AofMessage::Append { lsn: 5, .. })
        ));
    }

    /// Entries stamped BEFORE the fold's snapshot have their effects in the
    /// new base: once that generation takes effect they must not be written
    /// into its incr, or non-idempotent commands (INCR/APPEND/LPUSH…)
    /// double-apply on replay. Their parked AppendSync acks still resolve
    /// Synced (durable via the base).
    #[test]
    fn finish_drops_entries_stamped_below_the_floor() {
        let (_tx, rx) = channel::mpsc_bounded::<AofMessage>(1);
        let ovf = RewriteOverflow::new();
        ovf.arm();
        assert!(ovf.try_spill(append_at(1, b"pre-a", ovf.stamp())).is_ok());
        let (ack_tx, ack_rx) = crate::runtime::channel::oneshot::<AofAck>();
        assert!(
            ovf.try_spill(AofMessage::AppendSync {
                lsn: 2,
                db: 0,
                bytes: Bytes::from_static(b"pre-b"),
                ack: ack_tx,
                epoch: ovf.stamp(),
            })
            .is_ok()
        );
        // Snapshot instant: everything stamped above is pre-snapshot.
        let snapshot = ovf.advance_epoch();
        assert!(ovf.try_spill(append_at(3, b"post-c", ovf.stamp())).is_ok());

        let tmp = tempfile::tempdir().unwrap();
        let (path, mut file) = incr_file(tmp.path(), "incr.aof");
        let mut db_ctx = 0usize;
        ovf.finish_framed(&rx, &mut file, &mut db_ctx, snapshot)
            .unwrap();

        assert_eq!(
            read_framed(&path),
            vec![(3, b"post-c".to_vec())],
            "pre-snapshot entries are in the committed base and must NOT be re-written"
        );
        assert_eq!(
            ack_rx.try_recv(),
            Ok(AofAck::Synced),
            "a dropped pre-snapshot AppendSync is durable via the base and must still ack"
        );
    }

    /// The case a position cut got wrong: a producer mutated before the
    /// snapshot but reached the CHANNEL only after it (parked on a full
    /// channel, or awaiting between mutation and enqueue). By position it is
    /// "post-cut"; by its stamp it is pre-snapshot and must be dropped.
    #[test]
    fn finish_drops_a_late_channel_record_stamped_before_the_snapshot() {
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(4);
        let ovf = RewriteOverflow::new();
        ovf.arm();
        // Mutation happens, its stamp is taken — then the producer is held
        // up across the snapshot.
        let late_stamp = ovf.stamp();
        let snapshot = ovf.advance_epoch();
        tx.try_send(append_at(1, b"late-pre", late_stamp)).unwrap();
        tx.try_send(append_at(2, b"post", ovf.stamp())).unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let (path, mut file) = incr_file(tmp.path(), "incr.aof");
        let mut db_ctx = 0usize;
        ovf.finish_framed(&rx, &mut file, &mut db_ctx, snapshot)
            .unwrap();

        assert_eq!(
            read_framed(&path),
            vec![(2, b"post".to_vec())],
            "a record whose mutation preceded the snapshot is in the base, wherever it lands"
        );
    }

    /// Abort: the old base predates every entry, so the floor is unchanged
    /// and ALL of them are written to the old incr — including those stamped
    /// before the aborted fold's snapshot.
    #[test]
    fn aborted_finish_writes_all_entries() {
        let (_tx, rx) = channel::mpsc_bounded::<AofMessage>(1);
        let ovf = RewriteOverflow::new();
        let floor_before = FoldEpoch::INITIAL;
        ovf.arm();
        assert!(ovf.try_spill(append_at(1, b"pre-a", ovf.stamp())).is_ok());
        let _aborted_snapshot = ovf.advance_epoch();
        assert!(ovf.try_spill(append_at(2, b"post-b", ovf.stamp())).is_ok());

        let tmp = tempfile::tempdir().unwrap();
        let (path, mut file) = incr_file(tmp.path(), "incr.aof");
        let mut db_ctx = 0usize;
        ovf.finish_framed(&rx, &mut file, &mut db_ctx, floor_before)
            .unwrap();

        assert_eq!(
            read_framed(&path),
            vec![(1, b"pre-a".to_vec()), (2, b"post-b".to_vec())],
            "an aborted fold keeps the old base — nothing folded, write everything"
        );
    }

    /// Each fold only drops what its OWN snapshot covers: entries stamped
    /// after an earlier fold's snapshot survive a later fold that never
    /// took effect, and are dropped by the next one that does.
    #[test]
    fn a_later_fold_drops_only_what_its_own_snapshot_covers() {
        let (_tx, rx) = channel::mpsc_bounded::<AofMessage>(1);
        let ovf = RewriteOverflow::new();
        let tmp = tempfile::tempdir().unwrap();
        let mut db_ctx = 0usize;

        // Fold 1 takes effect.
        ovf.arm();
        assert!(
            ovf.try_spill(append_at(1, b"fold1-pre", ovf.stamp()))
                .is_ok()
        );
        let floor1 = ovf.advance_epoch();
        let (p1, mut f1) = incr_file(tmp.path(), "incr1.aof");
        ovf.finish_framed(&rx, &mut f1, &mut db_ctx, floor1)
            .unwrap();
        assert!(read_framed(&p1).is_empty());

        // Fold 2 aborts: the floor stays at fold 1's snapshot.
        ovf.arm();
        assert!(
            ovf.try_spill(append_at(2, b"after-fold1", ovf.stamp()))
                .is_ok()
        );
        let _aborted = ovf.advance_epoch();
        let (p2, mut f2) = incr_file(tmp.path(), "incr2.aof");
        ovf.finish_framed(&rx, &mut f2, &mut db_ctx, floor1)
            .unwrap();
        assert_eq!(
            read_framed(&p2),
            vec![(2, b"after-fold1".to_vec())],
            "fold 1's floor must not drop a record stamped after its snapshot"
        );

        // Fold 3 takes effect: a record stamped during fold 2's epoch is
        // now inside the base.
        ovf.arm();
        let stamped_before_fold3 = ovf.stamp();
        let floor3 = ovf.advance_epoch();
        assert!(
            ovf.try_spill(append_at(3, b"before-fold3", stamped_before_fold3))
                .is_ok()
        );
        assert!(
            ovf.try_spill(append_at(4, b"after-fold3", ovf.stamp()))
                .is_ok()
        );
        let (p3, mut f3) = incr_file(tmp.path(), "incr3.aof");
        ovf.finish_framed(&rx, &mut f3, &mut db_ctx, floor3)
            .unwrap();
        assert_eq!(read_framed(&p3), vec![(4, b"after-fold3".to_vec())]);
    }

    /// A zero-length AppendSync is an fsync barrier, not a record: it logs
    /// nothing, so no floor may swallow it — its ack must wait for the
    /// boundary fsync like any other barrier.
    #[test]
    fn finish_never_drops_a_barrier() {
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(1);
        let ovf = RewriteOverflow::new();
        ovf.arm();
        let (ack_tx, ack_rx) = crate::runtime::channel::oneshot::<AofAck>();
        tx.try_send(AofMessage::AppendSync {
            lsn: 0,
            db: 0,
            bytes: Bytes::new(),
            ack: ack_tx,
            epoch: FoldEpoch::INITIAL,
        })
        .unwrap();
        let floor = ovf.advance_epoch();
        assert!(!is_folded(
            &AofMessage::Append {
                lsn: 0,
                db: 0,
                bytes: Bytes::new(),
                epoch: FoldEpoch::INITIAL,
            },
            floor
        ));
        let tmp = tempfile::tempdir().unwrap();
        let (path, mut file) = incr_file(tmp.path(), "incr.aof");
        let mut db_ctx = 0usize;
        ovf.finish_framed(&rx, &mut file, &mut db_ctx, floor)
            .unwrap();

        assert_eq!(ack_rx.try_recv(), Ok(AofAck::Synced));
        assert!(read_framed(&path).is_empty());
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
                epoch: FoldEpoch::INITIAL,
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
        ovf.finish_framed(&rx, &mut file, &mut db_ctx, FoldEpoch::INITIAL)
            .unwrap();

        assert_eq!(ack_rx.try_recv(), Ok(AofAck::Synced));
        assert_eq!(read_framed(&path), vec![(7, b"durable".to_vec())]);
    }

    // --- moon#1094: aof_last_append_status heals only on a covering commit ---
    use crate::persistence::aof::rewrite::FoldOutcome;
    //
    // Per-writer state is asserted, not the process-wide counter: other tests
    // in this binary drop appends on their own pools in parallel.

    /// A drop before the snapshot is inside the committed base: the writer's
    /// hole closes when that fold commits.
    #[test]
    fn a_committed_fold_after_the_drop_clears_the_hole() {
        let ovf = RewriteOverflow::new();
        super::super::record_append_dropped(&ovf, 1);
        assert!(ovf.is_missing_appends(), "a drop must open a hole");
        let floor = ovf.advance_epoch();
        let adopted = FoldOutcome::Committed { floor }.adopt(FoldEpoch::INITIAL, &ovf);
        assert_eq!(adopted, floor);
        assert!(
            !ovf.is_missing_appends(),
            "the committed base holds the dropped write: the hole must close"
        );
    }

    /// A drop between the snapshot and the commit is missing from the new
    /// generation too: that commit must not clear it. The next fold that
    /// commits does.
    #[test]
    fn a_drop_during_the_fold_survives_its_commit() {
        let ovf = RewriteOverflow::new();
        super::super::record_append_dropped(&ovf, 1);
        let floor1 = ovf.advance_epoch();
        // Lands after the snapshot instant, before the writer adopts.
        super::super::record_append_dropped(&ovf, 1);
        let f = FoldOutcome::Committed { floor: floor1 }.adopt(FoldEpoch::INITIAL, &ovf);
        assert!(
            ovf.is_missing_appends(),
            "a drop after the snapshot is not in the base: the status must stay err"
        );
        let floor2 = ovf.advance_epoch();
        FoldOutcome::Committed { floor: floor2 }.adopt(f, &ovf);
        assert!(
            !ovf.is_missing_appends(),
            "a later committed fold covers it"
        );
    }

    /// An aborted fold leaves the old base, which covers nothing new.
    #[test]
    fn an_aborted_fold_keeps_the_hole() {
        let ovf = RewriteOverflow::new();
        super::super::record_append_dropped(&ovf, 1);
        let _snapshot = ovf.advance_epoch();
        let f = FoldOutcome::Aborted.adopt(FoldEpoch::INITIAL, &ovf);
        assert_eq!(f, FoldEpoch::INITIAL);
        assert!(ovf.is_missing_appends());
    }

    /// Writers heal separately: one writer's clean commit says nothing about
    /// another writer's drop, and the process-wide status counts both.
    #[test]
    fn each_writer_heals_on_its_own_commit() {
        let a = RewriteOverflow::new();
        let b = RewriteOverflow::new();
        super::super::record_append_dropped(&a, 1);
        super::super::record_append_dropped(&b, 1);
        let floor_a = a.advance_epoch();
        FoldOutcome::Committed { floor: floor_a }.adopt(FoldEpoch::INITIAL, &a);
        assert!(!a.is_missing_appends());
        assert!(
            b.is_missing_appends(),
            "b's hole is untouched by a's commit"
        );
        assert!(
            !super::super::aof_last_append_ok(),
            "one writer still missing an append keeps the status err"
        );
        let floor_b = b.advance_epoch();
        FoldOutcome::Committed { floor: floor_b }.adopt(FoldEpoch::INITIAL, &b);
        assert!(!b.is_missing_appends());
    }

    /// moon#1215 reclaim: the committed floor moves only on a COMMITTED
    /// fold, only upwards, and a mutation stamped before a fold's snapshot is
    /// captured by it once it commits.
    #[test]
    fn the_committed_floor_follows_committed_folds_only() {
        let ovf = RewriteOverflow::new();
        assert_eq!(ovf.committed_floor(), FoldEpoch::INITIAL);
        let before = ovf.stamp();
        let aborted = ovf.advance_epoch();
        let f = FoldOutcome::Aborted.adopt(FoldEpoch::INITIAL, &ovf);
        assert_eq!(
            ovf.committed_floor(),
            FoldEpoch::INITIAL,
            "abort: unchanged"
        );
        assert!(!before.folded_below(ovf.committed_floor()));
        let floor = ovf.advance_epoch();
        assert!(floor > aborted);
        let after_snapshot = ovf.stamp();
        FoldOutcome::Committed { floor }.adopt(f, &ovf);
        assert_eq!(ovf.committed_floor(), floor);
        assert!(before.folded_below(ovf.committed_floor()));
        assert!(
            !after_snapshot.folded_below(ovf.committed_floor()),
            "a mutation after the snapshot is not in that generation"
        );
        ovf.note_committed(FoldEpoch::INITIAL);
        assert_eq!(ovf.committed_floor(), floor, "never lowered");
    }
}
