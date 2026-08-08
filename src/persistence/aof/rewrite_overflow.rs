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
        }
    }

    /// Arm at fold start. Called by the writer task immediately before
    /// entering a `do_rewrite_*` fold.
    pub(crate) fn arm(&self) {
        self.armed.store(true, std::sync::atomic::Ordering::Release);
    }

    /// Producer fast-path gate: true when the producer must spill directly
    /// (skip `try_send`) to preserve order — see ordering rule 1.
    #[inline]
    pub(crate) fn spill_first(&self) -> bool {
        self.armed.load(std::sync::atomic::Ordering::Acquire)
            && self.bytes.load(std::sync::atomic::Ordering::Relaxed) > 0
    }

    /// Try to buffer an overflow append. Returns the message back when the
    /// overflow is not armed (no fold in progress) or the cap is reached —
    /// the caller falls through to its pre-existing block/drop path.
    pub(crate) fn try_spill(&self, msg: AofMessage) -> Result<(), AofMessage> {
        let len = match &msg {
            AofMessage::Append { bytes, .. } => bytes.len(),
            AofMessage::AppendSync { bytes, .. } => bytes.len(),
            _ => return Err(msg),
        };
        let mut buf = self.buf.lock();
        // Re-check under the lock: a concurrent finish_* may have disarmed
        // between the caller's armed read and our lock acquisition.
        if !self.armed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(msg);
        }
        let cur = self.bytes.load(std::sync::atomic::Ordering::Relaxed);
        if cur.saturating_add(len) > self.max_bytes {
            return Err(msg);
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
    #[cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
    pub(crate) fn finish_framed(
        &self,
        rx: &channel::MpscReceiver<AofMessage>,
        file: &mut std::fs::File,
        db_ctx: &mut usize,
    ) -> Result<(), MoonError> {
        self.finish_inner(rx, file, db_ctx, true)
    }

    /// Post-fold drain + disarm, TopLevel RAW RESP format (no framing).
    #[cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
    pub(crate) fn finish_raw(
        &self,
        rx: &channel::MpscReceiver<AofMessage>,
        file: &mut std::fs::File,
        db_ctx: &mut usize,
    ) -> Result<(), MoonError> {
        self.finish_inner(rx, file, db_ctx, false)
    }

    #[cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
    fn finish_inner(
        &self,
        rx: &channel::MpscReceiver<AofMessage>,
        file: &mut std::fs::File,
        db_ctx: &mut usize,
        framed: bool,
    ) -> Result<(), MoonError> {
        use std::io::Write;
        // Hold the buffer lock for the whole drain (ordering rule 3).
        let mut buf = self.buf.lock();
        let spilled = std::mem::take(&mut *buf);
        let result = (|| -> Result<(), MoonError> {
            if spilled.is_empty() && rx.is_empty() {
                return Ok(());
            }
            // (a) channel first — everything in it is older than the buffer
            // (ordering rule 2). Bounded by the current length so we never
            // consume appends enqueued after this point.
            let chan_bound = rx.len();
            let mut outcome = if framed {
                drain_pending_appends_framed(rx, file, chan_bound, db_ctx)?
            } else {
                drain_pending_appends_bounded(rx, file, chan_bound, db_ctx)?
            };
            // (b) then the buffered overflow, in push order.
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
            for msg in spilled {
                match msg {
                    AofMessage::Append { lsn, db, bytes } => {
                        if let Some(sel) = select_prefix_if_needed(db, bytes.is_empty(), db_ctx) {
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
                        // Zero-length AppendSync = fsync barrier: no on-disk
                        // record, ack parks for the boundary fsync below
                        // (H1-BARRIER, mirrors drain_pending_appends_framed).
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
            if outcome.drained > 0 || drained_overflow > 0 {
                sync_and_fulfill_drain(
                    &mut outcome,
                    file,
                    PathBuf::from("<aof rewrite overflow drain>"),
                )?;
                info!(
                    "rewrite overflow drained: {} channel + {} spilled appends flushed post-fold",
                    outcome.drained, drained_overflow
                );
            }
            Ok(())
        })();
        // Unconditional disarm + reset (still under the lock): a failed drain
        // must not leave producers spilling into a buffer nobody will drain.
        self.bytes.store(0, std::sync::atomic::Ordering::Relaxed);
        self.armed
            .store(false, std::sync::atomic::Ordering::Release);
        drop(buf);
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
    /// Fatal-path disarm (writer thread exiting without a usable file):
    /// counts every still-buffered append as dropped (fail-loud, never
    /// silent) and stops producers from spilling into a dead buffer.
    /// Only the tokio TopLevel writer has a fatal reopen path today.
    #[cfg(feature = "runtime-tokio")]
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
        ovf.finish_framed(&rx, &mut file, &mut db_ctx).unwrap();

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
            .finish_framed(&rx, &mut file, &mut db_ctx)
            .unwrap();
        let lsns: Vec<u64> = read_framed(&path).into_iter().map(|(l, _)| l).collect();
        assert_eq!(lsns, vec![3, 4]);

        assert!(pool.try_send_append(0, 5, 0, Bytes::from_static(b"back-to-channel")));
        assert!(matches!(
            rx.try_recv(),
            Ok(AofMessage::Append { lsn: 5, .. })
        ));
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
        ovf.finish_framed(&rx, &mut file, &mut db_ctx).unwrap();

        assert_eq!(ack_rx.try_recv(), Ok(AofAck::Synced));
        assert_eq!(read_framed(&path), vec![(7, b"durable".to_vec())]);
    }
}
