//! Off-shard-thread writer for the incremental snapshot (moon#1186).
//!
//! `SnapshotState` serializes one DashTable segment per shard tick. It used
//! to keep the ENTIRE file in one `Vec` until the last segment and then
//! `std::fs::write` + fsync + rename + dir-fsync it from the shard event loop
//! (`finalize_async` was synchronous under monoio, and its fsync was
//! synchronous under tokio too): a multi-GB snapshot stalled the shard for
//! seconds and held ~1-2x the file in RAM.
//!
//! Now the shard hands each filled block ([`SNAPSHOT_STREAM_CHUNK`]) to a
//! helper thread as it is produced; the helper appends it to the temp file
//! and folds it into the global CRC32 as the bytes pass. On finalize the
//! helper writes the CRC footer, fsyncs, renames, fsyncs the directory, and
//! reports the outcome on a channel the tick polls with a non-blocking
//! `try_recv` — the `data_file_sync` discipline: at most one helper per
//! snapshot, the periodic tick never waits.
//!
//! Crash safety is the pre-existing temp + fsync + rename + dir-fsync
//! sequence, now run by the helper: a kill at any point leaves either the
//! previous `.rrdshard` or the new one, never a torn file under the real
//! name. A snapshot abandoned before finalize (shard exit, error) disconnects
//! the channel and the helper deletes its temp file.
//!
//! Memory in flight is bounded: while more than
//! [`SNAPSHOT_STREAM_MAX_IN_FLIGHT`] bytes are handed over but not yet
//! written, the shard stops advancing segments (it never blocks) until the
//! helper catches up.

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crc32fast::Hasher;

/// Block size handed to the helper: the shard ships its output buffer once it
/// holds at least this much.
pub(crate) const SNAPSHOT_STREAM_CHUNK: usize = 256 * 1024;

/// Bytes handed to the helper but not yet written above which the shard
/// pauses segment serialization for the tick.
pub(crate) const SNAPSHOT_STREAM_MAX_IN_FLIGHT: usize = 32 << 20;

enum StreamMsg {
    Data(Vec<u8>),
    Finish,
}

/// Shard-side handle on a snapshot's helper thread.
pub(crate) struct SnapshotStream {
    tx: Option<flume::Sender<StreamMsg>>,
    done_rx: flume::Receiver<Result<(), String>>,
    in_flight: Arc<AtomicUsize>,
    failed: Arc<AtomicBool>,
    finishing: bool,
}

impl SnapshotStream {
    /// Spawn the helper for a snapshot published at `file_path` (written via
    /// `<file_path>.rrdshard.tmp`, the pre-existing temp name).
    pub(crate) fn spawn(shard_id: u16, file_path: PathBuf) -> std::io::Result<Self> {
        let (tx, rx) = flume::unbounded::<StreamMsg>();
        let (done_tx, done_rx) = flume::bounded::<Result<(), String>>(1);
        let in_flight = Arc::new(AtomicUsize::new(0));
        let failed = Arc::new(AtomicBool::new(false));
        let helper_in_flight = Arc::clone(&in_flight);
        let helper_failed = Arc::clone(&failed);
        let name = format!("moon-snap-{shard_id}");
        std::thread::Builder::new()
            .name(name.clone())
            .spawn(move || {
                // O5: spawned from a pinned shard thread — re-pin to the
                // non-shard cores before doing any I/O.
                crate::shard::numa::pin_current_aux_thread(&name);
                run_helper(file_path, rx, done_tx, helper_in_flight, helper_failed);
            })?;
        Ok(Self {
            tx: Some(tx),
            done_rx,
            in_flight,
            failed,
            finishing: false,
        })
    }

    /// Hand one block to the helper. Never blocks.
    pub(crate) fn send(&self, block: Vec<u8>) {
        if block.is_empty() {
            return;
        }
        let Some(tx) = &self.tx else {
            return;
        };
        let len = block.len();
        self.in_flight.fetch_add(len, Ordering::AcqRel);
        if tx.send(StreamMsg::Data(block)).is_err() {
            // Helper gone (it only exits early by panicking): the snapshot
            // cannot complete — surfaced at finalize.
            self.in_flight.fetch_sub(len, Ordering::AcqRel);
            self.failed.store(true, Ordering::Release);
        }
    }

    /// Ask the helper to write the footer and publish the file.
    pub(crate) fn finish(&mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(StreamMsg::Finish);
        }
        self.finishing = true;
    }

    /// True once [`Self::finish`] was called.
    #[inline]
    pub(crate) fn finishing(&self) -> bool {
        self.finishing
    }

    /// Non-blocking: the publish outcome once the helper is done.
    pub(crate) fn poll(&self) -> Option<Result<(), String>> {
        match self.done_rx.try_recv() {
            Ok(r) => Some(r),
            Err(flume::TryRecvError::Empty) => None,
            Err(flume::TryRecvError::Disconnected) => {
                Some(Err("snapshot writer thread exited without a result".into()))
            }
        }
    }

    /// Blocking wait for the publish outcome (synchronous `finalize` only —
    /// never called from the shard tick).
    pub(crate) fn wait(&self) -> Result<(), String> {
        self.done_rx
            .recv()
            .unwrap_or_else(|_| Err("snapshot writer thread exited without a result".into()))
    }

    /// More than [`SNAPSHOT_STREAM_MAX_IN_FLIGHT`] bytes are waiting for the
    /// disk: pause serialization this tick.
    #[inline]
    pub(crate) fn backlogged(&self) -> bool {
        self.in_flight.load(Ordering::Acquire) > SNAPSHOT_STREAM_MAX_IN_FLIGHT
    }

    /// A write already failed: the snapshot cannot succeed, stop producing.
    #[inline]
    pub(crate) fn failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    /// Bytes handed over but not yet written (instrumentation / tests).
    #[cfg(test)]
    pub(crate) fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }
}

fn run_helper(
    file_path: PathBuf,
    rx: flume::Receiver<StreamMsg>,
    done_tx: flume::Sender<Result<(), String>>,
    in_flight: Arc<AtomicUsize>,
    failed: Arc<AtomicBool>,
) {
    let tmp_path = file_path.with_extension("rrdshard.tmp");
    let mut err: Option<String> = None;
    let mut file = match std::fs::File::create(&tmp_path) {
        Ok(f) => Some(f),
        Err(e) => {
            err = Some(format!("{}: {e}", tmp_path.display()));
            failed.store(true, Ordering::Release);
            None
        }
    };
    let mut hasher = Hasher::new();
    loop {
        match rx.recv() {
            Ok(StreamMsg::Data(block)) => {
                if err.is_none()
                    && let Some(f) = file.as_mut()
                {
                    hasher.update(&block);
                    if let Err(e) = f.write_all(&block) {
                        err = Some(format!("{}: {e}", tmp_path.display()));
                        failed.store(true, Ordering::Release);
                    }
                }
                in_flight.fetch_sub(block.len(), Ordering::AcqRel);
            }
            Ok(StreamMsg::Finish) => {
                let result = match (err, file.take()) {
                    (Some(e), _) => Err(e),
                    (None, Some(f)) => publish(f, hasher, &tmp_path, &file_path),
                    (None, None) => Err("snapshot temp file unavailable".into()),
                };
                if result.is_err() {
                    let _ = std::fs::remove_file(&tmp_path);
                }
                let _ = done_tx.send(result);
                return;
            }
            Err(_) => {
                // Abandoned before finalize: nothing will ever publish this
                // temp file.
                drop(file.take());
                let _ = std::fs::remove_file(&tmp_path);
                return;
            }
        }
    }
}

/// CRC footer, fsync, rename, directory fsync — the pre-moon#1186 finalize
/// sequence, off the shard thread.
fn publish(
    mut f: std::fs::File,
    hasher: Hasher,
    tmp_path: &std::path::Path,
    file_path: &std::path::Path,
) -> Result<(), String> {
    let crc = hasher.finalize();
    f.write_all(&crc.to_le_bytes())
        .map_err(|e| format!("{}: {e}", tmp_path.display()))?;
    f.sync_all()
        .map_err(|e| format!("{}: {e}", tmp_path.display()))?;
    drop(f);
    std::fs::rename(tmp_path, file_path).map_err(|e| format!("{}: {e}", file_path.display()))?;
    if let Some(parent) = file_path.parent() {
        crate::persistence::fsync::fsync_directory(parent)
            .map_err(|e| format!("{}: {e}", parent.display()))?;
    }
    Ok(())
}
