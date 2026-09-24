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
//! name.
//!
//! A snapshot abandoned before finalize (an epoch a FLUSH*/SWAPDB aborted,
//! shard exit, error) CANCELS its helper and JOINS it (moon#1227 review F1):
//! the helper stops writing at its next chunk boundary, drops the rest of its
//! backlog unwritten, deletes its temp file and exits before the shard can
//! start the next snapshot of the same path. It used to keep draining up to
//! [`SNAPSHOT_STREAM_MAX_IN_FLIGHT`] of queued blocks into
//! `shard-N.rrdshard.tmp` while the next BGSAVE truncated and published that
//! same inode — `BGSAVE OK` for a file that failed its checksum at restart.
//! The join waits for at most one [`SNAPSHOT_STREAM_CHUNK`]-sized write plus
//! the unwritten backlog being freed, never for the backlog to reach the disk
//! and never for an fsync (a finishing snapshot is not cancelled).
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

/// Writer threads alive, by published path (tests only): lets a test wait
/// for — or assert the absence of — a writer that outlived its snapshot.
#[cfg(test)]
static LIVE_WRITERS: parking_lot::Mutex<Vec<PathBuf>> = parking_lot::Mutex::new(Vec::new());

/// Registered on the spawning thread (so a writer that has not been
/// scheduled yet already counts) and dropped when the writer thread exits.
#[cfg(test)]
struct LiveWriter(PathBuf);

#[cfg(test)]
impl LiveWriter {
    fn enter(file_path: &std::path::Path) -> Self {
        LIVE_WRITERS.lock().push(file_path.to_path_buf());
        Self(file_path.to_path_buf())
    }
}

#[cfg(test)]
impl Drop for LiveWriter {
    fn drop(&mut self) {
        let mut live = LIVE_WRITERS.lock();
        if let Some(i) = live.iter().position(|p| *p == self.0) {
            live.swap_remove(i);
        }
    }
}

/// Writer threads still running for a snapshot published at `file_path`.
#[cfg(test)]
pub(crate) fn live_writers_for_test(file_path: &std::path::Path) -> usize {
    LIVE_WRITERS
        .lock()
        .iter()
        .filter(|p| p.as_path() == file_path)
        .count()
}

/// Shard-side handle on a snapshot's helper thread.
pub(crate) struct SnapshotStream {
    tx: Option<flume::Sender<StreamMsg>>,
    done_rx: flume::Receiver<Result<(), String>>,
    in_flight: Arc<AtomicUsize>,
    failed: Arc<AtomicBool>,
    /// Set when the snapshot is abandoned before finalize: the helper stops
    /// writing at its next chunk boundary (moon#1227 review F1).
    cancelled: Arc<AtomicBool>,
    /// The helper, joined when the snapshot is abandoned so it is gone — its
    /// temp file closed and removed — before this path can be reused.
    helper: Option<std::thread::JoinHandle<()>>,
    finishing: bool,
}

impl Drop for SnapshotStream {
    /// Abandoned before finalize: cancel the helper and wait for it to exit.
    ///
    /// Bounded: the helper checks the flag between chunk-sized writes, so the
    /// join covers at most one [`SNAPSHOT_STREAM_CHUNK`] write, freeing the
    /// unwritten backlog, and unlinking the temp file. A FINISHING snapshot
    /// is left alone: its helper is fsyncing and renaming a complete file,
    /// and the tick only drops the state once the outcome arrived — the one
    /// exception is shard exit, which must not wait for that fsync.
    fn drop(&mut self) {
        if self.finishing {
            return;
        }
        self.cancelled.store(true, Ordering::Release);
        // Disconnect: once the (skipped) backlog is drained the helper sees
        // the channel close, removes its temp file and returns.
        drop(self.tx.take());
        if let Some(helper) = self.helper.take() {
            // A helper that panicked has nothing left to clean up; its temp
            // file is replaced by the next snapshot's fresh one.
            let _ = helper.join();
        }
    }
}

impl SnapshotStream {
    /// Spawn the helper for a snapshot published at `file_path` (written via
    /// `<file_path>.rrdshard.tmp`, the pre-existing temp name).
    pub(crate) fn spawn(shard_id: u16, file_path: PathBuf) -> std::io::Result<Self> {
        let (tx, rx) = flume::unbounded::<StreamMsg>();
        let (done_tx, done_rx) = flume::bounded::<Result<(), String>>(1);
        let in_flight = Arc::new(AtomicUsize::new(0));
        let failed = Arc::new(AtomicBool::new(false));
        let cancelled = Arc::new(AtomicBool::new(false));
        let helper_in_flight = Arc::clone(&in_flight);
        let helper_failed = Arc::clone(&failed);
        let helper_cancelled = Arc::clone(&cancelled);
        let name = format!("moon-snap-{shard_id}");
        #[cfg(test)]
        let live = LiveWriter::enter(&file_path);
        let helper = std::thread::Builder::new()
            .name(name.clone())
            .spawn(move || {
                #[cfg(test)]
                let _live = live;
                // O5: spawned from a pinned shard thread — re-pin to the
                // non-shard cores before doing any I/O.
                crate::shard::numa::pin_current_aux_thread(&name);
                run_helper(
                    file_path,
                    rx,
                    done_tx,
                    helper_in_flight,
                    helper_failed,
                    helper_cancelled,
                );
            })?;
        Ok(Self {
            tx: Some(tx),
            done_rx,
            in_flight,
            failed,
            cancelled,
            helper: Some(helper),
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
    cancelled: Arc<AtomicBool>,
) {
    let tmp_path = file_path.with_extension("rrdshard.tmp");
    let mut err: Option<String> = None;
    let mut file = match create_fresh(&tmp_path) {
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
                    // Chunk by chunk, checking for cancellation in between: a
                    // block can be far larger than a chunk (one huge value, or
                    // the in-memory path's whole buffer), and an abandoned
                    // snapshot's join must not wait for it (moon#1227 review
                    // F1). A cancelled helper never publishes, so the partial
                    // CRC state does not matter.
                    for piece in block.chunks(SNAPSHOT_STREAM_CHUNK) {
                        if cancelled.load(Ordering::Acquire) {
                            break;
                        }
                        hasher.update(piece);
                        if let Err(e) = f.write_all(piece) {
                            err = Some(format!("{}: {e}", tmp_path.display()));
                            failed.store(true, Ordering::Release);
                            break;
                        }
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

/// The temp file, on a FRESH inode (moon#1227 review F1, defence in depth
/// behind the cancel + join): a leftover of a crashed save is unlinked
/// first and `create_new` refuses a path something re-created meanwhile, so
/// this helper never shares an inode with any other writer. A straggler of
/// an earlier snapshot — which the join rules out — could then at worst
/// unlink this temp file and make the publish fail loudly (the rename finds
/// nothing); it could never append to the file this helper publishes, so
/// `BGSAVE OK` keeps meaning "the file is exactly what was serialized".
fn create_fresh(tmp_path: &std::path::Path) -> std::io::Result<std::fs::File> {
    match std::fs::remove_file(tmp_path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(tmp_path)
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
