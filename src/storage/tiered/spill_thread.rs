//! Background I/O thread for async eviction spill-to-disk.
//!
//! The monoio event loop is single-threaded. Synchronous pwrite during eviction
//! blocks ALL connections. This module provides a fire-and-forget channel
//! infrastructure so pwrite happens on a dedicated `std::thread`.
//!
//! ## Batching model
//!
//! The background thread accumulates incoming `SpillRequest`s in a buffer and
//! flushes them as a single multi-page `.mpf` file.  Flush triggers:
//!
//! - Buffer reaches `FLUSH_ENTRY_CAP` entries (size guard).
//! - The 100 ms `recv_timeout` tick fires with a non-empty buffer (latency guard).
//! - Shutdown / channel disconnect (drain guard).
//!
//! Each flush assigns ONE `file_id` (taken from `buffer[0].file_id`) and emits
//! ONE per-FILE `SpillCompletion` carrying all `(key, db_index, page_idx,
//! slot_idx)` tuples.  The caller registers ONE manifest entry per file, then
//! inserts into the cold_index for each tuple.  This bounds manifest entries to
//! `#files`, not `#keys`, removing the ~70-entry cap.
//!
//! Pattern: event loop builds `SpillRequest` (CPU-only, no I/O) -> sends via
//! flume channel -> background thread buffers + writes -> sends `SpillCompletion`
//! back -> event loop polls completions and updates manifest + ColdIndex.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Maximum entries to buffer before forcing a flush.
/// At ~200 B/entry this is ~50 KB of in-memory data — well under any
/// reasonable memory budget and keeps file sizes manageable.
const FLUSH_ENTRY_CAP: usize = 256;

/// Cumulative count of `SpillCompletion`s dropped. Dropping is **data loss**:
/// the `.mpf` file is on disk but its manifest `add_file` + `cold_index` insert
/// happen only when the event loop consumes the completion, and the keys are
/// already evicted from RAM. The sender therefore blocks on a full channel
/// instead of dropping, so this only increments in the rare
/// shutdown-with-full-channel edge. Exposed for INFO / metrics scraping.
static SPILL_COMPLETION_DROPPED: AtomicU64 = AtomicU64::new(0);

/// Returns the cumulative number of dropped spill completions across all
/// shards. Exposed for INFO / metrics scraping.
#[inline]
pub fn spill_completion_dropped_total() -> u64 {
    SPILL_COMPLETION_DROPPED.load(Ordering::Relaxed)
}

use bytes::Bytes;
use tracing::warn;

use crate::persistence::kv_page::ValueType;
use crate::persistence::manifest::{FileEntry, FileStatus, StorageTier};
use crate::persistence::page::PageType;
use crate::storage::tiered::kv_spill::{
    INLINE_MAX_VALUE_BYTES, SpillEntry, build_kv_spill_batch, build_kv_spill_pages,
    write_kv_spill_batch, write_kv_spill_pages,
};

/// Request sent from event loop to background spill thread.
///
/// Contains all data needed for pwrite -- no references to shard state.
/// `Bytes` fields are reference-counted (cheap clone on event loop side).
pub struct SpillRequest {
    pub key: Bytes,
    /// Logical database index the key was evicted from. Used by completion
    /// handler to update the correct per-DB cold_index.
    pub db_index: usize,
    /// Already-serialized value (string bytes or kv_serde output).
    pub value_bytes: Bytes,
    /// Value type discriminant from `kv_page::ValueType`.
    pub value_type: ValueType,
    /// Entry flags (HAS_TTL, OVERFLOW, etc.) from `kv_page::entry_flags`.
    pub flags: u8,
    /// Absolute TTL in milliseconds if `HAS_TTL` flag is set.
    pub ttl_ms: Option<u64>,
    /// Pre-assigned file ID (event loop increments `next_file_id` before sending).
    /// Under batching, the FIRST request in the buffer supplies the file_id for
    /// the whole flush; subsequent IDs in the same flush are unused (sparse gaps
    /// in the file_id space are harmless — recovery iterates manifest entries,
    /// not a dense id range).
    pub file_id: u64,
    /// Shard data directory path.
    pub shard_dir: PathBuf,
}

/// Per-entry result within a `SpillCompletion`.
pub struct SpillCompletionEntry {
    /// Original key (for cold_index insertion).
    pub key: Bytes,
    /// Logical DB index (for routing the cold_index update).
    pub db_index: usize,
    /// File-absolute 4KB page index within the DataFile.
    pub page_idx: u32,
    /// Slot index within that leaf page.
    pub slot_idx: u16,
}

/// Completion sent from background thread back to event loop.
///
/// ONE completion per flushed file (may cover many keys).
/// Carries everything needed for manifest + ColdIndex update.
pub struct SpillCompletion {
    /// Ready-to-use FileEntry for `manifest.add_file()`.
    pub file_entry: FileEntry,
    /// Per-entry locations within this file.
    pub entries: Vec<SpillCompletionEntry>,
    /// Whether the pwrite succeeded. If false, no entries should be indexed.
    pub success: bool,
}

/// Build a `FileEntry` skeleton for a spill file (fields not tracked by Moon are zero).
fn make_file_entry(file_id: u64, page_count: u32, byte_size: u64) -> FileEntry {
    FileEntry {
        file_id,
        file_type: PageType::KvLeaf as u8,
        status: FileStatus::Active,
        tier: StorageTier::Hot,
        page_size_log2: 12, // 4KB = 2^12
        page_count,
        byte_size,
        created_lsn: 0,
        min_key_hash: 0,
        max_key_hash: 0,
        last_modified_lsn: 0,
    }
}

/// Flush the buffered requests.
///
/// ## Routing
///
/// Entries are pre-screened by `value_bytes.len()`:
///
/// - **Inline** (`value_bytes.len() ≤ INLINE_MAX_VALUE_BYTES`): packed into ONE
///   multi-page `.mpf` file using `build_kv_spill_batch` + `write_kv_spill_batch`.
///   ONE `SpillCompletion` is emitted for the batch file.
///
/// - **Oversized** (`value_bytes.len() > INLINE_MAX_VALUE_BYTES`): each entry
///   gets its own single-page file via `build_kv_spill_pages` + `write_kv_spill_pages`
///   (the existing single-file path, same as `spill_to_datafile`).  ONE
///   `SpillCompletion` is emitted per oversized entry; its `page_idx` is always 0.
///
/// This keeps manifest entries == #files (not #keys) for inline entries, removing
/// the ~70-entry cap.  Oversized entries still cost one manifest entry each, but
/// they are rare in typical workloads.
///
/// Returns a `Vec<SpillCompletion>` (one per file written).  Never panics.
fn flush_buffer(buffer: &mut Vec<SpillRequest>) -> Vec<SpillCompletion> {
    if buffer.is_empty() {
        return Vec::new();
    }

    let mut completions: Vec<SpillCompletion> = Vec::new();

    // ── Partition into inline candidates and oversized entries ────────────────
    // We use indices to avoid re-allocating keys.  `inline_indices` are the
    // positions in `buffer` of entries that fit the inline threshold.
    let mut inline_indices: Vec<usize> = Vec::with_capacity(buffer.len());
    let mut oversized_indices: Vec<usize> = Vec::new();

    for (i, req) in buffer.iter().enumerate() {
        if req.value_bytes.len() <= INLINE_MAX_VALUE_BYTES {
            inline_indices.push(i);
        } else {
            oversized_indices.push(i);
        }
    }

    // ── Write ONE batch file for all inline entries ───────────────────────────
    if !inline_indices.is_empty() {
        // Use the file_id of the first inline entry for the batch file.
        let file_id = buffer[inline_indices[0]].file_id;
        let shard_dir = buffer[inline_indices[0]].shard_dir.clone();

        let spill_entries: Vec<SpillEntry> = inline_indices
            .iter()
            .map(|&i| SpillEntry {
                key: buffer[i].key.clone(),
                value_bytes: buffer[i].value_bytes.clone(),
                value_type: buffer[i].value_type,
                flags: buffer[i].flags,
                ttl_ms: buffer[i].ttl_ms,
            })
            .collect();

        match build_kv_spill_batch(&spill_entries, file_id) {
            Ok(batch) => {
                let total_pages = batch.leaves.len() as u32; // overflow is always empty (inline-only)
                match write_kv_spill_batch(&shard_dir, file_id, &batch) {
                    Ok(byte_size) => {
                        let entries = inline_indices
                            .iter()
                            .zip(batch.locations.iter())
                            .map(|(&buf_idx, &(page_idx, slot_idx))| SpillCompletionEntry {
                                key: buffer[buf_idx].key.clone(),
                                db_index: buffer[buf_idx].db_index,
                                page_idx,
                                slot_idx,
                            })
                            .collect();
                        completions.push(SpillCompletion {
                            file_entry: make_file_entry(file_id, total_pages, byte_size),
                            entries,
                            success: true,
                        });
                    }
                    Err(e) => {
                        warn!(
                            file_id,
                            error = %e,
                            count = inline_indices.len(),
                            "spill_thread: inline batch write failed; falling back to per-entry spill"
                        );
                        // Salvage each entry individually rather than dropping the
                        // whole already-evicted flush.
                        for &i in &inline_indices {
                            completions.push(spill_single_entry(&buffer[i], buffer[i].file_id));
                        }
                    }
                }
            }
            Err(e) => {
                warn!(
                    file_id,
                    error = %e,
                    count = inline_indices.len(),
                    "spill_thread: inline batch build failed; falling back to per-entry spill"
                );
                // One entry that does not fit a fresh inline leaf must not drop the
                // whole batch — write each via the overflow path instead.
                for &i in &inline_indices {
                    completions.push(spill_single_entry(&buffer[i], buffer[i].file_id));
                }
            }
        }
    }

    // ── Write ONE single-page file per oversized entry ────────────────────────
    for &i in &oversized_indices {
        completions.push(spill_single_entry(&buffer[i], buffer[i].file_id));
    }

    buffer.clear();
    completions
}

/// Spill ONE entry as its own single-page (+overflow) file via
/// `build_kv_spill_pages`.
///
/// Used both for oversized entries and as the inline-batch fallback: when an
/// inline batch fails (an entry that passed the `value_bytes.len()` pre-screen
/// still does not fit a fresh leaf), salvaging each entry here prevents one bad
/// candidate from dropping a whole flush whose keys are already out of RAM.
/// Returns a failed completion only if this single entry cannot be written even
/// with overflow pages (key too large for a leaf).
fn spill_single_entry(req: &SpillRequest, file_id: u64) -> SpillCompletion {
    match build_kv_spill_pages(
        &req.key,
        &req.value_bytes,
        req.value_type,
        req.flags,
        req.ttl_ms,
        file_id,
    ) {
        Ok(pages) => match write_kv_spill_pages(&req.shard_dir, file_id, &pages) {
            Ok(byte_size) => SpillCompletion {
                file_entry: make_file_entry(file_id, pages.total_pages, byte_size),
                entries: vec![SpillCompletionEntry {
                    key: req.key.clone(),
                    db_index: req.db_index,
                    page_idx: 0,
                    slot_idx: 0,
                }],
                success: true,
            },
            Err(e) => {
                warn!(
                    file_id,
                    error = %e,
                    key_len = req.key.len(),
                    "spill_thread: single-file write failed"
                );
                SpillCompletion {
                    file_entry: make_file_entry(file_id, 0, 0),
                    entries: Vec::new(),
                    success: false,
                }
            }
        },
        Err(e) => {
            warn!(
                file_id,
                error = %e,
                key_len = req.key.len(),
                "spill_thread: single-file build failed (key too large)"
            );
            SpillCompletion {
                file_entry: make_file_entry(file_id, 0, 0),
                entries: Vec::new(),
                success: false,
            }
        }
    }
}

/// Background thread that performs pwrite for evicted KV entries.
///
/// One per shard. Matches the WAL writer pattern: dedicated `std::thread`
/// that blocks on a flume channel, buffers requests, and flushes as batched
/// multi-page DataFiles.
pub struct SpillThread {
    request_tx: flume::Sender<SpillRequest>,
    completion_rx: flume::Receiver<SpillCompletion>,
    join_handle: Option<std::thread::JoinHandle<()>>,
    stop_flag: Arc<AtomicBool>,
}

impl SpillThread {
    /// Spawn a new background spill thread for the given shard.
    ///
    /// Creates two bounded flume channels:
    /// - `request`: bounded(4096), event loop -> bg thread
    /// - `completion`: bounded(8192), bg thread -> event loop
    ///
    /// The completion channel is bounded so a stalled event loop cannot let
    /// in-flight `SpillCompletion`s accumulate without limit. A dropped
    /// completion means the data is already on disk — the next checkpoint
    /// rebuilds `cold_index` from the manifest, so dropping is safe (though
    /// we count it for observability).
    pub fn new(shard_id: usize) -> Self {
        let (request_tx, request_rx) = flume::bounded::<SpillRequest>(4096);
        let (completion_tx, completion_rx) = flume::bounded::<SpillCompletion>(8192);
        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_flag_bg = stop_flag.clone();

        #[allow(clippy::expect_used)]
        // Startup: spill thread is critical infrastructure — spawn failure is fatal
        let join_handle = std::thread::Builder::new()
            .name(format!("spill-{shard_id}"))
            .spawn(move || {
                Self::run(request_rx, completion_tx, stop_flag_bg);
            })
            .expect("failed to spawn spill thread");

        Self {
            request_tx,
            completion_rx,
            join_handle: Some(join_handle),
            stop_flag,
        }
    }

    /// Background thread main loop.
    ///
    /// Buffers incoming requests.  Flushes when:
    /// - Buffer reaches `FLUSH_ENTRY_CAP` (size guard).
    /// - `recv_timeout(100ms)` fires with a non-empty buffer (latency guard).
    /// - stop_flag is set or channel disconnects — flush remaining, then exit.
    fn run(
        request_rx: flume::Receiver<SpillRequest>,
        completion_tx: flume::Sender<SpillCompletion>,
        stop_flag: Arc<AtomicBool>,
    ) {
        let mut buffer: Vec<SpillRequest> = Vec::with_capacity(FLUSH_ENTRY_CAP);

        loop {
            // Check stop flag — drain any still-queued requests and flush them
            // before exiting, so spills queued at shutdown are not lost (their
            // keys may already be evicted from RAM).
            if stop_flag.load(Ordering::Acquire) {
                while let Ok(req) = request_rx.try_recv() {
                    buffer.push(req);
                }
                if !buffer.is_empty() {
                    Self::send_completions(&completion_tx, flush_buffer(&mut buffer), &stop_flag);
                }
                break;
            }

            match request_rx.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(req) => {
                    buffer.push(req);
                    if buffer.len() >= FLUSH_ENTRY_CAP {
                        Self::send_completions(
                            &completion_tx,
                            flush_buffer(&mut buffer),
                            &stop_flag,
                        );
                    }
                }
                Err(flume::RecvTimeoutError::Timeout) => {
                    // Latency guard: flush non-empty buffer on tick.
                    if !buffer.is_empty() {
                        Self::send_completions(
                            &completion_tx,
                            flush_buffer(&mut buffer),
                            &stop_flag,
                        );
                    }
                }
                Err(flume::RecvTimeoutError::Disconnected) => {
                    // Drain guard: flush remaining entries then exit.
                    if !buffer.is_empty() {
                        Self::send_completions(
                            &completion_tx,
                            flush_buffer(&mut buffer),
                            &stop_flag,
                        );
                    }
                    break;
                }
            }
        }
    }

    /// Send multiple completions, applying backpressure on a full channel.
    fn send_completions(
        completion_tx: &flume::Sender<SpillCompletion>,
        completions: Vec<SpillCompletion>,
        stop_flag: &Arc<AtomicBool>,
    ) {
        for completion in completions {
            Self::send_one_completion(completion_tx, completion, stop_flag);
        }
    }

    /// Send a single completion to the event loop.
    ///
    /// Dropping a completion is **data loss**: by the time it is produced the
    /// key has already been evicted from RAM, and the manifest `add_file` +
    /// `cold_index` insert happen only when the event loop consumes the
    /// completion (`apply_spill_completions`). A dropped completion therefore
    /// orphans the on-disk `.mpf` file (never recorded in the manifest) and
    /// loses its keys permanently after restart.
    ///
    /// So we block until the event loop drains a slot rather than dropping.
    /// This is safe: the dedicated spill thread is the only blocker, and the
    /// eviction path enqueues `SpillRequest`s with `try_send` (never blocking
    /// on us), so there is no cyclic wait. We honour `stop_flag` so shutdown
    /// cannot wedge the join — and because `shutdown()` drains the channel
    /// after the join, the final-flush completions are still applied.
    fn send_one_completion(
        completion_tx: &flume::Sender<SpillCompletion>,
        completion: SpillCompletion,
        stop_flag: &Arc<AtomicBool>,
    ) {
        let mut pending = completion;
        loop {
            match completion_tx.send_timeout(pending, std::time::Duration::from_millis(100)) {
                Ok(()) => return,
                Err(flume::SendTimeoutError::Timeout(c)) => {
                    if stop_flag.load(Ordering::Acquire) {
                        // Shutting down and the event loop is no longer draining;
                        // we cannot persist the manifest entry ourselves. Count +
                        // warn. In practice unreachable: shutdown() drains the
                        // channel before the final flush, and that flush is bounded
                        // by FLUSH_ENTRY_CAP (< channel capacity), so it always fits.
                        SPILL_COMPLETION_DROPPED.fetch_add(1, Ordering::Relaxed);
                        warn!(
                            "spill_thread: completion channel full at shutdown; dropping completion (total dropped: {})",
                            SPILL_COMPLETION_DROPPED.load(Ordering::Relaxed)
                        );
                        return;
                    }
                    // Backpressure: the event loop will drain a slot. Retry rather
                    // than drop — a dropped completion is permanent key loss.
                    pending = c;
                }
                Err(flume::SendTimeoutError::Disconnected(_)) => {
                    // Event loop dropped its receiver -- shutting down; ignore.
                    return;
                }
            }
        }
    }

    /// Get a clone of the request sender for the event loop to hold.
    pub fn sender(&self) -> flume::Sender<SpillRequest> {
        self.request_tx.clone()
    }

    /// Non-blocking poll for a single completion.
    pub fn try_recv_completion(&self) -> Option<SpillCompletion> {
        self.completion_rx.try_recv().ok()
    }

    /// Drain all pending completions (non-blocking).
    pub fn drain_completions(&self) -> Vec<SpillCompletion> {
        let mut completions = Vec::new();
        while let Ok(c) = self.completion_rx.try_recv() {
            completions.push(c);
        }
        completions
    }

    /// Shut down the background thread cleanly.
    ///
    /// Sets a stop flag and joins. Safe to call even when cloned `Sender`s are
    /// still alive: the background thread polls the flag every 100 ms and
    /// exits without waiting for channel close. This avoids the deadlock where
    /// connection futures held cloned senders past shutdown.
    /// Returns any completions produced by the thread's final buffer flush that
    /// the event loop never drained, so the caller can still apply them
    /// (manifest `add_file` + `cold_index` insert) instead of losing those keys.
    #[must_use]
    pub fn shutdown(mut self) -> Vec<SpillCompletion> {
        self.stop_flag.store(true, Ordering::Release);
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.join();
        }
        // Thread has exited; surface any completions from its final buffer flush
        // that the event loop never drained, so the caller can still apply them.
        self.completion_rx.try_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::kv_page::{ValueType, entry_flags};
    use crate::persistence::page::PAGE_4K;
    use crate::storage::entry::current_time_ms;

    /// Helper: wait for at least `expected_entries` total entries across all
    /// completions, with a deadline.
    fn collect_entries(
        st: &SpillThread,
        expected_entries: usize,
        deadline: std::time::Instant,
    ) -> Vec<SpillCompletion> {
        let mut completions = Vec::new();
        let mut total_entries = 0;
        while total_entries < expected_entries && std::time::Instant::now() < deadline {
            let new = st.drain_completions();
            for c in &new {
                total_entries += c.entries.len();
            }
            completions.extend(new);
            if total_entries < expected_entries {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
        completions
    }

    #[test]
    fn test_spill_thread_new_returns_valid_handles() {
        let st = SpillThread::new(0);
        assert!(!st.request_tx.is_disconnected());
        assert!(!st.completion_rx.is_disconnected());
        let _ = st.shutdown();
    }

    /// Single request produces a successful per-FILE completion with one entry.
    #[test]
    fn test_spill_request_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let st = SpillThread::new(1);
        let sender = st.sender();

        let req = SpillRequest {
            key: Bytes::from_static(b"test_key"),
            db_index: 0,
            value_bytes: Bytes::from_static(b"test_value"),
            value_type: ValueType::String,
            flags: 0,
            ttl_ms: None,
            file_id: 1,
            shard_dir: tmp.path().to_path_buf(),
        };
        sender.send(req).unwrap();
        drop(sender);

        // Wait for the buffer to flush (100 ms tick or disconnect).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let completions = collect_entries(&st, 1, deadline);

        let total_entries: usize = completions.iter().map(|c| c.entries.len()).sum();
        assert_eq!(total_entries, 1, "expected 1 entry across all completions");

        let c = completions.iter().find(|c| !c.entries.is_empty()).unwrap();
        assert!(c.success);
        assert_eq!(c.file_entry.file_type, PageType::KvLeaf as u8);
        assert!(c.file_entry.page_count >= 1);
        assert!(c.file_entry.byte_size >= PAGE_4K as u64);

        let entry = &c.entries[0];
        assert_eq!(entry.key, Bytes::from_static(b"test_key"));
        assert_eq!(entry.db_index, 0);

        // File must exist on disk.
        let file_path = tmp
            .path()
            .join("data")
            .join(format!("heap-{:06}.mpf", c.file_entry.file_id));
        assert!(file_path.exists(), "spill file should exist");

        // Verify content via cold_read_at.
        use crate::storage::tiered::cold_index::ColdLocation;
        use crate::storage::tiered::cold_read::read_cold_entry_at;
        let loc = ColdLocation {
            file_id: c.file_entry.file_id,
            page_idx: entry.page_idx,
            slot_idx: entry.slot_idx,
        };
        let result = read_cold_entry_at(tmp.path(), loc, 0);
        assert!(result.is_some(), "should read entry back");
        let (value, _ttl) = result.unwrap();
        match value {
            crate::storage::entry::RedisValue::String(data) => {
                assert_eq!(data.as_ref(), b"test_value");
            }
            _ => panic!("expected String"),
        }

        let _ = st.shutdown();
    }

    /// A full completion channel must apply backpressure (block until the event
    /// loop drains a slot), never drop — a dropped completion permanently loses
    /// the already-evicted keys (orphaned `.mpf`, never recorded in the manifest).
    #[test]
    fn full_completion_channel_blocks_instead_of_dropping() {
        use std::sync::atomic::AtomicUsize;
        let (tx, rx) = flume::bounded::<SpillCompletion>(1);
        let stop = Arc::new(AtomicBool::new(false));
        let dummy = || SpillCompletion {
            file_entry: make_file_entry(7, 1, PAGE_4K as u64),
            entries: Vec::new(),
            success: true,
        };

        // Saturate the single slot so the next send must wait for a free slot.
        tx.try_send(dummy()).unwrap();

        let received = Arc::new(AtomicUsize::new(0));
        let r2 = received.clone();
        let drainer = std::thread::spawn(move || {
            // Delay draining so send_one_completion is forced to block first.
            std::thread::sleep(std::time::Duration::from_millis(50));
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            while std::time::Instant::now() < deadline {
                if rx
                    .recv_timeout(std::time::Duration::from_millis(50))
                    .is_ok()
                {
                    r2.fetch_add(1, Ordering::Relaxed);
                } else if r2.load(Ordering::Relaxed) >= 2 {
                    break;
                }
            }
        });

        // Must block until a slot frees, then deliver — never drop on Full.
        SpillThread::send_one_completion(&tx, dummy(), &stop);
        drop(tx);
        drainer.join().unwrap();

        assert_eq!(
            received.load(Ordering::Relaxed),
            2,
            "both completions must be delivered; a Full channel must block, not drop"
        );
    }

    /// `shutdown()` must surface the thread's final-flush completions so the
    /// caller can still apply them — they would otherwise be silently lost
    /// (file on disk, never added to the manifest).
    #[test]
    fn shutdown_drains_and_returns_unapplied_completions() {
        let tmp = tempfile::tempdir().unwrap();
        let st = SpillThread::new(9);
        let sender = st.sender();
        sender
            .send(SpillRequest {
                key: Bytes::from_static(b"shutdown_key"),
                db_index: 0,
                value_bytes: Bytes::from_static(b"v"),
                value_type: ValueType::String,
                flags: 0,
                ttl_ms: None,
                file_id: 1,
                shard_dir: tmp.path().to_path_buf(),
            })
            .unwrap();
        drop(sender);

        // Deliberately do NOT drain via the event loop. The completion is
        // produced by the thread's final flush; shutdown() must return it.
        let leftover = st.shutdown();
        let total: usize = leftover.iter().map(|c| c.entries.len()).sum();
        assert_eq!(
            total, 1,
            "shutdown must return the unapplied completion, not drop it"
        );
    }

    /// An entry that passes the `INLINE_MAX_VALUE_BYTES` pre-screen but does not
    /// fit a fresh inline leaf (large key + incompressible value) makes
    /// `build_kv_spill_batch` fail. That must NOT fail the whole inline flush —
    /// the offender is salvaged via the per-entry (overflow) path instead, since
    /// its key is already evicted from RAM.
    #[test]
    fn inline_batch_failure_falls_back_to_per_entry_spill() {
        let tmp = tempfile::tempdir().unwrap();

        // High-entropy (LZ4-incompressible) value at the inline threshold.
        let mut value = Vec::with_capacity(INLINE_MAX_VALUE_BYTES);
        let mut s: u32 = 0x1234_5678;
        for _ in 0..INLINE_MAX_VALUE_BYTES {
            s = s.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            value.push((s >> 24) as u8);
        }
        // Sizable key so key + value overflows a 4KB leaf (forces batch failure),
        // yet the key alone fits a leaf with an overflow pointer (pages succeed).
        let key = vec![b'k'; 800];

        let mut buffer = vec![SpillRequest {
            key: Bytes::from(key),
            db_index: 0,
            value_bytes: Bytes::from(value),
            value_type: ValueType::String,
            flags: 0,
            ttl_ms: None,
            file_id: 1,
            shard_dir: tmp.path().to_path_buf(),
        }];

        let completions = flush_buffer(&mut buffer);
        let succeeded: usize = completions
            .iter()
            .filter(|c| c.success)
            .map(|c| c.entries.len())
            .sum();
        assert_eq!(
            succeeded, 1,
            "inline entry that overflows a leaf must be salvaged via per-entry fallback, not dropped"
        );

        // And the salvaged entry must be readable back from its on-disk file.
        let c = completions
            .iter()
            .find(|c| c.success && !c.entries.is_empty())
            .expect("expected a successful fallback completion");
        let file_path = tmp
            .path()
            .join("data")
            .join(format!("heap-{:06}.mpf", c.file_entry.file_id));
        assert!(
            file_path.exists(),
            "fallback spill file should exist on disk"
        );
    }

    #[test]
    fn test_spill_request_with_ttl() {
        let tmp = tempfile::tempdir().unwrap();
        let st = SpillThread::new(2);
        let sender = st.sender();

        let future_ms = current_time_ms() + 60_000;
        let req = SpillRequest {
            key: Bytes::from_static(b"ttl_key"),
            db_index: 0,
            value_bytes: Bytes::from_static(b"expiring_val"),
            value_type: ValueType::String,
            flags: entry_flags::HAS_TTL,
            ttl_ms: Some(future_ms),
            file_id: 2,
            shard_dir: tmp.path().to_path_buf(),
        };
        sender.send(req).unwrap();
        drop(sender);

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let completions = collect_entries(&st, 1, deadline);

        let total: usize = completions.iter().map(|c| c.entries.len()).sum();
        assert_eq!(total, 1);

        let c = completions.iter().find(|c| !c.entries.is_empty()).unwrap();
        assert!(c.success);
        assert_eq!(c.file_entry.file_type, PageType::KvLeaf as u8);

        let _ = st.shutdown();
    }

    #[test]
    fn test_spill_thread_shutdown() {
        let st = SpillThread::new(3);
        let sender = st.sender();
        drop(sender);
        let _ = st.shutdown();
        // Reaching here without hang = clean exit.
    }

    /// 5 requests sent together must all appear as entries across completions.
    #[test]
    fn test_multiple_requests_all_entries_received() {
        let tmp = tempfile::tempdir().unwrap();
        let st = SpillThread::new(4);
        let sender = st.sender();

        for i in 0..5u64 {
            let req = SpillRequest {
                key: Bytes::from(format!("key_{i}")),
                db_index: 0,
                value_bytes: Bytes::from(format!("val_{i}")),
                value_type: ValueType::String,
                flags: 0,
                ttl_ms: None,
                file_id: i + 1,
                shard_dir: tmp.path().to_path_buf(),
            };
            sender.send(req).unwrap();
        }
        drop(sender);

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let completions = collect_entries(&st, 5, deadline);

        let total_entries: usize = completions.iter().map(|c| c.entries.len()).sum();
        assert_eq!(total_entries, 5, "all 5 entries must be accounted for");

        // Each entry must be readable on disk via its location.
        for c in &completions {
            if !c.success {
                continue;
            }
            for entry in &c.entries {
                let loc = crate::storage::tiered::cold_index::ColdLocation {
                    file_id: c.file_entry.file_id,
                    page_idx: entry.page_idx,
                    slot_idx: entry.slot_idx,
                };
                let result =
                    crate::storage::tiered::cold_read::read_cold_entry_at(tmp.path(), loc, 0);
                assert!(
                    result.is_some(),
                    "entry key={} should be readable",
                    String::from_utf8_lossy(&entry.key)
                );
            }
        }

        let _ = st.shutdown();
    }

    /// Full pipeline: 5 requests, verify round-trip via cold_read.
    #[test]
    fn test_full_pipeline_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let st = SpillThread::new(10);
        let sender = st.sender();

        for i in 0..5u64 {
            let req = SpillRequest {
                key: Bytes::from(format!("pipeline_key_{i}")),
                db_index: 0,
                value_bytes: Bytes::from(format!("pipeline_value_{i}_with_some_data")),
                value_type: ValueType::String,
                flags: 0,
                ttl_ms: None,
                file_id: 100 + i,
                shard_dir: tmp.path().to_path_buf(),
            };
            sender.send(req).unwrap();
        }
        drop(sender);

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let completions = collect_entries(&st, 5, deadline);

        let total: usize = completions.iter().map(|c| c.entries.len()).sum();
        assert_eq!(total, 5, "expected 5 entries across completions");

        for c in &completions {
            assert!(c.success);
        }

        let _ = st.shutdown();
    }

    #[test]
    fn test_channel_backpressure() {
        let tmp = tempfile::tempdir().unwrap();
        let st = SpillThread::new(11);
        let sender = st.sender();

        let mut sent = 0;
        for i in 0..128u64 {
            let req = SpillRequest {
                key: Bytes::from(format!("bp_key_{i}")),
                db_index: 0,
                value_bytes: Bytes::from(format!("bp_val_{i}")),
                value_type: ValueType::String,
                flags: 0,
                ttl_ms: None,
                file_id: 200 + i,
                shard_dir: tmp.path().to_path_buf(),
            };
            match sender.try_send(req) {
                Ok(()) => sent += 1,
                Err(flume::TrySendError::Full(_)) => break,
                Err(flume::TrySendError::Disconnected(_)) => {
                    panic!("channel disconnected unexpectedly");
                }
            }
        }
        assert!(sent >= 1, "should have sent at least 1 request");

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let completions = collect_entries(&st, sent, deadline);
        let received: usize = completions.iter().map(|c| c.entries.len()).sum();
        assert_eq!(received, sent, "should receive all sent entries");

        drop(sender);
        let _ = st.shutdown();
    }

    #[test]
    fn test_shutdown_with_pending_work() {
        let tmp = tempfile::tempdir().unwrap();
        let st = SpillThread::new(13);
        let sender = st.sender();

        for i in 0..3u64 {
            let req = SpillRequest {
                key: Bytes::from(format!("shutdown_key_{i}")),
                db_index: 0,
                value_bytes: Bytes::from(format!("shutdown_val_{i}")),
                value_type: ValueType::String,
                flags: 0,
                ttl_ms: None,
                file_id: 300 + i,
                shard_dir: tmp.path().to_path_buf(),
            };
            sender.send(req).unwrap();
        }
        drop(sender);

        let start = std::time::Instant::now();
        let _ = st.shutdown();
        let elapsed = start.elapsed();

        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "shutdown took too long: {:?}",
            elapsed
        );
    }
}
