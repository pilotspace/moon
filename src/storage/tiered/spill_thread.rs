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
//!
//! ## Batching is size-independent
//!
//! `flush_buffer` hands the WHOLE buffered batch to
//! `kv_spill::build_kv_spill_batch` regardless of individual entry size —
//! oversized (overflow-chained) entries get a dedicated leaf + overflow chain
//! INSIDE the shared file, not a dedicated file of their own (see that
//! function's doc). Before this, entries whose `value_bytes` exceeded
//! `INLINE_MAX_VALUE_BYTES` were pre-screened out of the batch and each
//! written to its own single-entry file — correct, but for a workload whose
//! values are consistently above that threshold (e.g. 10KB), it silently
//! degenerated `flush_buffer`'s otherwise-correct `FLUSH_ENTRY_CAP`-sized
//! grouping into ~1 file per evicted key (measured: a 10KB-value repro
//! produced ~1 file per completed spill, `spill_batches_flushed` staying in
//! the tens while `heap-*.mpf` count tracked the key count 1:1 — the v0.8
//! "spill-file batching" fix).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Maximum entries to buffer before forcing a flush.
///
/// This bound alone no longer caps in-RAM batch size (stale pre-v0.8 "batching"
/// claim — see `BATCH_BYTES_CAP` below): since the spill-file-batching fix,
/// oversized (overflow-chained) entries share a batch with everything else in
/// the flush instead of each getting their own dedicated file, so 256
/// consistently-large values (e.g. near-`maxmemory`-sized strings) could
/// still mean 256 * value_size resident in `build_kv_spill_batch`'s output
/// before a single byte reaches disk. `FLUSH_ENTRY_CAP` still bounds manifest
/// growth (#files ~ #keys/256, not #keys) and per-flush fsync count; it is
/// `BATCH_BYTES_CAP` that now bounds peak batch-build RAM.
const FLUSH_ENTRY_CAP: usize = 256;

/// Bytes-based sub-batch cap (review FIX 2): `build_kv_spill_batch`
/// materializes an entire sub-batch's physical pages in one `Vec<BatchSlot>`
/// before `write_kv_spill_batch` streams them to disk — spills fire under
/// memory pressure, so this is exactly the wrong place for an unbounded
/// working set. `flush_buffer` below splits a `FLUSH_ENTRY_CAP`-sized buffer
/// into sub-batches capped at this many cumulative `value_bytes`, each
/// becoming its own file (reusing that sub-batch's own first request's
/// pre-assigned `file_id` — see `SpillRequest::file_id`'s doc on why gaps in
/// the file_id space are harmless).
///
/// 4 MiB chosen against the OLD per-entry path's peak: before batching,
/// exactly one oversized entry's pages were ever materialized at a time (a
/// leaf + its own overflow chain, bounded by that single value's size, no
/// amplification across a flush). The new unconditional-batching path's worst
/// case is `FLUSH_ENTRY_CAP * max_value_size` — unbounded in principle. 4 MiB
/// keeps ordinary flushes (the G2 acceptance shape: 256 entries x ~10 KB =
/// ~2.5 MiB) as ONE file — no behavior change for the workload that
/// motivated this fix — while bounding the pathological case (256 large
/// values in one buffer) to ~4 MiB resident per sub-batch instead of
/// hundreds of MB, restoring a bound of the same order as the old per-entry
/// path (one value's worth of pages) rather than a multiplicative one.
const BATCH_BYTES_CAP: usize = 4 * 1024 * 1024;

/// Request-channel depth (see `SpillThread::new`). Also referenced by the
/// pacing cutoff below so "deep backlog" stays defined relative to capacity.
const REQUEST_QUEUE_CAP: usize = 4096;

/// Task #59 lever 2 — read/write device fairness, the pacing decision.
///
/// After each durable batch flush (a multi-MB pwrite + fsync), the spill
/// thread asks whether to briefly YIELD the device before building the next
/// batch. Under a sustained spill flood the device queue is otherwise
/// permanently deep with writer traffic, and a cold reader's pread waits
/// behind all of it (measured p99 208ms for a raw 64KB O_DIRECT read during
/// the G2-shape flood, vs 6ms idle).
///
/// Rules, in priority order:
/// - Nobody is reading → never pause (zero throughput cost when idle).
/// - Request backlog ≥ half capacity → never pause: a paced spill thread
///   must not push eviction into `try_send` failure, which surfaces as -OOM
///   to write clients. Full-speed drain wins over read latency there.
/// - Otherwise → one small fixed quantum. 4ms per flush caps the throughput
///   tax at ~a few percent of a typical flush's own write+fsync time while
///   giving a queued pread a window in which the device queue is empty.
///
/// Pure function of its inputs for unit-testability.
pub(crate) fn spill_pace_after_flush(
    reads_inflight: usize,
    backlog_len: usize,
) -> std::time::Duration {
    const PACE_QUANTUM_MS: u64 = 4;
    const BACKLOG_CUTOFF: usize = REQUEST_QUEUE_CAP / 2;
    if reads_inflight == 0 || backlog_len >= BACKLOG_CUTOFF {
        std::time::Duration::ZERO
    } else {
        std::time::Duration::from_millis(PACE_QUANTUM_MS)
    }
}

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

/// Liveness heartbeat: unix millis of the most recent spill-thread loop
/// iteration (across all shards). 0 = no spill thread has ever run. A stale
/// value under active eviction means the spill thread died silently — the
/// only other symptom is an unbounded eviction backlog.
static SPILL_LAST_HEARTBEAT_MS: AtomicU64 = AtomicU64::new(0);

/// Cumulative number of spill batches flushed to disk across all shards.
static SPILL_BATCHES_FLUSHED: AtomicU64 = AtomicU64::new(0);

/// Unix millis of the most recent spill-thread loop tick (0 = never ran).
#[inline]
pub fn spill_last_heartbeat_ms() -> u64 {
    SPILL_LAST_HEARTBEAT_MS.load(Ordering::Relaxed)
}

/// Cumulative spill batches flushed to disk. Exposed for INFO / metrics.
#[inline]
pub fn spill_batches_flushed_total() -> u64 {
    SPILL_BATCHES_FLUSHED.load(Ordering::Relaxed)
}

/// Cumulative keys re-inserted into the hot table after a FAILED spill write
/// (deep-review F1). Every increment means a pwrite failed (ENOSPC/EIO) after
/// the key was already evicted from RAM; without the re-insert the key would
/// read as nil until an AOF-replay restart. Exposed as
/// `spill_failed_reinserted` in INFO — a nonzero value is an operator signal
/// that the spill volume is failing writes.
static SPILL_FAILED_REINSERTED: AtomicU64 = AtomicU64::new(0);

/// Cumulative failed-spill hot re-inserts. Exposed for INFO / metrics.
#[inline]
pub fn spill_failed_reinserted_total() -> u64 {
    SPILL_FAILED_REINSERTED.load(Ordering::Relaxed)
}

/// Record one failed-spill hot re-insert.
#[inline]
pub fn record_spill_failed_reinserted() {
    SPILL_FAILED_REINSERTED.fetch_add(1, Ordering::Relaxed);
}

/// Spill completions that landed on disk but were NOT published to the cold
/// index because their in-flight record had been retired — the key was
/// deleted, overwritten, or read-promoted while the spill was in flight
/// (#459). Publishing those would resurrect a deleted key.
///
/// Not an error: it is the normal, correct outcome of a write racing a
/// spill. It is counted because the spilled bytes become orphaned in their
/// file, and a rate that tracks the eviction rate means the workload is
/// re-touching keys as fast as they are being spilled — the signal that
/// `maxmemory` is too small for the working set. Exposed as
/// `spill_completion_superseded` in INFO.
static SPILL_COMPLETION_SUPERSEDED: AtomicU64 = AtomicU64::new(0);

/// Cumulative superseded (unpublished) spill completions. For INFO / metrics.
#[inline]
pub fn spill_completion_superseded_total() -> u64 {
    SPILL_COMPLETION_SUPERSEDED.load(Ordering::Relaxed)
}

/// Record one superseded spill completion.
#[inline]
pub fn record_spill_completion_superseded() {
    SPILL_COMPLETION_SUPERSEDED.fetch_add(1, Ordering::Relaxed);
}

/// Spill completions whose file id the shard manifest ALREADY listed, so
/// `ShardManifest::add_file` refused them (moon#893). The keys went back to
/// the hot table from their in-flight payloads; nothing was published cold.
/// Unreachable while the file_id seed holds, so any nonzero value means an id
/// was re-issued. Exposed as `spill_completion_id_rejected` in INFO.
static SPILL_COMPLETION_ID_REJECTED: AtomicU64 = AtomicU64::new(0);

/// Cumulative spill completions refused for an already-listed file id.
#[inline]
pub fn spill_completion_id_rejected_total() -> u64 {
    SPILL_COMPLETION_ID_REJECTED.load(Ordering::Relaxed)
}

/// Record one spill completion refused for an already-listed file id.
#[inline]
pub fn record_spill_completion_id_rejected() {
    SPILL_COMPLETION_ID_REJECTED.fetch_add(1, Ordering::Relaxed);
}

/// Spill completions withdrawn because the AOF writer would not take their
/// `MOON.SPILLED` cut record within the backpressure bound (moon#1202). The
/// keys went back to the hot table from their in-flight payloads and the file
/// was left out of the manifest, so the next eviction pass spills them again.
/// A nonzero value means the AOF writer is saturated. Exposed as
/// `spill_completion_marker_withdrawn` in INFO.
static SPILL_COMPLETION_MARKER_WITHDRAWN: AtomicU64 = AtomicU64::new(0);

/// Cumulative spill completions withdrawn for an unloggable marker.
#[inline]
pub fn spill_completion_marker_withdrawn_total() -> u64 {
    SPILL_COMPLETION_MARKER_WITHDRAWN.load(Ordering::Relaxed)
}

/// Record one spill completion withdrawn for an unloggable marker.
#[inline]
pub fn record_spill_completion_marker_withdrawn() {
    SPILL_COMPLETION_MARKER_WITHDRAWN.fetch_add(1, Ordering::Relaxed);
}

use bytes::Bytes;
use tracing::warn;

use crate::persistence::kv_page::ValueType;
use crate::persistence::manifest::{FileEntry, FileStatus, StorageTier};
use crate::persistence::page::PageType;
use crate::storage::tiered::kv_spill::{
    SpillEntry, build_kv_spill_batch, build_kv_spill_pages, write_kv_spill_batch,
    write_kv_spill_pages,
};

/// Request sent from event loop to background spill thread.
///
/// Contains all data needed for pwrite -- no references to shard state.
/// `Bytes` fields are reference-counted (cheap clone on event loop side).
/// `Clone` exists so a FAILED write can carry the request back to the event
/// loop for hot re-insert (deep-review F1) — cheap: both payload fields are
/// refcounted `Bytes`.
#[derive(Clone)]
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
    /// Absolute TTL in milliseconds, carried through from the originating
    /// `SpillRequest` so the event loop can populate
    /// `ColdLocation::ttl_ms` (R1, H-2: proactive TTL-expiry sweep) without
    /// re-reading the just-written file.
    pub ttl_ms: Option<u64>,
    /// Value type, likewise carried through from the `SpillRequest` so the
    /// event loop can populate `ColdLocation::value_type` (#364: SCAN TYPE
    /// filter over cold keys) without re-reading the just-written file.
    pub value_type: ValueType,
    /// The originating request's `file_id` (each request gets a unique
    /// monotonic id even when batching flushes many requests into one
    /// file). Lets the completion handler retire the per-key in-flight
    /// spill record for exactly this request (stale-shadow guard).
    pub req_file_id: u64,
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
    /// On failure: the original request, so the event loop can re-insert the
    /// already-evicted key into the hot table (deep-review F1 — without this
    /// the key exists in neither plane and reads nil until restart).
    /// Always `None` on success.
    pub failed_request: Option<Box<SpillRequest>>,
}

/// Build a `FileEntry` skeleton for a spill file (fields not tracked by Moon are zero).
fn make_file_entry(file_id: u64, page_count: u32, byte_size: u64, db_index: usize) -> FileEntry {
    FileEntry {
        file_id,
        file_type: PageType::KvLeaf as u8,
        status: FileStatus::Active,
        tier: StorageTier::Hot,
        page_size_log2: 12, // 4KB = 2^12
        page_count,
        byte_size,
        created_lsn: 0,
        db_index: db_index as u64,
        max_key_hash: 0,
        last_modified_lsn: 0,
    }
}

/// Flush the buffered requests as ONE shared multi-page `.mpf` file, holding
/// every entry regardless of value size — `build_kv_spill_batch` gives
/// oversized entries a dedicated leaf + overflow chain INSIDE the batch file
/// instead of a dedicated file of their own (see that function's doc for why
/// this is what makes batching effective for large-value workloads).
///
/// This keeps manifest entries == #files (not #keys), removing the
/// ~70-entry cap — and, since v0.8, keeps it that way independent of value
/// size: a workload of consistently-oversized values used to defeat the
/// batching entirely (each such entry fell through to its own file, so
/// `FLUSH_ENTRY_CAP`-sized flushes still produced ~1 file per key).
///
/// On a build or write failure for the whole batch (defensive — e.g. a
/// pathologically large key that doesn't fit a leaf even with a 4-byte
/// overflow pointer), every entry in the buffer is salvaged individually via
/// [`spill_single_entry`] rather than dropped — the keys are already evicted
/// from RAM by the time this runs, so losing the flush would be silent data
/// loss.
///
/// Returns a `Vec<SpillCompletion>` (one per file written — one for the
/// common case, more when `BATCH_BYTES_CAP` splits an oversized-heavy flush
/// into several sub-batch files, or one per salvaged entry on the defensive
/// fallback). Never panics.
///
/// `pub(crate)`: also called synchronously (no channel, no background
/// thread) by `crate::storage::eviction`'s no-AOF-backstop durable spill path
/// (`--appendonly no`), which needs this exact batching to keep per-eviction
/// I/O at ~1 fsync per up-to-`FLUSH_ENTRY_CAP` keys instead of 1 fsync per
/// key -- reusing this function (rather than re-deriving the routing logic)
/// keeps the two paths' on-disk layout identical.
pub(crate) fn flush_buffer(buffer: &mut Vec<SpillRequest>) -> Vec<SpillCompletion> {
    if buffer.is_empty() {
        return Vec::new();
    }
    SPILL_BATCHES_FLUSHED.fetch_add(1, Ordering::Relaxed);

    let mut completions: Vec<SpillCompletion> = Vec::new();
    let shard_dir = buffer[0].shard_dir.clone();

    // Split the flush into `BATCH_BYTES_CAP`-bounded sub-batches (review
    // FIX 2) so `build_kv_spill_batch` never materializes more than ~one
    // cap's worth of pages in RAM at once, regardless of how many oversized
    // entries landed in this `FLUSH_ENTRY_CAP`-sized buffer. Each sub-batch
    // is a range `[start, end)`; a sub-batch always contains at least one
    // entry (a single entry larger than the cap gets its own sub-batch
    // rather than stalling progress).
    let mut start = 0usize;
    while start < buffer.len() {
        let mut end = start + 1;
        let mut chunk_bytes = buffer[start].value_bytes.len();
        // Chunks additionally never cross a logical-DB boundary: the
        // manifest attributes a whole file to ONE `db_index` (#139 — cold
        // recovery re-attaches each file to the database its keys were
        // evicted from), so a flush holding requests from several DBs
        // produces one file per contiguous same-DB run. Interleaved
        // multi-DB eviction costs extra files, never correctness.
        let chunk_db = buffer[start].db_index;
        while end < buffer.len() {
            let next_len = buffer[end].value_bytes.len();
            if chunk_bytes + next_len > BATCH_BYTES_CAP || buffer[end].db_index != chunk_db {
                break;
            }
            chunk_bytes += next_len;
            end += 1;
        }
        let chunk = &buffer[start..end];

        // Use the file_id of this sub-batch's own first request (see
        // `SpillRequest::file_id` doc — every request already carries its
        // own pre-assigned id; gaps left by unused ids elsewhere in the
        // flush are harmless sparse gaps).
        let file_id = chunk[0].file_id;

        let spill_entries: Vec<SpillEntry> = chunk
            .iter()
            .map(|req| SpillEntry {
                key: req.key.clone(),
                value_bytes: req.value_bytes.clone(),
                value_type: req.value_type,
                flags: req.flags,
                ttl_ms: req.ttl_ms,
            })
            .collect();

        match build_kv_spill_batch(&spill_entries, file_id) {
            Ok(batch) => {
                let total_pages = batch.pages.len() as u32;
                match write_kv_spill_batch(&shard_dir, file_id, &batch) {
                    Ok(byte_size) => {
                        let entries = chunk
                            .iter()
                            .zip(batch.locations.iter())
                            .map(|(req, &(page_idx, slot_idx))| SpillCompletionEntry {
                                key: req.key.clone(),
                                db_index: req.db_index,
                                page_idx,
                                slot_idx,
                                ttl_ms: req.ttl_ms,
                                value_type: req.value_type,
                                req_file_id: req.file_id,
                            })
                            .collect();
                        completions.push(SpillCompletion {
                            file_entry: make_file_entry(file_id, total_pages, byte_size, chunk_db),
                            entries,
                            success: true,
                            failed_request: None,
                        });
                    }
                    Err(e) => {
                        warn!(
                            file_id,
                            error = %e,
                            count = chunk.len(),
                            "spill_thread: batch write failed; falling back to per-entry spill"
                        );
                        for req in chunk.iter() {
                            completions.push(spill_single_entry(req, req.file_id));
                        }
                    }
                }
            }
            Err(e) => {
                warn!(
                    file_id,
                    error = %e,
                    count = chunk.len(),
                    "spill_thread: batch build failed; falling back to per-entry spill"
                );
                for req in chunk.iter() {
                    completions.push(spill_single_entry(req, req.file_id));
                }
            }
        }

        start = end;
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
                file_entry: make_file_entry(file_id, pages.total_pages, byte_size, req.db_index),
                entries: vec![SpillCompletionEntry {
                    key: req.key.clone(),
                    db_index: req.db_index,
                    page_idx: 0,
                    slot_idx: 0,
                    ttl_ms: req.ttl_ms,
                    value_type: req.value_type,
                    req_file_id: req.file_id,
                }],
                success: true,
                failed_request: None,
            },
            Err(e) => {
                warn!(
                    file_id,
                    error = %e,
                    key_len = req.key.len(),
                    "spill_thread: single-file write failed"
                );
                SpillCompletion {
                    file_entry: make_file_entry(file_id, 0, 0, req.db_index),
                    entries: Vec::new(),
                    success: false,
                    failed_request: Some(Box::new(req.clone())),
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
                file_entry: make_file_entry(file_id, 0, 0, req.db_index),
                entries: Vec::new(),
                success: false,
                failed_request: Some(Box::new(req.clone())),
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
        let (request_tx, request_rx) = flume::bounded::<SpillRequest>(REQUEST_QUEUE_CAP);
        let (completion_tx, completion_rx) = flume::bounded::<SpillCompletion>(8192);
        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_flag_bg = stop_flag.clone();

        #[allow(clippy::expect_used)]
        // Startup: spill thread is critical infrastructure — spawn failure is fatal
        let join_handle = std::thread::Builder::new()
            .name(format!("spill-{shard_id}"))
            .spawn(move || {
                // O5: this thread is spawned from the shard's own (pinned)
                // event-loop thread and would otherwise inherit its exact
                // single-core mask — re-pin to the non-shard core set as the
                // first act, before anything else runs.
                crate::shard::numa::pin_current_aux_thread(&format!("spill-{shard_id}"));
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

        // Task #59 lever 2: after a durable batch flush, briefly yield the
        // device to any waiting cold reader. Never paces at shutdown or when
        // the request backlog is deep (see `spill_pace_after_flush`).
        let pace = |request_rx: &flume::Receiver<SpillRequest>| {
            let pause = spill_pace_after_flush(
                super::cold_read_pool::COLD_READS_INFLIGHT.load(Ordering::Relaxed),
                request_rx.len(),
            );
            if !pause.is_zero() {
                std::thread::sleep(pause);
            }
        };

        loop {
            // Liveness heartbeat (R5): stamped every loop tick so INFO can
            // surface a silently-dead spill thread. Relaxed max-store is
            // enough — monotonicity across shards is not required.
            SPILL_LAST_HEARTBEAT_MS
                .fetch_max(crate::storage::entry::current_time_ms(), Ordering::Relaxed);

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
                        pace(&request_rx);
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
                        pace(&request_rx);
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
mod tests;
