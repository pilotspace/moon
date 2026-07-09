//! Off-loop cold-segment reload pool (prod-hardening #18).
//!
//! When an idle vector segment has been demoted to the COLD tier
//! (`UnloadedSegment` stub) under `--disk-offload`, the first FT.SEARCH that
//! touches it must reload it (`WarmSearchSegment::from_files`: mmap + page-in +
//! optional mlock of a megabytes-to-gigabytes segment). Doing that inline on
//! the single-threaded shard event loop stalls EVERY other connection on the
//! shard for the whole reload (hundreds of ms–seconds on real NVMe/EBS with a
//! cold page cache) — a multi-tenancy violation.
//!
//! This pool moves the reload I/O onto dedicated worker threads. The shard
//! thread `submit`s an `Arc<UnloadedSegment>` (non-blocking) and gets back a
//! `flume::Receiver`; the awaiting **async** connection task
//! `recv_async().await`s it, which parks the *task*, never the OS thread, so
//! sibling connections keep running (the same cross-thread-reply idiom the
//! graph-tier `search_pool` and the cross-shard coordinator already rely on —
//! flume's receiver is polled from *inside* the task, sidestepping the monoio
//! `!Send`-waker gotcha).
//!
//! Guarantees:
//! - **Single-flight:** N concurrent first-touches of the same segment share
//!   ONE reload job (deduped by `segment_id`); every waiter gets the result.
//! - **Completed cache:** a finished reload is held until the next capture on
//!   that index installs it into the holder's WARM tier (see
//!   `SegmentHolder::submit_unloaded_reloads`), so the segment is materialized
//!   exactly once for future queries and the stub's memory is reclaimed.
//! - **Design-for-failure:** a panicking or erroring reload replies `Err` to
//!   every waiter (never a hang); a disabled/uninitialized pool makes callers
//!   fall back to the synchronous blocking reload — identical to pre-#18.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::vector::persistence::unloaded_segment::UnloadedSegment;
use crate::vector::persistence::warm_search::WarmSearchSegment;

/// Result delivered to every waiter for one reload job. `Arc<io::Error>` keeps
/// the outcome `Clone` (io::Error is not) so a single reload can answer many
/// deduped waiters.
pub type ReloadOutcome = Result<Arc<WarmSearchSegment>, Arc<std::io::Error>>;

/// One in-flight reload: the stub to reload plus every waiter's reply sender.
struct InFlight {
    stub: Arc<UnloadedSegment>,
    waiters: Vec<flume::Sender<ReloadOutcome>>,
}

#[derive(Default)]
struct ReloadState {
    /// Segments currently being reloaded, keyed by `segment_id` (single-flight).
    in_flight: HashMap<u64, InFlight>,
    /// Finished reloads awaiting install into the holder's WARM tier.
    completed: HashMap<u64, Arc<WarmSearchSegment>>,
}

/// Worker-thread pool for cold-segment reloads. Create once per process
/// (see [`init_global`]); dropping it disconnects the channel and workers exit.
pub struct SegmentReloadPool {
    job_tx: flume::Sender<u64>,
    state: Arc<Mutex<ReloadState>>,
    num_workers: usize,
    _workers: Vec<std::thread::JoinHandle<()>>,
}

impl SegmentReloadPool {
    /// Spawn `num_workers` (≥1) reload threads.
    pub fn new(num_workers: usize) -> Self {
        assert!(num_workers >= 1, "at least one worker required");
        // Unbounded: job volume is (distinct cold segments touched), naturally
        // bounded by index count; the shard thread must NEVER block on submit.
        let (job_tx, job_rx) = flume::unbounded::<u64>();
        let state = Arc::new(Mutex::new(ReloadState::default()));
        let workers: Vec<_> = (0..num_workers)
            .map(|i| {
                let rx = job_rx.clone();
                let st = Arc::clone(&state);
                std::thread::Builder::new()
                    .name(format!("moon-vec-reload-{i}"))
                    .spawn(move || worker_loop(&rx, &st))
                    .expect("failed to spawn vector reload worker thread")
            })
            .collect();
        Self {
            job_tx,
            state,
            num_workers,
            _workers: workers,
        }
    }

    /// Submit `stub` for reload and return a receiver for its outcome.
    ///
    /// Non-blocking. If the segment is already reloaded (completed cache) the
    /// receiver resolves immediately; if a reload is already in flight this
    /// waiter joins it (single-flight); otherwise a new job is enqueued.
    pub fn submit(&self, stub: Arc<UnloadedSegment>) -> flume::Receiver<ReloadOutcome> {
        let sid = stub.segment_id();
        let (wtx, wrx) = flume::bounded(1);
        let mut enqueue = false;
        {
            let mut st = self.state.lock();
            if let Some(seg) = st.completed.get(&sid) {
                // Already reloaded — answer immediately, keep it in `completed`
                // so the next capture still installs it into the WARM tier.
                let _ = wtx.send(Ok(Arc::clone(seg)));
                return wrx;
            }
            match st.in_flight.entry(sid) {
                Entry::Occupied(mut e) => e.get_mut().waiters.push(wtx),
                Entry::Vacant(e) => {
                    e.insert(InFlight {
                        stub,
                        waiters: vec![wtx],
                    });
                    enqueue = true;
                }
            }
        }
        if enqueue {
            // Channel is unbounded; send only fails if the pool is shutting down,
            // in which case the waiter's sender is dropped → RecvError, not a hang.
            let _ = self.job_tx.send(sid);
        }
        wrx
    }

    /// Take a finished reload for `segment_id`, if any, removing it from the
    /// completed cache. Called by the holder to install the reloaded segment
    /// into its WARM tier for future queries.
    pub fn take_completed(&self, segment_id: u64) -> Option<Arc<WarmSearchSegment>> {
        self.state.lock().completed.remove(&segment_id)
    }

    pub fn num_workers(&self) -> usize {
        self.num_workers
    }
}

fn worker_loop(rx: &flume::Receiver<u64>, state: &Arc<Mutex<ReloadState>>) {
    while let Ok(sid) = rx.recv() {
        // Clone the stub out from under the lock so the (slow) reload I/O runs
        // lock-free — concurrent submits for OTHER segments never block on it.
        let stub = {
            let st = state.lock();
            st.in_flight.get(&sid).map(|f| Arc::clone(&f.stub))
        };
        let Some(stub) = stub else {
            continue; // Raced with a shutdown / already drained.
        };

        // Contain a panicking reload to this job (keep the worker alive).
        let outcome: ReloadOutcome =
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| stub.reload())) {
                Ok(Ok(seg)) => Ok(Arc::new(seg)),
                Ok(Err(e)) => Err(Arc::new(e)),
                Err(_) => Err(Arc::new(std::io::Error::other(
                    "cold segment reload panicked",
                ))),
            };

        let waiters = {
            let mut st = state.lock();
            if let Ok(ref seg) = outcome {
                // Hold the reloaded segment for the next capture to install.
                st.completed.insert(sid, Arc::clone(seg));
            }
            st.in_flight
                .remove(&sid)
                .map(|f| f.waiters)
                .unwrap_or_default()
        };
        for w in waiters {
            // Receiver may be gone (client disconnected mid-reload) — fine.
            let _ = w.send(outcome.clone());
        }
    }
    // job_tx dropped → channel disconnected → exit cleanly.
}

// ── Process-global pool ─────────────────────────────────────────────────────

/// `None` inside = explicitly disabled. Uninitialized = disabled: callers then
/// fall back to the synchronous blocking reload (pre-#18 behavior), and unit
/// tests see that exact path unless they opt in.
static GLOBAL_POOL: std::sync::OnceLock<Option<SegmentReloadPool>> = std::sync::OnceLock::new();

/// Initialize the process-global reload pool. `num_workers == 0` disables it
/// (callers fall back to blocking reload). First call wins (idempotent).
pub fn init_global(num_workers: usize) {
    let _ = GLOBAL_POOL.set(if num_workers == 0 {
        None
    } else {
        Some(SegmentReloadPool::new(num_workers))
    });
}

/// The process-global pool, or `None` when disabled/uninitialized.
pub fn global() -> Option<&'static SegmentReloadPool> {
    GLOBAL_POOL.get().and_then(|p| p.as_ref())
}

/// Per-shard WARM-tier resident-byte cap (`--vec-warm-mmap-budget`), mirrored
/// here so the reload/install path can bound WARM growth immediately instead of
/// waiting for the 10s `MmapBudget` tick. `0` = disabled (no install-time gate).
/// Set once at startup; the same per-shard value applies uniformly to every
/// shard, so a single global atomic suffices.
static WARM_BUDGET_PER_SHARD_BYTES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Publish the per-shard WARM budget (bytes) for the reload-install gate.
pub fn set_warm_budget_per_shard(bytes: u64) {
    WARM_BUDGET_PER_SHARD_BYTES.store(bytes, std::sync::atomic::Ordering::Relaxed);
}

/// Current per-shard WARM budget (bytes); `0` disables the install-time gate.
pub fn warm_budget_per_shard() -> u64 {
    WARM_BUDGET_PER_SHARD_BYTES.load(std::sync::atomic::Ordering::Relaxed)
}

/// Resolve the default reload-worker count.
///
/// `MOON_VEC_RELOAD_WORKERS=<n>` overrides (0 = disable, falling back to the
/// blocking reload). Otherwise a small default (2 on multicore, 1 on ≤2-core
/// boxes) — reloads are rare and single-flight, so a couple of workers cover
/// concurrent cold touches without competing with shard threads. Never fewer
/// than 1 unless explicitly disabled.
pub fn default_workers(cores: usize) -> usize {
    if let Ok(v) = std::env::var("MOON_VEC_RELOAD_WORKERS") {
        if let Ok(n) = v.trim().parse::<usize>() {
            return n;
        }
    }
    if cores <= 2 { 1 } else { 2 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::tiered::SegmentHandle;
    use crate::vector::distance;
    use crate::vector::hnsw::graph::HnswGraph;
    use crate::vector::persistence::warm_segment::{
        write_codes_mpf, write_graph_mpf, write_mvcc_mpf,
    };
    use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
    use crate::vector::types::DistanceMetric;
    use std::path::Path;

    fn make_collection() -> Arc<CollectionMetadata> {
        Arc::new(CollectionMetadata::new(
            1,
            128,
            DistanceMetric::L2,
            QuantizationConfig::TurboQuant4,
            42,
        ))
    }

    fn write_minimal_segment(seg_dir: &Path, seg_id: u64) {
        std::fs::create_dir_all(seg_dir).unwrap();
        let empty_graph = HnswGraph::new(
            0,
            16,
            32,
            0,
            0,
            crate::vector::aligned_buffer::AlignedBuffer::new(0),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            68,
        );
        let graph_bytes = empty_graph.to_bytes();
        write_codes_mpf(&seg_dir.join("codes.mpf"), seg_id, &[]).unwrap();
        write_graph_mpf(&seg_dir.join("graph.mpf"), seg_id, &graph_bytes).unwrap();
        write_mvcc_mpf(&seg_dir.join("mvcc.mpf"), seg_id, &[]).unwrap();
    }

    /// Build a real `UnloadedSegment` stub backed by on-disk `.mpf` files.
    fn make_stub(tmp: &Path, seg_id: u64) -> Arc<UnloadedSegment> {
        let seg_dir = tmp.join(format!("seg-{seg_id}"));
        write_minimal_segment(&seg_dir, seg_id);
        let handle = SegmentHandle::new(seg_id, seg_dir.clone());
        let warm =
            WarmSearchSegment::from_files(&seg_dir, seg_id, make_collection(), handle, false)
                .unwrap();
        Arc::new(UnloadedSegment::from_warm(&warm, false))
    }

    #[test]
    fn test_submit_reloads_off_thread_and_delivers() {
        distance::init();
        let tmp = tempfile::tempdir().unwrap();
        let stub = make_stub(tmp.path(), 1);
        let pool = SegmentReloadPool::new(1);

        let rx = pool.submit(Arc::clone(&stub));
        let outcome = rx.recv().expect("worker replies");
        let seg = outcome.expect("reload succeeds");
        assert_eq!(seg.segment_id(), 1);

        // Completed cache holds it for install until taken exactly once.
        assert!(pool.take_completed(1).is_some());
        assert!(pool.take_completed(1).is_none());
    }

    #[test]
    fn test_single_flight_dedupes_concurrent_submits() {
        distance::init();
        let tmp = tempfile::tempdir().unwrap();
        let stub = make_stub(tmp.path(), 7);
        let pool = SegmentReloadPool::new(2);

        // Three concurrent waiters for the SAME segment must all get the result
        // from ONE reload job.
        let r1 = pool.submit(Arc::clone(&stub));
        let r2 = pool.submit(Arc::clone(&stub));
        let r3 = pool.submit(Arc::clone(&stub));
        for r in [r1, r2, r3] {
            let seg = r.recv().expect("reply").expect("reload ok");
            assert_eq!(seg.segment_id(), 7);
        }
    }

    #[test]
    fn test_completed_cache_answers_later_submit_immediately() {
        distance::init();
        let tmp = tempfile::tempdir().unwrap();
        let stub = make_stub(tmp.path(), 3);
        let pool = SegmentReloadPool::new(1);

        // First reload populates the completed cache.
        pool.submit(Arc::clone(&stub)).recv().unwrap().unwrap();
        // A later submit (before anyone installs) resolves from cache — the
        // receiver is already ready.
        let rx = pool.submit(Arc::clone(&stub));
        let seg = rx.try_recv().expect("cache hit is immediate").unwrap();
        assert_eq!(seg.segment_id(), 3);
    }

    #[test]
    fn test_default_workers_env_override() {
        // Deterministic branch (no env set in the suite): ≤2 cores → 1, else 2.
        assert_eq!(default_workers(1), 1);
        assert_eq!(default_workers(2), 1);
        assert_eq!(default_workers(8), 2);
    }
}
