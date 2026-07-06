//! Intra-query FT.SEARCH worker pool — parallel fan-out of per-segment graph
//! searches within ONE query.
//!
//! ## Why intra-query (not per-query) parallelism
//!
//! Moon's post-compact index is a LIST of immutable segments; a KNN query
//! searches every segment and merges. The serial per-segment loop is the
//! dominant single-query latency term once ≥2 segments exist (each segment is
//! an independent HNSW beam search at the full resolved `ef_search`). Fanning
//! the per-segment searches across worker threads cuts p50 directly — which is
//! what closes a single-connection QPS gap (QPS = 1/latency there). A
//! RediSearch-style pool that only runs *different* queries concurrently
//! cannot move that number.
//!
//! ## Design
//!
//! - Plain `std::thread` workers + `flume` channels (runtime-agnostic: the
//!   shard side awaits replies with `recv_async`, which works under monoio and
//!   tokio alike and never blocks the event loop). Mirrors
//!   [`crate::vector::background_compact::BackgroundCompactor`].
//! - Jobs carry `Arc` segment references (`SegmentList` holds
//!   `Vec<Arc<ImmutableSegment>>`) — O(1) refcount bumps, no data copies. The
//!   segment types are already proven `Send + Sync` by the compaction pool,
//!   which ships them to its own threads.
//! - Each worker owns a cached [`SearchScratch`] (grown when a larger
//!   `padded_dim` arrives) — zero steady-state allocation per job.
//! - Mutable-segment scanning, cold (DiskANN) and IVF tiers stay on the
//!   calling task: mutable needs MVCC context, cold/IVF are rare tiers. The
//!   caller overlaps its own mutable scan with the workers.
//! - **Failure containment**: a panicking segment search is caught per-job;
//!   the worker survives and the caller receives an empty result for that
//!   segment (fail-degraded, never hung — the reply channel is always
//!   answered or dropped, and a dropped reply surfaces as `RecvError`).
//!
//! ## Identity guarantee
//!
//! Pooled and serial searches produce identical results: per-segment searches
//! are independent reads of committed-by-definition segments, and the caller
//! merges with `sort_unstable` under `SearchResult`'s total order (distance,
//! then id), so accumulation order cannot affect the final top-k.

use std::sync::Arc;
use std::sync::OnceLock;

use roaring::RoaringBitmap;
use smallvec::SmallVec;

use crate::vector::hnsw::search::SearchScratch;
use crate::vector::persistence::warm_search::WarmSearchSegment;
use crate::vector::segment::immutable::ImmutableSegment;
use crate::vector::turbo_quant::encoder::padded_dimension;
use crate::vector::types::SearchResult;

/// Hard cap on auto-sized worker count — beyond ~8 workers the per-query
/// segment count (merge auto-triggers at 16 segments) no longer fills the
/// pool, and extra idle threads only cost memory.
const MAX_AUTO_WORKERS: usize = 8;

/// A graph-tier segment reference a worker can search independently.
/// Immutable and warm segments are committed-by-definition — no MVCC context
/// crosses the thread boundary.
pub enum GraphSegmentRef {
    Immutable(Arc<ImmutableSegment>),
    Warm(Arc<WarmSearchSegment>),
}

/// One per-segment search unit. `reply` is answered exactly once (empty on
/// caught panic); if the worker dies anyway, the dropped sender surfaces as
/// `RecvError` on the caller side — never a hang.
pub struct SegmentSearchJob {
    pub segment: GraphSegmentRef,
    /// Owned query copy shared across this query's jobs (one alloc per query).
    pub query: Arc<[f32]>,
    pub fetch_k: usize,
    pub ef_search: usize,
    /// `Some` = ACORN-filtered traversal (allow-list). Post-filter oversampling
    /// mode ships `None` here; the caller applies the bitmap to the results.
    pub filter: Option<Arc<RoaringBitmap>>,
    pub reply: flume::Sender<SmallVec<[SearchResult; 32]>>,
}

/// Worker pool for intra-query segment fan-out. Create once per process
/// (see [`init_global`]); drop disconnects the channel and workers exit.
pub struct SearchWorkerPool {
    job_tx: flume::Sender<SegmentSearchJob>,
    num_workers: usize,
    _workers: Vec<std::thread::JoinHandle<()>>,
}

impl SearchWorkerPool {
    /// Spawn `num_workers` (≥1) searcher threads.
    pub fn new(num_workers: usize) -> Self {
        assert!(num_workers >= 1, "at least one worker required");
        // Unbounded: job volume is (graph segments per in-flight query) — small
        // and naturally bounded by connection count; the shard thread must
        // NEVER block on submit.
        let (job_tx, job_rx) = flume::unbounded::<SegmentSearchJob>();
        let workers: Vec<_> = (0..num_workers)
            .map(|i| {
                let rx = job_rx.clone();
                std::thread::Builder::new()
                    .name(format!("moon-vec-search-{i}"))
                    .spawn(move || worker_loop(&rx))
                    .expect("failed to spawn vector search worker thread")
            })
            .collect();
        Self {
            job_tx,
            num_workers,
            _workers: workers,
        }
    }

    /// Submit a job. Returns `false` if the pool is shut down (caller must
    /// then run the segment search inline — design-for-failure fallback).
    #[must_use]
    pub fn submit(&self, job: SegmentSearchJob) -> bool {
        self.job_tx.send(job).is_ok()
    }

    pub fn num_workers(&self) -> usize {
        self.num_workers
    }
}

fn worker_loop(rx: &flume::Receiver<SegmentSearchJob>) {
    // Cached scratch, rebuilt when the job's padded_dim differs — amortized
    // zero allocation per job for a homogeneous index workload. EXACT match is
    // required (not ≥): `query_rotated.len()` acts as the search's padded_dim
    // (LUT sizing + code_len invariants in hnsw/search.rs key off it), so an
    // oversized buffer is as wrong as an undersized one.
    let mut scratch = SearchScratch::new(0, 0);
    let mut scratch_pdim: u32 = 0;
    while let Ok(job) = rx.recv() {
        let pdim = padded_dimension(job.query.len() as u32);
        if pdim != scratch_pdim {
            scratch = SearchScratch::new(0, pdim);
            scratch_pdim = pdim;
        }
        // Contain a panicking segment search to this job: reply empty and keep
        // the worker alive. Scratch is rebuilt afterwards — a panic can leave
        // it in an arbitrary (but memory-safe) state.
        let outcome =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_job(&job, &mut scratch)));
        let results = match outcome {
            Ok(r) => r,
            Err(_) => {
                tracing::warn!(
                    "vector search worker caught a per-segment search panic; \
                     returning empty results for this segment"
                );
                scratch = SearchScratch::new(0, scratch_pdim);
                SmallVec::new()
            }
        };
        // Receiver may already be gone (client disconnected mid-search).
        let _ = job.reply.send(results);
    }
    // job_tx dropped → channel disconnected → exit cleanly.
}

fn run_job(job: &SegmentSearchJob, scratch: &mut SearchScratch) -> SmallVec<[SearchResult; 32]> {
    let filter = job.filter.as_deref();
    match &job.segment {
        GraphSegmentRef::Immutable(seg) => match filter {
            Some(bm) => {
                seg.search_filtered(&job.query, job.fetch_k, job.ef_search, scratch, Some(bm))
            }
            None => seg.search(&job.query, job.fetch_k, job.ef_search, scratch),
        },
        GraphSegmentRef::Warm(seg) => match filter {
            Some(bm) => {
                seg.search_filtered(&job.query, job.fetch_k, job.ef_search, scratch, Some(bm))
            }
            None => seg.search(&job.query, job.fetch_k, job.ef_search, scratch),
        },
    }
}

// ── Process-global pool ─────────────────────────────────────────────────────

/// `None` inside = explicitly disabled (0 workers). Uninitialized = disabled:
/// unit tests and embedded uses see the exact serial path unless they opt in.
static GLOBAL_POOL: OnceLock<Option<SearchWorkerPool>> = OnceLock::new();

/// Initialize the process-global pool. `num_workers == 0` disables pooling.
/// First call wins (idempotent); called once from server startup.
pub fn init_global(num_workers: usize) {
    let _ = GLOBAL_POOL.set(if num_workers == 0 {
        None
    } else {
        Some(SearchWorkerPool::new(num_workers))
    });
}

/// The process-global pool, or `None` when disabled/uninitialized.
pub fn global() -> Option<&'static SearchWorkerPool> {
    GLOBAL_POOL.get().and_then(|p| p.as_ref())
}

/// Auto-sizing: leave the shard threads their cores, cap at
/// [`MAX_AUTO_WORKERS`]. `cores <= shards` (the KV-tuned deployment shape)
/// yields 0 — pooling disabled, zero interference with KV latency.
pub fn auto_workers(cores: usize, shards: usize) -> usize {
    cores.saturating_sub(shards).min(MAX_AUTO_WORKERS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::distance;
    use crate::vector::segment::holder::{SearchSnapshot, SegmentHolder, YieldBudget};

    /// Yield-free budget: unit tests drive the async search with
    /// `futures::executor::block_on`, where monoio's `cooperative_yield`
    /// (scoped-TLS) would panic. With MAX caps no yield point is ever taken;
    /// the pooled path's `recv_async` is executor-agnostic.
    fn no_yield_budget() -> YieldBudget {
        YieldBudget {
            max_segments_per_chunk: usize::MAX,
            max_graph_nodes_per_chunk: usize::MAX,
            max_brute_force_vecs_per_chunk: usize::MAX,
        }
    }
    use crate::vector::store::VectorStore;
    use bytes::Bytes;

    fn random_vec(dim: usize, seed: u64) -> Vec<f32> {
        let mut state = seed.wrapping_add(1);
        (0..dim)
            .map(|_| {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                ((state >> 33) as f32) / (u32::MAX as f32) * 2.0 - 1.0
            })
            .collect()
    }

    /// Store with one index, `segs` immutable segments of `per_seg` vectors
    /// each, plus `tail` vectors left in the mutable segment.
    fn build_store(dim: u32, segs: usize, per_seg: usize, tail: usize) -> VectorStore {
        distance::init();
        let mut store = VectorStore::new();
        store
            .create_index(crate::vector::store::test_index_meta(dim))
            .unwrap();
        let mut n = 0u64;
        for _ in 0..segs {
            for _ in 0..per_seg {
                let key = format!("doc:{n}");
                let hash = xxhash_rust::xxh64::xxh64(key.as_bytes(), 0);
                store
                    .insert_vector(b"idx", &random_vec(dim as usize, n), hash, Bytes::from(key))
                    .unwrap();
                n += 1;
            }
            store.force_compact_index(b"idx").unwrap();
        }
        for _ in 0..tail {
            let key = format!("doc:{n}");
            let hash = xxhash_rust::xxh64::xxh64(key.as_bytes(), 0);
            store
                .insert_vector(b"idx", &random_vec(dim as usize, n), hash, Bytes::from(key))
                .unwrap();
            n += 1;
        }
        store
    }

    fn snapshot_for(store: &mut VectorStore, query: Vec<f32>, k: usize) -> SearchSnapshot {
        let committed = store.txn_manager().committed_snapshot();
        let snapshot_lsn = store.txn_manager().current_lsn();
        let idx = store.get_index_mut(b"idx").unwrap();
        let segments = idx.segments.load_full();
        let mutable_len = segments.mutable.len();
        let dim = query.len() as u32;
        SearchSnapshot {
            segments,
            query_f32: query,
            k,
            ef_search: 100,
            filter_bitmap: None,
            filter_strategy: crate::vector::filter::selectivity::FilterStrategy::Unfiltered,
            snapshot_lsn,
            my_txn_id: 0,
            committed,
            dimension: dim,
            mutable_len,
            scratch: SearchScratch::new(0, padded_dimension(dim)),
            key_hash_to_key: idx.key_hash_to_key.clone(),
            ef_defaulted: false,
        }
    }

    fn run_search(
        store: &mut VectorStore,
        query: &[f32],
        k: usize,
        pool: Option<&SearchWorkerPool>,
    ) -> Vec<(u32, u64)> {
        let mut snap = snapshot_for(store, query.to_vec(), k);
        let results = futures::executor::block_on(SegmentHolder::search_mvcc_yielding_with_pool(
            &mut snap,
            no_yield_budget(),
            pool,
        ));
        results.iter().map(|r| (r.id.0, r.key_hash)).collect()
    }

    /// G-IDENTITY for the pooled path: multi-segment + mutable-tail index must
    /// return exactly the serial path's results.
    #[test]
    fn test_pooled_search_identical_to_serial() {
        let dim = 32u32;
        let mut store = build_store(dim, 4, 60, 25);
        let pool = SearchWorkerPool::new(3);
        for q in 0..20u64 {
            let query = random_vec(dim as usize, 10_000 + q);
            let serial = run_search(&mut store, &query, 10, None);
            let pooled = run_search(&mut store, &query, 10, Some(&pool));
            assert_eq!(serial, pooled, "query {q}: pooled != serial");
            assert!(!serial.is_empty(), "query {q}: no results");
        }
    }

    /// Thread-safety under concurrency: many OS threads driving pooled
    /// searches against the same segments must each match serial.
    #[test]
    fn test_pooled_search_concurrent_stress() {
        let dim = 32u32;
        let mut store = build_store(dim, 5, 40, 0);
        let pool = Arc::new(SearchWorkerPool::new(4));

        // Serial ground truth per query.
        let queries: Vec<Vec<f32>> = (0..32u64)
            .map(|q| random_vec(dim as usize, 77_000 + q))
            .collect();
        let expected: Vec<_> = queries
            .iter()
            .map(|q| run_search(&mut store, q, 10, None))
            .collect();

        // Owned snapshot ingredients (SegmentList Arc is Send+Sync).
        let idx = store.get_index_mut(b"idx").unwrap();
        let segments = idx.segments.load_full();
        let key_map = idx.key_hash_to_key.clone();
        let committed = store.txn_manager().committed_snapshot();
        let snapshot_lsn = store.txn_manager().current_lsn();

        let handles: Vec<_> = (0..8)
            .map(|t| {
                let pool = Arc::clone(&pool);
                let segments = Arc::clone(&segments);
                let key_map = key_map.clone();
                let committed = committed.clone();
                let queries = queries.clone();
                let expected = expected.clone();
                std::thread::spawn(move || {
                    for (qi, query) in queries.iter().enumerate() {
                        let mut snap = SearchSnapshot {
                            segments: Arc::clone(&segments),
                            query_f32: query.clone(),
                            k: 10,
                            ef_search: 100,
                            filter_bitmap: None,
                            filter_strategy:
                                crate::vector::filter::selectivity::FilterStrategy::Unfiltered,
                            snapshot_lsn,
                            my_txn_id: 0,
                            committed: committed.clone(),
                            dimension: dim,
                            mutable_len: segments.mutable.len(),
                            scratch: SearchScratch::new(0, padded_dimension(dim)),
                            key_hash_to_key: key_map.clone(),
                            ef_defaulted: false,
                        };
                        let results = futures::executor::block_on(
                            SegmentHolder::search_mvcc_yielding_with_pool(
                                &mut snap,
                                no_yield_budget(),
                                Some(&pool),
                            ),
                        );
                        let got: Vec<(u32, u64)> =
                            results.iter().map(|r| (r.id.0, r.key_hash)).collect();
                        assert_eq!(got, expected[qi], "thread {t} query {qi} diverged");
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_auto_workers_sizing() {
        assert_eq!(auto_workers(8, 1), 7);
        assert_eq!(auto_workers(8, 8), 0, "cores==shards must disable pooling");
        assert_eq!(auto_workers(4, 12), 0);
        assert_eq!(auto_workers(32, 4), MAX_AUTO_WORKERS, "cap at 8");
    }
}
