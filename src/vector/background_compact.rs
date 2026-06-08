//! Background vector compaction: moves the CPU-heavy HNSW build off the shard
//! event loop onto a dedicated worker thread pool.
//!
//! ## Design
//!
//! - The shard thread calls [`BackgroundCompactor::submit`] with a frozen snapshot
//!   and an `Arc<CollectionMetadata>`. The worker builds the `ImmutableSegment`
//!   and sends the result back over a one-shot `flume::bounded(1)` channel.
//! - The shard thread polls [`BackgroundCompactor::try_recv`] on subsequent ticks
//!   without blocking. When a reply arrives it installs the segment.
//! - Worker threads exit when the compactor is dropped (channel disconnects).
//! - [`BackgroundCompactor::submit_merge`] works identically but the worker calls
//!   [`compaction::merge_immutable`] instead of `compact`. Both return the same
//!   `CompactionResult` type over the same reply channel type.
//!
//! ## Thread Safety
//!
//! `FrozenSegment` and `CollectionMetadata` are `Send + Sync` (verified: no Rc,
//! no raw pointers beyond AlignedBuffer which has `unsafe impl Send/Sync`).
//! `ImmutableSegment` is `Send` for the same reason.
//! `Arc<ImmutableSegment>` is `Send + Sync` — the worker only reads the source
//! segments (graph and TQ codes); it never writes to their interior state.

use std::sync::{Arc, OnceLock};

use crate::vector::segment::compaction::{self, CompactionError, MergeMode};
use crate::vector::segment::immutable::ImmutableSegment;
use crate::vector::segment::mutable::FrozenSegment;
use crate::vector::turbo_quant::collection::CollectionMetadata;

/// The operation a worker should perform.
///
/// Both variants produce the same `CompactionResult` so a single worker loop
/// handles both without special-casing the reply path.
enum BuildOp {
    /// Mutable-to-immutable: build HNSW graph from a frozen mutable snapshot.
    Compact {
        frozen: FrozenSegment,
        collection: Arc<CollectionMetadata>,
        seed: u64,
    },
    /// Many-immutables-to-one: merge N Arc'd immutable segments into one.
    ///
    /// The worker only reads the source segments (graph + TQ codes). No lock is
    /// held on the source Arcs during the build — graph search on the source
    /// segments is fine to run concurrently.
    Merge {
        segments: Vec<Arc<ImmutableSegment>>,
        collection: Arc<CollectionMetadata>,
        seed: u64,
        mode: MergeMode,
        recall_tolerance: f32,
    },
}

/// Payload sent from the shard thread to a worker.
struct WorkerJob {
    /// The operation to perform.
    op: BuildOp,
    /// One-shot reply channel. Worker sends exactly one message then drops it.
    reply_tx: flume::Sender<CompactionResult>,
}

/// Result sent back to the shard thread.
pub type CompactionResult = Result<ImmutableSegment, CompactionError>;

/// Background compactor: owns a pool of worker threads and a submission channel.
///
/// ## Lifecycle
///
/// Create once (per shard or per store) and pass a reference to
/// [`VectorIndex::begin_background_compact`]. Drop to shut down workers.
///
/// Workers are started **lazily on first submit** to match the codebase's
/// lazy-init policy (see CLAUDE.md — "Lazy Lua/backlog init").
pub struct BackgroundCompactor {
    /// Submission channel — workers receive `WorkerJob`s from here.
    job_tx: flume::Sender<WorkerJob>,
    /// Worker thread join handles.
    _workers: Vec<std::thread::JoinHandle<()>>,
}

impl BackgroundCompactor {
    /// Create a new compactor with `num_workers` worker threads (default 1).
    ///
    /// Workers block on the job channel and exit when it disconnects (i.e.,
    /// when this `BackgroundCompactor` is dropped).
    pub fn new(num_workers: usize) -> Self {
        assert!(num_workers >= 1, "at least one worker required");
        let (job_tx, job_rx) = flume::bounded::<WorkerJob>(num_workers * 2);
        let workers: Vec<_> = (0..num_workers)
            .map(|i| {
                let rx = job_rx.clone();
                std::thread::Builder::new()
                    .name(format!("moon-vec-compact-{i}"))
                    .spawn(move || {
                        while let Ok(job) = rx.recv() {
                            let result = match job.op {
                                BuildOp::Compact {
                                    frozen,
                                    collection,
                                    seed,
                                } => compaction::compact(&frozen, &collection, seed, None),
                                BuildOp::Merge {
                                    segments,
                                    collection,
                                    seed,
                                    mode,
                                    recall_tolerance,
                                } => compaction::merge_immutable(
                                    &segments,
                                    &collection,
                                    seed,
                                    mode,
                                    recall_tolerance,
                                ),
                            };
                            // If the receiver was dropped before we finished, swallow the error.
                            let _ = job.reply_tx.send(result);
                        }
                        // job_tx dropped → channel disconnected → we exit cleanly.
                    })
                    .expect("failed to spawn compaction worker thread")
            })
            .collect();
        Self {
            job_tx,
            _workers: workers,
        }
    }

    /// Submit a mutable→immutable compaction job.
    ///
    /// Returns an `Err` only when all workers have exited (compactor shut down).
    /// Returns the `reply_rx` — the caller polls it with [`try_recv`](flume::Receiver::try_recv).
    pub fn submit(
        &self,
        frozen: FrozenSegment,
        collection: Arc<CollectionMetadata>,
        seed: u64,
    ) -> Result<flume::Receiver<CompactionResult>, CompactionSubmitError> {
        let (reply_tx, reply_rx) = flume::bounded::<CompactionResult>(1);
        let job = WorkerJob {
            op: BuildOp::Compact {
                frozen,
                collection,
                seed,
            },
            reply_tx,
        };
        self.job_tx
            .try_send(job)
            .map_err(|_| CompactionSubmitError::WorkersBusy)?;
        Ok(reply_rx)
    }

    /// Submit an immutable→immutable merge job.
    ///
    /// The worker reads the source segment Arcs but never mutates them.
    /// Returns `reply_rx` — caller polls it non-blocking with `try_recv`.
    pub fn submit_merge(
        &self,
        segments: Vec<Arc<ImmutableSegment>>,
        collection: Arc<CollectionMetadata>,
        seed: u64,
        mode: MergeMode,
        recall_tolerance: f32,
    ) -> Result<flume::Receiver<CompactionResult>, CompactionSubmitError> {
        let (reply_tx, reply_rx) = flume::bounded::<CompactionResult>(1);
        let job = WorkerJob {
            op: BuildOp::Merge {
                segments,
                collection,
                seed,
                mode,
                recall_tolerance,
            },
            reply_tx,
        };
        self.job_tx
            .try_send(job)
            .map_err(|_| CompactionSubmitError::WorkersBusy)?;
        Ok(reply_rx)
    }
}

/// Resolve the global compactor's worker count.
///
/// Precedence:
///   1. `MOON_VEC_COMPACT_WORKERS` env var (explicit operator override; clamped ≥1).
///   2. Auto: `available_parallelism() / 2`, clamped to `[1, 8]`.
///
/// Rationale: idle workers just park on the job channel (≈ zero cost), so a
/// larger pool is cheap until a compaction burst. Sizing to half the cores lets
/// several shards' builds run concurrently while still leaving cores for the
/// thread-per-core shard loops at any instant (the OS time-slices during the
/// rare, bursty build window). Operators who want strict shard isolation set
/// `MOON_VEC_COMPACT_WORKERS=1`; write-heavy fleets bump it higher.
fn default_worker_count() -> usize {
    if let Ok(v) = std::env::var("MOON_VEC_COMPACT_WORKERS") {
        if let Ok(n) = v.parse::<usize>() {
            return n.max(1);
        }
    }
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    (cores / 2).clamp(1, 8)
}

/// Process-wide compactor shared by all shards.
///
/// Lazily initialized on first use with [`default_worker_count`] worker threads.
/// A global (rather than per-shard) compactor lets the search-path `try_compact`
/// dispatch a build without plumbing a `BackgroundCompactor` handle through the
/// shard and all three connection handlers. `flume` senders/receivers are
/// `Send + Sync`, so shard threads (including monoio `!Send` tasks) can
/// submit/poll freely; multiple workers let concurrent shard/index compactions
/// proceed in parallel instead of serializing on a single thread.
pub fn global() -> &'static BackgroundCompactor {
    static GLOBAL: OnceLock<BackgroundCompactor> = OnceLock::new();
    GLOBAL.get_or_init(|| BackgroundCompactor::new(default_worker_count()))
}

/// Error returned when [`BackgroundCompactor::submit`] fails.
#[derive(Debug, thiserror::Error)]
pub enum CompactionSubmitError {
    /// All worker threads are saturated and the job queue is full.
    #[error("compaction job queue is full — all workers are busy")]
    WorkersBusy,
    /// The compactor has been dropped (workers exited).
    #[error("compactor has been shut down")]
    ShutDown,
}
