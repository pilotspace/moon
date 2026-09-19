//! Checkpoint Finalize, step 0: every heap file the checkpoint's flush
//! wrote must be durable before the redo point may advance (#452) — and a
//! heap file that no longer exists must not hold the redo point back.
//!
//! Split out of `persistence_tick.rs`, which is past the 1500-line cap.

use std::path::Path;

use crate::persistence::checkpoint::CheckpointManager;
use crate::persistence::manifest::ShardManifest;
use crate::persistence::page_cache::PageCache;

/// `{shard_dir}/data/heap-{file_id:06}.mpf` — the KV heap file a checkpoint
/// flushes dirty pages into.
pub(super) fn heap_file_path(shard_dir: &Path, file_id: u64) -> std::path::PathBuf {
    shard_dir
        .join("data")
        .join(format!("heap-{:06}.mpf", file_id))
}

/// Whether the manifest still lists heap file `file_id` as a live KV spill
/// file (any status but `Tombstone`).
fn manifest_lists_live_heap_file(manifest: &ShardManifest, file_id: u64) -> bool {
    manifest.files().iter().any(|e| {
        e.file_id == file_id
            && e.file_type == crate::persistence::page::PageType::KvLeaf as u8
            && e.status != crate::persistence::manifest::FileStatus::Tombstone
    })
}

/// Heap file `file_id` no longer exists on disk (its `open` returned
/// `NotFound`, at the page write or at the fsync): stop waiting on it. It
/// leaves the checkpoint's pending-fsync set, its cached pages stop being
/// dirty, and it is counted and logged — never dropped silently.
///
/// Why this cannot lose a change the redo point depends on:
///
/// - The fsync exists to make durable the bytes this checkpoint wrote into
///   THAT inode. Once its name is unlinked no fsync can reach the inode (a
///   retry re-opens by path), and nothing can read it: heap pages are only
///   ever read through the cold index, which locates them by `file_id`.
/// - Heap files are unlinked in exactly two places, and neither removes a
///   file the cold index points into. `ColdIndex::drain_pending_unlink`
///   deletes a file only once its live-reference count is zero (re-checked
///   at drain time), and tombstones its entry in this same `ShardManifest`,
///   which Finalize commits (step 3) before the control file publishes the
///   redo point (step 4). The boot orphan sweep
///   (`kv_spill::classify_orphan_heap_files`) deletes only files the manifest
///   never registered, which were never indexed.
/// - The name cannot come back as a different file: `file_id`s come from one
///   monotonic per-shard counter seeded past every id on disk and in the
///   manifest (`file_id_seed`; pinned by
///   `test_cold_file_id_counter_has_one_home_and_never_moves_back`), and a
///   spill always writes a freshly minted id. So an fsync by path can never
///   land on some other inode that happens to carry the name.
/// - The WAL: the page's FullPageImage was appended after the checkpoint
///   began, so it lies ABOVE the redo point and is not recycled by it.
///   Recovery replays it into a re-created file that no live manifest entry
///   lists, so nothing reads it (at worst a small leftover file). The page's
///   own change record below the redo point is recycled, and nothing needs
///   it: the only file it could be applied to is gone and unreferenced.
///
/// The one cause this does not cover: something OTHER than those two paths
/// removed a file the manifest still lists as live (an operator, disk
/// tooling). The keys in it are lost whatever the checkpoint does — no retry
/// can fsync the unlinked inode — and holding the redo point back would only
/// grow the WAL until the disk is full, turning one lost file into a
/// whole-shard outage. So that case stops waiting too, but it is logged as an
/// ERROR and counted separately (`VanishedDataFiles::still_registered`); cold
/// reads of its keys report the file as missing. Its cached pages stay VALID,
/// so reads the page cache can still serve keep working until eviction.
pub(super) fn stop_waiting_on_vanished_heap_file(
    checkpoint_mgr: &mut CheckpointManager,
    page_cache: &PageCache,
    manifest: &ShardManifest,
    file_id: u64,
    site: &'static str,
) {
    let was_pending = checkpoint_mgr.forget_data_file(file_id);
    let abandoned_pages = page_cache.abandon_dirty_pages_of_file(file_id);
    let still_registered = manifest_lists_live_heap_file(manifest, file_id);
    checkpoint_mgr.note_data_file_vanished(still_registered);
    if still_registered {
        tracing::error!(
            file_id,
            site,
            was_pending,
            abandoned_pages,
            "Checkpoint: heap file heap-{:06}.mpf is gone although the manifest still lists \
             it as live — something other than the cold-tier GC removed it and the keys in \
             it are lost. The checkpoint no longer waits on it.",
            file_id
        );
    } else {
        tracing::debug!(
            file_id,
            site,
            was_pending,
            abandoned_pages,
            "Checkpoint: heap file retired (unlinked by the cold-tier GC) before it was made \
             durable; nothing references it, no longer waiting on it"
        );
    }
}

/// Where Finalize stands on making the heap files it wrote durable.
pub(super) enum HeapFilesDurability {
    /// Every heap file this checkpoint wrote is durable (or gone and
    /// unreferenced): the redo point may advance.
    Durable,
    /// An fsync batch is running off the shard thread; ask again next tick.
    Pending,
    /// A file could not be synced (or the helper was lost): back off, retry.
    Failed,
}

/// Finalize step 0: drive the off-loop fsync of every heap file the flush
/// wrote. NEVER blocks the shard thread: it polls the outstanding batch
/// with `try_recv`, applies a finished batch's report, and starts a new
/// batch only when none is outstanding — so a hung disk leaves exactly one
/// helper thread stuck, however many ticks ask.
pub(super) fn make_written_heap_files_durable(
    checkpoint_mgr: &mut CheckpointManager,
    page_cache: &PageCache,
    manifest: &ShardManifest,
    shard_dir: &Path,
) -> HeapFilesDurability {
    use crate::persistence::data_file_sync::{DataSyncJob, DataSyncOutcome, DataSyncPoll};
    use crate::persistence::wal_v3::segment::WAIT_DURABLE_TIMEOUT;

    match checkpoint_mgr.poll_data_sync(WAIT_DURABLE_TIMEOUT) {
        DataSyncPoll::Idle => {}
        DataSyncPoll::Pending { overdue } => {
            if let Some(outstanding) = overdue {
                tracing::warn!(
                    "Checkpoint: heap data-file fsync outstanding for {:?}; the redo point \
                     waits for it. The shard keeps serving and starts no second fsync.",
                    outstanding
                );
            }
            return HeapFilesDurability::Pending;
        }
        DataSyncPoll::Lost => {
            tracing::error!(
                "Checkpoint: heap data-file fsync helper exited without a result; retrying"
            );
            return HeapFilesDurability::Failed;
        }
        DataSyncPoll::Done(report) => {
            let mut failed = false;
            for (job, outcome) in report.results {
                match outcome {
                    DataSyncOutcome::Synced => {
                        checkpoint_mgr.note_data_file_synced(job.file_id, job.generation);
                    }
                    DataSyncOutcome::Vanished => stop_waiting_on_vanished_heap_file(
                        checkpoint_mgr,
                        page_cache,
                        manifest,
                        job.file_id,
                        "fsync",
                    ),
                    DataSyncOutcome::OpenFailed(e) => {
                        tracing::error!(
                            "Checkpoint: cannot open heap file {} to fsync it: {}; retrying",
                            job.path.display(),
                            e
                        );
                        failed = true;
                    }
                    DataSyncOutcome::SyncFailed(e) => {
                        tracing::error!(
                            "Checkpoint: fsync of heap file {} FAILED: {}. Pages it wrote \
                             cannot be proven durable; this shard publishes no further redo \
                             point.",
                            job.path.display(),
                            e
                        );
                        failed = true;
                    }
                    DataSyncOutcome::NotAttempted => failed = true,
                }
            }
            if failed {
                return HeapFilesDurability::Failed;
            }
        }
    }

    let jobs: Vec<DataSyncJob> = checkpoint_mgr
        .unsynced_data_files()
        .into_iter()
        .map(|(file_id, generation)| DataSyncJob {
            file_id,
            generation,
            path: heap_file_path(shard_dir, file_id),
        })
        .collect();
    if jobs.is_empty() {
        return HeapFilesDurability::Durable;
    }
    match checkpoint_mgr.start_data_sync(jobs) {
        Ok(()) => HeapFilesDurability::Pending,
        Err(e) => {
            tracing::error!("Checkpoint: cannot start the heap data-file fsync: {}", e);
            HeapFilesDurability::Failed
        }
    }
}
