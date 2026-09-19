//! The checkpoint may only advance the redo point over pages it has made
//! durable (#452): every page's FullPageImage in the WAL before the page is
//! overwritten in place, and every heap file it wrote fsynced before the
//! control file publishes the new redo point (the WAL below it is then
//! recycled). And it must do so without wedging: a heap file that no longer
//! exists cannot hold a redo point back forever, and the off-loop data-file
//! fsync never blocks the shard thread nor piles up helper threads.
//!
//! Lives beside `persistence_tick.rs` (a child module of it) because that
//! file is already past the 1500-line cap.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use super::tests::count_fpi_records;
use super::*;
use crate::persistence::checkpoint::CheckpointTrigger;
use crate::persistence::manifest::{FileEntry, FileStatus, StorageTier};
use crate::persistence::page::PageType;
use crate::persistence::wal_v3::record::WalRecordType;
use crate::persistence::wal_v3::segment::{DEFAULT_SEGMENT_SIZE, WalBounds};

/// A shard with ONE dirty, FPI-pending 4 KiB heap page (file 1, page 0)
/// whose modification is WAL record `page_lsn`, and a checkpoint begun
/// AFTER that record, so its redo point is strictly above the page's
/// change: once it publishes, replay never sees that record again.
struct DirtyPageShard {
    _tmp: tempfile::TempDir,
    wal_dir: std::path::PathBuf,
    heap_path: std::path::PathBuf,
    page_cache: PageCache,
    wal: WalWriterV3,
    checkpoint_mgr: CheckpointManager,
    manifest: ShardManifest,
    control: ShardControlFile,
    control_path: std::path::PathBuf,
    redo_lsn: u64,
}

const PAGE_BYTE: u8 = 0x5A;

/// Upper bound for a checkpoint that SHOULD finalize: generous against a
/// loaded CI host, and long enough for several finalize backoff rounds
/// (50 ms, 100 ms, 200 ms, …) so a checkpoint that never finalizes is told
/// apart from a slow one.
const FINALIZE_DEADLINE: Duration = Duration::from_secs(4);

impl DirtyPageShard {
    fn new() -> Self {
        Self::with_wal_bounds(WalBounds::DEFAULT)
    }

    fn with_wal_bounds(bounds: WalBounds) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        let wal_dir = shard_dir.join("wal-v3");
        let data_dir = shard_dir.join("data");
        std::fs::create_dir_all(&wal_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        let heap_path = data_dir.join("heap-000001.mpf");
        std::fs::write(&heap_path, vec![0u8; 4096]).unwrap();

        let mut wal = WalWriterV3::new(0, &wal_dir, DEFAULT_SEGMENT_SIZE, bounds).unwrap();
        let page_lsn = wal.append(WalRecordType::Command, b"page change");
        wal.flush_sync().unwrap();

        let page_cache = PageCache::new(4, 0);
        let handle = page_cache
            .fetch_page(1, 0, false, |buf| {
                buf.fill(PAGE_BYTE);
                Ok(())
            })
            .unwrap();
        page_cache.unpin_page(handle);
        page_cache.mark_dirty(1, 0, page_lsn);

        let redo_lsn = wal.current_lsn();
        assert!(redo_lsn > page_lsn);
        let mut checkpoint_mgr =
            CheckpointManager::new(CheckpointTrigger::new(300, 256 * 1024 * 1024, 0.9));
        assert!(checkpoint_mgr.begin(redo_lsn, page_cache.dirty_page_count()));
        page_cache.arm_all_fpi_pending();

        let manifest = ShardManifest::create(&shard_dir.join("manifest.dat")).unwrap();
        let control = ShardControlFile::new([0u8; 16]);
        let control_path = ShardControlFile::control_path(&shard_dir, 0);
        control.write(&control_path).unwrap();
        Self {
            _tmp: tmp,
            wal_dir,
            heap_path,
            page_cache,
            wal,
            checkpoint_mgr,
            manifest,
            control,
            control_path,
            redo_lsn,
        }
    }

    /// Register heap file 1 in the manifest as a live KV spill file, the
    /// state of a file the cold index still serves keys from.
    fn register_heap_file_as_live(&mut self) {
        self.manifest.add_file(FileEntry {
            file_id: 1,
            file_type: PageType::KvLeaf as u8,
            status: FileStatus::Active,
            tier: StorageTier::Hot,
            page_size_log2: 12,
            page_count: 1,
            byte_size: 4096,
            created_lsn: 0,
            db_index: 0,
            max_key_hash: 0,
            last_modified_lsn: 0,
        });
    }

    /// One checkpoint tick; `true` when a Finalize completed.
    fn tick(&mut self) -> bool {
        handle_checkpoint_tick(
            &mut self.checkpoint_mgr,
            &self.page_cache,
            &mut self.wal,
            &mut self.manifest,
            &mut self.control,
            &self.control_path,
            2,
            300,
            &mut |_| true,
        )
    }

    /// Tick every few milliseconds (the event loop ticks every 1 ms) until
    /// a Finalize completes or `deadline` passes; `true` if it finalized.
    fn tick_for(&mut self, deadline: Duration) -> bool {
        let until = Instant::now() + deadline;
        loop {
            if self.tick() {
                return true;
            }
            if Instant::now() >= until {
                return false;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn tick_until_finalized(&mut self) {
        assert!(
            self.tick_for(FINALIZE_DEADLINE),
            "checkpoint did not finalize within {FINALIZE_DEADLINE:?}: redo point still {}, \
             wanted {}",
            self.published_redo_lsn(),
            self.redo_lsn
        );
    }

    /// The redo point as persisted in the control file on disk.
    fn published_redo_lsn(&self) -> u64 {
        ShardControlFile::read(&self.control_path)
            .unwrap()
            .last_checkpoint_lsn
    }

    /// FullPageImage records present in the WAL files on DISK (records
    /// still in the writer's memory buffer do not count).
    fn fpi_records_on_disk(&self) -> usize {
        let mut n = 0;
        for entry in std::fs::read_dir(&self.wal_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|e| e == "wal") {
                n += count_fpi_records(&std::fs::read(&path).unwrap());
            }
        }
        n
    }

    fn heap_page_on_disk(&self) -> Vec<u8> {
        std::fs::read(&self.heap_path).unwrap()[..4096].to_vec()
    }

    /// Put a DIRECTORY where the heap file was: opening it for writing then
    /// fails with an error that is not `NotFound` (EISDIR on Unix, access
    /// denied on Windows) — a file that exists but cannot be written, the
    /// retryable class. Returns the path the real file was parked at.
    fn make_heap_file_unopenable(&self) -> std::path::PathBuf {
        let parked = self.heap_path.with_extension("parked");
        std::fs::rename(&self.heap_path, &parked).unwrap();
        std::fs::create_dir(&self.heap_path).unwrap();
        parked
    }

    fn restore_heap_file(&self, parked: &std::path::Path) {
        std::fs::remove_dir(&self.heap_path).unwrap();
        std::fs::rename(parked, &self.heap_path).unwrap();
    }
}

/// Log-before-data for torn-page protection: the moment a page's new
/// bytes reach its heap file, the FullPageImage that can repair a torn
/// write of that page must already be in the WAL on disk. The flush used
/// to append every FPI only AFTER the whole batch had been pwritten, into
/// the writer's memory buffer — a crash mid-pwrite left a torn page and
/// no image anywhere.
#[test]
fn checkpoint_fpi_reaches_the_wal_before_its_page_reaches_the_heap_file() {
    let mut shard = DirtyPageShard::new();
    assert_eq!(shard.fpi_records_on_disk(), 0);

    // One page, one page per tick: this tick flushes it.
    assert!(!shard.tick());
    assert!(
        shard.heap_page_on_disk().iter().all(|&b| b == PAGE_BYTE),
        "precondition: the flush tick wrote the page into its heap file"
    );
    assert_eq!(
        shard.fpi_records_on_disk(),
        1,
        "the page was overwritten in place while its FullPageImage was not yet in \
         the WAL on disk — a crash in that window leaves a torn page with no image"
    );
}

/// The checkpoint must not publish a redo point above a page change it
/// cannot prove durable. Here the heap file the page was written to exists
/// but cannot be opened to fsync it: Finalize must refuse — publishing
/// would recycle the only WAL record of the change while the page sits in
/// the kernel's page cache, unsynced. Once the file is openable again the
/// retry fsyncs it and completes.
#[test]
fn checkpoint_does_not_publish_redo_over_a_heap_file_it_cannot_fsync() {
    let mut shard = DirtyPageShard::new();
    assert!(!shard.tick(), "flush tick");
    assert!(shard.heap_page_on_disk().iter().all(|&b| b == PAGE_BYTE));
    let before = shard.published_redo_lsn();
    assert!(before < shard.redo_lsn);

    let parked = shard.make_heap_file_unopenable();
    let finalized = shard.tick_for(Duration::from_millis(300));
    assert!(
        !finalized,
        "Finalize published redo_lsn {} although the heap file holding the page \
         written below it could not be fsynced",
        shard.redo_lsn
    );
    assert_eq!(shard.published_redo_lsn(), before);
    assert_eq!(shard.control.last_checkpoint_lsn, before);
    assert!(shard.checkpoint_mgr.is_active());

    shard.restore_heap_file(&parked);
    shard.tick_until_finalized();
    assert_eq!(shard.published_redo_lsn(), shard.redo_lsn);
}

/// A page that failed to flush (its heap file could not be opened when the
/// flush tick ran) is still dirty and was never written. The checkpoint
/// must not finalize over it; it re-flushes the page and only then
/// publishes — with the page's bytes actually in the heap file.
#[test]
fn checkpoint_does_not_finalize_over_a_page_that_failed_to_flush() {
    let mut shard = DirtyPageShard::new();
    let parked = shard.make_heap_file_unopenable();

    assert!(!shard.tick(), "flush tick (the pwrite fails)");
    assert_eq!(shard.page_cache.dirty_page_count(), 1);
    let finalized = shard.tick_for(Duration::from_millis(300));
    assert!(
        !finalized,
        "Finalize published redo_lsn {} over a page that never reached its heap file",
        shard.redo_lsn
    );
    assert_eq!(shard.published_redo_lsn(), 0);

    shard.restore_heap_file(&parked);
    shard.tick_until_finalized();
    assert_eq!(shard.page_cache.dirty_page_count(), 0);
    assert!(shard.heap_page_on_disk().iter().all(|&b| b == PAGE_BYTE));
    assert_eq!(shard.published_redo_lsn(), shard.redo_lsn);
}

/// A page whose FullPageImage could not be appended was neither imaged nor
/// written: it is still dirty, so Finalize must re-flush it rather than
/// publish a redo point above its change. The image step is as much a
/// part of the page's flush as the write step.
#[test]
fn checkpoint_does_not_finalize_over_a_page_whose_full_page_image_failed() {
    let mut shard = DirtyPageShard::new();
    fail_next_full_page_image();
    assert!(!shard.tick(), "flush tick (the FPI append fails)");
    assert_eq!(
        shard.page_cache.dirty_page_count(),
        1,
        "precondition: the page whose image failed is still dirty"
    );

    shard.tick_until_finalized();
    assert!(
        shard.heap_page_on_disk().iter().all(|&b| b == PAGE_BYTE),
        "Finalize published redo_lsn {} over a page that was never written: its \
         FullPageImage failed and the checkpoint did not re-flush it",
        shard.redo_lsn
    );
    assert_eq!(shard.page_cache.dirty_page_count(), 0);
    assert_eq!(shard.fpi_records_on_disk(), 1);
    assert_eq!(shard.published_redo_lsn(), shard.redo_lsn);
}

/// A heap file the checkpoint wrote, then deleted before its fsync (the
/// cold-index GC unlinks a file whose last live key went away): nothing
/// can make that inode durable any more and nothing reads it, so it must
/// not hold the redo point back. Before, the fsync helper's `open` failed
/// with `NotFound` on every attempt, the file never left the pending set,
/// and the WAL grew until the disk was full.
#[test]
fn checkpoint_finalizes_when_a_written_heap_file_is_deleted_before_its_fsync() {
    let mut shard = DirtyPageShard::new();
    assert!(!shard.tick(), "flush tick");
    assert!(shard.heap_page_on_disk().iter().all(|&b| b == PAGE_BYTE));
    std::fs::remove_file(&shard.heap_path).unwrap();

    shard.tick_until_finalized();
    assert_eq!(shard.published_redo_lsn(), shard.redo_lsn);
    let vanished = shard.checkpoint_mgr.vanished_data_files();
    assert_eq!(
        (vanished.retired, vanished.still_registered),
        (1, 0),
        "the deleted file is counted once, as a retired file (the manifest does not \
         list it as live)"
    );
}

/// The same file deleted BEFORE the flush reached its dirty page: the
/// page's pwrite fails with `NotFound` on every re-flush. The page belongs
/// to a file that no longer exists, so it leaves the dirty set instead of
/// restarting the flush phase forever.
#[test]
fn checkpoint_finalizes_when_a_dirty_pages_heap_file_is_deleted_before_the_flush() {
    let mut shard = DirtyPageShard::new();
    std::fs::remove_file(&shard.heap_path).unwrap();

    shard.tick_until_finalized();
    assert_eq!(shard.published_redo_lsn(), shard.redo_lsn);
    assert_eq!(shard.page_cache.dirty_page_count(), 0);
    assert!(
        !shard.heap_path.exists(),
        "the checkpoint must not recreate a deleted heap file"
    );
    let vanished = shard.checkpoint_mgr.vanished_data_files();
    assert_eq!((vanished.retired, vanished.still_registered), (1, 0));
}

/// A heap file that vanished while the manifest still lists it as live was
/// removed by something other than the GC — the keys in it are gone
/// whatever the checkpoint does. It still must not wedge the checkpoint,
/// but it is counted separately (and logged as an error) so the loss is
/// never silent.
#[test]
fn a_vanished_heap_file_the_manifest_still_lists_is_counted_as_unexpected() {
    let mut shard = DirtyPageShard::new();
    shard.register_heap_file_as_live();
    assert!(!shard.tick(), "flush tick");
    std::fs::remove_file(&shard.heap_path).unwrap();

    shard.tick_until_finalized();
    let vanished = shard.checkpoint_mgr.vanished_data_files();
    assert_eq!((vanished.retired, vanished.still_registered), (0, 1));
}

/// The cold-tier GC's own sequence: the file was live, the GC tombstones
/// its manifest entry and unlinks it. That is the expected cause, counted
/// as retired.
#[test]
fn a_heap_file_the_gc_tombstoned_and_unlinked_is_counted_as_retired() {
    let mut shard = DirtyPageShard::new();
    shard.register_heap_file_as_live();
    assert!(!shard.tick(), "flush tick");
    shard.manifest.remove_file(1, PageType::KvLeaf);
    std::fs::remove_file(&shard.heap_path).unwrap();

    shard.tick_until_finalized();
    assert_eq!(shard.published_redo_lsn(), shard.redo_lsn);
    let vanished = shard.checkpoint_mgr.vanished_data_files();
    assert_eq!((vanished.retired, vanished.still_registered), (1, 0));
}

/// Calls of [`hanging_data_sync`] and the gate that releases them. Only the
/// test below uses them.
static HANGING_SYNC_CALLS: AtomicUsize = AtomicUsize::new(0);
static HANGING_SYNC_RELEASED: parking_lot::Mutex<bool> = parking_lot::Mutex::new(false);
static HANGING_SYNC_CV: parking_lot::Condvar = parking_lot::Condvar::new();

/// A data-file fsync that hangs like a dead disk until the test releases it
/// (capped at 60 s so a broken test cannot leak a thread forever).
fn hanging_data_sync(_file: &std::fs::File) -> std::io::Result<()> {
    HANGING_SYNC_CALLS.fetch_add(1, Ordering::SeqCst);
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut released = HANGING_SYNC_RELEASED.lock();
    while !*released {
        if HANGING_SYNC_CV
            .wait_until(&mut released, deadline)
            .timed_out()
        {
            break;
        }
    }
    Ok(())
}

/// On a hung disk the data-file fsync never returns. Finalize must neither
/// block the shard thread waiting for it nor start a second helper while
/// the first is outstanding: every attempt returns at once ("pending"), and
/// exactly one helper exists however many attempts run. When the disk
/// comes back, that one helper's result completes the checkpoint.
#[test]
fn finalize_never_blocks_on_the_data_sync_and_never_runs_two_helpers() {
    let mut shard = DirtyPageShard::new();
    shard.checkpoint_mgr.set_data_sync_fn(hanging_data_sync);
    assert!(!shard.tick(), "flush tick");

    let mut slowest = Duration::ZERO;
    let started = Instant::now();
    let mut attempts = 0;
    // Long enough for several finalize retries were an attempt to fail and
    // back off (50 ms, 100 ms, 200 ms, 400 ms). Once a tick HAS blocked,
    // keep going past a second blocking attempt so the report also shows
    // whether a second helper was started.
    let blocked = Duration::from_millis(250);
    while (started.elapsed() < Duration::from_millis(1200)
        || (slowest >= blocked && started.elapsed() < Duration::from_secs(12)))
        && attempts < 4000
    {
        let t = Instant::now();
        assert!(
            !shard.tick(),
            "the checkpoint finalized over an unsynced heap file"
        );
        slowest = slowest.max(t.elapsed());
        attempts += 1;
        std::thread::sleep(Duration::from_millis(5));
    }
    let helpers = HANGING_SYNC_CALLS.load(Ordering::SeqCst);

    *HANGING_SYNC_RELEASED.lock() = true;
    HANGING_SYNC_CV.notify_all();

    assert!(
        slowest < blocked && helpers == 1,
        "a checkpoint tick blocked the shard thread for {slowest:?} waiting on the \
         data-file fsync, and {helpers} fsync helper thread(s) were started over \
         {attempts} ticks (want: every tick returns at once, exactly 1 helper)"
    );

    shard.tick_until_finalized();
    assert_eq!(shard.published_redo_lsn(), shard.redo_lsn);
    assert_eq!(HANGING_SYNC_CALLS.load(Ordering::SeqCst), 1);
}

/// The shutdown checkpoint drives the whole checkpoint synchronously: it
/// must still complete when the data-file fsync runs on the off-loop helper
/// — here a slow one, far slower than the forced checkpoint's whole tick
/// budget, so a loop that only polled would give up before the fsync
/// reported.
#[test]
fn forced_checkpoint_completes_through_the_off_loop_data_sync() {
    let mut shard = DirtyPageShard::new();
    // Leave `begin` to force_checkpoint.
    shard.checkpoint_mgr = CheckpointManager::new(CheckpointTrigger::new(300, u64::MAX, 0.9));
    shard.checkpoint_mgr.set_data_sync_fn(|file| {
        std::thread::sleep(Duration::from_millis(400));
        file.sync_data()
    });
    let redo = shard.wal.current_lsn();
    force_checkpoint(
        ForcedCheckpoint::Shutdown,
        &mut shard.checkpoint_mgr,
        &shard.page_cache,
        &mut shard.wal,
        &mut shard.manifest,
        &mut shard.control,
        &shard.control_path,
        0,
        2,
        300,
        &mut |_| true,
    );
    assert!(!shard.checkpoint_mgr.is_active());
    assert_eq!(shard.published_redo_lsn(), redo);
    assert!(shard.heap_page_on_disk().iter().all(|&b| b == PAGE_BYTE));
}

/// Once a data-file fsync has failed on this shard, no checkpoint may
/// publish a redo point again — even when every file now syncs fine.
#[test]
fn checkpoint_never_publishes_redo_after_a_data_file_fsync_error() {
    let mut shard = DirtyPageShard::new();
    assert!(!shard.tick(), "flush tick");
    shard
        .checkpoint_mgr
        .data_sync_poison()
        .store(true, std::sync::atomic::Ordering::Release);
    assert!(!shard.tick_for(Duration::from_millis(300)));
    assert_eq!(shard.published_redo_lsn(), 0);
    assert!(shard.checkpoint_mgr.is_active());
}

/// An fsync that RETURNS an error, reached through the real tick: the shard
/// is poisoned and the redo point never moves.
#[test]
fn a_data_file_fsync_error_poisons_the_checkpoint() {
    let mut shard = DirtyPageShard::new();
    shard
        .checkpoint_mgr
        .set_data_sync_fn(|_| Err(std::io::Error::other("EIO")));
    assert!(!shard.tick(), "flush tick");
    assert!(!shard.tick_for(Duration::from_millis(300)));
    assert!(shard.checkpoint_mgr.is_data_sync_poisoned());
    assert_eq!(shard.published_redo_lsn(), 0);
}

/// The FPI payload layout the WAL replay decodes: file_id, page_offset,
/// flag, image — compressed only when that is smaller.
#[test]
fn full_page_image_payload_layout() {
    let small = full_page_image_payload(7, 3, &[1, 2, 3]);
    assert_eq!(&small[..8], &7u64.to_le_bytes());
    assert_eq!(&small[8..16], &3u64.to_le_bytes());
    assert_eq!(small[16], 0x00);
    assert_eq!(&small[17..], &[1, 2, 3]);

    let page = vec![0xAB; 4096];
    let big = full_page_image_payload(1, 0, &page);
    assert_eq!(big[16], 0x01, "a uniform page compresses");
    assert_eq!(
        lz4_flex::decompress_size_prepended(&big[17..]).unwrap(),
        page
    );
}

/// Calls of [`ceiling_hanging_sync`] and its release flag. Only the WAL
/// ceiling test uses them.
static CEILING_SYNC_CALLS: AtomicUsize = AtomicUsize::new(0);
static CEILING_SYNC_RELEASED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// A data-file fsync that hangs like a dead disk until the test releases it
/// (capped at 60 s so a broken test cannot leak a thread forever).
fn ceiling_hanging_sync(_file: &std::fs::File) -> std::io::Result<()> {
    CEILING_SYNC_CALLS.fetch_add(1, Ordering::SeqCst);
    let deadline = Instant::now() + Duration::from_secs(60);
    while !CEILING_SYNC_RELEASED.load(Ordering::Acquire) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    Ok(())
}

/// The WAL-ceiling trigger runs on the shard thread WHILE IT SERVES
/// CLIENTS. On a hung disk it must not wait for the heap data-file fsync:
/// it leaves the checkpoint to the periodic tick and returns. A second
/// trigger while that fsync is still outstanding must neither wait nor
/// start a second helper. And its emergency recycle may only cut below the
/// redo point actually published, which the pending checkpoint has not
/// moved.
///
/// Asserted logically, not by wall time: the syncer counts every blocking
/// wait it performs, and the hanging fsync is released only after both
/// triggers have returned.
#[test]
fn the_wal_ceiling_trigger_never_waits_on_the_data_file_fsync() {
    // Ceiling of one byte: any WAL on disk is over it.
    let mut shard = DirtyPageShard::with_wal_bounds(WalBounds::new(0, 1));
    // Leave `begin` to the trigger.
    shard.checkpoint_mgr = CheckpointManager::new(CheckpointTrigger::new(300, u64::MAX, 0.9));
    shard.checkpoint_mgr.set_data_sync_fn(ceiling_hanging_sync);
    let published_before = shard.published_redo_lsn();

    let trigger = |shard: &mut DirtyPageShard| {
        maybe_force_checkpoint_on_wal_overflow(
            &mut shard.checkpoint_mgr,
            &mut shard.wal,
            &shard.page_cache,
            &mut shard.manifest,
            &mut shard.control,
            &shard.control_path,
            0,
            Instant::now() - Duration::from_secs(3600),
            0,
            &mut |_| true,
        )
    };
    assert!(
        trigger(&mut shard),
        "precondition: the WAL is over its ceiling"
    );
    let waits_after_first = shard.checkpoint_mgr.data_sync_blocking_waits();
    assert!(trigger(&mut shard));
    let waits_after_second = shard.checkpoint_mgr.data_sync_blocking_waits();
    let still_outstanding = shard.checkpoint_mgr.data_sync_busy();
    let helpers = shard.checkpoint_mgr.data_sync_helpers_started();
    let in_memory_redo = shard.control.last_checkpoint_lsn;

    CEILING_SYNC_RELEASED.store(true, Ordering::Release);

    assert!(
        waits_after_first == 0 && waits_after_second == 0,
        "the WAL-ceiling trigger blocked the serving shard thread on the heap data-file \
         fsync ({waits_after_first} blocking wait(s) after the first trigger, \
         {waits_after_second} after the second); it must leave the checkpoint to the \
         periodic tick"
    );
    assert!(
        still_outstanding,
        "precondition: the fsync was still hung when both triggers returned"
    );
    assert_eq!(helpers, 1, "a second trigger started a second fsync helper");
    assert_eq!(
        in_memory_redo, published_before,
        "the pending checkpoint moved the redo point the emergency recycle cuts below"
    );
    assert_eq!(shard.published_redo_lsn(), published_before);

    // The periodic tick finishes the checkpoint once the disk answers.
    shard.tick_until_finalized();
    assert!(shard.published_redo_lsn() > published_before);
    assert_eq!(CEILING_SYNC_CALLS.load(Ordering::SeqCst), 1);
}

/// A Finalize whose control-file write fails has NOT published its redo
/// point, so the in-memory control copy must keep the old one: the WAL
/// ceiling's emergency recycle (and every other reader) cuts the WAL below
/// `control.last_checkpoint_lsn`, and cutting below an unpublished redo
/// point deletes WAL that recovery still starts from.
#[test]
fn a_failed_control_file_write_leaves_the_in_memory_redo_point_unpublished() {
    let mut shard = DirtyPageShard::new();
    let before = shard.control.clone();
    // A directory where the control file goes: the atomic rename onto it
    // fails, after the fsync of the heap file has succeeded.
    std::fs::remove_file(&shard.control_path).unwrap();
    std::fs::create_dir(&shard.control_path).unwrap();

    assert!(!shard.tick_for(Duration::from_millis(300)));
    assert_eq!(
        (
            shard.control.last_checkpoint_lsn,
            shard.control.last_checkpoint_epoch,
            shard.control.graph_floor_lsn
        ),
        (
            before.last_checkpoint_lsn,
            before.last_checkpoint_epoch,
            before.graph_floor_lsn
        ),
        "the control file write failed, yet the in-memory copy claims redo_lsn {} is \
         published",
        shard.redo_lsn
    );

    std::fs::remove_dir(&shard.control_path).unwrap();
    shard.tick_until_finalized();
    assert_eq!(shard.published_redo_lsn(), shard.redo_lsn);
    assert_eq!(shard.control.last_checkpoint_lsn, shard.redo_lsn);
}
