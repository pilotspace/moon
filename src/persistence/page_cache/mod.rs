//! PageCache buffer manager with clock-sweep eviction.
//!
//! Manages both 4KB and 64KB page frames with:
//! - Lock-free pin/unpin via packed AtomicU32 state
//! - Clock-sweep eviction respecting pinned frames
//! - WAL-before-data invariant enforcement at flush time
//! - DashMap page table for O(1) page lookup

pub mod eviction;
pub mod frame;

pub use eviction::ClockSweep;
pub use frame::{FrameDescriptor, FrameState};

use std::sync::atomic::Ordering;

use dashmap::DashMap;
use parking_lot::RwLock;
use smallvec::SmallVec;

use crate::persistence::page::PAGE_4K;
use crate::persistence::page::PAGE_64K;

use self::frame::FLAG_DIRTY;
use self::frame::FLAG_FPI_PENDING;

/// Handle returned by `fetch_page` representing a pinned page in the cache.
///
/// The caller MUST call `PageCache::unpin_page` when done with the page.
/// Failing to unpin will prevent eviction (memory leak in the buffer pool).
pub struct PageHandle {
    /// Index into the frame descriptor array.
    pub frame_index: u32,
    /// Whether this is a large (64KB) frame.
    pub is_large: bool,
}

/// Unified buffer manager for all disk-resident pages.
///
/// Supports two frame pools:
/// - 4KB pool: KV, graph, MVCC, metadata, control pages
/// - 64KB pool: VecCodes, VecFull pages
///
/// The WAL-before-data invariant is enforced at flush time: `flush_page`
/// calls the provided `wal_flush_fn` with the page's LSN before writing
/// dirty data to disk.
pub struct PageCache {
    /// Frame descriptors for 4KB pages.
    frames_4k: Vec<FrameDescriptor>,
    /// Buffers for 4KB pages, each protected by RwLock.
    buffers_4k: Vec<RwLock<Vec<u8>>>,
    /// Frame descriptors for 64KB pages.
    frames_64k: Vec<FrameDescriptor>,
    /// Buffers for 64KB pages, each protected by RwLock.
    buffers_64k: Vec<RwLock<Vec<u8>>>,
    /// Page table: (file_id, page_offset) -> (frame_index, is_large).
    page_table: DashMap<(u64, u64), (u32, bool)>,
    /// Clock-sweep for 4KB pool.
    sweep_4k: ClockSweep,
    /// Clock-sweep for 64KB pool.
    sweep_64k: ClockSweep,
    /// Running total of bytes committed across all page buffers.
    ///
    /// Buffers are lazily grown from empty to exactly their pool's page size
    /// on first use (`fetch_page` miss path) and NEVER shrink — eviction only
    /// clears the VALID flag, keeping the allocation for reuse. That makes a
    /// single monotonic counter, bumped at the one grow site, exactly
    /// equivalent to walking every buffer — without the per-frame lock
    /// acquisition that made `resident_buffer_bytes` the #1 idle-CPU consumer
    /// (called every 100ms per shard from the eviction tick).
    resident_bytes: std::sync::atomic::AtomicUsize,
}

/// Split a per-shard PageCache budget (bytes) into (4KB, 64KB) frame counts.
///
/// 75% of the budget backs 4KB frames, 25% backs 64KB frames, with minimum
/// floors of 64 / 8 frames so a tiny budget still yields a usable cache. With
/// lazy buffers these are only *capacities* — actual memory grows on demand.
pub fn pagecache_frame_counts(budget_bytes: u64) -> (usize, usize) {
    let num_4k = ((budget_bytes * 3 / 4) / PAGE_4K as u64) as usize;
    let num_64k = ((budget_bytes / 4) / PAGE_64K as u64) as usize;
    (num_4k.max(64), num_64k.max(8))
}

/// Divide a whole-instance PageCache budget across shards.
///
/// `--pagecache-size` (and the 25%-of-maxmemory default) express a
/// whole-instance intent, but each shard builds its own PageCache. Sizing every
/// shard to the whole budget over-committed by `num_shards`× — the multishard
/// "zombie eating RAM". Dividing here bounds total pre-allocation to the budget
/// regardless of shard count. `num_shards == 0` is treated as 1 (never panics).
pub fn per_shard_pagecache_budget(whole_budget_bytes: u64, num_shards: usize) -> u64 {
    whole_budget_bytes / (num_shards.max(1) as u64)
}

impl PageCache {
    /// Create a new PageCache with pre-allocated frame pools.
    ///
    /// - `num_frames_4k`: number of 4KB frame slots
    /// - `num_frames_64k`: number of 64KB frame slots
    pub fn new(num_frames_4k: usize, num_frames_64k: usize) -> Self {
        let frames_4k: Vec<FrameDescriptor> =
            (0..num_frames_4k).map(|_| FrameDescriptor::new()).collect();
        // Lazy buffers: start EMPTY (zero heap), grown to a full page on first
        // use in `fetch_page`. Eagerly committing `num_frames * PAGE` zeroed
        // bytes here was the multishard "zombie eating RAM" — a 4-shard server
        // with the auto memory guardrail pre-committed ≈80% of host RAM at
        // startup before serving a command. RSS now tracks the working set.
        let buffers_4k: Vec<RwLock<Vec<u8>>> = (0..num_frames_4k)
            .map(|_| RwLock::new(Vec::new()))
            .collect();

        let frames_64k: Vec<FrameDescriptor> = (0..num_frames_64k)
            .map(|_| FrameDescriptor::new())
            .collect();
        let buffers_64k: Vec<RwLock<Vec<u8>>> = (0..num_frames_64k)
            .map(|_| RwLock::new(Vec::new()))
            .collect();

        Self {
            frames_4k,
            buffers_4k,
            frames_64k,
            buffers_64k,
            page_table: DashMap::new(),
            sweep_4k: ClockSweep::new(num_frames_4k),
            sweep_64k: ClockSweep::new(num_frames_64k),
            resident_bytes: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Total bytes currently committed across all page buffers.
    ///
    /// With lazy buffers this reflects the actual resident working set, not the
    /// configured budget — a freshly constructed cache returns 0. O(1): a
    /// relaxed load of the running counter maintained at the buffer grow site
    /// (see `resident_bytes`); safe to call at eviction-tick cadence.
    pub fn resident_buffer_bytes(&self) -> usize {
        self.resident_bytes.load(Ordering::Relaxed)
    }

    /// Fetch a page into the cache and return a pinned handle.
    ///
    /// On cache hit: pins the frame, touches usage count, returns handle.
    /// On cache miss: evicts a victim (flushing if dirty), reads from disk
    /// via `read_fn`, pins the new frame, returns handle.
    ///
    /// `read_fn` is called with a mutable buffer slice that should be filled
    /// with the page data from disk. It is only called on cache miss.
    ///
    /// # Errors
    ///
    /// Returns `Err` if:
    /// - `read_fn` fails (I/O error reading page from disk)
    /// - No victim frame can be found (all frames pinned)
    pub fn fetch_page(
        &self,
        file_id: u64,
        page_offset: u64,
        is_large: bool,
        read_fn: impl FnOnce(&mut [u8]) -> std::io::Result<()>,
    ) -> std::io::Result<PageHandle> {
        let key = (file_id, page_offset);

        // Cache hit path
        if let Some(entry) = self.page_table.get(&key) {
            let (frame_idx, large) = *entry;
            let frames = if large {
                &self.frames_64k
            } else {
                &self.frames_4k
            };
            frames[frame_idx as usize].state.pin();
            frames[frame_idx as usize].state.touch();
            return Ok(PageHandle {
                frame_index: frame_idx,
                is_large: large,
            });
        }

        // Cache miss — find a victim
        let (frames, buffers, sweep) = if is_large {
            (&self.frames_64k, &self.buffers_64k, &self.sweep_64k)
        } else {
            (&self.frames_4k, &self.buffers_4k, &self.sweep_4k)
        };

        let victim_idx = sweep
            .find_victim(frames)
            .ok_or_else(|| std::io::Error::other("page cache full: all frames pinned"))?;

        let victim = &frames[victim_idx];

        // If victim had a valid page, remove it from the page table
        let old_file_id = victim.file_id.load(Ordering::Acquire);
        let old_offset = victim.page_offset.load(Ordering::Acquire);
        let old_state = victim.state.load();
        let (_, _, old_flags) = FrameState::unpack(old_state);
        if old_flags & frame::FLAG_VALID != 0 {
            self.page_table.remove(&(old_file_id, old_offset));
        }

        // Reset frame for new page
        victim.reset(file_id, page_offset);

        // Read page data from disk. Lazily commit this frame's buffer to a full
        // page on first use (it starts empty from `new`); a reused frame already
        // has the right length so this is a no-op after the first miss.
        let page_size = if is_large { PAGE_64K } else { PAGE_4K };
        {
            let mut buf = buffers[victim_idx].write();
            if buf.len() != page_size {
                // Sole buffer grow site: count the newly committed bytes even
                // if `read_fn` fails below — the allocation stays either way.
                self.resident_bytes
                    .fetch_add(page_size - buf.len(), Ordering::Relaxed);
                buf.resize(page_size, 0);
            }
            read_fn(&mut buf)?;
        }

        // Mark valid, pin, touch
        victim.state.set_valid();
        victim.state.pin();
        victim.state.touch();

        // Insert into page table
        self.page_table.insert(key, (victim_idx as u32, is_large));

        Ok(PageHandle {
            frame_index: victim_idx as u32,
            is_large,
        })
    }

    /// Get a read reference to the page data for a pinned handle.
    ///
    /// The caller must hold a valid pin (via `fetch_page`).
    pub fn page_data(&self, handle: &PageHandle) -> parking_lot::RwLockReadGuard<'_, Vec<u8>> {
        let buffers = if handle.is_large {
            &self.buffers_64k
        } else {
            &self.buffers_4k
        };
        buffers[handle.frame_index as usize].read()
    }

    /// Get a write reference to the page data for a pinned handle.
    ///
    /// The caller must hold a valid pin (via `fetch_page`).
    pub fn page_data_mut(&self, handle: &PageHandle) -> parking_lot::RwLockWriteGuard<'_, Vec<u8>> {
        let buffers = if handle.is_large {
            &self.buffers_64k
        } else {
            &self.buffers_4k
        };
        buffers[handle.frame_index as usize].write()
    }

    /// Mark a cached page as dirty and update its LSN.
    ///
    /// The page must already be in the cache. If not found, this is a no-op.
    pub fn mark_dirty(&self, file_id: u64, page_offset: u64, lsn: u64) {
        if let Some(entry) = self.page_table.get(&(file_id, page_offset)) {
            let (frame_idx, is_large) = *entry;
            let frames = if is_large {
                &self.frames_64k
            } else {
                &self.frames_4k
            };
            let frame = &frames[frame_idx as usize];
            frame.state.set_dirty();
            frame.page_lsn.store(lsn, Ordering::Release);
        }
    }

    /// Flush a dirty page to disk, enforcing the WAL-before-data invariant.
    ///
    /// Steps:
    /// 1. Look up the frame in the page table
    /// 2. Read the page's LSN
    /// 3. Call `wal_flush_fn(page_lsn)` to ensure WAL is flushed up to that LSN
    /// 4. Call `write_fn` with the buffer data to write the page to disk
    /// 5. Clear the DIRTY flag
    ///
    /// # Errors
    ///
    /// Returns `Err` if the WAL flush or disk write fails, or if the page
    /// is not in the cache.
    pub fn flush_page(
        &self,
        file_id: u64,
        page_offset: u64,
        wal_flush_fn: impl FnOnce(u64) -> std::io::Result<()>,
        write_fn: impl FnOnce(&[u8]) -> std::io::Result<()>,
    ) -> std::io::Result<()> {
        let entry = self
            .page_table
            .get(&(file_id, page_offset))
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotFound, "page not in cache")
            })?;

        let (frame_idx, is_large) = *entry;
        let frames = if is_large {
            &self.frames_64k
        } else {
            &self.frames_4k
        };
        let buffers = if is_large {
            &self.buffers_64k
        } else {
            &self.buffers_4k
        };

        let frame = &frames[frame_idx as usize];
        let page_lsn = frame.page_lsn.load(Ordering::Acquire);

        // WAL-before-data invariant: flush WAL up to this page's LSN
        wal_flush_fn(page_lsn)?;

        // Write page data to disk
        {
            let buf = buffers[frame_idx as usize].read();
            write_fn(&buf)?;
        }

        // Clear dirty flag
        frame.state.clear_dirty();

        Ok(())
    }

    /// Unpin a previously pinned page.
    ///
    /// Must be called exactly once for each successful `fetch_page` call.
    pub fn unpin_page(&self, handle: PageHandle) {
        let frames = if handle.is_large {
            &self.frames_64k
        } else {
            &self.frames_4k
        };
        frames[handle.frame_index as usize].state.unpin();
    }

    /// Explicitly evict up to `max_frames` unpinned, non-dirty frames using clock-sweep.
    ///
    /// Returns the number of frames evicted. Used by memory pressure cascade
    /// to proactively free PageCache memory before resorting to KV eviction.
    pub fn evict_cold_frames(&self, max_frames: usize) -> usize {
        let mut evicted = 0;
        // Sweep 4KB frames first (more numerous, smaller payoff per frame)
        for _ in 0..max_frames {
            if evicted >= max_frames {
                break;
            }
            if let Some(victim_idx) = self.sweep_4k.find_victim(&self.frames_4k) {
                let frame = &self.frames_4k[victim_idx];
                let val = frame.state.load();
                let (_, _, flags) = FrameState::unpack(val);
                // Only evict non-dirty, valid frames
                if flags & FLAG_DIRTY == 0 && flags & frame::FLAG_VALID != 0 {
                    let old_fid = frame.file_id.load(Ordering::Acquire);
                    let old_off = frame.page_offset.load(Ordering::Acquire);
                    self.page_table.remove(&(old_fid, old_off));
                    frame.state.clear_valid();
                    evicted += 1;
                }
            }
        }
        // Sweep 64KB frames (fewer but larger payoff per frame)
        for _ in 0..max_frames {
            if evicted >= max_frames {
                break;
            }
            if let Some(victim_idx) = self.sweep_64k.find_victim(&self.frames_64k) {
                let frame = &self.frames_64k[victim_idx];
                let val = frame.state.load();
                let (_, _, flags) = FrameState::unpack(val);
                if flags & FLAG_DIRTY == 0 && flags & frame::FLAG_VALID != 0 {
                    let old_fid = frame.file_id.load(Ordering::Acquire);
                    let old_off = frame.page_offset.load(Ordering::Acquire);
                    self.page_table.remove(&(old_fid, old_off));
                    frame.state.clear_valid();
                    evicted += 1;
                }
            }
        }
        evicted
    }

    /// Count the number of dirty pages across both pools.
    ///
    /// Used by checkpoint logic to determine how many pages need flushing.
    pub fn dirty_page_count(&self) -> usize {
        let mut count = 0;
        for frame in &self.frames_4k {
            let val = frame.state.load();
            let (_, _, flags) = FrameState::unpack(val);
            if flags & FLAG_DIRTY != 0 {
                count += 1;
            }
        }
        for frame in &self.frames_64k {
            let val = frame.state.load();
            let (_, _, flags) = FrameState::unpack(val);
            if flags & FLAG_DIRTY != 0 {
                count += 1;
            }
        }
        count
    }

    /// Set FPI_PENDING on all valid frames (called at checkpoint BEGIN).
    ///
    /// After this call, every valid page will require a full-page image written
    /// to WAL before its first flush in the checkpoint cycle — torn-page defense.
    pub fn arm_all_fpi_pending(&self) {
        for frame in &self.frames_4k {
            let val = frame.state.load();
            let (_, _, flags) = FrameState::unpack(val);
            if flags & frame::FLAG_VALID != 0 {
                frame.state.set_fpi_pending();
            }
        }
        for frame in &self.frames_64k {
            let val = frame.state.load();
            let (_, _, flags) = FrameState::unpack(val);
            if flags & frame::FLAG_VALID != 0 {
                frame.state.set_fpi_pending();
            }
        }
    }

    /// Flush up to `max_pages` dirty pages to disk, enforcing WAL-before-data.
    ///
    /// Iterates both frame pools (4KB then 64KB), finds dirty+valid frames,
    /// and flushes each. Returns the number of pages actually flushed.
    ///
    /// `wal_flush_fn` is called once per dirty page with that page's LSN to ensure
    /// WAL durability before the page write. `write_fn` receives (file_id, page_offset,
    /// is_large, data) for the actual disk write.
    pub fn flush_dirty_pages(
        &self,
        max_pages: usize,
        wal_flush_fn: &mut impl FnMut(u64) -> std::io::Result<()>,
        write_fn: &mut impl FnMut(u64, u64, bool, &[u8]) -> std::io::Result<()>,
    ) -> usize {
        let mut flushed = 0;
        // Scan 4KB frames
        for (idx, frame) in self.frames_4k.iter().enumerate() {
            if flushed >= max_pages {
                break;
            }
            let val = frame.state.load();
            let (_, _, flags) = FrameState::unpack(val);
            if flags & FLAG_DIRTY != 0 && flags & frame::FLAG_VALID != 0 {
                let file_id = frame.file_id.load(Ordering::Acquire);
                let page_offset = frame.page_offset.load(Ordering::Acquire);
                let page_lsn = frame.page_lsn.load(Ordering::Acquire);
                // WAL-before-data: ensure WAL durable past this page's LSN
                if let Err(e) = wal_flush_fn(page_lsn) {
                    tracing::error!("WAL flush for dirty page failed: {}", e);
                    continue;
                }
                // Write page data to disk
                {
                    let buf = self.buffers_4k[idx].read();
                    if let Err(e) = write_fn(file_id, page_offset, false, &buf) {
                        tracing::error!(
                            "Dirty page write failed: file_id={}, offset={}: {}",
                            file_id,
                            page_offset,
                            e
                        );
                        continue;
                    }
                }
                // Clear dirty flag
                frame.state.clear_dirty();
                flushed += 1;
            }
        }
        // Scan 64KB frames
        for (idx, frame) in self.frames_64k.iter().enumerate() {
            if flushed >= max_pages {
                break;
            }
            let val = frame.state.load();
            let (_, _, flags) = FrameState::unpack(val);
            if flags & FLAG_DIRTY != 0 && flags & frame::FLAG_VALID != 0 {
                let file_id = frame.file_id.load(Ordering::Acquire);
                let page_offset = frame.page_offset.load(Ordering::Acquire);
                let page_lsn = frame.page_lsn.load(Ordering::Acquire);
                if let Err(e) = wal_flush_fn(page_lsn) {
                    tracing::error!("WAL flush for dirty page failed: {}", e);
                    continue;
                }
                {
                    let buf = self.buffers_64k[idx].read();
                    if let Err(e) = write_fn(file_id, page_offset, true, &buf) {
                        tracing::error!(
                            "Dirty page write failed: file_id={}, offset={}: {}",
                            file_id,
                            page_offset,
                            e
                        );
                        continue;
                    }
                }
                frame.state.clear_dirty();
                flushed += 1;
            }
        }
        flushed
    }

    /// FPI-aware variant of `flush_dirty_pages`, one batch of up to
    /// `max_pages` dirty pages per call.
    ///
    /// The batch runs in three phases, so the WAL durability barrier is paid
    /// ONCE per batch instead of once per page:
    ///   1. For every FPI_PENDING page in the batch: append its full-page
    ///      image via `fpi_fn`. A page whose image fails leaves the batch.
    ///   2. Call `wal_flush_fn` ONCE with the highest `page_lsn` in the batch.
    ///      The caller must make the WAL durable through that LSN AND through
    ///      every FPI record it appended in phase 1. If it fails, no page of
    ///      the batch is written.
    ///   3. Write every remaining page via `write_fn`; only after its write
    ///      succeeds are its FPI_PENDING and DIRTY flags cleared.
    ///
    /// Log-before-data per page is unchanged: no page is written before the
    /// barrier that covers both its own change and its image has returned.
    ///
    /// Crash-safety invariants:
    /// - Each page's buffer read-lock is held from its FPI snapshot through
    ///   its data write — concurrent writers cannot mutate the buffer in
    ///   between, so the FPI on disk always matches the data page on disk.
    /// - A page whose image, barrier or write failed keeps DIRTY and
    ///   FPI_PENDING, so the next flush re-emits the image and torn-page
    ///   protection is preserved. It is counted in [`FlushOutcome::failed`].
    pub fn flush_dirty_pages_with_fpi(
        &self,
        max_pages: usize,
        wal_flush_fn: &mut impl FnMut(u64) -> std::io::Result<()>,
        fpi_fn: &mut impl FnMut(u64, u64, bool, &[u8]) -> std::io::Result<()>,
        write_fn: &mut impl FnMut(u64, u64, bool, &[u8]) -> std::io::Result<()>,
    ) -> FlushOutcome {
        let mut batch: SmallVec<[FlushCandidate<'_>; FLUSH_BATCH_INLINE]> = SmallVec::new();
        collect_dirty(
            &self.frames_4k,
            &self.buffers_4k,
            false,
            max_pages,
            &mut batch,
        );
        collect_dirty(
            &self.frames_64k,
            &self.buffers_64k,
            true,
            max_pages,
            &mut batch,
        );
        let mut outcome = FlushOutcome::default();

        // Phase 1: every image first.
        batch.retain(|c| {
            if !c.needs_fpi {
                return true;
            }
            match fpi_fn(c.file_id, c.page_offset, c.is_large, &c.buf) {
                Ok(()) => true,
                Err(e) => {
                    tracing::error!(
                        "FPI write failed: file_id={}, offset={}: {}",
                        c.file_id,
                        c.page_offset,
                        e
                    );
                    outcome.failed += 1;
                    false
                }
            }
        });
        let Some(upto) = batch.iter().map(|c| c.page_lsn).max() else {
            return outcome;
        };

        // Phase 2: one durability barrier for the whole batch.
        if let Err(e) = wal_flush_fn(upto) {
            tracing::error!("WAL flush for dirty page batch failed: {}", e);
            outcome.failed += batch.len();
            return outcome;
        }

        // Phase 3: the pages.
        for c in batch {
            if let Err(e) = write_fn(c.file_id, c.page_offset, c.is_large, &c.buf) {
                tracing::error!(
                    "Dirty page write failed: file_id={}, offset={}: {}",
                    c.file_id,
                    c.page_offset,
                    e
                );
                outcome.failed += 1;
                continue;
            }
            drop(c.buf);
            if c.needs_fpi {
                c.frame.state.clear_fpi_pending();
            }
            c.frame.state.clear_dirty();
            outcome.flushed += 1;
        }
        outcome
    }

    /// Stop trying to persist the cached pages of heap file `file_id`: clear
    /// DIRTY and FPI_PENDING on every valid frame of that file and return
    /// how many were dirty. For a file that no longer exists on disk, whose
    /// pages can never be written anywhere. The frames stay VALID (a cached
    /// page still serves reads) and become ordinary evictable clean frames.
    pub fn abandon_dirty_pages_of_file(&self, file_id: u64) -> usize {
        let mut abandoned = 0;
        for frame in self.frames_4k.iter().chain(self.frames_64k.iter()) {
            let (_, _, flags) = FrameState::unpack(frame.state.load());
            if flags & frame::FLAG_VALID == 0 || frame.file_id.load(Ordering::Acquire) != file_id {
                continue;
            }
            if flags & FLAG_DIRTY != 0 {
                abandoned += 1;
            }
            frame.state.clear_fpi_pending();
            frame.state.clear_dirty();
        }
        abandoned
    }
}

/// What one [`PageCache::flush_dirty_pages_with_fpi`] batch did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FlushOutcome {
    /// Pages written (DIRTY and FPI_PENDING now clear).
    pub flushed: usize,
    /// Pages taken into the batch but NOT written because their image, the
    /// batch's WAL barrier, or their write failed. Each is still DIRTY and,
    /// if it was, FPI_PENDING.
    pub failed: usize,
}

/// Batch sizes the checkpoint uses (`pages_per_tick` is clamped to 16) stay
/// on the stack.
const FLUSH_BATCH_INLINE: usize = 16;

/// A dirty page taken into a flush batch, with its buffer read-locked from
/// its FPI snapshot through its data write.
struct FlushCandidate<'a> {
    frame: &'a FrameDescriptor,
    buf: parking_lot::RwLockReadGuard<'a, Vec<u8>>,
    file_id: u64,
    page_offset: u64,
    page_lsn: u64,
    is_large: bool,
    needs_fpi: bool,
}

/// Add the dirty, valid frames of one pool to `batch` until it holds
/// `max_pages`.
fn collect_dirty<'a>(
    frames: &'a [FrameDescriptor],
    buffers: &'a [RwLock<Vec<u8>>],
    is_large: bool,
    max_pages: usize,
    batch: &mut SmallVec<[FlushCandidate<'a>; FLUSH_BATCH_INLINE]>,
) {
    for (idx, frame) in frames.iter().enumerate() {
        if batch.len() >= max_pages {
            return;
        }
        let (_, _, flags) = FrameState::unpack(frame.state.load());
        if flags & FLAG_DIRTY == 0 || flags & frame::FLAG_VALID == 0 {
            continue;
        }
        batch.push(FlushCandidate {
            frame,
            buf: buffers[idx].read(),
            file_id: frame.file_id.load(Ordering::Acquire),
            page_offset: frame.page_offset.load(Ordering::Acquire),
            page_lsn: frame.page_lsn.load(Ordering::Acquire),
            is_large,
            needs_fpi: flags & FLAG_FPI_PENDING != 0,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_cache_fetch_and_pin() {
        let cache = PageCache::new(4, 2);
        let handle = cache
            .fetch_page(1, 0, false, |buf| {
                buf[0] = 0xAB;
                Ok(())
            })
            .unwrap();

        // Verify data was read
        {
            let data = cache.page_data(&handle);
            assert_eq!(data[0], 0xAB);
        }

        // Verify frame is pinned (refcount > 0)
        let frame = &cache.frames_4k[handle.frame_index as usize];
        let (rc, _, _) = FrameState::unpack(frame.state.load());
        assert!(rc > 0);

        cache.unpin_page(handle);
    }

    #[test]
    fn test_page_cache_cache_hit() {
        let cache = PageCache::new(4, 2);
        let mut read_count = 0u32;

        // First fetch — cache miss, read_fn called
        let h1 = cache
            .fetch_page(1, 0, false, |buf| {
                read_count += 1;
                buf[0] = 0x42;
                Ok(())
            })
            .unwrap();
        cache.unpin_page(h1);
        assert_eq!(read_count, 1);

        // Second fetch — cache hit, read_fn NOT called
        let h2 = cache
            .fetch_page(1, 0, false, |_buf| {
                panic!("read_fn should not be called on cache hit");
            })
            .unwrap();

        let data = cache.page_data(&h2);
        assert_eq!(data[0], 0x42);
        drop(data);
        cache.unpin_page(h2);
    }

    #[test]
    fn test_page_cache_eviction_on_full() {
        // 2-frame cache
        let cache = PageCache::new(2, 1);

        // Fill both frames
        let h1 = cache
            .fetch_page(1, 0, false, |buf| {
                buf[0] = 0x01;
                Ok(())
            })
            .unwrap();
        cache.unpin_page(h1);

        let h2 = cache
            .fetch_page(2, 0, false, |buf| {
                buf[0] = 0x02;
                Ok(())
            })
            .unwrap();
        cache.unpin_page(h2);

        // Fetch a third page — should evict one of the first two
        let h3 = cache
            .fetch_page(3, 0, false, |buf| {
                buf[0] = 0x03;
                Ok(())
            })
            .unwrap();

        let data = cache.page_data(&h3);
        assert_eq!(data[0], 0x03);
        drop(data);
        cache.unpin_page(h3);

        // Verify page table has the new page
        assert!(cache.page_table.contains_key(&(3, 0)));
    }

    #[test]
    fn test_page_cache_mark_dirty() {
        let cache = PageCache::new(4, 2);
        let h = cache.fetch_page(1, 0, false, |_| Ok(())).unwrap();
        cache.unpin_page(h);

        assert_eq!(cache.dirty_page_count(), 0);

        cache.mark_dirty(1, 0, 100);
        assert_eq!(cache.dirty_page_count(), 1);

        // Verify LSN was updated
        let entry = cache.page_table.get(&(1, 0)).unwrap();
        let (idx, _) = *entry;
        let lsn = cache.frames_4k[idx as usize]
            .page_lsn
            .load(Ordering::Acquire);
        assert_eq!(lsn, 100);
    }

    #[test]
    fn test_page_cache_flush_wal_before_data() {
        use std::sync::atomic::AtomicU64;

        let cache = PageCache::new(4, 2);
        let h = cache.fetch_page(1, 0, false, |_| Ok(())).unwrap();
        cache.unpin_page(h);

        cache.mark_dirty(1, 0, 500);

        let wal_flushed_lsn = AtomicU64::new(0);
        let write_called = std::sync::atomic::AtomicBool::new(false);

        cache
            .flush_page(
                1,
                0,
                |lsn| {
                    wal_flushed_lsn.store(lsn, Ordering::SeqCst);
                    Ok(())
                },
                |_data| {
                    // WAL must have been flushed BEFORE this write
                    assert_eq!(wal_flushed_lsn.load(Ordering::SeqCst), 500);
                    write_called.store(true, Ordering::SeqCst);
                    Ok(())
                },
            )
            .unwrap();

        assert!(write_called.load(Ordering::SeqCst));
        // Dirty flag should be cleared
        assert_eq!(cache.dirty_page_count(), 0);
    }

    #[test]
    fn test_page_cache_mixed_sizes() {
        let cache = PageCache::new(4, 2);

        // Fetch a 4KB page
        let h_small = cache
            .fetch_page(1, 0, false, |buf| {
                assert_eq!(buf.len(), PAGE_4K);
                buf[0] = 0x04;
                Ok(())
            })
            .unwrap();
        assert!(!h_small.is_large);

        // Fetch a 64KB page
        let h_large = cache
            .fetch_page(2, 0, true, |buf| {
                assert_eq!(buf.len(), PAGE_64K);
                buf[0] = 0x64;
                Ok(())
            })
            .unwrap();
        assert!(h_large.is_large);

        // Verify both are readable
        {
            let data_s = cache.page_data(&h_small);
            assert_eq!(data_s[0], 0x04);
            assert_eq!(data_s.len(), PAGE_4K);
        }
        {
            let data_l = cache.page_data(&h_large);
            assert_eq!(data_l[0], 0x64);
            assert_eq!(data_l.len(), PAGE_64K);
        }

        cache.unpin_page(h_small);
        cache.unpin_page(h_large);
    }

    #[test]
    fn test_page_cache_all_pinned_returns_error() {
        let cache = PageCache::new(2, 1);

        // Pin both frames (don't unpin)
        let _h1 = cache.fetch_page(1, 0, false, |_| Ok(())).unwrap();
        let _h2 = cache.fetch_page(2, 0, false, |_| Ok(())).unwrap();

        // Third fetch should fail — all frames pinned
        let result = cache.fetch_page(3, 0, false, |_| Ok(()));
        assert!(result.is_err());
    }

    #[test]
    fn test_flush_dirty_pages_basic() {
        use std::sync::atomic::AtomicU64;
        let cache = PageCache::new(4, 2);

        // Load 3 pages, mark 2 dirty
        let h1 = cache.fetch_page(1, 0, false, |_| Ok(())).unwrap();
        cache.unpin_page(h1);
        let h2 = cache.fetch_page(2, 0, false, |_| Ok(())).unwrap();
        cache.unpin_page(h2);
        let h3 = cache.fetch_page(3, 0, false, |_| Ok(())).unwrap();
        cache.unpin_page(h3);

        cache.mark_dirty(1, 0, 100);
        cache.mark_dirty(3, 0, 300);
        assert_eq!(cache.dirty_page_count(), 2);

        let wal_max_lsn = AtomicU64::new(0);
        let mut write_count = 0u32;

        let flushed = cache.flush_dirty_pages(
            10,
            &mut |lsn| {
                wal_max_lsn.fetch_max(lsn, Ordering::SeqCst);
                Ok(())
            },
            &mut |_file_id, _offset, _large, _data| {
                write_count += 1;
                Ok(())
            },
        );

        assert_eq!(flushed, 2);
        assert_eq!(write_count, 2);
        assert_eq!(cache.dirty_page_count(), 0);
        // WAL should have been flushed to at least LSN 300
        assert!(wal_max_lsn.load(Ordering::SeqCst) >= 300);
    }

    #[test]
    fn test_flush_dirty_pages_respects_max() {
        let cache = PageCache::new(4, 2);

        for i in 0..4u64 {
            let h = cache.fetch_page(i, 0, false, |_| Ok(())).unwrap();
            cache.unpin_page(h);
            cache.mark_dirty(i, 0, i * 100);
        }
        assert_eq!(cache.dirty_page_count(), 4);

        let flushed = cache.flush_dirty_pages(2, &mut |_| Ok(()), &mut |_, _, _, _| Ok(()));

        assert_eq!(flushed, 2);
        assert_eq!(cache.dirty_page_count(), 2);
    }

    #[test]
    fn test_arm_all_fpi_pending_sets_on_valid_frames() {
        let cache = PageCache::new(4, 2);

        // Fetch 2 pages (makes them VALID)
        let h1 = cache.fetch_page(1, 0, false, |_| Ok(())).unwrap();
        cache.unpin_page(h1);
        let h2 = cache.fetch_page(2, 0, false, |_| Ok(())).unwrap();
        cache.unpin_page(h2);

        // No frames should have FPI_PENDING yet
        for frame in &cache.frames_4k {
            assert!(!frame.state.is_fpi_pending());
        }

        // Checkpoint begin: set FPI on all valid frames
        cache.arm_all_fpi_pending();

        // The 2 valid frames should have FPI_PENDING
        let mut fpi_count = 0;
        for frame in &cache.frames_4k {
            let val = frame.state.load();
            let (_, _, flags) = FrameState::unpack(val);
            if flags & frame::FLAG_VALID != 0 {
                assert!(frame.state.is_fpi_pending());
                fpi_count += 1;
            }
        }
        assert_eq!(fpi_count, 2);
    }

    #[test]
    fn test_flush_dirty_pages_with_fpi_calls_fpi_fn() {
        use std::cell::Cell;

        let cache = PageCache::new(4, 2);

        // Fetch, dirty, and set FPI_PENDING on a page
        let h = cache
            .fetch_page(1, 0, false, |buf| {
                buf[0] = 0xCC;
                Ok(())
            })
            .unwrap();
        cache.unpin_page(h);
        cache.mark_dirty(1, 0, 100);

        // Simulate checkpoint begin
        cache.arm_all_fpi_pending();

        let fpi_called = Cell::new(false);
        let write_called = Cell::new(false);

        let outcome = cache.flush_dirty_pages_with_fpi(
            10,
            &mut |_lsn| Ok(()),
            &mut |_fid, _off, _large, data| {
                // FPI should see the page data
                assert_eq!(data[0], 0xCC);
                fpi_called.set(true);
                Ok(())
            },
            &mut |_fid, _off, _large, _data| {
                // FPI must have been called BEFORE write
                assert!(fpi_called.get());
                write_called.set(true);
                Ok(())
            },
        );

        assert_eq!(
            outcome,
            FlushOutcome {
                flushed: 1,
                failed: 0
            }
        );
        assert!(fpi_called.get());
        assert!(write_called.get());
        // FPI_PENDING should be cleared after flush
        let entry = cache.page_table.get(&(1, 0)).unwrap();
        let (idx, _) = *entry;
        assert!(!cache.frames_4k[idx as usize].state.is_fpi_pending());
        assert_eq!(cache.dirty_page_count(), 0);
    }

    #[test]
    fn test_flush_dirty_pages_with_fpi_skips_non_fpi() {
        let cache = PageCache::new(4, 2);

        // Fetch and dirty a page but do NOT set FPI_PENDING
        let h = cache.fetch_page(1, 0, false, |_| Ok(())).unwrap();
        cache.unpin_page(h);
        cache.mark_dirty(1, 0, 100);

        let mut fpi_called = false;

        let outcome = cache.flush_dirty_pages_with_fpi(
            10,
            &mut |_| Ok(()),
            &mut |_, _, _, _| {
                fpi_called = true;
                Ok(())
            },
            &mut |_, _, _, _| Ok(()),
        );

        assert_eq!(
            outcome,
            FlushOutcome {
                flushed: 1,
                failed: 0
            }
        );
        assert!(
            !fpi_called,
            "FPI should not be called when FPI_PENDING is not set"
        );
    }

    /// A flush batch pays for WAL durability once, not once per page: every
    /// page's FullPageImage is appended first, then ONE durability barrier
    /// covers the highest LSN in the batch, then the pages are written. The
    /// log-before-data order per page is unchanged — no page write happens
    /// before the barrier returns.
    #[test]
    fn fpi_flush_batch_waits_for_wal_durability_once() {
        use std::cell::RefCell;

        let cache = PageCache::new(8, 2);
        for (page, lsn) in [(0u64, 100u64), (1, 300), (2, 200)] {
            let h = cache
                .fetch_page(1, page, false, |buf| {
                    buf.fill(page as u8 + 1);
                    Ok(())
                })
                .unwrap();
            cache.unpin_page(h);
            cache.mark_dirty(1, page, lsn);
        }
        cache.arm_all_fpi_pending();

        let events: RefCell<Vec<String>> = RefCell::new(Vec::new());
        let _ = cache.flush_dirty_pages_with_fpi(
            16,
            &mut |lsn| {
                events.borrow_mut().push(format!("wal:{lsn}"));
                Ok(())
            },
            &mut |_, off, _, _| {
                events.borrow_mut().push(format!("fpi:{off}"));
                Ok(())
            },
            &mut |_, off, _, _| {
                events.borrow_mut().push(format!("write:{off}"));
                Ok(())
            },
        );

        let events = events.into_inner();
        let waits: Vec<&String> = events.iter().filter(|e| e.starts_with("wal:")).collect();
        assert_eq!(
            waits.len(),
            1,
            "one WAL durability wait per FPI page instead of one per batch: {events:?}"
        );
        assert_eq!(
            waits[0], "wal:300",
            "the barrier covers the batch's highest LSN"
        );
        let barrier = events.iter().position(|e| e.starts_with("wal:")).unwrap();
        assert!(
            events[..barrier].iter().all(|e| e.starts_with("fpi:"))
                && events[barrier + 1..]
                    .iter()
                    .all(|e| e.starts_with("write:")),
            "every FPI before the barrier, every page write after it: {events:?}"
        );
        assert_eq!(cache.dirty_page_count(), 0);
    }

    fn dirty_fpi_pending_pages(cache: &PageCache, pages: &[(u64, u64, u64)]) {
        for &(file_id, page, lsn) in pages {
            let h = cache.fetch_page(file_id, page, false, |_| Ok(())).unwrap();
            cache.unpin_page(h);
            cache.mark_dirty(file_id, page, lsn);
        }
        cache.arm_all_fpi_pending();
    }

    /// Every way a page can miss its write is reported, and each such page
    /// stays dirty and FPI-pending: a failed image drops only that page, a
    /// failed barrier drops the whole batch (nothing is written before it),
    /// a failed write drops only that page.
    #[test]
    fn fpi_flush_reports_every_page_it_did_not_write() {
        let cache = PageCache::new(8, 0);
        dirty_fpi_pending_pages(&cache, &[(1, 0, 10), (1, 1, 20), (1, 2, 30)]);

        // Image of page 1 fails; the barrier covers only the pages left.
        let mut barrier = Vec::new();
        let outcome = cache.flush_dirty_pages_with_fpi(
            16,
            &mut |lsn| {
                barrier.push(lsn);
                Ok(())
            },
            &mut |_, off, _, _| {
                if off == 1 {
                    Err(std::io::Error::other("image failed"))
                } else {
                    Ok(())
                }
            },
            &mut |_, off, _, _| {
                if off == 2 {
                    Err(std::io::Error::other("write failed"))
                } else {
                    Ok(())
                }
            },
        );
        assert_eq!(
            outcome,
            FlushOutcome {
                flushed: 1,
                failed: 2
            }
        );
        assert_eq!(barrier, vec![30]);
        assert_eq!(cache.dirty_page_count(), 2);
        for off in [1, 2] {
            let (idx, _) = *cache.page_table.get(&(1, off)).unwrap();
            assert!(cache.frames_4k[idx as usize].state.is_fpi_pending());
        }

        // A failed barrier writes nothing.
        let mut writes = 0;
        let outcome = cache.flush_dirty_pages_with_fpi(
            16,
            &mut |_| Err(std::io::Error::other("WAL not durable")),
            &mut |_, _, _, _| Ok(()),
            &mut |_, _, _, _| {
                writes += 1;
                Ok(())
            },
        );
        assert_eq!(
            outcome,
            FlushOutcome {
                flushed: 0,
                failed: 2
            }
        );
        assert_eq!(writes, 0);
        assert_eq!(cache.dirty_page_count(), 2);
    }

    /// Abandoning a file's pages clears DIRTY and FPI_PENDING on that file's
    /// frames only, and leaves them cached (VALID) for reads.
    #[test]
    fn abandon_dirty_pages_of_file_touches_only_that_file() {
        let cache = PageCache::new(8, 0);
        dirty_fpi_pending_pages(&cache, &[(7, 0, 10), (7, 1, 20), (8, 0, 30)]);

        assert_eq!(cache.abandon_dirty_pages_of_file(7), 2);
        assert_eq!(cache.dirty_page_count(), 1);
        for off in [0, 1] {
            let (idx, _) = *cache.page_table.get(&(7, off)).unwrap();
            let state = &cache.frames_4k[idx as usize].state;
            assert!(!state.is_dirty() && !state.is_fpi_pending());
            let (_, _, flags) = FrameState::unpack(state.load());
            assert!(flags & frame::FLAG_VALID != 0, "still cached");
        }
        let (idx, _) = *cache.page_table.get(&(8, 0)).unwrap();
        assert!(cache.frames_4k[idx as usize].state.is_fpi_pending());
        assert_eq!(cache.abandon_dirty_pages_of_file(7), 0, "idempotent");
    }

    // ── Zombie-RAM regression: lazy buffers + per-shard budget ──────────────
    //
    // Root cause of "moon eats RAM in multishard": disk-offload is on by
    // default, so every shard EAGERLY committed a PageCache frame pool sized to
    // 25% of the WHOLE maxmemory at startup. N shards => N x 25% x maxmemory
    // pre-committed before serving one command (≈80% of host RAM with the auto
    // guardrail). These tests lock in the two fixes: (1) buffers allocate
    // lazily on first use, (2) the budget is divided across shards.

    #[test]
    fn buffers_are_lazily_allocated_not_eagerly() {
        // A pool big enough to be obvious if eager: 1000x4K + 100x64K ≈ 10.4 MB.
        let cache = PageCache::new(1000, 100);
        assert_eq!(
            cache.resident_buffer_bytes(),
            0,
            "a freshly constructed PageCache must NOT pre-commit page buffers \
             (the eager-alloc zombie); buffers grow on demand"
        );

        // First miss on a single 4K page commits exactly one frame's buffer.
        let h = cache
            .fetch_page(1, 0, false, |buf| {
                // The miss path must size the buffer to a full page before the
                // fill closure runs — callers index buf[..PAGE_4K].
                assert_eq!(buf.len(), PAGE_4K);
                buf[0] = 7;
                buf[PAGE_4K - 1] = 9;
                Ok(())
            })
            .expect("fetch_page");
        assert_eq!(
            cache.resident_buffer_bytes(),
            PAGE_4K,
            "exactly one 4K buffer resident after one 4K miss"
        );
        {
            let d = cache.page_data(&h);
            assert_eq!(d.len(), PAGE_4K);
            assert_eq!(d[0], 7);
            assert_eq!(d[PAGE_4K - 1], 9);
        }
        cache.unpin_page(h);

        // A 64K miss commits exactly one 64K buffer on top.
        let h2 = cache
            .fetch_page(2, 0, true, |buf| {
                assert_eq!(buf.len(), PAGE_64K);
                Ok(())
            })
            .expect("fetch_page large");
        assert_eq!(
            cache.resident_buffer_bytes(),
            PAGE_4K + PAGE_64K,
            "one 4K + one 64K buffer resident; the other 1098 frames stay unallocated"
        );
        cache.unpin_page(h2);
    }

    #[test]
    fn resident_counter_matches_buffer_walk_across_eviction_and_reuse() {
        // The O(1) counter must stay equal to the ground truth (walking every
        // buffer's committed length) through miss-grow, eviction, and frame
        // REUSE — a reused frame keeps its buffer, so re-fetching through it
        // must NOT double-count.
        let walk = |c: &PageCache| -> usize {
            let small: usize = c.buffers_4k.iter().map(|b| b.read().len()).sum();
            let large: usize = c.buffers_64k.iter().map(|b| b.read().len()).sum();
            small + large
        };

        let cache = PageCache::new(4, 2);
        // Fill all four 4K frames.
        for i in 0..4u64 {
            let h = cache.fetch_page(1, i * 4096, false, |_| Ok(())).unwrap();
            cache.unpin_page(h);
        }
        assert_eq!(cache.resident_buffer_bytes(), 4 * PAGE_4K);
        assert_eq!(cache.resident_buffer_bytes(), walk(&cache));

        // Evict two cold frames — buffers stay committed, counter unchanged.
        let evicted = cache.evict_cold_frames(2);
        assert!(evicted > 0, "unpinned clean frames must be evictable");
        assert_eq!(cache.resident_buffer_bytes(), walk(&cache));
        assert_eq!(cache.resident_buffer_bytes(), 4 * PAGE_4K);

        // New pages through the reused frames: len already == PAGE_4K, so the
        // grow site is skipped and the counter must not move.
        for i in 10..(10 + evicted as u64) {
            let h = cache.fetch_page(2, i * 4096, false, |_| Ok(())).unwrap();
            cache.unpin_page(h);
        }
        assert_eq!(cache.resident_buffer_bytes(), 4 * PAGE_4K);
        assert_eq!(cache.resident_buffer_bytes(), walk(&cache));

        // A failed read_fn still commits the buffer — counter counts it.
        let big = cache.fetch_page(3, 0, true, |_| Err(std::io::Error::other("boom")));
        assert!(big.is_err());
        assert_eq!(cache.resident_buffer_bytes(), 4 * PAGE_4K + PAGE_64K);
        assert_eq!(cache.resident_buffer_bytes(), walk(&cache));
    }

    #[test]
    fn pagecache_budget_divides_across_shards() {
        let whole = 1024u64 * 1024 * 1024; // 1 GiB whole-instance budget
        // The bug: each shard used the WHOLE budget. The fix: split by shards.
        assert_eq!(per_shard_pagecache_budget(whole, 4), whole / 4);
        assert_eq!(per_shard_pagecache_budget(whole, 1), whole);
        // div-by-zero guard: 0 shards treated as 1 (never panics).
        assert_eq!(per_shard_pagecache_budget(whole, 0), whole);

        // Frame counts scale down with the per-shard budget.
        let (f4_whole, f64_whole) = pagecache_frame_counts(whole);
        let (f4_quarter, f64_quarter) = pagecache_frame_counts(whole / 4);
        assert!(
            f4_quarter < f4_whole && f64_quarter < f64_whole,
            "per-shard frame counts must shrink with the divided budget"
        );
        assert!(
            f4_quarter >= 64 && f64_quarter >= 8,
            "minimum frame floors hold"
        );

        // Tiny budget still respects the minimum floors (never zero frames).
        let (f4_min, f64_min) = pagecache_frame_counts(1);
        assert_eq!((f4_min, f64_min), (64, 8));
    }
}
