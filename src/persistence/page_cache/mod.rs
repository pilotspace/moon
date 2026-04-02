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
}

impl PageCache {
    /// Create a new PageCache with pre-allocated frame pools.
    ///
    /// - `num_frames_4k`: number of 4KB frame slots
    /// - `num_frames_64k`: number of 64KB frame slots
    pub fn new(num_frames_4k: usize, num_frames_64k: usize) -> Self {
        let frames_4k: Vec<FrameDescriptor> =
            (0..num_frames_4k).map(|_| FrameDescriptor::new()).collect();
        let buffers_4k: Vec<RwLock<Vec<u8>>> = (0..num_frames_4k)
            .map(|_| RwLock::new(vec![0u8; PAGE_4K]))
            .collect();

        let frames_64k: Vec<FrameDescriptor> =
            (0..num_frames_64k).map(|_| FrameDescriptor::new()).collect();
        let buffers_64k: Vec<RwLock<Vec<u8>>> = (0..num_frames_64k)
            .map(|_| RwLock::new(vec![0u8; PAGE_64K]))
            .collect();

        Self {
            frames_4k,
            buffers_4k,
            frames_64k,
            buffers_64k,
            page_table: DashMap::new(),
            sweep_4k: ClockSweep::new(num_frames_4k),
            sweep_64k: ClockSweep::new(num_frames_64k),
        }
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
            let frames = if large { &self.frames_64k } else { &self.frames_4k };
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

        let victim_idx = sweep.find_victim(frames).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                "page cache full: all frames pinned",
            )
        })?;

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

        // Read page data from disk
        {
            let mut buf = buffers[victim_idx].write();
            read_fn(&mut buf)?;
        }

        // Mark valid, pin, touch
        victim.state.set_valid();
        victim.state.pin();
        victim.state.touch();

        // Insert into page table
        self.page_table
            .insert(key, (victim_idx as u32, is_large));

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
    pub fn page_data_mut(
        &self,
        handle: &PageHandle,
    ) -> parking_lot::RwLockWriteGuard<'_, Vec<u8>> {
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
    pub fn clear_all_fpi_pending(&self) {
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

    /// FPI-aware variant of `flush_dirty_pages`.
    ///
    /// Before writing a dirty page, checks if FPI_PENDING is set. If so,
    /// calls `fpi_fn` with the full page data to write a full-page image to
    /// WAL (torn-page defense), then clears the FPI_PENDING flag.
    ///
    /// `fpi_fn` signature matches `write_fn`: (file_id, page_offset, is_large, data).
    pub fn flush_dirty_pages_with_fpi(
        &self,
        max_pages: usize,
        wal_flush_fn: &mut impl FnMut(u64) -> std::io::Result<()>,
        fpi_fn: &mut impl FnMut(u64, u64, bool, &[u8]) -> std::io::Result<()>,
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
                if let Err(e) = wal_flush_fn(page_lsn) {
                    tracing::error!("WAL flush for dirty page failed: {}", e);
                    continue;
                }
                // FPI: write full-page image before page data if pending
                if flags & FLAG_FPI_PENDING != 0 {
                    let buf = self.buffers_4k[idx].read();
                    if let Err(e) = fpi_fn(file_id, page_offset, false, &buf) {
                        tracing::error!("FPI write failed: file_id={}, offset={}: {}", file_id, page_offset, e);
                        continue;
                    }
                    drop(buf);
                    frame.state.clear_fpi_pending();
                }
                {
                    let buf = self.buffers_4k[idx].read();
                    if let Err(e) = write_fn(file_id, page_offset, false, &buf) {
                        tracing::error!("Dirty page write failed: file_id={}, offset={}: {}", file_id, page_offset, e);
                        continue;
                    }
                }
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
                if flags & FLAG_FPI_PENDING != 0 {
                    let buf = self.buffers_64k[idx].read();
                    if let Err(e) = fpi_fn(file_id, page_offset, true, &buf) {
                        tracing::error!("FPI write failed: file_id={}, offset={}: {}", file_id, page_offset, e);
                        continue;
                    }
                    drop(buf);
                    frame.state.clear_fpi_pending();
                }
                {
                    let buf = self.buffers_64k[idx].read();
                    if let Err(e) = write_fn(file_id, page_offset, true, &buf) {
                        tracing::error!("Dirty page write failed: file_id={}, offset={}: {}", file_id, page_offset, e);
                        continue;
                    }
                }
                frame.state.clear_dirty();
                flushed += 1;
            }
        }
        flushed
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

        let flushed =
            cache.flush_dirty_pages(2, &mut |_| Ok(()), &mut |_, _, _, _| Ok(()));

        assert_eq!(flushed, 2);
        assert_eq!(cache.dirty_page_count(), 2);
    }

    #[test]
    fn test_clear_all_fpi_pending_sets_on_valid_frames() {
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
        cache.clear_all_fpi_pending();

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
        let h = cache.fetch_page(1, 0, false, |buf| {
            buf[0] = 0xCC;
            Ok(())
        }).unwrap();
        cache.unpin_page(h);
        cache.mark_dirty(1, 0, 100);

        // Simulate checkpoint begin
        cache.clear_all_fpi_pending();

        let fpi_called = Cell::new(false);
        let write_called = Cell::new(false);

        let flushed = cache.flush_dirty_pages_with_fpi(
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

        assert_eq!(flushed, 1);
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

        let flushed = cache.flush_dirty_pages_with_fpi(
            10,
            &mut |_| Ok(()),
            &mut |_, _, _, _| {
                fpi_called = true;
                Ok(())
            },
            &mut |_, _, _, _| Ok(()),
        );

        assert_eq!(flushed, 1);
        assert!(!fpi_called, "FPI should not be called when FPI_PENDING is not set");
    }
}
