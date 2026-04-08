//! 4KB-aligned buffer pool for O_DIRECT reads.
//!
//! `AlignedBuf` wraps a single `PAGE_4K`-aligned heap allocation.
//! `AlignedBufPool` manages a LIFO free-list of `AlignedBuf` instances
//! for cache-hot reuse during DiskANN beam search I/O.

use std::alloc::{Layout, alloc, dealloc};

use crate::persistence::page::PAGE_4K;

/// A single 4KB-aligned buffer for O_DIRECT reads.
///
/// Uses `std::alloc::alloc` with alignment = `PAGE_4K` to satisfy
/// the Linux O_DIRECT alignment requirement.
pub struct AlignedBuf {
    ptr: *mut u8,
    layout: Layout,
}

// SAFETY: `AlignedBuf` is a uniquely-owned heap allocation of `PAGE_4K` bytes
// with no interior mutability, no thread-local state, and no references into
// thread-specific resources (no TLS, no thread-bound handles). The contained
// raw pointer is owned exclusively by this value — there is no aliasing — and
// `Drop` frees it with the same layout it was allocated with. Moving the
// buffer between threads therefore transfers full, exclusive access with no
// data race and no dangling-reference hazard. `Sync` is intentionally NOT
// implemented: mutation through `&AlignedBuf` is not supported, and handing
// `&[u8]` views to multiple threads concurrently is not part of the API
// contract (all reads go through `&self`/`&mut self` on a single owner).
unsafe impl Send for AlignedBuf {}

impl AlignedBuf {
    /// Allocate one 4KB-aligned buffer.
    pub fn new() -> Self {
        // SAFETY: Layout is non-zero (4096 bytes), alignment is a power of 2 (4096).
        let layout =
            Layout::from_size_align(PAGE_4K, PAGE_4K).expect("PAGE_4K layout must be valid");
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        Self { ptr, layout }
    }

    /// Mutable slice over the entire buffer.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: `ptr` is valid for `PAGE_4K` bytes and uniquely owned via `&mut self`.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, PAGE_4K) }
    }

    /// Immutable slice over the entire buffer.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: `ptr` is valid for `PAGE_4K` bytes and borrowed via `&self`.
        unsafe { std::slice::from_raw_parts(self.ptr, PAGE_4K) }
    }

    /// Raw pointer for io_uring SQE submission.
    #[inline]
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        // SAFETY: `ptr` was allocated with `self.layout` via `std::alloc::alloc`.
        unsafe { dealloc(self.ptr, self.layout) };
    }
}

/// Pool of 4KB-aligned buffers. LIFO free-list for cache-hot reuse.
///
/// Modeled after `SendBufPool` in `src/io/uring_driver.rs`.
/// Each buffer is identified by a `u16` index for lightweight tracking
/// in io_uring CQE user_data.
pub struct AlignedBufPool {
    buffers: Vec<AlignedBuf>,
    free_list: Vec<u16>,
}

impl AlignedBufPool {
    /// Pre-allocate `count` aligned buffers, all initially free.
    pub fn new(count: u16) -> Self {
        let mut buffers = Vec::with_capacity(count as usize);
        let mut free_list = Vec::with_capacity(count as usize);
        for i in 0..count {
            buffers.push(AlignedBuf::new());
            free_list.push(i);
        }
        Self { buffers, free_list }
    }

    /// Allocate a buffer from the pool. Returns `(index, mutable slice)`.
    /// Returns `None` if the pool is exhausted.
    #[inline]
    pub fn alloc(&mut self) -> Option<(u16, &mut [u8])> {
        let idx = self.free_list.pop()?;
        let buf = &mut self.buffers[idx as usize];
        Some((idx, buf.as_mut_slice()))
    }

    /// Return a buffer to the pool. Out-of-bounds indices are silently
    /// ignored in release builds (asserted in debug) so a malformed CQE
    /// `user_data` cannot panic the shard thread.
    #[inline]
    pub fn reclaim(&mut self, idx: u16) {
        debug_assert!(
            (idx as usize) < self.buffers.len(),
            "reclaim index {idx} out of bounds (pool size {})",
            self.buffers.len(),
        );
        if (idx as usize) >= self.buffers.len() {
            return;
        }
        self.free_list.push(idx);
    }

    /// Raw pointer for io_uring SQE submission.
    #[inline]
    pub fn buf_ptr(&self, idx: u16) -> *mut u8 {
        self.buffers[idx as usize].as_ptr()
    }

    /// Immutable slice for reading completed data.
    #[inline]
    pub fn buf_slice(&self, idx: u16) -> &[u8] {
        self.buffers[idx as usize].as_slice()
    }

    /// Number of available (free) buffers.
    #[inline]
    pub fn free_count(&self) -> usize {
        self.free_list.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::page::PAGE_4K;

    #[test]
    fn test_aligned_buf_alignment() {
        let buf = AlignedBuf::new();
        assert_eq!(
            buf.as_ptr() as usize % PAGE_4K,
            0,
            "buffer pointer must be 4KB-aligned",
        );
    }

    #[test]
    fn test_pool_alloc_reclaim() {
        let mut pool = AlignedBufPool::new(3);
        assert_eq!(pool.free_count(), 3);

        let (i0, _) = pool.alloc().expect("alloc 0");
        let (i1, _) = pool.alloc().expect("alloc 1");
        let (i2, _) = pool.alloc().expect("alloc 2");
        assert_eq!(pool.free_count(), 0);
        assert!(pool.alloc().is_none(), "pool should be exhausted");

        pool.reclaim(i1);
        assert_eq!(pool.free_count(), 1);

        let (i3, _) = pool.alloc().expect("alloc after reclaim");
        assert_eq!(i3, i1, "LIFO should return the just-reclaimed index");
        assert_eq!(pool.free_count(), 0);

        pool.reclaim(i0);
        pool.reclaim(i2);
        pool.reclaim(i3);
        assert_eq!(pool.free_count(), 3);
    }

    #[test]
    fn test_pool_write_read() {
        let mut pool = AlignedBufPool::new(1);
        let (idx, slice) = pool.alloc().expect("alloc");

        // Write a pattern
        for (i, byte) in slice.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }

        // Read back via buf_slice
        let read = pool.buf_slice(idx);
        for (i, &byte) in read.iter().enumerate() {
            assert_eq!(byte, (i % 256) as u8, "mismatch at offset {i}");
        }

        pool.reclaim(idx);
    }
}
