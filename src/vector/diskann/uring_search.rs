//! Dedicated io_uring ring for DiskANN cold-tier beam search.
//!
//! Separate from the network io_uring ring to avoid interleaving
//! disk and network SQEs. One ring per DiskAnnSegment.
//!
//! This entire module is compiled only on Linux (`#[cfg(target_os = "linux")]`
//! in `mod.rs`).

use std::ffi::CString;
use std::io;
use std::os::fd::RawFd;
use std::path::Path;

use io_uring::IoUring;
use io_uring::opcode;
use io_uring::types;

use crate::persistence::page::PAGE_4K;

use super::aligned_buf::AlignedBufPool;

/// Dedicated io_uring instance for DiskANN disk reads.
///
/// Wraps a small ring (32 SQ entries) with an owned `AlignedBufPool`.
/// The ring is used exclusively for batch pread operations during
/// beam search, avoiding interference with the shard's network ring.
pub struct DiskAnnUring {
    ring: IoUring,
    buf_pool: AlignedBufPool,
    vamana_fd: RawFd,
}

impl DiskAnnUring {
    /// Create a new io_uring ring for DiskANN reads.
    ///
    /// `vamana_fd` must be an O_DIRECT-opened file descriptor (from
    /// `open_vamana_direct`). `pool_size` controls how many concurrent
    /// 4KB reads can be in flight.
    pub fn new(vamana_fd: RawFd, pool_size: u16) -> io::Result<Self> {
        let ring = IoUring::builder()
            .setup_single_issuer()
            .setup_coop_taskrun()
            .build(32)?;
        let buf_pool = AlignedBufPool::new(pool_size);
        Ok(Self {
            ring,
            buf_pool,
            vamana_fd,
        })
    }

    /// Submit batch read SQEs for the given node indices.
    ///
    /// Each node occupies one 4KB page at offset `node_index * PAGE_4K`.
    /// Allocates one aligned buffer per read from the pool.
    /// After submission, call `collect_completions` to harvest results.
    pub fn submit_reads(&mut self, node_indices: &[u32]) -> io::Result<()> {
        for &node_index in node_indices {
            let (buf_idx, _) = self
                .buf_pool
                .alloc()
                .expect("AlignedBufPool exhausted during submit_reads");

            let file_offset = node_index as u64 * PAGE_4K as u64;
            let read_op = opcode::Read::new(
                types::Fd(self.vamana_fd),
                self.buf_pool.buf_ptr(buf_idx),
                PAGE_4K as u32,
            )
            .offset(file_offset)
            .build()
            .user_data(buf_idx as u64);

            // SAFETY: The SQE references a buffer from our pool that will
            // remain valid until we reclaim it after completion.
            unsafe {
                self.ring
                    .submission()
                    .push(&read_op)
                    .map_err(|_| io::Error::new(io::ErrorKind::Other, "SQ full"))?;
            }
        }

        self.ring.submit_and_wait(node_indices.len())?;
        Ok(())
    }

    /// Drain `count` CQEs from the completion queue.
    ///
    /// Returns `(buf_idx, result)` pairs where `result` is the number
    /// of bytes read (positive) or a negative errno on failure.
    pub fn collect_completions(&mut self, count: usize) -> Vec<(u16, i32)> {
        let mut results = Vec::with_capacity(count);
        let cq = self.ring.completion();
        for cqe in cq.take(count) {
            let buf_idx = cqe.user_data() as u16;
            let result = cqe.result();
            results.push((buf_idx, result));
        }
        results
    }

    /// Read the buffer contents after a successful completion.
    #[inline]
    pub fn read_buf(&self, buf_idx: u16) -> &[u8] {
        self.buf_pool.buf_slice(buf_idx)
    }

    /// Return a buffer to the pool after processing.
    #[inline]
    pub fn reclaim_buf(&mut self, buf_idx: u16) {
        self.buf_pool.reclaim(buf_idx);
    }

    /// Access the buffer pool for diagnostics.
    #[inline]
    pub fn pool(&self) -> &AlignedBufPool {
        &self.buf_pool
    }
}

impl Drop for DiskAnnUring {
    fn drop(&mut self) {
        // SAFETY: We own this FD from open_vamana_direct(). Closing it
        // is required to avoid FD leaks. The io_uring ring does not
        // close the FD on its own.
        unsafe {
            libc::close(self.vamana_fd);
        }
    }
}

/// Open a Vamana graph file with O_DIRECT for bypassing the page cache.
///
/// Returns the raw file descriptor. The caller owns it and must ensure
/// it is closed (typically via `DiskAnnUring::drop`).
pub fn open_vamana_direct(path: &Path) -> io::Result<RawFd> {
    let c_path = CString::new(
        path.to_str()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "non-UTF8 path"))?,
    )
    .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains null byte"))?;

    // SAFETY: `c_path` is a valid null-terminated C string. O_RDONLY | O_DIRECT
    // are valid flags for libc::open. The returned FD is owned by the caller.
    let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDONLY | libc::O_DIRECT) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(fd)
}
