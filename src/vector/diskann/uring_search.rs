//! Dedicated io_uring ring for DiskANN cold-tier beam search.
//!
//! Separate from the network io_uring ring to avoid interleaving
//! disk and network SQEs. One ring per DiskAnnSegment.
//!
//! This entire module is compiled only on Linux (`#[cfg(target_os = "linux")]`
//! in `mod.rs`).

use std::ffi::CString;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
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
    /// Owned O_DIRECT file descriptor. `OwnedFd::drop` closes it automatically,
    /// so no manual `libc::close` is needed.
    vamana_fd: OwnedFd,
}

impl DiskAnnUring {
    /// Create a new io_uring ring for DiskANN reads.
    ///
    /// `vamana_fd` must be an O_DIRECT-opened file descriptor (from
    /// `open_vamana_direct`). `pool_size` controls how many concurrent
    /// 4KB reads can be in flight.
    pub fn new(vamana_fd: OwnedFd, pool_size: u16) -> io::Result<Self> {
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
    ///
    /// Returns the number of reads actually submitted. May be less than
    /// `node_indices.len()` if the buffer pool is exhausted.
    pub fn submit_reads(&mut self, node_indices: &[u32]) -> io::Result<usize> {
        if node_indices.is_empty() {
            return Ok(0);
        }

        let mut submitted = 0usize;
        for &node_index in node_indices {
            let Some((buf_idx, _)) = self.buf_pool.alloc() else {
                // Pool exhausted — submit what we have so far.
                break;
            };

            let file_offset = node_index as u64 * PAGE_4K as u64;
            let read_op = opcode::Read::new(
                types::Fd(self.vamana_fd.as_raw_fd()),
                self.buf_pool.buf_ptr(buf_idx),
                PAGE_4K as u32,
            )
            .offset(file_offset)
            .build()
            .user_data(buf_idx as u64);

            // SAFETY: The SQE references a buffer from our pool that will
            // remain valid until we reclaim it after completion.
            unsafe {
                if self.ring.submission().push(&read_op).is_err() {
                    // SQ full — reclaim buffer and stop.
                    self.buf_pool.reclaim(buf_idx);
                    break;
                }
            }
            submitted += 1;
        }

        if submitted == 0 {
            return Ok(0);
        }

        self.ring.submit_and_wait(submitted)?;
        Ok(submitted)
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

// `Drop` for `DiskAnnUring` is intentionally not implemented: `OwnedFd` closes
// the vamana fd automatically when the struct is dropped, and `IoUring` and
// `AlignedBufPool` own their own resources. Keeping this as an implicit drop
// removes the only remaining raw `libc::close` from this module.

/// Open a Vamana graph file with O_DIRECT for bypassing the page cache.
///
/// Returns an [`OwnedFd`] — the caller owns it and it is closed automatically
/// when dropped. Pass it to [`DiskAnnUring::new`] which takes ownership.
pub fn open_vamana_direct(path: &Path) -> io::Result<OwnedFd> {
    let c_path = CString::new(
        path.to_str()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "non-UTF8 path"))?,
    )
    .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains null byte"))?;

    // SAFETY: `c_path` is a valid null-terminated C string. O_RDONLY | O_DIRECT
    // are valid flags for libc::open. `libc::open` returns a fresh, owned fd
    // on success; we immediately wrap it in `OwnedFd` (which takes ownership
    // of the close) before returning, so there is no possibility of leak or
    // double-close along the happy path.
    let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDONLY | libc::O_DIRECT) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `fd` is a fresh kernel-allocated file descriptor that we have
    // not handed to anyone else and not registered with any other owner; this
    // is the sole transfer of ownership into `OwnedFd`.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}
