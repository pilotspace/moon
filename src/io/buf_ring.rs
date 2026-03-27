//! Provided buffer ring wrapper for io_uring multishot recv.
//!
//! Manages a ring of pre-allocated buffers registered with the kernel.
//! When a recv completes, the kernel selects a buffer from this ring.
//! After processing, the buffer MUST be returned via return_buf().
//!
//! This module is only compiled on Linux (cfg-gated in mod.rs).
//!
//! Uses the legacy `IORING_OP_PROVIDE_BUFFERS` approach for initial registration.
//! The ring-mapped buf_ring optimization (5.19+) can be added later if benchmarks warrant.

use io_uring::IoUring;

/// Configuration for per-shard provided buffer ring.
#[derive(Clone)]
pub struct BufRingConfig {
    /// Number of buffers (default 256).
    pub buf_count: u16,
    /// Size per buffer in bytes (default 4096).
    pub buf_size: usize,
    /// Buffer group ID for this shard.
    pub group_id: u16,
}

impl Default for BufRingConfig {
    fn default() -> Self {
        Self {
            buf_count: 256,
            buf_size: 4096,
            group_id: 0,
        }
    }
}

/// Manages a provided buffer ring for io_uring multishot recv.
///
/// Buffers are pre-allocated in a contiguous `Vec` and registered with the kernel
/// via `IORING_OP_PROVIDE_BUFFERS`. When a multishot recv CQE completes, it includes
/// a buffer ID indicating which buffer received data. After processing,
/// the buffer MUST be returned via `return_buf()` to be reused.
pub struct BufRingManager {
    config: BufRingConfig,
    /// Contiguous buffer storage: buf_count * buf_size bytes.
    storage: Vec<u8>,
    /// Track which buffers are currently in-use (lent to kernel or being processed).
    in_use: Vec<bool>,
}

impl BufRingManager {
    /// Create a new BufRingManager with pre-allocated buffer storage.
    pub fn new(config: BufRingConfig) -> Self {
        let total = config.buf_count as usize * config.buf_size;
        Self {
            storage: vec![0u8; total],
            in_use: vec![false; config.buf_count as usize],
            config,
        }
    }

    /// Set up the provided buffer ring with io_uring.
    ///
    /// Registers all buffers with the kernel under the configured `group_id`
    /// using `IORING_OP_PROVIDE_BUFFERS`. Must be called once after ring creation,
    /// before any multishot recv is submitted.
    pub fn setup_ring(&mut self, ring: &IoUring) -> std::io::Result<()> {
        use io_uring::opcode;

        let entry = opcode::ProvideBuffers::new(
            self.storage.as_mut_ptr(),
            self.config.buf_size as i32,
            self.config.buf_count,
            self.config.group_id,
            0, // starting buffer ID
        )
        .build()
        .user_data(0); // special: buffer registration

        unsafe {
            ring.submission().push(&entry).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "SQ full during buffer registration",
                )
            })?;
        }

        ring.submit_and_wait(1)?;

        // Drain the CQE to check for errors
        for cqe in ring.completion() {
            if cqe.result() < 0 {
                return Err(std::io::Error::from_raw_os_error(-cqe.result()));
            }
        }

        Ok(())
    }

    /// Get a slice of the buffer identified by `buf_id` from a CQE.
    /// `len` is the number of bytes received (from CQE result).
    ///
    /// # Panics
    /// Panics if `buf_id >= buf_count` or `len > buf_size`.
    #[inline]
    pub fn get_buf(&self, buf_id: u16, len: usize) -> &[u8] {
        let offset = buf_id as usize * self.config.buf_size;
        &self.storage[offset..offset + len]
    }

    /// Return a buffer to the pool for reuse.
    ///
    /// Submits a single `ProvideBuffers` SQE to re-provide this buffer to the kernel.
    /// Must be called after processing recv data, before the buffer can be reused.
    /// The SQE is batched (not submitted immediately) -- it will go out with the
    /// next `submit_and_wait` call.
    pub fn return_buf(&mut self, ring: &IoUring, buf_id: u16) -> std::io::Result<()> {
        self.in_use[buf_id as usize] = false;

        let offset = buf_id as usize * self.config.buf_size;
        let entry = io_uring::opcode::ProvideBuffers::new(
            self.storage[offset..].as_ptr() as *mut u8,
            self.config.buf_size as i32,
            1, // count = 1 buffer
            self.config.group_id,
            buf_id,
        )
        .build()
        .user_data(0);

        unsafe {
            ring.submission().push(&entry).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "SQ full during buffer return")
            })?;
        }

        Ok(())
    }

    /// Mark a buffer as in-use (called when CQE arrives with this buf_id).
    #[inline]
    pub fn mark_in_use(&mut self, buf_id: u16) {
        self.in_use[buf_id as usize] = true;
    }

    /// Check if a buffer is currently in-use.
    #[inline]
    pub fn is_in_use(&self, buf_id: u16) -> bool {
        self.in_use[buf_id as usize]
    }

    /// Number of buffers currently in use.
    pub fn in_use_count(&self) -> usize {
        self.in_use.iter().filter(|&&b| b).count()
    }

    pub fn config(&self) -> &BufRingConfig {
        &self.config
    }

    pub fn buf_count(&self) -> u16 {
        self.config.buf_count
    }

    pub fn buf_size(&self) -> usize {
        self.config.buf_size
    }

    pub fn group_id(&self) -> u16 {
        self.config.group_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = BufRingConfig::default();
        assert_eq!(config.buf_count, 256);
        assert_eq!(config.buf_size, 4096);
        assert_eq!(config.group_id, 0);
    }

    #[test]
    fn test_buf_ring_manager_new() {
        let mgr = BufRingManager::new(BufRingConfig::default());
        assert_eq!(mgr.buf_count(), 256);
        assert_eq!(mgr.buf_size(), 4096);
        assert_eq!(mgr.group_id(), 0);
        assert_eq!(mgr.in_use_count(), 0);
    }

    #[test]
    fn test_custom_config() {
        let config = BufRingConfig {
            buf_count: 512,
            buf_size: 8192,
            group_id: 3,
        };
        let mgr = BufRingManager::new(config);
        assert_eq!(mgr.buf_count(), 512);
        assert_eq!(mgr.buf_size(), 8192);
        assert_eq!(mgr.group_id(), 3);
    }

    #[test]
    fn test_storage_allocation() {
        let config = BufRingConfig {
            buf_count: 4,
            buf_size: 64,
            group_id: 0,
        };
        let mgr = BufRingManager::new(config);
        // 4 buffers * 64 bytes = 256 bytes
        assert_eq!(mgr.storage.len(), 256);
        assert_eq!(mgr.in_use.len(), 4);
    }

    #[test]
    fn test_get_buf() {
        let config = BufRingConfig {
            buf_count: 4,
            buf_size: 8,
            group_id: 0,
        };
        let mut mgr = BufRingManager::new(config);
        // Write some data into buffer 2
        let offset = 2 * 8;
        mgr.storage[offset..offset + 5].copy_from_slice(b"hello");
        let data = mgr.get_buf(2, 5);
        assert_eq!(data, b"hello");
    }

    #[test]
    fn test_mark_in_use() {
        let config = BufRingConfig {
            buf_count: 4,
            buf_size: 8,
            group_id: 0,
        };
        let mut mgr = BufRingManager::new(config);
        assert!(!mgr.is_in_use(1));
        assert_eq!(mgr.in_use_count(), 0);
        mgr.mark_in_use(1);
        assert!(mgr.is_in_use(1));
        assert_eq!(mgr.in_use_count(), 1);
    }
}
