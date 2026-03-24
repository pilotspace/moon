//! Provided buffer ring wrapper for io_uring multishot recv.
//!
//! Manages a ring of pre-allocated buffers registered with the kernel.
//! When a recv completes, the kernel selects a buffer from this ring.
//! After processing, the buffer MUST be returned via the ring.
//!
//! This module is only compiled on Linux (cfg-gated in mod.rs).
//! The actual io_uring_buf_ring integration happens in Plan 02.

/// Configuration for per-shard provided buffer ring.
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
/// Buffers are pre-allocated and registered with the kernel.
/// When a recv completes, the kernel selects a buffer from this ring.
/// After processing, the buffer MUST be returned via return_buf().
pub struct BufRingManager {
    config: BufRingConfig,
    // Actual ring setup deferred to Plan 02 UringDriver integration.
    // This establishes the API surface.
}

impl BufRingManager {
    pub fn new(config: BufRingConfig) -> Self {
        Self { config }
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
}
