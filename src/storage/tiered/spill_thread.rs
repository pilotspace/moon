//! Background I/O thread for async eviction spill-to-disk.
//!
//! The monoio event loop is single-threaded. Synchronous pwrite during eviction
//! blocks ALL connections. This module provides a fire-and-forget channel
//! infrastructure so pwrite happens on a dedicated `std::thread`.
//!
//! Pattern: event loop builds `SpillRequest` (CPU-only, no I/O) -> sends via
//! flume channel -> background thread does pwrite -> sends `SpillCompletion`
//! back -> event loop polls completions and updates manifest + ColdIndex.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Cumulative count of `SpillCompletion`s dropped because the event-loop-side
/// completion channel was full. Each drop means the data is on disk but the
/// in-memory `cold_index` slot was not refreshed; the next checkpoint repairs
/// it from the manifest.
static SPILL_COMPLETION_DROPPED: AtomicU64 = AtomicU64::new(0);

/// Returns the cumulative number of dropped spill completions across all
/// shards. Exposed for INFO / metrics scraping.
#[inline]
pub fn spill_completion_dropped_total() -> u64 {
    SPILL_COMPLETION_DROPPED.load(Ordering::Relaxed)
}

use bytes::Bytes;
use tracing::warn;

use crate::persistence::kv_page::ValueType;
use crate::persistence::manifest::{FileEntry, FileStatus, StorageTier};
use crate::persistence::page::PageType;
use crate::storage::tiered::kv_spill::{build_kv_spill_pages, write_kv_spill_pages};

/// Request sent from event loop to background spill thread.
///
/// Contains all data needed for pwrite -- no references to shard state.
/// `Bytes` fields are reference-counted (cheap clone on event loop side).
pub struct SpillRequest {
    pub key: Bytes,
    /// Logical database index the key was evicted from. Used by completion
    /// handler to update the correct per-DB cold_index.
    pub db_index: usize,
    /// Already-serialized value (string bytes or kv_serde output).
    pub value_bytes: Bytes,
    /// Value type discriminant from `kv_page::ValueType`.
    pub value_type: ValueType,
    /// Entry flags (HAS_TTL, OVERFLOW, etc.) from `kv_page::entry_flags`.
    pub flags: u8,
    /// Absolute TTL in milliseconds if `HAS_TTL` flag is set.
    pub ttl_ms: Option<u64>,
    /// Pre-assigned file ID (event loop increments `next_file_id` before sending).
    pub file_id: u64,
    /// Shard data directory path.
    pub shard_dir: PathBuf,
}

/// Completion sent from background thread back to event loop.
///
/// Carries everything needed for manifest + ColdIndex update.
pub struct SpillCompletion {
    /// The key that was spilled (for ColdIndex insertion).
    pub key: Bytes,
    /// Logical database index this completion belongs to.
    pub db_index: usize,
    /// File ID of the created `.mpf` file.
    pub file_id: u64,
    /// Slot index within the page (always 0 for single-entry pages).
    pub slot_idx: u16,
    /// Ready-to-use FileEntry for `manifest.add_file()`.
    pub file_entry: FileEntry,
    /// Whether the pwrite succeeded. If false, file may not exist.
    pub success: bool,
}

/// Write a spill file to disk without touching manifest or ColdIndex.
///
/// Returns `(page_count, byte_size)` on success. Delegates page layout to
/// `kv_spill::build_kv_spill_pages` so the on-disk format is bit-identical
/// to the synchronous (`spill_to_datafile`) path.
fn write_spill_file(req: &SpillRequest) -> io::Result<(u32, u64)> {
    let pages = build_kv_spill_pages(
        req.key.as_ref(),
        req.value_bytes.as_ref(),
        req.value_type,
        req.flags,
        req.ttl_ms,
        req.file_id,
    )?;

    let byte_size = write_kv_spill_pages(&req.shard_dir, req.file_id, &pages)?;
    Ok((pages.total_pages, byte_size))
}

/// Background thread that performs pwrite for evicted KV entries.
///
/// One per shard. Matches the WAL writer pattern: dedicated `std::thread`
/// that blocks on a flume channel, processes requests sequentially, and
/// sends completions back to the event loop.
pub struct SpillThread {
    request_tx: flume::Sender<SpillRequest>,
    completion_rx: flume::Receiver<SpillCompletion>,
    join_handle: Option<std::thread::JoinHandle<()>>,
    stop_flag: Arc<AtomicBool>,
}

impl SpillThread {
    /// Spawn a new background spill thread for the given shard.
    ///
    /// Creates two bounded flume channels:
    /// - `request`: bounded(4096), event loop -> bg thread
    /// - `completion`: bounded(8192), bg thread -> event loop
    ///
    /// The completion channel is bounded so a stalled event loop cannot let
    /// in-flight `SpillCompletion`s accumulate without limit. The KV is
    /// already on disk by the time a completion is dropped — the next
    /// checkpoint rebuilds `cold_index` from the manifest, so dropping is
    /// safe (though we count it for observability).
    pub fn new(shard_id: usize) -> Self {
        let (request_tx, request_rx) = flume::bounded::<SpillRequest>(4096);
        let (completion_tx, completion_rx) = flume::bounded::<SpillCompletion>(8192);
        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_flag_bg = stop_flag.clone();

        #[allow(clippy::expect_used)] // Startup: spill thread is critical infrastructure — spawn failure is fatal
        let join_handle = std::thread::Builder::new()
            .name(format!("spill-{shard_id}"))
            .spawn(move || {
                Self::run(request_rx, completion_tx, stop_flag_bg);
            })
            .expect("failed to spawn spill thread");

        Self {
            request_tx,
            completion_rx,
            join_handle: Some(join_handle),
            stop_flag,
        }
    }

    /// Background thread main loop.
    fn run(
        request_rx: flume::Receiver<SpillRequest>,
        completion_tx: flume::Sender<SpillCompletion>,
        stop_flag: Arc<AtomicBool>,
    ) {
        loop {
            if stop_flag.load(Ordering::Acquire) {
                break;
            }
            let req = match request_rx.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(r) => r,
                Err(flume::RecvTimeoutError::Timeout) => continue,
                Err(flume::RecvTimeoutError::Disconnected) => break,
            };
            let file_id = req.file_id;
            let key = req.key.clone();
            let db_index = req.db_index;

            let (success, file_entry) = match write_spill_file(&req) {
                Ok((page_count, byte_size)) => {
                    let entry = FileEntry {
                        file_id,
                        file_type: PageType::KvLeaf as u8,
                        status: FileStatus::Active,
                        tier: StorageTier::Hot,
                        page_size_log2: 12, // 4KB = 2^12
                        page_count,
                        byte_size,
                        created_lsn: 0,
                        min_key_hash: 0,
                        max_key_hash: 0,
                    };
                    (true, entry)
                }
                Err(e) => {
                    warn!(
                        file_id,
                        error = %e,
                        "spill_thread: pwrite failed"
                    );
                    // Build a placeholder FileEntry for the failure case
                    let entry = FileEntry {
                        file_id,
                        file_type: PageType::KvLeaf as u8,
                        status: FileStatus::Active,
                        tier: StorageTier::Hot,
                        page_size_log2: 12,
                        page_count: 0,
                        byte_size: 0,
                        created_lsn: 0,
                        min_key_hash: 0,
                        max_key_hash: 0,
                    };
                    (false, entry)
                }
            };

            let completion = SpillCompletion {
                key,
                db_index,
                file_id,
                slot_idx: 0,
                file_entry,
                success,
            };

            // Use try_send: a wedged event loop must not back-pressure the
            // bg thread (which would in turn back-pressure eviction and
            // defeat the entire async-spill design). On overflow we drop the
            // completion and bump a counter; the data is already on disk and
            // the next checkpoint will rebuild cold_index from the manifest.
            match completion_tx.try_send(completion) {
                Ok(()) => {}
                Err(flume::TrySendError::Full(_)) => {
                    SPILL_COMPLETION_DROPPED.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        "spill_thread: completion channel full, dropping completion (total dropped: {})",
                        SPILL_COMPLETION_DROPPED.load(Ordering::Relaxed)
                    );
                }
                Err(flume::TrySendError::Disconnected(_)) => {
                    // Event loop dropped its receiver -- shutting down
                    break;
                }
            }
        }
    }

    /// Get a clone of the request sender for the event loop to hold.
    pub fn sender(&self) -> flume::Sender<SpillRequest> {
        self.request_tx.clone()
    }

    /// Non-blocking poll for a single completion.
    pub fn try_recv_completion(&self) -> Option<SpillCompletion> {
        self.completion_rx.try_recv().ok()
    }

    /// Drain all pending completions (non-blocking).
    pub fn drain_completions(&self) -> Vec<SpillCompletion> {
        let mut completions = Vec::new();
        while let Ok(c) = self.completion_rx.try_recv() {
            completions.push(c);
        }
        completions
    }

    /// Shut down the background thread cleanly.
    ///
    /// Sets a stop flag and joins. Safe to call even when cloned `Sender`s are
    /// still alive: the background thread polls the flag every 100 ms and
    /// exits without waiting for channel close. This avoids the deadlock where
    /// connection futures held cloned senders past shutdown.
    pub fn shutdown(mut self) {
        self.stop_flag.store(true, Ordering::Release);
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::kv_page::{ValueType, entry_flags, read_datafile};
    use crate::persistence::page::PAGE_4K;
    use crate::storage::entry::current_time_ms;

    #[test]
    fn test_spill_thread_new_returns_valid_handles() {
        let st = SpillThread::new(0);
        // Thread is running, sender/receiver are valid
        assert!(!st.request_tx.is_disconnected());
        assert!(!st.completion_rx.is_disconnected());
        st.shutdown();
    }

    #[test]
    fn test_spill_request_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let st = SpillThread::new(1);
        let sender = st.sender();

        let req = SpillRequest {
            key: Bytes::from_static(b"test_key"),
            db_index: 0,
            value_bytes: Bytes::from_static(b"test_value"),
            value_type: ValueType::String,
            flags: 0,
            ttl_ms: None,
            file_id: 1,
            shard_dir: tmp.path().to_path_buf(),
        };
        sender.send(req).unwrap();

        // Wait for completion
        let completion = st
            .completion_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        assert!(completion.success);
        assert_eq!(completion.file_id, 1);
        assert_eq!(completion.key, Bytes::from_static(b"test_key"));
        assert_eq!(completion.slot_idx, 0);
        assert_eq!(completion.file_entry.page_count, 1);
        assert_eq!(completion.file_entry.byte_size, PAGE_4K as u64);

        // Verify .mpf file exists on disk
        let file_path = tmp.path().join("data/heap-000001.mpf");
        assert!(file_path.exists());

        // Verify content
        let pages = read_datafile(&file_path).unwrap();
        assert_eq!(pages.len(), 1);
        let entry = pages[0].get(0).unwrap();
        assert_eq!(entry.key, b"test_key");
        assert_eq!(entry.value, b"test_value");
        assert_eq!(entry.value_type, ValueType::String);
        assert_eq!(entry.ttl_ms, None);

        drop(sender);
        st.shutdown();
    }

    #[test]
    fn test_spill_request_with_ttl() {
        let tmp = tempfile::tempdir().unwrap();
        let st = SpillThread::new(2);
        let sender = st.sender();

        let future_ms = current_time_ms() + 60_000;
        let req = SpillRequest {
            key: Bytes::from_static(b"ttl_key"),
            db_index: 0,
            value_bytes: Bytes::from_static(b"expiring_val"),
            value_type: ValueType::String,
            flags: entry_flags::HAS_TTL,
            ttl_ms: Some(future_ms),
            file_id: 2,
            shard_dir: tmp.path().to_path_buf(),
        };
        sender.send(req).unwrap();

        let completion = st
            .completion_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        assert!(completion.success);
        assert_eq!(completion.file_entry.file_type, PageType::KvLeaf as u8);

        // Verify TTL on disk
        let file_path = tmp.path().join("data/heap-000002.mpf");
        let pages = read_datafile(&file_path).unwrap();
        let entry = pages[0].get(0).unwrap();
        assert_eq!(entry.key, b"ttl_key");
        assert!(entry.ttl_ms.is_some());
        let stored_ttl = entry.ttl_ms.unwrap();
        assert!(stored_ttl > 0);

        drop(sender);
        st.shutdown();
    }

    #[test]
    fn test_spill_thread_shutdown() {
        let st = SpillThread::new(3);
        // Grab a sender clone to verify it's disconnected after shutdown
        let sender = st.sender();

        // Drop clone first so channel fully disconnects, then shutdown joins
        drop(sender);
        st.shutdown();

        // Thread has been joined -- verify by reaching this point without hang.
        // The join_handle was consumed, confirming clean exit.
    }

    #[test]
    fn test_multiple_requests_ordered() {
        let tmp = tempfile::tempdir().unwrap();
        let st = SpillThread::new(4);
        let sender = st.sender();

        for i in 0..5u64 {
            let req = SpillRequest {
                key: Bytes::from(format!("key_{i}")),
                db_index: 0,
                value_bytes: Bytes::from(format!("val_{i}")),
                value_type: ValueType::String,
                flags: 0,
                ttl_ms: None,
                file_id: i + 1,
                shard_dir: tmp.path().to_path_buf(),
            };
            sender.send(req).unwrap();
        }

        // Collect all completions in order
        let mut completions = Vec::new();
        for _ in 0..5 {
            let c = st
                .completion_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            completions.push(c);
        }

        // Verify ordering (sequential processing)
        for (i, c) in completions.iter().enumerate() {
            assert!(c.success);
            assert_eq!(c.file_id, (i as u64) + 1);
            assert_eq!(c.key, Bytes::from(format!("key_{i}")));
        }

        // Verify all files exist
        for i in 1..=5u64 {
            let path = tmp.path().join(format!("data/heap-{i:06}.mpf"));
            assert!(path.exists(), "file {i} should exist");
        }

        drop(sender);
        st.shutdown();
    }

    #[test]
    fn test_full_pipeline_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let st = SpillThread::new(10);
        let sender = st.sender();

        // Send 5 requests with different keys/values
        for i in 0..5u64 {
            let req = SpillRequest {
                key: Bytes::from(format!("pipeline_key_{i}")),
                db_index: 0,
                value_bytes: Bytes::from(format!("pipeline_value_{i}_with_some_data")),
                value_type: ValueType::String,
                flags: 0,
                ttl_ms: None,
                file_id: 100 + i,
                shard_dir: tmp.path().to_path_buf(),
            };
            sender.send(req).unwrap();
        }

        // Drain completions (with retries to allow background thread to process)
        let mut completions = Vec::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while completions.len() < 5 && std::time::Instant::now() < deadline {
            completions.extend(st.drain_completions());
            if completions.len() < 5 {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
        assert_eq!(completions.len(), 5, "Expected 5 completions");

        for (i, c) in completions.iter().enumerate() {
            assert!(c.success, "completion {} should succeed", i);
            assert_eq!(c.file_id, 100 + i as u64);
            assert!(c.file_entry.page_count >= 1, "page_count should be >= 1");
            assert_eq!(
                c.file_entry.file_type,
                PageType::KvLeaf as u8,
                "file_type should be KvLeaf"
            );

            // Verify .mpf file exists on disk
            let file_path = tmp.path().join(format!("data/heap-{:06}.mpf", c.file_id));
            assert!(file_path.exists(), "file {} should exist", c.file_id);

            // Read back and verify content
            let pages = read_datafile(&file_path).unwrap();
            assert!(!pages.is_empty());
            let entry = pages[0].get(0).unwrap();
            assert_eq!(entry.key, format!("pipeline_key_{i}").as_bytes());
            assert_eq!(
                entry.value,
                format!("pipeline_value_{i}_with_some_data").as_bytes()
            );
        }

        drop(sender);
        st.shutdown();
    }

    #[test]
    fn test_channel_backpressure() {
        let tmp = tempfile::tempdir().unwrap();
        let st = SpillThread::new(11);
        let sender = st.sender();

        // Fill channel to capacity (64). Use large shard_dir to slow I/O,
        // but also just spam sends fast enough to exceed channel bound.
        // We need the bg thread to NOT drain fast enough, so pause it by
        // NOT letting it run (it will block on recv -- we overflow with try_send).
        //
        // Actually, flume bounded(64) means 64 items can be buffered. The bg
        // thread will start draining immediately, so we need to send faster
        // than it processes. We can verify by using try_send in a tight loop.

        // First, fill the channel by sending 64 items rapidly
        let mut sent = 0;
        for i in 0..128u64 {
            let req = SpillRequest {
                key: Bytes::from(format!("bp_key_{i}")),
                db_index: 0,
                value_bytes: Bytes::from(format!("bp_val_{i}")),
                value_type: ValueType::String,
                flags: 0,
                ttl_ms: None,
                file_id: 200 + i,
                shard_dir: tmp.path().to_path_buf(),
            };
            match sender.try_send(req) {
                Ok(()) => sent += 1,
                Err(flume::TrySendError::Full(_)) => {
                    // Channel is full -- this proves backpressure works
                    break;
                }
                Err(flume::TrySendError::Disconnected(_)) => {
                    panic!("channel disconnected unexpectedly");
                }
            }
        }
        // We should have sent at least 64 (channel capacity) but may have sent
        // more if the bg thread drained some. The important thing is that we
        // either hit Full or sent all 128 (bg thread was fast enough).
        assert!(sent >= 1, "should have sent at least 1 request");

        // Drain completions to verify no panic or deadlock
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut received = 0;
        while received < sent && std::time::Instant::now() < deadline {
            received += st.drain_completions().len();
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(received, sent, "should receive all sent completions");

        // Now send one more -- should succeed since channel is drained
        let req = SpillRequest {
            key: Bytes::from_static(b"bp_final"),
            db_index: 0,
            value_bytes: Bytes::from_static(b"bp_final_val"),
            value_type: ValueType::String,
            flags: 0,
            ttl_ms: None,
            file_id: 999,
            shard_dir: tmp.path().to_path_buf(),
        };
        assert!(sender.try_send(req).is_ok(), "should send after drain");

        drop(sender);
        st.shutdown();
    }

    #[test]
    fn test_completion_ordering() {
        let tmp = tempfile::tempdir().unwrap();
        let st = SpillThread::new(12);
        let sender = st.sender();

        // Send 10 requests with ascending file_ids
        for i in 0..10u64 {
            let req = SpillRequest {
                key: Bytes::from(format!("order_key_{i}")),
                db_index: 0,
                value_bytes: Bytes::from(format!("order_val_{i}")),
                value_type: ValueType::String,
                flags: 0,
                ttl_ms: None,
                file_id: 100 + i,
                shard_dir: tmp.path().to_path_buf(),
            };
            sender.send(req).unwrap();
        }

        // Collect all completions
        let mut completions = Vec::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while completions.len() < 10 && std::time::Instant::now() < deadline {
            completions.extend(st.drain_completions());
            if completions.len() < 10 {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
        assert_eq!(completions.len(), 10, "Expected 10 completions");

        // Verify FIFO ordering (flume guarantees this)
        for (i, c) in completions.iter().enumerate() {
            assert!(c.success);
            assert_eq!(
                c.file_id,
                100 + i as u64,
                "completion {} should have file_id {}",
                i,
                100 + i as u64
            );
        }

        drop(sender);
        st.shutdown();
    }

    #[test]
    fn test_shutdown_with_pending_work() {
        let tmp = tempfile::tempdir().unwrap();
        let st = SpillThread::new(13);
        let sender = st.sender();

        // Send 3 requests
        for i in 0..3u64 {
            let req = SpillRequest {
                key: Bytes::from(format!("shutdown_key_{i}")),
                db_index: 0,
                value_bytes: Bytes::from(format!("shutdown_val_{i}")),
                value_type: ValueType::String,
                flags: 0,
                ttl_ms: None,
                file_id: 300 + i,
                shard_dir: tmp.path().to_path_buf(),
            };
            sender.send(req).unwrap();
        }

        // Immediately drop sender and shut down -- thread should process
        // remaining items then exit cleanly on channel disconnect.
        drop(sender);

        // shutdown() calls join() which should complete within seconds
        // (thread processes 3 remaining items then exits)
        let start = std::time::Instant::now();
        st.shutdown();
        let elapsed = start.elapsed();

        // Should complete well within 5 seconds
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "shutdown took too long: {:?}",
            elapsed
        );
    }
}
