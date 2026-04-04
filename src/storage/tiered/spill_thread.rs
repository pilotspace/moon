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

use bytes::Bytes;
use tracing::warn;

use crate::persistence::kv_page::{
    KvLeafPage, PageFull, ValueType, entry_flags, write_datafile,
    build_overflow_chain, write_datafile_mixed,
};
use crate::persistence::manifest::{FileEntry, FileStatus, StorageTier};
use crate::persistence::page::{PageType, PAGE_4K};

/// Request sent from event loop to background spill thread.
///
/// Contains all data needed for pwrite -- no references to shard state.
/// `Bytes` fields are reference-counted (cheap clone on event loop side).
pub struct SpillRequest {
    pub key: Bytes,
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
/// Returns `(page_count, byte_size)` on success. This is the I/O-only
/// portion extracted from `kv_spill::spill_to_datafile`.
fn write_spill_file(req: &SpillRequest) -> io::Result<(u32, u64)> {
    let mut page = KvLeafPage::new(0, req.file_id);
    let overflow_pages: Vec<crate::persistence::kv_page::KvOverflowPage>;
    let total_pages: u32;

    match page.insert(req.key.as_ref(), req.value_bytes.as_ref(), req.value_type, req.flags, req.ttl_ms) {
        Ok(_) => {
            overflow_pages = Vec::new();
            total_pages = 1;
        }
        Err(PageFull) => {
            let chain = build_overflow_chain(req.value_bytes.as_ref(), req.file_id, 1);
            let chain_len = chain.len() as u32;

            let overflow_ptr = 1u32.to_le_bytes();
            let overflow_flags = req.flags | entry_flags::OVERFLOW;
            match page.insert(req.key.as_ref(), &overflow_ptr, req.value_type, overflow_flags, req.ttl_ms) {
                Ok(_) => {}
                Err(PageFull) => {
                    warn!(
                        key_len = req.key.len(),
                        "spill_thread: key too large for leaf page even with overflow pointer"
                    );
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "key too large for leaf page",
                    ));
                }
            }
            overflow_pages = chain;
            total_pages = 1 + chain_len;
        }
    }
    page.finalize();

    // Ensure data directory exists
    let data_dir = req.shard_dir.join("data");
    std::fs::create_dir_all(&data_dir)?;

    // Write DataFile
    let file_path = data_dir.join(format!("heap-{:06}.mpf", req.file_id));
    if overflow_pages.is_empty() {
        write_datafile(&file_path, &[&page])?;
    } else {
        write_datafile_mixed(&file_path, &page, &overflow_pages)?;
    }

    Ok((total_pages, (total_pages as u64) * (PAGE_4K as u64)))
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
}

impl SpillThread {
    /// Spawn a new background spill thread for the given shard.
    ///
    /// Creates two flume channels:
    /// - `request`: bounded(64), event loop -> bg thread
    /// - `completion`: unbounded, bg thread -> event loop
    pub fn new(shard_id: usize) -> Self {
        let (request_tx, request_rx) = flume::bounded::<SpillRequest>(64);
        let (completion_tx, completion_rx) = flume::unbounded::<SpillCompletion>();

        let join_handle = std::thread::Builder::new()
            .name(format!("spill-{shard_id}"))
            .spawn(move || {
                Self::run(request_rx, completion_tx);
            })
            .expect("failed to spawn spill thread");

        Self {
            request_tx,
            completion_rx,
            join_handle: Some(join_handle),
        }
    }

    /// Background thread main loop.
    fn run(
        request_rx: flume::Receiver<SpillRequest>,
        completion_tx: flume::Sender<SpillCompletion>,
    ) {
        while let Ok(req) = request_rx.recv() {
            let file_id = req.file_id;
            let key = req.key.clone();

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
                file_id,
                slot_idx: 0,
                file_entry,
                success,
            };

            if completion_tx.send(completion).is_err() {
                // Event loop dropped its receiver -- shutting down
                break;
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
    /// Drops the internal request sender and joins the thread.
    ///
    /// **Important:** The caller MUST drop all cloned senders (from `sender()`)
    /// before calling this, otherwise the background thread will not exit and
    /// `join` will block indefinitely.
    pub fn shutdown(mut self) {
        // Drop the sender to signal the bg thread to stop.
        // NOTE: if cloned senders still exist, the channel stays open.
        let (dead_tx, _) = flume::bounded(1);
        // Swap in a disconnected sender so the real one is dropped
        std::mem::drop(std::mem::replace(&mut self.request_tx, dead_tx));

        if let Some(handle) = self.join_handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::kv_page::{read_datafile, ValueType};
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
            value_bytes: Bytes::from_static(b"test_value"),
            value_type: ValueType::String,
            flags: 0,
            ttl_ms: None,
            file_id: 1,
            shard_dir: tmp.path().to_path_buf(),
        };
        sender.send(req).unwrap();

        // Wait for completion
        let completion = st.completion_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
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
            value_bytes: Bytes::from_static(b"expiring_val"),
            value_type: ValueType::String,
            flags: entry_flags::HAS_TTL,
            ttl_ms: Some(future_ms),
            file_id: 2,
            shard_dir: tmp.path().to_path_buf(),
        };
        sender.send(req).unwrap();

        let completion = st.completion_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
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
            let c = st.completion_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
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
}
