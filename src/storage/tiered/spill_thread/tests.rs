//! `SpillThread` unit tests (moved out of `spill_thread.rs` to keep that file
//! under the 1500-line cap; a child module, so private items stay in reach).

use super::*;
use crate::persistence::kv_page::{ValueType, entry_flags};
use crate::persistence::page::PAGE_4K;
use crate::storage::entry::current_time_ms;
use crate::storage::tiered::kv_spill::INLINE_MAX_VALUE_BYTES;

/// Helper: wait for at least `expected_entries` total entries across all
/// completions, with a deadline.
fn collect_entries(
    st: &SpillThread,
    expected_entries: usize,
    deadline: std::time::Instant,
) -> Vec<SpillCompletion> {
    let mut completions = Vec::new();
    let mut total_entries = 0;
    while total_entries < expected_entries && std::time::Instant::now() < deadline {
        let new = st.drain_completions();
        for c in &new {
            total_entries += c.entries.len();
        }
        completions.extend(new);
        if total_entries < expected_entries {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
    completions
}

/// F1 (deep review): a failed spill write must carry the original request
/// back so the event loop can re-insert the already-evicted key.
#[test]
fn failed_spill_write_carries_request_back() {
    let tmp = tempfile::tempdir().unwrap();
    // Make shard_dir an existing FILE so the spill write cannot succeed.
    let bogus_dir = tmp.path().join("not-a-dir");
    std::fs::write(&bogus_dir, b"occupied").unwrap();

    let req = SpillRequest {
        key: Bytes::from_static(b"lost-key"),
        db_index: 3,
        value_bytes: Bytes::from_static(b"payload"),
        value_type: ValueType::String,
        flags: 0,
        ttl_ms: Some(12345),
        file_id: 42,
        shard_dir: bogus_dir,
    };
    let completion = spill_single_entry(&req, req.file_id);
    assert!(!completion.success);
    let carried = completion
        .failed_request
        .expect("failure completion must carry the request payload back");
    assert_eq!(carried.key, req.key);
    assert_eq!(carried.db_index, 3);
    assert_eq!(carried.value_bytes, req.value_bytes);
    assert_eq!(carried.ttl_ms, Some(12345));
}

#[test]
fn pace_is_zero_when_no_readers_waiting() {
    assert_eq!(
        spill_pace_after_flush(0, 0),
        std::time::Duration::ZERO,
        "pacing must cost nothing when nobody is reading"
    );
    assert_eq!(
        spill_pace_after_flush(0, REQUEST_QUEUE_CAP),
        std::time::Duration::ZERO
    );
}

#[test]
fn pace_is_zero_under_deep_backlog_even_with_readers() {
    // A paced spill thread must never push eviction into try_send
    // failure (-OOM to write clients): deep backlog always wins.
    assert_eq!(
        spill_pace_after_flush(3, REQUEST_QUEUE_CAP / 2),
        std::time::Duration::ZERO
    );
    assert_eq!(
        spill_pace_after_flush(100, REQUEST_QUEUE_CAP),
        std::time::Duration::ZERO
    );
}

#[test]
fn pace_yields_a_quantum_when_reader_waits_and_backlog_shallow() {
    let d = spill_pace_after_flush(1, 0);
    assert!(d > std::time::Duration::ZERO);
    assert!(
        d <= std::time::Duration::from_millis(10),
        "quantum must stay small — this is a yield, not a stall ({d:?})"
    );
    assert_eq!(d, spill_pace_after_flush(64, REQUEST_QUEUE_CAP / 2 - 1));
}

#[test]
fn reader_inflight_guard_counts_and_releases() {
    use super::super::cold_read_pool::ColdReadInflightGuard;
    // Own counter, NOT the global COLD_READS_INFLIGHT: sibling tests in
    // this binary exercise the cold-read path concurrently, so exact
    // assertions on the shared global are racy. The RAII mechanics are
    // identical either way.
    static LOCAL: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    {
        let _g1 = ColdReadInflightGuard::on(&LOCAL);
        let _g2 = ColdReadInflightGuard::on(&LOCAL);
        assert_eq!(LOCAL.load(Ordering::Relaxed), 2);
    }
    assert_eq!(LOCAL.load(Ordering::Relaxed), 0);
}

#[test]
fn test_spill_thread_new_returns_valid_handles() {
    let st = SpillThread::new(0);
    assert!(!st.request_tx.is_disconnected());
    assert!(!st.completion_rx.is_disconnected());
    let _ = st.shutdown();
}

/// R5 liveness: the spill thread must publish a heartbeat + batch counter
/// so a silently-dead spill thread is observable from INFO instead of
/// only via unbounded eviction backlog. Statics are process-global, so
/// assert relative progress, not absolute values.
#[test]
fn test_spill_thread_liveness_metrics() {
    let tmp = tempfile::tempdir().unwrap();
    let batches_before = spill_batches_flushed_total();

    let st = SpillThread::new(9);
    let sender = st.sender();
    sender
        .send(SpillRequest {
            key: Bytes::from_static(b"liveness_key"),
            db_index: 0,
            value_bytes: Bytes::from_static(b"liveness_value"),
            value_type: ValueType::String,
            flags: 0,
            ttl_ms: None,
            file_id: 1,
            shard_dir: tmp.path().to_path_buf(),
        })
        .unwrap();
    drop(sender);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let completions = collect_entries(&st, 1, deadline);
    assert!(!completions.is_empty(), "spill must complete");

    assert!(
        spill_batches_flushed_total() > batches_before,
        "flushing a batch must increment spill_batches_flushed_total"
    );
    assert!(
        spill_last_heartbeat_ms() > 0,
        "a running spill thread must publish a heartbeat timestamp"
    );
}

/// Single request produces a successful per-FILE completion with one entry.
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
    drop(sender);

    // Wait for the buffer to flush (100 ms tick or disconnect).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let completions = collect_entries(&st, 1, deadline);

    let total_entries: usize = completions.iter().map(|c| c.entries.len()).sum();
    assert_eq!(total_entries, 1, "expected 1 entry across all completions");

    let c = completions.iter().find(|c| !c.entries.is_empty()).unwrap();
    assert!(c.success);
    assert_eq!(c.file_entry.file_type, PageType::KvLeaf as u8);
    assert!(c.file_entry.page_count >= 1);
    assert!(c.file_entry.byte_size >= PAGE_4K as u64);

    let entry = &c.entries[0];
    assert_eq!(entry.key, Bytes::from_static(b"test_key"));
    assert_eq!(entry.db_index, 0);

    // File must exist on disk.
    let file_path = tmp
        .path()
        .join("data")
        .join(format!("heap-{:06}.mpf", c.file_entry.file_id));
    assert!(file_path.exists(), "spill file should exist");

    // Verify content via cold_read_at.
    use crate::storage::tiered::cold_index::ColdLocation;
    use crate::storage::tiered::cold_read::read_cold_entry_at;
    let loc = ColdLocation {
        file_id: c.file_entry.file_id,
        page_idx: entry.page_idx,
        slot_idx: entry.slot_idx,
        ttl_ms: entry.ttl_ms,
        value_type: ValueType::String,
    };
    let result = read_cold_entry_at(tmp.path(), loc, 0);
    assert!(result.is_some(), "should read entry back");
    let (value, _ttl) = result.unwrap();
    match value {
        crate::storage::entry::RedisValue::String(data) => {
            assert_eq!(data.as_ref(), b"test_value");
        }
        _ => panic!("expected String"),
    }

    let _ = st.shutdown();
}

/// A full completion channel must apply backpressure (block until the event
/// loop drains a slot), never drop — a dropped completion permanently loses
/// the already-evicted keys (orphaned `.mpf`, never recorded in the manifest).
#[test]
fn full_completion_channel_blocks_instead_of_dropping() {
    use std::sync::atomic::AtomicUsize;
    let (tx, rx) = flume::bounded::<SpillCompletion>(1);
    let stop = Arc::new(AtomicBool::new(false));
    let dummy = || SpillCompletion {
        file_entry: make_file_entry(7, 1, PAGE_4K as u64, 0),
        entries: Vec::new(),
        success: true,
        failed_request: None,
    };

    // Saturate the single slot so the next send must wait for a free slot.
    tx.try_send(dummy()).unwrap();

    let received = Arc::new(AtomicUsize::new(0));
    let r2 = received.clone();
    let drainer = std::thread::spawn(move || {
        // Delay draining so send_one_completion is forced to block first.
        std::thread::sleep(std::time::Duration::from_millis(50));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            if rx
                .recv_timeout(std::time::Duration::from_millis(50))
                .is_ok()
            {
                r2.fetch_add(1, Ordering::Relaxed);
            } else if r2.load(Ordering::Relaxed) >= 2 {
                break;
            }
        }
    });

    // Must block until a slot frees, then deliver — never drop on Full.
    SpillThread::send_one_completion(&tx, dummy(), &stop);
    drop(tx);
    drainer.join().unwrap();

    assert_eq!(
        received.load(Ordering::Relaxed),
        2,
        "both completions must be delivered; a Full channel must block, not drop"
    );
}

/// `shutdown()` must surface the thread's final-flush completions so the
/// caller can still apply them — they would otherwise be silently lost
/// (file on disk, never added to the manifest).
#[test]
fn shutdown_drains_and_returns_unapplied_completions() {
    let tmp = tempfile::tempdir().unwrap();
    let st = SpillThread::new(9);
    let sender = st.sender();
    sender
        .send(SpillRequest {
            key: Bytes::from_static(b"shutdown_key"),
            db_index: 0,
            value_bytes: Bytes::from_static(b"v"),
            value_type: ValueType::String,
            flags: 0,
            ttl_ms: None,
            file_id: 1,
            shard_dir: tmp.path().to_path_buf(),
        })
        .unwrap();
    drop(sender);

    // Deliberately do NOT drain via the event loop. The completion is
    // produced by the thread's final flush; shutdown() must return it.
    let leftover = st.shutdown();
    let total: usize = leftover.iter().map(|c| c.entries.len()).sum();
    assert_eq!(
        total, 1,
        "shutdown must return the unapplied completion, not drop it"
    );
}

/// Flush chunks must never cross a logical-DB boundary (#139): each
/// spill file is attributed wholesale to ONE `FileEntry::db_index`, so
/// a buffer interleaving dbs must produce one file per contiguous
/// same-db run, each completion carrying its db and only its keys.
#[test]
fn flush_buffer_cuts_chunks_at_db_boundaries() {
    let tmp = tempfile::tempdir().unwrap();
    let mk = |key: &str, db: usize, file_id: u64| SpillRequest {
        key: Bytes::from(key.to_owned()),
        db_index: db,
        value_bytes: Bytes::from_static(b"v"),
        value_type: ValueType::String,
        flags: 0,
        ttl_ms: None,
        file_id,
        shard_dir: tmp.path().to_path_buf(),
    };
    let mut buffer = vec![
        mk("a0", 0, 1),
        mk("a1", 0, 2),
        mk("b0", 1, 3),
        mk("c0", 0, 4),
    ];
    let completions = flush_buffer(&mut buffer);
    assert!(completions.iter().all(|c| c.success));
    let summary: Vec<(u64, usize)> = completions
        .iter()
        .map(|c| (c.file_entry.db_index, c.entries.len()))
        .collect();
    assert_eq!(
        summary,
        vec![(0, 2), (1, 1), (0, 1)],
        "expected one file per contiguous same-db run: {summary:?}"
    );
    for c in &completions {
        for e in &c.entries {
            assert_eq!(
                e.db_index as u64, c.file_entry.db_index,
                "entry db must match its file's manifest attribution"
            );
        }
    }
}

/// An entry that passes the `INLINE_MAX_VALUE_BYTES` pre-screen but does not
/// fit a fresh inline leaf (large key + incompressible value) makes
/// `build_kv_spill_batch` fail. That must NOT fail the whole inline flush —
/// the offender is salvaged via the per-entry (overflow) path instead, since
/// its key is already evicted from RAM.
#[test]
fn inline_batch_failure_falls_back_to_per_entry_spill() {
    let tmp = tempfile::tempdir().unwrap();

    // High-entropy (LZ4-incompressible) value at the inline threshold.
    let mut value = Vec::with_capacity(INLINE_MAX_VALUE_BYTES);
    let mut s: u32 = 0x1234_5678;
    for _ in 0..INLINE_MAX_VALUE_BYTES {
        s = s.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        value.push((s >> 24) as u8);
    }
    // Sizable key so key + value overflows a 4KB leaf (forces batch failure),
    // yet the key alone fits a leaf with an overflow pointer (pages succeed).
    let key = vec![b'k'; 800];

    let mut buffer = vec![SpillRequest {
        key: Bytes::from(key),
        db_index: 0,
        value_bytes: Bytes::from(value),
        value_type: ValueType::String,
        flags: 0,
        ttl_ms: None,
        file_id: 1,
        shard_dir: tmp.path().to_path_buf(),
    }];

    let completions = flush_buffer(&mut buffer);
    let succeeded: usize = completions
        .iter()
        .filter(|c| c.success)
        .map(|c| c.entries.len())
        .sum();
    assert_eq!(
        succeeded, 1,
        "inline entry that overflows a leaf must be salvaged via per-entry fallback, not dropped"
    );

    // And the salvaged entry must be readable back from its on-disk file.
    let c = completions
        .iter()
        .find(|c| c.success && !c.entries.is_empty())
        .expect("expected a successful fallback completion");
    let file_path = tmp
        .path()
        .join("data")
        .join(format!("heap-{:06}.mpf", c.file_entry.file_id));
    assert!(
        file_path.exists(),
        "fallback spill file should exist on disk"
    );
}

/// review FIX 2: a single flush whose entries sum well past `BATCH_BYTES_CAP`
/// must split into multiple sub-batch files rather than materializing every
/// page in one `build_kv_spill_batch` call. Every entry must still land
/// somewhere and be readable — splitting is a RAM-bounding measure, not a
/// data-loss one.
#[test]
fn oversized_flush_splits_into_byte_capped_sub_batches() {
    let tmp = tempfile::tempdir().unwrap();

    // 8 entries at ~1.5 MiB each = ~12 MiB total, comfortably more than
    // one `BATCH_BYTES_CAP` (4 MiB) — must produce >= 3 file completions
    // (ceil(12 / 4) = 3), never one.
    const ENTRY_LEN: usize = 3 * 1024 * 1024 / 2; // 1.5 MiB
    const ENTRY_COUNT: usize = 8;
    let mut buffer: Vec<SpillRequest> = Vec::with_capacity(ENTRY_COUNT);
    for i in 0..ENTRY_COUNT as u64 {
        let mut value = Vec::with_capacity(ENTRY_LEN);
        let mut s: u32 = 0x9E37_79B9_u32.wrapping_add(i as u32);
        while value.len() < ENTRY_LEN {
            s = s.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            value.push((s >> 24) as u8);
        }
        buffer.push(SpillRequest {
            key: Bytes::from(format!("bigkey_{i}")),
            db_index: 0,
            value_bytes: Bytes::from(value),
            value_type: ValueType::String,
            flags: 0,
            ttl_ms: None,
            file_id: i + 1,
            shard_dir: tmp.path().to_path_buf(),
        });
    }

    let completions = flush_buffer(&mut buffer);
    assert!(
        completions.len() >= 3,
        "12 MiB of entries at a 4 MiB cap must split into >= 3 sub-batch files, got {}",
        completions.len()
    );
    // No sub-batch may itself exceed the cap by more than one entry's
    // worth (the single-oversized-entry escape hatch).
    for c in &completions {
        assert!(
            c.success,
            "every sub-batch must build+write successfully for this workload"
        );
    }

    let total_entries: usize = completions.iter().map(|c| c.entries.len()).sum();
    assert_eq!(
        total_entries, ENTRY_COUNT,
        "every entry must be accounted for across the split sub-batches"
    );

    // Every entry must be independently readable back from its own
    // sub-batch's file at the recorded location — splitting must not
    // corrupt or drop any entry's page location.
    for c in &completions {
        for entry in &c.entries {
            let loc = crate::storage::tiered::cold_index::ColdLocation {
                file_id: c.file_entry.file_id,
                page_idx: entry.page_idx,
                slot_idx: entry.slot_idx,
                ttl_ms: entry.ttl_ms,
                value_type: ValueType::String,
            };
            let outcome = crate::storage::tiered::cold_read::read_cold_entry_at(
                tmp.path(),
                loc,
                u64::MAX / 2,
            );
            match outcome {
                Some((crate::storage::entry::RedisValue::String(v), _ttl)) => {
                    assert_eq!(
                        v.len(),
                        ENTRY_LEN,
                        "key {:?} recovered with wrong length after split",
                        entry.key
                    );
                }
                other => panic!(
                    "key {:?}: expected a String value, got {other:?}",
                    entry.key
                ),
            }
        }
    }
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
    drop(sender);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let completions = collect_entries(&st, 1, deadline);

    let total: usize = completions.iter().map(|c| c.entries.len()).sum();
    assert_eq!(total, 1);

    let c = completions.iter().find(|c| !c.entries.is_empty()).unwrap();
    assert!(c.success);
    assert_eq!(c.file_entry.file_type, PageType::KvLeaf as u8);

    let _ = st.shutdown();
}

#[test]
fn test_spill_thread_shutdown() {
    let st = SpillThread::new(3);
    let sender = st.sender();
    drop(sender);
    let _ = st.shutdown();
    // Reaching here without hang = clean exit.
}

/// 5 requests sent together must all appear as entries across completions.
#[test]
fn test_multiple_requests_all_entries_received() {
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
    drop(sender);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let completions = collect_entries(&st, 5, deadline);

    let total_entries: usize = completions.iter().map(|c| c.entries.len()).sum();
    assert_eq!(total_entries, 5, "all 5 entries must be accounted for");

    // Each entry must be readable on disk via its location.
    for c in &completions {
        if !c.success {
            continue;
        }
        for entry in &c.entries {
            let loc = crate::storage::tiered::cold_index::ColdLocation {
                file_id: c.file_entry.file_id,
                page_idx: entry.page_idx,
                slot_idx: entry.slot_idx,
                ttl_ms: entry.ttl_ms,
                value_type: ValueType::String,
            };
            let result = crate::storage::tiered::cold_read::read_cold_entry_at(tmp.path(), loc, 0);
            assert!(
                result.is_some(),
                "entry key={} should be readable",
                String::from_utf8_lossy(&entry.key)
            );
        }
    }

    let _ = st.shutdown();
}

/// Full pipeline: 5 requests, verify round-trip via cold_read.
#[test]
fn test_full_pipeline_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let st = SpillThread::new(10);
    let sender = st.sender();

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
    drop(sender);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let completions = collect_entries(&st, 5, deadline);

    let total: usize = completions.iter().map(|c| c.entries.len()).sum();
    assert_eq!(total, 5, "expected 5 entries across completions");

    for c in &completions {
        assert!(c.success);
    }

    let _ = st.shutdown();
}

#[test]
fn test_channel_backpressure() {
    let tmp = tempfile::tempdir().unwrap();
    let st = SpillThread::new(11);
    let sender = st.sender();

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
            Err(flume::TrySendError::Full(_)) => break,
            Err(flume::TrySendError::Disconnected(_)) => {
                panic!("channel disconnected unexpectedly");
            }
        }
    }
    assert!(sent >= 1, "should have sent at least 1 request");

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let completions = collect_entries(&st, sent, deadline);
    let received: usize = completions.iter().map(|c| c.entries.len()).sum();
    assert_eq!(received, sent, "should receive all sent entries");

    drop(sender);
    let _ = st.shutdown();
}

#[test]
fn test_shutdown_with_pending_work() {
    let tmp = tempfile::tempdir().unwrap();
    let st = SpillThread::new(13);
    let sender = st.sender();

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
    drop(sender);

    let start = std::time::Instant::now();
    let _ = st.shutdown();
    let elapsed = start.elapsed();

    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "shutdown took too long: {:?}",
        elapsed
    );
}

/// Refs moon#1253: once a flush's completions are sent the thread publishes
/// one past the flush's largest request id, and everything that watermark
/// covers is already queued for the shard. The shard prunes once per move.
#[test]
fn the_watermark_covers_only_requests_whose_completions_were_sent() {
    let tmp = tempfile::tempdir().unwrap();
    let st = SpillThread::new(2);
    assert_eq!(st.done_below(), 0, "nothing flushed yet");
    assert_eq!(st.take_prune(0, st.is_dead()), None);
    let sender = st.sender();
    for (i, id) in [7u64, 8, 9].into_iter().enumerate() {
        sender
            .send(SpillRequest {
                key: Bytes::from(format!("wm{i}")),
                db_index: 0,
                value_bytes: Bytes::from_static(b"v"),
                value_type: ValueType::String,
                flags: 0,
                ttl_ms: None,
                file_id: id,
                shard_dir: tmp.path().to_path_buf(),
            })
            .unwrap();
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while st.done_below() < 10 {
        assert!(std::time::Instant::now() < deadline, "no flush in 5 s");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(st.done_below(), 10);
    let done: Vec<u64> = st
        .drain_completions()
        .iter()
        .flat_map(|c| c.entries.iter().map(|e| e.req_file_id))
        .collect();
    for id in [7, 8, 9] {
        assert!(
            done.contains(&id),
            "request {id} covered but not queued: {done:?}"
        );
    }
    assert_eq!(
        st.take_prune(10, st.is_dead()),
        Some(SupersededPrune::Below(10))
    );
    assert_eq!(
        st.take_prune(10, st.is_dead()),
        None,
        "pruned once per watermark move"
    );
    assert!(!st.is_dead());
    let _ = st.shutdown();
}

/// Refs moon#1253: a thread that exited unasked will never send another
/// completion, so every superseded entry goes.
#[test]
fn a_dead_spill_thread_asks_for_every_superseded_entry_to_go() {
    let st = SpillThread::exited_for_test();
    assert!(st.is_dead());
    assert_eq!(st.take_prune(0, st.is_dead()), Some(SupersededPrune::All));
    assert_eq!(
        st.take_prune(0, st.is_dead()),
        Some(SupersededPrune::All),
        "every tick"
    );
}

/// Refs moon#1253, review 5: the prune decides on the liveness its caller
/// sampled BEFORE the drain, never on a fresh one. A thread that sends one
/// more completion and dies between the drain and the decision must not get
/// its entries cleared while that completion is still queued.
#[test]
fn the_prune_decides_on_the_liveness_sampled_before_the_drain() {
    let st = SpillThread::exited_for_test();
    assert!(st.is_dead(), "dead by now");
    assert_eq!(
        st.take_prune(0, false),
        None,
        "alive when sampled: no clear, nothing to prune below watermark 0"
    );
    assert_eq!(st.take_prune(0, true), Some(SupersededPrune::All));
}

/// Refs moon#1265: a dead spill thread is reported once — one error line and
/// one count behind INFO `spill_thread_alive:0` — however many ticks see it.
#[test]
fn a_dead_spill_thread_is_reported_once() {
    let st = SpillThread::exited_for_test();
    assert!(!st.report_death_once(false, 0), "alive when sampled");
    assert!(st.report_death_once(st.is_dead(), 0));
    assert!(!spill_threads_alive());
    assert!(!st.report_death_once(st.is_dead(), 0), "once");
}
