//! moon#1279 (the Windows/tokio variant) — a spill completion that
//! published nothing. Split out of `persistence_tick.rs` (file-size rule).

use super::{ColdMarkerSink, apply_completion_vec};
use crate::persistence::manifest::ShardManifest;

/// moon#1279 (the Windows/tokio variant): a completed spill every key of
/// which was superseded while in flight (deleted, overwritten or
/// promoted) published nothing. It used to be LISTED anyway, with only
/// ghost slots no index entry references — never zero-ref, so no sweep
/// ever queued it (`cold_files_dead:1` for good). It is now left out of
/// the manifest and unlinked at once: no `MOON.SPILLED` marker names it.
#[test]
fn a_spill_whose_every_key_was_superseded_is_not_listed_and_is_unlinked() {
    use crate::persistence::kv_page::ValueType;
    use crate::persistence::manifest::{FileEntry, FileStatus, StorageTier};
    use crate::persistence::page::PageType;
    use crate::shard::slice::{ShardSlice, init_shard, test_support::make_init, with_shard_db};
    use crate::storage::tiered::spill_thread::{SpillCompletion, SpillCompletionEntry};
    std::thread::spawn(|| {
        init_shard(ShardSlice::new(make_init(0, 1)));
        let tmp = tempfile::tempdir().unwrap();
        let entry = FileEntry {
            file_id: 7,
            file_type: PageType::KvLeaf as u8,
            status: FileStatus::Active,
            tier: StorageTier::Hot,
            page_size_log2: 12,
            page_count: 1,
            byte_size: 4096,
            created_lsn: 0,
            db_index: 0,
            max_key_hash: 0,
            last_modified_lsn: 0,
        };
        let manifest = ShardManifest::create(&tmp.path().join("shard-0.manifest")).unwrap();
        let heap = tmp.path().join("data").join("heap-000007.mpf");
        std::fs::create_dir_all(heap.parent().unwrap()).unwrap();
        std::fs::write(&heap, vec![0u8; 4096]).unwrap();
        with_shard_db(0, |db| {
            db.cold_index = Some(crate::storage::tiered::cold_index::ColdIndex::new());
        });
        // No in-flight record for any key: each was superseded (a DEL
        // retired it) before the completion arrived.
        let completion = SpillCompletion {
            file_entry: entry,
            entries: (0..3)
                .map(|i| SpillCompletionEntry {
                    key: bytes::Bytes::from(format!("gone{i}")),
                    db_index: 0,
                    page_idx: 0,
                    slot_idx: i as u16,
                    ttl_ms: None,
                    value_type: ValueType::String,
                    req_file_id: 7,
                })
                .collect(),
            success: true,
            failed_request: None,
        };
        let mut sink = ColdMarkerSink {
            aof_pool: None,
            wal_writer: None,
            shard_id: 0,
            wal_kv_log: false,
        };
        let mut shard_manifest = Some(manifest);
        apply_completion_vec(vec![completion], &mut shard_manifest, &mut sink);
        assert!(
            !shard_manifest
                .as_ref()
                .unwrap()
                .has_entry(7, PageType::KvLeaf as u8),
            "a spill that published nothing must not be listed"
        );
        assert!(!heap.exists(), "and its file is unlinked, not left dead");
        with_shard_db(0, |db| {
            let ci = db.cold_index.as_ref().unwrap();
            assert!(ci.lookup(b"gone0").is_none());
        });
    })
    .join()
    .expect("case thread");
}
