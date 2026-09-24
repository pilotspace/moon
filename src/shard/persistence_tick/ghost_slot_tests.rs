//! moon#1215: a spill completion that publishes a file for SOME of its keys
//! leaves the others' slots on disk in a listed file — ghost slots a rebuild
//! would index. They must reach the cold index's dead-slot ledger; a file
//! that is not published (fully withdrawn) must not add any.

use super::*;
use crate::persistence::kv_page::ValueType;
use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
use crate::persistence::page::PageType;
use crate::shard::slice::{ShardSlice, init_shard, test_support::make_init, with_shard_db};
use crate::storage::db::PendingSpill;
use crate::storage::tiered::spill_thread::{SpillCompletion, SpillCompletionEntry};

fn file_entry(file_id: u64) -> FileEntry {
    FileEntry {
        file_id,
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
    }
}

fn completion(file_id: u64, keys: &[&'static [u8]]) -> SpillCompletion {
    SpillCompletion {
        file_entry: file_entry(file_id),
        entries: keys
            .iter()
            .enumerate()
            .map(|(i, k)| SpillCompletionEntry {
                key: bytes::Bytes::from_static(k),
                db_index: 0,
                page_idx: 0,
                slot_idx: i as u16,
                ttl_ms: None,
                value_type: ValueType::String,
                req_file_id: file_id,
            })
            .collect(),
        success: true,
        failed_request: None,
    }
}

fn mark(key: &'static [u8], req_id: u64) {
    with_shard_db(0, |db| {
        db.spill_inflight_mark(
            bytes::Bytes::from_static(key),
            PendingSpill {
                req_id,
                value_type: ValueType::String,
                value_bytes: bytes::Bytes::from_static(b"v"),
                ttl_ms: None,
            },
        );
    });
}

#[test]
fn a_superseded_key_in_a_published_file_is_a_dead_slot() {
    std::thread::spawn(|| {
        init_shard(ShardSlice::new(make_init(0, 1)));
        let tmp = tempfile::tempdir().unwrap();
        let manifest = ShardManifest::create(&tmp.path().join("shard-0.manifest")).unwrap();
        with_shard_db(0, |db| {
            db.cold_index = Some(crate::storage::tiered::cold_index::ColdIndex::new());
        });
        mark(b"kept", 5);
        // `deleted` was in flight too, but a DEL retired its record.
        let mut sink = ColdMarkerSink {
            aof_pool: None,
            wal_writer: None,
            shard_id: 0,
            wal_kv_log: false,
        };
        let mut shard_manifest = Some(manifest);
        apply_completion_vec(
            vec![completion(5, &[b"kept", b"deleted"])],
            &mut shard_manifest,
            &mut sink,
        );
        assert!(
            shard_manifest
                .as_ref()
                .unwrap()
                .has_entry(5, PageType::KvLeaf as u8)
        );
        with_shard_db(0, |db| {
            let ci = db.cold_index.as_ref().unwrap();
            assert!(ci.lookup(b"kept").is_some());
            assert!(ci.lookup(b"deleted").is_none());
            let dead: Vec<&[u8]> = ci.dead_slots().keys().map(|k| k.as_ref()).collect();
            assert_eq!(dead, vec![&b"deleted"[..]], "the ghost slot is recorded");
            assert!(ci.dead_slots().file_has_dead_slots(5));
        });
    })
    .join()
    .expect("shard thread");
}

#[test]
fn a_fully_withdrawn_file_adds_no_dead_slot() {
    use crate::persistence::aof::{AofMessage, AofWriterPool};
    std::thread::spawn(|| {
        init_shard(ShardSlice::new(make_init(0, 1)));
        let tmp = tempfile::tempdir().unwrap();
        let manifest = ShardManifest::create(&tmp.path().join("shard-0.manifest")).unwrap();
        with_shard_db(0, |db| {
            db.cold_index = Some(crate::storage::tiered::cold_index::ColdIndex::new());
        });
        mark(b"a", 5);
        mark(b"b", 5);
        // A full writer channel: the marker is refused, the spill withdrawn.
        let (tx, _rx) = crate::runtime::channel::mpsc_bounded::<AofMessage>(1);
        let pool = AofWriterPool::top_level(tx);
        assert!(pool.try_send_append(0, 1, 0, bytes::Bytes::from_static(b"filler")));
        let mut sink = ColdMarkerSink {
            aof_pool: Some(&pool),
            wal_writer: None,
            shard_id: 0,
            wal_kv_log: false,
        };
        let mut shard_manifest = Some(manifest);
        apply_completion_vec(
            vec![completion(5, &[b"a", b"b"])],
            &mut shard_manifest,
            &mut sink,
        );
        assert!(
            !shard_manifest
                .as_ref()
                .unwrap()
                .has_entry(5, PageType::KvLeaf as u8),
            "withdrawn: the file stays out of the manifest"
        );
        with_shard_db(0, |db| {
            let ci = db.cold_index.as_ref().unwrap();
            assert!(
                ci.dead_slots().is_empty(),
                "an unlisted file cannot resurrect anything"
            );
            assert!(
                db.is_hot(b"a") && db.is_hot(b"b"),
                "withdrawn keys are back in RAM"
            );
        });
    })
    .join()
    .expect("shard thread");
}
