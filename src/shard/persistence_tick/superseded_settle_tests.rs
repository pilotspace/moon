//! moon#1253: `Database::spill_superseded` is bounded. It remembers every
//! spill request a write retired while it was in flight, until the request's
//! completion is applied, and it must forget the request on EVERY outcome of
//! that completion, or the set leaks one key per superseded spill:
//!
//! - published: its file is listed and the superseded key is a ghost slot
//!   in the dead-slot ledger;
//! - failed write: no file;
//! - withdrawn: the `MOON.SPILLED` marker was refused and the keys go back
//!   to RAM;
//! - id rejected: the manifest already lists the file id, so the keys are
//!   rehydrated.

use super::*;
use crate::persistence::aof::{AofMessage, AofWriterPool};
use crate::persistence::kv_page::ValueType;
use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
use crate::persistence::page::PageType;
use crate::shard::slice::{ShardSlice, init_shard, test_support::make_init, with_shard_db};
use crate::storage::db::PendingSpill;
use crate::storage::tiered::spill_thread::{SpillCompletion, SpillCompletionEntry, SpillRequest};

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

fn published(file_id: u64, keys: &[&'static [u8]]) -> SpillCompletion {
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

fn failed(file_id: u64, key: &'static [u8]) -> SpillCompletion {
    SpillCompletion {
        file_entry: file_entry(file_id),
        entries: Vec::new(),
        success: false,
        failed_request: Some(Box::new(SpillRequest {
            key: bytes::Bytes::from_static(key),
            db_index: 0,
            value_bytes: bytes::Bytes::from_static(b"v"),
            value_type: ValueType::String,
            flags: 0,
            ttl_ms: None,
            file_id,
            shard_dir: std::path::PathBuf::new(),
        })),
    }
}

/// Evict `key` into request `req_id`, then DEL it while the spill is in
/// flight: the request is superseded.
fn in_flight_then_deleted(key: &'static [u8], req_id: u64) {
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
        assert!(db.remove_counting_cold(key).0, "DEL of an in-flight key");
    });
}

fn in_flight(key: &'static [u8], req_id: u64) {
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

fn superseded() -> usize {
    with_shard_db(0, |db| db.spill_superseded_len())
}

#[test]
fn every_completion_outcome_settles_its_superseded_request() {
    std::thread::spawn(|| {
        init_shard(ShardSlice::new(make_init(0, 1)));
        let tmp = tempfile::tempdir().unwrap();
        let mut manifest = ShardManifest::create(&tmp.path().join("shard-0.manifest")).unwrap();
        // File 9 is already listed: its completion is id-rejected.
        manifest.add_file(file_entry(9)).unwrap();
        let mut shard_manifest = Some(manifest);
        with_shard_db(0, |db| {
            db.cold_index = Some(crate::storage::tiered::cold_index::ColdIndex::new());
        });
        let baseline = with_shard_db(0, |db| db.pending_spill_bytes());

        // Published: `kept` publishes file 5, `ghost` is its ghost slot.
        in_flight(b"kept", 5);
        in_flight_then_deleted(b"ghost", 5);
        // Failed write.
        in_flight_then_deleted(b"lost-write", 6);
        // Id rejected (rehydrated).
        in_flight(b"rehydrated", 9);
        in_flight_then_deleted(b"rehydrated-ghost", 9);
        assert_eq!(superseded(), 3, "three requests superseded in flight");

        let mut sink = ColdMarkerSink {
            aof_pool: None,
            wal_writer: None,
            shard_id: 0,
            wal_kv_log: false,
        };
        apply_completion_vec(
            vec![
                published(5, &[b"kept", b"ghost"]),
                failed(6, b"lost-write"),
                published(9, &[b"rehydrated", b"rehydrated-ghost"]),
            ],
            &mut shard_manifest,
            &mut sink,
        );
        assert_eq!(
            superseded(),
            0,
            "published, failed and id-rejected all settle"
        );

        // Withdrawn: a saturated AOF writer refuses the marker.
        in_flight(b"withdrawn", 7);
        in_flight_then_deleted(b"withdrawn-ghost", 7);
        assert_eq!(superseded(), 1);
        let (tx, _rx) = crate::runtime::channel::mpsc_bounded::<AofMessage>(1);
        let pool = AofWriterPool::top_level(tx);
        assert!(pool.try_send_append(0, 1, 0, bytes::Bytes::from_static(b"filler")));
        let mut sink = ColdMarkerSink {
            aof_pool: Some(&pool),
            wal_writer: None,
            shard_id: 0,
            wal_kv_log: false,
        };
        apply_completion_vec(
            vec![published(7, &[b"withdrawn", b"withdrawn-ghost"])],
            &mut shard_manifest,
            &mut sink,
        );
        assert_eq!(superseded(), 0, "a withdrawn completion settles too");

        with_shard_db(0, |db| {
            let ci = db.cold_index.as_ref().unwrap();
            assert!(ci.lookup(b"kept").is_some(), "file 5 published `kept`");
            assert!(
                ci.dead_slots().file_has_dead_slots(5),
                "the ghost reached the ledger once the set let go of it"
            );
            assert!(db.is_hot(b"rehydrated") && db.is_hot(b"withdrawn"));
            assert!(!db.is_hot(b"ghost") && !db.is_hot(b"withdrawn-ghost"));
            assert_eq!(
                db.pending_spill_bytes(),
                baseline,
                "every handle credited back"
            );
        });
    })
    .join()
    .expect("shard thread");
}
