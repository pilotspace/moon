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
use crate::shard::slice::{
    ShardSlice, init_shard, test_support::make_init, with_shard, with_shard_db,
};
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
                "the pending charge is back to where it started"
            );
            assert_eq!(db.spill_superseded_bytes(), 0, "every handle let go");
        });
    })
    .join()
    .expect("shard thread");
}

/// `SWAPDB` swaps whole databases, their superseded sets included, while a
/// completion looks its request up in the db index the request was made in,
/// which after the swap holds the OTHER database. The request must settle
/// wherever it went, on every outcome that looks it up (review of moon#1253):
/// before, it stayed in the swapped database until a restart, and every AOF
/// rewrite of that db scanned it again.
///
/// Not asserted, and not changed here: the NEWEST record of a key in flight
/// across the swap (`kept`) moved too, so its completion takes it for
/// superseded and ghosts its slot; the record itself stays in the swapped
/// database until the key is next written.
#[test]
fn swapdb_strands_a_superseded_request() {
    std::thread::spawn(|| {
        init_shard(ShardSlice::new(make_init(0, 2)));
        let tmp = tempfile::tempdir().unwrap();
        let mut manifest = ShardManifest::create(&tmp.path().join("shard-0.manifest")).unwrap();
        // File 9 is already listed: its completion is id-rejected.
        manifest.add_file(file_entry(9)).unwrap();
        let mut shard_manifest = Some(manifest);
        for d in 0..2 {
            with_shard_db(d, |db| {
                db.cold_index = Some(crate::storage::tiered::cold_index::ColdIndex::new());
            });
        }
        // Published (5), failed write (6), id-rejected and rehydrated (9):
        // the three places a completion settles, all in db 0.
        in_flight(b"kept", 5);
        in_flight_then_deleted(b"ghost", 5);
        in_flight_then_deleted(b"lost-write", 6);
        in_flight(b"rehydrated", 9);
        in_flight_then_deleted(b"rehydrated-ghost", 9);
        assert_eq!(superseded(), 3);

        // SWAPDB 0 1 while the three requests are in flight.
        with_shard(|s| s.databases.swap(0, 1));
        assert_eq!(superseded(), 0);
        assert_eq!(with_shard_db(1, |db| db.spill_superseded_len()), 3);

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
        for d in 0..2 {
            with_shard_db(d, |db| {
                assert_eq!(
                    db.spill_superseded_len(),
                    0,
                    "db {d}: a superseded request is stranded across SWAPDB"
                );
                assert_eq!(db.spill_superseded_bytes(), 0, "db {d}: handles let go");
            });
        }
        let now = crate::storage::entry::current_time_ms();
        let head_dels = with_shard_db(1, |db| {
            let mut keys = Vec::new();
            crate::persistence::aof::fold_stream::for_each_cold_delete_chunk(
                db,
                1,
                now,
                Vec::new(),
                |chunk| {
                    keys.extend(chunk.keys);
                    true
                },
            );
            keys
        });
        assert!(
            head_dels.is_empty(),
            "no head DEL left behind in the swapped db: {head_dels:?}"
        );
    })
    .join()
    .expect("shard thread");
}

/// moon#1255, the completion half: a key whose in-flight TTL passed is
/// re-created by a collection write. The expired record is retired (superseded)
/// by the write, so its completion must NOT publish the stale slot as the
/// key's cold entry behind the live list. That publish also logged the
/// `MOON.SPILLED` marker AFTER the write, and replay then dropped the write.
#[test]
fn an_expired_in_flight_record_is_not_published_behind_a_recreated_key() {
    std::thread::spawn(|| {
        init_shard(ShardSlice::new(make_init(0, 1)));
        let tmp = tempfile::tempdir().unwrap();
        let mut shard_manifest =
            Some(ShardManifest::create(&tmp.path().join("shard-0.manifest")).unwrap());
        with_shard_db(0, |db| {
            db.cold_index = Some(crate::storage::tiered::cold_index::ColdIndex::new());
            db.spill_inflight_mark(
                bytes::Bytes::from_static(b"k"),
                PendingSpill {
                    req_id: 7,
                    value_type: ValueType::String,
                    value_bytes: bytes::Bytes::from_static(b"old"),
                    ttl_ms: Some(1),
                },
            );
            db.get_or_create_list(b"k")
                .expect("list")
                .push_back(bytes::Bytes::from_static(b"a"));
        });
        let mut sink = ColdMarkerSink {
            aof_pool: None,
            wal_writer: None,
            shard_id: 0,
            wal_kv_log: false,
        };
        apply_completion_vec(vec![published(7, &[b"k"])], &mut shard_manifest, &mut sink);
        with_shard_db(0, |db| {
            assert!(db.is_hot(b"k"), "the RPUSH value is live");
            assert!(
                db.cold_index.as_ref().unwrap().lookup(b"k").is_none(),
                "request 7 published k's EXPIRED slot as its cold entry behind the live list"
            );
            assert!(db.spill_superseded_is_empty(), "settled by the completion");
        });
    })
    .join()
    .expect("shard thread");
}

/// Refs moon#1253: a superseded request whose completion never arrives
/// (dropped at shutdown, say) stayed in the set for good. The spill thread's
/// watermark prunes it once a later request's completion has been sent and
/// applied; a request still queued stays.
#[test]
fn a_superseded_request_whose_completion_never_arrives_is_pruned_by_the_watermark() {
    std::thread::spawn(|| {
        init_shard(ShardSlice::new(make_init(0, 1)));
        let tmp = tempfile::tempdir().unwrap();
        let mut shard_manifest =
            Some(ShardManifest::create(&tmp.path().join("shard-0.manifest")).unwrap());
        with_shard_db(0, |db| {
            db.cold_index = Some(crate::storage::tiered::cold_index::ColdIndex::new());
        });
        let mut sink = ColdMarkerSink {
            aof_pool: None,
            wal_writer: None,
            shard_id: 0,
            wal_kv_log: false,
        };
        // Request 5's completion never comes; request 20 is still queued.
        in_flight_then_deleted(b"lost", 5);
        in_flight_then_deleted(b"queued", 20);
        assert_eq!(superseded(), 2);

        // Request 7 goes through a real spill thread.
        let st = crate::storage::tiered::spill_thread::SpillThread::new(0);
        in_flight(b"spilled", 7);
        st.sender()
            .send(SpillRequest {
                key: bytes::Bytes::from_static(b"spilled"),
                db_index: 0,
                value_bytes: bytes::Bytes::from_static(b"v"),
                value_type: ValueType::String,
                flags: 0,
                ttl_ms: None,
                file_id: 7,
                shard_dir: tmp.path().to_path_buf(),
            })
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while st.done_below() < 8 {
            assert!(std::time::Instant::now() < deadline, "no flush in 5 s");
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let bytes_before = with_shard_db(0, |db| db.spill_superseded_bytes());
        drain_and_apply(&st, &mut shard_manifest, &mut sink, 1);
        with_shard_db(0, |db| {
            assert_eq!(db.spill_superseded_len(), 1, "only request 20 is left");
            let left: Vec<_> = db.spill_superseded_keys_live_at(0).cloned().collect();
            assert_eq!(left, vec![bytes::Bytes::from_static(b"queued")]);
            let after = db.spill_superseded_bytes();
            assert!(
                after > 0 && after < bytes_before,
                "the pruned entry's bytes are credited"
            );
        });
        let _ = st.shutdown();

        // A dead spill thread: nothing it had will ever complete.
        let dead = crate::storage::tiered::spill_thread::SpillThread::exited_for_test();
        drain_and_apply(&dead, &mut shard_manifest, &mut sink, 1);
        with_shard_db(0, |db| {
            assert!(db.spill_superseded_is_empty());
            assert_eq!(db.spill_superseded_bytes(), 0);
        });
    })
    .join()
    .unwrap();
}
