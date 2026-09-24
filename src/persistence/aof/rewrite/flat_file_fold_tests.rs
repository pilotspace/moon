//! The flat-file fold (`rewrite_aof_sharded_sync`, the tokio `--shards 1`
//! layout) publishes its new file with a rename over `appendonly.aof`.

use std::sync::Arc;
use std::time::Duration;

use ringbuf::HeapRb;
use ringbuf::traits::{Consumer, Split};

use crate::persistence::aof::{AofMessage, FoldEpoch};
use crate::persistence::fsync::dir_fsync_probe;
use crate::runtime::channel;
use crate::shard::dispatch::{AofFoldSnapshot, ShardMessage};
use crate::shard::shared_databases::ShardDatabases;
use crate::storage::Database;

/// The writer keeps appending, and fsyncing, into the renamed file. An
/// fsync of a file does not make its name durable: without a directory fsync
/// after the rename, a power loss can bring back the OLD `appendonly.aof`,
/// and every record the writer acknowledged after the fold is gone.
#[test]
fn the_fold_fsyncs_the_directory_after_renaming_the_new_file_into_place() {
    let dir = tempfile::tempdir().expect("tempdir");
    let aof_path = dir.path().join("appendonly.aof");
    let mut old_file = std::fs::File::create(&aof_path).expect("old aof");
    let (shard_dbs, _inits) = ShardDatabases::new(vec![vec![Database::new()]]);
    let (_tx, rx) = channel::mpsc_bounded::<AofMessage>(8);

    // The shard side of the fold: answer one AofFold with an empty snapshot.
    let (prod, mut cons) = HeapRb::<ShardMessage>::new(4).split();
    let fold_channels = (
        Arc::new(parking_lot::Mutex::new(prod)),
        Arc::new(channel::Notify::new()),
    );
    let shard = std::thread::spawn(move || {
        for _ in 0..5000 {
            if let Some(ShardMessage::AofFold { reply_tx }) = cons.try_pop() {
                let (sink, image) = crate::persistence::aof::fold_stream::fold_image_channel();
                crate::persistence::aof::fold_stream::stream_fold_image(
                    &[&Database::new()],
                    0,
                    sink,
                );
                let _ = reply_tx.send(AofFoldSnapshot {
                    image,
                    pending_aof_count: 0,
                    cold_file_watermark: 1,
                    fold_epoch: FoldEpoch(1),
                });
                return;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!("no AofFold request arrived");
    });

    let mut last_db = 0usize;
    dir_fsync_probe::start();
    let result = super::rewrite_aof_sharded_sync(
        &shard_dbs,
        &aof_path,
        &rx,
        &mut old_file,
        Some(&fold_channels),
        &mut last_db,
        FoldEpoch::INITIAL,
    );
    let fsyncs = dir_fsync_probe::stop();
    shard.join().expect("shard thread");
    let (outcome, _new_file) = result.expect("the fold commits");
    assert!(matches!(outcome, super::FoldOutcome::Committed { .. }));

    let published = fsyncs.iter().any(|(d, names)| {
        d == dir.path()
            && names.iter().any(|n| n == "appendonly.aof")
            && !names.iter().any(|n| n == "appendonly.aof.tmp")
    });
    assert!(
        published,
        "no fsync of {} ran after the rename published the new file; fsyncs seen: {:?}",
        dir.path().display(),
        fsyncs
    );
}

/// moon#1215 + moon#1223 through the flat-file writer: the fold's cold
/// deletes reach the published file right after its `MOON.COLDCUT`, and an
/// in-flight spill is in its base. Replaying the published file on the same
/// cold index keeps the dead key dead and restores the in-flight one.
#[test]
fn the_flat_file_fold_writes_the_cold_deletes_and_the_in_flight_keys() {
    use bytes::Bytes;

    let dir = tempfile::tempdir().expect("tempdir");
    let aof_path = dir.path().join("appendonly.aof");
    let mut old_file = std::fs::File::create(&aof_path).expect("old aof");
    let (shard_dbs, _inits) = ShardDatabases::new(vec![vec![Database::new()]]);
    let (_tx, rx) = channel::mpsc_bounded::<AofMessage>(8);

    let (prod, mut cons) = HeapRb::<ShardMessage>::new(4).split();
    let fold_channels = (
        Arc::new(parking_lot::Mutex::new(prod)),
        Arc::new(channel::Notify::new()),
    );
    let shard = std::thread::spawn(move || {
        // The live shard: `dead` was cold in file 1 and DELeted (its slot
        // is still there, next to live neighbours); `flying` is mid-spill.
        let mut db = Database::new();
        let mut ci = crate::storage::tiered::cold_index::ColdIndex::new();
        let loc = crate::storage::tiered::cold_index::ColdLocation {
            file_id: 1,
            page_idx: 0,
            slot_idx: 0,
            ttl_ms: None,
            value_type: crate::persistence::kv_page::ValueType::String,
        };
        ci.insert(Bytes::from_static(b"dead"), loc);
        ci.insert(Bytes::from_static(b"neighbour"), loc);
        db.cold_index = Some(ci);
        assert!(db.remove_counting_cold(b"dead").0);
        db.spill_inflight_mark(
            Bytes::from_static(b"flying"),
            crate::storage::db::PendingSpill {
                req_id: 2,
                value_type: crate::persistence::kv_page::ValueType::String,
                value_bytes: Bytes::from_static(b"in-flight-value"),
                ttl_ms: None,
            },
        );
        for _ in 0..5000 {
            if let Some(ShardMessage::AofFold { reply_tx }) = cons.try_pop() {
                let (sink, image) = crate::persistence::aof::fold_stream::fold_image_channel();
                let _ = reply_tx.send(AofFoldSnapshot {
                    image,
                    pending_aof_count: 0,
                    cold_file_watermark: 3,
                    fold_epoch: FoldEpoch(1),
                });
                crate::persistence::aof::fold_stream::stream_fold_image(
                    &[&db],
                    crate::storage::entry::current_time_ms(),
                    sink,
                );
                return;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!("no AofFold request arrived");
    });

    let mut last_db = 0usize;
    let result = super::rewrite_aof_sharded_sync(
        &shard_dbs,
        &aof_path,
        &rx,
        &mut old_file,
        Some(&fold_channels),
        &mut last_db,
        FoldEpoch::INITIAL,
    );
    shard.join().expect("shard thread");
    let (outcome, _new_file) = result.expect("the fold commits");
    assert!(matches!(outcome, super::FoldOutcome::Committed { .. }));

    let published = std::fs::read(&aof_path).expect("published aof");
    let mut head = crate::persistence::cold_records::serialize_cold_cut(3).to_vec();
    head.extend_from_slice(b"*2\r\n$3\r\nDEL\r\n$4\r\ndead\r\n");
    assert!(
        published.ends_with(&head),
        "the published file must end with MOON.COLDCUT 3 + DEL dead, got tail {:?}",
        String::from_utf8_lossy(&published[published.len().saturating_sub(80)..])
    );

    // Replay it against the pre-rewrite cold index (what a restart rebuilds
    // from the still-listed file 1).
    let mut dbs = vec![Database::new()];
    let mut ci = crate::storage::tiered::cold_index::ColdIndex::new();
    let loc = crate::storage::tiered::cold_index::ColdLocation {
        file_id: 1,
        page_idx: 0,
        slot_idx: 0,
        ttl_ms: None,
        value_type: crate::persistence::kv_page::ValueType::String,
    };
    ci.insert(Bytes::from_static(b"dead"), loc);
    ci.insert(Bytes::from_static(b"neighbour"), loc);
    dbs[0].cold_index = Some(ci);
    crate::persistence::aof::replay_aof(
        &mut dbs,
        &aof_path,
        &crate::persistence::replay::DispatchReplayEngine::new(),
    )
    .expect("replay");
    let ci = dbs[0].cold_index.as_ref().expect("cold index kept");
    assert!(
        ci.lookup(b"dead").is_none(),
        "the dead slot must not come back"
    );
    assert!(
        ci.lookup(b"neighbour").is_some(),
        "its live neighbour stays"
    );
    assert_eq!(
        dbs[0]
            .data()
            .get(b"flying".as_slice())
            .and_then(|e| e.value.as_bytes().map(|b| b.to_vec()))
            .as_deref(),
        Some(&b"in-flight-value"[..]),
        "the in-flight key is in the base"
    );
}
