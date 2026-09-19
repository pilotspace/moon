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
                let _ = reply_tx.send(AofFoldSnapshot {
                    dbs: vec![Vec::new()],
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
