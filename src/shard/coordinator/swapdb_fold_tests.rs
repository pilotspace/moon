//! SWAPDB against a concurrent AOF rewrite fold (#455).
//!
//! A fold snapshots the keyspace on the shard thread and splits the writer's
//! stream at that instant: records enqueued before the snapshot go to the old
//! incr (which the new base replaces), and records stamped below the snapshot
//! epoch are dropped wherever they surface later. SWAPDB therefore needs its
//! record's stamp, its enqueue, and the swap itself to fall on the same side
//! of every snapshot. These tests take a "snapshot" at the two suspension
//! points the local leg used to have and check the invariant there.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use bytes::Bytes;
use ringbuf::HeapProd;

use crate::persistence::aof::{AofAck, AofMessage, AofWriterPool, FoldEpoch, FsyncPolicy};
use crate::protocol::Frame;
use crate::runtime::channel;
use crate::shard::dispatch::ShardMessage;
use crate::shard::shared_databases::ShardDatabases;
use crate::storage::Database;

/// Runs `fut` on a current-thread runtime of the compiled-in flavour, timers on.
fn block_on_with_timer<F: std::future::Future>(fut: F) -> F::Output {
    #[cfg(feature = "runtime-monoio")]
    {
        monoio::RuntimeBuilder::<monoio::LegacyDriver>::new()
            .enable_timer()
            .build()
            .expect("monoio runtime")
            .block_on(fut)
    }
    #[cfg(all(feature = "runtime-tokio", not(feature = "runtime-monoio")))]
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("tokio runtime")
            .block_on(fut)
    }
}

async fn sleep_ms(ms: u64) {
    #[cfg(feature = "runtime-monoio")]
    monoio::time::sleep(Duration::from_millis(ms)).await;
    #[cfg(all(feature = "runtime-tokio", not(feature = "runtime-monoio")))]
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

/// One shard, two dbs, `k` in db 0. Returns the shared handle; the slice is
/// installed on the calling thread.
fn one_shard_with_key_in_db0() -> std::sync::Arc<ShardDatabases> {
    let mut dbs = vec![Database::new(), Database::new()];
    dbs[0].set_string(b"k", Bytes::from_static(b"v"));
    let (shard_databases, mut inits) = ShardDatabases::new(vec![dbs]);
    crate::shard::slice::reset_test_shard(crate::shard::slice::ShardSlice::new(inits.remove(0)));
    shard_databases
}

fn key_in_db(db: usize) -> bool {
    crate::shard::slice::with_shard_db(db, |d| d.get(b"k").is_some())
}

/// Receive the next message, waiting on the runtime timer (bounded).
async fn recv_within(rx: &channel::MpscReceiver<AofMessage>, what: &str) -> AofMessage {
    for _ in 0..2000 {
        if let Ok(msg) = rx.try_recv() {
            return msg;
        }
        sleep_ms(1).await;
    }
    panic!("timed out waiting for {what}");
}

fn payload(msg: &AofMessage) -> &[u8] {
    match msg {
        AofMessage::Append { bytes, .. } | AofMessage::AppendSync { bytes, .. } => bytes,
        _ => b"<control>",
    }
}

/// The writer channel is full when SWAPDB runs, and a fold snapshots while
/// SWAPDB waits for room. The swap is not in that snapshot, so the record
/// must survive the floor the fold commits.
#[test]
fn swapdb_waiting_for_room_across_a_fold_snapshot_keeps_its_record() {
    let shard_databases = one_shard_with_key_in_db0();
    let (tx, rx) = channel::mpsc_bounded::<AofMessage>(1);
    tx.try_send(AofMessage::Append {
        lsn: 0,
        db: 0,
        bytes: Bytes::from_static(b"filler"),
        epoch: FoldEpoch::INITIAL,
    })
    .expect("fill the one-slot writer channel");
    let pool = std::sync::Arc::new(AofWriterPool::top_level_with_policy(
        tx,
        FsyncPolicy::EverySec,
        Duration::from_secs(5),
    ));
    let dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>> = Rc::new(RefCell::new(Vec::new()));
    let notifiers: Vec<std::sync::Arc<channel::Notify>> = Vec::new();

    let (reply, (snapshot_epoch, swapped_at_snapshot)) = block_on_with_timer(async {
        let swap = super::coordinate_swapdb(
            0,
            1,
            0,
            1,
            &shard_databases,
            &dispatch_tx,
            &notifiers,
            Some(&pool),
            &None,
        );
        let fold = async {
            // SWAPDB is now waiting on the full channel.
            sleep_ms(20).await;
            // The fold's snapshot instant (the `AofFold` handler).
            let snapshot_epoch = pool.overflow_for(0).advance_epoch();
            let swapped_at_snapshot = key_in_db(1);
            // The writer drains the filler: there is room again.
            let filler = rx.try_recv().expect("filler still queued");
            assert_eq!(payload(&filler), b"filler");
            (snapshot_epoch, swapped_at_snapshot)
        };
        futures::join!(swap, fold)
    });

    assert_eq!(reply, Frame::SimpleString(Bytes::from_static(b"OK")));
    assert!(
        !swapped_at_snapshot,
        "SWAPDB swapped while its record could not be enqueued"
    );
    assert!(key_in_db(1) && !key_in_db(0), "SWAPDB was applied");
    let record = rx.try_recv().expect("the SWAPDB record reached the writer");
    assert!(payload(&record).windows(6).any(|w| w == b"SWAPDB"));
    assert!(
        !crate::persistence::aof::is_folded(&record, snapshot_epoch),
        "the snapshot does not contain the swap, so the committed fold must keep its \
         record; dropping it loses the acked SWAPDB on restart"
    );
}

/// Under `always`, a fold that snapshots after the SWAPDB record is enqueued
/// but before its fsync is confirmed cuts the stream after that record: it
/// goes to the old incr, which the new base replaces. So the snapshot must
/// already contain the swap.
#[test]
fn swapdb_record_in_the_writer_channel_implies_the_swap_is_applied() {
    let shard_databases = one_shard_with_key_in_db0();
    let (tx, rx) = channel::mpsc_bounded::<AofMessage>(8);
    let pool = std::sync::Arc::new(AofWriterPool::top_level_with_policy(
        tx,
        FsyncPolicy::Always,
        Duration::from_secs(5),
    ));
    let dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>> = Rc::new(RefCell::new(Vec::new()));
    let notifiers: Vec<std::sync::Arc<channel::Notify>> = Vec::new();

    let (reply, swapped_when_record_visible) = block_on_with_timer(async {
        let swap = super::coordinate_swapdb(
            0,
            1,
            0,
            1,
            &shard_databases,
            &dispatch_tx,
            &notifiers,
            Some(&pool),
            &None,
        );
        let writer = async {
            let record = recv_within(&rx, "the SWAPDB record").await;
            assert!(payload(&record).windows(6).any(|w| w == b"SWAPDB"));
            // A fold snapshot taken now puts the record before its cut.
            let swapped = key_in_db(1);
            // Confirm the durability barrier that follows the record.
            match recv_within(&rx, "the fsync barrier").await {
                AofMessage::AppendSync { bytes, ack, .. } => {
                    assert!(bytes.is_empty(), "the barrier logs nothing");
                    let _ = ack.send(AofAck::Synced);
                }
                _ => panic!("expected the fsync barrier"),
            }
            swapped
        };
        futures::join!(swap, writer)
    });

    assert_eq!(reply, Frame::SimpleString(Bytes::from_static(b"OK")));
    assert!(
        swapped_when_record_visible,
        "the SWAPDB record was in the writer channel while the swap was not applied: \
         a fold snapshot at that instant loses the swap"
    );
    assert!(key_in_db(1) && !key_in_db(0));
}
