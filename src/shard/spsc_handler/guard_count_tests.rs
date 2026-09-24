//! moon#1198: the SPSC batch arms take ONE exclusive guard per command.
//!
//! `MultiExecute` and `PipelineBatchSlotted` used to take an extra exclusive
//! acquisition per message only to refresh the db clock, before taking it
//! again per command. Every exclusive hold of a db is a window in which a
//! foreign shard's `try_foreign_db_read` of it declines into a parked SPSC hop
//! (cost model §8.3), so the extra one was pure cost. Counted here with the
//! test-only `db_plane::exclusive_count`, not with a clock.

use super::*;
use crate::protocol::Frame;
use crate::server::response_slot::ResponseSlot;
use crate::shard::db_plane::exclusive_count;
use crate::shard::dispatch::ResponseSlotPtr;
use ringbuf::HeapRb;
use ringbuf::traits::{Producer, Split};

fn argv(parts: &[&'static [u8]]) -> Frame {
    Frame::Array(crate::protocol::FrameVec::from(
        parts
            .iter()
            .map(|p| Frame::BulkString(bytes::Bytes::from_static(p)))
            .collect::<Vec<_>>(),
    ))
}

/// Push `msg` onto a fresh ring and run ONE drain cycle over it; returns the
/// exclusive acquisitions the cycle took.
fn drain_one(shard_databases: &Arc<ShardDatabases>, msg: ShardMessage) -> u64 {
    let (mut prod, cons) = HeapRb::<ShardMessage>::new(4).split();
    assert!(prod.try_push(msg).is_ok());
    let mut consumers = vec![cons];
    let pubsub = parking_lot::RwLock::new(PubSubRegistry::new());
    let blocking = Rc::new(RefCell::new(BlockingRegistry::new(0)));
    let mut pending_snapshot = None;
    let mut snapshot_state: Option<SnapshotState> = None;
    let mut wal_writer: Option<WalWriterV3> = None;
    let backlog: crate::replication::backlog::SharedBacklog =
        Arc::new(parking_lot::Mutex::new(None));
    let mut replica_txs = Vec::new();
    let offsets: Option<crate::replication::state::OffsetHandle> = None;
    let script_cache = Rc::new(RefCell::new(crate::scripting::ScriptCache::new()));
    let clock = CachedClock::new();
    let mut migrations = Vec::new();
    let mut cdc = Vec::new();
    let mut manifest = None;
    let mut autovacuum = crate::shard::autovacuum::AutovacuumDaemon::new(Default::default());
    let rtcfg = Arc::new(parking_lot::RwLock::new(RuntimeConfig::default()));
    let spill_fid = Rc::new(Cell::new(1u64));
    let mut sampler = crate::admin::metrics_setup::CommandSampler::new();
    let mut metrics = crate::admin::metrics_setup::CachedMetricsHandles::new();
    let before = exclusive_count::get();
    drain_spsc_shared(
        shard_databases,
        &mut consumers,
        &pubsub,
        &blocking,
        &mut pending_snapshot,
        &mut snapshot_state,
        &mut wal_writer,
        &backlog,
        &mut replica_txs,
        &offsets,
        0,
        &script_cache,
        None,
        &clock,
        &mut migrations,
        &mut cdc,
        &mut manifest,
        1000,
        8,
        0.2,
        &mut autovacuum,
        None,
        false,
        &rtcfg,
        None,
        &spill_fid,
        None,
        &mut sampler,
        &mut metrics,
    );
    exclusive_count::get() - before
}

#[test]
fn batch_arms_take_one_exclusive_guard_per_command() {
    // A fresh OS thread: `init_shard` is once per thread, and the counter is
    // per thread.
    std::thread::spawn(|| {
        let (shard_databases, mut inits) = ShardDatabases::new(vec![vec![Database::new()]]);
        crate::shard::slice::init_shard(crate::shard::slice::ShardSlice::new(inits.remove(0)));

        let (reply_tx, reply_rx) = channel::oneshot();
        let multi = drain_one(
            &shard_databases,
            ShardMessage::MultiExecute {
                db_index: 0,
                commands: vec![(
                    bytes::Bytes::from_static(b"k"),
                    argv(&[b"MSET", b"k", b"v", b"j", b"w"]),
                )],
                reply_tx,
            },
        );
        let replies = reply_rx.try_recv().expect("MultiExecute answered");
        assert!(matches!(&replies[..], [Frame::SimpleString(s)] if s.as_ref() == b"OK"));

        let slot = Arc::new(ResponseSlot::new());
        let batch = drain_one(
            &shard_databases,
            ShardMessage::PipelineBatchSlotted {
                db_index: 0,
                commands: vec![
                    argv(&[b"SET", b"a", b"1"]),
                    argv(&[b"GET", b"a"]),
                    argv(&[b"INCR", b"n"]),
                ],
                response_slot: ResponseSlotPtr(Arc::clone(&slot)),
            },
        );
        assert_eq!(slot.try_take().expect("batch answered").len(), 3);

        assert_eq!(
            (multi, batch),
            (1, 3),
            "exclusive acquisitions per message must equal its command count \
             (MultiExecute of 1, PipelineBatchSlotted of 3) — an extra one only to \
             refresh the clock blocks foreign readers for nothing"
        );
    })
    .join()
    .expect("shard thread");
}
