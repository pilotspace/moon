//! moon#769: a routed write leg is admitted against its shard's AOF writer
//! BEFORE it applies anything, and the shard thread never waits for that
//! writer. A leg the writer cannot take stays at the head of its producer's
//! SPSC ring, unapplied, and is retried on every drain; everything behind it
//! from the same producer waits behind it, and every other producer and every
//! local connection keeps flowing. When the writer does not make room within
//! the admission wait, the leg is refused unapplied, never
//! applied-then-reported-lost.
//!
//! No assertion here reads a wall clock: each one is about what the drain did
//! (applied, answered, left queued), so CPU load cannot flip them.

use super::*;
use crate::persistence::aof::{AofMessage, AofWriterPool, FoldEpoch, FsyncPolicy};
use crate::protocol::Frame;
use crate::runtime::channel::mpsc_bounded;
use crate::server::response_slot::ResponseSlot;
use crate::shard::aof_admission::AOF_BACKPRESSURE_REFUSED_ERR;
use crate::shard::dispatch::ResponseSlotPtr;
use ringbuf::traits::{Observer, Producer, Split};
use ringbuf::{HeapProd, HeapRb};
use std::time::Duration;

const HEADROOM: usize = crate::shard::aof_admission::ROUTED_ADMISSION_HEADROOM;

fn argv(parts: &[&'static [u8]]) -> Frame {
    Frame::Array(crate::protocol::FrameVec::from(
        parts
            .iter()
            .map(|p| Frame::BulkString(bytes::Bytes::from_static(p)))
            .collect::<Vec<_>>(),
    ))
}

fn filler() -> AofMessage {
    AofMessage::Append {
        lsn: 0,
        db: 0,
        bytes: bytes::Bytes::from_static(b"*1\r\n$4\r\nPING\r\n"),
        epoch: FoldEpoch::INITIAL,
    }
}

fn is_refusal(f: &Frame) -> bool {
    matches!(f, Frame::Error(e) if e.as_ref() == AOF_BACKPRESSURE_REFUSED_ERR)
}

/// A writer channel of `cap` slots with `queued` records already in it.
fn writer(
    cap: usize,
    queued: usize,
    wait: Duration,
) -> (
    Arc<AofWriterPool>,
    crate::runtime::channel::MpscReceiver<AofMessage>,
) {
    let (tx, rx) = mpsc_bounded::<AofMessage>(cap);
    for _ in 0..queued {
        tx.try_send(filler()).expect("room for the filler");
    }
    (
        AofWriterPool::top_level_with_policy(tx, FsyncPolicy::EverySec, wait),
        rx,
    )
}

/// One shard on the calling thread, fed by `producers` SPSC rings (one per
/// peer shard). Each call to [`Shard::drain`] runs ONE drain cycle.
struct Shard {
    shard_databases: Arc<ShardDatabases>,
    consumers: Vec<ringbuf::HeapCons<ShardMessage>>,
    producers: Vec<HeapProd<ShardMessage>>,
}

impl Shard {
    fn new(producers: usize) -> Self {
        let (shard_databases, mut inits) = ShardDatabases::new(vec![vec![Database::new()]]);
        crate::shard::slice::init_shard(crate::shard::slice::ShardSlice::new(inits.remove(0)));
        let (producers, consumers) = (0..producers)
            .map(|_| HeapRb::<ShardMessage>::new(16).split())
            .unzip();
        Self {
            shard_databases,
            consumers,
            producers,
        }
    }

    fn send(&mut self, producer: usize, msg: ShardMessage) {
        assert!(
            self.producers[producer].try_push(msg).is_ok(),
            "ring accepts the test message"
        );
    }

    fn queued(&self, producer: usize) -> usize {
        self.consumers[producer].occupied_len()
    }

    fn drain(&mut self, pool: &Arc<AofWriterPool>) {
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
        drain_spsc_shared(
            &self.shard_databases,
            &mut self.consumers,
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
            Some(pool),
            false, // wal_kv_log
            &rtcfg,
            None,
            &spill_fid,
            None,
            &mut sampler,
            &mut metrics,
        );
    }
}

fn slot() -> Arc<ResponseSlot> {
    Arc::new(ResponseSlot::new())
}

fn one(command: Frame, slot: &Arc<ResponseSlot>) -> ShardMessage {
    ShardMessage::ExecuteSlotted {
        db_index: 0,
        command: Arc::new(command),
        response_slot: ResponseSlotPtr(Arc::clone(slot)),
    }
}

fn batch(commands: Vec<Frame>, slot: &Arc<ResponseSlot>) -> ShardMessage {
    ShardMessage::PipelineBatchSlotted {
        db_index: 0,
        commands: commands.into_iter().map(Arc::new).collect(),
        response_slot: ResponseSlotPtr(Arc::clone(slot)),
    }
}

fn key_exists(key: &[u8]) -> bool {
    crate::shard::slice::with_shard_db(0, |db| db.exists(key))
}

fn is_ok(replies: &[Frame]) -> bool {
    matches!(replies, [Frame::SimpleString(s)] if s.as_ref() == b"OK")
}

/// Run `f` on a fresh thread (`init_shard` is once per thread).
fn on_shard_thread(f: impl FnOnce() + Send + 'static) {
    std::thread::spawn(f).join().expect("shard thread");
}

/// The writer is a full channel behind. The drain must NOT wait for it: it
/// returns with the leg still queued, unapplied and unanswered. Once the
/// writer drains, the next cycle applies the leg normally.
#[test]
fn a_leg_the_writer_cannot_take_stays_queued_and_the_drain_returns() {
    on_shard_thread(|| {
        let (pool, rx) = writer(2, 2, Duration::from_secs(5));
        let mut shard = Shard::new(1);
        let s = slot();
        shard.send(
            0,
            batch(
                vec![argv(&[b"SET", b"k", b"v"]), argv(&[b"INCR", b"n"])],
                &s,
            ),
        );

        shard.drain(&pool);
        assert!(
            s.try_take().is_none(),
            "the drain answered a leg its writer could not take instead of leaving it queued"
        );
        assert_eq!(shard.queued(0), 1, "the leg stays at the head of its ring");
        assert!(!key_exists(b"k") && !key_exists(b"n"), "nothing ran");
        assert_eq!(rx.len(), 2, "no record was queued");

        // The writer catches up.
        while rx.try_recv().is_ok() {}
        shard.drain(&pool);
        let replies = s.try_take().expect("admitted once the writer has room");
        assert!(matches!(&replies[0], Frame::SimpleString(x) if x.as_ref() == b"OK"));
        assert!(key_exists(b"k") && key_exists(b"n"));
        assert_eq!(rx.len(), 2, "both records reached the writer");
    });
}

/// A later leg from the same producer never overtakes a parked one: the
/// read behind the parked SET is not answered until the SET ran, and then it
/// sees the SET's value.
#[test]
fn a_later_leg_from_the_same_producer_never_overtakes_a_parked_one() {
    on_shard_thread(|| {
        let (pool, rx) = writer(1, 1, Duration::from_millis(200));
        let mut shard = Shard::new(1);
        let (set, get) = (slot(), slot());
        shard.send(0, one(argv(&[b"SET", b"k", b"v1"]), &set));
        shard.send(0, one(argv(&[b"GET", b"k"]), &get));

        shard.drain(&pool);
        assert!(
            get.try_take().is_none(),
            "a read overtook the parked write queued before it"
        );
        assert!(set.try_take().is_none());

        while rx.try_recv().is_ok() {}
        shard.drain(&pool);
        assert!(is_ok(&set.try_take().expect("the SET ran")));
        let got = get.try_take().expect("then the GET ran");
        assert!(
            matches!(&got[..], [Frame::BulkString(v)] if v.as_ref() == b"v1"),
            "the GET must observe the SET queued before it: {got:?}"
        );
    });
}

/// Another producer's read (and its writes, once there is room) keep
/// flowing while one producer's head is parked.
#[test]
fn other_producers_keep_flowing_past_a_parked_leg() {
    on_shard_thread(|| {
        let (pool, _rx) = writer(1, 1, Duration::from_millis(200));
        let mut shard = Shard::new(2);
        let (parked, read) = (slot(), slot());
        shard.send(0, one(argv(&[b"SET", b"a", b"1"]), &parked));
        shard.send(1, one(argv(&[b"GET", b"b"]), &read));

        shard.drain(&pool);
        assert!(
            read.try_take().is_some(),
            "a read from another producer waited for the stalled writer"
        );
        assert!(
            parked.try_take().is_none(),
            "the write the writer could not take was answered instead of parked"
        );
        assert_eq!(shard.queued(0), 1);
    });
}

/// `--aof-fsync-timeout-ms 0` documents an unbounded wait for the async
/// write legs. For a routed leg it must still be a finite wait, and the
/// drain itself must never wait at all.
#[test]
fn a_zero_fsync_timeout_never_blocks_the_drain() {
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let (pool, _rx) = writer(1, 1, Duration::ZERO);
        let mut shard = Shard::new(1);
        let s = slot();
        shard.send(0, one(argv(&[b"SET", b"k", b"v"]), &s));
        shard.drain(&pool);
        let _ = done_tx.send((
            s.try_take().is_none(),
            shard.queued(0),
            pool.routed_admission_wait(),
        ));
    });
    let (pending, queued, wait) = done_rx
        .recv_timeout(Duration::from_secs(20))
        .expect("the drain never returned: it is waiting on the AOF writer on the shard thread");
    assert!(pending && queued == 1, "the leg is parked, not answered");
    assert_eq!(wait, crate::shard::aof_admission::ROUTED_ADMISSION_WAIT_CAP);
}

/// The wait expires: the parked leg is refused unapplied, and while the
/// writer stays stalled every later leg that finds no room is refused at
/// once instead of waiting a whole bound each (the circuit is open). The
/// first leg that finds room closes it again.
#[test]
fn an_expired_wait_refuses_unapplied_and_fails_the_next_legs_fast() {
    on_shard_thread(|| {
        let (pool, rx) = writer(1, 1, Duration::from_millis(20));
        let mut shard = Shard::new(1);
        let (a, read, b) = (slot(), slot(), slot());
        shard.send(0, one(argv(&[b"SET", b"a", b"1"]), &a));
        // A read between the two writes is admitted (it logs nothing), but
        // it proves no room: it must not close the circuit.
        shard.send(0, one(argv(&[b"GET", b"a"]), &read));
        shard.send(0, one(argv(&[b"SET", b"b", b"1"]), &b));

        let mut cycles = 0;
        while a
            .try_take()
            .map(|r| assert!(r.iter().all(is_refusal), "{r:?}"))
            .is_none()
        {
            cycles += 1;
            assert!(cycles < 10_000, "the parked leg was never refused");
            std::thread::sleep(Duration::from_millis(1));
            shard.drain(&pool);
        }
        assert!(read.try_take().is_some(), "the read behind it ran");
        let rb = b
            .try_take()
            .expect("the next write failed fast in the same cycle, although a read ran between");
        assert!(rb.iter().all(is_refusal), "{rb:?}");
        assert!(
            !key_exists(b"a") && !key_exists(b"b"),
            "refused legs never ran"
        );
        assert_eq!(rx.len(), 1, "no record was queued");

        while rx.try_recv().is_ok() {}
        let c = slot();
        shard.send(0, one(argv(&[b"SET", b"c", b"1"]), &c));
        shard.drain(&pool);
        assert!(is_ok(&c.try_take().expect("room again: admitted")));
    });
}

/// Admission reserves the room of every leg it admits in a cycle: the legs
/// only run after the whole cycle is collected, so each one must fit on top
/// of the ones admitted before it.
#[test]
fn legs_admitted_in_one_cycle_reserve_their_room() {
    on_shard_thread(|| {
        // Room for one two-record leg plus the headroom, not for two.
        let (pool, rx) = writer(HEADROOM + 3, 0, Duration::from_secs(5));
        let mut shard = Shard::new(1);
        let (first, second) = (slot(), slot());
        let two_sets = |a: &'static [u8], b: &'static [u8]| {
            vec![argv(&[b"SET", a, b"v"]), argv(&[b"SET", b, b"v"])]
        };
        shard.send(0, batch(two_sets(b"a1", b"a2"), &first));
        shard.send(0, batch(two_sets(b"b1", b"b2"), &second));

        shard.drain(&pool);
        assert!(first.try_take().is_some(), "the first leg fits");
        assert!(
            second.try_take().is_none(),
            "the second leg was admitted although the first leg's records already use its room"
        );
        assert_eq!(rx.len(), 2);
    });
}

/// A writer that is gone can never log the leg: refused at once, unapplied.
#[test]
fn a_leg_for_a_writer_that_is_gone_is_refused_unapplied() {
    on_shard_thread(|| {
        let (pool, rx) = writer(4, 0, Duration::from_secs(5));
        drop(rx);
        let mut shard = Shard::new(1);
        let s = slot();
        shard.send(0, one(argv(&[b"SET", b"k", b"v"]), &s));
        shard.drain(&pool);
        let r = s.try_take().expect("answered at once");
        assert!(r.iter().all(is_refusal), "{r:?}");
        assert!(!key_exists(b"k"));
    });
}

/// A rewrite fold with the overflow armed takes records without channel
/// room (they spill), so the leg is admitted.
#[test]
fn a_fold_with_the_overflow_armed_admits_at_once() {
    on_shard_thread(|| {
        let (pool, _rx) = writer(1, 1, Duration::from_secs(5));
        pool.overflow_for(0).arm();
        let mut shard = Shard::new(1);
        let s = slot();
        shard.send(0, one(argv(&[b"SET", b"k", b"v"]), &s));
        shard.drain(&pool);
        assert!(is_ok(&s.try_take().expect("admitted")));
        assert!(key_exists(b"k"));
    });
}
