//! The durability record of a served blocking pop, written by the shard that
//! POPPED, at the moment it popped (moon#1056, moon#1097).
//!
//! A blocking pop is a write, so it owes the AOF and the replication stream
//! a record (moon#827). That record used to be written by the WAITER's
//! connection task, after the reply reached it. Two things were wrong with
//! that:
//!
//! * **The wrong shard's log.** A waiter parks on the shard that owns its
//!   key, which at `--shards > 1` is usually not its own. The waiter logged
//!   the pop to its OWN shard's AOF, and per-shard replay routes each record
//!   by the file it sits in, so the pop was dropped on recovery: the element
//!   the client had already received came back after `kill -9`.
//! * **The wrong place in the order.** The record was written after the
//!   reply was handed over, which is after the owner had gone on to run other
//!   writes. A later write to the same key could be logged BEFORE the pop
//!   that preceded it (`--shards 1` too), and replay applied them in the
//!   wrong order. The same gap let an AOF rewrite snapshot fall between the
//!   pop and its record, so the pop was both in the new base and in the new
//!   incremental file and was applied twice on restart (moon#1097).
//!
//! The fix is to log where every other write on the owner is logged: on the
//! owner's thread, in the same synchronous stretch as the mutation. This
//! module is that stretch's sink. The shard event loop installs it once at
//! startup ([`install`]); the pop sites ([`crate::blocking::wakeup::deliver`]
//! for a waiter served by a wake, and the immediate pop in
//! `server::conn::blocking`) call [`log_pop`] right after the element leaves
//! the keyspace and before the reply is sent. Nothing between the pop and the
//! enqueue can suspend, so the record, its position in the owner's AOF
//! stream and the mutation always fall on the same side of any other write
//! and of any fold snapshot, which also runs on this thread.
//!
//! The legs mirror the connection handler's write tail exactly (see
//! `handler_monoio`'s `is_write` block): the replication record is appended
//! to the backlog and the offset advanced synchronously (monoio only, like
//! every connection-context replication leg: master-side PSYNC does not exist
//! under tokio), and the AOF append uses the bounded synchronous producer the
//! shard's SPSC arms use for every cross-shard write. Under `appendfsync
//! always` the fsync itself is confirmed by the waiter, which awaits
//! `fsync_barrier(owner)` before answering its client; the writer channel is
//! ordered, so that barrier covers this record.

use std::cell::RefCell;
use std::sync::Arc;

use bytes::Bytes;

use crate::persistence::aof::AofWriterPool;
use crate::protocol::Frame;
use crate::replication::state::ReplicationState;

/// What one shard needs to log a pop it served.
struct Sink {
    shard_id: usize,
    aof_pool: Option<Arc<AofWriterPool>>,
    repl_state: Option<Arc<parking_lot::RwLock<ReplicationState>>>,
}

thread_local! {
    /// This shard thread's sink. `None` until the event loop installs it,
    /// and forever on a thread that is not a shard (unit tests, embedders
    /// without a shard loop): a pop served there is logged nowhere, exactly
    /// as on a server with neither AOF nor replication.
    static SINK: RefCell<Option<Sink>> = const { RefCell::new(None) };
}

/// Install this shard thread's sink. Called once by the shard event loop,
/// right after the thread-local slice is initialised and before any command
/// can run. Installing again replaces the previous sink.
pub fn install(
    shard_id: usize,
    aof_pool: Option<Arc<AofWriterPool>>,
    repl_state: Option<Arc<parking_lot::RwLock<ReplicationState>>>,
) {
    SINK.with(|s| {
        *s.borrow_mut() = Some(Sink {
            shard_id,
            aof_pool,
            repl_state,
        });
    });
}

/// Remove this thread's sink. For tests that install one.
#[cfg(test)]
pub(crate) fn uninstall() {
    SINK.with(|s| *s.borrow_mut() = None);
}

/// How [`log_pop`] went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PopLog {
    /// Nothing to log to: no sink, or neither an AOF nor a replication
    /// stream. The pop reached no durability plane, so it may still be put
    /// back if its waiter turns out to be gone.
    Unlogged,
    /// The record reached every plane that is active.
    Logged,
    /// The replication leg (if active) took the record but the AOF append
    /// could not be enqueued within the backpressure bound. The pop stays
    /// applied, exactly like every other synchronous write that meets a
    /// saturated writer, and the waiter must be told so rather than handed
    /// the element as if it were durable.
    AofLost,
}

/// Is any plane listening? One thread-local read plus one `Option` check (and
/// one `Relaxed` load for the replication hint). Pop sites ask this BEFORE
/// building the record, so a server with neither AOF nor replication pays no
/// allocation for it.
#[inline]
pub(crate) fn has_work() -> bool {
    SINK.with(|s| {
        s.borrow().as_ref().is_some_and(|sink| {
            sink.aof_pool.is_some() || crate::replication::state::fanout_hint_active()
        })
    })
}

/// Could this shard's AOF writer take a record right now without blocking?
/// `true` with no AOF at all. The retry of a wake that stopped at a refused
/// record (moon#1111) waits for this, so it never spends a backpressure
/// bound on the shard thread while the writer is still saturated.
pub(crate) fn writer_has_room() -> bool {
    SINK.with(|s| {
        s.borrow().as_ref().is_none_or(|sink| {
            sink.aof_pool
                .as_ref()
                .is_none_or(|pool| !pool.append_would_block(sink.shard_id))
        })
    })
}

/// A fresh backpressure budget for ONE wake pass (or one immediate pop).
///
/// [`log_pop`] may block the shard thread while the AOF writer is saturated,
/// bounded by the budget it is handed. A wake that serves many waiters —
/// one `RPUSH` with N clients parked, an `EXEC` feeding several keys — shares
/// one budget across all of its pops, exactly like the SPSC batch arms share
/// one across a batch, so it stalls the shard for at most one
/// [`AOF_SPSC_BACKPRESSURE_BOUND`](crate::persistence::aof::AOF_SPSC_BACKPRESSURE_BOUND)
/// in total rather than one per served pop. Once the budget is spent, a pop
/// whose record meets a full channel is refused at once (and its waiter told
/// so) instead of waiting again.
#[inline]
pub(crate) fn wake_budget() -> std::time::Duration {
    crate::persistence::aof::AOF_SPSC_BACKPRESSURE_BOUND
}

/// Log `record` — the non-blocking command that reproduces a pop this shard
/// just served in database `db` — to this shard's replication stream and AOF,
/// blocking for at most what is left of `budget` (see [`wake_budget`]).
///
/// MUST be called on the shard thread that performed the pop, in the same
/// synchronous stretch as the pop, before the reply is sent. See the module
/// docs for why each of those three matters.
pub(crate) fn log_pop(db: usize, record: &Frame, budget: &mut std::time::Duration) -> PopLog {
    log_records(db, std::slice::from_ref(record), budget)
}

/// [`log_pop`] for a serve that one record cannot describe: a blocking
/// consumer-group read, which redis propagates as one `XCLAIM` per delivered
/// entry plus an `XGROUP SETID` (moon#1104, `blocking::stream_log`).
///
/// The records are enqueued in order, in this one synchronous stretch, so no
/// other write on this shard can fall between them. Each one reaches the
/// replication stream; the AOF takes them in order until one cannot be
/// enqueued within the budget, and none after it — the AOF then holds a
/// PREFIX of the serve, which replays to a consistent state (the entries it
/// does not cover are simply delivered again), never a later record without
/// the earlier ones.
pub(crate) fn log_records(
    db: usize,
    records: &[Frame],
    budget: &mut std::time::Duration,
) -> PopLog {
    SINK.with(|s| {
        let guard = s.borrow();
        let Some(sink) = guard.as_ref() else {
            return PopLog::Unlogged;
        };
        #[cfg(feature = "runtime-monoio")]
        let repl_active = crate::replication::state::fanout_active_for(&sink.repl_state);
        #[cfg(not(feature = "runtime-monoio"))]
        let repl_active = false;
        if !repl_active && sink.aof_pool.is_none() {
            return PopLog::Unlogged;
        }
        let mut outcome = PopLog::Logged;
        for record in records {
            let bytes = crate::persistence::aof::serialize_command_for_log(record);
            // Same offset contract as the connection handler's write tail:
            // when replication is live the backlog owns the offset and the
            // AOF leg must not advance it a second time (lsn = 0).
            let lsn = if repl_active {
                #[cfg(feature = "runtime-monoio")]
                if let Some(rs) = sink.repl_state.as_ref() {
                    let g = rs.read();
                    crate::replication::state::record_local_write_db_on(
                        &g,
                        sink.shard_id,
                        db,
                        bytes.clone(),
                    );
                }
                0
            } else if outcome == PopLog::AofLost {
                // Nothing more reaches the AOF: issue no lsn for it either.
                continue;
            } else {
                AofWriterPool::issue_append_lsn(&sink.repl_state, sink.shard_id, bytes.len())
            };
            if outcome == PopLog::AofLost {
                continue;
            }
            if let Some(pool) = sink.aof_pool.as_ref()
                && !pool.send_append_bounded_blocking(sink.shard_id, lsn, db, bytes, budget)
            {
                outcome = PopLog::AofLost;
            }
        }
        outcome
    })
}

/// The non-blocking command that reproduces a pop a wake just performed on
/// `key`, built from what the pop actually TOOK rather than from what the
/// waiter asked for: a `BLMPOP ... COUNT 10` that found three elements is
/// logged as popping three.
///
/// `None` when the undo does not describe a pop this waiter's command can
/// make, which no caller produces — a record invented from a shape this does
/// not understand would corrupt a replica far more cheaply than omitting it.
pub(crate) fn served_pop_record(
    cmd: &crate::blocking::BlockedCommand,
    key: &Bytes,
    undo: &crate::blocking::wakeup::WakeUndo,
) -> Option<Frame> {
    use crate::blocking::wakeup::WakeUndo;
    use crate::blocking::{BlockedCommand, Direction};

    fn bulk(s: &'static [u8]) -> Frame {
        Frame::BulkString(Bytes::from_static(s))
    }
    fn side(d: Direction) -> Frame {
        match d {
            Direction::Left => bulk(b"LEFT"),
            Direction::Right => bulk(b"RIGHT"),
        }
    }
    // `POP key` for one element, `POP key n` for several — the two spellings
    // replay identically, and the short one is what a single pop always was.
    fn pop(name: &'static [u8], key: &Bytes, n: usize) -> Option<Frame> {
        match n {
            0 => None,
            1 => Some(Frame::Array(crate::framevec![
                bulk(name),
                Frame::BulkString(key.clone()),
            ])),
            n => {
                let mut digits = itoa::Buffer::new();
                Some(Frame::Array(crate::framevec![
                    bulk(name),
                    Frame::BulkString(key.clone()),
                    Frame::BulkString(Bytes::copy_from_slice(digits.format(n).as_bytes())),
                ]))
            }
        }
    }
    match undo {
        WakeUndo::ListFront(vals) => pop(b"LPOP", key, vals.len()),
        WakeUndo::ListBack(vals) => pop(b"RPOP", key, vals.len()),
        WakeUndo::Moved {
            destination,
            wherefrom,
            whereto,
            ..
        } => Some(Frame::Array(crate::framevec![
            bulk(b"LMOVE"),
            Frame::BulkString(key.clone()),
            Frame::BulkString(destination.clone()),
            side(*wherefrom),
            side(*whereto),
        ])),
        WakeUndo::Zset(pairs) => {
            let min = match cmd {
                BlockedCommand::BZPopMin => true,
                BlockedCommand::BZPopMax => false,
                BlockedCommand::BZMPop { min, .. } => *min,
                _ => return None,
            };
            pop(if min { b"ZPOPMIN" } else { b"ZPOPMAX" }, key, pairs.len())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::aof::AofMessage;

    fn lpop(key: &'static [u8]) -> Frame {
        Frame::Array(crate::framevec![
            Frame::BulkString(bytes::Bytes::from_static(b"LPOP")),
            Frame::BulkString(bytes::Bytes::from_static(key)),
        ])
    }

    #[test]
    fn a_thread_with_no_sink_logs_nothing() {
        uninstall();
        assert!(!has_work());
        assert_eq!(
            log_pop(0, &lpop(b"k"), &mut wake_budget()),
            PopLog::Unlogged
        );
    }

    #[test]
    fn the_record_is_enqueued_on_the_installing_shards_writer_before_log_pop_returns() {
        let (tx, rx) = flume::bounded::<AofMessage>(8);
        install(0, Some(AofWriterPool::top_level(tx)), None);
        assert!(has_work());
        assert_eq!(log_pop(3, &lpop(b"k"), &mut wake_budget()), PopLog::Logged);
        match rx.try_recv() {
            Ok(AofMessage::Append { db, bytes, .. }) => {
                assert_eq!(db, 3, "the record carries the db the pop ran in");
                assert_eq!(&bytes[..], b"*2\r\n$4\r\nLPOP\r\n$1\r\nk\r\n");
            }
            _ => panic!("the record must already be in the writer channel"),
        }
        uninstall();
    }

    #[test]
    fn a_writer_that_cannot_take_the_record_is_reported_not_swallowed() {
        let (tx, rx) = flume::bounded::<AofMessage>(1);
        drop(rx);
        install(0, Some(AofWriterPool::top_level(tx)), None);
        assert_eq!(log_pop(0, &lpop(b"k"), &mut wake_budget()), PopLog::AofLost);
        uninstall();
    }

    // ---- the wake paths, driven through the real wakers ----

    use crate::blocking::wakeup::{try_wake_list_waiter, try_wake_zset_waiter};
    use crate::blocking::{BlockedCommand, BlockingRegistry, ClaimToken, Direction, WaitEntry};
    use crate::runtime::channel::{self, OneshotReceiver};
    use crate::storage::Database;
    use bytes::Bytes;

    fn b(s: &'static str) -> Bytes {
        Bytes::from_static(s.as_bytes())
    }

    fn park(
        reg: &mut BlockingRegistry,
        db_index: usize,
        key: &Bytes,
        cmd: BlockedCommand,
        claim: Option<ClaimToken>,
    ) -> OneshotReceiver<Option<Frame>> {
        let (tx, rx) = channel::oneshot();
        let wait_id = reg.next_wait_id();
        reg.register(
            db_index,
            key.clone(),
            WaitEntry {
                wait_id,
                cmd,
                reply_tx: tx,
                deadline: None,
                claim,
            },
        );
        rx
    }

    /// Every record in the channel, as `(db, RESP bytes)`.
    fn drain(rx: &flume::Receiver<AofMessage>) -> Vec<(usize, Vec<u8>)> {
        let mut out = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            if let AofMessage::Append { db, bytes, .. } = msg {
                out.push((db, bytes.to_vec()));
            }
        }
        out
    }

    fn list(db: &mut Database, key: &Bytes) -> Vec<Bytes> {
        let now_ms = db.now_ms();
        match db.get_list_ref_if_alive(key, now_ms) {
            Ok(Some(l)) => l.iter_bytes(),
            _ => Vec::new(),
        }
    }

    /// moon#1056 / moon#1097: the waker that POPS is the one that logs, and
    /// the record is already in this shard's writer channel when the wake
    /// returns — no suspension point between the pop and its record, so no
    /// other write and no fold snapshot on this thread can fall between them.
    #[test]
    fn a_wake_served_pop_is_enqueued_before_the_wake_returns() {
        let (tx, rx) = flume::bounded::<AofMessage>(16);
        install(0, Some(AofWriterPool::top_level(tx)), None);
        let mut reg = BlockingRegistry::new(0);
        let mut db = Database::new();
        let k = b("k");
        let waiter = park(&mut reg, 5, &k, BlockedCommand::BLPop, None);
        db.list_push_back(&k, b("a"));
        db.list_push_back(&k, b("b"));

        assert!(try_wake_list_waiter(&mut reg, &mut db, 5, &k));

        assert_eq!(
            drain(&rx),
            vec![(5, b"*2\r\n$4\r\nLPOP\r\n$1\r\nk\r\n".to_vec())],
            "exactly one record, in the waiter's db, naming what was popped"
        );
        assert!(matches!(waiter.try_recv(), Ok(Some(Frame::Array(_)))));
        assert_eq!(list(&mut db, &k), vec![b("b")]);
        uninstall();
    }

    /// The record reproduces what the pop TOOK: a `COUNT 10` that found three
    /// logs three, a zset pop keeps its end, a move keeps both directions.
    #[test]
    fn the_record_is_what_the_pop_took() {
        let (tx, rx) = flume::bounded::<AofMessage>(16);
        install(0, Some(AofWriterPool::top_level(tx)), None);
        let mut reg = BlockingRegistry::new(0);
        let mut db = Database::new();

        let m = b("m");
        let _w1 = park(
            &mut reg,
            0,
            &m,
            BlockedCommand::BLMPop {
                dir: Direction::Right,
                count: 10,
            },
            None,
        );
        for v in ["a", "b", "c"] {
            db.list_push_back(&m, b(v));
        }
        assert!(try_wake_list_waiter(&mut reg, &mut db, 0, &m));

        let z = b("z");
        let _w2 = park(&mut reg, 0, &z, BlockedCommand::BZPopMax, None);
        db.zset_restore(&z, b("m1"), 1.0);
        db.zset_restore(&z, b("m2"), 2.0);
        assert!(try_wake_zset_waiter(&mut reg, &mut db, 0, &z));

        let s = b("s");
        let _w3 = park(
            &mut reg,
            0,
            &s,
            BlockedCommand::BLMove {
                destination: b("d"),
                wherefrom: Direction::Left,
                whereto: Direction::Right,
            },
            None,
        );
        db.list_push_back(&s, b("x"));
        assert!(try_wake_list_waiter(&mut reg, &mut db, 0, &s));

        let got: Vec<Vec<u8>> = drain(&rx).into_iter().map(|(_, r)| r).collect();
        assert_eq!(
            got,
            vec![
                b"*3\r\n$4\r\nRPOP\r\n$1\r\nm\r\n$1\r\n3\r\n".to_vec(),
                b"*2\r\n$7\r\nZPOPMAX\r\n$1\r\nz\r\n".to_vec(),
                b"*5\r\n$5\r\nLMOVE\r\n$1\r\ns\r\n$1\r\nd\r\n$4\r\nLEFT\r\n$5\r\nRIGHT\r\n"
                    .to_vec(),
            ]
        );
        uninstall();
    }

    /// A shard that loses the claim puts the element back and logs NOTHING:
    /// the shard that won logs its own pop.
    #[test]
    fn a_lost_claim_logs_nothing() {
        let (tx, rx) = flume::bounded::<AofMessage>(16);
        install(0, Some(AofWriterPool::top_level(tx)), None);
        let mut reg = BlockingRegistry::new(0);
        let mut db = Database::new();
        let k = b("k");
        let claim = ClaimToken::new();
        assert!(claim.clone().try_claim(), "another shard won it");
        // Parked while the claim was still open; the other shard won since.
        let _w = park(&mut reg, 0, &k, BlockedCommand::BLPop, Some(claim));
        db.list_push_back(&k, b("a"));

        assert!(!try_wake_list_waiter(&mut reg, &mut db, 0, &k));
        assert!(
            drain(&rx).is_empty(),
            "a pop that was put back is not logged"
        );
        assert_eq!(list(&mut db, &k), vec![b("a")]);
        uninstall();
    }

    /// A record the writer cannot take: the pop stands (as for every
    /// synchronous write that meets a saturated writer) and the waiter is
    /// told so instead of being handed the element as if it were durable.
    #[test]
    fn a_lost_append_is_answered_loudly() {
        let (tx, rx) = flume::bounded::<AofMessage>(1);
        drop(rx);
        install(0, Some(AofWriterPool::top_level(tx)), None);
        let mut reg = BlockingRegistry::new(0);
        let mut db = Database::new();
        let k = b("k");
        let first = park(&mut reg, 0, &k, BlockedCommand::BLPop, None);
        let second = park(&mut reg, 0, &k, BlockedCommand::BLPop, None);
        db.list_push_back(&k, b("a"));
        db.list_push_back(&k, b("b"));

        assert!(try_wake_list_waiter(&mut reg, &mut db, 0, &k));
        match first.try_recv() {
            Ok(Some(Frame::Error(e))) => assert!(e.starts_with(b"MOONERR AOF backpressure")),
            other => panic!("expected the fail-loud error, got {other:?}"),
        }
        // The serve loop stops at the first lost append rather than waiting
        // out another bound on the shard thread for the next waiter.
        assert!(second.try_recv().is_err(), "the next waiter stays parked");
        assert_eq!(list(&mut db, &k), vec![b("b")]);
        uninstall();
    }

    /// One wake pass shares ONE backpressure budget across every pop it
    /// logs: with the writer channel full and never drained, the first pop
    /// spends the whole bound and every later pop in the pass is refused at
    /// once — the shard thread stalls one bound in total, not one per
    /// served waiter.
    #[test]
    fn a_wake_pass_spends_one_backpressure_budget_across_its_pops() {
        let (tx, rx) = flume::bounded::<AofMessage>(1);
        // Fill the channel and keep the receiver alive without draining it:
        // every append now has to wait for room that never comes.
        tx.try_send(AofMessage::Append {
            lsn: 0,
            db: 0,
            bytes: Bytes::from_static(b"filler"),
            epoch: crate::persistence::aof::FoldEpoch::INITIAL,
        })
        .map_err(|_| ())
        .expect("room for the filler");
        install(0, Some(AofWriterPool::top_level(tx)), None);
        let mut reg = BlockingRegistry::new(0);
        let mut db = Database::new();
        let keys = [b("k1"), b("k2"), b("k3")];
        let waiters: Vec<_> = keys
            .iter()
            .map(|k| {
                db.list_push_back(k, b("v"));
                park(&mut reg, 0, k, BlockedCommand::BLPop, None)
            })
            .collect();

        let mut budget = wake_budget();
        assert!(crate::blocking::wakeup::wake_keys_budgeted(
            &mut reg,
            &mut db,
            0,
            keys.iter().cloned(),
            &mut budget,
        ));

        assert!(
            budget.is_zero(),
            "the pass drew every wait from the one budget it was handed, which is now spent"
        );
        for w in &waiters {
            match w.try_recv() {
                Ok(Some(Frame::Error(e))) => assert!(e.starts_with(b"MOONERR AOF backpressure")),
                other => panic!("every served pop meets the full writer, got {other:?}"),
            }
        }
        drop(rx);
        uninstall();
    }

    /// moon#1111: the waiter a lost append left parked beside data is served
    /// by the shard's blocking tick as soon as the writer has room again —
    /// with no further write to the key — and not before.
    #[test]
    fn a_waiter_left_parked_by_a_lost_append_is_served_once_the_writer_has_room() {
        std::thread::spawn(|| {
            use crate::shard::slice::{
                ShardSlice, init_shard, test_support::make_init, with_shard_db,
            };
            init_shard(ShardSlice::new(make_init(0, 1)));
            let (tx, rx) = flume::bounded::<AofMessage>(1);
            // Full, and never drained until we say so: every append waits out
            // its budget and is refused.
            tx.try_send(AofMessage::Append {
                lsn: 0,
                db: 0,
                bytes: Bytes::from_static(b"filler"),
                epoch: crate::persistence::aof::FoldEpoch::INITIAL,
            })
            .map_err(|_| ())
            .expect("room for the filler");
            install(0, Some(AofWriterPool::top_level(tx)), None);
            let reg = std::cell::RefCell::new(BlockingRegistry::new(0));
            let k = b("k");
            let first = park(&mut reg.borrow_mut(), 0, &k, BlockedCommand::BLPop, None);
            let second = park(&mut reg.borrow_mut(), 0, &k, BlockedCommand::BLPop, None);
            with_shard_db(0, |db| {
                db.list_push_back(&k, b("a"));
                db.list_push_back(&k, b("b"));
                assert!(try_wake_list_waiter(&mut reg.borrow_mut(), db, 0, &k));
            });
            assert!(matches!(first.try_recv(), Ok(Some(Frame::Error(_)))));
            assert!(second.try_recv().is_err(), "left parked by the lost append");

            // Still saturated: the tick leaves it parked and spends nothing.
            let rc = std::rc::Rc::new(reg);
            crate::shard::timers::expire_blocked_clients(&rc);
            assert!(
                second.try_recv().is_err(),
                "no retry while the writer is full"
            );

            // The writer drains: the next tick serves it, logging the pop.
            assert_eq!(drain(&rx).len(), 1, "only the filler was ever enqueued");
            crate::shard::timers::expire_blocked_clients(&rc);
            match second.try_recv() {
                Ok(Some(Frame::Array(items))) => {
                    assert_eq!(items.get(1), Some(&Frame::BulkString(b("b"))));
                }
                other => panic!("the parked waiter was not served: {other:?}"),
            }
            assert_eq!(
                drain(&rx),
                vec![(0, b"*2\r\n$4\r\nLPOP\r\n$1\r\nk\r\n".to_vec())]
            );
            uninstall();
        })
        .join()
        .expect("test thread");
    }
}
