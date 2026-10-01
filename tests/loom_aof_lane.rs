//! Loom model for the AOF lane hand-over (moon#1266 Option 1A).
//!
//! The state machine is the REAL one: `src/persistence/aof/lane_protocol.rs`
//! is compiled into this test crate through `#[path]` (it is pure data; the
//! lane's mutex is loom's here, parking_lot's in production). Around it the
//! thread bodies mirror the production callers:
//!
//! - the PRODUCER (a shard thread, `AofLane::enqueue` + its flush points):
//!   under the lane lock, frame into the buffer when DIRECT, else send to the
//!   writer's channel before releasing the lock; on a full channel, count
//!   itself a slow sender (`enter_slow`), leave the lock, wait for room, then
//!   `leave_slow`; an `AppendSync` (a record that must not be overtaken) flips
//!   the lane, then is sent, under the same lock; `flush` writes the buffer;
//!   a control message (`Rewrite`/`Shutdown`) is sent after a `flip`;
//! - the WRITER (`writer_task/lane_hooks.rs`): on each message, `take_back`
//!   first (writing the buffer), then write the message; at the end of a
//!   wake, `release` when the channel (read under the lane lock) is empty.
//!
//! The kernel is a byte log; a record is one byte, its id. The record
//! context (`RecordCtx` in production) is a counter of records framed so far:
//! each record must be framed with the context the previous one left — the
//! SELECT / `MOON.TS` / `MOON.TXN` state moves with the append position.
//!
//! Verified under every interleaving (bounded preemption):
//!   1. the log holds every record exactly once, in the producer's order;
//!   2. every record is framed with the context its predecessor left (the
//!      context is never forked or rewound between the two owners);
//!   3. a slow sender holds the hand-over: without `enter_slow` loom finds a
//!      record that reaches the channel after the writer handed the position
//!      over, and overtakes it (negative control);
//!   4. deciding "channel" under the lock but sending after releasing it is
//!      caught the same way (negative control) — the send must be under it.
//!
//! Run with (from the repo root, in a target dir of its own):
//!   cargo rustc --release --test loom_aof_lane -- --cfg loom
//! then run the built test binary (see tests/loom_aof_fsync_agent.rs for why
//! not `RUSTFLAGS`). Without `--cfg loom` the same models run repeatedly on
//! std threads as a smoke test.

#![allow(unexpected_cfgs)]

#[path = "../src/persistence/aof/lane_protocol.rs"]
#[allow(dead_code)]
mod lane_protocol;

use std::collections::VecDeque;

use lane_protocol::LaneCore;

#[cfg(loom)]
use loom::sync::{Arc, Mutex};
#[cfg(not(loom))]
use std::sync::{Arc, Mutex};

/// The writer's channel capacity: 1, so the slow path is reachable.
const CAP: usize = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Msg {
    /// A record (`Append`, or an `AppendSync` that flipped first).
    Rec(u8),
    /// A control message (`Rewrite*`, `Shutdown`): carries no record.
    Ctl,
}

/// The producers' handle on the writer's file (a dup in production).
#[derive(Debug)]
struct Sink(Arc<Mutex<Vec<u8>>>);

/// How the producer sends when the lane is not DIRECT (the controls).
#[derive(Clone, Copy, PartialEq, Eq)]
enum How {
    /// Production: decided and sent under the lock; slow senders counted.
    Correct,
    /// Negative control: the slow path does not call `enter_slow`.
    UncountedSlowSender,
    /// Negative control: decided under the lock, sent after releasing it.
    UnlockedSend,
}

struct World {
    core: Mutex<LaneCore<u8, Sink>>,
    chan: Mutex<VecDeque<Msg>>,
    /// The kernel page cache: record ids in write order.
    log: Arc<Mutex<Vec<u8>>>,
}

fn write(sink: &mut Sink, bytes: &[u8]) -> std::io::Result<()> {
    sink.0.lock().unwrap().extend_from_slice(bytes);
    Ok(())
}

/// Frame record `id` with context `ctx` (checks 2.).
fn frame(ctx: &mut u8, id: u8, out: &mut Vec<u8>) {
    assert_eq!(
        *ctx,
        id - 1,
        "lane order violated: (2) record {id} framed with the context of record {ctx}"
    );
    *ctx += 1;
    out.push(id);
}

impl World {
    fn new() -> Self {
        Self {
            core: Mutex::new(LaneCore::new()),
            chan: Mutex::new(VecDeque::new()),
            log: Arc::new(Mutex::new(Vec::new())),
        }
    }

    // ── producer (shard thread) ────────────────────────────────────────

    /// `AofLane::enqueue` of an `Append`.
    fn append(&self, id: u8, how: How) {
        let mut core = self.core.lock().unwrap();
        if core.frame_with(|ctx, buf| frame(ctx, id, buf)) {
            return;
        }
        self.send_writer_mode(core, Msg::Rec(id), how);
    }

    /// `AofLane::enqueue` of an `AppendSync`: flip, then send, one lock.
    fn append_sync(&self, id: u8) {
        let mut core = self.core.lock().unwrap();
        core.flip(write);
        self.send_writer_mode(core, Msg::Rec(id), How::Correct);
    }

    /// The pool's control sends: `lane.flip()`, then the send.
    fn control(&self) {
        self.core.lock().unwrap().flip(write);
        self.chan.lock().unwrap().push_back(Msg::Ctl);
    }

    /// A flush point (reply macro, io_uring submit hook, oneshot send).
    fn flush(&self) {
        self.core.lock().unwrap().flush(write);
    }

    fn send_writer_mode(
        &self,
        mut core: impl std::ops::DerefMut<Target = LaneCore<u8, Sink>>,
        msg: Msg,
        how: How,
    ) {
        if how == How::UnlockedSend {
            drop(core);
            self.blocking_send(msg);
            return;
        }
        {
            let mut chan = self.chan.lock().unwrap();
            if chan.len() < CAP {
                chan.push_back(msg);
                return;
            }
        }
        // Full: the slow send (spill / park / send_timeout) leaves the lock.
        let counted = how != How::UncountedSlowSender;
        if counted {
            core.enter_slow();
        }
        drop(core);
        self.blocking_send(msg);
        if counted {
            self.core.lock().unwrap().leave_slow();
        }
    }

    fn blocking_send(&self, msg: Msg) {
        loop {
            {
                let mut chan = self.chan.lock().unwrap();
                if chan.len() < CAP {
                    chan.push_back(msg);
                    return;
                }
            }
            yield_now();
        }
    }

    // ── writer thread ─────────────────────────────────────────────────

    /// The writer writes one channel record with its own context.
    fn writer_write(&self, ctx: &mut Option<u8>, msg: Msg) {
        if let Msg::Rec(id) = msg {
            let ctx = ctx
                .as_mut()
                .expect("lane order violated: the writer wrote without its context");
            let mut out = Vec::new();
            frame(ctx, id, &mut out);
            self.log.lock().unwrap().extend_from_slice(&out);
        }
    }

    /// `lane_hooks::reclaim`: the position is the writer's again.
    fn reclaim(&self, ctx: &mut Option<u8>) {
        let back = self.core.lock().unwrap().take_back(write);
        assert!(!back.write_failed);
        if let Some(c) = back.ctx {
            assert!(ctx.is_none(), "lane order violated: two contexts");
            *ctx = Some(c);
        }
    }

    /// `lane_hooks::offer_std`: hand the position over if nothing is in flight.
    fn offer(&self, ctx: &mut Option<u8>) {
        let mut core = self.core.lock().unwrap();
        if !core.may_release() {
            return;
        }
        let Some(c) = ctx.take() else {
            return;
        };
        let empty = self.chan.lock().unwrap().is_empty();
        if let Err((c, _)) = core.release(c, Sink(Arc::clone(&self.log)), empty) {
            *ctx = Some(c);
        }
    }

    /// One writer wake: a message (reclaim first) or none, then the offer.
    fn writer_wake(&self, ctx: &mut Option<u8>) {
        let msg = self.chan.lock().unwrap().pop_front();
        if let Some(msg) = msg {
            self.reclaim(ctx);
            self.writer_write(ctx, msg);
        }
        self.offer(ctx);
    }

    /// After every thread joined: the writer's stop path (reclaim, then
    /// drain), and the checks 1.
    fn finish(&self, mut ctx: Option<u8>, n: u8) {
        self.reclaim(&mut ctx);
        loop {
            let msg = self.chan.lock().unwrap().pop_front();
            let Some(msg) = msg else { break };
            self.writer_write(&mut ctx, msg);
        }
        let log = self.log.lock().unwrap().clone();
        let want: Vec<u8> = (1..=n).collect();
        assert_eq!(
            log, want,
            "lane order violated: (1) the log is not every record once, in order"
        );
        assert_eq!(
            ctx,
            Some(n),
            "the context came back advanced past every record"
        );
    }
}

#[cfg(loom)]
fn yield_now() {
    loom::thread::yield_now();
}

#[cfg(not(loom))]
fn yield_now() {
    std::thread::yield_now();
}

#[cfg(loom)]
fn thread_spawn<T: Send + 'static>(
    f: impl FnOnce() -> T + Send + 'static,
) -> loom::thread::JoinHandle<T> {
    loom::thread::spawn(f)
}

#[cfg(not(loom))]
fn thread_spawn<T: Send + 'static>(
    f: impl FnOnce() -> T + Send + 'static,
) -> std::thread::JoinHandle<T> {
    std::thread::spawn(f)
}

/// The producer appends 1, flushes, sends an `AppendSync` 2, appends 3,
/// sends a control message, appends 4 and flushes, while the writer wakes
/// `wakes` times (each wake: a message if any, then an offer).
fn model_lane(how: How, wakes: usize) {
    let w = Arc::new(World::new());
    let writer = {
        let w = Arc::clone(&w);
        thread_spawn(move || {
            let mut ctx = Some(0u8);
            for _ in 0..wakes {
                w.writer_wake(&mut ctx);
            }
            ctx
        })
    };
    let producer = {
        let w = Arc::clone(&w);
        thread_spawn(move || {
            w.append(1, how);
            w.flush();
            w.append_sync(2);
            w.append(3, how);
            w.control();
            w.append(4, how);
            w.flush();
        })
    };
    producer.join().unwrap();
    let ctx = writer.join().unwrap();
    w.finish(ctx, 4);
}

/// A tighter shape for the slow path: two appends against a channel of 1.
fn model_slow_path(how: How) {
    let w = Arc::new(World::new());
    let writer = {
        let w = Arc::clone(&w);
        thread_spawn(move || {
            let mut ctx = Some(0u8);
            for _ in 0..3 {
                w.writer_wake(&mut ctx);
            }
            ctx
        })
    };
    let producer = {
        let w = Arc::clone(&w);
        thread_spawn(move || {
            w.append(1, how);
            w.append(2, how);
            w.append(3, how);
            w.flush();
        })
    };
    producer.join().unwrap();
    let ctx = writer.join().unwrap();
    w.finish(ctx, 3);
}

#[cfg(loom)]
mod loom_models {
    use super::*;

    fn model(f: impl Fn() + Sync + Send + 'static) {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(3);
        builder.check(f);
    }

    #[test]
    fn loom_lane_hand_over_keeps_order_and_context() {
        model(|| model_lane(How::Correct, 3));
    }

    #[test]
    fn loom_lane_slow_sender_holds_the_hand_over() {
        model(|| model_slow_path(How::Correct));
    }

    #[test]
    #[should_panic(expected = "lane order violated")]
    fn loom_uncounted_slow_sender_is_caught() {
        model(|| model_slow_path(How::UncountedSlowSender));
    }

    #[test]
    #[should_panic(expected = "lane order violated")]
    fn loom_send_outside_the_lock_is_caught() {
        model(|| model_slow_path(How::UnlockedSend));
    }
}

#[cfg(not(loom))]
mod smoke {
    use super::*;

    #[test]
    fn smoke_lane_hand_over_keeps_order_and_context() {
        for wakes in 0..6 {
            for _ in 0..200 {
                model_lane(How::Correct, wakes);
            }
        }
    }

    #[test]
    fn smoke_lane_slow_sender_holds_the_hand_over() {
        for _ in 0..500 {
            model_slow_path(How::Correct);
        }
    }
}
