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
//!      caught the same way (negative control) — the send must be under it;
//!   5. write before reply (W2B-1, `model_replies`): a lane starts HELD (a
//!      writer attached, no hand-over yet — the same state as right after
//!      the policy left `always`) and the writer holds it again on one wake
//!      (`always`); the producer reads the lane's hold (the
//!      `AofWriterPool::fsync_policy_for` view) after each append and, held,
//!      sends an `AppendSync` barrier and waits for its ack; then it flushes
//!      and "replies" — and the record must already be in the log. Loom
//!      finds the lost-ack interleaving when the producer ignores the hold,
//!      and when an `AppendSync` that flips a DIRECT lane does not hold it
//!      again (two negative controls).
//!
//!   6. two producers (R2b round 2 F4, `model_two_producers`): one may act on
//!      a STALE hold (it read "held", the writer then handed the lane over,
//!      and its `AppendSync` barrier flips the DIRECT lane and holds it
//!      again) while the other appends; every reply still follows its
//!      record's write. Negative control: a producer that reads the hold
//!      BEFORE its append (the inline-SET gate shape before F2) is caught —
//!      the other's stale barrier holds the lane between its read and its
//!      append, and its record waits in the channel while it replies.
//!
//! Outside the models: a reply sent while the lane is WRITER and UNHELD — a
//! rewrite fold and its post-fold drain, or a latched write error — may
//! precede its record's `write(2)`; that is the documented fold residual
//! (and moon#1314), so `model_replies` sends no control message. The
//! producers' flush points themselves (the drivers' hooks, the reply
//! macros) are modelled as one `flush` call before the reply.
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
use loom::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
#[cfg(loom)]
use loom::sync::{Arc, Mutex};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
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
    /// A zero-length `AppendSync` (`fsync_barrier`): acked by the writer
    /// once everything before it is written.
    Barrier(u8),
}

/// How the producer replies (`model_replies` and its controls).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Reply {
    /// Production: a barrier while the pool is held; an `AppendSync` that
    /// flips a DIRECT lane holds it again (`AofLane::enqueue`).
    Correct,
    /// Negative control: the producer never takes the acked path.
    IgnoreHold,
    /// Negative control: the `AppendSync` flip does not hold the lane.
    NoRehold,
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
    /// The producer has issued everything (the writer may stop waking).
    done: AtomicBool,
    /// The lane's hold, as the producers read it (`AofLane::held`, one lane
    /// here): changed under the lane lock, read without it.
    holds: AtomicUsize,
    /// The highest barrier the writer acked.
    acked: AtomicU8,
    /// `model_two_producers`: the last record id handed out (under the lane
    /// lock, so ids follow lock order).
    next: AtomicU8,
    /// `model_two_producers`: the record whose reply waits for its barrier's
    /// ack — the reply leaves when the writer acks, so the writer checks it.
    reply_at_ack: AtomicU8,
    /// The channel's capacity ([`CAP`]; the two-producer model's finite
    /// writer needs room for every message, or a slow sender would spin).
    cap: usize,
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
            done: AtomicBool::new(false),
            holds: AtomicUsize::new(0),
            acked: AtomicU8::new(0),
            next: AtomicU8::new(0),
            reply_at_ack: AtomicU8::new(0),
            cap: CAP,
        }
    }

    /// `AofLane::enqueue` of an `Append` whose id is allocated under the lane
    /// lock (two producers).
    fn append_next(&self) -> u8 {
        let mut core = self.core.lock().unwrap();
        let id = self.next.fetch_add(1, Ordering::Relaxed) + 1;
        if core.frame_with(|ctx, buf| frame(ctx, id, buf)) {
            return id;
        }
        self.send_writer_mode(core, Msg::Rec(id), How::Correct);
        id
    }

    /// The other producer of the two-producer model: its record, then — on a
    /// possibly STALE hold — its `AppendSync` barrier (flip, hold again,
    /// send), without waiting for the ack: its own reply is
    /// `model_replies`' business; here it only perturbs the lane.
    fn stale_barrier_producer(&self) {
        let id = self.append_next();
        if self.held() {
            let mut core = self.core.lock().unwrap();
            core.flip(write);
            if core.hold() {
                self.holds.fetch_add(1, Ordering::AcqRel);
            }
            self.send_writer_mode(core, Msg::Barrier(id), How::Correct);
        }
    }

    /// One acknowledged write of a two-producer model: append, the acked
    /// path if the lane is held (read AFTER the append; before it with
    /// `read_first` — the negative control), the flush point, the reply.
    /// No spinning: on the acked path the reply leaves when the writer acks
    /// the barrier, so the writer checks it there (`reply_at_ack`).
    fn write_and_reply_next(&self, read_first: bool) {
        let early = read_first && self.held();
        let id = self.append_next();
        let held = if read_first { early } else { self.held() };
        if held {
            self.reply_at_ack.store(id, Ordering::Release);
            let mut core = self.core.lock().unwrap();
            core.flip(write);
            if core.hold() {
                self.holds.fetch_add(1, Ordering::AcqRel);
            }
            self.send_writer_mode(core, Msg::Barrier(id), How::Correct);
            return;
        }
        self.flush();
        assert!(
            self.log.lock().unwrap().contains(&id),
            "write before reply violated: (6) record {id} acknowledged before its write"
        );
    }

    /// A lane its writer was just attached to (`AofWriterPool::lane`): held.
    fn attached() -> Self {
        let w = Self::new();
        assert!(w.core.lock().unwrap().hold());
        w.holds.store(1, Ordering::Release);
        w
    }

    /// `fsync_policy_for`'s view: must the producers take the acked path?
    fn held(&self) -> bool {
        self.holds.load(Ordering::Acquire) != 0
    }

    /// `fsync_barrier` while held: an `AppendSync` (flip, hold again, send
    /// under one lock), then wait for its ack.
    fn barrier(&self, id: u8, how: Reply) {
        {
            let mut core = self.core.lock().unwrap();
            core.flip(write);
            if how != Reply::NoRehold && core.hold() {
                self.holds.fetch_add(1, Ordering::AcqRel);
            }
            self.send_writer_mode(core, Msg::Barrier(id), How::Correct);
        }
        while self.acked.load(Ordering::Acquire) < id {
            yield_now();
        }
    }

    /// One write acknowledged: append, the acked path if the pool is held,
    /// the flush point, then the reply — after which the record must be in
    /// the kernel (checks 5.).
    fn write_and_reply(&self, id: u8, how: Reply) {
        self.append(id, How::Correct);
        if how != Reply::IgnoreHold && self.held() {
            self.barrier(id, how);
        }
        self.flush();
        assert!(
            self.log.lock().unwrap().contains(&id),
            "write before reply violated: (5) record {id} acknowledged before its write"
        );
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
            if chan.len() < self.cap {
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
                if chan.len() < self.cap {
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
        if let Msg::Barrier(id) = msg {
            // Monotonic: with two producers a later barrier can be acked
            // first; everything before it in the channel is written.
            self.acked.fetch_max(id, Ordering::AcqRel);
            // A reply waiting for this ack leaves now (two-producer model).
            let waiting = self.reply_at_ack.load(Ordering::Acquire);
            if waiting != 0 && waiting <= id {
                assert!(
                    self.log.lock().unwrap().contains(&waiting),
                    "write before reply violated: (6) record {waiting} acknowledged before \
                     its write"
                );
                self.reply_at_ack.store(0, Ordering::Release);
            }
            return;
        }
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
        let was = core.is_held();
        match core.release(c, Sink(Arc::clone(&self.log)), empty) {
            Ok(()) => {
                if was {
                    self.holds.fetch_sub(1, Ordering::AcqRel);
                }
            }
            Err((c, _)) => *ctx = Some(c),
        }
    }

    /// `lane_hooks::on_policy` under `always` (`AofLane::take_back_held`):
    /// take the position back AND hold, under one lock. (Two locks let a
    /// producer find the lane WRITER and unheld in between: loom found that
    /// interleaving, record 3, before production took both under one lock.)
    fn writer_always(&self, ctx: &mut Option<u8>) {
        let back = {
            let mut core = self.core.lock().unwrap();
            let back = core.take_back(write);
            if core.hold() {
                self.holds.fetch_add(1, Ordering::AcqRel);
            }
            back
        };
        assert!(!back.write_failed);
        if let Some(c) = back.ctx {
            assert!(ctx.is_none(), "lane order violated: two contexts");
            *ctx = Some(c);
        }
    }

    /// One writer wake: the offer before the receive (`top_of_wake`), a
    /// message (reclaim first) or none, then the offer again.
    fn writer_wake(&self, ctx: &mut Option<u8>) {
        self.offer(ctx);
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

/// The writer thread: wakes (a message if any, then an offer) until the
/// producer is done — a slow sender needs it to drain.
fn spawn_writer(w: &Arc<World>) -> impl FnOnce() -> Option<u8> {
    let w = Arc::clone(w);
    let handle = thread_spawn(move || {
        let mut ctx = Some(0u8);
        loop {
            w.writer_wake(&mut ctx);
            if w.done.load(Ordering::Acquire) {
                return ctx;
            }
            yield_now();
        }
    });
    move || handle.join().unwrap()
}

/// The writer for `model_replies`: like [`spawn_writer`], but its second wake
/// runs under `always` (takes the position back and holds the lane) and the
/// rest leave it again — what the producer sees right after a `CONFIG SET
/// appendfsync always` → `everysec`.
fn spawn_policy_writer(w: &Arc<World>) -> impl FnOnce() -> Option<u8> {
    let w = Arc::clone(w);
    let handle = thread_spawn(move || {
        let mut ctx = Some(0u8);
        let mut wake = 0u32;
        loop {
            wake += 1;
            if wake == 2 {
                w.writer_always(&mut ctx);
                let msg = w.chan.lock().unwrap().pop_front();
                if let Some(msg) = msg {
                    w.reclaim(&mut ctx);
                    w.writer_write(&mut ctx, msg);
                }
            } else {
                w.writer_wake(&mut ctx);
            }
            if w.done.load(Ordering::Acquire) {
                return ctx;
            }
            yield_now();
        }
    });
    move || handle.join().unwrap()
}

/// Write before reply (checks 5.): from a held boot, three acknowledged
/// writes while the writer hands over, holds (`always`) and hands over again.
fn model_replies(how: Reply) {
    let w = Arc::new(World::attached());
    let writer = spawn_policy_writer(&w);
    let producer = {
        let w = Arc::clone(&w);
        thread_spawn(move || {
            w.write_and_reply(1, how);
            w.write_and_reply(2, how);
            w.write_and_reply(3, how);
            w.done.store(true, Ordering::Release);
        })
    };
    producer.join().unwrap();
    let ctx = writer();
    w.finish(ctx, 3);
}

/// The two-producer model's writer: a fixed number of wakes (nothing spins
/// on it — a reply on the acked path is checked at its ack), then `finish`
/// drains the rest.
fn spawn_writer_steps(w: &Arc<World>, wakes: usize) -> impl FnOnce() -> Option<u8> {
    let w = Arc::clone(w);
    let handle = thread_spawn(move || {
        let mut ctx = Some(0u8);
        for _ in 0..wakes {
            w.writer_wake(&mut ctx);
        }
        ctx
    });
    move || handle.join().unwrap()
}

/// Write before reply with two producers (checks 6.), from a held boot: one
/// acknowledges a write, the other appends and barriers on a possibly stale
/// hold.
fn model_two_producers(read_first: bool) {
    let mut world = World::attached();
    world.cap = 8;
    let w = Arc::new(world);
    let writer = spawn_writer_steps(&w, 3);
    let replier = {
        let w = Arc::clone(&w);
        thread_spawn(move || w.write_and_reply_next(read_first))
    };
    let stale = {
        let w = Arc::clone(&w);
        thread_spawn(move || w.stale_barrier_producer())
    };
    replier.join().unwrap();
    stale.join().unwrap();
    let ctx = writer();
    w.finish(ctx, 2);
    assert_eq!(
        w.reply_at_ack.load(Ordering::Acquire),
        0,
        "a reply waited for an ack that never came"
    );
}

/// The producer appends 1, flushes, sends an `AppendSync` 2, appends 3,
/// sends a control message, appends 4 and flushes, while the writer wakes.
fn model_lane(how: How) {
    let w = Arc::new(World::new());
    let writer = spawn_writer(&w);
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
            w.done.store(true, Ordering::Release);
        })
    };
    producer.join().unwrap();
    let ctx = writer();
    w.finish(ctx, 4);
}

/// A tighter shape for the slow path: two appends against a channel of 1.
fn model_slow_path(how: How) {
    let w = Arc::new(World::new());
    let writer = spawn_writer(&w);
    let producer = {
        let w = Arc::clone(&w);
        thread_spawn(move || {
            w.append(1, how);
            w.append(2, how);
            w.append(3, how);
            w.flush();
            w.done.store(true, Ordering::Release);
        })
    };
    producer.join().unwrap();
    let ctx = writer();
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

    /// Three threads with a writer of several wakes take longer paths than
    /// loom's default branch budget.
    fn model_wide(f: impl Fn() + Sync + Send + 'static) {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(3);
        builder.max_branches = 100_000;
        builder.check(f);
    }

    #[test]
    fn loom_lane_hand_over_keeps_order_and_context() {
        model(|| model_lane(How::Correct));
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

    #[test]
    fn loom_held_lane_replies_after_the_write() {
        model(|| model_replies(Reply::Correct));
    }

    #[test]
    #[should_panic(expected = "write before reply violated")]
    fn loom_ignoring_the_hold_is_caught() {
        model(|| model_replies(Reply::IgnoreHold));
    }

    #[test]
    #[should_panic(expected = "write before reply violated")]
    fn loom_appendsync_flip_without_rehold_is_caught() {
        model(|| model_replies(Reply::NoRehold));
    }

    #[test]
    fn loom_two_producers_with_a_stale_hold_reply_after_the_write() {
        model_wide(|| model_two_producers(false));
    }

    #[test]
    #[should_panic(expected = "write before reply violated")]
    fn loom_reading_the_hold_before_the_append_is_caught() {
        model_wide(|| model_two_producers(true));
    }
}

#[cfg(not(loom))]
mod smoke {
    use super::*;

    #[test]
    fn smoke_lane_hand_over_keeps_order_and_context() {
        for _ in 0..1000 {
            model_lane(How::Correct);
        }
    }

    #[test]
    fn smoke_lane_slow_sender_holds_the_hand_over() {
        for _ in 0..500 {
            model_slow_path(How::Correct);
        }
    }

    #[test]
    fn smoke_held_lane_replies_after_the_write() {
        for _ in 0..500 {
            model_replies(Reply::Correct);
        }
    }

    #[test]
    fn smoke_two_producers_reply_after_the_write() {
        for _ in 0..500 {
            model_two_producers(false);
        }
    }
}
