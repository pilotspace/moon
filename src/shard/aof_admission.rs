//! Admission of routed write legs against the owning shard's AOF writer
//! (moon#769), decided BEFORE a leg applies anything and without ever making
//! the shard thread wait.
//!
//! # Where a leg waits
//!
//! A routed leg (`Execute`, `PipelineBatchSlotted`, `MultiExecute`,
//! `TxnExecute`) arrives on the SPSC ring of the shard that sent it.
//! The drain looks at the head of each ring before it pops it. When the
//! head is a write leg this shard's AOF writer cannot take yet, the drain
//! leaves it where it is and moves on to the next ring. The ring is the
//! producer's re-admission queue:
//!
//! - **FIFO per producer, by construction.** Nothing behind a parked head is
//!   popped while it is parked, so no later message from that producer (a
//!   write, a read of the key it writes, a `SWAPDB` leg) can overtake it.
//!   Nothing is copied out of the ring and put back, so there is no second
//!   queue whose order could drift from the ring's.
//! - **Everything else keeps flowing.** Other producers' rings, local
//!   connections, timers and control rings (the `AofFold` ring is separate)
//!   are served as usual. The drain never sleeps: a parked head costs one
//!   peek and one channel-length read per drain.
//! - **Retried every drain, in order.** The rotation visits every ring each
//!   cycle and the parked head is the first thing looked at on its ring. A
//!   non-empty ring also keeps the event loop off its idle park, so the
//!   retry runs at least every 1 ms tick.
//! - **Backpressure reaches the producer.** A parked head holds its ring;
//!   when the ring fills, the producer's bounded `spsc_send` waits on its
//!   own connection task.
//!
//! # When a parked leg gives up
//!
//! A head that has waited [`AofWriterPool::routed_admission_wait`] is
//! refused unapplied ([`AOF_BACKPRESSURE_REFUSED_ERR`]). That wait is
//! `--aof-fsync-timeout-ms`, with `0` (documented as unbounded) mapped to
//! [`ROUTED_ADMISSION_WAIT_CAP`], and never above it, so the refusal always
//! reaches the producer well inside its 30 s cross-shard reply timeout.
//!
//! After a refusal the shard treats its writer as stalled (a circuit
//! breaker): while it stays without room, every other routed write leg is
//! refused as soon as it reaches a ring head, instead of each waiting a full
//! bound in turn. The first admission that finds room closes the circuit
//! again. So whatever sits behind a parked head waits at most one admission
//! wait, which the script fan-out budget accounts for.
//!
//! # What admission counts
//!
//! A leg's records are its commands flagged `WRITE` or `MAY_REPLICATE`, and
//! the room it needs is those records plus [`ROUTED_ADMISSION_HEADROOM`] on
//! top of everything already admitted in the same drain cycle (the legs run
//! only once the cycle has been collected). The headroom absorbs what the
//! count cannot see in advance:
//!
//! - eviction reason-`DEL`s a write triggers under an evicting policy,
//! - the extra records of a script or function that writes more than once,
//! - `fsync_barrier`s other shards' connections push into this writer
//!   (the `SWAPDB` coordinator, `appendfsync always` batches),
//! - the records of non-routed arms (`SwapDb`) handled later in the cycle,
//! - the pops a write serves: a push that wakes blocked waiters logs one
//!   record per served pop, on this shard, as it pops
//!   (`blocking::pop_log`).
//!
//! What remains: a single leg whose uncounted records exceed the headroom
//! (a script writing hundreds of keys, a write that evicts hundreds, a push
//! that serves hundreds of waiters) can still meet a full channel after it
//! applied. That falls back to the existing post-apply bounded block and
//! its fail-loud `AOF_APPEND_LOST_ERR` (for a served pop, `pop_log`'s own
//! bounded producer and its backpressure error to the waiter), which is
//! rare now instead of routine. And a leg
//! with more records than the channel holds is admitted only once the
//! channel is empty; under sustained local writes to the same shard it can
//! wait out its bound and be refused.
//!
//! The gate applies under every `appendfsync` policy: it is about room in
//! the writer's channel, which all three share.

use std::cell::RefCell;
use std::time::{Duration, Instant};

use crate::persistence::aof::AofWriterPool;
use crate::protocol::Frame;
use crate::shard::dispatch::ShardMessage;

/// Reply for a routed command refused BEFORE it ran, because its shard's AOF
/// writer could not take its records within the admission wait (moon#769).
/// Nothing was applied: the keyspace, the AOF and the replicas all still
/// agree, and the client can retry.
///
/// A multi-shard command whose other legs DID apply reports
/// [`AOF_BACKPRESSURE_PARTIAL_ERR`] instead; see
/// `coordinator::refused_leg_error`.
pub(crate) const AOF_BACKPRESSURE_REFUSED_ERR: &[u8] =
    b"MOONERR AOF backpressure: command not executed, the AOF writer is stalled; retry";

/// Reply for a multi-shard command (`MSET`, `DEL`, `UNLINK`) when a shard
/// refused its part unapplied while other parts were applied. The command
/// is not atomic across shards, so this says exactly that: the stalled
/// shard's keys are unchanged, the others were applied. (A multi-shard
/// `FLUSHALL`/`FLUSHDB` already reports its own `MOONERR FLUSH partial`.)
pub(crate) const AOF_BACKPRESSURE_PARTIAL_ERR: &[u8] =
    b"MOONERR AOF backpressure: command partially executed; its keys on a shard whose AOF \
      writer is stalled were left unchanged, the rest were applied";

/// Upper bound on how long a routed write leg may wait at the head of its
/// ring for AOF room, whatever `--aof-fsync-timeout-ms` says (including `0`).
/// Well under the 30 s cross-shard reply timeout, so a producer always gets
/// the refusal instead of "no reply" and never has to guess.
pub(crate) const ROUTED_ADMISSION_WAIT_CAP: Duration = Duration::from_secs(10);

/// Room admission keeps free beyond the records it counts, for the records it
/// cannot count in advance (see the module docs). About 2.5% of the 10k
/// writer channel.
pub(crate) const ROUTED_ADMISSION_HEADROOM: usize = 256;

/// What the drain does with the leg at the head of a ring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Admission {
    /// Pop it and run it: its records have room (now reserved).
    Admit,
    /// Leave it at the head, unapplied; stop draining this ring for now.
    Wait,
    /// Pop it and answer it with [`AOF_BACKPRESSURE_REFUSED_ERR`], unapplied.
    Refuse,
}

/// Per-shard-thread admission state that outlives a drain cycle.
#[derive(Default)]
struct HeadState {
    /// When each ring's current head first had to wait (index = ring).
    parked_since: Vec<Option<Instant>>,
    /// The circuit breaker: a parked head's wait expired and no admission has
    /// found room since.
    stalled: bool,
}

thread_local! {
    // One shard per OS thread: this is that shard's admission state.
    static HEADS: RefCell<HeadState> = RefCell::new(HeadState::default());
}

/// Admission for one drain cycle. Create one per cycle; it carries the room
/// the cycle has already promised to the legs it admitted.
pub(crate) struct AdmissionCycle<'a> {
    pool: &'a AofWriterPool,
    shard_id: usize,
    /// Records of the legs admitted this cycle, not yet run.
    reserved: usize,
    /// Whether the thread's [`HeadState`] may hold anything to clear (a
    /// parked head or an open circuit). Read once per cycle, so an admission
    /// with nothing parked anywhere touches no thread-local state.
    heads_dirty: bool,
}

impl<'a> AdmissionCycle<'a> {
    pub(crate) fn new(pool: &'a AofWriterPool, shard_id: usize) -> Self {
        let heads_dirty = HEADS.with(|h| {
            let h = h.borrow();
            h.stalled || h.parked_since.iter().any(Option::is_some)
        });
        Self {
            pool,
            shard_id,
            reserved: 0,
            heads_dirty,
        }
    }

    /// Decide the head `msg` of ring `ring`. Never blocks.
    pub(crate) fn decide(&mut self, ring: usize, msg: &ShardMessage) -> Admission {
        let Some(upper) = routed_leg_len(msg) else {
            // Not a routed command leg: not gated here.
            return Admission::Admit;
        };
        // Fast path, one queue-length read: room for every command of the leg
        // counted as a record, on top of this cycle's reservations.
        let free = self.pool.free_append_slots(self.shard_id);
        let fast_need = self
            .reserved
            .saturating_add(upper)
            .saturating_add(ROUTED_ADMISSION_HEADROOM);
        if free >= fast_need && !self.pool.append_writer_gone(self.shard_id) {
            return self.admit(ring, upper, true);
        }
        let records = routed_leg_records(msg);
        if records == 0 {
            // A read logs nothing and never waits. It found no room, so it
            // must not close an open circuit.
            return self.admit(ring, 0, false);
        }
        if self.pool.append_writer_gone(self.shard_id) {
            // Nothing can ever log it: refusing now is the only honest answer.
            return self.refuse(ring);
        }
        if self.pool.has_append_room(
            self.shard_id,
            self.reserved,
            records,
            ROUTED_ADMISSION_HEADROOM,
        ) {
            return self.admit(ring, records, true);
        }
        let now = Instant::now();
        let wait = self.pool.routed_admission_wait();
        self.heads_dirty = true;
        HEADS.with(|h| {
            let mut h = h.borrow_mut();
            if h.stalled {
                // The writer already failed one full wait and has not made
                // room since: fail fast instead of waiting a bound per leg.
                slot(&mut h, ring).take();
                return Admission::Refuse;
            }
            let since = slot(&mut h, ring);
            let first = since.is_none();
            let since = *since.get_or_insert(now);
            if first {
                crate::persistence::aof::AOF_BACKPRESSURE_STALLS
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            if now.duration_since(since) >= wait {
                slot(&mut h, ring).take();
                h.stalled = true;
                tracing::warn!(
                    "shard {}: AOF writer made no room for a routed write in {:?}; refusing \
                     routed writes unapplied until it does",
                    self.shard_id,
                    wait
                );
                Admission::Refuse
            } else {
                Admission::Wait
            }
        })
    }

    /// `room_found`: the writer had room for this leg's records (as opposed
    /// to a read admitted because it logs nothing). Only that closes an open
    /// circuit.
    fn admit(&mut self, ring: usize, records: usize, room_found: bool) -> Admission {
        self.reserved = self.reserved.saturating_add(records);
        if self.heads_dirty {
            HEADS.with(|h| {
                let mut h = h.borrow_mut();
                if room_found {
                    h.stalled = false;
                }
                // This ring's head (if it had been parked) is no longer waiting.
                slot(&mut h, ring).take();
            });
        }
        Admission::Admit
    }

    fn refuse(&mut self, ring: usize) -> Admission {
        if self.heads_dirty {
            HEADS.with(|h| {
                slot(&mut h.borrow_mut(), ring).take();
            });
        }
        Admission::Refuse
    }
}

fn slot(h: &mut HeadState, ring: usize) -> &mut Option<Instant> {
    if h.parked_since.len() <= ring {
        h.parked_since.resize(ring + 1, None);
    }
    &mut h.parked_since[ring]
}

/// Whether `cmd` can produce an AOF record: write-flagged, or a script /
/// function that may replicate the writes it issues.
#[inline]
fn may_log_to_aof(cmd: &[u8]) -> bool {
    use crate::command::metadata::CommandFlags;
    crate::command::metadata::lookup(cmd).is_some_and(|m| {
        m.flags.contains(CommandFlags::WRITE) || m.flags.contains(CommandFlags::MAY_REPLICATE)
    })
}

#[inline]
fn frame_may_log(frame: &Frame) -> bool {
    crate::shard::spsc_handler::extract_command_static(frame)
        .is_some_and(|(cmd, _)| may_log_to_aof(cmd))
}

/// Commands in a routed command leg, an upper bound on the records it
/// enqueues (before headroom): `None` for a message that is not a routed
/// command leg. No classification.
pub(crate) fn routed_leg_len(msg: &ShardMessage) -> Option<usize> {
    match msg {
        ShardMessage::Execute { .. } => Some(1),
        ShardMessage::PipelineBatchSlotted { commands, .. } => Some(commands.len()),
        ShardMessage::MultiExecute { commands, .. } => Some(commands.len()),
        ShardMessage::TxnExecute(payload) => Some(payload.commands.len()),
        _ => None,
    }
}

/// The records a routed leg enqueues once applied: one per command that can
/// log. A script counts as one; its extra records come out of the headroom.
fn routed_leg_records(msg: &ShardMessage) -> usize {
    match msg {
        ShardMessage::Execute { command, .. } => usize::from(frame_may_log(command)),
        ShardMessage::PipelineBatchSlotted { commands, .. } => {
            commands.iter().filter(|c| frame_may_log(c)).count()
        }
        ShardMessage::MultiExecute { commands, .. } => {
            commands.iter().filter(|(_, c)| frame_may_log(c)).count()
        }
        ShardMessage::TxnExecute(payload) => {
            payload.commands.iter().filter(|c| frame_may_log(c)).count()
        }
        _ => 0,
    }
}

/// Answer a routed leg that admission refused, without running any of it:
/// every command's reply is [`AOF_BACKPRESSURE_REFUSED_ERR`].
pub(crate) fn refuse_routed_leg(msg: ShardMessage) {
    let err = || Frame::Error(bytes::Bytes::from_static(AOF_BACKPRESSURE_REFUSED_ERR));
    let refused = routed_leg_len(&msg).unwrap_or(0) as u64;
    crate::persistence::aof::AOF_BACKPRESSURE_REFUSED
        .fetch_add(refused, std::sync::atomic::Ordering::Relaxed);
    match msg {
        ShardMessage::Execute { reply_tx, .. } => {
            let _ = reply_tx.send(crate::shard::dispatch::ExecReply::plain(err()));
        }
        ShardMessage::MultiExecute {
            commands, reply_tx, ..
        } => {
            let _ = reply_tx.send(commands.iter().map(|_| err()).collect());
        }
        ShardMessage::PipelineBatchSlotted {
            commands,
            response_slot,
            ..
        } => {
            response_slot
                .0
                .fill(commands.iter().map(|_| err()).collect());
        }
        ShardMessage::TxnExecute(payload) => {
            let _ = payload.reply_tx.send(crate::shard::dispatch::TxnExecReply {
                result: err(),
                exec_publishes: Vec::new(),
                exec_flushes: Vec::new(),
                wrote: false,
                append_lost: false,
            });
        }
        // `routed_leg_len` admits every other message unconditionally.
        _ => {}
    }
}
