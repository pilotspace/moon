//! Same-shard message queue — the self-loop the SPSC mesh doesn't have.
//!
//! `ChannelMesh` is N·(N−1) with skip-self mapping (`target_index`
//! debug-asserts `my_id != target_id`), so a task running ON a shard's own
//! thread cannot SPSC a `ShardMessage` to that shard. Before this module,
//! three same-thread producers silently no-op'd at `shards=1` (where the
//! producer Vec is EMPTY) and would have targeted the WRONG shard at
//! `shards>1`:
//!
//!   1. the inline PSYNC task's `RegisterReplica` (master.rs) — every replica
//!      attach failed with "shard 0 producer missing" and the replica fell
//!      into a 0.5s reconnect/full-resync loop that MASKED the dead live
//!      stream (each resync's RDB carried the latest keyspace + FT defs);
//!   2. the FT.* index-definition replication fan-out (ft.rs);
//!   3. the graph WAL-record replication fan-out (write.rs).
//!
//! Replication messages carried here are DELIVERY-ONLY (`ReplicaLiveFanout`):
//! the backlog append and shard-offset advance happen synchronously at write
//! time in `record_local_write`, atomic with the mutation w.r.t. the inline
//! PSYNC task's snapshot capture. Deferring the offset advance to the drain
//! (the original design) let a mutation sit inside a FULLRESYNC RDB while
//! still below the advertised snapshot offset — re-delivered via backlog
//! catch-up, double-applying non-idempotent commands (adversarial-review
//! P0-2). `RegisterReplica.push_offset` is the matching pusher-side capture.
//!
//! One shard per OS thread (monoio thread-per-core), so a `thread_local!`
//! queue IS the per-shard self-channel — same pattern as `shard::slice`.
//! The event loop drains it inside `drain_spsc_shared` (ahead of the SPSC
//! consumers) and registers its `Notify` here at startup so a push from a
//! sibling task wakes a parked loop instead of waiting for the 1ms tick.
//!
//! ⚠ Tokio (work-stealing) tasks must NOT push here — their thread is not a
//! shard thread. All current producers are monoio-only paths.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::Arc;

use crate::runtime::channel::Notify;
use crate::shard::dispatch::ShardMessage;

thread_local! {
    static SELF_QUEUE: RefCell<VecDeque<ShardMessage>> = const { RefCell::new(VecDeque::new()) };
    static DRAIN_NOTIFY: RefCell<Option<Arc<Notify>>> = const { RefCell::new(None) };
}

/// Register the shard event loop's SPSC-drain `Notify` for this thread.
/// Called once at event-loop startup; a later `push` wakes the drain arm
/// immediately instead of stranding the message until the periodic tick.
pub fn register_drain_notify(notify: Arc<Notify>) {
    DRAIN_NOTIFY.with(|n| *n.borrow_mut() = Some(notify));
}

/// Enqueue a message for THIS shard's own drain loop and wake it.
///
/// Caller must be on a shard thread (connection handler / task spawned by
/// the shard's event loop). Messages are handled by the same
/// `handle_shard_message_shared` the SPSC consumers feed, in FIFO order.
pub fn push(msg: ShardMessage) {
    SELF_QUEUE.with(|q| q.borrow_mut().push_back(msg));
    DRAIN_NOTIFY.with(|n| {
        if let Some(notify) = n.borrow().as_ref() {
            notify.notify_one();
        }
    });
}

/// Pop the next self-message (drain side; event-loop thread only).
pub fn pop() -> Option<ShardMessage> {
    SELF_QUEUE.with(|q| q.borrow_mut().pop_front())
}
