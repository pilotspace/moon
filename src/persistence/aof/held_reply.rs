//! A reply decided from a policy read BEFORE its record's enqueue (moon#1266
//! 1A, R2b round 2 F2).
//!
//! A producer that read "not `always`, lane not held" and then appended a
//! plain record can find, after the enqueue, that the lane was held in
//! between: by the writer entering `always` (`take_back_held`), or by another
//! producer's `AppendSync` flip. Its record then sits in the writer's channel
//! with no ack for the reply to wait on. The awaiting paths re-read after the
//! enqueue and barrier (`AofWriterPool::try_send_append_durable`: its
//! `fsync_barrier` is a no-op unless the lane is held or the policy is
//! `always`). The monoio inline `SET` path cannot await, so it records the
//! debt here ([`note_after_append`]), and the connection handler pays it
//! ([`take_owed`] → `fsync_barrier`) before the batch's replies leave. Both
//! run on the same shard thread with no `.await` in between, so the flag
//! cannot be seen by another connection.

use std::cell::Cell;

use super::{AofWriterPool, FsyncPolicy};

thread_local! {
    /// An inline write's record went to a held lane since the last take.
    static OWED: Cell<bool> = const { Cell::new(false) };
}

/// After an inline append to `shard_id`'s writer: if that lane is held (or
/// the policy is `always`) NOW, the reply owes a barrier. One Acquire load.
#[inline]
pub fn note_after_append(pool: &AofWriterPool, shard_id: usize) {
    if pool.fsync_policy_for(shard_id) == FsyncPolicy::Always {
        let _ = OWED.try_with(|o| o.set(true));
    }
}

/// Whether this thread's inline writes owe a barrier (and clear the debt).
#[inline]
pub fn take_owed() -> bool {
    OWED.try_with(|o| o.replace(false)).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_held_lane_after_the_append_owes_a_barrier_once() {
        if !crate::persistence::aof::lane::enabled() {
            return; // MOON_AOF_SHARD_WRITE=0 in this test's environment
        }
        let (tx, _rx) = crate::runtime::channel::mpsc_bounded(8);
        let pool = AofWriterPool::top_level(tx);
        assert!(!take_owed());
        note_after_append(&pool, 0);
        assert!(!take_owed(), "everysec, unheld: nothing owed");
        let _lane = pool.lane(0); // attached: held until its first hand-over
        note_after_append(&pool, 0);
        assert!(take_owed());
        assert!(!take_owed(), "taken once");
    }
}
