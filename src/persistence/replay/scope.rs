//! Marks a log replay on this thread (moon#1286).
//!
//! redis counts no expiry while it loads (`keyIsExpired` answers 0 when
//! `server.loading`): the log already holds the deletions the live server
//! made — the active cycle's reason `DEL`s, a client's `DEL` of an expired
//! key, a write over one — and each was counted once, live. Replaying them
//! must not count them again, or every restart would start `expired_keys` at
//! the number of reaps logged since the last rewrite.
//!
//! [`ReplayScope`] is entered by `DispatchReplayEngine::replay_command`, the
//! one funnel every RESP log replay (AOF, manifest shards, WAL v3) takes, and
//! read by `admin::metrics_setup::counts_expiry`. A thread-local, like the
//! replica's `MasterStreamScope`: the replayed command runs on the thread
//! that replays it.

use std::cell::Cell;

thread_local! {
    /// Depth of [`ReplayScope`]s on this thread.
    static REPLAYING: Cell<u32> = const { Cell::new(0) };
}

/// Is this thread replaying a log record? One thread-local load.
#[inline]
pub(crate) fn replaying() -> bool {
    REPLAYING.with(Cell::get) != 0
}

/// One replayed record; cleared on drop, so an early return or an unwind
/// cannot leave a live command looking like a replay.
#[must_use = "the scope ends when the guard drops"]
pub(crate) struct ReplayScope(());

impl ReplayScope {
    pub(crate) fn enter() -> Self {
        REPLAYING.with(|d| d.set(d.get() + 1));
        ReplayScope(())
    }
}

impl Drop for ReplayScope {
    fn drop(&mut self) {
        REPLAYING.with(|d| d.set(d.get().saturating_sub(1)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_scope_nests_and_ends_with_its_guard() {
        assert!(!replaying());
        {
            let _outer = ReplayScope::enter();
            assert!(replaying());
            {
                let _inner = ReplayScope::enter();
                assert!(replaying());
            }
            assert!(replaying(), "the outer scope is still open");
        }
        assert!(!replaying());
    }

    /// A replayed `DEL` of a key whose TTL passed, and a replayed write over
    /// one, reap it as the live command did — and count nothing: the live
    /// server counted both (moon#1286). The same reap outside a replay counts.
    #[test]
    fn a_replayed_reap_is_not_counted_in_expired_keys() {
        use crate::admin::metrics_setup::this_thread_expired_keys;
        use crate::persistence::replay::{CommandReplayEngine, DispatchReplayEngine};
        use crate::protocol::Frame;
        use crate::storage::Database;
        use crate::storage::entry::{Entry, current_time_ms};
        use bytes::Bytes;

        let past = current_time_ms() - 1000;
        let expired = |db: &mut Database, k: &'static [u8]| {
            db.set(
                &Bytes::from_static(k),
                Entry::new_string_with_expiry(Bytes::from_static(b"v"), past),
            );
        };
        let bulk = |b: &'static [u8]| Frame::BulkString(Bytes::from_static(b));
        let mut dbs = vec![Database::new()];
        expired(&mut dbs[0], b"d");
        expired(&mut dbs[0], b"h");
        let engine = DispatchReplayEngine::new();
        let mut selected = 0usize;
        let before = this_thread_expired_keys();
        engine.replay_command(&mut dbs, b"DEL", &[bulk(b"d")], &mut selected);
        engine.replay_command(
            &mut dbs,
            b"HSET",
            &[bulk(b"h"), bulk(b"f"), bulk(b"v")],
            &mut selected,
        );
        assert_eq!(this_thread_expired_keys(), before, "replay counts nothing");
        assert!(!replaying(), "the scope ends with the record");

        expired(&mut dbs[0], b"live");
        let mut sel = 0usize;
        let _ = crate::command::dispatch(&mut dbs[0], b"DEL", &[bulk(b"live")], &mut sel, 16);
        assert_eq!(
            this_thread_expired_keys(),
            before + 1,
            "the same reap from a client counts"
        );
    }
}
