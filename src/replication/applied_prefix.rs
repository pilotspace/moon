//! The replica's `master_repl_offset` counts exactly the stream it APPLIED
//! (R2b round 4 X1-DBL).
//!
//! The read loop drains a socket read into a batch of records and applies
//! them one by one; since R2b round 3 X1 it may await between two of them
//! (`replica_aof::admit`, room in this node's AOF writer). A task superseded
//! while it waits — any `REPLICAOF`, `NO ONE` — exits without applying the
//! rest. The offset used to advance only once the whole batch was applied,
//! so such an exit left it at the batch's start: the next task's
//! `PSYNC <replid> <offset>` got `+CONTINUE` from there and the master
//! re-sent the applied prefix, which the replica applied (and logged) a
//! second time — an `INCR` counted twice.
//!
//! [`AppliedPrefix`] advances the offset, and the stream's db context, right
//! after each applied record, with no await in between. Every exit — a
//! supersede, a poison record, a missing shard, a dropped link — therefore
//! leaves the offset at the end of the last applied record: the frames after
//! it (including a `SELECT` that preceded the next record) are re-sent, the
//! applied ones never are.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::replication::apply::ReplCommand;

/// Commits the applied prefix of one drained batch (see the module doc).
pub(crate) struct AppliedPrefix {
    offset: Arc<AtomicU64>,
    /// Bytes of the current batch already added to `offset`.
    committed: usize,
}

impl AppliedPrefix {
    /// `offset` is the replication state's `master_repl_offset` (one `Arc`
    /// for the life of the state, so it is cloned once per link).
    pub(crate) fn new(offset: Arc<AtomicU64>) -> Self {
        Self {
            offset,
            committed: 0,
        }
    }

    /// A new batch starts: nothing of it is committed yet.
    pub(crate) fn start_batch(&mut self) {
        self.committed = 0;
    }

    /// `rc` was applied: the offset now covers the batch through its frame,
    /// and the stream's db context is the one it was applied in.
    pub(crate) fn applied(&mut self, rc: &ReplCommand, stream_db: &AtomicUsize) {
        self.advance_to(rc.end_offset);
        stream_db.store(rc.db_index, Ordering::Relaxed);
    }

    /// Every record of the batch was applied: the frames after the last one
    /// (`SELECT`, `PING`, `REPLCONF`) are consumed too, and `selected_db` is
    /// the db context the drain ended in.
    pub(crate) fn finish(&mut self, consumed: usize, selected_db: usize, stream_db: &AtomicUsize) {
        self.advance_to(consumed);
        stream_db.store(selected_db, Ordering::Relaxed);
    }

    fn advance_to(&mut self, end: usize) {
        if end > self.committed {
            self.offset
                .fetch_add((end - self.committed) as u64, Ordering::Relaxed);
            self.committed = end;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::apply::drain_replicated_commands;
    use bytes::BytesMut;

    fn resp(parts: &[&[u8]]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", parts.len()).into_bytes();
        for p in parts {
            out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            out.extend_from_slice(p);
            out.extend_from_slice(b"\r\n");
        }
        out
    }

    /// Each record carries the end of its own frame; the frames that are
    /// not records (SELECT, PING) only count once a later record or the
    /// batch's end covers them.
    #[test]
    fn a_record_ends_where_its_frame_ends() {
        let select = resp(&[b"SELECT", b"3"]);
        let incr = resp(&[b"INCR", b"c"]);
        let ping = resp(&[b"PING"]);
        let mut wire = select.clone();
        wire.extend_from_slice(&incr);
        wire.extend_from_slice(&ping);
        wire.extend_from_slice(&incr);
        let mut buf = BytesMut::from(&wire[..]);
        let mut db = 0usize;
        let r = drain_replicated_commands(&mut buf, &mut db);
        assert_eq!(r.commands.len(), 2);
        assert_eq!(r.commands[0].end_offset, select.len() + incr.len());
        assert_eq!(r.commands[1].end_offset, wire.len());
        assert_eq!(r.consumed, wire.len());
        assert_eq!(r.commands[0].db_index, 3);
    }

    /// X1-DBL: a batch abandoned after its first record leaves the offset at
    /// that record's end and the db context it ran in — not at the batch's
    /// start (the prefix would be re-sent and applied twice), not at its end
    /// (the rest would be lost).
    #[test]
    fn an_abandoned_batch_commits_exactly_its_applied_prefix() {
        let select = resp(&[b"SELECT", b"2"]);
        let incr = resp(&[b"INCR", b"c"]);
        let select5 = resp(&[b"SELECT", b"5"]);
        let mut wire = select.clone();
        wire.extend_from_slice(&incr);
        wire.extend_from_slice(&select5);
        wire.extend_from_slice(&incr);
        let mut buf = BytesMut::from(&wire[..]);
        let mut db = 0usize;
        let r = drain_replicated_commands(&mut buf, &mut db);
        let offset = Arc::new(AtomicU64::new(1000));
        let stream_db = AtomicUsize::new(0);
        let mut prefix = AppliedPrefix::new(offset.clone());
        prefix.start_batch();
        prefix.applied(&r.commands[0], &stream_db);
        // ... superseded here: the task exits.
        assert_eq!(
            offset.load(Ordering::Relaxed),
            1000 + (select.len() + incr.len()) as u64
        );
        assert_eq!(stream_db.load(Ordering::Relaxed), 2);

        // A batch applied to the end commits all of it, once.
        let offset = Arc::new(AtomicU64::new(0));
        let mut prefix = AppliedPrefix::new(offset.clone());
        prefix.start_batch();
        for rc in &r.commands {
            prefix.applied(rc, &stream_db);
        }
        prefix.finish(r.consumed, db, &stream_db);
        assert_eq!(offset.load(Ordering::Relaxed), wire.len() as u64);
        assert_eq!(stream_db.load(Ordering::Relaxed), 5);
        // The next batch starts from zero.
        prefix.start_batch();
        prefix.finish(0, 5, &stream_db);
        assert_eq!(offset.load(Ordering::Relaxed), wire.len() as u64);
    }
}
