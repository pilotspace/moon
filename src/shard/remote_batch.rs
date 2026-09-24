//! The connection handlers' per-batch cross-shard command buffer (moon#1177).
//!
//! A pipelined batch queues every command whose key lives on another shard
//! here, then phase 2 sends each target ONE `PipelineBatchSlotted` and awaits
//! the replies. This used to be a `HashMap<usize, Vec<(sink, Arc<Frame>,
//! Option<Bytes>, Option<TrackedWriteKeys>, shape)>>`, which cost per REMOTE
//! COMMAND a SipHash of the shard id, an `Arc` allocation (88 B, freed on the
//! other thread) for a frame nobody shared, and ~210 B of inline tuple — half
//! of it an `Option<SmallVec<[Bytes; 4]>>` that is `None` unless CLIENT
//! TRACKING is on; and per batch per target a drained `Vec` reallocated next
//! batch plus two more from `unzip`.
//!
//! Now: two `Vec`s indexed by shard id, kept for the connection's life. The
//! commands of a target are handed to its message with `mem::take` (they
//! travel to the other thread, so that one allocation per target per batch is
//! the floor), and the bookkeeping `Vec` comes back after the replies are
//! folded, so its capacity is reused batch after batch. Allocation happens on
//! the connection's first cross-shard command, never for a local-only one
//! (moon#1179 item 3).

use crate::protocol::Frame;
use crate::protocol::resp3::Resp3Shape;
use crate::server::conn::fanout::ReplySink;
use crate::tracking::invalidation::TrackedWriteKeys;

/// What the handler needs back, per remote command, to finish its reply.
#[derive(Debug)]
pub(crate) struct RemoteMeta {
    /// Where the reply goes.
    pub sink: ReplySink,
    /// A persisted write whose owner appended it to its AOF: the reply joins
    /// that owner's `appendfsync always` barrier. A `bool` — the owner derives
    /// the record from its own reply (moon#825), so the origin never needs the
    /// bytes (moon#1177: it used to serialize them and throw them away).
    pub persisted_write: bool,
    /// Keys to invalidate for CLIENT TRACKING once the write is confirmed.
    /// Boxed: `None` unless a tracking client exists, so the common case
    /// carries one pointer instead of an inline `SmallVec<[Bytes; 4]>`.
    pub track_keys: Option<Box<TrackedWriteKeys>>,
    /// RESP3 conversion, classified at enqueue while the args existed.
    pub shape: Resp3Shape,
}

/// Per-target remote commands of the current batch. See the module docs.
#[derive(Default)]
pub(crate) struct RemoteBatch {
    commands: Vec<Vec<Frame>>,
    meta: Vec<Vec<RemoteMeta>>,
    /// The db each target's commands run in, captured at its first command.
    /// Every remote command of one batch shares a db — a `SELECT` behind a
    /// pending remote write is an ordering barrier that defers the tail — but
    /// capturing it at enqueue keeps that from being a load-bearing
    /// assumption here.
    db: Vec<usize>,
    /// Commands queued in this batch, all targets.
    len: usize,
}

impl RemoteBatch {
    /// Queue `frame` for `target` (of `num_shards`) in db `db_index`.
    #[inline]
    pub(crate) fn push(
        &mut self,
        num_shards: usize,
        target: usize,
        db_index: usize,
        frame: Frame,
        meta: RemoteMeta,
    ) {
        if self.commands.len() < num_shards {
            self.commands.resize_with(num_shards, Vec::new);
            self.meta.resize_with(num_shards, Vec::new);
            self.db.resize(num_shards, 0);
        }
        if self.commands[target].is_empty() {
            self.db[target] = db_index;
        }
        self.commands[target].push(frame);
        self.meta[target].push(meta);
        self.len += 1;
    }

    /// No command queued in this batch.
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Commands queued in this batch, all targets.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Number of shard slots (0 until the first push).
    #[inline]
    pub(crate) fn slots(&self) -> usize {
        self.commands.len()
    }

    /// Hand over `target`'s commands, bookkeeping and db, or `None` when it
    /// has none. The commands move into the message; give the bookkeeping
    /// back with [`Self::recycle`] once its replies are folded.
    #[inline]
    pub(crate) fn take(&mut self, target: usize) -> Option<(Vec<Frame>, Vec<RemoteMeta>, usize)> {
        let commands = self.commands.get_mut(target)?;
        if commands.is_empty() {
            return None;
        }
        let commands = std::mem::take(commands);
        let meta = std::mem::take(&mut self.meta[target]);
        self.len -= commands.len();
        Some((commands, meta, self.db[target]))
    }

    /// Return a drained bookkeeping `Vec` so the next batch reuses its
    /// capacity.
    #[inline]
    pub(crate) fn recycle(&mut self, target: usize, mut meta: Vec<RemoteMeta>) {
        meta.clear();
        if let Some(slot) = self.meta.get_mut(target)
            && slot.capacity() < meta.capacity()
        {
            *slot = meta;
        }
    }

    /// Drop everything queued (a batch that ended early). Capacity is kept.
    #[inline]
    pub(crate) fn clear(&mut self) {
        if self.len == 0 {
            return;
        }
        for v in &mut self.commands {
            v.clear();
        }
        for v in &mut self.meta {
            v.clear();
        }
        self.len = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn meta(i: usize) -> RemoteMeta {
        RemoteMeta {
            sink: ReplySink::Direct(i),
            persisted_write: i % 2 == 0,
            track_keys: None,
            shape: Resp3Shape::None,
        }
    }

    fn cmd(s: &'static [u8]) -> Frame {
        Frame::BulkString(Bytes::from_static(s))
    }

    #[test]
    fn groups_by_target_in_push_order_and_reuses_meta_capacity() {
        let mut b = RemoteBatch::default();
        assert!(b.is_empty());
        assert_eq!(b.slots(), 0, "no allocation before the first push");
        b.push(4, 2, 0, cmd(b"a"), meta(0));
        b.push(4, 1, 0, cmd(b"b"), meta(1));
        b.push(4, 2, 0, cmd(b"c"), meta(2));
        assert_eq!(b.len(), 3);
        assert!(b.take(0).is_none() && b.take(3).is_none());
        let (cmds, m, db) = b.take(2).expect("target 2");
        assert_eq!(cmds, vec![cmd(b"a"), cmd(b"c")]);
        assert_eq!(
            m.iter().map(|x| x.sink).collect::<Vec<_>>(),
            vec![ReplySink::Direct(0), ReplySink::Direct(2)]
        );
        assert_eq!(db, 0);
        assert_eq!(b.len(), 1);
        let cap = m.capacity();
        b.recycle(2, m);
        let (_, m1, _) = b.take(1).expect("target 1");
        b.recycle(1, m1);
        assert!(b.is_empty());
        // The next batch reuses target 2's bookkeeping allocation.
        b.push(4, 2, 5, cmd(b"d"), meta(3));
        let (_, m2, db2) = b.take(2).expect("target 2 again");
        assert!(m2.capacity() >= cap);
        assert_eq!(db2, 5, "db captured at the target's first push");
    }

    #[test]
    fn clear_drops_queued_commands() {
        let mut b = RemoteBatch::default();
        b.push(2, 1, 0, cmd(b"a"), meta(0));
        b.clear();
        assert!(b.is_empty());
        assert!(b.take(1).is_none());
    }

    #[test]
    fn remote_meta_is_small() {
        // The inline `Option<SmallVec<[Bytes; 4]>>` alone was ~144 B.
        assert!(
            std::mem::size_of::<RemoteMeta>() <= 32,
            "{}",
            std::mem::size_of::<RemoteMeta>()
        );
    }
}
