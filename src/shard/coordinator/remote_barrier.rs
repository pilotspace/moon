//! moon#1322: a coordinated cross-shard write confirms its REMOTE legs'
//! durability before the reply.
//!
//! The coordinators send each remote leg as a `MultiExecute`; the owner shard
//! appends the leg's records to ITS writer (`wal_append_and_fanout`) and
//! replies at once. The connection handler barriers only its own shard
//! (`local_barrier_pending`). So under `appendfsync always` a spanning
//! `MSET`/`DEL`/`UNLINK`/`BITOP`/`COPY`, or a `FLUSHALL` broadcast, was
//! acknowledged with no fsync of the remote shard's file. And while a remote
//! lane was held (moon#1266 1A, W2B-1: boot, right after leaving `always`),
//! the reply could leave before the remote record's `write(2)`. `SWAPDB`
//! already closed this gap (`coordinate_swapdb`); these helpers do the same for
//! the others: after every remote leg has replied, ONE `fsync_barrier(target)`
//! per written remote shard. The leg's records are already queued at the
//! target (it appended before it replied), so the barrier's ack — after the
//! write, plus the fsync under `always` — covers them.
//!
//! Zero-cost in the steady state: [`barrier_needed`] is one Acquire load
//! (the pool-wide view is `always` only under `always` or while some lane is
//! held), and only then are the written keys hashed to their owners.
//!
//! The barriers — every written remote shard's, plus the local leg's when it
//! owes one — are SENT first and awaited together under one deadline
//! (`aof::barrier_set`, R2b round 3 P1): the reply waits for the slowest
//! fsync, not their sum, and a stalled disk costs one `fsync_timeout`, not
//! one per shard.

use std::sync::Arc;

use smallvec::SmallVec;

use crate::persistence::aof::barrier_set::PendingBarriers;
use crate::persistence::aof::{AofAck, AofWriterPool, FsyncPolicy};
use crate::protocol::Frame;
use crate::shard::dispatch::key_to_shard;

/// Owner shards of a coordinated write, beyond the coordinator's own.
pub(crate) type Targets = SmallVec<[usize; 8]>;

/// Whether any remote leg could owe a barrier now: the policy is `always`, or
/// a lane of this pool is held. When this reads `false`, every remote record
/// already reached its file before the leg replied (a DIRECT lane flushes
/// before `OneshotSender::send`), or was queued on a lane whose hand-over —
/// which needs an empty channel — has since written it.
#[inline]
pub(crate) fn barrier_needed(pool: Option<&Arc<AofWriterPool>>, num_shards: usize) -> bool {
    num_shards > 1 && pool.is_some_and(|p| p.fsync_policy() == FsyncPolicy::Always)
}

fn key_bytes(f: &Frame) -> Option<&[u8]> {
    match f {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b),
        _ => None,
    }
}

fn push_owner(out: &mut Targets, key: &[u8], my_shard: usize, num_shards: usize) {
    let owner = key_to_shard(key, num_shards);
    if owner != my_shard && !out.contains(&owner) {
        out.push(owner);
    }
}

/// The remote shards a coordinated multi-key command WRITES (the
/// `coordinate_multi_key` family): every key of `MSET`/`MSETNX`/`DEL`/
/// `UNLINK`, the destination of `BITOP`/`COPY`. Reads (`MGET`, `EXISTS`,
/// `TOUCH`) write nothing to the AOF.
pub(crate) fn written_remote_shards(
    cmd: &[u8],
    args: &[Frame],
    my_shard: usize,
    num_shards: usize,
) -> Targets {
    let mut out = Targets::new();
    let mut add = |f: &Frame| {
        if let Some(k) = key_bytes(f) {
            push_owner(&mut out, k, my_shard, num_shards);
        }
    };
    if cmd.eq_ignore_ascii_case(b"MSET") || cmd.eq_ignore_ascii_case(b"MSETNX") {
        args.iter().step_by(2).for_each(&mut add);
    } else if cmd.eq_ignore_ascii_case(b"DEL") || cmd.eq_ignore_ascii_case(b"UNLINK") {
        args.iter().for_each(&mut add);
    } else if cmd.eq_ignore_ascii_case(b"BITOP") || cmd.eq_ignore_ascii_case(b"COPY") {
        // BITOP <op> <dest> <src>...; COPY <src> <dst> [...]: index 1 both.
        if let Some(dest) = args.get(1) {
            add(dest);
        }
    }
    out
}

/// One barrier per target — all SENT before any is awaited, then awaited
/// together under one deadline; every one is awaited even after a failure
/// (each shard's durability is confirmed or reported), the first failure is
/// returned.
pub(crate) async fn barrier_targets(pool: &AofWriterPool, targets: &[usize]) -> Result<(), AofAck> {
    let mut set = PendingBarriers::new();
    for &t in targets {
        set.begin(pool, t);
    }
    set.wait(pool).await
}

/// `coordinate_multi_key`'s tail: confirm the remote legs of a write before
/// `reply` leaves — and the local leg with them when it owes a barrier
/// (`local_barrier_pending`, then cleared: the handler need not barrier this
/// reply again). A failed barrier replaces a successful reply (the write
/// stands in memory; its durability is unconfirmed), never an error reply.
pub(crate) async fn confirm_multi_key(
    pool: Option<&Arc<AofWriterPool>>,
    cmd: &[u8],
    args: &[Frame],
    my_shard: usize,
    num_shards: usize,
    local_barrier_pending: &mut bool,
    reply: Frame,
) -> Frame {
    let Some(p) = pool.filter(|_| barrier_needed(pool, num_shards)) else {
        return reply;
    };
    let mut targets = written_remote_shards(cmd, args, my_shard, num_shards);
    if std::mem::take(local_barrier_pending) {
        targets.push(my_shard);
    }
    match barrier_targets(p, &targets).await {
        Err(ack) if !matches!(reply, Frame::Error(_)) => {
            crate::persistence::aof::barrier_refusal_frame(ack)
        }
        _ => reply,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn bulk(s: &str) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    /// Keys whose owner (at 4 shards) is NOT shard `me`, one per remote owner.
    fn remote_keys(me: usize) -> Vec<(String, usize)> {
        let mut out: Vec<(String, usize)> = Vec::new();
        for i in 0..1000 {
            let k = format!("k{i}");
            let o = key_to_shard(k.as_bytes(), 4);
            if o != me && !out.iter().any(|(_, s)| *s == o) {
                out.push((k, o));
            }
        }
        out
    }

    #[test]
    fn writes_name_their_remote_owners_and_reads_name_none() {
        let me = 0;
        let rk = remote_keys(me);
        assert_eq!(rk.len(), 3);
        let local = (0..1000)
            .map(|i| format!("l{i}"))
            .find(|k| key_to_shard(k.as_bytes(), 4) == me)
            .expect("a local key");
        // MSET: keys at even positions only (a value that hashes remote is
        // not a key).
        let mset = [bulk(&rk[0].0), bulk(&rk[1].0), bulk(&local), bulk(&rk[2].0)];
        let t = written_remote_shards(b"MSET", &mset, me, 4);
        assert_eq!(t.as_slice(), &[rk[0].1]);
        // DEL: every key, deduplicated, never the local shard.
        let del = [bulk(&rk[0].0), bulk(&local), bulk(&rk[1].0), bulk(&rk[0].0)];
        let t = written_remote_shards(b"del", &del, me, 4);
        assert_eq!(t.as_slice(), &[rk[0].1, rk[1].1]);
        // BITOP AND dest src..: only the destination is written.
        let bitop = [bulk("AND"), bulk(&rk[2].0), bulk(&rk[0].0), bulk(&rk[1].0)];
        assert_eq!(
            written_remote_shards(b"BITOP", &bitop, me, 4).as_slice(),
            &[rk[2].1]
        );
        // COPY src dst: only the destination.
        let copy = [bulk(&rk[0].0), bulk(&rk[1].0)];
        assert_eq!(
            written_remote_shards(b"COPY", &copy, me, 4).as_slice(),
            &[rk[1].1]
        );
        // Reads write nothing.
        for cmd in [&b"MGET"[..], b"EXISTS", b"TOUCH"] {
            assert!(written_remote_shards(cmd, &del, me, 4).is_empty());
        }
    }

    #[test]
    fn no_barrier_without_a_pool_or_a_second_shard() {
        assert!(!barrier_needed(None, 4));
        let (tx, _rx) = crate::runtime::channel::mpsc_bounded(8);
        let pool = AofWriterPool::top_level(tx);
        // Everysec, nothing held: nothing owed.
        assert!(!barrier_needed(Some(&pool), 4));
        assert!(!barrier_needed(Some(&pool), 1));
    }
}
