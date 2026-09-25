//! The post-write index and queue hooks every shard-side write path owes
//! (moon#1162).
//!
//! A write that changes a hash, removes a key or empties a database has two
//! halves: the keyspace mutation `command::dispatch` performs, and the
//! bookkeeping in the per-shard stores that mirror the keyspace — the vector
//! and text indexes (`FT.*`) and the durable MQ registry. `dispatch` only sees
//! a `Database`, so every caller that owns a [`ShardSlice`] has to run the
//! second half itself.
//!
//! Before this module each dispatch arm carried its own copy of that list, and
//! the copies drifted: the `MultiExecute` arm — the one the coordinator's
//! spanning `DEL`/`UNLINK` and every `FLUSHALL`/`FLUSHDB` broadcast land on —
//! ran none of the hooks, so deleted documents kept matching `FT.SEARCH` and a
//! `FLUSHALL` cleared the index contents of one shard in N.
//!
//! [`run_post_write_hooks`] is the one list for the shard-side paths:
//! - the SPSC `Execute`, `MultiExecute` and `PipelineBatchSlotted` arms
//!   (`spsc_handler.rs`);
//! - every coordinator local leg (`coordinator::run_local`).
//!
//! Four paths still carry their own copy of the list, and a new hook must be
//! added to each of them by hand until they are routed through this function
//! (a follow-up):
//! - the monoio connection's local write path
//!   (`server/conn/handler_monoio/mod.rs`, `handle_connection_sharded_monoio`);
//! - the tokio connection's local write path
//!   (`server/conn/handler_sharded/mod.rs`, `handle_connection_sharded_inner`);
//! - the MULTI/EXEC executor (`server/conn/shared.rs`,
//!   `execute_transaction_sharded`);
//! - replica apply (`replication/apply.rs`, `apply_index_parity_hooks`).
//!
//! The hooks, and why each exists:
//! - `HSET` → [`auto_index_hset_public`]: index the hash's vector/text fields.
//! - `DEL`/`UNLINK` → [`auto_delete_vectors`] (tombstone the documents, and
//!   record the delete in the recovery ledger text-only indexes read) and
//!   [`auto_drop_mq_streams`] (tombstone durable queues so WAL replay does not
//!   resurrect them, task #46).
//! - `HDEL` → [`auto_hdel_vectors`]: a removed vector field tombstones it.
//! - `FLUSHDB`/`FLUSHALL` → [`auto_flush_indexes`] (index contents; the
//!   `FT.CREATE` definitions survive) and [`auto_drop_mq_streams_on_flush`].
//!
//! The keyspace half of `FLUSHALL` (every other database of the shard) is NOT
//! here: it needs the database guards, and each arm already runs it once its
//! own guard is released.
//!
//! [`auto_index_hset_public`]: crate::shard::spsc_handler::auto_index_hset_public
//! [`auto_delete_vectors`]: crate::shard::spsc_handler::auto_delete_vectors
//! [`auto_hdel_vectors`]: crate::shard::spsc_handler::auto_hdel_vectors
//! [`auto_flush_indexes`]: crate::shard::spsc_handler::auto_flush_indexes
//! [`auto_drop_mq_streams`]: crate::shard::mq_exec::auto_drop_mq_streams
//! [`auto_drop_mq_streams_on_flush`]: crate::shard::mq_exec::auto_drop_mq_streams_on_flush

use crate::protocol::Frame;
use crate::shard::slice::ShardSlice;

/// Which hook family a command belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HookKind {
    /// No index or queue bookkeeping — nearly every write.
    None,
    Hset,
    Delete,
    Hdel,
    FlushDb,
    FlushAll,
}

/// Classify `cmd`. Length first: an ordinary write (`SET`, `INCR`, `LPUSH`,
/// …) is rejected by one length compare or one name compare, where the arms
/// this replaced paid up to six case-insensitive compares per command.
#[inline]
pub(crate) fn hook_kind(cmd: &[u8]) -> HookKind {
    match cmd.len() {
        3 if cmd.eq_ignore_ascii_case(b"DEL") => HookKind::Delete,
        4 if cmd.eq_ignore_ascii_case(b"HSET") => HookKind::Hset,
        4 if cmd.eq_ignore_ascii_case(b"HDEL") => HookKind::Hdel,
        6 if cmd.eq_ignore_ascii_case(b"UNLINK") => HookKind::Delete,
        7 if cmd.eq_ignore_ascii_case(b"FLUSHDB") => HookKind::FlushDb,
        8 if cmd.eq_ignore_ascii_case(b"FLUSHALL") => HookKind::FlushAll,
        _ => HookKind::None,
    }
}

/// Whether `reply` proves a `DEL`/`UNLINK` removed nothing, so there is
/// nothing to log or replicate (redis propagates a delete only when it
/// deleted something). The coordinator's merged per-owner `DEL k1 k2 …`
/// legs (moon#1184) would otherwise log one no-op record per owner shard.
#[inline]
pub(crate) fn deleted_nothing(cmd: &[u8], reply: &Frame) -> bool {
    matches!(reply, Frame::Integer(0)) && hook_kind(cmd) == HookKind::Delete
}

/// Run the index and queue hooks `cmd args..` owes after it executed against
/// database `db_index` of this shard and answered `reply`.
///
/// Nothing runs for an error reply: the command wrote nothing. `reply` must be
/// the reply `dispatch` produced, not one a caller later replaced (an AOF
/// append failure turns the client's reply into an error, but the keyspace was
/// mutated, so the index must follow it).
///
/// The caller must not hold a guard on any of the shard's databases:
/// [`auto_drop_mq_streams`](crate::shard::mq_exec::auto_drop_mq_streams)
/// takes the whole slice.
#[inline]
pub(crate) fn run_post_write_hooks(
    s: &mut ShardSlice,
    cmd: &[u8],
    args: &[Frame],
    db_index: usize,
    reply: &Frame,
) {
    let kind = hook_kind(cmd);
    if kind == HookKind::None || matches!(reply, Frame::Error(_)) {
        return;
    }
    let db_tag = db_index as u8;
    match kind {
        HookKind::None => {}
        HookKind::Hset => {
            if let Some(Frame::BulkString(key)) = args.first() {
                // The `(index, key_hash)` list is for transactional callers
                // (Plan 166-02); these paths are not txn-aware.
                let _ = crate::shard::spsc_handler::auto_index_hset_public(
                    &mut s.vector_store,
                    &mut s.text_store,
                    key,
                    args,
                    db_tag,
                );
            }
        }
        HookKind::Delete => {
            crate::shard::spsc_handler::auto_delete_vectors(&mut s.vector_store, args, db_tag);
            crate::shard::mq_exec::auto_drop_mq_streams(s, args, db_index);
        }
        HookKind::Hdel => {
            crate::shard::spsc_handler::auto_hdel_vectors(&mut s.vector_store, args, db_tag);
        }
        HookKind::FlushDb | HookKind::FlushAll => {
            crate::shard::spsc_handler::auto_flush_indexes(
                &mut s.vector_store,
                &mut s.text_store,
                kind == HookKind::FlushDb,
                db_tag,
            );
            crate::shard::mq_exec::auto_drop_mq_streams_on_flush(s, db_index);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hook_kind_classifies_every_family_case_insensitively() {
        assert_eq!(hook_kind(b"DEL"), HookKind::Delete);
        assert_eq!(hook_kind(b"del"), HookKind::Delete);
        assert_eq!(hook_kind(b"UnLink"), HookKind::Delete);
        assert_eq!(hook_kind(b"hset"), HookKind::Hset);
        assert_eq!(hook_kind(b"HDEL"), HookKind::Hdel);
        assert_eq!(hook_kind(b"flushdb"), HookKind::FlushDb);
        assert_eq!(hook_kind(b"FLUSHALL"), HookKind::FlushAll);
    }

    #[test]
    fn deleted_nothing_only_for_a_zero_delete() {
        assert!(deleted_nothing(b"DEL", &Frame::Integer(0)));
        assert!(deleted_nothing(b"unlink", &Frame::Integer(0)));
        assert!(!deleted_nothing(b"DEL", &Frame::Integer(2)));
        assert!(!deleted_nothing(b"HDEL", &Frame::Integer(0)));
        assert!(!deleted_nothing(b"SREM", &Frame::Integer(0)));
    }

    #[test]
    fn hook_kind_ignores_neighbours_sharing_a_length() {
        // Same lengths as a hooked name, different command.
        for cmd in [
            &b"SET"[..],
            b"GET",
            b"TTL",
            b"HGET",
            b"INCR",
            b"MSET",
            b"HMSET",
            b"SUNION",
            b"HSETNX",
            b"RPUSHX",
            b"HINCRBY",
            b"FLUSHALLX",
            b"",
        ] {
            assert_eq!(hook_kind(cmd), HookKind::None, "{cmd:?}");
        }
    }
}
