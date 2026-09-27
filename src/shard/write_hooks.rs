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
//! - `RESTORE` → [`reindex_key_from_keyspace`] (moon#1285): the command
//!   replaces a whole value, so the key's documents are rebuilt from what it
//!   holds now. It is also the replica/replay half of `TXN.ABORT`, whose
//!   compensating record for a restored key is a `RESTORE … REPLACE`.
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
    /// `RESTORE` (moon#1285): rebuild the key's documents from its value.
    Restore,
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
        7 if cmd.eq_ignore_ascii_case(b"RESTORE") => HookKind::Restore,
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
        HookKind::Restore => {
            if let Some(Frame::BulkString(key)) = args.first() {
                let guard = s.databases.read(db_index);
                reindex_key_from_keyspace(
                    &mut s.vector_store,
                    &mut s.text_store,
                    &guard,
                    key,
                    db_index,
                );
            }
        }
    }
}

/// Rebuild `key`'s vector and text documents in database `db_index` from the
/// value the key holds NOW in `db` (moon#1285).
///
/// For a writer that replaced the key's value wholesale outside `HSET`:
/// `RESTORE` (live, replicated or replayed) and `TXN.ABORT`'s restore.
///
/// - A live hash is re-indexed exactly as an `HSET` of all its (unexpired)
///   fields: the HSET path tombstones the key's previous vector MVCC-style,
///   at the new insert's LSN, so `FT.SEARCH … AS_OF` a point before this
///   write still sees the previous version (a `DEL`-style tombstone would
///   erase it from every snapshot). An index whose vector field the hash
///   does not carry drops the key, as an `HDEL` of that field would.
/// - A key that is absent, expired or not a hash loses its documents, as
///   after a `DEL`.
///
/// Free when no index exists on the shard (two length loads); otherwise one
/// prefix lookup per store before anything is built.
pub(crate) fn reindex_key_from_keyspace(
    vector_store: &mut crate::vector::store::VectorStore,
    text_store: &mut crate::text::store::TextStore,
    db: &crate::storage::Database,
    key: &[u8],
    db_index: usize,
) {
    if vector_store.is_empty() && text_store.index_count() == 0 {
        return;
    }
    let db_tag = db_index as u8;
    let vector_indexes = vector_store.find_matching_index_names_for_db(key, db_tag);
    if vector_indexes.is_empty()
        && text_store
            .find_matching_index_names_for_db(key, db_tag)
            .is_empty()
    {
        return;
    }
    let Some(args) = hash_as_hset_args(db, key) else {
        vector_store.mark_deleted_for_key_for_db(key, db_tag);
        return;
    };
    for name in &vector_indexes {
        let carries_vector = vector_store.get_index(name).is_some_and(|idx| {
            idx.meta.vector_fields.first().is_some_and(|f| {
                crate::shard::spsc_handler::find_vector_blob(
                    &args,
                    &f.field_name,
                    f.dimension as usize,
                )
                .is_some()
            })
        });
        if !carries_vector {
            vector_store.mark_deleted_for_key_in_index(name, key);
        }
    }
    let _ = crate::shard::spsc_handler::auto_index_hset_public(
        vector_store,
        text_store,
        key,
        &args,
        db_tag,
    );
}

/// `key f1 v1 f2 v2 …` for a live hash — the argument shape the `HSET`
/// auto-indexer reads — or `None` when `key` holds no live hash. Fields whose
/// own TTL has passed are left out.
fn hash_as_hset_args(db: &crate::storage::Database, key: &[u8]) -> Option<Vec<Frame>> {
    use crate::storage::compact_value::RedisValueRef;
    let now_ms = db.now_ms();
    let entry = db.peek_if_alive(key, now_ms)?;
    let bulk = |b: &[u8]| Frame::BulkString(bytes::Bytes::copy_from_slice(b));
    let mut args = vec![bulk(key)];
    match entry.value.as_redis_value() {
        RedisValueRef::Hash(map) => {
            args.reserve(map.len() * 2);
            for (f, v) in map.iter() {
                args.push(Frame::BulkString(f.clone()));
                args.push(Frame::BulkString(v.clone()));
            }
        }
        RedisValueRef::HashWithTtl { fields, ttls, .. } => {
            args.reserve(fields.len() * 2);
            for (f, v) in fields.iter() {
                if ttls.get(f).is_some_and(|&deadline| now_ms >= deadline) {
                    continue;
                }
                args.push(Frame::BulkString(f.clone()));
                args.push(Frame::BulkString(v.clone()));
            }
        }
        RedisValueRef::HashListpack(lp) => {
            for (f, v) in lp.iter_pairs() {
                args.push(bulk(&f.as_bytes()));
                args.push(bulk(&v.as_bytes()));
            }
        }
        _ => return None,
    }
    Some(args)
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
        assert_eq!(hook_kind(b"restore"), HookKind::Restore);
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
            b"LINSERT",
            b"RESTORES",
            b"FLUSHALLX",
            b"",
        ] {
            assert_eq!(hook_kind(cmd), HookKind::None, "{cmd:?}");
        }
    }
}
