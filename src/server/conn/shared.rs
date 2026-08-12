// Both runtimes need this now: `execute_transaction_sharded` carries the
// WATCH token map, and that path is the one the monoio build ships.
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use bytes::Bytes;
#[cfg(feature = "runtime-tokio")]
use bytes::BytesMut;

use crate::command::config as config_cmd;
#[cfg(feature = "runtime-tokio")]
use crate::command::metadata;
use crate::command::{DispatchResult, dispatch};
use crate::config::{RuntimeConfig, ServerConfig};
use crate::protocol::Frame;
use crate::shard::shared_databases::ShardDatabases;
#[cfg(feature = "runtime-tokio")]
use crate::storage::Database;
use crate::storage::entry::CachedClock;
use crate::transaction::CrossStoreTxn;

use super::util::extract_command;

/// Type alias for the per-database RwLock container (tokio single-thread mode only).
#[cfg(feature = "runtime-tokio")]
pub(crate) type SharedDatabases = Arc<Vec<parking_lot::RwLock<Database>>>;

/// Resolve FT.SEARCH `as_of_lsn` with the canonical precedence (TEMP-04, ACID-09):
///
///   1. Explicit `AS_OF <wall_ms>` clause -> `TemporalRegistry::lsn_at(wall_ms)`.
///   2. Active cross-store TXN            -> `txn.snapshot_lsn`.
///   3. Default                           -> `0` (latest; visibility is
///      `created_lsn <= snapshot_lsn`).
///
/// The helper is called identically by all three connection handlers
/// (`handler_monoio.rs`, `handler_sharded.rs`, `handler_single.rs`).
/// `shard_databases` is `Option<&ShardDatabases>` because the single-shard
/// tokio handler has no `ShardDatabases` in scope at the FT.SEARCH call site;
/// it passes `None`.
///
/// Returns `Err(Frame::Error)` with the exact bytes
/// `b"ERR no temporal snapshot registered for the given AS_OF timestamp; call TEMPORAL.SNAPSHOT_AT first"`
/// when `AS_OF` is present AND either:
///   (a) `shard_databases` is `None` (no registry available to consult), OR
///   (b) the registry has no binding at or before the requested `wall_ms`.
///
/// No allocations on any path: the error message is `Bytes::from_static`.
#[inline]
pub(crate) fn resolve_ft_search_as_of_lsn(
    cmd_args: &[Frame],
    shard_databases: Option<&ShardDatabases>,
    active_cross_txn: Option<&CrossStoreTxn>,
) -> Result<u64, Frame> {
    use crate::command::vector_search::ft_search::parse::parse_as_of_clause;
    const ERR_MSG: &[u8] =
        b"ERR no temporal snapshot registered for the given AS_OF timestamp; call TEMPORAL.SNAPSHOT_AT first";
    if let Some(wall_ms) = parse_as_of_clause(cmd_args) {
        // `shard_databases` is `None` for the single-shard tokio handler, which
        // has no ShardDatabases in scope. Presence/absence is the guard; the
        // actual registry is accessed through the thread-local ShardSlice below.
        if shard_databases.is_none() {
            return Err(Frame::Error(Bytes::from_static(ERR_MSG)));
        }
        // Read temporal registry via thread-local slice (no lock needed on shard path).
        return crate::shard::slice::with_shard(|s| {
            s.temporal_registry.as_ref().and_then(|r| r.lsn_at(wall_ms))
        })
        .ok_or_else(|| Frame::Error(Bytes::from_static(ERR_MSG)));
    }
    Ok(active_cross_txn.map(|t| t.snapshot_lsn).unwrap_or(0))
}

/// Handle CONFIG GET/SET subcommands.
pub(crate) fn handle_config(
    args: &[Frame],
    runtime_config: &Arc<RwLock<RuntimeConfig>>,
    server_config: &Arc<ServerConfig>,
) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'config' command",
        ));
    }

    let subcmd = match &args[0] {
        Frame::BulkString(s) => s.as_ref(),
        Frame::SimpleString(s) => s.as_ref(),
        _ => {
            return Frame::Error(Bytes::from_static(b"ERR unknown subcommand for CONFIG"));
        }
    };

    let sub_args = &args[1..];

    if subcmd.eq_ignore_ascii_case(b"GET") {
        let rt = runtime_config.read();
        config_cmd::config_get(&rt, server_config, sub_args)
    } else if subcmd.eq_ignore_ascii_case(b"SET") {
        let mut rt = runtime_config.write();
        config_cmd::config_set(&mut rt, sub_args)
    } else if subcmd.eq_ignore_ascii_case(b"REWRITE") {
        let rt = runtime_config.read();
        config_cmd::config_rewrite(&rt, server_config)
    } else if subcmd.eq_ignore_ascii_case(b"RESETSTAT") {
        config_cmd::config_resetstat()
    } else {
        Frame::Error(Bytes::from(format!(
            "ERR unknown subcommand '{}'. Try CONFIG GET, CONFIG SET, CONFIG REWRITE, CONFIG RESETSTAT.",
            String::from_utf8_lossy(subcmd)
        )))
    }
}

/// What `WATCH k` recorded about `k`, and what `EXEC` re-checks.
///
/// `version` is `Database::get_version`, which is `0` exactly when the key is
/// absent (creation tickets start at 1 and the counter never yields 0), so
/// absent-vs-present needs no separate flag.
///
/// A struct rather than a bare `u32`: the ABA hole (delete + recreate handing
/// back the token WATCH recorded) is closed inside this one field by the per-db
/// creation ticket — see `Database::birth_counter` — but that ticket shares the
/// entry's 24 version bits and so still wraps at 16.7M creations. A true
/// incarnation field is the only way to retire the residue, and it would live
/// here; keeping the named type means adding it never churns the call sites.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WatchToken {
    pub version: u32,
}

/// True when any watched key's version no longer matches what WATCH recorded.
///
/// Caller must have checked `!watched.is_empty()` — this takes the shard slice,
/// which is not free, and the empty case is the overwhelming majority.
fn watch_conflict(db_index: usize, watched: &HashMap<Bytes, WatchToken>) -> bool {
    crate::shard::slice::with_shard_db(db_index, |db| {
        watched
            .iter()
            .any(|(key, tok)| db.get_version(key) != tok.version)
    })
}

/// Execute a queued transaction atomically under a single database lock.
///
/// Checks WATCH versions first -- if any watched key's version has changed since
/// the snapshot was taken, the transaction is aborted and Frame::Null is returned.
///
/// Returns the result Frame (Array of responses, or Null on abort) and a Vec of
/// AOF byte entries for write commands that succeeded (caller sends them async).
#[cfg(feature = "runtime-tokio")]
pub(crate) fn execute_transaction(
    db: &SharedDatabases,
    command_queue: &[Frame],
    watched_keys: &HashMap<Bytes, WatchToken>,
    selected_db: &mut usize,
    exec_publishes: &mut Vec<(usize, Bytes, Bytes)>,
) -> (Frame, Vec<Bytes>) {
    let mut guard = db[*selected_db].write();
    let db_count = db.len();
    guard.refresh_now();

    // Check WATCH versions -- if any key's version changed, abort
    for (key, watched_version) in watched_keys {
        let current_version = guard.get_version(key);
        if current_version != watched_version.version {
            return (Frame::Null, Vec::new()); // Transaction aborted
        }
    }

    // Execute all queued commands atomically (under the same lock)
    let mut results = Vec::with_capacity(command_queue.len());
    let mut aof_entries: Vec<Bytes> = Vec::new();

    for cmd_frame in command_queue {
        // Extract command name and args (zero-alloc)
        let (cmd, cmd_args) = match extract_command(cmd_frame) {
            Some(pair) => pair,
            None => {
                results.push(Frame::Error(Bytes::from_static(
                    b"ERR invalid command format",
                )));
                continue;
            }
        };

        // C2: PUBLISH queued inside MULTI — defer fan-out to the caller so it
        // happens after the transaction body (see execute_transaction_sharded).
        if queue_exec_publish(cmd, cmd_args, &mut results, exec_publishes) {
            continue;
        }

        // Check if this is a write command for AOF logging.
        // `is_persisted_write` (PR #282 review): a SELECT queued inside MULTI
        // must not be persisted as a literal record — it would shift the AOF
        // stream's db context under every OTHER record (task #35). All body
        // writes execute against the guard taken on the ENTRY db above, so
        // the caller attributes every entry to that one db.
        let is_write = metadata::is_persisted_write(cmd);

        // Serialize for AOF before dispatch
        let aof_bytes = if is_write {
            let mut buf = BytesMut::new();
            crate::protocol::serialize::serialize(cmd_frame, &mut buf);
            Some(buf.freeze())
        } else {
            None
        };

        let result = dispatch(&mut *guard, cmd, cmd_args, selected_db, db_count);
        let response = match result {
            DispatchResult::Response(f) => f,
            DispatchResult::Quit(f) => f, // QUIT inside MULTI just returns OK
        };

        // Collect AOF entry for successful writes (not error responses)
        if let Some(bytes) = aof_bytes {
            if !matches!(&response, Frame::Error(_)) {
                aof_entries.push(bytes);
            }
        }

        results.push(response);
    }

    (Frame::Array(results.into()), aof_entries)
}

/// Execute a queued transaction on the local shard (sharded path).
///
/// The caller must ensure every key in `command_queue` is owned by THIS shard
/// (see `analyze_txn_locality`) — the body runs on the local slice with no
/// per-key routing.
///
/// Returns the result Frame (an array of per-command responses), the
/// serialized AOF bytes for each successful write in order, and the
/// graph-leg wal-v3 records (db, bytes) produced by any `GRAPH.*` command in
/// the body. The caller MUST append the AOF entries to the shard's AOF
/// (previously this path did no persistence, so MULTI/EXEC writes were
/// silently lost on restart) and MUST both fan the graph records out to
/// replicas (single-shard, monoio only — see the live single-command
/// contract at `handler_monoio::write::try_handle_graph_command`) and
/// `wal_append` them (task #52 review round 2: appending them INSIDE this
/// function skipped the replication leg the live path always takes,
/// silently diverging a replica from a committed cross-store transaction —
/// this function has no `ConnectionContext`/replication access, so the
/// caller must do both, in the same synchronous stretch as the
/// already-synchronous call to this function, replicate-then-append, same
/// order as the live path).
pub(crate) fn execute_transaction_sharded(
    shard_databases: &std::sync::Arc<crate::shard::shared_databases::ShardDatabases>,
    _shard_id: usize,
    command_queue: &[Frame],
    selected_db: usize,
    // `proto`: the connection's protocol version. Each inner reply is
    // converted with ITS OWN command and args before joining `results` —
    // without this an EXEC'd HGETALL came back a flat Array while the same
    // command standalone came back a Map, i.e. the reply shape depended on
    // the calling context.
    proto: u8,
    cached_clock: &CachedClock,
    exec_publishes: &mut Vec<(usize, Bytes, Bytes)>,
    exec_flushes: &mut Vec<(usize, Frame, usize)>,
    // WATCH/CAS (task `watch-cas-transactions`): the versions this connection
    // recorded at WATCH time. Empty for the overwhelming majority of
    // transactions, which is why the check below early-outs on `is_empty`
    // before touching the shard at all.
    watched_keys: &HashMap<Bytes, WatchToken>,
) -> (Frame, Vec<(usize, Bytes)>, Vec<(usize, Vec<u8>)>) {
    let db_count = shard_databases.db_count();

    // The CAS gate. This runs synchronously on the shard thread, BEFORE the
    // first body command and with no `.await` between it and the body — that
    // adjacency is the whole guarantee. `execute_transaction` (the embedded
    // path) does the same thing at the top of its locked section; this path
    // had no equivalent, so a transaction that declared a dependency on a key
    // committed straight over a conflicting write.
    if !watched_keys.is_empty() && watch_conflict(selected_db, watched_keys) {
        return (Frame::Null, Vec::new(), Vec::new());
    }

    let mut results = Vec::with_capacity(command_queue.len());
    // Per-entry db (PR #282 review): this executor re-dispatches each body
    // command against the CURRENT `selected`, so a SELECT queued inside MULTI
    // really does redirect the commands after it — collapsing every entry to
    // one db mis-attributes recovery and replication whenever the body
    // switches dbs.
    let mut aof_entries: Vec<(usize, Bytes)> = Vec::new();
    let mut selected = selected_db;
    // task #52: graph-leg WAL records produced by GRAPH.* commands queued
    // inside this MULTI/EXEC body, bound to the db THIS command executed in
    // (same per-entry-db discipline as `aof_entries` — a queued SELECT
    // redirects the commands after it, never itself). The single-command
    // path (`try_handle_graph_command`) drains+appends these to wal-v3 (and
    // replicates them) per command; the txn executor previously had NO graph
    // branch at all, so a queued GRAPH.* command fell through to the generic
    // KV `dispatch()` table and errored as "unknown command" -- never
    // applied to the graph store, never durable, never replicated. Collected
    // here and returned for the CALLER to replicate + flush to wal-v3 (this
    // function has no replication access — see the doc comment above).
    #[cfg(feature = "graph")]
    let mut graph_records: Vec<(usize, Vec<u8>)> = Vec::new();
    #[cfg(not(feature = "graph"))]
    let graph_records: Vec<(usize, Vec<u8>)> = Vec::new();

    for cmd_frame in command_queue {
        let (cmd, cmd_args) = match extract_command(cmd_frame) {
            Some(pair) => pair,
            None => {
                results.push(Frame::Error(Bytes::from_static(
                    b"ERR invalid command format",
                )));
                continue;
            }
        };

        // C2: PUBLISH is a connection-plane command — the keyspace dispatch
        // table can't run it. Record it for the caller to fan out AFTER the
        // transaction body (all preceding writes applied first) and leave a
        // placeholder the caller patches with the receiver count.
        if queue_exec_publish(cmd, cmd_args, &mut results, exec_publishes) {
            continue;
        }

        // task #52: GRAPH.* commands live in a separate per-shard store
        // (`ShardSlice::graph_store`), not the KV `Database` this loop
        // otherwise dispatches against -- route them to the graph engine
        // exactly like the single-command path (`try_handle_graph_command`)
        // does, and stash the drained WAL records (bound to the db THIS
        // command executed in, captured before dispatch, same rule as
        // `entry_db` below) for the caller to replicate + flush.
        #[cfg(feature = "graph")]
        if cmd.len() > 6 && cmd[..6].eq_ignore_ascii_case(b"GRAPH.") {
            let entry_db = selected;
            let is_write = crate::command::graph::is_graph_write_cmd(cmd)
                || (cmd.eq_ignore_ascii_case(b"GRAPH.QUERY")
                    && crate::command::graph::is_cypher_write_query(cmd_args));
            let (response, records) = crate::shard::slice::with_shard(|s| {
                if is_write {
                    let resp = if cmd.eq_ignore_ascii_case(b"GRAPH.QUERY") {
                        crate::command::graph::graph_query_or_write(&mut s.graph_store, cmd_args).0
                    } else {
                        crate::command::graph::dispatch_graph_write(
                            &mut s.graph_store,
                            cmd,
                            cmd_args,
                        )
                    };
                    let records = s.graph_store.drain_wal();
                    (resp, records)
                } else {
                    let resp = crate::command::graph::dispatch_graph_read(
                        &s.graph_store,
                        cmd,
                        cmd_args,
                        None,
                    );
                    (resp, Vec::new())
                }
            });
            // Parity with the KV branch below: an errored write must not
            // reach the WAL.
            if !records.is_empty() && !matches!(&response, Frame::Error(_)) {
                graph_records.extend(records.into_iter().map(|r| (entry_db, r)));
            }
            results.push(super::util::apply_resp3_conversion(
                cmd, cmd_args, response, proto,
            ));
            continue;
        }

        // Serialize write commands for AOF *before* dispatch (matches the
        // single-shard `execute_transaction` path). Without this the sharded
        // MULTI/EXEC path logged nothing, so every transactional write was
        // silently lost on restart. Fully-qualified paths because `metadata`
        // is only `use`d under runtime-tokio but this fn compiles under both.
        // `is_persisted_write` (PR #282 review): a queued literal SELECT is
        // connection/txn state only — persisting it shifts the stream's db
        // context under other records (task #35).
        let aof_bytes = if crate::command::metadata::is_persisted_write(cmd) {
            let mut buf = bytes::BytesMut::new();
            crate::protocol::serialize::serialize(cmd_frame, &mut buf);
            Some(buf.freeze())
        } else {
            None
        };
        // The db THIS command executes in — captured before dispatch (a
        // queued SELECT mutates `selected` for the commands after it, never
        // for itself, and no persisted write mutates it mid-dispatch).
        let entry_db = selected;

        let result = crate::shard::slice::with_shard_db(selected, |db| {
            db.refresh_now_from_cache(cached_clock);
            dispatch(db, cmd, cmd_args, &mut selected, db_count)
        });
        let response = match result {
            DispatchResult::Response(f) => f,
            DispatchResult::Quit(f) => f,
        };

        // Only log the write if it actually succeeded (parity with the
        // single-shard path — an errored write must not reach the AOF).
        if let Some(bytes) = aof_bytes {
            if !matches!(&response, Frame::Error(_)) {
                aof_entries.push((entry_db, bytes));
            }
        }

        // Auto-index: if HSET succeeded, check for vector index match
        if cmd.eq_ignore_ascii_case(b"HSET") && !matches!(response, Frame::Error(_)) {
            if let Some(Frame::BulkString(key_bytes)) = cmd_args.first() {
                crate::shard::slice::with_shard(|s| {
                    // Plan 166-01 return value is not consumed here — this is a
                    // non-txn-aware batch write path. Plan 166-02 wires txn paths.
                    let _ = crate::shard::spsc_handler::auto_index_hset_public(
                        &mut s.vector_store,
                        &mut s.text_store,
                        key_bytes,
                        cmd_args,
                        selected as u8,
                    );
                });
            }
        }

        // Auto-delete vectors on DEL/UNLINK (parity with the HSET hook above).
        if !matches!(response, Frame::Error(_))
            && (cmd.eq_ignore_ascii_case(b"DEL") || cmd.eq_ignore_ascii_case(b"UNLINK"))
        {
            crate::shard::slice::with_shard(|s| {
                crate::shard::spsc_handler::auto_delete_vectors(
                    &mut s.vector_store,
                    cmd_args,
                    selected as u8,
                );
                // task #46: tombstone any durable MQ stream(s) this generic
                // DEL/UNLINK removed, so `replay_mq_wal` doesn't resurrect
                // them.
                crate::shard::mq_exec::auto_drop_mq_streams(s, cmd_args, selected);
            });
        }

        // R4: HDEL of an indexed vector field tombstones it.
        if !matches!(response, Frame::Error(_)) && cmd.eq_ignore_ascii_case(b"HDEL") {
            crate::shard::slice::with_shard(|s| {
                crate::shard::spsc_handler::auto_hdel_vectors(
                    &mut s.vector_store,
                    cmd_args,
                    selected as u8,
                );
            });
        }

        // R3: FLUSHALL/FLUSHDB clears index contents (definitions survive).
        // WS5a: FLUSHDB scopes to `selected`; FLUSHALL clears every db.
        if !matches!(response, Frame::Error(_))
            && (cmd.eq_ignore_ascii_case(b"FLUSHDB") || cmd.eq_ignore_ascii_case(b"FLUSHALL"))
        {
            // c10k E2: this loop clears only the LOCAL slice. The live
            // (non-MULTI) path fixed the same bug with
            // `coordinate_flush_broadcast`; a queued flush needs the identical
            // fan-out or `EXEC` answers +OK having emptied one shard of N.
            // Recorded rather than performed here for two reasons: this
            // function is synchronous (the broadcast awaits), and it runs on
            // the OWNER shard for a routed transaction, where fanning out from
            // inside the shard's own message loop risks a shard-to-shard wait
            // cycle. Same deferral contract as `exec_publishes` directly
            // above: `(result_index, command, db)`, patched by the originator.
            // `selected` is the per-entry db, so a queued SELECT before the
            // flush is honoured.
            exec_flushes.push((results.len(), cmd_frame.clone(), selected));
            crate::shard::slice::with_shard(|s| {
                crate::shard::spsc_handler::auto_flush_indexes(
                    &mut s.vector_store,
                    &mut s.text_store,
                    cmd.eq_ignore_ascii_case(b"FLUSHDB"),
                    selected as u8,
                );
                // task #46: tombstone every durable MQ stream this
                // FLUSHDB/FLUSHALL cleared.
                crate::shard::mq_exec::auto_drop_mq_streams_on_flush(s, selected);
            });
        }

        results.push(super::util::apply_resp3_conversion(
            cmd, cmd_args, response, proto,
        ));
    }

    (Frame::Array(results.into()), aof_entries, graph_records)
}

/// Persist the AOF entries of a just-executed sharded MULTI/EXEC body.
///
/// Mirrors the normal write path's group-commit: each entry is enqueued
/// fire-and-forget on the owning shard's writer (`send_append_group`), and a
/// single `fsync_barrier` is issued at the end under `appendfsync=always`
/// (`send_append_group` returns `Ok(true)` when a barrier is owed). All keys in
/// the body are owned by `ctx.shard_id` (Phase A rejects foreign-owned bodies),
/// so that is the correct AOF target.
///
/// Returns `Err(())` if any append or the barrier fails — the caller surfaces
/// `AOF_FSYNC_ERR` instead of acking a durability it can't guarantee. A no-op
/// (returns `Ok`) when AOF is disabled (`aof_pool` is `None`) or the body wrote
/// nothing.
///
/// `repl_recorded`: the caller already recorded every entry in the
/// replication plane (`record_local_write`, monoio local-leg fanout), which
/// advanced the shard offset — this AOF leg must then NOT advance it again
/// (lsn = 0; per-shard order is append order, same contract as the
/// single-command write legs). The tokio handler passes `false` (tokio-side
/// master fanout is not wired; monoio is the production replication runtime).
pub(crate) async fn persist_txn_aof(
    ctx: &crate::server::conn::core::ConnectionContext,
    // task #35 + PR #282 review: each entry carries the db THAT command
    // executed in — a SELECT queued inside MULTI redirects the commands
    // after it (sharded executor), so one collapsed db mis-attributes the
    // body on recovery.
    aof_entries: Vec<(usize, Bytes)>,
    repl_recorded: bool,
) -> Result<(), ()> {
    if aof_entries.is_empty() {
        return Ok(());
    }
    let Some(ref pool) = ctx.aof_pool else {
        return Ok(());
    };
    let mut barrier_pending = false;
    for (db, bytes) in aof_entries {
        let lsn = if repl_recorded {
            0
        } else {
            crate::persistence::aof::AofWriterPool::issue_append_lsn(
                &ctx.repl_state,
                ctx.shard_id,
                bytes.len(),
            )
        };
        match pool.send_append_group(ctx.shard_id, lsn, db, bytes).await {
            Ok(true) => barrier_pending = true,
            Ok(false) => {}
            Err(_) => return Err(()),
        }
    }
    // appendfsync=always: one barrier confirms the whole body is on disk.
    if barrier_pending && pool.fsync_barrier(ctx.shard_id).await.is_err() {
        return Err(());
    }
    Ok(())
}

/// Shared PUBLISH-inside-MULTI intercept for both transaction executors (C2).
///
/// Returns `true` when `cmd` is PUBLISH: pushes a `Frame::Integer(0)`
/// placeholder (or an arity error) into `results` and records
/// `(result_index, channel, message)` in `exec_publishes` so the caller can
/// fan the message out AFTER the transaction body and patch the placeholder
/// with the real receiver count.
fn queue_exec_publish(
    cmd: &[u8],
    cmd_args: &[Frame],
    results: &mut Vec<Frame>,
    exec_publishes: &mut Vec<(usize, Bytes, Bytes)>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"PUBLISH") {
        return false;
    }
    if cmd_args.len() != 2 {
        results.push(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'publish' command",
        )));
        return true;
    }
    match (
        super::util::extract_bytes(&cmd_args[0]),
        super::util::extract_bytes(&cmd_args[1]),
    ) {
        (Some(ch), Some(msg)) => {
            exec_publishes.push((results.len(), ch, msg));
            results.push(Frame::Integer(0)); // patched by the caller post-txn
        }
        _ => results.push(Frame::Error(Bytes::from_static(
            b"ERR invalid channel or message",
        ))),
    }
    true
}

/// Channel-ACL gate for PUBLISH. Returns the `NOPERM` error frame when `user`
/// lacks permission on `channel` (caller must skip the fan-out and patch the
/// reply with it), or `None` when allowed.
///
/// Used by both the immediate single-handler PUBLISH and the transactional
/// (MULTI/EXEC) fan-out in all three handlers, so a client denied a channel
/// cannot wrap `PUBLISH` in `MULTI/EXEC` to bypass the check. Moon has no
/// queue-time EXECABORT machinery, so the transactional check runs at fan-out
/// time rather than at queue time.
pub(crate) fn publish_channel_acl_deny(
    acl_table: &std::sync::RwLock<crate::acl::AclTable>,
    user: &str,
    channel: &[u8],
) -> Option<Frame> {
    #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
    let guard = acl_table.read().unwrap();
    guard
        .check_channel_permission(user, channel)
        .map(|reason| Frame::Error(Bytes::from(format!("NOPERM {reason}"))))
}

/// Command-level ACL gate for the pub/sub intercepts (H-3). PUBLISH/SUBSCRIBE/
/// PSUBSCRIBE are handled BEFORE the generic ACL gate in every handler, so
/// without this the command-level `+`/`-`/`-@pubsub` rules were never
/// consulted — only the per-channel `&pattern` rule was — and a `-@pubsub`
/// carve-out was silently ineffective for a user with `&*`. Returns the
/// `NOPERM` error frame when the command itself is denied, else `None`.
///
/// Only the tokio single/sharded handlers call this helper; the monoio handler
/// inlines the same check in `pubsub.rs`. Gate it to the tokio runtime so the
/// default (monoio) build doesn't flag it as dead code.
#[cfg(feature = "runtime-tokio")]
pub(crate) fn pubsub_command_acl_deny(
    acl_table: &std::sync::RwLock<crate::acl::AclTable>,
    user: &str,
    cmd: &[u8],
    cmd_args: &[Frame],
) -> Option<Frame> {
    #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
    let guard = acl_table.read().unwrap();
    guard
        .check_command_permission(user, cmd, cmd_args)
        .map(|reason| Frame::Error(Bytes::from(format!("NOPERM {reason}"))))
}

/// Fan out one EXEC-queued PUBLISH (C2): local shard synchronously, remote
/// shards via targeted `PubSubPublish` SPSC messages, awaited so the returned
/// count matches the immediate-PUBLISH path. Called by the sharded handlers
/// after `execute_transaction_sharded` returns — i.e. after every write queued
/// before the PUBLISH has been applied.
pub(crate) async fn publish_post_txn(
    ctx: &super::core::ConnectionContext,
    shutdown: &crate::runtime::cancel::CancellationToken,
    channel: &Bytes,
    message: &Bytes,
) -> i64 {
    use crate::shard::mesh::ChannelMesh;
    use ringbuf::traits::Producer;

    let local_count = crate::pubsub::publish_shared(&ctx.pubsub_registry, channel, message);
    let remote_targets: Vec<usize> = ctx
        .remote_subscriber_map
        .read()
        .target_shards(channel)
        .into_iter()
        .filter(|&t| t != ctx.shard_id)
        .collect();
    if remote_targets.is_empty() {
        return local_count;
    }

    let slot = Arc::new(crate::shard::dispatch::PubSubResponseSlot::new(
        remote_targets.len() as u32,
    ));
    for target in &remote_targets {
        // E1: bounded backpressure retry instead of one bare `try_push` — a
        // transiently-full ring no longer loses the message. The borrow of
        // `dispatch_tx` is taken+released inside each attempt, never held
        // across the backoff await.
        let mut pending = Some(crate::shard::dispatch::ShardMessage::PubSubPublish(
            Box::new(crate::shard::dispatch::PubSubPublishPayload {
                channel: channel.clone(),
                message: message.clone(),
                slot: slot.clone(),
            }),
        ));
        let idx = ChannelMesh::target_index(ctx.shard_id, *target);
        let outcome = crate::shard::dispatch::push_with_backpressure(
            shutdown,
            crate::shard::dispatch::CROSS_SHARD_PUSH_MAX_RETRIES,
            crate::shard::dispatch::CROSS_SHARD_PUSH_BACKOFF,
            || match pending.take() {
                None => true,
                Some(m) => {
                    let mut producers = ctx.dispatch_tx.borrow_mut();
                    match producers[idx].try_push(m) {
                        Ok(()) => true,
                        Err(back) => {
                            pending = Some(back);
                            false
                        }
                    }
                }
            },
        )
        .await;
        match outcome {
            crate::shard::dispatch::PushOutcome::Pushed => {
                ctx.spsc_notifiers[*target].notify_one();
            }
            outcome => {
                // Give-up: count the target as delivered-to-zero so the EXEC
                // reply can't hang — but LOUDLY: this is real message loss to
                // that shard's subscribers (was a silent drop pre-E1).
                tracing::warn!(
                    "shard {}: EXEC PUBLISH fan-out to shard {target} dropped ({outcome:?})",
                    ctx.shard_id
                );
                crate::admin::metrics_setup::record_xshard_fanout_drop("publish");
                slot.add(0);
            }
        }
    }
    // E4: bounded await — a wedged target shard can't hang the EXEC reply
    // forever. The slot is per-call, so abandoning it on expiry is safe; the
    // count degrades to whatever responded in time (under-report, loud).
    if !crate::shard::dispatch::await_pubsub_slot_bounded(
        &slot,
        crate::shard::dispatch::XSHARD_REPLY_TIMEOUT,
    )
    .await
    {
        tracing::warn!(
            "shard {}: EXEC PUBLISH reply timed out awaiting remote shards",
            ctx.shard_id
        );
        crate::admin::metrics_setup::record_xshard_reply_timeout("publish");
    }
    local_count + slot.get()
}

/// Fan one `SCRIPT LOAD` out to every other shard with bounded backpressure
/// (E3). A full ring used to drop the load SILENTLY, leaving that shard's
/// script cache divergent: EVALSHA there answered NOSCRIPT for a sha this
/// server had just returned. On give-up the drop is loud (warn + counter);
/// the client still gets the sha — the script IS loaded locally, and client
/// libraries' NOSCRIPT→EVAL fallback covers the divergent-shard window.
pub(crate) async fn script_fanout_bounded(
    ctx: &super::core::ConnectionContext,
    shutdown: &crate::runtime::cancel::CancellationToken,
    sha1: &str,
    script: &Bytes,
) {
    use crate::shard::mesh::ChannelMesh;
    use ringbuf::traits::Producer;

    for target in 0..ctx.num_shards {
        if target == ctx.shard_id {
            continue;
        }
        let idx = ChannelMesh::target_index(ctx.shard_id, target);
        let mut pending = Some(crate::shard::dispatch::ShardMessage::ScriptLoad {
            sha1: sha1.to_owned(),
            script: script.clone(),
        });
        let outcome = crate::shard::dispatch::push_with_backpressure(
            shutdown,
            crate::shard::dispatch::CROSS_SHARD_PUSH_MAX_RETRIES,
            crate::shard::dispatch::CROSS_SHARD_PUSH_BACKOFF,
            || match pending.take() {
                None => true,
                Some(m) => {
                    let mut producers = ctx.dispatch_tx.borrow_mut();
                    match producers[idx].try_push(m) {
                        Ok(()) => true,
                        Err(back) => {
                            pending = Some(back);
                            false
                        }
                    }
                }
            },
        )
        .await;
        match outcome {
            crate::shard::dispatch::PushOutcome::Pushed => {
                ctx.spsc_notifiers[target].notify_one();
            }
            outcome => {
                tracing::warn!(
                    "shard {}: SCRIPT LOAD fan-out to shard {target} dropped ({outcome:?}); \
                     that shard's script cache is divergent until the next load",
                    ctx.shard_id
                );
                crate::admin::metrics_setup::record_xshard_fanout_drop("script_load");
            }
        }
    }
}

/// Extract the primary key from a parsed command for shard routing.
///
/// Returns `None` for keyless commands (PING, DBSIZE, SELECT, etc.)
/// which should execute locally on the connection's shard.
pub(crate) fn extract_primary_key<'a>(cmd: &[u8], args: &'a [Frame]) -> Option<&'a Bytes> {
    // Fast check: is this command keyless? Uses (length, first_byte) dispatch.
    let len = cmd.len();
    if len == 0 {
        return None;
    }
    let b0 = cmd[0] | 0x20;

    let is_keyless = match (len, b0) {
        (4, b'a') => cmd.eq_ignore_ascii_case(b"AUTH"),
        (4, b'e') => cmd.eq_ignore_ascii_case(b"ECHO") || cmd.eq_ignore_ascii_case(b"EXEC"),
        (4, b'i') => cmd.eq_ignore_ascii_case(b"INFO"),
        (4, b'k') => cmd.eq_ignore_ascii_case(b"KEYS"),
        (4, b'p') => cmd.eq_ignore_ascii_case(b"PING"),
        (4, b'q') => cmd.eq_ignore_ascii_case(b"QUIT"),
        (4, b's') => cmd.eq_ignore_ascii_case(b"SCAN") || cmd.eq_ignore_ascii_case(b"SAVE"),
        (4, b'w') => cmd.eq_ignore_ascii_case(b"WAIT"),
        (5, b'd') => cmd.eq_ignore_ascii_case(b"DEBUG"),
        (5, b'h') => cmd.eq_ignore_ascii_case(b"HELLO"),
        (5, b'm') => cmd.eq_ignore_ascii_case(b"MULTI"),
        (5, b'p') => cmd.eq_ignore_ascii_case(b"PSYNC"),
        (6, b'a') => cmd.eq_ignore_ascii_case(b"ASKING"),
        (6, b'b') => cmd.eq_ignore_ascii_case(b"BGSAVE"),
        (6, b'c') => cmd.eq_ignore_ascii_case(b"CLIENT") || cmd.eq_ignore_ascii_case(b"CONFIG"),
        (6, b'd') => cmd.eq_ignore_ascii_case(b"DBSIZE"),
        (6, b's') => cmd.eq_ignore_ascii_case(b"SELECT"),
        (7, b'c') => cmd.eq_ignore_ascii_case(b"COMMAND") || cmd.eq_ignore_ascii_case(b"CLUSTER"),
        (7, b'd') => cmd.eq_ignore_ascii_case(b"DISCARD"),
        (7, b'h') => cmd.eq_ignore_ascii_case(b"HOTKEYS"),
        (7, b'p') => cmd.eq_ignore_ascii_case(b"PUBLISH"),
        (7, b's') => cmd.eq_ignore_ascii_case(b"SLAVEOF"),
        (8, b'l') => cmd.eq_ignore_ascii_case(b"LASTSAVE"),
        (8, b'r') => cmd.eq_ignore_ascii_case(b"REPLCONF"),
        (9, b'r') => cmd.eq_ignore_ascii_case(b"REPLICAOF"),
        (9, b's') => cmd.eq_ignore_ascii_case(b"SUBSCRIBE"),
        (10, b'p') => cmd.eq_ignore_ascii_case(b"PSUBSCRIBE"),
        (11, b'u') => cmd.eq_ignore_ascii_case(b"UNSUBSCRIBE"),
        (12, b'p') => cmd.eq_ignore_ascii_case(b"PUNSUBSCRIBE"),
        (13, b'b') => cmd.eq_ignore_ascii_case(b"BGREWRITEAOF"),
        _ => false,
    };

    if is_keyless || args.is_empty() {
        return None;
    }
    // OBJECT <subcommand> <key>: the key is the 2nd argument (first_key=2
    // in metadata). Routing by args[0] would hash the subcommand name and
    // send the command to an arbitrary shard ("ERR no such key").
    // OBJECT HELP has no key and falls through to None (executes locally).
    if (len, b0) == (6, b'o') && cmd.eq_ignore_ascii_case(b"OBJECT") {
        return match args.get(1) {
            Some(Frame::BulkString(key)) => Some(key),
            _ => None,
        };
    }
    // XGROUP <subcommand> <key> ...: args[0] is the subcommand ("CREATE",
    // "SETID", "DESTROY", "DELCONSUMER", "CREATECONSUMER") and args[1] is
    // the stream key. Same pattern as OBJECT above.
    // XINFO STREAM/GROUPS/CONSUMERS <key>: identical layout.
    if (len == 6 && b0 == b'x' && cmd.eq_ignore_ascii_case(b"XGROUP"))
        || (len == 5 && b0 == b'x' && cmd.eq_ignore_ascii_case(b"XINFO"))
    {
        return match args.get(1) {
            Some(Frame::BulkString(key)) => Some(key),
            _ => None,
        };
    }
    // BITOP <op> destkey key [key ...]: args[0] is the operation literal
    // ("AND"/"OR"/"XOR"/"NOT"); the first key is the DESTINATION at args[1]
    // (metadata first_key=2). Routing by args[0] hashed the operation name.
    // Multi-shard servers consume BITOP in the multi-key coordinator before
    // this routing runs; this arm fixes single-shard-local + cluster-slot
    // routing.
    if (len, b0) == (5, b'b') && cmd.eq_ignore_ascii_case(b"BITOP") {
        return match args.get(1) {
            Some(Frame::BulkString(key)) => Some(key),
            _ => None,
        };
    }
    // ZDIFF/ZINTER/ZUNION/ZINTERCARD numkeys key [key ...]:
    //   args[0] is the integer numkeys literal; the first actual key is args[1].
    // Routing by args[0] would hash "2" (the numkeys string) to an arbitrary
    // shard and produce "ERR wrong type" or a silent miss.
    // Note: ZDIFF = 5 bytes, ZINTER/ZUNION = 6 bytes, ZINTERCARD = 10 bytes.
    if (len == 5 && b0 == b'z' && cmd.eq_ignore_ascii_case(b"ZDIFF"))
        || (len == 6
            && b0 == b'z'
            && (cmd.eq_ignore_ascii_case(b"ZINTER") || cmd.eq_ignore_ascii_case(b"ZUNION")))
        || (len == 10 && b0 == b'z' && cmd.eq_ignore_ascii_case(b"ZINTERCARD"))
    {
        return match args.get(1) {
            Some(Frame::BulkString(key)) => Some(key),
            _ => None,
        };
    }
    // XREAD [COUNT count] [BLOCK ms] STREAMS key [key ...] id [id ...]:
    // Scan args for the STREAMS token; the key immediately follows it.
    // No allocation — scan &[Frame] linearly.
    if len == 5 && b0 == b'x' && cmd.eq_ignore_ascii_case(b"XREAD") {
        for (i, arg) in args.iter().enumerate() {
            if let Frame::BulkString(tok) = arg {
                if tok.eq_ignore_ascii_case(b"STREAMS") {
                    return match args.get(i + 1) {
                        Some(Frame::BulkString(key)) => Some(key),
                        _ => None,
                    };
                }
            }
        }
        return None;
    }
    match &args[0] {
        Frame::BulkString(key) => Some(key),
        _ => None,
    }
}

/// Check if a command is a multi-key command requiring VLL coordination.
///
/// These commands operate on multiple keys that may live on different shards.
/// Single-arg DEL/UNLINK/EXISTS are NOT multi-key (handled as single-key fast path).
pub(crate) fn is_multi_key_command(cmd: &[u8], args: &[Frame]) -> bool {
    let len = cmd.len();
    if len == 0 {
        return false;
    }
    let b0 = cmd[0] | 0x20;
    match (len, b0) {
        (4, b'm') => cmd.eq_ignore_ascii_case(b"MGET") || cmd.eq_ignore_ascii_case(b"MSET"),
        // MSETNX: atomic multi-key write; the coordinator rejects it (CROSSSLOT) when
        // keys span shards, and runs it atomically when they are co-located.
        (6, b'm') => cmd.eq_ignore_ascii_case(b"MSETNX"),
        // DEL, UNLINK, EXISTS with multiple keys
        (3, b'd') => args.len() > 1 && cmd.eq_ignore_ascii_case(b"DEL"),
        (6, b'u') => args.len() > 1 && cmd.eq_ignore_ascii_case(b"UNLINK"),
        (6, b'e') => args.len() > 1 && cmd.eq_ignore_ascii_case(b"EXISTS"),
        // BITOP <op> dest src...: dest + sources can live on different shards.
        (5, b'b') => args.len() >= 3 && cmd.eq_ignore_ascii_case(b"BITOP"),
        // COPY src dst [REPLACE]: src and dst can live on different shards.
        // The `COPY ... DB n` form is EXCLUDED: it needs two databases and is
        // owned by the handlers' two-db interception (cross-db + cross-shard
        // simultaneously is unsupported, as before).
        (4, b'c') => {
            args.len() >= 2
                && cmd.eq_ignore_ascii_case(b"COPY")
                && !args
                    .iter()
                    .skip(2)
                    .any(|a| matches!(a, Frame::BulkString(o) if o.eq_ignore_ascii_case(b"DB")))
        }
        _ => false,
    }
}

/// Shard-locality of a queued MULTI/EXEC body.
///
/// `execute_transaction_sharded` runs the whole body on ONE shard's slice with
/// no per-key routing, so a key owned by a different shard is silently written
/// to (or read from) the wrong table. This classifies a queued body so the
/// EXEC handler can either run it locally (all keys local), route it to the
/// owner shard (all keys on one remote shard — Phase B), or reject it
/// (`CrossShard`: a single-process shared-nothing engine can't atomically span
/// shards). Hash tags (`{tag}`) collapse a body to `SingleShard`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TxnLocality {
    /// No key-bearing commands — safe to run on any shard.
    Keyless,
    /// Every key in the body resolves to this single shard.
    SingleShard(usize),
    /// Keys span more than one shard — not atomically executable.
    CrossShard,
}

/// Tear down every subscription a connection holds, abstracting ONLY the lock
/// the registry happens to sit behind.
///
/// `ConnectionContext` holds it in an `RwLock`; `handler_single` holds it in a
/// `Mutex`. That difference is the sole reason `try_handle_reset` cannot simply
/// take a `&ConnectionContext`, and it is not worth a second copy of RESET.
pub(crate) trait PubSubTeardown {
    /// Drop every channel AND pattern subscription held by `subscriber_id`.
    fn unsubscribe_all_for(&self, subscriber_id: u64);
}

impl PubSubTeardown for parking_lot::RwLock<crate::pubsub::PubSubRegistry> {
    fn unsubscribe_all_for(&self, subscriber_id: u64) {
        let mut reg = self.write();
        reg.unsubscribe_all(subscriber_id);
        reg.punsubscribe_all(subscriber_id);
    }
}

impl PubSubTeardown for parking_lot::Mutex<crate::pubsub::PubSubRegistry> {
    fn unsubscribe_all_for(&self, subscriber_id: u64) {
        let mut reg = self.lock();
        reg.unsubscribe_all(subscriber_id);
        reg.punsubscribe_all(subscriber_id);
    }
}

/// Commands that EXECUTE while a transaction is open instead of queueing.
///
/// Redis's list is exactly six: MULTI, EXEC, DISCARD, WATCH, RESET, QUIT.
/// (WATCH executes only to be refused — `queue_time_rejection` returns its
/// error — but it must reach that intercept rather than land in the queue.)
///
/// The replication handshake verbs are added on top, and only those. A replica
/// never opens a transaction, so queueing REPLCONF or PSYNC could not fix any
/// real client and could break a handshake no test covers — the asymmetry is
/// the whole argument for the exemption.
pub(crate) fn is_transaction_control(cmd: &[u8]) -> bool {
    const CONTROL: [&[u8]; 12] = [
        b"MULTI",
        b"EXEC",
        b"DISCARD",
        b"WATCH",
        b"UNWATCH",
        b"RESET",
        b"QUIT",
        b"REPLCONF",
        b"PSYNC",
        b"SYNC",
        b"REPLICAOF",
        b"SLAVEOF",
    ];
    CONTROL.iter().any(|c| cmd.eq_ignore_ascii_case(c))
}

/// Commands the transaction executor CANNOT run, so queueing them would turn a
/// working command into a guaranteed error inside `EXEC`.
///
/// `execute_transaction` replays the queue through `dispatch()`. These commands
/// are connection-level *intercepts* — they never reach `dispatch()` at all, so
/// a queued one comes back as `-ERR unknown command`. Measured 2026-08-12 by
/// queueing each inside a MULTI and reading the EXEC array:
///
/// ```text
/// CONFIG · CLIENT · ACL · CLUSTER · SCRIPT · WAIT          -> unknown command
/// INFO · SLOWLOG · PUBLISH · MEMORY · DEBUG · COMMAND · OBJECT -> execute fine
/// ```
///
/// Redis queues all of them and runs them properly. Moon cannot yet, so these
/// keep EXECUTING immediately — the pre-existing divergence, which the
/// client-compat manifest already waives — rather than queueing into a hard
/// error. That is strictly better than the alternative: before the queue gate a
/// client got its data with the wrong reply shape; queueing it unconditionally
/// gave them an error instead, which is a regression, not a narrowing.
///
/// `LATENCY` looked like a member and is NOT: it errors outside a transaction
/// too, because Moon does not implement it at all. Exempting it would have
/// papered over nothing and hidden a genuine unimplemented command.
///
/// `PUBSUB` is absent from `COMMAND_META`, so `queue_time_rejection` would call
/// it an unknown command and poison the transaction — it has no dot, so the
/// dotted carve-out misses it. That is exactly the regression class the §1 ⚠
/// assumption named. It is handled by `queue_time_rejection` consulting this
/// list, not by exempting it from queueing.
///
/// Removing an entry from this list requires teaching `execute_transaction` to
/// run it. The test `me10` asserts every queued command's EXEC result is not an
/// error, so a premature removal fails loudly.
pub(crate) fn is_intercept_only(cmd: &[u8]) -> bool {
    const INTERCEPT_ONLY: [&[u8]; 7] = [
        b"CONFIG", b"CLIENT", b"ACL", b"CLUSTER", b"SCRIPT", b"WAIT", b"PUBSUB",
    ];
    INTERCEPT_ONLY.iter().any(|c| cmd.eq_ignore_ascii_case(c))
}

/// Validate a command at QUEUE time, the way Redis does before storing it.
///
/// Returns `Some(error)` when the command must not be queued. The caller
/// replies that error INSTEAD of `+QUEUED`, does not push the command, and
/// sets `conn.multi_dirty` so `EXEC` refuses the whole block.
///
/// This is the difference between a transaction that is atomic with respect to
/// typos and one that is not. Measured against redis-server 8.6.1:
///
/// ```text
/// MULTI / NOSUCHCMD / SET k v / EXEC
///   redis -> k UNSET   (EXECABORT, nothing ran)
///   moon  -> k SET     (the valid half ran)
/// ```
///
/// Shared by all THREE handlers, for the same reason `RESET` and `WATCH` are:
/// a queue-time check that lands on one runtime and not the others is not a
/// fix, and no single-runtime CI job would notice.
///
/// The arity/existence check reads `COMMAND_META` — the SAME table dispatch
/// consults — so a command cannot become queueable-but-undispatchable, or the
/// reverse. That symmetry is the whole safety argument for this function:
/// rejecting a command that DOES dispatch would be a regression strictly worse
/// than the bug being fixed.
pub(crate) fn queue_time_rejection(cmd: &[u8], args: &[Frame]) -> Option<Frame> {
    // Verbs that are refused rather than queued. Redis rejects these because
    // they change the connection's mode, which a queued command cannot
    // meaningfully do. Moon used to EXECUTE `SUBSCRIBE` immediately, putting
    // the connection into subscriber mode in the middle of a transaction.
    const UNQUEUEABLE: [&[u8]; 4] = [b"SUBSCRIBE", b"UNSUBSCRIBE", b"PSUBSCRIBE", b"PUNSUBSCRIBE"];
    for verb in UNQUEUEABLE {
        if cmd.eq_ignore_ascii_case(verb) {
            let name = String::from_utf8_lossy(verb).to_uppercase();
            return Some(Frame::Error(Bytes::from(format!(
                "ERR {name} is not allowed in transactions"
            ))));
        }
    }
    if cmd.eq_ignore_ascii_case(b"WATCH") {
        return Some(Frame::Error(Bytes::from_static(
            b"ERR WATCH inside MULTI is not allowed",
        )));
    }

    let Some(meta) = crate::command::metadata::lookup(cmd) else {
        // Only a NON-namespaced name can confidently be called unknown.
        //
        // `COMMAND_META` (263 entries) covers Redis's surface plus the Moon
        // extensions that were registered — `GRAPH.QUERY` is in it — but NOT
        // every dotted family the handlers intercept ahead of dispatch.
        // Rejecting an unregistered dotted name here would break a command
        // that works fine OUTSIDE a transaction: a regression strictly worse
        // than the bug this function exists to fix.
        //
        // Audited 2026-08-12: of the dotted families absent from the table,
        // `TXN` is intercepted before the queue block, `FT.*` is rejected
        // above it, and `TS.*` / `JSON.*` do not exist in Moon at all. So
        // nothing is broken today — but that is four separate accidents, not
        // a safety argument, and the next dotted family added without a table
        // entry would silently become unusable inside MULTI.
        //
        // The carve-out costs only the ability to catch a typo'd dotted name.
        // Dotted names that ARE registered still get their arity checked.
        if cmd.contains(&b'.') {
            return None;
        }
        // Redis's format: the name quoted, then the args it did get. A driver
        // author reading this sees their typo; a generic "unknown command"
        // sends them looking at Moon.
        let mut detail = String::new();
        for a in args.iter().take(20) {
            if let Frame::BulkString(b) | Frame::SimpleString(b) = a {
                detail.push('\'');
                detail.push_str(&String::from_utf8_lossy(b));
                detail.push_str("', ");
            }
        }
        return Some(Frame::Error(Bytes::from(format!(
            "ERR unknown command '{}', with args beginning with: {detail}",
            String::from_utf8_lossy(cmd)
        ))));
    };

    // `arity` counts the command name itself, so compare against args + 1.
    // Positive = exact; negative = minimum (variadic).
    let given = args.len() as i16 + 1;
    let bad_arity = if meta.arity >= 0 {
        given != meta.arity
    } else {
        given < -meta.arity
    };
    if bad_arity {
        return Some(Frame::Error(Bytes::from(format!(
            "ERR wrong number of arguments for '{}' command",
            meta.name.to_lowercase()
        ))));
    }
    None
}

/// The reply `EXEC` owes a transaction poisoned at queue time.
pub(crate) fn execabort_frame() -> Frame {
    Frame::Error(Bytes::from_static(
        b"EXECABORT Transaction discarded because of previous errors.",
    ))
}

/// Handle `RESET`, returning `true` when the command was consumed.
///
/// MUST be called BEFORE the MULTI queueing step. Measured against
/// redis-server 8.6.1: with a transaction open, `RESET` replies `+RESET` and
/// the following `EXEC` errors `without MULTI` — it is executed immediately,
/// never queued. Moon's red run caught this by replying `+QUEUED`.
///
/// Shared by all THREE handlers for the same reason `WATCH` is: this surface
/// already drifted once. A partial RESET existed only inside
/// `handler_sharded`'s subscribe-mode loop, so RESET worked if you happened to
/// be subscribed on one runtime and was an unknown command everywhere else.
///
/// "Default state" is deliberately taken from `restore_migrated_state(None, …)`
/// — the SAME function `ConnectionState::new` uses — so RESET's idea of default
/// cannot drift from connection setup's idea of default.
pub(crate) fn try_handle_reset(
    cmd: &[u8],
    args: &[Frame],
    client_id: u64,
    conn: &mut super::core::ConnectionState,
    // Taken as three pieces rather than a `&ConnectionContext` so the embedded
    // handler — which has no such struct and holds the registry behind a
    // `Mutex` where the context uses an `RwLock` — can share this exact body
    // instead of growing a second, drifting copy of "what RESET restores".
    requirepass: &Option<String>,
    tracking_table: &parking_lot::Mutex<crate::tracking::TrackingTable>,
    pubsub: &dyn PubSubTeardown,
    responses: &mut Vec<Frame>,
    // `None` on the sharded handler, which does direct buffer I/O with no
    // codec object — there `conn.protocol_version` is itself authoritative.
    codec: Option<&mut crate::server::codec::RespCodec>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"RESET") {
        return false;
    }
    if !args.is_empty() {
        // Registry arity is 1. A rejected RESET must not half-apply: return
        // before touching any state.
        responses.push(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'reset' command",
        )));
        return true;
    }

    // Transaction
    conn.in_multi = false;
    conn.command_queue.clear();
    // The dirty flag must not survive the transaction it belongs to; a leak
    // would abort the NEXT, innocent one.
    conn.multi_dirty = false;
    conn.watched_keys.clear();

    // Client-side caching
    conn.tracking_state = Default::default();
    conn.tracking_rx = None;
    tracking_table.lock().untrack_all(client_id);

    // Pub/Sub — exit subscribe mode entirely.
    if conn.subscription_count > 0 {
        pubsub.unsubscribe_all_for(conn.subscriber_id);
    }
    conn.subscription_count = 0;

    // Identity + protocol, from the one definition of "default".
    let (proto, db, authed, user, name) =
        crate::server::conn::util::restore_migrated_state(None, requirepass);
    conn.protocol_version = proto;
    conn.selected_db = db;
    conn.authenticated = authed;
    conn.current_user = user;
    conn.client_name = name;
    // The wire codec must move with the connection, or the very next reply is
    // serialized in a protocol the client is no longer speaking.
    if let Some(codec) = codec {
        codec.set_protocol_version(proto);
    }
    crate::client_registry::update(client_id, |e| {
        e.name = None;
    });

    responses.push(Frame::SimpleString(Bytes::from_static(b"RESET")));
    true
}

/// Classify the WATCHed keys by the shard(s) they hash to.
///
/// Same lattice as the body's: no keys is `Keyless`, all on one shard is
/// `SingleShard`, anything else is `CrossShard`.
pub(crate) fn analyze_watch_locality(
    watched: &HashMap<Bytes, WatchToken>,
    num_shards: usize,
) -> TxnLocality {
    let mut owner: Option<usize> = None;
    for key in watched.keys() {
        let s = crate::shard::dispatch::key_to_shard(key, num_shards);
        match owner {
            None => owner = Some(s),
            Some(existing) if existing == s => {}
            Some(_) => return TxnLocality::CrossShard,
        }
    }
    match owner {
        None => TxnLocality::Keyless,
        Some(s) => TxnLocality::SingleShard(s),
    }
}

/// Combine the body's locality with the WATCH set's.
///
/// The CAS check runs where the body runs, reading that shard's slice — so a
/// watched key owned by a DIFFERENT shard reads version 0 there and fabricates
/// a conflict. That is safe (it aborts) but wrong, and a retry loop that can
/// never succeed is a livelock. Refusing loudly is the contract.
///
/// `Keyless` is the identity: a keyless body inherits the watch set's shard,
/// and an unwatched body inherits the body's.
pub(crate) fn merge_locality(body: TxnLocality, watch: TxnLocality) -> TxnLocality {
    match (body, watch) {
        (TxnLocality::CrossShard, _) | (_, TxnLocality::CrossShard) => TxnLocality::CrossShard,
        (TxnLocality::Keyless, other) | (other, TxnLocality::Keyless) => other,
        (TxnLocality::SingleShard(a), TxnLocality::SingleShard(b)) => {
            if a == b {
                TxnLocality::SingleShard(a)
            } else {
                TxnLocality::CrossShard
            }
        }
    }
}

/// Classify a queued transaction body by the shard(s) its keys hash to.
///
/// Uses the command-metadata key specs (`command_keys`) so multi-key commands
/// (MSET/DEL/…) contribute every key, and `key_to_shard` so `{hash tags}` are
/// honored identically to the normal routing path.
pub(crate) fn analyze_txn_locality(command_queue: &[Frame], num_shards: usize) -> TxnLocality {
    let mut owner: Option<usize> = None;
    let visit = |key: &[u8], owner: &mut Option<usize>| -> bool {
        let s = crate::shard::dispatch::key_to_shard(key, num_shards);
        match *owner {
            None => {
                *owner = Some(s);
                true
            }
            Some(existing) => existing == s,
        }
    };
    for frame in command_queue {
        let Some((cmd, args)) = extract_command(frame) else {
            continue;
        };
        for key in crate::tracking::invalidation::command_keys(cmd, args) {
            if !visit(&key, &mut owner) {
                return TxnLocality::CrossShard;
            }
        }
        // Prod-hardening #15: SORT/GEORADIUS/GEORADIUSBYMEMBER declare only
        // their SOURCE key in command metadata (first_key==last_key==1); the
        // optional `STORE`/`STOREDIST` destination is positional (the arg
        // after the token) and is NOT covered by `command_keys`. Without
        // this, `MULTI; SORT src STORE dst; EXEC` where src and dst hash to
        // different shards is misclassified as SingleShard(owner-of-src) and
        // the whole body routes to that one shard — `dst` gets written to the
        // WRONG shard's dataset and is invisible to a later normally-routed
        // `GET dst`. Detecting the STORE dest here forces CrossShard, which
        // the caller rejects with CROSSSLOT instead of silently misrouting.
        if let Some(dest) = store_clause_dest(cmd, args) {
            if !visit(dest, &mut owner) {
                return TxnLocality::CrossShard;
            }
        }
        // task #52 review round 3 (CodeRabbit, P1): GRAPH.* commands declare
        // NO keys in command metadata (first_key==last_key==0), so
        // `command_keys` above contributes nothing for them — a graph-only
        // body was misclassified `Keyless` (executed on whatever shard the
        // connection happened to be pinned to) and a mixed KV+graph body was
        // classified by its KV keys alone, ignoring the graph name entirely.
        // The STANDALONE `GRAPH.*` path (`try_handle_graph_command`) routes
        // by `graph_to_shard(name, num_shards)` — which is IDENTICALLY
        // `key_to_shard` (same hash-tag-aware xxh64, see
        // `shard::dispatch::graph_to_shard`'s doc) — so treating the graph
        // name as just another "key" via the SAME `visit` closure used above
        // reuses that exact routing rule: a body whose graph name and KV
        // keys disagree on owner becomes `CrossShard` (rejected CROSSSLOT,
        // same as the SORT/GEORADIUS STORE-dest case above) instead of
        // silently applying the graph write to the wrong shard's store,
        // invisible to later normally-routed single-command reads. A
        // graph-only body becomes `SingleShard(graph_owner)`, which the
        // caller already hops to the owner shard for (`execute_txn_on_owner`
        // / `ShardMessage::TxnExecute`, the same whole-body-atomic mechanism
        // a KV-only body routes through when queued from a non-owner shard)
        // — no NEW hop is introduced, so the "body runs on ONE local slice"
        // invariant holds. `GRAPH.LIST` is excluded: it has no name argument
        // and is a scatter-gather read across every shard, not owned by one.
        if cmd.len() > 6
            && cmd[..6].eq_ignore_ascii_case(b"GRAPH.")
            && !cmd.eq_ignore_ascii_case(b"GRAPH.LIST")
        {
            if let Some(name) = args.first().and_then(super::util::extract_bytes) {
                if !visit(&name, &mut owner) {
                    return TxnLocality::CrossShard;
                }
            }
        }
    }
    match owner {
        None => TxnLocality::Keyless,
        Some(s) => TxnLocality::SingleShard(s),
    }
}

/// Extract the `STORE`/`STOREDIST` destination key from a SORT or GEORADIUS
/// family command, if present. Returns `None` for commands that have no such
/// clause or when the token has no following argument.
///
/// The destination is positional (the argument immediately after a `STORE`
/// or `STOREDIST` token in `args`, where `args` excludes the command name),
/// so it cannot be captured by the fixed `first_key/last_key/step` metadata
/// key specs the normal routing path uses.
fn store_clause_dest<'a>(cmd: &[u8], args: &'a [Frame]) -> Option<&'a [u8]> {
    let is_store_cmd = cmd.eq_ignore_ascii_case(b"SORT")
        || cmd.eq_ignore_ascii_case(b"GEORADIUS")
        || cmd.eq_ignore_ascii_case(b"GEORADIUSBYMEMBER");
    if !is_store_cmd {
        return None;
    }
    let mut i = 0;
    while i < args.len() {
        if let Frame::BulkString(tok) = &args[i] {
            if tok.eq_ignore_ascii_case(b"STORE") || tok.eq_ignore_ascii_case(b"STOREDIST") {
                if let Some(Frame::BulkString(dest)) = args.get(i + 1) {
                    return Some(dest.as_ref());
                }
                return None;
            }
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod as_of_tests {
    //! Unit tests for `resolve_ft_search_as_of_lsn` (TEMP-04 + ACID-09).
    //!
    //! Covers the five precedence branches the helper must honour:
    //!   1. No AS_OF, no TXN            -> Ok(0)
    //!   2. No AS_OF, TXN present       -> Ok(txn.snapshot_lsn)
    //!   3. AS_OF present + registry hit (even when TXN present) -> Ok(lsn)
    //!   4. AS_OF present + registry miss -> Err(Frame::Error)
    //!   5. AS_OF present + shard_databases=None (handler_single) -> Err(Frame::Error)
    use super::*;
    use crate::protocol::Frame;
    use crate::shard::shared_databases::ShardDatabases;
    use crate::storage::Database;
    use crate::temporal::TemporalRegistry;
    use crate::transaction::CrossStoreTxn;
    use bytes::Bytes;
    use std::sync::Arc;

    const ERR_BYTES: &[u8] =
        b"ERR no temporal snapshot registered for the given AS_OF timestamp; call TEMPORAL.SNAPSHOT_AT first";

    /// Build a 1-shard / 1-db `ShardDatabases` with a registered binding
    /// `wall_ms=1_000 -> lsn=42` so tests can exercise the registry path.
    ///
    /// Also initialises the thread-local `ShardSlice` (via `reset_test_shard`)
    /// with the temporal registry pre-populated, so `with_shard` queries in
    /// `resolve_ft_search_as_of_lsn` find the binding.
    fn build_fixture() -> Arc<ShardDatabases> {
        let (dbs, mut inits) = ShardDatabases::new(vec![vec![Database::new()]]);
        let mut init = inits.remove(0);
        // Pre-populate temporal registry on the ShardSliceInit before wiring it.
        let mut reg = Box::new(TemporalRegistry::new());
        reg.record(1_000, 42);
        init.temporal_registry = Some(reg);
        crate::shard::slice::reset_test_shard(crate::shard::slice::ShardSlice::new(init));
        dbs
    }

    fn frame_bulk(bytes: &'static [u8]) -> Frame {
        Frame::BulkString(Bytes::from_static(bytes))
    }

    /// Helper: construct a FT.SEARCH arg vec with or without AS_OF clause.
    fn ft_search_args(as_of: Option<i64>) -> Vec<Frame> {
        let mut args = vec![frame_bulk(b"idx"), frame_bulk(b"*")];
        if let Some(wall_ms) = as_of {
            args.push(frame_bulk(b"AS_OF"));
            // parse_as_of_clause reads i64 decimal text from a BulkString.
            args.push(Frame::BulkString(Bytes::from(wall_ms.to_string())));
        }
        args
    }

    #[test]
    fn extract_primary_key_object_routes_by_real_key() {
        // OBJECT <subcommand> <key>: routing must hash the key (arg 2),
        // never the subcommand name.
        let args = vec![frame_bulk(b"FREQ"), frame_bulk(b"mykey")];
        let got = extract_primary_key(b"OBJECT", &args);
        assert_eq!(got.map(|b| b.as_ref()), Some(&b"mykey"[..]));
        // OBJECT HELP has no key — executes locally.
        let help = vec![frame_bulk(b"HELP")];
        assert!(extract_primary_key(b"OBJECT", &help).is_none());
    }

    #[test]
    fn extract_primary_key_hotkeys_is_keyless() {
        // HOTKEYS COUNT 5 must not route by "COUNT".
        let args = vec![frame_bulk(b"COUNT"), frame_bulk(b"5")];
        assert!(extract_primary_key(b"HOTKEYS", &args).is_none());
    }

    #[test]
    fn resolve_ft_search_as_of_lsn_default_returns_zero() {
        let fixture = build_fixture();
        let args = ft_search_args(None);
        let got = resolve_ft_search_as_of_lsn(&args, Some(&fixture), None);
        assert_eq!(got, Ok(0));
    }

    #[test]
    fn resolve_ft_search_as_of_lsn_uses_txn_snapshot_when_no_explicit_as_of() {
        let fixture = build_fixture();
        let args = ft_search_args(None);
        let txn = CrossStoreTxn::new(1, 99, 0);
        let got = resolve_ft_search_as_of_lsn(&args, Some(&fixture), Some(&txn));
        assert_eq!(got, Ok(99));
    }

    #[test]
    fn resolve_ft_search_as_of_lsn_explicit_as_of_beats_txn_snapshot() {
        let fixture = build_fixture();
        let args = ft_search_args(Some(1_000));
        let txn = CrossStoreTxn::new(1, 99, 0);
        let got = resolve_ft_search_as_of_lsn(&args, Some(&fixture), Some(&txn));
        // Registry binding at wall_ms=1_000 is lsn=42, NOT txn.snapshot_lsn=99.
        assert_eq!(got, Ok(42));
    }

    #[test]
    fn resolve_ft_search_as_of_lsn_explicit_as_of_missing_snapshot_returns_err() {
        let fixture = build_fixture();
        // wall_ms=500 precedes the only registered binding (1_000 -> 42).
        let args = ft_search_args(Some(500));
        let got = resolve_ft_search_as_of_lsn(&args, Some(&fixture), None);
        match got {
            Err(Frame::Error(msg)) => assert_eq!(msg.as_ref(), ERR_BYTES),
            other => panic!("expected Err(Frame::Error(ERR_BYTES)), got {other:?}"),
        }
    }

    #[test]
    fn resolve_ft_search_as_of_lsn_explicit_as_of_with_none_registry_returns_err() {
        // handler_single.rs has no ShardDatabases in scope -> Option::None.
        // AS_OF cannot be resolved without a registry; surface the same ERR.
        let args = ft_search_args(Some(1_000));
        let got = resolve_ft_search_as_of_lsn(&args, None, None);
        match got {
            Err(Frame::Error(msg)) => assert_eq!(msg.as_ref(), ERR_BYTES),
            other => panic!("expected Err(Frame::Error(ERR_BYTES)), got {other:?}"),
        }
    }
}

/// Resolve the pending v3-5 local-leg group-commit barrier, if any.
///
/// Coordinator local-leg writes (MSET/MSETNX/BITOP/COPY/DEL/UNLINK legs owned
/// by the connection's own shard) enqueue their AOF append fire-and-forget
/// under `appendfsync=always` and record their response index here; ONE
/// `fsync_barrier` per batch confirms them all. This MUST run before ANY
/// flush of `responses` to the client — the batch end, but also the early
/// flushes (blocking commands, SUBSCRIBE entry, PSYNC hijack). Skipping it
/// there would (a) ack a write whose durability was never confirmed and
/// (b) leave stale indexes that panic or misattribute errors when the
/// response vec is replaced (PR #213 review finding).
///
/// Always drains `idxs`. On barrier failure every recorded response is
/// overwritten with `AOF_FSYNC_ERR` — never a false `+OK`.
pub async fn resolve_local_leg_barrier(
    aof_pool: &Option<Arc<crate::persistence::aof::AofWriterPool>>,
    shard_id: usize,
    idxs: &mut Vec<usize>,
    responses: &mut [Frame],
) {
    if idxs.is_empty() {
        return;
    }
    if let Some(pool) = aof_pool {
        if pool.fsync_barrier(shard_id).await.is_err() {
            for idx in idxs.iter() {
                if let Some(slot) = responses.get_mut(*idx) {
                    *slot =
                        Frame::Error(Bytes::from_static(crate::persistence::aof::AOF_FSYNC_ERR));
                }
            }
        }
    }
    idxs.clear();
}

#[cfg(test)]
mod txn_locality_tests {
    use super::{TxnLocality, analyze_txn_locality};
    use crate::protocol::Frame;
    use bytes::Bytes;

    /// Build a queued command frame (name + args) as the wire form.
    fn cmd(parts: &[&str]) -> Frame {
        Frame::Array(
            parts
                .iter()
                .map(|p| Frame::BulkString(Bytes::copy_from_slice(p.as_bytes())))
                .collect::<Vec<_>>()
                .into(),
        )
    }

    #[test]
    fn empty_and_keyless_are_keyless() {
        assert_eq!(analyze_txn_locality(&[], 4), TxnLocality::Keyless);
        let q = [cmd(&["PING"]), cmd(&["MULTI"])];
        assert_eq!(analyze_txn_locality(&q, 4), TxnLocality::Keyless);
    }

    #[test]
    fn single_shard_at_one_shard() {
        // With one shard every key resolves to shard 0.
        let q = [cmd(&["SET", "a", "1"]), cmd(&["SET", "b", "2"])];
        assert_eq!(analyze_txn_locality(&q, 1), TxnLocality::SingleShard(0));
    }

    #[test]
    fn hash_tags_collapse_to_one_shard() {
        // `{t}` forces co-location regardless of the surrounding key text.
        let q = [
            cmd(&["SET", "user:{t}:name", "x"]),
            cmd(&["INCR", "user:{t}:hits"]),
            cmd(&["DEL", "user:{t}:tmp"]),
        ];
        let owner = crate::shard::dispatch::key_to_shard(b"t", 8);
        assert_eq!(analyze_txn_locality(&q, 8), TxnLocality::SingleShard(owner));
    }

    #[test]
    fn spanning_keys_are_cross_shard() {
        // Find two keys that hash to different shards, then confirm CrossShard.
        let num = 8;
        let base = crate::shard::dispatch::key_to_shard(b"k0", num);
        let mut other = None;
        for i in 1..1000 {
            let k = format!("k{i}");
            if crate::shard::dispatch::key_to_shard(k.as_bytes(), num) != base {
                other = Some(k);
                break;
            }
        }
        let other = other.expect("two keys on different shards must exist across 8 shards");
        let q = [cmd(&["SET", "k0", "1"]), cmd(&["SET", &other, "2"])];
        assert_eq!(analyze_txn_locality(&q, num), TxnLocality::CrossShard);
    }

    #[test]
    fn multi_key_command_contributes_every_key() {
        // MSET's keys are checked individually; spanning keys ⇒ CrossShard.
        let num = 8;
        let base = crate::shard::dispatch::key_to_shard(b"m0", num);
        let mut other = None;
        for i in 1..1000 {
            let k = format!("m{i}");
            if crate::shard::dispatch::key_to_shard(k.as_bytes(), num) != base {
                other = Some(k);
                break;
            }
        }
        let other = other.expect("two keys on different shards must exist");
        let q = [cmd(&["MSET", "m0", "1", &other, "2"])];
        assert_eq!(analyze_txn_locality(&q, num), TxnLocality::CrossShard);
    }

    #[test]
    fn sort_store_dest_on_other_shard_is_cross_shard() {
        // Prod-hardening #15: `SORT src STORE dst` where src and dst hash to
        // different shards must classify CrossShard (→ CROSSSLOT reject), not
        // silently SingleShard(owner-of-src) and misroute the STORE write.
        let num = 8;
        let base = crate::shard::dispatch::key_to_shard(b"src", num);
        let mut dst = None;
        for i in 0..1000 {
            let k = format!("dst{i}");
            if crate::shard::dispatch::key_to_shard(k.as_bytes(), num) != base {
                dst = Some(k);
                break;
            }
        }
        let dst = dst.expect("a dst on a different shard than src must exist");
        let q = [cmd(&["SORT", "src", "STORE", &dst])];
        assert_eq!(analyze_txn_locality(&q, num), TxnLocality::CrossShard);
    }

    #[test]
    fn sort_store_dest_same_shard_stays_single_shard() {
        // Co-located source + STORE dest (hash tag) stays SingleShard — the
        // STORE clause must not spuriously force CrossShard.
        let q = [cmd(&["SORT", "{t}:src", "STORE", "{t}:dst"])];
        let owner = crate::shard::dispatch::key_to_shard(b"t", 8);
        assert_eq!(analyze_txn_locality(&q, 8), TxnLocality::SingleShard(owner));
    }

    #[test]
    fn georadius_storedist_dest_participates_in_locality() {
        // GEORADIUS ... STOREDIST dst — the dest key must be considered too.
        let num = 8;
        let base = crate::shard::dispatch::key_to_shard(b"geo", num);
        let mut dst = None;
        for i in 0..1000 {
            let k = format!("gd{i}");
            if crate::shard::dispatch::key_to_shard(k.as_bytes(), num) != base {
                dst = Some(k);
                break;
            }
        }
        let dst = dst.expect("a dst on a different shard must exist");
        let q = [cmd(&[
            "GEORADIUS",
            "geo",
            "15",
            "37",
            "200",
            "km",
            "STOREDIST",
            &dst,
        ])];
        assert_eq!(analyze_txn_locality(&q, num), TxnLocality::CrossShard);
    }
}

#[cfg(test)]
mod watch_locality_tests {
    use super::{TxnLocality, WatchToken, analyze_watch_locality, merge_locality};
    use bytes::Bytes;
    use std::collections::HashMap;

    fn watched(keys: &[&str]) -> HashMap<Bytes, WatchToken> {
        keys.iter()
            .map(|k| {
                (
                    Bytes::copy_from_slice(k.as_bytes()),
                    WatchToken { version: 1 },
                )
            })
            .collect()
    }

    /// Find two keys that hash to different shards, so the cross-shard case is
    /// asserted against the real hash rather than an assumed key layout.
    fn cross_shard_pair(num_shards: usize) -> (String, String) {
        let first = "wk0".to_string();
        let s0 = crate::shard::dispatch::key_to_shard(first.as_bytes(), num_shards);
        for i in 1..4096 {
            let cand = format!("wk{i}");
            if crate::shard::dispatch::key_to_shard(cand.as_bytes(), num_shards) != s0 {
                return (first, cand);
            }
        }
        panic!("no cross-shard key pair found in 4096 candidates at {num_shards} shards");
    }

    #[test]
    fn no_watched_keys_is_keyless() {
        assert_eq!(
            analyze_watch_locality(&watched(&[]), 4),
            TxnLocality::Keyless
        );
    }

    #[test]
    fn keys_on_one_shard_classify_to_that_shard() {
        let k = "solo";
        let expect = crate::shard::dispatch::key_to_shard(k.as_bytes(), 4);
        assert_eq!(
            analyze_watch_locality(&watched(&[k]), 4),
            TxnLocality::SingleShard(expect)
        );
    }

    #[test]
    fn keys_on_different_shards_are_cross_shard() {
        let (a, b) = cross_shard_pair(4);
        assert_eq!(
            analyze_watch_locality(&watched(&[&a, &b]), 4),
            TxnLocality::CrossShard
        );
    }

    /// A single shard is degenerate: every key lands on shard 0, so a watch set
    /// can never be cross-shard there. Worth pinning — the CROSSSLOT refusal
    /// must not fire at `--shards 1`, the default for most deployments.
    #[test]
    fn one_shard_never_classifies_cross_shard() {
        assert_eq!(
            analyze_watch_locality(&watched(&["a", "b", "c"]), 1),
            TxnLocality::SingleShard(0)
        );
    }

    // --- the merge lattice: all nine combinations ---

    #[test]
    fn keyless_is_the_identity() {
        assert_eq!(
            merge_locality(TxnLocality::Keyless, TxnLocality::Keyless),
            TxnLocality::Keyless
        );
        assert_eq!(
            merge_locality(TxnLocality::Keyless, TxnLocality::SingleShard(2)),
            TxnLocality::SingleShard(2)
        );
        assert_eq!(
            merge_locality(TxnLocality::SingleShard(2), TxnLocality::Keyless),
            TxnLocality::SingleShard(2)
        );
    }

    #[test]
    fn cross_shard_absorbs_everything() {
        for other in [
            TxnLocality::Keyless,
            TxnLocality::SingleShard(0),
            TxnLocality::CrossShard,
        ] {
            assert_eq!(
                merge_locality(TxnLocality::CrossShard, other),
                TxnLocality::CrossShard
            );
            assert_eq!(
                merge_locality(other, TxnLocality::CrossShard),
                TxnLocality::CrossShard
            );
        }
    }

    #[test]
    fn agreeing_shards_stay_single() {
        assert_eq!(
            merge_locality(TxnLocality::SingleShard(3), TxnLocality::SingleShard(3)),
            TxnLocality::SingleShard(3)
        );
    }

    /// The case the whole merge exists for: the body commits on one shard and
    /// the watch set lives on another, so the CAS check would read the wrong
    /// slice and fabricate a conflict the client can never clear. Refusing is
    /// the contract; silently aborting forever is a livelock.
    #[test]
    fn disagreeing_shards_become_cross_shard() {
        assert_eq!(
            merge_locality(TxnLocality::SingleShard(1), TxnLocality::SingleShard(2)),
            TxnLocality::CrossShard
        );
    }
}
