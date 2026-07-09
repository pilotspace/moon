#[cfg(feature = "runtime-tokio")]
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
    watched_keys: &HashMap<Bytes, u32>,
    selected_db: &mut usize,
    exec_publishes: &mut Vec<(usize, Bytes, Bytes)>,
) -> (Frame, Vec<Bytes>) {
    let mut guard = db[*selected_db].write();
    let db_count = db.len();
    guard.refresh_now();

    // Check WATCH versions -- if any key's version changed, abort
    for (key, watched_version) in watched_keys {
        let current_version = guard.get_version(key);
        if current_version != *watched_version {
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

        // Check if this is a write command for AOF logging
        let is_write = metadata::is_write(cmd);

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
/// Returns the result Frame (an array of per-command responses) **and** the
/// serialized AOF bytes for each successful write, in order. The caller MUST
/// append those entries to the shard's AOF (previously this path did no
/// persistence, so MULTI/EXEC writes were silently lost on restart).
pub(crate) fn execute_transaction_sharded(
    shard_databases: &std::sync::Arc<crate::shard::shared_databases::ShardDatabases>,
    _shard_id: usize,
    command_queue: &[Frame],
    selected_db: usize,
    cached_clock: &CachedClock,
    exec_publishes: &mut Vec<(usize, Bytes, Bytes)>,
) -> (Frame, Vec<Bytes>) {
    let db_count = shard_databases.db_count();

    let mut results = Vec::with_capacity(command_queue.len());
    let mut aof_entries: Vec<Bytes> = Vec::new();
    let mut selected = selected_db;

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

        // Serialize write commands for AOF *before* dispatch (matches the
        // single-shard `execute_transaction` path). Without this the sharded
        // MULTI/EXEC path logged nothing, so every transactional write was
        // silently lost on restart. Fully-qualified paths because `metadata`
        // is only `use`d under runtime-tokio but this fn compiles under both.
        let aof_bytes = if crate::command::metadata::is_write(cmd) {
            let mut buf = bytes::BytesMut::new();
            crate::protocol::serialize::serialize(cmd_frame, &mut buf);
            Some(buf.freeze())
        } else {
            None
        };

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
                aof_entries.push(bytes);
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
            crate::shard::slice::with_shard(|s| {
                crate::shard::spsc_handler::auto_flush_indexes(
                    &mut s.vector_store,
                    &mut s.text_store,
                    cmd.eq_ignore_ascii_case(b"FLUSHDB"),
                    selected as u8,
                );
            });
        }

        results.push(response);
    }

    (Frame::Array(results.into()), aof_entries)
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
pub(crate) async fn persist_txn_aof(
    ctx: &crate::server::conn::core::ConnectionContext,
    aof_entries: Vec<Bytes>,
) -> Result<(), ()> {
    if aof_entries.is_empty() {
        return Ok(());
    }
    let Some(ref pool) = ctx.aof_pool else {
        return Ok(());
    };
    let mut barrier_pending = false;
    for bytes in aof_entries {
        let lsn = crate::persistence::aof::AofWriterPool::issue_append_lsn(
            &ctx.repl_state,
            ctx.shard_id,
            bytes.len(),
        );
        match pool.send_append_group(ctx.shard_id, lsn, bytes).await {
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

/// Fan out one EXEC-queued PUBLISH (C2): local shard synchronously, remote
/// shards via targeted `PubSubPublish` SPSC messages, awaited so the returned
/// count matches the immediate-PUBLISH path. Called by the sharded handlers
/// after `execute_transaction_sharded` returns — i.e. after every write queued
/// before the PUBLISH has been applied.
pub(crate) async fn publish_post_txn(
    ctx: &super::core::ConnectionContext,
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
    {
        let mut producers = ctx.dispatch_tx.borrow_mut();
        for target in &remote_targets {
            let msg = crate::shard::dispatch::ShardMessage::PubSubPublish(Box::new(
                crate::shard::dispatch::PubSubPublishPayload {
                    channel: channel.clone(),
                    message: message.clone(),
                    slot: slot.clone(),
                },
            ));
            let idx = ChannelMesh::target_index(ctx.shard_id, *target);
            if producers[idx].try_push(msg).is_ok() {
                ctx.spsc_notifiers[*target].notify_one();
            } else {
                // Ring full: count this target as delivered-to-zero rather
                // than hanging the EXEC reply (mirrors the batch-flush path).
                slot.add(0);
            }
        }
    }
    crate::shard::dispatch::PubSubResponseFuture::new(slot.clone()).await;
    local_count + slot.get()
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
