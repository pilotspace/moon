// Both runtimes need this now: `execute_transaction_sharded` carries the
// WATCH token map, and that path is the one the monoio build ships.
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use bytes::Bytes;
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

/// Record that the reply at `at` — and every reply after it in this batch — is
/// produced under `version`.
///
/// Call from the `HELLO` handler with `responses.len()` BEFORE pushing the
/// HELLO reply, so the switch covers that reply too: Redis renders `HELLO`'s own
/// answer in the protocol it has just negotiated.
///
/// **Call this BEFORE assigning `conn.protocol_version`.** The first call in a
/// batch reads that field to learn what the batch STARTED in; calling it after
/// the assignment records the new version as the batch start and silently
/// restores the very bug this exists to fix. Only an end-to-end test can catch
/// that mistake — `tests/batch_protocol_version.rs::bpv1` and `::bpv3` are the
/// pins, and they DID catch it during development.
pub(crate) fn note_protocol_switch(
    conn: &mut super::core::ConnectionState,
    at: usize,
    version: u8,
) {
    if conn.proto_switches.is_empty() {
        conn.proto_batch_start = conn.protocol_version;
    }
    conn.proto_switches.push((at, version));
}

/// Walks a batch index-by-index, yielding the protocol version in effect at
/// each one.
///
/// Split out from `encode_response_batch` so the version arithmetic — the part
/// that is easy to get subtly wrong and expensive to observe on the wire — can
/// be tested directly. Indices must be visited in ascending order; the cursor
/// never rewinds, which keeps the walk O(batch) rather than O(batch x switches).
struct ProtoWalk<'a> {
    switches: &'a [(usize, u8)],
    next: usize,
    version: u8,
}

impl<'a> ProtoWalk<'a> {
    fn new(start: u8, switches: &'a [(usize, u8)]) -> Self {
        Self {
            switches,
            next: 0,
            version: start,
        }
    }

    /// Version for reply `idx`. A switch recorded AT `idx` applies to it —
    /// `HELLO`'s own reply is rendered in the protocol it just negotiated.
    fn version_at(&mut self, idx: usize) -> u8 {
        while let Some(&(at, to)) = self.switches.get(self.next)
            && at <= idx
        {
            self.version = to;
            self.next += 1;
        }
        self.version
    }
}

/// Serialize a whole response batch, honouring any protocol switch recorded
/// inside it, then clear the switch list ready for the next batch.
///
/// A pipelined `HELLO` moves `conn.protocol_version` the instant it is handled,
/// but the batch is not serialized until every command in it has run. Encoding
/// the batch under one final version therefore RETRO-encodes the replies
/// produced before the switch — measured against redis-server 8.6.1, a
/// `CONFIG GET` answered under RESP3 then followed by `HELLO 2` in the same
/// pipeline must still go out as `%1`, not `*2`.
///
/// With no switch recorded — every batch that contains no `HELLO`, which is
/// essentially all of them — this is exactly the single-version loop it
/// replaces, with one branch and no allocation.
pub(crate) fn encode_response_batch(
    conn: &mut super::core::ConnectionState,
    responses: &[Frame],
    buf: &mut BytesMut,
) {
    if conn.proto_switches.is_empty() {
        if conn.protocol_version >= 3 {
            for item in responses {
                crate::protocol::serialize_resp3(item, buf);
            }
        } else {
            for item in responses {
                crate::protocol::serialize(item, buf);
            }
        }
        return;
    }

    let mut walk = ProtoWalk::new(conn.proto_batch_start, &conn.proto_switches);
    for (idx, item) in responses.iter().enumerate() {
        if walk.version_at(idx) >= 3 {
            crate::protocol::serialize_resp3(item, buf);
        } else {
            crate::protocol::serialize(item, buf);
        }
    }
    conn.proto_switches.clear();
}

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
            // moon#670: a non-string subcommand is still an unknown one, and
            // it must read like every other container's rejection. `b""` rather
            // than the frame's bytes: there are none to echo.
            return crate::command::helpers::err_unknown_subcommand("CONFIG", b"");
        }
    };

    let sub_args = &args[1..];

    if let Some(help) = crate::command::help_text::help_if_requested("CONFIG", subcmd) {
        help
    } else if subcmd.eq_ignore_ascii_case(b"GET") {
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
        // moon#670: Redis points at CONFIG HELP rather than enumerating the
        // four names. Enumerating them here also went stale the moment a fifth
        // was added.
        crate::command::helpers::err_unknown_subcommand("CONFIG", subcmd)
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
            // Null ARRAY, not null bulk: Redis answers an aborted EXEC with
            // `*-1`, and every client library decodes EXEC as an array — so
            // the abort path is precisely the one optimistic-locking code is
            // written to handle (moon#482).
            return (Frame::NullArray, Vec::new()); // Transaction aborted
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
        //
        // moon#639 deliberately does NOT leave intercept placeholders here.
        // This executor has exactly one caller — `handler_single`, the
        // single-shard tokio / embedded path — and that handler runs its
        // AUTH / HELLO / ACL / CLIENT / CONFIG / WAIT intercepts ABOVE its
        // MULTI gate, so those never reach this loop at all. The families it
        // does queue (CLUSTER, SCRIPT, PUBSUB) reach `dispatch()` below and
        // answer for real; a placeholder here would replace a working reply
        // with a Null nobody fills. `handler_single` keeps the queue-time
        // divergence for its six intercepted families — it is a library entry
        // point, not the shipped server path, and `embedded.rs` already steers
        // transactional embedders to the sharded handler.
        if queue_exec_publish(cmd, cmd_args, &mut results, exec_publishes) {
            continue;
        }

        // moon#524: same immediate-only execution of an unrewritten blocking
        // pop as `execute_transaction_sharded` below — this executor sees the
        // identical queued frame, so it needs the identical branch or the
        // embedded (single-shard tokio) deployment answers "unknown command"
        // for a queued BLPOP.
        if crate::server::conn::blocking_txn::queues_unrewritten(cmd) {
            if let Some(outcome) = crate::server::conn::blocking_txn::try_exec_blocking_in_txn(
                cmd, cmd_args, &mut guard,
            ) {
                if let Some(effect) = outcome.effect {
                    let mut buf = BytesMut::new();
                    crate::protocol::serialize::serialize(&effect, &mut buf);
                    aof_entries.push(buf.freeze());
                }
                results.push(outcome.reply);
                continue;
            }
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
    // moon#606: keys this body wrote that a blocked client may be waiting on.
    // Raised by the CALLER after the body, never here — see the collection
    // site below for why.
    exec_wakes: &mut Vec<(usize, Bytes, crate::blocking::WaitFamily)>,
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
        // Null ARRAY — same reply as the embedded executor above (moon#482).
        return (Frame::NullArray, Vec::new(), Vec::new());
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
        // moon#639: a connection-level intercept (CONFIG, CLIENT, ACL,
        // CLUSTER, SCRIPT, WAIT, PUBSUB, AUTH, HELLO, FUNCTION) queues like any other
        // command but cannot be replayed through `dispatch()`. Leave its slot
        // and let the caller — which HAS the connection — fill it in. Keeping
        // the slot here is what makes the EXEC array one-per-queued-command
        // regardless of which executor ran the body.
        if is_txn_connection_intercept(cmd) {
            results.push(TXN_INTERCEPT_PLACEHOLDER);
            continue;
        }

        if queue_exec_publish(cmd, cmd_args, &mut results, exec_publishes) {
            continue;
        }

        // moon#524: a blocking pop queued inside MULTI runs here in
        // immediate-only mode, as the ORIGINAL command — it was deliberately
        // NOT rewritten at queue time, because `LPOP`/`ZPOPMIN` answer a
        // different shape (and `LPOP k1 k2` is a COUNT, not a second key).
        //
        // It takes `apply_resp3_conversion` for the same reason every other
        // arm of this loop does. The original note here said converting would
        // make the in-MULTI reply differ from the standalone one — true at the
        // time, because the LIVE monoio path did not convert either. moon#559
        // fixed that side (the live path is an intercept that was skipping the
        // choke point), so the two are equal again with BOTH converting, which
        // is the side that matches redis-server. The RESP2/RESP3 null spelling
        // remains the serializer's job (`Frame::NullArray` -> `*-1` / `_`).
        if crate::server::conn::blocking_txn::queues_unrewritten(cmd) {
            let outcome = crate::shard::slice::with_shard_db(selected, |db| {
                db.refresh_now_from_cache(cached_clock);
                crate::server::conn::blocking_txn::try_exec_blocking_in_txn(cmd, cmd_args, db)
            });
            if let Some(outcome) = outcome {
                // The AOF/replication record is the SYNTHESISED single-key
                // sibling, never the queued frame: a replica applying a
                // literal `BLPOP` would block its apply loop. `None` when
                // nothing popped, so a miss reaches neither plane.
                if let Some(effect) = outcome.effect {
                    let mut buf = bytes::BytesMut::new();
                    crate::protocol::serialize::serialize(&effect, &mut buf);
                    aof_entries.push((selected, buf.freeze()));
                }
                results.push(super::util::apply_resp3_conversion(
                    cmd,
                    cmd_args,
                    outcome.reply,
                    proto,
                ));
                continue;
            }
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

        // moon#606: a producer queued inside MULTI must wake whoever is
        // blocked on the key it wrote. This executor reaches none of the live
        // wake hooks, so a `MULTI ; LPUSH k v ; EXEC` used to leave a `BLPOP k`
        // asleep until its own timeout.
        //
        // Recorded for the caller rather than raised here, for two reasons.
        // It matches Redis, which defers to the ready-keys pass that runs
        // after the command — and EXEC is one command, so every waiter sees
        // the whole transaction applied, never a half-built body. And this
        // function has no registry access anyway (same reason `exec_publishes`
        // and the graph records are returned rather than acted on).
        //
        // The caller's own registry is the right one: `TxnLocality` rejects a
        // cross-shard body outright, so the shard executing this owns every
        // key in it.
        if !matches!(&response, Frame::Error(_))
            && let Some(family) = crate::blocking::wakeup::producer_family(cmd)
            && let Some(key) = cmd_args
                .get(crate::blocking::wakeup::producer_wake_key_index(cmd))
                .and_then(super::util::extract_bytes)
        {
            exec_wakes.push((entry_db, key, family));
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
                // moon#677: keyspace half, on the local shard. The deferred
                // broadcast above carries the same FLUSHALL to the other
                // shards, where the SPSC arm clears their databases.
                if cmd.eq_ignore_ascii_case(b"FLUSHALL") {
                    crate::command::server_admin::flush_every_database(&mut s.databases, selected);
                }
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
) -> FanoutOutcome {
    fanout_to_other_shards(ctx, shutdown, "script_load", |ack| {
        crate::shard::dispatch::ShardMessage::ScriptLoad {
            sha1: sha1.to_owned(),
            script: script.clone(),
            ack: Some(ack),
        }
    })
    .await
}

/// Shards whose inbound fan-out pushes are forced to fail, from
/// `MOON_TEST_DROP_FANOUT_TO_SHARD` (comma-separated ids). Read ONCE, into a
/// bitmask; never set in production.
///
/// The partial-fan-out path needs a wedged shard to reach, so without an
/// injection point the code that reports a partial `FUNCTION` mutation, and
/// the `ScriptCache` republish-on-retry, would ship covered only by reasoning.
/// The riskiest branch in a fix is the one no test can enter. Same pattern as
/// `MOON_TEST_SLOW_SHARD_START_MS`.
///
/// A SET rather than a single id because a test cannot choose which shard its
/// connection lands on: `SO_REUSEPORT` placement is the kernel's business, and
/// a connection is never a target of its own fan-out, so naming one shard
/// leaves the outcome dependent on where the client happened to land — which
/// is exactly how the first draft of this gate produced a test that passed by
/// accident. Listing every shard makes the fan-out fail from wherever the
/// client lands, deterministically.
fn fanout_drop_mask() -> u64 {
    static GATE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *GATE.get_or_init(|| {
        std::env::var("MOON_TEST_DROP_FANOUT_TO_SHARD")
            .ok()
            .map(|v| {
                v.split(',')
                    .filter_map(|p| p.trim().parse::<u32>().ok())
                    .filter(|id| *id < 64)
                    .fold(0u64, |m, id| m | (1u64 << id))
            })
            .unwrap_or(0)
    })
}

/// Push one message to one shard, retrying a full ring with bounded
/// backpressure and honouring shutdown.
///
/// The single primitive under every fan-out push here, so a second copy of the
/// retry loop cannot drift from the first.
async fn push_bounded(
    ctx: &super::core::ConnectionContext,
    shutdown: &crate::runtime::cancel::CancellationToken,
    target: usize,
    msg: crate::shard::dispatch::ShardMessage,
) -> crate::shard::dispatch::PushOutcome {
    use crate::shard::mesh::ChannelMesh;
    use ringbuf::traits::Producer;

    if target < 64 && fanout_drop_mask() & (1u64 << target) != 0 {
        // Reported as backpressure because that is what a wedged shard looks
        // like from here: never executed, target divergent.
        return crate::shard::dispatch::PushOutcome::Backpressure;
    }
    let idx = ChannelMesh::target_index(ctx.shard_id, target);
    let mut pending = Some(msg);
    let outcome = crate::shard::dispatch::push_with_backpressure(
        shutdown,
        crate::shard::dispatch::CROSS_SHARD_PUSH_MAX_RETRIES,
        crate::shard::dispatch::CROSS_SHARD_PUSH_BACKOFF,
        || match pending.take() {
            None => true,
            Some(m) => {
                // Borrow taken+released inside each attempt, never held
                // across the backoff await.
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
    if matches!(outcome, crate::shard::dispatch::PushOutcome::Pushed) {
        ctx.spsc_notifiers[target].notify_one();
    }
    outcome
}

/// How long a whole scripting fan-out may take — pushes AND acks together.
///
/// Deliberately far shorter than [`crate::shard::dispatch::XSHARD_REPLY_TIMEOUT`]
/// (30s), and deliberately ONE budget for the whole operation rather than a
/// fresh one per shard. Both halves compound per shard if left alone, and the
/// fan-out is on the `EVAL` path, not just on cold admin verbs:
///
/// * Acks: `XSHARD_REPLY_TIMEOUT` covers a target that is EXECUTING a command
///   and may legitimately be slow. An ack covers a queue drain and a map
///   insert — a shard that has not answered in seconds is wedged, not busy.
///   Per-receiver budgets would mean 450s at `--shards 16` with a wedged mesh.
/// * Pushes: `push_bounded` retries a full ring for
///   `CROSS_SHARD_PUSH_MAX_RETRIES × CROSS_SHARD_PUSH_BACKOFF` (~0.5s) per
///   target, sequentially, BEFORE any ack is awaited. Unbounded in aggregate
///   for the same reason.
///
/// On a healthy mesh a push costs microseconds and an ack one round trip, so
/// this ceiling is never approached; it exists so a wedged shard degrades to a
/// reported divergence instead of a stalled client.
const FANOUT_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);

/// What a fan-out managed to do, from the point of view of the client waiting
/// on the command that triggered it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FanoutOutcome {
    /// Every other shard applied the op.
    Complete,
    /// At least one shard did not: the push gave up, the ack never arrived, or
    /// the shard answered that it could not apply it. `reached` counts the
    /// ones that DID apply, out of `targets`.
    Partial { reached: usize, targets: usize },
}

/// Replay one registry mutation on every OTHER shard, WAIT for each to apply
/// it, and report whether they all did.
///
/// # Why it waits
///
/// Pushing to the ring only enqueues; the target applies the message in its
/// own drain loop. Returning `+OK` at push time makes `FUNCTION LOAD` /
/// `SCRIPT LOAD` a lie for as long as that queue takes to drain: a client that
/// loads and then immediately calls — the normal thing to do — can have its
/// `FCALL` reach a shard that has not installed the library yet and get
/// `ERR Function not found` for something the server just accepted. This is
/// not theoretical; it is exactly what `sff9` caught at `--shards 4`, on the
/// FIRST key of the loop.
///
/// `FUNCTION`/`SCRIPT` verbs are cold, administrative and once-per-body, so
/// the round trip is the right trade. Waiting also makes the client-visible
/// contract match Redis's: when the command returns, the state is installed
/// server-wide.
///
/// # Why the verdict is returned rather than logged
///
/// An earlier draft swallowed a partial fan-out, told the client `+OK`, and
/// leaned on a REPAIR LEG — a later routed call that hit `NOSCRIPT` /
/// `ERR Function not found` on the target re-pushed the state from a shard
/// that had it. That is unsound, and adversarial review found two ways it
/// corrupts rather than heals:
///
/// * The error a repair triggers on is also what a shard says when it has
///   ALREADY APPLIED a `FUNCTION DELETE` that has not reached the origin yet.
///   The repair cannot tell "you never got it" from "you already dropped it",
///   so it RESURRECTS deleted libraries — and since a diverged shard keeps
///   re-seeding whichever shard owns the next key, the divergence spreads.
/// * The same mechanism un-does `SCRIPT FLUSH`, which is local-only, putting a
///   flushed body back into the cache it was just evicted from.
///
/// Fixing that with epochs means a total order over registry mutations, which
/// this design does not have. The cheap, honest alternative — now that the
/// acks exist — is to tell the client the truth and let it retry: a retried
/// `FUNCTION DELETE`/`FLUSH` is idempotent, and a retried `FUNCTION LOAD
/// REPLACE` converges. A caller that cannot act on the verdict may ignore it,
/// but nothing in this module may claim success it did not observe.
///
/// # Failure modes, all reported as [`FanoutOutcome::Partial`]
///
/// * The push gives up — the target ring stayed full past the retry budget,
///   shutdown was signalled, or [`FANOUT_BUDGET`] had already expired on an
///   earlier target. That shard never gets the message.
/// * The push lands but no ack arrives inside what is left of
///   [`FANOUT_BUDGET`]. Wedged; the op may still apply later, which is exactly
///   why the caller must not report success.
/// * The ack arrives saying `false` — the shard received the op and could not
///   apply it (no Lua runtime, a colliding function name, a digest mismatch).
///
/// All three are also loud: `warn!` plus `moon_xshard_fanout_drop_total{kind}`.
async fn fanout_to_other_shards(
    ctx: &super::core::ConnectionContext,
    shutdown: &crate::runtime::cancel::CancellationToken,
    kind: &'static str,
    mut make_msg: impl FnMut(
        crate::runtime::channel::OneshotSender<bool>,
    ) -> crate::shard::dispatch::ShardMessage,
) -> FanoutOutcome {
    let targets = ctx.num_shards.saturating_sub(1);
    let mut acks: Vec<(usize, crate::runtime::channel::OneshotReceiver<bool>)> =
        Vec::with_capacity(targets);
    let mut reached = 0usize;
    // ONE deadline for pushes and acks together. A wedged mesh must cost the
    // client a bounded wait, not (retry budget + ack budget) x N shards.
    let deadline = std::time::Instant::now() + FANOUT_BUDGET;
    for target in 0..ctx.num_shards {
        if target == ctx.shard_id {
            continue;
        }
        let (ack_tx, ack_rx) = crate::runtime::channel::oneshot();
        let outcome = if std::time::Instant::now() >= deadline {
            // Earlier targets already spent the budget. Reported, not
            // attempted: pushing here would extend the stall by another full
            // retry budget for a mesh that has already proven itself wedged.
            crate::shard::dispatch::PushOutcome::Backpressure
        } else {
            push_bounded(ctx, shutdown, target, make_msg(ack_tx)).await
        };
        if matches!(outcome, crate::shard::dispatch::PushOutcome::Pushed) {
            acks.push((target, ack_rx));
        } else {
            tracing::warn!(
                "shard {}: {kind} fan-out to shard {target} dropped ({outcome:?}); that shard is \
                 divergent until the op is re-issued",
                ctx.shard_id
            );
            crate::admin::metrics_setup::record_xshard_fanout_drop(kind);
        }
    }
    // The targets apply concurrently, so on the happy path this is a single
    // round trip, not N.
    for (target, ack_rx) in acks {
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        match crate::shard::coordinator::recv_reply_within(ack_rx, left).await {
            Ok(true) => reached += 1,
            Ok(false) => {
                // The shard already logged WHY; this side logs that the client
                // is about to be told, which is what an operator correlates.
                tracing::warn!(
                    "shard {}: {kind} fan-out was rejected by shard {target}; it is divergent \
                     until the op is re-issued",
                    ctx.shard_id
                );
                crate::admin::metrics_setup::record_xshard_fanout_drop(kind);
            }
            Err(_) => {
                tracing::warn!(
                    "shard {}: {kind} fan-out to shard {target} was pushed but not acked \
                     within the {FANOUT_BUDGET:?} fan-out budget; that shard may be wedged and \
                     is divergent until it drains or the op is re-issued",
                    ctx.shard_id
                );
                crate::admin::metrics_setup::record_xshard_fanout_drop(kind);
            }
        }
    }
    if reached == targets {
        FanoutOutcome::Complete
    } else {
        FanoutOutcome::Partial { reached, targets }
    }
}

/// Cache an `EVAL`'s body on this shard and, the FIRST time this shard sees
/// it, fan it out to the others (moon#515).
///
/// Redis caches an `EVAL`'d script server-wide, so `EVAL` once then `EVALSHA`
/// by sha is a supported — and extremely common — idiom. moon cached it only
/// in the executing shard's `ScriptCache`, so at `--shards 4` a bare `EVAL`
/// followed by `EVALSHA` on twelve keys answered `NOSCRIPT` for ten of them:
/// the ones whose key routed to a shard that had never seen the body.
/// `SCRIPT LOAD` already fanned out, which is why `register_script`-based
/// clients (`redis.lock.Lock`) never hit this.
///
/// Published once per distinct body rather than per call: N-1 SPSC pushes and
/// a cross-shard round trip on every `EVAL` would be a real cost on the
/// scripting path. The duty is claimed by `ScriptCache::claim_fanout_duty` and
/// cleared only on a COMPLETE publish, so a fan-out that fails is retried by
/// the next `EVAL` of the same body instead of being written off.
///
/// A partial publish does NOT fail the `EVAL`. The script itself runs fine —
/// only a later `EVALSHA` routed to the shard that missed the body is
/// affected, and `NOSCRIPT` is precisely the error every client library
/// already handles by re-sending the full `EVAL` (which republishes). Failing
/// the `EVAL` would trade a self-healing degradation for an outage.
///
/// Called BEFORE routing so the ordering is well defined: the fan-out and the
/// routed execution travel the same SPSC ring in that order, and the fan-out
/// is acked before the routed call is even sent.
pub(crate) async fn eval_script_fanout(
    ctx: &super::core::ConnectionContext,
    shutdown: &crate::runtime::cancel::CancellationToken,
    cmd_args: &[Frame],
) {
    if ctx.num_shards <= 1 {
        return;
    }
    // Nothing is published for a malformed EVAL: `parse_eval_args` is the same
    // gate the local handler applies, so a body that is about to be rejected
    // for a missing `numkeys` never reaches another shard's cache.
    if crate::scripting::parse_eval_args(cmd_args).is_err() {
        return;
    }
    let Some(Frame::BulkString(script)) = cmd_args.first() else {
        return;
    };
    // Borrow scoped tight: never held across the fan-out's awaits.
    let (sha1, owed) = {
        let mut cache = ctx.script_cache.borrow_mut();
        cache.claim_fanout_duty(script.clone())
    };
    if !owed {
        return;
    }
    if script_fanout_bounded(ctx, shutdown, &sha1, script).await == FanoutOutcome::Complete {
        ctx.script_cache.borrow_mut().mark_fanned_out(&sha1);
    }
}

/// Replay a `FUNCTION LOAD`/`DELETE`/`FLUSH` on every other shard (moon#514).
///
/// `FUNCTION` is a server-wide verb in Redis; in moon each shard thread owns
/// its own registry, so without this a library loaded through one connection
/// was invisible to `FCALL` on every other shard — `ERR Function not found`
/// for whichever keys did not happen to land on the loading shard.
///
/// `DELETE`/`FLUSH` fan out for the same reason in reverse: a delete that only
/// reached one shard leaves the library callable elsewhere, which is a worse
/// lie than never having deleted it.
///
/// Returns the reply the client should get INSTEAD of the local one when the
/// replay did not reach every shard. `FUNCTION` is administrative and its ops
/// are idempotent under retry (`DELETE`/`FLUSH` outright, `LOAD` when the
/// client re-sends it with `REPLACE`), so reporting the partial state is
/// strictly better than `+OK` over a registry that answers differently
/// depending on which shard a key lands on.
///
/// # Known limitation: no total order
///
/// Each origin shard replays on its own set of rings, so two registry
/// mutations issued CONCURRENTLY from connections on different shards can
/// apply in different orders on different shards, and nothing reconciles them
/// afterwards. A `FUNCTION LOAD` on shard 0 racing a `FUNCTION FLUSH` on shard
/// 3 can leave the library present on some shards and absent on others, with
/// both clients told `+OK`. Fixing it needs an ordering authority — routing
/// every registry mutation through one designated shard — which is a larger
/// change than this fix and is tracked separately. Until then: issue
/// `FUNCTION` mutations from one connection at a time.
#[must_use]
pub(crate) async fn function_registry_fanout(
    ctx: &super::core::ConnectionContext,
    shutdown: &crate::runtime::cancel::CancellationToken,
    op: crate::scripting::FunctionRegistryOp,
) -> Option<Frame> {
    if ctx.num_shards <= 1 {
        return None;
    }
    let verb = match op {
        crate::scripting::FunctionRegistryOp::Load { .. } => "LOAD",
        crate::scripting::FunctionRegistryOp::Delete { .. } => "DELETE",
        crate::scripting::FunctionRegistryOp::Flush => "FLUSH",
    };
    match fanout_to_other_shards(ctx, shutdown, "function_op", |ack| {
        crate::shard::dispatch::ShardMessage::FunctionRegistry {
            op: op.clone(),
            ack: Some(ack),
        }
    })
    .await
    {
        FanoutOutcome::Complete => None,
        FanoutOutcome::Partial { reached, targets } => Some(Frame::Error(Bytes::from(format!(
            "MOONERR partialfanout FUNCTION {verb} applied on {} of {} shards; re-issue it \
             (LOAD needs REPLACE) to converge",
            reached + 1,
            targets + 1,
        )))),
    }
}

/// The fan-out replay a `FUNCTION` invocation owes the other shards, if any.
///
/// Returns `None` for read-only subcommands (`LIST`, `DUMP`, `STATS`) and for
/// any invocation the local registry rejected — replaying a failed mutation
/// would diverge the very shards it is meant to keep in step.
pub(crate) fn function_fanout_op(
    cmd_args: &[Frame],
    response: &Frame,
) -> Option<crate::scripting::FunctionRegistryOp> {
    if matches!(response, Frame::Error(_)) {
        return None;
    }
    let Some(Frame::BulkString(sub)) = cmd_args.first() else {
        return None;
    };
    if sub.eq_ignore_ascii_case(b"LOAD") {
        // FUNCTION LOAD [REPLACE] <body> — the body is the LAST argument in
        // both arities, and `handle_function_load` has already rejected every
        // other shape by the time we see a non-error response.
        let Some(Frame::BulkString(body)) = cmd_args.last() else {
            return None;
        };
        Some(crate::scripting::FunctionRegistryOp::Load {
            source: body.clone(),
        })
    } else if sub.eq_ignore_ascii_case(b"DELETE") {
        let Some(Frame::BulkString(lib)) = cmd_args.get(1) else {
            return None;
        };
        Some(crate::scripting::FunctionRegistryOp::Delete {
            library: lib.clone(),
        })
    } else if sub.eq_ignore_ascii_case(b"FLUSH") {
        Some(crate::scripting::FunctionRegistryOp::Flush)
    } else {
        None
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
        (8, b'r') => cmd.eq_ignore_ascii_case(b"REPLCONF") || cmd.eq_ignore_ascii_case(b"READONLY"),
        (9, b'r') => {
            cmd.eq_ignore_ascii_case(b"REPLICAOF") || cmd.eq_ignore_ascii_case(b"READWRITE")
        }
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
    // MEMORY USAGE <key> [SAMPLES n]: the key is args[1]. Without this arm
    // MEMORY fell through to "args[0] is the key" and hashed the literal
    // "USAGE" — one fixed shard for every key — so an existing key read as
    // absent unless it happened to live on that shard (moon#511; 22/24 wrong
    // at --shards 4, the 1-1/shards signature of a constant route).
    //
    // Matched on the SUBCOMMAND rather than blindly taking args[1], because
    // USAGE is the only MEMORY subcommand that takes a key: DOCTOR, STATS,
    // PURGE, MALLOC-STATS and HELP are keyless and must stay local, and a
    // blind args[1] would hash a future subcommand's first OPTION as a key.
    if (len, b0) == (6, b'm') && cmd.eq_ignore_ascii_case(b"MEMORY") {
        return match (args.first(), args.get(1)) {
            (Some(Frame::BulkString(sub)), Some(Frame::BulkString(key)))
                if sub.eq_ignore_ascii_case(b"USAGE") =>
            {
                Some(key)
            }
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
    // LMPOP/ZMPOP numkeys key [key ...] <LEFT|RIGHT|MIN|MAX>, and
    // SINTERCARD numkeys key [key ...]: args[0] is the numkeys LITERAL and the
    // first real key is args[1] — the same layout as ZDIFF above.
    //
    // Routing by args[0] hashed the count, so every invocation landed on one
    // fixed shard and a populated key read as EMPTY from every other shard —
    // `*-1` for the two MPOPs, `:0` for SINTERCARD, neither distinguishable
    // from a key that really is empty (moon#534).
    if (len == 5
        && ((b0 == b'l' && cmd.eq_ignore_ascii_case(b"LMPOP"))
            || (b0 == b'z' && cmd.eq_ignore_ascii_case(b"ZMPOP"))))
        || (len == 10 && b0 == b's' && cmd.eq_ignore_ascii_case(b"SINTERCARD"))
    {
        return match args.get(1) {
            Some(Frame::BulkString(key)) => Some(key),
            _ => None,
        };
    }
    // BLMPOP/BZMPOP timeout numkeys key [key ...] <dir>: args[0] is the
    // TIMEOUT and args[1] the numkeys, so the first real key is args[2].
    //
    // The blocking decision itself does NOT come through here —
    // `handle_blocking_command_*` extracts these keys directly, which is why
    // BLMPOP routes correctly today. This arm exists for the other consumers:
    // cluster slot resolution asks the same function, and hashing "0.05"
    // there yields a wrong slot and therefore a wrong MOVED target.
    if len == 6
        && b0 == b'b'
        && (cmd.eq_ignore_ascii_case(b"BLMPOP") || cmd.eq_ignore_ascii_case(b"BZMPOP"))
    {
        return match args.get(2) {
            Some(Frame::BulkString(key)) => Some(key),
            _ => None,
        };
    }
    // XREAD  [COUNT n] [BLOCK ms] STREAMS key [key ...] id [id ...]
    // XREADGROUP GROUP g c [COUNT n] [BLOCK ms] STREAMS key [key ...] id [...]
    // Scan args for the STREAMS token; the key immediately follows it.
    // No allocation — scan &[Frame] linearly.
    //
    // XREADGROUP is matched here rather than left to the fallthrough because
    // its args[0] is the literal token "GROUP": one hash, one shard, for every
    // stream on the server. It answered "-ERR ... requires the key to exist"
    // for every key that shard did not own (moon#533). It is 10 bytes, so the
    // original `len == 5` guard could never have caught it even though the
    // comment above it named both commands.
    if b0 == b'x'
        && ((len == 5 && cmd.eq_ignore_ascii_case(b"XREAD"))
            || (len == 10 && cmd.eq_ignore_ascii_case(b"XREADGROUP")))
    {
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

/// Must this command wait for the batch's already-deferred remote commands to
/// land before it may execute? (moon#507)
///
/// The sharded pipeline handlers DEFER a single-key command whose key lives on
/// another shard into `remote_groups`, dispatching the whole group as one
/// `PipelineBatchSlotted` at the end of the batch. Anything that executes
/// INLINE in the meantime runs against a shard whose earlier writes in the same
/// batch have not been sent yet — so it reads state the client already wrote,
/// or writes state the pending command then overwrites. Measured at
/// `--shards 2`: `SET a` + `MSET a` in one batch lost the MSET's value in 7 of
/// 20 trials, and `SET` + `FLUSHALL` left the key alive in 10 of 20.
///
/// The safe case is narrow and worth stating positively, because it is what
/// makes the rest of the pipeline fast: a command routed by its OWN single key
/// needs no wait. A key maps to exactly one shard, so if that shard is local
/// the key cannot be in `remote_groups` at all, and if it is remote the command
/// is appended BEHIND the pending ones on the same target and the slotted batch
/// preserves order. That is why `SET k` + `GET k` and `SET k` + `TYPE k` were
/// always correct while `MGET` was not.
///
/// Everything else waits. Three ways a command fails to be "routed by its own
/// single key":
///
/// 1. it is multi-key (MGET/MSET/DEL/EXISTS/…) — consumed by the cross-shard
///    coordinator before routing;
/// 2. it is keyless (`extract_primary_key` → `None`) — DBSIZE, KEYS, SCAN,
///    RANDOMKEY, FLUSHALL, INFO … all aggregate across shards inline. None of
///    these is a "multi-key command" in the registry sense, which is why this
///    predicate is keyed on ROUTABILITY rather than on a list of command names;
/// 3. it is intercepted inline by a `try_handle_*` handler BEFORE routing runs,
///    even though it does have an args[0] that `extract_primary_key` would
///    happily hash. That is the one case routability cannot see, so those
///    families are named in [`is_inline_intercepted`].
///
/// Deferring is conservative: a command wrongly sent down this path is merely
/// executed at the start of the next batch, which is always correct and costs
/// one batch boundary. Wrongly calling something SAFE is the direction that
/// corrupts data, so when in doubt, add it to the wait set.
pub(crate) fn must_wait_for_pending_remote(cmd: &[u8], args: &[Frame]) -> bool {
    is_multi_key_command(cmd, args)
        || is_inline_intercepted(cmd)
        || extract_primary_key(cmd, args).is_none()
}

/// Commands handled INLINE by a `try_handle_*` interceptor before the routing
/// step, and which `extract_primary_key` would nonetheless answer for.
///
/// Derived by reading the interceptor chain in `handler_monoio::dispatch` /
/// `handler_sharded`, not guessed: every other interceptor there guards a
/// command that `extract_primary_key` already reports keyless (AUTH, HELLO,
/// CLUSTER, CONFIG, CLIENT, INFO, WAIT, SELECT, KEYS, SCAN, DBSIZE, HOTKEYS,
/// the persistence verbs …), so those are caught by the keyless arm.
///
/// **Adding a new inline interceptor means adding its command here.** A new
/// interceptor for a command with a key-shaped first argument would silently
/// re-open moon#507 for that command.
/// `pco10_inline_intercepted_commands_see_their_own_batch` in
/// `tests/pipeline_cross_shard_ordering.rs` drives EVAL and SWAPDB — the two
/// entries that touch real keys — and fails if either is dropped from this
/// list. It cannot prove the list is COMPLETE against a future interceptor;
/// that is why the doc above says to err toward waiting.
fn is_inline_intercepted(cmd: &[u8]) -> bool {
    // Dotted families first, and deliberately so: a length-keyed match below
    // would swallow `FT.ALIAS` (8 bytes, 'f') into the FCALL_RO/FUNCTION arm
    // and answer false for it.
    const DOTTED: [&[u8]; 4] = [b"FT.", b"GRAPH.", b"CDC.", b"TS."];
    if DOTTED
        .iter()
        .any(|p| cmd.len() > p.len() && cmd[..p.len()].eq_ignore_ascii_case(p))
    {
        return true;
    }
    let len = cmd.len();
    if len == 0 {
        return false;
    }
    let b0 = cmd[0] | 0x20;
    match (len, b0) {
        // Lua and functions read and write real keys through the interceptor,
        // never through routing.
        (4, b'e') => cmd.eq_ignore_ascii_case(b"EVAL"),
        // `EVAL_RO` is also 7 bytes, so this arm answers for both.
        (7, b'e') => cmd.eq_ignore_ascii_case(b"EVALSHA") || cmd.eq_ignore_ascii_case(b"EVAL_RO"),
        (10, b'e') => cmd.eq_ignore_ascii_case(b"EVALSHA_RO"),
        (5, b'f') => cmd.eq_ignore_ascii_case(b"FCALL"),
        (8, b'f') => cmd.eq_ignore_ascii_case(b"FCALL_RO") || cmd.eq_ignore_ascii_case(b"FUNCTION"),
        // SWAPDB exchanges whole databases across every shard.
        // SCRIPT/ACL touch no keyspace data, but they are inline and cost
        // nothing to serialise behind pending writes.
        (6, b's') => cmd.eq_ignore_ascii_case(b"SCRIPT") || cmd.eq_ignore_ascii_case(b"SWAPDB"),
        (3, b'a') => cmd.eq_ignore_ascii_case(b"ACL"),
        // Container commands for the message-queue and workspace stores.
        (2, b'm') => cmd.eq_ignore_ascii_case(b"MQ"),
        (2, b'w') => cmd.eq_ignore_ascii_case(b"WS"),
        _ => false,
    }
}

/// If this script's keys all live on ANOTHER shard, run it there and return
/// that shard's reply. `None` means "run it here" — no keys, one shard, or the
/// keys are already local.
///
/// This is the moon#508 fix. Previously any script whose keys were not on the
/// connection's own shard was answered `CROSSSLOT`, which is only true when the
/// keys span shards — a single key never crosses anything, it just lives
/// somewhere else.
///
/// A genuinely cross-shard key set still returns `CROSSSLOT` here, before the
/// hop: a script executes against one shard's database and cannot reach
/// another's, so there is no target to send it to.
pub(crate) async fn route_script_elsewhere(
    cmd: &[u8],
    cmd_args: &[Frame],
    db_index: usize,
    // moon#569: the caller's ACL identity travels with the routed script so
    // the target shard authorizes each `redis.call` exactly as the origin
    // shard would have.
    script_acl: &crate::acl::ScriptAcl,
    ctx: &crate::server::conn::core::ConnectionContext,
) -> Option<Frame> {
    if ctx.num_shards <= 1 {
        return None;
    }
    // EVALSHA carries a sha where EVAL carries the body, but `parse_eval_args`
    // only reads args[1..] for numkeys/keys — so the same parse serves both and
    // the sha never has to be resolved just to decide where to run.
    let keys = match crate::scripting::parse_eval_args(cmd_args) {
        // Malformed args: let the local handler produce the exact error it
        // always did rather than inventing a routing-flavoured one here.
        Err(_) => return None,
        Ok((_script, _numkeys, keys, _argv)) => keys,
    };
    match crate::scripting::route_script_keys(&keys, ctx.shard_id, ctx.num_shards) {
        crate::scripting::ScriptRoute::Local => None,
        crate::scripting::ScriptRoute::CrossShard => Some(Frame::Error(Bytes::from_static(
            b"CROSSSLOT Keys in script don't hash to the same slot and shard",
        ))),
        crate::scripting::ScriptRoute::Remote(target) => {
            let mut parts = Vec::with_capacity(cmd_args.len() + 1);
            parts.push(Frame::BulkString(Bytes::copy_from_slice(cmd)));
            parts.extend_from_slice(cmd_args);
            let command = std::sync::Arc::new(Frame::Array(parts.into()));
            let reply = crate::shard::coordinator::coordinate_script(
                command,
                target,
                ctx.shard_id,
                db_index,
                // moon#569/#597: the caller's ACL travels with the routed
                // script. It now covers `FCALL`/`FCALL_RO` too, which reach
                // this helper for the first time in moon#514 — a routed
                // function authorizes each `redis.call` as the origin would.
                script_acl.clone(),
                &ctx.dispatch_tx,
                &ctx.spsc_notifiers,
            )
            .await;
            // The target's reply is returned VERBATIM. There is deliberately
            // no repair leg here — see `fanout_to_other_shards` for why
            // "target says NOSCRIPT / Function not found" is not evidence of a
            // fan-out gap and cannot be acted on without resurrecting deleted
            // state. Divergence is prevented at publish time (acked fan-out)
            // and reported to the client, not patched up after the fact.
            Some(reply)
        }
    }
}

// ---------------------------------------------------------------------------
// Cross-shard routing rule for the two-key WRITE family (moon#592)
// ---------------------------------------------------------------------------

/// The reply a two-key write owes its client when its keys are owned by more
/// than one shard.
///
/// Keeps the `CROSSSLOT` prefix every Redis client already recognises as
/// "co-locate these keys", and names the remedy moon actually supports.
pub(crate) const CROSS_SHARD_WRITE_ERROR: &[u8] =
    b"CROSSSLOT Keys in request don't hash to the same shard; \
      co-locate every key of the command with a {hash} tag";

/// Commands whose argv names a key the command does NOT route on.
///
/// Named for what it detects, not for writes alone: `XREAD` is read-only and
/// belongs here for the same reason `SINTERSTORE`'s remote SOURCE does — the
/// routed shard executes the whole command against one slice, so a key it
/// cannot see reads as absent.
///
/// `extract_primary_key` answers the command's `first_key` — the ROUTING key.
/// It is not "the key this command writes", and for this family it is not even
/// all of them. Reading it as the whole truth is the root of moon#592.
///
/// Every name here writes, or reads, at least one key beyond the one routing
/// picks. Deliberately EXCLUDED, each for a reason that must survive review:
///
/// * `MGET`/`MSET`/`MSETNX`/`DEL`/`UNLINK`/`EXISTS`/`BITOP`/`COPY` —
///   `is_multi_key_command` sends these to `shard::coordinator`, which groups
///   keys by owner and dispatches a leg to each. They are already correct and
///   must not start erroring.
/// * `LMOVE`/`RPOPLPUSH`/`BLMOVE`/`BRPOPLPUSH` — the same defect, owned by
///   moon#570 / PR #591, which additionally has to guard the two blocking
///   entry points (`blocking::immediate_scan`, `blocking::wakeup`) that this
///   pre-routing guard cannot see. Two overlapping guards for one family would
///   be worse than one complete one.
/// * `ZDIFFSTORE` — not implemented in moon (unknown command), so there is
///   no write to misplace, and claiming `CROSSSLOT` would send a client
///   chasing hash tags for a command that will never work.
///   `tests/two_key_write_cross_shard.rs::t2k4` fails the moment it starts
///   working, which is when it must be added here. `GEORADIUS`/
///   `GEORADIUSBYMEMBER` used to sit in this same bucket; moon#645
///   implemented their `STORE`/`STOREDIST` clause, so they moved INTO the
///   family below in the same change that made them able to write.
/// * The read-only multi-key commands (`SINTER`, `SUNION`, `SDIFF`, `ZDIFF`,
///   `ZINTER`, `ZUNION`, `ZINTERCARD`, `SINTERCARD`, `LCS`, `PFCOUNT`,
///   `TOUCH`, `LMPOP`, `ZMPOP`) — same routing rule, but the consequence is a
///   silently wrong ANSWER, never destroyed data. A separate decision with a
///   different blast radius; see the PR body.
///
/// Matched on `(len, first byte)` first so a single-key command falls through
/// after one integer compare and never reaches the key walk.
fn touches_a_key_it_did_not_route_on(cmd: &[u8]) -> bool {
    let len = cmd.len();
    if len == 0 {
        return false;
    }
    match (len, cmd[0] | 0x20) {
        (6, b'r') => cmd.eq_ignore_ascii_case(b"RENAME"),
        (8, b'r') => cmd.eq_ignore_ascii_case(b"RENAMENX"),
        (5, b's') => cmd.eq_ignore_ascii_case(b"SMOVE"),
        // `SORT src ... STORE dst`. Without a STORE clause the walker reports
        // one key and the check is a no-op, so no read-only SORT is affected.
        (4, b's') => cmd.eq_ignore_ascii_case(b"SORT"),
        (10, b's') => cmd.eq_ignore_ascii_case(b"SDIFFSTORE"),
        (11, b's') => {
            cmd.eq_ignore_ascii_case(b"SINTERSTORE") || cmd.eq_ignore_ascii_case(b"SUNIONSTORE")
        }
        (11, b'z') => {
            cmd.eq_ignore_ascii_case(b"ZRANGESTORE")
                || cmd.eq_ignore_ascii_case(b"ZUNIONSTORE")
                || cmd.eq_ignore_ascii_case(b"ZINTERSTORE")
        }
        (7, b'p') => cmd.eq_ignore_ascii_case(b"PFMERGE"),
        (14, b'g') => cmd.eq_ignore_ascii_case(b"GEOSEARCHSTORE"),
        // `GEORADIUS src ... STORE|STOREDIST dst` (moon#645). Without the
        // clause the walker reports one key and this check is a no-op, so no
        // read-only GEORADIUS is affected — exactly like `SORT` above. The
        // `_RO` twins never reach here: they reject the clause outright and
        // so can only ever name one key.
        (9, b'g') => cmd.eq_ignore_ascii_case(b"GEORADIUS"),
        (17, b'g') => cmd.eq_ignore_ascii_case(b"GEORADIUSBYMEMBER"),
        // moon#605. These two READ several streams, and the walker reports one
        // key per stream — so a single-stream `XREAD`/`XREADGROUP` is a no-op
        // here (one key cannot disagree with itself) and only a multi-stream
        // invocation can be refused.
        //
        // `XREAD` alone would be an under-read: the routed shard answers from
        // the streams it owns and reports nothing for the rest, which the
        // client reads as "those are quiet". `XREADGROUP` is worse, and is why
        // this is not filed as a cosmetic gap — measured at `--shards 4`, 12
        // pairs:
        //
        // ```text
        // XREADGROUP GROUP g cc COUNT 10 STREAMS a b > >
        //   -> -ERR The XREADGROUP subcommand requires the key to exist.
        //   ... and `a`'s entry is now PENDING for consumer `cc`
        // ```
        //
        // `read_group_new` claims the local stream's entries into the PEL and
        // THEN the command fails on the stream this shard cannot see. The
        // client is handed an error and never receives the entry, but the
        // entry is marked delivered-and-unacked: `XREADGROUP >` will never
        // return it again and only `XPENDING`/`XAUTOCLAIM` recovers it. 10 of
        // 12 pairs stranded an entry that way.
        (5, b'x') => cmd.eq_ignore_ascii_case(b"XREAD"),
        (10, b'x') => cmd.eq_ignore_ascii_case(b"XREADGROUP"),
        _ => false,
    }
}

/// moon#592: the refusal a two-key write is owed when its keys do not all
/// belong to one shard.
///
/// # The defect
///
/// moon is shared-nothing across shards. A command is routed to ONE shard —
/// the owner of `extract_primary_key`'s answer — and that shard executes the
/// whole command against its own slice. For this family the other key was
/// therefore read from, and written to, the ROUTING key's slice, under the
/// right name but in the wrong table. Every normally-routed access of that key
/// goes to its real owner and is blind to it. The client was told the write
/// succeeded:
///
/// ```text
/// SET alpha VALUE-1      -> +OK
/// RENAME alpha omega     -> +OK      (acked)
/// GET alpha              -> nil      (source destroyed)
/// GET omega              -> nil      (destination never written)
/// ```
///
/// Measured at `--shards 4` on the pre-fix binary, 12 constructed cross-shard
/// placements per command: 12/12 lost for all twelve commands in the family.
///
/// # Why refuse rather than hop
///
/// Refusing is the only answer that cannot lose data: it is decided from the
/// key NAMES alone, before anything is read, written or deleted, so there is
/// no window in which the data exists in neither place, no undo to get wrong
/// under a race or a timeout, and no partial state to recover after a crash. A
/// shard hop would trade the loss for a non-atomic `RENAME` — an intermediate
/// state Redis never exposes — plus two independent AOF records with no shared
/// commit point.
///
/// It is also the answer moon already gives everywhere else it cannot act
/// atomically across shards: a MULTI/EXEC body spanning shards (see
/// [`analyze_txn_locality`], which has classified exactly these key sets
/// correctly since prod-hardening #15), a script spanning shards, and
/// `MSETNX`. `{hash}` tags collapse the keys onto one shard and the command
/// then works exactly as it does at `--shards 1`.
///
/// # Failure design
///
/// There is no I/O, no hop, no timeout and no rollback on this path — that is
/// the point. The decision is a pure function of the argv and the shard count,
/// so a slow or dead destination shard, a dropped connection mid-command, or a
/// crash at any instant all leave the keyspace exactly as it was. The only
/// recovery story a client needs is the one in the error text.
///
/// # Contract
///
/// Returns `None` — leaving the command to run, and to produce its own error
/// if it has one — when:
///
/// * the server has one shard (nothing to cross);
/// * the command is not in this family;
/// * the argv is malformed (bad `numkeys`, a `STORE` with no destination, too
///   few arguments), so it still earns its own arity/syntax error rather than
///   a misleading `CROSSSLOT`;
/// * every key resolves to the same shard — including the degenerate
///   `RENAME k k` and any `{hash}`-tagged pair.
#[must_use]
pub(crate) fn cross_shard_multikey_rejection(
    cmd: &[u8],
    args: &[Frame],
    num_shards: usize,
) -> Option<Frame> {
    if num_shards <= 1 || !touches_a_key_it_did_not_route_on(cmd) {
        return None;
    }
    // The shared key-position walker (moon#582) — the same one ACL and cache
    // invalidation use, so `SORT ... STORE dst` and `ZUNIONSTORE dst numkeys
    // ...` are enumerated by the code that already knows those layouts rather
    // than by a second, drifting copy here.
    //
    // `AtPlusComputed` is `SORT ... BY w_*`: the weight keys cannot be named
    // by anyone, but the ones that CAN be named — source and STORE
    // destination — are exactly the pair this guard exists for, so they are
    // still checked. (The unnameable reads are a separate, read-only defect;
    // see the PR body.)
    let idx = match crate::acl::keyspec::command_key_positions(cmd, args) {
        crate::acl::keyspec::KeyPositions::At(idx)
        | crate::acl::keyspec::KeyPositions::AtPlusComputed(idx) => idx,
        crate::acl::keyspec::KeyPositions::None | crate::acl::keyspec::KeyPositions::Unknown => {
            return None;
        }
    };
    let mut owner: Option<usize> = None;
    // Every position, whatever its `KeyRole` (moon#537 gave the walker
    // read/write roles). The routed shard executes the WHOLE command against
    // its own slice, so a remote READ source is just as broken as a remote
    // write target: `SINTERSTORE dst src` reads an empty `src` and stores a
    // confidently wrong answer. Co-location is required for both.
    for k in idx {
        // Borrowed, not cloned: this runs before routing on every command in
        // the family, and a key position holding a non-string is a malformed
        // invocation — let the command reject it in its own words.
        let key: &[u8] = match args.get(k.idx) {
            Some(Frame::BulkString(b) | Frame::SimpleString(b)) => b.as_ref(),
            _ => return None,
        };
        let shard = crate::shard::dispatch::key_to_shard(key, num_shards);
        match owner {
            None => owner = Some(shard),
            Some(existing) if existing == shard => {}
            Some(_) => return Some(Frame::Error(Bytes::from_static(CROSS_SHARD_WRITE_ERROR))),
        }
    }
    None
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

/// moon#570: the refusal a non-blocking list MOVE is owed when its two keys
/// are owned by two different shards.
///
/// `LMOVE`/`RPOPLPUSH` are routed by their PRIMARY key — the source — and then
/// executed whole on that one shard, so the push half wrote the element into
/// the source owner's slice under the destination's name. Every normally
/// -routed read of the destination goes to the DESTINATION's owner and is
/// blind to it: measured at `--shards 4`, 11 of 12 key placements returned the
/// moved element to the client and left it nowhere in the keyspace.
///
/// The two blocking twins (`BLMOVE`/`BRPOPLPUSH`) are refused by the same rule
/// one layer up, in `blocking::immediate_scan`, before they can register a
/// waiter — see `command::list::cross_shard_move_refusal` for why refusing is
/// the only answer that cannot lose the element.
///
/// Deliberately NOT routed through `coordinate_multi_key` like `COPY`: the
/// co-located case works correctly today via ordinary primary-key routing,
/// and moving it onto the coordinator would swap a proven persistence path
/// for a different one to fix a case that is not broken. This guard changes
/// nothing except which commands are refused.
///
/// Returns `None` for a malformed argv so the command still earns its own
/// arity error rather than a misleading `CROSSSLOT`.
pub(crate) fn cross_shard_move_rejection(
    cmd: &[u8],
    args: &[Frame],
    num_shards: usize,
) -> Option<Frame> {
    if num_shards <= 1 {
        return None;
    }
    // `LMOVE source destination LEFT|RIGHT LEFT|RIGHT` (arity 5) and
    // `RPOPLPUSH source destination` (arity 3), minus the command name.
    let want_args = if cmd.eq_ignore_ascii_case(b"LMOVE") {
        4
    } else if cmd.eq_ignore_ascii_case(b"RPOPLPUSH") {
        2
    } else {
        return None;
    };
    if args.len() != want_args {
        return None;
    }
    let source = super::util::extract_bytes(args.first()?)?;
    let destination = super::util::extract_bytes(args.get(1)?)?;
    crate::command::list::cross_shard_move_refusal(&source, &destination, num_shards)
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

/// `FUNCTION` run from inside `EXEC` — moon#697.
///
/// Mirrors the live path (`try_handle_function_command`) rather than calling it:
/// that one returns early when `conn.in_multi`, which is precisely the state
/// this runs in.
///
/// The fan-out is NOT optional. The registry is per-shard-thread — its `RefCell`
/// is shared with that thread's SPSC drain loop, which applies inbound fan-outs
/// — so a `FUNCTION LOAD` that skipped the broadcast would land on ONE shard and
/// still answer with the library name. `fim4` is the test that catches it, and
/// only at `--shards 4`.
pub(crate) async fn try_handle_function_in_txn(
    cmd: &[u8],
    cmd_args: &[Frame],
    ctx: &super::core::ConnectionContext,
    shutdown: &crate::runtime::cancel::CancellationToken,
    func_registry: &std::rc::Rc<std::cell::RefCell<Option<crate::scripting::FunctionRegistry>>>,
    out: &mut Vec<Frame>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"FUNCTION") {
        return false;
    }
    crate::server::conn::core::ensure_function_registry(func_registry, ctx);
    // Borrow scoped and released BEFORE the fan-out await: the drain loop needs
    // this cell to apply arriving libraries, and `await_holding_refcell_ref`
    // rejects holding it across a yield even with a trailing `drop`.
    let mut response = {
        let mut guard = func_registry.borrow_mut();
        #[allow(clippy::unwrap_used)]
        // ensure_function_registry guarantees Some
        crate::command::functions::handle_function(guard.as_mut().unwrap(), cmd_args)
    };
    if let Some(op) = function_fanout_op(cmd_args, &response)
        && let Some(partial) = function_registry_fanout(ctx, shutdown, op).await
    {
        // A replay that did not reach every shard REPLACES the local reply, for
        // the same reason it does on the live path: a `+OK` over a registry that
        // answers differently per shard is the defect, not a shape to preserve.
        response = partial;
    }
    out.push(response);
    true
}

/// Commands that queue inside `MULTI` but are executed by the CONNECTION at
/// `EXEC` time rather than by the transaction executor.
///
/// These are connection-level *intercepts*: they never reach `dispatch()`, so
/// the executor cannot replay them the way it replays a keyspace command. They
/// also touch no keyspace data, so they do not need the executor's database
/// lock — what they need is the `ConnectionState` / `ConnectionContext` the
/// executor does not have (and, on a routed body, is not even on the same
/// thread as).
///
/// The executor pushes a placeholder into the result array for each of these
/// and the caller overwrites that slot with the real reply, so the array keeps
/// one slot per queued command, in queue order. See `run_txn_intercepts`.
///
/// **Ordering divergence, deliberate and bounded** (moon#639): the placeholders
/// are filled AFTER the keyspace body commits, not interleaved with it. Redis
/// runs the whole queue in order. Doing the same here would mean either running
/// connection-level code inside the executor's database lock or on a shard
/// thread that has no connection, so the side effects of these commands land
/// after the body's. The client-visible array shape and order are unaffected,
/// and running them after the body is what makes an aborted `EXEC` (a `WATCH`
/// conflict) skip them entirely — which is the property worth having.
///
/// Before moon#639 these commands did not queue at all: they executed at QUEUE
/// time, delivered their reply mid-transaction, and left no slot in the `EXEC`
/// array — so `MULTI / CONFIG SET maxmemory 0 / DISCARD` applied the change the
/// client had revoked, and a client that queued three commands read `*0`.
///
/// `AUTH` and `HELLO` are members even though they are intercepted far above
/// the queue gate (they must be, to work unauthenticated). Their intercepts
/// skip themselves while `conn.in_multi`, which is safe because being in a
/// transaction already implies being authenticated.
///
/// `FUNCTION` joined in moon#697. Its absence was not a design choice: the
/// executor fell through to the keyspace `dispatch()`, which has no FUNCTION
/// arm, and answered `ERR unknown command 'FUNCTION'` — breaking this list's own
/// "queueable iff dispatchable" invariant while claiming the command did not
/// exist. Its handler needs the per-shard-thread registry, so that travels down
/// to the filler with it (`try_handle_function_in_txn`).
pub(crate) fn is_txn_connection_intercept(cmd: &[u8]) -> bool {
    const CONNECTION_INTERCEPTS: [&[u8]; 10] = [
        b"CONFIG",
        b"CLIENT",
        b"ACL",
        b"CLUSTER",
        b"SCRIPT",
        b"WAIT",
        b"PUBSUB",
        b"AUTH",
        b"HELLO",
        b"FUNCTION",
    ];
    CONNECTION_INTERCEPTS
        .iter()
        .any(|c| cmd.eq_ignore_ascii_case(c))
}

/// Snapshot the queue for [`fill_txn_intercept_slots`], or an empty `Vec` when
/// the body holds no connection-level intercept (moon#639).
///
/// The filler needs the queue AND `&mut conn`, and the queue lives on the
/// connection — so it has to be copied out first. Copying only when there is
/// something to fill keeps an ordinary `EXEC` allocation-free on this path.
///
/// [`fill_txn_intercept_slots`]: crate::server::conn::handler_monoio::dispatch::fill_txn_intercept_slots
#[cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
pub(crate) fn txn_intercept_snapshot(queue: &[Frame]) -> Vec<Frame> {
    if queue
        .iter()
        .filter_map(crate::server::conn::util::extract_command)
        .any(|(c, _)| is_txn_connection_intercept(c))
    {
        queue.to_vec()
    } else {
        Vec::new()
    }
}

/// The placeholder the executor leaves for a [`is_txn_connection_intercept`]
/// command, for the caller to overwrite. Never reaches a client: `EXEC` on any
/// path either fills every placeholder or aborts the whole array.
pub(crate) const TXN_INTERCEPT_PLACEHOLDER: Frame = Frame::Null;

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

    // moon#670: a container's SUBCOMMAND is validated here too, because Redis
    // validates it here.
    //
    // ```text
    // MULTI / CONFIG BOGUS / SET k v / EXEC
    //   redis -> -ERR unknown subcommand … then -EXECABORT   (nothing ran)
    //   moon  -> +QUEUED                 then *2             (the SET ran)
    // ```
    //
    // Measured across all fourteen containers on redis-server 8.6.1
    // (2026-08-24): every one refuses at queue time and poisons the block. A
    // client that treats `+QUEUED` as "this command is valid" — which is what
    // Redis guarantees — sends the rest of a transaction Redis would have
    // refused wholesale, then applies the partial result.
    //
    // `is_known_subcommand` is the SAME predicate each container's own dispatch
    // guard consults, which is what keeps this half of the gate inside the
    // queueable-iff-dispatchable contract stated above. Reading the raw
    // `SUBCOMMAND_META` here instead would break it: that table is a
    // publication contract and omits `FUNCTION DUMP`, which dispatch accepts.
    if let Some(container) = gated_container(cmd)
        && let Some(sub) = args.first().and_then(|f| match f {
            Frame::BulkString(b) | Frame::SimpleString(b) => Some(b.as_ref()),
            _ => None,
        })
        && !crate::command::metadata::is_known_subcommand(container.as_bytes(), sub)
    {
        return Some(crate::command::helpers::err_unknown_subcommand(
            container, sub,
        ));
    }
    None
}

/// The containers whose subcommands are validated at `MULTI` queue time.
///
/// Returns the canonical uppercase name, which is also what the error echoes.
///
/// One container Moon has a subcommand table for is deliberately ABSENT, and the
/// absence is fenced by a test in `tests/container_subcommand_parity_670.rs`
/// rather than left to this comment:
///
///   * `CLUSTER` — with cluster support disabled every CLUSTER subcommand,
///     bogus ones included, is answered "This instance has cluster support
///     disabled". Dispatch never reports an unknown subcommand, so a gate that
///     did would refuse to queue what dispatch would happily have run (`csp7`).
///
/// `FUNCTION` was a second absence until moon#697: its `EXEC` executor answered
/// `ERR unknown command 'FUNCTION'` for every subcommand, so there was no agreed
/// notion of "known" to gate on. It dispatches now, so it is gated, and `csp6`
/// asserts that rather than fencing its absence.
fn gated_container(cmd: &[u8]) -> Option<&'static str> {
    // moon#697 added FUNCTION. Until then its EXEC executor answered `unknown
    // command` for EVERY subcommand, so the gate's notion of a known subcommand
    // and the executor's disagreed, and gating it would have swapped one
    // queued-vs-dispatched divergence for another. FUNCTION now dispatches
    // inside MULTI, so the "queueable iff dispatchable" invariant holds for it
    // and it joins the other twelve.
    const GATED: &[&str] = &[
        "ACL", "CLIENT", "COMMAND", "CONFIG", "FUNCTION", "MEMORY", "MODULE", "OBJECT", "PUBSUB",
        "SCRIPT", "SLOWLOG", "XGROUP", "XINFO",
    ];
    GATED
        .iter()
        .find(|c| c.as_bytes().eq_ignore_ascii_case(cmd))
        .copied()
}

/// Rebuild an argv frame with `args` in place of everything after the command
/// name.
///
/// moon#702: the MULTI queue stores FRAMES and `EXEC` replays them — it never
/// sees the workspace-rewritten `cmd_args` the surrounding dispatch shadows in.
/// A raw frame queued at the gate therefore reached the keyspace with no
/// `{ws_hex}:` prefix on any key, so a workspace-bound `MULTI` addressed the
/// GLOBAL keyspace and could name any OTHER tenant's prefixed key verbatim
/// (measured: tenant B read and overwrote tenant A's private value).
///
/// The blocking half of the gate was equally broken and needed no separate fix:
/// `queued_blocking_frame` already rebuilds its frame from `cmd_args` for an
/// unrelated reason (moon#524), so hoisting the rewrite above the gate
/// (moon#668) corrected it automatically. Before that hoist BOTH halves queued
/// unprefixed keys — measured: a queued `BLPOP` popped from the GLOBAL list.
pub(crate) fn reframe_argv(frame: &Frame, args: &[Frame]) -> Frame {
    let Frame::Array(items) = frame else {
        // `extract_command` rejects anything but an argv array before the gate
        // is reached, so this is unreachable; returning the original is the
        // fail-safe answer either way.
        return frame.clone();
    };
    let Some(name) = items.first() else {
        return frame.clone();
    };
    let mut out = Vec::with_capacity(1 + args.len());
    out.push(name.clone());
    out.extend_from_slice(args);
    Frame::Array(out.into())
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

    // MONITOR — detach from the command feed. RESET is contracted to return
    // the connection to its normal state, and a connection still receiving
    // feed lines after RESET would have `+…` lines injected into the reply
    // stream of a client that believes it is issuing ordinary commands.
    if conn.monitor_attached {
        crate::monitor::detach(client_id);
        conn.monitor_attached = false;
        conn.monitor_rx = None;
    }

    // Identity + protocol, from the one definition of "default".
    let (proto, db, authed, user, name) =
        crate::server::conn::util::restore_migrated_state(None, requirepass);
    // RESET is the SECOND protocol switch in the command set, and it moves the
    // protocol the same way a pipelined `HELLO 2` does. Recorded before the
    // assignment, at the index `+RESET` will occupy, so replies produced
    // earlier in this batch keep the protocol they were produced under. A fix
    // that covered only HELLO left `HELLO 3` + `RESET` in one write still
    // retro-downgrading — see `note_protocol_switch`.
    if proto != conn.protocol_version {
        note_protocol_switch(conn, responses.len(), proto);
    }
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
    fn extract_primary_key_numkeys_commands_route_by_real_key() {
        // LMPOP/ZMPOP/SINTERCARD numkeys key [key ...]: routing must hash the
        // key at args[1], never the numkeys literal at args[0] (moon#534).
        for cmd in [&b"LMPOP"[..], &b"ZMPOP"[..]] {
            let args = vec![frame_bulk(b"1"), frame_bulk(b"mykey"), frame_bulk(b"LEFT")];
            let got = extract_primary_key(cmd, &args);
            assert_eq!(
                got.map(|b| b.as_ref()),
                Some(&b"mykey"[..]),
                "{} routed by the numkeys literal",
                String::from_utf8_lossy(cmd)
            );
        }
        let sic = vec![frame_bulk(b"2"), frame_bulk(b"mykey"), frame_bulk(b"other")];
        assert_eq!(
            extract_primary_key(b"SINTERCARD", &sic).map(|b| b.as_ref()),
            Some(&b"mykey"[..])
        );
        // Lower case reaches the same arm — clients are not required to shout.
        let lower = vec![frame_bulk(b"1"), frame_bulk(b"mykey"), frame_bulk(b"MIN")];
        assert_eq!(
            extract_primary_key(b"zmpop", &lower).map(|b| b.as_ref()),
            Some(&b"mykey"[..])
        );
        // A truncated form must not index past the end.
        let short = vec![frame_bulk(b"1")];
        assert!(extract_primary_key(b"LMPOP", &short).is_none());
    }

    #[test]
    fn extract_primary_key_blocking_mpop_routes_by_real_key() {
        // BLMPOP/BZMPOP timeout numkeys key [key ...]: the key is args[2],
        // because args[0] is the TIMEOUT and args[1] the numkeys. The blocking
        // path extracts its own keys, but cluster slot resolution asks this
        // function and would otherwise hash "0.05" (moon#534).
        for cmd in [&b"BLMPOP"[..], &b"BZMPOP"[..]] {
            let args = vec![
                frame_bulk(b"0.05"),
                frame_bulk(b"1"),
                frame_bulk(b"mykey"),
                frame_bulk(b"LEFT"),
            ];
            assert_eq!(
                extract_primary_key(cmd, &args).map(|b| b.as_ref()),
                Some(&b"mykey"[..]),
                "{} routed by the timeout literal",
                String::from_utf8_lossy(cmd)
            );
        }
        let short = vec![frame_bulk(b"0.05"), frame_bulk(b"1")];
        assert!(extract_primary_key(b"BZMPOP", &short).is_none());
    }

    #[test]
    fn extract_primary_key_xreadgroup_routes_by_stream_key() {
        // XREADGROUP GROUP g c STREAMS key id: the key follows the STREAMS
        // token. args[0] is the literal "GROUP", which is what it used to
        // hash — one shard for every stream on the server (moon#533).
        let args = vec![
            frame_bulk(b"GROUP"),
            frame_bulk(b"mygroup"),
            frame_bulk(b"myconsumer"),
            frame_bulk(b"STREAMS"),
            frame_bulk(b"mystream"),
            frame_bulk(b">"),
        ];
        assert_eq!(
            extract_primary_key(b"XREADGROUP", &args).map(|b| b.as_ref()),
            Some(&b"mystream"[..])
        );

        // COUNT/BLOCK before STREAMS must not shift the key: the arm scans for
        // the token rather than counting positions.
        let opts = vec![
            frame_bulk(b"GROUP"),
            frame_bulk(b"g"),
            frame_bulk(b"c"),
            frame_bulk(b"COUNT"),
            frame_bulk(b"10"),
            frame_bulk(b"BLOCK"),
            frame_bulk(b"0"),
            frame_bulk(b"STREAMS"),
            frame_bulk(b"mystream"),
            frame_bulk(b">"),
        ];
        assert_eq!(
            extract_primary_key(b"XREADGROUP", &opts).map(|b| b.as_ref()),
            Some(&b"mystream"[..])
        );

        // No STREAMS token: malformed. Answer None (execute locally and let
        // the command itself produce the syntax error) rather than hashing
        // some other argument.
        let malformed = vec![frame_bulk(b"GROUP"), frame_bulk(b"g")];
        assert!(extract_primary_key(b"XREADGROUP", &malformed).is_none());

        // XREAD keeps working — the arm now matches two command names and it
        // would be easy to break the original one.
        let xread = vec![
            frame_bulk(b"COUNT"),
            frame_bulk(b"1"),
            frame_bulk(b"STREAMS"),
            frame_bulk(b"mystream"),
            frame_bulk(b"0-0"),
        ];
        assert_eq!(
            extract_primary_key(b"XREAD", &xread).map(|b| b.as_ref()),
            Some(&b"mystream"[..])
        );
    }

    #[test]
    fn extract_primary_key_lookalike_commands_are_not_captured() {
        // The new arms match on (length, first byte) then a full compare. This
        // pins that commands sharing a length and prefix are NOT swept in —
        // routing them by args[1] or args[2] would break keys that really are
        // at args[0].
        let args = vec![frame_bulk(b"mykey"), frame_bulk(b"1"), frame_bulk(b"2")];
        for cmd in [
            &b"LPUSH"[..],  // 5 bytes, 'l', like LMPOP
            &b"ZRANK"[..],  // 5 bytes, 'z', like ZMPOP
            &b"BITPOS"[..], // 6 bytes, 'b', like BLMPOP/BZMPOP
        ] {
            assert_eq!(
                extract_primary_key(cmd, &args).map(|b| b.as_ref()),
                Some(&b"mykey"[..]),
                "{} must still route by args[0]",
                String::from_utf8_lossy(cmd)
            );
        }
        // SINTERCARD is 10 bytes starting 's'; SUBSCRIBE-alikes must not match.
        let s10 = vec![frame_bulk(b"mykey"), frame_bulk(b"other")];
        assert_eq!(
            extract_primary_key(b"SDIFFSTORE", &s10).map(|b| b.as_ref()),
            Some(&b"mykey"[..])
        );
    }

    #[test]
    fn extract_primary_key_memory_usage_routes_by_real_key() {
        // MEMORY USAGE <key>: routing must hash the key (arg 2), never the
        // literal "USAGE" (moon#511).
        let args = vec![frame_bulk(b"USAGE"), frame_bulk(b"mykey")];
        let got = extract_primary_key(b"MEMORY", &args);
        assert_eq!(got.map(|b| b.as_ref()), Some(&b"mykey"[..]));

        // Trailing options must not shift the key position.
        let sampled = vec![
            frame_bulk(b"USAGE"),
            frame_bulk(b"mykey"),
            frame_bulk(b"SAMPLES"),
            frame_bulk(b"0"),
        ];
        let got = extract_primary_key(b"MEMORY", &sampled);
        assert_eq!(got.map(|b| b.as_ref()), Some(&b"mykey"[..]));

        // Subcommand casing is not the client's problem.
        let lower = vec![frame_bulk(b"usage"), frame_bulk(b"mykey")];
        let got = extract_primary_key(b"MEMORY", &lower);
        assert_eq!(got.map(|b| b.as_ref()), Some(&b"mykey"[..]));
    }

    #[test]
    fn extract_primary_key_keyless_memory_subcommands_stay_local() {
        // USAGE is the ONLY MEMORY subcommand with a key. The others must not
        // route by whatever happens to sit at args[1], or a future option
        // would be hashed as a key.
        for sub in [
            &b"DOCTOR"[..],
            &b"STATS"[..],
            &b"PURGE"[..],
            &b"MALLOC-STATS"[..],
            &b"HELP"[..],
        ] {
            let args = vec![frame_bulk(sub)];
            assert!(
                extract_primary_key(b"MEMORY", &args).is_none(),
                "MEMORY {} must be keyless",
                String::from_utf8_lossy(sub)
            );
        }
        // A bare MEMORY, and USAGE with no key, have nothing to route by.
        assert!(extract_primary_key(b"MEMORY", &[]).is_none());
        let usage_only = vec![frame_bulk(b"USAGE")];
        assert!(extract_primary_key(b"MEMORY", &usage_only).is_none());
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

    /// moon#570: `LMOVE`/`RPOPLPUSH` are refused before routing when their
    /// two keys are owned by two different shards, and left alone otherwise.
    #[test]
    fn cross_shard_move_rejection_fires_only_on_a_real_split() {
        use super::cross_shard_move_rejection;
        let bulk = |s: &str| Frame::BulkString(Bytes::copy_from_slice(s.as_bytes()));
        let n = 4;
        let base = crate::shard::dispatch::key_to_shard(b"src", n);
        let far = (0..1000)
            .map(|i| format!("dst{i}"))
            .find(|k| crate::shard::dispatch::key_to_shard(k.as_bytes(), n) != base)
            .expect("a key on another shard must exist");

        let split = [bulk("src"), bulk(&far), bulk("LEFT"), bulk("RIGHT")];
        assert!(
            matches!(
                cross_shard_move_rejection(b"LMOVE", &split, n),
                Some(Frame::Error(_))
            ),
            "a cross-shard LMOVE must be refused before it can write the wrong shard"
        );
        assert!(
            matches!(
                cross_shard_move_rejection(b"RPOPLPUSH", &split[..2], n),
                Some(Frame::Error(_))
            ),
            "RPOPLPUSH is LMOVE RIGHT LEFT and must answer identically"
        );

        // Single shard: nothing to refuse.
        assert!(cross_shard_move_rejection(b"LMOVE", &split, 1).is_none());
        // Co-located by hash tag: must still run.
        let tagged = [bulk("{t}:s"), bulk("{t}:d"), bulk("LEFT"), bulk("RIGHT")];
        assert!(cross_shard_move_rejection(b"LMOVE", &tagged, n).is_none());
        // Not a move at all.
        assert!(cross_shard_move_rejection(b"LPUSH", &split, n).is_none());
        // Malformed argv keeps its own arity error instead of a misleading
        // CROSSSLOT: a two-arg LMOVE is not an LMOVE.
        assert!(cross_shard_move_rejection(b"LMOVE", &split[..2], n).is_none());
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

#[cfg(test)]
mod proto_walk_tests {
    use super::ProtoWalk;

    fn seq(start: u8, switches: &[(usize, u8)], n: usize) -> Vec<u8> {
        let mut w = ProtoWalk::new(start, switches);
        (0..n).map(|i| w.version_at(i)).collect()
    }

    /// The defect this whole mechanism exists for: a reply produced under RESP3
    /// keeps RESP3 even though a later HELLO 2 downgraded the connection.
    #[test]
    fn a_switch_does_not_reach_backwards() {
        assert_eq!(seq(3, &[(1, 2)], 3), vec![3, 2, 2]);
    }

    /// HELLO's own reply is rendered in the protocol it just negotiated, so the
    /// switch applies AT its index, not after it.
    #[test]
    fn a_switch_applies_at_its_own_index_not_the_next_one() {
        assert_eq!(seq(2, &[(0, 3)], 2), vec![3, 3]);
    }

    /// Two HELLOs in one batch: each takes effect from its own index.
    #[test]
    fn every_switch_in_a_batch_is_honoured_in_order() {
        assert_eq!(seq(3, &[(1, 2), (3, 3)], 5), vec![3, 2, 2, 3, 3]);
    }

    /// The overwhelmingly common case — no HELLO in the batch.
    #[test]
    fn no_switches_means_one_version_throughout() {
        assert_eq!(seq(3, &[], 4), vec![3, 3, 3, 3]);
    }

    /// A switch past the end of the batch cannot affect it, and must not panic.
    #[test]
    fn a_switch_beyond_the_last_reply_is_inert() {
        assert_eq!(seq(2, &[(9, 3)], 3), vec![2, 2, 2]);
    }
}

#[cfg(test)]
mod cross_shard_write_tests {
    //! moon#592: the routing rule that stops a two-key write from acking a
    //! write it put on the wrong shard.
    //!
    //! Shard membership is never assumed here — every "far" and "near"
    //! destination is SEARCHED for with the routing hash the server itself
    //! uses, so the test cannot pass by accident on a lucky literal.

    use super::{CROSS_SHARD_WRITE_ERROR, cross_shard_multikey_rejection};
    use crate::protocol::Frame;
    use crate::shard::dispatch::key_to_shard;
    use bytes::Bytes;

    const N: usize = 4;

    fn bulk(s: &str) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    /// A key name that provably hashes to a DIFFERENT shard than `src`.
    fn far_from(src: &str) -> String {
        let owner = key_to_shard(src.as_bytes(), N);
        (0..1000)
            .map(|i| format!("far{i}"))
            .find(|k| key_to_shard(k.as_bytes(), N) != owner)
            .expect("a key on another shard must exist")
    }

    /// A key name that provably hashes to the SAME shard as `src`, without a
    /// hash tag — so the co-located case is exercised on its own merits.
    fn near_to(src: &str) -> String {
        let owner = key_to_shard(src.as_bytes(), N);
        (0..1000)
            .map(|i| format!("near{i}"))
            .find(|k| key_to_shard(k.as_bytes(), N) == owner)
            .expect("a key on the same shard must exist")
    }

    /// Every command the guard claims, in the argv shape a client sends.
    /// `{s}` is the source, `{d}` the key it is NOT routed on.
    const FAMILY: &[(&str, &[&str])] = &[
        ("RENAME", &["{s}", "{d}"]),
        ("RENAMENX", &["{s}", "{d}"]),
        ("SMOVE", &["{s}", "{d}", "member"]),
        ("SINTERSTORE", &["{d}", "{s}"]),
        ("SUNIONSTORE", &["{d}", "{s}"]),
        ("SDIFFSTORE", &["{d}", "{s}"]),
        ("ZRANGESTORE", &["{d}", "{s}", "0", "-1"]),
        ("ZUNIONSTORE", &["{d}", "1", "{s}"]),
        ("ZINTERSTORE", &["{d}", "1", "{s}"]),
        ("PFMERGE", &["{d}", "{s}"]),
        (
            "GEOSEARCHSTORE",
            &[
                "{d}",
                "{s}",
                "FROMLONLAT",
                "15",
                "37",
                "BYRADIUS",
                "200",
                "km",
            ],
        ),
        ("SORT", &["{s}", "STORE", "{d}"]),
        (
            "GEORADIUS",
            &["{s}", "15", "37", "200", "km", "STORE", "{d}"],
        ),
        (
            "GEORADIUSBYMEMBER",
            &["{s}", "Catania", "200", "km", "STOREDIST", "{d}"],
        ),
    ];

    /// The other half of the moon#645 contract: a GEORADIUS WITHOUT a store
    /// clause names one key, so it must stay routable to any shard. A guard
    /// that keyed off the command name alone would refuse every geo read at
    /// `--shards > 1`.
    #[test]
    fn a_georadius_without_a_store_clause_is_never_refused() {
        let src = "src";
        let far = far_from(src);
        for shape in [
            &["{s}", "15", "37", "200", "km"][..],
            &["{s}", "15", "37", "200", "km", "WITHCOORD", "ASC"][..],
            &["{s}", "15", "37", "200", "km", "COUNT", "1"][..],
        ] {
            assert!(
                cross_shard_multikey_rejection(b"GEORADIUS", &argv(shape, src, &far), N).is_none(),
                "a clause-free GEORADIUS names one key and must run: {shape:?}"
            );
        }
        assert!(
            cross_shard_multikey_rejection(
                b"GEORADIUSBYMEMBER",
                &argv(&["{s}", "Catania", "200", "km", "ASC"], src, &far),
                N
            )
            .is_none()
        );
        // The read-only twins refuse the clause themselves, so even spelled
        // out they name a single key and must never be pre-refused here.
        assert!(
            cross_shard_multikey_rejection(
                b"GEORADIUS_RO",
                &argv(&["{s}", "15", "37", "200", "km", "STORE", "{d}"], src, &far),
                N
            )
            .is_none()
        );
    }

    fn argv(shape: &[&str], src: &str, dst: &str) -> Vec<Frame> {
        shape
            .iter()
            .map(|p| match *p {
                "{s}" => bulk(src),
                "{d}" => bulk(dst),
                other => bulk(other),
            })
            .collect()
    }

    /// Each family member is refused when — and only when — its keys really
    /// do straddle a shard boundary.
    ///
    /// Dropping any one arm of `touches_a_key_it_did_not_route_on` makes
    /// exactly that command fail the first half; widening the guard to a
    /// blanket refusal makes every command fail the rest.
    #[test]
    fn every_family_member_is_refused_across_a_boundary_and_only_there() {
        let src = "src";
        let far = far_from(src);
        let near = near_to(src);
        for (cmd, shape) in FAMILY {
            let split = argv(shape, src, &far);
            match cross_shard_multikey_rejection(cmd.as_bytes(), &split, N) {
                Some(Frame::Error(e)) => assert_eq!(
                    e.as_ref(),
                    CROSS_SHARD_WRITE_ERROR,
                    "{cmd}: clients key off the CROSSSLOT prefix"
                ),
                other => panic!(
                    "{cmd}: a cross-shard invocation must be refused before it can write \
                     the wrong shard, got {other:?}"
                ),
            }

            // Co-located without a tag: the command works today and must keep
            // working.
            assert!(
                cross_shard_multikey_rejection(cmd.as_bytes(), &argv(shape, src, &near), N)
                    .is_none(),
                "{cmd}: a co-located pair must NOT be refused"
            );
            // `{hash}` tags are the documented remedy — they must actually
            // remedy.
            assert!(
                cross_shard_multikey_rejection(cmd.as_bytes(), &argv(shape, "{t}:s", "{t}:d"), N)
                    .is_none(),
                "{cmd}: a {{hash}}-tagged pair must NOT be refused"
            );
            // One shard: no boundary exists to cross.
            assert!(
                cross_shard_multikey_rejection(cmd.as_bytes(), &split, 1).is_none(),
                "{cmd}: a single-shard server has nothing to refuse"
            );
        }
    }

    /// The guard must not steal an error that belongs to the command, and must
    /// not claim commands it does not own.
    #[test]
    fn out_of_family_and_malformed_argvs_keep_their_own_answers() {
        let src = "src";
        let far = far_from(src);
        let two = [bulk(src), bulk(&far)];

        // Owned elsewhere: the coordinator routes a leg per shard for these,
        // so refusing them would break working commands.
        for cmd in [
            "MSET", "MGET", "DEL", "UNLINK", "EXISTS", "COPY", "BITOP", "MSETNX",
        ] {
            assert!(
                cross_shard_multikey_rejection(cmd.as_bytes(), &two, N).is_none(),
                "{cmd} is coordinated per-shard and must not be refused here"
            );
        }
        // moon#570 / PR #591 owns the list MOVE family, including its two
        // blocking entry points this pre-routing guard cannot reach.
        for cmd in ["LMOVE", "RPOPLPUSH", "BLMOVE", "BRPOPLPUSH"] {
            assert!(
                cross_shard_multikey_rejection(cmd.as_bytes(), &two, N).is_none(),
                "{cmd} belongs to moon#570, not here"
            );
        }
        // Read-only twins: same routing rule, different (wrong-answer) defect.
        for cmd in ["SINTER", "SUNION", "SDIFF", "PFCOUNT", "LCS", "TOUCH"] {
            assert!(
                cross_shard_multikey_rejection(cmd.as_bytes(), &two, N).is_none(),
                "{cmd} is read-only and out of scope for moon#592"
            );
        }

        // A SORT with no STORE clause names one key: nothing to straddle.
        let sort_ro = [bulk(src), bulk("LIMIT"), bulk("0"), bulk("10")];
        assert!(cross_shard_multikey_rejection(b"SORT", &sort_ro, N).is_none());

        // Malformed argvs must still earn their own arity/syntax errors rather
        // than a misleading CROSSSLOT that sends the client chasing hash tags.
        let cases: &[(&str, Vec<Frame>)] = &[
            // arity: RENAME needs two keys
            ("RENAME", vec![bulk(src)]),
            // numkeys larger than the argv
            ("ZUNIONSTORE", vec![bulk(&far), bulk("99"), bulk(src)]),
            // numkeys is not a number
            ("ZINTERSTORE", vec![bulk(&far), bulk("banana"), bulk(src)]),
            // STORE with no destination
            ("SORT", vec![bulk(src), bulk("STORE")]),
            // a key position holding a non-string
            ("RENAME", vec![bulk(src), Frame::Integer(7)]),
        ];
        for (cmd, args) in cases {
            assert!(
                cross_shard_multikey_rejection(cmd.as_bytes(), args, N).is_none(),
                "{cmd} {args:?}: a malformed argv must keep its own error"
            );
        }

        // The degenerate same-key form is one key, never a boundary.
        assert!(
            cross_shard_multikey_rejection(b"RENAME", &[bulk("k"), bulk("k")], 64).is_none(),
            "RENAME k k names one key"
        );
    }
}
