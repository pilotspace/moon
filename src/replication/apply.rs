//! Replication stream apply (R0): parse the master's RESP command stream and
//! apply each write to the local shard.
//!
//! The master fans out every write as `aof::serialize_command(cmd)` — a bare
//! RESP Array frame, identical to the AOF wire format (see
//! [`crate::shard::spsc_handler::wal_append_and_fanout`]). The replica
//! connection task runs ON its shard's thread, so for a single-shard deployment
//! every command targets the local shard and is applied directly through the
//! thread-local [`ShardSlice`](crate::shard::slice) via `with_shard` — there is
//! no SPSC self-hop (the ChannelMesh has no self-slot; local legs never go
//! through `spsc_send`). Multi-shard replica routing (hash each key to its
//! owning shard, broadcast keyless commands) is deferred to R2.
//!
//! **Read-only bypass:** the read-only-replica guard lives in the connection
//! layer (`try_enforce_readonly`), which the replica task never invokes. Applying
//! here therefore correctly bypasses it — a replica MUST apply whatever the
//! master streams regardless of its own read-only role.
//!
//! **Durability (R0 scope):** apply is in-memory only. The replica's own AOF is
//! NOT appended per replicated write; a replica recovers by re-syncing from its
//! master on restart (standard, safe). BGREWRITEAOF / RDB snapshots on the
//! replica still fold the applied in-memory state. Independent replica-side
//! incremental persistence is a documented follow-up.

use std::sync::Arc;

use bytes::BytesMut;

use crate::protocol::{Frame, ParseConfig, parse};

/// One replicated data command, already resolved to its logical db.
#[derive(Debug)]
pub(crate) struct ReplCommand {
    pub db_index: usize,
    pub command: Arc<Frame>,
}

/// Outcome of draining complete frames out of the replication read buffer.
pub(crate) struct DrainResult {
    /// Data commands to apply, in stream order.
    pub commands: Vec<ReplCommand>,
    /// Bytes consumed from `buf` — advance the replication offset by exactly
    /// this (NOT by the raw socket read count, which may split a frame).
    pub consumed: usize,
    /// A frame failed to parse. The RESP replication stream is unframed and
    /// cannot be safely resynced mid-stream, so the caller must drop the
    /// connection; the reconnect path then negotiates a fresh resync.
    pub fatal: bool,
}

/// Drain every COMPLETE RESP command frame from `buf`, tracking `SELECT` into
/// `selected_db`. Any partial trailing frame is left in `buf` for the next read
/// (the parser does not consume incomplete frames), so `consumed` counts only
/// whole frames.
///
/// `SELECT n` updates `selected_db` and is NOT emitted (carries no data).
/// Replication chatter (`PING`, `REPLCONF`) is skipped. Every other command is
/// emitted bound to the `selected_db` in effect when it was parsed.
pub(crate) fn drain_replicated_commands(
    buf: &mut BytesMut,
    selected_db: &mut usize,
) -> DrainResult {
    let config = ParseConfig::default();
    let mut commands = Vec::new();
    let mut consumed = 0usize;
    let mut fatal = false;

    loop {
        if buf.is_empty() {
            break;
        }
        let before = buf.len();
        match parse::parse(buf, &config) {
            Ok(Some(frame)) => {
                consumed += before - buf.len();
                classify(frame, selected_db, &mut commands);
            }
            // Incomplete trailing frame: parser left `buf` untouched — wait for
            // the next socket read to complete it.
            Ok(None) => break,
            Err(_) => {
                fatal = true;
                break;
            }
        }
    }

    DrainResult {
        commands,
        consumed,
        fatal,
    }
}

/// Route a single parsed frame: absorb `SELECT`, drop chatter, or record a data
/// command bound to the current `selected_db`.
fn classify(frame: Frame, selected_db: &mut usize, out: &mut Vec<ReplCommand>) {
    let Some((cmd, args)) = command_parts(&frame) else {
        return; // non-array / empty — ignore (e.g. inline-newline keepalive)
    };
    if cmd.eq_ignore_ascii_case(b"SELECT") {
        if let Some(db) = args.first().and_then(frame_to_usize) {
            *selected_db = db;
        }
        return;
    }
    // Keepalive / ack-negotiation frames the master may interleave — never data.
    if cmd.eq_ignore_ascii_case(b"PING") || cmd.eq_ignore_ascii_case(b"REPLCONF") {
        return;
    }
    out.push(ReplCommand {
        db_index: *selected_db,
        command: Arc::new(frame),
    });
}

/// Borrow `(command_name, args)` out of a RESP Array frame.
fn command_parts(frame: &Frame) -> Option<(&[u8], &[Frame])> {
    match frame {
        Frame::Array(arr) if !arr.is_empty() => {
            let name = match &arr[0] {
                Frame::BulkString(s) => s.as_ref(),
                Frame::SimpleString(s) => s.as_ref(),
                _ => return None,
            };
            Some((name, &arr[1..]))
        }
        _ => None,
    }
}

fn frame_to_usize(f: &Frame) -> Option<usize> {
    match f {
        Frame::BulkString(s) | Frame::SimpleString(s) => {
            std::str::from_utf8(s).ok()?.trim().parse().ok()
        }
        Frame::Integer(n) if *n >= 0 => Some(*n as usize),
        _ => None,
    }
}

/// Apply one replicated command to the local shard's database.
///
/// Runs synchronously on the shard thread through the thread-local `ShardSlice`
/// (no `.await`), so it cannot interleave with the shard event loop's own
/// `with_shard` access on the same cooperative thread. Bypasses the read-only
/// guard by construction (see module docs).
///
/// Returns `false` only if this thread has no initialized `ShardSlice` — which
/// would indicate the replica task was spawned off a shard thread (a wiring
/// bug); the caller logs and drops the stream.
pub(crate) fn apply_local(rc: &ReplCommand) -> bool {
    use crate::command::{DispatchResult, dispatch as cmd_dispatch};
    use crate::shard::spsc_handler::extract_command_static;

    let Some((cmd, args)) = extract_command_static(&rc.command) else {
        return true; // not an array command — nothing to apply (defensive)
    };
    crate::shard::slice::try_with_shard(|s| {
        let db_count = s.databases.len();
        if db_count == 0 {
            return;
        }
        let db_idx = rc.db_index.min(db_count - 1);
        if db_idx != rc.db_index {
            // Replica configured with fewer logical dbs than the master: a
            // high-index write is clamped rather than lost, but that is a
            // divergence — surface it.
            tracing::debug!(
                "replica apply: db {} clamped to {} ({} dbs on this shard)",
                rc.db_index,
                db_idx,
                db_count
            );
        }

        // FT.* index-definition commands (v0.7 R0.5) are streamed verbatim by
        // the master's connection layer (`ShardMessage::ReplicateVerbatim`) —
        // generic `dispatch()` does not know them. Route through the same
        // handlers the master's connection layer uses.
        if cmd.len() > 3 && cmd[..3].eq_ignore_ascii_case(b"FT.") {
            let resp = apply_ft(s, cmd, args, db_idx);
            warn_on_error(cmd, &resp);
            return;
        }

        // GRAPH.* mutations (v0.7 graph replication) arrive as the master's
        // DETERMINISTIC WAL-record form: id-pinned (GRAPH.ADDNODE <g>
        // <node_id> …) with FNV-hashed u16 label/prop-key ids — `label_to_id`
        // is a stateless hash, so both sides resolve the same strings to the
        // same ids. Generic `dispatch()` parses the USER syntax and would
        // re-allocate ids; route through the WAL replay collector instead,
        // exactly like restart recovery does.
        #[cfg(feature = "graph")]
        if crate::graph::replay::GraphReplayCollector::is_graph_command(cmd) {
            apply_graph(s, cmd, args);
            return;
        }

        // TEMPORAL.INVALIDATE arrives as the master's deterministic
        // wall-clock-pinned form (`TEMPORAL.INVALIDATE-AT <graph> <N|E>
        // <entity_id> <wall_ms>`, round-2 finding B) — apply with the SAME
        // wall_ms the master used so `valid_to` matches exactly. Generic
        // `dispatch()` does not know this internal record.
        #[cfg(feature = "graph")]
        if cmd.eq_ignore_ascii_case(b"TEMPORAL.INVALIDATE-AT") {
            apply_temporal_invalidate(s, cmd, args);
            return;
        }

        // MOVE / cross-db COPY touch two databases at once and are intercepted
        // BEFORE generic dispatch on the master (see `spsc_two_db`). Generic
        // `dispatch()` cannot apply them — it returns an error for MOVE and
        // silently mis-targets COPY..DB — so mirror the master's two-db
        // intercept here. A replica never evicts on apply (it follows the
        // master), so the destination-db eviction gate is skipped.
        if cmd.eq_ignore_ascii_case(b"MOVE") || cmd.eq_ignore_ascii_case(b"COPY") {
            if let Some(resp) = apply_two_db(cmd, args, &mut s.databases, db_idx, db_count) {
                warn_on_error(cmd, &resp);
                return;
            }
            // COPY with no DB clause / same-db COPY: fall through to dispatch.
        }

        let resp = {
            let db = &mut s.databases[db_idx];
            // Replica applies off the shard's periodic clock tick; refresh
            // directly so command-relative expiries (EXPIRE/SETEX) compute
            // against real time.
            db.refresh_now();
            let mut selected = db_idx;
            match cmd_dispatch(db, cmd, args, &mut selected, db_count) {
                DispatchResult::Response(f) | DispatchResult::Quit(f) => f,
            }
        };
        // Index-plane parity (v0.7 R0.5): mirror of the master's
        // connection-layer hook block — every dispatch path that applies KV
        // writes MUST run these, or the replica's FT indexes silently diverge
        // from its own keyspace (wire-parity requirement, see
        // `auto_delete_vectors` docs).
        if !matches!(resp, Frame::Error(_)) {
            apply_index_parity_hooks(s, cmd, args, db_idx as u8);
        }
        warn_on_error(cmd, &resp);
    })
    .is_some()
}

/// Apply a replicated FT.* index-definition command (FT.CREATE / FT.DROPINDEX
/// / FT.CONFIG SET) through the same handlers the master's connection layer
/// uses. Deliberately NO keyspace backfill on live FT.CREATE — master parity:
/// FT.CREATE never indexes pre-existing keys outside restart recovery (and,
/// on a replica, the snapshot-load path below).
fn apply_ft(
    s: &mut crate::shard::slice::ShardSlice,
    cmd: &[u8],
    args: &[Frame],
    db_idx: usize,
) -> Frame {
    use crate::command::vector_search::{ft_admin, ft_config, ft_create};
    let db_index = db_idx as u8;
    if cmd.eq_ignore_ascii_case(b"FT.CREATE") {
        ft_create::ft_create(&mut s.vector_store, &mut s.text_store, args, db_index)
    } else if cmd.eq_ignore_ascii_case(b"FT.DROPINDEX") {
        ft_admin::ft_dropindex(
            &mut s.vector_store,
            &mut s.text_store,
            s.databases.get_mut(db_idx),
            args,
            db_index,
        )
    } else if cmd.eq_ignore_ascii_case(b"FT.CONFIG") {
        ft_config::ft_config(&mut s.vector_store, &mut s.text_store, args, db_index)
    } else {
        // Only def mutations are streamed; any other FT.* here is unexpected —
        // skip quietly rather than fabricate a divergence warning.
        tracing::debug!(
            "replica apply: ignoring non-replicable FT command {}",
            String::from_utf8_lossy(cmd)
        );
        Frame::Null
    }
}

/// Apply one replicated graph WAL record (v0.7 graph replication) into this
/// shard's `GraphStore` through the same `GraphReplayCollector` restart
/// recovery uses. Records arrive one at a time in stream order; the collector
/// resolves edge/SET targets against nodes applied by EARLIER records via the
/// write-buffer seeding in `replay_epoch_aware`.
#[cfg(feature = "graph")]
fn apply_graph(s: &mut crate::shard::slice::ShardSlice, cmd: &[u8], args: &[Frame]) {
    use crate::graph::replay::GraphReplayCollector;
    let mut arg_bytes: Vec<&[u8]> = Vec::with_capacity(args.len());
    for a in args {
        match a {
            Frame::BulkString(b) | Frame::SimpleString(b) => arg_bytes.push(b.as_ref()),
            _ => {
                tracing::warn!(
                    "replica apply: non-bulk arg in graph record {} — skipped (graph diverges; \
                     full resync required)",
                    String::from_utf8_lossy(cmd)
                );
                return;
            }
        }
    }
    let mut collector = GraphReplayCollector::new();
    if !collector.collect_command(cmd, &arg_bytes) {
        tracing::warn!(
            "replica apply: unparseable graph record {} — skipped (graph diverges; \
             full resync required)",
            String::from_utf8_lossy(cmd)
        );
        return;
    }
    if collector.replay_into(&mut s.graph_store) == 0 {
        // Not always divergence (e.g. GRAPH.CREATE of an existing graph
        // counts 0), but worth surfacing at debug for stream forensics.
        tracing::debug!(
            "replica apply: graph record {} replayed 0 mutations",
            String::from_utf8_lossy(cmd)
        );
    }
}

/// Apply a replicated `TEMPORAL.INVALIDATE-AT` record: same mutation the
/// master ran (`apply_invalidate`) with the master's pinned `wall_ms`. The
/// returned `GraphTemporal` WAL payload is dropped (`Ok(_)` is ignored),
/// matching `apply_graph`'s no-local-persistence model (a restarted replica
/// resyncs from the master).
///
/// K1a: `apply_invalidate` now returns the payload directly instead of
/// pushing it into `gs.wal_pending` — there is nothing left to drain here.
#[cfg(feature = "graph")]
fn apply_temporal_invalidate(s: &mut crate::shard::slice::ShardSlice, cmd: &[u8], args: &[Frame]) {
    let Some((graph_name, is_node, entity_id, wall_ms)) =
        crate::command::temporal::parse_invalidate_at(args)
    else {
        tracing::warn!(
            "replica apply: malformed {} record — skipped (graph diverges; \
             full resync required)",
            String::from_utf8_lossy(cmd)
        );
        return;
    };
    if let Err(e) = crate::command::temporal::apply_invalidate(
        &mut s.graph_store,
        entity_id,
        is_node,
        &graph_name,
        wall_ms,
    ) {
        tracing::warn!(
            "replica apply: TEMPORAL.INVALIDATE-AT entity_id={} failed: {} \
             (graph diverges; full resync required)",
            entity_id,
            String::from_utf8_lossy(e)
        );
    }
}

/// Mirror of the master's connection-layer index-parity block
/// (`handler_monoio/mod.rs`, "HSET auto-index" onwards): HSET feeds the
/// auto-indexer, DEL/UNLINK tombstone, HDEL of a vector field tombstones,
/// FLUSHDB/FLUSHALL clear index contents (definitions survive).
fn apply_index_parity_hooks(
    s: &mut crate::shard::slice::ShardSlice,
    cmd: &[u8],
    args: &[Frame],
    db_index: u8,
) {
    use crate::shard::spsc_handler as hooks;
    if cmd.eq_ignore_ascii_case(b"HSET") {
        if let Some(key) = args
            .first()
            .and_then(crate::server::connection::extract_bytes)
        {
            // Return value (txn vector-intents) is only meaningful inside an
            // active CrossStoreTxn; replica apply has none.
            let _ = hooks::auto_index_hset_public(
                &mut s.vector_store,
                &mut s.text_store,
                key.as_ref(),
                args,
                db_index,
            );
        }
    } else if cmd.eq_ignore_ascii_case(b"DEL") || cmd.eq_ignore_ascii_case(b"UNLINK") {
        hooks::auto_delete_vectors(&mut s.vector_store, args, db_index);
    } else if cmd.eq_ignore_ascii_case(b"HDEL") {
        hooks::auto_hdel_vectors(&mut s.vector_store, args, db_index);
    } else if cmd.eq_ignore_ascii_case(b"FLUSHDB") || cmd.eq_ignore_ascii_case(b"FLUSHALL") {
        hooks::auto_flush_indexes(
            &mut s.vector_store,
            &mut s.text_store,
            cmd.eq_ignore_ascii_case(b"FLUSHDB"),
            db_index,
        );
    }
}

/// A replicated write that fails to apply is a silent-divergence risk — log it
/// loudly instead of dropping it on the floor. (Read-only errors cannot occur
/// here: apply bypasses the connection-layer read-only guard by construction.)
fn warn_on_error(cmd: &[u8], resp: &Frame) {
    if let Frame::Error(e) = resp {
        tracing::warn!(
            "replica apply: {} returned error, replica may diverge from master: {}",
            String::from_utf8_lossy(cmd),
            String::from_utf8_lossy(e)
        );
    }
}

/// Apply `MOVE` / cross-db `COPY ... DB n` on the replica using the same core
/// helpers as the master's two-db intercept. Returns `None` for a same-db /
/// no-`DB`-clause COPY (caller falls through to generic dispatch), `Some(resp)`
/// otherwise.
fn apply_two_db(
    cmd: &[u8],
    args: &[Frame],
    databases: &mut [crate::storage::Database],
    db_idx: usize,
    db_count: usize,
) -> Option<Frame> {
    use crate::command::keyspace::move_cmd as ksmv;

    if cmd.eq_ignore_ascii_case(b"MOVE") {
        let resp = match ksmv::parse_move_args(args, db_count) {
            Err(e) => e,
            Ok((_key, dst)) if dst == db_idx => Frame::Integer(0),
            Ok((key, dst)) => ksmv::with_two_slice_dbs(databases, db_idx, dst, |src, dstdb| {
                src.refresh_now();
                dstdb.refresh_now();
                ksmv::move_core(src, dstdb, &key)
            }),
        };
        return Some(resp);
    }

    // COPY: `?` returns None (fall through to dispatch) for no-DB / same-db.
    let copy_result = ksmv::parse_copy_db_args(args, db_idx, db_count)?;
    let resp = match copy_result {
        Err(e) => e,
        Ok(ca) => ksmv::with_two_slice_dbs(databases, db_idx, ca.dst_db, |src, dst| {
            src.refresh_now();
            dst.refresh_now();
            ksmv::copy_core(src, dst, &ca.src_key, &ca.dst_key, ca.replace)
        }),
    };
    Some(resp)
}

/// Load a full-resync RDB snapshot into the local shard's databases, replacing
/// existing contents (full resync = authoritative master state).
///
/// Returns the number of keys loaded, or an error if this thread has no
/// `ShardSlice` or the RDB is malformed.
pub(crate) fn load_snapshot(rdb: &[u8]) -> anyhow::Result<usize> {
    use crate::persistence::redis_rdb;
    // Moon-private aux fields (written by the master right after the RDB
    // header) carry the FT index DEFINITIONS; standard RDB loaders skip them.
    let vec_defs = redis_rdb::read_moon_aux(rdb, redis_rdb::MOON_AUX_VECTOR_DEFS);
    let text_defs = redis_rdb::read_moon_aux(rdb, redis_rdb::MOON_AUX_TEXT_DEFS);
    // R2 (task #20): a multi-shard master's merged snapshot carries one
    // graph-store aux entry PER shard (graph content is sharded) — collect
    // them all; a single-shard snapshot yields exactly one.
    #[cfg(feature = "graph")]
    let graph_blobs = redis_rdb::read_moon_aux_all(rdb, redis_rdb::MOON_AUX_GRAPH_STORE);
    match crate::shard::slice::try_with_shard(|s| {
        for db in s.databases.iter_mut() {
            db.clear();
        }
        let loaded = redis_rdb::load_rdb(&mut s.databases, rdb)?;
        install_snapshot_index_defs(s, vec_defs.as_deref(), text_defs.as_deref());
        // v0.7 graph replication: install the master's whole graph store
        // (authoritative replace — an EMPTY blob drops replica-local graphs;
        // an ABSENT aux means a pre-graph-sync master, warn-and-keep).
        #[cfg(feature = "graph")]
        match &graph_blobs[..] {
            blobs if !blobs.is_empty() => {
                match crate::replication::graph_sync::install_graph_store_many(
                    &mut s.graph_store,
                    blobs,
                ) {
                    Some(n) => {
                        if n > 0 {
                            tracing::info!(
                                "replica snapshot: installed {} graph(s) from {} shard blob(s)",
                                n,
                                blobs.len()
                            );
                        }
                    }
                    None => {
                        return Err(anyhow::anyhow!(
                            "replica snapshot: malformed graph-store aux blob"
                        ));
                    }
                }
            }
            _ => {
                if s.graph_store.graph_count() > 0 {
                    tracing::warn!(
                        "replica snapshot carried no graph-store aux but {} local graph(s) \
                         exist — master predates graph replication; keeping local graphs \
                         (they may diverge)",
                        s.graph_store.graph_count()
                    );
                }
            }
        }
        Ok(loaded)
    }) {
        Some(r) => r,
        None => Err(anyhow::anyhow!(
            "replica snapshot load: no ShardSlice on this thread"
        )),
    }
}

/// Full resync = authoritative replace: drop every replica-local FT index,
/// install the master's definitions, then backfill them from the just-loaded
/// keyspace. Backfill here is "restart semantics" — the same matching-hash
/// rescan restart recovery performs (`event_loop.rs`, "Auto-reindex existing
/// HASH keys"); live FT.CREATE apply deliberately does NOT backfill.
fn install_snapshot_index_defs(
    s: &mut crate::shard::slice::ShardSlice,
    vec_defs: Option<&[u8]>,
    text_defs: Option<&[u8]>,
) {
    let names: Vec<bytes::Bytes> = s.vector_store.index_names().into_iter().cloned().collect();
    let local_text_names = s.text_store.index_names();
    let (local_vec, local_text) = (names.len(), local_text_names.len());
    for n in &names {
        s.vector_store.drop_index(n);
    }
    for n in local_text_names {
        s.text_store.drop_index(&n);
    }
    // The authoritative drop above is silent when the master streams no defs
    // at all (pre-R0.5 master, or def serialization failed) — but wiping a
    // replica's local indexes with zero replacement is exactly the scenario an
    // operator needs to hear about (mixed-version rolling upgrade, REPLICAOF
    // pointed at the wrong host).
    if vec_defs.is_none() && text_defs.is_none() {
        if local_vec + local_text > 0 {
            tracing::warn!(
                "replica full resync: dropped {} local vector and {} local text index(es); \
                 the master's snapshot carried NO index definitions (pre-R0.5 master or \
                 def-serialization failure) — FT indexes are gone on this replica",
                local_vec,
                local_text
            );
        }
        return;
    }

    // (db_index, key_prefix) pairs feeding the backfill scan below.
    let mut prefixes: Vec<(u8, bytes::Bytes)> = Vec::new();

    if let Some(blob) = vec_defs {
        match crate::vector::index_persist::deserialize_index_metas_with_weights(blob) {
            Ok(pairs) => {
                for (meta, weight) in pairs {
                    for p in &meta.key_prefixes {
                        prefixes.push((meta.db_index, p.clone()));
                    }
                    let name = meta.name.clone();
                    if let Err(e) = s.vector_store.create_index(meta) {
                        tracing::warn!(
                            "replica snapshot: failed to create vector index '{}': {}",
                            String::from_utf8_lossy(&name),
                            e
                        );
                        continue;
                    }
                    if weight != 1.0
                        && let Some(idx) = s.vector_store.get_index_mut(&name)
                    {
                        idx.set_compaction_weight(weight);
                    }
                }
            }
            Err(e) => tracing::warn!(
                "replica snapshot: vector index defs unreadable ({}); vector indexes NOT replicated",
                e
            ),
        }
    }

    #[cfg(not(feature = "text-index"))]
    if text_defs.is_some() {
        tracing::warn!(
            "replica snapshot: master streamed text index defs but this build lacks the \
             text-index feature; text indexes NOT replicated"
        );
    }
    #[cfg(feature = "text-index")]
    if let Some(blob) = text_defs {
        match crate::text::index_persist::deserialize_text_index_metas(blob) {
            Ok(metas) => {
                for meta in metas {
                    for p in &meta.key_prefixes {
                        prefixes.push((meta.db_index, p.clone()));
                    }
                    let mut text_index = crate::text::store::TextIndex::new(
                        meta.name.clone(),
                        meta.key_prefixes.clone(),
                        meta.text_fields.clone(),
                        meta.bm25_config,
                    );
                    // Carry the master's db binding forward (WS5a) — same as
                    // the restart-recovery restore path.
                    text_index.db_index = meta.db_index;
                    if let Err(e) = s.text_store.create_index(meta.name.clone(), text_index) {
                        tracing::warn!(
                            "replica snapshot: failed to create text index '{}': {}",
                            String::from_utf8_lossy(&meta.name),
                            e
                        );
                    }
                }
            }
            Err(e) => tracing::warn!(
                "replica snapshot: text index defs unreadable ({}); text indexes NOT replicated",
                e
            ),
        }
    }

    if prefixes.is_empty() {
        return;
    }
    let db_count = s.databases.len();
    let mut backfilled = 0usize;
    for db_idx in 0..db_count {
        let wanted: Vec<&bytes::Bytes> = prefixes
            .iter()
            .filter(|(d, _)| *d as usize == db_idx)
            .map(|(_, p)| p)
            .collect();
        if wanted.is_empty() {
            continue;
        }
        let matching = collect_matching_hash_args(&s.databases[db_idx], |key| {
            wanted.iter().any(|p| key.starts_with(&p[..]))
        });
        for (key, args) in &matching {
            let _ = crate::shard::spsc_handler::auto_index_hset_public(
                &mut s.vector_store,
                &mut s.text_store,
                key,
                args,
                db_idx as u8,
            );
        }
        backfilled += matching.len();
    }
    if backfilled > 0 {
        tracing::info!(
            "replica snapshot: backfilled {} hash key(s) into replicated FT indexes",
            backfilled
        );
    }
}

/// Collect `(key, HSET-shaped args)` for every HASH key in `db` matching
/// `key_matches` — args are `[key, field, value, ...]`, ready for
/// `auto_index_hset_public`. Same extraction as restart recovery's rescan
/// closure in `event_loop.rs`.
fn collect_matching_hash_args(
    db: &crate::storage::Database,
    key_matches: impl Fn(&[u8]) -> bool,
) -> Vec<(Vec<u8>, Vec<Frame>)> {
    use crate::storage::compact_value::RedisValueRef;
    let mut matching = Vec::new();
    for (key, entry) in db.data().iter() {
        let key_bytes = key.as_bytes();
        if !key_matches(key_bytes) {
            continue;
        }
        let mut args = Vec::new();
        args.push(Frame::BulkString(bytes::Bytes::copy_from_slice(key_bytes)));
        match entry.as_redis_value() {
            RedisValueRef::Hash(map) => {
                for (field, value) in map.iter() {
                    args.push(Frame::BulkString(bytes::Bytes::copy_from_slice(field)));
                    args.push(Frame::BulkString(bytes::Bytes::copy_from_slice(value)));
                }
            }
            RedisValueRef::HashListpack(lp) => {
                let entries: Vec<_> = lp.iter().collect();
                let mut j = 0;
                while j + 1 < entries.len() {
                    args.push(Frame::BulkString(bytes::Bytes::from(entries[j].as_bytes())));
                    args.push(Frame::BulkString(bytes::Bytes::from(
                        entries[j + 1].as_bytes(),
                    )));
                    j += 2;
                }
            }
            _ => continue,
        }
        if args.len() > 1 {
            matching.push((key_bytes.to_vec(), args));
        }
    }
    matching
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    /// Build the RESP bytes for `SET key val` etc. (bare Array, AOF wire form).
    fn resp_cmd(parts: &[&[u8]]) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for p in parts {
            v.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            v.extend_from_slice(p);
            v.extend_from_slice(b"\r\n");
        }
        v
    }

    fn cmd_name(rc: &ReplCommand) -> Vec<u8> {
        match rc.command.as_ref() {
            Frame::Array(a) => match &a[0] {
                Frame::BulkString(s) => s.to_vec(),
                _ => Vec::new(),
            },
            _ => Vec::new(),
        }
    }

    #[test]
    fn single_complete_command_consumes_all() {
        let bytes = resp_cmd(&[b"SET", b"foo", b"bar"]);
        let total = bytes.len();
        let mut buf = BytesMut::from(&bytes[..]);
        let mut db = 0usize;
        let r = drain_replicated_commands(&mut buf, &mut db);
        assert_eq!(r.commands.len(), 1);
        assert_eq!(cmd_name(&r.commands[0]), b"SET");
        assert_eq!(r.commands[0].db_index, 0);
        assert_eq!(r.consumed, total);
        assert!(!r.fatal);
        assert!(buf.is_empty(), "whole frame must be consumed");
    }

    #[test]
    fn two_back_to_back_commands() {
        let mut bytes = resp_cmd(&[b"SET", b"a", b"1"]);
        bytes.extend_from_slice(&resp_cmd(&[b"DEL", b"a"]));
        let total = bytes.len();
        let mut buf = BytesMut::from(&bytes[..]);
        let mut db = 0usize;
        let r = drain_replicated_commands(&mut buf, &mut db);
        assert_eq!(r.commands.len(), 2);
        assert_eq!(cmd_name(&r.commands[0]), b"SET");
        assert_eq!(cmd_name(&r.commands[1]), b"DEL");
        assert_eq!(r.consumed, total);
        assert!(buf.is_empty());
    }

    #[test]
    fn partial_trailing_frame_is_retained() {
        let full = resp_cmd(&[b"SET", b"a", b"1"]);
        let complete_len = full.len();
        let mut bytes = full.clone();
        // Append a truncated second frame (header only, body missing).
        bytes.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$3\r\nfo");
        let mut buf = BytesMut::from(&bytes[..]);
        let mut db = 0usize;
        let r = drain_replicated_commands(&mut buf, &mut db);
        // Only the first (complete) command is emitted; consumed excludes the
        // partial tail, which stays buffered for the next read.
        assert_eq!(r.commands.len(), 1);
        assert_eq!(r.consumed, complete_len);
        assert!(!r.fatal);
        assert_eq!(&buf[..], b"*3\r\n$3\r\nSET\r\n$3\r\nfo");
    }

    #[test]
    fn select_updates_db_and_is_not_emitted() {
        let mut bytes = resp_cmd(&[b"SELECT", b"2"]);
        bytes.extend_from_slice(&resp_cmd(&[b"SET", b"k", b"v"]));
        let mut buf = BytesMut::from(&bytes[..]);
        let mut db = 0usize;
        let r = drain_replicated_commands(&mut buf, &mut db);
        assert_eq!(r.commands.len(), 1, "SELECT must not be emitted as data");
        assert_eq!(cmd_name(&r.commands[0]), b"SET");
        assert_eq!(r.commands[0].db_index, 2, "SET must bind to selected db 2");
        assert_eq!(db, 2, "selected_db persists across drains");
    }

    #[test]
    fn ping_and_replconf_are_skipped() {
        let mut bytes = resp_cmd(&[b"PING"]);
        bytes.extend_from_slice(&resp_cmd(&[b"REPLCONF", b"GETACK", b"*"]));
        bytes.extend_from_slice(&resp_cmd(&[b"SET", b"k", b"v"]));
        let mut buf = BytesMut::from(&bytes[..]);
        let mut db = 0usize;
        let r = drain_replicated_commands(&mut buf, &mut db);
        assert_eq!(r.commands.len(), 1);
        assert_eq!(cmd_name(&r.commands[0]), b"SET");
        assert!(buf.is_empty());
    }

    #[test]
    fn malformed_frame_is_fatal() {
        // A bulk-string length that cannot parse → hard parse error.
        let bytes = b"*1\r\n$-5\r\nX\r\n".to_vec();
        let mut buf = BytesMut::from(&bytes[..]);
        let mut db = 0usize;
        let r = drain_replicated_commands(&mut buf, &mut db);
        assert!(r.fatal, "unparseable frame must flag fatal for reconnect");
    }

    #[test]
    fn empty_buffer_is_noop() {
        let mut buf = BytesMut::new();
        let mut db = 3usize;
        let r = drain_replicated_commands(&mut buf, &mut db);
        assert!(r.commands.is_empty());
        assert_eq!(r.consumed, 0);
        assert!(!r.fatal);
        assert_eq!(db, 3);
    }

    #[test]
    fn integer_select_arg_parses() {
        // SELECT sent with an integer arg instead of bulk string.
        let frame = Frame::Array(
            vec![
                Frame::BulkString(Bytes::from_static(b"SELECT")),
                Frame::Integer(4),
            ]
            .into(),
        );
        let mut db = 0usize;
        let mut out = Vec::new();
        classify(frame, &mut db, &mut out);
        assert_eq!(db, 4);
        assert!(out.is_empty());
    }
}
