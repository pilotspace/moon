//! FT.* vector/text search command handlers.
//!
//! Handles FT.CREATE, FT.DROPINDEX, FT.SEARCH (vector + text), FT.AGGREGATE,
//! FT.INFO, FT.COMPACT, FT._LIST, FT.CACHESEARCH, FT.CONFIG, FT.RECOMMEND,
//! FT.NAVIGATE, FT.EXPAND. Multi-shard scatter-gather and single-shard fast paths.

use bytes::Bytes;

use crate::protocol::Frame;
use crate::server::conn::core::{ConnectionContext, ConnectionState};
use crate::server::conn::shared::resolve_ft_search_as_of_lsn;
use crate::workspace::strip_workspace_prefix_from_response;

/// True for the FT.* commands whose SUCCESS mutates index definitions and
/// therefore must fan out to replicas (v0.7 R0.5): FT.CREATE, FT.DROPINDEX,
/// and FT.CONFIG SET (GET is a read). Content mutations (HSET/DEL/…) already
/// replicate via the KV stream; FT.COMPACT is a local optimization.
fn is_replicated_ft_def_mutation(cmd: &[u8], cmd_args: &[Frame]) -> bool {
    if cmd.eq_ignore_ascii_case(b"FT.CREATE") || cmd.eq_ignore_ascii_case(b"FT.DROPINDEX") {
        return true;
    }
    if cmd.eq_ignore_ascii_case(b"FT.CONFIG") {
        if let Some(Frame::BulkString(sub) | Frame::SimpleString(sub)) = cmd_args.first() {
            return sub.eq_ignore_ascii_case(b"SET");
        }
    }
    false
}

/// True once the replication plane has anything to feed: a registered replica
/// or an allocated backlog (a PSYNC handshake allocates the backlog before any
/// snapshot is captured, so an FT.* mutation racing a first-ever attach still
/// fans out). Gates the serialize+SPSC round trip so servers that never
/// replicate pay nothing — mirrors `wal_fanout_has_work` on the KV path.
///
/// This is a deliberately SHORT-LIVED, independent lock from the one
/// [`record_local_write_db`]/[`record_local_write`] take: several call sites
/// reach an `.await` (e.g. the AOF `send_append_group` hop) between this gate
/// check and their next `repl_state` touch, so the gate's guard must not
/// outlive this function (task #70 — never hold a lock across `.await`).
/// Only the WRITE itself — `record_local_write_db`'s own internal chain,
/// entirely synchronous — collapses to one shared guard; see there.
#[inline]
pub(super) fn replication_fanout_active(ctx: &ConnectionContext) -> bool {
    // Cheap first gate: one Relaxed load; false until the first replica ever
    // begins attaching (REPLCONF/PSYNC → ensure_backlogs_allocated).
    if !crate::replication::state::fanout_hint_active() {
        return false;
    }
    ctx.repl_state.as_ref().is_some_and(|rs| {
        let g = rs.read();
        !g.replicas.is_empty()
            || g.per_shard_backlogs
                .first()
                .is_some_and(|slot| slot.lock().is_some())
    })
}

/// Record one successfully-executed local write in the replication plane.
///
/// The backlog append AND the shard-offset advance happen HERE, synchronously,
/// in the same no-await stretch as the keyspace mutation itself. This is the
/// linchpin of snapshot consistency: the inline PSYNC task reads
/// `total_offset()` in the same synchronous block as its RDB capture, so with
/// a synchronous advance a mutation baked into the RDB always has its offset
/// counted — the backlog catch-up range `[snapshot_offset, reg_offset)` can
/// never re-deliver it (double-applying INCR/LPUSH on the replica). The
/// previous design deferred backlog+offset to the event-loop drain of a
/// queued message, which opened exactly that window.
///
/// Only the live replica `try_send` is deferred (`ReplicaLiveFanout` on the
/// self queue): the replica sender list lives in the event loop's local
/// state. Exactly-once holds for every interleave with a registering replica
/// because `RegisterReplica.push_offset` is also captured at push time — a
/// write W and a registration R queued in either order agree on whether W is
/// below `reg_offset` (delivered via catch-up, fan-out message drains before
/// R registers) or at/above it (delivered live, catch-up excludes it).
///
/// A backlog slot that is `None` skips the byte append but still advances the
/// offset — mirroring `wal_append_and_fanout`: the offset counter is the
/// source of truth, and a later lazy allocation seeds the backlog at the
/// current counter (`ReplicationBacklog::new_at`).
///
/// Takes the `ReplicationState` guard by reference (task #70) instead of
/// re-locking `ctx.repl_state` itself, so a caller writing both a `SELECT`
/// prefix and a payload record (see [`record_local_write_db`]) pays for
/// exactly one `.read()` acquisition, not one per record.
///
/// ⚠ Monoio shard threads only (pushes to `shard::self_msg`) — callers are
/// all inside `handler_monoio`, which is `runtime-monoio`-gated.
#[inline]
pub(super) fn record_local_write(
    g: &crate::replication::state::ReplicationState,
    shard_id: usize,
    bytes: Bytes,
) {
    // Per-shard offset AFTER this record — the fan-out arm compares it
    // against each replica's snapshot cut (`ReplicaFanout::cut`) so a record
    // already inside a FULLRESYNC body is never live-delivered again.
    if let Some(slot) = g.per_shard_backlogs.get(shard_id) {
        if let Some(backlog) = slot.lock().as_mut() {
            backlog.append(&bytes);
        }
    }
    let end_offset = g.increment_shard_offset(shard_id, bytes.len() as u64);
    crate::shard::self_msg::push(crate::shard::dispatch::ShardMessage::ReplicaLiveFanout {
        bytes,
        end_offset,
    });
}

/// Db-aware variant of [`record_local_write`] (HIGH-2, task #22): prepends a
/// `SELECT <db>` record whenever the writing connection's logical db differs
/// from the stream's current db context (`ReplicationState::stream_db`), so a
/// replica's drain binds this command to the SAME db the master executed it
/// in. Both records go through `record_local_write` — backlog append + offset
/// advance stay synchronous with the mutation, and the SELECT is delivered in
/// order ahead of the payload on the same self-queue.
///
/// GRAPH.\* / TEMPORAL.\* records are db-agnostic on the replica (routed
/// before db resolution) but still use this variant: the uniform invariant is
/// "stream db context == the writing connection's selected db", so a
/// db-agnostic record can never silently strand a stale context for the next
/// KV write.
///
/// Task #70: takes exactly ONE `ctx.repl_state.read()` guard for the whole
/// call — shared by the `needs_select` decision and both potential
/// `record_local_write` calls (SELECT prefix + payload) — collapsing what was
/// previously up to 3 separate acquisitions (plus the caller's own
/// `replication_fanout_active` gate check) down to 1 here. `None` (no
/// `repl_state` configured) is a silent no-op, matching the prior behavior.
#[inline]
pub(super) fn record_local_write_db(ctx: &ConnectionContext, db: usize, bytes: Bytes) {
    let Some(rs) = ctx.repl_state.as_ref() else {
        return;
    };
    let g = rs.read();

    // R2 (task #20): multi-shard masters merge N shard streams onto one
    // replica wire, so the per-shard `stream_db` context tracking below is
    // meaningless there — another shard may have moved the wire's db context
    // between any two of this shard's records. Instead EVERY db-scoped record
    // is framed with its own `SELECT <db>` prefix, fused into ONE record so
    // no cross-shard interleave can split them. Gated on the fanout hint: a
    // multi-shard master that never had a replica attach pays nothing, and
    // the hint flips (process-global, then re-asserted by each shard's
    // `PrepareReplicaSync` arm BEFORE that shard's snapshot offset is
    // captured) before any of this shard's records can reach a wire.
    if ctx.num_shards > 1 {
        if crate::replication::state::fanout_hint_active() {
            let select = serialize_select(db);
            let mut combined = Vec::with_capacity(select.len() + bytes.len());
            combined.extend_from_slice(&select);
            combined.extend_from_slice(&bytes);
            record_local_write(&g, ctx.shard_id, Bytes::from(combined));
        } else {
            record_local_write(&g, ctx.shard_id, bytes);
        }
        return;
    }
    let needs_select = g.stream_db.get(ctx.shard_id).is_some_and(|slot| {
        if slot.load(std::sync::atomic::Ordering::Relaxed) != db as i64 {
            slot.store(db as i64, std::sync::atomic::Ordering::Relaxed);
            true
        } else {
            false
        }
    });
    if needs_select {
        record_local_write(&g, ctx.shard_id, Bytes::from(serialize_select(db)));
    }
    record_local_write(&g, ctx.shard_id, bytes);
}

/// RESP-serialize `SELECT <db>` for the replication stream.
fn serialize_select(db: usize) -> Vec<u8> {
    let mut n = itoa::Buffer::new();
    let db_str = n.format(db);
    let mut ln = itoa::Buffer::new();
    let mut buf = Vec::with_capacity(32);
    buf.extend_from_slice(b"*2\r\n$6\r\nSELECT\r\n$");
    buf.extend_from_slice(ln.format(db_str.len()).as_bytes());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(db_str.as_bytes());
    buf.extend_from_slice(b"\r\n");
    buf
}

/// Handle FT.* commands. Returns `true` if the command was consumed.
///
/// Caller should `continue` the frame loop when this returns `true`.
pub(super) async fn try_handle_ft_command(
    cmd: &[u8],
    cmd_args: &[Frame],
    frame: &Frame,
    conn: &ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if cmd.len() <= 3 || !cmd[..3].eq_ignore_ascii_case(b"FT.") {
        return false;
    }
    // Box::pin (c10k future diet): the FT.* body is a ~2.8 KB state machine
    // that would otherwise live inline in every connection future. The alloc
    // happens only when an FT.* command actually executes — never on the
    // name-mismatch fast path above — and is noise next to the search itself.
    Box::pin(ft_command_inner(cmd, cmd_args, frame, conn, ctx, responses)).await
}

async fn ft_command_inner(
    cmd: &[u8],
    cmd_args: &[Frame],
    frame: &Frame,
    conn: &ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if ctx.num_shards > 1 {
        // Multi-shard: dispatch via SPSC
        #[cfg(feature = "text-index")]
        if cmd.eq_ignore_ascii_case(b"FT.AGGREGATE") {
            // ── FT.AGGREGATE: multi-shard scatter-gather (Phase 152 Plan 03). ──
            // Mirrors handler_sharded.rs:1458. scatter_text_aggregate acquires
            // its own per-shard guards inside the single-shard block internally,
            // so we never hold a MutexGuard across the .await below.
            let parsed =
                match crate::command::vector_search::ft_aggregate::parse_aggregate_args(cmd_args) {
                    Ok(p) => p,
                    Err(err_frame) => {
                        responses.push(err_frame);
                        return true;
                    }
                };
            let response = crate::shard::scatter_aggregate::scatter_text_aggregate(
                parsed.index_name,
                parsed.query,
                parsed.pipeline,
                ctx.shard_id,
                ctx.num_shards,
                &ctx.shard_databases,
                &ctx.dispatch_tx,
                &ctx.spsc_notifiers,
                conn.selected_db as u8,
            )
            .await;
            responses.push(response);
            return true;
        }
        if cmd.eq_ignore_ascii_case(b"FT.SEARCH") {
            // Check if this is a text query BEFORE trying parse_ft_search_args
            // (which would return an error for non-KNN queries).
            let query_bytes = cmd_args
                .get(1)
                .and_then(|f| crate::command::vector_search::extract_bulk(f));
            let is_text = query_bytes
                .as_ref()
                .map_or(false, |q| crate::command::vector_search::is_text_query(q))
                && !crate::command::vector_search::has_sparse_clause(cmd_args);

            // ── HYBRID multi-shard path (Phase 152 Plan 05, D-13) ──────────
            #[cfg(feature = "text-index")]
            {
                match crate::command::vector_search::hybrid::parse_hybrid_modifier(cmd_args) {
                    Ok(Some(partial)) => {
                        let index_name = match cmd_args
                            .first()
                            .and_then(|f| crate::command::vector_search::extract_bulk(f))
                        {
                            Some(b) => b,
                            None => {
                                responses.push(Frame::Error(Bytes::from_static(
                                    b"ERR invalid index name",
                                )));
                                return true;
                            }
                        };
                        let text_query = match query_bytes.clone() {
                            Some(q) => q,
                            None => {
                                responses
                                    .push(Frame::Error(Bytes::from_static(b"ERR invalid query")));
                                return true;
                            }
                        };
                        let (limit_offset, limit_count) =
                            crate::command::vector_search::parse_limit_clause(cmd_args);
                        let top_k = if limit_count == usize::MAX {
                            limit_offset.saturating_add(10).max(1)
                        } else {
                            limit_offset.saturating_add(limit_count).max(1)
                        };
                        let hq = crate::command::vector_search::hybrid::HybridQuery {
                            index_name,
                            text_query,
                            dense_field: partial.dense_field,
                            dense_blob: partial.dense_blob,
                            sparse: partial.sparse,
                            weights: partial.weights,
                            k_per_stream: partial.k_per_stream,
                            top_k,
                            offset: limit_offset,
                            count: limit_count,
                            // CHANGE D: thread the parsed FILTER tree (if any) into
                            // HybridQuery so execute_hybrid_search_local / scatter
                            // can apply it pre-RRF on both branches (CHANGE B/F).
                            filter: partial.filter,
                        };
                        // Phase 171 HYB-02 / SCAT-02: resolve AS_OF / TXN LSN
                        // ONCE on the coordinator and forward to the scatter
                        // helper so responders honor temporal snapshots.
                        let as_of_lsn = match resolve_ft_search_as_of_lsn(
                            cmd_args,
                            Some(&ctx.shard_databases),
                            conn.active_cross_txn.as_deref(),
                        ) {
                            Ok(lsn) => lsn,
                            Err(err_frame) => {
                                responses.push(err_frame);
                                return true;
                            }
                        };
                        let response = crate::shard::scatter_hybrid::scatter_hybrid_search(
                            hq,
                            as_of_lsn,
                            ctx.shard_id,
                            ctx.num_shards,
                            &ctx.shard_databases,
                            &ctx.dispatch_tx,
                            &ctx.spsc_notifiers,
                            conn.selected_db as u8,
                        )
                        .await;
                        let mut response = response;
                        if let Some(ws_id) = conn.workspace_id.as_ref() {
                            strip_workspace_prefix_from_response(ws_id, cmd, &mut response);
                        }
                        responses.push(response);
                        return true;
                    }
                    Ok(None) => { /* fall through */ }
                    Err(err_frame) => {
                        responses.push(err_frame);
                        return true;
                    }
                }
            }

            if is_text {
                // ── Text FT.SEARCH: two-phase DFS scatter-gather ──────────────────
                let index_name = match cmd_args
                    .first()
                    .and_then(|f| crate::command::vector_search::extract_bulk(f))
                {
                    Some(b) => b,
                    None => {
                        responses.push(Frame::Error(Bytes::from_static(b"ERR invalid index name")));
                        return true;
                    }
                };
                // `is_text` was computed as `query_bytes.as_ref().map_or(false, ...)` (line ~65),
                // so reaching `if is_text { ... }` guarantees `query_bytes.is_some()`.
                #[allow(clippy::unwrap_used)]
                let query_str = query_bytes.unwrap();

                // fts-query-eval-dispatch 2b: pass raw query bytes to scatter_text_search.
                // The coordinator parses once (for Phase-1 df terms), then each shard
                // re-parses with the recursive-descent AST evaluator. This correctly
                // handles OR unions, multi-@clause intersections, tag/numeric filters,
                // and grouping — replacing the old pre_parse_field_filter / parse_text_query
                // pre-pass and the scatter_text_search_filter split path.
                let (offset, count) = crate::command::vector_search::parse_limit_clause(cmd_args);
                let top_k = if count == usize::MAX {
                    10000
                } else {
                    offset.saturating_add(count)
                }
                .max(1);

                let highlight_opts =
                    crate::command::vector_search::parse_highlight_clause(cmd_args);
                let summarize_opts =
                    crate::command::vector_search::parse_summarize_clause(cmd_args);

                let response = crate::shard::coordinator::scatter_text_search(
                    index_name,
                    query_str,
                    top_k,
                    offset,
                    count,
                    ctx.shard_id,
                    ctx.num_shards,
                    &ctx.shard_databases,
                    &ctx.dispatch_tx,
                    &ctx.spsc_notifiers,
                    highlight_opts,
                    summarize_opts,
                    conn.selected_db as u8,
                )
                .await;
                let mut response = response;
                if let Some(ws_id) = conn.workspace_id.as_ref() {
                    strip_workspace_prefix_from_response(ws_id, cmd, &mut response);
                }
                responses.push(response);
                return true;
            }

            // ── Vector FT.SEARCH (KNN / SPARSE): existing path ────────────────
            // Phase 171 SCAT-01: resolve AS_OF / TXN snapshot LSN ONCE on
            // the coordinator and forward through the scatter helper so
            // every responder honors the same temporal snapshot.
            let response = match crate::command::vector_search::parse_ft_search_args(cmd_args) {
                Ok((index_name, query_blob, k, filter, _offset, _count)) => {
                    if filter.is_some() {
                        Frame::Error(Bytes::from_static(
                            b"ERR FILTER not supported in multi-shard mode yet",
                        ))
                    } else {
                        match resolve_ft_search_as_of_lsn(
                            cmd_args,
                            Some(&ctx.shard_databases),
                            conn.active_cross_txn.as_deref(),
                        ) {
                            Err(err_frame) => err_frame,
                            Ok(as_of_lsn) => {
                                crate::shard::coordinator::scatter_vector_search_remote(
                                    index_name,
                                    query_blob,
                                    k,
                                    as_of_lsn,
                                    ctx.shard_id,
                                    ctx.num_shards,
                                    &ctx.shard_databases,
                                    &ctx.dispatch_tx,
                                    &ctx.spsc_notifiers,
                                    conn.selected_db as u8,
                                )
                                .await
                            }
                        }
                    }
                }
                Err(err_frame) => err_frame,
            };
            let mut response = response;
            if let Some(ws_id) = conn.workspace_id.as_ref() {
                strip_workspace_prefix_from_response(ws_id, cmd, &mut response);
            }
            responses.push(response);
            return true;
        }
        if cmd.eq_ignore_ascii_case(b"FT.CREATE") || cmd.eq_ignore_ascii_case(b"FT.DROPINDEX") {
            // Broadcast to ALL shards so every shard has the index
            let response = crate::shard::coordinator::broadcast_vector_command(
                std::sync::Arc::new(frame.clone()),
                ctx.shard_id,
                ctx.num_shards,
                &ctx.shard_databases,
                &ctx.dispatch_tx,
                &ctx.spsc_notifiers,
                conn.selected_db as u8,
            )
            .await;
            responses.push(response);
            return true;
        }
        if cmd.eq_ignore_ascii_case(b"FT.INFO") {
            // Scatter + sum additive stats across shards (XC-SHARD-1): the local
            // shard's index holds only its key-hash partition of the documents.
            let response = crate::shard::coordinator::scatter_ft_info(
                std::sync::Arc::new(frame.clone()),
                ctx.shard_id,
                ctx.num_shards,
                &ctx.shard_databases,
                &ctx.dispatch_tx,
                &ctx.spsc_notifiers,
                conn.selected_db as u8,
            )
            .await;
            responses.push(response);
            return true;
        }
        if cmd.eq_ignore_ascii_case(b"FT._LIST") {
            let db_index = conn.selected_db as u8;
            let response = crate::shard::slice::with_shard(|s| {
                crate::command::vector_search::ft_list(&s.vector_store, &s.text_store, db_index)
            });
            responses.push(response);
            return true;
        }
        if cmd.eq_ignore_ascii_case(b"FT.COMPACT") {
            let db_index = conn.selected_db as u8;
            let response = crate::shard::slice::with_shard(|s| {
                crate::command::vector_search::ft_compact(
                    &mut s.vector_store,
                    &mut s.text_store,
                    cmd_args,
                    db_index,
                )
            });
            responses.push(response);
            return true;
        }
        if cmd.eq_ignore_ascii_case(b"FT.CACHESEARCH") {
            let db_index = conn.selected_db as u8;
            let response = crate::shard::slice::with_shard(|s| {
                crate::command::vector_search::cache_search::ft_cachesearch(
                    &mut s.vector_store,
                    cmd_args,
                    db_index,
                )
            });
            responses.push(response);
            return true;
        }
        if cmd.eq_ignore_ascii_case(b"FT.CONFIG") {
            // SET must broadcast: each shard holds its own index partition, and a
            // local-only write would leave 7/8 shards on the old value (the tokio
            // sharded handler already broadcasts FT.CONFIG via its catch-all).
            // GET stays local — post-broadcast, every shard agrees. The
            // connection's SELECTed db rides the broadcast (WS5a db scoping).
            let db_index = conn.selected_db as u8;
            let is_set = cmd_args
                .first()
                .and_then(|f| match f {
                    crate::protocol::Frame::BulkString(b) => Some(b.as_ref()),
                    _ => None,
                })
                .is_some_and(|s| s.eq_ignore_ascii_case(b"SET"));
            let response = if is_set {
                crate::shard::coordinator::broadcast_vector_command(
                    std::sync::Arc::new(frame.clone()),
                    ctx.shard_id,
                    ctx.num_shards,
                    &ctx.shard_databases,
                    &ctx.dispatch_tx,
                    &ctx.spsc_notifiers,
                    db_index,
                )
                .await
            } else {
                crate::shard::slice::with_shard(|s| {
                    crate::command::vector_search::ft_config(
                        &mut s.vector_store,
                        &mut s.text_store,
                        cmd_args,
                        db_index,
                    )
                })
            };
            responses.push(response);
            return true;
        }
        // FT.RECOMMEND, FT.NAVIGATE, FT.EXPAND need db/graph — dispatch locally
        if cmd.eq_ignore_ascii_case(b"FT.RECOMMEND")
            || cmd.eq_ignore_ascii_case(b"FT.NAVIGATE")
            || cmd.eq_ignore_ascii_case(b"FT.EXPAND")
        {
            let db_index = conn.selected_db as u8;
            let response = crate::shard::slice::with_shard(|s| {
                if cmd.eq_ignore_ascii_case(b"FT.RECOMMEND") {
                    crate::command::vector_search::recommend::ft_recommend(
                        &mut s.vector_store,
                        cmd_args,
                        s.databases.get_mut(db_index as usize),
                        db_index,
                    )
                } else if cmd.eq_ignore_ascii_case(b"FT.NAVIGATE") {
                    #[cfg(feature = "graph")]
                    {
                        crate::command::vector_search::navigate::ft_navigate(
                            &mut s.vector_store,
                            Some(&s.graph_store),
                            cmd_args,
                            None,
                            db_index,
                        )
                    }
                    #[cfg(not(feature = "graph"))]
                    {
                        Frame::Error(Bytes::from_static(
                            b"ERR FT.NAVIGATE requires graph feature",
                        ))
                    }
                } else {
                    // FT.EXPAND
                    #[cfg(feature = "graph")]
                    {
                        crate::command::vector_search::ft_expand(&s.graph_store, cmd_args)
                    }
                    #[cfg(not(feature = "graph"))]
                    {
                        Frame::Error(Bytes::from_static(b"ERR FT.EXPAND requires graph feature"))
                    }
                }
            });
            responses.push(response);
            return true;
        }
        #[cfg(feature = "text-index")]
        if cmd.eq_ignore_ascii_case(b"FT.INVALIDATE_RANGE") {
            let response = crate::shard::coordinator::scatter_invalidate_range(
                std::sync::Arc::new(frame.clone()),
                ctx.shard_id,
                ctx.num_shards,
                &ctx.shard_databases,
                &ctx.dispatch_tx,
                &ctx.spsc_notifiers,
                conn.selected_db as u8,
            )
            .await;
            responses.push(response);
            return true;
        }
        #[cfg(not(feature = "text-index"))]
        if cmd.eq_ignore_ascii_case(b"FT.INVALIDATE_RANGE") {
            responses.push(Frame::Error(Bytes::from_static(
                b"ERR FT.INVALIDATE_RANGE requires text-index feature",
            )));
            return true;
        }
        responses.push(Frame::Error(Bytes::from_static(b"ERR unknown FT command")));
        return true;
    } else {
        // Single-shard: no SPSC channels needed.
        // Dispatch directly to shard's VectorStore via shared access.
        //
        // ── 154-01 single-shard FT.AGGREGATE fast path ────────────────
        // scatter_text_aggregate internally fast-paths num_shards == 1
        // to execute_local_full with locally-acquired guards dropped
        // before any .await. Calling the scatter entry here (instead
        // of execute_local_full directly) keeps the dispatch body
        // byte-symmetric with the multi-shard site above.
        #[cfg(feature = "text-index")]
        if cmd.eq_ignore_ascii_case(b"FT.AGGREGATE") {
            let parsed =
                match crate::command::vector_search::ft_aggregate::parse_aggregate_args(cmd_args) {
                    Ok(p) => p,
                    Err(err_frame) => {
                        responses.push(err_frame);
                        return true;
                    }
                };
            let response = crate::shard::scatter_aggregate::scatter_text_aggregate(
                parsed.index_name,
                parsed.query,
                parsed.pipeline,
                ctx.shard_id,
                ctx.num_shards,
                &ctx.shard_databases,
                &ctx.dispatch_tx,
                &ctx.spsc_notifiers,
                conn.selected_db as u8,
            )
            .await;
            responses.push(response);
            return true;
        }
        //
        // ── 151-03 single-shard text FT.SEARCH fast path ──────────────
        // Bare text queries (exact / fuzzy / prefix / field-targeted)
        // are not understood by `ft_search()` (which only parses
        // KNN / SPARSE / HYBRID) — they would otherwise return
        // `ERR invalid KNN query syntax`. Route them directly to
        // `run_text_query` here (see below) — NOT `execute_text_search_local`,
        // which is dead code outside its own test module (adversarial-review
        // round-2 hygiene fix; the real multi-shard path is
        // `scatter_text_search` → `run_text_query_on_index`). We skip HYBRID
        // (existing ft_search handles it) and KNN/SPARSE (is_text_query
        // returns false).
        #[cfg(feature = "text-index")]
        if cmd.eq_ignore_ascii_case(b"FT.SEARCH") {
            if let Some(Frame::BulkString(query_bytes)) = cmd_args.get(1) {
                match crate::command::vector_search::parse_hybrid_modifier(cmd_args) {
                    Ok(Some(_)) => {
                        // HYBRID present — defer to existing ft_search() below.
                    }
                    Err(frame_err) => {
                        responses.push(frame_err);
                        return true;
                    }
                    Ok(None) => {
                        if crate::command::vector_search::is_text_query(query_bytes.as_ref())
                            && !crate::command::vector_search::has_sparse_clause(cmd_args)
                        {
                            // Step 1: index_name from cmd_args[0].
                            let index_name = match cmd_args.first() {
                                Some(Frame::BulkString(b)) => b.clone(),
                                _ => {
                                    responses.push(Frame::Error(Bytes::from_static(
                                        b"ERR wrong number of arguments for FT.SEARCH",
                                    )));
                                    return true;
                                }
                            };
                            // fts-query-eval-dispatch 2b: single-shard local fast path.
                            // run_text_query handles index lookup, parse, eval, tag/numeric
                            // filters, OR, multi-@clause combinators in one call.
                            // HIGHLIGHT/SUMMARIZE: re-parse inside the same with_shard closure
                            // so text_index and databases[0] are borrowed disjointly off `s`.
                            let (offset, count) =
                                crate::command::vector_search::parse_limit_clause(cmd_args);
                            let top_k = if count == usize::MAX {
                                10000
                            } else {
                                offset.saturating_add(count)
                            }
                            .max(1);
                            let highlight_opts =
                                crate::command::vector_search::parse_highlight_clause(cmd_args);
                            let summarize_opts =
                                crate::command::vector_search::parse_summarize_clause(cmd_args);
                            let need_hl = highlight_opts.is_some() || summarize_opts.is_some();

                            let db_index = conn.selected_db as u8;
                            let mut response = crate::shard::slice::with_shard(|s| {
                                #[cfg(feature = "text-index")]
                                {
                                    let mut r = crate::command::vector_search::run_text_query(
                                        &s.text_store,
                                        &index_name,
                                        query_bytes.as_ref(),
                                        top_k,
                                        offset,
                                        count,
                                        db_index,
                                    );
                                    if need_hl {
                                        if let Some(text_index) =
                                            s.text_store.get_index_for_db(&index_name, db_index)
                                        {
                                            if let Ok(node) = crate::text::query::parse_query(
                                                query_bytes.as_ref(),
                                                &crate::text::query::QuerySchema::from_index(
                                                    text_index,
                                                ),
                                            ) {
                                                let terms =
                                                    crate::text::query::collect_highlight_terms(
                                                        &node, text_index,
                                                    );
                                                let db = &s.databases[0];
                                                crate::command::vector_search::apply_post_processing(
                                                    &mut r,
                                                    &terms,
                                                    text_index,
                                                    db,
                                                    highlight_opts.as_ref(),
                                                    summarize_opts.as_ref(),
                                                );
                                            }
                                        }
                                    }
                                    r
                                }
                                #[cfg(not(feature = "text-index"))]
                                Frame::Error(Bytes::from_static(
                                    b"ERR text-index feature not enabled",
                                ))
                            });
                            if let Some(ws_id) = conn.workspace_id.as_ref() {
                                strip_workspace_prefix_from_response(ws_id, cmd, &mut response);
                            }
                            responses.push(response);
                            return true;
                        }
                    }
                }
            }
        }
        // AS_OF temporal resolution uses ctx.shard_databases (temporal_registry
        // migrated in Phase 2b). Both paths share the same AS_OF resolution logic.
        let as_of_lsn_result = if cmd.eq_ignore_ascii_case(b"FT.SEARCH") {
            resolve_ft_search_as_of_lsn(
                cmd_args,
                Some(&ctx.shard_databases),
                conn.active_cross_txn.as_deref(),
            )
        } else {
            Ok(0u64)
        };
        let as_of_lsn = match as_of_lsn_result {
            Ok(lsn) => lsn,
            Err(err_frame) => {
                responses.push(err_frame);
                return true;
            }
        };
        // FT.SEARCH cooperative-yield seam (ft-search-off-eventloop §3): capture an
        // OWNED snapshot inside the shard slice, release the `&mut s` borrow, then
        // `.await` the chunked yielding search so a heavy KNN scan interleaves with
        // the 1ms tick and co-located commands instead of monopolizing the event
        // loop. Non-yieldable shapes (HYBRID/SPARSE/SESSION/RANGE/non-default-field/
        // unknown-index/parse-error) come back as `FtSearchPlan::Sync` from the
        // proven synchronous path — byte-identical to the legacy behavior.
        if cmd.eq_ignore_ascii_case(b"FT.SEARCH") {
            let has_session = cmd_args.iter().any(|a| {
                if let Frame::BulkString(b) = a {
                    b.eq_ignore_ascii_case(b"SESSION")
                } else {
                    false
                }
            });
            let db_index = conn.selected_db as u8;
            let plan = crate::shard::slice::with_shard(|s| {
                if has_session {
                    crate::command::vector_search::ft_search_capture(
                        &mut s.vector_store,
                        cmd_args,
                        s.databases.get_mut(db_index as usize),
                        Some(&s.text_store),
                        as_of_lsn,
                        db_index,
                    )
                } else {
                    crate::command::vector_search::ft_search_capture(
                        &mut s.vector_store,
                        cmd_args,
                        None,
                        Some(&s.text_store),
                        as_of_lsn,
                        db_index,
                    )
                }
            }); // shard-slice borrow released here — snapshot is owned ('static)
            let mut response = match plan {
                crate::command::vector_search::FtSearchPlan::Sync(frame) => frame,
                crate::command::vector_search::FtSearchPlan::Yield {
                    mut snapshot,
                    offset,
                    count,
                } => {
                    // #18: await any off-loop COLD→WARM reloads submitted at
                    // capture (parks this task, not the shard thread) so the
                    // scan below sees full recall while siblings keep running.
                    snapshot.await_pending_reloads().await;
                    let results =
                        crate::vector::segment::holder::SegmentHolder::search_mvcc_yielding(
                            &mut *snapshot,
                            crate::vector::segment::holder::ft_search_yield_budget(),
                        )
                        .await;
                    crate::command::vector_search::build_search_response(
                        &results,
                        &snapshot.key_hash_to_key,
                        offset,
                        count,
                    )
                }
            };
            if let Some(ws_id) = conn.workspace_id.as_ref() {
                strip_workspace_prefix_from_response(ws_id, cmd, &mut response);
            }
            responses.push(response);
            return true;
        }
        let db_index = conn.selected_db as u8;
        let response = crate::shard::slice::with_shard(|s| {
            if cmd.eq_ignore_ascii_case(b"FT.CREATE") {
                crate::command::vector_search::ft_create(
                    &mut s.vector_store,
                    &mut s.text_store,
                    cmd_args,
                    db_index,
                )
            } else if cmd.eq_ignore_ascii_case(b"FT.DROPINDEX") {
                crate::command::vector_search::ft_dropindex(
                    &mut s.vector_store,
                    &mut s.text_store,
                    s.databases.get_mut(db_index as usize),
                    cmd_args,
                    db_index,
                )
            } else if cmd.eq_ignore_ascii_case(b"FT.INFO") {
                crate::command::vector_search::ft_info(
                    &s.vector_store,
                    &s.text_store,
                    cmd_args,
                    db_index,
                )
            } else if cmd.eq_ignore_ascii_case(b"FT._LIST") {
                crate::command::vector_search::ft_list(&s.vector_store, &s.text_store, db_index)
            } else if cmd.eq_ignore_ascii_case(b"FT.COMPACT") {
                crate::command::vector_search::ft_compact(
                    &mut s.vector_store,
                    &mut s.text_store,
                    cmd_args,
                    db_index,
                )
            } else if cmd.eq_ignore_ascii_case(b"FT.CACHESEARCH") {
                crate::command::vector_search::cache_search::ft_cachesearch(
                    &mut s.vector_store,
                    cmd_args,
                    db_index,
                )
            } else if cmd.eq_ignore_ascii_case(b"FT.CONFIG") {
                crate::command::vector_search::ft_config(
                    &mut s.vector_store,
                    &mut s.text_store,
                    cmd_args,
                    db_index,
                )
            } else if cmd.eq_ignore_ascii_case(b"FT.RECOMMEND") {
                crate::command::vector_search::recommend::ft_recommend(
                    &mut s.vector_store,
                    cmd_args,
                    s.databases.get_mut(db_index as usize),
                    db_index,
                )
            } else if cmd.eq_ignore_ascii_case(b"FT.NAVIGATE") {
                #[cfg(feature = "graph")]
                {
                    crate::command::vector_search::navigate::ft_navigate(
                        &mut s.vector_store,
                        Some(&s.graph_store),
                        cmd_args,
                        None,
                        db_index,
                    )
                }
                #[cfg(not(feature = "graph"))]
                {
                    Frame::Error(Bytes::from_static(
                        b"ERR FT.NAVIGATE requires graph feature",
                    ))
                }
            } else if cmd.eq_ignore_ascii_case(b"FT.EXPAND") {
                #[cfg(feature = "graph")]
                {
                    crate::command::vector_search::ft_expand(&s.graph_store, cmd_args)
                }
                #[cfg(not(feature = "graph"))]
                {
                    Frame::Error(Bytes::from_static(b"ERR FT.EXPAND requires graph feature"))
                }
            } else if cmd.eq_ignore_ascii_case(b"FT.INVALIDATE_RANGE") {
                #[cfg(feature = "text-index")]
                {
                    crate::command::vector_search::ft_invalidate_range(
                        &mut s.text_store,
                        cmd_args,
                        db_index,
                    )
                }
                #[cfg(not(feature = "text-index"))]
                {
                    Frame::Error(Bytes::from_static(
                        b"ERR FT.INVALIDATE_RANGE requires text-index feature",
                    ))
                }
            } else {
                Frame::Error(Bytes::from_static(b"ERR unknown FT.* command"))
            }
        });
        // v0.7 R0.5: index-DEFINITION mutations must reach replicas. FT.*
        // executes here at the connection layer (never crosses the SPSC write
        // path), so on success record the ORIGINAL command bytes in the
        // replication plane — exact parity, no reconstruction. Single-shard
        // only (multi-shard FT.* replication rides the R2 broadcast
        // redesign). The replica applies it through the same
        // ft_create/ft_dropindex/ft_config handlers.
        if !matches!(response, Frame::Error(_))
            && is_replicated_ft_def_mutation(cmd, cmd_args)
            && replication_fanout_active(ctx)
        {
            let serialized = crate::persistence::aof::serialize_command(frame);
            record_local_write_db(ctx, conn.selected_db, serialized);
        }
        let mut response = response;
        if let Some(ws_id) = conn.workspace_id.as_ref() {
            strip_workspace_prefix_from_response(ws_id, cmd, &mut response);
        }
        responses.push(response);
        return true;
    }
    // Unreachable: all paths above return true
    #[allow(unreachable_code)]
    true
}
