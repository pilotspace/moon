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
                .map_or(false, |q| crate::command::vector_search::is_text_query(q));

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
                        };
                        // Phase 171 HYB-02 / SCAT-02: resolve AS_OF / TXN LSN
                        // ONCE on the coordinator and forward to the scatter
                        // helper so responders honor temporal snapshots.
                        let as_of_lsn = match resolve_ft_search_as_of_lsn(
                            cmd_args,
                            Some(&ctx.shard_databases),
                            ctx.shard_id,
                            conn.active_cross_txn.as_ref(),
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

                // B-01 SITE 2 FIX (Plan 152-06): FieldFilter short-circuit BEFORE
                // the analyzer-first parse_result block. TAG queries (and Plan 07
                // NumericRange) route through the InvertedSearch fan-out — no
                // analyzer touched, no field_idx resolution (the filter carries
                // its own field name; search_tag resolves against tag_fields).
                #[cfg(feature = "text-index")]
                {
                    match crate::command::vector_search::pre_parse_field_filter(query_str.as_ref())
                    {
                        Ok(Some(clause)) => {
                            if let Some(filter) = clause.filter {
                                let (offset, count) =
                                    crate::command::vector_search::parse_limit_clause(cmd_args);
                                let top_k = if count == usize::MAX {
                                    10000
                                } else {
                                    offset.saturating_add(count)
                                }
                                .max(1);
                                let response =
                                    crate::shard::coordinator::scatter_text_search_filter(
                                        index_name,
                                        filter,
                                        top_k,
                                        offset,
                                        count,
                                        ctx.shard_id,
                                        ctx.num_shards,
                                        &ctx.shard_databases,
                                        &ctx.dispatch_tx,
                                        &ctx.spsc_notifiers,
                                    )
                                    .await;
                                let mut response = response;
                                if let Some(ws_id) = conn.workspace_id.as_ref() {
                                    strip_workspace_prefix_from_response(ws_id, cmd, &mut response);
                                }
                                responses.push(response);
                                return true;
                            }
                        }
                        Ok(None) => { /* fall through to BM25 path */ }
                        Err(e) => {
                            responses.push(Frame::Error(Bytes::from(e.to_owned())));
                            return true;
                        }
                    }
                }

                // Parse query and resolve field_idx inside a block scope so the
                // MutexGuard from text_store() is dropped BEFORE .await.
                // We use the TextIndex's own field_analyzers (same pipeline used at index time).
                type ParseResult =
                    Result<(Vec<crate::command::vector_search::QueryTerm>, Option<usize>), String>;
                let parse_result: ParseResult = {
                    let ts = ctx.shard_databases.text_store(ctx.shard_id);
                    match ts.get_index(&index_name) {
                        None => Err("ERR no such index".to_owned()),
                        Some(text_index) => {
                            match text_index.field_analyzers.first() {
                                None => Err("ERR index has no TEXT fields".to_owned()),
                                Some(analyzer) => {
                                    // analyzer borrows text_index which borrows ts — all in this block.
                                    let parsed = crate::command::vector_search::parse_text_query(
                                        &query_str, analyzer,
                                    );
                                    match parsed {
                                        Err(e) => Err(e.to_owned()),
                                        Ok(clause) => {
                                            let field_idx = match &clause.field_name {
                                                None => Ok(None),
                                                Some(field_name) => match text_index
                                                    .text_fields
                                                    .iter()
                                                    .position(|f| {
                                                        f.field_name.as_ref().eq_ignore_ascii_case(
                                                            field_name.as_ref(),
                                                        )
                                                    }) {
                                                    Some(idx) => Ok(Some(idx)),
                                                    None => Err(format!(
                                                        "ERR unknown field '{}'",
                                                        String::from_utf8_lossy(field_name)
                                                    )),
                                                },
                                            };
                                            field_idx.map(|idx| (clause.terms, idx))
                                        }
                                    }
                                }
                            }
                        }
                    }
                }; // MutexGuard dropped here

                let (query_terms, field_idx) = match parse_result {
                    Ok(t) => t,
                    Err(e) => {
                        responses.push(Frame::Error(Bytes::from(e)));
                        return true;
                    }
                };

                let (offset, count) = crate::command::vector_search::parse_limit_clause(cmd_args);
                let top_k = if count == usize::MAX {
                    10000
                } else {
                    offset.saturating_add(count)
                }
                .max(1);

                // Parse optional HIGHLIGHT/SUMMARIZE clauses from args.
                let highlight_opts =
                    crate::command::vector_search::parse_highlight_clause(cmd_args);
                let summarize_opts =
                    crate::command::vector_search::parse_summarize_clause(cmd_args);

                let response = crate::shard::coordinator::scatter_text_search(
                    index_name,
                    query_terms,
                    field_idx,
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
                            ctx.shard_id,
                            conn.active_cross_txn.as_ref(),
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
            )
            .await;
            responses.push(response);
            return true;
        }
        if cmd.eq_ignore_ascii_case(b"FT.INFO") {
            let response = if crate::shard::slice::is_initialized() {
                crate::shard::slice::with_shard(|s| {
                    crate::command::vector_search::ft_info(&s.vector_store, &s.text_store, cmd_args)
                })
            } else {
                let vs = ctx.shard_databases.vector_store(ctx.shard_id);
                let ts = ctx.shard_databases.text_store(ctx.shard_id);
                crate::command::vector_search::ft_info(&vs, &ts, cmd_args)
            };
            responses.push(response);
            return true;
        }
        if cmd.eq_ignore_ascii_case(b"FT._LIST") {
            let response = if crate::shard::slice::is_initialized() {
                crate::shard::slice::with_shard(|s| {
                    crate::command::vector_search::ft_list(&s.vector_store)
                })
            } else {
                let vs = ctx.shard_databases.vector_store(ctx.shard_id);
                crate::command::vector_search::ft_list(&vs)
            };
            responses.push(response);
            return true;
        }
        if cmd.eq_ignore_ascii_case(b"FT.COMPACT") {
            let response = if crate::shard::slice::is_initialized() {
                crate::shard::slice::with_shard(|s| {
                    crate::command::vector_search::ft_compact(
                        &mut s.vector_store,
                        &mut s.text_store,
                        cmd_args,
                    )
                })
            } else {
                let mut vs = ctx.shard_databases.vector_store(ctx.shard_id);
                let mut ts = ctx.shard_databases.text_store(ctx.shard_id);
                crate::command::vector_search::ft_compact(&mut vs, &mut ts, cmd_args)
            };
            responses.push(response);
            return true;
        }
        if cmd.eq_ignore_ascii_case(b"FT.CACHESEARCH") {
            let response = if crate::shard::slice::is_initialized() {
                crate::shard::slice::with_shard(|s| {
                    crate::command::vector_search::cache_search::ft_cachesearch(
                        &mut s.vector_store,
                        cmd_args,
                    )
                })
            } else {
                let mut vs = ctx.shard_databases.vector_store(ctx.shard_id);
                crate::command::vector_search::cache_search::ft_cachesearch(&mut vs, cmd_args)
            };
            responses.push(response);
            return true;
        }
        if cmd.eq_ignore_ascii_case(b"FT.CONFIG") {
            let response = if crate::shard::slice::is_initialized() {
                crate::shard::slice::with_shard(|s| {
                    crate::command::vector_search::ft_config(
                        &mut s.vector_store,
                        &mut s.text_store,
                        cmd_args,
                    )
                })
            } else {
                let mut vs = ctx.shard_databases.vector_store(ctx.shard_id);
                let mut ts = ctx.shard_databases.text_store(ctx.shard_id);
                crate::command::vector_search::ft_config(&mut vs, &mut ts, cmd_args)
            };
            responses.push(response);
            return true;
        }
        // FT.RECOMMEND, FT.NAVIGATE, FT.EXPAND need db/graph — dispatch locally
        if cmd.eq_ignore_ascii_case(b"FT.RECOMMEND")
            || cmd.eq_ignore_ascii_case(b"FT.NAVIGATE")
            || cmd.eq_ignore_ascii_case(b"FT.EXPAND")
        {
            let response = if crate::shard::slice::is_initialized() {
                crate::shard::slice::with_shard(|s| {
                    if cmd.eq_ignore_ascii_case(b"FT.RECOMMEND") {
                        crate::command::vector_search::recommend::ft_recommend(
                            &mut s.vector_store,
                            cmd_args,
                            Some(&mut s.databases[0]),
                        )
                    } else if cmd.eq_ignore_ascii_case(b"FT.NAVIGATE") {
                        #[cfg(feature = "graph")]
                        {
                            crate::command::vector_search::navigate::ft_navigate(
                                &mut s.vector_store,
                                Some(&s.graph_store),
                                cmd_args,
                                None,
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
                            Frame::Error(Bytes::from_static(
                                b"ERR FT.EXPAND requires graph feature",
                            ))
                        }
                    }
                })
            } else {
                let sdb = &ctx.shard_databases;
                let mut vs = sdb.vector_store(ctx.shard_id);
                if cmd.eq_ignore_ascii_case(b"FT.RECOMMEND") {
                    let mut db_guard = sdb.write_db(ctx.shard_id, 0);
                    crate::command::vector_search::recommend::ft_recommend(
                        &mut vs,
                        cmd_args,
                        Some(&mut *db_guard),
                    )
                } else if cmd.eq_ignore_ascii_case(b"FT.NAVIGATE") {
                    #[cfg(feature = "graph")]
                    {
                        let graph_guard = sdb.graph_store_read(ctx.shard_id);
                        crate::command::vector_search::navigate::ft_navigate(
                            &mut vs,
                            Some(&graph_guard),
                            cmd_args,
                            None,
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
                        let graph_guard = sdb.graph_store_read(ctx.shard_id);
                        crate::command::vector_search::ft_expand(&graph_guard, cmd_args)
                    }
                    #[cfg(not(feature = "graph"))]
                    {
                        Frame::Error(Bytes::from_static(
                            b"ERR FT.EXPAND requires graph feature",
                        ))
                    }
                }
            };
            responses.push(response);
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
        // `execute_text_search_local` here, the same function the
        // multi-shard path uses once its per-shard scatter has
        // aggregated IDFs. We skip HYBRID (existing ft_search
        // handles it) and KNN/SPARSE (is_text_query returns false).
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
                        if crate::command::vector_search::is_text_query(query_bytes.as_ref()) {
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
                            // B-01 SITE 2 FIX (single-shard 151-03 fast path, Plan 152-06):
                            // FieldFilter short-circuit BEFORE the analyzer lookup and
                            // BEFORE the text_fields.is_empty() bail.
                            #[cfg(feature = "text-index")]
                            match crate::command::vector_search::pre_parse_field_filter(
                                query_bytes.as_ref(),
                            ) {
                                Ok(Some(clause)) => {
                                    if clause.filter.is_some() {
                                        let (offset, count) =
                                            crate::command::vector_search::parse_limit_clause(
                                                cmd_args,
                                            );
                                        let top_k = if count == usize::MAX {
                                            10000
                                        } else {
                                            offset.saturating_add(count)
                                        }
                                        .max(1);
                                        let response = if crate::shard::slice::is_initialized() {
                                            crate::shard::slice::with_shard(|s| {
                                                match s.text_store.get_index(&index_name) {
                                                    None => Frame::Error(Bytes::from_static(
                                                        b"ERR no such index",
                                                    )),
                                                    Some(text_index) => {
                                                        let results = crate::command::vector_search::ft_text_search::execute_query_on_index(
                                                            text_index, &clause, None, None, top_k,
                                                        );
                                                        crate::command::vector_search::ft_text_search::build_text_response(
                                                            &results, offset, count,
                                                        )
                                                    }
                                                }
                                            })
                                        } else {
                                            let ts_guard = ctx.shard_databases.text_store(ctx.shard_id);
                                            let r = match ts_guard.get_index(&index_name) {
                                                None => Frame::Error(Bytes::from_static(
                                                    b"ERR no such index",
                                                )),
                                                Some(text_index) => {
                                                    let results = crate::command::vector_search::ft_text_search::execute_query_on_index(
                                                        text_index, &clause, None, None, top_k,
                                                    );
                                                    crate::command::vector_search::ft_text_search::build_text_response(
                                                        &results, offset, count,
                                                    )
                                                }
                                            };
                                            r
                                        };
                                        let mut response = response;
                                        if let Some(ws_id) = conn.workspace_id.as_ref() {
                                            strip_workspace_prefix_from_response(
                                                ws_id,
                                                cmd,
                                                &mut response,
                                            );
                                        }
                                        responses.push(response);
                                        return true;
                                    }
                                }
                                Ok(None) => { /* fall through */ }
                                Err(e) => {
                                    responses
                                        .push(Frame::Error(Bytes::copy_from_slice(e.as_bytes())));
                                    return true;
                                }
                            }
                            // Step 7: LIMIT parsing + top_k cap (T-151-03-02).
                            let (offset, count) =
                                crate::command::vector_search::parse_limit_clause(cmd_args);
                            let top_k = if count == usize::MAX {
                                10000
                            } else {
                                offset.saturating_add(count)
                            }
                            .max(1);
                            // Step 8: HIGHLIGHT / SUMMARIZE options.
                            let highlight_opts =
                                crate::command::vector_search::parse_highlight_clause(cmd_args);
                            let summarize_opts =
                                crate::command::vector_search::parse_summarize_clause(cmd_args);
                            let needs_db = highlight_opts.is_some() || summarize_opts.is_some();
                            // Steps 2-10: acquire stores and execute. Result<Frame, Frame>
                            // where Ok = response to push, Err = error frame with early-return.
                            // Phase 2a: gate on is_initialized(); new path uses ShardSlice directly.
                            let text_search_result: Result<Frame, Frame> =
                                if crate::shard::slice::is_initialized() {
                                    crate::shard::slice::with_shard(|s| {
                                        // Step 2-3: resolve index from text_store.
                                        let text_index =
                                            match s.text_store.get_index(&index_name) {
                                                Some(idx) => idx,
                                                None => return Err(Frame::Error(
                                                    Bytes::from_static(b"ERR no such index"),
                                                )),
                                            };
                                        // Step 4: ensure TEXT fields.
                                        if text_index.text_fields.is_empty() {
                                            return Err(Frame::Error(Bytes::from_static(
                                                b"ERR index has no TEXT fields",
                                            )));
                                        }
                                        // Step 5: parse query.
                                        let analyzer =
                                            match text_index.field_analyzers.first() {
                                                Some(a) => a,
                                                None => return Err(Frame::Error(
                                                    Bytes::from_static(
                                                        b"ERR index has no TEXT fields",
                                                    ),
                                                )),
                                            };
                                        let clause = match crate::command::vector_search::parse_text_query(
                                            query_bytes.as_ref(),
                                            analyzer,
                                        ) {
                                            Ok(c) => c,
                                            Err(msg) => return Err(Frame::Error(
                                                Bytes::copy_from_slice(msg.as_bytes()),
                                            )),
                                        };
                                        // Step 5b: resolve field_idx.
                                        let field_idx = match &clause.field_name {
                                            None => None,
                                            Some(field_name) => {
                                                match text_index.text_fields.iter().position(|f| {
                                                    f.field_name
                                                        .as_ref()
                                                        .eq_ignore_ascii_case(field_name.as_ref())
                                                }) {
                                                    Some(idx) => Some(idx),
                                                    None => {
                                                        let bad = field_name.clone();
                                                        return Err(Frame::Error(Bytes::from(
                                                            format!(
                                                                "ERR unknown field '{}'",
                                                                String::from_utf8_lossy(&bad)
                                                            ),
                                                        )));
                                                    }
                                                }
                                            }
                                        };
                                        let query_terms = clause.terms;
                                        // Step 10: execute.
                                        let mut response =
                                            crate::command::vector_search::execute_text_search_local(
                                                &s.text_store,
                                                &index_name,
                                                field_idx,
                                                &query_terms,
                                                top_k,
                                                offset,
                                                count,
                                            );
                                        // Step 9+10b: optional post-processing with db access.
                                        if needs_db {
                                            let db = &s.databases[0];
                                            let term_strings: Vec<String> =
                                                query_terms.iter().map(|qt| qt.text.clone()).collect();
                                            crate::command::vector_search::apply_post_processing(
                                                &mut response,
                                                &term_strings,
                                                text_index,
                                                db,
                                                highlight_opts.as_ref(),
                                                summarize_opts.as_ref(),
                                            );
                                        }
                                        Ok(response)
                                    })
                                } else {
                                    // Step 2: acquire ts guard (single-shard monoio).
                                    let ts_guard =
                                        ctx.shard_databases.text_store(ctx.shard_id);
                                    // Step 3: resolve index.
                                    let text_index = match ts_guard.get_index(&index_name) {
                                        Some(idx) => idx,
                                        None => {
                                            return {
                                                responses.push(Frame::Error(Bytes::from_static(
                                                    b"ERR no such index",
                                                )));
                                                true
                                            };
                                        }
                                    };
                                    // Step 4: ensure at least one TEXT field.
                                    if text_index.text_fields.is_empty() {
                                        responses.push(Frame::Error(Bytes::from_static(
                                            b"ERR index has no TEXT fields",
                                        )));
                                        return true;
                                    }
                                    // Step 5: parse the query via the index's first analyzer.
                                    let analyzer = match text_index.field_analyzers.first() {
                                        Some(a) => a,
                                        None => {
                                            responses.push(Frame::Error(Bytes::from_static(
                                                b"ERR index has no TEXT fields",
                                            )));
                                            return true;
                                        }
                                    };
                                    let clause = match crate::command::vector_search::parse_text_query(
                                        query_bytes.as_ref(),
                                        analyzer,
                                    ) {
                                        Ok(c) => c,
                                        Err(msg) => {
                                            responses.push(Frame::Error(
                                                Bytes::copy_from_slice(msg.as_bytes()),
                                            ));
                                            return true;
                                        }
                                    };
                                    // Step 5b: resolve field_idx.
                                    let field_idx = match &clause.field_name {
                                        None => None,
                                        Some(field_name) => {
                                            match text_index.text_fields.iter().position(|f| {
                                                f.field_name
                                                    .as_ref()
                                                    .eq_ignore_ascii_case(field_name.as_ref())
                                            }) {
                                                Some(idx) => Some(idx),
                                                None => {
                                                    let bad_name = field_name.clone();
                                                    responses.push(Frame::Error(Bytes::from(
                                                        format!(
                                                            "ERR unknown field '{}'",
                                                            String::from_utf8_lossy(&bad_name)
                                                        ),
                                                    )));
                                                    return true;
                                                }
                                            }
                                        }
                                    };
                                    let query_terms = clause.terms;
                                    // Step 9: acquire DB read guard iff post-processing needed.
                                    let db_guard_opt = if needs_db {
                                        Some(ctx.shard_databases.read_db(ctx.shard_id, 0))
                                    } else {
                                        None
                                    };
                                    // Step 10: execute + optional post-processing.
                                    let mut response =
                                        crate::command::vector_search::execute_text_search_local(
                                            &ts_guard,
                                            &index_name,
                                            field_idx,
                                            &query_terms,
                                            top_k,
                                            offset,
                                            count,
                                        );
                                    if let Some(ref db_guard) = db_guard_opt {
                                        let term_strings: Vec<String> =
                                            query_terms.iter().map(|qt| qt.text.clone()).collect();
                                        crate::command::vector_search::apply_post_processing(
                                            &mut response,
                                            &term_strings,
                                            text_index,
                                            db_guard,
                                            highlight_opts.as_ref(),
                                            summarize_opts.as_ref(),
                                        );
                                    }
                                    drop(db_guard_opt);
                                    drop(ts_guard);
                                    Ok(response)
                                };
                            let mut response = match text_search_result {
                                Ok(r) => r,
                                Err(e) => {
                                    responses.push(e);
                                    return true;
                                }
                            };
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
        // Phase 2a: Option A — gate on is_initialized(). New path uses ShardSlice
        // directly (no RwLock); old path uses ctx.shard_databases guards. Dead code
        // until Phase 4 wires init_shard() at shard startup.
        //
        // FT.SEARCH AS_OF needs shard_databases for vector_store LSN resolution;
        // in the new path we resolve it inside the closure via s.vector_store.
        let response = if crate::shard::slice::is_initialized() {
            // Phase 2a: AS_OF temporal resolution still uses ctx.shard_databases
            // (temporal_registry is migrated in Phase 2b, not here). This is
            // intentional — both paths share the same AS_OF resolution logic.
            let as_of_lsn_result = if cmd.eq_ignore_ascii_case(b"FT.SEARCH") {
                resolve_ft_search_as_of_lsn(
                    cmd_args,
                    Some(&ctx.shard_databases),
                    ctx.shard_id,
                    conn.active_cross_txn.as_ref(),
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
            crate::shard::slice::with_shard(|s| {
                if cmd.eq_ignore_ascii_case(b"FT.CREATE") {
                    crate::command::vector_search::ft_create(
                        &mut s.vector_store,
                        &mut s.text_store,
                        cmd_args,
                    )
                } else if cmd.eq_ignore_ascii_case(b"FT.SEARCH") {
                    let has_session = cmd_args.iter().any(|a| {
                        if let Frame::BulkString(b) = a {
                            b.eq_ignore_ascii_case(b"SESSION")
                        } else {
                            false
                        }
                    });
                    if has_session {
                        crate::command::vector_search::ft_search(
                            &mut s.vector_store,
                            cmd_args,
                            Some(&mut s.databases[0]),
                            Some(&s.text_store),
                            as_of_lsn,
                        )
                    } else {
                        crate::command::vector_search::ft_search(
                            &mut s.vector_store,
                            cmd_args,
                            None,
                            Some(&s.text_store),
                            as_of_lsn,
                        )
                    }
                } else if cmd.eq_ignore_ascii_case(b"FT.DROPINDEX") {
                    crate::command::vector_search::ft_dropindex(
                        &mut s.vector_store,
                        &mut s.text_store,
                        Some(&mut s.databases[0]),
                        cmd_args,
                    )
                } else if cmd.eq_ignore_ascii_case(b"FT.INFO") {
                    crate::command::vector_search::ft_info(
                        &s.vector_store,
                        &s.text_store,
                        cmd_args,
                    )
                } else if cmd.eq_ignore_ascii_case(b"FT._LIST") {
                    crate::command::vector_search::ft_list(&s.vector_store)
                } else if cmd.eq_ignore_ascii_case(b"FT.COMPACT") {
                    crate::command::vector_search::ft_compact(
                        &mut s.vector_store,
                        &mut s.text_store,
                        cmd_args,
                    )
                } else if cmd.eq_ignore_ascii_case(b"FT.CACHESEARCH") {
                    crate::command::vector_search::cache_search::ft_cachesearch(
                        &mut s.vector_store,
                        cmd_args,
                    )
                } else if cmd.eq_ignore_ascii_case(b"FT.CONFIG") {
                    crate::command::vector_search::ft_config(
                        &mut s.vector_store,
                        &mut s.text_store,
                        cmd_args,
                    )
                } else if cmd.eq_ignore_ascii_case(b"FT.RECOMMEND") {
                    crate::command::vector_search::recommend::ft_recommend(
                        &mut s.vector_store,
                        cmd_args,
                        Some(&mut s.databases[0]),
                    )
                } else if cmd.eq_ignore_ascii_case(b"FT.NAVIGATE") {
                    #[cfg(feature = "graph")]
                    {
                        crate::command::vector_search::navigate::ft_navigate(
                            &mut s.vector_store,
                            Some(&s.graph_store),
                            cmd_args,
                            None,
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
                        Frame::Error(Bytes::from_static(
                            b"ERR FT.EXPAND requires graph feature",
                        ))
                    }
                } else {
                    Frame::Error(Bytes::from_static(b"ERR unknown FT.* command"))
                }
            })
        } else {
            let shard_databases_ref = &ctx.shard_databases;
            let mut vs = shard_databases_ref.vector_store(ctx.shard_id);
            let mut ts = shard_databases_ref.text_store(ctx.shard_id);
            if cmd.eq_ignore_ascii_case(b"FT.CREATE") {
                crate::command::vector_search::ft_create(&mut vs, &mut ts, cmd_args)
            } else if cmd.eq_ignore_ascii_case(b"FT.SEARCH") {
                // Resolve AS_OF temporal clause + TXN snapshot precedence (TEMP-04, ACID-09).
                let as_of_lsn = match resolve_ft_search_as_of_lsn(
                    cmd_args,
                    Some(shard_databases_ref),
                    ctx.shard_id,
                    conn.active_cross_txn.as_ref(),
                ) {
                    Ok(lsn) => lsn,
                    Err(err_frame) => {
                        responses.push(err_frame);
                        return true;
                    }
                };
                // Detect SESSION keyword to provide database access for sorted set tracking
                let has_session = cmd_args.iter().any(|a| {
                    if let Frame::BulkString(b) = a {
                        b.eq_ignore_ascii_case(b"SESSION")
                    } else {
                        false
                    }
                });
                if has_session {
                    let mut db_guard = shard_databases_ref.write_db(ctx.shard_id, 0);
                    crate::command::vector_search::ft_search(
                        &mut vs,
                        cmd_args,
                        Some(&mut *db_guard),
                        Some(&*ts),
                        as_of_lsn,
                    )
                } else {
                    crate::command::vector_search::ft_search(
                        &mut vs,
                        cmd_args,
                        None,
                        Some(&*ts),
                        as_of_lsn,
                    )
                }
            } else if cmd.eq_ignore_ascii_case(b"FT.DROPINDEX") {
                let mut db_guard = shard_databases_ref.write_db(ctx.shard_id, 0);
                crate::command::vector_search::ft_dropindex(
                    &mut vs,
                    &mut ts,
                    Some(&mut *db_guard),
                    cmd_args,
                )
            } else if cmd.eq_ignore_ascii_case(b"FT.INFO") {
                crate::command::vector_search::ft_info(&vs, &ts, cmd_args)
            } else if cmd.eq_ignore_ascii_case(b"FT._LIST") {
                crate::command::vector_search::ft_list(&vs)
            } else if cmd.eq_ignore_ascii_case(b"FT.COMPACT") {
                crate::command::vector_search::ft_compact(&mut vs, &mut ts, cmd_args)
            } else if cmd.eq_ignore_ascii_case(b"FT.CACHESEARCH") {
                crate::command::vector_search::cache_search::ft_cachesearch(&mut vs, cmd_args)
            } else if cmd.eq_ignore_ascii_case(b"FT.CONFIG") {
                crate::command::vector_search::ft_config(&mut vs, &mut ts, cmd_args)
            } else if cmd.eq_ignore_ascii_case(b"FT.RECOMMEND") {
                let mut db_guard = shard_databases_ref.write_db(ctx.shard_id, 0);
                crate::command::vector_search::recommend::ft_recommend(
                    &mut vs,
                    cmd_args,
                    Some(&mut *db_guard),
                )
            } else if cmd.eq_ignore_ascii_case(b"FT.NAVIGATE") {
                #[cfg(feature = "graph")]
                {
                    let graph_guard = shard_databases_ref.graph_store_read(ctx.shard_id);
                    crate::command::vector_search::navigate::ft_navigate(
                        &mut vs,
                        Some(&graph_guard),
                        cmd_args,
                        None,
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
                    let graph_guard = shard_databases_ref.graph_store_read(ctx.shard_id);
                    crate::command::vector_search::ft_expand(&graph_guard, cmd_args)
                }
                #[cfg(not(feature = "graph"))]
                {
                    Frame::Error(Bytes::from_static(b"ERR FT.EXPAND requires graph feature"))
                }
            } else {
                Frame::Error(Bytes::from_static(b"ERR unknown FT.* command"))
            }
        };
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
