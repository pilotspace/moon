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
            // -- FT.AGGREGATE: two-phase scatter-gather per Plan 03 (D-05/D-07) --
            // scatter_text_aggregate acquires its own guards internally
            // inside the single-shard block, so we never hold a MutexGuard
            // across the .await below.
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

            // moon#695: a bare `*` on a VECTOR-only index. `is_text_query("*")`
            // is true, so without this the query reaches the text engine — which
            // has no inverted index for such a schema and answers
            // `ERR no such index` for an index FT._LIST lists. Decided ahead of
            // every other FT.SEARCH branch, and a no-op for every other query.
            if is_text {
                if let Some(mut response) = crate::shard::coordinator::ft_match_all_if_vector_only(
                    cmd_args,
                    ctx.shard_id,
                    ctx.num_shards,
                    &ctx.dispatch_tx,
                    &ctx.spsc_notifiers,
                    conn.selected_db as u8,
                )
                .await
                {
                    if let Some(ws_id) = conn.workspace_id.as_ref() {
                        strip_workspace_prefix_from_response(ws_id, cmd, &mut response);
                    }
                    responses.push(response);
                    return true;
                }
            }

            // -- HYBRID multi-shard path (Phase 152 Plan 05, D-13) --
            // If the args contain a HYBRID clause, route through
            // scatter_hybrid_search (three-phase DFS -> fan-out -> RRF
            // merge). Single-shard is handled inside the scatter entry
            // point itself (fast path to execute_hybrid_search_local).
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
                        // Phase 171 HYB-02 / SCAT-02: resolve AS_OF /
                        // TXN LSN ONCE on the coordinator and forward to
                        // every responder via the scatter helper.
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
                    Ok(None) => { /* fall through to non-HYBRID paths */ }
                    Err(err_frame) => {
                        responses.push(err_frame);
                        return true;
                    }
                }
            }

            // moon#728: NOT routed when there is no text engine. Without
            // `text-index` every shard's text store is permanently empty, so the
            // DFS scatter runs, finds nothing anywhere, and `merge_text_results`
            // folds the replies into a SUCCESSFUL `[0]` -- indistinguishable from
            // an index that exists and holds nothing, and different from what the
            // same build answers at `--shards 1`. Falling through instead lands
            // in the KNN parser, which refuses a text-shaped query by name
            // (`ft_search/dispatch.rs`), so BOTH shard counts give one answer.
            // The moon#695 vector-only match-all above is deliberately ahead of
            // this and stays ungated: a vector-only schema needs no text engine.
            #[cfg(feature = "text-index")]
            if is_text {
                // -- Text FT.SEARCH: two-phase DFS scatter-gather --
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
                #[allow(clippy::unwrap_used)] // query_bytes is Some when is_text is true
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

                let mut response = crate::shard::coordinator::scatter_text_search(
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
                if let Some(ws_id) = conn.workspace_id.as_ref() {
                    strip_workspace_prefix_from_response(ws_id, cmd, &mut response);
                }
                responses.push(response);
                return true;
            }

            // -- Vector FT.SEARCH (KNN / SPARSE): existing path --
            // Phase 171 SCAT-01: resolve AS_OF / TXN snapshot LSN ONCE
            // on the coordinator and thread it through the scatter helper
            // so every responder honors the same temporal snapshot.
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
        // FT.INFO: stats are per-shard partitions — scatter to every shard and
        // sum the additive fields (XC-SHARD-1); broadcast_vector_command would
        // return only the local shard's counts (~1/N of the truth).
        if cmd.eq_ignore_ascii_case(b"FT.INFO") {
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

    // Single-shard: no SPSC channels available.
    // Dispatch directly to shard's VectorStore via shared access.
    //
    // -- 154-01 single-shard FT.AGGREGATE fast path --
    // scatter_text_aggregate internally fast-paths num_shards
    // == 1 to execute_local_full.
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
    // moon#695: `*` on a VECTOR-only index, single-shard. Deliberately NOT
    // gated on `text-index`: a vector-only schema builds no TextIndex at all,
    // so this has to work in a build with no text engine compiled in. The
    // predicate itself rejects HYBRID / SPARSE / KNN, so this only ever
    // claims a bare match-all the text engine cannot answer.
    if cmd.eq_ignore_ascii_case(b"FT.SEARCH") {
        if let Some(mut response) = crate::shard::coordinator::ft_match_all_if_vector_only(
            cmd_args,
            ctx.shard_id,
            ctx.num_shards,
            &ctx.dispatch_tx,
            &ctx.spsc_notifiers,
            conn.selected_db as u8,
        )
        .await
        {
            if let Some(ws_id) = conn.workspace_id.as_ref() {
                strip_workspace_prefix_from_response(ws_id, cmd, &mut response);
            }
            responses.push(response);
            return true;
        }
    }
    //
    // -- 151-03 single-shard text FT.SEARCH fast path --
    // Parity with handler_monoio.rs. Bare text queries bypass
    // ft_search() (which only parses KNN/SPARSE/HYBRID) and route directly
    // to run_text_query (see below) -- NOT execute_text_search_local, which
    // is dead code outside its own test module (adversarial-review round-2
    // hygiene fix; the real multi-shard path is scatter_text_search ->
    // run_text_query_on_index).
    #[cfg(feature = "text-index")]
    if cmd.eq_ignore_ascii_case(b"FT.SEARCH") {
        if let Some(Frame::BulkString(query_bytes)) = cmd_args.get(1) {
            match crate::command::vector_search::parse_hybrid_modifier(cmd_args) {
                Ok(Some(_)) => {
                    // HYBRID present -- existing ft_search() below handles it.
                }
                Err(frame_err) => {
                    responses.push(frame_err);
                    return true;
                }
                Ok(None) => {
                    if crate::command::vector_search::is_text_query(query_bytes.as_ref())
                        && !crate::command::vector_search::has_sparse_clause(cmd_args)
                    {
                        // Step 1: index_name.
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
                        let response = crate::shard::slice::with_shard(|s| {
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
                                            let terms = crate::text::query::collect_highlight_terms(
                                                &node, text_index,
                                            );
                                            // L4: exclusive, matching the
                                            // owner-thread exclusivity the bare
                                            // index had. S3 downgrades the read
                                            // sites behind its own benchmark
                                            // gate, not here. Twin of
                                            // `handler_monoio/ft.rs`.
                                            let db_guard = s.databases.write(0);
                                            crate::command::vector_search::apply_post_processing(
                                                &mut r,
                                                &terms,
                                                text_index,
                                                &db_guard,
                                                highlight_opts.as_ref(),
                                                summarize_opts.as_ref(),
                                            );
                                        }
                                    }
                                }
                                r
                            }
                            #[cfg(not(feature = "text-index"))]
                            Frame::Error(Bytes::from_static(b"ERR text-index feature not enabled"))
                        });
                        let mut response = response;
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
    // All companion stores (vector_store, text_store, databases[0], graph_store)
    // accessed from ONE with_shard closure to avoid re-entrant RefCell borrows.
    //
    // Resolve AS_OF outside the closure (acquires shard_databases for the
    // resolver helper, which itself reads vector_store; doing this inside
    // would re-enter with_shard).
    let as_of_lsn_opt: Option<Result<u64, Frame>> = if cmd.eq_ignore_ascii_case(b"FT.SEARCH") {
        Some(resolve_ft_search_as_of_lsn(
            cmd_args,
            Some(&ctx.shard_databases),
            conn.active_cross_txn.as_deref(),
        ))
    } else {
        None
    };

    // FT.SEARCH cooperative-yield seam (ft-search-off-eventloop §3): capture an
    // OWNED snapshot inside the shard slice, release the borrow, then `.await` the
    // chunked yielding search so a heavy KNN scan interleaves with the 1ms tick and
    // co-located commands instead of monopolizing the event loop. Non-yieldable
    // shapes (HYBRID/SPARSE/SESSION/RANGE/non-default-field/unknown-index/parse-error)
    // come back as `FtSearchPlan::Sync` from the proven synchronous path —
    // byte-identical to the legacy behavior. (Multi-shard scatter + text fast paths
    // already returned above; only the single-shard local vector path reaches here.)
    if cmd.eq_ignore_ascii_case(b"FT.SEARCH") {
        // as_of_lsn_opt is Some when cmd == FT.SEARCH (resolved just above).
        let as_of_lsn = match as_of_lsn_opt {
            Some(Ok(lsn)) => lsn,
            Some(Err(err_frame)) => {
                responses.push(err_frame);
                return true;
            }
            None => 0,
        };
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
                // L4: the destructure existed only because `dbs.get_mut(i)`
                // borrowed the whole slice off `s`. The guard borrows from the
                // `Arc`, not from `s`, so the disjoint-field dance is no longer
                // needed. Guard scoped to the capture and released with the
                // `with_shard` closure, well before the `.await` on the
                // returned (owned, 'static) snapshot below.
                let mut db_guard = s.databases.try_write(db_index as usize);
                crate::command::vector_search::ft_search_capture(
                    &mut s.vector_store,
                    cmd_args,
                    db_guard.as_deref_mut(),
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
                // capture (parks this task, not the shard thread) so the scan
                // below sees full recall while siblings keep running.
                snapshot.await_pending_reloads().await;
                let results = crate::vector::segment::holder::SegmentHolder::search_mvcc_yielding(
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

    // Unconditional slice path: ShardSlice is always initialized.
    // All companion stores (vector_store, text_store, databases[0], graph_store)
    // accessed from ONE with_shard closure to avoid re-entrant RefCell borrows.
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
            // L4: per-branch guard, so the sibling FT.* arms that never touched
            // a database still do not take one. Destructure dropped — see the
            // FT.SEARCH capture above.
            let mut db_guard = s.databases.try_write(db_index as usize);
            crate::command::vector_search::ft_dropindex(
                &mut s.vector_store,
                &mut s.text_store,
                db_guard.as_deref_mut(),
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
            // L4: per-branch guard, as for FT.DROPINDEX above.
            let mut db_guard = s.databases.try_write(db_index as usize);
            crate::command::vector_search::recommend::ft_recommend(
                &mut s.vector_store,
                cmd_args,
                db_guard.as_deref_mut(),
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
        } else {
            Frame::Error(Bytes::from_static(b"ERR unknown FT.* command"))
        }
    });

    let mut response = response;
    if let Some(ws_id) = conn.workspace_id.as_ref() {
        strip_workspace_prefix_from_response(ws_id, cmd, &mut response);
    }
    responses.push(response);
    true
}
