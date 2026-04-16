//! FT.SEARCH top-level dispatch and routing.
//!
//! Split from the monolithic `ft_search.rs` in Phase 152 (Plan 02.5). This file
//! is the FT.SEARCH entry point: it parses the user's args, selects the hybrid /
//! sparse-only / dense-only execution path, applies session + range filtering,
//! and delegates Frame construction to `response.rs` and local execution to
//! `execute.rs`. Parser helpers live in `parse.rs`.
//!
//! Also hosts `ft_search_with_graph` (graph feature), which wraps `ft_search`
//! with EXPAND GRAPH post-processing.
//!
//! No behavior change relative to the original `ft_search.rs` — pure relocation.

use bytes::Bytes;
use smallvec::SmallVec;

use crate::command::vector_search::session;
use crate::protocol::Frame;
use crate::vector::store::VectorStore;
use crate::vector::types::SearchResult;

use super::execute::{
    SearchRawResult, apply_range_filter, parse_sparse_query_blob, search_local_filtered,
    search_local_raw,
};
use super::parse::{
    extract_param_blob, parse_filter_clause, parse_inline_filter, parse_knn_query,
    parse_limit_clause, parse_range_clause, parse_session_clause, parse_sparse_clause,
};
use super::response::{build_hybrid_response, build_search_response};

/// FT.SEARCH idx "*=>[KNN 10 @vec $query]" PARAMS 2 query <blob>
///
/// Parses KNN query syntax, decodes the vector blob, runs local search.
/// For cross-shard, the coordinator calls this on each shard and merges.
///
/// Returns: Array [num_results, doc_id, [field_values], ...]
///
/// `db` is an optional mutable Database reference for SESSION clause support.
/// When `None`, SESSION clause is silently ignored (backward compatible).
pub fn ft_search(
    store: &mut VectorStore,
    args: &[Frame],
    mut db: Option<&mut crate::storage::db::Database>,
) -> Frame {
    // args[0] = index_name, args[1] = query_string, args[2..] = PARAMS ...
    if args.len() < 2 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'FT.SEARCH' command",
        ));
    }

    let index_name = match crate::command::vector_search::extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid index name")),
    };

    let query_str = match crate::command::vector_search::extract_bulk(&args[1]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid query")),
    };

    // Parse KNN from query string (optional — may be absent for sparse-only search)
    let knn_parsed = parse_knn_query(&query_str);

    // Parse optional SPARSE clause: SPARSE @field_name $param_name
    let sparse_clause = parse_sparse_clause(args);

    // Must have at least one retriever
    if knn_parsed.is_none() && sparse_clause.is_none() {
        return Frame::Error(Bytes::from_static(b"ERR invalid KNN query syntax"));
    }

    // Parse optional FILTER clause: explicit FILTER keyword OR inline prefix in query string
    let filter_expr = parse_filter_clause(args).or_else(|| parse_inline_filter(&query_str));

    // Parse optional LIMIT offset count
    let (limit_offset, limit_count) = parse_limit_clause(args);

    // Parse optional SESSION clause
    let session_key = parse_session_clause(args);

    // Parse optional RANGE threshold for distance filtering (AGNT-05)
    let range_threshold = parse_range_clause(args);

    // Look up metric for range filtering (cheap immutable borrow before search paths)
    let range_metric = if range_threshold.is_some() {
        store
            .get_index(index_name.as_ref())
            .map(|idx| idx.meta.default_field().metric)
    } else {
        None
    };

    // Determine k from KNN or default to 10 for sparse-only
    let (k, field_name, dense_param) = match &knn_parsed {
        Some((k, fname, pname)) => (*k, fname.clone(), Some(pname.clone())),
        None => (10, None, None),
    };

    // Extract dense query blob if KNN is present
    let dense_blob = if let Some(ref pname) = dense_param {
        match extract_param_blob(args, pname) {
            Some(blob) => Some(blob),
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR query vector parameter not found in PARAMS",
                ));
            }
        }
    } else {
        None
    };

    // Extract sparse query pairs if SPARSE clause is present
    let sparse_query = if let Some((ref sparse_field, ref sparse_param)) = sparse_clause {
        match extract_param_blob(args, sparse_param) {
            Some(blob) => {
                let pairs = parse_sparse_query_blob(&blob);
                if pairs.is_empty() {
                    return Frame::Error(Bytes::from_static(
                        b"ERR invalid sparse query blob (must be pairs of u32+f32 LE)",
                    ));
                }
                Some((sparse_field.clone(), pairs))
            }
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR sparse query parameter not found in PARAMS",
                ));
            }
        }
    } else {
        None
    };

    // --- Hybrid path: both dense + sparse ---
    if let (Some(blob), Some((sparse_field, sparse_pairs))) =
        (dense_blob.as_ref(), sparse_query.as_ref())
    {
        // Dense search
        let dense_raw = search_local_raw(
            store,
            &index_name,
            blob,
            k,
            filter_expr.as_ref(),
            field_name.as_ref(),
        );
        let (dense_results, key_hash_to_key) = match dense_raw {
            SearchRawResult::Ok {
                results,
                key_hash_to_key,
            } => (results, key_hash_to_key),
            SearchRawResult::Error(frame) => return frame,
        };

        // Sparse search
        let idx = match store.get_index_mut(index_name.as_ref()) {
            Some(i) => i,
            None => return Frame::Error(Bytes::from_static(b"Unknown Index name")),
        };
        let sparse_results = match idx.sparse_stores.get(sparse_field.as_ref()) {
            Some(ss) => ss.search(sparse_pairs, k),
            None => Vec::new(),
        };

        // RRF fusion
        let (mut fused, dense_count, sparse_count) =
            crate::vector::fusion::rrf_fuse(&dense_results, &sparse_results, k);

        // Apply RANGE threshold filter to fused results (AGNT-05)
        if let (Some(threshold), Some(metric)) = (range_threshold, range_metric) {
            let mut sv: SmallVec<[SearchResult; 32]> = fused.drain(..).collect();
            apply_range_filter(&mut sv, threshold, metric);
            fused = sv.into_vec();
        }

        // Session filtering for hybrid path (SESS-01, SESS-02)
        if let (Some(sess_key), Some(db)) = (session_key.as_ref(), db.as_mut()) {
            let session_members_snapshot: std::collections::HashMap<Bytes, f64> =
                match db.get_sorted_set(sess_key) {
                    Ok(Some((members, _tree))) => members.clone(),
                    Ok(None) => std::collections::HashMap::new(),
                    Err(_) => std::collections::HashMap::new(),
                };
            let sv: SmallVec<[SearchResult; 32]> = fused.drain(..).collect();
            let filtered =
                session::filter_session_results(&sv, &session_members_snapshot, &key_hash_to_key);
            fused = filtered.into_vec();

            crate::vector::metrics::increment_search();
            let response = build_hybrid_response(
                &fused,
                &key_hash_to_key,
                dense_count,
                sparse_count,
                limit_offset,
                limit_count,
            );

            // Record new results in the session sorted set
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0);
            let result_sv: SmallVec<[SearchResult; 32]> = fused.into_iter().collect();
            session::record_session_results(&result_sv, db, sess_key, &key_hash_to_key, timestamp);

            return response;
        }

        crate::vector::metrics::increment_search();
        return build_hybrid_response(
            &fused,
            &key_hash_to_key,
            dense_count,
            sparse_count,
            limit_offset,
            limit_count,
        );
    }

    // --- Sparse-only path ---
    if let Some((sparse_field, sparse_pairs)) = sparse_query {
        let idx = match store.get_index_mut(index_name.as_ref()) {
            Some(i) => i,
            None => return Frame::Error(Bytes::from_static(b"Unknown Index name")),
        };
        let sparse_results = match idx.sparse_stores.get(sparse_field.as_ref()) {
            Some(ss) => ss.search(&sparse_pairs, k),
            None => Vec::new(),
        };
        let key_hash_to_key = idx.key_hash_to_key.clone();
        let sparse_count = sparse_results.len();

        // Convert to SmallVec for build_hybrid_response
        let mut fused: Vec<SearchResult> = sparse_results;

        // Apply RANGE threshold filter (AGNT-05)
        if let (Some(threshold), Some(metric)) = (range_threshold, range_metric) {
            let mut sv: SmallVec<[SearchResult; 32]> = fused.drain(..).collect();
            apply_range_filter(&mut sv, threshold, metric);
            fused = sv.into_vec();
        }

        // Session filtering for sparse-only path (SESS-01, SESS-02)
        if let (Some(sess_key), Some(db)) = (session_key.as_ref(), db.as_mut()) {
            let session_members_snapshot: std::collections::HashMap<Bytes, f64> =
                match db.get_sorted_set(sess_key) {
                    Ok(Some((members, _tree))) => members.clone(),
                    Ok(None) => std::collections::HashMap::new(),
                    Err(_) => std::collections::HashMap::new(),
                };
            let sv: SmallVec<[SearchResult; 32]> = fused.drain(..).collect();
            let filtered =
                session::filter_session_results(&sv, &session_members_snapshot, &key_hash_to_key);
            fused = filtered.into_vec();

            crate::vector::metrics::increment_search();
            let response = build_hybrid_response(
                &fused,
                &key_hash_to_key,
                0,
                sparse_count,
                limit_offset,
                limit_count,
            );

            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0);
            let result_sv: SmallVec<[SearchResult; 32]> = fused.into_iter().collect();
            session::record_session_results(&result_sv, db, sess_key, &key_hash_to_key, timestamp);

            return response;
        }

        crate::vector::metrics::increment_search();
        return build_hybrid_response(
            &fused,
            &key_hash_to_key,
            0,
            sparse_count,
            limit_offset,
            limit_count,
        );
    }

    // --- Dense-only path (original behavior, backward compat) ---
    // Hybrid and sparse-only paths returned above; dense_blob is guaranteed Some here.
    #[allow(clippy::unwrap_used)] // guarded: hybrid and sparse-only paths returned above
    let blob = dense_blob.unwrap();

    // If SESSION clause present and db available, use session-aware path
    if let (Some(ref sess_key), Some(db)) = (session_key, db) {
        let result = search_local_raw(
            store,
            &index_name,
            &blob,
            k,
            filter_expr.as_ref(),
            field_name.as_ref(),
        );
        crate::vector::metrics::increment_search();
        return match result {
            SearchRawResult::Error(frame) => frame,
            SearchRawResult::Ok {
                mut results,
                key_hash_to_key,
            } => {
                // Read session sorted set for filtering
                let session_members_snapshot: std::collections::HashMap<Bytes, f64> =
                    match db.get_sorted_set(sess_key) {
                        Ok(Some((members, _tree))) => members.clone(),
                        Ok(None) => std::collections::HashMap::new(),
                        Err(_) => std::collections::HashMap::new(),
                    };

                // Filter out previously returned results
                results = session::filter_session_results(
                    &results,
                    &session_members_snapshot,
                    &key_hash_to_key,
                );

                // Apply RANGE threshold filter (AGNT-05)
                if let (Some(threshold), Some(metric)) = (range_threshold, range_metric) {
                    apply_range_filter(&mut results, threshold, metric);
                }

                // Build response from filtered results
                let response =
                    build_search_response(&results, &key_hash_to_key, limit_offset, limit_count);

                // Record new results in the session sorted set
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs_f64())
                    .unwrap_or(0.0);
                session::record_session_results(
                    &results,
                    db,
                    sess_key,
                    &key_hash_to_key,
                    timestamp,
                );

                response
            }
        };
    }

    // Standard dense-only path (no session)
    // When RANGE is set, use raw search + range filter + response build.
    // Otherwise, use the fused search_local_filtered for backward compat.
    if let (Some(threshold), Some(metric)) = (range_threshold, range_metric) {
        let raw = search_local_raw(
            store,
            &index_name,
            &blob,
            k,
            filter_expr.as_ref(),
            field_name.as_ref(),
        );
        crate::vector::metrics::increment_search();
        match raw {
            SearchRawResult::Error(frame) => frame,
            SearchRawResult::Ok {
                mut results,
                key_hash_to_key,
            } => {
                apply_range_filter(&mut results, threshold, metric);
                build_search_response(&results, &key_hash_to_key, limit_offset, limit_count)
            }
        }
    } else {
        let result = search_local_filtered(
            store,
            &index_name,
            &blob,
            k,
            filter_expr.as_ref(),
            limit_offset,
            limit_count,
            field_name.as_ref(),
        );
        crate::vector::metrics::increment_search();
        result
    }
}

/// FT.SEARCH with optional EXPAND GRAPH support.
///
/// Takes both VectorStore and an optional GraphStore reference. If the query
/// contains `EXPAND GRAPH depth:N` (or `EXPAND GRAPH N`), runs the normal
/// KNN search first, then expands results through graph topology, merging
/// both sets into a combined response with `__vec_score` and `__graph_hops`.
///
/// If no EXPAND clause is present or no graph_store is provided, delegates
/// to the standard `ft_search` (backward compatible).
#[cfg(feature = "graph")]
pub fn ft_search_with_graph(
    store: &mut VectorStore,
    graph_store: Option<&crate::graph::store::GraphStore>,
    args: &[Frame],
    db: Option<&mut crate::storage::db::Database>,
) -> Frame {
    let expand_depth = super::parse::parse_expand_clause(args);

    // No expansion requested or no graph store — fall back to standard search.
    if expand_depth.is_none() || graph_store.is_none() {
        return ft_search(store, args, db);
    }

    let depth = expand_depth.unwrap_or(1);
    #[allow(clippy::unwrap_used)] // guarded: is_none() check returns at line above
    let gs = graph_store.unwrap();

    // Run the standard KNN search first (session filtering happens inside ft_search).
    let knn_result = ft_search(store, args, db);

    // Extract (key, score) pairs from the KNN response for seeding graph expansion.
    let seed_keys = super::response::extract_seeds_from_response(&knn_result);
    if seed_keys.is_empty() {
        return knn_result; // no KNN hits to expand
    }

    // Find the first graph that contains any of the seed keys.
    let graph_names = gs.list_graphs();
    let mut target_graph: Option<&crate::graph::store::NamedGraph> = None;
    'outer: for gname in &graph_names {
        if let Some(g) = gs.get_graph(gname) {
            for (key, _) in &seed_keys {
                if g.lookup_node_by_key(key).is_some() {
                    target_graph = Some(g);
                    break 'outer;
                }
            }
        }
    }

    let Some(graph) = target_graph else {
        return knn_result; // no graph has these keys
    };

    // Expand via BFS through graph topology.
    let expanded = crate::command::vector_search::graph_expand::expand_results_via_graph(
        graph, &seed_keys, depth,
    );

    // Build combined response: original KNN results + expanded graph results.
    super::response::build_combined_response(&knn_result, &expanded)
}
