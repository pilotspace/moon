//! FT.* vector search command handlers.
//!
//! These commands operate on VectorStore, not Database, so they are NOT
//! dispatched through the standard command::dispatch() function.
//! Instead, the shard event loop intercepts FT.* commands and calls
//! these handlers directly with the per-shard VectorStore.

pub mod cache_search;
pub mod ft_admin;
pub mod ft_config;
pub mod ft_create;
pub mod ft_info;
pub mod ft_search;
pub mod ft_text_search;
#[cfg(feature = "graph")]
pub mod graph_expand;
pub mod helpers;
#[cfg(feature = "graph")]
pub mod navigate;
pub mod recommend;
pub mod session;

// Re-export public APIs so callers can use `vector_search::ft_create(...)` etc.
pub use ft_admin::{ft_compact, ft_dropindex, ft_list};
pub use ft_config::ft_config;
pub use ft_create::ft_create;
pub use ft_info::ft_info;
pub use ft_search::{
    ft_search, merge_search_results, parse_ft_search_args, parse_session_clause, search_local,
    search_local_filtered,
};
#[cfg(feature = "graph")]
pub use ft_search::ft_search_with_graph;
pub use ft_text_search::{
    apply_post_processing, execute_text_search_local, execute_text_search_with_global_idf,
    ft_text_search, highlight_field, is_text_query, merge_text_results, parse_highlight_clause,
    parse_summarize_clause, parse_text_query, summarize_field, HighlightOpts, SummarizeOpts,
};
pub use helpers::{metric_to_bytes, quantize_f32_to_sq, quantization_to_bytes};

// Re-export pub(crate) items for sibling submodules (cache_search, navigate, recommend, session).
#[cfg(feature = "graph")]
#[allow(unused_imports)] // Used by tests.rs, not by lib code directly
pub(crate) use ft_search::{
    build_combined_response, extract_seeds_from_response, parse_expand_clause,
};
#[allow(unused_imports)] // parse_range_clause, parse_sparse_clause used by tests.rs only
pub(crate) use ft_search::{
    build_search_response, extract_param_blob, parse_filter_clause, parse_knn_query,
    parse_limit_clause, parse_range_clause, parse_sparse_clause, parse_usize,
};

// Re-export types used by tests and sibling modules.
#[allow(unused_imports)]
pub(crate) use crate::vector::store::VectorStore;
#[allow(unused_imports)]
pub(crate) use crate::vector::types::DistanceMetric;

use bytes::Bytes;
#[cfg(feature = "graph")]
use smallvec::SmallVec;

use crate::protocol::Frame;
#[cfg(feature = "graph")]
use crate::protocol::FrameVec;

// -- FT.EXPAND (GraphRAG standalone expansion) --

/// FT.EXPAND idx key1 key2 ... DEPTH N [GRAPH graph_name]
///
/// Standalone graph expansion from explicit Redis keys. Returns graph neighbors
/// up to N hops with `__graph_hops` metadata per discovered node.
///
/// If `GRAPH graph_name` is specified, uses that named graph. Otherwise,
/// auto-detects by checking which graph has the first seed key registered.
///
/// Shard-local: only expands within the graph data on this shard (GRAF-05).
#[cfg(feature = "graph")]
pub fn ft_expand(graph_store: &crate::graph::store::GraphStore, args: &[Frame]) -> Frame {
    // args[0] = index name (reserved for consistency), then keys, then DEPTH N, optionally GRAPH name
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'FT.EXPAND' command",
        ));
    }

    // args[0] = index name (reserved, validate present)
    let _index_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid index name")),
    };

    // Scan for DEPTH keyword to find boundary between keys and options.
    let mut depth_pos = None;
    for i in 1..args.len() {
        if matches_keyword(&args[i], b"DEPTH") {
            depth_pos = Some(i);
            break;
        }
    }

    let depth_pos = match depth_pos {
        Some(p) => p,
        None => {
            return Frame::Error(Bytes::from_static(b"ERR syntax error: expected DEPTH N"));
        }
    };

    // Keys are args[1..depth_pos]
    if depth_pos <= 1 {
        return Frame::Error(Bytes::from_static(b"ERR no keys specified"));
    }

    let key_args = &args[1..depth_pos];

    // Parse DEPTH value
    if depth_pos + 1 >= args.len() {
        return Frame::Error(Bytes::from_static(
            b"ERR syntax error: DEPTH requires a value",
        ));
    }
    let depth = match parse_u32(&args[depth_pos + 1]) {
        Some(d) => d,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid DEPTH value")),
    };

    // Parse optional GRAPH graph_name after DEPTH N
    let mut graph_name: Option<Bytes> = None;
    let mut pos = depth_pos + 2;
    while pos < args.len() {
        if matches_keyword(&args[pos], b"GRAPH") {
            pos += 1;
            if pos >= args.len() {
                return Frame::Error(Bytes::from_static(
                    b"ERR syntax error: GRAPH requires a name",
                ));
            }
            graph_name = extract_bulk(&args[pos]);
            pos += 1;
        } else {
            pos += 1;
        }
    }

    // Build seed keys as (Bytes, f32) with vec_score = 0.0
    let seed_keys: SmallVec<[(Bytes, f32); 16]> = key_args
        .iter()
        .filter_map(|f| extract_bulk(f).map(|b| (b, 0.0f32)))
        .collect();

    if seed_keys.is_empty() {
        return Frame::Error(Bytes::from_static(b"ERR no keys specified"));
    }

    // Find the graph to use.
    let graph = if let Some(ref name) = graph_name {
        graph_store.get_graph(name)
    } else {
        // Auto-detect: find a graph that has the first seed key registered.
        let first_key = &seed_keys[0].0;
        let mut found = None;
        for gname in graph_store.list_graphs() {
            if let Some(g) = graph_store.get_graph(gname) {
                if g.lookup_node_by_key(first_key).is_some() {
                    found = Some(g);
                    break;
                }
            }
        }
        found
    };

    let graph = match graph {
        Some(g) => g,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR no graph contains the specified keys",
            ));
        }
    };

    // Perform graph expansion.
    let results = graph_expand::expand_results_via_graph(graph, &seed_keys, depth);

    // Format as RESP array: count, then for each result: key + field array.
    // Field array contains: __graph_hops, hops_value, __vec_score, "0"
    let total = results.len();
    let mut frames: Vec<Frame> = Vec::with_capacity(1 + total * 2);
    frames.push(Frame::Integer(total as i64));

    for r in &results {
        frames.push(Frame::BulkString(r.key.clone()));
        // Field array: [__graph_hops, N, __vec_score, "0"]
        let mut hop_buf = itoa::Buffer::new();
        let hop_str = hop_buf.format(r.graph_hops);
        let fields: FrameVec = vec![
            Frame::BulkString(Bytes::from_static(b"__graph_hops")),
            Frame::BulkString(Bytes::copy_from_slice(hop_str.as_bytes())),
            Frame::BulkString(Bytes::from_static(b"__vec_score")),
            Frame::BulkString(Bytes::from_static(b"0")),
        ]
        .into();
        frames.push(Frame::Array(fields));
    }

    Frame::Array(frames.into())
}

// -- Helpers (crate-visible) --

pub(crate) fn extract_bulk(frame: &Frame) -> Option<Bytes> {
    match frame {
        Frame::BulkString(b) => Some(b.clone()),
        _ => None,
    }
}

pub(crate) fn matches_keyword(frame: &Frame, keyword: &[u8]) -> bool {
    match frame {
        Frame::BulkString(b) => b.eq_ignore_ascii_case(keyword),
        _ => false,
    }
}

pub(crate) fn parse_u32(frame: &Frame) -> Option<u32> {
    match frame {
        Frame::BulkString(b) => std::str::from_utf8(b).ok()?.parse().ok(),
        Frame::Integer(n) => u32::try_from(*n).ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
