//! FT.NAVIGATE — multi-hop knowledge navigation.
//!
//! Composes three stages: KNN vector search, graph expansion via BFS,
//! and combined re-ranking with hop_depth metadata. This enables AI agents
//! to traverse knowledge graphs starting from vector similarity, discovering
//! related entities beyond direct vector matches.
//!
//! Syntax: FT.NAVIGATE idx "*=>[KNN k @field $vec_param]" HOPS N [HOP_PENALTY p] PARAMS 2 vec_param <blob>

use bytes::Bytes;
use std::collections::HashMap;
use std::fmt::Write;

use crate::protocol::Frame;
use crate::vector::store::VectorStore;

use super::graph_expand::{ExpandedResult, MAX_EXPAND_DEPTH, expand_results_via_graph};
use super::{extract_bulk, extract_seeds_from_response, matches_keyword, parse_u32};

/// Default penalty applied per graph hop to bias results toward vector-similar hits.
const DEFAULT_HOP_PENALTY: f32 = 0.1;

/// Maximum number of hops allowed (mirrors graph_expand::MAX_EXPAND_DEPTH).
const MAX_HOPS: u32 = MAX_EXPAND_DEPTH;

/// Hard cap multiplier: candidates = K * CAP_MULTIPLIER, then take top K.
const CAP_MULTIPLIER: usize = 3;

/// Execute FT.NAVIGATE: KNN search -> graph expand -> re-rank with hop_depth.
///
/// Returns an array response like FT.SEARCH but with `__hop_depth` and
/// `__vec_score` fields per result, enabling agents to reason about both
/// vector proximity and graph distance.
#[cfg(feature = "graph")]
pub fn ft_navigate(
    store: &mut VectorStore,
    graph_store: Option<&crate::graph::store::GraphStore>,
    args: &[Frame],
    db: Option<&mut crate::storage::db::Database>,
) -> Frame {
    // Require graph feature at runtime.
    let Some(gs) = graph_store else {
        return Frame::Error(Bytes::from_static(
            b"ERR FT.NAVIGATE requires graph feature",
        ));
    };

    // args: [index_name, query_string, ...options..., HOPS, N, ...]
    if args.len() < 4 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'FT.NAVIGATE' command",
        ));
    }

    // --- Parse HOPS N (required) ---
    let Some(hops) = parse_hops(args) else {
        return Frame::Error(Bytes::from_static(
            b"ERR HOPS <depth> is required for FT.NAVIGATE",
        ));
    };
    let hops = hops.min(MAX_HOPS);

    // --- Parse HOP_PENALTY p (optional, default 0.1) ---
    let hop_penalty = parse_hop_penalty(args).unwrap_or(DEFAULT_HOP_PENALTY);

    // --- Parse K from the KNN query for result cap ---
    let k = parse_k_from_query(args).unwrap_or(10) as usize;

    // --- Step 1: KNN search via ft_search ---
    // Build synthetic FT.SEARCH args by stripping HOPS/HOP_PENALTY keywords.
    let synthetic_args = build_search_args(args);
    let knn_result = super::ft_search(store, &synthetic_args, db);

    // --- Step 2: Graph expand ---
    let seed_keys = extract_seeds_from_response(&knn_result);
    if seed_keys.is_empty() {
        return knn_result; // no KNN hits to expand
    }

    // Find the first graph that contains any seed key.
    let target_graph = find_target_graph(gs, &seed_keys);
    let Some(graph) = target_graph else {
        // No graph contains these keys — return KNN results with hop_depth=0.
        return annotate_knn_only(&knn_result, k);
    };

    // BFS expand through graph topology.
    let expanded = expand_results_via_graph(graph, &seed_keys, hops);

    // --- Step 3: Re-rank and merge ---
    build_navigate_response(&knn_result, &expanded, hop_penalty, k)
}

/// Parse HOPS N from args. Returns None if not found.
fn parse_hops(args: &[Frame]) -> Option<u32> {
    for i in 0..args.len() {
        if matches_keyword(&args[i], b"HOPS") {
            if i + 1 < args.len() {
                return parse_u32(&args[i + 1]);
            }
        }
    }
    None
}

/// Parse HOP_PENALTY p from args. Returns None if not found.
fn parse_hop_penalty(args: &[Frame]) -> Option<f32> {
    for i in 0..args.len() {
        if matches_keyword(&args[i], b"HOP_PENALTY") {
            if i + 1 < args.len() {
                if let Some(b) = extract_bulk(&args[i + 1]) {
                    if let Ok(s) = std::str::from_utf8(&b) {
                        return s.parse::<f32>().ok();
                    }
                }
            }
        }
    }
    None
}

/// Extract K from the KNN query string: "*=>[KNN k @field $param]".
fn parse_k_from_query(args: &[Frame]) -> Option<u32> {
    if args.len() < 2 {
        return None;
    }
    let query = extract_bulk(&args[1])?;
    let query_str = std::str::from_utf8(&query).ok()?;
    // Look for "KNN <number>" pattern in the query string.
    let upper = query_str.to_ascii_uppercase();
    let knn_pos = upper.find("KNN ")?;
    let after_knn = &query_str[knn_pos + 4..];
    let num_str = after_knn.split_whitespace().next()?;
    num_str.parse::<u32>().ok()
}

/// Build synthetic FT.SEARCH args by stripping HOPS, HOP_PENALTY keywords.
fn build_search_args(args: &[Frame]) -> Vec<Frame> {
    let mut result = Vec::with_capacity(args.len());
    let mut skip_next = false;
    for (i, frame) in args.iter().enumerate() {
        if skip_next {
            skip_next = false;
            continue;
        }
        if matches_keyword(frame, b"HOPS") || matches_keyword(frame, b"HOP_PENALTY") {
            // Skip this keyword and its value.
            if i + 1 < args.len() {
                skip_next = true;
            }
            continue;
        }
        result.push(frame.clone());
    }
    result
}

/// Find the first graph that contains any of the seed keys.
#[cfg(feature = "graph")]
fn find_target_graph<'a>(
    gs: &'a crate::graph::store::GraphStore,
    seed_keys: &[(Bytes, f32)],
) -> Option<&'a crate::graph::store::NamedGraph> {
    let graph_names = gs.list_graphs();
    for gname in &graph_names {
        if let Some(g) = gs.get_graph(gname) {
            for (key, _) in seed_keys {
                if g.lookup_node_by_key(key).is_some() {
                    return Some(g);
                }
            }
        }
    }
    None
}

/// A merged candidate for re-ranking.
struct RankedCandidate {
    key: Bytes,
    vec_score: f32,
    hop_depth: u32,
    final_score: f32,
}

/// Annotate KNN-only results with hop_depth=0 when no graph is available.
fn annotate_knn_only(knn_response: &Frame, k: usize) -> Frame {
    let items = match knn_response {
        Frame::Array(items) => items,
        _ => return knn_response.clone(),
    };
    if items.len() < 2 {
        return knn_response.clone();
    }

    let mut out = Vec::with_capacity(items.len());
    out.push(items[0].clone()); // total count

    let mut count = 0;
    let mut i = 1;
    while i + 1 < items.len() && count < k {
        out.push(items[i].clone()); // doc key

        // Augment fields with __hop_depth = "0".
        let mut fields = match &items[i + 1] {
            Frame::Array(f) => f.to_vec(),
            _ => Vec::new(),
        };
        fields.push(Frame::BulkString(Bytes::from_static(b"__hop_depth")));
        fields.push(Frame::BulkString(Bytes::from_static(b"0")));
        out.push(Frame::Array(fields.into()));

        i += 2;
        count += 1;
    }

    Frame::Array(out.into())
}

/// Build the final FT.NAVIGATE response: merge KNN + expanded, re-rank, take top K.
fn build_navigate_response(
    knn_response: &Frame,
    expanded: &[ExpandedResult],
    hop_penalty: f32,
    k: usize,
) -> Frame {
    let knn_items = match knn_response {
        Frame::Array(items) => items,
        _ => return knn_response.clone(),
    };
    if knn_items.is_empty() {
        return knn_response.clone();
    }

    let cap = k * CAP_MULTIPLIER;

    // Collect KNN results as candidates (hop_depth = 0).
    let mut candidates: Vec<RankedCandidate> = Vec::with_capacity(cap);
    let mut seen: HashMap<Bytes, usize> = HashMap::new(); // key -> index in candidates

    let mut i = 1;
    while i + 1 < knn_items.len() && candidates.len() < cap {
        let key = match &knn_items[i] {
            Frame::BulkString(b) => b.clone(),
            _ => {
                i += 2;
                continue;
            }
        };
        let score = extract_score_from_fields(&knn_items[i + 1]);
        let idx = candidates.len();
        candidates.push(RankedCandidate {
            key: key.clone(),
            vec_score: score,
            hop_depth: 0,
            final_score: score, // KNN results: final_score = vec_distance
        });
        seen.insert(key, idx);
        i += 2;
    }

    // Add expanded graph results (dedup: skip if already in KNN set).
    for er in expanded {
        if candidates.len() >= cap {
            break;
        }
        if seen.contains_key(&er.key) {
            continue; // keep the KNN version (lower score, hop_depth=0)
        }
        let final_score = if er.vec_score > 0.0 {
            er.vec_score + (er.graph_hops as f32 * hop_penalty)
        } else {
            er.graph_hops as f32 * hop_penalty
        };
        let idx = candidates.len();
        candidates.push(RankedCandidate {
            key: er.key.clone(),
            vec_score: er.vec_score,
            hop_depth: er.graph_hops,
            final_score,
        });
        seen.insert(er.key.clone(), idx);
    }

    // Sort by final_score ascending (lower = better, consistent with L2 distance).
    candidates.sort_by(|a, b| {
        a.final_score
            .partial_cmp(&b.final_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // Take top K.
    candidates.truncate(k);

    // Build response Frame.
    let total = candidates.len() as i64;
    let mut items = Vec::with_capacity(1 + candidates.len() * 2);
    items.push(Frame::Integer(total));

    for c in &candidates {
        items.push(Frame::BulkString(c.key.clone()));

        let mut score_buf = String::with_capacity(12);
        let _ = write!(score_buf, "{}", c.vec_score);
        let mut hop_buf = itoa::Buffer::new();
        let hop_str = hop_buf.format(c.hop_depth);
        let mut final_buf = String::with_capacity(12);
        let _ = write!(final_buf, "{}", c.final_score);

        let fields = vec![
            Frame::BulkString(Bytes::from_static(b"__vec_score")),
            Frame::BulkString(Bytes::from(score_buf)),
            Frame::BulkString(Bytes::from_static(b"__hop_depth")),
            Frame::BulkString(Bytes::from(hop_str.to_owned())),
            Frame::BulkString(Bytes::from_static(b"__final_score")),
            Frame::BulkString(Bytes::from(final_buf)),
        ];
        items.push(Frame::Array(fields.into()));
    }

    Frame::Array(items.into())
}

/// Extract vec_score from a result fields array.
fn extract_score_from_fields(fields: &Frame) -> f32 {
    if let Frame::Array(items) = fields {
        for pair in items.chunks(2) {
            if pair.len() == 2 {
                if let Frame::BulkString(key) = &pair[0] {
                    if key.as_ref() == b"__vec_score" {
                        if let Frame::BulkString(val) = &pair[1] {
                            if let Ok(s) = std::str::from_utf8(val) {
                                return s.parse().unwrap_or(f32::MAX);
                            }
                        }
                    }
                }
            }
        }
    }
    f32::MAX
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_hops() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"myidx")),
            Frame::BulkString(Bytes::from_static(b"*=>[KNN 10 @vec $v]")),
            Frame::BulkString(Bytes::from_static(b"HOPS")),
            Frame::BulkString(Bytes::from_static(b"3")),
        ];
        assert_eq!(parse_hops(&args), Some(3));
    }

    #[test]
    fn test_parse_hops_missing() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"myidx")),
            Frame::BulkString(Bytes::from_static(b"*=>[KNN 10 @vec $v]")),
        ];
        assert_eq!(parse_hops(&args), None);
    }

    #[test]
    fn test_parse_hop_penalty() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"HOPS")),
            Frame::BulkString(Bytes::from_static(b"2")),
            Frame::BulkString(Bytes::from_static(b"HOP_PENALTY")),
            Frame::BulkString(Bytes::from_static(b"0.25")),
        ];
        let p = parse_hop_penalty(&args);
        assert!((p.unwrap_or(0.0) - 0.25).abs() < f32::EPSILON);
    }

    #[test]
    fn test_parse_k_from_query() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"idx")),
            Frame::BulkString(Bytes::from_static(b"*=>[KNN 5 @vec $v]")),
        ];
        assert_eq!(parse_k_from_query(&args), Some(5));
    }

    #[test]
    fn test_build_search_args_strips_hops() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"idx")),
            Frame::BulkString(Bytes::from_static(b"query")),
            Frame::BulkString(Bytes::from_static(b"HOPS")),
            Frame::BulkString(Bytes::from_static(b"3")),
            Frame::BulkString(Bytes::from_static(b"HOP_PENALTY")),
            Frame::BulkString(Bytes::from_static(b"0.2")),
            Frame::BulkString(Bytes::from_static(b"PARAMS")),
            Frame::BulkString(Bytes::from_static(b"2")),
        ];
        let result = build_search_args(&args);
        assert_eq!(result.len(), 4); // idx, query, PARAMS, 2
        // Verify HOPS and HOP_PENALTY are stripped.
        for f in &result {
            if let Frame::BulkString(b) = f {
                assert_ne!(b.as_ref(), b"HOPS");
                assert_ne!(b.as_ref(), b"HOP_PENALTY");
            }
        }
    }

    #[test]
    fn test_extract_score_from_fields() {
        let fields = Frame::Array(
            vec![
                Frame::BulkString(Bytes::from_static(b"__vec_score")),
                Frame::BulkString(Bytes::from_static(b"0.42")),
            ]
            .into(),
        );
        let score = extract_score_from_fields(&fields);
        assert!((score - 0.42).abs() < 0.001);
    }

    #[test]
    fn test_ranked_candidates_sort() {
        let mut candidates = vec![
            RankedCandidate {
                key: Bytes::from_static(b"b"),
                vec_score: 0.5,
                hop_depth: 0,
                final_score: 0.5,
            },
            RankedCandidate {
                key: Bytes::from_static(b"a"),
                vec_score: 0.1,
                hop_depth: 0,
                final_score: 0.1,
            },
            RankedCandidate {
                key: Bytes::from_static(b"c"),
                vec_score: 0.0,
                hop_depth: 2,
                final_score: 0.2,
            },
        ];
        candidates.sort_by(|a, b| {
            a.final_score
                .partial_cmp(&b.final_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        assert_eq!(candidates[0].key.as_ref(), b"a");
        assert_eq!(candidates[1].key.as_ref(), b"c");
        assert_eq!(candidates[2].key.as_ref(), b"b");
    }
}
