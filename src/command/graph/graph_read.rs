//! GRAPH.* read command handlers.
//!
//! These commands read from GraphStore: NEIGHBORS, INFO, LIST, QUERY, RO_QUERY, EXPLAIN.

use bytes::Bytes;
use slotmap::Key;

use crate::graph::cypher;
use crate::graph::store::GraphStore;
use crate::graph::types::Direction;
use crate::protocol::Frame;

use super::graph_write::extract_bulk;

/// GRAPH.NEIGHBORS <graph> <node_id> [TYPE <type>] [DEPTH <n>]
///
/// Returns an array of neighbor nodes/edges as RESP3 Maps.
/// Default direction: BOTH (outgoing + incoming).
/// DEPTH > 1 performs multi-hop expansion (BFS).
pub fn graph_neighbors(store: &GraphStore, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'GRAPH.NEIGHBORS' command",
        ));
    }

    let graph_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid graph name")),
    };

    let node_id = match parse_u64(&args[1]) {
        Some(id) => id,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid node ID")),
    };

    let graph = match store.get_graph(graph_name) {
        Some(g) => g,
        None => return Frame::Error(Bytes::from_static(b"ERR graph not found")),
    };

    // Parse optional TYPE and DEPTH arguments.
    let mut edge_type_filter: Option<u16> = None;
    let mut depth: u32 = 1;
    let mut pos = 2;

    while pos < args.len() {
        let key = match extract_bulk(&args[pos]) {
            Some(b) => b,
            None => {
                pos += 1;
                continue;
            }
        };

        if key.eq_ignore_ascii_case(b"TYPE") {
            pos += 1;
            if pos >= args.len() {
                return Frame::Error(Bytes::from_static(b"ERR missing TYPE value"));
            }
            if let Some(type_name) = extract_bulk(&args[pos]) {
                edge_type_filter = Some(super::graph_write::label_to_id(type_name));
            }
            pos += 1;
        } else if key.eq_ignore_ascii_case(b"DEPTH") {
            pos += 1;
            if pos >= args.len() {
                return Frame::Error(Bytes::from_static(b"ERR missing DEPTH value"));
            }
            depth = match parse_u32(&args[pos]) {
                Some(d) if d > 0 => d,
                _ => return Frame::Error(Bytes::from_static(b"ERR invalid DEPTH value")),
            };
            pos += 1;
        } else {
            pos += 1;
        }
    }

    // Cap depth to prevent explosion.
    let max_depth = 10u32;
    if depth > max_depth {
        return Frame::Error(Bytes::from_static(b"ERR DEPTH exceeds maximum (10)"));
    }

    let node_key = super::graph_write::external_id_to_node_key(node_id);

    // Use write_buf (mutable MemGraph) for reading since it has current data.
    // Future: merge with immutable CSR segments.
    let memgraph = &graph.write_buf;

    // Verify node exists.
    if memgraph.get_node(node_key).is_none() {
        return Frame::Error(Bytes::from_static(b"ERR node not found"));
    }

    // BFS expansion.
    let lsn = u64::MAX - 1; // See all live data (MAX-1 because deleted_lsn=MAX means alive).
    let mut visited = std::collections::HashSet::new();
    visited.insert(node_id);
    let mut frontier = vec![node_key];
    let mut results: Vec<Frame> = Vec::new();
    // Cap total results.
    let max_results = 10_000usize;

    for _hop in 0..depth {
        let mut next_frontier = Vec::new();

        for &current in &frontier {
            for (edge_key, neighbor_key) in memgraph.neighbors(current, Direction::Both, lsn) {
                // Apply edge type filter if specified.
                if let Some(filter_type) = edge_type_filter {
                    if let Some(edge) = memgraph.get_edge(edge_key) {
                        if edge.edge_type != filter_type {
                            continue;
                        }
                    }
                }

                let neighbor_ext_id = neighbor_key.data().as_ffi();

                if visited.contains(&neighbor_ext_id) {
                    continue;
                }
                visited.insert(neighbor_ext_id);

                // Add edge as RESP3 Map.
                if let Some(edge) = memgraph.get_edge(edge_key) {
                    results.push(edge_to_frame(edge_key, edge));
                }

                // Add neighbor node as RESP3 Map.
                if let Some(node) = memgraph.get_node(neighbor_key) {
                    results.push(node_to_frame(neighbor_key, node));
                }

                if results.len() >= max_results {
                    break;
                }

                next_frontier.push(neighbor_key);
            }

            if results.len() >= max_results {
                break;
            }
        }

        frontier = next_frontier;
        if frontier.is_empty() {
            break;
        }
    }

    Frame::Array(results.into())
}

/// GRAPH.INFO <graph>
///
/// Returns graph statistics as RESP3 Map.
pub fn graph_info(store: &GraphStore, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'GRAPH.INFO' command",
        ));
    }

    let graph_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid graph name")),
    };

    let graph = match store.get_graph(graph_name) {
        Some(g) => g,
        None => return Frame::Error(Bytes::from_static(b"ERR graph not found")),
    };

    let memgraph = &graph.write_buf;
    let segments = graph.segments.load();
    let stats = &graph.stats;

    let node_count = memgraph.node_count() as i64;
    let edge_count = memgraph.edge_count() as i64;
    let immutable_segments = segments.immutable.len() as i64;

    // Degree distribution from GraphStats.
    let degree_stats = Frame::Map(vec![
        (
            Frame::SimpleString(Bytes::from_static(b"avg")),
            Frame::Double(stats.degree_stats.avg),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"p50")),
            Frame::Integer(stats.degree_stats.p50 as i64),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"p99")),
            Frame::Integer(stats.degree_stats.p99 as i64),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"max")),
            Frame::Integer(stats.degree_stats.max as i64),
        ),
    ]);

    Frame::Map(vec![
        (
            Frame::SimpleString(Bytes::from_static(b"name")),
            Frame::BulkString(graph.name.clone()),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"node_count")),
            Frame::Integer(node_count),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"edge_count")),
            Frame::Integer(edge_count),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"immutable_segments")),
            Frame::Integer(immutable_segments),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"edge_threshold")),
            Frame::Integer(graph.edge_threshold as i64),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"created_lsn")),
            Frame::Integer(graph.created_lsn as i64),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"degree_stats")),
            degree_stats,
        ),
    ])
}

/// GRAPH.LIST
///
/// Returns an array of all graph names.
pub fn graph_list(store: &GraphStore) -> Frame {
    let names = store.list_graphs();
    let frames: Vec<Frame> = names
        .into_iter()
        .map(|name| Frame::BulkString(name.clone()))
        .collect();
    Frame::Array(frames.into())
}

// ---------------------------------------------------------------------------
// GRAPH.QUERY, GRAPH.RO_QUERY, GRAPH.EXPLAIN
// ---------------------------------------------------------------------------

/// GRAPH.QUERY <graph> <cypher_string>
///
/// Parses and compiles the Cypher query. Full execution is deferred to Phase 118+.
/// Currently returns the compiled physical plan as a diagnostic array.
pub fn graph_query(store: &GraphStore, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'GRAPH.QUERY' command",
        ));
    }

    let graph_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid graph name")),
    };

    if store.get_graph(graph_name).is_none() {
        return Frame::Error(Bytes::from_static(b"ERR graph not found"));
    }

    let cypher_bytes = match extract_bulk(&args[1]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid Cypher query")),
    };

    let query = match cypher::parse_cypher(cypher_bytes) {
        Ok(q) => q,
        Err(e) => {
            let msg = format!("ERR Cypher parse error: {e}");
            return Frame::Error(Bytes::from(msg));
        }
    };

    let plan = match cypher::planner::compile(&query) {
        Ok(p) => p,
        Err(e) => {
            let msg = format!("ERR Cypher plan error: {e}");
            return Frame::Error(Bytes::from(msg));
        }
    };

    // Return plan summary as array of operator descriptions.
    let ops: Vec<Frame> = plan
        .operators
        .iter()
        .map(|op| Frame::BulkString(Bytes::from(format!("{op:?}"))))
        .collect();

    Frame::Array(ops.into())
}

/// GRAPH.RO_QUERY <graph> <cypher_string>
///
/// Like GRAPH.QUERY but rejects write clauses (CREATE, DELETE, SET, MERGE).
pub fn graph_ro_query(store: &GraphStore, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'GRAPH.RO_QUERY' command",
        ));
    }

    let cypher_bytes = match extract_bulk(&args[1]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid Cypher query")),
    };

    let query = match cypher::parse_cypher(cypher_bytes) {
        Ok(q) => q,
        Err(e) => {
            let msg = format!("ERR Cypher parse error: {e}");
            return Frame::Error(Bytes::from(msg));
        }
    };

    if !query.is_read_only() {
        return Frame::Error(Bytes::from_static(
            b"ERR GRAPH.RO_QUERY does not allow write clauses (CREATE, DELETE, SET, MERGE)",
        ));
    }

    // Delegate to the regular query handler for parsing/planning.
    graph_query(store, args)
}

/// GRAPH.EXPLAIN <graph> <cypher_string>
///
/// Returns the execution plan without running the query.
/// Includes cost-based strategy selection when graph stats are available.
pub fn graph_explain(store: &GraphStore, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'GRAPH.EXPLAIN' command",
        ));
    }

    let graph_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid graph name")),
    };

    let cypher_bytes = match extract_bulk(&args[1]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid Cypher query")),
    };

    let query = match cypher::parse_cypher(cypher_bytes) {
        Ok(q) => q,
        Err(e) => {
            let msg = format!("ERR Cypher parse error: {e}");
            return Frame::Error(Bytes::from(msg));
        }
    };

    let plan = match cypher::planner::compile(&query) {
        Ok(p) => p,
        Err(e) => {
            let msg = format!("ERR Cypher plan error: {e}");
            return Frame::Error(Bytes::from(msg));
        }
    };

    // Return plan as a formatted string.
    let mut output = String::new();
    for (i, op) in plan.operators.iter().enumerate() {
        if !output.is_empty() {
            output.push('\n');
        }
        output.push_str(&format!("{i}: {op:?}"));
    }

    // Append cost-based strategy selection if graph exists.
    if let Some(graph) = store.get_graph(graph_name) {
        let stats = &graph.stats;

        // Extract traversal parameters from the plan operators.
        let hops = extract_max_hops(&plan);
        let k = 10u32; // Default k for vector search.
        let dim = 128u32; // Default dimension estimate.

        let estimate = cypher::planner::select_strategy(
            stats, 1, // start_nodes (single seed)
            hops, k, dim, None, // No specific start node degree without node ID.
        );

        output.push_str(&format!(
            "\n--- Cost Estimation ---\nStrategy: {}\nGraph-first cost: {:.1}\nVector-first cost: {:.1}\nHub detected: {}",
            estimate.strategy,
            estimate.graph_first_cost,
            estimate.vector_first_cost,
            estimate.hub_detected,
        ));
    }

    Frame::BulkString(Bytes::from(output))
}

/// Extract the maximum hop count from Expand operators in a physical plan.
fn extract_max_hops(plan: &cypher::planner::PhysicalPlan) -> u32 {
    let mut max_hops = 1u32;
    for op in &plan.operators {
        if let cypher::planner::PhysicalOp::Expand { max_hops: mh, .. } = op {
            if *mh > max_hops {
                max_hops = *mh;
            }
        }
    }
    max_hops
}

// ---------------------------------------------------------------------------
// RESP3 entity formatting
// ---------------------------------------------------------------------------

/// Format a node as a RESP3 Map: {id, labels, properties}.
fn node_to_frame(
    key: crate::graph::types::NodeKey,
    node: &crate::graph::types::MutableNode,
) -> Frame {
    let external_id = key.data().as_ffi();

    let labels: Vec<Frame> = node
        .labels
        .iter()
        .map(|&l| Frame::Integer(l as i64))
        .collect();

    let props = properties_to_frame(&node.properties);

    Frame::Map(vec![
        (
            Frame::SimpleString(Bytes::from_static(b"id")),
            Frame::Integer(external_id as i64),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"labels")),
            Frame::Array(labels.into()),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"properties")),
            props,
        ),
    ])
}

/// Format an edge as a RESP3 Map: {id, type, src, dst, properties}.
fn edge_to_frame(
    key: crate::graph::types::EdgeKey,
    edge: &crate::graph::types::MutableEdge,
) -> Frame {
    let external_id = key.data().as_ffi();

    let src_ext = edge.src.data().as_ffi();
    let dst_ext = edge.dst.data().as_ffi();

    let props = match &edge.properties {
        Some(p) => properties_to_frame(p),
        None => Frame::Map(Vec::new()),
    };

    Frame::Map(vec![
        (
            Frame::SimpleString(Bytes::from_static(b"id")),
            Frame::Integer(external_id as i64),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"type")),
            Frame::Integer(edge.edge_type as i64),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"src")),
            Frame::Integer(src_ext as i64),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"dst")),
            Frame::Integer(dst_ext as i64),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"weight")),
            Frame::Double(edge.weight),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"properties")),
            props,
        ),
    ])
}

/// Convert a PropertyMap to a RESP3 Map frame.
fn properties_to_frame(props: &crate::graph::types::PropertyMap) -> Frame {
    let pairs: Vec<(Frame, Frame)> = props
        .iter()
        .map(|(key, val)| {
            let k = Frame::Integer(*key as i64);
            let v = match val {
                crate::graph::types::PropertyValue::Int(n) => Frame::Integer(*n),
                crate::graph::types::PropertyValue::Float(f) => Frame::Double(*f),
                crate::graph::types::PropertyValue::String(s) => Frame::BulkString(s.clone()),
                crate::graph::types::PropertyValue::Bool(b) => Frame::Boolean(*b),
                crate::graph::types::PropertyValue::Bytes(b) => Frame::BulkString(b.clone()),
            };
            (k, v)
        })
        .collect();
    Frame::Map(pairs)
}

// ---------------------------------------------------------------------------
// GRAPH.VSEARCH — graph-filtered vector search (HYB-01)
// ---------------------------------------------------------------------------

/// GRAPH.VSEARCH <graph> <start_node_id> <hops> <k> <vector_blob> [THRESHOLD <n>] [TYPE <edge_type>]
///
/// Traverses `hops` from `start_node_id`, collects candidate nodes, then scores
/// by cosine similarity to `vector_blob`. Returns top-K results.
pub fn graph_vsearch(store: &GraphStore, args: &[Frame]) -> Frame {
    if args.len() < 5 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'GRAPH.VSEARCH' command",
        ));
    }

    let graph_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid graph name")),
    };

    let start_id = match parse_u64(&args[1]) {
        Some(id) => id,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid start node ID")),
    };

    let hops = match parse_u32(&args[2]) {
        Some(h) if h > 0 && h <= 10 => h,
        _ => return Frame::Error(Bytes::from_static(b"ERR invalid hops (1-10)")),
    };

    let k = match parse_u32(&args[3]) {
        Some(k) if k > 0 => k as usize,
        _ => return Frame::Error(Bytes::from_static(b"ERR invalid k")),
    };

    let query_vector = match extract_f32_vector(&args[4]) {
        Some(v) if !v.is_empty() => v,
        _ => return Frame::Error(Bytes::from_static(b"ERR invalid vector blob")),
    };

    let graph = match store.get_graph(graph_name) {
        Some(g) => g,
        None => return Frame::Error(Bytes::from_static(b"ERR graph not found")),
    };

    // Parse optional args.
    let mut threshold = crate::graph::hybrid::DEFAULT_STRATEGY_THRESHOLD;
    let mut edge_type_filter: Option<u16> = None;
    let mut pos = 5;
    while pos < args.len() {
        let key = match extract_bulk(&args[pos]) {
            Some(b) => b,
            None => {
                pos += 1;
                continue;
            }
        };
        if key.eq_ignore_ascii_case(b"THRESHOLD") {
            pos += 1;
            if pos < args.len() {
                if let Some(t) = parse_u32(&args[pos]) {
                    threshold = t as usize;
                }
            }
            pos += 1;
        } else if key.eq_ignore_ascii_case(b"TYPE") {
            pos += 1;
            if pos < args.len() {
                if let Some(type_name) = extract_bulk(&args[pos]) {
                    edge_type_filter = Some(super::graph_write::label_to_id(type_name));
                }
            }
            pos += 1;
        } else {
            pos += 1;
        }
    }

    let node_key = super::graph_write::external_id_to_node_key(start_id);
    let memgraph = &graph.write_buf;
    let lsn = u64::MAX - 1;

    let mut search =
        crate::graph::hybrid::GraphFilteredSearch::new(node_key, hops, query_vector, k);
    search.threshold = threshold;
    search.edge_type_filter = edge_type_filter;

    match search.execute(memgraph, lsn) {
        Ok(results) => hybrid_results_to_frame(&results),
        Err(e) => Frame::Error(Bytes::from(format!("ERR {e}"))),
    }
}

// ---------------------------------------------------------------------------
// GRAPH.HYBRID — general hybrid query dispatcher
// ---------------------------------------------------------------------------

/// GRAPH.HYBRID <graph> <mode> <args...>
///
/// Modes:
///   FILTER <start_id> <hops> <k> <vector> — graph-filtered vector search (HYB-01)
///   EXPAND <k> <expansion_hops> <vector> — vector-to-graph expansion (HYB-02)
///   WALK <start_id> <max_depth> <beam_width> <min_sim> <vector> — vector-guided walk (HYB-03)
pub fn graph_hybrid(store: &GraphStore, args: &[Frame]) -> Frame {
    if args.len() < 3 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'GRAPH.HYBRID' command",
        ));
    }

    let graph_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid graph name")),
    };

    let mode = match extract_bulk(&args[1]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid mode")),
    };

    let graph = match store.get_graph(graph_name) {
        Some(g) => g,
        None => return Frame::Error(Bytes::from_static(b"ERR graph not found")),
    };

    let memgraph = &graph.write_buf;
    let lsn = u64::MAX - 1;

    if mode.eq_ignore_ascii_case(b"FILTER") {
        // GRAPH.HYBRID g FILTER <start_id> <hops> <k> <vector>
        if args.len() < 6 {
            return Frame::Error(Bytes::from_static(
                b"ERR FILTER requires: start_id hops k vector",
            ));
        }
        let start_id = match parse_u64(&args[2]) {
            Some(id) => id,
            None => return Frame::Error(Bytes::from_static(b"ERR invalid start node ID")),
        };
        let hops = match parse_u32(&args[3]) {
            Some(h) if h > 0 && h <= 10 => h,
            _ => return Frame::Error(Bytes::from_static(b"ERR invalid hops")),
        };
        let k = match parse_u32(&args[4]) {
            Some(k) if k > 0 => k as usize,
            _ => return Frame::Error(Bytes::from_static(b"ERR invalid k")),
        };
        let query_vector = match extract_f32_vector(&args[5]) {
            Some(v) if !v.is_empty() => v,
            _ => return Frame::Error(Bytes::from_static(b"ERR invalid vector")),
        };

        let node_key = super::graph_write::external_id_to_node_key(start_id);
        let search =
            crate::graph::hybrid::GraphFilteredSearch::new(node_key, hops, query_vector, k);
        match search.execute(memgraph, lsn) {
            Ok(results) => hybrid_results_to_frame(&results),
            Err(e) => Frame::Error(Bytes::from(format!("ERR {e}"))),
        }
    } else if mode.eq_ignore_ascii_case(b"WALK") {
        // GRAPH.HYBRID g WALK <start_id> <max_depth> <beam_width> <min_sim> <vector>
        if args.len() < 7 {
            return Frame::Error(Bytes::from_static(
                b"ERR WALK requires: start_id max_depth beam_width min_sim vector",
            ));
        }
        let start_id = match parse_u64(&args[2]) {
            Some(id) => id,
            None => return Frame::Error(Bytes::from_static(b"ERR invalid start node ID")),
        };
        let max_depth = match parse_u32(&args[3]) {
            Some(d) if d > 0 && d <= 100 => d,
            _ => return Frame::Error(Bytes::from_static(b"ERR invalid max_depth")),
        };
        let beam_width = match parse_u32(&args[4]) {
            Some(bw) if bw > 0 => bw as usize,
            _ => return Frame::Error(Bytes::from_static(b"ERR invalid beam_width")),
        };
        let min_sim = match parse_f64(&args[5]) {
            Some(s) => s,
            None => return Frame::Error(Bytes::from_static(b"ERR invalid min_sim")),
        };
        let query_vector = match extract_f32_vector(&args[6]) {
            Some(v) if !v.is_empty() => v,
            _ => return Frame::Error(Bytes::from_static(b"ERR invalid vector")),
        };

        let node_key = super::graph_write::external_id_to_node_key(start_id);
        let mut walk =
            crate::graph::hybrid::VectorGuidedWalk::new(node_key, query_vector, max_depth);
        walk.beam_width = beam_width;
        walk.min_similarity = min_sim;

        match walk.execute(memgraph, lsn) {
            Ok(results) => hybrid_results_to_frame(&results),
            Err(e) => Frame::Error(Bytes::from(format!("ERR {e}"))),
        }
    } else {
        Frame::Error(Bytes::from_static(
            b"ERR unknown GRAPH.HYBRID mode (supported: FILTER, WALK)",
        ))
    }
}

// ---------------------------------------------------------------------------
// Hybrid result formatting
// ---------------------------------------------------------------------------

/// Convert hybrid results to a RESP3 Array of Maps.
fn hybrid_results_to_frame(results: &[crate::graph::hybrid::HybridResult]) -> Frame {
    let frames: Vec<Frame> = results
        .iter()
        .map(|r| {
            let ext_id = r.node.data().as_ffi();
            let mut pairs = vec![
                (
                    Frame::SimpleString(Bytes::from_static(b"id")),
                    Frame::Integer(ext_id as i64),
                ),
                (
                    Frame::SimpleString(Bytes::from_static(b"score")),
                    Frame::Double(r.score),
                ),
            ];

            if let Some(dist) = r.graph_distance {
                pairs.push((
                    Frame::SimpleString(Bytes::from_static(b"graph_distance")),
                    Frame::Integer(dist as i64),
                ));
            }

            if !r.context.is_empty() {
                let ctx: Vec<Frame> = r
                    .context
                    .iter()
                    .map(|c| {
                        let ctx_id = c.node.data().as_ffi();
                        Frame::Map(vec![
                            (
                                Frame::SimpleString(Bytes::from_static(b"id")),
                                Frame::Integer(ctx_id as i64),
                            ),
                            (
                                Frame::SimpleString(Bytes::from_static(b"edge_type")),
                                Frame::Integer(c.edge_type as i64),
                            ),
                            (
                                Frame::SimpleString(Bytes::from_static(b"hops")),
                                Frame::Integer(c.hops as i64),
                            ),
                        ])
                    })
                    .collect();
                pairs.push((
                    Frame::SimpleString(Bytes::from_static(b"context")),
                    Frame::Array(ctx.into()),
                ));
            }

            Frame::Map(pairs)
        })
        .collect();

    Frame::Array(frames.into())
}

/// Extract a float vector from a Frame (space-separated f32 values in a BulkString).
fn extract_f32_vector(frame: &Frame) -> Option<Vec<f32>> {
    let bytes = match frame {
        Frame::BulkString(b) => b.as_ref(),
        Frame::SimpleString(b) => b.as_ref(),
        _ => return None,
    };

    let text = core::str::from_utf8(bytes).ok()?;
    let values: Result<Vec<f32>, _> = text.split_whitespace().map(|s| s.parse::<f32>()).collect();
    values.ok()
}

/// Parse an f64 from a Frame.
fn parse_f64(frame: &Frame) -> Option<f64> {
    match frame {
        Frame::Double(f) => Some(*f),
        Frame::Integer(n) => Some(*n as f64),
        Frame::BulkString(b) | Frame::SimpleString(b) => core::str::from_utf8(b).ok()?.parse().ok(),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_u64(frame: &Frame) -> Option<u64> {
    match frame {
        Frame::Integer(n) => {
            if *n >= 0 {
                Some(*n as u64)
            } else {
                None
            }
        }
        Frame::BulkString(b) | Frame::SimpleString(b) => core::str::from_utf8(b).ok()?.parse().ok(),
        _ => None,
    }
}

fn parse_u32(frame: &Frame) -> Option<u32> {
    match frame {
        Frame::Integer(n) => {
            if *n >= 0 && *n <= u32::MAX as i64 {
                Some(*n as u32)
            } else {
                None
            }
        }
        Frame::BulkString(b) | Frame::SimpleString(b) => core::str::from_utf8(b).ok()?.parse().ok(),
        _ => None,
    }
}
