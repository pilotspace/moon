//! Cross-shard graph traversal via scatter-gather over SPSC mesh.
//!
//! When a graph traversal encounters nodes that may live on other shards,
//! the coordinator groups node IDs by target shard, sends `GraphTraverse`
//! messages via SPSC, and merges results from all participating shards.
//!
//! **Shard-local optimization:** If a graph name contains a hash tag
//! `{partition_key}`, all GRAPH.* operations route to the same shard and
//! no cross-shard traversal is needed.
//!
//! **Depth limit:** Cross-shard expansion stops at a configurable depth
//! (default 2 hops) and returns partial results with a truncation notice.
//!
//! **Snapshot consistency:** The originating traversal's snapshot-LSN is
//! forwarded to all participating shards so they all see the same graph version.

use bytes::Bytes;
use slotmap::Key;

use crate::graph::store::GraphStore;
use crate::graph::types::Direction;
use crate::protocol::Frame;

/// Default maximum cross-shard hops before truncation.
pub const DEFAULT_CROSS_SHARD_DEPTH_LIMIT: u32 = 2;

/// Result of a local shard expansion for cross-shard traversal.
///
/// Contains neighbor node external IDs discovered on this shard,
/// plus the edge/node Frame representations for the final response.
#[derive(Debug)]
pub struct TraversalShardResult {
    /// External IDs of discovered neighbor nodes.
    pub neighbor_ids: Vec<u64>,
    /// Edge type for each discovered neighbor (parallel with neighbor_ids).
    pub edge_types: Vec<u16>,
    /// RESP3 frames for edges and nodes found on this shard.
    pub frames: Vec<Frame>,
    /// Whether the result was truncated due to depth or size limits.
    pub truncated: bool,
}

/// Handle an incoming `GraphTraverse` SPSC message on this shard.
///
/// Expands the given node IDs locally using the shard's MemGraph,
/// returning a Frame::Array of neighbor edges and nodes, plus a
/// bulk string listing discovered neighbor external IDs for further
/// expansion by the coordinator.
///
/// Response format:
/// ```text
/// Array [
///   BulkString("NEIGHBORS"),     -- marker
///   Array [ ... frames ... ],    -- edge/node RESP3 maps
///   BulkString("DISCOVERED"),    -- marker
///   Array [ Integer(id), ... ],  -- neighbor external IDs for next hop
///   BulkString("TRUNCATED"),     -- only present if truncated
/// ]
/// ```
pub fn handle_graph_traverse(
    store: &GraphStore,
    graph_name: &[u8],
    node_ids: &[u64],
    edge_type_filter: Option<u16>,
    snapshot_lsn: u64,
) -> Frame {
    let graph = match store.get_graph(graph_name) {
        Some(g) => g,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR graph not found for cross-shard traversal",
            ));
        }
    };

    let memgraph = &graph.write_buf;
    let max_results = 10_000usize;
    let mut result_frames: Vec<Frame> = Vec::with_capacity(64);
    let mut discovered_ids: Vec<Frame> = Vec::with_capacity(64);
    let mut visited_this_batch = std::collections::HashSet::new();
    let mut truncated = false;

    for &ext_id in node_ids {
        let node_key = crate::command::graph::graph_write::external_id_to_node_key(ext_id);

        // Skip nodes that don't exist on this shard.
        if memgraph.get_node(node_key).is_none() {
            continue;
        }

        for (edge_key, neighbor_key) in
            memgraph.neighbors(node_key, Direction::Both, snapshot_lsn)
        {
            // Apply edge type filter.
            if let Some(filter) = edge_type_filter {
                if let Some(edge) = memgraph.get_edge(edge_key) {
                    if edge.edge_type != filter {
                        continue;
                    }
                }
            }

            let neighbor_ext_id = neighbor_key.data().as_ffi();

            if !visited_this_batch.insert(neighbor_ext_id) {
                continue;
            }

            // Add edge frame.
            if let Some(edge) = memgraph.get_edge(edge_key) {
                result_frames.push(edge_to_traverse_frame(edge_key, edge));
            }

            // Add node frame.
            if let Some(node) = memgraph.get_node(neighbor_key) {
                result_frames.push(node_to_traverse_frame(neighbor_key, node));
            }

            // Track discovered IDs for further expansion.
            discovered_ids.push(Frame::Integer(neighbor_ext_id as i64));

            if result_frames.len() >= max_results {
                truncated = true;
                break;
            }
        }

        if truncated {
            break;
        }
    }

    // Build response array.
    let mut response = Vec::with_capacity(5);
    response.push(Frame::BulkString(Bytes::from_static(b"NEIGHBORS")));
    response.push(Frame::Array(result_frames.into()));
    response.push(Frame::BulkString(Bytes::from_static(b"DISCOVERED")));
    response.push(Frame::Array(discovered_ids.into()));
    if truncated {
        response.push(Frame::BulkString(Bytes::from_static(b"TRUNCATED")));
    }

    Frame::Array(response.into())
}

/// Check if a graph name has a hash tag, meaning all operations are shard-local.
///
/// When a graph name like `{social}.friends` is used, the `{social}` tag
/// ensures all operations route to the same shard, so no cross-shard
/// traversal is ever needed.
#[inline]
pub fn graph_has_hash_tag(graph_name: &[u8]) -> bool {
    crate::shard::dispatch::extract_hash_tag(graph_name).is_some()
}

/// Parse a GraphTraverse response frame into structured data.
///
/// Used by the scatter-gather coordinator to extract discovered node IDs
/// from the response for the next hop.
pub fn parse_traverse_response(frame: &Frame) -> Option<TraversalShardResult> {
    let items = match frame {
        Frame::Array(items) => items,
        _ => return None,
    };

    if items.len() < 4 {
        return None;
    }

    // items[0] = "NEIGHBORS" marker
    // items[1] = Array of edge/node frames
    // items[2] = "DISCOVERED" marker
    // items[3] = Array of discovered IDs
    // items[4] = "TRUNCATED" (optional)

    let frames = match &items[1] {
        Frame::Array(f) => f.to_vec(),
        _ => return None,
    };

    let discovered = match &items[3] {
        Frame::Array(ids) => ids,
        _ => return None,
    };

    let neighbor_ids: Vec<u64> = discovered
        .iter()
        .filter_map(|f| match f {
            Frame::Integer(id) => Some(*id as u64),
            _ => None,
        })
        .collect();

    let truncated = items.len() > 4
        && matches!(&items[4], Frame::BulkString(b) if b.as_ref() == b"TRUNCATED");

    Some(TraversalShardResult {
        neighbor_ids,
        edge_types: Vec::new(), // Edge types tracked in frames, not separately
        frames,
        truncated,
    })
}

// ---------------------------------------------------------------------------
// Frame helpers (lightweight versions for traversal responses)
// ---------------------------------------------------------------------------

fn node_to_traverse_frame(
    key: crate::graph::types::NodeKey,
    node: &crate::graph::types::MutableNode,
) -> Frame {
    let external_id = key.data().as_ffi();

    let labels: Vec<Frame> = node
        .labels
        .iter()
        .map(|&l| Frame::Integer(l as i64))
        .collect();

    Frame::Map(vec![
        (
            Frame::SimpleString(Bytes::from_static(b"id")),
            Frame::Integer(external_id as i64),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"labels")),
            Frame::Array(labels.into()),
        ),
    ])
}

fn edge_to_traverse_frame(
    key: crate::graph::types::EdgeKey,
    edge: &crate::graph::types::MutableEdge,
) -> Frame {
    let external_id = key.data().as_ffi();
    let src_ext = edge.src.data().as_ffi();
    let dst_ext = edge.dst.data().as_ffi();

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
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::store::GraphStore;
    use crate::protocol::FrameVec;

    fn make_cmd(parts: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = parts
            .iter()
            .map(|p| Frame::BulkString(Bytes::from(p.to_vec())))
            .collect();
        Frame::Array(FrameVec::from_vec(frames))
    }

    #[test]
    fn test_graph_has_hash_tag() {
        assert!(graph_has_hash_tag(b"{social}.friends"));
        assert!(graph_has_hash_tag(b"{partition}"));
        assert!(!graph_has_hash_tag(b"social_graph"));
        assert!(!graph_has_hash_tag(b"{}empty_tag"));
    }

    #[test]
    fn test_handle_traverse_graph_not_found() {
        let store = GraphStore::new();
        let resp = handle_graph_traverse(&store, b"nonexistent", &[1, 2], None, u64::MAX - 1);
        assert!(matches!(resp, Frame::Error(_)));
    }

    #[test]
    fn test_handle_traverse_empty_nodes() {
        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"g"), 64_000, 1)
            .expect("ok");

        let resp = handle_graph_traverse(&store, b"g", &[], None, u64::MAX - 1);
        if let Frame::Array(items) = &resp {
            // Should have NEIGHBORS marker, empty array, DISCOVERED marker, empty array.
            assert_eq!(items.len(), 4);
            if let Frame::Array(neighbors) = &items[1] {
                assert_eq!(neighbors.len(), 0);
            }
            if let Frame::Array(discovered) = &items[3] {
                assert_eq!(discovered.len(), 0);
            }
        } else {
            panic!("expected Array, got {:?}", resp);
        }
    }

    #[test]
    fn test_handle_traverse_with_nodes() {
        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"g"), 64_000, 1)
            .expect("ok");

        // Add nodes and edges via command dispatch.
        let n1 = crate::command::graph::dispatch_graph_command(
            &mut store,
            &make_cmd(&[b"GRAPH.ADDNODE", b"g", b"Person"]),
        );
        let n2 = crate::command::graph::dispatch_graph_command(
            &mut store,
            &make_cmd(&[b"GRAPH.ADDNODE", b"g", b"Person"]),
        );

        let id1 = if let Frame::Integer(id) = n1 { id as u64 } else { panic!("expected int") };
        let id2 = if let Frame::Integer(id) = n2 { id as u64 } else { panic!("expected int") };

        let id1_str = id1.to_string();
        let id2_str = id2.to_string();
        crate::command::graph::dispatch_graph_command(
            &mut store,
            &make_cmd(&[
                b"GRAPH.ADDEDGE",
                b"g",
                id1_str.as_bytes(),
                id2_str.as_bytes(),
                b"KNOWS",
            ]),
        );

        // Traverse from node 1: should find node 2.
        let resp = handle_graph_traverse(&store, b"g", &[id1], None, u64::MAX - 1);
        if let Frame::Array(items) = &resp {
            assert!(items.len() >= 4);
            // NEIGHBORS should have frames (edge + node = 2 frames).
            if let Frame::Array(neighbors) = &items[1] {
                assert_eq!(neighbors.len(), 2);
            } else {
                panic!("expected Array for neighbors");
            }
            // DISCOVERED should have 1 ID.
            if let Frame::Array(discovered) = &items[3] {
                assert_eq!(discovered.len(), 1);
                if let Frame::Integer(disc_id) = &discovered[0] {
                    assert_eq!(*disc_id as u64, id2);
                }
            } else {
                panic!("expected Array for discovered");
            }
        } else {
            panic!("expected Array, got {:?}", resp);
        }
    }

    #[test]
    fn test_parse_traverse_response() {
        // Build a mock response.
        let response = Frame::Array(
            vec![
                Frame::BulkString(Bytes::from_static(b"NEIGHBORS")),
                Frame::Array(vec![Frame::Integer(42)].into()),
                Frame::BulkString(Bytes::from_static(b"DISCOVERED")),
                Frame::Array(vec![Frame::Integer(100), Frame::Integer(200)].into()),
            ]
            .into(),
        );

        let result = parse_traverse_response(&response);
        assert!(result.is_some());
        let result = result.expect("should parse");
        assert_eq!(result.neighbor_ids, vec![100, 200]);
        assert_eq!(result.frames.len(), 1);
        assert!(!result.truncated);
    }

    #[test]
    fn test_parse_traverse_response_truncated() {
        let response = Frame::Array(
            vec![
                Frame::BulkString(Bytes::from_static(b"NEIGHBORS")),
                Frame::Array(vec![].into()),
                Frame::BulkString(Bytes::from_static(b"DISCOVERED")),
                Frame::Array(vec![].into()),
                Frame::BulkString(Bytes::from_static(b"TRUNCATED")),
            ]
            .into(),
        );

        let result = parse_traverse_response(&response).expect("should parse");
        assert!(result.truncated);
    }

    #[test]
    fn test_parse_traverse_response_invalid() {
        assert!(parse_traverse_response(&Frame::Null).is_none());
        assert!(parse_traverse_response(&Frame::Integer(42)).is_none());
        assert!(
            parse_traverse_response(&Frame::Array(vec![Frame::Integer(1)].into())).is_none()
        );
    }

    #[test]
    fn test_default_depth_limit() {
        assert_eq!(DEFAULT_CROSS_SHARD_DEPTH_LIMIT, 2);
    }

    #[test]
    fn test_handle_traverse_edge_type_filter() {
        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"g"), 64_000, 1)
            .expect("ok");

        let n1 = crate::command::graph::dispatch_graph_command(
            &mut store,
            &make_cmd(&[b"GRAPH.ADDNODE", b"g", b"Person"]),
        );
        let n2 = crate::command::graph::dispatch_graph_command(
            &mut store,
            &make_cmd(&[b"GRAPH.ADDNODE", b"g", b"Person"]),
        );

        let id1 = if let Frame::Integer(id) = n1 { id as u64 } else { panic!("expected int") };
        let id2 = if let Frame::Integer(id) = n2 { id as u64 } else { panic!("expected int") };

        let id1_str = id1.to_string();
        let id2_str = id2.to_string();
        crate::command::graph::dispatch_graph_command(
            &mut store,
            &make_cmd(&[
                b"GRAPH.ADDEDGE",
                b"g",
                id1_str.as_bytes(),
                id2_str.as_bytes(),
                b"KNOWS",
            ]),
        );

        // Filter by a type that doesn't match: should get empty results.
        let resp = handle_graph_traverse(&store, b"g", &[id1], Some(9999), u64::MAX - 1);
        if let Frame::Array(items) = &resp {
            if let Frame::Array(neighbors) = &items[1] {
                assert_eq!(neighbors.len(), 0, "non-matching filter should yield no neighbors");
            }
        } else {
            panic!("expected Array");
        }
    }
}
