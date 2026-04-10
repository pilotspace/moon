//! GRAPH.* command handlers.
//!
//! These commands operate on GraphStore, not Database, so they are NOT
//! dispatched through the standard command::dispatch() function.
//! Instead, the shard event loop intercepts GRAPH.* commands and calls
//! these handlers directly with the per-shard GraphStore.

pub mod graph_read;
pub mod graph_write;

pub use graph_read::{graph_info, graph_list, graph_neighbors, graph_query, graph_ro_query, graph_explain};
pub use graph_write::{graph_addedge, graph_addnode, graph_create, graph_delete};

use bytes::Bytes;

use crate::graph::store::GraphStore;
use crate::protocol::Frame;

/// Dispatch a GRAPH.* command to the appropriate handler.
///
/// Called by the shard event loop (spsc_handler) for cross-shard messages
/// and by connection handlers for local-shard execution.
///
/// Returns a Frame response.
pub fn dispatch_graph_command(store: &mut GraphStore, command: &Frame) -> Frame {
    let (cmd, args) = match extract_command(command) {
        Some(pair) => pair,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR invalid command format for GRAPH.*",
            ));
        }
    };

    if cmd.eq_ignore_ascii_case(b"GRAPH.CREATE") {
        graph_create(store, args)
    } else if cmd.eq_ignore_ascii_case(b"GRAPH.ADDNODE") {
        graph_addnode(store, args)
    } else if cmd.eq_ignore_ascii_case(b"GRAPH.ADDEDGE") {
        graph_addedge(store, args)
    } else if cmd.eq_ignore_ascii_case(b"GRAPH.NEIGHBORS") {
        graph_neighbors(store, args)
    } else if cmd.eq_ignore_ascii_case(b"GRAPH.INFO") {
        graph_info(store, args)
    } else if cmd.eq_ignore_ascii_case(b"GRAPH.LIST") {
        graph_list(store)
    } else if cmd.eq_ignore_ascii_case(b"GRAPH.DELETE") {
        graph_delete(store, args)
    } else if cmd.eq_ignore_ascii_case(b"GRAPH.QUERY") {
        graph_query(store, args)
    } else if cmd.eq_ignore_ascii_case(b"GRAPH.RO_QUERY") {
        graph_ro_query(store, args)
    } else if cmd.eq_ignore_ascii_case(b"GRAPH.EXPLAIN") {
        graph_explain(args)
    } else if cmd.eq_ignore_ascii_case(b"GRAPH.VSEARCH")
        || cmd.eq_ignore_ascii_case(b"GRAPH.HYBRID")
    {
        Frame::Error(Bytes::from_static(b"ERR Hybrid queries not yet available"))
    } else {
        Frame::Error(Bytes::from_static(b"ERR unknown GRAPH.* command"))
    }
}

/// Dispatch a GRAPH.* command given pre-extracted cmd and args.
///
/// Used by connection handlers that have already extracted the command name.
pub fn dispatch_graph_cmd_args(store: &mut GraphStore, cmd: &[u8], args: &[Frame]) -> Frame {
    if cmd.eq_ignore_ascii_case(b"GRAPH.CREATE") {
        graph_create(store, args)
    } else if cmd.eq_ignore_ascii_case(b"GRAPH.ADDNODE") {
        graph_addnode(store, args)
    } else if cmd.eq_ignore_ascii_case(b"GRAPH.ADDEDGE") {
        graph_addedge(store, args)
    } else if cmd.eq_ignore_ascii_case(b"GRAPH.NEIGHBORS") {
        graph_neighbors(store, args)
    } else if cmd.eq_ignore_ascii_case(b"GRAPH.INFO") {
        graph_info(store, args)
    } else if cmd.eq_ignore_ascii_case(b"GRAPH.LIST") {
        graph_list(store)
    } else if cmd.eq_ignore_ascii_case(b"GRAPH.DELETE") {
        graph_delete(store, args)
    } else if cmd.eq_ignore_ascii_case(b"GRAPH.QUERY") {
        graph_query(store, args)
    } else if cmd.eq_ignore_ascii_case(b"GRAPH.RO_QUERY") {
        graph_ro_query(store, args)
    } else if cmd.eq_ignore_ascii_case(b"GRAPH.EXPLAIN") {
        graph_explain(args)
    } else if cmd.eq_ignore_ascii_case(b"GRAPH.VSEARCH")
        || cmd.eq_ignore_ascii_case(b"GRAPH.HYBRID")
    {
        Frame::Error(Bytes::from_static(b"ERR Hybrid queries not yet available"))
    } else {
        Frame::Error(Bytes::from_static(b"ERR unknown GRAPH.* command"))
    }
}

/// Extract command name and arguments from a Frame::Array.
fn extract_command(frame: &Frame) -> Option<(&[u8], &[Frame])> {
    let items = match frame {
        Frame::Array(items) => items,
        _ => return None,
    };
    if items.is_empty() {
        return None;
    }
    let cmd = match &items[0] {
        Frame::BulkString(b) => b.as_ref(),
        Frame::SimpleString(b) => b.as_ref(),
        _ => return None,
    };
    Some((cmd, &items[1..]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::FrameVec;

    fn make_cmd(parts: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = parts
            .iter()
            .map(|p| Frame::BulkString(Bytes::from(p.to_vec())))
            .collect();
        Frame::Array(FrameVec::from_vec(frames))
    }

    #[test]
    fn test_graph_create_and_list() {
        let mut store = GraphStore::new();

        let resp = dispatch_graph_command(&mut store, &make_cmd(&[b"GRAPH.CREATE", b"social"]));
        assert!(matches!(resp, Frame::SimpleString(_)));

        let resp = dispatch_graph_command(&mut store, &make_cmd(&[b"GRAPH.LIST"]));
        if let Frame::Array(items) = &resp {
            assert_eq!(items.len(), 1);
        } else {
            panic!("expected Array, got {:?}", resp);
        }
    }

    #[test]
    fn test_graph_create_duplicate_error() {
        let mut store = GraphStore::new();
        dispatch_graph_command(&mut store, &make_cmd(&[b"GRAPH.CREATE", b"g"]));
        let resp = dispatch_graph_command(&mut store, &make_cmd(&[b"GRAPH.CREATE", b"g"]));
        assert!(matches!(resp, Frame::Error(_)));
    }

    #[test]
    fn test_graph_addnode_and_info() {
        let mut store = GraphStore::new();
        dispatch_graph_command(&mut store, &make_cmd(&[b"GRAPH.CREATE", b"g"]));

        let resp = dispatch_graph_command(
            &mut store,
            &make_cmd(&[b"GRAPH.ADDNODE", b"g", b"Person", b"name", b"Alice"]),
        );
        // Should return a node ID.
        assert!(matches!(resp, Frame::Integer(_)));

        let resp = dispatch_graph_command(&mut store, &make_cmd(&[b"GRAPH.INFO", b"g"]));
        if let Frame::Map(pairs) = &resp {
            // node_count should be 1
            let node_count = pairs.iter().find(|(k, _)| {
                matches!(k, Frame::SimpleString(b) if b.as_ref() == b"node_count")
            });
            assert_eq!(
                node_count.map(|(_, v)| v),
                Some(&Frame::Integer(1))
            );
        } else {
            panic!("expected Map, got {:?}", resp);
        }
    }

    #[test]
    fn test_graph_addedge() {
        let mut store = GraphStore::new();
        dispatch_graph_command(&mut store, &make_cmd(&[b"GRAPH.CREATE", b"g"]));

        let n1 = dispatch_graph_command(
            &mut store,
            &make_cmd(&[b"GRAPH.ADDNODE", b"g", b"Person"]),
        );
        let n2 = dispatch_graph_command(
            &mut store,
            &make_cmd(&[b"GRAPH.ADDNODE", b"g", b"Person"]),
        );

        let id1 = if let Frame::Integer(id) = n1 { id } else { panic!("expected int") };
        let id2 = if let Frame::Integer(id) = n2 { id } else { panic!("expected int") };

        let id1_str = id1.to_string();
        let id2_str = id2.to_string();
        let resp = dispatch_graph_command(
            &mut store,
            &make_cmd(&[b"GRAPH.ADDEDGE", b"g", id1_str.as_bytes(), id2_str.as_bytes(), b"KNOWS"]),
        );
        assert!(matches!(resp, Frame::Integer(_)));
    }

    #[test]
    fn test_graph_neighbors() {
        let mut store = GraphStore::new();
        dispatch_graph_command(&mut store, &make_cmd(&[b"GRAPH.CREATE", b"g"]));

        let n1 = dispatch_graph_command(
            &mut store,
            &make_cmd(&[b"GRAPH.ADDNODE", b"g", b"Person", b"name", b"Alice"]),
        );
        let n2 = dispatch_graph_command(
            &mut store,
            &make_cmd(&[b"GRAPH.ADDNODE", b"g", b"Person", b"name", b"Bob"]),
        );

        let id1 = if let Frame::Integer(id) = n1 { id } else { panic!("expected int") };
        let id2 = if let Frame::Integer(id) = n2 { id } else { panic!("expected int") };

        let id1_str = id1.to_string();
        let id2_str = id2.to_string();
        dispatch_graph_command(
            &mut store,
            &make_cmd(&[b"GRAPH.ADDEDGE", b"g", id1_str.as_bytes(), id2_str.as_bytes(), b"KNOWS"]),
        );

        // Get neighbors of node 1.
        let resp = dispatch_graph_command(
            &mut store,
            &make_cmd(&[b"GRAPH.NEIGHBORS", b"g", id1_str.as_bytes()]),
        );
        if let Frame::Array(items) = &resp {
            // Should have edge + node = 2 items.
            assert_eq!(items.len(), 2);
        } else {
            panic!("expected Array, got {:?}", resp);
        }
    }

    #[test]
    fn test_graph_delete() {
        let mut store = GraphStore::new();
        dispatch_graph_command(&mut store, &make_cmd(&[b"GRAPH.CREATE", b"g"]));
        let resp = dispatch_graph_command(&mut store, &make_cmd(&[b"GRAPH.DELETE", b"g"]));
        assert!(matches!(resp, Frame::SimpleString(_)));

        let resp = dispatch_graph_command(&mut store, &make_cmd(&[b"GRAPH.INFO", b"g"]));
        assert!(matches!(resp, Frame::Error(_)));
    }

    #[test]
    fn test_graph_query_no_graph() {
        let mut store = GraphStore::new();
        let resp = dispatch_graph_command(&mut store, &make_cmd(&[b"GRAPH.QUERY", b"g", b"MATCH (n) RETURN n"]));
        if let Frame::Error(msg) = &resp {
            assert!(msg.as_ref().starts_with(b"ERR graph not found"));
        } else {
            panic!("expected error, got {:?}", resp);
        }
    }

    #[test]
    fn test_graph_query_parse_and_plan() {
        let mut store = GraphStore::new();
        dispatch_graph_command(&mut store, &make_cmd(&[b"GRAPH.CREATE", b"g"]));
        let resp = dispatch_graph_command(&mut store, &make_cmd(&[b"GRAPH.QUERY", b"g", b"MATCH (n:Person) RETURN n"]));
        // Should return an array of plan operators (not an error).
        assert!(matches!(resp, Frame::Array(_)), "expected Array, got {:?}", resp);
    }

    #[test]
    fn test_graph_ro_query_rejects_writes() {
        let mut store = GraphStore::new();
        dispatch_graph_command(&mut store, &make_cmd(&[b"GRAPH.CREATE", b"g"]));
        let resp = dispatch_graph_command(&mut store, &make_cmd(&[b"GRAPH.RO_QUERY", b"g", b"CREATE (n:Person)"]));
        if let Frame::Error(msg) = &resp {
            assert!(msg.as_ref().starts_with(b"ERR GRAPH.RO_QUERY"));
        } else {
            panic!("expected error for write in RO_QUERY");
        }
    }

    #[test]
    fn test_graph_explain() {
        let mut store = GraphStore::new();
        let resp = dispatch_graph_command(&mut store, &make_cmd(&[b"GRAPH.EXPLAIN", b"g", b"MATCH (n) RETURN n"]));
        // EXPLAIN returns a BulkString with the plan.
        assert!(matches!(resp, Frame::BulkString(_)), "expected BulkString, got {:?}", resp);
    }

    #[test]
    fn test_graph_stub_commands() {
        let mut store = GraphStore::new();
        let resp = dispatch_graph_command(&mut store, &make_cmd(&[b"GRAPH.VSEARCH", b"g"]));
        if let Frame::Error(msg) = &resp {
            assert!(msg.as_ref().starts_with(b"ERR Hybrid"));
        } else {
            panic!("expected error");
        }
    }

    #[test]
    fn test_graph_neighbors_depth_2() {
        let mut store = GraphStore::new();
        dispatch_graph_command(&mut store, &make_cmd(&[b"GRAPH.CREATE", b"g"]));

        // Create chain: A -> B -> C
        let a = if let Frame::Integer(id) = dispatch_graph_command(
            &mut store,
            &make_cmd(&[b"GRAPH.ADDNODE", b"g", b"Person"]),
        ) { id } else { panic!() };
        let b = if let Frame::Integer(id) = dispatch_graph_command(
            &mut store,
            &make_cmd(&[b"GRAPH.ADDNODE", b"g", b"Person"]),
        ) { id } else { panic!() };
        let c = if let Frame::Integer(id) = dispatch_graph_command(
            &mut store,
            &make_cmd(&[b"GRAPH.ADDNODE", b"g", b"Person"]),
        ) { id } else { panic!() };

        let a_s = a.to_string();
        let b_s = b.to_string();
        let c_s = c.to_string();
        dispatch_graph_command(
            &mut store,
            &make_cmd(&[b"GRAPH.ADDEDGE", b"g", a_s.as_bytes(), b_s.as_bytes(), b"KNOWS"]),
        );
        dispatch_graph_command(
            &mut store,
            &make_cmd(&[b"GRAPH.ADDEDGE", b"g", b_s.as_bytes(), c_s.as_bytes(), b"KNOWS"]),
        );

        // Depth 1 from A: should see B only (edge + node).
        let resp = dispatch_graph_command(
            &mut store,
            &make_cmd(&[b"GRAPH.NEIGHBORS", b"g", a_s.as_bytes(), b"DEPTH", b"1"]),
        );
        if let Frame::Array(items) = &resp {
            assert_eq!(items.len(), 2); // edge + node B
        } else {
            panic!("expected Array");
        }

        // Depth 2 from A: should see B and C (4 items: edge+B, edge+C).
        let resp = dispatch_graph_command(
            &mut store,
            &make_cmd(&[b"GRAPH.NEIGHBORS", b"g", a_s.as_bytes(), b"DEPTH", b"2"]),
        );
        if let Frame::Array(items) = &resp {
            assert_eq!(items.len(), 4); // edge+B, edge+C
        } else {
            panic!("expected Array");
        }
    }
}
