//! Graph WAL replay -- two-pass replay for node-before-edge ordering.
//!
//! During WAL replay, GRAPH.* commands may appear in any order. Edges may reference
//! nodes that have not yet been inserted (because the WAL records appear out of order).
//! The `GraphReplayCollector` collects all graph commands during the first pass, then
//! replays them in correct order: creates first, then nodes, then edges, then deletes.

use bytes::Bytes;
use smallvec::SmallVec;

use crate::graph::memgraph::MemGraph;
use crate::graph::store::GraphStore;
use crate::graph::types::{PropertyMap, PropertyValue};

/// A collected graph command for deferred replay.
#[derive(Debug)]
#[allow(dead_code)] // Fields read during future edge-remove replay support
enum GraphCommand {
    /// GRAPH.CREATE <name>
    Create { name: Bytes },
    /// GRAPH.ADDNODE <name> <node_id> <labels> <props> <embedding>
    AddNode {
        graph_name: Bytes,
        node_id: u64,
        labels: SmallVec<[u16; 4]>,
        properties: PropertyMap,
        embedding: Option<Vec<f32>>,
    },
    /// GRAPH.ADDEDGE <name> <edge_id> <src_id> <dst_id> <edge_type> <weight> <props>
    AddEdge {
        graph_name: Bytes,
        edge_id: u64,
        src_id: u64,
        dst_id: u64,
        edge_type: u16,
        weight: f64,
        properties: Option<PropertyMap>,
    },
    /// GRAPH.REMOVENODE <name> <node_id>
    RemoveNode { graph_name: Bytes, node_id: u64 },
    /// GRAPH.REMOVEEDGE <name> <edge_id>
    RemoveEdge { graph_name: Bytes, edge_id: u64 },
    /// GRAPH.DROP <name>
    Drop { name: Bytes },
}

/// Collector for graph WAL commands. Accumulates commands during replay,
/// then applies them in correct order via `replay_into`.
pub struct GraphReplayCollector {
    commands: Vec<GraphCommand>,
}

impl GraphReplayCollector {
    pub fn new() -> Self {
        Self {
            commands: Vec::new(),
        }
    }

    /// Check if a command name is a graph command that should be collected.
    pub fn is_graph_command(cmd: &[u8]) -> bool {
        cmd.eq_ignore_ascii_case(b"GRAPH.CREATE")
            || cmd.eq_ignore_ascii_case(b"GRAPH.ADDNODE")
            || cmd.eq_ignore_ascii_case(b"GRAPH.ADDEDGE")
            || cmd.eq_ignore_ascii_case(b"GRAPH.REMOVENODE")
            || cmd.eq_ignore_ascii_case(b"GRAPH.REMOVEEDGE")
            || cmd.eq_ignore_ascii_case(b"GRAPH.DROP")
    }

    /// Collect a graph command from WAL replay args.
    ///
    /// `cmd` is the command name (e.g. b"GRAPH.ADDNODE").
    /// `args` are the remaining arguments as byte slices.
    ///
    /// Returns `true` if the command was recognized and collected.
    pub fn collect_command(&mut self, cmd: &[u8], args: &[&[u8]]) -> bool {
        let upper = cmd.to_ascii_uppercase();
        match upper.as_slice() {
            b"GRAPH.CREATE" => {
                if args.is_empty() {
                    return false;
                }
                self.commands.push(GraphCommand::Create {
                    name: Bytes::copy_from_slice(args[0]),
                });
                true
            }
            b"GRAPH.DROP" => {
                if args.is_empty() {
                    return false;
                }
                self.commands.push(GraphCommand::Drop {
                    name: Bytes::copy_from_slice(args[0]),
                });
                true
            }
            b"GRAPH.REMOVENODE" => {
                if args.len() < 2 {
                    return false;
                }
                let Some(node_id) = parse_u64(args[1]) else {
                    return false;
                };
                self.commands.push(GraphCommand::RemoveNode {
                    graph_name: Bytes::copy_from_slice(args[0]),
                    node_id,
                });
                true
            }
            b"GRAPH.REMOVEEDGE" => {
                if args.len() < 2 {
                    return false;
                }
                let Some(edge_id) = parse_u64(args[1]) else {
                    return false;
                };
                self.commands.push(GraphCommand::RemoveEdge {
                    graph_name: Bytes::copy_from_slice(args[0]),
                    edge_id,
                });
                true
            }
            b"GRAPH.ADDNODE" => {
                // Format: <graph> <node_id> <num_labels> [labels...] <num_props> [key type val...] [VECTOR dim bytes]
                if args.len() < 4 {
                    return false;
                }
                let graph_name = Bytes::copy_from_slice(args[0]);
                let Some(node_id) = parse_u64(args[1]) else {
                    return false;
                };
                let Some(num_labels) = parse_usize(args[2]) else {
                    return false;
                };

                let mut pos = 3;
                let mut labels = SmallVec::new();
                for _ in 0..num_labels {
                    if pos >= args.len() {
                        return false;
                    }
                    let Some(label) = parse_u16(args[pos]) else {
                        return false;
                    };
                    labels.push(label);
                    pos += 1;
                }

                if pos >= args.len() {
                    return false;
                }
                let Some(num_props) = parse_usize(args[pos]) else {
                    return false;
                };
                pos += 1;

                let mut properties: PropertyMap = SmallVec::new();
                for _ in 0..num_props {
                    if pos + 2 >= args.len() {
                        return false;
                    }
                    let Some(key) = parse_u16(args[pos]) else {
                        return false;
                    };
                    let type_tag = args[pos + 1];
                    let val_bytes = args[pos + 2];
                    let Some(val) = parse_property_value(type_tag, val_bytes) else {
                        return false;
                    };
                    properties.push((key, val));
                    pos += 3;
                }

                // Optional embedding: VECTOR <dim> <bytes>
                let mut embedding = None;
                if pos + 2 < args.len() && args[pos] == b"VECTOR" {
                    let Some(dim) = parse_usize(args[pos + 1]) else {
                        return false;
                    };
                    let embed_bytes = args[pos + 2];
                    if embed_bytes.len() == dim * 4 {
                        let mut vec = Vec::with_capacity(dim);
                        for chunk in embed_bytes.chunks_exact(4) {
                            vec.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
                        }
                        embedding = Some(vec);
                    }
                }

                self.commands.push(GraphCommand::AddNode {
                    graph_name,
                    node_id,
                    labels,
                    properties,
                    embedding,
                });
                true
            }
            b"GRAPH.ADDEDGE" => {
                // Format: <graph> <edge_id> <src_id> <dst_id> <edge_type> <weight> <num_props> [key type val...]
                if args.len() < 7 {
                    return false;
                }
                let graph_name = Bytes::copy_from_slice(args[0]);
                let Some(edge_id) = parse_u64(args[1]) else {
                    return false;
                };
                let Some(src_id) = parse_u64(args[2]) else {
                    return false;
                };
                let Some(dst_id) = parse_u64(args[3]) else {
                    return false;
                };
                let Some(edge_type) = parse_u16(args[4]) else {
                    return false;
                };
                let Some(weight) = parse_f64(args[5]) else {
                    return false;
                };
                let Some(num_props) = parse_usize(args[6]) else {
                    return false;
                };

                let mut pos = 7;
                let mut properties: PropertyMap = SmallVec::new();
                for _ in 0..num_props {
                    if pos + 2 >= args.len() {
                        return false;
                    }
                    let Some(key) = parse_u16(args[pos]) else {
                        return false;
                    };
                    let type_tag = args[pos + 1];
                    let val_bytes = args[pos + 2];
                    let Some(val) = parse_property_value(type_tag, val_bytes) else {
                        return false;
                    };
                    properties.push((key, val));
                    pos += 3;
                }

                self.commands.push(GraphCommand::AddEdge {
                    graph_name,
                    edge_id,
                    src_id,
                    dst_id,
                    edge_type,
                    weight,
                    properties: if properties.is_empty() {
                        None
                    } else {
                        Some(properties)
                    },
                });
                true
            }
            _ => false,
        }
    }

    /// Two-pass replay: creates -> nodes -> edges -> removes -> drops.
    ///
    /// This ensures that edges are inserted after their referenced nodes exist,
    /// even if the WAL recorded them in a different order.
    ///
    /// Returns the number of commands successfully replayed.
    pub fn replay_into(&self, store: &mut GraphStore) -> usize {
        let mut replayed = 0;
        self.replay_per_graph(store, &mut replayed);
        replayed
    }

    /// Internal: replay commands grouped by graph, building fresh MemGraphs.
    fn replay_per_graph(&self, store: &mut GraphStore, replayed: &mut usize) {
        use std::collections::HashMap;

        // Phase 1: Collect graph creates and process them
        for cmd in &self.commands {
            if let GraphCommand::Create { name } = cmd {
                if store.create_graph(name.clone(), 64_000, 0).is_ok() {
                    *replayed += 1;
                }
            }
        }

        // Phase 2: Collect per-graph command lists for batched MemGraph access.
        // This avoids take_memgraph/put_memgraph per-node/edge (O(N) atomic swaps).
        let mut graph_nodes: HashMap<Bytes, Vec<usize>> = HashMap::new();
        let mut graph_edges: HashMap<Bytes, Vec<usize>> = HashMap::new();
        let mut graph_removes: HashMap<Bytes, Vec<usize>> = HashMap::new();

        for (idx, cmd) in self.commands.iter().enumerate() {
            match cmd {
                GraphCommand::AddNode { graph_name, .. } => {
                    graph_nodes.entry(graph_name.clone()).or_default().push(idx);
                }
                GraphCommand::AddEdge { graph_name, .. } => {
                    graph_edges.entry(graph_name.clone()).or_default().push(idx);
                }
                GraphCommand::RemoveNode { graph_name, .. } => {
                    graph_removes
                        .entry(graph_name.clone())
                        .or_default()
                        .push(idx);
                }
                GraphCommand::RemoveEdge { .. } => {
                    tracing::warn!("GRAPH.REMOVEEDGE during WAL replay is not yet supported");
                }
                _ => {}
            }
        }

        // Collect all graph names that have any mutations.
        let mut all_graphs: std::collections::HashSet<Bytes> = std::collections::HashSet::new();
        for k in graph_nodes.keys() {
            all_graphs.insert(k.clone());
        }
        for k in graph_edges.keys() {
            all_graphs.insert(k.clone());
        }
        for k in graph_removes.keys() {
            all_graphs.insert(k.clone());
        }

        let mut node_maps: HashMap<Bytes, HashMap<u64, crate::graph::types::NodeKey>> =
            HashMap::new();

        // For each graph: take memgraph once, replay all nodes+edges+removes, put back once.
        for graph_name in &all_graphs {
            let Some(graph) = store.get_graph_mut(graph_name) else {
                continue;
            };
            let (mut mg, immutable) = take_memgraph(graph);

            // Insert nodes
            if let Some(node_indices) = graph_nodes.get(graph_name) {
                for &idx in node_indices {
                    if let GraphCommand::AddNode {
                        node_id,
                        labels,
                        properties,
                        embedding,
                        ..
                    } = &self.commands[idx]
                    {
                        let nk =
                            mg.add_node(labels.clone(), properties.clone(), embedding.clone(), 0);
                        node_maps
                            .entry(graph_name.clone())
                            .or_default()
                            .insert(*node_id, nk);
                        *replayed += 1;
                    }
                }
            }

            // Insert edges (nodes already in memgraph)
            if let Some(edge_indices) = graph_edges.get(graph_name) {
                for &idx in edge_indices {
                    if let GraphCommand::AddEdge {
                        src_id,
                        dst_id,
                        edge_type,
                        weight,
                        properties,
                        ..
                    } = &self.commands[idx]
                    {
                        let node_map = node_maps.get(graph_name);
                        let src_key = node_map.and_then(|m| m.get(src_id)).copied();
                        let dst_key = node_map.and_then(|m| m.get(dst_id)).copied();
                        if let (Some(src), Some(dst)) = (src_key, dst_key) {
                            if mg
                                .add_edge(src, dst, *edge_type, *weight, properties.clone(), 0)
                                .is_ok()
                            {
                                *replayed += 1;
                            }
                        }
                    }
                }
            }

            // Process removes
            if let Some(remove_indices) = graph_removes.get(graph_name) {
                for &idx in remove_indices {
                    if let GraphCommand::RemoveNode { node_id, .. } = &self.commands[idx] {
                        let node_map = node_maps.get(graph_name);
                        if let Some(nk) = node_map.and_then(|m| m.get(node_id)).copied() {
                            if mg.remove_node(nk, 0) {
                                *replayed += 1;
                            }
                        }
                    }
                }
            }

            put_memgraph(graph, mg, immutable);
        }

        // Phase 4: Process graph drops
        for cmd in &self.commands {
            if let GraphCommand::Drop { name } = cmd {
                if store.drop_graph(name).is_ok() {
                    *replayed += 1;
                }
            }
        }
    }

    /// Number of collected commands.
    pub fn command_count(&self) -> usize {
        self.commands.len()
    }
}

/// Take the MemGraph out of the segment holder for mutation during replay.
/// Swaps in `None` for the mutable segment, returning the owned MemGraph.
fn take_memgraph(
    graph: &mut crate::graph::store::NamedGraph,
) -> (MemGraph, Vec<std::sync::Arc<crate::graph::csr::CsrSegment>>) {
    use crate::graph::segment::GraphSegmentList;

    // Clone the immutable list and extract the mutable Arc
    let old_list = graph.segments.load().as_ref().clone();
    let immutable = old_list.immutable;

    // Swap in a list with no mutable segment to release the ArcSwap's reference
    graph.segments.swap(GraphSegmentList {
        mutable: None,
        immutable: immutable.clone(),
    });

    // Now the only remaining Arc reference is in old_list.mutable
    let mg = match old_list.mutable {
        Some(arc_mg) => match std::sync::Arc::try_unwrap(arc_mg) {
            Ok(mg) => mg,
            Err(_arc) => {
                // Fallback: create fresh MemGraph (shouldn't happen at startup)
                tracing::warn!("MemGraph Arc has extra owners during replay, creating fresh");
                MemGraph::new(graph.edge_threshold)
            }
        },
        None => MemGraph::new(graph.edge_threshold),
    };

    (mg, immutable)
}

/// Put the MemGraph back into the segment holder after mutation.
fn put_memgraph(
    graph: &mut crate::graph::store::NamedGraph,
    mg: MemGraph,
    immutable: Vec<std::sync::Arc<crate::graph::csr::CsrSegment>>,
) {
    use crate::graph::segment::GraphSegmentList;
    graph.segments.swap(GraphSegmentList {
        mutable: Some(std::sync::Arc::new(mg)),
        immutable,
    });
}

// --- Parsing helpers ---

fn parse_u64(data: &[u8]) -> Option<u64> {
    core::str::from_utf8(data).ok()?.parse().ok()
}

fn parse_usize(data: &[u8]) -> Option<usize> {
    core::str::from_utf8(data).ok()?.parse().ok()
}

fn parse_u16(data: &[u8]) -> Option<u16> {
    core::str::from_utf8(data).ok()?.parse().ok()
}

fn parse_f64(data: &[u8]) -> Option<f64> {
    core::str::from_utf8(data).ok()?.parse().ok()
}

fn parse_property_value(type_tag: &[u8], val_bytes: &[u8]) -> Option<PropertyValue> {
    match type_tag {
        b"i" => {
            let i: i64 = core::str::from_utf8(val_bytes).ok()?.parse().ok()?;
            Some(PropertyValue::Int(i))
        }
        b"f" => {
            let f: f64 = core::str::from_utf8(val_bytes).ok()?.parse().ok()?;
            Some(PropertyValue::Float(f))
        }
        b"s" => Some(PropertyValue::String(Bytes::copy_from_slice(val_bytes))),
        b"b" => {
            let b = match val_bytes {
                b"1" => true,
                b"0" => false,
                _ => return None,
            };
            Some(PropertyValue::Bool(b))
        }
        b"x" => Some(PropertyValue::Bytes(Bytes::copy_from_slice(val_bytes))),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_graph_command() {
        assert!(GraphReplayCollector::is_graph_command(b"GRAPH.CREATE"));
        assert!(GraphReplayCollector::is_graph_command(b"graph.create"));
        assert!(GraphReplayCollector::is_graph_command(b"GRAPH.ADDNODE"));
        assert!(GraphReplayCollector::is_graph_command(b"GRAPH.ADDEDGE"));
        assert!(GraphReplayCollector::is_graph_command(b"GRAPH.REMOVENODE"));
        assert!(GraphReplayCollector::is_graph_command(b"GRAPH.REMOVEEDGE"));
        assert!(GraphReplayCollector::is_graph_command(b"GRAPH.DROP"));
        assert!(!GraphReplayCollector::is_graph_command(b"SET"));
        assert!(!GraphReplayCollector::is_graph_command(b"GET"));
    }

    #[test]
    fn test_collect_create() {
        let mut collector = GraphReplayCollector::new();
        assert!(collector.collect_command(b"GRAPH.CREATE", &[b"social"]));
        assert_eq!(collector.command_count(), 1);
    }

    #[test]
    fn test_collect_drop() {
        let mut collector = GraphReplayCollector::new();
        assert!(collector.collect_command(b"GRAPH.DROP", &[b"social"]));
        assert_eq!(collector.command_count(), 1);
    }

    #[test]
    fn test_collect_addnode_minimal() {
        let mut collector = GraphReplayCollector::new();
        // GRAPH.ADDNODE social 100 1 0 0
        // graph_name=social, node_id=100, num_labels=1, label=0, num_props=0
        let args: Vec<&[u8]> = vec![b"social", b"100", b"1", b"0", b"0"];
        assert!(collector.collect_command(b"GRAPH.ADDNODE", &args));
        assert_eq!(collector.command_count(), 1);
    }

    #[test]
    fn test_collect_addedge() {
        let mut collector = GraphReplayCollector::new();
        // GRAPH.ADDEDGE social 50 10 20 3 1.5 0
        let args: Vec<&[u8]> = vec![b"social", b"50", b"10", b"20", b"3", b"1.5", b"0"];
        assert!(collector.collect_command(b"GRAPH.ADDEDGE", &args));
        assert_eq!(collector.command_count(), 1);
    }

    #[test]
    fn test_collect_removenode() {
        let mut collector = GraphReplayCollector::new();
        assert!(collector.collect_command(b"GRAPH.REMOVENODE", &[b"social", b"42"]));
        assert_eq!(collector.command_count(), 1);
    }

    #[test]
    fn test_collect_invalid_returns_false() {
        let mut collector = GraphReplayCollector::new();
        // Missing args
        assert!(!collector.collect_command(b"GRAPH.CREATE", &[]));
        assert!(!collector.collect_command(b"GRAPH.ADDNODE", &[b"g"]));
        assert!(!collector.collect_command(b"GRAPH.ADDEDGE", &[b"g", b"1"]));
        assert_eq!(collector.command_count(), 0);
    }

    #[test]
    fn test_two_pass_replay_order() {
        let mut collector = GraphReplayCollector::new();

        // Collect edge BEFORE node (simulate out-of-order WAL)
        let edge_args: Vec<&[u8]> = vec![b"social", b"50", b"100", b"200", b"0", b"1.0", b"0"];
        collector.collect_command(b"GRAPH.ADDEDGE", &edge_args);

        // Collect nodes
        let node1_args: Vec<&[u8]> = vec![b"social", b"100", b"1", b"0", b"0"];
        collector.collect_command(b"GRAPH.ADDNODE", &node1_args);
        let node2_args: Vec<&[u8]> = vec![b"social", b"200", b"1", b"0", b"0"];
        collector.collect_command(b"GRAPH.ADDNODE", &node2_args);

        // Collect graph create AFTER nodes (also out of order)
        collector.collect_command(b"GRAPH.CREATE", &[b"social"]);

        assert_eq!(collector.command_count(), 4);

        // Replay should handle the ordering correctly
        let mut store = GraphStore::new();
        let replayed = collector.replay_into(&mut store);

        // Should have replayed: create(1) + 2 nodes + 1 edge = 4
        assert_eq!(replayed, 4);

        let graph = store.get_graph(b"social").expect("graph should exist");
        let segments = graph.segments.load();
        let mg = segments.mutable.as_ref().expect("mutable exists");
        assert_eq!(mg.node_count(), 2);
        assert_eq!(mg.edge_count(), 1);
    }

    #[test]
    fn test_replay_drop_after_create() {
        let mut collector = GraphReplayCollector::new();
        collector.collect_command(b"GRAPH.CREATE", &[b"temp"]);
        collector.collect_command(b"GRAPH.DROP", &[b"temp"]);

        let mut store = GraphStore::new();
        let replayed = collector.replay_into(&mut store);

        // Create + Drop = 2 replayed
        assert_eq!(replayed, 2);
        assert!(store.get_graph(b"temp").is_none());
    }

    #[test]
    fn test_parse_property_value() {
        assert_eq!(
            parse_property_value(b"i", b"42"),
            Some(PropertyValue::Int(42))
        );
        assert_eq!(
            parse_property_value(b"f", b"3.14"),
            Some(PropertyValue::Float(3.14))
        );
        assert_eq!(
            parse_property_value(b"s", b"hello"),
            Some(PropertyValue::String(Bytes::from_static(b"hello")))
        );
        assert_eq!(
            parse_property_value(b"b", b"1"),
            Some(PropertyValue::Bool(true))
        );
        assert_eq!(
            parse_property_value(b"b", b"0"),
            Some(PropertyValue::Bool(false))
        );
        assert_eq!(
            parse_property_value(b"x", b"\x00\x01"),
            Some(PropertyValue::Bytes(Bytes::from_static(b"\x00\x01")))
        );
        assert_eq!(parse_property_value(b"z", b"unknown"), None);
    }
}
