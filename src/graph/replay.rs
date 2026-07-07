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
    /// GRAPH.SETPROP <name> <N|E> <entity_id> <key> <type> <val> (W2-9:
    /// Cypher SET durability — replayed after inserts, before removes).
    SetProp {
        graph_name: Bytes,
        entity_id: u64,
        is_node: bool,
        key: u16,
        value: PropertyValue,
    },
    /// GRAPH.SETLABEL <name> <node_id> <label> (W2-9: `SET n:Label`).
    SetLabel {
        graph_name: Bytes,
        node_id: u64,
        label: u16,
    },
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
            || cmd.eq_ignore_ascii_case(b"GRAPH.SETPROP")
            || cmd.eq_ignore_ascii_case(b"GRAPH.SETLABEL")
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
            b"GRAPH.SETPROP" => {
                // Format: <graph> <N|E> <entity_id> <key> <type> <val>
                if args.len() < 6 {
                    return false;
                }
                let is_node = match args[1] {
                    b"N" => true,
                    b"E" => false,
                    _ => return false,
                };
                let Some(entity_id) = parse_u64(args[2]) else {
                    return false;
                };
                let Some(key) = parse_u16(args[3]) else {
                    return false;
                };
                let Some(value) = parse_property_value(args[4], args[5]) else {
                    return false;
                };
                self.commands.push(GraphCommand::SetProp {
                    graph_name: Bytes::copy_from_slice(args[0]),
                    entity_id,
                    is_node,
                    key,
                    value,
                });
                true
            }
            b"GRAPH.SETLABEL" => {
                // Format: <graph> <node_id> <label>
                if args.len() < 3 {
                    return false;
                }
                let Some(node_id) = parse_u64(args[1]) else {
                    return false;
                };
                let Some(label) = parse_u16(args[2]) else {
                    return false;
                };
                self.commands.push(GraphCommand::SetLabel {
                    graph_name: Bytes::copy_from_slice(args[0]),
                    node_id,
                    label,
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
                    // Reject unreasonably large dimensions to prevent DoS from malformed WAL.
                    if dim > 65536 {
                        return false;
                    }
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

    /// Epoch-aware replay: respects temporal ordering of create/drop boundaries.
    ///
    /// Commands are split into "epochs" per graph. An epoch starts at a Create
    /// and ends at a Drop. Within each epoch, nodes are inserted before edges
    /// (handling out-of-order WAL records). Across epochs, operations are
    /// processed in WAL order, so create→drop→recreate sequences replay correctly.
    ///
    /// Returns the number of commands successfully replayed.
    pub fn replay_into(&self, store: &mut GraphStore) -> usize {
        let mut replayed = 0;
        self.replay_epoch_aware(store, &mut replayed);
        replayed
    }

    /// Internal: epoch-aware replay that processes commands in WAL order.
    ///
    /// An "epoch" is a contiguous sequence of mutations between a Create and
    /// the next Drop (or end-of-WAL). Within each epoch:
    ///
    ///   1. Create the graph
    ///   2. Insert all nodes (so edges can reference them)
    ///   3. Insert all edges
    ///   4. Process removes (nodes, edges)
    ///
    /// Then if a Drop follows, drop the graph before starting the next epoch.
    fn replay_epoch_aware(&self, store: &mut GraphStore, replayed: &mut usize) {
        use std::collections::HashMap;

        // Group commands into epochs. Each epoch is bounded by Create..Drop.
        // epoch_key = (graph_name, epoch_index).
        struct Epoch {
            graph_name: Bytes,
            create_idx: Option<usize>,
            node_indices: Vec<usize>,
            edge_indices: Vec<usize>,
            /// SETPROP + SETLABEL in WAL order (W2-9) — applied after
            /// inserts (targets must exist) and before removes (a later
            /// DELETE tombstones the set like the live execution did).
            set_indices: Vec<usize>,
            remove_node_indices: Vec<usize>,
            remove_edge_indices: Vec<usize>,
            drop_idx: Option<usize>,
        }

        fn new_epoch(
            graph_name: Bytes,
            create_idx: Option<usize>,
            drop_idx: Option<usize>,
        ) -> Epoch {
            Epoch {
                graph_name,
                create_idx,
                node_indices: Vec::new(),
                edge_indices: Vec::new(),
                set_indices: Vec::new(),
                remove_node_indices: Vec::new(),
                remove_edge_indices: Vec::new(),
                drop_idx,
            }
        }

        // Track current open epoch per graph name.
        let mut current_epoch: HashMap<Bytes, usize> = HashMap::new();
        let mut epochs: Vec<Epoch> = Vec::new();

        for (idx, cmd) in self.commands.iter().enumerate() {
            match cmd {
                GraphCommand::Create { name } => {
                    // If there's already an open epoch for this graph (mutations
                    // arrived before the Create in WAL order), attach the Create
                    // to the existing epoch instead of starting a new one.
                    if let Some(&existing_eidx) = current_epoch.get(name) {
                        if epochs[existing_eidx].create_idx.is_none() {
                            epochs[existing_eidx].create_idx = Some(idx);
                            // Keep current_epoch pointing to the same epoch.
                        } else {
                            // Previous epoch already has a Create — start a new one.
                            let epoch_idx = epochs.len();
                            epochs.push(new_epoch(name.clone(), Some(idx), None));
                            current_epoch.insert(name.clone(), epoch_idx);
                        }
                    } else {
                        let epoch_idx = epochs.len();
                        epochs.push(new_epoch(name.clone(), Some(idx), None));
                        current_epoch.insert(name.clone(), epoch_idx);
                    }
                }
                GraphCommand::Drop { name } => {
                    if let Some(&eidx) = current_epoch.get(name) {
                        epochs[eidx].drop_idx = Some(idx);
                        current_epoch.remove(name);
                    } else {
                        // Drop without a preceding Create in this WAL.
                        // Still record it so the graph gets dropped.
                        let epoch_idx = epochs.len();
                        epochs.push(new_epoch(name.clone(), None, Some(idx)));
                        // Don't insert into current_epoch — it's immediately closed.
                        let _ = epoch_idx;
                    }
                }
                GraphCommand::AddNode { graph_name, .. } => {
                    let eidx = current_epoch.entry(graph_name.clone()).or_insert_with(|| {
                        let i = epochs.len();
                        epochs.push(new_epoch(graph_name.clone(), None, None));
                        i
                    });
                    epochs[*eidx].node_indices.push(idx);
                }
                GraphCommand::AddEdge { graph_name, .. } => {
                    let eidx = current_epoch.entry(graph_name.clone()).or_insert_with(|| {
                        let i = epochs.len();
                        epochs.push(new_epoch(graph_name.clone(), None, None));
                        i
                    });
                    epochs[*eidx].edge_indices.push(idx);
                }
                GraphCommand::SetProp { graph_name, .. }
                | GraphCommand::SetLabel { graph_name, .. } => {
                    let eidx = current_epoch.entry(graph_name.clone()).or_insert_with(|| {
                        let i = epochs.len();
                        epochs.push(new_epoch(graph_name.clone(), None, None));
                        i
                    });
                    epochs[*eidx].set_indices.push(idx);
                }
                GraphCommand::RemoveNode { graph_name, .. } => {
                    let eidx = current_epoch.entry(graph_name.clone()).or_insert_with(|| {
                        let i = epochs.len();
                        epochs.push(new_epoch(graph_name.clone(), None, None));
                        i
                    });
                    epochs[*eidx].remove_node_indices.push(idx);
                }
                GraphCommand::RemoveEdge { graph_name, .. } => {
                    let eidx = current_epoch.entry(graph_name.clone()).or_insert_with(|| {
                        let i = epochs.len();
                        epochs.push(new_epoch(graph_name.clone(), None, None));
                        i
                    });
                    epochs[*eidx].remove_edge_indices.push(idx);
                }
            }
        }

        // Replay epochs in order. This preserves temporal ordering across
        // create/drop boundaries while ensuring nodes-before-edges within epochs.
        for epoch in &epochs {
            // 1. Create graph if this epoch has a Create command.
            if epoch.create_idx.is_some() {
                if store
                    .create_graph(epoch.graph_name.clone(), 64_000, 0)
                    .is_ok()
                {
                    *replayed += 1;
                }
            }

            // 2. Replay mutations (nodes → edges → removes) if graph exists.
            if !epoch.node_indices.is_empty()
                || !epoch.edge_indices.is_empty()
                || !epoch.set_indices.is_empty()
                || !epoch.remove_node_indices.is_empty()
                || !epoch.remove_edge_indices.is_empty()
            {
                let Some(graph) = store.get_graph_mut(&epoch.graph_name) else {
                    continue;
                };
                let (mut mg, immutable) = take_memgraph(graph);

                // Seed node_maps from immutable CSR segments so edges referencing
                // CSR-resident nodes (loaded during recovery) can be resolved.
                // Also raise the id-allocation floor past every frozen
                // external_id so fresh post-replay inserts can never alias
                // a frozen row.
                let mut node_map: HashMap<u64, crate::graph::types::NodeKey> = HashMap::new();
                for csr_seg in &immutable {
                    for nm in csr_seg.node_meta() {
                        let key_data = slotmap::KeyData::from_ffi(nm.external_id);
                        let node_key = crate::graph::types::NodeKey::from(key_data);
                        node_map.insert(nm.external_id, node_key);
                        mg.ensure_node_id_floor(nm.external_id);
                    }
                }

                // Insert nodes.
                // Precompute _key property ID for graph expansion mapping.
                let key_prop_id = crate::command::graph::graph_write::label_to_id(b"_key");
                let mut key_registrations: Vec<(Bytes, crate::graph::types::NodeKey)> = Vec::new();
                for &idx in &epoch.node_indices {
                    if let GraphCommand::AddNode {
                        node_id,
                        labels,
                        properties,
                        embedding,
                        ..
                    } = &self.commands[idx]
                    {
                        // Re-materialize under the ORIGINAL logged id so
                        // client-cached node handles survive a restart.
                        let nk = mg.add_node_with_id(
                            *node_id,
                            labels.clone(),
                            properties.clone(),
                            embedding.clone(),
                            0,
                        );
                        node_map.insert(*node_id, nk);
                        // Track _key properties for registration after memgraph is returned.
                        for (prop_id, prop_val) in properties {
                            if *prop_id == key_prop_id {
                                if let PropertyValue::String(s) = prop_val {
                                    key_registrations.push((s.clone(), nk));
                                }
                            }
                        }
                        *replayed += 1;
                    }
                }

                // Insert edges.
                for &idx in &epoch.edge_indices {
                    if let GraphCommand::AddEdge {
                        edge_id,
                        src_id,
                        dst_id,
                        edge_type,
                        weight,
                        properties,
                        ..
                    } = &self.commands[idx]
                    {
                        let src_key = node_map.get(src_id).copied();
                        let dst_key = node_map.get(dst_id).copied();
                        if let (Some(src), Some(dst)) = (src_key, dst_key) {
                            // Cross-tier aware: endpoints seeded from CSR
                            // segments are non-resident in `mg` — a plain
                            // add_edge would silently drop the edge on
                            // replay. node_map membership IS the existence
                            // proof (replayed node or CSR row). The ORIGINAL
                            // logged edge id is preserved so client-cached
                            // edge handles (and later REMOVEEDGE records)
                            // resolve after restart.
                            if mg
                                .add_edge_across_tiers_with_id(
                                    *edge_id,
                                    src,
                                    dst,
                                    *edge_type,
                                    *weight,
                                    properties.clone(),
                                    0,
                                )
                                .is_ok()
                            {
                                *replayed += 1;
                            }
                        } else {
                            tracing::warn!(
                                "WAL replay: dropping edge (src={}, dst={}) — \
                                 referenced node(s) not found in WAL or CSR segments",
                                src_id,
                                dst_id
                            );
                        }
                    }
                }

                // Apply SETs (W2-9) in WAL order: after inserts (targets must
                // exist), before removes (a later DELETE tombstones the set
                // like the live execution did). Repeated sets of one key keep
                // last-write-wins by ordering.
                for &idx in &epoch.set_indices {
                    match &self.commands[idx] {
                        GraphCommand::SetProp {
                            entity_id,
                            is_node: true,
                            key,
                            value,
                            ..
                        } => {
                            let Some(nk) = node_map.get(entity_id).copied() else {
                                tracing::warn!(
                                    "WAL replay: SETPROP node_id={} not found in WAL or CSR",
                                    entity_id
                                );
                                continue;
                            };
                            // CSR-resident target (frozen before the SET was
                            // logged): copy the row up first, like live W2-2.
                            if mg.get_node(nk).is_none()
                                && !crate::graph::store::copy_up_into(&mut mg, &immutable, nk)
                            {
                                tracing::warn!(
                                    "WAL replay: SETPROP copy-up failed for node_id={}",
                                    entity_id
                                );
                                continue;
                            }
                            if let Some(node) = mg.get_node_mut(nk) {
                                match node.properties.iter_mut().find(|(k, _)| k == key) {
                                    Some(entry) => entry.1 = value.clone(),
                                    None => node.properties.push((*key, value.clone())),
                                }
                                *replayed += 1;
                            }
                        }
                        GraphCommand::SetProp {
                            entity_id,
                            is_node: false,
                            key,
                            value,
                            ..
                        } => {
                            // Edge SET is not producible by the executor today
                            // (SetItem::Property matches nodes only) — apply
                            // defensively if the edge is resident.
                            let ek = crate::graph::types::EdgeKey::from(
                                slotmap::KeyData::from_ffi(*entity_id),
                            );
                            if let Some(edge) = mg.get_edge_mut(ek) {
                                let props = edge.properties.get_or_insert_with(SmallVec::new);
                                match props.iter_mut().find(|(k, _)| k == key) {
                                    Some(entry) => entry.1 = value.clone(),
                                    None => props.push((*key, value.clone())),
                                }
                                *replayed += 1;
                            }
                        }
                        GraphCommand::SetLabel { node_id, label, .. } => {
                            let Some(nk) = node_map.get(node_id).copied() else {
                                tracing::warn!(
                                    "WAL replay: SETLABEL node_id={} not found in WAL or CSR",
                                    node_id
                                );
                                continue;
                            };
                            if mg.get_node(nk).is_none()
                                && !crate::graph::store::copy_up_into(&mut mg, &immutable, nk)
                            {
                                continue;
                            }
                            if let Some(node) = mg.get_node_mut(nk) {
                                if !node.labels.contains(label) {
                                    node.labels.push(*label);
                                }
                                *replayed += 1;
                            }
                        }
                        _ => {}
                    }
                }

                // Remove nodes.
                for &idx in &epoch.remove_node_indices {
                    if let GraphCommand::RemoveNode { node_id, .. } = &self.commands[idx] {
                        if let Some(nk) = node_map.get(node_id).copied() {
                            if mg.remove_node(nk, 0) {
                                *replayed += 1;
                            } else if mg.get_node(nk).is_none() {
                                // CSR-resident node (copy-up delete, W2-2):
                                // materialize a tombstone shadow so the frozen
                                // row stays hidden after replay.
                                if crate::graph::store::copy_up_into(&mut mg, &immutable, nk)
                                    && mg.remove_node(nk, 0)
                                {
                                    *replayed += 1;
                                }
                            }
                        }
                    }
                }

                // Remove edges.
                for &idx in &epoch.remove_edge_indices {
                    if let GraphCommand::RemoveEdge { edge_id, .. } = &self.commands[idx] {
                        if mg.remove_edge_by_id(*edge_id, 0) {
                            *replayed += 1;
                        } else {
                            tracing::warn!(
                                "WAL replay: REMOVEEDGE edge_id={} not found in mutable segment",
                                edge_id
                            );
                        }
                    }
                }

                put_memgraph(graph, mg);

                // Register _key→NodeKey mappings on the NamedGraph (survives restart).
                for (redis_key, node_key) in key_registrations {
                    graph.register_key(redis_key, node_key);
                }
            }

            // 3. Drop graph if this epoch ends with a Drop command.
            if epoch.drop_idx.is_some() {
                if store.drop_graph(&epoch.graph_name).is_ok() {
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

/// Take the mutable write buffer out of the graph for mutation during
/// replay, along with the immutable CSR segment list (for node re-seeding).
///
/// Replay mutates `write_buf` — the authoritative store every command
/// handler reads. (An earlier version replayed into `segments.mutable`,
/// which no handler consults: replayed unfrozen data was INVISIBLE after
/// restart.)
fn take_memgraph(
    graph: &mut crate::graph::store::NamedGraph,
) -> (MemGraph, Vec<std::sync::Arc<crate::graph::csr::CsrStorage>>) {
    let immutable = graph.segments.load().immutable.clone();
    let mg = std::mem::replace(&mut graph.write_buf, MemGraph::new(graph.edge_threshold));
    (mg, immutable)
}

/// Put the write buffer back after mutation.
fn put_memgraph(graph: &mut crate::graph::store::NamedGraph, mg: MemGraph) {
    graph.write_buf = mg;
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
        assert_eq!(graph.write_buf.node_count(), 2);
        assert_eq!(graph.write_buf.edge_count(), 1);
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
    fn test_create_drop_recreate_temporal_ordering() {
        let mut collector = GraphReplayCollector::new();

        // WAL sequence: create → addnode → drop → create → addnode (different)
        collector.collect_command(b"GRAPH.CREATE", &[b"g"]);
        let node1_args: Vec<&[u8]> = vec![b"g", b"10", b"1", b"0", b"0"];
        collector.collect_command(b"GRAPH.ADDNODE", &node1_args);
        collector.collect_command(b"GRAPH.DROP", &[b"g"]);
        collector.collect_command(b"GRAPH.CREATE", &[b"g"]);
        let node2_args: Vec<&[u8]> = vec![b"g", b"20", b"1", b"1", b"0"];
        collector.collect_command(b"GRAPH.ADDNODE", &node2_args);

        let mut store = GraphStore::new();
        let replayed = collector.replay_into(&mut store);

        // create(1) + node(1) + drop(1) + create(1) + node(1) = 5
        assert_eq!(replayed, 5);

        // Final state: graph "g" exists with exactly 1 node (from second epoch).
        let graph = store.get_graph(b"g").expect("graph should exist");
        assert_eq!(graph.write_buf.node_count(), 1);
    }

    /// P0-2 stable ids: WAL-logged node/edge ids are the PUBLIC handles
    /// clients cache (ADDNODE/ADDEDGE return them). Replay must re-insert
    /// entities under the SAME ids, into the AUTHORITATIVE write_buf that
    /// every read/write path uses.
    #[test]
    fn test_replay_preserves_node_and_edge_ids_in_write_buf() {
        // Ids as an original session would have logged them (slotmap as_ffi
        // values always carry an odd version word).
        let n1 = (1u64 << 32) | 100;
        let n2 = (1u64 << 32) | 200;
        let e1 = (1u64 << 32) | 300;
        let n1s = n1.to_string();
        let n2s = n2.to_string();
        let e1s = e1.to_string();

        let mut collector = GraphReplayCollector::new();
        collector.collect_command(b"GRAPH.CREATE", &[b"g"]);
        collector.collect_command(b"GRAPH.ADDNODE", &[b"g", n1s.as_bytes(), b"1", b"7", b"0"]);
        collector.collect_command(b"GRAPH.ADDNODE", &[b"g", n2s.as_bytes(), b"1", b"7", b"0"]);
        collector.collect_command(
            b"GRAPH.ADDEDGE",
            &[
                b"g",
                e1s.as_bytes(),
                n1s.as_bytes(),
                n2s.as_bytes(),
                b"3",
                b"1.5",
                b"0",
            ],
        );

        let mut store = GraphStore::new();
        collector.replay_into(&mut store);

        let graph = store.get_graph(b"g").expect("graph");
        // Replayed data must live in write_buf — the tier every handler
        // (Cypher, NEIGHBORS, freeze) actually reads — not in a side copy.
        assert_eq!(
            graph.write_buf.node_count(),
            2,
            "nodes must land in write_buf"
        );
        assert_eq!(
            graph.write_buf.edge_count(),
            1,
            "edge must land in write_buf"
        );

        let nk1 = crate::graph::types::NodeKey::from(slotmap::KeyData::from_ffi(n1));
        let nk2 = crate::graph::types::NodeKey::from(slotmap::KeyData::from_ffi(n2));
        let ek1 = crate::graph::types::EdgeKey::from(slotmap::KeyData::from_ffi(e1));
        assert!(
            graph.write_buf.get_node(nk1).is_some(),
            "pre-restart node handle {n1} must resolve after replay"
        );
        assert!(graph.write_buf.get_node(nk2).is_some());
        let edge = graph
            .write_buf
            .get_edge(ek1)
            .expect("pre-restart edge handle must resolve after replay");
        assert_eq!(edge.src, nk1);
        assert_eq!(edge.dst, nk2);
        assert_eq!(edge.edge_type, 3);
    }

    #[test]
    fn test_replay_remove_edge_by_original_id() {
        let n1 = ((1u64 << 32) | 10).to_string();
        let n2 = ((1u64 << 32) | 11).to_string();
        let e1 = (1u64 << 32) | 12;
        let e1s = e1.to_string();

        let mut collector = GraphReplayCollector::new();
        collector.collect_command(b"GRAPH.CREATE", &[b"g"]);
        collector.collect_command(b"GRAPH.ADDNODE", &[b"g", n1.as_bytes(), b"1", b"0", b"0"]);
        collector.collect_command(b"GRAPH.ADDNODE", &[b"g", n2.as_bytes(), b"1", b"0", b"0"]);
        collector.collect_command(
            b"GRAPH.ADDEDGE",
            &[
                b"g",
                e1s.as_bytes(),
                n1.as_bytes(),
                n2.as_bytes(),
                b"0",
                b"1.0",
                b"0",
            ],
        );
        collector.collect_command(b"GRAPH.REMOVEEDGE", &[b"g", e1s.as_bytes()]);

        let mut store = GraphStore::new();
        collector.replay_into(&mut store);

        let graph = store.get_graph(b"g").expect("graph");
        assert_eq!(
            graph.write_buf.edge_count(),
            0,
            "REMOVEEDGE by the WAL-logged id must delete the replayed edge"
        );
    }

    #[test]
    fn test_post_replay_inserts_do_not_alias_replayed_ids() {
        let n1 = (1u64 << 32) | 500;
        let n1s = n1.to_string();

        let mut collector = GraphReplayCollector::new();
        collector.collect_command(b"GRAPH.CREATE", &[b"g"]);
        collector.collect_command(b"GRAPH.ADDNODE", &[b"g", n1s.as_bytes(), b"1", b"0", b"0"]);

        let mut store = GraphStore::new();
        collector.replay_into(&mut store);

        let graph = store.get_graph_mut(b"g").expect("graph");
        let nk1 = crate::graph::types::NodeKey::from(slotmap::KeyData::from_ffi(n1));
        let new_key = graph
            .write_buf
            .add_node(SmallVec::new(), SmallVec::new(), None, 9);
        assert_ne!(
            new_key, nk1,
            "post-restart insert must never re-issue a replayed id"
        );
        use slotmap::Key;
        assert!(
            new_key.data().as_ffi() > n1,
            "new ids must be allocated above the replayed high-water mark"
        );
    }

    /// W2-2 copy-up delete: a WAL REMOVENODE whose target lives in a
    /// frozen CSR segment must materialize a TOMBSTONE shadow in write_buf
    /// (the frozen row itself is immutable).
    #[test]
    fn test_replay_removenode_on_frozen_node_creates_tombstone() {
        use slotmap::Key;
        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"g"), 4, 0)
            .expect("create");
        let g = store.get_graph_mut(b"g").expect("graph");
        let a = g.write_buf.add_node(
            smallvec::smallvec![1u16],
            smallvec::SmallVec::new(),
            None,
            1,
        );
        let b = g.write_buf.add_node(
            smallvec::smallvec![1u16],
            smallvec::SmallVec::new(),
            None,
            1,
        );
        g.write_buf.add_edge(a, b, 0, 1.0, None, 2).expect("edge");
        assert!(g.freeze_and_compact(10), "freeze must succeed");
        assert!(g.write_buf.get_node(a).is_none(), "freeze drains node a");

        let mut collector = GraphReplayCollector::new();
        let id = a.data().as_ffi().to_string();
        assert!(collector.collect_command(b"GRAPH.REMOVENODE", &[b"g", id.as_bytes()]));
        let replayed = collector.replay_into(&mut store);
        assert_eq!(replayed, 1, "REMOVENODE of a frozen node must replay");

        let g = store.get_graph(b"g").expect("graph");
        let shadow = g
            .write_buf
            .get_node(a)
            .expect("tombstone shadow must exist in write_buf");
        assert_ne!(shadow.deleted_lsn, u64::MAX, "shadow must be soft-deleted");
    }

    /// W2-9: SETPROP/SETLABEL replay — sets applied after inserts in WAL
    /// order (last-write-wins), labels idempotent.
    #[test]
    fn test_replay_setprop_and_setlabel() {
        let n1 = (1u64 << 32) | 7;
        let n1s = n1.to_string();

        let mut collector = GraphReplayCollector::new();
        assert!(collector.collect_command(b"GRAPH.CREATE", &[b"g"]));
        assert!(
            collector.collect_command(b"GRAPH.ADDNODE", &[b"g", n1s.as_bytes(), b"1", b"3", b"0"])
        );
        // Two sets of the same key: the later one must win.
        assert!(collector.collect_command(
            b"GRAPH.SETPROP",
            &[b"g", b"N", n1s.as_bytes(), b"9", b"i", b"1"]
        ));
        assert!(collector.collect_command(
            b"GRAPH.SETPROP",
            &[b"g", b"N", n1s.as_bytes(), b"9", b"i", b"42"]
        ));
        assert!(collector.collect_command(b"GRAPH.SETLABEL", &[b"g", n1s.as_bytes(), b"5"]));

        let mut store = GraphStore::new();
        let replayed = collector.replay_into(&mut store);
        assert_eq!(replayed, 5, "create + addnode + 2 setprops + setlabel");

        let g = store.get_graph(b"g").expect("graph");
        let nk = crate::graph::types::NodeKey::from(slotmap::KeyData::from_ffi(n1));
        let node = g.write_buf.get_node(nk).expect("node replayed");
        assert_eq!(
            node.properties
                .iter()
                .find(|(k, _)| *k == 9)
                .map(|(_, v)| v),
            Some(&PropertyValue::Int(42)),
            "last SETPROP wins"
        );
        assert!(node.labels.contains(&3), "original label kept");
        assert!(node.labels.contains(&5), "SETLABEL applied");
    }

    /// W2-9 + W2-2: a SETPROP whose target froze before the crash must copy
    /// the row up into write_buf (frozen rows are immutable) and mutate the
    /// shadow.
    #[test]
    fn test_replay_setprop_on_frozen_node_copies_up() {
        use slotmap::Key;
        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"g"), 4, 0)
            .expect("create");
        let g = store.get_graph_mut(b"g").expect("graph");
        let a = g.write_buf.add_node(
            smallvec::smallvec![1u16],
            smallvec::SmallVec::new(),
            None,
            1,
        );
        let b = g.write_buf.add_node(
            smallvec::smallvec![1u16],
            smallvec::SmallVec::new(),
            None,
            1,
        );
        g.write_buf.add_edge(a, b, 0, 1.0, None, 2).expect("edge");
        assert!(g.freeze_and_compact(10), "freeze must succeed");
        assert!(g.write_buf.get_node(a).is_none(), "freeze drains node a");

        let mut collector = GraphReplayCollector::new();
        let id = a.data().as_ffi().to_string();
        assert!(collector.collect_command(
            b"GRAPH.SETPROP",
            &[b"g", b"N", id.as_bytes(), b"4", b"s", b"hot"]
        ));
        let replayed = collector.replay_into(&mut store);
        assert_eq!(replayed, 1, "SETPROP on a frozen node must replay");

        let g = store.get_graph(b"g").expect("graph");
        let shadow = g
            .write_buf
            .get_node(a)
            .expect("copy-up shadow must exist in write_buf");
        assert_eq!(
            shadow
                .properties
                .iter()
                .find(|(k, _)| *k == 4)
                .map(|(_, v)| v),
            Some(&PropertyValue::String(Bytes::from_static(b"hot"))),
            "SETPROP applied to the copy-up shadow"
        );
    }

    #[test]
    fn test_parse_property_value() {
        assert_eq!(
            parse_property_value(b"i", b"42"),
            Some(PropertyValue::Int(42))
        );
        #[allow(clippy::approx_constant)]
        {
            assert_eq!(
                parse_property_value(b"f", b"3.14"),
                Some(PropertyValue::Float(3.14))
            );
        }
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
