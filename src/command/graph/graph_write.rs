//! GRAPH.* write command handlers.
//!
//! These commands mutate GraphStore state: CREATE, ADDNODE, ADDEDGE, DELETE.
//! All operate on the shard-local `NamedGraph.write_buf` (MemGraph) directly,
//! avoiding ArcSwap overhead on the write path.

use bytes::Bytes;
use slotmap::Key;
use smallvec::SmallVec;

use crate::graph::store::GraphStore;
use crate::graph::types::{PropertyMap, PropertyValue};
use crate::graph::wal;
use crate::protocol::Frame;

/// GRAPH.CREATE <name>
///
/// Creates a new named graph. Returns OK on success.
/// Error if graph already exists.
pub fn graph_create(store: &mut GraphStore, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'GRAPH.CREATE' command",
        ));
    }

    let name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid graph name")),
    };

    // Default edge threshold: 64K edges before freeze.
    let edge_threshold = 64_000;
    let lsn = store.allocate_lsn();

    match store.create_graph(Bytes::copy_from_slice(name), edge_threshold, lsn) {
        Ok(()) => {
            store.wal_pending.push(wal::serialize_graph_create(name));
            Frame::SimpleString(Bytes::from_static(b"OK"))
        }
        Err(crate::graph::store::GraphStoreError::GraphAlreadyExists) => {
            Frame::Error(Bytes::from_static(b"ERR graph already exists"))
        }
        Err(_) => Frame::Error(Bytes::from_static(b"ERR internal error")),
    }
}

/// GRAPH.ADDNODE <graph> <label> [<prop> <val>]... [VECTOR <field> <blob>]
///
/// Inserts a node into the named graph. Returns the node ID as an integer.
/// Label is stored as a u16 dictionary index (hashed from the label string).
/// Properties are key-value pairs parsed from the remaining arguments.
pub fn graph_addnode(store: &mut GraphStore, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'GRAPH.ADDNODE' command",
        ));
    }

    let graph_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid graph name")),
    };

    let label = match extract_bulk(&args[1]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid label")),
    };

    // Verify graph exists before parsing remaining args.
    if store.get_graph(graph_name).is_none() {
        return Frame::Error(Bytes::from_static(b"ERR graph not found"));
    }

    // Hash label to u16 dictionary index.
    let label_id = label_to_id(label);
    let labels: SmallVec<[u16; 4]> = SmallVec::from_elem(label_id, 1);

    // Parse optional properties and vector embedding.
    let mut properties: PropertyMap = SmallVec::new();
    let mut embedding: Option<Vec<f32>> = None;
    let mut pos = 2;

    while pos < args.len() {
        let key = match extract_bulk(&args[pos]) {
            Some(b) => b,
            None => {
                pos += 1;
                continue;
            }
        };

        if key.eq_ignore_ascii_case(b"VECTOR") {
            // VECTOR <field> <blob>
            pos += 1; // skip field name
            if pos >= args.len() {
                return Frame::Error(Bytes::from_static(b"ERR missing VECTOR field name"));
            }
            pos += 1; // skip to blob
            if pos >= args.len() {
                return Frame::Error(Bytes::from_static(b"ERR missing VECTOR blob"));
            }
            if let Some(blob) = extract_bulk(&args[pos]) {
                embedding = parse_f32_blob(blob);
                if embedding.is_none() {
                    return Frame::Error(Bytes::from_static(
                        b"ERR invalid VECTOR blob (must be f32 array)",
                    ));
                }
            }
            pos += 1;
        } else {
            // Property key-value pair.
            pos += 1;
            if pos >= args.len() {
                return Frame::Error(Bytes::from_static(b"ERR missing property value"));
            }
            let prop_key = label_to_id(key);
            let prop_val = parse_property_value(&args[pos]);
            properties.push((prop_key, prop_val));
            pos += 1;
        }
    }

    let lsn = store.allocate_lsn();

    // Scoped borrow of graph for mutation, then release for WAL push.
    let external_id = {
        let graph = match store.get_graph_mut(graph_name) {
            Some(g) => g,
            None => return Frame::Error(Bytes::from_static(b"ERR graph not found")),
        };

        let node_key =
            graph
                .write_buf
                .add_node(labels.clone(), properties.clone(), embedding.clone(), lsn);

        // Update graph stats incrementally.
        graph.stats.on_node_insert(&labels);

        // Update PropertyIndex for numeric properties (range query support).
        for (prop_id, prop_val) in &properties {
            let val = match prop_val {
                crate::graph::types::PropertyValue::Float(f) => Some(*f),
                crate::graph::types::PropertyValue::Int(i) => Some(*i as f64),
                _ => None,
            };
            if let Some(v) = val {
                graph
                    .property_indexes
                    .entry(*prop_id)
                    .or_insert_with(|| crate::graph::index::PropertyIndex::new(*prop_id))
                    .insert(v, node_key.data().as_ffi() as u32);
            }
        }

        node_key.data().as_ffi()
    };

    // Serialize WAL record for the node insertion.
    store.wal_pending.push(wal::serialize_add_node(
        graph_name,
        external_id,
        &labels,
        &properties,
        embedding.as_deref(),
    ));

    Frame::Integer(external_id as i64)
}

/// GRAPH.ADDEDGE <graph> <src_id> <dst_id> <type> [<prop> <val>]... [WEIGHT <w>]
///
/// Inserts a directed edge between two nodes. Returns the edge ID.
pub fn graph_addedge(store: &mut GraphStore, args: &[Frame]) -> Frame {
    if args.len() < 4 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'GRAPH.ADDEDGE' command",
        ));
    }

    let graph_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid graph name")),
    };

    let src_id = match parse_u64(&args[1]) {
        Some(id) => id,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid source node ID")),
    };

    let dst_id = match parse_u64(&args[2]) {
        Some(id) => id,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid destination node ID")),
    };

    let edge_type_str = match extract_bulk(&args[3]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid edge type")),
    };

    // Verify graph exists before parsing remaining args.
    if store.get_graph(graph_name).is_none() {
        return Frame::Error(Bytes::from_static(b"ERR graph not found"));
    }

    let edge_type_id = label_to_id(edge_type_str);

    // Parse optional WEIGHT and properties.
    let mut weight = 1.0f64;
    let mut properties: PropertyMap = SmallVec::new();
    let mut pos = 4;

    while pos < args.len() {
        let key = match extract_bulk(&args[pos]) {
            Some(b) => b,
            None => {
                pos += 1;
                continue;
            }
        };

        if key.eq_ignore_ascii_case(b"WEIGHT") {
            pos += 1;
            if pos >= args.len() {
                return Frame::Error(Bytes::from_static(b"ERR missing WEIGHT value"));
            }
            weight = match parse_f64(&args[pos]) {
                Some(w) => w,
                None => return Frame::Error(Bytes::from_static(b"ERR invalid WEIGHT value")),
            };
            pos += 1;
        } else {
            // Property key-value pair.
            pos += 1;
            if pos >= args.len() {
                return Frame::Error(Bytes::from_static(b"ERR missing property value"));
            }
            let prop_key = label_to_id(key);
            let prop_val = parse_property_value(&args[pos]);
            properties.push((prop_key, prop_val));
            pos += 1;
        }
    }

    // Convert external IDs back to NodeKey via slotmap::KeyData.
    let src_key = external_id_to_node_key(src_id);
    let dst_key = external_id_to_node_key(dst_id);

    let props = if properties.is_empty() {
        None
    } else {
        Some(properties)
    };

    let lsn = store.allocate_lsn();

    // Scoped borrow of graph for mutation, then release for WAL push.
    let edge_result = {
        let graph = match store.get_graph_mut(graph_name) {
            Some(g) => g,
            None => return Frame::Error(Bytes::from_static(b"ERR graph not found")),
        };

        // Capture old degrees for stats tracking before edge insertion.
        let src_old_degree = graph
            .write_buf
            .get_node(src_key)
            .map_or(0, |n| (n.outgoing.len() + n.incoming.len()) as u32);
        let dst_old_degree = graph
            .write_buf
            .get_node(dst_key)
            .map_or(0, |n| (n.outgoing.len() + n.incoming.len()) as u32);

        match graph
            .write_buf
            .add_edge(src_key, dst_key, edge_type_id, weight, props.clone(), lsn)
        {
            Ok(edge_key) => {
                graph
                    .stats
                    .on_edge_insert(edge_type_id, src_old_degree, dst_old_degree);
                Ok(edge_key.data().as_ffi())
            }
            Err(e) => Err(e),
        }
    };

    match edge_result {
        Ok(external_id) => {
            store.wal_pending.push(wal::serialize_add_edge(
                graph_name,
                external_id,
                src_id,
                dst_id,
                edge_type_id,
                weight,
                props.as_ref(),
            ));

            // Check if compaction threshold reached after edge insertion.
            // If so, freeze the mutable MemGraph and convert to an immutable
            // CSR segment. This is synchronous and fast (<5ms at 64K edges).
            let needs_compact = store
                .get_graph(graph_name)
                .is_some_and(|g| g.should_compact());
            if needs_compact {
                let compact_lsn = store.allocate_lsn();
                if let Some(graph) = store.get_graph_mut(graph_name) {
                    graph.freeze_and_compact(compact_lsn);
                }
            }

            Frame::Integer(external_id as i64)
        }
        Err(crate::graph::memgraph::GraphError::NodeNotFound) => Frame::Error(Bytes::from_static(
            b"ERR source or destination node not found",
        )),
        Err(crate::graph::memgraph::GraphError::SelfLoop) => {
            Frame::Error(Bytes::from_static(b"ERR self-loops are not allowed"))
        }
        Err(crate::graph::memgraph::GraphError::AlreadyFrozen) => {
            Frame::Error(Bytes::from_static(b"ERR graph segment is frozen"))
        }
    }
}

/// GRAPH.DELETE <graph>
///
/// Drops a named graph and all its data. Returns OK.
pub fn graph_delete(store: &mut GraphStore, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'GRAPH.DELETE' command",
        ));
    }

    let name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid graph name")),
    };

    match store.drop_graph(name) {
        Ok(()) => {
            store.wal_pending.push(wal::serialize_drop_graph(name));
            Frame::SimpleString(Bytes::from_static(b"OK"))
        }
        Err(crate::graph::store::GraphStoreError::GraphNotFound) => {
            Frame::Error(Bytes::from_static(b"ERR graph not found"))
        }
        Err(_) => Frame::Error(Bytes::from_static(b"ERR internal error")),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract raw bytes from a BulkString or SimpleString frame.
#[inline]
pub(crate) fn extract_bulk(frame: &Frame) -> Option<&[u8]> {
    match frame {
        Frame::BulkString(b) => Some(b.as_ref()),
        Frame::SimpleString(b) => Some(b.as_ref()),
        _ => None,
    }
}

/// Hash a label/type/property name to a u16 dictionary index.
/// Uses FNV-1a truncated to 16 bits for fast, deterministic mapping.
#[inline]
pub(crate) fn label_to_id(name: &[u8]) -> u16 {
    let mut hash: u32 = 0x811c_9dc5; // FNV offset basis
    for &b in name {
        hash ^= b as u32;
        hash = hash.wrapping_mul(0x0100_0193); // FNV prime
    }
    (hash & 0xFFFF) as u16
}

/// Convert an external node ID back to a slotmap NodeKey.
#[inline]
pub(crate) fn external_id_to_node_key(id: u64) -> crate::graph::types::NodeKey {
    slotmap::KeyData::from_ffi(id).into()
}

/// Convert an external edge ID back to a slotmap EdgeKey.
#[inline]
#[allow(dead_code)] // Used by future GRAPH.REMOVEEDGE command
pub(crate) fn external_id_to_edge_key(id: u64) -> crate::graph::types::EdgeKey {
    slotmap::KeyData::from_ffi(id).into()
}

/// Parse a Frame as a u64 integer.
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

/// Parse a Frame as an f64.
fn parse_f64(frame: &Frame) -> Option<f64> {
    match frame {
        Frame::Double(f) => Some(*f),
        Frame::Integer(n) => Some(*n as f64),
        Frame::BulkString(b) | Frame::SimpleString(b) => core::str::from_utf8(b).ok()?.parse().ok(),
        _ => None,
    }
}

/// Parse a property value from a Frame. Tries integer, then float, then string.
fn parse_property_value(frame: &Frame) -> PropertyValue {
    match frame {
        Frame::Integer(n) => PropertyValue::Int(*n),
        Frame::Double(f) => PropertyValue::Float(*f),
        Frame::Boolean(b) => PropertyValue::Bool(*b),
        Frame::BulkString(b) | Frame::SimpleString(b) => {
            // Try parsing as integer first.
            if let Ok(s) = core::str::from_utf8(b) {
                if let Ok(n) = s.parse::<i64>() {
                    return PropertyValue::Int(n);
                }
                if let Ok(f) = s.parse::<f64>() {
                    return PropertyValue::Float(f);
                }
            }
            PropertyValue::String(b.clone())
        }
        _ => PropertyValue::String(Bytes::from_static(b"")),
    }
}

/// Parse a raw byte blob as a sequence of f32 values (little-endian).
fn parse_f32_blob(blob: &[u8]) -> Option<Vec<f32>> {
    if !blob.len().is_multiple_of(4) {
        return None;
    }
    let count = blob.len() / 4;
    let mut vec = Vec::with_capacity(count);
    for chunk in blob.chunks_exact(4) {
        vec.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    Some(vec)
}
