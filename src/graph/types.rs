//! Core types for the graph storage engine.
//!
//! Defines node/edge keys (via SlotMap generational indices), property values,
//! mutable node/edge structs, and `#[repr(C)]` on-disk segment headers.

use bytes::Bytes;
use slotmap::new_key_type;
use smallvec::SmallVec;

new_key_type! {
    /// 64-bit generational index for nodes. SlotMap prevents ABA on long-running servers.
    pub struct NodeKey;
    /// 64-bit generational index for edges.
    pub struct EdgeKey;
}

/// Direction filter for neighbor queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Outgoing,
    Incoming,
    Both,
}

/// Typed property value stored on nodes and edges.
#[derive(Debug, Clone, PartialEq)]
pub enum PropertyValue {
    Int(i64),
    Float(f64),
    String(Bytes),
    Bool(bool),
    Bytes(Bytes),
}

/// Property map: inline SmallVec for typical 2-4 property nodes.
/// Key is a u16 index into a per-graph property name dictionary.
pub type PropertyMap = SmallVec<[(u16, PropertyValue); 4]>;

/// Mutable node in MemGraph adjacency list.
#[derive(Debug)]
pub struct MutableNode {
    /// Label dictionary indices.
    pub labels: SmallVec<[u16; 4]>,
    /// Outgoing edge keys.
    pub outgoing: SmallVec<[EdgeKey; 8]>,
    /// Incoming edge keys.
    pub incoming: SmallVec<[EdgeKey; 8]>,
    /// Node properties.
    pub properties: PropertyMap,
    /// Optional vector embedding for hybrid graph+vector queries.
    pub embedding: Option<Vec<f32>>,
    /// LSN at which this node was created.
    pub created_lsn: u64,
    /// LSN at which this node was soft-deleted (u64::MAX if alive).
    pub deleted_lsn: u64,
    /// Transaction ID that created this node (0 = no transaction / pre-MVCC).
    pub txn_id: u64,
}

/// Mutable edge in MemGraph.
#[derive(Debug)]
pub struct MutableEdge {
    /// Source node key.
    pub src: NodeKey,
    /// Destination node key.
    pub dst: NodeKey,
    /// Edge-type dictionary index.
    pub edge_type: u16,
    /// First-class weight (time/distance/cost).
    pub weight: f64,
    /// Optional edge properties.
    pub properties: Option<PropertyMap>,
    /// LSN at which this edge was created.
    pub created_lsn: u64,
    /// LSN at which this edge was soft-deleted (u64::MAX if alive).
    pub deleted_lsn: u64,
    /// Transaction ID that created this edge (0 = no transaction / pre-MVCC).
    pub txn_id: u64,
}

/// On-disk CSR segment header -- cache-line aligned, zero-copy mmap.
#[derive(Debug)]
#[repr(C, align(64))]
pub struct GraphSegmentHeader {
    /// Magic bytes: b"MNGR" (Moon Graph).
    pub magic: [u8; 4],
    pub version: u32,
    pub node_count: u32,
    pub edge_count: u32,
    pub min_node_id: u64,
    pub max_node_id: u64,
    pub row_offsets_offset: u64,
    pub col_indices_offset: u64,
    pub edge_meta_offset: u64,
    pub validity_bitmap_offset: u64,
    pub created_lsn: u64,
    pub checksum: u64,
}

// 4 + 4 + 4 + 4 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 = 80 bytes, padded to 128 with align(64)
// Actually: fields sum to 80 bytes. With align(64) the struct is padded to 128.
const _: () = assert!(core::mem::size_of::<GraphSegmentHeader>() == 128);
const _: () = assert!(core::mem::align_of::<GraphSegmentHeader>() == 64);

/// Edge metadata stored in CSR alongside col_indices.
#[derive(Debug, Clone, Copy)]
#[repr(C, align(8))]
pub struct EdgeMeta {
    /// Edge-type dictionary index.
    pub edge_type: u16,
    /// Flags: direction, weight presence, property presence.
    pub flags: u16,
    /// Offset into property block for this edge.
    pub property_offset: u32,
}

const _: () = assert!(core::mem::size_of::<EdgeMeta>() == 8);
const _: () = assert!(core::mem::align_of::<EdgeMeta>() == 8);

/// Node metadata in CSR (parallel array indexed by CSR row).
#[derive(Debug)]
#[repr(C)]
pub struct NodeMeta {
    /// External node ID (for reverse mapping).
    pub external_id: u64,
    /// Bitset of label IDs (supports 32 labels).
    pub label_bitmap: u32,
    /// Offset into property block for this node.
    pub property_offset: u32,
    /// LSN at which this node was created.
    pub created_lsn: u64,
    /// LSN at which this node was soft-deleted (u64::MAX if alive).
    pub deleted_lsn: u64,
}

// 8 + 4 + 4 + 8 + 8 = 32 bytes
const _: () = assert!(core::mem::size_of::<NodeMeta>() == 32);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_graph_segment_header_size_and_alignment() {
        assert_eq!(core::mem::size_of::<GraphSegmentHeader>(), 128);
        assert_eq!(core::mem::align_of::<GraphSegmentHeader>(), 64);
    }

    #[test]
    fn test_edge_meta_size() {
        assert_eq!(core::mem::size_of::<EdgeMeta>(), 8);
    }

    #[test]
    fn test_node_meta_size() {
        assert_eq!(core::mem::size_of::<NodeMeta>(), 32);
    }

    #[test]
    fn test_node_key_is_64bit() {
        assert_eq!(core::mem::size_of::<NodeKey>(), 8);
    }

    #[test]
    fn test_edge_key_is_64bit() {
        assert_eq!(core::mem::size_of::<EdgeKey>(), 8);
    }

    #[test]
    fn test_property_value_variants() {
        let int = PropertyValue::Int(42);
        let float = PropertyValue::Float(3.14);
        let string = PropertyValue::String(Bytes::from_static(b"hello"));
        let boolean = PropertyValue::Bool(true);
        let blob = PropertyValue::Bytes(Bytes::from_static(b"\x00\x01"));
        assert_eq!(int, PropertyValue::Int(42));
        assert_eq!(float, PropertyValue::Float(3.14));
        assert_eq!(string, PropertyValue::String(Bytes::from_static(b"hello")));
        assert_eq!(boolean, PropertyValue::Bool(true));
        assert_eq!(blob, PropertyValue::Bytes(Bytes::from_static(b"\x00\x01")));
    }

    #[test]
    fn test_direction_variants() {
        assert_ne!(Direction::Outgoing, Direction::Incoming);
        assert_ne!(Direction::Both, Direction::Outgoing);
    }
}
