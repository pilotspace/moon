//! Graph index structures for O(1) label, edge-type, node-ID, and property lookups.
//!
//! - `LabelIndex`: per-label Roaring bitmap of CSR row indices
//! - `EdgeTypeIndex`: per-edge-type Roaring bitmap of edge indices in col_indices
//! - `MphNodeIndex`: boomphf minimal perfect hash for NodeKey -> CSR row (3 bits/key)
//! - `PropertyIndex`: BTreeMap<f64, RoaringBitmap> for numeric range queries

use std::collections::{BTreeMap, HashMap};

use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;

use crate::graph::types::{EdgeMeta, NodeKey, NodeMeta};

// ---------------------------------------------------------------------------
// LabelIndex
// ---------------------------------------------------------------------------

/// Per-label Roaring bitmap mapping label ID -> set of CSR row indices.
///
/// Built during CSR construction from `NodeMeta::label_bitmap`.
/// Supports up to 32 labels (matching the 32-bit label bitmap in NodeMeta).
#[derive(Debug, Clone)]
pub struct LabelIndex {
    /// label_id -> bitmap of node row indices carrying that label.
    labels: HashMap<u16, RoaringBitmap>,
}

impl LabelIndex {
    /// Build a label index from CSR node metadata.
    ///
    /// Iterates once over `node_meta`, extracting set bits from each node's
    /// `label_bitmap` and inserting the row index into the corresponding bitmap.
    pub fn build(node_meta: &[NodeMeta]) -> Self {
        let mut labels: HashMap<u16, RoaringBitmap> = HashMap::new();
        for (row, meta) in node_meta.iter().enumerate() {
            let mut bitmap = meta.label_bitmap;
            while bitmap != 0 {
                let bit = bitmap.trailing_zeros() as u16;
                labels
                    .entry(bit)
                    .or_insert_with(RoaringBitmap::new)
                    .insert(row as u32);
                bitmap &= bitmap - 1; // clear lowest set bit
            }
        }
        labels.shrink_to_fit();
        Self { labels }
    }

    /// Returns the bitmap of node rows carrying the given label, or `None`.
    pub fn nodes_with_label(&self, label: u16) -> Option<&RoaringBitmap> {
        self.labels.get(&label)
    }

    /// Number of distinct labels indexed.
    pub fn label_count(&self) -> usize {
        self.labels.len()
    }

    /// Returns true if the index is empty (no labels).
    pub fn is_empty(&self) -> bool {
        self.labels.is_empty()
    }
}

// ---------------------------------------------------------------------------
// EdgeTypeIndex
// ---------------------------------------------------------------------------

/// Per-edge-type Roaring bitmap mapping edge_type -> set of edge indices in col_indices.
///
/// Built during CSR construction from `EdgeMeta::edge_type`.
/// Filtering edges by type is a bitmap lookup: O(1) per segment.
#[derive(Debug, Clone)]
pub struct EdgeTypeIndex {
    /// edge_type -> bitmap of edge indices in col_indices that have this type.
    types: HashMap<u16, RoaringBitmap>,
}

impl EdgeTypeIndex {
    /// Build an edge type index from CSR edge metadata.
    pub fn build(edge_meta: &[EdgeMeta]) -> Self {
        let mut types: HashMap<u16, RoaringBitmap> = HashMap::new();
        for (idx, meta) in edge_meta.iter().enumerate() {
            types
                .entry(meta.edge_type)
                .or_insert_with(RoaringBitmap::new)
                .insert(idx as u32);
        }
        types.shrink_to_fit();
        Self { types }
    }

    /// Returns the bitmap of edge indices of the given type, or `None`.
    pub fn edges_of_type(&self, edge_type: u16) -> Option<&RoaringBitmap> {
        self.types.get(&edge_type)
    }

    /// Number of distinct edge types indexed.
    pub fn type_count(&self) -> usize {
        self.types.len()
    }

    /// Returns true if the index is empty (no edge types).
    pub fn is_empty(&self) -> bool {
        self.types.is_empty()
    }
}

// ---------------------------------------------------------------------------
// MphNodeIndex
// ---------------------------------------------------------------------------

/// Minimal perfect hash for NodeKey -> CSR row offset.
///
/// Uses `boomphf::Mphf` (~3 bits/key) instead of `HashMap<NodeKey, u32>` (~50 bytes/key).
/// Since MPH maps input keys to a *permutation* of `0..n` (not preserving insertion order),
/// we store two arrays: `hash_to_row[mph_hash]` = CSR row, and `row_keys[row]` = NodeKey
/// for false-positive rejection.
#[derive(Debug)]
pub struct MphNodeIndex {
    /// The minimal perfect hash function. `None` when built from empty set.
    mph: Option<boomphf::Mphf<NodeKey>>,
    /// Maps mph hash value -> CSR row index. Length = n.
    hash_to_row: Vec<u32>,
    /// Verification: row_keys[row] = NodeKey for that CSR row. Length = n.
    row_keys: Vec<NodeKey>,
}

impl MphNodeIndex {
    /// Build an MPH index from NodeKeys in CSR row order (index = row).
    ///
    /// gamma = 1.7 is the recommended space/build-time trade-off.
    pub fn build(sorted_keys: &[NodeKey]) -> Self {
        if sorted_keys.is_empty() {
            return Self {
                mph: None,
                hash_to_row: Vec::new(),
                row_keys: Vec::new(),
            };
        }

        let mph = boomphf::Mphf::new(1.7, sorted_keys);

        // Build reverse map: for each key at CSR row `i`, compute mph hash,
        // and store hash_to_row[mph_hash] = i.
        let n = sorted_keys.len();
        let mut hash_to_row = vec![0u32; n];
        for (row, key) in sorted_keys.iter().enumerate() {
            let h = mph.hash(key) as usize;
            hash_to_row[h] = row as u32;
        }

        Self {
            mph: Some(mph),
            hash_to_row,
            row_keys: sorted_keys.to_vec(),
        }
    }

    /// Look up the CSR row for a NodeKey. Returns `None` if the key was not in
    /// the original set (false-positive rejection via verification array).
    ///
    /// Uses `try_hash` to avoid panics on keys not in the original set.
    pub fn lookup(&self, key: NodeKey) -> Option<u32> {
        let mph = self.mph.as_ref()?;
        let h = mph.try_hash(&key)? as usize;
        if h >= self.hash_to_row.len() {
            return None;
        }
        let row = self.hash_to_row[h];
        if (row as usize) < self.row_keys.len() && self.row_keys[row as usize] == key {
            Some(row)
        } else {
            None
        }
    }

    /// Number of keys in the index.
    pub fn len(&self) -> usize {
        self.row_keys.len()
    }

    /// Returns true if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.row_keys.is_empty()
    }
}

// ---------------------------------------------------------------------------
// PropertyIndex
// ---------------------------------------------------------------------------

/// B-tree index on a numeric property field for range queries.
///
/// Maps `OrderedFloat<f64>` property values to Roaring bitmaps of node row indices.
/// Supports efficient range queries without scanning all properties.
#[derive(Debug, Clone)]
pub struct PropertyIndex {
    /// property_name_id for documentation/debugging.
    pub property_id: u16,
    /// Sorted map: property_value -> bitmap of node rows with that value.
    tree: BTreeMap<OrderedFloat<f64>, RoaringBitmap>,
}

impl PropertyIndex {
    /// Create a new empty property index for the given property ID.
    pub fn new(property_id: u16) -> Self {
        Self {
            property_id,
            tree: BTreeMap::new(),
        }
    }

    /// Insert a (value, row) pair into the index.
    pub fn insert(&mut self, value: f64, row: u32) {
        self.tree
            .entry(OrderedFloat(value))
            .or_insert_with(RoaringBitmap::new)
            .insert(row);
    }

    /// Remove a (value, row) pair from the index.
    pub fn remove(&mut self, value: f64, row: u32) {
        if let Some(bitmap) = self.tree.get_mut(&OrderedFloat(value)) {
            bitmap.remove(row);
            if bitmap.is_empty() {
                self.tree.remove(&OrderedFloat(value));
            }
        }
    }

    /// Range query: returns a bitmap of all node rows whose property value
    /// falls in `[min, max]` (inclusive on both ends).
    pub fn range_query(&self, min: f64, max: f64) -> RoaringBitmap {
        let lo = OrderedFloat(min);
        let hi = OrderedFloat(max);
        let mut result = RoaringBitmap::new();
        for (_, bitmap) in self.tree.range(lo..=hi) {
            result |= bitmap;
        }
        result
    }

    /// Range query with exclusive upper bound: `[min, max)`.
    pub fn range_query_exclusive_hi(&self, min: f64, max: f64) -> RoaringBitmap {
        let lo = OrderedFloat(min);
        let hi = OrderedFloat(max);
        let mut result = RoaringBitmap::new();
        for (_, bitmap) in self.tree.range(lo..hi) {
            result |= bitmap;
        }
        result
    }

    /// Greater-than query: returns rows where property > threshold.
    pub fn gt(&self, threshold: f64) -> RoaringBitmap {
        let key = OrderedFloat(threshold);
        let mut result = RoaringBitmap::new();
        // range (threshold, +inf) -- skip the threshold itself
        for (k, bitmap) in self.tree.range(key..) {
            if *k > key {
                result |= bitmap;
            }
        }
        result
    }

    /// Greater-than-or-equal query: returns rows where property >= threshold.
    pub fn gte(&self, threshold: f64) -> RoaringBitmap {
        let key = OrderedFloat(threshold);
        let mut result = RoaringBitmap::new();
        for (_, bitmap) in self.tree.range(key..) {
            result |= bitmap;
        }
        result
    }

    /// Less-than query: returns rows where property < threshold.
    pub fn lt(&self, threshold: f64) -> RoaringBitmap {
        let key = OrderedFloat(threshold);
        let mut result = RoaringBitmap::new();
        for (_, bitmap) in self.tree.range(..key) {
            result |= bitmap;
        }
        result
    }

    /// Number of distinct values in the index.
    pub fn distinct_values(&self) -> usize {
        self.tree.len()
    }

    /// Total number of indexed entries (sum of all bitmap cardinalities).
    pub fn total_entries(&self) -> u64 {
        self.tree.values().map(|b| b.len()).sum()
    }

    /// Returns true if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::types::{EdgeMeta, NodeMeta};

    // --- LabelIndex tests ---

    #[test]
    fn test_label_index_build_and_query() {
        let node_meta = vec![
            NodeMeta {
                external_id: 1,
                label_bitmap: 0b0000_0011, // labels 0 and 1
                property_offset: 0,
                created_lsn: 1,
                deleted_lsn: u64::MAX,
            },
            NodeMeta {
                external_id: 2,
                label_bitmap: 0b0000_0010, // label 1 only
                property_offset: 0,
                created_lsn: 1,
                deleted_lsn: u64::MAX,
            },
            NodeMeta {
                external_id: 3,
                label_bitmap: 0b0000_0101, // labels 0 and 2
                property_offset: 0,
                created_lsn: 1,
                deleted_lsn: u64::MAX,
            },
        ];

        let idx = LabelIndex::build(&node_meta);
        assert_eq!(idx.label_count(), 3); // labels 0, 1, 2

        // Label 0: rows 0, 2
        let bm = idx.nodes_with_label(0).expect("label 0 exists");
        assert!(bm.contains(0));
        assert!(!bm.contains(1));
        assert!(bm.contains(2));
        assert_eq!(bm.len(), 2);

        // Label 1: rows 0, 1
        let bm = idx.nodes_with_label(1).expect("label 1 exists");
        assert!(bm.contains(0));
        assert!(bm.contains(1));
        assert!(!bm.contains(2));
        assert_eq!(bm.len(), 2);

        // Label 2: row 2 only
        let bm = idx.nodes_with_label(2).expect("label 2 exists");
        assert!(!bm.contains(0));
        assert!(!bm.contains(1));
        assert!(bm.contains(2));
        assert_eq!(bm.len(), 1);

        // Non-existent label
        assert!(idx.nodes_with_label(31).is_none());
    }

    #[test]
    fn test_label_index_empty() {
        let idx = LabelIndex::build(&[]);
        assert!(idx.is_empty());
        assert_eq!(idx.label_count(), 0);
    }

    #[test]
    fn test_label_index_no_labels() {
        let node_meta = vec![NodeMeta {
            external_id: 1,
            label_bitmap: 0, // no labels
            property_offset: 0,
            created_lsn: 1,
            deleted_lsn: u64::MAX,
        }];
        let idx = LabelIndex::build(&node_meta);
        assert!(idx.is_empty());
    }

    // --- EdgeTypeIndex tests ---

    #[test]
    fn test_edge_type_index_build_and_query() {
        let edge_meta = vec![
            EdgeMeta {
                edge_type: 1,
                flags: 0,
                property_offset: 0,
            },
            EdgeMeta {
                edge_type: 2,
                flags: 0,
                property_offset: 0,
            },
            EdgeMeta {
                edge_type: 1,
                flags: 0,
                property_offset: 0,
            },
            EdgeMeta {
                edge_type: 3,
                flags: 0,
                property_offset: 0,
            },
            EdgeMeta {
                edge_type: 2,
                flags: 0,
                property_offset: 0,
            },
        ];

        let idx = EdgeTypeIndex::build(&edge_meta);
        assert_eq!(idx.type_count(), 3); // types 1, 2, 3

        // Type 1: indices 0, 2
        let bm = idx.edges_of_type(1).expect("type 1 exists");
        assert!(bm.contains(0));
        assert!(bm.contains(2));
        assert_eq!(bm.len(), 2);

        // Type 2: indices 1, 4
        let bm = idx.edges_of_type(2).expect("type 2 exists");
        assert!(bm.contains(1));
        assert!(bm.contains(4));
        assert_eq!(bm.len(), 2);

        // Type 3: index 3
        let bm = idx.edges_of_type(3).expect("type 3 exists");
        assert!(bm.contains(3));
        assert_eq!(bm.len(), 1);

        // Non-existent type
        assert!(idx.edges_of_type(99).is_none());
    }

    #[test]
    fn test_edge_type_index_empty() {
        let idx = EdgeTypeIndex::build(&[]);
        assert!(idx.is_empty());
    }

    // --- MphNodeIndex tests ---

    #[test]
    fn test_mph_build_and_lookup() {
        use slotmap::SlotMap;

        // Create some NodeKeys via SlotMap.
        let mut sm: SlotMap<NodeKey, ()> = SlotMap::with_key();
        let keys: Vec<NodeKey> = (0..100).map(|_| sm.insert(())).collect();

        let mph = MphNodeIndex::build(&keys);
        assert_eq!(mph.len(), 100);
        assert!(!mph.is_empty());

        // Every key should resolve to its correct row.
        for (expected_row, key) in keys.iter().enumerate() {
            let row = mph.lookup(*key).expect("key should be found");
            assert_eq!(row as usize, expected_row);
        }
    }

    #[test]
    fn test_mph_rejects_unknown_key() {
        use slotmap::SlotMap;

        let mut sm: SlotMap<NodeKey, ()> = SlotMap::with_key();
        let keys: Vec<NodeKey> = (0..10).map(|_| sm.insert(())).collect();
        let unknown = sm.insert(()); // not in the index

        let mph = MphNodeIndex::build(&keys);
        assert!(mph.lookup(unknown).is_none());
    }

    #[test]
    fn test_mph_empty() {
        let mph = MphNodeIndex::build(&[]);
        assert!(mph.is_empty());
        assert_eq!(mph.len(), 0);
    }

    // --- PropertyIndex tests ---

    #[test]
    fn test_property_index_insert_and_range() {
        let mut idx = PropertyIndex::new(0);
        // Insert timestamps for 5 nodes
        idx.insert(100.0, 0);
        idx.insert(200.0, 1);
        idx.insert(300.0, 2);
        idx.insert(400.0, 3);
        idx.insert(500.0, 4);

        // Range [200, 400] should return rows 1, 2, 3
        let result = idx.range_query(200.0, 400.0);
        assert_eq!(result.len(), 3);
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(result.contains(3));

        // Range [100, 100] should return only row 0
        let result = idx.range_query(100.0, 100.0);
        assert_eq!(result.len(), 1);
        assert!(result.contains(0));
    }

    #[test]
    fn test_property_index_gt_gte_lt() {
        let mut idx = PropertyIndex::new(0);
        idx.insert(10.0, 0);
        idx.insert(20.0, 1);
        idx.insert(30.0, 2);
        idx.insert(40.0, 3);

        // gt(20) -> rows 2, 3
        let result = idx.gt(20.0);
        assert_eq!(result.len(), 2);
        assert!(result.contains(2));
        assert!(result.contains(3));

        // gte(20) -> rows 1, 2, 3
        let result = idx.gte(20.0);
        assert_eq!(result.len(), 3);
        assert!(result.contains(1));

        // lt(30) -> rows 0, 1
        let result = idx.lt(30.0);
        assert_eq!(result.len(), 2);
        assert!(result.contains(0));
        assert!(result.contains(1));
    }

    #[test]
    fn test_property_index_duplicate_values() {
        let mut idx = PropertyIndex::new(0);
        idx.insert(42.0, 0);
        idx.insert(42.0, 1);
        idx.insert(42.0, 2);

        let result = idx.range_query(42.0, 42.0);
        assert_eq!(result.len(), 3);
        assert_eq!(idx.distinct_values(), 1);
        assert_eq!(idx.total_entries(), 3);
    }

    #[test]
    fn test_property_index_remove() {
        let mut idx = PropertyIndex::new(0);
        idx.insert(10.0, 0);
        idx.insert(10.0, 1);
        idx.insert(20.0, 2);

        idx.remove(10.0, 0);
        let result = idx.range_query(10.0, 10.0);
        assert_eq!(result.len(), 1);
        assert!(result.contains(1));

        // Remove last entry for value 10.0 -- value should be cleaned up.
        idx.remove(10.0, 1);
        assert_eq!(idx.distinct_values(), 1); // only 20.0 remains
    }

    #[test]
    fn test_property_index_empty() {
        let idx = PropertyIndex::new(0);
        assert!(idx.is_empty());
        assert_eq!(idx.distinct_values(), 0);
        assert_eq!(idx.total_entries(), 0);

        let result = idx.range_query(0.0, 100.0);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_property_index_exclusive_hi() {
        let mut idx = PropertyIndex::new(0);
        idx.insert(10.0, 0);
        idx.insert(20.0, 1);
        idx.insert(30.0, 2);

        // [10, 30) should return rows 0 and 1
        let result = idx.range_query_exclusive_hi(10.0, 30.0);
        assert_eq!(result.len(), 2);
        assert!(result.contains(0));
        assert!(result.contains(1));
        assert!(!result.contains(2));
    }
}
