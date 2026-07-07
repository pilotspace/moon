//! Graph index structures for O(1) label, edge-type, node-ID, and property lookups.
//!
//! - `LabelIndex`: per-label Roaring bitmap of CSR row indices
//! - `EdgeTypeIndex`: per-edge-type Roaring bitmap of edge indices in col_indices
//! - `MphNodeIndex`: boomphf minimal perfect hash for NodeKey -> CSR row (3 bits/key)
//! - `PropertyIndex`: BTreeMap<f64, RoaringBitmap> for numeric range queries

use std::collections::{BTreeMap, HashMap};

use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;
use smallvec::SmallVec;

use crate::graph::fasthash::FxHashMap;
use crate::graph::types::{EdgeMeta, NodeKey, NodeMeta, PropertyMap, PropertyValue};

/// Normalize a property value into f64 numeric space: `Int`/`Float` as-is,
/// `Bool` as 0.0/1.0. `String`/`Bytes` return `None` (indexed by xxh64 hash
/// instead — see [`SegmentPropertyIndexes`] / [`MutablePropertyIndex`]).
///
/// Single source of truth for "what counts as numerically equal" shared by
/// BOTH the frozen tier (`SegmentPropertyIndexes::build`/`rows_eq`) and the
/// mutable tier (`MutablePropertyIndex`) so the two tiers can never silently
/// drift on equality semantics.
#[inline]
fn normalize_numeric(value: &PropertyValue) -> Option<f64> {
    match value {
        PropertyValue::Int(i) => Some(*i as f64),
        PropertyValue::Float(f) => Some(*f),
        PropertyValue::Bool(b) => Some(u8::from(*b) as f64),
        PropertyValue::String(_) | PropertyValue::Bytes(_) => None,
    }
}

// ---------------------------------------------------------------------------
// LabelIndex
// ---------------------------------------------------------------------------

/// Per-label Roaring bitmap mapping label ID -> set of CSR row indices.
///
/// Built during CSR construction from two sources: each node's `label_bitmap`
/// (the u32 fast path for labels 0-31) AND the per-segment `label_overflow`
/// map (labels >= 32, version >= 4). Together they index every `u16` label id,
/// so `nodes_with_label` resolves all labels without the historical 32 cap.
#[derive(Debug, Clone)]
pub struct LabelIndex {
    /// label_id -> bitmap of node row indices carrying that label.
    labels: HashMap<u16, RoaringBitmap>,
}

impl LabelIndex {
    /// Build a label index from CSR node metadata and the label-overflow map.
    ///
    /// Iterates once over `node_meta`, extracting set bits from each node's
    /// `label_bitmap` (labels 0-31), then folds in `overflow` (row -> labels of
    /// id 32 and up) so every `u16` label id is matchable. `overflow` is keyed
    /// by the same CSR row index as `node_meta`; pass an empty map for segments
    /// with no labels of id 32 or higher (version 3 and earlier, or sparse).
    pub fn build(node_meta: &[NodeMeta], overflow: &HashMap<u32, SmallVec<[u16; 4]>>) -> Self {
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
        // Fold in labels >= 32 from the overflow store (sparse, keyed by row).
        for (&row, label_ids) in overflow {
            for &label in label_ids {
                labels
                    .entry(label)
                    .or_insert_with(RoaringBitmap::new)
                    .insert(row);
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

    /// Less-than-or-equal query: returns rows where property <= threshold.
    pub fn lte(&self, threshold: f64) -> RoaringBitmap {
        let key = OrderedFloat(threshold);
        let mut result = RoaringBitmap::new();
        for (_, bitmap) in self.tree.range(..=key) {
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

    /// Approximate resident bytes: one `OrderedFloat<f64>` key plus each
    /// bitmap's `serialized_size()` (roaring's own compressed-container
    /// estimate -- close enough for the elastic memory budget, which only
    /// needs a monotonic signal, not exact byte accounting).
    pub fn resident_bytes(&self) -> usize {
        self.tree
            .values()
            .map(|bm| std::mem::size_of::<OrderedFloat<f64>>() + bm.serialized_size())
            .sum()
    }
}

// ---------------------------------------------------------------------------
// SegmentPropertyIndexes
// ---------------------------------------------------------------------------

/// Per-segment property indexes over CSR rows, built from the v5
/// node-property blob at first use (segments are immutable, so the index
/// never needs maintenance — this replaces the dead insert-time
/// `NamedGraph.property_indexes`, which indexed the wrong row space and was
/// never read).
///
/// Numeric values (Int / Float / Bool as 0-1) share one per-property B-tree
/// for equality AND range queries; String / Bytes values index by xxh64 hash
/// for equality. Hash collisions (and Bool-vs-Int aliasing) can only yield
/// SUPERSET candidate sets — callers keep their residual Filter downstream,
/// so a collision costs a re-check, never a wrong row.
///
/// The build is exhaustive over every row's properties, so an absent
/// (property, value) means NO row matches: lookups return empty bitmaps,
/// not a fall-back-to-scan signal. Pre-v5 segments have an empty blob and
/// genuinely hold no properties, so "empty" is correct there too.
#[derive(Debug, Default)]
pub struct SegmentPropertyIndexes {
    /// prop_id -> numeric B-tree (Int/Float/Bool normalized to f64).
    numeric: HashMap<u16, PropertyIndex>,
    /// prop_id -> xxh64(bytes) -> rows (String/Bytes equality).
    strings: HashMap<u16, HashMap<u64, RoaringBitmap>>,
}

impl SegmentPropertyIndexes {
    /// Build from CSR node metadata + the v5 node-property blob.
    pub fn build(node_meta: &[NodeMeta], node_props_blob: &[u8]) -> Self {
        let mut numeric: HashMap<u16, PropertyIndex> = HashMap::new();
        let mut strings: HashMap<u16, HashMap<u64, RoaringBitmap>> = HashMap::new();
        for (row, nm) in node_meta.iter().enumerate() {
            if nm.property_offset == 0 {
                continue; // no property record for this row
            }
            let props =
                crate::graph::csr::props::decode_node_props(node_props_blob, nm.property_offset);
            for (pid, val) in &props {
                if let Some(v) = normalize_numeric(val) {
                    numeric
                        .entry(*pid)
                        .or_insert_with(|| PropertyIndex::new(*pid))
                        .insert(v, row as u32);
                } else if let PropertyValue::String(s) | PropertyValue::Bytes(s) = val {
                    strings
                        .entry(*pid)
                        .or_default()
                        .entry(xxhash_rust::xxh64::xxh64(s, 0))
                        .or_default()
                        .insert(row as u32);
                }
            }
        }
        numeric.shrink_to_fit();
        strings.shrink_to_fit();
        Self { numeric, strings }
    }

    /// Rows whose property `prop_id` equals `value` (superset semantics for
    /// hashed strings / Bool-Int aliasing — see type docs).
    pub fn rows_eq(&self, prop_id: u16, value: &PropertyValue) -> RoaringBitmap {
        match normalize_numeric(value) {
            Some(v) => self.numeric_eq(prop_id, v),
            None => match value {
                PropertyValue::String(s) | PropertyValue::Bytes(s) => self
                    .strings
                    .get(&prop_id)
                    .and_then(|m| m.get(&xxhash_rust::xxh64::xxh64(s, 0)))
                    .cloned()
                    .unwrap_or_default(),
                PropertyValue::Int(_) | PropertyValue::Float(_) | PropertyValue::Bool(_) => {
                    RoaringBitmap::new()
                }
            },
        }
    }

    /// Rows whose numeric property `prop_id` falls in `[min, max]`.
    pub fn rows_range(&self, prop_id: u16, min: f64, max: f64) -> RoaringBitmap {
        self.numeric
            .get(&prop_id)
            .map(|ix| ix.range_query(min, max))
            .unwrap_or_default()
    }

    /// True when no property is indexed at all (segment without v5 blob).
    pub fn is_empty(&self) -> bool {
        self.numeric.is_empty() && self.strings.is_empty()
    }

    /// Approximate resident bytes across every numeric B-tree and string
    /// hash-bucket bitmap. Always heap-owned: built lazily on first use from
    /// the (possibly mmap-backed) property blob, but the index ITSELF is
    /// never mmap'd -- see `CsrStorage::resident_bytes`, which is the only
    /// caller and where the mmap-vs-heap distinction for the SOURCE blob is
    /// applied.
    pub fn resident_bytes(&self) -> usize {
        let numeric: usize = self
            .numeric
            .values()
            .map(PropertyIndex::resident_bytes)
            .sum();
        let strings: usize = self
            .strings
            .values()
            .flat_map(|inner| inner.values())
            .map(|bm| std::mem::size_of::<u64>() + bm.serialized_size())
            .sum();
        numeric + strings
    }

    /// The numeric B-tree for `prop_id`, if any row indexed a numeric value
    /// under it. `None` means no row can satisfy a numeric range on this
    /// property (the build is exhaustive — see type docs).
    pub fn numeric_index(&self, prop_id: u16) -> Option<&PropertyIndex> {
        self.numeric.get(&prop_id)
    }

    fn numeric_eq(&self, prop_id: u16, v: f64) -> RoaringBitmap {
        self.numeric
            .get(&prop_id)
            .map(|ix| ix.range_query(v, v))
            .unwrap_or_default()
    }
}

// ---------------------------------------------------------------------------
// MutablePropertyIndex
// ---------------------------------------------------------------------------

/// Incrementally-maintained property index over `MemGraph`'s mutable tier.
///
/// Mirrors [`SegmentPropertyIndexes`]'s numeric-BTree / string-hash split,
/// but:
///   - keys by [`NodeKey`] directly (the mutable tier has no stable dense
///     row space — NodeKeys are slotmap ffi-encoded, sparse, and reused
///     only within a generation; reusing a `RoaringBitmap`-of-row scheme
///     here would reintroduce an ABA hazard the generational key design
///     exists to prevent — see the mutable-property-index design doc,
///     rejected alternatives).
///   - is maintained INCREMENTALLY on every write (insert/remove) instead
///     of built once, exhaustively, at freeze time (the mutable tier is, by
///     definition, still being written to).
///   - is NOT keyed by label, same as `SegmentPropertyIndexes` — label is
///     applied as a separate check at query time.
///   - has EXACT (not superset) removal semantics: `remove` deletes the
///     precise `(prop_id, value, key)` triple. The SUPERSET behavior lives
///     entirely in string-hash collisions and Bool/Int aliasing (identical
///     to `SegmentPropertyIndexes`) — callers keep a residual Filter
///     downstream regardless, so an index hit can over-select, never
///     under-select.
///
/// Owned by `MemGraph` (not `NamedGraph`) — same lifetime and same
/// single-writer, no-lock ownership as `nodes`/`node_order` (the shard
/// thread exclusively owns `MemGraph`, so this index needs zero
/// synchronization: no `RwLock`, no atomics).
///
/// Forward-compat note: any FUTURE property-mutation call site (e.g. a
/// Cypher `REMOVE n.prop` clause, which does not exist yet) MUST route
/// through `MemGraph::set_node_property`/`remove_node_property` rather than
/// poking `MutableNode.properties` directly, or this index goes stale.
#[derive(Debug, Default)]
pub struct MutablePropertyIndex {
    /// prop_id -> sorted numeric value -> node keys with that value.
    /// `SmallVec<[NodeKey; 2]>` because point-lookup properties (ids) are
    /// near-unique in practice; low-cardinality properties spill to heap
    /// transparently — no correctness difference, just an allocation, same
    /// as today's per-node `SmallVec<4>` properties.
    numeric: FxHashMap<u16, BTreeMap<OrderedFloat<f64>, SmallVec<[NodeKey; 2]>>>,
    /// prop_id -> xxh64(bytes) -> node keys (String/Bytes equality).
    strings: FxHashMap<u16, FxHashMap<u64, SmallVec<[NodeKey; 2]>>>,
}

impl MutablePropertyIndex {
    /// Insert `(prop_id, value) -> key` into the index.
    pub fn insert(&mut self, pid: u16, value: &PropertyValue, key: NodeKey) {
        match normalize_numeric(value) {
            Some(v) => self
                .numeric
                .entry(pid)
                .or_default()
                .entry(OrderedFloat(v))
                .or_default()
                .push(key),
            None => {
                if let PropertyValue::String(s) | PropertyValue::Bytes(s) = value {
                    self.strings
                        .entry(pid)
                        .or_default()
                        .entry(xxhash_rust::xxh64::xxh64(s, 0))
                        .or_default()
                        .push(key);
                }
            }
        }
    }

    /// Remove the exact `(prop_id, value, key)` triple from the index.
    /// Cleans up now-empty buckets/prop entries so the index never leaks
    /// stale, permanently-empty containers across a long server lifetime.
    pub fn remove(&mut self, pid: u16, value: &PropertyValue, key: NodeKey) {
        match normalize_numeric(value) {
            Some(v) => {
                if let Some(tree) = self.numeric.get_mut(&pid) {
                    let ordered = OrderedFloat(v);
                    let mut drop_value = false;
                    if let Some(bucket) = tree.get_mut(&ordered) {
                        bucket.retain(|k| *k != key);
                        drop_value = bucket.is_empty();
                    }
                    if drop_value {
                        tree.remove(&ordered);
                    }
                    if tree.is_empty() {
                        self.numeric.remove(&pid);
                    }
                }
            }
            None => {
                if let PropertyValue::String(s) | PropertyValue::Bytes(s) = value {
                    if let Some(buckets) = self.strings.get_mut(&pid) {
                        let hash = xxhash_rust::xxh64::xxh64(s, 0);
                        let mut drop_hash = false;
                        if let Some(bucket) = buckets.get_mut(&hash) {
                            bucket.retain(|k| *k != key);
                            drop_hash = bucket.is_empty();
                        }
                        if drop_hash {
                            buckets.remove(&hash);
                        }
                        if buckets.is_empty() {
                            self.strings.remove(&pid);
                        }
                    }
                }
            }
        }
    }

    /// Index every entry in a node's property map (creation / undelete).
    pub fn index_node(&mut self, key: NodeKey, props: &PropertyMap) {
        for (pid, val) in props {
            self.insert(*pid, val, key);
        }
    }

    /// Unindex every entry in a node's property map (soft-delete /
    /// replace-in-place overwrite). `freeze()` uses `clear()` instead — see
    /// its doc comment.
    pub fn unindex_node(&mut self, key: NodeKey, props: &PropertyMap) {
        for (pid, val) in props {
            self.remove(*pid, val, key);
        }
    }

    /// Zero-alloc equality probe. Returns a borrowed slice (`&[]` when
    /// absent) — no candidate materialization until the caller chooses to
    /// (`.to_vec()`).
    pub fn keys_eq(&self, pid: u16, value: &PropertyValue) -> &[NodeKey] {
        match normalize_numeric(value) {
            Some(v) => self
                .numeric
                .get(&pid)
                .and_then(|tree| tree.get(&OrderedFloat(v)))
                .map(SmallVec::as_slice)
                .unwrap_or(&[]),
            None => match value {
                PropertyValue::String(s) | PropertyValue::Bytes(s) => self
                    .strings
                    .get(&pid)
                    .and_then(|m| m.get(&xxhash_rust::xxh64::xxh64(s, 0)))
                    .map(SmallVec::as_slice)
                    .unwrap_or(&[]),
                PropertyValue::Int(_) | PropertyValue::Float(_) | PropertyValue::Bool(_) => &[],
            },
        }
    }

    /// Range probe: `[min, max]` inclusive over the numeric B-tree for
    /// `prop_id`. Allocates (BTree range union) — same cost class as
    /// `PropertyIndex::range_query`; range queries are not the point-lookup
    /// hot path this index primarily targets.
    pub fn keys_range(&self, pid: u16, min: f64, max: f64) -> Vec<NodeKey> {
        let Some(tree) = self.numeric.get(&pid) else {
            return Vec::new();
        };
        let lo = OrderedFloat(min);
        let hi = OrderedFloat(max);
        tree.range(lo..=hi)
            .flat_map(|(_, keys)| keys.iter().copied())
            .collect()
    }

    /// Drop every indexed entry. `MemGraph::freeze()` drains ALL of
    /// `self.nodes` unconditionally (dead or alive), so the entire index is
    /// dead the instant freeze starts — `O(#buckets)`, not
    /// `O(#nodes * #props)`.
    pub fn clear(&mut self) {
        self.numeric.clear();
        self.strings.clear();
    }

    /// True when no property is indexed at all.
    pub fn is_empty(&self) -> bool {
        self.numeric.is_empty() && self.strings.is_empty()
    }

    /// Approximate resident bytes: feeds `MemGraph::resident_bytes()`'s
    /// elastic-memory-budget accounting (same "monotonic signal, not exact
    /// byte accounting" precedent as `PropertyIndex::resident_bytes`).
    pub fn resident_bytes(&self) -> usize {
        let numeric: usize = self
            .numeric
            .values()
            .flat_map(|tree| tree.values())
            .map(|bucket| {
                std::mem::size_of::<OrderedFloat<f64>>()
                    + bucket.len() * std::mem::size_of::<NodeKey>()
            })
            .sum();
        let strings: usize = self
            .strings
            .values()
            .flat_map(|inner| inner.values())
            .map(|bucket| {
                std::mem::size_of::<u64>() + bucket.len() * std::mem::size_of::<NodeKey>()
            })
            .sum();
        numeric + strings
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::types::{EdgeMeta, NodeMeta};
    use bytes::Bytes;

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
                valid_from: 0,
                valid_to: i64::MAX,
            },
            NodeMeta {
                external_id: 2,
                label_bitmap: 0b0000_0010, // label 1 only
                property_offset: 0,
                created_lsn: 1,
                deleted_lsn: u64::MAX,
                valid_from: 0,
                valid_to: i64::MAX,
            },
            NodeMeta {
                external_id: 3,
                label_bitmap: 0b0000_0101, // labels 0 and 2
                property_offset: 0,
                created_lsn: 1,
                deleted_lsn: u64::MAX,
                valid_from: 0,
                valid_to: i64::MAX,
            },
        ];

        let idx = LabelIndex::build(&node_meta, &HashMap::new());
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
        let idx = LabelIndex::build(&[], &HashMap::new());
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
            valid_from: 0,
            valid_to: i64::MAX,
        }];
        let idx = LabelIndex::build(&node_meta, &HashMap::new());
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

        // lte(30) -> rows 0, 1, 2 (inclusive upper bound)
        let result = idx.lte(30.0);
        assert_eq!(result.len(), 3);
        assert!(result.contains(2));

        // lte below the minimum -> empty
        assert!(idx.lte(9.0).is_empty());
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

    // --- MutablePropertyIndex tests ---

    fn make_keys(n: usize) -> Vec<NodeKey> {
        use slotmap::SlotMap;
        let mut sm: SlotMap<NodeKey, ()> = SlotMap::with_key();
        (0..n).map(|_| sm.insert(())).collect()
    }

    #[test]
    fn test_mutable_index_insert_eq_lookup() {
        let keys = make_keys(3);
        let mut idx = MutablePropertyIndex::default();
        idx.insert(0, &PropertyValue::Int(1), keys[0]);
        idx.insert(0, &PropertyValue::Int(2), keys[1]);
        idx.insert(0, &PropertyValue::Int(3), keys[2]);

        assert_eq!(idx.keys_eq(0, &PropertyValue::Int(1)), &[keys[0]]);
        assert_eq!(idx.keys_eq(0, &PropertyValue::Int(2)), &[keys[1]]);
        assert_eq!(idx.keys_eq(0, &PropertyValue::Int(3)), &[keys[2]]);
        assert!(idx.keys_eq(0, &PropertyValue::Int(4)).is_empty());
    }

    #[test]
    fn test_mutable_index_string_property_eq() {
        let keys = make_keys(2);
        let mut idx = MutablePropertyIndex::default();
        idx.insert(
            1,
            &PropertyValue::String(Bytes::from_static(b"alice")),
            keys[0],
        );
        idx.insert(
            1,
            &PropertyValue::String(Bytes::from_static(b"bob")),
            keys[1],
        );

        assert_eq!(
            idx.keys_eq(1, &PropertyValue::String(Bytes::from_static(b"alice"))),
            &[keys[0]]
        );
        assert_eq!(
            idx.keys_eq(1, &PropertyValue::String(Bytes::from_static(b"bob"))),
            &[keys[1]]
        );
        assert!(
            idx.keys_eq(1, &PropertyValue::String(Bytes::from_static(b"carol")))
                .is_empty()
        );
    }

    #[test]
    fn test_mutable_index_update_moves_bucket() {
        let keys = make_keys(1);
        let k = keys[0];
        let mut idx = MutablePropertyIndex::default();
        idx.insert(0, &PropertyValue::Int(10), k);
        assert_eq!(idx.keys_eq(0, &PropertyValue::Int(10)), &[k]);

        // Simulate an update: remove old value, insert new value.
        idx.remove(0, &PropertyValue::Int(10), k);
        idx.insert(0, &PropertyValue::Int(20), k);

        assert!(idx.keys_eq(0, &PropertyValue::Int(10)).is_empty());
        assert_eq!(idx.keys_eq(0, &PropertyValue::Int(20)), &[k]);
    }

    #[test]
    fn test_mutable_index_remove_is_exact() {
        let keys = make_keys(1);
        let k = keys[0];
        let mut idx = MutablePropertyIndex::default();
        idx.insert(0, &PropertyValue::Int(42), k);
        idx.remove(0, &PropertyValue::Int(42), k);
        assert!(idx.keys_eq(0, &PropertyValue::Int(42)).is_empty());
        // Bucket cleanup: index should be fully empty, not just the value gone.
        assert!(idx.is_empty());
    }

    #[test]
    fn test_mutable_index_range_query() {
        let keys = make_keys(5);
        let mut idx = MutablePropertyIndex::default();
        for (i, &k) in keys.iter().enumerate() {
            idx.insert(0, &PropertyValue::Int((i as i64 + 1) * 10), k);
        }
        // Values: 10, 20, 30, 40, 50. Range [20, 40] -> keys[1..=3].
        let mut result = idx.keys_range(0, 20.0, 40.0);
        result.sort_by_key(|k| format!("{k:?}"));
        let mut expected = vec![keys[1], keys[2], keys[3]];
        expected.sort_by_key(|k| format!("{k:?}"));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_mutable_index_bool_int_aliasing_is_superset() {
        let keys = make_keys(1);
        let k = keys[0];
        let mut idx = MutablePropertyIndex::default();
        idx.insert(0, &PropertyValue::Bool(true), k);
        // Bool(true) normalizes to 1.0, same bucket as Int(1) -- documented
        // superset behavior (residual Filter downstream disambiguates).
        assert_eq!(idx.keys_eq(0, &PropertyValue::Int(1)), &[k]);
        assert_eq!(idx.keys_eq(0, &PropertyValue::Bool(true)), &[k]);
    }

    #[test]
    fn test_mutable_index_resident_bytes_grows_with_inserts() {
        let keys = make_keys(10);
        let mut idx = MutablePropertyIndex::default();
        let before = idx.resident_bytes();
        for (i, &k) in keys.iter().enumerate() {
            idx.insert(0, &PropertyValue::Int(i as i64), k);
        }
        assert!(idx.resident_bytes() > before);
    }
}
