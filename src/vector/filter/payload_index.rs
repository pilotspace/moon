use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;

use bytes::Bytes;
use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;

use super::expression::FilterExpr;

/// The values one document wrote to one field.
///
/// `SmallVec<[_; 1]>` because the overwhelmingly common case is a document
/// holding exactly one value per field; a multi-value tag field stays correct,
/// it just spills to the heap.
#[derive(Default)]
struct DocFieldValues {
    tags: smallvec::SmallVec<[Bytes; 1]>,
    numerics: smallvec::SmallVec<[OrderedFloat<f64>; 1]>,
}

/// Payload index maintaining Roaring bitmaps per tag value and numeric value.
///
/// Each field gets its own index: tags use `HashMap<value, bitmap>`,
/// numerics use `BTreeMap<value, bitmap>` for efficient range queries.
pub struct PayloadIndex {
    /// field_name -> { tag_value -> bitmap of internal_ids }
    tag_indexes: HashMap<Bytes, HashMap<Bytes, RoaringBitmap>>,
    /// field_name -> { numeric_value -> bitmap of internal_ids }
    numeric_indexes: HashMap<Bytes, BTreeMap<OrderedFloat<f64>, RoaringBitmap>>,
    /// FORWARD index: internal_id -> field -> the values that document wrote.
    ///
    /// moon#614. Retiring a document used to walk EVERY distinct value bitmap
    /// in the field, because nothing recorded which ones the document was
    /// actually in — `O(distinct values)` per document regardless of how many
    /// values that document had, so `O(n^2)` across a bulk update of a
    /// high-cardinality field (`sku`, `price`, any coordinate). This makes the
    /// retire path proportional to what the document actually wrote.
    ///
    /// Same remedy as moon#613 applied to the text index, and the memory
    /// trade-off is the same and worth stating: one entry per
    /// (document, field) that was written, holding the values themselves.
    /// Tag values are `Bytes`, so they share the buffer already stored as the
    /// key of `tag_indexes`; numerics are 8 bytes each.
    doc_values: HashMap<u32, HashMap<Bytes, DocFieldValues>>,
    /// Full-text indexes (feature-gated behind `text-index`)
    #[cfg(feature = "text-index")]
    text_indexes: crate::vector::filter::text_index::TextIndex,
}

impl PayloadIndex {
    /// Create an empty payload index.
    pub fn new() -> Self {
        Self {
            tag_indexes: HashMap::new(),
            numeric_indexes: HashMap::new(),
            doc_values: HashMap::new(),
            #[cfg(feature = "text-index")]
            text_indexes: crate::vector::filter::text_index::TextIndex::new(),
        }
    }

    /// Insert a tag value for the given internal vector ID.
    pub fn insert_tag(&mut self, field: &Bytes, value: &Bytes, internal_id: u32) {
        self.tag_indexes
            .entry(field.clone())
            .or_default()
            .entry(value.clone())
            .or_default()
            .insert(internal_id);
        // Forward index (moon#614). Deduped: re-inserting the same value for
        // the same document is idempotent in the bitmap, so it must be
        // idempotent here too or the retire list grows without bound.
        let slot = self
            .doc_values
            .entry(internal_id)
            .or_default()
            .entry(field.clone())
            .or_default();
        if !slot.tags.contains(value) {
            slot.tags.push(value.clone());
        }
    }

    /// Insert a numeric value for the given internal vector ID.
    pub fn insert_numeric(&mut self, field: &Bytes, value: f64, internal_id: u32) {
        self.numeric_indexes
            .entry(field.clone())
            .or_default()
            .entry(OrderedFloat(value))
            .or_default()
            .insert(internal_id);
        // Forward index (moon#614). `insert_geo` routes through here for
        // `{field}__lat` / `{field}__lon`, so geo sub-fields are recorded under
        // their own names with no extra bookkeeping.
        let slot = self
            .doc_values
            .entry(internal_id)
            .or_default()
            .entry(field.clone())
            .or_default();
        let v = OrderedFloat(value);
        if !slot.numerics.contains(&v) {
            slot.numerics.push(v);
        }
    }

    /// Insert geo coordinates for the given internal vector ID.
    ///
    /// Stores lat/lon as two separate numeric sub-fields (`{field}__lat` and `{field}__lon`)
    /// so that range queries can produce candidate bitmaps before Haversine post-filter.
    pub fn insert_geo(&mut self, field: &Bytes, lat: f64, lon: f64, internal_id: u32) {
        let field_str = std::str::from_utf8(field).unwrap_or("");
        let lat_field = Bytes::from(format!("{field_str}__lat"));
        let lon_field = Bytes::from(format!("{field_str}__lon"));
        self.insert_numeric(&lat_field, lat, internal_id);
        self.insert_numeric(&lon_field, lon, internal_id);
    }

    /// Insert a text value into the full-text index for the given field and internal vector ID.
    ///
    /// Feature-gated: no-op when `text-index` feature is disabled.
    #[cfg(feature = "text-index")]
    pub fn insert_text(&mut self, field: &Bytes, text: &[u8], internal_id: u32) {
        self.text_indexes.insert(field, text, internal_id);
    }

    /// Insert a text value into the full-text index (stub when feature disabled).
    #[cfg(not(feature = "text-index"))]
    pub fn insert_text(&mut self, _field: &Bytes, _text: &[u8], _internal_id: u32) {
        // No-op: text-index feature not enabled
    }

    /// Retire `internal_id` from ONE field's bitmaps, and from that field's geo
    /// sub-fields (`{field}__lat`, `{field}__lon`) if it is a geo field.
    ///
    /// `O(values this document wrote to the field)` — effectively O(1), since a
    /// document almost always holds one value per field. It used to be
    /// `O(distinct values in the field)`: with no record of which bitmaps the
    /// document was in, it visited all of them (moon#614). That is fine for
    /// `color` or `status` and quadratic for `sku` or `price`, where the
    /// distinct-value count grows with the corpus.
    ///
    /// Emptied bitmaps are dropped rather than left behind. Retaining them is
    /// how the pre-fix text index grew without bound (moon#613): a field that
    /// had once seen a million values kept paying for all million on every
    /// later retire and every search miss, long after the documents were gone.
    pub fn remove_field(&mut self, field: &Bytes, internal_id: u32) {
        // Geo writes two numeric sub-fields, so retiring the parent field must
        // retire those too. They are recorded in the forward index under their
        // own names, which is why this is a lookup rather than a sweep.
        let field_str = std::str::from_utf8(field).unwrap_or("");
        let lat_field = Bytes::from(format!("{field_str}__lat"));
        let lon_field = Bytes::from(format!("{field_str}__lon"));

        if let Some(fields) = self.doc_values.get_mut(&internal_id) {
            for f in [field, &lat_field, &lon_field] {
                if let Some(values) = fields.remove(f) {
                    Self::retire_values(
                        &mut self.tag_indexes,
                        &mut self.numeric_indexes,
                        f,
                        &values,
                        internal_id,
                    );
                }
            }
            if fields.is_empty() {
                self.doc_values.remove(&internal_id);
            }
        }

        #[cfg(feature = "text-index")]
        self.text_indexes.remove_field(field, internal_id);
    }

    /// Retire `internal_id` from every field it was written to (vector deletion).
    ///
    /// `O(values this document wrote)`, was `O(fields * distinct values)`.
    pub fn remove(&mut self, internal_id: u32) {
        if let Some(fields) = self.doc_values.remove(&internal_id) {
            for (f, values) in &fields {
                Self::retire_values(
                    &mut self.tag_indexes,
                    &mut self.numeric_indexes,
                    f,
                    values,
                    internal_id,
                );
            }
        }
        #[cfg(feature = "text-index")]
        self.text_indexes.remove(internal_id);
    }

    /// Drop `internal_id` from exactly the bitmaps named by `values`, and drop
    /// any bitmap (and any field map) the removal left empty.
    ///
    /// Free function over the two maps rather than `&mut self`, so the caller
    /// can hold a borrow of `doc_values` across the call.
    fn retire_values(
        tag_indexes: &mut HashMap<Bytes, HashMap<Bytes, RoaringBitmap>>,
        numeric_indexes: &mut HashMap<Bytes, BTreeMap<OrderedFloat<f64>, RoaringBitmap>>,
        field: &Bytes,
        values: &DocFieldValues,
        internal_id: u32,
    ) {
        if !values.tags.is_empty()
            && let Some(tag_map) = tag_indexes.get_mut(field)
        {
            for value in &values.tags {
                if let Some(bitmap) = tag_map.get_mut(value) {
                    bitmap.remove(internal_id);
                    if bitmap.is_empty() {
                        tag_map.remove(value);
                    }
                }
            }
            if tag_map.is_empty() {
                tag_indexes.remove(field);
            }
        }
        if !values.numerics.is_empty()
            && let Some(num_map) = numeric_indexes.get_mut(field)
        {
            for value in &values.numerics {
                if let Some(bitmap) = num_map.get_mut(value) {
                    bitmap.remove(internal_id);
                    if bitmap.is_empty() {
                        num_map.remove(value);
                    }
                }
            }
            if num_map.is_empty() {
                numeric_indexes.remove(field);
            }
        }
    }

    /// Evaluate a filter expression and return the bitmap of matching internal IDs.
    ///
    /// `total_vectors` is needed for NOT (complement against universe 0..total_vectors).
    pub fn evaluate_bitmap(&self, expr: &FilterExpr, total_vectors: u32) -> RoaringBitmap {
        match expr {
            FilterExpr::TagEq { field, value } => self
                .tag_indexes
                .get(field)
                .and_then(|m| m.get(value))
                .cloned()
                .unwrap_or_default(),

            FilterExpr::NumEq { field, value } => self
                .numeric_indexes
                .get(field)
                .and_then(|m| m.get(value))
                .cloned()
                .unwrap_or_default(),

            FilterExpr::NumRange {
                field,
                min,
                max,
                min_excl,
                max_excl,
            } => {
                let Some(btree) = self.numeric_indexes.get(field) else {
                    return RoaringBitmap::new();
                };
                // `BTreeMap::range` PANICS when start > end, or when start ==
                // end with either bound excluded -- and a panic on a shard
                // thread aborts the whole process (moon#664). The parser
                // rejects those shapes, but this stays total anyway: the bug
                // existed precisely because only one of the two ever checked,
                // and `FilterExpr` is constructible from more than one parser.
                if min > max || (min == max && (*min_excl || *max_excl)) {
                    return RoaringBitmap::new();
                }
                let lo = if *min_excl {
                    Bound::Excluded(*min)
                } else {
                    Bound::Included(*min)
                };
                let hi = if *max_excl {
                    Bound::Excluded(*max)
                } else {
                    Bound::Included(*max)
                };
                let mut result = RoaringBitmap::new();
                for (_k, bm) in btree.range((lo, hi)) {
                    result |= bm;
                }
                result
            }

            FilterExpr::BoolEq { field, value } => {
                let tag_val = if *value {
                    Bytes::from_static(b"true")
                } else {
                    Bytes::from_static(b"false")
                };
                self.tag_indexes
                    .get(field)
                    .and_then(|m| m.get(&tag_val))
                    .cloned()
                    .unwrap_or_default()
            }

            FilterExpr::GeoRadius {
                field,
                lon,
                lat,
                radius_km,
            } => {
                let field_str = std::str::from_utf8(field).unwrap_or("");
                let lat_field = Bytes::from(format!("{field_str}__lat"));
                let lon_field = Bytes::from(format!("{field_str}__lon"));

                // Bounding-box pre-filter (cheap BTreeMap range queries)
                let delta_lat = radius_km / 111.0;
                let cos_lat = lat.to_radians().cos();
                let delta_lon = if cos_lat.abs() < 1e-10 {
                    180.0 // near poles, use full longitude range
                } else {
                    radius_km / (111.0 * cos_lat)
                };

                let lat_min = OrderedFloat(*lat - delta_lat);
                let lat_max = OrderedFloat(*lat + delta_lat);
                let lon_min = OrderedFloat(*lon - delta_lon);
                let lon_max = OrderedFloat(*lon + delta_lon);

                let lat_bm = self
                    .numeric_indexes
                    .get(&lat_field)
                    .map(|btree| {
                        let mut bm = RoaringBitmap::new();
                        for (_k, b) in btree.range(lat_min..=lat_max) {
                            bm |= b;
                        }
                        bm
                    })
                    .unwrap_or_default();

                let lon_bm = self
                    .numeric_indexes
                    .get(&lon_field)
                    .map(|btree| {
                        let mut bm = RoaringBitmap::new();
                        for (_k, b) in btree.range(lon_min..=lon_max) {
                            bm |= b;
                        }
                        bm
                    })
                    .unwrap_or_default();

                let candidates = lat_bm & lon_bm;

                // Haversine post-filter for exact distance
                let mut result = RoaringBitmap::new();
                for id in candidates.iter() {
                    if let (Some(c_lat), Some(c_lon)) = (
                        self.lookup_numeric_value(&lat_field, id),
                        self.lookup_numeric_value(&lon_field, id),
                    ) {
                        if haversine_km(*lat, *lon, c_lat, c_lon) <= *radius_km {
                            result.insert(id);
                        }
                    }
                }
                result
            }

            FilterExpr::And(left, right) => {
                let left_bm = self.evaluate_bitmap(left, total_vectors);
                let right_bm = self.evaluate_bitmap(right, total_vectors);
                left_bm & right_bm
            }

            FilterExpr::Or(left, right) => {
                let left_bm = self.evaluate_bitmap(left, total_vectors);
                let right_bm = self.evaluate_bitmap(right, total_vectors);
                left_bm | right_bm
            }

            FilterExpr::Not(inner) => {
                let inner_bm = self.evaluate_bitmap(inner, total_vectors);
                let mut universe = RoaringBitmap::new();
                if total_vectors > 0 {
                    universe.insert_range(0..total_vectors);
                }
                universe - inner_bm
            }

            FilterExpr::TextMatch { field, terms } => {
                #[cfg(feature = "text-index")]
                {
                    // Tokenize/stem each query term through the same pipeline
                    let stemmed: Vec<String> = terms
                        .iter()
                        .flat_map(|t| {
                            crate::vector::filter::text_index::TextIndex::tokenize(
                                std::str::from_utf8(t).unwrap_or(""),
                            )
                        })
                        .collect();
                    self.text_indexes.search(field, &stemmed)
                }
                #[cfg(not(feature = "text-index"))]
                {
                    let _ = (field, terms);
                    // Text matching disabled — return empty bitmap
                    RoaringBitmap::new()
                }
            }
        }
    }
    /// Look up the numeric value stored for a specific internal_id in a given field.
    ///
    /// Iterates the BTreeMap entries for `field` to find one whose bitmap contains `internal_id`.
    /// Returns the first matching value (each internal_id typically has exactly one value per field).
    fn lookup_numeric_value(&self, field: &Bytes, internal_id: u32) -> Option<f64> {
        let btree = self.numeric_indexes.get(field)?;
        for (val, bm) in btree {
            if bm.contains(internal_id) {
                return Some(val.0);
            }
        }
        None
    }
}

/// Haversine distance between two points in kilometers.
///
/// Formula: a = sin^2(dlat/2) + cos(lat1)*cos(lat2)*sin^2(dlon/2)
///          d = 2 * R * asin(sqrt(a))
/// where R = 6371.0 km (Earth mean radius).
fn haversine_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R: f64 = 6371.0;
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let lat1_r = lat1.to_radians();
    let lat2_r = lat2.to_radians();
    let a = (dlat / 2.0).sin().powi(2) + lat1_r.cos() * lat2_r.cos() * (dlon / 2.0).sin().powi(2);
    2.0 * R * a.sqrt().asin()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Cost of retiring documents, as a field's DISTINCT-VALUE COUNT grows.
    ///
    /// `#[ignore]`d: this is a measurement, not an assertion — it exists so the
    /// claim in the commit message is reproducible rather than asserted. Same
    /// harness shape as `text_index::bench_removal_cost_vs_vocabulary` (moon#613),
    /// because moon#614 is the tag/numeric/geo analogue of that defect and the
    /// two numbers should be read the same way.
    ///
    /// One document per distinct value, so each document belongs to exactly ONE
    /// bitmap while the pre-fix `remove_field` must visit all `n` of them.
    /// Report is µs per retired document: **flat means the field's cardinality
    /// no longer matters; linear growth is the quadratic still being there.**
    /// A constant-factor win and a removed quadratic look very different on this
    /// curve, and only the second justifies the extra index.
    ///
    /// Run: `cargo test --release --lib bench_payload_removal -- --ignored --nocapture`
    #[test]
    #[ignore = "measurement harness; run explicitly with --nocapture"]
    fn bench_payload_removal_cost_vs_cardinality() {
        use std::time::Instant;
        let tagf = field("sku");
        let numf = field("price");
        println!(
            "{:<7} {:<12} {:<14} total",
            "docs", "tag µs/doc", "numeric µs/doc"
        );
        for docs in [250_u32, 500, 1000, 2000, 4000] {
            // --- tag field: one distinct value per document ---
            let mut idx = PayloadIndex::new();
            for d in 0..docs {
                idx.insert_tag(&tagf, &Bytes::from(format!("sku-{d}")), d);
            }
            let t0 = Instant::now();
            for d in 0..docs {
                idx.remove_field(&tagf, d);
            }
            let tag_el = t0.elapsed();

            // --- numeric field: one distinct value per document ---
            let mut idx2 = PayloadIndex::new();
            for d in 0..docs {
                idx2.insert_numeric(&numf, f64::from(d), d);
            }
            let t1 = Instant::now();
            for d in 0..docs {
                idx2.remove_field(&numf, d);
            }
            let num_el = t1.elapsed();

            let per = |e: std::time::Duration| e.as_secs_f64() * 1e6 / f64::from(docs);
            println!(
                "{:<7} {:<12.3} {:<14.3} {:?} / {:?}",
                docs,
                per(tag_el),
                per(num_el),
                tag_el,
                num_el
            );
        }
    }

    fn field(s: &str) -> Bytes {
        Bytes::from(s.to_owned())
    }

    #[test]
    fn test_tag_equality() {
        let mut idx = PayloadIndex::new();
        idx.insert_tag(&field("color"), &field("red"), 0);
        idx.insert_tag(&field("color"), &field("red"), 2);
        idx.insert_tag(&field("color"), &field("blue"), 1);

        let expr = FilterExpr::TagEq {
            field: field("color"),
            value: field("red"),
        };
        let bm = idx.evaluate_bitmap(&expr, 3);
        assert!(bm.contains(0));
        assert!(!bm.contains(1));
        assert!(bm.contains(2));
        assert_eq!(bm.len(), 2);
    }

    #[test]
    fn test_numeric_equality() {
        let mut idx = PayloadIndex::new();
        idx.insert_numeric(&field("price"), 9.99, 0);
        idx.insert_numeric(&field("price"), 19.99, 1);
        idx.insert_numeric(&field("price"), 9.99, 2);

        let expr = FilterExpr::NumEq {
            field: field("price"),
            value: OrderedFloat(9.99),
        };
        let bm = idx.evaluate_bitmap(&expr, 3);
        assert_eq!(bm.len(), 2);
        assert!(bm.contains(0));
        assert!(bm.contains(2));
    }

    #[test]
    fn test_numeric_range() {
        let mut idx = PayloadIndex::new();
        idx.insert_numeric(&field("price"), 5.0, 0);
        idx.insert_numeric(&field("price"), 10.0, 1);
        idx.insert_numeric(&field("price"), 15.0, 2);
        idx.insert_numeric(&field("price"), 20.0, 3);

        let expr = FilterExpr::NumRange {
            field: field("price"),
            min: OrderedFloat(8.0),
            max: OrderedFloat(16.0),
            min_excl: false,
            max_excl: false,
        };
        let bm = idx.evaluate_bitmap(&expr, 4);
        assert_eq!(bm.len(), 2);
        assert!(bm.contains(1)); // 10.0
        assert!(bm.contains(2)); // 15.0
    }

    #[test]
    fn inverted_numeric_range_yields_empty_not_a_panic() {
        // moon#664: `BTreeMap::range` panics by contract when start > end, and
        // Moon's shard-panic policy aborts the WHOLE process. The parser is the
        // contract, but the evaluator has to be total as well -- this bug
        // existed because only one of the two ever checked.
        let mut idx = PayloadIndex::new();
        idx.insert_numeric(&field("price"), 5.0, 0);
        idx.insert_numeric(&field("price"), 15.0, 1);

        let expr = FilterExpr::NumRange {
            field: field("price"),
            min: OrderedFloat(300.0),
            max: OrderedFloat(100.0),
            min_excl: false,
            max_excl: false,
        };
        assert!(idx.evaluate_bitmap(&expr, 2).is_empty());
    }

    #[test]
    fn exclusive_numeric_bounds_drop_the_endpoints() {
        let mut idx = PayloadIndex::new();
        idx.insert_numeric(&field("vt"), 150.0, 0);
        idx.insert_numeric(&field("vt"), 250.0, 1);
        idx.insert_numeric(&field("vt"), 350.0, 2);

        let range = |min: f64, max: f64, min_excl, max_excl| FilterExpr::NumRange {
            field: field("vt"),
            min: OrderedFloat(min),
            max: OrderedFloat(max),
            min_excl,
            max_excl,
        };

        // [100 350] -> all three; [100 (350] -> drops 350.
        assert_eq!(
            idx.evaluate_bitmap(&range(100.0, 350.0, false, false), 3)
                .len(),
            3
        );
        assert_eq!(
            idx.evaluate_bitmap(&range(100.0, 350.0, false, true), 3)
                .len(),
            2
        );
        // [(150 350] -> drops 150.
        assert_eq!(
            idx.evaluate_bitmap(&range(150.0, 350.0, true, false), 3)
                .len(),
            2
        );
        // Both exclusive on the same value is empty, not everything.
        assert!(
            idx.evaluate_bitmap(&range(150.0, 150.0, true, true), 3)
                .is_empty()
        );
        // An exclusive bound on a degenerate range is empty too.
        assert!(
            idx.evaluate_bitmap(&range(250.0, 250.0, true, false), 3)
                .is_empty()
        );
    }

    #[test]
    fn test_and_composition() {
        let mut idx = PayloadIndex::new();
        idx.insert_tag(&field("color"), &field("red"), 0);
        idx.insert_tag(&field("color"), &field("red"), 1);
        idx.insert_numeric(&field("price"), 10.0, 1);
        idx.insert_numeric(&field("price"), 10.0, 2);

        let expr = FilterExpr::And(
            Box::new(FilterExpr::TagEq {
                field: field("color"),
                value: field("red"),
            }),
            Box::new(FilterExpr::NumEq {
                field: field("price"),
                value: OrderedFloat(10.0),
            }),
        );
        let bm = idx.evaluate_bitmap(&expr, 3);
        assert_eq!(bm.len(), 1);
        assert!(bm.contains(1)); // only id 1 is both red and price=10
    }

    #[test]
    fn test_or_composition() {
        let mut idx = PayloadIndex::new();
        idx.insert_tag(&field("color"), &field("red"), 0);
        idx.insert_tag(&field("color"), &field("blue"), 1);

        let expr = FilterExpr::Or(
            Box::new(FilterExpr::TagEq {
                field: field("color"),
                value: field("red"),
            }),
            Box::new(FilterExpr::TagEq {
                field: field("color"),
                value: field("blue"),
            }),
        );
        let bm = idx.evaluate_bitmap(&expr, 2);
        assert_eq!(bm.len(), 2);
    }

    #[test]
    fn test_not_complement() {
        let mut idx = PayloadIndex::new();
        idx.insert_tag(&field("color"), &field("red"), 0);
        idx.insert_tag(&field("color"), &field("red"), 2);

        let expr = FilterExpr::Not(Box::new(FilterExpr::TagEq {
            field: field("color"),
            value: field("red"),
        }));
        let bm = idx.evaluate_bitmap(&expr, 4);
        // Universe is {0,1,2,3}, red is {0,2}, NOT red is {1,3}
        assert_eq!(bm.len(), 2);
        assert!(bm.contains(1));
        assert!(bm.contains(3));
    }

    #[test]
    fn test_empty_index() {
        let idx = PayloadIndex::new();
        let expr = FilterExpr::TagEq {
            field: field("color"),
            value: field("red"),
        };
        let bm = idx.evaluate_bitmap(&expr, 100);
        assert!(bm.is_empty());
    }

    #[test]
    fn test_remove() {
        let mut idx = PayloadIndex::new();
        idx.insert_tag(&field("color"), &field("red"), 0);
        idx.insert_tag(&field("color"), &field("red"), 1);
        idx.insert_numeric(&field("price"), 10.0, 0);
        idx.insert_numeric(&field("price"), 10.0, 1);

        idx.remove(0);

        let tag_expr = FilterExpr::TagEq {
            field: field("color"),
            value: field("red"),
        };
        let bm = idx.evaluate_bitmap(&tag_expr, 2);
        assert_eq!(bm.len(), 1);
        assert!(bm.contains(1));

        let num_expr = FilterExpr::NumEq {
            field: field("price"),
            value: OrderedFloat(10.0),
        };
        let bm = idx.evaluate_bitmap(&num_expr, 2);
        assert_eq!(bm.len(), 1);
        assert!(bm.contains(1));
    }

    #[test]
    fn test_bool_eq_true() {
        let mut idx = PayloadIndex::new();
        idx.insert_tag(&field("active"), &Bytes::from_static(b"true"), 0);
        idx.insert_tag(&field("active"), &Bytes::from_static(b"false"), 1);
        idx.insert_tag(&field("active"), &Bytes::from_static(b"true"), 2);

        let expr = FilterExpr::BoolEq {
            field: field("active"),
            value: true,
        };
        let bm = idx.evaluate_bitmap(&expr, 3);
        assert_eq!(bm.len(), 2);
        assert!(bm.contains(0));
        assert!(!bm.contains(1));
        assert!(bm.contains(2));
    }

    #[test]
    fn test_bool_eq_false() {
        let mut idx = PayloadIndex::new();
        idx.insert_tag(&field("active"), &Bytes::from_static(b"true"), 0);
        idx.insert_tag(&field("active"), &Bytes::from_static(b"false"), 1);
        idx.insert_tag(&field("active"), &Bytes::from_static(b"false"), 2);

        let expr = FilterExpr::BoolEq {
            field: field("active"),
            value: false,
        };
        let bm = idx.evaluate_bitmap(&expr, 3);
        assert_eq!(bm.len(), 2);
        assert!(!bm.contains(0));
        assert!(bm.contains(1));
        assert!(bm.contains(2));
    }

    #[test]
    fn test_geo_radius_basic() {
        let mut idx = PayloadIndex::new();
        let loc = field("location");
        // SF: 37.78, -122.42
        idx.insert_geo(&loc, 37.78, -122.42, 0);
        // NYC: 40.71, -74.01
        idx.insert_geo(&loc, 40.71, -74.01, 1);
        // LA: 34.05, -118.24
        idx.insert_geo(&loc, 34.05, -118.24, 2);

        // Search: center=SF, radius=600km — should match LA (~559km) but not NYC (~4130km)
        let expr = FilterExpr::GeoRadius {
            field: loc,
            lon: -122.42,
            lat: 37.78,
            radius_km: 600.0,
        };
        let bm = idx.evaluate_bitmap(&expr, 3);
        assert!(bm.contains(0), "SF should match (center)");
        assert!(!bm.contains(1), "NYC should NOT match (~4130km)");
        assert!(bm.contains(2), "LA should match (~559km)");
    }

    #[test]
    fn test_geo_radius_empty() {
        let idx = PayloadIndex::new();
        let expr = FilterExpr::GeoRadius {
            field: field("location"),
            lon: -122.42,
            lat: 37.78,
            radius_km: 100.0,
        };
        let bm = idx.evaluate_bitmap(&expr, 0);
        assert!(bm.is_empty());
    }

    #[test]
    fn test_haversine() {
        // SF to LA: approximately 559 km
        let d = super::haversine_km(37.78, -122.42, 34.05, -118.24);
        assert!(
            (d - 559.0).abs() < 10.0,
            "SF-LA distance should be ~559km, got {d}"
        );
    }

    #[test]
    fn test_bool_eq_empty_index() {
        let idx = PayloadIndex::new();
        let expr = FilterExpr::BoolEq {
            field: field("active"),
            value: true,
        };
        let bm = idx.evaluate_bitmap(&expr, 10);
        assert!(bm.is_empty());
    }

    // ---- moon#614: the forward index must not change WHAT is retired, only
    // how much work retiring costs. These attack the bookkeeping directly.

    #[test]
    fn retiring_a_document_drops_the_bitmap_it_emptied() {
        let mut idx = PayloadIndex::new();
        let f = field("sku");
        idx.insert_tag(&f, &Bytes::from_static(b"only-doc-with-this"), 1);
        idx.insert_tag(&f, &Bytes::from_static(b"shared"), 1);
        idx.insert_tag(&f, &Bytes::from_static(b"shared"), 2);
        assert_eq!(idx.tag_indexes[&f].len(), 2);

        idx.remove(1);

        // The value only doc 1 held is gone entirely -- not left behind as an
        // empty bitmap. Retaining it is exactly how the pre-moon#613 text index
        // grew without bound and kept the retire cost quadratic.
        let tags = &idx.tag_indexes[&f];
        assert_eq!(tags.len(), 1, "emptied value bitmap was retained: {tags:?}");
        assert!(tags.contains_key(&Bytes::from_static(b"shared")));
        // And the forward-index entry for the retired document is gone too.
        assert!(!idx.doc_values.contains_key(&1));
    }

    #[test]
    fn retiring_the_last_document_drops_the_field_map() {
        let mut idx = PayloadIndex::new();
        let f = field("price");
        idx.insert_numeric(&f, 9.99, 7);
        idx.remove(7);
        assert!(
            !idx.numeric_indexes.contains_key(&f),
            "field map survived with no documents in it"
        );
        assert!(idx.doc_values.is_empty());
    }

    #[test]
    fn re_inserting_after_a_retire_indexes_the_document_again() {
        let mut idx = PayloadIndex::new();
        let f = field("color");
        let red = Bytes::from_static(b"red");
        idx.insert_tag(&f, &red, 1);
        idx.remove(1);
        idx.insert_tag(&f, &red, 1);

        let hits = idx.evaluate_bitmap(
            &FilterExpr::TagEq {
                field: f.clone(),
                value: red.clone(),
            },
            8,
        );
        assert!(hits.contains(1), "re-insert after retire was not indexed");
        // A second retire must still clear it -- the forward index was rebuilt,
        // not left stale from the first round.
        idx.remove(1);
        let hits = idx.evaluate_bitmap(
            &FilterExpr::TagEq {
                field: f,
                value: red,
            },
            8,
        );
        assert!(hits.is_empty());
    }

    #[test]
    fn inserting_the_same_value_twice_still_retires_in_one_call() {
        let mut idx = PayloadIndex::new();
        let f = field("tag");
        let v = Bytes::from_static(b"v");
        idx.insert_tag(&f, &v, 3);
        idx.insert_tag(&f, &v, 3);
        idx.insert_numeric(&field("n"), 1.0, 3);
        idx.insert_numeric(&field("n"), 1.0, 3);

        // Dedup matters twice over: the retire list must not grow on repeat
        // writes, and one remove must be enough (the bitmap holds one bit).
        assert_eq!(idx.doc_values[&3][&f].tags.len(), 1);
        assert_eq!(idx.doc_values[&3][&field("n")].numerics.len(), 1);

        idx.remove(3);
        assert!(idx.tag_indexes.is_empty());
        assert!(idx.numeric_indexes.is_empty());
    }

    #[test]
    fn retiring_an_unknown_document_is_a_no_op() {
        let mut idx = PayloadIndex::new();
        let f = field("color");
        idx.insert_tag(&f, &Bytes::from_static(b"red"), 1);

        idx.remove(999);
        idx.remove_field(&f, 999);
        idx.remove_field(&field("never-written"), 1);

        let hits = idx.evaluate_bitmap(
            &FilterExpr::TagEq {
                field: f,
                value: Bytes::from_static(b"red"),
            },
            8,
        );
        assert!(
            hits.contains(1),
            "an unrelated retire dropped a live document"
        );
    }

    #[test]
    fn remove_field_leaves_the_documents_other_fields_alone() {
        let mut idx = PayloadIndex::new();
        let color = field("color");
        let size = field("size");
        idx.insert_tag(&color, &Bytes::from_static(b"red"), 1);
        idx.insert_numeric(&size, 42.0, 1);

        idx.remove_field(&color, 1);

        assert!(
            idx.evaluate_bitmap(
                &FilterExpr::NumEq {
                    field: size,
                    value: OrderedFloat(42.0)
                },
                8
            )
            .contains(1),
            "removing one field retired the document from another"
        );
        // The document is still tracked, because it still has a field.
        assert!(idx.doc_values.contains_key(&1));
    }

    #[test]
    fn test_remove_field_tag() {
        let mut idx = PayloadIndex::new();
        idx.insert_tag(&field("color"), &field("red"), 0);
        idx.insert_tag(&field("color"), &field("red"), 1);
        idx.insert_tag(&field("size"), &field("large"), 0);

        // Remove only "color" for id 0
        idx.remove_field(&field("color"), 0);

        let color_expr = FilterExpr::TagEq {
            field: field("color"),
            value: field("red"),
        };
        let bm = idx.evaluate_bitmap(&color_expr, 2);
        assert_eq!(bm.len(), 1);
        assert!(bm.contains(1)); // id 1 still has "red"
        assert!(!bm.contains(0)); // id 0 removed from "color"

        // "size" should be untouched for id 0
        let size_expr = FilterExpr::TagEq {
            field: field("size"),
            value: field("large"),
        };
        let bm = idx.evaluate_bitmap(&size_expr, 2);
        assert!(bm.contains(0), "size should still contain id 0");
    }

    #[test]
    fn test_remove_field_numeric() {
        let mut idx = PayloadIndex::new();
        idx.insert_numeric(&field("price"), 10.0, 0);
        idx.insert_numeric(&field("price"), 10.0, 1);
        idx.insert_numeric(&field("qty"), 5.0, 0);

        // Remove only "price" for id 0
        idx.remove_field(&field("price"), 0);

        let price_expr = FilterExpr::NumEq {
            field: field("price"),
            value: OrderedFloat(10.0),
        };
        let bm = idx.evaluate_bitmap(&price_expr, 2);
        assert_eq!(bm.len(), 1);
        assert!(bm.contains(1));

        // "qty" untouched
        let qty_expr = FilterExpr::NumEq {
            field: field("qty"),
            value: OrderedFloat(5.0),
        };
        let bm = idx.evaluate_bitmap(&qty_expr, 2);
        assert!(bm.contains(0));
    }

    #[test]
    fn test_remove_field_geo() {
        let mut idx = PayloadIndex::new();
        let loc = field("location");
        idx.insert_geo(&loc, 37.78, -122.42, 0);
        idx.insert_tag(&field("type"), &field("office"), 0);

        // Remove "location" for id 0 — should also clear __lat/__lon
        idx.remove_field(&loc, 0);

        let geo_expr = FilterExpr::GeoRadius {
            field: loc,
            lon: -122.42,
            lat: 37.78,
            radius_km: 10.0,
        };
        let bm = idx.evaluate_bitmap(&geo_expr, 1);
        assert!(
            bm.is_empty(),
            "geo filter should find nothing after remove_field"
        );

        // "type" tag untouched
        let type_expr = FilterExpr::TagEq {
            field: field("type"),
            value: field("office"),
        };
        let bm = idx.evaluate_bitmap(&type_expr, 1);
        assert!(bm.contains(0));
    }
}
