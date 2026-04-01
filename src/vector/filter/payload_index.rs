use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;

use super::expression::FilterExpr;

/// Payload index maintaining Roaring bitmaps per tag value and numeric value.
///
/// Each field gets its own index: tags use `HashMap<value, bitmap>`,
/// numerics use `BTreeMap<value, bitmap>` for efficient range queries.
pub struct PayloadIndex {
    /// field_name -> { tag_value -> bitmap of internal_ids }
    tag_indexes: HashMap<Bytes, HashMap<Bytes, RoaringBitmap>>,
    /// field_name -> { numeric_value -> bitmap of internal_ids }
    numeric_indexes: HashMap<Bytes, BTreeMap<OrderedFloat<f64>, RoaringBitmap>>,
}

impl PayloadIndex {
    /// Create an empty payload index.
    pub fn new() -> Self {
        Self {
            tag_indexes: HashMap::new(),
            numeric_indexes: HashMap::new(),
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
    }

    /// Insert a numeric value for the given internal vector ID.
    pub fn insert_numeric(&mut self, field: &Bytes, value: f64, internal_id: u32) {
        self.numeric_indexes
            .entry(field.clone())
            .or_default()
            .entry(OrderedFloat(value))
            .or_default()
            .insert(internal_id);
    }

    /// Remove an internal ID from ALL bitmaps (for vector deletion).
    ///
    /// O(fields * values) -- acceptable because DEL is rare relative to search.
    pub fn remove(&mut self, internal_id: u32) {
        for field_map in self.tag_indexes.values_mut() {
            for bitmap in field_map.values_mut() {
                bitmap.remove(internal_id);
            }
        }
        for field_map in self.numeric_indexes.values_mut() {
            for bitmap in field_map.values_mut() {
                bitmap.remove(internal_id);
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

            FilterExpr::NumRange { field, min, max } => {
                let Some(btree) = self.numeric_indexes.get(field) else {
                    return RoaringBitmap::new();
                };
                let mut result = RoaringBitmap::new();
                for (_k, bm) in btree.range(*min..=*max) {
                    result |= bm;
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        };
        let bm = idx.evaluate_bitmap(&expr, 4);
        assert_eq!(bm.len(), 2);
        assert!(bm.contains(1)); // 10.0
        assert!(bm.contains(2)); // 15.0
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
}
