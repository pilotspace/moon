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

    /// Remove an internal ID from a specific field's bitmaps only (for metadata updates).
    ///
    /// Removes `internal_id` from both tag and numeric indexes for the given `field`,
    /// and from geo sub-fields (`{field}__lat`, `{field}__lon`) if present.
    /// O(values_per_field) -- acceptable because metadata updates are rare relative to search.
    pub fn remove_field(&mut self, field: &Bytes, internal_id: u32) {
        if let Some(tag_map) = self.tag_indexes.get_mut(field) {
            for bitmap in tag_map.values_mut() {
                bitmap.remove(internal_id);
            }
        }
        if let Some(num_map) = self.numeric_indexes.get_mut(field) {
            for bitmap in num_map.values_mut() {
                bitmap.remove(internal_id);
            }
        }
        // Also clean up geo sub-fields if this is a geo field
        let field_str = std::str::from_utf8(field).unwrap_or("");
        let lat_field = Bytes::from(format!("{field_str}__lat"));
        let lon_field = Bytes::from(format!("{field_str}__lon"));
        if let Some(num_map) = self.numeric_indexes.get_mut(&lat_field) {
            for bitmap in num_map.values_mut() {
                bitmap.remove(internal_id);
            }
        }
        if let Some(num_map) = self.numeric_indexes.get_mut(&lon_field) {
            for bitmap in num_map.values_mut() {
                bitmap.remove(internal_id);
            }
        }
        #[cfg(feature = "text-index")]
        self.text_indexes.remove_field(field, internal_id);
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
        #[cfg(feature = "text-index")]
        self.text_indexes.remove(internal_id);
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
        assert!(bm.is_empty(), "geo filter should find nothing after remove_field");

        // "type" tag untouched
        let type_expr = FilterExpr::TagEq {
            field: field("type"),
            value: field("office"),
        };
        let bm = idx.evaluate_bitmap(&type_expr, 1);
        assert!(bm.contains(0));
    }
}
