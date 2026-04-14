use bytes::Bytes;
use ordered_float::OrderedFloat;

/// Filter expression AST for vector search pre/post filtering.
/// Evaluated against PayloadIndex to produce a RoaringBitmap of matching vector IDs.
#[derive(Debug)]
pub enum FilterExpr {
    /// Tag equality: @field:{value}
    TagEq { field: Bytes, value: Bytes },
    /// Numeric equality: @field:[val val]
    NumEq {
        field: Bytes,
        value: OrderedFloat<f64>,
    },
    /// Numeric range: @field:[min max]
    NumRange {
        field: Bytes,
        min: OrderedFloat<f64>,
        max: OrderedFloat<f64>,
    },
    /// Logical AND
    And(Box<FilterExpr>, Box<FilterExpr>),
    /// Logical OR
    Or(Box<FilterExpr>, Box<FilterExpr>),
    /// Boolean equality: @field:{true} or @field:{false}
    /// Stored as tag with "true"/"false" strings, matched via tag_indexes.
    BoolEq { field: Bytes, value: bool },
    /// Geo-radius: @field:[lon lat radius_km]
    /// Evaluates using dual BTreeMap (__lat/__lon) + Haversine post-filter.
    GeoRadius {
        field: Bytes,
        lon: f64,
        lat: f64,
        radius_km: f64,
    },
    /// Logical NOT (complement against universe)
    Not(Box<FilterExpr>),
    /// Full-text match: @field:{term1 term2} — AND semantics.
    /// Requires `text-index` feature for meaningful evaluation;
    /// without it, evaluate_bitmap returns an empty bitmap.
    TextMatch { field: Bytes, terms: Vec<Bytes> },
}
