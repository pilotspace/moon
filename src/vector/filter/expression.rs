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
    /// Logical NOT (complement against universe)
    Not(Box<FilterExpr>),
}
