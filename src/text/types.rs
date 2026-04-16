/// Type definitions for the BM25 full-text search engine.
///
/// Contains field definitions and scoring configuration used by TextIndex.

/// Definition of a TEXT field within an index schema.
///
/// Parsed from FT.CREATE arguments, e.g.:
/// `FT.CREATE idx SCHEMA title TEXT WEIGHT 2.0 body TEXT NOSTEM`
#[derive(Debug, Clone)]
pub struct TextFieldDef {
    /// The hash field name to index (e.g., "title", "body").
    pub field_name: bytes::Bytes,
    /// Relevance weight multiplier (default 1.0). Higher weight boosts
    /// this field's contribution to BM25 scores.
    pub weight: f64,
    /// When true, skip Snowball stemming for this field.
    pub nostem: bool,
    /// When true, maintain a sorted copy of field values for aggregation.
    pub sortable: bool,
    /// When true, store field metadata but do not tokenize or index.
    pub noindex: bool,
}

impl TextFieldDef {
    /// Create a new TEXT field definition with default settings.
    pub fn new(field_name: bytes::Bytes) -> Self {
        Self {
            field_name,
            weight: 1.0,
            nostem: false,
            sortable: false,
            noindex: false,
        }
    }
}

/// BM25 scoring parameters, configurable per TextIndex.
///
/// Defaults: k1=1.2, b=0.75 (Lucene/Tantivy standard).
#[derive(Debug, Clone, Copy)]
pub struct BM25Config {
    /// Term frequency saturation parameter.
    /// Higher k1 increases the effect of term frequency.
    pub k1: f32,
    /// Length normalization parameter (0.0 = no normalization, 1.0 = full).
    pub b: f32,
}

impl Default for BM25Config {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}
