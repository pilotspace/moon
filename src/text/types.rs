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

/// TAG field definition parsed from FT.CREATE.
///
/// TAG semantics (matches RediSearch):
/// - Exact membership lookup via `@field:{value}` — no stemming, no stopword filtering.
/// - Case-insensitive by default (ASCII lowercase); `CASESENSITIVE` modifier flips this.
/// - Multi-value split on `separator` (default `,`). Empty tokens between separators are skipped.
/// - Field-name resolution is case-insensitive (`@Status:{open}` resolves `status`).
///
/// Plan 07 mirrors this shape for NUMERIC fields. The `FieldFilter` enum
/// (declared in `command::vector_search::ft_text_search`) carries TAG / NUMERIC
/// clauses through the query path without invoking the text analyzer.
#[cfg(feature = "text-index")]
#[derive(Debug, Clone)]
pub struct TagFieldDef {
    /// Canonical field name. Stored once; comparisons use `eq_ignore_ascii_case`
    /// so queries for `@Status:{open}` still resolve `status`.
    pub field_name: bytes::Bytes,
    /// Separator byte used to split multi-value tag payloads (default `b','`).
    pub separator: u8,
    /// When true, store tag values byte-for-byte. When false (default), values
    /// are ASCII-lowercased on both insert and lookup.
    pub case_sensitive: bool,
    /// Whether to expose the field for SORTABLE-aware aggregation (v1: stored, not consumed).
    pub sortable: bool,
    /// Declared but not indexed (schema recall only).
    pub noindex: bool,
}

#[cfg(feature = "text-index")]
impl TagFieldDef {
    /// Create a new TAG field definition with RediSearch-compatible defaults.
    pub fn new(field_name: bytes::Bytes) -> Self {
        Self {
            field_name,
            separator: b',',
            case_sensitive: false,
            sortable: false,
            noindex: false,
        }
    }
}

/// NUMERIC field definition parsed from FT.CREATE (Plan 152-07).
///
/// NUMERIC semantics (matches RediSearch):
/// - Range filter lookup via `@field:[min max]`, `[(min max]`, `[min (max]`,
///   `[(min (max]`, `[-inf +inf]` — closed / half-open, with `-inf` / `+inf`
///   sentinels for unbounded sides.
/// - Field-name resolution is case-insensitive (`@Score:[1 10]` resolves `score`).
/// - Values parsed as `f64` on HSET; non-numeric / NaN / Infinity values are
///   skipped silently with `tracing::debug!` (RediSearch-compatible — NOT an error).
///
/// The `FieldFilter` enum (declared in `command::vector_search::ft_text_search`)
/// carries NUMERIC range clauses through the query path without invoking the
/// text analyzer. Storage is `HashMap<field, BTreeMap<OrderedFloat<f64>, RoaringBitmap<doc_id>>>`
/// so `BTreeMap::range` yields O(log N) range resolution.
#[cfg(feature = "text-index")]
#[derive(Debug, Clone)]
pub struct NumericFieldDef {
    /// Canonical field name. Stored once; comparisons use `eq_ignore_ascii_case`
    /// so queries for `@Score:[1 10]` still resolve `score`.
    pub field_name: bytes::Bytes,
    /// Whether to expose the field for SORTABLE-aware aggregation (v1: stored, not consumed).
    pub sortable: bool,
    /// Declared but not indexed (schema recall only).
    pub noindex: bool,
}

#[cfg(feature = "text-index")]
impl NumericFieldDef {
    /// Create a new NUMERIC field definition with RediSearch-compatible defaults.
    pub fn new(field_name: bytes::Bytes) -> Self {
        Self {
            field_name,
            sortable: false,
            noindex: false,
        }
    }
}
