//! FT.SEARCH query AST — the frozen `QueryNode` shape (task fts-query-combinators §3 @ v1).
//!
//! `parse_query` (see [`super::parse`]) turns a query string into a `QueryNode`; the evaluator
//! (task `fts-query-eval-dispatch`) folds it to a matched doc-id `RoaringBitmap`. Tokens are RAW
//! (un-analyzed) — analysis/stemming happens at eval time so the parser stays a pure function.

use crate::text::store::TermModifier;
use bytes::Bytes;

/// One node of the parsed FT.SEARCH query tree.
///
/// `And`/`Or` children are evaluated by intersecting / unioning their matched doc-id sets.
/// A single-child `And`/`Or` is normalized away by the parser, so those vectors always hold ≥2
/// children when present. `Empty` matches no document and is NOT an error (it is how an
/// intentionally-empty leaf — e.g. a stripped-to-nothing term — folds into set algebra).
#[derive(Debug, Clone, PartialEq)]
pub enum QueryNode {
    /// A single term. `field` is the TEXT field index (into the index's text fields) when the
    /// term was `@field:`-scoped, else `None` (default / all text fields). `token` is the RAW
    /// term bytes (analyzed at eval time); `modifier` selects exact / fuzzy / prefix matching.
    Term {
        field: Option<usize>,
        token: Bytes,
        modifier: TermModifier,
    },
    /// Intersection (implicit-AND / juxtaposition). Always ≥2 children.
    And(Vec<QueryNode>),
    /// Union (the `|` operator). Always ≥2 children.
    Or(Vec<QueryNode>),
    /// TAG membership filter `@field:{a|b}` — `values` are OR-unioned within the tag field.
    Tag { field: Bytes, values: Vec<Bytes> },
    /// NUMERIC range filter `@field:[min max]`, inclusive unless the matching bound is exclusive.
    Numeric {
        field: Bytes,
        min: f64,
        max: f64,
        min_excl: bool,
        max_excl: bool,
    },
    /// Matches ∅. Not an error — a valid empty leaf in the set algebra.
    Empty,
}

/// Parse failure — maps 1:1 to the wire error codes the dispatch layer emits as `Frame::Error`.
///
/// The parser NEVER panics on malformed input (task M7): every malformed shape resolves to one
/// of these. The string forms are the contracted, RediSearch-adjacent error codes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryError {
    /// Unbalanced `()` / `{}` / `[]`, or otherwise unparseable structure.
    Syntax,
    /// Empty query, or an empty group `()` with no terms.
    EmptyQuery,
    /// `@name:` where `name` is not a field in the index schema. Carries the offending name.
    UnknownField(Bytes),
    /// NUMERIC filter whose bounds are non-numeric, or `min > max`.
    NumericInvalid,
    /// TAG filter with no values (`{}` / `{ }`).
    TagInvalid,
}

impl QueryError {
    /// The contracted wire error code (the byte string returned in `Frame::Error`).
    #[inline]
    pub fn code(&self) -> &'static str {
        match self {
            QueryError::Syntax => "syntax_error",
            QueryError::EmptyQuery => "empty_query",
            QueryError::UnknownField(_) => "unknown_field",
            QueryError::NumericInvalid => "numeric_filter_invalid",
            QueryError::TagInvalid => "tag_filter_invalid",
        }
    }
}
