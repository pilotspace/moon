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
    /// Matches every document in the index — RediSearch's `*` (moon#693).
    ///
    /// The dual of [`QueryNode::Empty`], and the identity of intersection: it is the one
    /// leaf whose membership comes from the index's document registry rather than from a
    /// posting list, so it also returns documents no term query can reach (a document whose
    /// text analyzed to nothing still exists and is still enumerable).
    MatchAll,
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

    /// The human-readable half of the wire error — what the user got wrong, in words.
    ///
    /// Kept separate from [`Self::code`] so the frozen token stays a stable, greppable
    /// identifier while the prose is free to improve.
    #[inline]
    fn detail(&self) -> &'static str {
        match self {
            QueryError::Syntax => "unbalanced or unparseable query",
            QueryError::EmptyQuery => "the query is empty",
            QueryError::UnknownField(_) => "no such field in the index schema",
            QueryError::NumericInvalid => {
                "numeric filter bounds must be numbers with min <= max"
            }
            QueryError::TagInvalid => "tag filter has no values",
        }
    }

    /// The full RESP error string: `ERR <code>: <detail>`.
    ///
    /// moon#691: these used to reach the wire as bare snake_case tokens (`-numeric_filter_invalid`).
    /// A RESP error's FIRST WORD is its code and every client branches on it — redis-py surfaces it
    /// through `ResponseError`, redis-rs matches `ErrorKind` off the leading token — and no client
    /// has a rule for `numeric_filter_invalid`, so the whole message was swallowed as the code and
    /// nothing downstream could tell a client-side query mistake from a server fault.
    ///
    /// The five tokens are a frozen §3 contract (`fts-query-combinators`), so they are KEPT
    /// verbatim inside the message rather than renamed out from under whatever still greps for
    /// them. Only the prefix and the detail are new.
    ///
    /// Allocating here is fine: this is the parse-failure path, not the hot path.
    pub fn wire_error(&self) -> Bytes {
        let code = self.code();
        let detail = self.detail();
        let name: &[u8] = match self {
            QueryError::UnknownField(n) => n.as_ref(),
            _ => b"",
        };
        let mut buf = Vec::with_capacity(4 + code.len() + 2 + detail.len() + name.len() + 4);
        buf.extend_from_slice(b"ERR ");
        buf.extend_from_slice(code.as_bytes());
        buf.extend_from_slice(b": ");
        buf.extend_from_slice(detail.as_bytes());
        if !name.is_empty() {
            buf.extend_from_slice(b" (");
            // The field name is USER INPUT being echoed into a frame that `serialize_frame`
            // writes raw, terminating it with CRLF. `is_term_byte` already excludes ASCII
            // whitespace so a CR/LF cannot reach here today, but a reply that desyncs the
            // connection is too sharp an edge to leave resting on a rule enforced somewhere
            // else: substitute every control byte rather than trust the parser to keep them out.
            buf.extend(name.iter().map(|&b| {
                if b < 0x20 || b == 0x7f {
                    b'?'
                } else {
                    b
                }
            }));
            buf.push(b')');
        }
        Bytes::from(buf)
    }
}
