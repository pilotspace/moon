//! Refusing a KNN full-text filter no index can answer (moon#1226).
//!
//! A `@field:{multi word}` prefilter parses to a [`FilterExpr::TextMatch`]
//! node. It is answered by one of two routes:
//! - the vector index's payload text index (the default), which the
//!   process-wide opt-out `MOON_VECTOR_PAYLOAD_TEXT=off` drops (moon#1194) and
//!   which a build without the `text-index` feature never has;
//! - the BM25 plane, for a declared TEXT field under the opt-in
//!   `MOON_VECTOR_PAYLOAD_SCHEMA=declared` policy.
//!
//! When the payload route is gone, a node that needs it used to match no
//! document at all — an empty page indistinguishable from "no hit", and a
//! primary and a replica with different settings answered the same query
//! differently. Such a filter is now refused with an explicit `ERR`, the
//! same rule `FilterParse::Invalid` applies to every other filter that
//! cannot be honoured as written (moon#648), and FT.INFO reports the setting
//! (`payload_text_index`).
//!
//! Two layers apply the rule:
//! - [`text_match_refusal_unscoped`] at FT.SEARCH parse time, before any
//!   index is known. Without the schema-aware mode no index can route a
//!   `TextMatch` to BM25, so every search path (sync, yielding, cache,
//!   cross-shard) is covered by the parse alone.
//! - `PayloadIndex::text_match_refusal` per index, for the schema-aware mode,
//!   where a declared TEXT field is routed to BM25 and must keep working.

use super::expression::FilterExpr;
use super::payload_index::payload_text_index_enabled;

/// The wire error for a `TextMatch` node whose only route is the payload
/// text index while `MOON_VECTOR_PAYLOAD_TEXT=off` has dropped it.
pub const ERR_TEXT_FILTER_PAYLOAD_TEXT_OFF: &[u8] = b"ERR full-text KNN filter @field:{multi word} needs the vector payload text index, disabled by MOON_VECTOR_PAYLOAD_TEXT=off";

/// [`ERR_TEXT_FILTER_PAYLOAD_TEXT_OFF`] for a build without the `text-index`
/// feature, where no full-text route exists at all.
pub const ERR_TEXT_FILTER_NO_TEXT_INDEX: &[u8] =
    b"ERR full-text KNN filter @field:{multi word} needs the text-index feature, absent from this build";

/// Why a `TextMatch` node answered through the payload text index can never
/// match a document, or `None` when that index is live.
pub fn payload_text_unavailable() -> Option<&'static [u8]> {
    payload_text_unavailable_for(cfg!(feature = "text-index"), payload_text_index_enabled())
}

/// [`payload_text_unavailable`] for explicit build/flag values (testable
/// without the process-wide `OnceLock` behind the flag).
fn payload_text_unavailable_for(
    text_index_built: bool,
    payload_text_enabled: bool,
) -> Option<&'static [u8]> {
    if !text_index_built {
        Some(ERR_TEXT_FILTER_NO_TEXT_INDEX)
    } else if !payload_text_enabled {
        Some(ERR_TEXT_FILTER_PAYLOAD_TEXT_OFF)
    } else {
        None
    }
}

/// `unavailable` when some `TextMatch` node of `expr` targets a field that
/// `bm25_routed` does not send to the BM25 plane; `None` = every node can be
/// answered.
pub(super) fn text_match_refusal_with(
    expr: &FilterExpr,
    unavailable: Option<&'static [u8]>,
    bm25_routed: impl Fn(&[u8]) -> bool,
) -> Option<&'static [u8]> {
    let why = unavailable?;
    expr.any_text_match_field(&|field| !bm25_routed(field))
        .then_some(why)
}

/// The parse-layer refusal (see the module docs): decided here unless the
/// schema-aware mode may route a declared TEXT field to BM25, in which case
/// `PayloadIndex::text_match_refusal` decides per index.
pub fn text_match_refusal_unscoped(expr: &FilterExpr) -> Option<&'static [u8]> {
    let bm25_possible =
        cfg!(feature = "text-index") && super::payload_schema::payload_schema_declared_mode();
    text_match_refusal_unscoped_for(expr, payload_text_unavailable(), bm25_possible)
}

fn text_match_refusal_unscoped_for(
    expr: &FilterExpr,
    unavailable: Option<&'static [u8]>,
    bm25_possible: bool,
) -> Option<&'static [u8]> {
    if bm25_possible {
        return None;
    }
    text_match_refusal_with(expr, unavailable, |_| false)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use ordered_float::OrderedFloat;

    use super::*;

    fn text(field: &'static str) -> FilterExpr {
        FilterExpr::TextMatch {
            field: Bytes::from_static(field.as_bytes()),
            terms: vec![Bytes::from_static(b"multi"), Bytes::from_static(b"word")],
        }
    }

    fn tag(field: &'static str) -> FilterExpr {
        FilterExpr::TagEq {
            field: Bytes::from_static(field.as_bytes()),
            value: Bytes::from_static(b"v"),
        }
    }

    #[test]
    fn availability_follows_build_and_flag() {
        assert_eq!(payload_text_unavailable_for(true, true), None);
        assert_eq!(
            payload_text_unavailable_for(true, false),
            Some(ERR_TEXT_FILTER_PAYLOAD_TEXT_OFF)
        );
        assert_eq!(
            payload_text_unavailable_for(false, true),
            Some(ERR_TEXT_FILTER_NO_TEXT_INDEX)
        );
        assert_eq!(
            payload_text_unavailable_for(false, false),
            Some(ERR_TEXT_FILTER_NO_TEXT_INDEX)
        );
        // Both are wire errors a client can read.
        assert!(ERR_TEXT_FILTER_PAYLOAD_TEXT_OFF.starts_with(b"ERR "));
        assert!(ERR_TEXT_FILTER_NO_TEXT_INDEX.starts_with(b"ERR "));
    }

    #[test]
    fn a_text_match_anywhere_in_the_tree_is_refused_when_unavailable() {
        let off = Some(ERR_TEXT_FILTER_PAYLOAD_TEXT_OFF);
        let nested = FilterExpr::And(
            Box::new(tag("lang")),
            Box::new(FilterExpr::Not(Box::new(FilterExpr::Or(
                Box::new(FilterExpr::NumRange {
                    field: Bytes::from_static(b"year"),
                    min: OrderedFloat(1.0),
                    max: OrderedFloat(2.0),
                    min_excl: false,
                    max_excl: false,
                }),
                Box::new(text("body")),
            )))),
        );
        assert_eq!(text_match_refusal_unscoped_for(&nested, off, false), off);
        assert_eq!(text_match_refusal_unscoped_for(&text("b"), off, false), off);
        // No TextMatch node: nothing to refuse.
        assert_eq!(
            text_match_refusal_unscoped_for(&tag("lang"), off, false),
            None
        );
        // Payload text index live: nothing to refuse.
        assert_eq!(text_match_refusal_unscoped_for(&nested, None, false), None);
        // Schema-aware mode may route to BM25: the parse layer defers.
        assert_eq!(text_match_refusal_unscoped_for(&nested, off, true), None);
    }

    #[test]
    fn a_bm25_routed_field_is_never_refused() {
        let off = Some(ERR_TEXT_FILTER_PAYLOAD_TEXT_OFF);
        let body_is_bm25 = |f: &[u8]| f == b"body";
        assert_eq!(
            text_match_refusal_with(&text("body"), off, body_is_bm25),
            None
        );
        assert_eq!(
            text_match_refusal_with(&text("tags"), off, body_is_bm25),
            off
        );
        let both = FilterExpr::And(Box::new(text("body")), Box::new(text("tags")));
        assert_eq!(text_match_refusal_with(&both, off, body_is_bm25), off);
    }
}
