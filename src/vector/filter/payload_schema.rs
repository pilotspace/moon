//! Schema-aware payload indexing (moon#1194).
//!
//! HEAD's auto-index path payload-indexes EVERY non-vector HASH field of a
//! document: each value is tag- or numeric-indexed (plus a forward-map entry)
//! and full-text indexed into a second, payload-only text index. For a
//! RAG-shaped document (a 2 KB `content` field) that is ~3.5 KB per document
//! — more than the vector itself — and when the schema declares `content
//! TEXT` the text is indexed twice, once here and once in the BM25 plane.
//!
//! With `MOON_VECTOR_PAYLOAD_SCHEMA=declared` an index whose FT.CREATE
//! declared any TEXT / TAG / NUMERIC field indexes only what it declared:
//!
//! | declared as | tag / numeric / geo index | payload text index | KNN `TextMatch` filter |
//! |-------------|---------------------------|--------------------|------------------------|
//! | TAG         | yes (any value shape, as before) | yes (`@f:{multi word}`) | payload text index |
//! | NUMERIC     | yes                       | no                 | —                      |
//! | TEXT        | no                        | no (BM25-owned)    | the BM25 plane         |
//! | undeclared  | no                        | no                 | matches nothing        |
//!
//! This changes what a KNN filter on an UNDECLARED field matches (nothing,
//! instead of HEAD's implicit index), and `TextMatch` on a TEXT field follows
//! the BM25 plane's analysis (its stop words and NOSTEM) instead of the
//! payload index's every-word stemming — hence opt-in. Unset (the default),
//! or an index that declared no payload field at all (vector-only schema, or
//! any sidecar written before index_persist v6), keeps HEAD's behaviour.

use bytes::Bytes;

use crate::vector::store::FieldType;

/// Which payload fields an index declared. `None` on a [`super::PayloadIndex`]
/// means "no schema policy": index every field (HEAD behaviour).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PayloadSchema {
    /// Declared TAG fields.
    tags: Vec<Bytes>,
    /// Declared NUMERIC fields.
    numerics: Vec<Bytes>,
    /// Declared TEXT fields — owned by the BM25 plane.
    texts: Vec<Bytes>,
}

impl PayloadSchema {
    /// The policy for `schema_fields`, or `None` when no TEXT/TAG/NUMERIC
    /// field was declared (then there is nothing to restrict to).
    pub fn from_fields(fields: &[FieldType]) -> Option<Self> {
        let mut s = Self::default();
        for f in fields {
            match f {
                FieldType::Tag { field_name } => s.tags.push(field_name.clone()),
                FieldType::Numeric { field_name } => s.numerics.push(field_name.clone()),
                FieldType::Text { field_name, .. } => s.texts.push(field_name.clone()),
                FieldType::Vector(_) => {}
            }
        }
        (!(s.tags.is_empty() && s.numerics.is_empty() && s.texts.is_empty())).then_some(s)
    }

    /// Tag / numeric / geo indexing: declared TAG or NUMERIC fields only.
    #[inline]
    pub fn indexes_filterable(&self, field: &[u8]) -> bool {
        self.tags
            .iter()
            .chain(&self.numerics)
            .any(|f| f[..] == *field)
    }

    /// Payload full-text indexing: declared TAG fields only (TEXT is BM25-owned).
    #[inline]
    pub fn indexes_text(&self, field: &[u8]) -> bool {
        self.tags.iter().any(|f| f[..] == *field)
    }

    /// Whether `field` is a declared TEXT field (KNN `TextMatch` → BM25 plane).
    #[inline]
    pub fn is_bm25_text(&self, field: &[u8]) -> bool {
        self.texts.iter().any(|f| f[..] == *field)
    }
}

/// `MOON_VECTOR_PAYLOAD_SCHEMA=declared` — opt into schema-aware payload
/// indexing for indexes that declare payload fields. Read once.
pub fn payload_schema_declared_mode() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        payload_schema_flag(std::env::var("MOON_VECTOR_PAYLOAD_SCHEMA").ok().as_deref())
    })
}

/// `MOON_VECTOR_PAYLOAD_SCHEMA` value → declared mode? Only an explicit
/// `declared` enables it; unset or anything else keeps HEAD's behaviour.
fn payload_schema_flag(v: Option<&str>) -> bool {
    v.map(str::trim)
        .is_some_and(|v| v.eq_ignore_ascii_case("declared"))
}

/// The policy an index created from `schema_fields` gets under the current
/// process flag.
pub fn schema_for_index(fields: &[FieldType]) -> Option<PayloadSchema> {
    if payload_schema_declared_mode() {
        PayloadSchema::from_fields(fields)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flag_is_opt_in() {
        assert!(!payload_schema_flag(None));
        assert!(!payload_schema_flag(Some("all")));
        assert!(!payload_schema_flag(Some("")));
        assert!(payload_schema_flag(Some("declared")));
        assert!(payload_schema_flag(Some(" DECLARED ")));
    }

    #[test]
    fn schema_without_payload_fields_is_no_policy() {
        assert_eq!(PayloadSchema::from_fields(&[]), None);
        let s = PayloadSchema::from_fields(&[
            FieldType::Tag {
                field_name: Bytes::from_static(b"lang"),
            },
            FieldType::Numeric {
                field_name: Bytes::from_static(b"year"),
            },
            FieldType::Text {
                field_name: Bytes::from_static(b"content"),
                weight: 1.0,
                nostem: false,
                sortable: false,
                noindex: false,
            },
        ])
        .expect("declared");
        assert!(s.indexes_filterable(b"lang") && s.indexes_filterable(b"year"));
        assert!(!s.indexes_filterable(b"content") && !s.indexes_filterable(b"other"));
        assert!(s.indexes_text(b"lang") && !s.indexes_text(b"content"));
        assert!(s.is_bm25_text(b"content") && !s.is_bm25_text(b"lang"));
    }
}
