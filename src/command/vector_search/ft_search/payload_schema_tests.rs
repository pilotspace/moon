//! moon#1194: schema-aware payload indexing, end to end through the HSET
//! auto-index hook and the KNN filter path.
//!
//! Two indexes see the same documents: one with HEAD's policy (every HASH
//! field payload-indexed) and one with the declared-schema policy
//! (`MOON_VECTOR_PAYLOAD_SCHEMA=declared`, installed directly here so the
//! test does not depend on the process environment).

use bytes::Bytes;

use crate::protocol::Frame;
use crate::text::store::{TextIndex, TextStore};
use crate::text::types::{BM25Config, NumericFieldDef, TagFieldDef, TextFieldDef};
use crate::vector::filter::payload_schema::PayloadSchema;
use crate::vector::filter::{FilterExpr, PayloadIndex};
use crate::vector::store::{FieldType, IndexMeta, VectorFieldMeta, VectorStore};
use crate::vector::turbo_quant::collection::{BuildMode, QuantizationConfig};
use crate::vector::turbo_quant::encoder::padded_dimension;
use crate::vector::types::DistanceMetric;

use super::execute::search_local_filtered_with_text;

const DIM: u32 = 16;

fn bulk(b: &[u8]) -> Frame {
    Frame::BulkString(Bytes::copy_from_slice(b))
}

fn meta(name: &str, prefix: &str) -> IndexMeta {
    let vf = VectorFieldMeta {
        field_name: Bytes::from_static(b"emb"),
        dimension: DIM,
        padded_dimension: padded_dimension(DIM),
        metric: DistanceMetric::L2,
        quantization: QuantizationConfig::TurboQuant4,
        build_mode: BuildMode::Light,
    };
    IndexMeta {
        name: Bytes::copy_from_slice(name.as_bytes()),
        dimension: DIM,
        padded_dimension: padded_dimension(DIM),
        metric: DistanceMetric::L2,
        hnsw_m: 16,
        hnsw_ef_construction: 200,
        hnsw_ef_runtime: 0,
        compact_threshold: 100_000,
        source_field: Bytes::from_static(b"emb"),
        key_prefixes: vec![Bytes::copy_from_slice(prefix.as_bytes())],
        quantization: QuantizationConfig::TurboQuant4,
        build_mode: BuildMode::Light,
        vector_fields: vec![vf.clone()],
        schema_fields: vec![
            FieldType::Vector(vf),
            FieldType::Text {
                field_name: Bytes::from_static(b"content"),
                weight: 1.0,
                nostem: false,
                sortable: false,
                noindex: false,
            },
            FieldType::Tag {
                field_name: Bytes::from_static(b"lang"),
            },
            FieldType::Numeric {
                field_name: Bytes::from_static(b"year"),
            },
        ],
        merge_mode: Default::default(),
        keep_raw: false,
        db_index: 0,
        rerank_mult: 4,
        exact_beam: false,
    }
}

const CONTENT: [&str; 4] = [
    "the quick brown fox jumps over the lazy dog",
    "a slow green turtle crosses the road",
    "quick thinking saves the brown bear",
    "the fox and the hound are friends",
];

/// Both indexes (`legacy`, `declared`) over the same 40 documents.
fn stores() -> (VectorStore, TextStore) {
    crate::vector::distance::init();
    let mut vs = VectorStore::new();
    let mut ts = TextStore::new();
    for (name, prefix, declared) in [("legacy", "l:", false), ("declared", "d:", true)] {
        let m = meta(name, prefix);
        let schema = PayloadSchema::from_fields(&m.schema_fields);
        vs.create_index(m).expect("create");
        if declared {
            vs.get_index_mut(name.as_bytes())
                .expect("idx")
                .payload_index = PayloadIndex::with_schema(schema);
        }
        ts.create_index(
            Bytes::copy_from_slice(name.as_bytes()),
            TextIndex::new_with_schema(
                Bytes::copy_from_slice(name.as_bytes()),
                vec![Bytes::copy_from_slice(prefix.as_bytes())],
                vec![TextFieldDef::new(Bytes::from_static(b"content"))],
                vec![TagFieldDef::new(Bytes::from_static(b"lang"))],
                vec![NumericFieldDef::new(Bytes::from_static(b"year"))],
                BM25Config::default(),
            ),
        )
        .expect("text create");
        for i in 0..40usize {
            let key = format!("{prefix}{i}");
            let emb: Vec<u8> = (0..DIM)
                .flat_map(|d| ((i as f32) * 0.1 + d as f32 * 0.01).to_le_bytes())
                .collect();
            let lang: &[u8] = if i % 3 == 0 { b"en" } else { b"de" };
            let year = (2000 + i).to_string();
            let args = vec![
                bulk(key.as_bytes()),
                bulk(b"emb"),
                bulk(&emb),
                bulk(b"content"),
                bulk(CONTENT[i % 4].as_bytes()),
                bulk(b"lang"),
                bulk(lang),
                bulk(b"year"),
                bulk(year.as_bytes()),
                bulk(b"note"),
                bulk(b"draft"),
            ];
            crate::shard::spsc_handler::auto_index_hset_public(
                &mut vs,
                &mut ts,
                key.as_bytes(),
                &args,
                0,
            );
        }
    }
    (vs, ts)
}

/// Document numbers the filtered KNN returns (k = all docs).
fn hits(vs: &mut VectorStore, ts: &TextStore, name: &str, filter: &FilterExpr) -> Vec<usize> {
    let q: Vec<u8> = (0..DIM).flat_map(|_| 0.0f32.to_le_bytes()).collect();
    let frame = search_local_filtered_with_text(
        vs,
        name.as_bytes(),
        &q,
        40,
        Some(filter),
        0,
        40,
        None,
        0,
        0,
        Some(ts),
    );
    let Frame::Array(items) = frame else {
        panic!("{frame:?}");
    };
    let mut out: Vec<usize> = items
        .iter()
        .filter_map(|f| match f {
            Frame::BulkString(b) => std::str::from_utf8(b)
                .ok()
                .and_then(|s| s.split(':').nth(1))
                .and_then(|n| n.parse().ok()),
            _ => None,
        })
        .collect();
    out.sort_unstable();
    out
}

fn text_match(field: &'static [u8], words: &[&'static [u8]]) -> FilterExpr {
    FilterExpr::TextMatch {
        field: Bytes::from_static(field),
        terms: words.iter().map(|w| Bytes::from_static(w)).collect(),
    }
}

#[test]
fn declared_policy_answers_declared_filters_like_head() {
    let (mut vs, ts) = stores();
    let filters = [
        FilterExpr::TagEq {
            field: Bytes::from_static(b"lang"),
            value: Bytes::from_static(b"en"),
        },
        FilterExpr::NumRange {
            field: Bytes::from_static(b"year"),
            min: 2010.0.into(),
            max: 2019.0.into(),
            min_excl: false,
            max_excl: false,
        },
        // TEXT field: answered by the BM25 plane under the declared policy.
        text_match(b"content", &[b"quick", b"brown"]),
        text_match(b"content", &[b"fox"]),
        FilterExpr::And(
            Box::new(text_match(b"content", &[b"fox"])),
            Box::new(FilterExpr::TagEq {
                field: Bytes::from_static(b"lang"),
                value: Bytes::from_static(b"en"),
            }),
        ),
    ];
    for f in &filters {
        let legacy = hits(&mut vs, &ts, "legacy", f);
        let declared = hits(&mut vs, &ts, "declared", f);
        assert!(!legacy.is_empty(), "{f:?}: fixture must match something");
        assert_eq!(declared, legacy, "{f:?}");
    }
}

#[test]
fn declared_policy_indexes_only_declared_fields() {
    let (vs, _ts) = stores();
    let legacy = &vs.get_index(b"legacy").expect("legacy").payload_index;
    let declared = &vs.get_index(b"declared").expect("declared").payload_index;
    // HEAD: the TEXT field is also payload-text-indexed (double-indexed with
    // the BM25 plane), and the undeclared `note` field is indexed too.
    for f in [&b"content"[..], b"note", b"lang", b"year"] {
        assert!(
            legacy.holds_field(&Bytes::copy_from_slice(f)),
            "legacy {f:?}"
        );
    }
    // Declared: TAG + NUMERIC only; TEXT lives in the BM25 plane alone.
    assert!(declared.holds_field(&Bytes::from_static(b"lang")));
    assert!(declared.holds_field(&Bytes::from_static(b"year")));
    assert!(!declared.holds_field(&Bytes::from_static(b"content")));
    assert!(!declared.holds_field(&Bytes::from_static(b"note")));
}

#[test]
fn declared_policy_undeclared_and_stop_word_semantics() {
    let (mut vs, ts) = stores();
    // Undeclared field: HEAD matched through its implicit index; the
    // declared policy has none (the documented, flag-gated change).
    let note = FilterExpr::TagEq {
        field: Bytes::from_static(b"note"),
        value: Bytes::from_static(b"draft"),
    };
    assert_eq!(hits(&mut vs, &ts, "legacy", &note).len(), 40);
    assert!(hits(&mut vs, &ts, "declared", &note).is_empty());
    // A TEXT filter follows the BM25 field's analysis: "the" is a stop word
    // there, so "the fox" means "fox" (HEAD's payload index required "the").
    let the_fox = text_match(b"content", &[b"the", b"fox"]);
    let fox = text_match(b"content", &[b"fox"]);
    assert_eq!(
        hits(&mut vs, &ts, "declared", &the_fox),
        hits(&mut vs, &ts, "declared", &fox)
    );
    // Without the BM25 plane reachable, a TEXT filter matches nothing
    // rather than falling back to an index the policy never filled.
    let q: Vec<u8> = (0..DIM).flat_map(|_| 0.0f32.to_le_bytes()).collect();
    let frame = search_local_filtered_with_text(
        &mut vs,
        b"declared",
        &q,
        40,
        Some(&fox),
        0,
        40,
        None,
        0,
        0,
        None,
    );
    assert!(
        matches!(frame, Frame::Array(ref a) if a.first() == Some(&Frame::Integer(0))),
        "{frame:?}"
    );
}
