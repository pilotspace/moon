//! moon#1194: resident cost of the vector payload index for RAG-shaped
//! documents, HEAD policy (every HASH field payload-indexed) vs the opt-in
//! declared-schema policy (`MOON_VECTOR_PAYLOAD_SCHEMA=declared`).
//!
//! Each document: a 16d vector (kept tiny so the payload dominates), a ~2 KB
//! `content` TEXT field with ~150 distinct words, a `lang` TAG, a `year`
//! NUMERIC and an undeclared `source` field. Both indexes feed the same BM25
//! plane (declared TEXT/TAG/NUMERIC), so the RSS difference between the two
//! loads is the payload index alone.
//!
//! Method: process `VmRSS` growth across each load, the declared index loaded
//! first and kept alive while the HEAD-policy index loads, so freed pages are
//! never reused across the two measurements. Linux-only; one `#[test]`.

#![cfg(all(feature = "text-index", target_os = "linux"))]

use bytes::Bytes;
use moon::protocol::Frame;
use moon::text::store::{TextIndex, TextStore};
use moon::text::types::{BM25Config, NumericFieldDef, TagFieldDef, TextFieldDef};
use moon::vector::filter::PayloadIndex;
use moon::vector::filter::payload_schema::PayloadSchema;
use moon::vector::store::{FieldType, IndexMeta, VectorFieldMeta, VectorStore};
use moon::vector::turbo_quant::collection::{BuildMode, QuantizationConfig};
use moon::vector::turbo_quant::encoder::padded_dimension;
use moon::vector::types::DistanceMetric;

const DOCS: usize = 6_000;
const DIM: u32 = 16;
const MB: f64 = 1024.0 * 1024.0;

fn rss() -> isize {
    let status = std::fs::read_to_string("/proc/self/status").expect("/proc/self/status");
    status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<isize>().ok())
        .expect("VmRSS")
        * 1024
}

fn bulk(b: &[u8]) -> Frame {
    Frame::BulkString(Bytes::copy_from_slice(b))
}

fn meta(name: &str) -> IndexMeta {
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
        key_prefixes: vec![Bytes::copy_from_slice(format!("{name}:").as_bytes())],
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

/// ~2 KB of prose from a 4,000-word vocabulary, ~150 distinct words.
fn content(doc: usize) -> String {
    let mut s = String::with_capacity(2100);
    let mut x = doc as u64 * 2_654_435_761 + 1;
    while s.len() < 2000 {
        x = x
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let w = (x >> 33) % 4000;
        // Zipf-ish: small ids are common words.
        let w = (w * w) / 4000;
        s.push_str("term");
        s.push_str(&w.to_string());
        s.push(' ');
    }
    s
}

fn load(vs: &mut VectorStore, ts: &mut TextStore, name: &str) {
    for i in 0..DOCS {
        let key = format!("{name}:{i}");
        let emb: Vec<u8> = (0..DIM)
            .flat_map(|d| ((i as f32) * 0.001 + d as f32).to_le_bytes())
            .collect();
        let year = (1990 + i % 35).to_string();
        let args = vec![
            bulk(key.as_bytes()),
            bulk(b"emb"),
            bulk(&emb),
            bulk(b"content"),
            bulk(content(i).as_bytes()),
            bulk(b"lang"),
            bulk(if i % 4 == 0 { b"en" } else { b"de" }),
            bulk(b"year"),
            bulk(year.as_bytes()),
            bulk(b"source"),
            bulk(format!("crawl-{}", i % 50).as_bytes()),
        ];
        moon::shard::spsc_handler::auto_index_hset_public(vs, ts, key.as_bytes(), &args, 0);
    }
}

fn index(vs: &mut VectorStore, ts: &mut TextStore, name: &str, declared: bool) {
    let m = meta(name);
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
            vec![Bytes::copy_from_slice(format!("{name}:").as_bytes())],
            vec![TextFieldDef::new(Bytes::from_static(b"content"))],
            vec![TagFieldDef::new(Bytes::from_static(b"lang"))],
            vec![NumericFieldDef::new(Bytes::from_static(b"year"))],
            BM25Config::default(),
        ),
    )
    .expect("text create");
}

#[test]
fn declared_schema_payload_index_costs_a_fraction_of_heads() {
    moon::vector::distance::init();
    let mut vs = VectorStore::new();
    let mut ts = TextStore::new();
    index(&mut vs, &mut ts, "declared", true);
    index(&mut vs, &mut ts, "legacy", false);

    let r0 = rss();
    load(&mut vs, &mut ts, "declared");
    let declared = (rss() - r0) as f64;
    let r1 = rss();
    load(&mut vs, &mut ts, "legacy");
    let legacy = (rss() - r1) as f64;

    let payload = legacy - declared;
    eprintln!(
        "{DOCS} RAG docs: RSS +{:.1} MB declared, +{:.1} MB HEAD policy \
         -> payload index {:.0} B/doc removed ({:.1}% of the HEAD-policy load)",
        declared / MB,
        legacy / MB,
        payload / DOCS as f64,
        100.0 * payload / legacy
    );
    assert!(
        payload / DOCS as f64 > 1000.0,
        "declared schema must drop >1 KB/doc of payload index (got {:.0} B/doc)",
        payload / DOCS as f64
    );
    assert_eq!(vs.index_names().len(), 2);
}
