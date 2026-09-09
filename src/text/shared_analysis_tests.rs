//! moon#885 — the text plane and the vector payload index share ONE
//! normalize + segment pass per field value, and each still observes exactly
//! what it observed when it ran its own pass.
//!
//! The two consumers differ on purpose, and those differences are the
//! contract these tests freeze:
//!
//! * the text plane drops the 33 default stop words, honours NOSTEM per
//!   field, and needs word positions for phrase proximity;
//! * the payload index keeps stop words (a filter on `@f:the` must still
//!   hit), always English-stems regardless of the text schema, and has no
//!   positions.
//!
//! What they share — NFKD, combining-mark strip, lowercase, UAX#29 word
//! segmentation, the 2-byte minimum, English Snowball — is the expensive
//! part, and the last test proves it now runs once per field value.

use crate::protocol::Frame;
use crate::shard::spsc_handler::auto_index_hset_public;
use crate::text::analyzer::{AnalyzerPipeline, segment_passes};
use crate::text::store::{TextIndex, TextStore};
use crate::text::types::{BM25Config, TextFieldDef};
use crate::vector::filter::{FilterExpr, TextIndex as PayloadTextIndex};
use crate::vector::store::{IndexMeta, VectorFieldMeta, VectorStore};
use crate::vector::turbo_quant::collection::QuantizationConfig;
use crate::vector::types::DistanceMetric;
use bytes::Bytes;

/// Word positions: The=0 runners=1 are=2 running=3 fast=4 I=5 am=6 a=7
/// Café=8 cat=9. Exercises a stop word, a stem, a 1-byte token and an
/// accented term in one value.
const SAMPLE: &str = "The runners are running fast, I am a Caf\u{e9} cat";
const TITLE: &str = "The Running Caf\u{e9}";
const DIM: usize = 4;

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

fn owned(v: Vec<(String, u32)>) -> Vec<(String, u32)> {
    v
}

fn as_pairs(v: &[(String, u32)]) -> Vec<(&str, u32)> {
    v.iter().map(|(t, p)| (t.as_str(), *p)).collect()
}

fn vector_meta() -> IndexMeta {
    let field = VectorFieldMeta {
        field_name: b("vec"),
        dimension: DIM as u32,
        padded_dimension: DIM as u32,
        metric: DistanceMetric::L2,
        quantization: QuantizationConfig::Sq8,
        build_mode: crate::vector::turbo_quant::collection::BuildMode::Light,
    };
    IndexMeta {
        name: b("ix"),
        dimension: DIM as u32,
        padded_dimension: DIM as u32,
        metric: DistanceMetric::L2,
        hnsw_m: 16,
        hnsw_ef_construction: 100,
        hnsw_ef_runtime: 0,
        compact_threshold: 0,
        source_field: b("vec"),
        key_prefixes: vec![b("doc:")],
        quantization: QuantizationConfig::Sq8,
        build_mode: crate::vector::turbo_quant::collection::BuildMode::Light,
        vector_fields: vec![field],
        schema_fields: Vec::new(),
        merge_mode: crate::vector::store::MergeMode::default(),
        keep_raw: false,
        db_index: 0,
        rerank_mult: 4,
        exact_beam: false,
    }
}

/// One VECTOR index and one TEXT index on the same `doc:` prefix, so a
/// single HSET feeds both planes. `body` is stemmed, `title` is NOSTEM.
fn two_plane_stores() -> (VectorStore, TextStore) {
    let mut vs = VectorStore::new();
    vs.create_index(vector_meta()).expect("vector index");
    let mut ts = TextStore::new();
    let body = TextFieldDef::new(b("body"));
    let mut title = TextFieldDef::new(b("title"));
    title.nostem = true;
    let idx = TextIndex::new(b("ix"), vec![b("doc:")], vec![body, title], BM25Config::default());
    ts.create_index(b("ix"), idx).expect("text index");
    (vs, ts)
}

fn blob() -> Bytes {
    let mut v = Vec::with_capacity(DIM * 4);
    for i in 0..DIM {
        v.extend_from_slice(&(i as f32 + 0.5).to_le_bytes());
    }
    Bytes::from(v)
}

fn hset_args(key: &str, pairs: &[(&str, &str)]) -> Vec<Frame> {
    let mut v = vec![
        Frame::BulkString(b(key)),
        Frame::BulkString(b("vec")),
        Frame::BulkString(blob()),
    ];
    for (f, val) in pairs {
        v.push(Frame::BulkString(b(f)));
        v.push(Frame::BulkString(b(val)));
    }
    v
}

fn payload_hit(vs: &VectorStore, key: &[u8], field: &str, query: &str) -> bool {
    let idx = vs.get_index(b"ix").expect("vector index");
    let kh = xxhash_rust::xxh64::xxh64(key, 0);
    let gid = *idx
        .key_hash_to_global_id
        .get(&kh)
        .expect("key was vector-indexed");
    let expr = FilterExpr::TextMatch {
        field: b(field),
        terms: vec![b(query)],
    };
    idx.payload_index.evaluate_bitmap(&expr, 1).contains(gid)
}

// ── Pins: current observable behaviour of each consumer ─────────────────────

#[test]
fn pin_payload_tokenizer_keeps_stop_words_stems_and_drops_one_byte_tokens() {
    assert_eq!(
        PayloadTextIndex::tokenize(SAMPLE),
        ["the", "runner", "are", "run", "fast", "am", "cafe", "cat"],
        "payload contract: no stop-word list, English stem, len >= 2"
    );
    let mut idx = PayloadTextIndex::new();
    idx.insert(&b("body"), SAMPLE.as_bytes(), 7);
    for q in ["the", "are", "running", "RUNNERS", "cafe", "caf\u{e9}"] {
        assert!(
            idx.search(&b("body"), &PayloadTextIndex::tokenize(q)).contains(7),
            "payload filter on {q:?} must hit"
        );
    }
    assert!(
        idx.search(&b("body"), &PayloadTextIndex::tokenize("I")).is_empty(),
        "a 1-byte query tokenizes to nothing and matches nothing"
    );
}

#[test]
fn pin_text_plane_drops_stop_words_keeps_positions_and_honours_nostem() {
    let stem = AnalyzerPipeline::new(rust_stemmers::Algorithm::English, false);
    let got = owned(stem.tokenize_with_positions(SAMPLE));
    assert_eq!(
        as_pairs(&got),
        [("runner", 1), ("run", 3), ("fast", 4), ("am", 6), ("cafe", 8), ("cat", 9)],
        "stemmed field: stop words gone, positions are ORIGINAL word ordinals"
    );
    let nostem = AnalyzerPipeline::new(rust_stemmers::Algorithm::English, true);
    let got = owned(nostem.tokenize_with_positions(SAMPLE));
    assert_eq!(
        as_pairs(&got),
        [("runners", 1), ("running", 3), ("fast", 4), ("am", 6), ("cafe", 8), ("cat", 9)],
        "NOSTEM field: surface forms survive, stop words still gone"
    );
}

#[test]
fn pin_both_planes_through_auto_index_hset() {
    let (mut vs, mut ts) = two_plane_stores();
    let args = hset_args("doc:1", &[("body", SAMPLE), ("title", TITLE)]);
    let _ = auto_index_hset_public(&mut vs, &mut ts, b"doc:1", &args, 0);

    // Payload index: stop words filterable, always stemmed — even on the
    // field the TEXT schema declares NOSTEM.
    assert!(payload_hit(&vs, b"doc:1", "body", "the"));
    assert!(payload_hit(&vs, b"doc:1", "body", "running"));
    assert!(payload_hit(&vs, b"doc:1", "body", "cafe"));
    assert!(payload_hit(&vs, b"doc:1", "title", "the"));
    assert!(
        payload_hit(&vs, b"doc:1", "title", "runs"),
        "payload stems `title` although the TEXT schema says NOSTEM"
    );
    assert!(!payload_hit(&vs, b"doc:1", "body", "zebra"));

    // Text plane: stop words absent, NOSTEM honoured, positions intact.
    let tidx = ts.get_index(b"ix").expect("text index");
    let kh = xxhash_rust::xxh64::xxh64(b"doc:1", 0);
    let doc = *tidx.key_hash_to_doc_id.get(&kh).expect("doc indexed");
    let (body_dict, title_dict) = (&tidx.field_term_dicts[0], &tidx.field_term_dicts[1]);
    assert!(body_dict.get("the").is_none(), "stop word must not enter the BM25 dictionary");
    assert!(body_dict.get("running").is_none(), "stemmed field stores the stem only");
    assert!(title_dict.get("run").is_none(), "NOSTEM field stores the surface form only");
    assert!(title_dict.get("the").is_none());
    let run = body_dict.get("run").expect("body has `run`");
    let runner = body_dict.get("runner").expect("Snowball: runners -> runner, not run");
    let running = title_dict.get("running").expect("title has `running`");
    let cafe = title_dict.get("cafe").expect("title has `cafe`");
    let pos = |f: usize, t: u32| {
        tidx.field_postings[f]
            .get_posting(t)
            .and_then(|p| p.positions_for(doc))
            .map(<[u32]>::to_vec)
    };
    assert_eq!(pos(0, run), Some(vec![3]), "running=3 -> run");
    assert_eq!(pos(0, runner), Some(vec![1]), "runners=1 -> runner");
    assert_eq!(pos(1, running), Some(vec![1]));
    assert_eq!(pos(1, cafe), Some(vec![2]));
}

/// `HSET k body alpha body beta`: the text plane indexes the FIRST value
/// (`find_field_value`), the payload index sees every pair. Any shared cache
/// must key on the value, never on the field name.
#[test]
fn pin_duplicate_field_names_in_one_hset() {
    let (mut vs, mut ts) = two_plane_stores();
    let args = hset_args("doc:2", &[("body", "alpha"), ("body", "beta")]);
    let _ = auto_index_hset_public(&mut vs, &mut ts, b"doc:2", &args, 0);
    assert!(payload_hit(&vs, b"doc:2", "body", "alpha"));
    assert!(payload_hit(&vs, b"doc:2", "body", "beta"));
    let dict = &ts.get_index(b"ix").expect("text index").field_term_dicts[0];
    assert!(dict.get("alpha").is_some());
    assert!(dict.get("beta").is_none(), "text plane takes the first occurrence only");
}

// ── The claim: one expensive pass per field value, not one per consumer ─────

#[test]
fn one_segment_pass_per_field_value_across_both_planes() {
    let (mut vs, mut ts) = two_plane_stores();
    let args = hset_args("doc:1", &[("body", SAMPLE), ("title", TITLE)]);
    let before = segment_passes();
    let _ = auto_index_hset_public(&mut vs, &mut ts, b"doc:1", &args, 0);
    let passes = segment_passes() - before;
    assert_eq!(
        passes, 2,
        "two field values reached both planes; each must be normalized and \
         segmented exactly once and shared, got {passes} passes"
    );
}
