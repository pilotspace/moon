//! Plan 152-07 NUMERIC storage unit tests.
//!
//! Kept as a sibling file (loaded via `#[path]` from `store.rs`) following the
//! same pattern as `store_tag_tests.rs`, so `store.rs` stays under the
//! 1500-LOC cap mandated by CLAUDE.md.

use bytes::Bytes;

use super::*;
use crate::text::types::{BM25Config, NumericFieldDef, TagFieldDef, TextFieldDef};

fn num_args(pairs: &[(&[u8], &[u8])]) -> Vec<crate::protocol::Frame> {
    let mut v = Vec::with_capacity(pairs.len() * 2);
    for (k, val) in pairs {
        v.push(crate::protocol::Frame::BulkString(Bytes::copy_from_slice(
            k,
        )));
        v.push(crate::protocol::Frame::BulkString(Bytes::copy_from_slice(
            val,
        )));
    }
    v
}

fn num_only_index(num_names: &[&[u8]]) -> TextIndex {
    let num_fields: Vec<NumericFieldDef> = num_names
        .iter()
        .map(|n| NumericFieldDef::new(Bytes::copy_from_slice(n)))
        .collect();
    TextIndex::new_with_schema(
        Bytes::from_static(b"num_idx"),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        num_fields,
        BM25Config::default(),
    )
}

#[test]
fn numeric_field_def_defaults() {
    let def = NumericFieldDef::new(Bytes::from_static(b"price"));
    assert_eq!(def.field_name.as_ref(), b"price");
    assert!(!def.sortable);
    assert!(!def.noindex);
}

#[test]
fn new_with_schema_seeds_numeric_indexes() {
    let idx = num_only_index(&[b"price", b"age"]);
    assert_eq!(idx.numeric_fields.len(), 2);
    assert!(
        idx.numeric_indexes
            .contains_key(&Bytes::from_static(b"price"))
    );
    assert!(
        idx.numeric_indexes
            .contains_key(&Bytes::from_static(b"age"))
    );
    assert!(idx.text_fields.is_empty());
    assert!(idx.tag_fields.is_empty());
    assert!(idx.field_analyzers.is_empty());
}

#[test]
fn new_preserves_legacy_signature_and_numeric_fields_empty() {
    let idx = TextIndex::new(
        Bytes::from_static(b"legacy"),
        Vec::new(),
        vec![TextFieldDef::new(Bytes::from_static(b"body"))],
        BM25Config::default(),
    );
    assert!(idx.numeric_fields.is_empty());
    assert!(idx.numeric_indexes.is_empty());
    assert_eq!(idx.text_fields.len(), 1);
}

#[test]
fn numeric_index_document_basic() {
    let mut idx = num_only_index(&[b"score", b"age"]);
    let args = num_args(&[(b"score", b"42.5"), (b"age", b"30")]);
    idx.numeric_index_document(1, b"doc:1", &args);

    let btree_score = idx
        .numeric_indexes
        .get(&Bytes::from_static(b"score"))
        .expect("score btree");
    let bm_42_5 = btree_score
        .get(&ordered_float::OrderedFloat(42.5))
        .expect("42.5 bitmap");
    assert!(bm_42_5.contains(0), "doc 0 should be in score=42.5 bucket");

    let btree_age = idx
        .numeric_indexes
        .get(&Bytes::from_static(b"age"))
        .expect("age btree");
    let bm_30 = btree_age
        .get(&ordered_float::OrderedFloat(30.0))
        .expect("30 bitmap");
    assert!(bm_30.contains(0));
}

#[test]
fn numeric_non_numeric_value_skipped() {
    let mut idx = num_only_index(&[b"score"]);
    let args = num_args(&[(b"score", b"not_a_number")]);
    idx.numeric_index_document(1, b"doc:1", &args);
    let btree = idx
        .numeric_indexes
        .get(&Bytes::from_static(b"score"))
        .expect("score btree should exist (seeded)");
    assert!(
        btree.is_empty(),
        "Non-numeric string must leave numeric_indexes['score'] empty"
    );
}

#[test]
fn numeric_nan_and_inf_rejected_on_write() {
    // T-152-07-02: Rust's f64 parser accepts "NaN"/"Infinity"/"inf" literals.
    // The post-parse is_nan()/is_infinite() guard is load-bearing — without it,
    // NaN would corrupt BTreeMap ordering (NaN != NaN).
    let mut idx = num_only_index(&[b"score"]);
    for (tag, raw) in [
        (1u64, "NaN"),
        (2, "Infinity"),
        (3, "-Infinity"),
        (4, "inf"),
        (5, "-inf"),
        (6, "+inf"),
    ] {
        let key = format!("doc:{}", tag);
        let args = num_args(&[(b"score", raw.as_bytes())]);
        idx.numeric_index_document(tag, key.as_bytes(), &args);
    }
    let btree = idx
        .numeric_indexes
        .get(&Bytes::from_static(b"score"))
        .expect("score btree");
    assert!(
        btree.is_empty(),
        "NaN / ±Infinity literals must be skipped (got {} keys)",
        btree.len()
    );
}

#[test]
fn numeric_per_field_upsert_preserves_untouched() {
    // Write score=10, age=30 → then re-HSET score=20 (without age).
    // score=10 bucket should lose doc_id; score=20 should gain it;
    // age=30 should STAY (untouched field preservation, Plan 06 Blocker 4 pattern).
    let mut idx = num_only_index(&[b"score", b"age"]);
    let args1 = num_args(&[(b"score", b"10"), (b"age", b"30")]);
    idx.numeric_index_document(7, b"doc:p", &args1);

    let args2 = num_args(&[(b"score", b"20")]);
    idx.numeric_index_document(7, b"doc:p", &args2);

    let score_btree = idx
        .numeric_indexes
        .get(&Bytes::from_static(b"score"))
        .unwrap();
    assert!(
        !score_btree.contains_key(&ordered_float::OrderedFloat(10.0)),
        "score=10 bucket should be empty-cleaned-up after upsert"
    );
    let bm_20 = score_btree
        .get(&ordered_float::OrderedFloat(20.0))
        .expect("score=20 bitmap");
    assert!(bm_20.contains(0));

    let age_btree = idx
        .numeric_indexes
        .get(&Bytes::from_static(b"age"))
        .unwrap();
    let bm_30 = age_btree
        .get(&ordered_float::OrderedFloat(30.0))
        .expect("age=30 still exists");
    assert!(
        bm_30.contains(0),
        "age=30 should be preserved (partial HSET)"
    );
}

#[test]
fn numeric_full_upsert_revokes_old() {
    let mut idx = num_only_index(&[b"score", b"age"]);
    let args1 = num_args(&[(b"score", b"10"), (b"age", b"30")]);
    idx.numeric_index_document(8, b"doc:f", &args1);

    let args2 = num_args(&[(b"score", b"20"), (b"age", b"31")]);
    idx.numeric_index_document(8, b"doc:f", &args2);

    let age_btree = idx
        .numeric_indexes
        .get(&Bytes::from_static(b"age"))
        .unwrap();
    assert!(
        !age_btree.contains_key(&ordered_float::OrderedFloat(30.0)),
        "age=30 must be evicted on full upsert"
    );
    let bm_31 = age_btree
        .get(&ordered_float::OrderedFloat(31.0))
        .expect("age=31 bitmap");
    assert!(bm_31.contains(0));
}

#[test]
fn numeric_empty_bitmap_cleanup() {
    // After upsert removes the last doc_id for a value, the BTreeMap entry
    // must be fully removed — not left as an empty RoaringBitmap.
    let mut idx = num_only_index(&[b"score"]);
    let args1 = num_args(&[(b"score", b"5")]);
    idx.numeric_index_document(9, b"d:c", &args1);
    let args2 = num_args(&[(b"score", b"6")]);
    idx.numeric_index_document(9, b"d:c", &args2);
    let btree = idx
        .numeric_indexes
        .get(&Bytes::from_static(b"score"))
        .unwrap();
    assert!(
        !btree.contains_key(&ordered_float::OrderedFloat(5.0)),
        "empty score=5 bucket must be removed entirely"
    );
}

#[test]
fn search_numeric_range_inclusive() {
    let mut idx = num_only_index(&[b"score"]);
    for (i, v) in [10.0, 20.0, 30.0, 40.0, 50.0].iter().enumerate() {
        let args = num_args(&[(b"score", format!("{}", v).as_bytes())]);
        idx.numeric_index_document(i as u64, format!("d:{}", i).as_bytes(), &args);
    }
    let r = idx.search_numeric_range(&Bytes::from_static(b"score"), 20.0, 40.0, false, false);
    assert_eq!(
        r.len(),
        3,
        "[20 40] inclusive should match 3 docs (20, 30, 40)"
    );
}

#[test]
fn search_numeric_range_exclusive_combinations() {
    let mut idx = num_only_index(&[b"score"]);
    for (i, v) in [10.0, 20.0, 30.0, 40.0, 50.0].iter().enumerate() {
        let args = num_args(&[(b"score", format!("{}", v).as_bytes())]);
        idx.numeric_index_document(i as u64, format!("d:{}", i).as_bytes(), &args);
    }
    // (20, 40] → 30, 40 → 2 docs
    let r = idx.search_numeric_range(&Bytes::from_static(b"score"), 20.0, 40.0, true, false);
    assert_eq!(r.len(), 2);
    // [20, 40) → 20, 30 → 2 docs
    let r = idx.search_numeric_range(&Bytes::from_static(b"score"), 20.0, 40.0, false, true);
    assert_eq!(r.len(), 2);
    // (20, 40) → 30 → 1 doc
    let r = idx.search_numeric_range(&Bytes::from_static(b"score"), 20.0, 40.0, true, true);
    assert_eq!(r.len(), 1);
}

#[test]
fn search_numeric_range_infinities() {
    let mut idx = num_only_index(&[b"score"]);
    for (i, v) in [10.0, 20.0, 30.0, 40.0, 50.0].iter().enumerate() {
        let args = num_args(&[(b"score", format!("{}", v).as_bytes())]);
        idx.numeric_index_document(i as u64, format!("d:{}", i).as_bytes(), &args);
    }
    // [-inf +inf] → all 5
    let r = idx.search_numeric_range(
        &Bytes::from_static(b"score"),
        f64::NEG_INFINITY,
        f64::INFINITY,
        false,
        false,
    );
    assert_eq!(r.len(), 5);
    // [30 +inf] → 30, 40, 50
    let r = idx.search_numeric_range(
        &Bytes::from_static(b"score"),
        30.0,
        f64::INFINITY,
        false,
        false,
    );
    assert_eq!(r.len(), 3);
    // [-inf 20] → 10, 20
    let r = idx.search_numeric_range(
        &Bytes::from_static(b"score"),
        f64::NEG_INFINITY,
        20.0,
        false,
        false,
    );
    assert_eq!(r.len(), 2);
}

#[test]
fn search_numeric_range_equality_shorthand() {
    let mut idx = num_only_index(&[b"score"]);
    for (i, v) in [10.0, 20.0, 30.0].iter().enumerate() {
        let args = num_args(&[(b"score", format!("{}", v).as_bytes())]);
        idx.numeric_index_document(i as u64, format!("d:{}", i).as_bytes(), &args);
    }
    // [20 20] → only doc with score=20
    let r = idx.search_numeric_range(&Bytes::from_static(b"score"), 20.0, 20.0, false, false);
    assert_eq!(r.len(), 1);
}

#[test]
fn search_numeric_range_unknown_field() {
    let idx = num_only_index(&[b"score"]);
    let r = idx.search_numeric_range(&Bytes::from_static(b"missing"), 0.0, 100.0, false, false);
    assert!(r.is_empty());
}

#[test]
fn search_numeric_range_case_insensitive_field() {
    let mut idx = num_only_index(&[b"score"]);
    let args = num_args(&[(b"score", b"42")]);
    idx.numeric_index_document(1, b"d:1", &args);
    // @Score (mixed-case) must resolve @score
    let r = idx.search_numeric_range(&Bytes::from_static(b"Score"), 0.0, 100.0, false, false);
    assert_eq!(r.len(), 1);
    let r = idx.search_numeric_range(&Bytes::from_static(b"SCORE"), 0.0, 100.0, false, false);
    assert_eq!(r.len(), 1);
}

#[test]
fn numeric_ensure_doc_id_cross_method() {
    // Plan 06 Blocker 7 discipline: ensure_doc_id shared across
    // index_document / tag_index_document / numeric_index_document.
    let mut idx = TextIndex::new_with_schema(
        Bytes::from_static(b"shared"),
        Vec::new(),
        vec![TextFieldDef::new(Bytes::from_static(b"body"))],
        vec![TagFieldDef::new(Bytes::from_static(b"status"))],
        vec![NumericFieldDef::new(Bytes::from_static(b"score"))],
        BM25Config::default(),
    );
    let args = num_args(&[(b"score", b"42")]);
    idx.numeric_index_document(1234, b"doc:x", &args);
    assert_eq!(idx.key_hash_to_doc_id.get(&1234), Some(&0));
    assert_eq!(
        idx.doc_id_to_key.get(&0).map(|b| b.as_ref()),
        Some(&b"doc:x"[..])
    );

    // Now also tag + text on the same key — should reuse doc_id=0.
    let tag_frames = num_args(&[(b"status", b"open")]);
    idx.tag_index_document(1234, b"doc:x", &tag_frames);
    let body_frames = num_args(&[(b"body", b"hello")]);
    idx.index_document(1234, b"doc:x", &body_frames);

    assert_eq!(idx.key_hash_to_doc_id.len(), 1);
    assert_eq!(idx.key_hash_to_doc_id.get(&1234), Some(&0));
}

#[test]
fn numeric_empty_schema_noop() {
    let mut idx = TextIndex::new(
        Bytes::from_static(b"x"),
        Vec::new(),
        vec![TextFieldDef::new(Bytes::from_static(b"body"))],
        BM25Config::default(),
    );
    let args = num_args(&[(b"score", b"42")]);
    idx.numeric_index_document(100, b"doc:n", &args);
    assert!(idx.numeric_indexes.is_empty());
    // ensure_doc_id should NOT be called when numeric_fields is empty.
    assert!(idx.key_hash_to_doc_id.is_empty());
}

// ── FieldFilter::NumericRange parser tests (Plan 152-07 Task 1b) ───────────

fn expect_numeric_range(
    query: &[u8],
) -> (bytes::Bytes, f64, f64, bool, bool) {
    use crate::command::vector_search::ft_text_search::{FieldFilter, pre_parse_field_filter};
    let clause = pre_parse_field_filter(query)
        .expect("ok")
        .expect("some clause");
    match clause.filter {
        Some(FieldFilter::NumericRange {
            field,
            min,
            max,
            min_exclusive,
            max_exclusive,
        }) => (field, min, max, min_exclusive, max_exclusive),
        other => panic!("expected NumericRange filter, got {:?}", other),
    }
}

#[test]
fn pre_parse_numeric_range_inclusive() {
    let (field, min, max, lo_excl, hi_excl) = expect_numeric_range(b"@score:[10 100]");
    assert_eq!(field.as_ref(), b"score");
    assert_eq!(min, 10.0);
    assert_eq!(max, 100.0);
    assert!(!lo_excl);
    assert!(!hi_excl);
}

#[test]
fn pre_parse_numeric_range_exclusive_bounds() {
    let (_, _, _, lo, hi) = expect_numeric_range(b"@score:[(10 100]");
    assert!(lo && !hi);
    let (_, _, _, lo, hi) = expect_numeric_range(b"@score:[10 (100]");
    assert!(!lo && hi);
    let (_, _, _, lo, hi) = expect_numeric_range(b"@score:[(10 (100]");
    assert!(lo && hi);
}

#[test]
fn pre_parse_numeric_range_infinities() {
    let (_, min, max, _, _) = expect_numeric_range(b"@score:[-inf +inf]");
    assert!(min == f64::NEG_INFINITY);
    assert!(max == f64::INFINITY);
    let (_, min, _, _, _) = expect_numeric_range(b"@score:[-INF 50]");
    assert!(min == f64::NEG_INFINITY);
    let (_, _, max, _, _) = expect_numeric_range(b"@score:[50 +inf]");
    assert!(max == f64::INFINITY);
    let (_, _, max, _, _) = expect_numeric_range(b"@score:[50 infinity]");
    assert!(max == f64::INFINITY);
    let (_, min, _, _, _) = expect_numeric_range(b"@score:[-Infinity 50]");
    assert!(min == f64::NEG_INFINITY);
}

#[test]
fn pre_parse_numeric_range_equality_shorthand() {
    let (_, min, max, _, _) = expect_numeric_range(b"@score:[42 42]");
    assert_eq!(min, 42.0);
    assert_eq!(max, 42.0);
}

#[test]
fn pre_parse_numeric_range_inverted_rejected() {
    // T-152-07-05: inverted range REJECTED with explicit error.
    use crate::command::vector_search::ft_text_search::pre_parse_field_filter;
    let err = pre_parse_field_filter(b"@score:[100 10]").unwrap_err();
    assert!(
        err.contains("min > max"),
        "expected 'min > max' error, got: {}",
        err
    );
}

#[test]
fn pre_parse_numeric_range_error_cases() {
    use crate::command::vector_search::ft_text_search::pre_parse_field_filter;
    // Unterminated
    assert!(
        pre_parse_field_filter(b"@score:[10 100")
            .unwrap_err()
            .contains("unterminated")
    );
    // Too few bounds
    assert!(
        pre_parse_field_filter(b"@score:[10]")
            .unwrap_err()
            .contains("two bounds")
    );
    // Too many
    assert!(
        pre_parse_field_filter(b"@score:[10 20 30]")
            .unwrap_err()
            .contains("two bounds")
    );
    // Non-numeric bound
    assert!(
        pre_parse_field_filter(b"@score:[bogus 100]")
            .unwrap_err()
            .contains("invalid numeric bound")
    );
    // Empty field
    assert!(pre_parse_field_filter(b"@:[10 100]").is_err());
}

#[test]
fn pre_parse_numeric_range_nan_bound_rejected() {
    // T-152-07-07: NaN literal bound rejected.
    use crate::command::vector_search::ft_text_search::pre_parse_field_filter;
    let err = pre_parse_field_filter(b"@score:[NaN 100]").unwrap_err();
    assert!(err.contains("NaN"), "expected NaN rejection, got: {}", err);
    let err = pre_parse_field_filter(b"@score:[10 NaN]").unwrap_err();
    assert!(err.contains("NaN"));
}

#[test]
fn pre_parse_numeric_range_length_guard() {
    // T-152-07-04: 256-byte inner cap.
    use crate::command::vector_search::ft_text_search::pre_parse_field_filter;
    let mut q = b"@score:[".to_vec();
    q.extend(std::iter::repeat_n(b'1', 300));
    q.extend_from_slice(b" 2]");
    let err = pre_parse_field_filter(&q).unwrap_err();
    assert!(
        err.contains("too long"),
        "expected 'too long' error, got: {}",
        err
    );
}

#[test]
fn pre_parse_numeric_tag_still_works() {
    // Regression: TAG parser unchanged.
    use crate::command::vector_search::ft_text_search::{FieldFilter, pre_parse_field_filter};
    let clause = pre_parse_field_filter(b"@status:{open}").unwrap().unwrap();
    assert!(matches!(clause.filter, Some(FieldFilter::Tag { .. })));
}

#[test]
fn pre_parse_numeric_bare_text_returns_none() {
    use crate::command::vector_search::ft_text_search::pre_parse_field_filter;
    assert!(pre_parse_field_filter(b"machine learning").unwrap().is_none());
    assert!(
        pre_parse_field_filter(b"@title:(terms)")
            .unwrap()
            .is_none()
    );
}

#[test]
fn execute_query_numeric_range_returns_docs() {
    // Task 1b Test 23: execute_query_on_index routes NumericRange → search_numeric_range.
    use crate::command::vector_search::ft_text_search::{
        TextQueryClause, execute_query_on_index, FieldFilter,
    };
    let mut idx = num_only_index(&[b"score"]);
    for (i, v) in [10.0, 20.0, 30.0].iter().enumerate() {
        let args = num_args(&[(b"score", format!("{}", v).as_bytes())]);
        idx.numeric_index_document(i as u64, format!("d:{}", i).as_bytes(), &args);
    }
    let clause = TextQueryClause {
        field_name: None,
        terms: Vec::new(),
        filter: Some(FieldFilter::NumericRange {
            field: Bytes::from_static(b"score"),
            min: 15.0,
            max: 25.0,
            min_exclusive: false,
            max_exclusive: false,
        }),
    };
    let results = execute_query_on_index(&idx, &clause, None, None, 100);
    assert_eq!(results.len(), 1, "only d:1 (score=20) matches [15, 25]");
    assert_eq!(results[0].score, 0.0, "FieldFilter results carry score 0.0");
}

#[test]
fn numeric_noindex_field_skipped() {
    let mut num_def = NumericFieldDef::new(Bytes::from_static(b"hidden"));
    num_def.noindex = true;
    let mut idx = TextIndex::new_with_schema(
        Bytes::from_static(b"ni"),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![num_def],
        BM25Config::default(),
    );
    let args = num_args(&[(b"hidden", b"42")]);
    idx.numeric_index_document(1, b"d:1", &args);
    let btree = idx
        .numeric_indexes
        .get(&Bytes::from_static(b"hidden"))
        .expect("seeded");
    assert!(
        btree.is_empty(),
        "NOINDEX field must not populate numeric index"
    );
}
