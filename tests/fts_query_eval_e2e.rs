//! RED end-to-end tests for `fts-query-eval-dispatch` task 2b — evaluating the parsed
//! `QueryNode` AST (from 2a) to a matched doc-id SET and wiring it into FT.SEARCH dispatch.
//!
//! Contract INHERITED from `fts-query-combinators` §3 (FROZEN @ v1): the dispatch path runs
//! `parse_query → eval_query → build_text_response`, so OR unions, multi-`@clause` intersections,
//! and grouping return correct result sets. This suite asserts RESULT SETS + ordering + coded
//! errors at the wire boundary (the parse-tree level is 2a's `tests/fts_query_parse.rs`).
//!
//! Two new symbols are exercised (both absent until 2b builds them → this suite is RED by
//! compile-failure, the correct TDD red state):
//!   * `moon::text::query::eval_query(idx, node, gdf, gn, top_k) -> Vec<TextSearchResult>`
//!     — the centralized evaluator kernel (frozen §3 set-semantics + best-effort BM25).
//!   * `moon::command::vector_search::run_text_query(store, index, raw, top_k, off, count) -> Frame`
//!     — the dispatch wrapper every handler text branch routes through (parse + eval + respond,
//!     Err → Frame::Error with the frozen wire code).
//!
//! Distinct corpora are used so union ≠ intersection is observable (the old AND/0-result bugs).
#![cfg(feature = "text-index")]

use std::collections::BTreeSet;

use bytes::Bytes;
use moon::command::vector_search::run_text_query;
use moon::protocol::Frame;
use moon::text::query::{QuerySchema, collect_df_field_terms, eval_query, parse_query};
use moon::text::store::{TextIndex, TextStore};
use moon::text::types::{BM25Config, NumericFieldDef, TagFieldDef, TextFieldDef};

// ── corpus construction ─────────────────────────────────────────────────────

/// A schema with TEXT body(0)+title(1), TAG "tag", NUMERIC "price".
fn empty_index() -> TextIndex {
    TextIndex::new_with_schema(
        Bytes::from_static(b"idx"),
        Vec::new(),
        vec![
            TextFieldDef::new(Bytes::from_static(b"body")),
            TextFieldDef::new(Bytes::from_static(b"title")),
        ],
        vec![TagFieldDef::new(Bytes::from_static(b"tag"))],
        vec![NumericFieldDef::new(Bytes::from_static(b"price"))],
        BM25Config::default(),
    )
}

/// Flatten `[(field,value)]` into HSET-style Frame args.
fn args(pairs: &[(&str, &str)]) -> Vec<Frame> {
    let mut v = Vec::with_capacity(pairs.len() * 2);
    for (f, val) in pairs {
        v.push(Frame::BulkString(Bytes::copy_from_slice(f.as_bytes())));
        v.push(Frame::BulkString(Bytes::copy_from_slice(val.as_bytes())));
    }
    v
}

/// Index one doc. All three index paths share `hash`+`key` so they collapse to one doc_id
/// (ensure_doc_id), giving TEXT/TAG/NUMERIC a shared doc-id space — the property set algebra
/// relies on.
fn add_doc(
    idx: &mut TextIndex,
    hash: u64,
    key: &str,
    text: &[(&str, &str)],
    tags: &[(&str, &str)],
    nums: &[(&str, &str)],
) {
    let kb = key.as_bytes();
    if !text.is_empty() {
        idx.index_document(hash, kb, &args(text));
    }
    if !tags.is_empty() {
        idx.tag_index_document(hash, kb, &args(tags));
    }
    if !nums.is_empty() {
        idx.numeric_index_document(hash, kb, &args(nums));
    }
}

fn store_of(idx: TextIndex) -> TextStore {
    let mut ts = TextStore::new();
    ts.create_index(Bytes::from_static(b"idx"), idx)
        .expect("create_index ok");
    ts
}

// ── frame helpers (mirror ft_text_search.rs extract_hits) ───────────────────

/// FT.SEARCH reply is `[total, key, fields, key, fields, ...]`; keys are the odd indices.
fn keys_set(frame: &Frame) -> BTreeSet<Vec<u8>> {
    match frame {
        Frame::Array(items) => items
            .iter()
            .skip(1)
            .step_by(2)
            .filter_map(|f| match f {
                Frame::BulkString(b) => Some(b.to_vec()),
                _ => None,
            })
            .collect(),
        other => panic!("expected Frame::Array, got {other:?}"),
    }
}

fn keys_ordered(frame: &Frame) -> Vec<Vec<u8>> {
    match frame {
        Frame::Array(items) => items
            .iter()
            .skip(1)
            .step_by(2)
            .filter_map(|f| match f {
                Frame::BulkString(b) => Some(b.to_vec()),
                _ => None,
            })
            .collect(),
        other => panic!("expected Frame::Array, got {other:?}"),
    }
}

fn total(frame: &Frame) -> i64 {
    match frame {
        Frame::Array(items) => match items.first() {
            Some(Frame::Integer(n)) => *n,
            _ => -1,
        },
        other => panic!("expected Frame::Array, got {other:?}"),
    }
}

fn err_bytes(frame: &Frame) -> Vec<u8> {
    match frame {
        Frame::Error(b) => b.to_vec(),
        other => panic!("expected Frame::Error, got {other:?}"),
    }
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

fn set_of(keys: &[&str]) -> BTreeSet<Vec<u8>> {
    keys.iter().map(|k| k.as_bytes().to_vec()).collect()
}

/// Run a full FT.SEARCH text query through the centralized dispatch wrapper, all results.
fn search(ts: &TextStore, q: &str) -> Frame {
    run_text_query(ts, b"idx", q.as_bytes(), 1000, 0, usize::MAX)
}

// ─────────────────────────── E1 — OR is a union ────────────────────────────
#[test]
fn test_or_union_e2e() {
    // alpha matches {a,b,c}; beta matches {c,d,e}. The old bug stripped `|` → AND → {c}.
    let mut idx = empty_index();
    add_doc(&mut idx, 1, "a", &[("body", "alpha")], &[], &[]);
    add_doc(&mut idx, 2, "b", &[("body", "alpha")], &[], &[]);
    add_doc(&mut idx, 3, "c", &[("body", "alpha beta")], &[], &[]);
    add_doc(&mut idx, 4, "d", &[("body", "beta")], &[], &[]);
    add_doc(&mut idx, 5, "e", &[("body", "beta")], &[], &[]);
    idx.build_fst();
    let ts = store_of(idx);

    let r = search(&ts, "alpha | beta");
    assert_eq!(keys_set(&r), set_of(&["a", "b", "c", "d", "e"]), "OR must union");
    assert_eq!(total(&r), 5);

    // sanity: AND still narrows to the overlap, single terms unchanged.
    assert_eq!(keys_set(&search(&ts, "alpha beta")), set_of(&["c"]));
    assert_eq!(keys_set(&search(&ts, "alpha")), set_of(&["a", "b", "c"]));
    assert_eq!(keys_set(&search(&ts, "beta")), set_of(&["c", "d", "e"]));
}

// ─────────────────── E1 — TEXT + TAG clauses intersect ──────────────────────
#[test]
fn test_text_tag_intersect_e2e() {
    // @body:foo = {1,2,3,4}; @tag:{bar} = {3,4,5}. Old bug word-tokenized the tag → 0 results.
    let mut idx = empty_index();
    add_doc(&mut idx, 1, "1", &[("body", "foo")], &[("tag", "x")], &[]);
    add_doc(&mut idx, 2, "2", &[("body", "foo")], &[("tag", "x")], &[]);
    add_doc(&mut idx, 3, "3", &[("body", "foo")], &[("tag", "bar")], &[]);
    add_doc(&mut idx, 4, "4", &[("body", "foo")], &[("tag", "bar")], &[]);
    add_doc(&mut idx, 5, "5", &[("body", "other")], &[("tag", "bar")], &[]);
    idx.build_fst();
    let ts = store_of(idx);

    assert_eq!(
        keys_set(&search(&ts, "@body:foo @tag:{bar}")),
        set_of(&["3", "4"]),
        "TEXT∩TAG must intersect, not 0"
    );
}

// ─────────────────── E1 — TEXT + NUMERIC clauses intersect ──────────────────
#[test]
fn test_text_numeric_intersect_e2e() {
    // @body:phone = {1,2,3}; @price:[10 20] = {2,3,9}. Intersection = {2,3}.
    let mut idx = empty_index();
    add_doc(&mut idx, 1, "1", &[("body", "phone")], &[], &[("price", "5")]);
    add_doc(&mut idx, 2, "2", &[("body", "phone")], &[], &[("price", "10")]);
    add_doc(&mut idx, 3, "3", &[("body", "phone")], &[], &[("price", "20")]);
    add_doc(&mut idx, 9, "9", &[("body", "tablet")], &[], &[("price", "15")]);
    idx.build_fst();
    let ts = store_of(idx);

    assert_eq!(
        keys_set(&search(&ts, "@body:phone @price:[10 20]")),
        set_of(&["2", "3"]),
        "TEXT∩NUMERIC must intersect"
    );
}

// ─────────────────────────── E1 — grouping ─────────────────────────────────
#[test]
fn test_grouping_e2e() {
    // red={1,2,3} blue={3,4} car={2,3,4,5}; car (red | blue) = car ∩ (red∪blue) = {2,3,4}.
    let mut idx = empty_index();
    add_doc(&mut idx, 1, "1", &[("body", "red")], &[], &[]);
    add_doc(&mut idx, 2, "2", &[("body", "red car")], &[], &[]);
    add_doc(&mut idx, 3, "3", &[("body", "red blue car")], &[], &[]);
    add_doc(&mut idx, 4, "4", &[("body", "blue car")], &[], &[]);
    add_doc(&mut idx, 5, "5", &[("body", "car")], &[], &[]);
    idx.build_fst();
    let ts = store_of(idx);

    assert_eq!(
        keys_set(&search(&ts, "car (red | blue)")),
        set_of(&["2", "3", "4"]),
        "grouping must scope the union under the AND"
    );
}

// ──────────────────── E2 — deterministic scored ordering ────────────────────
#[test]
fn test_or_scoring_order_deterministic() {
    let mut idx = empty_index();
    add_doc(&mut idx, 1, "a", &[("body", "alpha")], &[], &[]);
    add_doc(&mut idx, 2, "b", &[("body", "alpha alpha")], &[], &[]);
    add_doc(&mut idx, 3, "c", &[("body", "beta")], &[], &[]);
    idx.build_fst();
    let ts = store_of(idx);

    let first = keys_ordered(&search(&ts, "alpha | beta"));
    let second = keys_ordered(&search(&ts, "alpha | beta"));
    assert_eq!(first, second, "ordering must be deterministic across runs");
    assert_eq!(
        first.iter().collect::<BTreeSet<_>>().len(),
        first.len(),
        "no duplicate keys in the union result"
    );
}

// ────────────────── E3/E6 — malformed query → coded error ───────────────────
#[test]
fn test_malformed_returns_coded_error() {
    let mut idx = empty_index();
    add_doc(&mut idx, 1, "a", &[("body", "alpha")], &[], &[]);
    idx.build_fst();
    let ts = store_of(idx);

    let bad = search(&ts, "alpha | (beta"); // unbalanced paren
    assert!(
        contains(&err_bytes(&bad), b"syntax_error"),
        "malformed query must reply with the frozen `syntax_error` code"
    );

    // E6 — the server did not panic; the very next query still works.
    assert_eq!(keys_set(&search(&ts, "alpha")), set_of(&["a"]));
}

#[test]
fn test_unknown_field_error() {
    let mut idx = empty_index();
    add_doc(&mut idx, 1, "a", &[("body", "alpha")], &[], &[]);
    idx.build_fst();
    let ts = store_of(idx);

    let r = search(&ts, "@nope:foo");
    assert!(
        contains(&err_bytes(&r), b"unknown_field"),
        "querying an undeclared field must reply with `unknown_field`"
    );
}

// ─────────────── E2/A1 — no regression on already-correct shapes ────────────
#[test]
fn test_no_regression_single_and_filter() {
    // Each of these shapes worked before 2b; routing them through the new path must keep the
    // same result SETS. (Exact BM25 scores are held by the existing store/text unit suites,
    // which eval_query reuses verbatim — see §4 regression-net note.)
    let mut idx = empty_index();
    add_doc(&mut idx, 1, "1", &[("body", "alpha beta")], &[("tag", "bar")], &[("price", "10")]);
    add_doc(&mut idx, 2, "2", &[("body", "alpha")], &[("tag", "bar")], &[("price", "15")]);
    add_doc(&mut idx, 3, "3", &[("body", "beta")], &[("tag", "baz")], &[("price", "99")]);
    idx.build_fst();
    let ts = store_of(idx);

    assert_eq!(keys_set(&search(&ts, "alpha")), set_of(&["1", "2"]), "single term");
    assert_eq!(keys_set(&search(&ts, "alpha beta")), set_of(&["1"]), "implicit AND");
    assert_eq!(keys_set(&search(&ts, "@body:alpha")), set_of(&["1", "2"]), "single @field");
    assert_eq!(keys_set(&search(&ts, "@tag:{bar}")), set_of(&["1", "2"]), "bare tag filter");
    assert_eq!(keys_set(&search(&ts, "@price:[10 20]")), set_of(&["1", "2"]), "bare numeric filter");

    // pure-filter docs score 0.0; the reply is still well-formed (count matches set size).
    assert_eq!(total(&search(&ts, "@tag:{bar}")), 2);
}

// ───────────────── absent term → empty result, NOT an error ─────────────────
#[test]
fn test_empty_result_not_error() {
    let mut idx = empty_index();
    add_doc(&mut idx, 1, "a", &[("body", "alpha")], &[], &[]);
    idx.build_fst();
    let ts = store_of(idx);

    let r = search(&ts, "zzz"); // term present nowhere
    assert!(!matches!(r, Frame::Error(_)), "no-match is not an error");
    assert_eq!(total(&r), 0, "absent term → 0 results");
    assert!(keys_set(&r).is_empty());
}

// ─────────── frozen kernel symbol — eval_query gets a direct test ───────────
#[test]
fn test_eval_query_kernel_direct() {
    // The contract names eval_query as THE evaluator kernel; exercise it directly (no store /
    // dispatch wrapper) so a regression in the kernel surfaces independently of the wiring.
    let mut idx = empty_index();
    add_doc(&mut idx, 1, "a", &[("body", "alpha")], &[], &[]);
    add_doc(&mut idx, 2, "b", &[("body", "alpha")], &[], &[]);
    add_doc(&mut idx, 3, "c", &[("body", "beta")], &[], &[]);
    idx.build_fst();

    let schema = QuerySchema::from_index(&idx);
    let node = parse_query(b"alpha | beta", &schema).expect("parse ok");
    let results = eval_query(&idx, &node, None, None, 1000);

    let got: BTreeSet<Vec<u8>> = results.iter().map(|r| r.key.to_vec()).collect();
    assert_eq!(got, set_of(&["a", "b", "c"]), "kernel must union the OR branches");
    // text leaves carry a BM25 score; the union is non-empty and finite.
    assert!(results.iter().all(|r| r.score.is_finite()));
}

// ─── DFS Phase-1 N-invariant: collect_df_field_terms returns AT MOST ONE entry ───
// aggregate_doc_freq SUMS one "N" sentinel per (field,terms) entry across shards. >1 entry inflates
// the global N; 0 entries for a query WITH text leaves zeroes it — both corrupt multi-shard BM25 IDF.
#[test]
fn test_df_field_terms_single_entry_invariant() {
    let mut idx = empty_index();
    add_doc(&mut idx, 1, "1", &[("body", "alpha"), ("title", "beta")], &[("tag", "bar")], &[("price", "10")]);
    idx.build_fst();
    let schema = QuerySchema::from_index(&idx);

    // OR across fields + a tag filter: still exactly one df entry (so N is gathered exactly once).
    let node = parse_query(b"alpha | @title:beta @tag:{bar}", &schema).expect("parse ok");
    let fq = collect_df_field_terms(&node, &idx);
    assert!(fq.len() <= 1, "must emit at most one (field,terms) df entry, got {}", fq.len());
    assert_eq!(fq.len(), 1, "a query with text leaves must emit one entry so N is gathered");

    // Single-field query → hint is that field (pre-2b parity).
    let single = parse_query(b"@body:alpha", &schema).expect("parse ok");
    let fq1 = collect_df_field_terms(&single, &idx);
    assert_eq!(fq1.len(), 1);
    assert_eq!(fq1[0].0, Some(0), "single @body query → field-0 hint");
}

#[test]
fn test_df_field_terms_pure_filter_vs_fuzzy() {
    let mut idx = empty_index();
    add_doc(&mut idx, 1, "1", &[("body", "alpha")], &[("tag", "bar")], &[]);
    idx.build_fst();
    let schema = QuerySchema::from_index(&idx);

    // Pure TAG filter → NO text leaf → no entry (no text scoring → N not needed).
    let tag_only = parse_query(b"@tag:{bar}", &schema).expect("parse ok");
    assert!(collect_df_field_terms(&tag_only, &idx).is_empty(), "pure filter → no df entry");

    // Pure fuzzy → has a text leaf but no Exact term → still ONE entry (empty terms) so N is gathered.
    let fuzzy = parse_query(b"%alph%", &schema).expect("parse ok");
    let fq = collect_df_field_terms(&fuzzy, &idx);
    assert_eq!(fq.len(), 1, "fuzzy text leaf must still yield one entry so global N is gathered");
    assert!(fq[0].1.is_empty(), "fuzzy terms use LOCAL df → no exact df terms collected");
}
