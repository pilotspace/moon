//! RED tests for moon#693 — `FT.SEARCH <index> "*"`, the match-all query.
//!
//! `*` is how RediSearch says "every document in this index"; it is what every "show me
//! what is in here" example uses. moon refused it on every index, and the refusal named
//! KNN (`ERR invalid KNN query syntax`) even on an index with no vector field — so a user
//! trying to enumerate a TEXT index was told the vector syntax was wrong.
//!
//! Scope: `*` is answered by the inverted index, which is where the document registry
//! lives. An index built from VECTOR fields alone has no inverted index, and `*` there
//! behaves exactly as every other text query already does on such an index
//! (`ERR no such index`) — pre-existing, uniform, and not changed here.
#![cfg(feature = "text-index")]

use std::collections::BTreeSet;

use bytes::Bytes;
use moon::command::vector_search::{is_text_query, run_text_query};
use moon::protocol::Frame;
use moon::text::store::{TextIndex, TextStore};
use moon::text::types::{BM25Config, NumericFieldDef, TagFieldDef, TextFieldDef};

// ── harness ─────────────────────────────────────────────────────────────────

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

fn args(pairs: &[(&str, &str)]) -> Vec<Frame> {
    let mut v = Vec::with_capacity(pairs.len() * 2);
    for (f, val) in pairs {
        v.push(Frame::BulkString(Bytes::copy_from_slice(f.as_bytes())));
        v.push(Frame::BulkString(Bytes::copy_from_slice(val.as_bytes())));
    }
    v
}

fn add_doc(idx: &mut TextIndex, hash: u64, key: &str, text: &[(&str, &str)]) {
    idx.index_document(hash, key.as_bytes(), &args(text));
}

fn store_of(idx: TextIndex) -> TextStore {
    let mut ts = TextStore::new();
    ts.create_index(Bytes::from_static(b"idx"), idx)
        .expect("create_index ok");
    ts
}

fn search(ts: &TextStore, q: &str) -> Frame {
    run_text_query(ts, b"idx", q.as_bytes(), 1000, 0, usize::MAX, 0)
}

fn search_page(ts: &TextStore, q: &str, offset: usize, count: usize) -> Frame {
    run_text_query(ts, b"idx", q.as_bytes(), 1000, offset, count, 0)
}

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

fn total(frame: &Frame) -> i64 {
    match frame {
        Frame::Array(items) => match items.first() {
            Some(Frame::Integer(n)) => *n,
            _ => -1,
        },
        other => panic!("expected Frame::Array, got {other:?}"),
    }
}

fn set_of(keys: &[&str]) -> BTreeSet<Vec<u8>> {
    keys.iter().map(|k| k.as_bytes().to_vec()).collect()
}

// ── routing ─────────────────────────────────────────────────────────────────

/// `*` is a query for the text engine, not a malformed KNN query. This is the routing
/// decision every FT.SEARCH handler consults, so flipping it here fixes them all at once.
#[test]
fn star_routes_to_the_text_engine() {
    assert!(
        is_text_query(b"*"),
        "`*` is match-all — it belongs to the text engine, not the KNN parser"
    );
    // ...without dragging real KNN queries along with it.
    assert!(!is_text_query(b"*=>[KNN 10 @vec $query]"));
    assert!(!is_text_query(b"@year:[2020 2024]=>[KNN 5 @v $q]"));
}

// ── semantics ───────────────────────────────────────────────────────────────

#[test]
fn match_all_returns_every_document_in_the_index() {
    let mut idx = empty_index();
    add_doc(&mut idx, 1, "a", &[("body", "alpha")]);
    add_doc(&mut idx, 2, "b", &[("body", "bravo")]);
    add_doc(&mut idx, 3, "c", &[("title", "charlie")]);
    idx.build_fst();
    let ts = store_of(idx);

    let r = search(&ts, "*");
    assert!(
        !matches!(r, Frame::Error(_)),
        "`*` must not be an error: {r:?}"
    );
    assert_eq!(total(&r), 3, "`*` must report every document");
    assert_eq!(keys_set(&r), set_of(&["a", "b", "c"]));
}

/// The point of match-all is enumeration, so it must also return documents that no term
/// query can reach — here, one whose text analyzes to nothing at all.
#[test]
fn match_all_returns_documents_no_term_query_can_reach() {
    let mut idx = empty_index();
    add_doc(&mut idx, 1, "findable", &[("body", "alpha")]);
    add_doc(&mut idx, 2, "termless", &[("body", "the and of")]);
    idx.build_fst();
    let ts = store_of(idx);

    assert_eq!(
        keys_set(&search(&ts, "*")),
        set_of(&["findable", "termless"]),
        "a document with no indexable term is still IN the index and `*` must list it"
    );
}

#[test]
fn match_all_on_an_empty_index_is_zero_not_an_error() {
    let mut idx = empty_index();
    idx.build_fst();
    let ts = store_of(idx);

    let r = search(&ts, "*");
    assert!(!matches!(r, Frame::Error(_)), "empty index: {r:?}");
    assert_eq!(total(&r), 0);
}

/// `*` must not swallow the prefix operator: `alp*` is still a prefix query.
#[test]
fn match_all_does_not_break_prefix_queries() {
    let mut idx = empty_index();
    add_doc(&mut idx, 1, "a", &[("body", "alphabet")]);
    add_doc(&mut idx, 2, "b", &[("body", "bravo")]);
    idx.build_fst();
    let ts = store_of(idx);

    assert_eq!(
        keys_set(&search(&ts, "alp*")),
        set_of(&["a"]),
        "`alp*` is a prefix query, not match-all"
    );
    assert_eq!(keys_set(&search(&ts, "*")), set_of(&["a", "b"]));
}

/// LIMIT still pages a match-all, and `total` reports the full count, not the page size.
#[test]
fn match_all_pages_under_limit_but_reports_the_full_total() {
    let mut idx = empty_index();
    for (i, key) in ["a", "b", "c", "d", "e"].iter().enumerate() {
        add_doc(&mut idx, i as u64 + 1, key, &[("body", "alpha")]);
    }
    idx.build_fst();
    let ts = store_of(idx);

    let page = search_page(&ts, "*", 0, 2);
    assert_eq!(total(&page), 5, "total is the whole index, not the page");
    assert_eq!(keys_set(&page).len(), 2, "LIMIT 0 2 returns two documents");
}

// FT.AGGREGATE is deliberately NOT changed here. `materialize_rows` already short-circuits
// `*` (and the empty query) straight to the document registry, so it never reaches the query
// parser and was correct before this fix — verified by reading the path, not assumed. Its own
// module tests cover it; a thin pin here would only have re-asserted the parse step.
