//! RED tests for moon#690 — FT.SEARCH could not find ordinary English words.
//!
//! Two independent defects produced the same symptom (an empty result set, no error):
//!
//! 1. **The list.** `AnalyzerPipeline` filtered against `stop_words::get(LANGUAGE::English)`,
//!    which resolves to the *stopwords-iso* list — 1,298 entries including `hello`, `world`,
//!    `test`, `name`, `order`, `open` and `index`. Those words never reached the index, so a
//!    document whose only word was `hello` indexed zero terms and was unreachable by its own
//!    content. RediSearch's default list is 33 words.
//!
//! 2. **The asymmetry.** A query token that analyzed to nothing (a stop word) evaluated to ∅
//!    and was then *intersected* into its conjunction, so a stop word anywhere in a query
//!    zeroed the whole result set — `alpha the` returned 0 on a corpus where `alpha` returned
//!    2. RediSearch removes stop words from the query instead.
//!
//! Fixing only the list would leave `alpha the` broken for the 33 words that remain, and
//! fixing only the asymmetry would leave `hello` unindexed — hence both are asserted here.
#![cfg(feature = "text-index")]

use std::collections::BTreeSet;

use bytes::Bytes;
use moon::command::vector_search::run_text_query;
use moon::protocol::Frame;
use moon::text::analyzer::{AnalyzerPipeline, DEFAULT_STOP_WORDS};
use moon::text::store::{TextIndex, TextStore};
use moon::text::types::{BM25Config, NumericFieldDef, TagFieldDef, TextFieldDef};

// ── harness ─────────────────────────────────────────────────────────────────

fn analyzer() -> AnalyzerPipeline {
    AnalyzerPipeline::new(rust_stemmers::Algorithm::English, false)
}

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

/// FT.SEARCH reply is `[total, key, fields, ...]` — keys are the odd indices.
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

// ── 1. the list ─────────────────────────────────────────────────────────────

/// The default list is RediSearch's 33 words, exactly — not a 1,298-word superset.
#[test]
fn default_stop_word_list_is_redisearchs_thirty_three() {
    // https://redis.io/docs/latest/develop/interact/search-and-query/advanced-concepts/stopwords/
    let expected: BTreeSet<&str> = [
        "a", "is", "the", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in",
        "into", "it", "no", "not", "of", "on", "or", "such", "that", "their", "then", "there",
        "these", "they", "this", "to", "was", "will", "with",
    ]
    .into_iter()
    .collect();

    assert_eq!(expected.len(), 33, "the reference list itself is 33 words");
    assert_eq!(
        DEFAULT_STOP_WORDS.iter().copied().collect::<BTreeSet<_>>(),
        expected,
        "moon's default stop-word list must be RediSearch's, word for word"
    );
    assert_eq!(
        DEFAULT_STOP_WORDS.len(),
        33,
        "no duplicates hiding in the array"
    );
}

/// The words the stopwords-iso list swallowed are ordinary content words. Every one of them
/// must survive the analyzer and reach the index.
#[test]
fn ordinary_english_words_survive_the_analyzer() {
    let a = analyzer();
    // Sampled from the ~1,265 words stopwords-iso drops that RediSearch indexes.
    for word in [
        "hello", "world", "test", "name", "order", "open", "index", "here", "second", "third",
        "text", "another", "words", "more", "user", "value", "search", "new", "old", "case",
    ] {
        assert_eq!(
            a.tokenize_with_positions(word).len(),
            1,
            "`{word}` is a content word and must be indexed, not dropped as a stop word"
        );
    }
}

/// ...and the 33 that remain are still dropped. The fix is a smaller list, not no list.
#[test]
fn the_redisearch_stop_words_are_still_dropped() {
    let a = analyzer();
    for word in DEFAULT_STOP_WORDS {
        assert!(
            a.tokenize_with_positions(word).is_empty(),
            "`{word}` is on the default stop-word list and must not be indexed"
        );
    }
}

// ── 2. the query/index asymmetry ────────────────────────────────────────────

/// A document whose only word is `hello` is reachable by `hello`. This is the #690 headline:
/// under the iso list it indexed zero terms and nothing could ever match it.
#[test]
fn a_document_whose_only_word_is_hello_is_findable() {
    let mut idx = empty_index();
    add_doc(&mut idx, 1, "greeting", &[("body", "hello")]);
    add_doc(&mut idx, 2, "other", &[("body", "alpha")]);
    idx.build_fst();
    let ts = store_of(idx);

    assert_eq!(
        keys_set(&search(&ts, "hello")),
        set_of(&["greeting"]),
        "a one-word document must be findable by its only word"
    );
}

/// A stop word inside a conjunction is REMOVED from the query, not intersected as ∅.
/// `alpha the` means `alpha` — the corpus contains no stop words at all, so any behaviour
/// other than "identical to `alpha`" is the asymmetry.
#[test]
fn a_stop_word_in_a_conjunction_does_not_zero_the_result() {
    let mut idx = empty_index();
    add_doc(&mut idx, 1, "a", &[("body", "alpha bravo")]);
    add_doc(&mut idx, 2, "b", &[("body", "alpha charlie")]);
    add_doc(&mut idx, 3, "c", &[("body", "delta")]);
    idx.build_fst();
    let ts = store_of(idx);

    let baseline = keys_set(&search(&ts, "alpha"));
    assert_eq!(baseline, set_of(&["a", "b"]), "control: `alpha` matches 2");

    for query in ["alpha the", "the alpha", "alpha the bravo", "of alpha to"] {
        let got = keys_set(&search(&ts, query));
        let want = if query.contains("bravo") {
            set_of(&["a"])
        } else {
            baseline.clone()
        };
        assert_eq!(
            got, want,
            "`{query}`: stop words must drop out of the conjunction, leaving the rest intact"
        );
    }
}

/// A field-scoped stop word drops out too — `@title:the` carries no constraint.
#[test]
fn a_field_scoped_stop_word_drops_out_of_the_conjunction() {
    let mut idx = empty_index();
    add_doc(&mut idx, 1, "a", &[("body", "alpha"), ("title", "bravo")]);
    add_doc(&mut idx, 2, "b", &[("body", "delta"), ("title", "bravo")]);
    idx.build_fst();
    let ts = store_of(idx);

    assert_eq!(
        keys_set(&search(&ts, "@body:alpha @title:the")),
        set_of(&["a"]),
        "`@title:the` is a stop word and must not zero the conjunction"
    );
}

/// A query made only of stop words matches nothing — and says so as an empty result set,
/// not an error and not the whole index.
#[test]
fn a_query_of_only_stop_words_matches_nothing() {
    let mut idx = empty_index();
    add_doc(&mut idx, 1, "a", &[("body", "alpha")]);
    add_doc(&mut idx, 2, "b", &[("body", "bravo")]);
    idx.build_fst();
    let ts = store_of(idx);

    for query in ["the", "the and of", "@body:the"] {
        let r = search(&ts, query);
        assert!(
            !matches!(r, Frame::Error(_)),
            "`{query}` is a well-formed query, not an error"
        );
        assert_eq!(
            total(&r),
            0,
            "`{query}` analyzes to no terms and must match nothing, not everything"
        );
    }
}

/// A term that is NOT a stop word but is simply absent still zeroes its conjunction —
/// the fix must discriminate "analyzed to nothing" from "matched nothing".
#[test]
fn an_absent_term_still_zeroes_the_conjunction() {
    let mut idx = empty_index();
    add_doc(&mut idx, 1, "a", &[("body", "alpha bravo")]);
    idx.build_fst();
    let ts = store_of(idx);

    assert_eq!(
        total(&search(&ts, "alpha zzzznotindexed")),
        0,
        "an indexable term that matches no document must still zero the AND"
    );
}
