//! moon#1191 red → green through PUBLIC APIs only (compiles unchanged against HEAD `935c555`,
//! where it fails): FT.SEARCH (`eval_query_counted`, top 10) of a broad term used to score every
//! match TWICE and fully sort; it must now cost less than ONE full `search_field` pass. Timing
//! uses best-of-N and a generous ratio (same binary, same data, back to back).

#![cfg(feature = "text-index")]

use std::time::{Duration, Instant};

use bytes::Bytes;
use moon::protocol::Frame;
use moon::text::query::{QuerySchema, eval_query_counted, parse_query};
use moon::text::store::TextIndex;
use moon::text::types::{BM25Config, TextFieldDef};

fn best_of(n: usize, mut f: impl FnMut()) -> Duration {
    (0..n)
        .map(|_| {
            let t = Instant::now();
            f();
            t.elapsed()
        })
        .min()
        .unwrap_or_default()
}

/// moon#1191. HEAD: `eval_set` ran `search_field(.., usize::MAX)` (full score + full sort + a key
/// clone per match) just to collect ids, then scored every leaf AGAIN into a HashMap, then cloned
/// a key per match and fully sorted — > 2x one `search_field` pass. Now: bitmap membership plus
/// ONE cursor-driven scoring pass into a bounded heap.
#[test]
fn broad_ft_search_scores_each_match_once() {
    let mut idx = TextIndex::new(
        Bytes::from_static(b"red"),
        vec![Bytes::from_static(b"d:")],
        vec![TextFieldDef::new(Bytes::from_static(b"body"))],
        BM25Config::default(),
    );
    // "common" in every doc with a DISTINCT tf pattern; filler words vary.
    for d in 0..6_000u32 {
        let key = format!("d:{d}");
        let mut body = String::new();
        for _ in 0..=(d % 4) {
            body.push_str("common ");
        }
        body.push_str(&format!("w{} x{}", d % 97, d % 13));
        let args = [
            Frame::BulkString(Bytes::from_static(b"body")),
            Frame::BulkString(Bytes::from(body)),
        ];
        idx.index_document(
            xxhash_rust::xxh64::xxh64(key.as_bytes(), 0),
            key.as_bytes(),
            &args,
        );
    }
    let schema = QuerySchema::from_index(&idx);
    let node = parse_query(b"common", &schema).expect("parse");
    let (page, total) = eval_query_counted(&idx, &node, None, None, 10);
    assert_eq!(total, 6_000);
    let all = idx.search_field(0, &["common".to_owned()], None, None, usize::MAX);
    assert_eq!(all.len(), 6_000);
    for (a, b) in page.iter().zip(&all) {
        assert_eq!((a.doc_id, a.score.to_bits()), (b.doc_id, b.score.to_bits()));
    }
    let eval = best_of(7, || {
        std::hint::black_box(eval_query_counted(&idx, &node, None, None, 10));
    });
    let one_pass = best_of(7, || {
        std::hint::black_box(idx.search_field(0, &["common".to_owned()], None, None, usize::MAX));
    });
    assert!(
        eval.as_secs_f64() < one_pass.as_secs_f64() * 1.2,
        "top-10 FT.SEARCH took {eval:?}, one full scoring pass {one_pass:?}: still scoring twice"
    );
}
