//! moon#1220 (item 1) red → green through PUBLIC APIs only (compiles unchanged against the wave-1
//! base `f32546c`, where it fails): a prefix query that expands to 50 terms over (nearly) every
//! document used to seek all 50 posting cursors for every matched document — 50 seeks per match —
//! so it cost many times a single broad term over the same documents. Scored term-at-a-time it
//! walks each expanded posting once, and costs a small multiple of that single term.
//!
//! Timing is best-of-N, same corpus, back to back. Calibration (unoptimised `cargo test` build,
//! 30K docs, shared 4-vCPU box): base 98 ms vs 11.2 ms = 8.7x; term-at-a-time 34.6 ms vs 11.6 ms
//! = 3.0x. Optimised builds widen the gap (the per-seek overhead the base pays 50x per match is a
//! larger share there), so the 6x bound sits between the two with margin on both sides.

#![cfg(feature = "text-index")]

use std::time::{Duration, Instant};

use bytes::Bytes;
use moon::protocol::Frame;
use moon::text::query::{QuerySchema, eval_query_counted, parse_query};
use moon::text::store::TextIndex;
use moon::text::types::{BM25Config, TextFieldDef};

const SYL: [&str; 10] = ["ka", "lo", "mi", "nu", "pe", "ro", "su", "ti", "va", "zo"];

/// 2,000 four-syllable words; the first 1,000 all start with `ka`, so `ka*` expands to the
/// 50 highest-df terms (capped expansion) — which are also the Zipf head.
fn vocabulary() -> Vec<String> {
    let mut out = Vec::with_capacity(2_000);
    for a in SYL {
        for b in SYL {
            for c in SYL {
                for d in SYL {
                    out.push(format!("{a}{b}{c}{d}"));
                    if out.len() == 2_000 {
                        return out;
                    }
                }
            }
        }
    }
    out
}

struct Rng(u64);
impl Rng {
    fn unit(&mut self) -> f64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        ((z ^ (z >> 31)) >> 11) as f64 / (1u64 << 53) as f64
    }
}

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

#[test]
fn wide_prefix_expansion_costs_a_small_multiple_of_one_broad_term() {
    const DOCS: usize = 30_000;
    let vocab = vocabulary();
    let mut cdf = Vec::with_capacity(vocab.len());
    let mut acc = 0.0;
    for r in 0..vocab.len() {
        acc += 1.0 / ((r + 1) as f64).powf(1.1);
        cdf.push(acc);
    }
    let mut idx = TextIndex::new(
        Bytes::from_static(b"p"),
        vec![Bytes::from_static(b"d:")],
        vec![TextFieldDef::new(Bytes::from_static(b"body"))],
        BM25Config::default(),
    );
    let mut rng = Rng(1220);
    for d in 0..DOCS {
        let mut body = String::new();
        for i in 0..30 {
            if i > 0 {
                body.push(' ');
            }
            let u = rng.unit() * acc;
            body.push_str(&vocab[cdf.partition_point(|&c| c < u).min(vocab.len() - 1)]);
        }
        let key = format!("d:{d}");
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
    idx.build_fst();
    let schema = QuerySchema::from_index(&idx);
    let prefix = parse_query(b"ka*", &schema).expect("parse prefix");
    let broad = parse_query(vocab[0].as_bytes(), &schema).expect("parse term");

    let (page, prefix_total) = eval_query_counted(&idx, &prefix, None, None, 10);
    let (_, broad_total) = eval_query_counted(&idx, &broad, None, None, 10);
    assert_eq!(page.len(), 10);
    // Same order of work on the document side: both match (nearly) every document.
    assert!(
        broad_total * 10 >= DOCS * 9,
        "broad term matched {broad_total}"
    );
    assert!(
        prefix_total >= broad_total,
        "{prefix_total} < {broad_total}"
    );

    let t_prefix = best_of(5, || {
        std::hint::black_box(eval_query_counted(&idx, &prefix, None, None, 10));
    });
    let t_broad = best_of(5, || {
        std::hint::black_box(eval_query_counted(&idx, &broad, None, None, 10));
    });
    let ratio = t_prefix.as_secs_f64() / t_broad.as_secs_f64();
    eprintln!("prefix (50 terms) {t_prefix:?} vs one broad term {t_broad:?}: {ratio:.1}x");
    assert!(
        ratio < 6.0,
        "a 50-term prefix took {t_prefix:?} = {ratio:.1}x one broad term ({t_broad:?}): \
         still seeking every expanded cursor per matched document"
    );
}
