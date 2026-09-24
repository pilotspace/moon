//! Full-text search micro-benchmarks (moon#1191, moon#1195).
//!
//! A Zipf(s=1.1) corpus over a 2,000-word vocabulary, 30 tokens per document, so term document
//! frequencies span four orders of magnitude. Measured through the same entry points FT.SEARCH
//! uses (`parse_query` + `eval_query_counted`, `LIMIT 0 10`):
//!
//! * `ft_search/single/<rank>` — one term at a given frequency rank (rank 0 = broadest), so the
//!   cost can be read against the match-set size printed at setup. Before moon#1191 the cost was
//!   ~2 full BM25 passes + a full sort per matched document; after, one cursor-driven pass with a
//!   bounded top-k.
//! * `ft_search/and2` — two broad terms (rarest-first intersection, then one scoring pass).
//! * `ft_upsert/doc_first` vs `ft_upsert/doc_last` (moon#1195) — re-indexing the OLDEST vs the
//!   NEWEST document of the same corpus. With rank-aligned flat posting arrays the oldest doc
//!   paid a memmove of every posting it touches (O(Σ posting length)); with chunked columns the
//!   two must cost the same.
//!
//! `black_box` wraps inputs and outputs (CLAUDE.md criterion rule).
//!
//! Run: `cargo bench --bench text_search` (override the corpus size with `MOON_TEXT_BENCH_DOCS`).

use std::hint::black_box;

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};

use moon::protocol::Frame;
use moon::text::query::{QuerySchema, eval_query_counted, parse_query};
use moon::text::store::TextIndex;
use moon::text::types::{BM25Config, TextFieldDef};

const SYL: [&str; 10] = ["ka", "lo", "mi", "nu", "pe", "ro", "su", "ti", "va", "zo"];

fn vocabulary(n: usize) -> Vec<String> {
    let mut out = Vec::with_capacity(n);
    for a in SYL {
        for b in SYL {
            for c in SYL {
                for d in SYL {
                    out.push(format!("{a}{b}{c}{d}"));
                    if out.len() == n {
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
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

fn zipf_cdf(n: usize) -> Vec<f64> {
    let mut cdf = Vec::with_capacity(n);
    let mut acc = 0.0;
    for r in 0..n {
        acc += 1.0 / ((r + 1) as f64).powf(1.1);
        cdf.push(acc);
    }
    for c in &mut cdf {
        *c /= acc;
    }
    cdf
}

fn doc_args(rng: &mut Rng, cdf: &[f64], vocab: &[String]) -> Vec<Frame> {
    let mut body = String::new();
    for i in 0..30 {
        if i > 0 {
            body.push(' ');
        }
        let r = cdf
            .partition_point(|&c| c < rng.unit())
            .min(vocab.len() - 1);
        body.push_str(&vocab[r]);
    }
    vec![
        Frame::BulkString(Bytes::from_static(b"body")),
        Frame::BulkString(Bytes::from(body)),
    ]
}

fn build(docs: usize) -> (TextIndex, Vec<String>, Vec<f64>) {
    let vocab = vocabulary(2_000);
    let cdf = zipf_cdf(vocab.len());
    let mut idx = TextIndex::new(
        Bytes::from_static(b"bench"),
        vec![Bytes::from_static(b"doc:")],
        vec![TextFieldDef::new(Bytes::from_static(b"body"))],
        BM25Config::default(),
    );
    let mut rng = Rng(42);
    for d in 0..docs {
        let key = format!("doc:{d}");
        let args = doc_args(&mut rng, &cdf, &vocab);
        idx.index_document(
            xxhash_rust::xxh64::xxh64(key.as_bytes(), 0),
            key.as_bytes(),
            &args,
        );
    }
    (idx, vocab, cdf)
}

fn corpus_size() -> usize {
    std::env::var("MOON_TEXT_BENCH_DOCS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000)
}

fn bench_search(c: &mut Criterion) {
    let (idx, vocab, _) = build(corpus_size());
    let schema = QuerySchema::from_index(&idx);
    let mut group = c.benchmark_group("ft_search");
    for rank in [0usize, 3, 30, 300] {
        let node = parse_query(vocab[rank].as_bytes(), &schema).expect("parse");
        let (_, total) = eval_query_counted(&idx, &node, None, None, 10);
        eprintln!("ft_search/single/{rank}: {total} matching docs");
        group.bench_function(format!("single/{rank}"), |b| {
            b.iter(|| {
                black_box(eval_query_counted(
                    black_box(&idx),
                    black_box(&node),
                    None,
                    None,
                    black_box(10),
                ))
            })
        });
    }
    let q = format!("{} {}", vocab[0], vocab[1]);
    let node = parse_query(q.as_bytes(), &schema).expect("parse");
    group.bench_function("and2", |b| {
        b.iter(|| {
            black_box(eval_query_counted(
                black_box(&idx),
                black_box(&node),
                None,
                None,
                black_box(10),
            ))
        })
    });
    group.finish();
}

fn bench_upsert(c: &mut Criterion) {
    let docs = corpus_size();
    let (mut idx, vocab, cdf) = build(docs);
    let mut rng = Rng(7);
    let mut group = c.benchmark_group("ft_upsert");
    for (label, d) in [("doc_first", 0usize), ("doc_last", docs - 1)] {
        let key = format!("doc:{d}");
        let key_hash = xxhash_rust::xxh64::xxh64(key.as_bytes(), 0);
        group.bench_function(label, |b| {
            b.iter(|| {
                let args = doc_args(&mut rng, &cdf, &vocab);
                idx.index_document(black_box(key_hash), black_box(key.as_bytes()), &args);
                black_box(idx.num_docs())
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_search, bench_upsert);
criterion_main!(benches);
