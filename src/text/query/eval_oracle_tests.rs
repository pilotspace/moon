//! moon#1191 differential harness: the pre-#1191 (HEAD `935c555`) two-pass evaluator and the
//! pre-#1191 `search_field` / `search_field_or` / AS_OF wrappers, kept VERBATIM (modulo `self` ->
//! `idx`) as a test oracle, compared against the live single-pass implementation on randomized
//! Zipf corpora with distinct, asymmetric term frequencies (CONVENTIONS: equal tfs mask rank
//! misalignment).
//!
//! The comparison is stricter than "within f32 tolerance": result ids, order, `total` and the
//! score BITS must be identical — the live kernel preserves HEAD's f32 operation order, and the
//! wire formats scores with `{:.6}`, so bit-identity is what byte-identical replies need.

use std::collections::HashMap;

use bytes::Bytes;
use roaring::RoaringBitmap;

use super::*;
use crate::protocol::Frame;
use crate::text::bm25::bm25_score;
use crate::text::query::{QuerySchema, parse_query};
use crate::text::store::{TermModifier, TextIndex, TextSearchResult};
use crate::text::types::{BM25Config, NumericFieldDef, TagFieldDef, TextFieldDef};

// ── HEAD oracle (verbatim logic) ────────────────────────────────────────────────────────────────

fn head_search_field(
    idx: &TextIndex,
    field_idx: usize,
    query_terms: &[String],
    global_df: Option<&HashMap<String, u32>>,
    global_n: Option<u32>,
    top_k: usize,
) -> Vec<TextSearchResult> {
    if field_idx >= idx.field_postings.len() || query_terms.is_empty() {
        return Vec::new();
    }
    let mut term_postings: Vec<(String, u32)> = Vec::with_capacity(query_terms.len());
    for term in query_terms {
        let term_id = match idx.field_term_dicts[field_idx].get(term) {
            Some(id) => id,
            None => return Vec::new(),
        };
        if idx.field_postings[field_idx].get_posting(term_id).is_none() {
            return Vec::new();
        }
        term_postings.push((term.clone(), term_id));
    }
    let mut candidate_bitmap: RoaringBitmap = {
        let Some(first_posting) = idx.field_postings[field_idx].get_posting(term_postings[0].1)
        else {
            return Vec::new();
        };
        first_posting.doc_ids.clone()
    };
    for (_, term_id) in &term_postings[1..] {
        let Some(posting) = idx.field_postings[field_idx].get_posting(*term_id) else {
            return Vec::new();
        };
        candidate_bitmap &= &posting.doc_ids;
    }
    if candidate_bitmap.is_empty() {
        return Vec::new();
    }
    let stats = &idx.field_stats[field_idx];
    let n = global_n.unwrap_or(stats.num_docs);
    let avgdl = stats.avg_doc_len();
    let k1 = idx.bm25_config.k1;
    let b = idx.bm25_config.b;
    let weight = idx.text_fields[field_idx].weight as f32;
    let mut results: Vec<TextSearchResult> = Vec::with_capacity(candidate_bitmap.len() as usize);
    for doc_id in &candidate_bitmap {
        let dl = idx.doc_field_len(doc_id, field_idx);
        let mut doc_score = 0.0f32;
        for (term, term_id) in &term_postings {
            let Some(posting) = idx.field_postings[field_idx].get_posting(*term_id) else {
                continue;
            };
            let tf = posting.tf(doc_id) as f32;
            let df = global_df
                .and_then(|m| m.get(term.as_str()).copied())
                .unwrap_or_else(|| posting.doc_ids.len() as u32);
            doc_score += bm25_score(tf, df, n, dl, avgdl, k1, b) * weight;
        }
        let key = match idx.doc_id_to_key.get(&doc_id) {
            Some(k) => k.clone(),
            None => continue,
        };
        results.push(TextSearchResult {
            doc_id,
            key,
            score: doc_score,
        });
    }
    results.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    results.truncate(top_k);
    results
}

fn head_search_field_or(
    idx: &TextIndex,
    field_idx: usize,
    expanded_term_ids: &[u32],
    global_n: Option<u32>,
    top_k: usize,
) -> Vec<TextSearchResult> {
    if field_idx >= idx.field_postings.len() || expanded_term_ids.is_empty() {
        return Vec::new();
    }
    let mut candidate_bitmap = RoaringBitmap::new();
    for &term_id in expanded_term_ids {
        if let Some(posting) = idx.field_postings[field_idx].get_posting(term_id) {
            candidate_bitmap |= &posting.doc_ids;
        }
    }
    if candidate_bitmap.is_empty() {
        return Vec::new();
    }
    let stats = &idx.field_stats[field_idx];
    let n = global_n.unwrap_or(stats.num_docs);
    let avgdl = stats.avg_doc_len();
    let k1 = idx.bm25_config.k1;
    let b = idx.bm25_config.b;
    let weight = idx.text_fields[field_idx].weight as f32;
    let mut results: Vec<TextSearchResult> = Vec::with_capacity(candidate_bitmap.len() as usize);
    for doc_id in &candidate_bitmap {
        let dl = idx.doc_field_len(doc_id, field_idx);
        let mut best_score = 0.0f32;
        for &term_id in expanded_term_ids {
            let Some(posting) = idx.field_postings[field_idx].get_posting(term_id) else {
                continue;
            };
            if !posting.doc_ids.contains(doc_id) {
                continue;
            }
            let tf = posting.tf(doc_id) as f32;
            let df = posting.doc_ids.len() as u32;
            let score = bm25_score(tf, df, n, dl, avgdl, k1, b) * weight;
            if score > best_score {
                best_score = score;
            }
        }
        let key = match idx.doc_id_to_key.get(&doc_id) {
            Some(k) => k.clone(),
            None => continue,
        };
        results.push(TextSearchResult {
            doc_id,
            key,
            score: best_score,
        });
    }
    results.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    results.truncate(top_k);
    results
}

fn head_as_of(
    idx: &TextIndex,
    raw: Vec<TextSearchResult>,
    top_k: usize,
    as_of_lsn: u64,
) -> Vec<TextSearchResult> {
    let mut filtered: Vec<TextSearchResult> = raw
        .into_iter()
        .filter(|r| idx.is_doc_visible_at(r.doc_id, as_of_lsn))
        .collect();
    filtered.truncate(top_k);
    filtered
}

fn head_eval_set(node: &QueryNode, idx: &TextIndex) -> RoaringBitmap {
    match node {
        QueryNode::Empty => RoaringBitmap::new(),
        QueryNode::MatchAll => idx.doc_id_to_key.keys().copied().collect(),
        QueryNode::Term {
            field,
            token,
            modifier,
        } => head_term_results(idx, *field, token, modifier, None, None, usize::MAX)
            .into_iter()
            .map(|r| r.doc_id)
            .collect(),
        QueryNode::Tag { field, values } => {
            let mut bm = RoaringBitmap::new();
            for value in values {
                for doc_id in idx.search_tag(field, value) {
                    bm.insert(doc_id);
                }
            }
            bm
        }
        QueryNode::Numeric {
            field,
            min,
            max,
            min_excl,
            max_excl,
        } => {
            let mut bm = RoaringBitmap::new();
            for doc_id in idx.search_numeric_range(field, *min, *max, *min_excl, *max_excl) {
                bm.insert(doc_id);
            }
            bm
        }
        QueryNode::And(children) => {
            let mut iter = children.iter().filter(|c| !is_stop_word_only(c, idx));
            let Some(first) = iter.next() else {
                return RoaringBitmap::new();
            };
            let mut acc = head_eval_set(first, idx);
            for child in iter {
                if acc.is_empty() {
                    break;
                }
                acc &= &head_eval_set(child, idx);
            }
            acc
        }
        QueryNode::Or(children) => {
            let mut acc = RoaringBitmap::new();
            for child in children {
                acc |= &head_eval_set(child, idx);
            }
            acc
        }
    }
}

fn head_eval_query_counted(
    idx: &TextIndex,
    node: &QueryNode,
    global_df: Option<&HashMap<String, u32>>,
    global_n: Option<u32>,
    top_k: usize,
) -> (Vec<TextSearchResult>, usize) {
    let set = head_eval_set(node, idx);
    if set.is_empty() {
        return (Vec::new(), 0);
    }
    let mut scores: HashMap<u32, f32> = HashMap::new();
    head_accumulate_text_scores(node, idx, global_df, global_n, &mut scores);
    let mut results: Vec<TextSearchResult> = set
        .iter()
        .filter_map(|doc_id| {
            idx.doc_id_to_key.get(&doc_id).map(|key| TextSearchResult {
                doc_id,
                key: key.clone(),
                score: scores.get(&doc_id).copied().unwrap_or(0.0),
            })
        })
        .collect();
    let total_matched = results.len();
    results.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.doc_id.cmp(&b.doc_id))
    });
    results.truncate(top_k);
    (results, total_matched)
}

fn head_accumulate_text_scores(
    node: &QueryNode,
    idx: &TextIndex,
    global_df: Option<&HashMap<String, u32>>,
    global_n: Option<u32>,
    scores: &mut HashMap<u32, f32>,
) {
    match node {
        QueryNode::Empty
        | QueryNode::MatchAll
        | QueryNode::Tag { .. }
        | QueryNode::Numeric { .. } => {}
        QueryNode::Term {
            field,
            token,
            modifier,
        } => {
            for r in head_term_results(
                idx,
                *field,
                token,
                modifier,
                global_df,
                global_n,
                usize::MAX,
            ) {
                *scores.entry(r.doc_id).or_insert(0.0) += r.score;
            }
        }
        QueryNode::And(children) | QueryNode::Or(children) => {
            for child in children {
                head_accumulate_text_scores(child, idx, global_df, global_n, scores);
            }
        }
    }
}

fn head_term_results(
    idx: &TextIndex,
    field: Option<usize>,
    raw: &Bytes,
    modifier: &TermModifier,
    global_df: Option<&HashMap<String, u32>>,
    global_n: Option<u32>,
    top_k: usize,
) -> Vec<TextSearchResult> {
    let Ok(raw_str) = std::str::from_utf8(raw) else {
        return Vec::new();
    };
    match field {
        Some(fidx) => {
            head_term_results_in_field(idx, fidx, raw_str, modifier, global_df, global_n, top_k)
        }
        None => {
            let mut acc: HashMap<u32, (f32, Bytes)> = HashMap::new();
            for fidx in 0..idx.text_fields.len() {
                if idx.text_fields[fidx].noindex {
                    continue;
                }
                for r in head_term_results_in_field(
                    idx, fidx, raw_str, modifier, global_df, global_n, top_k,
                ) {
                    let entry = acc.entry(r.doc_id).or_insert((0.0, r.key.clone()));
                    entry.0 += r.score;
                }
            }
            acc.into_iter()
                .map(|(doc_id, (score, key))| TextSearchResult { doc_id, key, score })
                .collect()
        }
    }
}

fn head_term_results_in_field(
    idx: &TextIndex,
    fidx: usize,
    raw_str: &str,
    modifier: &TermModifier,
    global_df: Option<&HashMap<String, u32>>,
    global_n: Option<u32>,
    top_k: usize,
) -> Vec<TextSearchResult> {
    match modifier {
        TermModifier::Exact => {
            let terms = analyze_raw(idx, fidx, raw_str, modifier);
            if terms.is_empty() {
                return Vec::new();
            }
            head_search_field(idx, fidx, &terms, global_df, global_n, top_k)
        }
        TermModifier::Fuzzy(_) | TermModifier::Prefix => {
            let normalized = analyze_raw(idx, fidx, raw_str, modifier);
            let Some(normalized) = normalized.first() else {
                return Vec::new();
            };
            let ids = idx.expand_terms(fidx, normalized, modifier);
            if ids.is_empty() {
                return Vec::new();
            }
            head_search_field_or(idx, fidx, &ids, global_n, top_k)
        }
    }
}

// ── Randomized corpora ──────────────────────────────────────────────────────────────────────────

/// SplitMix64 — deterministic, dependency-free.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

const SYL: [&str; 10] = ["ka", "lo", "mi", "nu", "pe", "ro", "su", "ti", "va", "zo"];

/// Vocabulary of `n` pronounceable, stem-stable words (`kalo`, `kalomi`, …) sharing prefixes so
/// prefix/fuzzy expansion has real fan-out.
fn vocabulary(n: usize) -> Vec<String> {
    let mut out = Vec::with_capacity(n);
    'outer: for a in SYL {
        for b in SYL {
            out.push(format!("{a}{b}"));
            if out.len() == n {
                break 'outer;
            }
            for c in SYL {
                out.push(format!("{a}{b}{c}"));
                if out.len() == n {
                    break 'outer;
                }
            }
        }
    }
    out
}

/// Zipf(s≈1.1) rank sampler over `n` ranks via an inverse-CDF table.
struct Zipf {
    cdf: Vec<f64>,
}
impl Zipf {
    fn new(n: usize) -> Self {
        let mut cdf = Vec::with_capacity(n);
        let mut acc = 0.0;
        for r in 0..n {
            acc += 1.0 / ((r + 1) as f64).powf(1.1);
            cdf.push(acc);
        }
        for c in &mut cdf {
            *c /= acc;
        }
        Self { cdf }
    }
    fn sample(&self, rng: &mut Rng) -> usize {
        let u = (rng.next() >> 11) as f64 / (1u64 << 53) as f64;
        self.cdf.partition_point(|&c| c < u).min(self.cdf.len() - 1)
    }
}

fn bulk(s: &str) -> Frame {
    Frame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
}

fn words(rng: &mut Rng, zipf: &Zipf, vocab: &[String], max: u64) -> String {
    let n = rng.below(max + 1);
    let mut s = String::new();
    for i in 0..n {
        if i > 0 {
            s.push(' ');
        }
        s.push_str(&vocab[zipf.sample(rng)]);
        // Stop words sprinkled in: must be dropped identically by both evaluators.
        if rng.below(9) == 0 {
            s.push_str(" the");
        }
    }
    s
}

struct Corpus {
    idx: TextIndex,
    vocab: Vec<String>,
    max_lsn: u64,
}

/// A 3-TEXT-field (title w=2.5, body w=1.0, notes NOINDEX) + TAG + NUMERIC index with inserts,
/// upserts (full and partial — re-adding a doc mid-posting), and deletions.
fn build_corpus(seed: u64, docs: usize, with_fst: bool) -> Corpus {
    let mut rng = Rng(seed);
    let vocab = vocabulary(300);
    let zipf = Zipf::new(vocab.len());
    let mut title = TextFieldDef::new(Bytes::from_static(b"title"));
    title.weight = 2.5;
    let body = TextFieldDef::new(Bytes::from_static(b"body"));
    let mut notes = TextFieldDef::new(Bytes::from_static(b"notes"));
    notes.noindex = true;
    let mut idx = TextIndex::new_with_schema(
        Bytes::from_static(b"idx"),
        vec![Bytes::from_static(b"doc:")],
        vec![title, body, notes],
        vec![TagFieldDef::new(Bytes::from_static(b"tag"))],
        vec![NumericFieldDef::new(Bytes::from_static(b"num"))],
        BM25Config::default(),
    );
    let tags = ["red", "Green", "blue", "red,blue", "green,RED"];
    let mut lsn = 0u64;
    let mut index_one = |idx: &mut TextIndex, rng: &mut Rng, d: usize, partial: bool| {
        let key = format!("doc:{d}");
        let key_hash = xxhash_rust::xxh64::xxh64(key.as_bytes(), 0);
        let mut args = Vec::new();
        if !partial || rng.below(2) == 0 {
            if rng.below(5) != 0 {
                args.push(bulk("title"));
                args.push(bulk(&words(rng, &zipf, &vocab, 6)));
            }
            args.push(bulk("body"));
            args.push(bulk(&words(rng, &zipf, &vocab, 40)));
        } else {
            args.push(bulk("title"));
            args.push(bulk(&words(rng, &zipf, &vocab, 3)));
        }
        if rng.below(4) != 0 {
            args.push(bulk("tag"));
            args.push(bulk(tags[rng.below(tags.len() as u64) as usize]));
        }
        if rng.below(3) != 0 {
            args.push(bulk("num"));
            args.push(bulk(&format!("{}", rng.below(100) as f64 / 4.0)));
        }
        lsn += 1 + rng.below(3);
        // A quarter of the docs are pre-MVCC (lsn 0 = always visible).
        let doc_lsn = if rng.below(4) == 0 { 0 } else { lsn };
        idx.index_document_with_lsn(key_hash, key.as_bytes(), &args, doc_lsn);
        idx.tag_index_document(key_hash, key.as_bytes(), &args);
        idx.numeric_index_document(key_hash, key.as_bytes(), &args);
    };
    for d in 0..docs {
        index_one(&mut idx, &mut rng, d, false);
    }
    if with_fst {
        idx.build_fst();
    }
    // Upserts of random (mostly old, low-id) docs — full and partial.
    for _ in 0..docs / 4 {
        let d = rng.below(docs as u64) as usize;
        let partial = rng.below(3) == 0;
        index_one(&mut idx, &mut rng, d, partial);
    }
    // Deletions.
    for _ in 0..docs / 20 {
        let d = rng.below(docs as u64) as u32;
        idx.remove_doc_by_doc_id(d);
    }
    // Some post-FST terms (dual-path expansion).
    for d in docs..docs + docs / 10 {
        index_one(&mut idx, &mut rng, d, false);
    }
    Corpus {
        idx,
        vocab,
        max_lsn: lsn,
    }
}

fn random_query(rng: &mut Rng, vocab: &[String]) -> String {
    let zipf = Zipf::new(vocab.len());
    let w = |rng: &mut Rng| vocab[zipf.sample(rng)].clone();
    match rng.below(18) {
        0 => w(rng),
        1 => format!("{} {}", w(rng), w(rng)),
        2 => format!("{} | {}", w(rng), w(rng)),
        3 => format!("({} | {}) {}", w(rng), w(rng), w(rng)),
        4 => format!("@title:{}", w(rng)),
        5 => format!("@body:({} | {})", w(rng), w(rng)),
        6 => format!("{}*", &w(rng)[..2]),
        7 => format!("{}*", &w(rng)[..4.min(vocab[0].len())]),
        8 => format!("%{}%", w(rng)),
        9 => format!("%%{}%%", w(rng)),
        10 => "@tag:{red|blue}".to_owned(),
        11 => format!("@tag:{{green}} {}", w(rng)),
        12 => format!(
            "@num:[{} {}] | {}",
            rng.below(10),
            10 + rng.below(15),
            w(rng)
        ),
        13 => "*".to_owned(),
        14 => format!("the {} a", w(rng)),
        15 => format!("{} zzzabsent", w(rng)),
        16 => format!("@title:{} @body:{}", w(rng), w(rng)),
        _ => format!("{} {} {}", w(rng), w(rng), w(rng)),
    }
}

fn assert_same(label: &str, got: &[TextSearchResult], want: &[TextSearchResult]) {
    assert_eq!(got.len(), want.len(), "{label}: result count");
    for (i, (g, w)) in got.iter().zip(want).enumerate() {
        assert_eq!(g.doc_id, w.doc_id, "{label}: doc at rank {i}");
        assert_eq!(g.key, w.key, "{label}: key at rank {i}");
        assert_eq!(
            g.score.to_bits(),
            w.score.to_bits(),
            "{label}: score bits at rank {i} ({} vs {})",
            g.score,
            w.score
        );
    }
}

const TOP_KS: [usize; 6] = [1, 2, 7, 10, 1000, usize::MAX / 2];

#[test]
fn differential_eval_query_matches_head_on_random_corpora() {
    let mut compared = 0usize;
    let mut nonempty = 0usize;
    for (seed, with_fst) in [(11u64, false), (12, true), (13, true)] {
        let corpus = build_corpus(seed, 500, with_fst);
        let idx = &corpus.idx;
        let schema = QuerySchema::from_index(idx);
        let mut rng = Rng(seed ^ 0xABCD);
        for _ in 0..160 {
            let q = random_query(&mut rng, &corpus.vocab);
            let Ok(node) = parse_query(q.as_bytes(), &schema) else {
                continue;
            };
            assert_eq!(
                idx.restrict_to_live(eval_set(&node, idx)),
                head_eval_set(&node, idx) & idx.live_docs(),
                "membership of {q:?}"
            );
            for &k in &TOP_KS {
                let (got, got_total) = eval_query_counted(idx, &node, None, None, k);
                let (want, want_total) = head_eval_query_counted(idx, &node, None, None, k);
                assert_eq!(got_total, want_total, "{q:?} k={k}: total");
                assert_same(&format!("{q:?} k={k}"), &got, &want);
                compared += 1;
                nonempty += usize::from(!want.is_empty());
            }
            // DFS global-IDF path: distinct, asymmetric df/N overrides.
            let mut gdf: HashMap<String, u32> = HashMap::new();
            for (_, terms) in collect_df_field_terms(&node, idx) {
                for t in terms {
                    let df = 1 + (t.len() as u32 * 7) % 23;
                    gdf.insert(t, df);
                }
            }
            let gn = Some(idx.num_docs() * 4 + 3);
            for &k in &[3usize, usize::MAX / 2] {
                let (got, got_total) = eval_query_counted(idx, &node, Some(&gdf), gn, k);
                let (want, want_total) = head_eval_query_counted(idx, &node, Some(&gdf), gn, k);
                assert_eq!(got_total, want_total, "DFS {q:?} k={k}: total");
                assert_same(&format!("DFS {q:?} k={k}"), &got, &want);
            }
        }
    }
    // A harness that compares nothing proves nothing (CONVENTIONS: assert a nonzero ran-count).
    assert!(compared > 2000, "compared only {compared} query/k pairs");
    assert!(nonempty > 1000, "only {nonempty} non-empty comparisons");
}

#[test]
fn differential_search_field_paths_match_head() {
    let mut nonempty = 0usize;
    for (seed, with_fst) in [(21u64, true), (22, false)] {
        let corpus = build_corpus(seed, 400, with_fst);
        let idx = &corpus.idx;
        let mut rng = Rng(seed ^ 0x5151);
        let zipf = Zipf::new(corpus.vocab.len());
        for _ in 0..200 {
            let field = rng.below(3) as usize; // includes the NOINDEX field
            let nterms = 1 + rng.below(3) as usize;
            let terms: Vec<String> = (0..nterms)
                .map(|_| corpus.vocab[zipf.sample(&mut rng)].clone())
                .collect();
            let k = TOP_KS[rng.below(TOP_KS.len() as u64) as usize];
            let gdf: HashMap<String, u32> = terms
                .iter()
                .map(|t| (t.clone(), 1 + rng.below(40) as u32))
                .collect();
            let (gdf, gn) = if rng.below(2) == 0 {
                (Some(&gdf), Some(1 + rng.below(3000) as u32))
            } else {
                (None, None)
            };
            let want = head_search_field(idx, field, &terms, gdf, gn, k);
            nonempty += usize::from(!want.is_empty());
            assert_same(
                &format!("search_field {field} {terms:?} k={k}"),
                &idx.search_field(field, &terms, gdf, gn, k),
                &want,
            );
            // AS_OF: HEAD oversampled to next_doc_id, post-filtered, truncated.
            let lsn = 1 + rng.below(corpus.max_lsn);
            let oversample = (idx.next_doc_id() as usize).max(k).max(16);
            let want = head_as_of(
                idx,
                head_search_field(idx, field, &terms, gdf, gn, oversample),
                k,
                lsn,
            );
            assert_same(
                &format!("search_field_as_of {terms:?} lsn={lsn} k={k}"),
                &idx.search_field_as_of(field, &terms, gdf, gn, k, lsn),
                &want,
            );

            // OR path over expanded ids (prefix / fuzzy).
            let stem = &terms[0][..2];
            let modifier = if rng.below(2) == 0 {
                TermModifier::Prefix
            } else {
                TermModifier::Fuzzy(1 + rng.below(2) as u8)
            };
            let probe = if matches!(modifier, TermModifier::Prefix) {
                stem.to_owned()
            } else {
                terms[0].clone()
            };
            let ids = if field < 3 {
                idx.expand_terms(field, &probe, &modifier)
            } else {
                Vec::new()
            };
            let want = head_search_field_or(idx, field, &ids, gn, k);
            nonempty += usize::from(!want.is_empty());
            assert_same(
                &format!("search_field_or {probe:?} k={k}"),
                &idx.search_field_or(field, &ids, gdf, gn, k),
                &want,
            );
            let want = head_as_of(
                idx,
                head_search_field_or(idx, field, &ids, gn, oversample),
                k,
                lsn,
            );
            assert_same(
                &format!("search_field_or_as_of {probe:?} lsn={lsn} k={k}"),
                &idx.search_field_or_as_of(field, &ids, gdf, gn, k, lsn),
                &want,
            );
        }
    }
    assert!(nonempty > 300, "only {nonempty} non-empty comparisons");
}

/// The two-pass HEAD evaluator re-ran BM25 for every leaf; the live one must score each matched
/// doc once. Pinned behaviourally: on a broad single-term query the single pass returns the same
/// page as HEAD while doing strictly less work — measured as a wall-time ratio on the same corpus
/// (best-of-5 each; a loose 1.3x bound so shared-CPU noise cannot flake it).
#[test]
fn broad_term_single_pass_is_cheaper_than_head() {
    let corpus = build_corpus(31, 3000, false);
    let idx = &corpus.idx;
    let schema = QuerySchema::from_index(idx);
    // The most frequent vocabulary word is the broadest query.
    let node = parse_query(corpus.vocab[0].as_bytes(), &schema).expect("parse");
    let best = |f: &dyn Fn() -> usize| {
        (0..5)
            .map(|_| {
                let t = std::time::Instant::now();
                std::hint::black_box(f());
                t.elapsed()
            })
            .min()
            .unwrap_or_default()
    };
    let (_, total) = eval_query_counted(idx, &node, None, None, 10);
    assert!(total > 500, "broad term must match many docs, got {total}");
    let live = best(&|| eval_query_counted(idx, &node, None, None, 10).0.len());
    let head = best(&|| head_eval_query_counted(idx, &node, None, None, 10).0.len());
    assert!(
        live.as_secs_f64() * 1.3 < head.as_secs_f64(),
        "single pass {live:?} not clearly cheaper than HEAD two-pass {head:?}"
    );
}
