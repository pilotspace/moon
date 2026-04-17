//! Hybrid FT.SEARCH (BM25 + dense + sparse, three-way RRF) — Phase 152.
//!
//! # Syntax (per CONTEXT.md D-09)
//!
//! ```text
//! FT.SEARCH idx <text_query>
//!     HYBRID VECTOR @<dense_field> $<param>
//!     [SPARSE @<sparse_field> $<param>]
//!     FUSION RRF
//!     [WEIGHTS <w_bm25> <w_dense> <w_sparse>]
//!     [K_PER_STREAM N]
//!     [LIMIT offset count]
//!     PARAMS <count> <name1> <blob1> ...
//! ```
//!
//! # Invariants
//! - If `HYBRID` keyword is absent, `parse_hybrid_modifier` returns `Ok(None)` and
//!   caller falls through to the existing FT.SEARCH path (CONTEXT D-18 — zero impact
//!   on Phase 149–151 code paths).
//! - `FUSION RRF` is mandatory in v1. Other fusion modes return `Frame::Error`.
//! - `WEIGHTS` requires exactly 3 finite non-negative floats (CONTEXT D-17);
//!   `0.0` disables a stream silently; negative / NaN → `Frame::Error`.
//! - `K_PER_STREAM` default = `max(60, 3 * top_k)` (CONTEXT D-15), computed by
//!   [`HybridQuery::effective_k_per_stream`].
//! - A `SPARSE` clause on an index lacking the named sparse field returns
//!   `Frame::Error("ERR sparse field not defined in index")` (CONTEXT D-16) — this
//!   is enforced in [`execute_hybrid_search_local`], not the parser.
//!
//! This file owns the local (single-shard) path. Multi-shard coordination with
//! DFS pre-pass for global BM25 IDF lives in Plan 05.

use bytes::Bytes;

use crate::protocol::{Frame, FrameVec};
use crate::text::store::{TextIndex, TextSearchResult, TextStore};
use crate::vector::store::VectorStore;
use crate::vector::types::{SearchResult, VectorId};

use super::ft_search::execute::parse_sparse_query_blob;
use super::ft_search::parse::{extract_param_blob, parse_usize};
use super::{extract_bulk, matches_keyword};

// ─── Types ────────────────────────────────────────────────────────────────────

/// Fully-assembled hybrid query ready for local execution.
///
/// Produced by the caller (FT.SEARCH dispatcher) by combining:
/// - [`HybridQueryPartial`] from [`parse_hybrid_modifier`] (HYBRID clause fields)
/// - the FT.SEARCH index name and text query (already parsed from args[0..2])
/// - top_k / offset / count resolved from the LIMIT clause (if present)
#[derive(Debug, Clone)]
pub struct HybridQuery {
    pub index_name: Bytes,
    pub text_query: Bytes,
    pub dense_field: Bytes,
    pub dense_blob: Bytes,
    /// `(field_name, blob)` — absent ⇒ two-way fusion (BM25 + dense only) per D-16.
    pub sparse: Option<(Bytes, Bytes)>,
    /// `[w_bm25, w_dense, w_sparse]` — caller-validated finite non-negative per D-17.
    pub weights: [f32; 3],
    /// Per-stream candidate cap. `None` ⇒ use default `max(60, 3 * top_k)` (D-15).
    pub k_per_stream: Option<usize>,
    /// Final top-K after fusion.
    pub top_k: usize,
    /// LIMIT offset (default 0).
    pub offset: usize,
    /// LIMIT count (default top_k).
    pub count: usize,
}

impl HybridQuery {
    /// Resolve the effective per-stream candidate cap per CONTEXT D-15.
    ///
    /// Default: `max(60, 3 * top_k)`. This gives each stream at least 60
    /// candidates (matching RRF's k-constant headroom) and scales with top_k
    /// so fusion has enough rescue headroom for docs appearing in multiple streams.
    #[inline]
    pub fn effective_k_per_stream(&self) -> usize {
        self.k_per_stream
            .unwrap_or_else(|| 60usize.max(3usize.saturating_mul(self.top_k.max(1))))
    }
}

/// Output of [`parse_hybrid_modifier`] — the pure HYBRID-clause fields plus the
/// index into `args` one past the last HYBRID token consumed, so downstream
/// parsers (LIMIT, SESSION) can continue scanning without re-reading the HYBRID
/// block.
#[derive(Debug, Clone)]
pub struct HybridQueryPartial {
    pub dense_field: Bytes,
    pub dense_blob: Bytes,
    pub sparse: Option<(Bytes, Bytes)>,
    pub weights: [f32; 3],
    pub k_per_stream: Option<usize>,
    /// Index into `args` one past the last HYBRID-modifier token consumed.
    /// LIMIT / SESSION / PARAMS parsers may start their scan from here.
    pub end_index: usize,
}

// ─── Parser ───────────────────────────────────────────────────────────────────

/// Parse the HYBRID modifier from FT.SEARCH args.
///
/// Returns:
/// - `Ok(None)` — HYBRID keyword absent; caller falls through to existing path (D-18)
/// - `Ok(Some(..))` — valid HYBRID clause
/// - `Err(Frame::Error)` — HYBRID present but malformed; caller returns the frame
///
/// Resolution of `$<param>` references happens inline via the existing
/// `extract_param_blob` helper (same shape as KNN's `$blob` resolution).
pub fn parse_hybrid_modifier(args: &[Frame]) -> Result<Option<HybridQueryPartial>, Frame> {
    // Scan for HYBRID keyword (may appear after text_query, after LIMIT — arbitrary order).
    let mut i = 0;
    while i < args.len() {
        if matches_keyword(&args[i], b"HYBRID") {
            break;
        }
        i += 1;
    }
    if i >= args.len() {
        return Ok(None);
    }

    // ── VECTOR @<field> $<param> (mandatory) ──────────────────────────────────
    i += 1;
    if i + 2 >= args.len() || !matches_keyword(&args[i], b"VECTOR") {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR HYBRID requires VECTOR @field $blob",
        )));
    }
    let dense_field = extract_field_token(&args[i + 1])
        .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid VECTOR field")))?;
    let dense_param = extract_param_token(&args[i + 2])
        .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid VECTOR blob reference")))?;
    let dense_blob = resolve_param_blob(args, &dense_param)?;
    i += 3;

    // ── Optional SPARSE @<field> $<param> ─────────────────────────────────────
    let sparse = if i < args.len() && matches_keyword(&args[i], b"SPARSE") {
        if i + 2 >= args.len() {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR SPARSE requires @field $blob",
            )));
        }
        let sf = extract_field_token(&args[i + 1])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid SPARSE field")))?;
        let sp = extract_param_token(&args[i + 2]).ok_or_else(|| {
            Frame::Error(Bytes::from_static(b"ERR invalid SPARSE blob reference"))
        })?;
        let sb = resolve_param_blob(args, &sp)?;
        i += 3;
        Some((sf, sb))
    } else {
        None
    };

    // ── FUSION RRF (mandatory in v1) ──────────────────────────────────────────
    if i + 1 >= args.len() || !matches_keyword(&args[i], b"FUSION") {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR HYBRID requires FUSION RRF",
        )));
    }
    let fusion_name = extract_bulk(&args[i + 1])
        .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid FUSION mode")))?;
    if !fusion_name.eq_ignore_ascii_case(b"RRF") {
        let mut msg = b"ERR unknown FUSION mode: ".to_vec();
        msg.extend_from_slice(&fusion_name);
        return Err(Frame::Error(Bytes::from(msg)));
    }
    i += 2;

    // ── Optional WEIGHTS w_bm25 w_dense w_sparse (D-17) ───────────────────────
    let mut weights = [1.0f32, 1.0, 1.0];
    if i < args.len() && matches_keyword(&args[i], b"WEIGHTS") {
        // Need 3 more args after the WEIGHTS keyword.
        if i + 3 >= args.len() {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR WEIGHTS requires exactly 3 floats",
            )));
        }
        for j in 0..3 {
            let w = parse_f32(&args[i + 1 + j])
                .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid weight value")))?;
            if !w.is_finite() || w < 0.0 {
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR hybrid weights must be finite and non-negative",
                )));
            }
            weights[j] = w;
        }
        i += 4;
    }

    // ── Optional K_PER_STREAM N (D-15) ────────────────────────────────────────
    let k_per_stream = if i < args.len() && matches_keyword(&args[i], b"K_PER_STREAM") {
        if i + 1 >= args.len() {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR K_PER_STREAM requires a count",
            )));
        }
        let k = parse_usize(&args[i + 1])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid K_PER_STREAM")))?;
        i += 2;
        Some(k)
    } else {
        None
    };

    Ok(Some(HybridQueryPartial {
        dense_field,
        dense_blob,
        sparse,
        weights,
        k_per_stream,
        end_index: i,
    }))
}

// ─── Parser helpers (private) ─────────────────────────────────────────────────

/// Extract an `@field` or bare field name as Bytes. Strips leading `@` if present.
fn extract_field_token(frame: &Frame) -> Option<Bytes> {
    let raw = extract_bulk(frame)?;
    if let Some(stripped) = raw.strip_prefix(b"@") {
        Some(Bytes::copy_from_slice(stripped))
    } else {
        Some(raw)
    }
}

/// Extract a `$param` reference. Keeps the leading `$` for `resolve_param_blob`
/// to distinguish PARAMS-references from literal blobs.
fn extract_param_token(frame: &Frame) -> Option<Bytes> {
    extract_bulk(frame)
}

/// Resolve `$<name>` to the bytes of the named PARAMS entry. Literal bytes
/// (without `$` prefix) pass through unchanged. Unknown parameter names error.
fn resolve_param_blob(args: &[Frame], param_ref: &Bytes) -> Result<Bytes, Frame> {
    if let Some(name) = param_ref.strip_prefix(b"$") {
        extract_param_blob(args, name).ok_or_else(|| {
            let mut msg = b"ERR unknown parameter: ".to_vec();
            msg.extend_from_slice(name);
            Frame::Error(Bytes::from(msg))
        })
    } else {
        Ok(param_ref.clone())
    }
}

/// Parse a Frame as `f32`. Accepts bulk-string UTF-8 ("1.5", "NaN") and integer frames.
fn parse_f32(frame: &Frame) -> Option<f32> {
    match frame {
        Frame::BulkString(b) => std::str::from_utf8(b).ok()?.parse::<f32>().ok(),
        Frame::Integer(n) => Some(*n as f32),
        _ => None,
    }
}

// ─── BM25 → SearchResult bridge ───────────────────────────────────────────────

/// Convert BM25 text-search results to the unified `SearchResult` shape for RRF.
///
/// Two invariants this function enforces (RESEARCH Pitfalls 6, 8):
/// 1. **`distance = -score`** — BM25 is higher-is-better; RRF's `SearchResult::Ord`
///    is lower-is-better. Negating once at the boundary lets rank-based fusion
///    treat all three streams uniformly. (Pitfall 8.)
/// 2. **`key_hash = xxh64(key, 0)`** — Moon uses `xxh64` seed 0 everywhere
///    (see `src/shard/dispatch.rs` HASH_SEED). The dense and sparse streams'
///    `key_hash` fields are populated via the same hash at `auto_index_hset` /
///    `SparseStore::insert` time, so using the same seed here lets RRF
///    deduplicate across all three streams by key_hash. (Pitfall 6.)
///
/// `id = VectorId(doc_id)` — the BM25 doc_id is unrelated to the HNSW
/// vector id, but stored here so the response builder can produce a
/// `vec:<id>` fallback when the key lookup misses.
pub fn bm25_to_search_results(text_results: &[TextSearchResult]) -> Vec<SearchResult> {
    text_results
        .iter()
        .map(|r| SearchResult {
            distance: -r.score,
            id: VectorId(r.doc_id),
            key_hash: xxhash_rust::xxh64::xxh64(&r.key, 0),
        })
        .collect()
}

// ─── Local hybrid executor ────────────────────────────────────────────────────

/// Execute a single-shard hybrid FT.SEARCH: BM25 + dense + (optional sparse) → local RRF.
///
/// Per-shard order (D-13): BM25, then dense KNN, then optional sparse, then
/// weighted three-way RRF → top-K. The multi-shard coordinator variant (with
/// DFS pre-pass for global BM25 IDF per Pitfall 5) ships in Plan 05.
///
/// Errors:
/// - Unknown index → `Frame::Error("ERR unknown index")`
/// - SPARSE on an index lacking that sparse field → `Frame::Error("ERR sparse field not defined in index")` (D-16)
/// - Malformed text/dense/sparse input → propagated as `Frame::Error`
pub fn execute_hybrid_search_local(
    vector_store: &mut VectorStore,
    text_store: &TextStore,
    query: &HybridQuery,
) -> Frame {
    let k_per_stream = query.effective_k_per_stream();

    // ── Stream 1: BM25 (per D-10 — direct use of existing BM25 ranker) ────────
    let text_index = match text_store.get_index(query.index_name.as_ref()) {
        Some(ix) => ix,
        None => return Frame::Error(Bytes::from_static(b"ERR unknown index")),
    };
    let analyzer = match text_index.field_analyzers.first() {
        Some(a) => a,
        None => {
            return Frame::Error(Bytes::from_static(b"ERR index has no TEXT fields"));
        }
    };
    let text_clause = match crate::command::vector_search::parse_text_query(
        query.text_query.as_ref(),
        analyzer,
    ) {
        Ok(c) => c,
        Err(e) => {
            let mut msg = b"ERR ".to_vec();
            msg.extend_from_slice(e.as_bytes());
            return Frame::Error(Bytes::from(msg));
        }
    };
    let text_results: Vec<TextSearchResult> =
        crate::command::vector_search::ft_text_search::execute_query_on_index(
            text_index,
            &text_clause,
            None,
            None,
            k_per_stream,
        );
    let bm25_results = bm25_to_search_results(&text_results);

    // ── Stream 2: Dense KNN (per D-11 — existing KNN against the vector index) ─
    let idx = match vector_store.get_index_mut(query.index_name.as_ref()) {
        Some(ix) => ix,
        None => return Frame::Error(Bytes::from_static(b"ERR unknown index")),
    };
    let (dense_results, key_hash_to_key) =
        match run_dense_knn(idx, &query.dense_field, &query.dense_blob, k_per_stream) {
            Ok(v) => v,
            Err(frame) => return frame,
        };

    // ── Stream 3: Sparse (optional, per D-12 + D-16) ──────────────────────────
    let sparse_results: Vec<SearchResult> = if let Some((sf, sblob)) = &query.sparse {
        match idx.sparse_stores.get(sf.as_ref()) {
            Some(store) => {
                let pairs = parse_sparse_query_blob(sblob);
                if pairs.is_empty() {
                    return Frame::Error(Bytes::from_static(
                        b"ERR invalid sparse query blob (must be pairs of u32+f32 LE)",
                    ));
                }
                store.search(&pairs, k_per_stream)
            }
            None => {
                // D-16: user asked for SPARSE but index lacks that sparse field.
                return Frame::Error(Bytes::from_static(b"ERR sparse field not defined in index"));
            }
        }
    } else {
        Vec::new()
    };

    // ── Local RRF (per D-13) ──────────────────────────────────────────────────
    let (fused, bm25_count, dense_count, sparse_count) = crate::vector::fusion::rrf_fuse_three(
        &bm25_results,
        &dense_results,
        &sparse_results,
        query.weights,
        query.top_k,
    );

    build_hybrid_local_response(
        &fused,
        &key_hash_to_key,
        text_index,
        bm25_count,
        dense_count,
        sparse_count,
        query.offset,
        query.count,
    )
}

/// Run a dense KNN pass against `idx` using the existing FT.SEARCH decoding
/// rules (LE f32 blob OR comma-separated f32 text fallback). Returns the raw
/// results plus the key_hash→key map captured at search time, so the response
/// builder can resolve `doc:N` without re-cloning the full map.
pub(super) fn run_dense_knn(
    idx: &mut crate::vector::store::VectorIndex,
    field_name: &Bytes,
    blob: &Bytes,
    k: usize,
) -> Result<(Vec<SearchResult>, std::collections::HashMap<u64, Bytes>), Frame> {
    let field_opt = if field_name.is_empty() {
        None
    } else {
        Some(field_name)
    };
    let (dim, use_default_field) = if let Some(fname) = field_opt {
        if let Some(field_meta) = idx.meta.find_field(fname) {
            let is_default = fname.eq_ignore_ascii_case(&idx.meta.default_field().field_name);
            (field_meta.dimension as usize, is_default)
        } else {
            return Err(Frame::Error(Bytes::from(
                format!(
                    "ERR unknown vector field '@{}'",
                    String::from_utf8_lossy(fname)
                )
                .into_bytes(),
            )));
        }
    } else {
        (idx.meta.dimension as usize, true)
    };

    // Decode blob — LE f32 primary, comma-separated UTF-8 fallback (matches existing path).
    let query_f32: Vec<f32> = if blob.len() == dim * 4 {
        let mut v = Vec::with_capacity(dim);
        for chunk in blob.chunks_exact(4) {
            v.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
        }
        v
    } else if let Ok(text) = std::str::from_utf8(blob) {
        let parsed: Vec<f32> = text
            .split(',')
            .filter(|s| !s.is_empty())
            .filter_map(|s| s.trim().parse::<f32>().ok())
            .collect();
        if parsed.len() != dim {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR query vector dimension mismatch",
            )));
        }
        parsed
    } else {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR query vector dimension mismatch",
        )));
    };

    idx.try_compact();

    let ef_search = if idx.meta.hnsw_ef_runtime > 0 {
        idx.meta.hnsw_ef_runtime as usize
    } else {
        let base = (k * 20).max(200);
        let dim_factor = if dim >= 768 {
            2
        } else if dim >= 384 {
            3
        } else {
            2
        };
        (base * dim_factor / 2).clamp(200, 1000)
    };

    let empty_committed = roaring::RoaringTreemap::new();

    let results = if use_default_field {
        let mvcc_ctx = crate::vector::segment::holder::MvccContext {
            snapshot_lsn: 0,
            my_txn_id: 0,
            committed: &empty_committed,
            dirty_set: &[],
            dimension: dim as u32,
        };
        idx.segments
            .search_mvcc(&query_f32, k, ef_search, &mut idx.scratch, None, &mvcc_ctx)
    } else {
        #[allow(clippy::unwrap_used)] // guarded: use_default_field is false ⇒ field_opt is Some
        let fname = field_opt.unwrap();
        let Some(fs) = idx.field_segments.get_mut(fname.as_ref()) else {
            return Err(Frame::Error(Bytes::from(
                format!(
                    "ERR unknown vector field '@{}'",
                    String::from_utf8_lossy(fname)
                )
                .into_bytes(),
            )));
        };
        let mvcc_ctx = crate::vector::segment::holder::MvccContext {
            snapshot_lsn: 0,
            my_txn_id: 0,
            committed: &empty_committed,
            dirty_set: &[],
            dimension: dim as u32,
        };
        fs.segments
            .search_mvcc(&query_f32, k, ef_search, &mut fs.scratch, None, &mvcc_ctx)
    };

    let key_hash_to_key = idx.key_hash_to_key.clone();
    Ok((results.into_vec(), key_hash_to_key))
}

// ─── Response builder ─────────────────────────────────────────────────────────

/// Build the FT.SEARCH response for a hybrid query.
///
/// Shape (same convention as two-way hybrid build_hybrid_response):
/// ```text
/// [ total, key1, [__rrf_score, "0.033"], key2, [...], ..., bm25_hits, N1, dense_hits, N2, sparse_hits, N3 ]
/// ```
///
/// Key resolution priority:
/// 1. `vector_key_hash_to_key` (dense / sparse populated via auto_index_hset)
/// 2. `text_index.doc_id_to_key` (BM25-only docs that never got HSET'd for vectors
///    — rare but possible if user indexed text separately from vectors)
/// 3. `vec:<internal_id>` fallback
#[allow(clippy::too_many_arguments)]
pub fn build_hybrid_local_response(
    fused: &[SearchResult],
    vector_key_hash_to_key: &std::collections::HashMap<u64, Bytes>,
    text_index: &TextIndex,
    bm25_count: usize,
    dense_count: usize,
    sparse_count: usize,
    offset: usize,
    count: usize,
) -> Frame {
    let total = fused.len() as i64;
    let page: Vec<&SearchResult> = fused.iter().skip(offset).take(count).collect();
    // 1 (total) + page*2 (doc+fields) + 6 (metadata: 3 × (key, value))
    let mut items: Vec<Frame> = Vec::with_capacity(1 + page.len() * 2 + 6);
    items.push(Frame::Integer(total));

    for r in &page {
        let key = resolve_hybrid_doc_key(r, vector_key_hash_to_key, text_index);
        items.push(Frame::BulkString(key));

        // RRF score = -distance (distance is the sign-flipped score).
        let rrf_score = -r.distance;
        let mut score_buf = String::with_capacity(16);
        use std::fmt::Write;
        let _ = write!(score_buf, "{}", rrf_score);
        let fields: FrameVec = vec![
            Frame::BulkString(Bytes::from_static(b"__rrf_score")),
            Frame::BulkString(Bytes::from(score_buf)),
        ]
        .into();
        items.push(Frame::Array(fields));
    }

    // Per-stream hit counts (echoes the raw stream sizes, not the post-weight-filter counts).
    items.push(Frame::BulkString(Bytes::from_static(b"bm25_hits")));
    items.push(Frame::Integer(bm25_count as i64));
    items.push(Frame::BulkString(Bytes::from_static(b"dense_hits")));
    items.push(Frame::Integer(dense_count as i64));
    items.push(Frame::BulkString(Bytes::from_static(b"sparse_hits")));
    items.push(Frame::Integer(sparse_count as i64));

    Frame::Array(items.into())
}

pub(super) fn resolve_hybrid_doc_key(
    r: &SearchResult,
    vector_map: &std::collections::HashMap<u64, Bytes>,
    text_index: &TextIndex,
) -> Bytes {
    if r.key_hash != 0 {
        if let Some(k) = vector_map.get(&r.key_hash) {
            return k.clone();
        }
    }
    // Fall back to text index's doc_id → key map (BM25-only docs).
    if let Some(k) = text_index.doc_id_to_key.get(&r.id.0) {
        return k.clone();
    }
    // Final fallback: vec:<id>.
    let mut buf = itoa::Buffer::new();
    let id_str = buf.format(r.id.0);
    let mut v = Vec::with_capacity(4 + id_str.len());
    v.extend_from_slice(b"vec:");
    v.extend_from_slice(id_str.as_bytes());
    Bytes::from(v)
}

// ─── Multi-shard raw-streams path (Plan 05 D-13) ──────────────────────────────
//
// The per-shard raw-streams executor, `KeyedResult`/`ShardHybridReply`, and
// Frame codec live in the sibling `hybrid_multi.rs` module to keep this file
// under the 1500-line cap. Public items are re-exported at the module root
// via `src/command/vector_search/mod.rs`.

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    /// Build an args vector like real FT.SEARCH:
    /// `[index_name, query, ...hybrid_tokens..., PARAMS n name1 blob1 ...]`
    fn args_with_params(hybrid_tokens: Vec<Frame>, params: Vec<(&[u8], &[u8])>) -> Vec<Frame> {
        let mut args = vec![bs(b"myidx"), bs(b"machine learning")];
        args.extend(hybrid_tokens);
        if !params.is_empty() {
            args.push(bs(b"PARAMS"));
            args.push(bs(format!("{}", params.len() * 2).as_bytes()));
            for (n, v) in params {
                args.push(bs(n));
                args.push(bs(v));
            }
        }
        args
    }

    #[test]
    fn test_parse_hybrid_missing_keyword_returns_none() {
        // Non-HYBRID args should pass through untouched.
        let args = vec![
            bs(b"myidx"),
            bs(b"machine learning"),
            bs(b"LIMIT"),
            bs(b"0"),
            bs(b"10"),
        ];
        let got = parse_hybrid_modifier(&args).expect("no error");
        assert!(got.is_none(), "HYBRID absent → Ok(None)");
    }

    #[test]
    fn test_parse_hybrid_minimal() {
        // HYBRID VECTOR @vec $qv FUSION RRF   PARAMS 2 qv <blob>
        let args = args_with_params(
            vec![
                bs(b"HYBRID"),
                bs(b"VECTOR"),
                bs(b"@vec"),
                bs(b"$qv"),
                bs(b"FUSION"),
                bs(b"RRF"),
            ],
            vec![(b"qv", b"DENSE_BLOB")],
        );
        let got = parse_hybrid_modifier(&args)
            .expect("no error")
            .expect("some");
        assert_eq!(got.dense_field, Bytes::from_static(b"vec"));
        assert_eq!(got.dense_blob, Bytes::from_static(b"DENSE_BLOB"));
        assert!(got.sparse.is_none());
        assert_eq!(got.weights, [1.0, 1.0, 1.0]);
        assert!(got.k_per_stream.is_none());
    }

    #[test]
    fn test_parse_hybrid_with_sparse() {
        let args = args_with_params(
            vec![
                bs(b"HYBRID"),
                bs(b"VECTOR"),
                bs(b"@vec"),
                bs(b"$qv"),
                bs(b"SPARSE"),
                bs(b"@sp"),
                bs(b"$qs"),
                bs(b"FUSION"),
                bs(b"RRF"),
            ],
            vec![(b"qv", b"DBLOB"), (b"qs", b"SBLOB")],
        );
        let got = parse_hybrid_modifier(&args)
            .expect("no error")
            .expect("some");
        assert_eq!(got.dense_field, Bytes::from_static(b"vec"));
        let (sf, sb) = got.sparse.expect("sparse");
        assert_eq!(sf, Bytes::from_static(b"sp"));
        assert_eq!(sb, Bytes::from_static(b"SBLOB"));
    }

    #[test]
    fn test_parse_hybrid_weights() {
        let args = args_with_params(
            vec![
                bs(b"HYBRID"),
                bs(b"VECTOR"),
                bs(b"@vec"),
                bs(b"$qv"),
                bs(b"FUSION"),
                bs(b"RRF"),
                bs(b"WEIGHTS"),
                bs(b"1.0"),
                bs(b"1.5"),
                bs(b"0.5"),
            ],
            vec![(b"qv", b"DBLOB")],
        );
        let got = parse_hybrid_modifier(&args)
            .expect("no error")
            .expect("some");
        assert_eq!(got.weights, [1.0, 1.5, 0.5]);
    }

    #[test]
    fn test_parse_hybrid_k_per_stream() {
        let args = args_with_params(
            vec![
                bs(b"HYBRID"),
                bs(b"VECTOR"),
                bs(b"@vec"),
                bs(b"$qv"),
                bs(b"FUSION"),
                bs(b"RRF"),
                bs(b"K_PER_STREAM"),
                bs(b"100"),
            ],
            vec![(b"qv", b"DBLOB")],
        );
        let got = parse_hybrid_modifier(&args)
            .expect("no error")
            .expect("some");
        assert_eq!(got.k_per_stream, Some(100));
    }

    #[test]
    fn test_parse_hybrid_rejects_non_rrf_fusion() {
        let args = args_with_params(
            vec![
                bs(b"HYBRID"),
                bs(b"VECTOR"),
                bs(b"@vec"),
                bs(b"$qv"),
                bs(b"FUSION"),
                bs(b"FOO"),
            ],
            vec![(b"qv", b"DBLOB")],
        );
        let err = parse_hybrid_modifier(&args).expect_err("must error");
        match err {
            Frame::Error(msg) => {
                let s = std::str::from_utf8(&msg).unwrap();
                assert!(s.contains("unknown FUSION mode"), "got: {s}");
                assert!(s.contains("FOO"), "must echo offending mode: {s}");
            }
            _ => panic!("expected Frame::Error"),
        }
    }

    #[test]
    fn test_parse_hybrid_rejects_negative_weight() {
        let args = args_with_params(
            vec![
                bs(b"HYBRID"),
                bs(b"VECTOR"),
                bs(b"@vec"),
                bs(b"$qv"),
                bs(b"FUSION"),
                bs(b"RRF"),
                bs(b"WEIGHTS"),
                bs(b"1.0"),
                bs(b"-0.5"),
                bs(b"1.0"),
            ],
            vec![(b"qv", b"DBLOB")],
        );
        let err = parse_hybrid_modifier(&args).expect_err("must error");
        match err {
            Frame::Error(msg) => assert_eq!(
                &msg[..],
                b"ERR hybrid weights must be finite and non-negative"
            ),
            _ => panic!("expected Frame::Error"),
        }
    }

    #[test]
    fn test_parse_hybrid_rejects_nan_weight() {
        let args = args_with_params(
            vec![
                bs(b"HYBRID"),
                bs(b"VECTOR"),
                bs(b"@vec"),
                bs(b"$qv"),
                bs(b"FUSION"),
                bs(b"RRF"),
                bs(b"WEIGHTS"),
                bs(b"1.0"),
                bs(b"NaN"),
                bs(b"1.0"),
            ],
            vec![(b"qv", b"DBLOB")],
        );
        let err = parse_hybrid_modifier(&args).expect_err("must error");
        if let Frame::Error(msg) = err {
            assert_eq!(
                &msg[..],
                b"ERR hybrid weights must be finite and non-negative"
            );
        } else {
            panic!("expected Frame::Error");
        }
    }

    #[test]
    fn test_parse_hybrid_rejects_wrong_weight_count() {
        // WEIGHTS with only 2 floats followed by PARAMS — the PARAMS keyword
        // should trip parse_f32 (which cannot parse "PARAMS") OR the arg-count
        // guard if PARAMS clause is absent. We test the arg-count guard directly:
        let args = vec![
            bs(b"myidx"),
            bs(b"q"),
            bs(b"HYBRID"),
            bs(b"VECTOR"),
            bs(b"@vec"),
            bs(b"$qv"),
            bs(b"FUSION"),
            bs(b"RRF"),
            bs(b"WEIGHTS"),
            bs(b"1.0"),
            bs(b"1.0"),
            // Intentionally only 2 floats, no trailing tokens.
            bs(b"PARAMS"),
            bs(b"2"),
            bs(b"qv"),
            bs(b"DBLOB"),
        ];
        // With PARAMS following WEIGHTS, the 3rd "weight" slot parses as
        // "PARAMS" → parse_f32 returns None → ERR invalid weight value.
        let err = parse_hybrid_modifier(&args).expect_err("must error");
        if let Frame::Error(msg) = err {
            let s = std::str::from_utf8(&msg).unwrap();
            assert!(
                s.contains("WEIGHTS requires exactly 3 floats")
                    || s.contains("invalid weight value"),
                "got: {s}"
            );
        } else {
            panic!("expected Frame::Error");
        }
    }

    #[test]
    fn test_parse_hybrid_rejects_wrong_weight_count_exact() {
        // Arg-count guard path: WEIGHTS with only 2 trailing weight-slots (not 3)
        // AND no trailing non-weight tokens. PARAMS placed BEFORE HYBRID so the
        // $qv reference still resolves (PARAMS scan is global, not position-bound).
        // We test the arg-count guard specifically: 3 * w_i slots would walk off
        // the end of args, so the guard `i + 3 >= args.len()` trips before parse_f32.
        let args = vec![
            bs(b"myidx"),
            bs(b"q"),
            bs(b"PARAMS"),
            bs(b"2"),
            bs(b"qv"),
            bs(b"DBLOB"),
            bs(b"HYBRID"),
            bs(b"VECTOR"),
            bs(b"@vec"),
            bs(b"$qv"),
            bs(b"FUSION"),
            bs(b"RRF"),
            bs(b"WEIGHTS"),
            bs(b"1.0"),
            bs(b"1.0"),
            // Only 2 weight-slots available before end-of-args → guard trips.
        ];
        let err = parse_hybrid_modifier(&args).expect_err("must error");
        if let Frame::Error(msg) = err {
            assert_eq!(&msg[..], b"ERR WEIGHTS requires exactly 3 floats");
        } else {
            panic!("expected Frame::Error");
        }
    }

    #[test]
    fn test_parse_hybrid_accepts_zero_weight() {
        // Zero weights are silently allowed per D-17 (disables stream).
        let args = args_with_params(
            vec![
                bs(b"HYBRID"),
                bs(b"VECTOR"),
                bs(b"@vec"),
                bs(b"$qv"),
                bs(b"FUSION"),
                bs(b"RRF"),
                bs(b"WEIGHTS"),
                bs(b"0.0"),
                bs(b"1.0"),
                bs(b"1.0"),
            ],
            vec![(b"qv", b"DBLOB")],
        );
        let got = parse_hybrid_modifier(&args)
            .expect("no error")
            .expect("some");
        assert_eq!(got.weights, [0.0, 1.0, 1.0]);
    }

    #[test]
    fn test_parse_hybrid_extracts_dollar_blob() {
        // $mydense resolves via PARAMS.
        let args = args_with_params(
            vec![
                bs(b"HYBRID"),
                bs(b"VECTOR"),
                bs(b"@vec"),
                bs(b"$mydense"),
                bs(b"FUSION"),
                bs(b"RRF"),
            ],
            vec![(b"mydense", b"HELLO_WORLD_BLOB")],
        );
        let got = parse_hybrid_modifier(&args)
            .expect("no error")
            .expect("some");
        assert_eq!(got.dense_blob, Bytes::from_static(b"HELLO_WORLD_BLOB"));
    }

    #[test]
    fn test_parse_hybrid_unknown_param_errors() {
        let args = args_with_params(
            vec![
                bs(b"HYBRID"),
                bs(b"VECTOR"),
                bs(b"@vec"),
                bs(b"$missing"),
                bs(b"FUSION"),
                bs(b"RRF"),
            ],
            vec![(b"other", b"SOMETHING")],
        );
        let err = parse_hybrid_modifier(&args).expect_err("must error");
        if let Frame::Error(msg) = err {
            let s = std::str::from_utf8(&msg).unwrap();
            assert!(s.contains("unknown parameter"), "got: {s}");
            assert!(s.contains("missing"), "must echo name: {s}");
        } else {
            panic!("expected Frame::Error");
        }
    }

    #[test]
    fn test_effective_k_per_stream_default() {
        let hq = HybridQuery {
            index_name: Bytes::new(),
            text_query: Bytes::new(),
            dense_field: Bytes::new(),
            dense_blob: Bytes::new(),
            sparse: None,
            weights: [1.0, 1.0, 1.0],
            k_per_stream: None,
            top_k: 10,
            offset: 0,
            count: 10,
        };
        // max(60, 3*10) = 60
        assert_eq!(hq.effective_k_per_stream(), 60);

        let hq50 = HybridQuery {
            top_k: 50,
            ..hq.clone()
        };
        // max(60, 150) = 150
        assert_eq!(hq50.effective_k_per_stream(), 150);

        let hq_explicit = HybridQuery {
            k_per_stream: Some(200),
            ..hq
        };
        assert_eq!(hq_explicit.effective_k_per_stream(), 200);
    }

    // ── bm25_to_search_results tests (Task 3) ─────────────────────────────────

    #[test]
    fn test_bm25_to_search_results_sign_flip() {
        // RESEARCH Pitfall 8 — BM25 score (higher=better) flipped to distance (lower=better).
        let text_results = vec![
            TextSearchResult {
                doc_id: 0,
                key: Bytes::from_static(b"doc:a"),
                score: 10.0,
            },
            TextSearchResult {
                doc_id: 1,
                key: Bytes::from_static(b"doc:b"),
                score: 5.0,
            },
        ];
        let converted = bm25_to_search_results(&text_results);
        assert_eq!(converted.len(), 2);
        // score 10 → distance -10, score 5 → distance -5
        assert!((converted[0].distance + 10.0).abs() < 1e-6);
        assert!((converted[1].distance + 5.0).abs() < 1e-6);
        // Ordering is preserved (rank 0 is the best score, still at index 0).
        assert!(converted[0].distance < converted[1].distance);
    }

    #[test]
    fn test_bm25_to_search_results_xxh64_seed_zero() {
        // RESEARCH Pitfall 6 — key_hash MUST use xxh64 with seed 0 (HASH_SEED).
        // This matches VectorIndex.key_hash_to_key and SparseStore::insert convention,
        // allowing RRF to dedupe across all three streams by key_hash.
        let key = b"doc:42";
        let expected = xxhash_rust::xxh64::xxh64(key, 0);
        let text_results = vec![TextSearchResult {
            doc_id: 42,
            key: Bytes::from_static(key),
            score: 1.0,
        }];
        let converted = bm25_to_search_results(&text_results);
        assert_eq!(converted[0].key_hash, expected);
    }

    #[test]
    fn test_bm25_to_search_results_rank_preserved() {
        // Input order is preserved; the conversion is purely field-wise.
        let text_results: Vec<TextSearchResult> = vec![
            TextSearchResult {
                doc_id: 1,
                key: Bytes::from_static(b"k1"),
                score: 5.0,
            },
            TextSearchResult {
                doc_id: 2,
                key: Bytes::from_static(b"k2"),
                score: 3.0,
            },
            TextSearchResult {
                doc_id: 3,
                key: Bytes::from_static(b"k3"),
                score: 1.0,
            },
        ];
        let c = bm25_to_search_results(&text_results);
        assert_eq!(c.len(), 3);
        assert_eq!(c[0].id.0, 1);
        assert_eq!(c[1].id.0, 2);
        assert_eq!(c[2].id.0, 3);
        // After sign flip: [-5, -3, -1] — ascending order matches rank (0 is best).
        assert!(c[0].distance < c[1].distance);
        assert!(c[1].distance < c[2].distance);
    }

    // ── execute_hybrid_search_local tests (Task 3) ────────────────────────────

    #[test]
    fn test_execute_hybrid_local_unknown_index() {
        // Unknown index → Frame::Error. Uses empty stores — no fixture setup needed.
        let mut vs = VectorStore::new();
        let ts = TextStore::new();
        let query = HybridQuery {
            index_name: Bytes::from_static(b"nosuch"),
            text_query: Bytes::from_static(b"hello"),
            dense_field: Bytes::from_static(b"vec"),
            dense_blob: Bytes::from_static(b"\x00\x00\x00\x00"),
            sparse: None,
            weights: [1.0, 1.0, 1.0],
            k_per_stream: None,
            top_k: 10,
            offset: 0,
            count: 10,
        };
        let result = execute_hybrid_search_local(&mut vs, &ts, &query);
        match result {
            Frame::Error(msg) => {
                let s = std::str::from_utf8(&msg).unwrap();
                assert!(s.contains("unknown index"), "got: {s}");
            }
            _ => panic!("expected Frame::Error"),
        }
    }

    // ── build_hybrid_local_response tests (Task 3) ────────────────────────────

    #[test]
    #[cfg(feature = "text-index")]
    fn test_build_hybrid_response_shape() {
        // Pure-function test: fused results → response shape matches FT.SEARCH convention
        // [total, key, [__rrf_score, "N"], ..., bm25_hits, N, dense_hits, N, sparse_hits, N]
        let mut key_map = std::collections::HashMap::new();
        key_map.insert(100u64, Bytes::from_static(b"doc:alpha"));
        key_map.insert(200u64, Bytes::from_static(b"doc:beta"));

        let fused = vec![
            SearchResult {
                distance: -0.033,
                id: VectorId(1),
                key_hash: 100,
            },
            SearchResult {
                distance: -0.020,
                id: VectorId(2),
                key_hash: 200,
            },
        ];

        // Empty TextIndex — doc_id fallback not exercised since key_map covers both.
        let text_index = TextIndex::new(
            Bytes::from_static(b"idx"),
            vec![Bytes::from_static(b"doc:")],
            vec![crate::text::types::TextFieldDef::new(Bytes::from_static(
                b"body",
            ))],
            crate::text::types::BM25Config::default(),
        );

        let frame = build_hybrid_local_response(&fused, &key_map, &text_index, 5, 4, 3, 0, 10);
        match frame {
            Frame::Array(items) => {
                // Layout: total, key1, fields1, key2, fields2, "bm25_hits", 5, "dense_hits", 4, "sparse_hits", 3
                assert_eq!(items[0], Frame::Integer(2));
                assert_eq!(
                    items[1],
                    Frame::BulkString(Bytes::from_static(b"doc:alpha"))
                );
                // fields[1] is an inner array with __rrf_score + value
                if let Frame::Array(inner) = &items[2] {
                    assert_eq!(
                        inner[0],
                        Frame::BulkString(Bytes::from_static(b"__rrf_score"))
                    );
                } else {
                    panic!("expected inner Array for fields");
                }
                assert_eq!(items[3], Frame::BulkString(Bytes::from_static(b"doc:beta")));
                // Tail: bm25_hits + dense_hits + sparse_hits metadata
                let tail_start = items.len() - 6;
                assert_eq!(
                    items[tail_start],
                    Frame::BulkString(Bytes::from_static(b"bm25_hits"))
                );
                assert_eq!(items[tail_start + 1], Frame::Integer(5));
                assert_eq!(
                    items[tail_start + 2],
                    Frame::BulkString(Bytes::from_static(b"dense_hits"))
                );
                assert_eq!(items[tail_start + 3], Frame::Integer(4));
                assert_eq!(
                    items[tail_start + 4],
                    Frame::BulkString(Bytes::from_static(b"sparse_hits"))
                );
                assert_eq!(items[tail_start + 5], Frame::Integer(3));
            }
            _ => panic!("expected Array"),
        }
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_build_hybrid_response_vec_fallback() {
        // key_hash present but not in vector map AND not in text index → vec:<id> fallback.
        let key_map = std::collections::HashMap::new();
        let fused = vec![SearchResult {
            distance: -0.01,
            id: VectorId(77),
            key_hash: 999,
        }];
        let text_index = TextIndex::new(
            Bytes::from_static(b"idx"),
            vec![Bytes::from_static(b"doc:")],
            vec![crate::text::types::TextFieldDef::new(Bytes::from_static(
                b"body",
            ))],
            crate::text::types::BM25Config::default(),
        );
        let frame = build_hybrid_local_response(&fused, &key_map, &text_index, 0, 1, 0, 0, 10);
        if let Frame::Array(items) = frame {
            assert_eq!(items[1], Frame::BulkString(Bytes::from_static(b"vec:77")));
        } else {
            panic!("expected Array");
        }
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_build_hybrid_response_pagination() {
        // offset skips results; count truncates.
        let mut key_map = std::collections::HashMap::new();
        for i in 0..5 {
            key_map.insert(i as u64, Bytes::from(format!("k{i}")));
        }
        let fused: Vec<SearchResult> = (0..5)
            .map(|i| SearchResult {
                distance: -(i as f32) * 0.01,
                id: VectorId(i),
                key_hash: i as u64,
            })
            .collect();
        let text_index = TextIndex::new(
            Bytes::from_static(b"idx"),
            vec![Bytes::from_static(b"doc:")],
            vec![crate::text::types::TextFieldDef::new(Bytes::from_static(
                b"body",
            ))],
            crate::text::types::BM25Config::default(),
        );
        let frame = build_hybrid_local_response(&fused, &key_map, &text_index, 0, 0, 0, 1, 2);
        if let Frame::Array(items) = frame {
            // total = len(fused) = 5 (full match count, pre-pagination)
            assert_eq!(items[0], Frame::Integer(5));
            // page: skip 1, take 2 → ids 1 and 2 (k1, k2)
            assert_eq!(items[1], Frame::BulkString(Bytes::from_static(b"k1")));
            assert_eq!(items[3], Frame::BulkString(Bytes::from_static(b"k2")));
        } else {
            panic!("expected Array");
        }
    }

    // ── Backward compat regression test (Task 3) ──────────────────────────────

    #[test]
    fn test_hybrid_backward_compat_no_hybrid_keyword() {
        // Classic FT.SEARCH args (no HYBRID) must return Ok(None) from the parser.
        // This is the D-18 zero-impact guarantee — no code path change when HYBRID absent.
        let args = vec![
            bs(b"myidx"),
            bs(b"*=>[KNN 10 @vec $query]"),
            bs(b"PARAMS"),
            bs(b"2"),
            bs(b"query"),
            bs(b"\x00\x00\x00\x00"),
            bs(b"LIMIT"),
            bs(b"0"),
            bs(b"10"),
        ];
        assert!(parse_hybrid_modifier(&args).expect("no error").is_none());
    }

    // Multi-shard raw-streams codec tests live in hybrid_multi.rs.
}
