//! FT.SEARCH local execution helpers.
//!
//! Split from the monolithic `ft_search.rs` in Phase 152 (Plan 02.5). Owns the
//! mechanics of running a search against a `VectorStore`: dense KNN
//! (`search_local`, `search_local_filtered`, `search_local_raw`), the raw-result
//! `SearchRawResult` enum consumed by the orchestrator, post-filter range
//! trimming (`apply_range_filter`), and sparse query blob decoding
//! (`parse_sparse_query_blob`). Response building lives in `response.rs`;
//! high-level orchestration in `dispatch.rs`.
//!
//! The split itself was a pure relocation. Behaviour has since changed here
//! once: `apply_range_filter` now takes an explicit [`ScoreOrder`] instead of
//! inferring the comparison direction from the index's dense metric, which was
//! wrong for dense COSINE/IP and unrelated to the sparse and RRF scores it was
//! also applied to (moon#748).

use bytes::Bytes;
use smallvec::SmallVec;

use crate::protocol::Frame;
use crate::vector::filter::FilterExpr;
use crate::vector::keymap::BucketedKeyMap;
use crate::vector::store::VectorStore;
use crate::vector::types::SearchResult;

use super::response::build_search_response;

/// Result of search_local_raw — either raw results or an error Frame.
#[allow(clippy::large_enum_variant)]
pub(super) enum SearchRawResult {
    Ok {
        results: SmallVec<[SearchResult; 32]>,
        key_hash_to_key: BucketedKeyMap<Bytes>,
    },
    Error(Frame),
}

/// Search returning raw results (not yet built into a Frame response).
/// Used by session-aware ft_search to filter results before response building.
///
/// `field_name` selects which named vector field to search. `None` uses the default
/// (first) field. `Some(name)` dispatches to the named field's segments.
pub(super) fn search_local_raw(
    store: &mut VectorStore,
    index_name: &[u8],
    query_blob: &[u8],
    k: usize,
    filter: Option<&FilterExpr>,
    field_name: Option<&Bytes>,
    as_of_lsn: u64,
    db_index: u8,
) -> SearchRawResult {
    // Clone committed treemap BEFORE get_index_mut to satisfy the borrow checker.
    // Non-TXN readers need this to see entries whose owning txn has committed
    // (entries tagged with txn_id by auto_index_hset_public_txn; ACID-09 fix).
    let committed = store.txn_manager().committed_snapshot();
    // WS5a: db-scoped — an index owned by a different db is invisible (NOTFOUND).
    let idx = match store.get_index_mut_for_db(index_name, db_index) {
        Some(i) => i,
        None => {
            return SearchRawResult::Error(Frame::Error(Bytes::from_static(b"Unknown Index name")));
        }
    };

    // Resolve target field: determine dimension, segments, scratch, collection
    let (dim, use_default_field) = if let Some(fname) = field_name {
        if let Some(field_meta) = idx.meta.find_field(fname) {
            let is_default = fname.eq_ignore_ascii_case(&idx.meta.default_field().field_name);
            (field_meta.dimension as usize, is_default)
        } else {
            return SearchRawResult::Error(Frame::Error(Bytes::from(format!(
                "ERR unknown vector field '@{}'",
                String::from_utf8_lossy(fname)
            ))));
        }
    } else {
        (idx.meta.dimension as usize, true)
    };

    let query_f32 = if query_blob.len() == dim * 4 {
        let mut v = Vec::with_capacity(dim);
        for chunk in query_blob.chunks_exact(4) {
            v.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
        }
        v
    } else if let Ok(text) = std::str::from_utf8(query_blob) {
        let parsed: Vec<f32> = text
            .split(',')
            .filter(|s| !s.is_empty())
            .filter_map(|s| s.trim().parse::<f32>().ok())
            .collect();
        if parsed.len() != dim {
            return SearchRawResult::Error(Frame::Error(Bytes::from_static(
                b"ERR query vector dimension mismatch",
            )));
        }
        parsed
    } else {
        return SearchRawResult::Error(Frame::Error(Bytes::from_static(
            b"ERR query vector dimension mismatch",
        )));
    };

    idx.try_compact();

    // AE-1: remember whether ef came from the heuristic (segment-level
    // adaptive-ef estimates only apply then, never over a user EF_RUNTIME).
    let ef_defaulted = idx.meta.hnsw_ef_runtime == 0;
    let tuning = crate::vector::types::SearchTuning {
        rerank_mult: idx.meta.rerank_mult,
        exact_beam: idx.meta.exact_beam,
    };
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

    let filter_bitmap = filter.map(|f| {
        let total = idx.segments.total_vectors();
        idx.payload_index.evaluate_bitmap(f, total)
    });

    // Dispatch to correct field's segments
    if use_default_field {
        let mvcc_ctx = crate::vector::segment::holder::MvccContext {
            snapshot_lsn: as_of_lsn,
            my_txn_id: 0,
            committed: &committed,
            dirty_set: &[],
            dimension: dim as u32,
            ef_defaulted,
            tuning,
        };
        let results = idx.segments.search_mvcc(
            &query_f32,
            k,
            ef_search,
            &mut idx.scratch,
            filter_bitmap.as_ref(),
            &mvcc_ctx,
        );
        let key_hash_to_key = idx.key_hash_to_key.clone();
        SearchRawResult::Ok {
            results,
            key_hash_to_key,
        }
    } else {
        #[allow(clippy::unwrap_used)]
        // guarded: use_default_field is false only when field_name is Some
        let fname = field_name.unwrap();
        if let Some(fs) = idx.field_segments.get_mut(fname.as_ref()) {
            let mvcc_ctx = crate::vector::segment::holder::MvccContext {
                snapshot_lsn: as_of_lsn,
                my_txn_id: 0,
                committed: &committed,
                dirty_set: &[],
                dimension: dim as u32,
                ef_defaulted,
                tuning,
            };
            let results = fs.segments.search_mvcc(
                &query_f32,
                k,
                ef_search,
                &mut fs.scratch,
                filter_bitmap.as_ref(),
                &mvcc_ctx,
            );
            let key_hash_to_key = idx.key_hash_to_key.clone();
            SearchRawResult::Ok {
                results,
                key_hash_to_key,
            }
        } else {
            SearchRawResult::Error(Frame::Error(Bytes::from(format!(
                "ERR unknown vector field '@{}'",
                String::from_utf8_lossy(fname)
            ))))
        }
    }
}

/// Direct local search for cross-shard VectorSearch messages.
/// Skips FT.SEARCH parsing -- the coordinator already extracted index_name, blob, k.
///
/// Returns all results (no pagination) -- the coordinator applies LIMIT after merge.
/// Always searches the default field (cross-shard multi-field not in scope).
pub fn search_local(
    store: &mut VectorStore,
    index_name: &[u8],
    query_blob: &[u8],
    k: usize,
    db_index: u8,
) -> Frame {
    search_local_filtered(
        store,
        index_name,
        query_blob,
        k,
        None,
        0,
        usize::MAX,
        None,
        0,
        db_index,
    )
}

/// Local search with optional filter expression and pagination.
///
/// Evaluates filter against PayloadIndex to produce bitmap, then dispatches
/// to search_filtered which selects optimal strategy (brute-force/HNSW/post-filter).
///
/// `offset` and `count` control pagination of the result set. The total match count
/// is always returned as the first element; only the paginated slice of documents
/// is included in the response.
///
/// `field_name` selects which named vector field to search. `None` uses the default field.
pub fn search_local_filtered(
    store: &mut VectorStore,
    index_name: &[u8],
    query_blob: &[u8],
    k: usize,
    filter: Option<&FilterExpr>,
    offset: usize,
    count: usize,
    field_name: Option<&Bytes>,
    as_of_lsn: u64,
    db_index: u8,
) -> Frame {
    // Clone committed treemap BEFORE get_index_mut (borrow-checker ordering).
    // Ensures non-TXN readers see entries whose owning txn has committed.
    let committed = store.txn_manager().committed_snapshot();
    // WS5a: db-scoped — an index owned by a different db is invisible (NOTFOUND).
    let idx = match store.get_index_mut_for_db(index_name, db_index) {
        Some(i) => i,
        None => return Frame::Error(Bytes::from_static(b"Unknown Index name")),
    };

    // Resolve target field dimension
    let (dim, use_default_field) = if let Some(fname) = field_name {
        if let Some(field_meta) = idx.meta.find_field(fname) {
            let is_default = fname.eq_ignore_ascii_case(&idx.meta.default_field().field_name);
            (field_meta.dimension as usize, is_default)
        } else {
            return Frame::Error(Bytes::from(format!(
                "ERR unknown vector field '@{}'",
                String::from_utf8_lossy(fname)
            )));
        }
    } else {
        (idx.meta.dimension as usize, true)
    };

    // Primary path: binary little-endian f32 blob (RediSearch-compatible).
    // Fallback: comma-separated floats in a UTF-8 string. This supports the
    // Moon Console REST/WS bridge which transmits args as JSON strings and
    // cannot carry raw binary blobs.
    let query_f32 = if query_blob.len() == dim * 4 {
        let mut v = Vec::with_capacity(dim);
        for chunk in query_blob.chunks_exact(4) {
            v.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
        }
        v
    } else if let Ok(text) = std::str::from_utf8(query_blob) {
        let parsed: Vec<f32> = text
            .split(',')
            .filter(|s| !s.is_empty())
            .filter_map(|s| s.trim().parse::<f32>().ok())
            .collect();
        if parsed.len() != dim {
            return Frame::Error(Bytes::from_static(b"ERR query vector dimension mismatch"));
        }
        parsed
    } else {
        return Frame::Error(Bytes::from_static(b"ERR query vector dimension mismatch"));
    };

    // Auto-compact mutable -> HNSW if threshold reached (lazy, first search only).
    idx.try_compact();

    // ef_search: user-configurable via EF_RUNTIME in FT.CREATE, or auto-computed.
    // Higher ef = better recall but lower QPS. Auto scales with k and dimension:
    // base = k*20, min 200, boosted for high-d where TQ-ADC needs wider beam.
    // AE-1: remember whether ef came from the heuristic (segment-level
    // adaptive-ef estimates only apply then, never over a user EF_RUNTIME).
    let ef_defaulted = idx.meta.hnsw_ef_runtime == 0;
    let tuning = crate::vector::types::SearchTuning {
        rerank_mult: idx.meta.rerank_mult,
        exact_beam: idx.meta.exact_beam,
    };
    let ef_search = if idx.meta.hnsw_ef_runtime > 0 {
        idx.meta.hnsw_ef_runtime as usize
    } else {
        let base = (k * 20).max(200);
        // Dimension boost: +50% at 384d+, +100% at 768d+
        let dim_factor = if dim >= 768 {
            2
        } else if dim >= 384 {
            3
        } else {
            2
        };
        (base * dim_factor / 2).clamp(200, 1000)
    };

    let filter_bitmap = filter.map(|f| {
        let total = idx.segments.total_vectors();
        idx.payload_index.evaluate_bitmap(f, total)
    });

    // Dispatch to correct field's segments
    if use_default_field {
        let mvcc_ctx = crate::vector::segment::holder::MvccContext {
            snapshot_lsn: as_of_lsn,
            my_txn_id: 0,
            committed: &committed,
            dirty_set: &[],
            dimension: dim as u32,
            ef_defaulted,
            tuning,
        };
        let results = idx.segments.search_mvcc(
            &query_f32,
            k,
            ef_search,
            &mut idx.scratch,
            filter_bitmap.as_ref(),
            &mvcc_ctx,
        );
        build_search_response(&results, &idx.key_hash_to_key, offset, count)
    } else {
        #[allow(clippy::unwrap_used)]
        // guarded: use_default_field is false only when field_name is Some
        let fname = field_name.unwrap();
        if let Some(fs) = idx.field_segments.get_mut(fname.as_ref()) {
            let mvcc_ctx = crate::vector::segment::holder::MvccContext {
                snapshot_lsn: as_of_lsn,
                my_txn_id: 0,
                committed: &committed,
                dirty_set: &[],
                dimension: dim as u32,
                ef_defaulted,
                tuning,
            };
            let results = fs.segments.search_mvcc(
                &query_f32,
                k,
                ef_search,
                &mut fs.scratch,
                filter_bitmap.as_ref(),
                &mvcc_ctx,
            );
            build_search_response(&results, &idx.key_hash_to_key, offset, count)
        } else {
            Frame::Error(Bytes::from(format!(
                "ERR unknown vector field '@{}'",
                String::from_utf8_lossy(fname)
            )))
        }
    }
}

/// Maximum results from a RANGE query to prevent memory explosion.
const RANGE_HARD_CAP: usize = 10_000;

/// Which direction of a [`SearchResult::distance`] means "better".
///
/// `SearchResult.distance` does not carry one convention. Three different kinds
/// of score travel in that field, and only the call site knows which:
///
/// - **dense KNN** — a true distance. Lower is closer, for *every* metric: the
///   unit-sphere metrics normalize at encode time and score `‖a−b‖² = 2−2·cos`,
///   which increases with cosine distance just as L2 does.
/// - **sparse** — a raw dot product straight out of `SparseStore::search`,
///   never negated. Higher is better.
/// - **RRF fusion** — accumulated `1/(k+rank)` terms, but `rrf_fuse` stores
///   `distance = -score` so `SearchResult::Ord` sorts best-first. Lower is
///   better, like a dense distance.
///
/// Note how little the field name tells you: two of these three are "scores"
/// and one of those is negated. A live probe, not the name, is what settles it.
///
/// This used to be inferred from the index's *dense* metric even when the
/// values being filtered were sparse or fused scores, which is how moon#748
/// stayed hidden: the dense branch was reversed, and the sparse/fused sites were
/// right or wrong depending on an unrelated field's metric. Naming the
/// convention at the call site is what makes each one checkable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ScoreOrder {
    /// Dense KNN distances: keep `distance <= threshold`.
    LowerIsCloser,
    /// Sparse dot products and RRF fusion scores: keep `distance >= threshold`.
    HigherIsBetter,
}

/// Apply the `RANGE <threshold>` filter, then truncate to [`RANGE_HARD_CAP`].
///
/// `order` must describe the scores actually in `results` — see [`ScoreOrder`].
pub(super) fn apply_range_filter(
    results: &mut SmallVec<[SearchResult; 32]>,
    threshold: f32,
    order: ScoreOrder,
) {
    match order {
        ScoreOrder::LowerIsCloser => results.retain(|r| r.distance <= threshold),
        ScoreOrder::HigherIsBetter => results.retain(|r| r.distance >= threshold),
    }
    results.truncate(RANGE_HARD_CAP);
}

/// Parse a sparse query blob: alternating u32 (LE dim_id) + f32 (LE weight) pairs.
/// Returns empty Vec on invalid input.
///
/// `pub(crate)` exposure (Phase 152 Plan 04): the hybrid path decodes sparse
/// blobs the same way as the two-way SPARSE-only path — keeping a single shared
/// decoder avoids drift if the wire format changes.
pub(crate) fn parse_sparse_query_blob(blob: &[u8]) -> Vec<(u32, f32)> {
    if blob.len() % 8 != 0 || blob.is_empty() {
        return Vec::new();
    }
    let num_pairs = blob.len() / 8;
    let mut pairs = Vec::with_capacity(num_pairs);
    for i in 0..num_pairs {
        let offset = i * 8;
        let dim = u32::from_le_bytes([
            blob[offset],
            blob[offset + 1],
            blob[offset + 2],
            blob[offset + 3],
        ]);
        let weight = f32::from_le_bytes([
            blob[offset + 4],
            blob[offset + 5],
            blob[offset + 6],
            blob[offset + 7],
        ]);
        pairs.push((dim, weight));
    }
    pairs
}
