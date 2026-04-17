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
//! No behavior change relative to the original `ft_search.rs` — pure relocation.

use bytes::Bytes;
use smallvec::SmallVec;

use crate::protocol::Frame;
use crate::vector::filter::FilterExpr;
use crate::vector::store::VectorStore;
use crate::vector::types::{DistanceMetric, SearchResult};

use super::response::build_search_response;

/// Result of search_local_raw — either raw results or an error Frame.
#[allow(clippy::large_enum_variant)]
pub(super) enum SearchRawResult {
    Ok {
        results: SmallVec<[SearchResult; 32]>,
        key_hash_to_key: std::collections::HashMap<u64, Bytes>,
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
) -> SearchRawResult {
    let idx = match store.get_index_mut(index_name) {
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

    let empty_committed = roaring::RoaringTreemap::new();

    // Dispatch to correct field's segments
    if use_default_field {
        let mvcc_ctx = crate::vector::segment::holder::MvccContext {
            snapshot_lsn: 0,
            my_txn_id: 0,
            committed: &empty_committed,
            dirty_set: &[],
            dimension: dim as u32,
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
                snapshot_lsn: 0,
                my_txn_id: 0,
                committed: &empty_committed,
                dirty_set: &[],
                dimension: dim as u32,
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
) -> Frame {
    search_local_filtered(store, index_name, query_blob, k, None, 0, usize::MAX, None)
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
) -> Frame {
    let idx = match store.get_index_mut(index_name) {
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

    let empty_committed = roaring::RoaringTreemap::new();

    // Dispatch to correct field's segments
    if use_default_field {
        let mvcc_ctx = crate::vector::segment::holder::MvccContext {
            snapshot_lsn: 0,
            my_txn_id: 0,
            committed: &empty_committed,
            dirty_set: &[],
            dimension: dim as u32,
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
                snapshot_lsn: 0,
                my_txn_id: 0,
                committed: &empty_committed,
                dirty_set: &[],
                dimension: dim as u32,
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

/// Apply range threshold filtering to search results based on distance metric.
///
/// - L2: lower distance = more similar, keep `distance <= threshold`
/// - Cosine/IP: higher score = more similar, keep `distance >= threshold`
///
/// Truncates to `RANGE_HARD_CAP` after filtering.
pub(super) fn apply_range_filter(
    results: &mut SmallVec<[SearchResult; 32]>,
    threshold: f32,
    metric: DistanceMetric,
) {
    results.retain(|r| match metric {
        DistanceMetric::L2 => r.distance <= threshold,
        DistanceMetric::Cosine | DistanceMetric::InnerProduct => r.distance >= threshold,
    });
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
