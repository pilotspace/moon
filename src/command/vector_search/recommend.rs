//! FT.RECOMMEND — vector-based item recommendation using positive/negative examples.
//!
//! Computes a centroid from positive example keys, optionally adjusts with negative
//! examples, then runs KNN search with the centroid as query vector. Example keys
//! are excluded from returned results.
//!
//! Syntax:
//! ```text
//! FT.RECOMMEND idx POSITIVE key1 key2 ... [NEGATIVE key3 key4 ...] [K num] [FIELD field_name]
//! ```

use bytes::Bytes;
use smallvec::SmallVec;
use std::collections::HashSet;

use crate::protocol::Frame;
use crate::storage::db::Database;
use crate::vector::segment::holder::MvccContext;
use crate::vector::store::VectorStore;
use crate::vector::types::SearchResult;

use super::{build_search_response, extract_bulk, matches_keyword, parse_usize};

/// Maximum K value to prevent memory explosion.
const MAX_K: usize = 1000;

/// Weight applied to each negative vector when adjusting the centroid.
const NEGATIVE_WEIGHT: f32 = 0.5;

/// Parsed arguments for FT.RECOMMEND.
struct RecommendArgs {
    index_name: Bytes,
    positive_keys: SmallVec<[Bytes; 8]>,
    negative_keys: SmallVec<[Bytes; 4]>,
    k: usize,
    field_name: Option<Bytes>,
}

/// Parse FT.RECOMMEND arguments.
///
/// Expected layout:
///   args[0] = index_name
///   args[1] = POSITIVE keyword
///   args[2..] = positive keys, then optional NEGATIVE keys, K num, FIELD name
fn parse_recommend_args(args: &[Frame]) -> Result<RecommendArgs, Frame> {
    if args.len() < 3 {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'FT.RECOMMEND' command",
        )));
    }

    let index_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Err(Frame::Error(Bytes::from_static(b"ERR invalid index name"))),
    };

    // args[1] must be POSITIVE keyword
    if !matches_keyword(&args[1], b"POSITIVE") {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR expected POSITIVE keyword after index name",
        )));
    }

    let mut positive_keys: SmallVec<[Bytes; 8]> = SmallVec::new();
    let mut negative_keys: SmallVec<[Bytes; 4]> = SmallVec::new();
    let mut k: usize = 10;
    let mut field_name: Option<Bytes> = None;

    // Parse remaining args: collect positive keys until hitting a keyword
    let mut i = 2;
    let mut in_negative = false;

    while i < args.len() {
        if matches_keyword(&args[i], b"NEGATIVE") {
            in_negative = true;
            i += 1;
            continue;
        }
        if matches_keyword(&args[i], b"K") {
            i += 1;
            if i >= args.len() {
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR K requires a numeric value",
                )));
            }
            k = match parse_usize(&args[i]) {
                Some(v) if v > 0 => v.min(MAX_K),
                _ => {
                    return Err(Frame::Error(Bytes::from_static(
                        b"ERR invalid K value",
                    )));
                }
            };
            i += 1;
            continue;
        }
        if matches_keyword(&args[i], b"FIELD") {
            i += 1;
            if i >= args.len() {
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR FIELD requires a field name",
                )));
            }
            field_name = match extract_bulk(&args[i]) {
                Some(b) => Some(b),
                None => {
                    return Err(Frame::Error(Bytes::from_static(
                        b"ERR invalid FIELD value",
                    )));
                }
            };
            i += 1;
            continue;
        }

        // Regular key argument
        let key = match extract_bulk(&args[i]) {
            Some(b) => b,
            None => {
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR invalid key argument",
                )));
            }
        };
        if in_negative {
            negative_keys.push(key);
        } else {
            positive_keys.push(key);
        }
        i += 1;
    }

    if positive_keys.is_empty() {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR at least one POSITIVE key is required",
        )));
    }

    Ok(RecommendArgs {
        index_name,
        positive_keys,
        negative_keys,
        k,
        field_name,
    })
}

/// Read a vector blob from a hash key's field, decode as f32 little-endian.
///
/// Returns `None` if the key doesn't exist, has no vector field, or the blob
/// doesn't divide evenly into f32s.
fn read_vector_from_hash(
    db: &mut Database,
    key: &[u8],
    vector_field: &[u8],
    expected_dim: usize,
) -> Option<Vec<f32>> {
    let hash_map = match db.get_hash(key) {
        Ok(Some(m)) => m,
        _ => return None,
    };
    let blob = hash_map.get(vector_field)?;
    if blob.len() != expected_dim * 4 {
        return None;
    }
    let mut vec = Vec::with_capacity(expected_dim);
    for chunk in blob.chunks_exact(4) {
        vec.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    Some(vec)
}

/// Compute the element-wise mean of a collection of vectors.
///
/// Caller guarantees `vecs` is non-empty and all vectors have the same length.
fn compute_centroid(vecs: &[Vec<f32>]) -> Vec<f32> {
    let dim = vecs[0].len();
    let mut centroid = vec![0.0f32; dim];
    let inv_n = 1.0 / vecs.len() as f32;
    for v in vecs {
        for (c, &x) in centroid.iter_mut().zip(v.iter()) {
            *c += x;
        }
    }
    for c in centroid.iter_mut() {
        *c *= inv_n;
    }
    centroid
}

/// Apply negative vector adjustment: centroid[i] -= weight * neg[i]
fn apply_negative_adjustment(centroid: &mut [f32], negatives: &[Vec<f32>]) {
    for neg in negatives {
        for (c, &n) in centroid.iter_mut().zip(neg.iter()) {
            *c -= NEGATIVE_WEIGHT * n;
        }
    }
}

/// Normalize a vector to unit length (L2 norm). No-op if norm is zero.
fn normalize_l2(vec: &mut [f32]) {
    let norm_sq: f32 = vec.iter().map(|x| x * x).sum();
    if norm_sq > f32::EPSILON {
        let inv_norm = 1.0 / norm_sq.sqrt();
        for x in vec.iter_mut() {
            *x *= inv_norm;
        }
    }
}

/// Compute xxh64 hash matching the index's key_hash computation.
///
/// Uses the same hasher as `auto_index_hset` to ensure key_hash values
/// in the exclude set match those in SearchResult.
fn key_hash(key: &[u8]) -> u64 {
    xxhash_rust::xxh64::xxh64(key, 0)
}

/// FT.RECOMMEND — recommend items similar to positive examples, dissimilar to negatives.
///
/// Algorithm:
/// 1. Look up positive/negative key vectors from Database hash entries
/// 2. Compute centroid of positive vectors
/// 3. Subtract weighted negative vectors
/// 4. Normalize if cosine metric
/// 5. Run KNN search with centroid
/// 6. Filter out example keys from results
pub fn ft_recommend(
    store: &mut VectorStore,
    args: &[Frame],
    db: Option<&mut Database>,
) -> Frame {
    let db = match db {
        Some(d) => d,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR FT.RECOMMEND requires database access",
            ));
        }
    };

    let parsed = match parse_recommend_args(args) {
        Ok(p) => p,
        Err(e) => return e,
    };

    // Look up the index to get metadata (dimension, metric, field name)
    let (dim, metric, source_field) = {
        let idx = match store.get_index(parsed.index_name.as_ref()) {
            Some(i) => i,
            None => return Frame::Error(Bytes::from_static(b"Unknown Index name")),
        };

        let field_meta = if let Some(ref fname) = parsed.field_name {
            match idx.meta.find_field(fname) {
                Some(fm) => fm,
                None => {
                    return Frame::Error(Bytes::from_static(
                        b"ERR unknown vector field",
                    ));
                }
            }
        } else {
            idx.meta.default_field()
        };

        (
            field_meta.dimension as usize,
            field_meta.metric,
            field_meta.field_name.clone(),
        )
    };

    // Read positive vectors from database
    let mut positive_vecs = Vec::with_capacity(parsed.positive_keys.len());
    for key in &parsed.positive_keys {
        if let Some(vec) = read_vector_from_hash(db, key, &source_field, dim) {
            positive_vecs.push(vec);
        }
    }

    if positive_vecs.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR no valid vectors found for POSITIVE keys",
        ));
    }

    // Read negative vectors from database
    let mut negative_vecs = Vec::with_capacity(parsed.negative_keys.len());
    for key in &parsed.negative_keys {
        if let Some(vec) = read_vector_from_hash(db, key, &source_field, dim) {
            negative_vecs.push(vec);
        }
    }

    // Compute centroid from positive vectors
    let mut centroid = compute_centroid(&positive_vecs);

    // Apply negative adjustment
    if !negative_vecs.is_empty() {
        apply_negative_adjustment(&mut centroid, &negative_vecs);
    }

    // Normalize for cosine metric
    if metric == crate::vector::types::DistanceMetric::Cosine {
        normalize_l2(&mut centroid);
    }

    // Build exclude set from positive + negative key hashes
    let mut exclude: HashSet<u64> = HashSet::with_capacity(
        parsed.positive_keys.len() + parsed.negative_keys.len(),
    );
    for key in parsed.positive_keys.iter().chain(parsed.negative_keys.iter()) {
        exclude.insert(key_hash(key));
    }

    // Run KNN search using the centroid as query vector.
    // Request extra results to compensate for filtered-out example keys.
    let search_k = parsed.k + exclude.len();

    let idx = match store.get_index_mut(parsed.index_name.as_ref()) {
        Some(i) => i,
        None => return Frame::Error(Bytes::from_static(b"Unknown Index name")),
    };

    // Auto-compact if threshold reached
    idx.try_compact();

    // ef_search: same heuristic as search_local_filtered
    let ef_search = if idx.meta.hnsw_ef_runtime > 0 {
        idx.meta.hnsw_ef_runtime as usize
    } else {
        let base = (search_k * 20).max(200);
        let dim_factor = if dim >= 768 {
            2
        } else if dim >= 384 {
            3
        } else {
            2
        };
        (base * dim_factor / 2).clamp(200, 1000)
    };

    let empty_committed = roaring::RoaringBitmap::new();
    let mvcc_ctx = MvccContext {
        snapshot_lsn: 0,
        my_txn_id: 0,
        committed: &empty_committed,
        dirty_set: &[],
        dimension: dim as u32,
    };

    // Dispatch to correct field segments
    let results = if parsed.field_name.is_none()
        || parsed.field_name.as_ref().is_some_and(|f| {
            f.eq_ignore_ascii_case(&idx.meta.default_field().field_name)
        })
    {
        idx.segments.search_mvcc(
            &centroid,
            search_k,
            ef_search,
            &mut idx.scratch,
            None,
            &mvcc_ctx,
        )
    } else {
        let fname = parsed.field_name.as_ref().unwrap();
        match idx.field_segments.get_mut(fname.as_ref()) {
            Some(fs) => fs.segments.search_mvcc(
                &centroid,
                search_k,
                ef_search,
                &mut fs.scratch,
                None,
                &mvcc_ctx,
            ),
            None => {
                return Frame::Error(Bytes::from_static(b"ERR unknown vector field"));
            }
        }
    };

    // Filter out positive and negative example keys
    let filtered: SmallVec<[SearchResult; 32]> = results
        .into_iter()
        .filter(|r| r.key_hash == 0 || !exclude.contains(&r.key_hash))
        .take(parsed.k)
        .collect();

    crate::vector::metrics::increment_search();
    build_search_response(&filtered, &idx.key_hash_to_key, 0, parsed.k)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_centroid_single() {
        let vecs = vec![vec![1.0, 2.0, 3.0]];
        let c = compute_centroid(&vecs);
        assert_eq!(c, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_compute_centroid_multiple() {
        let vecs = vec![vec![1.0, 0.0, 3.0], vec![3.0, 4.0, 1.0]];
        let c = compute_centroid(&vecs);
        assert!((c[0] - 2.0).abs() < f32::EPSILON);
        assert!((c[1] - 2.0).abs() < f32::EPSILON);
        assert!((c[2] - 2.0).abs() < f32::EPSILON);
    }

    #[test]
    fn test_negative_adjustment() {
        let mut centroid = vec![2.0, 2.0, 2.0];
        let negatives = vec![vec![1.0, 1.0, 1.0]];
        apply_negative_adjustment(&mut centroid, &negatives);
        assert!((centroid[0] - 1.5).abs() < f32::EPSILON);
        assert!((centroid[1] - 1.5).abs() < f32::EPSILON);
        assert!((centroid[2] - 1.5).abs() < f32::EPSILON);
    }

    #[test]
    fn test_normalize_l2() {
        let mut v = vec![3.0, 4.0];
        normalize_l2(&mut v);
        assert!((v[0] - 0.6).abs() < 1e-6);
        assert!((v[1] - 0.8).abs() < 1e-6);
    }

    #[test]
    fn test_normalize_l2_zero() {
        let mut v = vec![0.0, 0.0];
        normalize_l2(&mut v);
        assert_eq!(v, vec![0.0, 0.0]);
    }

    #[test]
    fn test_parse_args_minimal() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"myidx")),
            Frame::BulkString(Bytes::from_static(b"POSITIVE")),
            Frame::BulkString(Bytes::from_static(b"doc:1")),
        ];
        let parsed = parse_recommend_args(&args).unwrap();
        assert_eq!(parsed.index_name, Bytes::from_static(b"myidx"));
        assert_eq!(parsed.positive_keys.len(), 1);
        assert!(parsed.negative_keys.is_empty());
        assert_eq!(parsed.k, 10);
        assert!(parsed.field_name.is_none());
    }

    #[test]
    fn test_parse_args_full() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"myidx")),
            Frame::BulkString(Bytes::from_static(b"POSITIVE")),
            Frame::BulkString(Bytes::from_static(b"doc:1")),
            Frame::BulkString(Bytes::from_static(b"doc:2")),
            Frame::BulkString(Bytes::from_static(b"NEGATIVE")),
            Frame::BulkString(Bytes::from_static(b"doc:3")),
            Frame::BulkString(Bytes::from_static(b"K")),
            Frame::BulkString(Bytes::from_static(b"5")),
            Frame::BulkString(Bytes::from_static(b"FIELD")),
            Frame::BulkString(Bytes::from_static(b"title_vec")),
        ];
        let parsed = parse_recommend_args(&args).unwrap();
        assert_eq!(parsed.positive_keys.len(), 2);
        assert_eq!(parsed.negative_keys.len(), 1);
        assert_eq!(parsed.k, 5);
        assert_eq!(parsed.field_name, Some(Bytes::from_static(b"title_vec")));
    }

    #[test]
    fn test_parse_args_missing_positive() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"myidx")),
            Frame::BulkString(Bytes::from_static(b"POSITIVE")),
        ];
        assert!(parse_recommend_args(&args).is_err());
    }

    #[test]
    fn test_parse_args_no_positive_keyword() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"myidx")),
            Frame::BulkString(Bytes::from_static(b"doc:1")),
        ];
        assert!(parse_recommend_args(&args).is_err());
    }

    #[test]
    fn test_k_capped_at_max() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"myidx")),
            Frame::BulkString(Bytes::from_static(b"POSITIVE")),
            Frame::BulkString(Bytes::from_static(b"doc:1")),
            Frame::BulkString(Bytes::from_static(b"K")),
            Frame::BulkString(Bytes::from_static(b"5000")),
        ];
        let parsed = parse_recommend_args(&args).unwrap();
        assert_eq!(parsed.k, MAX_K);
    }
}
