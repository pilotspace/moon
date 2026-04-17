//! Multi-shard hybrid FT.SEARCH raw-streams path (Phase 152 Plan 05).
//!
//! Per **D-13** ("Coordinator re-fuses across shards with global RRF on the
//! union"): each shard runs BM25 with injected global IDF + dense KNN +
//! optional sparse, then returns THREE separate raw per-stream lists UNFUSED.
//! The coordinator unions per stream across shards and calls
//! `rrf_fuse_three` exactly once on the unions — preserving the single-shard
//! RRF math invariant across shard counts.
//!
//! Per **W6** (key preservation) each result carries its shard-resolved key
//! bytes in a `KeyedResult { result, key }` wrapper, so the coordinator
//! response uses shard-preserved keys directly — no synthetic `vec:<id>`
//! fallback at the coordinator response layer.
//!
//! The single-shard fused path lives in `hybrid.rs`. This file owns the
//! multi-shard codec + per-shard raw-streams executor. The coordinator-side
//! scatter-gather orchestration lives in `src/shard/scatter_hybrid.rs`.

use bytes::Bytes;

use crate::protocol::{Frame, FrameVec};
use crate::text::store::{TextIndex, TextStore};
use crate::vector::store::VectorStore;
use crate::vector::types::{SearchResult, VectorId};

use super::ft_search::execute::parse_sparse_query_blob;
use super::hybrid::{bm25_to_search_results, resolve_hybrid_doc_key, run_dense_knn};

// ─── Types ───────────────────────────────────────────────────────────────────

/// A `SearchResult` paired with its shard-resolved key bytes.
///
/// W6: shard-side resolution avoids cross-shard `key_hash → Bytes` lookups at
/// the coordinator. Each shard owns the authoritative `doc_id_to_key` /
/// `key_hash_to_key` map for docs it indexed, and emits the key bytes directly.
#[derive(Debug, Clone)]
pub struct KeyedResult {
    pub result: SearchResult,
    pub key: Bytes,
}

/// Three-stream reply carrying UNFUSED ranked lists from a single shard.
///
/// Coordinator unions per stream across shards and calls `rrf_fuse_three`
/// exactly once on the unions (D-13 + D-14).
#[derive(Debug)]
pub struct ShardHybridReply {
    pub bm25: Vec<KeyedResult>,
    pub dense: Vec<KeyedResult>,
    pub sparse: Vec<KeyedResult>,
}

// ─── Per-shard raw-streams executor ──────────────────────────────────────────

/// Execute hybrid on this shard with GLOBAL IDF and return THREE raw streams.
///
/// Per **D-13** this function does NOT call `rrf_fuse_three`. It returns
/// three separate ranked lists (bm25, dense, sparse), each locally truncated
/// to `top_k` but UNFUSED. The coordinator performs the single RRF call.
///
/// Errors (all returned as `Frame::Error`, never panic):
/// - Unknown index → `"ERR unknown index"`
/// - Index has no TEXT fields → propagated via `execute_query_on_index`
/// - SPARSE on an index lacking that sparse field → `"ERR sparse field not defined in index"` (D-16)
/// - Malformed dense or sparse blob → propagated
#[allow(clippy::too_many_arguments)]
pub fn execute_hybrid_search_local_raw_streams(
    vector_store: &mut VectorStore,
    text_store: &TextStore,
    index_name: &Bytes,
    query_terms: &[crate::command::vector_search::ft_text_search::QueryTerm],
    dense_field: &Bytes,
    dense_blob: &Bytes,
    sparse: Option<(&Bytes, &Bytes)>,
    _weights: [f32; 3], // unused at shard level — coordinator applies weights
    k_per_stream: usize,
    top_k: usize,
    global_df: &std::collections::HashMap<String, u32>,
    global_n: u32,
) -> Frame {
    // ── Stream 1: BM25 with injected global IDF ──────────────────────────────
    let text_index = match text_store.get_index(index_name.as_ref()) {
        Some(ix) => ix,
        None => return Frame::Error(Bytes::from_static(b"ERR unknown index")),
    };
    let clause = crate::command::vector_search::ft_text_search::TextQueryClause {
        field_name: None,
        terms: query_terms.to_vec(),
        #[cfg(feature = "text-index")]
        filter: None,
    };
    let text_results = crate::command::vector_search::ft_text_search::execute_query_on_index(
        text_index,
        &clause,
        Some(global_df),
        Some(global_n),
        k_per_stream,
    );
    let bm25 = bm25_to_search_results(&text_results);

    // ── Stream 2: dense KNN ──────────────────────────────────────────────────
    let idx = match vector_store.get_index_mut(index_name.as_ref()) {
        Some(ix) => ix,
        None => return Frame::Error(Bytes::from_static(b"ERR unknown index")),
    };
    let (dense_results, key_hash_to_key) =
        match run_dense_knn(idx, dense_field, dense_blob, k_per_stream) {
            Ok(v) => v,
            Err(frame) => return frame,
        };

    // ── Stream 3: sparse (optional) ──────────────────────────────────────────
    let sparse_results: Vec<SearchResult> = if let Some((sf, sblob)) = sparse {
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
                return Frame::Error(Bytes::from_static(b"ERR sparse field not defined in index"));
            }
        }
    } else {
        Vec::new()
    };

    // Truncate each stream to top_k locally — shard does NOT fuse them.
    let bm25: Vec<SearchResult> = bm25.into_iter().take(top_k).collect();
    let dense: Vec<SearchResult> = dense_results.into_iter().take(top_k).collect();
    let sparse_results: Vec<SearchResult> = sparse_results.into_iter().take(top_k).collect();

    encode_shard_hybrid_partial(&bm25, &dense, &sparse_results, text_index, &key_hash_to_key)
}

// ─── Frame codec ─────────────────────────────────────────────────────────────

/// Encode three unfused per-stream ranked lists into a single `Frame::Array`.
///
/// Layout (flat):
/// ```text
/// Array[
///   bm25_count:Int, (dist:Bulk, id:Int, kh:Int, key:Bulk) * bm25_count,
///   dense_count:Int, (dist:Bulk, id:Int, kh:Int, key:Bulk) * dense_count,
///   sparse_count:Int, (dist:Bulk, id:Int, kh:Int, key:Bulk) * sparse_count,
/// ]
/// ```
///
/// Per W6 each result embeds its shard-resolved key bytes so the coordinator
/// response builder can use the key directly — no synthetic `vec:<id>`
/// fallback at the coordinator side.
pub fn encode_shard_hybrid_partial(
    bm25: &[SearchResult],
    dense: &[SearchResult],
    sparse: &[SearchResult],
    text_index: &TextIndex,
    vector_key_hash_to_key: &std::collections::HashMap<u64, Bytes>,
) -> Frame {
    let total_items = 3 + 4 * (bm25.len() + dense.len() + sparse.len());
    let mut out: Vec<Frame> = Vec::with_capacity(total_items);
    for stream in [bm25, dense, sparse] {
        out.push(Frame::Integer(stream.len() as i64));
        for r in stream {
            // distance as bulk string for precision preservation across RESP.
            let mut dist_buf = String::with_capacity(16);
            {
                use std::fmt::Write;
                let _ = write!(dist_buf, "{}", r.distance);
            }
            out.push(Frame::BulkString(Bytes::from(dist_buf)));
            out.push(Frame::Integer(r.id.0 as i64));
            out.push(Frame::Integer(r.key_hash as i64));
            let key = resolve_hybrid_doc_key(r, vector_key_hash_to_key, text_index);
            out.push(Frame::BulkString(key));
        }
    }
    Frame::Array(FrameVec::from(out))
}

/// Decode a three-stream shard reply into `ShardHybridReply`.
///
/// Returns `None` on any layout mismatch so the coordinator can collapse
/// malformed replies to a canonical error — threat T-152-05-02: no panics
/// on malformed shard input, ever.
pub fn decode_shard_hybrid_partial(frame: &Frame) -> Option<ShardHybridReply> {
    let items: &[Frame] = match frame {
        Frame::Array(a) => a.as_ref(),
        _ => return None,
    };
    let mut i: usize = 0;
    let mut streams: [Vec<KeyedResult>; 3] = [Vec::new(), Vec::new(), Vec::new()];
    for stream in streams.iter_mut() {
        let n = match items.get(i)? {
            Frame::Integer(n) => {
                if *n < 0 {
                    return None;
                }
                *n as usize
            }
            _ => return None,
        };
        i += 1;
        stream.reserve(n);
        for _ in 0..n {
            let distance_bytes = match items.get(i)? {
                Frame::BulkString(b) => b,
                _ => return None,
            };
            let distance: f32 = std::str::from_utf8(distance_bytes).ok()?.parse().ok()?;
            i += 1;
            let id_raw = match items.get(i)? {
                Frame::Integer(n) => *n as u32,
                _ => return None,
            };
            i += 1;
            let kh_raw = match items.get(i)? {
                Frame::Integer(n) => *n as u64,
                _ => return None,
            };
            i += 1;
            let key = match items.get(i)? {
                Frame::BulkString(b) => b.clone(),
                _ => return None,
            };
            i += 1;
            stream.push(KeyedResult {
                result: SearchResult {
                    distance,
                    id: VectorId(id_raw),
                    key_hash: kh_raw,
                },
                key,
            });
        }
    }
    let [bm25, dense, sparse] = streams;
    Some(ShardHybridReply {
        bm25,
        dense,
        sparse,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::text::store::TextIndex;

    fn tsr(distance: f32, id: u32, key_hash: u64) -> SearchResult {
        SearchResult {
            distance,
            id: VectorId(id),
            key_hash,
        }
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_shard_hybrid_reply_roundtrip_preserves_keys() {
        // W6: encode/decode preserve key bytes exactly, for every result.
        let bm25 = vec![tsr(-5.0, 1, 0xAA), tsr(-3.0, 2, 0xBB)];
        let dense = vec![tsr(0.1, 3, 0xCC)];
        let sparse = vec![tsr(0.2, 4, 0xDD), tsr(0.3, 5, 0xEE)];

        let mut vec_map: std::collections::HashMap<u64, Bytes> = std::collections::HashMap::new();
        vec_map.insert(0xCC, Bytes::from_static(b"doc:three"));
        vec_map.insert(0xDD, Bytes::from_static(b"doc:four"));
        vec_map.insert(0xEE, Bytes::from_static(b"doc:five"));

        // For BM25 keys, fall back through text_index.doc_id_to_key.
        let mut text_index = TextIndex::new(
            Bytes::from_static(b"idx"),
            vec![Bytes::from_static(b"doc:")],
            vec![crate::text::types::TextFieldDef::new(Bytes::from_static(
                b"body",
            ))],
            crate::text::types::BM25Config::default(),
        );
        text_index
            .doc_id_to_key
            .insert(1, Bytes::from_static(b"doc:one"));
        text_index
            .doc_id_to_key
            .insert(2, Bytes::from_static(b"doc:two"));

        let frame = encode_shard_hybrid_partial(&bm25, &dense, &sparse, &text_index, &vec_map);
        let decoded = decode_shard_hybrid_partial(&frame).expect("decode");

        assert_eq!(decoded.bm25.len(), 2);
        assert_eq!(decoded.dense.len(), 1);
        assert_eq!(decoded.sparse.len(), 2);

        assert_eq!(decoded.bm25[0].key.as_ref(), b"doc:one");
        assert_eq!(decoded.bm25[1].key.as_ref(), b"doc:two");
        assert_eq!(decoded.dense[0].key.as_ref(), b"doc:three");
        assert_eq!(decoded.sparse[0].key.as_ref(), b"doc:four");
        assert_eq!(decoded.sparse[1].key.as_ref(), b"doc:five");

        // Result values round-trip.
        assert!((decoded.bm25[0].result.distance - (-5.0)).abs() < 1e-4);
        assert_eq!(decoded.bm25[0].result.id.0, 1);
        assert_eq!(decoded.bm25[0].result.key_hash, 0xAA);
        assert!((decoded.dense[0].result.distance - 0.1).abs() < 1e-4);
        assert_eq!(decoded.sparse[1].result.key_hash, 0xEE);
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_shard_hybrid_reply_empty_streams() {
        let text_index = TextIndex::new(
            Bytes::from_static(b"idx"),
            vec![Bytes::from_static(b"doc:")],
            vec![crate::text::types::TextFieldDef::new(Bytes::from_static(
                b"body",
            ))],
            crate::text::types::BM25Config::default(),
        );
        let vec_map: std::collections::HashMap<u64, Bytes> = std::collections::HashMap::new();
        let frame = encode_shard_hybrid_partial(&[], &[], &[], &text_index, &vec_map);
        let decoded = decode_shard_hybrid_partial(&frame).expect("decode empty");
        assert!(decoded.bm25.is_empty());
        assert!(decoded.dense.is_empty());
        assert!(decoded.sparse.is_empty());
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_shard_hybrid_reply_malformed_returns_none() {
        // Non-array frame → None (coordinator collapses to canonical error).
        let bad = Frame::Integer(42);
        assert!(decode_shard_hybrid_partial(&bad).is_none());

        // Wrong-shape array: missing stream counts.
        let bad = Frame::Array(FrameVec::from(vec![Frame::Integer(1)]));
        assert!(decode_shard_hybrid_partial(&bad).is_none());

        // Truncated: claims 1 bm25 hit but only provides count and dist.
        let bad = Frame::Array(FrameVec::from(vec![
            Frame::Integer(1),
            Frame::BulkString(Bytes::from_static(b"0.5")),
            // missing id, kh, key → decode must return None
        ]));
        assert!(decode_shard_hybrid_partial(&bad).is_none());
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_execute_hybrid_search_local_raw_streams_unknown_index() {
        let mut vs = VectorStore::new();
        let ts = TextStore::new();
        let empty_df = std::collections::HashMap::new();
        let result = execute_hybrid_search_local_raw_streams(
            &mut vs,
            &ts,
            &Bytes::from_static(b"nosuch"),
            &[],
            &Bytes::from_static(b"vec"),
            &Bytes::from_static(b"\x00\x00\x00\x00"),
            None,
            [1.0, 1.0, 1.0],
            60,
            10,
            &empty_df,
            0,
        );
        match result {
            Frame::Error(msg) => {
                let s = std::str::from_utf8(&msg).unwrap();
                assert!(s.contains("unknown index"), "got: {s}");
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }
}
