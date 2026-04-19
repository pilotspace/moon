//! Coordinator-side scatter-gather for hybrid FT.SEARCH (Phase 152, Plan 05).
//!
//! Three-phase strategy per RESEARCH Pitfall 5 (choice A):
//!
//!   Phase 1: DocFreq pre-pass — reuse Plan 150's DFS machinery
//!            (`ShardMessage::DocFreq` + `aggregate_doc_freq`). Each shard
//!            reports per-term document frequency + local N; the coordinator
//!            sums them into `(global_df, global_n)`.
//!
//!   Phase 2: fan out `ShardMessage::FtHybrid` to every shard with the
//!            injected global IDF so BM25 scoring is multi-shard correct.
//!            Each shard returns THREE raw per-stream lists (bm25, dense,
//!            sparse) UNFUSED.
//!
//!   Phase 3: coordinator unions each stream across shards, then calls
//!            `rrf_fuse_three` exactly once on the unions (per D-13 + D-14).
//!
//! Per **D-13** ("Coordinator re-fuses across shards with global RRF on the
//! union") this preserves the single-shard RRF math invariant: whichever
//! shard count produces the same top-K ordering as single-shard for the
//! same dataset.
//!
//! Per **W6** (key preservation) each shard embeds its authoritative
//! `doc_id_to_key` / `key_hash_to_key` resolution into the wire format, so
//! the coordinator response uses shard-resolved keys directly — no synthetic
//! `vec:<id>` fallback at the coordinator level.

#![cfg(feature = "text-index")]

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use bytes::Bytes;
use ringbuf::HeapProd;

use crate::command::vector_search::hybrid::HybridQuery;
use crate::command::vector_search::hybrid_multi::{ShardHybridReply, decode_shard_hybrid_partial};
use crate::protocol::{Frame, FrameVec};
use crate::runtime::channel;
use crate::shard::coordinator::{aggregate_doc_freq, spsc_send};
use crate::shard::dispatch::{FtHybridPayload, ShardMessage};
use crate::shard::shared_databases::ShardDatabases;
use crate::vector::fusion::rrf_fuse_three;
use crate::vector::types::SearchResult;

/// Entry point for multi-shard FT.SEARCH HYBRID.
///
/// For `num_shards == 1`, takes the single-shard fast path directly through
/// `execute_hybrid_search_local` — no DFS, no SPSC fan-out. This is correct
/// because local IDF is globally accurate when there is exactly one shard.
///
/// For `num_shards > 1`, the three-phase scatter runs DFS → raw-streams
/// fan-out → coordinator RRF merge. All guards are dropped before any
/// `.await` point (RESEARCH Pitfall 2).
#[allow(clippy::too_many_arguments)]
pub async fn scatter_hybrid_search(
    query: HybridQuery,
    my_shard: usize,
    num_shards: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
) -> Frame {
    let top_k = query.top_k;
    let k_per_stream = query.effective_k_per_stream();

    // ─── Single-shard fast path ────────────────────────────────────────────
    if num_shards == 1 {
        // Local IDF is globally accurate — skip DFS, go straight through
        // the fused local path (identical to the non-HYBRID-routing code).
        return {
            let mut vs = shard_databases.vector_store(my_shard);
            let ts = shard_databases.text_store(my_shard);
            // v0.1.9 HYB-02 partial: single-shard path threads as_of_lsn=0 here;
            // the FT.SEARCH dispatcher at ft_search/dispatch.rs already routes
            // HYBRID through the direct call with the resolved LSN. This arm is
            // a fallback for multi-shard coordinators that chose the single-
            // shard fast path. Full multi-shard AS_OF scatter ships under
            // SCAT-01 (ShardMessage::VectorSearch LSN threading).
            crate::command::vector_search::hybrid::execute_hybrid_search_local(&mut vs, &ts, &query, 0)
            // guards drop at end of this block
        };
    }

    // ─── Parse text query into QueryTerms using the index's own analyzer ───
    // Block scope drops the MutexGuard before any `.await` below (Pitfall 2).
    let text_bytes: &[u8] = query.text_query.as_ref();
    type ParseOutcome = Result<
        (
            Vec<crate::command::vector_search::ft_text_search::QueryTerm>,
            Vec<String>,
        ),
        Frame,
    >;
    let parse_outcome: ParseOutcome = {
        let ts = shard_databases.text_store(my_shard);
        match ts.get_index(query.index_name.as_ref()) {
            None => Err(Frame::Error(Bytes::from_static(b"ERR unknown index"))),
            Some(text_index) => match text_index.field_analyzers.first() {
                None => Err(Frame::Error(Bytes::from_static(
                    b"ERR index has no TEXT fields",
                ))),
                Some(analyzer) => {
                    match crate::command::vector_search::parse_text_query(text_bytes, analyzer) {
                        Ok(clause) => {
                            let strings: Vec<String> =
                                clause.terms.iter().map(|qt| qt.text.clone()).collect();
                            Ok((clause.terms, strings))
                        }
                        Err(e) => {
                            let mut msg = b"ERR ".to_vec();
                            msg.extend_from_slice(e.as_bytes());
                            Err(Frame::Error(Bytes::from(msg)))
                        }
                    }
                }
            },
        }
    }; // MutexGuard dropped here — no .await held
    let (query_terms, term_strings) = match parse_outcome {
        Ok(v) => v,
        Err(f) => return f,
    };

    // ─── Phase 1: DocFreq scatter (reuses Plan 150 machinery) ──────────────
    let field_queries: Vec<(Option<usize>, Vec<String>)> = vec![(None, term_strings.clone())];
    let mut dfs_receivers: Vec<channel::OneshotReceiver<Frame>> =
        Vec::with_capacity(num_shards.saturating_sub(1));
    let mut local_dfs: Option<Frame> = None;

    for shard_id in 0..num_shards {
        if shard_id == my_shard {
            // Local DFS — direct read, no SPSC overhead. Drop guard before .await.
            let response = {
                let ts = shard_databases.text_store(shard_id);
                match ts.get_index(query.index_name.as_ref()) {
                    Some(text_index) => {
                        let mut items: Vec<Frame> = Vec::new();
                        for (field_idx_opt, terms) in &field_queries {
                            let fidx = field_idx_opt.unwrap_or(0);
                            let (term_dfs, n) = text_index.doc_freq_for_terms(fidx, terms);
                            for (term, df) in term_dfs {
                                items.push(Frame::BulkString(Bytes::from(term)));
                                items.push(Frame::Integer(i64::from(df)));
                            }
                            items.push(Frame::BulkString(Bytes::from_static(b"N")));
                            items.push(Frame::Integer(i64::from(n)));
                        }
                        Frame::Array(FrameVec::from(items))
                    }
                    None => Frame::Error(Bytes::from_static(b"ERR unknown index")),
                }
            }; // text_guard drops here
            local_dfs = Some(response);
        } else {
            let (reply_tx, reply_rx) = channel::oneshot();
            let msg = ShardMessage::DocFreq {
                index_name: query.index_name.clone(),
                field_queries: field_queries.clone(),
                reply_tx,
            };
            spsc_send(dispatch_tx, my_shard, shard_id, msg, spsc_notifiers).await;
            dfs_receivers.push(reply_rx);
        }
    }

    let mut dfs_responses: Vec<Frame> = Vec::with_capacity(num_shards);
    if let Some(local) = local_dfs {
        dfs_responses.push(local);
    }
    for rx in dfs_receivers {
        match rx.recv().await {
            Ok(frame) => dfs_responses.push(frame),
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR hybrid DFS phase channel closed unexpectedly",
                ));
            }
        }
    }
    let (global_df, global_n) = aggregate_doc_freq(&dfs_responses);

    // ─── Phase 2: fan out hybrid execution (three raw streams per shard) ───
    let sparse_field = query.sparse.as_ref().map(|(f, _)| f.clone());
    let sparse_blob = query.sparse.as_ref().map(|(_, b)| b.clone());
    let mut hyb_receivers: Vec<channel::OneshotReceiver<Frame>> =
        Vec::with_capacity(num_shards.saturating_sub(1));
    let mut local_hyb: Option<Frame> = None;

    for shard_id in 0..num_shards {
        if shard_id == my_shard {
            let response = {
                let mut vs = shard_databases.vector_store(shard_id);
                let ts = shard_databases.text_store(shard_id);
                let sparse_pair = match (sparse_field.as_ref(), sparse_blob.as_ref()) {
                    (Some(f), Some(b)) => Some((f, b)),
                    _ => None,
                };
                crate::command::vector_search::hybrid_multi::execute_hybrid_search_local_raw_streams(
                    &mut vs,
                    &ts,
                    &query.index_name,
                    &query_terms,
                    &query.dense_field,
                    &query.dense_blob,
                    sparse_pair,
                    query.weights,
                    k_per_stream,
                    top_k,
                    &global_df,
                    global_n,
                )
                // guards drop here
            };
            local_hyb = Some(response);
        } else {
            let (reply_tx, reply_rx) = channel::oneshot();
            let payload = FtHybridPayload {
                index_name: query.index_name.clone(),
                query_terms: query_terms.clone(),
                dense_field: query.dense_field.clone(),
                dense_blob: query.dense_blob.clone(),
                sparse_field: sparse_field.clone(),
                sparse_blob: sparse_blob.clone(),
                weights: query.weights,
                k_per_stream,
                top_k,
                global_df: global_df.clone(),
                global_n,
                reply_tx,
            };
            let msg = ShardMessage::FtHybrid(Box::new(payload));
            spsc_send(dispatch_tx, my_shard, shard_id, msg, spsc_notifiers).await;
            hyb_receivers.push(reply_rx);
        }
    }

    // ─── Phase 3a: collect per-shard raw three-stream replies ──────────────
    let mut replies: Vec<ShardHybridReply> = Vec::with_capacity(num_shards);
    if let Some(frame) = local_hyb {
        match decode_or_error(&frame) {
            Ok(r) => replies.push(r),
            Err(err_frame) => return err_frame,
        }
    }
    for rx in hyb_receivers {
        match rx.recv().await {
            Ok(frame) => match decode_or_error(&frame) {
                Ok(r) => replies.push(r),
                Err(err_frame) => return err_frame,
            },
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR hybrid fan-out channel closed unexpectedly",
                ));
            }
        }
    }

    // ─── Phase 3b: build per-stream unions + key_hash→Bytes resolver ───────
    // Union semantics: concat + sort by distance ascending.
    //   - BM25: bm25_to_search_results sets `distance = -score`, so ascending
    //     distance = descending BM25 score = best first.
    //   - Dense: distance is L2 / cosine distance, ascending = best first.
    //   - Sparse: same distance-asc-is-better convention.
    // `rrf_fuse_three` uses the index in the sorted Vec as the rank for the
    // RRF formula, matching exactly what the single-shard path does.
    let mut bm25_union: Vec<SearchResult> = Vec::new();
    let mut dense_union: Vec<SearchResult> = Vec::new();
    let mut sparse_union: Vec<SearchResult> = Vec::new();
    let mut key_resolver: std::collections::HashMap<u64, Bytes> = std::collections::HashMap::new();

    for reply in replies {
        for kr in reply.bm25 {
            key_resolver
                .entry(kr.result.key_hash)
                .or_insert_with(|| kr.key.clone());
            bm25_union.push(kr.result);
        }
        for kr in reply.dense {
            key_resolver
                .entry(kr.result.key_hash)
                .or_insert_with(|| kr.key.clone());
            dense_union.push(kr.result);
        }
        for kr in reply.sparse {
            key_resolver
                .entry(kr.result.key_hash)
                .or_insert_with(|| kr.key.clone());
            sparse_union.push(kr.result);
        }
    }

    bm25_union.sort_unstable_by(|a, b| {
        a.distance
            .partial_cmp(&b.distance)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    dense_union.sort_unstable_by(|a, b| {
        a.distance
            .partial_cmp(&b.distance)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    sparse_union.sort_unstable_by(|a, b| {
        a.distance
            .partial_cmp(&b.distance)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // ─── Phase 3c: single rrf_fuse_three call on unioned streams (D-13+14) ─
    let (fused, bm25_count, dense_count, sparse_count) = rrf_fuse_three(
        &bm25_union,
        &dense_union,
        &sparse_union,
        query.weights,
        top_k,
    );

    // ─── Response build: keys come from shard-preserved Bytes (W6) ─────────
    build_hybrid_merge_response(
        &fused,
        &key_resolver,
        bm25_count,
        dense_count,
        sparse_count,
        query.offset,
        query.count,
    )
}

/// Decode a shard's phase-2 reply, preserving Frame::Error and converting
/// malformed arrays into a canonical error (no panics — threat T-152-05-02).
fn decode_or_error(frame: &Frame) -> Result<ShardHybridReply, Frame> {
    match frame {
        Frame::Error(_) => Err(frame.clone()),
        _ => decode_shard_hybrid_partial(frame).ok_or_else(|| {
            Frame::Error(Bytes::from_static(
                b"ERR hybrid shard returned malformed result list",
            ))
        }),
    }
}

/// Build the final FT.SEARCH response using shard-preserved keys.
///
/// Response shape mirrors `build_hybrid_local_response`:
/// `[total, key1, [__rrf_score, "N"], key2, [...], ..., bm25_hits, N, dense_hits, N, sparse_hits, N]`
///
/// W6: keys come from the shard-preserved `key_resolver` map — NO synthetic
/// fallback is used here. If a key_hash cannot be resolved (shouldn't happen
/// post-W6), an empty Bytes is emitted so the response stays well-formed.
fn build_hybrid_merge_response(
    fused: &[SearchResult],
    key_resolver: &std::collections::HashMap<u64, Bytes>,
    bm25_count: usize,
    dense_count: usize,
    sparse_count: usize,
    offset: usize,
    count: usize,
) -> Frame {
    let total = fused.len() as i64;
    let page: Vec<&SearchResult> = fused.iter().skip(offset).take(count).collect();
    let mut items: Vec<Frame> = Vec::with_capacity(1 + page.len() * 2 + 6);
    items.push(Frame::Integer(total));
    for r in &page {
        let key = match key_resolver.get(&r.key_hash) {
            Some(k) => k.clone(),
            None => Bytes::from_static(b""),
        };
        items.push(Frame::BulkString(key));
        let rrf_score = -r.distance;
        let mut score_buf = String::with_capacity(16);
        {
            use std::fmt::Write;
            let _ = write!(score_buf, "{}", rrf_score);
        }
        let fields: FrameVec = vec![
            Frame::BulkString(Bytes::from_static(b"__rrf_score")),
            Frame::BulkString(Bytes::from(score_buf)),
        ]
        .into();
        items.push(Frame::Array(fields));
    }
    items.push(Frame::BulkString(Bytes::from_static(b"bm25_hits")));
    items.push(Frame::Integer(bm25_count as i64));
    items.push(Frame::BulkString(Bytes::from_static(b"dense_hits")));
    items.push(Frame::Integer(dense_count as i64));
    items.push(Frame::BulkString(Bytes::from_static(b"sparse_hits")));
    items.push(Frame::Integer(sparse_count as i64));
    Frame::Array(FrameVec::from(items))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::vector_search::hybrid_multi::{
        KeyedResult, ShardHybridReply, encode_shard_hybrid_partial,
    };
    use crate::vector::types::VectorId;

    fn tsr(distance: f32, id: u32, key_hash: u64) -> SearchResult {
        SearchResult {
            distance,
            id: VectorId(id),
            key_hash,
        }
    }

    /// Build a shard reply with given (distance, id, key_hash, key_bytes) tuples
    /// per stream.
    fn mk_reply(
        bm25: Vec<(f32, u32, u64, &[u8])>,
        dense: Vec<(f32, u32, u64, &[u8])>,
        sparse: Vec<(f32, u32, u64, &[u8])>,
    ) -> ShardHybridReply {
        let to_kr = |(d, i, kh, k): (f32, u32, u64, &[u8])| KeyedResult {
            result: tsr(d, i, kh),
            key: Bytes::copy_from_slice(k),
        };
        ShardHybridReply {
            bm25: bm25.into_iter().map(to_kr).collect(),
            dense: dense.into_iter().map(to_kr).collect(),
            sparse: sparse.into_iter().map(to_kr).collect(),
        }
    }

    /// Drive the Phase 3 merge path directly — coordinator unions three streams,
    /// calls rrf_fuse_three exactly once, builds the response using shard keys.
    ///
    /// This is the invariant test for D-13 (union-then-fuse) + W6 (shard keys
    /// in response, no `vec:<id>` fallback).
    #[test]
    fn test_scatter_phase3_unions_three_streams_and_calls_rrf_once() {
        // Two shards. Doc A (kh=100) on shard 0 wins BM25; doc B (kh=200) on
        // shard 1 wins dense. Both docs appear in multiple streams → RRF
        // should boost them above stream-only docs (kh=300, kh=400).
        let r0 = mk_reply(
            vec![(-10.0, 1, 100, b"doc:a"), (-5.0, 2, 300, b"doc:c")],
            vec![(0.5, 3, 100, b"doc:a")],
            vec![(0.3, 4, 100, b"doc:a")],
        );
        let r1 = mk_reply(
            vec![(-3.0, 5, 200, b"doc:b")],
            vec![(0.1, 6, 200, b"doc:b"), (0.4, 7, 400, b"doc:d")],
            vec![(0.2, 8, 200, b"doc:b")],
        );

        let replies = vec![r0, r1];

        // Union + sort (same logic as production code).
        let mut bm25_union: Vec<SearchResult> = Vec::new();
        let mut dense_union: Vec<SearchResult> = Vec::new();
        let mut sparse_union: Vec<SearchResult> = Vec::new();
        let mut key_resolver: std::collections::HashMap<u64, Bytes> =
            std::collections::HashMap::new();

        for reply in replies {
            for kr in reply.bm25 {
                key_resolver
                    .entry(kr.result.key_hash)
                    .or_insert_with(|| kr.key.clone());
                bm25_union.push(kr.result);
            }
            for kr in reply.dense {
                key_resolver
                    .entry(kr.result.key_hash)
                    .or_insert_with(|| kr.key.clone());
                dense_union.push(kr.result);
            }
            for kr in reply.sparse {
                key_resolver
                    .entry(kr.result.key_hash)
                    .or_insert_with(|| kr.key.clone());
                sparse_union.push(kr.result);
            }
        }
        bm25_union.sort_unstable_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        dense_union.sort_unstable_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        sparse_union.sort_unstable_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let (fused, bc, dc, sc) = rrf_fuse_three(
            &bm25_union,
            &dense_union,
            &sparse_union,
            [1.0, 1.0, 1.0],
            10,
        );

        // Counts reflect raw stream sizes before fusion.
        // r0.bm25=2, r1.bm25=1 → 3. r0.dense=1, r1.dense=2 → 3. r0.sparse=1, r1.sparse=1 → 2.
        assert_eq!(bc, 3, "bm25 union size");
        assert_eq!(dc, 3, "dense union size");
        assert_eq!(sc, 2, "sparse union size");

        // Doc A (kh=100) and Doc B (kh=200) each appear in all 3 streams,
        // so they dominate the fused top-2. The exact order depends on ranks
        // within each stream (RRF: rank_0 contributes 1/61, rank_1 1/62, etc).
        // Both must appear in the top-2 (stream-only singletons rank lower).
        assert!(fused.len() >= 2);
        let top2: std::collections::HashSet<u64> =
            fused.iter().take(2).map(|r| r.key_hash).collect();
        assert!(
            top2.contains(&100) && top2.contains(&200),
            "both triple-stream docs (kh=100, kh=200) must rank in top-2, got {top2:?}"
        );

        // Response build uses shard-preserved keys — NO synthetic vec:<id>.
        let frame = build_hybrid_merge_response(&fused, &key_resolver, bc, dc, sc, 0, 10);
        if let Frame::Array(items) = &frame {
            // items[0] = total. items[1] and items[3] are the first two keys.
            // They must be doc:a or doc:b (shard-preserved) — never vec:<id>.
            let keys: std::collections::HashSet<&[u8]> = [1usize, 3]
                .iter()
                .filter_map(|&i| match items.get(i) {
                    Some(Frame::BulkString(k)) => Some(k.as_ref()),
                    _ => None,
                })
                .collect();
            assert!(
                keys.contains(&b"doc:a"[..]) && keys.contains(&b"doc:b"[..]),
                "shard-preserved keys must be returned verbatim, got {keys:?}"
            );
            for k in &keys {
                assert!(
                    !std::str::from_utf8(k).unwrap_or("").starts_with("vec:"),
                    "must NOT synthesize vec:<id> fallback"
                );
            }
        } else {
            panic!("expected Array response");
        }
    }

    /// Guard: coordinator propagates shard errors verbatim.
    #[test]
    fn test_decode_or_error_preserves_error_frame() {
        let err = Frame::Error(Bytes::from_static(b"ERR unknown index"));
        match decode_or_error(&err) {
            Err(Frame::Error(msg)) => assert_eq!(&msg[..], b"ERR unknown index"),
            other => panic!("expected Error pass-through, got {other:?}"),
        }
    }

    #[test]
    fn test_decode_or_error_malformed_yields_canonical_error() {
        let bad = Frame::Integer(7);
        match decode_or_error(&bad) {
            Err(Frame::Error(msg)) => {
                let s = std::str::from_utf8(&msg).unwrap();
                assert!(s.contains("malformed"), "got: {s}");
            }
            other => panic!("expected malformed error, got {other:?}"),
        }
    }

    /// Response build: keys always come from `key_resolver`, never synthesized.
    #[test]
    fn test_build_hybrid_merge_response_uses_shard_keys_only() {
        let mut resolver = std::collections::HashMap::new();
        resolver.insert(100u64, Bytes::from_static(b"doc:alpha"));
        resolver.insert(200u64, Bytes::from_static(b"doc:beta"));
        let fused = vec![tsr(-0.05, 1, 100), tsr(-0.03, 2, 200)];
        let frame = build_hybrid_merge_response(&fused, &resolver, 3, 3, 3, 0, 10);
        if let Frame::Array(items) = frame {
            assert_eq!(
                items[1],
                Frame::BulkString(Bytes::from_static(b"doc:alpha"))
            );
            assert_eq!(items[3], Frame::BulkString(Bytes::from_static(b"doc:beta")));
        } else {
            panic!("expected Array");
        }
    }

    /// W6 guard: missing key_hash in resolver falls back to empty Bytes,
    /// NOT to synthetic `vec:<id>`. The response stays well-formed.
    #[test]
    fn test_build_hybrid_merge_response_missing_key_no_synthetic_fallback() {
        let resolver = std::collections::HashMap::new(); // intentionally empty
        let fused = vec![tsr(-0.05, 77, 999)];
        let frame = build_hybrid_merge_response(&fused, &resolver, 1, 0, 0, 0, 10);
        if let Frame::Array(items) = frame {
            // items[1] is the key; must be empty Bytes, not "vec:77".
            match &items[1] {
                Frame::BulkString(k) => {
                    assert_eq!(k.as_ref(), b"", "must NOT synthesize vec:<id>");
                    assert!(
                        !std::str::from_utf8(k).unwrap_or("").starts_with("vec:"),
                        "must NOT use vec:<id> fallback"
                    );
                }
                _ => panic!("expected BulkString"),
            }
        } else {
            panic!("expected Array");
        }
    }

    /// Round-trip: Frame codec preserves every (distance, id, key_hash, key)
    /// tuple so the coordinator decoder reconstructs the reply identically.
    #[test]
    fn test_shard_reply_frame_roundtrip_preserves_order() {
        use crate::text::store::TextIndex;
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
            .insert(1, Bytes::from_static(b"doc:first"));
        text_index
            .doc_id_to_key
            .insert(2, Bytes::from_static(b"doc:second"));

        let mut vec_map = std::collections::HashMap::new();
        vec_map.insert(300u64, Bytes::from_static(b"doc:dense-only"));
        vec_map.insert(400u64, Bytes::from_static(b"doc:sparse-only"));

        let bm25 = vec![tsr(-9.0, 1, 100), tsr(-3.0, 2, 200)];
        let dense = vec![tsr(0.01, 3, 300)];
        let sparse = vec![tsr(0.02, 4, 400)];

        let frame = encode_shard_hybrid_partial(&bm25, &dense, &sparse, &text_index, &vec_map);
        let decoded = decode_shard_hybrid_partial(&frame).expect("decode");

        assert_eq!(decoded.bm25.len(), 2);
        assert_eq!(decoded.bm25[0].key.as_ref(), b"doc:first");
        assert_eq!(decoded.bm25[1].key.as_ref(), b"doc:second");
        assert_eq!(decoded.dense[0].key.as_ref(), b"doc:dense-only");
        assert_eq!(decoded.sparse[0].key.as_ref(), b"doc:sparse-only");
    }
}
