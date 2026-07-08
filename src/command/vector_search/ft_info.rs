//! FT.INFO command handler — returns index metadata.

use bytes::Bytes;

use crate::protocol::Frame;
use crate::vector::store::VectorStore;

use super::{extract_bulk, helpers::metric_to_bytes, helpers::quantization_to_bytes};

/// FT.INFO index_name
///
/// Returns an array of key-value pairs describing the index.
/// Includes backward-compatible top-level fields (from default field) plus
/// a `vector_fields` nested array with per-field stats.
pub fn ft_info(
    store: &VectorStore,
    text_store: &crate::text::store::TextStore,
    args: &[Frame],
    db_index: u8,
) -> Frame {
    if args.len() != 1 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'FT.INFO' command",
        ));
    }
    let name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid index name")),
    };
    let idx = match store.get_index_for_db(&name, db_index) {
        Some(i) => i,
        None => {
            // Check TextStore for TEXT-only indexes (db-scoped: WS5a)
            if let Some(text_idx) = text_store.get_index_for_db(&name, db_index) {
                return ft_info_text_only(text_idx, text_store);
            }
            return Frame::Error(Bytes::from_static(b"Unknown Index name"));
        }
    };

    // Count default field docs across mutable + immutable + WARM segments.
    // WARM (WS3 idle-unload / age-based) segments must contribute too --
    // otherwise num_docs silently drops to 0 for an index whose only
    // segment demoted to the mmap-backed tier, even though it is still
    // fully searchable (see FT.INFO doc comment above re: summing across
    // segments).
    let snap = idx.segments.load();
    let mut num_docs = snap.mutable.len();
    for imm in snap.immutable.iter() {
        num_docs += imm.live_count() as usize;
    }
    for warm in snap.warm.iter() {
        num_docs += warm.total_count() as usize;
    }
    // HQ-1 observability (persistence-review R5): exact-rerank coverage.
    // A segment without the f16 sidecar silently answers with quantized ADC
    // distances only — surfacing the count makes a dropped sidecar (e.g. an
    // all-or-nothing GraphUnion merge with one sidecar-less source) visible.
    let graph_segments = snap.immutable.len();
    let segments_with_exact_rerank = snap
        .immutable
        .iter()
        .filter(|imm| imm.raw_f16().is_some())
        .count();
    // WS3 idle-unload observability: HOT (graph_segments, above) vs WARM
    // (mmap-backed, unloaded from the full in-memory HNSW+TQ structures once
    // idle/aged past the configured thresholds). `warm_segments_with_exact_rerank`
    // mirrors `segments_with_exact_rerank`'s "coverage < total ⇒ some segments
    // answer ADC-only" signal for the warm tier.
    let warm_segments = snap.warm.len();
    let warm_segments_with_exact_rerank =
        snap.warm.iter().filter(|w| w.raw_f16().is_some()).count();

    // Use itoa for numeric formatting -- no format!() on hot path.
    let ef_rt_bytes: Bytes = if idx.meta.hnsw_ef_runtime > 0 {
        let mut buf = itoa::Buffer::new();
        Bytes::copy_from_slice(buf.format(idx.meta.hnsw_ef_runtime).as_bytes())
    } else {
        Bytes::from_static(b"auto")
    };
    let ct_bytes: Bytes = if idx.meta.compact_threshold > 0 {
        let mut buf = itoa::Buffer::new();
        Bytes::copy_from_slice(buf.format(idx.meta.compact_threshold).as_bytes())
    } else {
        Bytes::from_static(b"1000")
    };
    let quant_bytes: Bytes = quantization_to_bytes(idx.meta.quantization);

    // Backward-compatible top-level fields (from default field)
    let mut items = vec![
        Frame::BulkString(Bytes::from_static(b"index_name")),
        Frame::BulkString(idx.meta.name.clone()),
        Frame::BulkString(Bytes::from_static(b"index_definition")),
        Frame::Array(
            vec![
                Frame::BulkString(Bytes::from_static(b"key_type")),
                Frame::BulkString(Bytes::from_static(b"HASH")),
            ]
            .into(),
        ),
        Frame::BulkString(Bytes::from_static(b"num_docs")),
        Frame::Integer(num_docs as i64),
        Frame::BulkString(Bytes::from_static(b"dimension")),
        Frame::Integer(idx.meta.dimension as i64),
        Frame::BulkString(Bytes::from_static(b"distance_metric")),
        Frame::BulkString(metric_to_bytes(idx.meta.metric)),
        Frame::BulkString(Bytes::from_static(b"M")),
        Frame::Integer(idx.meta.hnsw_m as i64),
        Frame::BulkString(Bytes::from_static(b"EF_CONSTRUCTION")),
        Frame::Integer(idx.meta.hnsw_ef_construction as i64),
        Frame::BulkString(Bytes::from_static(b"EF_RUNTIME")),
        Frame::BulkString(ef_rt_bytes),
        Frame::BulkString(Bytes::from_static(b"COMPACT_THRESHOLD")),
        Frame::BulkString(ct_bytes),
        Frame::BulkString(Bytes::from_static(b"QUANTIZATION")),
        Frame::BulkString(quant_bytes),
        Frame::BulkString(Bytes::from_static(b"graph_segments")),
        Frame::Integer(graph_segments as i64),
        Frame::BulkString(Bytes::from_static(b"segments_with_exact_rerank")),
        Frame::Integer(segments_with_exact_rerank as i64),
        Frame::BulkString(Bytes::from_static(b"warm_segments")),
        Frame::Integer(warm_segments as i64),
        Frame::BulkString(Bytes::from_static(b"warm_segments_with_exact_rerank")),
        Frame::Integer(warm_segments_with_exact_rerank as i64),
    ];

    // Per-field stats: vector_fields array
    let mut field_entries: Vec<Frame> = Vec::with_capacity(idx.meta.vector_fields.len());
    for (i, field_meta) in idx.meta.vector_fields.iter().enumerate() {
        let (field_num_docs, field_mutable, field_immutable_count) = if i == 0 {
            // Default field: use top-level segments
            let s = idx.segments.load();
            let mut docs = s.mutable.len();
            let imm_count = s.immutable.len();
            for imm in s.immutable.iter() {
                docs += imm.live_count() as usize;
            }
            for warm in s.warm.iter() {
                docs += warm.total_count() as usize;
            }
            (docs, s.mutable.len(), imm_count)
        } else if let Some(fs) = idx.field_segments.get(&field_meta.field_name) {
            let s = fs.segments.load();
            let mut docs = s.mutable.len();
            let imm_count = s.immutable.len();
            for imm in s.immutable.iter() {
                docs += imm.live_count() as usize;
            }
            for warm in s.warm.iter() {
                docs += warm.total_count() as usize;
            }
            (docs, s.mutable.len(), imm_count)
        } else {
            (0, 0, 0)
        };

        let field_quant = quantization_to_bytes(field_meta.quantization);

        let entry = vec![
            Frame::BulkString(Bytes::from_static(b"field_name")),
            Frame::BulkString(field_meta.field_name.clone()),
            Frame::BulkString(Bytes::from_static(b"dimension")),
            Frame::Integer(field_meta.dimension as i64),
            Frame::BulkString(Bytes::from_static(b"distance_metric")),
            Frame::BulkString(metric_to_bytes(field_meta.metric)),
            Frame::BulkString(Bytes::from_static(b"num_docs")),
            Frame::Integer(field_num_docs as i64),
            Frame::BulkString(Bytes::from_static(b"QUANTIZATION")),
            Frame::BulkString(field_quant),
            Frame::BulkString(Bytes::from_static(b"mutable_vectors")),
            Frame::Integer(field_mutable as i64),
            Frame::BulkString(Bytes::from_static(b"immutable_segments")),
            Frame::Integer(field_immutable_count as i64),
        ];
        field_entries.push(Frame::Array(entry.into()));
    }

    items.push(Frame::BulkString(Bytes::from_static(b"vector_fields")));
    items.push(Frame::Array(field_entries.into()));

    // Monotonic freshness counter for this shard's VSEARCH engine.
    // Starts at 0 on boot; NOT restored from WAL (freshness hint only).
    // Bumped after every successful vector insert, create_index, drop_index,
    // or mark_deleted_for_key. Use to detect stale query-cache entries.
    items.push(Frame::BulkString(Bytes::from_static(
        b"vector_version_token",
    )));
    items.push(Frame::Integer(store.version_token() as i64));

    // Hybrid index: append text field stats if this index also has a TextIndex
    // (db-scoped: WS5a — same name in a different db must not leak in here).
    if let Some(text_idx) = text_store.get_index_for_db(&name, db_index) {
        let mut text_field_entries: Vec<Frame> = Vec::with_capacity(text_idx.text_fields.len());
        for (i, field_def) in text_idx.text_fields.iter().enumerate() {
            let stats = &text_idx.field_stats[i];
            // NOTE: format!() acceptable — FT.INFO is diagnostic, not hot path
            let mut weight_buf = String::with_capacity(8);
            let mut avg_buf = String::with_capacity(8);
            {
                use std::fmt::Write;
                let _ = write!(weight_buf, "{:.1}", field_def.weight);
                let _ = write!(avg_buf, "{:.2}", stats.avg_doc_len());
            }
            text_field_entries.push(Frame::Array(
                vec![
                    Frame::BulkString(Bytes::from_static(b"field_name")),
                    Frame::BulkString(field_def.field_name.clone()),
                    Frame::BulkString(Bytes::from_static(b"weight")),
                    Frame::BulkString(Bytes::from(weight_buf)),
                    Frame::BulkString(Bytes::from_static(b"num_docs")),
                    Frame::Integer(stats.num_docs as i64),
                    Frame::BulkString(Bytes::from_static(b"avg_doc_len")),
                    Frame::BulkString(Bytes::from(avg_buf)),
                ]
                .into(),
            ));
        }
        items.push(Frame::BulkString(Bytes::from_static(b"text_fields")));
        items.push(Frame::Array(text_field_entries.into()));
        items.push(Frame::BulkString(Bytes::from_static(b"num_terms")));
        items.push(Frame::Integer(text_idx.num_terms() as i64));
        items.push(Frame::BulkString(Bytes::from_static(
            b"total_inverted_index_size",
        )));
        items.push(Frame::Integer(text_idx.total_posting_bytes() as i64));
        // Monotonic freshness counter for this shard's FT text engine.
        // Independent from vector_version_token — hybrid-index callers check both.
        items.push(Frame::BulkString(Bytes::from_static(b"text_version_token")));
        items.push(Frame::Integer(text_store.version_token() as i64));
    }

    Frame::Array(items.into())
}

/// Merge per-shard FT.INFO responses into one cluster-wide response
/// (XC-SHARD-1). Data is key-hash partitioned across shards, so document
/// counts are ADDITIVE; index configuration (dimension, metric, M, …) is
/// identical on every shard by FT.CREATE-broadcast construction and is taken
/// from the local response.
///
/// Additive top-level keys: `num_docs`, `num_terms`,
/// `total_inverted_index_size`, the exact-rerank coverage counters
/// `graph_segments` / `segments_with_exact_rerank` (R5), and the freshness
/// tokens `vector_version_token` / `text_version_token` (each shard's token
/// is monotonic, so the sum is monotonic too — any shard's write bumps the
/// aggregate).
/// Additive per-field keys (matched by `field_name` inside `vector_fields` /
/// `text_fields`): `num_docs`, `mutable_vectors`, `immutable_segments`.
///
/// Any `Frame::Error` (local or remote) is propagated unchanged (fail-loud,
/// same semantics as `scatter_invalidate_range`).
pub fn merge_ft_info_responses(local: Frame, remotes: &[Frame]) -> Frame {
    const ADDITIVE_TOP: &[&[u8]] = &[
        b"num_docs",
        b"num_terms",
        b"total_inverted_index_size",
        b"vector_version_token",
        b"text_version_token",
        b"graph_segments",
        b"segments_with_exact_rerank",
        b"warm_segments",
        b"warm_segments_with_exact_rerank",
    ];
    const ADDITIVE_FIELD: &[&[u8]] = &[b"num_docs", b"mutable_vectors", b"immutable_segments"];

    if matches!(local, Frame::Error(_)) {
        return local;
    }
    if let Some(err) = remotes.iter().find(|r| matches!(r, Frame::Error(_))) {
        return err.clone();
    }
    let mut items: Vec<Frame> = match &local {
        Frame::Array(a) => a.to_vec(),
        _ => return local,
    };

    // Value-index of `key` in an alternating key/value frame list.
    fn value_idx(items: &[Frame], key: &[u8]) -> Option<usize> {
        let mut i = 0;
        while i + 1 < items.len() {
            if let Frame::BulkString(k) = &items[i] {
                if k.as_ref() == key {
                    return Some(i + 1);
                }
            }
            i += 2;
        }
        None
    }

    fn int_at(items: &[Frame], idx: usize) -> Option<i64> {
        match items.get(idx) {
            Some(Frame::Integer(n)) => Some(*n),
            _ => None,
        }
    }

    // `field_name` value of one per-field entry array.
    fn entry_field_name(entry: &Frame) -> Option<Bytes> {
        if let Frame::Array(pairs) = entry {
            if let Some(vi) = value_idx(pairs, b"field_name") {
                if let Some(Frame::BulkString(name)) = pairs.get(vi) {
                    return Some(name.clone());
                }
            }
        }
        None
    }

    for remote in remotes {
        let r_items: &[Frame] = match remote {
            Frame::Array(a) => a,
            _ => continue,
        };
        for key in ADDITIVE_TOP {
            if let (Some(li), Some(ri)) = (value_idx(&items, key), value_idx(r_items, key)) {
                if let (Some(lv), Some(rv)) = (int_at(&items, li), int_at(r_items, ri)) {
                    items[li] = Frame::Integer(lv.saturating_add(rv));
                }
            }
        }
        for list_key in [b"vector_fields".as_slice(), b"text_fields".as_slice()] {
            let (Some(li), Some(ri)) = (value_idx(&items, list_key), value_idx(r_items, list_key))
            else {
                continue;
            };
            let remote_entries: Vec<Frame> = match r_items.get(ri) {
                Some(Frame::Array(a)) => a.to_vec(),
                _ => continue,
            };
            let Some(Frame::Array(local_entries)) = items.get(li) else {
                continue;
            };
            let mut local_entries: Vec<Frame> = local_entries.to_vec();
            for le in local_entries.iter_mut() {
                let Some(name) = entry_field_name(le) else {
                    continue;
                };
                let Some(re) = remote_entries
                    .iter()
                    .find(|re| entry_field_name(re).as_deref() == Some(name.as_ref()))
                else {
                    continue;
                };
                let (Frame::Array(lp), Frame::Array(rp)) = (&*le, re) else {
                    continue;
                };
                let mut pairs: Vec<Frame> = lp.to_vec();
                for key in ADDITIVE_FIELD {
                    if let (Some(lpi), Some(rpi)) = (value_idx(&pairs, key), value_idx(rp, key)) {
                        if let (Some(lv), Some(rv)) = (int_at(&pairs, lpi), int_at(rp, rpi)) {
                            pairs[lpi] = Frame::Integer(lv.saturating_add(rv));
                        }
                    }
                }
                *le = Frame::Array(pairs.into());
            }
            items[li] = Frame::Array(local_entries.into());
        }
    }

    Frame::Array(items.into())
}

/// Full FT.INFO response for TEXT-only indexes.
///
/// Returns index_name, num_docs, num_terms, per-field stats (num_docs,
/// avg_doc_len, weight, nostem), BM25 config, memory estimates, and
/// `text_version_token` (monotonic freshness counter for the FT text engine).
fn ft_info_text_only(
    idx: &crate::text::store::TextIndex,
    text_store: &crate::text::store::TextStore,
) -> Frame {
    let mut items = vec![
        Frame::BulkString(Bytes::from_static(b"index_name")),
        Frame::BulkString(idx.name.clone()),
        Frame::BulkString(Bytes::from_static(b"index_definition")),
        Frame::Array(
            vec![
                Frame::BulkString(Bytes::from_static(b"key_type")),
                Frame::BulkString(Bytes::from_static(b"HASH")),
            ]
            .into(),
        ),
        Frame::BulkString(Bytes::from_static(b"num_docs")),
        Frame::Integer(idx.num_docs() as i64),
        Frame::BulkString(Bytes::from_static(b"num_terms")),
        Frame::Integer(idx.num_terms() as i64),
    ];

    // Per-field text stats (NOTE: format!() acceptable here — FT.INFO is diagnostic, not hot path)
    let mut field_entries: Vec<Frame> = Vec::with_capacity(idx.text_fields.len());
    for (i, tf) in idx.text_fields.iter().enumerate() {
        let stats = &idx.field_stats[i];
        let mut weight_buf = String::with_capacity(8);
        use std::fmt::Write;
        let _ = write!(weight_buf, "{:.1}", tf.weight);
        let mut avg_buf = String::with_capacity(8);
        let _ = write!(avg_buf, "{:.2}", stats.avg_doc_len());
        let entry = vec![
            Frame::BulkString(Bytes::from_static(b"field_name")),
            Frame::BulkString(tf.field_name.clone()),
            Frame::BulkString(Bytes::from_static(b"type")),
            Frame::BulkString(Bytes::from_static(b"TEXT")),
            Frame::BulkString(Bytes::from_static(b"WEIGHT")),
            Frame::BulkString(Bytes::from(weight_buf)),
            Frame::BulkString(Bytes::from_static(b"nostem")),
            Frame::BulkString(if tf.nostem {
                Bytes::from_static(b"true")
            } else {
                Bytes::from_static(b"false")
            }),
            Frame::BulkString(Bytes::from_static(b"num_docs")),
            Frame::Integer(stats.num_docs as i64),
            Frame::BulkString(Bytes::from_static(b"avg_doc_len")),
            Frame::BulkString(Bytes::from(avg_buf)),
        ];
        field_entries.push(Frame::Array(entry.into()));
    }
    items.push(Frame::BulkString(Bytes::from_static(b"text_fields")));
    items.push(Frame::Array(field_entries.into()));

    // BM25 config
    let mut k1_buf = String::with_capacity(8);
    let mut b_buf = String::with_capacity(8);
    {
        use std::fmt::Write;
        let _ = write!(k1_buf, "{:.1}", idx.bm25_config.k1);
        let _ = write!(b_buf, "{:.2}", idx.bm25_config.b);
    }
    items.push(Frame::BulkString(Bytes::from_static(b"bm25_k1")));
    items.push(Frame::BulkString(Bytes::from(k1_buf)));
    items.push(Frame::BulkString(Bytes::from_static(b"bm25_b")));
    items.push(Frame::BulkString(Bytes::from(b_buf)));

    // Memory estimates
    let total_postings = idx.total_posting_bytes();
    let num_docs_val = idx.num_docs() as usize;
    let bytes_per = if num_docs_val > 0 {
        total_postings / num_docs_val
    } else {
        0
    };
    items.push(Frame::BulkString(Bytes::from_static(b"bytes_per_posting")));
    items.push(Frame::Integer(bytes_per as i64));
    items.push(Frame::BulkString(Bytes::from_static(
        b"total_inverted_index_size",
    )));
    items.push(Frame::Integer(total_postings as i64));

    // Monotonic freshness counter for this shard's FT text engine.
    // Starts at 0 on boot; NOT restored from WAL (freshness hint only).
    // Bumped after every successful text document index, create_index, or
    // drop_index. Use to detect stale query-cache entries.
    items.push(Frame::BulkString(Bytes::from_static(b"text_version_token")));
    items.push(Frame::Integer(text_store.version_token() as i64));

    Frame::Array(items.into())
}

#[cfg(test)]
mod merge_tests {
    use super::*;

    fn bs(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    fn info_frame(num_docs: i64, field_docs: i64, mutable: i64, imms: i64) -> Frame {
        let field_entry = Frame::Array(
            vec![
                bs(b"field_name"),
                bs(b"vec"),
                bs(b"dimension"),
                Frame::Integer(8),
                bs(b"num_docs"),
                Frame::Integer(field_docs),
                bs(b"mutable_vectors"),
                Frame::Integer(mutable),
                bs(b"immutable_segments"),
                Frame::Integer(imms),
            ]
            .into(),
        );
        Frame::Array(
            vec![
                bs(b"index_name"),
                bs(b"idx"),
                bs(b"num_docs"),
                Frame::Integer(num_docs),
                bs(b"dimension"),
                Frame::Integer(8),
                bs(b"vector_fields"),
                Frame::Array(vec![field_entry].into()),
                bs(b"vector_version_token"),
                Frame::Integer(7),
            ]
            .into(),
        )
    }

    fn get_int(frame: &Frame, key: &[u8]) -> i64 {
        let Frame::Array(items) = frame else {
            panic!("not an array")
        };
        let mut i = 0;
        while i + 1 < items.len() {
            if let Frame::BulkString(k) = &items[i] {
                if k.as_ref() == key {
                    if let Frame::Integer(n) = &items[i + 1] {
                        return *n;
                    }
                    panic!("value for {key:?} not Integer");
                }
            }
            i += 2;
        }
        panic!("key {key:?} not found");
    }

    #[test]
    fn sums_additive_fields_across_shards() {
        let local = info_frame(10, 10, 4, 1);
        let remotes = [info_frame(5, 5, 2, 1), info_frame(3, 3, 3, 0)];
        let merged = merge_ft_info_responses(local, &remotes);
        assert_eq!(get_int(&merged, b"num_docs"), 18);
        assert_eq!(get_int(&merged, b"vector_version_token"), 21);
        // Config fields untouched.
        assert_eq!(get_int(&merged, b"dimension"), 8);
        // Nested per-field additivity.
        let Frame::Array(items) = &merged else {
            unreachable!()
        };
        let vf_idx = items
            .iter()
            .position(|f| matches!(f, Frame::BulkString(b) if b.as_ref() == b"vector_fields"))
            .unwrap();
        let Frame::Array(entries) = &items[vf_idx + 1] else {
            panic!("vector_fields not array")
        };
        assert_eq!(get_int(&entries[0], b"num_docs"), 18);
        assert_eq!(get_int(&entries[0], b"mutable_vectors"), 9);
        assert_eq!(get_int(&entries[0], b"immutable_segments"), 2);
    }

    #[test]
    fn sums_exact_rerank_coverage_counters() {
        // R5: graph_segments / segments_with_exact_rerank are additive so a
        // sidecar-less segment on ANY shard shows up in the aggregate
        // (coverage < total ⇒ some segment answers with ADC-only distances).
        fn frame_with_coverage(total: i64, with_rerank: i64) -> Frame {
            Frame::Array(
                vec![
                    bs(b"index_name"),
                    bs(b"idx"),
                    bs(b"graph_segments"),
                    Frame::Integer(total),
                    bs(b"segments_with_exact_rerank"),
                    Frame::Integer(with_rerank),
                ]
                .into(),
            )
        }
        let merged = merge_ft_info_responses(
            frame_with_coverage(3, 3),
            &[frame_with_coverage(2, 1), frame_with_coverage(1, 0)],
        );
        assert_eq!(get_int(&merged, b"graph_segments"), 6);
        assert_eq!(get_int(&merged, b"segments_with_exact_rerank"), 4);
    }

    #[test]
    fn propagates_remote_error() {
        let local = info_frame(10, 10, 4, 1);
        let remotes = [Frame::Error(Bytes::from_static(b"ERR boom"))];
        let merged = merge_ft_info_responses(local, &remotes);
        assert!(matches!(merged, Frame::Error(_)));
    }

    #[test]
    fn no_remotes_is_identity() {
        let local = info_frame(10, 10, 4, 1);
        let merged = merge_ft_info_responses(local.clone(), &[]);
        assert_eq!(get_int(&merged, b"num_docs"), 10);
        assert_eq!(get_int(&merged, b"vector_version_token"), 7);
    }
}
