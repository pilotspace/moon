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
    let idx = match store.get_index(&name) {
        Some(i) => i,
        None => {
            // Check TextStore for TEXT-only indexes
            if let Some(text_idx) = text_store.get_index(&name) {
                return ft_info_text_only(text_idx);
            }
            return Frame::Error(Bytes::from_static(b"Unknown Index name"));
        }
    };

    // Count default field docs across mutable + immutable segments.
    let snap = idx.segments.load();
    let mut num_docs = snap.mutable.len();
    for imm in snap.immutable.iter() {
        num_docs += imm.live_count() as usize;
    }

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
            (docs, s.mutable.len(), imm_count)
        } else if let Some(fs) = idx.field_segments.get(&field_meta.field_name) {
            let s = fs.segments.load();
            let mut docs = s.mutable.len();
            let imm_count = s.immutable.len();
            for imm in s.immutable.iter() {
                docs += imm.live_count() as usize;
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

    // Hybrid index: append text field stats if this index also has a TextIndex
    if let Some(text_idx) = text_store.get_index(&name) {
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
        items.push(Frame::BulkString(Bytes::from_static(b"total_inverted_index_size")));
        items.push(Frame::Integer(text_idx.total_posting_bytes() as i64));
    }

    Frame::Array(items.into())
}

/// Full FT.INFO response for TEXT-only indexes.
///
/// Returns index_name, num_docs, num_terms, per-field stats (num_docs,
/// avg_doc_len, weight, nostem), BM25 config, and memory estimates.
fn ft_info_text_only(idx: &crate::text::store::TextIndex) -> Frame {
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
    items.push(Frame::BulkString(Bytes::from_static(b"total_inverted_index_size")));
    items.push(Frame::Integer(total_postings as i64));

    Frame::Array(items.into())
}
