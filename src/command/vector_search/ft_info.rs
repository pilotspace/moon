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
    _text_store: &crate::text::store::TextStore,
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
        None => return Frame::Error(Bytes::from_static(b"Unknown Index name")),
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

    Frame::Array(items.into())
}
