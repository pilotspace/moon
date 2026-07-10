#![no_main]
use libfuzzer_sys::fuzz_target;

use moon::graph::csr::props::{
    decode_edge_prop, decode_edge_props, decode_edge_weight, decode_node_embedding,
    decode_node_prop, decode_node_props, edge_record_len, node_record_len,
};

/// Fuzz the CSR node/edge property-record codec (segment format v5).
///
/// `blob` is untrusted bytes loaded straight from a `.csr` segment file, and
/// `stored_offset` is the raw (attacker/corruption-controlled) offset field
/// read out of `NodeMeta`/`EdgeMeta`. Exercises the bounds-checked prop, tag
/// (Int/Float/String/Bool/Bytes), embedding, and weight decoders against
/// truncated blobs, out-of-range offsets, and malformed tag bytes. Every
/// decoder in `src/graph/csr/props.rs` is documented to return `None`/empty
/// on malformed input rather than panic — any panic or OOB access here is a
/// bug.
fuzz_target!(|data: &[u8]| {
    if data.len() < 6 {
        return;
    }
    let (header, blob) = data.split_at(6);
    #[allow(clippy::unwrap_used)] // split_at(6) guarantees a 6-byte header
    let stored_offset = u32::from_le_bytes(header[0..4].try_into().unwrap());
    #[allow(clippy::unwrap_used)] // split_at(6) guarantees a 6-byte header
    let want_key = u16::from_le_bytes(header[4..6].try_into().unwrap());

    let _ = decode_node_props(blob, stored_offset);
    let _ = decode_node_prop(blob, stored_offset, want_key);
    let _ = decode_node_embedding(blob, stored_offset);
    let _ = node_record_len(blob, stored_offset);

    let _ = decode_edge_weight(blob, stored_offset);
    let _ = decode_edge_props(blob, stored_offset);
    let _ = decode_edge_prop(blob, stored_offset, want_key);
    let _ = edge_record_len(blob, stored_offset);
});
