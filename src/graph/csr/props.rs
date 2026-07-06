//! Property / embedding / weight blob codec for CSR segments (format v5).
//!
//! Before v5, `CsrSegment::from_frozen` wrote `property_offset: 0` for every
//! node and edge — properties, embeddings, and edge weights were silently
//! DISCARDED at freeze (2026-07 graph deep review P0-1). v5 appends two
//! variable-length sections to the segment payload and points the reserved
//! `NodeMeta::property_offset` / `EdgeMeta::property_offset` fields into them.
//!
//! Encoding (all little-endian, unaligned — read with bounds-checked helpers):
//!
//! ```text
//! node record: [n_props: u16] [prop]* [emb_dim: u32] [f32 * emb_dim]
//! edge record: [weight: f64] [n_props: u16] [prop]*
//! prop:        [key: u16] [tag: u8] [payload]
//!   tag 0 Int:    i64
//!   tag 1 Float:  f64
//!   tag 2 String: [len: u32] [bytes]
//!   tag 3 Bool:   u8 (0/1)
//!   tag 4 Bytes:  [len: u32] [bytes]
//! ```
//!
//! Offsets are stored **offset + 1**; `0` means "no record" (empty props, no
//! embedding, default weight 1.0). This keeps `0` — the value every pre-v5
//! segment carries — meaning exactly what it always meant.
//!
//! Decoders are defensive: any out-of-bounds or malformed input yields `None`
//! / empty rather than panicking (fuzz target: `graph_props_record`).

use bytes::Bytes;

use crate::graph::types::{PropertyMap, PropertyValue};

/// Default edge weight when no record exists.
pub const DEFAULT_EDGE_WEIGHT: f64 = 1.0;

const TAG_INT: u8 = 0;
const TAG_FLOAT: u8 = 1;
const TAG_STRING: u8 = 2;
const TAG_BOOL: u8 = 3;
const TAG_BYTES: u8 = 4;

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

fn encode_props(buf: &mut Vec<u8>, props: &[(u16, PropertyValue)]) {
    buf.extend_from_slice(&(props.len() as u16).to_le_bytes());
    for (key, value) in props {
        buf.extend_from_slice(&key.to_le_bytes());
        match value {
            PropertyValue::Int(v) => {
                buf.push(TAG_INT);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            PropertyValue::Float(v) => {
                buf.push(TAG_FLOAT);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            PropertyValue::String(b) => {
                buf.push(TAG_STRING);
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
            PropertyValue::Bool(v) => {
                buf.push(TAG_BOOL);
                buf.push(u8::from(*v));
            }
            PropertyValue::Bytes(b) => {
                buf.push(TAG_BYTES);
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
        }
    }
}

/// Append a node record (properties + optional embedding) to `blob`.
///
/// Returns the stored offset (`byte_pos + 1`), or `0` if the node has neither
/// properties nor an embedding (nothing is written).
pub fn encode_node_record(
    blob: &mut Vec<u8>,
    props: &[(u16, PropertyValue)],
    embedding: Option<&[f32]>,
) -> u32 {
    if props.is_empty() && embedding.is_none() {
        return 0;
    }
    let pos = blob.len();
    encode_props(blob, props);
    match embedding {
        Some(emb) => {
            blob.extend_from_slice(&(emb.len() as u32).to_le_bytes());
            for f in emb {
                blob.extend_from_slice(&f.to_le_bytes());
            }
        }
        None => blob.extend_from_slice(&0u32.to_le_bytes()),
    }
    (pos as u32) + 1
}

/// Append an edge record (weight + optional properties) to `blob`.
///
/// Returns the stored offset (`byte_pos + 1`), or `0` when the edge carries
/// the default weight and no properties (nothing is written).
pub fn encode_edge_record(
    blob: &mut Vec<u8>,
    weight: f64,
    props: Option<&PropertyMap>,
) -> u32 {
    let has_props = props.is_some_and(|p| !p.is_empty());
    if weight == DEFAULT_EDGE_WEIGHT && !has_props {
        return 0;
    }
    let pos = blob.len();
    blob.extend_from_slice(&weight.to_le_bytes());
    match props {
        Some(p) => encode_props(blob, p),
        None => blob.extend_from_slice(&0u16.to_le_bytes()),
    }
    (pos as u32) + 1
}

// ---------------------------------------------------------------------------
// Bounds-checked primitive reads (no panics on malformed input)
// ---------------------------------------------------------------------------

fn rd_u8(blob: &[u8], pos: usize) -> Option<u8> {
    blob.get(pos).copied()
}

fn rd_u16(blob: &[u8], pos: usize) -> Option<u16> {
    Some(u16::from_le_bytes(
        blob.get(pos..pos + 2)?.try_into().ok()?,
    ))
}

fn rd_u32(blob: &[u8], pos: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        blob.get(pos..pos + 4)?.try_into().ok()?,
    ))
}

fn rd_u64(blob: &[u8], pos: usize) -> Option<u64> {
    Some(u64::from_le_bytes(
        blob.get(pos..pos + 8)?.try_into().ok()?,
    ))
}

/// Decode one property at `pos`. Returns `(key, value, next_pos)`.
fn decode_prop(blob: &[u8], pos: usize) -> Option<(u16, PropertyValue, usize)> {
    let key = rd_u16(blob, pos)?;
    let tag = rd_u8(blob, pos + 2)?;
    let vpos = pos + 3;
    match tag {
        TAG_INT => Some((
            key,
            PropertyValue::Int(rd_u64(blob, vpos)? as i64),
            vpos + 8,
        )),
        TAG_FLOAT => Some((
            key,
            PropertyValue::Float(f64::from_bits(rd_u64(blob, vpos)?)),
            vpos + 8,
        )),
        TAG_STRING | TAG_BYTES => {
            let len = rd_u32(blob, vpos)? as usize;
            let start = vpos + 4;
            let bytes = blob.get(start..start.checked_add(len)?)?;
            let b = Bytes::copy_from_slice(bytes);
            let value = if tag == TAG_STRING {
                PropertyValue::String(b)
            } else {
                PropertyValue::Bytes(b)
            };
            Some((key, value, start + len))
        }
        TAG_BOOL => Some((key, PropertyValue::Bool(rd_u8(blob, vpos)? != 0), vpos + 1)),
        _ => None,
    }
}

/// Skip one property at `pos`, returning the next position.
fn skip_prop(blob: &[u8], pos: usize) -> Option<usize> {
    let tag = rd_u8(blob, pos + 2)?;
    let vpos = pos + 3;
    match tag {
        TAG_INT | TAG_FLOAT => Some(vpos + 8),
        TAG_STRING | TAG_BYTES => {
            let len = rd_u32(blob, vpos)? as usize;
            let end = (vpos + 4).checked_add(len)?;
            if end > blob.len() { None } else { Some(end) }
        }
        TAG_BOOL => Some(vpos + 1),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Node record decoding
// ---------------------------------------------------------------------------

/// Decode a node's full property map. `stored_offset` is the raw
/// `NodeMeta::property_offset` value (`0` = no record → empty map).
pub fn decode_node_props(blob: &[u8], stored_offset: u32) -> PropertyMap {
    let mut out = PropertyMap::new();
    if stored_offset == 0 {
        return out;
    }
    let mut pos = (stored_offset - 1) as usize;
    let Some(n) = rd_u16(blob, pos) else {
        return out;
    };
    pos += 2;
    for _ in 0..n {
        match decode_prop(blob, pos) {
            Some((key, value, next)) => {
                out.push((key, value));
                pos = next;
            }
            None => break,
        }
    }
    out
}

/// Look up a single property by key without materializing the map.
pub fn decode_node_prop(blob: &[u8], stored_offset: u32, want: u16) -> Option<PropertyValue> {
    if stored_offset == 0 {
        return None;
    }
    let mut pos = (stored_offset - 1) as usize;
    let n = rd_u16(blob, pos)?;
    pos += 2;
    for _ in 0..n {
        let key = rd_u16(blob, pos)?;
        if key == want {
            return decode_prop(blob, pos).map(|(_, v, _)| v);
        }
        pos = skip_prop(blob, pos)?;
    }
    None
}

/// Decode a node's embedding. Returns `None` for no record / no embedding /
/// malformed input.
pub fn decode_node_embedding(blob: &[u8], stored_offset: u32) -> Option<Vec<f32>> {
    if stored_offset == 0 {
        return None;
    }
    let mut pos = (stored_offset - 1) as usize;
    let n = rd_u16(blob, pos)?;
    pos += 2;
    for _ in 0..n {
        pos = skip_prop(blob, pos)?;
    }
    let dim = rd_u32(blob, pos)? as usize;
    if dim == 0 {
        return None;
    }
    pos += 4;
    let end = pos.checked_add(dim.checked_mul(4)?)?;
    let bytes = blob.get(pos..end)?;
    let mut out = Vec::with_capacity(dim);
    for chunk in bytes.chunks_exact(4) {
        #[allow(clippy::unwrap_used)] // chunks_exact(4) guarantees 4-byte chunks
        let arr: [u8; 4] = chunk.try_into().unwrap();
        out.push(f32::from_le_bytes(arr));
    }
    Some(out)
}

/// Byte length of the node record starting at `stored_offset` (for record
/// copying during segment merge). `None` for absent/malformed records.
pub fn node_record_len(blob: &[u8], stored_offset: u32) -> Option<usize> {
    if stored_offset == 0 {
        return None;
    }
    let start = (stored_offset - 1) as usize;
    let mut pos = start;
    let n = rd_u16(blob, pos)?;
    pos += 2;
    for _ in 0..n {
        pos = skip_prop(blob, pos)?;
    }
    let dim = rd_u32(blob, pos)? as usize;
    pos = (pos + 4).checked_add(dim.checked_mul(4)?)?;
    if pos > blob.len() { None } else { Some(pos - start) }
}

// ---------------------------------------------------------------------------
// Edge record decoding
// ---------------------------------------------------------------------------

/// Decode an edge's weight. `0` offset (or malformed input) yields the
/// default weight 1.0.
pub fn decode_edge_weight(blob: &[u8], stored_offset: u32) -> f64 {
    if stored_offset == 0 {
        return DEFAULT_EDGE_WEIGHT;
    }
    match rd_u64(blob, (stored_offset - 1) as usize) {
        Some(bits) => f64::from_bits(bits),
        None => DEFAULT_EDGE_WEIGHT,
    }
}

/// Decode an edge's full property map (empty for absent/malformed records).
pub fn decode_edge_props(blob: &[u8], stored_offset: u32) -> PropertyMap {
    let mut out = PropertyMap::new();
    if stored_offset == 0 {
        return out;
    }
    let mut pos = (stored_offset - 1) as usize + 8; // skip weight
    let Some(n) = rd_u16(blob, pos) else {
        return out;
    };
    pos += 2;
    for _ in 0..n {
        match decode_prop(blob, pos) {
            Some((key, value, next)) => {
                out.push((key, value));
                pos = next;
            }
            None => break,
        }
    }
    out
}

/// Look up a single edge property by key.
pub fn decode_edge_prop(blob: &[u8], stored_offset: u32, want: u16) -> Option<PropertyValue> {
    if stored_offset == 0 {
        return None;
    }
    let mut pos = (stored_offset - 1) as usize + 8; // skip weight
    let n = rd_u16(blob, pos)?;
    pos += 2;
    for _ in 0..n {
        let key = rd_u16(blob, pos)?;
        if key == want {
            return decode_prop(blob, pos).map(|(_, v, _)| v);
        }
        pos = skip_prop(blob, pos)?;
    }
    None
}

/// Byte length of the edge record starting at `stored_offset`.
pub fn edge_record_len(blob: &[u8], stored_offset: u32) -> Option<usize> {
    if stored_offset == 0 {
        return None;
    }
    let start = (stored_offset - 1) as usize;
    let mut pos = start + 8; // weight
    let n = rd_u16(blob, pos)?;
    pos += 2;
    for _ in 0..n {
        pos = skip_prop(blob, pos)?;
    }
    if pos > blob.len() { None } else { Some(pos - start) }
}

/// Copy an existing record verbatim into `dst`, returning the new stored
/// offset (`0` in = `0` out; malformed source records are dropped to `0`
/// rather than corrupting the destination blob).
pub fn copy_record(
    dst: &mut Vec<u8>,
    src: &[u8],
    stored_offset: u32,
    len_of: fn(&[u8], u32) -> Option<usize>,
) -> u32 {
    if stored_offset == 0 {
        return 0;
    }
    match len_of(src, stored_offset) {
        Some(len) => {
            let start = (stored_offset - 1) as usize;
            let pos = dst.len();
            dst.extend_from_slice(&src[start..start + len]);
            (pos as u32) + 1
        }
        None => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::smallvec;

    fn sample_props() -> PropertyMap {
        smallvec![
            (7u16, PropertyValue::Int(-42)),
            (9u16, PropertyValue::Float(2.5)),
            (11u16, PropertyValue::String(Bytes::from_static(b"alice"))),
            (13u16, PropertyValue::Bool(true)),
            (15u16, PropertyValue::Bytes(Bytes::from_static(b"\x00\x01"))),
        ]
    }

    #[test]
    fn node_record_roundtrip_props_and_embedding() {
        let mut blob = Vec::new();
        let props = sample_props();
        let emb = vec![1.0f32, -2.5, 0.0, 3.25];
        let off = encode_node_record(&mut blob, &props, Some(&emb));
        assert_ne!(off, 0);

        let decoded = decode_node_props(&blob, off);
        assert_eq!(decoded.as_slice(), props.as_slice());
        assert_eq!(decode_node_embedding(&blob, off).as_deref(), Some(&emb[..]));
        assert_eq!(
            decode_node_prop(&blob, off, 11),
            Some(PropertyValue::String(Bytes::from_static(b"alice")))
        );
        assert_eq!(decode_node_prop(&blob, off, 99), None);
        assert_eq!(node_record_len(&blob, off), Some(blob.len()));
    }

    #[test]
    fn empty_node_record_writes_nothing() {
        let mut blob = Vec::new();
        let off = encode_node_record(&mut blob, &[], None);
        assert_eq!(off, 0);
        assert!(blob.is_empty());
        assert!(decode_node_props(&blob, 0).is_empty());
        assert_eq!(decode_node_embedding(&blob, 0), None);
    }

    #[test]
    fn node_record_props_only_no_embedding() {
        let mut blob = Vec::new();
        let props: PropertyMap = smallvec![(1u16, PropertyValue::Int(5))];
        let off = encode_node_record(&mut blob, &props, None);
        assert_ne!(off, 0);
        assert_eq!(decode_node_props(&blob, off).as_slice(), props.as_slice());
        assert_eq!(decode_node_embedding(&blob, off), None);
    }

    #[test]
    fn edge_record_roundtrip() {
        let mut blob = Vec::new();
        let props: PropertyMap = smallvec![(3u16, PropertyValue::Float(0.125))];
        let off = encode_edge_record(&mut blob, 4.5, Some(&props));
        assert_ne!(off, 0);
        assert_eq!(decode_edge_weight(&blob, off), 4.5);
        assert_eq!(decode_edge_props(&blob, off).as_slice(), props.as_slice());
        assert_eq!(
            decode_edge_prop(&blob, off, 3),
            Some(PropertyValue::Float(0.125))
        );
        assert_eq!(edge_record_len(&blob, off), Some(blob.len()));
    }

    #[test]
    fn default_weight_no_props_writes_nothing() {
        let mut blob = Vec::new();
        let off = encode_edge_record(&mut blob, DEFAULT_EDGE_WEIGHT, None);
        assert_eq!(off, 0);
        assert!(blob.is_empty());
        assert_eq!(decode_edge_weight(&blob, 0), DEFAULT_EDGE_WEIGHT);
    }

    #[test]
    fn non_default_weight_without_props_is_stored() {
        let mut blob = Vec::new();
        let off = encode_edge_record(&mut blob, 0.5, None);
        assert_ne!(off, 0);
        assert_eq!(decode_edge_weight(&blob, off), 0.5);
        assert!(decode_edge_props(&blob, off).is_empty());
    }

    #[test]
    fn multiple_records_in_one_blob() {
        let mut blob = Vec::new();
        let p1: PropertyMap = smallvec![(1u16, PropertyValue::Int(1))];
        let p2: PropertyMap = smallvec![(2u16, PropertyValue::Int(2))];
        let o1 = encode_node_record(&mut blob, &p1, None);
        let o2 = encode_node_record(&mut blob, &p2, Some(&[9.0f32]));
        assert_ne!(o1, o2);
        assert_eq!(decode_node_props(&blob, o1).as_slice(), p1.as_slice());
        assert_eq!(decode_node_props(&blob, o2).as_slice(), p2.as_slice());
        assert_eq!(decode_node_embedding(&blob, o1), None);
        assert_eq!(decode_node_embedding(&blob, o2), Some(vec![9.0f32]));
    }

    #[test]
    fn copy_record_preserves_content() {
        let mut src = Vec::new();
        let props = sample_props();
        let off = encode_node_record(&mut src, &props, Some(&[1.0f32, 2.0]));
        let mut dst = vec![0xAAu8; 17]; // non-empty destination with prior content
        let new_off = copy_record(&mut dst, &src, off, node_record_len);
        assert_ne!(new_off, 0);
        assert_eq!(decode_node_props(&dst, new_off).as_slice(), props.as_slice());
        assert_eq!(
            decode_node_embedding(&dst, new_off),
            Some(vec![1.0f32, 2.0])
        );
        // Zero copies through as zero.
        assert_eq!(copy_record(&mut dst, &src, 0, node_record_len), 0);
    }

    #[test]
    fn malformed_input_never_panics() {
        // Truncated at every prefix of a valid record.
        let mut blob = Vec::new();
        let props = sample_props();
        let off = encode_node_record(&mut blob, &props, Some(&[1.0f32]));
        for cut in 0..blob.len() {
            let truncated = &blob[..cut];
            let _ = decode_node_props(truncated, off);
            let _ = decode_node_embedding(truncated, off);
            let _ = decode_node_prop(truncated, off, 7);
            let _ = node_record_len(truncated, off);
            let _ = decode_edge_weight(truncated, off);
            let _ = decode_edge_props(truncated, off);
            let _ = edge_record_len(truncated, off);
        }
        // Out-of-range offsets.
        for bogus in [1u32, 5, 1000, u32::MAX] {
            let _ = decode_node_props(&blob, bogus);
            let _ = decode_node_embedding(&blob, bogus);
            let _ = decode_edge_weight(&blob, bogus);
            let _ = decode_edge_props(&blob, bogus);
        }
        // Garbage bytes with a bogus tag.
        let garbage = vec![2u8, 0, 1, 0, 250, 9, 9, 9, 9];
        let _ = decode_node_props(&garbage, 1);
        let _ = decode_node_embedding(&garbage, 1);
    }
}
