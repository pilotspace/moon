//! Delta + VByte encoding for HNSW neighbor lists (design Section 12).
//!
//! Format:
//!   [count: VByte] [first: u32 LE] [delta_1: VByte] [delta_2: VByte] ...
//!
//! Neighbors are sorted ascending. Deltas are differences between consecutive
//! values. VByte uses 7 bits per byte, high bit = continuation (1 = more bytes).
//! SENTINEL (u32::MAX) values are stripped before encoding.
//!
//! This module is only used in the warm serialization path, NOT the hot search
//! path. Allocations in encode/decode are acceptable.

use super::graph::SENTINEL;

/// Encode a VByte value into the output buffer.
///
/// VByte: emit 7 bits per byte, high bit set means more bytes follow.
/// Maximum 5 bytes for u32.
#[inline]
fn encode_vbyte(mut val: u32, out: &mut Vec<u8>) {
    loop {
        let byte = (val & 0x7F) as u8;
        val >>= 7;
        if val == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// Decode a VByte value from `data` starting at `*pos`.
///
/// Returns `None` if the data is truncated (no terminating byte before end).
/// Advances `*pos` past the decoded bytes.
#[inline]
fn decode_vbyte(data: &[u8], pos: &mut usize) -> Option<u32> {
    let mut val: u32 = 0;
    let mut shift: u32 = 0;
    loop {
        if *pos >= data.len() {
            return None;
        }
        let byte = data[*pos];
        *pos += 1;
        val |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            return Some(val);
        }
        shift += 7;
        if shift >= 35 {
            // Overflow protection: u32 needs at most 5 bytes (5*7=35 bits)
            return None;
        }
    }
}

/// Encode a neighbor list using delta + VByte compression.
///
/// - SENTINEL values are filtered out
/// - Remaining values are sorted ascending
/// - First value stored as u32 LE (4 bytes)
/// - Subsequent values stored as VByte-encoded deltas
///
/// Returns the compressed byte buffer.
pub fn encode_neighbors(neighbors: &[u32]) -> Vec<u8> {
    // Filter sentinels and sort
    let mut sorted: Vec<u32> = neighbors.iter().copied().filter(|&v| v != SENTINEL).collect();
    sorted.sort_unstable();

    let mut out = Vec::with_capacity(sorted.len() * 2 + 5);

    // Write count as VByte
    encode_vbyte(sorted.len() as u32, &mut out);

    if sorted.is_empty() {
        return out;
    }

    // Write first value as 4 bytes LE
    out.extend_from_slice(&sorted[0].to_le_bytes());

    // Write deltas as VByte
    let mut prev = sorted[0];
    for &val in &sorted[1..] {
        let delta = val - prev;
        encode_vbyte(delta, &mut out);
        prev = val;
    }

    out
}

/// Decode a neighbor list from delta + VByte compressed format.
///
/// Returns the reconstructed sorted neighbor list. On any truncation or
/// format error, returns an empty vec (does not panic).
pub fn decode_neighbors(data: &[u8]) -> Vec<u32> {
    let mut pos = 0;

    // Read count
    let count = match decode_vbyte(data, &mut pos) {
        Some(c) => c as usize,
        None => return Vec::new(),
    };

    if count == 0 {
        return Vec::new();
    }

    // Read first value as u32 LE
    if pos + 4 > data.len() {
        return Vec::new();
    }
    let first = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
    pos += 4;

    let mut result = Vec::with_capacity(count);
    result.push(first);

    let mut prev = first;
    for _ in 1..count {
        let delta = match decode_vbyte(data, &mut pos) {
            Some(d) => d,
            None => return Vec::new(),
        };
        prev += delta;
        result.push(prev);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_roundtrip() {
        let encoded = encode_neighbors(&[]);
        assert_eq!(encoded, vec![0u8]); // zero-length prefix
        let decoded = decode_neighbors(&encoded);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_single_element_roundtrip() {
        let encoded = encode_neighbors(&[42]);
        let decoded = decode_neighbors(&encoded);
        assert_eq!(decoded, vec![42]);
    }

    #[test]
    fn test_sorted_list_roundtrip() {
        let input = [5, 10, 15, 20, 100];
        let encoded = encode_neighbors(&input);
        let decoded = decode_neighbors(&encoded);
        assert_eq!(decoded, vec![5, 10, 15, 20, 100]);
    }

    #[test]
    fn test_unsorted_input_gets_sorted() {
        let input = [100, 20, 5, 15, 10];
        let encoded = encode_neighbors(&input);
        let decoded = decode_neighbors(&encoded);
        assert_eq!(decoded, vec![5, 10, 15, 20, 100]);
    }

    #[test]
    fn test_sentinel_filtered() {
        let input = [10, SENTINEL, 20, SENTINEL, 30];
        let encoded = encode_neighbors(&input);
        let decoded = decode_neighbors(&encoded);
        assert_eq!(decoded, vec![10, 20, 30]);
    }

    #[test]
    fn test_large_values_roundtrip() {
        let input = [0, 1, 1_000_000, u32::MAX - 1];
        let encoded = encode_neighbors(&input);
        let decoded = decode_neighbors(&encoded);
        assert_eq!(decoded, vec![0, 1, 1_000_000, u32::MAX - 1]);
    }

    #[test]
    fn test_decode_truncated_returns_empty() {
        // Truncated: count says 5 but only 1 byte of data
        let encoded = encode_neighbors(&[10, 20, 30, 40, 50]);
        let truncated = &encoded[..3]; // count + partial first value
        let decoded = decode_neighbors(truncated);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_decode_empty_slice_returns_empty() {
        let decoded = decode_neighbors(&[]);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_compression_ratio() {
        // 32 neighbors in range 0..1000: deltas are small, VByte should compress well
        let input: Vec<u32> = (0..32).map(|i| i * 31).collect();
        let encoded = encode_neighbors(&input);
        let raw_size = 32 * 4; // 128 bytes
        assert!(
            encoded.len() < raw_size,
            "Encoded size {} should be less than raw size {}",
            encoded.len(),
            raw_size
        );
    }

    #[test]
    fn test_vbyte_single_byte_values() {
        // Values < 128 should encode as single byte
        let mut buf = Vec::new();
        encode_vbyte(0, &mut buf);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0], 0);

        buf.clear();
        encode_vbyte(127, &mut buf);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0], 127);
    }

    #[test]
    fn test_vbyte_multi_byte_values() {
        // 128 needs 2 bytes
        let mut buf = Vec::new();
        encode_vbyte(128, &mut buf);
        assert_eq!(buf.len(), 2);

        let mut pos = 0;
        let decoded = decode_vbyte(&buf, &mut pos).unwrap();
        assert_eq!(decoded, 128);

        // u32::MAX - 1 needs 5 bytes
        buf.clear();
        encode_vbyte(u32::MAX - 1, &mut buf);
        assert_eq!(buf.len(), 5);

        pos = 0;
        let decoded = decode_vbyte(&buf, &mut pos).unwrap();
        assert_eq!(decoded, u32::MAX - 1);
    }

    #[test]
    fn test_all_sentinel_input() {
        let input = [SENTINEL, SENTINEL, SENTINEL];
        let encoded = encode_neighbors(&input);
        let decoded = decode_neighbors(&encoded);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_duplicate_values_roundtrip() {
        let input = [5, 5, 10, 10, 10];
        let encoded = encode_neighbors(&input);
        let decoded = decode_neighbors(&encoded);
        assert_eq!(decoded, vec![5, 5, 10, 10, 10]);
    }
}
