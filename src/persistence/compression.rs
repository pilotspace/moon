// Delta-of-delta varint encoding for timestamps and Gorilla XOR encoding for f64 values.
// Design reference: MoonStore v2 design section 12.
//
// Delta encoding targets TTL timestamps (monotonic, small deltas).
// Gorilla encoding targets ZSET scores (slowly changing f64 values).

// ---------------------------------------------------------------------------
// Zigzag + Varint helpers
// ---------------------------------------------------------------------------

/// Zigzag-encode a signed i64 into an unsigned u64.
/// Maps negative values to odd numbers, positive to even, so small-magnitude
/// values (positive or negative) produce small unsigned values.
fn zigzag_encode(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

/// Decode a zigzag-encoded u64 back to i64.
fn zigzag_decode(n: u64) -> i64 {
    ((n >> 1) as i64) ^ -((n & 1) as i64)
}

/// Append a variable-length encoded u64 to `buf`.
/// Uses 7 bits per byte; high bit = continuation.
fn write_varint(buf: &mut Vec<u8>, mut val: u64) {
    loop {
        let byte = (val & 0x7F) as u8;
        val >>= 7;
        if val == 0 {
            buf.push(byte);
            return;
        }
        buf.push(byte | 0x80);
    }
}

/// Read a varint from `data` starting at `*pos`. Advances `*pos` past the
/// consumed bytes. Returns `None` if the data is truncated.
fn read_varint(data: &[u8], pos: &mut usize) -> Option<u64> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        if *pos >= data.len() {
            return None;
        }
        let byte = data[*pos];
        *pos += 1;
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some(result);
        }
        shift += 7;
        if shift >= 70 {
            return None; // overflow protection
        }
    }
}

// ---------------------------------------------------------------------------
// Delta-of-delta encoding for timestamps
// ---------------------------------------------------------------------------
// Format: [count: u32 LE][first_value: u64 LE][zigzag varints...]
//
// The first varint is the zigzag-encoded first delta.
// Subsequent varints are zigzag-encoded delta-of-deltas.

/// Encode a slice of u64 timestamps using delta-of-delta varint compression.
///
/// Monotonic timestamps with constant stride compress to ~1 byte per value.
pub fn delta_encode_timestamps(timestamps: &[u64]) -> Vec<u8> {
    if timestamps.is_empty() {
        return Vec::new();
    }

    // Estimate capacity: 4 (count) + 8 (first) + ~2 bytes per remaining value
    let mut buf = Vec::with_capacity(12 + timestamps.len() * 2);

    // Count prefix
    buf.extend_from_slice(&(timestamps.len() as u32).to_le_bytes());
    // First value raw
    buf.extend_from_slice(&timestamps[0].to_le_bytes());

    if timestamps.len() == 1 {
        return buf;
    }

    let mut prev_delta: i64 = 0;

    for i in 1..timestamps.len() {
        let delta = timestamps[i].wrapping_sub(timestamps[i - 1]) as i64;
        let dod = delta.wrapping_sub(prev_delta);
        write_varint(&mut buf, zigzag_encode(dod));
        prev_delta = delta;
    }

    buf
}

/// Decode a delta-of-delta encoded buffer back to the original timestamps.
///
/// Returns an empty Vec if the data is malformed or empty.
pub fn delta_decode_timestamps(data: &[u8]) -> Vec<u64> {
    if data.len() < 4 {
        return Vec::new();
    }

    let count = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if count == 0 {
        return Vec::new();
    }

    if data.len() < 12 {
        return Vec::new();
    }

    let first = u64::from_le_bytes([
        data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
    ]);

    let mut result = Vec::with_capacity(count);
    result.push(first);

    if count == 1 {
        return result;
    }

    let mut pos = 12;
    let mut prev_delta: i64 = 0;
    let mut prev_value = first;

    for _ in 1..count {
        let Some(zz) = read_varint(data, &mut pos) else {
            break;
        };
        let dod = zigzag_decode(zz);
        let delta = prev_delta.wrapping_add(dod);
        let value = prev_value.wrapping_add(delta as u64);
        result.push(value);
        prev_delta = delta;
        prev_value = value;
    }

    result
}

// ---------------------------------------------------------------------------
// Gorilla XOR encoding for f64 values
// ---------------------------------------------------------------------------
// Format: [count: u32 LE][first_value: f64 LE][bit-packed XOR deltas...]
//
// Facebook Gorilla paper adapted:
// - XOR == 0 => single `0` bit
// - XOR != 0 => `1` bit + 5-bit leading_zeros + 6-bit meaningful_bits + meaningful bits

struct BitWriter {
    buf: Vec<u8>,
    current_byte: u8,
    bit_pos: u8, // bits written in current byte (0..8)
}

impl BitWriter {
    fn new(capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
            current_byte: 0,
            bit_pos: 0,
        }
    }

    fn write_bit(&mut self, bit: bool) {
        if bit {
            self.current_byte |= 1 << (7 - self.bit_pos);
        }
        self.bit_pos += 1;
        if self.bit_pos == 8 {
            self.buf.push(self.current_byte);
            self.current_byte = 0;
            self.bit_pos = 0;
        }
    }

    fn write_bits(&mut self, val: u64, num_bits: u8) {
        for i in (0..num_bits).rev() {
            self.write_bit((val >> i) & 1 == 1);
        }
    }

    fn finish(mut self) -> Vec<u8> {
        if self.bit_pos > 0 {
            self.buf.push(self.current_byte);
        }
        self.buf
    }
}

struct BitReader<'a> {
    data: &'a [u8],
    byte_pos: usize,
    bit_pos: u8,
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8], start_byte: usize) -> Self {
        Self {
            data,
            byte_pos: start_byte,
            bit_pos: 0,
        }
    }

    fn read_bit(&mut self) -> Option<bool> {
        if self.byte_pos >= self.data.len() {
            return None;
        }
        let bit = (self.data[self.byte_pos] >> (7 - self.bit_pos)) & 1 == 1;
        self.bit_pos += 1;
        if self.bit_pos == 8 {
            self.byte_pos += 1;
            self.bit_pos = 0;
        }
        Some(bit)
    }

    fn read_bits(&mut self, num_bits: u8) -> Option<u64> {
        let mut val: u64 = 0;
        for _ in 0..num_bits {
            let bit = self.read_bit()?;
            val = (val << 1) | (bit as u64);
        }
        Some(val)
    }
}

/// Encode a slice of f64 values using Gorilla XOR compression.
///
/// Identical consecutive values compress to 1 bit each. Slowly-changing
/// values compress to ~15-20 bits each.
pub fn gorilla_encode_f64(values: &[f64]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }

    // Header: 4-byte count + 8-byte first value
    let mut header = Vec::with_capacity(12);
    header.extend_from_slice(&(values.len() as u32).to_le_bytes());
    header.extend_from_slice(&values[0].to_bits().to_le_bytes());

    if values.len() == 1 {
        return header;
    }

    let mut writer = BitWriter::new(values.len()); // rough estimate

    let mut prev_bits = values[0].to_bits();

    for &val in &values[1..] {
        let cur_bits = val.to_bits();
        let xor = prev_bits ^ cur_bits;

        if xor == 0 {
            writer.write_bit(false); // identical
        } else {
            writer.write_bit(true); // different

            let leading = xor.leading_zeros().min(31) as u8;
            let trailing = xor.trailing_zeros().min(63) as u8;
            let meaningful = 64 - (leading as u8) - trailing;

            writer.write_bits(leading as u64, 5);
            // Store meaningful_bits - 1 in 6 bits (range 1..=64 -> 0..=63)
            writer.write_bits((meaningful - 1) as u64, 6);
            writer.write_bits(xor >> trailing, meaningful);
        }

        prev_bits = cur_bits;
    }

    let bit_data = writer.finish();
    header.extend_from_slice(&bit_data);
    header
}

/// Decode a Gorilla XOR encoded buffer back to the original f64 values.
///
/// Returns an empty Vec if the data is malformed or empty.
pub fn gorilla_decode_f64(data: &[u8]) -> Vec<f64> {
    if data.len() < 4 {
        return Vec::new();
    }

    let count = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if count == 0 {
        return Vec::new();
    }

    if data.len() < 12 {
        return Vec::new();
    }

    let first_bits = u64::from_le_bytes([
        data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
    ]);

    let mut result = Vec::with_capacity(count);
    result.push(f64::from_bits(first_bits));

    if count == 1 {
        return result;
    }

    let mut reader = BitReader::new(data, 12);
    let mut prev_bits = first_bits;

    for _ in 1..count {
        let Some(is_different) = reader.read_bit() else {
            break;
        };

        if !is_different {
            result.push(f64::from_bits(prev_bits));
        } else {
            let Some(leading) = reader.read_bits(5) else {
                break;
            };
            let Some(meaningful_raw) = reader.read_bits(6) else {
                break;
            };
            // Stored as meaningful_bits - 1, so add 1 back
            let meaningful = (meaningful_raw as u8) + 1;
            let Some(meaningful_val) = reader.read_bits(meaningful) else {
                break;
            };
            let trailing = 64 - (leading as u8) - meaningful;
            let xor = meaningful_val << trailing;
            let cur_bits = prev_bits ^ xor;
            result.push(f64::from_bits(cur_bits));
            prev_bits = cur_bits;
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Zigzag helpers --

    #[test]
    fn test_zigzag_roundtrip() {
        for &v in &[0i64, 1, -1, 42, -42, i64::MAX, i64::MIN] {
            assert_eq!(zigzag_decode(zigzag_encode(v)), v);
        }
    }

    // -- Varint helpers --

    #[test]
    fn test_varint_roundtrip() {
        for &v in &[0u64, 1, 127, 128, 16383, 16384, u64::MAX] {
            let mut buf = Vec::new();
            write_varint(&mut buf, v);
            let mut pos = 0;
            assert_eq!(read_varint(&buf, &mut pos), Some(v));
            assert_eq!(pos, buf.len());
        }
    }

    // -- Delta encoding --

    #[test]
    fn test_delta_monotonic_stride1() {
        let input = vec![1000u64, 1001, 1002, 1003];
        let encoded = delta_encode_timestamps(&input);
        let decoded = delta_decode_timestamps(&encoded);
        assert_eq!(decoded, input);
    }

    #[test]
    fn test_delta_varying_strides() {
        let input = vec![0u64, 100, 300, 600, 1200];
        let encoded = delta_encode_timestamps(&input);
        let decoded = delta_decode_timestamps(&encoded);
        assert_eq!(decoded, input);
    }

    #[test]
    fn test_delta_empty() {
        let encoded = delta_encode_timestamps(&[]);
        assert!(encoded.is_empty());
        let decoded = delta_decode_timestamps(&[]);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_delta_single_value() {
        let input = vec![42u64];
        let encoded = delta_encode_timestamps(&input);
        let decoded = delta_decode_timestamps(&encoded);
        assert_eq!(decoded, input);
    }

    #[test]
    fn test_delta_all_same() {
        let input = vec![5u64, 5, 5, 5];
        let encoded = delta_encode_timestamps(&input);
        let decoded = delta_decode_timestamps(&encoded);
        assert_eq!(decoded, input);
        // After header (12 bytes), each dod=0 => zigzag(0)=0 => 1 byte per value
        assert!(encoded.len() <= 12 + 3, "all-same should compress well, got {} bytes", encoded.len());
    }

    #[test]
    fn test_delta_large_delta() {
        let input = vec![0u64, u64::MAX / 2];
        let encoded = delta_encode_timestamps(&input);
        let decoded = delta_decode_timestamps(&encoded);
        assert_eq!(decoded, input);
    }

    #[test]
    fn test_delta_monotonic_compression_ratio() {
        // Constant stride: delta-of-delta should be 0 after first delta
        let base = 1_700_000_000_000u64; // epoch ms
        let input: Vec<u64> = (0..100).map(|i| base + i * 1000).collect();
        let encoded = delta_encode_timestamps(&input);
        let decoded = delta_decode_timestamps(&encoded);
        assert_eq!(decoded, input);
        // 12 bytes header + varint for first delta (~3 bytes) + 98 * 1 byte (dod=0)
        // Should be well under 120 bytes for 100 values (vs 800 raw)
        assert!(encoded.len() < 120, "monotonic timestamps should compress well, got {} bytes", encoded.len());
    }

    // -- Gorilla encoding --

    #[test]
    fn test_gorilla_all_same() {
        let input = vec![1.0f64, 1.0, 1.0, 1.0];
        let encoded = gorilla_encode_f64(&input);
        let decoded = gorilla_decode_f64(&encoded);
        assert_eq!(decoded.len(), input.len());
        for (a, b) in decoded.iter().zip(input.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
        // 12 bytes header + 3 bits (padded to 1 byte) for 3 identical values
        assert!(encoded.len() <= 13, "all-same should compress to ~13 bytes, got {}", encoded.len());
    }

    #[test]
    fn test_gorilla_varying() {
        let input = vec![1.5f64, 2.5, 3.5, 4.5];
        let encoded = gorilla_encode_f64(&input);
        let decoded = gorilla_decode_f64(&encoded);
        assert_eq!(decoded.len(), input.len());
        for (a, b) in decoded.iter().zip(input.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }

    #[test]
    fn test_gorilla_special_values() {
        let input = vec![0.0f64, f64::MAX, f64::MIN, f64::NAN, f64::INFINITY];
        let encoded = gorilla_encode_f64(&input);
        let decoded = gorilla_decode_f64(&encoded);
        assert_eq!(decoded.len(), input.len());
        for (a, b) in decoded.iter().zip(input.iter()) {
            assert_eq!(a.to_bits(), b.to_bits(), "bit-exact mismatch for special value");
        }
    }

    #[test]
    fn test_gorilla_empty() {
        let encoded = gorilla_encode_f64(&[]);
        assert!(encoded.is_empty());
        let decoded = gorilla_decode_f64(&[]);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_gorilla_single() {
        let input = vec![42.0f64];
        let encoded = gorilla_encode_f64(&input);
        let decoded = gorilla_decode_f64(&encoded);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].to_bits(), input[0].to_bits());
    }

    #[test]
    fn test_gorilla_mixed() {
        let input = vec![100.0f64, 100.1, 100.2, 99.8, 100.0];
        let encoded = gorilla_encode_f64(&input);
        let decoded = gorilla_decode_f64(&encoded);
        assert_eq!(decoded.len(), input.len());
        for (a, b) in decoded.iter().zip(input.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }

    #[test]
    fn test_gorilla_bit_exact() {
        // Verify no floating-point drift through encode/decode
        let input: Vec<f64> = (0..50).map(|i| (i as f64) * 0.1).collect();
        let encoded = gorilla_encode_f64(&input);
        let decoded = gorilla_decode_f64(&encoded);
        assert_eq!(decoded.len(), input.len());
        for (i, (a, b)) in decoded.iter().zip(input.iter()).enumerate() {
            assert_eq!(a.to_bits(), b.to_bits(), "bit mismatch at index {i}");
        }
    }

    #[test]
    fn test_gorilla_negative_zero() {
        let input = vec![0.0f64, -0.0, 0.0];
        let encoded = gorilla_encode_f64(&input);
        let decoded = gorilla_decode_f64(&encoded);
        assert_eq!(decoded.len(), input.len());
        for (a, b) in decoded.iter().zip(input.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }
}
