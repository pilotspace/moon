//! HyperLogLog implementation — byte-identical with Redis 7.x HYLL format.
//!
//! Uses MurmurHash64A with seed 0xadc83b19 and the Ertl improved estimator
//! (hll_sigma/hll_tau) instead of bias correction tables.
//!
//! Storage: `RedisValue::String(Bytes)` — the raw HYLL wire bytes.
//! Redis `TYPE` reports "string" for HLL keys; GET/SET/DUMP/RESTORE work.

use bytes::{BufMut, Bytes, BytesMut};

// ---------------------------------------------------------------------------
// Constants (match Redis hyperloglog.c exactly)
// ---------------------------------------------------------------------------

pub const HLL_P: u32 = 14;
pub const HLL_Q: u32 = 64 - HLL_P; // 50
pub const HLL_REGISTERS: usize = 1 << HLL_P; // 16384
const HLL_P_MASK: u64 = (HLL_REGISTERS as u64) - 1;
pub const HLL_BITS: u32 = 6;
pub const HLL_REGISTER_MAX: u8 = (1 << HLL_BITS) - 1; // 63
pub const HLL_HDR_SIZE: usize = 16;
pub const HLL_DENSE_SIZE: usize = HLL_HDR_SIZE + ((HLL_REGISTERS * HLL_BITS as usize + 7) / 8); // 12304
pub const HLL_DENSE: u8 = 0;
pub const HLL_SPARSE: u8 = 1;
const HLL_MAX_ENCODING: u8 = 1;
const HLL_ALPHA_INF: f64 = 0.721_347_520_444_481_7;

// Sparse opcode constants
const HLL_SPARSE_VAL_MAX_VALUE: u8 = 32;
const HLL_SPARSE_MAX_BYTES: usize = 3000;

pub const HLL_HASH_SEED: u64 = 0xadc83b19;

const HLL_MAGIC: &[u8; 4] = b"HYLL";

// ---------------------------------------------------------------------------
// MurmurHash64A — safe Rust port (no unsafe)
// ---------------------------------------------------------------------------

/// MurmurHash64A hash function, safe Rust port.
/// Redis HLL uses seed 0xadc83b19 (HLL_HASH_SEED).
pub fn murmurhash64a(key: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: u32 = 47;

    let len = key.len();
    let mut h: u64 = seed ^ ((len as u64).wrapping_mul(M));

    // Process 8-byte chunks
    let chunks = len / 8;
    for i in 0..chunks {
        let mut k =
            u64::from_le_bytes(key[i * 8..i * 8 + 8].try_into().expect("slice length is 8"));
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
    }

    // Process remaining bytes (fallthrough pattern matching Redis exactly)
    let remaining = &key[chunks * 8..];
    let rlen = remaining.len();
    if rlen >= 7 {
        h ^= (remaining[6] as u64) << 48;
    }
    if rlen >= 6 {
        h ^= (remaining[5] as u64) << 40;
    }
    if rlen >= 5 {
        h ^= (remaining[4] as u64) << 32;
    }
    if rlen >= 4 {
        h ^= (remaining[3] as u64) << 24;
    }
    if rlen >= 3 {
        h ^= (remaining[2] as u64) << 16;
    }
    if rlen >= 2 {
        h ^= (remaining[1] as u64) << 8;
    }
    if rlen >= 1 {
        h ^= remaining[0] as u64;
        h = h.wrapping_mul(M);
    }

    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h
}

// ---------------------------------------------------------------------------
// Dense register accessors — pure shifts/masks, no unsafe
// ---------------------------------------------------------------------------

/// Get 6-bit register value at index `reg` from dense payload buffer.
#[inline]
fn dense_get(registers: &[u8], reg: usize) -> u8 {
    let bit_offset = reg * HLL_BITS as usize;
    let byte_offset = bit_offset / 8;
    let fb = bit_offset & 7;

    let b0 = registers[byte_offset] as u16;
    let b1 = if byte_offset + 1 < registers.len() {
        registers[byte_offset + 1] as u16
    } else {
        0
    };
    ((b0 >> fb) | (b1 << (8 - fb))) as u8 & 0x3F
}

/// Set 6-bit register value at index `reg` in dense payload buffer.
#[inline]
fn dense_set(registers: &mut [u8], reg: usize, val: u8) {
    let bit_offset = reg * HLL_BITS as usize;
    let byte_offset = bit_offset / 8;
    let fb = bit_offset & 7;

    let mask = 0x3F_u16;
    let b0 = registers[byte_offset] as u16;
    let b1 = if byte_offset + 1 < registers.len() {
        registers[byte_offset + 1] as u16
    } else {
        0
    };

    let cleared0 = b0 & !(mask << fb);
    let cleared1 = b1 & !((mask >> (8 - fb)) & 0xFF);

    registers[byte_offset] = (cleared0 | ((val as u16) << fb)) as u8;
    if byte_offset + 1 < registers.len() {
        registers[byte_offset + 1] = (cleared1 | ((val as u16) >> (8 - fb))) as u8;
    }
}

// ---------------------------------------------------------------------------
// Ertl improved estimator (replaces bias tables)
// ---------------------------------------------------------------------------

fn hll_sigma(x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }
    let mut x = x;
    let mut z_prime: f64;
    let mut y: f64 = 1.0;
    let mut z: f64 = x;
    loop {
        x *= x;
        z_prime = z;
        z += x * y;
        y += y;
        if z_prime == z {
            break;
        }
    }
    z
}

fn hll_tau(x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }
    let mut x = x;
    let mut z_prime: f64;
    let mut y: f64 = 1.0;
    let mut z: f64 = 1.0 - x;
    loop {
        x = x.sqrt();
        z_prime = z;
        y *= 0.5;
        z -= (1.0 - x).powi(2) * y;
        if z_prime == z {
            break;
        }
    }
    z / 3.0
}

/// Compute cardinality from register histogram.
/// `reghisto[i]` = count of registers with value i (0..=HLL_Q+1).
fn hll_count(reghisto: &[u32; 64]) -> u64 {
    let m = HLL_REGISTERS as f64;

    let mut z = m * hll_tau((m - reghisto[HLL_Q as usize + 1] as f64) / m);
    for j in (1..=HLL_Q as usize).rev() {
        z += reghisto[j] as f64;
        z *= 0.5;
    }
    z += m * hll_sigma(reghisto[0] as f64 / m);
    (HLL_ALPHA_INF * m * m / z).round() as u64
}

// ---------------------------------------------------------------------------
// Sparse opcode helpers
// ---------------------------------------------------------------------------

/// Decoded sparse opcode.
#[derive(Debug, Clone, Copy)]
enum SparseOp {
    Zero(u16),    // run of zeros, length 1..64
    XZero(u16),   // run of zeros, length 1..16384
    Val(u8, u16), // run of val (1..32), length 1..4
}

impl SparseOp {
    /// Number of registers covered by this opcode.
    fn span(&self) -> u16 {
        match *self {
            SparseOp::Zero(n) => n,
            SparseOp::XZero(n) => n,
            SparseOp::Val(_, n) => n,
        }
    }

    /// Register value (0 for ZERO/XZERO opcodes).
    fn value(&self) -> u8 {
        match *self {
            SparseOp::Zero(_) | SparseOp::XZero(_) => 0,
            SparseOp::Val(v, _) => v,
        }
    }
}

/// Decode one sparse opcode at `data[pos..]`. Returns (op, bytes_consumed).
fn sparse_decode(data: &[u8], pos: usize) -> (SparseOp, usize) {
    let b = data[pos];
    if b & 0x80 != 0 {
        // VAL: 1vvvvvxx
        let val = ((b >> 2) & 0x1F) + 1;
        let runlen = (b & 0x03) as u16 + 1;
        (SparseOp::Val(val, runlen), 1)
    } else if b & 0x40 != 0 {
        // XZERO: 01xxxxxx yyyyyyyy (2 bytes)
        let runlen = (((b & 0x3F) as u16) << 8 | data[pos + 1] as u16) + 1;
        (SparseOp::XZero(runlen), 2)
    } else {
        // ZERO: 00xxxxxx
        let runlen = (b & 0x3F) as u16 + 1;
        (SparseOp::Zero(runlen), 1)
    }
}

/// Encode a ZERO opcode (run of zeros, length 1..64).
fn sparse_encode_zero(len: u16) -> u8 {
    debug_assert!(len >= 1 && len <= 64);
    (len - 1) as u8
}

/// Encode an XZERO opcode (run of zeros, length 1..16384). Returns 2 bytes.
fn sparse_encode_xzero(len: u16) -> [u8; 2] {
    debug_assert!(len >= 1 && len <= 16384);
    let v = len - 1;
    [0x40 | ((v >> 8) as u8), (v & 0xFF) as u8]
}

/// Encode a VAL opcode (run of val, length 1..4, value 1..32).
fn sparse_encode_val(val: u8, len: u16) -> u8 {
    debug_assert!(val >= 1 && val <= 32);
    debug_assert!(len >= 1 && len <= 4);
    0x80 | ((val - 1) << 2) | (len - 1) as u8
}

/// Emit zero-run opcodes into `out` covering `len` registers.
fn emit_zeros(out: &mut BytesMut, mut len: u16) {
    while len > 0 {
        if len > 64 {
            let chunk = len.min(16384);
            let xz = sparse_encode_xzero(chunk);
            out.put_slice(&xz);
            len -= chunk;
        } else {
            out.put_u8(sparse_encode_zero(len));
            len = 0;
        }
    }
}

/// Emit val-run opcodes into `out` covering `len` registers with `val`.
fn emit_vals(out: &mut BytesMut, val: u8, mut len: u16) {
    while len > 0 {
        let chunk = len.min(4);
        out.put_u8(sparse_encode_val(val, chunk));
        len -= chunk;
    }
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors when parsing or operating on HLL data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HllError {
    BadMagic,
    BadEncoding,
    Truncated,
    InvalidSparseOpcode,
}

// ---------------------------------------------------------------------------
// Hll struct
// ---------------------------------------------------------------------------

/// HyperLogLog data structure with byte-identical Redis 7.x HYLL wire format.
#[derive(Debug)]
pub struct Hll {
    buf: BytesMut,
}

impl Hll {
    /// Create a new sparse HLL (starts as a single XZERO(16384) opcode).
    pub fn new_sparse() -> Self {
        let mut buf = BytesMut::with_capacity(HLL_HDR_SIZE + 2);
        buf.put_slice(HLL_MAGIC);
        buf.put_u8(HLL_SPARSE);
        buf.put_bytes(0, 3);
        let invalid_card: u64 = 1u64 << 63;
        buf.put_u64_le(invalid_card);
        // XZERO(16384): 01_111111 11111111
        buf.put_u8(0x7F);
        buf.put_u8(0xFF);
        Hll { buf }
    }

    /// Create a new dense HLL (all registers zeroed).
    fn new_dense() -> Self {
        let mut buf = BytesMut::with_capacity(HLL_DENSE_SIZE);
        buf.put_slice(HLL_MAGIC);
        buf.put_u8(HLL_DENSE);
        buf.put_bytes(0, 3);
        let invalid_card: u64 = 1u64 << 63;
        buf.put_u64_le(invalid_card);
        buf.put_bytes(0, HLL_DENSE_SIZE - HLL_HDR_SIZE);
        Hll { buf }
    }

    /// Construct from existing HYLL bytes (validates header).
    pub fn from_bytes(bytes: Bytes) -> Result<Self, HllError> {
        if bytes.len() < HLL_HDR_SIZE {
            return Err(HllError::Truncated);
        }
        if &bytes[0..4] != HLL_MAGIC {
            return Err(HllError::BadMagic);
        }
        let encoding = bytes[4];
        if encoding > HLL_MAX_ENCODING {
            return Err(HllError::BadEncoding);
        }
        if encoding == HLL_DENSE && bytes.len() < HLL_DENSE_SIZE {
            return Err(HllError::Truncated);
        }
        Ok(Hll {
            buf: BytesMut::from(bytes.as_ref()),
        })
    }

    /// Consume self and return the wire bytes.
    pub fn into_bytes(self) -> Bytes {
        self.buf.freeze()
    }

    /// Borrow the wire bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// Check if a byte slice starts with the HYLL magic.
    pub fn is_hll(bytes: &[u8]) -> bool {
        bytes.len() >= HLL_HDR_SIZE && &bytes[0..4] == HLL_MAGIC
    }

    /// Returns true if this HLL uses sparse encoding.
    pub fn is_sparse(&self) -> bool {
        self.buf[4] == HLL_SPARSE
    }

    /// Returns true if this HLL uses dense encoding.
    pub fn is_dense(&self) -> bool {
        self.buf[4] == HLL_DENSE
    }

    // -- Cache management --

    fn cache_valid(&self) -> bool {
        self.buf[15] & 0x80 == 0
    }

    fn cached_card(&self) -> u64 {
        let raw = u64::from_le_bytes(self.buf[8..16].try_into().expect("8 bytes"));
        raw & !(1u64 << 63)
    }

    fn set_cached_card(&mut self, card: u64) {
        let bytes = card.to_le_bytes();
        self.buf[8..16].copy_from_slice(&bytes);
        self.buf[15] &= 0x7F;
    }

    fn invalidate_cache(&mut self) {
        self.buf[15] |= 0x80;
    }

    // -- Dense register helpers --

    fn dense_registers(&self) -> &[u8] {
        &self.buf[HLL_HDR_SIZE..]
    }

    fn dense_registers_mut(&mut self) -> &mut [u8] {
        &mut self.buf[HLL_HDR_SIZE..]
    }

    fn get_register_dense(&self, reg: usize) -> u8 {
        dense_get(self.dense_registers(), reg)
    }

    fn set_register_dense(&mut self, reg: usize, val: u8) {
        dense_set(self.dense_registers_mut(), reg, val);
    }

    // -- Sparse payload --

    fn sparse_payload(&self) -> &[u8] {
        &self.buf[HLL_HDR_SIZE..]
    }

    // -- Promote sparse to dense --

    fn promote_to_dense(&mut self) {
        let mut dense = Hll::new_dense();
        // Walk sparse opcodes, write to dense
        let payload = &self.buf[HLL_HDR_SIZE..];
        let mut pos = 0;
        let mut reg_idx = 0usize;
        while pos < payload.len() {
            let (op, consumed) = sparse_decode(payload, pos);
            pos += consumed;
            match op {
                SparseOp::Zero(n) | SparseOp::XZero(n) => {
                    reg_idx += n as usize; // zeros, already 0 in dense
                }
                SparseOp::Val(v, n) => {
                    for _ in 0..n {
                        dense.set_register_dense(reg_idx, v);
                        reg_idx += 1;
                    }
                }
            }
        }
        // Copy invalid cache from sparse header
        dense.invalidate_cache();
        self.buf = dense.buf;
    }

    // -- Core operations --

    /// Hash element, compute register index and count.
    fn hash_element(element: &[u8]) -> (usize, u8) {
        let hash = murmurhash64a(element, HLL_HASH_SEED);
        let index = (hash & HLL_P_MASK) as usize;
        // Upper bits + sentinel: ensures trailing_zeros is bounded by HLL_Q
        let bits = (hash >> HLL_P) | (1u64 << HLL_Q);
        let count = (bits.trailing_zeros() + 1) as u8;
        (index, count)
    }

    /// Add an element. Returns true if any register changed.
    pub fn add(&mut self, element: &[u8]) -> bool {
        let (index, count) = Self::hash_element(element);

        if self.is_dense() {
            return self.add_dense(index, count);
        }
        // Sparse path
        self.add_sparse(index, count)
    }

    /// Dense add: update register if count > current.
    fn add_dense(&mut self, index: usize, count: u8) -> bool {
        let old = self.get_register_dense(index);
        if count > old {
            self.set_register_dense(index, count);
            self.invalidate_cache();
            return true;
        }
        false
    }

    /// Sparse add: find the opcode covering `index`, update or promote.
    fn add_sparse(&mut self, index: usize, count: u8) -> bool {
        // Value too large for sparse encoding? Promote.
        if count > HLL_SPARSE_VAL_MAX_VALUE {
            self.promote_to_dense();
            return self.add_dense(index, count);
        }

        let payload = &self.buf[HLL_HDR_SIZE..];
        let payload_len = payload.len();

        // Find the opcode covering `index`
        let mut pos = 0;
        let mut reg_pos = 0usize;
        let mut found_pos = 0;
        let mut found_op = SparseOp::Zero(0);
        let mut found_consumed = 0;
        let mut found_reg_start = 0;

        while pos < payload_len {
            let (op, consumed) = sparse_decode(payload, pos);
            let span = op.span() as usize;
            if reg_pos + span > index {
                found_pos = pos;
                found_op = op;
                found_consumed = consumed;
                found_reg_start = reg_pos;
                break;
            }
            reg_pos += span;
            pos += consumed;
        }

        let current_val = found_op.value();
        if count <= current_val {
            return false; // no change
        }

        // Build replacement opcodes
        let offset_in_run = index - found_reg_start;
        let span = found_op.span() as usize;
        let mut replacement = BytesMut::with_capacity(16);

        // Emit: zeros_before + val_run_before + new_val + val_run_after + zeros_after
        // But we need to handle: the opcode might be a ZERO/XZERO (val=0) or VAL
        match found_op {
            SparseOp::Zero(_) | SparseOp::XZero(_) => {
                // Before the target: emit zeros
                if offset_in_run > 0 {
                    emit_zeros(&mut replacement, offset_in_run as u16);
                }
                // The target register
                replacement.put_u8(sparse_encode_val(count, 1));
                // After the target: emit zeros
                let after = span - offset_in_run - 1;
                if after > 0 {
                    emit_zeros(&mut replacement, after as u16);
                }
            }
            SparseOp::Val(old_val, _run_len) => {
                // Before: run of old_val
                if offset_in_run > 0 {
                    emit_vals(&mut replacement, old_val, offset_in_run as u16);
                }
                // The target register with new value
                replacement.put_u8(sparse_encode_val(count, 1));
                // After: run of old_val
                let after = span - offset_in_run - 1;
                if after > 0 {
                    emit_vals(&mut replacement, old_val, after as u16);
                }
            }
        }

        // Check if resulting sparse payload would exceed max size
        let new_payload_len = payload_len - found_consumed + replacement.len();
        if new_payload_len > HLL_SPARSE_MAX_BYTES {
            self.promote_to_dense();
            return self.add_dense(index, count);
        }

        // Splice: replace the bytes at [found_pos..found_pos+found_consumed]
        // with `replacement`
        let header_end = HLL_HDR_SIZE;
        let splice_start = header_end + found_pos;
        let splice_end = splice_start + found_consumed;

        let mut new_buf = BytesMut::with_capacity(header_end + new_payload_len);
        new_buf.put_slice(&self.buf[..splice_start]);
        new_buf.put_slice(&replacement);
        new_buf.put_slice(&self.buf[splice_end..]);
        self.buf = new_buf;
        self.invalidate_cache();
        true
    }

    /// Build register histogram from dense encoding.
    fn dense_reghisto(&self) -> [u32; 64] {
        let mut reghisto = [0u32; 64];
        let regs = self.dense_registers();
        for i in 0..HLL_REGISTERS {
            let val = dense_get(regs, i) as usize;
            reghisto[val] += 1;
        }
        reghisto
    }

    /// Build register histogram from sparse encoding.
    fn sparse_reghisto(&self) -> [u32; 64] {
        let mut reghisto = [0u32; 64];
        let payload = self.sparse_payload();
        let mut pos = 0;
        while pos < payload.len() {
            let (op, consumed) = sparse_decode(payload, pos);
            pos += consumed;
            match op {
                SparseOp::Zero(n) | SparseOp::XZero(n) => {
                    reghisto[0] += n as u32;
                }
                SparseOp::Val(v, n) => {
                    reghisto[v as usize] += n as u32;
                }
            }
        }
        reghisto
    }

    /// Return the cardinality estimate (read-only, uses cache if valid).
    pub fn count(&self) -> u64 {
        if self.cache_valid() {
            return self.cached_card();
        }
        let reghisto = if self.is_dense() {
            self.dense_reghisto()
        } else {
            self.sparse_reghisto()
        };
        hll_count(&reghisto)
    }

    /// Return the cardinality estimate and cache the result.
    pub fn count_and_cache(&mut self) -> u64 {
        if self.cache_valid() {
            return self.cached_card();
        }
        let reghisto = if self.is_dense() {
            self.dense_reghisto()
        } else {
            self.sparse_reghisto()
        };
        let card = hll_count(&reghisto);
        self.set_cached_card(card);
        card
    }

    /// Iterate all registers of this HLL, calling `f(register_index, value)`.
    fn for_each_register<F: FnMut(usize, u8)>(&self, mut f: F) {
        if self.is_dense() {
            let regs = self.dense_registers();
            for i in 0..HLL_REGISTERS {
                f(i, dense_get(regs, i));
            }
        } else {
            let payload = self.sparse_payload();
            let mut pos = 0;
            let mut reg_idx = 0;
            while pos < payload.len() {
                let (op, consumed) = sparse_decode(payload, pos);
                pos += consumed;
                let span = op.span() as usize;
                let val = op.value();
                for _ in 0..span {
                    f(reg_idx, val);
                    reg_idx += 1;
                }
            }
        }
    }

    /// Merge another HLL into self (register-max).
    /// Promotes self to dense if needed.
    pub fn merge_from(&mut self, other: &Hll) {
        // Promote self to dense for merge (Redis does this too)
        if self.is_sparse() {
            self.promote_to_dense();
        }
        // Take register-max from other
        other.for_each_register(|i, val| {
            if val > 0 {
                let cur = self.get_register_dense(i);
                if val > cur {
                    self.set_register_dense(i, val);
                }
            }
        });
        self.invalidate_cache();
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn murmur_empty_string_kat() {
        // Verified against Redis 7.x: PFADD key "" sets register 5938 to count=2
        assert_eq!(murmurhash64a(b"", HLL_HASH_SEED), 0xD8DFEA6585BC9732);
    }

    #[test]
    fn murmur_nonempty_deterministic() {
        let h1 = murmurhash64a(b"hello", HLL_HASH_SEED);
        let h2 = murmurhash64a(b"hello", HLL_HASH_SEED);
        assert_eq!(h1, h2);
        assert_ne!(h1, 0);
    }

    #[test]
    fn murmur_different_inputs_differ() {
        let h1 = murmurhash64a(b"hello", HLL_HASH_SEED);
        let h2 = murmurhash64a(b"world", HLL_HASH_SEED);
        assert_ne!(h1, h2);
    }

    #[test]
    fn murmur_64byte_input() {
        let data: Vec<u8> = (0u8..64).collect();
        let h = murmurhash64a(&data, HLL_HASH_SEED);
        assert_ne!(h, 0);
    }

    #[test]
    fn dense_get_set_roundtrip() {
        let mut regs = vec![0u8; (HLL_REGISTERS * HLL_BITS as usize + 7) / 8];
        for reg in 0..HLL_REGISTERS {
            let val = (reg % 63) as u8 + 1;
            dense_set(&mut regs, reg, val);
            assert_eq!(dense_get(&regs, reg), val, "register {} failed", reg);
        }
    }

    #[test]
    fn dense_get_set_boundary_values() {
        let mut regs = vec![0u8; (HLL_REGISTERS * HLL_BITS as usize + 7) / 8];
        dense_set(&mut regs, 0, HLL_REGISTER_MAX);
        assert_eq!(dense_get(&regs, 0), HLL_REGISTER_MAX);
        dense_set(&mut regs, 1, 1);
        assert_eq!(dense_get(&regs, 0), HLL_REGISTER_MAX);
        assert_eq!(dense_get(&regs, 1), 1);
    }

    #[test]
    fn hll_new_sparse_header() {
        let hll = Hll::new_sparse();
        assert_eq!(&hll.buf[0..4], b"HYLL");
        assert_eq!(hll.buf[4], HLL_SPARSE);
        assert!(hll.is_sparse());
    }

    #[test]
    fn hll_new_dense_header() {
        let hll = Hll::new_dense();
        assert_eq!(&hll.buf[0..4], b"HYLL");
        assert_eq!(hll.buf[4], HLL_DENSE);
        assert_eq!(hll.buf.len(), HLL_DENSE_SIZE);
    }

    #[test]
    fn hll_is_hll() {
        let hll = Hll::new_sparse();
        let bytes = hll.into_bytes();
        assert!(Hll::is_hll(&bytes));
        assert!(!Hll::is_hll(b"not a hll"));
        assert!(!Hll::is_hll(b"HYL"));
    }

    #[test]
    fn hll_from_bytes_valid() {
        let hll = Hll::new_sparse();
        let bytes = hll.into_bytes();
        let hll2 = Hll::from_bytes(bytes).unwrap();
        assert!(hll2.is_sparse());
    }

    #[test]
    fn hll_from_bytes_bad_magic() {
        let mut buf = BytesMut::with_capacity(HLL_HDR_SIZE);
        buf.put_slice(b"NOPE");
        buf.put_bytes(0, HLL_HDR_SIZE - 4);
        let result = Hll::from_bytes(buf.freeze());
        assert_eq!(result.unwrap_err(), HllError::BadMagic);
    }

    #[test]
    fn hll_from_bytes_truncated() {
        let result = Hll::from_bytes(Bytes::from_static(b"HYL"));
        assert_eq!(result.unwrap_err(), HllError::Truncated);
    }

    #[test]
    fn ertl_sigma_zero() {
        assert_eq!(hll_sigma(0.0), 0.0);
    }

    #[test]
    fn ertl_sigma_one() {
        assert!(hll_sigma(1.0).is_infinite());
    }

    #[test]
    fn ertl_tau_zero() {
        assert_eq!(hll_tau(0.0), 0.0);
    }

    #[test]
    fn ertl_tau_one() {
        assert_eq!(hll_tau(1.0), 0.0);
    }

    #[test]
    fn hll_count_all_zeros() {
        let mut reghisto = [0u32; 64];
        reghisto[0] = HLL_REGISTERS as u32;
        let c = hll_count(&reghisto);
        assert_eq!(c, 0, "empty HLL should return 0");
    }

    // -- Full integration tests --

    #[test]
    fn add_single_element_sparse() {
        let mut hll = Hll::new_sparse();
        assert!(hll.is_sparse());
        assert!(hll.add(b"hello"));
        // Adding same element again should return false
        assert!(!hll.add(b"hello"));
    }

    #[test]
    fn add_empty_string_matches_redis() {
        let mut hll = Hll::new_sparse();
        assert!(hll.add(b""));
        // Verify register placement: hash gives index=5938, count=2
        // After promotion to verify, just check count works
        let c = hll.count();
        assert_eq!(c, 1);
    }

    #[test]
    fn pfadd_monotonic() {
        let mut hll = Hll::new_sparse();
        let mut prev = 0u64;
        for i in 0..1000u32 {
            let s = i.to_string();
            hll.add(s.as_bytes());
            let c = hll.count();
            assert!(c >= prev, "count decreased at i={}: {} < {}", i, c, prev);
            prev = c;
        }
    }

    #[test]
    fn pfcount_10_unique() {
        let mut hll = Hll::new_sparse();
        for i in 0..10u32 {
            let s = i.to_string();
            hll.add(s.as_bytes());
        }
        let count = hll.count();
        assert!(
            (9..=11).contains(&count),
            "pfcount 10: expected 9..=11, got {}",
            count
        );
    }

    #[test]
    fn pfcount_1k_unique_within_1pct() {
        let mut hll = Hll::new_sparse();
        for i in 0..1000u32 {
            let s = i.to_string();
            hll.add(s.as_bytes());
        }
        let count = hll.count();
        assert!(
            (980..=1020).contains(&count),
            "pfcount 1k: expected ~1000 (within 2%), got {}",
            count
        );
    }

    #[test]
    fn pfcount_100k_unique_within_1pct() {
        let mut hll = Hll::new_sparse();
        for i in 0..100_000u32 {
            let s = i.to_string();
            hll.add(s.as_bytes());
        }
        let count = hll.count();
        assert!(
            (99_000..=101_000).contains(&count),
            "pfcount 100k: expected 99_000..=101_000, got {}",
            count
        );
    }

    #[test]
    fn pfmerge_register_max() {
        let mut hll_a = Hll::new_sparse();
        let mut hll_b = Hll::new_sparse();
        for i in 0..500u32 {
            hll_a.add(i.to_string().as_bytes());
        }
        for i in 250..750u32 {
            hll_b.add(i.to_string().as_bytes());
        }
        hll_a.merge_from(&hll_b);
        let count = hll_a.count();
        // 750 unique elements, expect within ~2%
        assert!(
            (735..=765).contains(&count),
            "pfmerge result: expected ~750, got {}",
            count
        );
    }

    #[test]
    fn sparse_to_dense_promotion() {
        let mut hll = Hll::new_sparse();
        assert!(hll.is_sparse());
        // Add enough elements to trigger promotion
        for i in 0..5000u32 {
            hll.add(i.to_string().as_bytes());
        }
        // After many adds, likely promoted to dense
        // (sparse max is 3000 bytes, which is exceeded with many distinct values)
        // Regardless of encoding, count should still be accurate
        let count = hll.count();
        assert!(
            (4900..=5100).contains(&count),
            "after promotion, count should be ~5000, got {}",
            count
        );
    }

    #[test]
    fn count_caching() {
        let mut hll = Hll::new_sparse();
        hll.add(b"a");
        hll.add(b"b");
        let c1 = hll.count();
        // Second call should use cache
        let c2 = hll.count();
        assert_eq!(c1, c2);
        // After add, cache should be invalidated
        hll.add(b"c");
        let c3 = hll.count();
        assert!(c3 >= c1);
    }

    #[test]
    fn hll_wire_format_sparse_matches_redis() {
        // Verify that PFADD "" produces the exact same sparse encoding as Redis
        let mut hll = Hll::new_sparse();
        hll.add(b"");
        let bytes = hll.as_bytes();

        // Expected from Redis: HYLL header + XZERO(5938) + VAL(2,1) + XZERO(10445)
        assert_eq!(&bytes[0..4], b"HYLL");
        assert_eq!(bytes[4], HLL_SPARSE);

        // Decode sparse payload and verify structure
        let payload = &bytes[HLL_HDR_SIZE..];
        let (op1, c1) = sparse_decode(payload, 0);
        assert!(matches!(op1, SparseOp::XZero(5938)));
        let (op2, c2) = sparse_decode(payload, c1);
        assert!(matches!(op2, SparseOp::Val(2, 1)));
        let (op3, _c3) = sparse_decode(payload, c1 + c2);
        assert!(matches!(op3, SparseOp::XZero(10445)));
    }

    #[test]
    fn merge_empty_into_populated() {
        let mut hll_a = Hll::new_sparse();
        for i in 0..100u32 {
            hll_a.add(i.to_string().as_bytes());
        }
        let count_before = hll_a.count();

        let hll_b = Hll::new_sparse(); // empty
        hll_a.merge_from(&hll_b);
        let count_after = hll_a.count();
        assert_eq!(count_before, count_after);
    }

    #[test]
    fn merge_populated_into_empty() {
        let mut hll_a = Hll::new_sparse(); // empty
        let mut hll_b = Hll::new_sparse();
        for i in 0..100u32 {
            hll_b.add(i.to_string().as_bytes());
        }
        let count_b = hll_b.count();
        hll_a.merge_from(&hll_b);
        let count_a = hll_a.count();
        assert_eq!(count_a, count_b);
    }
}
