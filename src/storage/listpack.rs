use bytes::Bytes;
use std::collections::{HashMap, VecDeque};

const LP_HDR_SIZE: usize = 7; // 4 bytes total_bytes + 2 bytes num_elements + 1 byte terminator
const LP_TERMINATOR: u8 = 0xFF;

// Encoding prefix masks (reserved for future decode-path use)
#[allow(dead_code)]
const LP_ENCODING_7BIT_UINT_MASK: u8 = 0x80;
#[allow(dead_code)]
const LP_ENCODING_6BIT_STR_MASK: u8 = 0xC0;
#[allow(dead_code)]
const LP_ENCODING_13BIT_INT_MASK: u8 = 0xE0;
#[allow(dead_code)]
const LP_ENCODING_12BIT_STR_MASK: u8 = 0xF0;
const LP_ENCODING_16BIT_INT: u8 = 0xF1;
const LP_ENCODING_24BIT_INT: u8 = 0xF2;
const LP_ENCODING_32BIT_INT: u8 = 0xF3;
const LP_ENCODING_64BIT_INT: u8 = 0xF4;
const LP_ENCODING_32BIT_STR: u8 = 0xF0;

/// A compact byte-array encoding for small collections.
///
/// Format: [total_bytes: u32 LE][num_elements: u16 LE][entries...][0xFF terminator]
/// Each entry: [encoding_byte(s)][data][backlen]
#[derive(Debug, Clone)]
pub struct Listpack {
    data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListpackEntry {
    Integer(i64),
    String(Vec<u8>),
}

impl ListpackEntry {
    /// Serialize to bytes representation.
    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            ListpackEntry::Integer(v) => v.to_string().into_bytes(),
            ListpackEntry::String(s) => s.clone(),
        }
    }

    /// Convert to Bytes.
    pub fn to_bytes(&self) -> Bytes {
        Bytes::from(self.as_bytes())
    }
}

/// A borrowed view of one listpack entry.
///
/// [`ListpackEntry`] owns its bytes: decoding a string entry copies it into a
/// fresh `Vec`. That is the right shape when the caller keeps the value, but
/// every hot-path *lookup* -- HSET locating a field, SADD testing membership,
/// SISMEMBER -- decoded, allocated, compared, and immediately dropped the
/// copy. Scanning a 128-entry hash cost several hundred malloc/free pairs to
/// answer one boolean.
///
/// `ListpackRef` borrows instead, so a scan touches no allocator at all.
/// Integers are still returned as integers: they are stored decoded, and
/// [`ListpackRef::eq_bytes`] renders them into a stack buffer to compare.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListpackRef<'a> {
    Integer(i64),
    Str(&'a [u8]),
}

impl ListpackRef<'_> {
    /// Compare against a byte string without allocating.
    ///
    /// An integer entry matches only its CANONICAL decimal spelling, which is
    /// the same rule the encoder applies -- `7` is stored as an integer, so it
    /// must answer to `b"7"`, while `007` and `+7` were never integer-encoded
    /// in the first place and must not match one that was.
    #[inline]
    pub fn eq_bytes(&self, other: &[u8]) -> bool {
        match self {
            ListpackRef::Str(s) => *s == other,
            ListpackRef::Integer(v) => {
                let mut buf = itoa::Buffer::new();
                buf.format(*v).as_bytes() == other
            }
        }
    }

    /// Materialize the entry. Only for callers that keep the bytes -- a lookup
    /// should use [`ListpackRef::eq_bytes`] and stay allocation-free.
    pub fn to_vec(&self) -> Vec<u8> {
        match self {
            ListpackRef::Str(s) => s.to_vec(),
            ListpackRef::Integer(v) => {
                let mut buf = itoa::Buffer::new();
                buf.format(*v).as_bytes().to_vec()
            }
        }
    }
}

/// Forward iterator over borrowed listpack entries.
pub struct ListpackRefIter<'a> {
    data: &'a [u8],
    pos: usize,
    remaining: usize,
}

/// Pair iterator over borrowed field/value entries.
pub struct ListpackPairRefIter<'a> {
    inner: ListpackRefIter<'a>,
}

/// Forward iterator over listpack entries.
pub struct ListpackIter<'a> {
    data: &'a [u8],
    pos: usize,
    remaining: usize,
}

/// Reverse iterator over listpack entries.
pub struct ListpackRevIter<'a> {
    data: &'a [u8],
    pos: usize, // points to byte before current entry's backlen end (i.e., the last byte of backlen)
    remaining: usize,
}

/// Pair iterator for hash field-value pairs.
pub struct ListpackPairIter<'a> {
    inner: ListpackIter<'a>,
}

impl Listpack {
    /// Create a new empty listpack.
    pub fn new() -> Self {
        let mut data = Vec::with_capacity(LP_HDR_SIZE);
        // total_bytes: u32 LE
        data.extend_from_slice(&(LP_HDR_SIZE as u32).to_le_bytes());
        // num_elements: u16 LE
        data.extend_from_slice(&0u16.to_le_bytes());
        // terminator
        data.push(LP_TERMINATOR);
        Listpack { data }
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        u16::from_le_bytes([self.data[4], self.data[5]]) as usize
    }

    /// Whether the listpack is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Total bytes consumed by the listpack.
    pub fn total_bytes(&self) -> usize {
        self.data.len()
    }

    /// Append an entry at the end.
    pub fn push_back(&mut self, value: &[u8]) {
        let encoded = encode_entry(value);
        let insert_pos = self.data.len() - 1; // before terminator
        self.data
            .splice(insert_pos..insert_pos, encoded.iter().cloned());
        self.update_header();
    }

    /// Prepend an entry at the front.
    pub fn push_front(&mut self, value: &[u8]) {
        let encoded = encode_entry(value);
        let insert_pos = 6; // after header (4 + 2)
        self.data
            .splice(insert_pos..insert_pos, encoded.iter().cloned());
        self.update_header();
    }

    /// Get entry at a given index (O(N) scan).
    pub fn get_at(&self, index: usize) -> Option<ListpackEntry> {
        let mut pos = 6; // start after header
        for i in 0..=index {
            if pos >= self.data.len() - 1 || self.data[pos] == LP_TERMINATOR {
                return None;
            }
            let (entry, next_pos) = decode_entry_at(&self.data, pos);
            if i == index {
                return Some(entry);
            }
            pos = next_pos;
        }
        None
    }

    /// Remove entry at a given index. Returns true if removed.
    pub fn remove_at(&mut self, index: usize) -> bool {
        let mut pos = 6;
        for i in 0..=index {
            if pos >= self.data.len() - 1 || self.data[pos] == LP_TERMINATOR {
                return false;
            }
            let (_entry, next_pos) = decode_entry_at(&self.data, pos);
            if i == index {
                self.data.drain(pos..next_pos);
                self.update_header_dec();
                return true;
            }
            pos = next_pos;
        }
        false
    }

    /// Replace entry at a given index with a new value.
    pub fn replace_at(&mut self, index: usize, value: &[u8]) {
        let mut pos = 6;
        for i in 0..=index {
            if pos >= self.data.len() - 1 || self.data[pos] == LP_TERMINATOR {
                return;
            }
            let (_entry, next_pos) = decode_entry_at(&self.data, pos);
            if i == index {
                let encoded = encode_entry(value);
                self.data.splice(pos..next_pos, encoded.iter().cloned());
                // Update total_bytes in header (len unchanged)
                let total = self.data.len() as u32;
                self.data[0..4].copy_from_slice(&total.to_le_bytes());
                return;
            }
            pos = next_pos;
        }
    }

    /// Find the index of the first entry matching value.
    pub fn find(&self, value: &[u8]) -> Option<usize> {
        let target = make_entry_for_compare(value);
        for (i, entry) in self.iter().enumerate() {
            if entry == target {
                return Some(i);
            }
        }
        None
    }

    /// Forward iterator.
    pub fn iter(&self) -> ListpackIter<'_> {
        ListpackIter {
            data: &self.data,
            pos: 6,
            remaining: self.len(),
        }
    }

    /// Reverse iterator.
    pub fn iter_rev(&self) -> ListpackRevIter<'_> {
        ListpackRevIter {
            data: &self.data,
            pos: self.data.len() - 1, // terminator position
            remaining: self.len(),
        }
    }

    /// Forward iterator yielding BORROWED entries -- no allocation per entry.
    pub fn iter_refs(&self) -> ListpackRefIter<'_> {
        ListpackRefIter {
            data: &self.data,
            pos: 6,
            remaining: self.len(),
        }
    }

    /// Pair iterator yielding borrowed (field, value) entries.
    pub fn iter_pair_refs(&self) -> ListpackPairRefIter<'_> {
        ListpackPairRefIter {
            inner: self.iter_refs(),
        }
    }

    /// Index of the pair whose FIELD equals `field`, or `None`.
    ///
    /// The returned index counts PAIRS, so the field sits at raw entry
    /// `i * 2` and its value at `i * 2 + 1` -- which is what the HSET write
    /// path needs in order to call `replace_at`. Values are skipped, so a
    /// value that happens to equal `field` never produces a false hit.
    pub fn find_pair_index(&self, field: &[u8]) -> Option<usize> {
        for (i, (f, _v)) in self.iter_pair_refs().enumerate() {
            if f.eq_bytes(field) {
                return Some(i);
            }
        }
        None
    }

    /// Whether any entry equals `value`, without allocating.
    pub fn contains_element(&self, value: &[u8]) -> bool {
        self.iter_refs().any(|e| e.eq_bytes(value))
    }

    /// Iterate as (field, value) pairs for hash usage.
    pub fn iter_pairs(&self) -> ListpackPairIter<'_> {
        ListpackPairIter { inner: self.iter() }
    }

    /// Iterate as (score, member) pairs for sorted set usage.
    pub fn iter_score_member_pairs(&self) -> ListpackPairIter<'_> {
        self.iter_pairs()
    }

    /// Convert to a Vec of Bytes.
    pub fn to_vec(&self) -> Vec<Bytes> {
        self.iter().map(|e| e.to_bytes()).collect()
    }

    /// Convert to a HashMap from alternating field/value entries.
    pub fn to_hash_map(&self) -> HashMap<Bytes, Bytes> {
        let mut map = HashMap::new();
        let mut iter = self.iter();
        while let Some(field) = iter.next() {
            if let Some(value) = iter.next() {
                map.insert(field.to_bytes(), value.to_bytes());
            }
        }
        map
    }

    /// Convert to a HashSet.
    /// Promote to the full set representation.
    pub fn to_set_value(&self) -> crate::storage::entry::SetValue {
        self.iter().map(|e| e.to_bytes()).collect()
    }

    /// Convert to a VecDeque.
    pub fn to_vec_deque(&self) -> VecDeque<Bytes> {
        self.iter().map(|e| e.to_bytes()).collect()
    }

    /// Estimate memory usage.
    pub fn estimate_memory(&self) -> usize {
        std::mem::size_of::<Self>() + self.data.capacity()
    }

    // --- Internal helpers ---

    fn update_header(&mut self) {
        let total = self.data.len() as u32;
        self.data[0..4].copy_from_slice(&total.to_le_bytes());
        // Increment element count
        let count = u16::from_le_bytes([self.data[4], self.data[5]]);
        let new_count = count.wrapping_add(1);
        self.data[4..6].copy_from_slice(&new_count.to_le_bytes());
    }

    fn update_header_dec(&mut self) {
        let total = self.data.len() as u32;
        self.data[0..4].copy_from_slice(&total.to_le_bytes());
        let count = u16::from_le_bytes([self.data[4], self.data[5]]);
        let new_count = count.wrapping_sub(1);
        self.data[4..6].copy_from_slice(&new_count.to_le_bytes());
    }
}

/// Try to parse bytes as an integer for compact encoding.
fn try_encode_as_integer(value: &[u8]) -> Option<i64> {
    // Canonical forms only. A bare `parse::<i64>()` accepts `000000012345`
    // and `+5`, and storing the parsed value re-renders them as `12345` and
    // `5` -- the caller's bytes are gone. See `storage::numeric`.
    crate::storage::numeric::canonical_i64(value)
}

/// Create an entry for comparison from raw bytes.
fn make_entry_for_compare(value: &[u8]) -> ListpackEntry {
    if let Some(i) = try_encode_as_integer(value) {
        ListpackEntry::Integer(i)
    } else {
        ListpackEntry::String(value.to_vec())
    }
}

/// Encode a value into listpack entry bytes (encoding + data + backlen).
fn encode_entry(value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();

    if let Some(v) = try_encode_as_integer(value) {
        encode_integer_entry(v, &mut buf);
    } else {
        encode_string_entry(value, &mut buf);
    }

    // Compute and append backlen (total entry size before backlen)
    let entry_len = buf.len();
    let backlen = encode_backlen(entry_len);
    buf.extend_from_slice(&backlen);
    buf
}

fn encode_integer_entry(v: i64, buf: &mut Vec<u8>) {
    if v >= 0 && v <= 127 {
        // 7-bit unsigned: 0xxxxxxx
        buf.push(v as u8);
    } else if v >= -4096 && v <= 4095 {
        // 13-bit signed: 110xxxxx + 1 byte
        let uv = (v as i16 as u16) & 0x1FFF;
        let b0 = 0xC0 | ((uv >> 8) as u8 & 0x1F);
        let b1 = (uv & 0xFF) as u8;
        buf.push(b0);
        buf.push(b1);
    } else if v >= i16::MIN as i64 && v <= i16::MAX as i64 {
        // 16-bit signed
        buf.push(LP_ENCODING_16BIT_INT);
        buf.extend_from_slice(&(v as i16).to_le_bytes());
    } else if v >= -8388608 && v <= 8388607 {
        // 24-bit signed
        buf.push(LP_ENCODING_24BIT_INT);
        let bytes = (v as i32).to_le_bytes();
        buf.extend_from_slice(&bytes[..3]);
    } else if v >= i32::MIN as i64 && v <= i32::MAX as i64 {
        // 32-bit signed
        buf.push(LP_ENCODING_32BIT_INT);
        buf.extend_from_slice(&(v as i32).to_le_bytes());
    } else {
        // 64-bit signed
        buf.push(LP_ENCODING_64BIT_INT);
        buf.extend_from_slice(&v.to_le_bytes());
    }
}

fn encode_string_entry(value: &[u8], buf: &mut Vec<u8>) {
    let len = value.len();
    if len <= 63 {
        // 6-bit string: 10xxxxxx
        buf.push(0x80 | (len as u8));
        buf.extend_from_slice(value);
    } else if len <= 4095 {
        // 12-bit string: 1110xxxx + 1 byte
        let b0 = 0xE0 | ((len >> 8) as u8 & 0x0F);
        let b1 = (len & 0xFF) as u8;
        buf.push(b0);
        buf.push(b1);
        buf.extend_from_slice(value);
    } else {
        // 32-bit string: 11110000 + 4 bytes
        buf.push(LP_ENCODING_32BIT_STR);
        buf.extend_from_slice(&(len as u32).to_le_bytes());
        buf.extend_from_slice(value);
    }
}

/// Encode backlen as variable-length bytes (7 bits + continuation bit).
fn encode_backlen(entry_len: usize) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut len = entry_len;
    if len <= 127 {
        buf.push(len as u8);
    } else {
        while len > 0 {
            let mut byte = (len & 0x7F) as u8;
            len >>= 7;
            if len > 0 {
                byte |= 0x80;
            }
            buf.push(byte);
        }
    }
    buf
}

/// Byte width of the backlen field for an entry of `entry_len` bytes.
///
/// This is `encode_backlen(entry_len).len()` without the `Vec`. The decoder
/// only ever needs the WIDTH -- it walks forward and must know where the next
/// entry starts -- so allocating the encoded bytes just to call `.len()` on
/// them put one malloc/free on every entry decoded, on every scan.
#[inline]
fn backlen_size(entry_len: usize) -> usize {
    if entry_len <= 127 {
        1
    } else {
        let mut len = entry_len;
        let mut n = 0;
        while len > 0 {
            len >>= 7;
            n += 1;
        }
        n
    }
}

/// Decode backlen reading backward from the byte at `pos - 1`.
/// Returns (entry_len, backlen_size).
fn decode_backlen(data: &[u8], pos: usize) -> (usize, usize) {
    // pos is the start of the next entry (or terminator).
    // Backlen bytes are just before pos, reading backward.
    let mut val: usize = 0;
    let mut shift: usize = 0;
    let mut idx = pos - 1;
    loop {
        let byte = data[idx];
        val |= ((byte & 0x7F) as usize) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            break;
        }
        if idx == 0 {
            break;
        }
        idx -= 1;
    }
    let backlen_size = pos - idx;
    (val, backlen_size)
}

/// Decode an entry at position `pos` in the data.
/// Returns (entry, next_entry_position).
fn decode_entry_at(data: &[u8], pos: usize) -> (ListpackEntry, usize) {
    let b0 = data[pos];

    if b0 & 0x80 == 0 {
        // 7-bit unsigned int: 0xxxxxxx
        let val = b0 as i64;
        let entry_len = 1;
        let next = pos + entry_len + backlen_size(entry_len);
        (ListpackEntry::Integer(val), next)
    } else if b0 & 0xC0 == 0x80 {
        // 6-bit string: 10xxxxxx
        let len = (b0 & 0x3F) as usize;
        let start = pos + 1;
        let str_data = data[start..start + len].to_vec();
        let entry_len = 1 + len;
        let next = pos + entry_len + backlen_size(entry_len);
        (ListpackEntry::String(str_data), next)
    } else if b0 & 0xE0 == 0xC0 {
        // 13-bit signed int: 110xxxxx + 1 byte
        let b1 = data[pos + 1];
        let raw = (((b0 & 0x1F) as u16) << 8) | (b1 as u16);
        // Sign-extend from 13 bits
        let val = if raw & 0x1000 != 0 {
            (raw | 0xE000) as i16 as i64
        } else {
            raw as i64
        };
        let entry_len = 2;
        let next = pos + entry_len + backlen_size(entry_len);
        (ListpackEntry::Integer(val), next)
    } else if b0 & 0xF0 == 0xE0 {
        // 12-bit string: 1110xxxx + 1 byte len
        let len = (((b0 & 0x0F) as usize) << 8) | (data[pos + 1] as usize);
        let start = pos + 2;
        let str_data = data[start..start + len].to_vec();
        let entry_len = 2 + len;
        let next = pos + entry_len + backlen_size(entry_len);
        (ListpackEntry::String(str_data), next)
    } else {
        match b0 {
            LP_ENCODING_16BIT_INT => {
                let val = i16::from_le_bytes([data[pos + 1], data[pos + 2]]) as i64;
                let entry_len = 3;
                let next = pos + entry_len + backlen_size(entry_len);
                (ListpackEntry::Integer(val), next)
            }
            LP_ENCODING_24BIT_INT => {
                let b = [data[pos + 1], data[pos + 2], data[pos + 3]];
                let val = if b[2] & 0x80 != 0 {
                    i32::from_le_bytes([b[0], b[1], b[2], 0xFF]) as i64
                } else {
                    i32::from_le_bytes([b[0], b[1], b[2], 0x00]) as i64
                };
                let entry_len = 4;
                let next = pos + entry_len + backlen_size(entry_len);
                (ListpackEntry::Integer(val), next)
            }
            LP_ENCODING_32BIT_INT => {
                let val = i32::from_le_bytes([
                    data[pos + 1],
                    data[pos + 2],
                    data[pos + 3],
                    data[pos + 4],
                ]) as i64;
                let entry_len = 5;
                let next = pos + entry_len + backlen_size(entry_len);
                (ListpackEntry::Integer(val), next)
            }
            LP_ENCODING_64BIT_INT => {
                let val = i64::from_le_bytes([
                    data[pos + 1],
                    data[pos + 2],
                    data[pos + 3],
                    data[pos + 4],
                    data[pos + 5],
                    data[pos + 6],
                    data[pos + 7],
                    data[pos + 8],
                ]);
                let entry_len = 9;
                let next = pos + entry_len + backlen_size(entry_len);
                (ListpackEntry::Integer(val), next)
            }
            LP_ENCODING_32BIT_STR => {
                let len = u32::from_le_bytes([
                    data[pos + 1],
                    data[pos + 2],
                    data[pos + 3],
                    data[pos + 4],
                ]) as usize;
                let start = pos + 5;
                let str_data = data[start..start + len].to_vec();
                let entry_len = 5 + len;
                let next = pos + entry_len + backlen_size(entry_len);
                (ListpackEntry::String(str_data), next)
            }
            _ => panic!("Unknown listpack encoding byte: 0x{:02X}", b0),
        }
    }
}

/// Borrowing twin of [`decode_entry_at`].
///
/// Must stay byte-for-byte in agreement with it: `zero_alloc_scan_tests::
/// refs_agree_with_owned_entries` asserts that on every encoding width, so a
/// change to one decoder that is not mirrored in the other fails the suite.
fn decode_entry_ref_at(data: &[u8], pos: usize) -> (ListpackRef<'_>, usize) {
    let b0 = data[pos];

    if b0 & 0x80 == 0 {
        // 7-bit unsigned int: 0xxxxxxx
        (ListpackRef::Integer(b0 as i64), pos + 1 + backlen_size(1))
    } else if b0 & 0xC0 == 0x80 {
        // 6-bit string: 10xxxxxx
        let len = (b0 & 0x3F) as usize;
        let start = pos + 1;
        let entry_len = 1 + len;
        (
            ListpackRef::Str(&data[start..start + len]),
            pos + entry_len + backlen_size(entry_len),
        )
    } else if b0 & 0xE0 == 0xC0 {
        // 13-bit signed int: 110xxxxx + 1 byte
        let raw = (((b0 & 0x1F) as u16) << 8) | (data[pos + 1] as u16);
        let val = if raw & 0x1000 != 0 {
            (raw | 0xE000) as i16 as i64
        } else {
            raw as i64
        };
        (ListpackRef::Integer(val), pos + 2 + backlen_size(2))
    } else if b0 & 0xF0 == 0xE0 {
        // 12-bit string: 1110xxxx + 1 byte len
        let len = (((b0 & 0x0F) as usize) << 8) | (data[pos + 1] as usize);
        let start = pos + 2;
        let entry_len = 2 + len;
        (
            ListpackRef::Str(&data[start..start + len]),
            pos + entry_len + backlen_size(entry_len),
        )
    } else {
        match b0 {
            LP_ENCODING_16BIT_INT => {
                let val = i16::from_le_bytes([data[pos + 1], data[pos + 2]]) as i64;
                (ListpackRef::Integer(val), pos + 3 + backlen_size(3))
            }
            LP_ENCODING_24BIT_INT => {
                let b = [data[pos + 1], data[pos + 2], data[pos + 3]];
                let val = if b[2] & 0x80 != 0 {
                    i32::from_le_bytes([b[0], b[1], b[2], 0xFF]) as i64
                } else {
                    i32::from_le_bytes([b[0], b[1], b[2], 0x00]) as i64
                };
                (ListpackRef::Integer(val), pos + 4 + backlen_size(4))
            }
            LP_ENCODING_32BIT_INT => {
                let val = i32::from_le_bytes([
                    data[pos + 1],
                    data[pos + 2],
                    data[pos + 3],
                    data[pos + 4],
                ]) as i64;
                (ListpackRef::Integer(val), pos + 5 + backlen_size(5))
            }
            LP_ENCODING_64BIT_INT => {
                let val = i64::from_le_bytes([
                    data[pos + 1],
                    data[pos + 2],
                    data[pos + 3],
                    data[pos + 4],
                    data[pos + 5],
                    data[pos + 6],
                    data[pos + 7],
                    data[pos + 8],
                ]);
                (ListpackRef::Integer(val), pos + 9 + backlen_size(9))
            }
            LP_ENCODING_32BIT_STR => {
                let len = u32::from_le_bytes([
                    data[pos + 1],
                    data[pos + 2],
                    data[pos + 3],
                    data[pos + 4],
                ]) as usize;
                let start = pos + 5;
                let entry_len = 5 + len;
                (
                    ListpackRef::Str(&data[start..start + len]),
                    pos + entry_len + backlen_size(entry_len),
                )
            }
            // Mirrors `decode_entry_at`: the only writer is this module, so an
            // unknown byte means in-memory corruption, not untrusted input.
            _ => panic!("Unknown listpack encoding byte: 0x{:02X}", b0),
        }
    }
}

impl<'a> Iterator for ListpackRefIter<'a> {
    type Item = ListpackRef<'a>;

    fn next(&mut self) -> Option<ListpackRef<'a>> {
        if self.remaining == 0
            || self.pos >= self.data.len() - 1
            || self.data[self.pos] == LP_TERMINATOR
        {
            return None;
        }
        let (entry, next_pos) = decode_entry_ref_at(self.data, self.pos);
        self.pos = next_pos;
        self.remaining -= 1;
        Some(entry)
    }
}

impl<'a> Iterator for ListpackPairRefIter<'a> {
    type Item = (ListpackRef<'a>, ListpackRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        let f = self.inner.next()?;
        let v = self.inner.next()?;
        Some((f, v))
    }
}

impl<'a> Iterator for ListpackIter<'a> {
    type Item = ListpackEntry;

    fn next(&mut self) -> Option<ListpackEntry> {
        if self.remaining == 0
            || self.pos >= self.data.len() - 1
            || self.data[self.pos] == LP_TERMINATOR
        {
            return None;
        }
        let (entry, next_pos) = decode_entry_at(self.data, self.pos);
        self.pos = next_pos;
        self.remaining -= 1;
        Some(entry)
    }
}

impl<'a> Iterator for ListpackRevIter<'a> {
    type Item = ListpackEntry;

    fn next(&mut self) -> Option<ListpackEntry> {
        if self.remaining == 0 || self.pos <= 6 {
            return None;
        }
        // pos points to the terminator or the byte after the last entry's backlen
        // Read backlen backward from pos - 1
        let (entry_len, backlen_size) = decode_backlen(self.data, self.pos);
        let entry_start = self.pos - backlen_size - entry_len;
        let (entry, _) = decode_entry_at(self.data, entry_start);
        self.pos = entry_start;
        self.remaining -= 1;
        Some(entry)
    }
}

impl<'a> Iterator for ListpackPairIter<'a> {
    type Item = (ListpackEntry, ListpackEntry);

    fn next(&mut self) -> Option<(ListpackEntry, ListpackEntry)> {
        let first = self.inner.next()?;
        let second = self.inner.next()?;
        Some((first, second))
    }
}

impl Default for Listpack {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod noncanonical_tests {
    use super::*;

    /// A listpack must preserve the caller's bytes. `try_encode_as_integer`
    /// used a bare `parse::<i64>()`, so `000000012345` was stored as the
    /// integer 12345 and read back as "12345" -- silent data loss for
    /// zero-padded IDs, account numbers, zip codes.
    #[test]
    fn listpack_preserves_non_canonical_numeric_strings() {
        let cases: [&[u8]; 6] = [b"000000012345", b"007", b"+5", b"-0", b"00", b"0000"];
        for input in cases {
            let mut lp = Listpack::new();
            lp.push_back(input);
            let got = lp.get_at(0).expect("entry present").to_bytes();
            assert_eq!(
                got.as_ref(),
                input,
                "listpack rewrote {:?} as {:?}",
                std::str::from_utf8(input).unwrap(),
                String::from_utf8_lossy(got.as_ref())
            );
        }
    }

    /// Canonical integers must still take the compact integer encoding --
    /// the fix must not disable the optimisation it is protecting.
    #[test]
    fn listpack_still_integer_encodes_canonical_values() {
        for input in [&b"0"[..], b"7", b"12345", b"-42"] {
            assert!(
                try_encode_as_integer(input).is_some(),
                "canonical {:?} must still encode as an integer",
                std::str::from_utf8(input).unwrap()
            );
        }
        for input in [&b"000000012345"[..], b"+5", b"-0"] {
            assert!(
                try_encode_as_integer(input).is_none(),
                "non-canonical {:?} must NOT encode as an integer",
                std::str::from_utf8(input).unwrap()
            );
        }
    }

    /// Lookup must agree with storage: a non-canonical form is a *different*
    /// member, so finding it must not match a stored canonical one.
    #[test]
    fn listpack_find_does_not_match_across_canonical_forms() {
        let mut lp = Listpack::new();
        lp.push_back(b"12345");
        assert!(lp.find(b"12345").is_some(), "exact member must be found");
        assert!(
            lp.find(b"000000012345").is_none(),
            "000000012345 must not match a stored 12345"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_listpack() {
        let lp = Listpack::new();
        assert_eq!(lp.len(), 0);
        assert_eq!(lp.total_bytes(), 7);
        assert!(lp.is_empty());
    }

    #[test]
    fn test_push_back_string() {
        let mut lp = Listpack::new();
        lp.push_back(b"hello");
        assert_eq!(lp.len(), 1);
        let entries: Vec<_> = lp.iter().collect();
        assert_eq!(entries[0], ListpackEntry::String(b"hello".to_vec()));
    }

    #[test]
    fn test_push_back_100_entries() {
        let mut lp = Listpack::new();
        for i in 0..100 {
            lp.push_back(format!("entry_{}", i).as_bytes());
        }
        assert_eq!(lp.len(), 100);
        let entries: Vec<_> = lp.iter().collect();
        assert_eq!(entries.len(), 100);
        for i in 0..100 {
            let expected_str = format!("entry_{}", i);
            // "entry_N" is not a pure integer, so stored as string
            assert_eq!(entries[i], ListpackEntry::String(expected_str.into_bytes()));
        }
    }

    #[test]
    fn test_push_front() {
        let mut lp = Listpack::new();
        lp.push_back(b"second");
        lp.push_front(b"first");
        let entries: Vec<_> = lp.iter().collect();
        assert_eq!(entries[0], ListpackEntry::String(b"first".to_vec()));
        assert_eq!(entries[1], ListpackEntry::String(b"second".to_vec()));
    }

    #[test]
    fn test_7bit_int_encoding() {
        let mut lp = Listpack::new();
        lp.push_back(b"42");
        let entry = lp.get_at(0).unwrap();
        assert_eq!(entry, ListpackEntry::Integer(42));
    }

    #[test]
    fn test_string_abc_encoding() {
        let mut lp = Listpack::new();
        lp.push_back(b"abc");
        let entry = lp.get_at(0).unwrap();
        assert_eq!(entry, ListpackEntry::String(b"abc".to_vec()));
    }

    #[test]
    fn test_13bit_int_encoding() {
        let mut lp = Listpack::new();
        lp.push_back(b"1000");
        let entry = lp.get_at(0).unwrap();
        assert_eq!(entry, ListpackEntry::Integer(1000));
    }

    #[test]
    fn test_16bit_int_encoding() {
        let mut lp = Listpack::new();
        lp.push_back(b"10000");
        let entry = lp.get_at(0).unwrap();
        assert_eq!(entry, ListpackEntry::Integer(10000));
    }

    #[test]
    fn test_32bit_int_encoding() {
        let mut lp = Listpack::new();
        lp.push_back(b"2000000000");
        let entry = lp.get_at(0).unwrap();
        assert_eq!(entry, ListpackEntry::Integer(2_000_000_000));
    }

    #[test]
    fn test_64bit_int_encoding() {
        let mut lp = Listpack::new();
        let val = i64::MAX;
        lp.push_back(val.to_string().as_bytes());
        let entry = lp.get_at(0).unwrap();
        assert_eq!(entry, ListpackEntry::Integer(val));
    }

    #[test]
    fn test_string_100_bytes() {
        let mut lp = Listpack::new();
        let s = vec![b'x'; 100];
        lp.push_back(&s);
        let entry = lp.get_at(0).unwrap();
        assert_eq!(entry, ListpackEntry::String(s));
    }

    #[test]
    fn test_iter_rev() {
        let mut lp = Listpack::new();
        lp.push_back(b"a");
        lp.push_back(b"b");
        lp.push_back(b"c");
        let rev: Vec<_> = lp.iter_rev().collect();
        assert_eq!(rev[0], ListpackEntry::String(b"c".to_vec()));
        assert_eq!(rev[1], ListpackEntry::String(b"b".to_vec()));
        assert_eq!(rev[2], ListpackEntry::String(b"a".to_vec()));
    }

    #[test]
    fn test_find() {
        let mut lp = Listpack::new();
        lp.push_back(b"hello");
        lp.push_back(b"world");
        lp.push_back(b"42");
        assert_eq!(lp.find(b"world"), Some(1));
        assert_eq!(lp.find(b"42"), Some(2));
        assert_eq!(lp.find(b"missing"), None);
    }

    #[test]
    fn test_remove_at() {
        let mut lp = Listpack::new();
        lp.push_back(b"a");
        lp.push_back(b"b");
        lp.push_back(b"c");
        assert!(lp.remove_at(1));
        assert_eq!(lp.len(), 2);
        let entries: Vec<_> = lp.iter().collect();
        assert_eq!(entries[0], ListpackEntry::String(b"a".to_vec()));
        assert_eq!(entries[1], ListpackEntry::String(b"c".to_vec()));
    }

    #[test]
    fn test_replace_at() {
        let mut lp = Listpack::new();
        lp.push_back(b"old");
        lp.push_back(b"keep");
        lp.replace_at(0, b"new");
        let entries: Vec<_> = lp.iter().collect();
        assert_eq!(entries[0], ListpackEntry::String(b"new".to_vec()));
        assert_eq!(entries[1], ListpackEntry::String(b"keep".to_vec()));
    }

    #[test]
    fn test_roundtrip_50_entries() {
        let mut lp = Listpack::new();
        for i in 0..50 {
            lp.push_back(format!("item_{}", i).as_bytes());
        }
        let forward: Vec<_> = lp.iter().collect();
        let reverse: Vec<_> = lp.iter_rev().collect();
        assert_eq!(forward.len(), 50);
        assert_eq!(reverse.len(), 50);
        let mut rev_sorted = reverse;
        rev_sorted.reverse();
        assert_eq!(forward, rev_sorted);
    }

    #[test]
    fn test_get_at() {
        let mut lp = Listpack::new();
        lp.push_back(b"zero");
        lp.push_back(b"one");
        lp.push_back(b"two");
        assert_eq!(lp.get_at(0), Some(ListpackEntry::String(b"zero".to_vec())));
        assert_eq!(lp.get_at(1), Some(ListpackEntry::String(b"one".to_vec())));
        assert_eq!(lp.get_at(2), Some(ListpackEntry::String(b"two".to_vec())));
        assert_eq!(lp.get_at(3), None);
    }

    #[test]
    fn test_iter_pairs() {
        let mut lp = Listpack::new();
        lp.push_back(b"field1");
        lp.push_back(b"value1");
        lp.push_back(b"field2");
        lp.push_back(b"value2");
        let pairs: Vec<_> = lp.iter_pairs().collect();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0, ListpackEntry::String(b"field1".to_vec()));
        assert_eq!(pairs[0].1, ListpackEntry::String(b"value1".to_vec()));
    }

    #[test]
    fn test_clone() {
        let mut lp = Listpack::new();
        lp.push_back(b"test");
        let lp2 = lp.clone();
        lp.push_back(b"extra");
        assert_eq!(lp.len(), 2);
        assert_eq!(lp2.len(), 1);
    }

    #[test]
    fn test_to_hash_map() {
        let mut lp = Listpack::new();
        lp.push_back(b"key1");
        lp.push_back(b"val1");
        lp.push_back(b"key2");
        lp.push_back(b"val2");
        let map = lp.to_hash_map();
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&Bytes::from("key1")), Some(&Bytes::from("val1")));
        assert_eq!(map.get(&Bytes::from("key2")), Some(&Bytes::from("val2")));
    }

    #[test]
    fn test_to_vec_deque() {
        let mut lp = Listpack::new();
        lp.push_back(b"a");
        lp.push_back(b"b");
        let deque = lp.to_vec_deque();
        assert_eq!(deque.len(), 2);
        assert_eq!(deque[0], Bytes::from("a"));
        assert_eq!(deque[1], Bytes::from("b"));
    }

    #[test]
    fn test_negative_integer() {
        let mut lp = Listpack::new();
        lp.push_back(b"-100");
        let entry = lp.get_at(0).unwrap();
        assert_eq!(entry, ListpackEntry::Integer(-100));
    }

    #[test]
    fn test_zero_encoding() {
        let mut lp = Listpack::new();
        lp.push_back(b"0");
        let entry = lp.get_at(0).unwrap();
        assert_eq!(entry, ListpackEntry::Integer(0));
    }

    #[test]
    fn test_127_boundary() {
        let mut lp = Listpack::new();
        lp.push_back(b"127");
        let entry = lp.get_at(0).unwrap();
        assert_eq!(entry, ListpackEntry::Integer(127));

        let mut lp2 = Listpack::new();
        lp2.push_back(b"128");
        let entry2 = lp2.get_at(0).unwrap();
        assert_eq!(entry2, ListpackEntry::Integer(128));
    }
}

#[cfg(test)]
mod zero_alloc_scan_tests {
    use super::*;

    /// The borrowed view must agree with the owning decoder on every entry,
    /// for both the integer and the string encodings. If these ever diverge,
    /// the zero-alloc scan would silently answer a different question than the
    /// allocating one it replaces.
    #[test]
    fn refs_agree_with_owned_entries() {
        let mut lp = Listpack::new();
        // Cover every encoding width the decoder distinguishes.
        let inputs: Vec<Vec<u8>> = vec![
            b"0".to_vec(),
            b"127".to_vec(),
            b"-1".to_vec(),
            b"4095".to_vec(),
            b"-4096".to_vec(),
            b"32767".to_vec(),
            b"-32768".to_vec(),
            b"8388607".to_vec(),
            b"2147483647".to_vec(),
            b"9223372036854775807".to_vec(),
            b"-9223372036854775808".to_vec(),
            b"".to_vec(),
            b"short".to_vec(),
            // NOTE: non-canonical integer spellings (`0123`, `+5`) are
            // deliberately absent. On this branch the ENCODER still folds them
            // to `123` / `5`, losing the original bytes -- that is bug #795,
            // fixed separately in `try_encode_as_integer`. Asserting the
            // correct round trip here would fail for a defect this change does
            // not own. `find_pair_index_handles_integer_encoded_fields` still
            // covers the LOOKUP side, which is what this change is responsible
            // for: a non-canonical query must not match a canonical entry.
            vec![b'x'; 63],   // 6-bit string boundary
            vec![b'y'; 64],   // just past it
            vec![b'z'; 4095], // 12-bit string boundary
        ];
        for v in &inputs {
            lp.push_back(v);
        }

        let owned: Vec<ListpackEntry> = lp.iter().collect();
        let borrowed: Vec<ListpackRef<'_>> = lp.iter_refs().collect();
        assert_eq!(owned.len(), inputs.len(), "iter() lost entries");
        assert_eq!(borrowed.len(), inputs.len(), "iter_refs() lost entries");

        for (i, (o, b)) in owned.iter().zip(borrowed.iter()).enumerate() {
            assert_eq!(
                o.as_bytes(),
                b.to_vec(),
                "entry {i} decoded differently by iter_refs()"
            );
            // The comparison helper is the thing the hot paths actually call.
            assert!(
                b.eq_bytes(&inputs[i]),
                "entry {i} ({:?}) failed eq_bytes against its own input",
                inputs[i]
            );
            assert!(
                !b.eq_bytes(b"\xffdefinitely-not-this"),
                "entry {i} matched a value it does not hold"
            );
        }
    }

    /// `find_pair_index` replaces the allocating `iter_pairs()` scan in HSET.
    /// It must return the FIELD index (not the raw entry index) and must only
    /// ever match on field positions -- a value that happens to equal the
    /// field being searched for must not produce a hit.
    #[test]
    fn find_pair_index_matches_fields_only() {
        let mut lp = Listpack::new();
        // field/value pairs where one VALUE collides with a later FIELD name.
        for (f, v) in [
            (&b"alpha"[..], &b"beta"[..]),
            (&b"gamma"[..], &b"delta"[..]),
            (&b"epsilon"[..], &b"zeta"[..]),
        ] {
            lp.push_back(f);
            lp.push_back(v);
        }

        assert_eq!(lp.find_pair_index(b"alpha"), Some(0));
        assert_eq!(lp.find_pair_index(b"gamma"), Some(1));
        assert_eq!(lp.find_pair_index(b"epsilon"), Some(2));
        assert_eq!(lp.find_pair_index(b"missing"), None);
        // "beta" is a VALUE, never a field -- must not match.
        assert_eq!(lp.find_pair_index(b"beta"), None);
        assert_eq!(lp.find_pair_index(b"delta"), None);
    }

    /// Integer-encoded fields must be findable by their decimal spelling, and
    /// only by their CANONICAL spelling -- `007` is a different field from `7`.
    #[test]
    fn find_pair_index_handles_integer_encoded_fields() {
        let mut lp = Listpack::new();
        lp.push_back(b"7");
        lp.push_back(b"seven");
        lp.push_back(b"-42");
        lp.push_back(b"minus");

        assert_eq!(lp.find_pair_index(b"7"), Some(0));
        assert_eq!(lp.find_pair_index(b"-42"), Some(1));
        assert_eq!(
            lp.find_pair_index(b"007"),
            None,
            "non-canonical must not match"
        );
        assert_eq!(
            lp.find_pair_index(b"+7"),
            None,
            "non-canonical must not match"
        );
    }

    /// `contains_element` replaces the allocating `iter().any()` scan in SADD.
    #[test]
    fn contains_element_matches_any_position() {
        let mut lp = Listpack::new();
        for m in [&b"a"[..], &b"bb"[..], &b"123"[..]] {
            lp.push_back(m);
        }
        assert!(lp.contains_element(b"a"));
        assert!(lp.contains_element(b"bb"));
        assert!(lp.contains_element(b"123"));
        assert!(!lp.contains_element(b"c"));
        assert!(!lp.contains_element(b"0123"));
    }

    /// The scan must stay correct on an empty listpack and on one holding a
    /// single element, where the header/terminator arithmetic is tightest.
    #[test]
    fn degenerate_sizes() {
        let empty = Listpack::new();
        assert_eq!(empty.iter_refs().count(), 0);
        assert_eq!(empty.find_pair_index(b"x"), None);
        assert!(!empty.contains_element(b"x"));

        let mut one = Listpack::new();
        one.push_back(b"solo");
        assert_eq!(one.iter_refs().count(), 1);
        assert!(one.contains_element(b"solo"));
        // A lone field with no value is not a complete pair.
        assert_eq!(one.find_pair_index(b"solo"), None);
    }

    /// `backlen_size` is the non-allocating replacement for
    /// `encode_backlen(n).len()`; it must agree with it exactly, including
    /// across the 7-bit continuation boundaries.
    #[test]
    fn backlen_size_agrees_with_encode_backlen() {
        for n in [
            0usize, 1, 126, 127, 128, 129, 16383, 16384, 16385, 2097151, 2097152,
        ] {
            assert_eq!(
                backlen_size(n),
                encode_backlen(n).len(),
                "backlen_size disagreed at entry_len={n}"
            );
        }
    }
}
