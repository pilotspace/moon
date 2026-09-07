//! CompactValue: 16-byte value representation with Small String Optimization (SSO)
//! and tagged heap pointers for collection types.
//!
//! Layout:
//! - `len_and_tag: u32` -- high nibble encodes type tag, lower 28 bits encode length
//! - `payload: [u8; 12]` -- inline data (SSO) or prefix + tagged heap pointer
//!
//! SSO path (strings <= 12 bytes): data stored inline in `payload[0..len]`
//! Heap path (strings > 12 bytes or collections):
//!   `payload[4..12]` = tagged pointer (raw_ptr | type_tag_in_low_3_bits);
//!   for heap STRINGS `payload[0..4]` = low 32 bits of the length and the
//!   28 `LEN_MASK` bits of `len_and_tag` = the high bits (see
//!   [`CompactValue::heap_string_len`]), so the length of a heap string is
//!   readable without dereferencing the pointer. Collections leave
//!   `payload[0..4]` zero.

use bytes::Bytes;
use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;

use super::bptree::BPTree;
use super::entry::RedisValue;
use super::intset::Intset;
use super::listpack::Listpack;
use super::stream::Stream as StreamData;

// ---- Constants ----

const HEAP_MARKER: u32 = 0xF0000000; // high nibble = 0xF means heap-allocated
const TYPE_MASK: u32 = 0xF0000000; // high nibble of len_and_tag
const LEN_MASK: u32 = 0x0FFFFFFF; // lower 28 bits = length
const SSO_MAX_LEN: usize = 12;

// Inline type tags (high nibble of len_and_tag, for SSO values)
const TAG_STRING: u32 = 0x00000000; // 0 in high nibble

// Heap type tags (low 3 bits of pointer)
// Tag 0 = raw string stored as Box<[u8]> (NOT Box<RedisValue>!)
const HEAP_TAG_STRING: usize = 0;
const HEAP_TAG_HASH: usize = 1;
const HEAP_TAG_LIST: usize = 2;
const HEAP_TAG_SET: usize = 3;
const HEAP_TAG_ZSET: usize = 4;
const HEAP_TAG_STREAM: usize = 5;
const HEAP_TAG_MASK: usize = 0x7;

// The heap layout packs an 8-byte pointer into `payload[4..12]`; the
// 60-bit length split below assumes the same word size.
const _: () = assert!(std::mem::size_of::<usize>() == 8);

/// Split a heap string's length into the two places it is stored: the 28
/// `LEN_MASK` bits of `len_and_tag` (high part) and `payload[0..4]` (low 32
/// bits, native-endian).
///
/// 60 bits is not the full `usize` range, so this is a precondition, not a
/// tautology: `len` is the length of a `[u8]` that is ALLOCATED, and no
/// 64-bit target moon ships on can address 2^60 bytes (x86-64 with 5-level
/// paging stops at 2^57, aarch64 at 2^52), so an allocation of that length
/// cannot exist — a request for one aborts in `handle_alloc_error` long
/// before reaching here. The `debug_assert` pins the argument.
#[inline]
fn encode_heap_len(len: usize) -> (u32, [u8; 4]) {
    debug_assert!(
        (len >> 60) == 0,
        "a heap string longer than 2^60 bytes cannot be allocated"
    );
    let hi = ((len >> 32) as u32) & LEN_MASK;
    let lo = (len as u32).to_ne_bytes();
    (hi, lo)
}

/// Inverse of [`encode_heap_len`]. `hi` must already be masked to `LEN_MASK`.
#[inline]
fn decode_heap_len(hi: u32, lo: u32) -> usize {
    ((hi as usize) << 32) | (lo as usize)
}

/// Thin wrapper for heap-allocated strings.
///
/// Two words — pointer + length — so that the `Box<HeapString>` billed once per
/// stored value lands exactly in jemalloc's **16-byte** size class. A `Vec<u8>`
/// here would be three words (24 B) and round up to the **32-byte** class,
/// costing 16 extra bytes on every key whose value exceeds [`SSO_MAX_LEN`].
/// jemalloc's 64-bit small classes have no 24-byte entry:
/// 8, 16, 32, 48, 64, 80, ... (verified with `nallocx`; see the unit test
/// `heap_string_wrapper_fits_the_16_byte_jemalloc_class`).
///
/// Dropping `capacity` also removes a leak vector: a `Vec` built from an
/// over-allocated buffer used to strand its unused capacity for the whole
/// lifetime of the key. A boxed slice is always exactly `len` bytes.
///
/// The stored string is immutable in place-length terms — every writer
/// (SET, APPEND, SETRANGE, GETSET) replaces the whole `CompactValue` — so the
/// growable `Vec` this replaces bought nothing.
struct HeapString(Box<[u8]>);

/// Bytes the `Box<RedisValue>` behind every collection value really costs
/// (moon#788).
///
/// `RedisValue` is an enum sized by its largest variant, and jemalloc rounds
/// the box up to a size class. The ledger charged nothing for it — one
/// unbilled allocation on every hash, list, set, sorted-set and stream key.
const BOXED_REDIS_VALUE_BYTES: usize =
    crate::storage::mem_size::size_class(std::mem::size_of::<RedisValue>());

/// Borrowed view of a CompactValue, for zero-copy read access.
pub enum RedisValueRef<'a> {
    String(&'a [u8]),
    Hash(&'a HashMap<Bytes, Bytes>),
    /// Hash with per-field TTL sidecar (phase 195 / issue #106).
    /// Mirrors `RedisValue::HashWithTtl`. Readers must filter `fields` by
    /// `ttls` against the current shard clock to skip expired fields.
    HashWithTtl {
        fields: &'a HashMap<Bytes, Bytes>,
        ttls: &'a HashMap<Bytes, u64>,
        min_expiry_ms: u64,
    },
    List(&'a VecDeque<Bytes>),
    Set(&'a crate::storage::entry::SetValue),
    SortedSet {
        members: &'a HashMap<Bytes, f64>,
        scores: &'a BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
    },
    // Compact variants
    HashListpack(&'a Listpack),
    ListListpack(&'a Listpack),
    SetListpack(&'a Listpack),
    SetIntset(&'a Intset),
    SortedSetBPTree {
        tree: &'a BPTree,
        members: &'a HashMap<Bytes, f64>,
    },
    SortedSetListpack(&'a Listpack),
    Stream(&'a StreamData),
}

impl<'a> RedisValueRef<'a> {
    /// Return the encoding name for OBJECT ENCODING command.
    pub fn encoding_name(&self) -> &'static str {
        match self {
            RedisValueRef::String(s) => {
                if crate::storage::numeric::canonical_i64(s).is_some() {
                    "int"
                } else {
                    "embstr"
                }
            }
            RedisValueRef::Hash(_) => "hashtable",
            RedisValueRef::HashWithTtl { .. } => "hashtable",
            RedisValueRef::HashListpack(_) => "listpack",
            RedisValueRef::List(_) => "linkedlist",
            RedisValueRef::ListListpack(_) => "listpack",
            RedisValueRef::Set(_) => "hashtable",
            RedisValueRef::SetListpack(_) => "listpack",
            RedisValueRef::SetIntset(_) => "intset",
            RedisValueRef::SortedSet { .. } => "skiplist",
            RedisValueRef::SortedSetBPTree { .. } => "skiplist",
            RedisValueRef::SortedSetListpack(_) => "listpack",
            RedisValueRef::Stream(_) => "stream",
        }
    }
}

/// A 16-byte compact value representation with SSO for small strings
/// and tagged heap pointers for larger values and collection types.
#[repr(C)]
pub struct CompactValue {
    len_and_tag: u32,
    payload: [u8; 12],
}

const _: () = assert!(std::mem::size_of::<CompactValue>() == 16);

impl CompactValue {
    /// Check if the value is stored inline (SSO).
    #[inline]
    pub fn is_inline(&self) -> bool {
        (self.len_and_tag & TYPE_MASK) != HEAP_MARKER
    }

    /// Return the inline length (only valid for SSO values).
    #[inline]
    fn inline_len(&self) -> usize {
        (self.len_and_tag & LEN_MASK) as usize
    }

    /// Create an inline string value (data must be <= 12 bytes).
    pub fn inline_string(data: &[u8]) -> Self {
        debug_assert!(data.len() <= SSO_MAX_LEN);
        let mut payload = [0u8; 12];
        payload[..data.len()].copy_from_slice(data);
        CompactValue {
            len_and_tag: TAG_STRING | (data.len() as u32),
            payload,
        }
    }

    /// Create a string `CompactValue` from a borrowed slice.
    ///
    /// The same branch `from_redis_value` takes for `RedisValue::String`, minus the
    /// owned `Bytes` the caller would otherwise have to build first. Both arms copy
    /// the bytes anyway — SSO inlines them, and the heap arm reaches
    /// `Bytes::into::<Vec<u8>>()`, which is only zero-copy at refcount 1 and a slice
    /// of a shared read buffer never is — so nothing is lost by borrowing, and a
    /// caller with `itoa` output or a stack buffer avoids an allocation entirely.
    #[inline]
    pub fn from_slice(data: &[u8]) -> Self {
        if data.len() <= SSO_MAX_LEN {
            Self::inline_string(data)
        } else {
            Self::heap_string(data)
        }
    }

    /// Create a CompactValue from a RedisValue.
    ///
    /// Strings > 12 bytes are stored as `Box<[u8]>` (raw bytes) to eliminate the
    /// `RedisValue` enum wrapper (~40B savings per heap string).
    /// Collections are still stored as `Box<RedisValue>`.
    pub fn from_redis_value(value: RedisValue) -> Self {
        // String fast path: inline SSO or zero-copy owned Bytes
        if let RedisValue::String(s) = value {
            return if s.len() <= SSO_MAX_LEN {
                Self::inline_string(&s)
            } else {
                Self::heap_string_owned(s)
            };
        }

        // Collection heap path: store as Box<RedisValue>
        let heap_tag = match &value {
            RedisValue::Hash(_) | RedisValue::HashListpack(_) | RedisValue::HashWithTtl { .. } => {
                HEAP_TAG_HASH
            }
            RedisValue::List(_) | RedisValue::ListListpack(_) => HEAP_TAG_LIST,
            RedisValue::Set(_) | RedisValue::SetListpack(_) | RedisValue::SetIntset(_) => {
                HEAP_TAG_SET
            }
            RedisValue::SortedSet { .. }
            | RedisValue::SortedSetBPTree { .. }
            | RedisValue::SortedSetListpack(_) => HEAP_TAG_ZSET,
            RedisValue::Stream(_) => HEAP_TAG_STREAM,
            RedisValue::String(_) => unreachable!(),
        };

        let boxed = Box::new(value);
        let raw_ptr = Box::into_raw(boxed) as usize;
        debug_assert!(
            raw_ptr & HEAP_TAG_MASK == 0,
            "Box pointer insufficiently aligned"
        );
        let tagged_ptr = raw_ptr | heap_tag;

        let mut payload = [0u8; 12];
        // No prefix for collections
        payload[4..12].copy_from_slice(&tagged_ptr.to_ne_bytes());

        CompactValue {
            len_and_tag: HEAP_MARKER,
            payload,
        }
    }

    /// Create a heap-allocated string CompactValue from a byte slice (copies data).
    pub fn heap_string(data: &[u8]) -> Self {
        Self::heap_string_vec(data.to_vec())
    }

    /// Create from owned Bytes (converts to Vec<u8> via Bytes::into for zero-copy
    /// when Bytes has unique ownership, or copies when shared).
    pub fn heap_string_owned(data: Bytes) -> Self {
        // Bytes::into::<Vec<u8>> is zero-copy when refcount == 1, copies otherwise
        Self::heap_string_vec(data.into())
    }

    /// Create from an owned Vec<u8> directly — no copy, no refcount.
    /// This is the fastest path: one Box allocation for the HeapString wrapper.
    /// Public for RDB loader fast path.
    pub fn heap_string_vec_direct(data: Vec<u8>) -> Self {
        if data.len() <= SSO_MAX_LEN {
            return Self::inline_string(&data);
        }
        Self::heap_string_vec(data)
    }

    fn heap_string_vec(data: Vec<u8>) -> Self {
        debug_assert!(data.len() > SSO_MAX_LEN);
        let str_len = data.len();
        let (len_hi, len_lo) = encode_heap_len(str_len);

        // `into_boxed_slice` is a no-op when `capacity == len`, which is the case
        // for every hot-path caller: `heap_string` copies via `to_vec`, and
        // `heap_string_owned` goes through `Bytes -> Vec`, which copies into an
        // exactly-sized buffer whenever the `Bytes` is shared (always true for a
        // value parsed out of the connection's read buffer). When capacity does
        // exceed len it reallocates once here, in exchange for not stranding the
        // excess for the lifetime of the key.
        let hs = Box::new(HeapString(data.into_boxed_slice()));
        let raw_ptr = Box::into_raw(hs) as usize;
        debug_assert!(
            raw_ptr & HEAP_TAG_MASK == 0,
            "HeapString pointer insufficiently aligned"
        );
        let tagged_ptr = raw_ptr | HEAP_TAG_STRING;

        let mut payload = [0u8; 12];
        payload[..4].copy_from_slice(&len_lo);
        payload[4..12].copy_from_slice(&tagged_ptr.to_ne_bytes());

        CompactValue {
            len_and_tag: HEAP_MARKER | len_hi,
            payload,
        }
    }

    /// Length of a heap string, read from the 16-byte value itself — never
    /// from the heap.
    ///
    /// The overwrite path (`Database::set`'s update closure) bills the OLD
    /// value through `estimate_memory` before dropping it; when that read went
    /// through the pointer it was a second cache miss on every overwrite, for
    /// a number that was already sitting next to the pointer (moon perf
    /// campaign, G1 §5). Low 32 bits live in `payload[0..4]`, the high bits in
    /// the 28 `LEN_MASK` bits of `len_and_tag` — 60 bits, which no allocation
    /// on a 64-bit target can exceed.
    #[inline]
    fn heap_string_len(&self) -> usize {
        debug_assert!(!self.is_inline() && self.heap_type_tag() == HEAP_TAG_STRING);
        let lo = u32::from_ne_bytes([
            self.payload[0],
            self.payload[1],
            self.payload[2],
            self.payload[3],
        ]);
        decode_heap_len(self.len_and_tag & LEN_MASK, lo)
    }

    /// Get the tagged pointer from a heap-allocated value.
    #[inline]
    #[allow(clippy::unwrap_used)] // payload[4..12] is exactly 8 bytes — try_into::<[u8; 8]> is infallible
    fn heap_tagged_ptr(&self) -> usize {
        debug_assert!(!self.is_inline());
        usize::from_ne_bytes(self.payload[4..12].try_into().unwrap())
    }

    /// Get the raw (untagged) pointer.
    /// For strings (tag 0): points to HeapString.
    /// For collections: points to RedisValue.
    #[inline]
    fn heap_raw_usize(&self) -> usize {
        self.heap_tagged_ptr() & !HEAP_TAG_MASK
    }

    /// Get the raw pointer to a heap RedisValue (collections only — NOT strings).
    #[inline]
    fn heap_collection_ptr(&self) -> *mut RedisValue {
        debug_assert!(self.heap_type_tag() != HEAP_TAG_STRING);
        self.heap_raw_usize() as *mut RedisValue
    }

    /// Get the raw pointer to a HeapString (strings only).
    #[inline]
    fn heap_string_ptr(&self) -> *mut HeapString {
        debug_assert!(self.heap_type_tag() == HEAP_TAG_STRING);
        self.heap_raw_usize() as *mut HeapString
    }

    /// Get the heap type tag from the low 3 bits.
    #[inline]
    fn heap_type_tag(&self) -> usize {
        self.heap_tagged_ptr() & HEAP_TAG_MASK
    }

    /// Borrow the underlying RedisValue as a RedisValueRef for zero-copy reads.
    pub fn as_redis_value(&self) -> RedisValueRef<'_> {
        if self.is_inline() {
            let len = self.inline_len();
            RedisValueRef::String(&self.payload[..len])
        } else if self.heap_type_tag() == HEAP_TAG_STRING {
            // String path: HeapString (no RedisValue wrapper)
            // SAFETY: Tag is HEAP_TAG_STRING, so the pointer was created from Box::into_raw(Box<HeapString>)
            // and has not been freed. We hold &self so no mutable alias exists.
            let hs = unsafe { &*self.heap_string_ptr() };
            RedisValueRef::String(&hs.0)
        } else {
            // Collection path: Box<RedisValue>
            // SAFETY: Tag is a collection type, so the pointer was created from Box::into_raw(Box<RedisValue>)
            // and has not been freed. We hold &self so no mutable alias exists.
            let rv = unsafe { &*self.heap_collection_ptr() };
            match rv {
                RedisValue::Hash(map) => RedisValueRef::Hash(map),
                RedisValue::HashWithTtl {
                    fields,
                    ttls,
                    min_expiry_ms,
                } => RedisValueRef::HashWithTtl {
                    fields,
                    ttls,
                    min_expiry_ms: *min_expiry_ms,
                },
                RedisValue::List(list) => RedisValueRef::List(list),
                RedisValue::Set(set) => RedisValueRef::Set(set),
                RedisValue::SortedSet { members, scores } => {
                    RedisValueRef::SortedSet { members, scores }
                }
                RedisValue::HashListpack(lp) => RedisValueRef::HashListpack(lp),
                RedisValue::ListListpack(lp) => RedisValueRef::ListListpack(lp),
                RedisValue::SetListpack(lp) => RedisValueRef::SetListpack(lp),
                RedisValue::SetIntset(is) => RedisValueRef::SetIntset(is),
                RedisValue::SortedSetBPTree { tree, members } => {
                    RedisValueRef::SortedSetBPTree { tree, members }
                }
                RedisValue::SortedSetListpack(lp) => RedisValueRef::SortedSetListpack(lp),
                RedisValue::Stream(s) => RedisValueRef::Stream(s),
                RedisValue::String(_) => unreachable!("strings use HeapString path"),
            }
        }
    }

    /// Fast path: get string bytes (returns None for non-string types).
    pub fn as_bytes(&self) -> Option<&[u8]> {
        if self.is_inline() {
            let len = self.inline_len();
            Some(&self.payload[..len])
        } else if self.heap_type_tag() == HEAP_TAG_STRING {
            // SAFETY: Tag verified as HEAP_TAG_STRING; pointer from Box::into_raw is valid and not freed.
            let hs = unsafe { &*self.heap_string_ptr() };
            Some(&hs.0)
        } else {
            None
        }
    }

    /// Fast path: get string bytes as owned Bytes.
    /// For heap strings, copies from HeapString (Vec<u8> → Bytes).
    /// For inline SSO strings (<=12 bytes), copies from inline buffer.
    /// Returns None for non-string types.
    pub fn as_bytes_owned(&self) -> Option<Bytes> {
        if self.is_inline() {
            let len = self.inline_len();
            Some(Bytes::copy_from_slice(&self.payload[..len]))
        } else if self.heap_type_tag() == HEAP_TAG_STRING {
            // SAFETY: Tag verified as HEAP_TAG_STRING; pointer from Box::into_raw is valid and not freed.
            let hs = unsafe { &*self.heap_string_ptr() };
            Some(Bytes::copy_from_slice(&hs.0))
        } else {
            None
        }
    }

    /// Get a mutable reference to the underlying heap RedisValue.
    /// Returns None for inline (SSO) values and for heap strings (use string-specific mutators).
    pub fn as_redis_value_mut(&mut self) -> Option<&mut RedisValue> {
        if self.is_inline() || self.heap_type_tag() == HEAP_TAG_STRING {
            None
        } else {
            // SAFETY: We own this pointer uniquely (no aliasing since we have &mut self)
            Some(unsafe { &mut *self.heap_collection_ptr() })
        }
    }

    /// Get a mutable reference to the heap string bytes.
    /// Returns None for non-string types and inline values.
    ///
    /// The slice is fixed-length on purpose: `len_and_tag` caches the string
    /// length alongside the pointer, so a caller that resized the buffer
    /// in place would desynchronise the two. Writers that change the length
    /// replace the whole `CompactValue`.
    pub fn as_bytes_mut(&mut self) -> Option<&mut [u8]> {
        if self.is_inline() {
            None
        } else if self.heap_type_tag() == HEAP_TAG_STRING {
            // SAFETY: Tag verified as HEAP_TAG_STRING; pointer from Box::into_raw is valid.
            // We have &mut self so no other reference exists.
            let hs = unsafe { &mut *self.heap_string_ptr() };
            Some(&mut hs.0)
        } else {
            None
        }
    }

    /// Consuming conversion: returns the owned RedisValue.
    /// For inline strings, allocates a new Bytes.
    /// For heap strings, converts HeapString → Bytes.
    /// For collections, reconstructs the Box and extracts the value.
    pub fn into_redis_value(self) -> RedisValue {
        if self.is_inline() {
            let len = self.inline_len();
            let data = Bytes::copy_from_slice(&self.payload[..len]);
            std::mem::forget(self);
            RedisValue::String(data)
        } else if self.heap_type_tag() == HEAP_TAG_STRING {
            let ptr = self.heap_string_ptr();
            std::mem::forget(self);
            // SAFETY: ptr was created from Box::into_raw(Box<HeapString>). We called forget(self)
            // to prevent double-free, so Box::from_raw reclaims the unique allocation.
            let hs = unsafe { *Box::from_raw(ptr) };
            RedisValue::String(Bytes::from(hs.0))
        } else {
            let ptr = self.heap_collection_ptr();
            std::mem::forget(self);
            // SAFETY: ptr was created from Box::into_raw(Box<RedisValue>). We called forget(self)
            // to prevent double-free, so Box::from_raw reclaims the unique allocation.
            let boxed = unsafe { Box::from_raw(ptr) };
            *boxed
        }
    }

    /// Cloning conversion: returns a cloned RedisValue (for serialization/snapshots).
    pub fn to_redis_value(&self) -> RedisValue {
        if self.is_inline() {
            let len = self.inline_len();
            RedisValue::String(Bytes::copy_from_slice(&self.payload[..len]))
        } else if self.heap_type_tag() == HEAP_TAG_STRING {
            // SAFETY: Tag verified as HEAP_TAG_STRING; pointer from Box::into_raw is valid and not freed.
            let hs = unsafe { &*self.heap_string_ptr() };
            RedisValue::String(Bytes::from(hs.0.clone()))
        } else {
            // SAFETY: Tag is a collection type; pointer from Box::into_raw is valid and not freed.
            let rv = unsafe { &*self.heap_collection_ptr() };
            rv.clone()
        }
    }

    /// Return the Redis type name for this value.
    pub fn type_name(&self) -> &'static str {
        if self.is_inline() {
            "string"
        } else {
            match self.heap_type_tag() {
                HEAP_TAG_STRING => "string",
                HEAP_TAG_HASH => "hash",
                HEAP_TAG_LIST => "list",
                HEAP_TAG_SET => "set",
                HEAP_TAG_ZSET => "zset",
                HEAP_TAG_STREAM => "stream",
                _ => "unknown",
            }
        }
    }

    /// Return a numeric type discriminant (0-7).
    pub fn type_tag(&self) -> u8 {
        if self.is_inline() {
            0 // string
        } else {
            self.heap_type_tag() as u8
        }
    }

    /// Estimate memory usage of this value in bytes.
    pub fn estimate_memory(&self) -> usize {
        if self.is_inline() {
            self.inline_len()
        } else if self.heap_type_tag() == HEAP_TAG_STRING {
            // One wrapper allocation (`Box<HeapString>`, two words, jemalloc's
            // 16-byte class) plus the data buffer itself — the buffer rounded
            // to the class jemalloc actually hands out (moon#788). The length
            // comes from the value, not the heap: no pointer chase here.
            std::mem::size_of::<HeapString>()
                + crate::storage::mem_size::size_class(self.heap_string_len())
        } else {
            // SAFETY: Tag is a collection type; pointer from Box::into_raw is valid and not freed.
            let rv = unsafe { &*self.heap_collection_ptr() };
            // moon#788: EVERY container value is a `Box<RedisValue>`, and the
            // enum is sized by its largest variant. That box is a real
            // allocation the ledger never billed — `size_of::<RedisValue>()`
            // rounded to a size class, 128 B on the 64-bit targets moon ships
            // on — invisible to `--maxmemory` for hashes, lists, sets, sorted
            // sets and streams alike. It is a constant for the whole
            // life of the value, so charging it here keeps
            // `entry_overhead(create) + deltas == entry_overhead(remove)`
            // exactly (the WS6 mirror invariant).
            BOXED_REDIS_VALUE_BYTES + rv.estimate_memory()
        }
    }
}

impl Drop for CompactValue {
    fn drop(&mut self) {
        if !self.is_inline() {
            if self.heap_type_tag() == HEAP_TAG_STRING {
                // SAFETY: heap strings are Box<HeapString>
                unsafe {
                    drop(Box::from_raw(self.heap_string_ptr()));
                }
            } else {
                // SAFETY: collections are Box<RedisValue>
                unsafe {
                    drop(Box::from_raw(self.heap_collection_ptr()));
                }
            }
        }
    }
}

impl Clone for CompactValue {
    fn clone(&self) -> Self {
        if self.is_inline() {
            CompactValue {
                len_and_tag: self.len_and_tag,
                payload: self.payload,
            }
        } else if self.heap_type_tag() == HEAP_TAG_STRING {
            // SAFETY: Tag verified as HEAP_TAG_STRING; pointer from Box::into_raw is valid and not freed.
            let hs = unsafe { &*self.heap_string_ptr() };
            Self::heap_string(&hs.0)
        } else {
            // SAFETY: Tag is a collection type; pointer from Box::into_raw is valid and not freed.
            let rv = unsafe { &*self.heap_collection_ptr() };
            Self::from_redis_value(rv.clone())
        }
    }
}

impl fmt::Debug for CompactValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_inline() {
            let len = self.inline_len();
            let data = &self.payload[..len];
            write!(
                f,
                "CompactValue::Inline({:?})",
                String::from_utf8_lossy(data)
            )
        } else {
            write!(f, "CompactValue::Heap({})", self.type_name())
        }
    }
}

// SAFETY: CompactValue is Send/Sync because Box<RedisValue> is Send/Sync
// and inline values are just plain bytes.
unsafe impl Send for CompactValue {}
unsafe impl Sync for CompactValue {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_of_compact_value() {
        assert_eq!(std::mem::size_of::<CompactValue>(), 16);
    }

    /// Every stored string longer than `SSO_MAX_LEN` costs TWO allocations: the
    /// data buffer, and one `Box<HeapString>` wrapper. The wrapper's size is
    /// therefore billed once per key, rounded up to a jemalloc size class.
    ///
    /// jemalloc's 64-bit small classes, measured directly with `nallocx` on
    /// aarch64 (identical on x86_64 — the table is a function of `LG_QUANTUM`,
    /// which is 4 on both), are:
    ///
    ///     8, 16, 32, 48, 64, 80, 96, 112, 128, 160, 192, 224, 256, ...
    ///
    /// There is no 24-byte class. A `Vec<u8>` wrapper (ptr + cap + len = 24 B)
    /// lands in the **32-byte** class and bills 32 bytes per key, 8 of them pure
    /// slack. A `Box<[u8]>` wrapper (ptr + len = 16 B) lands in the **16-byte**
    /// class exactly: 16 bytes per key, zero slack.
    ///
    /// That is a flat **16 bytes/key** saving on every string value above the
    /// SSO cutoff, and it also drops the `capacity` field, so a value built from
    /// an over-capacity `Vec` can no longer strand its excess for the lifetime
    /// of the key.
    #[test]
    fn heap_string_wrapper_fits_the_16_byte_jemalloc_class() {
        assert_eq!(
            std::mem::size_of::<HeapString>(),
            16,
            "HeapString must be two words (ptr + len) so Box<HeapString> lands \
             in jemalloc's 16-byte class; 24 bytes would round up to 32"
        );
    }

    /// `estimate_memory` feeds `Database::used_memory`, which gates `--maxmemory`
    /// and per-db quotas. It must bill the wrapper the allocator actually hands
    /// out, not a stale constant.
    #[test]
    fn heap_string_estimate_memory_bills_the_real_wrapper() {
        let data = vec![b'x'; 64];
        let cv = CompactValue::heap_string(&data);
        assert!(!cv.is_inline());
        assert_eq!(
            cv.estimate_memory(),
            std::mem::size_of::<HeapString>() + 64,
            "per-key charge must track the wrapper's real size"
        );
        assert_eq!(cv.estimate_memory(), 80);
    }

    /// The size spectrum every string layout change must survive: empty, one
    /// byte, the SSO cutoff and its two neighbours, the first heap size, a
    /// few jemalloc class boundaries (the accounting seam), a value with
    /// interior NULs (the layout must never treat the payload as C-string),
    /// and a large value. Each one round-trips through every read API and
    /// through `Clone`, `into_redis_value` and `to_redis_value`.
    ///
    /// Byte `i` of the fixture is `i ^ 0xA5`, so no two positions repeat
    /// within 256 bytes and a length/pointer mix-up shows as a mismatch, not
    /// as a lucky equality.
    #[test]
    fn string_round_trips_across_the_size_spectrum() {
        let sizes: &[usize] = &[
            0,
            1,
            SSO_MAX_LEN - 1,
            SSO_MAX_LEN,
            SSO_MAX_LEN + 1,
            13,
            15,
            16,
            17,
            31,
            32,
            33,
            63,
            64,
            65,
            100,
            127,
            128,
            129,
            255,
            256,
            257,
            4_095,
            4_096,
            4_097,
            65_536,
            1 << 20,
        ];
        for &len in sizes {
            let data: Vec<u8> = (0..len).map(|i| (i as u8) ^ 0xA5).collect();

            let by_slice = CompactValue::from_slice(&data);
            let by_value =
                CompactValue::from_redis_value(RedisValue::String(Bytes::from(data.clone())));
            let by_vec = CompactValue::heap_string_vec_direct(data.clone());

            for (which, cv) in [("slice", by_slice), ("value", by_value), ("vec", by_vec)] {
                assert_eq!(cv.is_inline(), len <= SSO_MAX_LEN, "{which} len={len}");
                assert_eq!(
                    cv.as_bytes(),
                    Some(&data[..]),
                    "{which} len={len}: as_bytes"
                );
                assert_eq!(
                    cv.as_bytes_owned().as_deref(),
                    Some(&data[..]),
                    "{which} len={len}: as_bytes_owned"
                );
                match cv.as_redis_value() {
                    RedisValueRef::String(s) => {
                        assert_eq!(s, &data[..], "{which} len={len}: as_redis_value")
                    }
                    _ => panic!("{which} len={len}: not a string ref"),
                }
                assert_eq!(cv.type_name(), "string");
                assert_eq!(cv.type_tag(), 0);

                let cloned = cv.clone();
                assert_eq!(
                    cloned.as_bytes(),
                    Some(&data[..]),
                    "{which} len={len}: clone"
                );
                match cv.to_redis_value() {
                    RedisValue::String(s) => assert_eq!(&s[..], &data[..]),
                    _ => panic!("{which} len={len}: to_redis_value"),
                }
                match cv.into_redis_value() {
                    RedisValue::String(s) => assert_eq!(&s[..], &data[..]),
                    _ => panic!("{which} len={len}: into_redis_value"),
                }
                drop(cloned);
            }
        }

        // Interior NULs and 0xFF, at a heap size.
        let nul = [0u8, 0xFF, 0, b'a', 0, 0, 0xFF, 0, b'z', 0, 0, 0, 0, 0xFF, 0];
        let cv = CompactValue::from_slice(&nul);
        assert!(!cv.is_inline());
        assert_eq!(cv.as_bytes(), Some(&nul[..]));
        assert_eq!(cv.estimate_memory(), cv.clone().estimate_memory());
    }

    /// In-place mutation through `as_bytes_mut` keeps the length and the
    /// bytes consistent (SETRANGE without growth uses this path).
    #[test]
    fn as_bytes_mut_edits_in_place_at_every_heap_size() {
        for len in [13usize, 16, 17, 64, 65, 4_096] {
            let mut cv = CompactValue::from_slice(&vec![b'a'; len]);
            {
                let m = cv.as_bytes_mut().expect("heap string is mutable");
                assert_eq!(m.len(), len);
                m[0] = b'X';
                m[len - 1] = b'Y';
            }
            let got = cv.as_bytes().unwrap();
            assert_eq!(got.len(), len);
            assert_eq!(got[0], b'X');
            assert_eq!(got[len - 1], b'Y');
            assert!(got[1..len - 1].iter().all(|&b| b == b'a'));
        }
        assert!(
            CompactValue::inline_string(b"short")
                .as_bytes_mut()
                .is_none()
        );
    }

    /// The length of a heap string is split across `len_and_tag` (high 28
    /// bits) and `payload[0..4]` (low 32 bits). The codec must be exact at
    /// every boundary of that split — a 4 GiB value cannot be allocated in a
    /// unit test, so the pure functions are tested directly.
    #[test]
    fn heap_len_codec_is_exact_across_60_bits() {
        let cases: &[usize] = &[
            SSO_MAX_LEN + 1,
            u32::MAX as usize - 1,
            u32::MAX as usize,
            u32::MAX as usize + 1,
            1 << 32,
            (1 << 32) + 12_345,
            (1 << 40) | 0xDEAD_BEEF,
            (1 << 60) - 1,
        ];
        for &len in cases {
            let (hi, lo) = encode_heap_len(len);
            assert_eq!(hi & !LEN_MASK, 0, "high part must fit in LEN_MASK");
            assert_eq!(
                decode_heap_len(hi, u32::from_ne_bytes(lo)),
                len,
                "len={len:#x}"
            );
        }
        // The high nibble is the type marker and must never be touched by
        // the length, whatever the length is.
        let (hi, _) = encode_heap_len((1 << 60) - 1);
        assert_eq!(hi & TYPE_MASK, 0);
    }

    /// `estimate_memory` on a heap string must be a function of the 16-byte
    /// value alone. The proof: the value reports the same figure after its
    /// heap buffer has been overwritten byte-for-byte through `as_bytes_mut`
    /// — nothing on the heap encodes the length, so nothing on the heap can
    /// feed the estimate.
    #[test]
    fn heap_string_estimate_is_read_from_the_value_not_the_heap() {
        for len in [13usize, 64, 100, 4_097] {
            let mut cv = CompactValue::from_slice(&vec![0u8; len]);
            let before = cv.estimate_memory();
            cv.as_bytes_mut().unwrap().fill(0xFF);
            assert_eq!(cv.estimate_memory(), before, "len={len}");
            assert_eq!(cv.heap_string_len(), len);
            assert!(
                cv.estimate_memory() >= crate::storage::mem_size::size_class(len),
                "must bill at least the data buffer's size class"
            );
        }
    }

    #[test]
    fn test_inline_string_small() {
        let cv = CompactValue::inline_string(b"hello");
        assert!(cv.is_inline());
        assert_eq!(cv.as_bytes().unwrap(), b"hello");
        assert_eq!(cv.type_name(), "string");
        assert_eq!(cv.estimate_memory(), 5);
    }

    #[test]
    fn test_inline_string_empty() {
        let cv = CompactValue::inline_string(b"");
        assert!(cv.is_inline());
        assert_eq!(cv.as_bytes().unwrap(), b"");
        assert_eq!(cv.estimate_memory(), 0);
    }

    #[test]
    fn test_inline_string_max() {
        let data = b"123456789012"; // exactly 12 bytes
        let cv = CompactValue::inline_string(data);
        assert!(cv.is_inline());
        assert_eq!(cv.as_bytes().unwrap(), data);
    }

    #[test]
    fn test_from_redis_value_small_string() {
        let rv = RedisValue::String(Bytes::from_static(b"tiny"));
        let cv = CompactValue::from_redis_value(rv);
        assert!(cv.is_inline());
        assert_eq!(cv.as_bytes().unwrap(), b"tiny");
    }

    #[test]
    fn test_from_redis_value_large_string() {
        let rv = RedisValue::String(Bytes::from_static(b"this is a longer string"));
        let cv = CompactValue::from_redis_value(rv);
        assert!(!cv.is_inline());
        assert_eq!(cv.as_bytes().unwrap(), b"this is a longer string");
        assert_eq!(cv.type_name(), "string");
    }

    #[test]
    fn test_from_redis_value_hash() {
        let mut map = HashMap::new();
        map.insert(Bytes::from_static(b"k"), Bytes::from_static(b"v"));
        let rv = RedisValue::Hash(map);
        let cv = CompactValue::from_redis_value(rv);
        assert!(!cv.is_inline());
        assert_eq!(cv.type_name(), "hash");
        match cv.as_redis_value() {
            RedisValueRef::Hash(m) => assert_eq!(m.len(), 1),
            _ => panic!("Expected hash"),
        }
    }

    #[test]
    fn test_from_redis_value_list() {
        let mut list = VecDeque::new();
        list.push_back(Bytes::from_static(b"a"));
        let rv = RedisValue::List(list);
        let cv = CompactValue::from_redis_value(rv);
        assert!(!cv.is_inline());
        assert_eq!(cv.type_name(), "list");
    }

    #[test]
    fn test_from_redis_value_set() {
        let mut set = crate::storage::entry::SetValue::new();
        set.insert(Bytes::from_static(b"x"));
        let rv = RedisValue::Set(set);
        let cv = CompactValue::from_redis_value(rv);
        assert!(!cv.is_inline());
        assert_eq!(cv.type_name(), "set");
    }

    #[test]
    fn test_from_redis_value_sorted_set() {
        let rv = RedisValue::SortedSet {
            members: HashMap::new(),
            scores: BTreeMap::new(),
        };
        let cv = CompactValue::from_redis_value(rv);
        assert!(!cv.is_inline());
        assert_eq!(cv.type_name(), "zset");
    }

    #[test]
    fn test_into_redis_value_inline() {
        let cv = CompactValue::inline_string(b"hello");
        let rv = cv.into_redis_value();
        match rv {
            RedisValue::String(s) => assert_eq!(s.as_ref(), b"hello"),
            _ => panic!("Expected string"),
        }
    }

    #[test]
    fn test_into_redis_value_heap() {
        let rv = RedisValue::String(Bytes::from_static(b"this is a longer string value"));
        let cv = CompactValue::from_redis_value(rv);
        let rv_back = cv.into_redis_value();
        match rv_back {
            RedisValue::String(s) => assert_eq!(s.as_ref(), b"this is a longer string value"),
            _ => panic!("Expected string"),
        }
    }

    #[test]
    fn test_to_redis_value_clone() {
        let rv = RedisValue::String(Bytes::from_static(b"value"));
        let cv = CompactValue::from_redis_value(rv);
        let cloned = cv.to_redis_value();
        // Original should still work
        assert_eq!(cv.as_bytes().unwrap(), b"value");
        match cloned {
            RedisValue::String(s) => assert_eq!(s.as_ref(), b"value"),
            _ => panic!("Expected string"),
        }
    }

    #[test]
    fn test_clone_inline() {
        let cv = CompactValue::inline_string(b"hello");
        let cv2 = cv.clone();
        assert_eq!(cv.as_bytes().unwrap(), cv2.as_bytes().unwrap());
    }

    #[test]
    fn test_clone_heap() {
        let rv = RedisValue::String(Bytes::from_static(b"this is a longer heap string"));
        let cv = CompactValue::from_redis_value(rv);
        let cv2 = cv.clone();
        assert_eq!(cv.as_bytes().unwrap(), cv2.as_bytes().unwrap());
    }

    #[test]
    fn test_as_redis_value_mut_inline_returns_none() {
        let mut cv = CompactValue::inline_string(b"hi");
        assert!(cv.as_redis_value_mut().is_none());
    }

    #[test]
    fn test_as_redis_value_mut_heap() {
        let rv = RedisValue::Hash(HashMap::new());
        let mut cv = CompactValue::from_redis_value(rv);
        let inner = cv.as_redis_value_mut().unwrap();
        if let RedisValue::Hash(map) = inner {
            map.insert(Bytes::from_static(b"k"), Bytes::from_static(b"v"));
        }
        match cv.as_redis_value() {
            RedisValueRef::Hash(m) => assert_eq!(m.len(), 1),
            _ => panic!("Expected hash"),
        }
    }

    #[test]
    fn test_debug_format() {
        let cv = CompactValue::inline_string(b"test");
        let s = format!("{:?}", cv);
        assert!(s.contains("Inline"));
    }
}
