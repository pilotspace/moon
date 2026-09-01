//! CompactValue: 16-byte value representation with Small String Optimization
//! (SSO) for short strings and an owned heap pointer for everything else.
//!
//! # Layout
//!
//! ```text
//! len_and_tag: u32
//!   bit 31      HEAP_BIT      1 = heap-allocated, 0 = inline (SSO)
//!   bits 30..28 heap type tag string / hash / list / set / zset / stream
//!   bits 27..0  inline: byte length (0..=12)
//!               heap string: the HIGH 28 bits of the byte length
//!               collection: 0
//! payload: [u8; 12]
//!   inline      bytes 0..len  the string data itself
//!   heap        bytes 0..8    the raw, UNTAGGED owning pointer
//!               bytes 8..12   heap string: the LOW 32 bits of the byte length
//!                             collection: 0
//! ```
//!
//! # One allocation per string, like Redis `embstr`
//!
//! A heap string owns **exactly one** allocation, of **exactly `len` bytes** —
//! the string data and nothing else. There is no wrapper object and no inline
//! length header, because `CompactValue` already has 12 bytes of payload in
//! which to keep both the pointer and the length.
//!
//! The representation this replaced stored a `Box<HeapString>`, i.e. a boxed
//! `Box<[u8]>` fat pointer. That is a second allocation of 16 bytes, which
//! lands in jemalloc's 16-byte size class and is therefore billed in full
//! against **every key** whose value exceeds [`SSO_MAX_LEN`] — a flat 16 B/key.
//! An inline `[len: u32][data]` header would not have recovered it: at 13, 64
//! and 96 bytes the 4-byte header pushes the block into the *next* size class
//! and gives back exactly the 16 bytes it saved (measured with `nallocx`; see
//! `tests/compact_value_one_allocation.rs` for the allocation-count harness).
//!
//! # Why the type tag is not in the pointer
//!
//! The old layout kept the tag in the pointer's low 3 bits. That was sound only
//! while every heap payload was a `Box<T>` with `align_of::<T>() >= 8`. The
//! string allocation is a `[u8]`, whose alignment is **1**, so its address
//! carries no spare bits by any language-level guarantee. The tag therefore
//! lives in `len_and_tag`, and the stored pointer is the raw address, untagged.

use bytes::Bytes;
use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;

use super::bptree::BPTree;
use super::entry::RedisValue;
use super::intset::Intset;
use super::listpack::Listpack;
use super::stream::Stream as StreamData;

// ---- Constants ----

/// Bit 31 of `len_and_tag`: set means the payload holds an owning heap pointer.
const HEAP_BIT: u32 = 0x8000_0000;
/// Bits 30..28 of `len_and_tag` hold the heap type tag.
const HEAP_TAG_SHIFT: u32 = 28;
const HEAP_TAG_BITS: u32 = 0x7;
/// Bits 27..0 of `len_and_tag`: inline length, or a heap string's high length bits.
const LEN_MASK: u32 = 0x0FFF_FFFF;
const SSO_MAX_LEN: usize = 12;

// Inline tag bits (bit 31 clear, bits 30..28 zero).
const TAG_STRING: u32 = 0x0000_0000;

// Heap type tags, stored in bits 30..28 of `len_and_tag` — never in the pointer.
// Tag 0 = a raw byte buffer (NOT a Box<RedisValue>!).
const HEAP_TAG_STRING: u32 = 0;
const HEAP_TAG_HASH: u32 = 1;
const HEAP_TAG_LIST: u32 = 2;
const HEAP_TAG_SET: u32 = 3;
const HEAP_TAG_ZSET: u32 = 4;
const HEAP_TAG_STREAM: u32 = 5;

/// Widest heap string this encoding can represent: 28 high bits in
/// `len_and_tag` plus 32 low bits in the payload = 60 bits, or 1 EiB.
///
/// This is not a policy limit, it is a *structural* one, and it cannot be
/// reached: the length is read back off a `Box<[u8]>` that was successfully
/// allocated, and no 64-bit target moon builds for has more than 57 bits of
/// virtual address space (x86-64 LA57; aarch64 tops out at 52). moon's own
/// protocol cap is 512 MiB (`protocol::frame::DEFAULT_MAX_BULK_STRING_SIZE`),
/// eight orders of magnitude below this. A 32-bit-only field would NOT have
/// been safe: 4 GiB is reachable on real hardware, and a truncated length
/// would make `Box::from_raw` free the wrong number of bytes.
const MAX_HEAP_STR_LEN: usize = (1usize << 60) - 1;

// The payload must be able to hold a pointer in its first 8 bytes.
const _: () = assert!(std::mem::size_of::<usize>() == 8);

/// Split a heap string length into (high bits for `len_and_tag`, low 4 bytes).
#[inline]
fn encode_str_len(len: usize) -> (u32, [u8; 4]) {
    debug_assert!(len <= MAX_HEAP_STR_LEN);
    (((len >> 32) as u32) & LEN_MASK, (len as u32).to_ne_bytes())
}

/// Inverse of [`encode_str_len`].
#[inline]
fn decode_str_len(len_and_tag: u32, low: [u8; 4]) -> usize {
    (((len_and_tag & LEN_MASK) as usize) << 32) | u32::from_ne_bytes(low) as usize
}

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
    Set(&'a HashSet<Bytes>),
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
                if s.len() <= 20
                    && std::str::from_utf8(s)
                        .ok()
                        .and_then(|ss| ss.parse::<i64>().ok())
                        .is_some()
                {
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
        (self.len_and_tag & HEAP_BIT) == 0
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

        let raw_ptr = Box::into_raw(Box::new(value)).cast::<u8>();
        // A collection has no length to carry: `RedisValue` knows its own.
        Self::encode_heap(heap_tag, raw_ptr, 0)
    }

    /// Assemble the heap representation from its three components.
    ///
    /// The single place a `CompactValue` takes ownership of a raw pointer, and
    /// the only place the tag/length encoding is written. `len` is meaningful
    /// for [`HEAP_TAG_STRING`] only; every collection passes 0.
    #[inline]
    fn encode_heap(tag: u32, ptr: *mut u8, len: usize) -> Self {
        debug_assert!(tag <= HEAP_TAG_BITS);
        debug_assert!(!ptr.is_null());
        let (len_hi, len_lo) = encode_str_len(len);
        let mut payload = [0u8; 12];
        // `expose_provenance` is the sanctioned counterpart to the
        // `with_exposed_provenance_mut` in `heap_ptr`. The address has to make
        // a round trip through an integer because it is stored *unaligned*, at
        // byte offset 4 of a 16-byte struct: a real `*mut u8` field would force
        // the payload to 8-byte alignment and blow `CompactValue` out to 24
        // bytes, which is the entire point of this type.
        payload[..8].copy_from_slice(&ptr.expose_provenance().to_ne_bytes());
        payload[8..].copy_from_slice(&len_lo);
        CompactValue {
            len_and_tag: HEAP_BIT | (tag << HEAP_TAG_SHIFT) | len_hi,
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
    /// The fastest path: the `Vec`'s own buffer becomes the value's single
    /// allocation. Public for the RDB loader fast path.
    pub fn heap_string_vec_direct(data: Vec<u8>) -> Self {
        if data.len() <= SSO_MAX_LEN {
            return Self::inline_string(&data);
        }
        Self::heap_string_vec(data)
    }

    fn heap_string_vec(data: Vec<u8>) -> Self {
        // `into_boxed_slice` is a no-op when `capacity == len`, which is the case
        // for every hot-path caller: `heap_string` copies via `to_vec`, and
        // `heap_string_owned` goes through `Bytes -> Vec`, which copies into an
        // exactly-sized buffer whenever the `Bytes` is shared (always true for a
        // value parsed out of the connection's read buffer). When capacity does
        // exceed len it reallocates once here, in exchange for not stranding the
        // excess for the lifetime of the key.
        Self::heap_string_boxed(data.into_boxed_slice())
    }

    /// Take ownership of an exactly-sized byte buffer as the value's single
    /// heap allocation.
    fn heap_string_boxed(data: Box<[u8]>) -> Self {
        let len = data.len();
        debug_assert!(len > SSO_MAX_LEN);
        // `Box::into_raw` yields a `*mut [u8]`; `cast` drops the length
        // metadata, which `len_and_tag` + `payload[8..12]` now carry instead.
        Self::encode_heap(HEAP_TAG_STRING, Box::into_raw(data).cast::<u8>(), len)
    }

    // ── Reading the heap payload back ─────────────────────────────────────
    //
    // Everything below decodes the three fields written by `encode_heap`.
    // The decoders are safe; only `heap_str` and `take_heap_str` dereference.

    /// The raw owning pointer, exactly as stored — no tag bits to mask off.
    #[inline]
    #[allow(clippy::unwrap_used)] // payload[..8] is exactly 8 bytes — try_into::<[u8; 8]> is infallible
    fn heap_ptr(&self) -> *mut u8 {
        debug_assert!(!self.is_inline());
        let addr = usize::from_ne_bytes(self.payload[..8].try_into().unwrap());
        // Recovers the provenance `encode_heap` exposed for this address. Note
        // for future reviewers: because the pointer round-trips through an
        // integer, Miri cannot track its provenance precisely and
        // `-Zmiri-strict-provenance` will flag this line by design. The
        // representation this replaced stored the address the same way.
        std::ptr::with_exposed_provenance_mut::<u8>(addr)
    }

    /// Get the raw pointer to a heap RedisValue (collections only — NOT strings).
    #[inline]
    fn heap_collection_ptr(&self) -> *mut RedisValue {
        debug_assert!(self.heap_type_tag() != HEAP_TAG_STRING);
        self.heap_ptr().cast::<RedisValue>()
    }

    /// Byte length of a heap string (only valid for [`HEAP_TAG_STRING`]).
    #[inline]
    #[allow(clippy::unwrap_used)] // payload[8..12] is exactly 4 bytes — try_into::<[u8; 4]> is infallible
    fn heap_str_len(&self) -> usize {
        debug_assert!(self.heap_type_tag() == HEAP_TAG_STRING);
        decode_str_len(self.len_and_tag, self.payload[8..12].try_into().unwrap())
    }

    /// Fat pointer to the heap string buffer. Safe to build; not yet a borrow.
    #[inline]
    fn heap_str_raw(&self) -> *mut [u8] {
        std::ptr::slice_from_raw_parts_mut(self.heap_ptr(), self.heap_str_len())
    }

    /// Borrow the heap string buffer.
    ///
    /// # Where the invariant comes from
    ///
    /// The `(tag, pointer, length)` triple is written in exactly one place —
    /// [`CompactValue::encode_heap`] — and [`HEAP_TAG_STRING`] is passed to it
    /// from exactly one caller, [`CompactValue::heap_string_boxed`], alongside
    /// the `Box::into_raw` address of a `Box<[u8]>` and that box's own length.
    /// `Box::into_raw` guarantees the address is non-null, aligned for `u8`
    /// (alignment 1) and valid for `len` bytes. No other code path writes
    /// `len_and_tag` or `payload`, so a `HEAP_TAG_STRING` value always carries
    /// a consistent triple.
    ///
    /// The allocation is released only by [`CompactValue::take_heap_str`],
    /// which is private and reachable solely from `Drop` and
    /// `into_redis_value` — both of which consume the value. It therefore
    /// cannot have been freed while a `&self` borrow exists, and `&self` also
    /// excludes any concurrent `&mut`, so the returned borrow (whose lifetime
    /// is tied to `self`) never aliases a mutable one.
    #[inline]
    fn heap_str(&self) -> &[u8] {
        debug_assert!(!self.is_inline() && self.heap_type_tag() == HEAP_TAG_STRING);
        // SAFETY: per the type invariant above, the tag proves this pointer and
        // length came from `Box::into_raw(Box<[u8]>)`, the block is unfreed, and
        // `&self` bars a mutable alias. Otherwise: use-after-free / OOB read.
        unsafe { &*self.heap_str_raw() }
    }

    /// Reclaim the heap string buffer, leaving `self` an empty inline string.
    ///
    /// # Where the invariant comes from
    ///
    /// Same triple as [`CompactValue::heap_str`]: `raw` is the exact
    /// `Box::into_raw(Box<[u8]>)` pointer/length pair recorded by
    /// [`CompactValue::heap_string_boxed`], so `Box::from_raw` rebuilds a box
    /// over precisely the block, size and alignment the global allocator
    /// handed out.
    ///
    /// Uniqueness comes from `&mut self`, which excludes every other
    /// reference. This function is private and reachable only from `Drop` and
    /// `into_redis_value`, both of which consume the value; on top of that it
    /// resets `len_and_tag` to an empty inline string *before* the box
    /// escapes, so a second call — or a `Drop` running after
    /// `into_redis_value` — takes the inline branch and frees nothing.
    #[inline]
    fn take_heap_str(&mut self) -> Box<[u8]> {
        debug_assert!(!self.is_inline() && self.heap_type_tag() == HEAP_TAG_STRING);
        let raw = self.heap_str_raw();
        self.len_and_tag = TAG_STRING;
        // SAFETY: per the type invariant above, `raw` is the exact box this
        // value owns, `&mut self` makes ownership unique, and the reset on the
        // line above makes a repeat call impossible. Otherwise: double free.
        unsafe { Box::from_raw(raw) }
    }

    /// Get the heap type tag out of `len_and_tag` (never out of the pointer).
    #[inline]
    fn heap_type_tag(&self) -> u32 {
        debug_assert!(!self.is_inline());
        (self.len_and_tag >> HEAP_TAG_SHIFT) & HEAP_TAG_BITS
    }

    /// Borrow the underlying RedisValue as a RedisValueRef for zero-copy reads.
    pub fn as_redis_value(&self) -> RedisValueRef<'_> {
        if self.is_inline() {
            let len = self.inline_len();
            RedisValueRef::String(&self.payload[..len])
        } else if self.heap_type_tag() == HEAP_TAG_STRING {
            // String path: the buffer itself, no wrapper object.
            RedisValueRef::String(self.heap_str())
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
                RedisValue::String(_) => unreachable!("strings use the HEAP_TAG_STRING path"),
            }
        }
    }

    /// Fast path: get string bytes (returns None for non-string types).
    pub fn as_bytes(&self) -> Option<&[u8]> {
        if self.is_inline() {
            let len = self.inline_len();
            Some(&self.payload[..len])
        } else if self.heap_type_tag() == HEAP_TAG_STRING {
            Some(self.heap_str())
        } else {
            None
        }
    }

    /// Fast path: get string bytes as owned Bytes.
    /// For heap strings, copies out of the value's own buffer.
    /// For inline SSO strings (<=12 bytes), copies from inline buffer.
    /// Returns None for non-string types.
    pub fn as_bytes_owned(&self) -> Option<Bytes> {
        if self.is_inline() {
            let len = self.inline_len();
            Some(Bytes::copy_from_slice(&self.payload[..len]))
        } else if self.heap_type_tag() == HEAP_TAG_STRING {
            Some(Bytes::copy_from_slice(self.heap_str()))
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

    /// Consuming conversion: returns the owned RedisValue.
    /// For inline strings, allocates a new Bytes.
    /// For heap strings, hands the owned buffer straight to `Bytes`.
    /// For collections, reconstructs the Box and extracts the value.
    pub fn into_redis_value(self) -> RedisValue {
        if self.is_inline() {
            let len = self.inline_len();
            let data = Bytes::copy_from_slice(&self.payload[..len]);
            std::mem::forget(self);
            RedisValue::String(data)
        } else if self.heap_type_tag() == HEAP_TAG_STRING {
            // `take_heap_str` leaves `self` an empty inline string, so the
            // `ManuallyDrop` is belt-and-braces: neither path can double free.
            let mut me = std::mem::ManuallyDrop::new(self);
            RedisValue::String(Bytes::from(me.take_heap_str()))
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
            RedisValue::String(Bytes::copy_from_slice(self.heap_str()))
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
            // The value's whole heap footprint: one buffer of exactly `len`
            // bytes. There is no longer a wrapper allocation to bill.
            self.heap_str_len()
        } else {
            // SAFETY: Tag is a collection type; pointer from Box::into_raw is valid and not freed.
            let rv = unsafe { &*self.heap_collection_ptr() };
            rv.estimate_memory()
        }
    }
}

impl Drop for CompactValue {
    fn drop(&mut self) {
        if !self.is_inline() {
            if self.heap_type_tag() == HEAP_TAG_STRING {
                drop(self.take_heap_str());
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
            // Deep copy into a fresh, independently owned buffer.
            Self::heap_string_boxed(Box::<[u8]>::from(self.heap_str()))
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

// SAFETY: the only thing a CompactValue owns beyond plain inline bytes is one
// heap allocation — a `Box<[u8]>` for strings or a `Box<RedisValue>` for
// collections — reached through a raw pointer that no other value aliases
// (`Clone` deep-copies, and the two consuming paths reset the tag). Both boxed
// types are `Send + Sync`, so moving the value between threads or sharing `&`
// is exactly as sound as moving or sharing the box would be; the raw pointer
// alone is what costs us the automatic impls.
unsafe impl Send for CompactValue {}
unsafe impl Sync for CompactValue {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_of_compact_value() {
        assert_eq!(std::mem::size_of::<CompactValue>(), 16);
    }

    /// Every stored string longer than `SSO_MAX_LEN` used to cost TWO
    /// allocations: the data buffer, plus a `Box<HeapString>` wrapper holding
    /// the `Box<[u8]>` fat pointer. The wrapper is 16 bytes, so it was billed
    /// once per key against a jemalloc size class.
    ///
    /// jemalloc's 64-bit small classes, measured directly with `nallocx` on
    /// aarch64 (identical on x86_64 — the table is a function of `LG_QUANTUM`,
    /// which is 4 on both), are:
    ///
    ///     8, 16, 32, 48, 64, 80, 96, 112, 128, 160, 192, 224, 256, ...
    ///
    /// A 16-byte wrapper lands in the 16-byte class, so it cost a flat
    /// **16 bytes on every key** above the SSO cutoff. There is now no wrapper
    /// at all: `CompactValue` keeps the pointer in `payload[0..8]` and the
    /// length across `len_and_tag` and `payload[8..12]`, so a string value is
    /// ONE allocation of exactly `len` bytes.
    ///
    /// Note what an inline `[len: u32][data]` header would have done instead:
    /// `class(N + 4)` against the old `16 + class(N)` is a saving of zero at
    /// N = 13 (32 vs 32), 64 (80 vs 80) and 96 (112 vs 112) — the header pushes
    /// the block into the next class and gives back exactly what it saved.
    ///
    /// `tests/compact_value_one_allocation.rs` pins the allocation count and
    /// size directly, with a recording global allocator.
    #[test]
    fn heap_string_costs_no_wrapper_allocation() {
        // The payload must hold an 8-byte pointer and 4 bytes of length, and
        // the whole value must still be two words.
        assert_eq!(std::mem::size_of::<CompactValue>(), 16);

        let cv = CompactValue::heap_string(&[b'x'; 64]);
        assert!(!cv.is_inline());
        // The stored pointer IS the buffer address: no wrapper indirection,
        // and no tag bits to mask off.
        assert_eq!(
            cv.heap_ptr() as usize,
            cv.as_bytes().expect("string").as_ptr() as usize,
            "the payload pointer must be the data buffer itself"
        );
    }

    /// The type tag must live in `len_and_tag`, not in the pointer's low bits:
    /// a `[u8]` allocation has alignment 1 and carries no spare bits.
    #[test]
    fn type_tag_lives_in_len_and_tag_not_in_the_pointer() {
        let cases: [(CompactValue, u32, &str); 5] = [
            (
                CompactValue::heap_string(&[b'x'; 40]),
                HEAP_TAG_STRING,
                "string",
            ),
            (
                CompactValue::from_redis_value(RedisValue::Hash(HashMap::new())),
                HEAP_TAG_HASH,
                "hash",
            ),
            (
                CompactValue::from_redis_value(RedisValue::List(VecDeque::new())),
                HEAP_TAG_LIST,
                "list",
            ),
            (
                CompactValue::from_redis_value(RedisValue::Set(HashSet::new())),
                HEAP_TAG_SET,
                "set",
            ),
            (
                CompactValue::from_redis_value(RedisValue::SortedSet {
                    members: HashMap::new(),
                    scores: BTreeMap::new(),
                }),
                HEAP_TAG_ZSET,
                "zset",
            ),
        ];
        for (cv, tag, name) in cases {
            assert!(!cv.is_inline(), "{name} must be heap");
            assert_eq!(cv.heap_type_tag(), tag, "{name} tag");
            assert_eq!(cv.type_tag(), tag as u8, "{name} public tag");
            assert_eq!(cv.type_name(), name);
            // The pointer is stored exactly as the allocator returned it.
            if tag == HEAP_TAG_STRING {
                // For a string it IS the data buffer, with no indirection.
                assert_eq!(
                    cv.heap_ptr().cast_const(),
                    cv.as_bytes().expect("string").as_ptr(),
                    "{name}: stored pointer must be the buffer address"
                );
            } else {
                // For a collection, `Box::into_raw` is 8-aligned. Under the old
                // scheme a nonzero tag was OR-ed into these very bits; if any
                // survived here, this alignment check would fail.
                assert_eq!(
                    cv.heap_ptr() as usize % std::mem::align_of::<RedisValue>(),
                    0,
                    "{name}: pointer must carry no tag bits"
                );
            }
        }
    }

    /// The 60-bit length codec, exercised without allocating the lengths it
    /// encodes. 32 bits alone would not have been safe: 4 GiB is reachable on
    /// real hardware and a truncated length makes `Box::from_raw` free the
    /// wrong size.
    #[test]
    fn heap_string_length_codec_round_trips_past_four_gib() {
        for len in [
            0usize,
            1,
            12,
            13,
            64,
            u32::MAX as usize - 1,
            u32::MAX as usize,
            u32::MAX as usize + 1,
            (1usize << 40) + 12345,
            MAX_HEAP_STR_LEN,
        ] {
            let (hi, lo) = encode_str_len(len);
            assert_eq!(
                hi & !LEN_MASK,
                0,
                "{len}: high bits must fit the length field"
            );
            // The tag bits are OR-ed in beside `hi`; decoding must ignore them.
            for tag in 0..=HEAP_TAG_BITS {
                let word = HEAP_BIT | (tag << HEAP_TAG_SHIFT) | hi;
                assert_eq!(decode_str_len(word, lo), len, "len={len} tag={tag}");
                assert_eq!((word >> HEAP_TAG_SHIFT) & HEAP_TAG_BITS, tag);
            }
        }
    }

    /// `estimate_memory` feeds `Database::used_memory`, which gates
    /// `--maxmemory` and per-db quotas. It must bill what the allocator
    /// actually hands out — which no longer includes a wrapper.
    #[test]
    fn heap_string_estimate_memory_bills_only_the_buffer() {
        let cv = CompactValue::heap_string(&[b'x'; 64]);
        assert!(!cv.is_inline());
        assert_eq!(
            cv.estimate_memory(),
            64,
            "per-key charge is the buffer alone; the 16-byte wrapper is gone"
        );
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
        let mut set = HashSet::new();
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
