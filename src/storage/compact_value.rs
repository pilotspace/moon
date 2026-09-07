//! CompactValue: 16-byte value representation with Small String Optimization (SSO)
//! and tagged heap pointers for collection types.
//!
//! Layout (`#[repr(C)]`, 16 bytes):
//! - `len_and_tag: u32` -- high nibble = kind, lower 28 bits = length bits
//! - `payload: [u8; 12]`
//!
//! Three kinds, told apart by the high nibble of `len_and_tag` ALONE — the
//! pointer is never dereferenced, and its bits are never inspected, to learn
//! what a value is:
//!
//! ```text
//! kind             nibble  len_and_tag[0..28]  payload[0..4]      payload[4..12]
//! inline string    0x0     length (<= 12)      data (payload[0..len])
//! heap string      0xE     length bits 32..60  length bits 0..32  untagged thin *mut u8 of ONE Box<[u8]>
//! heap collection  0xF     0                   0                  Box<RedisValue> ptr, type tag in low 3 bits
//! ```
//!
//! A heap string is a single allocation of exactly `len` bytes. Both things
//! needed to read it (`ptr`, `len`) and both things needed to free it (the
//! same two — `Box<[u8]>`'s layout is `len` bytes, align 1) live in the
//! 16-byte value, so dropping or overwriting a heap string touches nothing
//! on the heap. That is the point (perf campaign G1 §5): the previous
//! `Box<HeapString(Box<[u8]>)>` cost one wrapper allocation per value and a
//! dependent load through it on every GET, every overwrite and every drop.
//!
//! The string pointer carries no tag bits, so the layout makes NO assumption
//! about the alignment the allocator gives a `[u8]` — none is guaranteed for
//! an align-1 request, and a tag squeezed into such a pointer would be sound
//! only by jemalloc's grace. Collections keep their tagged pointer:
//! `Box<RedisValue>` is 8-aligned by the language, so its low 3 bits are
//! free by contract.

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

const TYPE_MASK: u32 = 0xF000_0000; // high nibble of len_and_tag = kind
const HEAP_BIT: u32 = 0x8000_0000; // set for both heap kinds, clear for inline
const HEAP_STRING_MARKER: u32 = 0xE000_0000; // heap string: thin ptr + 60-bit length
const HEAP_MARKER: u32 = 0xF000_0000; // heap collection: tagged Box<RedisValue>
const LEN_MASK: u32 = 0x0FFF_FFFF; // lower 28 bits = length bits
const SSO_MAX_LEN: usize = 12;

// Inline type tags (high nibble of len_and_tag, for SSO values)
const TAG_STRING: u32 = 0x0000_0000; // 0 in high nibble

// Every kind is decided by the high nibble; the markers must agree with
// `HEAP_BIT`, which `is_inline` tests on its own.
const _: () = assert!(HEAP_STRING_MARKER & HEAP_BIT != 0);
const _: () = assert!(HEAP_MARKER & HEAP_BIT != 0);
const _: () = assert!(TAG_STRING & HEAP_BIT == 0);
const _: () = assert!(HEAP_STRING_MARKER & LEN_MASK == 0 && HEAP_MARKER & LEN_MASK == 0);

// Heap type tags. For collections they live in the low 3 bits of the
// `Box<RedisValue>` pointer. `HEAP_TAG_STRING` is only the discriminant
// `type_tag` REPORTS for a string — a heap string's pointer carries no tag.
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
///
/// # Invariants of the heap-string kind
///
/// Whenever `len_and_tag & TYPE_MASK == HEAP_STRING_MARKER`:
///
/// 1. `payload[4..12]` holds the address returned by `Box::<[u8]>::into_raw`
///    for a boxed slice of exactly [`Self::heap_string_len`] bytes, cast to
///    a thin pointer — so the range `[ptr, ptr + len)` is one live,
///    initialised allocation whose layout is `Layout::array::<u8>(len)`.
/// 2. That allocation is owned by this value alone. It is released exactly
///    once, and only through [`Self::take_heap_string`], which resets the
///    value to an empty inline string before re-owning the box.
/// 3. The length (`payload[0..4]` low bits, `len_and_tag & LEN_MASK` high
///    bits) is immutable for the life of the value: `as_bytes_mut` hands out
///    a fixed-length slice and every length-changing writer replaces the
///    whole `CompactValue`.
///
/// All three are established in one place, [`Self::heap_string_boxed`], and
/// every `unsafe` block on the string path cites them by number.
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

    /// Heap string: one `Box<[u8]>`, pointer and length both in the value.
    #[inline]
    fn is_heap_string(&self) -> bool {
        (self.len_and_tag & TYPE_MASK) == HEAP_STRING_MARKER
    }

    /// Heap collection: a tagged `Box<RedisValue>`.
    #[inline]
    fn is_collection(&self) -> bool {
        (self.len_and_tag & TYPE_MASK) == HEAP_MARKER
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
    /// Strings > 12 bytes are stored as one `Box<[u8]>` (raw bytes, no
    /// wrapper of any kind). Collections are stored as `Box<RedisValue>`.
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
        let raw_ptr = Box::into_raw(boxed).expose_provenance();
        debug_assert!(
            raw_ptr & HEAP_TAG_MASK == 0,
            "Box pointer insufficiently aligned"
        );
        let tagged_ptr = raw_ptr | heap_tag;

        let mut payload = [0u8; 12];
        // No length for collections
        payload[4..12].copy_from_slice(&tagged_ptr.to_ne_bytes());

        CompactValue {
            len_and_tag: HEAP_MARKER,
            payload,
        }
    }

    /// Create a heap-allocated string CompactValue from a byte slice (copies data).
    ///
    /// One allocation of exactly `data.len()` bytes: `Box<[u8]>: From<&[u8]>`
    /// allocates the boxed slice directly, with no intermediate `Vec`.
    pub fn heap_string(data: &[u8]) -> Self {
        Self::heap_string_boxed(Box::from(data))
    }

    /// Create from owned Bytes (converts to Vec<u8> via Bytes::into for zero-copy
    /// when Bytes has unique ownership, or copies when shared).
    pub fn heap_string_owned(data: Bytes) -> Self {
        // Bytes::into::<Vec<u8>> is zero-copy when refcount == 1, copies otherwise
        Self::heap_string_vec(data.into())
    }

    /// Create from an owned Vec<u8> directly — no copy, no refcount.
    /// This is the fastest path: the vector's buffer is adopted as-is when
    /// `capacity == len`, and trimmed once otherwise.
    /// Public for RDB loader fast path.
    pub fn heap_string_vec_direct(data: Vec<u8>) -> Self {
        if data.len() <= SSO_MAX_LEN {
            return Self::inline_string(&data);
        }
        Self::heap_string_vec(data)
    }

    fn heap_string_vec(data: Vec<u8>) -> Self {
        debug_assert!(data.len() > SSO_MAX_LEN);
        // `into_boxed_slice` is a no-op when `capacity == len`, which is the case
        // for every hot-path caller: `heap_string_owned` goes through
        // `Bytes -> Vec`, which copies into an exactly-sized buffer whenever the
        // `Bytes` is shared (always true for a value parsed out of the
        // connection's read buffer). When capacity does exceed len it
        // reallocates once here, in exchange for not stranding the excess for
        // the lifetime of the key.
        Self::heap_string_boxed(data.into_boxed_slice())
    }

    /// The ONE place the heap-string invariants (see the type docs) are
    /// established. Every string constructor ends here.
    fn heap_string_boxed(data: Box<[u8]>) -> Self {
        let len = data.len();
        debug_assert!(len > SSO_MAX_LEN);
        let (len_hi, len_lo) = encode_heap_len(len);

        // `Box::<[u8]>::into_raw` returns a FAT `*mut [u8]`: address plus
        // length. The length half is what `len_hi`/`len_lo` carry from now
        // on, so only the address is kept (invariant 1). Casting a pointer
        // is safe; it is only its later use that is not, and the three
        // places that use it each re-derive the fat pointer from these two
        // fields. `expose_provenance` is the strict-provenance spelling of
        // `as usize`, so Miri can pair it with the `with_exposed_provenance`
        // in `heap_string_ptr`.
        let ptr: *mut u8 = Box::into_raw(data).cast::<u8>();
        let addr = ptr.expose_provenance();

        let mut payload = [0u8; 12];
        payload[..4].copy_from_slice(&len_lo);
        payload[4..12].copy_from_slice(&addr.to_ne_bytes());

        CompactValue {
            len_and_tag: HEAP_STRING_MARKER | len_hi,
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
        debug_assert!(self.is_heap_string());
        let lo = u32::from_ne_bytes([
            self.payload[0],
            self.payload[1],
            self.payload[2],
            self.payload[3],
        ]);
        decode_heap_len(self.len_and_tag & LEN_MASK, lo)
    }

    /// Thin pointer to a heap string's buffer (invariant 1). Reading it is
    /// safe; every dereference lives in one of the three helpers below.
    #[inline]
    #[allow(clippy::unwrap_used)] // payload[4..12] is exactly 8 bytes — try_into::<[u8; 8]> is infallible
    fn heap_string_ptr(&self) -> *mut u8 {
        debug_assert!(self.is_heap_string());
        let addr = usize::from_ne_bytes(self.payload[4..12].try_into().unwrap());
        std::ptr::with_exposed_provenance_mut(addr)
    }

    /// The bytes of a heap string.
    ///
    /// Soundness rests on the type's invariants 1-2: `heap_string_ptr()` is
    /// the start of one live, initialised `Box<[u8]>` of exactly
    /// `heap_string_len()` bytes that this value alone owns, so
    /// `[ptr, ptr + len)` is in-bounds and readable, and a borrow tied to
    /// `&self` cannot coexist with a `&mut` to those bytes.
    #[inline]
    fn heap_str(&self) -> &[u8] {
        debug_assert!(self.is_heap_string());
        // SAFETY: (a) ptr/len name one live owned `Box<[u8]>` — type invariants 1-2,
        // established in `heap_string_boxed`; (b) the borrow is tied to `&self`, the sole
        // owner, so no `&mut` aliases it; (c) a stale ptr or wrong len would read out of bounds.
        unsafe { std::slice::from_raw_parts(self.heap_string_ptr(), self.heap_string_len()) }
    }

    /// The bytes of a heap string, mutably. Fixed length (invariant 3).
    ///
    /// Validity and bounds are as for [`Self::heap_str`] (invariant 1);
    /// uniqueness comes from `&mut self` being the only path to the
    /// allocation (invariant 2), so no other reference to the bytes exists
    /// while the returned borrow lives.
    #[inline]
    fn heap_str_mut(&mut self) -> &mut [u8] {
        debug_assert!(self.is_heap_string());
        // SAFETY: (a) ptr/len name one live owned `Box<[u8]>` — type invariants 1-2; (b) `&mut
        // self` is the only path to it, so the returned `&mut` is unique for its lifetime;
        // (c) a second live `&mut` to the same bytes, or a wrong len, would be aliasing/OOB UB.
        unsafe { std::slice::from_raw_parts_mut(self.heap_string_ptr(), self.heap_string_len()) }
    }

    /// Take a heap string's allocation back as the `Box<[u8]>` it was built
    /// from, leaving `self` an empty inline string so that `Drop` — or any
    /// later call — finds nothing to free.
    ///
    /// This is the ONLY way the string path releases memory: `Drop` and
    /// `into_redis_value` both go through it, so each allocation is freed
    /// exactly once by construction (invariant 2), and the free needs
    /// nothing from the heap — `Box<[u8]>`'s layout is `(len, align 1)`, both
    /// known from the value.
    #[inline]
    fn take_heap_string(&mut self) -> Box<[u8]> {
        debug_assert!(self.is_heap_string());
        let ptr = self.heap_string_ptr();
        let len = self.heap_string_len();
        // Field writes, not `*self = ...`: an assignment would run `Drop` on
        // the old value first and free the very allocation being taken.
        // After this, `self` no longer refers to the allocation, so the
        // `Box::from_raw` below is the one and only re-owning of it.
        self.len_and_tag = TAG_STRING;
        self.payload = [0u8; 12];
        // SAFETY: (a) `ptr`+`len` are what `Box::<[u8]>::into_raw` returned, so the rebuilt fat
        // pointer has the box's own layout — type invariant 1; (b) `self` was reset above, so
        // ownership moves exactly once — invariant 2; (c) re-owning twice would double free.
        unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len)) }
    }

    /// Get the tagged pointer from a heap collection.
    #[inline]
    #[allow(clippy::unwrap_used)] // payload[4..12] is exactly 8 bytes — try_into::<[u8; 8]> is infallible
    fn heap_tagged_ptr(&self) -> usize {
        debug_assert!(self.is_collection());
        usize::from_ne_bytes(self.payload[4..12].try_into().unwrap())
    }

    /// Get the raw pointer to a heap RedisValue (collections only — NOT strings).
    #[inline]
    fn heap_collection_ptr(&self) -> *mut RedisValue {
        debug_assert!(self.is_collection());
        std::ptr::with_exposed_provenance_mut(self.heap_tagged_ptr() & !HEAP_TAG_MASK)
    }

    /// Get the collection's type tag from the low 3 bits of its pointer.
    #[inline]
    fn heap_type_tag(&self) -> usize {
        debug_assert!(self.is_collection());
        self.heap_tagged_ptr() & HEAP_TAG_MASK
    }

    /// Borrow the underlying RedisValue as a RedisValueRef for zero-copy reads.
    pub fn as_redis_value(&self) -> RedisValueRef<'_> {
        if self.is_inline() {
            let len = self.inline_len();
            RedisValueRef::String(&self.payload[..len])
        } else if self.is_heap_string() {
            RedisValueRef::String(self.heap_str())
        } else {
            // Collection path: Box<RedisValue>
            // SAFETY: Kind is collection, so the pointer was created from Box::into_raw(Box<RedisValue>)
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
                RedisValue::String(_) => unreachable!("strings never live in a Box<RedisValue>"),
            }
        }
    }

    /// Fast path: get string bytes (returns None for non-string types).
    pub fn as_bytes(&self) -> Option<&[u8]> {
        if self.is_inline() {
            let len = self.inline_len();
            Some(&self.payload[..len])
        } else if self.is_heap_string() {
            Some(self.heap_str())
        } else {
            None
        }
    }

    /// Fast path: get string bytes as owned Bytes.
    /// For heap strings, copies from the heap buffer.
    /// For inline SSO strings (<=12 bytes), copies from inline buffer.
    /// Returns None for non-string types.
    pub fn as_bytes_owned(&self) -> Option<Bytes> {
        if self.is_inline() {
            let len = self.inline_len();
            Some(Bytes::copy_from_slice(&self.payload[..len]))
        } else if self.is_heap_string() {
            Some(Bytes::copy_from_slice(self.heap_str()))
        } else {
            None
        }
    }

    /// Get a mutable reference to the underlying heap RedisValue.
    /// Returns None for inline (SSO) values and for heap strings (use string-specific mutators).
    pub fn as_redis_value_mut(&mut self) -> Option<&mut RedisValue> {
        if self.is_inline() || self.is_heap_string() {
            None
        } else {
            // SAFETY: We own this pointer uniquely (no aliasing since we have &mut self)
            Some(unsafe { &mut *self.heap_collection_ptr() })
        }
    }

    /// Get a mutable reference to the heap string bytes.
    /// Returns None for non-string types and inline values.
    ///
    /// The slice is fixed-length on purpose: the value caches the string
    /// length alongside the pointer, so a caller that resized the buffer
    /// in place would desynchronise the two. Writers that change the length
    /// replace the whole `CompactValue`.
    pub fn as_bytes_mut(&mut self) -> Option<&mut [u8]> {
        if self.is_heap_string() {
            Some(self.heap_str_mut())
        } else {
            None
        }
    }

    /// Consuming conversion: returns the owned RedisValue.
    /// For inline strings, allocates a new Bytes.
    /// For heap strings, adopts the buffer into `Bytes` (no copy).
    /// For collections, reconstructs the Box and extracts the value.
    pub fn into_redis_value(mut self) -> RedisValue {
        if self.is_inline() {
            let len = self.inline_len();
            RedisValue::String(Bytes::copy_from_slice(&self.payload[..len]))
        } else if self.is_heap_string() {
            // `take_heap_string` leaves `self` inline, so the drop at the end
            // of this scope frees nothing.
            RedisValue::String(Bytes::from(self.take_heap_string()))
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
        } else if self.is_heap_string() {
            RedisValue::String(Bytes::copy_from_slice(self.heap_str()))
        } else {
            // SAFETY: Kind is collection; pointer from Box::into_raw is valid and not freed.
            let rv = unsafe { &*self.heap_collection_ptr() };
            rv.clone()
        }
    }

    /// Return the Redis type name for this value.
    pub fn type_name(&self) -> &'static str {
        if self.is_inline() || self.is_heap_string() {
            "string"
        } else {
            match self.heap_type_tag() {
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
        if self.is_inline() || self.is_heap_string() {
            HEAP_TAG_STRING as u8
        } else {
            self.heap_type_tag() as u8
        }
    }

    /// Estimate memory usage of this value in bytes.
    pub fn estimate_memory(&self) -> usize {
        if self.is_inline() {
            self.inline_len()
        } else if self.is_heap_string() {
            // One allocation of exactly `len` bytes, billed as the class
            // jemalloc actually hands out for it (moon#788). The length
            // comes from the value, not the heap: no pointer chase here.
            crate::storage::mem_size::size_class(self.heap_string_len())
        } else {
            // SAFETY: Kind is collection; pointer from Box::into_raw is valid and not freed.
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
        if self.is_inline() {
            return;
        }
        if self.is_heap_string() {
            // The free needs only (ptr, len), both in `self`: nothing on the
            // heap is read on the way out.
            drop(self.take_heap_string());
        } else {
            // SAFETY: collections are Box<RedisValue>
            unsafe {
                drop(Box::from_raw(self.heap_collection_ptr()));
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
        } else if self.is_heap_string() {
            Self::heap_string(self.heap_str())
        } else {
            // SAFETY: Kind is collection; pointer from Box::into_raw is valid and not freed.
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

// SAFETY: CompactValue owns exactly what a `Box<[u8]>` (heap string) or a
// `Box<RedisValue>` (collection) would own, both of which are Send + Sync,
// and inline values are plain bytes; the raw pointer is only an owned box's
// address, never shared, so moving or sharing the value across threads is
// exactly as sound as moving or sharing the box.
unsafe impl Send for CompactValue {}
unsafe impl Sync for CompactValue {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_of_compact_value() {
        assert_eq!(std::mem::size_of::<CompactValue>(), 16);
    }

    /// `estimate_memory` feeds `Database::used_memory`, which gates `--maxmemory`
    /// and per-db quotas. A heap string is ONE allocation of `len` bytes, so
    /// its bill is `size_class(len)` and nothing else — the 16-byte wrapper
    /// #786 billed no longer exists. Exactness at the class boundaries is what
    /// `tests/compact_value_alloc_accounting.rs` proves against a counting
    /// allocator; this pins the formula.
    #[test]
    fn heap_string_estimate_memory_bills_one_size_class() {
        use crate::storage::mem_size::size_class;
        for (len, class) in [
            (13usize, 16usize),
            (16, 16),
            (17, 32),
            (64, 64),
            (65, 80),
            (100, 112),
        ] {
            let cv = CompactValue::heap_string(&vec![b'x'; len]);
            assert!(!cv.is_inline());
            assert_eq!(size_class(len), class, "fixture: class table");
            assert_eq!(cv.estimate_memory(), class, "len={len}");
        }
    }

    /// The kind of a heap string is in `len_and_tag`, never in the pointer:
    /// the pointer is untagged, so no alignment is assumed of the allocator,
    /// and `type_name`/`type_tag`/`Drop` decide what a value is without
    /// touching the heap.
    #[test]
    fn heap_string_kind_lives_in_len_and_tag_not_in_the_pointer() {
        let cv = CompactValue::heap_string(b"thirteen bytes!");
        assert_eq!(cv.len_and_tag & TYPE_MASK, HEAP_STRING_MARKER);
        assert!(cv.is_heap_string() && !cv.is_inline() && !cv.is_collection());
        assert_eq!(cv.type_name(), "string");
        assert_eq!(cv.type_tag(), 0);
        // `payload[4..12]` is the raw address; it round-trips through the
        // helper unchanged, low bits included.
        let raw = usize::from_ne_bytes(cv.payload[4..12].try_into().unwrap());
        assert_eq!(cv.heap_string_ptr().addr(), raw);

        let coll = CompactValue::from_redis_value(RedisValue::List(VecDeque::new()));
        assert_eq!(coll.len_and_tag & TYPE_MASK, HEAP_MARKER);
        assert!(coll.is_collection() && !coll.is_heap_string() && !coll.is_inline());

        let inl = CompactValue::inline_string(b"tiny");
        assert_eq!(inl.len_and_tag & TYPE_MASK, TAG_STRING);
        assert!(inl.is_inline() && !inl.is_heap_string() && !inl.is_collection());
    }

    /// `take_heap_string` resets the value before re-owning the box, so a
    /// value that has been consumed by `into_redis_value` is inert: the
    /// `Bytes` it produced owns the buffer, and nothing is freed twice.
    #[test]
    fn into_redis_value_hands_the_buffer_over_exactly_once() {
        let data = vec![0xABu8; 300];
        let cv = CompactValue::from_slice(&data);
        let RedisValue::String(bytes) = cv.into_redis_value() else {
            panic!("string")
        };
        assert_eq!(&bytes[..], &data[..]);
        drop(bytes);

        let mut cv = CompactValue::from_slice(&data);
        let taken = cv.take_heap_string();
        assert!(cv.is_inline(), "reset before the box is re-owned");
        assert_eq!(cv.as_bytes(), Some(&b""[..]));
        assert_eq!(&taken[..], &data[..]);
        drop(cv); // inline now: nothing to free
        drop(taken);
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
