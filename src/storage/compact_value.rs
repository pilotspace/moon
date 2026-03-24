//! CompactValue: 16-byte value representation with Small String Optimization (SSO)
//! and tagged heap pointers for collection types.
//!
//! Layout:
//! - `len_and_tag: u32` -- high nibble encodes type tag, lower 28 bits encode length
//! - `payload: [u8; 12]` -- inline data (SSO) or prefix + tagged heap pointer
//!
//! SSO path (strings <= 12 bytes): data stored inline in `payload[0..len]`
//! Heap path (strings > 12 bytes or collections): `payload[0..4]` = prefix,
//!   `payload[4..12]` = tagged pointer (raw_ptr | type_tag_in_low_3_bits)

use bytes::Bytes;
use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;

use super::bptree::BPTree;
use super::entry::RedisValue;
use super::intset::Intset;
use super::listpack::Listpack;

// ---- Constants ----

const HEAP_MARKER: u32 = 0xF0000000; // high nibble = 0xF means heap-allocated
const TYPE_MASK: u32 = 0xF0000000; // high nibble of len_and_tag
const LEN_MASK: u32 = 0x0FFFFFFF; // lower 28 bits = length
const SSO_MAX_LEN: usize = 12;

// Inline type tags (high nibble of len_and_tag, for SSO values)
const TAG_STRING: u32 = 0x00000000; // 0 in high nibble

// Heap type tags (low 3 bits of pointer)
const HEAP_TAG_STRING: usize = 0;
const HEAP_TAG_HASH: usize = 1;
const HEAP_TAG_LIST: usize = 2;
const HEAP_TAG_SET: usize = 3;
const HEAP_TAG_ZSET: usize = 4;
const HEAP_TAG_MASK: usize = 0x7;

/// Borrowed view of a CompactValue, for zero-copy read access.
pub enum RedisValueRef<'a> {
    String(&'a [u8]),
    Hash(&'a HashMap<Bytes, Bytes>),
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
            RedisValueRef::HashListpack(_) => "listpack",
            RedisValueRef::List(_) => "linkedlist",
            RedisValueRef::ListListpack(_) => "listpack",
            RedisValueRef::Set(_) => "hashtable",
            RedisValueRef::SetListpack(_) => "listpack",
            RedisValueRef::SetIntset(_) => "intset",
            RedisValueRef::SortedSet { .. } => "skiplist",
            RedisValueRef::SortedSetBPTree { .. } => "skiplist",
            RedisValueRef::SortedSetListpack(_) => "listpack",
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

    /// Create a CompactValue from a RedisValue.
    pub fn from_redis_value(value: RedisValue) -> Self {
        match &value {
            RedisValue::String(s) if s.len() <= SSO_MAX_LEN => {
                return Self::inline_string(s);
            }
            _ => {}
        }
        // Heap path: box the value, extract raw pointer, tag it
        let (heap_tag, str_len) = match &value {
            RedisValue::String(s) => (HEAP_TAG_STRING, s.len()),
            RedisValue::Hash(_) | RedisValue::HashListpack(_) => (HEAP_TAG_HASH, 0),
            RedisValue::List(_) | RedisValue::ListListpack(_) => (HEAP_TAG_LIST, 0),
            RedisValue::Set(_) | RedisValue::SetListpack(_) | RedisValue::SetIntset(_) => {
                (HEAP_TAG_SET, 0)
            }
            RedisValue::SortedSet { .. }
            | RedisValue::SortedSetBPTree { .. }
            | RedisValue::SortedSetListpack(_) => (HEAP_TAG_ZSET, 0),
        };

        // Get prefix bytes for strings
        let mut prefix = [0u8; 4];
        if let RedisValue::String(s) = &value {
            let copy_len = s.len().min(4);
            prefix[..copy_len].copy_from_slice(&s[..copy_len]);
        }

        let boxed = Box::new(value);
        let raw_ptr = Box::into_raw(boxed) as usize;
        // Verify alignment allows tagging (Box alignment >= 8 for these types)
        debug_assert!(raw_ptr & HEAP_TAG_MASK == 0, "Box pointer insufficiently aligned");
        let tagged_ptr = raw_ptr | heap_tag;

        let mut payload = [0u8; 12];
        payload[..4].copy_from_slice(&prefix);
        payload[4..12].copy_from_slice(&tagged_ptr.to_ne_bytes());

        CompactValue {
            len_and_tag: HEAP_MARKER | ((str_len as u32) & LEN_MASK),
            payload,
        }
    }

    /// Get the tagged pointer from a heap-allocated value.
    #[inline]
    fn heap_tagged_ptr(&self) -> usize {
        debug_assert!(!self.is_inline());
        usize::from_ne_bytes(self.payload[4..12].try_into().unwrap())
    }

    /// Get the raw (untagged) pointer to the heap RedisValue.
    #[inline]
    fn heap_raw_ptr(&self) -> *mut RedisValue {
        (self.heap_tagged_ptr() & !HEAP_TAG_MASK) as *mut RedisValue
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
        } else {
            // SAFETY: We created this pointer via Box::into_raw and it hasn't been freed
            let rv = unsafe { &*self.heap_raw_ptr() };
            match rv {
                RedisValue::String(s) => RedisValueRef::String(s),
                RedisValue::Hash(map) => RedisValueRef::Hash(map),
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
            }
        }
    }

    /// Fast path: get string bytes (returns None for non-string types).
    pub fn as_bytes(&self) -> Option<&[u8]> {
        if self.is_inline() {
            let len = self.inline_len();
            Some(&self.payload[..len])
        } else if self.heap_type_tag() == HEAP_TAG_STRING {
            let rv = unsafe { &*self.heap_raw_ptr() };
            match rv {
                RedisValue::String(s) => Some(s),
                _ => None,
            }
        } else {
            None
        }
    }

    /// Get a mutable reference to the underlying heap RedisValue.
    /// Returns None for inline (SSO) values.
    pub fn as_redis_value_mut(&mut self) -> Option<&mut RedisValue> {
        if self.is_inline() {
            None
        } else {
            // SAFETY: We own this pointer uniquely (no aliasing since we have &mut self)
            Some(unsafe { &mut *self.heap_raw_ptr() })
        }
    }

    /// Consuming conversion: returns the owned RedisValue.
    /// For inline strings, allocates a new Bytes.
    /// For heap values, reconstructs the Box and extracts the value.
    pub fn into_redis_value(self) -> RedisValue {
        if self.is_inline() {
            let len = self.inline_len();
            let data = Bytes::copy_from_slice(&self.payload[..len]);
            // Don't run Drop (nothing to free for inline)
            std::mem::forget(self);
            RedisValue::String(data)
        } else {
            let ptr = self.heap_raw_ptr();
            // Prevent Drop from also freeing the pointer
            std::mem::forget(self);
            // SAFETY: we created this via Box::into_raw, and we're the sole owner
            let boxed = unsafe { Box::from_raw(ptr) };
            *boxed
        }
    }

    /// Cloning conversion: returns a cloned RedisValue (for serialization/snapshots).
    pub fn to_redis_value(&self) -> RedisValue {
        if self.is_inline() {
            let len = self.inline_len();
            RedisValue::String(Bytes::copy_from_slice(&self.payload[..len]))
        } else {
            let rv = unsafe { &*self.heap_raw_ptr() };
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
        } else {
            let rv = unsafe { &*self.heap_raw_ptr() };
            rv.estimate_memory()
        }
    }
}

impl Drop for CompactValue {
    fn drop(&mut self) {
        if !self.is_inline() {
            let ptr = self.heap_raw_ptr();
            // SAFETY: we created this via Box::into_raw, we own it
            unsafe {
                drop(Box::from_raw(ptr));
            }
        }
    }
}

impl Clone for CompactValue {
    fn clone(&self) -> Self {
        if self.is_inline() {
            // Bitwise copy is safe for inline values
            CompactValue {
                len_and_tag: self.len_and_tag,
                payload: self.payload,
            }
        } else {
            // Clone the underlying RedisValue and create a new heap CompactValue
            let rv = unsafe { &*self.heap_raw_ptr() };
            Self::from_redis_value(rv.clone())
        }
    }
}

impl fmt::Debug for CompactValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_inline() {
            let len = self.inline_len();
            let data = &self.payload[..len];
            write!(f, "CompactValue::Inline({:?})", String::from_utf8_lossy(data))
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
