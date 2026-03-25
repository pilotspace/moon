//! CompactKey: 24-byte key representation with Small String Optimization (SSO).
//!
//! Layout (24 bytes total, same size as `Bytes`):
//! - **Inline path** (keys <= 23 bytes): `data[0]` = length (0..=23, high bit clear),
//!   `data[1..1+len]` = key bytes.
//! - **Heap path** (keys > 23 bytes): `data[0]` = `0x80 | (len >> 24)`,
//!   `data[1]` = `(len >> 16) as u8`, `data[2..4]` = `(len as u16).to_le_bytes()`,
//!   `data[4..12]` = raw pointer to `Box<[u8]>` heap allocation (as `usize` LE),
//!   `data[12..24]` = first 12 bytes of key (inline prefix for fast comparison rejection).

use bytes::Bytes;
use std::borrow::Borrow;
use std::fmt;
use std::hash::{Hash, Hasher};

/// Maximum key length that can be stored inline (1 byte for length tag, 23 bytes for data).
const INLINE_MAX: usize = 23;

/// Bit set in `data[0]` to indicate heap-allocated key.
const HEAP_FLAG: u8 = 0x80;

/// Number of prefix bytes cached inline for heap keys (fast mismatch rejection).
const HEAP_PREFIX_LEN: usize = 12;

/// A 24-byte compact key with small-string optimization.
///
/// Keys up to 23 bytes are stored entirely inline with zero heap allocation.
/// Longer keys are heap-allocated via `Box<[u8]>` with 12 bytes of inline prefix
/// cached for fast comparison short-circuiting.
#[repr(C)]
pub struct CompactKey {
    data: [u8; 24],
}

const _: () = assert!(std::mem::size_of::<CompactKey>() == 24);

// SAFETY: CompactKey is Send/Sync because the heap path owns a Box<[u8]> (which is
// Send/Sync) and the inline path is plain bytes.
unsafe impl Send for CompactKey {}
unsafe impl Sync for CompactKey {}

impl CompactKey {
    /// Return the key bytes as a slice.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.as_ref()
    }

    /// Convert to owned `Bytes` (copies inline data, or copies heap data).
    #[inline]
    pub fn to_bytes(&self) -> Bytes {
        Bytes::copy_from_slice(self.as_ref())
    }

    /// Return true if the key is stored inline (no heap allocation).
    #[inline]
    pub fn is_inline(&self) -> bool {
        self.data[0] & HEAP_FLAG == 0
    }

    /// Return the length of the key in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        if self.is_inline() {
            self.data[0] as usize
        } else {
            self.heap_len()
        }
    }

    /// Decode length from the heap header (4 bytes: data[0..4]).
    #[inline]
    fn heap_len(&self) -> usize {
        let hi = ((self.data[0] & !HEAP_FLAG) as usize) << 24;
        let mid = (self.data[1] as usize) << 16;
        let lo = u16::from_le_bytes([self.data[2], self.data[3]]) as usize;
        hi | mid | lo
    }

    /// Reconstruct the raw pointer to the heap `Box<[u8]>` data.
    #[inline]
    fn heap_ptr(&self) -> *mut u8 {
        let ptr_val = usize::from_le_bytes(self.data[4..12].try_into().unwrap());
        ptr_val as *mut u8
    }

    /// Get the heap-allocated slice.
    ///
    /// # Safety
    /// Caller must ensure `!self.is_inline()` and the pointer is valid.
    #[inline]
    unsafe fn heap_slice(&self) -> &[u8] {
        let len = self.heap_len();
        let ptr = self.heap_ptr();
        // SAFETY: caller guarantees this is a valid heap key.
        unsafe { std::slice::from_raw_parts(ptr, len) }
    }
}

impl AsRef<[u8]> for CompactKey {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        if self.is_inline() {
            let len = self.data[0] as usize;
            &self.data[1..1 + len]
        } else {
            // SAFETY: heap pointer was created via Box::into_raw and has not been freed.
            unsafe { self.heap_slice() }
        }
    }
}

impl Borrow<[u8]> for CompactKey {
    #[inline]
    fn borrow(&self) -> &[u8] {
        self.as_ref()
    }
}

impl Hash for CompactKey {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}

impl PartialEq for CompactKey {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for CompactKey {}

impl PartialOrd for CompactKey {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CompactKey {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}

impl PartialEq<[u8]> for CompactKey {
    #[inline]
    fn eq(&self, other: &[u8]) -> bool {
        self.as_ref() == other
    }
}

impl Clone for CompactKey {
    fn clone(&self) -> Self {
        if self.is_inline() {
            // Bitwise copy is safe for inline keys.
            CompactKey { data: self.data }
        } else {
            // Deep-copy: allocate new Box<[u8]> with the heap data.
            let src = unsafe { self.heap_slice() };
            Self::from(src)
        }
    }
}

impl Drop for CompactKey {
    fn drop(&mut self) {
        if !self.is_inline() {
            let len = self.heap_len();
            let ptr = self.heap_ptr();
            // SAFETY: ptr was created via Box::into_raw(Box<[u8]>) with exactly `len` bytes.
            // We reconstruct and drop the Box to free the allocation.
            unsafe {
                let slice_ptr = std::ptr::slice_from_raw_parts_mut(ptr, len);
                drop(Box::from_raw(slice_ptr));
            }
        }
    }
}

impl fmt::Debug for CompactKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = self.as_ref();
        match std::str::from_utf8(bytes) {
            Ok(s) => write!(f, "CompactKey({:?})", s),
            Err(_) => write!(f, "CompactKey({:x?})", bytes),
        }
    }
}

impl From<&[u8]> for CompactKey {
    fn from(src: &[u8]) -> Self {
        let mut data = [0u8; 24];
        if src.len() <= INLINE_MAX {
            data[0] = src.len() as u8;
            data[1..1 + src.len()].copy_from_slice(src);
        } else {
            // Encode length into data[0..4]
            let len = src.len();
            data[0] = HEAP_FLAG | ((len >> 24) as u8);
            data[1] = (len >> 16) as u8;
            data[2..4].copy_from_slice(&(len as u16).to_le_bytes());

            // Allocate heap copy
            let boxed: Box<[u8]> = Box::from(src);
            let ptr = Box::into_raw(boxed) as *mut u8 as usize;
            data[4..12].copy_from_slice(&ptr.to_le_bytes());

            // Cache inline prefix for fast comparison rejection
            let prefix_len = src.len().min(HEAP_PREFIX_LEN);
            data[12..12 + prefix_len].copy_from_slice(&src[..prefix_len]);
        }
        CompactKey { data }
    }
}

impl From<&str> for CompactKey {
    #[inline]
    fn from(s: &str) -> Self {
        Self::from(s.as_bytes())
    }
}

impl From<String> for CompactKey {
    #[inline]
    fn from(s: String) -> Self {
        Self::from(s.as_bytes())
    }
}

impl From<Bytes> for CompactKey {
    #[inline]
    fn from(b: Bytes) -> Self {
        Self::from(b.as_ref())
    }
}

impl From<Vec<u8>> for CompactKey {
    #[inline]
    fn from(v: Vec<u8>) -> Self {
        Self::from(v.as_slice())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    fn hash_of(key: &CompactKey) -> u64 {
        let mut h = DefaultHasher::new();
        key.hash(&mut h);
        h.finish()
    }

    #[test]
    fn test_size_is_24_bytes() {
        assert_eq!(std::mem::size_of::<CompactKey>(), 24);
    }

    #[test]
    fn test_inline_empty() {
        let k = CompactKey::from(b"" as &[u8]);
        assert!(k.is_inline());
        assert_eq!(k.len(), 0);
        assert_eq!(k.as_bytes(), b"");
    }

    #[test]
    fn test_inline_one_byte() {
        let k = CompactKey::from(b"x" as &[u8]);
        assert!(k.is_inline());
        assert_eq!(k.len(), 1);
        assert_eq!(k.as_bytes(), b"x");
    }

    #[test]
    fn test_inline_max_23_bytes() {
        let data = b"12345678901234567890123"; // exactly 23 bytes
        assert_eq!(data.len(), 23);
        let k = CompactKey::from(&data[..]);
        assert!(k.is_inline());
        assert_eq!(k.len(), 23);
        assert_eq!(k.as_bytes(), data);
    }

    #[test]
    fn test_heap_24_bytes() {
        let data = b"123456789012345678901234"; // 24 bytes
        assert_eq!(data.len(), 24);
        let k = CompactKey::from(&data[..]);
        assert!(!k.is_inline());
        assert_eq!(k.len(), 24);
        assert_eq!(k.as_bytes(), data);
    }

    #[test]
    fn test_heap_100_bytes() {
        let data: Vec<u8> = (0..100).collect();
        let k = CompactKey::from(data.as_slice());
        assert!(!k.is_inline());
        assert_eq!(k.len(), 100);
        assert_eq!(k.as_bytes(), data.as_slice());
    }

    #[test]
    fn test_hash_consistency() {
        let a = CompactKey::from("hello");
        let b = CompactKey::from(b"hello" as &[u8]);
        assert_eq!(hash_of(&a), hash_of(&b));
    }

    #[test]
    fn test_hash_consistency_heap() {
        let long = "a]".repeat(20); // 40 bytes, heap path
        let a = CompactKey::from(long.as_str());
        let b = CompactKey::from(long.as_bytes());
        assert_eq!(hash_of(&a), hash_of(&b));
    }

    #[test]
    fn test_eq_same_content() {
        let a = CompactKey::from("test");
        let b = CompactKey::from("test");
        assert_eq!(a, b);
    }

    #[test]
    fn test_eq_different_content() {
        let a = CompactKey::from("foo");
        let b = CompactKey::from("bar");
        assert_ne!(a, b);
    }

    #[test]
    fn test_eq_inline_vs_inline() {
        let a = CompactKey::from("short");
        let b = CompactKey::from("short");
        assert_eq!(a, b);
    }

    #[test]
    fn test_partial_eq_slice() {
        let k = CompactKey::from("hello");
        let slice: &[u8] = b"hello";
        assert!(k == *slice);
    }

    #[test]
    fn test_clone_inline() {
        let a = CompactKey::from("inline");
        let b = a.clone();
        assert_eq!(a.as_bytes(), b.as_bytes());
        assert!(b.is_inline());
    }

    #[test]
    fn test_clone_heap() {
        let data: Vec<u8> = (0..50).collect();
        let a = CompactKey::from(data.as_slice());
        let b = a.clone();
        assert_eq!(a.as_bytes(), b.as_bytes());
        assert!(!b.is_inline());
        // Ensure they are independent allocations (different pointers)
        assert_ne!(a.data[4..12], b.data[4..12]);
    }

    #[test]
    fn test_drop_safety_inline() {
        // Should not panic or leak
        let _k = CompactKey::from("drop me");
    }

    #[test]
    fn test_drop_safety_heap() {
        // Should not panic, double-free, or leak
        let _k = CompactKey::from(&[0u8; 100][..]);
    }

    #[test]
    fn test_from_bytes() {
        let b = Bytes::from_static(b"from_bytes");
        let k = CompactKey::from(b);
        assert_eq!(k.as_bytes(), b"from_bytes");
        assert!(k.is_inline());
    }

    #[test]
    fn test_from_bytes_heap() {
        let b = Bytes::from(vec![42u8; 30]);
        let k = CompactKey::from(b);
        assert_eq!(k.len(), 30);
        assert!(!k.is_inline());
        assert_eq!(k.as_bytes(), &[42u8; 30]);
    }

    #[test]
    fn test_from_slice() {
        let k = CompactKey::from(b"slice_test" as &[u8]);
        assert_eq!(k.as_bytes(), b"slice_test");
    }

    #[test]
    fn test_from_str() {
        let k = CompactKey::from("str_test");
        assert_eq!(k.as_bytes(), b"str_test");
    }

    #[test]
    fn test_borrow_returns_correct_slice() {
        let k = CompactKey::from("borrow_test");
        let borrowed: &[u8] = k.borrow();
        assert_eq!(borrowed, b"borrow_test");
    }

    #[test]
    fn test_borrow_heap_returns_correct_slice() {
        let data: Vec<u8> = (0..50).collect();
        let k = CompactKey::from(data.as_slice());
        let borrowed: &[u8] = k.borrow();
        assert_eq!(borrowed, data.as_slice());
    }

    #[test]
    fn test_debug_utf8() {
        let k = CompactKey::from("readable");
        let s = format!("{:?}", k);
        assert!(s.contains("readable"), "Debug output: {}", s);
    }

    #[test]
    fn test_debug_non_utf8() {
        let k = CompactKey::from(&[0xFF, 0xFE, 0xFD][..]);
        let s = format!("{:?}", k);
        assert!(s.contains("CompactKey"), "Debug output: {}", s);
    }

    #[test]
    fn test_from_vec() {
        let v = vec![1u8, 2, 3, 4, 5];
        let k = CompactKey::from(v);
        assert_eq!(k.as_bytes(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_large_key_length_encoding() {
        // Test a key that exercises the full 32-bit length encoding
        let data = vec![0xABu8; 70_000];
        let k = CompactKey::from(data.as_slice());
        assert_eq!(k.len(), 70_000);
        assert_eq!(k.as_bytes(), data.as_slice());
    }
}
