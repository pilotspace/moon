//! 64-byte aligned memory buffer for SIMD-friendly vector storage.
//!
//! `AlignedBuffer<T>` guarantees that the backing allocation is aligned to 64 bytes,
//! satisfying the strictest SIMD requirement (AVX-512 / cache line alignment).

use std::alloc::{self, Layout};
use std::ops::{Deref, DerefMut};
use std::ptr;

/// Alignment guarantee in bytes. Matches cache line size and AVX-512 register width.
const ALIGN: usize = 64;

/// A heap-allocated buffer of `T` values with 64-byte alignment.
///
/// The alignment ensures optimal performance for SSE2/AVX2/AVX-512/NEON loads
/// and avoids cache-line splits on all modern CPUs.
pub struct AlignedBuffer<T: Copy + Default> {
    ptr: *mut T,
    len: usize,
    layout: Layout,
}

// SAFETY: AlignedBuffer owns its allocation exclusively. T: Copy + Default
// guarantees no interior mutability or drop side-effects. The raw pointer
// is only accessed through &self / &mut self, enforcing Rust's aliasing rules.
unsafe impl<T: Copy + Default + Send> Send for AlignedBuffer<T> {}
unsafe impl<T: Copy + Default + Sync> Sync for AlignedBuffer<T> {}

impl<T: Copy + Default> AlignedBuffer<T> {
    /// Allocate a zero-initialized buffer of `len` elements at 64-byte alignment.
    ///
    /// # Panics
    /// Panics if the allocation fails (out of memory) or if `len * size_of::<T>()` overflows.
    pub fn new(len: usize) -> Self {
        if len == 0 || std::mem::size_of::<T>() == 0 {
            return Self {
                ptr: ALIGN as *mut T, // dangling but aligned
                len: 0,
                layout: Layout::from_size_align(0, ALIGN).unwrap(),
            };
        }

        let byte_size = len
            .checked_mul(std::mem::size_of::<T>())
            .expect("AlignedBuffer: size overflow");
        let layout = Layout::from_size_align(byte_size, ALIGN).expect("AlignedBuffer: invalid layout");

        // SAFETY: layout has non-zero size (checked above). alloc_zeroed returns a
        // valid pointer to `byte_size` zero-initialized bytes with the requested alignment,
        // or null on allocation failure.
        let raw = unsafe { alloc::alloc_zeroed(layout) };
        if raw.is_null() {
            alloc::handle_alloc_error(layout);
        }

        Self {
            ptr: raw as *mut T,
            len,
            layout,
        }
    }

    /// Create an aligned buffer from an existing `Vec<T>`.
    ///
    /// If the vec's allocation is already 64-byte aligned, this reuses it.
    /// Otherwise, it copies into a new aligned allocation.
    pub fn from_vec(v: Vec<T>) -> Self {
        let src_ptr = v.as_ptr();
        let src_aligned = (src_ptr as usize) % ALIGN == 0;

        if src_aligned && v.len() == v.capacity() && !v.is_empty() {
            let len = v.len();
            let byte_size = len * std::mem::size_of::<T>();
            let layout = Layout::from_size_align(byte_size, ALIGN).expect("AlignedBuffer: invalid layout");
            let ptr = v.as_ptr() as *mut T;
            std::mem::forget(v);
            Self { ptr, len, layout }
        } else {
            let mut buf = Self::new(v.len());
            if !v.is_empty() {
                // SAFETY: buf.ptr points to a valid allocation of at least `v.len() * size_of::<T>()`
                // bytes. src_ptr is valid for `v.len()` elements. The regions do not overlap
                // because buf.ptr is a fresh allocation.
                unsafe {
                    ptr::copy_nonoverlapping(v.as_ptr(), buf.ptr, v.len());
                }
            }
            buf
        }
    }

    /// Returns a shared slice over the buffer contents.
    #[inline]
    pub fn as_slice(&self) -> &[T] {
        if self.len == 0 {
            return &[];
        }
        // SAFETY: self.ptr is valid for self.len elements (allocated in new/from_vec),
        // properly aligned, and not aliased mutably (shared reference to self).
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    /// Returns a mutable slice over the buffer contents.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        if self.len == 0 {
            return &mut [];
        }
        // SAFETY: self.ptr is valid for self.len elements, properly aligned,
        // and we have exclusive access (mutable reference to self).
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    /// Returns the number of elements in the buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the buffer contains no elements.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the raw pointer to the first element.
    #[inline]
    pub fn as_ptr(&self) -> *const T {
        self.ptr
    }
}

impl<T: Copy + Default> Deref for AlignedBuffer<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T: Copy + Default> DerefMut for AlignedBuffer<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [T] {
        self.as_mut_slice()
    }
}

impl<T: Copy + Default> Drop for AlignedBuffer<T> {
    fn drop(&mut self) {
        if self.layout.size() > 0 {
            // SAFETY: self.ptr was allocated via alloc::alloc_zeroed with self.layout
            // in new(), or taken from a Vec with matching layout in from_vec().
            // This is the only deallocation path (Drop runs once).
            unsafe {
                alloc::dealloc(self.ptr as *mut u8, self.layout);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alignment() {
        let buf: AlignedBuffer<f32> = AlignedBuffer::new(256);
        assert_eq!(buf.as_ptr() as usize % 64, 0, "buffer must be 64-byte aligned");
        assert_eq!(buf.len(), 256);
    }

    #[test]
    fn test_read_write() {
        let mut buf: AlignedBuffer<f32> = AlignedBuffer::new(4);
        buf[0] = 1.0;
        buf[1] = 2.0;
        buf[2] = 3.0;
        buf[3] = 4.0;
        assert_eq!(buf.as_slice(), &[1.0, 2.0, 3.0, 4.0]);
    }

    #[test]
    fn test_from_vec() {
        let v = vec![10i8, 20, 30, 40, 50];
        let buf = AlignedBuffer::from_vec(v);
        assert_eq!(buf.as_ptr() as usize % 64, 0);
        assert_eq!(buf.as_slice(), &[10, 20, 30, 40, 50]);
    }

    #[test]
    fn test_empty() {
        let buf: AlignedBuffer<f32> = AlignedBuffer::new(0);
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.as_slice(), &[] as &[f32]);
    }

    #[test]
    fn test_from_empty_vec() {
        let v: Vec<f32> = vec![];
        let buf = AlignedBuffer::from_vec(v);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_deref() {
        let mut buf: AlignedBuffer<u32> = AlignedBuffer::new(3);
        buf[0] = 100;
        buf[1] = 200;
        buf[2] = 300;
        // Test Deref: use slice methods directly
        assert_eq!(buf.iter().sum::<u32>(), 600);
    }
}
