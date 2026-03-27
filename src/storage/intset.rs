use bytes::Bytes;
use std::collections::HashSet;

/// Encoding width for intset values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntsetEncoding {
    Int16 = 2,
    Int32 = 4,
    Int64 = 8,
}

impl IntsetEncoding {
    fn width(self) -> usize {
        self as usize
    }
}

/// A sorted integer set stored as a packed byte array with automatic width upgrade.
#[derive(Debug, Clone)]
pub struct Intset {
    encoding: IntsetEncoding,
    data: Vec<u8>,
}

/// Iterator over intset values in sorted ascending order.
pub struct IntsetIter<'a> {
    intset: &'a Intset,
    index: usize,
}

impl Intset {
    /// Create a new empty intset with Int16 encoding.
    pub fn new() -> Self {
        Intset {
            encoding: IntsetEncoding::Int16,
            data: Vec::new(),
        }
    }

    /// Number of elements in the set.
    pub fn len(&self) -> usize {
        self.data.len() / self.encoding.width()
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Current encoding width.
    pub fn encoding(&self) -> IntsetEncoding {
        self.encoding
    }

    /// Insert a value. Returns true if the value was newly inserted.
    pub fn insert(&mut self, value: i64) -> bool {
        let needed = encoding_for_value(value);
        if needs_upgrade(self.encoding, needed) {
            // Upgrade encoding, then insert
            self.upgrade_and_insert(needed, value);
            return true;
        }

        // Check if value fits in current encoding (it should if no upgrade needed)
        match self.binary_search(value) {
            Ok(_) => false, // duplicate
            Err(insert_pos) => {
                let width = self.encoding.width();
                let byte_pos = insert_pos * width;
                let encoded = encode_value(value, self.encoding);
                // Insert bytes at position
                self.data
                    .splice(byte_pos..byte_pos, encoded.iter().cloned());
                true
            }
        }
    }

    /// Remove a value. Returns true if it existed.
    pub fn remove(&mut self, value: i64) -> bool {
        // If value doesn't fit in current encoding, it can't be in the set
        if !value_fits(value, self.encoding) {
            return false;
        }
        match self.binary_search(value) {
            Ok(pos) => {
                let width = self.encoding.width();
                let byte_pos = pos * width;
                self.data.drain(byte_pos..byte_pos + width);
                true
            }
            Err(_) => false,
        }
    }

    /// Check if a value is in the set.
    pub fn contains(&self, value: i64) -> bool {
        if !value_fits(value, self.encoding) {
            return false;
        }
        self.binary_search(value).is_ok()
    }

    /// Get value at sorted position index.
    pub fn get(&self, index: usize) -> Option<i64> {
        if index >= self.len() {
            return None;
        }
        Some(read_at(&self.data, self.encoding, index))
    }

    /// Forward iterator in sorted ascending order.
    pub fn iter(&self) -> IntsetIter<'_> {
        IntsetIter {
            intset: self,
            index: 0,
        }
    }

    /// Return a random element from the set.
    pub fn random(&self) -> Option<i64> {
        let len = self.len();
        if len == 0 {
            return None;
        }
        use rand::Rng;
        let idx = rand::rng().random_range(0..len);
        self.get(idx)
    }

    /// Convert to a HashSet of Bytes (string-encoded integers).
    pub fn to_hash_set(&self) -> HashSet<Bytes> {
        self.iter()
            .map(|v| Bytes::from(v.to_string().into_bytes()))
            .collect()
    }

    /// Estimate memory usage.
    pub fn estimate_memory(&self) -> usize {
        std::mem::size_of::<Self>() + self.data.capacity()
    }

    // --- Internal ---

    fn binary_search(&self, value: i64) -> Result<usize, usize> {
        let len = self.len();
        if len == 0 {
            return Err(0);
        }
        let mut lo = 0usize;
        let mut hi = len;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mid_val = read_at(&self.data, self.encoding, mid);
            if mid_val < value {
                lo = mid + 1;
            } else if mid_val > value {
                hi = mid;
            } else {
                return Ok(mid);
            }
        }
        Err(lo)
    }

    fn upgrade_and_insert(&mut self, new_encoding: IntsetEncoding, value: i64) {
        let old_len = self.len();
        let old_encoding = self.encoding;
        let new_width = new_encoding.width();

        // Allocate new data for old_len + 1 elements at new width
        let mut new_data = vec![0u8; (old_len + 1) * new_width];

        // Determine where the new value goes in the sorted order.
        // Since the value requires a wider encoding, it's either very negative or very positive.
        // We can find the insert position by scanning.
        // Actually for upgrade scenarios, the new value is outside current encoding range,
        // so it goes at the beginning (if negative) or at the end (if positive).
        let insert_at_end = value > 0;

        if insert_at_end {
            // Copy old values to positions 0..old_len, new value at old_len
            for i in 0..old_len {
                let v = read_at(&self.data, old_encoding, i);
                write_at(&mut new_data, new_encoding, i, v);
            }
            write_at(&mut new_data, new_encoding, old_len, value);
        } else {
            // New value at position 0, old values at 1..=old_len
            write_at(&mut new_data, new_encoding, 0, value);
            for i in 0..old_len {
                let v = read_at(&self.data, old_encoding, i);
                write_at(&mut new_data, new_encoding, i + 1, v);
            }
        }

        self.encoding = new_encoding;
        self.data = new_data;
    }
}

impl Default for Intset {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> Iterator for IntsetIter<'a> {
    type Item = i64;

    fn next(&mut self) -> Option<i64> {
        if self.index >= self.intset.len() {
            return None;
        }
        let val = read_at(&self.intset.data, self.intset.encoding, self.index);
        self.index += 1;
        Some(val)
    }
}

/// Determine minimum encoding for a value.
fn encoding_for_value(value: i64) -> IntsetEncoding {
    if value >= i16::MIN as i64 && value <= i16::MAX as i64 {
        IntsetEncoding::Int16
    } else if value >= i32::MIN as i64 && value <= i32::MAX as i64 {
        IntsetEncoding::Int32
    } else {
        IntsetEncoding::Int64
    }
}

/// Check if an encoding upgrade is needed.
fn needs_upgrade(current: IntsetEncoding, needed: IntsetEncoding) -> bool {
    needed.width() > current.width()
}

/// Check if a value fits within the given encoding.
fn value_fits(value: i64, encoding: IntsetEncoding) -> bool {
    match encoding {
        IntsetEncoding::Int16 => value >= i16::MIN as i64 && value <= i16::MAX as i64,
        IntsetEncoding::Int32 => value >= i32::MIN as i64 && value <= i32::MAX as i64,
        IntsetEncoding::Int64 => true,
    }
}

/// Read an integer at a given index within the data buffer.
fn read_at(data: &[u8], encoding: IntsetEncoding, index: usize) -> i64 {
    let width = encoding.width();
    let offset = index * width;
    match encoding {
        IntsetEncoding::Int16 => i16::from_le_bytes([data[offset], data[offset + 1]]) as i64,
        IntsetEncoding::Int32 => i32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as i64,
        IntsetEncoding::Int64 => i64::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ]),
    }
}

/// Write an integer at a given index within the data buffer.
fn write_at(data: &mut [u8], encoding: IntsetEncoding, index: usize, value: i64) {
    let width = encoding.width();
    let offset = index * width;
    match encoding {
        IntsetEncoding::Int16 => {
            data[offset..offset + 2].copy_from_slice(&(value as i16).to_le_bytes());
        }
        IntsetEncoding::Int32 => {
            data[offset..offset + 4].copy_from_slice(&(value as i32).to_le_bytes());
        }
        IntsetEncoding::Int64 => {
            data[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        }
    }
}

/// Encode a value into bytes at the given encoding width.
fn encode_value(value: i64, encoding: IntsetEncoding) -> Vec<u8> {
    match encoding {
        IntsetEncoding::Int16 => (value as i16).to_le_bytes().to_vec(),
        IntsetEncoding::Int32 => (value as i32).to_le_bytes().to_vec(),
        IntsetEncoding::Int64 => value.to_le_bytes().to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_intset() {
        let is = Intset::new();
        assert_eq!(is.len(), 0);
        assert_eq!(is.encoding(), IntsetEncoding::Int16);
        assert!(is.is_empty());
    }

    #[test]
    fn test_insert_small() {
        let mut is = Intset::new();
        assert!(is.insert(100));
        assert_eq!(is.len(), 1);
        assert!(is.contains(100));
    }

    #[test]
    fn test_insert_sorted() {
        let mut is = Intset::new();
        is.insert(30);
        is.insert(10);
        is.insert(20);
        let vals: Vec<_> = is.iter().collect();
        assert_eq!(vals, vec![10, 20, 30]);
    }

    #[test]
    fn test_insert_duplicate() {
        let mut is = Intset::new();
        assert!(is.insert(42));
        assert!(!is.insert(42));
        assert_eq!(is.len(), 1);
    }

    #[test]
    fn test_remove_existing() {
        let mut is = Intset::new();
        is.insert(1);
        is.insert(2);
        is.insert(3);
        assert!(is.remove(2));
        assert_eq!(is.len(), 2);
        assert!(!is.contains(2));
    }

    #[test]
    fn test_remove_non_existent() {
        let mut is = Intset::new();
        is.insert(1);
        assert!(!is.remove(99));
        assert_eq!(is.len(), 1);
    }

    #[test]
    fn test_contains_false() {
        let is = Intset::new();
        assert!(!is.contains(42));
    }

    #[test]
    fn test_upgrade_to_int32() {
        let mut is = Intset::new();
        is.insert(1);
        is.insert(2);
        assert_eq!(is.encoding(), IntsetEncoding::Int16);
        is.insert(i16::MAX as i64 + 1); // 32768 triggers upgrade
        assert_eq!(is.encoding(), IntsetEncoding::Int32);
        // All values preserved
        assert!(is.contains(1));
        assert!(is.contains(2));
        assert!(is.contains(i16::MAX as i64 + 1));
        assert_eq!(is.len(), 3);
    }

    #[test]
    fn test_upgrade_to_int64() {
        let mut is = Intset::new();
        is.insert(1);
        is.insert(100);
        is.insert(i32::MAX as i64 + 1); // triggers upgrade to Int64
        assert_eq!(is.encoding(), IntsetEncoding::Int64);
        assert!(is.contains(1));
        assert!(is.contains(100));
        assert!(is.contains(i32::MAX as i64 + 1));
    }

    #[test]
    fn test_negative_value() {
        let mut is = Intset::new();
        is.insert(-50);
        is.insert(50);
        is.insert(-100);
        let vals: Vec<_> = is.iter().collect();
        assert_eq!(vals, vec![-100, -50, 50]);
    }

    #[test]
    fn test_i64_min_max() {
        let mut is = Intset::new();
        assert!(is.insert(i64::MIN));
        assert!(is.insert(i64::MAX));
        assert_eq!(is.encoding(), IntsetEncoding::Int64);
        assert!(is.contains(i64::MIN));
        assert!(is.contains(i64::MAX));
        let vals: Vec<_> = is.iter().collect();
        assert_eq!(vals, vec![i64::MIN, i64::MAX]);
    }

    #[test]
    fn test_iter_sorted() {
        let mut is = Intset::new();
        for v in [5, 3, 1, 4, 2] {
            is.insert(v);
        }
        let vals: Vec<_> = is.iter().collect();
        assert_eq!(vals, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_get() {
        let mut is = Intset::new();
        is.insert(10);
        is.insert(20);
        is.insert(30);
        assert_eq!(is.get(0), Some(10));
        assert_eq!(is.get(1), Some(20));
        assert_eq!(is.get(2), Some(30));
        assert_eq!(is.get(3), None);
    }

    #[test]
    fn test_random() {
        let mut is = Intset::new();
        assert_eq!(is.random(), None);
        is.insert(42);
        assert_eq!(is.random(), Some(42));
        // With multiple elements, random should return a valid member
        is.insert(100);
        is.insert(200);
        let r = is.random().unwrap();
        assert!(r == 42 || r == 100 || r == 200);
    }

    #[test]
    fn test_to_hash_set() {
        let mut is = Intset::new();
        is.insert(1);
        is.insert(2);
        is.insert(3);
        let hs = is.to_hash_set();
        assert_eq!(hs.len(), 3);
        assert!(hs.contains(&Bytes::from("1")));
        assert!(hs.contains(&Bytes::from("2")));
        assert!(hs.contains(&Bytes::from("3")));
    }

    #[test]
    fn test_clone_independent() {
        let mut is = Intset::new();
        is.insert(1);
        let is2 = is.clone();
        is.insert(2);
        assert_eq!(is.len(), 2);
        assert_eq!(is2.len(), 1);
    }

    #[test]
    fn test_512_entries() {
        let mut is = Intset::new();
        for i in 0..512 {
            assert!(is.insert(i));
        }
        assert_eq!(is.len(), 512);
        for i in 0..512 {
            assert!(is.contains(i), "missing {}", i);
        }
    }

    #[test]
    fn test_upgrade_preserves_sorted_order() {
        let mut is = Intset::new();
        is.insert(-100);
        is.insert(0);
        is.insert(100);
        // Upgrade with a large negative number
        is.insert(i32::MIN as i64 - 1);
        let vals: Vec<_> = is.iter().collect();
        assert_eq!(vals, vec![i32::MIN as i64 - 1, -100, 0, 100]);
    }
}
