/// Shared, lazily-initialized per-shard backlog.
///
/// `Mutex<Option<...>>`: `None` until the first replica handshake arrives,
/// after which a single allocation is performed. The shard event loop is the
/// only writer (uncontended lock acquire on the write path); PSYNC handlers
/// briefly take the lock to read backlog bytes during resync.
pub type SharedBacklog = std::sync::Arc<parking_lot::Mutex<Option<ReplicationBacklog>>>;

/// Per-shard circular replication backlog.
///
/// Captures WAL bytes as they flow. Replicas use this for partial resync.
/// The critical invariant: start_offset and end_offset are MONOTONICALLY INCREASING
/// and NEVER reset (unlike WalWriter::bytes_written which resets on snapshot truncation).
pub struct ReplicationBacklog {
    buf: std::collections::VecDeque<u8>,
    capacity: usize,
    /// Monotonic WAL offset of the byte currently at buf[0]. Never resets.
    start_offset: u64,
    /// Monotonic WAL offset of (last appended byte + 1). Never resets.
    end_offset: u64,
}

impl ReplicationBacklog {
    pub fn new(capacity: usize) -> Self {
        ReplicationBacklog {
            buf: std::collections::VecDeque::with_capacity(capacity),
            capacity,
            start_offset: 0,
            end_offset: 0,
        }
    }

    /// Append bytes to the backlog. Evicts oldest bytes when at capacity.
    ///
    /// Bulk-copy implementation (QW5, 2026-06 review finding 1.5): one drain
    /// + one extend instead of a per-byte eviction loop. State machine is
    /// identical to the per-byte version — the live window is always the last
    /// `capacity` bytes ever appended, and `start_offset` maintains the
    /// invariant `start_offset = end_offset - buf.len()`.
    pub fn append(&mut self, data: &[u8]) {
        self.end_offset += data.len() as u64;
        if data.len() >= self.capacity {
            // The live window comes entirely from the tail of `data`.
            self.buf.clear();
            self.buf
                .extend(data[data.len() - self.capacity..].iter().copied());
        } else {
            let overflow = (self.buf.len() + data.len()).saturating_sub(self.capacity);
            if overflow > 0 {
                self.buf.drain(..overflow);
            }
            self.buf.extend(data.iter().copied());
        }
        self.start_offset = self.end_offset - self.buf.len() as u64;
    }

    /// Returns owned Vec of bytes from `offset` to end_offset, or None if offset was evicted.
    pub fn bytes_from(&self, offset: u64) -> Option<Vec<u8>> {
        if offset > self.end_offset {
            return None;
        }
        if offset < self.start_offset {
            return None; // Evicted
        }
        let skip = (offset - self.start_offset) as usize;
        Some(self.buf.iter().skip(skip).copied().collect())
    }

    /// Returns true if offset is available in the backlog (not evicted, not in future).
    pub fn contains_offset(&self, offset: u64) -> bool {
        offset >= self.start_offset && offset <= self.end_offset
    }

    pub fn start_offset(&self) -> u64 {
        self.start_offset
    }

    pub fn end_offset(&self) -> u64 {
        self.end_offset
    }

    /// Resident bytes used by the backlog ring buffer.
    /// Returns the allocated capacity of the VecDeque (not current length).
    /// O(1), zero allocation.
    #[inline]
    pub fn resident_bytes(&self) -> usize {
        self.buf.capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_backlog_offsets() {
        let bl = ReplicationBacklog::new(1024);
        assert_eq!(bl.start_offset(), 0);
        assert_eq!(bl.end_offset(), 0);
    }

    #[test]
    fn test_append_and_read() {
        let mut bl = ReplicationBacklog::new(1024);
        bl.append(b"hello");
        assert_eq!(bl.start_offset(), 0);
        assert_eq!(bl.end_offset(), 5);
        assert_eq!(bl.bytes_from(0), Some(b"hello".to_vec()));
        assert_eq!(bl.bytes_from(2), Some(b"llo".to_vec()));
        assert_eq!(bl.bytes_from(5), Some(vec![]));
    }

    #[test]
    fn test_bytes_from_empty_backlog() {
        let bl = ReplicationBacklog::new(1024);
        // offset 0 on empty backlog returns Some([]) not None
        assert_eq!(bl.bytes_from(0), Some(vec![]));
    }

    #[test]
    fn test_eviction_on_capacity() {
        let mut bl = ReplicationBacklog::new(4);
        bl.append(b"abcd"); // full: [a,b,c,d], start=0, end=4
        assert_eq!(bl.start_offset(), 0);
        assert_eq!(bl.end_offset(), 4);

        bl.append(b"ef"); // evicts a,b: [c,d,e,f], start=2, end=6
        assert_eq!(bl.start_offset(), 2);
        assert_eq!(bl.end_offset(), 6);

        // Offset 0 was evicted
        assert_eq!(bl.bytes_from(0), None);
        assert_eq!(bl.bytes_from(1), None);
        // Offset 2 is still available
        assert_eq!(bl.bytes_from(2), Some(b"cdef".to_vec()));
        assert_eq!(bl.bytes_from(4), Some(b"ef".to_vec()));
    }

    #[test]
    fn test_bytes_from_future_offset() {
        let mut bl = ReplicationBacklog::new(1024);
        bl.append(b"abc");
        assert_eq!(bl.bytes_from(10), None);
    }

    #[test]
    fn test_contains_offset() {
        let mut bl = ReplicationBacklog::new(4);
        bl.append(b"abcdef"); // capacity 4 -> evicts a,b -> [c,d,e,f], start=2, end=6
        assert!(!bl.contains_offset(0));
        assert!(!bl.contains_offset(1));
        assert!(bl.contains_offset(2));
        assert!(bl.contains_offset(4));
        assert!(bl.contains_offset(6));
        assert!(!bl.contains_offset(7));
    }

    #[test]
    fn test_large_append_eviction() {
        // Append 2MB to a 1MB backlog
        let capacity = 1024 * 1024; // 1MB
        let mut bl = ReplicationBacklog::new(capacity);
        let data = vec![0xABu8; 2 * capacity]; // 2MB
        bl.append(&data);
        assert_eq!(bl.start_offset(), capacity as u64);
        assert_eq!(bl.end_offset(), 2 * capacity as u64);
        // Offset 0 evicted
        assert_eq!(bl.bytes_from(0), None);
        // Offset at start_offset is valid
        assert!(bl.bytes_from(capacity as u64).is_some());
    }

    #[test]
    fn test_monotonic_offsets() {
        let mut bl = ReplicationBacklog::new(8);
        bl.append(b"1234");
        let s1 = bl.start_offset();
        let e1 = bl.end_offset();
        bl.append(b"5678");
        let s2 = bl.start_offset();
        let e2 = bl.end_offset();
        assert!(s2 >= s1, "start_offset must be monotonic");
        assert!(e2 > e1, "end_offset must be strictly increasing on append");
        bl.append(b"9abc");
        let s3 = bl.start_offset();
        let e3 = bl.end_offset();
        assert!(s3 >= s2, "start_offset must be monotonic");
        assert!(e3 > e2, "end_offset must be strictly increasing on append");
    }
}
