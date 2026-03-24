//! Redis Stream data type: append-only log with consumer group support.
//!
//! Entries are keyed by StreamId (milliseconds-sequence) and stored in a BTreeMap
//! for ordered iteration. Consumer groups, pending entries, and per-consumer state
//! are stored for XREADGROUP/XACK support (Plan 02).

use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};

use super::entry::current_time_ms;

/// Stream entry ID: <milliseconds>-<sequence>.
/// Ordered by (ms, seq) for BTreeMap keying.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub const ZERO: StreamId = StreamId { ms: 0, seq: 0 };
    pub const MAX: StreamId = StreamId {
        ms: u64::MAX,
        seq: u64::MAX,
    };

    /// Parse from bytes: "ms-seq", "ms" (default_seq used), "-" (ZERO), "+" (MAX).
    /// "*" is NOT parsed here -- caller handles auto-ID generation.
    pub fn parse(s: &[u8], default_seq: u64) -> Result<Self, &'static str> {
        let s_str = std::str::from_utf8(s).map_err(|_| "Invalid UTF-8 in stream ID")?;
        if s_str == "-" {
            return Ok(StreamId::ZERO);
        }
        if s_str == "+" {
            return Ok(StreamId::MAX);
        }
        if s_str == "*" {
            return Err("Auto-ID '*' must be handled by caller");
        }

        if let Some(pos) = s_str.find('-') {
            let ms_part = &s_str[..pos];
            let seq_part = &s_str[pos + 1..];
            let ms = ms_part
                .parse::<u64>()
                .map_err(|_| "Invalid milliseconds in stream ID")?;
            if seq_part == "*" {
                // "ms-*" means auto-sequence: use default_seq
                return Ok(StreamId {
                    ms,
                    seq: default_seq,
                });
            }
            let seq = seq_part
                .parse::<u64>()
                .map_err(|_| "Invalid sequence in stream ID")?;
            Ok(StreamId { ms, seq })
        } else {
            let ms = s_str
                .parse::<u64>()
                .map_err(|_| "Invalid stream ID format")?;
            Ok(StreamId {
                ms,
                seq: default_seq,
            })
        }
    }

    /// Format as "ms-seq" Bytes.
    pub fn to_bytes(self) -> Bytes {
        Bytes::from(format!("{}-{}", self.ms, self.seq))
    }
}

/// A Redis Stream: append-only log with consumer group support.
#[derive(Debug, Clone)]
pub struct Stream {
    /// Entries ordered by ID. Value is Vec of (field, value) pairs.
    pub entries: BTreeMap<StreamId, Vec<(Bytes, Bytes)>>,
    /// Logical length (entries.len() tracks actual, this tracks adds - deletes).
    pub length: u64,
    /// Last generated entry ID (for monotonic guarantee).
    pub last_id: StreamId,
    /// Consumer groups keyed by group name.
    pub groups: HashMap<Bytes, ConsumerGroup>,
}

/// Consumer group state.
#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub last_delivered_id: StreamId,
    pub pel: BTreeMap<StreamId, PendingEntry>,
    pub consumers: HashMap<Bytes, Consumer>,
}

/// A pending (unacknowledged) entry in the PEL.
#[derive(Debug, Clone)]
pub struct PendingEntry {
    pub consumer: Bytes,
    pub delivery_time: u64,
    pub delivery_count: u64,
}

/// Per-consumer state within a group.
#[derive(Debug, Clone)]
pub struct Consumer {
    pub name: Bytes,
    pub pending: BTreeMap<StreamId, ()>,
    pub seen_time: u64,
}

impl Stream {
    pub fn new() -> Self {
        Stream {
            entries: BTreeMap::new(),
            length: 0,
            last_id: StreamId::ZERO,
            groups: HashMap::new(),
        }
    }

    /// Generate next auto-ID. Uses max(now_ms, last_id.ms) for monotonic guarantee.
    /// If clock goes backward, reuses last_id.ms and increments seq.
    pub fn next_auto_id(&mut self) -> StreamId {
        let now_ms = current_time_ms();
        if now_ms > self.last_id.ms {
            StreamId {
                ms: now_ms,
                seq: 0,
            }
        } else {
            StreamId {
                ms: self.last_id.ms,
                seq: self.last_id.seq + 1,
            }
        }
    }

    /// Validate an explicit ID (must be > last_id, or for 0-0 stream first entry must be > 0-0).
    pub fn validate_explicit_id(&self, id: StreamId) -> Result<StreamId, &'static str> {
        if id <= self.last_id {
            return Err(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item",
            );
        }
        Ok(id)
    }

    /// Add an entry. Returns the assigned ID. Caller must ensure id > last_id.
    pub fn add(&mut self, id: StreamId, fields: Vec<(Bytes, Bytes)>) -> StreamId {
        self.entries.insert(id, fields);
        self.length += 1;
        self.last_id = id;
        id
    }

    /// Range query [start..=end] with optional count limit.
    pub fn range(
        &self,
        start: StreamId,
        end: StreamId,
        count: Option<usize>,
    ) -> Vec<(StreamId, &Vec<(Bytes, Bytes)>)> {
        let mut result = Vec::new();
        for (&id, fields) in self.entries.range(start..=end) {
            if let Some(c) = count {
                if result.len() >= c {
                    break;
                }
            }
            result.push((id, fields));
        }
        result
    }

    /// Reverse range query [start..=end], iterating from end to start.
    pub fn range_rev(
        &self,
        start: StreamId,
        end: StreamId,
        count: Option<usize>,
    ) -> Vec<(StreamId, &Vec<(Bytes, Bytes)>)> {
        let mut result = Vec::new();
        for (&id, fields) in self.entries.range(start..=end).rev() {
            if let Some(c) = count {
                if result.len() >= c {
                    break;
                }
            }
            result.push((id, fields));
        }
        result
    }

    /// Trim by MAXLEN. If approximate, only trim when len exceeds maxlen by ~10%.
    /// Returns count of removed entries.
    pub fn trim_maxlen(&mut self, maxlen: u64, approximate: bool) -> u64 {
        let current = self.entries.len() as u64;
        if current <= maxlen {
            return 0;
        }
        if approximate && current <= maxlen + maxlen / 10 + 1 {
            return 0;
        }
        let to_remove = (current - maxlen) as usize;
        let mut removed = 0u64;
        for _ in 0..to_remove {
            if let Some((&id, _)) = self.entries.iter().next() {
                self.entries.remove(&id);
                removed += 1;
            }
        }
        self.length = self.length.saturating_sub(removed);
        removed
    }

    /// Trim by MINID -- remove entries with ID < minid.
    /// If approximate, may keep some entries below minid.
    pub fn trim_minid(&mut self, minid: StreamId, approximate: bool) -> u64 {
        let to_remove: Vec<StreamId> = self
            .entries
            .range(..minid)
            .map(|(&id, _)| id)
            .collect();
        if approximate && to_remove.len() <= 1 {
            return 0;
        }
        let removed = to_remove.len() as u64;
        for id in to_remove {
            self.entries.remove(&id);
        }
        self.length = self.length.saturating_sub(removed);
        removed
    }

    /// Delete specific entries by ID. Returns count of actually deleted.
    pub fn delete(&mut self, ids: &[StreamId]) -> u64 {
        let mut count = 0u64;
        for id in ids {
            if self.entries.remove(id).is_some() {
                count += 1;
            }
        }
        self.length = self.length.saturating_sub(count);
        count
    }

    /// Estimate memory usage.
    pub fn estimate_memory(&self) -> usize {
        let mut mem = 64; // struct overhead
        for (_, fields) in &self.entries {
            mem += 16; // StreamId
            for (f, v) in fields {
                mem += f.len() + v.len() + 48;
            }
        }
        // Consumer groups overhead
        for (name, group) in &self.groups {
            mem += name.len() + 64;
            mem += group.pel.len() * 64;
            for (cname, consumer) in &group.consumers {
                mem += cname.len() + 48;
                mem += consumer.pending.len() * 16;
            }
        }
        mem
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_id_parse_ms_seq() {
        let id = StreamId::parse(b"1234-5", 0).unwrap();
        assert_eq!(id.ms, 1234);
        assert_eq!(id.seq, 5);
    }

    #[test]
    fn test_stream_id_parse_ms_only() {
        let id = StreamId::parse(b"1234", 99).unwrap();
        assert_eq!(id.ms, 1234);
        assert_eq!(id.seq, 99);
    }

    #[test]
    fn test_stream_id_parse_special() {
        assert_eq!(StreamId::parse(b"-", 0).unwrap(), StreamId::ZERO);
        assert_eq!(StreamId::parse(b"+", 0).unwrap(), StreamId::MAX);
    }

    #[test]
    fn test_stream_id_parse_star_rejected() {
        assert!(StreamId::parse(b"*", 0).is_err());
    }

    #[test]
    fn test_stream_id_parse_ms_star_seq() {
        // "1234-*" should use default_seq
        let id = StreamId::parse(b"1234-*", 42).unwrap();
        assert_eq!(id.ms, 1234);
        assert_eq!(id.seq, 42);
    }

    #[test]
    fn test_stream_id_ordering() {
        let a = StreamId { ms: 1, seq: 0 };
        let b = StreamId { ms: 1, seq: 1 };
        let c = StreamId { ms: 2, seq: 0 };
        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
    }

    #[test]
    fn test_stream_id_to_bytes() {
        let id = StreamId { ms: 100, seq: 3 };
        assert_eq!(id.to_bytes().as_ref(), b"100-3");
    }

    #[test]
    fn test_stream_add_and_range() {
        let mut s = Stream::new();
        let id1 = StreamId { ms: 1, seq: 0 };
        let id2 = StreamId { ms: 2, seq: 0 };
        let id3 = StreamId { ms: 3, seq: 0 };

        s.add(
            id1,
            vec![(Bytes::from("f1"), Bytes::from("v1"))],
        );
        s.add(
            id2,
            vec![(Bytes::from("f2"), Bytes::from("v2"))],
        );
        s.add(
            id3,
            vec![(Bytes::from("f3"), Bytes::from("v3"))],
        );

        assert_eq!(s.length, 3);
        assert_eq!(s.last_id, id3);

        let range = s.range(StreamId::ZERO, StreamId::MAX, None);
        assert_eq!(range.len(), 3);
        assert_eq!(range[0].0, id1);
        assert_eq!(range[2].0, id3);
    }

    #[test]
    fn test_stream_range_with_count() {
        let mut s = Stream::new();
        for i in 0..10 {
            s.add(
                StreamId { ms: i, seq: 0 },
                vec![(Bytes::from("f"), Bytes::from("v"))],
            );
        }
        let range = s.range(StreamId::ZERO, StreamId::MAX, Some(3));
        assert_eq!(range.len(), 3);
        assert_eq!(range[0].0.ms, 0);
        assert_eq!(range[2].0.ms, 2);
    }

    #[test]
    fn test_stream_range_rev() {
        let mut s = Stream::new();
        for i in 0..5 {
            s.add(
                StreamId { ms: i, seq: 0 },
                vec![(Bytes::from("f"), Bytes::from("v"))],
            );
        }
        let rev = s.range_rev(StreamId::ZERO, StreamId::MAX, Some(2));
        assert_eq!(rev.len(), 2);
        assert_eq!(rev[0].0.ms, 4);
        assert_eq!(rev[1].0.ms, 3);
    }

    #[test]
    fn test_stream_auto_id_monotonic() {
        let mut s = Stream::new();
        // Set last_id to far future to simulate clock going backward
        s.last_id = StreamId {
            ms: u64::MAX - 1,
            seq: 5,
        };
        let id = s.next_auto_id();
        assert_eq!(id.ms, u64::MAX - 1);
        assert_eq!(id.seq, 6);
    }

    #[test]
    fn test_stream_validate_explicit_id() {
        let mut s = Stream::new();
        s.add(
            StreamId { ms: 10, seq: 0 },
            vec![(Bytes::from("f"), Bytes::from("v"))],
        );
        // ID greater than last is valid
        assert!(s.validate_explicit_id(StreamId { ms: 11, seq: 0 }).is_ok());
        // ID equal to last is invalid
        assert!(s.validate_explicit_id(StreamId { ms: 10, seq: 0 }).is_err());
        // ID less than last is invalid
        assert!(s.validate_explicit_id(StreamId { ms: 9, seq: 0 }).is_err());
    }

    #[test]
    fn test_stream_trim_maxlen_exact() {
        let mut s = Stream::new();
        for i in 0..10 {
            s.add(
                StreamId { ms: i, seq: 0 },
                vec![(Bytes::from("f"), Bytes::from("v"))],
            );
        }
        let removed = s.trim_maxlen(5, false);
        assert_eq!(removed, 5);
        assert_eq!(s.entries.len(), 5);
        assert_eq!(s.length, 5);
        // Should have kept entries 5-9
        assert!(s.entries.contains_key(&StreamId { ms: 5, seq: 0 }));
        assert!(!s.entries.contains_key(&StreamId { ms: 4, seq: 0 }));
    }

    #[test]
    fn test_stream_trim_maxlen_approximate() {
        let mut s = Stream::new();
        for i in 0..10 {
            s.add(
                StreamId { ms: i, seq: 0 },
                vec![(Bytes::from("f"), Bytes::from("v"))],
            );
        }
        // With approximate, small excess might not trigger trim
        let removed = s.trim_maxlen(9, true);
        // 10 entries, maxlen 9 => excess is 1, threshold is 9 + 0 + 1 = 10, so should NOT trim
        assert_eq!(removed, 0);

        // Large excess should trim
        let removed = s.trim_maxlen(5, true);
        assert_eq!(removed, 5);
    }

    #[test]
    fn test_stream_trim_minid() {
        let mut s = Stream::new();
        for i in 0..10 {
            s.add(
                StreamId { ms: i, seq: 0 },
                vec![(Bytes::from("f"), Bytes::from("v"))],
            );
        }
        let removed = s.trim_minid(StreamId { ms: 5, seq: 0 }, false);
        assert_eq!(removed, 5);
        assert_eq!(s.entries.len(), 5);
        assert!(s.entries.contains_key(&StreamId { ms: 5, seq: 0 }));
    }

    #[test]
    fn test_stream_delete() {
        let mut s = Stream::new();
        for i in 0..5 {
            s.add(
                StreamId { ms: i, seq: 0 },
                vec![(Bytes::from("f"), Bytes::from("v"))],
            );
        }
        let deleted = s.delete(&[
            StreamId { ms: 1, seq: 0 },
            StreamId { ms: 3, seq: 0 },
            StreamId { ms: 99, seq: 0 }, // doesn't exist
        ]);
        assert_eq!(deleted, 2);
        assert_eq!(s.entries.len(), 3);
        assert_eq!(s.length, 3);
    }

    #[test]
    fn test_stream_estimate_memory() {
        let mut s = Stream::new();
        s.add(
            StreamId { ms: 1, seq: 0 },
            vec![(Bytes::from("field"), Bytes::from("value"))],
        );
        let mem = s.estimate_memory();
        assert!(mem > 0);
    }
}
