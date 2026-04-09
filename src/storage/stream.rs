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
            StreamId { ms: now_ms, seq: 0 }
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
        let to_remove: Vec<StreamId> = self.entries.range(..minid).map(|(&id, _)| id).collect();
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

    // ---- Consumer group methods (Plan 02) ----

    /// Create a consumer group. Returns Err if group already exists.
    pub fn create_group(
        &mut self,
        name: Bytes,
        last_delivered_id: StreamId,
    ) -> Result<(), &'static str> {
        if self.groups.contains_key(&name) {
            return Err("BUSYGROUP Consumer Group name already exists");
        }
        self.groups.insert(
            name,
            ConsumerGroup {
                last_delivered_id,
                pel: BTreeMap::new(),
                consumers: HashMap::new(),
            },
        );
        Ok(())
    }

    /// Destroy a consumer group. Returns true if it existed.
    pub fn destroy_group(&mut self, name: &[u8]) -> bool {
        self.groups.remove(name).is_some()
    }

    /// Set the last-delivered-id for a group.
    pub fn set_group_id(&mut self, name: &[u8], id: StreamId) -> Result<(), &'static str> {
        match self.groups.get_mut(name) {
            Some(group) => {
                group.last_delivered_id = id;
                Ok(())
            }
            None => Err("NOGROUP No such consumer group for key name"),
        }
    }

    /// Create a consumer in a group. Returns true if created, false if already exists.
    pub fn create_consumer(
        &mut self,
        group_name: &[u8],
        consumer_name: Bytes,
    ) -> Result<bool, &'static str> {
        let group = self
            .groups
            .get_mut(group_name)
            .ok_or("NOGROUP No such consumer group for key name")?;
        if group.consumers.contains_key(&consumer_name) {
            Ok(false)
        } else {
            group.consumers.insert(
                consumer_name.clone(),
                Consumer {
                    name: consumer_name,
                    pending: BTreeMap::new(),
                    seen_time: current_time_ms(),
                },
            );
            Ok(true)
        }
    }

    /// Delete a consumer from a group. Returns the number of pending entries that were dropped.
    pub fn delete_consumer(
        &mut self,
        group_name: &[u8],
        consumer_name: &[u8],
    ) -> Result<u64, &'static str> {
        let group = self
            .groups
            .get_mut(group_name)
            .ok_or("NOGROUP No such consumer group for key name")?;
        match group.consumers.remove(consumer_name) {
            Some(consumer) => {
                let count = consumer.pending.len() as u64;
                // Remove from group PEL
                for (id, _) in &consumer.pending {
                    group.pel.remove(id);
                }
                Ok(count)
            }
            None => Ok(0),
        }
    }

    /// Ensure a consumer exists in a group, auto-creating if needed.
    fn ensure_consumer(group: &mut ConsumerGroup, consumer_name: &Bytes) {
        if let Some(consumer) = group.consumers.get_mut(consumer_name) {
            consumer.seen_time = current_time_ms();
        } else {
            group.consumers.insert(
                consumer_name.clone(),
                Consumer {
                    name: consumer_name.clone(),
                    pending: BTreeMap::new(),
                    seen_time: current_time_ms(),
                },
            );
        }
    }

    /// Read new entries for a consumer group (> semantics).
    /// Auto-creates consumer. Adds entries to PEL. Updates last_delivered_id.
    pub fn read_group_new(
        &mut self,
        group_name: &Bytes,
        consumer_name: &Bytes,
        count: Option<usize>,
        noack: bool,
    ) -> Result<Vec<(StreamId, Vec<(Bytes, Bytes)>)>, &'static str> {
        let group = self
            .groups
            .get_mut(group_name.as_ref())
            .ok_or("NOGROUP No such consumer group for key name")?;
        Self::ensure_consumer(group, consumer_name);

        let start = StreamId {
            ms: group.last_delivered_id.ms,
            seq: if group.last_delivered_id == StreamId::ZERO {
                0
            } else {
                group.last_delivered_id.seq.saturating_add(1)
            },
        };
        // Handle wrap
        let start = if group.last_delivered_id != StreamId::ZERO
            && group.last_delivered_id.seq == u64::MAX
        {
            StreamId {
                ms: group.last_delivered_id.ms.saturating_add(1),
                seq: 0,
            }
        } else {
            start
        };

        let mut results = Vec::new();
        let now = current_time_ms();
        for (&id, fields) in self.entries.range(start..=StreamId::MAX) {
            if let Some(c) = count {
                if results.len() >= c {
                    break;
                }
            }
            results.push((id, fields.clone()));

            // Update last_delivered_id
            group.last_delivered_id = id;

            if !noack {
                // Add to group PEL
                group.pel.insert(
                    id,
                    PendingEntry {
                        consumer: consumer_name.clone(),
                        delivery_time: now,
                        delivery_count: 1,
                    },
                );
                // Add to consumer's pending set
                if let Some(c) = group.consumers.get_mut(consumer_name) {
                    c.pending.insert(id, ());
                }
            }
        }
        Ok(results)
    }

    /// Read pending entries for a consumer (0 or explicit ID semantics).
    /// Does NOT add to PEL, just replays what consumer already has pending.
    pub fn read_group_pending(
        &mut self,
        group_name: &Bytes,
        consumer_name: &Bytes,
        start: StreamId,
        count: Option<usize>,
    ) -> Result<Vec<(StreamId, Vec<(Bytes, Bytes)>)>, &'static str> {
        let group = self
            .groups
            .get(group_name.as_ref())
            .ok_or("NOGROUP No such consumer group for key name")?;

        let consumer = match group.consumers.get(consumer_name.as_ref()) {
            Some(c) => c,
            None => return Ok(Vec::new()),
        };

        let mut results = Vec::new();
        for (&id, _) in consumer.pending.range(start..) {
            if let Some(c) = count {
                if results.len() >= c {
                    break;
                }
            }
            if let Some(fields) = self.entries.get(&id) {
                results.push((id, fields.clone()));
            }
        }
        Ok(results)
    }

    /// Acknowledge entries. Returns count of successfully acknowledged.
    pub fn xack(&mut self, group_name: &Bytes, ids: &[StreamId]) -> Result<u64, &'static str> {
        let group = self
            .groups
            .get_mut(group_name.as_ref())
            .ok_or("NOGROUP No such consumer group for key name")?;

        let mut count = 0u64;
        for id in ids {
            if let Some(pe) = group.pel.remove(id) {
                count += 1;
                // Remove from consumer's pending set
                if let Some(c) = group.consumers.get_mut(&pe.consumer) {
                    c.pending.remove(id);
                }
            }
        }
        Ok(count)
    }

    /// Get pending entries summary: [count, min_id, max_id, [[consumer, count], ...]]
    pub fn xpending_summary(
        &self,
        group_name: &Bytes,
    ) -> Result<Vec<(Bytes, StreamId, StreamId, Vec<(Bytes, u64)>)>, &'static str> {
        let group = self
            .groups
            .get(group_name.as_ref())
            .ok_or("NOGROUP No such consumer group for key name")?;

        if group.pel.is_empty() {
            return Ok(Vec::new()); // empty signals zero pending
        }

        // pel confirmed non-empty above — first/last keys are guaranteed to exist
        let Some(&min_id) = group.pel.keys().next() else {
            return Ok(Vec::new());
        };
        let Some(&max_id) = group.pel.keys().next_back() else {
            return Ok(Vec::new());
        };

        // Count per consumer
        let mut consumer_counts: HashMap<Bytes, u64> = HashMap::new();
        for pe in group.pel.values() {
            *consumer_counts.entry(pe.consumer.clone()).or_insert(0) += 1;
        }
        let consumers: Vec<(Bytes, u64)> = consumer_counts.into_iter().collect();

        // We return a single-element vec to signal "has data"
        Ok(vec![(Bytes::new(), min_id, max_id, consumers)])
    }

    /// Get pending entries detail.
    pub fn xpending_detail(
        &self,
        group_name: &Bytes,
        start: StreamId,
        end: StreamId,
        count: usize,
        consumer_filter: Option<&Bytes>,
    ) -> Result<Vec<(StreamId, Bytes, u64, u64)>, &'static str> {
        let group = self
            .groups
            .get(group_name.as_ref())
            .ok_or("NOGROUP No such consumer group for key name")?;

        let now = current_time_ms();
        let mut results = Vec::new();
        for (&id, pe) in group.pel.range(start..=end) {
            if results.len() >= count {
                break;
            }
            if let Some(filter) = consumer_filter {
                if &pe.consumer != filter {
                    continue;
                }
            }
            let idle = now.saturating_sub(pe.delivery_time);
            results.push((id, pe.consumer.clone(), idle, pe.delivery_count));
        }
        Ok(results)
    }

    /// Claim pending entries for a different consumer.
    pub fn xclaim(
        &mut self,
        group_name: &Bytes,
        consumer_name: &Bytes,
        min_idle_time: u64,
        ids: &[StreamId],
    ) -> Result<Vec<(StreamId, Vec<(Bytes, Bytes)>)>, &'static str> {
        let group = self
            .groups
            .get_mut(group_name.as_ref())
            .ok_or("NOGROUP No such consumer group for key name")?;
        Self::ensure_consumer(group, consumer_name);

        let now = current_time_ms();
        let mut results = Vec::new();
        for id in ids {
            if let Some(pe) = group.pel.get_mut(id) {
                let idle = now.saturating_sub(pe.delivery_time);
                if idle < min_idle_time {
                    continue;
                }

                // Remove from old consumer's pending
                let old_consumer = pe.consumer.clone();
                if let Some(c) = group.consumers.get_mut(&old_consumer) {
                    c.pending.remove(id);
                }

                // Transfer ownership
                pe.consumer = consumer_name.clone();
                pe.delivery_time = now;
                pe.delivery_count += 1;

                // Add to new consumer's pending
                if let Some(c) = group.consumers.get_mut(consumer_name) {
                    c.pending.insert(*id, ());
                }

                // Add entry data to results
                if let Some(fields) = self.entries.get(id) {
                    results.push((*id, fields.clone()));
                }
            }
        }
        Ok(results)
    }

    /// Auto-claim idle pending entries SCAN-style.
    /// Returns (next_cursor_id, claimed_entries, deleted_ids).
    pub fn xautoclaim(
        &mut self,
        group_name: &Bytes,
        consumer_name: &Bytes,
        min_idle_time: u64,
        start: StreamId,
        count: usize,
    ) -> Result<
        (
            StreamId,
            Vec<(StreamId, Vec<(Bytes, Bytes)>)>,
            Vec<StreamId>,
        ),
        &'static str,
    > {
        let group = self
            .groups
            .get_mut(group_name.as_ref())
            .ok_or("NOGROUP No such consumer group for key name")?;
        Self::ensure_consumer(group, consumer_name);

        let now = current_time_ms();
        let mut claimed = Vec::new();
        let mut deleted = Vec::new();
        let mut next_id = StreamId::ZERO;
        let mut scanned = 0;

        // Collect IDs to claim first to avoid borrow issues
        let candidates: Vec<(StreamId, Bytes)> = group
            .pel
            .range(start..)
            .filter(|(_, pe)| now.saturating_sub(pe.delivery_time) >= min_idle_time)
            .take(count + 1) // take one extra to know the next cursor
            .map(|(&id, pe)| (id, pe.consumer.clone()))
            .collect();

        for (i, (id, old_consumer)) in candidates.iter().enumerate() {
            if i >= count {
                // This is the "next cursor" entry
                next_id = *id;
                break;
            }
            scanned += 1;

            // Check if entry still exists in stream
            if self.entries.contains_key(id) {
                // Remove from old consumer's pending
                if let Some(c) = group.consumers.get_mut(old_consumer) {
                    c.pending.remove(id);
                }

                // Update PEL entry
                if let Some(pe) = group.pel.get_mut(id) {
                    pe.consumer = consumer_name.clone();
                    pe.delivery_time = now;
                    pe.delivery_count += 1;
                }

                // Add to new consumer's pending
                if let Some(c) = group.consumers.get_mut(consumer_name) {
                    c.pending.insert(*id, ());
                }

                if let Some(fields) = self.entries.get(id) {
                    claimed.push((*id, fields.clone()));
                }
            } else {
                // Entry was deleted from stream, remove from PEL
                group.pel.remove(id);
                if let Some(c) = group.consumers.get_mut(old_consumer) {
                    c.pending.remove(id);
                }
                deleted.push(*id);
            }
        }

        // If we didn't find a next cursor (scanned all candidates), return 0-0
        if next_id == StreamId::ZERO && scanned > 0 {
            next_id = StreamId::ZERO; // signals end of iteration
        }

        Ok((next_id, claimed, deleted))
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

        s.add(id1, vec![(Bytes::from("f1"), Bytes::from("v1"))]);
        s.add(id2, vec![(Bytes::from("f2"), Bytes::from("v2"))]);
        s.add(id3, vec![(Bytes::from("f3"), Bytes::from("v3"))]);

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
