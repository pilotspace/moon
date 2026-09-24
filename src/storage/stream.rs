//! Redis Stream data type: append-only log with consumer group support.
//!
//! Entries are keyed by StreamId (milliseconds-sequence) and stored in a BTreeMap
//! for ordered iteration. Consumer groups, pending entries, and per-consumer state
//! are stored for XREADGROUP/XACK support (Plan 02).

use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};

use super::entry::current_time_ms;
use super::mem_size::{amortized_hash_slot, size_class, vec_bytes};
use super::owned_bytes::detach;

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
    ///
    /// moon#1198: two `itoa` renders into a stack buffer and ONE exact-size
    /// copy — this runs per entry of every XRANGE/XREAD/XREADGROUP reply.
    /// `format!` built a growing `String` and `Bytes::from` then allocated a
    /// shared header for its slack capacity.
    pub fn to_bytes(self) -> Bytes {
        // u64::MAX renders in 20 digits: 20 + 1 + 20.
        let mut buf = [0u8; 41];
        let mut ms = itoa::Buffer::new();
        let ms = ms.format(self.ms).as_bytes();
        let mut seq = itoa::Buffer::new();
        let seq = seq.format(self.seq).as_bytes();
        let dash = ms.len();
        let end = dash + 1 + seq.len();
        buf[..dash].copy_from_slice(ms);
        buf[dash] = b'-';
        buf[dash + 1..end].copy_from_slice(seq);
        Bytes::copy_from_slice(&buf[..end])
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
    /// True if this stream is managed by MQ.* commands (at-least-once delivery).
    pub durable: bool,
    /// Maximum delivery attempts before dead-letter routing (0 = disabled).
    pub max_delivery_count: u32,
    /// moon#1163: the contents bytes the `used_memory` ledger carries for
    /// this stream — what `entry_overhead` bills (via
    /// [`Self::billed_memory`]) and therefore what a DEL credits back.
    ///
    /// Kept apart from the true size ([`Self::estimate_memory`], a scan) so
    /// charge and credit can never disagree: every change to it is made in
    /// lockstep with `used_memory` ([`Self::take_unbilled`] at the command
    /// sites, [`Self::settle_billing`] where a whole value enters the
    /// keyspace). A mutation that nobody bills — a caller outside the stream
    /// commands — is picked up by the next command that drains it, and until
    /// then is simply not yet counted; it can never be credited without
    /// having been charged (the moon#861 saturation class).
    billed: usize,
    /// Exact byte delta of the mutations made since the last drain, kept by
    /// every mutating method below from the insert/remove results it sees.
    unbilled: isize,
    /// `billed` has been measured against the contents (see
    /// [`Self::settle_billing`]). A stream built field by field by a loader
    /// is not, until it enters the keyspace.
    settled: bool,
}

// ---------------------------------------------------------------------------
// moon#1163: allocator-truthful per-element costs, shared by the O(1) deltas
// the mutating methods keep and by the O(n) `estimate_memory` scan, so the two
// cannot drift. Same method as `storage::db`'s moon#788 constants: the
// element's own bytes at their jemalloc size class, plus its container slot.
// ---------------------------------------------------------------------------

/// Fixed part of a stream's contents (its own scalars and empty maps).
const STREAM_BASE: usize = 64;

/// Per-pair share of a std `BTreeMap` leaf: up to 11 pairs plus ~12 B of
/// header in one allocation. Stream ids, PEL ids and pending ids are appended
/// in order, and an in-order insert splits a full leaf in two halves the map
/// never refills — a leaf carries ~5 live pairs. Internal nodes (< 1/6 of
/// the leaves) are left out; the leaf rounding already errs high.
const fn btree_slot(kv: usize) -> usize {
    size_class(12 + 11 * kv) / 5
}

/// One entry's slot in `entries`.
const ENTRY_SLOT: usize = btree_slot(std::mem::size_of::<(StreamId, Vec<(Bytes, Bytes)>)>());
/// One PEL entry (its `consumer` is a clone of the consumer's stored name).
const PEL_SLOT: usize = btree_slot(std::mem::size_of::<(StreamId, PendingEntry)>());
/// One id in a consumer's pending set.
const PENDING_SLOT: usize = btree_slot(std::mem::size_of::<(StreamId, ())>());
/// One consumer's slot in its group's map.
const CONSUMER_SLOT: usize = amortized_hash_slot(std::mem::size_of::<(Bytes, Consumer)>());
/// One group's slot in `groups`.
const GROUP_SLOT: usize = amortized_hash_slot(std::mem::size_of::<(Bytes, ConsumerGroup)>());

/// Bytes one stream entry costs: its slot, its field vector (sized by the
/// capacity the caller allocated) and every field and value.
#[inline]
pub(crate) fn entry_cost(fields: &Vec<(Bytes, Bytes)>) -> usize {
    ENTRY_SLOT
        + vec_bytes(fields.capacity(), std::mem::size_of::<(Bytes, Bytes)>())
        + fields
            .iter()
            .map(|(f, v)| size_class(f.len()) + size_class(v.len()))
            .sum::<usize>()
}

/// A consumer without its pending ids (its name is ONE allocation shared by
/// the map key and `Consumer::name`).
#[inline]
fn consumer_cost(name: &[u8]) -> usize {
    CONSUMER_SLOT + size_class(name.len())
}

/// A group without its consumers and PEL.
#[inline]
fn group_shell_cost(name: &[u8]) -> usize {
    GROUP_SLOT + size_class(name.len())
}

/// A whole group: shell, consumers, their pending ids and the PEL.
fn group_cost(name: &[u8], group: &ConsumerGroup) -> usize {
    group_shell_cost(name)
        + group.pel.len() * PEL_SLOT
        + group
            .consumers
            .iter()
            .map(|(cname, c)| consumer_cost(cname) + c.pending.len() * PENDING_SLOT)
            .sum::<usize>()
}

#[inline]
fn signed(bytes: usize) -> isize {
    isize::try_from(bytes).unwrap_or(isize::MAX)
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

/// The options of one `XCLAIM`, parsed (see [`Stream::xclaim`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct XclaimOptions {
    /// `min-idle-time`: an already-pending entry idle for less is skipped.
    pub min_idle: u64,
    /// The delivery time to stamp, as `TIME` gave it or `now - IDLE`;
    /// `None` stamps now. Out-of-range values are clamped to now.
    pub delivery_time: Option<i64>,
    /// `RETRYCOUNT`: the delivery count to set; `None` increments it.
    pub retry_count: Option<u64>,
    /// `FORCE`: create the PEL entry for an id that is not pending.
    pub force: bool,
    /// `JUSTID`: reply with ids and leave the delivery count alone.
    pub justid: bool,
    /// `LASTID`: raise the group's last-delivered id to this, never lower it.
    pub last_id: Option<StreamId>,
}

impl Stream {
    pub fn new() -> Self {
        Stream {
            entries: BTreeMap::new(),
            length: 0,
            last_id: StreamId::ZERO,
            groups: HashMap::new(),
            durable: false,
            max_delivery_count: 0,
            billed: 0,
            unbilled: 0,
            settled: false,
        }
    }

    /// The contents' true size, by scan: every entry, group, consumer and
    /// PEL slot at its [`entry_cost`]-family price. O(n) — the ledger never
    /// calls it on a hot path (`MEMORY USAGE`, the settle of a whole value
    /// arriving, tests).
    fn contents_scan(&self) -> usize {
        self.entries.values().map(entry_cost).sum::<usize>()
            + self
                .groups
                .iter()
                .map(|(name, g)| group_cost(name, g))
                .sum::<usize>()
    }

    /// What the `used_memory` ledger carries for this stream (O(1)) — the
    /// figure `entry_overhead` uses to charge it on the way in and credit it
    /// on the way out (moon#1163).
    #[inline]
    pub fn billed_memory(&self) -> usize {
        STREAM_BASE + self.billed
    }

    /// Measure `billed` against the contents, once, for a stream that enters
    /// the keyspace whole (a loader built it field by field, or it arrives
    /// from RESTORE / the cold tier / a replica). Its first charge is then the
    /// truth, and every later change is a drained delta.
    pub fn settle_billing(&mut self) {
        if !self.settled {
            self.billed = self.contents_scan();
            self.unbilled = 0;
            self.settled = true;
        }
    }

    /// Fold the mutations made since the last drain into `billed` and return
    /// the change, which the caller applies to `used_memory` in the same
    /// breath (moon#1163). Clamped at zero, and the clamped figure is what is
    /// returned, so the ledger and `billed` move by the same amount.
    #[must_use]
    pub fn take_unbilled(&mut self) -> isize {
        let d = std::mem::take(&mut self.unbilled);
        let next = signed(self.billed).saturating_add(d).max(0);
        let actual = next - signed(self.billed);
        self.billed = usize::try_from(next).unwrap_or(0);
        actual
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
        validate_explicit_id_against(self.last_id, id)
    }

    /// Add an entry. Returns the assigned ID. Caller must ensure id > last_id.
    ///
    /// moon#1160: every field and value is stored as an exact-size copy —
    /// the callers (`XADD`, the MQ paths) hand in `Bytes` sliced from the
    /// request buffer, and storing those kept the whole buffer alive.
    pub fn add(&mut self, id: StreamId, mut fields: Vec<(Bytes, Bytes)>) -> StreamId {
        for (f, v) in &mut fields {
            *f = detach(f);
            *v = detach(v);
        }
        self.unbilled += signed(entry_cost(&fields));
        if let Some(old) = self.entries.insert(id, fields) {
            self.unbilled -= signed(entry_cost(&old));
        }
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
            if let Some((_, fields)) = self.entries.pop_first() {
                self.unbilled -= signed(entry_cost(&fields));
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
            if let Some(fields) = self.entries.remove(&id) {
                self.unbilled -= signed(entry_cost(&fields));
            }
        }
        self.length = self.length.saturating_sub(removed);
        removed
    }

    /// Delete specific entries by ID. Returns count of actually deleted.
    pub fn delete(&mut self, ids: &[StreamId]) -> u64 {
        let mut count = 0u64;
        for id in ids {
            if let Some(fields) = self.entries.remove(id) {
                self.unbilled -= signed(entry_cost(&fields));
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
        self.unbilled += signed(group_shell_cost(&name));
        self.groups.insert(
            // moon#1160: never store the request's slice.
            detach(&name),
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
        match self.groups.remove_entry(name) {
            Some((name, group)) => {
                self.unbilled -= signed(group_cost(&name, &group));
                true
            }
            None => false,
        }
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
            Self::insert_consumer(group, &consumer_name);
            self.unbilled += signed(consumer_cost(&consumer_name));
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
                let mut freed =
                    consumer_cost(consumer_name) + consumer.pending.len() * PENDING_SLOT;
                // Remove from group PEL
                for (id, _) in &consumer.pending {
                    if group.pel.remove(id).is_some() {
                        freed += PEL_SLOT;
                    }
                }
                self.unbilled -= signed(freed);
                Ok(count)
            }
            None => Ok(0),
        }
    }

    /// Insert a NEW consumer, its name stored as ONE exact-size copy that the
    /// map key and `Consumer::name` share (moon#1160: `consumer_name` is the
    /// request's slice). Returns the stored name.
    fn insert_consumer(group: &mut ConsumerGroup, consumer_name: &[u8]) -> Bytes {
        let name = detach(consumer_name);
        group.consumers.insert(
            name.clone(),
            Consumer {
                name: name.clone(),
                pending: BTreeMap::new(),
                seen_time: current_time_ms(),
            },
        );
        name
    }

    /// Ensure a consumer exists in a group, auto-creating if needed, and
    /// return its STORED name — the handle a PEL entry must keep (moon#1160:
    /// a clone of the caller's `consumer_name` pinned the request buffer once
    /// per delivered entry) — plus the bytes a creation added (moon#1163).
    fn ensure_consumer(group: &mut ConsumerGroup, consumer_name: &Bytes) -> (Bytes, isize) {
        if let Some(consumer) = group.consumers.get_mut(consumer_name) {
            consumer.seen_time = current_time_ms();
            (consumer.name.clone(), 0)
        } else {
            (
                Self::insert_consumer(group, consumer_name),
                signed(consumer_cost(consumer_name)),
            )
        }
    }

    /// Would a `>` read of `group_name` deliver anything right now — is there
    /// an entry after the group's last-delivered id? `None` when the group
    /// does not exist. Read-only: lets a waker decide a group reader is
    /// servable, and win its claim, BEFORE the read moves anything into a PEL
    /// (moon#1047).
    pub fn group_has_new(&self, group_name: &[u8]) -> Option<bool> {
        let group = self.groups.get(group_name)?;
        let last = group.last_delivered_id;
        Some(
            self.entries
                .range((std::ops::Bound::Excluded(last), std::ops::Bound::Unbounded))
                .next()
                .is_some(),
        )
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
        let (stored_name, created) = Self::ensure_consumer(group, consumer_name);
        let mut delta = created;

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
                if group
                    .pel
                    .insert(
                        id,
                        PendingEntry {
                            consumer: stored_name.clone(),
                            delivery_time: now,
                            delivery_count: 1,
                        },
                    )
                    .is_none()
                {
                    delta += signed(PEL_SLOT);
                }
                // Add to consumer's pending set
                if let Some(c) = group.consumers.get_mut(consumer_name)
                    && c.pending.insert(id, ()).is_none()
                {
                    delta += signed(PENDING_SLOT);
                }
            }
        }
        self.unbilled += delta;
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
        let mut freed = 0usize;
        for id in ids {
            if let Some(pe) = group.pel.remove(id) {
                count += 1;
                freed += PEL_SLOT;
                // Remove from consumer's pending set
                if let Some(c) = group.consumers.get_mut(&pe.consumer)
                    && c.pending.remove(id).is_some()
                {
                    freed += PENDING_SLOT;
                }
            }
        }
        self.unbilled -= signed(freed);
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

    /// `XCLAIM`: move pending entries to `consumer_name`, redis's
    /// `xclaimCommand` semantics option for option. Returns the ids claimed,
    /// in argument order; every one of them names an entry that still exists,
    /// so a caller rendering full entries always finds them.
    ///
    /// This is also how a consumer-group read is REPLAYED: redis propagates
    /// `XREADGROUP` as one `XCLAIM key group consumer 0 id TIME t RETRYCOUNT n
    /// FORCE JUSTID LASTID id` per delivered entry, and moon's blocking group
    /// reads log the same records (moon#1104). `FORCE` is what recreates the
    /// PEL entry on the replaying side, `TIME` / `RETRYCOUNT` restore its
    /// delivery metadata exactly, and `LASTID` never moves the cursor back.
    ///
    /// * An id whose entry was deleted from the stream is not claimable, and a
    ///   PEL entry left behind for it is dropped.
    /// * `FORCE` creates a PEL entry for an id that exists in the stream but
    ///   is not pending; `min_idle` does not apply to it.
    /// * The delivery count is set to `retry_count` when given, otherwise
    ///   incremented unless `justid`.
    pub fn xclaim(
        &mut self,
        group_name: &[u8],
        consumer_name: &Bytes,
        ids: &[StreamId],
        opts: &XclaimOptions,
    ) -> Result<Vec<StreamId>, &'static str> {
        let entries = &self.entries;
        let group = self
            .groups
            .get_mut(group_name)
            .ok_or("NOGROUP No such consumer group for key name")?;
        let now = current_time_ms();
        if let Some(last) = opts.last_id
            && last > group.last_delivered_id
        {
            group.last_delivered_id = last;
        }
        // Redis clamps a bogus TIME/IDLE (negative, or in the future) to now
        // rather than failing a command that may already have claimed.
        let delivery_time = match opts.delivery_time {
            Some(t) if t >= 0 && (t as u64) <= now => t as u64,
            _ => now,
        };
        let (stored_name, created) = Self::ensure_consumer(group, consumer_name);
        let mut delta = created;

        let mut claimed = Vec::with_capacity(ids.len());
        for &id in ids {
            if !entries.contains_key(&id) {
                if let Some(stale) = group.pel.remove(&id) {
                    delta -= signed(PEL_SLOT);
                    if let Some(c) = group.consumers.get_mut(&stale.consumer)
                        && c.pending.remove(&id).is_some()
                    {
                        delta -= signed(PENDING_SLOT);
                    }
                }
                continue;
            }
            let forced = if group.pel.contains_key(&id) {
                false
            } else if opts.force {
                group.pel.insert(
                    id,
                    PendingEntry {
                        consumer: stored_name.clone(),
                        delivery_time: now,
                        delivery_count: 1,
                    },
                );
                delta += signed(PEL_SLOT);
                true
            } else {
                continue;
            };
            let Some(pe) = group.pel.get_mut(&id) else {
                continue;
            };
            if !forced && opts.min_idle > 0 && now.saturating_sub(pe.delivery_time) < opts.min_idle
            {
                continue;
            }
            if forced || pe.consumer != *consumer_name {
                if !forced
                    && let Some(c) = group.consumers.get_mut(&pe.consumer)
                    && c.pending.remove(&id).is_some()
                {
                    delta -= signed(PENDING_SLOT);
                }
                pe.consumer = stored_name.clone();
                if let Some(c) = group.consumers.get_mut(consumer_name)
                    && c.pending.insert(id, ()).is_none()
                {
                    delta += signed(PENDING_SLOT);
                }
            }
            pe.delivery_time = delivery_time;
            match opts.retry_count {
                Some(n) => pe.delivery_count = n,
                None if !opts.justid => pe.delivery_count += 1,
                None => {}
            }
            claimed.push(id);
        }
        self.unbilled += delta;
        Ok(claimed)
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
        let (stored_name, created) = Self::ensure_consumer(group, consumer_name);
        let mut delta = created;

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
                if let Some(c) = group.consumers.get_mut(old_consumer)
                    && c.pending.remove(id).is_some()
                {
                    delta -= signed(PENDING_SLOT);
                }

                // Update PEL entry
                if let Some(pe) = group.pel.get_mut(id) {
                    pe.consumer = stored_name.clone();
                    pe.delivery_time = now;
                    pe.delivery_count += 1;
                }

                // Add to new consumer's pending
                if let Some(c) = group.consumers.get_mut(consumer_name)
                    && c.pending.insert(*id, ()).is_none()
                {
                    delta += signed(PENDING_SLOT);
                }

                if let Some(fields) = self.entries.get(id) {
                    claimed.push((*id, fields.clone()));
                }
            } else {
                // Entry was deleted from stream, remove from PEL
                if group.pel.remove(id).is_some() {
                    delta -= signed(PEL_SLOT);
                }
                if let Some(c) = group.consumers.get_mut(old_consumer)
                    && c.pending.remove(id).is_some()
                {
                    delta -= signed(PENDING_SLOT);
                }
                deleted.push(*id);
            }
        }
        self.unbilled += delta;

        // If we didn't find a next cursor (scanned all candidates), return 0-0
        if next_id == StreamId::ZERO && scanned > 0 {
            next_id = StreamId::ZERO; // signals end of iteration
        }

        Ok((next_id, claimed, deleted))
    }

    /// The stream's true size, by scan (O(n)): the fixed part plus every
    /// entry, group, consumer and PEL slot at the same prices the O(1) deltas
    /// use (moon#1163). What `MEMORY USAGE` reports; the ledger bills
    /// [`Self::billed_memory`].
    pub fn estimate_memory(&self) -> usize {
        STREAM_BASE + self.contents_scan()
    }
}

/// The ordering rule `XADD` enforces on an explicit ID, taking `last_id` rather
/// than `&self`.
///
/// Factored out of [`Stream::validate_explicit_id`] for moon#823: `xadd`
/// has to apply this rule BEFORE `get_or_create_stream`, or every rejected ID
/// leaves a phantom stream in the keyspace that is charged, `DBSIZE`-visible,
/// and never written to the AOF. `validate_explicit_id` now delegates here, so
/// the pre-check and the post-check cannot drift apart.
pub fn validate_explicit_id_against(
    last_id: StreamId,
    id: StreamId,
) -> Result<StreamId, &'static str> {
    if id <= last_id {
        return Err(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item",
        );
    }
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// moon#1198: the rendering is exact for every width, and lands in an
    /// exact-size buffer — `format!`'s `String` carried slack capacity that
    /// `Bytes::from` then had to wrap in a second, shared allocation.
    #[test]
    fn stream_id_renders_exactly_into_an_exact_buffer() {
        for (ms, seq) in [
            (0u64, 0u64),
            (1, 2),
            (1_700_000_000_123, 7),
            (u64::MAX, u64::MAX),
            (u64::MAX, 0),
            (9, 10),
        ] {
            let b = StreamId { ms, seq }.to_bytes();
            assert_eq!(b, Bytes::from(format!("{ms}-{seq}")));
            let len = b.len();
            let m = b
                .try_into_mut()
                .unwrap_or_else(|b| bytes::BytesMut::from(&b[..]));
            assert_eq!(
                m.capacity(),
                len,
                "{ms}-{seq}: rendered through a buffer with {} B of slack",
                m.capacity() - len
            );
        }
    }

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

    #[test]
    fn test_stream_new_defaults() {
        let s = Stream::new();
        assert!(!s.durable, "new streams should not be durable by default");
        assert_eq!(
            s.max_delivery_count, 0,
            "max_delivery_count should default to 0 (disabled)"
        );
    }
}
