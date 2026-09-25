//! The dead-slot ledger (moon#1215): spill-file slots that are still on disk
//! in a manifest-listed file but are no longer their key's cold-index entry.
//!
//! A spill file is unlinked only when its LAST live key leaves it, so a file
//! with live neighbours keeps every slot it was written with — including the
//! slots of keys that have since been deleted, flushed, overwritten,
//! read-promoted, re-spilled elsewhere, or expired. After a restart the cold
//! index is rebuilt from EVERY slot of every listed file, so each such slot
//! comes back unless something durable says it is gone. Until an AOF rewrite
//! that something is the DEL/FLUSH record in the log; the rewrite discards
//! it, and its new generation's `MOON.COLDCUT` authorizes the file wholesale.
//!
//! This ledger is what the rewrite uses instead: at the fold instant every
//! key it holds that is not alive gets a plain `DEL` at the head of the new
//! generation (`persistence::aof::fold_stream`). It lives inside
//! [`super::cold_index::ColdIndex`] so that every path that takes a slot out
//! of the index records it, with no call site to forget.
//!
//! # What is recorded (PR #1233 review)
//!
//! Every entry holds its key in RAM until its file is unlinked. An entry is
//! therefore kept only when it can do its one job — make a later AOF fold
//! write a `DEL` that a restart needs:
//!
//! - **No AOF writer in this process: nothing is recorded.** The only reader
//!   is an AOF rewrite fold, and a process whose `--appendonly` was off at
//!   boot never creates one (`CONFIG SET appendonly` does not start a
//!   writer). [`enable_ledger`] is called when an AOF writer's fold state is
//!   created — before recovery replays the log — so the replayed deletes are
//!   recorded too.
//! - **A slot whose own absolute TTL has passed is dropped.** The rebuild
//!   indexes it, but every value-giving cold read judges the slot's deadline
//!   (`cold_read::read_cold_entry` on the on-disk entry, the
//!   `ColdLocation::ttl_ms` checks in `kv_ops` on the index copy): it reads
//!   as expired, never as a value, so it cannot resurrect and needs no
//!   `DEL`. `ColdIndex::sweep_expired` does not record the slots it
//!   reclaims; an entry whose deadline passes after it was recorded is
//!   removed by [`DeadSlots::prune_expired`] (run by that sweep) and skipped
//!   by the fold ([`DeadSlots::keys_live_at`]). A wall clock stepped
//!   backwards is outside this argument, exactly as it is for an absolute
//!   `PEXPIREAT` carried by the base.
//!
//! Lifetime: a file's entries go when the file is unlinked (its slots are
//! gone from disk with it). A key that is alive again at a later fold is
//! filtered then, not forgotten, because its old slot is still on disk and
//! would come back the moment the key dies again.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;

/// One dead slot: its key and the slot's own absolute TTL (0 = none).
#[derive(Debug, Clone)]
struct DeadSlot {
    key: Bytes,
    ttl_ms: u64,
}

impl DeadSlot {
    /// Whether the slot reads as expired at `now_ms` (and at every later
    /// instant): the cold read path's own `now_ms > ttl_ms` rule.
    #[inline]
    fn expired_at(&self, now_ms: u64) -> bool {
        self.ttl_ms != 0 && now_ms > self.ttl_ms
    }
}

/// Approximate RAM of one ledger entry beyond its key bytes: the entry itself
/// (`Bytes` handle + TTL) in its per-file `Vec`. Monotonic, not exact — the
/// same approximation style as `cold_index::COLD_ENTRY_OVERHEAD`.
const DEAD_SLOT_OVERHEAD: usize = std::mem::size_of::<DeadSlot>();

#[inline]
fn dead_slot_cost(key_len: usize) -> usize {
    key_len + DEAD_SLOT_OVERHEAD
}

/// The dead slots of one file.
#[derive(Debug, Default)]
struct FileDead {
    slots: Vec<DeadSlot>,
    bytes: usize,
}

/// Set once an AOF writer's fold state exists in this process (see the
/// module doc). Never cleared: a writer lives for the process.
static LEDGER_ENABLED: AtomicBool = AtomicBool::new(false);

/// An AOF fold can consume the ledger from now on. Called when an AOF
/// writer's fold state is created (`aof::rewrite::RewriteOverflow::new`),
/// which every AOF writer pool does at boot, before the log is replayed.
pub fn enable_ledger() {
    LEDGER_ENABLED.store(true, Ordering::Relaxed);
}

/// Whether [`enable_ledger`] has run in this process — the production
/// predicate. Unit tests go through [`force_ledger`] instead.
#[inline]
pub fn aof_consumer_present() -> bool {
    LEDGER_ENABLED.load(Ordering::Relaxed)
}

#[cfg(test)]
thread_local! {
    /// Test-only override of [`ledger_enabled`]. Unit tests run with the
    /// ledger ON by default — whatever another test in the same process did
    /// to the process-wide flag — and a test that needs it OFF forces it
    /// here for its own thread.
    static LEDGER_OVERRIDE: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
}

/// Test-only RAII guard from [`force_ledger`].
#[cfg(test)]
pub(crate) struct ForceLedger(Option<bool>);

#[cfg(test)]
impl Drop for ForceLedger {
    fn drop(&mut self) {
        LEDGER_OVERRIDE.with(|c| c.set(self.0));
    }
}

/// Test-only: force [`ledger_enabled`] on this thread until the guard drops.
#[cfg(test)]
#[must_use]
pub(crate) fn force_ledger(enabled: bool) -> ForceLedger {
    ForceLedger(LEDGER_OVERRIDE.with(|c| c.replace(Some(enabled))))
}

/// Whether dead slots are recorded (see the module doc).
#[inline]
fn ledger_enabled() -> bool {
    #[cfg(test)]
    {
        LEDGER_OVERRIDE.with(|c| c.get()).unwrap_or(true)
    }
    #[cfg(not(test))]
    {
        aof_consumer_present()
    }
}

/// Keys with a dead slot, by the file that holds the slot.
#[derive(Debug)]
pub struct DeadSlots {
    by_file: HashMap<u64, FileDead>,
    resident_bytes: usize,
    len: usize,
    /// Smallest TTL among the entries (`u64::MAX` = none carries one), so
    /// [`Self::prune_expired`] is O(1) when nothing can have expired.
    earliest_ttl: u64,
}

impl Default for DeadSlots {
    fn default() -> Self {
        Self {
            by_file: HashMap::new(),
            resident_bytes: 0,
            len: 0,
            earliest_ttl: u64::MAX,
        }
    }
}

impl DeadSlots {
    /// Record that `file_id` holds a slot of `key` (absolute TTL `ttl_ms`)
    /// that is no longer the key's cold-index entry. A no-op when no AOF fold
    /// can consume it (see the module doc).
    pub fn note(&mut self, file_id: u64, key: Bytes, ttl_ms: Option<u64>) {
        if !ledger_enabled() {
            return;
        }
        let cost = dead_slot_cost(key.len());
        let ttl_ms = ttl_ms.unwrap_or(0);
        if ttl_ms != 0 {
            self.earliest_ttl = self.earliest_ttl.min(ttl_ms);
        }
        self.resident_bytes += cost;
        self.len += 1;
        let file = self.by_file.entry(file_id).or_default();
        file.bytes += cost;
        file.slots.push(DeadSlot { key, ttl_ms });
    }

    /// `file_id` is gone from disk: its slots cannot come back.
    pub fn forget_file(&mut self, file_id: u64) {
        if let Some(file) = self.by_file.remove(&file_id) {
            self.len -= file.slots.len();
            self.resident_bytes = self.resident_bytes.saturating_sub(file.bytes);
        }
    }

    /// Drop every entry whose slot reads as expired at `now_ms` — it can no
    /// longer come back (see the module doc). Returns how many were dropped.
    /// O(1) unless some entry's TTL has actually passed.
    pub fn prune_expired(&mut self, now_ms: u64) -> usize {
        if self.earliest_ttl == u64::MAX || now_ms <= self.earliest_ttl {
            return 0;
        }
        let mut dropped = 0usize;
        let mut freed = 0usize;
        let mut earliest = u64::MAX;
        self.by_file.retain(|_, file| {
            file.slots.retain(|slot| {
                if slot.expired_at(now_ms) {
                    let cost = dead_slot_cost(slot.key.len());
                    dropped += 1;
                    freed += cost;
                    file.bytes = file.bytes.saturating_sub(cost);
                    false
                } else {
                    if slot.ttl_ms != 0 {
                        earliest = earliest.min(slot.ttl_ms);
                    }
                    true
                }
            });
            !file.slots.is_empty()
        });
        self.earliest_ttl = earliest;
        self.len -= dropped;
        self.resident_bytes = self.resident_bytes.saturating_sub(freed);
        dropped
    }

    /// Fold another ledger into this one (recovery merges per-db indexes).
    /// Entries move as they are: whether to record them was decided when
    /// they were recorded.
    pub fn merge(&mut self, mut other: DeadSlots) {
        for (file_id, theirs) in std::mem::take(&mut other.by_file) {
            let mine = self.by_file.entry(file_id).or_default();
            mine.bytes += theirs.bytes;
            mine.slots.extend(theirs.slots);
        }
        self.len += std::mem::take(&mut other.len);
        self.resident_bytes += std::mem::take(&mut other.resident_bytes);
        self.earliest_ttl = self.earliest_ttl.min(other.earliest_ttl);
    }

    /// Every key with at least one dead slot. A key dead in several files is
    /// yielded once per file; callers dedupe.
    pub fn keys(&self) -> impl Iterator<Item = &Bytes> {
        self.by_file
            .values()
            .flat_map(|f| f.slots.iter().map(|s| &s.key))
    }

    /// The keys whose dead slot can still come back at `now_ms` (its own TTL
    /// has not passed): what a fold cut at `now_ms` must consider.
    pub fn keys_live_at(&self, now_ms: u64) -> impl Iterator<Item = &Bytes> {
        self.by_file.values().flat_map(move |f| {
            f.slots
                .iter()
                .filter(move |s| !s.expired_at(now_ms))
                .map(|s| &s.key)
        })
    }

    /// Number of dead slots recorded.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Approximate RAM this ledger holds.
    #[inline]
    pub fn resident_bytes(&self) -> usize {
        self.resident_bytes
    }

    /// Whether `file_id` has any dead slot recorded (tests, diagnostics).
    pub fn file_has_dead_slots(&self, file_id: u64) -> bool {
        self.by_file
            .get(&file_id)
            .is_some_and(|f| !f.slots.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_files_entries_go_with_the_file_and_the_accounting_follows() {
        let mut d = DeadSlots::default();
        assert!(d.is_empty());
        d.note(1, Bytes::from_static(b"a"), None);
        d.note(1, Bytes::from_static(b"bb"), None);
        d.note(2, Bytes::from_static(b"a"), None);
        assert_eq!(d.len(), 3);
        let full = d.resident_bytes();
        assert_eq!(full, 3 * DEAD_SLOT_OVERHEAD + 4);
        let mut keys: Vec<&[u8]> = d.keys().map(|k| k.as_ref()).collect();
        keys.sort_unstable();
        assert_eq!(keys, vec![&b"a"[..], b"a", b"bb"]);

        d.forget_file(1);
        assert_eq!(d.len(), 1);
        assert!(!d.file_has_dead_slots(1));
        assert!(d.file_has_dead_slots(2));
        assert_eq!(d.resident_bytes(), DEAD_SLOT_OVERHEAD + 1);
        d.forget_file(9); // unknown file: no-op
        d.forget_file(2);
        assert!(d.is_empty());
        assert_eq!(d.resident_bytes(), 0);
    }

    #[test]
    fn merge_keeps_every_entry() {
        let mut a = DeadSlots::default();
        a.note(1, Bytes::from_static(b"x"), None);
        let mut b = DeadSlots::default();
        b.note(1, Bytes::from_static(b"y"), Some(50));
        b.note(3, Bytes::from_static(b"z"), None);
        a.merge(b);
        assert_eq!(a.len(), 3);
        assert!(a.file_has_dead_slots(3));
        assert_eq!(a.prune_expired(51), 1, "a merged entry's TTL is pruned too");
        assert_eq!(a.len(), 2);
    }

    /// Kept vs dropped: a slot whose own TTL has passed can never read as a
    /// value again, so it is pruned; one whose TTL is still ahead, or that
    /// has none, is kept. Nothing is scanned while nothing can have expired.
    #[test]
    fn prune_drops_exactly_the_slots_whose_own_ttl_has_passed() {
        let mut d = DeadSlots::default();
        d.note(1, Bytes::from_static(b"no-ttl"), None);
        d.note(1, Bytes::from_static(b"soon"), Some(100));
        d.note(2, Bytes::from_static(b"later"), Some(500));
        assert_eq!(d.prune_expired(100), 0, "`now == ttl` is not expired yet");
        assert_eq!(d.prune_expired(101), 1);
        let mut keys: Vec<&[u8]> = d.keys().map(|k| k.as_ref()).collect();
        keys.sort_unstable();
        assert_eq!(keys, vec![&b"later"[..], b"no-ttl"]);
        assert_eq!(
            d.resident_bytes(),
            2 * DEAD_SLOT_OVERHEAD + b"later".len() + b"no-ttl".len()
        );
        assert_eq!(d.prune_expired(10_000), 1);
        assert!(!d.file_has_dead_slots(2), "an emptied file leaves the map");
        assert_eq!(d.len(), 1);
        let live: Vec<&[u8]> = d.keys_live_at(u64::MAX).map(|k| k.as_ref()).collect();
        assert_eq!(live, vec![&b"no-ttl"[..]]);
    }

    /// The fold's view skips an expired slot even before a prune ran.
    #[test]
    fn keys_live_at_skips_slots_expired_at_that_instant() {
        let mut d = DeadSlots::default();
        d.note(1, Bytes::from_static(b"gone"), Some(10));
        d.note(1, Bytes::from_static(b"kept"), Some(20));
        let at = |now| {
            let mut v: Vec<&[u8]> = d.keys_live_at(now).map(|k| k.as_ref()).collect();
            v.sort_unstable();
            v
        };
        assert_eq!(at(10), vec![&b"gone"[..], b"kept"]);
        assert_eq!(at(11), vec![&b"kept"[..]]);
        assert!(at(21).is_empty());
    }

    /// Dropped: with no AOF fold to consume them, nothing is recorded.
    #[test]
    fn nothing_is_recorded_without_an_aof_consumer() {
        let _off = force_ledger(false);
        let mut d = DeadSlots::default();
        d.note(1, Bytes::from_static(b"k"), None);
        assert!(d.is_empty());
        assert_eq!(d.resident_bytes(), 0);
    }
}
