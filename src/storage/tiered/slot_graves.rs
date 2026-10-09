//! The no-AOF dead-slot record (moon#1281): the location of every spill-file
//! slot that is still on disk in a listed file but is no longer its key's
//! cold-index entry.
//!
//! It is the no-AOF twin of [`super::dead_slots`]. That ledger keeps each
//! dead slot's KEY so an AOF rewrite fold can write a `DEL` for it; without
//! an AOF there is no fold, and the durable authority is the snapshot, which
//! needs only WHERE the slot is: at every snapshot start the shard encodes
//! this record into the snapshot's cold-graves trailer
//! (`persistence::snapshot::cold_graves`), and the boot drops exactly those
//! slots before it rebuilds the index. 8 bytes per slot in RAM, no key copy.
//!
//! Like the ledger it lives inside [`super::cold_index::ColdIndex`], so every
//! path that takes a slot out of the index records it (the same call sites),
//! and it forgets a file's slots when the file is unlinked. A file holds at
//! most one spill batch (`FLUSH_ENTRY_CAP` = 256 entries), so the record is
//! bounded by the dead slots actually on disk.
//!
//! Recorded only by a process with no AOF writer
//! ([`super::snapshot_hold::applies`]): with an AOF the log and its folds are
//! the authority and the key-carrying ledger does this job.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::persistence::snapshot::cold_graves::pack_slot;

#[cfg(test)]
thread_local! {
    /// Test-only override of [`enabled`] for the current thread.
    static OVERRIDE: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
}

/// Test-only: force [`enabled`] on this thread.
#[cfg(test)]
pub(crate) fn force_enabled(on: Option<bool>) {
    OVERRIDE.with(|c| c.set(on));
}

/// Whether dead slots are recorded here: no AOF writer in this process.
#[inline]
fn enabled() -> bool {
    #[cfg(test)]
    {
        OVERRIDE
            .with(std::cell::Cell::get)
            .unwrap_or_else(|| !super::dead_slots::aof_consumer_present())
    }
    #[cfg(not(test))]
    {
        !super::dead_slots::aof_consumer_present()
    }
}

/// Grave slots held by every record in the process (`INFO` Memory
/// `cold_grave_slots`; review F7). One relaxed add or sub per change.
static GRAVE_SLOTS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// `(grave slots, approximate bytes)` over every record in the process.
#[must_use]
pub fn totals() -> (u64, u64) {
    let n = GRAVE_SLOTS_TOTAL.load(Ordering::Relaxed);
    (n, n * std::mem::size_of::<u64>() as u64)
}

/// Dead slot locations, by file.
#[derive(Debug, Default)]
pub struct SlotGraves {
    by_file: HashMap<u64, Vec<u64>>,
    len: usize,
    /// Bumped by every change to the record; `len` alone can go down and
    /// back up to the same value between two looks ([`Self::generation`]).
    generation: u64,
}

impl Drop for SlotGraves {
    /// A dropped record (its index replaced or freed) leaves the totals.
    fn drop(&mut self) {
        if self.len != 0 {
            GRAVE_SLOTS_TOTAL.fetch_sub(self.len as u64, Ordering::Relaxed);
        }
    }
}

impl SlotGraves {
    /// Record that `(file_id, page_idx, slot_idx)` is dead. A no-op with an
    /// AOF writer in the process.
    #[inline]
    pub fn note(&mut self, file_id: u64, page_idx: u32, slot_idx: u16) {
        if !enabled() {
            return;
        }
        self.by_file
            .entry(file_id)
            .or_default()
            .push(pack_slot(page_idx, slot_idx));
        self.len += 1;
        self.generation = self.generation.wrapping_add(1);
        GRAVE_SLOTS_TOTAL.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a slot that is already known dead (the boot re-seeding what the
    /// snapshot it loaded carried), whatever the process mode.
    pub(crate) fn note_unconditionally(&mut self, file_id: u64, packed: u64) {
        self.by_file.entry(file_id).or_default().push(packed);
        self.len += 1;
        self.generation = self.generation.wrapping_add(1);
        GRAVE_SLOTS_TOTAL.fetch_add(1, Ordering::Relaxed);
    }

    /// `file_id` is gone from disk: its slots cannot come back.
    pub fn forget_file(&mut self, file_id: u64) {
        if let Some(v) = self.by_file.remove(&file_id) {
            self.len -= v.len();
            self.generation = self.generation.wrapping_add(1);
            GRAVE_SLOTS_TOTAL.fetch_sub(v.len() as u64, Ordering::Relaxed);
        }
    }

    /// Fold another record into this one (recovery merges per-db indexes).
    pub fn merge(&mut self, mut other: SlotGraves) {
        for (file_id, slots) in std::mem::take(&mut other.by_file) {
            self.by_file.entry(file_id).or_default().extend(slots);
        }
        let moved = std::mem::take(&mut other.len);
        if moved != 0 {
            self.len += moved;
            self.generation = self.generation.wrapping_add(1);
        }
    }

    /// Take the slots of every file `moves` selects into a new record (a
    /// replayed SWAPDB, like [`super::dead_slots::DeadSlots::split_off_files`]).
    pub fn split_off_files(&mut self, moves: impl Fn(u64) -> bool) -> SlotGraves {
        let mut out = SlotGraves::default();
        let files: Vec<u64> = self.by_file.keys().copied().filter(|f| moves(*f)).collect();
        for file_id in files {
            if let Some(v) = self.by_file.remove(&file_id) {
                self.len -= v.len();
                self.generation = self.generation.wrapping_add(1);
                out.len += v.len();
                out.by_file.insert(file_id, v);
            }
        }
        out
    }

    /// Number of dead slots recorded (a slot recorded twice counts twice).
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// A value that changes whenever the record does (a slot noted, a file
    /// forgotten, a merge or split). Equal generations mean an unchanged
    /// record; equal [`Self::len`]s do not.
    #[inline]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// `(file_id, dead slots recorded)` for every file with at least one:
    /// the no-AOF reclaim's input (moon#1297). O(files with a grave).
    pub fn files(&self) -> impl Iterator<Item = (u64, usize)> + '_ {
        self.by_file.iter().map(|(&f, s)| (f, s.len()))
    }

    /// The dead slots recorded for `file_id` (packed; tests).
    #[cfg(test)]
    pub(crate) fn slots_of(&self, file_id: u64) -> &[u64] {
        self.by_file.get(&file_id).map_or(&[], Vec::as_slice)
    }

    /// Append `(file_id, packed slots)` for every file into `out` (tests
    /// and tools; the snapshot uses [`Self::encode_into`]).
    pub fn collect_into(&self, out: &mut Vec<(u64, Vec<u64>)>) {
        out.extend(self.by_file.iter().map(|(&f, s)| (f, s.clone())));
    }

    /// Write every file's slots into the trailer being built: what a snapshot
    /// starting now carries. O(slots), no intermediate copy (review F6).
    pub fn encode_into(&self, enc: &mut crate::persistence::snapshot::cold_graves::Encoder) {
        for (&file_id, slots) in &self.by_file {
            enc.add_file(file_id, slots);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::snapshot::cold_graves;

    #[test]
    fn records_forgets_and_round_trips_through_the_trailer() {
        force_enabled(Some(true));
        let mut g = SlotGraves::default();
        g.note(1, 0, 3);
        g.note(1, 2, 7);
        g.note(4, 0, 0);
        assert_eq!(g.len(), 3);
        let mut files = Vec::new();
        g.collect_into(&mut files);
        let decoded = cold_graves::decode(&cold_graves::encode(&files)).expect("decode");
        assert!(
            decoded.contains(1, 0, 3) && decoded.contains(1, 2, 7) && decoded.contains(4, 0, 0)
        );
        assert!(!decoded.contains(1, 0, 4));
        g.forget_file(1);
        assert_eq!(g.len(), 1);
        let moved = g.split_off_files(|f| f == 4);
        assert!(g.is_empty());
        assert_eq!(moved.len(), 1);
        let mut back = SlotGraves::default();
        back.merge(moved);
        assert_eq!(back.len(), 1);
        force_enabled(None);
    }

    #[test]
    fn nothing_is_recorded_when_an_aof_is_the_authority() {
        force_enabled(Some(false));
        let mut g = SlotGraves::default();
        g.note(1, 0, 0);
        assert!(g.is_empty());
        g.note_unconditionally(1, 0);
        assert_eq!(g.len(), 1, "the boot re-seed records whatever the mode");
        force_enabled(None);
    }
}
