//! What a database's cold tier ties to its logical db index, for SWAPDB
//! (moon#1237).
//!
//! A spill file's manifest entry records the logical db it was spilled from
//! (`FileEntry::db_index`, the database's slot index at eviction), and a
//! restart rebuilds each file's slots into THAT db. `SWAPDB a b` swaps whole
//! `Database` structures — cold index, dead-slot ledger and in-flight spills
//! included — but not the manifest tags. Two consequences, both handled from
//! here:
//!
//! - **Live** ([`ColdIndex::has_footprint`]): a SWAPDB is refused while either
//!   database owns anything a restart would rebuild under its old index —
//!   live cold entries, dead slots still on disk, zero-ref files not yet
//!   unlinked or held, compactions in flight. Re-tagging the files instead
//!   cannot be made crash-consistent with the AOF's own `SWAPDB` record
//!   without a new record or manifest field (see the WS20 NOTES).
//! - **Replay** ([`ColdIndex::split_off_files`]): recovery attaches every
//!   file to its tag BEFORE the log replays, so a replayed `SWAPDB` must move
//!   only the cold entries of files that existed when the swap happened —
//!   files below the generation's `MOON.COLDCUT` or whose `MOON.SPILLED`
//!   marker has already replayed. A file spilled after the swap is tagged
//!   with its post-swap db and must stay where it is. Moving everything (the
//!   old behaviour) carried every later spill of the swapped db into the
//!   other db at the next restart.

use std::collections::HashMap;

use bytes::Bytes;

use super::cold_index::{ColdIndex, ColdLocation, cold_entry_cost, scan_h48};

impl ColdIndex {
    /// Whether anything in this index ties the database to on-disk spill
    /// slots that a restart would rebuild under its logical db index: a live
    /// entry, a rebuild's older copy, a dead slot still on disk, a zero-ref
    /// file queued or held for unlink, or a compaction in progress.
    #[must_use]
    pub fn has_footprint(&self) -> bool {
        !self.map.is_empty()
            || !self.file_refs.is_empty()
            || !self.older_copies.is_empty()
            || !self.pending_unlink.is_empty()
            || !self.dead.is_empty()
            || !self.hold.is_empty()
            || !self.reclaim.is_idle()
    }

    /// Move everything that belongs to the files `moves` selects — entries,
    /// older copies, dead slots, queued unlinks — into a new index, with the
    /// file references and byte accounting moved along (a move is not a
    /// zero-ref event: nothing is queued for unlink by it).
    ///
    /// A key whose current entry stays but that has older copies in moved
    /// files gets its newest moved copy as its entry in the new index (and
    /// vice versa): each side must be able to read its own newest copy.
    /// Held files and compaction state stay (both are empty during replay,
    /// the only caller).
    pub fn split_off_files(&mut self, moves: impl Fn(u64) -> bool) -> ColdIndex {
        let mut out = ColdIndex::new();
        let moved_keys: Vec<(u64, Bytes)> = self
            .map
            .iter()
            .filter(|(_, loc)| moves(loc.file_id))
            .map(|(k, _)| k.clone())
            .collect();
        for k in moved_keys {
            if let Some(loc) = self.map.remove(&k) {
                let cost = cold_entry_cost(k.1.len());
                self.resident_bytes = self.resident_bytes.saturating_sub(cost);
                ref_move(&mut self.file_refs, &mut out.file_refs, loc.file_id);
                out.resident_bytes += cost;
                out.map.insert(k, loc);
            }
        }
        let mut older_moved: HashMap<Bytes, Vec<ColdLocation>> = HashMap::new();
        for (key, copies) in self.older_copies.iter_mut() {
            let (go, stay): (Vec<ColdLocation>, Vec<ColdLocation>) =
                copies.iter().partition(|loc| moves(loc.file_id));
            if !go.is_empty() {
                for loc in &go {
                    ref_move(&mut self.file_refs, &mut out.file_refs, loc.file_id);
                }
                *copies = stay;
                older_moved.insert(key.clone(), go);
            }
        }
        self.older_copies.retain(|_, copies| !copies.is_empty());
        out.older_copies = older_moved;
        self.promote_orphaned_older_copies();
        out.promote_orphaned_older_copies();
        out.dead = self.dead.split_off_files(&moves);
        let (go, stay): (Vec<u64>, Vec<u64>) = self.pending_unlink.iter().partition(|f| moves(**f));
        self.pending_unlink = stay;
        out.pending_unlink = go;
        let (go, stay): (Vec<u64>, Vec<u64>) =
            self.missing_at_rebuild.iter().partition(|f| moves(**f));
        self.missing_at_rebuild = stay;
        out.missing_at_rebuild = go;
        out
    }

    /// Merge `newer` — the cold entries a [`Self::split_off_files`] kept back
    /// for this slot — into this index. Unlike [`Self::merge`] (disjoint
    /// per-db rebuilds), a key may be in both: it keeps ONE entry, its newest
    /// copy by [`ColdLocation::recency_key`], with every other copy behind it
    /// as an older copy (newest first, moon#1140), each keeping its file
    /// reference — nothing becomes a dead slot or a zero-ref file here.
    pub fn merge_newer(&mut self, newer: ColdIndex) {
        let ColdIndex {
            map,
            older_copies,
            dead,
            pending_unlink,
            missing_at_rebuild,
            hold,
            ..
        } = newer;
        for ((h, key), loc) in map {
            *self.file_refs.entry(loc.file_id).or_insert(0) += 1;
            match self.map.remove(&(h, key.clone())) {
                None => {
                    self.resident_bytes += cold_entry_cost(key.len());
                    self.map.insert((h, key), loc);
                }
                Some(prev) => {
                    let mut all = vec![loc, prev];
                    all.extend(self.older_copies.remove(&key).unwrap_or_default());
                    all.sort_by_key(|l| std::cmp::Reverse(l.recency_key()));
                    let newest = all.remove(0);
                    self.map.insert((h, key.clone()), newest);
                    if !all.is_empty() {
                        self.older_copies.insert(key, all);
                    }
                }
            }
        }
        for (key, copies) in older_copies {
            for loc in &copies {
                *self.file_refs.entry(loc.file_id).or_insert(0) += 1;
            }
            let mine = self.older_copies.entry(key).or_default();
            mine.extend(copies);
            mine.sort_by_key(|l| std::cmp::Reverse(l.recency_key()));
        }
        self.dead.merge(dead);
        self.hold.merge(hold);
        self.pending_unlink.extend(pending_unlink);
        self.missing_at_rebuild.extend(missing_at_rebuild);
    }

    /// A key left with older copies but no entry (its entry went to the other
    /// side of a split): the newest remaining copy becomes its entry. Copies
    /// are newest first; the reference each holds carries over to the entry.
    fn promote_orphaned_older_copies(&mut self) {
        let orphaned: Vec<Bytes> = self
            .older_copies
            .keys()
            .filter(|key| self.lookup(key).is_none())
            .cloned()
            .collect();
        for key in orphaned {
            let Some(mut copies) = self.older_copies.remove(&key) else {
                continue;
            };
            if copies.is_empty() {
                continue;
            }
            let newest = copies.remove(0);
            self.resident_bytes += cold_entry_cost(key.len());
            self.map.insert((scan_h48(&key), key.clone()), newest);
            if !copies.is_empty() {
                self.older_copies.insert(key, copies);
            }
        }
    }
}

/// Move one live reference to `file_id` from `from` to `to`.
fn ref_move(from: &mut HashMap<u64, u32>, to: &mut HashMap<u64, u32>, file_id: u64) {
    if let Some(c) = from.get_mut(&file_id) {
        *c = c.saturating_sub(1);
        if *c == 0 {
            from.remove(&file_id);
        }
    }
    *to.entry(file_id).or_insert(0) += 1;
}
