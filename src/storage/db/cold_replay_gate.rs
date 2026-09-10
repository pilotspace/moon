//! Replay-time cut between the two KV durability planes (moon#902).
//!
//! Under `--appendonly yes --disk-offload enable` a key can be durable in
//! BOTH planes at once: the AOF holds the command that produced it, and the
//! shard manifest holds the spilled copy of the value it produced. Recovery
//! rebuilds the cold index from the manifest first and then replays the AOF
//! on top, and every `get_or_create_*` accessor the replayed write goes
//! through promotes the cold copy back into RAM before mutating it. A
//! non-idempotent write (RPUSH, APPEND, INCRBY, HINCRBY, ZINCRBY, BITFIELD
//! INCRBY, …) therefore lands on the value it already produced — every
//! element doubles, and it compounds by one per unclean restart because the
//! replay-driven eviction re-spills the doubled value.
//!
//! The cut this module implements is **per cold file, in-band in the AOF**:
//!
//! * `MOON.COLDCUT <w>` opens every AOF generation (written by `initialize*`
//!   and by every rewrite, as the first record of the new incr). It says:
//!   cold files with `file_id < w` were sealed BEFORE this generation's base
//!   was cut, so every record in the generation post-dates them and they are
//!   a valid base for those records (the base RDB is hot-only, so the cold
//!   copy is the ONLY copy of such a key — hiding it would lose data in the
//!   other direction).
//! * `MOON.SPILLED <file_id> key…` is appended to the AOF the moment a spill
//!   completion publishes its keys into the cold index. On replay it says:
//!   from here on, the cold copy in `file_id` IS these keys' state — drop the
//!   hot copy the preceding records rebuilt (restart-as-cold, exactly where
//!   task #56 wanted it), and let later records hydrate from it.
//!
//! While the gate is installed, a cold entry is INVISIBLE to the value-giving
//! read paths (`cold_contains_alive`, `get_cold_value`, `cold_lookup_location`)
//! unless its file is authorized by one of the two records above. Tombstoning
//! paths (`remove_counting_cold`, `clear`) never consult visibility, so a
//! replayed DEL/FLUSH still reaches the cold plane (moon#257).
//!
//! At the end of replay [`Database::finish_replay_cold_reconcile`] runs: a key
//! that is hot AND still cold at that point was rebuilt by the AOF alone
//! (its marker was lost in the crash tail, or its cold entry is a stale file
//! the manifest still lists) — the hot copy is the complete history, so the
//! COLD entry is dropped. A generation with no `MOON.COLDCUT` (written before
//! this fix) installs no gate and keeps the pre-existing behaviour verbatim.

use std::collections::HashSet;

use bytes::Bytes;

use super::Database;
use crate::storage::tiered::cold_index::ColdLocation;

/// Which cold files an AOF-authority replay may read values from.
#[derive(Debug, Clone, Default)]
pub struct ReplayColdGate {
    /// Files with `file_id < pre_generation_below` were sealed before this
    /// generation's base cut (`MOON.COLDCUT`).
    pre_generation_below: u64,
    /// Files whose `MOON.SPILLED` marker has been replayed in this generation.
    authorized: HashSet<u64>,
}

impl ReplayColdGate {
    /// Whether a value stored in `file_id` may be read during replay.
    #[inline]
    pub fn is_authorized(&self, file_id: u64) -> bool {
        file_id < self.pre_generation_below || self.authorized.contains(&file_id)
    }

    /// The `MOON.COLDCUT` watermark this gate was installed with.
    #[inline]
    pub fn pre_generation_below(&self) -> u64 {
        self.pre_generation_below
    }
}

/// What [`Database::finish_replay_cold_reconcile`] did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReplayColdReconcile {
    /// `true` when a gate was installed (a `MOON.COLDCUT`-opened generation):
    /// hot∩cold shadows resolved hot-wins by dropping the cold entry.
    pub gated: bool,
    /// Hot copies dropped in favour of the cold entry (legacy generation only,
    /// the task #56 `demote_replayed_cold_shadows` pass).
    pub hot_demoted: usize,
    /// Cold entries dropped in favour of the hot copy (gated generation only).
    pub cold_dropped: usize,
}

impl Database {
    /// Open an AOF-authority replay generation: cold files with
    /// `file_id < pre_generation_below` are visible, everything else waits for
    /// its `MOON.SPILLED` marker. Idempotent per generation (a second
    /// `MOON.COLDCUT` in the same log, e.g. a legacy-upgrade capture, keeps the
    /// wider of the two watermarks and every authorization already granted).
    pub fn install_replay_cold_gate(&mut self, pre_generation_below: u64) {
        match self.replay_cold_gate.as_mut() {
            Some(gate) => {
                gate.pre_generation_below = gate.pre_generation_below.max(pre_generation_below);
            }
            None => {
                self.replay_cold_gate = Some(ReplayColdGate {
                    pre_generation_below,
                    authorized: HashSet::new(),
                });
            }
        }
    }

    /// Whether an AOF-authority replay gate is currently installed.
    #[inline]
    pub fn replay_cold_gate_active(&self) -> bool {
        self.replay_cold_gate.is_some()
    }

    /// The gate, for tests and diagnostics.
    #[inline]
    pub fn replay_cold_gate(&self) -> Option<&ReplayColdGate> {
        self.replay_cold_gate.as_ref()
    }

    /// The cold-index location of `key` if — and only if — a value may be
    /// read from it right now. Outside replay this is a plain lookup; while
    /// a gate is installed it also requires the entry's file to be authorized.
    ///
    /// This is the ONE choke point for every value-giving cold read
    /// (`cold_contains_alive`, `get_cold_value`, `cold_lookup_location`).
    /// Tombstoning paths deliberately bypass it.
    #[inline]
    pub(super) fn cold_location_visible(&self, key: &[u8]) -> Option<ColdLocation> {
        let location = self.cold_index.as_ref()?.lookup(key)?;
        match self.replay_cold_gate.as_ref() {
            Some(gate) if !gate.is_authorized(location.file_id) => None,
            _ => Some(location),
        }
    }

    /// Replay a `MOON.SPILLED <file_id> key…` record: every listed key whose
    /// CURRENT cold entry lives in `file_id` drops its hot copy (the cold copy
    /// is its state from this point of the log on), and `file_id` becomes
    /// readable for the rest of the replay. A key whose cold entry points at a
    /// LATER file is left alone — that later file's own marker will cut it.
    ///
    /// Returns how many hot copies were dropped. Safe to call without a gate
    /// (legacy generation): the drop is still exact, and the end-of-replay
    /// demote pass would have made the same decision less precisely.
    pub fn replay_cold_spilled<'k>(
        &mut self,
        file_id: u64,
        keys: impl IntoIterator<Item = &'k [u8]>,
    ) -> usize {
        let mut dropped = 0usize;
        for key in keys {
            let current = self.cold_index.as_ref().and_then(|ci| ci.lookup(key));
            if current.is_some_and(|loc| loc.file_id == file_id) && self.remove_hot(key).is_some() {
                dropped += 1;
            }
        }
        if let Some(gate) = self.replay_cold_gate.as_mut() {
            gate.authorized.insert(file_id);
        }
        dropped
    }

    /// Close the replay generation and resolve every key present in both
    /// planes. Call ONCE per shard after AOF replay finishes and before the
    /// server accepts connections.
    ///
    /// * Gated generation: a key hot AND cold now was rebuilt by the AOF
    ///   without its marker cutting it (marker lost in the crash tail, or a
    ///   stale cold entry from a file the manifest still lists — e.g. a key
    ///   written while its spill was in flight, whose publish was withdrawn
    ///   in RAM but whose file stayed Active). The hot copy is the complete
    ///   history; the cold entry is dropped (its file is reclaimed by the
    ///   orphan sweep once no key references it).
    /// * Legacy generation (no `MOON.COLDCUT` seen): the task #56 behaviour,
    ///   [`Self::demote_replayed_cold_shadows`], unchanged.
    pub fn finish_replay_cold_reconcile(&mut self) -> ReplayColdReconcile {
        let Some(_gate) = self.replay_cold_gate.take() else {
            return ReplayColdReconcile {
                gated: false,
                hot_demoted: self.demote_replayed_cold_shadows(),
                cold_dropped: 0,
            };
        };
        let Some(ci) = self.cold_index.as_ref() else {
            return ReplayColdReconcile {
                gated: true,
                ..Default::default()
            };
        };
        let shadowed: Vec<Bytes> = ci
            .iter()
            .filter(|(key, _)| self.is_hot(key))
            .map(|(key, _)| key.clone())
            .collect();
        let mut cold_dropped = 0usize;
        if let Some(ci) = self.cold_index.as_mut() {
            for key in &shadowed {
                if ci.remove(key) {
                    cold_dropped += 1;
                }
            }
        }
        ReplayColdReconcile {
            gated: true,
            hot_demoted: 0,
            cold_dropped,
        }
    }

    /// A `MOON.COLDCUT` watermark computed from this database alone: one past
    /// the highest cold file any live cold entry or in-flight spill refers to.
    /// The shard's own `spill_file_id` counter is exact and preferred; this is
    /// for the rewrite path that only holds database guards. Never below 1
    /// (file ids start at 1, so `1` authorizes nothing).
    pub fn cold_file_watermark_hint(&self) -> u64 {
        let indexed = self.cold_index.as_ref().and_then(|ci| ci.max_file_id());
        let inflight = self.spill_inflight.values().map(|p| p.req_id).max();
        indexed
            .into_iter()
            .chain(inflight)
            .max()
            .map_or(1, |max| max.saturating_add(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::kv_page::ValueType;
    use crate::storage::entry::Entry;
    use crate::storage::tiered::cold_index::ColdIndex;

    fn loc(file_id: u64) -> ColdLocation {
        ColdLocation {
            file_id,
            page_idx: 0,
            slot_idx: 0,
            ttl_ms: None,
            value_type: ValueType::String,
        }
    }

    fn db_with_cold(entries: &[(&'static [u8], u64)]) -> Database {
        let mut db = Database::new();
        let mut ci = ColdIndex::new();
        for (key, file_id) in entries {
            ci.insert(Bytes::from_static(key), loc(*file_id));
        }
        db.cold_index = Some(ci);
        db
    }

    #[test]
    fn ungated_lookup_sees_every_cold_entry() {
        let db = db_with_cold(&[(b"k", 7)]);
        assert!(db.cold_location_visible(b"k").is_some());
        assert!(!db.replay_cold_gate_active());
    }

    #[test]
    fn gate_hides_files_at_or_past_the_watermark_until_their_marker() {
        let mut db = db_with_cold(&[(b"old", 3), (b"new", 5)]);
        db.install_replay_cold_gate(4);
        assert!(
            db.cold_location_visible(b"old").is_some(),
            "file 3 < 4 is pre-generation"
        );
        assert!(
            db.cold_location_visible(b"new").is_none(),
            "file 5 waits for MOON.SPILLED 5"
        );
        db.replay_cold_spilled(5, [b"new".as_slice()]);
        assert!(db.cold_location_visible(b"new").is_some());
        // `cold_contains_alive` shares the choke point.
        assert!(db.cold_contains_alive(b"new", 0));
    }

    #[test]
    fn gate_is_invisible_to_tombstones() {
        let mut db = db_with_cold(&[(b"k", 9)]);
        db.install_replay_cold_gate(1);
        assert!(db.cold_location_visible(b"k").is_none());
        // A replayed DEL must still reach the cold plane (moon#257).
        assert!(db.remove_counting_cold(b"k").0);
        assert!(db.cold_index.as_ref().unwrap().lookup(b"k").is_none());
    }

    #[test]
    fn marker_drops_hot_copy_only_when_the_cold_entry_is_in_that_file() {
        let mut db = db_with_cold(&[(b"a", 2), (b"b", 3)]);
        db.install_replay_cold_gate(1);
        db.set(b"a", Entry::new_string(Bytes::from_static(b"va")));
        db.set(b"b", Entry::new_string(Bytes::from_static(b"vb")));
        db.set(b"c", Entry::new_string(Bytes::from_static(b"vc")));
        // Marker for file 2 lists a (matches), b (cold copy is in file 3 —
        // a LATER spill; leave it), and c (not cold at all).
        let dropped =
            db.replay_cold_spilled(2, [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]);
        assert_eq!(dropped, 1);
        assert!(!db.is_hot(b"a"));
        assert!(db.is_hot(b"b"));
        assert!(db.is_hot(b"c"));
        assert!(db.replay_cold_gate().unwrap().is_authorized(2));
        assert!(!db.replay_cold_gate().unwrap().is_authorized(3));
    }

    #[test]
    fn marker_without_gate_still_drops_exactly() {
        let mut db = db_with_cold(&[(b"a", 2)]);
        db.set(b"a", Entry::new_string(Bytes::from_static(b"va")));
        assert_eq!(db.replay_cold_spilled(2, [b"a".as_slice()]), 1);
        assert!(!db.is_hot(b"a"));
        assert!(db.cold_index.as_ref().unwrap().lookup(b"a").is_some());
    }

    #[test]
    fn gated_finish_is_hot_wins_and_closes_the_gate() {
        let mut db = db_with_cold(&[(b"shadow", 2), (b"cold_only", 2)]);
        db.install_replay_cold_gate(1);
        db.set(b"shadow", Entry::new_string(Bytes::from_static(b"v2")));
        let outcome = db.finish_replay_cold_reconcile();
        assert_eq!(
            outcome,
            ReplayColdReconcile {
                gated: true,
                hot_demoted: 0,
                cold_dropped: 1
            }
        );
        assert!(db.is_hot(b"shadow"), "hot copy is the complete history");
        assert!(db.cold_index.as_ref().unwrap().lookup(b"shadow").is_none());
        assert!(
            db.cold_index
                .as_ref()
                .unwrap()
                .lookup(b"cold_only")
                .is_some()
        );
        assert!(!db.replay_cold_gate_active());
        assert!(
            db.cold_location_visible(b"cold_only").is_some(),
            "gate closed: everything visible"
        );
    }

    #[test]
    fn legacy_finish_is_the_task_56_demote() {
        let mut db = db_with_cold(&[(b"shadow", 2)]);
        db.set(b"shadow", Entry::new_string(Bytes::from_static(b"v1")));
        let outcome = db.finish_replay_cold_reconcile();
        assert_eq!(
            outcome,
            ReplayColdReconcile {
                gated: false,
                hot_demoted: 1,
                cold_dropped: 0
            }
        );
        assert!(!db.is_hot(b"shadow"));
        assert!(db.cold_index.as_ref().unwrap().lookup(b"shadow").is_some());
    }

    #[test]
    fn second_coldcut_widens_never_narrows() {
        let mut db = db_with_cold(&[(b"k", 5)]);
        db.install_replay_cold_gate(6);
        db.install_replay_cold_gate(2);
        assert!(db.cold_location_visible(b"k").is_some());
        assert_eq!(db.replay_cold_gate().unwrap().pre_generation_below(), 6);
    }

    #[test]
    fn watermark_hint_is_one_past_the_highest_live_file() {
        assert_eq!(Database::new().cold_file_watermark_hint(), 1);
        let db = db_with_cold(&[(b"a", 4), (b"b", 9)]);
        assert_eq!(db.cold_file_watermark_hint(), 10);
    }
}
