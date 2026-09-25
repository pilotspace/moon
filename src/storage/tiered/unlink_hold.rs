//! When a spill file with no live key left may be unlinked (moon#1231).
//!
//! A file loses its last live key by any path — a read promotion, a
//! read-modify-write promotion, DEL, an overwrite, expiry, FLUSH, the release
//! of a rebuild's older copies. Unlinking it then is safe only if no AOF
//! generation that can still be replayed reads it.
//!
//! A file below a fold's cold cut is exactly such a source. A key that was
//! cold at the fold instant is not in that fold's base: the file is its only
//! copy for the whole generation, and any replayed record that reads the key
//! before overwriting it reads the file — the records after a promotion (an
//! `APPEND` replays onto nothing without it), a non-promoting cold read logged
//! as a write (`SUNIONSTORE dst k`, `COPY k dst`), or the gated replay's
//! older-copy fallback (moon#1140). The orphan sweep used to unlink such a file
//! as soon as its last key left, and a restart then lost or corrupted keys that
//! were acknowledged and never deleted.
//!
//! # The rule
//!
//! The sweep hands the index a fresh [`FoldView`] right before each unlink
//! decision (same shard thread, no await in between):
//!
//! - A file at or above [`UnlinkHold::hold_below`] was minted after every cut
//!   any committed or in-progress generation has, so no generation reads it
//!   below its cut: it is unlinked as before.
//! - A file below it is **held**, stamped with the view's fold epoch, until a
//!   fold whose snapshot is later than that stamp has COMMITTED
//!   (`stamp < committed_floor`). That fold's base holds every key the file
//!   backed — hot, deleted by the dead-slot ledger's head `DEL`s (moon#1215), or
//!   cold in another listed file — so nothing reads the file any more.
//!
//! `hold_below` is raised to the shard's spill-file counter whenever the
//! view's epoch differs from the last one seen: a fold cut happened in between,
//! and the cut was read from the same counter, which only grows. The first view
//! sets it too, which covers the generation recovered at boot (every file that
//! existed then is below the counter). The stamp is taken when the drain first
//! sees the file, never earlier than the zero-ref itself, so it can only err
//! towards holding longer.
//!
//! Without an AOF writer there is no view: no fold exists to protect, and no
//! committed fold could ever release a hold, so files are unlinked as before.
//! An index that has seen a view refuses to decide without a fresh one — a
//! stale view could understate `hold_below`, which is the unsafe direction.

/// A reading of the shard's AOF fold state, taken right before an unlink
/// decision on the shard thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FoldView {
    /// The writer's current fold epoch (`RewriteOverflow::stamp`).
    pub epoch: u64,
    /// The snapshot epoch of the latest committed fold
    /// (`RewriteOverflow::committed_floor`).
    pub committed_floor: u64,
    /// The shard's spill-file counter: every file id minted so far is below it.
    pub next_file_id: u64,
}

/// Per-database hold state, kept inside `ColdIndex`.
#[derive(Debug, Default)]
pub struct UnlinkHold {
    /// Set by the first [`FoldView`]; never cleared.
    active: bool,
    seen_epoch: u64,
    hold_below: u64,
    /// `(file_id, stamp)`: zero-ref files a replayable generation may read.
    held: Vec<(u64, u64)>,
    /// The view for the next decision, consumed by it.
    fresh: Option<FoldView>,
}

/// What [`UnlinkHold::admit`] decided for a batch of zero-ref files.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Admitted {
    /// Unlink these now.
    pub unlink: Vec<u64>,
    /// No fresh view: leave these queued for the next decision.
    pub requeue: Vec<u64>,
}

impl UnlinkHold {
    /// Record `view` for the next decision (see the module doc).
    pub fn observe(&mut self, view: FoldView) {
        if !self.active || self.seen_epoch != view.epoch {
            self.hold_below = self.hold_below.max(view.next_file_id);
            self.seen_epoch = view.epoch;
        }
        self.active = true;
        self.fresh = Some(view);
    }

    /// Drop the fresh view without deciding anything (a sweep that had
    /// nothing to unlink): no later decision may use it.
    pub fn end_decision(&mut self) {
        self.fresh = None;
    }

    /// Decide for the zero-ref files in `queued`, and release every held file
    /// a committed fold now covers. Consumes the fresh view.
    pub fn admit(&mut self, queued: Vec<u64>) -> Admitted {
        if !self.active {
            return Admitted {
                unlink: queued,
                requeue: Vec::new(),
            };
        }
        let Some(view) = self.fresh.take() else {
            return Admitted {
                unlink: Vec::new(),
                requeue: queued,
            };
        };
        let mut unlink = Vec::new();
        for file_id in queued {
            if file_id >= self.hold_below {
                unlink.push(file_id);
            } else if !self.held.iter().any(|&(f, _)| f == file_id) {
                self.held.push((file_id, view.epoch));
            }
        }
        self.held.retain(|&(file_id, stamp)| {
            let covered = stamp < view.committed_floor;
            if covered {
                unlink.push(file_id);
            }
            !covered
        });
        Admitted {
            unlink,
            requeue: Vec::new(),
        }
    }

    /// Take `file_ids` out of the held set — the reclaim's adoption unlinks a
    /// file whose every live slot now has a durable copy below the committed
    /// cut, wherever it is queued. Returns the ones that were held.
    pub fn take(&mut self, file_ids: &[u64]) -> Vec<u64> {
        let mut taken = Vec::new();
        self.held.retain(|&(file_id, _)| {
            let hit = file_ids.contains(&file_id);
            if hit {
                taken.push(file_id);
            }
            !hit
        });
        taken
    }

    /// Forget a held file that became referenced again (defensive: ids are
    /// minted once, so only a recovery merge could do it).
    pub fn forget_referenced(&mut self, is_referenced: impl Fn(u64) -> bool) {
        self.held.retain(|&(file_id, _)| !is_referenced(file_id));
    }

    /// Files held now.
    #[inline]
    pub fn len(&self) -> usize {
        self.held.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.held.is_empty()
    }

    /// Whether some held file still waits for a fold to commit (its stamp is
    /// not below `committed_floor`) — i.e. whether a fold would release
    /// anything that is not already releasable.
    pub fn awaits_fold(&self, committed_floor: u64) -> bool {
        self.held.iter().any(|&(_, stamp)| stamp >= committed_floor)
    }

    /// Whether `file_id` is held (tests, diagnostics).
    pub fn is_held(&self, file_id: u64) -> bool {
        self.held.iter().any(|&(f, _)| f == file_id)
    }

    /// Fold another index's hold state into this one (recovery merges per-db
    /// indexes; none of them has seen a view yet, so this is defensive).
    pub fn merge(&mut self, other: UnlinkHold) {
        if other.active {
            self.active = true;
            self.hold_below = self.hold_below.max(other.hold_below);
            self.seen_epoch = self.seen_epoch.max(other.seen_epoch);
        }
        for held in other.held {
            if !self.held.iter().any(|&(f, _)| f == held.0) {
                self.held.push(held);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(epoch: u64, committed_floor: u64, next_file_id: u64) -> FoldView {
        FoldView {
            epoch,
            committed_floor,
            next_file_id,
        }
    }

    #[test]
    fn without_a_view_every_file_goes_as_before() {
        let mut h = UnlinkHold::default();
        assert_eq!(
            h.admit(vec![3, 9]),
            Admitted {
                unlink: vec![3, 9],
                requeue: vec![]
            }
        );
        assert!(h.is_empty());
    }

    /// Files that existed at the first view are held until a fold that
    /// starts after the decision commits; files minted after the latest cut
    /// go at once.
    #[test]
    fn a_file_below_the_latest_cut_waits_for_a_committed_fold_after_it() {
        let mut h = UnlinkHold::default();
        h.observe(view(0, 0, 20));
        assert_eq!(h.admit(vec![5, 20, 25]).unlink, vec![20, 25]);
        assert!(h.is_held(5));

        // A fold cut at epoch 1 and committed: the stamp (0) is below it.
        // Nothing new queued, the held file is released.
        h.observe(view(1, 1, 30));
        assert_eq!(h.admit(vec![]).unlink, vec![5]);
        assert!(h.is_empty());
    }

    /// The case moon#1231 is about: the file went zero-ref AFTER the fold cut
    /// that committed, so that fold's base lacks what it backed.
    #[test]
    fn a_fold_committed_before_the_zero_ref_releases_nothing() {
        let mut h = UnlinkHold::default();
        // The fold with snapshot epoch 1 cut at file id 20 and committed.
        h.observe(view(1, 1, 20));
        assert!(h.admit(vec![5]).unlink.is_empty(), "stamp 1 is not < 1");
        assert!(h.awaits_fold(1));
        // A second fold is cut (epoch 2) but has not committed yet.
        h.observe(view(2, 1, 21));
        assert!(h.admit(vec![]).unlink.is_empty());
        // It aborts: floor unchanged; still held.
        h.observe(view(2, 1, 21));
        assert!(h.admit(vec![]).unlink.is_empty());
        // A third one commits.
        h.observe(view(3, 3, 22));
        assert_eq!(h.admit(vec![]).unlink, vec![5]);
        assert!(!h.awaits_fold(3));
    }

    /// A cut raises the bound to the counter: a file minted between two cuts
    /// is below the newer one.
    #[test]
    fn every_epoch_change_raises_the_bound_to_the_counter() {
        let mut h = UnlinkHold::default();
        h.observe(view(0, 0, 10));
        assert_eq!(h.admit(vec![12]).unlink, vec![12], "minted after the view");
        h.observe(view(1, 0, 15));
        assert!(h.admit(vec![12]).unlink.is_empty(), "12 < the new cut");
        // Same epoch again: the bound does not move with the counter.
        h.observe(view(1, 0, 40));
        assert_eq!(h.admit(vec![20]).unlink, vec![20]);
    }

    #[test]
    fn a_decision_without_a_fresh_view_unlinks_nothing_new() {
        let mut h = UnlinkHold::default();
        h.observe(view(0, 0, 10));
        assert_eq!(h.admit(vec![12]).unlink, vec![12]);
        assert_eq!(
            h.admit(vec![14]),
            Admitted {
                unlink: vec![],
                requeue: vec![14]
            },
            "the view is consumed by the decision it was taken for"
        );
    }

    #[test]
    fn taken_and_referenced_files_leave_the_held_set() {
        let mut h = UnlinkHold::default();
        h.observe(view(1, 0, 10));
        assert!(h.admit(vec![1, 2, 3]).unlink.is_empty());
        assert_eq!(h.take(&[2, 7]), vec![2]);
        h.forget_referenced(|f| f == 3);
        assert!(h.is_held(1) && !h.is_held(2) && !h.is_held(3));
        assert_eq!(h.len(), 1);
    }
}
