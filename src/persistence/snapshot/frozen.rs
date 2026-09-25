//! What a table a FLUSHDB detached keeps while the epoch holds it
//! (moon#1228, reviews 5 and 6).
//!
//! `Database::clear` hands the epoch its table AS FLUSHED: the database's
//! epoch-start rows, and every row written since the epoch began — rows the
//! file must not contain (each has a pre-image in the epoch's overflow map:
//! its epoch-start entry, or a tombstone for a key created since). Held
//! whole, that is unbounded: a client that fills a database and flushes it,
//! database after database, made one held save keep 8 x 40 MB of
//! post-epoch rows under `--maxmemory 64mb`, none of it in `used_memory`.
//! So the epoch TRIMS a frozen table to the epoch-start rows its database
//! still has to write ([`Trim`]).
//!
//! The trim is budgeted (review 6): at most [`TRIM_BUDGET`] row operations
//! per drain, across every frozen table, continued at the next drain — done
//! whole inside one drain it cost 0.88 s for a pending database of 0.5M
//! epoch-start rows grown by 1.5M, 4.33 s at 2M + 6M (release). The FILE
//! never depends on the trim: until a key's pre-image is folded into the
//! table, the walk shadows the table's row with it, and the walk never reads
//! below its cursor. Only the byte bound does, so it holds once the trim is
//! done: after `ceil(work / TRIM_BUDGET)` drains, work being the keys
//! written since the epoch began + the rows below the cursor at the flush +
//! (for a rebuild) the rows kept.

use super::{SnapshotState, Source, Table, pre_image_bytes, segment_block};
use crate::storage::compact_key::CompactKey;
use crate::storage::dashtable::hash_key;
use crate::storage::db::{LAZY_FREE_THRESHOLD, entry_overhead, lazy_free_weight};
use crate::storage::entry::Entry;

/// Row operations the trim does per drain, across every frozen table: a
/// row removed, restored or moved, or a segment visited — plus, for a
/// collection removed or restored, one per `LAZY_FREE_THRESHOLD` of its
/// elements, which its charge walks (review 7: a collection counted as one
/// row whatever its size, and 512 hashes of 4,000 fields made one drain
/// stall PING for 74-106 ms). A row or segment that would not fit waits for
/// the next drain, which takes it even alone over the budget: a collection
/// of more than ~32K elements is walked whole by one drain (~17 ns an
/// element, release). An operation costs ~0.1 us (a written row) to ~1.3 us
/// (a pre-image of a million-row table, a rebuild move, or 64 elements of a
/// collection) in a release build, so a drain spends under ~0.7 ms here,
/// below the 1 ms tick: at 2,048 a drain could outlast the tick and the loop
/// ran ticks back to back (PING p99.9 10-27 ms while an 8M-row table was
/// trimmed; 1.0 ms at 512). Measured end to end in NOTES (reviews 6 and 7);
/// `table_swap_tests::trim_cost_of_a_large_flushed_table` (`--ignored`)
/// measures drains in-process.
pub(crate) const TRIM_BUDGET: usize = 512;

/// What the trim frees off the shard thread.
enum Discard {
    /// The emptied source of a rebuild: its skeleton alone took 19.6 ms to
    /// free inline (58,833 segments, release).
    Table(#[allow(dead_code)] Box<Table>),
    /// A post-epoch collection the trim removed, of more than
    /// `LAZY_FREE_THRESHOLD` elements.
    Value(#[allow(dead_code)] Entry),
}

/// Hand `item` to the lazily started `moon-snapdrop` helper, which drops it
/// (one per process, parked in `recv` when idle — the `moon-lazyfree`
/// pattern). Dropped here if the helper cannot be started or is gone.
fn discard(item: Discard) {
    static DROPPER: std::sync::OnceLock<Option<flume::Sender<Discard>>> =
        std::sync::OnceLock::new();
    let dropper = DROPPER.get_or_init(|| {
        let (tx, rx) = flume::unbounded::<Discard>();
        std::thread::Builder::new()
            .name("moon-snapdrop".to_string())
            .spawn(move || {
                crate::shard::numa::pin_current_aux_thread("moon-snapdrop");
                while let Ok(item) = rx.recv() {
                    drop(item);
                }
            })
            .ok()
            .map(|_| tx)
    });
    if let Some(tx) = dropper {
        // A failed send hands the item back in the error; it drops here.
        let _ = tx.send(item);
    }
}

/// Free a row the trim removed: off the shard thread if UNLINK would free
/// it lazily (more than `LAZY_FREE_THRESHOLD` elements), inline otherwise —
/// an O(1) free. (Review 7: the cut was 4,096 elements, so a drain freed up
/// to 512 collections of 4,095 elements inline.)
fn dispose(entry: Entry) {
    if lazy_free_weight(&entry).is_some() {
        discard(Discard::Value(entry));
    } else {
        #[cfg(test)]
        ELEMENTS_FOR_TEST.with(|c| {
            let (charged, inline) = c.get();
            c.set((charged, inline + lazy_free_weight(&entry).unwrap_or(0)));
        });
    }
}

#[cfg(test)]
thread_local! {
    /// Test-only override of [`TRIM_BUDGET`] (this thread's drains).
    static BUDGET_FOR_TEST: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
    /// Test-only: elements of the collections this thread's trims charged
    /// (removed or restored), and of those they freed inline.
    static ELEMENTS_FOR_TEST: std::cell::Cell<(usize, usize)> =
        const { std::cell::Cell::new((0, 0)) };
}

/// Test-only: `(charged, freed inline)` collection elements of this
/// thread's trims since the last call.
#[cfg(test)]
pub(crate) fn take_trim_elements_for_test() -> (usize, usize) {
    ELEMENTS_FOR_TEST.with(|c| c.replace((0, 0)))
}

/// The budget of one drain's trim: [`TRIM_BUDGET`], or a test's override.
#[inline]
pub(crate) fn trim_budget() -> usize {
    #[cfg(test)]
    if let Some(budget) = BUDGET_FOR_TEST.with(std::cell::Cell::get) {
        return budget;
    }
    TRIM_BUDGET
}

/// Test-only: run this thread's trims with `budget` row operations per
/// drain (`None`: [`TRIM_BUDGET`]) — a tiny one spreads every trim over many
/// drains, interleaved with the walk.
#[cfg(test)]
pub(crate) fn set_trim_budget_for_test(budget: Option<usize>) {
    BUDGET_FOR_TEST.with(|b| b.set(budget));
}

/// A table a FLUSHDB detached from an epoch database the walk had not
/// finished (moon#1228): what the epoch reads that database from.
pub(crate) struct Frozen {
    pub(super) table: Box<Table>,
    /// What the table's rows are billed (INFO `current_cow_size`): its
    /// `used_memory` at the flush, less the rows the trim removed, plus the
    /// epoch-start entries it restored — each at its `entry_overhead`.
    pub(super) bill: u64,
    /// The database's `used_memory` when the epoch began: what the bill
    /// holds above it while steps 1-2 run is post-epoch (review 6, S2).
    start_bill: u64,
    trim: Trim,
}

/// Where a frozen table's trim stands. The steps run in order; each can
/// stop at any row and resume at the next drain.
enum Trim {
    /// 1. Every key written since the epoch began has a pre-image in the
    ///    database's overflow map (the drain folds the queued captures in
    ///    before it freezes a table, and nothing writes the slot's epoch
    ///    table after the flush). Its post-epoch row leaves the table, and
    ///    the epoch-start entry goes back in its place (a tombstone puts
    ///    nothing back). Done when the map is empty. `below`: the cursor
    ///    when the table froze, if it was the database in progress (else 0).
    PreImages {
        below: u64,
    },
    /// 2. The rows hashing below `below` — the walk's cursor when step 1
    ///    finished, for the database in progress — are written already, or
    ///    are post-epoch: written into a written range (no pre-image is
    ///    taken there), or a key whose pre-image the walk took itself while
    ///    step 1 ran. The walk never reads them. Removed segment by segment
    ///    from hash `at`.
    Written {
        at: u64,
        below: u64,
    },
    /// 3. Removing rows does not shrink a DashTable's segments: when the
    ///    post-epoch rows had grown the table past twice its epoch-start
    ///    segment count, the kept rows move into `sized`, segment by segment
    ///    from storage index `next`; rows below the walk's cursor (written
    ///    since the flush) are dropped instead. The walk waits for this step
    ///    to finish before it reads the table again.
    Rebuild {
        sized: Box<Table>,
        next: usize,
    },
    Done,
}

impl Frozen {
    pub(super) fn new(table: Box<Table>, bill: u64, below: u64, start_bill: u64) -> Self {
        Frozen {
            table,
            bill,
            start_bill,
            trim: Trim::PreImages { below },
        }
    }

    /// Post-epoch bytes the table still holds: its bill above the
    /// database's epoch-start bill until steps 1-2 are done (the rebuild
    /// moves rows, it removes none), 0 after.
    fn untrimmed_excess(&self) -> u64 {
        match self.trim {
            Trim::PreImages { .. } | Trim::Written { .. } => {
                self.bill.saturating_sub(self.start_bill)
            }
            Trim::Rebuild { .. } | Trim::Done => 0,
        }
    }

    /// The walk must not read the table: its rows are moving to another.
    pub(super) fn rebuilding(&self) -> bool {
        matches!(self.trim, Trim::Rebuild { .. })
    }

    pub(super) fn trimmed(&self) -> bool {
        matches!(self.trim, Trim::Done)
    }
}

/// What a row costs the trim's budget beyond its one operation: a
/// collection's charge ([`charge`]) walks its elements, one operation per
/// `LAZY_FREE_THRESHOLD` of them (review 7). 0 for a string or a listpack.
#[inline]
fn size_ops(entry: &Entry) -> usize {
    lazy_free_weight(entry).map_or(0, |n| n / LAZY_FREE_THRESHOLD)
}

#[inline]
fn charge(key: &[u8], entry: &Entry) -> u64 {
    #[cfg(test)]
    ELEMENTS_FOR_TEST.with(|c| {
        let (charged, inline) = c.get();
        c.set((charged + lazy_free_weight(entry).unwrap_or(0), inline));
    });
    entry_overhead(key, entry) as u64
}

impl SnapshotState {
    /// The lazy-free drain freed `bytes` of a value whose remaining charge
    /// was in epoch database `db`'s ledger when a FLUSHDB froze its table
    /// (review 7): the bill took that ledger, so it drops by them. A table
    /// already released (or an aborted epoch) has no bill left to credit.
    pub(crate) fn credit_frozen(&mut self, db: usize, bytes: u64) {
        if let Some(Source::Frozen(frozen)) = self.sources.get_mut(db) {
            frozen.bill = frozen.bill.saturating_sub(bytes);
        }
    }

    /// Post-epoch bytes the frozen tables still hold, summed (review 6, S2):
    /// what `snapshot_cow::note_cleared_table` weighs a further grown flush
    /// against. Published after every drain.
    pub(crate) fn untrimmed_excess(&self) -> u64 {
        self.sources
            .iter()
            .map(|s| match s {
                Source::Frozen(frozen) => frozen.untrimmed_excess(),
                _ => 0,
            })
            .sum()
    }

    /// Advance the trims of the frozen tables, lowest database first (the
    /// walk needs it soonest), by at most `budget` row operations in all.
    /// Returns the operations done. Called by every drain.
    pub(crate) fn trim_frozen(&mut self, budget: usize) -> usize {
        let mut used = 0;
        for db in self.current_db..self.num_databases {
            if used >= budget {
                break;
            }
            let Some(Source::Frozen(frozen)) = self.sources.get(db) else {
                continue;
            };
            if frozen.trimmed() {
                continue;
            }
            let Source::Frozen(mut frozen) =
                std::mem::replace(&mut self.sources[db], Source::Written)
            else {
                continue;
            };
            used += self.trim_step(db, &mut frozen, budget - used);
            self.sources[db] = Source::Frozen(frozen);
        }
        #[cfg(test)]
        {
            self.trim_ops_last_drain = used;
        }
        used
    }

    /// Up to `budget` row operations of `db`'s trim.
    fn trim_step(&mut self, db: usize, frozen: &mut Frozen, budget: usize) -> usize {
        let mut used = 0;
        while used < budget {
            match &mut frozen.trim {
                Trim::PreImages { below } => {
                    let Some(((_, key), pre_image)) = self.overflow[db].first_key_value() else {
                        // The walk may have passed more of the database in
                        // progress meanwhile, taking the pre-images there
                        // itself: the post-epoch rows of those keys are
                        // still in the table, below the cursor. Step 2
                        // removes everything below the cursor as it is now.
                        let cursor = if db == self.current_db {
                            self.cursor
                        } else {
                            0
                        };
                        frozen.trim = Trim::Written {
                            at: 0,
                            below: (*below).max(cursor),
                        };
                        continue;
                    };
                    // The post-epoch row and the pre-image are both charged.
                    let cost = 1
                        + frozen.table.get(key).map_or(0, size_ops)
                        + pre_image.as_ref().map_or(0, size_ops);
                    if used > 0 && used + cost > budget {
                        break; // the next drain takes this row
                    }
                    let Some(((_, key), pre_image)) = self.overflow[db].pop_first() else {
                        break;
                    };
                    self.overflow_bytes = self
                        .overflow_bytes
                        .saturating_sub(pre_image_bytes(&key, &pre_image));
                    if let Some(post) = frozen.table.remove(&key) {
                        frozen.bill = frozen.bill.saturating_sub(charge(&key, &post));
                        dispose(post);
                    }
                    if let Some(entry) = pre_image {
                        frozen.bill = frozen.bill.saturating_add(charge(&key, &entry));
                        frozen.table.insert(CompactKey::from(key), entry);
                    }
                    used += cost;
                }
                Trim::Written { at, below } => {
                    if *at >= *below {
                        let start_segments =
                            self.segment_counts.get(db).copied().unwrap_or(1).max(1);
                        frozen.trim = if frozen.table.segment_count() > 2 * start_segments {
                            Trim::Rebuild {
                                // Grown by splits as rows arrive, never
                                // pre-sized: `with_capacity` adds a depth
                                // level of headroom (twice the segments the
                                // kept rows need).
                                sized: Box::new(Table::new()),
                                next: 0,
                            }
                        } else {
                            Trim::Done
                        };
                        continue;
                    }
                    let table = &mut frozen.table;
                    let segment = table.segment(table.segment_index_for_hash(*at));
                    let mut cost = 1;
                    let written: Vec<CompactKey> = segment
                        .iter_occupied()
                        .filter(|(key, _)| hash_key(key.as_bytes()) < *below)
                        .map(|(key, entry)| {
                            cost += 1 + size_ops(entry);
                            key.clone()
                        })
                        .collect();
                    let end = segment_block(*at, segment.depth()).1;
                    if used > 0 && used + cost > budget {
                        break; // the next drain takes this segment
                    }
                    used += cost;
                    for key in written {
                        if let Some(entry) = table.remove(key.as_bytes()) {
                            frozen.bill =
                                frozen.bill.saturating_sub(charge(key.as_bytes(), &entry));
                            dispose(entry);
                        }
                    }
                    *at = end.unwrap_or(*below);
                }
                Trim::Rebuild { sized, next } => {
                    if *next >= frozen.table.segment_count() {
                        let sized = std::mem::replace(sized, Box::new(Table::new()));
                        discard(Discard::Table(std::mem::replace(&mut frozen.table, sized)));
                        frozen.trim = Trim::Done;
                        continue;
                    }
                    // Rows the walk has passed since the flush (the database
                    // in progress) are written: dropped, not moved.
                    let written_below = if db == self.current_db {
                        self.cursor
                    } else {
                        0
                    };
                    let mut cost = 1;
                    let keys: Vec<(CompactKey, bool)> = frozen
                        .table
                        .segment(*next)
                        .iter_occupied()
                        .map(|(key, entry)| {
                            let written = hash_key(key.as_bytes()) < written_below;
                            cost += 1 + if written { size_ops(entry) } else { 0 };
                            (key.clone(), written)
                        })
                        .collect();
                    if used > 0 && used + cost > budget {
                        break; // the next drain takes this segment
                    }
                    used += cost;
                    for (key, written) in keys {
                        let Some((key, entry)) = frozen.table.remove_entry(key.as_bytes()) else {
                            continue;
                        };
                        if written {
                            frozen.bill =
                                frozen.bill.saturating_sub(charge(key.as_bytes(), &entry));
                            dispose(entry);
                        } else {
                            sized.insert(key, entry);
                        }
                    }
                    *next += 1;
                }
                Trim::Done => break,
            }
        }
        used
    }
}
