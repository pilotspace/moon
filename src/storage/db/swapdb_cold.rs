//! SWAPDB and the cold tier (moon#1237).
//!
//! A spill file's manifest entry carries the logical db its keys were
//! spilled from, and a restart rebuilds each file into that db. `SWAPDB`
//! swaps whole `Database` structures, cold index included, but not the tags.
//!
//! - **Live** — [`swapdb_cold_refusal`]: a `SWAPDB` is refused while either
//!   database, on any shard, has a cold footprint ([`Database::has_cold_footprint`]).
//!   So a database that is swapped owns no spill file, and every file's tag
//!   stays equal to the slot of the database that owns it — what an AOF
//!   rewrite (whose base is written per slot) and a snapshot need. Re-tagging
//!   the files at the swap instead is not crash-consistent with the logged
//!   `SWAPDB` record: whichever of the two becomes durable first, a crash
//!   between them rebuilds the cold plane swapped while the log replays the
//!   swap again (or not at all). See the WS20 NOTES.
//! - **Replay** — [`swap_replayed`]: recovery attaches every spill file to its
//!   tag BEFORE the log replays, so a replayed `SWAPDB` carries along only the
//!   cold entries of files that existed at that point of the log (below the
//!   generation's `MOON.COLDCUT`, or whose `MOON.SPILLED` marker already
//!   replayed); a file spilled after the swap stays in its own db. Moving
//!   everything carried every later spill of the swapped database into the
//!   other database at the next restart (found by WS20, both layouts).

use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::tiered::cold_index::ColdIndex;

/// The reply to a SWAPDB refused because a swapped database holds cold data.
///
/// It names the way out (REVIEW-WS20 F10): the refusal lasts while a
/// database has cold keys OR spill files not yet reclaimed. After its cold
/// keys are deleted (a FLUSHDB/FLUSHALL included) or read back into RAM,
/// their files are held until a fold — `BGREWRITEAOF` with
/// `--appendonly yes` — or a snapshot — `BGSAVE` without an AOF — covers
/// them (moon#1231, moon#1260), then the next orphan sweep reclaims them;
/// the automatic rewrite may not run for a long time (64 MB, 100% growth).
pub const ERR_SWAPDB_COLD: &[u8] = b"ERR SWAPDB is not allowed while either database has keys \
or unreclaimed spill files in the disk-offload cold tier; after deleting or reading back its \
cold keys, run BGREWRITEAOF (appendonly yes) or BGSAVE (appendonly no) and retry";

/// The reply when a shard's databases could not be inspected in time (the
/// owner held a database for the whole bounded wait). Nothing was swapped.
pub const ERR_SWAPDB_BUSY: &[u8] =
    b"ERR SWAPDB could not check the disk-offload cold tier of every shard, try again";

/// How many non-blocking attempts [`swapdb_cold_refusal`] makes per foreign
/// database before it gives up (each followed by a `yield_now`): the owner
/// holds a database only for one command or one maintenance chunk.
///
/// REVIEW-WS20 F11: while a foreign database stays busy, the check SPINS on
/// the connection's shard thread — up to this many `yield_now`s (measured
/// ~7.5 ms on a 4-vCPU box), serving nothing else meanwhile — and then
/// answers [`ERR_SWAPDB_BUSY`]. Safe (nothing is logged or swapped), bounded,
/// and SWAPDB is rare; a refusal under that contention is the price of
/// never parking a shard thread on another shard's lock.
const FOREIGN_READ_ATTEMPTS: usize = 20_000;

impl Database {
    /// Whether this database owns anything a restart would rebuild under its
    /// logical db index, or that a spill completion would publish there: a
    /// cold-tier footprint ([`ColdIndex::has_footprint`]), an in-flight spill,
    /// or a spill retired while in flight whose completion has not arrived
    /// (moon#1253).
    #[must_use]
    pub fn has_cold_footprint(&self) -> bool {
        !self.spill_inflight.is_empty()
            || !self.spill_superseded.is_empty()
            || self
                .cold_index
                .as_ref()
                .is_some_and(ColdIndex::has_footprint)
    }

    /// Whether a value stored in `file_id` belongs to this database at the
    /// current point of an AOF replay: the gate authorizes it (below the
    /// `MOON.COLDCUT`, or its marker replayed), or — in a generation with no
    /// cut — its `MOON.SPILLED` marker replayed. A pre-#902 log (neither
    /// record) authorizes nothing, so a replayed swap keeps every cold entry
    /// in place there.
    fn replay_file_existed(&self, file_id: u64) -> bool {
        match self.replay_cold_gate.as_ref() {
            Some(gate) => gate.is_authorized(file_id),
            None => self.replay_markers.marked(file_id),
        }
    }

    /// Detach the cold entries of files spilled after this point of the
    /// replay (not [`Self::replay_file_existed`]), with the shard dir they
    /// are read from, to be put back into this SLOT after a swap.
    fn take_cold_born_later(&mut self) -> Option<(Option<std::path::PathBuf>, ColdIndex)> {
        let dir = self.cold_shard_dir.clone();
        let mut ci = self.cold_index.take()?;
        let later = ci.split_off_files(|file_id| !self.replay_file_existed(file_id));
        self.cold_index = Some(ci);
        later.has_footprint().then_some((dir, later))
    }

    fn put_back_cold(&mut self, later: Option<(Option<std::path::PathBuf>, ColdIndex)>) {
        let Some((dir, later)) = later else {
            return;
        };
        if self.cold_shard_dir.is_none() {
            self.cold_shard_dir = dir;
        }
        match self.cold_index.as_mut() {
            Some(ci) => ci.merge_newer(later),
            None => self.cold_index = Some(later),
        }
    }
}

/// `SWAPDB` replayed from an AOF / WAL record: swap the two databases, but
/// leave in each SLOT the cold entries of files spilled there after this
/// point of the log (see the module doc). Everything else — the hot planes,
/// the cold entries that existed, the replay gate that authorized them —
/// moves with the swap, exactly as `db_plane::swap_contents` does live.
pub fn swap_replayed(a: &mut Database, b: &mut Database) {
    let a_later = a.take_cold_born_later();
    let b_later = b.take_cold_born_later();
    crate::shard::db_plane::swap_contents(a, b);
    // Seeing a marker at all proves the log is #902-era (moon#965), for both.
    let saw = a.replay_markers.saw() || b.replay_markers.saw();
    a.replay_markers.set_saw(saw);
    b.replay_markers.set_saw(saw);
    a.put_back_cold(a_later);
    b.put_back_cold(b_later);
}

/// Why `SWAPDB a b` must be refused on this server, or `None` to go ahead:
/// db `a` or db `b` has a cold footprint on some shard. Called on the
/// connection's shard thread before anything is logged or swapped. The local
/// shard is read through its owner guards; every other shard through the
/// shared read plane, without ever parking (a foreign reader's rule), for a
/// bounded number of attempts.
///
/// The check and the swaps are not one atomic step across shards: a shard
/// that spills a key of `a` or `b` between this check and its own swap still
/// swaps (see [`note_swap_with_cold_footprint`]).
///
/// The foreign reads go through the process-wide shared read plane
/// (`db_plane::shard_dbs`), installed once per process: a SECOND server in
/// the same process (embedded tests only) reads the FIRST server's databases
/// here (REVIEW-WS20 F11).
#[must_use]
pub fn swapdb_cold_refusal(
    a: usize,
    b: usize,
    my_shard: usize,
    num_shards: usize,
) -> Option<Frame> {
    let local = crate::shard::slice::with_shard_db_read(a, Database::has_cold_footprint)
        || crate::shard::slice::with_shard_db_read(b, Database::has_cold_footprint);
    if local {
        return Some(Frame::Error(bytes::Bytes::from_static(ERR_SWAPDB_COLD)));
    }
    for shard in (0..num_shards).filter(|s| *s != my_shard) {
        let Some(set) = crate::shard::db_plane::shard_dbs(shard) else {
            // No shared plane (unit tests that build one slice): nothing else
            // to inspect.
            continue;
        };
        for db in [a, b] {
            match foreign_footprint(set, db) {
                Some(false) => {}
                Some(true) => {
                    return Some(Frame::Error(bytes::Bytes::from_static(ERR_SWAPDB_COLD)));
                }
                None => return Some(Frame::Error(bytes::Bytes::from_static(ERR_SWAPDB_BUSY))),
            }
        }
    }
    None
}

/// [`swapdb_cold_refusal`] for the single-listener handler (`handler_single`,
/// the embedded server), whose databases sit behind their own locks.
#[must_use]
pub fn swap_refused_for_cold(dbs: &[parking_lot::RwLock<Database>], a: usize, b: usize) -> bool {
    dbs[a].read().has_cold_footprint() || dbs[b].read().has_cold_footprint()
}

/// One foreign database's footprint through non-blocking reads (`None`: the
/// owner held it for every attempt).
fn foreign_footprint(set: &crate::shard::db_plane::ShardDbSet, db: usize) -> Option<bool> {
    for _ in 0..FOREIGN_READ_ATTEMPTS {
        if let Some(guard) = set.try_read(db) {
            return Some(guard.has_cold_footprint());
        }
        std::thread::yield_now();
    }
    None
}

/// At a shard's own swap: say so, loudly, if a swapped database acquired a
/// cold footprint after [`swapdb_cold_refusal`] cleared it (a spill in the
/// window between the check and this shard's swap). The swap still happens —
/// every other shard has swapped, and the hot data must stay consistent —
/// and the keys spilled in that window are exposed to moon#1237 until their
/// files are reclaimed. Owner thread, before the swap.
pub fn note_swap_with_cold_footprint(shard_id: usize, a: usize, b: usize) {
    if a == b {
        return;
    }
    let dirty = crate::shard::slice::with_shard_db_read(a, Database::has_cold_footprint)
        || crate::shard::slice::with_shard_db_read(b, Database::has_cold_footprint);
    if dirty {
        tracing::error!(
            shard = shard_id,
            db_a = a,
            db_b = b,
            "SWAPDB raced a spill of a swapped database: its spill files keep their \
             pre-swap db tags, so keys spilled in that window may reappear in the \
             other database after an AOF rewrite and a restart (moon#1237)"
        );
    }
}

#[cfg(test)]
#[path = "swapdb_cold_tests.rs"]
mod tests;
