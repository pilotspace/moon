//! In-memory cold index tracking KV entries spilled to disk DataFiles.
//!
//! Maps key bytes to (file_id, slot_idx) for cold lookup.
//! Populated at spill time, rebuilt from heap DataFiles during recovery.
//!
//! Since #368 the index is ordered by `(scan_hash48(key), key)` — the same
//! stable 48-bit hash order the hot plane's SCAN page walk uses — so SCAN's
//! cold side can range-resume from a cursor in O(log n + COUNT) instead of
//! filtering the whole index on every page. Lookup pays O(log n) instead of
//! O(1); cold lookups sit on disk-read-through and sweep paths where a
//! ~100ns tree descent is noise next to the I/O they front.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use bytes::Bytes;

/// The SCAN cursor hash: the hot table's fixed-seed xxh64 truncated to its
/// top 48 bits. MUST stay identical to the hot plane's cursor mapping
/// (`Database::scan_hot_page` maps `hash_key(key) >> 16`) — both planes
/// feed one merged hash-ordered page walk.
#[inline]
pub(super) fn scan_h48(key: &[u8]) -> u64 {
    crate::storage::dashtable::hash_key(key) >> 16
}

/// Maximum TTL-expired cold entries reclaimed per [`ColdIndex::sweep_expired`]
/// call. Bounds one sweep tick's work so a large cold index under an expiry
/// storm cannot turn the shard's periodic sweep into an unbounded stall.
pub const MAX_EXPIRED_SWEEP_BATCH: usize = 4096;

/// Statistics returned by [`ColdIndex::orphan_sweep`].
///
/// An orphan is a cold entry whose key has been re-written to the hot
/// in-memory DashTable. The cold copy is stale and safe to delete.
#[derive(Debug, Default, Clone, Copy)]
pub struct SweepStats {
    /// Number of cold index entries removed (one per orphaned key).
    pub entries_reclaimed: usize,
    /// Sum of DataFile sizes deleted, in bytes.
    pub bytes_reclaimed: u64,
}

/// Location of a cold KV entry on disk.
///
/// Multi-page spill files store many KV entries across several KvLeafPages.
/// `page_idx` is the FILE-ABSOLUTE 4 KB chunk index (0-based); `slot_idx`
/// is the slot within that page.  For the legacy single-page path, both are
/// always 0.
/// `PartialEq`/`Eq` (task #59): lets an async cold-read caller that
/// suspended mid-read revalidate, once it resumes on the shard thread, that
/// the cold index still maps its key to the SAME location before promoting
/// the (now possibly stale) result — see
/// `Database::promote_cold_outcome`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColdLocation {
    /// Manifest file_id of the heap DataFile.
    pub file_id: u64,
    /// File-absolute 4KB page index within the DataFile (0 = first page).
    pub page_idx: u32,
    /// Slot index within the KvLeafPage at `page_idx`.
    pub slot_idx: u16,
    /// Absolute expiry time in milliseconds, mirroring the on-disk
    /// `KvEntry::ttl_ms` this location points at. `None` = no TTL.
    ///
    /// R1 (H-2, tmp/OFFLOAD-COMPRESSION-REVIEW.md): populated at insert time
    /// (spill / recovery rebuild) purely so the proactive sweep
    /// ([`ColdIndex::sweep_expired`]) can judge expiry from the in-RAM index
    /// alone — WITHOUT a pread of the cold file. This is a cached copy, not a
    /// new source of truth: the on-disk `KvLeafPage` entry stays authoritative
    /// and is exactly what [`Self::rebuild_from_manifest`] re-derives this
    /// field from after a restart. No on-disk format changed by this field;
    /// `ColdIndex` itself has no serialized form of its own (it is rebuilt
    /// fresh from the manifest + heap files on every startup), so there is no
    /// index format to version and no old-format file that could ever fail to
    /// load because of it.
    pub ttl_ms: Option<u64>,
    /// Redis value type of the on-disk entry, mirroring the on-disk
    /// `KvEntry::value_type` this location points at.
    ///
    /// Same cached-copy contract as `ttl_ms` (#364): populated at insert
    /// time (spill / recovery rebuild) purely so SCAN's TYPE filter can
    /// judge a cold-only key from the in-RAM index alone — WITHOUT a pread
    /// of the cold file and WITHOUT promoting the entry into hot RAM. The
    /// on-disk `KvLeafPage` entry stays authoritative and
    /// [`ColdIndex::rebuild_from_manifest`] re-derives this field from it
    /// after a restart; no on-disk format changes. Fits the struct's
    /// existing padding (after `slot_idx`), so the in-RAM index does not
    /// grow.
    pub value_type: crate::persistence::kv_page::ValueType,
}

impl ColdLocation {
    /// The order in which two on-disk copies of the SAME key were written —
    /// the later copy is the newer value (moon#983).
    ///
    /// `file_id` is the shard's spill allocation sequence: it is handed out
    /// at eviction time from a per-shard counter that only ever increases and
    /// is re-seeded on restart strictly above every id already in use
    /// (`file_id_seed::next_file_id_seed`). A key can only be spilled
    /// again after it came back hot (a write, or a read-through promotion),
    /// so its second copy always carries a strictly higher `file_id` and a
    /// value at least as new. Within one file, `(page_idx, slot_idx)` is the
    /// order the batch builder packed the requests in, which is their
    /// request order.
    ///
    /// What this is NOT: manifest order. `ShardManifest::files()` is
    /// `add_file` push order, and the two spill paths push at different
    /// moments — the async path (`--appendonly yes`) pushes when the
    /// background completion is applied, the durable-batch path
    /// (`--appendonly no`) pushes at eviction time. A `CONFIG SET appendonly`
    /// flip with a completion still in flight pushes a higher `file_id` ahead
    /// of a lower one, and the same ordering holds across a restart because
    /// `gc_tombstones`, manifest compaction and reopen all preserve push
    /// order. Anything that must pick the newest copy orders by this key.
    ///
    /// The restart seed is proven or the server refuses to start (moon#997):
    /// a scan that cannot list a directory, read an entry or open the
    /// manifest is an error, never a fallback to `1` — a reset counter would
    /// re-mint `heap-000001.mpf` and rename over the older copy before any
    /// ordering question arose.
    #[inline]
    #[must_use]
    pub fn recency_key(&self) -> (u64, u32, u16) {
        (self.file_id, self.page_idx, self.slot_idx)
    }
}

/// In-memory index from key to cold disk location.
///
/// NOT on the hot path -- only consulted when DashTable lookup misses
/// and disk-offload is enabled.
#[derive(Debug)]
pub struct ColdIndex {
    /// Primary index, ordered by `(scan_hash48(key), key)` so SCAN's cold
    /// plane can range-resume from a hash-space cursor (#368). The u64 is
    /// always `scan_h48` of the Bytes it is paired with — every mutation
    /// site derives it from the key, never stores it independently.
    pub(super) map: BTreeMap<(u64, Bytes), ColdLocation>,
    /// Reverse liveness: `file_id` -> count of live `map` entries pointing at it.
    ///
    /// A batched spill file (`heap-NNNNNN.mpf`) holds up to `FLUSH_ENTRY_CAP`
    /// (256) KVs across its pages, so it is safe to unlink ONLY when this count
    /// reaches 0 (no co-located live key still references it). Deleting a file
    /// on a single orphan key silently orphans its co-located keys — observed
    /// empirically as cold read-through collapsing 200/200 -> 88/200 once the
    /// orphan sweep runs.
    pub(super) file_refs: HashMap<u64, u32>,
    /// `file_id`s that dropped to zero live refs (via an `insert` overwrite or a
    /// `remove`) and are awaiting unlink. Drained off the hot path by the orphan
    /// sweep ([`Self::drain_pending_unlink`]). Pushed only on a zero-ref
    /// transition (rare), so it does not allocate on the common insert path.
    pub(super) pending_unlink: Vec<u64>,
    /// Listed spill files the boot rebuild found missing from disk (a lost
    /// tombstone commit, see [`Self::drain_pending_unlink`]), queued in
    /// `pending_unlink` by the rebuild. Consumed by the first drain: only
    /// these may skip the moon#1231 hold. Empty outside recovery.
    pub(super) missing_at_rebuild: Vec<u64>,
    /// Running total of approximate resident bytes charged by [`Self::insert`]
    /// / [`Self::remove`] / the sweep methods' direct removals / [`Self::clear_all`]
    /// (K4 accounting spine, kernel-m2-brief-2026-07-12 stage 2).
    ///
    /// Deliberately an O(1) incremental accumulator, NOT an O(n) walk over
    /// `map` computed at read time (the pattern used by
    /// `graph::store::GraphStore::resident_bytes`, which is O(segment_count)
    /// -- a much smaller bound). A cold index backing a disk-offloaded
    /// dataset is exactly the structure G2 ("serve 10x RAM datasets") sizes
    /// up to tens of millions of entries; an O(n) walk every 100ms shard
    /// tick would regress the workload this index exists for. See
    /// [`Self::resident_bytes`] for the read side.
    pub(super) resident_bytes: usize,
    /// Copies of a key that a rebuild found in OLDER files than the one the
    /// key now points at, newest first. Recovery-only: filled by
    /// [`Self::rebuild_from_manifest_per_db`] and released when the AOF
    /// replay generation closes ([`Self::release_older_copies`]). Nothing on
    /// the live path inserts here.
    ///
    /// moon#1140: a replay gated by `MOON.COLDCUT <w>` hides every file at or
    /// past `w` until its `MOON.SPILLED` marker replays. A key cold at the
    /// rewrite and spilled again afterwards points at the newer file, and
    /// when that file's marker never reached the AOF (SIGKILL before the
    /// writer flushed it, or dropped under backpressure) while its manifest
    /// entry did, the key's only replayable base is the older copy below the
    /// cut. The gate reads it from here instead of treating the key as
    /// absent, which replayed every post-rewrite write onto an empty value.
    pub(super) older_copies: HashMap<Bytes, Vec<ColdLocation>>,
    /// Slots still on disk in a listed file that are no longer their key's
    /// entry here (moon#1215) — see [`super::dead_slots`]. Every path below
    /// that takes a slot out of `map` or `older_copies` records it, and
    /// [`Self::drain_pending_unlink`] forgets a file's entries once the file
    /// is gone. The AOF rewrite fold reads it to keep deleted keys deleted.
    pub(super) dead: super::dead_slots::DeadSlots,
    /// The same slots by location, for a process WITHOUT an AOF: what each
    /// snapshot carries as its cold-graves trailer (moon#1281). Recorded at
    /// the same sites as `dead`, through [`Self::note_dead`].
    pub(super) graves: super::slot_graves::SlotGraves,
    /// Mostly-dead files compacted into new, not-yet-listed spill files,
    /// waiting for a committed AOF fold before they are adopted and the old
    /// files unlinked — the ledger's bound (see [`super::cold_reclaim`]).
    pub(super) reclaim: super::cold_reclaim::ReclaimState,
    /// Zero-ref files a replayable AOF generation may still read, kept on
    /// disk until a committed fold covers them (moon#1231) — see
    /// [`super::unlink_hold`].
    pub(super) hold: super::unlink_hold::UnlinkHold,
}

/// Approximate fixed cost of one cold-index entry beyond the key bytes: the
/// `ColdLocation` value plus the ordered map's per-entry share of node
/// overhead (hash48 tuple field + B-tree node slack). Not exact --
/// `BTreeMap`'s node layout is an implementation detail -- but monotonic,
/// matching the approximation style already used by
/// `Database::entry_overhead` (WS6) and
/// `text::term_dict::TermDictionary::resident_bytes`.
const COLD_ENTRY_OVERHEAD: usize = std::mem::size_of::<ColdLocation>() + 48;

#[inline]
pub(super) fn cold_entry_cost(key_len: usize) -> usize {
    key_len + COLD_ENTRY_OVERHEAD
}

/// What one cold-index rebuild ([`ColdIndex::rebuild_from_manifest_per_db`])
/// read, and — the point of it — what it could NOT read (moon#875).
///
/// Every entry the rebuild fails to recover reads afterwards as an ABSENT
/// key: cold read-through is index-driven, `GET` answers nil, `EXISTS`
/// answers 0, and nothing downstream can tell that answer from a key that
/// was never written. So every skip is counted here, by cause, and the
/// caller logs the whole report once — a rebuild that lost forty files and
/// one that lost nothing used to print the same single line.
///
/// The loss classes, and why each is only counted rather than fatal:
///
/// * `files_missing` — the manifest says Active, `read` says `NotFound`.
///   Expected in one crash window: the orphan sweep unlinks a zero-ref file
///   BEFORE it commits the manifest tombstone (`drain_pending_unlink`), so a
///   `SIGKILL` between the two leaves exactly this state, and nothing was
///   lost (every key in a zero-ref file has a newer copy elsewhere or was
///   deleted). The other cause is external removal, which IS loss; the two
///   are indistinguishable here. The file is queued for the sweep so its
///   manifest entry is retired and the warning does not repeat on every
///   boot forever (the moon#546a pattern).
/// * `files_unreadable` — any other `io::Error` (EACCES, EIO, EISDIR…). Up to
///   `FLUSH_ENTRY_CAP` (256) keys become unreachable. This is the class an
///   operator can usually FIX (permissions, a mount) — and the index entry
///   is skipped, never tombstoned, so a restart after the fix recovers the
///   keys. Refusing to boot would be the stronger answer, but a recovery
///   `Err` today falls back to v2 recovery (discarding the whole v3 replay:
///   worse), and shard init has no refuse-boot path — see the follow-up
///   noted in the PR for moon#875.
/// * `files_short` — the file is shorter than the `byte_size` its manifest
///   entry was stamped with. Whole pages lost to truncation are invisible
///   to every other check here (`chunks_exact` sees only whole pages), so
///   this is the only detector for that damage.
/// * `pages_rejected` — bad magic, a page type that is neither `KvLeaf` nor
///   `KvOverflow`, or a CRC32C mismatch. Skipping is the correct DECISION
///   (a corrupt page must not be trusted); the bytes are gone and no boot
///   refusal would bring them back, so it is counted and logged.
/// * `partial_page_bytes` — a trailing remainder shorter than `PAGE_4K`.
///   The writer cannot produce one (`write_kv_spill_batch` writes whole
///   pages to a temp file, fsyncs, then renames), so its presence means the
///   file was damaged after the rename. Same class as a rejected page.
/// * `entries_rejected` — a slot inside a CRC-valid page that does not
///   decode (`KvLeafPage::get` → `None`): an unknown `ValueType`, which is
///   what a downgrade to a binary older than the one that spilled the value
///   looks like. One key each.
///
/// `pages_overflow` is NOT a loss class: `KvOverflow` pages carry large
/// values and are reached through their leaf's pointer, never scanned for
/// keys. `KvLeafPage::from_bytes` rejects them too, which is why the rebuild
/// classifies the header itself before deciding what a `None` means.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ColdRebuildReport {
    /// Active `KvLeaf` manifest entries the rebuild tried to read.
    pub files_attempted: u64,
    /// Files read in full.
    pub files_read: u64,
    /// `NotFound` — see the type docs.
    pub files_missing: u64,
    /// Any other read error — see the type docs.
    pub files_unreadable: u64,
    /// Files shorter than their manifest `byte_size`.
    pub files_short: u64,
    /// Bytes those short files are missing, summed.
    pub short_file_bytes: u64,
    /// Whole `PAGE_4K` chunks examined.
    pub pages_scanned: u64,
    /// Valid `KvOverflow` pages (expected; not a loss).
    pub pages_overflow: u64,
    /// Pages that failed magic, type or CRC.
    pub pages_rejected: u64,
    /// Bytes in trailing partial pages, summed.
    pub partial_page_bytes: u64,
    /// Entries decoded and handed to the index (before duplicate resolution).
    pub entries_recovered: u64,
    /// Slots inside valid pages that did not decode.
    pub entries_rejected: u64,
    /// Slots the loaded snapshot's cold-graves trailer names as dead
    /// (moon#1281): dropped before each key's newest copy is chosen. Not a
    /// loss — the snapshot proves these keys were gone.
    pub entries_tombstoned: u64,
}

impl ColdRebuildReport {
    /// `true` if any loss class fired. A degraded rebuild means some number
    /// of keys now read as absent; the caller logs it at `error`.
    #[must_use]
    pub fn is_degraded(&self) -> bool {
        self.files_missing > 0
            || self.files_unreadable > 0
            || self.files_short > 0
            || self.pages_rejected > 0
            || self.partial_page_bytes > 0
            || self.entries_rejected > 0
    }
}

/// The result of [`ColdIndex::rebuild_from_manifest_per_db`]: one index per
/// logical db, plus the [`ColdRebuildReport`] the caller must log.
#[derive(Debug)]
pub struct ColdRebuild {
    /// `(db_index, index)` for every db with at least one recovered entry
    /// or one missing file queued for manifest retirement.
    pub per_db: Vec<(usize, ColdIndex)>,
    pub report: ColdRebuildReport,
}

/// How many per-file / per-page problems one rebuild logs individually
/// before falling back to the summary counts. A corpus with hundreds of
/// damaged files should not print hundreds of lines on the boot path; the
/// summary carries the totals and the first few carry the `file_id`s an
/// operator needs to start from.
const REBUILD_DETAIL_LOG_CAP: u64 = 16;

/// Why one `PAGE_4K` chunk was not a usable `KvLeaf` page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PageVerdict {
    Leaf,
    Overflow,
    BadHeader,
    ForeignType,
    BadChecksum,
}

/// Classify one page-sized chunk by its header BEFORE `KvLeafPage::from_bytes`
/// gets a say — that constructor answers `None` for a valid overflow page and
/// for a corrupt one alike, and only one of those is a loss.
pub(super) fn classify_page(chunk: &[u8]) -> PageVerdict {
    use crate::persistence::page::{MoonPageHeader, PageType};
    let Some(hdr) = MoonPageHeader::read_from(chunk) else {
        return PageVerdict::BadHeader;
    };
    match hdr.page_type {
        PageType::KvOverflow => {
            if MoonPageHeader::verify_checksum(chunk) {
                PageVerdict::Overflow
            } else {
                PageVerdict::BadChecksum
            }
        }
        PageType::KvLeaf => {
            if MoonPageHeader::verify_checksum(chunk) {
                PageVerdict::Leaf
            } else {
                PageVerdict::BadChecksum
            }
        }
        _ => PageVerdict::ForeignType,
    }
}

impl ColdIndex {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
            file_refs: HashMap::new(),
            pending_unlink: Vec::new(),
            missing_at_rebuild: Vec::new(),
            resident_bytes: 0,
            older_copies: HashMap::new(),
            dead: super::dead_slots::DeadSlots::default(),
            graves: super::slot_graves::SlotGraves::default(),
            reclaim: super::cold_reclaim::ReclaimState::default(),
            hold: super::unlink_hold::UnlinkHold::default(),
        }
    }

    /// The dead-slot ledger (moon#1215): keys whose slot is still on disk in
    /// a listed file although it is no longer their entry here.
    #[inline]
    pub fn dead_slots(&self) -> &super::dead_slots::DeadSlots {
        &self.dead
    }

    /// The ledger's resident bytes — the part of [`Self::resident_bytes`] no
    /// eviction can free (PR #1233 review: charged at write admission, kept
    /// out of the eviction and pressure-cascade targets), plus what pending
    /// reclaim compactions hold until they are adopted. O(1).
    #[inline]
    pub fn dead_slot_bytes(&self) -> usize {
        self.dead.resident_bytes() + self.reclaim.resident_bytes()
    }

    /// Record a slot of `key` in `file_id` that never became its entry here —
    /// a spill completion that published `file_id` for OTHER keys while this
    /// one was superseded or withdrawn (a ghost slot, moon#1215). `ttl_ms` is
    /// the slot's own absolute TTL, as written.
    pub fn note_dead_slot(&mut self, key: Bytes, location: ColdLocation) {
        self.note_dead(key, &location);
    }

    /// Append this index's no-AOF dead slots, by file, to `out`: what a
    /// snapshot starting now carries as its cold-graves trailer (moon#1281).
    pub fn collect_graves_into(&self, out: &mut Vec<(u64, Vec<u64>)>) {
        self.graves.collect_into(out);
    }

    /// The no-AOF dead-slot record (moon#1281; tests, diagnostics).
    #[inline]
    pub fn slot_graves(&self) -> &super::slot_graves::SlotGraves {
        &self.graves
    }

    /// Test shim for the pre-moon#1281 signature: a ghost slot at page 0,
    /// slot 0 of `file_id` (only the ledger's key matters to those tests).
    #[cfg(test)]
    pub(crate) fn note_dead_slot_in(&mut self, file_id: u64, key: Bytes, ttl_ms: Option<u64>) {
        let location = ColdLocation {
            file_id,
            page_idx: 0,
            slot_idx: 0,
            ttl_ms,
            value_type: crate::persistence::kv_page::ValueType::String,
        };
        self.note_dead(key, &location);
    }

    /// Record one dead slot in both records: the AOF fold's key ledger and the
    /// no-AOF snapshot's location record (each a no-op in the other mode).
    #[inline]
    fn note_dead(&mut self, key: Bytes, location: &ColdLocation) {
        self.graves
            .note(location.file_id, location.page_idx, location.slot_idx);
        self.dead.note(location.file_id, key, location.ttl_ms);
    }

    /// The copies of `key` a rebuild found behind its current entry, newest
    /// first (see the `older_copies` field). Empty outside recovery.
    #[inline]
    pub fn older_copies(&self, key: &[u8]) -> &[ColdLocation] {
        if self.older_copies.is_empty() {
            return &[];
        }
        self.older_copies.get(key).map_or(&[], Vec::as_slice)
    }

    /// Drop the superseded copies once replay no longer needs them, releasing
    /// the file reference each one holds. A file left with no referrer joins
    /// the unlink queue here — exactly where the rebuild would have put it had
    /// the copies never been retained. Returns how many keys carried any.
    pub fn release_older_copies(&mut self) -> usize {
        let older = std::mem::take(&mut self.older_copies);
        let keys = older.len();
        for (key, copies) in older {
            for location in copies {
                // The slot stays on disk and outlives this release: once the
                // key's newer copy goes, it would be the one a rebuild finds.
                self.note_dead(key.clone(), &location);
                if self.ref_dec(location.file_id) {
                    self.pending_unlink.push(location.file_id);
                }
            }
        }
        keys
    }

    /// Release the superseded copies of one key. A key leaving the index
    /// (DEL/UNLINK, promotion back to RAM, expiry reclaim) has no reader left
    /// for the copies behind it, so they release their file references here
    /// rather than waiting for the end of the generation.
    fn release_older_copies_of(&mut self, key: &[u8]) {
        if self.older_copies.is_empty() {
            return;
        }
        let Some((owned, copies)) = self.older_copies.remove_entry(key) else {
            return;
        };
        for location in copies {
            self.note_dead(owned.clone(), &location);
            if self.ref_dec(location.file_id) {
                self.pending_unlink.push(location.file_id);
            }
        }
    }

    /// Approximate resident bytes of this index: O(1) read of the running
    /// total maintained by every mutation site. See the field doc comment
    /// on `resident_bytes` for why this is incremental rather than a
    /// per-call walk.
    #[inline]
    pub fn resident_bytes(&self) -> usize {
        self.resident_bytes + self.dead_slot_bytes()
    }

    /// Increment a file's live-ref count.
    #[inline]
    fn ref_inc(&mut self, file_id: u64) {
        *self.file_refs.entry(file_id).or_insert(0) += 1;
    }

    /// Decrement a file's live-ref count. Returns `true` when it reaches zero —
    /// the file no longer backs any live cold entry and may be unlinked.
    #[inline]
    fn ref_dec(&mut self, file_id: u64) -> bool {
        if let Some(c) = self.file_refs.get_mut(&file_id) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                self.file_refs.remove(&file_id);
                return true;
            }
        }
        false
    }

    /// Record a spilled key's disk location.
    ///
    /// Maintains the reverse [`Self::file_refs`] liveness index. When this call
    /// overwrites an existing entry whose key moves to a *different* file (the
    /// re-eviction case), the old file loses its referrer; if that was its last
    /// referrer the old `file_id` is queued for unlink — the hot∩cold sweep can
    /// never see such a file because no key references it anymore.
    pub fn insert(&mut self, key: Bytes, location: ColdLocation) {
        // A fresh location supersedes the rebuild's view of this key, so the
        // copies recorded behind the entry it replaces are no longer a base
        // anything may fall back to. Free outside recovery: the map this
        // consults is empty on the live path.
        self.release_older_copies_of(&key);
        let new_file = location.file_id;
        let key_len = key.len();
        let h = scan_h48(&key);
        if let Some(old) = self.map.insert((h, key.clone()), location) {
            if old.file_id != new_file {
                // The superseded slot stays in its file (moon#1215).
                self.note_dead(key, &old);
                if self.ref_dec(old.file_id) {
                    self.pending_unlink.push(old.file_id);
                }
                self.ref_inc(new_file);
            }
            // Same file_id (different slot/page): live-ref count is
            // unchanged. `BTreeMap::insert` keeps the pre-existing key on an
            // overwrite (the new key argument, byte-equal, is dropped), so
            // `resident_bytes` is unchanged too -- no delta to apply.
        } else {
            self.ref_inc(new_file);
            self.resident_bytes += cold_entry_cost(key_len);
        }
    }

    /// Remove the entry for `key` from the ordered map, returning its
    /// location. Finds the owned map key via an equal-hash range probe
    /// (the `Bytes` clone is a refcount bump, not a data copy) — `BTreeMap`
    /// cannot borrow-match a `(u64, &[u8])` probe against a
    /// `(u64, Bytes)` key.
    ///
    /// Every removal leaves the slot on disk, so it is recorded in the
    /// dead-slot ledger here, once, for every caller (moon#1215) — except
    /// [`Self::sweep_expired`]'s, whose slots have all expired and so can
    /// never read as a value again ([`Self::remove_raw_unrecorded`]).
    fn remove_raw(&mut self, key: &[u8]) -> Option<ColdLocation> {
        let (owned, location) = self.remove_raw_unrecorded(key)?;
        self.note_dead(owned, &location);
        Some(location)
    }

    /// [`Self::remove_raw`] without the ledger entry; returns the owned key
    /// too.
    fn remove_raw_unrecorded(&mut self, key: &[u8]) -> Option<(Bytes, ColdLocation)> {
        let h = scan_h48(key);
        let owned = self
            .map
            .range((h, Bytes::new())..)
            .take_while(|(k, _)| k.0 == h)
            .find(|(k, _)| k.1.as_ref() == key)
            .map(|(k, _)| k.clone())?;
        let location = self.map.remove(&owned)?;
        Some((owned.1, location))
    }

    /// Remove a key from the cold index (promotion back to RAM, DEL/UNLINK,
    /// expired-on-read reclaim). Returns `true` when the key was present.
    ///
    /// Decrements the backing file's live-ref count; if this removes the file's
    /// last referrer, the `file_id` is queued for unlink by the next sweep.
    pub fn remove(&mut self, key: &[u8]) -> bool {
        self.release_older_copies_of(key);
        if let Some(old) = self.remove_raw(key) {
            if self.ref_dec(old.file_id) {
                self.pending_unlink.push(old.file_id);
            }
            self.resident_bytes = self
                .resident_bytes
                .saturating_sub(cold_entry_cost(key.len()));
            true
        } else {
            false
        }
    }

    /// Drop EVERY entry and queue every backing file for unlink (FLUSHDB /
    /// FLUSHALL — D1: flushed keys must not stay readable from disk). The
    /// files themselves are removed by the next orphan sweep, which holds
    /// the manifest handle this method deliberately does not need.
    pub fn clear_all(&mut self) {
        // Every slot stays on disk until the sweep unlinks its file; a
        // rewrite before that must still delete each key (moon#1215).
        for ((_, key), location) in std::mem::take(&mut self.map) {
            self.note_dead(key, &location);
        }
        for (key, copies) in std::mem::take(&mut self.older_copies) {
            for location in copies {
                self.note_dead(key.clone(), &location);
            }
        }
        self.resident_bytes = 0;
        for (&file_id, _) in self.file_refs.iter() {
            self.pending_unlink.push(file_id);
        }
        self.file_refs.clear();
    }

    /// Look up a key's cold location (equal-hash range probe; the group is
    /// almost always a single entry at 48 bits).
    pub fn lookup(&self, key: &[u8]) -> Option<ColdLocation> {
        let h = scan_h48(key);
        self.map
            .range((h, Bytes::new())..)
            .take_while(|(k, _)| k.0 == h)
            .find(|(k, _)| k.1.as_ref() == key)
            .map(|(_, loc)| *loc)
    }

    /// Merge another ColdIndex into this one (used during recovery).
    ///
    /// Routes through [`Self::insert`] so the reverse [`Self::file_refs`]
    /// liveness index is rebuilt for the merged entries (a raw `map.extend`
    /// would leave the ref counts inconsistent and break safe reclamation).
    pub fn merge(&mut self, other: ColdIndex) {
        for ((_h, key), location) in other.map {
            self.insert(key, location);
        }
        // The copies a gated replay may fall back to, each with the file
        // reference it holds. The loop above rebuilt the references of the
        // entries in FRONT of them only.
        for (key, copies) in other.older_copies {
            for location in &copies {
                self.ref_inc(location.file_id);
            }
            self.older_copies.entry(key).or_default().extend(copies);
        }
        self.dead.merge(other.dead);
        self.graves.merge(other.graves);
        self.hold.merge(other.hold);
        // Zero-ref files the other index queued (a rebuild queues the listed
        // files it found missing): the drain re-checks references before it
        // unlinks anything, so a file `self` references is kept.
        self.pending_unlink.extend(other.pending_unlink);
        self.missing_at_rebuild.extend(other.missing_at_rebuild);
    }

    /// Number of entries tracked.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Highest `file_id` any live entry still references (`None` when the
    /// index is empty). O(files), not O(keys) — walks the liveness map.
    pub fn max_file_id(&self) -> Option<u64> {
        self.file_refs.keys().copied().max()
    }

    /// Whether any zero-ref files are queued for unlink by the next sweep,
    /// or held until a committed fold covers them (moon#1231).
    ///
    /// Files orphaned by `insert` overwrite (re-eviction) or `remove`
    /// (promotion) carry no hot∩cold key, so the sweep trigger must consult
    /// this in addition to the orphan-key set — otherwise those files are never
    /// reclaimed on ticks where no key is hot-shadowed. A held file needs the
    /// sweep too: the sweep is what releases it once a fold has committed.
    pub fn has_pending_unlink(&self) -> bool {
        !self.pending_unlink.is_empty() || !self.hold.is_empty()
    }

    /// How many zero-ref files are queued for unlink — `INFO MoonStore`'s
    /// `cold_files_pending_unlink` (moon#656).
    ///
    /// The level behind [`Self::has_pending_unlink`]'s boolean. A number that
    /// stays high across sweeps means files are being orphaned faster than the
    /// sweep reclaims them, which is invisible from a boolean.
    ///
    /// Held files (moon#1231) count too: they are zero-ref and waiting, for a
    /// committed fold rather than for the sweep.
    #[inline]
    pub fn pending_unlink_len(&self) -> usize {
        self.pending_unlink.len() + self.hold.len()
    }

    /// Hand the next unlink decision a fresh reading of the shard's AOF fold
    /// state (moon#1231). The orphan sweep calls this right before each of its
    /// sweeps whenever the process has an AOF writer; see
    /// [`super::unlink_hold`] for the rule it enables.
    pub fn observe_fold(&mut self, view: super::unlink_hold::FoldView) {
        self.hold.observe(view);
    }

    /// Drop the view [`Self::observe_fold`] recorded without deciding
    /// anything: no later unlink decision may use it (moon#1279's boot view).
    pub fn end_fold_decision(&mut self) {
        self.hold.end_decision();
    }

    /// Whether `file_id` is held until a committed fold covers it (moon#1231).
    pub fn is_unlink_held(&self, file_id: u64) -> bool {
        self.hold.is_held(file_id)
    }

    /// How many distinct heap files still hold at least one live cold key —
    /// `INFO MoonStore`'s `cold_files_referenced` (moon#656).
    ///
    /// Deliberately NOT the number of heap files on disk: a file is unlinked
    /// only after its LAST live key goes away (see [`Self::file_refs`]), so
    /// subtracting this from the manifest's live `KvLeaf` file count is the
    /// dead-file figure at the granularity reclaim actually operates on.
    #[inline]
    pub fn referenced_file_count(&self) -> usize {
        self.file_refs.len()
    }

    /// Iterate over all cold entries as `(key, location)` pairs.
    ///
    /// Used by the orphan sweeper to walk all entries without taking ownership.
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &ColdLocation)> {
        self.map.iter().map(|(k, loc)| (&k.1, loc))
    }

    /// Hash-ordered iteration starting at `from_h48` in the SCAN cursor's
    /// 48-bit hash space (#368 cold-plane range resume). Yields
    /// `(hash48, key, location)` ascending by `(hash48, key)`; the caller
    /// applies liveness/shadow filters and stops after COUNT candidates —
    /// O(log n) to seek plus O(items yielded).
    pub fn range_from(&self, from_h48: u64) -> impl Iterator<Item = (u64, &Bytes, &ColdLocation)> {
        self.map
            .range((from_h48, Bytes::new())..)
            .map(|(k, loc)| (k.0, &k.1, loc))
    }

    /// Sweep cold entries that are shadowed by a live hot key.
    ///
    /// # Orphan definition
    ///
    /// A cold entry is an **orphan** when `db.is_hot(key)` returns `true` —
    /// the key was re-written to the in-memory DashTable after being spilled.
    /// The cold DataFile is now stale and wastes disk space.
    ///
    /// We deliberately do **NOT** treat "absent from hot tier" as an orphan.
    /// A cold-only entry (present in cold index, absent from DashTable) is the
    /// normal live state for a spilled key — deleting it would destroy user data.
    ///
    /// # Concurrency safety
    ///
    /// The caller must hold the shard write lock for the duration of this call.
    /// - Spill runs under the same write lock, so a spill-in-progress cannot
    ///   race with sweep — the entry won't appear in `cold_index` until spill
    ///   commits, and by then the write lock is held by the sweeper.
    /// - Cold reads (read path) take the shard read lock; the write lock blocks
    ///   them, so no reader can observe a partial deletion.
    ///
    /// # Parameters
    ///
    /// - `db`: read-only reference to the current shard Database (for `is_hot`).
    /// - `shard_dir`: the shard's storage directory (used to locate DataFiles).
    /// - `manifest`: optional shard manifest to tombstone orphaned file entries.
    ///   If `None`, file deletion still proceeds but no manifest update is made
    ///   (useful when disk-offload is enabled without a manifest).
    ///
    /// # Returns
    ///
    /// `Ok(SweepStats)` with counts and bytes reclaimed, or an `io::Error` if
    /// manifest commit fails (individual file-deletion errors are logged and
    /// counted as reclaimed if the index entry is removed regardless).
    /// Sweep cold entries that are shadowed by a live hot key.
    ///
    /// # Orphan definition
    ///
    /// A cold entry is an **orphan** when `db.is_hot(key)` returns `true` —
    /// the key was re-written to the in-memory DashTable after being spilled.
    /// The cold DataFile is now stale and wastes disk space.
    ///
    /// We deliberately do **NOT** treat "absent from hot tier" as an orphan.
    /// A cold-only entry (present in cold index, absent from DashTable) is the
    /// normal live state for a spilled key — deleting it would destroy user data.
    ///
    /// # Concurrency safety
    ///
    /// The caller must hold the shard write lock for the duration of this call.
    /// Spill, read-promotion, and eviction all run under the same write lock,
    /// preventing TOCTOU races with the sweep.
    ///
    /// # Two-phase design
    ///
    /// When `ColdIndex` is a field of `Database` the caller cannot borrow both
    /// `&mut cold_index` and `&db` at the same time. In that case, use the
    /// two-phase pattern with [`sweep_known_orphans`]:
    ///
    /// 1. Collect orphan keys: `ci.iter().filter(|(k,_)| db.is_hot(k)).map(…).collect()`
    /// 2. Delete: `ci.sweep_known_orphans(keys, shard_dir, manifest)`
    ///
    /// This method is the one-shot API for tests where `ColdIndex` is held
    /// separately from `Database`.
    pub fn orphan_sweep(
        &mut self,
        db: &crate::storage::db::Database,
        shard_dir: &Path,
        manifest: Option<&mut crate::persistence::manifest::ShardManifest>,
    ) -> std::io::Result<SweepStats> {
        // Phase 1: identify orphans — immutable borrow of self.map only.
        let orphan_keys: Vec<Bytes> = self
            .map
            .keys()
            .map(|k| &k.1)
            .filter(|key| db.is_hot(key))
            .cloned()
            .collect();

        // Phase 2: delete (immutable db ref not held anymore).
        self.sweep_known_orphans(orphan_keys, shard_dir, manifest)
    }

    /// Delete a pre-identified set of orphan keys from the cold index and disk.
    ///
    /// This is the second phase of the two-phase orphan sweep used when
    /// `ColdIndex` and `Database` share ownership (the caller cannot pass both
    /// `&mut self` and `&Database` simultaneously because the cold index is a
    /// field of Database). The caller computes `orphan_keys` externally:
    ///
    /// ```ignore
    /// // Phase 1: collect while we can borrow db immutably
    /// let orphan_keys: Vec<Bytes> = guard
    ///     .cold_index.as_ref().unwrap()
    ///     .iter()
    ///     .filter(|(k, _)| guard.is_hot(k))
    ///     .map(|(k, _)| k.clone())
    ///     .collect();
    /// // Phase 2: delete (only cold_index is &mut, no db borrow needed)
    /// guard.cold_index.as_mut().unwrap()
    ///     .sweep_known_orphans(orphan_keys, shard_dir, manifest)
    /// ```
    ///
    /// This two-phase approach is also used by unit tests that keep the
    /// `ColdIndex` separate from `Database`.
    pub fn sweep_known_orphans(
        &mut self,
        orphan_keys: Vec<Bytes>,
        shard_dir: &Path,
        manifest: Option<&mut crate::persistence::manifest::ShardManifest>,
    ) -> std::io::Result<SweepStats> {
        let mut stats = SweepStats::default();

        // Phase 1: remove orphan ENTRIES and decrement their files' ref counts.
        // A file becomes an unlink candidate only when its LAST live ref is
        // removed — NEVER on a single orphan key while co-located keys in the
        // same batched `.mpf` still reference it (the data-loss bug this fixes).
        for key in &orphan_keys {
            if let Some(old) = self.remove_raw(key.as_ref()) {
                stats.entries_reclaimed += 1;
                self.resident_bytes = self
                    .resident_bytes
                    .saturating_sub(cold_entry_cost(key.len()));
                if self.ref_dec(old.file_id) {
                    self.pending_unlink.push(old.file_id);
                }
            }
        }

        // Phase 2: unlink files that now have zero live refs (off the hot path).
        stats.bytes_reclaimed = self.drain_pending_unlink(shard_dir, manifest)?;

        if stats.entries_reclaimed > 0 || stats.bytes_reclaimed > 0 {
            tracing::info!(
                entries = stats.entries_reclaimed,
                bytes = stats.bytes_reclaimed,
                "orphan_sweep: completed",
            );
        }

        Ok(stats)
    }

    /// Sweep cold entries whose TTL has passed WITHOUT ever being re-read
    /// (R1, H-2: `tmp/OFFLOAD-COMPRESSION-REVIEW.md`).
    ///
    /// The on-read reclaim in `cold_read.rs` only fires when a caller
    /// actually issues a `GET` against an expired cold key. A key that
    /// expires and is never touched again previously leaked its index entry
    /// (RAM, full key bytes) and pinned its file's refcount (disk) forever —
    /// unbounded growth for the flagship offload use case (TTL'd sessions,
    /// caches). This sweep judges expiry from [`ColdLocation::ttl_ms`] alone,
    /// so it never reads the cold file itself.
    ///
    /// # Bounded work (design-for-failure)
    ///
    /// Scans at most `max_batch` expired entries per call, so an expiry storm
    /// against a very large cold index cannot turn one sweep tick into an
    /// unbounded stall of the shard event loop. If more expired entries
    /// remain than fit in this call's batch, a `tracing::warn!` is emitted
    /// and the remainder is picked up by the next scheduled sweep tick — no
    /// entry is skipped permanently, reclamation is just spread across ticks.
    ///
    /// # Concurrency safety
    ///
    /// Same contract as [`Self::orphan_sweep`]: the caller must have
    /// exclusive access to this shard's database for the duration of the
    /// call (in practice: invoked from the shard's own single-threaded event
    /// loop via `with_shard_db`, never from another thread). Spill and
    /// read-promotion cannot race a sweep because they all run through the
    /// same per-shard exclusivity.
    ///
    /// # Crash / partial-sweep safety
    ///
    /// Idempotent and safe to interrupt at any point:
    /// - If the process crashes between removing an index entry (Phase 1)
    ///   and the file unlink committing (Phase 2, [`Self::drain_pending_unlink`]),
    ///   the in-RAM index entry is gone but the file may still be on disk with
    ///   a stale or already-tombstoned manifest entry. Recovery rebuilds the
    ///   index from the manifest + heap files ([`Self::rebuild_from_manifest`])
    ///   and re-derives `ttl_ms` fresh from the on-disk `KvEntry` — an already
    ///   expired-at-crash-time entry is simply expired again on the next
    ///   sweep after restart. No corruption, no double free: `drain_pending_unlink`
    ///   unlinks the file (idempotent — `NotFound` is treated as already-done)
    ///   BEFORE the manifest commit, so a crash there leaves, at worst, a
    ///   manifest entry pointing at an already-vanished file, which recovery
    ///   already tolerates (`rebuild_from_manifest` skips unreadable files).
    /// - A key that gets promoted back to hot (via a concurrent `GET`) or
    ///   re-spilled between Phase 1's scan and Phase 2's removal cannot
    ///   happen under the single-threaded-per-shard model this sweep runs
    ///   under — there is no window for that race to open.
    pub fn sweep_expired(
        &mut self,
        now_ms: u64,
        shard_dir: &Path,
        manifest: Option<&mut crate::persistence::manifest::ShardManifest>,
        max_batch: usize,
    ) -> std::io::Result<SweepStats> {
        // Dead slots whose own TTL has passed since they were recorded can no
        // longer come back: stop holding their keys (moon#1215 ledger, PR
        // #1233 review). O(1) unless one has actually expired.
        self.dead.prune_expired(now_ms);

        // Phase 1: identify expired keys, bounded to `max_batch`. Iteration
        // is in (hash48, key) order and restarts from the front each call;
        // an entry left behind by the cap is picked up by a later sweep once
        // earlier expired entries are reclaimed — it is never permanently
        // skipped (reclaimed entries stop occupying batch slots).
        let mut expired_keys: Vec<Bytes> = Vec::new();
        let mut more_remain = false;
        for (k, loc) in self.map.iter() {
            if loc.ttl_ms.is_some_and(|t| now_ms > t) {
                if expired_keys.len() >= max_batch {
                    more_remain = true;
                    break;
                }
                expired_keys.push(k.1.clone());
            }
        }

        if more_remain {
            tracing::warn!(
                capped_at = max_batch,
                "cold_expired_sweep: more TTL-expired entries remain than fit this tick's \
                 batch cap; deferring the remainder to the next sweep tick",
            );
        }

        if expired_keys.is_empty() {
            // Nothing to unlink: the fold view (moon#1231) dies with this
            // sweep rather than serve a later decision stale.
            self.hold.end_decision();
            return Ok(SweepStats::default());
        }

        // Phase 2: remove entries + decrement their files' ref counts (same
        // shape as `sweep_known_orphans`). Not recorded as dead slots: each
        // one's own TTL has passed, so it can never read as a value again
        // (see `dead_slots`).
        let mut stats = SweepStats::default();
        for key in &expired_keys {
            if let Some((_, old)) = self.remove_raw_unrecorded(key.as_ref()) {
                // moon#1013: a tracked key can be spilled and then expire on
                // disk; its trackers must hear about it like a hot expiry.
                crate::tracking::invalidation::invalidate_server_removed(key.as_ref());
                stats.entries_reclaimed += 1;
                self.resident_bytes = self
                    .resident_bytes
                    .saturating_sub(cold_entry_cost(key.len()));
                if self.ref_dec(old.file_id) {
                    self.pending_unlink.push(old.file_id);
                }
            }
        }

        // Phase 3: unlink now-zero-ref files (off the hot path).
        stats.bytes_reclaimed = self.drain_pending_unlink(shard_dir, manifest)?;

        if stats.entries_reclaimed > 0 {
            crate::command::info_reclamation::record_cold_expired_reclaim(
                stats.entries_reclaimed as u64,
                stats.bytes_reclaimed,
            );
            tracing::info!(
                entries = stats.entries_reclaimed,
                bytes = stats.bytes_reclaimed,
                "cold_expired_sweep: reclaimed TTL-expired cold entries that were never re-read",
            );
        }

        Ok(stats)
    }

    /// Unlink every `pending_unlink` file that still has zero live refs,
    /// tombstone its manifest entry, and commit once. Returns bytes reclaimed.
    ///
    /// A file is queued only on a zero-ref transition, but a later `insert`
    /// could re-reference the same `file_id` before the drain runs (file ids are
    /// minted monotonically per process, so this is defensive); such files are
    /// skipped. The `.mpf` is unlinked BEFORE the manifest commit, so disk is
    /// freed even when the commit fails (e.g. manifest root overflow at >70
    /// entries) — the commit error is surfaced only after best-effort
    /// reclamation, and a file whose unlink itself errors is re-queued for a
    /// later sweep rather than leaked.
    ///
    /// moon#1231: which queued files go now, which are held until a committed
    /// fold covers them, and which held files that fold releases, is
    /// [`super::unlink_hold::UnlinkHold::admit`]'s decision.
    fn drain_pending_unlink(
        &mut self,
        shard_dir: &Path,
        manifest: Option<&mut crate::persistence::manifest::ShardManifest>,
    ) -> std::io::Result<u64> {
        if self.pending_unlink.is_empty() && self.hold.is_empty() {
            self.hold.end_decision();
            return Ok(0);
        }
        let queued = std::mem::take(&mut self.pending_unlink);
        // A listed file the boot rebuild found missing — a manifest whose
        // tombstone commit a crash lost (the reclaim's deferred one,
        // moon#1240, or this sweep's own unlink-then-commit) — has nothing
        // left for the hold to protect: the rebuild indexed none of its keys.
        // Retire its manifest entry at the first drain rather than after a
        // committed fold, so the rebuild counts it (`files_missing`, a
        // `DEGRADED` summary line) on one boot, not on every boot until a
        // fold (moon#1231 review). Only THOSE ids, and only if still missing:
        // a file that is merely unreachable while a live sweep runs (moon#875:
        // a remount, an operator `mv` — the bytes come back) goes through the
        // hold like any other, or a crash before the next fold would lose the
        // keys the committed generation reads there (review 5).
        let rebuilt_missing = std::mem::take(&mut self.missing_at_rebuild);
        let (gone, queued): (Vec<u64>, Vec<u64>) = if rebuilt_missing.is_empty() {
            (Vec::new(), queued)
        } else {
            let data_dir = shard_dir.join("data");
            queued.into_iter().partition(|&file_id| {
                rebuilt_missing.contains(&file_id)
                    && !self.file_refs.contains_key(&file_id)
                    && matches!(
                        std::fs::symlink_metadata(data_dir.join(format!("heap-{file_id:06}.mpf"))),
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound
                    )
            })
        };
        let refs = &self.file_refs;
        self.hold
            .forget_referenced(|file_id| refs.contains_key(&file_id));
        let mut admitted = self.hold.admit(queued);
        self.pending_unlink.extend(admitted.requeue);
        admitted.unlink.extend(gone);
        if admitted.unlink.is_empty() {
            return Ok(0);
        }
        self.unlink_queued(admitted.unlink, shard_dir, manifest, false)
    }

    /// [`Self::drain_pending_unlink`] for exactly the queued files among
    /// `file_ids`; every other queued file stays queued for the orphan
    /// sweep. The reclaim uses it to unlink the files it just emptied
    /// without advancing the sweep's own schedule for anything else.
    ///
    /// Its tombstone commit is DEFERRED (moon#1240: the reclaim runs from the
    /// shard's tick and must not wait for an fsync): a lost one leaves a
    /// listed-but-missing file, which recovery counts (`files_missing`) and
    /// retires — the same window the orphan sweep's unlink-then-commit has.
    ///
    /// It bypasses the moon#1231 hold, including for a file already held:
    /// the caller must know that no replayable generation reads the file —
    /// the reclaim's adoption does, for an old file each of whose live slots
    /// has a durable copy below the committed cut.
    pub fn unlink_now(
        &mut self,
        file_ids: &[u64],
        shard_dir: &Path,
        manifest: Option<&mut crate::persistence::manifest::ShardManifest>,
    ) -> std::io::Result<u64> {
        let (mut now, later): (Vec<u64>, Vec<u64>) = std::mem::take(&mut self.pending_unlink)
            .into_iter()
            .partition(|id| file_ids.contains(id));
        self.pending_unlink = later;
        now.extend(self.hold.take(file_ids));
        if now.is_empty() {
            return Ok(0);
        }
        self.unlink_queued(now, shard_dir, manifest, true)
    }

    fn unlink_queued(
        &mut self,
        mut queued: Vec<u64>,
        shard_dir: &Path,
        mut manifest: Option<&mut crate::persistence::manifest::ShardManifest>,
        deferred_commit: bool,
    ) -> std::io::Result<u64> {
        let data_dir = shard_dir.join("data");
        queued.sort_unstable();
        queued.dedup();

        let mut bytes_reclaimed: u64 = 0;
        let mut manifest_dirty = false;
        for file_id in queued {
            // A live ref re-appeared after queueing -> keep the file.
            if self.file_refs.contains_key(&file_id) {
                continue;
            }
            let file_path = data_dir.join(format!("heap-{:06}.mpf", file_id));
            let file_bytes = std::fs::metadata(&file_path).map(|m| m.len()).unwrap_or(0);

            // Delete the DataFile (idempotent — missing = already gone).
            match std::fs::remove_file(&file_path) {
                // Gone from disk: its dead slots cannot come back (moon#1215).
                Ok(()) => {
                    self.dead.forget_file(file_id);
                    self.graves.forget_file(file_id);
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    self.dead.forget_file(file_id);
                    self.graves.forget_file(file_id);
                }
                Err(e) => {
                    tracing::warn!(
                        file = %file_path.display(),
                        err = %e,
                        "orphan_sweep: failed to delete cold DataFile — will retry",
                    );
                    // Re-queue rather than leak; a later sweep retries.
                    self.pending_unlink.push(file_id);
                    continue;
                }
            }

            // Tombstone manifest entry so GC / recovery doesn't re-index it.
            if let Some(ref mut m) = manifest.as_deref_mut() {
                m.remove_file(file_id, crate::persistence::page::PageType::KvLeaf);
                manifest_dirty = true;
            }

            bytes_reclaimed = bytes_reclaimed.saturating_add(file_bytes);
            crate::command::info_reclamation::record_cold_orphan_reclaim(file_bytes);

            tracing::debug!(
                file_id,
                bytes = file_bytes,
                "orphan_sweep: reclaimed zero-ref cold DataFile",
            );
        }

        // Single manifest commit for all tombstones in this drain.
        if manifest_dirty {
            if let Some(m) = manifest {
                let committed = if deferred_commit {
                    m.commit_deferred()
                } else {
                    m.commit()
                };
                if let Err(e) = committed {
                    tracing::error!(err = %e, "orphan_sweep: manifest commit failed");
                    return Err(e);
                }
            }
        }

        Ok(bytes_reclaimed)
    }
}

#[cfg(test)]
mod tests;

mod rebuild;
