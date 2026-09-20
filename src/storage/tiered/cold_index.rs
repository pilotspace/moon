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
fn scan_h48(key: &[u8]) -> u64 {
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
    map: BTreeMap<(u64, Bytes), ColdLocation>,
    /// Reverse liveness: `file_id` -> count of live `map` entries pointing at it.
    ///
    /// A batched spill file (`heap-NNNNNN.mpf`) holds up to `FLUSH_ENTRY_CAP`
    /// (256) KVs across its pages, so it is safe to unlink ONLY when this count
    /// reaches 0 (no co-located live key still references it). Deleting a file
    /// on a single orphan key silently orphans its co-located keys — observed
    /// empirically as cold read-through collapsing 200/200 -> 88/200 once the
    /// orphan sweep runs.
    file_refs: HashMap<u64, u32>,
    /// `file_id`s that dropped to zero live refs (via an `insert` overwrite or a
    /// `remove`) and are awaiting unlink. Drained off the hot path by the orphan
    /// sweep ([`Self::drain_pending_unlink`]). Pushed only on a zero-ref
    /// transition (rare), so it does not allocate on the common insert path.
    pending_unlink: Vec<u64>,
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
    resident_bytes: usize,
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
    older_copies: HashMap<Bytes, Vec<ColdLocation>>,
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
fn cold_entry_cost(key_len: usize) -> usize {
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
enum PageVerdict {
    Leaf,
    Overflow,
    BadHeader,
    ForeignType,
    BadChecksum,
}

/// Classify one page-sized chunk by its header BEFORE `KvLeafPage::from_bytes`
/// gets a say — that constructor answers `None` for a valid overflow page and
/// for a corrupt one alike, and only one of those is a loss.
fn classify_page(chunk: &[u8]) -> PageVerdict {
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
            resident_bytes: 0,
            older_copies: HashMap::new(),
        }
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
        for location in older.into_values().flatten() {
            if self.ref_dec(location.file_id) {
                self.pending_unlink.push(location.file_id);
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
        let Some(copies) = self.older_copies.remove(key) else {
            return;
        };
        for location in copies {
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
        self.resident_bytes
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
        if let Some(old) = self.map.insert((h, key), location) {
            if old.file_id != new_file {
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
    fn remove_raw(&mut self, key: &[u8]) -> Option<ColdLocation> {
        let h = scan_h48(key);
        let owned = self
            .map
            .range((h, Bytes::new())..)
            .take_while(|(k, _)| k.0 == h)
            .find(|(k, _)| k.1.as_ref() == key)
            .map(|(k, _)| k.clone())?;
        self.map.remove(&owned)
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
        self.map.clear();
        self.older_copies = HashMap::new();
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

    /// Whether any zero-ref files are queued for unlink by the next sweep.
    ///
    /// Files orphaned by `insert` overwrite (re-eviction) or `remove`
    /// (promotion) carry no hot∩cold key, so the sweep trigger must consult
    /// this in addition to the orphan-key set — otherwise those files are never
    /// reclaimed on ticks where no key is hot-shadowed.
    pub fn has_pending_unlink(&self) -> bool {
        !self.pending_unlink.is_empty()
    }

    /// How many zero-ref files are queued for unlink — `INFO MoonStore`'s
    /// `cold_files_pending_unlink` (moon#656).
    ///
    /// The level behind [`Self::has_pending_unlink`]'s boolean. A number that
    /// stays high across sweeps means files are being orphaned faster than the
    /// sweep reclaims them, which is invisible from a boolean.
    #[inline]
    pub fn pending_unlink_len(&self) -> usize {
        self.pending_unlink.len()
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
            return Ok(SweepStats::default());
        }

        // Phase 2: remove entries + decrement their files' ref counts (same
        // shape as `sweep_known_orphans`).
        let mut stats = SweepStats::default();
        for key in &expired_keys {
            if let Some(old) = self.remove_raw(key.as_ref()) {
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
    fn drain_pending_unlink(
        &mut self,
        shard_dir: &Path,
        mut manifest: Option<&mut crate::persistence::manifest::ShardManifest>,
    ) -> std::io::Result<u64> {
        if self.pending_unlink.is_empty() {
            return Ok(0);
        }
        let data_dir = shard_dir.join("data");
        let mut queued = std::mem::take(&mut self.pending_unlink);
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
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
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
                if let Err(e) = m.commit() {
                    tracing::error!(err = %e, "orphan_sweep: manifest commit failed");
                    return Err(e);
                }
            }
        }

        Ok(bytes_reclaimed)
    }

    /// Rebuild the cold index from all heap DataFiles in a shard directory.
    ///
    /// Scans manifest for KvLeaf entries, reads each DataFile, and populates
    /// the index. Called during v3 recovery.
    pub fn rebuild_from_manifest(
        shard_dir: &Path,
        manifest: &crate::persistence::manifest::ShardManifest,
    ) -> Self {
        let mut merged = Self::new();
        for (_db, index) in Self::rebuild_from_manifest_per_db(shard_dir, manifest).per_db {
            merged.merge(index);
        }
        merged
    }

    /// Rebuild one cold index PER LOGICAL DATABASE from the manifest's
    /// KvLeaf files (#139). Each spill file is attributed wholesale via
    /// `FileEntry::db_index` — the spill path guarantees single-db files
    /// (flush chunks cut at db boundaries), and manifests from before the
    /// field existed read as db 0, matching their actual (db-blind,
    /// attach-to-db0) provenance. Returns `(db_index, index)` pairs for
    /// every db that has at least one recovered entry (or a missing file
    /// queued for manifest retirement), plus the [`ColdRebuildReport`].
    ///
    /// Nothing is skipped silently (moon#875): every file, page or entry
    /// this cannot recover is counted in the report by cause, and the first
    /// [`REBUILD_DETAIL_LOG_CAP`] of each cause are logged here with the
    /// `file_id` (and page) so an operator can find the damage. The caller
    /// owns the one-line summary. See [`ColdRebuildReport`] for what each
    /// class means and why none of them aborts the rebuild.
    pub fn rebuild_from_manifest_per_db(
        shard_dir: &Path,
        manifest: &crate::persistence::manifest::ShardManifest,
    ) -> ColdRebuild {
        use crate::persistence::manifest::FileStatus;
        use crate::persistence::page::{PAGE_4K, PageType};

        let mut report = ColdRebuildReport::default();
        // Files the manifest lists as Active whose bytes are gone (NotFound),
        // per db, queued onto the rebuilt index's `pending_unlink` so the
        // next orphan sweep retires the manifest entry.
        let mut missing_per_db: Vec<(usize, Vec<u64>)> = Vec::new();

        // Pass 1 — decode every Active KvLeaf file into a flat per-db pair
        // vector. Manifest order is merely the order the files are READ in;
        // it decides nothing (moon#983 — see `ColdLocation::recency_key` for
        // why it cannot be trusted to). Nothing is inserted into a `BTreeMap`
        // here: an ordered map fed one random-ordered key at a time pays an
        // O(log n) descent plus node splits per key, and that insert loop
        // measured 75% of this whole function's wall time on a real 466,912-
        // entry / 114 MiB spill corpus (file I/O was 13%, the page copy 2%,
        // the CRC32C verify 3%, entry decode 8%). Pass 2 replaces it with one
        // sort + one bulk load.
        //
        // The cost of that is a transient: this vector holds every recovered
        // pair (~80 B each) until its db's map is built, on top of the map
        // itself. Recovery is single-threaded and pre-accept, so the peak is
        // this shard's alone — but it IS proportional to the cold index, so
        // see the measured RSS note in `from_pairs_newest_wins`.
        let mut per_db: Vec<(usize, Vec<((u64, Bytes), ColdLocation)>)> = Vec::new();
        let data_dir = shard_dir.join("data");

        for entry in manifest.files() {
            if entry.status == FileStatus::Active && entry.file_type == PageType::KvLeaf as u8 {
                let db = entry.db_index as usize;
                let pairs = match per_db.iter_mut().find(|(d, _)| *d == db) {
                    Some((_, p)) => p,
                    None => {
                        per_db.push((db, Vec::new()));
                        #[allow(clippy::unwrap_used)] // pushed on the previous line
                        let last = per_db.last_mut().unwrap();
                        &mut last.1
                    }
                };
                let heap_path = data_dir.join(format!("heap-{:06}.mpf", entry.file_id));
                let file_id = entry.file_id;
                report.files_attempted += 1;
                // Read raw bytes and iterate by absolute chunk index.
                // `read_datafile` skips overflow pages (returns only KvLeaf pages),
                // so its enumerate index ≠ file-absolute page index in multi-page files.
                // We must use the raw chunk index to produce a correct `page_idx`.
                let raw = match std::fs::read(&heap_path) {
                    Ok(b) => b,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        report.files_missing += 1;
                        if report.files_missing <= REBUILD_DETAIL_LOG_CAP {
                            tracing::warn!(
                                file_id,
                                db,
                                path = %heap_path.display(),
                                "cold recovery: manifest lists an Active heap file that is not \
                                 on disk; its keys (if any were live) now read as absent. Benign \
                                 if the orphan sweep unlinked it before committing the manifest \
                                 (nothing lost); otherwise the file was removed externally. \
                                 Queued so the sweep retires the manifest entry"
                            );
                        }
                        match missing_per_db.iter_mut().find(|(d, _)| *d == db) {
                            Some((_, ids)) => ids.push(file_id),
                            None => missing_per_db.push((db, vec![file_id])),
                        }
                        continue;
                    }
                    Err(e) => {
                        report.files_unreadable += 1;
                        if report.files_unreadable <= REBUILD_DETAIL_LOG_CAP {
                            tracing::error!(
                                file_id,
                                db,
                                path = %heap_path.display(),
                                err = %e,
                                "cold recovery: heap file could not be read; every key in it \
                                 (up to 256) now reads as ABSENT until the file is readable and \
                                 the server restarts. The index entry is skipped, not tombstoned"
                            );
                        }
                        continue;
                    }
                };
                report.files_read += 1;
                if entry.byte_size > 0 && (raw.len() as u64) < entry.byte_size {
                    report.files_short += 1;
                    report.short_file_bytes += entry.byte_size - raw.len() as u64;
                    if report.files_short <= REBUILD_DETAIL_LOG_CAP {
                        tracing::error!(
                            file_id,
                            db,
                            path = %heap_path.display(),
                            on_disk = raw.len(),
                            manifest = entry.byte_size,
                            "cold recovery: heap file is shorter than its manifest entry; the \
                             keys in the missing tail now read as ABSENT"
                        );
                    }
                }
                let mut chunks = raw.chunks_exact(PAGE_4K);
                for (page_idx, chunk) in chunks.by_ref().enumerate() {
                    report.pages_scanned += 1;
                    let verdict = classify_page(chunk);
                    match verdict {
                        PageVerdict::Leaf => {}
                        PageVerdict::Overflow => {
                            report.pages_overflow += 1;
                            continue;
                        }
                        PageVerdict::BadHeader
                        | PageVerdict::ForeignType
                        | PageVerdict::BadChecksum => {
                            report.pages_rejected += 1;
                            if report.pages_rejected <= REBUILD_DETAIL_LOG_CAP {
                                tracing::error!(
                                    file_id,
                                    db,
                                    path = %heap_path.display(),
                                    page_idx,
                                    reason = ?verdict,
                                    "cold recovery: heap page rejected (corrupt); every key in \
                                     it now reads as ABSENT"
                                );
                            }
                            continue;
                        }
                    }
                    let mut buf = [0u8; PAGE_4K];
                    buf.copy_from_slice(chunk);
                    // `classify_page` already proved type + CRC; `from_bytes`
                    // re-checks them, cheaply enough for a boot path.
                    let Some(page) = crate::persistence::kv_page::KvLeafPage::from_bytes(buf)
                    else {
                        continue;
                    };
                    for slot_idx in 0..page.slot_count() {
                        let Some(kv) = page.get(slot_idx) else {
                            report.entries_rejected += 1;
                            if report.entries_rejected <= REBUILD_DETAIL_LOG_CAP {
                                tracing::error!(
                                    file_id,
                                    db,
                                    path = %heap_path.display(),
                                    page_idx,
                                    slot_idx,
                                    "cold recovery: slot in a valid heap page did not decode \
                                     (unknown value type — a downgrade?); that key now reads \
                                     as ABSENT"
                                );
                            }
                            continue;
                        };
                        report.entries_recovered += 1;
                        let key = Bytes::from(kv.key);
                        pairs.push((
                            (scan_h48(&key), key),
                            ColdLocation {
                                file_id,
                                page_idx: page_idx as u32,
                                slot_idx,
                                ttl_ms: kv.ttl_ms,
                                value_type: kv.value_type,
                            },
                        ));
                    }
                }
                let tail = chunks.remainder().len() as u64;
                if tail > 0 {
                    report.partial_page_bytes += tail;
                    tracing::error!(
                        file_id,
                        db,
                        path = %heap_path.display(),
                        bytes = tail,
                        "cold recovery: heap file ends in a partial page; the writer never \
                         produces one, so the file was damaged after it was written. The \
                         keys in that page now read as ABSENT"
                    );
                }
            }
        }

        // Pass 2 — bulk-load each db's pairs into its ordered map, resolving
        // every duplicated key to its newest on-disk copy. A db that recovered
        // nothing but has missing files to retire still gets an (empty) index
        // so the queue has somewhere to live.
        for (db, _) in &missing_per_db {
            if !per_db.iter().any(|(d, _)| d == db) {
                per_db.push((*db, Vec::new()));
            }
        }
        let per_db = per_db
            .into_iter()
            .map(|(db, pairs)| {
                let mut index = Self::from_pairs_newest_wins(pairs);
                if let Some((_, ids)) = missing_per_db.iter().find(|(d, _)| *d == db) {
                    index.pending_unlink.extend_from_slice(ids);
                }
                (db, index)
            })
            .collect();
        ColdRebuild { per_db, report }
    }

    /// Build an index from `((scan_h48(key), key), location)` pairs in ANY
    /// order, resolving a key present more than once to the copy with the
    /// highest [`ColdLocation::recency_key`] — the copy written last.
    ///
    /// This is the same answer the live path arrives at: a re-spill runs
    /// through [`Self::insert`] after its predecessor, so the later
    /// `file_id` overwrites the earlier one. The live path gets that order
    /// for free from time; recovery reads files in manifest order, which is
    /// push order and is NOT guaranteed to be recency order (moon#983). So
    /// the winner is chosen here, explicitly, from the location itself —
    /// never from where its file happened to sit in the input.
    ///
    /// Mechanically: sort by key ascending and recency DESCENDING, then keep
    /// the first of each equal-key run. The survivor is the newest copy by
    /// construction, the sorted, duplicate-free vector bulk-loads into the
    /// `BTreeMap` in one bottom-up pass (`FromIterator` re-sorts, but a
    /// sorted input is its fast path), and nothing depends on which of two
    /// equal keys a collection type happens to keep.
    ///
    /// The derived state is recomputed from the finished map rather than
    /// maintained incrementally:
    /// - `file_refs` — one live-entry count per `file_id`, by definition equal
    ///   to a walk of the final map.
    /// - `resident_bytes` — [`Self::insert`] charges [`cold_entry_cost`] once
    ///   per *distinct* key (an overwrite is free), i.e. the same sum.
    /// - `pending_unlink` — a file lands here exactly when every copy it holds
    ///   lost to a newer one, i.e. exactly when it is mentioned by the input
    ///   but referenced by no surviving map entry. The *set* is identical to
    ///   what a per-key insert loop would queue; the order within it is not
    ///   specified by either path (it only sequences a later orphan sweep's
    ///   unlinks).
    fn from_pairs_newest_wins(mut pairs: Vec<((u64, Bytes), ColdLocation)>) -> Self {
        if pairs.is_empty() {
            return Self::new();
        }

        // Every file_id the input mentions, in first-appearance order.
        let mut seen_files: Vec<u64> = Vec::new();
        let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for (_, loc) in &pairs {
            if seen.insert(loc.file_id) {
                seen_files.push(loc.file_id);
            }
        }

        // Key ascending, then newest copy FIRST — so the dedup below, which
        // keeps the first of each run, keeps the newest.
        pairs.sort_unstable_by(|a, b| {
            a.0.cmp(&b.0)
                .then_with(|| b.1.recency_key().cmp(&a.1.recency_key()))
        });
        // Keep the first (newest) of each equal-key run in the map; the rest
        // are the older copies a gated replay may still need (moon#1140).
        let mut older_copies: HashMap<Bytes, Vec<ColdLocation>> = HashMap::new();
        let mut newest: Vec<((u64, Bytes), ColdLocation)> = Vec::with_capacity(pairs.len());
        for (key, loc) in pairs {
            match newest.last() {
                Some((prev, _)) if *prev == key => {
                    older_copies.entry(key.1).or_default().push(loc);
                }
                _ => newest.push((key, loc)),
            }
        }

        let map: BTreeMap<(u64, Bytes), ColdLocation> = newest.into_iter().collect();

        let mut file_refs: HashMap<u64, u32> = HashMap::with_capacity(seen_files.len());
        let mut resident_bytes = 0usize;
        for ((_, key), loc) in &map {
            *file_refs.entry(loc.file_id).or_insert(0) += 1;
            resident_bytes += cold_entry_cost(key.len());
        }
        // A retained copy is a referrer too: a gated replay may read it, so
        // its file must not be unlinked underneath that read. The reference
        // is given back by `release_older_copies`, which queues the file then
        // if nothing else holds it. `resident_bytes` is deliberately NOT
        // charged — it accounts one cost per distinct key, and these keys are
        // already counted by the map entry in front of them.
        for location in older_copies.values().flatten() {
            *file_refs.entry(location.file_id).or_insert(0) += 1;
        }

        let pending_unlink: Vec<u64> = seen_files
            .into_iter()
            .filter(|f| !file_refs.contains_key(f))
            .collect();

        Self {
            map,
            file_refs,
            pending_unlink,
            resident_bytes,
            older_copies,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loc_in(file_id: u64, slot: u16) -> ColdLocation {
        ColdLocation {
            file_id,
            page_idx: 0,
            slot_idx: slot,
            ttl_ms: None,
            value_type: crate::persistence::kv_page::ValueType::String,
        }
    }

    /// moon#656: `INFO MoonStore` reports the cold tier's file-level shape,
    /// and the two counters it needs are already maintained here — they were
    /// simply not readable from outside this module.
    #[test]
    fn referenced_file_count_tracks_files_with_at_least_one_live_key() {
        let mut idx = ColdIndex::new();
        assert_eq!(
            idx.referenced_file_count(),
            0,
            "empty index references no file"
        );

        // Two keys co-located in one file, one key in another: two files,
        // three keys. A batched spill file holds up to FLUSH_ENTRY_CAP keys,
        // so file count and key count are genuinely independent numbers —
        // which is the whole point of reporting both.
        idx.insert(Bytes::from_static(b"a"), loc_in(7, 0));
        idx.insert(Bytes::from_static(b"b"), loc_in(7, 1));
        idx.insert(Bytes::from_static(b"c"), loc_in(9, 0));
        assert_eq!(idx.referenced_file_count(), 2);
        assert_eq!(idx.len(), 3);

        // Removing one of two co-located keys must NOT drop the file: its
        // sibling still lives there. This is the invariant whose violation
        // once collapsed cold read-through 200/200 -> 88/200.
        idx.remove(b"a");
        assert_eq!(idx.referenced_file_count(), 2, "file 7 still holds key b");

        idx.remove(b"b");
        assert_eq!(idx.referenced_file_count(), 1, "file 7 is now unreferenced");
    }

    /// The count reported as `cold_files_pending_unlink` — files whose last
    /// live reference dropped and which the sweep has not yet unlinked.
    #[test]
    fn pending_unlink_len_counts_files_awaiting_the_sweep() {
        let mut idx = ColdIndex::new();
        assert_eq!(idx.pending_unlink_len(), 0);
        assert!(!idx.has_pending_unlink());

        idx.insert(Bytes::from_static(b"a"), loc_in(7, 0));
        idx.insert(Bytes::from_static(b"c"), loc_in(9, 0));
        assert_eq!(
            idx.pending_unlink_len(),
            0,
            "both files are still referenced"
        );

        idx.remove(b"a");
        assert_eq!(idx.pending_unlink_len(), 1, "file 7 dropped to zero refs");
        assert!(idx.has_pending_unlink());

        idx.remove(b"c");
        assert_eq!(idx.pending_unlink_len(), 2);

        // The existing boolean and the new count must never disagree — the
        // sweep trigger reads one and INFO reports the other.
        assert_eq!(idx.has_pending_unlink(), idx.pending_unlink_len() > 0);
    }

    #[test]
    fn test_cold_index_insert_lookup_remove() {
        let mut idx = ColdIndex::new();
        let loc = ColdLocation {
            file_id: 1,
            page_idx: 0,
            slot_idx: 0,
            ttl_ms: None,
            value_type: crate::persistence::kv_page::ValueType::String,
        };
        idx.insert(Bytes::from_static(b"key1"), loc);
        assert_eq!(idx.len(), 1);
        let found = idx.lookup(b"key1").unwrap();
        assert_eq!(found.file_id, 1);
        assert_eq!(found.page_idx, 0);
        assert_eq!(found.slot_idx, 0);
        idx.remove(b"key1");
        assert!(idx.lookup(b"key1").is_none());
    }

    #[test]
    fn range_from_is_hash_ordered_and_resumable() {
        // 200 entries paged via range_from must drain every key exactly
        // once in ascending (hash48, key) order — the #368 cold-plane
        // contract SCAN's merged page walk depends on.
        let mut idx = ColdIndex::new();
        let loc = ColdLocation {
            file_id: 1,
            page_idx: 0,
            slot_idx: 0,
            ttl_ms: None,
            value_type: crate::persistence::kv_page::ValueType::String,
        };
        for i in 0..200 {
            idx.insert(Bytes::from(format!("rk:{i}")), loc);
        }
        let mut seen: std::collections::HashSet<Bytes> = std::collections::HashSet::new();
        let mut cursor = 0u64;
        loop {
            let page: Vec<(u64, Bytes)> = idx
                .range_from(cursor)
                .take(16)
                .map(|(h, k, _)| (h, k.clone()))
                .collect();
            if page.is_empty() {
                break;
            }
            let mut prev: Option<(u64, &Bytes)> = None;
            for (h, k) in &page {
                assert_eq!(*h, scan_h48(k.as_ref()), "stored hash must match key");
                assert!(*h >= cursor, "entry below resume point");
                if let Some((ph, pk)) = prev {
                    assert!((ph, pk) < (*h, k), "not ascending by (hash, key)");
                }
                prev = Some((*h, k));
                assert!(seen.insert(k.clone()), "duplicate across pages");
            }
            #[allow(clippy::unwrap_used)] // page verified non-empty above
            let last = page.last().unwrap().0;
            cursor = last + 1;
        }
        assert_eq!(seen.len(), 200, "every entry exactly once");
    }

    #[test]
    fn lookup_and_remove_survive_equal_hash_range_probe() {
        // lookup/remove go through an equal-hash range probe now; two keys
        // in the index must stay independently addressable, and removing
        // one must not disturb the other.
        let mut idx = ColdIndex::new();
        let mk = |fid| ColdLocation {
            file_id: fid,
            page_idx: 0,
            slot_idx: 0,
            ttl_ms: None,
            value_type: crate::persistence::kv_page::ValueType::String,
        };
        idx.insert(Bytes::from_static(b"alpha"), mk(1));
        idx.insert(Bytes::from_static(b"beta"), mk(2));
        assert_eq!(idx.lookup(b"alpha").map(|l| l.file_id), Some(1));
        assert_eq!(idx.lookup(b"beta").map(|l| l.file_id), Some(2));
        assert!(!idx.remove(b"missing"));
        assert!(idx.remove(b"alpha"));
        assert!(idx.lookup(b"alpha").is_none());
        assert_eq!(idx.lookup(b"beta").map(|l| l.file_id), Some(2));
        assert_eq!(idx.len(), 1);
    }

    // ── K4 accounting spine: resident_bytes O(1) accumulator ─────────────

    #[test]
    fn resident_bytes_zero_when_empty() {
        assert_eq!(ColdIndex::new().resident_bytes(), 0);
    }

    #[test]
    fn resident_bytes_grows_on_insert_shrinks_on_remove() {
        let mut idx = ColdIndex::new();
        let loc = ColdLocation {
            file_id: 1,
            page_idx: 0,
            slot_idx: 0,
            ttl_ms: None,
            value_type: crate::persistence::kv_page::ValueType::String,
        };
        idx.insert(Bytes::from_static(b"a_reasonably_long_key"), loc);
        let after_one = idx.resident_bytes();
        assert!(after_one > 0);

        idx.insert(Bytes::from_static(b"another_key"), loc);
        assert!(idx.resident_bytes() > after_one);

        idx.remove(b"another_key");
        assert_eq!(idx.resident_bytes(), after_one);

        idx.remove(b"a_reasonably_long_key");
        assert_eq!(idx.resident_bytes(), 0);
    }

    #[test]
    fn resident_bytes_overwrite_same_key_does_not_double_count() {
        let mut idx = ColdIndex::new();
        let loc_a = ColdLocation {
            file_id: 1,
            page_idx: 0,
            slot_idx: 0,
            ttl_ms: None,
            value_type: crate::persistence::kv_page::ValueType::String,
        };
        let loc_b = ColdLocation {
            file_id: 2,
            page_idx: 0,
            slot_idx: 1,
            ttl_ms: None,
            value_type: crate::persistence::kv_page::ValueType::String,
        };
        idx.insert(Bytes::from_static(b"key1"), loc_a);
        let after_first = idx.resident_bytes();
        // Overwrite the SAME key with a different location (re-eviction to a
        // different file). Byte length is unchanged, so resident_bytes must
        // not grow.
        idx.insert(Bytes::from_static(b"key1"), loc_b);
        assert_eq!(idx.resident_bytes(), after_first);
    }

    #[test]
    fn resident_bytes_zero_after_clear_all() {
        let mut idx = ColdIndex::new();
        let loc = ColdLocation {
            file_id: 1,
            page_idx: 0,
            slot_idx: 0,
            ttl_ms: None,
            value_type: crate::persistence::kv_page::ValueType::String,
        };
        idx.insert(Bytes::from_static(b"key1"), loc);
        idx.insert(Bytes::from_static(b"key2"), loc);
        assert!(idx.resident_bytes() > 0);
        idx.clear_all();
        assert_eq!(idx.resident_bytes(), 0);
    }

    /// Create a shard dir with a `data/` subdir and a dummy heap-NNNNNN.mpf
    /// file standing in for a batched multi-KV spill file.
    fn make_shard_with_heap(file_ids: &[u64]) -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        for &fid in file_ids {
            let p = data_dir.join(format!("heap-{:06}.mpf", fid));
            std::fs::write(&p, vec![0xABu8; 4096]).unwrap();
        }
        tmp
    }

    fn heap_path(shard_dir: &Path, file_id: u64) -> std::path::PathBuf {
        shard_dir
            .join("data")
            .join(format!("heap-{:06}.mpf", file_id))
    }

    /// REGRESSION (batch-file shared-deletion data loss): sweeping ONE orphan
    /// key must NOT delete its `.mpf` while a co-located live key still
    /// references the same file. Under spill batching a single file holds up
    /// to 256 KVs; deleting it on one orphan key silently orphans the rest
    /// (observed empirically as cold read-through 200/200 -> 88/200).
    #[test]
    fn test_sweep_retains_file_with_colocated_live_key() {
        let tmp = make_shard_with_heap(&[5]);
        let shard_dir = tmp.path();

        let mut ci = ColdIndex::new();
        // Two keys co-located in the SAME batched file (file_id = 5).
        ci.insert(
            Bytes::from_static(b"k_orphan"),
            ColdLocation {
                file_id: 5,
                page_idx: 0,
                slot_idx: 0,
                ttl_ms: None,
                value_type: crate::persistence::kv_page::ValueType::String,
            },
        );
        ci.insert(
            Bytes::from_static(b"k_live"),
            ColdLocation {
                file_id: 5,
                page_idx: 0,
                slot_idx: 1,
                ttl_ms: None,
                value_type: crate::persistence::kv_page::ValueType::String,
            },
        );

        let before_sweep = ci.resident_bytes();

        // Sweep ONLY the orphan key.
        ci.sweep_known_orphans(vec![Bytes::from_static(b"k_orphan")], shard_dir, None)
            .unwrap();

        // K4: resident_bytes must shrink by exactly the orphan's cost, via
        // the direct `self.map.remove` inside `sweep_known_orphans` (the
        // bypass site that does NOT go through the public `remove()`).
        assert!(
            ci.resident_bytes() < before_sweep,
            "sweeping an orphan must shrink resident_bytes"
        );

        // The co-located live key must remain resolvable AND its file present.
        assert!(
            ci.lookup(b"k_live").is_some(),
            "co-located live key dropped from cold index",
        );
        assert!(
            heap_path(shard_dir, 5).exists(),
            "DATA LOSS: file holding a live co-located key was deleted",
        );
        // The orphan entry itself is gone.
        assert!(ci.lookup(b"k_orphan").is_none(), "orphan entry not removed");
    }

    /// A batched file is unlinked only once its LAST live ref is removed.
    #[test]
    fn test_sweep_deletes_file_only_when_last_ref_removed() {
        let tmp = make_shard_with_heap(&[7]);
        let shard_dir = tmp.path();

        let mut ci = ColdIndex::new();
        ci.insert(
            Bytes::from_static(b"k1"),
            ColdLocation {
                file_id: 7,
                page_idx: 0,
                slot_idx: 0,
                ttl_ms: None,
                value_type: crate::persistence::kv_page::ValueType::String,
            },
        );
        ci.insert(
            Bytes::from_static(b"k2"),
            ColdLocation {
                file_id: 7,
                page_idx: 0,
                slot_idx: 1,
                ttl_ms: None,
                value_type: crate::persistence::kv_page::ValueType::String,
            },
        );

        // Sweep k1: one ref remains (k2) -> file MUST survive.
        ci.sweep_known_orphans(vec![Bytes::from_static(b"k1")], shard_dir, None)
            .unwrap();
        assert!(
            heap_path(shard_dir, 7).exists(),
            "file deleted while k2 still references it",
        );

        // Sweep k2: last ref removed -> file now reclaimed.
        ci.sweep_known_orphans(vec![Bytes::from_static(b"k2")], shard_dir, None)
            .unwrap();
        assert!(
            !heap_path(shard_dir, 7).exists(),
            "file not reclaimed after its last ref was swept",
        );
    }

    /// Re-eviction churn: `insert` overwriting a key's location (old file_id ->
    /// new file_id) drops the old file to zero refs. The hot∩cold sweep can
    /// never see this file (no key references it anymore), so the index must
    /// enqueue it for reclamation and a subsequent sweep must unlink it.
    #[test]
    fn test_overwrite_reclaims_orphaned_old_file() {
        let tmp = make_shard_with_heap(&[10, 11]);
        let shard_dir = tmp.path();

        let mut ci = ColdIndex::new();
        ci.insert(
            Bytes::from_static(b"k"),
            ColdLocation {
                file_id: 10,
                page_idx: 0,
                slot_idx: 0,
                ttl_ms: None,
                value_type: crate::persistence::kv_page::ValueType::String,
            },
        );
        // Key re-spilled to a NEW file (re-eviction) -> file 10 orphaned.
        ci.insert(
            Bytes::from_static(b"k"),
            ColdLocation {
                file_id: 11,
                page_idx: 0,
                slot_idx: 0,
                ttl_ms: None,
                value_type: crate::persistence::kv_page::ValueType::String,
            },
        );

        // A sweep with NO orphan keys must still drain the pending unlink.
        ci.sweep_known_orphans(vec![], shard_dir, None).unwrap();

        assert!(
            !heap_path(shard_dir, 10).exists(),
            "orphaned old file (10) not reclaimed after overwrite",
        );
        assert!(
            heap_path(shard_dir, 11).exists(),
            "live file (11) wrongly deleted",
        );
        assert_eq!(ci.lookup(b"k").map(|l| l.file_id), Some(11));
    }

    // ── R1 / H-2: proactive TTL-expiry sweep ────────────────────────────────

    /// RED->GREEN: a cold entry whose TTL has passed and which is NEVER
    /// re-read (no GET touches it — the on-read reclaim path never fires)
    /// must still be reclaimed by `sweep_expired`. This is the exact leak
    /// described in tmp/OFFLOAD-COMPRESSION-REVIEW.md R1: before this fix,
    /// nothing else in the system ever looks at a cold entry's TTL except a
    /// read that never comes.
    #[test]
    fn test_sweep_expired_reclaims_never_read_entry() {
        let tmp = make_shard_with_heap(&[20]);
        let shard_dir = tmp.path();

        let mut ci = ColdIndex::new();
        ci.insert(
            Bytes::from_static(b"stale_session"),
            ColdLocation {
                file_id: 20,
                page_idx: 0,
                slot_idx: 0,
                ttl_ms: Some(1_000), // expires at t=1000ms
                value_type: crate::persistence::kv_page::ValueType::String,
            },
        );

        assert!(ci.resident_bytes() > 0, "insert must charge resident_bytes");

        // Sweep strictly after expiry. The key was never read.
        let stats = ci
            .sweep_expired(2_000, shard_dir, None, MAX_EXPIRED_SWEEP_BATCH)
            .unwrap();

        assert_eq!(
            stats.entries_reclaimed, 1,
            "sweep must reclaim the never-read expired entry"
        );
        // K4: the direct `self.map.remove` bypass inside `sweep_expired`
        // must also decrement resident_bytes.
        assert_eq!(
            ci.resident_bytes(),
            0,
            "resident_bytes must drop to 0 once the only entry expires"
        );
        assert!(
            stats.bytes_reclaimed > 0,
            "sweep must reclaim the backing file's bytes (last live ref)"
        );
        assert!(
            ci.lookup(b"stale_session").is_none(),
            "index entry must be gone after sweep"
        );
        assert!(
            !heap_path(shard_dir, 20).exists(),
            "backing DataFile must be unlinked once its last live ref expires"
        );
    }

    /// A cold entry with no TTL (`ttl_ms: None`) or a TTL still in the future
    /// must NEVER be swept — only entries strictly past `now_ms` are expired.
    #[test]
    fn test_sweep_expired_ignores_live_entries() {
        let tmp = make_shard_with_heap(&[21]);
        let shard_dir = tmp.path();

        let mut ci = ColdIndex::new();
        ci.insert(
            Bytes::from_static(b"no_ttl"),
            ColdLocation {
                file_id: 21,
                page_idx: 0,
                slot_idx: 0,
                ttl_ms: None,
                value_type: crate::persistence::kv_page::ValueType::String,
            },
        );
        ci.insert(
            Bytes::from_static(b"future_ttl"),
            ColdLocation {
                file_id: 21,
                page_idx: 0,
                slot_idx: 1,
                ttl_ms: Some(5_000),
                value_type: crate::persistence::kv_page::ValueType::String,
            },
        );

        let stats = ci
            .sweep_expired(1_000, shard_dir, None, MAX_EXPIRED_SWEEP_BATCH)
            .unwrap();

        assert_eq!(stats.entries_reclaimed, 0, "no entry has expired yet");
        assert!(ci.lookup(b"no_ttl").is_some());
        assert!(ci.lookup(b"future_ttl").is_some());
        assert!(
            heap_path(shard_dir, 21).exists(),
            "file must survive when nothing has expired"
        );
    }

    /// Batch-file colocation must hold for the TTL sweep exactly as it does
    /// for the orphan-shadow sweep: an expired key must NOT drag down a
    /// co-located LIVE key's file. The file unlinks only once BOTH entries
    /// are gone (mirrors `test_sweep_deletes_file_only_when_last_ref_removed`).
    #[test]
    fn test_sweep_expired_retains_file_with_colocated_live_key() {
        let tmp = make_shard_with_heap(&[22]);
        let shard_dir = tmp.path();

        let mut ci = ColdIndex::new();
        ci.insert(
            Bytes::from_static(b"expired"),
            ColdLocation {
                file_id: 22,
                page_idx: 0,
                slot_idx: 0,
                ttl_ms: Some(1_000),
                value_type: crate::persistence::kv_page::ValueType::String,
            },
        );
        ci.insert(
            Bytes::from_static(b"still_live"),
            ColdLocation {
                file_id: 22,
                page_idx: 0,
                slot_idx: 1,
                ttl_ms: Some(9_999_000),
                value_type: crate::persistence::kv_page::ValueType::String,
            },
        );

        // First sweep: only "expired" is past TTL.
        let stats1 = ci
            .sweep_expired(2_000, shard_dir, None, MAX_EXPIRED_SWEEP_BATCH)
            .unwrap();
        assert_eq!(stats1.entries_reclaimed, 1);
        assert!(ci.lookup(b"expired").is_none());
        assert!(
            ci.lookup(b"still_live").is_some(),
            "co-located live-TTL key must remain resolvable"
        );
        assert!(
            heap_path(shard_dir, 22).exists(),
            "DATA LOSS: file holding a live co-located key was deleted"
        );

        // Second sweep, now past the second key's TTL too: last ref drops,
        // file must finally be reclaimed.
        let stats2 = ci
            .sweep_expired(9_999_001, shard_dir, None, MAX_EXPIRED_SWEEP_BATCH)
            .unwrap();
        assert_eq!(stats2.entries_reclaimed, 1);
        assert!(
            !heap_path(shard_dir, 22).exists(),
            "file must be reclaimed once its last live ref also expires"
        );
    }

    /// Design-for-failure: `max_batch` bounds one call's work. With more
    /// expired entries than the cap, a single call must reclaim at most
    /// `max_batch` of them (never zero, never more) — the remainder is
    /// picked up by a subsequent call, proving no per-tick unbounded stall.
    #[test]
    fn test_sweep_expired_respects_batch_cap() {
        let tmp = make_shard_with_heap(&[23]);
        let shard_dir = tmp.path();

        let mut ci = ColdIndex::new();
        for i in 0..10u16 {
            ci.insert(
                Bytes::from(format!("k{i}")),
                ColdLocation {
                    file_id: 23,
                    page_idx: 0,
                    slot_idx: i,
                    ttl_ms: Some(1_000),
                    value_type: crate::persistence::kv_page::ValueType::String,
                },
            );
        }
        assert_eq!(ci.len(), 10);

        // Cap at 3 per call.
        let stats1 = ci.sweep_expired(2_000, shard_dir, None, 3).unwrap();
        assert_eq!(
            stats1.entries_reclaimed, 3,
            "one call must reclaim exactly max_batch entries when more are expired"
        );
        assert_eq!(ci.len(), 7, "the other 7 must remain for the next sweep");

        // Draining the rest across further capped calls must eventually
        // reach zero — no entry is permanently skipped by the cap.
        let mut total_reclaimed = stats1.entries_reclaimed;
        for _ in 0..10 {
            if ci.len() == 0 {
                break;
            }
            let s = ci.sweep_expired(2_000, shard_dir, None, 3).unwrap();
            total_reclaimed += s.entries_reclaimed;
        }
        assert_eq!(
            ci.len(),
            0,
            "all 10 expired entries must eventually reclaim"
        );
        assert_eq!(total_reclaimed, 10);
    }

    /// A sweep call with nothing expired must be a true no-op: zero entries
    /// reclaimed, zero bytes, and it must not touch `pending_unlink` state
    /// belonging to unrelated (non-TTL) reclamation paths.
    #[test]
    fn test_sweep_expired_noop_when_nothing_expired() {
        let tmp = make_shard_with_heap(&[24]);
        let shard_dir = tmp.path();

        let mut ci = ColdIndex::new();
        ci.insert(
            Bytes::from_static(b"k"),
            ColdLocation {
                file_id: 24,
                page_idx: 0,
                slot_idx: 0,
                ttl_ms: None,
                value_type: crate::persistence::kv_page::ValueType::String,
            },
        );

        let stats = ci
            .sweep_expired(1_000, shard_dir, None, MAX_EXPIRED_SWEEP_BATCH)
            .unwrap();
        assert_eq!(stats.entries_reclaimed, 0);
        assert_eq!(stats.bytes_reclaimed, 0);
        assert!(!ci.has_pending_unlink());
        assert_eq!(ci.len(), 1);
    }
}
