//! Boot rebuild of the cold index from the manifest's spill files (split
//! out of `cold_index.rs`, moon#1281 — the file-size rule). The rules — a
//! key's newest copy wins, older copies are kept for a gated replay, nothing
//! is skipped silently — are documented on each function.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use bytes::Bytes;

use super::{
    ColdIndex, ColdLocation, ColdRebuild, ColdRebuildReport, PageVerdict, REBUILD_DETAIL_LOG_CAP,
    classify_page, cold_entry_cost, scan_h48,
};

impl ColdIndex {
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
        Self::rebuild_from_manifest_per_db_with_graves(shard_dir, manifest, None)
    }

    /// [`Self::rebuild_from_manifest_per_db`], dropping every slot `graves`
    /// names (moon#1281: the dead slots the loaded no-AOF snapshot carried)
    /// BEFORE each key's newest copy is chosen — so a dead newer slot can
    /// never shadow a live older one, and a key whose every slot is dead is
    /// simply absent. The dropped slots stay on disk while their file backs
    /// other keys, so they are re-recorded in the rebuilt index's
    /// [`crate::storage::tiered::slot_graves::SlotGraves`]: the next snapshot must carry them
    /// again. A listed file left with no live slot is queued for unlink (the
    /// snapshot hold keeps it until a later snapshot commits).
    pub fn rebuild_from_manifest_per_db_with_graves(
        shard_dir: &Path,
        manifest: &crate::persistence::manifest::ShardManifest,
        graves: Option<&crate::persistence::snapshot::cold_graves::ColdGraves>,
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
        // moon#1281: the grave slots dropped per db, `(file_id, packed slot)`,
        // re-recorded into that db's rebuilt index below.
        let mut buried_per_db: Vec<(usize, Vec<(u64, u64)>)> = Vec::new();
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
                                 if the orphan sweep or the cold reclaim's adoption unlinked it \
                                 and a crash lost the manifest tombstone that followed (nothing \
                                 lost; this boot's DEGRADED summary then counts it too); \
                                 otherwise the file was removed externally. Queued so the first \
                                 sweep retires the manifest entry"
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
                        if graves.is_some_and(|g| g.contains(file_id, page_idx as u32, slot_idx)) {
                            report.entries_tombstoned += 1;
                            let packed = crate::persistence::snapshot::cold_graves::pack_slot(
                                page_idx as u32,
                                slot_idx,
                            );
                            match buried_per_db.iter_mut().find(|(d, _)| *d == db) {
                                Some((_, v)) => v.push((file_id, packed)),
                                None => buried_per_db.push((db, vec![(file_id, packed)])),
                            }
                            continue;
                        }
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
        let extra_dbs = missing_per_db.iter().map(|(d, _)| *d);
        for db in extra_dbs.chain(buried_per_db.iter().map(|(d, _)| *d)) {
            if !per_db.iter().any(|(d, _)| *d == db) {
                per_db.push((db, Vec::new()));
            }
        }
        let per_db = per_db
            .into_iter()
            .map(|(db, pairs)| {
                let mut index = Self::from_pairs_newest_wins(pairs);
                if let Some((_, ids)) = missing_per_db.iter().find(|(d, _)| *d == db) {
                    index.pending_unlink.extend_from_slice(ids);
                    index.missing_at_rebuild.extend_from_slice(ids);
                }
                if let Some((_, buried)) = buried_per_db.iter().find(|(d, _)| *d == db) {
                    let mut files = std::collections::HashSet::new();
                    for &(file_id, packed) in buried {
                        index.graves.note_unconditionally(file_id, packed);
                        files.insert(file_id);
                    }
                    for file_id in files {
                        if !index.file_refs.contains_key(&file_id)
                            && !index.pending_unlink.contains(&file_id)
                        {
                            index.pending_unlink.push(file_id);
                        }
                    }
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
            missing_at_rebuild: Vec::new(),
            resident_bytes,
            older_copies,
            dead: crate::storage::tiered::dead_slots::DeadSlots::default(),
            graves: crate::storage::tiered::slot_graves::SlotGraves::default(),
            reclaim: crate::storage::tiered::cold_reclaim::ReclaimState::default(),
            hold: crate::storage::tiered::unlink_hold::UnlinkHold::default(),
        }
    }
}
