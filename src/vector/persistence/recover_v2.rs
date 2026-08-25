//! Vector-index durability (B3): startup recovery into the LIVE
//! `ShardSlice.vector_store`.
//!
//! Supersedes the old WAL-replay `recovery.rs` module (deleted — it only
//! ever populated the `Shard`-owned `vector_store`, which is discarded
//! wholesale at `event_loop.rs`'s `_discarded_vector_store`). Recovery
//! authority is now: sidecar (`vector-indexes.meta`, index *definitions*) +
//! the B1/B2 manifest/segment/keymap durability layout (index *contents*),
//! reconciled against the live keyspace by a dedup rescan.
//!
//! See `tmp/VECTOR-DURABILITY-DESIGN.md` for the full design. This module
//! implements the "Recovery" section end to end; `event_loop.rs` only wires
//! [`RecoveryState`]'s three phase methods into its existing three call
//! sites (index-restore loop, rescan loop, post-scan finalize) because
//! `with_shard`/`with_shard_db` re-entrancy is forbidden — the orchestration
//! (which of those two functions to call, and when) has to stay there.
//!
//! ## Crash-safety degradation ladder (per segment)
//!
//! 1. Segment loads AND its `collection_id` matches the manifest's — used
//!    as-is.
//! 2. Segment loads but `collection_id` MISMATCHES the manifest (stale/
//!    tampered file) — the segment object is still in hand, so its exact
//!    `key_hash`es are read from its own MVCC headers and dropped from the
//!    loaded keymap. The rescan re-indexes exactly those keys; nothing else
//!    is touched.
//! 3. Segment fails to load at all (corrupt graph, bad checksum, I/O error)
//!    but `mvcc_headers.bin` alone still parses — same precise drop as (2),
//!    via [`segment_io::read_mvcc_headers_only`].
//! 4. Segment fails to load AND its headers are ALSO unreadable — the lost
//!    key_hashes can't be attributed. The only safe response is to abandon
//!    ALL recovered state for this index (no loaded segments, no loaded
//!    keymap) and let the full rescan rebuild everything: keeping the
//!    OTHER, successfully-loaded segments while blanking the keymap would
//!    let the rescan re-insert their keys fresh into the mutable segment
//!    while the old copies still live in those segments — duplicate search
//!    results, which the crash-safety contract (never wrong, only ever
//!    slower) forbids.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use tracing::{info, warn};

use crate::protocol::Frame;
use crate::text::store::TextStore;
use crate::vector::keymap::BucketedKeyMap;
use crate::vector::persistence::manifest::{self, IndexManifest};
use crate::vector::persistence::segment_io;
use crate::vector::segment::SegmentList;
use crate::vector::segment::immutable::ImmutableSegment;
use crate::vector::segment::mutable::MutableSegment;
use crate::vector::store::{IndexMeta, VectorStore};

/// Per-index counters for the B3 startup acceptance-signal log line.
#[derive(Default, Clone, Copy)]
pub struct IndexRecoveryCounters {
    pub loaded_segments: usize,
    pub verified_unchanged: usize,
    pub re_indexed: usize,
    pub tombstoned: usize,
}

/// Says "still moving" while a long recovery reconciles the keyspace.
///
/// moon#546: a production restart spent ~94 minutes inside the reconcile loop
/// and logged nothing at all between the sidecar-restore banner and the final
/// count. An operator reading that log cannot separate a slow repair from a
/// wedged process, and the two call for opposite actions.
///
/// The trigger is wall-clock, not a key count: the recoveries that need a line
/// are exactly the ones whose keys are individually slow, and a "every N keys"
/// rule stays silent through precisely those. The interval restarts from the
/// last line so a run that slows down mid-way doesn't earn a backlog of them.
pub(crate) struct RecoveryProgress {
    started: std::time::Instant,
    last: std::time::Instant,
    interval: std::time::Duration,
}

impl RecoveryProgress {
    pub(crate) fn new(interval: std::time::Duration, now: std::time::Instant) -> Self {
        Self {
            started: now,
            last: now,
            interval,
        }
    }

    /// Should a progress line be emitted now? Records the emission if so.
    ///
    /// `_done` is carried for the caller's message only; the decision is time-
    /// based by design (see the type docs).
    pub(crate) fn tick_at(&mut self, _done: u64, now: std::time::Instant) -> bool {
        if now.duration_since(self.last) < self.interval {
            return false;
        }
        self.last = now;
        true
    }

    /// Keys per second over the WHOLE run — the number an operator multiplies
    /// by the keys remaining to decide whether to keep waiting.
    pub(crate) fn keys_per_sec(&self, done: u64, now: std::time::Instant) -> f64 {
        let secs = now.duration_since(self.started).as_secs_f64();
        if secs <= 0.0 {
            return 0.0;
        }
        done as f64 / secs
    }

    /// Seconds since recovery started, for the progress line.
    pub(crate) fn elapsed_secs(&self, now: std::time::Instant) -> f64 {
        now.duration_since(self.started).as_secs_f64()
    }
}

/// Threaded through the whole B3 startup sequence by `event_loop.rs`.
///
/// Lifecycle: construct once, call [`Self::create_index`] once per sidecar
/// index definition, call [`Self::reconcile_key`] once per matching hash key
/// found during the keyspace scan, then call [`Self::finish`] exactly once.
/// All three phase methods assume they run from inside an already-open
/// `with_shard` closure — none of them call `with_shard`/`with_shard_db`
/// themselves (forbidden re-entrancy, see `crate::shard::slice`).
pub struct RecoveryState {
    /// Names of indexes that have durable state loaded from a manifest —
    /// eligible for the dedup rescan. Everything else (no manifest found,
    /// or manifest present but every segment + its headers were
    /// unreadable) is a plain fresh index and always takes the full path.
    recovered_names: HashSet<Bytes>,
    /// Snapshot of each recovered index's key_hash set exactly as loaded
    /// from the keymap, taken BEFORE the rescan mutates anything — the
    /// deletion probe's baseline.
    original_key_hashes: HashMap<Bytes, HashSet<u64>>,
    /// Accumulated during the rescan: key_hashes observed (still present in
    /// the keyspace, whether unchanged or changed) per recovered index.
    observed_key_hashes: HashMap<Bytes, HashSet<u64>>,
    /// `index_name_hex` of every index definition in the sidecar (recovered
    /// or not) — used by the cross-index orphan sweep to tell "a dropped
    /// index whose background dir-delete never finished" from "a live
    /// index whose manifest just hasn't been read yet".
    known_index_hexes: HashSet<String>,
    counters: HashMap<Bytes, IndexRecoveryCounters>,
}

impl RecoveryState {
    pub fn new() -> Self {
        Self {
            recovered_names: HashSet::new(),
            original_key_hashes: HashMap::new(),
            observed_key_hashes: HashMap::new(),
            known_index_hexes: HashSet::new(),
            counters: HashMap::new(),
        }
    }

    /// Phase 1: create one index from its sidecar definition. If a
    /// `manifest.json` exists under `<idx_persist_root>/idx-<hex>/`, pins
    /// the index's collection_id to the manifest's value and loads its
    /// segments + keymap (see module docs for the degradation ladder).
    /// Otherwise behaves exactly like the pre-B3 `create_index` restore.
    pub fn create_index(
        &mut self,
        vector_store: &mut VectorStore,
        idx_persist_root: &Path,
        meta: &IndexMeta,
    ) {
        let name = meta.name.clone();
        self.known_index_hexes
            .insert(manifest::index_name_hex(name.as_ref()));

        let idx_dir = manifest::index_persist_dir(idx_persist_root, name.as_ref());
        let loaded_manifest = manifest::read_manifest_tolerant(&idx_dir);

        let recovered_counters = match &loaded_manifest {
            Some(m) => {
                if let Err(e) = vector_store.create_index_with_collection_id(meta.clone(), m) {
                    warn!(
                        "vector index {}: failed to recreate with recovered collection_id: {e} \
                         — skipping this index entirely",
                        String::from_utf8_lossy(&name)
                    );
                    return;
                }
                load_segments_and_keymap(vector_store, name.as_ref(), &idx_dir, m)
            }
            None => {
                if let Err(e) = vector_store.create_index(meta.clone()) {
                    warn!(
                        "vector index {}: failed to restore from sidecar: {e}",
                        String::from_utf8_lossy(&name)
                    );
                }
                None
            }
        };

        if let Some(counters) = recovered_counters {
            self.recovered_names.insert(name.clone());
            self.counters.insert(name.clone(), counters);
        }
    }

    /// Phase 1.5: snapshot every recovered index's key_hash set as the
    /// deletion probe's baseline (see [`Self::finish`]). Call exactly once,
    /// after ALL of phase 1 (every [`Self::create_index`] call) AND after
    /// `VectorStore::register_warm_segments` has reattached any WARM
    /// segments — in that order — but BEFORE phase 2
    /// ([`Self::reconcile_key`]) begins.
    ///
    /// This is deliberately NOT folded into `create_index` itself (an
    /// earlier revision snapshotted right there, immediately after each
    /// index's Stack B load). `load_segments_and_keymap` only populates
    /// `key_hash_to_key` from LOADED (immutable/HOT) segments — a WARM
    /// segment's keys are phantom-dropped (see the `segment_resident` gate
    /// below) since nothing has attached it to `idx.segments` yet at that
    /// point. Snapshotting there would permanently exclude every WARM key
    /// from the deletion probe's baseline: a WARM key whose KV hash was
    /// deleted while the server was down would never be recognized as
    /// "used to exist, now doesn't" and would never get tombstoned. Calling
    /// this AFTER `register_warm_segments` (which does populate
    /// `key_hash_to_key` for cleanly-attached WARM keys — see its own docs)
    /// makes the baseline complete.
    pub fn snapshot_recovered_baseline(&mut self, vector_store: &VectorStore) {
        for name in &self.recovered_names {
            if let Some(idx) = vector_store.get_index(name) {
                self.original_key_hashes
                    .insert(name.clone(), idx.key_hash_to_key.keys().copied().collect());
            }
        }
    }

    /// Phase 2: reconcile one hash key that matches at least one vector or
    /// text index prefix (caller already filtered this — see
    /// `event_loop.rs`'s `collect_matching`).
    ///
    /// For every VECTOR index this key matches, decide whether its default
    /// vector field is provably unchanged since the last snapshot
    /// (checksum match against a recovered index's keymap). Only when
    /// *every* matching index agrees "unchanged" does the default vector
    /// field get stripped from `args` before delegating to
    /// `auto_index_hset_public` (routing it into the existing
    /// metadata-only rebuild path — no HNSW/TQ re-encode). A single
    /// disagreement (unknown key, missing/zero checksum — the documented
    /// multi-field gap, mismatched checksum, or an index with no durable
    /// state at all) falls back to the full path for every index matching
    /// this key: conservative by design, per the crash-safety contract
    /// (never wrong, only occasionally slower).
    ///
    /// Text-only matches are unaffected either way — `auto_index_hset`
    /// always re-derives text/TAG/NUMERIC from `args[1..]`, and stripping
    /// only ever removes a vector field's (name, value) pair.
    /// WS5a round 4 (adversarial review, CRITICAL): `db_index` scopes
    /// `find_matching_index_names_for_db` and the `auto_index_hset_public`
    /// delegate below. Without it, a restart's dedup rescan would silently
    /// RE-INTRODUCE the exact cross-db auto-index leak fixed on the live
    /// write path (`auto_index_hset` in `src/shard/spsc_handler.rs`) on
    /// every reboot — `event_loop.rs` already collects matching keys
    /// per-db (`for db_idx in 0..db_count`) before calling this, so the
    /// caller has always had the right value; it just wasn't threaded in.
    pub fn reconcile_key(
        &mut self,
        vector_store: &mut VectorStore,
        text_store: &mut TextStore,
        key: &[u8],
        args: &[Frame],
        db_index: u8,
    ) {
        let matching_vector = vector_store.find_matching_index_names_for_db(key, db_index);
        let key_hash = xxhash_rust::xxh64::xxh64(key, 0);

        let mut any_recovered_checked = false;
        let mut all_unchanged = true;
        let mut dirty_recovered: Vec<Bytes> = Vec::new();

        for idx_name in &matching_vector {
            if !self.recovered_names.contains(idx_name) {
                // Fresh (no durable state) index matching this key — can't
                // vouch for "unchanged" at all; forces the full path below.
                all_unchanged = false;
                continue;
            }
            self.observed_key_hashes
                .entry(idx_name.clone())
                .or_default()
                .insert(key_hash);
            any_recovered_checked = true;

            let Some(idx) = vector_store.get_index(idx_name) else {
                continue;
            };
            let known = idx.key_hash_to_global_id.contains_key(&key_hash);
            let stored_checksum = idx.key_hash_to_vec_checksum.get(&key_hash).copied();
            let default_field = idx.meta.vector_fields[0].field_name.clone();
            let dim = idx.meta.vector_fields[0].dimension as usize;
            let live_checksum =
                crate::shard::spsc_handler::find_vector_blob(args, &default_field, dim)
                    .map(|blob| xxhash_rust::xxh64::xxh64(blob, 0));

            // `stored_checksum == Some(0)` is the documented multi-field gap
            // (additional fields never write a checksum) — treated as
            // "changed" (safe re-encode), same as unknown/missing.
            let unchanged = matches!(
                (known, stored_checksum, live_checksum),
                (true, Some(stored), Some(live)) if stored != 0 && stored == live
            );
            if !unchanged {
                all_unchanged = false;
                if known {
                    dirty_recovered.push(idx_name.clone());
                }
            }
        }

        // Known-and-different indexes: tombstone the stale copy BEFORE the
        // (necessarily full, since all_unchanged is now false) re-encode
        // below inserts the new one.
        for idx_name in &dirty_recovered {
            vector_store.mark_deleted_for_key_in_index(idx_name, key);
        }

        if any_recovered_checked && all_unchanged {
            let stripped = strip_default_vector_fields(&matching_vector, vector_store, args);
            let inserted = crate::shard::spsc_handler::auto_index_hset_public(
                vector_store,
                text_store,
                key,
                &stripped,
                db_index,
            );
            debug_assert!(
                inserted.is_empty(),
                "B3 dedup rescan: stripped args must never re-encode a vector"
            );
            for idx_name in &matching_vector {
                if let Some(c) = self.counters.get_mut(idx_name) {
                    c.verified_unchanged += 1;
                }
            }
        } else {
            let _ = crate::shard::spsc_handler::auto_index_hset_public(
                vector_store,
                text_store,
                key,
                args,
                db_index,
            );
            for idx_name in &matching_vector {
                if self.recovered_names.contains(idx_name) {
                    if let Some(c) = self.counters.get_mut(idx_name) {
                        c.re_indexed += 1;
                    }
                }
            }
        }
    }

    /// Phase 3: deletion probe + orphan sweep + acceptance-signal log
    /// lines. Call exactly once, after the full keyspace scan.
    pub fn finish(self, vector_store: &mut VectorStore, idx_persist_root: &Path) {
        // Deletion probe: any key_hash the manifest's keymap loaded that was
        // never observed during the rescan no longer exists anywhere in the
        // keyspace (a plain DEL/UNLINK, or an HDEL of the vector field that
        // removed the whole matching hash) — tombstone it. Read the key
        // bytes from the index's OWN (still-live) key map first: entries
        // for keys the rescan already touched (verified-unchanged or
        // re-indexed) are irrelevant here (they WERE observed), so a
        // shrunk-but-still-present entry is fine.
        let mut counters = self.counters;
        for (name, original) in &self.original_key_hashes {
            let observed = self.observed_key_hashes.get(name);
            let mut to_delete: Vec<Vec<u8>> = Vec::new();
            if let Some(idx) = vector_store.get_index(name) {
                for kh in original {
                    if observed.is_some_and(|o| o.contains(kh)) {
                        continue;
                    }
                    if let Some(key_bytes) = idx.key_hash_to_key.get(kh) {
                        to_delete.push(key_bytes.to_vec());
                    }
                }
            }
            for key_bytes in &to_delete {
                vector_store.mark_deleted_for_key_in_index(name, key_bytes);
            }
            if !to_delete.is_empty() {
                if let Some(c) = counters.get_mut(name) {
                    c.tombstoned += to_delete.len();
                }
            }
        }

        // Per-index orphan sweep (segment-*/staging-*/keymap-* not
        // referenced by the manifest that was actually loaded).
        for name in &self.recovered_names {
            let idx_dir = manifest::index_persist_dir(idx_persist_root, name);
            if let Some(m) = manifest::read_manifest_tolerant(&idx_dir) {
                if let Err(e) = manifest::sweep_orphans_from_disk(&idx_dir, &m) {
                    warn!(
                        "vector index {}: orphan sweep failed: {e}",
                        String::from_utf8_lossy(name)
                    );
                }
            }
        }

        // Cross-index sweep: `idx-<hex>` dirs on disk with no matching
        // sidecar index at all — e.g. a dropped index whose best-effort
        // background directory delete never completed before a crash.
        sweep_unknown_index_dirs(idx_persist_root, &self.known_index_hexes);

        for (name, c) in &counters {
            info!(
                "vector index {}: B3 recovery — loaded {} segment(s), {} key(s) verified \
                 unchanged, {} re-indexed, {} tombstoned",
                String::from_utf8_lossy(name),
                c.loaded_segments,
                c.verified_unchanged,
                c.re_indexed,
                c.tombstoned,
            );
        }
    }
}

impl Default for RecoveryState {
    fn default() -> Self {
        Self::new()
    }
}

/// Load `manifest`'s segments + keymap into the (already pinned-cid-created)
/// index named `name`. Returns `None` only when a segment failure's
/// key_hashes could not be attributed at all (see module docs, degradation
/// level 4) — the index still exists (fresh/empty, pinned cid) but carries
/// no recovered state, so the caller treats it as NOT recovered (full
/// rescan for all of its matching keys).
fn load_segments_and_keymap(
    vector_store: &mut VectorStore,
    name: &[u8],
    idx_dir: &Path,
    manifest: &IndexManifest,
) -> Option<IndexRecoveryCounters> {
    let mut immutable: Vec<Arc<ImmutableSegment>> = Vec::with_capacity(manifest.segment_ids.len());
    let mut dropped_key_hashes: HashSet<u64> = HashSet::new();

    for &segment_id in &manifest.segment_ids {
        match segment_io::read_immutable_segment(idx_dir, segment_id) {
            Ok((seg, collection)) if collection.collection_id == manifest.collection_id => {
                immutable.push(Arc::new(seg.with_disk_segment_id(Some(segment_id))));
            }
            Ok((seg, collection)) => {
                // Degradation level 2: loaded fine, but its collection_id
                // (hence QJL seed) doesn't match this recovery epoch's
                // manifest — never install it (would search wrong/garbage
                // distances). The segment object is in hand, so precise
                // attribution needs no fallback.
                warn!(
                    "vector index {}: segment-{segment_id} collection_id {} != manifest \
                     collection_id {} — treating as corrupt, its keys will be re-indexed",
                    String::from_utf8_lossy(name),
                    collection.collection_id,
                    manifest.collection_id
                );
                for h in seg.mvcc_headers() {
                    dropped_key_hashes.insert(h.key_hash);
                }
            }
            Err(e) => {
                warn!(
                    "vector index {}: segment-{segment_id} failed to load: {e} — \
                     attempting header-only key attribution",
                    String::from_utf8_lossy(name)
                );
                match segment_io::read_mvcc_headers_only(idx_dir, segment_id) {
                    Some(headers) => {
                        for h in headers {
                            dropped_key_hashes.insert(h.key_hash);
                        }
                    }
                    None => {
                        // Degradation level 4: can't attribute this
                        // segment's keys at all. Keeping the OTHER,
                        // successfully-loaded segments while blanking the
                        // keymap would let the rescan re-insert their keys
                        // fresh alongside the old copies still living in
                        // those segments — duplicate search results.
                        // Abandon ALL recovered state for this index.
                        warn!(
                            "vector index {}: segment-{segment_id} headers ALSO unreadable — \
                             cannot attribute its keys; abandoning all recovered segments/keymap \
                             for this index (falling back to a full rescan) to avoid \
                             duplicate/stale search results",
                            String::from_utf8_lossy(name)
                        );
                        return None;
                    }
                }
            }
        }
    }

    // Segment-membership gate: the durable keymap covers EVERY indexed key
    // (mutable + immutable) at snapshot-submit time, so after a crash it can
    // be a strict SUPERSET of the durable segments — any key that was still
    // mutable-resident when the last snapshot committed (or whose freshly
    // installed segment's snapshot job never ran before the kill) has a
    // keymap entry with a perfectly matching checksum but NO doc in any
    // loaded segment. Loading such a phantom entry would make the B3 dedup
    // rescan "verify" the key as unchanged and silently drop its document.
    // Only keys live in a successfully loaded segment may enter the
    // recovered maps; everything else stays unknown → full re-index from
    // the AOF rescan.
    let mut segment_resident: HashSet<u64> = HashSet::new();
    for seg in &immutable {
        segment_resident.extend(seg.live_key_hashes());
    }

    let entries =
        manifest::read_keymap_tolerant(idx_dir, manifest.keymap_epoch).unwrap_or_default();
    let mut key_hash_to_key = BucketedKeyMap::new();
    let mut key_hash_to_global_id = BucketedKeyMap::new();
    let mut key_hash_to_vec_checksum = BucketedKeyMap::new();
    let mut phantom_entries = 0usize;
    for e in entries {
        if dropped_key_hashes.contains(&e.key_hash) {
            continue;
        }
        if !segment_resident.contains(&e.key_hash) {
            phantom_entries += 1;
            continue;
        }
        key_hash_to_key.insert(e.key_hash, e.key);
        key_hash_to_global_id.insert(e.key_hash, e.global_id);
        key_hash_to_vec_checksum.insert(e.key_hash, e.vec_checksum);
    }
    if phantom_entries > 0 {
        info!(
            "vector index {}: {} keymap entr{} not backed by any loaded segment \
             (crash inside the async-snapshot window) — dropped; the rescan will \
             re-index those keys from the AOF",
            String::from_utf8_lossy(name),
            phantom_entries,
            if phantom_entries == 1 { "y" } else { "ies" }
        );
    }

    let loaded_segments = immutable.len();
    let idx = vector_store.get_index_mut(name)?;

    let mutable = Arc::new(MutableSegment::new(
        idx.meta.dimension,
        idx.collection.clone(),
    ));
    mutable.set_global_id_base(manifest.next_global_id);
    idx.segments.swap(SegmentList {
        mutable,
        immutable,
        ivf: Vec::new(),
        // WARM segments DO survive a restart, but not through this function:
        // `VectorIndex::try_warm_transitions_idle` now calls
        // `persist_hook_after_install` after every transition, which drops
        // the departed segment's id from this index's Stack B `segment_ids`
        // (so the scan above never re-discovers it here, and its now-stale
        // `idx-<hex>/segment-<old_id>/` directory gets GC'd instead of
        // leaking). The real restore path is Stack A: `Shard::restore_from_persistence`
        // discovers WARM segments from the `ShardManifest` and stages them on
        // `Shard::recovered_warm_segments`; `event_loop.rs` reattaches them via
        // `VectorStore::register_warm_segments` right after this function's
        // caller (`RecoveryState::finish`) returns.
        warm: Vec::new(),
        // COLD (unloaded) segments are on-disk stubs with no restart-time
        // restoration path today: same leak fix as WARM drops their id from
        // `segment_ids` too (their data lives under the identical Stack A
        // `shard_dir/vectors/segment-<id>/` layout as WARM -- see
        // `try_warm_transitions_idle`), but there is no Stack A discovery +
        // reattachment step for the `unloaded` tier yet. They simply don't
        // survive a restart as stubs or segments; the dedup rescan below
        // re-indexes their live keys from the keyspace instead (self-healing,
        // same fallback as any index with no durable manifest state).
        unloaded: Vec::new(),
    });
    idx.key_hash_to_key = key_hash_to_key;
    idx.key_hash_to_global_id = key_hash_to_global_id;
    idx.key_hash_to_vec_checksum = key_hash_to_vec_checksum;

    Some(IndexRecoveryCounters {
        loaded_segments,
        ..Default::default()
    })
}

/// Build a copy of `args` with the DEFAULT vector field's (name, value) pair
/// removed for every index in `matching_vector` (by the time this is
/// called, every one of them has already been confirmed both recovered AND
/// checksum-unchanged — see `reconcile_key`). Additional (non-default)
/// vector fields are never stripped: B1/B2 never persists their segments or
/// tracks their checksums, so they always re-encode on every restart — the
/// documented, safe multi-field gap.
fn strip_default_vector_fields(
    matching_vector: &[Bytes],
    vector_store: &VectorStore,
    args: &[Frame],
) -> Vec<Frame> {
    let mut strip_fields: Vec<Bytes> = Vec::with_capacity(matching_vector.len());
    for idx_name in matching_vector {
        if let Some(idx) = vector_store.get_index(idx_name) {
            strip_fields.push(idx.meta.vector_fields[0].field_name.clone());
        }
    }
    if strip_fields.is_empty() || args.is_empty() {
        return args.to_vec();
    }
    let mut out = Vec::with_capacity(args.len());
    out.push(args[0].clone()); // key
    let mut i = 1;
    while i + 1 < args.len() {
        let is_stripped = matches!(&args[i], Frame::BulkString(f) if strip_fields.iter().any(|sf| sf.eq_ignore_ascii_case(f)));
        if !is_stripped {
            out.push(args[i].clone());
            out.push(args[i + 1].clone());
        }
        i += 2;
    }
    out
}

/// Delete any `idx-<hex>` directory under `vector_persist_dir` whose hex
/// doesn't match a currently-defined sidecar index — e.g. a dropped index
/// whose best-effort background directory delete (`spawn_delete_index_persist_dir`)
/// never completed before a crash. Best-effort: failures are logged, never
/// propagated (matches the rest of the B3 recovery contract).
fn sweep_unknown_index_dirs(vector_persist_dir: &Path, known_hexes: &HashSet<String>) {
    let Ok(entries) = std::fs::read_dir(vector_persist_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Some(name) = entry.file_name().to_str().map(|s| s.to_owned()) else {
            continue;
        };
        let Some(hex) = name.strip_prefix("idx-") else {
            continue;
        };
        if known_hexes.contains(hex) {
            continue;
        }
        let path = entry.path();
        warn!(
            "sweeping unknown vector index directory {} (no matching sidecar index)",
            path.display()
        );
        if let Err(e) = std::fs::remove_dir_all(&path) {
            warn!("failed to remove unknown index dir {}: {e}", path.display());
        }
    }
}

#[cfg(test)]
#[path = "recover_v2_tests.rs"]
mod tests;
