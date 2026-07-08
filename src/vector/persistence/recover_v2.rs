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
            if let Some(idx) = vector_store.get_index(&name) {
                self.original_key_hashes
                    .insert(name.clone(), idx.key_hash_to_key.keys().copied().collect());
            }
            self.recovered_names.insert(name.clone());
            self.counters.insert(name.clone(), counters);
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
        warm: Vec::new(),
        cold: Vec::new(),
        // WS3 round 2: COLD (unloaded) segments are on-disk stubs with no
        // restart-time restoration path today, same as WARM -- they simply
        // don't survive a restart as stubs; the segment directories they
        // point at are reloaded as fresh HOT/immutable segments by the scan
        // above `loaded_segments` performs, exactly like WARM already does.
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
mod tests {
    use super::*;
    use crate::vector::store::VectorFieldMeta;
    use crate::vector::turbo_quant::collection::QuantizationConfig;
    use crate::vector::types::DistanceMetric;

    fn make_meta(name: &str, dim: u32, prefix: &str) -> IndexMeta {
        IndexMeta {
            name: Bytes::copy_from_slice(name.as_bytes()),
            dimension: dim,
            padded_dimension: dim,
            metric: DistanceMetric::L2,
            hnsw_m: 16,
            hnsw_ef_construction: 100,
            hnsw_ef_runtime: 0,
            compact_threshold: 0,
            source_field: Bytes::from_static(b"vec"),
            key_prefixes: vec![Bytes::copy_from_slice(prefix.as_bytes())],
            quantization: QuantizationConfig::Sq8,
            build_mode: crate::vector::turbo_quant::collection::BuildMode::Light,
            vector_fields: vec![VectorFieldMeta {
                field_name: Bytes::from_static(b"vec"),
                dimension: dim,
                padded_dimension: dim,
                metric: DistanceMetric::L2,
                quantization: QuantizationConfig::Sq8,
                build_mode: crate::vector::turbo_quant::collection::BuildMode::Light,
            }],
            schema_fields: Vec::new(),
            merge_mode: crate::vector::store::MergeMode::default(),
            keep_raw: false,
            db_index: 0,
            rerank_mult: 4,
            exact_beam: false,
        }
    }

    fn f32_blob(dim: usize, seed: u32) -> Bytes {
        let mut v = Vec::with_capacity(dim * 4);
        let mut s = seed;
        for _ in 0..dim {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            let f = (s as f32) / (u32::MAX as f32);
            v.extend_from_slice(&f.to_le_bytes());
        }
        Bytes::from(v)
    }

    /// Poll for `manifest.json` to appear (bounded, short interval) — the
    /// background `global_snapshot_pool()` worker is a single shared thread
    /// across every test in this binary, so its queue depth (hence latency)
    /// is not under this test's control. Fails the assertion (returns
    /// `None`) rather than hanging if the deadline is exceeded.
    fn wait_for_manifest(idx_dir: &std::path::Path) -> Option<IndexManifest> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if let Some(m) = manifest::read_manifest_tolerant(idx_dir) {
                return Some(m);
            }
            if std::time::Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    /// No manifest on disk -> `create_index` behaves exactly like the plain
    /// pre-B3 path (fresh collection_id, no recovered state).
    #[test]
    fn create_index_no_manifest_is_plain_fresh() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = VectorStore::new();
        store.set_persist_dir(tmp.path().to_path_buf());
        let mut state = RecoveryState::new();
        let meta = make_meta("idx", 8, "doc:");

        state.create_index(&mut store, tmp.path(), &meta);

        assert!(store.get_index(b"idx").is_some());
        assert!(
            !state.recovered_names.contains(&Bytes::from_static(b"idx")),
            "no manifest on disk -> index must NOT be marked recovered"
        );
    }

    /// Round-trip: build a store with a persist_dir, insert vectors, force a
    /// synchronous compact + snapshot so manifest/keymap/segments land on
    /// disk, then recover into a FRESH store. Verifies:
    /// - `verified_unchanged == n` (dedup actually fires for every key, not
    ///   just "results happen to match" — a full re-encode would also leave
    ///   every key indexed and the allocator seeded, so this counter check
    ///   is the one assertion that actually pins dedup behavior).
    /// - the mutable segment's global-id allocator is seeded above every
    ///   loaded global_id (a post-recovery insert never collides).
    /// - a same-query HNSW search returns the identical top-1 global_id
    ///   before persist and after recovery — this is real end-to-end proof
    ///   that `suggested_ef` round-trips AND that dedup preserves the
    ///   original global_id (a silent re-encode of doc:5 would reassign it
    ///   a fresh id from the seeded allocator and this assertion would go
    ///   red, independently of `verified_unchanged`'s own count check).
    ///   NOTE (verified by a temporary experiment, then reverted): offsetting
    ///   `create_index_with_collection_id`'s pinned cid by 1000 left this
    ///   test green. Root cause, confirmed by reading `ImmutableSegment::
    ///   search` (immutable.rs:499): it passes `&self.collection_meta` — the
    ///   segment's OWN disk-reconstructed rotation/codebook data — into
    ///   `hnsw_search`, never the recreated index's collection object. After
    ///   `force_compact()` all data lives in that one self-contained
    ///   immutable segment, so a search hitting only pre-recovery data is
    ///   structurally unable to observe a wrong pin — true for every
    ///   quantization mode, not an SQ8-specific gap. The pin's actual
    ///   invariant is merge-compatibility of POST-recovery inserts (a future
    ///   re-compact/GraphUnion must stitch new codes over the old ones using
    ///   shared rotation) — untestable via search and out of scope for B3
    ///   test hardening (would need a full second compact+merge cycle).
    ///   Recorded here as a known, deliberate test gap rather than implied
    ///   coverage.
    #[test]
    fn recover_round_trip_dedups_unchanged_keys_and_seeds_allocator() {
        use crate::protocol::Frame;

        let tmp = tempfile::tempdir().unwrap();
        let dim = 8usize;

        // ---- Build + persist ----
        let mut store = VectorStore::new();
        store.set_persist_dir(tmp.path().to_path_buf());
        let meta = make_meta("idx", dim as u32, "doc:");
        store.create_index(meta.clone()).unwrap();

        let n = 20usize;
        for i in 0..n {
            let key = format!("doc:{i}");
            let blob = f32_blob(dim, i as u32 + 1);
            let args = vec![
                Frame::BulkString(Bytes::from(key.clone())),
                Frame::BulkString(Bytes::from_static(b"vec")),
                Frame::BulkString(blob),
            ];
            let _ = crate::shard::spsc_handler::auto_index_hset_public(
                &mut store,
                &mut TextStore::new(),
                key.as_bytes(),
                &args,
                0,
            );
        }

        {
            let idx = store.get_index_mut(b"idx").unwrap();
            // `force_compact` writes the segment directory INLINE (the
            // staged writer runs synchronously on this thread) but commits
            // the manifest/keymap via `persist_hook_after_install`, which
            // hands the job to the process-wide `global_snapshot_pool()`
            // (a single background worker THREAD, shared by every test in
            // this binary). Do NOT also build and run a second, manually
            // constructed `SnapshotJob` here: both would race the exact
            // same `manifest.json.tmp`/`keymap-1.bin.tmp` paths (fixed
            // names, not job-unique) and can stomp each other's fsync —
            // this was tried and is flaky. Instead just wait for the ONE
            // real job the production code path already submitted.
            idx.force_compact();
        }
        let idx_dir = manifest::index_persist_dir(tmp.path(), b"idx");
        let loaded_manifest = wait_for_manifest(&idx_dir)
            .expect("manifest must land within the timeout via global_snapshot_pool()");
        assert!(!loaded_manifest.segment_ids.is_empty());
        let max_loaded_global_id: u32 = {
            let idx = store.get_index(b"idx").unwrap();
            idx.key_hash_to_global_id.values().copied().max().unwrap()
        };

        // Pre-persist search baseline: query with doc:5's own vector (exact
        // match, distance 0) — nearest neighbor must be itself. Captured
        // AFTER force_compact() so both sides of the comparison search the
        // same shape of state (empty mutable + 1 immutable HNSW segment).
        let query_idx = 5usize;
        let query_key = format!("doc:{query_idx}");
        let query_blob_bytes = f32_blob(dim, query_idx as u32 + 1);
        let query_f32: Vec<f32> = query_blob_bytes
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
            .collect();
        let expected_global_id = {
            let idx = store.get_index(b"idx").unwrap();
            let kh = xxhash_rust::xxh64::xxh64(query_key.as_bytes(), 0);
            *idx.key_hash_to_global_id.get(&kh).unwrap()
        };
        let pre_results = store.search_index(b"idx", &query_f32, 1, 64).unwrap();
        assert_eq!(
            pre_results.first().copied(),
            Some(expected_global_id),
            "sanity: pre-persist search must return the exact-match vector as top-1"
        );

        // ---- Recover into a FRESH store ----
        let mut fresh = VectorStore::new();
        fresh.set_persist_dir(tmp.path().to_path_buf());
        let mut state = RecoveryState::new();
        state.create_index(&mut fresh, tmp.path(), &meta);
        assert!(state.recovered_names.contains(&Bytes::from_static(b"idx")));

        let counters = *state.counters.get(&Bytes::from_static(b"idx")).unwrap();
        assert_eq!(counters.loaded_segments, loaded_manifest.segment_ids.len());

        // Re-run the SAME HSETs through the dedup rescan — every one must
        // be recognized as unchanged (checksum match), never re-encoded.
        for i in 0..n {
            let key = format!("doc:{i}");
            let blob = f32_blob(dim, i as u32 + 1);
            let args = vec![
                Frame::BulkString(Bytes::from(key.clone())),
                Frame::BulkString(Bytes::from_static(b"vec")),
                Frame::BulkString(blob),
            ];
            let mut text_store = TextStore::new();
            state.reconcile_key(&mut fresh, &mut text_store, key.as_bytes(), &args, 0);
        }

        // The load-bearing assertion: every key must have taken the
        // metadata-only dedup path, not the full re-encode path. A full
        // re-encode of all n keys would ALSO leave every key indexed and the
        // allocator correctly seeded (checked below), so those checks alone
        // cannot distinguish "dedup worked" from "dedup is silently dead".
        let verified_unchanged = state
            .counters
            .get(&Bytes::from_static(b"idx"))
            .unwrap()
            .verified_unchanged;
        assert_eq!(
            verified_unchanged, n,
            "every unchanged key must dedup (metadata-only rebuild), not re-encode"
        );

        state.finish(&mut fresh, tmp.path());

        let final_counters = {
            // finish() consumed `state`; re-derive expectations from the
            // fresh store's live state instead.
            fresh.get_index(b"idx").unwrap().key_hash_to_global_id.len()
        };
        assert_eq!(
            final_counters, n,
            "all keys must still be indexed post-recovery"
        );

        // Post-recovery insert must get a global_id above every loaded one.
        let new_key = b"doc:new".to_vec();
        let new_blob = f32_blob(dim, 9999);
        let args = vec![
            Frame::BulkString(Bytes::from(new_key.clone())),
            Frame::BulkString(Bytes::from_static(b"vec")),
            Frame::BulkString(new_blob),
        ];
        let mut text_store = TextStore::new();
        let _ = crate::shard::spsc_handler::auto_index_hset_public(
            &mut fresh,
            &mut text_store,
            &new_key,
            &args,
            0,
        );
        let new_global_id = *fresh
            .get_index(b"idx")
            .unwrap()
            .key_hash_to_global_id
            .get(&xxhash_rust::xxh64::xxh64(&new_key, 0))
            .unwrap();
        assert!(
            new_global_id > max_loaded_global_id,
            "post-recovery insert global_id ({new_global_id}) must exceed every loaded \
             global_id ({max_loaded_global_id})"
        );

        // Post-recovery search must return the SAME top-1 as the
        // pre-persist baseline — this is what actually exercises
        // `suggested_ef` round-tripping and the collection_id/QJL-seed pin:
        // a mismatched seed scrambles HNSW beam search (wrong/garbage
        // distances), which a plain key-count check would never catch.
        let post_results = fresh.search_index(b"idx", &query_f32, 1, 64).unwrap();
        assert_eq!(
            post_results.first().copied(),
            Some(expected_global_id),
            "post-recovery search must return the same top-1 global_id as the pre-persist baseline"
        );
    }

    /// Deterministic regression test for the "phantom keymap entry"
    /// silent-loss hole (found by `tests/crash_recovery_vector_durability.rs`
    /// S1 flaking: post-crash `num_docs` 1950/2000 with `verified_unchanged
    /// == 2000, re_indexed == 0`).
    ///
    /// The durable keymap covers EVERY indexed key (mutable + immutable) at
    /// snapshot-submit time; a kill -9 landing before the NEXT snapshot
    /// (covering a just-frozen segment) leaves on-disk keymap ⊃ on-disk
    /// segments. This test stages that state directly: persist n docs
    /// normally, then rewrite the durable keymap with one EXTRA entry whose
    /// checksum matches its AOF blob but whose doc is in NO segment. The
    /// dedup rescan must RE-INDEX that key (making it searchable), never
    /// "verify" it unchanged (which drops the doc silently).
    #[test]
    fn recover_reindexes_keymap_entries_not_backed_by_any_segment() {
        use crate::protocol::Frame;

        let tmp = tempfile::tempdir().unwrap();
        let dim = 8usize;

        // ---- Build + persist n real docs (same flow as the round-trip test) ----
        let mut store = VectorStore::new();
        store.set_persist_dir(tmp.path().to_path_buf());
        let meta = make_meta("idx", dim as u32, "doc:");
        store.create_index(meta.clone()).unwrap();

        let n = 8usize;
        for i in 0..n {
            let key = format!("doc:{i}");
            let blob = f32_blob(dim, i as u32 + 1);
            let args = vec![
                Frame::BulkString(Bytes::from(key.clone())),
                Frame::BulkString(Bytes::from_static(b"vec")),
                Frame::BulkString(blob),
            ];
            let _ = crate::shard::spsc_handler::auto_index_hset_public(
                &mut store,
                &mut TextStore::new(),
                key.as_bytes(),
                &args,
                0,
            );
        }
        {
            let idx = store.get_index_mut(b"idx").unwrap();
            idx.force_compact();
        }
        let idx_dir = manifest::index_persist_dir(tmp.path(), b"idx");
        let loaded_manifest = wait_for_manifest(&idx_dir)
            .expect("manifest must land within the timeout via global_snapshot_pool()");

        // ---- Stage the crash window: durable keymap ⊃ durable segments ----
        // The phantom's checksum MATCHES the blob the rescan will replay
        // (that is the crash-window signature: the key was mutable-resident
        // and fully checksummed when this keymap snapshot committed, but its
        // segment never reached disk).
        let phantom_key = b"doc:phantom".to_vec();
        let phantom_hash = xxhash_rust::xxh64::xxh64(&phantom_key, 0);
        let phantom_blob = f32_blob(dim, 4242);
        let max_gid = {
            let idx = store.get_index(b"idx").unwrap();
            idx.key_hash_to_global_id.values().copied().max().unwrap()
        };
        let mut entries = manifest::read_keymap_tolerant(&idx_dir, loaded_manifest.keymap_epoch)
            .expect("keymap must be readable");
        assert_eq!(entries.len(), n, "sanity: durable keymap covers all n docs");
        entries.push(manifest::KeymapEntry {
            key_hash: phantom_hash,
            global_id: max_gid + 1,
            vec_checksum: xxhash_rust::xxh64::xxh64(&phantom_blob, 0),
            key: Bytes::from(phantom_key.clone()),
        });
        manifest::write_keymap_atomic(&idx_dir, loaded_manifest.keymap_epoch, &entries)
            .expect("rewrite keymap with phantom entry");

        // ---- Recover into a FRESH store ----
        let mut fresh = VectorStore::new();
        fresh.set_persist_dir(tmp.path().to_path_buf());
        let mut state = RecoveryState::new();
        state.create_index(&mut fresh, tmp.path(), &meta);
        assert!(state.recovered_names.contains(&Bytes::from_static(b"idx")));

        // The phantom must NOT have been loaded into the recovered maps —
        // its doc exists in no loaded segment.
        assert!(
            fresh
                .get_index(b"idx")
                .unwrap()
                .key_hash_to_key
                .get(&phantom_hash)
                .is_none(),
            "keymap entry with no backing segment doc must be dropped at load"
        );

        // Rescan: replay all n real keys plus the phantom (its HSET is in
        // the AOF — the key genuinely exists in the keyspace).
        let mut text_store = TextStore::new();
        for i in 0..n {
            let key = format!("doc:{i}");
            let blob = f32_blob(dim, i as u32 + 1);
            let args = vec![
                Frame::BulkString(Bytes::from(key.clone())),
                Frame::BulkString(Bytes::from_static(b"vec")),
                Frame::BulkString(blob),
            ];
            state.reconcile_key(&mut fresh, &mut text_store, key.as_bytes(), &args, 0);
        }
        let phantom_args = vec![
            Frame::BulkString(Bytes::from(phantom_key.clone())),
            Frame::BulkString(Bytes::from_static(b"vec")),
            Frame::BulkString(phantom_blob.clone()),
        ];
        state.reconcile_key(&mut fresh, &mut text_store, &phantom_key, &phantom_args, 0);

        let counters = *state.counters.get(&Bytes::from_static(b"idx")).unwrap();
        assert_eq!(
            counters.verified_unchanged, n,
            "the n segment-backed keys dedup as unchanged"
        );
        assert_eq!(
            counters.re_indexed, 1,
            "the phantom key must take the full re-index path (checksum match \
             without a backing segment doc must never count as unchanged)"
        );

        state.finish(&mut fresh, tmp.path());

        // End-to-end: the phantom's document must actually be searchable —
        // exact-match query returns it as top-1 (before the fix the doc was
        // silently absent).
        let phantom_gid = *fresh
            .get_index(b"idx")
            .unwrap()
            .key_hash_to_global_id
            .get(&phantom_hash)
            .expect("phantom key must be indexed after the rescan");
        let query_f32: Vec<f32> = phantom_blob
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
            .collect();
        let results = fresh.search_index(b"idx", &query_f32, 1, 64).unwrap();
        assert_eq!(
            results.first().copied(),
            Some(phantom_gid),
            "phantom key's doc must be searchable post-recovery (was silently lost)"
        );
    }

    /// A segment whose directory is entirely missing (simulating a crash
    /// that lost the whole segment, headers included) must not panic
    /// recovery — falls back to abandoning this index's recovered state
    /// (full rescan covers it).
    #[test]
    fn recover_corrupt_segment_with_unreadable_headers_falls_back_cleanly() {
        let tmp = tempfile::tempdir().unwrap();
        let idx_dir = manifest::index_persist_dir(tmp.path(), b"idx");
        std::fs::create_dir_all(&idx_dir).unwrap();

        // Manifest references segment-1, but NOTHING is written on disk for
        // it (not even mvcc_headers.bin) — the "headers also unreadable"
        // case.
        let m = IndexManifest {
            format_version: manifest::MANIFEST_FORMAT_VERSION,
            index_name_hex: manifest::index_name_hex(b"idx"),
            collection_id: 7,
            next_collection_id_floor: 8,
            next_segment_id: 2,
            next_global_id: 5,
            segment_ids: vec![1],
            keymap_epoch: 1,
        };
        manifest::write_manifest_atomic(&idx_dir, &m).unwrap();

        let mut store = VectorStore::new();
        store.set_persist_dir(tmp.path().to_path_buf());
        let meta = make_meta("idx", 8, "doc:");
        let mut state = RecoveryState::new();

        state.create_index(&mut store, tmp.path(), &meta);

        // Index exists (pinned cid) but is NOT marked recovered.
        assert!(store.get_index(b"idx").is_some());
        assert!(!state.recovered_names.contains(&Bytes::from_static(b"idx")));
    }

    /// Deletion probe (`finish()`'s `original_key_hashes` − `observed_key_hashes`
    /// set difference): a key that existed at persist time but is never
    /// observed during the rescan (deleted from the keyspace between
    /// restarts) must be tombstoned — removed from the live index's
    /// key_hash maps — even though it was never touched by `reconcile_key`.
    #[test]
    fn recover_finish_tombstones_keys_missing_from_rescan() {
        use crate::protocol::Frame;

        let tmp = tempfile::tempdir().unwrap();
        let dim = 8usize;

        let mut store = VectorStore::new();
        store.set_persist_dir(tmp.path().to_path_buf());
        let meta = make_meta("idx", dim as u32, "doc:");
        store.create_index(meta.clone()).unwrap();

        let n = 5usize;
        for i in 0..n {
            let key = format!("doc:{i}");
            let blob = f32_blob(dim, i as u32 + 1);
            let args = vec![
                Frame::BulkString(Bytes::from(key.clone())),
                Frame::BulkString(Bytes::from_static(b"vec")),
                Frame::BulkString(blob),
            ];
            let _ = crate::shard::spsc_handler::auto_index_hset_public(
                &mut store,
                &mut TextStore::new(),
                key.as_bytes(),
                &args,
                0,
            );
        }
        {
            let idx = store.get_index_mut(b"idx").unwrap();
            idx.force_compact();
        }
        let idx_dir = manifest::index_persist_dir(tmp.path(), b"idx");
        wait_for_manifest(&idx_dir)
            .expect("manifest must land within the timeout via global_snapshot_pool()");

        // Recover into a FRESH store, but only rescan n-1 of the n keys —
        // "doc:3" is simulated as deleted from the keyspace between restarts
        // (e.g. a DEL that happened while the server was down).
        let mut fresh = VectorStore::new();
        fresh.set_persist_dir(tmp.path().to_path_buf());
        let mut state = RecoveryState::new();
        state.create_index(&mut fresh, tmp.path(), &meta);
        assert!(state.recovered_names.contains(&Bytes::from_static(b"idx")));

        let missing_key = "doc:3";
        let missing_key_hash = xxhash_rust::xxh64::xxh64(missing_key.as_bytes(), 0);
        assert!(
            fresh
                .get_index(b"idx")
                .unwrap()
                .key_hash_to_global_id
                .contains_key(&missing_key_hash),
            "sanity: the to-be-deleted key must be present right after recovery, \
             before the rescan runs"
        );

        for i in 0..n {
            if i == 3 {
                continue; // never observed by reconcile_key — simulates a DEL
            }
            let key = format!("doc:{i}");
            let blob = f32_blob(dim, i as u32 + 1);
            let args = vec![
                Frame::BulkString(Bytes::from(key.clone())),
                Frame::BulkString(Bytes::from_static(b"vec")),
                Frame::BulkString(blob),
            ];
            let mut text_store = TextStore::new();
            state.reconcile_key(&mut fresh, &mut text_store, key.as_bytes(), &args, 0);
        }
        state.finish(&mut fresh, tmp.path());

        let idx = fresh.get_index(b"idx").unwrap();
        assert!(
            !idx.key_hash_to_global_id.contains_key(&missing_key_hash),
            "key never observed during the rescan must be tombstoned by finish()"
        );
        assert!(
            !idx.key_hash_to_key.contains_key(&missing_key_hash),
            "tombstoning must also prune the key_hash_to_key map"
        );
        assert_eq!(
            idx.key_hash_to_global_id.len(),
            n - 1,
            "the other n-1 keys (all observed) must remain indexed"
        );
    }
}
