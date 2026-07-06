//! Vector-index durability (B1/B2 write path): manifest + keymap persistence
//! and the background snapshot job that commits them after a compact/merge
//! install.
//!
//! ## On-disk layout
//!
//! Under `<vector_persist_dir>/idx-<hex(xxh64(index_name))>/`:
//! ```text
//! manifest.json          -- atomic pointer (tmp + fsync + rename + dir fsync)
//! keymap-<epoch>.bin     -- key_hash -> (global_id, vec_checksum, key)
//! segment-<id>/          -- segment_io v1 format, written via the staged
//!                            (staging-<id> -> rename) writer
//! ```
//!
//! ## Crash-safety invariant
//!
//! The manifest may UNDERSTATE what exists on disk (a crash between a
//! segment/keymap write and the manifest rename just leaves an orphan
//! directory/file — harmless, swept at B3 startup) but must NEVER OVERSTATE
//! (recovery must never be pointed at a segment/keymap that isn't durably on
//! disk). See `tmp/VECTOR-DURABILITY-DESIGN.md` for the full design.
//!
//! ## Per-index ordering
//!
//! The background compaction pool (`background_compact.rs`) is a GLOBAL,
//! multi-worker pool with NO ordering guarantee across jobs — two snapshot
//! jobs for the SAME index can be dequeued and completed by different
//! workers in either order. [`SnapshotJob`] carries a per-index monotonic
//! `seq` (allocated on the shard thread at enqueue time) plus a shared
//! `watermark` lock; [`run_snapshot_job`] holds that lock across its entire
//! write+GC critical section, so a stale job (built from an older install)
//! can never overwrite a newer, already-committed manifest — see that
//! function's doc comment for the full argument.

use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::OnceLock;

use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::persistence::fsync::{fsync_directory, fsync_file};

/// Current on-disk manifest format version.
pub const MANIFEST_FORMAT_VERSION: u32 = 1;

/// Magic bytes identifying a `keymap-<epoch>.bin` file.
const KEYMAP_MAGIC: &[u8; 4] = b"MKM1";
/// `magic(4) + count(8) + payload_xxh64(8)`.
const KEYMAP_HEADER_LEN: usize = 20;

/// Atomic pointer describing one vector index's durable state: which
/// collection id / segment ids / keymap epoch / id-allocator floors are
/// currently live. Written with tmp+fsync+rename+dir-fsync (never partially
/// visible); read tolerantly (any corruption/mismatch degrades to "absent",
/// never a panic or hard error — the B3 rescan covers the gap).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexManifest {
    /// Format version — bumped on any incompatible layout change.
    pub format_version: u32,
    /// Hex-encoded `xxh64(index_name)` — sanity/debugging aid; also the
    /// directory's own suffix, so a manifest can be self-checked against
    /// the directory it was found in.
    pub index_name_hex: String,
    /// QJL rotation seed for the default field — MUST match the `collection_id`
    /// the index is recreated with, or loaded segments become unsearchable
    /// (see design doc's "#1 correctness trap").
    pub collection_id: u64,
    /// One past the highest collection id used by ANY field of this index
    /// (default + additional named vector fields). Recovery seeds the
    /// store-wide `next_collection_id` allocator with `max(this, current)`
    /// so a later FT.CREATE never collides with a restored collection id.
    pub next_collection_id_floor: u64,
    /// Next on-disk segment id to allocate for this index.
    pub next_segment_id: u64,
    /// Floor for the mutable segment's global-id allocator.
    pub next_global_id: u32,
    /// Live immutable segment ids referenced by this manifest (in no
    /// particular order). Segment directories that exist on disk but are
    /// NOT in this list are orphans (superseded by a merge, or a compact
    /// whose manifest commit never landed) — safe to delete.
    pub segment_ids: Vec<u64>,
    /// Which `keymap-<epoch>.bin` is current.
    pub keymap_epoch: u64,
}

/// One entry in a `keymap-<epoch>.bin` file.
#[derive(Debug, Clone, PartialEq)]
pub struct KeymapEntry {
    pub key_hash: u64,
    pub global_id: u32,
    /// `xxh64(vector-field bytes, seed 0)` at last (re-)insert.
    pub vec_checksum: u64,
    /// Original Redis key bytes.
    pub key: Bytes,
}

/// Directory an index's manifest/keymap/segments live under, given the
/// store-level `vector_persist_dir` and the index's name. Hex avoids
/// path-unsafe index names.
pub fn index_persist_dir(store_persist_dir: &Path, index_name: &[u8]) -> PathBuf {
    store_persist_dir.join(format!("idx-{}", index_name_hex(index_name)))
}

/// Hex-encoded `xxh64(index_name)`, used both for the `idx-<hex>` directory
/// name and the manifest's self-describing `index_name_hex` field.
pub fn index_name_hex(index_name: &[u8]) -> String {
    format!("{:016x}", xxhash_rust::xxh64::xxh64(index_name, 0))
}

fn manifest_path(dir: &Path) -> PathBuf {
    dir.join("manifest.json")
}

fn manifest_tmp_path(dir: &Path) -> PathBuf {
    dir.join("manifest.json.tmp")
}

fn keymap_path(dir: &Path, epoch: u64) -> PathBuf {
    dir.join(format!("keymap-{epoch}.bin"))
}

fn keymap_tmp_path(dir: &Path, epoch: u64) -> PathBuf {
    dir.join(format!("keymap-{epoch}.bin.tmp"))
}

fn segment_dir_path(dir: &Path, segment_id: u64) -> PathBuf {
    dir.join(format!("segment-{segment_id}"))
}

/// Write `manifest.json` atomically: serialize -> write to `manifest.json.tmp`
/// -> fsync the tmp file -> `rename` over `manifest.json` -> fsync `dir`.
///
/// The rename is the only visible state transition: readers either see the
/// complete old manifest or the complete new one, never a partial write.
pub fn write_manifest_atomic(dir: &Path, manifest: &IndexManifest) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    let json = serde_json::to_vec_pretty(manifest)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    let tmp_path = manifest_tmp_path(dir);
    fs::write(&tmp_path, &json)?;
    fsync_file(&tmp_path)?;
    fs::rename(&tmp_path, manifest_path(dir))?;
    fsync_directory(dir)?;
    Ok(())
}

/// Read `manifest.json` tolerantly: absent (never written, or a stale
/// pre-B2 index) -> `None` with no log noise (the common case); present but
/// corrupt/unparseable -> `None` with a `tracing::warn!` (a real anomaly the
/// operator should be able to spot, but never a panic or propagated error —
/// the B3 rescan degrades gracefully to a full re-index for that one index).
pub fn read_manifest_tolerant(dir: &Path) -> Option<IndexManifest> {
    let path = manifest_path(dir);
    let bytes = match fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return None,
        Err(e) => {
            tracing::debug!(
                "manifest.json at {}: {e} — treating as absent",
                dir.display()
            );
            return None;
        }
    };
    match serde_json::from_slice::<IndexManifest>(&bytes) {
        Ok(m) => Some(m),
        Err(e) => {
            tracing::warn!(
                "manifest.json at {} is corrupt: {e} — treating as absent \
                 (full rescan will cover this index)",
                dir.display()
            );
            None
        }
    }
}

fn read_u16_le(b: &[u8], off: usize) -> Option<u16> {
    b.get(off..off + 2)?.try_into().ok().map(u16::from_le_bytes)
}
fn read_u32_le(b: &[u8], off: usize) -> Option<u32> {
    b.get(off..off + 4)?.try_into().ok().map(u32::from_le_bytes)
}
fn read_u64_le(b: &[u8], off: usize) -> Option<u64> {
    b.get(off..off + 8)?.try_into().ok().map(u64::from_le_bytes)
}

/// Write `keymap-<epoch>.bin` atomically (tmp + fsync + rename + dir-fsync),
/// same protocol as [`write_manifest_atomic`].
///
/// Format (little-endian):
/// ```text
/// header: magic "MKM1" (4B) | count u64 | payload_xxh64 u64
/// entry:  key_hash u64 | global_id u32 | vec_checksum u64 | key_len u16 | key bytes
/// ```
/// `payload_xxh64` covers every entry byte (seed 0) — verified on read.
pub fn write_keymap_atomic(dir: &Path, epoch: u64, entries: &[KeymapEntry]) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    let mut payload = Vec::with_capacity(entries.len() * 24);
    for e in entries {
        payload.extend_from_slice(&e.key_hash.to_le_bytes());
        payload.extend_from_slice(&e.global_id.to_le_bytes());
        payload.extend_from_slice(&e.vec_checksum.to_le_bytes());
        // Truncate degenerately long keys rather than erroring — this is
        // a best-effort persistence cache, not the source of truth (the
        // DashTable is); an oversize key is vanishingly unlikely to be
        // preceded by a HASH key long enough to overflow u16 (64KiB).
        let key_len = e.key.len().min(u16::MAX as usize);
        payload.extend_from_slice(&(key_len as u16).to_le_bytes());
        payload.extend_from_slice(&e.key[..key_len]);
    }
    let checksum = xxhash_rust::xxh64::xxh64(&payload, 0);

    let mut buf = Vec::with_capacity(KEYMAP_HEADER_LEN + payload.len());
    buf.extend_from_slice(KEYMAP_MAGIC);
    buf.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    buf.extend_from_slice(&checksum.to_le_bytes());
    buf.extend_from_slice(&payload);

    let tmp_path = keymap_tmp_path(dir, epoch);
    fs::write(&tmp_path, &buf)?;
    fsync_file(&tmp_path)?;
    fs::rename(&tmp_path, keymap_path(dir, epoch))?;
    fsync_directory(dir)?;
    Ok(())
}

/// Read `keymap-<epoch>.bin` tolerantly. Any structural problem — missing
/// file, bad magic, truncated header/entries, or a payload checksum
/// mismatch — returns `None` (with a `tracing::warn!` for anything other
/// than plain absence) instead of erroring: a corrupt keymap costs a
/// rescan of that index, never a crash.
pub fn read_keymap_tolerant(dir: &Path, epoch: u64) -> Option<Vec<KeymapEntry>> {
    let path = keymap_path(dir, epoch);
    let bytes = match fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return None,
        Err(e) => {
            tracing::debug!(
                "keymap-{epoch}.bin at {}: {e} — treating as absent",
                dir.display()
            );
            return None;
        }
    };
    if bytes.len() < KEYMAP_HEADER_LEN || &bytes[0..4] != KEYMAP_MAGIC {
        tracing::warn!(
            "keymap-{epoch}.bin at {} has bad magic or is truncated — treating as absent",
            dir.display()
        );
        return None;
    }
    let Some(count) = read_u64_le(&bytes, 4) else {
        tracing::warn!("keymap-{epoch}.bin at {}: truncated header", dir.display());
        return None;
    };
    let Some(expected_checksum) = read_u64_le(&bytes, 12) else {
        tracing::warn!("keymap-{epoch}.bin at {}: truncated header", dir.display());
        return None;
    };
    let payload = &bytes[KEYMAP_HEADER_LEN..];
    let actual_checksum = xxhash_rust::xxh64::xxh64(payload, 0);
    if actual_checksum != expected_checksum {
        tracing::warn!(
            "keymap-{epoch}.bin at {} failed integrity check (checksum {actual_checksum:#x} != \
             expected {expected_checksum:#x}) — treating as absent",
            dir.display()
        );
        return None;
    }

    let mut entries = Vec::with_capacity(count as usize);
    let mut off = 0usize;
    for _ in 0..count {
        let Some(key_hash) = read_u64_le(payload, off) else {
            tracing::warn!("keymap-{epoch}.bin at {}: entry truncated", dir.display());
            return None;
        };
        off += 8;
        let Some(global_id) = read_u32_le(payload, off) else {
            tracing::warn!("keymap-{epoch}.bin at {}: entry truncated", dir.display());
            return None;
        };
        off += 4;
        let Some(vec_checksum) = read_u64_le(payload, off) else {
            tracing::warn!("keymap-{epoch}.bin at {}: entry truncated", dir.display());
            return None;
        };
        off += 8;
        let Some(key_len) = read_u16_le(payload, off) else {
            tracing::warn!("keymap-{epoch}.bin at {}: entry truncated", dir.display());
            return None;
        };
        off += 2;
        let Some(key_bytes) = payload.get(off..off + key_len as usize) else {
            tracing::warn!(
                "keymap-{epoch}.bin at {}: key bytes truncated",
                dir.display()
            );
            return None;
        };
        off += key_len as usize;
        entries.push(KeymapEntry {
            key_hash,
            global_id,
            vec_checksum,
            key: Bytes::copy_from_slice(key_bytes),
        });
    }
    Some(entries)
}

/// Given the entry names present in an index's persist dir and its current
/// manifest, return the names that are NOT referenced by the manifest and
/// are therefore safe to delete:
/// - `staging-*` — always an orphan (an interrupted segment build; a
///   completed one was renamed away to `segment-*` before the manifest that
///   references it was ever written).
/// - `segment-<id>` — an orphan when `id` is not in `manifest.segment_ids`
///   (or the name doesn't parse as `segment-<u64>`).
/// - `keymap-<epoch>.bin` — an orphan when `epoch != manifest.keymap_epoch`
///   (or the name doesn't parse as `keymap-<u64>.bin`, e.g. a leftover
///   `keymap-<n>.bin.tmp`).
/// - `manifest.json` (and its own `.tmp`) and anything else unrecognized are
///   never returned — this helper only ever names things it is certain are
///   superseded.
///
/// Pure function — no I/O. Intended to run at B3 startup (no concurrent
/// writer for the index at that point); the LIVE path uses the online
/// set-difference GC in [`run_snapshot_job`] instead, which only ever
/// removes what the manifest transition itself proves is superseded (a
/// directory scan while a concurrent compaction is mid-write would delete a
/// live, not-yet-referenced `segment-<id>`).
pub fn compute_orphans(entries: &[String], manifest: &IndexManifest) -> Vec<String> {
    let live_segments: HashSet<u64> = manifest.segment_ids.iter().copied().collect();
    entries
        .iter()
        .filter(|name| {
            if let Some(rest) = name.strip_prefix("staging-") {
                // Any staging dir is an interrupted/superseded build,
                // regardless of what id it names.
                let _ = rest;
                return true;
            }
            if let Some(id_str) = name.strip_prefix("segment-") {
                return match id_str.parse::<u64>() {
                    Ok(id) => !live_segments.contains(&id),
                    Err(_) => true, // malformed name — not a real segment dir
                };
            }
            if let Some(rest) = name.strip_prefix("keymap-") {
                return match rest
                    .strip_suffix(".bin")
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    Some(epoch) => epoch != manifest.keymap_epoch,
                    None => true, // not a well-formed "keymap-<epoch>.bin" (incl. .tmp leftovers)
                };
            }
            false
        })
        .cloned()
        .collect()
}

/// Disk-backed variant of [`compute_orphans`]: lists `dir`, computes the
/// orphan set against `manifest`, and deletes each one (best-effort — a
/// single removal failure is logged and does not abort the sweep).
///
/// Startup-only (B3): safe because nothing else is writing to this index's
/// directory yet. NOT safe to call on the live/steady-state path (use the
/// online set-difference GC inside [`run_snapshot_job`] there).
pub fn sweep_orphans_from_disk(dir: &Path, manifest: &IndexManifest) -> io::Result<Vec<PathBuf>> {
    let mut names = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if let Some(name) = entry.file_name().to_str() {
            names.push(name.to_owned());
        }
    }
    let orphans = compute_orphans(&names, manifest);
    let mut removed = Vec::with_capacity(orphans.len());
    for name in orphans {
        let path = dir.join(&name);
        let result = if path.is_dir() {
            fs::remove_dir_all(&path)
        } else {
            fs::remove_file(&path)
        };
        match result {
            Ok(()) => removed.push(path),
            Err(e) => tracing::warn!("orphan sweep: failed to remove {}: {e}", path.display()),
        }
    }
    Ok(removed)
}

/// A background job that durably commits one index's keymap + manifest
/// snapshot after a compact/merge install, then GCs whatever the new
/// manifest supersedes.
pub struct SnapshotJob {
    /// This index's persist directory (`idx-<hex>/`).
    pub idx_dir: PathBuf,
    /// Monotonic per-index sequence number, allocated on the shard thread at
    /// enqueue time. Also used as the keymap epoch.
    pub seq: u64,
    /// Shared per-index watermark — see module docs and [`run_snapshot_job`].
    pub watermark: Arc<Mutex<u64>>,
    /// The manifest to commit if this job is not stale. `keymap_epoch` MUST
    /// equal `seq`.
    pub manifest: IndexManifest,
    pub key_hash_to_key: Arc<std::collections::HashMap<u64, Bytes>>,
    pub key_hash_to_global_id: Arc<std::collections::HashMap<u64, u32>>,
    pub key_hash_to_vec_checksum: Arc<std::collections::HashMap<u64, u64>>,
}

/// Run one snapshot job to completion.
///
/// ## Ordering guarantee
///
/// The background compaction pool has multiple workers and no cross-job
/// ordering — a job built from an OLDER install can be dequeued and reach
/// this function AFTER a job built from a NEWER install already committed.
/// A bare "load watermark, compare, write" is racy (two jobs can both read
/// the same watermark and then race the final rename); instead this
/// function holds `job.watermark`'s lock across the ENTIRE critical section
/// (stale check -> keymap write -> manifest write -> GC -> watermark
/// update). Since all jobs for one index share the same `Arc<Mutex<_>>`,
/// this fully serializes them: whichever job's thread acquires the lock
/// first runs to completion; every other job for that index blocks, then
/// (because `seq` is monotonically allocated on the single shard thread and
/// therefore totally ordered) either proceeds — if it's newer than what
/// just committed — or is dropped as stale — if it's older. No I/O happens
/// before the stale check, so a stale job costs nothing but the lock wait.
///
/// ## GC is a set-difference, not a directory scan
///
/// Only segment ids present in the OLD on-disk manifest but absent from the
/// NEW one are removed (merge-obsoleted sources), plus the old keymap epoch
/// if it changed. A concurrent compaction's in-flight `staging-<id>` was
/// never in the old manifest, so this can never touch it — unlike a
/// directory scan, which would.
pub fn run_snapshot_job(job: SnapshotJob) {
    let mut watermark = job.watermark.lock();
    if job.seq <= *watermark {
        tracing::debug!(
            seq = job.seq,
            watermark = *watermark,
            dir = %job.idx_dir.display(),
            "vector snapshot job stale — dropped (a newer snapshot already committed)"
        );
        return;
    }

    let old_manifest = read_manifest_tolerant(&job.idx_dir);

    let mut entries: Vec<KeymapEntry> = Vec::with_capacity(job.key_hash_to_key.len());
    for (&key_hash, key) in job.key_hash_to_key.iter() {
        let global_id = job
            .key_hash_to_global_id
            .get(&key_hash)
            .copied()
            .unwrap_or(0);
        let vec_checksum = job
            .key_hash_to_vec_checksum
            .get(&key_hash)
            .copied()
            .unwrap_or(0);
        entries.push(KeymapEntry {
            key_hash,
            global_id,
            vec_checksum,
            key: key.clone(),
        });
    }

    if let Err(e) = write_keymap_atomic(&job.idx_dir, job.seq, &entries) {
        tracing::warn!(
            "vector snapshot: failed to write keymap-{} at {}: {e} — leaving prior \
             manifest in place (this compact/merge install is simply not durable yet)",
            job.seq,
            job.idx_dir.display()
        );
        return;
    }
    if let Err(e) = write_manifest_atomic(&job.idx_dir, &job.manifest) {
        tracing::warn!(
            "vector snapshot: failed to write manifest.json at {}: {e}",
            job.idx_dir.display()
        );
        return;
    }

    if let Some(old) = old_manifest {
        let new_ids: HashSet<u64> = job.manifest.segment_ids.iter().copied().collect();
        for old_id in old.segment_ids {
            if new_ids.contains(&old_id) {
                continue;
            }
            let path = segment_dir_path(&job.idx_dir, old_id);
            if let Err(e) = fs::remove_dir_all(&path) {
                if e.kind() != io::ErrorKind::NotFound {
                    tracing::warn!(
                        "vector snapshot GC: failed to remove superseded segment-{old_id} \
                         at {}: {e}",
                        path.display()
                    );
                }
            }
        }
        if old.keymap_epoch != job.manifest.keymap_epoch {
            let old_path = keymap_path(&job.idx_dir, old.keymap_epoch);
            if let Err(e) = fs::remove_file(&old_path) {
                if e.kind() != io::ErrorKind::NotFound {
                    tracing::warn!(
                        "vector snapshot GC: failed to remove superseded keymap-{}: {e}",
                        old.keymap_epoch
                    );
                }
            }
        }
    }

    *watermark = job.seq;
}

/// Background snapshot pool: a small, dedicated worker pool that runs
/// [`run_snapshot_job`]. Deliberately separate from
/// `background_compact::BackgroundCompactor` (different job shape — this is
/// small, fsync-bound I/O, not CPU-bound HNSW build work) but the same
/// lazy-init-singleton shape.
pub struct SnapshotPool {
    job_tx: flume::Sender<SnapshotJob>,
    _workers: Vec<std::thread::JoinHandle<()>>,
}

impl SnapshotPool {
    fn new(num_workers: usize) -> Self {
        let (job_tx, job_rx) = flume::unbounded::<SnapshotJob>();
        let workers: Vec<_> = (0..num_workers.max(1))
            .map(|i| {
                let rx = job_rx.clone();
                // Startup-time OS thread spawn failure is unrecoverable
                // (matches the existing `BackgroundCompactor::new` convention).
                #[allow(clippy::unwrap_used)]
                let handle = std::thread::Builder::new()
                    .name(format!("moon-vec-snapshot-{i}"))
                    .spawn(move || {
                        while let Ok(job) = rx.recv() {
                            run_snapshot_job(job);
                        }
                    })
                    .expect("failed to spawn vector snapshot worker thread");
                handle
            })
            .collect();
        Self {
            job_tx,
            _workers: workers,
        }
    }

    /// Enqueue a snapshot job. Best-effort: send only fails if every worker
    /// has exited (e.g. all panicked), in which case the job is dropped with
    /// a warning — per the crash-safety invariant this only leaves the
    /// on-disk state a bit more stale (understating), never wrong.
    pub fn submit(&self, job: SnapshotJob) {
        if self.job_tx.send(job).is_err() {
            tracing::warn!(
                "vector snapshot pool: all workers gone — snapshot job dropped \
                 (index falls behind on durability, no correctness impact)"
            );
        }
    }
}

/// Process-wide snapshot pool, lazily initialized on first use. A single
/// worker is sufficient: jobs are small, fsync-bound, and infrequent (one
/// per compact/merge install), so there is no throughput reason to add
/// concurrency here — and a single worker keeps per-index ordering trivial
/// to reason about even before accounting for the watermark guard.
pub fn global_snapshot_pool() -> &'static SnapshotPool {
    static GLOBAL: OnceLock<SnapshotPool> = OnceLock::new();
    GLOBAL.get_or_init(|| SnapshotPool::new(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_manifest(dir_name: &str, segment_ids: Vec<u64>, keymap_epoch: u64) -> IndexManifest {
        IndexManifest {
            format_version: MANIFEST_FORMAT_VERSION,
            index_name_hex: dir_name.to_owned(),
            collection_id: 7,
            next_collection_id_floor: 8,
            next_segment_id: segment_ids.iter().copied().max().map_or(0, |m| m + 1),
            next_global_id: 12345,
            segment_ids,
            keymap_epoch,
        }
    }

    #[test]
    fn test_manifest_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let manifest = sample_manifest("abc123", vec![1, 3], 5);

        write_manifest_atomic(tmp.path(), &manifest).unwrap();
        let loaded = read_manifest_tolerant(tmp.path()).unwrap();

        assert_eq!(loaded, manifest);
        // No leftover tmp file after a successful commit.
        assert!(!manifest_tmp_path(tmp.path()).exists());
    }

    #[test]
    fn test_manifest_absent_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(read_manifest_tolerant(tmp.path()).is_none());
    }

    #[test]
    fn test_manifest_corrupt_json_tolerated() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(manifest_path(tmp.path()), b"{ not valid json").unwrap();
        assert!(read_manifest_tolerant(tmp.path()).is_none());
    }

    #[test]
    fn test_manifest_only_tmp_present_is_treated_as_absent() {
        // Simulates a crash between `fs::write(&tmp_path, ...)` and the
        // `rename` — the tmp file alone must never be mistaken for a
        // committed manifest.
        let tmp = tempfile::tempdir().unwrap();
        let manifest = sample_manifest("abc123", vec![1], 1);
        let json = serde_json::to_vec_pretty(&manifest).unwrap();
        fs::write(manifest_tmp_path(tmp.path()), json).unwrap();

        assert!(read_manifest_tolerant(tmp.path()).is_none());
    }

    #[test]
    fn test_manifest_write_is_atomic_second_write_replaces_first() {
        let tmp = tempfile::tempdir().unwrap();
        let m1 = sample_manifest("abc123", vec![1], 1);
        let m2 = sample_manifest("abc123", vec![1, 2], 2);

        write_manifest_atomic(tmp.path(), &m1).unwrap();
        write_manifest_atomic(tmp.path(), &m2).unwrap();

        let loaded = read_manifest_tolerant(tmp.path()).unwrap();
        assert_eq!(loaded, m2);
    }

    fn sample_entries() -> Vec<KeymapEntry> {
        vec![
            KeymapEntry {
                key_hash: 111,
                global_id: 1,
                vec_checksum: 0xDEAD_BEEF,
                key: Bytes::from_static(b"doc:1"),
            },
            KeymapEntry {
                key_hash: 222,
                global_id: 2,
                vec_checksum: 0xCAFE_F00D,
                key: Bytes::from_static(b"doc:two-with-a-longer-key-name"),
            },
            KeymapEntry {
                key_hash: 333,
                global_id: 3,
                vec_checksum: 0,
                key: Bytes::from_static(b""),
            },
        ]
    }

    #[test]
    fn test_keymap_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let entries = sample_entries();

        write_keymap_atomic(tmp.path(), 9, &entries).unwrap();
        let loaded = read_keymap_tolerant(tmp.path(), 9).unwrap();

        assert_eq!(loaded.len(), entries.len());
        for e in &entries {
            assert!(loaded.contains(e));
        }
        assert!(!keymap_tmp_path(tmp.path(), 9).exists());
    }

    #[test]
    fn test_keymap_empty_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        write_keymap_atomic(tmp.path(), 1, &[]).unwrap();
        let loaded = read_keymap_tolerant(tmp.path(), 1).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_keymap_absent_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(read_keymap_tolerant(tmp.path(), 42).is_none());
    }

    #[test]
    fn test_keymap_bad_magic_tolerated() {
        let tmp = tempfile::tempdir().unwrap();
        write_keymap_atomic(tmp.path(), 1, &sample_entries()).unwrap();
        let path = keymap_path(tmp.path(), 1);
        let mut bytes = fs::read(&path).unwrap();
        bytes[0] = b'X'; // corrupt the magic
        fs::write(&path, &bytes).unwrap();

        assert!(read_keymap_tolerant(tmp.path(), 1).is_none());
    }

    #[test]
    fn test_keymap_corrupt_checksum_tolerated() {
        let tmp = tempfile::tempdir().unwrap();
        write_keymap_atomic(tmp.path(), 1, &sample_entries()).unwrap();
        let path = keymap_path(tmp.path(), 1);
        let mut bytes = fs::read(&path).unwrap();
        // Flip a byte inside the payload (past the 20-byte header) without
        // touching the stored checksum — must be detected as corrupt.
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        fs::write(&path, &bytes).unwrap();

        assert!(read_keymap_tolerant(tmp.path(), 1).is_none());
    }

    #[test]
    fn test_keymap_truncated_tolerated() {
        let tmp = tempfile::tempdir().unwrap();
        write_keymap_atomic(tmp.path(), 1, &sample_entries()).unwrap();
        let path = keymap_path(tmp.path(), 1);
        let bytes = fs::read(&path).unwrap();
        fs::write(&path, &bytes[..bytes.len() - 5]).unwrap();

        assert!(read_keymap_tolerant(tmp.path(), 1).is_none());
    }

    #[test]
    fn test_keymap_only_tmp_present_is_treated_as_absent() {
        let tmp = tempfile::tempdir().unwrap();
        // Write directly to the tmp path, simulating a crash before rename.
        fs::write(keymap_tmp_path(tmp.path(), 3), b"MKM1garbage").unwrap();
        assert!(read_keymap_tolerant(tmp.path(), 3).is_none());
    }

    #[test]
    fn test_orphan_sweep_correctness() {
        let manifest = sample_manifest("abc", vec![1, 3], 12);
        let entries: Vec<String> = vec![
            "segment-1".to_owned(),         // live — kept
            "segment-2".to_owned(),         // stale — orphan
            "segment-3".to_owned(),         // live — kept
            "keymap-10.bin".to_owned(),     // stale epoch — orphan
            "keymap-12.bin".to_owned(),     // live — kept
            "staging-4".to_owned(),         // interrupted build — orphan
            "manifest.json".to_owned(),     // never swept
            "manifest.json.tmp".to_owned(), // never swept by this helper (not segment/keymap-named)
        ];

        let mut orphans = compute_orphans(&entries, &manifest);
        orphans.sort();
        assert_eq!(
            orphans,
            vec![
                "keymap-10.bin".to_owned(),
                "segment-2".to_owned(),
                "staging-4".to_owned(),
            ]
        );
    }

    #[test]
    fn test_orphan_sweep_treats_stray_tmp_keymap_as_orphan() {
        // A "keymap-3.bin.tmp" left over from an interrupted write starts
        // with "keymap-" but does not parse as "keymap-<epoch>.bin" — it
        // must be swept (it's definitely garbage), not silently kept.
        let manifest = sample_manifest("abc", vec![], 3);
        let entries = vec!["keymap-3.bin.tmp".to_owned(), "keymap-3.bin".to_owned()];
        let orphans = compute_orphans(&entries, &manifest);
        assert_eq!(orphans, vec!["keymap-3.bin.tmp".to_owned()]);
    }

    #[test]
    fn test_sweep_orphans_from_disk_deletes_only_orphans() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        fs::create_dir_all(dir.join("segment-1")).unwrap();
        fs::create_dir_all(dir.join("segment-2")).unwrap();
        fs::create_dir_all(dir.join("staging-9")).unwrap();
        fs::write(dir.join("keymap-1.bin"), b"live").unwrap();
        fs::write(dir.join("keymap-0.bin"), b"stale").unwrap();
        fs::write(dir.join("manifest.json"), b"{}").unwrap();

        let manifest = sample_manifest("abc", vec![1], 1);
        let removed = sweep_orphans_from_disk(dir, &manifest).unwrap();

        assert_eq!(removed.len(), 3);
        assert!(dir.join("segment-1").exists());
        assert!(!dir.join("segment-2").exists());
        assert!(!dir.join("staging-9").exists());
        assert!(dir.join("keymap-1.bin").exists());
        assert!(!dir.join("keymap-0.bin").exists());
        assert!(dir.join("manifest.json").exists());
    }

    fn checksum_map(entries: &[KeymapEntry]) -> Arc<std::collections::HashMap<u64, u64>> {
        Arc::new(
            entries
                .iter()
                .map(|e| (e.key_hash, e.vec_checksum))
                .collect(),
        )
    }
    fn global_id_map(entries: &[KeymapEntry]) -> Arc<std::collections::HashMap<u64, u32>> {
        Arc::new(entries.iter().map(|e| (e.key_hash, e.global_id)).collect())
    }
    fn key_map(entries: &[KeymapEntry]) -> Arc<std::collections::HashMap<u64, Bytes>> {
        Arc::new(
            entries
                .iter()
                .map(|e| (e.key_hash, e.key.clone()))
                .collect(),
        )
    }

    #[test]
    fn test_run_snapshot_job_commits_and_advances_watermark() {
        let tmp = tempfile::tempdir().unwrap();
        let entries = sample_entries();
        let manifest = sample_manifest("abc", vec![1], 1);
        let watermark = Arc::new(Mutex::new(0u64));

        run_snapshot_job(SnapshotJob {
            idx_dir: tmp.path().to_path_buf(),
            seq: 1,
            watermark: watermark.clone(),
            manifest: manifest.clone(),
            key_hash_to_key: key_map(&entries),
            key_hash_to_global_id: global_id_map(&entries),
            key_hash_to_vec_checksum: checksum_map(&entries),
        });

        assert_eq!(*watermark.lock(), 1);
        let loaded_manifest = read_manifest_tolerant(tmp.path()).unwrap();
        assert_eq!(loaded_manifest, manifest);
        let loaded_keymap = read_keymap_tolerant(tmp.path(), 1).unwrap();
        assert_eq!(loaded_keymap.len(), entries.len());
    }

    #[test]
    fn test_run_snapshot_job_drops_stale_job_without_io() {
        let tmp = tempfile::tempdir().unwrap();
        let entries = sample_entries();
        let watermark = Arc::new(Mutex::new(5u64));
        let manifest = sample_manifest("abc", vec![1], 3);

        run_snapshot_job(SnapshotJob {
            idx_dir: tmp.path().to_path_buf(),
            seq: 3, // <= watermark(5) -> stale
            watermark: watermark.clone(),
            manifest,
            key_hash_to_key: key_map(&entries),
            key_hash_to_global_id: global_id_map(&entries),
            key_hash_to_vec_checksum: checksum_map(&entries),
        });

        assert_eq!(
            *watermark.lock(),
            5,
            "stale job must not move the watermark"
        );
        assert!(
            read_manifest_tolerant(tmp.path()).is_none(),
            "stale job must not write anything"
        );
    }

    #[test]
    fn test_run_snapshot_job_gcs_superseded_segments_and_keymap() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let entries = sample_entries();
        let watermark = Arc::new(Mutex::new(0u64));

        // Seed an "old" committed state: segment-1, segment-2 live, keymap epoch 1.
        fs::create_dir_all(dir.join("segment-1")).unwrap();
        fs::create_dir_all(dir.join("segment-2")).unwrap();
        let old_manifest = sample_manifest("abc", vec![1, 2], 1);
        write_manifest_atomic(dir, &old_manifest).unwrap();
        write_keymap_atomic(dir, 1, &entries).unwrap();
        *watermark.lock() = 1;

        // A merge superseded segment-1 and segment-2 with a new segment-3.
        fs::create_dir_all(dir.join("segment-3")).unwrap();
        let new_manifest = sample_manifest("abc", vec![3], 2);

        run_snapshot_job(SnapshotJob {
            idx_dir: dir.to_path_buf(),
            seq: 2,
            watermark: watermark.clone(),
            manifest: new_manifest.clone(),
            key_hash_to_key: key_map(&entries),
            key_hash_to_global_id: global_id_map(&entries),
            key_hash_to_vec_checksum: checksum_map(&entries),
        });

        assert_eq!(*watermark.lock(), 2);
        assert!(
            !dir.join("segment-1").exists(),
            "superseded segment must be GC'd"
        );
        assert!(
            !dir.join("segment-2").exists(),
            "superseded segment must be GC'd"
        );
        assert!(dir.join("segment-3").exists(), "live segment must survive");
        assert!(
            !keymap_path(dir, 1).exists(),
            "superseded keymap epoch must be GC'd"
        );
        assert!(
            keymap_path(dir, 2).exists(),
            "current keymap epoch must survive"
        );
        assert_eq!(read_manifest_tolerant(dir).unwrap(), new_manifest);
    }

    #[test]
    fn test_index_persist_dir_and_hex_are_deterministic() {
        let a = index_persist_dir(Path::new("/tmp/shard-0-vectors"), b"myidx");
        let b = index_persist_dir(Path::new("/tmp/shard-0-vectors"), b"myidx");
        assert_eq!(a, b);
        assert!(a.to_string_lossy().contains("idx-"));
        assert_eq!(index_name_hex(b"myidx").len(), 16);
    }
}
