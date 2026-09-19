//! Where a shard's cold-tier `file_id` counter may resume after a restart
//! (moon#997, moon#893).
//!
//! One per-shard counter names every id-addressed artifact under the shard's
//! disk-offload directory:
//!
//! | artifact | path | manifest `file_type` |
//! |---|---|---|
//! | KV spill file | `data/heap-{id:06}.mpf` (`.tmp` while being written) | `KvLeaf` |
//! | warm vector segment | `vectors/segment-{id}/` (`.segment-{id}.staging` while being written) | `VecCodes` |
//!
//! A restart must resume the counter strictly above every id any of them
//! holds. Re-issuing a held id is silent data loss in three different ways:
//! a spill renames its batch onto a LIVE `heap-*.mpf` (moon#997); a warm
//! transition collides with a live segment directory (moon#893); and a
//! manifest entry of one type shares its id with an entry of another, so a
//! mutator that matches on id alone retires both.
//!
//! [`next_file_id_seed`] is the single authority. It takes the maximum over
//! the manifest — every entry, every type, every status — AND the files on
//! disk, because each covers what the other cannot: a spill that wrote its
//! file but crashed before the manifest commit is on disk only (and a
//! crash-orphan sweep deletes that path later, so reusing it would delete the
//! new file), while an entry whose file is already gone is in the manifest
//! only.
//!
//! It fails CLOSED. A directory it cannot list, an entry it cannot read, or a
//! manifest it cannot open is an error, never a smaller number: a seed that
//! cannot be proven above every live id is not a seed. The caller refuses to
//! start the server on that error.

use std::cell::Cell;
use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};

use crate::persistence::manifest::ShardManifest;

/// Why a shard's `file_id` seed could not be proven safe.
#[derive(Debug, thiserror::Error)]
pub enum FileIdSeedError {
    /// A directory that holds id-named artifacts exists but could not be listed.
    #[error(
        "cannot list {dir} to find the highest file_id in use: {cause}. Fix the \
         permission or I/O error and restart; nothing needs to be removed"
    )]
    ListDir {
        /// The directory.
        dir: PathBuf,
        /// The underlying error (in the message, so every log line that
        /// prints this error names the OS reason).
        cause: io::Error,
    },
    /// Listing began but one entry could not be read — the highest id may be
    /// exactly the entry that was lost.
    #[error(
        "cannot read an entry of {dir} while finding the highest file_id in use: \
         {cause}. Fix the I/O error and restart; nothing needs to be removed"
    )]
    ReadEntry {
        /// The directory being listed.
        dir: PathBuf,
        /// The underlying error (in the message, so every log line that
        /// prints this error names the OS reason).
        cause: io::Error,
    },
    /// The shard manifest exists at its full length but could not be opened.
    /// (A file shorter than its two root pages is a torn create and is
    /// treated as empty, never reported here.)
    #[error(
        "cannot open shard manifest {path} to find the highest file_id in use: \
         {cause}. {advice}"
    )]
    Manifest {
        /// The manifest path.
        path: PathBuf,
        /// The underlying error (in the message, so every log line that
        /// prints this error names the OS reason).
        cause: io::Error,
        /// What the operator can safely do about it.
        advice: &'static str,
    },
    /// Every id up to `u64::MAX` is already used.
    #[error("file_id space exhausted: an artifact already holds id {max}")]
    Exhausted {
        /// The highest id in use.
        max: u64,
    },
}

/// The first `file_id` a shard may allocate: one past the highest id held by
/// any manifest entry or any id-named file or directory under `shard_dir`.
///
/// `shard_dir` is the shard's disk-offload directory
/// (`<offload>/shard-{shard_id}`). Returns `1` when nothing exists yet — a
/// fresh shard, or one whose directories were never created (`NotFound` is
/// the one error that proves there is nothing to collide with).
///
/// # Errors
///
/// Fails closed — see [`FileIdSeedError`] and the module docs.
pub fn next_file_id_seed(shard_dir: &Path, shard_id: usize) -> Result<u64, FileIdSeedError> {
    let manifest_max = manifest_max_file_id(&shard_dir.join(format!("shard-{shard_id}.manifest")))?;
    let heap_max = max_id_in_dir(&shard_dir.join("data"), heap_file_id)?;
    let segment_max = max_id_in_dir(&shard_dir.join("vectors"), segment_dir_id)?;

    match [manifest_max, heap_max, segment_max]
        .into_iter()
        .flatten()
        .max()
    {
        None => Ok(1),
        Some(max) => max.checked_add(1).ok_or(FileIdSeedError::Exhausted { max }),
    }
}

/// Allocate cold file ids from the shard's ONE counter.
///
/// A shard has exactly one source of cold file ids — the `Rc<Cell<u64>>`
/// seeded by [`next_file_id_seed`] and shared by the connection handlers, the
/// cross-shard SPSC drain, the scripting bridge, the eviction tick and the
/// warm-vector transitions. Every consumer that takes a `&mut u64` cursor
/// borrows it through here: the cursor starts at the counter's current value
/// and is written back with `max`, so the counter never moves backwards.
///
/// It used to have a second home — an event-loop local re-synced with
/// `max(local, cell)` once per tick and written back with a plain `set`. The
/// SPSC drain ran between the sync and the eviction tick, so the tick re-issued
/// the drain's ids and then LOWERED the cell, and the next handler spill
/// re-issued them again (moon#997 review).
///
/// `f` must be synchronous and must not itself allocate from `counter`: the
/// shard thread is single-threaded, so nothing else can allocate while `f`
/// runs, which is what makes the borrowed cursor exact.
pub fn allocate_from<R>(counter: &Cell<u64>, f: impl FnOnce(&mut u64) -> R) -> R {
    let mut next = counter.get();
    let out = f(&mut next);
    counter.set(counter.get().max(next));
    out
}

/// For a corrupt manifest: removing it is NOT the way past the refusal.
const CORRUPT_MANIFEST_ADVICE: &str = "Both root pages are unreadable, so this \
     file DID hold entries: do NOT delete it to get past this - without it the \
     next boot classifies every heap-*.mpf beside it as a crash orphan and deletes \
     it. Restore the manifest from a backup; the only safe removal is the whole \
     shard directory, and only if its cold keys may be lost";

/// For any other open error (permissions, I/O): nothing is damaged.
const IO_MANIFEST_ADVICE: &str =
    "Fix the permission or I/O error and restart; nothing needs to be removed";

/// Highest id held by any entry of the manifest at `path`; `None` when there
/// is no manifest or it is a torn create.
///
/// A file shorter than the two root pages can only be a `create` that never
/// finished (`ShardManifest::is_torn_create`): it committed no entry, so it
/// contributes no id. It is reported and treated as empty — failing on it
/// would refuse every boot over a file that holds nothing. A full-length
/// manifest that cannot be opened still fails closed: it did hold entries.
fn manifest_max_file_id(path: &Path) -> Result<Option<u64>, FileIdSeedError> {
    let open_error = |cause: io::Error| {
        let advice = if cause.kind() == io::ErrorKind::InvalidData {
            CORRUPT_MANIFEST_ADVICE
        } else {
            IO_MANIFEST_ADVICE
        };
        FileIdSeedError::Manifest {
            path: path.to_path_buf(),
            cause,
            advice,
        }
    };
    match ShardManifest::is_torn_create(path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(cause) => Err(open_error(cause)),
        Ok(true) => {
            tracing::warn!(
                path = %path.display(),
                "shard manifest is shorter than its two root pages: an interrupted \
                 create that committed no entry - it contributes no file_id (the \
                 torn file is safe to remove)"
            );
            Ok(None)
        }
        Ok(false) => Ok(ShardManifest::open(path)
            .map_err(open_error)?
            .files()
            .iter()
            .map(|e| e.file_id)
            .max()),
    }
}

/// The id of a spill file name the writer produces: `heap-{id}.mpf`, or the
/// `heap-{id}.tmp` it renames from. The `.tmp` counts: the crash-orphan sweep
/// deletes such a leftover by path in the background, so a new spill that
/// reused its id would have its in-progress file deleted under it.
fn heap_file_id(name: &str) -> Option<u64> {
    let rest = name.strip_prefix("heap-")?;
    let digits = rest
        .strip_suffix(".mpf")
        .or_else(|| rest.strip_suffix(".tmp"))?;
    parse_id(digits)
}

/// The id of a warm segment directory name: `segment-{id}`, or the
/// `.segment-{id}.staging` a transition renames from.
fn segment_dir_id(name: &str) -> Option<u64> {
    let digits = name.strip_prefix("segment-").or_else(|| {
        name.strip_prefix(".segment-")
            .and_then(|r| r.strip_suffix(".staging"))
    })?;
    parse_id(digits)
}

/// Decimal digits only. A name that does not parse cannot collide with any
/// name the writers produce (`{id}` / `{id:06}` of a `u64` always parses), so
/// ignoring it cannot under-count.
fn parse_id(digits: &str) -> Option<u64> {
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// Highest id among the entries of `dir` that `parse` recognises. `NotFound`
/// for `dir` itself means no artifact of this kind was ever written.
fn max_id_in_dir(
    dir: &Path,
    parse: fn(&str) -> Option<u64>,
) -> Result<Option<u64>, FileIdSeedError> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(cause) => {
            return Err(FileIdSeedError::ListDir {
                dir: dir.to_path_buf(),
                cause,
            });
        }
    };
    max_id_in_entries(dir, entries.map(|r| r.map(|e| e.file_name())), parse)
}

/// [`max_id_in_dir`] over an already-open listing. Split out so the
/// per-entry error — which no real filesystem produces on demand — can be
/// injected by the tests.
fn max_id_in_entries(
    dir: &Path,
    entries: impl Iterator<Item = io::Result<OsString>>,
    parse: fn(&str) -> Option<u64>,
) -> Result<Option<u64>, FileIdSeedError> {
    let mut max: Option<u64> = None;
    for entry in entries {
        let name = entry.map_err(|cause| FileIdSeedError::ReadEntry {
            dir: dir.to_path_buf(),
            cause,
        })?;
        // A non-UTF-8 name is not one the writers produce (they are ASCII).
        if let Some(id) = name.to_str().and_then(parse) {
            max = Some(max.map_or(id, |m| m.max(id)));
        }
    }
    Ok(max)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::manifest::{FileEntry, FileStatus, StorageTier};
    use crate::persistence::page::PageType;

    fn entry(file_id: u64, file_type: PageType, status: FileStatus) -> FileEntry {
        FileEntry {
            file_id,
            file_type: file_type as u8,
            status,
            tier: StorageTier::Hot,
            page_size_log2: 12,
            page_count: 1,
            byte_size: 4096,
            created_lsn: 0,
            db_index: 0,
            max_key_hash: 0,
            last_modified_lsn: 0,
        }
    }

    fn touch(path: &Path) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, b"x").unwrap();
    }

    /// The counter hands every id out once and never moves backwards, even
    /// when a consumer's cursor ends below what the counter already reached.
    #[test]
    fn allocate_from_is_the_one_monotonic_counter() {
        let counter = Cell::new(100);
        // A consumer allocates three ids.
        let ids: Vec<u64> = allocate_from(&counter, |next| {
            (0..3)
                .map(|_| {
                    let id = *next;
                    *next += 1;
                    id
                })
                .collect()
        });
        assert_eq!(ids, vec![100, 101, 102]);
        assert_eq!(counter.get(), 103);
        // The next consumer starts where that one stopped: no id repeats.
        let next_first = allocate_from(&counter, |next| {
            let id = *next;
            *next += 1;
            id
        });
        assert_eq!(next_first, 103);
        // A consumer whose cursor ends LOWER (a stale copy) cannot pull the
        // counter back — the old plain `set` did, and the next spill reused
        // ids already on disk.
        allocate_from(&counter, |next| *next = 50);
        assert_eq!(counter.get(), 104, "the counter never moves backwards");
    }

    #[test]
    fn fresh_shard_seeds_at_one() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(next_file_id_seed(tmp.path(), 0).unwrap(), 1);
        // Directories that exist but are empty are just as fresh.
        std::fs::create_dir_all(tmp.path().join("data")).unwrap();
        std::fs::create_dir_all(tmp.path().join("vectors")).unwrap();
        assert_eq!(next_file_id_seed(tmp.path(), 0).unwrap(), 1);
    }

    #[test]
    fn heap_files_seed_above_the_highest_including_tmp_leftovers() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        for id in [3u64, 513, 42] {
            touch(&data.join(format!("heap-{id:06}.mpf")));
        }
        assert_eq!(next_file_id_seed(tmp.path(), 0).unwrap(), 514);
        // An interrupted batch write above every sealed file.
        touch(&data.join("heap-000900.tmp"));
        assert_eq!(next_file_id_seed(tmp.path(), 0).unwrap(), 901);
        // Past the 6-digit pad the writer's names grow; they still parse.
        touch(&data.join("heap-1234567.mpf"));
        assert_eq!(next_file_id_seed(tmp.path(), 0).unwrap(), 1_234_568);
    }

    /// moon#893: a shard whose highest id is a warm vector segment — the old
    /// seed scanned `data/` only and answered 1 here.
    #[test]
    fn vector_segments_alone_seed_above_the_highest_segment() {
        let tmp = tempfile::tempdir().unwrap();
        let vectors = tmp.path().join("vectors");
        std::fs::create_dir_all(vectors.join("segment-7")).unwrap();
        std::fs::create_dir_all(vectors.join("segment-19")).unwrap();
        assert_eq!(next_file_id_seed(tmp.path(), 0).unwrap(), 20);
        // A transition that died before its rename.
        std::fs::create_dir_all(vectors.join(".segment-30.staging")).unwrap();
        assert_eq!(next_file_id_seed(tmp.path(), 0).unwrap(), 31);
    }

    #[test]
    fn highest_of_heap_segment_and_manifest_wins() {
        let tmp = tempfile::tempdir().unwrap();
        touch(&tmp.path().join("data").join("heap-000010.mpf"));
        std::fs::create_dir_all(tmp.path().join("vectors").join("segment-12")).unwrap();
        assert_eq!(next_file_id_seed(tmp.path(), 3).unwrap(), 13);

        // An entry whose file is already gone still holds its id — and a
        // Tombstone does too (its file may still be open by a reader until
        // gc_tombstones prunes it).
        let mut m = ShardManifest::create(&tmp.path().join("shard-3.manifest")).unwrap();
        m.add_file(entry(40, PageType::KvLeaf, FileStatus::Active));
        m.add_file(entry(55, PageType::VecCodes, FileStatus::Tombstone));
        m.commit().unwrap();
        assert_eq!(next_file_id_seed(tmp.path(), 3).unwrap(), 56);

        // Another shard's manifest name is not this shard's manifest.
        assert_eq!(
            next_file_id_seed(tmp.path(), 4).unwrap(),
            13,
            "shard 4 must not read shard-3.manifest"
        );
    }

    /// Names outside the writers' grammar are ignored — none of them is a
    /// name a writer could produce, so ignoring them cannot under-count.
    #[test]
    fn foreign_names_are_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        touch(&data.join("heap-000004.mpf"));
        for name in [
            "heap-.mpf",
            "heap-+99.mpf",
            "heap-99x.mpf",
            "heap-000099.bak",
            "heap-99999999999999999999999.mpf",
            "README",
        ] {
            touch(&data.join(name));
        }
        std::fs::create_dir_all(tmp.path().join("vectors").join("segment-abc")).unwrap();
        assert_eq!(next_file_id_seed(tmp.path(), 0).unwrap(), 5);
    }

    /// moon#997: a cold dir that exists but cannot be listed. The old seed
    /// answered 1 — the most dangerous possible answer. `ENOTDIR` stands in
    /// for `EACCES` here because permission bits do not bind root. Unix only:
    /// Windows may report listing a regular file as `NotFound`, which is the
    /// one error the seed treats as "nothing here".
    #[cfg(unix)]
    #[test]
    fn unlistable_directory_fails_closed() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("data"), b"not a directory").unwrap();
        match next_file_id_seed(tmp.path(), 0) {
            Err(FileIdSeedError::ListDir { dir, .. }) => {
                assert_eq!(dir, tmp.path().join("data"));
            }
            other => panic!("an unlistable data dir must fail closed, got {other:?}"),
        }

        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("vectors"), b"not a directory").unwrap();
        assert!(matches!(
            next_file_id_seed(tmp.path(), 0),
            Err(FileIdSeedError::ListDir { .. })
        ));
    }

    /// moon#997, the half the issue did not name: one entry the kernel could
    /// not return. `.flatten()` used to drop it — and it may be exactly the
    /// highest id.
    #[test]
    fn unreadable_entry_fails_closed_instead_of_being_skipped() {
        let dir = Path::new("/nonexistent/data");
        let entries = vec![
            Ok(OsString::from("heap-000002.mpf")),
            Err(io::Error::other("EIO while reading the directory")),
            Ok(OsString::from("heap-000001.mpf")),
        ];
        match max_id_in_entries(dir, entries.into_iter(), heap_file_id) {
            Err(FileIdSeedError::ReadEntry { dir: d, .. }) => assert_eq!(d, dir),
            other => panic!("a lost directory entry must fail closed, got {other:?}"),
        }
        // Control: the same listing without the error answers normally.
        let entries = vec![
            Ok(OsString::from("heap-000002.mpf")),
            Ok(OsString::from("heap-000001.mpf")),
        ];
        assert_eq!(
            max_id_in_entries(dir, entries.into_iter(), heap_file_id).unwrap(),
            Some(2)
        );
    }

    /// A full-length manifest whose roots are both corrupt DID hold entries:
    /// fail closed, and say that deleting it is not the way out.
    #[test]
    fn corrupt_manifest_fails_closed() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("shard-0.manifest"), vec![0xA5u8; 8192]).unwrap();
        match next_file_id_seed(tmp.path(), 0) {
            Err(e @ FileIdSeedError::Manifest { .. }) => {
                let msg = e.to_string();
                assert!(msg.contains("do NOT delete it"), "{msg}");
            }
            other => panic!("a corrupt full-length manifest must fail closed, got {other:?}"),
        }
    }

    /// moon#997 review: a manifest shorter than its two root pages is a torn
    /// create — no committed entry — and must not block startup forever.
    #[test]
    fn torn_create_manifest_is_treated_as_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        touch(&data.join("heap-000007.mpf"));
        for len in [0usize, 100, 8191] {
            std::fs::write(tmp.path().join("shard-0.manifest"), vec![0u8; len]).unwrap();
            assert_eq!(
                next_file_id_seed(tmp.path(), 0).unwrap(),
                8,
                "a {len}-byte manifest holds no entry; the disk still counts"
            );
        }
    }

    #[test]
    fn exhausted_id_space_fails_closed() {
        let tmp = tempfile::tempdir().unwrap();
        touch(
            &tmp.path()
                .join("data")
                .join(format!("heap-{}.mpf", u64::MAX)),
        );
        assert!(matches!(
            next_file_id_seed(tmp.path(), 0),
            Err(FileIdSeedError::Exhausted { max: u64::MAX })
        ));
    }
}
