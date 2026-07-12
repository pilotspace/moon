//! Shared atomic file-write primitive for crash-safe durable persistence.
//!
//! Kernel M2 stage 1 (K3, `.planning/reviews/kernel-m2-brief-2026-07-12.md`):
//! every durable on-disk artifact in this codebase used a hand-rolled copy
//! of the same four-step sequence, and several copies quietly dropped a
//! step (usually the directory fsync). This module is the ONE place that
//! sequence lives now.
//!
//! ## Contract
//!
//! [`atomic_write_durable`] writes `bytes` to `path` such that a kill-9 at
//! any point leaves `path` either:
//! - completely absent (crash before rename), or
//! - the complete PREVIOUS contents (crash before rename), or
//! - the complete NEW contents, durable across a subsequent reboot (crash
//!   after rename + dir-fsync) or durable-once-fsync-replayed (crash after
//!   rename, before dir-fsync — most filesystems still preserve the rename
//!   in this window, but this function does not promise that; see below).
//!
//! It never leaves `path` holding a torn/partial write, because the new
//! bytes only ever become visible at `path` via a single `rename(2)` — an
//! atomic directory-entry swap on every platform this codebase targets.
//!
//! The four steps:
//! 1. Write `bytes` to a hidden same-directory temp file (`.{filename}.tmp`).
//! 2. `fsync` the temp file's data (page cache -> stable storage) before
//!    anyone can observe it under the final name.
//! 3. `rename(2)` the temp file over `path` — the only visible state
//!    transition.
//! 4. `fsync` the parent directory so the rename's directory-entry update
//!    itself survives a crash. On `data=ordered` (ext4) and similar POSIX
//!    filesystems, a crash between step 3 and step 4 can revert the
//!    directory entry to the old name even though the rename() call
//!    returned success — the parent-directory fsync is what makes the
//!    rename durable, not the rename call itself.
//!
//! A crash between steps 1-2 leaves only an orphaned temp file — never a
//! corruption, just wasted disk (the next successful write truncates and
//! reuses the same temp name via `File::create`). A crash between steps
//! 3-4 leaves the rename possibly-not-durable (step 4's error, if it
//! surfaces at all, is reported to the caller — see the `SyncDir` variant).
//!
//! ## Payload-before-reference ordering rule
//!
//! Whenever a durable artifact (e.g. a CSR graph segment, a vector
//! immutable segment) is referenced by a second, higher-level pointer file
//! (e.g. a manifest listing which segments are currently live), the
//! payload's `atomic_write_durable` call MUST return `Ok(())` before the
//! reference is written. A manifest that is more durable than the bytes it
//! points at is exactly the crash window this module exists to close:
//! recovery reads a segment id out of the manifest and finds nothing (or,
//! pre-K3, a torn write) on disk. This module only fixes the atomicity of
//! ONE write; ordering two separate `atomic_write_durable` calls correctly
//! is the caller's responsibility. See `src/graph/recovery.rs::save_graph_store`
//! for a compliant sequence: every segment file is durable before
//! `GraphManifest::save` runs.
//!
//! ## Reference implementation
//!
//! Extracted from the vector engine's Stack-B index/keymap sidecar writers
//! (`src/vector/persistence/manifest.rs::write_manifest_atomic` /
//! `write_keymap_atomic`) — the first call sites in this codebase to get
//! the full four-step sequence right, including the directory fsync most
//! earlier call sites were missing (see the K3 audit,
//! `.planning/reviews/storage-audit-2026-07-12-graph-fts.md`).

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::persistence::fsync::fsync_directory;

/// Error returned by [`atomic_write_durable`], tagged with the stage of the
/// write -> fsync -> rename -> dir-fsync sequence that failed so callers
/// (and their logs) can tell a "the new content never became durable"
/// failure apart from a "the content IS live, we just couldn't prove the
/// last inch of durability" one — the two demand different recovery
/// actions from a caller gating further progress (e.g. a WAL checkpoint)
/// on this write succeeding.
#[derive(Debug, Error)]
pub enum AtomicWriteError {
    /// `path` has no usable parent directory to stage a temp file in or to
    /// fsync (e.g. a bare filename with no directory component, or `/`).
    #[error("path has no parent directory to write into: {0}")]
    NoParentDir(PathBuf),
    /// Writing the temp file's contents failed. `path` (if it already
    /// existed) is untouched; nothing durable was attempted.
    #[error("failed to write temp file {path}: {source}")]
    WriteTemp { path: PathBuf, source: io::Error },
    /// `fsync` on the temp file failed before rename. `path` (if it
    /// already existed) is untouched. The orphaned temp file is harmless —
    /// the next successful call truncates and reuses the same name.
    #[error("failed to fsync temp file {path}: {source}")]
    SyncTemp { path: PathBuf, source: io::Error },
    /// `rename(2)` failed. Both the original `path` (untouched) and the
    /// temp file (orphaned, safe to leave) still exist.
    #[error("failed to rename {from} -> {to}: {source}")]
    Rename {
        from: PathBuf,
        to: PathBuf,
        source: io::Error,
    },
    /// The rename succeeded — `path` now holds the new content and any
    /// reader opening it sees the complete new bytes — but the
    /// parent-directory fsync failed, so the rename's directory-entry
    /// update is not provably durable across a crash. Callers that gate
    /// further progress on durability (a WAL checkpoint advancing its
    /// replay floor, a manifest about to reference this file) MUST treat
    /// this the same as a hard failure: the write is live but not proven
    /// durable.
    #[error("wrote and renamed {path} but failed to fsync its parent directory: {source}")]
    SyncDir { path: PathBuf, source: io::Error },
}

impl From<AtomicWriteError> for io::Error {
    fn from(e: AtomicWriteError) -> Self {
        let kind = match &e {
            AtomicWriteError::NoParentDir(_) => io::ErrorKind::InvalidInput,
            AtomicWriteError::WriteTemp { source, .. }
            | AtomicWriteError::SyncTemp { source, .. }
            | AtomicWriteError::Rename { source, .. }
            | AtomicWriteError::SyncDir { source, .. } => source.kind(),
        };
        io::Error::new(kind, e.to_string())
    }
}

/// Build the same-directory hidden temp-file path used to stage `path`'s
/// new contents: `{parent}/.{file_name}.tmp`. Hidden (dot-prefixed) to
/// match the convention already used by the text/vector sidecar writers
/// this module consolidates, and so it never collides with a glob over the
/// final artifact's own extension.
///
/// Returns `None` when `path` has no parent directory or no file name
/// component (both treated as [`AtomicWriteError::NoParentDir`] by the
/// caller).
fn temp_path_for(path: &Path) -> Option<PathBuf> {
    let parent = path.parent().filter(|p| !p.as_os_str().is_empty())?;
    let file_name = path.file_name()?.to_string_lossy();
    Some(parent.join(format!(".{file_name}.tmp")))
}

/// Atomically write `bytes` to `path`. See the module docs for the full
/// contract.
///
/// `path`'s parent directory must already exist — this function does not
/// create it (every existing call site already calls
/// `fs::create_dir_all` itself when the directory may be fresh, and
/// silently creating directories here would hide caller bugs).
#[must_use = "an Err means the write is not durable (or not written at all) — never ignore it"]
pub fn atomic_write_durable(path: &Path, bytes: &[u8]) -> Result<(), AtomicWriteError> {
    let tmp_path =
        temp_path_for(path).ok_or_else(|| AtomicWriteError::NoParentDir(path.to_path_buf()))?;
    // Parent existence was already implied by `temp_path_for` returning
    // `Some`, but re-derive it cheaply for the final dir-fsync rather than
    // threading it through — `path.parent()` is O(1).
    let parent = tmp_path
        .parent()
        .expect("temp_path_for always returns a path with a parent");

    let mut f = File::create(&tmp_path).map_err(|source| AtomicWriteError::WriteTemp {
        path: tmp_path.clone(),
        source,
    })?;
    f.write_all(bytes)
        .map_err(|source| AtomicWriteError::WriteTemp {
            path: tmp_path.clone(),
            source,
        })?;
    f.sync_all().map_err(|source| AtomicWriteError::SyncTemp {
        path: tmp_path.clone(),
        source,
    })?;
    drop(f);

    fs::rename(&tmp_path, path).map_err(|source| AtomicWriteError::Rename {
        from: tmp_path.clone(),
        to: path.to_path_buf(),
        source,
    })?;

    fsync_directory(parent).map_err(|source| AtomicWriteError::SyncDir {
        path: path.to_path_buf(),
        source,
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn test_creates_file_with_correct_contents() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("artifact.bin");

        atomic_write_durable(&path, b"hello durable world").unwrap();

        assert_eq!(fs::read(&path).unwrap(), b"hello durable world");
    }

    #[test]
    fn test_overwrites_existing_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("artifact.bin");

        atomic_write_durable(&path, b"version one").unwrap();
        atomic_write_durable(&path, b"version two, longer content").unwrap();

        assert_eq!(fs::read(&path).unwrap(), b"version two, longer content");
    }

    #[test]
    fn test_no_leftover_temp_file_on_success() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("artifact.bin");

        atomic_write_durable(&path, b"payload").unwrap();

        let entries: Vec<_> = fs::read_dir(tmp.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(entries, vec![std::ffi::OsString::from("artifact.bin")]);
    }

    #[test]
    fn test_temp_path_is_hidden_and_same_directory() {
        let path = Path::new("/data/shard_0/manifest.json");
        let tmp = temp_path_for(path).unwrap();
        assert_eq!(tmp, Path::new("/data/shard_0/.manifest.json.tmp"));
    }

    #[test]
    fn test_no_parent_dir_returns_error_not_panic() {
        // A bare relative filename has `Path::parent() == Some("")`, which
        // this module treats as "no usable parent" rather than silently
        // writing into an ambiguous cwd-relative temp name.
        let result = atomic_write_durable(Path::new("bare-filename.bin"), b"x");
        assert!(matches!(result, Err(AtomicWriteError::NoParentDir(_))));
    }

    #[test]
    fn test_original_untouched_when_temp_write_fails() {
        // Force the WriteTemp stage to fail by pre-creating a *directory*
        // at the temp-file path — `File::create` on a path that is already
        // a directory fails with an I/O error (portable across platforms,
        // unlike permission-bit tricks).
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("artifact.bin");
        fs::write(&path, b"original content").unwrap();
        let tmp_path = temp_path_for(&path).unwrap();
        fs::create_dir(&tmp_path).unwrap();

        let result = atomic_write_durable(&path, b"new content");

        assert!(matches!(result, Err(AtomicWriteError::WriteTemp { .. })));
        // The original file must be completely unaffected by the failed
        // attempt — no partial state, no truncation.
        assert_eq!(fs::read(&path).unwrap(), b"original content");
    }

    #[test]
    fn test_missing_parent_directory_fails_loud_not_panic() {
        let path = Path::new("/nonexistent-dir-for-moon-atomic-write-test/artifact.bin");
        let result = atomic_write_durable(path, b"x");
        assert!(matches!(result, Err(AtomicWriteError::WriteTemp { .. })));
    }

    #[test]
    fn test_atomic_write_error_converts_to_io_error() {
        let path = Path::new("bare-filename.bin");
        let result = atomic_write_durable(path, b"x");
        let io_err: io::Error = result.unwrap_err().into();
        assert_eq!(io_err.kind(), io::ErrorKind::InvalidInput);
    }

    /// Rename atomicity, proven rather than assumed: a concurrent reader
    /// hammering `fs::read` while a writer repeatedly alternates between
    /// two differently-sized payloads must NEVER observe anything other
    /// than one of the two complete payloads (or the file not existing
    /// yet, before the first rename). This is the property a regression to
    /// a bare `fs::write(path, bytes)` (no temp file, no rename) would
    /// break — a concurrent reader could catch a partially-written file.
    #[test]
    fn test_concurrent_readers_never_see_a_torn_write() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("target.dat");
        let payload_a = vec![b'A'; 200 * 1024];
        let payload_b = vec![b'B'; 350 * 1024];

        atomic_write_durable(&path, &payload_a).unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let writer_stop = stop.clone();
        let writer_path = path.clone();
        let (pa, pb) = (payload_a.clone(), payload_b.clone());
        let writer = std::thread::spawn(move || {
            let mut i = 0u32;
            while !writer_stop.load(Ordering::Relaxed) && i < 300 {
                let payload = if i % 2 == 0 { &pa } else { &pb };
                atomic_write_durable(&writer_path, payload).unwrap();
                i += 1;
            }
        });

        for _ in 0..5000 {
            match fs::read(&path) {
                Ok(data) => {
                    assert!(
                        data == payload_a || data == payload_b,
                        "torn read: got {} bytes, matching neither full payload",
                        data.len()
                    );
                }
                Err(e) => {
                    // A rename mid-flight can transiently report NotFound
                    // on some platforms between unlinking the old name and
                    // linking the new one; anything else is unexpected.
                    assert_eq!(e.kind(), io::ErrorKind::NotFound);
                }
            }
        }
        stop.store(true, Ordering::Relaxed);
        writer.join().unwrap();
    }
}
