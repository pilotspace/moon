//! Durable fsync helpers for crash-safe persistence.
//!
//! These functions ensure metadata and data durability on disk after
//! atomic rename operations, WAL truncation, and segment writes.

use std::path::Path;

/// Fsync a directory to ensure rename/unlink metadata durability.
///
/// Required after: snapshot rename, segment staging rename, WAL segment creation.
/// On POSIX systems, directory fsync makes the directory entry durable so that
/// a power failure after rename does not lose the new name.
///
/// On Windows this is a no-op (beyond an existence check): directory handles
/// cannot be opened for flushing without `FILE_FLAG_BACKUP_SEMANTICS`, and
/// NTFS journals rename metadata without an explicit directory flush — the
/// same approach LevelDB/RocksDB take on Windows.
pub fn fsync_directory(dir: &Path) -> std::io::Result<()> {
    #[cfg(test)]
    dir_fsync_probe::record(dir);
    #[cfg(unix)]
    {
        let f = std::fs::File::open(dir)?;
        f.sync_all()
    }
    #[cfg(not(unix))]
    {
        // Preserve error semantics for bad paths so callers still surface them.
        if dir.is_dir() {
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("not a directory: {}", dir.display()),
            ))
        }
    }
}

/// `create_dir_all` whose directory entries survive a power loss.
///
/// Fsyncs the parent of `path` and the parent of every ancestor this call
/// created, so a file later committed under `path` (a manifest, a base RDB)
/// is never left in a directory the next boot cannot see. The parent of
/// `path` is fsynced even when `path` already existed: its entry may have
/// been created by an earlier, unsynced call. An ancestor this process may
/// not open (`EACCES`) is logged and skipped; any other error propagates.
pub fn create_dir_all_durable(path: &Path) -> std::io::Result<()> {
    // Newly created directories, deepest first.
    let mut missing: Vec<&Path> = Vec::new();
    let mut cur = path;
    while !cur.exists() {
        missing.push(cur);
        match cur.parent() {
            Some(p) if !p.as_os_str().is_empty() => cur = p,
            _ => break,
        }
    }
    std::fs::create_dir_all(path)?;
    fsync_parent_entry(parent_or_cwd(path))?;
    // `missing[0]` is `path` itself, whose parent was just fsynced.
    for dir in missing.iter().skip(1) {
        fsync_parent_entry(parent_or_cwd(dir))?;
    }
    Ok(())
}

/// Fsync a directory this process may not be allowed to open: an ancestor
/// such as an execute-only (`0711`) home directory refuses `open`, and
/// refusing to boot over that would be worse than the power-loss window it
/// closes. Any other error (EIO from the fsync itself) still propagates.
fn fsync_parent_entry(dir: &Path) -> std::io::Result<()> {
    match fsync_directory(dir) {
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            tracing::warn!(
                "cannot fsync {} to persist a new directory entry: {e}",
                dir.display()
            );
            Ok(())
        }
        r => r,
    }
}

/// The directory holding `path`'s entry (`.` for a bare relative name).
fn parent_or_cwd(path: &Path) -> &Path {
    match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    }
}

/// Test-only record of the directory fsyncs this thread issues, each with the
/// entry names the directory held at that instant: an entry is durable only
/// if a directory fsync ran after it was created.
#[cfg(test)]
pub(crate) mod dir_fsync_probe {
    use std::cell::RefCell;
    use std::ffi::OsString;
    use std::path::{Path, PathBuf};

    type Log = Vec<(PathBuf, Vec<OsString>)>;

    thread_local! {
        static LOG: RefCell<Option<Log>> = const { RefCell::new(None) };
    }

    /// Start recording on this thread.
    pub(crate) fn start() {
        LOG.with(|l| *l.borrow_mut() = Some(Vec::new()));
    }

    /// Stop recording and return what was recorded.
    pub(crate) fn stop() -> Log {
        LOG.with(|l| l.borrow_mut().take().unwrap_or_default())
    }

    pub(super) fn record(dir: &Path) {
        LOG.with(|l| {
            if let Some(log) = l.borrow_mut().as_mut() {
                let names = std::fs::read_dir(dir)
                    .map(|it| it.filter_map(|e| e.ok().map(|e| e.file_name())).collect())
                    .unwrap_or_default();
                log.push((dir.to_path_buf(), names));
            }
        });
    }
}

/// Fsync a file to ensure data durability before rename.
///
/// Flushes OS page cache and filesystem metadata to stable storage.
/// POSIX allows fsync on a read-only descriptor; Windows
/// `FlushFileBuffers` requires a writable handle, so the file is opened
/// with write access there (contents are never modified).
pub fn fsync_file(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    let f = std::fs::File::open(path)?;
    #[cfg(not(unix))]
    let f = std::fs::OpenOptions::new().write(true).open(path)?;
    f.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fsync_directory() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(fsync_directory(tmp.path()).is_ok());
    }

    #[test]
    fn test_fsync_file() {
        let tmp = tempfile::tempdir().unwrap();
        let file_path = tmp.path().join("test.dat");
        std::fs::write(&file_path, b"hello world").unwrap();
        assert!(fsync_file(&file_path).is_ok());
    }

    #[test]
    fn create_dir_all_durable_fsyncs_the_parent_of_every_new_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let leaf = tmp.path().join("a").join("b").join("c");
        dir_fsync_probe::start();
        create_dir_all_durable(&leaf).unwrap();
        let synced: Vec<_> = dir_fsync_probe::stop()
            .into_iter()
            .map(|(d, _)| d)
            .collect();
        assert!(leaf.is_dir());
        // Each new entry (a, b, c) is durable: its parent was fsynced after it existed.
        for parent in [
            tmp.path().to_path_buf(),
            tmp.path().join("a"),
            tmp.path().join("a").join("b"),
        ] {
            assert!(
                synced.contains(&parent),
                "{} not fsynced: {synced:?}",
                parent.display()
            );
        }
    }

    #[test]
    fn create_dir_all_durable_on_an_existing_dir_still_fsyncs_its_parent() {
        let tmp = tempfile::tempdir().unwrap();
        let leaf = tmp.path().join("appendonlydir");
        std::fs::create_dir(&leaf).unwrap();
        dir_fsync_probe::start();
        create_dir_all_durable(&leaf).unwrap();
        let synced: Vec<_> = dir_fsync_probe::stop()
            .into_iter()
            .map(|(d, _)| d)
            .collect();
        assert_eq!(synced, vec![tmp.path().to_path_buf()]);
    }

    #[test]
    fn test_fsync_nonexistent_returns_error() {
        let result = fsync_directory(Path::new("/nonexistent/path/that/does/not/exist"));
        assert!(result.is_err());
    }
}
