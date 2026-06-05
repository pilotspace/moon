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
    fn test_fsync_nonexistent_returns_error() {
        let result = fsync_directory(Path::new("/nonexistent/path/that/does/not/exist"));
        assert!(result.is_err());
    }
}
