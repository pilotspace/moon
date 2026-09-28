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
/// been created by an earlier, unsynced call.
///
/// A directory that ALREADY existed may refuse the fsync without failing
/// the call ([`dir_fsync_error_is_tolerable`]): an ancestor this process may
/// not open (`EACCES`, a `0711` home), or a filesystem without directory
/// fsync (`EINVAL` / `EBADF` / `ENOTSUP` — squashfs, erofs, iso9660,
/// vboxsf, WSL1 drvfs, procfs), as PostgreSQL's `fsync_fname_ext` ignores
/// `EBADF` / `EINVAL` on directories. `main.rs` fsyncs the parent of the
/// data directory on every boot, so refusing there made a server that booted
/// before stop booting. Any other error (`EIO`), and any error on a
/// directory this call created, propagates. A skip is a warning only when
/// the entry it leaves unsynced was created by this call; for a
/// pre-existing entry it is logged at debug, so a boot does not warn every
/// time.
pub fn create_dir_all_durable(path: &Path) -> std::io::Result<()> {
    for skip in create_dir_all_durable_with(path, fsync_directory)? {
        if skip.entry_created {
            tracing::warn!(
                "cannot fsync {} to persist the new directory entry {}: {}",
                skip.dir.display(),
                skip.entry.display(),
                skip.error
            );
        } else {
            tracing::debug!(
                "cannot fsync {} (entry {} already existed): {}",
                skip.dir.display(),
                skip.entry.display(),
                skip.error
            );
        }
    }
    Ok(())
}

/// A directory fsync [`create_dir_all_durable`] skipped.
#[derive(Debug)]
struct SkippedDirSync {
    /// The directory whose fsync failed.
    dir: std::path::PathBuf,
    /// The entry in `dir` the fsync would have made durable.
    entry: std::path::PathBuf,
    /// Whether this call created `entry`.
    entry_created: bool,
    error: std::io::Error,
}

/// [`create_dir_all_durable`] with the directory fsync injected (tests), and
/// the skipped fsyncs returned rather than logged.
fn create_dir_all_durable_with(
    path: &Path,
    mut fsync: impl FnMut(&Path) -> std::io::Result<()>,
) -> std::io::Result<Vec<SkippedDirSync>> {
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
    let mut skipped = Vec::new();
    // `missing[0]` is `path` itself (when it was created): its parent is
    // `missing[1]`, created by this call too, if there is one. The parent of
    // the last created directory already existed.
    let entries = std::iter::once((path, !missing.is_empty(), missing.len() > 1)).chain(
        missing
            .iter()
            .enumerate()
            .skip(1)
            .map(|(i, dir)| (*dir, true, i + 1 < missing.len())),
    );
    for (entry, entry_created, dir_created) in entries {
        let dir = parent_or_cwd(entry);
        if let Err(error) = fsync(dir) {
            if !dir_fsync_error_is_tolerable(&error, dir_created) {
                return Err(error);
            }
            skipped.push(SkippedDirSync {
                dir: dir.to_path_buf(),
                entry: entry.to_path_buf(),
                entry_created,
                error,
            });
        }
    }
    Ok(skipped)
}

/// May [`create_dir_all_durable`] skip a directory fsync that failed with
/// `error`? Only for a directory that already existed (`dir_created` false),
/// and only when the failure says the fsync cannot be done there at all: no
/// permission to open it, or a filesystem that does not fsync directories
/// (`EINVAL`, `EBADF`, `ENOTSUP` / `ErrorKind::Unsupported`). `EIO` — the
/// fsync ran and failed — is never skipped.
fn dir_fsync_error_is_tolerable(error: &std::io::Error, dir_created: bool) -> bool {
    if dir_created {
        return false;
    }
    if matches!(
        error.kind(),
        std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::Unsupported
    ) {
        return true;
    }
    #[cfg(unix)]
    {
        matches!(error.raw_os_error(), Some(libc::EINVAL | libc::EBADF))
    }
    #[cfg(not(unix))]
    {
        false
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

    #[cfg(unix)]
    fn os_err(code: i32) -> std::io::Error {
        std::io::Error::from_raw_os_error(code)
    }

    /// PR #1301 review round 3: a pre-existing directory that cannot be
    /// fsynced (no permission, or a filesystem without directory fsync) is
    /// skipped; `EIO`, and anything on a directory this call created, is not.
    #[cfg(unix)]
    #[test]
    fn dir_fsync_error_classification() {
        use std::io::{Error, ErrorKind};
        for (e, pre_existing_ok) in [
            (os_err(libc::EINVAL), true),
            (os_err(libc::EBADF), true),
            (os_err(libc::ENOTSUP), true),
            (os_err(libc::EACCES), true),
            (Error::from(ErrorKind::PermissionDenied), true),
            (Error::from(ErrorKind::Unsupported), true),
            (os_err(libc::EIO), false),
            (os_err(libc::ENOSPC), false),
            (Error::from(ErrorKind::NotFound), false),
        ] {
            assert_eq!(
                dir_fsync_error_is_tolerable(&e, false),
                pre_existing_ok,
                "{e:?}"
            );
            assert!(
                !dir_fsync_error_is_tolerable(&e, true),
                "created dir: {e:?}"
            );
        }
    }

    /// The pre-existing parent of a new data dir answers `EINVAL`: the dir is
    /// created, the skip is reported as leaving a NEW entry unsynced (a
    /// warning). On an already-existing dir the same skip is quiet.
    #[cfg(unix)]
    #[test]
    fn create_dir_all_durable_skips_einval_on_a_pre_existing_parent() {
        let tmp = tempfile::tempdir().unwrap();
        let leaf = tmp.path().join("data");
        let einval = |_: &Path| Err(os_err(libc::EINVAL));
        let skipped = create_dir_all_durable_with(&leaf, einval).unwrap();
        assert!(leaf.is_dir());
        assert_eq!(skipped.len(), 1, "{skipped:?}");
        assert_eq!(skipped[0].dir, tmp.path());
        assert!(skipped[0].entry_created, "the new entry's skip warns");
        // Every later boot: the dir exists, the skip is not warned about.
        let skipped = create_dir_all_durable_with(&leaf, einval).unwrap();
        assert_eq!(skipped.len(), 1, "{skipped:?}");
        assert!(
            !skipped[0].entry_created,
            "a pre-existing entry's skip is quiet"
        );
    }

    /// `EIO` is fatal even on a pre-existing parent; any error on a
    /// directory this call created is fatal.
    #[cfg(unix)]
    #[test]
    fn create_dir_all_durable_keeps_eio_and_created_dir_errors_fatal() {
        let tmp = tempfile::tempdir().unwrap();
        let eio =
            create_dir_all_durable_with(&tmp.path().join("x"), |_: &Path| Err(os_err(libc::EIO)))
                .unwrap_err();
        assert_eq!(eio.raw_os_error(), Some(libc::EIO));
        // `a/b/c` is new: `a/b` and `a` were created by this call and refuse
        // with EINVAL; the pre-existing `tmp` would be tolerated.
        let base = tmp.path().to_path_buf();
        let leaf = base.join("a").join("b").join("c");
        let err =
            create_dir_all_durable_with(&leaf, |_: &Path| Err(os_err(libc::EINVAL))).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EINVAL));
        // Only the pre-existing `tmp` refuses: tolerated, and the three new
        // entries' parents were all asked.
        let mut asked = Vec::new();
        let leaf2 = base.join("p").join("q");
        let skipped = create_dir_all_durable_with(&leaf2, |d: &Path| {
            asked.push(d.to_path_buf());
            if d == base {
                Err(os_err(libc::EINVAL))
            } else {
                Ok(())
            }
        })
        .unwrap();
        assert_eq!(asked, vec![base.join("p"), base.clone()]);
        assert_eq!(skipped.len(), 1);
        assert!(skipped[0].entry_created);
    }

    /// A real filesystem without directory fsync: procfs answers `EINVAL`.
    /// `/proc/self` exists, so its parent `/proc` is a pre-existing directory
    /// and the call succeeds (it failed with `EINVAL` before).
    #[cfg(target_os = "linux")]
    #[test]
    fn create_dir_all_durable_tolerates_procfs() {
        if fsync_directory(Path::new("/proc")).map_err(|e| e.raw_os_error())
            != Err(Some(libc::EINVAL))
        {
            return; // this kernel fsyncs /proc: nothing to show
        }
        create_dir_all_durable(Path::new("/proc/self")).unwrap();
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
