//! Data-dir instance lock: refuse a second moon on the same `--dir`.
//!
//! Two moon processes writing the same directory means two writers on the
//! same per-shard WAL / AOF / spill manifests — silent corruption with no
//! error at either end (observed in dev as dozens of leaked instances
//! accumulating against shared folders, 2026-07-16). The guard is a
//! kernel-advisory `flock(2)` on `<dir>/moon.lock`, taken exclusively and
//! non-blocking at startup and held for the life of the process:
//!
//! * `flock` dies WITH the process — `kill -9` releases it instantly, so a
//!   crash-restart never sees a stale lock (the classic pidfile failure
//!   mode). This is exactly the property the kill-9 crash suites rely on.
//! * The lock file's CONTENT (holder pid) is diagnostic only; the kernel
//!   lock is the source of truth.
//! * Advisory means unrelated tools reading the directory are unaffected.
//!
//! The guard must stay alive until exit — dropping it unlocks. `main()`
//! binds it for its full scope. Note `std::fs::File` opens with
//! `O_CLOEXEC`, so the `--memory-arenas-cap` `execve` re-spawn releases and
//! re-acquires the lock across the exec boundary (same pid, no window for a
//! competing instance to corrupt: the re-exec'd image locks again before
//! opening any data file).
//!
//! Windows: no `flock`; the guard is a warn-once no-op (Windows is a
//! compile target, not a production one — see CLAUDE.md Target Platform).

use std::path::Path;

pub const LOCK_FILE_NAME: &str = "moon.lock";

/// Holds the exclusive lock on a data directory for the process lifetime.
/// Dropping releases the lock — keep it bound until exit.
#[derive(Debug)]
pub struct DirLock {
    #[cfg(unix)]
    _lock: nix::fcntl::Flock<std::fs::File>,
}

/// Why the lock could not be taken.
#[derive(Debug, thiserror::Error)]
pub enum DirLockError {
    #[error(
        "data dir {dir:?} is locked by another moon instance{holder} — \
         two servers on the same directory would corrupt its WAL/manifests; \
         stop the other instance or use a different --dir"
    )]
    Held {
        dir: std::path::PathBuf,
        holder: String,
    },
    #[error("failed to open lock file in {dir:?}: {source}")]
    Io {
        dir: std::path::PathBuf,
        source: std::io::Error,
    },
}

/// Take the exclusive instance lock on `dir` (which must already exist).
/// Fails fast with [`DirLockError::Held`] when another live process holds it.
#[cfg(unix)]
pub fn acquire(dir: &Path) -> Result<DirLock, DirLockError> {
    use std::io::{Read, Seek, Write};

    let lock_path = dir.join(LOCK_FILE_NAME);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|source| DirLockError::Io {
            dir: dir.to_path_buf(),
            source,
        })?;

    match nix::fcntl::Flock::lock(file, nix::fcntl::FlockArg::LockExclusiveNonblock) {
        Ok(mut lock) => {
            // Best-effort holder-pid breadcrumb for the refusal diagnostic
            // and for operators inspecting the dir. Failures here are
            // non-fatal — the kernel lock is already held.
            let _ = lock.set_len(0);
            let _ = lock.rewind();
            let _ = write!(&mut *lock, "{}", std::process::id());
            let _ = lock.flush();
            Ok(DirLock { _lock: lock })
        }
        Err((mut file, _errno)) => {
            let mut pid = String::new();
            let _ = file.read_to_string(&mut pid);
            let pid = pid.trim();
            let holder = if pid.is_empty() {
                String::new()
            } else {
                format!(" (pid {pid})")
            };
            Err(DirLockError::Held {
                dir: dir.to_path_buf(),
                holder,
            })
        }
    }
}

#[cfg(not(unix))]
pub fn acquire(dir: &Path) -> Result<DirLock, DirLockError> {
    tracing::warn!(
        dir = %dir.display(),
        "data-dir instance lock is a no-op on this platform — do not run \
         two moon instances against the same directory"
    );
    Ok(DirLock {})
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn acquire_then_reacquire_same_process_dir_pair() {
        let dir = tempfile::tempdir().expect("tempdir");
        let first = acquire(dir.path()).expect("first acquire");
        // flock is per-open-file-description: a SECOND open in the same
        // process still conflicts, which is what models a second instance
        // closely enough for a unit test (process-level proof lives in
        // tests/instance_lock.rs).
        let second = acquire(dir.path());
        assert!(
            matches!(second, Err(DirLockError::Held { .. })),
            "second open-description acquire must be refused: {second:?}"
        );
        drop(first);
        let third = acquire(dir.path()).expect("re-acquire after drop");
        drop(third);
    }

    #[test]
    fn held_error_carries_holder_pid() {
        let dir = tempfile::tempdir().expect("tempdir");
        let _first = acquire(dir.path()).expect("first acquire");
        let err = acquire(dir.path()).expect_err("second must fail");
        let msg = err.to_string();
        assert!(
            msg.contains(&std::process::id().to_string()),
            "diagnostic should name the holder pid: {msg}"
        );
        assert!(msg.to_lowercase().contains("lock"), "diagnostic: {msg}");
    }

    #[test]
    fn io_error_when_dir_missing() {
        let missing = std::path::Path::new("/nonexistent-moon-lock-dir-42");
        assert!(matches!(acquire(missing), Err(DirLockError::Io { .. })));
    }
}
