//! The legacy single-file AOF (`<dir>/appendonly.aof`): its name, and what a
//! boot does when that file cannot be read (R2b round 2).
//!
//! Since R2b review P1 the file, once it holds a record, is the ONLY KV
//! source of a boot (`recovery::KvSources::AofOnly`): the snapshot is not
//! loaded. A file whose replay fails outright — an RDB preamble that does not
//! load ("no valid EOF+CRC found"), an I/O error — therefore cannot fall back
//! to anything, and booting EMPTY then served an empty dataset and appended
//! new writes behind the unreadable bytes, so they were lost too. redis
//! 7.2.7 exits with status 1 on the same file. A boot that meets one refuses to start instead,
//! before any AOF writer opens the file ([`UnreadableAof`]).
//!
//! Mid-stream corruption is a refusal too (R2b round 3 F-B; redis "Bad file
//! format"). A clean truncated tail — a record torn by a crash — is not: the
//! boot cuts it off the file before anything is appended behind it
//! ([`super::torn_tail`]).

use std::path::{Path, PathBuf};

/// The file the legacy (flat) layout replays and appends: the boot's replay
/// reads exactly this name in the persistence dir.
pub const FLAT_AOF_NAME: &str = "appendonly.aof";

/// `<dir>/appendonly.aof`.
pub fn flat_aof_path(dir: &Path) -> PathBuf {
    dir.join(FLAT_AOF_NAME)
}

/// A flat AOF the boot could not replay: the boot must refuse to start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnreadableAof {
    pub path: PathBuf,
    pub error: String,
}

impl UnreadableAof {
    pub fn new(path: &Path, error: impl std::fmt::Display) -> Self {
        Self {
            path: path.to_path_buf(),
            error: error.to_string(),
        }
    }

    /// The operator-facing refusal: what failed, why nothing else is loaded,
    /// and the remedies.
    pub fn message(&self) -> String {
        format!(
            "refusing to start: {} could not be replayed ({}). With --appendonly yes it is the \
             only source of the dataset, so booting would serve an EMPTY dataset and append \
             new writes behind the unreadable bytes. Remedies: restore the file from a backup; \
             or, to keep the records before the damage, truncate a copy at the byte offset \
             named above (truncate -s <offset> <copy>) and boot from it; or, to boot from the \
             snapshot instead, move {} aside and restart (the snapshot then loads and a new \
             AOF is opened over it).",
            self.path.display(),
            self.error,
            FLAT_AOF_NAME
        )
    }
}

/// A manifest now owns this dir's AOF (monoio, or `--shards N` PerShard):
/// retire the flat `appendonly.aof` by renaming it to `appendonly.aof.legacy`
/// (`.legacy.<n>` when that name is taken — an older retired file is never
/// overwritten). Every branch that creates or replays a manifest calls it
/// once the manifest is the authority (R2b round 2 F2): a flat file left
/// behind — even one holding only a fresh generation's head — becomes the
/// ONLY KV source of a later tokio `--shards 1` boot (`KvSources::AofOnly`),
/// which then booted without the manifest's data and appended to the stale
/// file. Returns the new name, `None` when there was no flat file.
pub fn retire(dir: &Path) -> std::io::Result<Option<PathBuf>> {
    match rename_aside(dir)? {
        None => Ok(None),
        Some(to) => {
            crate::persistence::fsync::fsync_directory(dir)?;
            Ok(Some(to))
        }
    }
}

/// The rename of [`retire`], without the directory fsync.
fn rename_aside(dir: &Path) -> std::io::Result<Option<PathBuf>> {
    let flat = flat_aof_path(dir);
    if !flat.exists() {
        return Ok(None);
    }
    let mut retired = dir.join(format!("{FLAT_AOF_NAME}.legacy"));
    let mut n = 1u32;
    while retired.exists() {
        retired = dir.join(format!("{FLAT_AOF_NAME}.legacy.{n}"));
        n += 1;
    }
    std::fs::rename(&flat, &retired)?;
    Ok(Some(retired))
}

/// [`retire`], logged: a failure is loud (the next tokio `--shards 1` boot
/// would take the stale file as its dataset), never fatal (this boot's
/// manifest is already committed). A rename that succeeded but whose
/// directory fsync failed is told apart: the file IS aside, but a power loss
/// before the directory reaches disk can bring it back.
pub fn retire_logged(dir: &Path) {
    let flat = flat_aof_path(dir);
    match rename_aside(dir) {
        Ok(Some(to)) => match crate::persistence::fsync::fsync_directory(dir) {
            Ok(()) => tracing::info!(
                "Retired legacy AOF {} -> {} (the AOF manifest is the authority now)",
                flat.display(),
                to.display()
            ),
            Err(e) => tracing::error!(
                "Retired legacy AOF {} -> {}, but fsyncing {} failed: {e}. The rename may \
                 not survive a power loss: after one, check that {} is gone before booting \
                 this dir with tokio --shards 1 (it would load it as the whole dataset)",
                flat.display(),
                to.display(),
                dir.display(),
                flat.display()
            ),
        },
        Ok(None) => {}
        Err(e) => tracing::error!(
            "Failed to retire legacy AOF {}: {e}. Move it aside by hand: a tokio --shards 1 \
             boot of this dir would load it as the whole dataset",
            flat.display()
        ),
    }
}

/// A snapshot the boot skips because `appendonly.aof` is the only KV source
/// (R2b round 2 F3): when it was written AFTER the AOF was last appended and
/// holds keys, it may be a backup the operator just restored — which this
/// boot ignores. Say so, at WARN, with the remedy. (A crash right after a
/// BGSAVE with no later write also matches; the line then costs nothing.)
pub fn note_skipped_snapshot(snapshot: &Path, dir: &Path, databases: usize) {
    if let Some(len) = newer_snapshot_holding_keys(snapshot, &flat_aof_path(dir), databases) {
        tracing::warn!(
            "snapshot {} ({len} bytes, written after {} was last appended) is NOT loaded: \
             with --appendonly yes the AOF is the only source of the dataset. If you \
             restored this snapshot from a backup, stop moon, move {} (and appendonlydir/, \
             if present) aside, and restart: the snapshot then loads and a new AOF is \
             opened over it",
            snapshot.display(),
            flat_aof_path(dir).display(),
            FLAT_AOF_NAME
        );
    }
}

/// `Some(snapshot length)` when `snapshot` is newer than `aof` (mtime) and
/// larger than an empty snapshot of `databases` databases.
fn newer_snapshot_holding_keys(snapshot: &Path, aof: &Path, databases: usize) -> Option<u64> {
    let snap = std::fs::metadata(snapshot).ok()?;
    let aof = std::fs::metadata(aof).ok()?;
    if snap.modified().ok()? <= aof.modified().ok()? {
        return None;
    }
    (snap.len() > empty_snapshot_len(databases)?).then_some(snap.len())
}

/// The size of a snapshot holding no key, for `databases` databases (it
/// carries per-database sections): written once to a temp file and measured.
fn empty_snapshot_len(databases: usize) -> Option<u64> {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp = std::env::temp_dir().join(format!(
        "moon-empty-snapshot-{}-{seq}.rrdshard",
        std::process::id()
    ));
    let dbs: Vec<crate::storage::Database> = (0..databases.max(1))
        .map(|_| crate::storage::Database::new())
        .collect();
    let saved = crate::persistence::snapshot::shard_snapshot_save(0, 0, &dbs, &tmp);
    let len = saved
        .ok()
        .and_then(|_| std::fs::metadata(&tmp).ok())
        .map(|m| m.len());
    let _ = std::fs::remove_file(&tmp);
    len
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_refusal_names_the_file_the_error_and_the_remedy() {
        let u = UnreadableAof::new(Path::new("/d/appendonly.aof"), "no valid EOF+CRC found");
        let m = u.message();
        assert!(m.contains("/d/appendonly.aof"), "{m}");
        assert!(m.contains("no valid EOF+CRC found"), "{m}");
        assert!(m.contains("move appendonly.aof aside"), "{m}");
        assert_eq!(
            flat_aof_path(Path::new("/d")),
            PathBuf::from("/d/appendonly.aof")
        );
    }

    /// A snapshot written after the AOF and holding a key is flagged; an
    /// older one, or an empty one, is not.
    #[test]
    fn a_newer_snapshot_with_keys_is_flagged() {
        use crate::storage::{Database, Entry};
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path();
        let aof = flat_aof_path(d);
        std::fs::write(&aof, b"*1\r\n$4\r\nPING\r\n").unwrap();
        let set_mtime = |p: &Path, ms: u64| {
            std::fs::File::options()
                .write(true)
                .open(p)
                .unwrap()
                .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_millis(ms))
                .unwrap();
        };
        set_mtime(&aof, 1_000_000);
        let snap = d.join("shard-0.rrdshard");
        let mut dbs: Vec<Database> = (0..16).map(|_| Database::new()).collect();
        crate::persistence::snapshot::shard_snapshot_save(0, 1, &dbs, &snap).unwrap();
        set_mtime(&snap, 2_000_000);
        assert_eq!(newer_snapshot_holding_keys(&snap, &aof, 16), None, "empty");
        dbs[0].set(b"k", Entry::new_string(bytes::Bytes::from_static(b"v")));
        crate::persistence::snapshot::shard_snapshot_save(0, 1, &dbs, &snap).unwrap();
        set_mtime(&snap, 2_000_000);
        assert!(
            newer_snapshot_holding_keys(&snap, &aof, 16).is_some(),
            "newer, with a key"
        );
        set_mtime(&snap, 500_000);
        assert_eq!(
            newer_snapshot_holding_keys(&snap, &aof, 16),
            None,
            "older than the AOF"
        );
    }

    #[test]
    fn retire_renames_and_never_overwrites_an_older_retired_file() {
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path();
        assert_eq!(retire(d).unwrap(), None, "no flat file");
        std::fs::write(d.join("appendonly.aof"), b"one").unwrap();
        assert_eq!(retire(d).unwrap(), Some(d.join("appendonly.aof.legacy")));
        std::fs::write(d.join("appendonly.aof"), b"two").unwrap();
        assert_eq!(retire(d).unwrap(), Some(d.join("appendonly.aof.legacy.1")));
        assert!(!d.join("appendonly.aof").exists());
        assert_eq!(
            std::fs::read(d.join("appendonly.aof.legacy")).unwrap(),
            b"one"
        );
        assert_eq!(
            std::fs::read(d.join("appendonly.aof.legacy.1")).unwrap(),
            b"two"
        );
    }
}
