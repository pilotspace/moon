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
//! A clean truncated tail and mid-stream corruption are NOT refusals: replay
//! keeps the valid prefix, as before.

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
             or repair it (redis-check-aof --fix on a copy); or, to boot from the snapshot \
             instead, move {} aside and restart (the snapshot then loads and a new AOF is \
             opened over it).",
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
    crate::persistence::fsync::fsync_directory(dir)?;
    Ok(Some(retired))
}

/// [`retire`], logged: a failure is loud (the next tokio `--shards 1` boot
/// would take the stale file as its dataset), never fatal (this boot's
/// manifest is already committed).
pub fn retire_logged(dir: &Path) {
    match retire(dir) {
        Ok(Some(to)) => tracing::info!(
            "Retired legacy AOF {} -> {} (the AOF manifest is the authority now)",
            flat_aof_path(dir).display(),
            to.display()
        ),
        Ok(None) => {}
        Err(e) => tracing::error!(
            "Failed to retire legacy AOF {}: {e}. Move it aside by hand: a tokio --shards 1 \
             boot of this dir would load it as the whole dataset",
            flat_aof_path(dir).display()
        ),
    }
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
