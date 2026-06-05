//! Multi-part AOF manifest: tracks base (RDB) and incremental (RESP) files.
//!
//! Part of the **storage format v1** umbrella commitment — see
//! [`docs/STORAGE-FORMAT-V1.md`](../../../docs/STORAGE-FORMAT-V1.md). The
//! manifest framing is the canonical on-disk marker; the human-readable
//! "v1" umbrella also covers WAL v3 and RDB v2 sub-formats.
//!
//! Two on-disk layouts coexist (selected at manifest creation time, never mixed
//! within one directory):
//!
//! **TopLevel (manifest v1, single-shard / legacy):**
//! ```text
//! appendonlydir/
//!   moon.aof.1.base.rdb     # RDB snapshot base
//!   moon.aof.1.incr.aof     # Incremental RESP since base
//!   moon.aof.manifest       # v1 text format
//! ```
//!
//! **PerShard (manifest v2, multi-shard durability):**
//! ```text
//! appendonlydir/
//!   moon.aof.manifest       # v2 text format (carries shard count + max_lsn)
//!   shard-0/
//!     moon.aof.1.base.rdb
//!     moon.aof.1.incr.aof
//!   shard-1/
//!     moon.aof.1.base.rdb
//!     moon.aof.1.incr.aof
//!   …
//! ```
//!
//! The manifest text format is line-prefix based. v1 manifests have no
//! `version` line; v2 manifests begin with `version 2`. On BGREWRITEAOF the
//! sequence increments, a new base + incr pair is created per shard (PerShard)
//! or at top level (TopLevel), and old files are deleted.

use std::io::Write;
use std::path::{Path, PathBuf};

use tracing::{error, info, warn};

use crate::persistence::fsync::fsync_directory;

mod shard_replay;
mod shard_rewrite;

// Re-export the relocated public replay API so external paths
// (`crate::persistence::aof_manifest::replay_*`) keep resolving (issue #143).
pub use shard_replay::{OrderedEntry, replay_multi_part, replay_ordered_merge, replay_per_shard};

const MANIFEST_NAME: &str = "moon.aof.manifest";
const AOF_DIR_NAME: &str = "appendonlydir";

/// Fsync the parent directory of `path` (best-effort).
///
/// POSIX guarantees atomicity of `rename()` but does NOT guarantee that the
/// directory entry update is durable after a crash. On ext4 and XFS without
/// `data=ordered`, a crash between the rename and a directory fsync can leave
/// the old file name visible on the next boot even though the rename completed
/// in memory. Calling this after every manifest-visible rename closes that gap.
///
/// Best-effort: logs on failure but does not propagate the error. A failed
/// dir fsync means the rename may not survive a crash — the worst case is
/// that recovery falls back to the previous manifest state, which is still
/// consistent (the atomic rename guarantees the file is either fully old or
/// fully new). Call sites that CAN propagate (i.e., are in a fallible fn that
/// returns `std::io::Result`) should call `fsync_directory(parent)?` directly.
fn fsync_parent_best_effort(path: &Path) {
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => return, // root or no parent — nothing to fsync
    };
    if let Err(e) = fsync_directory(parent) {
        warn!(
            "fsync_parent_best_effort: failed to fsync dir {} after rename of {}: {}",
            parent.display(),
            path.display(),
            e
        );
    }
}

/// On-disk layout discriminator.
///
/// `TopLevel` is the legacy single-shard layout from manifest v1. `PerShard`
/// is the multi-shard layout introduced with manifest v2 — used whenever
/// `num_shards >= 2`. A `--shards 1` deployment with an existing v1 manifest
/// stays TopLevel until explicitly migrated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AofLayout {
    /// Legacy single-shard layout: `appendonlydir/moon.aof.{seq}.{base|incr}.*`.
    TopLevel,
    /// Per-shard layout: `appendonlydir/shard-{N}/moon.aof.{seq}.{base|incr}.*`.
    PerShard,
}

/// Per-shard manifest entry. One per shard in `PerShard` layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardManifest {
    /// Shard ID (0..num_shards).
    pub shard_id: u16,
    /// Max LSN persisted to this shard's incr file so far. Semantics defined
    /// by step 3 (LSN tagging) of the per-shard AOF RFC — until then this is
    /// 0 and recovery does not use it. Once step 3 ships, recovery seeds
    /// `master_repl_offset = max(shards[*].max_lsn)` before accepting writes.
    pub max_lsn: u64,
}

/// Active AOF file set tracked by the manifest.
#[derive(Debug, Clone)]
pub struct AofManifest {
    /// Base directory (parent of `appendonlydir/`)
    pub dir: PathBuf,
    /// Current sequence number (incremented on each rewrite).
    pub seq: u64,
    /// On-disk layout. Determines path computation for base/incr files.
    pub layout: AofLayout,
    /// Per-shard metadata. Length is 1 for `TopLevel`, `num_shards` for
    /// `PerShard`. Indexed by `shard_id`.
    pub shards: Vec<ShardManifest>,
}

impl AofManifest {
    /// Path to the `appendonlydir/` directory.
    pub fn aof_dir(&self) -> PathBuf {
        self.dir.join(AOF_DIR_NAME)
    }

    /// Path to the manifest file.
    pub fn manifest_path(&self) -> PathBuf {
        self.aof_dir().join(MANIFEST_NAME)
    }

    /// Path to the base RDB file for the current sequence.
    ///
    /// Layout-aware: TopLevel returns `appendonlydir/moon.aof.{seq}.base.rdb`;
    /// PerShard routes to `appendonlydir/shard-0/moon.aof.{seq}.base.rdb`.
    /// This single-file helper is meaningful only when there is one shard
    /// (post-migration `--shards 1`); a multi-shard PerShard manifest has N
    /// base files and the caller must use [`Self::shard_base_path`] instead.
    /// In debug builds, calling this on a multi-shard PerShard manifest
    /// asserts; in release it returns the shard-0 path so production stays
    /// recoverable rather than panicking on a stale call site.
    pub fn base_path(&self) -> PathBuf {
        match self.layout {
            AofLayout::TopLevel => self
                .aof_dir()
                .join(format!("moon.aof.{}.base.rdb", self.seq)),
            AofLayout::PerShard => {
                debug_assert!(
                    self.shards.len() == 1,
                    "base_path() called on multi-shard PerShard manifest; use shard_base_path(shard_id)",
                );
                self.shard_base_path_seq(0, self.seq)
            }
        }
    }

    /// Path to the incremental RESP file for the current sequence.
    ///
    /// Layout-aware — see [`Self::base_path`] for the same routing rules.
    pub fn incr_path(&self) -> PathBuf {
        match self.layout {
            AofLayout::TopLevel => self
                .aof_dir()
                .join(format!("moon.aof.{}.incr.aof", self.seq)),
            AofLayout::PerShard => {
                debug_assert!(
                    self.shards.len() == 1,
                    "incr_path() called on multi-shard PerShard manifest; use shard_incr_path(shard_id)",
                );
                self.shard_incr_path_seq(0, self.seq)
            }
        }
    }

    /// Path to the base RDB file for a given sequence. Layout-aware — see
    /// [`Self::base_path`].
    pub fn base_path_seq(&self, seq: u64) -> PathBuf {
        match self.layout {
            AofLayout::TopLevel => self.aof_dir().join(format!("moon.aof.{}.base.rdb", seq)),
            AofLayout::PerShard => {
                debug_assert!(
                    self.shards.len() == 1,
                    "base_path_seq() called on multi-shard PerShard manifest; use shard_base_path_seq(shard_id, seq)",
                );
                self.shard_base_path_seq(0, seq)
            }
        }
    }

    /// Path to the incremental RESP file for a given sequence. Layout-aware —
    /// see [`Self::base_path`].
    pub fn incr_path_seq(&self, seq: u64) -> PathBuf {
        match self.layout {
            AofLayout::TopLevel => self.aof_dir().join(format!("moon.aof.{}.incr.aof", seq)),
            AofLayout::PerShard => {
                debug_assert!(
                    self.shards.len() == 1,
                    "incr_path_seq() called on multi-shard PerShard manifest; use shard_incr_path_seq(shard_id, seq)",
                );
                self.shard_incr_path_seq(0, seq)
            }
        }
    }

    /// Create the `appendonlydir/` and write the initial manifest.
    ///
    /// Prefer [`Self::initialize_with_base`] when the in-memory databases
    /// already contain state (e.g. first upgrade from legacy single-file AOF
    /// or per-shard WAL) — otherwise subsequent boots cannot reconstruct that
    /// state because there is no base RDB for `replay_multi_part` to load.
    ///
    /// B4 fix: even on fresh install (no prior state), materialize an EMPTY
    /// base RDB so the `(base + incr)` invariant always holds. Without this,
    /// the recovery path refuses to replay incr-only state and the server
    /// fails to restart after a graceful shutdown that only wrote incr.
    pub fn initialize(dir: &Path) -> std::io::Result<Self> {
        let manifest = Self {
            dir: dir.to_path_buf(),
            seq: 1,
            layout: AofLayout::TopLevel,
            shards: vec![ShardManifest {
                shard_id: 0,
                max_lsn: 0,
            }],
        };
        std::fs::create_dir_all(manifest.aof_dir())?;

        // Serialize an empty database vector to an empty base RDB so the
        // (base + incr) invariant holds from the first boot.
        let empty_dbs: [crate::storage::Database; 0] = [];
        let empty_rdb = crate::persistence::rdb::save_to_bytes(&empty_dbs)
            .map_err(|e| std::io::Error::other(format!("empty RDB serialize: {e}")))?;
        let base_path = manifest.base_path();
        let tmp_path = base_path.with_extension("rdb.tmp");
        {
            let mut f = std::fs::File::create(&tmp_path)?;
            f.write_all(&empty_rdb)?;
            f.sync_data()?;
        }
        std::fs::rename(&tmp_path, &base_path)?;
        fsync_parent_best_effort(&base_path);

        // Create the empty incr file so the writer has a target.
        std::fs::File::create(manifest.incr_path())?;

        manifest.write_manifest()?;
        Ok(manifest)
    }

    /// Create the `appendonlydir/` and write an initial manifest with a base RDB
    /// capturing the current in-memory state.
    ///
    /// Used on first upgrade from legacy persistence formats: after
    /// `restore_from_persistence` has loaded state from the per-shard WAL or
    /// `appendonly.aof`, this call materializes that state as the seq 1 base
    /// RDB. Without a base, on the next boot the multi-part replay path would
    /// clear the databases and then fail (missing base with non-empty incr)
    /// or silently restart from empty state.
    pub fn initialize_with_base(dir: &Path, rdb_bytes: &[u8]) -> std::io::Result<Self> {
        let manifest = Self {
            dir: dir.to_path_buf(),
            seq: 1,
            layout: AofLayout::TopLevel,
            shards: vec![ShardManifest {
                shard_id: 0,
                max_lsn: 0,
            }],
        };
        std::fs::create_dir_all(manifest.aof_dir())?;

        // Write base RDB atomically: tmp file + fsync + rename.
        let base_path = manifest.base_path();
        let tmp_path = base_path.with_extension("rdb.tmp");
        {
            let mut f = std::fs::File::create(&tmp_path)?;
            f.write_all(rdb_bytes)?;
            f.sync_data()?;
        }
        std::fs::rename(&tmp_path, &base_path)?;
        fsync_parent_best_effort(&base_path);

        // Create empty incr file so the writer has something to append to.
        std::fs::File::create(manifest.incr_path())?;

        manifest.write_manifest()?;
        Ok(manifest)
    }

    /// Load manifest from disk.
    ///
    /// Returns:
    /// - `Ok(None)` — manifest file does not exist (fresh install or legacy single-file AOF)
    /// - `Ok(Some(manifest))` — manifest loaded successfully
    /// - `Err(_)` — manifest file exists but is unreadable or corrupt.
    ///   Callers MUST treat this as fatal: overwriting a corrupt manifest with a
    ///   fresh one silently destroys the reference to the real base RDB and loses data.
    pub fn load(dir: &Path) -> std::io::Result<Option<Self>> {
        let aof_dir = dir.join(AOF_DIR_NAME);
        let manifest_path = aof_dir.join(MANIFEST_NAME);

        if !manifest_path.exists() {
            return Ok(None);
        }

        let content = std::fs::read_to_string(&manifest_path)?;

        // Detect format version. v1 manifests have no `version` line and use
        // line prefixes `seq`/`base`/`incr`. v2 manifests start with `version 2`
        // and carry per-shard records.
        let mut format_version: u8 = 1;
        for line in content.lines() {
            let line = line.trim();
            if let Some(val) = line.strip_prefix("version ") {
                if let Ok(v) = val.parse::<u8>() {
                    format_version = v;
                }
                break;
            }
            if !line.is_empty() {
                // First non-blank line is not a version header → v1.
                break;
            }
        }

        let manifest = match format_version {
            1 => Self::parse_v1(&content, dir, &manifest_path)?,
            2 => Self::parse_v2(&content, dir, &manifest_path)?,
            other => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "AOF manifest at {} has unsupported format version {} (max supported: 2)",
                        manifest_path.display(),
                        other,
                    ),
                ));
            }
        };

        // Best-effort orphan cleanup: delete stray base/incr files from aborted
        // rewrites. A crash between advance() steps 1-3 leaves a new base RDB on
        // disk that the active manifest never references. Without this sweep,
        // repeated crashes during rewrite can fill the disk with zombie files.
        //
        // Safe to call here: parse_* verified the manifest has all required
        // records, so cleanup_orphans won't delete the active files.
        manifest.cleanup_orphans();

        Ok(Some(manifest))
    }

    /// Parse a v1 (TopLevel, single-shard) manifest.
    fn parse_v1(content: &str, dir: &Path, manifest_path: &Path) -> std::io::Result<Self> {
        let mut seq = 0u64;
        let mut has_base_record = false;
        let mut has_incr_record = false;
        for line in content.lines() {
            let line = line.trim();
            if let Some(val) = line.strip_prefix("seq ") {
                if let Ok(n) = val.parse::<u64>() {
                    seq = n;
                }
            } else if line.starts_with("base ") {
                has_base_record = true;
            } else if line.starts_with("incr ") {
                has_incr_record = true;
            }
        }

        if seq == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "AOF manifest at {} has no valid sequence number",
                    manifest_path.display()
                ),
            ));
        }

        if !has_base_record || !has_incr_record {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "AOF manifest at {} is truncated: seq={} base={} incr={}",
                    manifest_path.display(),
                    seq,
                    has_base_record,
                    has_incr_record,
                ),
            ));
        }

        Ok(Self {
            dir: dir.to_path_buf(),
            seq,
            layout: AofLayout::TopLevel,
            shards: vec![ShardManifest {
                shard_id: 0,
                max_lsn: 0,
            }],
        })
    }

    /// Parse a v2 (PerShard, multi-shard) manifest.
    ///
    /// Expected line format:
    /// ```text
    /// version 2
    /// seq N
    /// shards K
    /// shard 0 max_lsn LSN0
    /// shard 1 max_lsn LSN1
    /// ...
    /// ```
    ///
    /// Per-shard `base`/`incr` paths are derived from `shard-{N}/moon.aof.{seq}.*`
    /// rather than stored explicitly — the layout is canonical, so storing
    /// paths invites drift between the stored value and the computed one.
    fn parse_v2(content: &str, dir: &Path, manifest_path: &Path) -> std::io::Result<Self> {
        let mut seq = 0u64;
        let mut num_shards: Option<u16> = None;
        let mut shards: Vec<ShardManifest> = Vec::new();

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if line == "version 2" {
                continue;
            } else if let Some(val) = line.strip_prefix("seq ") {
                seq = val.parse::<u64>().map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "AOF manifest at {} has invalid seq line `{}`: {}",
                            manifest_path.display(),
                            line,
                            e,
                        ),
                    )
                })?;
            } else if let Some(val) = line.strip_prefix("shards ") {
                num_shards = Some(val.parse::<u16>().map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "AOF manifest at {} has invalid shards line `{}`: {}",
                            manifest_path.display(),
                            line,
                            e,
                        ),
                    )
                })?);
            } else if let Some(rest) = line.strip_prefix("shard ") {
                // Format: `shard <id> max_lsn <lsn>`
                let mut it = rest.split_whitespace();
                let id_str = it.next().ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "AOF manifest at {} has shard line missing id: `{}`",
                            manifest_path.display(),
                            line,
                        ),
                    )
                })?;
                let id: u16 = id_str.parse().map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "AOF manifest at {} has shard line invalid id `{}`: {}",
                            manifest_path.display(),
                            id_str,
                            e,
                        ),
                    )
                })?;
                // Expect `max_lsn <lsn>`.
                let label = it.next().unwrap_or("");
                let val_str = it.next().unwrap_or("0");
                if label != "max_lsn" {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "AOF manifest at {} shard {} expected `max_lsn`, got `{}`",
                            manifest_path.display(),
                            id,
                            label,
                        ),
                    ));
                }
                let max_lsn: u64 = val_str.parse().map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "AOF manifest at {} shard {} invalid max_lsn `{}`: {}",
                            manifest_path.display(),
                            id,
                            val_str,
                            e,
                        ),
                    )
                })?;
                shards.push(ShardManifest {
                    shard_id: id,
                    max_lsn,
                });
            }
            // Unknown lines are tolerated (forward-compat). Strict parsers can
            // be added at v3 if needed.
        }

        if seq == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "AOF manifest at {} has no valid sequence number",
                    manifest_path.display()
                ),
            ));
        }

        let expected = num_shards.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "AOF manifest at {} is missing required `shards N` line",
                    manifest_path.display()
                ),
            )
        })?;

        if shards.len() != expected as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "AOF manifest at {} declares shards={} but has {} shard records",
                    manifest_path.display(),
                    expected,
                    shards.len(),
                ),
            ));
        }

        // Sort by shard_id and verify contiguous range [0, expected).
        shards.sort_by_key(|s| s.shard_id);
        for (i, s) in shards.iter().enumerate() {
            if s.shard_id as usize != i {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "AOF manifest at {} has non-contiguous shard ids (expected {} at position {}, got {})",
                        manifest_path.display(),
                        i,
                        i,
                        s.shard_id,
                    ),
                ));
            }
        }

        Ok(Self {
            dir: dir.to_path_buf(),
            seq,
            layout: AofLayout::PerShard,
            shards,
        })
    }

    /// Delete any base/incr files in `appendonlydir/` that do not match the
    /// current sequence. Best-effort — logs but does not propagate errors.
    ///
    /// For `PerShard` layout, also recurses into every `shard-N/` subdirectory
    /// and removes stale/tmp files there. Aborted BGREWRITEAOF runs leave
    /// `.rdb.tmp` files in the shard subdirs that otherwise accumulate forever.
    fn cleanup_orphans(&self) {
        match self.layout {
            AofLayout::TopLevel => {
                self.cleanup_orphans_dir(&self.aof_dir(), self.seq);
            }
            AofLayout::PerShard => {
                // Top-level appendonlydir/ holds only the manifest — no data files
                // to clean up there. All data lives in shard-N/ subdirs.
                for shard in &self.shards {
                    self.cleanup_orphans_shard(shard.shard_id);
                }
            }
        }
    }

    /// Scan a single shard's directory for orphan base/incr/tmp files that do
    /// not correspond to the current manifest sequence. Best-effort.
    fn cleanup_orphans_shard(&self, shard_id: u16) {
        self.cleanup_orphans_dir(&self.shard_dir(shard_id), self.seq);
    }

    /// Core orphan sweep: scan `dir` and remove any `moon.aof.*` files whose
    /// sequence is not `keep_seq`. Skips the manifest file itself.
    fn cleanup_orphans_dir(&self, dir: &Path, keep_seq: u64) {
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        let current_base = format!("moon.aof.{}.base.rdb", keep_seq);
        let current_incr = format!("moon.aof.{}.incr.aof", keep_seq);
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = match name.to_str() {
                Some(s) => s,
                None => continue,
            };
            // Keep manifest, current base, current incr. Delete any other moon.aof.*.
            if name_str == MANIFEST_NAME || name_str == current_base || name_str == current_incr {
                continue;
            }
            let is_moon_aof = name_str.starts_with("moon.aof.")
                && (name_str.ends_with(".base.rdb")
                    || name_str.ends_with(".incr.aof")
                    || name_str.ends_with(".rdb.tmp")
                    || name_str.ends_with(".tmp"));
            if !is_moon_aof {
                continue;
            }
            let path = entry.path();
            match std::fs::remove_file(&path) {
                Ok(()) => info!("AOF orphan cleanup: removed {}", path.display()),
                Err(e) => warn!(
                    "AOF orphan cleanup: failed to remove {}: {}",
                    path.display(),
                    e
                ),
            }
        }
    }

    /// Write the manifest file atomically (write tmp + rename).
    ///
    /// Emits v1 format for `TopLevel` and v2 for `PerShard`. The format is
    /// selected by `self.layout`, never by callers — preserving the invariant
    /// that one directory holds one layout.
    pub fn write_manifest(&self) -> std::io::Result<()> {
        let manifest_path = self.manifest_path();
        let tmp_path = manifest_path.with_extension("tmp");

        let content = match self.layout {
            AofLayout::TopLevel => format!(
                "seq {}\nbase moon.aof.{}.base.rdb\nincr moon.aof.{}.incr.aof\n",
                self.seq, self.seq, self.seq
            ),
            AofLayout::PerShard => {
                let mut s = String::with_capacity(64 + self.shards.len() * 40);
                s.push_str("version 2\n");
                s.push_str(&format!("seq {}\n", self.seq));
                s.push_str(&format!("shards {}\n", self.shards.len()));
                for shard in &self.shards {
                    s.push_str(&format!(
                        "shard {} max_lsn {}\n",
                        shard.shard_id, shard.max_lsn
                    ));
                }
                s
            }
        };

        let mut f = std::fs::File::create(&tmp_path)?;
        f.write_all(content.as_bytes())?;
        f.sync_data()?;
        std::fs::rename(&tmp_path, &manifest_path)?;
        fsync_parent_best_effort(&manifest_path);
        Ok(())
    }

    // ------------------------------------------------------------------
    // Per-shard layout helpers
    // ------------------------------------------------------------------

    /// Directory holding a shard's AOF files.
    ///
    /// - `TopLevel`: `appendonlydir/` (the shard_id argument is asserted to be 0).
    /// - `PerShard`: `appendonlydir/shard-{shard_id}/`.
    pub fn shard_dir(&self, shard_id: u16) -> PathBuf {
        match self.layout {
            AofLayout::TopLevel => {
                debug_assert_eq!(shard_id, 0, "TopLevel layout only has shard 0");
                self.aof_dir()
            }
            AofLayout::PerShard => self.aof_dir().join(format!("shard-{}", shard_id)),
        }
    }

    /// Path to a shard's base RDB file for the current sequence.
    pub fn shard_base_path(&self, shard_id: u16) -> PathBuf {
        self.shard_dir(shard_id)
            .join(format!("moon.aof.{}.base.rdb", self.seq))
    }

    /// Path to a shard's incremental RESP file for the current sequence.
    pub fn shard_incr_path(&self, shard_id: u16) -> PathBuf {
        self.shard_dir(shard_id)
            .join(format!("moon.aof.{}.incr.aof", self.seq))
    }

    /// Path to a shard's base RDB file for a given sequence.
    pub fn shard_base_path_seq(&self, shard_id: u16, seq: u64) -> PathBuf {
        self.shard_dir(shard_id)
            .join(format!("moon.aof.{}.base.rdb", seq))
    }

    /// Path to a shard's incremental RESP file for a given sequence.
    pub fn shard_incr_path_seq(&self, shard_id: u16, seq: u64) -> PathBuf {
        self.shard_dir(shard_id)
            .join(format!("moon.aof.{}.incr.aof", seq))
    }

    /// Maximum LSN persisted across all shards.
    ///
    /// Computed (not stored) so the stored value can never drift from
    /// the per-shard records. Returns 0 if `shards` is empty (defensive;
    /// constructors guarantee at least one shard).
    pub fn global_max_lsn(&self) -> u64 {
        self.shards.iter().map(|s| s.max_lsn).max().unwrap_or(0)
    }

    /// Verify that the manifest matches the runtime shard count.
    ///
    /// Returns the verbatim error from RFC § 3 if the shard count differs,
    /// for operator-facing consistency. Callers (typically `main.rs` boot)
    /// should treat this as fatal: continuing with a mismatched shard count
    /// silently drops data from shards that no longer exist or replays a
    /// shard's data into the wrong DashTable.
    pub fn verify_shard_count(&self, expected: u16) -> Result<(), String> {
        let actual = self.shards.len() as u16;
        if actual != expected {
            // Full GitHub URL (not a repo-relative path): installed binaries
            // surface this at startup on machines that don't have the source
            // tree checked out.
            return Err(format!(
                "ERR shard count changed (manifest={}, config={}); refusing to start to avoid data loss. See https://github.com/pilotspace/moon/blob/main/docs/runbooks/shard-count-change.md",
                actual, expected
            ));
        }
        Ok(())
    }

    /// Returns true if the on-disk layout under `appendonlydir/` matches the
    /// legacy TopLevel format (files at top level, no `shard-N/` subdirs).
    ///
    /// Used by callers to detect when a v1 single-shard deployment is being
    /// upgraded to v2 multi-shard and needs explicit migration. Does NOT
    /// migrate — separate from `migrate_top_level_to_per_shard` so the side
    /// effect is opt-in, not hidden in a load path.
    pub fn is_legacy_top_level_layout(dir: &Path) -> bool {
        let aof_dir = dir.join(AOF_DIR_NAME);
        if !aof_dir.exists() {
            return false;
        }

        // Check manifest version first. If a valid v2 (PerShard) manifest exists,
        // return false regardless of stray top-level files. Operators occasionally
        // leave old base.rdb / incr.aof files at the top level during debugging
        // or failed upgrades; scanning filenames without reading the manifest would
        // produce a misleading "legacy detected" result and trigger unwanted
        // migration on an already-upgraded deployment.
        if let Ok(Some(m)) = Self::load(dir) {
            if m.layout == AofLayout::PerShard {
                return false;
            }
        }

        let entries = match std::fs::read_dir(&aof_dir) {
            Ok(e) => e,
            Err(_) => return false,
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name_str) = name.to_str() else {
                continue;
            };
            if name_str.starts_with("moon.aof.")
                && (name_str.ends_with(".base.rdb") || name_str.ends_with(".incr.aof"))
            {
                return true;
            }
        }
        false
    }

    /// Migrate a single-shard TopLevel layout in place to a single-shard
    /// PerShard layout.
    ///
    /// Moves `appendonlydir/moon.aof.{seq}.{base.rdb,incr.aof}` into
    /// `appendonlydir/shard-0/`, then rewrites the manifest as v2 with
    /// `shards 1`. Idempotent: a second call on an already-PerShard manifest
    /// returns Ok with no filesystem changes.
    ///
    /// This is the RFC § 5 case 1 migration — zero data movement (rename only),
    /// safe to run on first boot after upgrading from v0.1.x. Multi-shard
    /// migrations from legacy AOF (case 2) use the `moon migrate-aof`
    /// subcommand and are NOT handled here.
    pub fn migrate_top_level_to_per_shard(&mut self) -> std::io::Result<()> {
        if self.layout == AofLayout::PerShard {
            return Ok(());
        }
        if self.shards.len() != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "migrate_top_level_to_per_shard called with {} shards; \
                     only single-shard TopLevel can be migrated in place",
                    self.shards.len()
                ),
            ));
        }

        // Compute paths up front. shard_dir/shard_*_path_seq for a single-
        // shard target are pure path computations and do NOT depend on
        // self.layout, so it is safe to derive them while layout is still
        // TopLevel.
        let old_base = self
            .aof_dir()
            .join(format!("moon.aof.{}.base.rdb", self.seq));
        let old_incr = self
            .aof_dir()
            .join(format!("moon.aof.{}.incr.aof", self.seq));
        let new_dir = self.aof_dir().join("shard-0");
        let new_base = new_dir.join(format!("moon.aof.{}.base.rdb", self.seq));
        let new_incr = new_dir.join(format!("moon.aof.{}.incr.aof", self.seq));

        if !old_base.exists() {
            // Pre-flight check: nothing moved yet, no rollback needed.
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "TopLevel→PerShard migration: source base {} not found",
                    old_base.display()
                ),
            ));
        }
        std::fs::create_dir_all(&new_dir)?;

        // Move base. If the rename itself fails, no on-disk mutation has
        // happened yet — bail without rollback. Layout stays TopLevel until
        // commit at the bottom.
        std::fs::rename(&old_base, &new_base)?;

        // Fsync the target directory so the base rename is durable before we
        // proceed. A crash after rename but before dir-fsync could leave the
        // old filename visible on the next boot.
        //
        // NOTE: if this fsync fails, old_base has already moved to new_base —
        // rollback the rename before returning so the manifest stays consistent.
        if let Err(e) = fsync_directory(&new_dir) {
            if let Err(re) = std::fs::rename(&new_base, &old_base) {
                error!(
                    "Migration rollback: failed to restore base {} → {} after fsync_directory failure: {}",
                    new_base.display(),
                    old_base.display(),
                    re
                );
            }
            return Err(e);
        }

        // Base is now durably in shard-0/. Any subsequent error must restore it.
        let moved_incr: bool;
        let created_incr: bool;
        if old_incr.exists() {
            if let Err(e) = std::fs::rename(&old_incr, &new_incr) {
                if let Err(re) = std::fs::rename(&new_base, &old_base) {
                    error!(
                        "Migration rollback: failed to restore base {} → {}: {}",
                        new_base.display(),
                        old_base.display(),
                        re
                    );
                }
                return Err(e);
            }
            // Fsync the shard directory to make the incr rename durable.
            // If this fails, roll back both incr and base renames.
            if let Err(e) = fsync_directory(&new_dir) {
                if let Err(re) = std::fs::rename(&new_incr, &old_incr) {
                    error!(
                        "Migration rollback: failed to restore incr {} → {} after fsync_directory failure: {}",
                        new_incr.display(),
                        old_incr.display(),
                        re
                    );
                }
                if let Err(re) = std::fs::rename(&new_base, &old_base) {
                    error!(
                        "Migration rollback: failed to restore base {} → {} after fsync_directory failure: {}",
                        new_base.display(),
                        old_base.display(),
                        re
                    );
                }
                return Err(e);
            }
            moved_incr = true;
            created_incr = false;
        } else {
            match std::fs::File::create(&new_incr) {
                Ok(_) => {
                    moved_incr = false;
                    created_incr = true;
                }
                Err(e) => {
                    if let Err(re) = std::fs::rename(&new_base, &old_base) {
                        error!(
                            "Migration rollback: failed to restore base {} → {}: {}",
                            new_base.display(),
                            old_base.display(),
                            re
                        );
                    }
                    return Err(e);
                }
            }
        }

        // Commit: flip layout, persist as v2. If write_manifest fails, undo
        // every filesystem mutation and restore layout so the next boot still
        // sees a valid v1 TopLevel deployment.
        self.layout = AofLayout::PerShard;
        if let Err(e) = self.write_manifest() {
            self.layout = AofLayout::TopLevel;
            if moved_incr {
                if let Err(re) = std::fs::rename(&new_incr, &old_incr) {
                    error!(
                        "Migration rollback: failed to restore incr {} → {}: {}",
                        new_incr.display(),
                        old_incr.display(),
                        re
                    );
                }
            } else if created_incr {
                if let Err(re) = std::fs::remove_file(&new_incr) {
                    warn!(
                        "Migration rollback: failed to remove freshly created incr {}: {}",
                        new_incr.display(),
                        re
                    );
                }
            }
            if let Err(re) = std::fs::rename(&new_base, &old_base) {
                error!(
                    "Migration rollback: failed to restore base {} → {}: {}. \
                     Manifest dir {} may be in an inconsistent state.",
                    new_base.display(),
                    old_base.display(),
                    re,
                    self.dir.display()
                );
            }
            return Err(e);
        }

        info!(
            "AOF migrated: TopLevel → PerShard (single shard) at {}",
            self.aof_dir().display()
        );
        Ok(())
    }

    /// Advance to the next sequence: write new base RDB, create new incr file,
    /// update manifest, delete old files.
    ///
    /// Returns the path to the new incremental file (caller should switch writing to it).
    pub fn advance(&mut self, rdb_bytes: &[u8]) -> Result<PathBuf, crate::error::MoonError> {
        let old_seq = self.seq;
        let new_seq = old_seq + 1;

        let aof_dir = self.aof_dir();
        std::fs::create_dir_all(&aof_dir).map_err(|e| crate::error::AofError::Io {
            path: aof_dir.clone(),
            source: e,
        })?;

        // 1. Write new base RDB (atomic: tmp + fsync + rename).
        //    Must fsync the data BEFORE renaming — a rename without prior fsync
        //    can publish a file whose contents aren't durable, so a crash leaves
        //    the manifest pointing at an empty/partial base RDB.
        let new_base = self.base_path_seq(new_seq);
        let tmp_base = new_base.with_extension("rdb.tmp");
        {
            let mut f =
                std::fs::File::create(&tmp_base).map_err(|e| crate::error::AofError::Io {
                    path: tmp_base.clone(),
                    source: e,
                })?;
            f.write_all(rdb_bytes)
                .map_err(|e| crate::error::AofError::Io {
                    path: tmp_base.clone(),
                    source: e,
                })?;
            f.sync_data().map_err(|e| crate::error::AofError::Io {
                path: tmp_base.clone(),
                source: e,
            })?;
        }
        std::fs::rename(&tmp_base, &new_base).map_err(|e| {
            crate::error::AofError::RewriteFailed {
                detail: format!("rename base: {}", e),
            }
        })?;
        fsync_parent_best_effort(&new_base);

        // 2. Create empty new incremental file
        let new_incr = self.incr_path_seq(new_seq);
        std::fs::File::create(&new_incr).map_err(|e| crate::error::AofError::Io {
            path: new_incr.clone(),
            source: e,
        })?;

        // 3. Update manifest (atomic)
        self.seq = new_seq;
        self.write_manifest()
            .map_err(|e| crate::error::AofError::Io {
                path: self.manifest_path(),
                source: e,
            })?;

        // 4. Delete old files (best-effort)
        let old_base = self.base_path_seq(old_seq);
        let old_incr = self.incr_path_seq(old_seq);
        if old_base.exists() {
            if let Err(e) = std::fs::remove_file(&old_base) {
                warn!("Failed to delete old base {}: {}", old_base.display(), e);
            }
        }
        if old_incr.exists() {
            if let Err(e) = std::fs::remove_file(&old_incr) {
                warn!("Failed to delete old incr {}: {}", old_incr.display(), e);
            }
        }

        info!(
            "AOF advanced to seq {}: base={} bytes, incr={}",
            new_seq,
            rdb_bytes.len(),
            new_incr.display()
        );

        Ok(new_incr)
    }
}

#[cfg(test)]
mod tests_v2 {
    //! Unit tests for the v2 (PerShard) manifest format.
    //!
    //! Covers the Step 1 deliverable of the per-shard AOF RFC:
    //! - v1 manifests continue to load as TopLevel (single-shard, shard_id=0)
    //! - v2 round-trip: write → load → equivalent struct shape
    //! - shard count mismatch produces the verbatim RFC § 3 error
    //! - migrate_top_level_to_per_shard performs in-place rename and rewrites
    //!   the manifest as v2
    //! - global_max_lsn computes max across shards
    //! - is_legacy_top_level_layout detects top-level files

    use super::*;
    use std::fs;

    fn temp_dir() -> PathBuf {
        // Use a global atomic counter so parallel test threads (cargo test runs
        // unit tests in parallel) never produce the same directory name even
        // when PID and nanosecond clock resolution are the same for two threads.
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let d = std::env::temp_dir().join(format!(
            "moon-aof-manifest-test-{}-{}",
            std::process::id(),
            n,
        ));
        fs::create_dir_all(&d).expect("temp dir create");
        d
    }

    #[test]
    fn v1_manifest_loads_as_top_level_single_shard() {
        let dir = temp_dir();
        let m = AofManifest::initialize(&dir).expect("initialize v1");

        assert_eq!(m.layout, AofLayout::TopLevel);
        assert_eq!(m.shards.len(), 1);
        assert_eq!(m.shards[0].shard_id, 0);
        assert_eq!(m.shards[0].max_lsn, 0);

        // Reload from disk
        let reloaded = AofManifest::load(&dir).expect("load").expect("present");
        assert_eq!(reloaded.layout, AofLayout::TopLevel);
        assert_eq!(reloaded.shards.len(), 1);
        assert_eq!(reloaded.seq, m.seq);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn v2_manifest_round_trips() {
        let dir = temp_dir();
        let m = AofManifest::initialize_multi(&dir, 4).expect("initialize_multi");

        assert_eq!(m.layout, AofLayout::PerShard);
        assert_eq!(m.shards.len(), 4);
        for (i, s) in m.shards.iter().enumerate() {
            assert_eq!(s.shard_id, i as u16);
            assert_eq!(s.max_lsn, 0);
        }

        // Per-shard subdirs were created with empty base + incr.
        for i in 0..4u16 {
            assert!(m.shard_dir(i).exists(), "shard-{} dir exists", i);
            assert!(m.shard_base_path(i).exists(), "shard-{} base exists", i);
            assert!(m.shard_incr_path(i).exists(), "shard-{} incr exists", i);
        }

        let reloaded = AofManifest::load(&dir).expect("load").expect("present");
        assert_eq!(reloaded.layout, AofLayout::PerShard);
        assert_eq!(reloaded.shards.len(), 4);
        assert_eq!(reloaded.seq, m.seq);
        for (i, s) in reloaded.shards.iter().enumerate() {
            assert_eq!(s.shard_id, i as u16);
        }

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn verify_shard_count_emits_rfc_error_verbatim() {
        let m = AofManifest {
            dir: PathBuf::from("/tmp/nowhere"),
            seq: 1,
            layout: AofLayout::PerShard,
            shards: vec![
                ShardManifest {
                    shard_id: 0,
                    max_lsn: 0,
                },
                ShardManifest {
                    shard_id: 1,
                    max_lsn: 0,
                },
            ],
        };
        let err = m.verify_shard_count(4).expect_err("should mismatch");
        assert_eq!(
            err,
            "ERR shard count changed (manifest=2, config=4); refusing to start to avoid data loss. See https://github.com/pilotspace/moon/blob/main/docs/runbooks/shard-count-change.md"
        );

        // Matching count succeeds.
        m.verify_shard_count(2).expect("match");
    }

    #[test]
    fn migrate_top_level_to_per_shard_moves_files_and_rewrites_manifest() {
        let dir = temp_dir();
        let mut m = AofManifest::initialize(&dir).expect("initialize v1");

        // Write a marker into the incr file so we can prove the contents
        // survive the rename.
        let original_incr = m.aof_dir().join(format!("moon.aof.{}.incr.aof", m.seq));
        fs::write(&original_incr, b"MARKER").expect("write incr marker");

        m.migrate_top_level_to_per_shard().expect("migrate");

        assert_eq!(m.layout, AofLayout::PerShard);
        assert!(!original_incr.exists(), "old incr removed by rename");
        let new_incr = m.shard_incr_path(0);
        assert!(new_incr.exists(), "new shard-0 incr exists");
        let contents = fs::read(&new_incr).expect("read new incr");
        assert_eq!(contents, b"MARKER", "incr contents preserved");

        // Reloaded manifest is v2.
        let reloaded = AofManifest::load(&dir).expect("load").expect("present");
        assert_eq!(reloaded.layout, AofLayout::PerShard);
        assert_eq!(reloaded.shards.len(), 1);

        // Idempotency: second call is a no-op.
        m.migrate_top_level_to_per_shard().expect("idempotent");
        assert_eq!(m.layout, AofLayout::PerShard);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn global_max_lsn_returns_max_across_shards() {
        let m = AofManifest {
            dir: PathBuf::from("/tmp/nowhere"),
            seq: 1,
            layout: AofLayout::PerShard,
            shards: vec![
                ShardManifest {
                    shard_id: 0,
                    max_lsn: 100,
                },
                ShardManifest {
                    shard_id: 1,
                    max_lsn: 500,
                },
                ShardManifest {
                    shard_id: 2,
                    max_lsn: 250,
                },
            ],
        };
        assert_eq!(m.global_max_lsn(), 500);
    }

    #[test]
    fn is_legacy_top_level_layout_detects_v1_files() {
        let dir = temp_dir();
        // No appendonlydir yet → false.
        assert!(!AofManifest::is_legacy_top_level_layout(&dir));

        // After v1 initialize, top-level files present → true.
        let _m = AofManifest::initialize(&dir).expect("init v1");
        assert!(AofManifest::is_legacy_top_level_layout(&dir));

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn is_legacy_top_level_layout_returns_false_for_v2() {
        let dir = temp_dir();
        let _m = AofManifest::initialize_multi(&dir, 2).expect("init v2");
        assert!(
            !AofManifest::is_legacy_top_level_layout(&dir),
            "v2 layout has no top-level moon.aof.* files"
        );

        fs::remove_dir_all(&dir).ok();
    }

    /// FIX-W3-4: v2 manifest with stray top-level .base.rdb must return false,
    /// not true. The filename scan is misleading when a valid v2 manifest exists.
    ///
    /// Scenario: operator upgraded to v2 but left a stale `moon.aof.1.base.rdb`
    /// at the top level (e.g., copied during debugging). `is_legacy_top_level_layout`
    /// must check the manifest first and return false when v2 is confirmed.
    #[test]
    fn is_legacy_top_level_layout_ignores_stray_files_when_v2_manifest_present() {
        let dir = temp_dir();
        // Initialize a genuine v2 (PerShard) layout.
        let _m = AofManifest::initialize_multi(&dir, 2).expect("init v2");

        // Plant a stale top-level base.rdb to simulate the stray-file scenario.
        let stray = dir.join(AOF_DIR_NAME).join("moon.aof.1.base.rdb");
        fs::write(&stray, b"REDIS0011\xff").expect("write stray base.rdb");

        // Even though the stray file matches the filename pattern, a valid v2
        // manifest is present, so is_legacy_top_level_layout must return false.
        assert!(
            !AofManifest::is_legacy_top_level_layout(&dir),
            "v2 manifest + stray top-level file must still return false"
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn parse_v2_rejects_shard_count_mismatch_in_file() {
        let dir = temp_dir();
        let aof = dir.join(AOF_DIR_NAME);
        fs::create_dir_all(&aof).unwrap();
        // Manifest claims shards 3 but only declares two shard records.
        fs::write(
            aof.join(MANIFEST_NAME),
            "version 2\nseq 1\nshards 3\nshard 0 max_lsn 0\nshard 1 max_lsn 0\n",
        )
        .unwrap();

        let err = AofManifest::load(&dir).expect_err("should reject");
        let msg = err.to_string();
        assert!(
            msg.contains("declares shards=3 but has 2 shard records"),
            "got: {}",
            msg
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn parse_v2_rejects_non_contiguous_shard_ids() {
        let dir = temp_dir();
        let aof = dir.join(AOF_DIR_NAME);
        fs::create_dir_all(&aof).unwrap();
        // shards=2 but ids are {0, 2} not {0, 1}.
        fs::write(
            aof.join(MANIFEST_NAME),
            "version 2\nseq 1\nshards 2\nshard 0 max_lsn 0\nshard 2 max_lsn 0\n",
        )
        .unwrap();

        let err = AofManifest::load(&dir).expect_err("should reject");
        let msg = err.to_string();
        assert!(msg.contains("non-contiguous shard ids"), "got: {}", msg);

        fs::remove_dir_all(&dir).ok();
    }

    // ------------------------------------------------------------------
    // Reviewer-flagged fixes: layout-aware path helpers + migration
    // rollback. See the "Verify findings against current code" review
    // comment on aof_manifest.rs:669-775 and :688-717.
    // ------------------------------------------------------------------

    #[test]
    fn base_incr_paths_route_to_shard_zero_after_migration() {
        let dir = temp_dir();
        let mut m = AofManifest::initialize(&dir).expect("init v1");
        // Pre-migration: TopLevel paths under appendonlydir/ directly.
        assert_eq!(m.base_path(), m.aof_dir().join("moon.aof.1.base.rdb"));
        assert_eq!(m.incr_path(), m.aof_dir().join("moon.aof.1.incr.aof"));

        m.migrate_top_level_to_per_shard().expect("migrate");

        // Post-migration: single-file helpers must route to shard-0/ so
        // replay_multi_part and advance() find the actual files. This is
        // the bug the reviewer flagged for aof_manifest.rs:669-775.
        let shard0 = m.aof_dir().join("shard-0");
        assert_eq!(m.base_path(), shard0.join("moon.aof.1.base.rdb"));
        assert_eq!(m.incr_path(), shard0.join("moon.aof.1.incr.aof"));
        assert_eq!(m.base_path_seq(7), shard0.join("moon.aof.7.base.rdb"));
        assert_eq!(m.incr_path_seq(7), shard0.join("moon.aof.7.incr.aof"));
        // The path the helper returns must be where the file actually lives.
        assert!(m.base_path().exists(), "base file at returned path");
        assert!(m.incr_path().exists(), "incr file at returned path");

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn migrate_rolls_back_filesystem_when_incr_rename_fails() {
        // Simulate the rename(old_incr → new_incr) failure path by making
        // the destination already exist as a directory (rename onto a
        // non-empty directory is an error on every supported OS).
        let dir = temp_dir();
        let mut m = AofManifest::initialize(&dir).expect("init v1");
        let original_base = m.aof_dir().join("moon.aof.1.base.rdb");
        let original_incr = m.aof_dir().join("moon.aof.1.incr.aof");
        fs::write(&original_incr, b"INCR_MARKER").expect("seed incr");
        let base_bytes_before = fs::read(&original_base).expect("read base");

        // Pre-create shard-0/moon.aof.1.incr.aof as a DIRECTORY so the
        // rename fails after the base rename has already succeeded.
        let shard0 = m.aof_dir().join("shard-0");
        fs::create_dir_all(shard0.join("moon.aof.1.incr.aof")).expect("seed blocker");

        let err = m
            .migrate_top_level_to_per_shard()
            .expect_err("incr rename should fail");
        let _ = err; // exact error kind depends on OS

        // Rollback invariants:
        //   1. Layout stays TopLevel in memory.
        //   2. base file restored to its original TopLevel path.
        //   3. base file contents unchanged.
        //   4. on-disk manifest is still v1 (load returns layout TopLevel).
        assert_eq!(m.layout, AofLayout::TopLevel, "in-memory layout reverted");
        assert!(original_base.exists(), "base restored to top-level");
        let base_bytes_after = fs::read(&original_base).expect("read base");
        assert_eq!(base_bytes_after, base_bytes_before, "base contents intact");
        let reloaded = AofManifest::load(&dir).expect("load").expect("present");
        assert_eq!(reloaded.layout, AofLayout::TopLevel, "on-disk manifest v1");

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn migrate_does_not_mutate_on_missing_base() {
        let dir = temp_dir();
        let mut m = AofManifest::initialize(&dir).expect("init v1");
        let base = m.aof_dir().join("moon.aof.1.base.rdb");
        fs::remove_file(&base).expect("remove base");

        let err = m
            .migrate_top_level_to_per_shard()
            .expect_err("missing base should fail");
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
        // Layout never flipped, no rollback needed.
        assert_eq!(m.layout, AofLayout::TopLevel);

        fs::remove_dir_all(&dir).ok();
    }
}
