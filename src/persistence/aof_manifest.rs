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

const MANIFEST_NAME: &str = "moon.aof.manifest";
const AOF_DIR_NAME: &str = "appendonlydir";

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
    fn cleanup_orphans(&self) {
        let aof_dir = self.aof_dir();
        let entries = match std::fs::read_dir(&aof_dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        let current_base = format!("moon.aof.{}.base.rdb", self.seq);
        let current_incr = format!("moon.aof.{}.incr.aof", self.seq);
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
            return Err(format!(
                "ERR shard count changed (manifest={}, config={}); refusing to start to avoid data loss. See docs/runbooks/shard-count-change.md",
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
        let old_base = self.aof_dir().join(format!("moon.aof.{}.base.rdb", self.seq));
        let old_incr = self.aof_dir().join(format!("moon.aof.{}.incr.aof", self.seq));
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

        // Move base. If this fails, no on-disk mutation happened yet — bail
        // without rollback. Layout stays TopLevel until commit at the bottom.
        std::fs::rename(&old_base, &new_base)?;

        // Base is now in shard-0/. Any subsequent error must restore it.
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

    /// Create the `appendonlydir/` and write an initial v2 manifest for the
    /// given shard count.
    ///
    /// Each shard gets its own `shard-{N}/` subdirectory with an empty base
    /// RDB and an empty incr file. Mirrors `initialize()` semantics: the
    /// `(base + incr)` invariant holds from the first boot, so recovery can
    /// replay incr-only state without complaint.
    pub fn initialize_multi(dir: &Path, num_shards: u16) -> std::io::Result<Self> {
        if num_shards == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "initialize_multi requires num_shards >= 1",
            ));
        }
        let manifest = Self {
            dir: dir.to_path_buf(),
            seq: 1,
            layout: AofLayout::PerShard,
            shards: (0..num_shards)
                .map(|id| ShardManifest {
                    shard_id: id,
                    max_lsn: 0,
                })
                .collect(),
        };
        std::fs::create_dir_all(manifest.aof_dir())?;

        // Per-shard empty RDB. Single Database::default() inside a 1-element
        // slice matches `initialize()`'s empty-RDB shape for each shard.
        let empty_dbs: [crate::storage::Database; 0] = [];
        let empty_rdb = crate::persistence::rdb::save_to_bytes(&empty_dbs)
            .map_err(|e| std::io::Error::other(format!("empty RDB serialize: {e}")))?;

        for shard_id in 0..num_shards {
            let shard_dir = manifest.shard_dir(shard_id);
            std::fs::create_dir_all(&shard_dir)?;

            let base_path = manifest.shard_base_path(shard_id);
            let tmp_path = base_path.with_extension("rdb.tmp");
            {
                let mut f = std::fs::File::create(&tmp_path)?;
                f.write_all(&empty_rdb)?;
                f.sync_data()?;
            }
            std::fs::rename(&tmp_path, &base_path)?;
            std::fs::File::create(manifest.shard_incr_path(shard_id))?;
        }

        manifest.write_manifest()?;
        Ok(manifest)
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

/// Replay multi-part AOF: load base RDB then replay incremental RESP.
///
/// Returns total keys/commands loaded.
pub fn replay_multi_part(
    databases: &mut [crate::storage::Database],
    manifest: &AofManifest,
    engine: &dyn crate::persistence::replay::CommandReplayEngine,
) -> Result<usize, crate::error::MoonError> {
    let mut total = 0usize;

    // Load base RDB
    let base_path = manifest.base_path();
    if base_path.exists() {
        match crate::persistence::rdb::load(databases, &base_path) {
            Ok(n) => {
                info!(
                    "AOF base RDB loaded: {} keys from {}",
                    n,
                    base_path.display()
                );
                total += n;
            }
            Err(e) => {
                // Base RDB is corrupt or unreadable — applying incremental
                // deltas on top of missing/corrupt base gives wrong results.
                error!("AOF base RDB load failed: {}", e);
                return Err(e);
            }
        }
    } else {
        // Missing base is tolerable only when the incr log is also empty
        // (fresh manifest from initialize(), or first boot after legacy
        // upgrade). If there's incremental content but no base, replaying
        // deltas (DEL, EXPIRE, HINCRBY, …) on an empty database produces
        // incorrect state — fail loudly rather than silently corrupt.
        let incr_path = manifest.incr_path();
        let incr_len = std::fs::metadata(&incr_path).map(|m| m.len()).unwrap_or(0);
        if incr_len > 0 {
            return Err(crate::error::MoonError::from(
                crate::error::AofError::RewriteFailed {
                    detail: format!(
                        "AOF base RDB missing at {} but incr {} is {} bytes; refusing to replay incr against empty state",
                        base_path.display(),
                        incr_path.display(),
                        incr_len,
                    ),
                },
            ));
        }
        warn!(
            "AOF base RDB not found: {} (incr empty, treating as fresh init)",
            base_path.display()
        );
    }

    // Replay incremental RESP
    let incr_path = manifest.incr_path();
    if incr_path.exists() {
        let data = std::fs::read(&incr_path)?;
        if !data.is_empty() {
            // Pure RESP — use replay_aof_resp (no RDB preamble detection needed)
            let count = replay_incr_resp(databases, &data, engine)?;
            info!(
                "AOF incr replayed: {} commands from {}",
                count,
                incr_path.display()
            );
            total += count;
        }
    }

    Ok(total)
}

/// Replay pure RESP commands from a byte slice.
///
/// **Corruption handling:** On mid-stream parse errors this returns an error
/// rather than silently resyncing to the next `*` byte. Silent resync in a
/// multi-part AOF is dangerous: an undetected run of dropped commands leaves
/// the database in an inconsistent state that cannot be reconstructed.
/// Truncated tails (parser returns `Ok(None)` with bytes remaining) are
/// logged and treated as the legitimate end of the incremental log, matching
/// `replay_aof` semantics for crash-time tail truncation.
fn replay_incr_resp(
    databases: &mut [crate::storage::Database],
    data: &[u8],
    engine: &dyn crate::persistence::replay::CommandReplayEngine,
) -> Result<usize, crate::error::MoonError> {
    use crate::protocol::{Frame, ParseConfig, parse};
    use bytes::BytesMut;

    let total_len = data.len();
    let mut buf = BytesMut::from(data);
    let config = ParseConfig::default();
    let mut selected_db: usize = 0;
    let mut count: usize = 0;

    loop {
        if buf.is_empty() {
            break;
        }
        match parse::parse(&mut buf, &config) {
            Ok(Some(frame)) => {
                let (cmd, cmd_args) = match &frame {
                    Frame::Array(arr) if !arr.is_empty() => {
                        let name = match &arr[0] {
                            Frame::BulkString(s) => s.as_ref(),
                            Frame::SimpleString(s) => s.as_ref(),
                            other => {
                                return Err(crate::error::MoonError::from(
                                    crate::error::AofError::RewriteFailed {
                                        detail: format!(
                                            "AOF incr command at offset {} has non-string name frame: {:?}",
                                            total_len - buf.len(),
                                            std::mem::discriminant(other)
                                        ),
                                    },
                                ));
                            }
                        };
                        (name as &[u8], &arr[1..])
                    }
                    other => {
                        return Err(crate::error::MoonError::from(
                            crate::error::AofError::RewriteFailed {
                                detail: format!(
                                    "AOF incr non-array frame at offset {}: {:?}",
                                    total_len - buf.len(),
                                    std::mem::discriminant(other)
                                ),
                            },
                        ));
                    }
                };
                engine.replay_command(databases, cmd, cmd_args, &mut selected_db);
                count += 1;
            }
            Ok(None) => {
                if !buf.is_empty() {
                    let offset = total_len - buf.len();
                    warn!(
                        "AOF incr truncated tail: {} bytes at offset {} (treating as crash-time EOF)",
                        buf.len(),
                        offset
                    );
                }
                break;
            }
            Err(e) => {
                let offset = total_len - buf.len();
                return Err(crate::error::MoonError::from(
                    crate::error::AofError::RewriteFailed {
                        detail: format!("AOF incr parse error at offset {}: {:?}", offset, e),
                    },
                ));
            }
        }
    }

    Ok(count)
}

/// An entry that was tagged `OrderedAcrossShards` (RFC § 2 Rule 2) and
/// must be merge-replayed in global LSN order after per-shard replay
/// completes. The `shard_id` records which shard's file it came from so
/// the merge step can dispatch each entry back to its origin shard's
/// databases.
#[derive(Debug, Clone)]
pub struct OrderedEntry {
    pub shard_id: u16,
    pub lsn: u64,
    pub bytes: bytes::Bytes,
}

/// Replay a framed PerShard incr file: `[u64 lsn LE][u32 len LE][RESP bytes]`.
///
/// Step 3 wrote this format; step 4 reads it. Step 5 extends the LSN field:
/// the high bit (`crate::persistence::aof::ORDERED_LSN_FLAG`) marks the
/// entry as `OrderedAcrossShards` — those entries are NOT replayed inline,
/// instead they are pushed into `ordered_buf` for the caller to merge-replay
/// in global LSN order across all shards.
///
/// Returns `(commands_replayed, max_lsn)` — the count covers only inline
/// (non-ordered) replays, and `max_lsn` covers both inline AND ordered
/// entries (the high bit is masked out before max comparison, so it reflects
/// the true issued LSN).
///
/// **Truncated entries:** a header partly written at crash time is treated as
/// EOF (parity with `replay_incr_resp` semantics). A whole header followed by
/// a truncated payload is also EOF — the writer's invariant is that the
/// header is written first then the payload, and on partial write the most we
/// can lose is the last entry's payload tail.
///
/// **Corruption:** a mid-stream RESP parse error inside an otherwise-complete
/// payload is fatal (same reasoning as `replay_incr_resp`).
fn replay_incr_framed(
    shard_id: u16,
    databases: &mut [crate::storage::Database],
    data: &[u8],
    engine: &dyn crate::persistence::replay::CommandReplayEngine,
    ordered_buf: &mut Vec<OrderedEntry>,
) -> Result<(usize, u64), crate::error::MoonError> {
    use crate::protocol::{Frame, ParseConfig, parse};
    use bytes::BytesMut;

    const HEADER_LEN: usize = 12; // u64 lsn LE + u32 len LE

    let total_len = data.len();
    let mut offset: usize = 0;
    let config = ParseConfig::default();
    let mut selected_db: usize = 0;
    let mut count: usize = 0;
    let mut max_lsn: u64 = 0;

    while offset < total_len {
        if total_len - offset < HEADER_LEN {
            warn!(
                "AOF incr framed truncated header: {} bytes at offset {} (treating as crash-time EOF)",
                total_len - offset,
                offset
            );
            break;
        }
        let raw_lsn =
            u64::from_le_bytes(data[offset..offset + 8].try_into().expect("8 bytes"));
        let len = u32::from_le_bytes(data[offset + 8..offset + 12].try_into().expect("4 bytes"))
            as usize;
        let payload_start = offset + HEADER_LEN;
        let payload_end = payload_start.saturating_add(len);
        if payload_end > total_len {
            warn!(
                "AOF incr framed truncated payload at offset {} (lsn {:#x}, declared len {}, have {} bytes); treating as crash-time EOF",
                offset,
                raw_lsn,
                len,
                total_len - payload_start
            );
            break;
        }

        // Strip the OrderedAcrossShards flag to recover the true LSN.
        let is_ordered = raw_lsn & crate::persistence::aof::ORDERED_LSN_FLAG != 0;
        let lsn = raw_lsn & !crate::persistence::aof::ORDERED_LSN_FLAG;

        // Ordered entries: buffer for cross-shard merge replay; do NOT
        // dispatch inline.
        if is_ordered {
            let bytes = bytes::Bytes::copy_from_slice(&data[payload_start..payload_end]);
            ordered_buf.push(OrderedEntry {
                shard_id,
                lsn,
                bytes,
            });
            if lsn > max_lsn {
                max_lsn = lsn;
            }
            offset = payload_end;
            continue;
        }

        // Parse RESP from the payload slice. A standalone slice ensures one
        // header maps to exactly one command — no implicit pipelining across
        // headers.
        let mut buf = BytesMut::from(&data[payload_start..payload_end]);
        match parse::parse(&mut buf, &config) {
            Ok(Some(frame)) => {
                let (cmd, cmd_args) = match &frame {
                    Frame::Array(arr) if !arr.is_empty() => {
                        let name = match &arr[0] {
                            Frame::BulkString(s) => s.as_ref(),
                            Frame::SimpleString(s) => s.as_ref(),
                            other => {
                                return Err(crate::error::MoonError::from(
                                    crate::error::AofError::RewriteFailed {
                                        detail: format!(
                                            "AOF incr framed command at offset {} (lsn {}) has non-string name frame: {:?}",
                                            offset,
                                            lsn,
                                            std::mem::discriminant(other)
                                        ),
                                    },
                                ));
                            }
                        };
                        (name as &[u8], &arr[1..])
                    }
                    other => {
                        return Err(crate::error::MoonError::from(
                            crate::error::AofError::RewriteFailed {
                                detail: format!(
                                    "AOF incr framed non-array frame at offset {} (lsn {}): {:?}",
                                    offset,
                                    lsn,
                                    std::mem::discriminant(other)
                                ),
                            },
                        ));
                    }
                };
                engine.replay_command(databases, cmd, cmd_args, &mut selected_db);
                count += 1;
                if lsn > max_lsn {
                    max_lsn = lsn;
                }
            }
            Ok(None) => {
                // Header said `len` bytes of RESP, but parser can't make a
                // frame from those bytes. That's corruption inside a fully
                // declared payload, not a truncated tail — escalate.
                return Err(crate::error::MoonError::from(
                    crate::error::AofError::RewriteFailed {
                        detail: format!(
                            "AOF incr framed payload at offset {} (lsn {}, len {}) parsed as incomplete frame; corrupt entry",
                            offset, lsn, len
                        ),
                    },
                ));
            }
            Err(e) => {
                return Err(crate::error::MoonError::from(
                    crate::error::AofError::RewriteFailed {
                        detail: format!(
                            "AOF incr framed parse error at offset {} (lsn {}, len {}): {:?}",
                            offset, lsn, len, e
                        ),
                    },
                ));
            }
        }

        offset = payload_end;
    }

    Ok((count, max_lsn))
}

/// Replay a PerShard multi-part AOF into N parallel `Vec<Database>` buffers.
///
/// `per_shard_databases[i]` is shard `i`'s database vector. The manifest's
/// `shards` length MUST equal `per_shard_databases.len()`; the caller is
/// expected to have run [`AofManifest::verify_shard_count`] at boot.
///
/// Per-shard work is independent (different shards never touch the same
/// DashTable), so this is parallelizable in principle. Step 4 keeps the
/// initial implementation sequential — it's correct, simple, and the cold
/// recovery path is not throughput-critical. Parallelizing across shards is
/// a future optimization (RFC § 1 recovery-parallelism claim) once the
/// crash-matrix tests soak the sequential path.
///
/// Returns `(total_commands_replayed, global_max_lsn, ordered_entries)`:
///   - `total_commands_replayed` covers all inline (non-ordered) entries
///     plus the base-RDB key count.
///   - `global_max_lsn` is `max(per-shard max LSN)` across both inline and
///     ordered entries; the caller is expected to call
///     `ReplicationState::seed_master_offset(global_max_lsn)` before
///     accepting client traffic (RFC § 2 Rule 3).
///   - `ordered_entries` is the set of `OrderedAcrossShards`-tagged entries
///     across ALL shards; the caller passes them to
///     [`replay_ordered_merge`] for the cross-shard merge replay.
pub fn replay_per_shard(
    per_shard_databases: &mut [&mut [crate::storage::Database]],
    manifest: &AofManifest,
    engine: &dyn crate::persistence::replay::CommandReplayEngine,
) -> Result<(usize, u64, Vec<OrderedEntry>), crate::error::MoonError> {
    debug_assert_eq!(
        manifest.layout,
        AofLayout::PerShard,
        "replay_per_shard called on TopLevel manifest"
    );
    if manifest.shards.len() != per_shard_databases.len() {
        return Err(crate::error::MoonError::from(
            crate::error::AofError::RewriteFailed {
                detail: format!(
                    "replay_per_shard shard-count mismatch: manifest has {} shards, caller passed {} database vectors",
                    manifest.shards.len(),
                    per_shard_databases.len()
                ),
            },
        ));
    }

    let mut total: usize = 0;
    let mut global_max_lsn: u64 = 0;
    let mut ordered_entries: Vec<OrderedEntry> = Vec::new();

    for shard_id in 0..manifest.shards.len() {
        let sid = shard_id as u16;
        let base_path = manifest.shard_base_path(sid);
        let incr_path = manifest.shard_incr_path(sid);
        let databases = &mut *per_shard_databases[shard_id];

        // Load this shard's base RDB.
        if base_path.exists() {
            match crate::persistence::rdb::load(databases, &base_path) {
                Ok(n) => {
                    info!(
                        "AOF shard-{} base RDB loaded: {} keys from {}",
                        sid,
                        n,
                        base_path.display()
                    );
                    total += n;
                }
                Err(e) => {
                    error!("AOF shard-{} base RDB load failed: {}", sid, e);
                    return Err(e);
                }
            }
        } else {
            // Missing base is tolerable only when this shard's incr file is
            // empty (or absent). Same invariant as `replay_multi_part`.
            let incr_len = std::fs::metadata(&incr_path).map(|m| m.len()).unwrap_or(0);
            if incr_len > 0 {
                return Err(crate::error::MoonError::from(
                    crate::error::AofError::RewriteFailed {
                        detail: format!(
                            "AOF shard-{} base RDB missing at {} but incr {} is {} bytes; refusing to replay incr against empty state",
                            sid,
                            base_path.display(),
                            incr_path.display(),
                            incr_len,
                        ),
                    },
                ));
            }
            warn!(
                "AOF shard-{} base RDB not found: {} (incr empty, treating as fresh init)",
                sid,
                base_path.display()
            );
        }

        // Replay this shard's framed incr file.
        if incr_path.exists() {
            let data = std::fs::read(&incr_path)?;
            if !data.is_empty() {
                let (count, shard_max_lsn) =
                    replay_incr_framed(sid, databases, &data, engine, &mut ordered_entries)?;
                info!(
                    "AOF shard-{} incr replayed: {} commands from {} (max lsn {})",
                    sid,
                    count,
                    incr_path.display(),
                    shard_max_lsn
                );
                total += count;
                if shard_max_lsn > global_max_lsn {
                    global_max_lsn = shard_max_lsn;
                }
            }
        }
    }

    Ok((total, global_max_lsn, ordered_entries))
}

/// Merge-replay `OrderedAcrossShards` entries collected across all shards
/// in global LSN order (RFC § 2 Rule 2).
///
/// `entries` is sorted by `lsn` ascending, then each entry is dispatched
/// against its origin shard's databases — the per-shard partition is
/// preserved because each `OrderedEntry` carries the `shard_id` it was
/// read from. This guarantees that a cross-shard atomic operation
/// committed at LSN N is replayed as a coherent group (every
/// shard's portion at LSN N is applied before any shard's LSN N+1 work).
///
/// **Crash-time atomicity:** if a cross-shard commit was mid-write at
/// crash time, some shards may have the LSN-N entry while others don't.
/// Step 5 ships the merge mechanism only; detecting partial commits and
/// performing the corresponding rollback is left to the future cross-shard
/// TXN consumer — `replay_ordered_merge` currently best-effort-applies
/// whichever entries survived. A `warn!` is emitted when the entry count
/// per LSN is uneven across shards so operators have a forensic trail.
///
/// **Today's emitters:** none in production code. The path is exercised
/// by tests so the round-trip wiring is verified end-to-end and ready for
/// future use.
pub fn replay_ordered_merge(
    per_shard_databases: &mut [&mut [crate::storage::Database]],
    mut entries: Vec<OrderedEntry>,
    engine: &dyn crate::persistence::replay::CommandReplayEngine,
) -> Result<usize, crate::error::MoonError> {
    use crate::protocol::{Frame, ParseConfig, parse};
    use bytes::BytesMut;

    if entries.is_empty() {
        return Ok(0);
    }

    entries.sort_by_key(|e| e.lsn);

    // Per-LSN cardinality audit: detect torn cross-shard commits.
    //
    // A "torn" commit is one where LSN N appears in fewer shard files than
    // the maximum cardinality seen for any other LSN in this batch. Applying
    // partial entries violates atomicity — if the write was interrupted mid-
    // commit (e.g., crash between shard-0 and shard-1 writes), replaying only
    // the shard-0 portion produces an inconsistent state that cannot be
    // compensated. DROP the entire torn LSN instead of applying partial data.
    //
    // NOTE: "torn" detection is heuristic — it compares each LSN's count
    // against the maximum cardinality observed. An LSN that legitimately spans
    // fewer shards (e.g. single-shard ordered op) can only occur if the batch
    // is heterogeneous. Production emitters (future cross-shard TXN) must
    // guarantee uniform cardinality per LSN, so this heuristic is correct for
    // all currently-reachable code paths.
    let mut counts: std::collections::BTreeMap<u64, usize> =
        std::collections::BTreeMap::new();
    for e in &entries {
        *counts.entry(e.lsn).or_insert(0) += 1;
    }
    let max_count = counts.values().copied().max().unwrap_or(0);
    let mut torn_lsns: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
    for (&lsn, &n) in &counts {
        if n < max_count {
            warn!(
                "OrderedAcrossShards LSN {} appears in only {} of {} shard files; \
                 torn cross-shard commit detected — dropping entry for atomicity",
                lsn, n, max_count
            );
            torn_lsns.insert(lsn);
        }
    }

    let config = ParseConfig::default();
    let mut replayed: usize = 0;

    for entry in entries {
        // Skip entries belonging to a torn (partially-written) commit.
        if torn_lsns.contains(&entry.lsn) {
            continue;
        }
        let shard_idx = entry.shard_id as usize;
        if shard_idx >= per_shard_databases.len() {
            return Err(crate::error::MoonError::from(
                crate::error::AofError::RewriteFailed {
                    detail: format!(
                        "OrderedAcrossShards entry references shard {} but only {} shards present",
                        entry.shard_id,
                        per_shard_databases.len()
                    ),
                },
            ));
        }
        let mut buf = BytesMut::from(entry.bytes.as_ref());
        match parse::parse(&mut buf, &config) {
            Ok(Some(Frame::Array(arr))) if !arr.is_empty() => {
                let cmd = match &arr[0] {
                    Frame::BulkString(s) => s.as_ref(),
                    Frame::SimpleString(s) => s.as_ref(),
                    _ => {
                        return Err(crate::error::MoonError::from(
                            crate::error::AofError::RewriteFailed {
                                detail: format!(
                                    "OrderedAcrossShards entry at lsn {} has non-string command frame",
                                    entry.lsn
                                ),
                            },
                        ));
                    }
                };
                let mut selected_db: usize = 0;
                let databases = &mut *per_shard_databases[shard_idx];
                engine.replay_command(databases, cmd, &arr[1..], &mut selected_db);
                replayed += 1;
            }
            other => {
                return Err(crate::error::MoonError::from(
                    crate::error::AofError::RewriteFailed {
                        detail: format!(
                            "OrderedAcrossShards entry at lsn {} on shard {} did not parse as RESP array: {:?}",
                            entry.lsn,
                            entry.shard_id,
                            other.map(|_| ()).err()
                        ),
                    },
                ));
            }
        }
    }

    Ok(replayed)
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
        let d = std::env::temp_dir().join(format!(
            "moon-aof-manifest-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
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
                ShardManifest { shard_id: 0, max_lsn: 0 },
                ShardManifest { shard_id: 1, max_lsn: 0 },
            ],
        };
        let err = m.verify_shard_count(4).expect_err("should mismatch");
        assert_eq!(
            err,
            "ERR shard count changed (manifest=2, config=4); refusing to start to avoid data loss. See docs/runbooks/shard-count-change.md"
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
                ShardManifest { shard_id: 0, max_lsn: 100 },
                ShardManifest { shard_id: 1, max_lsn: 500 },
                ShardManifest { shard_id: 2, max_lsn: 250 },
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
        let stray = dir
            .join(AOF_DIR_NAME)
            .join("moon.aof.1.base.rdb");
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

    // -- Step 4 (per-shard replay) tests ---------------------------------

    fn frame_entry(lsn: u64, resp: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(12 + resp.len());
        buf.extend_from_slice(&lsn.to_le_bytes());
        buf.extend_from_slice(&(resp.len() as u32).to_le_bytes());
        buf.extend_from_slice(resp);
        buf
    }

    /// Minimal `CommandReplayEngine` that records (lsn-implicit-via-order, cmd
    /// name) calls without touching real storage. Tests use this to assert
    /// the framed parser hands the right command sequence to the engine.
    struct RecordingEngine {
        calls: std::cell::RefCell<Vec<String>>,
    }

    impl RecordingEngine {
        fn new() -> Self {
            Self {
                calls: std::cell::RefCell::new(Vec::new()),
            }
        }
    }

    impl crate::persistence::replay::CommandReplayEngine for RecordingEngine {
        fn replay_command(
            &self,
            _databases: &mut [crate::storage::Database],
            cmd: &[u8],
            _args: &[crate::protocol::Frame],
            _selected_db: &mut usize,
        ) {
            self.calls
                .borrow_mut()
                .push(String::from_utf8_lossy(cmd).into_owned());
        }
    }

    #[test]
    fn replay_incr_framed_decodes_lsn_and_resp() {
        // Two framed entries: PING and DBSIZE (no args, both small RESP arrays).
        let mut bytes = frame_entry(7, b"*1\r\n$4\r\nPING\r\n");
        bytes.extend_from_slice(&frame_entry(11, b"*1\r\n$6\r\nDBSIZE\r\n"));

        let mut dbs: Vec<crate::storage::Database> = vec![crate::storage::Database::new()];
        let engine = RecordingEngine::new();
        let mut ordered: Vec<OrderedEntry> = Vec::new();
        let (count, max_lsn) = replay_incr_framed(0, &mut dbs, &bytes, &engine, &mut ordered)
            .expect("framed replay");
        assert!(ordered.is_empty(), "no ordered entries in this stream");

        assert_eq!(count, 2);
        assert_eq!(max_lsn, 11);
        let calls = engine.calls.borrow();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0], "PING");
        assert_eq!(calls[1], "DBSIZE");
    }

    #[test]
    fn replay_incr_framed_truncated_header_is_crash_eof() {
        // One valid entry, then a partial 5-byte header (crash mid-write).
        let mut bytes = frame_entry(3, b"*1\r\n$4\r\nPING\r\n");
        bytes.extend_from_slice(&[0u8; 5]);

        let mut dbs: Vec<crate::storage::Database> = vec![crate::storage::Database::new()];
        let engine = RecordingEngine::new();
        let mut ordered: Vec<OrderedEntry> = Vec::new();
        let (count, max_lsn) =
            replay_incr_framed(0, &mut dbs, &bytes, &engine, &mut ordered)
                .expect("truncated-header is EOF");

        assert_eq!(count, 1);
        assert_eq!(max_lsn, 3);
    }

    #[test]
    fn replay_incr_framed_truncated_payload_is_crash_eof() {
        // Header declares 14 bytes of RESP but only 5 actually present.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&5u64.to_le_bytes());
        bytes.extend_from_slice(&14u32.to_le_bytes());
        bytes.extend_from_slice(b"*1\r\n$"); // 5 bytes, payload truncated

        let mut dbs: Vec<crate::storage::Database> = vec![crate::storage::Database::new()];
        let engine = RecordingEngine::new();
        let mut ordered: Vec<OrderedEntry> = Vec::new();
        let (count, max_lsn) =
            replay_incr_framed(0, &mut dbs, &bytes, &engine, &mut ordered)
                .expect("truncated-payload is EOF");

        assert_eq!(count, 0);
        assert_eq!(max_lsn, 0);
    }

    #[test]
    fn replay_incr_framed_complete_but_corrupt_payload_errors() {
        // Header declares 4 bytes, payload is 4 bytes of garbage that won't
        // parse as a RESP frame.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&4u32.to_le_bytes());
        bytes.extend_from_slice(b"XXXX");

        let mut dbs: Vec<crate::storage::Database> = vec![crate::storage::Database::new()];
        let engine = RecordingEngine::new();
        let mut ordered: Vec<OrderedEntry> = Vec::new();
        let err = replay_incr_framed(0, &mut dbs, &bytes, &engine, &mut ordered)
            .expect_err("complete-but-corrupt should error");
        let msg = format!("{err}");
        assert!(
            msg.contains("framed"),
            "error should mention framed context, got: {msg}"
        );
    }

    #[test]
    fn replay_per_shard_round_trips_two_shards() {
        use crate::persistence::replay::DispatchReplayEngine;

        let dir = temp_dir();
        let manifest =
            AofManifest::initialize_multi(&dir, 2).expect("initialize_multi 2 shards");

        // Hand-author framed incr files: shard-0 SETs k0/v0 at lsn=10,
        // shard-1 SETs k1/v1 at lsn=20.
        let set_k0 = frame_entry(10, b"*3\r\n$3\r\nSET\r\n$2\r\nk0\r\n$2\r\nv0\r\n");
        let set_k1 = frame_entry(20, b"*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n");
        fs::write(manifest.shard_incr_path(0), &set_k0).expect("write shard-0 incr");
        fs::write(manifest.shard_incr_path(1), &set_k1).expect("write shard-1 incr");

        // Two independent shard database vectors.
        let mut shard0: Vec<crate::storage::Database> = vec![crate::storage::Database::new()];
        let mut shard1: Vec<crate::storage::Database> = vec![crate::storage::Database::new()];

        let (total, global_max_lsn, ordered) = {
            let mut slices: Vec<&mut [crate::storage::Database]> =
                vec![&mut shard0, &mut shard1];
            replay_per_shard(&mut slices, &manifest, &DispatchReplayEngine::new())
                .expect("per-shard replay")
        };

        assert_eq!(total, 2, "two SETs replayed");
        assert_eq!(global_max_lsn, 20, "global max lsn = max(shard maxes)");
        assert!(ordered.is_empty(), "no ordered entries in this stream");

        // Each shard's DB now holds its key (and only its key).
        assert!(shard0[0].len() >= 1, "shard 0 has k0");
        assert!(shard1[0].len() >= 1, "shard 1 has k1");

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn replay_per_shard_rejects_shard_count_mismatch() {
        use crate::persistence::replay::DispatchReplayEngine;

        let dir = temp_dir();
        let manifest =
            AofManifest::initialize_multi(&dir, 2).expect("initialize_multi 2 shards");

        // Only one slice — manifest says 2.
        let mut shard0: Vec<crate::storage::Database> = vec![crate::storage::Database::new()];
        let mut slices: Vec<&mut [crate::storage::Database]> = vec![&mut shard0];

        let err =
            replay_per_shard(&mut slices, &manifest, &DispatchReplayEngine::new())
                .expect_err("shard count mismatch must error");
        let msg = format!("{err}");
        assert!(
            msg.contains("shard-count mismatch"),
            "error message should call out the mismatch, got: {msg}"
        );

        fs::remove_dir_all(&dir).ok();
    }

    // -- Step 5 (OrderedAcrossShards merge) tests ------------------------

    /// Frame an ordered entry: same on-disk layout as `frame_entry`, with
    /// the high bit of LSN set.
    fn frame_ordered(lsn: u64, resp: &[u8]) -> Vec<u8> {
        assert_eq!(
            lsn & crate::persistence::aof::ORDERED_LSN_FLAG,
            0,
            "test helper expects raw lsn without the ordered flag"
        );
        let tagged = lsn | crate::persistence::aof::ORDERED_LSN_FLAG;
        let mut buf = Vec::with_capacity(12 + resp.len());
        buf.extend_from_slice(&tagged.to_le_bytes());
        buf.extend_from_slice(&(resp.len() as u32).to_le_bytes());
        buf.extend_from_slice(resp);
        buf
    }

    #[test]
    fn replay_incr_framed_buffers_ordered_entries() {
        // Mix: normal PING, then an ordered SET, then normal DBSIZE.
        let mut bytes = frame_entry(5, b"*1\r\n$4\r\nPING\r\n");
        bytes.extend_from_slice(&frame_ordered(
            8,
            b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n",
        ));
        bytes.extend_from_slice(&frame_entry(12, b"*1\r\n$6\r\nDBSIZE\r\n"));

        let mut dbs: Vec<crate::storage::Database> =
            vec![crate::storage::Database::new()];
        let engine = RecordingEngine::new();
        let mut ordered: Vec<OrderedEntry> = Vec::new();
        let (count, max_lsn) =
            replay_incr_framed(3, &mut dbs, &bytes, &engine, &mut ordered)
                .expect("framed replay with ordered");

        assert_eq!(count, 2, "two inline entries dispatched (PING, DBSIZE)");
        assert_eq!(max_lsn, 12, "max LSN tracks both inline and ordered");
        assert_eq!(ordered.len(), 1, "one entry buffered as ordered");
        let buffered = &ordered[0];
        assert_eq!(buffered.shard_id, 3, "shard_id forwarded");
        assert_eq!(
            buffered.lsn, 8,
            "buffered LSN has the high bit masked off"
        );
        let calls = engine.calls.borrow();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0], "PING");
        assert_eq!(calls[1], "DBSIZE", "ordered SET was NOT dispatched inline");
    }

    #[test]
    fn replay_ordered_merge_sorts_by_lsn_across_shards() {
        use crate::persistence::replay::DispatchReplayEngine;

        // Three ordered entries across two shards, deliberately out of LSN
        // order on the wire so the merge step has work to do.
        let entries = vec![
            OrderedEntry {
                shard_id: 1,
                lsn: 30,
                bytes: bytes::Bytes::from_static(b"*3\r\n$3\r\nSET\r\n$2\r\nb1\r\n$1\r\n3\r\n"),
            },
            OrderedEntry {
                shard_id: 0,
                lsn: 10,
                bytes: bytes::Bytes::from_static(b"*3\r\n$3\r\nSET\r\n$2\r\na1\r\n$1\r\n1\r\n"),
            },
            OrderedEntry {
                shard_id: 0,
                lsn: 20,
                bytes: bytes::Bytes::from_static(b"*3\r\n$3\r\nSET\r\n$2\r\na2\r\n$1\r\n2\r\n"),
            },
        ];

        let mut shard0: Vec<crate::storage::Database> =
            vec![crate::storage::Database::new()];
        let mut shard1: Vec<crate::storage::Database> =
            vec![crate::storage::Database::new()];
        let replayed = {
            let mut slices: Vec<&mut [crate::storage::Database]> =
                vec![&mut shard0, &mut shard1];
            replay_ordered_merge(&mut slices, entries, &DispatchReplayEngine::new())
                .expect("ordered merge replay")
        };

        assert_eq!(replayed, 3);
        assert!(shard0[0].len() >= 2, "shard 0 received a1 + a2");
        assert!(shard1[0].len() >= 1, "shard 1 received b1");
    }

    #[test]
    fn replay_ordered_merge_empty_returns_zero() {
        use crate::persistence::replay::DispatchReplayEngine;

        let mut shard0: Vec<crate::storage::Database> =
            vec![crate::storage::Database::new()];
        let mut slices: Vec<&mut [crate::storage::Database]> = vec![&mut shard0];
        let replayed =
            replay_ordered_merge(&mut slices, Vec::new(), &DispatchReplayEngine::new())
                .expect("empty merge ok");
        assert_eq!(replayed, 0);
    }

    /// FIX-W3-3: torn cross-shard commit must be DROPPED entirely, not partially applied.
    ///
    /// Synthesize a 2-shard AOF where LSN 100 appears on shard 0 only (N=1
    /// of K=2 expected). After replay, shard 0 must NOT have the key written
    /// by the LSN-100 entry (it was dropped for atomicity).
    #[test]
    fn replay_ordered_merge_drops_torn_commit() {
        use crate::persistence::replay::DispatchReplayEngine;

        // Two shards, two complete entries at LSN 10 (one per shard) — these
        // should succeed. LSN 100 appears only on shard 0 (torn) — must be dropped.
        let entries = vec![
            // Complete pair: LSN 10 on both shards
            OrderedEntry {
                shard_id: 0,
                lsn: 10,
                bytes: bytes::Bytes::from_static(
                    b"*3\r\n$3\r\nSET\r\n$2\r\nc0\r\n$1\r\n1\r\n",
                ),
            },
            OrderedEntry {
                shard_id: 1,
                lsn: 10,
                bytes: bytes::Bytes::from_static(
                    b"*3\r\n$3\r\nSET\r\n$2\r\nc1\r\n$1\r\n1\r\n",
                ),
            },
            // Torn entry: LSN 100 only on shard 0, not shard 1
            OrderedEntry {
                shard_id: 0,
                lsn: 100,
                bytes: bytes::Bytes::from_static(
                    b"*3\r\n$3\r\nSET\r\n$5\r\ntorn0\r\n$1\r\nv\r\n",
                ),
            },
        ];

        let mut shard0: Vec<crate::storage::Database> =
            vec![crate::storage::Database::new()];
        let mut shard1: Vec<crate::storage::Database> =
            vec![crate::storage::Database::new()];
        let replayed = {
            let mut slices: Vec<&mut [crate::storage::Database]> =
                vec![&mut shard0, &mut shard1];
            replay_ordered_merge(&mut slices, entries, &DispatchReplayEngine::new())
                .expect("ordered merge replay")
        };

        // The torn LSN-100 entry must NOT be applied (dropped for atomicity).
        assert_eq!(replayed, 2, "only the complete LSN-10 pair is replayed");
        assert_eq!(
            shard0[0].len(),
            1,
            "shard-0 only has the complete LSN-10 key; torn LSN-100 entry must not be applied"
        );
        // Verify the torn key is absent
        assert!(
            shard0[0].get(b"torn0").is_none(),
            "torn shard-0 entry (LSN 100) must NOT be applied"
        );
    }

    #[test]
    fn ordered_entry_lsn_flag_set_via_try_send_append_ordered() {
        use crate::persistence::aof::{AofMessage, AofWriterPool, ORDERED_LSN_FLAG};
        use crate::runtime::channel;

        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(4);
        let pool = AofWriterPool::per_shard(vec![tx0, tx1]);

        // Raw lsn = 42; high bit must end up set on the receive side.
        pool.try_send_append_ordered(0, 42, bytes::Bytes::from_static(b"x"));
        let msg = rx0.try_recv().expect("ordered append delivered");
        match msg {
            AofMessage::Append { lsn, .. } => {
                assert_eq!(
                    lsn & ORDERED_LSN_FLAG,
                    ORDERED_LSN_FLAG,
                    "ordered flag set on lsn"
                );
                assert_eq!(
                    lsn & !ORDERED_LSN_FLAG,
                    42,
                    "low bits preserve the original lsn"
                );
            }
            _ => panic!("expected Append"),
        }
    }
}
