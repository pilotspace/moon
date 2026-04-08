//! Multi-part AOF manifest: tracks base (RDB) and incremental (RESP) files.
//!
//! Implements the same directory-based AOF format as Redis 7+:
//! ```text
//! appendonlydir/
//!   moon.aof.1.base.rdb     # RDB snapshot base
//!   moon.aof.1.incr.aof     # Incremental RESP since base
//!   moon.aof.manifest        # This file
//! ```
//!
//! The manifest is a simple text file listing the active base and incremental
//! files with their sequence numbers. On BGREWRITEAOF, the sequence increments,
//! a new base + incr pair is created, and old files are deleted.

use std::io::Write;
use std::path::{Path, PathBuf};

use tracing::{error, info, warn};

const MANIFEST_NAME: &str = "moon.aof.manifest";
const AOF_DIR_NAME: &str = "appendonlydir";

/// Active AOF file set tracked by the manifest.
#[derive(Debug, Clone)]
pub struct AofManifest {
    /// Base directory (parent of `appendonlydir/`)
    pub dir: PathBuf,
    /// Current sequence number (incremented on each rewrite)
    pub seq: u64,
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
    pub fn base_path(&self) -> PathBuf {
        self.aof_dir()
            .join(format!("moon.aof.{}.base.rdb", self.seq))
    }

    /// Path to the incremental RESP file for the current sequence.
    pub fn incr_path(&self) -> PathBuf {
        self.aof_dir()
            .join(format!("moon.aof.{}.incr.aof", self.seq))
    }

    /// Path to the base RDB file for a given sequence.
    pub fn base_path_seq(&self, seq: u64) -> PathBuf {
        self.aof_dir().join(format!("moon.aof.{}.base.rdb", seq))
    }

    /// Path to the incremental RESP file for a given sequence.
    pub fn incr_path_seq(&self, seq: u64) -> PathBuf {
        self.aof_dir().join(format!("moon.aof.{}.incr.aof", seq))
    }

    /// Create the `appendonlydir/` and write the initial manifest.
    pub fn initialize(dir: &Path) -> std::io::Result<Self> {
        let manifest = Self {
            dir: dir.to_path_buf(),
            seq: 1,
        };
        std::fs::create_dir_all(manifest.aof_dir())?;
        manifest.write_manifest()?;
        Ok(manifest)
    }

    /// Load manifest from disk. Returns `None` if manifest doesn't exist.
    pub fn load(dir: &Path) -> Option<Self> {
        let aof_dir = dir.join(AOF_DIR_NAME);
        let manifest_path = aof_dir.join(MANIFEST_NAME);

        if !manifest_path.exists() {
            return None;
        }

        let content = match std::fs::read_to_string(&manifest_path) {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to read AOF manifest: {}", e);
                return None;
            }
        };

        let mut seq = 0u64;
        for line in content.lines() {
            let line = line.trim();
            if let Some(val) = line.strip_prefix("seq ") {
                if let Ok(n) = val.parse::<u64>() {
                    seq = n;
                }
            }
        }

        if seq == 0 {
            error!("AOF manifest has no valid sequence number");
            return None;
        }

        Some(Self {
            dir: dir.to_path_buf(),
            seq,
        })
    }

    /// Write the manifest file atomically (write tmp + rename).
    pub fn write_manifest(&self) -> std::io::Result<()> {
        let manifest_path = self.manifest_path();
        let tmp_path = manifest_path.with_extension("tmp");

        let content = format!(
            "seq {}\nbase moon.aof.{}.base.rdb\nincr moon.aof.{}.incr.aof\n",
            self.seq, self.seq, self.seq
        );

        let mut f = std::fs::File::create(&tmp_path)?;
        f.write_all(content.as_bytes())?;
        f.sync_data()?;
        std::fs::rename(&tmp_path, &manifest_path)?;
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

        // 1. Write new base RDB (atomic: tmp + rename)
        let new_base = self.base_path_seq(new_seq);
        let tmp_base = new_base.with_extension("rdb.tmp");
        std::fs::write(&tmp_base, rdb_bytes).map_err(|e| crate::error::AofError::Io {
            path: tmp_base.clone(),
            source: e,
        })?;
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
        warn!("AOF base RDB not found: {}", base_path.display());
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
                            _ => {
                                count += 1;
                                continue;
                            }
                        };
                        (name as &[u8], &arr[1..])
                    }
                    _ => {
                        count += 1;
                        continue;
                    }
                };
                engine.replay_command(databases, cmd, cmd_args, &mut selected_db);
                count += 1;
            }
            Ok(None) => {
                if !buf.is_empty() {
                    let offset = total_len - buf.len();
                    warn!(
                        "AOF incr truncated: {} bytes at offset {}",
                        buf.len(),
                        offset
                    );
                }
                break;
            }
            Err(_) => {
                let _ = buf.split_to(1);
                if let Some(pos) = buf.iter().position(|&b| b == b'*') {
                    let _ = buf.split_to(pos);
                } else {
                    break;
                }
            }
        }
    }

    Ok(count)
}
