//! AOF v1→v2 migration: single-file legacy AOF to per-shard PerShard layout.
//!
//! # Background
//!
//! Moon v0.1.x used a single `appendonly.aof` (or `appendonlydir/` TopLevel layout
//! with one base RDB + one incr file). v0.1.12 introduces the PerShard layout where
//! each shard has its own `shard-N/moon.aof.1.base.rdb` + `shard-N/moon.aof.1.incr.aof`.
//!
//! Operators upgrading from a single-shard v1 deployment to a multi-shard v2
//! deployment need to redistribute their existing AOF entries across N shard files.
//! This module provides the migration logic.
//!
//! # Algorithm
//!
//! 1. Load the source RDB base (preamble or separate base.rdb) into scratch
//!    databases. Partition each key by `key_to_shard(key, num_shards)` into
//!    per-shard scratch databases. Serialize each shard's scratch database as its
//!    `shard-N/moon.aof.1.base.rdb` (replaces the empty placeholder from
//!    `initialize_multi`). This preserves all data that was in the RDB preamble —
//!    which is where the bulk of the data lives after `BGREWRITEAOF`.
//! 2. Read the RESP tail from the source AOF. For each command, extract the first
//!    key argument and route to a shard via `key_to_shard(key, num_shards)`.
//!    Commands without a key argument (PING, DBSIZE, FLUSHDB, FLUSHALL) are
//!    routed to shard 0 (conservative — FLUSHDB/FLUSHALL affect all shards but
//!    the migration path leaves the operator to verify). SELECT 0 is skipped as
//!    a no-op; SELECT N (N>0) causes an immediate `Err` — multi-DB AOF cannot
//!    be safely migrated (see Limitations).
//! 3. Write each RESP command to the target shard's incr file in v2 framing format:
//!    `[u64 lsn LE][u32 len LE][RESP bytes]`. LSNs are sequential per-shard
//!    counters starting at 1. One corrupt command stops the remainder of the incr
//!    migration (safe truncation — matches crash-time EOF behavior).
//! 4. Write a v2 manifest covering all shards with `seq = 1`.
//!
//! # Limitations
//!
//! - Multi-db AOF (SELECT N where N>0): migration returns `Err` immediately.
//!   Per-shard replay runs each shard independently and cannot preserve the
//!   logical database across commands — silently dropping SELECT would corrupt
//!   data. SELECT 0 is treated as a no-op and skipped. Operators must flush
//!   non-default databases before migrating.
//! - MULTI/EXEC blocks are not treated atomically — each command in the block
//!   is routed independently.
//! - Keyless commands (FLUSHDB, FLUSHALL) in the RESP tail go to shard 0 only;
//!   operators must verify correctness for these commands.
//!
//! # Usage
//!
//! ```
//! moon --migrate-aof-from /old/dir --migrate-aof-to /new/dir --migrate-aof-shards 4
//! ```
//!
//! The server exits after migration. Start the server normally pointing at
//! `--migrate-aof-to` to use the migrated data.

use std::io::Write;
use std::path::{Path, PathBuf};

use bytes::{Bytes, BytesMut};
use tracing::{info, warn};

use crate::persistence::aof_manifest::AofManifest;
use crate::protocol::{Frame, ParseConfig, parse};
use crate::storage::Database;

/// Outcome of a migrate-aof run.
#[derive(Debug)]
pub struct MigrateAofResult {
    /// Total RESP commands read from the source AOF.
    pub commands_read: usize,
    /// Commands routed and written to a shard incr file.
    pub commands_written: usize,
    /// Commands that could not be routed (no key arg) and were skipped.
    pub commands_skipped: usize,
    /// Number of keys migrated from the source RDB base/preamble.
    pub rdb_keys_migrated: usize,
}

/// Migrate a legacy single-file AOF from `from_dir` into a PerShard layout at `to_dir`.
///
/// `from_dir` may contain:
///   - `appendonly.aof` (flat RESP or RDB-preamble RESP)
///   - `appendonlydir/` with `moon.aof.{seq}.base.rdb` + `moon.aof.{seq}.incr.aof`
///     (TopLevel manifest format)
///
/// `to_dir` must be empty or non-existent. The function creates the PerShard
/// directory layout under `to_dir/appendonlydir/`.
///
/// Returns a summary of the migration or an error.
pub fn migrate_aof(
    from_dir: &Path,
    to_dir: &Path,
    num_shards: u16,
) -> Result<MigrateAofResult, crate::error::MoonError> {
    if num_shards == 0 {
        return Err(crate::error::MoonError::from(
            crate::error::AofError::RewriteFailed {
                detail: "migrate_aof: num_shards must be >= 1".to_owned(),
            },
        ));
    }

    // ── Guard: from_dir == to_dir ────────────────────────────────────────────
    // Migrating into the same directory would clobber the source layout.
    if from_dir == to_dir {
        return Err(crate::error::MoonError::from(
            crate::error::AofError::RewriteFailed {
                detail: format!(
                    "migrate_aof: from_dir and to_dir must differ (both are {}). \
                     Specify a separate empty directory for --migrate-aof-to.",
                    from_dir.display()
                ),
            },
        ));
    }

    // ── Guard: to_dir must not already contain AOF data ─────────────────────
    // A PerShard manifest in to_dir means a previous migration ran (or the
    // operator is reusing a live data directory). Refuse to avoid partial
    // overwrites — use a fresh --migrate-aof-to.
    match AofManifest::load(to_dir) {
        Ok(Some(_)) => {
            return Err(crate::error::MoonError::from(
                crate::error::AofError::RewriteFailed {
                    detail: format!(
                        "migrate_aof: to_dir ({}) already contains an AOF manifest. \
                         Use a fresh, non-existent or empty directory for --migrate-aof-to.",
                        to_dir.display()
                    ),
                },
            ));
        }
        Ok(None) => {} // expected: to_dir is empty or non-existent
        Err(_) => {}   // I/O errors from a non-existent dir are fine; proceed
    }

    // ── Step 1: Load source data ─────────────────────────────────────────────
    // Returns the RDB base bytes (if any) and the pure-RESP tail bytes.
    let (rdb_base_bytes, resp_tail) = load_source(from_dir)?;

    // ── Step 2: Initialize target PerShard manifest ──────────────────────────
    // This creates empty base RDB stubs and empty incr files for each shard.
    AofManifest::initialize_multi(to_dir, num_shards).map_err(|e| {
        crate::error::MoonError::from(crate::error::AofError::Io {
            path: to_dir.to_path_buf(),
            source: e,
        })
    })?;

    let manifest = AofManifest::load(to_dir)
        .map_err(|e| {
            crate::error::MoonError::from(crate::error::AofError::Io {
                path: to_dir.to_path_buf(),
                source: e,
            })
        })?
        .ok_or_else(|| {
            crate::error::MoonError::from(crate::error::AofError::RewriteFailed {
                detail: "migrate_aof: manifest not found after initialize_multi".to_owned(),
            })
        })?;

    // ── Step 3: Partition RDB preamble/base across shards ────────────────────
    let rdb_keys_migrated = if !rdb_base_bytes.is_empty() {
        partition_rdb_into_shards(&rdb_base_bytes, &manifest, num_shards)?
    } else {
        0
    };

    // ── Step 4: Append RESP tail to per-shard incr files ────────────────────
    let (commands_read, commands_written, commands_skipped) =
        append_resp_to_shards(&resp_tail, &manifest, num_shards)?;

    info!(
        "migrate_aof complete: {} RDB keys + {} RESP commands written across {} shards ({} skipped)",
        rdb_keys_migrated, commands_written, num_shards, commands_skipped
    );

    Ok(MigrateAofResult {
        commands_read,
        commands_written,
        commands_skipped,
        rdb_keys_migrated,
    })
}

// ── Internal helpers ─────────────────────────────────────────────────────────

/// Load the source data from `from_dir`.
///
/// Returns `(rdb_base_bytes, resp_tail_bytes)`:
/// - `rdb_base_bytes`: raw RDB bytes for the snapshot (may be empty if no preamble)
/// - `resp_tail_bytes`: pure RESP command stream following the preamble
fn load_source(from_dir: &Path) -> Result<(Bytes, Bytes), crate::error::MoonError> {
    // Option 1: flat appendonly.aof — may have RDB preamble + RESP tail.
    let flat = from_dir.join("appendonly.aof");
    if flat.exists() {
        info!("migrate_aof: reading from {}", flat.display());
        let raw = std::fs::read(&flat).map_err(|e| {
            crate::error::MoonError::from(crate::error::AofError::Io {
                path: flat.clone(),
                source: e,
            })
        })?;
        return Ok(split_rdb_preamble(raw.into()));
    }

    // Option 2: TopLevel manifest — separate base.rdb + incr.aof.
    if let Ok(Some(m)) = AofManifest::load(from_dir) {
        let base_path = m.shard_base_path_seq(0, m.seq);
        let incr_path = m.incr_path();

        let rdb_bytes = if base_path.exists() {
            info!("migrate_aof: reading base RDB from {}", base_path.display());
            std::fs::read(&base_path).map(Bytes::from).map_err(|e| {
                crate::error::MoonError::from(crate::error::AofError::Io {
                    path: base_path.clone(),
                    source: e,
                })
            })?
        } else {
            Bytes::new()
        };

        let resp_bytes = if incr_path.exists() {
            info!("migrate_aof: reading incr from {}", incr_path.display());
            std::fs::read(&incr_path).map(Bytes::from).map_err(|e| {
                crate::error::MoonError::from(crate::error::AofError::Io {
                    path: incr_path.clone(),
                    source: e,
                })
            })?
        } else {
            Bytes::new()
        };

        return Ok((rdb_bytes, resp_bytes));
    }

    Err(crate::error::MoonError::from(
        crate::error::AofError::RewriteFailed {
            detail: format!(
                "migrate_aof: no AOF source found in {}. \
                 Expected appendonly.aof or appendonlydir/moon.aof.*.(base|incr).aof",
                from_dir.display()
            ),
        },
    ))
}

/// Split AOF bytes into `(rdb_preamble_bytes, resp_tail_bytes)`.
///
/// If the file starts with `MOON` or `REDIS` magic, the RDB preamble is
/// split off — the raw preamble bytes (header through CRC32) are returned
/// as-is so `rdb::load_from_bytes` can parse them. If no preamble is
/// present, the first element is empty and the second is the entire input.
fn split_rdb_preamble(bytes: Bytes) -> (Bytes, Bytes) {
    const MOON_MAGIC: &[u8] = b"MOON";
    const REDIS_MAGIC: &[u8] = b"REDIS";
    const EOF_MARKER: u8 = 0xFF;

    if !bytes.starts_with(MOON_MAGIC) && !bytes.starts_with(REDIS_MAGIC) {
        // Pure RESP — no preamble.
        return (Bytes::new(), bytes);
    }

    // Locate the RDB end: scan for EOF_MARKER (0xFF) followed by 4 CRC bytes
    // whose CRC32 of bytes[0..=i] matches. Use the same single-pass approach
    // as `rdb::load_from_bytes`.
    use crc32fast::Hasher;
    let mut running_hasher = Hasher::new();
    if bytes.len() > 5 {
        running_hasher.update(&bytes[..5]);
    }
    for i in 5..bytes.len().saturating_sub(4) {
        running_hasher.update(&bytes[i..i + 1]);
        if bytes[i] == EOF_MARKER {
            // Candidate EOF: check CRC of bytes[0..=i] matches bytes[i+1..i+5]
            let stored =
                u32::from_le_bytes([bytes[i + 1], bytes[i + 2], bytes[i + 3], bytes[i + 4]]);
            // Clone the hasher to avoid consuming state (running_hasher must
            // continue in case this candidate is a false positive).
            let check = running_hasher.clone();
            if check.finalize() == stored {
                let rdb_end = i + 1 + 4; // past EOF marker + CRC32
                info!(
                    "migrate_aof: found RDB preamble of {} bytes at offset 0",
                    rdb_end
                );
                return (bytes.slice(..rdb_end), bytes.slice(rdb_end..));
            }
        }
    }

    // No valid EOF marker found — treat as pure RESP (or empty preamble).
    warn!("migrate_aof: no valid RDB EOF marker found; treating entire file as RESP");
    (Bytes::new(), bytes)
}

/// Load `rdb_bytes` into scratch databases, then partition each key into a
/// per-shard scratch database, and write per-shard base RDB files at
/// `manifest.shard_base_path(sid)`.
///
/// Returns the total number of keys partitioned.
fn partition_rdb_into_shards(
    rdb_bytes: &[u8],
    manifest: &AofManifest,
    num_shards: u16,
) -> Result<usize, crate::error::MoonError> {
    // Allocate scratch databases — one Database per logical db index (16 max).
    const MAX_DBS: usize = 16;
    let mut scratch: Vec<Database> = (0..MAX_DBS).map(|_| Database::new()).collect();

    // Load the RDB into scratch databases.
    let (keys_loaded, _consumed) =
        crate::persistence::rdb::load_from_bytes(&mut scratch, rdb_bytes)?;

    if keys_loaded == 0 {
        info!("migrate_aof: RDB preamble/base contains 0 live keys; skipping partitioning");
        return Ok(0);
    }

    info!(
        "migrate_aof: loaded {} keys from RDB; partitioning across {} shards",
        keys_loaded, num_shards
    );

    // Allocate per-shard scratch databases (same logical DB count as source).
    let mut shard_dbs: Vec<Vec<Database>> = (0..num_shards)
        .map(|_| (0..MAX_DBS).map(|_| Database::new()).collect())
        .collect();

    // Partition keys from scratch into shard_dbs.
    let mut total_partitioned: usize = 0;
    for (db_idx, db) in scratch.iter().enumerate() {
        for (key, entry) in db.data().iter() {
            let key_bytes = key.as_bytes();
            let shard_idx = crate::shard::dispatch::key_to_shard(key_bytes, num_shards as usize);
            shard_dbs[shard_idx][db_idx].set(Bytes::copy_from_slice(key_bytes), entry.clone());
            total_partitioned += 1;
        }
    }

    // Write per-shard base RDB files (overwrite the empty placeholders created
    // by initialize_multi).
    for sid in 0..num_shards {
        let base_path = manifest.shard_base_path(sid);
        let rdb = crate::persistence::rdb::save_to_bytes(&shard_dbs[sid as usize])?;
        let tmp = base_path.with_extension("rdb.tmp");
        {
            let mut f = std::fs::File::create(&tmp).map_err(|e| {
                crate::error::MoonError::from(crate::error::AofError::Io {
                    path: tmp.clone(),
                    source: e,
                })
            })?;
            f.write_all(&rdb).map_err(|e| {
                crate::error::MoonError::from(crate::error::AofError::Io {
                    path: tmp.clone(),
                    source: e,
                })
            })?;
            f.sync_data().map_err(|e| {
                crate::error::MoonError::from(crate::error::AofError::Io {
                    path: tmp.clone(),
                    source: e,
                })
            })?;
        }
        std::fs::rename(&tmp, &base_path).map_err(|e| {
            crate::error::MoonError::from(crate::error::AofError::Io {
                path: base_path.clone(),
                source: e,
            })
        })?;
        info!(
            "migrate_aof: shard-{} base RDB written ({} bytes)",
            sid,
            rdb.len()
        );
    }

    Ok(total_partitioned)
}

/// Parse the RESP tail and append framed entries to each shard's incr file.
///
/// Returns `(commands_read, commands_written, commands_skipped)`.
/// Stops at the first parse error (crash-time EOF semantics).
fn append_resp_to_shards(
    resp_bytes: &[u8],
    manifest: &AofManifest,
    num_shards: u16,
) -> Result<(usize, usize, usize), crate::error::MoonError> {
    if resp_bytes.is_empty() {
        return Ok((0, 0, 0));
    }

    // Open per-shard incr files for appending.
    let mut shard_files: Vec<std::fs::File> = (0..num_shards)
        .map(|sid| {
            std::fs::OpenOptions::new()
                .append(true)
                .open(manifest.shard_incr_path(sid))
                .map_err(|e| {
                    crate::error::MoonError::from(crate::error::AofError::Io {
                        path: manifest.shard_incr_path(sid),
                        source: e,
                    })
                })
        })
        .collect::<Result<_, _>>()?;

    let mut shard_lsn: Vec<u64> = vec![0; num_shards as usize];
    let config = ParseConfig::default();
    let mut buf = BytesMut::from(resp_bytes);
    let mut commands_read: usize = 0;
    let mut commands_written: usize = 0;
    let mut commands_skipped: usize = 0;

    loop {
        if buf.is_empty() {
            break;
        }
        match parse::parse(&mut buf, &config) {
            Ok(Some(frame)) => {
                commands_read += 1;
                let arr = match frame {
                    Frame::Array(ref arr) if !arr.is_empty() => arr,
                    _ => {
                        warn!(
                            "migrate_aof: non-array frame at command {}; skipping",
                            commands_read
                        );
                        commands_skipped += 1;
                        continue;
                    }
                };

                // Extract command name for routing decisions.
                let cmd_name = match &arr[0] {
                    Frame::BulkString(s) => s.clone(),
                    Frame::SimpleString(s) => s.clone(),
                    _ => {
                        warn!(
                            "migrate_aof: non-string command name at command {}; skipping",
                            commands_read
                        );
                        commands_skipped += 1;
                        continue;
                    }
                };

                // SELECT: allow only SELECT 0 (no-op); refuse SELECT N (N>0).
                // Per-shard replay runs each shard independently and does not
                // persist the logical database across commands. A multi-DB
                // legacy AOF (SELECT 1 + commands in db 1) cannot be safely
                // migrated — commands after SELECT N would land in db 0 of
                // their elected shard, silently corrupting data.
                let cmd_upper = cmd_name.to_ascii_uppercase();
                if cmd_upper.as_slice() == b"SELECT" {
                    // Parse the db argument (if present).
                    let db_arg = arr.get(1).and_then(|f| match f {
                        Frame::BulkString(b) => std::str::from_utf8(b.as_ref()).ok(),
                        Frame::SimpleString(s) => std::str::from_utf8(s.as_ref()).ok(),
                        _ => None,
                    });
                    let db_num: i64 = db_arg.and_then(|s| s.trim().parse().ok()).unwrap_or(0);
                    if db_num != 0 {
                        return Err(crate::error::MoonError::from(
                            crate::error::AofError::RewriteFailed {
                                detail: format!(
                                    "migrate_aof: multi-DB legacy AOF detected (SELECT {db_num} \
                                     at command {commands_read}). Per-shard replay cannot \
                                     correctly route commands from non-default databases. \
                                     Manually re-route or flush the non-default database \
                                     before migrating. Use a fresh --migrate-aof-to directory \
                                     after fixing the source AOF."
                                ),
                            },
                        ));
                    }
                    // SELECT 0 is a no-op — skip it silently.
                    commands_skipped += 1;
                    continue;
                }

                // Route to shard: keyless commands go to shard 0.
                let shard_idx = if arr.len() < 2 {
                    if matches!(cmd_upper.as_slice(), b"FLUSHALL" | b"FLUSHDB") {
                        warn!(
                            "migrate_aof: {} at command {} affects all shards but will only be \
                             written to shard 0; verify correctness after migration",
                            String::from_utf8_lossy(&cmd_upper),
                            commands_read
                        );
                    }
                    0usize
                } else {
                    match &arr[1] {
                        Frame::BulkString(k) => {
                            crate::shard::dispatch::key_to_shard(k.as_ref(), num_shards as usize)
                        }
                        Frame::SimpleString(k) => {
                            crate::shard::dispatch::key_to_shard(k.as_ref(), num_shards as usize)
                        }
                        _ => {
                            warn!(
                                "migrate_aof: non-string key at command {}; skipping",
                                commands_read
                            );
                            commands_skipped += 1;
                            continue;
                        }
                    }
                };

                // Serialize the original RESP frame for the framed incr entry.
                let mut resp_buf = BytesMut::new();
                crate::protocol::serialize::serialize(&frame, &mut resp_buf);
                let resp_bytes_out: Bytes = resp_buf.freeze();

                // Write framed entry: [u64 lsn LE][u32 len LE][RESP bytes].
                shard_lsn[shard_idx] += 1;
                let lsn = shard_lsn[shard_idx];
                let file = &mut shard_files[shard_idx];
                write_framed(
                    file,
                    lsn,
                    &resp_bytes_out,
                    manifest.shard_incr_path(shard_idx as u16),
                )?;
                commands_written += 1;
            }
            Ok(None) => {
                // Incomplete frame at end — crash-time EOF.
                if !buf.is_empty() {
                    warn!(
                        "migrate_aof: truncated RESP tail ({} bytes) at command {}; \
                         treating as crash-time EOF",
                        buf.len(),
                        commands_read
                    );
                }
                break;
            }
            Err(e) => {
                warn!(
                    "migrate_aof: parse error at command {}: {:?}; stopping migration \
                     (remaining {} bytes discarded — treat as crash-time EOF)",
                    commands_read,
                    e,
                    buf.len()
                );
                break;
            }
        }
    }

    // Fsync all shard incr files.
    for (sid, file) in shard_files.iter().enumerate() {
        file.sync_data().map_err(|e| {
            crate::error::MoonError::from(crate::error::AofError::Io {
                path: manifest.shard_incr_path(sid as u16),
                source: std::io::Error::new(e.kind(), format!("fsync shard-{sid}: {e}")),
            })
        })?;
    }

    Ok((commands_read, commands_written, commands_skipped))
}

/// Write one framed entry `[u64 lsn LE][u32 len LE][RESP bytes]` to `file`.
fn write_framed(
    file: &mut std::fs::File,
    lsn: u64,
    resp: &[u8],
    path: PathBuf,
) -> Result<(), crate::error::MoonError> {
    let len = resp.len() as u32;
    file.write_all(&lsn.to_le_bytes()).map_err(|e| {
        crate::error::MoonError::from(crate::error::AofError::Io {
            path: path.clone(),
            source: e,
        })
    })?;
    file.write_all(&len.to_le_bytes()).map_err(|e| {
        crate::error::MoonError::from(crate::error::AofError::Io {
            path: path.clone(),
            source: e,
        })
    })?;
    file.write_all(resp).map_err(|e| {
        crate::error::MoonError::from(crate::error::AofError::Io {
            path: path.clone(),
            source: e,
        })
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    /// Helper: serialize a RESP array command.
    fn cmd_resp(parts: &[&str]) -> Vec<u8> {
        let mut buf = BytesMut::new();
        let frames: Vec<Frame> = parts
            .iter()
            .map(|s| Frame::BulkString(Bytes::copy_from_slice(s.as_bytes())))
            .collect();
        let frame = Frame::Array(frames.into());
        crate::protocol::serialize::serialize(&frame, &mut buf);
        buf.to_vec()
    }

    // ── FIX-W3-2 red tests ──────────────────────────────────────────────────

    /// SELECT N (N>0) in a RESP tail must cause migrate_aof to return Err
    /// with a message mentioning "multi-DB". On current HEAD this test FAILS
    /// because SELECT is silently dropped (skipped) and Ok is returned.
    #[test]
    fn migrate_aof_select_nonzero_db_returns_err() {
        let src_dir = tempfile::tempdir().unwrap();
        let dst_dir = tempfile::tempdir().unwrap();

        // Build a RESP tail: SET a value, SELECT 1, SET b value.
        let mut aof_data: Vec<u8> = Vec::new();
        aof_data.extend(cmd_resp(&["SET", "a", "v"]));
        aof_data.extend(cmd_resp(&["SELECT", "1"]));
        aof_data.extend(cmd_resp(&["SET", "b", "v"]));
        std::fs::write(src_dir.path().join("appendonly.aof"), &aof_data).expect("write source aof");

        let result = migrate_aof(src_dir.path(), dst_dir.path(), 2);
        assert!(
            result.is_err(),
            "migrate_aof must refuse when RESP tail contains SELECT N (N>0)"
        );
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("multi-DB") || msg.contains("SELECT"),
            "error message must mention multi-DB or SELECT, got: {msg}"
        );
    }

    /// migrate_aof(same_dir, same_dir, n) must return Err with a guard message.
    /// Without a guard, this may succeed (incidentally error on manifest load)
    /// but would not carry a meaningful "same directory" message.
    #[test]
    fn migrate_aof_same_dir_returns_err() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("appendonly.aof"), b"").unwrap();

        let result = migrate_aof(dir.path(), dir.path(), 2);
        assert!(
            result.is_err(),
            "migrate_aof must refuse when from_dir == to_dir"
        );
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("same") || msg.contains("from_dir") || msg.contains("to_dir"),
            "error must identify the same-directory problem, got: {msg}"
        );
    }

    /// migrate_aof into a to_dir that already contains a PerShard manifest
    /// must return Err. Without the guard, initialize_multi may partially
    /// overwrite or the run silently proceeds.
    #[test]
    fn migrate_aof_existing_manifest_returns_err() {
        let src_dir = tempfile::tempdir().unwrap();
        let dst_dir = tempfile::tempdir().unwrap();
        std::fs::write(src_dir.path().join("appendonly.aof"), b"").unwrap();

        // Pre-populate to_dir with a PerShard manifest.
        AofManifest::initialize_multi(dst_dir.path(), 2).expect("first initialize_multi succeeds");

        let result = migrate_aof(src_dir.path(), dst_dir.path(), 2);
        assert!(
            result.is_err(),
            "migrate_aof must refuse when to_dir already contains AOF data"
        );
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("already") || msg.contains("exist") || msg.contains("non-empty"),
            "error must identify the pre-existing data problem, got: {msg}"
        );
    }

    /// Helper: serialize a SET command to RESP.
    fn set_resp(key: &str, val: &str) -> Vec<u8> {
        let mut buf = BytesMut::new();
        let frame = Frame::Array(
            vec![
                Frame::BulkString(Bytes::copy_from_slice(b"SET")),
                Frame::BulkString(Bytes::copy_from_slice(key.as_bytes())),
                Frame::BulkString(Bytes::copy_from_slice(val.as_bytes())),
            ]
            .into(),
        );
        crate::protocol::serialize::serialize(&frame, &mut buf);
        buf.to_vec()
    }

    #[test]
    fn migrate_aof_routes_keys_to_correct_shards() {
        let src_dir = tempfile::tempdir().unwrap();
        let dst_dir = tempfile::tempdir().unwrap();

        let mut aof_data: Vec<u8> = Vec::new();
        for i in 0..10u32 {
            aof_data.extend(set_resp(&format!("key{i}"), "value"));
        }

        std::fs::write(src_dir.path().join("appendonly.aof"), &aof_data).expect("write source aof");

        let result = migrate_aof(src_dir.path(), dst_dir.path(), 4).expect("migration succeeds");

        assert_eq!(result.commands_read, 10, "all 10 SETs read");
        assert_eq!(result.commands_written, 10, "all 10 SETs written");
        assert_eq!(result.commands_skipped, 0, "no skips");
        assert_eq!(result.rdb_keys_migrated, 0, "no RDB preamble");

        let manifest = AofManifest::load(dst_dir.path())
            .expect("load manifest")
            .expect("manifest present");
        assert_eq!(
            manifest.layout,
            crate::persistence::aof_manifest::AofLayout::PerShard
        );
        assert_eq!(manifest.shards.len(), 4, "4 shards in manifest");

        let total_incr_bytes: u64 = (0..4u16)
            .map(|sid| {
                std::fs::metadata(manifest.shard_incr_path(sid))
                    .map(|m| m.len())
                    .unwrap_or(0)
            })
            .sum();
        assert!(
            total_incr_bytes > 0,
            "at least some bytes written to shard incr files"
        );
    }

    #[test]
    fn migrate_aof_hash_tag_routing_deterministic() {
        let src_dir = tempfile::tempdir().unwrap();
        let dst_dir = tempfile::tempdir().unwrap();

        let mut aof_data: Vec<u8> = Vec::new();
        for i in 0..4u32 {
            aof_data.extend(set_resp(&format!("{{0}}:key{i}"), "value"));
        }
        std::fs::write(src_dir.path().join("appendonly.aof"), &aof_data).expect("write source aof");

        let result = migrate_aof(src_dir.path(), dst_dir.path(), 4).expect("migration succeeds");
        assert_eq!(result.commands_written, 4);

        let manifest = AofManifest::load(dst_dir.path())
            .expect("load manifest")
            .expect("present");
        let expected_shard = crate::shard::dispatch::key_to_shard(b"{0}:key0", 4) as u16;
        let shard_incr_len = std::fs::metadata(manifest.shard_incr_path(expected_shard))
            .map(|m| m.len())
            .unwrap_or(0);
        for sid in 0..4u16 {
            if sid == expected_shard {
                assert!(shard_incr_len > 0, "target shard has data");
            } else {
                let len = std::fs::metadata(manifest.shard_incr_path(sid))
                    .map(|m| m.len())
                    .unwrap_or(0);
                assert_eq!(len, 0, "non-target shard {} must be empty", sid);
            }
        }
    }

    #[test]
    fn migrate_aof_empty_source_produces_valid_manifest() {
        let src_dir = tempfile::tempdir().unwrap();
        let dst_dir = tempfile::tempdir().unwrap();

        std::fs::write(src_dir.path().join("appendonly.aof"), b"").expect("write empty source aof");

        let result = migrate_aof(src_dir.path(), dst_dir.path(), 2)
            .expect("migration of empty aof succeeds");
        assert_eq!(result.commands_read, 0);
        assert_eq!(result.commands_written, 0);
        assert_eq!(result.rdb_keys_migrated, 0);

        let manifest = AofManifest::load(dst_dir.path())
            .expect("load manifest")
            .expect("present");
        assert_eq!(manifest.shards.len(), 2);
    }

    /// Round-trip test (RESP-only source): migrate N keys via RESP tail, then
    /// call replay_per_shard and assert every key is recoverable.
    #[test]
    fn migrate_aof_round_trips_through_replay_per_shard() {
        use crate::persistence::aof_manifest::replay_per_shard;
        use crate::persistence::replay::DispatchReplayEngine;

        let src_dir = tempfile::tempdir().unwrap();
        let dst_dir = tempfile::tempdir().unwrap();
        const N_SHARDS: u16 = 4;

        let mut aof_data: Vec<u8> = Vec::new();
        let keys: Vec<String> = (0..12u32).map(|i| format!("key:{i}")).collect();
        for key in &keys {
            aof_data.extend(set_resp(key, "val"));
        }
        std::fs::write(src_dir.path().join("appendonly.aof"), &aof_data).expect("write source aof");

        let result =
            migrate_aof(src_dir.path(), dst_dir.path(), N_SHARDS).expect("migration succeeds");
        assert_eq!(result.commands_read, 12);
        assert_eq!(result.commands_written, 12);

        let manifest = AofManifest::load(dst_dir.path())
            .expect("load ok")
            .expect("manifest present");

        let mut shard_dbs: Vec<Vec<Database>> =
            (0..N_SHARDS).map(|_| vec![Database::new()]).collect();
        let mut slices: Vec<&mut [Database]> =
            shard_dbs.iter_mut().map(|v| v.as_mut_slice()).collect();

        let (total_replayed, _max_lsn, ordered) = replay_per_shard(
            &mut slices,
            &manifest,
            &(|| {
                Box::new(DispatchReplayEngine::new())
                    as Box<dyn crate::persistence::replay::CommandReplayEngine + Send>
            }),
        )
        .expect("replay_per_shard must succeed on migrated layout");

        assert_eq!(
            total_replayed, 12,
            "all 12 commands must be recovered by replay_per_shard"
        );
        assert!(
            ordered.is_empty(),
            "non-ordered commands must not appear in ordered buffer"
        );

        let mut total_found = 0usize;
        for key in &keys {
            let shard_idx = crate::shard::dispatch::key_to_shard(key.as_bytes(), N_SHARDS as usize);
            let db = &mut shard_dbs[shard_idx][0];
            if db.get(key.as_bytes()).is_some() {
                total_found += 1;
            }
        }
        assert_eq!(
            total_found, 12,
            "all 12 keys must be retrievable from their routing shard after replay"
        );
    }

    /// Round-trip test (RDB preamble source): migrate keys stored in an RDB
    /// preamble (simulating a BGREWRITEAOF'd AOF), then call replay_per_shard
    /// and assert all keys survive.
    ///
    /// This is the critical test the advisor identified: without preamble
    /// partitioning, this test would lose all keys (reporting 0 recovered).
    #[test]
    fn migrate_aof_round_trips_rdb_preamble_through_replay_per_shard() {
        use crate::persistence::aof_manifest::replay_per_shard;
        use crate::persistence::replay::DispatchReplayEngine;
        use crate::storage::entry::Entry;

        let src_dir = tempfile::tempdir().unwrap();
        let dst_dir = tempfile::tempdir().unwrap();
        const N_SHARDS: u16 = 4;
        const N_KEYS: usize = 20;

        // Build a source database with N_KEYS string entries.
        let mut source_db: Vec<Database> = vec![Database::new()];
        let keys: Vec<String> = (0..N_KEYS).map(|i| format!("rdb_key:{i}")).collect();
        for key in &keys {
            let entry = Entry::new_string(Bytes::copy_from_slice(format!("val_{key}").as_bytes()));
            source_db[0].set(Bytes::copy_from_slice(key.as_bytes()), entry);
        }

        // Serialize to RDB preamble bytes, then write as appendonly.aof.
        let rdb_bytes =
            crate::persistence::rdb::save_to_bytes(&source_db).expect("RDB serialize succeeds");
        // No RESP tail — this simulates a fully-compacted AOF.
        std::fs::write(src_dir.path().join("appendonly.aof"), &rdb_bytes)
            .expect("write source aof with RDB preamble");

        let result =
            migrate_aof(src_dir.path(), dst_dir.path(), N_SHARDS).expect("migration succeeds");
        assert_eq!(
            result.rdb_keys_migrated, N_KEYS,
            "all {N_KEYS} RDB keys must be partitioned"
        );
        assert_eq!(result.commands_written, 0, "no RESP tail commands");

        let manifest = AofManifest::load(dst_dir.path())
            .expect("load ok")
            .expect("manifest present");

        let mut shard_dbs: Vec<Vec<Database>> =
            (0..N_SHARDS).map(|_| vec![Database::new()]).collect();
        let mut slices: Vec<&mut [Database]> =
            shard_dbs.iter_mut().map(|v| v.as_mut_slice()).collect();

        let (total_replayed, _max_lsn, ordered) = replay_per_shard(
            &mut slices,
            &manifest,
            &(|| {
                Box::new(DispatchReplayEngine::new())
                    as Box<dyn crate::persistence::replay::CommandReplayEngine + Send>
            }),
        )
        .expect("replay_per_shard must succeed on migrated layout");

        // replay_per_shard counts RDB keys loaded from the base RDB.
        assert_eq!(
            total_replayed, N_KEYS,
            "all {N_KEYS} RDB-preamble keys must be recovered after migration"
        );
        assert!(ordered.is_empty());

        // Verify each key is in the correct shard.
        let mut total_found = 0usize;
        for key in &keys {
            let shard_idx = crate::shard::dispatch::key_to_shard(key.as_bytes(), N_SHARDS as usize);
            let db = &mut shard_dbs[shard_idx][0];
            if db.get(key.as_bytes()).is_some() {
                total_found += 1;
            }
        }
        assert_eq!(
            total_found, N_KEYS,
            "all {N_KEYS} keys must be retrievable from correct shard after RDB preamble migration"
        );
    }
}
