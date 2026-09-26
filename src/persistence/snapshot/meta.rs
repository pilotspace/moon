//! RRDSHARD header peek (`read_snapshot_metadata`), split out of `snapshot.rs`
//! to keep that file under the 1500-line cap.

use super::*;

/// Peek at a snapshot file's header without fully loading it.
///
/// Returns the version, shard_id, epoch, last_lsn, and created_at_unix_ms.
/// Used by P3 recovery to enumerate available snapshots and pick the one
/// with the highest `last_lsn` that is still `<= target_lsn`.
///
/// Does NOT verify the global CRC32 — that's only meaningful when the full
/// payload is being loaded. Header bytes are integrity-checked by the magic
/// + version validation; corrupt headers return `Corrupted` errors.
pub fn read_snapshot_metadata(path: &Path) -> Result<SnapshotMeta, MoonError> {
    use std::io::Read;
    let mut file = std::fs::File::open(path).map_err(|e| SnapshotError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    // v2 preamble is 35 bytes — read up to that.
    let mut buf = [0u8; 35];
    let n = file.read(&mut buf).map_err(|e| SnapshotError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    if n < 19 {
        return Err(SnapshotError::Corrupted {
            detail: format!("snapshot header truncated: {} bytes", n),
        }
        .into());
    }
    if &buf[0..8] != SHARD_RDB_MAGIC {
        return Err(SnapshotError::Corrupted {
            detail: "invalid RRDSHARD magic header".into(),
        }
        .into());
    }
    let version = buf[8];
    if version != SHARD_RDB_VERSION_V1
        && version != SHARD_RDB_VERSION_V2
        && version != SHARD_RDB_VERSION_V3
    {
        return Err(SnapshotError::VersionMismatch {
            expected: SHARD_RDB_VERSION as u32,
            actual: version as u32,
        }
        .into());
    }
    let shard_id = u16::from_le_bytes([buf[9], buf[10]]);
    let epoch = u64::from_le_bytes([
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18],
    ]);
    // V2 and V3 share the same preamble layout (LSN + timestamp after epoch).
    let (last_lsn, created_at_unix_ms) =
        if version == SHARD_RDB_VERSION_V2 || version == SHARD_RDB_VERSION_V3 {
            if n < 35 {
                return Err(SnapshotError::Corrupted {
                    detail: format!("v2 snapshot header truncated: {} bytes", n),
                }
                .into());
            }
            let lsn = u64::from_le_bytes([
                buf[19], buf[20], buf[21], buf[22], buf[23], buf[24], buf[25], buf[26],
            ]);
            let ts = u64::from_le_bytes([
                buf[27], buf[28], buf[29], buf[30], buf[31], buf[32], buf[33], buf[34],
            ]);
            (lsn, ts)
        } else {
            (0u64, 0u64)
        };
    Ok(SnapshotMeta {
        version,
        shard_id,
        epoch,
        last_lsn,
        created_at_unix_ms,
    })
}
