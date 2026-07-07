//! WAL v3 replay engine — v3 is the only WAL format; auto-detection
//! distinguishes it from raw AOF and rejects the removed v2 format.
//!
//! The replay engine is the recovery path after crash or restart. It handles:
//! - v3 WAL files (version byte=3) with per-record LSN tracking
//! - Raw RESP (v1 AOF) by delegating to AOF replay
//! - v2 WAL files (version byte=2) are rejected loudly: WAL v2 was removed in
//!   the pre-1.0 WAL-v3-only format freeze (see `WalError::UnsupportedVersion`)
//! - Auto-detection at byte offset 6 to distinguish formats
//!
//! FPI (Full Page Image) records during replay unconditionally overwrite the
//! target page — this is the torn-page defense mechanism.
//! Corrupted records stop replay gracefully, returning commands replayed so far.

use std::path::Path;

use super::record::{
    WalRecord, WalRecordType, decode_graph_temporal, decode_temporal_upsert, read_wal_v3_record,
};
use super::segment::{WAL_V3_HEADER_SIZE, WAL_V3_MAGIC, WAL_V3_VERSION};

/// Result of a WAL v3 replay operation.
#[derive(Debug, Clone, Default)]
pub struct WalV3ReplayResult {
    /// Number of command records replayed.
    pub commands_replayed: usize,
    /// LSN of the last record processed.
    pub last_lsn: u64,
    /// Number of FPI records applied.
    pub fpi_applied: usize,
}

/// Auto-detect WAL format and replay accordingly.
///
/// Reads the first bytes of the file to determine the format:
/// - `RRDWAL` magic + version=3 => use v3 replay engine
/// - No `RRDWAL` magic => delegate to AOF (raw RESP v1) replay
/// - `RRDWAL` magic + version=2 => return `UnsupportedVersion` (WAL v2 was
///   removed in the pre-1.0 WAL-v3-only format freeze; re-ingest via AOF)
/// - Any other version => return `UnsupportedVersion` error
pub fn replay_wal_auto(
    databases: &mut [crate::storage::Database],
    path: &Path,
    engine: &dyn crate::persistence::replay::CommandReplayEngine,
) -> Result<usize, crate::error::MoonError> {
    let data = std::fs::read(path)?;
    if data.is_empty() {
        return Ok(0);
    }

    // Check for RRDWAL magic at bytes [0..6]
    if data.len() >= WAL_V3_HEADER_SIZE && data[..6] == *WAL_V3_MAGIC {
        match data[6] {
            2 => {
                // WAL v2 was removed in the pre-1.0 WAL-v3-only format
                // freeze -- this build cannot replay it. Fail loudly rather
                // than silently dropping writes; the operator must re-ingest
                // via the AOF (the recovery authority) or re-import from a
                // pre-freeze build.
                tracing::error!(
                    "WAL v2 file {:?} found but WAL v2 support was removed \
                     (pre-1.0 WAL-v3-only format freeze) — re-ingest via AOF \
                     or replay it on a pre-freeze Moon build first",
                    path
                );
                Err(crate::error::WalError::UnsupportedVersion { version: 2 }.into())
            }
            3 => {
                // v3 format — replay commands through engine
                let mut commands_replayed = 0usize;
                let mut selected_db = 0usize;
                let on_command = &mut |record: &WalRecord| {
                    match record.record_type {
                        WalRecordType::Command => {
                            // Parse RESP from payload and dispatch
                            engine.replay_command(
                                databases,
                                &record.payload,
                                &[],
                                &mut selected_db,
                            );
                        }
                        WalRecordType::XactBegin => {
                            // XactBegin: payload contains txn_id (u64 LE)
                            // No action needed - commit or abort will follow
                            tracing::trace!(lsn = record.lsn, "WAL replay: XactBegin");
                        }
                        WalRecordType::XactCommit => {
                            // XactCommit: db-index-aware KV replay
                            replay_xact_commit(databases, &record.payload);
                            tracing::trace!(lsn = record.lsn, "WAL replay: XactCommit");
                        }
                        WalRecordType::XactAbort => {
                            // XactAbort: no action - changes were never committed
                            tracing::trace!(lsn = record.lsn, "WAL replay: XactAbort");
                        }
                        WalRecordType::TemporalUpsert => {
                            // Decode payload and log for temporal KV index restoration.
                            // Full state restoration happens in ShardDatabases::replay_temporal_wal
                            // (TemporalKvIndex lives on ShardDatabases, not on databases: &mut [Database]).
                            if let Some((key, valid_from, _system_from, _value)) =
                                crate::persistence::wal_v3::record::decode_temporal_upsert(
                                    &record.payload,
                                )
                            {
                                tracing::debug!(
                                    lsn = record.lsn,
                                    key_len = key.len(),
                                    valid_from = valid_from,
                                    "WAL replay: TemporalUpsert (deferred to temporal replay path)"
                                );
                            } else {
                                tracing::warn!(
                                    lsn = record.lsn,
                                    "WAL replay: malformed TemporalUpsert payload"
                                );
                            }
                        }
                        WalRecordType::GraphTemporal => {
                            // Decode payload and log for graph entity valid_to restoration.
                            // Full state restoration happens in ShardDatabases::replay_temporal_wal
                            // (GraphStore lives on ShardDatabases, not on databases: &mut [Database]).
                            if let Some((entity_id, is_node, valid_to, _system_from)) =
                                crate::persistence::wal_v3::record::decode_graph_temporal(
                                    &record.payload,
                                )
                            {
                                tracing::debug!(
                                    lsn = record.lsn,
                                    entity_id = entity_id,
                                    is_node = is_node,
                                    valid_to = valid_to,
                                    "WAL replay: GraphTemporal (deferred to temporal replay path)"
                                );
                            } else {
                                tracing::warn!(
                                    lsn = record.lsn,
                                    "WAL replay: malformed GraphTemporal payload"
                                );
                            }
                        }
                        _ => {
                            // Other record types (Vector*, Checkpoint, etc.)
                        }
                    }
                    commands_replayed += 1;
                };
                let on_fpi = &mut |_record: &WalRecord| {
                    // FPI unconditionally overwrites — handled by caller in full recovery
                };
                let result = replay_wal_v3_file(path, 0, on_command, on_fpi)
                    .map_err(|e| crate::error::MoonError::Io(e))?;
                let _ = result;
                Ok(commands_replayed)
            }
            other => Err(crate::error::WalError::UnsupportedVersion {
                version: other as u32,
            }
            .into()),
        }
    } else {
        // No magic — v1 raw RESP, delegate to AOF replay
        crate::persistence::aof::replay_aof(databases, path, engine)
    }
}

/// Replay every Command/XactCommit record across all segments in a WAL v3
/// directory through `engine`, applying the same record-type dispatch as
/// `replay_wal_auto`'s v3 branch (this function is the multi-segment,
/// directory-scoped counterpart — `replay_wal_auto` only handles one file).
///
/// Used by legacy (non-disk-offload) shard recovery: WAL v3 is written even
/// when `--disk-offload` is off (rooted at `<persistence_dir>/shard-N/wal-v3`,
/// see `event_loop::run`'s writer-creation block), fsynced on the same 1s
/// cadence as the retired WAL v2 writer was. To preserve the pre-1.0 "no
/// durable write lost on restart" property that WAL v2's
/// prefer-WAL-fall-back-to-AOF contract gave, callers should treat a non-zero
/// return here as authoritative (skip AOF) and only replay the AOF when this
/// returns `Ok(0)` or `Err` — exactly mirroring how the disk-offload v3
/// recovery protocol already treats its own primary WAL replay relative to
/// its AOF fallback (see `recover_shard_v3_pitr`).
///
/// Returns `Ok(0)` (not an error) when `wal_dir` does not exist or is empty,
/// so callers can use the return value directly as an "authoritative source
/// available?" signal.
pub fn replay_wal_v3_dir_commands(
    wal_dir: &Path,
    databases: &mut [crate::storage::Database],
    engine: &dyn crate::persistence::replay::CommandReplayEngine,
) -> std::io::Result<usize> {
    if !wal_dir.exists() {
        return Ok(0);
    }

    let mut commands_replayed = 0usize;
    let mut selected_db = 0usize;
    let on_command = &mut |record: &WalRecord| {
        match record.record_type {
            WalRecordType::Command => {
                engine.replay_command(databases, &record.payload, &[], &mut selected_db);
            }
            WalRecordType::XactBegin => {
                tracing::trace!(lsn = record.lsn, "WAL replay: XactBegin");
            }
            WalRecordType::XactCommit => {
                replay_xact_commit(databases, &record.payload);
                tracing::trace!(lsn = record.lsn, "WAL replay: XactCommit");
            }
            WalRecordType::XactAbort => {
                tracing::trace!(lsn = record.lsn, "WAL replay: XactAbort");
            }
            WalRecordType::TemporalUpsert => {
                if decode_temporal_upsert(&record.payload).is_none() {
                    tracing::warn!(
                        lsn = record.lsn,
                        "WAL replay: malformed TemporalUpsert payload"
                    );
                }
                // Full state restoration happens in the temporal replay path
                // (TemporalKvIndex lives on ShardDatabases, not on databases).
            }
            WalRecordType::GraphTemporal => {
                if decode_graph_temporal(&record.payload).is_none() {
                    tracing::warn!(
                        lsn = record.lsn,
                        "WAL replay: malformed GraphTemporal payload"
                    );
                }
                // Full state restoration happens in the graph replay path
                // (GraphStore lives on ShardDatabases, not on databases).
            }
            _ => {
                // Other record types (Vector*, Checkpoint, etc.) are not
                // meaningful for the legacy KV-only replay path.
            }
        }
        commands_replayed += 1;
    };
    let on_fpi = &mut |_record: &WalRecord| {
        // No page cache in legacy (non-disk-offload) mode; FPI records are
        // never written there (WalWriterV3 is created without a page-cache
        // handle), so this is unreachable in practice but kept for symmetry.
    };

    replay_wal_v3_dir(wal_dir, 0, on_command, on_fpi)?;
    Ok(commands_replayed)
}

/// Replay all WAL v3 segment files in a directory.
///
/// Scans `wal_dir` for `*.wal` files, sorts by filename (zero-padded sequence
/// ensures lexicographic = numeric order), and replays each segment in order.
/// Records with `lsn <= redo_lsn` are skipped (already applied).
pub fn replay_wal_v3_dir(
    wal_dir: &Path,
    redo_lsn: u64,
    on_command: &mut dyn FnMut(&WalRecord),
    on_fpi: &mut dyn FnMut(&WalRecord),
) -> std::io::Result<WalV3ReplayResult> {
    replay_wal_v3_dir_until(wal_dir, redo_lsn, None, on_command, on_fpi)
}

/// PITR variant of `replay_wal_v3_dir`: stop replaying once a record with
/// `lsn > stop_at_lsn` is observed. `stop_at_lsn = None` reproduces the
/// classic "replay everything past redo_lsn" behavior.
///
/// Used by recovery when `--recovery-target-lsn` is set. Returns the same
/// `WalV3ReplayResult` shape; `last_lsn` reflects the highest LSN actually
/// applied (which is `<= stop_at_lsn`).
pub fn replay_wal_v3_dir_until(
    wal_dir: &Path,
    redo_lsn: u64,
    stop_at_lsn: Option<u64>,
    on_command: &mut dyn FnMut(&WalRecord),
    on_fpi: &mut dyn FnMut(&WalRecord),
) -> std::io::Result<WalV3ReplayResult> {
    let mut segments: Vec<_> = std::fs::read_dir(wal_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_str().is_some_and(|n| n.ends_with(".wal")))
        .map(|e| e.path())
        .collect();

    // Sort by filename (zero-padded sequence ensures correct order)
    segments.sort();

    let mut combined = WalV3ReplayResult::default();
    for seg_path in &segments {
        let result = replay_wal_v3_file_until(seg_path, redo_lsn, stop_at_lsn, on_command, on_fpi)?;
        combined.commands_replayed += result.commands_replayed;
        combined.fpi_applied += result.fpi_applied;
        if result.last_lsn > combined.last_lsn {
            combined.last_lsn = result.last_lsn;
        }
        // Early exit: once a segment indicates we crossed the stop boundary,
        // later segments only contain higher LSNs (segments are monotonic).
        if let Some(target) = stop_at_lsn {
            if combined.last_lsn >= target {
                break;
            }
        }
    }
    Ok(combined)
}

/// Replay a single WAL v3 segment file.
///
/// Reads the file, verifies the v3 header, then iterates records starting at
/// offset 64 (after header). For each record:
/// - Skip if `record.lsn <= redo_lsn` (already applied)
/// - Command/Vector*/File* records => `on_command` callback
/// - FullPageImage records => `on_fpi` callback (unconditional overwrite)
/// - Checkpoint records => tracked but not dispatched
/// - On corrupt/truncated record (read_wal_v3_record returns None): stop, return so far
pub fn replay_wal_v3_file(
    path: &Path,
    redo_lsn: u64,
    on_command: &mut dyn FnMut(&WalRecord),
    on_fpi: &mut dyn FnMut(&WalRecord),
) -> std::io::Result<WalV3ReplayResult> {
    replay_wal_v3_file_until(path, redo_lsn, None, on_command, on_fpi)
}

/// PITR variant of `replay_wal_v3_file`: stops once `record.lsn > stop_at_lsn`.
/// See `replay_wal_v3_dir_until` for the dir-level entry point.
pub fn replay_wal_v3_file_until(
    path: &Path,
    redo_lsn: u64,
    stop_at_lsn: Option<u64>,
    on_command: &mut dyn FnMut(&WalRecord),
    on_fpi: &mut dyn FnMut(&WalRecord),
) -> std::io::Result<WalV3ReplayResult> {
    let data = std::fs::read(path)?;

    if data.len() < WAL_V3_HEADER_SIZE {
        return Ok(WalV3ReplayResult::default());
    }

    // Verify v3 header
    if &data[..6] != WAL_V3_MAGIC || data[6] != WAL_V3_VERSION {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "not a WAL v3 segment",
        ));
    }

    let mut result = WalV3ReplayResult::default();
    let mut offset = WAL_V3_HEADER_SIZE;

    while offset < data.len() {
        // Need at least 4 bytes for record_len
        if offset + 4 > data.len() {
            break;
        }

        let record = match read_wal_v3_record(&data[offset..]) {
            Some(r) => r,
            None => {
                // Corrupt or truncated — stop replay, return what we have
                tracing::warn!(
                    "WAL v3 replay: corrupt/truncated record at offset {}, stopping",
                    offset
                );
                break;
            }
        };

        // Advance offset by record_len
        let record_len = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += record_len;

        // PITR stop: once a record's LSN exceeds the target, halt without
        // applying it. We do NOT bump `last_lsn` past the cutoff -- callers
        // use `last_lsn` as "highest applied LSN" for restoring control file
        // state, and bumping it would falsely advance wal_flush_lsn.
        if let Some(target) = stop_at_lsn {
            if record.lsn > target {
                break;
            }
        }

        // Track last LSN seen
        if record.lsn > result.last_lsn {
            result.last_lsn = record.lsn;
        }

        // Skip records already applied
        if record.lsn <= redo_lsn {
            continue;
        }

        match record.record_type {
            WalRecordType::Command
            | WalRecordType::VectorUpsert
            | WalRecordType::VectorDelete
            | WalRecordType::VectorTxnCommit
            | WalRecordType::VectorTxnAbort
            | WalRecordType::VectorCheckpoint
            | WalRecordType::FileCreate
            | WalRecordType::FileDelete
            | WalRecordType::FileTierChange
            | WalRecordType::XactBegin
            | WalRecordType::XactCommit
            | WalRecordType::XactAbort
            | WalRecordType::TemporalUpsert
            | WalRecordType::GraphTemporal
            | WalRecordType::WorkspaceCreate
            | WalRecordType::WorkspaceDrop
            | WalRecordType::MqCreate
            | WalRecordType::MqAck => {
                on_command(&record);
                result.commands_replayed += 1;
            }
            WalRecordType::FullPageImage => {
                on_fpi(&record);
                result.fpi_applied += 1;
            }
            WalRecordType::Checkpoint => {
                // Checkpoint marker — tracked but not dispatched
            }
        }
    }

    Ok(result)
}

/// Resolve a wall-clock instant to the LSN at which to stop WAL replay.
///
/// Walks all segments in `wal_dir` newest-LSN-last, examining only records
/// that carry a timestamp:
///   * `TemporalUpsert` -> uses `system_from` (decoded from payload)
///   * `GraphTemporal`  -> uses `system_from` (decoded from payload)
///
/// Returns the highest LSN whose timestamp is `<= target_unix_ms`. If no
/// timestamped record exists (or all are after `target_unix_ms`), returns
/// `None`. Callers should fall back to `--recovery-target-lsn` with an
/// operator-supplied value when this resolver yields `None`.
///
/// **Coverage limitation.** Today only temporal commands stamp `system_from`
/// into the WAL. Workloads without temporal ops have no time anchors and
/// must use `--recovery-target-lsn` directly. A future commit can add a
/// periodic `TimestampMarker` record to give every WAL implicit time
/// anchors; for v0.2 this honest limitation is acceptable.
pub fn resolve_target_time_to_lsn(
    wal_dir: &Path,
    target_unix_ms: i64,
) -> std::io::Result<Option<u64>> {
    let mut segments: Vec<_> = std::fs::read_dir(wal_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_str().is_some_and(|n| n.ends_with(".wal")))
        .map(|e| e.path())
        .collect();
    segments.sort();

    let mut best: Option<u64> = None;

    for seg_path in &segments {
        let data = std::fs::read(seg_path)?;
        if data.len() < WAL_V3_HEADER_SIZE {
            continue;
        }
        if &data[..6] != WAL_V3_MAGIC || data[6] != WAL_V3_VERSION {
            // Non-v3 segment — skip rather than fail; the caller may be
            // mid-format-migration and we should not block PITR on that.
            continue;
        }

        let mut offset = WAL_V3_HEADER_SIZE;
        while offset + 4 <= data.len() {
            let record_len = u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            if record_len == 0 || offset + record_len > data.len() {
                break;
            }
            let record = match read_wal_v3_record(&data[offset..offset + record_len]) {
                Some(r) => r,
                None => break,
            };
            offset += record_len;

            let ts: Option<i64> = match record.record_type {
                WalRecordType::TemporalUpsert => decode_temporal_upsert(&record.payload)
                    .map(|(_, _, system_from, _)| system_from),
                WalRecordType::GraphTemporal => {
                    decode_graph_temporal(&record.payload).map(|(_, _, _, system_from)| system_from)
                }
                _ => None,
            };
            if let Some(t) = ts {
                if t <= target_unix_ms {
                    // Record is at or before the target — eligible.
                    if best.map(|b| record.lsn > b).unwrap_or(true) {
                        best = Some(record.lsn);
                    }
                } else {
                    // First timestamp past the target -> no later record in
                    // this or subsequent segments can be earlier (WAL is
                    // append-only and monotonic per shard). Stop scanning.
                    return Ok(best);
                }
            }
        }
    }
    Ok(best)
}

/// Replay a cross-store transaction commit record (db-index-aware).
///
/// Payload layout (little-endian): `txn_id: u64, db_index: u32,
/// kv_op_count: u32, ops…`. Per op:
///   - op_type: u8 (0=SET, 1=DEL)
///   - key_len: u32
///   - key: [u8; key_len]
///   - value_len: u32 (only for SET)
///   - value: [u8; value_len] (only for SET)
///
/// An out-of-range `db_index` (server restarted with fewer `--databases`)
/// falls back to db 0 with a loud warning rather than dropping the ops.
///
/// Vector and graph ops are handled by their respective replay paths
/// (VectorTxnCommit already exists).
fn replay_xact_commit(databases: &mut [crate::storage::Database], payload: &[u8]) {
    if payload.len() < 16 {
        tracing::warn!("XactCommit payload too short: {} bytes", payload.len());
        return;
    }

    let txn_id = u64::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
        payload[7],
    ]);
    let db_index = u32::from_le_bytes([payload[8], payload[9], payload[10], payload[11]]) as usize;
    let kv_op_count =
        u32::from_le_bytes([payload[12], payload[13], payload[14], payload[15]]) as usize;

    tracing::debug!(txn_id, db_index, kv_op_count, "Replaying XactCommit");

    let target = if db_index < databases.len() {
        db_index
    } else {
        tracing::warn!(
            txn_id,
            db_index,
            db_count = databases.len(),
            "XactCommit db index out of range (server restarted with fewer \
             --databases?) — replaying into db 0"
        );
        0
    };
    replay_xact_kv_ops(&mut databases[target], payload, 16, kv_op_count);
}

/// Shared op-loop for XactCommit: apply `kv_op_count` SET/DEL ops read
/// from `payload` starting at `offset` to `db`. Truncation-defensive — a
/// short payload warns and stops rather than panicking.
fn replay_xact_kv_ops(
    db: &mut crate::storage::Database,
    payload: &[u8],
    mut offset: usize,
    kv_op_count: usize,
) {
    for _ in 0..kv_op_count {
        if offset >= payload.len() {
            tracing::warn!("XactCommit payload truncated at op boundary");
            break;
        }

        let op_type = payload[offset];
        offset += 1;

        if offset + 4 > payload.len() {
            tracing::warn!("XactCommit payload truncated reading key_len");
            break;
        }
        let key_len = u32::from_le_bytes([
            payload[offset],
            payload[offset + 1],
            payload[offset + 2],
            payload[offset + 3],
        ]) as usize;
        offset += 4;

        if offset + key_len > payload.len() {
            tracing::warn!("XactCommit payload truncated reading key");
            break;
        }
        let key = bytes::Bytes::copy_from_slice(&payload[offset..offset + key_len]);
        offset += key_len;

        match op_type {
            0 => {
                // SET
                if offset + 4 > payload.len() {
                    tracing::warn!("XactCommit payload truncated reading value_len");
                    break;
                }
                let value_len = u32::from_le_bytes([
                    payload[offset],
                    payload[offset + 1],
                    payload[offset + 2],
                    payload[offset + 3],
                ]) as usize;
                offset += 4;

                if offset + value_len > payload.len() {
                    tracing::warn!("XactCommit payload truncated reading value");
                    break;
                }
                let value = bytes::Bytes::copy_from_slice(&payload[offset..offset + value_len]);
                offset += value_len;

                db.set_string(key, value);
            }
            1 => {
                // DEL
                db.remove(&key);
            }
            _ => {
                tracing::warn!(op_type, "Unknown KV op type in XactCommit");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::record::write_wal_v3_record;
    use super::super::segment::WAL_V3_HEADER_SIZE;
    use super::*;

    /// Build a minimal v3 segment header.
    fn make_v3_header(shard_id: u16) -> Vec<u8> {
        let mut header = vec![0u8; WAL_V3_HEADER_SIZE];
        header[0..6].copy_from_slice(b"RRDWAL");
        header[6] = 3; // version = 3
        header[7] = 0x01; // flags = FPI_ENABLED
        header[8..10].copy_from_slice(&shard_id.to_le_bytes());
        header
    }

    /// Build a minimal v2 header (32 bytes).
    fn make_v2_header(shard_id: u16) -> Vec<u8> {
        let mut header = vec![0u8; 32];
        header[0..6].copy_from_slice(b"RRDWAL");
        header[6] = 2; // version = 2
        header[7..9].copy_from_slice(&shard_id.to_le_bytes());
        header
    }

    // D-1: XactCommit must replay KV ops into the db the transaction ran
    // in — the deleted pre-1.0 v1 record hardcoded db 0, silently corrupting
    // recovery for any TXN.BEGIN…COMMIT issued under SELECT != 0.
    #[test]
    fn test_xact_commit_replays_into_selected_db() {
        use crate::persistence::wal_v3::record::encode_xact_commit_payload;
        use crate::storage::Database;
        use crate::transaction::UndoRecord;
        use bytes::Bytes;

        // Committed state lives in db 3.
        let mut src_db = Database::new();
        src_db.set_string(Bytes::from_static(b"txnkey"), Bytes::from_static(b"v3val"));
        let records = vec![UndoRecord::Insert {
            key: Bytes::from_static(b"txnkey"),
        }];
        let payload = encode_xact_commit_payload(7, 3, &records, &src_db);

        let mut databases: Vec<Database> = (0..4).map(|_| Database::new()).collect();
        replay_xact_commit(&mut databases, &payload);

        assert!(
            databases[3].data().get(b"txnkey".as_ref()).is_some(),
            "XactCommit must restore the key into db 3"
        );
        assert!(
            databases[0].data().get(b"txnkey".as_ref()).is_none(),
            "db 0 must NOT receive a db-3 transaction's keys"
        );
    }

    #[test]
    fn test_xact_commit_out_of_range_db_falls_back_to_db0() {
        use crate::persistence::wal_v3::record::encode_xact_commit_payload;
        use crate::storage::Database;
        use crate::transaction::UndoRecord;
        use bytes::Bytes;

        let mut src_db = Database::new();
        src_db.set_string(Bytes::from_static(b"k"), Bytes::from_static(b"v"));
        let records = vec![UndoRecord::Insert {
            key: Bytes::from_static(b"k"),
        }];
        // db_index 9 but the restarted server only has 2 dbs.
        let payload = encode_xact_commit_payload(8, 9, &records, &src_db);

        let mut databases: Vec<Database> = (0..2).map(|_| Database::new()).collect();
        replay_xact_commit(&mut databases, &payload);

        assert!(
            databases[0].data().get(b"k".as_ref()).is_some(),
            "out-of-range db index must fall back to db 0, not drop the ops"
        );
    }

    #[test]
    fn test_v3_replay_commands() {
        let tmp = tempfile::tempdir().unwrap();
        let seg_path = tmp.path().join("000000000001.wal");

        // Build segment: header + 5 command records
        let mut data = make_v3_header(0);
        for i in 1..=5u64 {
            write_wal_v3_record(&mut data, i, WalRecordType::Command, b"SET k v");
        }
        std::fs::write(&seg_path, &data).unwrap();

        let mut cmd_count = 0usize;
        let mut fpi_count = 0usize;
        let result = replay_wal_v3_file(&seg_path, 0, &mut |_| cmd_count += 1, &mut |_| {
            fpi_count += 1
        })
        .unwrap();

        assert_eq!(result.commands_replayed, 5);
        assert_eq!(cmd_count, 5);
        assert_eq!(result.fpi_applied, 0);
        assert_eq!(fpi_count, 0);
        assert_eq!(result.last_lsn, 5);
    }

    #[test]
    fn test_v3_replay_fpi() {
        let tmp = tempfile::tempdir().unwrap();
        let seg_path = tmp.path().join("000000000001.wal");

        let mut data = make_v3_header(0);
        // 1 command + 1 FPI
        write_wal_v3_record(&mut data, 1, WalRecordType::Command, b"SET a 1");
        write_wal_v3_record(&mut data, 2, WalRecordType::FullPageImage, &[0xABu8; 128]);
        std::fs::write(&seg_path, &data).unwrap();

        let mut fpi_count = 0usize;
        let result =
            replay_wal_v3_file(&seg_path, 0, &mut |_| {}, &mut |_| fpi_count += 1).unwrap();

        assert_eq!(result.commands_replayed, 1);
        assert_eq!(result.fpi_applied, 1);
        assert_eq!(fpi_count, 1);
    }

    #[test]
    fn test_v3_replay_corrupt_stops() {
        let tmp = tempfile::tempdir().unwrap();
        let seg_path = tmp.path().join("000000000001.wal");

        let mut data = make_v3_header(0);
        // Write 2 good records
        write_wal_v3_record(&mut data, 1, WalRecordType::Command, b"SET a 1");
        write_wal_v3_record(&mut data, 2, WalRecordType::Command, b"SET b 2");
        let corrupt_offset = data.len();
        // Write 3rd record then corrupt its CRC
        write_wal_v3_record(&mut data, 3, WalRecordType::Command, b"SET c 3");
        // Corrupt a byte in the 3rd record's payload area
        data[corrupt_offset + 16] ^= 0xFF;

        std::fs::write(&seg_path, &data).unwrap();

        let mut cmd_count = 0usize;
        let result =
            replay_wal_v3_file(&seg_path, 0, &mut |_| cmd_count += 1, &mut |_| {}).unwrap();

        // Only first 2 records should have replayed
        assert_eq!(result.commands_replayed, 2);
        assert_eq!(cmd_count, 2);
        assert_eq!(result.last_lsn, 2);
    }

    #[test]
    fn test_v3_replay_skips_below_redo_lsn() {
        let tmp = tempfile::tempdir().unwrap();
        let seg_path = tmp.path().join("000000000001.wal");

        let mut data = make_v3_header(0);
        for i in 1..=5u64 {
            write_wal_v3_record(&mut data, i, WalRecordType::Command, b"SET k v");
        }
        std::fs::write(&seg_path, &data).unwrap();

        let mut replayed_lsns = Vec::new();
        let result = replay_wal_v3_file(
            &seg_path,
            3, // redo_lsn=3 => skip LSNs 1, 2, 3
            &mut |r| replayed_lsns.push(r.lsn),
            &mut |_| {},
        )
        .unwrap();

        assert_eq!(result.commands_replayed, 2); // only LSN 4, 5
        assert_eq!(replayed_lsns, vec![4, 5]);
        assert_eq!(result.last_lsn, 5); // last_lsn tracks all records seen
    }

    #[test]
    fn test_v3_replay_multi_segment() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        // Segment 1: LSNs 1-3
        let mut data1 = make_v3_header(0);
        for i in 1..=3u64 {
            write_wal_v3_record(&mut data1, i, WalRecordType::Command, b"SET a 1");
        }
        std::fs::write(wal_dir.join("000000000001.wal"), &data1).unwrap();

        // Segment 2: LSNs 4-6
        let mut data2 = make_v3_header(0);
        for i in 4..=6u64 {
            write_wal_v3_record(&mut data2, i, WalRecordType::Command, b"SET b 2");
        }
        std::fs::write(wal_dir.join("000000000002.wal"), &data2).unwrap();

        let mut cmd_count = 0usize;
        let result = replay_wal_v3_dir(&wal_dir, 0, &mut |_| cmd_count += 1, &mut |_| {}).unwrap();

        assert_eq!(result.commands_replayed, 6);
        assert_eq!(cmd_count, 6);
        assert_eq!(result.last_lsn, 6);
    }

    #[test]
    fn test_v3_replay_checkpoint_not_dispatched() {
        let tmp = tempfile::tempdir().unwrap();
        let seg_path = tmp.path().join("000000000001.wal");

        let mut data = make_v3_header(0);
        write_wal_v3_record(&mut data, 1, WalRecordType::Command, b"SET a 1");
        write_wal_v3_record(&mut data, 2, WalRecordType::Checkpoint, b"");
        write_wal_v3_record(&mut data, 3, WalRecordType::Command, b"SET b 2");
        std::fs::write(&seg_path, &data).unwrap();

        let mut cmd_count = 0usize;
        let mut fpi_count = 0usize;
        let result = replay_wal_v3_file(&seg_path, 0, &mut |_| cmd_count += 1, &mut |_| {
            fpi_count += 1
        })
        .unwrap();

        // Checkpoint should NOT be dispatched to either callback
        assert_eq!(result.commands_replayed, 2);
        assert_eq!(cmd_count, 2);
        assert_eq!(result.fpi_applied, 0);
        assert_eq!(fpi_count, 0);
        assert_eq!(result.last_lsn, 3);
    }

    /// P3 — replay must stop at `stop_at_lsn`: records strictly above the
    /// target are not dispatched, and `last_lsn` reflects only the records
    /// actually applied. This is the core PITR primitive.
    #[test]
    fn test_v3_replay_stops_at_target_lsn() {
        let tmp = tempfile::tempdir().unwrap();
        let seg_path = tmp.path().join("000000000001.wal");

        // 10 command records, LSN 1..=10.
        let mut data = make_v3_header(0);
        for i in 1..=10u64 {
            write_wal_v3_record(&mut data, i, WalRecordType::Command, b"SET k v");
        }
        std::fs::write(&seg_path, &data).unwrap();

        let mut applied: Vec<u64> = Vec::new();
        let result = replay_wal_v3_file_until(
            &seg_path,
            0,
            Some(5),
            &mut |r| applied.push(r.lsn),
            &mut |_| {},
        )
        .unwrap();

        assert_eq!(result.commands_replayed, 5, "should apply LSN 1..=5 only");
        assert_eq!(applied, vec![1, 2, 3, 4, 5]);
        assert_eq!(
            result.last_lsn, 5,
            "last_lsn must not advance past the target (used to restore wal_flush_lsn)",
        );
    }

    /// P3 — multi-segment replay must stop at the cutoff even when the
    /// target LSN lives mid-segment.
    #[test]
    fn test_v3_replay_dir_stops_at_target_lsn() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        // Segment 1: LSNs 1..=3
        let mut data1 = make_v3_header(0);
        for i in 1..=3u64 {
            write_wal_v3_record(&mut data1, i, WalRecordType::Command, b"SET a 1");
        }
        std::fs::write(wal_dir.join("000000000001.wal"), &data1).unwrap();
        // Segment 2: LSNs 4..=8
        let mut data2 = make_v3_header(0);
        for i in 4..=8u64 {
            write_wal_v3_record(&mut data2, i, WalRecordType::Command, b"SET b 2");
        }
        std::fs::write(wal_dir.join("000000000002.wal"), &data2).unwrap();

        let mut cmd_count = 0usize;
        let result =
            replay_wal_v3_dir_until(&wal_dir, 0, Some(5), &mut |_| cmd_count += 1, &mut |_| {})
                .unwrap();
        assert_eq!(
            result.commands_replayed, 5,
            "should apply LSN 1..=5 across both segments"
        );
        assert_eq!(cmd_count, 5);
        assert_eq!(result.last_lsn, 5);
    }

    /// P3 — `stop_at_lsn = None` must reproduce the classic replay behavior
    /// so the new code path doesn't regress existing recovery callers.
    #[test]
    fn test_v3_replay_no_stop_matches_classic() {
        let tmp = tempfile::tempdir().unwrap();
        let seg_path = tmp.path().join("000000000001.wal");
        let mut data = make_v3_header(0);
        for i in 1..=4u64 {
            write_wal_v3_record(&mut data, i, WalRecordType::Command, b"SET k v");
        }
        std::fs::write(&seg_path, &data).unwrap();

        let classic = replay_wal_v3_file(&seg_path, 0, &mut |_| {}, &mut |_| {}).unwrap();
        let same = replay_wal_v3_file_until(&seg_path, 0, None, &mut |_| {}, &mut |_| {}).unwrap();
        assert_eq!(classic.commands_replayed, same.commands_replayed);
        assert_eq!(classic.last_lsn, same.last_lsn);
        assert_eq!(classic.fpi_applied, same.fpi_applied);
    }

    /// P3 — `resolve_target_time_to_lsn` walks TemporalUpsert + GraphTemporal
    /// records and returns the highest LSN whose timestamp is <= target.
    #[test]
    fn test_resolve_target_time_to_lsn() {
        use super::super::record::{encode_graph_temporal, encode_temporal_upsert};

        let tmp = tempfile::tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        // Build one segment with mixed records, each carrying explicit system_from.
        let mut data = make_v3_header(0);
        // LSN 1: TemporalUpsert at ts=100
        let p1 = encode_temporal_upsert(b"k1", 100, 100, b"v1");
        write_wal_v3_record(&mut data, 1, WalRecordType::TemporalUpsert, &p1);
        // LSN 2: GraphTemporal at ts=200
        let p2 = encode_graph_temporal(42, true, i64::MAX, 200);
        write_wal_v3_record(&mut data, 2, WalRecordType::GraphTemporal, &p2);
        // LSN 3: plain Command — has no timestamp, ignored by the resolver
        write_wal_v3_record(&mut data, 3, WalRecordType::Command, b"SET a 1");
        // LSN 4: TemporalUpsert at ts=400
        let p4 = encode_temporal_upsert(b"k4", 400, 400, b"v4");
        write_wal_v3_record(&mut data, 4, WalRecordType::TemporalUpsert, &p4);
        std::fs::write(wal_dir.join("000000000001.wal"), &data).unwrap();

        // Target ts=250 should land on LSN 2 (the GraphTemporal with ts=200);
        // LSN 4 (ts=400) is past the target.
        let lsn = resolve_target_time_to_lsn(&wal_dir, 250).unwrap();
        assert_eq!(lsn, Some(2));

        // Target ts=500 covers everything that has a timestamp -> LSN 4.
        let lsn = resolve_target_time_to_lsn(&wal_dir, 500).unwrap();
        assert_eq!(lsn, Some(4));

        // Target ts=50 has nothing eligible -> None (caller falls back to
        // --recovery-target-lsn).
        let lsn = resolve_target_time_to_lsn(&wal_dir, 50).unwrap();
        assert_eq!(lsn, None);
    }

    #[test]
    fn test_v3_replay_empty_file() {
        let tmp = tempfile::tempdir().unwrap();
        let seg_path = tmp.path().join("000000000001.wal");

        // Write only header, no records
        let data = make_v3_header(0);
        std::fs::write(&seg_path, &data).unwrap();

        let result = replay_wal_v3_file(&seg_path, 0, &mut |_| {}, &mut |_| {}).unwrap();

        assert_eq!(result.commands_replayed, 0);
        assert_eq!(result.fpi_applied, 0);
        assert_eq!(result.last_lsn, 0);
    }

    #[test]
    fn test_auto_detect_v3() {
        let tmp = tempfile::tempdir().unwrap();
        let seg_path = tmp.path().join("test.wal");

        // Write a valid v3 file with header + records
        let mut data = make_v3_header(0);
        write_wal_v3_record(&mut data, 1, WalRecordType::Command, b"SET a 1");
        write_wal_v3_record(&mut data, 2, WalRecordType::Command, b"SET b 2");
        std::fs::write(&seg_path, &data).unwrap();

        // replay_wal_auto needs databases + engine, which we can't easily mock
        // in unit tests. Instead, verify the auto-detect logic directly.
        let file_data = std::fs::read(&seg_path).unwrap();
        assert_eq!(&file_data[..6], b"RRDWAL");
        assert_eq!(file_data[6], 3); // version = 3
    }

    #[test]
    fn test_auto_detect_v2_header() {
        // Verify that a v2 header is distinguishable
        let header = make_v2_header(0);
        assert_eq!(&header[..6], b"RRDWAL");
        assert_eq!(header[6], 2); // version = 2
    }

    #[test]
    fn test_auto_detect_raw_resp() {
        // Raw RESP starts with '*' (0x2A), not 'R'
        let raw = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n";
        assert_ne!(&raw[..6], b"RRDWAL");
    }

    #[test]
    fn test_v3_replay_vector_records() {
        let tmp = tempfile::tempdir().unwrap();
        let seg_path = tmp.path().join("000000000001.wal");

        let mut data = make_v3_header(0);
        write_wal_v3_record(&mut data, 1, WalRecordType::VectorUpsert, b"vec data");
        write_wal_v3_record(&mut data, 2, WalRecordType::VectorDelete, b"del data");
        write_wal_v3_record(&mut data, 3, WalRecordType::FileCreate, b"file data");
        std::fs::write(&seg_path, &data).unwrap();

        let mut cmd_count = 0usize;
        let result =
            replay_wal_v3_file(&seg_path, 0, &mut |_| cmd_count += 1, &mut |_| {}).unwrap();

        // Vector and File records go through on_command
        assert_eq!(result.commands_replayed, 3);
        assert_eq!(cmd_count, 3);
    }
}
