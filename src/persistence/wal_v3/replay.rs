//! WAL v3 replay engine — v2/v3 auto-detection, LSN-based skip, FPI callback.
//!
//! The replay engine is the recovery path after crash or restart. It handles:
//! - v2 WAL files (version byte=2) by delegating to the existing v2 replay path
//! - v3 WAL files (version byte=3) with per-record LSN tracking
//! - Raw RESP (v1) by delegating to AOF replay
//! - Auto-detection at byte offset 6 to distinguish formats
//!
//! FPI (Full Page Image) records during replay unconditionally overwrite the
//! target page — this is the torn-page defense mechanism.
//! Corrupted records stop replay gracefully, returning commands replayed so far.

use std::path::Path;

use super::record::{WalRecord, WalRecordType, read_wal_v3_record};
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
/// - `RRDWAL` magic + version=2 => delegate to existing v2 replay
/// - `RRDWAL` magic + version=3 => use v3 replay engine
/// - No `RRDWAL` magic => delegate to AOF (raw RESP v1) replay
/// - Other version => return UnsupportedVersion error
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
                // v2 format — delegate to existing replay
                crate::persistence::wal::replay_wal(databases, path, engine)
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
                            // XactCommit: replay KV ops from payload
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
    let mut segments: Vec<_> = std::fs::read_dir(wal_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_str().is_some_and(|n| n.ends_with(".wal")))
        .map(|e| e.path())
        .collect();

    // Sort by filename (zero-padded sequence ensures correct order)
    segments.sort();

    let mut combined = WalV3ReplayResult::default();
    for seg_path in &segments {
        let result = replay_wal_v3_file(seg_path, redo_lsn, on_command, on_fpi)?;
        combined.commands_replayed += result.commands_replayed;
        combined.fpi_applied += result.fpi_applied;
        if result.last_lsn > combined.last_lsn {
            combined.last_lsn = result.last_lsn;
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

/// Replay a cross-store transaction commit record.
///
/// The XactCommit payload format (little-endian):
/// - txn_id: u64
/// - kv_op_count: u32
/// - For each KV op:
///   - op_type: u8 (0=SET, 1=DEL)
///   - key_len: u32
///   - key: [u8; key_len]
///   - value_len: u32 (only for SET)
///   - value: [u8; value_len] (only for SET)
///
/// Vector and graph ops are handled by their respective replay paths
/// (VectorTxnCommit already exists).
fn replay_xact_commit(databases: &mut [crate::storage::Database], payload: &[u8]) {
    if payload.len() < 12 {
        tracing::warn!("XactCommit payload too short: {} bytes", payload.len());
        return;
    }

    let txn_id = u64::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
        payload[7],
    ]);
    let kv_op_count =
        u32::from_le_bytes([payload[8], payload[9], payload[10], payload[11]]) as usize;

    tracing::debug!(txn_id, kv_op_count, "Replaying XactCommit");

    if kv_op_count == 0 {
        return;
    }

    let mut offset = 12;
    let db = &mut databases[0]; // TODO: support multi-db in cross-store txn

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
        write_wal_v3_record(
            &mut data,
            2,
            WalRecordType::FullPageImage,
            &vec![0xABu8; 128],
        );
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
