//! Crash recovery for vector data: WAL replay + immutable segment loading.
//!
//! Recovery algorithm:
//! 1. Scan WAL file for vector record frames (tag 0x56)
//! 2. Replay VectorUpsert/Delete into MutableSegment per collection
//! 3. Handle TxnCommit/Abort/Checkpoint records
//! 4. Rollback uncommitted transactions at WAL end
//! 5. Load immutable segments from on-disk directories

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use tracing::{info, warn};

use crate::vector::persistence::segment_io::{SegmentIoError, read_immutable_segment};
use crate::vector::persistence::wal_record::{VECTOR_RECORD_TAG, VectorWalRecord, WalRecordError};
use crate::vector::segment::immutable::ImmutableSegment;
use crate::vector::segment::mutable::MutableSegment;
use crate::vector::turbo_quant::collection::CollectionMetadata;

/// Error type for recovery operations.
#[derive(Debug)]
pub enum RecoveryError {
    Io(std::io::Error),
    SegmentLoad(SegmentIoError),
}

impl From<std::io::Error> for RecoveryError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<SegmentIoError> for RecoveryError {
    fn from(e: SegmentIoError) -> Self {
        Self::SegmentLoad(e)
    }
}

/// Recovered collection data: mutable segment + immutable segments.
pub struct RecoveredCollection {
    pub mutable: MutableSegment,
    pub immutable: Vec<(ImmutableSegment, Arc<CollectionMetadata>)>,
}

/// Full recovered state from WAL + disk segments.
pub struct RecoveredState {
    /// collection_id -> recovered collection data
    pub collections: HashMap<u64, RecoveredCollection>,
    /// Last checkpoint LSN seen (for future WAL truncation)
    pub last_checkpoint_lsn: u64,
}

/// State accumulated during WAL replay for one collection.
struct CollectionReplayState {
    mutable: MutableSegment,
    /// point_id -> internal_id in mutable segment
    point_map: HashMap<u64, u32>,
    /// txn_id -> list of internal_ids inserted by that txn
    pending_txns: HashMap<u64, Vec<u32>>,
    /// Committed txn_ids
    committed_txns: HashSet<u64>,
    #[allow(dead_code)]
    dimension: u32,
}

/// Scan WAL bytes for vector record frames.
///
/// Skips RESP block frames (identified by not having the VECTOR_RECORD_TAG).
/// Stops on CRC mismatch, truncation, or any parse error (conservative).
fn scan_vector_records(wal_data: &[u8]) -> Vec<VectorWalRecord> {
    let mut records = Vec::new();
    let mut pos = 32; // skip WAL header
    while pos < wal_data.len() {
        if wal_data[pos] == VECTOR_RECORD_TAG {
            match VectorWalRecord::from_wal_frame(&wal_data[pos..]) {
                Ok((record, consumed)) => {
                    records.push(record);
                    pos += consumed;
                }
                Err(WalRecordError::CrcMismatch { .. }) => {
                    warn!("CRC mismatch at WAL offset {}, stopping vector replay", pos);
                    break;
                }
                Err(WalRecordError::Truncated) => {
                    warn!("Truncated vector record at WAL offset {}, stopping", pos);
                    break;
                }
                Err(e) => {
                    warn!("Vector WAL record error at offset {}: {}, stopping", pos, e);
                    break;
                }
            }
        } else {
            // RESP block frame -- skip it
            if pos + 4 > wal_data.len() {
                break;
            }
            let block_len = u32::from_le_bytes([
                wal_data[pos],
                wal_data[pos + 1],
                wal_data[pos + 2],
                wal_data[pos + 3],
            ]) as usize;
            if block_len > 100_000_000 || pos + 4 + block_len > wal_data.len() {
                warn!(
                    "Vector WAL: invalid RESP block length {} at offset {}, stopping recovery",
                    block_len, pos
                );
                break;
            }
            pos += 4 + block_len;
        }
    }
    records
}

/// Enumerate segment directories in a persistence directory.
///
/// Looks for directories named `segment-{id}` and returns sorted IDs.
fn enumerate_segments(dir: &Path) -> Vec<u64> {
    let mut ids = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if let Some(id_str) = name.strip_prefix("segment-") {
                    if let Ok(id) = id_str.parse::<u64>() {
                        ids.push(id);
                    }
                }
            }
        }
    }
    ids.sort();
    ids
}

/// Replay vector WAL records into per-collection mutable segments.
///
/// Returns map of collection_id -> MutableSegment plus last checkpoint LSN.
fn replay_vector_wal(records: &[VectorWalRecord]) -> (HashMap<u64, MutableSegment>, u64) {
    let mut states: HashMap<u64, CollectionReplayState> = HashMap::new();
    let mut last_checkpoint_lsn: u64 = 0;
    let mut next_lsn: u64 = 1;

    for record in records {
        match record {
            VectorWalRecord::VectorUpsert {
                txn_id,
                collection_id,
                point_id,
                sq_vector,
                tq_code: _,
                norm,
                f32_vector,
            } => {
                let dim = f32_vector.len() as u32;
                let state = states.entry(*collection_id).or_insert_with(|| {
                    CollectionReplayState {
                        mutable: MutableSegment::new(dim, std::sync::Arc::new(
                        crate::vector::turbo_quant::collection::CollectionMetadata::new(
                            *collection_id, dim,
                            crate::vector::types::DistanceMetric::L2,
                            crate::vector::turbo_quant::collection::QuantizationConfig::TurboQuant4,
                            *collection_id,
                        ))),
                        point_map: HashMap::new(),
                        pending_txns: HashMap::new(),
                        committed_txns: HashSet::new(),
                        dimension: dim,
                    }
                });

                let internal_id = if *txn_id != 0 {
                    state.mutable.append_transactional(
                        *point_id, f32_vector, sq_vector, *norm, next_lsn, *txn_id,
                    )
                } else {
                    state
                        .mutable
                        .append(*point_id, f32_vector, sq_vector, *norm, next_lsn)
                };
                state.point_map.insert(*point_id, internal_id);
                if *txn_id != 0 {
                    state
                        .pending_txns
                        .entry(*txn_id)
                        .or_default()
                        .push(internal_id);
                }
                next_lsn += 1;
            }
            VectorWalRecord::VectorDelete {
                txn_id,
                collection_id,
                point_id,
            } => {
                if let Some(state) = states.get(collection_id) {
                    if let Some(&internal_id) = state.point_map.get(point_id) {
                        state.mutable.mark_deleted(internal_id, next_lsn);
                    }
                    // If point_id not found, skip silently (no panic)
                }
                // Track in pending txns for potential abort rollback
                // (deletes don't add internal_ids -- they mark existing ones)
                let _ = txn_id; // used below if needed
                next_lsn += 1;
            }
            VectorWalRecord::TxnCommit { txn_id, commit_lsn } => {
                // Mark txn as committed in all collections
                for state in states.values_mut() {
                    if state.pending_txns.contains_key(txn_id) {
                        state.committed_txns.insert(*txn_id);
                    }
                }
                let _ = commit_lsn;
            }
            VectorWalRecord::TxnAbort { txn_id } => {
                // Roll back all entries from this txn
                for state in states.values() {
                    if let Some(internal_ids) = state.pending_txns.get(txn_id) {
                        for &iid in internal_ids {
                            state.mutable.mark_deleted(iid, next_lsn);
                        }
                    }
                }
                next_lsn += 1;
            }
            VectorWalRecord::Checkpoint {
                segment_id: _,
                last_lsn,
            } => {
                last_checkpoint_lsn = *last_lsn;
            }
        }
    }

    // Rollback uncommitted transactions at end of WAL
    for state in states.values() {
        for (txn_id, internal_ids) in &state.pending_txns {
            if !state.committed_txns.contains(txn_id) {
                for &iid in internal_ids {
                    state.mutable.mark_deleted(iid, next_lsn);
                }
            }
        }
    }

    let mut result = HashMap::new();
    for (cid, state) in states {
        result.insert(cid, state.mutable);
    }
    (result, last_checkpoint_lsn)
}

/// Recover vector store state from WAL + on-disk segments.
///
/// 1. Enumerate segment directories, load each immutable segment.
/// 2. Read WAL file, extract vector record frames.
/// 3. Replay into MutableSegment per collection.
/// 4. Rollback uncommitted transactions.
/// 5. Return RecoveredState with all collections.
pub fn recover_vector_store(
    wal_path: &Path,
    persist_dir: &Path,
) -> Result<RecoveredState, RecoveryError> {
    let mut collections: HashMap<u64, RecoveredCollection> = HashMap::new();

    // 1. Load immutable segments from disk
    let segment_ids = enumerate_segments(persist_dir);
    for seg_id in &segment_ids {
        match read_immutable_segment(persist_dir, *seg_id) {
            Ok((segment, meta)) => {
                let cid = meta.collection_id;
                info!("Loaded immutable segment {} for collection {}", seg_id, cid);
                let entry = collections.entry(cid).or_insert_with(|| {
                    RecoveredCollection {
                    mutable: MutableSegment::new(meta.dimension, std::sync::Arc::new(
                    crate::vector::turbo_quant::collection::CollectionMetadata::new(
                        cid, meta.dimension,
                        crate::vector::types::DistanceMetric::L2,
                        crate::vector::turbo_quant::collection::QuantizationConfig::TurboQuant4,
                        cid,
                    ))),
                    immutable: Vec::new(),
                }
                });
                entry.immutable.push((segment, meta));
            }
            Err(e) => {
                warn!("Failed to load segment {}: {:?}, skipping", seg_id, e);
            }
        }
    }

    // 2. Read WAL and extract vector records
    let mut last_checkpoint_lsn = 0u64;
    if wal_path.exists() {
        let wal_data = std::fs::read(wal_path)?;
        if wal_data.len() > 32 {
            let records = scan_vector_records(&wal_data);
            info!("Scanned {} vector WAL records", records.len());

            // 3. Replay into mutable segments
            let (mutable_map, ckpt_lsn) = replay_vector_wal(&records);
            last_checkpoint_lsn = ckpt_lsn;

            // 4. Merge mutable segments into collections
            for (cid, mutable) in mutable_map {
                match collections.entry(cid) {
                    std::collections::hash_map::Entry::Vacant(e) => {
                        e.insert(RecoveredCollection {
                            mutable,
                            immutable: Vec::new(),
                        });
                    }
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        // Collection already has immutable segments from disk.
                        // Replace the placeholder mutable with the replayed one.
                        e.get_mut().mutable = mutable;
                    }
                }
            }
        }
    }

    Ok(RecoveredState {
        collections,
        last_checkpoint_lsn,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::persistence::wal_record::VectorWalRecord;

    /// Build a minimal WAL file header (32 bytes).
    fn make_wal_header() -> Vec<u8> {
        let mut header = vec![0u8; 32];
        header[0..6].copy_from_slice(b"RRDWAL");
        header[6] = 2; // version
        header
    }

    #[test]
    fn test_wal_writer_append_vector_record_roundtrip() {
        // Write a vector record frame, then parse it back
        let record = VectorWalRecord::VectorUpsert {
            txn_id: 0,
            collection_id: 1,
            point_id: 42,
            sq_vector: vec![1, -2, 3, -4],
            tq_code: vec![0xAB],
            norm: 1.5,
            f32_vector: vec![0.1, 0.2, 0.3, 0.4],
        };
        let frame = record.to_wal_frame();

        // Simulate what append_vector_record does: just buffer the frame bytes
        let mut buf = Vec::new();
        buf.extend_from_slice(&frame);

        // Parse back
        let (decoded, consumed) = VectorWalRecord::from_wal_frame(&buf).unwrap();
        assert_eq!(consumed, frame.len());
        assert_eq!(decoded, record);
    }

    #[test]
    fn test_recover_mutable_upsert_count() {
        let records = vec![
            VectorWalRecord::VectorUpsert {
                txn_id: 0,
                collection_id: 1,
                point_id: 10,
                sq_vector: vec![1, 2, 3, 4],
                tq_code: vec![],
                norm: 1.0,
                f32_vector: vec![0.1, 0.2, 0.3, 0.4],
            },
            VectorWalRecord::VectorUpsert {
                txn_id: 0,
                collection_id: 1,
                point_id: 20,
                sq_vector: vec![5, 6, 7, 8],
                tq_code: vec![],
                norm: 1.0,
                f32_vector: vec![0.5, 0.6, 0.7, 0.8],
            },
        ];
        let (mutables, _) = replay_vector_wal(&records);
        let seg = mutables.get(&1).unwrap();
        assert_eq!(seg.len(), 2);
    }

    #[test]
    fn test_recover_mutable_delete_nonexistent_no_panic() {
        // Delete a point_id that was never upserted -- should not panic
        let records = vec![VectorWalRecord::VectorDelete {
            txn_id: 0,
            collection_id: 1,
            point_id: 999,
        }];
        let (mutables, _) = replay_vector_wal(&records);
        // No collection created because no upserts
        assert!(mutables.is_empty() || mutables.get(&1).map_or(true, |s| s.len() == 0));
    }

    #[test]
    fn test_recover_mutable_delete_marks_entry() {
        let records = vec![
            VectorWalRecord::VectorUpsert {
                txn_id: 0,
                collection_id: 1,
                point_id: 10,
                sq_vector: vec![1, 2, 3, 4],
                tq_code: vec![],
                norm: 1.0,
                f32_vector: vec![0.1, 0.2, 0.3, 0.4],
            },
            VectorWalRecord::VectorDelete {
                txn_id: 0,
                collection_id: 1,
                point_id: 10,
            },
        ];
        let (mutables, _) = replay_vector_wal(&records);
        let seg = mutables.get(&1).unwrap();
        // The entry is still there but marked deleted
        assert_eq!(seg.len(), 1);
        let frozen = seg.freeze();
        assert_ne!(frozen.entries[0].delete_lsn, 0);
    }

    #[test]
    fn test_recover_txn_abort_rolls_back() {
        let records = vec![
            VectorWalRecord::VectorUpsert {
                txn_id: 42,
                collection_id: 1,
                point_id: 10,
                sq_vector: vec![1, 2, 3, 4],
                tq_code: vec![],
                norm: 1.0,
                f32_vector: vec![0.1, 0.2, 0.3, 0.4],
            },
            VectorWalRecord::TxnAbort { txn_id: 42 },
        ];
        let (mutables, _) = replay_vector_wal(&records);
        let seg = mutables.get(&1).unwrap();
        let frozen = seg.freeze();
        // Entry should be marked deleted due to abort
        assert_ne!(frozen.entries[0].delete_lsn, 0);
    }

    #[test]
    fn test_recover_uncommitted_at_eof_rolled_back() {
        // Upsert in a txn, no commit or abort -- should be rolled back
        let records = vec![VectorWalRecord::VectorUpsert {
            txn_id: 99,
            collection_id: 1,
            point_id: 10,
            sq_vector: vec![1, 2, 3, 4],
            tq_code: vec![],
            norm: 1.0,
            f32_vector: vec![0.1, 0.2, 0.3, 0.4],
        }];
        let (mutables, _) = replay_vector_wal(&records);
        let seg = mutables.get(&1).unwrap();
        let frozen = seg.freeze();
        assert_ne!(
            frozen.entries[0].delete_lsn, 0,
            "uncommitted txn should be rolled back"
        );
    }

    #[test]
    fn test_recover_committed_txn_survives() {
        let records = vec![
            VectorWalRecord::VectorUpsert {
                txn_id: 42,
                collection_id: 1,
                point_id: 10,
                sq_vector: vec![1, 2, 3, 4],
                tq_code: vec![],
                norm: 1.0,
                f32_vector: vec![0.1, 0.2, 0.3, 0.4],
            },
            VectorWalRecord::TxnCommit {
                txn_id: 42,
                commit_lsn: 100,
            },
        ];
        let (mutables, _) = replay_vector_wal(&records);
        let seg = mutables.get(&1).unwrap();
        let frozen = seg.freeze();
        assert_eq!(
            frozen.entries[0].delete_lsn, 0,
            "committed entry should not be deleted"
        );
    }

    #[test]
    fn test_recover_checkpoint_records_lsn() {
        let records = vec![
            VectorWalRecord::Checkpoint {
                segment_id: 5,
                last_lsn: 500,
            },
            VectorWalRecord::Checkpoint {
                segment_id: 6,
                last_lsn: 600,
            },
        ];
        let (_, last_ckpt) = replay_vector_wal(&records);
        assert_eq!(last_ckpt, 600);
    }

    #[test]
    fn test_recover_empty_wal_and_no_segments() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_path = tmp.path().join("shard-0.wal");
        let persist_dir = tmp.path().join("vectors");
        // Neither file nor directory exists
        let result = recover_vector_store(&wal_path, &persist_dir).unwrap();
        assert!(result.collections.is_empty());
        assert_eq!(result.last_checkpoint_lsn, 0);
    }

    #[test]
    fn test_recover_vector_store_from_wal() {
        let tmp = tempfile::tempdir().unwrap();
        let persist_dir = tmp.path().join("vectors");
        std::fs::create_dir_all(&persist_dir).unwrap();

        // Build a WAL file with vector records
        let mut wal_data = make_wal_header();

        let upsert1 = VectorWalRecord::VectorUpsert {
            txn_id: 0,
            collection_id: 1,
            point_id: 10,
            sq_vector: vec![1, 2, 3, 4],
            tq_code: vec![],
            norm: 1.0,
            f32_vector: vec![0.1, 0.2, 0.3, 0.4],
        };
        let upsert2 = VectorWalRecord::VectorUpsert {
            txn_id: 0,
            collection_id: 1,
            point_id: 20,
            sq_vector: vec![5, 6, 7, 8],
            tq_code: vec![],
            norm: 2.0,
            f32_vector: vec![0.5, 0.6, 0.7, 0.8],
        };
        wal_data.extend_from_slice(&upsert1.to_wal_frame());
        wal_data.extend_from_slice(&upsert2.to_wal_frame());

        let wal_path = tmp.path().join("shard-0.wal");
        std::fs::write(&wal_path, &wal_data).unwrap();

        let result = recover_vector_store(&wal_path, &persist_dir).unwrap();
        assert_eq!(result.collections.len(), 1);
        let coll = result.collections.get(&1).unwrap();
        assert_eq!(coll.mutable.len(), 2);
    }

    #[test]
    fn test_recover_corrupt_crc_stops_replay() {
        let tmp = tempfile::tempdir().unwrap();
        let persist_dir = tmp.path().join("vectors");
        std::fs::create_dir_all(&persist_dir).unwrap();

        let mut wal_data = make_wal_header();

        // Good record
        let good = VectorWalRecord::VectorUpsert {
            txn_id: 0,
            collection_id: 1,
            point_id: 10,
            sq_vector: vec![1, 2, 3, 4],
            tq_code: vec![],
            norm: 1.0,
            f32_vector: vec![0.1, 0.2, 0.3, 0.4],
        };
        wal_data.extend_from_slice(&good.to_wal_frame());

        // Corrupt record
        let mut bad_frame = VectorWalRecord::VectorUpsert {
            txn_id: 0,
            collection_id: 1,
            point_id: 20,
            sq_vector: vec![5, 6, 7, 8],
            tq_code: vec![],
            norm: 2.0,
            f32_vector: vec![0.5, 0.6, 0.7, 0.8],
        }
        .to_wal_frame();
        let len = bad_frame.len();
        bad_frame[len - 1] ^= 0xFF; // corrupt CRC
        wal_data.extend_from_slice(&bad_frame);

        // Third record that should NOT be recovered
        let third = VectorWalRecord::VectorUpsert {
            txn_id: 0,
            collection_id: 1,
            point_id: 30,
            sq_vector: vec![9, 10, 11, 12],
            tq_code: vec![],
            norm: 3.0,
            f32_vector: vec![0.9, 1.0, 1.1, 1.2],
        };
        wal_data.extend_from_slice(&third.to_wal_frame());

        let wal_path = tmp.path().join("shard-0.wal");
        std::fs::write(&wal_path, &wal_data).unwrap();

        let result = recover_vector_store(&wal_path, &persist_dir).unwrap();
        // Only the first record should be recovered (CRC stops at second)
        let coll = result.collections.get(&1).unwrap();
        assert_eq!(coll.mutable.len(), 1, "corrupt CRC should stop replay");
    }
}
