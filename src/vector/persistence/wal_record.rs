//! Vector WAL record format with manual LE serialization and CRC32 framing.
//!
//! Frame format:
//! ```text
//! [u8: VECTOR_RECORD_TAG = 0x56]  -- distinguishes from RESP block frames
//! [u32 LE: payload_len]           -- length of record_type + payload bytes
//! [u8: record_type]               -- 0=Upsert, 1=Delete, 2=TxnCommit, 3=TxnAbort, 4=Checkpoint
//! [payload bytes]                 -- record-specific fields, all LE
//! [u32 LE: crc32]                 -- CRC32 over record_type + payload
//! ```

/// Tag byte distinguishing vector WAL records from RESP block frames.
pub const VECTOR_RECORD_TAG: u8 = 0x56; // 'V'

/// Error type for WAL record serialization/deserialization.
#[derive(Debug)]
pub enum WalRecordError {
    Truncated,
    InvalidTag(u8),
    InvalidRecordType(u8),
    CrcMismatch { expected: u32, actual: u32 },
    DeserializeFailed(String),
}

impl std::fmt::Display for WalRecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => write!(f, "WAL record truncated"),
            Self::InvalidTag(t) => write!(f, "invalid WAL record tag: 0x{t:02x}"),
            Self::InvalidRecordType(t) => write!(f, "invalid WAL record type: {t}"),
            Self::CrcMismatch { expected, actual } => {
                write!(f, "CRC mismatch: expected 0x{expected:08x}, got 0x{actual:08x}")
            }
            Self::DeserializeFailed(msg) => write!(f, "deserialize failed: {msg}"),
        }
    }
}

/// Structured WAL record for vector operations.
///
/// Each variant captures all fields needed to replay the operation during
/// crash recovery. Serialized with manual LE encoding (no serde/bincode)
/// for predictable format and zero overhead.
#[derive(Debug, Clone, PartialEq)]
pub enum VectorWalRecord {
    VectorUpsert {
        txn_id: u64,
        collection_id: u64,
        point_id: u64,
        sq_vector: Vec<i8>,
        tq_code: Vec<u8>,
        norm: f32,
        f32_vector: Vec<f32>,
    },
    VectorDelete {
        txn_id: u64,
        collection_id: u64,
        point_id: u64,
    },
    TxnCommit {
        txn_id: u64,
        commit_lsn: u64,
    },
    TxnAbort {
        txn_id: u64,
    },
    Checkpoint {
        segment_id: u64,
        last_lsn: u64,
    },
}

impl VectorWalRecord {
    /// Returns the record type discriminant (0-4).
    fn record_type(&self) -> u8 {
        match self {
            Self::VectorUpsert { .. } => 0,
            Self::VectorDelete { .. } => 1,
            Self::TxnCommit { .. } => 2,
            Self::TxnAbort { .. } => 3,
            Self::Checkpoint { .. } => 4,
        }
    }

    /// Serialize record-specific fields to a byte buffer (all LE).
    fn serialize_payload(&self, buf: &mut Vec<u8>) {
        match self {
            Self::VectorUpsert {
                txn_id,
                collection_id,
                point_id,
                sq_vector,
                tq_code,
                norm,
                f32_vector,
            } => {
                buf.extend_from_slice(&txn_id.to_le_bytes());
                buf.extend_from_slice(&collection_id.to_le_bytes());
                buf.extend_from_slice(&point_id.to_le_bytes());
                // sq_vector: len:u32 + raw i8 bytes
                buf.extend_from_slice(&(sq_vector.len() as u32).to_le_bytes());
                for &v in sq_vector {
                    buf.push(v as u8);
                }
                // tq_code: len:u32 + raw bytes
                buf.extend_from_slice(&(tq_code.len() as u32).to_le_bytes());
                buf.extend_from_slice(tq_code);
                // norm: f32 LE
                buf.extend_from_slice(&norm.to_le_bytes());
                // f32_vector: len:u32 + f32 LE values
                buf.extend_from_slice(&(f32_vector.len() as u32).to_le_bytes());
                for &v in f32_vector {
                    buf.extend_from_slice(&v.to_le_bytes());
                }
            }
            Self::VectorDelete {
                txn_id,
                collection_id,
                point_id,
            } => {
                buf.extend_from_slice(&txn_id.to_le_bytes());
                buf.extend_from_slice(&collection_id.to_le_bytes());
                buf.extend_from_slice(&point_id.to_le_bytes());
            }
            Self::TxnCommit { txn_id, commit_lsn } => {
                buf.extend_from_slice(&txn_id.to_le_bytes());
                buf.extend_from_slice(&commit_lsn.to_le_bytes());
            }
            Self::TxnAbort { txn_id } => {
                buf.extend_from_slice(&txn_id.to_le_bytes());
            }
            Self::Checkpoint {
                segment_id,
                last_lsn,
            } => {
                buf.extend_from_slice(&segment_id.to_le_bytes());
                buf.extend_from_slice(&last_lsn.to_le_bytes());
            }
        }
    }

    /// Deserialize record-specific fields from a byte slice.
    fn deserialize_payload(record_type: u8, data: &[u8]) -> Result<Self, WalRecordError> {
        let mut pos = 0;

        let read_u32 = |pos: &mut usize| -> Result<u32, WalRecordError> {
            if *pos + 4 > data.len() {
                return Err(WalRecordError::Truncated);
            }
            let val = u32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
            *pos += 4;
            Ok(val)
        };

        let read_u64 = |pos: &mut usize| -> Result<u64, WalRecordError> {
            if *pos + 8 > data.len() {
                return Err(WalRecordError::Truncated);
            }
            let val = u64::from_le_bytes([
                data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3],
                data[*pos + 4], data[*pos + 5], data[*pos + 6], data[*pos + 7],
            ]);
            *pos += 8;
            Ok(val)
        };

        let read_f32 = |pos: &mut usize| -> Result<f32, WalRecordError> {
            if *pos + 4 > data.len() {
                return Err(WalRecordError::Truncated);
            }
            let val = f32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
            *pos += 4;
            Ok(val)
        };

        match record_type {
            0 => {
                let txn_id = read_u64(&mut pos)?;
                let collection_id = read_u64(&mut pos)?;
                let point_id = read_u64(&mut pos)?;
                // sq_vector
                let sq_len = read_u32(&mut pos)? as usize;
                if pos + sq_len > data.len() {
                    return Err(WalRecordError::Truncated);
                }
                let sq_vector: Vec<i8> = data[pos..pos + sq_len].iter().map(|&b| b as i8).collect();
                pos += sq_len;
                // tq_code
                let tq_len = read_u32(&mut pos)? as usize;
                if pos + tq_len > data.len() {
                    return Err(WalRecordError::Truncated);
                }
                let tq_code = data[pos..pos + tq_len].to_vec();
                pos += tq_len;
                // norm
                let norm = read_f32(&mut pos)?;
                // f32_vector
                let f32_len = read_u32(&mut pos)? as usize;
                if pos + f32_len * 4 > data.len() {
                    return Err(WalRecordError::Truncated);
                }
                let mut f32_vector = Vec::with_capacity(f32_len);
                for _ in 0..f32_len {
                    f32_vector.push(read_f32(&mut pos)?);
                }
                Ok(Self::VectorUpsert {
                    txn_id,
                    collection_id,
                    point_id,
                    sq_vector,
                    tq_code,
                    norm,
                    f32_vector,
                })
            }
            1 => {
                let txn_id = read_u64(&mut pos)?;
                let collection_id = read_u64(&mut pos)?;
                let point_id = read_u64(&mut pos)?;
                Ok(Self::VectorDelete {
                    txn_id,
                    collection_id,
                    point_id,
                })
            }
            2 => {
                let txn_id = read_u64(&mut pos)?;
                let commit_lsn = read_u64(&mut pos)?;
                Ok(Self::TxnCommit { txn_id, commit_lsn })
            }
            3 => {
                let txn_id = read_u64(&mut pos)?;
                Ok(Self::TxnAbort { txn_id })
            }
            4 => {
                let segment_id = read_u64(&mut pos)?;
                let last_lsn = read_u64(&mut pos)?;
                Ok(Self::Checkpoint {
                    segment_id,
                    last_lsn,
                })
            }
            _ => Err(WalRecordError::InvalidRecordType(record_type)),
        }
    }

    /// Build a complete WAL frame: TAG + payload_len + record_type + payload + CRC32.
    pub fn to_wal_frame(&self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(64);
        payload.push(self.record_type());
        self.serialize_payload(&mut payload);

        let payload_len = payload.len() as u32;

        // CRC32 over record_type + payload (the entire payload vec)
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&payload);
        let crc = hasher.finalize();

        // Frame: TAG(1) + payload_len(4) + payload(N) + crc32(4)
        let frame_len = 1 + 4 + payload.len() + 4;
        let mut frame = Vec::with_capacity(frame_len);
        frame.push(VECTOR_RECORD_TAG);
        frame.extend_from_slice(&payload_len.to_le_bytes());
        frame.extend_from_slice(&payload);
        frame.extend_from_slice(&crc.to_le_bytes());
        frame
    }

    /// Parse a WAL frame from a byte slice.
    ///
    /// Returns `(record, bytes_consumed)` on success.
    /// Verifies CRC32. Returns `Err` on CRC mismatch, truncation, or invalid data.
    pub fn from_wal_frame(data: &[u8]) -> Result<(Self, usize), WalRecordError> {
        // Minimum frame: TAG(1) + payload_len(4) + record_type(1) + crc32(4) = 10
        if data.len() < 10 {
            return Err(WalRecordError::Truncated);
        }

        // Tag
        if data[0] != VECTOR_RECORD_TAG {
            return Err(WalRecordError::InvalidTag(data[0]));
        }

        // Payload length
        let payload_len = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
        let frame_len = 1 + 4 + payload_len + 4; // TAG + len + payload + crc

        if data.len() < frame_len {
            return Err(WalRecordError::Truncated);
        }

        // Payload slice: starts at offset 5, length = payload_len
        let payload = &data[5..5 + payload_len];

        // CRC32 check
        let stored_crc = u32::from_le_bytes([
            data[5 + payload_len],
            data[5 + payload_len + 1],
            data[5 + payload_len + 2],
            data[5 + payload_len + 3],
        ]);
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(payload);
        let computed_crc = hasher.finalize();

        if stored_crc != computed_crc {
            return Err(WalRecordError::CrcMismatch {
                expected: stored_crc,
                actual: computed_crc,
            });
        }

        // Record type is first byte of payload
        let record_type = payload[0];
        let record_data = &payload[1..];

        let record = Self::deserialize_payload(record_type, record_data)?;
        Ok((record, frame_len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upsert_roundtrip() {
        let record = VectorWalRecord::VectorUpsert {
            txn_id: 42,
            collection_id: 7,
            point_id: 100,
            sq_vector: vec![1, -2, 3, -4],
            tq_code: vec![0xAB, 0xCD, 0xEF],
            norm: 1.5,
            f32_vector: vec![0.1, 0.2, 0.3],
        };
        let frame = record.to_wal_frame();
        let (decoded, consumed) = VectorWalRecord::from_wal_frame(&frame).unwrap();
        assert_eq!(consumed, frame.len());
        assert_eq!(decoded, record);
    }

    #[test]
    fn test_delete_roundtrip() {
        let record = VectorWalRecord::VectorDelete {
            txn_id: 10,
            collection_id: 5,
            point_id: 99,
        };
        let frame = record.to_wal_frame();
        let (decoded, _) = VectorWalRecord::from_wal_frame(&frame).unwrap();
        assert_eq!(decoded, record);
    }

    #[test]
    fn test_txn_commit_roundtrip() {
        let record = VectorWalRecord::TxnCommit {
            txn_id: 123,
            commit_lsn: 456,
        };
        let frame = record.to_wal_frame();
        let (decoded, _) = VectorWalRecord::from_wal_frame(&frame).unwrap();
        assert_eq!(decoded, record);
    }

    #[test]
    fn test_txn_abort_roundtrip() {
        let record = VectorWalRecord::TxnAbort { txn_id: 789 };
        let frame = record.to_wal_frame();
        let (decoded, _) = VectorWalRecord::from_wal_frame(&frame).unwrap();
        assert_eq!(decoded, record);
    }

    #[test]
    fn test_checkpoint_roundtrip() {
        let record = VectorWalRecord::Checkpoint {
            segment_id: 55,
            last_lsn: 9999,
        };
        let frame = record.to_wal_frame();
        let (decoded, _) = VectorWalRecord::from_wal_frame(&frame).unwrap();
        assert_eq!(decoded, record);
    }

    #[test]
    fn test_crc_mismatch_returns_error() {
        let record = VectorWalRecord::VectorDelete {
            txn_id: 1,
            collection_id: 2,
            point_id: 3,
        };
        let mut frame = record.to_wal_frame();
        let len = frame.len();
        frame[len - 1] ^= 0xFF;
        match VectorWalRecord::from_wal_frame(&frame) {
            Err(WalRecordError::CrcMismatch { .. }) => {}
            other => panic!("expected CrcMismatch, got {:?}", other),
        }
    }

    #[test]
    fn test_truncated_frame_returns_error() {
        let record = VectorWalRecord::TxnCommit {
            txn_id: 1,
            commit_lsn: 2,
        };
        let frame = record.to_wal_frame();
        match VectorWalRecord::from_wal_frame(&frame[..3]) {
            Err(WalRecordError::Truncated) => {}
            other => panic!("expected Truncated, got {:?}", other),
        }
    }

    #[test]
    fn test_to_wal_frame_has_tag_and_length() {
        let record = VectorWalRecord::TxnAbort { txn_id: 1 };
        let frame = record.to_wal_frame();
        assert_eq!(frame[0], VECTOR_RECORD_TAG);
        let payload_len = u32::from_le_bytes([frame[1], frame[2], frame[3], frame[4]]);
        assert_eq!(frame.len(), 1 + 4 + payload_len as usize + 4);
    }

    #[test]
    fn test_from_wal_frame_rejects_bad_tag() {
        let record = VectorWalRecord::TxnAbort { txn_id: 1 };
        let mut frame = record.to_wal_frame();
        frame[0] = 0x00;
        match VectorWalRecord::from_wal_frame(&frame) {
            Err(WalRecordError::InvalidTag(0x00)) => {}
            other => panic!("expected InvalidTag, got {:?}", other),
        }
    }
}
