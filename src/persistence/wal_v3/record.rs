//! WAL v3 record format — per-record LSN, CRC32C, FPI with LZ4.
//!
//! Each WAL v3 record is self-describing with a monotonic LSN for
//! point-in-time recovery. Full Page Image (FPI) records use LZ4
//! compression for payloads exceeding the threshold.
//!
//! **Record byte layout (little-endian):**
//! ```text
//! Offset  Size  Field
//! 0       4     record_len (u32 LE) — total record size including this field
//! 4       8     lsn (u64 LE) — monotonic log sequence number
//! 12      1     record_type (u8)
//! 13      1     flags (u8)
//! 14      2     padding (zeroes)
//! 16      N     payload (raw or LZ4-compressed)
//! 16+N    4     crc32c (u32 LE) — over bytes [4..16+N]
//! ```

/// LZ4 compression flag (bit 0).
pub const FLAG_LZ4_COMPRESSED: u8 = 0x01;

/// Minimum payload size for FPI LZ4 compression.
pub const FPI_COMPRESS_THRESHOLD: usize = 256;

/// Minimum record size: 4 (len) + 12 (header) + 4 (crc) = 20 bytes.
const MIN_RECORD_SIZE: usize = 20;

/// WAL v3 record type discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalRecordType {
    /// Standard KV command (RESP-encoded).
    Command = 0x01,
    /// Full Page Image for torn-page defense.
    FullPageImage = 0x10,
    /// Checkpoint marker.
    Checkpoint = 0x20,
    /// Vector upsert operation.
    VectorUpsert = 0x30,
    /// Vector delete operation.
    VectorDelete = 0x31,
    /// Vector transaction commit.
    VectorTxnCommit = 0x32,
    /// Vector transaction abort.
    VectorTxnAbort = 0x33,
    /// Vector checkpoint marker.
    VectorCheckpoint = 0x34,
    /// Temporal KV upsert with literal system_from timestamp.
    TemporalUpsert = 0x35,
    /// Graph node/edge valid_to update with literal system_from timestamp.
    GraphTemporal = 0x36,
    /// File creation event.
    FileCreate = 0x40,
    /// File deletion event.
    FileDelete = 0x41,
    /// File tier change event.
    FileTierChange = 0x42,
    /// Cross-store transaction begin.
    XactBegin = 0x50,
    /// Cross-store transaction commit (contains all store operations).
    XactCommit = 0x51,
    /// Cross-store transaction abort.
    XactAbort = 0x52,
    /// Workspace creation record.
    WorkspaceCreate = 0x60,
    /// Workspace deletion record.
    WorkspaceDrop = 0x61,
}

impl WalRecordType {
    /// Deserialize from a raw byte.
    #[inline]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Self::Command),
            0x10 => Some(Self::FullPageImage),
            0x20 => Some(Self::Checkpoint),
            0x30 => Some(Self::VectorUpsert),
            0x31 => Some(Self::VectorDelete),
            0x32 => Some(Self::VectorTxnCommit),
            0x33 => Some(Self::VectorTxnAbort),
            0x34 => Some(Self::VectorCheckpoint),
            0x35 => Some(Self::TemporalUpsert),
            0x36 => Some(Self::GraphTemporal),
            0x40 => Some(Self::FileCreate),
            0x41 => Some(Self::FileDelete),
            0x42 => Some(Self::FileTierChange),
            0x50 => Some(Self::XactBegin),
            0x51 => Some(Self::XactCommit),
            0x52 => Some(Self::XactAbort),
            0x60 => Some(Self::WorkspaceCreate),
            0x61 => Some(Self::WorkspaceDrop),
            _ => None,
        }
    }
}

/// Parsed WAL v3 record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    /// Monotonic log sequence number.
    pub lsn: u64,
    /// Record type discriminant.
    pub record_type: WalRecordType,
    /// Record flags (compression, etc.).
    pub flags: u8,
    /// Decompressed payload bytes.
    pub payload: Vec<u8>,
}

/// Serialize a WAL v3 record into `buf`.
///
/// FPI records with payloads exceeding [`FPI_COMPRESS_THRESHOLD`] are
/// LZ4-compressed. All other record types store raw payloads.
///
/// Returns the byte offset in `buf` where this record starts.
pub fn write_wal_v3_record(
    buf: &mut Vec<u8>,
    lsn: u64,
    record_type: WalRecordType,
    payload: &[u8],
) -> usize {
    let start = buf.len();

    // Determine compression
    let should_compress =
        record_type == WalRecordType::FullPageImage && payload.len() > FPI_COMPRESS_THRESHOLD;

    let (actual_payload, flags) = if should_compress {
        (
            lz4_flex::compress_prepend_size(payload),
            FLAG_LZ4_COMPRESSED,
        )
    } else {
        (payload.to_vec(), 0u8)
    };

    // record_len = 4 (len field) + 12 (header) + payload + 4 (crc)
    let record_len = (MIN_RECORD_SIZE + actual_payload.len()) as u32;

    // Write record_len
    buf.extend_from_slice(&record_len.to_le_bytes());

    // Write header: lsn(8) + type(1) + flags(1) + pad(2) = 12 bytes
    let crc_start = buf.len();
    buf.extend_from_slice(&lsn.to_le_bytes());
    buf.push(record_type as u8);
    buf.push(flags);
    buf.extend_from_slice(&[0u8; 2]); // padding

    // Write payload
    buf.extend_from_slice(&actual_payload);

    // CRC32C over everything after record_len: [crc_start .. current]
    let crc = crc32c::crc32c(&buf[crc_start..]);
    buf.extend_from_slice(&crc.to_le_bytes());

    start
}

/// Deserialize a WAL v3 record from `data`.
///
/// Returns `None` if data is too short, CRC check fails, or record type is unknown.
pub fn read_wal_v3_record(data: &[u8]) -> Option<WalRecord> {
    if data.len() < MIN_RECORD_SIZE {
        return None;
    }

    let record_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if data.len() < record_len || record_len < MIN_RECORD_SIZE {
        return None;
    }

    // Verify CRC32C: covers bytes [4..record_len-4]
    let crc_stored = u32::from_le_bytes([
        data[record_len - 4],
        data[record_len - 3],
        data[record_len - 2],
        data[record_len - 1],
    ]);
    let crc_computed = crc32c::crc32c(&data[4..record_len - 4]);
    if crc_stored != crc_computed {
        return None;
    }

    // Parse header
    let lsn = u64::from_le_bytes([
        data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
    ]);
    let record_type = WalRecordType::from_u8(data[12])?;
    let flags = data[13];
    // data[14..16] = padding

    // Extract payload
    let payload_raw = &data[16..record_len - 4];

    let payload = if flags & FLAG_LZ4_COMPRESSED != 0 {
        crate::persistence::compression::safe_lz4_decompress(
            payload_raw,
            crate::persistence::compression::MAX_LZ4_DECOMPRESSED,
        )?
    } else {
        payload_raw.to_vec()
    };

    Some(WalRecord {
        lsn,
        record_type,
        flags,
        payload,
    })
}

/// Encode a TemporalUpsert WAL payload.
///
/// Layout: `[key_len: u32 LE][key: bytes][valid_from: i64 LE][system_from: i64 LE][value_len: u32 LE][value: bytes]`
///
/// `system_from` is the literal wall-clock timestamp captured at the handler
/// level. Replay MUST restore this value directly -- never substitute NOW().
pub fn encode_temporal_upsert(key: &[u8], valid_from: i64, system_from: i64, value: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(4 + key.len() + 8 + 8 + 4 + value.len());
    payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&valid_from.to_le_bytes());
    payload.extend_from_slice(&system_from.to_le_bytes());
    payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
    payload.extend_from_slice(value);
    payload
}

/// Decode a TemporalUpsert WAL payload.
///
/// Returns `(key, valid_from, system_from, value)` or `None` if payload is malformed.
pub fn decode_temporal_upsert(payload: &[u8]) -> Option<(&[u8], i64, i64, &[u8])> {
    if payload.len() < 4 {
        return None;
    }
    let key_len = u32::from_le_bytes(payload[..4].try_into().ok()?) as usize;
    let pos = 4;
    if payload.len() < pos + key_len + 8 + 8 + 4 {
        return None;
    }
    let key = &payload[pos..pos + key_len];
    let valid_from = i64::from_le_bytes(payload[pos + key_len..pos + key_len + 8].try_into().ok()?);
    let system_from =
        i64::from_le_bytes(payload[pos + key_len + 8..pos + key_len + 16].try_into().ok()?);
    let value_len =
        u32::from_le_bytes(payload[pos + key_len + 16..pos + key_len + 20].try_into().ok()?)
            as usize;
    let value_start = pos + key_len + 20;
    if payload.len() < value_start + value_len {
        return None;
    }
    let value = &payload[value_start..value_start + value_len];
    Some((key, valid_from, system_from, value))
}

/// Encode a GraphTemporal WAL payload.
///
/// Layout: `[entity_id: u64 LE][is_node: u8][valid_to: i64 LE][system_from: i64 LE]`
///
/// `system_from` is the literal wall-clock timestamp captured at the handler
/// level. Replay MUST restore this value directly -- never substitute NOW().
pub fn encode_graph_temporal(entity_id: u64, is_node: bool, valid_to: i64, system_from: i64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(8 + 1 + 8 + 8);
    payload.extend_from_slice(&entity_id.to_le_bytes());
    payload.push(is_node as u8);
    payload.extend_from_slice(&valid_to.to_le_bytes());
    payload.extend_from_slice(&system_from.to_le_bytes());
    payload
}

/// Decode a GraphTemporal WAL payload.
///
/// Returns `(entity_id, is_node, valid_to, system_from)` or `None` if payload is malformed.
pub fn decode_graph_temporal(payload: &[u8]) -> Option<(u64, bool, i64, i64)> {
    if payload.len() < 25 {
        // 8 (entity_id) + 1 (is_node) + 8 (valid_to) + 8 (system_from)
        return None;
    }
    let entity_id = u64::from_le_bytes(payload[..8].try_into().ok()?);
    let is_node = payload[8] != 0;
    let valid_to = i64::from_le_bytes(payload[9..17].try_into().ok()?);
    let system_from = i64::from_le_bytes(payload[17..25].try_into().ok()?);
    Some((entity_id, is_node, valid_to, system_from))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_command_record() {
        let mut buf = Vec::new();
        let payload = b"SET key value";
        write_wal_v3_record(&mut buf, 42, WalRecordType::Command, payload);

        let record = read_wal_v3_record(&buf).expect("should parse");
        assert_eq!(record.lsn, 42);
        assert_eq!(record.record_type, WalRecordType::Command);
        assert_eq!(record.flags, 0);
        assert_eq!(record.payload, payload);
    }

    #[test]
    fn test_fpi_large_payload_compressed() {
        let mut buf = Vec::new();
        // 4KB payload (exceeds threshold of 256)
        let payload = vec![0xABu8; 4096];
        write_wal_v3_record(&mut buf, 100, WalRecordType::FullPageImage, &payload);

        let record = read_wal_v3_record(&buf).expect("should parse");
        assert_eq!(record.lsn, 100);
        assert_eq!(record.record_type, WalRecordType::FullPageImage);
        assert_eq!(record.flags & FLAG_LZ4_COMPRESSED, FLAG_LZ4_COMPRESSED);
        assert_eq!(record.payload, payload);
        // Compressed record should be smaller than raw
        assert!(buf.len() < 4096 + MIN_RECORD_SIZE);
    }

    #[test]
    fn test_fpi_small_payload_not_compressed() {
        let mut buf = Vec::new();
        // 128 bytes (below threshold of 256)
        let payload = vec![0xCDu8; 128];
        write_wal_v3_record(&mut buf, 200, WalRecordType::FullPageImage, &payload);

        let record = read_wal_v3_record(&buf).expect("should parse");
        assert_eq!(record.flags & FLAG_LZ4_COMPRESSED, 0);
        assert_eq!(record.payload, payload);
        // Uncompressed: exact size = 20 + 128 = 148
        assert_eq!(buf.len(), MIN_RECORD_SIZE + 128);
    }

    #[test]
    fn test_crc_verification_corrupt_payload() {
        let mut buf = Vec::new();
        write_wal_v3_record(&mut buf, 1, WalRecordType::Command, b"hello");

        // Corrupt a payload byte
        buf[16] ^= 0xFF;

        assert!(
            read_wal_v3_record(&buf).is_none(),
            "corrupted CRC should fail"
        );
    }

    #[test]
    fn test_record_type_discriminants() {
        assert_eq!(WalRecordType::Command as u8, 0x01);
        assert_eq!(WalRecordType::FullPageImage as u8, 0x10);
        assert_eq!(WalRecordType::Checkpoint as u8, 0x20);
        assert_eq!(WalRecordType::VectorUpsert as u8, 0x30);
        assert_eq!(WalRecordType::VectorDelete as u8, 0x31);
        assert_eq!(WalRecordType::VectorTxnCommit as u8, 0x32);
        assert_eq!(WalRecordType::VectorTxnAbort as u8, 0x33);
        assert_eq!(WalRecordType::VectorCheckpoint as u8, 0x34);
        assert_eq!(WalRecordType::TemporalUpsert as u8, 0x35);
        assert_eq!(WalRecordType::GraphTemporal as u8, 0x36);
        assert_eq!(WalRecordType::FileCreate as u8, 0x40);
        assert_eq!(WalRecordType::FileDelete as u8, 0x41);
        assert_eq!(WalRecordType::FileTierChange as u8, 0x42);
        assert_eq!(WalRecordType::XactBegin as u8, 0x50);
        assert_eq!(WalRecordType::XactCommit as u8, 0x51);
        assert_eq!(WalRecordType::XactAbort as u8, 0x52);
        assert_eq!(WalRecordType::WorkspaceCreate as u8, 0x60);
        assert_eq!(WalRecordType::WorkspaceDrop as u8, 0x61);

        // from_u8 roundtrips
        for &v in &[
            0x01, 0x10, 0x20, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x40, 0x41, 0x42, 0x50,
            0x51, 0x52, 0x60, 0x61,
        ] {
            assert!(WalRecordType::from_u8(v).is_some());
        }
        assert!(WalRecordType::from_u8(0xFF).is_none());
    }

    #[test]
    fn test_empty_payload_record_size() {
        let mut buf = Vec::new();
        write_wal_v3_record(&mut buf, 0, WalRecordType::Command, &[]);

        // 4 (len) + 8 (lsn) + 1 (type) + 1 (flags) + 2 (pad) + 0 (payload) + 4 (crc) = 20
        assert_eq!(buf.len(), 20);
    }

    #[test]
    fn test_temporal_upsert_roundtrip() {
        let key = b"mykey";
        let valid_from: i64 = 1_713_394_800_000;
        let system_from: i64 = 1_713_394_800_001;
        let value = b"myvalue";
        let payload = encode_temporal_upsert(key, valid_from, system_from, value);
        let (dk, dvf, dsf, dv) = decode_temporal_upsert(&payload).unwrap();
        assert_eq!(dk, key);
        assert_eq!(dvf, valid_from);
        assert_eq!(dsf, system_from);
        assert_eq!(dv, value);
    }

    #[test]
    fn test_temporal_upsert_empty_key_and_value() {
        let payload = encode_temporal_upsert(b"", 0, 0, b"");
        let (dk, dvf, dsf, dv) = decode_temporal_upsert(&payload).unwrap();
        assert!(dk.is_empty());
        assert_eq!(dvf, 0);
        assert_eq!(dsf, 0);
        assert!(dv.is_empty());
    }

    #[test]
    fn test_temporal_upsert_malformed_returns_none() {
        assert!(decode_temporal_upsert(b"").is_none());
        assert!(decode_temporal_upsert(&[0; 3]).is_none());
        // key_len says 100 but payload is too short
        let mut bad = Vec::new();
        bad.extend_from_slice(&100u32.to_le_bytes());
        assert!(decode_temporal_upsert(&bad).is_none());
        // Enough for key but not for timestamps + value_len
        let mut trunc = Vec::new();
        trunc.extend_from_slice(&2u32.to_le_bytes());
        trunc.extend_from_slice(b"ab");
        assert!(decode_temporal_upsert(&trunc).is_none());
        // Enough header but value_len exceeds remaining
        let mut short_val = Vec::new();
        short_val.extend_from_slice(&1u32.to_le_bytes()); // key_len=1
        short_val.push(b'k');
        short_val.extend_from_slice(&0i64.to_le_bytes()); // valid_from
        short_val.extend_from_slice(&0i64.to_le_bytes()); // system_from
        short_val.extend_from_slice(&100u32.to_le_bytes()); // value_len=100
        assert!(decode_temporal_upsert(&short_val).is_none());
    }

    #[test]
    fn test_graph_temporal_roundtrip() {
        let entity_id: u64 = 42;
        let is_node = true;
        let valid_to: i64 = 1_713_394_800_000;
        let system_from: i64 = 1_713_394_800_001;
        let payload = encode_graph_temporal(entity_id, is_node, valid_to, system_from);
        let (eid, in_, vt, sf) = decode_graph_temporal(&payload).unwrap();
        assert_eq!(eid, entity_id);
        assert!(in_);
        assert_eq!(vt, valid_to);
        assert_eq!(sf, system_from);
    }

    #[test]
    fn test_graph_temporal_edge_roundtrip() {
        let payload = encode_graph_temporal(99, false, i64::MAX, 12345);
        let (eid, in_, vt, sf) = decode_graph_temporal(&payload).unwrap();
        assert_eq!(eid, 99);
        assert!(!in_);
        assert_eq!(vt, i64::MAX);
        assert_eq!(sf, 12345);
    }

    #[test]
    fn test_graph_temporal_malformed_returns_none() {
        assert!(decode_graph_temporal(b"").is_none());
        assert!(decode_graph_temporal(&[0; 24]).is_none()); // 24 < 25
    }

    #[test]
    fn test_temporal_upsert_wal_record_roundtrip() {
        let payload = encode_temporal_upsert(b"key1", 1000, 1001, b"val1");
        let mut buf = Vec::new();
        write_wal_v3_record(&mut buf, 50, WalRecordType::TemporalUpsert, &payload);
        let record = read_wal_v3_record(&buf).unwrap();
        assert_eq!(record.record_type, WalRecordType::TemporalUpsert);
        assert_eq!(record.lsn, 50);
        let (dk, dvf, dsf, dv) = decode_temporal_upsert(&record.payload).unwrap();
        assert_eq!(dk, b"key1");
        assert_eq!(dvf, 1000);
        assert_eq!(dsf, 1001);
        assert_eq!(dv, b"val1");
    }

    #[test]
    fn test_graph_temporal_wal_record_roundtrip() {
        let payload = encode_graph_temporal(7, true, 9999, 8888);
        let mut buf = Vec::new();
        write_wal_v3_record(&mut buf, 60, WalRecordType::GraphTemporal, &payload);
        let record = read_wal_v3_record(&buf).unwrap();
        assert_eq!(record.record_type, WalRecordType::GraphTemporal);
        assert_eq!(record.lsn, 60);
        let (eid, in_, vt, sf) = decode_graph_temporal(&record.payload).unwrap();
        assert_eq!(eid, 7);
        assert!(in_);
        assert_eq!(vt, 9999);
        assert_eq!(sf, 8888);
    }
}
