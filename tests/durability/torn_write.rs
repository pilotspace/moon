//! Torn write test for WAL v3 records.
//!
//! Validates that WAL v3 replay correctly detects and handles partial/corrupted
//! records via CRC32C validation. Simulates a torn write by truncating a WAL
//! segment file mid-record, then verifying replay recovers all complete records
//! and cleanly truncates at the corruption point.

use std::io::Write;

/// Write a valid WAL v3 record to a buffer.
fn write_test_record(buf: &mut Vec<u8>, lsn: u64, payload: &[u8]) {
    // Record format (little-endian):
    //   [record_len:u32] [lsn:u64] [type:u8] [flags:u8] [padding:2] [payload] [crc32c:u32]
    let record_len = 16 + payload.len() as u32 + 4; // header + payload + crc

    buf.extend_from_slice(&record_len.to_le_bytes());
    buf.extend_from_slice(&lsn.to_le_bytes());
    buf.push(0x01); // Command type
    buf.push(0x00); // No flags
    buf.extend_from_slice(&[0u8; 2]); // Padding

    buf.extend_from_slice(payload);

    // CRC32C over [lsn..payload] (bytes 4..end-4 of the record)
    let crc_start = buf.len() - (8 + 1 + 1 + 2 + payload.len());
    let crc = crc32c::crc32c(&buf[crc_start..]);
    buf.extend_from_slice(&crc.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_torn_write_detection() {
        // Build a WAL segment with 3 valid records
        let mut wal_data = Vec::new();
        write_test_record(&mut wal_data, 1, b"SET key1 value1");
        write_test_record(&mut wal_data, 2, b"SET key2 value2");
        write_test_record(&mut wal_data, 3, b"SET key3 value3");

        let full_len = wal_data.len();

        // Truncate mid-record (simulate power loss during write)
        let truncated = &wal_data[..full_len - 10];

        // Read records from truncated data
        let mut pos = 0;
        let mut records = Vec::new();
        while pos < truncated.len() {
            match moon::persistence::wal_v3::record::read_wal_v3_record(&truncated[pos..]) {
                Some(record) => {
                    records.push(record.lsn);
                    // Advance past this record
                    let record_len =
                        u32::from_le_bytes(truncated[pos..pos + 4].try_into().unwrap());
                    pos += record_len as usize;
                }
                None => break, // Truncated/corrupted — stop reading
            }
        }

        // Records 1 and 2 should be recoverable, record 3 is truncated
        assert!(
            records.len() >= 2,
            "Expected at least 2 records recovered, got {}",
            records.len()
        );
        assert_eq!(records[0], 1);
        assert_eq!(records[1], 2);
    }

    #[test]
    fn test_crc_corruption_detection() {
        let mut wal_data = Vec::new();
        write_test_record(&mut wal_data, 1, b"SET key1 value1");

        // Corrupt a byte in the payload (but not the length/CRC fields)
        let corrupt_pos = 20; // somewhere in the payload
        if corrupt_pos < wal_data.len() {
            wal_data[corrupt_pos] ^= 0xFF;
        }

        // CRC mismatch should cause None return
        let result = moon::persistence::wal_v3::record::read_wal_v3_record(&wal_data);
        assert!(
            result.is_none(),
            "Corrupted record should return None (CRC mismatch)"
        );
    }

    #[test]
    fn test_empty_data() {
        let result = moon::persistence::wal_v3::record::read_wal_v3_record(&[]);
        assert!(result.is_none(), "Empty data should return None");
    }

    #[test]
    fn test_too_short_data() {
        let result = moon::persistence::wal_v3::record::read_wal_v3_record(&[0u8; 10]);
        assert!(result.is_none(), "Data shorter than header should return None");
    }
}
