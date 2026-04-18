//! WAL encode/decode for MQ lifecycle operations.
//!
//! Provides serialization for `MqCreate` and `MqAck` WAL records.
//! Layout follows the same defensive decode pattern as
//! `encode_workspace_create` / `decode_workspace_create` in
//! `workspace/wal.rs`.
//!
//! All decode functions return `Option<T>` -- NEVER panic or unwrap.

/// Encode an MqCreate WAL payload.
///
/// Layout: `[key_len: u32 LE][key: N bytes][max_delivery_count: u32 LE]`
pub fn encode_mq_create(queue_key: &[u8], max_delivery_count: u32) -> Vec<u8> {
    let mut payload = Vec::with_capacity(4 + queue_key.len() + 4);
    payload.extend_from_slice(&(queue_key.len() as u32).to_le_bytes());
    payload.extend_from_slice(queue_key);
    payload.extend_from_slice(&max_delivery_count.to_le_bytes());
    payload
}

/// Decode an MqCreate WAL payload.
///
/// Returns `(queue_key, max_delivery_count)` or `None` if malformed.
/// Returns owned `Vec<u8>` for queue_key to avoid lifetime issues in WAL replay.
pub fn decode_mq_create(payload: &[u8]) -> Option<(Vec<u8>, u32)> {
    // Minimum: 4 (key_len) = 4 bytes to read the length
    if payload.len() < 4 {
        return None;
    }
    let key_len = u32::from_le_bytes(payload[..4].try_into().ok()?) as usize;
    // Need: 4 (key_len) + key_len + 4 (max_delivery_count)
    if payload.len() < 4 + key_len + 4 {
        return None;
    }
    let key = payload[4..4 + key_len].to_vec();
    let mdc_offset = 4 + key_len;
    let max_delivery_count =
        u32::from_le_bytes(payload[mdc_offset..mdc_offset + 4].try_into().ok()?);
    Some((key, max_delivery_count))
}

/// Encode an MqAck WAL payload.
///
/// Layout: `[key_len: u32 LE][key: N bytes][msg_id_ms: u64 LE][msg_id_seq: u64 LE]`
pub fn encode_mq_ack(queue_key: &[u8], msg_id_ms: u64, msg_id_seq: u64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(4 + queue_key.len() + 16);
    payload.extend_from_slice(&(queue_key.len() as u32).to_le_bytes());
    payload.extend_from_slice(queue_key);
    payload.extend_from_slice(&msg_id_ms.to_le_bytes());
    payload.extend_from_slice(&msg_id_seq.to_le_bytes());
    payload
}

/// Decode an MqAck WAL payload.
///
/// Returns `(queue_key, ms, seq)` or `None` if malformed.
/// Returns owned `Vec<u8>` for queue_key to avoid lifetime issues in WAL replay.
pub fn decode_mq_ack(payload: &[u8]) -> Option<(Vec<u8>, u64, u64)> {
    // Minimum: 4 (key_len) = 4 bytes to read the length
    if payload.len() < 4 {
        return None;
    }
    let key_len = u32::from_le_bytes(payload[..4].try_into().ok()?) as usize;
    // Need: 4 (key_len) + key_len + 8 (ms) + 8 (seq) = 4 + key_len + 16
    if payload.len() < 4 + key_len + 16 {
        return None;
    }
    let key = payload[4..4 + key_len].to_vec();
    let ms_offset = 4 + key_len;
    let ms = u64::from_le_bytes(payload[ms_offset..ms_offset + 8].try_into().ok()?);
    let seq_offset = ms_offset + 8;
    let seq = u64::from_le_bytes(payload[seq_offset..seq_offset + 8].try_into().ok()?);
    Some((key, ms, seq))
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- MqCreate roundtrip ---

    #[test]
    fn test_mq_create_roundtrip() {
        let key = b"orders";
        let max_delivery = 5u32;
        let payload = encode_mq_create(key, max_delivery);

        let (decoded_key, decoded_max) = decode_mq_create(&payload).unwrap();
        assert_eq!(decoded_key, key);
        assert_eq!(decoded_max, max_delivery);
    }

    #[test]
    fn test_mq_create_roundtrip_zero_delivery() {
        let payload = encode_mq_create(b"q", 0);
        let (key, mdc) = decode_mq_create(&payload).unwrap();
        assert_eq!(key, b"q");
        assert_eq!(mdc, 0);
    }

    #[test]
    fn test_mq_create_roundtrip_max_delivery() {
        let payload = encode_mq_create(b"q", u32::MAX);
        let (_, mdc) = decode_mq_create(&payload).unwrap();
        assert_eq!(mdc, u32::MAX);
    }

    #[test]
    fn test_mq_create_roundtrip_empty_key() {
        let payload = encode_mq_create(b"", 3);
        let (key, mdc) = decode_mq_create(&payload).unwrap();
        assert!(key.is_empty());
        assert_eq!(mdc, 3);
    }

    #[test]
    fn test_mq_create_roundtrip_long_key() {
        let long_key = vec![b'x'; 1024];
        let payload = encode_mq_create(&long_key, 10);
        let (key, mdc) = decode_mq_create(&payload).unwrap();
        assert_eq!(key, long_key);
        assert_eq!(mdc, 10);
    }

    #[test]
    fn test_mq_create_malformed_empty() {
        assert!(decode_mq_create(b"").is_none());
    }

    #[test]
    fn test_mq_create_malformed_too_short() {
        // Only 3 bytes -- can't even read key_len
        assert!(decode_mq_create(&[0, 0, 0]).is_none());
    }

    #[test]
    fn test_mq_create_malformed_truncated_key() {
        // key_len says 100 but payload is too short
        let mut bad = Vec::new();
        bad.extend_from_slice(&100u32.to_le_bytes());
        bad.extend_from_slice(&[0u8; 10]); // only 10 bytes of key
        assert!(decode_mq_create(&bad).is_none());
    }

    #[test]
    fn test_mq_create_malformed_missing_max_delivery() {
        // key_len = 4, key = "test", but no max_delivery_count bytes
        let mut bad = Vec::new();
        bad.extend_from_slice(&4u32.to_le_bytes());
        bad.extend_from_slice(b"test");
        assert!(decode_mq_create(&bad).is_none());
    }

    // --- MqAck roundtrip ---

    #[test]
    fn test_mq_ack_roundtrip() {
        let key = b"orders";
        let ms = 1_713_394_800_000u64;
        let seq = 42u64;
        let payload = encode_mq_ack(key, ms, seq);

        let (decoded_key, decoded_ms, decoded_seq) = decode_mq_ack(&payload).unwrap();
        assert_eq!(decoded_key, key);
        assert_eq!(decoded_ms, ms);
        assert_eq!(decoded_seq, seq);
    }

    #[test]
    fn test_mq_ack_roundtrip_zero_id() {
        let payload = encode_mq_ack(b"q", 0, 0);
        let (key, ms, seq) = decode_mq_ack(&payload).unwrap();
        assert_eq!(key, b"q");
        assert_eq!(ms, 0);
        assert_eq!(seq, 0);
    }

    #[test]
    fn test_mq_ack_roundtrip_max_id() {
        let payload = encode_mq_ack(b"q", u64::MAX, u64::MAX);
        let (_, ms, seq) = decode_mq_ack(&payload).unwrap();
        assert_eq!(ms, u64::MAX);
        assert_eq!(seq, u64::MAX);
    }

    #[test]
    fn test_mq_ack_roundtrip_empty_key() {
        let payload = encode_mq_ack(b"", 100, 200);
        let (key, ms, seq) = decode_mq_ack(&payload).unwrap();
        assert!(key.is_empty());
        assert_eq!(ms, 100);
        assert_eq!(seq, 200);
    }

    #[test]
    fn test_mq_ack_malformed_empty() {
        assert!(decode_mq_ack(b"").is_none());
    }

    #[test]
    fn test_mq_ack_malformed_too_short() {
        assert!(decode_mq_ack(&[0, 0, 0]).is_none());
    }

    #[test]
    fn test_mq_ack_malformed_truncated_key() {
        let mut bad = Vec::new();
        bad.extend_from_slice(&100u32.to_le_bytes());
        bad.extend_from_slice(&[0u8; 10]);
        assert!(decode_mq_ack(&bad).is_none());
    }

    #[test]
    fn test_mq_ack_malformed_missing_seq() {
        // key_len = 2, key = "ok", ms = 8 bytes, but no seq
        let mut bad = Vec::new();
        bad.extend_from_slice(&2u32.to_le_bytes());
        bad.extend_from_slice(b"ok");
        bad.extend_from_slice(&1000u64.to_le_bytes());
        // Missing seq bytes
        assert!(decode_mq_ack(&bad).is_none());
    }

    #[test]
    fn test_mq_ack_extra_bytes_ignored() {
        // Extra bytes after valid payload should not cause failure
        let mut payload = encode_mq_ack(b"q", 100, 200);
        payload.extend_from_slice(b"extra_trailing_data");
        let (key, ms, seq) = decode_mq_ack(&payload).unwrap();
        assert_eq!(key, b"q");
        assert_eq!(ms, 100);
        assert_eq!(seq, 200);
    }

    #[test]
    fn test_mq_create_extra_bytes_ignored() {
        let mut payload = encode_mq_create(b"q", 5);
        payload.extend_from_slice(b"trailing");
        let (key, mdc) = decode_mq_create(&payload).unwrap();
        assert_eq!(key, b"q");
        assert_eq!(mdc, 5);
    }
}
