//! WAL encode/decode for workspace lifecycle operations.
//!
//! Provides serialization for `WorkspaceCreate` and `WorkspaceDrop` WAL
//! records. Layout follows the same defensive decode pattern as
//! `encode_temporal_upsert` / `decode_temporal_upsert` in `wal_v3/record.rs`.

/// Encode a WorkspaceCreate WAL payload.
///
/// Layout: `[ws_id: 16 bytes][name_len: u32 LE][name: N bytes]`
pub fn encode_workspace_create(ws_id: &[u8; 16], name: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(16 + 4 + name.len());
    payload.extend_from_slice(ws_id);
    payload.extend_from_slice(&(name.len() as u32).to_le_bytes());
    payload.extend_from_slice(name);
    payload
}

/// Decode a WorkspaceCreate WAL payload.
///
/// Returns `(ws_id, name)` or `None` if the payload is malformed.
/// Returns owned `Vec<u8>` for name to avoid lifetime issues in WAL replay.
pub fn decode_workspace_create(payload: &[u8]) -> Option<([u8; 16], Vec<u8>)> {
    // Minimum: 16 (ws_id) + 4 (name_len) = 20 bytes
    if payload.len() < 20 {
        return None;
    }
    let mut ws_id = [0u8; 16];
    ws_id.copy_from_slice(&payload[..16]);

    let name_len = u32::from_le_bytes(payload[16..20].try_into().ok()?) as usize;
    if payload.len() < 20 + name_len {
        return None;
    }
    let name = payload[20..20 + name_len].to_vec();
    Some((ws_id, name))
}

/// Encode a WorkspaceDrop WAL payload.
///
/// Layout: `[ws_id: 16 bytes]`
pub fn encode_workspace_drop(ws_id: &[u8; 16]) -> Vec<u8> {
    ws_id.to_vec()
}

/// Decode a WorkspaceDrop WAL payload.
///
/// Returns the 16-byte workspace ID or `None` if the payload is too short.
pub fn decode_workspace_drop(payload: &[u8]) -> Option<[u8; 16]> {
    if payload.len() < 16 {
        return None;
    }
    let mut ws_id = [0u8; 16];
    ws_id.copy_from_slice(&payload[..16]);
    Some(ws_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workspace_create_roundtrip() {
        let ws_id = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let name = b"my-workspace";
        let payload = encode_workspace_create(&ws_id, name);

        let (decoded_id, decoded_name) = decode_workspace_create(&payload).unwrap();
        assert_eq!(decoded_id, ws_id);
        assert_eq!(decoded_name, name);
    }

    #[test]
    fn test_workspace_create_empty_name() {
        let ws_id = [0u8; 16];
        let payload = encode_workspace_create(&ws_id, b"");

        let (decoded_id, decoded_name) = decode_workspace_create(&payload).unwrap();
        assert_eq!(decoded_id, ws_id);
        assert!(decoded_name.is_empty());
    }

    #[test]
    fn test_workspace_create_long_name() {
        let ws_id = [0xFFu8; 16];
        let name = vec![b'a'; 64]; // max allowed name length
        let payload = encode_workspace_create(&ws_id, &name);

        let (decoded_id, decoded_name) = decode_workspace_create(&payload).unwrap();
        assert_eq!(decoded_id, ws_id);
        assert_eq!(decoded_name, name);
    }

    #[test]
    fn test_workspace_create_malformed_returns_none() {
        // Empty payload
        assert!(decode_workspace_create(b"").is_none());
        // Too short (only 15 bytes)
        assert!(decode_workspace_create(&[0u8; 15]).is_none());
        // Has ws_id but no name_len
        assert!(decode_workspace_create(&[0u8; 18]).is_none());
        // name_len says 100 but payload is too short
        let mut bad = [0u8; 20].to_vec();
        bad[16..20].copy_from_slice(&100u32.to_le_bytes());
        assert!(decode_workspace_create(&bad).is_none());
    }

    #[test]
    fn test_workspace_drop_roundtrip() {
        let ws_id = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let payload = encode_workspace_drop(&ws_id);

        let decoded_id = decode_workspace_drop(&payload).unwrap();
        assert_eq!(decoded_id, ws_id);
    }

    #[test]
    fn test_workspace_drop_exact_16_bytes() {
        let ws_id = [0xABu8; 16];
        let payload = encode_workspace_drop(&ws_id);
        assert_eq!(payload.len(), 16);

        let decoded_id = decode_workspace_drop(&payload).unwrap();
        assert_eq!(decoded_id, ws_id);
    }

    #[test]
    fn test_workspace_drop_malformed_returns_none() {
        assert!(decode_workspace_drop(b"").is_none());
        assert!(decode_workspace_drop(&[0u8; 15]).is_none());
    }

    #[test]
    fn test_workspace_drop_extra_bytes_ignored() {
        // Extra bytes after ws_id should not cause failure
        let mut payload = [0xABu8; 16].to_vec();
        payload.extend_from_slice(b"extra");
        let decoded_id = decode_workspace_drop(&payload).unwrap();
        assert_eq!(decoded_id, [0xABu8; 16]);
    }
}
