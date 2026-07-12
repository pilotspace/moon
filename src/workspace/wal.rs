//! WAL encode/decode for workspace lifecycle operations.
//!
//! Provides serialization for `WorkspaceCreate` and `WorkspaceDrop` WAL
//! records. Layout follows the same defensive decode pattern as
//! `encode_temporal_upsert` / `decode_temporal_upsert` in `wal_v3/record.rs`.

/// Encode a WorkspaceCreate WAL payload.
///
/// Layout (K1b, versioned): `[ws_id: 16 bytes][name_len: u32 LE][name: N
/// bytes][created_at_ms: i64 LE]`. The trailing `created_at_ms` field was
/// added in K1b — see [`decode_workspace_create`] for the backward-compat
/// contract with the pre-K1b 20+name_len-byte form.
pub fn encode_workspace_create(ws_id: &[u8; 16], name: &[u8], created_at_ms: i64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(16 + 4 + name.len() + 8);
    payload.extend_from_slice(ws_id);
    payload.extend_from_slice(&(name.len() as u32).to_le_bytes());
    payload.extend_from_slice(name);
    payload.extend_from_slice(&created_at_ms.to_le_bytes());
    payload
}

/// Decode a WorkspaceCreate WAL payload.
///
/// Returns `(ws_id, name, created_at_ms)` or `None` if the payload is
/// malformed. Returns owned `Vec<u8>` for name to avoid lifetime issues in
/// WAL replay.
///
/// K1b backward compatibility (lenient by design): if fewer than 8 bytes
/// follow the name (including the pre-K1b `20 + name_len` layout), the
/// payload decodes with `created_at_ms = 0` — matching the value restart
/// used to restore unconditionally before this fix. If at least 8 bytes
/// follow, the first 8 decode as the creation time and any further trailing
/// bytes are ignored (room for future versioned suffixes). `None` only when
/// the payload can't hold `ws_id` + `name_len` + `name`.
pub fn decode_workspace_create(payload: &[u8]) -> Option<([u8; 16], Vec<u8>, i64)> {
    // Minimum: 16 (ws_id) + 4 (name_len) = 20 bytes
    if payload.len() < 20 {
        return None;
    }
    let mut ws_id = [0u8; 16];
    ws_id.copy_from_slice(&payload[..16]);

    let name_len = u32::from_le_bytes(payload[16..20].try_into().ok()?) as usize;
    let base_len = 20 + name_len;
    if payload.len() < base_len {
        return None;
    }
    let name = payload[20..base_len].to_vec();
    let created_at_ms = if payload.len() >= base_len + 8 {
        i64::from_le_bytes(payload[base_len..base_len + 8].try_into().ok()?)
    } else {
        0
    };
    Some((ws_id, name, created_at_ms))
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
        let payload = encode_workspace_create(&ws_id, name, 1_752_000_000_000);

        let (decoded_id, decoded_name, created_at) = decode_workspace_create(&payload).unwrap();
        assert_eq!(decoded_id, ws_id);
        assert_eq!(decoded_name, name);
        assert_eq!(created_at, 1_752_000_000_000);
    }

    #[test]
    fn test_workspace_create_empty_name() {
        let ws_id = [0u8; 16];
        let payload = encode_workspace_create(&ws_id, b"", 42);

        let (decoded_id, decoded_name, created_at) = decode_workspace_create(&payload).unwrap();
        assert_eq!(decoded_id, ws_id);
        assert!(decoded_name.is_empty());
        assert_eq!(created_at, 42);
    }

    #[test]
    fn test_workspace_create_long_name() {
        let ws_id = [0xFFu8; 16];
        let name = vec![b'a'; 64]; // max allowed name length
        let payload = encode_workspace_create(&ws_id, &name, 7);

        let (decoded_id, decoded_name, created_at) = decode_workspace_create(&payload).unwrap();
        assert_eq!(decoded_id, ws_id);
        assert_eq!(decoded_name, name);
        assert_eq!(created_at, 7);
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

    /// K1b: pre-K1b on-disk records are exactly `20 + name_len` bytes (no
    /// trailing `created_at_ms`). The decoder MUST still accept them —
    /// mixed-segment compatibility, no format bump — returning
    /// `created_at = 0`, matching what restart used to restore unconditionally
    /// before this fix.
    #[test]
    fn test_workspace_create_legacy_format_no_created_at() {
        let ws_id = [9u8; 16];
        let name = b"legacy-ws";
        // Hand-build the PRE-K1b payload shape: no created_at_ms suffix.
        let mut legacy_payload = Vec::with_capacity(20 + name.len());
        legacy_payload.extend_from_slice(&ws_id);
        legacy_payload.extend_from_slice(&(name.len() as u32).to_le_bytes());
        legacy_payload.extend_from_slice(name);
        assert_eq!(legacy_payload.len(), 20 + name.len());

        let (decoded_id, decoded_name, created_at) =
            decode_workspace_create(&legacy_payload).unwrap();
        assert_eq!(decoded_id, ws_id);
        assert_eq!(decoded_name, name);
        assert_eq!(
            created_at, 0,
            "legacy records tolerate missing created_at as 0"
        );
    }

    /// K1b: a new-format record's created_at must survive a negative
    /// (pre-1970, pathological) value too — the field is a plain i64, no
    /// range validation.
    #[test]
    fn test_workspace_create_created_at_negative_value() {
        let ws_id = [3u8; 16];
        let payload = encode_workspace_create(&ws_id, b"neg", -1);
        let (_, _, created_at) = decode_workspace_create(&payload).unwrap();
        assert_eq!(created_at, -1);
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
