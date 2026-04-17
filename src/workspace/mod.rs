//! Workspace partitioning types and functions.
//!
//! Provides `WorkspaceId` (UUID v7), `workspace_key()` prefix injection,
//! `WorkspaceRegistry` for per-shard metadata, and WAL encode/decode helpers.
//! The `{ws_hex}:` hash tag ensures all workspace keys route to the same shard
//! via `extract_hash_tag()` in `shard::dispatch`.

pub mod registry;
pub mod wal;

use bytes::Bytes;

pub use registry::{WorkspaceMetadata, WorkspaceRegistry};

/// Unique workspace identifier backed by UUID v7 (time-ordered, 74-bit random).
///
/// Stored as raw 16 bytes internally. Display uses standard UUID format (8-4-4-4-12).
/// `as_hex()` returns 32 lowercase hex chars (no dashes) for hash tag embedding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkspaceId([u8; 16]);

impl WorkspaceId {
    /// Generate a new UUID v7 workspace identifier.
    pub fn new_v7() -> Self {
        Self(uuid::Uuid::now_v7().into_bytes())
    }

    /// Create from raw 16-byte representation (WAL replay, WS.AUTH parsing).
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Return the raw 16-byte representation.
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Return the hex string representation (32 lowercase chars, no dashes).
    pub fn as_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl std::fmt::Display for WorkspaceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Standard UUID format: 8-4-4-4-12
        let u = uuid::Uuid::from_bytes(self.0);
        write!(f, "{}", u)
    }
}

/// Construct a workspace-scoped key.
///
/// If `workspace_id` is `Some`, prepends `{ws_hex}:` to the key.
/// The `{ws_hex}` hash tag ensures all workspace keys route to the same shard
/// via `extract_hash_tag()` in `shard::dispatch`.
///
/// If `workspace_id` is `None`, returns the key unchanged (passthrough path).
#[inline]
pub fn workspace_key(workspace_id: Option<&WorkspaceId>, key: &[u8]) -> Bytes {
    match workspace_id {
        None => Bytes::copy_from_slice(key),
        Some(ws_id) => {
            // Format: {<32-char hex>}:<original_key>
            // Prefix length: 1 ({) + 32 (hex) + 1 (}) + 1 (:) = 35 bytes
            let ws_hex = ws_id.as_hex();
            let mut buf = Vec::with_capacity(35 + key.len());
            buf.push(b'{');
            buf.extend_from_slice(ws_hex.as_bytes());
            buf.push(b'}');
            buf.push(b':');
            buf.extend_from_slice(key);
            Bytes::from(buf)
        }
    }
}

/// Check if a command name is the WS command group.
#[inline]
pub fn is_ws_command(cmd: &[u8]) -> bool {
    cmd.eq_ignore_ascii_case(b"WS")
}

/// Strip the workspace prefix from a key if it matches the given workspace ID.
///
/// Expected prefix format: `{<32-char hex>}:` (35 bytes total).
/// Returns the key with prefix removed, or the original key if no match.
pub fn strip_workspace_prefix<'a>(workspace_id: &WorkspaceId, key: &'a [u8]) -> &'a [u8] {
    // Prefix: { (1) + hex (32) + } (1) + : (1) = 35 bytes
    const PREFIX_LEN: usize = 35;
    if key.len() < PREFIX_LEN {
        return key;
    }
    if key[0] != b'{' || key[33] != b'}' || key[34] != b':' {
        return key;
    }
    // Verify the hex portion matches this workspace
    let hex_portion = &key[1..33];
    let expected_hex = workspace_id.as_hex();
    if hex_portion != expected_hex.as_bytes() {
        return key;
    }
    &key[PREFIX_LEN..]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workspace_id_new_v7() {
        let id = WorkspaceId::new_v7();
        assert_eq!(id.as_bytes().len(), 16);
        assert_eq!(id.as_hex().len(), 32);
        // UUID v7: version nibble (bits 48-51) should be 0x7
        assert_eq!(id.as_bytes()[6] >> 4, 7);
    }

    #[test]
    fn test_workspace_id_from_bytes_roundtrip() {
        let bytes = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let id = WorkspaceId::from_bytes(bytes);
        assert_eq!(*id.as_bytes(), bytes);
    }

    #[test]
    fn test_workspace_id_display_uuid_format() {
        let id = WorkspaceId::from_bytes([0u8; 16]);
        let display = id.to_string();
        // Standard UUID: 00000000-0000-0000-0000-000000000000
        assert_eq!(display, "00000000-0000-0000-0000-000000000000");
        assert_eq!(display.len(), 36);
    }

    #[test]
    fn test_workspace_id_hex_no_dashes() {
        let id = WorkspaceId::from_bytes([0u8; 16]);
        let hex = id.as_hex();
        assert_eq!(hex, "00000000000000000000000000000000");
        assert_eq!(hex.len(), 32);
        assert!(!hex.contains('-'));
    }

    #[test]
    fn test_workspace_key_none_passthrough() {
        let key = b"mykey";
        let result = workspace_key(None, key);
        assert_eq!(result.as_ref(), b"mykey");
    }

    #[test]
    fn test_workspace_key_prefixes_with_hash_tag() {
        let id = WorkspaceId::new_v7();
        let result = workspace_key(Some(&id), b"mykey");
        assert!(result.starts_with(b"{"));
        assert!(result.ends_with(b"mykey"));
        // Should contain }:mykey
        let s = std::str::from_utf8(&result).unwrap();
        assert!(s.contains("}:mykey"));
    }

    #[test]
    fn test_workspace_key_format() {
        let id = WorkspaceId::from_bytes([0u8; 16]);
        let result = workspace_key(Some(&id), b"mykey");
        assert_eq!(
            result.as_ref(),
            b"{00000000000000000000000000000000}:mykey"
        );
    }

    #[test]
    fn test_workspace_key_total_length() {
        let id = WorkspaceId::from_bytes([0u8; 16]);
        let key = b"test";
        let result = workspace_key(Some(&id), key);
        // 1 ({) + 32 (hex) + 1 (}) + 1 (:) + 4 (key) = 39
        assert_eq!(result.len(), 39);
    }

    #[test]
    fn test_hash_tag_routes_to_same_shard() {
        let id = WorkspaceId::new_v7();
        let key1 = workspace_key(Some(&id), b"key1");
        let key2 = workspace_key(Some(&id), b"key2");
        let key3 = workspace_key(Some(&id), b"totally:different:key");

        let shard1 = crate::shard::dispatch::key_to_shard(&key1, 4);
        let shard2 = crate::shard::dispatch::key_to_shard(&key2, 4);
        let shard3 = crate::shard::dispatch::key_to_shard(&key3, 4);

        assert_eq!(shard1, shard2);
        assert_eq!(shard2, shard3);
    }

    #[test]
    fn test_is_ws_command() {
        assert!(is_ws_command(b"WS"));
        assert!(is_ws_command(b"ws"));
        assert!(is_ws_command(b"Ws"));
        assert!(!is_ws_command(b"GET"));
        assert!(!is_ws_command(b"WSS"));
        assert!(!is_ws_command(b"W"));
    }

    #[test]
    fn test_strip_workspace_prefix() {
        let id = WorkspaceId::from_bytes([0u8; 16]);
        let prefixed = workspace_key(Some(&id), b"mykey");
        let stripped = strip_workspace_prefix(&id, &prefixed);
        assert_eq!(stripped, b"mykey");
    }

    #[test]
    fn test_strip_workspace_prefix_no_match() {
        let id = WorkspaceId::from_bytes([0u8; 16]);
        // Key without prefix
        assert_eq!(strip_workspace_prefix(&id, b"mykey"), b"mykey");
        // Key too short
        assert_eq!(strip_workspace_prefix(&id, b"short"), b"short");
        // Key with wrong workspace ID
        let other_id = WorkspaceId::from_bytes([0xFF; 16]);
        let prefixed = workspace_key(Some(&other_id), b"mykey");
        assert_eq!(strip_workspace_prefix(&id, &prefixed), prefixed.as_ref());
    }

    #[test]
    fn test_strip_workspace_prefix_empty_key() {
        let id = WorkspaceId::from_bytes([0u8; 16]);
        let prefixed = workspace_key(Some(&id), b"");
        let stripped = strip_workspace_prefix(&id, &prefixed);
        assert_eq!(stripped, b"");
    }

    #[test]
    fn test_workspace_id_uniqueness() {
        let id1 = WorkspaceId::new_v7();
        let id2 = WorkspaceId::new_v7();
        assert_ne!(id1, id2);
    }
}
