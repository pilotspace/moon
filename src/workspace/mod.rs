//! Workspace partitioning types and functions.
//!
//! Provides `WorkspaceId` (UUID v7), `workspace_key()` prefix injection,
//! `WorkspaceRegistry` for per-shard metadata, and WAL encode/decode helpers.
//! The `{ws_hex}:` hash tag ensures all workspace keys route to the same shard
//! via `extract_hash_tag()` in `shard::dispatch`.

pub mod registry;
pub mod wal;

use bytes::Bytes;

use crate::protocol::Frame;

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

/// Commands that take NO key arguments — return args unchanged.
/// Checked case-insensitively.
const NO_KEY_COMMANDS: &[&[u8]] = &[
    b"PING", b"INFO", b"CLIENT", b"AUTH", b"SELECT", b"MULTI", b"EXEC",
    b"DISCARD", b"DBSIZE", b"TIME", b"RANDOMKEY", b"QUIT", b"COMMAND",
    b"CONFIG", b"DEBUG", b"SLOWLOG", b"CLUSTER", b"WAIT", b"SWAPDB",
    b"MEMORY", b"LATENCY", b"SUBSCRIBE", b"UNSUBSCRIBE", b"PSUBSCRIBE",
    b"PUNSUBSCRIBE", b"PUBLISH", b"TXN", b"TEMPORAL", b"WS", b"HELLO",
    b"RESET", b"ECHO", b"OBJECT", b"FLUSHALL", b"FLUSHDB", b"SAVE",
    b"BGSAVE", b"BGREWRITEAOF", b"LASTSAVE", b"SHUTDOWN", b"REPLICAOF",
    b"SLAVEOF", b"REPLCONF", b"PSYNC", b"SCRIPT", b"EVALSHA", b"EVAL",
    b"ACL", b"MODULE", b"XINFO",
];

/// Commands where ALL args are keys (prefix every arg).
const ALL_KEYS_COMMANDS: &[&[u8]] = &[
    b"MGET", b"DEL", b"UNLINK", b"EXISTS", b"WATCH", b"SINTER",
    b"SUNION", b"SDIFF", b"PFCOUNT", b"PFMERGE",
];

/// Commands where args[0] and args[1] are both keys.
const TWO_KEY_COMMANDS: &[&[u8]] = &[
    b"RENAME", b"RENAMENX", b"COPY", b"RPOPLPUSH", b"LMOVE",
    b"SMOVE", b"BRPOPLPUSH",
];

/// Commands where the dest key is args[0] and source keys follow a numkeys pattern:
/// args[0] = dest, args[1] = numkeys, args[2..2+numkeys] = source keys.
const STORE_NUMKEYS_COMMANDS: &[&[u8]] = &[
    b"ZUNIONSTORE", b"ZINTERSTORE", b"ZDIFFSTORE",
];

/// Commands where dest is args[0], all remaining args are source keys.
const STORE_ALL_COMMANDS: &[&[u8]] = &[
    b"SINTERSTORE", b"SUNIONSTORE", b"SDIFFSTORE",
];

/// Helper: case-insensitive membership check for command byte slices.
#[inline]
fn cmd_in(cmd: &[u8], list: &[&[u8]]) -> bool {
    list.iter().any(|c| cmd.eq_ignore_ascii_case(c))
}

/// Prefix a single key frame with the workspace hash tag.
///
/// If the frame is a `BulkString`, returns a new `BulkString` with the prefixed key.
/// Otherwise returns the frame unchanged (clone).
#[inline]
fn prefix_frame(frame: &Frame, ws_id: &WorkspaceId) -> Frame {
    match frame {
        Frame::BulkString(key) => Frame::BulkString(workspace_key(Some(ws_id), key)),
        other => other.clone(),
    }
}

/// Rewrite command arguments to prefix key/index/graph name positions
/// with the workspace hash tag `{ws_hex}:`.
///
/// This is the **single injection point** for workspace key prefixing.
/// Called once per command in the handler, before `extract_primary_key` / `key_to_shard`.
///
/// # Strategy
/// - No-key commands: return args as-is (cloned).
/// - Multi-key commands: prefix all key positions per command-specific layout.
/// - FT.* / GRAPH.*: prefix index/graph name in args[0].
/// - Default (single-key): prefix args[0] only.
pub fn workspace_rewrite_args(cmd: &[u8], args: &[Frame], ws_id: &WorkspaceId) -> Vec<Frame> {
    // Empty args: nothing to rewrite.
    if args.is_empty() {
        return args.to_vec();
    }

    // No-key commands: passthrough.
    if cmd_in(cmd, NO_KEY_COMMANDS) {
        return args.to_vec();
    }

    // --- FT.* commands ---
    if cmd.len() > 3 && cmd[..3].eq_ignore_ascii_case(b"FT.") {
        // FT._LIST has no index name arg — passthrough.
        if cmd.eq_ignore_ascii_case(b"FT._LIST") {
            return args.to_vec();
        }
        // FT.CREATE, FT.SEARCH, FT.INFO, FT.DROPINDEX, FT.COMPACT,
        // FT.CACHESEARCH, FT.CONFIG, FT.RECOMMEND, FT.NAVIGATE, FT.EXPAND,
        // FT.AGGREGATE — args[0] is always the index name.
        let mut out = args.to_vec();
        out[0] = prefix_frame(&args[0], ws_id);
        return out;
    }

    // --- GRAPH.* commands ---
    if cmd.len() > 6 && cmd[..6].eq_ignore_ascii_case(b"GRAPH.") {
        // GRAPH.LIST has no graph name arg — passthrough.
        if cmd.eq_ignore_ascii_case(b"GRAPH.LIST") {
            return args.to_vec();
        }
        // GRAPH.QUERY, GRAPH.CREATE, GRAPH.DELETE, etc. — args[0] is graph name.
        let mut out = args.to_vec();
        out[0] = prefix_frame(&args[0], ws_id);
        return out;
    }

    // --- MSET: even-indexed args (0, 2, 4, ...) are keys ---
    if cmd.eq_ignore_ascii_case(b"MSET") || cmd.eq_ignore_ascii_case(b"MSETNX") {
        let mut out = Vec::with_capacity(args.len());
        for (i, arg) in args.iter().enumerate() {
            if i % 2 == 0 {
                out.push(prefix_frame(arg, ws_id));
            } else {
                out.push(arg.clone());
            }
        }
        return out;
    }

    // --- All-keys commands (MGET, DEL, UNLINK, EXISTS, etc.) ---
    if cmd_in(cmd, ALL_KEYS_COMMANDS) {
        return args.iter().map(|a| prefix_frame(a, ws_id)).collect();
    }

    // --- Two-key commands (RENAME, COPY, RPOPLPUSH, LMOVE, SMOVE, etc.) ---
    if cmd_in(cmd, TWO_KEY_COMMANDS) {
        let mut out = args.to_vec();
        out[0] = prefix_frame(&args[0], ws_id);
        if args.len() > 1 {
            out[1] = prefix_frame(&args[1], ws_id);
        }
        return out;
    }

    // --- STORE with numkeys (ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE) ---
    // Layout: dest numkeys src1 src2 ... [WEIGHTS ...] [AGGREGATE ...]
    if cmd_in(cmd, STORE_NUMKEYS_COMMANDS) {
        let mut out = args.to_vec();
        // args[0] = dest key
        out[0] = prefix_frame(&args[0], ws_id);
        // args[1] = numkeys
        if let Some(Frame::BulkString(nk_bytes)) = args.get(1) {
            if let Ok(nk_str) = std::str::from_utf8(nk_bytes) {
                if let Ok(numkeys) = nk_str.parse::<usize>() {
                    for i in 2..std::cmp::min(2 + numkeys, args.len()) {
                        out[i] = prefix_frame(&args[i], ws_id);
                    }
                }
            }
        }
        return out;
    }

    // --- STORE-all commands (SINTERSTORE, SUNIONSTORE, SDIFFSTORE) ---
    // Layout: dest src1 src2 ...
    if cmd_in(cmd, STORE_ALL_COMMANDS) {
        return args.iter().map(|a| prefix_frame(a, ws_id)).collect();
    }

    // --- XREAD / XREADGROUP: keys appear after STREAMS keyword ---
    if cmd.eq_ignore_ascii_case(b"XREAD") || cmd.eq_ignore_ascii_case(b"XREADGROUP") {
        let mut out = args.to_vec();
        // Find the STREAMS keyword position.
        let streams_pos = args.iter().position(|a| {
            matches!(a, Frame::BulkString(b) if b.eq_ignore_ascii_case(b"STREAMS"))
        });
        if let Some(pos) = streams_pos {
            // After STREAMS: keys come first, then IDs.
            // Number of keys = (args.len() - pos - 1) / 2
            let remaining = args.len() - pos - 1;
            let num_keys = remaining / 2;
            for i in 0..num_keys {
                let idx = pos + 1 + i;
                if idx < args.len() {
                    out[idx] = prefix_frame(&args[idx], ws_id);
                }
            }
        }
        return out;
    }

    // --- SORT: args[0] is key ---
    if cmd.eq_ignore_ascii_case(b"SORT") || cmd.eq_ignore_ascii_case(b"SORT_RO") {
        let mut out = args.to_vec();
        out[0] = prefix_frame(&args[0], ws_id);
        return out;
    }

    // --- OBJECT subcommands: args[1] is key (args[0] is subcommand) ---
    if cmd.eq_ignore_ascii_case(b"OBJECT") {
        let mut out = args.to_vec();
        if args.len() > 1 {
            out[1] = prefix_frame(&args[1], ws_id);
        }
        return out;
    }

    // --- GEORADIUS/GEORADIUSBYMEMBER STORE variants: args[0] is key, STORE/STOREDIST keys ---
    // For simplicity, prefix args[0] only (standard single-key behavior).
    // The STORE dest key is a separate concern handled later if needed.

    // --- Default: single-key command — prefix args[0] only ---
    let mut out = args.to_vec();
    out[0] = prefix_frame(&args[0], ws_id);
    out
}

/// Strip workspace prefix from response frames for key-returning commands.
///
/// For commands that return key names in their response (KEYS, SCAN, RANDOMKEY,
/// FT.SEARCH), this function strips the `{ws_hex}:` prefix so clients see
/// workspace-internal key names. For all other commands, this is a no-op.
pub fn strip_workspace_prefix_from_response(ws_id: &WorkspaceId, cmd: &[u8], response: &mut Frame) {
    // KEYS: response is Array of key names
    if cmd.eq_ignore_ascii_case(b"KEYS") {
        strip_array_keys(ws_id, response);
        return;
    }

    // SCAN / HSCAN / SSCAN / ZSCAN: response is Array [cursor, Array of key names]
    if cmd.eq_ignore_ascii_case(b"SCAN")
        || cmd.eq_ignore_ascii_case(b"HSCAN")
        || cmd.eq_ignore_ascii_case(b"SSCAN")
        || cmd.eq_ignore_ascii_case(b"ZSCAN")
    {
        if let Frame::Array(outer) = response {
            if outer.len() == 2 {
                strip_array_keys(ws_id, &mut outer[1]);
            }
        }
        return;
    }

    // RANDOMKEY: response is single BulkString
    if cmd.eq_ignore_ascii_case(b"RANDOMKEY") {
        let needs_strip = if let Frame::BulkString(key) = &*response {
            let stripped = strip_workspace_prefix(ws_id, key);
            stripped.len() != key.len()
        } else {
            false
        };
        if needs_strip {
            if let Frame::BulkString(key) = &*response {
                let stripped = strip_workspace_prefix(ws_id, key);
                *response = Frame::BulkString(Bytes::copy_from_slice(stripped));
            }
        }
        return;
    }

    // FT.SEARCH: response array contains key names at positions 1, 3, 5, ...
    // Format: [total_results, key1, fields1, key2, fields2, ...]
    // With LIMIT/pagination: same structure.
    if cmd.eq_ignore_ascii_case(b"FT.SEARCH") {
        if let Frame::Array(arr) = response {
            let len = arr.len();
            let mut i = 1;
            while i < len {
                let needs = if let Frame::BulkString(key) = &arr[i] {
                    strip_workspace_prefix(ws_id, key).len() != key.len()
                } else {
                    false
                };
                if needs {
                    if let Frame::BulkString(key) = &arr[i] {
                        let stripped = strip_workspace_prefix(ws_id, key);
                        arr[i] = Frame::BulkString(Bytes::copy_from_slice(stripped));
                    }
                }
                i += 2; // skip fields array
            }
        }
    }
    // All other commands: no-op (responses don't contain key names).
}

/// Strip workspace prefix from each BulkString element in a Frame::Array.
fn strip_array_keys(ws_id: &WorkspaceId, frame: &mut Frame) {
    if let Frame::Array(arr) = frame {
        for i in 0..arr.len() {
            let needs = if let Frame::BulkString(key) = &arr[i] {
                strip_workspace_prefix(ws_id, key).len() != key.len()
            } else {
                false
            };
            if needs {
                if let Frame::BulkString(key) = &arr[i] {
                    let stripped = strip_workspace_prefix(ws_id, key);
                    arr[i] = Frame::BulkString(Bytes::copy_from_slice(stripped));
                }
            }
        }
    }
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

    // --- workspace_rewrite_args tests ---

    fn bs(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    fn extract_bs(f: &Frame) -> &[u8] {
        match f {
            Frame::BulkString(b) => b,
            _ => panic!("expected BulkString"),
        }
    }

    #[test]
    fn test_rewrite_args_get() {
        let ws = WorkspaceId::from_bytes([0u8; 16]);
        let args = vec![bs(b"mykey")];
        let result = workspace_rewrite_args(b"GET", &args, &ws);
        assert_eq!(result.len(), 1);
        assert_eq!(
            extract_bs(&result[0]),
            b"{00000000000000000000000000000000}:mykey"
        );
    }

    #[test]
    fn test_rewrite_args_set() {
        let ws = WorkspaceId::from_bytes([0u8; 16]);
        let args = vec![bs(b"mykey"), bs(b"myvalue")];
        let result = workspace_rewrite_args(b"SET", &args, &ws);
        assert_eq!(result.len(), 2);
        assert_eq!(
            extract_bs(&result[0]),
            b"{00000000000000000000000000000000}:mykey"
        );
        // Value unchanged
        assert_eq!(extract_bs(&result[1]), b"myvalue");
    }

    #[test]
    fn test_rewrite_args_mget() {
        let ws = WorkspaceId::from_bytes([0u8; 16]);
        let args = vec![bs(b"k1"), bs(b"k2"), bs(b"k3")];
        let result = workspace_rewrite_args(b"MGET", &args, &ws);
        assert_eq!(result.len(), 3);
        for r in &result {
            assert!(extract_bs(r).starts_with(b"{00000000000000000000000000000000}:"));
        }
        assert!(extract_bs(&result[0]).ends_with(b"k1"));
        assert!(extract_bs(&result[1]).ends_with(b"k2"));
        assert!(extract_bs(&result[2]).ends_with(b"k3"));
    }

    #[test]
    fn test_rewrite_args_mset() {
        let ws = WorkspaceId::from_bytes([0u8; 16]);
        let args = vec![bs(b"k1"), bs(b"v1"), bs(b"k2"), bs(b"v2")];
        let result = workspace_rewrite_args(b"MSET", &args, &ws);
        assert_eq!(result.len(), 4);
        // Keys (even indices) prefixed
        assert!(extract_bs(&result[0]).ends_with(b"k1"));
        assert!(extract_bs(&result[0]).starts_with(b"{"));
        assert!(extract_bs(&result[2]).ends_with(b"k2"));
        assert!(extract_bs(&result[2]).starts_with(b"{"));
        // Values (odd indices) unchanged
        assert_eq!(extract_bs(&result[1]), b"v1");
        assert_eq!(extract_bs(&result[3]), b"v2");
    }

    #[test]
    fn test_rewrite_args_no_key_command() {
        let ws = WorkspaceId::from_bytes([0u8; 16]);
        // PING with no args
        let args: Vec<Frame> = vec![];
        let result = workspace_rewrite_args(b"PING", &args, &ws);
        assert!(result.is_empty());

        // INFO with one arg
        let args = vec![bs(b"server")];
        let result = workspace_rewrite_args(b"INFO", &args, &ws);
        assert_eq!(extract_bs(&result[0]), b"server"); // unchanged
    }

    #[test]
    fn test_rewrite_args_ft_search() {
        let ws = WorkspaceId::from_bytes([0u8; 16]);
        let args = vec![bs(b"myindex"), bs(b"*")];
        let result = workspace_rewrite_args(b"FT.SEARCH", &args, &ws);
        assert_eq!(result.len(), 2);
        assert_eq!(
            extract_bs(&result[0]),
            b"{00000000000000000000000000000000}:myindex"
        );
        // Query unchanged
        assert_eq!(extract_bs(&result[1]), b"*");
    }

    #[test]
    fn test_rewrite_args_ft_create() {
        let ws = WorkspaceId::from_bytes([0u8; 16]);
        let args = vec![bs(b"myindex"), bs(b"ON"), bs(b"HASH")];
        let result = workspace_rewrite_args(b"FT.CREATE", &args, &ws);
        assert_eq!(result.len(), 3);
        assert!(extract_bs(&result[0]).starts_with(b"{"));
        assert!(extract_bs(&result[0]).ends_with(b"myindex"));
        // Other args unchanged
        assert_eq!(extract_bs(&result[1]), b"ON");
        assert_eq!(extract_bs(&result[2]), b"HASH");
    }

    #[test]
    fn test_rewrite_args_ft_list() {
        let ws = WorkspaceId::from_bytes([0u8; 16]);
        let args: Vec<Frame> = vec![];
        let result = workspace_rewrite_args(b"FT._LIST", &args, &ws);
        assert!(result.is_empty());
    }

    #[test]
    fn test_rewrite_args_graph_query() {
        let ws = WorkspaceId::from_bytes([0u8; 16]);
        let args = vec![bs(b"mygraph"), bs(b"MATCH (n) RETURN n")];
        let result = workspace_rewrite_args(b"GRAPH.QUERY", &args, &ws);
        assert_eq!(result.len(), 2);
        assert_eq!(
            extract_bs(&result[0]),
            b"{00000000000000000000000000000000}:mygraph"
        );
        // Query unchanged
        assert_eq!(extract_bs(&result[1]), b"MATCH (n) RETURN n");
    }

    #[test]
    fn test_rewrite_args_graph_list() {
        let ws = WorkspaceId::from_bytes([0u8; 16]);
        let args: Vec<Frame> = vec![];
        let result = workspace_rewrite_args(b"GRAPH.LIST", &args, &ws);
        assert!(result.is_empty());
    }

    #[test]
    fn test_rewrite_args_del_multi() {
        let ws = WorkspaceId::from_bytes([0u8; 16]);
        let args = vec![bs(b"k1"), bs(b"k2")];
        let result = workspace_rewrite_args(b"DEL", &args, &ws);
        assert_eq!(result.len(), 2);
        assert!(extract_bs(&result[0]).ends_with(b"k1"));
        assert!(extract_bs(&result[1]).ends_with(b"k2"));
    }

    #[test]
    fn test_rewrite_args_rename() {
        let ws = WorkspaceId::from_bytes([0u8; 16]);
        let args = vec![bs(b"old"), bs(b"new")];
        let result = workspace_rewrite_args(b"RENAME", &args, &ws);
        assert_eq!(result.len(), 2);
        assert!(extract_bs(&result[0]).ends_with(b"old"));
        assert!(extract_bs(&result[0]).starts_with(b"{"));
        assert!(extract_bs(&result[1]).ends_with(b"new"));
        assert!(extract_bs(&result[1]).starts_with(b"{"));
    }

    #[test]
    fn test_rewrite_args_zunionstore() {
        let ws = WorkspaceId::from_bytes([0u8; 16]);
        // ZUNIONSTORE dest 2 src1 src2 WEIGHTS 1 2
        let args = vec![
            bs(b"dest"),
            bs(b"2"),
            bs(b"src1"),
            bs(b"src2"),
            bs(b"WEIGHTS"),
            bs(b"1"),
            bs(b"2"),
        ];
        let result = workspace_rewrite_args(b"ZUNIONSTORE", &args, &ws);
        assert_eq!(result.len(), 7);
        // dest prefixed
        assert!(extract_bs(&result[0]).ends_with(b"dest"));
        assert!(extract_bs(&result[0]).starts_with(b"{"));
        // numkeys unchanged
        assert_eq!(extract_bs(&result[1]), b"2");
        // source keys prefixed
        assert!(extract_bs(&result[2]).ends_with(b"src1"));
        assert!(extract_bs(&result[2]).starts_with(b"{"));
        assert!(extract_bs(&result[3]).ends_with(b"src2"));
        assert!(extract_bs(&result[3]).starts_with(b"{"));
        // WEIGHTS / values unchanged
        assert_eq!(extract_bs(&result[4]), b"WEIGHTS");
        assert_eq!(extract_bs(&result[5]), b"1");
        assert_eq!(extract_bs(&result[6]), b"2");
    }

    #[test]
    fn test_rewrite_args_xread() {
        let ws = WorkspaceId::from_bytes([0u8; 16]);
        // XREAD COUNT 10 STREAMS stream1 stream2 0 0
        let args = vec![
            bs(b"COUNT"),
            bs(b"10"),
            bs(b"STREAMS"),
            bs(b"stream1"),
            bs(b"stream2"),
            bs(b"0"),
            bs(b"0"),
        ];
        let result = workspace_rewrite_args(b"XREAD", &args, &ws);
        assert_eq!(result.len(), 7);
        // COUNT and 10 unchanged
        assert_eq!(extract_bs(&result[0]), b"COUNT");
        assert_eq!(extract_bs(&result[1]), b"10");
        // STREAMS unchanged
        assert_eq!(extract_bs(&result[2]), b"STREAMS");
        // stream keys prefixed
        assert!(extract_bs(&result[3]).ends_with(b"stream1"));
        assert!(extract_bs(&result[3]).starts_with(b"{"));
        assert!(extract_bs(&result[4]).ends_with(b"stream2"));
        assert!(extract_bs(&result[4]).starts_with(b"{"));
        // IDs unchanged
        assert_eq!(extract_bs(&result[5]), b"0");
        assert_eq!(extract_bs(&result[6]), b"0");
    }

    // --- strip_workspace_prefix_from_response tests ---

    #[test]
    fn test_strip_response_keys() {
        let ws = WorkspaceId::from_bytes([0u8; 16]);
        let prefix = b"{00000000000000000000000000000000}:";
        let mut response = Frame::Array(
            vec![
                Frame::BulkString(Bytes::from([prefix.as_slice(), b"k1"].concat())),
                Frame::BulkString(Bytes::from([prefix.as_slice(), b"k2"].concat())),
            ]
            .into(),
        );
        strip_workspace_prefix_from_response(&ws, b"KEYS", &mut response);
        if let Frame::Array(ref arr) = response {
            assert_eq!(extract_bs(&arr[0]), b"k1");
            assert_eq!(extract_bs(&arr[1]), b"k2");
        } else {
            panic!("expected Array");
        }
    }

    #[test]
    fn test_strip_response_scan() {
        let ws = WorkspaceId::from_bytes([0u8; 16]);
        let prefix = b"{00000000000000000000000000000000}:";
        let mut response = Frame::Array(
            vec![
                Frame::BulkString(Bytes::from_static(b"0")),
                Frame::Array(
                    vec![Frame::BulkString(Bytes::from(
                        [prefix.as_slice(), b"mykey"].concat(),
                    ))]
                    .into(),
                ),
            ]
            .into(),
        );
        strip_workspace_prefix_from_response(&ws, b"SCAN", &mut response);
        if let Frame::Array(ref outer) = response {
            if let Frame::Array(ref inner) = outer[1] {
                assert_eq!(extract_bs(&inner[0]), b"mykey");
            } else {
                panic!("expected inner Array");
            }
        } else {
            panic!("expected outer Array");
        }
    }

    #[test]
    fn test_strip_response_ft_search() {
        let ws = WorkspaceId::from_bytes([0u8; 16]);
        let prefix = b"{00000000000000000000000000000000}:";
        let mut response = Frame::Array(
            vec![
                Frame::Integer(2),
                Frame::BulkString(Bytes::from([prefix.as_slice(), b"doc1"].concat())),
                Frame::Array(vec![].into()), // fields
                Frame::BulkString(Bytes::from([prefix.as_slice(), b"doc2"].concat())),
                Frame::Array(vec![].into()), // fields
            ]
            .into(),
        );
        strip_workspace_prefix_from_response(&ws, b"FT.SEARCH", &mut response);
        if let Frame::Array(ref arr) = response {
            assert_eq!(extract_bs(&arr[1]), b"doc1");
            assert_eq!(extract_bs(&arr[3]), b"doc2");
        } else {
            panic!("expected Array");
        }
    }

    #[test]
    fn test_strip_response_randomkey() {
        let ws = WorkspaceId::from_bytes([0u8; 16]);
        let prefix = b"{00000000000000000000000000000000}:";
        let mut response =
            Frame::BulkString(Bytes::from([prefix.as_slice(), b"mykey"].concat()));
        strip_workspace_prefix_from_response(&ws, b"RANDOMKEY", &mut response);
        assert_eq!(extract_bs(&response), b"mykey");
    }

    #[test]
    fn test_strip_response_noop_for_get() {
        let ws = WorkspaceId::from_bytes([0u8; 16]);
        let mut response = Frame::BulkString(Bytes::from_static(b"value"));
        strip_workspace_prefix_from_response(&ws, b"GET", &mut response);
        // Should be unchanged
        assert_eq!(extract_bs(&response), b"value");
    }
}
