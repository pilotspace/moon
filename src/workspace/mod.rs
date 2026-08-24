//! Workspace partitioning types and functions.
//!
//! Provides `WorkspaceId` (UUID v7), `workspace_key()` prefix injection,
//! `WorkspaceRegistry` for per-shard metadata, and WAL encode/decode helpers.
//! The `{ws_hex}:` hash tag ensures all workspace keys route to the same shard
//! via `extract_hash_tag()` in `shard::dispatch`.

pub mod registry;
pub mod repl;
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

    /// Parse from a hex-encoded byte slice (32 hex chars, no dashes).
    pub fn from_hex(hex_bytes: &[u8]) -> Option<Self> {
        let s = std::str::from_utf8(hex_bytes).ok()?;
        let mut buf = [0u8; 16];
        hex::decode_to_slice(s, &mut buf).ok()?;
        Some(Self(buf))
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

/// Clone `args`, prefixing exactly the positions in `positions`.
fn prefix_at(args: &[Frame], positions: &[usize], ws_id: &WorkspaceId) -> Vec<Frame> {
    let mut out = args.to_vec();
    for &i in positions {
        if let Some(slot) = out.get_mut(i) {
            *slot = prefix_frame(&args[i], ws_id);
        }
    }
    out
}

/// The error a workspace-bound connection gets when an argv's key positions
/// cannot be determined.
///
/// Refusing is the only safe answer. Running the command unrewritten would let
/// it read and write the GLOBAL keyspace from inside a workspace, which is the
/// isolation break the rewrite exists to prevent; guessing a position would
/// corrupt an argument instead. In practice this is reachable only through a
/// malformed argv (an unparseable `numkeys`, a missing `STREAMS` token) that
/// the command itself would have rejected a moment later.
fn refuse_indeterminate(cmd: &[u8]) -> Frame {
    let name = String::from_utf8_lossy(cmd).to_uppercase();
    Frame::Error(Bytes::from(format!(
        "ERR workspace: refusing '{name}': its key positions could not be determined from this argv"
    )))
}

/// The positions of `SORT`/`SORT_RO`'s `BY`/`GET` PATTERNS.
///
/// These are the keys [`AtPlusComputed`](crate::acl::keyspec::KeyPositions::AtPlusComputed)
/// reports as unnameable:
/// `BY w_*` dereferences `w_<element>` once per element, and the element names
/// are only known at run time. ACL has to treat that as indeterminate, but a
/// workspace does not have to — the pattern is expanded by simple `*`
/// substitution (`command::key_extra::apply_pattern`), so prefixing the PATTERN
/// makes every expansion of it land inside the workspace:
/// `{ws}:w_*` -> `{ws}:w_<element>`.
///
/// Two spellings are sentinels rather than patterns and must be left alone:
/// `BY nosort` (skip sorting) and `GET #` (return the element itself).
/// Prefixing either changes what the command DOES, not where it looks.
///
/// The walk deliberately mirrors `acl::keyspec::source_plus_store` token for
/// token, including its `i += 2` skip past a `STORE` destination, so the two
/// cannot drift into disagreeing about which slots hold keywords.
fn sort_pattern_positions(cmd: &[u8], args: &[Frame]) -> smallvec::SmallVec<[usize; 4]> {
    let mut out = smallvec::SmallVec::new();
    if !(cmd.eq_ignore_ascii_case(b"SORT") || cmd.eq_ignore_ascii_case(b"SORT_RO")) {
        return out;
    }
    let mut i = 1;
    while i < args.len() {
        let Frame::BulkString(tok) = &args[i] else {
            i += 1;
            continue;
        };
        if tok.eq_ignore_ascii_case(b"STORE") || tok.eq_ignore_ascii_case(b"STOREDIST") {
            // The destination is already a NAMED key position.
            i += 2;
            continue;
        }
        if tok.eq_ignore_ascii_case(b"BY") || tok.eq_ignore_ascii_case(b"GET") {
            if let Some(Frame::BulkString(pattern)) = args.get(i + 1)
                && pattern.as_ref() != b"nosort"
                && pattern.as_ref() != b"#"
            {
                out.push(i + 1);
            }
            i += 2;
            continue;
        }
        i += 1;
    }
    out
}

/// Scope `SCAN` to the workspace by prefixing (or injecting) its `MATCH` glob.
///
/// `SCAN` names no key, so the shared walker correctly reports no key positions
/// — but an unscoped `SCAN` walks the WHOLE keyspace and would hand a
/// workspace-bound client every other workspace's keys. The cursor must never
/// be touched (it is a number, not a name); the glob must always be, and when
/// the client supplied none, one is injected.
fn scan_scoped(args: &[Frame], ws_id: &WorkspaceId) -> Vec<Frame> {
    // Every SCAN option is a two-token pair (`MATCH g`, `COUNT n`, `TYPE t`)
    // following the cursor at args[0].
    let mut i = 1;
    while i + 1 < args.len() {
        if let Frame::BulkString(tok) = &args[i]
            && tok.eq_ignore_ascii_case(b"MATCH")
        {
            return prefix_at(args, &[i + 1], ws_id);
        }
        i += 2;
    }
    let mut out = args.to_vec();
    out.push(Frame::BulkString(Bytes::from_static(b"MATCH")));
    out.push(Frame::BulkString(workspace_key(Some(ws_id), b"*")));
    out
}

/// Rewrite command arguments to prefix key/index/graph name positions
/// with the workspace hash tag `{ws_hex}:`.
///
/// This is the **single injection point** for workspace key prefixing.
/// Called once per command in the handler, before `extract_primary_key` / `key_to_shard`.
///
/// # Where the key positions come from
///
/// From [`acl::keyspec::command_key_positions`](crate::acl::keyspec::command_key_positions)
/// — the shared walker (moon#582) that the ACL layer, cache-invalidation, and
/// the cross-shard multi-key check already read. Until moon#668 this file
/// carried a SECOND, hand-rolled walker: four hardcoded name lists plus a
/// handful of special cases, with a single-key default underneath. It had
/// drifted badly. A registry-wide sweep found **50** commands where the two
/// walkers disagreed, in two flavours, both of which the sweep test now fences:
///
///   * *positions the list walker missed* — the key was left UNPREFIXED and so
///     was read or written in the GLOBAL keyspace from inside a workspace.
///     `BITOP dest src...` (every position), `SORT ... STORE dst`,
///     `GEORADIUS* ... STORE|STOREDIST dst`, `ZRANGESTORE`/`GEOSEARCHSTORE`/
///     `LCS` (the source), `BLPOP`/`BRPOP`/`BLMOVE`/`BZPOPMIN`/`TOUCH`
///     (everything after the first key), `EVAL`/`EVALSHA` (all of `KEYS[]`),
///     and `OBJECT`/`XINFO`/`MEMORY` (the key behind the subcommand).
///   * *positions it invented* — the single-key default prefixed `args[0]` on
///     commands whose `args[0]` is not a key at all, so the command broke
///     outright inside a workspace: the `numkeys` count of `ZUNION`/`ZINTER`/
///     `ZDIFF`/`ZINTERCARD`/`SINTERCARD`/`LMPOP`/`ZMPOP`/`BLMPOP`/`BZMPOP`,
///     `BITOP`'s operation name, `XGROUP`'s subcommand, `FCALL`'s function
///     name, and `SCAN`'s cursor.
///
/// # The policy this file adds on top of those facts
///
/// `command_key_positions` reports FACTS about keyspace keys; each caller
/// applies its own POLICY. A workspace partitions three things the ACL layer
/// does not, so they are handled HERE, before the shared walker:
///
///   * **`FT.*` / `GRAPH.*`** — index and graph names live in their own
///     namespace, which ACL key patterns do not cover (the walker answers
///     `None`). Workspaces do partition it: `args[0]` is the name.
///   * **`KEYS <glob>` and `SCAN ... MATCH <glob>`** — a glob is not a key, but
///     an unscoped one enumerates every OTHER workspace's keys.
///   * **`SORT ... BY|GET <pattern>`** — see [`sort_pattern_positions`].
///
/// # What a workspace does NOT partition
///
/// Stated because the old lists partitioned some of it BY ACCIDENT, and the
/// accident was neither consistent nor tested:
///
///   * **Pub/sub channels.** `PUBLISH`/`SUBSCRIBE`/`PSUBSCRIBE` were in the old
///     no-key list and were never scoped; `SPUBLISH`/`SSUBSCRIBE`/`SUNSUBSCRIBE`
///     were not, so the single-key default scoped THEIR channel — sharded
///     pub/sub was workspace-isolated and ordinary pub/sub was not. A channel is
///     not a key, the shared walker names none, and all six are now global.
///   * **`MQ` queue names** — `MQ` was in the old no-key list, so these were
///     never scoped and still are not.
///   * **`FUNCTION` libraries** — the old default prefixed `args[0]`, the
///     SUBCOMMAND, so `FUNCTION` did not work inside a workspace at all. It
///     works now, and its libraries are server-wide.
///   * **A Lua script's direct `redis.call('GET', 'k')`.** `KEYS[]` is
///     prefixed (`EVAL`'s key vector is a named position); a literal key inside
///     the script body is not reachable from the argv at all.
///
/// Tracked in moon#703.
pub fn workspace_rewrite_args(
    cmd: &[u8],
    args: &[Frame],
    ws_id: &WorkspaceId,
) -> Result<Vec<Frame>, Frame> {
    if args.is_empty() {
        return Ok(args.to_vec());
    }

    // --- index / graph namespaces (policy, not keyspace keys) ---
    if cmd.len() > 3 && cmd[..3].eq_ignore_ascii_case(b"FT.") {
        // FT._LIST has no index name arg — passthrough.
        if cmd.eq_ignore_ascii_case(b"FT._LIST") {
            return Ok(args.to_vec());
        }
        return Ok(prefix_at(args, &[0], ws_id));
    }
    if cmd.len() > 6 && cmd[..6].eq_ignore_ascii_case(b"GRAPH.") {
        // GRAPH.LIST has no graph name arg — passthrough.
        if cmd.eq_ignore_ascii_case(b"GRAPH.LIST") {
            return Ok(args.to_vec());
        }
        return Ok(prefix_at(args, &[0], ws_id));
    }

    // --- keyspace globs (policy, not keyspace keys) ---
    if cmd.eq_ignore_ascii_case(b"KEYS") {
        return Ok(prefix_at(args, &[0], ws_id));
    }
    if cmd.eq_ignore_ascii_case(b"SCAN") {
        return Ok(scan_scoped(args, ws_id));
    }

    // --- everything else: the one shared key walker ---
    match crate::acl::keyspec::command_key_positions(cmd, args) {
        crate::acl::keyspec::KeyPositions::None => Ok(args.to_vec()),
        crate::acl::keyspec::KeyPositions::At(idx) => {
            let positions: smallvec::SmallVec<[usize; 8]> = idx.iter().map(|k| k.idx).collect();
            Ok(prefix_at(args, &positions, ws_id))
        }
        crate::acl::keyspec::KeyPositions::AtPlusComputed(idx) => {
            let mut positions: smallvec::SmallVec<[usize; 8]> = idx.iter().map(|k| k.idx).collect();
            positions.extend(sort_pattern_positions(cmd, args));
            Ok(prefix_at(args, &positions, ws_id))
        }
        crate::acl::keyspec::KeyPositions::Unknown => {
            // An unregistered name is not an indeterminate argv: dispatch is
            // about to answer `unknown command`, which is a far better error
            // than anything this layer could invent. Pass it through unchanged
            // — it names no key precisely because it is not a command.
            if crate::command::metadata::lookup(cmd).is_none() {
                return Ok(args.to_vec());
            }
            Err(refuse_indeterminate(cmd))
        }
    }
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
        assert_eq!(result.as_ref(), b"{00000000000000000000000000000000}:mykey");
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

    /// `workspace_rewrite_args` for an argv that must NOT be refused.
    ///
    /// Every rewrite test goes through this rather than `.unwrap()`, so a
    /// regression that starts refusing a well-formed argv fails with the error
    /// text instead of a bare unwrap panic.
    fn expect_rewrite(cmd: &[u8], args: &[Frame], ws: &WorkspaceId) -> Vec<Frame> {
        match workspace_rewrite_args(cmd, args, ws) {
            Ok(out) => out,
            Err(err) => panic!(
                "{} was refused inside a workspace: {err:?}",
                String::from_utf8_lossy(cmd)
            ),
        }
    }

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
        let result = expect_rewrite(b"GET", &args, &ws);
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
        let result = expect_rewrite(b"SET", &args, &ws);
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
        let result = expect_rewrite(b"MGET", &args, &ws);
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
        let result = expect_rewrite(b"MSET", &args, &ws);
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
        let result = expect_rewrite(b"PING", &args, &ws);
        assert!(result.is_empty());

        // INFO with one arg
        let args = vec![bs(b"server")];
        let result = expect_rewrite(b"INFO", &args, &ws);
        assert_eq!(extract_bs(&result[0]), b"server"); // unchanged
    }

    #[test]
    fn test_rewrite_args_ft_search() {
        let ws = WorkspaceId::from_bytes([0u8; 16]);
        let args = vec![bs(b"myindex"), bs(b"*")];
        let result = expect_rewrite(b"FT.SEARCH", &args, &ws);
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
        let result = expect_rewrite(b"FT.CREATE", &args, &ws);
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
        let result = expect_rewrite(b"FT._LIST", &args, &ws);
        assert!(result.is_empty());
    }

    #[test]
    fn test_rewrite_args_graph_query() {
        let ws = WorkspaceId::from_bytes([0u8; 16]);
        let args = vec![bs(b"mygraph"), bs(b"MATCH (n) RETURN n")];
        let result = expect_rewrite(b"GRAPH.QUERY", &args, &ws);
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
        let result = expect_rewrite(b"GRAPH.LIST", &args, &ws);
        assert!(result.is_empty());
    }

    #[test]
    fn test_rewrite_args_del_multi() {
        let ws = WorkspaceId::from_bytes([0u8; 16]);
        let args = vec![bs(b"k1"), bs(b"k2")];
        let result = expect_rewrite(b"DEL", &args, &ws);
        assert_eq!(result.len(), 2);
        assert!(extract_bs(&result[0]).ends_with(b"k1"));
        assert!(extract_bs(&result[1]).ends_with(b"k2"));
    }

    #[test]
    fn test_rewrite_args_rename() {
        let ws = WorkspaceId::from_bytes([0u8; 16]);
        let args = vec![bs(b"old"), bs(b"new")];
        let result = expect_rewrite(b"RENAME", &args, &ws);
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
        let result = expect_rewrite(b"ZUNIONSTORE", &args, &ws);
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
        let result = expect_rewrite(b"XREAD", &args, &ws);
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
        let mut response = Frame::BulkString(Bytes::from([prefix.as_slice(), b"mykey"].concat()));
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
