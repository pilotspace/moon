//! CRC16 hash slot computation for Redis Cluster compatibility.
//!
//! Redis uses CRC-16-CCITT / XMODEM variant (polynomial 0x1021, init 0x0000, no reflection).
//! CRITICAL: Use crc16::XMODEM -- not ARC, not BUYPASS. Wrong variant silently misroutes keys.
//!
//! Test vector: slot_for_key(b"foo") MUST return 12356.

use crc16::{State, XMODEM};

use crate::shard::dispatch::extract_hash_tag;

/// Compute the Redis Cluster hash slot for a key.
///
/// If the key contains a hash tag `{tag}`, only the tag content is hashed.
/// Empty tags `{}` are ignored -- the full key is hashed instead.
///
/// Returns a slot in `[0, 16383]`.
#[inline]
pub fn slot_for_key(key: &[u8]) -> u16 {
    let hash_input = extract_hash_tag(key).unwrap_or(key);
    State::<XMODEM>::calculate(hash_input) % 16384
}

/// Does any KEY of this argv hash to a slot other than `first_slot`?
///
/// The cluster `CROSSSLOT` pre-check (both handlers' cluster routing) asks
/// this for every multi-key command. It used to walk every argument after the
/// routing key as if it were a key, so `MSET {t}a x {t}b y` — one slot, two
/// keys — was refused because the VALUE `x` hashed elsewhere (moon#1012).
/// Redis 8.6.1 answers `+OK`; only a user who happened to hash-tag their
/// values got through.
///
/// Key positions come from the shared key walker
/// ([`command_key_positions`](crate::acl::keyspec::command_key_positions),
/// moon#582) — the one ACL `~pattern` enforcement, the moon#592 cross-shard
/// write guard, and cache invalidation already share — rather than from a new
/// per-command table. It reads the registry's `first_key`/`last_key`/`step`,
/// which for `MSET`/`MSETNX` is `1, -1, 2`: every other argument. `COPY`'s
/// `REPLACE` literal and `BITOP`'s operation token fall out of the same specs
/// without the special case the old walk needed.
///
/// `args` excludes the command name, as everywhere else in dispatch.
///
/// Returns `false` for an argv the walker cannot enumerate (`Unknown`) or that
/// names no key: those are malformed invocations of the fixed-spec commands
/// that reach here, and Redis reports their arity/syntax error rather than
/// `CROSSSLOT` — its arity check runs before the cluster check. A non-string
/// frame at a key position is skipped for the same reason; the command
/// rejects it in its own words.
#[must_use]
pub fn keys_span_slots(cmd: &[u8], args: &[crate::protocol::Frame], first_slot: u16) -> bool {
    use crate::acl::keyspec::{KeyPositions, command_key_positions};
    use crate::protocol::Frame;
    let idx = match command_key_positions(cmd, args) {
        KeyPositions::At(idx) | KeyPositions::AtPlusComputed(idx) => idx,
        KeyPositions::None | KeyPositions::Unknown => return false,
    };
    idx.iter().any(|k| match args.get(k.idx) {
        Some(Frame::BulkString(b) | Frame::SimpleString(b)) => slot_for_key(b) != first_slot,
        _ => false,
    })
}

/// Map a cluster slot to the local shard index.
///
/// In cluster mode this replaces xxhash64 % num_shards for key routing.
/// The non-cluster path in dispatch.rs (xxhash64) is NOT changed.
#[inline]
pub fn local_shard_for_slot(slot: u16, num_shards: usize) -> usize {
    slot as usize % num_shards
}

/// Format a MOVED error frame payload.
/// Redis wire format: `MOVED <slot> <host>:<port>`
pub fn moved_error_msg(slot: u16, host: &str, port: u16) -> String {
    format!("MOVED {} {}:{}", slot, host, port)
}

/// Format an ASK error frame payload.
/// Redis wire format: `ASK <slot> <host>:<port>`
pub fn ask_error_msg(slot: u16, host: &str, port: u16) -> String {
    format!("ASK {} {}:{}", slot, host, port)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CLUSTER-01: Redis canonical test vector.
    /// CRC16-CCITT/XMODEM("foo") = 44950, 44950 % 16384 = 12182.
    #[test]
    fn test_foo_slot() {
        assert_eq!(slot_for_key(b"foo"), 12182);
    }

    /// CLUSTER-02: Hash tag co-location.
    #[test]
    fn test_hash_tag_co_location() {
        assert_eq!(slot_for_key(b"{user}.name"), slot_for_key(b"{user}.email"));
        assert_eq!(slot_for_key(b"{user}.name"), slot_for_key(b"{user}.age"));
    }

    /// CLUSTER-03: local_shard_for_slot is slot % num_shards.
    #[test]
    fn test_local_shard_for_slot() {
        assert_eq!(local_shard_for_slot(12182, 8), 12182 % 8);
        assert_eq!(local_shard_for_slot(0, 4), 0);
        assert_eq!(local_shard_for_slot(16383, 4), 16383 % 4);
    }

    /// Empty hash tag {} -- full key is used as hash input.
    #[test]
    fn test_empty_hash_tag_uses_full_key() {
        let s1 = slot_for_key(b"{}foo");
        let s2 = State::<XMODEM>::calculate(b"{}foo") % 16384;
        assert_eq!(s1, s2);
    }

    /// MOVED / ASK error format strings.
    #[test]
    fn test_error_format() {
        assert_eq!(
            moved_error_msg(12182, "127.0.0.1", 6380),
            "MOVED 12182 127.0.0.1:6380"
        );
        assert_eq!(
            ask_error_msg(12182, "127.0.0.1", 6380),
            "ASK 12182 127.0.0.1:6380"
        );
    }

    fn argv(parts: &[&str]) -> Vec<crate::protocol::Frame> {
        parts
            .iter()
            .map(|p| {
                crate::protocol::Frame::BulkString(bytes::Bytes::copy_from_slice(p.as_bytes()))
            })
            .collect()
    }

    /// `cmd` + argv (excluding the name), judged from the slot of its FIRST
    /// argument — which is where both handlers take `first_slot` from
    /// (`extract_primary_key`) for every command below except BITOP.
    fn spans(cmd: &str, parts: &[&str]) -> bool {
        let args = argv(parts);
        keys_span_slots(cmd.as_bytes(), &args, slot_for_key(b"{t}"))
    }

    /// moon#1012: a VALUE is not a key. Every row is the verdict redis-server
    /// 8.6.1 returned in cluster mode (`+OK`/`:1` = same slot, `CROSSSLOT` =
    /// spans), measured over a raw socket with all 16384 slots on one node.
    #[test]
    fn values_between_keys_are_not_slot_checked_1012() {
        // The precondition every row rests on: the two tags really are in
        // different slots, so a pass cannot be a hash coincidence.
        assert_ne!(slot_for_key(b"{t}"), slot_for_key(b"{other}"));
        assert_ne!(slot_for_key(b"{t}"), slot_for_key(b"x"));

        // Keys co-located, values elsewhere: ONE slot. The reported bug.
        assert!(!spans("MSET", &["{t}a", "x", "{t}b", "y"]));
        assert!(!spans("MSET", &["{t}a", "{other}1", "{t}b", "{other}2"]));
        assert!(!spans("MSETNX", &["{t}a", "x", "{t}b", "y"]));
        assert!(!spans("MSET", &["{t}a", "{other}"]));
        // Keys genuinely in two slots still span, whatever the values are.
        assert!(spans("MSET", &["{t}a", "{t}v", "{other}b", "{t}w"]));
        assert!(spans("MSETNX", &["{t}c", "x", "{other}d", "y"]));
        // A malformed MSET (dangling key, no value) must not become CROSSSLOT
        // on the strength of the dangling argument alone: redis reports arity.
        assert!(!spans("MSET", &["{t}a", "x", "{t}b"]));
    }

    /// The rest of the multi-key family the pre-check covers, so moving it to
    /// the key walker cannot quietly drop a refusal the old walk made.
    #[test]
    fn step_one_multikey_commands_keep_their_verdicts_1012() {
        assert!(!spans("MGET", &["{t}a", "{t}b"]));
        assert!(spans("MGET", &["{t}a", "{other}b"]));
        assert!(!spans("DEL", &["{t}a", "{t}b"]));
        assert!(spans("DEL", &["{t}a", "{other}b"]));
        assert!(spans("UNLINK", &["{t}a", "{other}b"]));
        assert!(spans("EXISTS", &["{t}a", "{other}b"]));
        assert!(spans("TOUCH", &["{t}a", "{other}b"]));
        // COPY: the REPLACE literal is not a key.
        assert!(!spans("COPY", &["{t}a", "{t}z", "REPLACE"]));
        assert!(spans("COPY", &["{t}a", "{other}z"]));
        // BITOP: the operation token is not a key; dest and sources are.
        let bitop = |parts: &[&str]| keys_span_slots(b"BITOP", &argv(parts), slot_for_key(b"{t}d"));
        assert!(!bitop(&["AND", "{t}d", "{t}s1", "{t}s2"]));
        assert!(bitop(&["AND", "{t}d", "{other}s1"]));
        assert!(bitop(&["AND", "{t}d", "{t}s1", "{other}s2"]));
    }

    /// Single-key commands whose trailing arguments are field names and
    /// values never span, however those hash — the other shape the issue
    /// asked to audit. (They do not reach the pre-check today, since
    /// `is_multi_key_command` admits none of them; this pins the helper's own
    /// answer so a future caller cannot misread them either.)
    #[test]
    fn single_key_commands_with_value_arguments_never_span_1012() {
        assert!(!spans("HSET", &["{t}h", "{other}f", "{other}v"]));
        assert!(!spans("XADD", &["{t}x", "*", "{other}f", "{other}v"]));
        assert!(!spans("SET", &["{t}k", "{other}v"]));
    }
}
