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
}
