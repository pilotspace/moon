//! Binary gossip protocol for the Redis Cluster bus.
//!
//! PING: sent every ~100ms to a random peer.
//! PONG: response to PING; contains our full node state and up to 3 gossip sections.
//! Gossip sections carry rumors about other nodes we know.

#[cfg(test)]
mod tests {
    /// CLUSTER-10: serialize then deserialize a PING produces identical GossipMessage.
    #[test]
    fn test_ping_roundtrip() {
        panic!("not implemented");
    }

    /// PONG round-trip preserves node_id, epoch, slot bitmap, sender_ip, sender_port.
    #[test]
    fn test_pong_with_sections_roundtrip() {
        panic!("not implemented");
    }

    /// GossipSection round-trip preserves all fields.
    #[test]
    fn test_gossip_section_roundtrip() {
        panic!("not implemented");
    }

    /// Magic bytes mismatch on deserialize returns Err.
    #[test]
    fn test_bad_magic_returns_err() {
        panic!("not implemented");
    }
}
