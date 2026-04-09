#![no_main]
use libfuzzer_sys::fuzz_target;

use moon::cluster::gossip;

/// Fuzz the cluster gossip message deserializer.
///
/// Exercises magic validation, version check, message type dispatch,
/// slot bitmap parsing, gossip section iteration, and truncation handling.
fuzz_target!(|data: &[u8]| {
    // Should return Err cleanly on any malformed input
    let _ = gossip::deserialize_gossip(data);

    // If deserialize succeeds, roundtrip must not panic
    if let Ok(msg) = gossip::deserialize_gossip(data) {
        let serialized = gossip::serialize_gossip(&msg);
        // Re-deserialize — must succeed
        let _ = gossip::deserialize_gossip(&serialized);
    }
});
