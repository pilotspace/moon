use bytes::Bytes;
use xxhash_rust::xxh64::xxh64;

use crate::protocol::Frame;
use crate::runtime::channel;
use crate::server::response_slot::ResponseSlot;

const HASH_SEED: u64 = 0;

/// Determine which shard owns a key.
///
/// Uses xxhash64 for fast, high-quality hashing. If the key contains a hash
/// tag (content between `{` and `}`), only the tag is hashed, enabling
/// co-location of related keys on the same shard.
#[inline]
pub fn key_to_shard(key: &[u8], num_shards: usize) -> usize {
    let hash_input = extract_hash_tag(key).unwrap_or(key);
    (xxh64(hash_input, HASH_SEED) % num_shards as u64) as usize
}

/// Extract hash tag: content between first `{` and first `}` after it.
///
/// Empty tags `{}` are ignored (returns `None`), matching Redis Cluster behavior.
pub fn extract_hash_tag(key: &[u8]) -> Option<&[u8]> {
    let open = key.iter().position(|&b| b == b'{')?;
    let close = key[open + 1..].iter().position(|&b| b == b'}')?;
    if close == 0 {
        return None;
    }
    Some(&key[open + 1..open + 1 + close])
}

/// Messages sent to a shard via SPSC channels from the connection layer
/// or from other shards for cross-shard operations.
pub enum ShardMessage {
    /// New TCP connection assigned to this shard by the listener.
    NewConnection(crate::runtime::TcpStream),
    /// Execute a single-key command on this shard, send result back.
    /// The command is wrapped in Arc to avoid deep-cloning the Frame::Array
    /// when dispatching across shards (only an Arc refcount bump instead of
    /// heap-allocating a new Vec + cloning each inner Frame).
    Execute {
        db_index: usize,
        command: std::sync::Arc<Frame>,
        reply_tx: channel::OneshotSender<Frame>,
    },
    /// Execute a multi-key sub-operation on this shard.
    MultiExecute {
        db_index: usize,
        commands: Vec<(Bytes, Frame)>, // (key, command) pairs for this shard
        reply_tx: channel::OneshotSender<Vec<Frame>>,
    },
    /// Execute a batch of pipelined commands on this shard.
    /// Each command is independent (not transactional). Returns one response per command.
    PipelineBatch {
        db_index: usize,
        commands: Vec<std::sync::Arc<Frame>>,
        reply_tx: channel::OneshotSender<Vec<Frame>>,
    },
    /// Publish a message to local subscribers on this shard.
    PubSubFanOut { channel: Bytes, message: Bytes },
    /// Begin a cooperative snapshot at the given epoch.
    /// Shard creates SnapshotState and advances one segment per tick.
    /// Sends reply when snapshot is complete.
    SnapshotBegin {
        epoch: u64,
        snapshot_dir: std::path::PathBuf,
        reply_tx: channel::OneshotSender<Result<(), String>>,
    },
    /// Register a blocked client waiting for data on a key (cross-shard).
    BlockRegister {
        db_index: usize,
        key: Bytes,
        wait_id: u64,
        cmd: crate::blocking::BlockedCommand,
        reply_tx: channel::OneshotSender<Option<crate::protocol::Frame>>,
    },
    /// Cancel a blocked client registration (woken by another shard or timed out).
    BlockCancel { wait_id: u64 },
    /// Register a connected replica's per-shard sender channel with this shard.
    /// Called once per shard per replica when a new replica connection is established.
    /// The shard adds `tx` to its replica_txs list for WAL fan-out.
    RegisterReplica {
        replica_id: u64,
        tx: channel::MpscSender<bytes::Bytes>,
    },
    /// Remove a replica's sender channel from this shard's fan-out list.
    /// Called when a replica disconnects or REPLICAOF NO ONE is executed.
    UnregisterReplica { replica_id: u64 },
    /// Return keys in a specific hash slot (for CLUSTER GETKEYSINSLOT).
    GetKeysInSlot {
        db_index: usize,
        slot: u16,
        count: usize,
        reply_tx: channel::OneshotSender<Vec<bytes::Bytes>>,
    },
    /// Notify shard of slot ownership changes (no-op placeholder for future per-shard caching).
    SlotOwnershipUpdate {
        add_slots: Vec<u16>,
        remove_slots: Vec<u16>,
    },
    /// Fan-out a loaded script to all shards so EVALSHA works regardless of which shard receives it.
    /// Sent by the connection handler on SCRIPT LOAD; received by all other shards' SPSC drain loops.
    ScriptLoad { sha1: String, script: bytes::Bytes },
    /// Migrate a connection's file descriptor to this shard.
    /// The source shard has deregistered the FD and extracted connection state.
    /// This shard must reconstruct the TCP stream and spawn a new handler.
    MigrateConnection {
        fd: std::os::unix::io::RawFd,
        state: crate::server::conn::affinity::MigratedConnectionState,
    },
    /// Execute a single command with pre-allocated response slot (zero allocation).
    /// Used instead of Execute for cross-shard write dispatch.
    ExecuteSlotted {
        db_index: usize,
        command: std::sync::Arc<Frame>,
        response_slot: *const ResponseSlot,
    },
    /// Execute multi-key sub-operation with pre-allocated response slot.
    /// Used instead of MultiExecute for cross-shard multi-key dispatch.
    MultiExecuteSlotted {
        db_index: usize,
        commands: Vec<(Bytes, Frame)>,
        response_slot: *const ResponseSlot,
    },
    /// Execute pipelined batch with pre-allocated response slot.
    /// Used instead of PipelineBatch for cross-shard pipeline dispatch.
    PipelineBatchSlotted {
        db_index: usize,
        commands: Vec<std::sync::Arc<Frame>>,
        response_slot: *const ResponseSlot,
    },
    /// Graceful shutdown signal.
    Shutdown,
}

// SAFETY: All existing ShardMessage fields are Send. The new *const ResponseSlot
// fields point to ResponseSlot values that are Send+Sync, and the pointers remain
// valid for the lifetime of the connection (the ResponseSlotPool outlives all
// dispatched messages). Raw pointers are !Send by default, requiring this explicit impl.
unsafe impl Send for ShardMessage {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_key_to_shard_deterministic() {
        let key = b"user:1234";
        let num_shards = 8;
        let shard1 = key_to_shard(key, num_shards);
        let shard2 = key_to_shard(key, num_shards);
        let shard3 = key_to_shard(key, num_shards);
        assert_eq!(shard1, shard2);
        assert_eq!(shard2, shard3);
    }

    #[test]
    fn test_key_to_shard_distribution() {
        let num_shards = 8;
        let mut seen = HashSet::new();
        // With enough different keys, we should hit multiple shards
        for i in 0..100 {
            let key = format!("key:{}", i);
            seen.insert(key_to_shard(key.as_bytes(), num_shards));
        }
        // With 100 keys across 8 shards, we should see most shards used
        assert!(
            seen.len() >= 4,
            "Expected at least 4 shards used, got {}",
            seen.len()
        );
    }

    #[test]
    fn test_extract_hash_tag_basic() {
        assert_eq!(extract_hash_tag(b"{user}.name"), Some(b"user".as_slice()));
        assert_eq!(
            extract_hash_tag(b"prefix{tag}suffix"),
            Some(b"tag".as_slice())
        );
    }

    #[test]
    fn test_extract_hash_tag_empty() {
        // Empty tag {} should return None
        assert_eq!(extract_hash_tag(b"{}key"), None);
        assert_eq!(extract_hash_tag(b"a{}b"), None);
    }

    #[test]
    fn test_extract_hash_tag_none() {
        // No braces at all
        assert_eq!(extract_hash_tag(b"simplekey"), None);
        // Only opening brace
        assert_eq!(extract_hash_tag(b"key{nope"), None);
    }

    #[test]
    fn test_hash_tag_co_location() {
        let num_shards = 16;
        let shard_name = key_to_shard(b"{user}.name", num_shards);
        let shard_email = key_to_shard(b"{user}.email", num_shards);
        let shard_age = key_to_shard(b"{user}.age", num_shards);
        assert_eq!(shard_name, shard_email);
        assert_eq!(shard_email, shard_age);
    }

    #[test]
    fn test_key_to_shard_single_shard() {
        // With only 1 shard, everything maps to shard 0
        assert_eq!(key_to_shard(b"any_key", 1), 0);
        assert_eq!(key_to_shard(b"another_key", 1), 0);
        assert_eq!(key_to_shard(b"{tag}.key", 1), 0);
    }
}
