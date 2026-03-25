use bytes::Bytes;
use rand::Rng;
use std::sync::atomic::{AtomicU64, Ordering};

pub use crate::replication::handshake::ReplicaHandshakeState;

pub struct ReplicationState {
    pub role: ReplicationRole,
    /// Primary replication ID (40-char hex). Survives restarts -- persisted to disk.
    pub repl_id: String,
    /// Secondary replication ID (previous master's ID). Used after failover.
    pub repl_id2: String,
    /// Per-shard write offset (monotonic bytes appended, NEVER resets on WAL truncation).
    /// Length = num_shards.
    pub shard_offsets: Vec<AtomicU64>,
    /// Sum of all shard offsets -- global master replication offset.
    pub master_repl_offset: AtomicU64,
    /// Connected replicas (master mode). Guarded by Arc<RwLock<ReplicationState>> callers.
    pub replicas: Vec<ReplicaInfo>,
}

pub enum ReplicationRole {
    Master,
    Replica {
        host: String,
        port: u16,
        state: ReplicaHandshakeState,
    },
}

pub struct ReplicaInfo {
    pub id: u64, // monotonic ID for unregister
    pub addr: std::net::SocketAddr,
    /// Per-shard acknowledged offsets from this replica (updated on REPLCONF ACK).
    pub ack_offsets: Vec<AtomicU64>,
    /// Channels to per-shard sender tasks. shard_txs[shard_id] = Sender for that shard.
    pub shard_txs: Vec<crate::runtime::channel::MpscSender<Bytes>>,
    /// Last ACK time as unix seconds (for lag computation in INFO).
    pub last_ack_time: AtomicU64,
}

impl ReplicationState {
    pub fn new(num_shards: usize, repl_id: String, repl_id2: String) -> Self {
        ReplicationState {
            role: ReplicationRole::Master,
            repl_id,
            repl_id2,
            shard_offsets: (0..num_shards).map(|_| AtomicU64::new(0)).collect(),
            master_repl_offset: AtomicU64::new(0),
            replicas: Vec::new(),
        }
    }

    /// Increment the offset for the given shard by delta bytes.
    /// Also adds delta to master_repl_offset.
    pub fn increment_shard_offset(&self, shard_id: usize, delta: u64) {
        if shard_id < self.shard_offsets.len() {
            self.shard_offsets[shard_id].fetch_add(delta, Ordering::Relaxed);
            self.master_repl_offset.fetch_add(delta, Ordering::Relaxed);
        }
    }

    /// Returns sum of all per-shard offsets.
    pub fn total_offset(&self) -> u64 {
        self.master_repl_offset.load(Ordering::Relaxed)
    }

    /// Returns the per-shard offset for a specific shard.
    pub fn shard_offset(&self, shard_id: usize) -> u64 {
        self.shard_offsets
            .get(shard_id)
            .map(|o| o.load(Ordering::Relaxed))
            .unwrap_or(0)
    }
}

const ZEROED_ID: &str = "0000000000000000000000000000000000000000";

/// Generate a Redis-compatible 40-char hex replication ID.
pub fn generate_repl_id() -> String {
    let mut rng = rand::rng();
    (0..20).map(|_| format!("{:02x}", rng.random::<u8>())).collect()
}

/// Atomically persist replication IDs to {dir}/replication.state.
/// Uses .tmp + rename pattern (same as WalWriter::truncate_after_snapshot).
pub fn save_replication_state(
    dir: &std::path::Path,
    repl_id: &str,
    repl_id2: &str,
) -> std::io::Result<()> {
    let tmp = dir.join("replication.state.tmp");
    let dst = dir.join("replication.state");
    std::fs::write(&tmp, format!("{}\n{}\n", repl_id, repl_id2))?;
    std::fs::rename(&tmp, &dst)?;
    Ok(())
}

/// Load replication IDs from {dir}/replication.state.
/// If file missing or malformed, generates new IDs and saves them.
pub fn load_replication_state(dir: &std::path::Path) -> (String, String) {
    let path = dir.join("replication.state");
    if let Ok(content) = std::fs::read_to_string(&path) {
        let mut lines = content.lines();
        let id1 = lines.next().unwrap_or("").to_string();
        let id2 = lines.next().unwrap_or(ZEROED_ID).to_string();
        if id1.len() == 40 {
            let id2 = if id2.len() == 40 {
                id2
            } else {
                ZEROED_ID.to_string()
            };
            return (id1, id2);
        }
    }
    let id1 = generate_repl_id();
    let id2 = ZEROED_ID.to_string();
    let _ = save_replication_state(dir, &id1, &id2);
    (id1, id2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_repl_id_length_and_hex() {
        let id = generate_repl_id();
        assert_eq!(id.len(), 40, "repl_id must be exactly 40 chars");
        assert!(
            id.chars().all(|c| c.is_ascii_hexdigit()),
            "repl_id must be hex"
        );
    }

    #[test]
    fn test_generate_repl_id_uniqueness() {
        let a = generate_repl_id();
        let b = generate_repl_id();
        assert_ne!(a, b, "two generated IDs should differ");
    }

    #[test]
    fn test_replication_state_new_shard_offsets() {
        let state = ReplicationState::new(4, generate_repl_id(), ZEROED_ID.to_string());
        assert_eq!(state.shard_offsets.len(), 4);
        for i in 0..4 {
            assert_eq!(state.shard_offset(i), 0);
        }
    }

    #[test]
    fn test_increment_shard_offset() {
        let state = ReplicationState::new(2, generate_repl_id(), ZEROED_ID.to_string());
        state.increment_shard_offset(0, 100);
        state.increment_shard_offset(1, 200);
        assert_eq!(state.shard_offset(0), 100);
        assert_eq!(state.shard_offset(1), 200);
    }

    #[test]
    fn test_total_offset() {
        let state = ReplicationState::new(3, generate_repl_id(), ZEROED_ID.to_string());
        state.increment_shard_offset(0, 50);
        state.increment_shard_offset(1, 75);
        state.increment_shard_offset(2, 25);
        assert_eq!(state.total_offset(), 150);
    }

    #[test]
    fn test_save_load_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let id1 = generate_repl_id();
        let id2 = generate_repl_id();
        save_replication_state(dir.path(), &id1, &id2).unwrap();
        let (loaded1, loaded2) = load_replication_state(dir.path());
        assert_eq!(loaded1, id1);
        assert_eq!(loaded2, id2);
    }

    #[test]
    fn test_load_generates_fresh_when_missing() {
        let dir = tempfile::tempdir().unwrap();
        let (id1, id2) = load_replication_state(dir.path());
        assert_eq!(id1.len(), 40);
        assert_eq!(id2, ZEROED_ID);
        // Should have been persisted
        let (id1b, id2b) = load_replication_state(dir.path());
        assert_eq!(id1b, id1, "reloading should return same ID");
        assert_eq!(id2b, id2);
    }

    #[test]
    fn test_load_zeroed_id2_when_only_id1_present() {
        let dir = tempfile::tempdir().unwrap();
        let id1 = generate_repl_id();
        // Write only id1 (no id2 line)
        std::fs::write(dir.path().join("replication.state"), format!("{}\n", id1)).unwrap();
        let (loaded1, loaded2) = load_replication_state(dir.path());
        assert_eq!(loaded1, id1);
        assert_eq!(loaded2, ZEROED_ID);
    }
}
