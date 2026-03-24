use crate::replication::backlog::ReplicationBacklog;
use crate::replication::state::{ReplicationRole, ReplicationState};
use std::sync::atomic::Ordering;

/// The decision outcome of evaluating a PSYNC request.
#[derive(Debug, PartialEq)]
pub enum PsyncDecision {
    /// Full resync required: master sends RDB snapshots + backlog since snapshot start.
    FullResync,
    /// Partial resync possible: master streams backlog from from_offset to current.
    PartialResync { from_offset: u64 },
}

/// State machine for the PSYNC2 handshake on the REPLICA side.
#[derive(Debug, Clone)]
pub enum ReplicaHandshakeState {
    /// Waiting to send PING / receive PONG.
    PingPending,
    /// PING sent, awaiting PONG.
    PingSent,
    /// Sending REPLCONF listening-port.
    ReplConfPort,
    /// Sending REPLCONF capa psync2.
    ReplConfCapa,
    /// Sending PSYNC <repl_id> <offset>.
    PsyncPending,
    /// Received +FULLRESYNC, loading per-shard RDB snapshots.
    FullResyncLoading { shards_remaining: usize },
    /// Streaming: applying WAL bytes from master to local shards.
    Streaming,
    /// Disconnected / retrying.
    Disconnected,
}

impl ReplicaHandshakeState {
    pub fn new() -> Self {
        ReplicaHandshakeState::PingPending
    }
}

impl Default for ReplicaHandshakeState {
    fn default() -> Self {
        Self::new()
    }
}

/// Evaluate whether a replica's PSYNC request can use partial resync.
///
/// client_offset of -1 (or PSYNC ? -1) always forces full resync.
/// ID must match either repl_id (current) or repl_id2 (previous after failover).
/// ALL per-shard backlogs must contain the requested offset for partial resync.
pub fn evaluate_psync(
    client_repl_id: &str,
    client_offset: i64,
    server_repl_id: &str,
    server_repl_id2: &str,
    per_shard_backlogs: &[ReplicationBacklog],
) -> PsyncDecision {
    if client_offset < 0 {
        return PsyncDecision::FullResync;
    }
    let id_matches = client_repl_id == server_repl_id || client_repl_id == server_repl_id2;
    if !id_matches {
        return PsyncDecision::FullResync;
    }
    let offset = client_offset as u64;
    // Per-shard granularity: ALL backlogs must cover the offset.
    if per_shard_backlogs.iter().all(|b| b.contains_offset(offset)) {
        PsyncDecision::PartialResync { from_offset: offset }
    } else {
        PsyncDecision::FullResync
    }
}

/// Build the "# Replication" section of INFO output.
///
/// Compatible with Redis 7.x INFO replication format for redis-cli and client libraries.
pub fn build_info_replication(repl_state: &ReplicationState) -> String {
    let mut s = String::from("# Replication\r\n");
    match &repl_state.role {
        ReplicationRole::Master => {
            s.push_str("role:master\r\n");
            let num_replicas = repl_state.replicas.len();
            s.push_str(&format!("connected_slaves:{}\r\n", num_replicas));
            s.push_str("master_failover_state:no-failover\r\n");
            s.push_str(&format!("master_replid:{}\r\n", repl_state.repl_id));
            s.push_str(&format!("master_replid2:{}\r\n", repl_state.repl_id2));
            let offset = repl_state.master_repl_offset.load(Ordering::Relaxed);
            s.push_str(&format!("master_repl_offset:{}\r\n", offset));
            s.push_str("second_repl_offset:-1\r\n");
            s.push_str("repl_backlog_active:1\r\n");
            s.push_str("repl_backlog_size:1048576\r\n");
            s.push_str("repl_backlog_first_byte_offset:1\r\n");
            s.push_str(&format!("repl_backlog_histlen:{}\r\n", offset));
            for (i, replica) in repl_state.replicas.iter().enumerate() {
                let ack_offset: u64 = replica
                    .ack_offsets
                    .iter()
                    .map(|a| a.load(Ordering::Relaxed))
                    .sum();
                let lag = offset.saturating_sub(ack_offset);
                s.push_str(&format!(
                    "slave{}:ip={},port={},state=online,offset={},lag={}\r\n",
                    i,
                    replica.addr.ip(),
                    replica.addr.port(),
                    ack_offset,
                    lag
                ));
            }
        }
        ReplicationRole::Replica { host, port, .. } => {
            s.push_str("role:slave\r\n");
            s.push_str(&format!("master_host:{}\r\n", host));
            s.push_str(&format!("master_port:{}\r\n", port));
            s.push_str("master_link_status:up\r\n");
            s.push_str("master_last_io_seconds_ago:0\r\n");
            s.push_str("master_sync_in_progress:0\r\n");
            let offset = repl_state.master_repl_offset.load(Ordering::Relaxed);
            s.push_str(&format!("slave_repl_offset:{}\r\n", offset));
            s.push_str("slave_priority:100\r\n");
            s.push_str("slave_read_only:1\r\n");
            s.push_str("replica_announced:1\r\n");
            s.push_str("connected_slaves:0\r\n");
            s.push_str(&format!("master_replid:{}\r\n", repl_state.repl_id));
            s.push_str(&format!("master_replid2:{}\r\n", repl_state.repl_id2));
            s.push_str(&format!("master_repl_offset:{}\r\n", offset));
            s.push_str("second_repl_offset:-1\r\n");
            s.push_str("repl_backlog_active:0\r\n");
        }
    }
    s.push_str("\r\n");
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::state::generate_repl_id;

    fn make_backlogs(num_shards: usize, capacity: usize) -> Vec<ReplicationBacklog> {
        (0..num_shards)
            .map(|_| ReplicationBacklog::new(capacity))
            .collect()
    }

    #[test]
    fn test_evaluate_psync_initial_sync() {
        let backlogs = make_backlogs(2, 1024);
        let result = evaluate_psync("?", -1, "abc", "def", &backlogs);
        assert_eq!(result, PsyncDecision::FullResync);
    }

    #[test]
    fn test_evaluate_psync_negative_offset() {
        let id = generate_repl_id();
        let backlogs = make_backlogs(1, 1024);
        let result = evaluate_psync(&id, -1, &id, "0000", &backlogs);
        assert_eq!(result, PsyncDecision::FullResync);
    }

    #[test]
    fn test_evaluate_psync_id_mismatch() {
        let mut backlogs = make_backlogs(1, 1024);
        backlogs[0].append(b"data");
        let result = evaluate_psync("wrong_id_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 0, "server_id_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "server_id2_aaaaaaaaaaaaaaaaaaaaaaaaaaaaa", &backlogs);
        assert_eq!(result, PsyncDecision::FullResync);
    }

    #[test]
    fn test_evaluate_psync_partial_resync_repl_id_match() {
        let id = generate_repl_id();
        let mut backlogs = make_backlogs(2, 1024);
        backlogs[0].append(b"hello");
        backlogs[1].append(b"world!");
        // Both backlogs contain offset 0
        let result = evaluate_psync(&id, 0, &id, "0000", &backlogs);
        assert_eq!(result, PsyncDecision::PartialResync { from_offset: 0 });
    }

    #[test]
    fn test_evaluate_psync_partial_resync_repl_id2_match() {
        let id1 = generate_repl_id();
        let id2 = generate_repl_id();
        let mut backlogs = make_backlogs(1, 1024);
        backlogs[0].append(b"some data");
        // client sends id2 (failover scenario)
        let result = evaluate_psync(&id2, 0, &id1, &id2, &backlogs);
        assert_eq!(result, PsyncDecision::PartialResync { from_offset: 0 });
    }

    #[test]
    fn test_evaluate_psync_offset_evicted_any_shard() {
        let id = generate_repl_id();
        let mut backlogs = make_backlogs(2, 4);
        // Shard 0: append enough to evict offset 0
        backlogs[0].append(b"abcdef"); // capacity 4 -> start=2, end=6
        // Shard 1: has offset 0 available
        backlogs[1].append(b"ab"); // start=0, end=2

        // Offset 0 evicted from shard 0 -> full resync
        let result = evaluate_psync(&id, 0, &id, "0000", &backlogs);
        assert_eq!(result, PsyncDecision::FullResync);
    }

    #[test]
    fn test_evaluate_psync_offset_within_all_backlogs() {
        let id = generate_repl_id();
        let mut backlogs = make_backlogs(2, 16);
        backlogs[0].append(b"abcdefgh"); // start=0, end=8
        backlogs[1].append(b"12345678"); // start=0, end=8
        // Offset 4 is within both backlogs
        let result = evaluate_psync(&id, 4, &id, "0000", &backlogs);
        assert_eq!(result, PsyncDecision::PartialResync { from_offset: 4 });
    }

    #[test]
    fn test_replica_handshake_state_new() {
        let state = ReplicaHandshakeState::new();
        assert!(matches!(state, ReplicaHandshakeState::PingPending));
    }

    #[test]
    fn test_build_info_replication_master() {
        let state = ReplicationState::new(2, "a".repeat(40), "b".repeat(40));
        let info = build_info_replication(&state);
        assert!(info.contains("role:master\r\n"));
        assert!(info.contains("connected_slaves:0\r\n"));
        assert!(info.contains(&format!("master_replid:{}\r\n", "a".repeat(40))));
        assert!(info.contains(&format!("master_replid2:{}\r\n", "b".repeat(40))));
        assert!(info.contains("master_repl_offset:0\r\n"));
    }

    #[test]
    fn test_build_info_replication_replica() {
        let mut state = ReplicationState::new(1, "c".repeat(40), "d".repeat(40));
        state.role = ReplicationRole::Replica {
            host: "127.0.0.1".to_string(),
            port: 6379,
            state: ReplicaHandshakeState::Streaming,
        };
        let info = build_info_replication(&state);
        assert!(info.contains("role:slave\r\n"));
        assert!(info.contains("master_host:127.0.0.1\r\n"));
        assert!(info.contains("master_port:6379\r\n"));
        assert!(info.contains(&format!("master_replid:{}\r\n", "c".repeat(40))));
    }
}
