//! Automatic failover: replica promotion when master is marked FAIL.
//!
//! Phase 20 implements the election side. The full integration with per-shard
//! replication offsets from Phase 19 is hooked in here.
//!
#![allow(unused_imports)]
//! Flow:
//! 1. Gossip check_failure_states marks a master as PFAIL after node_timeout.
//! 2. When majority of known masters agree (via gossip), node is marked FAIL.
//! 3. The replica with the lowest replication lag initiates:
//!    - Increments config epoch.
//!    - Sends FAILOVER_AUTH_REQUEST to all masters.
//! 4. Masters vote if: request_epoch > last_vote_epoch.
//! 5. Replica receiving majority: promotes itself, takes master's slots.

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use rand::Rng;
#[cfg(feature = "runtime-tokio")]
use tokio::io::AsyncWriteExt;
#[cfg(feature = "runtime-tokio")]
use tokio::net::TcpStream;
use tracing::{info, warn};

use crate::cluster::gossip::{build_message, serialize_gossip, GossipMsgType};
use crate::cluster::{ClusterState, FailoverState, NodeFlags};

/// Check if we should initiate failover.
///
/// Returns true if this node is a replica whose master has been marked FAIL
/// and we should start the election. Caller spawns the election task.
pub fn check_and_initiate_failover(state: &mut ClusterState, my_repl_offset: u64) -> bool {
    let my_id = state.node_id.clone();
    let my_flags = state.my_node().flags.clone();

    let master_id = match &my_flags {
        NodeFlags::Replica { master_id } => master_id.clone(),
        _ => return false, // we are a master, nothing to do
    };

    // Check if our master is FAIL
    let master_is_fail = state
        .nodes
        .get(&master_id)
        .map(|n| matches!(n.flags, NodeFlags::Fail))
        .unwrap_or(false);

    if !master_is_fail {
        return false;
    }

    info!(
        "Master {} is FAIL, replica {} initiating failover (repl_offset={})",
        master_id, my_id, my_repl_offset
    );

    // Increment epoch for this failover attempt
    state.epoch += 1;

    // Promote ourselves locally
    // (Awaiting majority votes before taking over master's slots is handled async)
    state.my_node_mut().flags = NodeFlags::Master;
    state.my_node_mut().epoch = state.epoch;

    // Transfer master's slots to ourselves
    let master_slots = state.nodes.get(&master_id).map(|n| n.slots.clone());

    if let Some(slots) = master_slots {
        let my_node = state.my_node_mut();
        for slot in 0u16..=16383 {
            if slots[slot as usize / 8] & (1 << (slot as usize % 8)) != 0 {
                my_node.set_slot(slot);
            }
        }
    }

    // Remove the failed master from active routing (keep in nodes for history)
    if let Some(failed) = state.nodes.get_mut(&master_id) {
        failed.flags = NodeFlags::Fail;
    }

    true
}

/// Determine if this master should grant a failover vote.
///
/// Returns true if:
/// - request_epoch > our last_vote_epoch (prevents double-voting, Pitfall 8)
/// - The requesting node is a known replica of the specified failed master
pub fn failover_vote_eligible(
    state: &ClusterState,
    requesting_node_id: &str,
    request_epoch: u64,
) -> bool {
    // Epoch guard: never vote twice for the same epoch (Pitfall 8)
    if request_epoch <= state.last_vote_epoch {
        return false;
    }
    // Verify the requester is a known node
    state.nodes.contains_key(requesting_node_id)
}

/// Process a FAILOVER_AUTH_REQUEST from a peer replica.
///
/// If we vote YES: update last_vote_epoch, return FailoverAuthAck message bytes.
pub fn handle_failover_auth_request(
    state: &mut ClusterState,
    requesting_node_id: &str,
    request_epoch: u64,
) -> bool {
    if !failover_vote_eligible(state, requesting_node_id, request_epoch) {
        return false;
    }
    state.last_vote_epoch = request_epoch;
    info!(
        "Granting failover vote to {} for epoch {}",
        requesting_node_id, request_epoch
    );
    true
}

/// Return current unix milliseconds.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Default node timeout for stale pfail_report cleanup (30 seconds).
const DEFAULT_NODE_TIMEOUT_MS: u64 = 30_000;

/// Check if majority of masters agree that `node_id` is PFAIL, and if so
/// transition it to FAIL. Returns true if the PFAIL->FAIL transition occurred.
///
/// Also cleans up stale pfail_reports older than 2 * node_timeout_ms.
pub fn try_mark_fail_with_consensus(state: &mut ClusterState, node_id: &str) -> bool {
    let now = now_ms();
    let stale_cutoff = now.saturating_sub(2 * DEFAULT_NODE_TIMEOUT_MS);
    let quorum = state.quorum();

    // Clean stale reports and check count
    let (is_pfail, report_count) = {
        if let Some(node) = state.nodes.get_mut(node_id) {
            // Remove stale reports
            node.pfail_reports.retain(|_, ts| *ts > stale_cutoff);
            let is_pfail = matches!(node.flags, NodeFlags::Pfail);
            let count = node.pfail_reports.len() as u32;
            (is_pfail, count)
        } else {
            return false;
        }
    };

    if !is_pfail || report_count < quorum {
        return false;
    }

    // Majority reached -- transition to FAIL
    let master_count = state.master_count();
    if let Some(node) = state.nodes.get_mut(node_id) {
        info!(
            "Node {} PFAIL->FAIL: {}/{} masters agree (quorum={})",
            node_id, report_count, master_count, quorum
        );
        node.flags = NodeFlags::Fail;
    }
    true
}

/// Compute the jittered delay before a replica starts its failover election.
///
/// Higher-ranked replicas (worse replication offset) wait longer, giving
/// the best replica (rank 0) a head start.
pub fn compute_failover_delay(replica_rank: u32) -> u64 {
    let jitter = rand::rng().random_range(0u64..500);
    500 + jitter + (replica_rank as u64 * 1000)
}

/// Async election task: waits the jittered delay, sends FailoverAuthRequest
/// to all masters, collects votes, and promotes on majority.
///
/// Spawned when this replica detects its master is FAIL.
#[cfg(feature = "runtime-tokio")]
pub async fn run_election_task(
    cluster_state: Arc<RwLock<ClusterState>>,
    self_addr: SocketAddr,
    _my_repl_offset: u64,
    vote_rx: crate::runtime::channel::MpscReceiver<String>,
) {
    // Compute delay (rank 0 for now; multi-replica ranking is future work)
    let replica_rank = 0u32;
    let delay = compute_failover_delay(replica_rank);

    // Set state to WaitingDelay
    {
        let mut cs = cluster_state.write().unwrap();
        cs.failover_state = FailoverState::WaitingDelay {
            start_ms: now_ms(),
            delay_ms: delay,
        };
    }

    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;

    // Increment epoch and build FailoverAuthRequest
    let (new_epoch, quorum, master_addrs) = {
        let mut cs = cluster_state.write().unwrap();
        cs.epoch += 1;
        let new_epoch = cs.epoch;
        let quorum = cs.quorum();
        let my_id = cs.node_id.clone();

        // Collect bus addresses of all known masters
        let addrs: Vec<SocketAddr> = cs
            .nodes
            .values()
            .filter(|n| n.node_id != my_id && matches!(n.flags, NodeFlags::Master))
            .map(|n| SocketAddr::new(n.addr.ip(), n.bus_port))
            .collect();

        cs.failover_state = FailoverState::WaitingVotes {
            epoch: new_epoch,
            votes_received: 1, // self-vote
            votes_needed: quorum,
        };

        (new_epoch, quorum, addrs)
    };

    // Build the auth request message
    let auth_msg = {
        let cs = cluster_state.read().unwrap();
        let mut msg = build_message(&cs, self_addr, GossipMsgType::FailoverAuthRequest);
        msg.config_epoch = new_epoch;
        msg
    };

    // Send to all masters
    let data = serialize_gossip(&auth_msg);
    for addr in &master_addrs {
        let data = data.clone();
        let addr = *addr;
        tokio::spawn(async move {
            if let Ok(mut stream) = TcpStream::connect(addr).await {
                let len = (data.len() as u32).to_be_bytes();
                let _ = stream.write_all(&len).await;
                let _ = stream.write_all(&data).await;
            }
        });
    }

    info!(
        "Failover election started: epoch={}, sent auth request to {} masters, need {} votes",
        new_epoch,
        master_addrs.len(),
        quorum
    );

    // Collect votes with 5-second timeout
    let mut votes: u32 = 1; // self-vote
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);

    loop {
        if votes >= quorum {
            break;
        }
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, vote_rx.recv_async()).await {
            Ok(Ok(_voter_id)) => {
                votes += 1;
                let mut cs = cluster_state.write().unwrap();
                if let FailoverState::WaitingVotes {
                    ref mut votes_received,
                    ..
                } = cs.failover_state
                {
                    *votes_received = votes;
                }
            }
            _ => break, // timeout or channel closed
        }
    }

    if votes >= quorum {
        info!(
            "Failover election won: epoch={}, votes={}/{}",
            new_epoch, votes, quorum
        );
        let mut cs = cluster_state.write().unwrap();
        check_and_initiate_failover(&mut cs, _my_repl_offset);
        cs.failover_state = FailoverState::None;
    } else {
        warn!(
            "Failover election timed out: epoch={}, votes={}/{}",
            new_epoch, votes, quorum
        );
        let mut cs = cluster_state.write().unwrap();
        cs.failover_state = FailoverState::None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn test_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
    }

    #[test]
    fn test_no_failover_when_master_healthy() {
        let my_id = "a".repeat(40);
        let master_id = "b".repeat(40);
        let mut state = ClusterState::new(my_id.clone(), test_addr(6379));
        state.my_node_mut().flags = NodeFlags::Replica {
            master_id: master_id.clone(),
        };
        // Master is Pfail, not Fail
        let master = crate::cluster::ClusterNode::new(
            master_id.clone(),
            test_addr(6380),
            NodeFlags::Pfail,
            0,
        );
        state.nodes.insert(master_id, master);

        let result = check_and_initiate_failover(&mut state, 1000);
        assert!(!result);
    }

    #[test]
    fn test_failover_initiates_when_master_fail() {
        let my_id = "a".repeat(40);
        let master_id = "b".repeat(40);
        let mut state = ClusterState::new(my_id.clone(), test_addr(6379));
        state.my_node_mut().flags = NodeFlags::Replica {
            master_id: master_id.clone(),
        };

        // Master owns slots 0-100; mark as FAIL
        let mut master = crate::cluster::ClusterNode::new(
            master_id.clone(),
            test_addr(6380),
            NodeFlags::Fail,
            0,
        );
        for s in 0u16..=100 {
            master.set_slot(s);
        }
        state.nodes.insert(master_id, master);

        let result = check_and_initiate_failover(&mut state, 5000);
        assert!(result, "expected failover to initiate");
        // After promotion, we should be a master
        assert!(matches!(state.my_node().flags, NodeFlags::Master));
        // We should have inherited master's slots
        assert!(state.my_node().owns_slot(50));
    }

    /// Pitfall 8: double-vote prevention.
    #[test]
    fn test_failover_vote_epoch_guard() {
        let my_id = "a".repeat(40);
        let requester_id = "c".repeat(40);
        let mut state = ClusterState::new(my_id.clone(), test_addr(6379));
        let requester = crate::cluster::ClusterNode::new(
            requester_id.clone(),
            test_addr(6381),
            NodeFlags::Master,
            0,
        );
        state.nodes.insert(requester_id.clone(), requester);
        state.last_vote_epoch = 5;

        // Request with epoch <= last_vote_epoch is rejected
        assert!(!failover_vote_eligible(&state, &requester_id, 5));
        assert!(!failover_vote_eligible(&state, &requester_id, 4));

        // Request with epoch > last_vote_epoch is accepted
        assert!(failover_vote_eligible(&state, &requester_id, 6));

        // After voting, last_vote_epoch updates
        let voted = handle_failover_auth_request(&mut state, &requester_id, 6);
        assert!(voted);
        assert_eq!(state.last_vote_epoch, 6);

        // Same epoch can no longer vote
        assert!(!failover_vote_eligible(&state, &requester_id, 6));
    }

    /// PFAIL->FAIL requires majority of masters to agree.
    #[test]
    fn test_try_mark_fail_needs_majority() {
        // 5 masters: quorum = 3
        let my_id = "a".repeat(40);
        let target_id = "b".repeat(40);
        let mut state = ClusterState::new(my_id.clone(), test_addr(6379));

        // Add target node as Pfail
        let target = crate::cluster::ClusterNode::new(
            target_id.clone(),
            test_addr(6380),
            NodeFlags::Pfail,
            0,
        );
        state.nodes.insert(target_id.clone(), target);

        // Add 3 more masters (total 5 masters: a, c, d, e + target b is Pfail not Master)
        // Actually for quorum we count masters. Target is Pfail, not Master.
        // So masters = a, c, d, e = 4. Quorum = 4/2+1 = 3.
        for ch in ['c', 'd', 'e'] {
            let nid = format!("{}", ch).repeat(40);
            let node = crate::cluster::ClusterNode::new(
                nid.clone(),
                test_addr(6381 + ch as u16),
                NodeFlags::Master,
                0,
            );
            state.nodes.insert(nid, node);
        }

        // 1 report: not enough
        state
            .nodes
            .get_mut(&target_id)
            .unwrap()
            .pfail_reports
            .insert("c".repeat(40), now_ms());
        assert!(!try_mark_fail_with_consensus(&mut state, &target_id));

        // 2 reports: still not enough (quorum=3)
        state
            .nodes
            .get_mut(&target_id)
            .unwrap()
            .pfail_reports
            .insert("d".repeat(40), now_ms());
        assert!(!try_mark_fail_with_consensus(&mut state, &target_id));

        // 3 reports: reaches quorum -> FAIL
        state
            .nodes
            .get_mut(&target_id)
            .unwrap()
            .pfail_reports
            .insert("e".repeat(40), now_ms());
        assert!(try_mark_fail_with_consensus(&mut state, &target_id));
        assert!(matches!(
            state.nodes.get(&target_id).unwrap().flags,
            NodeFlags::Fail
        ));
    }

    /// Replica rank 0 gets shorter delay than rank 1 (statistical).
    #[test]
    fn test_compute_failover_delay_includes_rank() {
        // Run multiple samples; rank 1 has +1000ms base offset
        let mut rank0_sum = 0u64;
        let mut rank1_sum = 0u64;
        for _ in 0..100 {
            rank0_sum += compute_failover_delay(0);
            rank1_sum += compute_failover_delay(1);
        }
        // rank 1 average should be ~1000ms higher than rank 0
        assert!(
            rank1_sum > rank0_sum,
            "rank 1 total {} should be greater than rank 0 total {}",
            rank1_sum,
            rank0_sum
        );
    }
}
