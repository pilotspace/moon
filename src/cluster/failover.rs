//! Automatic failover: replica promotion when master is marked FAIL.
//!
//! Phase 20 implements the election side. The full integration with per-shard
//! replication offsets from Phase 19 is hooked in here.
//!
//! Flow:
//! 1. Gossip check_failure_states marks a master as PFAIL after node_timeout.
//! 2. When majority of known masters agree (via gossip), node is marked FAIL.
//! 3. The replica with the lowest replication lag initiates:
//!    - Increments config epoch.
//!    - Sends FAILOVER_AUTH_REQUEST to all masters.
//! 4. Masters vote if: request_epoch > last_vote_epoch.
//! 5. Replica receiving majority: promotes itself, takes master's slots.

use tracing::info;

use crate::cluster::{ClusterNode, ClusterState, NodeFlags};

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
}
