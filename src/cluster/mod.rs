//! Redis Cluster state: node membership, slot ownership, routing decisions.
//!
//! ClusterState is wrapped in Option<Arc<RwLock<ClusterState>>> everywhere.
//! When None, all cluster routing is skipped with zero overhead.

pub mod bus;
pub mod command;
pub mod failover;
pub mod gossip;
pub mod migration;
pub mod slots;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;

use crate::protocol::Frame;
use slots::{ask_error_msg, moved_error_msg};

// --- Node flags ------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub enum NodeFlags {
    Master,
    Replica { master_id: String },
    Pfail,
    Fail,
}

// --- Cluster-level status --------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub enum ClusterStatus {
    Ok,
    Fail,
}

// --- ClusterNode -----------------------------------------------------------------

/// A single known node in the cluster (may be self or a peer).
#[derive(Clone, Debug)]
pub struct ClusterNode {
    /// 40-char lowercase hex node ID (same format as replication ID).
    pub node_id: String,
    /// Data-plane socket address (ip:port).
    pub addr: SocketAddr,
    /// Cluster bus port = addr.port() + 10000.
    pub bus_port: u16,
    pub flags: NodeFlags,
    /// Config epoch for this node's slot assignment.
    pub epoch: u64,
    /// Unix millis when we last sent PING to this node.
    pub ping_sent_ms: u64,
    /// Unix millis when we last received PONG from this node.
    pub pong_recv_ms: u64,
    /// Slot ownership bitmap: bit i set means this node owns slot i.
    /// 16384 bits = 2048 bytes.
    pub slots: Box<[u8; 2048]>,
    /// Maps reporter_node_id -> report_time_ms. Tracks which masters have
    /// reported this node as PFAIL via gossip.
    pub pfail_reports: HashMap<String, u64>,
}

impl ClusterNode {
    pub fn new(node_id: String, addr: SocketAddr, flags: NodeFlags, epoch: u64) -> Self {
        // Redis convention: bus_port = port + 10000. Guard overflow for test
        // servers on ephemeral ports (49152-65535) where port + 10000 > u16::MAX.
        let bus_port = addr.port().checked_add(10000).unwrap_or_else(|| {
            // Wrap into valid range for test compatibility
            ((addr.port() as u32 + 10000) % 65536) as u16
        });
        ClusterNode {
            node_id,
            addr,
            bus_port,
            flags,
            epoch,
            ping_sent_ms: 0,
            pong_recv_ms: 0,
            slots: Box::new([0u8; 2048]),
            pfail_reports: HashMap::new(),
        }
    }

    #[inline]
    pub fn owns_slot(&self, slot: u16) -> bool {
        let byte = slot as usize / 8;
        let bit = slot as usize % 8;
        self.slots[byte] & (1 << bit) != 0
    }

    #[inline]
    pub fn set_slot(&mut self, slot: u16) {
        let byte = slot as usize / 8;
        let bit = slot as usize % 8;
        self.slots[byte] |= 1 << bit;
    }

    #[inline]
    pub fn clear_slot(&mut self, slot: u16) {
        let byte = slot as usize / 8;
        let bit = slot as usize % 8;
        self.slots[byte] &= !(1 << bit);
    }

    /// Count total slots owned by this node.
    pub fn slot_count(&self) -> usize {
        self.slots.iter().map(|b| b.count_ones() as usize).sum()
    }
}

// --- SlotRoute -------------------------------------------------------------------

/// Result of a slot routing decision for one key.
#[derive(Debug, PartialEq)]
pub enum SlotRoute {
    /// Slot belongs to this node -- proceed to dispatch.
    Local,
    /// Slot is permanently owned by another node: send MOVED.
    Moved { host: String, port: u16 },
    /// Slot is being migrated to another node: send ASK.
    Ask { host: String, port: u16 },
    /// Multi-key command with keys spanning different slots.
    CrossSlot,
}

impl SlotRoute {
    /// Convert this route decision into a Frame::Error for the client.
    pub fn into_error_frame(self, slot: u16) -> Frame {
        match self {
            SlotRoute::Moved { ref host, port } => {
                Frame::Error(Bytes::from(moved_error_msg(slot, host, port)))
            }
            SlotRoute::Ask { ref host, port } => {
                Frame::Error(Bytes::from(ask_error_msg(slot, host, port)))
            }
            SlotRoute::CrossSlot => Frame::Error(Bytes::from_static(
                b"CROSSSLOT Keys in request don't hash to the same slot",
            )),
            SlotRoute::Local => unreachable!("Local routes do not produce error frames"),
        }
    }
}

// --- FailoverState ---------------------------------------------------------------

/// Tracks the progress of a failover election for this replica.
#[derive(Clone, Debug, PartialEq)]
pub enum FailoverState {
    None,
    WaitingDelay {
        start_ms: u64,
        delay_ms: u64,
    },
    WaitingVotes {
        epoch: u64,
        votes_received: u32,
        votes_needed: u32,
    },
}

// --- ClusterState ----------------------------------------------------------------

/// Full cluster state for this node.
///
/// Wrapped in `Option<Arc<RwLock<ClusterState>>>` in shard event loops.
/// When the outer Option is None, cluster mode is disabled and all routing
/// is bypassed with a single branch check.
pub struct ClusterState {
    /// This node's 40-char hex ID.
    pub node_id: String,
    /// Config epoch -- increments on failover or topology change. Persisted to nodes.conf.
    pub epoch: u64,
    /// Overall cluster status.
    pub status: ClusterStatus,
    /// All known nodes keyed by node_id (includes self).
    pub nodes: HashMap<String, ClusterNode>,
    /// Slots currently being IMPORTED on this node (slot -> source node_id).
    /// Target node accepts ASK-redirected commands for these slots.
    pub importing: HashMap<u16, String>,
    /// Slots currently being MIGRATED from this node (slot -> target node_id).
    /// Source node redirects key-not-found to target with ASK.
    pub migrating: HashMap<u16, String>,
    /// Gossip message counters for CLUSTER INFO.
    pub messages_sent: u64,
    pub messages_received: u64,
    /// Epoch of the last vote cast during failover (prevents double-voting).
    pub last_vote_epoch: u64,
    /// Current failover election state (only relevant for replicas).
    pub failover_state: FailoverState,
}

impl ClusterState {
    pub fn new(node_id: String, self_addr: SocketAddr) -> Self {
        let mut state = ClusterState {
            node_id: node_id.clone(),
            epoch: 0,
            status: ClusterStatus::Ok,
            nodes: HashMap::new(),
            importing: HashMap::new(),
            migrating: HashMap::new(),
            messages_sent: 0,
            messages_received: 0,
            last_vote_epoch: 0,
            failover_state: FailoverState::None,
        };
        let self_node = ClusterNode::new(node_id.clone(), self_addr, NodeFlags::Master, 0);
        state.nodes.insert(node_id, self_node);
        state
    }

    /// Return a reference to this node's own ClusterNode entry.
    pub fn my_node(&self) -> &ClusterNode {
        self.nodes
            .get(&self.node_id)
            .expect("self node always present")
    }

    /// Return a mutable reference to this node's own ClusterNode entry.
    pub fn my_node_mut(&mut self) -> &mut ClusterNode {
        self.nodes
            .get_mut(&self.node_id)
            .expect("self node always present")
    }

    /// Find which node owns a given slot (None if unconfigured).
    pub fn slot_owner(&self, slot: u16) -> Option<&ClusterNode> {
        self.nodes.values().find(|n| n.owns_slot(slot))
    }

    /// Determine how to route a command for a given slot.
    ///
    /// `asking` is the per-connection ASKING flag (consumed by caller after this call).
    pub fn route_slot(&self, slot: u16, asking: bool) -> SlotRoute {
        // If client sent ASKING and this slot is IMPORTING, allow it through.
        if asking && self.importing.contains_key(&slot) {
            return SlotRoute::Local;
        }

        let my_node = self.my_node();

        if my_node.owns_slot(slot) {
            // We own the slot. Check if it's being migrated away (key-miss -> ASK).
            if let Some(target_id) = self.migrating.get(&slot) {
                if let Some(target) = self.nodes.get(target_id) {
                    return SlotRoute::Ask {
                        host: target.addr.ip().to_string(),
                        port: target.addr.port(),
                    };
                }
            }
            return SlotRoute::Local;
        }

        // We do not own the slot. Find the actual owner.
        if let Some(owner) = self.slot_owner(slot) {
            return SlotRoute::Moved {
                host: owner.addr.ip().to_string(),
                port: owner.addr.port(),
            };
        }

        // Unconfigured slot -- treat as local (single-node setup / initial bootstrap).
        SlotRoute::Local
    }

    /// How many slots total are assigned to any node in this cluster.
    pub fn assigned_slot_count(&self) -> usize {
        self.nodes.values().map(|n| n.slot_count()).sum()
    }

    /// Quorum: strict majority of masters required for consensus decisions.
    pub fn quorum(&self) -> u32 {
        let masters = self.master_count() as u32;
        masters / 2 + 1
    }

    /// How many distinct master nodes are in the cluster.
    pub fn master_count(&self) -> usize {
        self.nodes
            .values()
            .filter(|n| matches!(n.flags, NodeFlags::Master))
            .count()
    }
}

// --- AtomicBool gate -------------------------------------------------------------

/// Global flag: is cluster mode enabled?
///
/// Checked on every command before acquiring the RwLock.
/// Avoids any locking overhead when cluster mode is disabled.
pub static CLUSTER_ENABLED: AtomicBool = AtomicBool::new(false);

#[inline]
pub fn cluster_enabled() -> bool {
    CLUSTER_ENABLED.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn test_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
    }

    fn make_state_with_two_nodes() -> ClusterState {
        let my_id = "a".repeat(40);
        let peer_id = "b".repeat(40);
        let mut state = ClusterState::new(my_id.clone(), test_addr(6379));

        let mut peer = ClusterNode::new(peer_id.clone(), test_addr(6380), NodeFlags::Master, 0);
        // Peer owns slots 8193-16383
        for slot in 8193..=16383u16 {
            peer.set_slot(slot);
        }
        state.nodes.insert(peer_id, peer);

        // Self owns slots 0-8192
        let my_node = state.my_node_mut();
        for slot in 0..=8192u16 {
            my_node.set_slot(slot);
        }
        state
    }

    #[test]
    fn test_owns_slot_bitmap() {
        let addr = test_addr(6379);
        let mut node = ClusterNode::new("a".repeat(40), addr, NodeFlags::Master, 0);
        assert!(!node.owns_slot(0));
        node.set_slot(0);
        assert!(node.owns_slot(0));
        node.clear_slot(0);
        assert!(!node.owns_slot(0));

        node.set_slot(16383);
        assert!(node.owns_slot(16383));
        assert!(!node.owns_slot(16382));
    }

    #[test]
    fn test_route_local_owned_slot() {
        let state = make_state_with_two_nodes();
        assert_eq!(state.route_slot(0, false), SlotRoute::Local);
        assert_eq!(state.route_slot(8192, false), SlotRoute::Local);
    }

    #[test]
    fn test_route_moved_for_peer_slot() {
        let state = make_state_with_two_nodes();
        let route = state.route_slot(8193, false);
        match route {
            SlotRoute::Moved { host, port } => {
                assert_eq!(host, "127.0.0.1");
                assert_eq!(port, 6380);
            }
            other => panic!("expected Moved, got {:?}", other),
        }
    }

    /// CLUSTER-04: MOVED error format.
    #[test]
    fn test_moved_error_frame_format() {
        let state = make_state_with_two_nodes();
        let route = state.route_slot(8193, false);
        let frame = route.into_error_frame(8193);
        match frame {
            Frame::Error(msg) => {
                let s = std::str::from_utf8(&msg).unwrap();
                assert_eq!(s, "MOVED 8193 127.0.0.1:6380");
            }
            _ => panic!("expected Error frame"),
        }
    }

    /// CLUSTER-05: ASKING + IMPORTING allows local execution.
    #[test]
    fn test_asking_flag_with_importing_slot() {
        let mut state = make_state_with_two_nodes();
        // Slot 8193 is owned by peer. Mark it as IMPORTING (we are the target).
        state.importing.insert(8193, "b".repeat(40));

        // Without ASKING: still Moved (we don't own 8193)
        let route_no_ask = state.route_slot(8193, false);
        assert!(matches!(route_no_ask, SlotRoute::Moved { .. }));

        // With ASKING: Local (ASKING + IMPORTING lets it through)
        let route_with_ask = state.route_slot(8193, true);
        assert_eq!(route_with_ask, SlotRoute::Local);
    }

    /// ASKING with non-importing slot still returns Moved.
    #[test]
    fn test_asking_without_importing_still_moved() {
        let state = make_state_with_two_nodes();
        // Slot 8193 is peer's but NOT in our importing map.
        let route = state.route_slot(8193, true);
        assert!(matches!(route, SlotRoute::Moved { .. }));
    }

    #[test]
    fn test_my_node_id() {
        let my_id = "a".repeat(40);
        let state = ClusterState::new(my_id.clone(), test_addr(6379));
        assert_eq!(state.my_node().node_id, my_id);
    }
}
