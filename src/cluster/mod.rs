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

/// What a node IS. Independent of whether it is reachable.
#[derive(Clone, Debug, PartialEq)]
pub enum NodeRole {
    Master,
    Replica { master_id: String },
}

/// How a node is DOING. Independent of what it is.
///
/// Split out of the old single `NodeFlags` enum, whose `Master` / `Replica` /
/// `Pfail` / `Fail` variants were mutually exclusive: marking a node PFAIL
/// destroyed its role, and for a replica destroyed the `master_id` saying which
/// shard it belongs to. Redis treats these as orthogonal, which is why a dead
/// master is reported as `role: master, health: fail` — a shape the old type
/// could not express at all. Measured against redis-server 8.6.1.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeHealth {
    Online,
    Pfail,
    Fail,
}

// --- Cluster-level status --------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClusterStatus {
    Ok,
    Fail,
}

/// Cluster-wide slot health, counted per slot rather than per node.
///
/// Every field answers a question `CLUSTER INFO` asks, and each was a constant
/// before: `ok` was reported as "however many slots are assigned" and `pfail` /
/// `fail` were the literals `0`, so a cluster that had lost a master still
/// reported every slot healthy.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SlotCoverage {
    /// Slots claimed by some node, whatever its health.
    pub assigned: usize,
    /// Slots whose owner is `Online`.
    pub ok: usize,
    /// Slots whose owner is suspected down (`Pfail`) but not yet confirmed.
    pub pfail: usize,
    /// Slots whose owner is confirmed `Fail`.
    pub fail: usize,
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
    /// What this node is. Survives every health transition.
    pub role: NodeRole,
    /// How this node is doing. Survives every role transition.
    pub health: NodeHealth,
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
    pub fn new(node_id: String, addr: SocketAddr, role: NodeRole, epoch: u64) -> Self {
        // Redis convention: bus_port = port + 10000. Guard overflow for test
        // servers on ephemeral ports (49152-65535) where port + 10000 > u16::MAX.
        let bus_port = addr.port().checked_add(10000).unwrap_or_else(|| {
            // Wrap into valid range for test compatibility (ephemeral-port
            // test clusters). Our OWN port is refused at startup when
            // port+10000 > 65535 (main.rs), so a wrap here means a PEER
            // announced such a port — its bus is almost certainly not
            // reachable at the wrapped value (deep-review A5): say so.
            let wrapped = ((addr.port() as u32 + 10000) % 65536) as u16;
            tracing::warn!(
                "cluster node {} announces client port {} whose bus port wraps to {} — \
                 gossip to this peer will likely fail (peer ports must be <= 55535)",
                node_id,
                addr.port(),
                wrapped
            );
            wrapped
        });
        ClusterNode {
            node_id,
            addr,
            bus_port,
            role,
            health: NodeHealth::Online,
            epoch,
            ping_sent_ms: 0,
            pong_recv_ms: 0,
            slots: Box::new([0u8; 2048]),
            pfail_reports: HashMap::new(),
        }
    }

    /// Is this node a master? Unaffected by health, unlike the old flags enum.
    #[inline]
    pub fn is_master(&self) -> bool {
        matches!(self.role, NodeRole::Master)
    }

    /// The master this node follows, if it is a replica.
    #[inline]
    pub fn master_id(&self) -> Option<&str> {
        match &self.role {
            NodeRole::Replica { master_id } => Some(master_id.as_str()),
            NodeRole::Master => None,
        }
    }

    /// Confirmed failed. `Pfail` is a suspicion and deliberately does NOT
    /// count here — only consensus promotes it.
    #[inline]
    pub fn is_failed(&self) -> bool {
        self.health == NodeHealth::Fail
    }

    /// Suspected or confirmed down — i.e. not serving.
    #[inline]
    pub fn is_down(&self) -> bool {
        matches!(self.health, NodeHealth::Pfail | NodeHealth::Fail)
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
    /// No node in a FORMED cluster claims this slot, so there is nobody to
    /// redirect to and nobody who can answer. Serving it locally would hand the
    /// client a partial view of the keyspace it cannot detect.
    ///
    /// Distinct from the cluster-wide fail-closed state: Redis has TWO
    /// CLUSTERDOWN messages and this is the per-slot one. See
    /// `into_error_frame`.
    SlotNotServed,
    /// `cluster_state` is `fail`, so this node refuses ALL keyspace traffic —
    /// including keys in slots it owns and could serve.
    ///
    /// That looks like over-refusal and is deliberate, measured against Redis:
    /// a partially-visible keyspace is worse than a refusal, because a client
    /// cannot tell which half of the data it is seeing.
    ClusterDown,
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
            // Redis has TWO CLUSTERDOWN messages and they are not
            // interchangeable. This is the PER-SLOT one: the slot has no owner,
            // so there is nobody to redirect to. `CLUSTERDOWN The cluster is
            // down` is the CLUSTER-WIDE one, sent when cluster_state is fail,
            // and belongs to the fail-closed gate rather than here.
            //
            // Measured against redis-server (3 masters,
            // cluster-require-full-coverage no, CLUSTER DELSLOTS 12182 on the
            // owner, then GET/SET of a key hashing to 12182 on that node):
            //   CLUSTERDOWN Hash slot not served
            // A peer that still believed the old owner answered MOVED, which is
            // why this must be the ex-owner's own reply and not a redirect.
            SlotRoute::SlotNotServed => {
                Frame::Error(Bytes::from_static(b"CLUSTERDOWN Hash slot not served"))
            }
            // The CLUSTER-WIDE message, the counterpart to the per-slot one
            // above. Sent when cluster_state is fail even for a slot this node
            // owns; a client distinguishes the two texts.
            SlotRoute::ClusterDown => {
                Frame::Error(Bytes::from_static(b"CLUSTERDOWN The cluster is down"))
            }
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
    // NOTE: there is deliberately no `status` field. It existed, was assigned
    // exactly once in `new()`, and was never written again — so `cluster_state`
    // reported the constant `ok` even after a master died. `cluster_state` is
    // now derived on read by `cluster_status()`, which cannot go stale.
    /// Hot-path mirror of "is `cluster_status()` currently `Fail`?".
    ///
    /// `cluster_status()` remains the authority and every client-visible answer
    /// is computed from it. This exists only because the fail-closed gate sits
    /// on the keyspace path, where recomputing coverage would mean ORing every
    /// node's 2048-byte bitmap on every single command.
    ///
    /// It is a cache, not the second source of truth the old `status` field
    /// was: that field had no refresh path at all, while this one is refreshed
    /// unconditionally by the 100ms gossip tick as well as at each mutation
    /// site, so a refresh site someone forgets to add self-heals within a tick
    /// instead of pinning a wrong answer forever.
    fail_closed: bool,
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
            fail_closed: false,
            nodes: HashMap::new(),
            importing: HashMap::new(),
            migrating: HashMap::new(),
            messages_sent: 0,
            messages_received: 0,
            last_vote_epoch: 0,
            failover_state: FailoverState::None,
        };
        let self_node = ClusterNode::new(node_id.clone(), self_addr, NodeRole::Master, 0);
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

        // Nobody claims this slot.
        //
        // Checked FIRST, before the fail-closed gate, because the two
        // CLUSTERDOWN messages are not interchangeable and Redis picks the
        // per-slot one here. Measured: a lone `--cluster-enabled` node with no
        // slots reports `cluster_state:fail` AND answers a key command with
        // `CLUSTERDOWN Hash slot not served` — the unbound-slot answer wins
        // over the cluster-down answer.
        //
        // For a LONE node this is instead the bootstrap case: a single server
        // with cluster mode on and no slots assigned must still serve, or it is
        // useless until someone runs ADDSLOTS. That is a deliberate divergence
        // from Redis (M3), which refuses. For a FORMED cluster it is a coverage
        // hole, and serving locally is what produced #485 — a write accepted by
        // a node that does not own the slot, invisible to the node that does,
        // with no error to tell the client.
        if self.slot_owner(slot).is_none() {
            return if self.nodes.len() <= 1 {
                SlotRoute::Local
            } else {
                SlotRoute::SlotNotServed
            };
        }

        // The slot HAS an owner, but the cluster as a whole has lost coverage
        // somewhere else: refuse everything, including keys this node owns and
        // could serve. A single relaxed atomic load — the coverage scan behind
        // it runs on topology change, never per command (see
        // `refresh_fail_closed`).
        if self.fail_closed {
            return SlotRoute::ClusterDown;
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

        // Unreachable in practice: the no-owner case is handled at the top of
        // this function, so reaching here means some node owns the slot but
        // neither the MOVED nor the Local branch claimed it. Route locally
        // rather than inventing an error.
        SlotRoute::Local
    }

    /// How many slots total are assigned to any node in this cluster.
    pub fn assigned_slot_count(&self) -> usize {
        self.nodes.values().map(|n| n.slot_count()).sum()
    }

    /// Classify all 16384 slots by the health of the node that owns them.
    ///
    /// Counted through the owners' bitmaps in one pass rather than by summing
    /// per-node slot counts: summing would double-count a slot two nodes both
    /// claim, and could then report `assigned == 16384` for a cluster with an
    /// actual coverage hole. First claimant wins, matching `slot_owner`.
    pub fn slot_coverage(&self) -> SlotCoverage {
        let mut claimed = [0u8; 2048];
        let mut ok = [0u8; 2048];
        let mut pfail = [0u8; 2048];
        let mut fail = [0u8; 2048];

        for node in self.nodes.values() {
            let bucket = match node.health {
                NodeHealth::Online => &mut ok,
                NodeHealth::Pfail => &mut pfail,
                NodeHealth::Fail => &mut fail,
            };
            for i in 0..2048 {
                // Only slots nobody has claimed yet fall to this node.
                bucket[i] |= node.slots[i] & !claimed[i];
            }
            for i in 0..2048 {
                claimed[i] |= node.slots[i];
            }
        }

        let popcount = |b: &[u8; 2048]| b.iter().map(|x| x.count_ones() as usize).sum();
        SlotCoverage {
            assigned: popcount(&claimed),
            ok: popcount(&ok),
            pfail: popcount(&pfail),
            fail: popcount(&fail),
        }
    }

    /// Recompute the hot-path fail-closed cache from real coverage.
    ///
    /// A lone node never trips the gate: a single server with cluster mode on
    /// and no slots assigned is the bootstrap case and must stay usable (M3).
    /// Redis refuses there; this is Moon's contracted divergence.
    pub fn refresh_fail_closed(&mut self) {
        self.fail_closed = self.nodes.len() > 1 && self.cluster_status() == ClusterStatus::Fail;
    }

    /// Is the fail-closed gate currently engaged? Test/introspection accessor;
    /// the hot path reads the field directly.
    #[inline]
    pub fn is_fail_closed(&self) -> bool {
        self.fail_closed
    }

    /// The cluster's real state, derived rather than stored.
    ///
    /// Derived deliberately: the old `status` field was assigned once in the
    /// initialiser and never again, so `cluster_state` was the constant `ok` —
    /// a cluster that had lost a master still told every client it was healthy.
    /// A value computed at read time cannot go stale.
    ///
    /// `Ok` iff every one of the 16384 slots is claimed by a node that is not
    /// confirmed `Fail`. `Pfail` is a local suspicion, not consensus, and does
    /// NOT by itself bring the cluster down — matching Redis, which fails the
    /// cluster on an uncovered slot or a FAIL owner.
    pub fn cluster_status(&self) -> ClusterStatus {
        let mut served = [0u8; 2048];
        for node in self.nodes.values() {
            if node.is_failed() {
                continue;
            }
            for i in 0..2048 {
                served[i] |= node.slots[i];
            }
        }
        if served.iter().all(|b| *b == 0xff) {
            ClusterStatus::Ok
        } else {
            ClusterStatus::Fail
        }
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
            .filter(|n| matches!(n.role, NodeRole::Master))
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

        let mut peer = ClusterNode::new(peer_id.clone(), test_addr(6380), NodeRole::Master, 0);
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
        let mut node = ClusterNode::new("a".repeat(40), addr, NodeRole::Master, 0);
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

    /// The fail-closed gate refuses a key this node OWNS.
    ///
    /// Freeze flag 2, and the surprising half on purpose: measured against
    /// redis-server, a degraded cluster refuses even a slot the receiving node
    /// could serve. A partially-visible keyspace is worse than a refusal.
    #[test]
    fn test_fail_closed_gate_refuses_a_locally_owned_slot() {
        let mut state = make_state_with_two_nodes();
        // Full coverage, everyone healthy -> gate open.
        state.refresh_fail_closed();
        assert!(!state.is_fail_closed());
        assert_eq!(state.route_slot(0, false), SlotRoute::Local);

        // The peer dies. Its half of the keyspace has no live owner.
        let peer_id = "b".repeat(40);
        state.nodes.get_mut(&peer_id).unwrap().health = NodeHealth::Fail;
        state.refresh_fail_closed();
        assert!(state.is_fail_closed());

        // Slot 0 is OURS and we are healthy — and it is still refused.
        assert_eq!(state.route_slot(0, false), SlotRoute::ClusterDown);
    }

    /// An unowned slot answers the PER-SLOT message even while the cluster is
    /// fail — the two CLUSTERDOWN texts are not interchangeable.
    ///
    /// Measured: a lone `--cluster-enabled` node reports `cluster_state:fail`
    /// and yet answers `CLUSTERDOWN Hash slot not served`, so the unbound-slot
    /// check must be ordered BEFORE the fail-closed check.
    #[test]
    fn test_unowned_slot_beats_the_fail_closed_message() {
        let mut state = make_state_with_two_nodes();
        // Drop slot 9000 from the peer: now nobody owns it AND the cluster is
        // no longer fully covered, so both conditions are live at once.
        let peer_id = "b".repeat(40);
        state.nodes.get_mut(&peer_id).unwrap().clear_slot(9000);
        state.refresh_fail_closed();
        assert!(state.is_fail_closed(), "coverage hole must trip the gate");

        let route = state.route_slot(9000, false);
        assert_eq!(route, SlotRoute::SlotNotServed);
        match route.into_error_frame(9000) {
            Frame::Error(msg) => assert_eq!(
                std::str::from_utf8(&msg).unwrap(),
                "CLUSTERDOWN Hash slot not served"
            ),
            other => panic!("expected Error, got {other:?}"),
        }
    }

    /// A lone node never trips the gate, however uncovered it is.
    ///
    /// Moon's contracted divergence from Redis (M3): a single server with
    /// cluster mode on and no slots must stay usable, or it is useless until
    /// someone runs ADDSLOTS.
    #[test]
    fn test_lone_node_never_trips_the_fail_closed_gate() {
        let mut state = ClusterState::new("a".repeat(40), test_addr(6379));
        assert_eq!(state.cluster_status(), ClusterStatus::Fail, "no coverage");
        state.refresh_fail_closed();
        assert!(
            !state.is_fail_closed(),
            "the bootstrap case must stay serveable"
        );
        assert_eq!(state.route_slot(1234, false), SlotRoute::Local);
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
