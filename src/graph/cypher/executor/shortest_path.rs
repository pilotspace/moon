//! Shared `run_shortest_path` helper — single source of truth for both
//! the MATCH-form `PhysicalOp::ShortestPath` and the expression-form
//! `Expr::ShortestPathCall`. Extracted in Phase 174 FIX-04 to eliminate
//! divergence in edge_types, direction, max_hops cap, and CSR visibility.

use std::sync::Arc;

use super::*;
use crate::graph::csr::CsrStorage;
use crate::graph::memgraph::MemGraph;
use crate::graph::traversal::{DijkstraTraversal, SegmentMergeReader};

/// Maximum traversal depth for shortest-path queries. Prevents unbounded
/// Dijkstra expansion on adversarial graphs.
const MAX_HOPS_CAP: u32 = 32;

/// Run shortest-path (Dijkstra) from `src_key` to `dst_key`, honouring
/// edge_types filter, direction, and the 32-hop cap.
///
/// Returns `Some(path)` on success or `None` if no path exists within the
/// capped depth. Both MATCH-form and expression-form delegate here.
pub(crate) fn run_shortest_path(
    memgraph: &MemGraph,
    immutable_segs: &[Arc<CsrStorage>],
    snapshot_lsn: u64,
    decay: Option<crate::graph::scoring::DecayConfig>,
    src_key: NodeKey,
    dst_key: NodeKey,
    edge_types: &[String],
    direction: EdgeDirection,
    max_hops: u32,
) -> Option<Vec<NodeKey>> {
    let dir = match direction {
        EdgeDirection::Right => Direction::Outgoing,
        EdgeDirection::Left => Direction::Incoming,
        EdgeDirection::Both => Direction::Both,
    };

    let type_ids: Vec<u16> = edge_types
        .iter()
        .map(|t| label_to_id(t.as_bytes()))
        .collect();

    let edge_type_filter = if type_ids.len() == 1 {
        Some(type_ids[0])
    } else {
        None
    };

    let effective_lsn = if snapshot_lsn == 0 {
        u64::MAX
    } else {
        snapshot_lsn
    };

    let reader = SegmentMergeReader::new(
        Some(memgraph),
        immutable_segs,
        dir,
        effective_lsn,
        edge_type_filter,
    );

    // Decay off (None): distance-only Dijkstra — the time term is zero and
    // edge ages are never read, preserving the exact pre-decay behavior.
    // Decay on: cost = lambda*time_weight*age_sec + |weight| (see DecayConfig).
    let cost_fn = match decay {
        Some(d) => d.cost_fn(),
        None => crate::graph::scoring::WeightedCostFn::new(0.0, 1.0, 0),
    };
    let capped_hops = max_hops.min(MAX_HOPS_CAP);
    let dijkstra = DijkstraTraversal::new(cost_fn, capped_hops);

    match dijkstra.shortest_path(&reader, src_key, dst_key) {
        Ok(Some(result)) => Some(result.path),
        _ => None,
    }
}
