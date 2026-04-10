//! GraphSegmentHolder -- ArcSwap-based lock-free segment management.
//!
//! Mirrors the vector `SegmentHolder` pattern. All reads go through a single
//! atomic `load()`, providing consistent snapshots for concurrent queries.

use std::sync::Arc;

use arc_swap::{ArcSwap, Guard};

use crate::graph::csr::CsrSegment;
use crate::graph::memgraph::MemGraph;

/// Snapshot of all graph segments at a point in time.
#[derive(Debug)]
pub struct GraphSegmentList {
    /// Active mutable segment (None after freeze, before new one is created).
    pub mutable: Option<Arc<MemGraph>>,
    /// Immutable CSR segments, newest first.
    pub immutable: Vec<Arc<CsrSegment>>,
}

/// Lock-free segment holder for graph data.
///
/// Reads are a single atomic load (~2ns). Writers atomically swap in new
/// segment lists. Old segments are dropped when all Arc references (from
/// in-flight queries holding Guards) are released.
pub struct GraphSegmentHolder {
    segments: ArcSwap<GraphSegmentList>,
}

impl GraphSegmentHolder {
    /// Create a holder with a fresh MemGraph and empty immutable list.
    pub fn new(edge_threshold: usize) -> Self {
        Self {
            segments: ArcSwap::from_pointee(GraphSegmentList {
                mutable: Some(Arc::new(MemGraph::new(edge_threshold))),
                immutable: Vec::new(),
            }),
        }
    }

    /// Single atomic load, lock-free. Returns a guard that keeps the snapshot alive.
    pub fn load(&self) -> Guard<Arc<GraphSegmentList>> {
        self.segments.load()
    }

    /// Atomically replace the entire segment list.
    pub fn swap(&self, new_list: GraphSegmentList) {
        self.segments.store(Arc::new(new_list));
    }

    /// Add a new immutable CSR segment (e.g. from compaction) to the list.
    pub fn add_immutable(&self, csr: CsrSegment) {
        let current = self.segments.load();
        let mut new_immutable = current.immutable.clone();
        new_immutable.insert(0, Arc::new(csr)); // newest first
        self.segments.store(Arc::new(GraphSegmentList {
            mutable: current.mutable.clone(),
            immutable: new_immutable,
        }));
    }

    /// Replace multiple immutable CSR segments (identified by created_lsn)
    /// with a single compacted segment.
    pub fn replace_immutable(&self, old_lsns: &[u64], new_csr: CsrSegment) {
        let current = self.segments.load();
        let mut new_immutable: Vec<Arc<CsrSegment>> = current
            .immutable
            .iter()
            .filter(|seg| !old_lsns.contains(&seg.created_lsn))
            .cloned()
            .collect();
        new_immutable.insert(0, Arc::new(new_csr)); // newest first
        self.segments.store(Arc::new(GraphSegmentList {
            mutable: current.mutable.clone(),
            immutable: new_immutable,
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_returns_initial_state() {
        let holder = GraphSegmentHolder::new(1000);
        let snap = holder.load();
        assert!(snap.mutable.is_some());
        assert!(snap.immutable.is_empty());
    }

    #[test]
    fn test_swap_updates_state() {
        let holder = GraphSegmentHolder::new(1000);
        holder.swap(GraphSegmentList {
            mutable: None,
            immutable: Vec::new(),
        });
        let snap = holder.load();
        assert!(snap.mutable.is_none());
    }

    #[test]
    fn test_concurrent_readers_see_consistent_snapshot() {
        let holder = Arc::new(GraphSegmentHolder::new(1000));
        let holder2 = holder.clone();

        // Take a snapshot before swap.
        let snap_before = holder.load();
        assert!(snap_before.mutable.is_some());

        // Spawn a thread that swaps to a new state.
        let handle = std::thread::spawn(move || {
            holder2.swap(GraphSegmentList {
                mutable: None,
                immutable: Vec::new(),
            });
        });
        handle.join().expect("thread ok");

        // The old snapshot is still valid (Arc keeps it alive).
        assert!(snap_before.mutable.is_some());

        // New load sees the updated state.
        let snap_after = holder.load();
        assert!(snap_after.mutable.is_none());
    }

    #[test]
    fn test_add_immutable() {
        use smallvec::smallvec;

        let holder = GraphSegmentHolder::new(1000);

        // Build a minimal CSR for testing.
        let mut mg = MemGraph::new(100);
        let a = mg.add_node(smallvec![0], smallvec![], None, 1);
        let b = mg.add_node(smallvec![0], smallvec![], None, 1);
        mg.add_edge(a, b, 0, 1.0, None, 2).expect("ok");
        let frozen = mg.freeze().expect("ok");
        let csr = CsrSegment::from_frozen(frozen, 10).expect("ok");

        holder.add_immutable(csr);
        let snap = holder.load();
        assert_eq!(snap.immutable.len(), 1);
        assert_eq!(snap.immutable[0].created_lsn, 10);
    }

    #[test]
    fn test_replace_immutable() {
        use smallvec::smallvec;

        let holder = GraphSegmentHolder::new(1000);

        // Add two CSR segments with different LSNs.
        for lsn in [10u64, 20] {
            let mut mg = MemGraph::new(100);
            let a = mg.add_node(smallvec![0], smallvec![], None, 1);
            let b = mg.add_node(smallvec![0], smallvec![], None, 1);
            mg.add_edge(a, b, 0, 1.0, None, 2).expect("ok");
            let frozen = mg.freeze().expect("ok");
            let csr = CsrSegment::from_frozen(frozen, lsn).expect("ok");
            holder.add_immutable(csr);
        }

        let snap = holder.load();
        assert_eq!(snap.immutable.len(), 2);

        // Replace both with a single compacted segment.
        let mut mg = MemGraph::new(100);
        let a = mg.add_node(smallvec![0], smallvec![], None, 1);
        let b = mg.add_node(smallvec![0], smallvec![], None, 1);
        mg.add_edge(a, b, 0, 1.0, None, 2).expect("ok");
        let frozen = mg.freeze().expect("ok");
        let compacted = CsrSegment::from_frozen(frozen, 30).expect("ok");

        holder.replace_immutable(&[10, 20], compacted);

        let snap = holder.load();
        assert_eq!(snap.immutable.len(), 1);
        assert_eq!(snap.immutable[0].created_lsn, 30);
    }
}
