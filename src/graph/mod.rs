//! Graph storage engine -- per-shard, segment-aligned property graph.
//!
//! Feature-gated under `graph` so the default build is unaffected.

pub mod compaction;
pub mod cypher;
pub mod csr;
pub mod hybrid;
pub mod index;
pub mod memgraph;
pub mod replay;
pub mod scoring;
pub mod segment;
pub mod stats;
pub mod store;
pub mod traversal;
pub mod traversal_guard;
pub mod types;
pub mod visibility;
pub mod wal;

pub use csr::CsrSegment;
pub use memgraph::MemGraph;
pub use scoring::{CompositeScorer, DistanceScorer, TemporalDecayScorer, WeightedCostFn};
pub use segment::GraphSegmentHolder;
pub use store::GraphStore;
pub use traversal::{BoundedBfs, BoundedDfs, DijkstraTraversal, SegmentMergeReader};
pub use cypher::{parse_cypher, is_read_only, CypherQuery, CypherError};
pub use hybrid::{
    FilterStrategy, GraphFilteredSearch, HybridError, HybridResult, VectorGuidedWalk,
    VectorToGraphExpansion,
};
pub use index::{EdgeTypeIndex, LabelIndex, MphNodeIndex, PropertyIndex};
pub use stats::GraphStats;
pub use types::{Direction, EdgeKey, NodeKey, PropertyMap, PropertyValue};
