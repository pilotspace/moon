//! Graph storage engine -- per-shard, segment-aligned property graph.
//!
//! Feature-gated under `graph` so the default build is unaffected.

pub mod compaction;
pub mod cross_shard;
pub mod csr;
pub mod cypher;
pub mod hybrid;
pub mod index;
pub mod manifest;
pub mod memgraph;
pub mod recovery;
pub mod replay;
pub mod scoring;
pub mod segment;
pub mod simd;
pub mod stats;
pub mod store;
pub mod traversal;
pub mod traversal_guard;
pub mod types;
pub mod visibility;
pub mod wal;

pub use cross_shard::{
    DEFAULT_CROSS_SHARD_DEPTH_LIMIT, TraversalShardResult, graph_has_hash_tag,
    handle_graph_traverse, parse_traverse_response,
};
pub use csr::{CsrSegment, CsrStorage, MmapCsrSegment};
pub use cypher::{CypherError, CypherQuery, is_read_only, parse_cypher};
pub use hybrid::{
    FilterStrategy, GraphConstrainedReRanker, GraphFilteredSearch, HybridError, HybridResult,
    VectorGuidedWalk, VectorToGraphExpansion,
};
pub use index::{EdgeTypeIndex, LabelIndex, MphNodeIndex, PropertyIndex};
pub use memgraph::MemGraph;
pub use scoring::{CompositeScorer, DistanceScorer, TemporalDecayScorer, WeightedCostFn};
pub use segment::GraphSegmentHolder;
pub use stats::GraphStats;
pub use store::GraphStore;
pub use traversal::{BoundedBfs, BoundedDfs, DijkstraTraversal, ParallelBfs, SegmentMergeReader};
pub use types::{Direction, EdgeKey, NodeKey, PropertyMap, PropertyValue};
