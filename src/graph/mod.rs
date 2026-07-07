//! Graph storage engine -- per-shard, segment-aligned property graph.
//!
//! Feature-gated under `graph` so the default build is unaffected.
//!
//! # Sharding model: single-shard graphs (W2-11)
//!
//! A named graph lives ENTIRELY on one shard — every GRAPH.* command routes
//! by hashing the graph name (`extract_primary_key` returns args[0]), so all
//! nodes, edges, segments, WAL records, and traversals for a graph are
//! shard-local. Redis-style hash tags in the graph name (`g{tenant1}`)
//! co-locate multiple graphs on one shard the same way they co-locate keys.
//! There is NO cross-shard traversal: an earlier scatter-gather scaffold
//! (`cross_shard.rs`: `handle_graph_traverse` + a `GraphTraverse` SPSC
//! message) never had a coordinator that sent it and was deleted — partition
//! a workload by GRAPH, not within one.

pub mod compaction;
pub mod csr;
pub mod cypher;
pub mod fasthash;
pub mod hnsw_bridge;
pub mod hybrid;
pub mod index;
pub mod manifest;
pub mod memgraph;
pub mod recovery;
pub mod replay;
pub mod row_bfs;
pub mod scoring;
pub mod segment;
pub mod simd;
pub mod stats;
pub mod store;
pub mod traversal;
pub mod traversal_guard;
pub mod types;
pub mod view;
pub mod visibility;
pub mod wal;

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
pub use view::MergedNodeView;
