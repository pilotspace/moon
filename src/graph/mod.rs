//! Graph storage engine -- per-shard, segment-aligned property graph.
//!
//! Feature-gated under `graph` so the default build is unaffected.

pub mod compaction;
pub mod csr;
pub mod memgraph;
pub mod segment;
pub mod store;
pub mod traversal_guard;
pub mod types;
pub mod visibility;

pub use csr::CsrSegment;
pub use memgraph::MemGraph;
pub use segment::GraphSegmentHolder;
pub use store::GraphStore;
pub use types::{Direction, EdgeKey, NodeKey, PropertyMap, PropertyValue};
