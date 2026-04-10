//! Graph storage engine -- per-shard, segment-aligned property graph.
//!
//! Feature-gated under `graph` so the default build is unaffected.

pub mod memgraph;
pub mod types;

pub use memgraph::MemGraph;
pub use types::{Direction, EdgeKey, NodeKey, PropertyMap, PropertyValue};
