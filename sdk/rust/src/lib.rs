//! # moon-client
//!
//! Async Rust client for the [Moon](https://github.com/pilotspace/moon) server — a
//! high-performance Redis-compatible database with built-in vector search, graph
//! engine, full-text search, semantic caching, workspaces, and message queues.
//!
//! ## Quick start
//!
//! ```no_run
//! use moon::{MoonClient, Result};
//! use moon::types::{VectorIndexOptions, DistanceMetric};
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let mut client = MoonClient::connect("redis://127.0.0.1:6399").await?;
//!
//!     // Standard Redis commands
//!     client.set("hello", "world").await?;
//!     let val: String = client.get("hello").await?;
//!     println!("{val}");
//!
//!     // Vector search
//!     let mut vector = client.vector();
//!     vector.create_index(
//!         "docs",
//!         VectorIndexOptions::new(384, DistanceMetric::Cosine),
//!     ).await?;
//!
//!     // Graph engine
//!     let mut graph = client.graph();
//!     graph.create("social").await?;
//!     let alice = graph.add_node("social", "Person", &[("name", "Alice")]).await?;
//!
//!     // Moon ACID transactions (cross-store)
//!     client.txn_begin().await?;
//!     client.set("counter", 42).await?;
//!     client.txn_commit().await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Feature flags
//!
//! | Feature | Description |
//! |---------|-------------|
//! | `tls-rustls` | TLS via rustls |
//! | `tls-native-tls` | TLS via native-tls |

pub mod cache;
pub mod client;
pub mod error;
pub mod graph;
pub mod mq;
pub mod session;
pub mod temporal;
pub mod text;
pub mod types;
mod util;
pub mod vector;
pub mod workspace;

pub use client::MoonClient;
pub use error::{MoonError, Result};
pub use graph::NeighborDirection;
pub use temporal::EntityType;
pub use types::{
    AggregateRow, CacheSearchResult, DistanceMetric, GraphEdge, GraphNode, IndexAlgorithm,
    IndexInfo, MqMessage, PubSubMessage, QueryResult, Reducer, SchemaField, SearchResult,
    TextSearchHit, VectorIndexOptions, WorkspaceInfo, decode_vector, encode_vector,
};
