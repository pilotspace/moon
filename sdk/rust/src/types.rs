use crate::error::{MoonError, Result};
use std::collections::HashMap;

// ── Vector encoding ─────────────────────────────────────────────────────────

/// Encode a `f32` slice to little-endian bytes for Moon vector storage.
pub fn encode_vector(vec: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(vec.len() * 4);
    for &v in vec {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

/// Decode little-endian bytes back to a `f32` vector.
pub fn decode_vector(bytes: &[u8]) -> Result<Vec<f32>> {
    if bytes.len() % 4 != 0 {
        return Err(MoonError::parse(format!(
            "vector byte length {} is not a multiple of 4",
            bytes.len()
        )));
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
        .collect())
}

// ── Vector search types ──────────────────────────────────────────────────────

/// Distance metric for vector indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceMetric {
    L2,
    Cosine,
    InnerProduct,
}

impl DistanceMetric {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::L2 => "L2",
            Self::Cosine => "COSINE",
            Self::InnerProduct => "IP",
        }
    }
}

/// Index algorithm for vector search. Moon only supports HNSW.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexAlgorithm {
    Hnsw,
}

impl IndexAlgorithm {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hnsw => "HNSW",
        }
    }
}

/// Extra schema field appended to a vector index.
#[derive(Debug, Clone)]
pub enum SchemaField {
    Text(String),
    Tag(String),
    Numeric(String),
}

impl SchemaField {
    pub fn to_args(&self) -> Vec<String> {
        match self {
            Self::Text(name) => vec![name.clone(), "TEXT".to_string()],
            Self::Tag(name) => vec![name.clone(), "TAG".to_string()],
            Self::Numeric(name) => vec![name.clone(), "NUMERIC".to_string()],
        }
    }
}

/// Options for creating a vector index via `FT.CREATE`.
#[derive(Debug, Clone)]
pub struct VectorIndexOptions {
    pub prefix: String,
    pub field_name: String,
    pub algorithm: IndexAlgorithm,
    pub dim: usize,
    pub dtype: String,
    pub metric: DistanceMetric,
    pub m: usize,
    pub ef_construction: usize,
    pub ef_runtime: Option<usize>,
    pub compact_threshold: Option<usize>,
    pub extra_schema: Vec<SchemaField>,
}

impl VectorIndexOptions {
    pub fn new(dim: usize, metric: DistanceMetric) -> Self {
        Self {
            prefix: "doc:".into(),
            field_name: "vec".into(),
            algorithm: IndexAlgorithm::Hnsw,
            dim,
            dtype: "FLOAT32".into(),
            metric,
            m: 16,
            ef_construction: 200,
            ef_runtime: None,
            compact_threshold: None,
            extra_schema: vec![],
        }
    }

    pub fn prefix(mut self, p: impl Into<String>) -> Self {
        self.prefix = p.into();
        self
    }
    pub fn field_name(mut self, f: impl Into<String>) -> Self {
        self.field_name = f.into();
        self
    }
    pub fn m(mut self, m: usize) -> Self {
        self.m = m;
        self
    }
    pub fn ef_construction(mut self, ef: usize) -> Self {
        self.ef_construction = ef;
        self
    }
    pub fn ef_runtime(mut self, ef: usize) -> Self {
        self.ef_runtime = Some(ef);
        self
    }
    pub fn compact_threshold(mut self, t: usize) -> Self {
        self.compact_threshold = Some(t);
        self
    }
    pub fn add_field(mut self, field: SchemaField) -> Self {
        self.extra_schema.push(field);
        self
    }
}

/// A single vector search result.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub key: String,
    pub score: f64,
    pub fields: HashMap<String, String>,
    pub graph_hops: Option<i64>,
    pub cache_hit: Option<bool>,
}

/// Metadata about a vector index returned by `FT.INFO`.
#[derive(Debug, Clone, Default)]
pub struct IndexInfo {
    pub name: String,
    pub num_docs: i64,
    pub dimension: i64,
    pub distance_metric: String,
    pub fields: Vec<String>,
    pub extra: HashMap<String, String>,
}

/// Result from a semantic cache search.
#[derive(Debug, Clone)]
pub struct CacheSearchResult {
    pub results: Vec<SearchResult>,
    pub cache_hit: bool,
}

// ── Graph types ──────────────────────────────────────────────────────────────

/// A node in a Moon graph.
#[derive(Debug, Clone)]
pub struct GraphNode {
    pub node_id: i64,
    pub label: String,
    pub properties: HashMap<String, String>,
}

/// An edge in a Moon graph.
#[derive(Debug, Clone)]
pub struct GraphEdge {
    pub src_id: i64,
    pub dst_id: i64,
    pub edge_type: String,
    pub weight: f64,
    pub properties: HashMap<String, String>,
}

/// Result from a `GRAPH.QUERY` or `GRAPH.RO_QUERY` command.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub stats: Vec<String>,
}

// ── Text search types ────────────────────────────────────────────────────────

/// A full-text search hit returned by `FT.SEARCH` in BM25 mode.
#[derive(Debug, Clone)]
pub struct TextSearchHit {
    pub key: String,
    pub score: f64,
    pub fields: HashMap<String, String>,
}

/// Aggregate reducer types for `FT.AGGREGATE`.
#[derive(Debug, Clone)]
pub enum Reducer {
    Count,
    Sum(String),
    Avg(String),
    Min(String),
    Max(String),
    CountDistinct(String),
}

impl Reducer {
    pub fn to_args(&self) -> Vec<String> {
        match self {
            Self::Count => vec!["REDUCE".into(), "COUNT".into(), "0".into()],
            Self::Sum(f) => vec!["REDUCE".into(), "SUM".into(), "1".into(), f.clone()],
            Self::Avg(f) => vec!["REDUCE".into(), "AVG".into(), "1".into(), f.clone()],
            Self::Min(f) => vec!["REDUCE".into(), "MIN".into(), "1".into(), f.clone()],
            Self::Max(f) => vec!["REDUCE".into(), "MAX".into(), "1".into(), f.clone()],
            Self::CountDistinct(f) => {
                vec![
                    "REDUCE".into(),
                    "COUNT_DISTINCT".into(),
                    "1".into(),
                    f.clone(),
                ]
            }
        }
    }
}

/// A row in an FT.AGGREGATE result.
#[derive(Debug, Clone)]
pub struct AggregateRow {
    pub fields: HashMap<String, String>,
}

// ── Workspace types ──────────────────────────────────────────────────────────

/// Metadata about a workspace returned by `WS INFO`.
#[derive(Debug, Clone)]
pub struct WorkspaceInfo {
    pub id: String,
    pub name: String,
    pub created_at: i64,
    pub extra: HashMap<String, String>,
}

// ── Message queue types ──────────────────────────────────────────────────────

/// A message returned by `MQ POP`.
#[derive(Debug, Clone)]
pub struct MqMessage {
    pub id: String,
    pub data: bytes::Bytes,
    pub delivery_count: i64,
}

// ── Pub/Sub ──────────────────────────────────────────────────────────────────

/// A pub/sub message received on a subscribed channel.
#[derive(Debug, Clone)]
pub struct PubSubMessage {
    pub pattern: Option<String>,
    pub channel: String,
    pub payload: bytes::Bytes,
}
