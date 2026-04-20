use std::collections::HashMap;
use crate::error::Result;
use crate::types::{GraphEdge, GraphNode, QueryResult};
use crate::util::{parse_query_result, value_to_i64, value_to_string};

/// Graph engine sub-client (`GRAPH.*` commands).
///
/// Obtain via [`MoonClient::graph`](crate::MoonClient::graph).
///
/// # Example
/// ```no_run
/// use moon_client::MoonClient;
///
/// #[tokio::main]
/// async fn main() -> moon_client::Result<()> {
///     let client = MoonClient::connect("redis://127.0.0.1:6399").await?;
///     let mut g = client.graph();
///     g.create("social").await?;
///     let alice = g.add_node("social", "Person", &[("name", "Alice"), ("age", "30")]).await?;
///     let bob = g.add_node("social", "Person", &[("name", "Bob")]).await?;
///     g.add_edge("social", alice, bob, "KNOWS", 1.0, &[]).await?;
///     let result = g.query("social", "MATCH (p:Person) RETURN p.name").await?;
///     for row in result.rows {
///         println!("{:?}", row);
///     }
///     Ok(())
/// }
/// ```
pub struct GraphClient {
    pub(crate) conn: redis::aio::MultiplexedConnection,
}

impl GraphClient {
    // ── Graph lifecycle ──────────────────────────────────────────────────────

    /// Create a named graph (`GRAPH.CREATE`).
    pub async fn create(&mut self, name: &str) -> Result<()> {
        redis::cmd("GRAPH.CREATE")
            .arg(name)
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    /// Drop a graph and all its data (`GRAPH.DELETE` or `GRAPH.DROP`).
    pub async fn delete(&mut self, name: &str) -> Result<()> {
        redis::cmd("GRAPH.DELETE")
            .arg(name)
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    /// List all graphs (`GRAPH.LIST`).
    pub async fn list(&mut self) -> Result<Vec<String>> {
        Ok(redis::cmd("GRAPH.LIST").query_async(&mut self.conn).await?)
    }

    /// Get graph statistics (`GRAPH.INFO`).
    pub async fn info(&mut self, name: &str) -> Result<redis::Value> {
        Ok(redis::cmd("GRAPH.INFO").arg(name).query_async(&mut self.conn).await?)
    }

    // ── Nodes ────────────────────────────────────────────────────────────────

    /// Add a node and return its server-assigned numeric ID (`GRAPH.ADDNODE`).
    ///
    /// `properties` is a slice of `(field_name, field_value)` pairs.
    pub async fn add_node(
        &mut self,
        graph: &str,
        label: &str,
        properties: &[(&str, &str)],
    ) -> Result<i64> {
        let mut cmd = redis::cmd("GRAPH.ADDNODE");
        cmd.arg(graph).arg(label);
        for (k, v) in properties {
            cmd.arg(*k).arg(*v);
        }
        let raw: redis::Value = cmd.query_async(&mut self.conn).await?;
        Ok(value_to_i64(&raw).unwrap_or(0))
    }

    /// Retrieve a node by ID (`GRAPH.GETNODE`). Returns `None` if not found.
    pub async fn get_node(&mut self, graph: &str, node_id: i64) -> Result<Option<GraphNode>> {
        let raw: redis::Value = redis::cmd("GRAPH.GETNODE")
            .arg(graph)
            .arg(node_id)
            .query_async(&mut self.conn)
            .await?;
        Ok(parse_node_value(raw))
    }

    /// Delete a node by ID (`GRAPH.DELNODE`).
    pub async fn del_node(&mut self, graph: &str, node_id: i64) -> Result<()> {
        redis::cmd("GRAPH.DELNODE")
            .arg(graph)
            .arg(node_id)
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    // ── Edges ────────────────────────────────────────────────────────────────

    /// Add a directed edge between two nodes (`GRAPH.ADDEDGE`).
    pub async fn add_edge(
        &mut self,
        graph: &str,
        src_id: i64,
        dst_id: i64,
        edge_type: &str,
        weight: f64,
        properties: &[(&str, &str)],
    ) -> Result<()> {
        let mut cmd = redis::cmd("GRAPH.ADDEDGE");
        cmd.arg(graph).arg(src_id).arg(dst_id).arg(edge_type).arg("WEIGHT").arg(weight);
        for (k, v) in properties {
            cmd.arg(*k).arg(*v);
        }
        cmd.query_async::<()>(&mut self.conn).await?;
        Ok(())
    }

    /// Retrieve an edge by source and destination IDs (`GRAPH.GETEDGE`).
    pub async fn get_edge(&mut self, graph: &str, src_id: i64, dst_id: i64) -> Result<Option<GraphEdge>> {
        let raw: redis::Value = redis::cmd("GRAPH.GETEDGE")
            .arg(graph)
            .arg(src_id)
            .arg(dst_id)
            .query_async(&mut self.conn)
            .await?;
        Ok(parse_edge_value(raw))
    }

    /// Delete an edge (`GRAPH.DELEDGE`).
    pub async fn del_edge(&mut self, graph: &str, src_id: i64, dst_id: i64) -> Result<()> {
        redis::cmd("GRAPH.DELEDGE")
            .arg(graph)
            .arg(src_id)
            .arg(dst_id)
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    // ── Traversal ────────────────────────────────────────────────────────────

    /// Get neighbors of a node (`GRAPH.NEIGHBORS`).
    ///
    /// Returns a list of `(src_id, dst_id, edge_type, weight)` tuples.
    pub async fn neighbors(
        &mut self,
        graph: &str,
        node_id: i64,
        direction: NeighborDirection,
    ) -> Result<Vec<(i64, i64, String, f64)>> {
        let _ = direction; // server does not filter by direction; kept for API forwards-compat
        let raw: redis::Value = redis::cmd("GRAPH.NEIGHBORS")
            .arg(graph)
            .arg(node_id)
            .query_async(&mut self.conn)
            .await?;
        Ok(crate::util::parse_neighbors(raw))
    }

    /// Get degree of a node (`GRAPH.DEGREE`).
    pub async fn degree(&mut self, graph: &str, node_id: i64) -> Result<i64> {
        Ok(redis::cmd("GRAPH.DEGREE")
            .arg(graph)
            .arg(node_id)
            .query_async(&mut self.conn)
            .await?)
    }

    // ── Cypher queries ───────────────────────────────────────────────────────

    /// Execute a read-write Cypher query (`GRAPH.QUERY`).
    pub async fn query(&mut self, graph: &str, cypher: &str) -> Result<QueryResult> {
        let raw: redis::Value = redis::cmd("GRAPH.QUERY")
            .arg(graph)
            .arg(cypher)
            .query_async(&mut self.conn)
            .await?;
        Ok(parse_query_result(raw))
    }

    /// Execute a read-only Cypher query (`GRAPH.RO_QUERY`).
    pub async fn ro_query(&mut self, graph: &str, cypher: &str) -> Result<QueryResult> {
        let raw: redis::Value = redis::cmd("GRAPH.RO_QUERY")
            .arg(graph)
            .arg(cypher)
            .query_async(&mut self.conn)
            .await?;
        Ok(parse_query_result(raw))
    }

    /// Temporal Cypher query at a specific wall-clock timestamp (`GRAPH.QUERY ... VALID_AT <ms>`).
    pub async fn query_at(&mut self, graph: &str, cypher: &str, valid_at_ms: i64) -> Result<QueryResult> {
        let raw: redis::Value = redis::cmd("GRAPH.QUERY")
            .arg(graph)
            .arg(cypher)
            .arg("VALID_AT")
            .arg(valid_at_ms)
            .query_async(&mut self.conn)
            .await?;
        Ok(parse_query_result(raw))
    }

    /// Explain a Cypher query execution plan (`GRAPH.EXPLAIN`).
    pub async fn explain(&mut self, graph: &str, cypher: &str) -> Result<Vec<String>> {
        Ok(redis::cmd("GRAPH.EXPLAIN")
            .arg(graph)
            .arg(cypher)
            .query_async(&mut self.conn)
            .await?)
    }

    /// Profile a Cypher query with execution stats (`GRAPH.PROFILE`).
    pub async fn profile(&mut self, graph: &str, cypher: &str) -> Result<redis::Value> {
        Ok(redis::cmd("GRAPH.PROFILE")
            .arg(graph)
            .arg(cypher)
            .query_async(&mut self.conn)
            .await?)
    }

    /// Vector-similarity graph search (`GRAPH.VSEARCH`).
    ///
    /// Traverses up to `hops` from `start_node_id`, scores candidates by cosine
    /// similarity to `query_vec`, and returns the top-`k` results.
    pub async fn vsearch(
        &mut self,
        graph: &str,
        start_node_id: i64,
        hops: usize,
        k: usize,
        query_vec: &[f32],
    ) -> Result<redis::Value> {
        let blob = crate::types::encode_vector(query_vec);
        Ok(redis::cmd("GRAPH.VSEARCH")
            .arg(graph)
            .arg(start_node_id)
            .arg(hops)
            .arg(k)
            .arg(&blob)
            .query_async(&mut self.conn)
            .await?)
    }
}

// ── Edge/node response parsers ───────────────────────────────────────────────

fn parse_node_value(raw: redis::Value) -> Option<GraphNode> {
    let arr = match raw {
        redis::Value::Array(a) => a,
        redis::Value::Nil => return None,
        _ => return None,
    };
    if arr.is_empty() {
        return None;
    }
    let kv = flat_kv(&arr);
    let node_id = kv.get("node_id")?.parse().ok()?;
    let label = kv.get("label").cloned().unwrap_or_default();
    let mut properties = HashMap::new();
    for (k, v) in kv {
        if !matches!(k.as_str(), "node_id" | "label") {
            properties.insert(k, v);
        }
    }
    Some(GraphNode { node_id, label, properties })
}

fn parse_edge_value(raw: redis::Value) -> Option<GraphEdge> {
    let arr = match raw {
        redis::Value::Array(a) => a,
        redis::Value::Nil => return None,
        _ => return None,
    };
    if arr.is_empty() {
        return None;
    }
    let kv = flat_kv(&arr);
    let src_id = kv.get("src_id")?.parse().ok()?;
    let dst_id = kv.get("dst_id")?.parse().ok()?;
    let edge_type = kv.get("edge_type").cloned().unwrap_or_default();
    let weight: f64 = kv.get("weight").and_then(|v| v.parse().ok()).unwrap_or(1.0);
    let mut properties = HashMap::new();
    for (k, v) in kv {
        if !matches!(k.as_str(), "src_id" | "dst_id" | "edge_type" | "weight") {
            properties.insert(k, v);
        }
    }
    Some(GraphEdge { src_id, dst_id, edge_type, weight, properties })
}

fn flat_kv(arr: &[redis::Value]) -> HashMap<String, String> {
    let mut m = HashMap::new();
    let mut i = 0;
    while i + 1 < arr.len() {
        let k = value_to_string(&arr[i]);
        let v = value_to_string(&arr[i + 1]);
        if !k.is_empty() {
            m.insert(k, v);
        }
        i += 2;
    }
    m
}

/// Direction filter for `GRAPH.NEIGHBORS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NeighborDirection {
    Both,
    Out,
    In,
}

impl NeighborDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Both => "BOTH",
            Self::Out => "OUT",
            Self::In => "IN",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn neighbor_direction_strings() {
        assert_eq!(NeighborDirection::Both.as_str(), "BOTH");
        assert_eq!(NeighborDirection::Out.as_str(), "OUT");
        assert_eq!(NeighborDirection::In.as_str(), "IN");
    }

    #[test]
    fn parse_empty_node_value() {
        assert!(parse_node_value(redis::Value::Nil).is_none());
        assert!(parse_node_value(redis::Value::Array(vec![])).is_none());
    }
}
