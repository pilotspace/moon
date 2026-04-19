//! Cypher execution engine -- walks PhysicalPlan operators and produces result rows.
//!
//! Row-based pipeline model: each operator transforms a `Vec<Row>` where
//! `Row = HashMap<String, Value>`. The executor starts with one empty seed row
//! and sequentially applies each `PhysicalOp` from the plan.

mod eval;
mod read;
mod write;

pub(crate) use eval::*;
pub use read::*;
pub use write::*;

use std::collections::HashMap;

use bytes::Bytes;
use smallvec::SmallVec;

use crate::command::graph::graph_write::label_to_id;
use crate::graph::cypher::ast::*;
use crate::graph::cypher::planner::*;
use crate::graph::store::NamedGraph;
use crate::graph::traversal::SegmentMergeReader;
use crate::graph::types::*;

/// Runtime value in the executor pipeline.
#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Node(NodeKey),
    Edge(EdgeKey),
    List(Vec<Value>),
    Map(Vec<(String, Value)>),
    /// A path: ordered list of NodeKey from source to target. v0.1.9
    /// (CYP-04/05) adds this variant for `MATCH p = shortestPath(...)`.
    /// Serialized to RESP3 as Array[Integer] of node IDs.
    Path(Vec<NodeKey>),
}

/// A single result row: variable bindings.
pub type Row = HashMap<String, Value>;

/// Execution error.
#[derive(Debug)]
pub enum ExecError {
    GraphNotFound,
    NodeNotFound,
    TypeError(String),
    Unsupported(String),
}

impl core::fmt::Display for ExecError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::GraphNotFound => write!(f, "graph not found"),
            Self::NodeNotFound => write!(f, "node not found"),
            Self::TypeError(msg) => write!(f, "type error: {msg}"),
            Self::Unsupported(msg) => write!(f, "unsupported: {msg}"),
        }
    }
}

/// Per-operator profiling statistics.
pub struct OpProfile {
    pub name: &'static str,
    pub row_count: u64,
    pub duration_us: u64,
}

/// Execute result with per-operator profiling.
pub struct ProfileResult {
    pub exec_result: ExecResult,
    pub operator_profiles: Vec<OpProfile>,
}

/// Record of a mutation performed during execute_mut, used for WAL generation.
#[derive(Debug)]
pub enum MutationRecord {
    CreateNode {
        node_id: u64,
        labels: SmallVec<[u16; 4]>,
        properties: PropertyMap,
        embedding: Option<Vec<f32>>,
    },
    CreateEdge {
        edge_id: u64,
        src_id: u64,
        dst_id: u64,
        edge_type: u16,
        weight: f64,
        properties: Option<PropertyMap>,
    },
}

/// Intent describing a Cypher-created entity that must be rolled back on
/// `TXN.ABORT`. Produced by [`graph_query_or_write`] (Phase 167, CYP-01/02)
/// for every `PhysicalOp::CreatePattern` node/edge and every `PhysicalOp::Merge`
/// create-branch node/edge. Idempotent MERGE match-branches produce no intent.
///
/// `entity_id` is the `NodeKey` or `EdgeKey` encoded via `slotmap::KeyData::as_ffi()`,
/// matching the format consumed by [`crate::transaction::abort::abort_cross_store_txn`].
#[derive(Debug, Clone, Copy)]
pub struct GraphWriteIntent {
    /// `NodeKey::data().as_ffi()` or `EdgeKey::data().as_ffi()`.
    pub entity_id: u64,
    /// `true` for a node, `false` for an edge.
    pub is_node: bool,
}

/// Execute result: column headers + data rows + statistics.
pub struct ExecResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub nodes_created: u64,
    pub nodes_deleted: u64,
    pub properties_set: u64,
    pub execution_time_us: u64,
    /// Mutations performed during execute_mut, for WAL record generation.
    pub mutations: Vec<MutationRecord>,
}

/// Context passed to the Cypher executor for MVCC + bi-temporal queries.
///
/// All fields have zero-value defaults: `ExecutionContext::default()` is
/// equivalent to the current non-transactional, non-temporal read behavior.
#[derive(Debug, Clone, Default)]
pub struct ExecutionContext {
    /// Snapshot LSN for MVCC reads (0 = non-transactional).
    pub snapshot_lsn: u64,
    /// Caller's transaction ID (0 = no transaction).
    pub my_txn_id: u64,
    /// Valid-time filter: only entities with valid_from <= T <= valid_to.
    /// None = no valid-time filter (current behavior).
    pub valid_time_as_of: Option<i64>,
}

// ---------------------------------------------------------------------------
// Use slotmap::Key for as_ffi()
// ---------------------------------------------------------------------------
use slotmap::Key;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::store::GraphStore;
    use bytes::Bytes;
    use smallvec::SmallVec;

    #[test]
    fn test_execute_simple_match_return() {
        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"test"), 64_000, 0)
            .expect("create ok");
        let graph_mut = store.get_graph_mut(b"test").expect("graph");
        let label_id = label_to_id(b"Person");
        let prop_id = label_to_id(b"name");
        let mut props = SmallVec::new();
        props.push((prop_id, PropertyValue::String(Bytes::from_static(b"Alice"))));
        graph_mut
            .write_buf
            .add_node(SmallVec::from_elem(label_id, 1), props, None, 1);

        let query =
            crate::graph::cypher::parse_cypher(b"MATCH (n:Person) RETURN n.name").expect("parse");
        let plan = crate::graph::cypher::planner::compile(&query).expect("compile");
        let graph = store.get_graph(b"test").expect("graph");
        let result =
            execute(graph, &plan, &HashMap::new(), &ExecutionContext::default()).expect("exec");

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0], "n.name");
        assert_eq!(result.rows.len(), 1);
        match &result.rows[0][0] {
            Value::String(s) => assert_eq!(s, "Alice"),
            other => panic!("expected String, got {other:?}"),
        }
    }

    #[test]
    fn test_execute_filter_by_property() {
        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"test"), 64_000, 0)
            .expect("create ok");
        let graph_mut = store.get_graph_mut(b"test").expect("graph");
        let label_id = label_to_id(b"Person");
        let age_id = label_to_id(b"age");

        // Add Alice (age 35) and Bob (age 25).
        let mut p1 = SmallVec::new();
        p1.push((age_id, PropertyValue::Int(35)));
        graph_mut
            .write_buf
            .add_node(SmallVec::from_elem(label_id, 1), p1, None, 1);

        let mut p2 = SmallVec::new();
        p2.push((age_id, PropertyValue::Int(25)));
        graph_mut
            .write_buf
            .add_node(SmallVec::from_elem(label_id, 1), p2, None, 2);

        let query =
            crate::graph::cypher::parse_cypher(b"MATCH (n:Person) WHERE n.age > 30 RETURN n.age")
                .expect("parse");
        let plan = crate::graph::cypher::planner::compile(&query).expect("compile");
        let graph = store.get_graph(b"test").expect("graph");
        let result =
            execute(graph, &plan, &HashMap::new(), &ExecutionContext::default()).expect("exec");

        assert_eq!(result.rows.len(), 1);
        match &result.rows[0][0] {
            Value::Int(n) => assert_eq!(*n, 35),
            other => panic!("expected Int(35), got {other:?}"),
        }
    }

    #[test]
    fn test_execute_with_limit() {
        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"test"), 64_000, 0)
            .expect("create ok");
        let graph_mut = store.get_graph_mut(b"test").expect("graph");
        let label_id = label_to_id(b"Person");

        for i in 0..10 {
            graph_mut.write_buf.add_node(
                SmallVec::from_elem(label_id, 1),
                SmallVec::new(),
                None,
                i,
            );
        }

        let query = crate::graph::cypher::parse_cypher(b"MATCH (n:Person) RETURN n LIMIT 3")
            .expect("parse");
        let plan = crate::graph::cypher::planner::compile(&query).expect("compile");
        let graph = store.get_graph(b"test").expect("graph");
        let result =
            execute(graph, &plan, &HashMap::new(), &ExecutionContext::default()).expect("exec");

        assert_eq!(result.rows.len(), 3);
    }

    #[test]
    fn test_value_comparison() {
        use std::cmp::Ordering;

        assert_eq!(
            compare_values(&Value::Int(1), &Value::Int(2)),
            Ordering::Less
        );
        assert_eq!(
            compare_values(&Value::Int(5), &Value::Float(3.0)),
            Ordering::Greater
        );
        assert_eq!(compare_values(&Value::Null, &Value::Int(0)), Ordering::Less);
        assert_eq!(
            compare_values(&Value::String("abc".into()), &Value::String("def".into())),
            Ordering::Less
        );
    }

    #[test]
    fn test_binary_ops() {
        assert!(matches!(
            eval_binary_op(&Value::Int(2), BinaryOperator::Add, &Value::Int(3)),
            Value::Int(5)
        ));
        assert!(matches!(
            eval_binary_op(&Value::Int(10), BinaryOperator::Div, &Value::Int(3)),
            Value::Int(3)
        ));
        assert!(matches!(
            eval_binary_op(&Value::Int(2), BinaryOperator::Equal, &Value::Float(2.0)),
            Value::Bool(true)
        ));
        assert!(matches!(
            eval_binary_op(
                &Value::String("hello".into()),
                BinaryOperator::Add,
                &Value::String(" world".into())
            ),
            Value::String(ref s) if s == "hello world"
        ));
    }

    #[test]
    fn test_execute_merge_create_when_not_found() {
        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"test"), 64_000, 0)
            .expect("create ok");

        let query = crate::graph::cypher::parse_cypher(
            b"MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.age = 30 RETURN n",
        )
        .expect("parse");
        let plan = crate::graph::cypher::planner::compile(&query).expect("compile");

        let graph = store.get_graph_mut(b"test").expect("graph");
        let result = execute_mut(graph, &plan, &HashMap::new(), 0).expect("exec");

        assert_eq!(result.nodes_created, 1);
        assert_eq!(result.rows.len(), 1);
        // Verify the node has the right properties.
        let graph_ref = store.get_graph(b"test").expect("graph");
        let nodes: Vec<_> = graph_ref.write_buf.iter_nodes().collect();
        assert_eq!(nodes.len(), 1);
        let node = nodes[0].1;
        let name_id = label_to_id(b"name");
        let age_id = label_to_id(b"age");
        assert!(node.properties.iter().any(
            |(p, v)| *p == name_id && *v == PropertyValue::String(Bytes::from_static(b"Alice"))
        ));
        assert!(
            node.properties
                .iter()
                .any(|(p, v)| *p == age_id && *v == PropertyValue::Int(30))
        );
    }

    #[test]
    fn test_execute_merge_match_when_found() {
        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"test"), 64_000, 0)
            .expect("create ok");

        // Pre-insert Alice.
        let graph = store.get_graph_mut(b"test").expect("graph");
        let label_id = label_to_id(b"Person");
        let name_id = label_to_id(b"name");
        let mut props = SmallVec::new();
        props.push((name_id, PropertyValue::String(Bytes::from_static(b"Alice"))));
        graph
            .write_buf
            .add_node(SmallVec::from_elem(label_id, 1), props, None, 1);

        // Now MERGE should find Alice and apply ON MATCH SET.
        let query = crate::graph::cypher::parse_cypher(
            b"MERGE (n:Person {name: 'Alice'}) ON MATCH SET n.updated = true RETURN n",
        )
        .expect("parse");
        let plan = crate::graph::cypher::planner::compile(&query).expect("compile");

        let graph = store.get_graph_mut(b"test").expect("graph");
        let result = execute_mut(graph, &plan, &HashMap::new(), 0).expect("exec");

        assert_eq!(result.nodes_created, 0, "should not create a new node");
        assert_eq!(result.rows.len(), 1);
        // Verify updated property was set.
        let graph_ref = store.get_graph(b"test").expect("graph");
        let nodes: Vec<_> = graph_ref.write_buf.iter_nodes().collect();
        assert_eq!(nodes.len(), 1);
        let node = nodes[0].1;
        let updated_id = label_to_id(b"updated");
        assert!(
            node.properties
                .iter()
                .any(|(p, v)| *p == updated_id && *v == PropertyValue::Bool(true)),
            "expected updated=true on matched node, got: {:?}",
            node.properties
        );
    }

    #[test]
    fn test_execute_merge_edge_pattern() {
        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"test"), 64_000, 0)
            .expect("create ok");

        // MERGE edge pattern: should create both nodes and the edge.
        let query = crate::graph::cypher::parse_cypher(
            b"MERGE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, b",
        )
        .expect("parse");
        let plan = crate::graph::cypher::planner::compile(&query).expect("compile");

        let graph = store.get_graph_mut(b"test").expect("graph");
        let result = execute_mut(graph, &plan, &HashMap::new(), 0).expect("exec");

        assert_eq!(result.nodes_created, 2, "should create two nodes");
        assert_eq!(result.rows.len(), 1);

        // Verify the graph has 2 nodes and 1 edge.
        let graph_ref = store.get_graph(b"test").expect("graph");
        assert_eq!(graph_ref.write_buf.node_count(), 2);
        assert_eq!(graph_ref.write_buf.edge_count(), 1);
    }
}
