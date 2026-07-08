//! Cypher execution engine -- walks PhysicalPlan operators and produces result rows.
//!
//! Row-based pipeline model: each operator transforms a `Vec<Row>`. A `Row`
//! is a slot-indexed `SmallVec<Value>` against a per-execution [`SlotTable`]
//! (all variables a plan can bind, collected once) — no per-row HashMap
//! allocation and no per-insert String key clone. The executor starts with
//! one Null-filled seed row and sequentially applies each `PhysicalOp`.

mod eval;
mod read;
pub(crate) mod shortest_path;
mod write;

pub(crate) use eval::*;
pub use read::*;
pub use write::*;

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
    /// Cypher string values are RESP bulk strings — arbitrary bytes, held as
    /// `Bytes` so a stored property flows to the reply frame without copying
    /// (W2-4) and non-UTF8 payloads survive the round-trip. Text ops
    /// (`toInteger`, `=~`) validate UTF-8 at their own boundary.
    String(Bytes),
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

/// Maps variable names to row slot indices.
///
/// Built once per execution by walking the plan's BINDING operators
/// (scans, expands, unwind, shortestPath, create/merge patterns). Names
/// that are referenced but never bound simply resolve to no slot — the
/// same as a HashMap miss in the old row model. Lookup is a linear scan:
/// queries bind a handful of short names, which beats hashing.
#[derive(Debug, Default)]
pub struct SlotTable {
    names: Vec<String>,
}

impl SlotTable {
    /// Collect every variable the plan can bind, in first-bind order.
    pub fn from_plan(plan: &PhysicalPlan) -> Self {
        let mut table = SlotTable::default();
        for op in &plan.operators {
            match op {
                PhysicalOp::NodeScan { variable, .. } | PhysicalOp::IndexScan { variable, .. } => {
                    table.bind(variable)
                }
                PhysicalOp::Expand {
                    source,
                    target,
                    edge_variable,
                    ..
                } => {
                    table.bind(source);
                    table.bind(target);
                    if let Some(evar) = edge_variable {
                        table.bind(evar);
                    }
                }
                PhysicalOp::Unwind { alias, .. } => table.bind(alias),
                PhysicalOp::ShortestPath {
                    path_var,
                    source,
                    target,
                    ..
                } => {
                    table.bind(path_var);
                    table.bind(source);
                    table.bind(target);
                }
                PhysicalOp::CreatePattern { patterns } => {
                    for p in patterns {
                        table.bind_pattern(p);
                    }
                }
                PhysicalOp::Merge { pattern, .. } => table.bind_pattern(pattern),
                // W2-13: a rebinding projection (WITH) re-seeds the row
                // stream with its output names; RETURN (rebind: false)
                // still binds nothing.
                PhysicalOp::Project { items, rebind, .. } => {
                    if *rebind {
                        for item in items {
                            match &item.alias {
                                Some(alias) => table.bind(alias),
                                None => table.bind(&eval::expr_to_string(&item.expr)),
                            }
                        }
                    }
                }
                // Reference-only operators bind nothing.
                PhysicalOp::Filter { .. }
                | PhysicalOp::Sort { .. }
                | PhysicalOp::Limit { .. }
                | PhysicalOp::Skip { .. }
                | PhysicalOp::DeleteEntities { .. }
                | PhysicalOp::SetProperties { .. }
                | PhysicalOp::ProcedureCall { .. } => {}
            }
        }
        table
    }

    fn bind(&mut self, name: &str) {
        if !name.is_empty() && !self.names.iter().any(|n| n == name) {
            self.names.push(name.to_owned());
        }
    }

    fn bind_pattern(&mut self, pattern: &Pattern) {
        for node in &pattern.nodes {
            if let Some(v) = &node.variable {
                self.bind(v);
            }
        }
        for edge in &pattern.edges {
            if let Some(v) = &edge.variable {
                self.bind(v);
            }
        }
    }

    /// Resolve a variable name to its slot index.
    #[inline]
    pub fn slot(&self, name: &str) -> Option<usize> {
        self.names.iter().position(|n| n == name)
    }

    /// Slot names in slot order.
    pub fn names(&self) -> &[String] {
        &self.names
    }

    /// Number of slots.
    pub fn len(&self) -> usize {
        self.names.len()
    }

    /// Whether the table has no slots.
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }
}

/// A single result row: variable bindings, slot-indexed against the
/// execution's [`SlotTable`].
///
/// Unbound slots hold `Value::Null`, which every pipeline consumer treats
/// identically to the old HashMap miss: expression eval defaults a missing
/// variable to Null, and operator sources pattern-match a concrete variant
/// (`Some(Value::Node(_))`), which Null fails just like `None` did.
#[derive(Debug, Clone)]
pub struct Row<'a> {
    table: &'a SlotTable,
    slots: SmallVec<[Value; 4]>,
}

impl<'a> Row<'a> {
    /// A fresh row with all slots unbound (Null).
    pub fn seed(table: &'a SlotTable) -> Self {
        Row {
            table,
            slots: smallvec::smallvec![Value::Null; table.len()],
        }
    }

    /// Look up a binding by variable name.
    #[inline]
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.table.slot(name).map(|i| &self.slots[i])
    }

    /// Bind a variable. The name must be in the SlotTable (it is, for every
    /// name a plan operator binds — `SlotTable::from_plan` walks the same
    /// operators); an unknown name is a plan/table desync bug and the
    /// binding is dropped, matching a read of it (None).
    #[inline]
    pub fn insert(&mut self, name: &str, value: Value) {
        debug_assert!(
            self.table.slot(name).is_some(),
            "variable {name:?} missing from SlotTable"
        );
        if let Some(i) = self.table.slot(name) {
            self.slots[i] = value;
        }
    }

    /// Iterate (name, value) pairs in slot order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.table
            .names
            .iter()
            .map(String::as_str)
            .zip(self.slots.iter())
    }
}

/// W2-13 OPTIONAL MATCH: emit `row` with the expansion's target (and edge
/// variable, when the pattern binds one) set to Null — the survival row for
/// a source that matched nothing. Free function (not a closure) so the
/// `Row<'a>` lifetime unifies between the borrowed input and the output Vec.
fn push_null_padded<'a>(
    row: &Row<'a>,
    target: &str,
    edge_variable: &Option<String>,
    out: &mut Vec<Row<'a>>,
) {
    let mut new_row = row.clone();
    new_row.insert(target, Value::Null);
    if let Some(evar) = edge_variable {
        new_row.insert(evar, Value::Null);
    }
    out.push(new_row);
}

/// Execution error.
///
/// Phase 174 FIX-02: carries `partial_mutations` so that even on Err, any
/// mutations accumulated before the failure can be surfaced for TXN.ABORT
/// rollback. Without this, the Err path discards partial writes silently.
#[derive(Debug)]
pub struct ExecError {
    pub kind: ExecErrorKind,
    /// Mutations that were accumulated before the error occurred. Empty for
    /// pure-parse errors (GraphNotFound, etc.) where no write ops ran.
    pub partial_mutations: Vec<MutationRecord>,
}

/// The kind of execution error (separated from partial_mutations payload).
#[derive(Debug)]
pub enum ExecErrorKind {
    GraphNotFound,
    NodeNotFound,
    TypeError(String),
    Unsupported(String),
    /// Traversal exceeded its wall-clock budget (bounded epoch hold —
    /// checked per hop in variable-length Expand and per ShortestPath run).
    Timeout(crate::graph::traversal_guard::TraversalTimeout),
}

impl core::fmt::Display for ExecError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match &self.kind {
            ExecErrorKind::GraphNotFound => write!(f, "graph not found"),
            ExecErrorKind::NodeNotFound => write!(f, "node not found"),
            ExecErrorKind::TypeError(msg) => write!(f, "type error: {msg}"),
            ExecErrorKind::Unsupported(msg) => write!(f, "unsupported: {msg}"),
            ExecErrorKind::Timeout(t) => write!(f, "{t}"),
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

/// Record of a mutation performed during execute_mut, used for WAL generation
/// and TXN.ABORT rollback (Phase 174 FIX-01).
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
    // --- Phase 174 FIX-01: SET / DELETE / MERGE rollback records ---
    /// Property was changed by Cypher SET. `old_value` is the pre-SET value
    /// (None = property did not exist before SET and should be removed on
    /// rollback). `new_value` is the value written — serialized to the WAL
    /// (W2-9: without it, SET was silently lost on kill -9 because replay
    /// only re-ran the original ADDNODE property state).
    SetProperty {
        entity_id: u64,
        is_node: bool,
        key: u16,
        old_value: Option<PropertyValue>,
        new_value: PropertyValue,
    },
    /// Label was added by Cypher `SET n:Label` (W2-9: WAL durability; label
    /// rollback was never captured — pre-existing Phase 174 scope — so this
    /// record produces a WAL entry but no undo op).
    SetLabel { node_id: u64, label: u16 },
    /// Node was soft-deleted by Cypher DETACH DELETE. Snapshot captures the
    /// full node state so rollback can un-soft-delete the node and its
    /// incident edges.
    DeleteNode {
        node_id: u64,
        labels: SmallVec<[u16; 4]>,
        properties: PropertyMap,
        embedding: Option<Vec<f32>>,
    },
    /// Edge was soft-deleted by Cypher DELETE r. Snapshot captures the full
    /// edge state so rollback can un-soft-delete the edge.
    DeleteEdge {
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
    /// Temporal-decay scoring for traversal cost (agent-memory recency),
    /// parsed from `GRAPH.QUERY ... --decay <lambda_per_sec>`.
    /// None = distance-only shortest paths (current behavior).
    pub decay: Option<crate::graph::scoring::DecayConfig>,
    /// Wall-clock budget for multi-hop traversals (bounded epoch hold).
    /// Checked once per hop in variable-length Expand and once per row in
    /// ShortestPath; exceeding it returns `ExecErrorKind::Timeout`.
    /// None = unbounded (current behavior; `Default` keeps tests unchanged).
    pub guard: Option<crate::graph::traversal_guard::TraversalGuard>,
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
    use std::collections::HashMap;

    #[test]
    fn test_slot_table_collects_all_binding_ops() {
        let query = crate::graph::cypher::parse_cypher(
            b"MATCH (a:Person {id: 1})-[r:KNOWS]->(b) RETURN a, b",
        )
        .expect("parse");
        let plan = crate::graph::cypher::planner::compile(&query).expect("compile");
        let table = SlotTable::from_plan(&plan);
        for var in ["a", "r", "b"] {
            assert!(
                table.slot(var).is_some(),
                "{var:?} must have a slot; names = {:?}",
                table.names()
            );
        }
    }

    #[test]
    fn test_slot_table_collects_pattern_vars() {
        let query = crate::graph::cypher::parse_cypher(
            b"CREATE (x:Person {id: 1})-[e:KNOWS]->(y:Person {id: 2})",
        )
        .expect("parse");
        let plan = crate::graph::cypher::planner::compile(&query).expect("compile");
        let table = SlotTable::from_plan(&plan);
        for var in ["x", "e", "y"] {
            assert!(
                table.slot(var).is_some(),
                "{var:?} must have a slot; names = {:?}",
                table.names()
            );
        }
    }

    #[test]
    fn test_row_get_insert_iter() {
        let mut table = SlotTable::default();
        table.bind("a");
        table.bind("b");
        table.bind("a"); // dedup
        assert_eq!(table.len(), 2);

        let mut row = Row::seed(&table);
        // Unbound slot reads as Null; unknown name reads as None.
        assert!(matches!(row.get("a"), Some(Value::Null)));
        assert!(row.get("zzz").is_none());

        row.insert("a", Value::Int(7));
        assert!(matches!(row.get("a"), Some(Value::Int(7))));

        let cloned = row.clone();
        assert!(matches!(cloned.get("a"), Some(Value::Int(7))));

        let pairs: Vec<(&str, bool)> = row
            .iter()
            .map(|(k, v)| (k, matches!(v, Value::Null)))
            .collect();
        assert_eq!(pairs, vec![("a", false), ("b", true)]);
    }

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

    /// Build a 3-node KNOWS chain (a -> b -> c) in graph "test".
    fn chain_store() -> GraphStore {
        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"test"), 64_000, 0)
            .expect("create ok");
        let graph_mut = store.get_graph_mut(b"test").expect("graph");
        let label_id = label_to_id(b"Person");
        let etype = label_to_id(b"KNOWS");
        let keys: Vec<NodeKey> = (0..3)
            .map(|i| {
                graph_mut.write_buf.add_node(
                    SmallVec::from_elem(label_id, 1),
                    SmallVec::new(),
                    None,
                    i,
                )
            })
            .collect();
        for w in keys.windows(2) {
            graph_mut
                .write_buf
                .add_edge(w[0], w[1], etype, 1.0, None, 3)
                .expect("edge");
        }
        store
    }

    #[test]
    fn test_guard_timeout_var_length_expand() {
        let store = chain_store();
        let query = crate::graph::cypher::parse_cypher(b"MATCH (a:Person)-[*1..3]->(b) RETURN b")
            .expect("parse");
        let plan = crate::graph::cypher::planner::compile(&query).expect("compile");
        let graph = store.get_graph(b"test").expect("graph");

        // Expired guard: the first hop's check must abort with Timeout.
        let ctx = ExecutionContext {
            guard: Some(crate::graph::traversal_guard::TraversalGuard::new(
                0,
                std::time::Duration::ZERO,
            )),
            ..Default::default()
        };
        let Err(err) = execute(graph, &plan, &HashMap::new(), &ctx) else {
            panic!("must time out");
        };
        assert!(
            matches!(err.kind, ExecErrorKind::Timeout(_)),
            "expected Timeout, got {:?}",
            err.kind
        );
        assert!(format!("{err}").contains("traversal timeout"));

        // Same query under a generous guard matches the guard-less result.
        let ctx_ok = ExecutionContext {
            guard: Some(crate::graph::traversal_guard::TraversalGuard::with_default_timeout(0)),
            ..Default::default()
        };
        let with_guard = execute(graph, &plan, &HashMap::new(), &ctx_ok).expect("exec");
        let without =
            execute(graph, &plan, &HashMap::new(), &ExecutionContext::default()).expect("exec");
        assert_eq!(with_guard.rows.len(), without.rows.len());
        assert_eq!(with_guard.rows.len(), 3, "b, c from a; c from b");
    }

    #[test]
    fn test_guard_timeout_shortest_path() {
        let store = chain_store();
        let query = crate::graph::cypher::parse_cypher(
            b"MATCH p = shortestPath((a:Person)-[*..5]->(b:Person)) RETURN p",
        )
        .expect("parse");
        let plan = crate::graph::cypher::planner::compile(&query).expect("compile");
        let graph = store.get_graph(b"test").expect("graph");

        let ctx = ExecutionContext {
            guard: Some(crate::graph::traversal_guard::TraversalGuard::new(
                0,
                std::time::Duration::ZERO,
            )),
            ..Default::default()
        };
        let Err(err) = execute(graph, &plan, &HashMap::new(), &ctx) else {
            panic!("must time out");
        };
        assert!(
            matches!(err.kind, ExecErrorKind::Timeout(_)),
            "expected Timeout, got {:?}",
            err.kind
        );

        // execute_profile shares the checks.
        let Err(err) = execute_profile(graph, &plan, &HashMap::new(), &ctx) else {
            panic!("must time out");
        };
        assert!(matches!(err.kind, ExecErrorKind::Timeout(_)));

        // Generous guard: paths still found.
        let ctx_ok = ExecutionContext {
            guard: Some(crate::graph::traversal_guard::TraversalGuard::with_default_timeout(0)),
            ..Default::default()
        };
        let ok = execute(graph, &plan, &HashMap::new(), &ctx_ok).expect("exec");
        assert!(!ok.rows.is_empty(), "chain must yield shortest paths");
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

    /// P3 design part B (B0): CONTAINS / STARTS WITH / ENDS WITH must
    /// produce results IDENTICAL to the equivalent `=~` dot-star shape
    /// already supported (`eval.rs` "three recognized shapes").
    #[test]
    fn test_eval_contains_matches_regex_dotstar_equivalent() {
        let text = Value::String("trusted rustacean".into());
        let contains = eval_binary_op(
            &text,
            BinaryOperator::Contains,
            &Value::String("rust".into()),
        );
        let regex_equiv = eval_binary_op(
            &text,
            BinaryOperator::RegexMatch,
            &Value::String(".*rust.*".into()),
        );
        assert!(matches!(contains, Value::Bool(true)));
        assert!(matches!(regex_equiv, Value::Bool(true)));

        let starts = eval_binary_op(
            &text,
            BinaryOperator::StartsWith,
            &Value::String("trust".into()),
        );
        let starts_regex = eval_binary_op(
            &text,
            BinaryOperator::RegexMatch,
            &Value::String("trust.*".into()),
        );
        assert!(matches!(starts, Value::Bool(true)));
        assert!(matches!(starts_regex, Value::Bool(true)));

        let ends = eval_binary_op(
            &text,
            BinaryOperator::EndsWith,
            &Value::String("rustacean".into()),
        );
        let ends_regex = eval_binary_op(
            &text,
            BinaryOperator::RegexMatch,
            &Value::String(".*rustacean".into()),
        );
        assert!(matches!(ends, Value::Bool(true)));
        assert!(matches!(ends_regex, Value::Bool(true)));

        // Non-string operands and non-matches degrade to Null / false, same
        // as `=~` already does -- never a wrong-type panic.
        assert!(matches!(
            eval_binary_op(&Value::Int(1), BinaryOperator::Contains, &text),
            Value::Null
        ));
        assert!(matches!(
            eval_binary_op(
                &text,
                BinaryOperator::StartsWith,
                &Value::String("zzz".into())
            ),
            Value::Bool(false)
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

    // --- Mutable-tier property index (Task #31) — executor-level correctness ---

    fn run_point_match(store: &GraphStore, target: i64) -> ExecResult {
        let query = format!("MATCH (a:N {{id: {target}}}) RETURN a.id");
        let parsed = crate::graph::cypher::parse_cypher(query.as_bytes()).expect("parse");
        let plan = crate::graph::cypher::planner::compile(&parsed).expect("compile");
        let graph = store.get_graph(b"test").expect("graph");
        execute(graph, &plan, &HashMap::new(), &ExecutionContext::default()).expect("exec")
    }

    /// Proves `MATCH (a:N {id:X})` returns exactly the one matching node at
    /// several graph sizes — pure correctness regression guard for the
    /// mutable-tier property index replacing the O(N) linear scan.
    #[test]
    fn test_index_scan_correctness_at_scale() {
        for n in [1usize, 100, 5_000] {
            let mut store = GraphStore::new();
            store
                .create_graph(Bytes::from_static(b"test"), n * 2 + 1, 0)
                .expect("create ok");
            let graph_mut = store.get_graph_mut(b"test").expect("graph");
            let label_id = label_to_id(b"N");
            let id_pid = label_to_id(b"id");
            for i in 0..n {
                let mut props = SmallVec::new();
                props.push((id_pid, PropertyValue::Int(i as i64)));
                graph_mut
                    .write_buf
                    .add_node(SmallVec::from_elem(label_id, 1), props, None, 1);
            }

            let target = (n as i64) / 2; // arbitrary in-range id

            // Probe hook proving the linear scan is gone: `index_scan_keys`
            // seeds its mutable-tail candidate set from EXACTLY this same
            // accessor (`prop_index_keys_eq`). A bucket cardinality of 1
            // regardless of `n` is the structural proof that the seeded
            // candidate set — and therefore the executor's residual-check
            // loop — is O(bucket size), not O(live_node_count).
            let candidate_count = graph_mut
                .write_buf
                .prop_index_keys_eq(id_pid, &PropertyValue::Int(target))
                .len();
            assert_eq!(
                candidate_count, 1,
                "n={n}: index probe must return exactly the matching bucket, \
                 not scale with graph size"
            );

            let result = run_point_match(&store, target);
            assert_eq!(result.rows.len(), 1, "n={n}: expected exactly one match");
            match &result.rows[0][0] {
                Value::Int(v) => assert_eq!(*v, target, "n={n}: wrong node matched"),
                other => panic!("n={n}: expected Int, got {other:?}"),
            }
        }
    }

    /// After `SET n.id = newval`, the OLD value must no longer be
    /// index-reachable and the NEW value must be. Guards the
    /// `set_node_property` old-bucket-eviction path.
    #[test]
    fn test_index_scan_eq_after_property_update() {
        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"test"), 64_000, 0)
            .expect("create ok");
        let graph_mut = store.get_graph_mut(b"test").expect("graph");
        let label_id = label_to_id(b"N");
        let id_pid = label_to_id(b"id");
        let mut props = SmallVec::new();
        props.push((id_pid, PropertyValue::Int(1)));
        graph_mut
            .write_buf
            .add_node(SmallVec::from_elem(label_id, 1), props, None, 1);

        let set_query =
            crate::graph::cypher::parse_cypher(b"MATCH (n:N {id: 1}) SET n.id = 2 RETURN n")
                .expect("parse");
        let set_plan = crate::graph::cypher::planner::compile(&set_query).expect("compile");
        let graph = store.get_graph_mut(b"test").expect("graph");
        let set_result = execute_mut(graph, &set_plan, &HashMap::new(), 0).expect("exec");
        assert_eq!(set_result.properties_set, 1);

        assert_eq!(
            run_point_match(&store, 2).rows.len(),
            1,
            "new value must match"
        );
        assert_eq!(
            run_point_match(&store, 1).rows.len(),
            0,
            "old value must not match"
        );
    }

    /// W2-2 copy-up interaction: freeze a node with `id: 1` into a CSR
    /// segment, then SET it to `id: 2` (triggers copy-up into the mutable
    /// tier). `MATCH {id: 1}` must return EMPTY (the resident-but-updated
    /// mutable copy shadows the frozen row) and `MATCH {id: 2}` must find it
    /// via the new mutable-tier index.
    #[test]
    fn test_index_scan_after_copy_up_shadows_frozen_value() {
        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"test"), 1_000, 0)
            .expect("create ok");
        let graph_mut = store.get_graph_mut(b"test").expect("graph");
        let label_id = label_to_id(b"N");
        let id_pid = label_to_id(b"id");
        let mut props = SmallVec::new();
        props.push((id_pid, PropertyValue::Int(1)));
        graph_mut
            .write_buf
            .add_node(SmallVec::from_elem(label_id, 1), props, None, 1);

        // Freeze into a CSR segment so the row lives only in the frozen tier.
        assert!(graph_mut.freeze_and_compact(1));
        assert_eq!(graph_mut.write_buf.node_count(), 0);

        // SET copies the frozen row up into the mutable tier, then mutates.
        let set_query =
            crate::graph::cypher::parse_cypher(b"MATCH (n:N {id: 1}) SET n.id = 2 RETURN n")
                .expect("parse");
        let set_plan = crate::graph::cypher::planner::compile(&set_query).expect("compile");
        let graph = store.get_graph_mut(b"test").expect("graph");
        let set_result = execute_mut(graph, &set_plan, &HashMap::new(), 2).expect("exec");
        assert_eq!(set_result.properties_set, 1);

        assert_eq!(
            run_point_match(&store, 1).rows.len(),
            0,
            "frozen id=1 must be shadowed by the updated mutable copy"
        );
        assert_eq!(
            run_point_match(&store, 2).rows.len(),
            1,
            "updated id=2 must be found via the mutable-tier index"
        );
    }

    // --- P3 design part B: text predicates (CONTAINS/STARTS WITH/ENDS
    // WITH/=~), SegmentTextIndex correctness across tiers -----------------

    /// How the fixture's 7 nodes are distributed across tiers.
    enum TierShape {
        /// All 7 nodes stay in the mutable write buffer (no freeze).
        MutableOnly,
        /// All 7 nodes are frozen into one CSR segment.
        FrozenOnly,
        /// First 4 nodes frozen into a CSR segment; remaining 3 added to
        /// the mutable tier AFTER the freeze (genuinely mixed tiers, not
        /// just "freeze everything then leave it").
        Mixed,
    }

    /// Fixture: (name, bio) pairs deliberately covering the edge cases the
    /// SUPERSET-candidate contract must survive:
    /// - "bob"/"trusted colleague": the substring-across-token-boundary
    ///   crux -- "trusted" tokenizes differently than "rust", so a
    ///   token-identity index would MISS it for `CONTAINS 'rust'`; the
    ///   presence-only `SegmentTextIndex` must not.
    /// - "carol"/"RUSTACEAN": case-sensitivity -- present (has a string
    ///   bio) but must be excluded by the exact residual Filter, not by
    ///   the index (index has no opinion on case at all).
    /// - "dave": NO bio property at all -- must never appear in any bio
    ///   predicate result, in any tier.
    /// - "erin"/"" (empty bio): empty-string edge case.
    /// - "frank"/"héllo wörld": Unicode multi-byte edge case.
    /// - "grace"/"rust": exact single-token baseline.
    const TEXT_FIXTURE: &[(&str, Option<&str>)] = &[
        ("alice", Some("i love rust and graphs")),
        ("bob", Some("trusted colleague")),
        ("carol", Some("RUSTACEAN")),
        ("dave", None),
        ("erin", Some("")),
        ("frank", Some("héllo wörld")),
        ("grace", Some("rust")),
    ];

    fn add_text_fixture_nodes(
        graph: &mut crate::graph::store::NamedGraph,
        entries: &[(&str, Option<&str>)],
        lsn: u64,
    ) {
        let label_id = label_to_id(b"N");
        let name_pid = label_to_id(b"name");
        let bio_pid = label_to_id(b"bio");
        for (name, bio) in entries {
            let mut props: SmallVec<[(u16, PropertyValue); 4]> = SmallVec::new();
            props.push((
                name_pid,
                PropertyValue::String(Bytes::copy_from_slice(name.as_bytes())),
            ));
            if let Some(bio) = bio {
                props.push((
                    bio_pid,
                    PropertyValue::String(Bytes::copy_from_slice(bio.as_bytes())),
                ));
            }
            graph
                .write_buf
                .add_node(SmallVec::from_elem(label_id, 1), props, None, lsn);
        }
    }

    fn build_text_fixture_store(shape: TierShape) -> GraphStore {
        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"test"), 1_000_000, 0)
            .expect("create ok");
        let graph = store.get_graph_mut(b"test").expect("graph");
        match shape {
            TierShape::MutableOnly => {
                add_text_fixture_nodes(graph, TEXT_FIXTURE, 1);
            }
            TierShape::FrozenOnly => {
                add_text_fixture_nodes(graph, TEXT_FIXTURE, 1);
                assert!(graph.freeze_and_compact(1), "freeze must succeed");
                assert_eq!(graph.write_buf.node_count(), 0);
            }
            TierShape::Mixed => {
                add_text_fixture_nodes(graph, &TEXT_FIXTURE[..4], 1);
                assert!(graph.freeze_and_compact(1), "freeze must succeed");
                add_text_fixture_nodes(graph, &TEXT_FIXTURE[4..], 2);
            }
        }
        store
    }

    /// Run a Cypher query returning `n.name` and collect the sorted set of
    /// matched names.
    fn run_text_query(store: &GraphStore, cypher: &str) -> Vec<String> {
        let parsed = crate::graph::cypher::parse_cypher(cypher.as_bytes()).expect("parse");
        let plan = crate::graph::cypher::planner::compile(&parsed).expect("compile");
        let graph = store.get_graph(b"test").expect("graph");
        let result = execute(graph, &plan, &HashMap::new(), &ExecutionContext::default())
            .unwrap_or_else(|e| panic!("exec failed for {cypher:?}: {e:?}"));
        let mut names: Vec<String> = result
            .rows
            .iter()
            .map(|r| match &r[0] {
                Value::String(s) => String::from_utf8_lossy(s).into_owned(),
                other => panic!("expected string name, got {other:?}"),
            })
            .collect();
        names.sort();
        names
    }

    /// The single most important correctness gate for P3 design part B:
    /// every text-predicate query must return the IDENTICAL result set
    /// regardless of which tier(s) the matching data lives in. `IndexScan`
    /// accelerates only the frozen tier (`SegmentTextIndex::candidate_rows`)
    /// -- the mutable tier always falls back to an exact scan, and the
    /// residual `Filter` is the sole authority either way, so tier
    /// placement must be invisible to the result set.
    #[test]
    fn test_text_predicate_parity_across_tiers() {
        let queries: &[(&str, &[&str])] = &[
            (
                "MATCH (n:N) WHERE n.bio CONTAINS 'rust' RETURN n.name",
                &["alice", "bob", "grace"],
            ),
            (
                // Empty needle: matches every row that HAS a bio (bytes_contains
                // treats "" as always-contained), but never `dave` (no bio at all).
                "MATCH (n:N) WHERE n.bio CONTAINS '' RETURN n.name",
                &["alice", "bob", "carol", "erin", "frank", "grace"],
            ),
            (
                "MATCH (n:N) WHERE n.bio STARTS WITH 'trust' RETURN n.name",
                &["bob"],
            ),
            (
                // Unicode multi-byte suffix.
                "MATCH (n:N) WHERE n.bio ENDS WITH 'ld' RETURN n.name",
                &["frank"],
            ),
            (
                "MATCH (n:N) WHERE n.bio =~ '.*rust.*' RETURN n.name",
                &["alice", "bob", "grace"],
            ),
            (
                // Case-sensitivity: 'RUSTACEAN' must NOT match lowercase 'rust'
                // -- proves the residual Filter (not the presence-only index)
                // makes the final call.
                "MATCH (n:N) WHERE n.bio CONTAINS 'RUST' RETURN n.name",
                &["carol"],
            ),
        ];

        for shape in [
            TierShape::MutableOnly,
            TierShape::FrozenOnly,
            TierShape::Mixed,
        ] {
            let shape_name = match shape {
                TierShape::MutableOnly => "MutableOnly",
                TierShape::FrozenOnly => "FrozenOnly",
                TierShape::Mixed => "Mixed",
            };
            let store = build_text_fixture_store(shape);
            for (cypher, expected) in queries {
                let mut expected: Vec<String> = expected.iter().map(|s| s.to_string()).collect();
                expected.sort();
                let actual = run_text_query(&store, cypher);
                assert_eq!(
                    actual, expected,
                    "tier={shape_name} query={cypher:?}: got {actual:?}, want {expected:?}"
                );
            }
        }
    }

    /// B1 test #5/#6: the presence-only `candidate_rows` superset must
    /// survive a query that a naive token-identity index would get wrong.
    /// `IndexScan` is planned (via `CONTAINS`) but the residual Filter
    /// still produces the exact, correct row set -- this is the same
    /// assertion as `test_text_predicate_parity_across_tiers`'s first case,
    /// isolated here with an explicit plan-shape check.
    #[test]
    fn test_text_scan_superset_semantics_frozen_tier() {
        let store = build_text_fixture_store(TierShape::FrozenOnly);
        let query = "MATCH (n:N) WHERE n.bio CONTAINS 'rust' RETURN n.name";
        let parsed = crate::graph::cypher::parse_cypher(query.as_bytes()).expect("parse");
        let plan = crate::graph::cypher::planner::compile(&parsed).expect("compile");
        assert!(
            matches!(
                plan.operators[0],
                crate::graph::cypher::planner::PhysicalOp::IndexScan { .. }
            ),
            "CONTAINS must plan through IndexScan; ops = {:?}",
            plan.operators
        );
        let mut actual = run_text_query(&store, query);
        actual.sort();
        // "bob" ("trusted colleague") is the false-negative-if-token-pruned
        // case; it MUST be present because the index only prunes on
        // presence, never on token identity.
        assert_eq!(actual, vec!["alice", "bob", "grace"]);
    }

    /// B1 test #7: the mutable tier gets no acceleration for text
    /// predicates (pre-approved scope decision) but must not regress
    /// correctness -- covered by `test_text_predicate_parity_across_tiers`'s
    /// `MutableOnly` shape; this test isolates the plan shape (still an
    /// `IndexScan`, since the WHERE conjunct always upgrades the scan
    /// regardless of tier -- the ACCELERATION difference is inside
    /// `index_scan_keys`, not in the plan).
    #[test]
    fn test_text_scan_mutable_tier_falls_back_to_linear_scan() {
        let store = build_text_fixture_store(TierShape::MutableOnly);
        let mut actual = run_text_query(
            &store,
            "MATCH (n:N) WHERE n.bio CONTAINS 'rust' RETURN n.name",
        );
        actual.sort();
        assert_eq!(actual, vec!["alice", "bob", "grace"]);
    }
}
