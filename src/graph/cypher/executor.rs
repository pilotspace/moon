//! Cypher execution engine -- walks PhysicalPlan operators and produces result rows.
//!
//! Row-based pipeline model: each operator transforms a `Vec<Row>` where
//! `Row = HashMap<String, Value>`. The executor starts with one empty seed row
//! and sequentially applies each `PhysicalOp` from the plan.

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

/// Execute a physical plan against a named graph.
pub fn execute(
    graph: &NamedGraph,
    plan: &PhysicalPlan,
    params: &HashMap<String, Value>,
) -> Result<ExecResult, ExecError> {
    let start = std::time::Instant::now();

    // Seed row: one empty row to bootstrap the pipeline.
    let mut rows: Vec<Row> = vec![HashMap::new()];
    let mut columns = Vec::new();
    // After Project, rows are converted to positional arrays.
    let mut projected_rows: Option<Vec<Vec<Value>>> = None;
    let nodes_created: u64 = 0;
    let nodes_deleted: u64 = 0;
    let properties_set: u64 = 0;

    let memgraph = &graph.write_buf;

    // Build a SegmentMergeReader for cross-segment neighbor queries.
    let segments_guard = graph.segments.load();
    let csr_segs = &segments_guard.immutable;

    for op in &plan.operators {
        match op {
            PhysicalOp::NodeScan { variable, label } => {
                let label_id = label.as_ref().map(|l| label_to_id(l.as_bytes()));
                let mut new_rows = Vec::new();
                for row in &rows {
                    for (key, node) in memgraph.iter_nodes() {
                        if let Some(lid) = label_id {
                            if !node.labels.contains(&lid) {
                                continue;
                            }
                        }
                        let mut new_row = row.clone();
                        new_row.insert(variable.clone(), Value::Node(key));
                        new_rows.push(new_row);
                    }
                }
                rows = new_rows;
            }

            PhysicalOp::Expand {
                source,
                target,
                edge_types,
                direction,
                min_hops,
                max_hops,
            } => {
                let type_ids: Vec<u16> = edge_types
                    .iter()
                    .map(|t| label_to_id(t.as_bytes()))
                    .collect();

                let dir = match direction {
                    EdgeDirection::Right => Direction::Outgoing,
                    EdgeDirection::Left => Direction::Incoming,
                    EdgeDirection::Both => Direction::Both,
                };

                // Build a per-expand SegmentMergeReader with the correct
                // direction and edge type filter for this operator.
                let edge_type_filter = if type_ids.len() == 1 {
                    Some(type_ids[0])
                } else {
                    None
                };
                let reader = SegmentMergeReader::new(
                    Some(memgraph),
                    csr_segs,
                    dir,
                    u64::MAX,
                    edge_type_filter,
                );

                let mut new_rows = Vec::new();
                for row in &rows {
                    let src_key = match row.get(source) {
                        Some(Value::Node(k)) => *k,
                        _ => continue,
                    };

                    if *max_hops <= 1 {
                        // Single-hop expansion via SegmentMergeReader.
                        for merged in reader.neighbors(src_key) {
                            // Multi-type filter (SegmentMergeReader handles
                            // single-type; we need extra check for multi-type).
                            if type_ids.len() > 1
                                && !type_ids.contains(&merged.edge_type)
                            {
                                continue;
                            }
                            let mut new_row = row.clone();
                            new_row.insert(target.clone(), Value::Node(merged.node));
                            new_rows.push(new_row);
                        }
                    } else {
                        // Variable-length expansion via BFS using SegmentMergeReader.
                        let mut frontier = vec![src_key];
                        let mut visited =
                            std::collections::HashSet::new();
                        visited.insert(src_key);

                        for hop in 1..=*max_hops {
                            let mut next_frontier = Vec::new();
                            for &current in &frontier {
                                for merged in reader.neighbors(current) {
                                    if visited.contains(&merged.node) {
                                        continue;
                                    }
                                    if type_ids.len() > 1
                                        && !type_ids.contains(&merged.edge_type)
                                    {
                                        continue;
                                    }
                                    visited.insert(merged.node);
                                    next_frontier.push(merged.node);

                                    if hop >= *min_hops {
                                        let mut new_row = row.clone();
                                        new_row.insert(
                                            target.clone(),
                                            Value::Node(merged.node),
                                        );
                                        new_rows.push(new_row);
                                    }
                                }
                            }
                            frontier = next_frontier;
                            if frontier.is_empty() {
                                break;
                            }
                        }
                    }
                }
                rows = new_rows;
            }

            PhysicalOp::Filter { expr } => {
                rows.retain(|row| {
                    matches!(eval_expr(expr, row, memgraph, params), Value::Bool(true))
                });
            }

            PhysicalOp::Project { items, distinct } => {
                columns = items
                    .iter()
                    .map(|item| {
                        if let Some(alias) = &item.alias {
                            alias.clone()
                        } else {
                            expr_to_string(&item.expr)
                        }
                    })
                    .collect();

                let mut projected: Vec<Vec<Value>> = rows
                    .iter()
                    .map(|row| {
                        items
                            .iter()
                            .map(|item| {
                                if matches!(item.expr, Expr::Star) {
                                    let entries: Vec<(String, Value)> = row
                                        .iter()
                                        .map(|(k, v)| (k.clone(), v.clone()))
                                        .collect();
                                    Value::Map(entries)
                                } else {
                                    eval_expr(&item.expr, row, memgraph, params)
                                }
                            })
                            .collect()
                    })
                    .collect();

                if *distinct {
                    dedup_rows(&mut projected);
                }

                projected_rows = Some(projected);
                rows.clear();
            }

            PhysicalOp::Sort { items } => {
                if let Some(ref mut pr) = projected_rows {
                    // After projection, sort by evaluating expressions on
                    // positional columns. Build a temporary index mapping.
                    let col_indices: Vec<Option<usize>> = items
                        .iter()
                        .map(|(expr, _)| {
                            let name = expr_to_string(expr);
                            columns.iter().position(|c| *c == name)
                        })
                        .collect();

                    pr.sort_by(|a, b| {
                        for (i, (_, ascending)) in items.iter().enumerate() {
                            let va = col_indices[i]
                                .and_then(|idx| a.get(idx))
                                .cloned()
                                .unwrap_or(Value::Null);
                            let vb = col_indices[i]
                                .and_then(|idx| b.get(idx))
                                .cloned()
                                .unwrap_or(Value::Null);
                            let ord = compare_values(&va, &vb);
                            let ord = if *ascending { ord } else { ord.reverse() };
                            if ord != std::cmp::Ordering::Equal {
                                return ord;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                } else {
                    rows.sort_by(|a, b| {
                        for (expr, ascending) in items {
                            let va = eval_expr(expr, a, memgraph, params);
                            let vb = eval_expr(expr, b, memgraph, params);
                            let ord = compare_values(&va, &vb);
                            let ord = if *ascending { ord } else { ord.reverse() };
                            if ord != std::cmp::Ordering::Equal {
                                return ord;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
            }

            PhysicalOp::Limit { count } => {
                let n = match eval_expr(count, &HashMap::new(), memgraph, params) {
                    Value::Int(n) if n >= 0 => n as usize,
                    _ => 0,
                };
                if let Some(ref mut pr) = projected_rows {
                    pr.truncate(n);
                } else {
                    rows.truncate(n);
                }
            }

            PhysicalOp::Skip { count } => {
                let n = match eval_expr(count, &HashMap::new(), memgraph, params) {
                    Value::Int(n) if n >= 0 => n as usize,
                    _ => 0,
                };
                if let Some(ref mut pr) = projected_rows {
                    if n < pr.len() {
                        *pr = pr.split_off(n);
                    } else {
                        pr.clear();
                    }
                } else if n < rows.len() {
                    rows = rows.split_off(n);
                } else {
                    rows.clear();
                }
            }

            PhysicalOp::Unwind { expr, alias } => {
                let mut new_rows = Vec::new();
                for row in &rows {
                    let val = eval_expr(expr, row, memgraph, params);
                    if let Value::List(items) = val {
                        for item in items {
                            let mut new_row = row.clone();
                            new_row.insert(alias.clone(), item);
                            new_rows.push(new_row);
                        }
                    }
                }
                rows = new_rows;
            }

            PhysicalOp::CreatePattern { .. } => {
                return Err(ExecError::Unsupported(
                    "write operations require GRAPH.QUERY with write lock".into(),
                ));
            }

            PhysicalOp::DeleteEntities { .. } => {
                return Err(ExecError::Unsupported(
                    "write operations require GRAPH.QUERY with write lock".into(),
                ));
            }

            PhysicalOp::SetProperties { .. } => {
                return Err(ExecError::Unsupported(
                    "write operations require GRAPH.QUERY with write lock".into(),
                ));
            }

            PhysicalOp::ProcedureCall { .. } => {
                return Err(ExecError::Unsupported(
                    "procedure calls not yet implemented in executor".into(),
                ));
            }

            PhysicalOp::Merge { .. } => {
                return Err(ExecError::Unsupported(
                    "write operations require GRAPH.QUERY with write lock".into(),
                ));
            }
        }
    }

    let final_rows = if let Some(pr) = projected_rows {
        pr
    } else {
        // No Project operator: return all row bindings as columns.
        if columns.is_empty() && !rows.is_empty() {
            columns = rows[0].keys().cloned().collect();
            columns.sort();
        }
        rows.iter()
            .map(|row| {
                columns
                    .iter()
                    .map(|c| row.get(c).cloned().unwrap_or(Value::Null))
                    .collect()
            })
            .collect()
    };

    let elapsed = start.elapsed().as_micros() as u64;
    Ok(ExecResult {
        columns,
        rows: final_rows,
        nodes_created,
        nodes_deleted,
        properties_set,
        execution_time_us: elapsed,
    })
}

// ---------------------------------------------------------------------------
// Profiling executor
// ---------------------------------------------------------------------------

/// Get a human-readable name for a physical operator.
fn op_name(op: &PhysicalOp) -> &'static str {
    match op {
        PhysicalOp::NodeScan { .. } => "NodeScan",
        PhysicalOp::Expand { .. } => "Expand",
        PhysicalOp::Filter { .. } => "Filter",
        PhysicalOp::Project { .. } => "Project",
        PhysicalOp::Sort { .. } => "Sort",
        PhysicalOp::Limit { .. } => "Limit",
        PhysicalOp::Skip { .. } => "Skip",
        PhysicalOp::CreatePattern { .. } => "CreatePattern",
        PhysicalOp::DeleteEntities { .. } => "DeleteEntities",
        PhysicalOp::SetProperties { .. } => "SetProperties",
        PhysicalOp::ProcedureCall { .. } => "ProcedureCall",
        PhysicalOp::Unwind { .. } => "Unwind",
        PhysicalOp::Merge { .. } => "Merge",
    }
}

/// Execute a physical plan with per-operator timing instrumentation.
///
/// Structurally identical to [`execute`] but wraps each operator with
/// `Instant::now()` timing. This is a debug command (GRAPH.PROFILE), not a
/// hot path, so per-operator `Instant::now()` is acceptable.
pub fn execute_profile(
    graph: &NamedGraph,
    plan: &PhysicalPlan,
    params: &HashMap<String, Value>,
) -> Result<ProfileResult, ExecError> {
    let start = std::time::Instant::now();

    let mut rows: Vec<Row> = vec![HashMap::new()];
    let mut columns = Vec::new();
    let mut projected_rows: Option<Vec<Vec<Value>>> = None;
    let nodes_created: u64 = 0;
    let nodes_deleted: u64 = 0;
    let properties_set: u64 = 0;
    let mut profiles = Vec::with_capacity(plan.operators.len());

    let memgraph = &graph.write_buf;

    for op in &plan.operators {
        let op_start = std::time::Instant::now();

        match op {
            PhysicalOp::NodeScan { variable, label } => {
                let label_id = label.as_ref().map(|l| label_to_id(l.as_bytes()));
                let mut new_rows = Vec::new();
                for row in &rows {
                    for (key, node) in memgraph.iter_nodes() {
                        if let Some(lid) = label_id {
                            if !node.labels.contains(&lid) {
                                continue;
                            }
                        }
                        let mut new_row = row.clone();
                        new_row.insert(variable.clone(), Value::Node(key));
                        new_rows.push(new_row);
                    }
                }
                rows = new_rows;
            }

            PhysicalOp::Expand {
                source,
                target,
                edge_types,
                direction,
                min_hops,
                max_hops,
            } => {
                let type_ids: Vec<u16> = edge_types
                    .iter()
                    .map(|t| label_to_id(t.as_bytes()))
                    .collect();

                let dir = match direction {
                    EdgeDirection::Right => Direction::Outgoing,
                    EdgeDirection::Left => Direction::Incoming,
                    EdgeDirection::Both => Direction::Both,
                };

                let mut new_rows = Vec::new();
                for row in &rows {
                    let src_key = match row.get(source) {
                        Some(Value::Node(k)) => *k,
                        _ => continue,
                    };

                    if *max_hops <= 1 {
                        for (edge_key, neighbor_key) in
                            memgraph.neighbors(src_key, dir, u64::MAX)
                        {
                            if !type_ids.is_empty() {
                                if let Some(edge) = memgraph.get_edge(edge_key) {
                                    if !type_ids.contains(&edge.edge_type) {
                                        continue;
                                    }
                                }
                            }
                            let mut new_row = row.clone();
                            new_row.insert(target.clone(), Value::Node(neighbor_key));
                            new_rows.push(new_row);
                        }
                    } else {
                        let mut frontier = vec![src_key];
                        let mut visited = std::collections::HashSet::new();
                        visited.insert(src_key);

                        for hop in 1..=*max_hops {
                            let mut next_frontier = Vec::new();
                            for &current in &frontier {
                                for (edge_key, neighbor_key) in
                                    memgraph.neighbors(current, dir, u64::MAX)
                                {
                                    if visited.contains(&neighbor_key) {
                                        continue;
                                    }
                                    if !type_ids.is_empty() {
                                        if let Some(edge) = memgraph.get_edge(edge_key) {
                                            if !type_ids.contains(&edge.edge_type) {
                                                continue;
                                            }
                                        }
                                    }
                                    visited.insert(neighbor_key);
                                    next_frontier.push(neighbor_key);

                                    if hop >= *min_hops {
                                        let mut new_row = row.clone();
                                        new_row.insert(
                                            target.clone(),
                                            Value::Node(neighbor_key),
                                        );
                                        new_rows.push(new_row);
                                    }
                                }
                            }
                            frontier = next_frontier;
                            if frontier.is_empty() {
                                break;
                            }
                        }
                    }
                }
                rows = new_rows;
            }

            PhysicalOp::Filter { expr } => {
                rows.retain(|row| {
                    matches!(eval_expr(expr, row, memgraph, params), Value::Bool(true))
                });
            }

            PhysicalOp::Project { items, distinct } => {
                columns = items
                    .iter()
                    .map(|item| {
                        if let Some(alias) = &item.alias {
                            alias.clone()
                        } else {
                            expr_to_string(&item.expr)
                        }
                    })
                    .collect();

                let mut projected: Vec<Vec<Value>> = rows
                    .iter()
                    .map(|row| {
                        items
                            .iter()
                            .map(|item| {
                                if matches!(item.expr, Expr::Star) {
                                    let entries: Vec<(String, Value)> = row
                                        .iter()
                                        .map(|(k, v)| (k.clone(), v.clone()))
                                        .collect();
                                    Value::Map(entries)
                                } else {
                                    eval_expr(&item.expr, row, memgraph, params)
                                }
                            })
                            .collect()
                    })
                    .collect();

                if *distinct {
                    dedup_rows(&mut projected);
                }

                projected_rows = Some(projected);
                rows.clear();
            }

            PhysicalOp::Sort { items } => {
                if let Some(ref mut pr) = projected_rows {
                    let col_indices: Vec<Option<usize>> = items
                        .iter()
                        .map(|(expr, _)| {
                            let name = expr_to_string(expr);
                            columns.iter().position(|c| *c == name)
                        })
                        .collect();

                    pr.sort_by(|a, b| {
                        for (i, (_, ascending)) in items.iter().enumerate() {
                            let va = col_indices[i]
                                .and_then(|idx| a.get(idx))
                                .cloned()
                                .unwrap_or(Value::Null);
                            let vb = col_indices[i]
                                .and_then(|idx| b.get(idx))
                                .cloned()
                                .unwrap_or(Value::Null);
                            let ord = compare_values(&va, &vb);
                            let ord = if *ascending { ord } else { ord.reverse() };
                            if ord != std::cmp::Ordering::Equal {
                                return ord;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                } else {
                    rows.sort_by(|a, b| {
                        for (expr, ascending) in items {
                            let va = eval_expr(expr, a, memgraph, params);
                            let vb = eval_expr(expr, b, memgraph, params);
                            let ord = compare_values(&va, &vb);
                            let ord = if *ascending { ord } else { ord.reverse() };
                            if ord != std::cmp::Ordering::Equal {
                                return ord;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
            }

            PhysicalOp::Limit { count } => {
                let n = match eval_expr(count, &HashMap::new(), memgraph, params) {
                    Value::Int(n) if n >= 0 => n as usize,
                    _ => 0,
                };
                if let Some(ref mut pr) = projected_rows {
                    pr.truncate(n);
                } else {
                    rows.truncate(n);
                }
            }

            PhysicalOp::Skip { count } => {
                let n = match eval_expr(count, &HashMap::new(), memgraph, params) {
                    Value::Int(n) if n >= 0 => n as usize,
                    _ => 0,
                };
                if let Some(ref mut pr) = projected_rows {
                    if n < pr.len() {
                        *pr = pr.split_off(n);
                    } else {
                        pr.clear();
                    }
                } else if n < rows.len() {
                    rows = rows.split_off(n);
                } else {
                    rows.clear();
                }
            }

            PhysicalOp::Unwind { expr, alias } => {
                let mut new_rows = Vec::new();
                for row in &rows {
                    let val = eval_expr(expr, row, memgraph, params);
                    if let Value::List(items) = val {
                        for item in items {
                            let mut new_row = row.clone();
                            new_row.insert(alias.clone(), item);
                            new_rows.push(new_row);
                        }
                    }
                }
                rows = new_rows;
            }

            PhysicalOp::CreatePattern { .. } => {
                return Err(ExecError::Unsupported(
                    "write operations require GRAPH.QUERY with write lock".into(),
                ));
            }

            PhysicalOp::DeleteEntities { .. } => {
                return Err(ExecError::Unsupported(
                    "write operations require GRAPH.QUERY with write lock".into(),
                ));
            }

            PhysicalOp::SetProperties { .. } => {
                return Err(ExecError::Unsupported(
                    "write operations require GRAPH.QUERY with write lock".into(),
                ));
            }

            PhysicalOp::ProcedureCall { .. } => {
                return Err(ExecError::Unsupported(
                    "procedure calls not yet implemented in executor".into(),
                ));
            }

            PhysicalOp::Merge { .. } => {
                return Err(ExecError::Unsupported(
                    "write operations require GRAPH.QUERY with write lock".into(),
                ));
            }
        }

        let op_elapsed = op_start.elapsed();
        let row_count = if let Some(ref pr) = projected_rows {
            pr.len() as u64
        } else {
            rows.len() as u64
        };
        profiles.push(OpProfile {
            name: op_name(op),
            row_count,
            duration_us: op_elapsed.as_micros() as u64,
        });
    }

    let final_rows = if let Some(pr) = projected_rows {
        pr
    } else {
        if columns.is_empty() && !rows.is_empty() {
            columns = rows[0].keys().cloned().collect();
            columns.sort();
        }
        rows.iter()
            .map(|row| {
                columns
                    .iter()
                    .map(|c| row.get(c).cloned().unwrap_or(Value::Null))
                    .collect()
            })
            .collect()
    };

    let elapsed = start.elapsed().as_micros() as u64;
    Ok(ProfileResult {
        exec_result: ExecResult {
            columns,
            rows: final_rows,
            nodes_created,
            nodes_deleted,
            properties_set,
            execution_time_us: elapsed,
        },
        operator_profiles: profiles,
    })
}

// ---------------------------------------------------------------------------
// Expression evaluator
// ---------------------------------------------------------------------------

/// Evaluate an expression in the context of a row.
fn eval_expr(
    expr: &Expr,
    row: &Row,
    memgraph: &crate::graph::memgraph::MemGraph,
    params: &HashMap<String, Value>,
) -> Value {
    match expr {
        Expr::Integer(n) => Value::Int(*n),
        Expr::Float(f) => Value::Float(*f),
        Expr::StringLit(s) => Value::String(s.clone()),
        Expr::Bool(b) => Value::Bool(*b),
        Expr::Null => Value::Null,

        Expr::Ident(name) => row.get(name).cloned().unwrap_or(Value::Null),

        Expr::Parameter(name) => params.get(name).cloned().unwrap_or(Value::Null),

        Expr::PropertyAccess { object, property } => {
            let obj = eval_expr(object, row, memgraph, params);
            match obj {
                Value::Node(key) => {
                    if let Some(node) = memgraph.get_node(key) {
                        let prop_id = label_to_id(property.as_bytes());
                        for (pid, pval) in &node.properties {
                            if *pid == prop_id {
                                return property_value_to_value(pval);
                            }
                        }
                    }
                    Value::Null
                }
                Value::Edge(key) => {
                    if let Some(edge) = memgraph.get_edge(key) {
                        if let Some(props) = &edge.properties {
                            let prop_id = label_to_id(property.as_bytes());
                            for (pid, pval) in props {
                                if *pid == prop_id {
                                    return property_value_to_value(pval);
                                }
                            }
                        }
                    }
                    Value::Null
                }
                Value::Map(entries) => {
                    for (k, v) in &entries {
                        if k == property {
                            return v.clone();
                        }
                    }
                    Value::Null
                }
                _ => Value::Null,
            }
        }

        Expr::BinaryOp { left, op, right } => {
            let lv = eval_expr(left, row, memgraph, params);
            let rv = eval_expr(right, row, memgraph, params);
            eval_binary_op(&lv, *op, &rv)
        }

        Expr::Not(inner) => {
            let v = eval_expr(inner, row, memgraph, params);
            match v {
                Value::Bool(b) => Value::Bool(!b),
                Value::Null => Value::Null,
                _ => Value::Null,
            }
        }

        Expr::Negate(inner) => {
            let v = eval_expr(inner, row, memgraph, params);
            match v {
                Value::Int(n) => Value::Int(-n),
                Value::Float(f) => Value::Float(-f),
                _ => Value::Null,
            }
        }

        Expr::IsNull { expr, negated } => {
            let v = eval_expr(expr, row, memgraph, params);
            let is_null = matches!(v, Value::Null);
            Value::Bool(if *negated { !is_null } else { is_null })
        }

        Expr::InList { expr, list } => {
            let val = eval_expr(expr, row, memgraph, params);
            let list_val = eval_expr(list, row, memgraph, params);
            match list_val {
                Value::List(items) => {
                    let found = items.iter().any(|item| {
                        matches!(compare_values(&val, item), std::cmp::Ordering::Equal)
                    });
                    Value::Bool(found)
                }
                _ => Value::Null,
            }
        }

        Expr::FunctionCall { name, args, .. } => {
            let lower_name = name.to_ascii_lowercase();
            match lower_name.as_str() {
                "id" => {
                    if let Some(arg) = args.first() {
                        let v = eval_expr(arg, row, memgraph, params);
                        match v {
                            Value::Node(k) => {
                                Value::Int(k.data().as_ffi() as i64)
                            }
                            Value::Edge(k) => {
                                Value::Int(k.data().as_ffi() as i64)
                            }
                            _ => Value::Null,
                        }
                    } else {
                        Value::Null
                    }
                }
                "labels" => {
                    if let Some(arg) = args.first() {
                        let v = eval_expr(arg, row, memgraph, params);
                        if let Value::Node(k) = v {
                            if let Some(node) = memgraph.get_node(k) {
                                let labels: Vec<Value> = node
                                    .labels
                                    .iter()
                                    .map(|&l| Value::Int(l as i64))
                                    .collect();
                                return Value::List(labels);
                            }
                        }
                        Value::Null
                    } else {
                        Value::Null
                    }
                }
                "type" => {
                    if let Some(arg) = args.first() {
                        let v = eval_expr(arg, row, memgraph, params);
                        if let Value::Edge(k) = v {
                            if let Some(edge) = memgraph.get_edge(k) {
                                return Value::Int(edge.edge_type as i64);
                            }
                        }
                        Value::Null
                    } else {
                        Value::Null
                    }
                }
                "size" => {
                    if let Some(arg) = args.first() {
                        let v = eval_expr(arg, row, memgraph, params);
                        match v {
                            Value::List(items) => Value::Int(items.len() as i64),
                            Value::String(s) => Value::Int(s.len() as i64),
                            _ => Value::Null,
                        }
                    } else {
                        Value::Null
                    }
                }
                "tointeger" | "toint" => {
                    if let Some(arg) = args.first() {
                        let v = eval_expr(arg, row, memgraph, params);
                        match v {
                            Value::Int(n) => Value::Int(n),
                            Value::Float(f) => Value::Int(f as i64),
                            Value::String(s) => {
                                s.parse::<i64>().map_or(Value::Null, Value::Int)
                            }
                            _ => Value::Null,
                        }
                    } else {
                        Value::Null
                    }
                }
                "tofloat" => {
                    if let Some(arg) = args.first() {
                        let v = eval_expr(arg, row, memgraph, params);
                        match v {
                            Value::Float(f) => Value::Float(f),
                            Value::Int(n) => Value::Float(n as f64),
                            Value::String(s) => {
                                s.parse::<f64>().map_or(Value::Null, Value::Float)
                            }
                            _ => Value::Null,
                        }
                    } else {
                        Value::Null
                    }
                }
                "tostring" => {
                    if let Some(arg) = args.first() {
                        let v = eval_expr(arg, row, memgraph, params);
                        Value::String(value_to_string(&v))
                    } else {
                        Value::Null
                    }
                }
                "count" | "collect" => {
                    // Aggregate functions are a no-op per-row; handled in
                    // the Project phase as a future enhancement. For now,
                    // return the value or null for count.
                    if let Some(arg) = args.first() {
                        eval_expr(arg, row, memgraph, params)
                    } else {
                        Value::Null
                    }
                }
                _ => Value::Null,
            }
        }

        Expr::List(items) => {
            let values: Vec<Value> = items
                .iter()
                .map(|item| eval_expr(item, row, memgraph, params))
                .collect();
            Value::List(values)
        }

        Expr::MapLit(entries) => {
            let map: Vec<(String, Value)> = entries
                .iter()
                .map(|(k, v)| (k.clone(), eval_expr(v, row, memgraph, params)))
                .collect();
            Value::Map(map)
        }

        Expr::Star => {
            let entries: Vec<(String, Value)> =
                row.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            Value::Map(entries)
        }
    }
}

// ---------------------------------------------------------------------------
// Binary operator evaluation
// ---------------------------------------------------------------------------

fn eval_binary_op(left: &Value, op: BinaryOperator, right: &Value) -> Value {
    match op {
        // Logical operators
        BinaryOperator::And => match (left, right) {
            (Value::Bool(a), Value::Bool(b)) => Value::Bool(*a && *b),
            _ => Value::Null,
        },
        BinaryOperator::Or => match (left, right) {
            (Value::Bool(a), Value::Bool(b)) => Value::Bool(*a || *b),
            _ => Value::Null,
        },

        // Comparison operators
        BinaryOperator::Equal => Value::Bool(compare_values(left, right) == std::cmp::Ordering::Equal),
        BinaryOperator::NotEqual => Value::Bool(compare_values(left, right) != std::cmp::Ordering::Equal),
        BinaryOperator::LessThan => Value::Bool(compare_values(left, right) == std::cmp::Ordering::Less),
        BinaryOperator::GreaterThan => Value::Bool(compare_values(left, right) == std::cmp::Ordering::Greater),
        BinaryOperator::LessEqual => {
            let ord = compare_values(left, right);
            Value::Bool(ord == std::cmp::Ordering::Less || ord == std::cmp::Ordering::Equal)
        }
        BinaryOperator::GreaterEqual => {
            let ord = compare_values(left, right);
            Value::Bool(ord == std::cmp::Ordering::Greater || ord == std::cmp::Ordering::Equal)
        }

        // Arithmetic operators
        BinaryOperator::Add => match (left, right) {
            (Value::Int(a), Value::Int(b)) => Value::Int(a.wrapping_add(*b)),
            (Value::Float(a), Value::Float(b)) => Value::Float(a + b),
            (Value::Int(a), Value::Float(b)) => Value::Float(*a as f64 + b),
            (Value::Float(a), Value::Int(b)) => Value::Float(a + *b as f64),
            (Value::String(a), Value::String(b)) => {
                let mut s = a.clone();
                s.push_str(b);
                Value::String(s)
            }
            _ => Value::Null,
        },
        BinaryOperator::Sub => match (left, right) {
            (Value::Int(a), Value::Int(b)) => Value::Int(a.wrapping_sub(*b)),
            (Value::Float(a), Value::Float(b)) => Value::Float(a - b),
            (Value::Int(a), Value::Float(b)) => Value::Float(*a as f64 - b),
            (Value::Float(a), Value::Int(b)) => Value::Float(a - *b as f64),
            _ => Value::Null,
        },
        BinaryOperator::Mul => match (left, right) {
            (Value::Int(a), Value::Int(b)) => Value::Int(a.wrapping_mul(*b)),
            (Value::Float(a), Value::Float(b)) => Value::Float(a * b),
            (Value::Int(a), Value::Float(b)) => Value::Float(*a as f64 * b),
            (Value::Float(a), Value::Int(b)) => Value::Float(a * *b as f64),
            _ => Value::Null,
        },
        BinaryOperator::Div => match (left, right) {
            (Value::Int(a), Value::Int(b)) if *b != 0 => Value::Int(a / b),
            (Value::Float(a), Value::Float(b)) if *b != 0.0 => Value::Float(a / b),
            (Value::Int(a), Value::Float(b)) if *b != 0.0 => Value::Float(*a as f64 / b),
            (Value::Float(a), Value::Int(b)) if *b != 0 => Value::Float(a / *b as f64),
            _ => Value::Null,
        },
        BinaryOperator::Mod => match (left, right) {
            (Value::Int(a), Value::Int(b)) if *b != 0 => Value::Int(a % b),
            (Value::Float(a), Value::Float(b)) if *b != 0.0 => Value::Float(a % b),
            (Value::Int(a), Value::Float(b)) if *b != 0.0 => Value::Float(*a as f64 % b),
            (Value::Float(a), Value::Int(b)) if *b != 0 => Value::Float(a % *b as f64),
            _ => Value::Null,
        },

        BinaryOperator::RegexMatch => {
            // Basic regex matching -- just string contains for now.
            match (left, right) {
                (Value::String(text), Value::String(pattern)) => {
                    Value::Bool(text.contains(pattern.as_str()))
                }
                _ => Value::Null,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Value comparison (for Sort and equality)
// ---------------------------------------------------------------------------

/// Compare two Values for ordering.
/// NULL < Bool < Int/Float < String < Node < Edge < List < Map.
fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    fn type_rank(v: &Value) -> u8 {
        match v {
            Value::Null => 0,
            Value::Bool(_) => 1,
            Value::Int(_) | Value::Float(_) => 2,
            Value::String(_) => 3,
            Value::Node(_) => 4,
            Value::Edge(_) => 5,
            Value::List(_) => 6,
            Value::Map(_) => 7,
        }
    }

    let ra = type_rank(a);
    let rb = type_rank(b);
    if ra != rb {
        return ra.cmp(&rb);
    }

    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        // Numeric comparison with i64/f64 promotion.
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Value::Int(a), Value::Float(b)) => {
            (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (Value::Float(a), Value::Int(b)) => {
            a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
        }
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Node(a), Value::Node(b)) => {
            a.data().as_ffi().cmp(&b.data().as_ffi())
        }
        (Value::Edge(a), Value::Edge(b)) => {
            a.data().as_ffi().cmp(&b.data().as_ffi())
        }
        _ => Ordering::Equal,
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert a PropertyValue to a runtime Value.
fn property_value_to_value(pv: &PropertyValue) -> Value {
    match pv {
        PropertyValue::Int(n) => Value::Int(*n),
        PropertyValue::Float(f) => Value::Float(*f),
        PropertyValue::String(s) => {
            Value::String(core::str::from_utf8(s).unwrap_or("").to_owned())
        }
        PropertyValue::Bool(b) => Value::Bool(*b),
        PropertyValue::Bytes(b) => {
            Value::String(core::str::from_utf8(b).unwrap_or("").to_owned())
        }
    }
}

/// Convert a Value to a display string.
fn value_to_string(v: &Value) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Int(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Node(k) => format!("node:{}", k.data().as_ffi()),
        Value::Edge(k) => format!("edge:{}", k.data().as_ffi()),
        Value::List(items) => {
            let parts: Vec<String> = items.iter().map(value_to_string).collect();
            format!("[{}]", parts.join(", "))
        }
        Value::Map(entries) => {
            let parts: Vec<String> = entries
                .iter()
                .map(|(k, v)| format!("{}: {}", k, value_to_string(v)))
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
    }
}

/// Convert an Expr to a display string (for column headers).
fn expr_to_string(expr: &Expr) -> String {
    match expr {
        Expr::Ident(name) => name.clone(),
        Expr::PropertyAccess { object, property } => {
            format!("{}.{}", expr_to_string(object), property)
        }
        Expr::FunctionCall { name, args, .. } => {
            let arg_strs: Vec<String> = args.iter().map(expr_to_string).collect();
            format!("{}({})", name, arg_strs.join(", "))
        }
        Expr::Star => "*".into(),
        Expr::Integer(n) => n.to_string(),
        Expr::Float(f) => f.to_string(),
        Expr::StringLit(s) => format!("'{s}'"),
        Expr::Bool(b) => b.to_string(),
        Expr::Null => "NULL".into(),
        _ => format!("{expr:?}"),
    }
}

/// Deduplicate projected rows (for DISTINCT).
fn dedup_rows(rows: &mut Vec<Vec<Value>>) {
    // Use string representation for dedup (simple approach).
    let mut seen = std::collections::HashSet::new();
    rows.retain(|row| {
        let key: Vec<String> = row.iter().map(value_to_string).collect();
        let key_str = key.join("|");
        seen.insert(key_str)
    });
}

// ---------------------------------------------------------------------------
// Use slotmap::Key for as_ffi()
// ---------------------------------------------------------------------------
use slotmap::Key;

// ---------------------------------------------------------------------------
// Value ↔ PropertyValue conversion
// ---------------------------------------------------------------------------

/// Convert a runtime Value to a PropertyValue for storage.
fn value_to_property_value(v: &Value) -> Option<PropertyValue> {
    match v {
        Value::Int(n) => Some(PropertyValue::Int(*n)),
        Value::Float(f) => Some(PropertyValue::Float(*f)),
        Value::String(s) => Some(PropertyValue::String(Bytes::from(s.clone()))),
        Value::Bool(b) => Some(PropertyValue::Bool(*b)),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Write-capable executor (execute_mut)
// ---------------------------------------------------------------------------

/// Execute a physical plan with write access to the named graph.
///
/// Identical pipeline to [`execute`] but handles write operators:
/// `CreatePattern`, `SetProperties`, `DeleteEntities`, and `Merge`.
pub fn execute_mut(
    graph: &mut NamedGraph,
    plan: &PhysicalPlan,
    params: &HashMap<String, Value>,
) -> Result<ExecResult, ExecError> {
    let start = std::time::Instant::now();

    let mut rows: Vec<Row> = vec![HashMap::new()];
    let mut columns = Vec::new();
    let mut projected_rows: Option<Vec<Vec<Value>>> = None;
    let mut nodes_created: u64 = 0;
    let mut nodes_deleted: u64 = 0;
    let mut properties_set: u64 = 0;

    for op in &plan.operators {
        match op {
            PhysicalOp::NodeScan { variable, label } => {
                let label_id = label.as_ref().map(|l| label_to_id(l.as_bytes()));
                let mut new_rows = Vec::new();
                for row in &rows {
                    for (key, node) in graph.write_buf.iter_nodes() {
                        if let Some(lid) = label_id {
                            if !node.labels.contains(&lid) {
                                continue;
                            }
                        }
                        let mut new_row = row.clone();
                        new_row.insert(variable.clone(), Value::Node(key));
                        new_rows.push(new_row);
                    }
                }
                rows = new_rows;
            }

            PhysicalOp::Expand {
                source,
                target,
                edge_types,
                direction,
                min_hops,
                max_hops,
            } => {
                let type_ids: Vec<u16> = edge_types
                    .iter()
                    .map(|t| label_to_id(t.as_bytes()))
                    .collect();

                let dir = match direction {
                    EdgeDirection::Right => Direction::Outgoing,
                    EdgeDirection::Left => Direction::Incoming,
                    EdgeDirection::Both => Direction::Both,
                };

                let mut new_rows = Vec::new();
                for row in &rows {
                    let src_key = match row.get(source) {
                        Some(Value::Node(k)) => *k,
                        _ => continue,
                    };

                    if *max_hops <= 1 {
                        for (edge_key, neighbor_key) in
                            graph.write_buf.neighbors(src_key, dir, u64::MAX)
                        {
                            if !type_ids.is_empty() {
                                if let Some(edge) = graph.write_buf.get_edge(edge_key) {
                                    if !type_ids.contains(&edge.edge_type) {
                                        continue;
                                    }
                                }
                            }
                            let mut new_row = row.clone();
                            new_row.insert(target.clone(), Value::Node(neighbor_key));
                            new_rows.push(new_row);
                        }
                    } else {
                        let mut frontier = vec![src_key];
                        let mut visited = std::collections::HashSet::new();
                        visited.insert(src_key);

                        for hop in 1..=*max_hops {
                            let mut next_frontier = Vec::new();
                            for &current in &frontier {
                                for (edge_key, neighbor_key) in
                                    graph.write_buf.neighbors(current, dir, u64::MAX)
                                {
                                    if visited.contains(&neighbor_key) {
                                        continue;
                                    }
                                    if !type_ids.is_empty() {
                                        if let Some(edge) = graph.write_buf.get_edge(edge_key) {
                                            if !type_ids.contains(&edge.edge_type) {
                                                continue;
                                            }
                                        }
                                    }
                                    visited.insert(neighbor_key);
                                    next_frontier.push(neighbor_key);

                                    if hop >= *min_hops {
                                        let mut new_row = row.clone();
                                        new_row.insert(
                                            target.clone(),
                                            Value::Node(neighbor_key),
                                        );
                                        new_rows.push(new_row);
                                    }
                                }
                            }
                            frontier = next_frontier;
                            if frontier.is_empty() {
                                break;
                            }
                        }
                    }
                }
                rows = new_rows;
            }

            PhysicalOp::Filter { expr } => {
                rows.retain(|row| {
                    matches!(
                        eval_expr(expr, row, &graph.write_buf, params),
                        Value::Bool(true)
                    )
                });
            }

            PhysicalOp::Project { items, distinct } => {
                columns = items
                    .iter()
                    .map(|item| {
                        if let Some(alias) = &item.alias {
                            alias.clone()
                        } else {
                            expr_to_string(&item.expr)
                        }
                    })
                    .collect();

                let mut projected: Vec<Vec<Value>> = rows
                    .iter()
                    .map(|row| {
                        items
                            .iter()
                            .map(|item| {
                                if matches!(item.expr, Expr::Star) {
                                    let entries: Vec<(String, Value)> = row
                                        .iter()
                                        .map(|(k, v)| (k.clone(), v.clone()))
                                        .collect();
                                    Value::Map(entries)
                                } else {
                                    eval_expr(&item.expr, row, &graph.write_buf, params)
                                }
                            })
                            .collect()
                    })
                    .collect();

                if *distinct {
                    dedup_rows(&mut projected);
                }

                projected_rows = Some(projected);
                rows.clear();
            }

            PhysicalOp::Sort { items } => {
                if let Some(ref mut pr) = projected_rows {
                    let col_indices: Vec<Option<usize>> = items
                        .iter()
                        .map(|(expr, _)| {
                            let name = expr_to_string(expr);
                            columns.iter().position(|c| *c == name)
                        })
                        .collect();

                    pr.sort_by(|a, b| {
                        for (i, (_, ascending)) in items.iter().enumerate() {
                            let va = col_indices[i]
                                .and_then(|idx| a.get(idx))
                                .cloned()
                                .unwrap_or(Value::Null);
                            let vb = col_indices[i]
                                .and_then(|idx| b.get(idx))
                                .cloned()
                                .unwrap_or(Value::Null);
                            let ord = compare_values(&va, &vb);
                            let ord = if *ascending { ord } else { ord.reverse() };
                            if ord != std::cmp::Ordering::Equal {
                                return ord;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                } else {
                    rows.sort_by(|a, b| {
                        for (expr, ascending) in items {
                            let va = eval_expr(expr, a, &graph.write_buf, params);
                            let vb = eval_expr(expr, b, &graph.write_buf, params);
                            let ord = compare_values(&va, &vb);
                            let ord = if *ascending { ord } else { ord.reverse() };
                            if ord != std::cmp::Ordering::Equal {
                                return ord;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
            }

            PhysicalOp::Limit { count } => {
                let n = match eval_expr(count, &HashMap::new(), &graph.write_buf, params) {
                    Value::Int(n) if n >= 0 => n as usize,
                    _ => 0,
                };
                if let Some(ref mut pr) = projected_rows {
                    pr.truncate(n);
                } else {
                    rows.truncate(n);
                }
            }

            PhysicalOp::Skip { count } => {
                let n = match eval_expr(count, &HashMap::new(), &graph.write_buf, params) {
                    Value::Int(n) if n >= 0 => n as usize,
                    _ => 0,
                };
                if let Some(ref mut pr) = projected_rows {
                    if n < pr.len() {
                        *pr = pr.split_off(n);
                    } else {
                        pr.clear();
                    }
                } else if n < rows.len() {
                    rows = rows.split_off(n);
                } else {
                    rows.clear();
                }
            }

            PhysicalOp::Unwind { expr, alias } => {
                let mut new_rows = Vec::new();
                for row in &rows {
                    let val = eval_expr(expr, row, &graph.write_buf, params);
                    if let Value::List(items) = val {
                        for item in items {
                            let mut new_row = row.clone();
                            new_row.insert(alias.clone(), item);
                            new_rows.push(new_row);
                        }
                    }
                }
                rows = new_rows;
            }

            PhysicalOp::CreatePattern { patterns } => {
                let lsn = 0u64; // Placeholder LSN; WAL layer assigns real LSNs.
                let mut new_rows = Vec::with_capacity(rows.len());
                for row in &rows {
                    let mut new_row = row.clone();
                    for pattern in patterns {
                        // Create nodes.
                        let mut node_keys = Vec::with_capacity(pattern.nodes.len());
                        for pn in &pattern.nodes {
                            let labels: SmallVec<[u16; 4]> = pn
                                .labels
                                .iter()
                                .map(|l| label_to_id(l.as_bytes()))
                                .collect();
                            let props: PropertyMap = pn
                                .properties
                                .iter()
                                .filter_map(|(name, expr)| {
                                    let val = eval_expr(expr, &new_row, &graph.write_buf, params);
                                    value_to_property_value(&val)
                                        .map(|pv| (label_to_id(name.as_bytes()), pv))
                                })
                                .collect();
                            let nk = graph.write_buf.add_node(labels, props, None, lsn);
                            nodes_created += 1;
                            if let Some(ref var) = pn.variable {
                                new_row.insert(var.clone(), Value::Node(nk));
                            }
                            node_keys.push(nk);
                        }
                        // Create edges.
                        for (i, pe) in pattern.edges.iter().enumerate() {
                            let src = node_keys[i];
                            let dst = node_keys[i + 1];
                            let edge_type = pe
                                .edge_types
                                .first()
                                .map(|t| label_to_id(t.as_bytes()))
                                .unwrap_or(0);
                            let _ = graph.write_buf.add_edge(
                                src, dst, edge_type, 1.0, None, lsn,
                            );
                        }
                    }
                    new_rows.push(new_row);
                }
                rows = new_rows;
            }

            PhysicalOp::SetProperties { items } => {
                for row in &rows {
                    for item in items {
                        match item {
                            SetItem::Property {
                                variable,
                                property,
                                value,
                            } => {
                                if let Some(Value::Node(nk)) = row.get(variable) {
                                    let val =
                                        eval_expr(value, row, &graph.write_buf, params);
                                    if let Some(pv) = value_to_property_value(&val) {
                                        let pid = label_to_id(property.as_bytes());
                                        if let Some(node) =
                                            graph.write_buf.get_node_mut(*nk)
                                        {
                                            // Update existing or append.
                                            let mut found = false;
                                            for entry in node.properties.iter_mut() {
                                                if entry.0 == pid {
                                                    entry.1 = pv.clone();
                                                    found = true;
                                                    break;
                                                }
                                            }
                                            if !found {
                                                node.properties.push((pid, pv));
                                            }
                                            properties_set += 1;
                                        }
                                    }
                                }
                            }
                            SetItem::Label { variable, label } => {
                                if let Some(Value::Node(nk)) = row.get(variable) {
                                    let lid = label_to_id(label.as_bytes());
                                    if let Some(node) = graph.write_buf.get_node_mut(*nk) {
                                        if !node.labels.contains(&lid) {
                                            node.labels.push(lid);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            PhysicalOp::DeleteEntities { exprs, detach } => {
                let _ = detach; // Detach is always implied for MemGraph soft-delete.
                for row in &rows {
                    for expr in exprs {
                        let val = eval_expr(expr, row, &graph.write_buf, params);
                        match val {
                            Value::Node(nk) => {
                                graph.write_buf.remove_node(nk, 0);
                                nodes_deleted += 1;
                            }
                            Value::Edge(ek) => {
                                graph.write_buf.remove_edge(ek, 0);
                            }
                            _ => {}
                        }
                    }
                }
            }

            PhysicalOp::Merge {
                pattern,
                on_create,
                on_match,
            } => {
                let lsn = 0u64;
                let mut new_rows = Vec::with_capacity(rows.len());

                for row in &rows {
                    let mut new_row = row.clone();

                    if pattern.edges.is_empty() && !pattern.nodes.is_empty() {
                        // ---- Node-only MERGE ----
                        let pn = &pattern.nodes[0];
                        let label_ids: SmallVec<[u16; 4]> = pn
                            .labels
                            .iter()
                            .map(|l| label_to_id(l.as_bytes()))
                            .collect();

                        // Evaluate property expressions for matching.
                        let match_props: Vec<(u16, PropertyValue)> = pn
                            .properties
                            .iter()
                            .filter_map(|(name, expr)| {
                                let val = eval_expr(expr, &new_row, &graph.write_buf, params);
                                value_to_property_value(&val)
                                    .map(|pv| (label_to_id(name.as_bytes()), pv))
                            })
                            .collect();

                        // Search for existing node matching labels + properties.
                        let found = graph
                            .write_buf
                            .iter_nodes()
                            .find(|(_, node)| {
                                // All required labels must be present.
                                for &lid in &label_ids {
                                    if !node.labels.contains(&lid) {
                                        return false;
                                    }
                                }
                                // All required properties must match.
                                for (pid, pval) in &match_props {
                                    let has_match = node.properties.iter().any(|(np, nv)| {
                                        *np == *pid && *nv == *pval
                                    });
                                    if !has_match {
                                        return false;
                                    }
                                }
                                true
                            })
                            .map(|(k, _)| k);

                        if let Some(existing_key) = found {
                            // MATCH path: bind variable and apply on_match.
                            if let Some(ref var) = pn.variable {
                                new_row.insert(var.clone(), Value::Node(existing_key));
                            }
                            apply_set_items(
                                on_match,
                                &new_row,
                                &mut graph.write_buf,
                                params,
                                &mut properties_set,
                            );
                        } else {
                            // CREATE path: create node.
                            let props: PropertyMap = match_props
                                .into_iter()
                                .collect();
                            let nk = graph.write_buf.add_node(
                                label_ids, props, None, lsn,
                            );
                            nodes_created += 1;
                            if let Some(ref var) = pn.variable {
                                new_row.insert(var.clone(), Value::Node(nk));
                            }
                            apply_set_items(
                                on_create,
                                &new_row,
                                &mut graph.write_buf,
                                params,
                                &mut properties_set,
                            );
                        }
                    } else if !pattern.edges.is_empty() && pattern.nodes.len() >= 2 {
                        // ---- Edge MERGE ----
                        let src_pn = &pattern.nodes[0];
                        let dst_pn = &pattern.nodes[1];
                        let pe = &pattern.edges[0];

                        // Resolve or find source node.
                        let src_key = resolve_or_find_node(
                            src_pn, &new_row, &graph.write_buf, params,
                        );
                        let dst_key = resolve_or_find_node(
                            dst_pn, &new_row, &graph.write_buf, params,
                        );

                        let edge_type_id = pe
                            .edge_types
                            .first()
                            .map(|t| label_to_id(t.as_bytes()))
                            .unwrap_or(0);

                        match (src_key, dst_key) {
                            (Some(sk), Some(dk)) => {
                                // Check if edge exists.
                                let edge_exists = graph
                                    .write_buf
                                    .neighbors(sk, Direction::Outgoing, u64::MAX)
                                    .any(|(ek, nk)| {
                                        nk == dk
                                            && graph
                                                .write_buf
                                                .get_edge(ek)
                                                .map_or(false, |e| e.edge_type == edge_type_id)
                                    });

                                if edge_exists {
                                    // Bind variables.
                                    if let Some(ref var) = src_pn.variable {
                                        new_row.insert(var.clone(), Value::Node(sk));
                                    }
                                    if let Some(ref var) = dst_pn.variable {
                                        new_row.insert(var.clone(), Value::Node(dk));
                                    }
                                    apply_set_items(
                                        on_match,
                                        &new_row,
                                        &mut graph.write_buf,
                                        params,
                                        &mut properties_set,
                                    );
                                } else {
                                    // Create edge.
                                    let _ = graph.write_buf.add_edge(
                                        sk, dk, edge_type_id, 1.0, None, lsn,
                                    );
                                    if let Some(ref var) = src_pn.variable {
                                        new_row.insert(var.clone(), Value::Node(sk));
                                    }
                                    if let Some(ref var) = dst_pn.variable {
                                        new_row.insert(var.clone(), Value::Node(dk));
                                    }
                                    apply_set_items(
                                        on_create,
                                        &new_row,
                                        &mut graph.write_buf,
                                        params,
                                        &mut properties_set,
                                    );
                                }
                            }
                            _ => {
                                // Create missing nodes and edge.
                                let sk = src_key.unwrap_or_else(|| {
                                    let labels: SmallVec<[u16; 4]> = src_pn
                                        .labels
                                        .iter()
                                        .map(|l| label_to_id(l.as_bytes()))
                                        .collect();
                                    let props: PropertyMap = src_pn
                                        .properties
                                        .iter()
                                        .filter_map(|(name, expr)| {
                                            let val = eval_expr(
                                                expr, &new_row, &graph.write_buf, params,
                                            );
                                            value_to_property_value(&val)
                                                .map(|pv| (label_to_id(name.as_bytes()), pv))
                                        })
                                        .collect();
                                    let nk = graph.write_buf.add_node(
                                        labels, props, None, lsn,
                                    );
                                    nodes_created += 1;
                                    nk
                                });
                                let dk = dst_key.unwrap_or_else(|| {
                                    let labels: SmallVec<[u16; 4]> = dst_pn
                                        .labels
                                        .iter()
                                        .map(|l| label_to_id(l.as_bytes()))
                                        .collect();
                                    let props: PropertyMap = dst_pn
                                        .properties
                                        .iter()
                                        .filter_map(|(name, expr)| {
                                            let val = eval_expr(
                                                expr, &new_row, &graph.write_buf, params,
                                            );
                                            value_to_property_value(&val)
                                                .map(|pv| (label_to_id(name.as_bytes()), pv))
                                        })
                                        .collect();
                                    let nk = graph.write_buf.add_node(
                                        labels, props, None, lsn,
                                    );
                                    nodes_created += 1;
                                    nk
                                });
                                let _ = graph.write_buf.add_edge(
                                    sk, dk, edge_type_id, 1.0, None, lsn,
                                );
                                if let Some(ref var) = src_pn.variable {
                                    new_row.insert(var.clone(), Value::Node(sk));
                                }
                                if let Some(ref var) = dst_pn.variable {
                                    new_row.insert(var.clone(), Value::Node(dk));
                                }
                                apply_set_items(
                                    on_create,
                                    &new_row,
                                    &mut graph.write_buf,
                                    params,
                                    &mut properties_set,
                                );
                            }
                        }
                    }

                    new_rows.push(new_row);
                }
                rows = new_rows;
            }

            PhysicalOp::ProcedureCall { .. } => {
                return Err(ExecError::Unsupported(
                    "procedure calls not yet implemented in executor".into(),
                ));
            }
        }
    }

    let final_rows = if let Some(pr) = projected_rows {
        pr
    } else {
        if columns.is_empty() && !rows.is_empty() {
            columns = rows[0].keys().cloned().collect();
            columns.sort();
        }
        rows.iter()
            .map(|row| {
                columns
                    .iter()
                    .map(|c| row.get(c).cloned().unwrap_or(Value::Null))
                    .collect()
            })
            .collect()
    };

    let elapsed = start.elapsed().as_micros() as u64;
    Ok(ExecResult {
        columns,
        rows: final_rows,
        nodes_created,
        nodes_deleted,
        properties_set,
        execution_time_us: elapsed,
    })
}

/// Apply SET items (ON CREATE SET / ON MATCH SET) to a node in the row.
fn apply_set_items(
    items: &[SetItem],
    row: &Row,
    memgraph: &mut crate::graph::memgraph::MemGraph,
    params: &HashMap<String, Value>,
    properties_set: &mut u64,
) {
    for item in items {
        match item {
            SetItem::Property {
                variable,
                property,
                value,
            } => {
                if let Some(Value::Node(nk)) = row.get(variable) {
                    let val = eval_expr(value, row, memgraph, params);
                    if let Some(pv) = value_to_property_value(&val) {
                        let pid = label_to_id(property.as_bytes());
                        if let Some(node) = memgraph.get_node_mut(*nk) {
                            let mut found = false;
                            for entry in node.properties.iter_mut() {
                                if entry.0 == pid {
                                    entry.1 = pv.clone();
                                    found = true;
                                    break;
                                }
                            }
                            if !found {
                                node.properties.push((pid, pv));
                            }
                            *properties_set += 1;
                        }
                    }
                }
            }
            SetItem::Label { variable, label } => {
                if let Some(Value::Node(nk)) = row.get(variable) {
                    let lid = label_to_id(label.as_bytes());
                    if let Some(node) = memgraph.get_node_mut(*nk) {
                        if !node.labels.contains(&lid) {
                            node.labels.push(lid);
                        }
                    }
                }
            }
        }
    }
}

/// Resolve a pattern node: if it's a bound variable in the row, return that key.
/// Otherwise, search the memgraph for a matching node by labels + properties.
fn resolve_or_find_node(
    pn: &PatternNode,
    row: &Row,
    memgraph: &crate::graph::memgraph::MemGraph,
    params: &HashMap<String, Value>,
) -> Option<NodeKey> {
    // Check if already bound.
    if let Some(ref var) = pn.variable {
        if let Some(Value::Node(k)) = row.get(var) {
            return Some(*k);
        }
    }

    let label_ids: SmallVec<[u16; 4]> = pn
        .labels
        .iter()
        .map(|l| label_to_id(l.as_bytes()))
        .collect();

    let match_props: Vec<(u16, PropertyValue)> = pn
        .properties
        .iter()
        .filter_map(|(name, expr)| {
            let val = eval_expr(expr, row, memgraph, params);
            value_to_property_value(&val).map(|pv| (label_to_id(name.as_bytes()), pv))
        })
        .collect();

    memgraph
        .iter_nodes()
        .find(|(_, node)| {
            for &lid in &label_ids {
                if !node.labels.contains(&lid) {
                    return false;
                }
            }
            for (pid, pval) in &match_props {
                let has_match = node
                    .properties
                    .iter()
                    .any(|(np, nv)| *np == *pid && *nv == *pval);
                if !has_match {
                    return false;
                }
            }
            true
        })
        .map(|(k, _)| k)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::graph::store::GraphStore;
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
        let plan =
            crate::graph::cypher::planner::compile(&query).expect("compile");
        let graph = store.get_graph(b"test").expect("graph");
        let result = execute(graph, &plan, &HashMap::new()).expect("exec");

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

        let query = crate::graph::cypher::parse_cypher(
            b"MATCH (n:Person) WHERE n.age > 30 RETURN n.age",
        )
        .expect("parse");
        let plan = crate::graph::cypher::planner::compile(&query).expect("compile");
        let graph = store.get_graph(b"test").expect("graph");
        let result = execute(graph, &plan, &HashMap::new()).expect("exec");

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

        let query = crate::graph::cypher::parse_cypher(
            b"MATCH (n:Person) RETURN n LIMIT 3",
        )
        .expect("parse");
        let plan = crate::graph::cypher::planner::compile(&query).expect("compile");
        let graph = store.get_graph(b"test").expect("graph");
        let result = execute(graph, &plan, &HashMap::new()).expect("exec");

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
        assert_eq!(
            compare_values(&Value::Null, &Value::Int(0)),
            Ordering::Less
        );
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
        let result = execute_mut(graph, &plan, &HashMap::new()).expect("exec");

        assert_eq!(result.nodes_created, 1);
        assert_eq!(result.rows.len(), 1);
        // Verify the node has the right properties.
        let graph_ref = store.get_graph(b"test").expect("graph");
        let nodes: Vec<_> = graph_ref.write_buf.iter_nodes().collect();
        assert_eq!(nodes.len(), 1);
        let node = nodes[0].1;
        let name_id = label_to_id(b"name");
        let age_id = label_to_id(b"age");
        assert!(node.properties.iter().any(|(p, v)| *p == name_id
            && *v == PropertyValue::String(Bytes::from_static(b"Alice"))));
        assert!(node
            .properties
            .iter()
            .any(|(p, v)| *p == age_id && *v == PropertyValue::Int(30)));
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
        let result = execute_mut(graph, &plan, &HashMap::new()).expect("exec");

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
        let result = execute_mut(graph, &plan, &HashMap::new()).expect("exec");

        assert_eq!(result.nodes_created, 2, "should create two nodes");
        assert_eq!(result.rows.len(), 1);

        // Verify the graph has 2 nodes and 1 edge.
        let graph_ref = store.get_graph(b"test").expect("graph");
        assert_eq!(graph_ref.write_buf.node_count(), 2);
        assert_eq!(graph_ref.write_buf.edge_count(), 1);
    }
}
