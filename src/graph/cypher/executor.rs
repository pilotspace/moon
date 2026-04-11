//! Cypher execution engine -- walks PhysicalPlan operators and produces result rows.
//!
//! Row-based pipeline model: each operator transforms a `Vec<Row>` where
//! `Row = HashMap<String, Value>`. The executor starts with one empty seed row
//! and sequentially applies each `PhysicalOp` from the plan.

use std::collections::HashMap;

use crate::command::graph::graph_write::label_to_id;
use crate::graph::cypher::ast::*;
use crate::graph::cypher::planner::*;
use crate::graph::store::NamedGraph;
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

/// Execute result: column headers + data rows + statistics.
pub struct ExecResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub nodes_created: u64,
    pub nodes_deleted: u64,
    pub properties_set: u64,
    pub execution_time_us: u64,
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

                let mut new_rows = Vec::new();
                for row in &rows {
                    let src_key = match row.get(source) {
                        Some(Value::Node(k)) => *k,
                        _ => continue,
                    };

                    if *max_hops <= 1 {
                        // Single-hop expansion.
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
                        // Variable-length expansion via BFS.
                        let mut frontier = vec![src_key];
                        let mut visited =
                            std::collections::HashSet::new();
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
}
