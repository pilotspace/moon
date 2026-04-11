use std::collections::HashMap;

use super::*;

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
                        // Enforce limits to prevent DoS via exponential row growth.
                        const MAX_HOPS_LIMIT: u32 = 20;
                        const MAX_RESULT_ROWS: usize = 100_000;
                        let capped_max_hops = (*max_hops).min(MAX_HOPS_LIMIT);

                        let mut frontier = vec![src_key];
                        let mut visited =
                            std::collections::HashSet::new();
                        visited.insert(src_key);

                        for hop in 1..=capped_max_hops {
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
                                        if new_rows.len() >= MAX_RESULT_ROWS {
                                            break;
                                        }
                                    }
                                }
                                if new_rows.len() >= MAX_RESULT_ROWS {
                                    break;
                                }
                            }
                            frontier = next_frontier;
                            if frontier.is_empty() || new_rows.len() >= MAX_RESULT_ROWS {
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
        mutations: Vec::new(),
    })
}

// ---------------------------------------------------------------------------
// Profiling executor
// ---------------------------------------------------------------------------

/// Get a human-readable name for a physical operator.
pub(crate) fn op_name(op: &PhysicalOp) -> &'static str {
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
                        // Variable-length expansion via BFS.
                        // Enforce limits to prevent DoS via exponential row growth.
                        const MAX_HOPS_LIMIT: u32 = 20;
                        const MAX_RESULT_ROWS: usize = 100_000;
                        let capped_max_hops = (*max_hops).min(MAX_HOPS_LIMIT);

                        let mut frontier = vec![src_key];
                        let mut visited = std::collections::HashSet::new();
                        visited.insert(src_key);

                        for hop in 1..=capped_max_hops {
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
                                        if new_rows.len() >= MAX_RESULT_ROWS {
                                            break;
                                        }
                                    }
                                }
                                if new_rows.len() >= MAX_RESULT_ROWS {
                                    break;
                                }
                            }
                            frontier = next_frontier;
                            if frontier.is_empty() || new_rows.len() >= MAX_RESULT_ROWS {
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
            mutations: Vec::new(),
        },
        operator_profiles: profiles,
    })
}
