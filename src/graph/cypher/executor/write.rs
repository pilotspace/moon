use std::collections::HashMap;

use super::*;

/// Execute a physical plan with write access to the named graph.
///
/// Identical pipeline to [`execute`] but handles write operators:
/// `CreatePattern`, `SetProperties`, `DeleteEntities`, and `Merge`.
pub fn execute_mut(
    graph: &mut NamedGraph,
    plan: &PhysicalPlan,
    params: &HashMap<String, Value>,
    lsn: u64,
) -> Result<ExecResult, ExecError> {
    let start = std::time::Instant::now();

    let mut rows: Vec<Row> = vec![HashMap::new()];
    let mut columns = Vec::new();
    let mut projected_rows: Option<Vec<Vec<Value>>> = None;
    let mut nodes_created: u64 = 0;
    let mut nodes_deleted: u64 = 0;
    let mut mutations: Vec<MutationRecord> = Vec::new();
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
                            let labels_clone = labels.clone();
                            let props_clone = props.clone();
                            let nk = graph.write_buf.add_node(labels, props, None, lsn);
                            nodes_created += 1;
                            mutations.push(MutationRecord::CreateNode {
                                node_id: nk.data().as_ffi(),
                                labels: labels_clone,
                                properties: props_clone,
                                embedding: None,
                            });
                            if let Some(ref var) = pn.variable {
                                new_row.insert(var.clone(), Value::Node(nk));
                            }
                            node_keys.push(nk);
                        }
                        // Create edges.
                        if pattern.edges.len() >= node_keys.len() && !pattern.edges.is_empty() {
                            // More edges than nodes-1: malformed pattern, skip edges.
                            continue;
                        }
                        for (i, pe) in pattern.edges.iter().enumerate() {
                            let Some(&src) = node_keys.get(i) else { break };
                            let Some(&dst) = node_keys.get(i + 1) else { break };
                            let edge_type = pe
                                .edge_types
                                .first()
                                .map(|t| label_to_id(t.as_bytes()))
                                .unwrap_or(0);
                            if let Ok(ek) = graph.write_buf.add_edge(
                                src, dst, edge_type, 1.0, None, lsn,
                            ) {
                                mutations.push(MutationRecord::CreateEdge {
                                    edge_id: ek.data().as_ffi(),
                                    src_id: src.data().as_ffi(),
                                    dst_id: dst.data().as_ffi(),
                                    edge_type,
                                    weight: 1.0,
                                    properties: None,
                                });
                            }
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
                                graph.write_buf.remove_node(nk, lsn);
                                nodes_deleted += 1;
                            }
                            Value::Edge(ek) => {
                                graph.write_buf.remove_edge(ek, lsn);
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
                            let labels_clone = label_ids.clone();
                            let props_clone = props.clone();
                            let nk = graph.write_buf.add_node(
                                label_ids, props, None, lsn,
                            );
                            nodes_created += 1;
                            mutations.push(MutationRecord::CreateNode {
                                node_id: nk.data().as_ffi(),
                                labels: labels_clone,
                                properties: props_clone,
                                embedding: None,
                            });
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
                                    if let Ok(ek) = graph.write_buf.add_edge(
                                        sk, dk, edge_type_id, 1.0, None, lsn,
                                    ) {
                                        mutations.push(MutationRecord::CreateEdge {
                                            edge_id: ek.data().as_ffi(),
                                            src_id: sk.data().as_ffi(),
                                            dst_id: dk.data().as_ffi(),
                                            edge_type: edge_type_id,
                                            weight: 1.0,
                                            properties: None,
                                        });
                                    }
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
                                let sk = if let Some(existing) = src_key {
                                    existing
                                } else {
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
                                    let labels_clone = labels.clone();
                                    let props_clone = props.clone();
                                    let nk = graph.write_buf.add_node(
                                        labels, props, None, lsn,
                                    );
                                    nodes_created += 1;
                                    mutations.push(MutationRecord::CreateNode {
                                        node_id: nk.data().as_ffi(),
                                        labels: labels_clone,
                                        properties: props_clone,
                                        embedding: None,
                                    });
                                    nk
                                };
                                let dk = if let Some(existing) = dst_key {
                                    existing
                                } else {
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
                                    let labels_clone = labels.clone();
                                    let props_clone = props.clone();
                                    let nk = graph.write_buf.add_node(
                                        labels, props, None, lsn,
                                    );
                                    nodes_created += 1;
                                    mutations.push(MutationRecord::CreateNode {
                                        node_id: nk.data().as_ffi(),
                                        labels: labels_clone,
                                        properties: props_clone,
                                        embedding: None,
                                    });
                                    nk
                                };
                                if let Ok(ek) = graph.write_buf.add_edge(
                                    sk, dk, edge_type_id, 1.0, None, lsn,
                                ) {
                                    mutations.push(MutationRecord::CreateEdge {
                                        edge_id: ek.data().as_ffi(),
                                        src_id: sk.data().as_ffi(),
                                        dst_id: dk.data().as_ffi(),
                                        edge_type: edge_type_id,
                                        weight: 1.0,
                                        properties: None,
                                    });
                                }
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
        mutations,
    })
}

/// Apply SET items (ON CREATE SET / ON MATCH SET) to a node in the row.
pub(crate) fn apply_set_items(
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
pub(crate) fn resolve_or_find_node(
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
