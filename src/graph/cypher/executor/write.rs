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

    // Frozen CSR segments, cloned once (cheap Arc clones): the write path
    // must SEE the frozen tier (scans/expands/filters) so SET/DELETE/MERGE
    // can copy-up frozen rows instead of silently missing them. No freeze
    // can run mid-query (freeze_and_compact is only driven by the ADDEDGE
    // handler), so a snapshot at entry is safe.
    let csr_segs: Vec<std::sync::Arc<crate::graph::csr::CsrStorage>> =
        graph.segments.load().immutable.clone();

    let slot_table = SlotTable::from_plan(plan);
    let empty_table = SlotTable::default();
    let empty_row = Row::seed(&empty_table);
    let mut rows: Vec<Row> = vec![Row::seed(&slot_table)];
    let mut columns = Vec::new();
    let mut projected_rows: Option<Vec<Vec<Value>>> = None;
    let mut nodes_created: u64 = 0;
    let mut nodes_deleted: u64 = 0;
    let mut mutations: Vec<MutationRecord> = Vec::new();
    let mut properties_set: u64 = 0;

    for op in &plan.operators {
        match op {
            // W2-2 copy-up: the write path scans BOTH tiers so SET/DELETE/
            // MERGE can target frozen rows (the mutation arms copy the row
            // up into the write buffer first). The IndexScan arm degrades to
            // the same label scan; the residual Filter the planner emits
            // keeps results exact.
            PhysicalOp::NodeScan { variable, label }
            | PhysicalOp::IndexScan {
                variable, label, ..
            } => {
                let label_id = label.as_ref().map(|l| label_to_id(l.as_bytes()));
                let committed = roaring::RoaringBitmap::new();
                let view = crate::graph::view::MergedNodeView::new(&graph.write_buf, &csr_segs);
                let mut keys = Vec::new();
                view.for_each_visible_node(label_id, u64::MAX, 0, &committed, None, |k| {
                    keys.push(k)
                });
                let mut new_rows = Vec::with_capacity(rows.len() * keys.len());
                for row in &rows {
                    for &key in &keys {
                        let mut new_row = row.clone();
                        new_row.insert(variable, Value::Node(key));
                        new_rows.push(new_row);
                    }
                }
                rows = new_rows;
            }

            PhysicalOp::Expand {
                source,
                target,
                edge_variable,
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

                // Merge reader: mutable adjacency + frozen CSR edges, so
                // MATCH...SET/DELETE finds frozen endpoints. (Frozen edges
                // carry a placeholder EdgeKey — SET/DELETE on a frozen EDGE
                // stays a no-op; nodes are the copy-up unit.)
                let edge_type_filter = if type_ids.len() == 1 {
                    Some(type_ids[0])
                } else {
                    None
                };
                let reader = crate::graph::traversal::SegmentMergeReader::new(
                    Some(&graph.write_buf),
                    &csr_segs,
                    dir,
                    u64::MAX,
                    edge_type_filter,
                );
                let mut nb_seen = crate::graph::fasthash::FxHashSet::default();
                let mut nb_buf: Vec<crate::graph::traversal::MergedNeighbor> = Vec::new();

                let mut new_rows = Vec::new();
                for row in &rows {
                    let src_key = match row.get(source) {
                        Some(Value::Node(k)) => *k,
                        _ => continue,
                    };

                    if *max_hops <= 1 {
                        reader.neighbors_into(src_key, &mut nb_seen, &mut nb_buf);
                        for merged in &nb_buf {
                            if type_ids.len() > 1 && !type_ids.contains(&merged.edge_type) {
                                continue;
                            }
                            let mut new_row = row.clone();
                            new_row.insert(target, Value::Node(merged.node));
                            // Phase 174 FIX-01: bind edge variable so DELETE r
                            // can reference it. Previously ignored (`_`), which
                            // made `DELETE r` a silent no-op.
                            if let Some(evar) = edge_variable {
                                new_row.insert(evar, Value::Edge(merged.edge));
                            }
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
                                reader.neighbors_into(current, &mut nb_seen, &mut nb_buf);
                                for merged in &nb_buf {
                                    let neighbor_key = merged.node;
                                    if visited.contains(&neighbor_key) {
                                        continue;
                                    }
                                    if type_ids.len() > 1 && !type_ids.contains(&merged.edge_type) {
                                        continue;
                                    }
                                    visited.insert(neighbor_key);
                                    next_frontier.push(neighbor_key);

                                    if hop >= *min_hops {
                                        let mut new_row = row.clone();
                                        new_row.insert(target, Value::Node(neighbor_key));
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
                        eval_expr(
                            expr,
                            row,
                            &graph.write_buf,
                            params,
                            &csr_segs,
                            u64::MAX,
                            None
                        ),
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
                                        .map(|(k, v)| (k.to_owned(), v.clone()))
                                        .collect();
                                    Value::Map(entries)
                                } else {
                                    eval_expr(
                                        &item.expr,
                                        row,
                                        &graph.write_buf,
                                        params,
                                        &csr_segs,
                                        u64::MAX,
                                        None,
                                    )
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
                            let va = eval_expr(
                                expr,
                                a,
                                &graph.write_buf,
                                params,
                                &csr_segs,
                                u64::MAX,
                                None,
                            );
                            let vb = eval_expr(
                                expr,
                                b,
                                &graph.write_buf,
                                params,
                                &csr_segs,
                                u64::MAX,
                                None,
                            );
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
                let n = match eval_expr(
                    count,
                    &empty_row,
                    &graph.write_buf,
                    params,
                    &csr_segs,
                    u64::MAX,
                    None,
                ) {
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
                let n = match eval_expr(
                    count,
                    &empty_row,
                    &graph.write_buf,
                    params,
                    &csr_segs,
                    u64::MAX,
                    None,
                ) {
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
                    let val = eval_expr(
                        expr,
                        row,
                        &graph.write_buf,
                        params,
                        &csr_segs,
                        u64::MAX,
                        None,
                    );
                    if let Value::List(items) = val {
                        for item in items {
                            let mut new_row = row.clone();
                            new_row.insert(alias, item);
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
                                    let val = eval_expr(
                                        expr,
                                        &new_row,
                                        &graph.write_buf,
                                        params,
                                        &csr_segs,
                                        u64::MAX,
                                        None,
                                    );
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
                                new_row.insert(var, Value::Node(nk));
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
                            let Some(&dst) = node_keys.get(i + 1) else {
                                break;
                            };
                            let edge_type = pe
                                .edge_types
                                .first()
                                .map(|t| label_to_id(t.as_bytes()))
                                .unwrap_or(0);
                            if let Ok(ek) = graph
                                .write_buf
                                .add_edge(src, dst, edge_type, 1.0, None, lsn)
                            {
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
                                    let val = eval_expr(
                                        value,
                                        row,
                                        &graph.write_buf,
                                        params,
                                        &csr_segs,
                                        u64::MAX,
                                        None,
                                    );
                                    if let Some(pv) = value_to_property_value(&val) {
                                        let pid = label_to_id(property.as_bytes());
                                        // W2-2: frozen target → copy the row up
                                        // into the write buffer, then mutate.
                                        graph.copy_up_node(*nk);
                                        if let Some(node) = graph.write_buf.get_node_mut(*nk) {
                                            // Phase 174 FIX-01: snapshot old value BEFORE
                                            // mutating so TXN.ABORT can restore it.
                                            let old_value = node
                                                .properties
                                                .iter()
                                                .find(|(k, _)| *k == pid)
                                                .map(|(_, v)| v.clone());
                                            mutations.push(MutationRecord::SetProperty {
                                                entity_id: nk.data().as_ffi(),
                                                is_node: true,
                                                key: pid,
                                                old_value,
                                                new_value: pv.clone(),
                                            });

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
                                    graph.copy_up_node(*nk);
                                    if let Some(node) = graph.write_buf.get_node_mut(*nk) {
                                        if !node.labels.contains(&lid) {
                                            node.labels.push(lid);
                                            // W2-9: record for WAL durability
                                            // (idempotent — only when newly
                                            // added).
                                            mutations.push(MutationRecord::SetLabel {
                                                node_id: nk.data().as_ffi(),
                                                label: lid,
                                            });
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
                        let val = eval_expr(
                            expr,
                            row,
                            &graph.write_buf,
                            params,
                            &csr_segs,
                            u64::MAX,
                            None,
                        );
                        match val {
                            Value::Node(nk) => {
                                // W2-2: frozen target → copy the row up so the
                                // soft-delete lands in the write buffer as a
                                // TOMBSTONE shadowing the frozen row.
                                graph.copy_up_node(nk);
                                // Phase 174 FIX-01: snapshot node state BEFORE
                                // soft-delete so TXN.ABORT can un-delete.
                                if let Some(node) = graph.write_buf.get_node(nk) {
                                    if node.deleted_lsn == u64::MAX {
                                        mutations.push(MutationRecord::DeleteNode {
                                            node_id: nk.data().as_ffi(),
                                            labels: node.labels.clone(),
                                            properties: node.properties.clone(),
                                            embedding: node.embedding.clone(),
                                        });
                                    }
                                }
                                graph.write_buf.remove_node(nk, lsn);
                                nodes_deleted += 1;
                            }
                            Value::Edge(ek) => {
                                // Phase 174 FIX-01: snapshot edge state BEFORE
                                // soft-delete so TXN.ABORT can un-delete.
                                if let Some(edge) = graph.write_buf.get_edge(ek) {
                                    if edge.deleted_lsn == u64::MAX {
                                        mutations.push(MutationRecord::DeleteEdge {
                                            edge_id: ek.data().as_ffi(),
                                            src_id: edge.src.data().as_ffi(),
                                            dst_id: edge.dst.data().as_ffi(),
                                            edge_type: edge.edge_type,
                                            weight: edge.weight,
                                            properties: edge.properties.clone(),
                                        });
                                    }
                                }
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
                                let val = eval_expr(
                                    expr,
                                    &new_row,
                                    &graph.write_buf,
                                    params,
                                    &csr_segs,
                                    u64::MAX,
                                    None,
                                );
                                value_to_property_value(&val)
                                    .map(|pv| (label_to_id(name.as_bytes()), pv))
                            })
                            .collect();

                        // Search BOTH tiers for an existing node matching
                        // labels + properties (a mutable-only search would
                        // duplicate frozen nodes on every MERGE).
                        let found = find_node_merged(graph, &csr_segs, &label_ids, &match_props);

                        if let Some(existing_key) = found {
                            // MATCH path: bind variable and apply on_match.
                            if let Some(ref var) = pn.variable {
                                new_row.insert(var, Value::Node(existing_key));
                            }
                            apply_set_items(
                                on_match,
                                &new_row,
                                graph,
                                &csr_segs,
                                params,
                                &mut properties_set,
                                Some(&mut mutations),
                            );
                        } else {
                            // CREATE path: create node.
                            let props: PropertyMap = match_props.into_iter().collect();
                            let labels_clone = label_ids.clone();
                            let props_clone = props.clone();
                            let nk = graph.write_buf.add_node(label_ids, props, None, lsn);
                            nodes_created += 1;
                            mutations.push(MutationRecord::CreateNode {
                                node_id: nk.data().as_ffi(),
                                labels: labels_clone,
                                properties: props_clone,
                                embedding: None,
                            });
                            if let Some(ref var) = pn.variable {
                                new_row.insert(var, Value::Node(nk));
                            }
                            apply_set_items(
                                on_create,
                                &new_row,
                                graph,
                                &csr_segs,
                                params,
                                &mut properties_set,
                                // W2-9: ON CREATE SET needs mutation records
                                // too — the CreateNode WAL snapshot predates
                                // the SET (see apply_set_items doc).
                                Some(&mut mutations),
                            );
                        }
                    } else if !pattern.edges.is_empty() && pattern.nodes.len() >= 2 {
                        // ---- Edge MERGE ----
                        let src_pn = &pattern.nodes[0];
                        let dst_pn = &pattern.nodes[1];
                        let pe = &pattern.edges[0];

                        // Resolve or find source node (both tiers).
                        let src_key =
                            resolve_or_find_node(src_pn, &new_row, graph, &csr_segs, params);
                        let dst_key =
                            resolve_or_find_node(dst_pn, &new_row, graph, &csr_segs, params);

                        let edge_type_id = pe
                            .edge_types
                            .first()
                            .map(|t| label_to_id(t.as_bytes()))
                            .unwrap_or(0);

                        match (src_key, dst_key) {
                            (Some(sk), Some(dk)) => {
                                // Check if edge exists in EITHER tier (frozen
                                // edges live in CSR adjacency).
                                let edge_exists = {
                                    let reader = crate::graph::traversal::SegmentMergeReader::new(
                                        Some(&graph.write_buf),
                                        &csr_segs,
                                        Direction::Outgoing,
                                        u64::MAX,
                                        Some(edge_type_id),
                                    );
                                    reader.neighbors(sk).iter().any(|m| m.node == dk)
                                };

                                if edge_exists {
                                    // Bind variables.
                                    if let Some(ref var) = src_pn.variable {
                                        new_row.insert(var, Value::Node(sk));
                                    }
                                    if let Some(ref var) = dst_pn.variable {
                                        new_row.insert(var, Value::Node(dk));
                                    }
                                    apply_set_items(
                                        on_match,
                                        &new_row,
                                        graph,
                                        &csr_segs,
                                        params,
                                        &mut properties_set,
                                        Some(&mut mutations),
                                    );
                                } else {
                                    // Create edge.
                                    if let Ok(ek) = graph.write_buf.add_edge_across_tiers(
                                        sk,
                                        dk,
                                        edge_type_id,
                                        1.0,
                                        None,
                                        lsn,
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
                                        new_row.insert(var, Value::Node(sk));
                                    }
                                    if let Some(ref var) = dst_pn.variable {
                                        new_row.insert(var, Value::Node(dk));
                                    }
                                    apply_set_items(
                                        on_create,
                                        &new_row,
                                        graph,
                                        &csr_segs,
                                        params,
                                        &mut properties_set,
                                        // W2-9: see apply_set_items doc.
                                        Some(&mut mutations),
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
                                                expr,
                                                &new_row,
                                                &graph.write_buf,
                                                params,
                                                &csr_segs,
                                                u64::MAX,
                                                None,
                                            );
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
                                                expr,
                                                &new_row,
                                                &graph.write_buf,
                                                params,
                                                &csr_segs,
                                                u64::MAX,
                                                None,
                                            );
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
                                    nk
                                };
                                if let Ok(ek) = graph.write_buf.add_edge_across_tiers(
                                    sk,
                                    dk,
                                    edge_type_id,
                                    1.0,
                                    None,
                                    lsn,
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
                                    new_row.insert(var, Value::Node(sk));
                                }
                                if let Some(ref var) = dst_pn.variable {
                                    new_row.insert(var, Value::Node(dk));
                                }
                                apply_set_items(
                                    on_create,
                                    &new_row,
                                    graph,
                                    &csr_segs,
                                    params,
                                    &mut properties_set,
                                    None,
                                );
                            }
                        }
                    }

                    new_rows.push(new_row);
                }
                rows = new_rows;
            }

            PhysicalOp::ProcedureCall { .. } => {
                // Phase 174 FIX-02: surface partial mutations accumulated
                // before the unsupported op so TXN.ABORT can roll them back.
                return Err(ExecError {
                    kind: ExecErrorKind::Unsupported(
                        "procedure calls not yet implemented in executor".into(),
                    ),
                    partial_mutations: mutations,
                });
            }

            PhysicalOp::ShortestPath { .. } => {
                // v0.1.9 CYP-04/05: shortestPath() is a read-only query
                // operator. It is not meaningful inside a CREATE/SET/DELETE
                // write-path. Reject here and route users to GRAPH.RO_QUERY
                // or GRAPH.QUERY for reads.
                //
                // Phase 174 FIX-02: surface partial mutations accumulated
                // before the unsupported op so TXN.ABORT can roll them back.
                return Err(ExecError {
                    kind: ExecErrorKind::Unsupported(
                        "shortestPath() requires a read-only Cypher query".into(),
                    ),
                    partial_mutations: mutations,
                });
            }
        }
    }

    let final_rows = if let Some(pr) = projected_rows {
        pr
    } else {
        if columns.is_empty() && !rows.is_empty() {
            columns = slot_table.names().to_vec();
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
///
/// Phase 174 FIX-01: accepts an optional `mutations` vec to emit
/// `MutationRecord::SetProperty` records for MERGE ON MATCH SET rollback.
/// W2-9: mutation records now also drive WAL generation, so ON CREATE SET
/// paths must pass `Some` too — the CreateNode WAL record snapshots
/// properties BEFORE the SET applies, and without a SetProperty record the
/// ON CREATE SET values are silently lost on kill -9. (Rollback stays
/// correct: the extra RestoreProperty undo is a no-op on a node the
/// CreateNode intent removes entirely.)
///
/// W2-2: takes the whole `NamedGraph` (not just the write buffer) so a
/// frozen target node can be copied up before the in-place mutation.
pub(crate) fn apply_set_items(
    items: &[SetItem],
    row: &Row<'_>,
    graph: &mut NamedGraph,
    csr_segs: &[std::sync::Arc<crate::graph::csr::CsrStorage>],
    params: &HashMap<String, Value>,
    properties_set: &mut u64,
    mut mutations: Option<&mut Vec<MutationRecord>>,
) {
    for item in items {
        match item {
            SetItem::Property {
                variable,
                property,
                value,
            } => {
                if let Some(Value::Node(nk)) = row.get(variable) {
                    let val = eval_expr(
                        value,
                        row,
                        &graph.write_buf,
                        params,
                        csr_segs,
                        u64::MAX,
                        None,
                    );
                    if let Some(pv) = value_to_property_value(&val) {
                        let pid = label_to_id(property.as_bytes());
                        graph.copy_up_node(*nk);
                        if let Some(node) = graph.write_buf.get_node_mut(*nk) {
                            // Phase 174 FIX-01: snapshot old value for rollback.
                            if let Some(muts) = mutations.as_mut() {
                                let old_value = node
                                    .properties
                                    .iter()
                                    .find(|(k, _)| *k == pid)
                                    .map(|(_, v)| v.clone());
                                muts.push(MutationRecord::SetProperty {
                                    entity_id: nk.data().as_ffi(),
                                    is_node: true,
                                    key: pid,
                                    old_value,
                                    new_value: pv.clone(),
                                });
                            }

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
                    graph.copy_up_node(*nk);
                    if let Some(node) = graph.write_buf.get_node_mut(*nk) {
                        if !node.labels.contains(&lid) {
                            node.labels.push(lid);
                            // W2-9: WAL durability (idempotent — only when
                            // newly added).
                            if let Some(muts) = mutations.as_mut() {
                                muts.push(MutationRecord::SetLabel {
                                    node_id: nk.data().as_ffi(),
                                    label: lid,
                                });
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Find a node matching all `label_ids` + `match_props` across BOTH tiers.
/// Mutable tier first (direct field access, no clones), then frozen segments
/// via `MergedNodeView` (which skips rows shadowed by copy-up entries).
pub(crate) fn find_node_merged(
    graph: &NamedGraph,
    csr_segs: &[std::sync::Arc<crate::graph::csr::CsrStorage>],
    label_ids: &[u16],
    match_props: &[(u16, PropertyValue)],
) -> Option<NodeKey> {
    let matches_mutable = |node: &crate::graph::types::MutableNode| {
        label_ids.iter().all(|lid| node.labels.contains(lid))
            && match_props.iter().all(|(pid, pval)| {
                node.properties
                    .iter()
                    .any(|(np, nv)| *np == *pid && *nv == *pval)
            })
    };
    if let Some(k) = graph
        .write_buf
        .iter_nodes()
        .find(|(_, node)| matches_mutable(node))
        .map(|(k, _)| k)
    {
        return Some(k);
    }

    let view = crate::graph::view::MergedNodeView::new(&graph.write_buf, csr_segs);
    let committed = roaring::RoaringBitmap::new();
    let mut found = None;
    view.for_each_visible_node(
        label_ids.first().copied(),
        u64::MAX,
        0,
        &committed,
        None,
        |k| {
            if found.is_some() {
                return;
            }
            // Mutable tier already searched above.
            if graph.write_buf.get_node(k).is_some() {
                return;
            }
            let Some(labels) = view.labels(k) else {
                return;
            };
            if !label_ids.iter().all(|lid| labels.contains(lid)) {
                return;
            }
            for (pid, pval) in match_props {
                match view.property(k, *pid) {
                    Some(v) if v == *pval => {}
                    _ => return,
                }
            }
            found = Some(k);
        },
    );
    found
}

/// Resolve a pattern node: if it's a bound variable in the row, return that key.
/// Otherwise, search BOTH tiers for a matching node by labels + properties.
pub(crate) fn resolve_or_find_node(
    pn: &PatternNode,
    row: &Row<'_>,
    graph: &NamedGraph,
    csr_segs: &[std::sync::Arc<crate::graph::csr::CsrStorage>],
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
            let val = eval_expr(
                expr,
                row,
                &graph.write_buf,
                params,
                csr_segs,
                u64::MAX,
                None,
            );
            value_to_property_value(&val).map(|pv| (label_to_id(name.as_bytes()), pv))
        })
        .collect();

    find_node_merged(graph, csr_segs, &label_ids, &match_props)
}
