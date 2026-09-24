use std::collections::HashMap;

use super::*;

#[path = "index_scan.rs"]
mod index_scan;
#[path = "pipeline.rs"]
mod pipeline;
#[path = "topk.rs"]
mod topk;
pub(super) use index_scan::{index_scan_keys, index_scan_try_for_each};
#[cfg(test)]
pub(crate) use topk::fused_counts;

/// W2-12: if `expr` is a top-level aggregate call, return
/// `(lowercase name, input expr, distinct)`. `None` input = `count(*)` /
/// bare `count()` (counts rows, not values). Aggregates nested inside a
/// larger expression (`count(n) + 1`) are NOT recognized — the item then
/// evaluates per-row like any scalar (pre-existing behavior), so keep
/// aggregates at the top level of a RETURN item.
fn aggregate_call(expr: &Expr) -> Option<(&'static str, Option<&Expr>, bool)> {
    let Expr::FunctionCall {
        name,
        args,
        distinct,
    } = expr
    else {
        return None;
    };
    let canon: &'static str = match name.to_ascii_lowercase().as_str() {
        "count" => "count",
        "sum" => "sum",
        "avg" => "avg",
        "min" => "min",
        "max" => "max",
        "collect" => "collect",
        _ => return None,
    };
    let input = args.first().filter(|a| !matches!(a, Expr::Star));
    Some((canon, input, *distinct))
}

/// W2-12: aggregate projection with implicit grouping (openCypher): the
/// non-aggregate RETURN items form the group key; each group emits one row.
/// No group key + zero input rows still emits ONE row (`count` = 0, `sum` =
/// 0, `collect` = [], others Null); a present group key over zero rows
/// emits none. Null inputs are skipped by every aggregate except `count(*)`,
/// which counts rows. Returns `None` when no item is an aggregate (caller
/// takes the plain per-row projection path).
fn try_project_aggregate(
    items: &[ReturnItem],
    row_count: usize,
    mut eval_at: impl FnMut(&Expr, usize) -> Value,
) -> Option<Vec<Vec<Value>>> {
    let specs: Vec<Option<(&'static str, Option<&Expr>, bool)>> =
        items.iter().map(|it| aggregate_call(&it.expr)).collect();
    if specs.iter().all(Option::is_none) {
        return None;
    }
    let has_group_key = specs.iter().any(Option::is_none);
    let agg_count = specs.iter().filter(|s| s.is_some()).count();

    // Group rows by the stringified key (same "simple approach" as
    // dedup_rows), keeping first-seen order for deterministic output.
    struct Group {
        key_values: Vec<Value>,
        inputs: Vec<Vec<Value>>,
    }
    let mut order: Vec<String> = Vec::new();
    let mut groups: HashMap<String, Group> = HashMap::new();

    for ri in 0..row_count {
        let mut key_values = Vec::new();
        let mut key_str = String::new();
        for (item, spec) in items.iter().zip(&specs) {
            if spec.is_none() {
                let v = eval_at(&item.expr, ri);
                key_str.push_str(&value_to_string(&v));
                key_str.push('\u{1f}');
                key_values.push(v);
            }
        }
        let group = groups.entry(key_str.clone()).or_insert_with(|| {
            order.push(key_str);
            Group {
                key_values,
                inputs: vec![Vec::new(); agg_count],
            }
        });
        for (ai, spec) in specs.iter().flatten().enumerate() {
            match spec.1 {
                Some(input_expr) => {
                    let v = eval_at(input_expr, ri);
                    if !matches!(v, Value::Null) {
                        group.inputs[ai].push(v);
                    }
                }
                // count(*): every row counts.
                None => group.inputs[ai].push(Value::Int(1)),
            }
        }
    }

    // Global aggregate over zero rows: one synthetic empty group.
    if groups.is_empty() && !has_group_key {
        let key = String::new();
        order.push(key.clone());
        groups.insert(
            key,
            Group {
                key_values: Vec::new(),
                inputs: vec![Vec::new(); agg_count],
            },
        );
    }

    let finalize = |name: &str, mut inputs: Vec<Value>, distinct: bool| -> Value {
        if distinct {
            let mut seen = std::collections::HashSet::new();
            inputs.retain(|v| seen.insert(value_to_string(v)));
        }
        match name {
            "count" => Value::Int(inputs.len() as i64),
            "sum" => {
                if inputs.iter().any(|v| matches!(v, Value::Float(_))) {
                    Value::Float(inputs.iter().fold(0.0, |acc, v| match v {
                        Value::Int(i) => acc + *i as f64,
                        Value::Float(f) => acc + f,
                        _ => acc,
                    }))
                } else {
                    Value::Int(inputs.iter().fold(0i64, |acc, v| match v {
                        Value::Int(i) => acc.saturating_add(*i),
                        _ => acc,
                    }))
                }
            }
            "avg" => {
                let numeric: Vec<f64> = inputs
                    .iter()
                    .filter_map(|v| match v {
                        Value::Int(i) => Some(*i as f64),
                        Value::Float(f) => Some(*f),
                        _ => None,
                    })
                    .collect();
                if numeric.is_empty() {
                    Value::Null
                } else {
                    Value::Float(numeric.iter().sum::<f64>() / numeric.len() as f64)
                }
            }
            "min" => inputs
                .into_iter()
                .min_by(compare_values)
                .unwrap_or(Value::Null),
            "max" => inputs
                .into_iter()
                .max_by(compare_values)
                .unwrap_or(Value::Null),
            "collect" => Value::List(inputs),
            _ => Value::Null,
        }
    };

    let mut out = Vec::with_capacity(order.len());
    for key in &order {
        let Some(group) = groups.remove(key) else {
            continue;
        };
        let mut key_iter = group.key_values.into_iter();
        let mut input_iter = group.inputs.into_iter();
        let row: Vec<Value> = specs
            .iter()
            .map(|spec| match spec {
                None => key_iter.next().unwrap_or(Value::Null),
                Some((name, _, distinct)) => {
                    finalize(name, input_iter.next().unwrap_or_default(), *distinct)
                }
            })
            .collect();
        out.push(row);
    }
    Some(out)
}

/// Per-hop wall-clock check for multi-hop operators (bounded epoch hold).
/// No-op when the context carries no guard (`Default` / unit tests).
#[inline]
fn guard_check(ctx: &ExecutionContext) -> Result<(), ExecError> {
    if let Some(guard) = &ctx.guard {
        if let Err(t) = guard.check_timeout() {
            return Err(ExecError {
                kind: ExecErrorKind::Timeout(t),
                partial_mutations: Vec::new(),
            });
        }
    }
    Ok(())
}

/// Execute a physical plan against a named graph.
///
/// Builds a fresh `SlotTable` from `plan` on every call. Hot paths that
/// already have a `SlotTable` on hand (e.g. a plan-cache hit, which cached
/// the table alongside the plan at insert time) should call
/// `execute_with_slots` directly instead to skip the rebuild.
pub fn execute(
    graph: &NamedGraph,
    plan: &PhysicalPlan,
    params: &HashMap<String, Value>,
    ctx: &ExecutionContext,
) -> Result<ExecResult, ExecError> {
    let slot_table = SlotTable::from_plan(plan);
    execute_with_slots(graph, plan, &slot_table, params, ctx)
}

/// Execute a physical plan against a named graph, using a caller-supplied
/// `SlotTable` instead of rebuilding one from `plan`.
///
/// Plan-cache hot path (see `PlanCache::get` / `graph_read.rs`): the
/// `SlotTable` is built exactly once, when the plan is first compiled and
/// inserted into the cache, and reused verbatim on every subsequent cache
/// hit -- a `SlotTable` depends only on the plan's bound variables, which
/// never change for a given `PhysicalPlan`.
///
/// moon#1197: operators still run one at a time over the whole row vector,
/// but (a) a leading `scan → row-local ops → [1:1 projection]` run whose
/// output a `LIMIT` bounds is fed in chunks and stops once enough rows exist,
/// and (b) `ORDER BY` precomputes its keys and keeps only the rows the plan
/// can use. Results — rows, order, ties — are identical to full evaluation.
pub fn execute_with_slots(
    graph: &NamedGraph,
    plan: &PhysicalPlan,
    slot_table: &SlotTable,
    params: &HashMap<String, Value>,
    ctx: &ExecutionContext,
) -> Result<ExecResult, ExecError> {
    let start = std::time::Instant::now();

    // Seed row: one empty row to bootstrap the pipeline.
    let empty_table = SlotTable::default();
    let empty_row = Row::seed(&empty_table);
    let nodes_created: u64 = 0;
    let nodes_deleted: u64 = 0;
    let properties_set: u64 = 0;

    // Build a SegmentMergeReader for cross-segment neighbor queries.
    let segments_guard = graph.segments.load();
    let env = pipeline::OpEnv {
        memgraph: &graph.write_buf,
        csr_segs: &segments_guard.immutable,
        params,
        ctx,
        slot_table,
        empty_row: &empty_row,
    };
    let mut st = pipeline::OpState {
        rows: vec![Row::seed(slot_table)],
        projected_rows: None,
        columns: Vec::new(),
        nodes_scanned: 0,
    };

    let ops = &plan.operators;
    let demand = pipeline::row_demand(ops, |count| env.count(count));
    let mut first = 0;
    // moon#1220: scan → row-local ops → RETURN … ORDER BY … LIMIT, streamed into a top-k.
    if let Some(end) = topk::stream_sorted_scan(ops, &demand, &mut st, &env)? {
        first = end;
    } else if let Some(end) = pipeline::streamable_prefix(ops, &demand) {
        pipeline::run_streamed_prefix(&ops[..end], demand[end - 1], &mut st, &env)?;
        first = end;
    }
    let mut i = first;
    while let Some(op) = ops.get(i) {
        // moon#1220: RETURN … ORDER BY … LIMIT projects only the rows it keeps.
        if let Some(fused) = topk::project_sorted_rows(ops, &demand, i, &mut st, &env) {
            i += fused;
            continue;
        }
        // A one-shot run: no rows emitted by earlier stream chunks (moon#1221 F3).
        apply_op(op, &mut st, &env, demand[i], 0)?;
        i += 1;
    }

    let pipeline::OpState {
        rows,
        projected_rows,
        mut columns,
        nodes_scanned,
    } = st;

    let final_rows = if let Some(pr) = projected_rows {
        pr
    } else {
        // No Project operator: return all row bindings as columns.
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
        nodes_scanned,
        execution_time_us: elapsed,
        mutations: Vec::new(),
    })
}

#[cfg(test)]
thread_local! {
    /// Rows every variable-length `Expand` emitted on this thread — test-only
    /// instrumentation for the expansion bound (moon#1221 review).
    pub(super) static VAR_LEN_ROWS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Apply one operator to the row stream. `keep` bounds how many of its output
/// rows the rest of the plan can use (only `Sort` exploits it). `emitted_before`
/// is how many rows this operator already produced for EARLIER chunks of the
/// same input (the streamed prefix feeds it chunk by chunk; 0 otherwise): the
/// variable-length `Expand`'s output cap counts them, so it spans the whole
/// input exactly as one-shot evaluation does (moon#1221 review).
fn apply_op<'t>(
    op: &PhysicalOp,
    st: &mut pipeline::OpState<'t>,
    env: &pipeline::OpEnv<'_, 't>,
    keep: usize,
    emitted_before: usize,
) -> Result<(), ExecError> {
    let memgraph = env.memgraph;
    let csr_segs = env.csr_segs;
    let params = env.params;
    let ctx = env.ctx;
    let slot_table = env.slot_table;
    let empty_row = env.empty_row;
    match op {
        PhysicalOp::NodeScan { variable, label } => {
            let label_id = label.as_ref().map(|l| label_to_id(l.as_bytes()));
            let committed = roaring::RoaringBitmap::new();
            // Scan BOTH tiers: the mutable write buffer and frozen CSR
            // segments (freeze DRAINS nodes — a memgraph-only scan loses
            // every frozen node).
            let view = crate::graph::view::MergedNodeView::new(memgraph, csr_segs);
            let mut keys = Vec::new();
            view.for_each_visible_node(
                label_id,
                ctx.snapshot_lsn,
                ctx.my_txn_id,
                &committed,
                ctx.valid_time_as_of,
                |k| keys.push(k),
            );
            st.nodes_scanned += keys.len() as u64;
            let mut new_rows = Vec::with_capacity(st.rows.len() * keys.len());
            for row in &st.rows {
                for &key in &keys {
                    let mut new_row = row.clone();
                    new_row.insert(variable, Value::Node(key));
                    new_rows.push(new_row);
                }
            }
            st.rows = new_rows;
        }

        PhysicalOp::IndexScan {
            variable,
            label,
            prop_eq,
            prop_range,
            text_pred,
        } => {
            let keys = index_scan_keys(
                memgraph,
                csr_segs,
                label.as_ref(),
                prop_eq,
                prop_range,
                text_pred,
                params,
                ctx,
            );
            st.nodes_scanned += keys.len() as u64;
            let mut new_rows = Vec::with_capacity(st.rows.len() * keys.len());
            for row in &st.rows {
                for &key in &keys {
                    let mut new_row = row.clone();
                    new_row.insert(variable, Value::Node(key));
                    new_rows.push(new_row);
                }
            }
            st.rows = new_rows;
        }

        PhysicalOp::Expand {
            source,
            target,
            edge_variable,
            edge_types,
            direction,
            min_hops,
            max_hops,
            optional,
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
            let reader =
                SegmentMergeReader::new(Some(memgraph), csr_segs, dir, u64::MAX, edge_type_filter);

            let committed = roaring::RoaringBitmap::new();
            let view = crate::graph::view::MergedNodeView::new(memgraph, csr_segs);
            // Scratch reused across every neighbor lookup in this Expand
            // (the allocating `neighbors()` built a HashSet+Vec per call).
            let mut nb_seen = crate::graph::fasthash::FxHashSet::default();
            let mut nb_buf: Vec<crate::graph::traversal::MergedNeighbor> = Vec::new();
            let mut new_rows = Vec::new();
            for row in &st.rows {
                let src_key = match row.get(source) {
                    Some(Value::Node(k)) => *k,
                    _ => {
                        // W2-13: a Null/unbound source under OPTIONAL
                        // MATCH survives null-padded instead of dropping.
                        if *optional {
                            push_null_padded(row, target, edge_variable, &mut new_rows);
                        }
                        continue;
                    }
                };
                let row_start = new_rows.len();

                if *max_hops <= 1 {
                    // Single-hop expansion via SegmentMergeReader.
                    reader.neighbors_into(src_key, &mut nb_seen, &mut nb_buf);
                    for merged in &nb_buf {
                        // Multi-type filter (SegmentMergeReader handles
                        // single-type; we need extra check for multi-type).
                        if type_ids.len() > 1 && !type_ids.contains(&merged.edge_type) {
                            continue;
                        }
                        // Bi-temporal visibility check on target node
                        // (merged view — frozen targets get the CSR
                        // NodeMeta check instead of a free pass).
                        if !view.is_visible(
                            merged.node,
                            ctx.snapshot_lsn,
                            ctx.my_txn_id,
                            &committed,
                            ctx.valid_time_as_of,
                        ) {
                            continue;
                        }
                        let mut new_row = row.clone();
                        new_row.insert(target, Value::Node(merged.node));
                        // v0.1.9 CYP-06: bind edge variable for single-hop
                        // expansion so WHERE r.valid_to >= $asof works.
                        if let Some(evar) = edge_variable {
                            new_row.insert(evar, Value::Edge(merged.edge));
                        }
                        new_rows.push(new_row);
                    }
                } else {
                    // Variable-length expansion via BFS using SegmentMergeReader.
                    // Enforce limits to prevent DoS via exponential row growth.
                    const MAX_HOPS_LIMIT: u32 = 20;
                    const MAX_RESULT_ROWS: usize = 100_000;
                    let capped_max_hops = (*max_hops).min(MAX_HOPS_LIMIT);
                    // The cap counts this operator's rows for the WHOLE input, earlier
                    // streamed chunks included.
                    let cap = MAX_RESULT_ROWS.saturating_sub(emitted_before);

                    let mut frontier = vec![src_key];
                    let mut visited = crate::graph::fasthash::FxHashSet::default();
                    visited.insert(src_key);

                    for hop in 1..=capped_max_hops {
                        guard_check(ctx)?;
                        let mut next_frontier = Vec::new();
                        for &current in &frontier {
                            reader.neighbors_into(current, &mut nb_seen, &mut nb_buf);
                            for merged in &nb_buf {
                                if visited.contains(&merged.node) {
                                    continue;
                                }
                                if type_ids.len() > 1 && !type_ids.contains(&merged.edge_type) {
                                    continue;
                                }
                                visited.insert(merged.node);
                                next_frontier.push(merged.node);

                                if hop >= *min_hops {
                                    let mut new_row = row.clone();
                                    new_row.insert(target, Value::Node(merged.node));
                                    new_rows.push(new_row);
                                    if new_rows.len() >= cap {
                                        break;
                                    }
                                }
                            }
                            if new_rows.len() >= cap {
                                break;
                            }
                        }
                        frontier = next_frontier;
                        if frontier.is_empty() || new_rows.len() >= cap {
                            break;
                        }
                    }
                }

                // W2-13: zero matches under OPTIONAL MATCH → the source
                // row survives with target/edge bound to Null.
                if *optional && new_rows.len() == row_start {
                    push_null_padded(row, target, edge_variable, &mut new_rows);
                }
            }
            #[cfg(test)]
            if *max_hops > 1 {
                VAR_LEN_ROWS.with(|n| n.set(n.get() + new_rows.len()));
            }
            st.rows = new_rows;
        }

        PhysicalOp::Filter { expr } => {
            st.rows.retain(|row| {
                matches!(
                    eval_expr(
                        expr,
                        row,
                        memgraph,
                        params,
                        csr_segs,
                        ctx.snapshot_lsn,
                        ctx.decay
                    ),
                    Value::Bool(true)
                )
            });
        }

        PhysicalOp::Project {
            items,
            distinct,
            rebind,
        } => {
            st.columns = items
                .iter()
                .map(|item| {
                    if let Some(alias) = &item.alias {
                        alias.clone()
                    } else {
                        expr_to_string(&item.expr)
                    }
                })
                .collect();

            // W2-12: aggregate items (count/sum/avg/min/max/collect)
            // switch the projection into grouped-aggregation mode.
            let aggregated = try_project_aggregate(items, st.rows.len(), |e, ri| {
                eval_expr(
                    e,
                    &st.rows[ri],
                    memgraph,
                    params,
                    csr_segs,
                    ctx.snapshot_lsn,
                    ctx.decay,
                )
            });

            let mut projected: Vec<Vec<Value>> = match aggregated {
                Some(agg_rows) => agg_rows,
                None => st
                    .rows
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
                                        memgraph,
                                        params,
                                        csr_segs,
                                        ctx.snapshot_lsn,
                                        ctx.decay,
                                    )
                                }
                            })
                            .collect()
                    })
                    .collect(),
            };

            if *distinct {
                dedup_rows(&mut projected);
            }

            if *rebind {
                // W2-13 WITH: re-seed the variable-binding row stream
                // with the projection outputs so later clauses (WHERE /
                // ORDER BY / MATCH / RETURN) keep executing. `st.columns`
                // stays set but the final RETURN overwrites it.
                let mut new_rows = Vec::with_capacity(projected.len());
                for vals in projected {
                    let mut new_row = Row::seed(slot_table);
                    for (name, val) in st.columns.iter().zip(vals) {
                        new_row.insert(name, val);
                    }
                    new_rows.push(new_row);
                }
                st.rows = new_rows;
                st.projected_rows = None;
            } else {
                st.projected_rows = Some(projected);
                st.rows.clear();
            }
        }

        PhysicalOp::Sort { items } => {
            // moon#1197: sort keys computed ONCE per row and compared by
            // reference (HEAD cloned both values — or evaluated both
            // expressions — inside every comparison), and only the rows the
            // rest of the plan can use (`keep`: ORDER BY … [SKIP s] LIMIT n)
            // are selected + sorted. `stable_order` reproduces HEAD's stable
            // `sort_by` + truncation exactly.
            let ascending: SmallVec<[bool; 4]> = items.iter().map(|(_, asc)| *asc).collect();
            if let Some(pr) = st.projected_rows.take() {
                // After projection, sort keys are positional columns.
                let col_indices: SmallVec<[Option<usize>; 4]> = items
                    .iter()
                    .map(|(expr, _)| {
                        let name = expr_to_string(expr);
                        st.columns.iter().position(|c| *c == name)
                    })
                    .collect();
                let null = Value::Null;
                let total = pipeline::totally_ordered(pr.iter().flat_map(|row| {
                    col_indices
                        .iter()
                        .map(|&c| pipeline::column_key(row, c, &null))
                }));
                let order = pipeline::stable_order(pr.len(), keep, total, |a, b| {
                    pipeline::cmp_keys(
                        col_indices
                            .iter()
                            .map(|&c| pipeline::column_key(&pr[a], c, &null)),
                        col_indices
                            .iter()
                            .map(|&c| pipeline::column_key(&pr[b], c, &null)),
                        &ascending,
                    )
                });
                st.projected_rows = Some(pipeline::permute(pr, &order));
            } else {
                let keys: Vec<SmallVec<[Value; 2]>> = st
                    .rows
                    .iter()
                    .map(|row| {
                        items
                            .iter()
                            .map(|(expr, _)| {
                                eval_expr(
                                    expr,
                                    row,
                                    memgraph,
                                    params,
                                    csr_segs,
                                    ctx.snapshot_lsn,
                                    ctx.decay,
                                )
                            })
                            .collect()
                    })
                    .collect();
                let total = pipeline::totally_ordered(keys.iter().flatten());
                let order = pipeline::stable_order(keys.len(), keep, total, |a, b| {
                    pipeline::cmp_keys(keys[a].iter(), keys[b].iter(), &ascending)
                });
                let rows = std::mem::take(&mut st.rows);
                st.rows = pipeline::permute(rows, &order);
            }
        }

        PhysicalOp::Limit { count } => {
            let n = match eval_expr(
                count,
                empty_row,
                memgraph,
                params,
                csr_segs,
                ctx.snapshot_lsn,
                ctx.decay,
            ) {
                Value::Int(n) if n >= 0 => n as usize,
                _ => 0,
            };
            if let Some(ref mut pr) = st.projected_rows {
                pr.truncate(n);
            } else {
                st.rows.truncate(n);
            }
        }

        PhysicalOp::Skip { count } => {
            let n = match eval_expr(
                count,
                empty_row,
                memgraph,
                params,
                csr_segs,
                ctx.snapshot_lsn,
                ctx.decay,
            ) {
                Value::Int(n) if n >= 0 => n as usize,
                _ => 0,
            };
            if let Some(ref mut pr) = st.projected_rows {
                if n < pr.len() {
                    *pr = pr.split_off(n);
                } else {
                    pr.clear();
                }
            } else if n < st.rows.len() {
                st.rows = st.rows.split_off(n);
            } else {
                st.rows.clear();
            }
        }

        PhysicalOp::Unwind { expr, alias } => {
            let mut new_rows = Vec::new();
            for row in &st.rows {
                let val = eval_expr(
                    expr,
                    row,
                    memgraph,
                    params,
                    csr_segs,
                    ctx.snapshot_lsn,
                    ctx.decay,
                );
                if let Value::List(items) = val {
                    for item in items {
                        let mut new_row = row.clone();
                        new_row.insert(alias, item);
                        new_rows.push(new_row);
                    }
                }
            }
            st.rows = new_rows;
        }

        PhysicalOp::CreatePattern { .. } => {
            return Err(ExecError {
                kind: ExecErrorKind::Unsupported(
                    "write operations require GRAPH.QUERY with write lock".into(),
                ),
                partial_mutations: Vec::new(),
            });
        }

        PhysicalOp::DeleteEntities { .. } => {
            return Err(ExecError {
                kind: ExecErrorKind::Unsupported(
                    "write operations require GRAPH.QUERY with write lock".into(),
                ),
                partial_mutations: Vec::new(),
            });
        }

        PhysicalOp::SetProperties { .. } => {
            return Err(ExecError {
                kind: ExecErrorKind::Unsupported(
                    "write operations require GRAPH.QUERY with write lock".into(),
                ),
                partial_mutations: Vec::new(),
            });
        }

        PhysicalOp::ProcedureCall { .. } => {
            return Err(ExecError {
                kind: ExecErrorKind::Unsupported(
                    "procedure calls not yet implemented in executor".into(),
                ),
                partial_mutations: Vec::new(),
            });
        }

        PhysicalOp::Merge { .. } => {
            return Err(ExecError {
                kind: ExecErrorKind::Unsupported(
                    "write operations require GRAPH.QUERY with write lock".into(),
                ),
                partial_mutations: Vec::new(),
            });
        }

        PhysicalOp::ShortestPath {
            path_var,
            source,
            target,
            max_hops,
            edge_types,
            direction,
        } => {
            // Phase 174 FIX-04: delegates to shared run_shortest_path helper.
            let mut new_rows = Vec::new();
            for row in &st.rows {
                let src_key = match row.get(source) {
                    Some(Value::Node(k)) => *k,
                    _ => continue,
                };
                let dst_key = match row.get(target) {
                    Some(Value::Node(k)) => *k,
                    _ => continue,
                };
                guard_check(ctx)?;
                if let Some(path) = super::shortest_path::run_shortest_path(
                    memgraph,
                    csr_segs,
                    ctx.snapshot_lsn,
                    ctx.decay,
                    src_key,
                    dst_key,
                    edge_types,
                    *direction,
                    *max_hops,
                ) {
                    let mut new_row = row.clone();
                    new_row.insert(path_var, Value::Path(path));
                    new_rows.push(new_row);
                }
            }
            st.rows = new_rows;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Profiling executor
// ---------------------------------------------------------------------------

/// Get a human-readable name for a physical operator.
pub(crate) fn op_name(op: &PhysicalOp) -> &'static str {
    match op {
        PhysicalOp::NodeScan { .. } => "NodeScan",
        PhysicalOp::IndexScan { .. } => "IndexScan",
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
        PhysicalOp::ShortestPath { .. } => "ShortestPath",
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
    ctx: &ExecutionContext,
) -> Result<ProfileResult, ExecError> {
    let start = std::time::Instant::now();

    let slot_table = SlotTable::from_plan(plan);
    let empty_table = SlotTable::default();
    let empty_row = Row::seed(&empty_table);
    let mut rows: Vec<Row> = vec![Row::seed(&slot_table)];
    let mut columns = Vec::new();
    let mut projected_rows: Option<Vec<Vec<Value>>> = None;
    let nodes_created: u64 = 0;
    let mut nodes_scanned: u64 = 0;
    let nodes_deleted: u64 = 0;
    let properties_set: u64 = 0;
    let mut profiles = Vec::with_capacity(plan.operators.len());

    let memgraph = &graph.write_buf;

    // Phase 174 FIX-04/05: load immutable CSR segments for ShortestPath
    // and eval_expr (expression-form shortestPath needs real segments).
    let segments_guard = graph.segments.load();
    let csr_segs = &segments_guard.immutable;

    for op in &plan.operators {
        let op_start = std::time::Instant::now();

        match op {
            PhysicalOp::NodeScan { variable, label } => {
                let label_id = label.as_ref().map(|l| label_to_id(l.as_bytes()));
                let committed = roaring::RoaringBitmap::new();
                // Merged-tier scan (parity with the main executor).
                let view = crate::graph::view::MergedNodeView::new(memgraph, csr_segs);
                let mut keys = Vec::new();
                view.for_each_visible_node(
                    label_id,
                    ctx.snapshot_lsn,
                    ctx.my_txn_id,
                    &committed,
                    ctx.valid_time_as_of,
                    |k| keys.push(k),
                );
                nodes_scanned += keys.len() as u64;
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

            PhysicalOp::IndexScan {
                variable,
                label,
                prop_eq,
                prop_range,
                text_pred,
            } => {
                let keys = index_scan_keys(
                    memgraph,
                    csr_segs,
                    label.as_ref(),
                    prop_eq,
                    prop_range,
                    text_pred,
                    params,
                    ctx,
                );
                nodes_scanned += keys.len() as u64;
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
                optional,
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

                // Cross-segment expansion (parity with the main executor —
                // memgraph-only neighbors miss every frozen CSR edge).
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

                let committed = roaring::RoaringBitmap::new();
                let view = crate::graph::view::MergedNodeView::new(memgraph, csr_segs);
                // Scratch reused across every neighbor lookup in this Expand
                // (the allocating `neighbors()` built a HashSet+Vec per call).
                let mut nb_seen = crate::graph::fasthash::FxHashSet::default();
                let mut nb_buf: Vec<crate::graph::traversal::MergedNeighbor> = Vec::new();
                let mut new_rows = Vec::new();
                for row in &rows {
                    let src_key = match row.get(source) {
                        Some(Value::Node(k)) => *k,
                        _ => {
                            // W2-13 OPTIONAL MATCH (parity with main executor).
                            if *optional {
                                push_null_padded(row, target, edge_variable, &mut new_rows);
                            }
                            continue;
                        }
                    };
                    let row_start = new_rows.len();

                    if *max_hops <= 1 {
                        reader.neighbors_into(src_key, &mut nb_seen, &mut nb_buf);
                        for merged in &nb_buf {
                            if type_ids.len() > 1 && !type_ids.contains(&merged.edge_type) {
                                continue;
                            }
                            // Bi-temporal visibility check on target node
                            // (merged view — parity with main executor).
                            if !view.is_visible(
                                merged.node,
                                ctx.snapshot_lsn,
                                ctx.my_txn_id,
                                &committed,
                                ctx.valid_time_as_of,
                            ) {
                                continue;
                            }
                            let mut new_row = row.clone();
                            new_row.insert(target, Value::Node(merged.node));
                            // v0.1.9 CYP-06: bind edge variable in execute_profile
                            // single-hop path (parity with main executor).
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
                        let mut visited = crate::graph::fasthash::FxHashSet::default();
                        visited.insert(src_key);

                        for hop in 1..=capped_max_hops {
                            guard_check(ctx)?;
                            let mut next_frontier = Vec::new();
                            for &current in &frontier {
                                reader.neighbors_into(current, &mut nb_seen, &mut nb_buf);
                                for merged in &nb_buf {
                                    if visited.contains(&merged.node) {
                                        continue;
                                    }
                                    if type_ids.len() > 1 && !type_ids.contains(&merged.edge_type) {
                                        continue;
                                    }
                                    visited.insert(merged.node);
                                    next_frontier.push(merged.node);

                                    if hop >= *min_hops {
                                        let mut new_row = row.clone();
                                        new_row.insert(target, Value::Node(merged.node));
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

                    if *optional && new_rows.len() == row_start {
                        push_null_padded(row, target, edge_variable, &mut new_rows);
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
                            memgraph,
                            params,
                            csr_segs,
                            ctx.snapshot_lsn,
                            ctx.decay
                        ),
                        Value::Bool(true)
                    )
                });
            }

            PhysicalOp::Project {
                items,
                distinct,
                rebind,
            } => {
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

                // W2-12: aggregate items switch into grouped-aggregation
                // mode (shared with the non-profile executor).
                let aggregated = try_project_aggregate(items, rows.len(), |e, ri| {
                    eval_expr(
                        e,
                        &rows[ri],
                        memgraph,
                        params,
                        csr_segs,
                        ctx.snapshot_lsn,
                        ctx.decay,
                    )
                });

                let mut projected: Vec<Vec<Value>> = match aggregated {
                    Some(agg_rows) => agg_rows,
                    None => rows
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
                                            memgraph,
                                            params,
                                            csr_segs,
                                            ctx.snapshot_lsn,
                                            ctx.decay,
                                        )
                                    }
                                })
                                .collect()
                        })
                        .collect(),
                };

                if *distinct {
                    dedup_rows(&mut projected);
                }

                if *rebind {
                    // W2-13 WITH rebind (parity with main executor).
                    let mut new_rows = Vec::with_capacity(projected.len());
                    for vals in projected {
                        let mut new_row = Row::seed(&slot_table);
                        for (name, val) in columns.iter().zip(vals) {
                            new_row.insert(name, val);
                        }
                        new_rows.push(new_row);
                    }
                    rows = new_rows;
                    projected_rows = None;
                } else {
                    projected_rows = Some(projected);
                    rows.clear();
                }
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
                                memgraph,
                                params,
                                csr_segs,
                                ctx.snapshot_lsn,
                                ctx.decay,
                            );
                            let vb = eval_expr(
                                expr,
                                b,
                                memgraph,
                                params,
                                csr_segs,
                                ctx.snapshot_lsn,
                                ctx.decay,
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
                    memgraph,
                    params,
                    csr_segs,
                    ctx.snapshot_lsn,
                    ctx.decay,
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
                    memgraph,
                    params,
                    csr_segs,
                    ctx.snapshot_lsn,
                    ctx.decay,
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
                        memgraph,
                        params,
                        csr_segs,
                        ctx.snapshot_lsn,
                        ctx.decay,
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

            PhysicalOp::CreatePattern { .. } => {
                return Err(ExecError {
                    kind: ExecErrorKind::Unsupported(
                        "write operations require GRAPH.QUERY with write lock".into(),
                    ),
                    partial_mutations: Vec::new(),
                });
            }

            PhysicalOp::DeleteEntities { .. } => {
                return Err(ExecError {
                    kind: ExecErrorKind::Unsupported(
                        "write operations require GRAPH.QUERY with write lock".into(),
                    ),
                    partial_mutations: Vec::new(),
                });
            }

            PhysicalOp::SetProperties { .. } => {
                return Err(ExecError {
                    kind: ExecErrorKind::Unsupported(
                        "write operations require GRAPH.QUERY with write lock".into(),
                    ),
                    partial_mutations: Vec::new(),
                });
            }

            PhysicalOp::ProcedureCall { .. } => {
                return Err(ExecError {
                    kind: ExecErrorKind::Unsupported(
                        "procedure calls not yet implemented in executor".into(),
                    ),
                    partial_mutations: Vec::new(),
                });
            }

            PhysicalOp::Merge { .. } => {
                return Err(ExecError {
                    kind: ExecErrorKind::Unsupported(
                        "write operations require GRAPH.QUERY with write lock".into(),
                    ),
                    partial_mutations: Vec::new(),
                });
            }

            PhysicalOp::ShortestPath {
                path_var,
                source,
                target,
                max_hops,
                edge_types,
                direction,
            } => {
                // Phase 174 FIX-04/05: delegates to shared run_shortest_path
                // helper with real csr_segs (fixes the empty_segs bug in PROFILE).
                let mut new_rows = Vec::new();
                for row in &rows {
                    let src_key = match row.get(source) {
                        Some(Value::Node(k)) => *k,
                        _ => continue,
                    };
                    let dst_key = match row.get(target) {
                        Some(Value::Node(k)) => *k,
                        _ => continue,
                    };
                    guard_check(ctx)?;
                    if let Some(path) = super::shortest_path::run_shortest_path(
                        memgraph,
                        csr_segs,
                        ctx.snapshot_lsn,
                        ctx.decay,
                        src_key,
                        dst_key,
                        edge_types,
                        *direction,
                        *max_hops,
                    ) {
                        let mut new_row = row.clone();
                        new_row.insert(path_var, Value::Path(path));
                        new_rows.push(new_row);
                    }
                }
                rows = new_rows;
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
    Ok(ProfileResult {
        exec_result: ExecResult {
            columns,
            rows: final_rows,
            nodes_created,
            nodes_deleted,
            properties_set,
            nodes_scanned,
            execution_time_us: elapsed,
            mutations: Vec::new(),
        },
        operator_profiles: profiles,
    })
}
