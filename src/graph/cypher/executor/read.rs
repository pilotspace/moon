use std::collections::HashMap;

use super::*;

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

/// Resolve an `IndexScan` into the matching node keys across both tiers.
///
/// Frozen tier: per segment, intersect the property-equality bitmaps
/// (`SegmentPropertyIndexes::rows_eq`) with the label bitmap, then apply
/// MVCC/valid-time visibility per surviving row. Mutable tail: linear scan
/// with an inline (numeric-coercing, superset-consistent) property check —
/// the tail is bounded by `edge_threshold` by design.
///
/// `prop_eq` values are literals or parameters; if any resolves to a
/// non-scalar the whole scan degrades to the plain merged label scan (the
/// residual Filter downstream keeps results exact either way).
///
/// `prop_range` conjuncts (`n.p > $x` etc.) prune via the per-segment
/// numeric B-trees. A conjunct whose threshold resolves non-numeric is
/// dropped (can't prune the numeric space) — never narrowed: the pruned set
/// stays a SUPERSET of the rows the residual Filter accepts, because the
/// post-W2-3 comparison semantics make cross-type / missing-property
/// comparisons evaluate to Null (dropped by WHERE), exactly the rows the
/// numeric index excludes.
///
/// `text_pred` conjuncts (`n.p CONTAINS 'x'`, `STARTS WITH`, `ENDS WITH`,
/// `=~`; P3 design part B) prune the FROZEN tier only, via
/// `SegmentTextIndex::candidate_rows` — a PRESENCE-only superset (rows
/// whose value at that property is a String/Bytes at all; see
/// `text_index.rs` module docs for why token-level pruning is unsound for
/// substring/prefix/suffix predicates). The MUTABLE tier has no text index
/// (pre-approved scope decision): a text-only conjunct (no `prop_eq`/
/// `prop_range` alongside it) falls back to an exact full scan of the
/// mutable tail, seeded from `memgraph.iter_nodes()` — correct, just
/// unaccelerated, matching the mutable tier's existing story for numeric
/// properties before freeze.
#[allow(clippy::too_many_arguments)]
pub(super) fn index_scan_keys(
    memgraph: &crate::graph::memgraph::MemGraph,
    csr_segs: &[std::sync::Arc<crate::graph::csr::CsrStorage>],
    label: Option<&String>,
    prop_eq: &[(String, Expr)],
    prop_range: &[(String, RangeCmp, Expr)],
    text_pred: &[(String, BinaryOperator, Expr)],
    params: &HashMap<String, Value>,
    ctx: &ExecutionContext,
) -> Vec<NodeKey> {
    let label_id = label.map(|l| label_to_id(l.as_bytes()));
    let committed = roaring::RoaringBitmap::new();
    let view = crate::graph::view::MergedNodeView::new(memgraph, csr_segs);
    let empty_table = SlotTable::default();
    let empty_row = Row::seed(&empty_table);

    // Resolve each equality target to a concrete PropertyValue.
    let mut targets: Vec<(u16, PropertyValue)> = Vec::with_capacity(prop_eq.len());
    for (name, expr) in prop_eq {
        let v = eval_expr(
            expr,
            &empty_row,
            memgraph,
            params,
            csr_segs,
            ctx.snapshot_lsn,
            ctx.decay,
        );
        match value_to_property_value(&v) {
            Some(pv) => targets.push((label_to_id(name.as_bytes()), pv)),
            None => {
                // Unresolvable target (e.g. Null parameter): fall back to
                // the merged label scan; the residual Filter stays exact.
                let mut keys = Vec::new();
                view.for_each_visible_node(
                    label_id,
                    ctx.snapshot_lsn,
                    ctx.my_txn_id,
                    &committed,
                    ctx.valid_time_as_of,
                    |k| keys.push(k),
                );
                return keys;
            }
        }
    }

    // Resolve each range conjunct to (prop_id, cmp, numeric threshold). A
    // threshold that resolves non-numeric (e.g. a String parameter) cannot
    // prune the numeric index — drop the conjunct (superset-safe); the
    // residual Filter stays exact.
    let mut ranges: Vec<(u16, RangeCmp, f64)> = Vec::with_capacity(prop_range.len());
    for (name, cmp, expr) in prop_range {
        let v = eval_expr(
            expr,
            &empty_row,
            memgraph,
            params,
            csr_segs,
            ctx.snapshot_lsn,
            ctx.decay,
        );
        match v {
            Value::Int(i) => ranges.push((label_to_id(name.as_bytes()), *cmp, i as f64)),
            Value::Float(f) => ranges.push((label_to_id(name.as_bytes()), *cmp, f)),
            _ => {}
        }
    }

    // Resolve each text conjunct to a bare prop_id (deduplicated). No value
    // is evaluated: `SegmentTextIndex::candidate_rows` prunes on PRESENCE
    // alone (see its module docs) -- valid as a superset regardless of the
    // pattern's content, so the pattern expression is never consulted here.
    let mut text_prop_ids: Vec<u16> = Vec::with_capacity(text_pred.len());
    for (name, _op, _expr) in text_pred {
        let pid = label_to_id(name.as_bytes());
        if !text_prop_ids.contains(&pid) {
            text_prop_ids.push(pid);
        }
    }

    // Nothing prunable (pure-range scan whose thresholds all resolved
    // non-numeric, and no text conjunct either): full merged label scan,
    // residual Filter stays exact.
    if targets.is_empty() && ranges.is_empty() && text_prop_ids.is_empty() {
        let mut keys = Vec::new();
        view.for_each_visible_node(
            label_id,
            ctx.snapshot_lsn,
            ctx.my_txn_id,
            &committed,
            ctx.valid_time_as_of,
            |k| keys.push(k),
        );
        return keys;
    }

    let mut keys = Vec::new();

    // Mutable tail: seed a small candidate set from the mutable-tier
    // property index (Task #31) instead of scanning every live node, then
    // apply the SAME superset-consistent label/visibility/property checks
    // as before to that candidate set. SUPERSET contract preserved: the
    // index only prunes candidates, never decides — these checks (and the
    // planner's residual Filter downstream) stay authoritative.
    //
    // Seed from the first equality target when available (exact bucket,
    // typically 0-1 entries for a unique id); otherwise from the full
    // indexed value range of the first range conjunct (conservative — the
    // `ranges_match` check below narrows it exactly). One of the two is
    // always present here: `targets.is_empty() && ranges.is_empty()` was
    // already handled by the early return above.
    let candidates: Vec<NodeKey> = if let Some((pid, want)) = targets.first() {
        memgraph.prop_index_keys_eq(*pid, want).to_vec()
    } else if let Some((pid, cmp, threshold)) = ranges.first() {
        // Boundary-INCLUSIVE on the threshold side regardless of Gt/Lt vs
        // Gte/Lte — a superset seed; the exact `ranges_match` check below
        // enforces strictness.
        let (lo, hi) = match cmp {
            RangeCmp::Gt | RangeCmp::Gte => (*threshold, f64::INFINITY),
            RangeCmp::Lt | RangeCmp::Lte => (f64::NEG_INFINITY, *threshold),
        };
        memgraph.prop_index_keys_range(*pid, lo, hi)
    } else if !text_prop_ids.is_empty() {
        // Text-only conjunct(s): the mutable tier has no text index
        // (pre-approved scope decision, see `text_index.rs` module docs) --
        // fall back to a full label-scoped scan of the mutable tail. Still
        // bounded by `edge_threshold` (freeze keeps the mutable tail
        // small), and the generic label/visibility checks below plus the
        // planner's residual Filter downstream stay authoritative, exactly
        // like every other candidate source in this function.
        memgraph.iter_nodes().map(|(key, _)| key).collect()
    } else {
        // Structurally unreachable (see above) — an empty candidate set is
        // the safe degradation if this invariant is ever violated by a
        // future refactor, never a panic on live traffic.
        Vec::new()
    };

    for key in candidates {
        let Some(node) = memgraph.get_node(key) else {
            continue; // tombstone/race guard: index entry outlived the node
        };
        if let Some(lid) = label_id {
            if !node.labels.contains(&lid) {
                continue;
            }
        }
        if !crate::graph::visibility::is_node_visible(
            node,
            ctx.snapshot_lsn,
            ctx.my_txn_id,
            &committed,
            ctx.valid_time_as_of,
        ) {
            continue;
        }
        let all_match = targets.iter().all(|(pid, want)| {
            node.properties
                .iter()
                .any(|(id, have)| id == pid && prop_value_loose_eq(have, want))
        });
        // Range check mirrors the index's numeric normalization (Int/Float/
        // Bool as 0-1 in one f64 space) so both tiers prune identically.
        let ranges_match = ranges.iter().all(|(pid, cmp, threshold)| {
            node.properties.iter().any(|(id, have)| {
                if id != pid {
                    return false;
                }
                let num = match have {
                    PropertyValue::Int(i) => *i as f64,
                    PropertyValue::Float(f) => *f,
                    PropertyValue::Bool(b) => f64::from(u8::from(*b)),
                    _ => return false,
                };
                range_cmp_holds(num, *cmp, *threshold)
            })
        });
        if all_match && ranges_match {
            keys.push(key);
        }
    }

    // Frozen tier: bitmap intersection per segment. `emitted` dedups keys a
    // re-frozen copy-up shadow left in multiple segments (stale-index hits
    // are dropped by the planner's residual Filter downstream).
    let mut emitted = crate::graph::fasthash::FxHashSet::default();
    for seg in csr_segs {
        let mut bm: Option<roaring::RoaringBitmap> = None;
        for (pid, pv) in &targets {
            let rows = seg.property_index().rows_eq(*pid, pv);
            let acc = match bm.take() {
                Some(acc) => acc & rows,
                None => rows,
            };
            if acc.is_empty() {
                bm = Some(acc);
                break;
            }
            bm = Some(acc);
        }
        for (pid, cmp, threshold) in &ranges {
            if bm.as_ref().is_some_and(roaring::RoaringBitmap::is_empty) {
                break;
            }
            let rows = match seg.property_index().numeric_index(*pid) {
                Some(ix) => match cmp {
                    RangeCmp::Gt => ix.gt(*threshold),
                    RangeCmp::Gte => ix.gte(*threshold),
                    RangeCmp::Lt => ix.lt(*threshold),
                    RangeCmp::Lte => ix.lte(*threshold),
                },
                // No numeric value ever indexed under this prop in this
                // segment -> no row here can pass the residual comparison.
                None => roaring::RoaringBitmap::new(),
            };
            let acc = match bm.take() {
                Some(acc) => acc & rows,
                None => rows,
            };
            bm = Some(acc);
        }
        for pid in &text_prop_ids {
            if bm.as_ref().is_some_and(roaring::RoaringBitmap::is_empty) {
                break;
            }
            let rows = seg
                .text_index()
                .candidate_rows(*pid)
                .cloned()
                .unwrap_or_default();
            let acc = match bm.take() {
                Some(acc) => acc & rows,
                None => rows,
            };
            bm = Some(acc);
        }
        let Some(mut bm) = bm else { continue };
        if bm.is_empty() {
            continue;
        }
        if let Some(lid) = label_id {
            match seg.label_index().nodes_with_label(lid) {
                Some(lbm) => bm &= lbm,
                None => continue,
            }
        }
        let metas = seg.node_meta();
        for row in bm {
            let Some(meta) = metas.get(row as usize) else {
                continue;
            };
            if !crate::graph::visibility::is_meta_visible(
                meta,
                ctx.snapshot_lsn,
                ctx.my_txn_id,
                &committed,
                ctx.valid_time_as_of,
            ) {
                continue;
            }
            let key = NodeKey::from(slotmap::KeyData::from_ffi(meta.external_id));
            // Copy-up shadow (W2-2): the mutable tier overrides this row —
            // a live shadow was already scanned above (with its CURRENT
            // property values); a dead shadow is a tombstone.
            if memgraph.get_node(key).is_some() {
                continue;
            }
            if !emitted.insert(key) {
                continue;
            }
            keys.push(key);
        }
    }

    keys
}

/// Numeric range comparison for the mutable-tail index check.
fn range_cmp_holds(value: f64, cmp: RangeCmp, threshold: f64) -> bool {
    match cmp {
        RangeCmp::Gt => value > threshold,
        RangeCmp::Gte => value >= threshold,
        RangeCmp::Lt => value < threshold,
        RangeCmp::Lte => value <= threshold,
    }
}

/// Superset-consistent equality between stored and queried property values,
/// mirroring the index's numeric normalization (Int/Float/Bool share one
/// f64 space; String/Bytes compare bytewise). Never narrower than the
/// residual Filter's semantics — a loose match only ADDS candidates.
fn prop_value_loose_eq(have: &PropertyValue, want: &PropertyValue) -> bool {
    fn as_num(v: &PropertyValue) -> Option<f64> {
        match v {
            PropertyValue::Int(i) => Some(*i as f64),
            PropertyValue::Float(f) => Some(*f),
            PropertyValue::Bool(b) => Some(u8::from(*b) as f64),
            _ => None,
        }
    }
    fn as_bytes(v: &PropertyValue) -> Option<&[u8]> {
        match v {
            PropertyValue::String(s) | PropertyValue::Bytes(s) => Some(s.as_ref()),
            _ => None,
        }
    }
    match (as_num(have), as_num(want)) {
        (Some(a), Some(b)) => a == b,
        _ => matches!((as_bytes(have), as_bytes(want)), (Some(a), Some(b)) if a == b),
    }
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
    let mut rows: Vec<Row> = vec![Row::seed(slot_table)];
    let mut columns = Vec::new();
    // After Project, rows are converted to positional arrays.
    let mut projected_rows: Option<Vec<Vec<Value>>> = None;
    let nodes_created: u64 = 0;
    let mut nodes_scanned: u64 = 0;
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

                    // W2-13: zero matches under OPTIONAL MATCH → the source
                    // row survives with target/edge bound to Null.
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

                // W2-12: aggregate items (count/sum/avg/min/max/collect)
                // switch the projection into grouped-aggregation mode.
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
                    // W2-13 WITH: re-seed the variable-binding row stream
                    // with the projection outputs so later clauses (WHERE /
                    // ORDER BY / MATCH / RETURN) keep executing. `columns`
                    // stays set but the final RETURN overwrites it.
                    let mut new_rows = Vec::with_capacity(projected.len());
                    for vals in projected {
                        let mut new_row = Row::seed(slot_table);
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
                // Phase 174 FIX-04: delegates to shared run_shortest_path helper.
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
    }

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
