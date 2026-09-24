//! `IndexScan` key resolution across the mutable and frozen tiers (moved out of `read.rs`).
//!
//! moon#1220: the scan is a visitor ([`index_scan_try_for_each`]) so a LIMIT-bounded plan can
//! consume it in chunks and stop early, like the label scan (moon#1197); [`index_scan_keys`]
//! collects it for the operators that need every key. Same keys, same order either way.

use std::collections::HashMap;
use std::ops::ControlFlow;

use super::*;

/// Every key [`index_scan_try_for_each`] visits, in visit order.
#[allow(clippy::too_many_arguments)]
pub(in crate::graph::cypher::executor) fn index_scan_keys(
    memgraph: &crate::graph::memgraph::MemGraph,
    csr_segs: &[std::sync::Arc<crate::graph::csr::CsrStorage>],
    label: Option<&String>,
    prop_eq: &[(String, Expr)],
    prop_range: &[(String, RangeCmp, Expr)],
    text_pred: &[(String, BinaryOperator, Expr)],
    params: &HashMap<String, Value>,
    ctx: &ExecutionContext,
) -> Vec<NodeKey> {
    let mut keys = Vec::new();
    let _ = index_scan_try_for_each::<()>(
        memgraph,
        csr_segs,
        label,
        prop_eq,
        prop_range,
        text_pred,
        params,
        ctx,
        |k| {
            keys.push(k);
            ControlFlow::Continue(())
        },
    );
    keys
}

/// Resolve an `IndexScan` into the matching node keys across both tiers, visiting each key
/// in order; stops as soon as `f` breaks (and returns the break).
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
pub(in crate::graph::cypher::executor) fn index_scan_try_for_each<B>(
    memgraph: &crate::graph::memgraph::MemGraph,
    csr_segs: &[std::sync::Arc<crate::graph::csr::CsrStorage>],
    label: Option<&String>,
    prop_eq: &[(String, Expr)],
    prop_range: &[(String, RangeCmp, Expr)],
    text_pred: &[(String, BinaryOperator, Expr)],
    params: &HashMap<String, Value>,
    ctx: &ExecutionContext,
    mut f: impl FnMut(NodeKey) -> ControlFlow<B>,
) -> ControlFlow<B> {
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
                return view.try_for_each_visible_node(
                    label_id,
                    ctx.snapshot_lsn,
                    ctx.my_txn_id,
                    &committed,
                    ctx.valid_time_as_of,
                    &mut f,
                );
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
        return view.try_for_each_visible_node(
            label_id,
            ctx.snapshot_lsn,
            ctx.my_txn_id,
            &committed,
            ctx.valid_time_as_of,
            &mut f,
        );
    }

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
            f(key)?;
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
            f(key)?;
        }
    }

    ControlFlow::Continue(())
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
