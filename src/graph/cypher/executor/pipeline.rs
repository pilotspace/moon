//! Row-demand analysis, LIMIT-aware streaming and top-k sorting for the read
//! executor (moon#1197).
//!
//! The executor is operator-at-a-time: every operator transforms the whole
//! row vector. HEAD therefore materialised (scan, clone, project) every row of
//! a label before `LIMIT` truncated it, and `ORDER BY` fully sorted with two
//! `eval_expr` / two `Value` clones per comparison. This module supplies:
//!
//! * [`row_demand`] — per operator, how many of its output rows the rest of the
//!   plan can possibly use (`LIMIT n` → n, `SKIP s` → +s, a 1:1 projection
//!   passes the demand through; anything that needs all rows — ORDER BY,
//!   aggregation, DISTINCT, row-multiplying ops — is unbounded).
//! * [`streamable_prefix`] — the leading `scan → (Filter | Expand | Unwind)* →
//!   [1:1 Project]` run whose output demand is bounded: the executor feeds scan
//!   keys through it in chunks and stops once enough rows exist. Every op in
//!   the run maps each input row to an ordered run of output rows independently
//!   of the others, so the chunked output is exactly a prefix of the full
//!   output.
//! * [`stable_order`] — the stable sort permutation, truncated to the demand
//!   with a selection when the comparator is a total preorder (no NaN, no
//!   precision-losing int/float mix) and a plain stable sort otherwise, so the
//!   result is always HEAD's `sort_by` + `truncate`.

use std::cmp::Ordering;

use super::{Expr, PhysicalOp, ReturnItem, Value};

/// Demand beyond which the chunked scan is not used: a variable-length
/// `Expand` caps its output at this many rows per invocation, so only a
/// demand within the cap keeps the chunked output a prefix of the full one.
pub(super) const STREAM_MAX_ROWS: usize = 100_000;

/// Whether a projection aggregates (then it needs every input row).
pub(super) fn has_aggregate(items: &[ReturnItem]) -> bool {
    items
        .iter()
        .any(|it| super::aggregate_call(&it.expr).is_some())
}

/// `demand[i]` = the most output rows of `ops[i]` the rest of the plan can use
/// (`usize::MAX` = all). `count_of` evaluates a SKIP/LIMIT count exactly as the
/// executor does.
pub(super) fn row_demand(
    ops: &[PhysicalOp],
    mut count_of: impl FnMut(&Expr) -> usize,
) -> Vec<usize> {
    let mut demand = vec![usize::MAX; ops.len()];
    let mut after = usize::MAX;
    for i in (0..ops.len()).rev() {
        demand[i] = after;
        // Demand on op i's INPUT.
        after = match &ops[i] {
            PhysicalOp::Limit { count } => after.min(count_of(count)),
            PhysicalOp::Skip { count } => after.saturating_add(count_of(count)),
            PhysicalOp::Project {
                items,
                distinct: false,
                ..
            } if !has_aggregate(items) => after,
            _ => usize::MAX,
        };
    }
    demand
}

/// End (exclusive) of the leading streamable run, when its output demand is
/// bounded: `ops[0]` is a `NodeScan`/`IndexScan`, followed by row-local,
/// order-preserving `Filter` / `Expand` / `Unwind` ops and at most one 1:1
/// `Project` (which ends the run).
pub(super) fn streamable_prefix(ops: &[PhysicalOp], demand: &[usize]) -> Option<usize> {
    if !matches!(
        ops.first()?,
        PhysicalOp::NodeScan { .. } | PhysicalOp::IndexScan { .. }
    ) {
        return None;
    }
    let mut end = 1;
    while let Some(op) = ops.get(end) {
        match op {
            PhysicalOp::Filter { .. } | PhysicalOp::Expand { .. } | PhysicalOp::Unwind { .. } => {
                end += 1
            }
            PhysicalOp::Project {
                items,
                distinct: false,
                ..
            } if !has_aggregate(items) => {
                end += 1;
                break;
            }
            _ => break,
        }
    }
    let need = *demand.get(end - 1)?;
    (need <= STREAM_MAX_ROWS).then_some(end)
}

/// `true` when `compare_values` is a total preorder over `vals`: no NaN, and
/// no Int beyond ±2^53 next to a Float (the `as f64` promotion would make the
/// order intransitive). Then "stable sort + truncate" equals a selection.
pub(super) fn totally_ordered<'v>(vals: impl Iterator<Item = &'v Value>) -> bool {
    const EXACT: i64 = 1 << 53;
    let (mut float, mut big_int) = (false, false);
    for v in vals {
        match v {
            Value::Float(f) if f.is_nan() => return false,
            Value::Float(_) => float = true,
            Value::Int(i) if !(-EXACT..=EXACT).contains(i) => big_int = true,
            _ => {}
        }
        if float && big_int {
            return false;
        }
    }
    true
}

/// The permutation HEAD's stable `sort_by(cmp)` applies to `n` rows, cut to
/// the first `keep`. With a total preorder and `keep < n` it selects the `keep`
/// smallest by `(cmp, original index)` and sorts only those — the same rows in
/// the same order as the stable sort's prefix; otherwise it is the stable sort
/// itself (identical comparisons on identical values, so identical output even
/// for a comparator that is not a total order).
pub(super) fn stable_order(
    n: usize,
    keep: usize,
    total: bool,
    cmp: impl Fn(usize, usize) -> Ordering,
) -> Vec<usize> {
    let mut idx: Vec<usize> = (0..n).collect();
    if total && keep < n {
        let key = |a: &usize, b: &usize| cmp(*a, *b).then(a.cmp(b));
        if keep > 0 {
            idx.select_nth_unstable_by(keep - 1, key);
        }
        idx.truncate(keep);
        idx.sort_unstable_by(key);
    } else {
        idx.sort_by(|a, b| cmp(*a, *b));
        idx.truncate(keep);
    }
    idx
}

/// Reorder `rows` by `order` (indices into `rows`, each at most once).
pub(super) fn permute<T>(rows: Vec<T>, order: &[usize]) -> Vec<T> {
    let mut slots: Vec<Option<T>> = rows.into_iter().map(Some).collect();
    order
        .iter()
        .filter_map(|&i| slots.get_mut(i).and_then(Option::take))
        .collect()
}

/// A post-projection sort key: the projected column (`None` = the ORDER BY
/// expression is not a projected column, which HEAD read as Null).
#[inline]
pub(super) fn column_key<'r>(row: &'r [Value], col: Option<usize>, null: &'r Value) -> &'r Value {
    col.and_then(|c| row.get(c)).unwrap_or(null)
}

/// Compare two precomputed sort keys item by item (`ascending[i]` per item).
pub(super) fn cmp_keys<'v>(
    a: impl Iterator<Item = &'v Value>,
    b: impl Iterator<Item = &'v Value>,
    ascending: &[bool],
) -> Ordering {
    for ((va, vb), asc) in a.zip(b).zip(ascending) {
        let ord = super::compare_values(va, vb);
        let ord = if *asc { ord } else { ord.reverse() };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_order_matches_sort_then_truncate() {
        // Distinct values with deliberate ties (stability is observable).
        let vals = [5i64, 1, 5, 3, 1, 9, 0, 3, 5, -2, 7, 1];
        let cmp = |a: usize, b: usize| vals[a].cmp(&vals[b]);
        let mut full: Vec<usize> = (0..vals.len()).collect();
        full.sort_by(|a, b| cmp(*a, *b));
        for keep in 0..=vals.len() + 1 {
            for total in [true, false] {
                let got = stable_order(vals.len(), keep, total, cmp);
                let want: Vec<usize> = full.iter().copied().take(keep).collect();
                assert_eq!(got, want, "keep={keep} total={total}");
            }
        }
    }

    #[test]
    fn total_order_detection() {
        let ok = [Value::Int(3), Value::Float(2.5), Value::Null];
        assert!(totally_ordered(ok.iter()));
        let nan = [Value::Float(f64::NAN)];
        assert!(!totally_ordered(nan.iter()));
        let mixed = [Value::Int(1 << 60), Value::Float(1.0)];
        assert!(!totally_ordered(mixed.iter()));
        let big_only = [Value::Int(1 << 60), Value::Int(3)];
        assert!(totally_ordered(big_only.iter()));
    }

    #[test]
    fn permute_reorders() {
        assert_eq!(permute(vec!['a', 'b', 'c'], &[2, 0]), vec!['c', 'a']);
    }
}
