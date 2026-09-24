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
//!   the run maps each input row to an ordered run of output rows that depends
//!   only on that row — except the variable-length `Expand`, whose 100K output
//!   cap counts rows across its whole input; the sink carries each op's emitted
//!   count from chunk to chunk (moon#1221 review), so the chunked output is
//!   exactly a prefix of the full output and the cap still bounds the work.
//! * [`stable_order`] — the stable sort permutation, truncated to the demand
//!   with a selection when the comparator is a total preorder (no NaN, no
//!   precision-losing int/float mix) and a plain stable sort otherwise, so the
//!   result is always HEAD's `sort_by` + `truncate`.

use std::cmp::Ordering;

use super::*;

/// Demand beyond which the chunked scan is not used (the moon#1197 bound, equal
/// to the variable-length `Expand` output cap). Since that cap is carried across
/// chunks, the prefix property no longer depends on it; it only keeps a huge
/// `LIMIT` on the one-shot path.
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

/// Read-only inputs every operator evaluates against.
pub(super) struct OpEnv<'a, 't> {
    pub(super) memgraph: &'a crate::graph::memgraph::MemGraph,
    pub(super) csr_segs: &'a [std::sync::Arc<crate::graph::csr::CsrStorage>],
    pub(super) params: &'a HashMap<String, Value>,
    pub(super) ctx: &'a ExecutionContext,
    pub(super) slot_table: &'t SlotTable,
    pub(super) empty_row: &'a Row<'a>,
}

impl OpEnv<'_, '_> {
    /// A SKIP / LIMIT count, evaluated exactly as those operators do.
    pub(super) fn count(&self, count: &Expr) -> usize {
        match eval_expr(
            count,
            self.empty_row,
            self.memgraph,
            self.params,
            self.csr_segs,
            self.ctx.snapshot_lsn,
            self.ctx.decay,
        ) {
            Value::Int(n) if n >= 0 => n as usize,
            _ => 0,
        }
    }
}

/// The row stream between operators.
pub(super) struct OpState<'t> {
    pub(super) rows: Vec<Row<'t>>,
    /// After a non-rebinding Project, rows are positional value arrays.
    pub(super) projected_rows: Option<Vec<Vec<Value>>>,
    pub(super) columns: Vec<String>,
    pub(super) nodes_scanned: u64,
}

/// Run the leading streamable run `prefix` (`prefix[0]` is the scan) in
/// chunks of scan keys, stopping once `need` output rows exist (moon#1197).
/// Every op after the scan is row-local and order-preserving, so the rows
/// produced are exactly the first rows full evaluation would produce; the
/// downstream SKIP/LIMIT then cut them identically.
pub(super) fn run_streamed_prefix<'t>(
    prefix: &[PhysicalOp],
    need: usize,
    st: &mut OpState<'t>,
    env: &OpEnv<'_, 't>,
) -> Result<(), ExecError> {
    let Some((scan, segment)) = prefix.split_first() else {
        return Ok(());
    };
    let mut sink = StreamSink::new(need, segment.len());
    match scan {
        PhysicalOp::NodeScan { variable, label } => {
            let label_id = label.as_ref().map(|l| label_to_id(l.as_bytes()));
            let committed = roaring::RoaringBitmap::new();
            let view = crate::graph::view::MergedNodeView::new(env.memgraph, env.csr_segs);
            let mut chunk: Vec<NodeKey> = Vec::with_capacity(sink.chunk_len());
            let mut failed: Option<ExecError> = None;
            let _ = view.try_for_each_visible_node(
                label_id,
                env.ctx.snapshot_lsn,
                env.ctx.my_txn_id,
                &committed,
                env.ctx.valid_time_as_of,
                |key| {
                    chunk.push(key);
                    if chunk.len() < sink.chunk_len() {
                        return std::ops::ControlFlow::Continue(());
                    }
                    match sink.feed(&chunk, variable, segment, env) {
                        Ok(false) => {
                            chunk.clear();
                            std::ops::ControlFlow::Continue(())
                        }
                        Ok(true) => std::ops::ControlFlow::Break(()),
                        Err(e) => {
                            failed = Some(e);
                            std::ops::ControlFlow::Break(())
                        }
                    }
                },
            );
            if let Some(e) = failed {
                return Err(e);
            }
            if !sink.done() {
                sink.feed(&chunk, variable, segment, env)?;
            }
        }
        PhysicalOp::IndexScan {
            variable,
            label,
            prop_eq,
            prop_range,
            text_pred,
        } => {
            // moon#1220: streamed like the label scan — the index scan stops
            // once the demand is met instead of collecting every key first.
            let mut chunk: Vec<NodeKey> = Vec::with_capacity(sink.chunk_len());
            let mut failed: Option<ExecError> = None;
            let _ = index_scan_try_for_each(
                env.memgraph,
                env.csr_segs,
                label.as_ref(),
                prop_eq,
                prop_range,
                text_pred,
                env.params,
                env.ctx,
                |key| {
                    chunk.push(key);
                    if chunk.len() < sink.chunk_len() {
                        return std::ops::ControlFlow::Continue(());
                    }
                    match sink.feed(&chunk, variable, segment, env) {
                        Ok(false) => {
                            chunk.clear();
                            std::ops::ControlFlow::Continue(())
                        }
                        Ok(true) => std::ops::ControlFlow::Break(()),
                        Err(e) => {
                            failed = Some(e);
                            std::ops::ControlFlow::Break(())
                        }
                    }
                },
            );
            if let Some(e) = failed {
                return Err(e);
            }
            if !sink.done() {
                sink.feed(&chunk, variable, segment, env)?;
            }
        }
        _ => return Ok(()),
    }
    sink.finish(st, segment, env)
}

/// Accumulates the streamed prefix's output chunk by chunk.
struct StreamSink<'t> {
    need: usize,
    chunk_len: usize,
    scanned: u64,
    fed_any: bool,
    rows: Vec<Row<'t>>,
    projected: Option<Vec<Vec<Value>>>,
    columns: Vec<String>,
    /// Per segment op: rows it emitted for the chunks fed so far (`apply_op`'s
    /// `emitted_before`).
    emitted: SmallVec<[usize; 8]>,
}

impl<'t> StreamSink<'t> {
    fn new(need: usize, segment_len: usize) -> Self {
        Self {
            emitted: SmallVec::from_elem(0, segment_len),
            need,
            // Start near the demand; double per chunk so a selective filter
            // costs O(log) chunk rounds, not O(n / demand).
            chunk_len: need.clamp(16, 1024),
            scanned: 0,
            fed_any: false,
            rows: Vec::new(),
            projected: None,
            columns: Vec::new(),
        }
    }

    fn chunk_len(&self) -> usize {
        self.chunk_len
    }

    fn produced(&self) -> usize {
        self.projected.as_ref().map_or(self.rows.len(), Vec::len)
    }

    fn done(&self) -> bool {
        self.fed_any && self.produced() >= self.need
    }

    /// Run `keys` through the scan binding and `segment`; `true` once the
    /// demand is met.
    fn feed(
        &mut self,
        keys: &[NodeKey],
        variable: &str,
        segment: &[PhysicalOp],
        env: &OpEnv<'_, 't>,
    ) -> Result<bool, ExecError> {
        self.scanned += keys.len() as u64;
        let mut part = OpState {
            rows: keys
                .iter()
                .map(|&key| {
                    let mut row = Row::seed(env.slot_table);
                    row.insert(variable, Value::Node(key));
                    row
                })
                .collect(),
            projected_rows: None,
            columns: Vec::new(),
            nodes_scanned: 0,
        };
        for (op, emitted) in segment.iter().zip(self.emitted.iter_mut()) {
            apply_op(op, &mut part, env, usize::MAX, *emitted)?;
            // Row-stream ops (Filter / Expand / Unwind) leave their output in `rows`;
            // only the var-length Expand reads the count back.
            *emitted = emitted.saturating_add(part.rows.len());
        }
        match part.projected_rows {
            Some(p) => self.projected.get_or_insert_with(Vec::new).extend(p),
            None => self.rows.extend(part.rows),
        }
        self.columns = part.columns;
        self.fed_any = true;
        self.chunk_len = (self.chunk_len * 2).min(8192);
        Ok(self.done())
    }

    fn finish(
        mut self,
        st: &mut OpState<'t>,
        segment: &[PhysicalOp],
        env: &OpEnv<'_, 't>,
    ) -> Result<(), ExecError> {
        if !self.fed_any {
            // Nothing scanned (or LIMIT 0): still run the segment once so a
            // projection names its columns exactly as full evaluation does.
            self.feed(&[], "", segment, env)?;
        }
        st.nodes_scanned += self.scanned;
        st.rows = self.rows;
        st.projected_rows = self.projected;
        st.columns = self.columns;
        Ok(())
    }
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
