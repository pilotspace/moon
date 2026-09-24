//! `RETURN … ORDER BY … [SKIP s] LIMIT k` without projecting every row (moon#1220).
//!
//! The plan `… → Project (RETURN) → Sort → [Skip] → Limit` projected EVERY input row — every
//! RETURN item evaluated, one `Vec<Value>` per row — before the fused ORDER BY + LIMIT of
//! moon#1197 selected the page. Here only the RETURN columns ORDER BY reads are evaluated for
//! every row; the best `keep = s + k` rows are kept by (sort key, input order) in a buffer that
//! is cut back to `keep` whenever it doubles, rejecting outright any row that cannot beat the
//! current `keep`-th; and only the kept rows are projected. When the rows come from a scan
//! through row-local operators (`Filter`, `Unwind`, single-hop `Expand`) they are streamed in
//! chunks and never materialised at all.
//!
//! **Identical to full evaluation** (`execute_profile` is the oracle): HEAD's stable sort +
//! truncate keeps exactly the `keep` smallest rows by (comparator, input index), in that order,
//! whenever the comparator is a total preorder (`pipeline::totally_ordered`); every value is the
//! RETURN item evaluated on the same row. A NaN, or an int beyond ±2^53 next to a float, makes
//! `compare_values` intransitive — the fused path then declines (`None`) and the plan runs
//! operator by operator, i.e. HEAD's exact stable sort. Variable-length `Expand` is never
//! streamed: its row cap is per operator invocation, so chunking could change which rows it
//! yields.

use std::cmp::Ordering;

use super::pipeline::{self, OpEnv, OpState};
use super::*;

/// Largest `keep` the fused path takes; beyond it the buffer saves little over full
/// projection and the moon#1197 selection handles the sort.
const TOPK_MAX_KEEP: usize = pipeline::STREAM_MAX_ROWS;

/// Scan keys per streamed chunk.
const CHUNK: usize = 1024;

#[cfg(test)]
thread_local! {
    /// `(streamed, materialised)` fused top-k runs on this thread (tests assert the path they
    /// compare was actually taken).
    static FUSED: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
}

/// `(streamed, materialised)` fused RETURN + ORDER BY runs so far on this thread.
#[cfg(test)]
pub(crate) fn fused_counts() -> (usize, usize) {
    FUSED.with(std::cell::Cell::get)
}

/// The part of `Project` + `Sort` the fused path needs.
struct Shape<'p> {
    items: &'p [ReturnItem],
    columns: Vec<String>,
    /// Per ORDER BY item: its position in the evaluated key vector, or `None` when it names no
    /// RETURN column (HEAD's `Sort` then reads Null for every row).
    sort_keys: SmallVec<[Option<usize>; 4]>,
    ascending: SmallVec<[bool; 4]>,
    /// Per RETURN column: its position in the key vector when ORDER BY reads it.
    col_key: SmallVec<[Option<usize>; 8]>,
    /// The RETURN columns ORDER BY reads, ascending — the key vector's layout.
    key_cols: SmallVec<[usize; 4]>,
}

/// `Project` then `Sort`, in the shape the fused path handles: a plain RETURN projection (no
/// DISTINCT, no aggregate, not a WITH rebind) followed by ORDER BY.
fn shape<'p>(project: &'p PhysicalOp, sort: &PhysicalOp) -> Option<Shape<'p>> {
    let PhysicalOp::Project {
        items,
        distinct: false,
        rebind: false,
    } = project
    else {
        return None;
    };
    let PhysicalOp::Sort { items: sort_items } = sort else {
        return None;
    };
    if pipeline::has_aggregate(items) {
        return None;
    }
    // Column names exactly as `Project` names them; ORDER BY items resolve to the FIRST column
    // of the same name, exactly as `Sort` resolves them.
    let columns: Vec<String> = items
        .iter()
        .map(|item| match &item.alias {
            Some(alias) => alias.clone(),
            None => expr_to_string(&item.expr),
        })
        .collect();
    let sort_cols: SmallVec<[Option<usize>; 4]> = sort_items
        .iter()
        .map(|(expr, _)| {
            let name = expr_to_string(expr);
            columns.iter().position(|c| *c == name)
        })
        .collect();
    let mut key_cols: SmallVec<[usize; 4]> = sort_cols.iter().flatten().copied().collect();
    key_cols.sort_unstable();
    key_cols.dedup();
    let key_pos = |c: usize| key_cols.iter().position(|&k| k == c);
    let sort_keys = sort_cols.iter().map(|c| c.and_then(key_pos)).collect();
    let col_key = (0..items.len()).map(key_pos).collect();
    Some(Shape {
        items,
        columns,
        sort_keys,
        ascending: sort_items.iter().map(|(_, asc)| *asc).collect(),
        col_key,
        key_cols,
    })
}

type Keys = SmallVec<[Value; 2]>;

impl Shape<'_> {
    /// One RETURN item on one row — exactly `Project`'s per-row evaluation.
    fn eval_item(&self, item: &ReturnItem, row: &Row<'_>, env: &OpEnv<'_, '_>) -> Value {
        if matches!(item.expr, Expr::Star) {
            Value::Map(row.iter().map(|(k, v)| (k.to_owned(), v.clone())).collect())
        } else {
            eval_expr(
                &item.expr,
                row,
                env.memgraph,
                env.params,
                env.csr_segs,
                env.ctx.snapshot_lsn,
                env.ctx.decay,
            )
        }
    }

    /// The RETURN columns ORDER BY reads, evaluated on `row`.
    fn keys(&self, row: &Row<'_>, env: &OpEnv<'_, '_>) -> Keys {
        self.key_cols
            .iter()
            .map(|&c| self.eval_item(&self.items[c], row, env))
            .collect()
    }

    /// `Sort`'s comparator over two key vectors (`pipeline::cmp_keys` order).
    fn cmp(&self, a: &Keys, b: &Keys) -> Ordering {
        let null = Value::Null;
        for (pos, asc) in self.sort_keys.iter().zip(&self.ascending) {
            let ord = compare_values(
                pos.and_then(|p| a.get(p)).unwrap_or(&null),
                pos.and_then(|p| b.get(p)).unwrap_or(&null),
            );
            let ord = if *asc { ord } else { ord.reverse() };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    }

    /// The full RETURN row for a kept candidate: key columns move out of `keys`, every other
    /// column is evaluated on the row.
    fn project(&self, mut keys: Keys, row: &Row<'_>, env: &OpEnv<'_, '_>) -> Vec<Value> {
        self.items
            .iter()
            .zip(&self.col_key)
            .map(|(item, key)| match key.and_then(|k| keys.get_mut(k)) {
                Some(v) => std::mem::replace(v, Value::Null),
                None => self.eval_item(item, row, env),
            })
            .collect()
    }
}

/// Running [`pipeline::totally_ordered`] over every key value offered.
#[derive(Default)]
struct TotalOrder {
    float: bool,
    big_int: bool,
    broken: bool,
}

impl TotalOrder {
    fn feed(&mut self, keys: &Keys) {
        const EXACT: i64 = 1 << 53;
        for v in keys {
            match v {
                Value::Float(f) if f.is_nan() => self.broken = true,
                Value::Float(_) => self.float = true,
                Value::Int(i) if !(-EXACT..=EXACT).contains(i) => self.big_int = true,
                _ => {}
            }
        }
        if self.float && self.big_int {
            self.broken = true;
        }
    }
}

struct Candidate<T> {
    keys: Keys,
    seq: u64,
    payload: T,
}

/// The best `keep` offers by (key, offer order).
struct TopRows<'s, 'p, T> {
    shape: &'s Shape<'p>,
    keep: usize,
    buf: Vec<Candidate<T>>,
    /// Keys of the worst kept candidate after the last cut: an offer that does not compare
    /// strictly better is not in the top `keep` (a tie loses on offer order).
    threshold: Option<Keys>,
    order: TotalOrder,
    seq: u64,
}

impl<'s, 'p, T> TopRows<'s, 'p, T> {
    fn new(shape: &'s Shape<'p>, keep: usize) -> Self {
        Self {
            shape,
            keep,
            buf: Vec::with_capacity(keep.saturating_mul(2).min(4096)),
            threshold: None,
            order: TotalOrder::default(),
            seq: 0,
        }
    }

    fn broken(&self) -> bool {
        self.order.broken
    }

    fn offer(&mut self, keys: Keys, payload: T) {
        self.order.feed(&keys);
        let seq = self.seq;
        self.seq += 1;
        if let Some(t) = &self.threshold
            && self.shape.cmp(&keys, t) != Ordering::Less
        {
            return;
        }
        self.buf.push(Candidate { keys, seq, payload });
        if self.buf.len() >= self.keep.saturating_mul(2).max(self.keep + 256) {
            self.cut();
        }
    }

    /// Keep the best `keep` candidates and remember the worst of them.
    fn cut(&mut self) {
        let shape = self.shape;
        let by = |a: &Candidate<T>, b: &Candidate<T>| {
            shape.cmp(&a.keys, &b.keys).then(a.seq.cmp(&b.seq))
        };
        if self.buf.len() > self.keep {
            self.buf.select_nth_unstable_by(self.keep - 1, by);
            self.buf.truncate(self.keep);
        }
        if self.buf.len() == self.keep {
            self.threshold = self
                .buf
                .iter()
                .max_by(|a, b| by(a, b))
                .map(|c| c.keys.clone());
        }
    }

    /// The kept candidates, best first — `None` when the comparator turned out not to be a
    /// total preorder (the caller falls back to full evaluation).
    fn finish(mut self) -> Option<Vec<Candidate<T>>> {
        if self.broken() {
            return None;
        }
        self.cut();
        let shape = self.shape;
        self.buf
            .sort_unstable_by(|a, b| shape.cmp(&a.keys, &b.keys).then(a.seq.cmp(&b.seq)));
        Some(self.buf)
    }
}

/// The Sort's output demand when `ops[p]`/`ops[p + 1]` form a fusable RETURN + ORDER BY.
fn fused_keep<'p>(ops: &'p [PhysicalOp], demand: &[usize], p: usize) -> Option<(Shape<'p>, usize)> {
    let shape = shape(ops.get(p)?, ops.get(p + 1)?)?;
    let keep = *demand.get(p + 1)?;
    (keep > 0 && keep <= TOPK_MAX_KEEP).then_some((shape, keep))
}

/// Fused RETURN + ORDER BY over materialised rows at `ops[p]` (the `Project`). `Some(2)` when
/// it replaced `ops[p]` and `ops[p + 1]`; `None` to run them as usual (not the shape, demand
/// unbounded, or a non-total key order).
pub(super) fn project_sorted_rows<'t>(
    ops: &[PhysicalOp],
    demand: &[usize],
    p: usize,
    st: &mut OpState<'t>,
    env: &OpEnv<'_, 't>,
) -> Option<usize> {
    if st.projected_rows.is_some() {
        return None;
    }
    let (shape, keep) = fused_keep(ops, demand, p)?;
    if keep >= st.rows.len() {
        return None; // nothing to leave out
    }
    let mut top = TopRows::new(&shape, keep);
    for (i, row) in st.rows.iter().enumerate() {
        top.offer(shape.keys(row, env), i);
        if top.broken() {
            return None;
        }
    }
    let kept = top.finish()?;
    let projected: Vec<Vec<Value>> = kept
        .into_iter()
        .map(|c| shape.project(c.keys, &st.rows[c.payload], env))
        .collect();
    st.rows.clear();
    st.projected_rows = Some(projected);
    st.columns = shape.columns;
    #[cfg(test)]
    FUSED.with(|c| c.set((c.get().0, c.get().1 + 1)));
    Some(2)
}

/// Fused scan → row-local ops → RETURN + ORDER BY, streamed. `Some(end)` when it consumed
/// `ops[..end]` (through the `Sort`) into `st`; `None` (with `st` untouched) to run the plan
/// as usual.
pub(super) fn stream_sorted_scan<'t>(
    ops: &[PhysicalOp],
    demand: &[usize],
    st: &mut OpState<'t>,
    env: &OpEnv<'_, 't>,
) -> Result<Option<usize>, ExecError> {
    let Some(scan) = ops.first() else {
        return Ok(None);
    };
    let (PhysicalOp::NodeScan { variable, .. } | PhysicalOp::IndexScan { variable, .. }) = scan
    else {
        return Ok(None);
    };
    let mut p = 1;
    while let Some(op) = ops.get(p) {
        match op {
            PhysicalOp::Filter { .. } | PhysicalOp::Unwind { .. } => p += 1,
            PhysicalOp::Expand { max_hops, .. } if *max_hops <= 1 => p += 1,
            _ => break,
        }
    }
    let Some((shape, keep)) = fused_keep(ops, demand, p) else {
        return Ok(None);
    };
    let segment = &ops[1..p];
    let mut top: TopRows<'_, '_, Row<'t>> = TopRows::new(&shape, keep);
    let mut scanned = 0u64;
    let mut feed = |keys: &[NodeKey], top: &mut TopRows<'_, '_, Row<'t>>| {
        scanned += keys.len() as u64;
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
        for op in segment {
            apply_op(op, &mut part, env, usize::MAX)?;
        }
        for row in part.rows {
            let keys = shape.keys(&row, env);
            top.offer(keys, row);
            if top.broken() {
                break;
            }
        }
        Ok::<(), ExecError>(())
    };
    match scan {
        PhysicalOp::NodeScan { label, .. } => {
            let label_id = label.as_ref().map(|l| label_to_id(l.as_bytes()));
            let committed = roaring::RoaringBitmap::new();
            let view = crate::graph::view::MergedNodeView::new(env.memgraph, env.csr_segs);
            let mut chunk: Vec<NodeKey> = Vec::with_capacity(CHUNK);
            let mut failed: Option<ExecError> = None;
            let _ = view.try_for_each_visible_node(
                label_id,
                env.ctx.snapshot_lsn,
                env.ctx.my_txn_id,
                &committed,
                env.ctx.valid_time_as_of,
                |key| {
                    chunk.push(key);
                    if chunk.len() < CHUNK {
                        return std::ops::ControlFlow::Continue(());
                    }
                    if let Err(e) = feed(&chunk, &mut top) {
                        failed = Some(e);
                        return std::ops::ControlFlow::Break(());
                    }
                    chunk.clear();
                    if top.broken() {
                        return std::ops::ControlFlow::Break(());
                    }
                    std::ops::ControlFlow::Continue(())
                },
            );
            if let Some(e) = failed {
                return Err(e);
            }
            if !top.broken() {
                feed(&chunk, &mut top)?;
            }
        }
        PhysicalOp::IndexScan {
            label,
            prop_eq,
            prop_range,
            text_pred,
            ..
        } => {
            let keys = index_scan_keys(
                env.memgraph,
                env.csr_segs,
                label.as_ref(),
                prop_eq,
                prop_range,
                text_pred,
                env.params,
                env.ctx,
            );
            for chunk in keys.chunks(CHUNK) {
                feed(chunk, &mut top)?;
                if top.broken() {
                    break;
                }
            }
        }
        _ => return Ok(None),
    }
    let Some(kept) = top.finish() else {
        return Ok(None);
    };
    st.projected_rows = Some(
        kept.into_iter()
            .map(|c| shape.project(c.keys, &c.payload, env))
            .collect(),
    );
    st.rows.clear();
    st.columns = shape.columns;
    st.nodes_scanned += scanned;
    #[cfg(test)]
    FUSED.with(|c| c.set((c.get().0 + 1, c.get().1)));
    Ok(Some(p + 2))
}
