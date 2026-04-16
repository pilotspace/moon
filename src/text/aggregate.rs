//! FT.AGGREGATE pipeline core (Phase 152).
//!
//! Honors CONTEXT.md D-01 (enum pipeline), D-02 (SmallVec row buffer), D-03
//! (v1 reducer set COUNT/COUNT_DISTINCT/SUM/AVG/MIN/MAX), D-04 (APPLY errors),
//! D-05/D-06 (associative-commutative merges for shard fan-in).
//!
//! Source-audit override of D-06: reuses production `crate::storage::hll::Hll`
//! (P=14, 16384 registers, Redis-HYLL wire format) rather than hand-rolling a
//! 4 KiB/P=12 sketch. Zero new unsafe, zero new format to maintain.
//!
//! Out of scope: parser (Plan 02), scatter-gather dispatch (Plan 03), I/O.

#![cfg(feature = "text-index")]

use std::sync::Arc;

use bytes::Bytes;
use ordered_float::OrderedFloat;
use smallvec::SmallVec;

use crate::storage::hll::Hll;
use crate::vector::filter::expression::FilterExpr;

// ---------------------------------------------------------------------------
// Pipeline operator AST
// ---------------------------------------------------------------------------

/// A single pipeline stage.
///
/// Executed iteratively by `execute_pipeline` over a materialized row buffer
/// (`Vec<AggregateRow>`). Order of variants is significant only inside the
/// `Vec<AggregateStep>` the caller constructs — Plan 02's parser preserves the
/// client-supplied order.
///
/// `FilterExpr` is wrapped in `Arc` because `FilterExpr` intentionally does not
/// derive `Clone` (the AST can contain `Bytes` and boxed sub-expressions that
/// would otherwise invite deep clones on every step traversal). Sharing the
/// parsed filter by refcount is O(1) and matches how the scatter-gather layer
/// wants to ship the pipeline to every shard.
#[derive(Debug, Clone)]
pub enum AggregateStep {
    /// FILTER clause. In v1 the FILTER is evaluated **before** rows reach this
    /// pipeline (Plan 02's handler uses `PayloadIndex::evaluate_bitmap` at the
    /// TextIndex boundary). If a FILTER step reaches `execute_pipeline` it is
    /// a pass-through — the variant is kept so the pipeline remains a faithful
    /// record of the client's request for introspection and future inline
    /// filter evaluation.
    Filter(Arc<FilterExpr>),
    /// GROUPBY / REDUCE. `fields` are the grouping columns in declared order;
    /// `reducers` are the REDUCE clauses in declared order — indices are
    /// implicit and must match between per-shard partials and the coordinator
    /// merge.
    GroupBy {
        fields: SmallVec<[Bytes; 4]>,
        reducers: Vec<ReducerSpec>,
    },
    /// SORTBY. `keys` are sort keys with direction; optional `max` truncates
    /// after sort (RediSearch `MAX N`).
    SortBy {
        keys: SmallVec<[(Bytes, SortOrder); 4]>,
        max: Option<usize>,
    },
    /// APPLY stub — parsed but not evaluated in v1 (D-04). `execute_pipeline`
    /// returns `Err(Bytes::from_static(b"ERR APPLY stage not supported in v1"))`.
    Apply { expr: Bytes, alias: Bytes },
    /// LIMIT offset count. Parser is responsible for enforcing the global
    /// `AGGREGATE_LIMIT_CAP` clamp (Plan 02).
    Limit { offset: usize, count: usize },
}

/// Sort direction for `AggregateStep::SortBy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    Asc,
    Desc,
}

/// REDUCE function selector.
///
/// Exactly the set declared in CONTEXT.md D-03. New reducers (STDDEV, QUANTILE,
/// TOLIST, …) land behind AGG-05 in a separate plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReducerFn {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    CountDistinct,
}

/// Parsed REDUCE clause.
///
/// - `fn_name` is the selector.
/// - `field` is `None` for COUNT only; every other reducer requires a source
///   field to be aggregated over.
/// - `alias` is the result column name (the `AS` target).
#[derive(Debug, Clone)]
pub struct ReducerSpec {
    pub fn_name: ReducerFn,
    pub field: Option<Bytes>,
    pub alias: Bytes,
}

impl ReducerSpec {
    /// Validate that `field` is supplied when required.
    ///
    /// Called by the parser (Plan 02) before pipeline assembly. Returns a
    /// static error string suitable for wrapping in a `Frame::Error`.
    pub fn validate(&self) -> Result<(), &'static str> {
        match self.fn_name {
            ReducerFn::Count => Ok(()),
            ReducerFn::Sum
            | ReducerFn::Avg
            | ReducerFn::Min
            | ReducerFn::Max
            | ReducerFn::CountDistinct => {
                if self.field.is_some() {
                    Ok(())
                } else {
                    Err("ERR reducer requires a field")
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Row buffer and value types
// ---------------------------------------------------------------------------

/// A single row flowing between pipeline stages.
///
/// `SmallVec` inline at 8 tuples — typical aggregation rows carry 1–4 GROUPBY
/// keys plus 1–3 reducer outputs, so the inline path covers > 95% of cases.
/// Field lookup is O(N) but N is tiny; a hashmap would cost more than the scan
/// for rows this small.
pub type AggregateRow = SmallVec<[(Bytes, AggregateValue); 8]>;

/// A stage-internal value.
///
/// `Float` uses `OrderedFloat<f64>` so NaN participates in a total order
/// without panicking — critical for SORTBY which must never crash on bad data.
#[derive(Debug, Clone)]
pub enum AggregateValue {
    Null,
    Int(i64),
    Float(OrderedFloat<f64>),
    Str(Bytes),
}

/// Stable ordered GROUPBY key.
///
/// Constructed by joining field values **in the order declared by the GroupBy
/// step** (RESEARCH Pitfall 9). This gives every shard the same key for the
/// same row regardless of insertion order, which is the non-negotiable
/// precondition for associative merge at the coordinator.
pub type GroupKey = SmallVec<[Bytes; 4]>;

// ---------------------------------------------------------------------------
// Partial reducer state — associative, commutative merge
// ---------------------------------------------------------------------------

/// Per-shard reducer state, shipped back to the coordinator.
///
/// Each variant is a **commutative monoid** with respect to `merge`:
/// - `Count`: additive identity 0.
/// - `Sum`: additive identity `{ sum: 0.0, count: 0 }`; `is_avg` at finalize
///   time decides whether to divide by count or return the raw sum.
/// - `Min` / `Max`: identity `None`, monotone min/max combine.
/// - `CountDistinct`: identity is an empty `Hll`; `Hll::merge_from` is
///   register-max associative merge.
///
/// `Clone` is implemented manually because `storage::hll::Hll` does not
/// derive `Clone`. Cloning a `CountDistinct` goes through `Hll::from_bytes`
/// on a copy of the wire bytes — O(wire-length), still well under a
/// microsecond for sparse sketches and ~12 KiB copy for dense.
#[derive(Debug)]
pub enum PartialReducerState {
    Count(u64),
    /// Used for both SUM and AVG. `is_avg` is passed to `finalize` to pick
    /// between raw sum and sum/count.
    Sum {
        sum: f64,
        count: u64,
    },
    Min(Option<OrderedFloat<f64>>),
    Max(Option<OrderedFloat<f64>>),
    CountDistinct(Hll),
}

impl Clone for PartialReducerState {
    fn clone(&self) -> Self {
        match self {
            Self::Count(n) => Self::Count(*n),
            Self::Sum { sum, count } => Self::Sum {
                sum: *sum,
                count: *count,
            },
            Self::Min(v) => Self::Min(*v),
            Self::Max(v) => Self::Max(*v),
            Self::CountDistinct(h) => {
                // Hll::from_bytes validates the header — a freshly-serialized
                // HYLL blob is always valid, so this branch cannot fail. We
                // fall back to an empty sketch on the unreachable error path
                // rather than unwrap (CLAUDE.md error-handling rule).
                let bytes = Bytes::copy_from_slice(h.as_bytes());
                match Hll::from_bytes(bytes) {
                    Ok(copy) => Self::CountDistinct(copy),
                    Err(_) => Self::CountDistinct(Hll::new_sparse()),
                }
            }
        }
    }
}

impl PartialReducerState {
    /// Associative, commutative merge.
    ///
    /// Type-mismatched variants leave `self` unchanged — a mismatch is a
    /// coordinator bug, not an attacker signal, and panicking here would take
    /// down the shard loop (T-152-01-03).
    pub fn merge(&mut self, other: Self) {
        match (self, other) {
            (Self::Count(a), Self::Count(b)) => *a += b,
            (Self::Sum { sum, count }, Self::Sum { sum: s2, count: c2 }) => {
                *sum += s2;
                *count += c2;
            }
            (Self::Min(a), Self::Min(b)) => {
                *a = match (*a, b) {
                    (Some(x), Some(y)) => Some(OrderedFloat(x.into_inner().min(y.into_inner()))),
                    (None, v) | (v, None) => v,
                };
            }
            (Self::Max(a), Self::Max(b)) => {
                *a = match (*a, b) {
                    (Some(x), Some(y)) => Some(OrderedFloat(x.into_inner().max(y.into_inner()))),
                    (None, v) | (v, None) => v,
                };
            }
            (Self::CountDistinct(a), Self::CountDistinct(b)) => a.merge_from(&b),
            // Type mismatch — leave state unchanged (T-152-01-03 disposition: accept).
            _ => {}
        }
    }

    /// Produce the final user-visible value.
    ///
    /// `is_avg = true` is only meaningful for the `Sum` variant: it divides
    /// `sum / count`, returning `Null` when `count == 0` (T-152-01-05). All
    /// other variants ignore `is_avg`.
    pub fn finalize(self, is_avg: bool) -> AggregateValue {
        match self {
            Self::Count(n) => AggregateValue::Int(n as i64),
            Self::Sum { sum, count } => {
                if is_avg {
                    if count == 0 {
                        AggregateValue::Null
                    } else {
                        AggregateValue::Float(OrderedFloat(sum / count as f64))
                    }
                } else {
                    AggregateValue::Float(OrderedFloat(sum))
                }
            }
            Self::Min(Some(v)) | Self::Max(Some(v)) => AggregateValue::Float(v),
            Self::Min(None) | Self::Max(None) => AggregateValue::Null,
            Self::CountDistinct(hll) => AggregateValue::Int(hll.count() as i64),
        }
    }
}

// ---------------------------------------------------------------------------
// Pipeline executor
// ---------------------------------------------------------------------------

/// Per-shard cap on the number of distinct GROUPBY keys the executor will
/// accept (RESEARCH Pitfall 7, threat T-152-01-01 DoS mitigation).
///
/// The cap is enforced **before** the group is inserted into the HashMap so
/// worst-case memory is bounded at `N * reducer-state-size` — never N+1.
/// 10_000 distinct groups × the ~16 KiB dense HLL ceiling ≈ 160 MiB per shard
/// in the worst COUNT_DISTINCT case; sparse encoding keeps the typical
/// overhead to ~50 bytes per idle group.
pub const GROUPBY_CARDINALITY_LIMIT: usize = 10_000;

/// Execute the aggregation pipeline on a materialised row buffer.
///
/// Runs each stage iteratively; intermediate buffers are owned and replaced
/// between stages so no row is held across a stage boundary.
///
/// Returns:
/// - `Ok(rows)` on success.
/// - `Err(Bytes)` when a stage cannot run (APPLY is always an error in v1
///   per D-04; GROUPBY exceeding `GROUPBY_CARDINALITY_LIMIT` returns the
///   registered DoS-mitigation error). The returned `Bytes` is suitable
///   for wrapping in `Frame::Error`.
pub fn execute_pipeline(
    rows: Vec<AggregateRow>,
    pipeline: &[AggregateStep],
) -> Result<Vec<AggregateRow>, Bytes> {
    let mut current = rows;
    for step in pipeline {
        current = match step {
            AggregateStep::Filter(expr) => apply_filter(current, expr),
            AggregateStep::GroupBy { fields, reducers } => {
                apply_groupby(current, fields, reducers)?
            }
            AggregateStep::SortBy { keys, max } => apply_sortby(current, keys, *max),
            AggregateStep::Apply { .. } => {
                return Err(Bytes::from_static(b"ERR APPLY stage not supported in v1"));
            }
            AggregateStep::Limit { offset, count } => apply_limit(current, *offset, *count),
        };
    }
    Ok(current)
}

/// FILTER stage. v1 evaluates filters **before** the pipeline runs (the
/// handler in Plan 02 calls `PayloadIndex::evaluate_bitmap` at the TextIndex
/// boundary and materialises rows only for the resulting doc set). If a
/// FILTER step is encountered here it is a pass-through — kept as an
/// explicit no-op rather than rejected so the pipeline stays a faithful
/// record of the parsed request for later introspection.
fn apply_filter(rows: Vec<AggregateRow>, _expr: &Arc<FilterExpr>) -> Vec<AggregateRow> {
    rows
}

/// LIMIT stage with saturating arithmetic — offset past end yields an empty
/// buffer without panicking.
fn apply_limit(rows: Vec<AggregateRow>, offset: usize, count: usize) -> Vec<AggregateRow> {
    if offset >= rows.len() {
        return Vec::new();
    }
    let end = offset.saturating_add(count).min(rows.len());
    // `end - offset` is always non-negative because of the `offset >= rows.len()`
    // early return above.
    rows.into_iter().skip(offset).take(end - offset).collect()
}

/// SORTBY stage. Uses a stable sort so ties preserve input order
/// (RediSearch-compatible). Multi-key sort applies keys left-to-right.
fn apply_sortby(
    mut rows: Vec<AggregateRow>,
    keys: &[(Bytes, SortOrder)],
    max: Option<usize>,
) -> Vec<AggregateRow> {
    rows.sort_by(|a, b| {
        for (field, order) in keys {
            let av = row_get(a, field);
            let bv = row_get(b, field);
            let ord = compare_values(av, bv);
            if ord != std::cmp::Ordering::Equal {
                return if *order == SortOrder::Desc {
                    ord.reverse()
                } else {
                    ord
                };
            }
        }
        std::cmp::Ordering::Equal
    });
    if let Some(n) = max {
        rows.truncate(n);
    }
    rows
}

/// GROUPBY + REDUCE stage.
///
/// Groups are keyed by a `GroupKey` built from the declared field list in
/// declared order (RESEARCH Pitfall 9 ensures cross-shard key stability).
///
/// **Cardinality cap (W5):** the check fires **before** the HashMap insert
/// so memory use is bounded at `GROUPBY_CARDINALITY_LIMIT` distinct keys —
/// not N+1. Re-seeing an existing key never triggers the cap.
fn apply_groupby(
    rows: Vec<AggregateRow>,
    fields: &[Bytes],
    reducers: &[ReducerSpec],
) -> Result<Vec<AggregateRow>, Bytes> {
    use std::collections::HashMap;

    let mut groups: HashMap<GroupKey, Vec<PartialReducerState>> = HashMap::new();
    for row in rows {
        let key: GroupKey = fields
            .iter()
            .map(|f| row_get(&row, f).cloned_as_bytes())
            .collect();

        // W5 — check BEFORE mutating (mirror of Plan 03 `build_shard_partial`).
        // A re-seen key does not trigger the cap; only the (N+1)th distinct
        // key does, and the error fires before the insert so cardinality is
        // never allowed to reach N+1.
        if !groups.contains_key(&key) && groups.len() >= GROUPBY_CARDINALITY_LIMIT {
            return Err(Bytes::from_static(
                b"ERR GROUPBY cardinality limit exceeded",
            ));
        }

        let states = groups.entry(key).or_insert_with(|| init_states(reducers));
        update_reducers(states, &row, reducers);
    }

    // Emit one output row per group: GROUPBY fields first (in declared order),
    // then reducer aliases in declared order.
    let mut out = Vec::with_capacity(groups.len());
    for (key, states) in groups {
        let mut row: AggregateRow = SmallVec::new();
        for (fname, val) in fields.iter().zip(key.iter()) {
            row.push((fname.clone(), AggregateValue::Str(val.clone())));
        }
        for (spec, state) in reducers.iter().zip(states) {
            let is_avg = matches!(spec.fn_name, ReducerFn::Avg);
            row.push((spec.alias.clone(), state.finalize(is_avg)));
        }
        out.push(row);
    }
    Ok(out)
}

/// Build the initial reducer-state vector for a fresh group.
fn init_states(reducers: &[ReducerSpec]) -> Vec<PartialReducerState> {
    reducers
        .iter()
        .map(|s| match s.fn_name {
            ReducerFn::Count => PartialReducerState::Count(0),
            ReducerFn::Sum | ReducerFn::Avg => PartialReducerState::Sum { sum: 0.0, count: 0 },
            ReducerFn::Min => PartialReducerState::Min(None),
            ReducerFn::Max => PartialReducerState::Max(None),
            ReducerFn::CountDistinct => PartialReducerState::CountDistinct(Hll::new_sparse()),
        })
        .collect()
}

/// Fold a row into each reducer state in position order.
///
/// Missing / null field values are skipped silently — the reducer simply
/// does not advance for that row. This matches RediSearch semantics and is
/// the correctness preserver for cross-shard merge (a missing value adds
/// zero to `count`, leaves `min`/`max` unchanged, does not add a bucket to
/// the HLL sketch).
fn update_reducers(
    states: &mut [PartialReducerState],
    row: &AggregateRow,
    reducers: &[ReducerSpec],
) {
    for (i, spec) in reducers.iter().enumerate() {
        match (&mut states[i], spec.fn_name) {
            (PartialReducerState::Count(n), ReducerFn::Count) => {
                *n += 1;
            }
            (PartialReducerState::Sum { sum, count }, ReducerFn::Sum | ReducerFn::Avg) => {
                if let Some(field) = &spec.field {
                    if let Some(f) = value_to_f64(row_get(row, field)) {
                        *sum += f;
                        *count += 1;
                    }
                }
            }
            (PartialReducerState::Min(opt), ReducerFn::Min) => {
                if let Some(field) = &spec.field {
                    if let Some(f) = value_to_f64(row_get(row, field)) {
                        *opt = match *opt {
                            None => Some(OrderedFloat(f)),
                            Some(cur) => Some(OrderedFloat(cur.into_inner().min(f))),
                        };
                    }
                }
            }
            (PartialReducerState::Max(opt), ReducerFn::Max) => {
                if let Some(field) = &spec.field {
                    if let Some(f) = value_to_f64(row_get(row, field)) {
                        *opt = match *opt {
                            None => Some(OrderedFloat(f)),
                            Some(cur) => Some(OrderedFloat(cur.into_inner().max(f))),
                        };
                    }
                }
            }
            (PartialReducerState::CountDistinct(hll), ReducerFn::CountDistinct) => {
                if let Some(field) = &spec.field {
                    if let Some(bytes) = value_to_bytes(row_get(row, field)) {
                        hll.add(&bytes);
                    }
                }
            }
            // Reducer / state mismatch shouldn't happen if init_states is in sync
            // with the ReducerSpec slice. Silently skip rather than panic.
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Row helpers (private)
// ---------------------------------------------------------------------------

fn row_get<'a>(row: &'a AggregateRow, field: &Bytes) -> Option<&'a AggregateValue> {
    // RediSearch-compatible name lookup: SORTBY / pipeline references use
    // `@foo` for field names while GROUPBY field names and REDUCE aliases are
    // stored verbatim (may or may not carry the `@`). Normalise both sides
    // by stripping one optional leading `@` before comparison, so
    // `@count` matches the stored alias `count` and vice versa.
    let needle = strip_at(field.as_ref());
    row.iter().find_map(|(f, v)| {
        if strip_at(f.as_ref()) == needle {
            Some(v)
        } else {
            None
        }
    })
}

#[inline]
fn strip_at(b: &[u8]) -> &[u8] {
    if let Some(rest) = b.strip_prefix(b"@") {
        rest
    } else {
        b
    }
}

fn value_to_f64(v: Option<&AggregateValue>) -> Option<f64> {
    match v? {
        AggregateValue::Int(n) => Some(*n as f64),
        AggregateValue::Float(of) => Some(of.into_inner()),
        AggregateValue::Str(b) => std::str::from_utf8(b).ok()?.parse().ok(),
        AggregateValue::Null => None,
    }
}

fn value_to_bytes(v: Option<&AggregateValue>) -> Option<Bytes> {
    match v? {
        AggregateValue::Int(n) => Some(Bytes::from(itoa::Buffer::new().format(*n).to_owned())),
        AggregateValue::Float(of) => Some(Bytes::from(format!("{}", of.into_inner()))),
        AggregateValue::Str(b) => Some(b.clone()),
        AggregateValue::Null => None,
    }
}

fn compare_values(a: Option<&AggregateValue>, b: Option<&AggregateValue>) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    let af = value_to_f64(a);
    let bf = value_to_f64(b);
    match (af, bf) {
        (Some(x), Some(y)) => x.partial_cmp(&y).unwrap_or(Equal),
        (Some(_), None) => Less,
        (None, Some(_)) => Greater,
        (None, None) => match (a, b) {
            (Some(AggregateValue::Str(x)), Some(AggregateValue::Str(y))) => x.cmp(y),
            _ => Equal,
        },
    }
}

/// Converter used by GROUPBY to build a stable `Bytes`-keyed `GroupKey`.
///
/// Null / missing values collapse to an empty byte string so every row
/// produces a well-formed key even when a GROUPBY field is absent.
trait CloneAsBytes {
    fn cloned_as_bytes(&self) -> Bytes;
}

impl CloneAsBytes for Option<&AggregateValue> {
    fn cloned_as_bytes(&self) -> Bytes {
        match self {
            Some(AggregateValue::Str(b)) => b.clone(),
            Some(AggregateValue::Int(n)) => Bytes::from(itoa::Buffer::new().format(*n).to_owned()),
            Some(AggregateValue::Float(of)) => Bytes::from(format!("{}", of.into_inner())),
            Some(AggregateValue::Null) | None => Bytes::from_static(b""),
        }
    }
}

// ---------------------------------------------------------------------------
// Coordinator-side associative merge
// ---------------------------------------------------------------------------

/// A shard's contribution to an FT.AGGREGATE request.
///
/// One entry per distinct `GroupKey` on that shard, each carrying reducer
/// states in **positional** correspondence with the `reducers` slice of the
/// pipeline's GroupBy step. Positional (not named) is the cheapest way to
/// keep shard/coordinator in sync because the pipeline is broadcast
/// identically to every shard.
///
/// Inline size 4 matches the common case of ≤ 4 reducer clauses per
/// FT.AGGREGATE query.
pub type ShardPartial = Vec<(GroupKey, SmallVec<[PartialReducerState; 4]>)>;

// ---------------------------------------------------------------------------
// Public wrappers for Plan 03 scatter-gather
// ---------------------------------------------------------------------------

/// Initialize per-reducer partial states for a fresh shard group.
///
/// Exposed publicly so the scatter-gather path (`ft_aggregate::build_shard_partial`)
/// can allocate states identically to the local `apply_groupby` path without
/// duplicating the match. Output shape is `SmallVec<[_; 4]>` (matches the
/// `ShardPartial` wire type); the inline 4 covers ≤ 4 reducer clauses which
/// is the overwhelmingly common case.
pub fn init_shard_states(reducers: &[ReducerSpec]) -> SmallVec<[PartialReducerState; 4]> {
    reducers
        .iter()
        .map(|s| match s.fn_name {
            ReducerFn::Count => PartialReducerState::Count(0),
            ReducerFn::Sum | ReducerFn::Avg => PartialReducerState::Sum { sum: 0.0, count: 0 },
            ReducerFn::Min => PartialReducerState::Min(None),
            ReducerFn::Max => PartialReducerState::Max(None),
            ReducerFn::CountDistinct => PartialReducerState::CountDistinct(Hll::new_sparse()),
        })
        .collect()
}

/// Public wrapper around the private `update_reducers` — exposed for the
/// Plan 03 scatter path which builds `ShardPartial` outside this module.
pub fn update_reducers_public(
    states: &mut [PartialReducerState],
    row: &AggregateRow,
    reducers: &[ReducerSpec],
) {
    update_reducers(states, row, reducers);
}

/// Public helper used by `build_shard_partial` to construct a `GroupKey` entry.
///
/// Normalises field values into `Bytes` using the same rules as the internal
/// `CloneAsBytes` impl so cross-shard keys are identical for the same row
/// (RESEARCH Pitfall 9). Null / missing fields collapse to an empty byte
/// string.
pub fn row_get_as_bytes(row: &AggregateRow, field: &Bytes) -> Bytes {
    row_get(row, field).cloned_as_bytes()
}

/// Associatively merge partial reducer states from multiple shards into a
/// finalized row set.
///
/// All shards **MUST** have executed the same pipeline — the reducer order
/// is implied by `reducers`, not serialized. `PartialReducerState::merge`
/// handles the per-variant combine; type mismatches (which should not occur
/// under this invariant) are silent no-ops.
///
/// Output shape matches per-shard `apply_groupby`: GroupBy field names first
/// (in declared order), reducer aliases second (in declared order). This
/// lets the coordinator feed the output straight into downstream SORTBY /
/// LIMIT stages at the plan-03 gather site.
pub fn merge_partial_states(
    shard_partials: Vec<ShardPartial>,
    groupby_fields: &[Bytes],
    reducers: &[ReducerSpec],
) -> Vec<AggregateRow> {
    use std::collections::HashMap;

    let mut merged: HashMap<GroupKey, SmallVec<[PartialReducerState; 4]>> = HashMap::new();
    for partial in shard_partials {
        for (key, states) in partial {
            match merged.get_mut(&key) {
                Some(acc) => {
                    // Merge state-by-state in position order. A length
                    // mismatch would indicate a pipeline-shape bug; `zip`
                    // stops at the shorter side so we never panic on
                    // mismatched data — the coordinator loses the surplus
                    // states rather than crashing.
                    for (a, b) in acc.iter_mut().zip(states) {
                        a.merge(b);
                    }
                }
                None => {
                    merged.insert(key, states);
                }
            }
        }
    }

    // Finalize: emit one AggregateRow per group.
    let mut out = Vec::with_capacity(merged.len());
    for (key, states) in merged {
        let mut row: AggregateRow = SmallVec::new();
        for (fname, val) in groupby_fields.iter().zip(key.iter()) {
            row.push((fname.clone(), AggregateValue::Str(val.clone())));
        }
        for (spec, state) in reducers.iter().zip(states) {
            let is_avg = matches!(spec.fn_name, ReducerFn::Avg);
            row.push((spec.alias.clone(), state.finalize(is_avg)));
        }
        out.push(row);
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -------- ReducerSpec --------

    #[test]
    fn test_reducer_spec_count_has_no_field() {
        let spec = ReducerSpec {
            fn_name: ReducerFn::Count,
            field: None,
            alias: Bytes::from_static(b"count"),
        };
        assert!(spec.validate().is_ok());
    }

    #[test]
    fn test_reducer_spec_sum_requires_field() {
        let bad = ReducerSpec {
            fn_name: ReducerFn::Sum,
            field: None,
            alias: Bytes::from_static(b"total"),
        };
        assert_eq!(bad.validate(), Err("ERR reducer requires a field"));

        let good = ReducerSpec {
            fn_name: ReducerFn::Sum,
            field: Some(Bytes::from_static(b"@value")),
            alias: Bytes::from_static(b"total"),
        };
        assert!(good.validate().is_ok());
    }

    #[test]
    fn test_reducer_spec_all_non_count_require_field() {
        for fn_name in [
            ReducerFn::Sum,
            ReducerFn::Avg,
            ReducerFn::Min,
            ReducerFn::Max,
            ReducerFn::CountDistinct,
        ] {
            let bad = ReducerSpec {
                fn_name,
                field: None,
                alias: Bytes::from_static(b"a"),
            };
            assert_eq!(
                bad.validate(),
                Err("ERR reducer requires a field"),
                "{:?} should require a field",
                fn_name,
            );
        }
    }

    // -------- AggregateValue --------

    #[test]
    fn test_aggregate_value_ord_float_nan_safe() {
        // Sorting floats that include NaN must not panic. OrderedFloat sorts
        // NaN greater than any real number, consistently.
        let mut vals: Vec<OrderedFloat<f64>> = vec![
            OrderedFloat(3.0),
            OrderedFloat(f64::NAN),
            OrderedFloat(1.0),
            OrderedFloat(2.0),
        ];
        vals.sort();
        // Non-NaN values come first in ascending order.
        assert_eq!(vals[0], OrderedFloat(1.0));
        assert_eq!(vals[1], OrderedFloat(2.0));
        assert_eq!(vals[2], OrderedFloat(3.0));
        assert!(vals[3].into_inner().is_nan(), "NaN should be the largest");
    }

    // -------- PartialReducerState::merge --------

    #[test]
    fn test_partial_state_merge_count() {
        let mut a = PartialReducerState::Count(3);
        a.merge(PartialReducerState::Count(5));
        match a {
            PartialReducerState::Count(n) => assert_eq!(n, 8),
            _ => panic!("expected Count"),
        }
    }

    #[test]
    fn test_partial_state_merge_sum() {
        let mut a = PartialReducerState::Sum { sum: 2.0, count: 1 };
        a.merge(PartialReducerState::Sum { sum: 3.0, count: 2 });
        match a {
            PartialReducerState::Sum { sum, count } => {
                assert!((sum - 5.0).abs() < f64::EPSILON);
                assert_eq!(count, 3);
            }
            _ => panic!("expected Sum"),
        }
    }

    #[test]
    fn test_partial_state_merge_min_handles_none() {
        // None ∪ Some → Some
        let mut a = PartialReducerState::Min(None);
        a.merge(PartialReducerState::Min(Some(OrderedFloat(2.0))));
        match a {
            PartialReducerState::Min(Some(v)) => {
                assert!((v.into_inner() - 2.0).abs() < f64::EPSILON);
            }
            _ => panic!("expected Min(Some)"),
        }

        // Some(1.0) ∪ Some(2.0) → Some(1.0)
        let mut a = PartialReducerState::Min(Some(OrderedFloat(1.0)));
        a.merge(PartialReducerState::Min(Some(OrderedFloat(2.0))));
        match a {
            PartialReducerState::Min(Some(v)) => {
                assert!((v.into_inner() - 1.0).abs() < f64::EPSILON);
            }
            _ => panic!("expected Min(Some)"),
        }

        // Some ∪ None → Some (identity)
        let mut a = PartialReducerState::Min(Some(OrderedFloat(7.5)));
        a.merge(PartialReducerState::Min(None));
        match a {
            PartialReducerState::Min(Some(v)) => {
                assert!((v.into_inner() - 7.5).abs() < f64::EPSILON);
            }
            _ => panic!("expected Min(Some)"),
        }
    }

    #[test]
    fn test_partial_state_merge_max_handles_none() {
        // None ∪ Some → Some
        let mut a = PartialReducerState::Max(None);
        a.merge(PartialReducerState::Max(Some(OrderedFloat(2.0))));
        match a {
            PartialReducerState::Max(Some(v)) => {
                assert!((v.into_inner() - 2.0).abs() < f64::EPSILON);
            }
            _ => panic!("expected Max(Some)"),
        }

        // Some(1.0) ∪ Some(2.0) → Some(2.0)
        let mut a = PartialReducerState::Max(Some(OrderedFloat(1.0)));
        a.merge(PartialReducerState::Max(Some(OrderedFloat(2.0))));
        match a {
            PartialReducerState::Max(Some(v)) => {
                assert!((v.into_inner() - 2.0).abs() < f64::EPSILON);
            }
            _ => panic!("expected Max(Some)"),
        }
    }

    #[test]
    fn test_partial_state_merge_hll_associative() {
        // Two shards add disjoint sets. merge(a, b) and merge(b, a) must
        // produce identical cardinalities, both within 2% of ground truth.
        // Use PartialReducerState::clone() (implemented manually via
        // Hll::from_bytes) to obtain independent copies for the reverse merge.
        let mut a = Hll::new_sparse();
        let mut b = Hll::new_sparse();
        for i in 0..1000u32 {
            a.add(&i.to_le_bytes());
        }
        for i in 1000..2000u32 {
            b.add(&i.to_le_bytes());
        }
        let truth = 2000.0;

        let state_a = PartialReducerState::CountDistinct(a);
        let state_b = PartialReducerState::CountDistinct(b);

        let mut left = state_a.clone();
        left.merge(state_b.clone());

        let mut right = state_b;
        right.merge(state_a);

        let (est_l, est_r) = match (&left, &right) {
            (PartialReducerState::CountDistinct(l), PartialReducerState::CountDistinct(r)) => {
                (l.count(), r.count())
            }
            _ => panic!("expected CountDistinct"),
        };

        assert_eq!(est_l, est_r, "HLL merge must be commutative");
        let err = (est_l as f64 - truth).abs() / truth;
        assert!(err < 0.02, "HLL error {} should be < 2% of {}", err, truth);
    }

    #[test]
    fn test_partial_state_merge_type_mismatch_is_noop() {
        // Type mismatch leaves self unchanged (T-152-01-03).
        let mut a = PartialReducerState::Count(42);
        a.merge(PartialReducerState::Min(Some(OrderedFloat(99.0))));
        match a {
            PartialReducerState::Count(n) => assert_eq!(n, 42),
            _ => panic!("expected Count unchanged"),
        }
    }

    // -------- PartialReducerState::finalize --------

    #[test]
    fn test_partial_state_finalize_avg_divides() {
        let s = PartialReducerState::Sum {
            sum: 10.0,
            count: 4,
        };
        match s.finalize(true) {
            AggregateValue::Float(v) => {
                assert!((v.into_inner() - 2.5).abs() < f64::EPSILON);
            }
            other => panic!("expected Float(2.5), got {:?}", other),
        }
    }

    #[test]
    fn test_partial_state_finalize_avg_zero_count_returns_null() {
        let s = PartialReducerState::Sum { sum: 0.0, count: 0 };
        match s.finalize(true) {
            AggregateValue::Null => {}
            other => panic!("expected Null, got {:?}", other),
        }
    }

    #[test]
    fn test_partial_state_finalize_sum_returns_raw_sum() {
        let s = PartialReducerState::Sum {
            sum: 10.0,
            count: 4,
        };
        match s.finalize(false) {
            AggregateValue::Float(v) => {
                assert!((v.into_inner() - 10.0).abs() < f64::EPSILON);
            }
            other => panic!("expected Float(10.0), got {:?}", other),
        }
    }

    #[test]
    fn test_partial_state_finalize_count_returns_int() {
        let s = PartialReducerState::Count(7);
        match s.finalize(false) {
            AggregateValue::Int(n) => assert_eq!(n, 7),
            other => panic!("expected Int(7), got {:?}", other),
        }
    }

    #[test]
    fn test_partial_state_finalize_min_max_none_is_null() {
        assert!(matches!(
            PartialReducerState::Min(None).finalize(false),
            AggregateValue::Null
        ));
        assert!(matches!(
            PartialReducerState::Max(None).finalize(false),
            AggregateValue::Null
        ));
    }

    #[test]
    fn test_partial_state_finalize_count_distinct_returns_int() {
        let mut hll = Hll::new_sparse();
        for i in 0..100u32 {
            hll.add(&i.to_le_bytes());
        }
        let s = PartialReducerState::CountDistinct(hll);
        match s.finalize(false) {
            AggregateValue::Int(n) => {
                // Within 2% of 100.
                let err = (n as f64 - 100.0).abs() / 100.0;
                assert!(err < 0.02, "HLL count error {} too high", err);
            }
            other => panic!("expected Int, got {:?}", other),
        }
    }

    // ============================================================
    // execute_pipeline tests (Task 2)
    // ============================================================

    /// Build a row with string fields.
    fn row_str(pairs: &[(&[u8], &[u8])]) -> AggregateRow {
        let mut row: AggregateRow = SmallVec::new();
        for (k, v) in pairs {
            row.push((
                Bytes::copy_from_slice(k),
                AggregateValue::Str(Bytes::copy_from_slice(v)),
            ));
        }
        row
    }

    /// Build a row with string + numeric fields.
    fn row_mixed(string_fields: &[(&[u8], &[u8])], float_fields: &[(&[u8], f64)]) -> AggregateRow {
        let mut row: AggregateRow = SmallVec::new();
        for (k, v) in string_fields {
            row.push((
                Bytes::copy_from_slice(k),
                AggregateValue::Str(Bytes::copy_from_slice(v)),
            ));
        }
        for (k, f) in float_fields {
            row.push((
                Bytes::copy_from_slice(k),
                AggregateValue::Float(OrderedFloat(*f)),
            ));
        }
        row
    }

    /// Extract a numeric value from an output row.
    fn row_num(row: &AggregateRow, field: &[u8]) -> Option<f64> {
        for (f, v) in row {
            if f.as_ref() == field {
                return match v {
                    AggregateValue::Int(n) => Some(*n as f64),
                    AggregateValue::Float(of) => Some(of.into_inner()),
                    AggregateValue::Null => None,
                    AggregateValue::Str(b) => std::str::from_utf8(b).ok()?.parse().ok(),
                };
            }
        }
        None
    }

    /// Extract a string value from an output row.
    fn row_str_val(row: &AggregateRow, field: &[u8]) -> Option<Bytes> {
        for (f, v) in row {
            if f.as_ref() == field {
                return match v {
                    AggregateValue::Str(b) => Some(b.clone()),
                    _ => None,
                };
            }
        }
        None
    }

    #[test]
    fn test_execute_pipeline_apply_errors() {
        let rows = vec![row_str(&[(b"@a", b"1")])];
        let pipeline = vec![AggregateStep::Apply {
            expr: Bytes::from_static(b"@a+1"),
            alias: Bytes::from_static(b"b"),
        }];
        let err = execute_pipeline(rows, &pipeline).expect_err("APPLY must error");
        assert_eq!(
            err,
            Bytes::from_static(b"ERR APPLY stage not supported in v1")
        );
    }

    #[test]
    fn test_execute_pipeline_limit() {
        let rows: Vec<AggregateRow> = (0..10)
            .map(|i| row_mixed(&[], &[(b"@i", i as f64)]))
            .collect();
        let pipeline = vec![AggregateStep::Limit {
            offset: 2,
            count: 3,
        }];
        let out = execute_pipeline(rows, &pipeline).expect("limit ok");
        assert_eq!(out.len(), 3);
        assert_eq!(row_num(&out[0], b"@i"), Some(2.0));
        assert_eq!(row_num(&out[1], b"@i"), Some(3.0));
        assert_eq!(row_num(&out[2], b"@i"), Some(4.0));
    }

    #[test]
    fn test_execute_pipeline_limit_past_end() {
        let rows: Vec<AggregateRow> = (0..5)
            .map(|i| row_mixed(&[], &[(b"@i", i as f64)]))
            .collect();
        let pipeline = vec![AggregateStep::Limit {
            offset: 10,
            count: 3,
        }];
        let out = execute_pipeline(rows, &pipeline).expect("limit past end ok");
        assert!(out.is_empty());
    }

    #[test]
    fn test_execute_pipeline_sortby_desc_numeric() {
        let rows: Vec<AggregateRow> = [3.0, 1.0, 4.0, 2.0]
            .iter()
            .map(|&v| row_mixed(&[], &[(b"@score", v)]))
            .collect();
        let mut keys: SmallVec<[(Bytes, SortOrder); 4]> = SmallVec::new();
        keys.push((Bytes::from_static(b"@score"), SortOrder::Desc));
        let pipeline = vec![AggregateStep::SortBy { keys, max: None }];
        let out = execute_pipeline(rows, &pipeline).expect("sortby ok");
        let ordered: Vec<f64> = out
            .iter()
            .map(|r| row_num(r, b"@score").unwrap_or(0.0))
            .collect();
        assert_eq!(ordered, vec![4.0, 3.0, 2.0, 1.0]);
    }

    #[test]
    fn test_execute_pipeline_sortby_stable_ties() {
        // Two rows with identical sort keys but distinct ids; stable sort
        // preserves input order (id 1 before id 2).
        let r1 = row_mixed(&[(b"@id", b"1")], &[(b"@score", 5.0)]);
        let r2 = row_mixed(&[(b"@id", b"2")], &[(b"@score", 5.0)]);
        let rows = vec![r1, r2];
        let mut keys: SmallVec<[(Bytes, SortOrder); 4]> = SmallVec::new();
        keys.push((Bytes::from_static(b"@score"), SortOrder::Asc));
        let pipeline = vec![AggregateStep::SortBy { keys, max: None }];
        let out = execute_pipeline(rows, &pipeline).expect("sortby ok");
        assert_eq!(row_str_val(&out[0], b"@id").as_deref(), Some(b"1".as_ref()));
        assert_eq!(row_str_val(&out[1], b"@id").as_deref(), Some(b"2".as_ref()));
    }

    #[test]
    fn test_execute_pipeline_groupby_count() {
        let statuses = ["open", "open", "closed", "open", "closed"];
        let rows: Vec<AggregateRow> = statuses
            .iter()
            .map(|s| row_str(&[(b"@status", s.as_bytes())]))
            .collect();
        let mut fields: SmallVec<[Bytes; 4]> = SmallVec::new();
        fields.push(Bytes::from_static(b"@status"));
        let reducers = vec![ReducerSpec {
            fn_name: ReducerFn::Count,
            field: None,
            alias: Bytes::from_static(b"cnt"),
        }];
        let pipeline = vec![AggregateStep::GroupBy { fields, reducers }];
        let out = execute_pipeline(rows, &pipeline).expect("groupby ok");
        assert_eq!(out.len(), 2);

        let mut open_count = None;
        let mut closed_count = None;
        for row in &out {
            let status = row_str_val(row, b"@status");
            let cnt = row_num(row, b"cnt");
            match status.as_deref() {
                Some(b"open") => open_count = cnt,
                Some(b"closed") => closed_count = cnt,
                other => panic!("unexpected status {:?}", other),
            }
        }
        assert_eq!(open_count, Some(3.0));
        assert_eq!(closed_count, Some(2.0));
    }

    #[test]
    fn test_execute_pipeline_groupby_sum() {
        let rows = vec![
            row_mixed(&[(b"@priority", b"high")], &[(b"@value", 10.0)]),
            row_mixed(&[(b"@priority", b"high")], &[(b"@value", 20.0)]),
            row_mixed(&[(b"@priority", b"low")], &[(b"@value", 5.0)]),
            row_mixed(&[(b"@priority", b"low")], &[(b"@value", 3.0)]),
        ];
        let mut fields: SmallVec<[Bytes; 4]> = SmallVec::new();
        fields.push(Bytes::from_static(b"@priority"));
        let reducers = vec![ReducerSpec {
            fn_name: ReducerFn::Sum,
            field: Some(Bytes::from_static(b"@value")),
            alias: Bytes::from_static(b"total"),
        }];
        let pipeline = vec![AggregateStep::GroupBy { fields, reducers }];
        let out = execute_pipeline(rows, &pipeline).expect("groupby sum ok");
        assert_eq!(out.len(), 2);

        let mut high = None;
        let mut low = None;
        for row in &out {
            match row_str_val(row, b"@priority").as_deref() {
                Some(b"high") => high = row_num(row, b"total"),
                Some(b"low") => low = row_num(row, b"total"),
                other => panic!("unexpected priority {:?}", other),
            }
        }
        assert_eq!(high, Some(30.0));
        assert_eq!(low, Some(8.0));
    }

    #[test]
    fn test_execute_pipeline_groupby_avg_count_distinct() {
        // Four rows, group by category, AVG the @value field and COUNT_DISTINCT
        // the @user field.
        let rows = vec![
            row_mixed(&[(b"@cat", b"a"), (b"@user", b"u1")], &[(b"@v", 2.0)]),
            row_mixed(&[(b"@cat", b"a"), (b"@user", b"u2")], &[(b"@v", 4.0)]),
            row_mixed(&[(b"@cat", b"a"), (b"@user", b"u1")], &[(b"@v", 6.0)]),
            row_mixed(&[(b"@cat", b"b"), (b"@user", b"u3")], &[(b"@v", 10.0)]),
        ];
        let mut fields: SmallVec<[Bytes; 4]> = SmallVec::new();
        fields.push(Bytes::from_static(b"@cat"));
        let reducers = vec![
            ReducerSpec {
                fn_name: ReducerFn::Avg,
                field: Some(Bytes::from_static(b"@v")),
                alias: Bytes::from_static(b"avg_v"),
            },
            ReducerSpec {
                fn_name: ReducerFn::CountDistinct,
                field: Some(Bytes::from_static(b"@user")),
                alias: Bytes::from_static(b"uniq_users"),
            },
        ];
        let pipeline = vec![AggregateStep::GroupBy { fields, reducers }];
        let out = execute_pipeline(rows, &pipeline).expect("groupby ok");
        assert_eq!(out.len(), 2);

        for row in &out {
            let cat = row_str_val(row, b"@cat");
            let avg = row_num(row, b"avg_v");
            let uniq = row_num(row, b"uniq_users");
            match cat.as_deref() {
                Some(b"a") => {
                    // (2+4+6)/3 = 4.0
                    assert!((avg.unwrap_or(0.0) - 4.0).abs() < 1e-6);
                    // 2 distinct users (u1 appears twice)
                    let err = (uniq.unwrap_or(0.0) - 2.0).abs() / 2.0;
                    assert!(err < 0.02, "HLL error {} for cat=a", err);
                }
                Some(b"b") => {
                    assert!((avg.unwrap_or(0.0) - 10.0).abs() < 1e-6);
                    // 1 distinct user
                    let err = (uniq.unwrap_or(0.0) - 1.0).abs();
                    assert!(err < 0.1, "HLL error {} for cat=b", err);
                }
                other => panic!("unexpected cat {:?}", other),
            }
        }
    }

    #[test]
    fn test_execute_pipeline_multi_stage() {
        // Compose Filter (no-op) -> GroupBy -> SortBy DESC by count -> Limit 0,2.
        use crate::vector::filter::expression::FilterExpr;

        let statuses = ["a", "b", "a", "c", "a", "b", "d"];
        let rows: Vec<AggregateRow> = statuses
            .iter()
            .map(|s| row_str(&[(b"@g", s.as_bytes())]))
            .collect();

        let filter_expr = Arc::new(FilterExpr::TagEq {
            field: Bytes::from_static(b"@g"),
            value: Bytes::from_static(b"any"),
        });

        let mut group_fields: SmallVec<[Bytes; 4]> = SmallVec::new();
        group_fields.push(Bytes::from_static(b"@g"));
        let group_reducers = vec![ReducerSpec {
            fn_name: ReducerFn::Count,
            field: None,
            alias: Bytes::from_static(b"cnt"),
        }];

        let mut sort_keys: SmallVec<[(Bytes, SortOrder); 4]> = SmallVec::new();
        sort_keys.push((Bytes::from_static(b"cnt"), SortOrder::Desc));

        let pipeline = vec![
            AggregateStep::Filter(filter_expr),
            AggregateStep::GroupBy {
                fields: group_fields,
                reducers: group_reducers,
            },
            AggregateStep::SortBy {
                keys: sort_keys,
                max: None,
            },
            AggregateStep::Limit {
                offset: 0,
                count: 2,
            },
        ];
        let out = execute_pipeline(rows, &pipeline).expect("pipeline ok");
        assert_eq!(out.len(), 2);
        // Top group is "a" with count 3.
        assert_eq!(row_str_val(&out[0], b"@g").as_deref(), Some(b"a".as_ref()));
        assert_eq!(row_num(&out[0], b"cnt"), Some(3.0));
        // Second group is "b" with count 2.
        assert_eq!(row_str_val(&out[1], b"@g").as_deref(), Some(b"b".as_ref()));
        assert_eq!(row_num(&out[1], b"cnt"), Some(2.0));
    }

    #[test]
    fn test_execute_pipeline_groupby_cardinality_cap() {
        // Generate > GROUPBY_CARDINALITY_LIMIT distinct keys — should error.
        let n = GROUPBY_CARDINALITY_LIMIT + 5;
        let mut rows: Vec<AggregateRow> = Vec::with_capacity(n);
        for i in 0..n {
            let key = format!("k{}", i);
            rows.push(row_str(&[(b"@g", key.as_bytes())]));
        }
        let mut fields: SmallVec<[Bytes; 4]> = SmallVec::new();
        fields.push(Bytes::from_static(b"@g"));
        let reducers = vec![ReducerSpec {
            fn_name: ReducerFn::Count,
            field: None,
            alias: Bytes::from_static(b"cnt"),
        }];
        let pipeline = vec![AggregateStep::GroupBy { fields, reducers }];
        let err = execute_pipeline(rows, &pipeline).expect_err("should cap");
        assert_eq!(
            err,
            Bytes::from_static(b"ERR GROUPBY cardinality limit exceeded")
        );
    }

    #[test]
    fn test_execute_pipeline_groupby_rejects_on_new_key_past_cap() {
        // Fill to exactly GROUPBY_CARDINALITY_LIMIT distinct keys, then:
        //   - Re-seeing one of the existing keys must NOT error.
        //   - The (N+1)th distinct key must error before insert.
        let n = GROUPBY_CARDINALITY_LIMIT;
        let mut rows: Vec<AggregateRow> = Vec::with_capacity(n);
        for i in 0..n {
            let key = format!("k{}", i);
            rows.push(row_str(&[(b"@g", key.as_bytes())]));
        }
        // One re-seen key — must still succeed.
        rows.push(row_str(&[(b"@g", b"k0")]));

        let mut fields: SmallVec<[Bytes; 4]> = SmallVec::new();
        fields.push(Bytes::from_static(b"@g"));
        let reducers = vec![ReducerSpec {
            fn_name: ReducerFn::Count,
            field: None,
            alias: Bytes::from_static(b"cnt"),
        }];
        let pipeline = vec![AggregateStep::GroupBy {
            fields: fields.clone(),
            reducers: reducers.clone(),
        }];
        let out = execute_pipeline(rows.clone(), &pipeline).expect("re-seen key ok at cap");
        assert_eq!(out.len(), n);

        // Now push ONE new distinct key — must error.
        rows.push(row_str(&[(b"@g", b"overflow")]));
        let pipeline2 = vec![AggregateStep::GroupBy { fields, reducers }];
        let err = execute_pipeline(rows, &pipeline2).expect_err("should cap on (N+1)th");
        assert_eq!(
            err,
            Bytes::from_static(b"ERR GROUPBY cardinality limit exceeded")
        );
    }

    // ============================================================
    // merge_partial_states tests (Task 3)
    // ============================================================

    fn group_key(parts: &[&[u8]]) -> GroupKey {
        let mut k: GroupKey = SmallVec::new();
        for p in parts {
            k.push(Bytes::copy_from_slice(p));
        }
        k
    }

    fn count_states(n: u64) -> SmallVec<[PartialReducerState; 4]> {
        let mut v: SmallVec<[PartialReducerState; 4]> = SmallVec::new();
        v.push(PartialReducerState::Count(n));
        v
    }

    fn groupby_fields_single(field: &[u8]) -> Vec<Bytes> {
        vec![Bytes::copy_from_slice(field)]
    }

    fn count_reducer(alias: &[u8]) -> Vec<ReducerSpec> {
        vec![ReducerSpec {
            fn_name: ReducerFn::Count,
            field: None,
            alias: Bytes::copy_from_slice(alias),
        }]
    }

    #[test]
    fn test_merge_partial_states_associative_count() {
        let key_a = group_key(&[b"a"]);
        let key_b = group_key(&[b"b"]);
        let key_c = group_key(&[b"c"]);

        let shard0: ShardPartial = vec![
            (key_a.clone(), count_states(3)),
            (key_b.clone(), count_states(2)),
        ];
        let shard1: ShardPartial = vec![
            (key_a.clone(), count_states(5)),
            (key_c.clone(), count_states(1)),
        ];

        let fields = groupby_fields_single(b"@g");
        let reducers = count_reducer(b"cnt");

        let out = merge_partial_states(vec![shard0, shard1], &fields, &reducers);
        assert_eq!(out.len(), 3);

        let mut found_a = None;
        let mut found_b = None;
        let mut found_c = None;
        for row in &out {
            let g = row_str_val(row, b"@g");
            let c = row_num(row, b"cnt");
            match g.as_deref() {
                Some(b"a") => found_a = c,
                Some(b"b") => found_b = c,
                Some(b"c") => found_c = c,
                other => panic!("unexpected group {:?}", other),
            }
        }
        assert_eq!(found_a, Some(8.0));
        assert_eq!(found_b, Some(2.0));
        assert_eq!(found_c, Some(1.0));
    }

    #[test]
    fn test_merge_partial_states_commutative() {
        // Swap shard order — counts must be unchanged (associative + commutative).
        let key_a = group_key(&[b"a"]);
        let key_b = group_key(&[b"b"]);
        let key_c = group_key(&[b"c"]);

        let shard0: ShardPartial = vec![
            (key_a.clone(), count_states(3)),
            (key_b.clone(), count_states(2)),
        ];
        let shard1: ShardPartial = vec![
            (key_a.clone(), count_states(5)),
            (key_c.clone(), count_states(1)),
        ];

        let fields = groupby_fields_single(b"@g");
        let reducers = count_reducer(b"cnt");

        // Reversed order.
        let out = merge_partial_states(vec![shard1, shard0], &fields, &reducers);
        assert_eq!(out.len(), 3);

        let mut found_a = None;
        let mut found_b = None;
        let mut found_c = None;
        for row in &out {
            let g = row_str_val(row, b"@g");
            let c = row_num(row, b"cnt");
            match g.as_deref() {
                Some(b"a") => found_a = c,
                Some(b"b") => found_b = c,
                Some(b"c") => found_c = c,
                other => panic!("unexpected group {:?}", other),
            }
        }
        assert_eq!(found_a, Some(8.0));
        assert_eq!(found_b, Some(2.0));
        assert_eq!(found_c, Some(1.0));
    }

    #[test]
    fn test_merge_partial_states_empty_input() {
        let fields = groupby_fields_single(b"@g");
        let reducers = count_reducer(b"cnt");
        let out: Vec<AggregateRow> = merge_partial_states(vec![vec![], vec![]], &fields, &reducers);
        assert!(out.is_empty());
    }

    #[test]
    fn test_merge_partial_states_handles_only_one_shard_has_group() {
        let shard0: ShardPartial = vec![(group_key(&[b"a"]), count_states(4))];
        let shard1: ShardPartial = vec![(group_key(&[b"b"]), count_states(7))];

        let fields = groupby_fields_single(b"@g");
        let reducers = count_reducer(b"cnt");

        let out = merge_partial_states(vec![shard0, shard1], &fields, &reducers);
        assert_eq!(out.len(), 2);
        let mut found_a = None;
        let mut found_b = None;
        for row in &out {
            match row_str_val(row, b"@g").as_deref() {
                Some(b"a") => found_a = row_num(row, b"cnt"),
                Some(b"b") => found_b = row_num(row, b"cnt"),
                other => panic!("unexpected group {:?}", other),
            }
        }
        assert_eq!(found_a, Some(4.0));
        assert_eq!(found_b, Some(7.0));
    }

    #[test]
    fn test_merge_partial_states_preserves_group_field_names() {
        // Two GroupBy fields + one reducer alias — output must have all three
        // columns in declared order: @f1, @f2, then cnt.
        let mut fields: Vec<Bytes> = Vec::new();
        fields.push(Bytes::from_static(b"@f1"));
        fields.push(Bytes::from_static(b"@f2"));
        let reducers = count_reducer(b"cnt");

        let key = group_key(&[b"x", b"y"]);
        let shard0: ShardPartial = vec![(key.clone(), count_states(1))];
        let shard1: ShardPartial = vec![(key, count_states(2))];

        let out = merge_partial_states(vec![shard0, shard1], &fields, &reducers);
        assert_eq!(out.len(), 1);
        let row = &out[0];
        // Column 0 = @f1 = "x", column 1 = @f2 = "y", column 2 = cnt = 3.
        assert_eq!(row.len(), 3);
        assert_eq!(row[0].0.as_ref(), b"@f1".as_ref());
        match &row[0].1 {
            AggregateValue::Str(b) => assert_eq!(b.as_ref(), b"x".as_ref()),
            other => panic!("expected Str(x), got {:?}", other),
        }
        assert_eq!(row[1].0.as_ref(), b"@f2".as_ref());
        match &row[1].1 {
            AggregateValue::Str(b) => assert_eq!(b.as_ref(), b"y".as_ref()),
            other => panic!("expected Str(y), got {:?}", other),
        }
        assert_eq!(row[2].0.as_ref(), b"cnt".as_ref());
        match &row[2].1 {
            AggregateValue::Int(n) => assert_eq!(*n, 3),
            other => panic!("expected Int(3), got {:?}", other),
        }
    }

    #[test]
    fn test_merge_partial_states_hll_across_shards() {
        // Each shard builds its own HLL with distinct users for the same key.
        // After coordinator merge the cardinality must be ≈ the sum of
        // distinct users (not just max).
        let mut hll_a = Hll::new_sparse();
        for i in 0..500u32 {
            hll_a.add(&i.to_le_bytes());
        }
        let mut hll_b = Hll::new_sparse();
        for i in 500..1500u32 {
            hll_b.add(&i.to_le_bytes());
        }

        let mut sa: SmallVec<[PartialReducerState; 4]> = SmallVec::new();
        sa.push(PartialReducerState::CountDistinct(hll_a));
        let mut sb: SmallVec<[PartialReducerState; 4]> = SmallVec::new();
        sb.push(PartialReducerState::CountDistinct(hll_b));

        let key = group_key(&[b"k"]);
        let shard0: ShardPartial = vec![(key.clone(), sa)];
        let shard1: ShardPartial = vec![(key, sb)];

        let fields = groupby_fields_single(b"@g");
        let reducers = vec![ReducerSpec {
            fn_name: ReducerFn::CountDistinct,
            field: Some(Bytes::from_static(b"@user")),
            alias: Bytes::from_static(b"uniq"),
        }];

        let out = merge_partial_states(vec![shard0, shard1], &fields, &reducers);
        assert_eq!(out.len(), 1);
        let est = row_num(&out[0], b"uniq").expect("uniq value");
        let truth = 1500.0;
        let err = (est - truth).abs() / truth;
        assert!(err < 0.02, "HLL cross-shard error {} too high", err);
    }
}
