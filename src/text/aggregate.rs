//! FT.AGGREGATE pipeline core (Phase 152).
//!
//! Per CONTEXT.md decisions:
//!   - **D-01:** Pipeline is `Vec<AggregateStep>` executed iteratively — not a trait
//!     object, not a streaming iterator. Enum dispatch keeps control flow explicit
//!     and shard-serialization-friendly.
//!   - **D-02:** Rows flow between stages as `Vec<AggregateRow>` where
//!     `AggregateRow = SmallVec<[(Bytes, AggregateValue); 8]>` — typical rows have
//!     ≤ 8 fields so the inline buffer avoids heap traffic.
//!   - **D-03:** v1 reducer set is COUNT / COUNT_DISTINCT / SUM / AVG / MIN / MAX.
//!     TOLIST, STDDEV, QUANTILE, MEDIAN are deferred (AGG-05+).
//!   - **D-04:** APPLY stage is parsed but `execute_pipeline` returns
//!     `Frame::Error("ERR APPLY stage not supported in v1")`. The pipeline shape
//!     is locked now; the expression evaluator lands post-v1.
//!   - **D-05/D-06:** Reducer partial states are reducer-specific and all merges
//!     are associative + commutative, enabling shard → coordinator fan-in.
//!
//! **Source-audit override of D-06:** D-06 literally specifies "fixed 2^12
//! registers = 4 KiB" for COUNT_DISTINCT, but Moon already ships a
//! production-grade HyperLogLog in `crate::storage::hll::Hll` at P=14 (16384
//! registers, ~0.81% error) with Redis-HYLL-wire-compatible dense/sparse
//! encodings. This module **reuses** that implementation — zero new unsafe,
//! zero new format to maintain, zero new bugs. See RESEARCH.md §Summary.
//!
//! **Non-goals for this module:**
//!   - No FT.AGGREGATE parser (lives in Plan 02).
//!   - No scatter-gather dispatch (lives in Plan 03).
//!   - No I/O, no locking, no shard-local hash-store reads.
//!
//! The only external types this module touches are:
//!   - `bytes::Bytes` — zero-copy field names and string values.
//!   - `ordered_float::OrderedFloat<f64>` — NaN-safe numeric compares for SORTBY.
//!   - `smallvec::SmallVec` — inline row buffer.
//!   - `crate::storage::hll::Hll` — COUNT_DISTINCT sketch (reuse).
//!   - `crate::vector::filter::expression::FilterExpr` — FILTER step AST (Plan 02
//!     passes the parsed AST in; this module does not parse).

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
                    (Some(x), Some(y)) => {
                        Some(OrderedFloat(x.into_inner().min(y.into_inner())))
                    }
                    (None, v) | (v, None) => v,
                };
            }
            (Self::Max(a), Self::Max(b)) => {
                *a = match (*a, b) {
                    (Some(x), Some(y)) => {
                        Some(OrderedFloat(x.into_inner().max(y.into_inner())))
                    }
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
            (
                PartialReducerState::CountDistinct(l),
                PartialReducerState::CountDistinct(r),
            ) => (l.count(), r.count()),
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
        let s = PartialReducerState::Sum { sum: 10.0, count: 4 };
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
        let s = PartialReducerState::Sum { sum: 10.0, count: 4 };
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
}
