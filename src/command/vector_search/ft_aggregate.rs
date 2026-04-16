//! FT.AGGREGATE command handler (Phase 152, Plan 02).
//!
//! RediSearch-compatible grammar:
//! ```text
//!   FT.AGGREGATE idx query
//!     [GROUPBY <n> @f1 @f2 ... REDUCE FN <argc> <args...> AS <alias> ...]
//!     [SORTBY <n> @f1 [ASC|DESC] ...]
//!     [APPLY <expr> AS <alias>]   -- parse-time Frame::Error per D-04
//!     [LIMIT offset count]
//!     [FILTER "<expr>"]           -- v1: parsed & consumed as NO-OP. Primary filter path
//!                                    in v1 is the query prefix (e.g. "@status:{open}").
//!                                    Top-level FILTER-stage evaluation is deferred (I11).
//! ```
//!
//! # Design invariants
//! - Per **D-01** the pipeline is a flat `Vec<AggregateStep>` produced by the parser and
//!   executed iteratively by `crate::text::aggregate::execute_pipeline`.
//! - Per **D-04** APPLY is rejected **AT PARSE TIME** (not execute time) so clients fail
//!   fast. The pipeline never carries an `Apply` variant from this module.
//! - Per **D-19** FT.AGGREGATE has its own dispatch entry. **W8:** the dispatch table is
//!   a linear `else-if` chain in `spsc_handler::dispatch_vector_command` — NOT a `phf`
//!   map. D-19's phf reference is superseded by RESEARCH §ARM's established pattern.
//! - Per **D-20** ACL category is `@read + @search` (same as FT.SEARCH).
//! - Per **I11 / AGG-02** top-level `FILTER` clause is PARSED (tokens consumed) but the
//!   v1 executor treats it as a NO-OP; the primary filter path is the query-prefix
//!   (e.g. `@status:{open}`). Top-level FILTER-stage evaluation is deferred.
//!
//! # DoS mitigations (threat model T-152-02-01)
//! - `AGGREGATE_LIMIT_CAP = 100_000` clamps LIMIT count at parse time.
//! - `GROUPBY_CARDINALITY_LIMIT` (10_000) is enforced by `execute_pipeline` in Plan 01.

#![cfg(feature = "text-index")]

use bytes::Bytes;
use smallvec::SmallVec;

use crate::protocol::Frame;
use crate::text::aggregate::{AggregateStep, ReducerFn, ReducerSpec, SortOrder};

use super::{extract_bulk, matches_keyword, parse_u32};

/// Upper bound on LIMIT count (DoS cap). Mirrors the hard cap used across
/// FT.SEARCH's pagination path — clients can paginate further, but a single
/// request never materialises more than this many rows.
pub const AGGREGATE_LIMIT_CAP: usize = 100_000;

// ---------------------------------------------------------------------------
// Parser — FT.AGGREGATE RESP args
// ---------------------------------------------------------------------------

/// Parsed FT.AGGREGATE arguments.
///
/// Produced by [`parse_aggregate_args`] and consumed by `ft_aggregate` (Task 2).
/// `pipeline` is the flat `Vec<AggregateStep>` the executor walks; `query`
/// is the text-query string passed to the BM25 entry (`"*"` or empty means
/// match-all, anything else is a bare-terms / `@field:(terms)` text query).
#[derive(Debug, Clone)]
pub struct AggregateArgs {
    pub index_name: Bytes,
    pub query: Bytes,
    pub pipeline: Vec<AggregateStep>,
}

/// Parse the raw RESP args for FT.AGGREGATE.
///
/// Returns `Err(Frame::Error(...))` on any syntax error — caller propagates
/// verbatim. This function never panics on malformed input (threat T-152-02-04).
pub fn parse_aggregate_args(args: &[Frame]) -> Result<AggregateArgs, Frame> {
    if args.len() < 2 {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'FT.AGGREGATE' command",
        )));
    }
    let index_name = extract_bulk(&args[0])
        .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid index name")))?;
    let query = extract_bulk(&args[1])
        .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid query string")))?;

    let mut pipeline: Vec<AggregateStep> = Vec::new();
    let mut i = 2;
    while i < args.len() {
        if matches_keyword(&args[i], b"GROUPBY") {
            i = parse_groupby(args, i + 1, &mut pipeline)?;
        } else if matches_keyword(&args[i], b"SORTBY") {
            i = parse_sortby(args, i + 1, &mut pipeline)?;
        } else if matches_keyword(&args[i], b"LIMIT") {
            i = parse_limit(args, i + 1, &mut pipeline)?;
        } else if matches_keyword(&args[i], b"APPLY") {
            // D-04: reject at parse time for fast failure — never mutate the pipeline.
            return Err(Frame::Error(Bytes::from_static(
                b"ERR APPLY stage not supported in v1",
            )));
        } else if matches_keyword(&args[i], b"FILTER") {
            // I11 / AGG-02: v1 consumes the FILTER arg but evaluation is NO-OP.
            // Primary filter path in v1 is the query-prefix. See module doc.
            i += 1;
            if i >= args.len() {
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR FILTER requires an expression",
                )));
            }
            // Validate the FILTER expression is a BulkString (consume it, no-op in v1).
            if extract_bulk(&args[i]).is_none() {
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR invalid FILTER expression",
                )));
            }
            i += 1;
        } else {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR unknown FT.AGGREGATE keyword",
            )));
        }
    }

    Ok(AggregateArgs {
        index_name,
        query,
        pipeline,
    })
}

fn parse_groupby(
    args: &[Frame],
    start: usize,
    out: &mut Vec<AggregateStep>,
) -> Result<usize, Frame> {
    if start >= args.len() {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR GROUPBY requires field count",
        )));
    }
    let n = parse_u32(&args[start])
        .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid GROUPBY field count")))?
        as usize;
    let fields_start = start + 1;
    if fields_start + n > args.len() {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR GROUPBY wrong number of arguments",
        )));
    }
    let mut fields: SmallVec<[Bytes; 4]> = SmallVec::new();
    for j in 0..n {
        let f = extract_bulk(&args[fields_start + j])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid GROUPBY field")))?;
        fields.push(f);
    }
    // Parse zero or more consecutive REDUCE clauses.
    let mut reducers: Vec<ReducerSpec> = Vec::new();
    let mut j = fields_start + n;
    while j < args.len() && matches_keyword(&args[j], b"REDUCE") {
        j = parse_reduce(args, j + 1, &mut reducers)?;
    }
    out.push(AggregateStep::GroupBy { fields, reducers });
    Ok(j)
}

fn parse_reduce(args: &[Frame], start: usize, out: &mut Vec<ReducerSpec>) -> Result<usize, Frame> {
    if start + 1 >= args.len() {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR REDUCE requires function and arg count",
        )));
    }
    let fn_bytes = extract_bulk(&args[start])
        .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid REDUCE function")))?;
    let fn_name = parse_reducer_fn(&fn_bytes)?;
    let argc = parse_u32(&args[start + 1])
        .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid REDUCE arg count")))?
        as usize;
    let args_start = start + 2;
    if args_start + argc > args.len() {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR REDUCE wrong number of arguments",
        )));
    }

    // Extract optional first arg as field (RediSearch convention: the reducer's
    // source field is the first positional arg after `argc`).
    let field: Option<Bytes> = if argc > 0 {
        extract_bulk(&args[args_start])
    } else {
        None
    };

    // Validate field-requirement per reducer kind.
    validate_reducer_field(fn_name, &field)?;

    // AS <alias> — optional per RediSearch; we auto-generate when absent so clients
    // that send `REDUCE COUNT 0` (no AS) still get a usable column name.
    let mut pos = args_start + argc;
    let alias: Bytes = if pos < args.len() && matches_keyword(&args[pos], b"AS") {
        pos += 1;
        if pos >= args.len() {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR REDUCE AS requires alias",
            )));
        }
        let a = extract_bulk(&args[pos])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid reducer alias")))?;
        pos += 1;
        a
    } else {
        auto_alias(fn_name, field.as_ref())
    };

    out.push(ReducerSpec {
        fn_name,
        field,
        alias,
    });
    Ok(pos)
}

/// Validate `field` presence per reducer kind.
fn validate_reducer_field(fn_name: ReducerFn, field: &Option<Bytes>) -> Result<(), Frame> {
    match (fn_name, field) {
        // COUNT takes 0 args by RediSearch convention; extra positional args (if any)
        // are ignored silently so `REDUCE COUNT 0 AS n` and `REDUCE COUNT 1 @x AS n`
        // are both accepted.
        (ReducerFn::Count, _) => Ok(()),
        (_, Some(_)) => Ok(()),
        (reducer, None) => {
            let msg: &[u8] = match reducer {
                ReducerFn::Sum => b"ERR SUM requires a field",
                ReducerFn::Avg => b"ERR AVG requires a field",
                ReducerFn::Min => b"ERR MIN requires a field",
                ReducerFn::Max => b"ERR MAX requires a field",
                ReducerFn::CountDistinct => b"ERR COUNT_DISTINCT requires a field",
                // Unreachable because Count is handled above; if a new variant is added
                // that allows None-field we want the error below to surface as a bug.
                ReducerFn::Count => b"ERR reducer requires a field",
            };
            Err(Frame::Error(Bytes::copy_from_slice(msg)))
        }
    }
}

/// Auto-generate a reducer alias when the client omits `AS <alias>`.
///
/// Conventions:
/// - COUNT → `count`
/// - SUM   → `sum_<field>` (leading `@` stripped)
/// - AVG   → `avg_<field>`
/// - MIN   → `min_<field>`
/// - MAX   → `max_<field>`
/// - COUNT_DISTINCT → `count_distinct_<field>`
fn auto_alias(fn_name: ReducerFn, field: Option<&Bytes>) -> Bytes {
    let bare: &[u8] = field
        .map(|f| {
            if f.starts_with(b"@") {
                &f[1..]
            } else {
                f.as_ref()
            }
        })
        .unwrap_or(b"");
    let prefix: &[u8] = match fn_name {
        ReducerFn::Count => return Bytes::from_static(b"count"),
        ReducerFn::Sum => b"sum_",
        ReducerFn::Avg => b"avg_",
        ReducerFn::Min => b"min_",
        ReducerFn::Max => b"max_",
        ReducerFn::CountDistinct => b"count_distinct_",
    };
    let mut out = Vec::with_capacity(prefix.len() + bare.len());
    out.extend_from_slice(prefix);
    out.extend_from_slice(bare);
    Bytes::from(out)
}

fn parse_reducer_fn(name: &[u8]) -> Result<ReducerFn, Frame> {
    if name.eq_ignore_ascii_case(b"COUNT") {
        Ok(ReducerFn::Count)
    } else if name.eq_ignore_ascii_case(b"SUM") {
        Ok(ReducerFn::Sum)
    } else if name.eq_ignore_ascii_case(b"AVG") {
        Ok(ReducerFn::Avg)
    } else if name.eq_ignore_ascii_case(b"MIN") {
        Ok(ReducerFn::Min)
    } else if name.eq_ignore_ascii_case(b"MAX") {
        Ok(ReducerFn::Max)
    } else if name.eq_ignore_ascii_case(b"COUNT_DISTINCT") {
        Ok(ReducerFn::CountDistinct)
    } else {
        let mut msg = b"ERR unknown REDUCE function: ".to_vec();
        msg.extend_from_slice(name);
        Err(Frame::Error(Bytes::from(msg)))
    }
}

fn parse_sortby(
    args: &[Frame],
    start: usize,
    out: &mut Vec<AggregateStep>,
) -> Result<usize, Frame> {
    if start >= args.len() {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR SORTBY requires arg count",
        )));
    }
    let n = parse_u32(&args[start])
        .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid SORTBY arg count")))?
        as usize;
    let mut pos = start + 1;
    let mut keys: SmallVec<[(Bytes, SortOrder); 4]> = SmallVec::new();
    let mut consumed = 0usize;
    while consumed < n && pos < args.len() {
        let field = extract_bulk(&args[pos])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid SORTBY field")))?;
        pos += 1;
        consumed += 1;
        // Optional ASC/DESC token (counts toward n if present).
        let order = if consumed < n && pos < args.len() {
            if matches_keyword(&args[pos], b"ASC") {
                pos += 1;
                consumed += 1;
                SortOrder::Asc
            } else if matches_keyword(&args[pos], b"DESC") {
                pos += 1;
                consumed += 1;
                SortOrder::Desc
            } else {
                SortOrder::Asc
            }
        } else {
            SortOrder::Asc
        };
        keys.push((field, order));
    }
    if consumed < n {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR SORTBY wrong number of arguments",
        )));
    }
    out.push(AggregateStep::SortBy { keys, max: None });
    Ok(pos)
}

fn parse_limit(args: &[Frame], start: usize, out: &mut Vec<AggregateStep>) -> Result<usize, Frame> {
    if start + 1 >= args.len() {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR LIMIT requires offset and count",
        )));
    }
    let offset = parse_u32(&args[start])
        .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid LIMIT offset")))?
        as usize;
    let raw_count = parse_u32(&args[start + 1])
        .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid LIMIT count")))?
        as usize;
    // T-152-02-01: clamp count at parse time so the executor never sees a
    // request that could materialise more rows than the DoS cap allows.
    let count = raw_count.min(AGGREGATE_LIMIT_CAP);
    out.push(AggregateStep::Limit { offset, count });
    Ok(start + 2)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Helpers --

    fn bulk(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    /// Build an args vector from a sequence of &[u8] slices.
    fn args_from(pieces: &[&[u8]]) -> Vec<Frame> {
        pieces.iter().map(|p| bulk(p)).collect()
    }

    // ==================================================================
    // parse_aggregate_args tests
    // ==================================================================

    #[test]
    fn test_parse_minimal_groupby() {
        // FT.AGGREGATE myidx "*" GROUPBY 1 @priority REDUCE COUNT 0 AS count
        let args = args_from(&[
            b"myidx", b"*", b"GROUPBY", b"1", b"@priority", b"REDUCE", b"COUNT", b"0", b"AS",
            b"count",
        ]);
        let parsed = parse_aggregate_args(&args).expect("parse ok");
        assert_eq!(parsed.index_name.as_ref(), b"myidx");
        assert_eq!(parsed.query.as_ref(), b"*");
        assert_eq!(parsed.pipeline.len(), 1);
        match &parsed.pipeline[0] {
            AggregateStep::GroupBy { fields, reducers } => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].as_ref(), b"@priority");
                assert_eq!(reducers.len(), 1);
                assert_eq!(reducers[0].fn_name, ReducerFn::Count);
                assert!(reducers[0].field.is_none());
                assert_eq!(reducers[0].alias.as_ref(), b"count");
            }
            _ => panic!("expected GroupBy"),
        }
    }

    #[test]
    fn test_parse_full_example_from_context() {
        // FT.AGGREGATE myidx "@status:{open}" GROUPBY 1 @priority REDUCE COUNT 0 AS count
        //   SORTBY 2 @count DESC
        let args = args_from(&[
            b"myidx",
            b"@status:{open}",
            b"GROUPBY",
            b"1",
            b"@priority",
            b"REDUCE",
            b"COUNT",
            b"0",
            b"AS",
            b"count",
            b"SORTBY",
            b"2",
            b"@count",
            b"DESC",
        ]);
        let parsed = parse_aggregate_args(&args).expect("parse ok");
        assert_eq!(parsed.query.as_ref(), b"@status:{open}");
        // 2 steps: GroupBy + SortBy. Filter lives in the query prefix (I11).
        assert_eq!(parsed.pipeline.len(), 2);
        match &parsed.pipeline[0] {
            AggregateStep::GroupBy { .. } => {}
            _ => panic!("expected GroupBy first"),
        }
        match &parsed.pipeline[1] {
            AggregateStep::SortBy { keys, max } => {
                assert_eq!(keys.len(), 1);
                assert_eq!(keys[0].0.as_ref(), b"@count");
                assert_eq!(keys[0].1, SortOrder::Desc);
                assert!(max.is_none());
            }
            _ => panic!("expected SortBy second"),
        }
    }

    #[test]
    fn test_parse_rejects_unknown_reducer() {
        let args = args_from(&[
            b"myidx", b"*", b"GROUPBY", b"1", b"@f", b"REDUCE", b"FOO", b"0", b"AS", b"x",
        ]);
        match parse_aggregate_args(&args) {
            Err(Frame::Error(msg)) => {
                let s = std::str::from_utf8(&msg).unwrap();
                assert!(
                    s.starts_with("ERR unknown REDUCE function: FOO"),
                    "got: {s}"
                );
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_rejects_apply() {
        let args = args_from(&[b"myidx", b"*", b"APPLY", b"@a+1", b"AS", b"b"]);
        match parse_aggregate_args(&args) {
            Err(Frame::Error(msg)) => {
                assert_eq!(&msg[..], b"ERR APPLY stage not supported in v1");
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_rejects_unclosed_groupby() {
        // GROUPBY 2 @a  — declared 2 fields but only one given
        let args = args_from(&[b"myidx", b"*", b"GROUPBY", b"2", b"@a"]);
        match parse_aggregate_args(&args) {
            Err(Frame::Error(msg)) => {
                let s = std::str::from_utf8(&msg).unwrap();
                assert!(s.contains("wrong number of arguments"), "got: {s}");
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_sortby_direction() {
        let args = args_from(&[b"myidx", b"*", b"SORTBY", b"2", b"@score", b"DESC"]);
        let parsed = parse_aggregate_args(&args).expect("parse ok");
        assert_eq!(parsed.pipeline.len(), 1);
        match &parsed.pipeline[0] {
            AggregateStep::SortBy { keys, max } => {
                assert_eq!(keys.len(), 1);
                assert_eq!(keys[0].0.as_ref(), b"@score");
                assert_eq!(keys[0].1, SortOrder::Desc);
                assert!(max.is_none());
            }
            _ => panic!("expected SortBy"),
        }
    }

    #[test]
    fn test_parse_limit() {
        let args = args_from(&[b"myidx", b"*", b"LIMIT", b"0", b"10"]);
        let parsed = parse_aggregate_args(&args).expect("parse ok");
        match &parsed.pipeline[0] {
            AggregateStep::Limit { offset, count } => {
                assert_eq!(*offset, 0);
                assert_eq!(*count, 10);
            }
            _ => panic!("expected Limit"),
        }
    }

    #[test]
    fn test_parse_limit_clamps_count() {
        let args = args_from(&[b"myidx", b"*", b"LIMIT", b"0", b"1000000000"]);
        let parsed = parse_aggregate_args(&args).expect("parse ok");
        match &parsed.pipeline[0] {
            AggregateStep::Limit { count, .. } => {
                assert_eq!(*count, AGGREGATE_LIMIT_CAP);
            }
            _ => panic!("expected Limit"),
        }
    }

    #[test]
    fn test_parse_reducer_count_distinct() {
        let args = args_from(&[
            b"myidx",
            b"*",
            b"GROUPBY",
            b"0",
            b"REDUCE",
            b"COUNT_DISTINCT",
            b"1",
            b"@user",
            b"AS",
            b"distinct_users",
        ]);
        let parsed = parse_aggregate_args(&args).expect("parse ok");
        match &parsed.pipeline[0] {
            AggregateStep::GroupBy { reducers, .. } => {
                assert_eq!(reducers.len(), 1);
                assert_eq!(reducers[0].fn_name, ReducerFn::CountDistinct);
                assert_eq!(reducers[0].field.as_ref().unwrap().as_ref(), b"@user");
                assert_eq!(reducers[0].alias.as_ref(), b"distinct_users");
            }
            _ => panic!("expected GroupBy"),
        }
    }

    #[test]
    fn test_parse_reducer_sum_requires_field() {
        let args = args_from(&[
            b"myidx", b"*", b"GROUPBY", b"0", b"REDUCE", b"SUM", b"0", b"AS", b"x",
        ]);
        match parse_aggregate_args(&args) {
            Err(Frame::Error(msg)) => {
                assert_eq!(&msg[..], b"ERR SUM requires a field");
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_reducer_auto_alias_when_no_as() {
        // REDUCE COUNT 0  (no AS alias) — auto-generate "count"
        let args = args_from(&[
            b"myidx", b"*", b"GROUPBY", b"1", b"@priority", b"REDUCE", b"COUNT", b"0",
        ]);
        let parsed = parse_aggregate_args(&args).expect("parse ok");
        match &parsed.pipeline[0] {
            AggregateStep::GroupBy { reducers, .. } => {
                assert_eq!(reducers[0].alias.as_ref(), b"count");
            }
            _ => panic!("expected GroupBy"),
        }
    }

    #[test]
    fn test_parse_reducer_auto_alias_sum() {
        // REDUCE SUM 1 @price  (no AS) — auto-generate "sum_price"
        let args = args_from(&[
            b"myidx",
            b"*",
            b"GROUPBY",
            b"0",
            b"REDUCE",
            b"SUM",
            b"1",
            b"@price",
        ]);
        let parsed = parse_aggregate_args(&args).expect("parse ok");
        match &parsed.pipeline[0] {
            AggregateStep::GroupBy { reducers, .. } => {
                assert_eq!(reducers[0].alias.as_ref(), b"sum_price");
            }
            _ => panic!("expected GroupBy"),
        }
    }

    #[test]
    fn test_parse_filter_is_consumed_as_noop() {
        // I11: top-level FILTER is parsed + consumed but evaluation is deferred.
        let args = args_from(&[
            b"myidx",
            b"*",
            b"FILTER",
            b"@price > 10",
            b"GROUPBY",
            b"1",
            b"@category",
            b"REDUCE",
            b"COUNT",
            b"0",
            b"AS",
            b"n",
        ]);
        let parsed = parse_aggregate_args(&args).expect("parse ok");
        // FILTER contributes no pipeline step in v1 (NO-OP).
        assert_eq!(
            parsed.pipeline.len(),
            1,
            "only GROUPBY step should be present"
        );
    }

    #[test]
    fn test_parse_empty_args_errors() {
        let args: Vec<Frame> = vec![];
        match parse_aggregate_args(&args) {
            Err(Frame::Error(msg)) => {
                let s = std::str::from_utf8(&msg).unwrap();
                assert!(s.contains("wrong number of arguments"));
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_unknown_keyword_errors() {
        let args = args_from(&[b"myidx", b"*", b"UNKNOWN"]);
        match parse_aggregate_args(&args) {
            Err(Frame::Error(msg)) => {
                assert_eq!(&msg[..], b"ERR unknown FT.AGGREGATE keyword");
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }
}
