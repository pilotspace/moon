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

use crate::protocol::{Frame, FrameVec};
use crate::storage::db::Database;
use crate::text::aggregate::{
    AggregateRow, AggregateStep, AggregateValue, ReducerFn, ReducerSpec, SortOrder,
    execute_pipeline,
};
use crate::text::store::{TextIndex, TextStore};
use crate::vector::store::VectorStore;

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
/// Produced by [`parse_aggregate_args`] and consumed by [`ft_aggregate`].
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
// Handler — executes a parsed FT.AGGREGATE on a single shard
// ---------------------------------------------------------------------------

/// FT.AGGREGATE entry point (single shard; Plan 03 wires the multi-shard path).
///
/// Flow:
/// 1. Parse args via `parse_aggregate_args`.
/// 2. Resolve `TextIndex` by name from the `TextStore`.
/// 3. Run BM25 on the `query` string (or "*" for match-all) to produce a
///    candidate doc-id set.
/// 4. Materialise rows from the `Database` hash store (reads @field values
///    per doc, only for fields referenced by the pipeline — we never read
///    the whole hash).
/// 5. Execute the pipeline.
/// 6. Build the RediSearch-compatible response.
///
/// ACL category: `@read + @search` (matches FT.SEARCH per D-20).
pub fn ft_aggregate(
    _vector_store: &mut VectorStore,
    text_store: &TextStore,
    args: &[Frame],
    db: &Database,
) -> Frame {
    let parsed = match parse_aggregate_args(args) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let text_index = match text_store.get_index(&parsed.index_name) {
        Some(ix) => ix,
        None => return Frame::Error(Bytes::from_static(b"ERR unknown index")),
    };

    // Materialise candidate rows.
    let rows = match materialize_rows(text_index, db, &parsed.query, &parsed.pipeline) {
        Ok(r) => r,
        Err(e) => return e,
    };

    // Execute the pipeline.
    match execute_pipeline(rows, &parsed.pipeline) {
        Ok(final_rows) => build_aggregate_response(&final_rows),
        Err(msg) => Frame::Error(msg),
    }
}

/// Collect candidate docs (BM25 on the query string, or all docs for `*`)
/// and read the fields referenced by the pipeline from the Database hash
/// store.
pub fn materialize_rows(
    text_index: &TextIndex,
    db: &Database,
    query: &Bytes,
    pipeline: &[AggregateStep],
) -> Result<Vec<AggregateRow>, Frame> {
    // Collect the distinct set of @fields referenced anywhere in the pipeline —
    // we only read what we need (Pitfall 1 RESEARCH: minimise per-doc hash reads).
    let mut wanted: SmallVec<[Bytes; 8]> = SmallVec::new();
    for step in pipeline {
        match step {
            AggregateStep::GroupBy { fields, reducers } => {
                for f in fields {
                    push_unique(&mut wanted, f.clone());
                }
                for r in reducers {
                    if let Some(f) = &r.field {
                        push_unique(&mut wanted, f.clone());
                    }
                }
            }
            AggregateStep::SortBy { keys, .. } => {
                for (f, _) in keys {
                    push_unique(&mut wanted, f.clone());
                }
            }
            _ => {}
        }
    }

    // Resolve candidate doc_ids. `*` and empty query ⇒ match-all; anything
    // else goes through the Phase 150 BM25 text parser.
    let candidate_doc_ids: Vec<u32> = if query.as_ref() == b"*" || query.is_empty() {
        text_index.doc_id_to_key.keys().copied().collect()
    } else {
        bm25_candidate_doc_ids(text_index, query)?
    };

    // For each candidate doc_id, look up the Redis key and fetch wanted fields.
    let now_ms = db.now_ms();
    let mut rows: Vec<AggregateRow> = Vec::with_capacity(candidate_doc_ids.len());
    for doc_id in candidate_doc_ids {
        let key = match text_index.doc_id_to_key.get(&doc_id) {
            Some(k) => k.clone(),
            None => continue,
        };
        // db.get_hash_ref_if_alive returns Err on WRONGTYPE, Ok(None) when the key
        // is missing/expired, Ok(Some(HashRef)) on success. We silently skip the
        // first two cases — a doc that is indexed but whose hash was deleted out
        // from under the index is a race, not a fatal error.
        let hash = match db.get_hash_ref_if_alive(&key, now_ms) {
            Ok(Some(h)) => h,
            _ => continue,
        };
        let mut row: AggregateRow = SmallVec::new();
        for field in &wanted {
            // Strip leading '@' if the pipeline used the RediSearch field reference syntax.
            let bare: &[u8] = if field.starts_with(b"@") {
                &field[1..]
            } else {
                field.as_ref()
            };
            let val = hash
                .get_field(bare)
                .map(AggregateValue::Str)
                .unwrap_or(AggregateValue::Null);
            row.push((field.clone(), val));
        }
        rows.push(row);
    }
    Ok(rows)
}

/// Resolve candidate doc IDs for a BM25 text query (field-targeted or bare-terms).
///
/// Returns `Frame::Error` on parse failure. A query that analyses to zero tokens
/// (e.g. all stop words) returns an empty candidate set rather than an error —
/// matches RediSearch behaviour where `FT.AGGREGATE idx the GROUPBY ...` yields
/// `[0]` without surfacing an internal BM25 error.
fn bm25_candidate_doc_ids(text_index: &TextIndex, query: &Bytes) -> Result<Vec<u32>, Frame> {
    use crate::command::vector_search::ft_text_search::{
        execute_query_on_index, parse_text_query, pre_parse_field_filter,
    };

    // Plan 152-06 fast path: non-BM25 FieldFilter clauses (TAG, and Plan 07
    // NumericRange). Safe on TAG-only / NUMERIC-only indexes because we never
    // touch `field_analyzers`. Fixes Blocker 1 — `aggidx` fixture with zero
    // TEXT fields no longer returns `ERR index has no TEXT fields` on
    // @status:{open}.
    match pre_parse_field_filter(query.as_ref()) {
        Ok(Some(clause)) => {
            let results =
                execute_query_on_index(text_index, &clause, None, None, u32::MAX as usize);
            return Ok(results.into_iter().map(|r| r.doc_id).collect());
        }
        Ok(None) => { /* fall through to BM25 path */ }
        Err(e) => {
            let mut msg = b"ERR ".to_vec();
            msg.extend_from_slice(e.as_bytes());
            return Err(Frame::Error(Bytes::from(msg)));
        }
    }

    // BM25 path: analyzer is required. Indexes without TEXT fields cannot
    // serve bare-term queries — surface the cause instead of a generic error.
    let analyzer = match text_index.field_analyzers.first() {
        Some(a) => a,
        None => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR bare-term text query requires at least one TEXT field in the index",
            )));
        }
    };
    let clause = match parse_text_query(query.as_ref(), analyzer) {
        Ok(c) => c,
        Err(e) => {
            // Treat "empty query after analysis" as match-none rather than surfacing
            // the error — clients expect GROUPBY to work even when a query is
            // degenerate (e.g. all stop words filtered out).
            if e.contains("empty query after analysis") {
                return Ok(Vec::new());
            }
            let mut msg = b"ERR ".to_vec();
            msg.extend_from_slice(e.as_bytes());
            return Err(Frame::Error(Bytes::from(msg)));
        }
    };
    // Search all matching docs — `u32::MAX as usize` is the sentinel for "no cap";
    // GROUPBY cardinality limit + LIMIT cap guard downstream.
    let results = execute_query_on_index(text_index, &clause, None, None, u32::MAX as usize);
    Ok(results.into_iter().map(|r| r.doc_id).collect())
}

/// Push `b` onto `v` iff it is not already present. O(N²) but N is tiny
/// (typical ≤ 8 distinct fields per pipeline).
fn push_unique(v: &mut SmallVec<[Bytes; 8]>, b: Bytes) {
    if !v.iter().any(|x| x == &b) {
        v.push(b);
    }
}

// ---------------------------------------------------------------------------
// Response builder — RediSearch-compatible RESP shape
// ---------------------------------------------------------------------------

/// Build the RESP array response for FT.AGGREGATE.
///
/// Shape (matches RediSearch):
/// ```text
///   [
///     <num_rows : Integer>,
///     [<field_name1>, <value1>, <field_name2>, <value2>, ...],
///     ...
///   ]
/// ```
pub fn build_aggregate_response(rows: &[AggregateRow]) -> Frame {
    let mut top: Vec<Frame> = Vec::with_capacity(rows.len() + 1);
    top.push(Frame::Integer(rows.len() as i64));
    for row in rows {
        let mut fields: Vec<Frame> = Vec::with_capacity(row.len() * 2);
        for (name, val) in row {
            fields.push(Frame::BulkString(name.clone()));
            fields.push(value_to_frame(val));
        }
        top.push(Frame::Array(FrameVec::from(fields)));
    }
    Frame::Array(FrameVec::from(top))
}

fn value_to_frame(v: &AggregateValue) -> Frame {
    match v {
        AggregateValue::Null => Frame::Null,
        AggregateValue::Int(n) => {
            let mut buf = itoa::Buffer::new();
            Frame::BulkString(Bytes::copy_from_slice(buf.format(*n).as_bytes()))
        }
        AggregateValue::Float(of) => {
            // Format floats without trailing zeros — matches RediSearch.
            let s = format!("{}", of.into_inner());
            Frame::BulkString(Bytes::from(s))
        }
        AggregateValue::Str(b) => Frame::BulkString(b.clone()),
    }
}

// ---------------------------------------------------------------------------
// Plan 03 — multi-shard scatter-gather entry points
// ---------------------------------------------------------------------------
//
// `execute_local_full` mirrors `ft_aggregate` but skips argument parsing
// (the coordinator parsed args once and broadcasts the already-validated
// pipeline). This is the single-shard fast path.
//
// `execute_local_partial` runs the pipeline UP TO post-GROUPBY and ships
// the resulting `ShardPartial` back encoded as a `Frame::Array` via
// `encode_shard_partial`. The coordinator decodes and calls
// `merge_partial_states` (Plan 01) to associatively combine across shards,
// then applies global SORTBY/LIMIT per D-07.

use crate::storage::hll::Hll;
use crate::text::aggregate::{
    GROUPBY_CARDINALITY_LIMIT, GroupKey, PartialReducerState, ShardPartial, init_shard_states,
    row_get_as_bytes, update_reducers_public,
};

/// Single-shard fast path. Executes the full pipeline locally and builds
/// the final RESP response directly — no scatter, no encode/decode.
///
/// Caller must hold the `TextStore` and `Database` guards; this function
/// performs no locking of its own.
pub fn execute_local_full(
    _vector_store: &mut VectorStore,
    text_store: &TextStore,
    index_name: &Bytes,
    query: &Bytes,
    pipeline: &[AggregateStep],
    db: &Database,
) -> Frame {
    let text_index = match text_store.get_index(index_name) {
        Some(ix) => ix,
        None => return Frame::Error(Bytes::from_static(b"ERR unknown index")),
    };
    let rows = match materialize_rows(text_index, db, query, pipeline) {
        Ok(r) => r,
        Err(e) => return e,
    };
    match execute_pipeline(rows, pipeline) {
        Ok(final_rows) => build_aggregate_response(&final_rows),
        Err(msg) => Frame::Error(msg),
    }
}

/// Multi-shard PHASE 1 entry.
///
/// Executes the pipeline UP TO and including GROUPBY+REDUCE on the local
/// shard and returns an encoded `ShardPartial` (`Frame::Array`) or
/// `Frame::Error`. Per D-07, SORTBY and LIMIT are NOT applied here — the
/// coordinator applies them globally after merging all shard partials.
///
/// A pipeline without a GROUPBY is explicitly rejected in multi-shard
/// mode: every reducer merge assumes group-keyed state, and projecting raw
/// rows across shards is out of scope for v1 (AGG-03 is facet aggregation).
pub fn execute_local_partial(
    text_store: &TextStore,
    index_name: &Bytes,
    query: &Bytes,
    pipeline: &[AggregateStep],
    db: &Database,
) -> Frame {
    let text_index = match text_store.get_index(index_name) {
        Some(ix) => ix,
        None => return Frame::Error(Bytes::from_static(b"ERR unknown index")),
    };

    // Find the (single) GroupBy step. v1 grammar allows at most one.
    let gb = pipeline.iter().find_map(|s| match s {
        AggregateStep::GroupBy { fields, reducers } => Some((fields.to_vec(), reducers.clone())),
        _ => None,
    });
    let (gb_fields, gb_reducers) = match gb {
        Some(g) => g,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR FT.AGGREGATE without GROUPBY is not supported in multi-shard mode",
            ));
        }
    };

    // Materialise candidate rows using the same path as single-shard so
    // only referenced fields are read from the hash store.
    let rows = match materialize_rows(text_index, db, query, pipeline) {
        Ok(r) => r,
        Err(e) => return e,
    };

    // Build per-shard partial groups. Applies FILTER if present (v1:
    // FILTER is a NO-OP in the pipeline but kept for future wiring).
    let filtered = apply_filter_stages(rows, pipeline);

    let partial: ShardPartial = match build_shard_partial(filtered, &gb_fields, &gb_reducers) {
        Ok(p) => p,
        Err(msg) => return Frame::Error(msg),
    };

    encode_shard_partial(&partial)
}

/// Apply any FILTER stages found in the pipeline. In v1 `execute_pipeline`
/// treats FILTER as a pass-through (see `text/aggregate.rs`), so this
/// helper mirrors that contract — it exists to keep `execute_local_partial`
/// symmetrical with `execute_local_full` and so the FILTER evaluator can
/// drop in without touching the scatter path.
fn apply_filter_stages(rows: Vec<AggregateRow>, _pipeline: &[AggregateStep]) -> Vec<AggregateRow> {
    rows
}

/// Build per-shard `ShardPartial` from materialised rows.
///
/// Enforces the GROUPBY cardinality cap per shard (same `W5` pattern as
/// `text::aggregate::apply_groupby`): the check fires **before** the
/// HashMap insert so memory use is bounded at `GROUPBY_CARDINALITY_LIMIT`
/// distinct keys — never N+1.
fn build_shard_partial(
    rows: Vec<AggregateRow>,
    fields: &[Bytes],
    reducers: &[ReducerSpec],
) -> Result<ShardPartial, Bytes> {
    use std::collections::HashMap;
    let mut groups: HashMap<GroupKey, SmallVec<[PartialReducerState; 4]>> = HashMap::new();
    for row in rows {
        let key: GroupKey = fields.iter().map(|f| row_get_as_bytes(&row, f)).collect();
        if !groups.contains_key(&key) && groups.len() >= GROUPBY_CARDINALITY_LIMIT {
            return Err(Bytes::from_static(
                b"ERR GROUPBY cardinality limit exceeded",
            ));
        }
        let states = groups
            .entry(key)
            .or_insert_with(|| init_shard_states(reducers));
        update_reducers_public(states, &row, reducers);
    }
    Ok(groups.into_iter().collect())
}

// ---------------------------------------------------------------------------
// ShardPartial <-> Frame codec (hand-rolled, matches existing dispatch
// patterns — Frame-based rather than bincode, per RESEARCH alternatives).
// ---------------------------------------------------------------------------
//
// Wire layout (all top-level items in a Frame::Array):
//
//   [
//     Integer(group_count),
//     for each group:
//       Integer(key_field_count),
//       BulkString(key_field_1_bytes),
//       ...
//       BulkString(key_field_N_bytes),
//       Integer(state_count),
//       for each state: <tag BulkString> [<payload frames per tag>]
//   ]
//
// State payload shapes:
//   "COUNT"   Integer(n)
//   "SUM"     BulkString(sum_as_ascii_f64)   Integer(count)
//   "MIN"     BulkString(value_as_ascii_f64)
//   "MIN_NONE"
//   "MAX"     BulkString(value_as_ascii_f64)
//   "MAX_NONE"
//   "HLL"     BulkString(hll_wire_bytes)     // Redis HYLL format
//
// Chosen over bincode because it matches the existing DocFreq / TextSearch
// `ShardMessage` reply shapes, keeps the codec visible to RESP-level
// tooling, and avoids adding a serde derive to every state variant.

/// Encode a `ShardPartial` as a single `Frame::Array`.
pub fn encode_shard_partial(partial: &ShardPartial) -> Frame {
    // Pre-allocate a generous capacity: 1 (group count) + per-group
    // overhead (~4 frames) + per-state overhead (~3 frames).
    let mut out: Vec<Frame> = Vec::with_capacity(1 + partial.len() * 6);
    out.push(Frame::Integer(partial.len() as i64));
    for (key, states) in partial {
        out.push(Frame::Integer(key.len() as i64));
        for field_val in key {
            out.push(Frame::BulkString(field_val.clone()));
        }
        out.push(Frame::Integer(states.len() as i64));
        for state in states {
            encode_partial_state(state, &mut out);
        }
    }
    Frame::Array(FrameVec::from(out))
}

fn encode_partial_state(state: &PartialReducerState, out: &mut Vec<Frame>) {
    match state {
        PartialReducerState::Count(n) => {
            out.push(Frame::BulkString(Bytes::from_static(b"COUNT")));
            out.push(Frame::Integer(*n as i64));
        }
        PartialReducerState::Sum { sum, count } => {
            out.push(Frame::BulkString(Bytes::from_static(b"SUM")));
            out.push(Frame::BulkString(Bytes::from(format!("{}", sum))));
            out.push(Frame::Integer(*count as i64));
        }
        PartialReducerState::Min(Some(of)) => {
            out.push(Frame::BulkString(Bytes::from_static(b"MIN")));
            out.push(Frame::BulkString(Bytes::from(format!(
                "{}",
                of.into_inner()
            ))));
        }
        PartialReducerState::Min(None) => {
            out.push(Frame::BulkString(Bytes::from_static(b"MIN_NONE")));
        }
        PartialReducerState::Max(Some(of)) => {
            out.push(Frame::BulkString(Bytes::from_static(b"MAX")));
            out.push(Frame::BulkString(Bytes::from(format!(
                "{}",
                of.into_inner()
            ))));
        }
        PartialReducerState::Max(None) => {
            out.push(Frame::BulkString(Bytes::from_static(b"MAX_NONE")));
        }
        PartialReducerState::CountDistinct(hll) => {
            out.push(Frame::BulkString(Bytes::from_static(b"HLL")));
            out.push(Frame::BulkString(Bytes::copy_from_slice(hll.as_bytes())));
        }
    }
}

/// Decode a `ShardPartial` from a Frame produced by `encode_shard_partial`.
///
/// Returns `None` on any structural error. The coordinator maps `None` to
/// `Frame::Error("ERR FT.AGGREGATE shard returned malformed partial state")`
/// so malformed shard replies never panic the coordinator loop
/// (T-152-03-02).
pub fn decode_shard_partial(frame: &Frame) -> Option<ShardPartial> {
    let items = match frame {
        Frame::Array(a) => a,
        _ => return None,
    };
    let items: &[Frame] = items;
    let mut i = 0usize;
    let group_count = as_i64(items.get(i)?)? as usize;
    i += 1;
    let mut partial: ShardPartial = Vec::with_capacity(group_count);
    for _ in 0..group_count {
        let key_len = as_i64(items.get(i)?)? as usize;
        i += 1;
        let mut key: GroupKey = SmallVec::new();
        for _ in 0..key_len {
            key.push(as_bytes(items.get(i)?)?);
            i += 1;
        }
        let state_count = as_i64(items.get(i)?)? as usize;
        i += 1;
        let mut states: SmallVec<[PartialReducerState; 4]> = SmallVec::new();
        for _ in 0..state_count {
            states.push(decode_partial_state(items, &mut i)?);
        }
        partial.push((key, states));
    }
    Some(partial)
}

fn decode_partial_state(items: &[Frame], i: &mut usize) -> Option<PartialReducerState> {
    let tag = as_bytes(items.get(*i)?)?;
    *i += 1;
    match tag.as_ref() {
        b"COUNT" => {
            let n = as_i64(items.get(*i)?)?;
            *i += 1;
            Some(PartialReducerState::Count(n as u64))
        }
        b"SUM" => {
            let sum_bytes = as_bytes(items.get(*i)?)?;
            *i += 1;
            let sum: f64 = std::str::from_utf8(&sum_bytes).ok()?.parse().ok()?;
            let count = as_i64(items.get(*i)?)?;
            *i += 1;
            Some(PartialReducerState::Sum {
                sum,
                count: count as u64,
            })
        }
        b"MIN" => {
            let v_bytes = as_bytes(items.get(*i)?)?;
            *i += 1;
            let v: f64 = std::str::from_utf8(&v_bytes).ok()?.parse().ok()?;
            Some(PartialReducerState::Min(Some(ordered_float::OrderedFloat(
                v,
            ))))
        }
        b"MIN_NONE" => Some(PartialReducerState::Min(None)),
        b"MAX" => {
            let v_bytes = as_bytes(items.get(*i)?)?;
            *i += 1;
            let v: f64 = std::str::from_utf8(&v_bytes).ok()?.parse().ok()?;
            Some(PartialReducerState::Max(Some(ordered_float::OrderedFloat(
                v,
            ))))
        }
        b"MAX_NONE" => Some(PartialReducerState::Max(None)),
        b"HLL" => {
            let b = as_bytes(items.get(*i)?)?;
            *i += 1;
            Hll::from_bytes(b)
                .ok()
                .map(PartialReducerState::CountDistinct)
        }
        _ => None,
    }
}

#[inline]
fn as_i64(f: &Frame) -> Option<i64> {
    match f {
        Frame::Integer(n) => Some(*n),
        _ => None,
    }
}

#[inline]
fn as_bytes(f: &Frame) -> Option<Bytes> {
    match f {
        Frame::BulkString(b) => Some(b.clone()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::db::Database;
    use crate::text::store::TextStore;
    use crate::text::types::{BM25Config, TextFieldDef};

    // -- Helpers --

    fn bulk(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    fn int(n: i64) -> Frame {
        Frame::Integer(n)
    }

    /// Build an args vector from a sequence of &[u8] slices.
    fn args_from(pieces: &[&[u8]]) -> Vec<Frame> {
        pieces.iter().map(|p| bulk(p)).collect()
    }

    fn make_title_body_index() -> TextIndex {
        let title = TextFieldDef::new(Bytes::from_static(b"title"));
        let body = TextFieldDef::new(Bytes::from_static(b"body"));
        TextIndex::new(
            Bytes::from_static(b"myidx"),
            vec![Bytes::from_static(b"doc:")],
            vec![title, body],
            BM25Config::default(),
        )
    }

    fn make_status_priority_index() -> TextIndex {
        // Two TEXT fields so the analyzer pipeline is present. The aggregation
        // tests read @status and @priority straight out of the Database hash,
        // bypassing BM25 indexing on those specific field names — that's fine
        // because `*` match-all enumerates all registered doc_ids.
        let status = TextFieldDef::new(Bytes::from_static(b"title"));
        let body = TextFieldDef::new(Bytes::from_static(b"body"));
        TextIndex::new(
            Bytes::from_static(b"myidx"),
            vec![Bytes::from_static(b"doc:")],
            vec![status, body],
            BM25Config::default(),
        )
    }

    /// Insert `key` into `db`'s hash store with the given `[field,value]` pairs,
    /// and also index it in the TextIndex so doc_id_to_key is populated.
    fn insert_doc(db: &mut Database, idx: &mut TextIndex, key: &[u8], pairs: &[(&[u8], &[u8])]) {
        let map = db.get_or_create_hash(key).expect("hash create ok");
        for (f, v) in pairs {
            map.insert(Bytes::copy_from_slice(f), Bytes::copy_from_slice(v));
        }
        // Index into the TextIndex using the field name the index knows about
        // (`title` in our fixture). For aggregation tests we want the doc to
        // appear in doc_id_to_key; the actual token content doesn't matter
        // because the test uses `*` match-all.
        let title_value = pairs
            .iter()
            .find(|(f, _)| f == b"title")
            .map(|(_, v)| v.to_vec())
            .unwrap_or_else(|| b"placeholder".to_vec());
        let hset_args = vec![bulk(b"title"), bulk(&title_value)];
        let key_hash = xxhash_rust::xxh64::xxh64(key, 0);
        idx.index_document(key_hash, key, &hset_args);
    }

    // ==================================================================
    // parse_aggregate_args tests
    // ==================================================================

    #[test]
    fn test_parse_minimal_groupby() {
        // FT.AGGREGATE myidx "*" GROUPBY 1 @priority REDUCE COUNT 0 AS count
        let args = args_from(&[
            b"myidx",
            b"*",
            b"GROUPBY",
            b"1",
            b"@priority",
            b"REDUCE",
            b"COUNT",
            b"0",
            b"AS",
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
            b"myidx",
            b"*",
            b"GROUPBY",
            b"1",
            b"@priority",
            b"REDUCE",
            b"COUNT",
            b"0",
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
            b"myidx", b"*", b"GROUPBY", b"0", b"REDUCE", b"SUM", b"1", b"@price",
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

    // ==================================================================
    // ft_aggregate handler tests
    // ==================================================================

    #[test]
    fn test_ft_aggregate_single_shard_count() {
        let mut vs = VectorStore::new();
        let mut ts = TextStore::new();
        let mut db = Database::new();

        let mut idx = make_status_priority_index();
        insert_doc(
            &mut db,
            &mut idx,
            b"doc:1",
            &[(b"title", b"one"), (b"status", b"open")],
        );
        insert_doc(
            &mut db,
            &mut idx,
            b"doc:2",
            &[(b"title", b"two"), (b"status", b"open")],
        );
        insert_doc(
            &mut db,
            &mut idx,
            b"doc:3",
            &[(b"title", b"three"), (b"status", b"open")],
        );
        insert_doc(
            &mut db,
            &mut idx,
            b"doc:4",
            &[(b"title", b"four"), (b"status", b"closed")],
        );
        insert_doc(
            &mut db,
            &mut idx,
            b"doc:5",
            &[(b"title", b"five"), (b"status", b"closed")],
        );
        ts.create_index(Bytes::from_static(b"myidx"), idx).unwrap();

        let args = args_from(&[
            b"myidx", b"*", b"GROUPBY", b"1", b"@status", b"REDUCE", b"COUNT", b"0", b"AS",
            b"count",
        ]);

        let resp = ft_aggregate(&mut vs, &ts, &args, &db);
        let items = match resp {
            Frame::Array(a) => a,
            other => panic!("expected Array, got {other:?}"),
        };
        assert_eq!(items[0], int(2), "should have 2 groups (open / closed)");
        // 1 count + 2 groups
        assert_eq!(items.len(), 3);

        // Inspect each group row
        let mut open_count: Option<i64> = None;
        let mut closed_count: Option<i64> = None;
        for row_frame in items.iter().skip(1) {
            let row = match row_frame {
                Frame::Array(a) => a,
                _ => panic!("expected row array"),
            };
            // [@status, "<val>", count, "<n>"]
            assert_eq!(row.len(), 4);
            let status_val = match &row[1] {
                Frame::BulkString(b) => b.clone(),
                _ => panic!("expected bulk string status"),
            };
            let count_val = match &row[3] {
                Frame::BulkString(b) => std::str::from_utf8(b).unwrap().parse::<i64>().unwrap(),
                _ => panic!("expected bulk string count"),
            };
            if status_val.as_ref() == b"open" {
                open_count = Some(count_val);
            } else if status_val.as_ref() == b"closed" {
                closed_count = Some(count_val);
            }
        }
        assert_eq!(open_count, Some(3));
        assert_eq!(closed_count, Some(2));
    }

    #[test]
    fn test_ft_aggregate_sum_and_avg() {
        let mut vs = VectorStore::new();
        let mut ts = TextStore::new();
        let mut db = Database::new();

        let mut idx = make_status_priority_index();
        insert_doc(
            &mut db,
            &mut idx,
            b"doc:1",
            &[(b"title", b"a"), (b"price", b"10")],
        );
        insert_doc(
            &mut db,
            &mut idx,
            b"doc:2",
            &[(b"title", b"b"), (b"price", b"20")],
        );
        insert_doc(
            &mut db,
            &mut idx,
            b"doc:3",
            &[(b"title", b"c"), (b"price", b"30")],
        );
        ts.create_index(Bytes::from_static(b"myidx"), idx).unwrap();

        // GROUPBY 0 (global aggregation), SUM AS total, AVG AS avg
        let args = args_from(&[
            b"myidx", b"*", b"GROUPBY", b"0", b"REDUCE", b"SUM", b"1", b"@price", b"AS", b"total",
            b"REDUCE", b"AVG", b"1", b"@price", b"AS", b"avg",
        ]);
        let resp = ft_aggregate(&mut vs, &ts, &args, &db);
        let items = match resp {
            Frame::Array(a) => a,
            other => panic!("expected Array, got {other:?}"),
        };
        assert_eq!(items[0], int(1), "global groupby yields 1 row");
        let row = match &items[1] {
            Frame::Array(a) => a,
            _ => panic!("expected row array"),
        };
        // row = [total, "60", avg, "20"]
        // Only 2 reducer outputs (no GROUPBY fields), so 2 pairs
        assert_eq!(row.len(), 4);
        let total = match &row[1] {
            Frame::BulkString(b) => std::str::from_utf8(b).unwrap().parse::<f64>().unwrap(),
            _ => panic!("expected bulk string"),
        };
        let avg = match &row[3] {
            Frame::BulkString(b) => std::str::from_utf8(b).unwrap().parse::<f64>().unwrap(),
            _ => panic!("expected bulk string"),
        };
        assert!((total - 60.0).abs() < 1e-6, "total={total}");
        assert!((avg - 20.0).abs() < 1e-6, "avg={avg}");
    }

    #[test]
    fn test_ft_aggregate_sortby_limit() {
        let mut vs = VectorStore::new();
        let mut ts = TextStore::new();
        let mut db = Database::new();

        let mut idx = make_status_priority_index();
        // 5 status groups with counts 1..5
        let mut doc_no = 0u32;
        for (status, n) in [("s1", 1), ("s2", 2), ("s3", 3), ("s4", 4), ("s5", 5)] {
            for _ in 0..n {
                doc_no += 1;
                let key = format!("doc:{doc_no}");
                let title = format!("t{doc_no}");
                insert_doc(
                    &mut db,
                    &mut idx,
                    key.as_bytes(),
                    &[(b"title", title.as_bytes()), (b"status", status.as_bytes())],
                );
            }
        }
        ts.create_index(Bytes::from_static(b"myidx"), idx).unwrap();

        let args = args_from(&[
            b"myidx", b"*", b"GROUPBY", b"1", b"@status", b"REDUCE", b"COUNT", b"0", b"AS",
            b"count", b"SORTBY", b"2", b"@count", b"DESC", b"LIMIT", b"0", b"3",
        ]);
        let resp = ft_aggregate(&mut vs, &ts, &args, &db);
        let items = match resp {
            Frame::Array(a) => a,
            other => panic!("expected Array, got {other:?}"),
        };
        assert_eq!(items[0], int(3), "LIMIT 0 3 must cap result at 3");
        // Expect top 3 by count desc: s5=5, s4=4, s3=3
        let mut counts: Vec<i64> = Vec::new();
        for f in items.iter().skip(1) {
            let row = match f {
                Frame::Array(a) => a,
                _ => panic!("row"),
            };
            let count_val = match &row[3] {
                Frame::BulkString(b) => std::str::from_utf8(b).unwrap().parse::<i64>().unwrap(),
                _ => panic!("count"),
            };
            counts.push(count_val);
        }
        assert_eq!(counts, vec![5, 4, 3]);
    }

    #[test]
    fn test_ft_aggregate_count_distinct_on_user_ids() {
        let mut vs = VectorStore::new();
        let mut ts = TextStore::new();
        let mut db = Database::new();

        let mut idx = make_status_priority_index();
        // 10 docs, users = u1 u2 u1 u3 u2 u1 u3 u2 u4 u5 → distinct = 5
        let users: [&[u8]; 10] = [
            b"u1", b"u2", b"u1", b"u3", b"u2", b"u1", b"u3", b"u2", b"u4", b"u5",
        ];
        for (i, u) in users.iter().enumerate() {
            let key = format!("doc:{i}");
            let title = format!("t{i}");
            insert_doc(
                &mut db,
                &mut idx,
                key.as_bytes(),
                &[(b"title", title.as_bytes()), (b"user", u)],
            );
        }
        ts.create_index(Bytes::from_static(b"myidx"), idx).unwrap();

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
        let resp = ft_aggregate(&mut vs, &ts, &args, &db);
        let items = match resp {
            Frame::Array(a) => a,
            other => panic!("expected Array, got {other:?}"),
        };
        assert_eq!(items[0], int(1));
        let row = match &items[1] {
            Frame::Array(a) => a,
            _ => panic!("row"),
        };
        let distinct = match &row[1] {
            Frame::BulkString(b) => std::str::from_utf8(b).unwrap().parse::<i64>().unwrap(),
            _ => panic!("count"),
        };
        // HLL sketch estimate — assert within 2% of ground truth 5.
        let gt = 5.0f64;
        let err = ((distinct as f64 - gt).abs() / gt) * 100.0;
        assert!(
            err <= 2.0,
            "COUNT_DISTINCT estimate {distinct} outside 2% of {gt} (err={err:.2}%)"
        );
    }

    #[test]
    fn test_ft_aggregate_missing_index_returns_error() {
        let mut vs = VectorStore::new();
        let ts = TextStore::new();
        let db = Database::new();
        let args = args_from(&[b"no_such_idx", b"*"]);
        match ft_aggregate(&mut vs, &ts, &args, &db) {
            Frame::Error(msg) => {
                let s = std::str::from_utf8(&msg).unwrap();
                assert!(s.contains("unknown index"), "got: {s}");
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn test_ft_aggregate_empty_pipeline_returns_input_count() {
        let mut vs = VectorStore::new();
        let mut ts = TextStore::new();
        let mut db = Database::new();
        let mut idx = make_title_body_index();
        insert_doc(&mut db, &mut idx, b"doc:1", &[(b"title", b"alpha")]);
        insert_doc(&mut db, &mut idx, b"doc:2", &[(b"title", b"beta")]);
        ts.create_index(Bytes::from_static(b"myidx"), idx).unwrap();

        let args = args_from(&[b"myidx", b"*"]);
        let resp = ft_aggregate(&mut vs, &ts, &args, &db);
        let items = match resp {
            Frame::Array(a) => a,
            other => panic!("expected Array, got {other:?}"),
        };
        // Empty pipeline — rows are the raw materialised docs (no fields
        // wanted because no step references any field), so per-row array is
        // empty. Total count = 2.
        assert_eq!(items[0], int(2));
        assert_eq!(items.len(), 3);
    }

    #[test]
    fn test_ft_aggregate_limit_past_end_returns_empty() {
        let mut vs = VectorStore::new();
        let mut ts = TextStore::new();
        let mut db = Database::new();
        let mut idx = make_status_priority_index();
        insert_doc(
            &mut db,
            &mut idx,
            b"doc:1",
            &[(b"title", b"a"), (b"status", b"open")],
        );
        ts.create_index(Bytes::from_static(b"myidx"), idx).unwrap();
        let args = args_from(&[
            b"myidx", b"*", b"GROUPBY", b"1", b"@status", b"REDUCE", b"COUNT", b"0", b"AS",
            b"count", b"LIMIT", b"10", b"5",
        ]);
        let resp = ft_aggregate(&mut vs, &ts, &args, &db);
        let items = match resp {
            Frame::Array(a) => a,
            other => panic!("expected Array, got {other:?}"),
        };
        assert_eq!(items[0], int(0));
        assert_eq!(items.len(), 1, "offset past end yields just the count");
    }

    #[test]
    fn test_ft_aggregate_sortby_non_numeric_lexicographic() {
        // SORTBY on a non-numeric field — current executor falls back to
        // compare_values which treats equal on non-parseable floats; ensure
        // the request doesn't crash and returns rows.
        let mut vs = VectorStore::new();
        let mut ts = TextStore::new();
        let mut db = Database::new();
        let mut idx = make_status_priority_index();
        insert_doc(
            &mut db,
            &mut idx,
            b"doc:1",
            &[(b"title", b"a"), (b"status", b"zebra")],
        );
        insert_doc(
            &mut db,
            &mut idx,
            b"doc:2",
            &[(b"title", b"b"), (b"status", b"apple")],
        );
        ts.create_index(Bytes::from_static(b"myidx"), idx).unwrap();

        let args = args_from(&[
            b"myidx", b"*", b"GROUPBY", b"1", b"@status", b"REDUCE", b"COUNT", b"0", b"AS",
            b"count", b"SORTBY", b"1", b"@status",
        ]);
        let resp = ft_aggregate(&mut vs, &ts, &args, &db);
        let items = match resp {
            Frame::Array(a) => a,
            other => panic!("expected Array, got {other:?}"),
        };
        assert_eq!(items[0], int(2));
    }

    // ==================================================================
    // Plan 03 — execute_local_full / execute_local_partial + codec
    // ==================================================================

    fn parse_args_ok(pieces: &[&[u8]]) -> AggregateArgs {
        parse_aggregate_args(&args_from(pieces)).expect("parse ok")
    }

    #[test]
    fn test_execute_local_full_matches_handler() {
        let mut vs = VectorStore::new();
        let mut ts = TextStore::new();
        let mut db = Database::new();
        let mut idx = make_status_priority_index();
        insert_doc(
            &mut db,
            &mut idx,
            b"doc:1",
            &[(b"title", b"a"), (b"status", b"open")],
        );
        insert_doc(
            &mut db,
            &mut idx,
            b"doc:2",
            &[(b"title", b"b"), (b"status", b"open")],
        );
        insert_doc(
            &mut db,
            &mut idx,
            b"doc:3",
            &[(b"title", b"c"), (b"status", b"closed")],
        );
        ts.create_index(Bytes::from_static(b"myidx"), idx).unwrap();

        // Identical args route through parse + execute_local_full.
        let raw_args = args_from(&[
            b"myidx", b"*", b"GROUPBY", b"1", b"@status", b"REDUCE", b"COUNT", b"0", b"AS",
            b"count",
        ]);
        let expected = ft_aggregate(&mut vs, &ts, &raw_args, &db);
        let parsed = parse_aggregate_args(&raw_args).expect("parse ok");

        let actual = execute_local_full(
            &mut vs,
            &ts,
            &parsed.index_name,
            &parsed.query,
            &parsed.pipeline,
            &db,
        );

        // Compare the two Frames by content, not raw formatted order —
        // HashMap-backed GROUPBY iteration is non-deterministic, so both
        // paths can produce the same groups in different orderings.
        fn as_sorted_groups(f: &Frame) -> Vec<String> {
            let items = match f {
                Frame::Array(a) => a,
                _ => panic!("expected Array"),
            };
            let mut out: Vec<String> = Vec::new();
            for row in items.iter().skip(1) {
                out.push(format!("{row:?}"));
            }
            out.sort();
            out
        }
        assert_eq!(as_sorted_groups(&expected), as_sorted_groups(&actual));
        // And the count integer at index 0 must match.
        let (e0, a0) = match (&expected, &actual) {
            (Frame::Array(e), Frame::Array(a)) => (&e[0], &a[0]),
            _ => panic!("expected Arrays"),
        };
        assert_eq!(e0, a0);
    }

    #[test]
    fn test_execute_local_partial_returns_encoded_partial() {
        let mut ts = TextStore::new();
        let mut db = Database::new();
        let mut idx = make_status_priority_index();
        // 3 open + 2 closed
        for i in 0..3 {
            let key = format!("doc:o{i}");
            insert_doc(
                &mut db,
                &mut idx,
                key.as_bytes(),
                &[(b"title", b"t"), (b"status", b"open")],
            );
        }
        for i in 0..2 {
            let key = format!("doc:c{i}");
            insert_doc(
                &mut db,
                &mut idx,
                key.as_bytes(),
                &[(b"title", b"t"), (b"status", b"closed")],
            );
        }
        ts.create_index(Bytes::from_static(b"myidx"), idx).unwrap();

        let parsed = parse_args_ok(&[
            b"myidx", b"*", b"GROUPBY", b"1", b"@status", b"REDUCE", b"COUNT", b"0", b"AS",
            b"count",
        ]);

        let frame = execute_local_partial(
            &ts,
            &parsed.index_name,
            &parsed.query,
            &parsed.pipeline,
            &db,
        );
        let partial = decode_shard_partial(&frame).expect("decode ok");
        assert_eq!(partial.len(), 2, "two groups expected (open + closed)");

        // Find counts per status value.
        let mut open_count: Option<u64> = None;
        let mut closed_count: Option<u64> = None;
        for (key, states) in &partial {
            assert_eq!(key.len(), 1);
            let status = key[0].clone();
            assert_eq!(states.len(), 1);
            let n = match &states[0] {
                PartialReducerState::Count(n) => *n,
                other => panic!("expected Count, got {other:?}"),
            };
            match status.as_ref() {
                b"open" => open_count = Some(n),
                b"closed" => closed_count = Some(n),
                other => panic!("unexpected status {other:?}"),
            }
        }
        assert_eq!(open_count, Some(3));
        assert_eq!(closed_count, Some(2));
    }

    #[test]
    fn test_execute_local_partial_rejects_pipeline_without_groupby() {
        let mut ts = TextStore::new();
        let mut db = Database::new();
        let mut idx = make_status_priority_index();
        insert_doc(
            &mut db,
            &mut idx,
            b"doc:1",
            &[(b"title", b"a"), (b"status", b"open")],
        );
        ts.create_index(Bytes::from_static(b"myidx"), idx).unwrap();

        // Pipeline with SORTBY only — no GROUPBY.
        let parsed = parse_args_ok(&[b"myidx", b"*", b"SORTBY", b"1", b"@status"]);
        let frame = execute_local_partial(
            &ts,
            &parsed.index_name,
            &parsed.query,
            &parsed.pipeline,
            &db,
        );
        match frame {
            Frame::Error(msg) => {
                assert!(
                    msg.starts_with(b"ERR FT.AGGREGATE without GROUPBY"),
                    "got: {:?}",
                    std::str::from_utf8(&msg).ok()
                );
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn test_shard_partial_roundtrip_count() {
        let key: GroupKey = {
            let mut k: GroupKey = SmallVec::new();
            k.push(Bytes::from_static(b"open"));
            k
        };
        let states: SmallVec<[PartialReducerState; 4]> = {
            let mut s: SmallVec<[PartialReducerState; 4]> = SmallVec::new();
            s.push(PartialReducerState::Count(5));
            s
        };
        let partial: ShardPartial = vec![(key, states)];
        let frame = encode_shard_partial(&partial);
        let decoded = decode_shard_partial(&frame).expect("decode ok");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].0[0].as_ref(), b"open");
        match &decoded[0].1[0] {
            PartialReducerState::Count(n) => assert_eq!(*n, 5),
            other => panic!("expected Count(5), got {other:?}"),
        }
    }

    #[test]
    fn test_shard_partial_roundtrip_sum() {
        let key: GroupKey = {
            let mut k: GroupKey = SmallVec::new();
            k.push(Bytes::from_static(b"a"));
            k
        };
        let states: SmallVec<[PartialReducerState; 4]> = {
            let mut s: SmallVec<[PartialReducerState; 4]> = SmallVec::new();
            s.push(PartialReducerState::Sum {
                sum: 12.5,
                count: 3,
            });
            s
        };
        let partial: ShardPartial = vec![(key, states)];
        let frame = encode_shard_partial(&partial);
        let decoded = decode_shard_partial(&frame).expect("decode ok");
        match &decoded[0].1[0] {
            PartialReducerState::Sum { sum, count } => {
                assert!((*sum - 12.5).abs() < f64::EPSILON);
                assert_eq!(*count, 3);
            }
            other => panic!("expected Sum, got {other:?}"),
        }
    }

    #[test]
    fn test_shard_partial_roundtrip_min_max_with_none() {
        let mk_group =
            |state: PartialReducerState| -> (GroupKey, SmallVec<[PartialReducerState; 4]>) {
                let mut k: GroupKey = SmallVec::new();
                k.push(Bytes::from_static(b"k"));
                let mut v: SmallVec<[PartialReducerState; 4]> = SmallVec::new();
                v.push(state);
                (k, v)
            };

        let cases = vec![
            PartialReducerState::Min(None),
            PartialReducerState::Max(None),
            PartialReducerState::Min(Some(ordered_float::OrderedFloat(1.5))),
            PartialReducerState::Max(Some(ordered_float::OrderedFloat(9.0))),
        ];
        for state in cases {
            let partial: ShardPartial = vec![mk_group(state.clone())];
            let frame = encode_shard_partial(&partial);
            let decoded = decode_shard_partial(&frame).expect("decode ok");
            assert_eq!(decoded.len(), 1);
            let got = &decoded[0].1[0];
            // Format comparison is sufficient — OrderedFloat variants use
            // the same formatter on both sides.
            assert_eq!(format!("{state:?}"), format!("{got:?}"));
        }
    }

    #[test]
    fn test_shard_partial_roundtrip_hll() {
        let mut hll = Hll::new_sparse();
        for i in 0..100u32 {
            hll.add(&i.to_le_bytes());
        }
        let before_count = hll.count();

        let mut k: GroupKey = SmallVec::new();
        k.push(Bytes::from_static(b"g"));
        let mut v: SmallVec<[PartialReducerState; 4]> = SmallVec::new();
        v.push(PartialReducerState::CountDistinct(hll));
        let partial: ShardPartial = vec![(k, v)];

        let frame = encode_shard_partial(&partial);
        let decoded = decode_shard_partial(&frame).expect("decode ok");
        match &decoded[0].1[0] {
            PartialReducerState::CountDistinct(h) => {
                assert_eq!(h.count(), before_count);
            }
            other => panic!("expected CountDistinct, got {other:?}"),
        }
    }

    #[test]
    fn test_decode_shard_partial_malformed_returns_none() {
        // A truncated / wrong-type frame must never panic.
        let bad = Frame::Integer(42);
        assert!(decode_shard_partial(&bad).is_none());
        let bad2 = Frame::Array(FrameVec::from(vec![Frame::Integer(1)])); // missing group
        assert!(decode_shard_partial(&bad2).is_none());
    }

    // ==================================================================
    // Plan 03 — Task 3: SPSC handler routing simulation
    //
    // The real SPSC match arm in `shard::spsc_handler::handle_shard_message_shared`
    // destructures `ShardMessage::TextAggregate(payload)` and delegates to
    // `execute_local_partial(&text_guard, ..., &db_guard)` with guards held
    // across a synchronous block. These tests drive the same call path
    // the arm takes, exercising the code the SPSC handler relies on.
    // ==================================================================

    #[test]
    fn test_spsc_handler_routes_text_aggregate() {
        // Simulate what the SPSC arm does: build a payload, invoke
        // execute_local_partial with the store + db, decode the result.
        let mut ts = TextStore::new();
        let mut db = Database::new();
        let mut idx = make_status_priority_index();
        insert_doc(
            &mut db,
            &mut idx,
            b"doc:1",
            &[(b"title", b"a"), (b"status", b"open")],
        );
        insert_doc(
            &mut db,
            &mut idx,
            b"doc:2",
            &[(b"title", b"b"), (b"status", b"open")],
        );
        insert_doc(
            &mut db,
            &mut idx,
            b"doc:3",
            &[(b"title", b"c"), (b"status", b"closed")],
        );
        ts.create_index(Bytes::from_static(b"myidx"), idx).unwrap();

        let parsed = parse_args_ok(&[
            b"myidx", b"*", b"GROUPBY", b"1", b"@status", b"REDUCE", b"COUNT", b"0", b"AS",
            b"count",
        ]);

        // Exactly what ShardMessage::TextAggregate arm delegates to:
        let response = execute_local_partial(
            &ts,
            &parsed.index_name,
            &parsed.query,
            &parsed.pipeline,
            &db,
        );
        let partial =
            decode_shard_partial(&response).expect("SPSC arm must return decodable partial");
        assert_eq!(partial.len(), 2);

        // Sanity-check the counts per group.
        let mut open = 0u64;
        let mut closed = 0u64;
        for (key, states) in &partial {
            let n = match &states[0] {
                PartialReducerState::Count(n) => *n,
                other => panic!("expected Count, got {other:?}"),
            };
            match key[0].as_ref() {
                b"open" => open = n,
                b"closed" => closed = n,
                other => panic!("unexpected status {other:?}"),
            }
        }
        assert_eq!(open, 2);
        assert_eq!(closed, 1);

        // And also confirm that wrapping the exact arguments in a
        // ShardMessage::TextAggregate payload preserves them — the boxed
        // payload is what crosses the SPSC channel.
        let (reply_tx, reply_rx) = crate::runtime::channel::oneshot();
        let msg = crate::shard::dispatch::ShardMessage::TextAggregate(Box::new(
            crate::shard::dispatch::TextAggregatePayload {
                index_name: parsed.index_name.clone(),
                query: parsed.query.clone(),
                pipeline: parsed.pipeline.clone(),
                reply_tx,
            },
        ));
        // Destructure as the arm does.
        match msg {
            crate::shard::dispatch::ShardMessage::TextAggregate(payload) => {
                assert_eq!(payload.index_name, parsed.index_name);
                assert_eq!(payload.query, parsed.query);
                let _ = payload.reply_tx.send(response);
            }
            _ => panic!("expected TextAggregate"),
        }
        // The arm replies with the encoded partial; receiver sees it.
        let got = reply_rx.try_recv().expect("reply received");
        assert!(decode_shard_partial(&got).is_some());
    }

    #[test]
    fn test_spsc_handler_text_aggregate_unknown_index() {
        // Unknown index path — the SPSC arm must propagate the error
        // Frame unchanged (no encoded partial).
        let ts = TextStore::new();
        let db = Database::new();

        let parsed = parse_args_ok(&[
            b"no_such_idx",
            b"*",
            b"GROUPBY",
            b"1",
            b"@f",
            b"REDUCE",
            b"COUNT",
            b"0",
            b"AS",
            b"c",
        ]);
        let response = execute_local_partial(
            &ts,
            &parsed.index_name,
            &parsed.query,
            &parsed.pipeline,
            &db,
        );
        match response {
            Frame::Error(msg) => {
                let s = std::str::from_utf8(&msg).unwrap();
                assert!(s.contains("unknown index"), "got: {s}");
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }
}
