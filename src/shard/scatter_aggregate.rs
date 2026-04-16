//! Coordinator-side scatter-gather for FT.AGGREGATE (Phase 152, Plan 03).
//!
//! Two-phase strategy honouring CONTEXT.md D-05/D-06/D-07/D-08:
//!
//!   Phase 1: every shard runs the pipeline UP TO and including
//!            GROUPBY+REDUCE, then ships its `ShardPartial` back (encoded
//!            as a `Frame::Array` via `encode_shard_partial`).
//!   Phase 2: the coordinator merges all shard partials via
//!            `merge_partial_states`, then applies SORTBY + LIMIT
//!            globally to the merged row set.
//!
//! Kept in a sibling file (not `coordinator.rs`) because coordinator.rs
//! is already 1427 LOC and ~250 LOC of scatter-gather would push it past
//! the 1500-line cap (CLAUDE.md File Size, RESEARCH Pitfall 3).

#![cfg(feature = "text-index")]

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use bytes::Bytes;
use ringbuf::HeapProd;

use crate::protocol::Frame;
use crate::runtime::channel;
use crate::shard::coordinator::spsc_send;
use crate::shard::dispatch::{ShardMessage, TextAggregatePayload};
use crate::shard::shared_databases::ShardDatabases;
use crate::text::aggregate::{AggregateStep, ShardPartial, execute_pipeline, merge_partial_states};

/// Entry point for multi-shard FT.AGGREGATE.
///
/// Caller must have already parsed the command into an `index_name`,
/// `query`, and `pipeline`. The function:
///
/// 1. For `num_shards == 1`, takes the single-shard fast path via
///    `execute_local_full` — no SPSC fan-out, no encode/decode.
/// 2. Otherwise, fans `ShardMessage::TextAggregate` out to every remote
///    shard and runs `execute_local_partial` locally (no SPSC self-send).
/// 3. Collects `ShardPartial`s, merges associatively, applies global
///    SORTBY + LIMIT per D-07, and serialises the final rows.
pub async fn scatter_text_aggregate(
    index_name: Bytes,
    query: Bytes,
    pipeline: Vec<AggregateStep>,
    my_shard: usize,
    num_shards: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
) -> Frame {
    // ─── Single-shard fast path ───────────────────────────────────────────
    if num_shards == 1 {
        let result = {
            let mut vs = shard_databases.vector_store(my_shard);
            let ts = shard_databases.text_store(my_shard);
            let db_guard = shard_databases.read_db(my_shard, 0);
            crate::command::vector_search::ft_aggregate::execute_local_full(
                &mut vs,
                &ts,
                &index_name,
                &query,
                &pipeline,
                &db_guard,
            )
            // guards drop at end of this block, before any .await below
        };
        return result;
    }

    // ─── Phase 1: scatter partial-aggregate to every shard ────────────────
    let mut receivers: Vec<channel::OneshotReceiver<Frame>> =
        Vec::with_capacity(num_shards.saturating_sub(1));
    let mut local_partial: Option<Frame> = None;

    for shard_id in 0..num_shards {
        if shard_id == my_shard {
            // Local: direct call, avoids SPSC self-send and serialisation
            // round trip (CONTEXT.md D-08 follows scatter_text_search
            // template).
            let response = {
                let ts = shard_databases.text_store(shard_id);
                let db_guard = shard_databases.read_db(shard_id, 0);
                crate::command::vector_search::ft_aggregate::execute_local_partial(
                    &ts,
                    &index_name,
                    &query,
                    &pipeline,
                    &db_guard,
                )
                // guards drop before the subsequent .await
            };
            local_partial = Some(response);
        } else {
            let (reply_tx, reply_rx) = channel::oneshot();
            let msg = ShardMessage::TextAggregate(Box::new(TextAggregatePayload {
                index_name: index_name.clone(),
                query: query.clone(),
                pipeline: pipeline.clone(),
                reply_tx,
            }));
            spsc_send(dispatch_tx, my_shard, shard_id, msg, spsc_notifiers).await;
            receivers.push(reply_rx);
        }
    }

    // ─── Collect all shard partials ───────────────────────────────────────
    let mut shard_partials: Vec<ShardPartial> = Vec::with_capacity(num_shards);

    if let Some(local) = local_partial {
        match try_decode_partial_or_error(&local) {
            Ok(p) => shard_partials.push(p),
            Err(err_frame) => return err_frame,
        }
    }
    for rx in receivers {
        match rx.recv().await {
            Ok(frame) => match try_decode_partial_or_error(&frame) {
                Ok(p) => shard_partials.push(p),
                Err(err_frame) => return err_frame,
            },
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR FT.AGGREGATE phase 1 channel closed unexpectedly",
                ));
            }
        }
    }

    // ─── Phase 2: merge + global SORTBY/LIMIT per D-07 ────────────────────
    let (groupby_fields, reducers) = match pipeline.iter().find_map(|s| match s {
        AggregateStep::GroupBy { fields, reducers } => Some((fields.to_vec(), reducers.clone())),
        _ => None,
    }) {
        Some(gb) => gb,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR FT.AGGREGATE requires GROUPBY in multi-shard mode",
            ));
        }
    };

    let merged_rows = merge_partial_states(shard_partials, &groupby_fields, &reducers);

    // Apply only the global (post-GROUPBY) stages: SORTBY, LIMIT. FILTER is
    // a NO-OP in v1 (Plan 01) but preserved for future inline evaluation.
    // APPLY is rejected at parse time so it never reaches here.
    let post_pipeline: Vec<AggregateStep> = pipeline
        .iter()
        .filter(|s| {
            matches!(
                s,
                AggregateStep::SortBy { .. } | AggregateStep::Limit { .. }
            )
        })
        .cloned()
        .collect();
    let final_rows = match execute_pipeline(merged_rows, &post_pipeline) {
        Ok(r) => r,
        Err(msg) => return Frame::Error(msg),
    };
    crate::command::vector_search::ft_aggregate::build_aggregate_response(&final_rows)
}

/// Decode a shard's phase-1 reply Frame into a `ShardPartial`.
///
/// Preserves `Frame::Error` values verbatim (so the coordinator
/// short-circuits on shard-side errors). Anything that is neither an
/// Error nor a decodable partial collapses to a single canonical
/// "malformed partial" error — never a panic (threat T-152-03-02).
fn try_decode_partial_or_error(frame: &Frame) -> Result<ShardPartial, Frame> {
    match frame {
        Frame::Error(_) => Err(frame.clone()),
        _ => crate::command::vector_search::ft_aggregate::decode_shard_partial(frame).ok_or_else(
            || {
                Frame::Error(Bytes::from_static(
                    b"ERR FT.AGGREGATE shard returned malformed partial state",
                ))
            },
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use smallvec::SmallVec;

    use crate::command::vector_search::ft_aggregate::{decode_shard_partial, encode_shard_partial};
    #[cfg(feature = "runtime-tokio")]
    use crate::storage::Database;
    use crate::text::aggregate::{
        AggregateStep, GroupKey, PartialReducerState, ReducerFn, ReducerSpec, SortOrder,
    };

    /// Build a `ShardPartial` containing a single GROUPBY field with the
    /// given (key, count) pairs. Used to bypass materialisation and feed
    /// the coordinator pipeline directly.
    fn mk_partial(pairs: &[(&[u8], u64)]) -> ShardPartial {
        let mut out: ShardPartial = Vec::with_capacity(pairs.len());
        for (k, n) in pairs {
            let mut key: GroupKey = SmallVec::new();
            key.push(Bytes::copy_from_slice(k));
            let mut states: SmallVec<[PartialReducerState; 4]> = SmallVec::new();
            states.push(PartialReducerState::Count(*n));
            out.push((key, states));
        }
        out
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn test_scatter_text_aggregate_single_shard_skips_spsc() {
        // num_shards=1 must return from execute_local_full without touching
        // dispatch_tx. We pass an empty Vec of ringbuf producers — any
        // SPSC dispatch would panic on index access, proving the fast path
        // was taken. Empty TextStore yields "ERR unknown index".
        let dbs = vec![Database::new()];
        let shard_databases = ShardDatabases::new(vec![dbs]);

        let dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>> =
            Rc::new(RefCell::new(Vec::new()));
        let notifiers: Vec<Arc<channel::Notify>> = Vec::new();

        // Simple pipeline (GROUPBY) — must not reach the merge path since
        // single-shard is a direct execute_local_full.
        let mut gb_fields: SmallVec<[Bytes; 4]> = SmallVec::new();
        gb_fields.push(Bytes::from_static(b"@status"));
        let reducers = vec![ReducerSpec {
            fn_name: ReducerFn::Count,
            field: None,
            alias: Bytes::from_static(b"cnt"),
        }];
        let pipeline = vec![AggregateStep::GroupBy {
            fields: gb_fields,
            reducers,
        }];

        let result = scatter_text_aggregate(
            Bytes::from_static(b"nonexistent_idx"),
            Bytes::from_static(b"*"),
            pipeline,
            0, // my_shard
            1, // num_shards = 1 -> fast path
            &shard_databases,
            &dispatch_tx,
            &notifiers,
        )
        .await;

        match &result {
            Frame::Error(msg) => {
                let s = std::str::from_utf8(msg).unwrap_or("");
                assert!(
                    s.contains("unknown index"),
                    "expected 'unknown index' error, got: {s}"
                );
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn test_try_decode_partial_or_error_preserves_error_frame() {
        let err = Frame::Error(Bytes::from_static(b"ERR unknown index"));
        let got = try_decode_partial_or_error(&err);
        match got {
            Err(Frame::Error(msg)) => assert_eq!(&msg[..], b"ERR unknown index"),
            other => panic!("expected Error pass-through, got {other:?}"),
        }
    }

    #[test]
    fn test_try_decode_partial_or_error_malformed_yields_canonical_error() {
        let bad = Frame::Integer(42);
        match try_decode_partial_or_error(&bad) {
            Err(Frame::Error(msg)) => {
                let s = std::str::from_utf8(&msg).unwrap();
                assert!(s.contains("malformed partial"), "got: {s}");
            }
            other => panic!("expected malformed-partial error, got {other:?}"),
        }
    }

    /// Exercise the Phase 2 merge + global SORTBY/LIMIT path directly on
    /// in-process `ShardPartial`s — we don't need a real SPSC mesh to
    /// prove associative merge correctness plus global sort/limit.
    #[test]
    fn test_scatter_phase2_merges_counts_and_applies_global_sort_limit() {
        // Shard 0: open=2, closed=1; Shard 1: open=1, closed=3, new=4.
        let p0 = mk_partial(&[(b"open", 2), (b"closed", 1)]);
        let p1 = mk_partial(&[(b"open", 1), (b"closed", 3), (b"new", 4)]);

        // Round-trip both partials through the Frame codec so we exercise
        // what the SPSC path delivers.
        let f0 = encode_shard_partial(&p0);
        let f1 = encode_shard_partial(&p1);
        let d0 = decode_shard_partial(&f0).expect("decode s0");
        let d1 = decode_shard_partial(&f1).expect("decode s1");

        // Same merge path `scatter_text_aggregate` uses.
        let mut gb_fields_v: Vec<Bytes> = Vec::new();
        gb_fields_v.push(Bytes::from_static(b"@status"));
        let reducers = vec![ReducerSpec {
            fn_name: ReducerFn::Count,
            field: None,
            alias: Bytes::from_static(b"cnt"),
        }];
        let merged =
            crate::text::aggregate::merge_partial_states(vec![d0, d1], &gb_fields_v, &reducers);
        assert_eq!(merged.len(), 3, "three distinct status groups expected");

        // Apply global SORTBY DESC by cnt + LIMIT 0 2 — top-2 should be
        // (new=4), (closed=4). `closed` ties `new` at 4; stable sort
        // preserves input order but HashMap iteration is non-deterministic
        // — we only assert both top-2 counts equal 4.
        let mut sort_keys: SmallVec<[(Bytes, SortOrder); 4]> = SmallVec::new();
        sort_keys.push((Bytes::from_static(b"cnt"), SortOrder::Desc));
        let post = vec![
            AggregateStep::SortBy {
                keys: sort_keys,
                max: None,
            },
            AggregateStep::Limit {
                offset: 0,
                count: 2,
            },
        ];
        let final_rows = crate::text::aggregate::execute_pipeline(merged, &post).expect("ok");
        assert_eq!(final_rows.len(), 2);

        let top_count = |row: &crate::text::aggregate::AggregateRow| -> i64 {
            for (name, val) in row {
                if name.as_ref() == b"cnt" {
                    match val {
                        crate::text::aggregate::AggregateValue::Int(n) => return *n,
                        other => panic!("expected Int, got {other:?}"),
                    }
                }
            }
            panic!("cnt not found")
        };
        let c0 = top_count(&final_rows[0]);
        let c1 = top_count(&final_rows[1]);
        assert_eq!(c0, 4, "top-1 count must be 4");
        assert_eq!(c1, 4, "top-2 count must be 4 (closed or new, tied)");
    }

    /// Simulated multi-shard merge: two shard fixtures, no SPSC harness.
    /// Exercises the same merge + finalize used by `scatter_text_aggregate`
    /// Phase 2. 4-shard equivalence with 1-shard is covered by the
    /// existing `test_execute_pipeline_groupby_count` in text/aggregate.rs
    /// combined with the associative merge tests
    /// (`test_merge_partial_states_associative_count`).
    #[test]
    fn test_scatter_phase2_two_shards_merges_counts() {
        let p0 = mk_partial(&[(b"open", 2), (b"closed", 1)]);
        let p1 = mk_partial(&[(b"open", 1), (b"closed", 3)]);

        let mut gb_fields_v: Vec<Bytes> = Vec::new();
        gb_fields_v.push(Bytes::from_static(b"@status"));
        let reducers = vec![ReducerSpec {
            fn_name: ReducerFn::Count,
            field: None,
            alias: Bytes::from_static(b"cnt"),
        }];

        let merged =
            crate::text::aggregate::merge_partial_states(vec![p0, p1], &gb_fields_v, &reducers);
        assert_eq!(merged.len(), 2);

        let mut open = None;
        let mut closed = None;
        for row in &merged {
            let mut g = None;
            let mut c = None;
            for (name, val) in row {
                match name.as_ref() {
                    b"@status" => {
                        if let crate::text::aggregate::AggregateValue::Str(b) = val {
                            g = Some(b.clone());
                        }
                    }
                    b"cnt" => {
                        if let crate::text::aggregate::AggregateValue::Int(n) = val {
                            c = Some(*n);
                        }
                    }
                    _ => {}
                }
            }
            match g.as_deref() {
                Some(b"open") => open = c,
                Some(b"closed") => closed = c,
                other => panic!("unexpected group {other:?}"),
            }
        }
        assert_eq!(open, Some(3), "open = 2 (s0) + 1 (s1) = 3");
        assert_eq!(closed, Some(4), "closed = 1 (s0) + 3 (s1) = 4");
    }
}
