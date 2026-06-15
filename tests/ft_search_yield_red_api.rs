//! ADD task `ft-search-off-eventloop` §4 TESTS — compile-red API pins for the
//! cooperative-yield search seam (C1–C3 of the frozen §3 contract).
//!
//! RED until §5 BUILD defines the symbols in `moon::vector::segment::holder`:
//!   - `SearchSnapshot`            — C1: the owned capture (Arc<SegmentList> + owned
//!                                    SearchScratch + START-captured key map), `'static`.
//!   - `YieldBudget` + `FT_SEARCH_YIELD_BUDGET` — C3: the explicit bounded-yield cap.
//!   - `SegmentHolder::search_mvcc_yielding`     — C2: the async yielding seam.
//!
//! Before build the imports below are UNRESOLVED → this test crate fails to
//! compile. That compile failure IS the red signal for this file; every other
//! test crate is independent and still builds. After build the crate compiles
//! and the asserts pass.
//!
//! Running:  cargo test --test ft_search_yield_red_api   # compile error = red, by design

use moon::vector::segment::holder::{
    FT_SEARCH_YIELD_BUDGET, SearchSnapshot, SegmentHolder, YieldBudget,
};

/// C1 / M3 G-NOBORROW — the captured snapshot OWNS its state, so it can be held
/// across a `cooperative_yield().await` with NO borrow into `VectorStore`/`VectorIndex`.
/// A `'static` bound is exactly "owns everything it needs, borrows nothing of the index".
fn assert_static<T: 'static>() {}

#[test]
fn c1_search_snapshot_is_static_owned() {
    assert_static::<SearchSnapshot>();
}

/// C3 / M1 — the explicit bounded-yield cap exists with sane named defaults
/// (neither too coarse, leaving the co-located p99 unbounded, nor zero).
#[test]
fn c3_yield_budget_named_defaults() {
    let b: YieldBudget = FT_SEARCH_YIELD_BUDGET;
    assert!(
        b.max_segments_per_chunk >= 1,
        "C3: yield at least once per segment"
    );
    assert!(
        b.max_graph_nodes_per_chunk > 0,
        "C3: bounded chunk inside a large HNSW segment"
    );
    assert!(
        b.max_brute_force_vecs_per_chunk > 0,
        "C3: bounded chunk inside a large mutable brute-force scan"
    );
}

/// C2 — the yielding seam exists with the contracted shape: an async method on
/// `SegmentHolder` driving the chunked search against an owned `&mut SearchSnapshot`
/// under a `YieldBudget`. Referencing the method as a path value forces name +
/// arity resolution (compile-red until it exists). We do NOT call it here —
/// constructing a `SearchSnapshot` needs a live index; the existence pin is enough
/// (its result-identity is exercised by the runtime suite + verify differential).
#[test]
fn c2_search_mvcc_yielding_symbol_exists() {
    let _seam = SegmentHolder::search_mvcc_yielding;
    let _ = &_seam;
}
