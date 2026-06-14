//! ADD task `xshard-read-fastpath` §4 TESTS — compile-red + runtime-red API pins
//! for the adaptive idle-gate mechanism (C1 of the frozen §3 contract).
//!
//! These are RED until §5 BUILD defines the symbols:
//!   - `moon::shard::slice::xshard_may_spin` / `XshardWaitGuard` / `XSHARD_SPIN_GATE`
//!   - `moon::shard::dispatch::CoalescedReadBatch`
//! Before build the imports below are UNRESOLVED → this test crate fails to compile.
//! That compile failure IS the red signal for this file (the other test crates are
//! independent and still build). After build, the crate compiles and the asserts pass.
//!
//! Per-thread isolation: `XSHARD_INFLIGHT` is a thread-local `Cell<u32>`; each test
//! mutates only its own thread's count and restores it (RAII drop), so the cargo
//! test thread-pool cannot leak state between these tests.
//!
//! Running:  cargo test --test xshard_fastpath_api

use moon::shard::slice::{xshard_may_spin, XshardWaitGuard, XSHARD_SPIN_GATE};

/// xrf1 — a near-idle shard (no in-flight cross-shard reply-waiters on this thread)
/// MUST allow the reply-side spin. This is the entire c1 latency win: when the
/// requesting connection is effectively alone, polling its reply skips the reply-side
/// cross-thread wake.
#[test]
fn idle_gate_allows_spin_when_alone() {
    assert!(
        xshard_may_spin(),
        "a fresh thread has 0 in-flight cross-shard waiters → the reply-side spin must be allowed"
    );
}

/// xrf1 / reject shard_starvation — above the gate the path MUST park, not spin.
/// This is the anti-starvation invariant: the spike proved a fixed spin=4096 cut
/// s4-c100 throughput −48% by stealing co-located connections' turns. The gate, not a
/// fixed budget, is the fix. Also pins `XshardWaitGuard`'s RAII inc-on-construct /
/// dec-on-drop contract.
#[test]
fn idle_gate_blocks_spin_above_gate() {
    let mut guards = Vec::new();
    // Hold one MORE than the gate allows ⇒ the shard is "busy" with cross-shard waiters.
    for _ in 0..=XSHARD_SPIN_GATE {
        guards.push(XshardWaitGuard::new());
    }
    assert!(
        !xshard_may_spin(),
        "with > XSHARD_SPIN_GATE in-flight reply-waiters the path must park (no spin) — anti-starvation"
    );

    drop(guards);
    assert!(
        xshard_may_spin(),
        "after the waiters drain (RAII decrement on drop), the spin is allowed again"
    );
}

/// xrf1 — the gate is exactly an upper bound: AT the gate, spin is still allowed;
/// one above, refused. Pins the `<=` boundary the contract specifies.
#[test]
fn idle_gate_boundary_is_inclusive() {
    let mut guards = Vec::new();
    for _ in 0..XSHARD_SPIN_GATE {
        guards.push(XshardWaitGuard::new());
    }
    // Exactly XSHARD_SPIN_GATE waiters held ⇒ still <= gate ⇒ may spin.
    assert!(
        xshard_may_spin(),
        "at exactly XSHARD_SPIN_GATE in-flight waiters the gate is still open (inclusive bound)"
    );
    guards.push(XshardWaitGuard::new()); // gate + 1
    assert!(
        !xshard_may_spin(),
        "one waiter past the gate closes it"
    );
    drop(guards);
}

/// xrf2 — the cross-connection coalescing message type exists and is referenceable.
/// Routing CORRECTNESS (read-your-writes, per-connection submission order) is proven
/// by the consistency suite (`scripts/test-consistency.sh` 197/197 @1/4/12), NOT here:
/// the frozen contract names the 197-suite + xrf-ryw as the oracle for C3.
#[test]
fn coalesced_read_batch_type_exists() {
    fn _assert_type_exists<T>() {}
    _assert_type_exists::<moon::shard::dispatch::CoalescedReadBatch>();
}
