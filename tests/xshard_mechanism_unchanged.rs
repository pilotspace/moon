//! Measure-only tripwire for task `xshard-read-gcloud-validation` (§4 / §3 C5).
//!
//! This task VALIDATES the shipped cross-shard read C2 fast-path on a trustworthy
//! absolute instrument (GCloud bare-metal); it must NOT change the mechanism. These
//! green-pins assert the C2 gate constants are byte-identical to the shipped values.
//! If a build re-tunes the gate (scope_creep_into_fix, §1 reject), one of these flips
//! red — the deterministic half of the measure-only invariant whose other half is the
//! evidence-gated `git diff --stat src/ == empty` check recorded in §6.
//!
//! NOT a behavior test (that is `xshard_fastpath_api.rs`); purely a "did the numbers
//! that define the mechanism move" pin.

use moon::shard::slice::{XSHARD_SPIN_GATE, XSHARD_SPIN_MAX_BATCH_REMOTE, xshard_should_spin};

/// The near-idle inclusive bound the reply-side spin gates on. Shipped value = 2.
#[test]
fn c2_spin_gate_const_pinned() {
    assert_eq!(
        XSHARD_SPIN_GATE, 2,
        "XSHARD_SPIN_GATE moved — the measure-only task must not re-tune the C2 gate (scope_creep_into_fix)"
    );
}

/// The singleton batch-depth bound (only a lone foreign read spins; a pipeline parks).
/// Shipped value = 1.
#[test]
fn c2_max_batch_remote_const_pinned() {
    assert_eq!(
        XSHARD_SPIN_MAX_BATCH_REMOTE, 1,
        "XSHARD_SPIN_MAX_BATCH_REMOTE moved — pipeline batch-gate retuned; forbidden in a measure-only task"
    );
}

/// The shipped decision shape: a singleton (batch_remote == 1) is spin-eligible; a
/// pipelined batch (batch_remote > MAX) is never spin-eligible regardless of idleness.
/// Pins the gate's STRUCTURE, not its idle state (which is thread-local runtime state).
#[test]
fn c2_should_spin_rejects_pipelined_batch() {
    assert!(
        !xshard_should_spin(XSHARD_SPIN_MAX_BATCH_REMOTE + 1),
        "a pipelined cross-shard batch must park, never spin (the s4-P16 −27% guard)"
    );
}
