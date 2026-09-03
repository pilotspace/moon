//! PERF-08 timing net: `DashTable::insert_or_update` (one fused SIMD probe)
//! vs the legacy `get_mut` + `insert` pattern (two probes on a miss).
//!
//! The original semantic-parity test against `Database::data_mut()` was
//! removed when `data_mut()` was deleted in e2ce03b (its only callers
//! were tests; the legacy path is now unreachable from the public API).
//! The timing comparison below still exercises the optimisation directly
//! on `DashTable`.
//!
//! # What this test measures, and what it does NOT (moon#789)
//!
//! It measures a **wall-clock ratio**, and a wall-clock ratio is not a
//! structural guarantee. The single-probe *property* — that the fused path
//! never scans a control-byte group twice — is gated deterministically by
//! `test_segment_insert_or_update_at_probes_at_most_two_groups_on_miss` in
//! `src/storage/dashtable/segment/mod.rs`. **That** unit test, not this one,
//! is the regression net for PERF-08's structure. This test guards the
//! remaining, weaker claim: that the fused path's cost has not blown up.
//!
//! # Why this file was rewritten (moon#789)
//!
//! The previous version measured a control loop and a test loop back to back
//! in one process, always control first, and kept the **best** of five ratios.
//! Its comment asserted that runner noise "can only *inflate* the ratio toward
//! 1.0, never deflate it". That claim was false, and the failure mode it
//! created was not noise but a systematic bias:
//!
//! * The first timed loop in the process takes ~5,000 minor page faults that
//!   no later loop takes (measured via `/proc/self/stat` on both arches:
//!   `minflt = 5061` for loop #1, `0` for every loop after it). That is a
//!   one-time process warm-up cost, and it landed entirely on whichever loop
//!   ran first — which was always the control.
//! * Best-of-K then *deterministically selected that rep*, because it is the
//!   rep in which the control is handicapped. So best-of-K deflated the ratio.
//!
//! Measured on GCE `t2a-standard-8` (Neoverse-N1), moon @ 7678156f,
//! 2026-09-03, 1M keys:
//!
//! | rep | control ns | test ns | ratio | control minflt |
//! |-----|-----------|---------|-------|----------------|
//! | 0   | 516075424 | 402049716 | 0.779 | 5061 |
//! | 1   | 404898236 | 397043277 | 0.981 | 0 |
//! | 2   | 409555316 | 404051316 | 0.987 | 0 |
//! | 3   | 405751236 | 395974997 | 0.976 | 0 |
//! | 4   | 390301678 | 397372397 | 1.018 | 0 |
//!
//! Swapping the loop order moved the artifact onto the test loop instead
//! (rep 0 ratio 1.190, `test minflt = 5061`), confirming it is positional and
//! not a property of either code path. **The aarch64 pass was the artifact**:
//! warm the process up and the same mixed workload gives a median of 0.975 on
//! aarch64 — above the old 0.95 threshold. The regression net had been green
//! on aarch64 for the wrong reason.
//!
//! # What the corrected measurement says
//!
//! Warmed, isolated (one table alive at a time), order alternating per rep,
//! median of five. moon @ 7678156f, 2026-09-03, 1M keys, idle hosts:
//!
//! | workload | aarch64 (t2a, Neoverse-N1) | x86_64 (c3, Xeon 8481C) |
//! |----------|---------------------------|--------------------------|
//! | pure miss (100% insert, equal work) | **0.893** | **1.166** |
//! | mixed 75/25 (the old workload)      | 0.975     | 1.118     |
//!
//! Two conclusions, both measured:
//!
//! 1. **The fused path's win is real on aarch64 and inverted on x86_64.** On
//!    the miss path — the only path PERF-08 changes — it is ~11% faster on
//!    Neoverse-N1 and ~17% *slower* on Xeon 8481C, at identical work. The
//!    fused helper does strictly fewer SIMD probes than `find`+`find`+
//!    `find_free_slot_in_group`, so this is a codegen/microarchitecture
//!    effect, not extra work. Tracked in moon#789 for follow-up; it is not a
//!    correctness problem and `Database::set` still issues one probe.
//! 2. **The mixed workload cannot show the miss-path win**, because the legacy
//!    control builds a `CompactKey` only on a miss while the fused API
//!    requires one per call. That construction costs ~9.4 ns/key on aarch64
//!    and ~0 ns/key on x86_64 (measured), which on a 25%-hit workload gives
//!    back most of the aarch64 win. So this test measures the pure-miss
//!    workload, where both loops construct exactly one `CompactKey` per
//!    iteration in the same code shape and the comparison is honest.
//!
//! Thresholds below are per-arch because the measured reality is per-arch.
//! They are documented ceilings/floors around measured medians, not tuned
//! until green.

use bytes::Bytes;
use moon::storage::compact_key::CompactKey;
use moon::storage::dashtable::DashTable;
use std::hint::black_box;
use std::time::Instant;

const N: usize = 1_000_000;

/// Five reps with the loop order alternating, so any residual positional bias
/// cancels across the set, and the **median** is taken. Not best-of-K: see the
/// module docs — best-of-K selects the most biased rep, it does not reject it.
const REPS: usize = 5;

/// aarch64: the fused path must still be measurably faster on the miss path.
/// Measured median 0.893 (samples 0.888-0.908) on t2a-standard-8.
#[cfg(target_arch = "aarch64")]
const MAX_RATIO: f64 = 0.95;

/// x86_64: the fused path is measurably *slower* here (measured median 1.166,
/// samples 1.141-1.174 on c3-standard-8 / Xeon 8481C). This is a ceiling that
/// catches a blow-up — e.g. an accidental full-segment fallback scan on every
/// miss — not a claim that the optimisation wins on this arch. See moon#789.
#[cfg(target_arch = "x86_64")]
const MAX_RATIO: f64 = 1.30;

/// Any other arch: unmeasured. Use the conservative ceiling so a new target
/// reports a number instead of failing on a claim nobody has validated there.
#[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
const MAX_RATIO: f64 = 1.30;

/// Legacy pattern: probe with `get_mut`, and on a miss probe again inside
/// `insert`. Builds the `CompactKey` inside the miss branch, which is exactly
/// what `Database::set` did before PERF-08.
fn run_control(dt: &mut DashTable<CompactKey, u64>, pre: &[Bytes]) -> u128 {
    let t = Instant::now();
    for (i, k) in pre.iter().enumerate() {
        let lk = k.clone();
        if let Some(old) = dt.get_mut(lk.as_ref()) {
            *old = i as u64;
        } else {
            dt.insert(CompactKey::from(lk), i as u64);
        }
    }
    t.elapsed().as_nanos()
}

/// Single-probe pattern: one fused find-or-insert per key.
fn run_test(dt: &mut DashTable<CompactKey, u64>, pre: &[Bytes]) -> u128 {
    let t = Instant::now();
    for (i, k) in pre.iter().enumerate() {
        dt.insert_or_update(CompactKey::from(k.clone()), |v| *v = i as u64, || i as u64);
    }
    t.elapsed().as_nanos()
}

/// Fill a fresh, pre-sized table with `body` and return the elapsed ns.
///
/// The table is allocated *outside* the timer and dropped immediately after,
/// so exactly one table is alive per measurement and neither loop is charged
/// for the other's residency. `black_box` on the finished table keeps the fill
/// from being optimised out and pins the observable result.
fn timed(
    body: fn(&mut DashTable<CompactKey, u64>, &[Bytes]) -> u128,
    pre: &[Bytes],
) -> (u128, usize) {
    let mut dt: DashTable<CompactKey, u64> = DashTable::with_capacity(N);
    let ns = body(&mut dt, black_box(pre));
    let len = black_box(dt.len());
    drop(dt);
    (ns, len)
}

#[test]
fn test_insert_or_update_single_probe_miss_path_ratio() {
    let pre: Vec<Bytes> = (0..N).map(|i| Bytes::from(format!("t_{:08}", i))).collect();

    // Warm-up. The first timed loop in this process pays ~5,000 minor page
    // faults that no later loop pays (moon#789). Burn it here, on a pair that
    // is not measured, so it cannot be charged to either side.
    let _ = timed(run_control, &pre);
    let _ = timed(run_test, &pre);

    let mut ratios: Vec<f64> = Vec::with_capacity(REPS);
    for rep in 0..REPS {
        // Alternate which loop runs first so residual position effects cancel.
        let (control_ns, control_len, test_ns, test_len) = if rep % 2 == 0 {
            let (c, cl) = timed(run_control, &pre);
            let (t, tl) = timed(run_test, &pre);
            (c, cl, t, tl)
        } else {
            let (t, tl) = timed(run_test, &pre);
            let (c, cl) = timed(run_control, &pre);
            (c, cl, t, tl)
        };
        // Both loops must have built the same table, or the ratio is fiction.
        assert_eq!(
            control_len, test_len,
            "control and test tables disagree on length ({control_len} vs {test_len}); \
             the two loops are not doing the same work"
        );
        assert_eq!(
            control_len, N,
            "expected {N} distinct keys, got {control_len}"
        );

        let ratio = test_ns as f64 / control_ns as f64;
        eprintln!(
            "PERF-08 rep {}/{}: ratio {:.3} (control={}ns test={}ns)",
            rep + 1,
            REPS,
            ratio,
            control_ns,
            test_ns
        );
        ratios.push(ratio);
    }

    ratios.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let median = ratios[REPS / 2];
    eprintln!(
        "PERF-08 median-of-{} miss-path ratio: {:.3} (target <{:.2} on {}) samples={:?}",
        REPS,
        median,
        MAX_RATIO,
        std::env::consts::ARCH,
        ratios.iter().map(|r| format!("{r:.3}")).collect::<Vec<_>>()
    );
    assert!(
        median < MAX_RATIO,
        "insert_or_update median-of-{} miss-path timing ratio {:.3} >= {:.2} on {}. \
         See the module docs for the measured per-arch baselines (moon#789); a value \
         above the ceiling means the fused probe got materially more expensive, not \
         that the runner was noisy — the measurement is warmed, isolated and \
         order-alternating. Samples: {:?}",
        REPS,
        median,
        MAX_RATIO,
        std::env::consts::ARCH,
        ratios
    );
}
