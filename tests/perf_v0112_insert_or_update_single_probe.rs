//! PERF-08 timing net: `DashTable::insert_or_update` (one fused SIMD probe)
//! vs the legacy `get_mut` + `insert` pattern (two probes on a miss).
//!
//! The original semantic-parity test against `Database::data_mut()` was
//! removed when `data_mut()` was deleted in e2ce03b (its only callers
//! were tests; the legacy path is now unreachable from the public API).
//!
//! # What this test measures, and what it does NOT (moon#789)
//!
//! It measures a **wall-clock ratio**. A wall-clock ratio is not a structural
//! guarantee, and on a loaded CI runner it is barely a measurement at all
//! unless it is estimated carefully. The single-probe *property* — that the
//! fused path scans strictly fewer control-byte groups on a miss than
//! `get_mut` + `insert` does — is gated deterministically by
//! `test_insert_or_update_scans_fewer_groups_than_get_mut_plus_insert` in
//! `src/storage/dashtable/segment/mod.rs`. **That** test, not this one, is the
//! regression net for PERF-08. It is arch-independent, load-independent and
//! exact. This test guards the weaker, but still useful, claim that the fused
//! path's cost has not blown up.
//!
//! # Why this file was rewritten (moon#789)
//!
//! ## Round 1 — the measurement was an artifact
//!
//! The original version timed a control loop and a test loop back to back in
//! one process, always control first, and kept the **best** of five ratios.
//! Its comment asserted that runner noise "can only *inflate* the ratio toward
//! 1.0, never deflate it". That was false, and the bias was systematic:
//!
//! * The first timed loop in the process takes ~5,000 minor page faults that no
//!   later loop takes (measured via `/proc/self/stat`: `minflt = 5061` for
//!   loop #1, `0` for every loop after). It is a one-time process warm-up cost
//!   and it landed on whichever loop ran first — always the control.
//! * Best-of-K then *deterministically selected that rep*, because it is the
//!   rep where the control is handicapped. Best-of-K deflated the ratio.
//!
//! Swapping the loop order moved the artifact onto the test loop (aarch64
//! rep 0: 0.779 with `control minflt = 5061` → 1.190 with `test minflt = 5061`),
//! confirming it positional. **The aarch64 pass was the artifact**: warmed, the
//! old mixed workload gives a median of 0.975 on aarch64 — above its own 0.95
//! threshold. The net had been green on aarch64 for the wrong reason.
//!
//! ## Round 2 — a median over a bad estimator still flakes
//!
//! Round 1 replaced best-of-K with a median over warmed, order-alternating
//! whole-loop timings. That is unbiased but *noisy*, and it false-failed on a
//! developer's 12-core macOS host under parallel-build load. Measuring the
//! A/A noise floor (the same legacy loop timed against itself, so the true
//! ratio is exactly 1.000) shows why — GCE, 1M keys, 15 reps:
//!
//! | estimator | host | condition | A/A min..max | A/A spread |
//! |-----------|------|-----------|--------------|------------|
//! | whole-loop | t2a aarch64 | idle | 0.973..1.046 | 0.073 |
//! | whole-loop | t2a aarch64 | 16 spinners / 8 vCPU | 0.880..1.179 | **0.299** |
//! | whole-loop | c3 x86_64 | idle | 0.899..1.045 | 0.146 |
//! | whole-loop | c3 x86_64 | 16 spinners / 8 vCPU | 0.559..1.795 | **1.236** |
//! | chunked | t2a aarch64 | idle | 0.997..1.004 | 0.007 |
//! | chunked | t2a aarch64 | 16 spinners / 8 vCPU | 0.950..1.057 | 0.107 |
//! | chunked | c3 x86_64 | idle | 0.996..1.000 | 0.004 |
//! | chunked | c3 x86_64 | 16 spinners / 8 vCPU | 0.867..1.093 | 0.227 |
//!
//! Under load a whole-loop estimator returns ratios from 0.56 to 1.80 **on
//! identical code**. No threshold survives that. The fix is not more reps — it
//! is a better estimator.
//!
//! # The estimator: paired chunk interleaving
//!
//! Instead of timing one 400 ms control fill and then one 400 ms test fill,
//! the fill is split into `CHUNKS` slices and the two sides alternate slice by
//! slice, each accumulating its own elapsed time. A scheduler steal now lands
//! inside some slice of *both* sides rather than entirely on one of them, and
//! the paired difference cancels. Measured above: 3x tighter on aarch64 and
//! 5x tighter on x86_64 under load, and the A/A ratio centres on 1.00 (0.997
//! aarch64, 0.998 x86_64) — i.e. the estimator is unbiased, not merely quiet.
//!
//! Both tables are alive at once here, which is deliberate: it is symmetric,
//! and the alternation means neither side owns the warm cache.
//!
//! # What the corrected measurement says
//!
//! Chunk-interleaved, warmed, median of five. moon @ 7678156f, 1M keys, GCE.
//! Eight runs per arch of this exact test binary, medians:
//!
//! | | aarch64 (t2a, Neoverse-N1) | x86_64 (c3, Xeon 8481C) |
//! |---|---|---|
//! | idle, 3 runs | 0.895, 0.899, 0.900 | 1.129, 1.133, 1.134 |
//! | loaded, 5 runs | 0.888..0.927 | 1.113..1.140 |
//!
//! "Loaded" is 16 spinners on 8 vCPU, which drove 1-minute load average past
//! 30 by the end of the sweep — well beyond anything CI should produce.
//! 16/16 green, and the medians barely move between idle and loaded. That is
//! the paired estimator doing its job; the same sweep with a whole-loop
//! estimator produced individual x86_64 A/A ratios from 0.56 to 1.80.
//!
//! **The fused path's win is real on aarch64 and inverted on x86_64.** On the
//! miss path — the only path PERF-08 changes — it is ~10% faster on
//! Neoverse-N1 and ~11-14% *slower* on Xeon 8481C, at identical work. The fused
//! helper issues strictly fewer SIMD probes, so the x86_64 loss is a
//! codegen/microarchitecture effect, not extra work. Left open on moon#789; it
//! is not a correctness problem and `Database::set` still issues one probe.
//!
//! # What this test asserts, and why both arms are ceilings
//!
//! Both arms assert only that the fused path's cost has not blown up. Neither
//! asserts a win, on either arch, even though the win is real on aarch64.
//!
//! PERF-08's actual claim is held by two deterministic tests that count
//! control-byte group scans — one at the `Segment::insert_or_update_at` level
//! and one at the `DashTable::insert_or_update` level that `Database::set`
//! calls. They are exact, arch-independent, load-independent, and run in
//! microseconds. Forcing the PERF-09 fallback scan on every miss
//! (`has_non_home_keys` -> `true`) makes them report "fused scanned 6, legacy
//! scanned 5" and fail on both arches. A wall clock cannot do that, and does
//! not need to.
//!
//! What the wall clock still earns its place for is the regression the
//! counters are blind to: same number of group scans, more time per scan — a
//! lost `#[inline]`, an added copy, a more expensive key comparison. A ceiling
//! catches that without needing a quiet runner.
//!
//! The ceilings are per-arch because the *baseline* is per-arch, not because
//! the two arms claim different things.

use bytes::Bytes;
use moon::storage::compact_key::CompactKey;
use moon::storage::dashtable::DashTable;
use std::hint::black_box;
use std::time::Instant;

const N: usize = 1_000_000;

/// Slices per fill. 64 gives ~6 ms of work per slice at N = 1M, comfortably
/// finer than a scheduler quantum, so a steal cannot land on one side only.
const CHUNKS: usize = 64;

/// Median of five. With the paired estimator the samples are tight enough that
/// five is plenty; the reps are there to reject an outright preemption spike,
/// not to average away a biased estimator.
const REPS: usize = 5;

/// aarch64 blow-up ceiling. Healthy medians measured 0.888-0.927 across 8 runs
/// (idle and under heavy CPU oversubscription); the forced-fallback regression
/// measures ~1.10. 1.02 clears healthy by ~10% and catches that regression by
/// ~8%.
///
/// This is deliberately NOT the old `< 0.95` "it is faster" assertion. The win
/// is real on aarch64 (0.88, see the table above) but asserting it here put a
/// wall-clock floor on the arch that runs the merge bar — `ci-local.sh` drives
/// its VM suites on aarch64, and there is a hosted macOS aarch64 leg that
/// cannot be validated from Linux under this project's measurement rules. The
/// win claim now lives in the two deterministic probe-count tests instead,
/// which catch a structural regression exactly and in microseconds.
#[cfg(target_arch = "aarch64")]
const MAX_RATIO: f64 = 1.02;

/// x86_64 blow-up ceiling. The fused path is measurably *slower* here: healthy
/// medians 1.113-1.140 across 8 runs (idle and loaded), against a
/// forced-fallback regression at ~1.34. Same role as the aarch64 arm — catch a
/// cost blow-up, claim nothing about a win. See moon#789.
#[cfg(target_arch = "x86_64")]
const MAX_RATIO: f64 = 1.30;

/// Any other arch: unmeasured. Conservative ceiling, so a new target reports a
/// number rather than failing on a claim nobody has validated there.
#[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
const MAX_RATIO: f64 = 1.30;

type Table = DashTable<CompactKey, u64>;

/// Legacy pattern over `pre[lo..hi]`: probe with `get_mut`, and on a miss probe
/// again inside `insert`. Builds the `CompactKey` in the miss branch, which is
/// what `Database::set` did before PERF-08.
fn control_range(dt: &mut Table, pre: &[Bytes], lo: usize, hi: usize) -> u128 {
    let t = Instant::now();
    // Iterate the sub-slice rather than indexing `pre[i]`: same work, but it
    // satisfies `clippy::needless_range_loop`, and both loops below use the
    // identical idiom so the comparison stays symmetric.
    for (offset, k) in pre[lo..hi].iter().enumerate() {
        let i = lo + offset;
        let lk = k.clone();
        if let Some(old) = dt.get_mut(lk.as_ref()) {
            *old = i as u64;
        } else {
            dt.insert(CompactKey::from(lk), i as u64);
        }
    }
    t.elapsed().as_nanos()
}

/// Single-probe pattern over `pre[lo..hi]`: one fused find-or-insert per key.
///
/// Every key is distinct, so every call takes the miss path — the only path
/// PERF-08 changes, and the shape where both loops build exactly one
/// `CompactKey` per iteration, so the comparison is equal work.
fn test_range(dt: &mut Table, pre: &[Bytes], lo: usize, hi: usize) -> u128 {
    let t = Instant::now();
    // Same iteration idiom as `control_range` — see the note there.
    for (offset, k) in pre[lo..hi].iter().enumerate() {
        let i = lo + offset;
        dt.insert_or_update(CompactKey::from(k.clone()), |v| *v = i as u64, || i as u64);
    }
    t.elapsed().as_nanos()
}

/// One paired rep: fill two fresh tables in interleaved slices, accumulating
/// each side's elapsed time separately. Returns
/// `(control_ns, test_ns, control_len, test_len)`.
///
/// Both tables are allocated *outside* any timer. `black_box` on each finished
/// table pins the observable result so neither fill can be optimised away.
fn paired_rep(pre: &[Bytes]) -> (u128, u128, usize, usize) {
    let mut control: Table = DashTable::with_capacity(N);
    let mut test: Table = DashTable::with_capacity(N);
    let (mut control_ns, mut test_ns) = (0u128, 0u128);
    for c in 0..CHUNKS {
        let lo = c * N / CHUNKS;
        let hi = (c + 1) * N / CHUNKS;
        // Alternate which side leads each slice as well, so any systematic
        // lead/follow effect cancels within a single rep.
        if c % 2 == 0 {
            control_ns += control_range(&mut control, black_box(pre), lo, hi);
            test_ns += test_range(&mut test, black_box(pre), lo, hi);
        } else {
            test_ns += test_range(&mut test, black_box(pre), lo, hi);
            control_ns += control_range(&mut control, black_box(pre), lo, hi);
        }
    }
    let lens = (black_box(control.len()), black_box(test.len()));
    (control_ns, test_ns, lens.0, lens.1)
}

#[test]
fn test_insert_or_update_single_probe_miss_path_ratio() {
    let pre: Vec<Bytes> = (0..N).map(|i| Bytes::from(format!("t_{:08}", i))).collect();

    // Warm-up. The first timed loop in this process pays ~5,000 minor page
    // faults that no later loop pays (moon#789). Burn it on an unmeasured rep.
    let _ = paired_rep(&pre);

    let mut ratios: Vec<f64> = Vec::with_capacity(REPS);
    for rep in 0..REPS {
        let (control_ns, test_ns, control_len, test_len) = paired_rep(&pre);

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
            "PERF-08 rep {}/{}: ratio {:.3} (control={}ns test={}ns over {} paired chunks)",
            rep + 1,
            REPS,
            ratio,
            control_ns,
            test_ns,
            CHUNKS
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
         This is a blow-up ceiling, not a win claim — see the module docs for the \
         measured per-arch baselines under both idle and loaded conditions (moon#789). \
         The estimator is warmed and chunk-interleaved, and its A/A noise floor stays \
         inside 0.87..1.10 even at 2x CPU oversubscription, so a median past the ceiling \
         is a real cost change, not a busy runner. If the two deterministic probe-count \
         tests in src/storage/dashtable/ are still green, the group-scan count is intact \
         and the cost went up per scan rather than in the number of scans. Samples: {:?}",
        REPS,
        median,
        MAX_RATIO,
        std::env::consts::ARCH,
        ratios
    );
}
