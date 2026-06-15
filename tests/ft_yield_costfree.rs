//! ADD task `ft-yield-costfree-monoio` §4 TESTS — runtime-red behavioral pins.
//!
//! These use ONLY existing public symbols, so the crate compiles today; they are
//! RED because the values/behavior they assert do not exist until §5 BUILD:
//!   - the brute-force yield chunk default is still 16384 on main (the timer-park
//!     knee). After the cost-free self-pipe yield lands it MUST drop to the
//!     build-measured knee 512 (verify-phase A/B: +2.74% vs sync at 20k×384d).
//!
//! The mechanism-level proofs (cost-free overhead, co-located relief, fallback)
//! live as monoio+linux unit tests in `src/runtime/mod.rs` (they need a live
//! monoio io_uring runtime in-process). The QPS-recovery A/B is a verify-phase
//! VM bench, not a CI unit test.
//!
//! Running:  cargo test --test ft_yield_costfree

use moon::vector::segment::holder::{FT_SEARCH_YIELD_BUDGET, ft_search_yield_budget};

/// M (Must: re-tune chunk) — the compile-time default brute-force chunk must be
/// re-tuned DOWN from the timer-park-era 16384 to the build-measured knee 512
/// once the per-yield cost is ~µs (verify A/B: +2.74% vs a sync control, 2× margin
/// under the 5% throughput bound; 256 lands on the line at 384d).
/// RED on main (== 16384); GREEN after the self-pipe yield + re-tune.
#[test]
fn chunk_default_retuned_to_512() {
    assert_eq!(
        FT_SEARCH_YIELD_BUDGET.max_brute_force_vecs_per_chunk, 512,
        "brute-force chunk default must be the build-measured knee 512 after the \
         cost-free yield re-tune (down from the timer-era 16384); got {}",
        FT_SEARCH_YIELD_BUDGET.max_brute_force_vecs_per_chunk
    );
}

/// M (Must: env override preserved) — GUARD. `MOON_FT_YIELD_CHUNK` must still win
/// over the default. This binary sets it before the first resolve (OnceLock cache).
/// Green on main and after; it protects the operator knob across the re-tune.
#[test]
fn chunk_env_override_still_honored() {
    // SAFETY of ordering: this is the only test in this binary that reads the
    // budget, so the OnceLock is first-initialized here with the override set.
    unsafe {
        std::env::set_var("MOON_FT_YIELD_CHUNK", "2048");
    }
    assert_eq!(
        ft_search_yield_budget().max_brute_force_vecs_per_chunk,
        2048,
        "MOON_FT_YIELD_CHUNK override must take precedence over the default"
    );
}
