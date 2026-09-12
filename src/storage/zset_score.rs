//! Sorted-set score text: the one encoder and the one decoder.
//!
//! A `SortedSetListpack` stores `[member, score, member, score, …]`. The
//! listpack has no float entry type (Redis stores a `double` there; moon's
//! listpack stores integers and byte strings), so the score is kept as its
//! canonical decimal rendering — the same text `ZSCORE` replies with — and
//! the listpack re-encodes an integral rendering (`ZADD z 3.0 m` -> `3`) as an
//! integer entry, which is smaller than the string would be.
//!
//! Two properties are load-bearing and pinned by tests:
//!
//! * [`render_score`] is byte-identical to
//!   `command::sorted_set::format_score_bytes`, so a client reading a score
//!   back sees the same text whether the zset is a listpack or a B+tree.
//! * `render_score` -> [`parse_score`] is exact: Rust's `{}` for `f64` is the
//!   shortest round-trip rendering, so the stored text recovers the identical
//!   bits. Every consumer that decodes a listpack score (`SortedSetRef::score`,
//!   the value codec, both RDB writers, the AOF rewriter, `DEBUG DIGEST`)
//!   depends on that.
//!
//! This lives in `storage`, not `command`, because the RDB decode side
//! (`value_codec::compact_after_decode`) renders scores too, and storage must
//! not import from the command layer.

use smallvec::SmallVec;
use std::fmt::Write as _;

/// Inline capacity of a [`ScoreBuf`]. 32 bytes holds every shortest-round-trip
/// `f64` the way `{}` prints it (at most 17 significant digits plus sign and
/// point) EXCEPT the plain-decimal expansion of a huge or tiny magnitude:
/// `Display` never uses exponent notation, so `1e300` renders as 301 digits
/// and spills to the heap. That is a legal score and a rare one.
pub const SCORE_INLINE_BYTES: usize = 32;

/// Stack buffer for one rendered score. Allocation-free for every score a
/// benchmark or an application is likely to write; see [`SCORE_INLINE_BYTES`].
pub type ScoreBuf = SmallVec<[u8; SCORE_INLINE_BYTES]>;

/// `fmt::Write` adapter so `write!` can target a [`ScoreBuf`] directly.
struct Sink<'a>(&'a mut ScoreBuf);

impl std::fmt::Write for Sink<'_> {
    #[inline]
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.0.extend_from_slice(s.as_bytes());
        Ok(())
    }
}

// ── work budget (moon#942) ─────────────────────────────────────────────────
//
// Plain `//` comments on the macro invocation: a doc comment there trips
// `unused_doc_comments`, which CI denies.
//
// How many scores were rendered through `core::fmt`'s `f64` Display — the
// shortest-round-trip (Grisu/Dragon) formatter — rather than through the
// integer fast path. Redis's `d2string` takes the same fork: `double2ll`
// first, `ll2string` when it succeeds, `fpconv_dtoa` only when it does not.
// Test-only; compiles to nothing in a release build.
#[cfg(test)]
thread_local! {
    static FLOAT_FORMATS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// Record one `core::fmt` `f64` rendering. No-op outside test builds.
#[cfg(test)]
#[inline]
fn note_float_format() {
    FLOAT_FORMATS.with(|c| c.set(c.get() + 1));
}

/// No-op in non-test builds — zero production cost.
#[cfg(not(test))]
#[inline(always)]
fn note_float_format() {}

/// Read and reset the per-thread `core::fmt` float-rendering counter.
///
/// `pub(crate)` so the sorted-set command tests can measure a whole `ZADD`.
#[cfg(test)]
pub(crate) fn take_float_formats() -> u32 {
    FLOAT_FORMATS.with(|c| c.replace(0))
}

/// Render `score` into `out` (cleared first), exactly as `ZSCORE` replies it:
/// `inf` / `-inf` for the infinities, otherwise Rust's shortest round-trip
/// `{}` rendering (`3` for `3.0`, `1000` for `1e3`, `3.5` for `3.5000`).
///
/// NaN is not a score — `ZADD` rejects it before any encoder runs — and would
/// render as `NaN`, which [`parse_score`] refuses; there is no silent path
/// for it through a listpack.
#[inline]
pub fn render_score(score: f64, out: &mut ScoreBuf) {
    out.clear();
    if score == f64::INFINITY {
        out.extend_from_slice(b"inf");
    } else if score == f64::NEG_INFINITY {
        out.extend_from_slice(b"-inf");
    } else if let Some(v) = integral_score(score) {
        let mut buf = itoa::Buffer::new();
        out.extend_from_slice(buf.format(v).as_bytes());
    } else {
        note_float_format();
        // `write!` into a `SmallVec` cannot fail.
        let _ = write!(Sink(out), "{score}");
    }
}

/// The largest magnitude the integer fast path accepts: 2^53, the last point
/// where every integral `f64` is exactly representable.
///
/// Above it the `f64` grid is coarser than the integers, so `score as i64` is
/// still exact for the values that land on the grid but the DECIMAL SPELLING
/// stops being the shortest round-trip one — `1e17` is exactly representable
/// and `{}` prints its 18 digits, but so would `itoa`, and at wider exponents
/// the shortest-round-trip digits and the exact integer diverge. The cutoff is
/// where the proof is easy, not where the arithmetic first breaks; Redis's
/// `double2ll` draws it one binade lower still, at 2^52.
const INTEGRAL_RENDER_LIMIT: f64 = 9_007_199_254_740_992.0; // 2^53

/// `Some(n)` when `score` is an integer whose `itoa` rendering is
/// byte-identical to `{score}` — the fork Redis's `d2string` takes with
/// `double2ll` before it reaches `fpconv_dtoa`.
///
/// Three exclusions, each one a place where the two renderings differ:
///
/// * **`-0.0`.** `{}` prints `-0`; `0i64` prints `0`. `ZADD z -0 m` must keep
///   answering `-0`, and the value codec must keep round-tripping the sign
///   bit, so the negative zero goes down the slow path.
/// * **Non-integers**, where there is nothing to render as an integer.
/// * **Anything past [`INTEGRAL_RENDER_LIMIT`]**, including both infinities
///   (`inf.fract()` is NaN) and NaN itself (which `ZADD` rejects long before
///   this, and which `{}` would print as `NaN`).
#[inline]
#[must_use]
pub fn integral_score(score: f64) -> Option<i64> {
    if score == 0.0 {
        // `-0.0 == 0.0`, so this arm catches both zeros and the sign bit
        // separates them.
        return if score.is_sign_negative() {
            None
        } else {
            Some(0)
        };
    }
    // NaN and the infinities fail both of these: `NaN.fract()` is NaN and
    // `inf.fract()` is NaN, neither of which equals 0.0.
    if score.fract() != 0.0 || score.abs() > INTEGRAL_RENDER_LIMIT {
        return None;
    }
    Some(score as i64)
}

/// Parse a score as stored by [`render_score`]. The grammar is the one
/// `ZADD` accepts (`str::parse::<f64>`, which takes `inf`/`-inf`), minus NaN.
///
/// `None` for a STORED score means in-memory corruption: the only writers are
/// the `ZADD` listpack path and the RDB decode-side re-derivation, both of
/// which go through `render_score`. Callers decide fail-closed (the value
/// codec refuses to persist) versus fail-open (a read answers nil).
#[inline]
pub fn parse_score(raw: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(raw).ok()?;
    let v: f64 = s.parse().ok()?;
    if v.is_nan() { None } else { Some(v) }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CASES: &[f64] = &[
        0.0,
        -0.0,
        1.0,
        -1.0,
        3.0,
        3.5,
        1e3,
        1e21,
        1e300,
        1e-7,
        1e-300,
        0.1 + 0.2,
        1.0000000000000002,
        f64::MAX,
        f64::MIN_POSITIVE,
        f64::EPSILON,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ];

    #[test]
    fn render_then_parse_is_exact_for_every_case() {
        let mut buf = ScoreBuf::new();
        for &v in CASES {
            render_score(v, &mut buf);
            let back = parse_score(&buf).unwrap_or_else(|| {
                panic!(
                    "{:?} rendered as {:?} and did not parse",
                    v,
                    String::from_utf8_lossy(&buf)
                )
            });
            assert_eq!(
                back.to_bits(),
                v.to_bits(),
                "{v:?} -> {:?} -> {back:?}: the round trip must be bit-exact",
                String::from_utf8_lossy(&buf)
            );
        }
    }

    #[test]
    fn integral_scores_render_without_a_fraction() {
        // This is what lets the listpack store them as integer entries.
        let mut buf = ScoreBuf::new();
        for (v, want) in [(3.0, "3"), (1e3, "1000"), (-7.0, "-7"), (3.5, "3.5")] {
            render_score(v, &mut buf);
            assert_eq!(std::str::from_utf8(&buf).unwrap(), want);
        }
    }

    #[test]
    fn infinities_render_as_redis_spells_them() {
        let mut buf = ScoreBuf::new();
        render_score(f64::INFINITY, &mut buf);
        assert_eq!(&buf[..], b"inf");
        render_score(f64::NEG_INFINITY, &mut buf);
        assert_eq!(&buf[..], b"-inf");
        assert_eq!(parse_score(b"inf"), Some(f64::INFINITY));
        assert_eq!(parse_score(b"-inf"), Some(f64::NEG_INFINITY));
    }

    #[test]
    fn ordinary_scores_stay_inline_and_huge_ones_spill() {
        let mut buf = ScoreBuf::new();
        render_score(0.1 + 0.2, &mut buf);
        assert!(!buf.spilled(), "a 19-char score must not touch the heap");
        render_score(1e300, &mut buf);
        assert_eq!(buf.len(), 301, "Display never uses exponents");
        assert!(buf.spilled());
    }

    #[test]
    fn parse_refuses_nan_and_garbage() {
        assert_eq!(parse_score(b"nan"), None);
        assert_eq!(parse_score(b"NaN"), None);
        assert_eq!(parse_score(b"notafloat"), None);
        assert_eq!(parse_score(b""), None);
        assert_eq!(parse_score(b"\xff"), None);
    }

    /// moon#942: the integer fast path must be INVISIBLE.
    ///
    /// `render_score` now forks the way Redis's `d2string` does — `double2ll`
    /// then `ll2string`, falling back to the float formatter — and the whole
    /// change is safe only if the two arms are byte-identical wherever the
    /// fast one is taken. This asserts that against `{}` DIRECTLY, not against
    /// another moon function: the ground truth is `core::fmt`, because every
    /// score moon ever wrote, persisted or replied with came out of it, and a
    /// listpack written before this commit must still read back the same.
    ///
    /// Mutation check: widen `INTEGRAL_RENDER_LIMIT` to `f64::MAX` and the
    /// `1e21` case reports `1000000000000000000000` against
    /// `-9223372036854775808` (the saturating `as i64`). Drop the `-0.0` arm
    /// from `integral_score` and the `-0.0` case reports `0` against `-0`.
    #[test]
    fn the_integer_fast_path_is_byte_identical_to_core_fmt() {
        fn reference(v: f64) -> String {
            if v == f64::INFINITY {
                "inf".to_owned()
            } else if v == f64::NEG_INFINITY {
                "-inf".to_owned()
            } else {
                format!("{v}")
            }
        }

        let mut cases: Vec<f64> = CASES.to_vec();
        // The boundary of the fast path, from both sides, and the powers of
        // two either side of it.
        cases.extend([
            9_007_199_254_740_992.0,  // 2^53, the limit itself
            -9_007_199_254_740_992.0, // and its negation
            9_007_199_254_740_991.0,  // 2^53 - 1
            9_007_199_254_740_994.0,  // the next f64 above 2^53
            4_503_599_627_370_496.0,  // 2^52, Redis's own double2ll bound
            -4_503_599_627_370_496.0,
            1e15,
            1e16,
            1e17,
            1e18,
            1e19,
            1e21,
            -1e21,
            f64::MIN,
            -0.0,
            0.0,
            127.0,
            128.0,
            -128.0,
            -129.0,
            4095.0,
            4096.0,
            32767.0,
            32768.0,
            2_147_483_647.0,
            2_147_483_648.0,
            i64::MAX as f64,
            i64::MIN as f64,
            0.5,
            -0.5,
            1.0000000000000002,
            -3.25,
        ]);
        // Every integer with a short decimal spelling, plus their negations.
        for i in -1100i64..=1100 {
            cases.push(i as f64);
        }
        // A deterministic sweep of bit patterns: an xorshift over the f64
        // domain, NaNs skipped (ZADD rejects them before any renderer runs).
        let mut state: u64 = 0x2545_F491_4F6C_DD1D;
        for _ in 0..20_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let v = f64::from_bits(state);
            if v.is_nan() {
                continue;
            }
            cases.push(v);
            // And its integral neighbour, so the fast path is actually hit
            // by the sweep rather than only by the hand-written cases.
            if v.is_finite() && v.abs() < 1e18 {
                cases.push(v.trunc());
            }
        }

        let mut buf = ScoreBuf::new();
        let mut fast_path_hits = 0usize;
        for v in cases {
            render_score(v, &mut buf);
            let got = std::str::from_utf8(&buf).expect("render_score writes ASCII");
            assert_eq!(
                got,
                reference(v),
                "render_score({:?}) [bits {:#018x}] diverged from core::fmt",
                v,
                v.to_bits()
            );
            if integral_score(v).is_some() {
                fast_path_hits += 1;
            }
        }
        assert!(
            fast_path_hits > 2000,
            "only {fast_path_hits} cases took the integer fast path — the \
             sweep stopped exercising what it exists to check"
        );
    }

    /// The fast path must also stay round-trip exact, which is what every
    /// listpack reader depends on (`SortedSetRef::score`, the value codec,
    /// both RDB writers, the AOF rewriter, `DEBUG DIGEST`).
    #[test]
    fn the_integer_fast_path_round_trips_bit_exactly() {
        let mut buf = ScoreBuf::new();
        for i in [
            0i64,
            1,
            -1,
            7,
            -7,
            127,
            128,
            -129,
            4096,
            65_535,
            1 << 31,
            -(1i64 << 31),
            (1i64 << 53) - 1,
            1i64 << 53,
            -(1i64 << 53),
        ] {
            let v = i as f64;
            render_score(v, &mut buf);
            let back = parse_score(&buf).unwrap_or_else(|| {
                panic!(
                    "{v:?} rendered as {:?} and did not parse",
                    String::from_utf8_lossy(&buf)
                )
            });
            assert_eq!(back.to_bits(), v.to_bits(), "{v:?} did not round-trip");
        }
    }

    /// `-0.0` is the one zero the fast path must refuse: `{}` prints `-0`,
    /// `itoa` of `0i64` prints `0`, and `ZADD z -0 m` must keep answering
    /// `-0` the way it always has.
    #[test]
    fn negative_zero_keeps_its_sign() {
        assert_eq!(
            integral_score(-0.0),
            None,
            "-0.0 must not take the fast path"
        );
        assert_eq!(integral_score(0.0), Some(0), "+0.0 must take it");
        let mut buf = ScoreBuf::new();
        render_score(-0.0, &mut buf);
        assert_eq!(std::str::from_utf8(&buf).unwrap(), "-0");
        render_score(0.0, &mut buf);
        assert_eq!(std::str::from_utf8(&buf).unwrap(), "0");
    }

    /// Neither infinity nor NaN may reach the integer conversion: `as i64`
    /// saturates rather than failing, so an unguarded fast path would print
    /// `9223372036854775807` for `inf`.
    #[test]
    fn non_finite_scores_never_take_the_integer_path() {
        assert_eq!(integral_score(f64::INFINITY), None);
        assert_eq!(integral_score(f64::NEG_INFINITY), None);
        assert_eq!(integral_score(f64::NAN), None);
    }
}
