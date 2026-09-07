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
    } else {
        // `write!` into a `SmallVec` cannot fail.
        let _ = write!(Sink(out), "{score}");
    }
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
}
