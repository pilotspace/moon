//! RESP2 -> RESP3 reply-shape policy.
//!
//! Split deliberately in two halves (ADD task `resp3-type-fidelity`):
//!
//! 1. [`resp3_shape_of`] CLASSIFIES a command into a [`Resp3Shape`] from its
//!    name and arguments. Pure, allocation-free, never looks at the reply.
//! 2. [`apply_shape`] APPLIES that shape to the reply.
//!
//! The split exists because the two halves do not always run in the same
//! place. On the local path both happen at dispatch exit. On the cross-shard
//! path the reply comes back on a batch whose metadata carries only the
//! command NAME — the arguments are long gone — so classification happens at
//! ENQUEUE time and the 1-byte `Copy` tag rides along in `RemoteMeta`. Carrying
//! the arguments instead would mean an allocation per remote command on the
//! shard hot path, which `CLAUDE.md` forbids in `src/server/conn/`.
//!
//! Every shape below is transcribed from a live sweep against redis-server
//! 8.6.1 — never from Moon's own prior behavior. The predecessor of this
//! module (`maybe_convert_resp3`) keyed only on the command name, which is why
//! it could not express the rule that actually governs half these replies:
//! `WITHSCORES`, `WITHVALUES` and a `<count>` argument change the reply SHAPE.

use super::{Frame, FrameVec};
use bytes::Bytes;

/// The RESP3 shape a command's reply must take.
///
/// `Copy` and one byte wide so the cross-shard batch can carry it by value.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum Resp3Shape {
    /// Pass through unchanged. The default for every unlisted command, so
    /// adding a command to the policy is always opt-in and never implicit.
    #[default]
    None,
    /// Flat `[k, v, k, v]` array -> Map. HGETALL, CONFIG GET, XINFO STREAM.
    Map,
    /// Array -> Set. SMEMBERS/SINTER/SUNION/SDIFF, and SPOP with a count.
    Set,
    /// Bulk -> Double. ZSCORE, ZINCRBY.
    Double,
    /// `[bulk, bulk]` -> `[double, double]`, preserving Null. ZMSCORE.
    DoubleArray,
    /// Flat `[member, score, ...]` -> `[[member, double], ...]`.
    /// The WITHSCORES/WITHVALUES family, and ZPOPMIN/ZPOPMAX with a count.
    ScoredPairs,
    /// `[member, score]` -> `[member, double]`, NOT wrapped.
    /// ZPOPMIN/ZPOPMAX with no count — Redis keeps this one flat.
    ScoredFlat,
    /// Flat `[field, value, ...]` -> `[[field, value], ...]`, values untouched.
    /// HRANDFIELD WITHVALUES. Note this is an array of pairs, NOT a Map —
    /// the inversion that made redis-py raise on Moon.
    ValuePairs,
    /// `[[x, y], ...]` -> `[[double, double], ...]`, preserving Null. GEOPOS.
    CoordPairs,
    /// Bulk -> VerbatimString. CLIENT INFO.
    Verbatim,
}

/// True when `args` contains `token`, case-insensitively.
#[inline]
fn has_token(args: &[Frame], token: &[u8]) -> bool {
    args.iter().any(|a| match a {
        Frame::BulkString(b) | Frame::SimpleString(b) => b.eq_ignore_ascii_case(token),
        _ => false,
    })
}

/// True when `args[idx]` equals `token`, case-insensitively.
#[inline]
fn arg_is(args: &[Frame], idx: usize, token: &[u8]) -> bool {
    match args.get(idx) {
        Some(Frame::BulkString(b) | Frame::SimpleString(b)) => b.eq_ignore_ascii_case(token),
        _ => false,
    }
}

/// Classify a command into the RESP3 shape its reply must take.
///
/// `cmd_upper` must already be uppercase. `args` EXCLUDES the command name.
/// Allocation-free, and never inspects the reply.
#[inline]
pub fn resp3_shape_of(cmd_upper: &[u8], args: &[Frame]) -> Resp3Shape {
    match cmd_upper {
        // ---- unconditional ------------------------------------------------
        b"HGETALL" => Resp3Shape::Map,
        b"SMEMBERS" | b"SINTER" | b"SUNION" | b"SDIFF" => Resp3Shape::Set,
        b"ZSCORE" | b"ZINCRBY" => Resp3Shape::Double,
        b"ZMSCORE" => Resp3Shape::DoubleArray,
        b"GEOPOS" => Resp3Shape::CoordPairs,

        // ---- container commands: only ONE subcommand converts --------------
        // `CONFIG GET` is a Map; CONFIG SET/RESETSTAT/REWRITE are not.
        b"CONFIG" if arg_is(args, 0, b"GET") => Resp3Shape::Map,
        // `CLIENT INFO` is a Verbatim string; every other CLIENT subcommand
        // keeps its own type (CLIENT LIST is Bulk, CLIENT ID is Integer).
        b"CLIENT" if arg_is(args, 0, b"INFO") => Resp3Shape::Verbatim,
        // `XINFO STREAM` is a Map; XINFO GROUPS/CONSUMERS are arrays of maps
        // and are converted by their own handler, not here.
        b"XINFO" if arg_is(args, 0, b"STREAM") => Resp3Shape::Map,

        // ---- arg-dependent: the modifier decides the shape -----------------
        // Without WITHSCORES these are flat arrays of members, and converting
        // them would be just as wrong as not converting the WITHSCORES form.
        b"ZRANGE" | b"ZREVRANGE" | b"ZRANGEBYSCORE" | b"ZREVRANGEBYSCORE" | b"ZRANGEBYLEX"
        | b"ZREVRANGEBYLEX" | b"ZDIFF" | b"ZUNION" | b"ZINTER" | b"ZRANDMEMBER"
            if has_token(args, b"WITHSCORES") =>
        {
            Resp3Shape::ScoredPairs
        }
        b"HRANDFIELD" if has_token(args, b"WITHVALUES") => Resp3Shape::ValuePairs,

        // ZPOPMIN/ZPOPMAX: `<count>` present -> wrapped pairs; absent -> ONE
        // flat [member, score] pair. Redis really does change the nesting on
        // the presence of the count.
        b"ZPOPMIN" | b"ZPOPMAX" | b"BZPOPMIN" | b"BZPOPMAX" => {
            if args.len() >= 2 {
                Resp3Shape::ScoredPairs
            } else {
                Resp3Shape::ScoredFlat
            }
        }

        // SPOP/SRANDMEMBER-style count switch: `SPOP key` is a single Bulk,
        // `SPOP key <count>` is a Set.
        b"SPOP" if args.len() >= 2 => Resp3Shape::Set,

        _ => Resp3Shape::None,
    }
}

/// Apply a classified shape to a reply.
///
/// Returns `response` untouched when the protocol is RESP2, when the reply is
/// an Error or Null, or when the reply does not have the arity the shape
/// expects. A conversion never panics, never truncates and never partially
/// rewrites a reply — a malformed inner reply is passed through whole.
#[inline]
pub fn apply_shape(shape: Resp3Shape, response: Frame, proto: u8) -> Frame {
    if proto < 3 || shape == Resp3Shape::None {
        return response;
    }
    // Errors and nulls are shape-independent and always pass through.
    if matches!(&response, Frame::Error(_) | Frame::Null) {
        return response;
    }

    match shape {
        Resp3Shape::None => response,
        Resp3Shape::Map => array_to_map(response),
        Resp3Shape::Set => array_to_set(response),
        Resp3Shape::Double => bulk_to_double(response),
        Resp3Shape::DoubleArray => map_elements(response, bulk_to_double),
        Resp3Shape::ScoredPairs => pair_wrap(response, true),
        Resp3Shape::ScoredFlat => scored_flat(response),
        Resp3Shape::ValuePairs => pair_wrap(response, false),
        Resp3Shape::CoordPairs => {
            map_elements(response, |inner| map_elements(inner, bulk_to_double))
        }
        Resp3Shape::Verbatim => bulk_to_verbatim(response),
    }
}

/// Flat `[k, v, k, v]` -> `%{k: v, k: v}`.
fn array_to_map(frame: Frame) -> Frame {
    match frame {
        // An EMPTY array converts too: `HGETALL nosuchkey` and `CONFIG GET
        // nosuchparam` are `%0` in real Redis, not `*0`. Emptiness must never
        // change the reply TYPE — the miss path is the one clients hit most.
        Frame::Array(items) if items.len() % 2 == 0 => {
            let mut pairs = Vec::with_capacity(items.len() / 2);
            let mut iter = items.into_iter();
            while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
                pairs.push((k, v));
            }
            Frame::Map(pairs)
        }
        other => other,
    }
}

fn array_to_set(frame: Frame) -> Frame {
    match frame {
        Frame::Array(items) => Frame::Set(items),
        other => other,
    }
}

/// Bulk -> Double. A value that does not parse as a float stays Bulk: a
/// reply we cannot interpret is passed through, never guessed at.
fn bulk_to_double(frame: Frame) -> Frame {
    match frame {
        Frame::BulkString(ref s) => match std::str::from_utf8(s).map(str::parse::<f64>) {
            Ok(Ok(f)) => Frame::Double(f),
            _ => frame,
        },
        other => other,
    }
}

/// Bulk -> VerbatimString with the `txt` encoding hint Redis uses.
fn bulk_to_verbatim(frame: Frame) -> Frame {
    match frame {
        Frame::BulkString(data) => Frame::VerbatimString {
            encoding: Bytes::from_static(b"txt"),
            data,
        },
        other => other,
    }
}

/// Apply `f` to every element of an array, leaving the array itself intact.
fn map_elements(frame: Frame, f: impl Fn(Frame) -> Frame) -> Frame {
    match frame {
        Frame::Array(items) => Frame::Array(items.into_iter().map(f).collect()),
        other => other,
    }
}

/// Flat `[a, b, a, b]` -> `[[a, b], [a, b]]`.
///
/// With `score = true` the second element of each pair becomes a Double
/// (WITHSCORES); with `false` it is left alone (WITHVALUES).
///
/// An odd element count means the reply is not the flat pair list this shape
/// expects — it is returned unchanged rather than losing its tail.
fn pair_wrap(frame: Frame, score: bool) -> Frame {
    match frame {
        Frame::Array(items) if items.len() % 2 == 0 => {
            // Halving the outer length: the new Vec is half the size of the
            // one being consumed, never a second full-size copy.
            let mut out: Vec<Frame> = Vec::with_capacity(items.len() / 2);
            let mut iter = items.into_iter();
            while let (Some(a), Some(b)) = (iter.next(), iter.next()) {
                let b = if score { bulk_to_double(b) } else { b };
                out.push(Frame::Array(FrameVec::from_vec(vec![a, b])));
            }
            Frame::Array(FrameVec::from_vec(out))
        }
        other => other,
    }
}

/// `[member, score]` -> `[member, Double(score)]`, nesting untouched.
fn scored_flat(frame: Frame) -> Frame {
    match frame {
        Frame::Array(items) if items.len() == 2 => {
            let mut iter = items.into_iter();
            // SAFETY-FREE: length checked above, so both nexts are Some.
            let (Some(m), Some(s)) = (iter.next(), iter.next()) else {
                return Frame::Array(FrameVec::new());
            };
            Frame::Array(FrameVec::from_vec(vec![m, bulk_to_double(s)]))
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framevec;
    use bytes::Bytes;

    fn bulk(s: &'static str) -> Frame {
        Frame::BulkString(Bytes::from_static(s.as_bytes()))
    }

    fn args(list: &[&'static str]) -> Vec<Frame> {
        list.iter().map(|s| bulk(s)).collect()
    }

    // ---- classification ---------------------------------------------------

    #[test]
    fn withscores_decides_the_shape_not_the_command_name() {
        assert_eq!(
            resp3_shape_of(b"ZRANGE", &args(&["z", "0", "-1", "WITHSCORES"])),
            Resp3Shape::ScoredPairs
        );
        assert_eq!(
            resp3_shape_of(b"ZRANGE", &args(&["z", "0", "-1"])),
            Resp3Shape::None,
            "plain ZRANGE must stay a flat array"
        );
        // Case-insensitive, as the wire allows.
        assert_eq!(
            resp3_shape_of(b"ZRANGE", &args(&["z", "0", "-1", "withscores"])),
            Resp3Shape::ScoredPairs
        );
    }

    #[test]
    fn hrandfield_withvalues_is_pairs_never_a_map() {
        assert_eq!(
            resp3_shape_of(b"HRANDFIELD", &args(&["h", "1", "WITHVALUES"])),
            Resp3Shape::ValuePairs
        );
        assert_eq!(
            resp3_shape_of(b"HRANDFIELD", &args(&["h", "1"])),
            Resp3Shape::None
        );
    }

    #[test]
    fn zpopmin_nesting_depends_on_the_count() {
        assert_eq!(
            resp3_shape_of(b"ZPOPMIN", &args(&["z"])),
            Resp3Shape::ScoredFlat
        );
        assert_eq!(
            resp3_shape_of(b"ZPOPMIN", &args(&["z", "2"])),
            Resp3Shape::ScoredPairs
        );
    }

    #[test]
    fn spop_count_switches_to_a_set() {
        assert_eq!(resp3_shape_of(b"SPOP", &args(&["s"])), Resp3Shape::None);
        assert_eq!(resp3_shape_of(b"SPOP", &args(&["s", "2"])), Resp3Shape::Set);
    }

    #[test]
    fn only_the_named_subcommand_converts() {
        assert_eq!(
            resp3_shape_of(b"CONFIG", &args(&["GET", "maxmemory"])),
            Resp3Shape::Map
        );
        assert_eq!(
            resp3_shape_of(b"CONFIG", &args(&["SET", "maxmemory", "0"])),
            Resp3Shape::None
        );
        assert_eq!(
            resp3_shape_of(b"CLIENT", &args(&["INFO"])),
            Resp3Shape::Verbatim
        );
        assert_eq!(
            resp3_shape_of(b"CLIENT", &args(&["LIST"])),
            Resp3Shape::None
        );
        assert_eq!(
            resp3_shape_of(b"XINFO", &args(&["STREAM", "s"])),
            Resp3Shape::Map
        );
        assert_eq!(
            resp3_shape_of(b"XINFO", &args(&["GROUPS", "s"])),
            Resp3Shape::None
        );
    }

    #[test]
    fn predicates_are_not_in_the_policy_at_all() {
        // The predecessor converted all seven of these to Boolean. Redis
        // 8.6.1 answers Integer for every one, verified on the wire.
        for cmd in [
            &b"SISMEMBER"[..],
            b"HEXISTS",
            b"EXPIRE",
            b"PEXPIRE",
            b"PERSIST",
            b"SETNX",
            b"MSETNX",
        ] {
            assert_eq!(
                resp3_shape_of(cmd, &args(&["k", "v"])),
                Resp3Shape::None,
                "{} must not be converted",
                String::from_utf8_lossy(cmd)
            );
        }
    }

    #[test]
    fn incrbyfloat_is_not_in_the_policy_at_all() {
        // Redis keeps these Bulk so no precision is lost in a float round-trip.
        assert_eq!(
            resp3_shape_of(b"INCRBYFLOAT", &args(&["f", "0.1"])),
            Resp3Shape::None
        );
        assert_eq!(
            resp3_shape_of(b"HINCRBYFLOAT", &args(&["h", "f", "0.1"])),
            Resp3Shape::None
        );
    }

    #[test]
    fn an_unlisted_command_is_never_converted() {
        assert_eq!(resp3_shape_of(b"GET", &args(&["k"])), Resp3Shape::None);
        assert_eq!(resp3_shape_of(b"NOSUCHCMD", &[]), Resp3Shape::None);
    }

    // ---- application ------------------------------------------------------

    #[test]
    fn resp2_is_never_converted() {
        let arr = Frame::Array(framevec![bulk("k"), bulk("v")]);
        assert_eq!(apply_shape(Resp3Shape::Map, arr.clone(), 2), arr);
        assert_eq!(apply_shape(Resp3Shape::ScoredPairs, arr.clone(), 2), arr);
    }

    #[test]
    fn errors_and_nulls_pass_through_every_shape() {
        let err = Frame::Error(Bytes::from_static(b"WRONGTYPE nope"));
        assert_eq!(apply_shape(Resp3Shape::ScoredPairs, err.clone(), 3), err);
        assert_eq!(apply_shape(Resp3Shape::Map, err.clone(), 3), err);
        assert_eq!(
            apply_shape(Resp3Shape::Double, Frame::Null, 3),
            Frame::Null,
            "ZSCORE of an absent member stays Null"
        );
    }

    #[test]
    fn scored_pairs_wraps_and_doubles() {
        let flat = Frame::Array(framevec![bulk("a"), bulk("1"), bulk("b"), bulk("2")]);
        let got = apply_shape(Resp3Shape::ScoredPairs, flat, 3);
        assert_eq!(
            got,
            Frame::Array(framevec![
                Frame::Array(framevec![bulk("a"), Frame::Double(1.0)]),
                Frame::Array(framevec![bulk("b"), Frame::Double(2.0)]),
            ])
        );
    }

    #[test]
    fn value_pairs_wraps_without_doubling() {
        let flat = Frame::Array(framevec![bulk("f"), bulk("v")]);
        let got = apply_shape(Resp3Shape::ValuePairs, flat, 3);
        assert_eq!(
            got,
            Frame::Array(framevec![Frame::Array(framevec![bulk("f"), bulk("v")])]),
            "HRANDFIELD values are opaque strings and must not become Doubles"
        );
    }

    #[test]
    fn an_odd_element_count_passes_through_whole() {
        // A malformed inner reply must not lose its tail or panic.
        let odd = Frame::Array(framevec![bulk("a"), bulk("1"), bulk("b")]);
        assert_eq!(
            apply_shape(Resp3Shape::ScoredPairs, odd.clone(), 3),
            odd,
            "an odd-length scored reply is returned unchanged, not truncated"
        );
    }

    #[test]
    fn an_empty_map_reply_is_an_empty_map_not_an_empty_array() {
        // Oracle (redis-server 8.6.1, RESP3): `HGETALL nosuchkey` -> `%0\r\n`,
        // `CONFIG GET nosuchparam` -> `%0\r\n`. An empty result is still a map;
        // emptiness must not silently change the reply TYPE, or a client that
        // dispatches on the type byte breaks on exactly the miss path.
        assert_eq!(
            apply_shape(Resp3Shape::Map, Frame::Array(framevec![]), 3),
            Frame::Map(Vec::new()),
            "an empty map-shaped reply must serialize as %0, never *0"
        );
    }

    #[test]
    fn an_empty_scored_reply_stays_an_empty_array() {
        let empty = Frame::Array(framevec![]);
        assert_eq!(
            apply_shape(Resp3Shape::ScoredPairs, empty.clone(), 3),
            empty
        );
    }

    #[test]
    fn scored_flat_keeps_the_nesting() {
        let flat = Frame::Array(framevec![bulk("a"), bulk("1.5")]);
        assert_eq!(
            apply_shape(Resp3Shape::ScoredFlat, flat, 3),
            Frame::Array(framevec![bulk("a"), Frame::Double(1.5)]),
            "ZPOPMIN with no count is NOT wrapped"
        );
    }

    #[test]
    fn double_array_preserves_null() {
        let arr = Frame::Array(framevec![bulk("1.5"), Frame::Null]);
        assert_eq!(
            apply_shape(Resp3Shape::DoubleArray, arr, 3),
            Frame::Array(framevec![Frame::Double(1.5), Frame::Null]),
            "ZMSCORE keeps Null for an absent member"
        );
    }

    #[test]
    fn coord_pairs_doubles_both_axes() {
        let arr = Frame::Array(framevec![Frame::Array(framevec![
            bulk("13.361389"),
            bulk("38.115556")
        ])]);
        assert_eq!(
            apply_shape(Resp3Shape::CoordPairs, arr, 3),
            Frame::Array(framevec![Frame::Array(framevec![
                Frame::Double(13.361389),
                Frame::Double(38.115556)
            ])])
        );
    }

    #[test]
    fn an_unparseable_score_stays_bulk() {
        let flat = Frame::Array(framevec![bulk("a"), bulk("notafloat")]);
        assert_eq!(
            apply_shape(Resp3Shape::ScoredPairs, flat, 3),
            Frame::Array(framevec![Frame::Array(framevec![
                bulk("a"),
                bulk("notafloat")
            ])]),
            "a score we cannot parse is left alone; the rest of the reply still converts"
        );
    }

    #[test]
    fn map_and_set_and_verbatim() {
        assert_eq!(
            apply_shape(
                Resp3Shape::Map,
                Frame::Array(framevec![bulk("k"), bulk("v")]),
                3
            ),
            Frame::Map(vec![(bulk("k"), bulk("v"))])
        );
        assert_eq!(
            apply_shape(Resp3Shape::Set, Frame::Array(framevec![bulk("a")]), 3),
            Frame::Set(framevec![bulk("a")])
        );
        assert_eq!(
            apply_shape(Resp3Shape::Verbatim, bulk("id=1 addr=x"), 3),
            Frame::VerbatimString {
                encoding: Bytes::from_static(b"txt"),
                data: Bytes::from_static(b"id=1 addr=x"),
            }
        );
    }
}
