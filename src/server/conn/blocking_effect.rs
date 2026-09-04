//! The AOF/replication record a blocking pop owes the durability planes
//! (moon#827).
//!
//! A blocking pop that actually pops is a write, but outside `MULTI` it
//! propagated nothing: the element was handed to the client, removed from the
//! master, and never appended to the AOF or streamed to a replica. It came
//! back on the next restart. Measured across all eight commands on both the
//! immediately-satisfiable and the parked-then-woken path — sixteen cases,
//! sixteen losses.
//!
//! ## Why the record is synthesised rather than the command itself
//!
//! Propagating the verbatim frame is not an option: a replica applying a
//! literal `BLPOP` would park its apply loop, and an AOF replaying one would
//! stall recovery. The record has to be the non-blocking sibling that
//! reproduces the same mutation.
//!
//! ## Why it is derived from the REPLY
//!
//! A blocking pop takes many keys and only one of them serves. The arguments
//! cannot say which — only the reply can, and it already does. This mirrors
//! [`crate::tracking::invalidation::blocking_served_keys`], which moon#644
//! added for exactly this reason on exactly this path; deriving both facts the
//! same way is what keeps the invalidation and the durability record talking
//! about the same key.
//!
//! Reading the reply also makes the two completion paths one case rather than
//! two. The immediately-satisfiable pop returns inline from `immediate_scan`;
//! the parked pop completes inside the wakeup machinery. They are different
//! code, but they converge on the same reply frame, so a record computed from
//! that frame covers both without either path being taught about propagation.
//!
//! ## The `COUNT` rule
//!
//! `BLMPOP`/`BZMPOP` take a requested count and may pop fewer. The record
//! carries the number ACTUALLY popped, counted from the reply — never the
//! requested one. A `COUNT 10` that popped 3 must not replay as 10 against a
//! replica that has since received more elements.

use bytes::Bytes;

use crate::framevec;
use crate::protocol::Frame;

/// Build a `BulkString` frame from a static byte slice.
#[inline]
fn bulk(s: &'static [u8]) -> Frame {
    Frame::BulkString(Bytes::from_static(s))
}

/// Build a `BulkString` frame holding the decimal text of `n`.
#[inline]
fn bulk_usize(n: usize) -> Frame {
    let mut b = itoa::Buffer::new();
    Frame::BulkString(Bytes::copy_from_slice(b.format(n).as_bytes()))
}

/// The `LEFT`/`RIGHT` (or `MIN`/`MAX`) selector of an `MPOP`-shaped argv.
///
/// `BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT n]` puts the
/// selector after a variable-length key list, so it is found by scanning for
/// it rather than by index. `COUNT`'s own value can never be mistaken for one:
/// it is a number.
fn mpop_direction(args: &[Frame], a: &'static [u8], b: &'static [u8]) -> Option<&'static [u8]> {
    for arg in args {
        let Frame::BulkString(v) = arg else { continue };
        if v.eq_ignore_ascii_case(a) {
            return Some(a);
        }
        if v.eq_ignore_ascii_case(b) {
            return Some(b);
        }
    }
    None
}

/// The first two arguments of a `BLMOVE`/`BRPOPLPUSH`, which are its source
/// and destination.
fn two_keys(args: &[Frame]) -> Option<(Bytes, Bytes)> {
    let (Some(Frame::BulkString(src)), Some(Frame::BulkString(dst))) = (args.first(), args.get(1))
    else {
        return None;
    };
    Some((src.clone(), dst.clone()))
}

/// The non-blocking command that reproduces the mutation `reply` records, or
/// `None` when nothing was written.
///
/// `None` covers every shape that mutated nothing and must therefore reach
/// neither plane: a timeout (either null spelling), an error, a reply from a
/// command that is not a blocking pop, and any reply whose shape does not
/// match what the command is supposed to answer — the last because a record
/// invented from a shape this function does not understand would corrupt a
/// replica far more cheaply than omitting it.
pub(crate) fn blocking_effect_record(cmd: &[u8], args: &[Frame], reply: &Frame) -> Option<Frame> {
    // A timeout or an error modified nothing. Checked first so no branch below
    // has to re-establish it.
    if matches!(reply, Frame::Null | Frame::NullArray | Frame::Error(_)) {
        return None;
    }

    // BLMOVE / BRPOPLPUSH: the reply is the moved element, so the keys come
    // from the arguments — both are fixed and both are modified.
    if cmd.eq_ignore_ascii_case(b"BLMOVE") {
        let (src, dst) = two_keys(args)?;
        // `BLMOVE src dst <wherefrom> <whereto> timeout` -> `LMOVE` with the
        // same two directions. They are positional, unlike the MPOP selector.
        let (Some(Frame::BulkString(from)), Some(Frame::BulkString(to))) =
            (args.get(2), args.get(3))
        else {
            return None;
        };
        return Some(Frame::Array(framevec![
            bulk(b"LMOVE"),
            Frame::BulkString(src),
            Frame::BulkString(dst),
            Frame::BulkString(from.clone()),
            Frame::BulkString(to.clone()),
        ]));
    }
    if cmd.eq_ignore_ascii_case(b"BRPOPLPUSH") {
        let (src, dst) = two_keys(args)?;
        return Some(Frame::Array(framevec![
            bulk(b"RPOPLPUSH"),
            Frame::BulkString(src),
            Frame::BulkString(dst),
        ]));
    }

    // Everything else answers an array whose FIRST element is the key that
    // served.
    let Frame::Array(items) = reply else {
        return None;
    };
    let Some(Frame::BulkString(key)) = items.first() else {
        return None;
    };
    let key = key.clone();

    // BLPOP/BRPOP -> `[key, value]`; BZPOPMIN/BZPOPMAX -> `[key, member,
    // score]`. One element either way, so the sibling needs no count.
    let single = if cmd.eq_ignore_ascii_case(b"BLPOP") {
        Some(b"LPOP".as_slice())
    } else if cmd.eq_ignore_ascii_case(b"BRPOP") {
        Some(b"RPOP".as_slice())
    } else if cmd.eq_ignore_ascii_case(b"BZPOPMIN") {
        Some(b"ZPOPMIN".as_slice())
    } else if cmd.eq_ignore_ascii_case(b"BZPOPMAX") {
        Some(b"ZPOPMAX".as_slice())
    } else {
        None
    };
    if let Some(sibling) = single {
        return Some(Frame::Array(framevec![
            Frame::BulkString(Bytes::copy_from_slice(sibling)),
            Frame::BulkString(key),
        ]));
    }

    // BLMPOP/BZMPOP -> `[key, [element, ...]]`. The popped count is the length
    // of that inner array — what actually happened, not what was asked for.
    let popped = match items.get(1) {
        Some(Frame::Array(elems)) => elems.len(),
        _ => return None,
    };
    if popped == 0 {
        return None;
    }
    let sibling: &'static [u8] = if cmd.eq_ignore_ascii_case(b"BLMPOP") {
        match mpop_direction(args, b"LEFT", b"RIGHT")? {
            b"LEFT" => b"LPOP",
            _ => b"RPOP",
        }
    } else if cmd.eq_ignore_ascii_case(b"BZMPOP") {
        match mpop_direction(args, b"MIN", b"MAX")? {
            b"MIN" => b"ZPOPMIN",
            _ => b"ZPOPMAX",
        }
    } else {
        return None;
    };
    Some(Frame::Array(framevec![
        bulk(sibling),
        Frame::BulkString(key),
        bulk_usize(popped),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(b: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(b))
    }

    /// Render a record as `CMD arg arg` for readable assertions.
    fn flat(f: &Frame) -> String {
        let Frame::Array(items) = f else {
            return "<not-an-array>".into();
        };
        items
            .iter()
            .map(|i| match i {
                Frame::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
                other => format!("{other:?}"),
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    #[test]
    fn the_record_names_the_key_that_served_not_the_first_key_asked_for() {
        // `BLPOP k1 k2 0` served k2. Recording k1 would pop from the wrong
        // list on every replica — the durability twin of moon#584's
        // over-invalidation.
        let args = [bs(b"k1"), bs(b"k2"), bs(b"0")];
        let reply = Frame::Array(framevec![bs(b"k2"), bs(b"v")]);
        let rec = blocking_effect_record(b"BLPOP", &args, &reply).expect("a pop is a write");
        assert_eq!(flat(&rec), "LPOP k2");
    }

    #[test]
    fn each_command_maps_to_the_sibling_that_reproduces_it() {
        let one = |k: &[u8]| Frame::Array(framevec![bs(k), bs(b"v")]);
        let zone = |k: &[u8]| Frame::Array(framevec![bs(k), bs(b"m"), bs(b"1")]);
        let t = [bs(b"q"), bs(b"0")];

        assert_eq!(
            flat(&blocking_effect_record(b"BLPOP", &t, &one(b"q")).unwrap()),
            "LPOP q"
        );
        assert_eq!(
            flat(&blocking_effect_record(b"BRPOP", &t, &one(b"q")).unwrap()),
            "RPOP q"
        );
        assert_eq!(
            flat(&blocking_effect_record(b"BZPOPMIN", &t, &zone(b"z")).unwrap()),
            "ZPOPMIN z"
        );
        assert_eq!(
            flat(&blocking_effect_record(b"BZPOPMAX", &t, &zone(b"z")).unwrap()),
            "ZPOPMAX z"
        );
    }

    #[test]
    fn a_move_takes_both_keys_and_both_directions_from_the_arguments() {
        // The reply is the element, so the arguments are the only source for
        // the two keys — and for BLMOVE, for the two directions, which are
        // positional and must not be dropped or swapped.
        let args = [bs(b"src"), bs(b"dst"), bs(b"LEFT"), bs(b"RIGHT"), bs(b"0")];
        let rec = blocking_effect_record(b"BLMOVE", &args, &bs(b"elem")).unwrap();
        assert_eq!(flat(&rec), "LMOVE src dst LEFT RIGHT");

        let args = [bs(b"src"), bs(b"dst"), bs(b"0")];
        let rec = blocking_effect_record(b"BRPOPLPUSH", &args, &bs(b"elem")).unwrap();
        assert_eq!(flat(&rec), "RPOPLPUSH src dst");
    }

    #[test]
    fn the_mpop_count_is_what_was_popped_not_what_was_asked_for() {
        // `COUNT 10` against a 3-element list pops 3. Replaying `COUNT 10`
        // against a replica that has since received more elements would pop
        // ten — the record must say three.
        let args = [
            bs(b"0"),
            bs(b"1"),
            bs(b"L"),
            bs(b"LEFT"),
            bs(b"COUNT"),
            bs(b"10"),
        ];
        let reply = Frame::Array(framevec![
            bs(b"L"),
            Frame::Array(framevec![bs(b"a"), bs(b"b"), bs(b"c")]),
        ]);
        let rec = blocking_effect_record(b"BLMPOP", &args, &reply).unwrap();
        assert_eq!(flat(&rec), "LPOP L 3");
    }

    #[test]
    fn the_mpop_direction_is_read_from_the_argv_not_assumed() {
        let mk = |dir: &[u8]| [bs(b"0"), bs(b"1"), bs(b"L"), bs(dir)];
        let reply = Frame::Array(framevec![bs(b"L"), Frame::Array(framevec![bs(b"a")])]);

        assert_eq!(
            flat(&blocking_effect_record(b"BLMPOP", &mk(b"LEFT"), &reply).unwrap()),
            "LPOP L 1"
        );
        assert_eq!(
            flat(&blocking_effect_record(b"BLMPOP", &mk(b"RIGHT"), &reply).unwrap()),
            "RPOP L 1"
        );

        // BZMPOP's inner elements are [member, score] pairs; the count is the
        // number of PAIRS, not of scalars.
        let zreply = Frame::Array(framevec![
            bs(b"Z"),
            Frame::Array(framevec![
                Frame::Array(framevec![bs(b"m1"), bs(b"1")]),
                Frame::Array(framevec![bs(b"m2"), bs(b"2")]),
            ]),
        ]);
        assert_eq!(
            flat(&blocking_effect_record(b"BZMPOP", &mk(b"MIN"), &zreply).unwrap()),
            "ZPOPMIN Z 2"
        );
        assert_eq!(
            flat(&blocking_effect_record(b"BZMPOP", &mk(b"MAX"), &zreply).unwrap()),
            "ZPOPMAX Z 2"
        );
    }

    #[test]
    fn nothing_that_did_not_write_produces_a_record() {
        let args = [bs(b"q"), bs(b"0")];
        // Both null spellings: RESP2 `*-1` and the RESP3 form.
        assert!(blocking_effect_record(b"BLPOP", &args, &Frame::NullArray).is_none());
        assert!(blocking_effect_record(b"BLPOP", &args, &Frame::Null).is_none());
        // An error mutated nothing — including the WRONGTYPE the type gate
        // answers before any pop is attempted.
        assert!(
            blocking_effect_record(
                b"BLPOP",
                &args,
                &Frame::Error(Bytes::from_static(b"WRONGTYPE nope"))
            )
            .is_none()
        );
        // A command that is not a blocking pop.
        let reply = Frame::Array(framevec![bs(b"q"), bs(b"v")]);
        assert!(blocking_effect_record(b"GET", &args, &reply).is_none());
        // An MPOP that reports an empty element list wrote nothing.
        let empty = Frame::Array(framevec![bs(b"L"), Frame::Array(framevec![])]);
        let margs = [bs(b"0"), bs(b"1"), bs(b"L"), bs(b"LEFT")];
        assert!(blocking_effect_record(b"BLMPOP", &margs, &empty).is_none());
    }

    #[test]
    fn a_shape_this_function_does_not_understand_is_omitted_not_guessed() {
        // Inventing a record from an unrecognised shape would corrupt a
        // replica; omitting one only loses what is already lost today.
        let args = [bs(b"q"), bs(b"0")];
        assert!(blocking_effect_record(b"BLPOP", &args, &Frame::Integer(1)).is_none());
        // First element not a key.
        let odd = Frame::Array(framevec![Frame::Integer(7), bs(b"v")]);
        assert!(blocking_effect_record(b"BLPOP", &args, &odd).is_none());
        // BLMOVE with a truncated argv cannot name its directions.
        assert!(blocking_effect_record(b"BLMOVE", &[bs(b"s"), bs(b"d")], &bs(b"e")).is_none());
        // BLMPOP with no direction token at all.
        let reply = Frame::Array(framevec![bs(b"L"), Frame::Array(framevec![bs(b"a")])]);
        assert!(
            blocking_effect_record(b"BLMPOP", &[bs(b"0"), bs(b"1"), bs(b"L")], &reply).is_none()
        );
    }
}
