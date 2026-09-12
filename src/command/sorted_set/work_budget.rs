//! Work budget for the sorted-set write path (moon#942).
//!
//! Test-only counters, in the shape `storage::dashtable`'s key-lookup counter
//! established: a `thread_local! { Cell<u32> }` under `cfg(test)`, and an
//! `#[inline(always)]` empty function outside it, so a release build carries
//! nothing at all.
//!
//! # Why counters and not a timer
//!
//! The same reason `storage::db::probe_budget` gives. moon#789's PERF-08
//! measured **+11% on aarch64 and −17% on x86_64** for one probe-count
//! reduction — the two architectures disagreed in *sign*. A count here is
//! therefore **not** a throughput claim and this module makes none. It pins a
//! structural fact: how much work one `ZADD`/`ZINCRBY`/`ZREM` does per member.
//!
//! A number going UP is the regression these counters exist to catch.
//!
//! Three quantities, each one a thing Redis does exactly once per member and
//! moon used to do more than once:
//!
//! * [`take_arg_score_parses`] — `str::parse::<f64>` over a *score argument*.
//!   Redis calls `getDoubleFromObject` once per pair. moon's `ZADD` parsed
//!   every pair twice: once in the moon#814 validation pre-pass and again in
//!   the mutation loop.
//! * [`take_stored_score_parses`] — `str::parse::<f64>` over a score *already
//!   in a listpack*. Redis reads a `double` out of the listpack; moon stores
//!   the canonical text (a listpack has no float entry type), so decoding one
//!   is a real parse and must happen only when a flag or `CH` actually
//!   consults it.
//! * [`take_member_lookups`] — hash lookups into the `SortedSetBPTree`
//!   `members` map. Redis's `zsetAdd` does ONE `dictFind` and writes the new
//!   score through the entry it found.
//! * [`take_listpack_score_writes`] — score bytes actually written into a
//!   listpack. Redis's `zsetAdd` re-inserts only `if (score != curscore)`;
//!   moon rewrote the entry unconditionally, so `ZADD z 1 m` repeated on the
//!   same member spliced the same bytes back over themselves every time.
//! * [`take_bptree_score_writes`] — the same quantity for the full
//!   `SortedSetBPTree` form: a B+tree delete plus a B+tree insert, run even
//!   when the member already carried that exact score.

// Plain `//` comments on the macro invocation: a doc comment there trips
// `unused_doc_comments`, which CI denies.
#[cfg(test)]
thread_local! {
    static ARG_SCORE_PARSES: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    static STORED_SCORE_PARSES: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    static MEMBER_LOOKUPS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    static LISTPACK_SCORE_WRITES: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    static BPTREE_SCORE_WRITES: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// Record one `f64` parse of a score ARGUMENT. No-op outside test builds.
#[cfg(test)]
#[inline]
pub(super) fn note_arg_score_parse() {
    ARG_SCORE_PARSES.with(|c| c.set(c.get() + 1));
}

/// No-op in non-test builds — zero production cost.
#[cfg(not(test))]
#[inline(always)]
pub(super) fn note_arg_score_parse() {}

/// Record one `f64` parse of a score already STORED in a listpack.
#[cfg(test)]
#[inline]
pub(super) fn note_stored_score_parse() {
    STORED_SCORE_PARSES.with(|c| c.set(c.get() + 1));
}

/// No-op in non-test builds — zero production cost.
#[cfg(not(test))]
#[inline(always)]
pub(super) fn note_stored_score_parse() {}

/// Record one hash lookup into a `SortedSetBPTree`'s `members` map.
#[cfg(test)]
#[inline]
pub(super) fn note_member_lookup() {
    MEMBER_LOOKUPS.with(|c| c.set(c.get() + 1));
}

/// No-op in non-test builds — zero production cost.
#[cfg(not(test))]
#[inline(always)]
pub(super) fn note_member_lookup() {}

/// Record one score written into a listpack entry.
#[cfg(test)]
#[inline]
pub(super) fn note_listpack_score_write() {
    LISTPACK_SCORE_WRITES.with(|c| c.set(c.get() + 1));
}

/// No-op in non-test builds — zero production cost.
#[cfg(not(test))]
#[inline(always)]
pub(super) fn note_listpack_score_write() {}

/// Record one `(score, member)` re-insertion into a `SortedSetBPTree`.
#[cfg(test)]
#[inline]
pub(super) fn note_bptree_score_write() {
    BPTREE_SCORE_WRITES.with(|c| c.set(c.get() + 1));
}

/// No-op in non-test builds — zero production cost.
#[cfg(not(test))]
#[inline(always)]
pub(super) fn note_bptree_score_write() {}

/// Read and reset the per-thread score-ARGUMENT parse counter.
#[cfg(test)]
pub(crate) fn take_arg_score_parses() -> u32 {
    ARG_SCORE_PARSES.with(|c| c.replace(0))
}

/// Read and reset the per-thread STORED-score parse counter.
#[cfg(test)]
pub(crate) fn take_stored_score_parses() -> u32 {
    STORED_SCORE_PARSES.with(|c| c.replace(0))
}

/// Read and reset the per-thread `members` lookup counter.
#[cfg(test)]
pub(crate) fn take_member_lookups() -> u32 {
    MEMBER_LOOKUPS.with(|c| c.replace(0))
}

/// Read and reset the per-thread listpack score-write counter.
#[cfg(test)]
pub(crate) fn take_listpack_score_writes() -> u32 {
    LISTPACK_SCORE_WRITES.with(|c| c.replace(0))
}

/// Read and reset the per-thread B+tree score-write counter.
#[cfg(test)]
pub(crate) fn take_bptree_score_writes() -> u32 {
    BPTREE_SCORE_WRITES.with(|c| c.replace(0))
}

// ───────────────────────────────────────────────────────────────────────────
// The budget itself (moon#942)
// ───────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod budget {
    use super::*;
    use crate::command::sorted_set::{zadd, zincrby, zrem};
    use crate::protocol::Frame;
    use crate::storage::Database;
    use crate::storage::dashtable::take_key_lookups;
    use crate::storage::zset_score::take_float_formats;
    use bytes::Bytes;

    fn bulk(s: &str) -> Frame {
        Frame::BulkString(Bytes::from(s.to_owned()))
    }

    /// Everything one command spent, per member.
    #[derive(Debug, PartialEq, Eq)]
    struct Budget {
        /// `str::parse::<f64>` over a score ARGUMENT.
        arg_score_parses: u32,
        /// `str::parse::<f64>` over a score already in a listpack.
        stored_score_parses: u32,
        /// Hash lookups into a `SortedSetBPTree`'s `members` map.
        member_lookups: u32,
        /// Score entries spliced into a listpack.
        listpack_score_writes: u32,
        /// `(score, member)` re-insertions into a B+tree.
        bptree_score_writes: u32,
        /// Scores rendered through `core::fmt`'s `f64` Display.
        float_formats: u32,
    }

    /// Zero every counter, run `f`, return its result and what it spent.
    fn measure<T>(f: impl FnOnce() -> T) -> (T, Budget) {
        let _ = take_arg_score_parses();
        let _ = take_stored_score_parses();
        let _ = take_member_lookups();
        let _ = take_listpack_score_writes();
        let _ = take_bptree_score_writes();
        let _ = take_float_formats();
        let out = f();
        (
            out,
            Budget {
                arg_score_parses: take_arg_score_parses(),
                stored_score_parses: take_stored_score_parses(),
                member_lookups: take_member_lookups(),
                listpack_score_writes: take_listpack_score_writes(),
                bptree_score_writes: take_bptree_score_writes(),
                float_formats: take_float_formats(),
            },
        )
    }

    /// A zset past `zset-max-listpack-entries`, so it holds the full
    /// `SortedSetBPTree` form and every member lands in the `members` map.
    fn bptree_zset() -> Database {
        let mut db = Database::new();
        let mut args = vec![bulk("z")];
        for i in 0..200u32 {
            args.push(bulk(&i.to_string()));
            args.push(bulk(&format!("m{i}")));
        }
        assert_eq!(
            zadd(&mut db, &args),
            Frame::Integer(200),
            "fixture: the 200-member batch must have been accepted"
        );
        assert_eq!(
            crate::command::key::object(&mut db, &[bulk("ENCODING"), bulk("z")]),
            Frame::BulkString(Bytes::from_static(b"skiplist")),
            "fixture: 200 members must exceed zset-max-listpack-entries"
        );
        db
    }

    // ── score ARGUMENTS are parsed once, as Redis parses them ───────────────

    #[test]
    fn zadd_parses_each_score_argument_exactly_once() {
        // moon#814 requires every pair to be proven parseable BEFORE the
        // keyspace is touched, and that pre-pass is not negotiable: the
        // mutation loop returns from inside the `before … adjust_memory`
        // window, so a late rejection strands the charge for every member
        // already written. What IS negotiable is throwing the result away.
        // The loop re-ran `parse_zadd_pair` on bytes the pre-pass had just
        // decoded — `str::parse::<f64>` twice over the same argument, once
        // per member. Redis calls `getDoubleFromObject` once per pair and
        // keeps the `double` in its `scores` array.
        let mut db = Database::new();

        let one = [bulk("z"), bulk("1"), bulk("m")];
        let (r, b) = measure(|| zadd(&mut db, &one));
        assert_eq!(r, Frame::Integer(1));
        assert_eq!(
            b.arg_score_parses, 1,
            "ZADD parsed the score argument {} times for ONE pair — moon#942. \
             The moon#814 pre-pass must keep validating first; it must also \
             keep what it decoded.",
            b.arg_score_parses
        );

        let four = [
            bulk("z4"),
            bulk("1"),
            bulk("a"),
            bulk("2"),
            bulk("b"),
            bulk("3"),
            bulk("c"),
            bulk("4"),
            bulk("d"),
        ];
        let (r, b) = measure(|| zadd(&mut db, &four));
        assert_eq!(r, Frame::Integer(4));
        assert_eq!(
            b.arg_score_parses, 4,
            "ZADD parsed {} score arguments for FOUR pairs — moon#942.",
            b.arg_score_parses
        );
    }

    #[test]
    fn a_rejected_batch_still_validates_every_pair_before_the_keyspace() {
        // The other half of the same contract, and the one a cache is most
        // likely to break: the pre-pass must still reach the BAD pair, and
        // the key must not exist afterwards (moon#814 / Redis parity — ZADD
        // is all-or-nothing and does not create the key when it errors).
        let mut db = Database::new();
        let args = [
            bulk("bad"),
            bulk("1"),
            bulk("a"),
            bulk("not-a-float"),
            bulk("b"),
        ];
        let r = zadd(&mut db, &args);
        assert!(
            matches!(&r, Frame::Error(e) if e.starts_with(b"ERR value is not a valid float")),
            "expected the float error, got {r:?}"
        );
        assert_eq!(
            crate::command::key::exists(&mut db, &[bulk("bad")]),
            Frame::Integer(0),
            "an erroring ZADD must not create the key"
        );
    }

    // ── the STORED score is decoded only when something consults it ─────────

    #[test]
    fn zadd_decodes_the_stored_score_only_when_a_flag_or_ch_consults_it() {
        // A listpack has no float entry type, so moon keeps a score as its
        // canonical decimal text: reading one back is a real
        // `str::parse::<f64>`. `ZADD z <score> <member>` with no flag and no
        // CH consults it for NOTHING — `should_update` is unconditionally
        // true and the `changed` tally it feeds is never returned.
        let mut db = Database::new();
        assert_eq!(
            zadd(&mut db, &[bulk("z"), bulk("1"), bulk("m")]),
            Frame::Integer(1)
        );

        let plain = [bulk("z"), bulk("2"), bulk("m")];
        let (r, b) = measure(|| zadd(&mut db, &plain));
        assert_eq!(r, Frame::Integer(0), "an existing member is not an add");
        assert_eq!(
            b.stored_score_parses, 0,
            "a plain ZADD decoded the stored score {} time(s) and used it for \
             nothing — moon#942",
            b.stored_score_parses
        );

        // GT genuinely needs it: the control that proves the counter is live.
        let gt = [bulk("z"), bulk("GT"), bulk("3"), bulk("m")];
        let (_, b) = measure(|| zadd(&mut db, &gt));
        assert_eq!(
            b.stored_score_parses, 1,
            "GT must still compare against the stored score"
        );

        // So does CH, which reports whether the score MOVED.
        let ch = [bulk("z"), bulk("CH"), bulk("4"), bulk("m")];
        let (r, b) = measure(|| zadd(&mut db, &ch));
        assert_eq!(r, Frame::Integer(1), "CH counts the update");
        assert_eq!(
            b.stored_score_parses, 1,
            "CH must still compare against the stored score"
        );

        // NX declines without reading it: the decision does not depend on
        // the old value at all.
        let nx = [bulk("z"), bulk("NX"), bulk("5"), bulk("m")];
        let (r, b) = measure(|| zadd(&mut db, &nx));
        assert_eq!(r, Frame::Integer(0), "NX must not touch an existing member");
        assert_eq!(
            b.stored_score_parses, 0,
            "NX refuses on presence alone and must not decode the score"
        );
        assert_eq!(
            b.listpack_score_writes, 0,
            "NX must not write anything either"
        );
    }

    // ── a score that is already there is not written again ──────────────────

    #[test]
    fn zadd_rewriting_an_identical_score_writes_no_listpack_bytes() {
        // Redis's `zsetAdd` re-inserts only `if (score != curscore)`, on both
        // encodings. moon spliced the rendered score back over itself every
        // time, so the idempotent re-post a leaderboard client makes — the
        // benchmark's own `zadd z:<n> 1 m:<n>` shape, where the score is the
        // literal `1` on every call — paid an `encode_entry` and a
        // `write_entry` to change nothing.
        let mut db = Database::new();
        assert_eq!(
            zadd(&mut db, &[bulk("z"), bulk("1"), bulk("m")]),
            Frame::Integer(1)
        );

        let same = [bulk("z"), bulk("1"), bulk("m")];
        let (r, b) = measure(|| zadd(&mut db, &same));
        assert_eq!(r, Frame::Integer(0));
        assert_eq!(
            b.listpack_score_writes, 0,
            "re-adding a member at its CURRENT score wrote {} score entr(ies) \
             — moon#942",
            b.listpack_score_writes
        );

        // A genuinely different score still writes: the control.
        let moved = [bulk("z"), bulk("2"), bulk("m")];
        let (_, b) = measure(|| zadd(&mut db, &moved));
        assert_eq!(
            b.listpack_score_writes, 1,
            "a score that MOVED must still be written"
        );
        assert_eq!(
            crate::command::sorted_set::zscore(&mut db, &[bulk("z"), bulk("m")]),
            Frame::BulkString(Bytes::from_static(b"2")),
            "and the write must have landed"
        );
    }

    #[test]
    fn zadd_rewriting_an_identical_score_touches_no_bptree() {
        // The same rule on the full form, where the cost is far higher: a
        // B+tree delete and a B+tree insert, plus the `members` remove and
        // re-insert, to put the member back exactly where it was.
        let mut db = bptree_zset();

        let same = [bulk("z"), bulk("7"), bulk("m7")];
        let (r, b) = measure(|| zadd(&mut db, &same));
        assert_eq!(r, Frame::Integer(0));
        assert_eq!(
            b.bptree_score_writes, 0,
            "re-adding a member at its CURRENT score ran {} B+tree \
             re-insertion(s) — moon#942",
            b.bptree_score_writes
        );

        let moved = [bulk("z"), bulk("999"), bulk("m7")];
        let (_, b) = measure(|| zadd(&mut db, &moved));
        assert_eq!(
            b.bptree_score_writes, 1,
            "a score that MOVED must still be re-inserted"
        );
        assert_eq!(
            crate::command::sorted_set::zscore(&mut db, &[bulk("z"), bulk("m7")]),
            Frame::BulkString(Bytes::from_static(b"999")),
            "and the write must have landed"
        );
    }

    #[test]
    fn the_listpack_identical_score_skip_is_decided_on_bytes() {
        // The skip asks `current.eq_bytes(&rendered)`, not `old == score`, and
        // that choice is load-bearing at exactly one point: `-0.0 == 0.0` is
        // true while `-0` and `0` are different bytes. Redis's `zsetAdd`
        // compares the doubles and therefore LEAVES a `0` in place when a
        // client writes `-0`; moon has always stored whichever spelling the
        // client sent, and this change must not quietly switch sides — a
        // silent reply change is worse than the divergence it would fix.
        let mut db = Database::new();
        assert_eq!(
            zadd(&mut db, &[bulk("z"), bulk("0"), bulk("m")]),
            Frame::Integer(1)
        );

        // `-0` over a stored `0`: different bytes, so it is written.
        let flip = [bulk("z"), bulk("-0"), bulk("m")];
        let (r, b) = measure(|| zadd(&mut db, &flip));
        assert_eq!(r, Frame::Integer(0));
        assert_eq!(
            b.listpack_score_writes, 1,
            "-0 over a stored 0 must still be written"
        );
        assert_eq!(
            crate::command::sorted_set::zscore(&mut db, &[bulk("z"), bulk("m")]),
            Frame::BulkString(Bytes::from_static(b"-0")),
            "the sign must have survived into the listpack"
        );

        // And `-0` again over the stored `-0`: identical bytes, skipped.
        let again = [bulk("z"), bulk("-0"), bulk("m")];
        let (_, b) = measure(|| zadd(&mut db, &again));
        assert_eq!(
            b.listpack_score_writes, 0,
            "identical bytes are not rewritten"
        );
    }

    // ── one hash lookup per member, as Redis's one `dictFind` ───────────────

    #[test]
    fn zadd_on_the_full_form_costs_one_member_lookup() {
        // Redis's `zsetAdd` does ONE `dictFind` and writes the new score
        // through the `dictEntry` it found. moon did three: `members.get` for
        // the flag decision, `members.remove` inside `zadd_member`, and
        // `members.insert` after it — hashing the same member three times and
        // cloning the `Bytes` twice, on a path `src/command/` forbids cloning
        // on at all.
        let mut db = bptree_zset();

        let update = [bulk("z"), bulk("999"), bulk("m7")];
        let (r, b) = measure(|| zadd(&mut db, &update));
        assert_eq!(r, Frame::Integer(0));
        assert_eq!(
            b.member_lookups, 1,
            "ZADD onto an EXISTING member hashed it {} times — moon#942",
            b.member_lookups
        );

        // A brand-new member is one lookup to miss plus the insert, which is
        // the shape `HashMap` gives without a raw entry API.
        let fresh = [bulk("z"), bulk("5"), bulk("brand-new")];
        let (r, b) = measure(|| zadd(&mut db, &fresh));
        assert_eq!(r, Frame::Integer(1));
        assert_eq!(
            b.member_lookups, 2,
            "ZADD of a NEW member hashed it {} times — moon#942",
            b.member_lookups
        );

        // ZREM was already one, and must stay one.
        let gone = [bulk("z"), bulk("brand-new")];
        let (r, b) = measure(|| zrem(&mut db, &gone));
        assert_eq!(r, Frame::Integer(1));
        assert_eq!(b.member_lookups, 1, "ZREM must stay at one lookup");
    }

    #[test]
    fn zincrby_on_the_full_form_costs_one_member_lookup() {
        let mut db = bptree_zset();

        let bump = [bulk("z"), bulk("3"), bulk("m7")];
        let (r, b) = measure(|| zincrby(&mut db, &bump));
        assert_eq!(r, Frame::BulkString(Bytes::from_static(b"10")));
        assert_eq!(
            b.member_lookups, 1,
            "ZINCRBY onto an EXISTING member hashed it {} times — moon#942",
            b.member_lookups
        );

        let fresh = [bulk("z"), bulk("3"), bulk("nowhere")];
        let (r, b) = measure(|| zincrby(&mut db, &fresh));
        assert_eq!(r, Frame::BulkString(Bytes::from_static(b"3")));
        assert_eq!(
            b.member_lookups, 2,
            "ZINCRBY of a NEW member hashed it {} times — moon#942",
            b.member_lookups
        );
    }

    // ── integral scores do not reach `core::fmt`'s float formatter ──────────

    #[test]
    fn an_integral_score_is_rendered_without_the_float_formatter() {
        // Redis's `d2string` tries `double2ll` first and only falls back to
        // `fpconv_dtoa`. moon sent every score — including the `1` the
        // benchmark writes on every call, and every leaderboard score ever —
        // through `core::fmt`'s shortest-round-trip `f64` Display, which is
        // the Grisu/Dragon path plus the whole `Formatter` machinery.
        let mut db = Database::new();

        let integral = [bulk("z"), bulk("1"), bulk("m")];
        let (r, b) = measure(|| zadd(&mut db, &integral));
        assert_eq!(r, Frame::Integer(1));
        assert_eq!(
            b.float_formats, 0,
            "an integral score reached `core::fmt`'s f64 Display {} time(s) \
             — moon#942",
            b.float_formats
        );

        // A score that genuinely is not an integer still needs it: the
        // control that proves the counter is live.
        let fractional = [bulk("z"), bulk("1.5"), bulk("frac")];
        let (r, b) = measure(|| zadd(&mut db, &fractional));
        assert_eq!(r, Frame::Integer(1));
        assert_eq!(
            b.float_formats, 1,
            "a fractional score must still take the slow renderer"
        );

        // ZINCRBY renders too, and lands on the same fork.
        let (r, b) = measure(|| zincrby(&mut db, &[bulk("z"), bulk("2"), bulk("m")]));
        assert_eq!(r, Frame::BulkString(Bytes::from_static(b"3")));
        assert_eq!(
            b.float_formats, 0,
            "ZINCRBY's integral result reached the float formatter {} time(s)",
            b.float_formats
        );
    }

    // ── the accessor budget: how many times ZADD hashes its KEY ─────────────

    #[test]
    fn zadd_end_to_end_probe_budget() {
        // The benchmark's own shape (`REPORT-family-audit.md:75`):
        // `zadd z:__rand_int__ 1 m:__rand_int__`. The 2026-09-11 population
        // tally on that run recorded `listpack=200` of 200 sampled keys, so
        // the LISTPACK arms are the ones the measured gap is made of; the
        // `skiplist` arm is pinned here because it is where the remaining
        // duplicate accessor lives.
        let key = || bulk("z:000000000042");

        // (a) Absent key -> a SortedSetListpack is created.
        let mut db = Database::new();
        let args = [key(), bulk("1"), bulk("m:000000000007")];
        let _ = take_key_lookups();
        let r = zadd(&mut db, &args);
        let create = take_key_lookups();
        assert_eq!(r, Frame::Integer(1));

        // (b) Steady state on the listpack encoding.
        let args2 = [key(), bulk("1"), bulk("m:000000000008")];
        let _ = take_key_lookups();
        let r = zadd(&mut db, &args2);
        let listpack_hit = take_key_lookups();
        assert_eq!(r, Frame::Integer(1));

        // (c) Steady state on the full B+tree form.
        let mut db2 = bptree_zset();
        let args3 = [bulk("z"), bulk("1"), bulk("m:000000000009")];
        let _ = take_key_lookups();
        let r = zadd(&mut db2, &args3);
        let bptree_hit = take_key_lookups();
        assert_eq!(r, Frame::Integer(1));

        assert_eq!(
            (create, listpack_hit, bptree_hit),
            (3, 2, 4),
            "ZADD end-to-end probe budget moved \
             (create={create}, listpack_hit={listpack_hit}, bptree_hit={bptree_hit}) \
             — moon#942. The B+tree arm pays TWO accessors: \
             `get_or_create_zset_listpack` answers `Ok(None)` and \
             `get_or_create_sorted_set` then repeats the whole skeleton \
             (`hot_state`, `settle_not_live`, `get_mut`, `stamp_mutation`, \
             `SortedSetKind::upgrade`) on a key the first call already had in \
             hand. That is the SADD shape a4ae9775 collapsed with `SetHandle`, \
             and collapsing it here needs `accessors.rs` — a file this branch \
             does not own, so the fix is filed as a proposal and this number \
             is pinned at what it costs TODAY. A RISE is a regression; a DROP \
             to 2 is that proposal landing, and this assertion is where it \
             announces itself."
        );
    }
}
