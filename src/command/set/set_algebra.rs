//! Set algebra over BORROWED [`SetRef`]s (moon#1169).
//!
//! SINTER / SINTERCARD / SDIFF / SUNION and their `*STORE` forms used to copy
//! EVERY input set — the 1M-member one included — into a fresh
//! `std::collections::HashSet<Bytes>` (SipHash, a refcount bump or an
//! allocation per member, intset members rendered with `to_string`) before
//! doing any work, and then cloned the smallest one again: 205.7 ms for
//! `SINTER small(10) big(1M)` against 83 µs on redis 7.0.15. The `*STORE`
//! forms read their SOURCES through `get_set`, whose `get_promoted` core
//! permanently rewrote an intset/listpack source as a hashtable (moon#832's
//! defect, on the write path).
//!
//! Here, like redis's `sinterGenericCommand` / `sunionDiffGenericCommand`:
//!
//! * every key is resolved to a [`SetRef`] through the shared-borrow
//!   accessor — WRONGTYPE on ANY key wins, a missing key is an empty set,
//!   and no source is ever re-encoded;
//! * SINTER walks the SMALLEST set once and probes the others with
//!   [`SetRef::contains`] (O(1) hashtable, O(log n) intset, a bounded
//!   listpack scan), so it is O(|smallest| x K) and SINTERCARD's LIMIT stops
//!   the walk;
//! * SDIFF walks the FIRST set and probes the rest (redis's algorithm 1);
//! * members are visited borrowed — an intset member is rendered into a
//!   stack buffer, a listpack entry is a slice — and only a member that is
//!   actually returned or stored becomes an owned `Bytes`.

use std::collections::HashSet;

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::db::SetRef;
use crate::storage::listpack::ListpackRef;

/// One set member, borrowed from whichever encoding holds it.
#[derive(Clone, Copy)]
pub(super) enum Member<'a> {
    /// A hashtable member: cloning it for a reply is a refcount bump.
    Stored(&'a Bytes),
    /// A listpack string entry.
    Packed(&'a [u8]),
    /// An intset member, or a listpack integer entry.
    Int(i64),
}

impl Member<'_> {
    /// Run `f` over the member's bytes. An integer is rendered with `itoa`
    /// into a stack buffer — the canonical spelling every encoder uses, so
    /// `7` compares equal to a stored `b"7"` and to nothing else.
    #[inline]
    pub(super) fn with_bytes<R>(self, f: impl FnOnce(&[u8]) -> R) -> R {
        match self {
            Member::Stored(b) => f(b),
            Member::Packed(s) => f(s),
            Member::Int(v) => {
                let mut buf = itoa::Buffer::new();
                f(buf.format(v).as_bytes())
            }
        }
    }

    /// The owned reply / storage form.
    #[inline]
    pub(super) fn to_bytes(self) -> Bytes {
        match self {
            Member::Stored(b) => b.clone(),
            Member::Packed(s) => Bytes::copy_from_slice(s),
            Member::Int(v) => {
                let mut buf = itoa::Buffer::new();
                Bytes::copy_from_slice(buf.format(v).as_bytes())
            }
        }
    }
}

/// Visit every member of `set`, borrowed, until `visit` returns `false`.
pub(super) fn for_each_member<'a>(set: &'a SetRef<'_>, mut visit: impl FnMut(Member<'a>) -> bool) {
    match set {
        SetRef::Hash(s) => {
            for m in s.iter() {
                if !visit(Member::Stored(m)) {
                    return;
                }
            }
        }
        SetRef::Owned(s) => {
            for m in s.iter() {
                if !visit(Member::Stored(m)) {
                    return;
                }
            }
        }
        SetRef::Listpack(lp) => {
            for e in lp.iter_refs() {
                let m = match e {
                    ListpackRef::Str(s) => Member::Packed(s),
                    ListpackRef::Integer(v) => Member::Int(v),
                };
                if !visit(m) {
                    return;
                }
            }
        }
        SetRef::Intset(is) => {
            for v in is.iter() {
                if !visit(Member::Int(v)) {
                    return;
                }
            }
        }
    }
}

/// Membership probe. An integer against an intset compares integers
/// directly — no render, no parse.
#[inline]
pub(super) fn contains(set: &SetRef<'_>, m: Member<'_>) -> bool {
    match (set, m) {
        (SetRef::Intset(is), Member::Int(v)) => is.contains(v),
        _ => m.with_bytes(|b| set.contains(b)),
    }
}

/// Resolve every key, WRONGTYPE-checking ALL of them before any work — the
/// order redis's set-algebra commands check in. `None` = missing (an empty
/// set). Never re-encodes: a shared borrow cannot reach `SetKind::upgrade`.
pub(super) fn lookup_all<'a, 'k>(
    db: &'a Database,
    keys: impl IntoIterator<Item = &'k [u8]>,
    now_ms: u64,
) -> Result<Vec<Option<SetRef<'a>>>, Frame> {
    let keys = keys.into_iter();
    let mut sets = Vec::with_capacity(keys.size_hint().0);
    for key in keys {
        sets.push(db.get_set_ref_if_alive(key, now_ms)?);
    }
    Ok(sets)
}

/// Emit the intersection of `sets` (all present — a missing key makes the
/// intersection empty, which the callers answer before getting here) by
/// walking the smallest and probing the others, until `emit` returns
/// `false`. O(|smallest| x K).
pub(super) fn intersect<'a>(sets: &'a [SetRef<'a>], mut emit: impl FnMut(Member<'a>) -> bool) {
    let Some(smallest) = (0..sets.len()).min_by_key(|&i| sets[i].len()) else {
        return;
    };
    for_each_member(&sets[smallest], |m| {
        let in_all = sets
            .iter()
            .enumerate()
            .all(|(j, s)| j == smallest || contains(s, m));
        !in_all || emit(m)
    });
}

/// Emit the members of `first` found in none of `others` (missing keys are
/// empty sets and exclude nothing), until `emit` returns `false`.
pub(super) fn difference<'a>(
    first: &'a SetRef<'a>,
    others: &'a [Option<SetRef<'a>>],
    mut emit: impl FnMut(Member<'a>) -> bool,
) {
    for_each_member(first, |m| {
        let excluded = others.iter().flatten().any(|s| contains(s, m));
        excluded || emit(m)
    });
}

/// The union of the present sets, as owned members. A member already
/// collected is recognised by a borrowed lookup, so a repeat never
/// allocates.
pub(super) fn union(sets: &[Option<SetRef<'_>>]) -> HashSet<Bytes> {
    let mut out: HashSet<Bytes> = HashSet::new();
    for set in sets.iter().flatten() {
        for_each_member(set, |m| {
            if !m.with_bytes(|b| out.contains(b)) {
                out.insert(m.to_bytes());
            }
            true
        });
    }
    out
}
