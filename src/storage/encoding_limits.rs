//! One authority for every compact-encoding threshold.
//!
//! Three places decide whether a container lives in its compact form — the
//! ENTRY gate a write command runs before it touches a listpack, the UPGRADE
//! check it runs after the push loop, and the restart-side re-derivation in
//! `value_codec::compact_after_decode`. Until moon#896 each of them held its
//! own copy of the arithmetic, and two of them disagreed about the UNIT: the
//! entry gate compared `args.len() - 1` — listpack ENTRIES, two per field —
//! against a threshold the upgrade check applied to `lp.len() / 2` — logical
//! ITEMS. A bulk `HSET` of 65 fields therefore promoted at half the intended
//! cardinality (argv 128 -> 130 flipped `listpack` to `hashtable`) while the
//! same hash built one field at a time stayed compact.
//!
//! This module exists so that class of bug cannot recur, not merely so the
//! instance is fixed:
//!
//! * The thresholds live in ONE `Copy` struct, [`EncodingLimits`], snapshotted
//!   per shard on `Database`. No consultation site names a raw constant.
//! * The predicate takes a [`Shape`], never a raw count. The entries-per-item
//!   factor (`Hash` and `SortedSet` lay out two listpack entries per item,
//!   `Set` and `List` one) is applied by the authority, so a caller cannot
//!   pass the wrong unit: [`Shape::items_in`] converts an argv or listpack
//!   length into items, and [`EncodingLimits::listpack_fits`] reads the stored
//!   listpack itself.
//! * The POLICY limit and the SAFETY limit are distinct, named things. The
//!   policy is the `*-max-listpack-entries` family a user reasons about. The
//!   safety ceiling, [`LISTPACK_SAFE_BATCH_ENTRIES`], is the bound on what one
//!   command may push through a listpack regardless of policy: the header
//!   counts entries in a `u16` (moon#865), and `SADD`'s membership check is a
//!   linear in-place scan, so an unbounded batch is quadratic inside one
//!   command on one shard thread. Before this module the two shared the
//!   constant `128` and nothing said which was which.
//!
//! The values here are moon's CURRENT ones. Adopting Redis's (hash 512, list
//! as an 8 KB byte budget) is a separate decision that changes shipping
//! behaviour and needs a Linux sweep; the authority carries the fields so
//! that decision is one edit, not a hunt.

use crate::storage::listpack::Listpack;

/// SAFETY ceiling, in listpack ENTRIES, on the largest listpack the authority
/// will ever let a policy threshold describe — and therefore on the largest
/// batch one command may push through the listpack path.
///
/// Why it exists, independently of the policy thresholds: the listpack header
/// counts entries in a `u16`, and `Listpack::update_header` saturates at
/// 65_535 rather than wrapping (moon#865) — so past that point the failure
/// mode is TRUNCATION of an acknowledged write. And `SADD` proves membership
/// by a linear scan of the listpack, so one command pushing `n` members costs
/// `O(n^2)` on the shard thread. A policy value is clamped to this ceiling in
/// [`EncodingLimits::max_items`], so a configured `set-max-listpack-entries`
/// of a million cannot reach either edge: a listpack holds at most
/// `LISTPACK_SAFE_BATCH_ENTRIES` entries before a batch, a batch adds at most
/// that many again, and `2 * 8_192` is a quarter of the header's range.
///
/// Sixty-four times the policy default, so it never decides a normal write.
pub const LISTPACK_SAFE_BATCH_ENTRIES: usize = 8_192;

// moon's current policy thresholds. Private on purpose: nothing outside this
// module can name them, so every consultation goes through the predicate.
// `scripts/audit-encoding-limits.sh` keeps the names out of the rest of `src/`.
const HASH_MAX_LISTPACK_ENTRIES: usize = 128;
const HASH_MAX_LISTPACK_VALUE: usize = 64;
const SET_MAX_LISTPACK_ENTRIES: usize = 128;
const SET_MAX_LISTPACK_VALUE: usize = 64;
const SET_MAX_INTSET_ENTRIES: usize = 512;
const ZSET_MAX_LISTPACK_ENTRIES: usize = 128;
const ZSET_MAX_LISTPACK_VALUE: usize = 64;
const LIST_MAX_LISTPACK_ENTRIES: usize = 128;

/// How a container lays its items out in a listpack.
///
/// The shape is the unit converter. A hash field is TWO listpack entries
/// (`field`, `value`); a zset member is two (`member`, `score`); a set member
/// and a list element are one each. Every count the authority takes is in
/// logical ITEMS; a caller holding an entry count — an argv slice, or
/// `Listpack::len()` — converts through [`Shape::items_in`] rather than
/// dividing by a number it remembers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Shape {
    /// `[field, value, field, value, …]`.
    Hash,
    /// `[member, member, …]`.
    Set,
    /// `[member, score, member, score, …]`.
    SortedSet,
    /// `[element, element, …]`.
    List,
}

impl Shape {
    /// Listpack entries one logical item occupies.
    #[inline]
    #[must_use]
    pub const fn entries_per_item(self) -> usize {
        match self {
            Shape::Hash | Shape::SortedSet => 2,
            Shape::Set | Shape::List => 1,
        }
    }

    /// Logical items in `entries` listpack entries — or in an argv slice laid
    /// out the same way (`HSET`'s `field value …`, `ZADD`'s `score member …`).
    #[inline]
    #[must_use]
    pub const fn items_in(self, entries: usize) -> usize {
        entries / self.entries_per_item()
    }
}

/// The compact-encoding thresholds one shard applies. `Copy`, so a command
/// takes a snapshot by value and never holds a borrow of the database across
/// its mutation.
///
/// Fields are public so a later configuration wave and the tests can build
/// one; consultation sites must use the predicates, never the fields
/// ([`EncodingLimits::fits`], [`EncodingLimits::listpack_fits`],
/// [`EncodingLimits::intset_fits`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EncodingLimits {
    /// `hash-max-listpack-entries`, in fields.
    pub hash_entries: usize,
    /// `hash-max-listpack-value`, in bytes; applies to fields AND values.
    pub hash_value: usize,
    /// `set-max-listpack-entries`, in members.
    pub set_entries: usize,
    /// `set-max-listpack-value`, in bytes.
    pub set_value: usize,
    /// `set-max-intset-entries`, in members.
    pub set_intset: usize,
    /// `zset-max-listpack-entries`, in members.
    pub zset_entries: usize,
    /// `zset-max-listpack-value`, in bytes; applies to the MEMBER only, as in
    /// Redis — a score is stored as its rendering, but the threshold does not
    /// govern it.
    pub zset_value: usize,
    /// moon's list threshold, in elements. Redis has no such count: its
    /// `list-max-listpack-size` default of `-2` is an 8 KB node BUDGET, which
    /// is what [`EncodingLimits::list_bytes`] is reserved for.
    pub list_entries: usize,
    /// Reserved: a per-node byte budget in the shape of Redis's
    /// `list-max-listpack-size -2`. NOT consulted by any predicate yet —
    /// `usize::MAX` means "no budget", and honouring a budget needs the byte
    /// measure at all three consultation sites in the same change, or the
    /// sites disagree again. Wired so that change is a field flip plus one
    /// predicate, not a hunt.
    pub list_bytes: usize,
}

impl EncodingLimits {
    /// moon's current thresholds — the values every consultation site applied
    /// before the authority existed, so routing through it changes nothing.
    #[inline]
    #[must_use]
    pub const fn moon_defaults() -> Self {
        Self {
            hash_entries: HASH_MAX_LISTPACK_ENTRIES,
            hash_value: HASH_MAX_LISTPACK_VALUE,
            set_entries: SET_MAX_LISTPACK_ENTRIES,
            set_value: SET_MAX_LISTPACK_VALUE,
            set_intset: SET_MAX_INTSET_ENTRIES,
            zset_entries: ZSET_MAX_LISTPACK_ENTRIES,
            zset_value: ZSET_MAX_LISTPACK_VALUE,
            list_entries: LIST_MAX_LISTPACK_ENTRIES,
            list_bytes: usize::MAX,
        }
    }

    /// The most logical items `shape` may hold and stay compact: the policy
    /// threshold, clamped so the listpack it describes never exceeds
    /// [`LISTPACK_SAFE_BATCH_ENTRIES`] entries.
    #[inline]
    #[must_use]
    pub const fn max_items(self, shape: Shape) -> usize {
        let policy = match shape {
            Shape::Hash => self.hash_entries,
            Shape::Set => self.set_entries,
            Shape::SortedSet => self.zset_entries,
            Shape::List => self.list_entries,
        };
        let safe = LISTPACK_SAFE_BATCH_ENTRIES / shape.entries_per_item();
        if policy < safe { policy } else { safe }
    }

    /// The longest element, in bytes, `shape` may hold and stay compact.
    #[inline]
    #[must_use]
    pub const fn max_value(self, shape: Shape) -> usize {
        match shape {
            Shape::Hash => self.hash_value,
            Shape::Set => self.set_value,
            Shape::SortedSet => self.zset_value,
            // A list has no element-size policy of its own in moon today; it
            // shares the set's, which is what the pre-authority code did by
            // reading the single `LISTPACK_MAX_ELEMENT_SIZE`.
            Shape::List => self.set_value,
        }
    }

    /// THE predicate: may `items` logical items of `shape`, the longest of
    /// them `max_elem` bytes, live in the compact form?
    ///
    /// Serves the entry gate (`items` = the batch, `max_elem` = its longest
    /// element), the decode-side re-derivation (`items` = the whole
    /// container) and, through [`EncodingLimits::listpack_fits`], the upgrade
    /// check. Inclusive on both bounds, as Redis is: exactly
    /// `set-max-listpack-entries` members is still a listpack.
    #[inline]
    #[must_use]
    pub const fn fits(self, shape: Shape, items: usize, max_elem: usize) -> bool {
        items <= self.max_items(shape) && max_elem <= self.max_value(shape)
    }

    /// The upgrade check, on the stored listpack: does it still fit?
    ///
    /// Reads `lp.len()` and converts to items through the shape, so the
    /// caller never divides. Element size is not re-measured: the entry gate
    /// refused any oversized element before it was pushed, and an element
    /// already in the listpack passed the same gate when it arrived.
    #[inline]
    #[must_use]
    pub fn listpack_fits(self, shape: Shape, lp: &Listpack) -> bool {
        self.fits(shape, shape.items_in(lp.len()), 0)
    }

    /// May a set of `items` all-integer members live in an intset?
    #[inline]
    #[must_use]
    pub const fn intset_fits(self, items: usize) -> bool {
        items <= self.set_intset
    }
}

impl Default for EncodingLimits {
    #[inline]
    fn default() -> Self {
        Self::moon_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const L: EncodingLimits = EncodingLimits::moon_defaults();

    fn listpack_with(entries: usize) -> Listpack {
        let mut lp = Listpack::new();
        for i in 0..entries {
            let s = format!("e{i}");
            lp.push_back(s.as_bytes());
        }
        lp
    }

    #[test]
    fn defaults_are_moons_current_constants() {
        // Pinned by value: the authority must be behaviour-neutral for the
        // refactor that introduces it. A change here is a policy change and
        // needs the Linux sweep the module docs describe.
        assert_eq!(L.max_items(Shape::Hash), 128);
        assert_eq!(L.max_items(Shape::Set), 128);
        assert_eq!(L.max_items(Shape::SortedSet), 128);
        assert_eq!(L.max_items(Shape::List), 128);
        assert_eq!(L.max_value(Shape::Hash), 64);
        assert_eq!(L.max_value(Shape::Set), 64);
        assert_eq!(L.max_value(Shape::SortedSet), 64);
        assert_eq!(L.max_value(Shape::List), 64);
        assert_eq!(L.set_intset, 512);
        assert!(L.intset_fits(512));
        assert!(!L.intset_fits(513));
        assert_eq!(EncodingLimits::default(), L);
    }

    #[test]
    fn shape_owns_the_entries_per_item_factor() {
        assert_eq!(Shape::Hash.entries_per_item(), 2);
        assert_eq!(Shape::SortedSet.entries_per_item(), 2);
        assert_eq!(Shape::Set.entries_per_item(), 1);
        assert_eq!(Shape::List.entries_per_item(), 1);
        // `HSET k f v f v` — argv after the key is 4 positions, 2 fields.
        assert_eq!(Shape::Hash.items_in(4), 2);
        assert_eq!(Shape::SortedSet.items_in(130), 65);
        assert_eq!(Shape::Set.items_in(130), 130);
    }

    #[test]
    fn fits_is_inclusive_on_both_bounds() {
        for shape in [Shape::Hash, Shape::Set, Shape::SortedSet, Shape::List] {
            assert!(L.fits(shape, 128, 64), "{shape:?}: 128 items of 64 B fit");
            assert!(!L.fits(shape, 129, 64), "{shape:?}: 129 items do not");
            assert!(
                !L.fits(shape, 128, 65),
                "{shape:?}: a 65 B element does not"
            );
            assert!(L.fits(shape, 0, 0), "{shape:?}: empty fits");
        }
    }

    /// The moon#896 shape, stated on the predicate: the SAME verdict must
    /// come out whether the caller holds an argv length, an item count, or
    /// the stored listpack.
    #[test]
    fn argv_items_and_listpack_agree_for_every_shape() {
        for shape in [Shape::Hash, Shape::Set, Shape::SortedSet, Shape::List] {
            let epi = shape.entries_per_item();
            for items in [0usize, 1, 63, 64, 65, 127, 128, 129, 256, 257] {
                let entries = items * epi;
                let by_items = L.fits(shape, items, 0);
                let by_argv = L.fits(shape, shape.items_in(entries), 0);
                let by_listpack = L.listpack_fits(shape, &listpack_with(entries));
                assert_eq!(
                    by_items, by_argv,
                    "{shape:?} items={items}: argv path disagrees"
                );
                assert_eq!(
                    by_items, by_listpack,
                    "{shape:?} items={items}: listpack path disagrees"
                );
                assert_eq!(by_items, items <= 128, "{shape:?} items={items}");
            }
        }
    }

    /// The safety ceiling is load-bearing on its own: a hostile policy is
    /// clamped, so no listpack the authority blesses can approach the
    /// header's `u16` range, and no one-command batch can either.
    #[test]
    fn safety_ceiling_clamps_a_hostile_policy() {
        let hostile = EncodingLimits {
            hash_entries: 1_000_000,
            set_entries: 1_000_000,
            zset_entries: 1_000_000,
            list_entries: 1_000_000,
            ..L
        };
        assert_eq!(hostile.max_items(Shape::Set), LISTPACK_SAFE_BATCH_ENTRIES);
        assert_eq!(hostile.max_items(Shape::List), LISTPACK_SAFE_BATCH_ENTRIES);
        assert_eq!(
            hostile.max_items(Shape::Hash),
            LISTPACK_SAFE_BATCH_ENTRIES / 2
        );
        assert_eq!(
            hostile.max_items(Shape::SortedSet),
            LISTPACK_SAFE_BATCH_ENTRIES / 2
        );
        // The moon#865 batch: 70_000 members in one SADD must never enter
        // the listpack path, whatever the policy says.
        assert!(!hostile.fits(Shape::Set, 70_000, 1));
        assert!(!hostile.fits(Shape::Hash, 40_000, 1));
        // And the clamp leaves the header a 4x margin even when a full
        // listpack takes a full batch.
        assert!(2 * LISTPACK_SAFE_BATCH_ENTRIES <= usize::from(u16::MAX) / 2);
        // The policy default is nowhere near the ceiling, so the clamp never
        // decides a normal write.
        assert!(L.max_items(Shape::Set) * 64 <= LISTPACK_SAFE_BATCH_ENTRIES);
    }

    #[test]
    fn a_smaller_policy_is_honoured_as_is() {
        let tight = EncodingLimits {
            hash_entries: 4,
            hash_value: 8,
            ..L
        };
        assert!(tight.fits(Shape::Hash, 4, 8));
        assert!(!tight.fits(Shape::Hash, 5, 8));
        assert!(!tight.fits(Shape::Hash, 4, 9));
        assert!(tight.listpack_fits(Shape::Hash, &listpack_with(8)));
        assert!(!tight.listpack_fits(Shape::Hash, &listpack_with(10)));
        // Other shapes untouched.
        assert!(tight.fits(Shape::Set, 128, 64));
    }
}
