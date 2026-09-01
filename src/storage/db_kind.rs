//! W5: per-type classification/projection traits backing `Database`'s
//! generic typed accessors.
//!
//! The ~20-method accessor matrix in `db.rs` (`get_X` / `get_or_create_X` /
//! `get_X_ref_if_alive` per container type) copy-pasted the same skeleton —
//! expiry check → cold promote/read-through → compact-encoding upgrade →
//! variant match — once per type. The skeleton now exists once per access
//! shape as a generic in `db.rs` (`get_ref_if_alive::<K>`,
//! `get_or_create::<K>`, `get_promoted::<K>`); everything genuinely
//! per-type — which enum variants map to which view, how a compact encoding
//! upgrades, what a fresh entry looks like — lives here as a [`ValueKind`] /
//! [`OwnedKind`] impl on a zero-sized marker type. Static dispatch only:
//! every `K` is a concrete marker resolved at compile time, so the generated
//! code is identical to the hand-written originals.

use bytes::Bytes;
use std::collections::{HashMap, HashSet, VecDeque};

use super::bptree::BPTree;
use super::compact_value::{CompactValue, RedisValueRef};
use super::db_read::{HashRef, ListRef, SetRef, SortedSetRef, StreamRef};
use super::entry::{Entry, RedisValue};
use super::stream::Stream as StreamData;

/// Classification failure: the value exists but holds a different type.
/// The generic accessors translate this into the `WRONGTYPE` error frame.
pub struct WrongType;

/// A container type readable through a borrowed-or-owned view (`*Ref` enum)
/// without `&mut Database` — backs `Database::get_ref_if_alive::<K>`.
pub trait ValueKind {
    /// The view handed to command code (`HashRef`, `ListRef`, …).
    type Ref<'a>;

    /// Classify a HOT entry's borrowed value into the view.
    fn classify_hot(val: RedisValueRef<'_>, now_ms: u64) -> Result<Self::Ref<'_>, WrongType>;

    /// Classify an OWNED value decoded fresh from the cold tier. The
    /// returned view holds owned data, so it satisfies any caller lifetime.
    fn classify_cold<'a>(val: RedisValue, now_ms: u64) -> Result<Self::Ref<'a>, WrongType>;
}

/// A container type accessible in its full (non-compact) encoding via
/// `&mut Database` — backs `Database::get_or_create::<K>` and
/// `Database::get_promoted::<K>`.
pub trait OwnedKind {
    /// Mutable projection of the full encoding (e.g. `&mut HashMap<..>`, or
    /// the `(&mut members, &mut tree)` pair for sorted sets).
    type Mut<'a>;
    /// Shared projection of the full encoding.
    type Shared<'a>;

    /// A fresh empty entry for `get_or_create` to insert on miss.
    fn new_entry() -> Entry;

    /// Upgrade this type's compact encoding(s) to the full one, in place.
    /// Exactly the conversions the hand-written accessors performed — e.g.
    /// sorted sets upgrade both the legacy `BTreeMap` form and (since
    /// moon#787) `SortedSetListpack`.
    fn upgrade(entry: &mut Entry);

    /// Project the (post-upgrade) full encoding out of the value, mutably.
    fn project_mut(val: &mut RedisValue) -> Result<Self::Mut<'_>, WrongType>;

    /// Project the (post-upgrade) full encoding out of the value, shared.
    fn project_ref(val: RedisValueRef<'_>) -> Result<Self::Shared<'_>, WrongType>;
}

// ── Markers ─────────────────────────────────────────────────────────────

pub struct HashKind;
pub struct ListKind;
pub struct SetKind;
pub struct SortedSetKind;
pub struct StreamKind;

// ── Hash ────────────────────────────────────────────────────────────────

impl ValueKind for HashKind {
    type Ref<'a> = HashRef<'a>;

    fn classify_hot(val: RedisValueRef<'_>, now_ms: u64) -> Result<Self::Ref<'_>, WrongType> {
        match val {
            RedisValueRef::Hash(map) => Ok(HashRef::Map(map)),
            RedisValueRef::HashListpack(lp) => Ok(HashRef::Listpack(lp)),
            RedisValueRef::HashWithTtl {
                fields,
                ttls,
                min_expiry_ms,
            } => Ok(HashRef::WithTtl {
                fields,
                ttls,
                now_ms,
                min_expiry_ms,
            }),
            _ => Err(WrongType),
        }
    }

    fn classify_cold<'a>(val: RedisValue, now_ms: u64) -> Result<Self::Ref<'a>, WrongType> {
        match val {
            RedisValue::Hash(map) => Ok(HashRef::Owned(map)),
            RedisValue::HashWithTtl {
                fields,
                ttls,
                min_expiry_ms,
            } => Ok(HashRef::OwnedWithTtl {
                fields,
                ttls,
                now_ms,
                min_expiry_ms,
            }),
            _ => Err(WrongType),
        }
    }
}

impl OwnedKind for HashKind {
    type Mut<'a> = &'a mut HashMap<Bytes, Bytes>;
    type Shared<'a> = &'a HashMap<Bytes, Bytes>;

    fn new_entry() -> Entry {
        Entry::new_hash()
    }

    fn upgrade(entry: &mut Entry) {
        if let Some(v) = entry.value.as_redis_value_mut() {
            if let RedisValue::HashListpack(lp) = v {
                let map = lp.to_hash_map();
                *v = RedisValue::Hash(map);
            }
        }
    }

    fn project_mut(val: &mut RedisValue) -> Result<Self::Mut<'_>, WrongType> {
        match val {
            RedisValue::Hash(map) => Ok(map),
            // HashWithTtl: the fields sub-map is a plain HashMap. HSET/
            // HMSET/HINCRBY mutate it directly; they never touch the ttls
            // sidecar, which is managed by the HEXPIRE family.
            RedisValue::HashWithTtl { fields, .. } => Ok(fields),
            _ => Err(WrongType),
        }
    }

    fn project_ref(val: RedisValueRef<'_>) -> Result<Self::Shared<'_>, WrongType> {
        match val {
            RedisValueRef::Hash(map) => Ok(map),
            RedisValueRef::HashWithTtl { fields, .. } => Ok(fields),
            _ => Err(WrongType),
        }
    }
}

// ── List ────────────────────────────────────────────────────────────────

impl ValueKind for ListKind {
    type Ref<'a> = ListRef<'a>;

    fn classify_hot(val: RedisValueRef<'_>, _now_ms: u64) -> Result<Self::Ref<'_>, WrongType> {
        match val {
            RedisValueRef::List(list) => Ok(ListRef::Deque(list)),
            RedisValueRef::ListListpack(lp) => Ok(ListRef::Listpack(lp)),
            _ => Err(WrongType),
        }
    }

    fn classify_cold<'a>(val: RedisValue, _now_ms: u64) -> Result<Self::Ref<'a>, WrongType> {
        match val {
            RedisValue::List(list) => Ok(ListRef::Owned(list)),
            _ => Err(WrongType),
        }
    }
}

impl OwnedKind for ListKind {
    type Mut<'a> = &'a mut VecDeque<Bytes>;
    type Shared<'a> = &'a VecDeque<Bytes>;

    fn new_entry() -> Entry {
        Entry::new_list()
    }

    fn upgrade(entry: &mut Entry) {
        if let Some(v) = entry.value.as_redis_value_mut() {
            if let RedisValue::ListListpack(lp) = v {
                let list = lp.to_vec_deque();
                *v = RedisValue::List(list);
            }
        }
    }

    fn project_mut(val: &mut RedisValue) -> Result<Self::Mut<'_>, WrongType> {
        match val {
            RedisValue::List(list) => Ok(list),
            _ => Err(WrongType),
        }
    }

    fn project_ref(val: RedisValueRef<'_>) -> Result<Self::Shared<'_>, WrongType> {
        match val {
            RedisValueRef::List(list) => Ok(list),
            _ => Err(WrongType),
        }
    }
}

// ── Set ─────────────────────────────────────────────────────────────────

impl ValueKind for SetKind {
    type Ref<'a> = SetRef<'a>;

    fn classify_hot(val: RedisValueRef<'_>, _now_ms: u64) -> Result<Self::Ref<'_>, WrongType> {
        match val {
            RedisValueRef::Set(set) => Ok(SetRef::Hash(set)),
            RedisValueRef::SetListpack(lp) => Ok(SetRef::Listpack(lp)),
            RedisValueRef::SetIntset(is) => Ok(SetRef::Intset(is)),
            _ => Err(WrongType),
        }
    }

    fn classify_cold<'a>(val: RedisValue, _now_ms: u64) -> Result<Self::Ref<'a>, WrongType> {
        match val {
            RedisValue::Set(set) => Ok(SetRef::Owned(set)),
            _ => Err(WrongType),
        }
    }
}

impl OwnedKind for SetKind {
    type Mut<'a> = &'a mut HashSet<Bytes>;
    type Shared<'a> = &'a HashSet<Bytes>;

    fn new_entry() -> Entry {
        Entry::new_set()
    }

    fn upgrade(entry: &mut Entry) {
        if let Some(v) = entry.value.as_redis_value_mut() {
            match v {
                RedisValue::SetListpack(lp) => {
                    let set = lp.to_hash_set();
                    *v = RedisValue::Set(set);
                }
                RedisValue::SetIntset(is) => {
                    let set = is.to_hash_set();
                    *v = RedisValue::Set(set);
                }
                _ => {}
            }
        }
    }

    fn project_mut(val: &mut RedisValue) -> Result<Self::Mut<'_>, WrongType> {
        match val {
            RedisValue::Set(set) => Ok(set),
            _ => Err(WrongType),
        }
    }

    fn project_ref(val: RedisValueRef<'_>) -> Result<Self::Shared<'_>, WrongType> {
        match val {
            RedisValueRef::Set(set) => Ok(set),
            _ => Err(WrongType),
        }
    }
}

// ── Sorted set ──────────────────────────────────────────────────────────

/// Materialise a `SortedSetListpack`'s `[member, score, …]` pairs into the
/// full `(members, tree)` pair of the `SortedSetBPTree` encoding.
///
/// Shared by [`SortedSetKind::upgrade`] and
/// `Database::upgrade_zset_listpack_to_bptree` so the promotion has exactly
/// one implementation. An unparseable score means in-memory corruption (the
/// only writer is the ZADD listpack path, which always writes
/// `format_score_bytes` output); this is a read-side conversion with no way
/// to report an error, so it keeps the member at 0.0 and logs rather than
/// dropping data on the floor.
pub(super) fn zset_listpack_to_bptree(
    lp: &super::listpack::Listpack,
) -> (HashMap<Bytes, f64>, BPTree) {
    let mut tree = BPTree::new();
    let mut members: HashMap<Bytes, f64> = HashMap::with_capacity(lp.len() / 2);
    for (member_entry, score_entry) in lp.iter_pairs() {
        let member = member_entry.to_bytes();
        let score = match score_entry.as_score() {
            Some(s) => s,
            None => {
                tracing::error!("zset listpack holds an unparseable score; keeping member at 0.0");
                0.0
            }
        };
        // Defensive: the writer rejects duplicates, but a corrupt listpack
        // must not leave `members` and `tree` disagreeing on cardinality.
        if let Some(old) = members.insert(member.clone(), score) {
            tree.remove(ordered_float::OrderedFloat(old), &member);
        }
        tree.insert(ordered_float::OrderedFloat(score), member);
    }
    (members, tree)
}

impl ValueKind for SortedSetKind {
    type Ref<'a> = SortedSetRef<'a>;

    fn classify_hot(val: RedisValueRef<'_>, _now_ms: u64) -> Result<Self::Ref<'_>, WrongType> {
        match val {
            RedisValueRef::SortedSetBPTree { tree, members } => {
                Ok(SortedSetRef::BPTree { tree, members })
            }
            RedisValueRef::SortedSetListpack(lp) => Ok(SortedSetRef::Listpack(lp)),
            RedisValueRef::SortedSet { members, scores } => {
                Ok(SortedSetRef::Legacy { members, scores })
            }
            _ => Err(WrongType),
        }
    }

    fn classify_cold<'a>(val: RedisValue, _now_ms: u64) -> Result<Self::Ref<'a>, WrongType> {
        match val {
            RedisValue::SortedSetBPTree { tree, members } => {
                Ok(SortedSetRef::Owned { tree, members })
            }
            _ => Err(WrongType),
        }
    }
}

impl OwnedKind for SortedSetKind {
    type Mut<'a> = (&'a mut HashMap<Bytes, f64>, &'a mut BPTree);
    type Shared<'a> = (&'a HashMap<Bytes, f64>, &'a BPTree);

    fn new_entry() -> Entry {
        Entry::new_sorted_set_bptree()
    }

    /// Upgrade a compact/legacy sorted-set encoding to `SortedSetBPTree`.
    ///
    /// Two sources: the legacy `SortedSet` (BTreeMap) form, and — since
    /// moon#787 made ZADD produce them — `SortedSetListpack`. The listpack
    /// arm is the safety net for the whole zset command surface: only ZADD
    /// knows how to mutate a listpack in place, so every OTHER command
    /// reaches the value through `get_or_create` / `get_promoted` /
    /// `get_mut_if_present`, all of which call this. Without the arm those
    /// commands would see `project_mut` fail and answer WRONGTYPE on a
    /// perfectly good zset. Promotion is one-way, matching Redis: a zset
    /// that leaves the listpack encoding never returns to it.
    fn upgrade(entry: &mut Entry) {
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::SortedSet { members, scores }) => {
                let mut tree = BPTree::new();
                let new_members = std::mem::take(members);
                let old_scores = std::mem::take(scores);
                for ((score, member), ()) in old_scores {
                    tree.insert(score, member);
                }
                entry.value = CompactValue::from_redis_value(RedisValue::SortedSetBPTree {
                    tree,
                    members: new_members,
                });
            }
            Some(RedisValue::SortedSetListpack(lp)) => {
                let (members, tree) = zset_listpack_to_bptree(lp);
                entry.value =
                    CompactValue::from_redis_value(RedisValue::SortedSetBPTree { tree, members });
            }
            _ => {}
        }
    }

    fn project_mut(val: &mut RedisValue) -> Result<Self::Mut<'_>, WrongType> {
        match val {
            RedisValue::SortedSetBPTree { members, tree } => Ok((members, tree)),
            _ => Err(WrongType),
        }
    }

    fn project_ref(val: RedisValueRef<'_>) -> Result<Self::Shared<'_>, WrongType> {
        match val {
            RedisValueRef::SortedSetBPTree { tree, members } => Ok((members, tree)),
            _ => Err(WrongType),
        }
    }
}

// ── Stream ──────────────────────────────────────────────────────────────

impl ValueKind for StreamKind {
    type Ref<'a> = StreamRef<'a>;

    fn classify_hot(val: RedisValueRef<'_>, _now_ms: u64) -> Result<Self::Ref<'_>, WrongType> {
        match val {
            RedisValueRef::Stream(s) => Ok(StreamRef::Borrowed(s)),
            _ => Err(WrongType),
        }
    }

    fn classify_cold<'a>(val: RedisValue, _now_ms: u64) -> Result<Self::Ref<'a>, WrongType> {
        match val {
            RedisValue::Stream(s) => Ok(StreamRef::Owned(s)),
            _ => Err(WrongType),
        }
    }
}

impl OwnedKind for StreamKind {
    type Mut<'a> = &'a mut StreamData;
    type Shared<'a> = &'a StreamData;

    fn new_entry() -> Entry {
        Entry::new_stream()
    }

    /// Streams have no compact encoding — nothing to upgrade.
    fn upgrade(_entry: &mut Entry) {}

    fn project_mut(val: &mut RedisValue) -> Result<Self::Mut<'_>, WrongType> {
        match val {
            RedisValue::Stream(s) => Ok(s.as_mut()),
            _ => Err(WrongType),
        }
    }

    fn project_ref(val: RedisValueRef<'_>) -> Result<Self::Shared<'_>, WrongType> {
        match val {
            RedisValueRef::Stream(s) => Ok(s),
            _ => Err(WrongType),
        }
    }
}
