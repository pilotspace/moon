//! What the boot-time index rescan reads from the keyspace.
//!
//! After a restart, vector and text recovery walk every HASH key that an index
//! covers. Each key is either reconciled against the recovered durable state
//! (`RecoveryState::reconcile_key`) or, if nothing matched it, left
//! unobserved. The deletion probe that follows (`RecoveryState::finish`, and
//! the text plane's `TextRecoveryState::finish`) removes every recovered
//! document whose key was not observed, on the grounds that the key was
//! deleted while the server was down.
//!
//! So the walk has to see EVERY live indexed hash. A key it misses is not only
//! left un-reindexed: it is actively tombstoned. Two kinds of key used to be
//! missed (moon#1074):
//!
//! * a hash spilled to the KV cold tier. The walk read only the hot table,
//!   while `EXISTS`/`HGETALL` still answer the key through the cold index.
//! * a hash with a per-field TTL (`HashWithTtl`, after `HEXPIRE`). It is a
//!   hash in every other respect, but the walk skipped its variant.
//!
//! The keyspace is NOT frozen while the walk runs. The walk yields between
//! slices, and although this shard refuses commands from its own connections
//! while it loads (`shard::loading`), writes routed to it from another shard
//! (SPSC) are applied, and the eviction tick can spill a hot key. So the walk
//! lists KEYS up front and resolves each one's current state at the moment it
//! is reconciled ([`RescanKeys::resolve`]): hot, mid-spill, cold, or gone.
//! Resolving, reading a cold payload and reconciling run with no `.await`
//! between them, so no write can land in between; a key that changed since
//! the listing is reconciled as it is now, never from a stale payload.

use std::ops::Range;
use std::path::{Path, PathBuf};

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::compact_value::{CompactValue, RedisValueRef};
use crate::storage::tiered::cold_index::ColdLocation;
use crate::storage::tiered::cold_read::{ColdReadOutcome, read_cold_entry};
use crate::util::prefix_map::PrefixMap;

/// `[key, field, value, field, value, ...]`, the argument list
/// `RecoveryState::reconcile_key` takes, for a hash value. `None` for any
/// other type, or for a hash with no live field.
///
/// A field whose own TTL has passed at `now_ms` is left out, as every hash
/// read does.
pub(crate) fn hash_rescan_args(
    key: &[u8],
    value: RedisValueRef<'_>,
    now_ms: u64,
) -> Option<Vec<Frame>> {
    let bulk = |b: &[u8]| Frame::BulkString(Bytes::copy_from_slice(b));
    let mut args = vec![bulk(key)];
    match value {
        RedisValueRef::Hash(map) => {
            for (field, v) in map {
                args.push(bulk(field));
                args.push(bulk(v));
            }
        }
        RedisValueRef::HashWithTtl { fields, ttls, .. } => {
            for (field, v) in fields {
                if ttls.get(field).is_some_and(|&t| t <= now_ms) {
                    continue;
                }
                args.push(bulk(field));
                args.push(bulk(v));
            }
        }
        RedisValueRef::HashListpack(lp) => {
            let entries: Vec<_> = lp.iter().collect();
            for pair in entries.chunks_exact(2) {
                args.push(Frame::BulkString(Bytes::from(pair[0].as_bytes())));
                args.push(Frame::BulkString(Bytes::from(pair[1].as_bytes())));
            }
        }
        _ => return None,
    }
    (args.len() > 1).then_some(args)
}

/// Every key of one db that an index prefix covers: the hot ones first, then
/// the ones that were only in the cold tier when the list was taken, sorted
/// by heap file and page so their reads walk each file forwards.
pub(crate) struct RescanKeys {
    keys: Vec<Bytes>,
    hot: usize,
    shard_dir: Option<PathBuf>,
}

impl RescanKeys {
    /// List the keys. Takes no payload: each key is resolved when it is
    /// reconciled ([`Self::resolve`]).
    pub(crate) fn collect(db: &Database, prefixes: &PrefixMap) -> Self {
        let mut keys: Vec<Bytes> = db
            .data()
            .iter()
            .map(|(key, _)| key.as_bytes())
            .filter(|key| prefixes.any_matching(key))
            .map(Bytes::copy_from_slice)
            .collect();
        let hot = keys.len();
        if let Some(index) = db.cold_index.as_ref() {
            let mut cold: Vec<(&Bytes, &ColdLocation)> = index
                .iter()
                .filter(|(key, _)| prefixes.any_matching(key) && !db.is_hot(key))
                .collect();
            cold.sort_unstable_by_key(|(_, loc)| (loc.file_id, loc.page_idx, loc.slot_idx));
            keys.extend(cold.into_iter().map(|(key, _)| key.clone()));
        }
        Self {
            keys,
            hot,
            shard_dir: db.cold_shard_dir.clone(),
        }
    }

    /// Keys that were hot when the list was taken.
    pub(crate) fn hot_len(&self) -> usize {
        self.hot
    }

    /// Keys that were only in the cold tier when the list was taken.
    pub(crate) fn cold_len(&self) -> usize {
        self.keys.len() - self.hot
    }

    pub(crate) fn key(&self, i: usize) -> &[u8] {
        self.keys.get(i).map_or(&[], |k| k.as_ref())
    }

    /// What each key in `range` is NOW. Call under the db guard, then
    /// [`Self::read`] each result with the guard released, then reconcile,
    /// with no `.await` anywhere in between.
    pub(crate) fn resolve(&self, db: &Database, range: Range<usize>, now_ms: u64) -> Vec<Resolved> {
        self.keys
            .get(range)
            .unwrap_or_default()
            .iter()
            .map(|key| resolve_key(db, key, now_ms))
            .collect()
    }

    /// Finish a resolved key: a cold location is read from disk (one page
    /// read). Holds no guard.
    pub(crate) fn read(&self, key: &[u8], resolved: Resolved, now_ms: u64) -> Rescan {
        match resolved {
            Resolved::Hash(args) => Rescan::Hash(args),
            Resolved::Absent => Rescan::Absent,
            Resolved::Cold(loc) => match self.shard_dir.as_deref() {
                Some(dir) => read_cold_rescan(dir, key, loc, now_ms),
                None => Rescan::Unreadable,
            },
        }
    }
}

/// A key's state at the moment it is resolved.
pub(crate) enum Resolved {
    /// Hot, or mid-spill (its payload is still in RAM): the arguments.
    Hash(Vec<Frame>),
    /// Only in the cold tier, at this location: read it.
    Cold(ColdLocation),
    /// Gone, expired, or not a hash.
    Absent,
}

fn resolve_key(db: &Database, key: &[u8], now_ms: u64) -> Resolved {
    let hash = |args: Option<Vec<Frame>>| args.map_or(Resolved::Absent, Resolved::Hash);
    if let Some(entry) = db.data().get(key) {
        return hash(hash_rescan_args(key, entry.as_redis_value(), now_ms));
    }
    // Mid-spill: left hot RAM, not yet in the cold index.
    if let Some(entry) = db.spill_inflight_entry(key, now_ms) {
        return hash(hash_rescan_args(key, entry.as_redis_value(), now_ms));
    }
    match db.cold_index.as_ref().and_then(|index| index.lookup(key)) {
        Some(loc) => Resolved::Cold(loc),
        None => Resolved::Absent,
    }
}

/// What a key's current payload says about its indexed documents.
pub(crate) enum Rescan {
    /// A live hash: reconcile it.
    Hash(Vec<Frame>),
    /// Expired, gone, or not a hash: leave it unobserved.
    Absent,
    /// Indexed in the cold tier, but its bytes could not be read (moon#875).
    /// The key still exists (`EXISTS` answers 1, a read answers `-IOERR`), and
    /// a later read may succeed, so its recovered documents must be kept.
    Unreadable,
}

fn read_cold_rescan(shard_dir: &Path, key: &[u8], loc: ColdLocation, now_ms: u64) -> Rescan {
    match read_cold_entry(shard_dir, loc, now_ms, None) {
        ColdReadOutcome::Hit(value, _ttl) => {
            let value = CompactValue::from_redis_value(value);
            match hash_rescan_args(key, value.as_redis_value(), now_ms) {
                Some(args) => Rescan::Hash(args),
                None => Rescan::Absent,
            }
        }
        ColdReadOutcome::Expired | ColdReadOutcome::Miss => Rescan::Absent,
        ColdReadOutcome::Unreadable(_) => Rescan::Unreadable,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::persistence::kv_page::ValueType;
    use crate::storage::entry::{Entry, RedisValue};
    use crate::storage::tiered::cold_index::ColdIndex;

    fn frames(args: &[Frame]) -> Vec<Vec<u8>> {
        args.iter()
            .map(|f| match f {
                Frame::BulkString(b) => b.to_vec(),
                other => panic!("not a bulk string: {other:?}"),
            })
            .collect()
    }

    fn hash_entry(field: &'static [u8], value: &'static [u8]) -> Entry {
        let mut map = HashMap::new();
        map.insert(Bytes::from_static(field), Bytes::from_static(value));
        let mut entry = Entry::new_string(Bytes::new());
        entry.value = CompactValue::from_redis_value(RedisValue::Hash(Box::new(map)));
        entry
    }

    fn cold_loc(file_id: u64) -> ColdLocation {
        ColdLocation {
            file_id,
            page_idx: 0,
            slot_idx: 0,
            ttl_ms: None,
            value_type: ValueType::Hash,
        }
    }

    fn prefixes() -> PrefixMap {
        let mut p = PrefixMap::new();
        p.insert(&Bytes::from_static(b"idx"), &[Bytes::from_static(b"doc:")]);
        p
    }

    #[test]
    fn keys_are_listed_hot_first_then_cold_only_in_file_order() {
        let mut db = Database::new();
        db.set(b"doc:hot", hash_entry(b"vec", b"1"));
        db.set(b"other:hot", hash_entry(b"vec", b"1"));
        let mut ci = ColdIndex::new();
        ci.insert(Bytes::from_static(b"doc:c9"), cold_loc(9));
        ci.insert(Bytes::from_static(b"doc:c2"), cold_loc(2));
        // Shadowed by its hot copy: listed once, as hot.
        ci.insert(Bytes::from_static(b"doc:hot"), cold_loc(1));
        db.cold_index = Some(ci);

        let keys = RescanKeys::collect(&db, &prefixes());
        assert_eq!((keys.hot_len(), keys.cold_len()), (1, 2));
        let n = keys.hot_len() + keys.cold_len();
        let listed: Vec<&[u8]> = (0..n).map(|i| keys.key(i)).collect();
        assert_eq!(
            listed,
            vec![
                b"doc:hot".as_slice(),
                b"doc:c2".as_slice(),
                b"doc:c9".as_slice()
            ]
        );
    }

    /// The keyspace moves while the walk runs: each key is taken as it is
    /// when resolved, not as it was listed.
    #[test]
    fn a_key_is_resolved_as_it_is_now_not_as_it_was_listed() {
        let mut db = Database::new();
        db.set(b"doc:a", hash_entry(b"vec", b"old"));
        db.set(b"doc:b", hash_entry(b"vec", b"1"));
        let mut ci = ColdIndex::new();
        ci.insert(Bytes::from_static(b"doc:c"), cold_loc(3));
        ci.insert(Bytes::from_static(b"doc:d"), cold_loc(4));
        db.cold_index = Some(ci);
        let keys = RescanKeys::collect(&db, &prefixes());

        // After the listing: a is re-written, b deleted, c promoted to hot and
        // re-written, d deleted from the cold tier.
        db.set(b"doc:a", hash_entry(b"vec", b"new"));
        db.remove(b"doc:b");
        db.set(b"doc:c", hash_entry(b"vec", b"hot"));
        if let Some(ci) = db.cold_index.as_mut() {
            ci.remove(b"doc:c");
            ci.remove(b"doc:d");
        }

        let got: Vec<(Vec<u8>, Option<Vec<Vec<u8>>>)> = keys
            .resolve(&db, 0..keys.hot_len() + keys.cold_len(), 0)
            .into_iter()
            .enumerate()
            .map(|(i, r)| {
                let args = match r {
                    Resolved::Hash(args) => Some(frames(&args)),
                    Resolved::Cold(_) => panic!("{:?} is no longer cold", keys.key(i)),
                    Resolved::Absent => None,
                };
                (keys.key(i).to_vec(), args)
            })
            .collect();
        let field = |v: &[u8]| Some(vec![b"vec".to_vec(), v.to_vec()]);
        let with_key = |k: &[u8], v: &[u8]| {
            field(v).map(|mut f| {
                f.insert(0, k.to_vec());
                f
            })
        };
        let mut want = vec![
            (b"doc:a".to_vec(), with_key(b"doc:a", b"new")),
            (b"doc:b".to_vec(), None),
            (b"doc:c".to_vec(), with_key(b"doc:c", b"hot")),
            (b"doc:d".to_vec(), None),
        ];
        let mut got = got;
        got.sort();
        want.sort();
        assert_eq!(got, want);
    }

    #[test]
    fn a_still_cold_key_resolves_to_its_current_location() {
        let mut db = Database::new();
        let mut ci = ColdIndex::new();
        ci.insert(Bytes::from_static(b"doc:c"), cold_loc(3));
        db.cold_index = Some(ci);
        let keys = RescanKeys::collect(&db, &prefixes());
        if let Some(ci) = db.cold_index.as_mut() {
            // Re-spilled to another file since the listing.
            ci.insert(Bytes::from_static(b"doc:c"), cold_loc(8));
        }
        match keys.resolve(&db, 0..1, 0).pop() {
            Some(Resolved::Cold(loc)) => assert_eq!(loc.file_id, 8),
            _ => panic!("doc:c must resolve to its current cold location"),
        }
    }

    #[test]
    fn hash_args_carry_the_key_then_every_field() {
        let mut map = HashMap::new();
        map.insert(Bytes::from_static(b"vec"), Bytes::from_static(b"0123"));
        let args = hash_rescan_args(b"doc:1", RedisValueRef::Hash(&map), 0).unwrap();
        assert_eq!(
            frames(&args),
            vec![b"doc:1".to_vec(), b"vec".to_vec(), b"0123".to_vec()]
        );
    }

    #[test]
    fn a_hash_with_field_ttls_is_rescanned_without_its_expired_fields() {
        let mut fields = HashMap::new();
        fields.insert(Bytes::from_static(b"vec"), Bytes::from_static(b"0123"));
        fields.insert(Bytes::from_static(b"gone"), Bytes::from_static(b"x"));
        fields.insert(Bytes::from_static(b"later"), Bytes::from_static(b"y"));
        let mut ttls = HashMap::new();
        ttls.insert(Bytes::from_static(b"gone"), 100);
        ttls.insert(Bytes::from_static(b"later"), 10_000);
        let args = hash_rescan_args(
            b"doc:1",
            RedisValueRef::HashWithTtl {
                fields: &fields,
                ttls: &ttls,
                min_expiry_ms: 100,
            },
            100,
        )
        .unwrap();
        let got = frames(&args);
        assert_eq!(got[0], b"doc:1".to_vec());
        let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = got[1..]
            .chunks_exact(2)
            .map(|p| (p[0].clone(), p[1].clone()))
            .collect();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![
                (b"later".to_vec(), b"y".to_vec()),
                (b"vec".to_vec(), b"0123".to_vec()),
            ]
        );
    }

    #[test]
    fn a_non_hash_or_an_empty_hash_is_not_rescanned() {
        assert!(hash_rescan_args(b"k", RedisValueRef::String(b"v"), 0).is_none());
        let empty = HashMap::new();
        assert!(hash_rescan_args(b"k", RedisValueRef::Hash(&empty), 0).is_none());
    }
}
