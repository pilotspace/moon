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
//! No command can write the keyspace while the shard is loading
//! (`shard::loading`), so the cold candidates collected under the db guard stay
//! valid while their payloads are read slice by slice afterwards.

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

/// The indexed keys of one db that live only in the KV cold tier, with the
/// location to read each from.
pub(crate) struct ColdCandidates {
    shard_dir: PathBuf,
    keys: Vec<(Bytes, ColdLocation)>,
}

impl ColdCandidates {
    /// Every cold-index key that an index prefix covers and that has no hot
    /// copy (a hot copy shadows the cold one, and the hot walk already saw
    /// it). Sorted by file and page, so the reads that follow walk each heap
    /// file forwards. `None` when this db has no cold tier.
    pub(crate) fn collect(db: &Database, prefixes: &PrefixMap) -> Option<Self> {
        let shard_dir = db.cold_shard_dir.as_ref()?;
        let index = db.cold_index.as_ref()?;
        let mut keys: Vec<(Bytes, ColdLocation)> = index
            .iter()
            .filter(|(key, _)| prefixes.any_matching(key) && !db.is_hot(key))
            .map(|(key, loc)| (key.clone(), *loc))
            .collect();
        if keys.is_empty() {
            return None;
        }
        keys.sort_unstable_by_key(|(_, loc)| (loc.file_id, loc.page_idx, loc.slot_idx));
        Some(Self {
            shard_dir: shard_dir.clone(),
            keys,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.keys.len()
    }

    /// The `i`th candidate's key, and what reading it produced.
    pub(crate) fn read(&self, i: usize, now_ms: u64) -> Option<(&[u8], ColdRescan)> {
        let (key, loc) = self.keys.get(i)?;
        Some((key, read_cold_rescan(&self.shard_dir, key, *loc, now_ms)))
    }
}

/// What a cold candidate's payload says about its indexed documents.
pub(crate) enum ColdRescan {
    /// A live hash: reconcile it like a hot one.
    Hash(Vec<Frame>),
    /// Expired, gone, or not a hash: leave it unobserved, exactly like a hot
    /// key of the same shape.
    Absent,
    /// Indexed, but its bytes could not be read (moon#875). The key still
    /// exists (`EXISTS` answers 1, a read answers `-IOERR`), and a later read
    /// may succeed, so its recovered documents must be kept, not deleted.
    Unreadable,
}

fn read_cold_rescan(shard_dir: &Path, key: &[u8], loc: ColdLocation, now_ms: u64) -> ColdRescan {
    match read_cold_entry(shard_dir, loc, now_ms, None) {
        ColdReadOutcome::Hit(value, _ttl) => {
            let value = CompactValue::from_redis_value(value);
            match hash_rescan_args(key, value.as_redis_value(), now_ms) {
                Some(args) => ColdRescan::Hash(args),
                None => ColdRescan::Absent,
            }
        }
        ColdReadOutcome::Expired | ColdReadOutcome::Miss => ColdRescan::Absent,
        ColdReadOutcome::Unreadable(_) => ColdRescan::Unreadable,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn frames(args: &[Frame]) -> Vec<Vec<u8>> {
        args.iter()
            .map(|f| match f {
                Frame::BulkString(b) => b.to_vec(),
                other => panic!("not a bulk string: {other:?}"),
            })
            .collect()
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
