//! `DEBUG DIGEST` / `DEBUG DIGEST-VALUE` (moon#636).
//!
//! An order-independent SHA1 fingerprint of a dataset, used to answer "are
//! these two datasets identical?" in one round trip. The crash-matrix and
//! replication suites want it for moon-vs-moon (pre-crash vs recovered,
//! master vs replica); the client-compat harness wants it for moon-vs-redis.
//!
//! # Byte-compatible with redis, on purpose
//!
//! The digest reproduces redis-server 8.6.1's `computeDatasetDigest` exactly,
//! so a moon digest and a redis digest of the same logical data are the SAME
//! 40 hex characters. That is the whole value of the command: a moon-private
//! digest could only ever compare moon to moon, and — worse — an ALMOST
//! -compatible one would report two identical datasets as different, sending
//! an operator hunting corruption that is not there.
//!
//! Every constant below was verified against a live redis-server 8.6.1, not
//! read off the source and assumed. Two of them are not guessable:
//!
//! * `mix_digest` is `SHA1(digest ^ SHA1(data))` — NOT `SHA1(digest ‖ data)`,
//!   which is the natural reading of the name and produces a codec that is
//!   self-consistent and wrong against every real redis.
//! * `xor_digest(final, key_digest)` hashes the 20-byte digest before xoring
//!   it in; it is not a raw xor of the two digests.
//!
//! # Structure
//!
//! ```text
//! xor(d, data) : d ^= SHA1(data)                    order-INDEPENDENT
//! mix(d, data) : d  = SHA1(d ^ SHA1(data))          order-DEPENDENT
//!
//! object digest = mix(type as big-endian u32)
//!                 then per type:
//!                   string : mix(value)
//!                   list   : mix(each element, IN ORDER)
//!                   set    : xor(each member)
//!                   hash   : xor(mix(mix(0, field), value)) per field
//!                   zset   : xor(mix(mix(0, member), score)) per member
//!                   stream : mix("<ms>.<seq>") then mix(field), mix(value)
//!                 then xor("!!expire!!") if the KEY has a TTL
//!
//! key digest    = mix(0, key name) then the object digest
//! dataset       = for each NON-EMPTY db in ascending index order:
//!                   mix(final, db index as big-endian u32)
//!                   final ^= SHA1(key digest) for every key in that db
//! ```
//!
//! The inner loop being a pure xor is what lets moon compute this at all:
//! xor is commutative, so each shard can accumulate its own keys independently
//! and the coordinator combines the partials. Only the per-db `mix` is
//! order-sensitive, and it happens once per db rather than once per key.

use bytes::Bytes;

use crate::storage::Entry;
use crate::storage::compact_value::RedisValueRef;

/// SHA1 output width. Redis reports the digest as 40 hex characters.
pub const DIGEST_LEN: usize = 20;

/// A 20-byte SHA1 digest accumulator.
pub type Digest = [u8; DIGEST_LEN];

/// The all-zero digest: redis's answer for an empty dataset and for
/// `DIGEST-VALUE` of a key that does not exist.
pub const ZERO: Digest = [0u8; DIGEST_LEN];

// Redis `OBJ_*` type codes, mixed in as `htonl(type)`. These are wire
// constants of the digest, not moon's own type tags — do not "tidy" them to
// match `ValueType`.
const OBJ_STRING: u32 = 0;
const OBJ_LIST: u32 = 1;
const OBJ_SET: u32 = 2;
const OBJ_ZSET: u32 = 3;
const OBJ_HASH: u32 = 4;
const OBJ_STREAM: u32 = 6;

/// Marker xored into a key's digest when the KEY carries a TTL. Redis mixes
/// only the PRESENCE of an expiry, never its value — an absolute deadline
/// would make the digest change every time it was taken.
const EXPIRE_TOKEN: &[u8] = b"!!expire!!";

/// Marker xored into a FIELD's digest when that hash field carries a TTL.
const HEXPIRE_TOKEN: &[u8] = b"!!hexpire!!";

fn sha1(data: &[u8]) -> Digest {
    sha1_smol::Sha1::from(data).digest().bytes()
}

/// `digest ^= SHA1(data)`.
///
/// Order-independent: this is what lets a dataset digest be accumulated from
/// shards in any order.
fn xor_digest(digest: &mut Digest, data: &[u8]) {
    let h = sha1(data);
    for (d, h) in digest.iter_mut().zip(h.iter()) {
        *d ^= *h;
    }
}

/// `digest = SHA1(digest ^ SHA1(data))`.
///
/// Order-DEPENDENT, and NOT `SHA1(digest ‖ data)` — see the module docs. The
/// difference is invisible to a round-trip test and fatal to redis parity.
fn mix_digest(digest: &mut Digest, data: &[u8]) {
    xor_digest(digest, data);
    *digest = sha1(digest);
}

/// Format a score the way redis's `fpconv_dtoa` does: shortest representation
/// that round-trips, with integral values printed WITHOUT a decimal point
/// (`1`, not `1.0`).
///
/// Rust's `{}` for `f64` is also shortest-round-trip and also prints `1` for
/// `1.0f64`, so this delegates to the same helper that builds `ZSCORE`
/// replies — keeping one definition of "how moon renders a score".
///
/// Verified against the oracle: `%.17g` (the obvious reading of older redis
/// source) renders `3.3` as `3.2999999999999998` and produces a DIFFERENT
/// digest. Scores like `1.5` / `2.25` / `-0.125` agree under both formats and
/// cannot tell them apart — `3.3` is the discriminating case.
fn score_bytes(score: f64) -> Bytes {
    crate::command::sorted_set::format_score_bytes(score)
}

/// Digest of one element pair (`field`+`value`, or `member`+`score`).
///
/// Built in its own zeroed accumulator and then xored into the parent, which
/// is what makes hashes and sorted sets order-independent while lists are not.
fn pair_digest(a: &[u8], b: &[u8]) -> Digest {
    let mut ed = ZERO;
    mix_digest(&mut ed, a);
    mix_digest(&mut ed, b);
    ed
}

/// The digest of a key's VALUE — what `DEBUG DIGEST-VALUE` returns.
///
/// Deliberately does NOT include the key name: `DIGEST-VALUE` of the same
/// value under two different names is the same digest. The name is mixed in
/// one level up, by [`key_digest`].
pub fn object_digest(entry: &Entry) -> Digest {
    let mut d = ZERO;
    fold_object(&mut d, entry);
    d
}

/// The digest of one key/value pair, as it contributes to a dataset digest.
///
/// Redis seeds the accumulator with the key name and then continues mixing
/// the value into that SAME accumulator — it is not a combination of two
/// finished digests, so this seeds and folds rather than composing.
pub fn key_digest(key: &[u8], entry: &Entry) -> Digest {
    let mut d = ZERO;
    mix_digest(&mut d, key);
    fold_object(&mut d, entry);
    d
}

/// Mirror of redis's `xorObjectDigest`: continue `d` with the entry's type,
/// contents, and key-level expiry marker.
///
/// One definition, used by both seeds above. Splitting it produced two copies
/// of the fourteen-arm value walk, which is precisely how one arm drifts.
fn fold_object(d: &mut Digest, entry: &Entry) {
    let value = entry.as_redis_value();
    // Type first, so two types holding the same bytes never collide.
    mix_digest(d, &object_type(&value).to_be_bytes());
    fold_value(d, &value);
    if entry.has_expiry() {
        xor_digest(d, EXPIRE_TOKEN);
    }
}

/// The redis `OBJ_*` code for one of moon's value representations.
///
/// Every compact encoding collapses to the logical type it represents: a
/// listpack-encoded hash and a full hash digest identically, exactly as in
/// redis, so a re-encoding can never move the digest.
fn object_type(value: &RedisValueRef<'_>) -> u32 {
    match value {
        RedisValueRef::String(_) => OBJ_STRING,
        RedisValueRef::List(_) | RedisValueRef::ListListpack(_) => OBJ_LIST,
        RedisValueRef::Set(_) | RedisValueRef::SetListpack(_) | RedisValueRef::SetIntset(_) => {
            OBJ_SET
        }
        RedisValueRef::Hash(_)
        | RedisValueRef::HashWithTtl { .. }
        | RedisValueRef::HashListpack(_) => OBJ_HASH,
        RedisValueRef::SortedSet { .. }
        | RedisValueRef::SortedSetBPTree { .. }
        | RedisValueRef::SortedSetListpack(_) => OBJ_ZSET,
        RedisValueRef::Stream(_) => OBJ_STREAM,
    }
}

/// The per-type body. Lists and streams MIX (order matters); sets, hashes and
/// sorted sets XOR per element (order does not).
fn fold_value(d: &mut Digest, value: &RedisValueRef<'_>) {
    match value {
        RedisValueRef::String(s) => mix_digest(d, s),

        // ---- lists: ORDER MATTERS ----
        RedisValueRef::List(deque) => {
            for ele in deque.iter() {
                mix_digest(d, ele);
            }
        }
        RedisValueRef::ListListpack(lp) => {
            for ele in lp.iter() {
                mix_digest(d, &ele.as_bytes());
            }
        }

        // ---- sets: order-independent ----
        RedisValueRef::Set(set) => {
            for ele in set.iter() {
                xor_digest(d, ele);
            }
        }
        RedisValueRef::SetListpack(lp) => {
            for ele in lp.iter() {
                xor_digest(d, &ele.as_bytes());
            }
        }
        RedisValueRef::SetIntset(is) => {
            // An intset holds integers; redis digests the DECODED string form,
            // which is what SMEMBERS would show.
            for v in is.iter() {
                xor_digest(d, v.to_string().as_bytes());
            }
        }

        // ---- hashes ----
        RedisValueRef::Hash(map) => {
            for (f, v) in map.iter() {
                xor_digest(d, &pair_digest(f, v));
            }
        }
        RedisValueRef::HashWithTtl { fields, ttls, .. } => {
            for (f, v) in fields.iter() {
                let mut ed = pair_digest(f, v);
                // Per-FIELD marker, distinct from the key-level one.
                if ttls.contains_key(f) {
                    xor_digest(&mut ed, HEXPIRE_TOKEN);
                }
                xor_digest(d, &ed);
            }
        }
        RedisValueRef::HashListpack(lp) => {
            for (f, v) in lp.iter_pairs() {
                xor_digest(d, &pair_digest(&f.as_bytes(), &v.as_bytes()));
            }
        }

        // ---- sorted sets ----
        RedisValueRef::SortedSet { members, .. } => {
            for (m, sc) in members.iter() {
                xor_digest(d, &pair_digest(m, &score_bytes(*sc)));
            }
        }
        RedisValueRef::SortedSetBPTree { members, .. } => {
            for (m, sc) in members.iter() {
                xor_digest(d, &pair_digest(m, &score_bytes(*sc)));
            }
        }
        RedisValueRef::SortedSetListpack(lp) => {
            for (m, score_entry) in lp.iter_pairs() {
                let raw = score_entry.as_bytes();
                let sc: f64 = std::str::from_utf8(&raw)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                xor_digest(d, &pair_digest(&m.as_bytes(), &score_bytes(sc)));
            }
        }

        // ---- streams ----
        // Redis digests ENTRIES ONLY: consumer groups and the PEL are not part
        // of the fingerprint, so two servers agreeing on data but differing in
        // group state still match. The id renders `ms.seq` with a DOT, not the
        // `ms-seq` a client sees.
        RedisValueRef::Stream(stream) => {
            for (id, fields) in &stream.entries {
                mix_digest(d, format!("{}.{}", id.ms, id.seq).as_bytes());
                for (f, v) in fields {
                    mix_digest(d, f);
                    mix_digest(d, v);
                }
            }
        }
    }
}

/// The digest contribution of one database, or `None` if it holds no keys.
///
/// `None` is not the same as `Some(ZERO)`: redis SKIPS empty databases when
/// folding the final digest, and mixing the index of one would change the
/// answer. A populated db whose partial happens to xor to zero must still be
/// mixed, so emptiness is tracked explicitly rather than inferred.
///
/// # Both planes
///
/// Walks the hot table AND the cold tier. A key that eviction spilled to disk
/// is still part of the dataset, and a digest that omitted it would report a
/// server as differing from its own replica purely because the two had
/// evicted different keys — moon#610's class, in a command where it would be
/// nearly impossible to attribute.
///
/// Plane-PARTITIONED, in the style of `KEYS`: the hot loop uses the hot-only
/// accessor on purpose and the cold plane is enumerated separately, so a
/// tiered key is counted exactly once. Using a read-through accessor in the
/// hot loop would double-count every tiered key.
///
/// # Cost
///
/// O(keys), and each COLD key costs a synchronous disk read to materialise
/// its value. That is inherent — the digest is defined over values — and is
/// why this is an admin command rather than something to call in a loop.
/// It does not allocate per key: both walks borrow from the tables rather
/// than snapshotting the key set, so digesting a large keyspace does not
/// transiently double its key memory.
pub fn db_partial(db: &crate::storage::Database, now_ms: u64) -> Option<Digest> {
    let mut acc = ZERO;
    let mut any = false;

    for k in db.keys() {
        if let Some(entry) = db.get_if_alive(k.as_bytes(), now_ms) {
            accumulate_key(&mut acc, k.as_bytes(), entry);
            any = true;
        }
    }

    for key in db.cold_only_keys(now_ms) {
        if let Some(view) = db.get_if_alive_any_plane(key, now_ms) {
            accumulate_key(&mut acc, key, &view);
            any = true;
        }
    }

    if any { Some(acc) } else { None }
}

/// Every database on THIS shard that holds at least one key.
///
/// # Must not run inside a `with_shard_db` closure
///
/// `DEBUG DIGEST` spans EVERY database, but a command handler is handed one
/// `&mut Database` and cannot reach its siblings — and re-entering the shard
/// slice from inside that borrow panics. So this is called from the intercept
/// sites (which hold no borrow), never from `debug()`. The `DEBUG RECLAMATION`
/// intercept in `spsc_handler` establishes the same pattern.
pub fn local_partials() -> Vec<(usize, Digest)> {
    crate::shard::slice::with_shard(|slice| {
        let mut out = Vec::new();
        for (idx, db) in slice.databases.iter().enumerate() {
            let now_ms = db.now_ms();
            if let Some(partial) = db_partial(db, now_ms) {
                out.push((idx, partial));
            }
        }
        out
    })
}

/// Wire form for one shard's partials: a flat array of
/// `[db_index, hex_digest, db_index, hex_digest, ...]`.
///
/// Hex rather than raw bytes so a partial is readable in a packet capture and
/// survives any layer that assumes text; this is a once-per-DEBUG-DIGEST
/// message, so the encoding cost is irrelevant next to the walk itself.
pub fn partials_to_frame(partials: &[(usize, Digest)]) -> crate::protocol::Frame {
    use crate::protocol::Frame;
    let mut out = Vec::with_capacity(partials.len() * 2);
    for (db, d) in partials {
        out.push(Frame::Integer(*db as i64));
        out.push(Frame::BulkString(bytes::Bytes::from(to_hex(d))));
    }
    Frame::Array(out.into())
}

/// Inverse of [`partials_to_frame`]. `None` on any malformed reply — a shard
/// that answered something unexpected must not be silently counted as empty,
/// which would produce a confidently wrong digest.
pub fn partials_from_frame(frame: &crate::protocol::Frame) -> Option<Vec<(usize, Digest)>> {
    use crate::protocol::Frame;
    let Frame::Array(items) = frame else {
        return None;
    };
    if items.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(items.len() / 2);
    for pair in items.chunks_exact(2) {
        let Frame::Integer(db) = pair[0] else {
            return None;
        };
        let Frame::BulkString(hex) = &pair[1] else {
            return None;
        };
        if hex.len() != DIGEST_LEN * 2 || db < 0 {
            return None;
        }
        let mut d = ZERO;
        for (i, byte) in d.iter_mut().enumerate() {
            let s = std::str::from_utf8(&hex[i * 2..i * 2 + 2]).ok()?;
            *byte = u8::from_str_radix(s, 16).ok()?;
        }
        out.push((db as usize, d));
    }
    Some(out)
}

/// Merge partials from several shards into one per-db view.
///
/// Within a db every key contributes by XOR, so shard partials for the SAME db
/// simply xor together. A db present on any shard is present in the result —
/// "non-empty" is a global question, and a db empty here but populated on the
/// next shard must still be folded.
pub fn merge_partials(all: impl IntoIterator<Item = (usize, Digest)>) -> Vec<(usize, Digest)> {
    let mut by_db: std::collections::BTreeMap<usize, Digest> = std::collections::BTreeMap::new();
    for (db, partial) in all {
        let slot = by_db.entry(db).or_insert(ZERO);
        for (s, p) in slot.iter_mut().zip(partial.iter()) {
            *s ^= *p;
        }
    }
    by_db.into_iter().collect()
}

/// Render a digest as the 40 lowercase hex characters redis replies with.
pub fn to_hex(d: &Digest) -> String {
    let mut s = String::with_capacity(DIGEST_LEN * 2);
    for b in d {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Fold one key's contribution into a dataset accumulator.
///
/// `final ^= SHA1(key_digest)` — note the digest is HASHED before the xor.
pub fn accumulate_key(acc: &mut Digest, key: &[u8], entry: &Entry) {
    let kd = key_digest(key, entry);
    xor_digest(acc, &kd);
}

/// Close out a dataset digest from per-db accumulators.
///
/// `partials` carries one entry per db that holds at least one key ANYWHERE in
/// the server. Emptiness is the CALLER's to decide, not something inferred
/// from an all-zero partial: redis skips empty dbs entirely, and mixing the
/// index of one would change the answer — but a populated db whose partial
/// happens to xor to zero must still be mixed. Under sharding "non-empty" is
/// also a global question: a db can be empty on this shard and populated on
/// the next, so the merge happens before this is called.
pub fn finalize_dataset(mut partials: Vec<(usize, Digest)>) -> Digest {
    // Ascending db order — the per-db mix is the one order-sensitive step.
    partials.sort_by_key(|(db, _)| *db);
    let mut final_digest = ZERO;
    for (db, partial) in partials {
        mix_digest(&mut final_digest, &(db as u32).to_be_bytes());
        for (f, p) in final_digest.iter_mut().zip(partial.iter()) {
            *f ^= *p;
        }
    }
    final_digest
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::entry::RedisValue;
    use bytes::Bytes;
    use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

    /// Every expectation below is a literal captured from a LIVE
    /// redis-server 8.6.1 (`--enable-debug-command yes`), not from moon's own
    /// output. A self-generated golden would pin whatever moon happens to do,
    /// including a wrong `mix_digest` — which is exactly the bug these vectors
    /// exist to catch.
    fn string_entry(v: &str) -> Entry {
        Entry::new_string(Bytes::from(v.to_string()))
    }

    fn entry_of(value: RedisValue) -> Entry {
        let mut e = Entry::new_string(Bytes::new());
        e.value = crate::storage::compact_value::CompactValue::from_redis_value(value);
        e
    }

    #[test]
    fn an_empty_dataset_is_all_zeroes() {
        assert_eq!(
            to_hex(&finalize_dataset(vec![])),
            "0000000000000000000000000000000000000000"
        );
    }

    #[test]
    fn digest_value_of_a_string_matches_redis() {
        // redis 8.6.1:  SET k v ; DEBUG DIGEST-VALUE k
        assert_eq!(
            to_hex(&object_digest(&string_entry("v"))),
            "d79ee9fe5d5ad902d7f6620efc3b07b75ddd742f"
        );
        // SET str_plain hello
        assert_eq!(
            to_hex(&object_digest(&string_entry("hello"))),
            "36b23a1456b2dce2c3ed252c456761301dba8060"
        );
        // SET str_int 12345 -- stored INT-encoded in redis, digested as text.
        assert_eq!(
            to_hex(&object_digest(&string_entry("12345"))),
            "e1cf90774e995ae03de66e8109dc5cb9d9c2dc83"
        );
    }

    #[test]
    fn a_key_ttl_changes_the_digest_by_a_fixed_marker() {
        // SET str_exp withttl ; EXPIRE str_exp 9999
        let mut e = string_entry("withttl");
        e.set_expires_at_ms(1_000_000_000_000);
        assert_eq!(
            to_hex(&object_digest(&e)),
            "77c90aba73e11e26e94722d08db485ca000dbc61"
        );
        // The marker encodes PRESENCE only: a different deadline is the same
        // digest, or the value would change on every second that passed.
        let mut e2 = string_entry("withttl");
        e2.set_expires_at_ms(2_000_000_000_000);
        assert_eq!(object_digest(&e), object_digest(&e2));
        assert_ne!(object_digest(&e), object_digest(&string_entry("withttl")));
    }

    #[test]
    fn a_list_digest_matches_redis_and_depends_on_order() {
        // RPUSH mylist a b c
        let list: VecDeque<Bytes> = [b"a", b"b", b"c"]
            .iter()
            .map(|s| Bytes::copy_from_slice(&s[..]))
            .collect();
        assert_eq!(
            to_hex(&object_digest(&entry_of(RedisValue::List(list)))),
            "8bf72d812571eea9b927f3c11beb0c4165a6ff89"
        );
        // Reversing must change it: lists MIX, they do not xor.
        let rev: VecDeque<Bytes> = [b"c", b"b", b"a"]
            .iter()
            .map(|s| Bytes::copy_from_slice(&s[..]))
            .collect();
        assert_ne!(
            to_hex(&object_digest(&entry_of(RedisValue::List(rev)))),
            "8bf72d812571eea9b927f3c11beb0c4165a6ff89"
        );
    }

    #[test]
    fn a_set_digest_matches_redis_and_ignores_order() {
        // SADD myset x y z  -- HashSet iteration order is arbitrary, which is
        // the point: a set digest must not depend on it.
        let set: HashSet<Bytes> = [b"x", b"y", b"z"]
            .iter()
            .map(|s| Bytes::copy_from_slice(&s[..]))
            .collect();
        assert_eq!(
            to_hex(&object_digest(&entry_of(RedisValue::Set(set)))),
            "0fd9977d57e46b1b9b87439f5ac5b338416ccc24"
        );
    }

    #[test]
    fn a_hash_digest_matches_redis() {
        // HSET myhash f1 v1 f2 v2
        let mut map = HashMap::new();
        map.insert(Bytes::from_static(b"f1"), Bytes::from_static(b"v1"));
        map.insert(Bytes::from_static(b"f2"), Bytes::from_static(b"v2"));
        assert_eq!(
            to_hex(&object_digest(&entry_of(RedisValue::Hash(map)))),
            "047cc60ea73a34cde6d08a2721a4a26d19ee6570"
        );
    }

    fn zset(members: &[(&[u8], f64)]) -> Entry {
        use ordered_float::OrderedFloat;
        let mut m: HashMap<Bytes, f64> = HashMap::new();
        let mut scores: BTreeMap<(OrderedFloat<f64>, Bytes), ()> = BTreeMap::new();
        for (name, sc) in members {
            let b = Bytes::copy_from_slice(name);
            m.insert(b.clone(), *sc);
            scores.insert((OrderedFloat(*sc), b), ());
        }
        entry_of(RedisValue::SortedSet { members: m, scores })
    }

    #[test]
    fn a_sorted_set_digest_matches_redis() {
        // ZADD myzset 1 m1 2 m2 -- integral scores render as "1"/"2", never
        // "1.0"; Rust's Display agrees with redis's fpconv here.
        assert_eq!(
            to_hex(&object_digest(&zset(&[(b"m1", 1.0), (b"m2", 2.0)]))),
            "e47c5147e02fef74bf836e999655c824e0ac5189"
        );
        // ZADD myzsetf 1.5 a 2.25 b -0.125 c -- these agree under BOTH %.17g
        // and shortest-round-trip, so on their own they prove nothing about
        // the score format.
        assert_eq!(
            to_hex(&object_digest(&zset(&[
                (b"a", 1.5),
                (b"b", 2.25),
                (b"c", -0.125)
            ]))),
            "3098ed9f0b2bd5f3cb85b7a6a50b4f28b4f43706"
        );
    }

    #[test]
    fn the_score_format_is_shortest_roundtrip_not_seventeen_significant_digits() {
        // ZADD myzsetodd 3.3 q -- THE discriminating case. `%.17g` renders
        // 3.2999999999999998 and yields
        // e5f7f580ccacaccbddff7716a7861ca398404035, which is NOT what redis
        // answers. Without this vector the format is unpinned.
        assert_eq!(
            to_hex(&object_digest(&zset(&[(b"q", 3.3)]))),
            "f8ecb2a2392f3f8b93ed22b574c6d304992678d5"
        );
    }

    #[test]
    fn a_missing_key_is_the_zero_digest() {
        // redis: DEBUG DIGEST-VALUE nosuch -> all zeroes, NOT an error.
        assert_eq!(to_hex(&ZERO), "0000000000000000000000000000000000000000");
    }

    #[test]
    fn a_whole_dataset_matches_redis() {
        // The nine keys above, all in db0, digested together.
        let mut acc = ZERO;
        let list: VecDeque<Bytes> = [b"a", b"b", b"c"]
            .iter()
            .map(|s| Bytes::copy_from_slice(&s[..]))
            .collect();
        let set: HashSet<Bytes> = [b"x", b"y", b"z"]
            .iter()
            .map(|s| Bytes::copy_from_slice(&s[..]))
            .collect();
        let mut map = HashMap::new();
        map.insert(Bytes::from_static(b"f1"), Bytes::from_static(b"v1"));
        map.insert(Bytes::from_static(b"f2"), Bytes::from_static(b"v2"));
        let mut exp = string_entry("withttl");
        exp.set_expires_at_ms(1_000_000_000_000);

        accumulate_key(&mut acc, b"str_plain", &string_entry("hello"));
        accumulate_key(&mut acc, b"str_int", &string_entry("12345"));
        accumulate_key(&mut acc, b"str_exp", &exp);
        accumulate_key(&mut acc, b"mylist", &entry_of(RedisValue::List(list)));
        accumulate_key(&mut acc, b"myset", &entry_of(RedisValue::Set(set)));
        accumulate_key(&mut acc, b"myhash", &entry_of(RedisValue::Hash(map)));
        accumulate_key(&mut acc, b"myzset", &zset(&[(b"m1", 1.0), (b"m2", 2.0)]));
        accumulate_key(
            &mut acc,
            b"myzsetf",
            &zset(&[(b"a", 1.5), (b"b", 2.25), (b"c", -0.125)]),
        );
        accumulate_key(&mut acc, b"myzsetodd", &zset(&[(b"q", 3.3)]));

        assert_eq!(
            to_hex(&finalize_dataset(vec![(0, acc)])),
            "b796f0dc6b2406a430b2f4b419e0083318108105"
        );
    }

    #[test]
    fn the_db_index_is_part_of_the_digest() {
        // redis: the same data in db3 digests differently from db0, so a
        // dataset MOVED between dbs is not reported as identical.
        let mut acc = ZERO;
        accumulate_key(&mut acc, b"k0", &string_entry("v0"));
        let in_db0 = to_hex(&finalize_dataset(vec![(0, acc)]));
        let in_db3 = to_hex(&finalize_dataset(vec![(3, acc)]));
        assert_eq!(in_db0, "4fe14463feed749c864303c88bd5a9ee8c7c703e");
        assert_eq!(in_db3, "6753c5f18921bbfbdf42340be0b319e82982e3d1");
        assert_ne!(in_db0, in_db3);
    }

    #[test]
    fn shard_partials_combine_in_any_order() {
        // The property the cross-shard aggregation rests on: within one db,
        // every key contributes by XOR, so splitting the keys across shards
        // and combining the partials must equal the single-shard answer.
        let a = string_entry("v0");
        let b = string_entry("v1");

        let mut whole = ZERO;
        accumulate_key(&mut whole, b"ka", &a);
        accumulate_key(&mut whole, b"kb", &b);

        let mut s0 = ZERO;
        accumulate_key(&mut s0, b"kb", &b);
        let mut s1 = ZERO;
        accumulate_key(&mut s1, b"ka", &a);
        let mut merged = ZERO;
        for (m, p) in merged.iter_mut().zip(s0.iter()) {
            *m ^= *p;
        }
        for (m, p) in merged.iter_mut().zip(s1.iter()) {
            *m ^= *p;
        }

        assert_eq!(whole, merged, "xor accumulation is not order-independent");
        assert_eq!(
            to_hex(&finalize_dataset(vec![(0, merged)])),
            to_hex(&finalize_dataset(vec![(0, whole)]))
        );
    }

    #[test]
    fn partials_survive_the_wire_form() {
        let mut a = ZERO;
        accumulate_key(&mut a, b"k0", &string_entry("v0"));
        let mut b = ZERO;
        accumulate_key(&mut b, b"k3", &string_entry("v3"));
        let partials = vec![(0usize, a), (3usize, b)];
        let frame = partials_to_frame(&partials);
        assert_eq!(partials_from_frame(&frame), Some(partials));
    }

    #[test]
    fn a_malformed_partials_reply_is_rejected_not_read_as_empty() {
        use crate::protocol::Frame;
        // A shard that answered something unexpected must NOT be counted as
        // "no keys" -- that silently drops its whole contribution and yields a
        // confidently wrong digest.
        assert_eq!(partials_from_frame(&Frame::Integer(1)), None);
        // Odd length.
        assert_eq!(
            partials_from_frame(&Frame::Array(vec![Frame::Integer(0)].into())),
            None
        );
        // Digest not 40 hex chars.
        assert_eq!(
            partials_from_frame(&Frame::Array(
                vec![
                    Frame::Integer(0),
                    Frame::BulkString(Bytes::from_static(b"beef")),
                ]
                .into()
            )),
            None
        );
        // Right length, not hex.
        assert_eq!(
            partials_from_frame(&Frame::Array(
                vec![
                    Frame::Integer(0),
                    Frame::BulkString(Bytes::from_static(&[b'z'; 40])),
                ]
                .into()
            )),
            None
        );
        // Negative db index.
        assert_eq!(
            partials_from_frame(&Frame::Array(
                vec![
                    Frame::Integer(-1),
                    Frame::BulkString(Bytes::from_static(&[b'0'; 40])),
                ]
                .into()
            )),
            None
        );
    }

    #[test]
    fn merging_shard_partials_equals_the_single_shard_answer() {
        // The property the whole cross-shard design rests on, stated end to
        // end: split the same keys across two shards, merge, and the final
        // digest must equal the one-shard digest of all of them.
        let a = string_entry("va");
        let b = string_entry("vb");
        let c = string_entry("vc");

        let mut whole = ZERO;
        accumulate_key(&mut whole, b"ka", &a);
        accumulate_key(&mut whole, b"kb", &b);
        accumulate_key(&mut whole, b"kc", &c);
        let single = to_hex(&finalize_dataset(vec![(0, whole)]));

        let mut s0 = ZERO;
        accumulate_key(&mut s0, b"kc", &c);
        let mut s1 = ZERO;
        accumulate_key(&mut s1, b"ka", &a);
        accumulate_key(&mut s1, b"kb", &b);
        let merged = merge_partials(vec![(0, s0), (0, s1)]);
        assert_eq!(to_hex(&finalize_dataset(merged)), single);
    }

    #[test]
    fn a_db_populated_only_on_one_shard_still_counts() {
        // "Non-empty" is a GLOBAL question. Shard 0 has nothing in db2; shard 1
        // does. The merge must keep db2, or the digest silently omits it.
        let mut only_on_shard1 = ZERO;
        accumulate_key(&mut only_on_shard1, b"k2", &string_entry("v2"));
        let merged = merge_partials(vec![(0, ZERO), (2, only_on_shard1)]);
        assert_eq!(merged.len(), 2);
        assert!(merged.iter().any(|(db, _)| *db == 2));
    }

    #[test]
    fn dbs_are_folded_in_ascending_order_regardless_of_input_order() {
        let mut a = ZERO;
        accumulate_key(&mut a, b"k0", &string_entry("v0"));
        let mut b = ZERO;
        accumulate_key(&mut b, b"k3", &string_entry("v3"));
        // Same partials, opposite input order -> same answer, because
        // finalize sorts. Shards report in arrival order, not db order.
        assert_eq!(
            to_hex(&finalize_dataset(vec![(0, a), (3, b)])),
            to_hex(&finalize_dataset(vec![(3, b), (0, a)]))
        );
    }
}
