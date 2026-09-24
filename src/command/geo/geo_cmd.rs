use bytes::Bytes;
use ordered_float::OrderedFloat;

use crate::protocol::Frame;
use crate::storage::Database;

use crate::command::helpers::{err_wrong_args, extract_bytes};

use super::geo_search::{
    GeoForm, GeoMatch, GeoOpts, fmt_distance, geosearch_arity, geosearch_core,
};
use super::{
    convert_distance, fmt_geo_coord, geohash_decode, geohash_encode, geohash_to_string,
    haversine_distance, parse_unit,
};

fn parse_f64(frame: &Frame) -> Option<f64> {
    let b = extract_bytes(frame)?;
    std::str::from_utf8(b).ok()?.parse().ok()
}

/// The longitude range GEOADD accepts. Shared by the validation pre-pass and
/// the mutation loop so the two cannot drift apart — see `parse_geoadd_triple`.
const GEO_LON_MIN: f64 = -180.0;
const GEO_LON_MAX: f64 = 180.0;
/// The Web-Mercator latitude clamp, matching Redis's `GEO_LAT_MIN`/`MAX`.
const GEO_LAT_MIN: f64 = -85.05112878;
const GEO_LAT_MAX: f64 = 85.05112878;

const GEO_BAD_FLOAT: &[u8] = b"ERR value is not a valid float or out of range";

/// Parse one `longitude latitude member` triple of a `GEOADD`.
///
/// The single source of truth for what `GEOADD` accepts. Both the validation
/// pre-pass and the mutation loop call it, and that is load-bearing rather
/// than tidy: the moment the two disagree — the pre-pass accepting something
/// the loop then rejects — moon#814 returns, because the loop's error arms
/// return from inside the `table_before … charge_memory()` window and strand
/// the charge for every member already inserted.
#[inline]
fn parse_geoadd_triple(chunk: &[Frame]) -> Result<(f64, f64, &[u8]), Frame> {
    let [lon_arg, lat_arg, member_arg] = chunk else {
        // `chunks_exact(3)` yields nothing else; the guard in `geoadd` already
        // rejected a length that is not a multiple of three.
        return Err(err_wrong_args("GEOADD"));
    };
    let Some(lon) = parse_f64(lon_arg).filter(|v| (GEO_LON_MIN..=GEO_LON_MAX).contains(v)) else {
        return Err(Frame::Error(Bytes::from_static(GEO_BAD_FLOAT)));
    };
    let Some(lat) = parse_f64(lat_arg).filter(|v| (GEO_LAT_MIN..=GEO_LAT_MAX).contains(v)) else {
        return Err(Frame::Error(Bytes::from_static(GEO_BAD_FLOAT)));
    };
    let Some(member) = extract_bytes(member_arg) else {
        return Err(err_wrong_args("GEOADD"));
    };
    Ok((lon, lat, member))
}

/// GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]
pub fn geoadd(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 4 {
        return err_wrong_args("GEOADD");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("GEOADD"),
    };

    // Parse optional NX/XX/CH flags
    let mut nx = false;
    let mut xx = false;
    let mut ch = false;
    let mut i = 1;
    while i < args.len() {
        let arg = match extract_bytes(&args[i]) {
            Some(a) => a,
            None => break,
        };
        if arg.eq_ignore_ascii_case(b"NX") {
            nx = true;
            i += 1;
        } else if arg.eq_ignore_ascii_case(b"XX") {
            xx = true;
            i += 1;
        } else if arg.eq_ignore_ascii_case(b"CH") {
            ch = true;
            i += 1;
        } else {
            break;
        }
    }

    if nx && xx {
        return Frame::Error(Bytes::from_static(
            b"ERR XX and NX options at the same time are not compatible",
        ));
    }

    // Remaining args must be triples: longitude latitude member
    let remaining = &args[i..];
    if remaining.len() < 3 || !remaining.len().is_multiple_of(3) {
        return err_wrong_args("GEOADD");
    }

    // moon#814: validate EVERY triple BEFORE touching the keyspace — same
    // window, same consequence as ZADD. Returning from inside the mutation
    // loop below left members inserted with `mem_charge` never applied, and a
    // later delete credited back memory that was never charged, drifting
    // `used_memory` monotonically down. Validating first is also Redis parity:
    // the key is not created when the command errors.
    for chunk in remaining.chunks_exact(3) {
        if let Err(e) = parse_geoadd_triple(chunk) {
            return e;
        }
    }

    let (members, tree) = match db.get_or_create_sorted_set(key) {
        Ok(pair) => pair,
        Err(e) => return e,
    };

    let mut added = 0i64;
    let mut changed = 0i64;
    // moon#788: GEOADD grows a sorted set through a raw `&mut` and charged
    // NOTHING for it — the same WS6 hole HSET/LPUSH had, still open for the
    // geo family. A geo key could grow without limit under `--maxmemory`.
    let mut mem_charge: usize = 0;
    let table_before = crate::storage::db::zset_table_bytes(members, tree);

    for chunk in remaining.chunks_exact(3) {
        // Cannot fail: the pre-pass above validated every triple with this
        // exact function before the keyspace was touched. Kept as a real match
        // anyway — a bare `unwrap` here would be the one place the two passes
        // could silently diverge.
        let (lon, lat, member) = match parse_geoadd_triple(chunk) {
            Ok(parsed) => parsed,
            Err(e) => return e,
        };
        let member = Bytes::copy_from_slice(member);

        let score = geohash_encode(lon, lat);
        let exists = members.contains_key(&member);

        if nx && exists {
            continue;
        }
        if xx && !exists {
            continue;
        }

        if exists {
            let old_score = members[&member];
            if (old_score - score).abs() > f64::EPSILON {
                tree.remove(OrderedFloat(old_score), &member);
                tree.insert(OrderedFloat(score), member.clone());
                members.insert(member, score);
                changed += 1;
            }
        } else {
            tree.insert(OrderedFloat(score), member.clone());
            mem_charge += crate::storage::db::zset_member_cost(&member);
            members.insert(member, score);
            added += 1;
            changed += 1;
        }
    }
    let table_after = crate::storage::db::zset_table_bytes(members, tree);
    // `members`/`tree`'s borrow of `db` ends above.
    db.charge_memory(mem_charge);
    db.adjust_memory(table_before, table_after);

    Frame::Integer(if ch { changed } else { added })
}

// ---------------------------------------------------------------------------
// moon#1172 — the mutable dispatch path reads through the SHARED one
//
// GEOPOS / GEODIST / GEOHASH on `&mut Database` (cross-shard SPSC `Execute`,
// MULTI/EXEC, Lua) reached the zset through `get_sorted_set`, which flattens
// a <=128-member listpack geo set to the B+tree form for good, and GEODIST /
// GEOHASH then CLONED the whole member map to look up one or two members.
// The `_readonly` twins below borrow through `get_sorted_set_ref_if_alive`
// (every encoding, no conversion, no clone) — the same move moon#832/#928
// made for the set and zset families.
// ---------------------------------------------------------------------------

/// GEOPOS key member [member ...]
pub fn geopos(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    geopos_readonly(db, args, now_ms)
}

/// GEODIST key member1 member2 [M|KM|FT|MI]
pub fn geodist(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    geodist_readonly(db, args, now_ms)
}

/// GEOHASH key member [member ...]
pub fn geohash(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    geohash_readonly(db, args, now_ms)
}

/// GEOSEARCH key FROMMEMBER member|FROMLONLAT lon lat
///   BYRADIUS radius M|KM|FT|MI|BYBOX width height M|KM|FT|MI
///   [ASC|DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]
pub fn geosearch(db: &mut Database, args: &[Frame]) -> Frame {
    let (_matches, _opts, results) = geosearch_inner(db, args, GeoForm::Search);
    results
}

/// GEORADIUS key longitude latitude radius M|KM|FT|MI [WITHCOORD] [WITHDIST]
///   [WITHHASH] [COUNT n [ANY]] [ASC|DESC] [STORE key|STOREDIST key]
///
/// Deprecated since Redis 6.2. Parsed by its own grammar in `geosearch_core`
/// (redis's `RADIUS_COORDS`), then searched exactly as GEOSEARCH is.
pub fn georadius(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 5 {
        return err_wrong_args("GEORADIUS");
    }
    run_legacy(db, args, GeoForm::RadiusCoords { store: true })
}

/// GEORADIUSBYMEMBER key member radius M|KM|FT|MI [opts...] [STORE key|STOREDIST key]
///
/// Deprecated since Redis 6.2 (redis's `RADIUS_MEMBER`).
pub fn georadiusbymember(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 4 {
        return err_wrong_args("GEORADIUSBYMEMBER");
    }
    run_legacy(db, args, GeoForm::RadiusMember { store: true })
}

/// GEORADIUS_RO key longitude latitude radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT n] [ASC|DESC]
///
/// Read-only twin of GEORADIUS, on the mutable dispatch track. Its grammar has
/// no STORE/STOREDIST clause (`GeoForm::RadiusCoords { store: false }`), so
/// the keyword is redis's own `ERR syntax error` and the command can never
/// write — which is what keeps it safely routable to replicas (moon#645).
pub fn georadius_ro(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 5 {
        return err_wrong_args("GEORADIUS_RO");
    }
    run_legacy(db, args, GeoForm::RadiusCoords { store: false })
}

/// Read-only twin of `georadius_ro` for the `dispatch_read` fast path.
pub fn georadius_ro_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 5 {
        return err_wrong_args("GEORADIUS_RO");
    }
    geosearch_shared(db, args, now_ms, GeoForm::RadiusCoords { store: false })
}

/// GEORADIUSBYMEMBER_RO key member radius M|KM|FT|MI [opts...]
///
/// Read-only twin of GEORADIUSBYMEMBER, on the mutable dispatch track; no
/// STORE/STOREDIST clause, as for `georadius_ro`.
pub fn georadiusbymember_ro(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 4 {
        return err_wrong_args("GEORADIUSBYMEMBER_RO");
    }
    run_legacy(db, args, GeoForm::RadiusMember { store: false })
}

/// Read-only twin of `georadiusbymember_ro` for the `dispatch_read` fast path.
pub fn georadiusbymember_ro_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 4 {
        return err_wrong_args("GEORADIUSBYMEMBER_RO");
    }
    geosearch_shared(db, args, now_ms, GeoForm::RadiusMember { store: false })
}

/// GEOSEARCHSTORE destination source ... [STOREDIST]
pub fn geosearchstore(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("GEOSEARCHSTORE");
    }
    let Some(dest) = extract_bytes(&args[0]) else {
        return err_wrong_args("GEOSEARCHSTORE");
    };

    // Shift args so args[0] is now the source key
    let (matches, opts, reply) = geosearch_inner(db, &args[1..], GeoForm::SearchStore);

    // A parse failure is an ERROR, not "nothing matched". Reading only the
    // (empty) match list answered `:0` AND deleted the destination — redis
    // reports the error and leaves the key alone (moon#645).
    if matches!(reply, Frame::Error(_)) {
        return reply;
    }

    store_geo_matches(db, dest, &matches, opts.unit_mult, opts.storedist)
}

/// Run a legacy GEORADIUS* form and, if it carried a `STORE`/`STOREDIST`
/// clause, apply it. Without a clause the reply is the member array, with
/// one it is the stored count — exactly redis's split. A parse error is
/// reported and leaves the destination alone (moon#645).
fn run_legacy(db: &mut Database, args: &[Frame], form: GeoForm) -> Frame {
    let (matches, opts, reply) = geosearch_inner(db, args, form);
    if matches!(reply, Frame::Error(_)) {
        return reply;
    }
    match opts
        .store_dest
        .and_then(|i| args.get(i))
        .and_then(extract_bytes)
    {
        Some(dest) => store_geo_matches(db, dest, &matches, opts.unit_mult, opts.storedist),
        None => reply,
    }
}

/// Write `matches` to `dest` as a fresh sorted set — the shared tail of
/// GEOSEARCHSTORE and of the legacy `STORE`/`STOREDIST` clause. An empty
/// match list deletes the destination and answers `:0`, as redis does.
fn store_geo_matches(
    db: &mut Database,
    dest: &[u8],
    matches: &[GeoMatch],
    unit_mult: f64,
    by_distance: bool,
) -> Frame {
    if matches.is_empty() {
        db.remove(dest);
        return Frame::Integer(0);
    }

    // Build a fresh sorted set from matches and store at dest
    let mut new_members = std::collections::HashMap::with_capacity(matches.len());
    let mut new_tree = crate::storage::bptree::BPTree::new();
    for (member, dist, _lon, _lat, score) in matches {
        // STOREDIST scores are the distance expressed in the unit the query
        // used; `dist` is carried in meters throughout, as WITHDIST is.
        let stored = if by_distance {
            dist / unit_mult
        } else {
            *score
        };
        new_members.insert(member.clone(), stored);
        new_tree.insert(OrderedFloat(stored), member.clone());
    }
    let mut entry = crate::storage::entry::Entry::new_sorted_set_bptree();
    entry.value = crate::storage::compact_value::CompactValue::from_redis_value(
        crate::storage::entry::RedisValue::SortedSetBPTree {
            tree: Box::new(new_tree),
            members: Box::new(new_members),
        },
    );
    db.set(dest, entry);

    Frame::Integer(matches.len() as i64)
}

/// The mutable track's geo search: the same shared-borrow read as the
/// `_readonly` twins (moon#1172), then the parse + search in `geo_search`.
/// Returns the matches and options too, for the STORE paths.
fn geosearch_inner(
    db: &Database,
    args: &[Frame],
    form: GeoForm,
) -> (Vec<GeoMatch>, GeoOpts, Frame) {
    geosearch_at(db, args, db.now_ms(), form)
}

/// Every geo search's key fetch. The legacy forms' callers have checked their
/// arity; GEOSEARCH's is checked here.
fn geosearch_at(
    db: &Database,
    args: &[Frame],
    now_ms: u64,
    form: GeoForm,
) -> (Vec<GeoMatch>, GeoOpts, Frame) {
    if matches!(form, GeoForm::Search | GeoForm::SearchStore)
        && let Some(e) = geosearch_arity(args)
    {
        return (Vec::new(), GeoOpts::default(), e);
    }
    let Some(key) = args.first().and_then(extract_bytes) else {
        return (Vec::new(), GeoOpts::default(), err_wrong_args("GEOSEARCH"));
    };
    // Single up-front fetch: redis resolves the key object (and answers
    // WRONGTYPE) before it validates any option.
    let zref = match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(z) => z,
        Err(e) => return (Vec::new(), GeoOpts::default(), e),
    };
    geosearch_core(zref.as_ref(), args, form)
}

/// A read-only geo search on the shared-lock path: the reply only (these
/// forms have no store clause).
fn geosearch_shared(db: &Database, args: &[Frame], now_ms: u64, form: GeoForm) -> Frame {
    let (_matches, _opts, reply) = geosearch_at(db, args, now_ms, form);
    reply
}

// ---------------------------------------------------------------------------
// Read-only twins for the shared-lock (dispatch_read) path
// ---------------------------------------------------------------------------
//
// GEO data is stored as a sorted set: all twins use `get_sorted_set_ref_if_alive`.

/// GEOPOS key member [member …] — read-only twin.
pub fn geopos_readonly(db: &crate::storage::db::Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("GEOPOS");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("GEOPOS"),
    };

    // Ref accessor: handles every encoding (BPTree, Listpack from RDB load,
    // Legacy) — the BPTree-only accessor would treat a listpack zset as missing.
    let scores: Vec<Option<f64>> = {
        let zref = match db.get_sorted_set_ref_if_alive(key, now_ms) {
            Ok(Some(z)) => Some(z),
            Ok(None) => None,
            Err(e) => return e,
        };
        args[1..]
            .iter()
            .map(|arg| {
                let member = extract_bytes(arg)?;
                zref.as_ref()?.score(member)
            })
            .collect()
    };

    let results: Vec<Frame> = scores
        .into_iter()
        .map(|opt_score| match opt_score {
            Some(score) => {
                let (lon, lat) = geohash_decode(score);
                Frame::Array(
                    vec![
                        Frame::BulkString(Bytes::from(fmt_geo_coord(lon))),
                        Frame::BulkString(Bytes::from(fmt_geo_coord(lat))),
                    ]
                    .into(),
                )
            }
            // Null ARRAY, nested inside the outer array: `GEOPOS k absent` is
            // `*1\r\n*-1\r\n`. GEOHASH — same file, same command family —
            // answers `$-1` for the same miss, so the two must NOT be made to
            // agree (moon#482; both measured against redis-server 8.6.1).
            None => Frame::NullArray,
        })
        .collect();

    Frame::Array(results.into())
}

/// GEODIST key member1 member2 [M|KM|FT|MI] — read-only twin.
pub fn geodist_readonly(db: &crate::storage::db::Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 3 {
        return err_wrong_args("GEODIST");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("GEODIST"),
    };
    let m1 = match extract_bytes(&args[1]) {
        Some(m) => m,
        None => return err_wrong_args("GEODIST"),
    };
    let m2 = match extract_bytes(&args[2]) {
        Some(m) => m,
        None => return err_wrong_args("GEODIST"),
    };
    let unit = if args.len() >= 4 {
        match extract_bytes(&args[3]) {
            Some(u) => {
                if parse_unit(u).is_none() {
                    return Frame::Error(Bytes::from_static(
                        b"ERR unsupported unit provided. please use M, KM, FT, MI",
                    ));
                }
                u
            }
            None => b"m" as &[u8],
        }
    } else {
        b"m"
    };

    // Ref accessor: every encoding, borrowed lookups — no map clone.
    let zref = match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(z)) => z,
        Ok(None) => return Frame::Null,
        Err(e) => return e,
    };

    let score1 = match zref.score(m1) {
        Some(s) => s,
        None => return Frame::Null,
    };
    let score2 = match zref.score(m2) {
        Some(s) => s,
        None => return Frame::Null,
    };

    let (lon1, lat1) = geohash_decode(score1);
    let (lon2, lat2) = geohash_decode(score2);
    let dist = haversine_distance(lon1, lat1, lon2, lat2);
    let converted = convert_distance(dist, unit);

    // redis's `addReplyDoubleDistance` (`llrint(d * 1e4)`), shared with
    // GEOSEARCH WITHDIST (moon#1172).
    Frame::BulkString(fmt_distance(converted))
}

/// GEOHASH key member [member …] — read-only twin.
pub fn geohash_readonly(db: &crate::storage::db::Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("GEOHASH");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("GEOHASH"),
    };

    // Ref accessor: every encoding, borrowed lookups — no map clone.
    let zref = match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(z)) => Some(z),
        Ok(None) => None,
        Err(e) => return e,
    };

    let mut results = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        let member = match extract_bytes(arg) {
            Some(m) => m,
            None => {
                results.push(Frame::Null);
                continue;
            }
        };

        match zref.as_ref().and_then(|z| z.score(member)) {
            Some(score) => {
                let hash_str = geohash_to_string(score);
                results.push(Frame::BulkString(Bytes::from(hash_str)));
            }
            None => results.push(Frame::Null),
        }
    }

    Frame::Array(results.into())
}

/// GEOSEARCH key FROMMEMBER|FROMLONLAT … BYRADIUS|BYBOX … — read-only twin.
///
/// Shares the whole parse + neighbour-cell search with the mutable path via
/// `geo_search::geosearch_core` (moon#1172). The ref accessor handles every
/// encoding without converting it.
pub fn geosearch_readonly(db: &crate::storage::db::Database, args: &[Frame], now_ms: u64) -> Frame {
    geosearch_shared(db, args, now_ms, GeoForm::Search)
}
