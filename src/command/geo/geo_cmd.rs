use bytes::Bytes;
use ordered_float::OrderedFloat;

use crate::protocol::Frame;
use crate::storage::Database;

use crate::command::helpers::{err_wrong_args, extract_bytes};

use super::{
    convert_distance, fmt_geo_coord, geohash_decode, geohash_encode, geohash_to_string,
    haversine_distance, parse_unit,
};

fn parse_f64(frame: &Frame) -> Option<f64> {
    let b = extract_bytes(frame)?;
    std::str::from_utf8(b).ok()?.parse().ok()
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
        let lon = match parse_f64(&chunk[0]) {
            Some(v) if (-180.0..=180.0).contains(&v) => v,
            _ => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not a valid float or out of range",
                ));
            }
        };
        let lat = match parse_f64(&chunk[1]) {
            Some(v) if (-85.05112878..=85.05112878).contains(&v) => v,
            _ => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not a valid float or out of range",
                ));
            }
        };
        let member = match extract_bytes(&chunk[2]) {
            Some(m) => Bytes::copy_from_slice(m),
            None => return err_wrong_args("GEOADD"),
        };

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

/// GEOPOS key member [member ...]
pub fn geopos(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("GEOPOS");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("GEOPOS"),
    };

    // Collect scores first to avoid holding borrow across format! allocations
    let scores: Vec<Option<f64>> = {
        let members_map = match db.get_sorted_set(key) {
            Ok(Some((members, _))) => Some(members),
            Ok(None) => None,
            Err(e) => return e,
        };
        args[1..]
            .iter()
            .map(|arg| {
                let member = extract_bytes(arg)?;
                members_map.as_ref()?.get(member).copied()
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

/// GEODIST key member1 member2 [M|KM|FT|MI]
pub fn geodist(db: &mut Database, args: &[Frame]) -> Frame {
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

    let members_map = match db.get_sorted_set(key) {
        Ok(Some((members, _))) => members.clone(),
        Ok(None) => return Frame::Null,
        Err(e) => return e,
    };

    let score1 = match members_map.get(m1) {
        Some(&s) => s,
        None => return Frame::Null,
    };
    let score2 = match members_map.get(m2) {
        Some(&s) => s,
        None => return Frame::Null,
    };

    let (lon1, lat1) = geohash_decode(score1);
    let (lon2, lat2) = geohash_decode(score2);
    let dist = haversine_distance(lon1, lat1, lon2, lat2);
    let converted = convert_distance(dist, unit);

    Frame::BulkString(Bytes::from(format!("{:.4}", converted)))
}

/// GEOHASH key member [member ...]
pub fn geohash(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("GEOHASH");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("GEOHASH"),
    };

    let members_map = match db.get_sorted_set(key) {
        Ok(Some((members, _))) => Some(members.clone()),
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

        match &members_map {
            Some(m) => match m.get(member) {
                Some(&score) => {
                    let hash_str = geohash_to_string(score);
                    results.push(Frame::BulkString(Bytes::from(hash_str)));
                }
                None => results.push(Frame::Null),
            },
            None => results.push(Frame::Null),
        }
    }

    Frame::Array(results.into())
}

/// GEOSEARCH key FROMMEMBER member|FROMLONLAT lon lat
///   BYRADIUS radius M|KM|FT|MI|BYBOX width height M|KM|FT|MI
///   [ASC|DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]
pub fn geosearch(db: &mut Database, args: &[Frame]) -> Frame {
    let (_matches, _opts, results) = geosearch_inner(db, args, false);
    results
}

/// GEORADIUS key longitude latitude radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT n] [ASC|DESC]
///
/// Deprecated since Redis 6.2 — translates to GEOSEARCH internally.
pub fn georadius(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 5 {
        return err_wrong_args("GEORADIUS");
    }
    let (opts, store) = match split_store_clause(&args[5..]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    // Translate: GEORADIUS key lon lat radius unit [opts...]
    // → GEOSEARCH key FROMLONLAT lon lat BYRADIUS radius unit [opts...]
    let mut new_args = Vec::with_capacity(opts.len() + 7);
    new_args.push(args[0].clone()); // key
    new_args.push(Frame::BulkString(Bytes::from_static(b"FROMLONLAT")));
    new_args.push(args[1].clone()); // lon
    new_args.push(args[2].clone()); // lat
    new_args.push(Frame::BulkString(Bytes::from_static(b"BYRADIUS")));
    new_args.push(args[3].clone()); // radius
    new_args.push(args[4].clone()); // unit
    new_args.extend_from_slice(&opts); // remaining options, STORE clause removed
    run_geosearch(db, &new_args, store)
}

/// GEORADIUSBYMEMBER key member radius M|KM|FT|MI [opts...]
///
/// Deprecated since Redis 6.2 — translates to GEOSEARCH internally.
pub fn georadiusbymember(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 4 {
        return err_wrong_args("GEORADIUSBYMEMBER");
    }
    let (opts, store) = match split_store_clause(&args[4..]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut new_args = Vec::with_capacity(opts.len() + 6);
    new_args.push(args[0].clone()); // key
    new_args.push(Frame::BulkString(Bytes::from_static(b"FROMMEMBER")));
    new_args.push(args[1].clone()); // member
    new_args.push(Frame::BulkString(Bytes::from_static(b"BYRADIUS")));
    new_args.push(args[2].clone()); // radius
    new_args.push(args[3].clone()); // unit
    new_args.extend_from_slice(&opts); // remaining options, STORE clause removed
    run_geosearch(db, &new_args, store)
}

/// Reject GEORADIUS_RO/GEORADIUSBYMEMBER_RO args containing STORE/STOREDIST.
///
/// This is LOAD-BEARING, not cosmetic. Both `_RO` entry points delegate to
/// `georadius`/`georadiusbymember`, and since moon#645 those implement the
/// clause — so deleting this check turns a command declared read-only
/// (`flags: R`, routable to a replica, dispatched on the shared-lock read
/// path) into one that writes a key. `test_georadius_ro_rejects_store` pins
/// both halves: the error, and the destination staying absent.
///
/// The message is redis's own `ERR syntax error`: `_RO` simply has no STORE
/// clause in its grammar, and a client that matches on redis's text must see
/// redis's text. (moon used to answer a bespoke "does not support
/// STORE/STOREDIST" here, which additionally implied the writable forms did
/// support it back when they did not — the self-inconsistency moon#645 was
/// filed for.)
fn geo_ro_rejects(args: &[Frame]) -> Option<Frame> {
    for a in args.iter().skip(1) {
        if let Some(tok) = extract_bytes(a) {
            if tok.eq_ignore_ascii_case(b"STORE") || tok.eq_ignore_ascii_case(b"STOREDIST") {
                return Some(Frame::Error(Bytes::from_static(b"ERR syntax error")));
            }
        }
    }
    None
}

/// GEORADIUS_RO key longitude latitude radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT n] [ASC|DESC]
///
/// Read-only twin of GEORADIUS — rejects STORE/STOREDIST so it stays safely
/// routable to replicas. Used on the mutable dispatch track; delegates to
/// `georadius()` (itself a GEOSEARCH translation) once STORE is ruled out.
pub fn georadius_ro(db: &mut Database, args: &[Frame]) -> Frame {
    if let Some(e) = geo_ro_rejects(args) {
        return e;
    }
    georadius(db, args)
}

/// Read-only twin of `georadius_ro` for the `dispatch_read` fast path:
/// translates to GEOSEARCH args and calls `geosearch_readonly` (immutable
/// member-map access throughout).
pub fn georadius_ro_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 5 {
        return err_wrong_args("GEORADIUS_RO");
    }
    if let Some(e) = geo_ro_rejects(args) {
        return e;
    }
    let mut new_args = Vec::with_capacity(args.len() + 3);
    new_args.push(args[0].clone()); // key
    new_args.push(Frame::BulkString(Bytes::from_static(b"FROMLONLAT")));
    new_args.push(args[1].clone()); // lon
    new_args.push(args[2].clone()); // lat
    new_args.push(Frame::BulkString(Bytes::from_static(b"BYRADIUS")));
    new_args.push(args[3].clone()); // radius
    new_args.push(args[4].clone()); // unit
    new_args.extend_from_slice(&args[5..]); // remaining options
    geosearch_readonly(db, &new_args, now_ms)
}

/// GEORADIUSBYMEMBER_RO key member radius M|KM|FT|MI [opts...]
///
/// Read-only twin of GEORADIUSBYMEMBER — rejects STORE/STOREDIST so it stays
/// safely routable to replicas. Used on the mutable dispatch track;
/// delegates to `georadiusbymember()` once STORE is ruled out.
pub fn georadiusbymember_ro(db: &mut Database, args: &[Frame]) -> Frame {
    if let Some(e) = geo_ro_rejects(args) {
        return e;
    }
    georadiusbymember(db, args)
}

/// Read-only twin of `georadiusbymember_ro` for the `dispatch_read` fast
/// path: translates to GEOSEARCH args and calls `geosearch_readonly`.
pub fn georadiusbymember_ro_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 4 {
        return err_wrong_args("GEORADIUSBYMEMBER_RO");
    }
    if let Some(e) = geo_ro_rejects(args) {
        return e;
    }
    let mut new_args = Vec::with_capacity(args.len() + 3);
    new_args.push(args[0].clone()); // key
    new_args.push(Frame::BulkString(Bytes::from_static(b"FROMMEMBER")));
    new_args.push(args[1].clone()); // member
    new_args.push(Frame::BulkString(Bytes::from_static(b"BYRADIUS")));
    new_args.push(args[2].clone()); // radius
    new_args.push(args[3].clone()); // unit
    new_args.extend_from_slice(&args[4..]); // remaining options
    geosearch_readonly(db, &new_args, now_ms)
}

/// GEOSEARCHSTORE destination source ... [STOREDIST]
pub fn geosearchstore(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("GEOSEARCHSTORE");
    }
    let dest = match extract_bytes(&args[0]) {
        Some(k) => Bytes::copy_from_slice(k),
        None => return err_wrong_args("GEOSEARCHSTORE"),
    };

    // Shift args so args[0] is now the source key
    let (matches, opts, reply) = geosearch_inner(db, &args[1..], true);

    // A parse failure is an ERROR, not "nothing matched". Reading only the
    // (empty) match list answered `:0` AND deleted the destination — redis
    // reports the error and leaves the key alone (moon#645).
    if matches!(reply, Frame::Error(_)) {
        return reply;
    }

    store_geo_matches(db, dest, &matches, opts.unit_mult, opts.storedist)
}

/// The destination clause of the legacy `GEORADIUS*` forms: `STORE key` or
/// `STOREDIST key`.
struct StoreClause {
    dest: Bytes,
    /// `STOREDIST`: score each member by its distance in the query's unit
    /// rather than by the 52-bit geohash.
    by_distance: bool,
}

/// Run a translated GEOSEARCH and, if the legacy form carried one, apply its
/// destination clause. Without a clause the reply is the member array, with
/// one it is the stored count — exactly redis's split.
fn run_geosearch(db: &mut Database, args: &[Frame], store: Option<StoreClause>) -> Frame {
    let Some(clause) = store else {
        let (_matches, _opts, reply) = geosearch_inner(db, args, false);
        return reply;
    };
    let (matches, opts, reply) = geosearch_inner(db, args, false);
    if matches!(reply, Frame::Error(_)) {
        return reply;
    }
    store_geo_matches(
        db,
        clause.dest,
        &matches,
        opts.unit_mult,
        clause.by_distance,
    )
}

/// Write `matches` to `dest` as a fresh sorted set — the shared tail of
/// GEOSEARCHSTORE and of the legacy `STORE`/`STOREDIST` clause. An empty
/// match list deletes the destination and answers `:0`, as redis does.
fn store_geo_matches(
    db: &mut Database,
    dest: Bytes,
    matches: &[GeoMatch],
    unit_mult: f64,
    by_distance: bool,
) -> Frame {
    if matches.is_empty() {
        db.remove(&dest);
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
            tree: new_tree,
            members: new_members,
        },
    );
    db.set(&dest, entry);

    Frame::Integer(matches.len() as i64)
}

/// Split a legacy `GEORADIUS*` option tail into the options GEOSEARCH
/// understands and the optional destination clause.
///
/// The scan is grammar-aware, not a token search: `STORE` consumes the next
/// argv slot whatever it spells, so `STORE WITHDIST` names a destination key
/// called `WITHDIST` and must NOT trip the WITH* incompatibility check
/// (measured against redis-server 8.6.1, which answers `:2` there). When both
/// clauses appear the LAST one wins and the earlier destination is never
/// written.
fn split_store_clause(opts: &[Frame]) -> Result<(Vec<Frame>, Option<StoreClause>), Frame> {
    let mut kept = Vec::with_capacity(opts.len());
    let mut clause: Option<StoreClause> = None;
    let mut with_flag = false;
    let mut i = 0;
    while i < opts.len() {
        let Some(tok) = extract_bytes(&opts[i]) else {
            kept.push(opts[i].clone());
            i += 1;
            continue;
        };
        if tok.eq_ignore_ascii_case(b"STORE") || tok.eq_ignore_ascii_case(b"STOREDIST") {
            let by_distance = tok.eq_ignore_ascii_case(b"STOREDIST");
            let Some(dest) = opts.get(i + 1).and_then(extract_bytes) else {
                return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
            };
            clause = Some(StoreClause {
                dest: Bytes::copy_from_slice(dest),
                by_distance,
            });
            i += 2;
            continue;
        }
        if tok.eq_ignore_ascii_case(b"WITHCOORD")
            || tok.eq_ignore_ascii_case(b"WITHDIST")
            || tok.eq_ignore_ascii_case(b"WITHHASH")
        {
            with_flag = true;
        } else if tok.eq_ignore_ascii_case(b"COUNT") {
            // COUNT's value — and an optional ANY — are data, not options;
            // stepping over them keeps a count of, say, `1` from ever being
            // mistaken for a clause keyword.
            kept.push(opts[i].clone());
            i += 1;
            if let Some(v) = opts.get(i) {
                kept.push(v.clone());
                i += 1;
            }
            if let Some(any) = opts.get(i)
                && extract_bytes(any).is_some_and(|a| a.eq_ignore_ascii_case(b"ANY"))
            {
                kept.push(any.clone());
                i += 1;
            }
            continue;
        }
        kept.push(opts[i].clone());
        i += 1;
    }
    if clause.is_some() && with_flag {
        // Redis names the three flags in this fixed order, and says
        // "in GEORADIUS" even when the command was GEORADIUSBYMEMBER.
        return Err(Frame::Error(Bytes::from_static(
            b"ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
        )));
    }
    Ok((kept, clause))
}

/// Returned by geosearch_inner: (member, dist_m, lon, lat, score)
type GeoMatch = (Bytes, f64, f64, f64, f64);

/// What the option tail asked for, beyond the match list itself. The store
/// paths need both: `unit_mult` to turn the meters every match carries back
/// into the query's unit, and `storedist` for GEOSEARCHSTORE's bare flag.
#[derive(Clone, Copy)]
struct GeoOpts {
    /// Meters per unit of the query's BYRADIUS/BYBOX unit.
    unit_mult: f64,
    /// GEOSEARCHSTORE's `STOREDIST`: score by distance, not by geohash.
    storedist: bool,
}

impl Default for GeoOpts {
    fn default() -> Self {
        // 1.0 = meters, the identity for every `dist / unit_mult` below, so
        // an error return can never scale a distance by zero.
        Self {
            unit_mult: 1.0,
            storedist: false,
        }
    }
}

fn geosearch_inner(
    db: &mut Database,
    args: &[Frame],
    store_mode: bool,
) -> (Vec<GeoMatch>, GeoOpts, Frame) {
    if args.len() < 6 {
        return (Vec::new(), GeoOpts::default(), err_wrong_args("GEOSEARCH"));
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return (Vec::new(), GeoOpts::default(), err_wrong_args("GEOSEARCH")),
    };
    // Single up-front fetch (Redis also resolves the key object before
    // validating options). `get_sorted_set` lazy-expires — write-path
    // semantics unchanged; the parse/filter logic is shared with the
    // read-only twin via geosearch_core.
    let members_opt = match db.get_sorted_set(key) {
        Ok(Some((members, _))) => Some(members),
        Ok(None) => None,
        Err(e) => return (Vec::new(), GeoOpts::default(), e),
    };
    geosearch_core(members_opt, args, store_mode)
}

/// Shared GEOSEARCH parse + filter, independent of how the sorted set was
/// fetched (mutable lazy-expiring path or shared-lock read path). `args`
/// still has the key at index 0 — parsing starts at index 1. A missing key
/// (None) yields an empty array at exactly the points the old code fetched.
fn geosearch_core(
    members_opt: Option<&std::collections::HashMap<Bytes, f64>>,
    args: &[Frame],
    store_mode: bool,
) -> (Vec<GeoMatch>, GeoOpts, Frame) {
    // Parse source: FROMMEMBER or FROMLONLAT
    let mut center_lon = 0.0f64;
    let mut center_lat = 0.0f64;
    let mut i = 1;
    let mut found_from = false;

    while i < args.len() && !found_from {
        let arg = match extract_bytes(&args[i]) {
            Some(a) => a,
            None => {
                i += 1;
                continue;
            }
        };
        if arg.eq_ignore_ascii_case(b"FROMMEMBER") {
            i += 1;
            let member = match extract_bytes(args.get(i).unwrap_or(&Frame::Null)) {
                Some(m) => m,
                None => {
                    return (
                        Vec::new(),
                        GeoOpts::default(),
                        Frame::Error(Bytes::from_static(b"ERR syntax error")),
                    );
                }
            };
            // Look up member's score
            let members_map = match members_opt {
                Some(m) => m,
                None => {
                    return (
                        Vec::new(),
                        GeoOpts::default(),
                        Frame::Array(Vec::new().into()),
                    );
                }
            };
            match members_map.get(member) {
                Some(&score) => {
                    let (lon, lat) = geohash_decode(score);
                    center_lon = lon;
                    center_lat = lat;
                }
                None => {
                    return (
                        Vec::new(),
                        GeoOpts::default(),
                        Frame::Array(Vec::new().into()),
                    );
                }
            }
            found_from = true;
        } else if arg.eq_ignore_ascii_case(b"FROMLONLAT") {
            i += 1;
            center_lon = match args.get(i).and_then(|f| parse_f64(f)) {
                Some(v) => v,
                None => {
                    return (
                        Vec::new(),
                        GeoOpts::default(),
                        Frame::Error(Bytes::from_static(b"ERR syntax error")),
                    );
                }
            };
            i += 1;
            center_lat = match args.get(i).and_then(|f| parse_f64(f)) {
                Some(v) => v,
                None => {
                    return (
                        Vec::new(),
                        GeoOpts::default(),
                        Frame::Error(Bytes::from_static(b"ERR syntax error")),
                    );
                }
            };
            found_from = true;
        }
        i += 1;
    }

    if !found_from {
        return (
            Vec::new(),
            GeoOpts::default(),
            Frame::Error(Bytes::from_static(b"ERR syntax error")),
        );
    }

    // Parse shape: BYRADIUS or BYBOX
    let mut radius_m = None;
    let mut box_width_m = None;
    let mut box_height_m = None;
    let mut ascending = true;
    let mut count_limit = None;
    let mut withcoord = false;
    let mut withdist = false;
    let mut withhash = false;
    let mut storedist = false;
    let mut output_unit_mult = 1.0f64; // for WITHDIST: convert meters → query unit

    let unit_err = || {
        (
            Vec::new(),
            GeoOpts::default(),
            Frame::Error(Bytes::from_static(
                b"ERR unsupported unit provided. please use M, KM, FT, MI",
            )),
        )
    };

    while i < args.len() {
        let arg = match extract_bytes(&args[i]) {
            Some(a) => a,
            None => {
                i += 1;
                continue;
            }
        };
        if arg.eq_ignore_ascii_case(b"BYRADIUS") {
            if box_width_m.is_some() {
                return (
                    Vec::new(),
                    GeoOpts::default(),
                    Frame::Error(Bytes::from_static(
                        b"ERR exactly one of BYRADIUS and BYBOX arguments must be provided",
                    )),
                );
            }
            i += 1;
            let r = match args.get(i).and_then(|f| parse_f64(f)) {
                Some(v) => v,
                None => {
                    return (
                        Vec::new(),
                        GeoOpts::default(),
                        Frame::Error(Bytes::from_static(b"ERR syntax error")),
                    );
                }
            };
            i += 1;
            let unit_mult = match args
                .get(i)
                .and_then(|f| extract_bytes(f))
                .and_then(|b| parse_unit(b))
            {
                Some(v) => v,
                None => return unit_err(),
            };
            output_unit_mult = unit_mult;
            radius_m = Some(r * unit_mult);
        } else if arg.eq_ignore_ascii_case(b"BYBOX") {
            if radius_m.is_some() {
                return (
                    Vec::new(),
                    GeoOpts::default(),
                    Frame::Error(Bytes::from_static(
                        b"ERR exactly one of BYRADIUS and BYBOX arguments must be provided",
                    )),
                );
            }
            i += 1;
            let w = match args.get(i).and_then(|f| parse_f64(f)) {
                Some(v) => v,
                None => {
                    return (
                        Vec::new(),
                        GeoOpts::default(),
                        Frame::Error(Bytes::from_static(b"ERR syntax error")),
                    );
                }
            };
            i += 1;
            let h = match args.get(i).and_then(|f| parse_f64(f)) {
                Some(v) => v,
                None => {
                    return (
                        Vec::new(),
                        GeoOpts::default(),
                        Frame::Error(Bytes::from_static(b"ERR syntax error")),
                    );
                }
            };
            i += 1;
            let unit_mult = match args
                .get(i)
                .and_then(|f| extract_bytes(f))
                .and_then(|b| parse_unit(b))
            {
                Some(v) => v,
                None => return unit_err(),
            };
            output_unit_mult = unit_mult;
            box_width_m = Some(w * unit_mult);
            box_height_m = Some(h * unit_mult);
        } else if arg.eq_ignore_ascii_case(b"ASC") {
            ascending = true;
        } else if arg.eq_ignore_ascii_case(b"DESC") {
            ascending = false;
        } else if arg.eq_ignore_ascii_case(b"COUNT") {
            i += 1;
            let c = match args.get(i).and_then(|f| parse_f64(f)) {
                Some(v) if v > 0.0 => v as usize,
                _ => {
                    return (
                        Vec::new(),
                        GeoOpts::default(),
                        Frame::Error(Bytes::from_static(b"ERR syntax error")),
                    );
                }
            };
            count_limit = Some(c);
            // Skip optional ANY
            if i + 1 < args.len() {
                if let Some(next) = extract_bytes(&args[i + 1]) {
                    if next.eq_ignore_ascii_case(b"ANY") {
                        i += 1;
                    }
                }
            }
        } else if arg.eq_ignore_ascii_case(b"WITHCOORD")
            || arg.eq_ignore_ascii_case(b"WITHDIST")
            || arg.eq_ignore_ascii_case(b"WITHHASH")
        {
            // GEOSEARCHSTORE stores a sorted set, so it has nowhere to put
            // the extras and redis refuses them by name rather than quietly
            // dropping them (moon#645).
            if store_mode {
                return (
                    Vec::new(),
                    GeoOpts::default(),
                    Frame::Error(Bytes::from_static(
                        b"ERR GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
                    )),
                );
            }
            withcoord |= arg.eq_ignore_ascii_case(b"WITHCOORD");
            withdist |= arg.eq_ignore_ascii_case(b"WITHDIST");
            withhash |= arg.eq_ignore_ascii_case(b"WITHHASH");
        } else if store_mode && arg.eq_ignore_ascii_case(b"STOREDIST") {
            // GEOSEARCHSTORE's STOREDIST is a bare flag with no argument.
            // Plain GEOSEARCH has no such clause at all, so it stays a
            // syntax error there.
            storedist = true;
        } else {
            return (
                Vec::new(),
                GeoOpts::default(),
                Frame::Error(Bytes::from_static(b"ERR syntax error")),
            );
        }
        i += 1;
    }

    if radius_m.is_none() && box_width_m.is_none() {
        return (
            Vec::new(),
            GeoOpts::default(),
            Frame::Error(Bytes::from_static(
                b"ERR exactly one of BYRADIUS and BYBOX arguments must be provided",
            )),
        );
    }

    // Get all members with their coordinates
    let members_map = match members_opt {
        Some(m) => m,
        None => {
            return (
                Vec::new(),
                GeoOpts::default(),
                Frame::Array(Vec::new().into()),
            );
        }
    };

    // Filter by shape
    let mut matches: Vec<(Bytes, f64, f64, f64, f64)> = Vec::new(); // (member, dist, lon, lat, score)
    for (member, &score) in members_map {
        let (lon, lat) = geohash_decode(score);
        let dist = haversine_distance(center_lon, center_lat, lon, lat);

        let in_range = if let Some(r) = radius_m {
            dist <= r
        } else {
            // Box check: approximate using haversine
            let dx = haversine_distance(center_lon, center_lat, lon, center_lat);
            let dy = haversine_distance(center_lon, center_lat, center_lon, lat);
            dx <= box_width_m.unwrap_or(0.0) / 2.0 && dy <= box_height_m.unwrap_or(0.0) / 2.0
        };

        if in_range {
            matches.push((member.clone(), dist, lon, lat, score));
        }
    }

    // Sort by distance
    matches.sort_by(|a, b| {
        let cmp = a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal);
        if ascending { cmp } else { cmp.reverse() }
    });

    // Apply COUNT limit
    if let Some(limit) = count_limit {
        matches.truncate(limit);
    }

    let has_extras = withcoord || withdist || withhash;

    let results: Vec<Frame> = matches
        .iter()
        .map(|(member, dist, lon, lat, score)| {
            if has_extras {
                let mut entry = vec![Frame::BulkString(member.clone())];
                if withdist {
                    // Convert meters to the same unit used in BYRADIUS/BYBOX query
                    let dist_in_unit = dist / output_unit_mult;
                    entry.push(Frame::BulkString(Bytes::from(format!(
                        "{:.4}",
                        dist_in_unit
                    ))));
                }
                if withhash {
                    entry.push(Frame::Integer(*score as i64));
                }
                if withcoord {
                    // Full shortest-round-tripping decimal, exactly as GEOPOS
                    // prints it — Redis builds both through the same
                    // `addReplyHumanLongDouble` path. `{:.4}` here truncated
                    // WITHCOORD to ~11m of resolution on BOTH protocols
                    // (moon#568). WITHDIST above keeps `{:.4}`: that one really
                    // is `addReplyDoubleDistance` in Redis.
                    entry.push(Frame::Array(
                        vec![
                            Frame::BulkString(Bytes::from(fmt_geo_coord(*lon))),
                            Frame::BulkString(Bytes::from(fmt_geo_coord(*lat))),
                        ]
                        .into(),
                    ));
                }
                Frame::Array(entry.into())
            } else {
                Frame::BulkString(member.clone())
            }
        })
        .collect();

    (
        matches,
        GeoOpts {
            unit_mult: output_unit_mult,
            storedist,
        },
        Frame::Array(results.into()),
    )
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

    Frame::BulkString(Bytes::from(format!("{:.4}", converted)))
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
/// Shares the full parse/filter logic with the mutable path via
/// `geosearch_core`. The ref accessor handles every encoding (BPTree,
/// Listpack from RDB load, Legacy); BPTree/Legacy borrow their member map
/// (zero copy), listpacks materialize a small bounded map.
pub fn geosearch_readonly(db: &crate::storage::db::Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 6 {
        return err_wrong_args("GEOSEARCH");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("GEOSEARCH"),
    };
    let owned: std::collections::HashMap<Bytes, f64>;
    let members_opt = match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(zref)) => match zref.members_map() {
            Some(m) => Some(m),
            None => {
                owned = zref.entries_sorted().into_iter().collect();
                Some(&owned)
            }
        },
        Ok(None) => None,
        Err(e) => return e,
    };
    let (_matches, _opts, results) = geosearch_core(members_opt, args, false);
    results
}
