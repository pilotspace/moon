use bytes::Bytes;
use ordered_float::OrderedFloat;

use crate::protocol::Frame;
use crate::storage::Database;

use crate::command::helpers::{err_wrong_args, extract_bytes};

use super::{
    convert_distance, geohash_decode, geohash_encode, geohash_to_string, haversine_distance,
    parse_unit,
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
            members.insert(member, score);
            added += 1;
            changed += 1;
        }
    }

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
                        Frame::BulkString(Bytes::from(format!("{:.4}", lon))),
                        Frame::BulkString(Bytes::from(format!("{:.4}", lat))),
                    ]
                    .into(),
                )
            }
            None => Frame::Null,
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
    let (_matches, results) = geosearch_inner(db, args, false);
    results
}

/// GEOSEARCHSTORE destination source ...
pub fn geosearchstore(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("GEOSEARCHSTORE");
    }
    let dest = match extract_bytes(&args[0]) {
        Some(k) => Bytes::copy_from_slice(k),
        None => return err_wrong_args("GEOSEARCHSTORE"),
    };

    // Shift args so args[0] is now the source key
    let (matches, _) = geosearch_inner(db, &args[1..], true);

    if matches.is_empty() {
        db.remove(&dest);
        return Frame::Integer(0);
    }

    // Build a fresh sorted set from matches and store at dest
    let mut new_members = std::collections::HashMap::new();
    let mut new_tree = crate::storage::bptree::BPTree::new();
    for (member, _dist, _lon, _lat, score) in &matches {
        new_members.insert(member.clone(), *score);
        new_tree.insert(OrderedFloat(*score), member.clone());
    }
    let mut entry = crate::storage::entry::Entry::new_sorted_set_bptree();
    entry.value = crate::storage::compact_value::CompactValue::from_redis_value(
        crate::storage::entry::RedisValue::SortedSetBPTree {
            tree: new_tree,
            members: new_members,
        },
    );
    db.set(dest, entry);

    Frame::Integer(matches.len() as i64)
}

/// Returned by geosearch_inner: (member, dist_m, lon, lat, score)
type GeoMatch = (Bytes, f64, f64, f64, f64);

fn geosearch_inner(db: &mut Database, args: &[Frame], _store_mode: bool) -> (Vec<GeoMatch>, Frame) {
    if args.len() < 6 {
        return (Vec::new(), err_wrong_args("GEOSEARCH"));
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return (Vec::new(), err_wrong_args("GEOSEARCH")),
    };

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
                        Frame::Error(Bytes::from_static(b"ERR syntax error")),
                    );
                }
            };
            // Look up member's score
            let members_map = match db.get_sorted_set(key) {
                Ok(Some((members, _))) => members.clone(),
                Ok(None) => return (Vec::new(), Frame::Array(Vec::new().into())),
                Err(e) => return (Vec::new(), e),
            };
            match members_map.get(member) {
                Some(&score) => {
                    let (lon, lat) = geohash_decode(score);
                    center_lon = lon;
                    center_lat = lat;
                }
                None => return (Vec::new(), Frame::Array(Vec::new().into())),
            }
            found_from = true;
        } else if arg.eq_ignore_ascii_case(b"FROMLONLAT") {
            i += 1;
            center_lon = match args.get(i).and_then(|f| parse_f64(f)) {
                Some(v) => v,
                None => {
                    return (
                        Vec::new(),
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
    let mut output_unit_mult = 1.0f64; // for WITHDIST: convert meters → query unit

    let unit_err = || {
        (
            Vec::new(),
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
        } else if arg.eq_ignore_ascii_case(b"WITHCOORD") {
            withcoord = true;
        } else if arg.eq_ignore_ascii_case(b"WITHDIST") {
            withdist = true;
        } else if arg.eq_ignore_ascii_case(b"WITHHASH") {
            withhash = true;
        } else {
            return (
                Vec::new(),
                Frame::Error(Bytes::from_static(b"ERR syntax error")),
            );
        }
        i += 1;
    }

    if radius_m.is_none() && box_width_m.is_none() {
        return (
            Vec::new(),
            Frame::Error(Bytes::from_static(
                b"ERR exactly one of BYRADIUS and BYBOX arguments must be provided",
            )),
        );
    }

    // Get all members with their coordinates
    let members_map = match db.get_sorted_set(key) {
        Ok(Some((members, _))) => members.clone(),
        Ok(None) => return (Vec::new(), Frame::Array(Vec::new().into())),
        Err(e) => return (Vec::new(), e),
    };

    // Filter by shape
    let mut matches: Vec<(Bytes, f64, f64, f64, f64)> = Vec::new(); // (member, dist, lon, lat, score)
    for (member, &score) in &members_map {
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
                    entry.push(Frame::Array(
                        vec![
                            Frame::BulkString(Bytes::from(format!("{:.4}", lon))),
                            Frame::BulkString(Bytes::from(format!("{:.4}", lat))),
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

    (matches, Frame::Array(results.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Database;

    fn bs(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    #[test]
    fn test_geoadd_and_geopos() {
        let mut db = Database::new();
        let result = geoadd(
            &mut db,
            &[
                bs(b"mygeo"),
                bs(b"13.361389"),
                bs(b"38.115556"),
                bs(b"Palermo"),
                bs(b"15.087269"),
                bs(b"37.502669"),
                bs(b"Catania"),
            ],
        );
        assert_eq!(result, Frame::Integer(2));

        let result = geopos(&mut db, &[bs(b"mygeo"), bs(b"Palermo"), bs(b"NonExistent")]);
        match result {
            Frame::Array(ref arr) => {
                assert_eq!(arr.len(), 2);
                assert!(matches!(&arr[0], Frame::Array(_)));
                assert_eq!(arr[1], Frame::Null);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_geodist() {
        let mut db = Database::new();
        geoadd(
            &mut db,
            &[
                bs(b"mygeo"),
                bs(b"13.361389"),
                bs(b"38.115556"),
                bs(b"Palermo"),
                bs(b"15.087269"),
                bs(b"37.502669"),
                bs(b"Catania"),
            ],
        );
        let result = geodist(
            &mut db,
            &[bs(b"mygeo"), bs(b"Palermo"), bs(b"Catania"), bs(b"km")],
        );
        match result {
            Frame::BulkString(b) => {
                let dist: f64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
                assert!((dist - 166.2742).abs() < 1.0, "got {dist}");
            }
            _ => panic!("Expected bulk string"),
        }
    }

    #[test]
    fn test_geohash() {
        let mut db = Database::new();
        geoadd(
            &mut db,
            &[
                bs(b"mygeo"),
                bs(b"13.361389"),
                bs(b"38.115556"),
                bs(b"Palermo"),
            ],
        );
        let result = geohash(&mut db, &[bs(b"mygeo"), bs(b"Palermo")]);
        match result {
            Frame::Array(ref arr) => {
                assert_eq!(arr.len(), 1);
                match &arr[0] {
                    Frame::BulkString(b) => {
                        assert_eq!(b.len(), 11);
                    }
                    _ => panic!("Expected bulk string"),
                }
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_geosearch_byradius() {
        let mut db = Database::new();
        geoadd(
            &mut db,
            &[
                bs(b"mygeo"),
                bs(b"13.361389"),
                bs(b"38.115556"),
                bs(b"Palermo"),
                bs(b"15.087269"),
                bs(b"37.502669"),
                bs(b"Catania"),
                bs(b"2.349014"),
                bs(b"48.864716"),
                bs(b"Paris"),
            ],
        );

        let result = geosearch(
            &mut db,
            &[
                bs(b"mygeo"),
                bs(b"FROMLONLAT"),
                bs(b"15"),
                bs(b"37"),
                bs(b"BYRADIUS"),
                bs(b"200"),
                bs(b"km"),
                bs(b"ASC"),
            ],
        );
        match result {
            Frame::Array(ref arr) => {
                // Should find Catania and Palermo (within 200km of 15,37), not Paris
                assert_eq!(arr.len(), 2);
            }
            _ => panic!("Expected array, got {:?}", result),
        }
    }

    #[test]
    fn test_geoadd_nx_xx() {
        let mut db = Database::new();
        geoadd(
            &mut db,
            &[bs(b"g"), bs(b"10.0"), bs(b"20.0"), bs(b"member1")],
        );

        // NX should not update existing
        let result = geoadd(
            &mut db,
            &[
                bs(b"g"),
                bs(b"NX"),
                bs(b"11.0"),
                bs(b"21.0"),
                bs(b"member1"),
            ],
        );
        assert_eq!(result, Frame::Integer(0));

        // NX should add new
        let result = geoadd(
            &mut db,
            &[
                bs(b"g"),
                bs(b"NX"),
                bs(b"12.0"),
                bs(b"22.0"),
                bs(b"member2"),
            ],
        );
        assert_eq!(result, Frame::Integer(1));
    }
}
