//! GEOSEARCH / GEORADIUS* by geohash neighbour cells (moon#1172).
//!
//! `geosearch_core` used to decode and run haversine on EVERY member of the
//! zset (2-3 haversines per member for BYBOX) and sort every match before
//! COUNT truncated — 20.9 ms on 200K points, 248x redis 7.0.15.
//!
//! This is a port of redis 7.0.15's search (`geo.c` / `geohash.c` /
//! `geohash_helper.c`), operation for operation, so the answer is redis's —
//! not merely an equivalent one:
//!
//! 1. `areas_by_shape` = `geohashCalculateAreasByShapeWGS84`: estimate the
//!    geohash step from the radius, encode the centre, take its 8
//!    neighbours, step down once if the box pokes out of the cell ring, and
//!    zero the neighbours the bounding box cannot reach.
//! 2. `members_of_all_neighbors` = `membersOfAllNeighbors`: for each of the
//!    9 cells in redis's order (centre, N, S, E, W, NE, NW, SE, SW), skipping
//!    zeroed cells and a cell equal to the previously processed one, walk the
//!    zset's `[min, max)` score range — one `BPTree` order-statistic seek,
//!    then the leaf chain — in ascending (score, member) order, the
//!    skiplist's order. Only those candidates are decoded and tested.
//! 3. `within_shape` = `geoWithinShape`: `geohashGetDistanceIfInRadius` /
//!    `geohashGetDistanceIfInRectangle` with redis's exact distance formula
//!    (`geo_distance`) — BYBOX measures the longitude leg at the POINT's
//!    latitude, which the old per-member check did not.
//! 4. `COUNT n ANY` stops the walk at `n` matches (redis's `limit`); `COUNT`
//!    without `ANY` forces ASC (redis does exactly that), and a partial sort
//!    uses `select_nth_unstable_by` before sorting only the kept prefix.
//!    Without ASC/DESC/COUNT the reply keeps the walk order, as redis's does
//!    (moon used to sort ASC regardless).
//! 5. WITHDIST is `fixedpoint_d2string(d, 4)` (`llrint(d * 1e4)`).
//!
//! Only the candidate set changes the cost: O(K + log N) for K points in the
//! 9 cells instead of O(N).

use bytes::Bytes;

use crate::command::helpers::{err_wrong_args, extract_bytes};
use crate::protocol::Frame;
use crate::storage::bptree::BPTree;
use crate::storage::db::SortedSetRef;

use super::{deinterleave_even, fmt_geo_coord, geohash_decode, interleave64, parse_unit};

const GEO_LAT_MIN: f64 = -85.05112878;
const GEO_LAT_MAX: f64 = 85.05112878;
const GEO_LONG_MIN: f64 = -180.0;
const GEO_LONG_MAX: f64 = 180.0;

/// `#define D_R (M_PI / 180.0)` — ONE constant, multiplied in. `x * PI /
/// 180.0` rounds differently in the last bit.
const D_R: f64 = std::f64::consts::PI / 180.0;
const EARTH_RADIUS_IN_METERS: f64 = 6372797.560856;
const MERCATOR_MAX: f64 = 20037726.37;

#[inline]
fn deg_rad(ang: f64) -> f64 {
    ang * D_R
}

#[inline]
fn rad_deg(ang: f64) -> f64 {
    ang / D_R
}

/// `geohashGetLatDistance`.
#[inline]
pub(crate) fn geo_lat_distance(lat1d: f64, lat2d: f64) -> f64 {
    EARTH_RADIUS_IN_METERS * (deg_rad(lat2d) - deg_rad(lat1d)).abs()
}

/// `geohashGetDistance` — redis 7.0+ (unchanged through 8.x), including the
/// same-longitude shortcut to the latitude distance.
pub(crate) fn geo_distance(lon1d: f64, lat1d: f64, lon2d: f64, lat2d: f64) -> f64 {
    let lon1r = deg_rad(lon1d);
    let lon2r = deg_rad(lon2d);
    let v = ((lon2r - lon1r) / 2.0).sin();
    // if v == 0 we can avoid doing expensive math when lons are practically the same
    if v == 0.0 {
        return geo_lat_distance(lat1d, lat2d);
    }
    let lat1r = deg_rad(lat1d);
    let lat2r = deg_rad(lat2d);
    let u = ((lat2r - lat1r) / 2.0).sin();
    let a = u * u + lat1r.cos() * lat2r.cos() * v * v;
    2.0 * EARTH_RADIUS_IN_METERS * a.sqrt().asin()
}

/// `fixedpoint_d2string(d, 4)` — `addReplyDoubleDistance`, used by WITHDIST
/// and GEODIST: `llrint(d * 10000)` (round half to even, the default
/// rounding mode), then the integer printed with four implied decimals.
/// `{:.4}` rounds the exact binary value instead and can differ on a tie.
pub(crate) fn fmt_distance(d: f64) -> Bytes {
    if d == 0.0 {
        return Bytes::from_static(b"0.0000");
    }
    let scaled = (d * 10000.0).round_ties_even();
    // `llrint` saturates like this on overflow in practice; distances on
    // Earth are nowhere near it.
    let svalue = scaled as i64;
    let mut out = Vec::with_capacity(24);
    if svalue < 0 {
        out.push(b'-');
    }
    let value = svalue.unsigned_abs();
    let int_part = value / 10000;
    let frac = value % 10000;
    let mut buf = itoa::Buffer::new();
    out.extend_from_slice(buf.format(int_part).as_bytes());
    out.push(b'.');
    let f = buf.format(frac);
    out.extend(std::iter::repeat_n(b'0', 4 - f.len()));
    out.extend_from_slice(f.as_bytes());
    Bytes::from(out)
}

// ---------------------------------------------------------------------------
// geohash.c
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HashBits {
    bits: u64,
    step: u8,
}

impl HashBits {
    const ZERO: HashBits = HashBits { bits: 0, step: 0 };

    /// `HASHISZERO`
    fn is_zero(self) -> bool {
        self.bits == 0 && self.step == 0
    }
}

#[derive(Debug, Clone, Copy)]
struct Area {
    lon_min: f64,
    lon_max: f64,
    lat_min: f64,
    lat_max: f64,
}

/// `geohashEncode` over the WGS84 ranges. The caller has validated the
/// coordinates (redis's `extractLongLatOrReply`).
fn encode(lon: f64, lat: f64, step: u8) -> HashBits {
    let lat_offset = (lat - GEO_LAT_MIN) / (GEO_LAT_MAX - GEO_LAT_MIN) * (1u64 << step) as f64;
    let long_offset = (lon - GEO_LONG_MIN) / (GEO_LONG_MAX - GEO_LONG_MIN) * (1u64 << step) as f64;
    HashBits {
        bits: interleave64(lat_offset as u32, long_offset as u32),
        step,
    }
}

/// `geohashDecode` over the WGS84 ranges.
fn decode_area(hash: HashBits) -> Area {
    let ilato = deinterleave_even(hash.bits) as f64;
    let ilono = deinterleave_even(hash.bits >> 1) as f64;
    let scale = (1u64 << hash.step) as f64;
    let lat_scale = GEO_LAT_MAX - GEO_LAT_MIN;
    let long_scale = GEO_LONG_MAX - GEO_LONG_MIN;
    Area {
        lat_min: GEO_LAT_MIN + (ilato * 1.0 / scale) * lat_scale,
        lat_max: GEO_LAT_MIN + ((ilato + 1.0) * 1.0 / scale) * lat_scale,
        lon_min: GEO_LONG_MIN + (ilono * 1.0 / scale) * long_scale,
        lon_max: GEO_LONG_MIN + ((ilono + 1.0) * 1.0 / scale) * long_scale,
    }
}

/// `geohash_move_x`
fn move_x(hash: &mut HashBits, d: i8) {
    if d == 0 {
        return;
    }
    let mut x = hash.bits & 0xaaaa_aaaa_aaaa_aaaa;
    let y = hash.bits & 0x5555_5555_5555_5555;
    let zz = 0x5555_5555_5555_5555u64 >> (64 - hash.step as u32 * 2);
    if d > 0 {
        x = x.wrapping_add(zz + 1);
    } else {
        x |= zz;
        x = x.wrapping_sub(zz + 1);
    }
    x &= 0xaaaa_aaaa_aaaa_aaaau64 >> (64 - hash.step as u32 * 2);
    hash.bits = x | y;
}

/// `geohash_move_y`
fn move_y(hash: &mut HashBits, d: i8) {
    if d == 0 {
        return;
    }
    let x = hash.bits & 0xaaaa_aaaa_aaaa_aaaa;
    let mut y = hash.bits & 0x5555_5555_5555_5555;
    let zz = 0xaaaa_aaaa_aaaa_aaaau64 >> (64 - hash.step as u32 * 2);
    if d > 0 {
        y = y.wrapping_add(zz + 1);
    } else {
        y |= zz;
        y = y.wrapping_sub(zz + 1);
    }
    y &= 0x5555_5555_5555_5555u64 >> (64 - hash.step as u32 * 2);
    hash.bits = x | y;
}

fn moved(hash: HashBits, dx: i8, dy: i8) -> HashBits {
    let mut h = hash;
    move_x(&mut h, dx);
    move_y(&mut h, dy);
    h
}

/// The 9 cells in `membersOfAllNeighbors`' order: centre, north, south,
/// east, west, north-east, north-west, south-east, south-west.
type Cells = [HashBits; 9];

// ---------------------------------------------------------------------------
// geohash_helper.c
// ---------------------------------------------------------------------------

/// `geohashEstimateStepsByRadius`
fn estimate_steps_by_radius(range_meters: f64, lat: f64) -> u8 {
    if range_meters == 0.0 {
        return 26;
    }
    let mut range = range_meters;
    let mut step: i32 = 1;
    while range < MERCATOR_MAX {
        range *= 2.0;
        step += 1;
    }
    step -= 2; // Make sure range is included in most of the base cases.

    // Wider range towards the poles.
    if !(-66.0..=66.0).contains(&lat) {
        step -= 1;
        if !(-80.0..=80.0).contains(&lat) {
            step -= 1;
        }
    }
    step.clamp(1, 26) as u8
}

/// What is searched: a radius or an axis-aligned box, in the query's unit,
/// with `conversion` meters per unit — kept apart exactly as redis's
/// `GeoShape` keeps them, because the step estimate and the bounding box
/// multiply in a different order than the membership tests.
#[derive(Debug, Clone, Copy)]
pub(super) enum ShapeKind {
    Radius(f64),
    Box { width: f64, height: f64 },
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Shape {
    pub(super) lon: f64,
    pub(super) lat: f64,
    pub(super) kind: ShapeKind,
    pub(super) conversion: f64,
}

/// `geohashBoundingBox` -> `[min_lon, min_lat, max_lon, max_lat]`.
fn bounding_box(shape: &Shape) -> [f64; 4] {
    let (longitude, latitude) = (shape.lon, shape.lat);
    let (height, width) = match shape.kind {
        ShapeKind::Radius(r) => (shape.conversion * r, shape.conversion * r),
        ShapeKind::Box { width, height } => (
            shape.conversion * (height / 2.0),
            shape.conversion * (width / 2.0),
        ),
    };
    let lat_delta = rad_deg(height / EARTH_RADIUS_IN_METERS);
    let long_delta_top =
        rad_deg(width / EARTH_RADIUS_IN_METERS / deg_rad(latitude + lat_delta).cos());
    let long_delta_bottom =
        rad_deg(width / EARTH_RADIUS_IN_METERS / deg_rad(latitude - lat_delta).cos());
    let southern = latitude < 0.0;
    [
        if southern {
            longitude - long_delta_bottom
        } else {
            longitude - long_delta_top
        },
        latitude - lat_delta,
        if southern {
            longitude + long_delta_bottom
        } else {
            longitude + long_delta_top
        },
        latitude + lat_delta,
    ]
}

/// `geohashCalculateAreasByShapeWGS84`
fn areas_by_shape(shape: &Shape) -> Cells {
    let [min_lon, min_lat, max_lon, max_lat] = bounding_box(shape);
    let radius_meters = match shape.kind {
        ShapeKind::Radius(r) => r,
        ShapeKind::Box { width, height } => {
            ((width / 2.0) * (width / 2.0) + (height / 2.0) * (height / 2.0)).sqrt()
        }
    } * shape.conversion;

    let mut steps = estimate_steps_by_radius(radius_meters, shape.lat);
    let mut hash = encode(shape.lon, shape.lat, steps);
    let mut area = decode_area(hash);

    // Step down once if a N/S/E/W neighbour does not reach the box's edge.
    let north = decode_area(moved(hash, 0, 1));
    let south = decode_area(moved(hash, 0, -1));
    let east = decode_area(moved(hash, 1, 0));
    let west = decode_area(moved(hash, -1, 0));
    let decrease_step = north.lat_max < max_lat
        || south.lat_min > min_lat
        || east.lon_max < max_lon
        || west.lon_min > min_lon;
    if steps > 1 && decrease_step {
        steps -= 1;
        hash = encode(shape.lon, shape.lat, steps);
        area = decode_area(hash);
    }

    let mut cells: Cells = [
        hash,
        moved(hash, 0, 1),   // north
        moved(hash, 0, -1),  // south
        moved(hash, 1, 0),   // east
        moved(hash, -1, 0),  // west
        moved(hash, 1, 1),   // north_east
        moved(hash, -1, 1),  // north_west
        moved(hash, 1, -1),  // south_east
        moved(hash, -1, -1), // south_west
    ];
    // Exclude the search areas that are useless.
    if steps >= 2 {
        if area.lat_min < min_lat {
            for i in [2, 8, 7] {
                cells[i] = HashBits::ZERO; // south, south_west, south_east
            }
        }
        if area.lat_max > max_lat {
            for i in [1, 5, 6] {
                cells[i] = HashBits::ZERO; // north, north_east, north_west
            }
        }
        if area.lon_min < min_lon {
            for i in [4, 8, 6] {
                cells[i] = HashBits::ZERO; // west, south_west, north_west
            }
        }
        if area.lon_max > max_lon {
            for i in [3, 7, 5] {
                cells[i] = HashBits::ZERO; // east, south_east, north_east
            }
        }
    }
    cells
}

/// `geohashAlign52Bits`
fn align52(hash: HashBits) -> u64 {
    hash.bits << (52 - hash.step as u32 * 2)
}

// ---------------------------------------------------------------------------
// geo.c
// ---------------------------------------------------------------------------

/// A point that passed the shape test: (member, dist_m, lon, lat, score).
pub(super) type GeoMatch = (Bytes, f64, f64, f64, f64);

/// The zset's scores in ascending (score, member) order — the tree's own
/// order statistics, or a sorted copy for a listpack (bounded by
/// `zset-max-listpack-entries`) or a cold-tier decode.
enum ScoreIndex<'a> {
    Tree(&'a BPTree),
    Sorted(Vec<(Bytes, f64)>),
}

impl<'a> ScoreIndex<'a> {
    fn of(zref: &'a SortedSetRef<'_>) -> Self {
        match zref.any_tree() {
            Some(t) => ScoreIndex::Tree(t),
            None => ScoreIndex::Sorted(zref.entries_sorted()),
        }
    }

    /// Visit the entries with `min <= score < max` in ascending order until
    /// `visit` returns false — `geoGetPointsInRange`'s zset walk.
    fn walk_range(&self, min: f64, max: f64, mut visit: impl FnMut(&Bytes, f64) -> bool) {
        match self {
            ScoreIndex::Tree(t) => {
                let start = t.count_while(|s, _| s < min);
                for (s, m) in t.iter_from_rank(start) {
                    if s.0 >= max || !visit(m, s.0) {
                        return;
                    }
                }
            }
            ScoreIndex::Sorted(v) => {
                let start = v.partition_point(|(_, s)| *s < min);
                for (m, s) in &v[start..] {
                    if *s >= max || !visit(m, *s) {
                        return;
                    }
                }
            }
        }
    }
}

/// `geoWithinShape`: the distance from the centre when the point is inside.
fn within_shape(shape: &Shape, lon: f64, lat: f64) -> Option<f64> {
    match shape.kind {
        ShapeKind::Radius(r) => {
            let d = geo_distance(shape.lon, shape.lat, lon, lat);
            (d <= r * shape.conversion).then_some(d)
        }
        ShapeKind::Box { width, height } => {
            // `geohashGetDistanceIfInRectangle`: latitude leg first (cheaper),
            // longitude leg at the POINT's latitude.
            if geo_lat_distance(lat, shape.lat) > height * shape.conversion / 2.0 {
                return None;
            }
            if geo_distance(lon, lat, shape.lon, lat) > width * shape.conversion / 2.0 {
                return None;
            }
            Some(geo_distance(shape.lon, shape.lat, lon, lat))
        }
    }
}

/// `membersOfAllNeighbors`: collect the matches of every useful cell, in
/// redis's cell order, stopping at `limit` matches when `limit > 0` (ANY).
fn members_of_all_neighbors(
    index: &ScoreIndex<'_>,
    cells: &Cells,
    shape: &Shape,
    limit: usize,
) -> Vec<GeoMatch> {
    let mut out: Vec<GeoMatch> = Vec::new();
    let mut last_processed = 0usize;
    for (i, cell) in cells.iter().enumerate() {
        if cell.is_zero() {
            continue;
        }
        // A huge radius can make adjacent neighbours the same cell.
        if last_processed != 0 && *cell == cells[last_processed] {
            continue;
        }
        if !out.is_empty() && limit != 0 && out.len() >= limit {
            break;
        }
        let min = align52(*cell) as f64;
        let max = align52(HashBits {
            bits: cell.bits + 1,
            step: cell.step,
        }) as f64;
        index.walk_range(min, max, |member, score| {
            let (lon, lat) = geohash_decode(score);
            if let Some(dist) = within_shape(shape, lon, lat) {
                out.push((member.clone(), dist, lon, lat, score));
            }
            !(!out.is_empty() && limit != 0 && out.len() >= limit)
        });
        last_processed = i;
    }
    out
}

/// What the option tail asked for, beyond the match list itself. The store
/// paths need both: `unit_mult` to turn the meters every match carries back
/// into the query's unit, and `storedist` for GEOSEARCHSTORE's bare flag.
#[derive(Clone, Copy)]
pub(super) struct GeoOpts {
    /// Meters per unit of the query's BYRADIUS/BYBOX unit.
    pub(super) unit_mult: f64,
    /// GEOSEARCHSTORE's `STOREDIST`: score by distance, not by geohash.
    pub(super) storedist: bool,
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum Sort {
    None,
    Asc,
    Desc,
}

fn parse_f64(frame: &Frame) -> Option<f64> {
    let b = extract_bytes(frame)?;
    std::str::from_utf8(b).ok()?.parse().ok()
}

fn fail(msg: &'static [u8]) -> (Vec<GeoMatch>, GeoOpts, Frame) {
    (
        Vec::new(),
        GeoOpts::default(),
        Frame::Error(Bytes::from_static(msg)),
    )
}

fn empty() -> (Vec<GeoMatch>, GeoOpts, Frame) {
    (
        Vec::new(),
        GeoOpts::default(),
        Frame::Array(Vec::new().into()),
    )
}

/// `-ERR invalid longitude,latitude pair %f,%f` (redis's
/// `extractLongLatOrReply`). `%f` is six fixed decimals.
fn invalid_lonlat(lon: f64, lat: f64) -> (Vec<GeoMatch>, GeoOpts, Frame) {
    use std::io::Write;
    let mut msg = Vec::with_capacity(64);
    // Writing into a Vec cannot fail.
    let _ = write!(msg, "ERR invalid longitude,latitude pair {lon:.6},{lat:.6}");
    (
        Vec::new(),
        GeoOpts::default(),
        Frame::Error(Bytes::from(msg)),
    )
}

/// Shared GEOSEARCH parse + search, independent of how the sorted set was
/// fetched — every dispatch path reads it through `get_sorted_set_ref_if_
/// alive` (moon#1172: the mutable path used `get_sorted_set`, which
/// flattened a listpack geo set for good). `args` still has the key at index
/// 0 — parsing starts at index 1. A missing key (`None`) yields an empty
/// array at exactly the points the old code answered one.
pub(super) fn geosearch_core(
    zref: Option<&SortedSetRef<'_>>,
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
                None => return fail(b"ERR syntax error"),
            };
            let Some(zref) = zref else {
                return empty();
            };
            match zref.score(member) {
                Some(score) => {
                    let (lon, lat) = geohash_decode(score);
                    center_lon = lon;
                    center_lat = lat;
                }
                None => return empty(),
            }
            found_from = true;
        } else if arg.eq_ignore_ascii_case(b"FROMLONLAT") {
            i += 1;
            center_lon = match args.get(i).and_then(parse_f64) {
                Some(v) => v,
                None => return fail(b"ERR syntax error"),
            };
            i += 1;
            center_lat = match args.get(i).and_then(parse_f64) {
                Some(v) => v,
                None => return fail(b"ERR syntax error"),
            };
            // The cell search needs an encodable centre, and redis refuses
            // one that is not (`extractLongLatOrReply`).
            if !(GEO_LONG_MIN..=GEO_LONG_MAX).contains(&center_lon)
                || !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&center_lat)
            {
                return invalid_lonlat(center_lon, center_lat);
            }
            found_from = true;
        }
        i += 1;
    }

    if !found_from {
        return fail(b"ERR syntax error");
    }

    // Parse shape: BYRADIUS or BYBOX
    let mut kind: Option<ShapeKind> = None;
    let mut conversion = 1.0f64;
    let mut sort = Sort::None;
    let mut count: Option<usize> = None;
    let mut any = false;
    let mut withcoord = false;
    let mut withdist = false;
    let mut withhash = false;
    let mut storedist = false;

    const UNIT_ERR: &[u8] = b"ERR unsupported unit provided. please use M, KM, FT, MI";
    const EXACTLY_ONE: &[u8] = b"ERR exactly one of BYRADIUS and BYBOX arguments must be provided";

    while i < args.len() {
        let arg = match extract_bytes(&args[i]) {
            Some(a) => a,
            None => {
                i += 1;
                continue;
            }
        };
        if arg.eq_ignore_ascii_case(b"BYRADIUS") {
            if matches!(kind, Some(ShapeKind::Box { .. })) {
                return fail(EXACTLY_ONE);
            }
            i += 1;
            let r = match args.get(i).and_then(parse_f64) {
                Some(v) => v,
                None => return fail(b"ERR syntax error"),
            };
            i += 1;
            conversion = match args
                .get(i)
                .and_then(extract_bytes)
                .and_then(|b| parse_unit(b))
            {
                Some(v) => v,
                None => return fail(UNIT_ERR),
            };
            kind = Some(ShapeKind::Radius(r));
        } else if arg.eq_ignore_ascii_case(b"BYBOX") {
            if matches!(kind, Some(ShapeKind::Radius(_))) {
                return fail(EXACTLY_ONE);
            }
            i += 1;
            let w = match args.get(i).and_then(parse_f64) {
                Some(v) => v,
                None => return fail(b"ERR syntax error"),
            };
            i += 1;
            let h = match args.get(i).and_then(parse_f64) {
                Some(v) => v,
                None => return fail(b"ERR syntax error"),
            };
            i += 1;
            conversion = match args
                .get(i)
                .and_then(extract_bytes)
                .and_then(|b| parse_unit(b))
            {
                Some(v) => v,
                None => return fail(UNIT_ERR),
            };
            kind = Some(ShapeKind::Box {
                width: w,
                height: h,
            });
        } else if arg.eq_ignore_ascii_case(b"ASC") {
            sort = Sort::Asc;
        } else if arg.eq_ignore_ascii_case(b"DESC") {
            sort = Sort::Desc;
        } else if arg.eq_ignore_ascii_case(b"ANY") {
            any = true;
        } else if arg.eq_ignore_ascii_case(b"COUNT") {
            i += 1;
            // redis: `getLongLongFromObjectOrReply` then `count <= 0`.
            let Some(raw) = args.get(i).and_then(extract_bytes) else {
                return fail(b"ERR syntax error");
            };
            let c: i64 = match std::str::from_utf8(raw).ok().and_then(|s| s.parse().ok()) {
                Some(c) => c,
                None => return fail(b"ERR value is not an integer or out of range"),
            };
            if c <= 0 {
                return fail(b"ERR COUNT must be > 0");
            }
            count = Some(usize::try_from(c).unwrap_or(usize::MAX));
        } else if arg.eq_ignore_ascii_case(b"WITHCOORD")
            || arg.eq_ignore_ascii_case(b"WITHDIST")
            || arg.eq_ignore_ascii_case(b"WITHHASH")
        {
            // GEOSEARCHSTORE stores a sorted set, so it has nowhere to put
            // the extras and redis refuses them by name rather than quietly
            // dropping them (moon#645).
            if store_mode {
                return fail(
                    b"ERR GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
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
            return fail(b"ERR syntax error");
        }
        i += 1;
    }

    let Some(kind) = kind else {
        return fail(EXACTLY_ONE);
    };
    if any && count.is_none() {
        return fail(b"ERR the ANY argument requires COUNT argument");
    }
    let Some(zref) = zref else {
        return empty();
    };

    // COUNT without ordering does not make much sense (the N closest are
    // wanted): redis forces ASC — but not for ANY.
    if count.is_some() && sort == Sort::None && !any {
        sort = Sort::Asc;
    }

    let shape = Shape {
        lon: center_lon,
        lat: center_lat,
        kind,
        conversion,
    };
    let cells = areas_by_shape(&shape);
    let index = ScoreIndex::of(zref);
    let limit = if any { count.unwrap_or(0) } else { 0 };
    let mut matches = members_of_all_neighbors(&index, &cells, &shape, limit);

    let returned = count.map_or(matches.len(), |c| c.min(matches.len()));
    if sort != Sort::None {
        let cmp = |a: &GeoMatch, b: &GeoMatch| {
            let o = a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal);
            if sort == Sort::Desc { o.reverse() } else { o }
        };
        if returned < matches.len() {
            // Partial sort: select the `returned` best, then order only them.
            if returned > 0 {
                matches.select_nth_unstable_by(returned - 1, cmp);
            }
            matches.truncate(returned);
        }
        matches.sort_by(cmp);
    }
    matches.truncate(returned);

    let has_extras = withcoord || withdist || withhash;
    let results: Vec<Frame> = matches
        .iter()
        .map(|(member, dist, lon, lat, score)| {
            if !has_extras {
                return Frame::BulkString(member.clone());
            }
            let mut entry = Vec::with_capacity(4);
            entry.push(Frame::BulkString(member.clone()));
            if withdist {
                entry.push(Frame::BulkString(fmt_distance(dist / conversion)));
            }
            if withhash {
                entry.push(Frame::Integer(*score as i64));
            }
            if withcoord {
                // Full shortest-round-tripping decimal, exactly as GEOPOS
                // prints it (moon#568).
                entry.push(Frame::Array(
                    vec![
                        Frame::BulkString(Bytes::from(fmt_geo_coord(*lon))),
                        Frame::BulkString(Bytes::from(fmt_geo_coord(*lat))),
                    ]
                    .into(),
                ));
            }
            Frame::Array(entry.into())
        })
        .collect();

    (
        matches,
        GeoOpts {
            unit_mult: conversion,
            storedist,
        },
        Frame::Array(results.into()),
    )
}

/// `GEOSEARCH` arity guard shared by the entry points.
pub(super) fn geosearch_arity(args: &[Frame]) -> Option<Frame> {
    (args.len() < 6).then(|| err_wrong_args("GEOSEARCH"))
}
