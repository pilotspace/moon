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

use super::{
    GEO_STEP_MAX, deinterleave_even, fmt_geo_coord, geohash_decode, interleave64, parse_unit,
};

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

/// `geohashEstimateStepsByRadius`, defined and bounded for every input.
///
/// redis only calls it with a radius its parser accepted — a number, not
/// negative — and so does moon (`parse_radius`, `parse_box`). The function
/// is total anyway:
///
/// * a range that is not a positive number — 0, redis's own early return,
///   and a negative or NaN one no caller passes — is the finest step, 26:
///   the smallest search area, which a zero radius needs and which is
///   harmless for a shape nothing can lie inside;
/// * `+inf` is the coarsest step, 1: what redis's loop yields when its
///   condition is false from the start;
/// * the doubling loop stops once `step` reaches 26 + 4. From there the two
///   base-case decrements and at most two polar ones leave it at or above
///   26, the clamp's ceiling, so doubling further cannot change the answer
///   (`step_estimate_bound_matches_redis_for_every_positive_radius`).
pub(super) fn estimate_steps_by_radius(range_meters: f64, lat: f64) -> u8 {
    const STEP_MAX: i32 = GEO_STEP_MAX as i32;
    if range_meters.is_nan() || range_meters <= 0.0 {
        return GEO_STEP_MAX;
    }
    if range_meters == f64::INFINITY {
        return 1;
    }
    let mut range = range_meters;
    let mut step: i32 = 1;
    while range < MERCATOR_MAX && step < STEP_MAX + 4 {
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
    step.clamp(1, STEP_MAX) as u8
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
        // Cell bits are at most 53 bits wide (a centre on the upper latitude
        // edge encodes one row past the last, as in redis), so the successor
        // cannot overflow; saturating keeps that true for any input.
        let max = align52(HashBits {
            bits: cell.bits.saturating_add(1),
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
/// paths need it: `unit_mult` turns the meters every match carries back into
/// the query's unit, `storedist` picks the stored score, and `store_dest`
/// names the legacy forms' destination.
#[derive(Clone, Copy)]
pub(super) struct GeoOpts {
    /// Meters per unit of the query's BYRADIUS/BYBOX unit.
    pub(super) unit_mult: f64,
    /// Score the stored set by distance, not by geohash: GEOSEARCHSTORE's
    /// bare `STOREDIST`, or a legacy `STOREDIST key` clause.
    pub(super) storedist: bool,
    /// The legacy `STORE key` / `STOREDIST key` destination, as an index into
    /// the `args` that were parsed. The LAST clause wins, as in redis. Always
    /// `None` for GEOSEARCH and GEOSEARCHSTORE.
    pub(super) store_dest: Option<usize>,
}

impl Default for GeoOpts {
    fn default() -> Self {
        // 1.0 = meters, the identity for every `dist / unit_mult` below, so
        // an error return can never scale a distance by zero.
        Self {
            unit_mult: 1.0,
            storedist: false,
            store_dest: None,
        }
    }
}

/// Which grammar [`geosearch_core`] parses — the `flags` redis's
/// `georadiusGeneric` is called with (`GEOSEARCH`, `GEOSEARCHSTORE`,
/// `RADIUS_COORDS`, `RADIUS_MEMBER`, `RADIUS_NOSTORE`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GeoForm {
    /// `GEOSEARCH key <options>`.
    Search,
    /// `GEOSEARCHSTORE dest key <options>`, parsed from the source key on.
    SearchStore,
    /// `GEORADIUS key lon lat radius unit <options>`. `store` is false for
    /// `GEORADIUS_RO`: its grammar has no `STORE`/`STOREDIST` clause, so the
    /// keyword is a syntax error like any unknown one. That is what keeps the
    /// read-only twin (`flags: R`, replica-routable, served on the shared-lock
    /// read path) from ever writing a key.
    RadiusCoords { store: bool },
    /// `GEORADIUSBYMEMBER key member radius unit <options>`; `store` as above
    /// for `GEORADIUSBYMEMBER_RO`.
    RadiusMember { store: bool },
}

impl GeoForm {
    fn is_search(self) -> bool {
        matches!(self, GeoForm::Search | GeoForm::SearchStore)
    }

    fn has_store_clause(self) -> bool {
        matches!(
            self,
            GeoForm::RadiusCoords { store: true } | GeoForm::RadiusMember { store: true }
        )
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Sort {
    None,
    Asc,
    Desc,
}

type CoreReply = (Vec<GeoMatch>, GeoOpts, Frame);

const SYNTAX_ERR: &[u8] = b"ERR syntax error";
const NOT_A_FLOAT: &[u8] = b"ERR value is not a valid float";
const NOT_AN_INTEGER: &[u8] = b"ERR value is not an integer or out of range";
const UNIT_ERR: &[u8] = b"ERR unsupported unit provided. please use M, KM, FT, MI";
const NO_SUCH_MEMBER: &[u8] = b"ERR could not decode requested zset member";

fn fail(msg: &'static [u8]) -> CoreReply {
    fail_with(Frame::Error(Bytes::from_static(msg)))
}

fn fail_with(reply: Frame) -> CoreReply {
    (Vec::new(), GeoOpts::default(), reply)
}

fn error(msg: &'static [u8]) -> Frame {
    Frame::Error(Bytes::from_static(msg))
}

/// redis's `string2d` (`getDoubleFromObject`): the whole argument is one
/// decimal, `inf` or `infinity` literal (any case, optional sign) — no
/// surrounding space, not NaN, and not a literal `strtod` flags `ERANGE` on
/// (one that overflows to infinity or underflows to zero). `strtod`'s
/// hexadecimal floats are the one spelling this does not read.
fn parse_double(arg: &Frame) -> Option<f64> {
    let s = std::str::from_utf8(extract_bytes(arg)?).ok()?;
    let v: f64 = s.parse().ok()?;
    if v.is_nan() {
        return None;
    }
    let unsigned = s.strip_prefix(['+', '-']).unwrap_or(s);
    if v.is_infinite()
        && !(unsigned.eq_ignore_ascii_case("inf") || unsigned.eq_ignore_ascii_case("infinity"))
    {
        return None;
    }
    let nonzero_mantissa = unsigned
        .bytes()
        .take_while(|b| !matches!(b, b'e' | b'E'))
        .any(|b| b.is_ascii_digit() && b != b'0');
    if v == 0.0 && nonzero_mantissa {
        return None;
    }
    Some(v)
}

/// `-ERR invalid longitude,latitude pair %f,%f` (redis's
/// `extractLongLatOrReply`). `%f` is six fixed decimals.
fn invalid_lonlat(lon: f64, lat: f64) -> Frame {
    use std::io::Write;
    let mut msg = Vec::with_capacity(64);
    // Writing into a Vec cannot fail.
    let _ = write!(msg, "ERR invalid longitude,latitude pair {lon:.6},{lat:.6}");
    Frame::Error(Bytes::from(msg))
}

/// `extractLongLatOrReply`: both numbers, then the WGS84 range the cell
/// search can encode.
fn parse_lonlat(lon: &Frame, lat: &Frame) -> Result<(f64, f64), Frame> {
    let lon = parse_double(lon).ok_or_else(|| error(NOT_A_FLOAT))?;
    let lat = parse_double(lat).ok_or_else(|| error(NOT_A_FLOAT))?;
    if !(GEO_LONG_MIN..=GEO_LONG_MAX).contains(&lon) || !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&lat)
    {
        return Err(invalid_lonlat(lon, lat));
    }
    Ok((lon, lat))
}

/// `extractUnitOrReply`: meters per unit.
fn parse_unit_arg(unit: &Frame) -> Result<f64, Frame> {
    extract_bytes(unit)
        .and_then(|u| parse_unit(u))
        .ok_or_else(|| error(UNIT_ERR))
}

/// `extractDistanceOrReply`: the radius must be a number, then not negative
/// (`-0` is zero), and only then is the unit read. `+inf` is accepted, as in
/// redis. Returns `(radius, meters per unit)`.
fn parse_radius(radius: &Frame, unit: &Frame) -> Result<(f64, f64), Frame> {
    let r = parse_double(radius).ok_or_else(|| error(b"ERR need numeric radius"))?;
    if r < 0.0 {
        return Err(error(b"ERR radius cannot be negative"));
    }
    Ok((r, parse_unit_arg(unit)?))
}

/// `extractBoxOrReply`: width then height must be numbers, then neither may
/// be negative, and only then is the unit read. Returns `(width, height,
/// meters per unit)`.
fn parse_box(width: &Frame, height: &Frame, unit: &Frame) -> Result<(f64, f64, f64), Frame> {
    let w = parse_double(width).ok_or_else(|| error(b"ERR need numeric width"))?;
    let h = parse_double(height).ok_or_else(|| error(b"ERR need numeric height"))?;
    if w < 0.0 || h < 0.0 {
        return Err(error(b"ERR height or width cannot be negative"));
    }
    Ok((w, h, parse_unit_arg(unit)?))
}

/// `longLatFromMember`: the decoded position of a member, or `None` when the
/// set does not hold it.
fn member_position(zref: &SortedSetRef<'_>, member: &Frame) -> Option<(f64, f64)> {
    zref.score(extract_bytes(member)?).map(geohash_decode)
}

/// GEOSEARCH / GEOSEARCHSTORE / GEORADIUS* parse + search, independent of
/// how the sorted set was fetched — every dispatch path reads it through
/// `get_sorted_set_ref_if_alive` (moon#1172: the mutable path used
/// `get_sorted_set`, which flattened a listpack geo set for good). `args`
/// starts at the source key.
///
/// The parse is redis 7.0.15's `georadiusGeneric`, clause for clause, so the
/// errors are redis's and come in redis's order:
///
/// 1. the legacy forms' positional head — GEORADIUS's centre (`value is not
///    a valid float`, `invalid longitude,latitude pair`) then radius;
///    GEORADIUSBYMEMBER's member (`could not decode requested zset member`)
///    then radius. On a MISSING key GEORADIUSBYMEMBER skips its head
///    entirely (there is no member to decode) — redis does exactly that;
/// 2. every option, left to right, GEOSEARCH's FROM*/BY* clauses included
///    and in any order (a repeated one overrides). The radius, width and
///    height are validated as they are parsed (`need numeric radius`,
///    `radius cannot be negative`, `need numeric width|height`, `height or
///    width cannot be negative`) and before their unit;
/// 3. WITH* with a store, a missing FROM*/BY*, ANY without COUNT;
/// 4. only then a missing key: an empty array, or for a store the empty
///    match list its caller turns into `:0` plus a deleted destination.
///
/// So a missing key still validates every option (moon answered `[]` for a
/// bad COUNT there), and the cell search only ever sees a radius, width and
/// height that are numbers and not negative.
pub(super) fn geosearch_core(
    zref: Option<&SortedSetRef<'_>>,
    args: &[Frame],
    form: GeoForm,
) -> CoreReply {
    let mut center: Option<(f64, f64)> = None;
    let mut kind: Option<ShapeKind> = None;
    let mut conversion = 1.0f64;

    // The legacy forms' positional head (`base_args`).
    let base = match form {
        GeoForm::RadiusCoords { .. } => {
            let [_key, lon, lat, radius, unit, ..] = args else {
                return fail_with(err_wrong_args("GEORADIUS"));
            };
            let c = match parse_lonlat(lon, lat) {
                Ok(c) => c,
                Err(e) => return fail_with(e),
            };
            match parse_radius(radius, unit) {
                Ok((r, conv)) => {
                    center = Some(c);
                    kind = Some(ShapeKind::Radius(r));
                    conversion = conv;
                }
                Err(e) => return fail_with(e),
            }
            5
        }
        GeoForm::RadiusMember { .. } => {
            let [_key, member, radius, unit, ..] = args else {
                return fail_with(err_wrong_args("GEORADIUSBYMEMBER"));
            };
            if let Some(zref) = zref {
                let Some(c) = member_position(zref, member) else {
                    return fail(NO_SUCH_MEMBER);
                };
                match parse_radius(radius, unit) {
                    Ok((r, conv)) => {
                        center = Some(c);
                        kind = Some(ShapeKind::Radius(r));
                        conversion = conv;
                    }
                    Err(e) => return fail_with(e),
                }
            }
            4
        }
        GeoForm::Search | GeoForm::SearchStore => 1,
    };

    let mut sort = Sort::None;
    // 0 = unlimited, as redis's `count`.
    let mut count: i64 = 0;
    let mut any = false;
    let mut withcoord = false;
    let mut withdist = false;
    let mut withhash = false;
    let mut storedist = false;
    let mut store_dest: Option<usize> = None;
    let mut frommember = false;
    let mut fromloc = false;
    let mut byradius = false;
    let mut bybox = false;

    let mut i = base;
    while i < args.len() {
        let Some(arg) = extract_bytes(&args[i]) else {
            return fail(SYNTAX_ERR);
        };
        // How many arguments follow this one: a clause with too few
        // operands is not that clause, and ends as a syntax error.
        let left = args.len() - i - 1;
        let is = |kw: &[u8]| arg.eq_ignore_ascii_case(kw);
        if is(b"WITHDIST") {
            withdist = true;
        } else if is(b"WITHHASH") {
            withhash = true;
        } else if is(b"WITHCOORD") {
            withcoord = true;
        } else if is(b"ANY") {
            any = true;
        } else if is(b"ASC") {
            sort = Sort::Asc;
        } else if is(b"DESC") {
            sort = Sort::Desc;
        } else if is(b"COUNT") && left >= 1 {
            // `getLongLongFromObjectOrReply` (`string2ll`), then `count <= 0`.
            let Some(c) =
                extract_bytes(&args[i + 1]).and_then(|b| crate::storage::numeric::canonical_i64(b))
            else {
                return fail(NOT_AN_INTEGER);
            };
            if c <= 0 {
                return fail(b"ERR COUNT must be > 0");
            }
            count = c;
            i += 1;
        } else if form.has_store_clause() && (is(b"STORE") || is(b"STOREDIST")) && left >= 1 {
            // The next slot is the destination whatever it spells.
            storedist = is(b"STOREDIST");
            store_dest = Some(i + 1);
            i += 1;
        } else if form == GeoForm::SearchStore && is(b"STOREDIST") {
            // GEOSEARCHSTORE's STOREDIST is a bare flag.
            storedist = true;
        } else if form.is_search() && is(b"FROMMEMBER") && left >= 1 && !fromloc {
            // No source key: nothing to decode; the parse goes on and the
            // empty reply comes at the end.
            if let Some(zref) = zref {
                let Some(c) = member_position(zref, &args[i + 1]) else {
                    return fail(NO_SUCH_MEMBER);
                };
                center = Some(c);
            }
            frommember = true;
            i += 1;
        } else if form.is_search() && is(b"FROMLONLAT") && left >= 2 && !frommember {
            match parse_lonlat(&args[i + 1], &args[i + 2]) {
                Ok(c) => center = Some(c),
                Err(e) => return fail_with(e),
            }
            fromloc = true;
            i += 2;
        } else if form.is_search() && is(b"BYRADIUS") && left >= 2 && !bybox {
            match parse_radius(&args[i + 1], &args[i + 2]) {
                Ok((r, conv)) => {
                    kind = Some(ShapeKind::Radius(r));
                    conversion = conv;
                }
                Err(e) => return fail_with(e),
            }
            byradius = true;
            i += 2;
        } else if form.is_search() && is(b"BYBOX") && left >= 3 && !byradius {
            match parse_box(&args[i + 1], &args[i + 2], &args[i + 3]) {
                Ok((width, height, conv)) => {
                    kind = Some(ShapeKind::Box { width, height });
                    conversion = conv;
                }
                Err(e) => return fail_with(e),
            }
            bybox = true;
            i += 3;
        } else {
            return fail(SYNTAX_ERR);
        }
        i += 1;
    }

    // Options not compatible with a store: it has nowhere to put the extras,
    // and redis refuses them by name rather than quietly dropping them
    // (moon#645) — in this fixed order, and saying "in GEORADIUS" for
    // GEORADIUSBYMEMBER too.
    let storing = store_dest.is_some() || form == GeoForm::SearchStore;
    if storing && (withdist || withhash || withcoord) {
        return fail(if form == GeoForm::SearchStore {
            b"ERR GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options"
        } else {
            b"ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options"
        });
    }
    // redis names the command as it was called; these are its canonical
    // (upper-case) spellings.
    if form.is_search() && !(frommember || fromloc) {
        return fail(if form == GeoForm::SearchStore {
            b"ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCHSTORE"
        } else {
            b"ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH"
        });
    }
    if form.is_search() && !(byradius || bybox) {
        return fail(if form == GeoForm::SearchStore {
            b"ERR exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCHSTORE"
        } else {
            b"ERR exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH"
        });
    }
    if any && count == 0 {
        return fail(b"ERR the ANY argument requires COUNT argument");
    }

    let opts = GeoOpts {
        unit_mult: conversion,
        storedist,
        store_dest,
    };
    // Return ASAP when the source key does not exist.
    let (Some(zref), Some((center_lon, center_lat)), Some(kind)) = (zref, center, kind) else {
        return (Vec::new(), opts, Frame::Array(Vec::new().into()));
    };

    // COUNT without ordering does not make much sense (the N closest are
    // wanted): redis forces ASC — but not for ANY.
    if count != 0 && sort == Sort::None && !any {
        sort = Sort::Asc;
    }
    let count = (count != 0).then(|| usize::try_from(count).unwrap_or(usize::MAX));

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

    // A store replies with its count; its caller never reads this frame.
    if storing {
        return (matches, opts, Frame::Array(Vec::new().into()));
    }

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

    (matches, opts, Frame::Array(results.into()))
}

/// `GEOSEARCH` arity guard shared by the entry points.
pub(super) fn geosearch_arity(args: &[Frame]) -> Option<Frame> {
    (args.len() < 6).then(|| err_wrong_args("GEOSEARCH"))
}
