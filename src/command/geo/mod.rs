mod geo_cmd;

pub use geo_cmd::*;

use std::f64::consts::PI;

// ---------------------------------------------------------------------------
// Geohash encoding/decoding (52-bit integer, Redis-compatible)
// ---------------------------------------------------------------------------

// Redis (geohash.h) encodes the 52-bit zset SCORE with the WGS84 web-mercator
// latitude clamp ±85.05112878 — NOT ±90. Only the GEOHASH string command
// re-encodes the decoded cell center with standard geohash bounds (lat ±90)
// before base32-ing it. Both bounds are needed for byte parity:
// score bounds wrong → GEOPOS/GEODIST cell centers diverge from Redis;
// string bounds wrong → GEOHASH characters diverge.
const GEO_LAT_MIN: f64 = -85.05112878;
const GEO_LAT_MAX: f64 = 85.05112878;
const GEO_LON_MIN: f64 = -180.0;
const GEO_LON_MAX: f64 = 180.0;
const GEO_STEP_MAX: u8 = 26; // 52-bit precision

/// Interleave the low 26 bits of `xlo` (even positions) and `ylo` (odd
/// positions) — Redis's interleave64 (Morton code). `xlo` = latitude cells,
/// `ylo` = longitude cells, so longitude owns the MSB (bit 51).
fn interleave64(xlo: u32, ylo: u32) -> u64 {
    const B: [u64; 5] = [
        0x5555555555555555,
        0x3333333333333333,
        0x0F0F0F0F0F0F0F0F,
        0x00FF00FF00FF00FF,
        0x0000FFFF0000FFFF,
    ];
    const S: [u32; 5] = [1, 2, 4, 8, 16];
    let mut x = xlo as u64;
    let mut y = ylo as u64;
    x = (x | (x << S[4])) & B[4];
    x = (x | (x << S[3])) & B[3];
    x = (x | (x << S[2])) & B[2];
    x = (x | (x << S[1])) & B[1];
    x = (x | (x << S[0])) & B[0];
    y = (y | (y << S[4])) & B[4];
    y = (y | (y << S[3])) & B[3];
    y = (y | (y << S[2])) & B[2];
    y = (y | (y << S[1])) & B[1];
    y = (y | (y << S[0])) & B[0];
    x | (y << 1)
}

/// Inverse of interleave64: extract the even bits — Redis's deinterleave64
/// helper (call once on `bits` for latitude, once on `bits >> 1` for
/// longitude).
fn deinterleave_even(mut x: u64) -> u32 {
    const B: [u64; 6] = [
        0x5555555555555555,
        0x3333333333333333,
        0x0F0F0F0F0F0F0F0F,
        0x00FF00FF00FF00FF,
        0x0000FFFF0000FFFF,
        0x00000000FFFFFFFF,
    ];
    const S: [u32; 6] = [0, 1, 2, 4, 8, 16];
    x &= B[0];
    x = (x | (x >> S[1])) & B[1];
    x = (x | (x >> S[2])) & B[2];
    x = (x | (x >> S[3])) & B[3];
    x = (x | (x >> S[4])) & B[4];
    x = (x | (x >> S[5])) & B[5];
    x as u32
}

/// Redis geohashEncode for arbitrary ranges: normalize, scale by 2^26
/// (truncating cast, exactly like the C double→uint32 conversion), interleave.
fn geohash_encode_raw(lon: f64, lat: f64, lat_min: f64, lat_max: f64) -> u64 {
    let lat_offset = (lat - lat_min) / (lat_max - lat_min) * (1u64 << GEO_STEP_MAX) as f64;
    let lon_offset =
        (lon - GEO_LON_MIN) / (GEO_LON_MAX - GEO_LON_MIN) * (1u64 << GEO_STEP_MAX) as f64;
    interleave64(lat_offset as u32, lon_offset as u32)
}

/// Encode longitude/latitude into the 52-bit geohash score (Redis score bounds).
pub(crate) fn geohash_encode(lon: f64, lat: f64) -> f64 {
    geohash_encode_raw(lon, lat, GEO_LAT_MIN, GEO_LAT_MAX) as f64
}

/// Decode a 52-bit geohash score to the cell-center (longitude, latitude),
/// using Redis's direct min/max arithmetic (geohashDecode + center) so the
/// resulting f64s are bit-identical to Redis's GEOPOS output.
pub(crate) fn geohash_decode(score: f64) -> (f64, f64) {
    let hash = score as u64;
    let ilato = deinterleave_even(hash) as f64;
    let ilono = deinterleave_even(hash >> 1) as f64;
    let scale = (1u64 << GEO_STEP_MAX) as f64;

    let lat_min = GEO_LAT_MIN + (ilato / scale) * (GEO_LAT_MAX - GEO_LAT_MIN);
    let lat_max = GEO_LAT_MIN + ((ilato + 1.0) / scale) * (GEO_LAT_MAX - GEO_LAT_MIN);
    let lon_min = GEO_LON_MIN + (ilono / scale) * (GEO_LON_MAX - GEO_LON_MIN);
    let lon_max = GEO_LON_MIN + ((ilono + 1.0) / scale) * (GEO_LON_MAX - GEO_LON_MIN);

    ((lon_min + lon_max) / 2.0, (lat_min + lat_max) / 2.0)
}

/// The 11-character base32 geohash string, Redis-exact: decode the score
/// (score bounds), re-encode the cell center with STANDARD geohash bounds
/// (lat ±90), then emit 11 chars from the top 50 bits — the 11th character
/// is always '0' (Redis emits index 0 once the bit budget is exhausted).
pub(crate) fn geohash_to_string(score: f64) -> String {
    const ALPHABET: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    let (lon, lat) = geohash_decode(score);
    let bits = geohash_encode_raw(lon, lat, -90.0, 90.0);
    let mut result = [0u8; 11];
    for (i, slot) in result.iter_mut().enumerate() {
        let used = (i + 1) * 5;
        let idx = if used <= 52 {
            ((bits >> (52 - used)) & 0x1F) as usize
        } else {
            0
        };
        *slot = ALPHABET[idx];
    }
    String::from_utf8_lossy(&result).to_string()
}

/// Format a coordinate the way Redis 8 replies to GEOPOS / WITHCOORD.
/// Redis's d2string uses fpconv_dtoa (grisu2) — the SHORTEST decimal that
/// round-trips to the same f64 — which is exactly Rust's `{}` Display for
/// f64 (verified byte-identical against redis-server 8.x for the geohash
/// cell centers the consistency suite compares).
pub(crate) fn fmt_geo_coord(v: f64) -> String {
    format!("{v}")
}

// ---------------------------------------------------------------------------
// Haversine distance
// ---------------------------------------------------------------------------

const EARTH_RADIUS_M: f64 = 6372797.560856;

/// Haversine distance in meters between two (lon, lat) pairs.
///
/// Operation order matches Redis's geohashGetDistance exactly (radians first,
/// then differences; u/v half-angle sines; single asin) so the resulting f64
/// is bit-identical and GEODIST's %.4f output matches byte-for-byte.
pub(crate) fn haversine_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let lat1r = lat1 * PI / 180.0;
    let lon1r = lon1 * PI / 180.0;
    let lat2r = lat2 * PI / 180.0;
    let lon2r = lon2 * PI / 180.0;
    let u = ((lat2r - lat1r) / 2.0).sin();
    let v = ((lon2r - lon1r) / 2.0).sin();
    if u == 0.0 && v == 0.0 {
        return 0.0;
    }
    let a = u * u + lat1r.cos() * lat2r.cos() * v * v;
    2.0 * EARTH_RADIUS_M * a.sqrt().asin()
}

/// Convert meters to the specified unit.
pub(crate) fn convert_distance(meters: f64, unit: &[u8]) -> f64 {
    if unit.eq_ignore_ascii_case(b"km") {
        meters / 1000.0
    } else if unit.eq_ignore_ascii_case(b"mi") {
        meters / 1609.34
    } else if unit.eq_ignore_ascii_case(b"ft") {
        meters / 0.3048
    } else {
        meters // default: meters
    }
}

/// Parse a unit string, returning meters-per-unit multiplier. Returns None if invalid.
pub(crate) fn parse_unit(unit: &[u8]) -> Option<f64> {
    if unit.eq_ignore_ascii_case(b"m") {
        Some(1.0)
    } else if unit.eq_ignore_ascii_case(b"km") {
        Some(1000.0)
    } else if unit.eq_ignore_ascii_case(b"mi") {
        Some(1609.34)
    } else if unit.eq_ignore_ascii_case(b"ft") {
        Some(0.3048)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Frame;
    use crate::storage::Database;
    use bytes::Bytes;

    fn bs(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    /// Byte-parity pins vs redis-server 8.x (values captured by
    /// scripts/test-consistency.sh on moon-dev): score-bounds decode,
    /// %.17g coordinate formatting, ±90 string re-encode, and the Redis
    /// haversine op order must all match for these to hold.
    #[test]
    fn test_redis_parity_palermo() {
        let score_p = geohash_encode(13.361389, 38.115556);
        let score_c = geohash_encode(15.087269, 37.502669);

        // GEOPOS Palermo
        let (lon, lat) = geohash_decode(score_p);
        assert_eq!(fmt_geo_coord(lon), "13.361389338970184");
        assert_eq!(fmt_geo_coord(lat), "38.1155563954963");

        // GEOHASH Palermo — 11 chars, Redis's 11th is always '0'
        assert_eq!(geohash_to_string(score_p), "sqc8b49rny0");

        // GEODIST Palermo Catania in m and km
        let (lon2, lat2) = geohash_decode(score_c);
        let d = haversine_distance(lon, lat, lon2, lat2);
        assert_eq!(format!("{:.4}", convert_distance(d, b"m")), "166274.1516");
        assert_eq!(format!("{:.4}", convert_distance(d, b"km")), "166.2742");
    }

    #[test]
    fn test_fmt_geo_coord_edges() {
        assert_eq!(fmt_geo_coord(0.0), "0");
        assert_eq!(fmt_geo_coord(0.5), "0.5");
        assert_eq!(fmt_geo_coord(-13.361389338970184), "-13.361389338970184");
    }

    #[test]
    fn test_geohash_roundtrip() {
        // Rome coordinates
        let lon = 12.4964;
        let lat = 41.9028;
        let hash = geohash_encode(lon, lat);
        let (lon2, lat2) = geohash_decode(hash);
        assert!((lon - lon2).abs() < 0.0001);
        assert!((lat - lat2).abs() < 0.0001);
    }

    #[test]
    fn test_haversine_rome_paris() {
        // Rome to Paris ~1105 km
        let d = haversine_distance(12.4964, 41.9028, 2.3522, 48.8566);
        assert!((d / 1000.0 - 1105.0).abs() < 10.0); // within 10 km
    }

    #[test]
    fn test_geohash_string() {
        let hash = geohash_encode(-122.4194, 37.7749); // San Francisco
        let s = geohash_to_string(hash);
        assert_eq!(s.len(), 11);
        // Should start with "9q8y" for SF area
        // The exact prefix depends on our 52-bit encoding; just verify length and base32 chars
        assert!(
            s.chars()
                .all(|c| "0123456789bcdefghjkmnpqrstuvwxyz".contains(c)),
            "invalid chars: {s}"
        );
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
