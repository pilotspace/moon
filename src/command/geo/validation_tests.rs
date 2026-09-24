//! GEO argument validation and redis 7.0.15 parity (moon#1227 review, refs
//! moon#1172): the BYRADIUS radius and the BYBOX width/height are checked
//! when they are parsed, with redis's own error texts, on every command form
//! and dispatch entry point; the parse runs to the end on a missing key, as
//! redis's `georadiusGeneric` does; and the cell-step estimator is defined
//! and bounded for every input.
//!
//! Every expected reply below was captured from redis-server 7.0.15. Command
//! lines are space-separated templates; `R`, `W` and `H` are replaced by the
//! radius, width and height under test (which may be empty or hold spaces).

use bytes::Bytes;

use super::geo_search::estimate_steps_by_radius;
use super::*;
use crate::protocol::Frame;
use crate::storage::Database;

const NEG_RADIUS: &str = "ERR radius cannot be negative";
const NUM_RADIUS: &str = "ERR need numeric radius";
const NEG_BOX: &str = "ERR height or width cannot be negative";
const NUM_WIDTH: &str = "ERR need numeric width";
const NUM_HEIGHT: &str = "ERR need numeric height";
const NOT_INT: &str = "ERR value is not an integer or out of range";
const NO_MEMBER: &str = "ERR could not decode requested zset member";
const SYNTAX: &str = "ERR syntax error";

fn frames(args: &[&str]) -> Vec<Frame> {
    args.iter()
        .map(|a| Frame::BulkString(Bytes::copy_from_slice(a.as_bytes())))
        .collect()
}

/// Split a template into words, replacing each placeholder word.
fn words<'a>(template: &'a str, subst: &[(&str, &'a str)]) -> Vec<&'a str> {
    template
        .split(' ')
        .map(|w| subst.iter().find(|(k, _)| *k == w).map_or(w, |(_, v)| *v))
        .collect()
}

fn error(msg: &str) -> Frame {
    Frame::Error(Bytes::copy_from_slice(msg.as_bytes()))
}

fn array(members: &[&str]) -> Frame {
    Frame::Array(
        members
            .iter()
            .map(|m| Frame::BulkString(Bytes::copy_from_slice(m.as_bytes())))
            .collect::<Vec<_>>()
            .into(),
    )
}

/// `g` holds Palermo and Catania (redis's documentation seed); `d` is a
/// string that a failed STORE must leave in place.
fn sicily() -> Database {
    let mut db = Database::new();
    geoadd(
        &mut db,
        &frames(&[
            "g",
            "13.361389",
            "38.115556",
            "Palermo",
            "15.087269",
            "37.502669",
            "Catania",
        ]),
    );
    db.set_string(b"d", Bytes::from_static(b"keep me"));
    db
}

/// Run one command line through every handler that serves it — the mutable
/// entry point (`command::dispatch`: SPSC cross-shard, MULTI, Lua) and, where
/// one exists, the `_readonly` twin (`command::dispatch_read`) — and return
/// each reply with the handler's name.
fn run_everywhere(db: &mut Database, line: &[&str]) -> Vec<(&'static str, Frame)> {
    let (name, rest) = line.split_first().expect("a command line");
    let args = frames(rest);
    match *name {
        "GEOSEARCH" => vec![
            ("geosearch", geosearch(db, &args)),
            ("geosearch_readonly", geosearch_readonly(db, &args, 0)),
        ],
        "GEOSEARCHSTORE" => vec![("geosearchstore", geosearchstore(db, &args))],
        "GEORADIUS" => vec![("georadius", georadius(db, &args))],
        "GEORADIUS_RO" => vec![
            ("georadius_ro", georadius_ro(db, &args)),
            ("georadius_ro_readonly", georadius_ro_readonly(db, &args, 0)),
        ],
        "GEORADIUSBYMEMBER" => vec![("georadiusbymember", georadiusbymember(db, &args))],
        "GEORADIUSBYMEMBER_RO" => vec![
            ("georadiusbymember_ro", georadiusbymember_ro(db, &args)),
            (
                "georadiusbymember_ro_readonly",
                georadiusbymember_ro_readonly(db, &args, 0),
            ),
        ],
        other => panic!("not a geo search command: {other}"),
    }
}

fn assert_reply(db: &mut Database, line: &[&str], want: &Frame) {
    for (handler, got) in run_everywhere(db, line) {
        assert_eq!(&got, want, "{handler}: {line:?}");
    }
    // A rejected STORE must not have touched its destination.
    if matches!(want, Frame::Error(_)) {
        assert_eq!(
            db.get(b"d")
                .and_then(|e| e.value.as_bytes().map(|b| b.to_vec())),
            Some(b"keep me".to_vec()),
            "a rejected command changed the STORE destination: {line:?}"
        );
    }
}

/// Every form that takes a radius. A missing key validates the radius
/// exactly as an existing one does — except GEORADIUSBYMEMBER, pinned
/// separately below.
const RADIUS_FORMS: &[&str] = &[
    "GEOSEARCH g FROMLONLAT 15 37 BYRADIUS R km",
    "GEOSEARCH g FROMMEMBER Palermo BYRADIUS R km",
    "GEOSEARCH g BYRADIUS R km FROMLONLAT 15 37",
    "GEOSEARCH missing FROMLONLAT 15 37 BYRADIUS R km",
    "GEOSEARCH missing FROMMEMBER x BYRADIUS R km",
    "GEOSEARCH g FROMLONLAT 15 37 BYRADIUS R km COUNT 1 ANY",
    "GEOSEARCHSTORE d g FROMLONLAT 15 37 BYRADIUS R km",
    "GEOSEARCHSTORE d g FROMMEMBER Palermo BYRADIUS R km STOREDIST",
    "GEOSEARCHSTORE d missing FROMMEMBER x BYRADIUS R km",
    "GEORADIUS g 15 37 R km",
    "GEORADIUS g 15 37 R km WITHDIST ASC",
    "GEORADIUS g 15 37 R km STORE d",
    "GEORADIUS g 15 37 R km WITHDIST STORE d",
    "GEORADIUS missing 15 37 R km",
    "GEORADIUS missing 15 37 R km STOREDIST d",
    "GEORADIUS_RO g 15 37 R km",
    "GEORADIUS_RO missing 15 37 R km",
    "GEORADIUSBYMEMBER g Palermo R km",
    "GEORADIUSBYMEMBER g Palermo R km STOREDIST d",
    "GEORADIUSBYMEMBER_RO g Palermo R km COUNT 1",
];

/// `(radius, reply)`: a negative number (including `-inf`) and anything
/// `string2d` refuses — NaN, text, surrounding space, and a literal that
/// overflows to infinity or underflows to zero.
const BAD_RADII: &[(&str, &str)] = &[
    ("-1", NEG_RADIUS),
    ("-0.001", NEG_RADIUS),
    ("-inf", NEG_RADIUS),
    ("-1e300", NEG_RADIUS),
    ("nan", NUM_RADIUS),
    ("-nan", NUM_RADIUS),
    ("abc", NUM_RADIUS),
    ("", NUM_RADIUS),
    (" 5", NUM_RADIUS),
    ("5 ", NUM_RADIUS),
    ("1e400", NUM_RADIUS),
    ("-1e400", NUM_RADIUS),
    ("1e-400", NUM_RADIUS),
];

#[test]
fn radius_is_validated_on_every_form_like_redis() {
    let mut db = sicily();
    for form in RADIUS_FORMS {
        for (radius, msg) in BAD_RADII {
            assert_reply(&mut db, &words(form, &[("R", radius)]), &error(msg));
        }
    }
}

/// The unit is only read after the radius has been accepted.
#[test]
fn a_bad_radius_is_reported_before_a_bad_unit() {
    let mut db = sicily();
    for form in RADIUS_FORMS {
        let line = words(form, &[("R", "-1"), ("km", "parsecs")]);
        assert_reply(&mut db, &line, &error(NEG_RADIUS));
    }
}

#[test]
fn box_width_and_height_are_validated_like_redis() {
    let mut db = sicily();
    // Both are parsed before either sign is checked.
    let cases: &[(&str, &str, &str)] = &[
        ("-1", "5", NEG_BOX),
        ("5", "-1", NEG_BOX),
        ("-inf", "1", NEG_BOX),
        ("-1", "-1", NEG_BOX),
        ("nan", "5", NUM_WIDTH),
        ("5", "nan", NUM_HEIGHT),
        ("abc", "def", NUM_WIDTH),
        ("5", "def", NUM_HEIGHT),
        ("-1", "abc", NUM_HEIGHT),
        ("1e400", "1", NUM_WIDTH),
        ("1", "1e-400", NUM_HEIGHT),
    ];
    for &(w, h, msg) in cases {
        for form in [
            "GEOSEARCH g FROMLONLAT 15 37 BYBOX W H km",
            "GEOSEARCH g BYBOX W H parsecs FROMMEMBER Palermo",
            "GEOSEARCH missing FROMMEMBER x BYBOX W H km",
            "GEOSEARCHSTORE d g FROMLONLAT 15 37 BYBOX W H km",
            "GEOSEARCHSTORE d missing FROMLONLAT 15 37 BYBOX W H km",
        ] {
            assert_reply(&mut db, &words(form, &[("W", w), ("H", h)]), &error(msg));
        }
    }
}

/// The accepted edges: zero, negative zero, infinity (redis 7.0.15 answers
/// every member for an infinite radius or box, and none for a zero one away
/// from a member).
#[test]
fn zero_and_infinite_shapes_are_accepted_like_redis() {
    let mut db = sicily();
    let both = array(&["Palermo", "Catania"]);
    for (line, want) in [
        ("GEOSEARCH g FROMLONLAT 15 37 BYRADIUS 0 km", array(&[])),
        ("GEOSEARCH g FROMLONLAT 15 37 BYRADIUS -0 km", array(&[])),
        ("GEOSEARCH g FROMLONLAT 15 37 BYRADIUS inf km", both.clone()),
        (
            "GEOSEARCH g FROMLONLAT 15 37 BYRADIUS INFINITY m",
            both.clone(),
        ),
        (
            "GEOSEARCH g FROMLONLAT 15 37 BYRADIUS 1e308 km",
            both.clone(),
        ),
        ("GEOSEARCH g FROMLONLAT 15 37 BYBOX 0 0 km", array(&[])),
        (
            "GEOSEARCH g FROMLONLAT 15 37 BYBOX inf inf km",
            both.clone(),
        ),
        ("GEOSEARCH g FROMLONLAT 15 37 BYBOX inf 0 km", array(&[])),
        ("GEORADIUS g 15 37 inf km", both.clone()),
        ("GEORADIUSBYMEMBER g Palermo 0 km", array(&["Palermo"])),
        ("GEORADIUSBYMEMBER g Palermo inf km", both),
    ] {
        assert_reply(&mut db, &words(line, &[]), &want);
    }
}

/// m2: redis parses every option before it looks at whether the key exists,
/// so a bad COUNT (or ANY without COUNT, or a bad unit) on a missing key is
/// still an error — moon answered `[]`.
#[test]
fn a_missing_key_still_validates_every_option() {
    let mut db = sicily();
    for (line, msg) in [
        (
            "GEOSEARCH missing FROMMEMBER x BYRADIUS 1 km COUNT abc",
            NOT_INT,
        ),
        (
            "GEOSEARCH missing FROMMEMBER x BYRADIUS 1 km COUNT 1.5",
            NOT_INT,
        ),
        (
            "GEOSEARCH missing FROMMEMBER x BYRADIUS 1 km COUNT 0",
            "ERR COUNT must be > 0",
        ),
        (
            "GEOSEARCH missing FROMMEMBER x BYRADIUS 1 km ANY",
            "ERR the ANY argument requires COUNT argument",
        ),
        (
            "GEOSEARCH missing FROMMEMBER x BYRADIUS 1 parsecs",
            "ERR unsupported unit provided. please use M, KM, FT, MI",
        ),
        (
            "GEOSEARCH missing FROMLONLAT 15 37 BYRADIUS 1 km COUNT abc",
            NOT_INT,
        ),
        (
            "GEOSEARCHSTORE d missing FROMMEMBER x BYRADIUS 1 km COUNT abc",
            NOT_INT,
        ),
        (
            "GEOSEARCHSTORE d missing FROMMEMBER x BYRADIUS 1 km WITHDIST",
            "ERR GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
        ),
        ("GEORADIUS missing 15 37 1 km COUNT abc STORE d", NOT_INT),
        ("GEORADIUSBYMEMBER missing Palermo 1 km COUNT abc", NOT_INT),
        (
            "GEORADIUSBYMEMBER missing Palermo 1 km COUNT 1.5 STORE d",
            NOT_INT,
        ),
        (
            "GEORADIUSBYMEMBER_RO missing Palermo 1 km COUNT abc",
            NOT_INT,
        ),
    ] {
        assert_reply(&mut db, &words(line, &[]), &error(msg));
    }
}

/// redis's one exception: GEORADIUSBYMEMBER on a missing key skips its
/// positional member/radius/unit (there is no member to decode) and still
/// parses the options, so a bad radius there answers `[]` — in 7.0.15 and 8.x.
#[test]
fn georadiusbymember_on_a_missing_key_skips_the_radius_like_redis() {
    let mut db = sicily();
    for form in [
        "GEORADIUSBYMEMBER missing Palermo R km",
        "GEORADIUSBYMEMBER_RO missing Palermo R km",
        "GEORADIUSBYMEMBER missing Palermo 1 R",
    ] {
        for radius in ["-1", "nan", "abc", "parsecs"] {
            assert_reply(&mut db, &words(form, &[("R", radius)]), &array(&[]));
        }
    }
}

/// FROMMEMBER of a member the set does not hold is redis's
/// `could not decode requested zset member`, reported when FROMMEMBER is
/// parsed — before the radius of GEORADIUSBYMEMBER. moon answered `[]`.
#[test]
fn an_absent_frommember_is_an_error_like_redis() {
    let mut db = sicily();
    for line in [
        "GEOSEARCH g FROMMEMBER nope BYRADIUS 1 km",
        "GEOSEARCH g BYRADIUS 1 km FROMMEMBER nope",
        "GEOSEARCHSTORE d g FROMMEMBER nope BYRADIUS 1 km",
        "GEORADIUSBYMEMBER g nope -1 km",
        "GEORADIUSBYMEMBER g nope 1 km STORE d",
        "GEORADIUSBYMEMBER_RO g STORE 1 km",
    ] {
        assert_reply(&mut db, &words(line, &[]), &error(NO_MEMBER));
    }
}

/// The rest of `georadiusGeneric`'s grammar: GEOSEARCH options in any order
/// (the last FROM*/BY* wins), redis's float error for a centre, its
/// FROM*/BY* presence errors, and GEOSEARCH-only keywords refused in the
/// legacy forms.
#[test]
fn the_option_grammar_is_redis_georadius_generic() {
    let mut db = sicily();
    let asc = array(&["Catania", "Palermo"]);
    for (line, want) in [
        (
            "GEOSEARCH g BYRADIUS 200 km FROMLONLAT 15 37 ASC",
            asc.clone(),
        ),
        (
            "GEOSEARCH g FROMLONLAT 13 38 FROMLONLAT 15 37 BYRADIUS 1 km BYRADIUS 200 km DESC ASC",
            asc.clone(),
        ),
        (
            "GEOSEARCH g FROMMEMBER Palermo BYRADIUS 1 km FROMMEMBER Catania",
            array(&["Catania"]),
        ),
        (
            "GEOSEARCH g FROMLONLAT 15 37 BYRADIUS 200 km COUNT 1 COUNT 2",
            asc,
        ),
        (
            "GEOSEARCH g FROMLONLAT abc 37 BYRADIUS 1 km",
            error("ERR value is not a valid float"),
        ),
        (
            "GEOSEARCH g FROMLONLAT 15 nan BYRADIUS 1 km",
            error("ERR value is not a valid float"),
        ),
        (
            "GEORADIUS g abc 37 1 km",
            error("ERR value is not a valid float"),
        ),
        ("GEORADIUS g 15 37 -1 km COUNT abc", error(NEG_RADIUS)),
        (
            "GEOSEARCH g BYRADIUS 1 km ASC WITHDIST",
            error("ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH"),
        ),
        (
            "GEOSEARCHSTORE d g BYRADIUS 1 km ASC STOREDIST",
            error(
                "ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCHSTORE",
            ),
        ),
        (
            "GEOSEARCH g FROMLONLAT 15 37 COUNT 1 ASC",
            error("ERR exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH"),
        ),
        (
            "GEOSEARCH g FROMLONLAT 15 37 FROMMEMBER Palermo BYRADIUS 1 km",
            error(SYNTAX),
        ),
        (
            "GEOSEARCH g FROMLONLAT 15 37 BYRADIUS 1 km BYBOX 1 1 km",
            error(SYNTAX),
        ),
        ("GEOSEARCH g FROMLONLAT 15 37 BYRADIUS 1", error(SYNTAX)),
        (
            "GEOSEARCH g FROMLONLAT 15 37 BYRADIUS 1 km COUNT",
            error(SYNTAX),
        ),
        ("GEORADIUS g 15 37 1 km FROMLONLAT 1 1", error(SYNTAX)),
        ("GEORADIUS g 15 37 1 km BYRADIUS 1 km", error(SYNTAX)),
        ("GEORADIUS_RO g 15 37 1 km STOREDIST x", error(SYNTAX)),
        ("GEORADIUS_RO g 15 37 1 km COUNT STORE", error(NOT_INT)),
    ] {
        assert_reply(&mut db, &words(line, &[]), &want);
    }
}

/// The step estimate is defined for every input and never loops unbounded:
/// zero (redis's own early return) and everything the parser now refuses
/// map to the finest step; an infinite range to the coarsest, which is what
/// redis's loop yields when it never runs.
#[test]
fn step_estimate_is_total_and_bounded() {
    for lat in [0.0, 45.0, 70.0, -85.0] {
        for range in [
            0.0,
            -0.0,
            -1.0,
            -1e300,
            f64::NAN,
            -f64::NAN,
            f64::NEG_INFINITY,
        ] {
            assert_eq!(
                estimate_steps_by_radius(range, lat),
                26,
                "range {range} lat {lat}"
            );
        }
        assert_eq!(estimate_steps_by_radius(f64::INFINITY, lat), 1, "lat {lat}");
        // The smallest positive double doubles ~1100 times before it
        // reaches the Mercator width; the answer is the finest step.
        assert_eq!(
            estimate_steps_by_radius(f64::from_bits(1), lat),
            26,
            "lat {lat}"
        );
        assert_eq!(estimate_steps_by_radius(f64::MAX, lat), 1, "lat {lat}");
    }
}

/// redis's `geohashEstimateStepsByRadius`, verbatim, for the inputs it is
/// defined on (a positive radius).
fn redis_estimate(range_meters: f64, lat: f64) -> u8 {
    const MERCATOR_MAX: f64 = 20037726.37;
    let mut range = range_meters;
    let mut step: i64 = 1;
    while range < MERCATOR_MAX {
        range *= 2.0;
        step += 1;
    }
    step -= 2;
    if !(-66.0..=66.0).contains(&lat) {
        step -= 1;
        if !(-80.0..=80.0).contains(&lat) {
            step -= 1;
        }
    }
    step.clamp(1, 26) as u8
}

/// The loop bound never changes an answer redis computes: every positive
/// radius from a millimetre to past the Mercator width, at the latitudes
/// where the polar widening kicks in.
#[test]
fn step_estimate_bound_matches_redis_for_every_positive_radius() {
    let mut range = 1e-3;
    while range < 1e9 {
        for lat in [0.0, 65.9, 66.0, 66.1, -66.1, 79.9, 80.1, -80.1, 85.05] {
            assert_eq!(
                estimate_steps_by_radius(range, lat),
                redis_estimate(range, lat),
                "range {range} lat {lat}"
            );
        }
        range *= 1.37;
    }
}
