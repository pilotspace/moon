//! GEO argument validation on a live server (moon#1227 review, refs
//! moon#1172): every search form rejects a negative, NaN or non-numeric
//! radius, width or height with redis 7.0.15's exact error text, on keys
//! spread over several shards, inside MULTI/EXEC and from Lua — and the
//! server keeps answering PING after each rejected command.
//!
//! Run alone with:
//!   MOON_BIN=/path/to/moon cargo test --test geo_input_validation

#![allow(clippy::unwrap_used)]

mod common;

use std::process::{Command, Stdio};

use common::{Conn, ServerGuard};

/// A spawned server and its data dir, both gone on drop (panics included).
struct Moon {
    guard: ServerGuard,
    port: u16,
    dir: std::path::PathBuf,
}

impl Drop for Moon {
    fn drop(&mut self) {
        self.guard.kill_now();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn spawn_moon(shards: &str) -> Moon {
    let bin = common::find_moon_binary();
    let dir = common::unique_test_dir("geo-input-validation");
    let d = dir.clone();
    let (guard, port) = common::spawn_listening_guarded(|port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                shards,
                "--admin-port",
                "0",
                "--appendonly",
                "no",
                "--save",
                "",
                "--disk-free-min-pct",
                "0",
                "--dir",
                d.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(common::server_stderr(&d))
            .spawn()
            .expect("spawn moon")
    });
    Moon { guard, port, dir }
}

const NEG_RADIUS: &str = "-ERR radius cannot be negative\r\n";
const NUM_RADIUS: &str = "-ERR need numeric radius\r\n";
const NEG_BOX: &str = "-ERR height or width cannot be negative\r\n";
const NUM_WIDTH: &str = "-ERR need numeric width\r\n";
const NUM_HEIGHT: &str = "-ERR need numeric height\r\n";

/// Keys whose hash slots land on different shards of a 4-shard server, so
/// both the connection's own shard and remote ones (SPSC `Execute` on the
/// mutable dispatch path) parse the command.
const KEYS: [&str; 4] = ["geo:a", "geo:b", "geo:c", "geo:d"];

fn seed(c: &mut Conn) {
    for key in KEYS {
        let r = c.send(&[
            "GEOADD",
            key,
            "13.361389",
            "38.115556",
            "Palermo",
            "15.087269",
            "37.502669",
            "Catania",
        ]);
        assert_eq!(r, ":2\r\n", "GEOADD {key}");
    }
}

/// Split a space-separated command template into words, replacing each
/// placeholder word (`K` key, `R` radius, `W`/`H` box sides).
fn words<'a>(template: &'a str, subst: &[(&str, &'a str)]) -> Vec<&'a str> {
    template
        .split(' ')
        .map(|w| subst.iter().find(|(k, _)| *k == w).map_or(w, |(_, v)| *v))
        .collect()
}

fn assert_rejected_then_ping(c: &mut Conn, cmd: &[&str], want: &str) {
    let got = c.send(cmd);
    assert_eq!(got, want, "{}", cmd.join(" "));
    assert_eq!(
        c.send(&["PING"]),
        "+PONG\r\n",
        "PING after: {}",
        cmd.join(" ")
    );
}

#[test]
fn every_geo_search_form_rejects_bad_shapes_with_redis_errors() {
    let moon = spawn_moon("4");
    let mut c = Conn::open(moon.port);
    seed(&mut c);

    let radii: [(&str, &str); 5] = [
        ("-1", NEG_RADIUS),
        ("-inf", NEG_RADIUS),
        ("nan", NUM_RADIUS),
        ("abc", NUM_RADIUS),
        ("1e400", NUM_RADIUS),
    ];
    let radius_forms = [
        "GEOSEARCH K FROMLONLAT 15 37 BYRADIUS R km",
        "GEOSEARCH K FROMMEMBER Palermo BYRADIUS R km ASC",
        "GEOSEARCHSTORE K K FROMLONLAT 15 37 BYRADIUS R km",
        "GEORADIUS K 15 37 R km",
        "GEORADIUS K 15 37 R km STORE K",
        "GEORADIUS_RO K 15 37 R km WITHDIST",
        "GEORADIUSBYMEMBER K Palermo R km",
        "GEORADIUSBYMEMBER K Palermo R km STOREDIST K",
        "GEORADIUSBYMEMBER_RO K Palermo R km COUNT 1",
    ];
    let box_forms = [
        "GEOSEARCH K FROMLONLAT 15 37 BYBOX W H km",
        "GEOSEARCHSTORE K K FROMMEMBER Catania BYBOX W H m",
    ];
    for key in KEYS {
        for (r, want) in radii {
            for form in radius_forms {
                let cmd = words(form, &[("K", key), ("R", r)]);
                assert_rejected_then_ping(&mut c, &cmd, want);
            }
        }
        for (w, h, want) in [
            ("-1", "5", NEG_BOX),
            ("5", "-inf", NEG_BOX),
            ("nan", "5", NUM_WIDTH),
            ("5", "nan", NUM_HEIGHT),
        ] {
            for form in box_forms {
                let cmd = words(form, &[("K", key), ("W", w), ("H", h)]);
                assert_rejected_then_ping(&mut c, &cmd, want);
            }
        }
        // The rejected STOREs left the source set alone.
        assert_eq!(c.send(&["ZCARD", key]), ":2\r\n", "{key} was overwritten");
    }
}

/// The same checks through MULTI/EXEC and a Lua script, the other two
/// callers of the mutable dispatch path.
#[test]
fn transactions_and_scripts_get_the_same_errors() {
    let moon = spawn_moon("1");
    let mut c = Conn::open(moon.port);
    seed(&mut c);

    assert_eq!(c.send(&["MULTI"]), "+OK\r\n");
    for form in [
        "GEOSEARCH geo:a FROMLONLAT 15 37 BYRADIUS -1 km",
        "GEORADIUS geo:a 15 37 nan km STORE geo:dst",
        "GEOSEARCH geo:a FROMLONLAT 15 37 BYBOX 1 -1 km",
    ] {
        assert_eq!(c.send(&words(form, &[])), "+QUEUED\r\n", "{form}");
    }
    assert_eq!(
        c.send(&["EXEC"]),
        format!("*3\r\n{NEG_RADIUS}{NUM_RADIUS}{NEG_BOX}")
    );
    assert_eq!(c.send(&["PING"]), "+PONG\r\n");

    let script = "return redis.pcall('GEORADIUS', KEYS[1], '15', '37', ARGV[1], 'km')";
    for (r, want) in [
        ("-1", NEG_RADIUS),
        ("-inf", NEG_RADIUS),
        ("nan", NUM_RADIUS),
    ] {
        assert_eq!(
            c.send(&["EVAL", script, "1", "geo:a", r]),
            want,
            "EVAL radius {r}"
        );
        assert_eq!(c.send(&["PING"]), "+PONG\r\n");
    }
}

/// The accepted edges answer like redis 7.0.15: a zero radius or box finds
/// nothing away from a member, an infinite one finds every member.
#[test]
fn zero_and_infinite_shapes_answer_like_redis() {
    let moon = spawn_moon("1");
    let mut c = Conn::open(moon.port);
    seed(&mut c);
    let both = "*2\r\n$7\r\nPalermo\r\n$7\r\nCatania\r\n";
    for (form, want) in [
        ("GEOSEARCH geo:a FROMLONLAT 15 37 BYRADIUS 0 km", "*0\r\n"),
        ("GEOSEARCH geo:a FROMLONLAT 15 37 BYRADIUS -0 km", "*0\r\n"),
        ("GEOSEARCH geo:a FROMLONLAT 15 37 BYBOX 0 0 km", "*0\r\n"),
        ("GEOSEARCH geo:a FROMLONLAT 15 37 BYRADIUS inf km", both),
        ("GEOSEARCH geo:a FROMLONLAT 15 37 BYBOX inf inf km", both),
        ("GEORADIUS geo:a 15 37 inf km", both),
    ] {
        assert_eq!(c.send(&words(form, &[])), want, "{form}");
        assert_eq!(c.send(&["PING"]), "+PONG\r\n");
    }
}
