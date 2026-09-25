//! WS7 / moon#1178: counters moved off the per-event registry path must still
//! come out of `/metrics` with the same names, labels and values.
//!
//! The per-event cost itself is proven red/green in the lib
//! (`admin::metrics_setup::tests_1178`); this suite is the wire-level parity
//! guard for the scrape-time publish, and is green on `ae21476` too — which is
//! the point: nothing a scraper sees may change.
//!
//! Run: `MOON_BIN=/path/to/moon cargo test --test perf_ws7_metrics`

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;
mod perf_ws7_support;

use perf_ws7_support as ws7;

fn run_workload_and_scrape(shards: &str) -> String {
    let srv = ws7::spawn("ws7-metrics", shards, &[]);
    let mut a = ws7::conn(srv.port);
    let mut b = ws7::conn(srv.port);
    for i in 0..20 {
        assert_eq!(a.send(&["SET", &format!("m:{i}"), "v"]), "+OK\r\n");
    }
    for i in 0..20 {
        assert_eq!(a.send(&["GET", &format!("m:{i}")]), "$1\r\nv\r\n");
    }
    for i in 0..10 {
        assert_eq!(b.send(&["GET", &format!("missing:{i}")]), "$-1\r\n");
    }
    // A mixed pipeline: the old one-entry handle cache thrashed on this.
    let mut cmds: Vec<Vec<String>> = Vec::new();
    for i in 0..5 {
        cmds.push(vec![
            "HSET".into(),
            format!("h:{i}"),
            "f".into(),
            "1".into(),
        ]);
        cmds.push(vec!["INCR".into(), format!("c:{i}")]);
        cmds.push(vec!["GET".into(), format!("m:{i}")]);
    }
    let refs: Vec<Vec<&str>> = cmds
        .iter()
        .map(|c| c.iter().map(String::as_str).collect())
        .collect();
    let slices: Vec<&[&str]> = refs.iter().map(Vec::as_slice).collect();
    let _ = b.pipeline(&slices);
    for _ in 0..4 {
        assert_eq!(a.send(&["PUBLISH", "news", "hi"]), ":0\r\n");
    }
    ws7::scrape(srv.admin_port)
}

fn check(shards: &str) {
    let body = run_workload_and_scrape(shards);
    let get = |name: &str, labels: &[&str]| ws7::sample(&body, name, labels);
    assert_eq!(
        get("moon_commands_total", &["cmd=\"get\""]),
        35,
        "moon_commands_total{{cmd=\"get\"}}\n{body}"
    );
    assert_eq!(get("moon_commands_total", &["cmd=\"set\""]), 20);
    assert_eq!(get("moon_commands_total", &["cmd=\"hset\""]), 5);
    assert_eq!(get("moon_commands_total", &["cmd=\"incr\""]), 5);
    assert!(get("moon_keyspace_hits_total", &[]) >= 25, "{body}");
    assert!(get("moon_keyspace_misses_total", &[]) >= 10, "{body}");
    let paths: u64 = ["local", "local_inline", "cross_spsc", "cross_read_fast"]
        .iter()
        .map(|p| get("moon_dispatch_path_total", &[&format!("path=\"{p}\"")]))
        .sum();
    assert!(
        paths >= 50,
        "moon_dispatch_path_total must count the keyed commands: {paths}\n{body}"
    );
    // Never-touched series stay absent (an untouched counter was never
    // emitted), and no zero-valued command series appears.
    assert!(
        !body.contains("cmd=\"zadd\""),
        "a command never run must not appear: {body}"
    );
}

#[test]
fn metrics_names_labels_and_values_are_unchanged_1_shard() {
    check("1");
}

#[test]
fn metrics_names_labels_and_values_are_unchanged_2_shards() {
    check("2");
}
