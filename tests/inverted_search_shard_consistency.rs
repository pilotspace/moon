//! Phase 152 Plan 06 W-01: cross-shard FieldFilter correctness.
//!
//! Verifies that a TAG filter (`@status:{open}`) on the same fixture
//! returns byte-identical sorted key sets when run on a 1-shard server
//! and a 4-shard server. This is the load-bearing test that the
//! `ShardMessage::InvertedSearch` wire format transports the
//! `FieldFilter` across the SPSC without corruption (B-02 proof).
//!
//! Requires two running Moon instances with `--features text-index`:
//!
//! ```bash
//! ./target/release/moon --port 6401 --shards 1 &
//! ./target/release/moon --port 6404 --shards 4 &
//! MOON_PORT_1SHARD=6401 MOON_PORT_4SHARD=6404 \
//!   cargo test --test inverted_search_shard_consistency --features text-index -- --ignored
//! ```
//!
//! In ordinary `cargo test` runs the tests are `#[ignore]`d so CI does
//! not need live servers. The shard-consistency invariant is also
//! covered by the shell integration test `TAG-06` in
//! `scripts/test-commands.sh`.

#![cfg(feature = "text-index")]

use redis::{Commands, RedisResult};

fn port_single() -> u16 {
    std::env::var("MOON_PORT_1SHARD")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(6401)
}

fn port_four() -> u16 {
    std::env::var("MOON_PORT_4SHARD")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(6404)
}

fn conn(port: u16) -> redis::Connection {
    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut c = client.get_connection().unwrap();
    // FLUSHDB for isolation (doesn't drop FT.* indexes on Moon; FT.DROPINDEX below).
    let _: RedisResult<String> = redis::cmd("FLUSHDB").query(&mut c);
    let _: RedisResult<String> = redis::cmd("FT.DROPINDEX").arg("idx").query(&mut c);
    c
}

fn seed(conn: &mut redis::Connection) {
    // Identical fixture on both servers: 20 keys. Shard-assignment is hash-based,
    // so the 4-shard server spreads these across shards naturally.
    let _: String = redis::cmd("FT.CREATE")
        .arg("idx")
        .arg("ON")
        .arg("HASH")
        .arg("PREFIX")
        .arg("1")
        .arg("k:")
        .arg("SCHEMA")
        .arg("status")
        .arg("TAG")
        .arg("priority")
        .arg("TAG")
        .query(conn)
        .expect("FT.CREATE");
    for i in 0..20i32 {
        let status = if i % 3 == 0 { "closed" } else { "open" };
        let priority = if i % 2 == 0 { "high" } else { "low" };
        let key = format!("k:{}", i);
        let _: i64 = conn
            .hset_multiple(&key, &[("status", status), ("priority", priority)])
            .expect("HSET");
    }
}

fn search_open_keys(conn: &mut redis::Connection) -> Vec<String> {
    let result: Vec<redis::Value> = redis::cmd("FT.SEARCH")
        .arg("idx")
        .arg("@status:{open}")
        .arg("LIMIT")
        .arg(0)
        .arg(100)
        .query(conn)
        .expect("FT.SEARCH");
    let mut out = Vec::new();
    for v in &result {
        if let redis::Value::BulkString(bytes) = v {
            let s = String::from_utf8_lossy(bytes).to_string();
            if s.starts_with("k:") {
                out.push(s);
            }
        }
    }
    out.sort();
    out
}

#[test]
#[ignore]
fn tag_filter_1_shard_vs_4_shard_identical() {
    let mut c1 = conn(port_single());
    let mut c4 = conn(port_four());

    seed(&mut c1);
    seed(&mut c4);

    let keys1 = search_open_keys(&mut c1);
    let keys4 = search_open_keys(&mut c4);

    assert!(
        !keys1.is_empty(),
        "1-shard returned no results — fixture or TAG dispatch broken"
    );
    assert_eq!(
        keys1, keys4,
        "Cross-shard FieldFilter mismatch: 1-shard={:?} vs 4-shard={:?}",
        keys1, keys4
    );
}

#[test]
#[ignore]
fn tag_filter_case_insensitive_field_multi_shard() {
    let mut c = conn(port_four());
    seed(&mut c);

    let result: Vec<redis::Value> = redis::cmd("FT.SEARCH")
        .arg("idx")
        .arg("@Status:{open}")
        .arg("LIMIT")
        .arg(0)
        .arg(100)
        .query(&mut c)
        .expect("FT.SEARCH mixed-case field");

    let count = result
        .iter()
        .filter(|v| matches!(v, redis::Value::BulkString(b) if String::from_utf8_lossy(b).starts_with("k:")))
        .count();
    assert!(
        count > 0,
        "mixed-case field @Status:{{open}} must resolve canonical `status`"
    );
}

#[test]
#[ignore]
fn tag_filter_multi_tag_or_rejected() {
    let mut c = conn(port_four());
    seed(&mut c);

    let result: RedisResult<redis::Value> = redis::cmd("FT.SEARCH")
        .arg("idx")
        .arg("@status:{open|closed}")
        .query(&mut c);
    match result {
        Err(e) => {
            let msg = format!("{}", e);
            assert!(
                msg.to_lowercase().contains("multi-tag or"),
                "expected actionable multi-tag OR rejection, got: {}",
                msg
            );
        }
        Ok(ok) => panic!("expected rejection, got Ok({:?})", ok),
    }
}
