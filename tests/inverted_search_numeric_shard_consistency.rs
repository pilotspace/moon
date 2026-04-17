//! Phase 152 Plan 07 W-01: cross-shard FieldFilter::NumericRange correctness.
//!
//! Verifies that a NUMERIC range filter (`@score:[5 15]`) on the same
//! fixture returns byte-identical sorted key sets when run on a 1-shard
//! server and a 4-shard server. This is the load-bearing test that the
//! `ShardMessage::InvertedSearch` wire format (introduced by Plan 06)
//! transports the `FieldFilter::NumericRange` variant across the SPSC
//! without corruption (B-02 inheritance proof).
//!
//! Requires two running Moon instances with `--features text-index`:
//!
//! ```bash
//! ./target/release/moon --port 6411 --shards 1 &
//! ./target/release/moon --port 6414 --shards 4 &
//! MOON_NUM_PORT_1SHARD=6411 MOON_NUM_PORT_4SHARD=6414 \
//!   cargo test --test inverted_search_numeric_shard_consistency \
//!       --features text-index -- --ignored
//! ```
//!
//! In ordinary `cargo test` runs the tests are `#[ignore]`d so CI does
//! not need live servers. The shard-consistency invariant is also
//! covered by the shell integration test `NUMERIC-07` in
//! `scripts/test-commands.sh`.

#![cfg(feature = "text-index")]

use redis::{Commands, RedisResult};

fn port_single() -> u16 {
    std::env::var("MOON_NUM_PORT_1SHARD")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(6411)
}

fn port_four() -> u16 {
    std::env::var("MOON_NUM_PORT_4SHARD")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(6414)
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
        .arg("score")
        .arg("NUMERIC")
        .query(conn)
        .expect("FT.CREATE");
    for i in 0..20i32 {
        let status = if i % 3 == 0 { "closed" } else { "open" };
        let score = i.to_string();
        let key = format!("k:{}", i);
        let _: i64 = conn
            .hset_multiple(&key, &[("status", status), ("score", score.as_str())])
            .expect("HSET");
    }
}

fn search_score_range_keys(conn: &mut redis::Connection, query: &str) -> Vec<String> {
    let result: Vec<redis::Value> = redis::cmd("FT.SEARCH")
        .arg("idx")
        .arg(query)
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
fn numeric_range_1_shard_vs_4_shard_identical() {
    let mut c1 = conn(port_single());
    let mut c4 = conn(port_four());

    seed(&mut c1);
    seed(&mut c4);

    let keys1 = search_score_range_keys(&mut c1, "@score:[5 15]");
    let keys4 = search_score_range_keys(&mut c4, "@score:[5 15]");

    assert!(
        !keys1.is_empty(),
        "1-shard returned no results — fixture or NUMERIC dispatch broken"
    );
    assert_eq!(
        keys1, keys4,
        "Cross-shard FieldFilter::NumericRange mismatch: 1-shard={:?} vs 4-shard={:?}",
        keys1, keys4
    );
    // Sanity: scores 5..=15 inclusive = 11 docs (fixture writes score=i for i in 0..20).
    assert_eq!(
        keys1.len(),
        11,
        "expected 11 docs with score in [5, 15], got {}",
        keys1.len()
    );
}

#[test]
#[ignore]
fn numeric_range_inverted_rejected_multi_shard() {
    let mut c = conn(port_four());
    seed(&mut c);
    let result: RedisResult<redis::Value> = redis::cmd("FT.SEARCH")
        .arg("idx")
        .arg("@score:[100 10]")
        .query(&mut c);
    match result {
        Err(e) => {
            let msg = format!("{}", e);
            assert!(
                msg.to_lowercase().contains("min > max"),
                "expected actionable inverted-range rejection, got: {}",
                msg
            );
        }
        Ok(ok) => panic!("expected rejection, got Ok({:?})", ok),
    }
}

#[test]
#[ignore]
fn numeric_range_exclusive_bound_multi_shard() {
    let mut c = conn(port_four());
    seed(&mut c);
    // (5 15] → scores 6..=15 = 10 docs
    let keys = search_score_range_keys(&mut c, "@score:[(5 15]");
    assert_eq!(
        keys.len(),
        10,
        "(5 15] exclusive-low inclusive-high should yield 10 docs, got {}",
        keys.len()
    );
}

#[test]
#[ignore]
fn numeric_range_infinity_multi_shard() {
    let mut c = conn(port_four());
    seed(&mut c);
    // [15 +inf] → scores 15..=19 = 5 docs
    let keys = search_score_range_keys(&mut c, "@score:[15 +inf]");
    assert_eq!(keys.len(), 5);
    // [-inf 4] → scores 0..=4 = 5 docs
    let keys = search_score_range_keys(&mut c, "@score:[-inf 4]");
    assert_eq!(keys.len(), 5);
}
