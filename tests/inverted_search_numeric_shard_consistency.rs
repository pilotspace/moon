//! Cross-shard NUMERIC consistency for `FT.SEARCH` (moon#1219; Phase 152 Plan 07 W-01).
//!
//! The same asymmetric fixture is loaded into a `--shards 1` and a `--shards 4`
//! server, and every range query must return the same total, key set and
//! fields on both — AND the set an in-test oracle computes from the fixture.
//! The 4-shard answer is a scatter (each shard evaluates the query AST over its
//! keyspace slice) plus `merge_text_results`, so this guards the cross-shard
//! NUMERIC path. The oracle is what makes it a guard: two servers wrong the
//! same way still agree.
//!
//! ## History
//!
//! Until moon#1219 this suite was `#[ignore]`d, expected two hand-started
//! servers on fixed ports, shared ONE index name that every test FLUSHDB'd and
//! FT.DROPINDEX'd while the others ran in parallel, and seeded with
//! `let _: i64 = hset_multiple(..)`, which receives HMSET's `+OK` and panicked
//! before any assertion — it guarded nothing. Its inverted-range test also
//! pinned a `min > max` message the query AST has since replaced with the
//! frozen `numeric_filter_invalid` code (moon#691).
//!
//! Each test now spawns its own pair of servers (`MOON_BIN`, else the binary
//! Cargo built for this run), uses its own index name and key prefix, checks
//! the seed through the `HSET` reply and a `*` count, and asserts how many
//! comparisons it ran (CONVENTIONS: 0 ran is not a pass).
//!
//! ```bash
//! MOON_BIN=/path/to/moon cargo test --test inverted_search_numeric_shard_consistency
//! ```
#![cfg(feature = "text-index")]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

// ── servers ─────────────────────────────────────────────────────────────────

/// One spawned server, SIGKILLed (and its dir removed) on drop — including
/// when an assertion unwinds past it (moon#713).
struct Server {
    guard: common::ServerGuard,
    port: u16,
    shards: u32,
    dir: PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.guard.kill_now();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn start(shards: u32) -> Server {
    let dir = common::unique_test_dir(&format!("moon-ft-num-consistency-s{shards}"));
    std::fs::create_dir_all(&dir).expect("create server dir");
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "no",
                "--save",
                "",
                "--maxmemory",
                "0",
                "--disk-offload",
                "disable",
                "--disk-free-min-pct",
                "0",
                "--dir",
            ])
            .arg(&dir)
            .stdout(std::process::Stdio::null())
            .stderr(common::server_stderr(&dir))
            .spawn()
            .unwrap_or_else(|e| panic!("spawn {}: {e}", bin.display()))
    });
    let server = Server {
        guard: common::ServerGuard::new(child),
        port,
        shards,
        dir,
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(mut c) = try_conn(port)
            && redis::cmd("PING").query::<String>(&mut c).is_ok()
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "server (shards={shards}) on port {port} never answered PING"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    server
}

fn try_conn(port: u16) -> redis::RedisResult<redis::Connection> {
    redis::Client::open(format!("redis://127.0.0.1:{port}/"))?.get_connection()
}

fn conn(s: &Server) -> redis::Connection {
    try_conn(s.port).unwrap_or_else(|e| panic!("connect shards={}: {e}", s.shards))
}

// ── fixture ─────────────────────────────────────────────────────────────────

const DOCS: u32 = 72;

/// Deliberately asymmetric: `score` has duplicates, negatives and uneven
/// gaps; `price` is fractional, uses exponent notation for some docs and is
/// ABSENT on every 5th doc (absent must never match any range).
struct Doc {
    status: &'static str,
    score: i64,
    /// `(stored text, parsed value)`.
    price: Option<(String, f64)>,
    title: &'static str,
}

fn doc(i: u32) -> Doc {
    let status = if i.is_multiple_of(3) {
        "closed"
    } else {
        "open"
    };
    let score = i64::from((i * 7) % 23) - 5;
    let price = (!i.is_multiple_of(5)).then(|| {
        let v = f64::from(i) * 1.25 - 10.0;
        let text = if i % 4 == 1 {
            format!("{v:e}")
        } else {
            v.to_string()
        };
        (text, v)
    });
    let title = if i.is_multiple_of(2) {
        "alpha"
    } else {
        "gamma"
    };
    Doc {
        status,
        score,
        price,
        title,
    }
}

struct Ns {
    index: String,
    prefix: String,
}

impl Ns {
    fn new(test: &str) -> Self {
        Self {
            index: format!("numcons_{test}"),
            prefix: format!("nc:{test}:"),
        }
    }
    fn key(&self, i: u32) -> String {
        format!("{}{i}", self.prefix)
    }
}

fn create_index(c: &mut redis::Connection, ns: &Ns) {
    let ok: String = redis::cmd("FT.CREATE")
        .arg(&ns.index)
        .arg("ON")
        .arg("HASH")
        .arg("PREFIX")
        .arg(1)
        .arg(&ns.prefix)
        .arg("SCHEMA")
        .arg("title")
        .arg("TEXT")
        .arg("status")
        .arg("TAG")
        .arg("score")
        .arg("NUMERIC")
        .arg("price")
        .arg("NUMERIC")
        .query(c)
        .expect("FT.CREATE");
    assert_eq!(ok, "OK");
}

/// HSET one new document and check the number of fields ADDED (the old seed
/// typed HMSET's `+OK` as `i64` and panicked here).
fn put(c: &mut redis::Connection, ns: &Ns, i: u32, d: &Doc) {
    let mut cmd = redis::cmd("HSET");
    cmd.arg(ns.key(i))
        .arg("title")
        .arg(d.title)
        .arg("status")
        .arg(d.status)
        .arg("score")
        .arg(d.score);
    let mut fields = 3;
    if let Some((text, _)) = &d.price {
        cmd.arg("price").arg(text);
        fields += 1;
    }
    let added: i64 = cmd.query(c).expect("HSET");
    assert_eq!(
        added,
        fields,
        "HSET {} added {added} of {fields} fields",
        ns.key(i)
    );
}

fn seed(s: &Server, ns: &Ns) -> redis::Connection {
    let mut c = conn(s);
    create_index(&mut c, ns);
    for i in 0..DOCS {
        put(&mut c, ns, i, &doc(i));
    }
    let all = search(&mut c, ns, "*", 0, 1000);
    assert_eq!(
        shards_spanned(&all.keys),
        4,
        "fixture must cover every shard"
    );
    assert_eq!(
        all.total,
        i64::from(DOCS),
        "shards={}: `*` must see every seeded doc",
        s.shards
    );
    c
}

// ── replies ─────────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq)]
struct Reply {
    total: i64,
    keys: Vec<String>,
    /// Returned hash fields per key, `__bm25_score` excluded (it depends on
    /// per-shard length statistics).
    fields: BTreeMap<String, BTreeMap<String, String>>,
}

fn text(v: &redis::Value) -> Option<String> {
    match v {
        redis::Value::BulkString(b) => Some(String::from_utf8_lossy(b).into_owned()),
        redis::Value::SimpleString(s) => Some(s.clone()),
        _ => None,
    }
}

fn search(c: &mut redis::Connection, ns: &Ns, query: &str, offset: usize, count: usize) -> Reply {
    let items: Vec<redis::Value> = redis::cmd("FT.SEARCH")
        .arg(&ns.index)
        .arg(query)
        .arg("LIMIT")
        .arg(offset)
        .arg(count)
        .query(c)
        .unwrap_or_else(|e| panic!("FT.SEARCH {query:?}: {e}"));
    let total = match items.first() {
        Some(redis::Value::Int(n)) => *n,
        other => panic!("FT.SEARCH {query:?}: reply[0] is not an integer: {other:?}"),
    };
    let mut keys = Vec::new();
    let mut fields = BTreeMap::new();
    for pair in items[1..].chunks(2) {
        let key = text(&pair[0]).unwrap_or_else(|| panic!("{query:?}: key {:?}", pair[0]));
        let mut map = BTreeMap::new();
        if let Some(redis::Value::Array(kv)) = pair.get(1) {
            for f in kv.chunks(2) {
                if let (Some(k), Some(v)) = (text(&f[0]), f.get(1).and_then(text))
                    && k != "__bm25_score"
                {
                    map.insert(k, v);
                }
            }
        }
        fields.insert(key.clone(), map);
        keys.push(key);
    }
    Reply {
        total,
        keys,
        fields,
    }
}

fn sorted(keys: &[String]) -> Vec<String> {
    let mut k = keys.to_vec();
    k.sort();
    k
}

/// Instrument check: how many of a 4-shard server's shards own at least one of
/// `keys`. An answer living on one shard would let a broken merge pass.
fn shards_spanned(keys: &[String]) -> usize {
    keys.iter()
        .map(|k| moon::shard::dispatch::key_to_shard(k.as_bytes(), 4))
        .collect::<BTreeSet<_>>()
        .len()
}

/// shards 1 == shards 4 == oracle, on the total, the key set and the returned
/// fields. Returns 1 (ran count).
fn check(
    c1: &mut redis::Connection,
    c4: &mut redis::Connection,
    ns: &Ns,
    query: &str,
    oracle: impl Fn(u32) -> bool,
) -> usize {
    let want: Vec<String> = (0..DOCS)
        .filter(|&i| oracle(i))
        .map(|i| ns.key(i))
        .collect();
    let want = sorted(&want);
    if want.len() >= 4 {
        assert!(shards_spanned(&want) >= 2, "{query:?}: answer on one shard");
    }
    let r1 = search(c1, ns, query, 0, 1000);
    let r4 = search(c4, ns, query, 0, 1000);
    assert_eq!(
        r1.total as usize,
        want.len(),
        "shards=1 {query:?}: total vs oracle"
    );
    assert_eq!(sorted(&r1.keys), want, "shards=1 {query:?}: keys vs oracle");
    assert_eq!(
        r4.total, r1.total,
        "{query:?}: total differs, shards 1 vs 4"
    );
    assert_eq!(
        sorted(&r4.keys),
        sorted(&r1.keys),
        "{query:?}: key set differs, shards 1 vs 4"
    );
    assert_eq!(
        r4.fields, r1.fields,
        "{query:?}: returned fields differ, shards 1 vs 4"
    );
    1
}

fn score(i: u32) -> i64 {
    doc(i).score
}

fn price(i: u32) -> Option<f64> {
    doc(i).price.map(|(_, v)| v)
}

fn price_in(i: u32, lo: f64, hi: f64) -> bool {
    price(i).is_some_and(|p| lo <= p && p <= hi)
}

/// The NUMERIC query matrix, each with its oracle. Returns the ran count.
fn run_numeric_matrix(c1: &mut redis::Connection, c4: &mut redis::Connection, ns: &Ns) -> usize {
    let mut ran = 0;
    ran += check(c1, c4, ns, "@score:[5 15]", |i| {
        (5..=15).contains(&score(i))
    });
    ran += check(c1, c4, ns, "@score:[(5 15]", |i| {
        (6..=15).contains(&score(i))
    });
    ran += check(c1, c4, ns, "@score:[5 (15]", |i| {
        (5..15).contains(&score(i))
    });
    ran += check(c1, c4, ns, "@score:[(5 (15]", |i| {
        (6..15).contains(&score(i))
    });
    ran += check(c1, c4, ns, "@score:[3 3]", |i| score(i) == 3);
    ran += check(c1, c4, ns, "@score:[-inf 0]", |i| score(i) <= 0);
    ran += check(c1, c4, ns, "@score:[10 +inf]", |i| score(i) >= 10);
    ran += check(c1, c4, ns, "@score:[-inf +inf]", |_| true);
    ran += check(c1, c4, ns, "@price:[-2.5 20.75]", |i| {
        price_in(i, -2.5, 20.75)
    });
    ran += check(c1, c4, ns, "@price:[1e1 1e2]", |i| price_in(i, 10.0, 100.0));
    // Absent `price` never matches, even the widest range.
    ran += check(c1, c4, ns, "@price:[-inf +inf]", |i| price(i).is_some());
    ran += check(c1, c4, ns, "@score:[5 15] @status:{open}", |i| {
        (5..=15).contains(&score(i)) && doc(i).status == "open"
    });
    ran += check(c1, c4, ns, "@score:[-inf 0] | @price:[40 +inf]", |i| {
        score(i) <= 0 || price_in(i, 40.0, f64::INFINITY)
    });
    ran += check(c1, c4, ns, "@score:[5 15] alpha", |i| {
        (5..=15).contains(&score(i)) && i.is_multiple_of(2)
    });
    ran += check(c1, c4, ns, "@score:[0 10] @price:[0 50]", |i| {
        (0..=10).contains(&score(i)) && price_in(i, 0.0, 50.0)
    });
    ran += check(c1, c4, ns, "@score:[1000 2000]", |_| false);
    ran
}

// ── tests ───────────────────────────────────────────────────────────────────

/// The fixture must be asymmetric for the comparisons to mean anything.
#[test]
fn fixture_is_asymmetric() {
    let scores: BTreeSet<i64> = (0..DOCS).map(score).collect();
    assert!(
        scores.len() > 10 && scores.len() < DOCS as usize,
        "duplicates + spread"
    );
    assert!(scores.iter().any(|&s| s < 0) && scores.iter().any(|&s| s > 15));
    let with_price = (0..DOCS).filter(|&i| price(i).is_some()).count();
    assert!(with_price > 0 && with_price < DOCS as usize);
    assert!((0..DOCS).any(|i| doc(i).price.is_some_and(|(t, _)| t.contains('e'))));
    let in_5_15 = (0..DOCS).filter(|&i| (5..=15).contains(&score(i))).count();
    let in_ex = (0..DOCS).filter(|&i| (6..15).contains(&score(i))).count();
    assert!(
        in_5_15 > in_ex && in_ex > 0,
        "exclusive bounds must be observable"
    );
}

#[test]
fn numeric_range_1_shard_vs_4_shard_identical() {
    let ns = Ns::new("matrix");
    let (s1, s4) = (start(1), start(4));
    let (mut c1, mut c4) = (seed(&s1, &ns), seed(&s4, &ns));
    let ran = run_numeric_matrix(&mut c1, &mut c4, &ns);
    assert_eq!(ran, 16, "every NUMERIC comparison must have run");
}

/// Re-writing a NUMERIC value moves the doc between ranges on the shard that
/// owns it (per-field upsert: `price` untouched); both shard counts must agree
/// with the updated oracle.
#[test]
fn numeric_range_after_upserts_1_shard_vs_4_shard_identical() {
    let ns = Ns::new("upsert");
    let (s1, s4) = (start(1), start(4));
    let (mut c1, mut c4) = (seed(&s1, &ns), seed(&s4, &ns));
    let new_score = |i: u32| {
        if i % 3 == 1 {
            100 + i64::from(i)
        } else {
            score(i)
        }
    };
    for c in [&mut c1, &mut c4] {
        for i in (0..DOCS).filter(|i| i % 3 == 1) {
            let _: i64 = redis::cmd("HSET")
                .arg(ns.key(i))
                .arg("score")
                .arg(new_score(i))
                .query(c)
                .expect("HSET upsert");
        }
    }
    let mut ran = 0;
    ran += check(&mut c1, &mut c4, &ns, "@score:[5 15]", |i| {
        (5..=15).contains(&new_score(i))
    });
    ran += check(&mut c1, &mut c4, &ns, "@score:[100 +inf]", |i| {
        new_score(i) >= 100
    });
    ran += check(&mut c1, &mut c4, &ns, "@price:[-inf +inf]", |i| {
        price(i).is_some()
    });
    ran += check(
        &mut c1,
        &mut c4,
        &ns,
        "@score:[(100 150] @price:[0 +inf]",
        |i| new_score(i) > 100 && new_score(i) <= 150 && price_in(i, 0.0, f64::INFINITY),
    );
    assert_eq!(ran, 4);
}

/// Paging over a range answer at both shard counts covers it exactly once.
#[test]
fn numeric_range_pages_partition_the_answer_at_both_shard_counts() {
    const PAGE: usize = 5;
    let ns = Ns::new("paging");
    let (s1, s4) = (start(1), start(4));
    let (mut c1, mut c4) = (seed(&s1, &ns), seed(&s4, &ns));
    let query = "@score:[-inf 12]";
    let want: Vec<String> = (0..DOCS)
        .filter(|&i| score(i) <= 12)
        .map(|i| ns.key(i))
        .collect();
    let want = sorted(&want);
    assert!(want.len() > 4 * PAGE);
    let mut ran = 0;
    for c in [&mut c1, &mut c4] {
        let mut seen = Vec::new();
        let mut offset = 0;
        while offset < want.len() + PAGE {
            let r = search(c, &ns, query, offset, PAGE);
            assert_eq!(r.total as usize, want.len(), "page {offset}: total");
            assert_eq!(r.keys.len(), PAGE.min(want.len().saturating_sub(offset)));
            seen.extend(r.keys);
            offset += PAGE;
        }
        let unique: BTreeSet<&String> = seen.iter().collect();
        assert_eq!(unique.len(), seen.len(), "a key repeats across pages");
        assert_eq!(sorted(&seen), want);
        ran += 1;
    }
    assert_eq!(ran, 2);
}

/// Malformed ranges are refused with the frozen code, byte-identically at both
/// shard counts (the old test pinned a `min > max` message the AST replaced).
#[test]
fn numeric_range_errors_identical_multi_shard() {
    let ns = Ns::new("errors");
    let (s1, s4) = (start(1), start(4));
    let (mut c1, mut c4) = (seed(&s1, &ns), seed(&s4, &ns));
    let queries = [
        "@score:[100 10]",
        "@score:[abc 10]",
        "@score:[1 2 3]",
        "@score:[1 2",
    ];
    let mut errors = Vec::new();
    for c in [&mut c1, &mut c4] {
        for q in queries {
            let e = redis::cmd("FT.SEARCH")
                .arg(&ns.index)
                .arg(q)
                .query::<redis::Value>(c)
                .expect_err(q);
            errors.push(format!("{q} -> {e}"));
        }
    }
    let (one, four) = errors.split_at(queries.len());
    assert_eq!(one, four, "NUMERIC errors differ between shard counts");
    for e in &one[..3] {
        assert!(e.contains("numeric_filter_invalid"), "{e}");
    }
    assert!(one[3].contains("syntax_error"), "{}", one[3]);
}
