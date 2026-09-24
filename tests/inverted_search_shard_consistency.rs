//! Cross-shard TAG consistency for `FT.SEARCH` (moon#1219; Phase 152 Plan 06 W-01).
//!
//! The same asymmetric fixture is loaded into a `--shards 1` and a `--shards 4`
//! server, and every query must return the same total, key set and fields on
//! both — AND the set an in-test oracle computes from the fixture. The 4-shard
//! answer is a scatter (each shard evaluates the query AST over its keyspace
//! slice) plus `merge_text_results` (sum of per-shard totals, merged and
//! re-paged), so this is the guard for the cross-shard TAG path. The oracle is
//! what makes it a guard at all: two servers wrong the same way still agree.
//!
//! ## History
//!
//! Until moon#1219 this suite was `#[ignore]`d, expected two servers started by
//! hand on fixed ports, shared ONE index name that every test FLUSHDB'd and
//! FT.DROPINDEX'd while the others ran in parallel, and seeded with
//! `let _: i64 = hset_multiple(..)`, which receives HMSET's `+OK` and panicked
//! before any assertion. It guarded nothing.
//!
//! Now each test spawns its own pair of servers (`MOON_BIN`, else the binary
//! Cargo built for this run), uses its own index name and key prefix, checks
//! the seed through the `HSET` reply and a `*` count, and asserts how many
//! comparisons it ran (CONVENTIONS: a suite that ran 0 cases is not a pass).
//!
//! ```bash
//! MOON_BIN=/path/to/moon cargo test --test inverted_search_shard_consistency
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
    let dir = common::unique_test_dir(&format!("moon-ft-tag-consistency-s{shards}"));
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
    // Accepting a TCP connection is not answering commands yet.
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

const DOCS: u32 = 64;

/// One document of the fixture. Deliberately asymmetric (CONVENTIONS: equal
/// counts hide misalignment): status values have distinct, unequal counts, two
/// of them stored mixed-case, `labels` is multi-valued and absent on some docs.
struct Doc {
    status: &'static str,
    priority: &'static str,
    labels: Vec<&'static str>,
    title: String,
}

fn doc(i: u32) -> Doc {
    let status = if i.is_multiple_of(5) {
        "closed"
    } else if i.is_multiple_of(13) {
        "Pending"
    } else if i.is_multiple_of(3) {
        "BLOCKED"
    } else {
        "open"
    };
    let priority = match i % 4 {
        0 => "high",
        1 => "low",
        _ => "mid",
    };
    let mut labels = Vec::new();
    if i.is_multiple_of(3) {
        labels.push("bug");
    }
    if i % 5 == 1 {
        labels.push("ui");
    }
    if i % 6 == 2 {
        labels.push("perf");
    }
    let word = if i.is_multiple_of(2) {
        "alpha"
    } else {
        "gamma"
    };
    let extra = if i.is_multiple_of(7) { " beta" } else { "" };
    Doc {
        status,
        priority,
        labels,
        title: format!("{word}{extra} w{i}"),
    }
}

/// Distinct names per test: nothing one test does can reach another's data,
/// even if a future edit shares servers between tests.
struct Ns {
    index: String,
    prefix: String,
}

impl Ns {
    fn new(test: &str) -> Self {
        Self {
            index: format!("tagcons_{test}"),
            prefix: format!("tc:{test}:"),
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
        .arg("priority")
        .arg("TAG")
        .arg("labels")
        .arg("TAG")
        .arg("SEPARATOR")
        .arg(",")
        .query(c)
        .expect("FT.CREATE");
    assert_eq!(ok, "OK");
}

/// HSET one new document and check the reply: the number of fields ADDED. The
/// old seed typed this reply as `i64` against HMSET's `+OK` and panicked.
fn put(c: &mut redis::Connection, ns: &Ns, i: u32, d: &Doc) {
    let mut cmd = redis::cmd("HSET");
    cmd.arg(ns.key(i))
        .arg("title")
        .arg(&d.title)
        .arg("status")
        .arg(d.status)
        .arg("priority")
        .arg(d.priority);
    let mut fields = 3;
    if !d.labels.is_empty() {
        cmd.arg("labels").arg(d.labels.join(","));
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
    /// Keys in reply order.
    keys: Vec<String>,
    /// Returned hash fields per key, `__bm25_score` excluded (BM25 depends on
    /// per-shard length statistics, so it legitimately differs by shard count).
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

/// Every page of `LIMIT o 7` over `[0, total)` concatenated must be the full
/// answer exactly once: no duplicate and no missing key across page boundaries
/// (the coordinator re-pages the merged per-shard pages).
fn assert_pages_partition(c: &mut redis::Connection, ns: &Ns, query: &str, want: &[String]) {
    const PAGE: usize = 7;
    let mut seen = Vec::new();
    let mut offset = 0;
    while offset < want.len() + PAGE {
        let r = search(c, ns, query, offset, PAGE);
        assert_eq!(
            r.total as usize,
            want.len(),
            "{query:?} page {offset}: total"
        );
        assert_eq!(
            r.keys.len(),
            PAGE.min(want.len().saturating_sub(offset)),
            "{query:?} page at offset {offset}: size"
        );
        seen.extend(r.keys);
        offset += PAGE;
    }
    let unique: BTreeSet<&String> = seen.iter().collect();
    assert_eq!(
        unique.len(),
        seen.len(),
        "{query:?}: a key repeats across pages"
    );
    assert_eq!(
        sorted(&seen),
        want,
        "{query:?}: pages do not cover the answer"
    );
}

/// Instrument check: how many of a 4-shard server's shards own at least one of
/// `keys`. An answer living on one shard would let a broken merge pass.
fn shards_spanned(keys: &[String]) -> usize {
    keys.iter()
        .map(|k| moon::shard::dispatch::key_to_shard(k.as_bytes(), 4))
        .collect::<BTreeSet<_>>()
        .len()
}

/// The comparison every query goes through: shards 1 == shards 4 == oracle,
/// on the total, the key set and the returned fields. Returns 1 (ran count).
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

/// A query's expected-membership predicate over fixture doc numbers.
type Oracle = fn(u32) -> bool;

fn status(i: u32) -> String {
    doc(i).status.to_ascii_lowercase()
}

fn has_label(i: u32, l: &str) -> bool {
    doc(i).labels.contains(&l)
}

/// The TAG query matrix, each with its oracle. Returns the ran count.
fn run_tag_matrix(c1: &mut redis::Connection, c4: &mut redis::Connection, ns: &Ns) -> usize {
    let pr = |i: u32| doc(i).priority;
    let mut ran = 0;
    ran += check(c1, c4, ns, "@status:{open}", |i| status(i) == "open");
    ran += check(c1, c4, ns, "@status:{closed}", |i| status(i) == "closed");
    // Stored as `Pending` / `BLOCKED`; TAG values fold to lower case both ways.
    ran += check(c1, c4, ns, "@status:{pending}", |i| status(i) == "pending");
    ran += check(c1, c4, ns, "@status:{Blocked}", |i| status(i) == "blocked");
    ran += check(c1, c4, ns, "@status:{open|pending}", |i| {
        status(i) == "open" || status(i) == "pending"
    });
    ran += check(c1, c4, ns, "@status:{open} @priority:{high}", |i| {
        status(i) == "open" && pr(i) == "high"
    });
    ran += check(c1, c4, ns, "@status:{closed} | @priority:{low}", |i| {
        status(i) == "closed" || pr(i) == "low"
    });
    ran += check(c1, c4, ns, "@labels:{bug}", |i| has_label(i, "bug"));
    ran += check(c1, c4, ns, "@labels:{bug|perf} @priority:{mid}", |i| {
        (has_label(i, "bug") || has_label(i, "perf")) && pr(i) == "mid"
    });
    ran += check(
        c1,
        c4,
        ns,
        "@labels:{ui} (@status:{open} | @status:{blocked})",
        |i| has_label(i, "ui") && (status(i) == "open" || status(i) == "blocked"),
    );
    ran += check(c1, c4, ns, "alpha @status:{open}", |i| {
        i.is_multiple_of(2) && status(i) == "open"
    });
    ran += check(c1, c4, ns, "@title:beta @labels:{bug}", |i| {
        i.is_multiple_of(7) && has_label(i, "bug")
    });
    ran += check(c1, c4, ns, "@status:{nosuchvalue}", |_| false);
    ran
}

// ── tests ───────────────────────────────────────────────────────────────────

/// The fixture must be asymmetric for the comparisons to mean anything.
#[test]
fn fixture_is_asymmetric() {
    let mut counts: BTreeMap<String, u32> = BTreeMap::new();
    for i in 0..DOCS {
        *counts.entry(status(i)).or_default() += 1;
    }
    let distinct: BTreeSet<u32> = counts.values().copied().collect();
    assert_eq!(counts.len(), 4, "{counts:?}");
    assert_eq!(distinct.len(), 4, "status counts must differ: {counts:?}");
    assert!((0..DOCS).any(|i| doc(i).labels.len() >= 2));
    assert!((0..DOCS).any(|i| doc(i).labels.is_empty()));
}

#[test]
fn tag_filter_1_shard_vs_4_shard_identical() {
    let ns = Ns::new("matrix");
    let (s1, s4) = (start(1), start(4));
    let (mut c1, mut c4) = (seed(&s1, &ns), seed(&s4, &ns));
    let ran = run_tag_matrix(&mut c1, &mut c4, &ns);
    assert_eq!(ran, 13, "every TAG comparison must have run");
}

/// Re-writing documents moves them between TAG values on whichever shard owns
/// them; both shard counts must agree with the updated oracle afterwards.
#[test]
fn tag_filter_after_upserts_1_shard_vs_4_shard_identical() {
    let ns = Ns::new("upsert");
    let (s1, s4) = (start(1), start(4));
    let (mut c1, mut c4) = (seed(&s1, &ns), seed(&s4, &ns));
    // Every 4th doc becomes `closed` with the `perf` label only.
    for c in [&mut c1, &mut c4] {
        for i in (0..DOCS).step_by(4) {
            let _: i64 = redis::cmd("HSET")
                .arg(ns.key(i))
                .arg("status")
                .arg("closed")
                .arg("labels")
                .arg("perf")
                .query(c)
                .expect("HSET upsert");
        }
    }
    let st = |i: u32| {
        if i.is_multiple_of(4) {
            "closed".to_string()
        } else {
            status(i)
        }
    };
    let label = |i: u32, l: &str| {
        if i.is_multiple_of(4) {
            l == "perf"
        } else {
            has_label(i, l)
        }
    };
    let mut ran = 0;
    ran += check(&mut c1, &mut c4, &ns, "@status:{closed}", |i| {
        st(i) == "closed"
    });
    ran += check(&mut c1, &mut c4, &ns, "@status:{open}", |i| st(i) == "open");
    ran += check(&mut c1, &mut c4, &ns, "@labels:{perf}", |i| {
        label(i, "perf")
    });
    ran += check(&mut c1, &mut c4, &ns, "@labels:{bug}", |i| label(i, "bug"));
    ran += check(
        &mut c1,
        &mut c4,
        &ns,
        "@status:{closed} @priority:{high}",
        |i| st(i) == "closed" && doc(i).priority == "high",
    );
    assert_eq!(ran, 5);
}

/// Paging: the merged answer, walked page by page, is exactly the full answer
/// once at both shard counts — the coordinator's re-pagination of per-shard pages.
#[test]
fn tag_filter_pages_partition_the_answer_at_both_shard_counts() {
    let ns = Ns::new("paging");
    let (s1, s4) = (start(1), start(4));
    let (mut c1, mut c4) = (seed(&s1, &ns), seed(&s4, &ns));
    let queries: [(&str, Oracle); 3] = [
        ("@status:{open}", |i| status(i) == "open"),
        ("@labels:{bug|ui}", |i| {
            has_label(i, "bug") || has_label(i, "ui")
        }),
        ("alpha", |i| i.is_multiple_of(2)),
    ];
    let mut ran = 0;
    for (query, oracle) in queries {
        let want: Vec<String> = (0..DOCS)
            .filter(|&i| oracle(i))
            .map(|i| ns.key(i))
            .collect();
        let want = sorted(&want);
        assert!(want.len() > 7, "{query:?} must span several pages");
        assert_pages_partition(&mut c1, &ns, query, &want);
        assert_pages_partition(&mut c4, &ns, query, &want);
        ran += 1;
    }
    assert_eq!(ran, 3);
}

/// A mixed-case field name resolves to the declared field at every shard count.
#[test]
fn tag_filter_case_insensitive_field_multi_shard() {
    let ns = Ns::new("fieldcase");
    let (s1, s4) = (start(1), start(4));
    let (mut c1, mut c4) = (seed(&s1, &ns), seed(&s4, &ns));
    let ran = check(&mut c1, &mut c4, &ns, "@Status:{open}", |i| {
        status(i) == "open"
    }) + check(&mut c1, &mut c4, &ns, "@PRIORITY:{low}", |i| {
        doc(i).priority == "low"
    });
    assert_eq!(ran, 2);
}

/// `{a|b}` is a TAG union (the query AST superseded the v1 pre-parser that
/// refused it with "multi-tag OR"): it equals the union of the single-value
/// answers, and malformed TAG queries are refused identically at both shard
/// counts.
#[test]
fn tag_filter_multi_value_union_and_errors_multi_shard() {
    let ns = Ns::new("union");
    let (s1, s4) = (start(1), start(4));
    let (mut c1, mut c4) = (seed(&s1, &ns), seed(&s4, &ns));
    for c in [&mut c1, &mut c4] {
        let open = search(c, &ns, "@status:{open}", 0, 1000);
        let closed = search(c, &ns, "@status:{closed}", 0, 1000);
        let both = search(c, &ns, "@status:{open|closed}", 0, 1000);
        let union: Vec<String> = open.keys.iter().chain(&closed.keys).cloned().collect();
        assert!(!open.keys.is_empty() && !closed.keys.is_empty());
        assert_eq!(sorted(&both.keys), sorted(&union));
        assert_eq!(both.total, open.total + closed.total);
    }
    let mut errors = Vec::new();
    for c in [&mut c1, &mut c4] {
        for q in ["@status:{}", "@status:{open", "@nosuchfield:{open}"] {
            let e = redis::cmd("FT.SEARCH")
                .arg(&ns.index)
                .arg(q)
                .query::<redis::Value>(c)
                .expect_err(q);
            errors.push(format!("{q} -> {e}"));
        }
    }
    let (one, four) = errors.split_at(3);
    assert_eq!(one, four, "TAG errors differ between shard counts");
    assert!(one[0].contains("tag_filter_invalid"), "{}", one[0]);
    assert!(one[2].contains("unknown_field"), "{}", one[2]);
}
