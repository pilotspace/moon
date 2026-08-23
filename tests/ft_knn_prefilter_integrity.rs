//! FT.SEARCH KNN-prefilter integrity: moon#648 (silent degradation) and
//! moon#664 (remote process abort).
//!
//! `FT.SEARCH` has two numeric-range parsers. The full query grammar
//! (`src/text/query/parse.rs`) handles `(`-prefixed exclusive bounds and
//! rejects an inverted range. The KNN-prefilter grammar
//! (`src/command/vector_search/ft_search/parse.rs`) did neither:
//!
//!   - an unparseable filter returned `None` for the WHOLE expression, and the
//!     caller read `None` as "no filter" and ran an UNFILTERED KNN. The query
//!     returned more rows than the caller scoped, with no error and no metric
//!     (moon#648). A filter that has silently stopped filtering is
//!     indistinguishable from one that legitimately matched everything.
//!   - an inverted range reached `BTreeMap::range(min..=max)`, which panics by
//!     contract when start > end, and Moon's shard-panic policy aborts the
//!     whole process (moon#664).
//!
//! These run against a spawned server rather than an in-process listener
//! because moon#664's assertion is about the PROCESS: a per-connection error
//! and a process abort look identical from a client socket that just got
//! disconnected. The test has to outlive the query and ask the OS.
//!
//! Run: cargo build --release && cargo test --release \
//!        --test ft_knn_prefilter_integrity -- --ignored --test-threads=1

mod common;

use std::process::{Child, Command};
use std::time::Duration;

/// 16 ASCII bytes = one FLOAT32 DIM 4 vector, so query blobs travel through
/// the plain-text RESP helper in `tests/common` without a binary-safe path.
const VEC_A: &str = "ABCDEFGHIJKLMNOP";

fn spawn_moon(dir: &std::path::Path) -> (Child, u16) {
    common::spawn_listening(|port| {
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                "1",
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
            .env("RUST_LOG", "moon=info")
            .spawn()
            .expect("spawn moon")
    })
}

struct ServerGuard(Child);
impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Create the index and three documents at vt = 150 / 250 / 350.
fn seed(c: &mut common::Conn) {
    let reply = c.send(&[
        "FT.CREATE",
        "pidx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "vt",
        "NUMERIC",
        "vec",
        "VECTOR",
        "HNSW",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "COSINE",
    ]);
    assert!(reply.starts_with("+OK"), "FT.CREATE: {reply}");
    for (key, vt) in [("p:1", "150"), ("p:2", "250"), ("p:3", "350")] {
        let r = c.send(&["HSET", key, "vt", vt, "vec", VEC_A]);
        assert!(r.starts_with(":2"), "HSET {key} must add both fields: {r}");
    }
}

/// The leading integer of an FT.SEARCH array reply is the match count.
fn match_count(reply: &str) -> Option<i64> {
    let rest = reply.strip_prefix('*')?;
    let after_len = rest.split_once("\r\n")?.1;
    after_len
        .strip_prefix(':')?
        .split_once("\r\n")?
        .0
        .parse()
        .ok()
}

fn knn(c: &mut common::Conn, query: &str) -> String {
    c.send(&[
        "FT.SEARCH",
        "pidx",
        query,
        "PARAMS",
        "2",
        "qq",
        VEC_A,
        "DIALECT",
        "2",
    ])
}

#[test]
#[ignore] // Spawns a server; run explicitly.
fn knn_prefilter_honours_exclusive_bounds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path());
    let _guard = ServerGuard(child);
    std::thread::sleep(Duration::from_millis(500));
    let mut c = common::Conn::open(port);
    seed(&mut c);

    // Baselines: the filter is applied at all, and matches the full grammar.
    let all = knn(&mut c, "*=>[KNN 4 @vec $qq]");
    assert_eq!(match_count(&all), Some(3), "unfiltered KNN sees 3: {all}");
    let closed = knn(&mut c, "@vt:[100 200]=>[KNN 4 @vec $qq]");
    assert_eq!(
        match_count(&closed),
        Some(1),
        "[100 200] -> 150 only: {closed}"
    );

    // RED (moon#648): the `(` bound made the numeric branch return None for the
    // whole expression, and the caller ran unfiltered -> 3 instead of 2.
    let upper_excl = knn(&mut c, "@vt:[100 (300]=>[KNN 4 @vec $qq]");
    assert_eq!(
        match_count(&upper_excl),
        Some(2),
        "[100 (300] must match 150 and 250, not degrade to unfiltered: {upper_excl}"
    );

    // Lower exclusive: [(150 400] drops 150 and keeps 250, 350. Distinct from
    // the inclusive answer, so a dropped filter cannot pass this by accident.
    let lower_excl = knn(&mut c, "@vt:[(150 400]=>[KNN 4 @vec $qq]");
    assert_eq!(
        match_count(&lower_excl),
        Some(2),
        "[(150 400] must exclude the 150 endpoint: {lower_excl}"
    );

    // Both bounds exclusive on the same value matches nothing -- the shape most
    // likely to be silently turned into "everything".
    let empty = knn(&mut c, "@vt:[(250 (250]=>[KNN 4 @vec $qq]");
    assert_eq!(
        match_count(&empty),
        Some(0),
        "[(250 (250] is empty, never unfiltered: {empty}"
    );
}

#[test]
#[ignore] // Spawns a server; run explicitly.
fn knn_prefilter_that_cannot_be_parsed_is_an_error_not_an_unfiltered_search() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path());
    let _guard = ServerGuard(child);
    std::thread::sleep(Duration::from_millis(500));
    let mut c = common::Conn::open(port);
    seed(&mut c);

    // RED (moon#648): each of these returned 3 rows -- the caller's filter was
    // discarded and it had no way to detect that. On a scoped or multi-tenant
    // store "silently returns more than asked for" is a confidentiality bug,
    // not a recall bug.
    for bad in [
        "@vt:[abc def]=>[KNN 4 @vec $qq]",
        "@vt:[100]=>[KNN 4 @vec $qq]",
        "@vt:[1 2 3 4]=>[KNN 4 @vec $qq]",
        "@vt:[( 300]=>[KNN 4 @vec $qq]",
    ] {
        let reply = knn(&mut c, bad);
        assert!(
            reply.starts_with('-'),
            "unparseable prefilter {bad:?} must be an error, got: {reply}"
        );
    }

    // A filter that IS parseable still works after the rejections -- the guard
    // must reject the query, not poison the connection or the index.
    let ok = knn(&mut c, "@vt:[100 200]=>[KNN 4 @vec $qq]");
    assert_eq!(match_count(&ok), Some(1), "valid filter still works: {ok}");
}

#[test]
#[ignore] // Spawns a server; run explicitly.
fn inverted_knn_prefilter_range_does_not_kill_the_server() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path());
    // Guarded like its two siblings: every assertion below is load-bearing, so
    // any one of them failing unwinds — and without the guard that unwind
    // leaks a moon process holding the port.
    let mut guard = ServerGuard(child);
    std::thread::sleep(Duration::from_millis(500));
    let mut c = common::Conn::open(port);
    seed(&mut c);

    // RED (moon#664): `BTreeMap::range(300..=100)` panics, and the shard-panic
    // policy aborts the WHOLE process. One malformed query from any client
    // takes down every connection and every other shard.
    // `[+inf 5]` is the case the first cut of this guard let through: it
    // tested `min.is_finite() && max.is_finite() && min > max`, so an inverted
    // range with an infinite bound skipped the parser rejection entirely.
    for bad in [
        "@vt:[300 100]=>[KNN 4 @vec $qq]",
        "@vt:[+inf 5]=>[KNN 4 @vec $qq]",
        "@vt:[5 -inf]=>[KNN 4 @vec $qq]",
    ] {
        let reply = knn(&mut c, bad);
        assert!(
            reply.starts_with('-'),
            "an inverted range must be a per-query error: {bad} -> {reply}"
        );
    }

    // The load-bearing assertion. A dead connection and a dead process look the
    // same from the client socket, so ask the OS instead: a live server answers
    // on a NEW connection, and the process has not exited.
    let mut c2 = common::Conn::open(port);
    assert!(
        c2.send(&["PING"]).starts_with("+PONG"),
        "server must still serve new connections"
    );
    assert_eq!(
        guard.0.try_wait().expect("try_wait"),
        None,
        "the moon process must still be running -- it aborted on the inverted range"
    );

    let log = std::fs::read_to_string(dir.path().join("moon.stderr.log")).unwrap_or_default();
    assert!(
        !log.contains("panicked at"),
        "no shard may panic on malformed client input; stderr:\n{log}"
    );
}

/// The SAME defect one command away: `FT.SEARCH ... HYBRID ... FILTER NUMERIC
/// <field> <min> <max>` validated that each bound was finite and never that
/// they were ordered, so an inverted range reached
/// `TextIndex::search_numeric_range`'s `BTreeMap::range` and aborted the
/// process exactly as the KNN prefilter did.
///
/// Measured on a binary with the guards reverted, `--shards 1`:
///
/// ```text
/// FT.SEARCH cidx "machine learning" HYBRID VECTOR @vec $q FUSION RRF \
///     FILTER NUMERIC @score 300 100 PARAMS 2 q <blob>
///   -> Error: Server closed the connection
///   Abort trap: 6 -- thread 'shard-0' panicked ...
///     range start is greater than range end in BTreeMap
///   -> next connection: Connection refused
/// ```
///
/// Fixed in two layers: the parser refuses the range by name, and
/// `search_numeric_range` is total so the next caller cannot rediscover this.
#[test]
#[ignore] // Spawns a server; run explicitly.
fn an_inverted_hybrid_numeric_filter_does_not_kill_the_server() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path());
    let mut guard = ServerGuard(child);
    std::thread::sleep(Duration::from_millis(500));
    let mut c = common::Conn::open(port);

    let created = c.send(&[
        "FT.CREATE",
        "hidx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "h:",
        "SCHEMA",
        "title",
        "TEXT",
        "score",
        "NUMERIC",
        "vec",
        "VECTOR",
        "HNSW",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "COSINE",
    ]);
    assert!(created.starts_with('+'), "FT.CREATE: {created}");
    for (i, title) in [
        "machine learning doc one",
        "machine learning doc two",
        "unrelated filler text",
    ]
    .iter()
    .enumerate()
    {
        let key = format!("h:{i}");
        let score = ((i + 1) * 10).to_string();
        let r = c.send(&["HSET", &key, "title", title, "score", &score, "vec", VEC_A]);
        assert!(r.starts_with(':'), "HSET {key}: {r}");
    }
    std::thread::sleep(Duration::from_millis(500));

    let hybrid = |c: &mut common::Conn, lo: &str, hi: &str| {
        c.send(&[
            "FT.SEARCH",
            "hidx",
            "machine learning",
            "HYBRID",
            "VECTOR",
            "@vec",
            "$q",
            "FUSION",
            "RRF",
            "FILTER",
            "NUMERIC",
            "@score",
            lo,
            hi,
            "LIMIT",
            "0",
            "3",
            "PARAMS",
            "2",
            "q",
            VEC_A,
        ])
    };

    // The control FIRST: if this does not return rows, the inverted probe
    // below never reaches the evaluator and the test proves nothing.
    let ok = hybrid(&mut c, "0", "100");
    assert!(
        match_count(&ok).is_some_and(|n| n > 0),
        "the fixture must actually match before the inverted probe means anything: {ok}"
    );

    let bad = hybrid(&mut c, "300", "100");
    assert!(
        bad.starts_with('-'),
        "an inverted FILTER NUMERIC must be a per-query error: {bad}"
    );

    // The load-bearing assertion: ask the OS, not the socket.
    let mut c2 = common::Conn::open(port);
    assert!(
        c2.send(&["PING"]).starts_with("+PONG"),
        "server must still serve new connections"
    );
    assert_eq!(
        guard.0.try_wait().expect("try_wait"),
        None,
        "the moon process must still be running -- it aborted on the inverted range"
    );
    let log = std::fs::read_to_string(dir.path().join("moon.stderr.log")).unwrap_or_default();
    assert!(
        !log.contains("panicked at"),
        "no shard may panic on malformed client input; stderr:\n{log}"
    );
}
