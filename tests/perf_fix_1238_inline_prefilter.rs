//! moon#1238: at `--shards > 1` FT.SEARCH dropped an inline KNN prefilter
//! (`@f:{v}=>[KNN …]`) and answered an UNFILTERED search.
//!
//! Both multi-shard connection handlers parsed the query with
//! `parse_ft_search_args`, which read the filter from an explicit `FILTER`
//! clause only. The prefix before `=>` was neither parsed nor refused, and the
//! scatter carried no filter to any shard. The query returned the nearest
//! documents whatever the filter said, and an unparseable prefilter returned
//! rows instead of the `ERR` it answers at `--shards 1`. This is the moon#648
//! class: a prefilter is honoured or refused, never silently dropped.
//!
//! Every test here runs the same queries at `--shards 1`, `2` and `4` and
//! requires the multi-shard replies to be byte-identical to `--shards 1`
//! (same keys, same scores, same errors). `--shards 1` is itself checked
//! against the expected keys, so the comparison cannot pass on a wrong
//! baseline.
//!
//! Red on `1a635f1` (and on `ae21476`): at `--shards 2`, `@lang:{fr}` answers
//! d:0..d:4 (three of them are not `fr`) and `@year:[abc def]` answers five rows
//! instead of `-ERR invalid FILTER expression`.
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --test perf_fix_1238_inline_prefilter`.

mod common;

use std::process::Command;

const SHARD_COUNTS: [usize; 3] = [1, 2, 4];
const DOCS: usize = 12;
const LANGS: [&str; 3] = ["en", "fr", "de"];

/// Query vector: 16 ASCII bytes = one FLOAT32 DIM 4 vector. Each component's
/// little-endian bytes are `'0' '0' c 'A'`, so the value grows with `c`.
const QUERY_VEC: &str = "000A000A000A000A";

/// Document `i`'s vector: component `j` uses `c = '0' + 3i + j`, so the L2
/// distance to [`QUERY_VEC`] grows strictly with `i` — no ties, so the merged
/// order is fully determined and must equal the single-shard order.
fn doc_vector(i: usize) -> String {
    let mut v = String::with_capacity(16);
    for j in 0..4 {
        v.push_str("00");
        v.push(char::from(b'0' + (3 * i + j) as u8));
        v.push('A');
    }
    v
}

fn lang(i: usize) -> &'static str {
    LANGS[i % LANGS.len()]
}

/// Spawn a server at `shards` with `env` set (and the payload knobs cleared
/// otherwise, so the host environment cannot change the answers).
fn spawn(dir: &std::path::Path, shards: usize, env: &[(&str, &str)]) -> (common::ServerGuard, u16) {
    std::fs::create_dir_all(dir).expect("create test dir");
    let bin = common::find_moon_binary();
    common::spawn_listening_guarded(|port| {
        let mut cmd = Command::new(&bin);
        cmd.args([
            "--port",
            &port.to_string(),
            "--dir",
            &dir.to_string_lossy(),
            "--shards",
            &shards.to_string(),
            "--appendonly",
            "no",
            "--save",
            "",
            "--disk-free-min-pct",
            "0",
        ])
        .stdout(common::server_stderr(dir))
        .stderr(common::server_stderr(dir))
        .env_remove("MOON_VECTOR_PAYLOAD_TEXT")
        .env_remove("MOON_VECTOR_PAYLOAD_SCHEMA");
        for (k, v) in env {
            cmd.env(k, v);
        }
        cmd.spawn().expect("spawn moon")
    })
}

/// Create `fidx` (a 4-dim vector plus `schema_extra`) and write the documents.
/// Every document carries `lang`, `year` and `body`; `note` is written too
/// when `with_note` is set.
fn seed(c: &mut common::Conn, schema_extra: &[&str], with_note: bool) {
    let mut create = vec![
        "FT.CREATE",
        "fidx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "d:",
        "SCHEMA",
        "vec",
        "VECTOR",
        "HNSW",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ];
    create.extend_from_slice(schema_extra);
    let reply = c.send(&create);
    assert!(reply.starts_with("+OK"), "FT.CREATE: {reply:?}");
    for i in 0..DOCS {
        let key = format!("d:{i}");
        let vec = doc_vector(i);
        let year = (2000 + i).to_string();
        let body = format!("word{i} common text");
        let mut hset = vec![
            "HSET",
            key.as_str(),
            "vec",
            vec.as_str(),
            "lang",
            lang(i),
            "year",
            year.as_str(),
            "body",
            body.as_str(),
        ];
        if with_note {
            hset.extend_from_slice(&["note", "alpha beta"]);
        }
        let want = format!(":{}\r\n", (hset.len() - 2) / 2);
        let r = c.send(&hset);
        assert_eq!(r, want, "HSET {key}");
    }
}

/// The raw RESP reply to every query in `queries`, one server per call.
fn answers(
    tag: &str,
    shards: usize,
    env: &[(&str, &str)],
    schema_extra: &[&str],
    with_note: bool,
    queries: &[&[&str]],
) -> Vec<String> {
    let dir = common::unique_test_dir(&format!("fix1238-{tag}-s{shards}"));
    let (guard, port) = spawn(&dir, shards, env);
    let mut c = common::Conn::open(port);
    seed(&mut c, schema_extra, with_note);
    let out = queries.iter().map(|q| c.send(q)).collect();
    drop(c);
    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
    out
}

/// The keys of an FT.SEARCH reply, in reply order, or `None` for a non-array
/// reply (an error). Only `d:<n>` bulk strings are keys in this fixture.
fn keys(reply: &str) -> Option<Vec<String>> {
    if !reply.starts_with('*') {
        return None;
    }
    Some(
        reply
            .split("\r\n")
            .filter(|l| l.starts_with("d:"))
            .map(str::to_owned)
            .collect(),
    )
}

fn expect_keys(ids: &[usize]) -> Vec<String> {
    ids.iter().map(|i| format!("d:{i}")).collect()
}

/// Run `queries` at every shard count in [`SHARD_COUNTS`] and assert each
/// multi-shard reply is byte-identical to the `--shards 1` reply. Returns the
/// `--shards 1` replies for the caller's own content checks.
fn same_answer_at_every_shard_count(
    tag: &str,
    env: &[(&str, &str)],
    schema_extra: &[&str],
    with_note: bool,
    queries: &[&[&str]],
) -> Vec<String> {
    let one = answers(tag, 1, env, schema_extra, with_note, queries);
    for &shards in &SHARD_COUNTS[1..] {
        let many = answers(tag, shards, env, schema_extra, with_note, queries);
        for ((q, a1), an) in queries.iter().zip(&one).zip(&many) {
            assert_eq!(
                an, a1,
                "shards={shards}: {q:?} must answer exactly what --shards 1 answers \
                 (a dropped prefilter answers the unfiltered nearest documents)"
            );
        }
    }
    one
}

fn knn_args(query: &str) -> [&str; 9] {
    [
        "FT.SEARCH",
        "fidx",
        query,
        "PARAMS",
        "2",
        "q",
        QUERY_VEC,
        "DIALECT",
        "2",
    ]
}

/// TAG and NUMERIC inline prefilters, including `(` exclusive bounds, a
/// single-document match, a compound AND and a filter matching nothing.
#[test]
fn inline_tag_and_numeric_prefilters_answer_the_single_shard_result_at_every_shard_count() {
    let cases: [(&'static str, &[usize]); 9] = [
        // Control: no prefilter.
        ("*=>[KNN 5 @vec $q]", &[0, 1, 2, 3, 4]),
        ("@lang:{fr}=>[KNN 5 @vec $q]", &[1, 4, 7, 10]),
        ("@year:[2003 2007]=>[KNN 10 @vec $q]", &[3, 4, 5, 6, 7]),
        ("@year:[(2003 2007]=>[KNN 10 @vec $q]", &[4, 5, 6, 7]),
        ("@year:[2003 (2007]=>[KNN 10 @vec $q]", &[3, 4, 5, 6]),
        ("@year:[(2003 (2007]=>[KNN 10 @vec $q]", &[4, 5, 6]),
        // One matching document among twelve (the moon#1238 reproduction).
        ("@year:[2007 2007]=>[KNN 5 @vec $q]", &[7]),
        (
            "@lang:{fr} @year:[2004 +inf]=>[KNN 10 @vec $q]",
            &[4, 7, 10],
        ),
        // A filter matching nothing is an empty page, not the nearest docs.
        ("@lang:{zz}=>[KNN 5 @vec $q]", &[]),
    ];
    let args: Vec<[&str; 9]> = cases.iter().map(|(q, _)| knn_args(q)).collect();
    let queries: Vec<&[&str]> = args.iter().map(|a| &a[..]).collect();
    let one = same_answer_at_every_shard_count("tagnum", &[], &[], false, &queries);
    for ((q, want), reply) in cases.iter().zip(&one) {
        assert_eq!(
            keys(reply),
            Some(expect_keys(want)),
            "--shards 1 {q:?}: {reply:?}"
        );
    }
}

/// A `@field:{multi word}` (TextMatch) inline prefilter answered by the vector
/// payload text index. Without the `text-index` feature there is no full-text
/// route, and the prefilter must be refused alike at every shard count.
#[test]
fn inline_text_match_prefilter_answers_the_single_shard_result_at_every_shard_count() {
    let args = [
        knn_args("@body:{word7 common}=>[KNN 5 @vec $q]"),
        knn_args("@body:{word7 common} @lang:{fr}=>[KNN 5 @vec $q]"),
        knn_args("@body:{word7 common} @lang:{en}=>[KNN 5 @vec $q]"),
    ];
    let queries: Vec<&[&str]> = args.iter().map(|a| &a[..]).collect();
    let one = same_answer_at_every_shard_count("text", &[], &[], false, &queries);
    if cfg!(feature = "text-index") {
        let want: [&[usize]; 3] = [&[7], &[7], &[]];
        for ((reply, w), q) in one.iter().zip(want).zip(&args) {
            assert_eq!(
                keys(reply),
                Some(expect_keys(w)),
                "--shards 1 {q:?}: {reply:?}"
            );
        }
    } else {
        for reply in &one {
            assert!(
                reply.starts_with("-ERR full-text KNN filter"),
                "no text engine: a TextMatch prefilter must be refused, got {reply:?}"
            );
        }
    }
}

/// An inline prefilter that cannot be honoured as written is an ERR at every
/// shard count — never a fall-back to an unfiltered search (moon#648).
#[test]
fn an_unparseable_inline_prefilter_is_an_error_at_every_shard_count() {
    let args = [
        knn_args("@year:[abc def]=>[KNN 5 @vec $q]"),
        // Inverted range (moon#664).
        knn_args("@year:[2007 2003]=>[KNN 5 @vec $q]"),
        knn_args("lang:{fr}=>[KNN 5 @vec $q]"),
        knn_args("@year:[1 2 3 4]=>[KNN 5 @vec $q]"),
        knn_args("@lang:{fr} garbage=>[KNN 5 @vec $q]"),
    ];
    let queries: Vec<&[&str]> = args.iter().map(|a| &a[..]).collect();
    let one = same_answer_at_every_shard_count("invalid", &[], &[], false, &queries);
    for (reply, q) in one.iter().zip(&args) {
        assert_eq!(
            reply, "-ERR invalid FILTER expression\r\n",
            "--shards 1 {q:?}"
        );
    }
}

/// An explicit `FILTER` clause is still refused at `--shards > 1` (kept
/// behaviour), alone or next to an inline prefilter: an error, never an
/// unfiltered answer. An unreadable `FILTER` answers the same ERR at every
/// shard count.
#[test]
fn an_explicit_filter_clause_is_refused_at_multi_shard_never_dropped() {
    let clause = |filter: &'static str, query: &'static str| -> [&'static str; 11] {
        [
            "FT.SEARCH",
            "fidx",
            query,
            "FILTER",
            filter,
            "PARAMS",
            "2",
            "q",
            QUERY_VEC,
            "DIALECT",
            "2",
        ]
    };
    let honoured = [
        clause("@lang:{fr}", "*=>[KNN 5 @vec $q]"),
        // Both present: `--shards 1` resolves to the FILTER clause.
        clause("@lang:{fr}", "@lang:{en}=>[KNN 5 @vec $q]"),
    ];
    let invalid = clause("@year:[abc def]", "*=>[KNN 5 @vec $q]");
    let mut queries: Vec<&[&str]> = honoured.iter().map(|a| &a[..]).collect();
    queries.push(&invalid[..]);

    let one = answers("clause", 1, &[], &[], false, &queries);
    for (reply, q) in one[..2].iter().zip(&honoured) {
        assert_eq!(
            keys(reply),
            Some(expect_keys(&[1, 4, 7, 10])),
            "--shards 1 {q:?}: {reply:?}"
        );
    }
    assert_eq!(one[2], "-ERR invalid FILTER expression\r\n");
    for &shards in &SHARD_COUNTS[1..] {
        let many = answers("clause", shards, &[], &[], false, &queries);
        for (reply, q) in many[..2].iter().zip(&honoured) {
            assert_eq!(
                reply, "-ERR FILTER not supported in multi-shard mode yet\r\n",
                "shards={shards}: {q:?}"
            );
        }
        assert_eq!(many[2], one[2], "shards={shards}: unreadable FILTER");
    }
}

/// The KNN prefilter's input limit (`MAX_KNN_FILTER_CONDITIONS`, 128
/// conditions; parity with HybridFilter's parse-time limits and a bounded
/// evaluation cost): a prefilter of exactly 128 conditions is honoured, one of
/// 129 answers `ERR invalid FILTER expression` — inline or as an explicit
/// `FILTER` clause, alike at every shard count.
#[test]
fn a_prefilter_over_the_condition_limit_is_an_error_at_every_shard_count() {
    const LIMIT: usize = 128;
    // `LIMIT - 1` conditions every document meets, then `@lang:{fr}`.
    let at_limit = format!("{}@lang:{{fr}}", "@year:[2000 +inf] ".repeat(LIMIT - 1));
    let over_limit = format!("@year:[(1999 2100] {at_limit}");
    let inline_at = format!("{at_limit}=>[KNN 5 @vec $q]");
    let inline_over = format!("{over_limit}=>[KNN 5 @vec $q]");
    let clause = |filter: &str| -> Vec<String> {
        [
            "FT.SEARCH",
            "fidx",
            "*=>[KNN 5 @vec $q]",
            "FILTER",
            filter,
            "PARAMS",
            "2",
            "q",
            QUERY_VEC,
            "DIALECT",
            "2",
        ]
        .map(str::to_owned)
        .to_vec()
    };
    let clause_over = clause(&over_limit);
    let clause_at = clause(&at_limit);
    let clause_over: Vec<&str> = clause_over.iter().map(String::as_str).collect();
    let clause_at: Vec<&str> = clause_at.iter().map(String::as_str).collect();
    let inline_at = knn_args(&inline_at);
    let inline_over = knn_args(&inline_over);
    // Answered alike at every shard count; the at-limit FILTER clause is
    // checked apart, since a FILTER clause is refused at `--shards > 1`.
    let queries: [&[&str]; 3] = [&inline_at, &inline_over, &clause_over];
    let one = same_answer_at_every_shard_count("limit", &[], &[], false, &queries);
    assert_eq!(
        keys(&one[0]),
        Some(expect_keys(&[1, 4, 7, 10])),
        "{LIMIT} conditions: {:?}",
        one[0]
    );
    for reply in &one[1..] {
        assert_eq!(
            reply,
            "-ERR invalid FILTER expression\r\n",
            "{} conditions",
            LIMIT + 1
        );
    }
    let clause_one = answers("limit-clause", 1, &[], &[], false, &[&clause_at]);
    assert_eq!(
        keys(&clause_one[0]),
        Some(expect_keys(&[1, 4, 7, 10])),
        "FILTER, {LIMIT} conditions: {:?}",
        clause_one[0]
    );
}

/// Schema-aware payload mode (`MOON_VECTOR_PAYLOAD_SCHEMA=declared`) with the
/// payload text index off: a TextMatch on the declared TEXT field is answered
/// through the BM25 plane on EVERY leg (each shard's own text index), and a
/// TextMatch on an undeclared field — which no index can answer — is refused
/// by every leg. The refusal must reach the client as the ERR `--shards 1`
/// answers, not be folded into an empty page by the cross-shard merge.
#[cfg(feature = "text-index")]
#[test]
fn declared_schema_text_match_is_answered_or_refused_alike_on_every_leg() {
    let env = [
        ("MOON_VECTOR_PAYLOAD_TEXT", "off"),
        ("MOON_VECTOR_PAYLOAD_SCHEMA", "declared"),
    ];
    let schema = ["lang", "TAG", "year", "NUMERIC", "body", "TEXT"];
    let args = [
        knn_args("@body:{word7 common}=>[KNN 5 @vec $q]"),
        knn_args("@lang:{fr} @year:[(2003 2010]=>[KNN 5 @vec $q]"),
        knn_args("@note:{alpha beta}=>[KNN 5 @vec $q]"),
        knn_args("@lang:{fr} @note:{alpha beta}=>[KNN 5 @vec $q]"),
    ];
    let queries: Vec<&[&str]> = args.iter().map(|a| &a[..]).collect();
    let one = same_answer_at_every_shard_count("declared", &env, &schema, true, &queries);
    assert_eq!(keys(&one[0]), Some(expect_keys(&[7])), "{:?}", one[0]);
    assert_eq!(
        keys(&one[1]),
        Some(expect_keys(&[4, 7, 10])),
        "{:?}",
        one[1]
    );
    for reply in &one[2..] {
        assert!(
            reply.starts_with("-ERR full-text KNN filter"),
            "a TextMatch no index can answer must be refused, got {reply:?}"
        );
    }
}
