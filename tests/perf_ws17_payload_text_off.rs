//! moon#1226: with `MOON_VECTOR_PAYLOAD_TEXT=off` a KNN full-text prefilter
//! (`@field:{multi word}`, a `TextMatch` node) used to match NO document,
//! silently — an empty page indistinguishable from "no hit", and a primary and
//! a replica with different settings answered the same query differently.
//! It is now refused with an explicit ERR, FT.INFO reports the setting, and a
//! server with the default setting still answers the filter.
//!
//! Red on `ae21476`: the off-server answers `*1 :0` (an empty page) and FT.INFO
//! has no `payload_text_index` field.

#![cfg(feature = "text-index")]

mod common;

use std::process::Command;

/// 16 ASCII bytes = one FLOAT32 DIM 4 vector (plain-text RESP helper).
const VEC_A: &str = "ABCDEFGHIJKLMNOP";
const VEC_B: &str = "PONMLKJIHGFEDCBA";

fn spawn(dir: &std::path::Path, payload_text: Option<&str>) -> (common::ServerGuard, u16) {
    common::spawn_listening_guarded(|port| {
        let mut cmd = Command::new(common::find_moon_binary());
        cmd.args([
            "--port",
            &port.to_string(),
            "--dir",
            &dir.to_string_lossy(),
            "--shards",
            "1",
            "--appendonly",
            "no",
            "--disk-free-min-pct",
            "0",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(common::server_stderr(dir))
        .env_remove("MOON_VECTOR_PAYLOAD_TEXT")
        .env_remove("MOON_VECTOR_PAYLOAD_SCHEMA");
        if let Some(v) = payload_text {
            cmd.env("MOON_VECTOR_PAYLOAD_TEXT", v);
        }
        cmd.spawn().expect("spawn moon")
    })
}

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
    ]);
    assert!(reply.starts_with("+OK"), "FT.CREATE: {reply}");
    for (key, body, lang, v) in [
        ("p:1", "red apple pie", "en", VEC_A),
        ("p:2", "green pear tart", "en", VEC_B),
        ("p:3", "red cherry cake", "fr", VEC_B),
    ] {
        let r = c.send(&["HSET", key, "body", body, "lang", lang, "vec", v]);
        assert!(r.starts_with(":3"), "HSET {key}: {r}");
    }
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

fn info_value(info: &str, key: &str) -> Option<String> {
    let mut lines = info.split("\r\n");
    while let Some(l) = lines.next() {
        if l == key {
            // `$<len>` header, then the value.
            lines.next()?;
            return lines.next().map(str::to_owned);
        }
    }
    None
}

#[test]
fn a_full_text_knn_filter_is_refused_when_the_payload_text_index_is_off() {
    let dir = common::unique_test_dir("ws17-payload-text-off");
    let (_guard, port) = spawn(&dir, Some("off"));
    let mut c = common::Conn::open(port);
    seed(&mut c);

    // Inline prefix and explicit FILTER forms are both refused.
    let inline = knn(&mut c, "@body:{red apple}=>[KNN 3 @vec $qq]");
    assert!(
        inline.starts_with("-ERR full-text KNN filter"),
        "inline TextMatch with the payload text index off must be an error, got {inline:?}"
    );
    let explicit = c.send(&[
        "FT.SEARCH",
        "pidx",
        "*=>[KNN 3 @vec $qq]",
        "FILTER",
        "@lang:{en} @body:{red apple}",
        "PARAMS",
        "2",
        "qq",
        VEC_A,
        "DIALECT",
        "2",
    ]);
    assert!(
        explicit.starts_with("-ERR full-text KNN filter"),
        "explicit FILTER with a TextMatch node must be an error, got {explicit:?}"
    );

    // A tag filter does not need the payload text index: still answered.
    let tag = knn(&mut c, "@lang:{fr}=>[KNN 3 @vec $qq]");
    assert!(tag.starts_with("*3\r\n:1\r\n"), "tag prefilter: {tag:?}");
    assert!(tag.contains("p:3"), "tag prefilter: {tag:?}");

    let info = c.send(&["FT.INFO", "pidx"]);
    assert_eq!(
        info_value(&info, "payload_text_index").as_deref(),
        Some("off"),
        "FT.INFO must report the setting: {info:?}"
    );
}

#[test]
fn the_default_server_still_answers_a_full_text_knn_filter() {
    let dir = common::unique_test_dir("ws17-payload-text-on");
    let (_guard, port) = spawn(&dir, None);
    let mut c = common::Conn::open(port);
    seed(&mut c);

    let reply = knn(&mut c, "@body:{red apple}=>[KNN 3 @vec $qq]");
    assert!(reply.starts_with("*3\r\n:1\r\n"), "TextMatch: {reply:?}");
    assert!(reply.contains("p:1"), "TextMatch: {reply:?}");

    let info = c.send(&["FT.INFO", "pidx"]);
    assert_eq!(
        info_value(&info, "payload_text_index").as_deref(),
        Some("on"),
        "FT.INFO must report the setting: {info:?}"
    );
}
