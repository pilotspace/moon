//! WS7 / moon#1198 (connection items 2 and 3): no EXCLUSIVE shard-db guard
//! for read-only work on the connection path, and the command's metadata
//! resolved once per command in the monoio frame loop.
//!
//! Per the cross-shard cost model (§8.3) every exclusive hold of an owner's
//! db is a window in which a foreign shard's lock-free `try_read` declines
//! into a parked SPSC hop; these holds were taken only to READ.
//!
//! Source scans (the handler loop has no seam a unit test can drive). Red on
//! the `ae21476` sources.
//!
//! Run: `cargo test --test perf_ws7_guards`
//! Red: `MOON_SRC_ROOT=<extracted ae21476 tree> cargo test --test perf_ws7_guards`

#![allow(clippy::unwrap_used, clippy::expect_used)]

fn source(rel: &str) -> String {
    let root =
        std::env::var("MOON_SRC_ROOT").unwrap_or_else(|_| env!("CARGO_MANIFEST_DIR").to_string());
    let path = std::path::Path::new(&root).join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// The inline SET's eviction pre-gate reads `estimated_memory()` (a field
/// read) under the SHARED guard, not the exclusive one it takes again for the
/// write itself.
#[test]
fn inline_set_pre_gate_reads_under_the_shared_guard() {
    let src = source("src/server/conn/blocking.rs");
    let bad: Vec<&str> = src
        .lines()
        .filter(|l| l.contains("estimated_memory()") && l.contains("with_shard_db("))
        .collect();
    assert!(
        bad.is_empty(),
        "the inline SET takes the EXCLUSIVE db guard just to read \
         `estimated_memory()`: {bad:?}"
    );
}

/// The non-inlined local GET's cold-tier peek asks `is_hot` under the
/// SHARED guard before anything takes the exclusive one.
#[test]
fn local_get_peek_asks_is_hot_under_the_shared_guard() {
    let src = source("src/server/conn/handler_monoio/mod.rs");
    let at = src
        .find("if cmd.eq_ignore_ascii_case(b\"GET\") {")
        .expect("the local GET cold-peek block");
    let window = &src[at..at + 3000.min(src.len() - at)];
    let shared = window.find("with_shard_db_read(");
    let exclusive = window.find("with_shard_db(");
    assert!(
        matches!((shared, exclusive), (Some(s), Some(e)) if s < e),
        "the local GET peek takes the EXCLUSIVE guard before (or without) a \
         shared-guard `is_hot` check:\n{}",
        &window[..1200.min(window.len())]
    );
}

/// The monoio frame loop resolves `COMMAND_META` once per command and reuses
/// it, instead of a lookup per `metadata::is_write(cmd)` call.
#[test]
fn frame_loop_resolves_command_metadata_once() {
    let src = source("src/server/conn/handler_monoio/mod.rs");
    assert!(
        src.contains("let cmd_meta = metadata::lookup(cmd);"),
        "the frame loop does not resolve the command's metadata once"
    );
    let skip_gate = src
        .lines()
        .any(|l| l.contains("let skip_name_gates") && l.contains("metadata::lookup(cmd)"));
    assert!(
        !skip_gate,
        "the NO_INTERCEPT gate still re-resolves the metadata"
    );
}
