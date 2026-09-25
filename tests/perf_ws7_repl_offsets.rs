//! WS7 / moon#1176: the connection write path must not read-lock the
//! process-wide `RwLock<ReplicationState>` per write — not to issue an AOF
//! LSN, not to probe whether replication fan-out is live, not to record a
//! write into the backlog.
//!
//! A source scan (the `intercept_flag_drift` pattern: the handlers' write
//! legs have no seam a unit test can drive). Red on the `ae21476` sources:
//! seven `issue_append_lsn(&ctx.repl_state, ..)` sites and a
//! `repl_state.read()` + shard-0 backlog mutex inside the fan-out probe.
//! The lock-freedom itself is unit-tested in `replication::state::tests`
//! (`write_handle_takes_no_state_lock`).
//!
//! Run: `cargo test --test perf_ws7_repl_offsets`
//! Red: `MOON_SRC_ROOT=<extracted ae21476 tree> cargo test --test perf_ws7_repl_offsets`

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::{Path, PathBuf};

fn root() -> PathBuf {
    PathBuf::from(
        std::env::var("MOON_SRC_ROOT").unwrap_or_else(|_| env!("CARGO_MANIFEST_DIR").to_string()),
    )
}

fn rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        if p.is_dir() {
            rs_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

/// `(file:line: text)` of every `issue_append_lsn(` call under `dir` whose
/// arguments (the call line plus the next three, comments stripped) name
/// `repl_state`.
fn lsn_calls_through_repl_state(dir: &Path) -> Vec<String> {
    let mut files = Vec::new();
    rs_files(dir, &mut files);
    let mut hits = Vec::new();
    for f in files {
        let src = std::fs::read_to_string(&f).unwrap();
        let lines: Vec<&str> = src.lines().collect();
        for i in 0..lines.len() {
            let code = lines[i].split("//").next().unwrap_or("");
            if !code.contains("issue_append_lsn(") || code.contains("fn issue_append_lsn") {
                continue;
            }
            let joined: String = lines[i..(i + 4).min(lines.len())]
                .iter()
                .map(|l| l.split("//").next().unwrap_or(""))
                .collect();
            if joined.contains("repl_state") {
                hits.push(format!("{}:{}: {}", f.display(), i + 1, lines[i].trim()));
            }
        }
    }
    hits
}

#[test]
fn connection_write_path_issues_lsns_without_the_state_lock() {
    let hits: Vec<String> = lsn_calls_through_repl_state(&root().join("src/server/conn"))
        .into_iter()
        // handler_single is the library-only single-shard handler; it has no
        // `ConnectionContext` and no replication plane of its own.
        .filter(|h| !h.contains("handler_single.rs") && !h.contains("single_aof_log.rs"))
        .collect();
    assert!(
        hits.is_empty(),
        "the connection layer issues AOF LSNs through `repl_state` (a \
         process-wide read lock per write) instead of \
         `ConnectionContext::issue_append_lsn` / `ReplWriteHandle`:\n{}",
        hits.join("\n")
    );
}

#[test]
fn fanout_probe_takes_no_lock() {
    let ft = std::fs::read_to_string(root().join("src/server/conn/handler_monoio/ft.rs")).unwrap();
    let start = ft
        .find("fn replication_fanout_active")
        .expect("probe present");
    let body_end = ft[start..].find("\n}\n").map(|e| start + e).unwrap();
    let body = &ft[start..body_end];
    for lock in [".read()", ".lock()"] {
        assert!(
            !body.contains(lock),
            "`replication_fanout_active` takes a lock (`{lock}`) on every \
             write:\n{body}"
        );
    }
}
