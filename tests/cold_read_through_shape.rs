//! moon#610 shape pin (source-grep): command handlers must not read the hot
//! plane alone.
//!
//! `Database::get_if_alive` probes the HOT table and stops, so a key that has
//! been spilled to the cold tier is indistinguishable from a missing one. That
//! single accessor choice made eighteen read commands answer as though a live
//! key did not exist (`STRLEN` 0, `TYPE` none, `TTL` -2, `PFCOUNT` 0 — while
//! `EXISTS` answered 1 for the same key).
//!
//! The fix converted every affected handler to `get_if_alive_any_plane`, but a
//! conversion is not a guarantee: nothing stops the NEXT read handler from
//! reaching for the hot-only accessor, and the bug is invisible without a
//! disk-offload server under memory pressure. `PFCOUNT` proves the point — it
//! survived two conversion passes because it loads its value through a helper
//! of its own, and was found only by grepping the accessor's call sites.
//!
//! This pin inverts that: `get_if_alive` is DENIED inside `src/command/` unless
//! the call site is on the allowlist below, with a reason. Adding a hot-only
//! read to a command now fails a test that names the file and the line.
//!
//! Source-grep in the style of `tests/xshard_cleanup_shape.rs` — reads `.rs`
//! files at runtime, no server, no feature flags.
//!
//! Running:  cargo test --test cold_read_through_shape

use std::path::{Path, PathBuf};

/// Call sites that are hot-only ON PURPOSE, each with the reason it is correct.
///
/// The enumerating commands are *plane-partitioned*: they walk the hot table
/// with `get_if_alive` and then enumerate the cold plane separately
/// (`Database::cold_only_keys` / `cold_type_matches`), so giving them a
/// read-through accessor would list every tiered key TWICE. `GET` performs its
/// own cold read after the hot miss, capturing the cold location under the
/// shard guard rather than reading disk inside it.
const ALLOWED: &[(&str, &str)] = &[
    (
        "src/command/key.rs",
        "KEYS / SCAN / RANDOMKEY: plane-partitioned, cold plane enumerated separately",
    ),
    (
        "src/command/string/string_read.rs",
        "GET: has its own cold read-through after the hot miss",
    ),
    (
        "src/command/debug_digest.rs",
        "DEBUG DIGEST: plane-partitioned like KEYS -- the hot loop is hot-only \
         on purpose and `cold_only_keys` supplies the rest, so a tiered key is \
         digested exactly once. A read-through accessor here would count every \
         tiered key TWICE and produce a digest that matches nothing.",
    ),
];

fn src_command_rs_files() -> Vec<PathBuf> {
    let base = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("command");
    let mut out = Vec::new();
    collect_rs(&base, &mut out);
    out
}

fn collect_rs(dir: &Path, out: &mut Vec<PathBuf>) {
    let rd = match std::fs::read_dir(dir) {
        Ok(r) => r,
        Err(_) => return,
    };
    for entry in rd.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rs(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

fn repo_relative(path: &Path) -> String {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

#[test]
fn command_handlers_do_not_read_the_hot_plane_alone() {
    let files = src_command_rs_files();
    assert!(
        !files.is_empty(),
        "found no .rs files under src/command — the walker is broken, \
         which would make this test vacuously green"
    );

    let mut violations: Vec<String> = Vec::new();
    for path in &files {
        let rel = repo_relative(path);
        if ALLOWED.iter().any(|(f, _)| *f == rel) {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        for (idx, line) in text.lines().enumerate() {
            // `get_if_alive_any_plane` also contains `get_if_alive`, so match
            // the call form exactly: the hot-only accessor is always followed
            // by its open paren.
            if line.contains("get_if_alive(") {
                violations.push(format!("{}:{}: {}", rel, idx + 1, line.trim()));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "command handlers must read through every plane (moon#610).\n\
         `Database::get_if_alive` is HOT-PLANE ONLY: it answers None for a key \
         that has been spilled to the cold tier, so the command reports the key \
         as absent while EXISTS reports it present.\n\
         Use `Database::get_if_alive_any_plane` instead, or add the call site to \
         ALLOWED in this file with the reason it is genuinely hot-only.\n\n\
         Offending call sites:\n  {}",
        violations.join("\n  ")
    );
}

/// The allowlist must not outlive the call sites it excuses: a stale entry
/// silently re-opens the hole it was written to keep narrow.
#[test]
fn the_allowlist_has_no_stale_entries() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for (file, reason) in ALLOWED {
        let path = root.join(file);
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("allowlisted file {file} is unreadable: {e}"));
        assert!(
            text.contains("get_if_alive("),
            "{file} is on the hot-only allowlist ({reason}) but no longer calls \
             `get_if_alive(` — drop the entry so the next hot-only read in this \
             file is caught."
        );
    }
}
