//! ADD task `xshard-read-fastpath` §4 TESTS — M3 cleanup shape pin (source-grep).
//!
//! Source-grep pin (reads `.rs` files at runtime; no server, no feature flags),
//! in the style of `tests/shardslice_shape.rs`.
//!
//! **Scope revised (L4 S4).** M3 hard-deleted the orphaned
//! `--cross-shard-fast-path` surface, and this test caught the commit that
//! brought it back — working exactly as its `reject removed_flag_referenced`
//! docstring intended. The revision is deliberate, not a silenced guard.
//!
//! D3 removed the flag on an explicit premise, recorded in the task's ground
//! truth: *"the per-shard storage (DashTable/Database) is a plain `&mut self`
//! structure with NO interior concurrency — single-thread `!Send` ownership IS
//! the safety; a foreign lock-free read is storage-impossible in place."* That
//! was true of the storage model in June 2026. The L4 shared-read plane
//! replaced that model: `Database` now lives behind a per-(shard, db) lock in a
//! `Send + Sync` process registry, and `slice::try_foreign_db_read` performs
//! exactly the foreign read the premise called impossible. The flag is
//! therefore LIVE again, and only the surface that is still genuinely orphaned
//! stays on the dead list below.
//!
//! So this file now pins BOTH directions, which is stronger than before:
//!   * the five symbols M3 removed and nothing re-created stay at zero, and
//!   * the fast path stays actually WIRED — a later cleanup that deletes the
//!     dispatch site while leaving the flag parsing would otherwise leave a
//!     flag that silently does nothing, which is the bug the docs already
//!     shipped once.
//!
//! Running:  cargo test --test xshard_cleanup_shape

use std::path::{Path, PathBuf};

/// Dead symbols that M3 must remove. Each is specific enough not to match legitimate
/// cross-shard code (e.g. plain `cross_shard` dispatch is NOT in this list).
const DEAD_SYMBOLS: &[&str] = &[
    // `cross_shard_fast_path` is NOT here any more — L4 S4 re-implemented it
    // against the new storage model (see the module docs above). Every entry
    // below is still at zero references and must stay there.
    "CrossShardFastPath", // the config enum — S4 uses a plain String, not an enum
    "moon_cross_shard_lock_contention_total", // the dead metric
    "moon_dispatch_cross_read_fastpath_latency_us", // the dead histogram
    "record_dispatch_cross_read_fastpath", // the dead recorder fns (S4's is `..._cross_read_fast`)
    "cross_read_fast_dispatches", // the hardcoded-0 stat field (S4's is `total_dispatch_cross_read_fast`)
];

/// The live S4 surface. Deleting any of these while leaving the flag parsable
/// reproduces the exact defect docs/production-guide.md shipped for months: a
/// documented flag that the binary accepts and that does nothing.
const LIVE_SYMBOLS: &[(&str, &str)] = &[
    ("cross_shard_fast_path_enabled", "the dispatch-site gate"),
    ("set_cross_shard_fast_path", "the startup setter"),
    (
        "try_foreign_db_read",
        "the foreign shared-guard read itself",
    ),
    ("record_dispatch_cross_read_fast", "the fast-path counter"),
    (
        "resolve_cross_shard_fast_path",
        "the auto/on/off policy behind the shipped default",
    ),
];

/// Recursively collect all `.rs` files under `src/` (relative to manifest root).
fn src_rs_files() -> Vec<PathBuf> {
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
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

#[test]
fn dead_fastpath_surface_removed() {
    let files = src_rs_files();
    assert!(
        !files.is_empty(),
        "found no src/*.rs files — test harness path is wrong"
    );

    let mut hits: Vec<String> = Vec::new();
    for path in &files {
        let text = match std::fs::read_to_string(path) {
            Ok(t) => t,
            Err(_) => continue,
        };
        for (lineno, line) in text.lines().enumerate() {
            for sym in DEAD_SYMBOLS {
                if line.contains(sym) {
                    let rel = path
                        .strip_prefix(env!("CARGO_MANIFEST_DIR"))
                        .unwrap_or(path)
                        .display();
                    hits.push(format!("{rel}:{}  [{sym}]  {}", lineno + 1, line.trim()));
                }
            }
        }
    }

    assert!(
        hits.is_empty(),
        "M3 not complete — the dead --cross-shard-fast-path surface still has {} production reference(s):\n{}",
        hits.len(),
        hits.join("\n")
    );
}

/// The other half of the pin: the S4 surface must remain wired.
#[test]
fn live_fastpath_surface_is_still_wired() {
    let files = src_rs_files();
    assert!(
        !files.is_empty(),
        "found no src/*.rs files — harness path is wrong"
    );
    let corpus: String = files
        .iter()
        .filter_map(|p| std::fs::read_to_string(p).ok())
        .collect();

    let missing: Vec<&str> = LIVE_SYMBOLS
        .iter()
        .filter(|(sym, _)| !corpus.contains(sym))
        .map(|(_sym, why)| *why)
        .collect();

    assert!(
        missing.is_empty(),
        "the cross-shard read fast path lost {} piece(s) of its live surface: {:?}. \
         A flag that parses but no longer dispatches is worse than no flag — that is \
         precisely the state docs/production-guide.md documented for months.",
        missing.len(),
        missing
    );
}
