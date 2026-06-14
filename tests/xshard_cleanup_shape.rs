//! ADD task `xshard-read-fastpath` §4 TESTS — M3 cleanup shape pin (source-grep).
//!
//! Asserts the orphaned `--cross-shard-fast-path` surface is fully DELETED from
//! production `src/`. This is a source-grep pin (reads `.rs` files at runtime; no
//! server, no feature flags), in the style of `tests/shardslice_shape.rs`.
//!
//! Red/green split:
//!   RED NOW  — the symbols still exist (config.rs, admin/metrics_setup.rs,
//!              handler_sharded/mod.rs); this test FAILS until §5 M3 removes them.
//!   GREEN    — after M3 deletes the flag/enum/metric/histogram/fns + hardcoded stat.
//!
//! Doubles as the `reject removed_flag_referenced` guard: any later commit that
//! resurrects one of these symbols re-reds this test in CI, naming the symbol.
//!
//! Running:  cargo test --test xshard_cleanup_shape

use std::path::{Path, PathBuf};

/// Dead symbols that M3 must remove. Each is specific enough not to match legitimate
/// cross-shard code (e.g. plain `cross_shard` dispatch is NOT in this list).
const DEAD_SYMBOLS: &[&str] = &[
    "cross_shard_fast_path",                       // the `cross_shard_fast_path` field + `--cross-shard-fast-path` arg + `cross_shard_fast_path_enabled`
    "CrossShardFastPath",                          // the config enum
    "moon_cross_shard_lock_contention_total",      // the dead metric
    "moon_dispatch_cross_read_fastpath_latency_us",// the dead histogram
    "record_dispatch_cross_read_fastpath",         // the dead recorder fns
    "cross_read_fast_dispatches",                  // the hardcoded-0 stat field
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
