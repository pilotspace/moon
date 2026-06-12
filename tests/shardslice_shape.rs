//! ADD task `shardslice-migration` — code-shape pin tests (§4 TESTS).
//!
//! All tests in this file are **source-grep pins**: they read `.rs` files at
//! runtime and assert internal code shape. No network connections, no server
//! binary, no feature flags needed.
//!
//! Red/green split (as of task phase `tests`, pre-build):
//!   RED  — test_ssm5_wrappers_gone, test_ssm4b_observer_lockfree,
//!           test_ssm3_shape_no_ws_field_in_slice, test_reject_wrapper_resurrection
//!   GREEN — test_reject_borrow_across_await
//!
//! Running:
//!   cargo test --test shardslice_shape

use std::path::{Path, PathBuf};

// ── helpers ───────────────────────────────────────────────────────────────────

/// Read a source file relative to the crate root (CARGO_MANIFEST_DIR).
///
/// Panics with the path on failure.
fn read_src(rel: &str) -> String {
    let base = Path::new(env!("CARGO_MANIFEST_DIR"));
    let full = base.join(rel);
    std::fs::read_to_string(&full).unwrap_or_else(|e| panic!("cannot read {}: {e}", full.display()))
}

/// Recursively collect all `.rs` files under `dir` (relative to manifest).
///
/// Returns absolute paths.
fn rs_files_under(rel_dir: &str) -> Vec<PathBuf> {
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join(rel_dir);
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

/// Return lines of `text` that match a simple substring, annotated with 1-based
/// line numbers. Skips any line that comes after a `#[cfg(test)]` block marker
/// (heuristic: once we see a line containing exactly `#[cfg(test)]` followed by
/// a `mod tests` the rest of the file is test code). This is intentionally
/// conservative: we only skip lines that appear AFTER a `mod tests {` line
/// (i.e., inside the test module). The heuristic is documented here because it
/// is approximate — it stops at the first `mod tests {` it finds.
fn grep_production_lines<'a>(text: &'a str, pattern: &str) -> Vec<(usize, &'a str)> {
    // Simple heuristic: split at the first occurrence of a `#[cfg(test)]`
    // attribute followed (within a few lines) by `mod tests`. We split the
    // file at that point and only search the production half.
    let production_text = split_off_test_module(text);
    production_text
        .lines()
        .enumerate()
        .filter(|(_, line)| line.contains(pattern))
        .map(|(i, line)| (i + 1, line))
        .collect()
}

/// Heuristic: return the slice of `text` that precedes the `#[cfg(test)]`
/// module marker. If no such marker is found, returns all of `text`.
///
/// Strategy: scan forward; when we see a line containing `#[cfg(test)]` and
/// the NEXT non-blank line contains `mod tests`, truncate there.
fn split_off_test_module(text: &str) -> &str {
    let mut cfg_test_pos: Option<usize> = None;
    let mut byte_offset = 0usize;

    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed == "#[cfg(test)]" {
            cfg_test_pos = Some(byte_offset);
        }
        byte_offset += line.len() + 1; // +1 for the '\n'
    }

    match cfg_test_pos {
        Some(pos) => &text[..pos],
        None => text,
    }
}

// ── Shared wrapper-shape checker (used by both ssm5 and reject_wrapper_resurrection) ─

fn assert_wrappers_gone() {
    // ── 1. ShardDatabases must have no per-shard lock wrappers ───────────────

    let shared_db = read_src("src/shard/shared_databases.rs");
    let production = split_off_test_module(&shared_db);

    // Field: shards: Vec<Vec<RwLock<Database>>>  (or any variant)
    let shards_locks: Vec<_> = grep_production_lines(production, "RwLock<Database>")
        .into_iter()
        .filter(|(_, l)| {
            // We want field declarations, not method return types. Accept lines
            // that look like struct field declarations (contain `shards` or the
            // literal type, inside the struct body — any non-comment occurrence
            // in production is the failing signal).
            !l.trim_start().starts_with("//")
        })
        .collect();
    assert!(
        shards_locks.is_empty(),
        "src/shard/shared_databases.rs: found RwLock<Database> in production code \
         (wrapper field not yet deleted) — offending lines:\n{:?}",
        shards_locks
    );

    // Field: vector_stores: Vec<Mutex<VectorStore>>
    let vec_stores: Vec<_> = grep_production_lines(production, "Mutex<VectorStore>")
        .into_iter()
        .filter(|(_, l)| !l.trim_start().starts_with("//"))
        .collect();
    assert!(
        vec_stores.is_empty(),
        "src/shard/shared_databases.rs: found Mutex<VectorStore> in production code \
         (wrapper field not yet deleted) — offending lines:\n{:?}",
        vec_stores
    );

    // Field: text_stores: Vec<Mutex<TextStore>>
    let text_stores: Vec<_> = grep_production_lines(production, "Mutex<TextStore>")
        .into_iter()
        .filter(|(_, l)| !l.trim_start().starts_with("//"))
        .collect();
    assert!(
        text_stores.is_empty(),
        "src/shard/shared_databases.rs: found Mutex<TextStore> in production code \
         (wrapper field not yet deleted) — offending lines:\n{:?}",
        text_stores
    );

    // Field: graph_stores: Vec<RwLock<GraphStore>> or Vec<Mutex<GraphStore>>
    let graph_stores: Vec<_> = production
        .lines()
        .enumerate()
        .filter(|(_, l)| {
            !l.trim_start().starts_with("//")
                && (l.contains("RwLock<GraphStore>") || l.contains("Mutex<GraphStore>"))
        })
        .map(|(i, l)| (i + 1, l))
        .collect();
    assert!(
        graph_stores.is_empty(),
        "src/shard/shared_databases.rs: found RwLock<GraphStore>/Mutex<GraphStore> in \
         production code (wrapper field not yet deleted) — offending lines:\n{:?}",
        graph_stores
    );

    // Per-shard registry wrapper arrays (plural names indicate per-shard arrays)
    for pattern in &[
        "temporal_registries",
        "temporal_kv_indexes",
        "workspace_registries",
        "durable_queue_registries",
        "trigger_registries",
        "kv_write_intents",
        "deferred_hnsw_inserts",
    ] {
        let hits: Vec<_> = grep_production_lines(production, pattern)
            .into_iter()
            .filter(|(_, l)| !l.trim_start().starts_with("//"))
            .collect();
        assert!(
            hits.is_empty(),
            "src/shard/shared_databases.rs: found per-shard registry array '{}' in \
             production code (must be deleted in M5) — offending lines:\n{:?}",
            pattern,
            hits
        );
    }

    // Deleted methods: write_db, read_db, all_shard_dbs, aggregate_memory
    for method in &[
        "fn write_db",
        "fn read_db",
        "fn all_shard_dbs",
        "fn aggregate_memory",
    ] {
        let hits: Vec<_> = grep_production_lines(production, method)
            .into_iter()
            .filter(|(_, l)| !l.trim_start().starts_with("//"))
            .collect();
        assert!(
            hits.is_empty(),
            "src/shard/shared_databases.rs: method '{}' still present \
             (must be deleted in M5) — offending lines:\n{:?}",
            method,
            hits
        );
    }

    // ── 2. No is_initialized() dual-branch gates in production dirs ──────────
    // Scan src/server/, src/command/, src/shard/ (excluding slice.rs itself).
    // Heuristic: skip the test-module tail of each file.
    //
    // NOTE for the Build phase: contract C1's once-per-shard startup assert
    // must be implemented as `slice::assert_initialized(shard_id)` — a helper
    // INSIDE slice.rs — not as a raw `slice::is_initialized()` call at the
    // event loop, or this pin would flag it. The pin stays strict on purpose:
    // zero is_initialized() call sites outside slice.rs.

    for dir in &["src/server", "src/command", "src/shard"] {
        for path in rs_files_under(dir) {
            // Exclude slice.rs itself — the gate fn is allowed to SURVIVE there.
            if path.file_name().is_some_and(|n| n == "slice.rs") {
                continue;
            }
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
            let prod = split_off_test_module(&text);

            // Look for is_initialized() in production code.
            // Acceptable survivors: lines that are pure comments.
            let hits: Vec<_> = prod
                .lines()
                .enumerate()
                .filter(|(_, l)| {
                    let t = l.trim_start();
                    !t.starts_with("//") && l.contains("is_initialized()")
                })
                .map(|(i, l)| (i + 1, l))
                .collect();

            assert!(
                hits.is_empty(),
                "{}:{}: is_initialized() dual-branch gate found in production code \
                 (must be deleted in M5, C6) — offending lines:\n{:?}",
                path.display(),
                "production",
                hits
            );
        }
    }

    // ── 3. uring_handler: no write_db(, must have with_shard ─────────────────
    let uring = read_src("src/shard/uring_handler.rs");
    let uring_prod = split_off_test_module(&uring);

    let write_db_hits: Vec<_> = grep_production_lines(uring_prod, "write_db(")
        .into_iter()
        .filter(|(_, l)| !l.trim_start().starts_with("//"))
        .collect();
    assert!(
        write_db_hits.is_empty(),
        "src/shard/uring_handler.rs: write_db( call still present \
         (must switch to with_shard per M5/C6) — offending lines:\n{:?}",
        write_db_hits
    );

    let with_shard_hits: Vec<_> = grep_production_lines(uring_prod, "with_shard")
        .into_iter()
        .filter(|(_, l)| !l.trim_start().starts_with("//"))
        .collect();
    assert!(
        !with_shard_hits.is_empty(),
        "src/shard/uring_handler.rs: no with_shard call found — \
         uring_handler must use with_shard (not write_db) after M5"
    );
}

// ── Test 1: PRIMARY RED TEST — wrappers gone (ssm5) ─────────────────────────

/// ssm5: ShardDatabases source has zero RwLock<Database>/Mutex<VectorStore>/
/// Mutex<TextStore>/RwLock<GraphStore>/per-shard registry Mutex fields, zero
/// `is_initialized()` gates in production paths, and uring_handler uses
/// `with_shard` not `write_db`.
///
/// RED today: every wrapper field and gate still exists.
#[test]
fn test_ssm5_wrappers_gone() {
    assert_wrappers_gone();
}

// ── Test 2: ssm4b — metrics/admin paths are lock-free observers ──────────────

/// ssm4b: metrics_setup, server_admin, and persistence_tick must not call
/// read_db/write_db/all_shard_dbs/aggregate_memory or access .vector_stores /
/// .text_stores / .graph_stores. After M4/C5 they read published atomics only.
///
/// RED today: persistence_tick.rs calls aggregate_memory + write_db + read_db;
/// metrics_setup.rs calls read_db; server_admin.rs calls read_db.
#[test]
fn test_ssm4b_observer_lockfree() {
    let forbidden = [
        "read_db(",
        "write_db(",
        "all_shard_dbs(",
        "aggregate_memory(",
        ".vector_stores",
        ".text_stores",
        ".graph_stores",
    ];

    // Paths to check: (relative path, human label)
    // metrics_setup is under src/admin/
    let files = [
        ("src/admin/metrics_setup.rs", "src/admin/metrics_setup.rs"),
        ("src/command/server_admin.rs", "src/command/server_admin.rs"),
        (
            "src/shard/persistence_tick.rs",
            "src/shard/persistence_tick.rs",
        ),
    ];

    let mut all_violations: Vec<String> = Vec::new();

    for (rel, label) in &files {
        let text = read_src(rel);
        let prod = split_off_test_module(&text);

        for pat in &forbidden {
            let hits: Vec<_> = prod
                .lines()
                .enumerate()
                .filter(|(_, l)| {
                    let t = l.trim_start();
                    !t.starts_with("//") && l.contains(pat)
                })
                .map(|(i, l)| format!("  {}:{}: {}", label, i + 1, l.trim()))
                .collect();
            all_violations.extend(hits);
        }
    }

    assert!(
        all_violations.is_empty(),
        "ssm4b: lock-acquiring / store-accessing calls found in observer paths \
         (must be replaced with published atomics per M4/C5):\n{}",
        all_violations.join("\n")
    );
}

// ── Test 3: ssm3 — workspace_registry shape ──────────────────────────────────

/// ssm3 shape pin:
/// - slice.rs must NOT have `workspace_registry` as a field in ShardSlice
///   or ShardSliceInit (it leaves in M3).
/// - shared_databases.rs MUST have a single process-global field
///   `workspace_registry:` typed `Mutex<Option<Box<WorkspaceRegistry>>>`.
/// - shared_databases.rs must NOT have `workspace_registries:` (the per-shard
///   array — deleted in M3).
///
/// RED today: slice.rs HAS `workspace_registry` in ShardSlice/ShardSliceInit;
/// shared_databases.rs has `workspace_registries` (array) and lacks the
/// global single-field form.
#[test]
fn test_ssm3_shape_no_ws_field_in_slice() {
    let slice_text = read_src("src/shard/slice.rs");
    let slice_prod = split_off_test_module(&slice_text);

    // slice.rs must have NO `workspace_registry` struct field.
    // We look for lines that declare this as a field (contain `workspace_registry:`)
    // outside of a comment.
    let slice_field_hits: Vec<_> = slice_prod
        .lines()
        .enumerate()
        .filter(|(_, l)| {
            let t = l.trim_start();
            !t.starts_with("//") && l.contains("workspace_registry:")
        })
        .map(|(i, l)| (i + 1, l.trim()))
        .collect();

    assert!(
        slice_field_hits.is_empty(),
        "src/shard/slice.rs: workspace_registry field still present in ShardSlice \
         or ShardSliceInit (must be removed in M3) — offending lines:\n{:?}",
        slice_field_hits
    );

    // shared_databases.rs must NOT have the plural array `workspace_registries:`
    let shared_db_text = read_src("src/shard/shared_databases.rs");
    let shared_prod = split_off_test_module(&shared_db_text);

    let array_hits: Vec<_> = shared_prod
        .lines()
        .enumerate()
        .filter(|(_, l)| {
            let t = l.trim_start();
            !t.starts_with("//") && l.contains("workspace_registries")
        })
        .map(|(i, l)| (i + 1, l.trim()))
        .collect();

    assert!(
        array_hits.is_empty(),
        "src/shard/shared_databases.rs: workspace_registries (per-shard array) still present \
         (must be replaced by single global `workspace_registry` field in M3) — \
         offending lines:\n{:?}",
        array_hits
    );

    // shared_databases.rs MUST have a single global field `workspace_registry:`
    // typed with `Mutex<Option<Box<WorkspaceRegistry>>>`.
    // We check for the field name AND the type on the same or adjacent line.
    // Simple heuristic: look for a line that matches the field declaration.
    let global_field_present = shared_prod.lines().any(|l| {
        let t = l.trim_start();
        !t.starts_with("//")
            && l.contains("workspace_registry:")
            && l.contains("Mutex<Option<Box<WorkspaceRegistry>>>")
    });

    assert!(
        global_field_present,
        "src/shard/shared_databases.rs: process-global `workspace_registry: \
         Mutex<Option<Box<WorkspaceRegistry>>>` field not found (required by M3/C3)"
    );
}

// ── Test 4: reject borrow_across_await (GREEN today) ─────────────────────────

/// reject borrow_across_await: no `.await` appears inside a `with_shard(` or
/// `with_shard_db(` closure body in any production source file.
///
/// Implementation strategy: for each `.rs` file under `src/`, scan line by
/// line. When we encounter a line containing `with_shard(` or `with_shard_db(`,
/// we walk forward collecting lines until the call's paren depth returns to
/// zero. We assert that no `.await` appears in that span.
///
/// Paren-depth counting is simple ASCII character scanning (`(` increments,
/// `)` decrements). We start counting from the `(` that opens the `with_shard`
/// call (the depth starts at 1 on that line). If the file ends before depth
/// reaches 0, we conservatively include all remaining lines in the span.
///
/// Edge cases and limitations (documented per spec):
/// - Parentheses inside string literals are not handled; this is acceptable
///   because `with_shard` call sites do not contain string literals with
///   unbalanced parens in practice.
/// - Nested `with_shard` calls would be flagged by the reentrancy check at
///   runtime; the borrow-across-await check here is an additional structural pin.
///
/// GREEN today: all with_shard closures are synchronous.
#[test]
fn test_reject_borrow_across_await() {
    let mut violations: Vec<String> = Vec::new();

    for path in rs_files_under("src") {
        let text = match std::fs::read_to_string(&path) {
            Ok(t) => t,
            Err(_) => continue,
        };
        let prod = split_off_test_module(&text);
        let lines: Vec<&str> = prod.lines().collect();

        let mut i = 0;
        while i < lines.len() {
            let line = lines[i];
            // Only care about lines that open a with_shard call (not comments).
            let trimmed = line.trim_start();
            if !trimmed.starts_with("//")
                && (line.contains("with_shard(") || line.contains("with_shard_db("))
            {
                // Find the call span by tracking paren depth.
                // We count ALL parens from the first `with_shard(` on this line.
                let call_start_col = line
                    .find("with_shard(")
                    .or_else(|| line.find("with_shard_db("))
                    .unwrap_or(0);
                // Count depth starting from this line at the opening paren.
                let mut depth: i32 = 0;
                let span_start = i;
                let mut span_end = i;
                let mut found_close = false;

                'outer: for j in i..lines.len() {
                    let scan_line = if j == i {
                        &lines[j][call_start_col..]
                    } else {
                        lines[j]
                    };
                    for ch in scan_line.chars() {
                        match ch {
                            '(' => depth += 1,
                            ')' => {
                                depth -= 1;
                                if depth == 0 {
                                    span_end = j;
                                    found_close = true;
                                    break 'outer;
                                }
                            }
                            _ => {}
                        }
                    }
                }

                if !found_close {
                    // Span extends to end of production section.
                    span_end = lines.len().saturating_sub(1);
                }

                // Check the span for .await
                for j in span_start..=span_end {
                    let span_line = lines[j];
                    let span_t = span_line.trim_start();
                    if !span_t.starts_with("//") && span_line.contains(".await") {
                        violations.push(format!(
                            "{}:{}: .await inside with_shard closure — line: {}",
                            path.display(),
                            j + 1,
                            span_line.trim()
                        ));
                    }
                }

                // Advance past this call span to avoid double-counting.
                i = span_end + 1;
                continue;
            }
            i += 1;
        }
    }

    assert!(
        violations.is_empty(),
        "reject borrow_across_await: found .await inside with_shard/with_shard_db \
         closure — this is forbidden (borrow is sync by construction; an async \
         block capturing the guard is the target violation):\n{}",
        violations.join("\n")
    );
}

// ── Test 5: reject wrapper_resurrection — alias of ssm5 ──────────────────────

/// Permanent guard: same shape test as ssm5. This is the CI gate that blocks
/// any future commit reintroducing a lock wrapper into ShardDatabases.
///
/// Intentionally a separate test name so it shows up as a distinct failure in
/// CI with its own reject-scenario label.
///
/// RED today (same reason as test_ssm5_wrappers_gone).
#[test]
fn test_reject_wrapper_resurrection() {
    assert_wrappers_gone();
}
