//! moon#774 defect 3: the two connection handlers must record dispatch paths
//! the same way.
//!
//! `record_dispatch_local` / `record_dispatch_cross_spsc` /
//! `record_dispatch_cross_read_fast` each do a global atomic RMW **per
//! command**. Batched variants exist that do one RMW per pipeline batch, and
//! `handler_sharded` (the tokio runtime) already uses them. `handler_monoio`
//! — *the runtime that ships* — did not, so the optimisation landed only on
//! the runtime nobody deploys.
//!
//! Nothing else catches this: both spellings produce the SAME counter value,
//! so every INFO assertion, every Prometheus scrape, and every consistency
//! row stays green while the shipping path pays N atomics instead of one.
//! The only observable is the call site itself.
//!
//! This is a source-text pin, not a behavioural test, and it is deliberate:
//! the property under test *is* which function the source calls.

use std::path::PathBuf;

/// The per-command recorders. The trailing `()` is load-bearing — it is what
/// distinguishes `record_dispatch_local()` from `record_dispatch_local_batch(n)`.
const PER_COMMAND_RECORDERS: [&str; 3] = [
    "record_dispatch_local()",
    "record_dispatch_cross_spsc()",
    "record_dispatch_cross_read_fast()",
];

const HANDLERS: [&str; 2] = [
    "src/server/conn/handler_monoio/mod.rs",
    "src/server/conn/handler_sharded/mod.rs",
];

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is the crate root wherever the checkout lives — a
    // hardcoded path would pass here and fail on every other machine.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn both_connection_handlers_use_the_batched_dispatch_recorders() {
    let root = repo_root();
    let mut offenders: Vec<String> = Vec::new();

    for handler in HANDLERS {
        let path = root.join(handler);
        let src = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        for (lineno, line) in src.lines().enumerate() {
            // Skip doc/comment lines: the recorders are named in prose.
            let trimmed = line.trim_start();
            if trimmed.starts_with("//") {
                continue;
            }
            for needle in PER_COMMAND_RECORDERS {
                if line.contains(needle) {
                    offenders.push(format!("{handler}:{}: {needle}", lineno + 1));
                }
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "these call sites do one global atomic RMW per command where a \
         per-batch accumulator + one `*_batch(n)` flush would do one per \
         batch (moon#774):\n  {}\n\
         Both handlers must use the batched recorders; a fix that lands on \
         only one of them is the exact drift this test exists to catch.",
        offenders.join("\n  ")
    );
}

/// Non-vacuity guard: the needles must actually be findable in the tree, or
/// a rename would turn this test into a permanent silent pass.
#[test]
fn the_batched_recorders_this_test_demands_exist() {
    let src = std::fs::read_to_string(repo_root().join("src/admin/metrics_setup/recorders.rs"))
        .expect("read recorders.rs");
    for batched in [
        "pub fn record_dispatch_local_batch(",
        "pub fn record_dispatch_cross_spsc_batch(",
        "pub fn record_dispatch_cross_read_fast_batch(",
    ] {
        assert!(
            src.contains(batched),
            "`{batched}` is gone — either it was renamed (update this test) or \
             the batched recorder never existed (the drift test above would \
             then be demanding the impossible)"
        );
    }
}
