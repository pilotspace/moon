//! Per-thread command counters and the scrape-time Prometheus publish
//! (moon#1178).
//!
//! # What was wrong
//!
//! With `--admin-port` set, every counted event went through a `counter!()`
//! macro: a registry lookup (hash of name + labels, a map probe, an `Arc`
//! clone and drop) per GET for `moon_keyspace_hits_total`, per batch for each
//! `moon_dispatch_path_total` path, per PUBLISH, per SPSC drain for the drain
//! histogram (plus a `String` for its label). The per-command family cached ONE
//! command's handles per connection, so a mixed pipeline re-registered three
//! metrics on every command switch — and the handle it did cache is a single
//! `AtomicU64` every shard increments.
//!
//! # What this does instead
//!
//! Every such counter already has — or now has — a per-thread slot:
//! [`super::HOT_COUNTERS`] for the per-command family members and
//! [`CMD_COUNTS`] here for `moon_commands_total{cmd}` /
//! `moon_command_errors_total{cmd}`, indexed by the command's label index.
//! The hot path is one TLS read and one uncontended relaxed add on the
//! thread's own line. [`publish_sharded_counters`] sums the slots and hands
//! the totals to the recorder with `.absolute()` just before `/metrics`
//! renders, so a scrape reports exactly what the per-event path reported: the
//! same names, the same labels, the same values. A series whose total is 0 is
//! not published, exactly as an untouched counter was never emitted before.
//!
//! `absolute` is a `fetch_max` in the exporter, so a publish racing with
//! another can never move a counter backwards.

use std::sync::atomic::{AtomicU64, Ordering};

use metrics::counter;

use super::{COMMAND_COUNTER_SLOT, COMMAND_COUNTER_SLOTS, METRICS_INITIALIZED, sum_hot};

use super::command_metrics::{CMD_LABEL_COUNT, CMD_LABELS};

/// One thread's per-command counts, indexed by [`super::command_metrics::cmd_label_index`].
/// Aligned so two threads' rows never share a cache line.
#[repr(align(64))]
pub(super) struct CmdRow {
    pub(super) calls: [AtomicU64; CMD_LABEL_COUNT],
    pub(super) errors: [AtomicU64; CMD_LABEL_COUNT],
}

#[allow(clippy::declare_interior_mutable_const)] // template for static array init only
const ZERO: AtomicU64 = AtomicU64::new(0);
#[allow(clippy::declare_interior_mutable_const)] // template for static array init only
const CMD_ROW_ZERO: CmdRow = CmdRow {
    calls: [ZERO; CMD_LABEL_COUNT],
    errors: [ZERO; CMD_LABEL_COUNT],
};

/// `COMMAND_COUNTER_SLOTS` rows, one per thread slot (zero-initialised, so a
/// row costs no RSS until its thread counts something).
pub(super) static CMD_COUNTS: [CmdRow; COMMAND_COUNTER_SLOTS] =
    [CMD_ROW_ZERO; COMMAND_COUNTER_SLOTS];

/// Count one execution of the command with label index `idx` on this thread.
#[inline]
pub(super) fn bump_cmd_call(idx: usize) {
    COMMAND_COUNTER_SLOT.with(|&slot| {
        if let Some(c) = CMD_COUNTS[slot].calls.get(idx) {
            c.fetch_add(1, Ordering::Relaxed);
        }
    });
}

/// Count one error reply of the command with label index `idx`.
#[inline]
pub(super) fn bump_cmd_error(idx: usize) {
    COMMAND_COUNTER_SLOT.with(|&slot| {
        if let Some(c) = CMD_COUNTS[slot].errors.get(idx) {
            c.fetch_add(1, Ordering::Relaxed);
        }
    });
}

fn sum_cmd(idx: usize, errors: bool) -> u64 {
    CMD_COUNTS
        .iter()
        .map(|row| {
            let arr = if errors { &row.errors } else { &row.calls };
            arr[idx].load(Ordering::Relaxed)
        })
        .sum()
}

/// Publish every per-thread counter to the Prometheus recorder. Called by the
/// `/metrics` handler right before it renders; a no-op without the exporter.
///
/// Cost: O(slots x (hot fields + 2 x labels)) relaxed loads, ~27K on a scrape,
/// on the admin thread — never a shard thread.
pub fn publish_sharded_counters() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    // The handle is built only for a non-zero total: registering a series
    // makes the exporter render it, and an untouched counter must stay absent.
    let publish = |total: u64, c: &dyn Fn() -> metrics::Counter| {
        if total > 0 {
            c().absolute(total);
        }
    };
    publish(sum_hot(|s| &s.keyspace_hits), &|| {
        counter!("moon_keyspace_hits_total")
    });
    publish(sum_hot(|s| &s.keyspace_misses), &|| {
        counter!("moon_keyspace_misses_total")
    });
    publish(
        sum_hot(|s| &s.dispatch_local),
        &|| counter!("moon_dispatch_path_total", "path" => "local"),
    );
    publish(
        sum_hot(|s| &s.dispatch_local_inline),
        &|| counter!("moon_dispatch_path_total", "path" => "local_inline"),
    );
    publish(
        sum_hot(|s| &s.dispatch_cross_spsc),
        &|| counter!("moon_dispatch_path_total", "path" => "cross_spsc"),
    );
    publish(
        sum_hot(|s| &s.dispatch_cross_read_fast),
        &|| counter!("moon_dispatch_path_total", "path" => "cross_read_fast"),
    );
    publish(sum_hot(|s| &s.pubsub_published), &|| {
        counter!("moon_pubsub_messages_published_total")
    });
    for (idx, &label) in CMD_LABELS.iter().enumerate() {
        let calls = sum_cmd(idx, false);
        let errors = sum_cmd(idx, true);
        publish(calls, &|| counter!("moon_commands_total", "cmd" => label));
        // A command that ran has its error series even at 0: the per-command
        // handle cache registered all three series on first use, so a scrape
        // always showed `moon_command_errors_total{cmd=..} 0` for it.
        if calls > 0 || errors > 0 {
            counter!("moon_command_errors_total", "cmd" => label).absolute(errors);
        }
    }
}
