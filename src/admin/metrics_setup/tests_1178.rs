//! moon#1178: with the exporter on, the per-event recording helpers must not
//! reach the metrics registry — counters are published from per-thread slots
//! at scrape, and the few handles that remain (duration histograms, the SPSC
//! drain histogram) are resolved once per thread, not per event.
//!
//! Both tests flip the process-global `METRICS_INITIALIZED` for their own
//! duration, so they (and every test that asserts it is still false) hold
//! [`metrics_init_test_lock`].

use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

/// Serialises every test that reads or flips `METRICS_INITIALIZED`.
pub(super) fn metrics_init_test_lock() -> parking_lot::MutexGuard<'static, ()> {
    static LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());
    LOCK.lock()
}

/// A recorder that only counts how often the hot path asked it for a handle.
#[derive(Default)]
struct CountingRecorder {
    registrations: AtomicUsize,
}

impl metrics::Recorder for CountingRecorder {
    fn describe_counter(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_gauge(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_histogram(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn register_counter(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        self.registrations.fetch_add(1, Ordering::Relaxed);
        metrics::Counter::noop()
    }
    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        self.registrations.fetch_add(1, Ordering::Relaxed);
        metrics::Gauge::noop()
    }
    fn register_histogram(
        &self,
        _: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        self.registrations.fetch_add(1, Ordering::Relaxed);
        metrics::Histogram::noop()
    }
}

/// Restores `METRICS_INITIALIZED = false` even if the body panics.
struct ExporterOn;
impl ExporterOn {
    fn new() -> Self {
        METRICS_INITIALIZED.store(true, Ordering::SeqCst);
        ExporterOn
    }
}
impl Drop for ExporterOn {
    fn drop(&mut self) {
        METRICS_INITIALIZED.store(false, Ordering::SeqCst);
    }
}

/// 1000 rounds of every per-event helper, with a 4-command mixed pipeline
/// through the per-connection cache. Before moon#1178 each round asked the
/// registry ~10 times (a counter per keyspace hit/miss, per dispatch path,
/// per publish, a label `String` + histogram per drain, and three handles on
/// every command switch). Now: at most one histogram handle per distinct
/// command plus one drain histogram, ever.
#[test]
fn hot_path_recording_does_not_reach_the_registry_per_event() {
    let _lock = metrics_init_test_lock();
    let rec = CountingRecorder::default();
    {
        let _on = ExporterOn::new();
        metrics::with_local_recorder(&rec, || {
            let mut cache = CachedMetricsHandles::new();
            let cmds: [&[u8]; 4] = [b"GET", b"SET", b"INCR", b"HSET"];
            for i in 0..1000usize {
                // (not `record_keyspace_miss`: `hot_counter_sum_is_exact_*`
                // asserts an exact miss delta and runs concurrently)
                record_keyspace_hit();
                record_dispatch_local_batch(2);
                record_dispatch_local_inline(2);
                record_dispatch_cross_spsc_batch(1);
                record_dispatch_cross_read_fast_batch(1);
                record_pubsub_published();
                record_spsc_drain(3, 5);
                cache.observe(cmds[i % 4], (i % 16 == 0).then_some(10));
            }
        });
    }
    let n = rec.registrations.load(Ordering::Relaxed);
    assert!(
        n <= 8,
        "{n} metric-registry lookups for 1000 rounds of hot-path recording: the \
         per-event path is still resolving handles instead of bumping per-thread slots"
    );
}

/// What the hot path stopped writing to the registry must still come out at
/// scrape: `publish_sharded_counters` hands every non-zero slot total to the
/// recorder (and skips a zero one, as an untouched counter was never emitted).
#[test]
fn scrape_publishes_the_slot_totals() {
    let _lock = metrics_init_test_lock();
    let rec = CountingRecorder::default();
    {
        let _on = ExporterOn::new();
        metrics::with_local_recorder(&rec, || {
            record_keyspace_hit();
            record_dispatch_local_inline(1);
            let mut cache = CachedMetricsHandles::new();
            cache.observe(b"GET", None);
            rec.registrations.store(0, Ordering::Relaxed);
            publish_sharded_counters();
        });
    }
    let n = rec.registrations.load(Ordering::Relaxed);
    assert!(
        n >= 3,
        "scrape published only {n} series; keyspace hits, local_inline and \
         commands_total{{cmd=get}} were all non-zero"
    );
}

/// The label index is the same cardinality guard `sanitize_cmd_label` was.
#[test]
fn cmd_label_index_is_the_cardinality_guard() {
    use super::command_metrics::{CMD_LABELS, cmd_label_index};
    assert_eq!(CMD_LABELS[cmd_label_index(b"GET")], "get");
    assert_eq!(CMD_LABELS[cmd_label_index(b"hSeT")], "hset");
    assert_eq!(CMD_LABELS[cmd_label_index(b"FT.SEARCH")], "ft.search");
    assert_eq!(CMD_LABELS[cmd_label_index(b"NOSUCHCOMMAND")], "unknown");
    assert_eq!(CMD_LABELS[cmd_label_index(b"GET\xff")], "unknown");
    assert_eq!(CMD_LABELS[cmd_label_index(b"")], "unknown");
    assert_eq!(CMD_LABELS[cmd_label_index(&[b'a'; 21])], "unknown");
    let mut seen = std::collections::HashSet::new();
    for l in CMD_LABELS {
        assert!(seen.insert(l), "duplicate label {l}");
        if l != "unknown" {
            assert_eq!(CMD_LABELS[cmd_label_index(l.as_bytes())], l);
        }
    }
}
