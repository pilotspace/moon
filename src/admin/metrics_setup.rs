//! Prometheus metrics initialization and recording helpers.
//!
//! Uses the `metrics` facade crate so metric recording is a single atomic
//! operation on the hot path (counter increment or histogram observation).

use std::sync::atomic::{AtomicBool, Ordering};

use metrics::{counter, gauge, histogram};

static METRICS_INITIALIZED: AtomicBool = AtomicBool::new(false);

/// Initialize the Prometheus metrics exporter.
///
/// Must be called once before any metrics recording. Spawns a background
/// HTTP listener on `addr` that serves `/metrics` in Prometheus text format.
///
/// Also responds to `/healthz` (liveness) and `/readyz` (readiness).
pub fn init_metrics(admin_port: u16, bind: &str) {
    if admin_port == 0 {
        return;
    }

    let addr = format!("{}:{}", bind, admin_port);
    let builder = metrics_exporter_prometheus::PrometheusBuilder::new();

    // Install as the global recorder — panics if called twice
    if METRICS_INITIALIZED
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        match builder
            .with_http_listener(addr.parse::<std::net::SocketAddr>().unwrap_or_else(|_| {
                tracing::warn!("Invalid admin bind address '{}', using 0.0.0.0:{}", addr, admin_port);
                std::net::SocketAddr::from(([0, 0, 0, 0], admin_port))
            }))
            .install()
        {
            Ok(()) => {
                tracing::info!("Admin metrics server listening on {}", addr);
            }
            Err(e) => {
                tracing::error!("Failed to start metrics exporter: {}", e);
            }
        }
    }
}

// ── Command metrics ─────────────────────────────────────────────────────

/// Record a command execution.
#[inline]
pub fn record_command(cmd: &str, latency_us: u64) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_commands_total", "cmd" => cmd.to_ascii_lowercase()).increment(1);
    histogram!("moon_command_duration_microseconds", "cmd" => cmd.to_ascii_lowercase())
        .record(latency_us as f64);
}

/// Record a command error.
#[inline]
pub fn record_command_error(cmd: &str) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_command_errors_total", "cmd" => cmd.to_ascii_lowercase()).increment(1);
}

// ── Connection metrics ──────────────────────────────────────────────────

/// Record a new client connection.
#[inline]
pub fn record_connection_opened() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_connections_total").increment(1);
    gauge!("moon_connected_clients").increment(1.0);
}

/// Record a client disconnection.
#[inline]
pub fn record_connection_closed() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    gauge!("moon_connected_clients").decrement(1.0);
}

// ── Keyspace metrics ────────────────────────────────────────────────────

/// Record keyspace hit/miss.
#[inline]
pub fn record_keyspace_hit() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_keyspace_hits_total").increment(1);
}

#[inline]
pub fn record_keyspace_miss() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_keyspace_misses_total").increment(1);
}

// ── Eviction metrics ────────────────────────────────────────────────────

/// Record an eviction event.
#[inline]
pub fn record_eviction() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_evicted_keys_total").increment(1);
}

// ── Persistence metrics ─────────────────────────────────────────────────

/// Record an AOF fsync duration.
#[inline]
pub fn record_aof_fsync(duration_us: u64) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    histogram!("moon_aof_fsync_duration_microseconds").record(duration_us as f64);
}

/// Record a WAL segment rotation.
#[inline]
pub fn record_wal_rotation() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_wal_rotations_total").increment(1);
}

// ── Shard metrics ───────────────────────────────────────────────────────

/// Record SPSC queue drain batch size.
#[inline]
pub fn record_spsc_drain(shard_id: usize, count: u64) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    let shard = itoa::Buffer::new().format(shard_id).to_string();
    histogram!("moon_spsc_drain_batch_size", "shard" => shard).record(count as f64);
}

// ── Pub/Sub metrics ─────────────────────────────────────────────────────

/// Record a pub/sub message published.
#[inline]
pub fn record_pubsub_published() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_pubsub_messages_published_total").increment(1);
}

/// Record a slow subscriber drop.
#[inline]
pub fn record_pubsub_slow_drop() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_pubsub_slow_subscriber_drops_total").increment(1);
}

// ── Memory metrics ──────────────────────────────────────────────────────

/// Update RSS gauge (called periodically by shard timer).
#[inline]
pub fn update_rss_bytes(rss: u64) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    gauge!("moon_rss_bytes").set(rss as f64);
}
