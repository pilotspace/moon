//! Prometheus exporter / admin-server bring-up.
//!
//! Split out of the former single-file `metrics_setup.rs` (moon#479, file-size
//! ceiling); `init_metrics` and its gauge priming are unchanged.

use std::sync::atomic::{AtomicBool, Ordering};

use metrics::{Unit, describe_gauge, gauge};

use crate::admin::metrics_setup::METRICS_INITIALIZED;

/// Initialize the Prometheus metrics exporter and admin HTTP server.
///
/// Must be called once before any metrics recording. Spawns a custom admin
/// HTTP server on `addr` that serves `/metrics`, `/healthz`, and `/readyz`.
///
/// Returns an `Arc<AtomicBool>` readiness flag. Set it to `true` once all
/// shards have finished persistence recovery to make `/readyz` return 200.
///
/// When the `console` feature is enabled, the three hardening policies
/// (`auth`, `cors`, `rate`) are threaded into the server via the extra
/// arguments. Callers build the policies from `ServerConfig` in `main.rs`.
pub fn init_metrics(
    admin_port: u16,
    bind: &str,
    #[cfg(feature = "console")] auth: std::sync::Arc<crate::admin::auth::AuthPolicy>,
    #[cfg(feature = "console")] cors: std::sync::Arc<crate::admin::cors::CorsPolicy>,
    #[cfg(feature = "console")] rate_limit_rps: f64,
    #[cfg(feature = "console")] rate_limit_burst: f64,
) -> Option<std::sync::Arc<AtomicBool>> {
    if admin_port == 0 {
        return None;
    }

    let addr_str = format!("{}:{}", bind, admin_port);
    let addr: std::net::SocketAddr = addr_str.parse().unwrap_or_else(|_| {
        tracing::warn!(
            "Invalid admin bind address '{}', using 0.0.0.0:{}",
            addr_str,
            admin_port
        );
        std::net::SocketAddr::from(([0, 0, 0, 0], admin_port))
    });

    // Build recorder without starting the built-in HTTP listener
    if METRICS_INITIALIZED
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let prometheus_handle = recorder.handle();

        // Install as the global metrics recorder
        if let Err(e) = metrics::set_global_recorder(recorder) {
            tracing::error!("Failed to set global metrics recorder: {}", e);
            return None;
        }

        let ready = std::sync::Arc::new(AtomicBool::new(false));
        crate::admin::http_server::spawn_admin_server(
            addr,
            prometheus_handle,
            ready.clone(),
            #[cfg(feature = "console")]
            auth,
            #[cfg(feature = "console")]
            cors,
            #[cfg(feature = "console")]
            rate_limit_rps,
            #[cfg(feature = "console")]
            rate_limit_burst,
        );
        // Register per-subsystem memory gauge and prime all 7 labels so
        // disabled subsystems still surface a zero-valued series.
        describe_gauge!(
            "moon_memory_bytes",
            Unit::Bytes,
            "Resident bytes per subsystem; sum approximates RSS"
        );
        prime_moon_memory_bytes();
        // Task #56 (used_memory truthfulness): headline gauge matching
        // INFO's `used_memory` field -- the logical ledger `--maxmemory`
        // eviction gates on (KV + ColdIndex + vector/text/graph), NOT RSS.
        // `moon_rss_bytes` (primed by the first `update_rss_bytes` call)
        // remains the true OS-level footprint for comparison.
        describe_gauge!(
            "moon_used_memory_bytes",
            Unit::Bytes,
            "Logical used-memory ledger --maxmemory eviction gates on (KV+ColdIndex+vector+text+graph); compare against a configured maxmemory, NOT moon_rss_bytes"
        );
        gauge!("moon_used_memory_bytes").set(0.0);

        Some(ready)
    } else {
        None
    }
}

/// Prime every `moon_memory_bytes{kind=...}` series with `0.0` so they
/// appear in `/metrics` output from the first scrape, even when subsystems
/// are feature-gated off or not yet initialized.
///
/// NOTE: This scrape path intentionally does NOT call `mallctl("epoch")`.
/// See the documented jemalloc leak at the `get_rss_bytes()` doc-comment
/// (~1 MB / 20 s growth). `allocator_overhead` is computed as
/// `max(0, RSS − sum(other kinds))` — the same formula MEMORY DOCTOR uses.
fn prime_moon_memory_bytes() {
    for kind in [
        "dashtable",
        "hnsw",
        // K4 (kernel-m2-brief-2026-07-12 stage 2): text (FTS) resident
        // bytes -- previously hard-coded 0 at the publish site, so this
        // series existed nowhere until now.
        "text",
        "csr",
        "wal",
        "sealed",
        "replication_backlog",
        // Pre-existing gap fixed alongside the "text" addition above:
        // `update_moon_memory_bytes` has emitted this kind since C4
        // (wave-5 hygiene), but it was never primed, so it silently
        // didn't appear in `/metrics` until the first 15s update tick.
        "lua_scripts",
        "allocator_overhead",
        // task #58 (LOW-2): per-shard PageCache resident buffer bytes.
        "pagecache",
    ] {
        gauge!("moon_memory_bytes", "kind" => kind).set(0.0);
    }
}
