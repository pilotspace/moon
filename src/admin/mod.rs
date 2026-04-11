//! Admin HTTP server for observability endpoints.
//!
//! Serves `/metrics` (Prometheus), `/healthz` (liveness), `/readyz` (readiness)
//! on a separate port from the RESP data port.

pub mod http_server;
pub mod metrics_setup;
pub mod slowlog;

#[cfg(feature = "console")]
pub mod console_gateway;
