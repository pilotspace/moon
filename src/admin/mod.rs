//! Admin HTTP server for observability endpoints.
//!
//! Serves `/metrics` (Prometheus), `/healthz` (liveness), `/readyz` (readiness)
//! on a separate port from the RESP data port.

pub mod http_server;
pub mod metrics_setup;
pub mod slowlog;

#[cfg(feature = "console")]
pub mod console_gateway;

#[cfg(feature = "console")]
pub mod sse_stream;
#[cfg(feature = "console")]
pub mod static_files;
#[cfg(feature = "console")]
pub mod ws_bridge;

#[cfg(feature = "console")]
pub(crate) mod hnsw_trace;
#[cfg(feature = "console")]
pub(crate) mod http_server_support;
#[cfg(feature = "console")]
pub(crate) mod memory_treemap;
