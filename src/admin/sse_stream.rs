//! Server-Sent Events (SSE) streaming for real-time metric delivery.
//!
//! Uses `tokio::sync::watch` to hold only the latest metric snapshot.
//! Each SSE client polls the watch channel at ~1 Hz. No ring buffer,
//! no per-event Arc allocation — eliminates the ~33 KB/sec RSS growth
//! caused by `tokio::sync::broadcast`'s internal slot bookkeeping.

use std::convert::Infallible;
use std::sync::OnceLock;

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, StreamBody};
use hyper::Response;
use hyper::body::Frame as HttpFrame;
use tokio::sync::watch;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::WatchStream;

/// Metric event sent to SSE clients.
#[derive(Clone, Debug, serde::Serialize)]
pub struct MetricEvent {
    pub event: &'static str,
    pub total_ops: u64,
    pub ops_per_sec: u64,
    pub total_memory: u64,
    pub connected_clients: u64,
    pub uptime_seconds: u64,
    pub total_keys: u64,
}

impl Default for MetricEvent {
    fn default() -> Self {
        Self {
            event: "server_stats",
            total_ops: 0,
            ops_per_sec: 0,
            total_memory: 0,
            connected_clients: 0,
            uptime_seconds: 0,
            total_keys: 0,
        }
    }
}

/// Global watch sender for metric events.
static METRICS_WATCH: OnceLock<watch::Sender<MetricEvent>> = OnceLock::new();

/// Initialize the metrics watch channel.
pub fn init_metrics_broadcast() -> watch::Sender<MetricEvent> {
    let (tx, _) = watch::channel(MetricEvent::default());
    let _ = METRICS_WATCH.set(tx.clone());
    tx
}

/// Get the watch sender (for publishing from metrics collection).
pub fn get_metrics_sender() -> Option<&'static watch::Sender<MetricEvent>> {
    METRICS_WATCH.get()
}

/// Handle an SSE connection: returns a streaming HTTP response.
///
/// Each client gets a `WatchStream` that yields only when the value changes.
/// Since the publisher writes at ~1 Hz and the value always changes (uptime
/// increments), each client receives ~1 event/sec with zero buffering.
pub fn handle_sse_stream() -> Response<BoxBody<Bytes, Infallible>> {
    let rx = METRICS_WATCH
        .get()
        .map(|tx| tx.subscribe())
        .unwrap_or_else(|| {
            let (_tx, rx) = watch::channel(MetricEvent::default());
            rx
        });

    let stream = WatchStream::new(rx).map(|event| -> Result<HttpFrame<Bytes>, Infallible> {
        // Pre-size buffer: "data: " (6) + JSON (~150) + "\n\n" (2) ≈ 160 bytes.
        let mut buf = String::with_capacity(192);
        buf.push_str("data: ");
        // serde_json::to_writer avoids an intermediate String allocation.
        // SAFETY: serde_json only writes valid UTF-8, so the String invariant is preserved.
        let _ = serde_json::to_writer(unsafe { buf.as_mut_vec() }, &event);
        buf.push_str("\n\n");
        Ok(HttpFrame::data(Bytes::from(buf)))
    });

    Response::builder()
        .status(200)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .header("connection", "keep-alive")
        .body(BodyExt::boxed(StreamBody::new(stream)))
        .unwrap_or_else(|_| {
            Response::new(
                http_body_util::Full::new(Bytes::from_static(b"SSE init error"))
                    .map_err(|never| match never {})
                    .boxed(),
            )
        })
}
