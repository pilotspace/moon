//! Server-Sent Events (SSE) streaming for real-time metric delivery.
//!
//! Uses `tokio::sync::broadcast` for fan-out to multiple SSE clients.
//! Slow consumers receive `RecvError::Lagged` and skip to current data
//! rather than causing backpressure.

use std::convert::Infallible;
use std::sync::OnceLock;

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame as HttpFrame;
use hyper::Response;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

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

/// Global broadcast sender for metric events.
static METRICS_BROADCAST: OnceLock<broadcast::Sender<MetricEvent>> = OnceLock::new();

/// Initialize the metrics broadcast channel.
/// Returns the sender (for the metrics publisher task).
pub fn init_metrics_broadcast() -> broadcast::Sender<MetricEvent> {
    let (tx, _) = broadcast::channel(1024);
    let _ = METRICS_BROADCAST.set(tx.clone());
    tx
}

/// Get the broadcast sender (for publishing from metrics collection).
pub fn get_metrics_sender() -> Option<&'static broadcast::Sender<MetricEvent>> {
    METRICS_BROADCAST.get()
}

/// Handle an SSE connection: returns a streaming HTTP response.
///
/// Response uses `text/event-stream` content type with `data:` framing.
/// Multiple clients each get their own broadcast receiver. Slow clients
/// that fall behind the 1024-event buffer get `Lagged` errors and skip
/// to current data.
pub fn handle_sse_stream() -> Response<BoxBody<Bytes, Infallible>> {
    let rx = METRICS_BROADCAST
        .get()
        .map(|tx| tx.subscribe())
        .unwrap_or_else(|| {
            // No broadcast initialized yet -- create a dummy channel
            let (_tx, rx) = broadcast::channel(1);
            rx
        });

    let stream =
        BroadcastStream::new(rx).filter_map(|result| -> Option<Result<HttpFrame<Bytes>, Infallible>> {
            match result {
                Ok(event) => {
                    let json = serde_json::to_string(&event).ok()?;
                    let sse_data = format!("data: {json}\n\n");
                    Some(Ok(HttpFrame::data(Bytes::from(sse_data))))
                }
                Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                    tracing::debug!("SSE client lagged by {} events", n);
                    None
                }
            }
        });

    Response::builder()
        .status(200)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .header("connection", "keep-alive")
        .header("access-control-allow-origin", "*")
        .body(BodyExt::boxed(StreamBody::new(stream)))
        .unwrap_or_else(|_| {
            Response::new(
                http_body_util::Full::new(Bytes::from_static(b"SSE init error"))
                    .map_err(|never| match never {})
                    .boxed(),
            )
        })
}
