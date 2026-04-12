//! WebSocket-to-RESP3 bridge for interactive command sessions.
//!
//! Each WebSocket connection runs as a tokio task on the admin thread's
//! single-threaded runtime. Commands are dispatched through the ConsoleGateway
//! SPSC channels to shard event loops.

use std::sync::Arc;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use hyper_tungstenite::HyperWebsocketStream;
use hyper_tungstenite::tungstenite::Message;

use crate::admin::console_gateway::ConsoleGateway;

/// Maximum number of pending outgoing messages before dropping.
/// Combined with tungstenite `max_write_buffer_size` (configured at upgrade)
/// this provides a two-tier backpressure mechanism:
/// - Soft limit: `MAX_SEND_QUEUE` (fast check, approximate)
/// - Hard limit: `max_write_buffer_size` (kernel-level, exact)
const MAX_SEND_QUEUE: usize = 256;

/// Handle a single WebSocket connection.
///
/// Protocol: Client sends JSON `{"cmd": "GET", "args": ["key"], "db": 0, "id": "req-1"}`
/// Server responds with JSON `{"result": ..., "type": "string", "id": "req-1"}`
///
/// The `id` field is optional -- if present, echoed back for request correlation.
/// The `db` field is optional -- if omitted, uses the session's current database.
/// `SELECT` is handled locally (changes session db for subsequent commands).
pub async fn handle_ws_connection(ws_stream: HyperWebsocketStream, gateway: Arc<ConsoleGateway>) {
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    let mut selected_db: usize = 0;
    let mut send_queue_depth: usize = 0;

    while let Some(msg) = ws_receiver.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                tracing::debug!("WebSocket read error: {}", e);
                break;
            }
        };

        match msg {
            Message::Text(text) => {
                let response = process_ws_message(&text, &gateway, &mut selected_db).await;
                let response_text = serde_json::to_string(&response).unwrap_or_default();

                // Drop-if-full policy for slow consumers (GW-08).
                // This is a soft limit; the tungstenite max_write_buffer_size
                // configured at upgrade time is the hard limit.
                if send_queue_depth >= MAX_SEND_QUEUE {
                    tracing::warn!(
                        "WebSocket send queue full ({} pending), dropping message",
                        send_queue_depth
                    );
                    continue;
                }

                send_queue_depth += 1;
                if ws_sender.send(Message::text(response_text)).await.is_err() {
                    tracing::debug!("WebSocket send error, closing connection");
                    break;
                }
                // Decrement after successful send. Since we await each send,
                // depth stays near 0 for well-behaved clients.
                send_queue_depth = send_queue_depth.saturating_sub(1);
            }
            Message::Binary(_) => {
                let err =
                    serde_json::json!({"error": "Binary messages not supported. Use JSON text."});
                let _ = ws_sender.send(Message::text(err.to_string())).await;
            }
            Message::Ping(_) => {
                // tungstenite handles pong automatically
            }
            Message::Pong(_) => {}
            Message::Close(_) => {
                tracing::debug!("WebSocket close received");
                break;
            }
            Message::Frame(_) => {}
        }
    }

    // Connection cleanup
    let _ = ws_sender.close().await;
    tracing::debug!("WebSocket connection closed");
}

/// Process a single WebSocket text message.
///
/// Expected JSON format:
/// ```json
/// {"cmd": "GET", "args": ["mykey"], "db": 0, "id": "req-123"}
/// ```
///
/// Returns JSON:
/// ```json
/// {"result": "value", "type": "string", "id": "req-123"}
/// ```
async fn process_ws_message(
    text: &str,
    gateway: &ConsoleGateway,
    selected_db: &mut usize,
) -> serde_json::Value {
    let parsed: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            return serde_json::json!({
                "error": format!("Invalid JSON: {}", e)
            });
        }
    };

    // Extract request_id FIRST so error responses can echo it back and
    // clients can correlate them to pending promises (preventing timeouts
    // on malformed input).
    let request_id = parsed.get("id").cloned();

    let cmd = match parsed.get("cmd").and_then(|v| v.as_str()) {
        Some(c) => c.to_uppercase(),
        None => {
            let mut resp = serde_json::json!({
                "error": "Missing 'cmd' field"
            });
            if let Some(id) = request_id {
                resp["id"] = id;
            }
            return resp;
        }
    };

    // Handle SELECT locally (changes session db).
    if cmd == "SELECT" {
        if let Some(db) = parsed
            .get("args")
            .and_then(|a| a.as_array())
            .and_then(|a| a.first())
            .and_then(|v| v.as_str().or_else(|| v.as_u64().map(|_| "")))
        {
            // Try parsing from string representation or direct integer
            let db_num = if db.is_empty() {
                parsed
                    .get("args")
                    .and_then(|a| a.as_array())
                    .and_then(|a| a.first())
                    .and_then(|v| v.as_u64())
                    .map(|n| n as usize)
            } else {
                db.parse::<usize>().ok()
            };

            if let Some(db_num) = db_num {
                *selected_db = db_num;
                let mut resp = serde_json::json!({"result": "OK", "type": "simple_string"});
                if let Some(id) = request_id {
                    resp["id"] = id;
                }
                return resp;
            }
        }
    }

    // If request specifies db, use that; otherwise use session db.
    let db_index = parsed
        .get("db")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)
        .unwrap_or(*selected_db);

    // Parse args -- supports both strings and numbers in the array.
    let args: Vec<Bytes> = parsed
        .get("args")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .map(|v| match v.as_str() {
                    Some(s) => Bytes::from(s.to_string()),
                    None => Bytes::from(v.to_string()),
                })
                .collect()
        })
        .unwrap_or_default();

    // Execute command via gateway.
    match gateway.execute_command(db_index, &cmd, &args).await {
        Ok(frame) => {
            let result = ConsoleGateway::frame_to_json(&frame);
            let frame_type = frame_type_name(&frame);
            let mut resp = serde_json::json!({
                "result": result,
                "type": frame_type,
            });
            if let Some(id) = request_id {
                resp["id"] = id;
            }
            resp
        }
        Err(e) => {
            let mut resp = serde_json::json!({"error": e});
            if let Some(id) = request_id {
                resp["id"] = id;
            }
            resp
        }
    }
}

/// Return a string name for the Frame type (for the "type" field in JSON responses).
fn frame_type_name(frame: &crate::protocol::Frame) -> &'static str {
    use crate::protocol::Frame;
    match frame {
        Frame::SimpleString(_) => "simple_string",
        Frame::Error(_) => "error",
        Frame::Integer(_) => "integer",
        Frame::BulkString(_) => "bulk_string",
        Frame::Array(_) => "array",
        Frame::Null => "null",
        Frame::Map(_) => "map",
        Frame::Set(_) => "set",
        Frame::Double(_) => "double",
        Frame::Boolean(_) => "boolean",
        Frame::VerbatimString { .. } => "verbatim_string",
        Frame::BigNumber(_) => "big_number",
        Frame::Push(_) => "push",
        Frame::PreSerialized(_) => "bulk_string",
    }
}
