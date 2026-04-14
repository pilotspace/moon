//! Custom admin HTTP server for `/metrics`, `/healthz`, `/readyz`, and REST API endpoints.
//!
//! Serves health/readiness probes alongside Prometheus metrics on a single port.
//! When the `console` feature is enabled, also serves REST API at `/api/v1/*`
//! for command execution, key CRUD, and server introspection.

use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use metrics_exporter_prometheus::PrometheusHandle;

/// Shared state for the admin HTTP server.
struct AdminState {
    prometheus_handle: PrometheusHandle,
    ready: Arc<AtomicBool>,
    // Hardening policies (HARD-01/02/03, Phase 137). Built under the
    // `console` feature because the admin REST API only ships there; the
    // basic /healthz,/readyz,/metrics triplet in non-console builds has no
    // attack surface to harden.
    #[cfg(feature = "console")]
    auth: Arc<crate::admin::auth::AuthPolicy>,
    #[cfg(feature = "console")]
    cors: Arc<crate::admin::cors::CorsPolicy>,
    #[cfg(feature = "console")]
    rate: Arc<crate::admin::rate_limit::RateLimiter>,
}

/// Wrap `Full<Bytes>` into a `BoxBody` for unified response types.
fn full_body(data: Bytes) -> BoxBody<Bytes, Infallible> {
    Full::new(data).map_err(|never| match never {}).boxed()
}

/// Build an HTTP response with the given status and body.
fn response(status: StatusCode, body: &'static str) -> Response<BoxBody<Bytes, Infallible>> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(full_body(Bytes::from_static(body.as_bytes())))
        .unwrap_or_else(|_| Response::new(full_body(Bytes::from_static(b"Internal Server Error"))))
}

// ---------------------------------------------------------------------------
// Console HTTP helpers
// ---------------------------------------------------------------------------
//
// Shared helpers (`json_response`, `add_cors_headers`, `percent_decode`,
// `parse_query_params`) now live in `crate::admin::http_server_support` so
// per-endpoint handler modules (`memory_treemap`, `hnsw_trace`) can reuse
// them without duplicating code.

#[cfg(feature = "console")]
use crate::admin::http_server_support::{json_response, parse_query_params, percent_decode};

#[cfg(feature = "console")]
async fn read_body(req: Request<Incoming>) -> Result<Bytes, Response<BoxBody<Bytes, Infallible>>> {
    match req.collect().await {
        Ok(collected) => Ok(collected.to_bytes()),
        Err(_) => Err(json_response(
            StatusCode::BAD_REQUEST,
            &serde_json::json!({"error": "Failed to read request body"}),
        )),
    }
}

/// Extract key name from a TTL path like `/api/v1/key/{name}/ttl`.
#[cfg(feature = "console")]
fn extract_key_from_ttl_path(path: &str) -> String {
    let stripped = path
        .strip_prefix("/api/v1/key/")
        .unwrap_or("")
        .strip_suffix("/ttl")
        .unwrap_or("");
    percent_decode(stripped)
}

// ---------------------------------------------------------------------------
// Request routing
// ---------------------------------------------------------------------------

/// Route incoming requests to the appropriate handler.
///
/// When the `console` feature is enabled, this function runs the
/// HARD-01/02/03 middleware chain (CORS preflight -> Auth -> Rate limit)
/// before dispatching to per-route handlers. `/healthz`, `/readyz`, and
/// `/metrics` bypass auth so probes keep working when operators enable
/// `--console-auth-required`.
#[allow(unused_mut)] // `req` is mutated only when `console` feature is enabled (WebSocket upgrade)
#[allow(unused_variables)] // `remote_ip` is only consumed under `console`
async fn handle_request(
    mut req: Request<Incoming>,
    state: Arc<AdminState>,
    remote_ip: IpAddr,
) -> Result<Response<BoxBody<Bytes, Infallible>>, Infallible> {
    let path = req.uri().path().to_string();

    // ── Middleware chain (console feature only) ────────────────────
    // Order is deliberate: CORS preflight MUST complete even when auth is
    // required; rate limit runs after auth so anonymous flooding doesn't
    // DoS the auth-compute path.
    #[cfg(feature = "console")]
    {
        use crate::admin::middleware::{self, MiddlewareOutcome};

        // 1) CORS preflight short-circuit.
        if let MiddlewareOutcome::Respond(r) = middleware::handle_preflight(&req, &state.cors) {
            return Ok(r);
        }
        // 2) Auth (liveness/readiness/metrics are exempt).
        if !middleware::is_auth_exempt(&path) {
            if let MiddlewareOutcome::Respond(r) = middleware::check_auth(&req, &state.auth) {
                return Ok(r);
            }
        }
        // 3) Rate limit (always on; disabled limiter is a cheap no-op).
        if let MiddlewareOutcome::Respond(r) = middleware::check_rate_limit(remote_ip, &state.rate)
        {
            return Ok(r);
        }
    }

    // Capture request Origin for CORS header attachment on the response.
    #[cfg(feature = "console")]
    let request_origin: Option<String> = req
        .headers()
        .get("origin")
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    let resp = match path.as_str() {
        "/healthz" => response(StatusCode::OK, "OK"),

        "/readyz" => {
            if state.ready.load(Ordering::Relaxed) {
                response(StatusCode::OK, "OK")
            } else {
                response(StatusCode::SERVICE_UNAVAILABLE, "NOT READY")
            }
        }

        "/metrics" => {
            // Run upkeep to flush pending metric values before rendering.
            state.prometheus_handle.run_upkeep();
            let rendered = state.prometheus_handle.render();
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
                .body(full_body(Bytes::from(rendered)))
                .unwrap_or_else(|_| {
                    Response::new(full_body(Bytes::from_static(b"Internal Server Error")))
                })
        }

        // SSE endpoint for real-time metric streaming (console feature only).
        #[cfg(feature = "console")]
        "/events" => {
            return Ok(crate::admin::sse_stream::handle_sse_stream());
        }

        // WebSocket upgrade at /ws for interactive command sessions.
        #[cfg(feature = "console")]
        "/ws" => {
            if hyper_tungstenite::is_upgrade_request(&req) {
                match crate::admin::console_gateway::get_global_gateway() {
                    Some(gw) => {
                        let mut ws_config =
                            hyper_tungstenite::tungstenite::protocol::WebSocketConfig::default();
                        // 4 MB hard limit on outgoing buffer (GW-08).
                        ws_config.max_write_buffer_size = 4 * 1024 * 1024;
                        // 64 KB max incoming message/frame.
                        ws_config.max_message_size = Some(64 * 1024);
                        ws_config.max_frame_size = Some(64 * 1024);
                        match hyper_tungstenite::upgrade(&mut req, Some(ws_config)) {
                            Ok((ws_response, ws_future)) => {
                                let gw = gw.clone();
                                tokio::spawn(async move {
                                    match ws_future.await {
                                        Ok(ws_stream) => {
                                            crate::admin::ws_bridge::handle_ws_connection(
                                                ws_stream, gw,
                                            )
                                            .await;
                                        }
                                        Err(e) => {
                                            tracing::debug!("WebSocket upgrade failed: {}", e);
                                        }
                                    }
                                });
                                // Return 101 Switching Protocols to complete the upgrade.
                                return Ok(
                                    ws_response.map(|b| b.map_err(|never| match never {}).boxed())
                                );
                            }
                            Err(e) => {
                                tracing::debug!("WebSocket upgrade error: {}", e);
                                response(StatusCode::BAD_REQUEST, "WebSocket upgrade failed")
                            }
                        }
                    }
                    None => response(StatusCode::SERVICE_UNAVAILABLE, "Console not initialized"),
                }
            } else {
                response(StatusCode::BAD_REQUEST, "Expected WebSocket upgrade")
            }
        }

        #[cfg(feature = "console")]
        p if p.starts_with("/api/v1/") => {
            match crate::admin::console_gateway::get_global_gateway() {
                Some(gw) => handle_api_request(req, &path, gw).await,
                None => response(StatusCode::SERVICE_UNAVAILABLE, "Console not initialized"),
            }
        }

        // OPTIONS is handled by the middleware preflight stage above; if a
        // non-OPTIONS request somehow reaches this fallback branch under the
        // `console` feature we fall through to static file serving (SPA
        // fallback). Any stray OPTIONS that misses the preflight handler is
        // answered with the SPA 404 which is semantically harmless.

        // Static file serving with SPA fallback (console feature only).
        #[cfg(feature = "console")]
        _ => {
            let path = req.uri().path();
            let resp = crate::admin::static_files::serve_static_file(path);
            resp.map(|b| b.map_err(|never| match never {}).boxed())
        }

        #[cfg(not(feature = "console"))]
        _ => response(StatusCode::NOT_FOUND, "Not Found"),
    };

    // Attach CORS headers per policy to the final response. `insert`
    // overwrites any stale header set by a handler.
    #[cfg(feature = "console")]
    let mut resp = resp;
    #[cfg(feature = "console")]
    crate::admin::middleware::attach_cors_headers(
        &mut resp,
        request_origin.as_deref(),
        &state.cors,
    );
    Ok(resp)
}

// ---------------------------------------------------------------------------
// REST API router (console feature only)
// ---------------------------------------------------------------------------

#[cfg(feature = "console")]
async fn handle_api_request(
    req: Request<Incoming>,
    path: &str,
    gw: &crate::admin::console_gateway::ConsoleGateway,
) -> Response<BoxBody<Bytes, Infallible>> {
    use hyper::Method;

    let method = req.method().clone();
    let query = req.uri().query().unwrap_or("").to_string();

    // TTL routes must match before general key routes (both start with /api/v1/key/).
    match (&method, path) {
        // GET /api/v1/key/{name}/ttl
        (&Method::GET, p) if p.starts_with("/api/v1/key/") && p.ends_with("/ttl") => {
            let key = extract_key_from_ttl_path(p);
            handle_get_ttl(&key, gw).await
        }

        // PUT /api/v1/key/{name}/ttl
        (&Method::PUT, p) if p.starts_with("/api/v1/key/") && p.ends_with("/ttl") => {
            let key = extract_key_from_ttl_path(p);
            handle_set_ttl(&key, req, gw).await
        }

        // POST /api/v1/command -- execute arbitrary RESP command
        (&Method::POST, "/api/v1/command") => handle_command(req, gw).await,

        // GET /api/v1/keys?pattern=*&cursor=0&count=100 -- SCAN
        (&Method::GET, "/api/v1/keys") => handle_scan_keys(&query, gw).await,

        // GET /api/v1/key/{name} -- get key value
        (&Method::GET, p) if p.starts_with("/api/v1/key/") => {
            let key = percent_decode(p.strip_prefix("/api/v1/key/").unwrap_or(""));
            handle_get_key(&key, &query, gw).await
        }

        // PUT /api/v1/key/{name} -- set key value
        (&Method::PUT, p) if p.starts_with("/api/v1/key/") => {
            let key = percent_decode(p.strip_prefix("/api/v1/key/").unwrap_or(""));
            handle_set_key(&key, req, gw).await
        }

        // DELETE /api/v1/key/{name} -- delete key
        (&Method::DELETE, p) if p.starts_with("/api/v1/key/") => {
            let key = percent_decode(p.strip_prefix("/api/v1/key/").unwrap_or(""));
            handle_delete_key(&key, gw).await
        }

        // GET /api/v1/info -- server info
        (&Method::GET, "/api/v1/info") => handle_info(gw).await,

        // GET /api/v1/memory/treemap?prefix=&limit=N -- server-side memory tree (INT-01)
        (&Method::GET, "/api/v1/memory/treemap") => {
            crate::admin::memory_treemap::handle_memory_treemap(&query, gw).await
        }

        // GET /api/v1/hnsw/trace?index=...&query=<b64>&k=N&ef_search=N -- HNSW trace (INT-02)
        (&Method::GET, "/api/v1/hnsw/trace") => {
            crate::admin::hnsw_trace::handle_hnsw_trace(&query, gw).await
        }

        _ => json_response(
            StatusCode::NOT_FOUND,
            &serde_json::json!({"error": "Not found"}),
        ),
    }
}

// ---------------------------------------------------------------------------
// Handler implementations (console feature only)
// ---------------------------------------------------------------------------

/// POST /api/v1/command -- execute arbitrary RESP command.
/// Body: `{"cmd": "GET", "args": ["key"], "db": 0}`
#[cfg(feature = "console")]
async fn handle_command(
    req: Request<Incoming>,
    gw: &crate::admin::console_gateway::ConsoleGateway,
) -> Response<BoxBody<Bytes, Infallible>> {
    let body = match read_body(req).await {
        Ok(b) => b,
        Err(resp) => return resp,
    };

    let parsed: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &serde_json::json!({"error": format!("Invalid JSON: {}", e)}),
            );
        }
    };

    let cmd = match parsed["cmd"].as_str() {
        Some(c) => c,
        None => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &serde_json::json!({"error": "Missing 'cmd' field"}),
            );
        }
    };

    let args: Vec<Bytes> = parsed["args"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .map(|v| match v.as_str() {
                    Some(s) => Bytes::from(s.to_string()),
                    None => Bytes::from(v.to_string()),
                })
                .collect()
        })
        .unwrap_or_default();

    let db_index = parsed["db"].as_u64().unwrap_or(0) as usize;

    match gw.execute_command(db_index, cmd, &args).await {
        Ok(frame) => {
            let result = crate::admin::console_gateway::ConsoleGateway::frame_to_json(&frame);
            let type_name = frame_type_name(&frame);
            json_response(
                StatusCode::OK,
                &serde_json::json!({"result": result, "type": type_name}),
            )
        }
        Err(e) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &serde_json::json!({"error": e}),
        ),
    }
}

/// Return a short type name for a Frame variant.
#[cfg(feature = "console")]
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
        Frame::PreSerialized(_) => "pre_serialized",
    }
}

/// GET /api/v1/keys?pattern=*&cursor=0&count=100 -- SCAN keys across all shards.
///
/// Fans out SCAN to every shard via a composite cursor of the form
/// `"<shard_id>:<per_shard_cursor>"`. Returns the unified cursor to the caller;
/// consumers paginate until `cursor == "0"`. See `crate::admin::scan_fanout`.
#[cfg(feature = "console")]
async fn handle_scan_keys(
    query: &str,
    gw: &crate::admin::console_gateway::ConsoleGateway,
) -> Response<BoxBody<Bytes, Infallible>> {
    use crate::admin::scan_fanout::{Cursor, scan_all_shards};

    let params = parse_query_params(query);
    let pattern = params.get("pattern").map(|s| s.as_str()).unwrap_or("*");
    let cursor_str = params.get("cursor").map(|s| s.as_str()).unwrap_or("0");
    let count: u64 = params
        .get("count")
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let cursor = match Cursor::parse(cursor_str, gw.num_shards()) {
        Ok(c) => c,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &serde_json::json!({"error": format!("invalid cursor: {}", e)}),
            );
        }
    };

    match scan_all_shards(gw, 0, cursor, pattern, count).await {
        Ok((next, keys)) => {
            let key_vals: Vec<serde_json::Value> = keys
                .iter()
                .map(|b| match std::str::from_utf8(b) {
                    Ok(s) => serde_json::Value::String(s.to_string()),
                    Err(_) => {
                        use base64::Engine;
                        let enc = base64::engine::general_purpose::STANDARD.encode(b.as_ref());
                        serde_json::json!({ "base64": enc })
                    }
                })
                .collect();
            json_response(
                StatusCode::OK,
                &serde_json::json!({
                    "cursor": next.encode(),
                    "keys": key_vals,
                }),
            )
        }
        Err(e) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &serde_json::json!({"error": e}),
        ),
    }
}

/// GET /api/v1/key/{name} -- get key value with type detection.
#[cfg(feature = "console")]
async fn handle_get_key(
    key: &str,
    _query: &str,
    gw: &crate::admin::console_gateway::ConsoleGateway,
) -> Response<BoxBody<Bytes, Infallible>> {
    let key_bytes = Bytes::from(key.to_string());

    // First, get the key type.
    let type_result = match gw
        .execute_command(0, "TYPE", std::slice::from_ref(&key_bytes))
        .await
    {
        Ok(f) => f,
        Err(e) => {
            return json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &serde_json::json!({"error": e}),
            );
        }
    };

    let type_str = match &type_result {
        crate::protocol::Frame::SimpleString(b) | crate::protocol::Frame::BulkString(b) => {
            String::from_utf8_lossy(b).to_string()
        }
        _ => "none".to_string(),
    };

    if type_str == "none" {
        return json_response(
            StatusCode::NOT_FOUND,
            &serde_json::json!({"error": "Key not found"}),
        );
    }

    // Fetch value based on type.
    let (cmd, args): (&str, Vec<Bytes>) = match type_str.as_str() {
        "string" => ("GET", vec![key_bytes]),
        "hash" => ("HGETALL", vec![key_bytes]),
        "list" => (
            "LRANGE",
            vec![
                key_bytes,
                Bytes::from_static(b"0"),
                Bytes::from_static(b"-1"),
            ],
        ),
        "set" => ("SMEMBERS", vec![key_bytes]),
        "zset" => (
            "ZRANGE",
            vec![
                key_bytes,
                Bytes::from_static(b"0"),
                Bytes::from_static(b"-1"),
                Bytes::from_static(b"WITHSCORES"),
            ],
        ),
        other => {
            return json_response(
                StatusCode::OK,
                &serde_json::json!({"type": other, "value": null}),
            );
        }
    };

    match gw.execute_command(0, cmd, &args).await {
        Ok(frame) => {
            let value = crate::admin::console_gateway::ConsoleGateway::frame_to_json(&frame);
            json_response(
                StatusCode::OK,
                &serde_json::json!({"type": type_str, "value": value}),
            )
        }
        Err(e) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &serde_json::json!({"error": e}),
        ),
    }
}

/// PUT /api/v1/key/{name} -- set key value.
/// Body: `{"value": "...", "type": "string"}`
#[cfg(feature = "console")]
async fn handle_set_key(
    key: &str,
    req: Request<Incoming>,
    gw: &crate::admin::console_gateway::ConsoleGateway,
) -> Response<BoxBody<Bytes, Infallible>> {
    let body = match read_body(req).await {
        Ok(b) => b,
        Err(resp) => return resp,
    };

    let parsed: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &serde_json::json!({"error": format!("Invalid JSON: {}", e)}),
            );
        }
    };

    let value = match parsed["value"].as_str() {
        Some(v) => v,
        None => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &serde_json::json!({"error": "Missing 'value' field (must be string)"}),
            );
        }
    };

    let key_bytes = Bytes::from(key.to_string());
    let val_bytes = Bytes::from(value.to_string());

    match gw.execute_command(0, "SET", &[key_bytes, val_bytes]).await {
        Ok(_) => json_response(StatusCode::OK, &serde_json::json!({"ok": true})),
        Err(e) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &serde_json::json!({"error": e}),
        ),
    }
}

/// DELETE /api/v1/key/{name} -- delete a key.
#[cfg(feature = "console")]
async fn handle_delete_key(
    key: &str,
    gw: &crate::admin::console_gateway::ConsoleGateway,
) -> Response<BoxBody<Bytes, Infallible>> {
    let key_bytes = Bytes::from(key.to_string());

    match gw.execute_command(0, "DEL", &[key_bytes]).await {
        Ok(frame) => {
            let deleted = match &frame {
                crate::protocol::Frame::Integer(n) => *n,
                _ => 0,
            };
            json_response(StatusCode::OK, &serde_json::json!({"deleted": deleted}))
        }
        Err(e) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &serde_json::json!({"error": e}),
        ),
    }
}

/// GET /api/v1/key/{name}/ttl -- get key TTL.
#[cfg(feature = "console")]
async fn handle_get_ttl(
    key: &str,
    gw: &crate::admin::console_gateway::ConsoleGateway,
) -> Response<BoxBody<Bytes, Infallible>> {
    let key_bytes = Bytes::from(key.to_string());

    match gw.execute_command(0, "TTL", &[key_bytes]).await {
        Ok(frame) => {
            let ttl = match &frame {
                crate::protocol::Frame::Integer(n) => *n,
                _ => -2,
            };
            json_response(StatusCode::OK, &serde_json::json!({"ttl": ttl}))
        }
        Err(e) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &serde_json::json!({"error": e}),
        ),
    }
}

/// PUT /api/v1/key/{name}/ttl -- set key TTL.
/// Body: `{"ttl": N}` where N > 0 sets EXPIRE, N == -1 means PERSIST.
#[cfg(feature = "console")]
async fn handle_set_ttl(
    key: &str,
    req: Request<Incoming>,
    gw: &crate::admin::console_gateway::ConsoleGateway,
) -> Response<BoxBody<Bytes, Infallible>> {
    let body = match read_body(req).await {
        Ok(b) => b,
        Err(resp) => return resp,
    };

    let parsed: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &serde_json::json!({"error": format!("Invalid JSON: {}", e)}),
            );
        }
    };

    let ttl = match parsed["ttl"].as_i64() {
        Some(t) => t,
        None => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &serde_json::json!({"error": "Missing 'ttl' field (must be integer)"}),
            );
        }
    };

    let key_bytes = Bytes::from(key.to_string());

    let result = if ttl == -1 {
        gw.execute_command(0, "PERSIST", &[key_bytes]).await
    } else if ttl > 0 {
        let ttl_bytes = Bytes::from(ttl.to_string());
        gw.execute_command(0, "EXPIRE", &[key_bytes, ttl_bytes])
            .await
    } else {
        return json_response(
            StatusCode::BAD_REQUEST,
            &serde_json::json!({"error": "TTL must be > 0 or -1 (persist)"}),
        );
    };

    match result {
        Ok(_) => json_response(StatusCode::OK, &serde_json::json!({"ok": true})),
        Err(e) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &serde_json::json!({"error": e}),
        ),
    }
}

/// GET /api/v1/info -- server info.
#[cfg(feature = "console")]
async fn handle_info(
    gw: &crate::admin::console_gateway::ConsoleGateway,
) -> Response<BoxBody<Bytes, Infallible>> {
    match gw.execute_command(0, "INFO", &[]).await {
        Ok(frame) => {
            let info_str = match &frame {
                crate::protocol::Frame::BulkString(b) => String::from_utf8_lossy(b).to_string(),
                other => {
                    let json = crate::admin::console_gateway::ConsoleGateway::frame_to_json(other);
                    json.to_string()
                }
            };
            json_response(StatusCode::OK, &serde_json::json!({"info": info_str}))
        }
        Err(e) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &serde_json::json!({"error": e}),
        ),
    }
}

// ---------------------------------------------------------------------------
// Server spawn
// ---------------------------------------------------------------------------

/// Spawn the admin HTTP server on a dedicated thread.
///
/// The server uses a single-threaded tokio runtime so it works regardless of
/// which async runtime (monoio / tokio) the main server uses.
///
/// When the `console` feature is enabled, the auth + CORS policies are
/// passed in by the caller. The rate limiter is constructed inside the
/// spawned admin runtime (its cleanup task needs `tokio::spawn`).
pub fn spawn_admin_server(
    addr: SocketAddr,
    prometheus_handle: PrometheusHandle,
    ready: Arc<AtomicBool>,
    #[cfg(feature = "console")] auth: Arc<crate::admin::auth::AuthPolicy>,
    #[cfg(feature = "console")] cors: Arc<crate::admin::cors::CorsPolicy>,
    #[cfg(feature = "console")] rate_limit_rps: f64,
    #[cfg(feature = "console")] rate_limit_burst: f64,
) {
    if let Err(e) = std::thread::Builder::new()
        .name("admin-http".to_string())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    tracing::error!("Failed to build admin-http runtime: {}", e);
                    return;
                }
            };

            rt.block_on(async move {
                // Build state after the admin runtime exists so the
                // RateLimiter can spawn its cleanup task (HARD-03).
                #[cfg(feature = "console")]
                let rate =
                    crate::admin::rate_limit::RateLimiter::new(rate_limit_rps, rate_limit_burst);
                let state = Arc::new(AdminState {
                    prometheus_handle,
                    ready,
                    #[cfg(feature = "console")]
                    auth,
                    #[cfg(feature = "console")]
                    cors,
                    #[cfg(feature = "console")]
                    rate,
                });

                let listener = match tokio::net::TcpListener::bind(addr).await {
                    Ok(l) => l,
                    Err(e) => {
                        tracing::error!("Admin HTTP server failed to bind {}: {}", addr, e);
                        return;
                    }
                };
                tracing::info!("Admin HTTP server listening on {}", addr);

                #[cfg(feature = "console")]
                {
                    crate::admin::sse_stream::init_metrics_broadcast();
                    crate::admin::metrics_setup::spawn_metrics_publisher();
                }

                loop {
                    let (stream, peer) = match listener.accept().await {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::warn!("Admin HTTP accept error: {}", e);
                            continue;
                        }
                    };

                    let state = state.clone();
                    let peer_ip: IpAddr = peer.ip();
                    let io = hyper_util::rt::TokioIo::new(stream);

                    tokio::spawn(async move {
                        let mut builder = hyper_util::server::conn::auto::Builder::new(
                            hyper_util::rt::TokioExecutor::new(),
                        );
                        // Cap request header buffer to limit RSS from stray
                        // browser cookies (localhost shares cookies across
                        // ports — Supabase tokens alone add ~6KB/request).
                        builder.http1().max_buf_size(32 * 1024);
                        let conn = builder.serve_connection_with_upgrades(
                            io,
                            service_fn(move |req| {
                                let state = state.clone();
                                handle_request(req, state, peer_ip)
                            }),
                        );
                        if let Err(e) = conn.await {
                            tracing::debug!("Admin HTTP connection error: {}", e);
                        }
                    });
                }
            });
        })
    {
        tracing::error!("Failed to spawn admin-http thread: {}", e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_healthz_response() {
        let resp = response(StatusCode::OK, "OK");
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_readyz_not_ready() {
        let resp = response(StatusCode::SERVICE_UNAVAILABLE, "NOT READY");
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[cfg(feature = "console")]
    #[test]
    fn test_percent_decode() {
        assert_eq!(percent_decode("hello%20world"), "hello world");
        assert_eq!(percent_decode("key%3Avalue"), "key:value");
        assert_eq!(percent_decode("no%encoding"), "no%encoding"); // invalid hex
        assert_eq!(percent_decode("plain"), "plain");
        assert_eq!(percent_decode("a%2Fb"), "a/b");
    }

    #[cfg(feature = "console")]
    #[test]
    fn test_extract_key_from_ttl_path() {
        assert_eq!(extract_key_from_ttl_path("/api/v1/key/mykey/ttl"), "mykey");
        assert_eq!(
            extract_key_from_ttl_path("/api/v1/key/my%20key/ttl"),
            "my key"
        );
    }

    #[cfg(feature = "console")]
    #[test]
    fn test_parse_query_params() {
        let params = parse_query_params("pattern=*&cursor=0&count=100");
        assert_eq!(params.get("pattern").map(|s| s.as_str()), Some("*"));
        assert_eq!(params.get("cursor").map(|s| s.as_str()), Some("0"));
        assert_eq!(params.get("count").map(|s| s.as_str()), Some("100"));
    }

    #[cfg(feature = "console")]
    #[test]
    fn test_json_response_no_hardcoded_cors() {
        // Post-Phase-137 (HARD-02): CORS is policy-driven. `json_response`
        // no longer emits wildcard headers — middleware decides.
        let resp = json_response(StatusCode::OK, &serde_json::json!({"test": true}));
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(!resp.headers().contains_key("access-control-allow-origin"));
    }

    #[cfg(feature = "console")]
    #[test]
    fn test_frame_type_name() {
        use crate::protocol::Frame;
        assert_eq!(
            frame_type_name(&Frame::SimpleString(Bytes::from_static(b"OK"))),
            "simple_string"
        );
        assert_eq!(frame_type_name(&Frame::Integer(42)), "integer");
        assert_eq!(frame_type_name(&Frame::Null), "null");
    }
}
