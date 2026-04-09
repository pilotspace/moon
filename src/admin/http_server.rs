//! Custom admin HTTP server for `/metrics`, `/healthz`, and `/readyz` endpoints.
//!
//! Replaces the built-in `metrics-exporter-prometheus` HTTP listener so we can
//! serve health/readiness probes alongside Prometheus metrics on a single port.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use metrics_exporter_prometheus::PrometheusHandle;

/// Shared state for the admin HTTP server.
struct AdminState {
    prometheus_handle: PrometheusHandle,
    ready: Arc<AtomicBool>,
}

/// Build an HTTP response with the given status and body.
fn response(status: StatusCode, body: &'static str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Full::new(Bytes::from_static(body.as_bytes())))
        .unwrap()
}

/// Route incoming requests to the appropriate handler.
async fn handle_request(
    req: Request<Incoming>,
    state: Arc<AdminState>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let resp = match req.uri().path() {
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
                .header(
                    "content-type",
                    "text/plain; version=0.0.4; charset=utf-8",
                )
                .body(Full::new(Bytes::from(rendered)))
                .unwrap()
        }

        _ => response(StatusCode::NOT_FOUND, "Not Found"),
    };
    Ok(resp)
}

/// Spawn the admin HTTP server on a dedicated thread.
///
/// The server uses a single-threaded tokio runtime so it works regardless of
/// which async runtime (monoio / tokio) the main server uses.
pub fn spawn_admin_server(
    addr: SocketAddr,
    prometheus_handle: PrometheusHandle,
    ready: Arc<AtomicBool>,
) {
    let state = Arc::new(AdminState {
        prometheus_handle,
        ready,
    });

    std::thread::Builder::new()
        .name("admin-http".to_string())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build admin-http runtime");

            rt.block_on(async move {
                let listener = match tokio::net::TcpListener::bind(addr).await {
                    Ok(l) => l,
                    Err(e) => {
                        tracing::error!("Admin HTTP server failed to bind {}: {}", addr, e);
                        return;
                    }
                };
                tracing::info!("Admin HTTP server listening on {}", addr);

                loop {
                    let (stream, _) = match listener.accept().await {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::warn!("Admin HTTP accept error: {}", e);
                            continue;
                        }
                    };

                    let state = state.clone();
                    let io = hyper_util::rt::TokioIo::new(stream);

                    tokio::task::spawn_local(async move {
                        if let Err(e) = hyper::server::conn::http1::Builder::new()
                            .serve_connection(
                                io,
                                service_fn(move |req| {
                                    let state = state.clone();
                                    handle_request(req, state)
                                }),
                            )
                            .await
                        {
                            tracing::debug!("Admin HTTP connection error: {}", e);
                        }
                    });
                }
            });
        })
        .expect("failed to spawn admin-http thread");
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
}
