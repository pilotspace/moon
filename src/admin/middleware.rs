//! Middleware chain for the admin HTTP server (HARD-01/02/03, Phase 137).
//!
//! Ordering (invoked once per request, before routing):
//!   1. CORS preflight — `OPTIONS` is always answered with 204 and the
//!      appropriate `Access-Control-Allow-*` headers. Never consumes auth
//!      nor rate budget.
//!   2. Auth — if [`AuthPolicy::is_required`], the `Authorization: Bearer
//!      <token>` header is verified (constant-time HMAC compare). Exempt
//!      paths (/healthz, /readyz, /metrics) bypass this stage; the exemption
//!      decision is made by the caller.
//!   3. Rate limit — per-IP token bucket. On exhaustion returns 429 with
//!      `Retry-After: N`.
//!   4. Route dispatch (done by `http_server::handle_request`).
//!
//! A helper [`attach_cors_headers`] is also provided to decorate the final
//! response with CORS headers so the browser accepts cross-origin reads.

use std::convert::Infallible;
use std::net::IpAddr;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};

use crate::admin::auth::AuthPolicy;
use crate::admin::cors::CorsPolicy;
use crate::admin::rate_limit::RateLimiter;

/// Shared policy handles injected into `AdminState`.
pub struct MiddlewareState {
    pub auth: Arc<AuthPolicy>,
    pub cors: Arc<CorsPolicy>,
    pub rate: Arc<RateLimiter>,
}

fn full_body(data: Bytes) -> BoxBody<Bytes, Infallible> {
    Full::new(data).map_err(|never| match never {}).boxed()
}

fn status_text(status: StatusCode, body: &'static str) -> Response<BoxBody<Bytes, Infallible>> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(full_body(Bytes::from_static(body.as_bytes())))
        .unwrap_or_else(|_| Response::new(full_body(Bytes::from_static(b"err"))))
}

/// Outcome of a middleware stage.
///
/// `Continue` means the request should proceed to the next stage; `Respond`
/// short-circuits the chain and returns the contained response immediately.
pub enum MiddlewareOutcome {
    Continue,
    Respond(Response<BoxBody<Bytes, Infallible>>),
}

/// Paths that bypass auth (liveness/readiness/metrics scraping).
///
/// Kept as a small function so callers can inline check and the list stays
/// in one place.
#[inline]
pub fn is_auth_exempt(path: &str) -> bool {
    matches!(path, "/healthz" | "/readyz" | "/metrics")
}

/// Handle CORS preflight.
///
/// If the request is `OPTIONS`, returns a 204 response (with the allowed
/// origin headers when the origin matches the policy). Otherwise returns
/// `Continue` so the outer routing path runs.
///
/// Per the CORS spec, preflight MUST complete even when credentials are
/// required, so this stage never invokes auth or rate-limit logic.
pub fn handle_preflight(req: &Request<Incoming>, cors: &CorsPolicy) -> MiddlewareOutcome {
    if req.method() != Method::OPTIONS {
        return MiddlewareOutcome::Continue;
    }
    let origin = req.headers().get("origin").and_then(|v| v.to_str().ok());

    let mut builder = Response::builder().status(StatusCode::NO_CONTENT);
    if let Some(allowed) = cors.allowed_header(origin) {
        builder = builder
            .header("access-control-allow-origin", allowed)
            .header(
                "access-control-allow-methods",
                "GET, POST, PUT, DELETE, OPTIONS",
            )
            .header(
                "access-control-allow-headers",
                "authorization, content-type",
            )
            .header("access-control-max-age", "3600")
            .header("vary", "origin");
    }
    let resp = builder
        .body(full_body(Bytes::new()))
        .unwrap_or_else(|_| Response::new(full_body(Bytes::new())));
    MiddlewareOutcome::Respond(resp)
}

/// Verify Bearer auth. When auth is disabled, returns `Continue` immediately.
pub fn check_auth(req: &Request<Incoming>, auth: &AuthPolicy) -> MiddlewareOutcome {
    if !auth.is_required() {
        return MiddlewareOutcome::Continue;
    }
    let hdr = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok());
    match auth.verify_header(hdr) {
        Ok(_user) => MiddlewareOutcome::Continue,
        Err(_) => {
            let mut resp = status_text(StatusCode::UNAUTHORIZED, "Unauthorized");
            resp.headers_mut().insert(
                "www-authenticate",
                hyper::header::HeaderValue::from_static("Bearer realm=\"moon-console\""),
            );
            MiddlewareOutcome::Respond(resp)
        }
    }
}

/// Consume one token from the per-IP bucket. Returns 429 with `Retry-After`
/// when the bucket is empty.
pub fn check_rate_limit(ip: IpAddr, rate: &RateLimiter) -> MiddlewareOutcome {
    match rate.check(ip) {
        Ok(()) => MiddlewareOutcome::Continue,
        Err(retry_after) => {
            let mut resp = status_text(StatusCode::TOO_MANY_REQUESTS, "Rate limited");
            let hv = hyper::header::HeaderValue::from_str(&retry_after.to_string())
                .unwrap_or(hyper::header::HeaderValue::from_static("1"));
            resp.headers_mut().insert("retry-after", hv);
            MiddlewareOutcome::Respond(resp)
        }
    }
}

/// Attach `Access-Control-Allow-Origin` (and credentials) to the final
/// response based on the request `Origin` header and the policy. Call once
/// after route dispatch so downstream handlers stay policy-agnostic.
pub fn attach_cors_headers(
    resp: &mut Response<BoxBody<Bytes, Infallible>>,
    origin: Option<&str>,
    cors: &CorsPolicy,
) {
    if let Some(allowed) = cors.allowed_header(origin) {
        if let Ok(hv) = hyper::header::HeaderValue::from_str(&allowed) {
            resp.headers_mut().insert("access-control-allow-origin", hv);
        }
        // credentials=true only makes sense when the allowed origin is a
        // concrete value (not `*`). When wildcard, browsers reject
        // credentials; elide the header in that case.
        if !cors.is_wildcard() {
            resp.headers_mut().insert(
                "access-control-allow-credentials",
                hyper::header::HeaderValue::from_static("true"),
            );
            resp.headers_mut()
                .insert("vary", hyper::header::HeaderValue::from_static("origin"));
        }
    }
}

#[cfg(test)]
mod tests {
    //! Pure-logic unit tests. Full request-level coverage lives in
    //! `tests/admin_auth_cors_ratelimit.rs` which spawns a real binary.

    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn is_auth_exempt_recognises_probes() {
        assert!(is_auth_exempt("/healthz"));
        assert!(is_auth_exempt("/readyz"));
        assert!(is_auth_exempt("/metrics"));
        assert!(!is_auth_exempt("/api/v1/info"));
        assert!(!is_auth_exempt("/healthz2"));
    }

    #[tokio::test]
    async fn rate_limit_blocks_after_burst() {
        let rate = RateLimiter::new(1.0, 1.0);
        let ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
        assert!(matches!(
            check_rate_limit(ip, &rate),
            MiddlewareOutcome::Continue
        ));
        match check_rate_limit(ip, &rate) {
            MiddlewareOutcome::Respond(r) => {
                assert_eq!(r.status(), StatusCode::TOO_MANY_REQUESTS);
                assert!(r.headers().get("retry-after").is_some());
            }
            _ => panic!("expected 429"),
        }
    }

    #[test]
    fn attach_cors_adds_origin_for_allowed() {
        let cors = CorsPolicy::new(&["https://ok.example".to_string()], true).unwrap();
        let mut resp: Response<BoxBody<Bytes, Infallible>> = Response::new(full_body(Bytes::new()));
        attach_cors_headers(&mut resp, Some("https://ok.example"), &cors);
        assert_eq!(
            resp.headers()
                .get("access-control-allow-origin")
                .and_then(|v| v.to_str().ok()),
            Some("https://ok.example")
        );
        assert_eq!(
            resp.headers()
                .get("access-control-allow-credentials")
                .and_then(|v| v.to_str().ok()),
            Some("true")
        );
    }

    #[test]
    fn attach_cors_elides_header_for_disallowed() {
        let cors = CorsPolicy::new(&["https://ok.example".to_string()], true).unwrap();
        let mut resp: Response<BoxBody<Bytes, Infallible>> = Response::new(full_body(Bytes::new()));
        attach_cors_headers(&mut resp, Some("https://evil.example"), &cors);
        assert!(resp.headers().get("access-control-allow-origin").is_none());
        assert!(
            resp.headers()
                .get("access-control-allow-credentials")
                .is_none()
        );
    }

    #[test]
    fn attach_cors_wildcard_no_credentials() {
        let cors = CorsPolicy::new(&["*".to_string()], false).unwrap();
        let mut resp: Response<BoxBody<Bytes, Infallible>> = Response::new(full_body(Bytes::new()));
        attach_cors_headers(&mut resp, Some("https://any.example"), &cors);
        assert_eq!(
            resp.headers()
                .get("access-control-allow-origin")
                .and_then(|v| v.to_str().ok()),
            Some("*")
        );
        // Wildcard must NOT emit credentials=true (browser rejects that combo).
        assert!(
            resp.headers()
                .get("access-control-allow-credentials")
                .is_none()
        );
    }
}
