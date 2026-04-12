//! Shared HTTP helpers for console REST handlers.
//!
//! Extracted from `http_server.rs` so that per-endpoint handler modules
//! (`memory_treemap`, `hnsw_trace`, …) can share `json_response`,
//! `parse_query_params`, and `percent_decode` without cfg-soup re-exports.
//!
//! This module is feature-gated behind `console` at the parent (`mod.rs`).

use std::collections::HashMap;
use std::convert::Infallible;

use bytes::Bytes;
use http_body_util::BodyExt;
use http_body_util::Full;
use http_body_util::combinators::BoxBody;
use hyper::{Response, StatusCode};

// ---------------------------------------------------------------------------
// Body wrapping
// ---------------------------------------------------------------------------

/// Wrap `Full<Bytes>` into a `BoxBody` for unified response types.
#[inline]
pub(crate) fn full_body(data: Bytes) -> BoxBody<Bytes, Infallible> {
    Full::new(data).map_err(|never| match never {}).boxed()
}

// ---------------------------------------------------------------------------
// CORS
// ---------------------------------------------------------------------------

/// Attach permissive CORS headers to a response.
///
/// Shared across every JSON/API endpoint so the console (served from the
/// same admin port but possibly different origin during dev) can call it.
pub(crate) fn add_cors_headers<B>(resp: &mut Response<B>) {
    let h = resp.headers_mut();
    h.insert(
        "access-control-allow-origin",
        hyper::header::HeaderValue::from_static("*"),
    );
    h.insert(
        "access-control-allow-methods",
        hyper::header::HeaderValue::from_static("GET, POST, PUT, DELETE, OPTIONS"),
    );
    h.insert(
        "access-control-allow-headers",
        hyper::header::HeaderValue::from_static("content-type, authorization"),
    );
    h.insert(
        "access-control-max-age",
        hyper::header::HeaderValue::from_static("86400"),
    );
}

// ---------------------------------------------------------------------------
// JSON response builder
// ---------------------------------------------------------------------------

/// Build a JSON response with CORS headers attached.
///
/// On serialization failure (should be impossible for our value types), falls
/// back to an empty `{}` body so the caller always gets a well-formed HTTP
/// response.
pub(crate) fn json_response(
    status: StatusCode,
    value: &serde_json::Value,
) -> Response<BoxBody<Bytes, Infallible>> {
    let body = serde_json::to_vec(value).unwrap_or_default();
    let mut resp = match Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(full_body(Bytes::from(body)))
    {
        Ok(r) => r,
        Err(_) => Response::new(full_body(Bytes::from_static(b"{}"))),
    };
    add_cors_headers(&mut resp);
    resp
}

// ---------------------------------------------------------------------------
// Percent decode + query parsing
// ---------------------------------------------------------------------------

/// Minimal percent-decode for URL-encoded key names.
/// Handles `%XX` sequences (e.g., `%20` for space, `%3A` for colon).
///
/// Invalid escapes are passed through verbatim (so `"no%encoding"` stays
/// as-is). Always returns a `String`; non-UTF-8 bytes are replaced.
pub(crate) fn percent_decode(input: &str) -> String {
    let mut out = Vec::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = hex_val(bytes[i + 1]);
            let lo = hex_val(bytes[i + 2]);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push(h << 4 | l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).to_string())
}

#[inline]
fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Parse a query string (without leading `?`) into key/value pairs.
/// Values are percent-decoded.
pub(crate) fn parse_query_params(query: &str) -> HashMap<String, String> {
    query
        .split('&')
        .filter(|s| !s.is_empty())
        .filter_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next()?;
            let value = parts.next().unwrap_or("");
            Some((key.to_string(), percent_decode(value)))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_decode_handles_common_escapes() {
        assert_eq!(percent_decode("hello%20world"), "hello world");
        assert_eq!(percent_decode("key%3Avalue"), "key:value");
        assert_eq!(percent_decode("a%2Fb"), "a/b");
        assert_eq!(percent_decode("plain"), "plain");
    }

    #[test]
    fn percent_decode_passes_through_invalid_escape() {
        // "%en" isn't a valid hex escape — leave the `%` untouched.
        assert_eq!(percent_decode("no%encoding"), "no%encoding");
    }

    #[test]
    fn parse_query_params_basic() {
        let params = parse_query_params("pattern=*&cursor=0&count=100");
        assert_eq!(params.get("pattern").map(String::as_str), Some("*"));
        assert_eq!(params.get("cursor").map(String::as_str), Some("0"));
        assert_eq!(params.get("count").map(String::as_str), Some("100"));
    }

    #[test]
    fn parse_query_params_percent_decoded_values() {
        let params = parse_query_params("prefix=user%3A");
        assert_eq!(params.get("prefix").map(String::as_str), Some("user:"));
    }

    #[test]
    fn parse_query_params_empty() {
        let params = parse_query_params("");
        assert!(params.is_empty());
    }

    #[test]
    fn json_response_sets_cors_headers() {
        let resp = json_response(StatusCode::OK, &serde_json::json!({"ok": true}));
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.headers().contains_key("access-control-allow-origin"));
        assert!(resp.headers().contains_key("access-control-allow-methods"));
        assert_eq!(
            resp.headers()
                .get("content-type")
                .map(|v| v.to_str().unwrap_or("")),
            Some("application/json")
        );
    }
}
