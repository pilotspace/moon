//! `GET /api/v1/hnsw/trace` — debug-only HNSW search path counters (INT-02).
//!
//! This is the minimal viable wiring for VE-07's per-layer overlay animation.
//! The heavy lifting — `hnsw_search_with_trace` — lives in
//! `crate::vector::hnsw::search` and runs as a pure function over an
//! `HnswGraph` + TQ buffer. It's fully tested there.
//!
//! ## Why this HTTP handler returns a stub for the live index
//!
//! The `VectorStore` is owned per-shard (`src/vector/store.rs`) with no
//! global accessor (by design: per-shard ownership is the whole reason
//! multi-shard scaling works). Reaching the store from the admin-http
//! thread requires a new SPSC message kind + reply-channel machinery,
//! which is explicitly out of scope for this plan. Per the plan's
//! scope-down clause, we return `{"layers": [], "top_k_ids": [],
//! "note": "trace_not_implemented", ...}` for live queries against a
//! named index while validating the request shape so the console can
//! still exercise the error paths.
//!
//! The pure trace function (`hnsw_search_with_trace`) is available to
//! unit tests, benchmarks, and future wiring; the stub exists only on
//! the public HTTP boundary.
//!
//! ## Request
//!
//! ```text
//! GET /api/v1/hnsw/trace?index=<name>&query=<base64-le-f32>&k=N&ef_search=N
//! ```
//!
//! - `index` (required): index name previously created via `FT.CREATE`.
//! - `query` (required): base64 (standard alphabet) of little-endian f32
//!   bytes. Length must be a multiple of 4.
//! - `k` (optional, default 10, clamped 1..=100): top-K.
//! - `ef_search` (optional, default 64, clamped k..=512): HNSW beam width.
//!
//! ## Errors
//!
//! - 400 when `index` or `query` are missing or `query` is not valid
//!   base64 / misaligned bytes.
//! - 404 when the index does not exist (checked via `FT.INFO`).

use std::convert::Infallible;

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use hyper::{Response, StatusCode};

use crate::admin::console_gateway::ConsoleGateway;
use crate::admin::http_server_support::{json_response, parse_query_params};
use crate::protocol::Frame;

/// Trace endpoint max k (clamp). Higher values are rejected silently
/// (clamped) rather than 400 — callers just get the capped result.
const K_MIN: usize = 1;
const K_MAX: usize = 100;
const K_DEFAULT: usize = 10;

const EF_MAX: usize = 512;
const EF_DEFAULT: usize = 64;

/// JSON error response builder.
fn err(status: StatusCode, msg: &str) -> Response<BoxBody<Bytes, Infallible>> {
    json_response(status, &serde_json::json!({ "error": msg }))
}

/// Decode a base64 string into its raw bytes. Accepts both standard and
/// URL-safe alphabets (console clients sometimes URL-encode the blob).
fn decode_base64(s: &str) -> Result<Vec<u8>, String> {
    use base64::Engine;
    use base64::engine::general_purpose::{STANDARD, URL_SAFE};
    STANDARD
        .decode(s)
        .or_else(|_| URL_SAFE.decode(s))
        .map_err(|e| e.to_string())
}

/// Parse a little-endian f32 vector from raw bytes. Returns `Err` if the
/// length isn't a multiple of 4.
fn parse_query_vec(raw: &[u8]) -> Result<Vec<f32>, String> {
    if !raw.len().is_multiple_of(4) {
        return Err("query byte length must be a multiple of 4".into());
    }
    let mut out = Vec::with_capacity(raw.len() / 4);
    for chunk in raw.chunks_exact(4) {
        out.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    Ok(out)
}

/// Main HTTP handler. Validates request, verifies the index exists via
/// `FT.INFO`, and returns the stub trace response.
pub async fn handle_hnsw_trace(
    query: &str,
    gw: &ConsoleGateway,
) -> Response<BoxBody<Bytes, Infallible>> {
    let params = parse_query_params(query);

    let index_name = match params.get("index") {
        Some(s) if !s.is_empty() => s.clone(),
        _ => return err(StatusCode::BAD_REQUEST, "missing 'index' query param"),
    };

    let k = params
        .get("k")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(K_DEFAULT)
        .clamp(K_MIN, K_MAX);

    let ef_search = params
        .get("ef_search")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(EF_DEFAULT)
        .clamp(k, EF_MAX);

    let query_b64 = match params.get("query") {
        Some(s) if !s.is_empty() => s.clone(),
        _ => {
            return err(
                StatusCode::BAD_REQUEST,
                "missing 'query' query param (base64 f32 LE)",
            );
        }
    };
    let raw = match decode_base64(&query_b64) {
        Ok(v) => v,
        Err(e) => {
            return err(
                StatusCode::BAD_REQUEST,
                &format!("invalid base64 in 'query': {}", e),
            );
        }
    };
    let query_vec = match parse_query_vec(&raw) {
        Ok(v) => v,
        Err(e) => return err(StatusCode::BAD_REQUEST, &e),
    };
    if query_vec.is_empty() {
        return err(StatusCode::BAD_REQUEST, "query vector is empty");
    }

    // Verify the index exists via FT.INFO. Anything other than an Array
    // reply is treated as "not found" (Frame::Error / Null both land here).
    let info_frame = match gw
        .execute_command(0, "FT.INFO", &[Bytes::from(index_name.clone())])
        .await
    {
        Ok(f) => f,
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e),
    };
    if !matches!(info_frame, Frame::Array(_) | Frame::Map(_)) {
        return err(StatusCode::NOT_FOUND, "index not found");
    }

    // Minimal viable response: request validated, index exists, but the
    // live trace is not yet wired through the shard SPSC path. Downstream
    // consumers (VE-07 overlay) treat a `trace_not_implemented` note as a
    // signal to fall back to their synthetic animation.
    let body = serde_json::json!({
        "index": index_name,
        "dim": query_vec.len(),
        "k": k,
        "ef_search": ef_search,
        "layers": [],
        "top_k_ids": [],
        "note": "trace_not_implemented",
        "warning": "debug endpoint; per-layer trace wiring is pending",
    });
    json_response(StatusCode::OK, &body)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_query_vec_rejects_misaligned_bytes() {
        assert!(parse_query_vec(&[0u8, 0, 0]).is_err());
        assert!(parse_query_vec(&[0u8; 5]).is_err());
    }

    #[test]
    fn parse_query_vec_parses_le_f32() {
        // 1.0_f32 LE = 0x00 0x00 0x80 0x3F
        let bytes = [0x00, 0x00, 0x80, 0x3F, 0x00, 0x00, 0x00, 0x00];
        let v = parse_query_vec(&bytes).expect("well-formed");
        assert_eq!(v.len(), 2);
        assert!((v[0] - 1.0).abs() < f32::EPSILON);
        assert_eq!(v[1], 0.0);
    }

    #[test]
    fn decode_base64_accepts_standard_alphabet() {
        // base64("hi") = "aGk="
        let out = decode_base64("aGk=").expect("valid base64");
        assert_eq!(out, b"hi");
    }

    #[test]
    fn decode_base64_accepts_url_safe_alphabet() {
        // base64url("??>?") — contains '-' and '_' in url-safe.
        // Pick a small payload that uses url-safe chars: bytes [0xFB, 0xFF]
        // → url-safe "-_8=" vs standard "+/8=".
        let url_safe = "-_8=";
        let out = decode_base64(url_safe).expect("url-safe decodes");
        assert_eq!(out, vec![0xFB, 0xFF]);
    }

    #[test]
    fn decode_base64_rejects_garbage() {
        assert!(decode_base64("!!!not base64!!!").is_err());
    }
}
