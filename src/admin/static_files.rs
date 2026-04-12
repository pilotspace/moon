//! Serve embedded frontend assets via rust-embed.
//!
//! In debug mode, reads from filesystem (hot reload during development).
//! In release mode, assets are compressed and embedded in the binary.

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};
use rust_embed::Embed;

#[derive(Embed)]
#[folder = "console/dist/"]
#[prefix = ""]
struct ConsoleAssets;

/// Serve an embedded static file.
///
/// - Empty path or "/" serves index.html
/// - Known files served with correct MIME type and cache headers
/// - Unknown paths serve index.html (SPA fallback for client-side routing)
pub fn serve_static_file(path: &str) -> Response<Full<Bytes>> {
    let path = path.trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    match ConsoleAssets::get(path) {
        Some(file) => {
            let mime = mime_guess::from_path(path).first_or_text_plain();
            let cache_control = if path == "index.html" {
                // index.html must not be aggressively cached (SPA entry point)
                "no-cache"
            } else {
                // Hashed assets (JS, CSS) can be cached immutably
                "public, max-age=31536000, immutable"
            };

            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", mime.as_ref())
                .header("cache-control", cache_control)
                .body(Full::new(Bytes::from(file.data.into_owned())))
                .unwrap_or_else(|_| {
                    Response::new(Full::new(Bytes::from_static(b"Internal Server Error")))
                })
        }
        None => {
            // Only SPA-fallback to index.html for route URLs (no extension).
            // Missing asset URLs (.js, .css, .wasm, .map) should 404 directly
            // so stale-hash cache busting works instead of silently returning HTML.
            let has_extension = path.contains('.') && !path.ends_with('/');
            if has_extension {
                return Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .header("content-type", "text/plain")
                    .body(Full::new(Bytes::from_static(b"Not Found")))
                    .unwrap_or_else(|_| {
                        Response::new(Full::new(Bytes::from_static(b"Not Found")))
                    });
            }
            // SPA fallback: serve index.html for client-side routing
            match ConsoleAssets::get("index.html") {
                Some(file) => Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/html; charset=utf-8")
                    .header("cache-control", "no-cache")
                    .body(Full::new(Bytes::from(file.data.into_owned())))
                    .unwrap_or_else(|_| {
                        Response::new(Full::new(Bytes::from_static(b"Internal Server Error")))
                    }),
                None => Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .header("content-type", "text/plain")
                    .body(Full::new(Bytes::from_static(b"Console not available")))
                    .unwrap_or_else(|_| Response::new(Full::new(Bytes::from_static(b"Not Found")))),
            }
        }
    }
}
