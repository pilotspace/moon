//! `GET /api/v1/memory/treemap` — server-side keyspace memory aggregation.
//!
//! Implements INT-01 from Phase 136: aggregate keyspace into a `MemoryNode`
//! namespace tree so the console can stop synthesising treemaps from
//! client-side SCAN loops.
//!
//! ## Shape
//! The response body is:
//! ```json
//! {
//!   "tree": { "name": "root", "bytes": N, "count": M, "children": [...] },
//!   "scanned": M,
//!   "truncated": bool,
//!   "prefix": "<prefix>",
//!   "limit": N
//! }
//! ```
//!
//! ## Non-blocking guarantee
//! Aggregation runs inside an async handler on the single-threaded
//! admin-http tokio runtime. The handler yields via `tokio::task::yield_now`
//! every 200 keys so it cannot starve WebSocket / SSE tasks running on the
//! same runtime.
//!
//! ## Shard routing caveat
//! `SCAN`, `TYPE`, and `MEMORY USAGE` all dispatch through the
//! `ConsoleGateway::execute_command` path. In the current gateway, `SCAN`
//! and `MEMORY` are routed to shard 0 (keyless routing), which is correct
//! for single-shard deployments (the recommended config for memory
//! benchmarks — see `CLAUDE.md` "Gotchas"). For multi-shard deployments the
//! treemap shows only shard 0's keyspace; full cross-shard aggregation is
//! deferred.

use std::collections::HashMap;
use std::convert::Infallible;

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use hyper::{Response, StatusCode};

use crate::admin::console_gateway::ConsoleGateway;
use crate::admin::http_server_support::{json_response, parse_query_params};
use crate::protocol::Frame;

/// Default cap on the number of keys scanned per request.
const DEFAULT_LIMIT: u64 = 10_000;
/// Hard ceiling even if `?limit=` is larger; keeps one request bounded.
const MAX_LIMIT: u64 = 200_000;
/// Yield back to the runtime every N keys. Keeps the admin loop responsive
/// when the keyspace is large and the SCAN pipe is continuously full.
const YIELD_EVERY: u64 = 200;
/// Number of keys to request per SCAN iteration.
const SCAN_BATCH: u64 = 500;
/// Path separator for namespace segmentation.
const NAMESPACE_SEP: char = ':';

/// Publicly serialised memory tree node. Each node represents either a
/// namespace segment (interior) or a concrete key (leaf with `kind` set).
#[derive(Debug, serde::Serialize)]
pub struct MemoryNode {
    /// Namespace segment (e.g. `"user"`, `"1234"`) or `"root"` at the top.
    pub name: String,
    /// Total bytes for all keys under this node.
    pub bytes: u64,
    /// Total leaf keys under this node.
    pub count: u64,
    /// Redis type (`"string"`, `"hash"`, ...) for leaves only. `None` for
    /// interior nodes. Skipped when serialising `None` to keep the payload
    /// tight.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Children sorted by `bytes` descending.
    pub children: Vec<MemoryNode>,
}

/// Internal mutable builder used during aggregation. Converted into a
/// `MemoryNode` once the SCAN loop finishes.
struct Interior {
    name: String,
    bytes: u64,
    count: u64,
    kind: Option<String>,
    children: HashMap<String, Interior>,
}

impl Interior {
    fn new(name: String) -> Self {
        Self {
            name,
            bytes: 0,
            count: 0,
            kind: None,
            children: HashMap::new(),
        }
    }

    /// Insert a single key into the tree along its `segments` path.
    /// Accumulates `bytes` and `count` at every ancestor; stamps `kind`
    /// on the terminal leaf.
    fn insert(&mut self, segments: &[&str], bytes: u64, kind: &str) {
        self.bytes = self.bytes.saturating_add(bytes);
        self.count = self.count.saturating_add(1);
        if let Some((head, rest)) = segments.split_first() {
            let entry = self
                .children
                .entry((*head).to_string())
                .or_insert_with(|| Interior::new((*head).to_string()));
            if rest.is_empty() {
                entry.bytes = entry.bytes.saturating_add(bytes);
                entry.count = entry.count.saturating_add(1);
                entry.kind = Some(kind.to_string());
            } else {
                entry.insert(rest, bytes, kind);
            }
        }
    }

    /// Convert to the public shape, sorting children by `bytes` desc.
    fn into_node(self) -> MemoryNode {
        let mut children: Vec<MemoryNode> = self
            .children
            .into_values()
            .map(Interior::into_node)
            .collect();
        children.sort_by(|a, b| b.bytes.cmp(&a.bytes));
        MemoryNode {
            name: self.name,
            bytes: self.bytes,
            count: self.count,
            kind: self.kind,
            children,
        }
    }
}

/// Extract `(cursor, keys)` from a RESP `SCAN` reply.
///
/// Returns `None` for any shape mismatch so the caller can treat it as an
/// internal error — RESP defends against malformed client input, but the
/// gateway's SCAN response should always be well-formed; `None` means a
/// regression, not user error.
fn extract_scan(frame: &Frame) -> Option<(Bytes, Vec<String>)> {
    let arr = match frame {
        Frame::Array(a) => a,
        _ => return None,
    };
    if arr.len() != 2 {
        return None;
    }
    let cursor = match &arr[0] {
        Frame::BulkString(b) | Frame::SimpleString(b) => b.clone(),
        _ => return None,
    };
    let keys = match &arr[1] {
        Frame::Array(items) => items
            .iter()
            .filter_map(|f| match f {
                Frame::BulkString(b) | Frame::SimpleString(b) => {
                    Some(String::from_utf8_lossy(b).into_owned())
                }
                _ => None,
            })
            .collect(),
        _ => return None,
    };
    Some((cursor, keys))
}

fn json_err(status: StatusCode, msg: impl Into<String>) -> Response<BoxBody<Bytes, Infallible>> {
    json_response(status, &serde_json::json!({ "error": msg.into() }))
}

/// Main handler entry point. Builds a `MemoryNode` tree by iterating SCAN
/// batches, folding each key's type + memory usage into the namespace tree,
/// and yielding every `YIELD_EVERY` keys so the admin runtime stays
/// responsive.
pub async fn handle_memory_treemap(
    query: &str,
    gw: &ConsoleGateway,
) -> Response<BoxBody<Bytes, Infallible>> {
    let params = parse_query_params(query);
    let prefix = params.get("prefix").cloned().unwrap_or_default();
    // Malformed `limit` falls back silently to the default — the endpoint
    // must never 400 on a typo.
    let limit = params
        .get("limit")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_LIMIT)
        .min(MAX_LIMIT);

    let pattern = if prefix.is_empty() {
        "*".to_string()
    } else {
        format!("{}*", prefix)
    };

    let mut cursor = Bytes::from_static(b"0");
    let mut root = Interior::new("root".into());
    let mut scanned: u64 = 0;
    let mut truncated = false;

    'outer: loop {
        let scan_args = vec![
            cursor.clone(),
            Bytes::from_static(b"MATCH"),
            Bytes::from(pattern.clone()),
            Bytes::from_static(b"COUNT"),
            Bytes::from(SCAN_BATCH.to_string()),
        ];
        let frame = match gw.execute_command(0, "SCAN", &scan_args).await {
            Ok(f) => f,
            Err(e) => return json_err(StatusCode::INTERNAL_SERVER_ERROR, e),
        };

        let (next_cursor, keys) = match extract_scan(&frame) {
            Some(v) => v,
            None => {
                return json_err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "unexpected SCAN reply shape",
                );
            }
        };

        for key in keys {
            if scanned >= limit {
                truncated = true;
                break 'outer;
            }
            scanned += 1;

            let key_bytes = Bytes::from(key.clone());

            // TYPE → skip on error, skip non-existent keys (race w/ expiry).
            let kind = match gw
                .execute_command(0, "TYPE", std::slice::from_ref(&key_bytes))
                .await
            {
                Ok(Frame::SimpleString(b)) | Ok(Frame::BulkString(b)) => {
                    String::from_utf8_lossy(&b).into_owned()
                }
                // Any other shape or error: skip this key silently.
                _ => continue,
            };
            if kind == "none" {
                continue;
            }

            // MEMORY USAGE → default to 0 if the reply is not an Integer.
            // Missing key (race) or command error both bucket into 0 bytes.
            let bytes_used = match gw
                .execute_command(
                    0,
                    "MEMORY",
                    &[Bytes::from_static(b"USAGE"), key_bytes.clone()],
                )
                .await
            {
                Ok(Frame::Integer(n)) if n > 0 => n as u64,
                _ => 0,
            };

            let segments: Vec<&str> = key.split(NAMESPACE_SEP).collect();
            root.insert(&segments, bytes_used, &kind);

            // Cooperative yield so we do not starve other admin-http tasks
            // (WebSocket gateway, SSE stream). Cheap on tokio current-thread
            // runtime — just a reschedule.
            if scanned.is_multiple_of(YIELD_EVERY) {
                tokio::task::yield_now().await;
            }
        }

        if next_cursor.as_ref() == b"0" {
            break;
        }
        cursor = next_cursor;
    }

    let tree = root.into_node();
    let body = serde_json::json!({
        "tree": tree,
        "scanned": scanned,
        "truncated": truncated,
        "prefix": prefix,
        "limit": limit,
    });
    json_response(StatusCode::OK, &body)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::FrameVec;

    #[test]
    fn interior_insert_builds_nested_tree() {
        let mut root = Interior::new("root".into());
        root.insert(&["user", "1", "name"], 120, "string");
        root.insert(&["user", "2", "name"], 80, "string");
        root.insert(&["cache", "x"], 40, "string");

        let node = root.into_node();

        // Root aggregates everything.
        assert_eq!(node.name, "root");
        assert_eq!(node.count, 3);
        assert_eq!(node.bytes, 240);
        assert_eq!(node.children.len(), 2);

        // Children sorted by bytes desc: user (200) before cache (40).
        assert_eq!(node.children[0].name, "user");
        assert_eq!(node.children[0].bytes, 200);
        assert_eq!(node.children[0].count, 2);
        assert_eq!(node.children[0].children.len(), 2);

        assert_eq!(node.children[1].name, "cache");
        assert_eq!(node.children[1].bytes, 40);
        assert_eq!(node.children[1].count, 1);

        // Leaves carry `kind`.
        let user_children = &node.children[0].children;
        for child in user_children {
            // "1" and "2" are interior here because they contain "name".
            assert!(child.kind.is_none());
            assert_eq!(child.children.len(), 1);
            let leaf = &child.children[0];
            assert_eq!(leaf.name, "name");
            assert_eq!(leaf.kind.as_deref(), Some("string"));
        }
    }

    #[test]
    fn interior_insert_leaf_at_top_level() {
        // Key with no separator becomes a direct child leaf.
        let mut root = Interior::new("root".into());
        root.insert(&["foo"], 10, "hash");

        let node = root.into_node();
        assert_eq!(node.children.len(), 1);
        assert_eq!(node.children[0].name, "foo");
        assert_eq!(node.children[0].kind.as_deref(), Some("hash"));
        assert_eq!(node.children[0].count, 1);
        assert_eq!(node.children[0].bytes, 10);
    }

    #[test]
    fn extract_scan_parses_standard_shape() {
        let frame = Frame::Array(FrameVec::from_vec(vec![
            Frame::BulkString(Bytes::from_static(b"0")),
            Frame::Array(FrameVec::from_vec(vec![
                Frame::BulkString(Bytes::from_static(b"user:1")),
                Frame::BulkString(Bytes::from_static(b"user:2")),
            ])),
        ]));
        let (cur, keys) = extract_scan(&frame).expect("well-formed SCAN reply");
        assert_eq!(cur.as_ref(), b"0");
        assert_eq!(keys, vec!["user:1".to_string(), "user:2".to_string()]);
    }

    #[test]
    fn extract_scan_rejects_malformed() {
        // Not an array at all.
        assert!(extract_scan(&Frame::Null).is_none());

        // Array with one element (missing keys).
        let single = Frame::Array(FrameVec::from_vec(vec![Frame::BulkString(
            Bytes::from_static(b"0"),
        )]));
        assert!(extract_scan(&single).is_none());

        // Array with wrong cursor type.
        let bad_cursor = Frame::Array(FrameVec::from_vec(vec![
            Frame::Integer(0),
            Frame::Array(FrameVec::from_vec(vec![])),
        ]));
        assert!(extract_scan(&bad_cursor).is_none());
    }

    #[test]
    fn memory_node_serialises_without_kind_for_interior() {
        let node = MemoryNode {
            name: "root".into(),
            bytes: 100,
            count: 1,
            kind: None,
            children: vec![MemoryNode {
                name: "leaf".into(),
                bytes: 100,
                count: 1,
                kind: Some("string".into()),
                children: vec![],
            }],
        };
        let s = serde_json::to_string(&node).unwrap_or_default();
        // Interior has no "kind" key in the JSON.
        assert!(!s.contains("\"kind\":null"));
        assert!(s.contains("\"kind\":\"string\""));
    }
}
