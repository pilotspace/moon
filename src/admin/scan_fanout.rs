//! Multi-shard SCAN fan-out for the REST API (UX-01 Phase 137).
//!
//! Composite cursor format: `"<shard_id>:<per_shard_cursor>"`.
//! - Start: `"0"` (same as Redis — shard 0, per-shard cursor 0)
//! - Mid:   `"<N>:<C>"` where `N < num_shards`, `C` is the opaque per-shard cursor
//! - End:   `"0"` (same as Redis — iteration complete)
//!
//! Semantics: we drain shard `N` fully (per-shard cursor returns `"0"`) before
//! advancing to shard `N+1`. This keeps COUNT semantics close to Redis's
//! (returns *at most* COUNT keys per call, often fewer near shard boundaries)
//! while guaranteeing every key is visited exactly once.

use std::sync::Arc;

use bytes::Bytes;

use crate::admin::console_gateway::ConsoleGateway;
use crate::protocol::Frame;

/// Composite cursor. `"0"` is both the start and end state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    pub shard: usize,
    pub per_shard: u64,
}

impl Cursor {
    /// The initial cursor (and the terminal cursor once iteration completes).
    pub fn start() -> Self {
        Self {
            shard: 0,
            per_shard: 0,
        }
    }

    /// Whether this cursor represents the terminal state (`"0"`).
    pub fn is_end(&self) -> bool {
        self.shard == 0 && self.per_shard == 0
    }

    /// Parse a textual cursor. Accepts `"0"` (start/end) or `"<shard>:<per_shard>"`.
    pub fn parse(s: &str, num_shards: usize) -> Result<Self, &'static str> {
        if s == "0" {
            return Ok(Self::start());
        }
        let (shard_str, pc_str) = s.split_once(':').ok_or("missing ':'")?;
        if shard_str.is_empty() || pc_str.is_empty() {
            return Err("empty cursor segment");
        }
        let shard: usize = shard_str.parse().map_err(|_| "shard not u64")?;
        let per_shard: u64 = pc_str.parse().map_err(|_| "per_shard not u64")?;
        if shard >= num_shards {
            return Err("shard out of range");
        }
        Ok(Self { shard, per_shard })
    }

    /// Encode the cursor to wire form. Terminal cursor serializes as `"0"`.
    pub fn encode(&self) -> String {
        if self.is_end() {
            return "0".to_string();
        }
        format!("{}:{}", self.shard, self.per_shard)
    }
}

/// Extract bytes from a string-like frame (BulkString / SimpleString).
fn as_bytes(frame: &Frame) -> Option<&Bytes> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b),
        _ => None,
    }
}

/// Run SCAN on the shard pointed to by `cursor`. Returns `(next_cursor, keys)`.
///
/// If the shard finishes (per-shard cursor returns `"0"`) and more shards
/// remain, `next_cursor` advances to the next shard at per-shard cursor `0`
/// so the next call's keys come from the next shard. If this was the last
/// shard, `next_cursor == Cursor::start()` (iteration end, wire form `"0"`).
pub async fn scan_all_shards(
    gw: &ConsoleGateway,
    db_index: usize,
    cursor: Cursor,
    pattern: &str,
    count: u64,
) -> Result<(Cursor, Vec<Bytes>), String> {
    if gw.num_shards() == 0 {
        return Err("gateway has zero shards".to_string());
    }
    if cursor.shard >= gw.num_shards() {
        return Err(format!(
            "cursor shard {} out of range (num_shards={})",
            cursor.shard,
            gw.num_shards()
        ));
    }

    let args = vec![
        Bytes::from(cursor.per_shard.to_string()),
        Bytes::from_static(b"MATCH"),
        Bytes::from(pattern.to_string()),
        Bytes::from_static(b"COUNT"),
        Bytes::from(count.to_string()),
    ];
    let frame = Arc::new(ConsoleGateway::build_frame("SCAN", &args));
    let reply = gw.execute_on_shard(db_index, cursor.shard, frame).await?;

    // SCAN returns [cursor_bulk, [keys...]].
    let (new_pc, keys): (u64, Vec<Bytes>) = match reply {
        Frame::Array(v) if v.len() == 2 => {
            let cursor_frame = &v[0];
            let keys_frame = &v[1];

            let c = match as_bytes(cursor_frame) {
                Some(b) => std::str::from_utf8(b)
                    .map_err(|_| "cursor not utf8".to_string())?
                    .parse::<u64>()
                    .map_err(|_| "cursor not u64".to_string())?,
                None => return Err("malformed SCAN reply: cursor not string".to_string()),
            };

            let ks: Vec<Bytes> = match keys_frame {
                Frame::Array(ks) => ks
                    .iter()
                    .filter_map(|k| as_bytes(k).cloned())
                    .collect(),
                _ => Vec::new(),
            };
            (c, ks)
        }
        Frame::Error(e) => {
            let msg = String::from_utf8_lossy(&e).to_string();
            return Err(format!("shard SCAN returned error: {}", msg));
        }
        other => return Err(format!("malformed SCAN reply: {:?}", other)),
    };

    // Advance cursor.
    let next = if new_pc == 0 {
        // This shard is drained. Advance to next shard or wrap to end.
        let next_shard = cursor.shard + 1;
        if next_shard >= gw.num_shards() {
            Cursor::start() // end state == start state == "0"
        } else {
            Cursor {
                shard: next_shard,
                per_shard: 0,
            }
        }
    } else {
        Cursor {
            shard: cursor.shard,
            per_shard: new_pc,
        }
    };
    Ok((next, keys))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_parse_start() {
        assert_eq!(Cursor::parse("0", 4).unwrap(), Cursor::start());
    }

    #[test]
    fn cursor_parse_mid() {
        assert_eq!(
            Cursor::parse("2:42", 4).unwrap(),
            Cursor {
                shard: 2,
                per_shard: 42,
            }
        );
    }

    #[test]
    fn cursor_parse_rejects_out_of_range() {
        assert!(Cursor::parse("9:0", 4).is_err());
        // Equal to num_shards is also out of range (valid shards are 0..num_shards).
        assert!(Cursor::parse("4:0", 4).is_err());
    }

    #[test]
    fn cursor_parse_rejects_malformed() {
        assert!(Cursor::parse("abc", 4).is_err());
        assert!(Cursor::parse("1:", 4).is_err());
        assert!(Cursor::parse(":5", 4).is_err());
        assert!(Cursor::parse("1:abc", 4).is_err());
    }

    #[test]
    fn cursor_encode_roundtrip() {
        let c = Cursor {
            shard: 3,
            per_shard: 100,
        };
        assert_eq!(Cursor::parse(&c.encode(), 4).unwrap(), c);
    }

    #[test]
    fn cursor_end_is_zero() {
        assert_eq!(Cursor::start().encode(), "0");
        assert!(Cursor::start().is_end());
    }

    #[test]
    fn cursor_non_end_encodes_colon_form() {
        let c = Cursor {
            shard: 1,
            per_shard: 0,
        };
        assert!(!c.is_end());
        assert_eq!(c.encode(), "1:0");
    }
}
