//! Console gateway: translates REST/WS requests into RESP Frame commands,
//! dispatches to shards via SPSC channels, and converts responses to JSON.
//!
//! This module is feature-gated behind `console`.

use std::sync::Arc;
use std::sync::OnceLock;

use bytes::Bytes;
use parking_lot::Mutex;
use ringbuf::HeapProd;
use ringbuf::traits::Producer;

use crate::protocol::{Frame, FrameVec};
use crate::runtime::channel;
use crate::shard::dispatch::{ShardMessage, key_to_shard};

/// Global console gateway instance, set once during startup.
static CONSOLE_GATEWAY: OnceLock<Arc<ConsoleGateway>> = OnceLock::new();

/// Set the global console gateway (called once from main.rs).
pub fn set_global_gateway(gw: Arc<ConsoleGateway>) {
    let _ = CONSOLE_GATEWAY.set(gw);
}

/// Get a reference to the global console gateway, if initialized.
pub fn get_global_gateway() -> Option<&'static Arc<ConsoleGateway>> {
    CONSOLE_GATEWAY.get()
}

/// Console gateway: translates REST/WS requests into RESP Frame commands,
/// dispatches to shards via SPSC channels, and converts responses to JSON.
pub struct ConsoleGateway {
    /// One SPSC producer per shard. Mutex because admin thread is multi-task
    /// on a single-threaded tokio runtime (tasks are cooperative, not concurrent,
    /// but Mutex satisfies Send+Sync for Arc<ConsoleGateway>).
    shard_producers: Vec<Mutex<HeapProd<ShardMessage>>>,
    /// Notifiers to wake shard event loops after SPSC push.
    spsc_notifiers: Vec<Arc<channel::Notify>>,
    /// Number of shards for key routing.
    num_shards: usize,
}

/// Commands that do not operate on a specific key and should route to shard 0.
fn is_keyless_command(cmd: &str) -> bool {
    matches!(
        cmd,
        "PING"
            | "INFO"
            | "DBSIZE"
            | "COMMAND"
            | "CONFIG"
            | "CLIENT"
            | "FLUSHALL"
            | "FLUSHDB"
            | "SLOWLOG"
            | "SELECT"
            | "SUBSCRIBE"
            | "UNSUBSCRIBE"
            | "PSUBSCRIBE"
            | "PUNSUBSCRIBE"
            | "MONITOR"
            | "DEBUG"
            | "CLUSTER"
            | "MULTI"
            | "EXEC"
            | "DISCARD"
            | "WAIT"
            | "SAVE"
            | "BGSAVE"
            | "BGREWRITEAOF"
            | "LASTSAVE"
            | "TIME"
            | "MEMORY"
            | "LATENCY"
            | "RANDOMKEY"
            | "SCAN"
            | "KEYS"
            | "OBJECT"
    )
}

impl ConsoleGateway {
    /// Create a new console gateway with SPSC producers and shard notifiers.
    pub fn new(
        producers: Vec<HeapProd<ShardMessage>>,
        notifiers: Vec<Arc<channel::Notify>>,
    ) -> Self {
        let num_shards = producers.len();
        let shard_producers = producers.into_iter().map(Mutex::new).collect();
        Self {
            shard_producers,
            spsc_notifiers: notifiers,
            num_shards,
        }
    }

    /// Number of shards this gateway fans out to.
    pub fn num_shards(&self) -> usize {
        self.num_shards
    }

    /// Execute a command by name and arguments.
    ///
    /// Builds a RESP Frame, determines the target shard from the first key argument
    /// (shard 0 for keyless commands), creates a oneshot channel, pushes
    /// `ShardMessage::Execute` via SPSC, and awaits the reply.
    pub async fn execute_command(
        &self,
        db_index: usize,
        cmd: &str,
        args: &[Bytes],
    ) -> Result<Frame, String> {
        let cmd_upper = cmd.to_ascii_uppercase();
        let frame = Self::build_frame(&cmd_upper, args);

        let shard_id = if args.is_empty() || is_keyless_command(&cmd_upper) {
            0
        } else {
            key_to_shard(&args[0], self.num_shards)
        };

        self.execute_on_shard(db_index, shard_id, Arc::new(frame))
            .await
    }

    /// Push a pre-built Frame to a specific shard and await the response.
    pub async fn execute_on_shard(
        &self,
        db_index: usize,
        shard_id: usize,
        frame: Arc<Frame>,
    ) -> Result<Frame, String> {
        if shard_id >= self.num_shards {
            return Err(format!(
                "shard_id {} out of range (num_shards={})",
                shard_id, self.num_shards
            ));
        }

        let (reply_tx, reply_rx) = channel::oneshot();
        let msg = ShardMessage::Execute {
            db_index,
            command: frame,
            reply_tx,
        };

        // Push to the SPSC ring buffer. If full, the shard is overloaded.
        {
            let mut prod = self.shard_producers[shard_id].lock();
            prod.try_push(msg)
                .map_err(|_| "shard SPSC buffer full (overloaded)".to_string())?;
        }

        // Wake the target shard's event loop.
        self.spsc_notifiers[shard_id].notify_one();

        // Await the response from the shard.
        reply_rx
            .recv()
            .await
            .map_err(|_| "shard dropped reply channel".to_string())
    }

    /// Build a RESP `Frame::Array` from a command name and arguments.
    pub fn build_frame(cmd: &str, args: &[Bytes]) -> Frame {
        let mut elements = Vec::with_capacity(1 + args.len());
        elements.push(Frame::BulkString(Bytes::copy_from_slice(cmd.as_bytes())));
        for arg in args {
            elements.push(Frame::BulkString(arg.clone()));
        }
        Frame::Array(FrameVec::from_vec(elements))
    }

    /// Convert a RESP `Frame` into a `serde_json::Value`.
    ///
    /// Handles all Frame variants recursively:
    /// - Strings attempt UTF-8, fall back to base64
    /// - Errors produce `{"error": "..."}`
    /// - Maps produce JSON objects (keys stringified)
    /// - PreSerialized produces `{"raw": true}` (should not appear in console)
    pub fn frame_to_json(frame: &Frame) -> serde_json::Value {
        match frame {
            Frame::SimpleString(b) | Frame::BulkString(b) => match std::str::from_utf8(b) {
                Ok(s) => serde_json::Value::String(s.to_string()),
                Err(_) => {
                    use base64::Engine;
                    let encoded = base64::engine::general_purpose::STANDARD.encode(b.as_ref());
                    serde_json::json!({ "base64": encoded })
                }
            },
            Frame::Error(b) => {
                let msg = String::from_utf8_lossy(b);
                serde_json::json!({ "error": msg })
            }
            Frame::Integer(i) => serde_json::Value::Number((*i).into()),
            Frame::Array(v) => {
                let items: Vec<serde_json::Value> = v.iter().map(Self::frame_to_json).collect();
                serde_json::Value::Array(items)
            }
            Frame::Null => serde_json::Value::Null,
            Frame::Map(m) => {
                let obj: serde_json::Map<String, serde_json::Value> = m
                    .iter()
                    .map(|(k, v)| {
                        let key = match k {
                            Frame::SimpleString(b) | Frame::BulkString(b) => {
                                String::from_utf8_lossy(b).to_string()
                            }
                            other => format!("{:?}", other),
                        };
                        (key, Self::frame_to_json(v))
                    })
                    .collect();
                serde_json::Value::Object(obj)
            }
            Frame::Set(v) => {
                let items: Vec<serde_json::Value> = v.iter().map(Self::frame_to_json).collect();
                serde_json::Value::Array(items)
            }
            Frame::Double(f) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Frame::Boolean(b) => serde_json::Value::Bool(*b),
            Frame::VerbatimString { data, .. } => match std::str::from_utf8(data) {
                Ok(s) => serde_json::Value::String(s.to_string()),
                Err(_) => {
                    use base64::Engine;
                    let encoded = base64::engine::general_purpose::STANDARD.encode(data.as_ref());
                    serde_json::json!({ "base64": encoded })
                }
            },
            Frame::BigNumber(b) => {
                let s = String::from_utf8_lossy(b);
                serde_json::Value::String(s.to_string())
            }
            Frame::Push(v) => {
                let items: Vec<serde_json::Value> = v.iter().map(Self::frame_to_json).collect();
                serde_json::Value::Array(items)
            }
            Frame::PreSerialized(_) => serde_json::json!({ "raw": true }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_to_json_string() {
        let frame = Frame::BulkString(Bytes::from_static(b"hello"));
        let json = ConsoleGateway::frame_to_json(&frame);
        assert_eq!(json, serde_json::Value::String("hello".into()));
    }

    #[test]
    fn test_frame_to_json_simple_string() {
        let frame = Frame::SimpleString(Bytes::from_static(b"OK"));
        let json = ConsoleGateway::frame_to_json(&frame);
        assert_eq!(json, serde_json::Value::String("OK".into()));
    }

    #[test]
    fn test_frame_to_json_integer() {
        let frame = Frame::Integer(42);
        let json = ConsoleGateway::frame_to_json(&frame);
        assert_eq!(json, serde_json::json!(42));
    }

    #[test]
    fn test_frame_to_json_null() {
        let json = ConsoleGateway::frame_to_json(&Frame::Null);
        assert!(json.is_null());
    }

    #[test]
    fn test_frame_to_json_array() {
        let frame = Frame::Array(FrameVec::from_vec(vec![
            Frame::BulkString(Bytes::from_static(b"a")),
            Frame::Integer(1),
        ]));
        let json = ConsoleGateway::frame_to_json(&frame);
        assert_eq!(json, serde_json::json!(["a", 1]));
    }

    #[test]
    fn test_frame_to_json_error() {
        let frame = Frame::Error(Bytes::from_static(b"ERR unknown"));
        let json = ConsoleGateway::frame_to_json(&frame);
        assert_eq!(json["error"], "ERR unknown");
    }

    #[test]
    fn test_frame_to_json_boolean() {
        let frame = Frame::Boolean(true);
        let json = ConsoleGateway::frame_to_json(&frame);
        assert_eq!(json, serde_json::Value::Bool(true));
    }

    #[test]
    fn test_frame_to_json_double() {
        let frame = Frame::Double(3.14);
        let json = ConsoleGateway::frame_to_json(&frame);
        assert_eq!(json, serde_json::json!(3.14));
    }

    #[test]
    fn test_frame_to_json_map() {
        let frame = Frame::Map(vec![(
            Frame::BulkString(Bytes::from_static(b"key")),
            Frame::BulkString(Bytes::from_static(b"value")),
        )]);
        let json = ConsoleGateway::frame_to_json(&frame);
        assert_eq!(json["key"], "value");
    }

    #[test]
    fn test_frame_to_json_set() {
        let frame = Frame::Set(FrameVec::from_vec(vec![
            Frame::BulkString(Bytes::from_static(b"a")),
            Frame::BulkString(Bytes::from_static(b"b")),
        ]));
        let json = ConsoleGateway::frame_to_json(&frame);
        assert_eq!(json, serde_json::json!(["a", "b"]));
    }

    #[test]
    fn test_frame_to_json_verbatim_string() {
        let frame = Frame::VerbatimString {
            encoding: Bytes::from_static(b"txt"),
            data: Bytes::from_static(b"hello world"),
        };
        let json = ConsoleGateway::frame_to_json(&frame);
        assert_eq!(json, serde_json::Value::String("hello world".into()));
    }

    #[test]
    fn test_frame_to_json_big_number() {
        let frame = Frame::BigNumber(Bytes::from_static(b"123456789012345678901234567890"));
        let json = ConsoleGateway::frame_to_json(&frame);
        assert_eq!(json, "123456789012345678901234567890");
    }

    #[test]
    fn test_frame_to_json_push() {
        let frame = Frame::Push(FrameVec::from_vec(vec![
            Frame::BulkString(Bytes::from_static(b"message")),
            Frame::BulkString(Bytes::from_static(b"channel")),
            Frame::BulkString(Bytes::from_static(b"data")),
        ]));
        let json = ConsoleGateway::frame_to_json(&frame);
        assert_eq!(json, serde_json::json!(["message", "channel", "data"]));
    }

    #[test]
    fn test_frame_to_json_preserialized() {
        let frame = Frame::PreSerialized(Bytes::from_static(b"+OK\r\n"));
        let json = ConsoleGateway::frame_to_json(&frame);
        assert_eq!(json, serde_json::json!({"raw": true}));
    }

    #[test]
    fn test_frame_to_json_binary_data() {
        let frame = Frame::BulkString(Bytes::from_static(&[0xFF, 0xFE, 0x00, 0x01]));
        let json = ConsoleGateway::frame_to_json(&frame);
        // Should produce base64 since it's not valid UTF-8
        assert!(json.is_object());
        assert!(json["base64"].is_string());
    }

    #[test]
    fn test_build_frame() {
        let frame = ConsoleGateway::build_frame("GET", &[Bytes::from_static(b"mykey")]);
        match frame {
            Frame::Array(v) => {
                assert_eq!(v.len(), 2);
                assert_eq!(v[0], Frame::BulkString(Bytes::from_static(b"GET")));
                assert_eq!(v[1], Frame::BulkString(Bytes::from_static(b"mykey")));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_build_frame_no_args() {
        let frame = ConsoleGateway::build_frame("PING", &[]);
        match frame {
            Frame::Array(v) => {
                assert_eq!(v.len(), 1);
                assert_eq!(v[0], Frame::BulkString(Bytes::from_static(b"PING")));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_build_frame_multiple_args() {
        let frame = ConsoleGateway::build_frame(
            "SET",
            &[
                Bytes::from_static(b"key"),
                Bytes::from_static(b"value"),
                Bytes::from_static(b"EX"),
                Bytes::from_static(b"60"),
            ],
        );
        match frame {
            Frame::Array(v) => {
                assert_eq!(v.len(), 5);
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_is_keyless_command() {
        assert!(is_keyless_command("PING"));
        assert!(is_keyless_command("INFO"));
        assert!(is_keyless_command("DBSIZE"));
        assert!(is_keyless_command("SCAN"));
        assert!(!is_keyless_command("GET"));
        assert!(!is_keyless_command("SET"));
        assert!(!is_keyless_command("HSET"));
    }
}
