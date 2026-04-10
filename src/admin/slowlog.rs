//! Slowlog — records commands that exceed a configurable latency threshold.
//!
//! Redis-compatible SLOWLOG GET/LEN/RESET/HELP commands.
//! Per-shard ring buffer; SLOWLOG GET merges across shards sorted by timestamp.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use parking_lot::Mutex;

use crate::protocol::Frame;

/// Global slowlog ID counter (monotonic across all shards).
static NEXT_ID: AtomicU64 = AtomicU64::new(0);

/// A single slowlog entry.
#[derive(Debug, Clone)]
pub struct SlowlogEntry {
    /// Unique monotonic ID.
    pub id: u64,
    /// Unix timestamp (seconds) when the command started.
    pub timestamp: u64,
    /// Execution duration in microseconds.
    pub duration_us: u64,
    /// The command and arguments (truncated to first 128 bytes per arg).
    pub command: Vec<Bytes>,
    /// Client address (if available).
    pub client_addr: Bytes,
    /// Client name (if set via CLIENT SETNAME).
    pub client_name: Bytes,
}

/// Global slowlog buffer.
///
/// `max_len` and `threshold_us` are stored as atomics so the global
/// instance (created via `once_cell::sync::Lazy`) can be reconfigured
/// from `main()` before shard threads start.
pub struct Slowlog {
    entries: Mutex<VecDeque<SlowlogEntry>>,
    max_len: AtomicU64,
    threshold_us: AtomicU64,
}

impl Slowlog {
    /// Create a new slowlog with the given max length and threshold.
    pub fn new(max_len: usize, threshold_us: u64) -> Self {
        Self {
            entries: Mutex::new(VecDeque::with_capacity(max_len.min(1024))),
            max_len: AtomicU64::new(max_len as u64),
            threshold_us: AtomicU64::new(threshold_us),
        }
    }

    /// Reconfigure max length and threshold.
    ///
    /// Clears existing entries since the threshold may have changed.
    pub fn reconfigure(&self, max_len: usize, threshold_us: u64) {
        self.max_len.store(max_len as u64, Ordering::Release);
        self.threshold_us.store(threshold_us, Ordering::Release);
        self.entries.lock().clear();
    }

    /// Record a command if it exceeds the slowlog threshold.
    #[inline]
    pub fn maybe_record(
        &self,
        duration_us: u64,
        command: &[Frame],
        client_addr: &[u8],
        client_name: &[u8],
    ) {
        let threshold = self.threshold_us.load(Ordering::Relaxed);
        if duration_us < threshold {
            return;
        }

        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // Truncate each arg to 128 bytes
        let cmd_args: Vec<Bytes> = command
            .iter()
            .take(128) // max 128 args logged
            .map(|f| match f {
                Frame::BulkString(b) => {
                    if b.len() > 128 {
                        Bytes::copy_from_slice(&b[..128])
                    } else {
                        b.clone()
                    }
                }
                _ => Bytes::from_static(b"?"),
            })
            .collect();

        let entry = SlowlogEntry {
            id,
            timestamp,
            duration_us,
            command: cmd_args,
            client_addr: Bytes::copy_from_slice(client_addr),
            client_name: Bytes::copy_from_slice(client_name),
        };

        let max_len = self.max_len.load(Ordering::Relaxed) as usize;
        if max_len == 0 {
            return; // max_len=0 means slowlog disabled (Redis convention)
        }
        let mut entries = self.entries.lock();
        if entries.len() >= max_len {
            entries.pop_back();
        }
        entries.push_front(entry);
    }

    /// Get the last N entries (or all if count is None).
    pub fn get(&self, count: Option<usize>) -> Vec<SlowlogEntry> {
        let entries = self.entries.lock();
        let n = count.unwrap_or(10).min(entries.len());
        entries.iter().take(n).cloned().collect()
    }

    /// Get the number of entries.
    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    /// Reset (clear) all entries.
    pub fn reset(&self) {
        self.entries.lock().clear();
    }
}

/// Serialize a slowlog entry to RESP array format (Redis-compatible).
pub fn entry_to_frame(entry: &SlowlogEntry) -> Frame {
    let mut args = Vec::with_capacity(entry.command.len());
    for arg in &entry.command {
        args.push(Frame::BulkString(arg.clone()));
    }

    Frame::Array(crate::protocol::FrameVec::from(vec![
        Frame::Integer(entry.id as i64),
        Frame::Integer(entry.timestamp as i64),
        Frame::Integer(entry.duration_us as i64),
        Frame::Array(crate::protocol::FrameVec::from(args)),
        Frame::BulkString(entry.client_addr.clone()),
        Frame::BulkString(entry.client_name.clone()),
    ]))
}

/// Handle the SLOWLOG command (GET/LEN/RESET/HELP).
pub fn handle_slowlog(slowlog: &Slowlog, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'slowlog' command",
        ));
    }

    let subcmd = match &args[0] {
        Frame::BulkString(b) => b.to_ascii_uppercase(),
        _ => {
            return Frame::Error(Bytes::from_static(b"ERR invalid slowlog subcommand"));
        }
    };

    match subcmd.as_slice() {
        b"GET" => {
            let count = if args.len() > 1 {
                match &args[1] {
                    Frame::BulkString(b) => {
                        // Parse as i64 first to detect negatives
                        match atoi::atoi::<i64>(b) {
                            Some(n) if n < 0 => {
                                return Frame::Error(Bytes::from_static(
                                    b"ERR count must be a non-negative integer",
                                ));
                            }
                            Some(n) => Some(n as usize),
                            None => {
                                return Frame::Error(Bytes::from_static(
                                    b"ERR value is not an integer or out of range",
                                ));
                            }
                        }
                    }
                    Frame::Integer(n) => {
                        if *n < 0 {
                            return Frame::Error(Bytes::from_static(
                                b"ERR count must be a non-negative integer",
                            ));
                        }
                        Some(*n as usize)
                    }
                    _ => {
                        return Frame::Error(Bytes::from_static(
                            b"ERR value is not an integer or out of range",
                        ));
                    }
                }
            } else {
                None
            };

            let entries = slowlog.get(count);
            let frames: Vec<Frame> = entries.iter().map(entry_to_frame).collect();
            Frame::Array(crate::protocol::FrameVec::from(frames))
        }
        b"LEN" => Frame::Integer(slowlog.len() as i64),
        b"RESET" => {
            slowlog.reset();
            Frame::SimpleString(Bytes::from_static(b"OK"))
        }
        b"HELP" => {
            let help = vec![
                Frame::BulkString(Bytes::from_static(b"SLOWLOG GET [<count>]")),
                Frame::BulkString(Bytes::from_static(
                    b"    Return top <count> entries from the slowlog (default 10).",
                )),
                Frame::BulkString(Bytes::from_static(b"SLOWLOG LEN")),
                Frame::BulkString(Bytes::from_static(
                    b"    Return the number of entries in the slowlog.",
                )),
                Frame::BulkString(Bytes::from_static(b"SLOWLOG RESET")),
                Frame::BulkString(Bytes::from_static(b"    Reset the slowlog.")),
            ];
            Frame::Array(crate::protocol::FrameVec::from(help))
        }
        _ => Frame::Error(Bytes::from_static(
            b"ERR unknown slowlog subcommand. Try SLOWLOG HELP.",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slowlog_basic() {
        let sl = Slowlog::new(10, 100); // 100us threshold

        // Below threshold — not recorded
        sl.maybe_record(50, &[], b"127.0.0.1:1234", b"");
        assert_eq!(sl.len(), 0);

        // Above threshold — recorded
        let cmd = vec![
            Frame::BulkString(Bytes::from_static(b"SET")),
            Frame::BulkString(Bytes::from_static(b"key")),
            Frame::BulkString(Bytes::from_static(b"value")),
        ];
        sl.maybe_record(200, &cmd, b"127.0.0.1:1234", b"my-client");
        assert_eq!(sl.len(), 1);

        let entries = sl.get(None);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].duration_us, 200);
        assert_eq!(entries[0].command.len(), 3);
    }

    #[test]
    fn test_slowlog_max_len() {
        let sl = Slowlog::new(3, 1);
        for i in 0..5 {
            let cmd = vec![Frame::BulkString(Bytes::from(format!("cmd{}", i)))];
            sl.maybe_record(10, &cmd, b"", b"");
        }
        assert_eq!(sl.len(), 3);
        // Most recent first
        let entries = sl.get(None);
        assert!(entries[0].id > entries[1].id);
    }

    #[test]
    fn test_slowlog_reset() {
        let sl = Slowlog::new(10, 1);
        sl.maybe_record(10, &[], b"", b"");
        assert_eq!(sl.len(), 1);
        sl.reset();
        assert_eq!(sl.len(), 0);
    }

    #[test]
    fn test_handle_slowlog_help() {
        let sl = Slowlog::new(10, 1);
        let args = vec![Frame::BulkString(Bytes::from_static(b"HELP"))];
        let result = handle_slowlog(&sl, &args);
        match result {
            Frame::Array(_) => {} // expected
            _ => panic!("Expected array response from SLOWLOG HELP"),
        }
    }

    #[test]
    fn test_threshold_zero_logs_everything() {
        // threshold=0 means "log every command" (Redis convention)
        let sl = Slowlog::new(10, 0);
        sl.maybe_record(0, &[], b"127.0.0.1:1234", b"");
        assert_eq!(sl.len(), 1);
    }

    #[test]
    fn test_max_len_zero_disables() {
        // max_len=0 means "disabled" (Redis convention)
        let sl = Slowlog::new(0, 0);
        sl.maybe_record(100, &[], b"", b"");
        assert_eq!(sl.len(), 0);
    }

    #[test]
    fn test_get_negative_count_error() {
        let sl = Slowlog::new(10, 1);
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"GET")),
            Frame::BulkString(Bytes::from_static(b"-5")),
        ];
        let result = handle_slowlog(&sl, &args);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_get_non_numeric_error() {
        let sl = Slowlog::new(10, 1);
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"GET")),
            Frame::BulkString(Bytes::from_static(b"abc")),
        ];
        let result = handle_slowlog(&sl, &args);
        assert!(matches!(result, Frame::Error(_)));
    }
}
