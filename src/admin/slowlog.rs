//! Slowlog — records commands that exceed a configurable latency threshold.
//!
//! Redis-compatible SLOWLOG GET/LEN/RESET/HELP commands.
//! Per-shard ring buffer; SLOWLOG GET merges across shards sorted by timestamp.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
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

/// The argv a dispatch path can offer the slowlog.
///
/// Every path that runs a command has its argv in ONE of two shapes: a parsed
/// multibulk (`Frame::Array` payload) or, on the inline `GET`/`SET` fast path,
/// raw slices of the read buffer. Both are borrowed — nothing is copied
/// unless the sample crosses the threshold.
#[derive(Debug, Clone, Copy)]
pub enum SlowlogArgv<'a> {
    /// A parsed multibulk. Non-bulk elements log as `?`.
    Frames(&'a [Frame]),
    /// Raw argv, command name first, as the inline fast path sees it.
    Raw(&'a [&'a [u8]]),
    /// Not a multibulk (a frame that is not an array): nothing to log.
    None,
}

impl<'a> From<&'a Frame> for SlowlogArgv<'a> {
    #[inline]
    fn from(frame: &'a Frame) -> Self {
        match frame {
            Frame::Array(args) => SlowlogArgv::Frames(args.as_slice()),
            _ => SlowlogArgv::None,
        }
    }
}

/// Global slowlog buffer.
///
/// ONE process-wide ring (every shard pushes into it), so a `CONFIG SET` of
/// either parameter takes effect on every shard with the store that makes it
/// (moon#995).
///
/// The hot path reads exactly one word, [`Self::effective_threshold_us`]: the
/// threshold when the slowlog can record anything, `-1` when it cannot
/// (negative threshold, or `max_len == 0`). Command timing is skipped
/// entirely on `-1` (moon#994). That word sits on its own cache line so the
/// ring's lock traffic never invalidates it on the shards that only read it.
pub struct Slowlog {
    effective_threshold_us: CachePadded<AtomicI64>,
    entries: Mutex<VecDeque<SlowlogEntry>>,
    /// As configured (`CONFIG GET` reads these back verbatim).
    max_len: AtomicU64,
    threshold_us: AtomicI64,
    /// Serialises the two setters' read-modify-publish of the effective
    /// threshold; never taken on the command path.
    config: Mutex<()>,
}

/// A value alone on its 64-byte line.
#[repr(align(64))]
struct CachePadded<T>(T);

/// Clamp a configured max-len into the `u64` the ring stores. `usize` is at
/// most 64 bits on every supported target, so this never saturates there.
#[inline]
fn max_len_u64(max_len: usize) -> u64 {
    u64::try_from(max_len).unwrap_or(u64::MAX)
}

impl Slowlog {
    /// Create a new slowlog with the given max length and threshold (µs;
    /// negative disables it, as redis's `slowlog-log-slower-than -1`).
    pub fn new(max_len: usize, threshold_us: i64) -> Self {
        let sl = Self {
            effective_threshold_us: CachePadded(AtomicI64::new(-1)),
            entries: Mutex::new(VecDeque::with_capacity(max_len.min(1024))),
            max_len: AtomicU64::new(max_len_u64(max_len)),
            threshold_us: AtomicI64::new(threshold_us),
            config: Mutex::new(()),
        };
        sl.publish_effective();
        sl
    }

    /// Recompute the one word the command path reads.
    fn publish_effective(&self) {
        let threshold = self.threshold_us.load(Ordering::Relaxed);
        let effective = if self.max_len.load(Ordering::Relaxed) == 0 {
            -1
        } else {
            threshold
        };
        self.effective_threshold_us
            .0
            .store(effective, Ordering::Relaxed);
    }

    /// Reconfigure max length and threshold together, clearing the ring.
    /// Startup only (`init_global_slowlog`): a live `CONFIG SET` goes through
    /// [`Self::set_threshold_us`] / [`Self::set_max_len`], which keep the
    /// entries as redis does.
    pub fn reconfigure(&self, max_len: usize, threshold_us: i64) {
        let _cfg = self.config.lock();
        self.max_len.store(max_len_u64(max_len), Ordering::Relaxed);
        self.threshold_us.store(threshold_us, Ordering::Relaxed);
        self.entries.lock().clear();
        self.publish_effective();
    }

    /// `CONFIG SET slowlog-log-slower-than`. Existing entries are kept
    /// (measured on redis-server 8.6.1: `SLOWLOG LEN` is unchanged).
    pub fn set_threshold_us(&self, threshold_us: i64) {
        let _cfg = self.config.lock();
        self.threshold_us.store(threshold_us, Ordering::Relaxed);
        self.publish_effective();
    }

    /// `CONFIG SET slowlog-max-len`. A shrink trims the ring at once, keeping
    /// the newest entries (redis-server 8.6.1: `SLOWLOG LEN` drops to the new
    /// length on the `CONFIG SET` itself, not on the next push).
    pub fn set_max_len(&self, max_len: usize) {
        let _cfg = self.config.lock();
        self.max_len.store(max_len_u64(max_len), Ordering::Relaxed);
        self.entries.lock().truncate(max_len);
        self.publish_effective();
    }

    /// The configured threshold, as `CONFIG GET` reports it.
    pub fn threshold_us(&self) -> i64 {
        self.threshold_us.load(Ordering::Relaxed)
    }

    /// The configured maximum length, as `CONFIG GET` reports it.
    pub fn max_len(&self) -> u64 {
        self.max_len.load(Ordering::Relaxed)
    }

    /// The threshold a command's duration is compared against, or a negative
    /// value when nothing can be recorded and timing should be skipped. One
    /// relaxed load of a line nobody writes on the command path.
    #[inline]
    pub fn effective_threshold_us(&self) -> i64 {
        self.effective_threshold_us.0.load(Ordering::Relaxed)
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
        self.maybe_record_argv(
            duration_us,
            SlowlogArgv::Frames(command),
            client_addr,
            client_name,
        );
    }

    /// Record a command if it exceeds the slowlog threshold, from whichever
    /// argv shape the dispatch path has (moon#963: the inline `GET`/`SET`
    /// path never builds a `Frame`, so it hands over raw byte slices).
    ///
    /// Cheap on the common path: one relaxed load and a compare. Argv is
    /// copied only once the sample has crossed the threshold.
    #[inline]
    pub fn maybe_record_argv(
        &self,
        duration_us: u64,
        argv: SlowlogArgv<'_>,
        client_addr: &[u8],
        client_name: &[u8],
    ) {
        self.record_if_over(
            duration_us,
            self.effective_threshold_us(),
            argv,
            client_addr,
            client_name,
        );
    }

    /// [`Self::maybe_record_argv`] against a threshold the caller already
    /// loaded (the latency probe loads it once to decide whether to time the
    /// command at all). A negative threshold records nothing; `0` records
    /// every command (redis: `duration >= slowlog-log-slower-than`).
    #[inline]
    pub fn record_if_over(
        &self,
        duration_us: u64,
        threshold_us: i64,
        argv: SlowlogArgv<'_>,
        client_addr: &[u8],
        client_name: &[u8],
    ) {
        let Ok(threshold) = u64::try_from(threshold_us) else {
            return;
        };
        if duration_us < threshold {
            return;
        }
        // Not a multibulk: there is no argv to show, and an entry whose
        // command renders as an empty array is noise, not a record.
        if matches!(argv, SlowlogArgv::None) {
            return;
        }
        self.push(duration_us, argv, client_addr, client_name);
    }

    /// The cold half of [`Self::maybe_record_argv`]: allocate the entry and
    /// push it. Out of line so the hot path inlines only the threshold test.
    #[cold]
    #[inline(never)]
    fn push(
        &self,
        duration_us: u64,
        argv: SlowlogArgv<'_>,
        client_addr: &[u8],
        client_name: &[u8],
    ) {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // Truncate to 128 args, each to 128 bytes (Redis convention).
        let cmd_args: Vec<Bytes> = match argv {
            SlowlogArgv::Frames(frames) => frames
                .iter()
                .take(128)
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
                .collect(),
            SlowlogArgv::Raw(args) => args
                .iter()
                .take(128)
                .map(|a| Bytes::copy_from_slice(&a[..a.len().min(128)]))
                .collect(),
            SlowlogArgv::None => Vec::new(),
        };

        let entry = SlowlogEntry {
            id,
            timestamp,
            duration_us,
            command: cmd_args,
            client_addr: Bytes::copy_from_slice(client_addr),
            client_name: Bytes::copy_from_slice(client_name),
        };

        let mut entries = self.entries.lock();
        // Loaded under the ring lock: `set_max_len` stores the new length
        // BEFORE it takes this lock to trim, so a push can never re-grow a
        // ring past a shrink that has already happened.
        let max_len = usize::try_from(self.max_len.load(Ordering::Relaxed)).unwrap_or(usize::MAX);
        if max_len == 0 {
            return; // max_len=0 means slowlog disabled (Redis convention)
        }
        while entries.len() >= max_len {
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

    // Case-insensitive byte compare — no uppercase copy on the read path.
    let subcmd: &[u8] = match &args[0] {
        Frame::BulkString(b) => b,
        _ => {
            return Frame::Error(Bytes::from_static(b"ERR invalid slowlog subcommand"));
        }
    };

    // moon#670: an unknown subcommand is refused with Redis's shape BEFORE any
    // arity check, and from the SAME table the `MULTI` queue gate consults. An
    // arity error here reads to a client as "the subcommand exists, you called
    // it wrong", which is how `SLOWLOG BOGUS` used to answer.
    if !crate::command::metadata::is_known_subcommand(b"SLOWLOG", subcmd) {
        {
            return crate::command::helpers::err_unknown_subcommand("SLOWLOG", subcmd);
        }
    }

    match subcmd {
        s if s.eq_ignore_ascii_case(b"GET") => {
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
        s if s.eq_ignore_ascii_case(b"LEN") => Frame::Integer(slowlog.len() as i64),
        s if s.eq_ignore_ascii_case(b"RESET") => {
            slowlog.reset();
            Frame::SimpleString(Bytes::from_static(b"OK"))
        }
        s if s.eq_ignore_ascii_case(b"HELP") => {
            // moon#698: shape and body both come from the shared table, which is
            // what stopped SLOWLOG emitting bulk strings with no header line
            // while Redis emits simple strings with one.
            crate::command::help_text::help_or_empty("SLOWLOG")
        }
        // Unreachable: the guard above already refused every name absent from
        // `SUBCOMMAND_META`. Kept as the match's exhaustive arm, answering the
        // same shape rather than a second spelling of it.
        _ => crate::command::helpers::err_unknown_subcommand("SLOWLOG", subcmd),
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

    #[test]
    fn negative_threshold_records_nothing_and_reports_disabled() {
        let sl = Slowlog::new(10, -1);
        assert!(sl.effective_threshold_us() < 0);
        sl.maybe_record(1_000_000, &[], b"", b"");
        assert_eq!(sl.len(), 0);
        assert_eq!(sl.threshold_us(), -1);
    }

    #[test]
    fn max_len_zero_reports_disabled_to_the_command_path() {
        let sl = Slowlog::new(0, 0);
        assert!(
            sl.effective_threshold_us() < 0,
            "nothing can be recorded, so commands need not be timed"
        );
        sl.set_max_len(4);
        assert_eq!(sl.effective_threshold_us(), 0);
    }

    #[test]
    fn set_max_len_shrink_trims_at_once_keeping_the_newest() {
        let sl = Slowlog::new(128, 0);
        for i in 0..6 {
            let cmd = vec![Frame::BulkString(Bytes::from(format!("cmd{i}")))];
            sl.maybe_record(10, &cmd, b"", b"");
        }
        sl.set_max_len(3);
        assert_eq!(sl.len(), 3);
        let kept: Vec<_> = sl.get(None).iter().map(|e| e.command[0].clone()).collect();
        assert_eq!(
            kept,
            vec![
                Bytes::from_static(b"cmd5"),
                Bytes::from_static(b"cmd4"),
                Bytes::from_static(b"cmd3")
            ]
        );
        assert_eq!(sl.max_len(), 3);
    }

    #[test]
    fn set_threshold_keeps_existing_entries() {
        let sl = Slowlog::new(128, 0);
        sl.maybe_record(10, &[], b"", b"");
        sl.set_threshold_us(5_000);
        assert_eq!(sl.len(), 1, "redis keeps the log across a threshold change");
        assert_eq!(sl.threshold_us(), 5_000);
        sl.maybe_record(10, &[], b"", b"");
        assert_eq!(sl.len(), 1, "10 us is under the new threshold");
    }

    #[test]
    fn duration_equal_to_threshold_is_recorded() {
        let sl = Slowlog::new(8, 100);
        sl.maybe_record(100, &[], b"", b"");
        assert_eq!(sl.len(), 1, "redis logs duration >= threshold");
    }
}
