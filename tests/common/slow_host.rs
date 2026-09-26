//! Waiting correctly on a slow or stalled host (moon#1065, moon#1273).
//!
//! Main's post-merge run for `4a96cd5f` went red on Windows: a roughly
//! 3-minute runner-wide stall made every server-spawning test 2-17x slower,
//! and the tests that failed were the ones treating a fixed sleep, a fixed
//! round count or a sub-second window as a fact about the server. The rules
//! this module encodes:
//!
//! 1. **Wait for the observable condition**, bounded by a deadline that is
//!    only ever reached on failure — never a fixed sleep. A green run returns
//!    as soon as the condition holds, so it is no slower than the sleep was.
//! 2. **A write moon refused under AOF backpressure is retried**, with
//!    backoff, and counted. Under `appendfsync everysec` a write waits at most
//!    `--aof-fsync-timeout-ms` for room in the writer's channel and is then
//!    refused with the `-ERR AOF fsync failed; write not durable` text,
//!    although no fsync ran (moon#1272). A filler that discards its replies
//!    turns that into a missing key or a missing spill much later; one that
//!    asserts on it fails on a slow disk. The count is reported, never
//!    swallowed.
//! 3. **A timeout carries evidence**: the spill, eviction and persistence
//!    counters from `INFO` and the tail of the server's `server.err`, so a red
//!    run can tell "eviction never fired" from "the spill write failed" from
//!    "the writer was refusing" (moon#1065).
//!
//! Additive: nothing here changes an existing helper of this module's parent.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use super::{Conn, encode, framed_len};

/// The longest moon holds one write before refusing it for AOF backpressure:
/// `--aof-fsync-timeout-ms`, default 2000 (`src/config.rs`). Measured on a
/// debug monoio server with its `aof-writer` thread frozen and the channel
/// full: one 50-`SET` pipeline took 18.7 s and answered 9 refusals, 2.0 s
/// apiece.
pub const AOF_WRITE_BOUND: Duration = Duration::from_millis(2000);

/// Headroom on top of the server's own bounds for a runner that is not
/// running us at all. The Windows stall of run 36206030818 lasted 179 s
/// (00:53:10-00:56:09) and slowed operations 2-17x rather than freezing them;
/// no single wait in these suites normally takes more than about 3 s, so 17x
/// of it fits with room to spare.
pub const STALL_MARGIN: Duration = Duration::from_secs(60);

/// How long a "the server gets there" condition (a spill lands, a file is
/// reclaimed, a record reaches the AOF) may take before it is a failure.
/// Normally each takes 0.1-7 s; 17x of 7 s is 119 s.
pub const CONDITION_DEADLINE: Duration = Duration::from_secs(120);

/// Read budget for a pipeline of `writes` commands: each may legitimately
/// take [`AOF_WRITE_BOUND`] under backpressure, plus [`STALL_MARGIN`]. The
/// 20 s default of [`Conn::read_replies`] is at or below the server's own
/// bound for any pipeline of ten writes or more.
pub fn pipeline_budget(writes: usize) -> Duration {
    let writes = u32::try_from(writes).unwrap_or(u32::MAX);
    AOF_WRITE_BOUND
        .saturating_mul(writes)
        .saturating_add(STALL_MARGIN)
}

/// The text of moon's AOF backpressure refusal (moon#1272). The same text
/// also reports a real fsync failure; the two cannot be told apart from the
/// reply, which is why a persisting refusal fails the caller at its deadline
/// instead of being retried forever.
pub const AOF_REFUSAL: &str = "AOF fsync failed; write not durable";

/// Split a buffer holding complete top-level RESP replies into those replies.
pub fn split_replies(buf: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut rest = buf;
    while !rest.is_empty() {
        let Some(n) = framed_len(rest.as_bytes(), 1) else {
            out.push(rest);
            break;
        };
        out.push(&rest[..n]);
        rest = &rest[n..];
    }
    out
}

/// What a filler's writes came to. Printed by every caller and carried in
/// every failure message.
#[derive(Debug, Default, Clone)]
pub struct WriteStats {
    /// Commands put on the wire, re-sends included.
    pub sent: u64,
    /// Commands the server applied.
    pub accepted: u64,
    /// `-OOM` answers: legitimately not applied under memory pressure, and
    /// not retried (what the suites did before).
    pub oom: u64,
    /// AOF backpressure refusals ([`AOF_REFUSAL`]); every one was re-sent.
    pub aof_refused: u64,
    /// Pipelines re-sent after a backoff.
    pub backoffs: u64,
    /// The last command the server applied: what the AOF must hold before a
    /// `SIGKILL` for the kill to lose nothing the test counts on.
    pub last_accepted: Option<Vec<String>>,
}

impl std::fmt::Display for WriteStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "sent {} / accepted {} / -OOM {} / AOF-backpressure refusals {} (re-sent after \
             {} backoffs)",
            self.sent, self.accepted, self.oom, self.aof_refused, self.backoffs
        )
    }
}

/// Send `cmds` in pipelines of `depth`, re-sending every command moon refused
/// for AOF backpressure after a backoff (50 ms, doubling to 1 s), until each
/// has another answer. `-OOM` is counted and not re-sent. Any other error
/// panics: it is not a pressure answer.
///
/// Only for commands that are safe to apply twice (`SET` of a fixed value,
/// `DEL`): a refused write may still have been applied (the cancel-safety
/// note on `send_append_backpressure`).
///
/// Panics with [`diagnostics`] when refusals persist past `deadline`.
pub fn write_all(
    c: &mut Conn,
    cmds: &[Vec<String>],
    depth: usize,
    stats: &mut WriteStats,
    deadline: Instant,
    dir: &Path,
) {
    let depth = depth.max(1);
    for chunk in cmds.chunks(depth) {
        let mut pending: Vec<&Vec<String>> = chunk.iter().collect();
        let mut backoff = Duration::from_millis(50);
        loop {
            let parts: Vec<Vec<&str>> = pending
                .iter()
                .map(|c| c.iter().map(String::as_str).collect())
                .collect();
            let mut out = Vec::new();
            for p in &parts {
                out.extend_from_slice(&encode(p));
            }
            c.sock.write_all(&out).expect("write");
            stats.sent += pending.len() as u64;
            let raw = c.read_replies_within(pending.len(), pipeline_budget(pending.len()));
            let replies = split_replies(&raw);
            assert_eq!(replies.len(), pending.len(), "framing: {raw:.300}");
            let mut refused = Vec::new();
            for (cmd, reply) in pending.iter().zip(&replies) {
                if reply.starts_with('-') && reply.contains(AOF_REFUSAL) {
                    stats.aof_refused += 1;
                    refused.push(*cmd);
                } else if reply.starts_with("-OOM") {
                    stats.oom += 1;
                } else if reply.starts_with('-') {
                    panic!(
                        "{} answered an unexpected error: {reply:?} ({stats}){}",
                        cmd.first().map_or("", String::as_str),
                        diagnostics(peer_port(c), dir)
                    );
                } else {
                    stats.accepted += 1;
                    stats.last_accepted = Some((*cmd).clone());
                }
            }
            if refused.is_empty() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "{} writes still refused for AOF backpressure at the deadline ({stats}) — \
                 the AOF writer never drained{}",
                refused.len(),
                diagnostics(peer_port(c), dir)
            );
            stats.backoffs += 1;
            std::thread::sleep(backoff);
            backoff = (backoff * 2).min(Duration::from_secs(1));
            pending = refused;
        }
    }
}

/// The port a [`Conn`] is talking to (0 if the socket cannot say).
pub fn peer_port(c: &Conn) -> u16 {
    c.sock.peer_addr().map_or(0, |a| a.port())
}

/// Poll `cond` every 50 ms until it holds; returns how long that took.
/// Panics after `within` with `what`, the elapsed time and [`diagnostics`].
pub fn wait_until(
    what: &str,
    within: Duration,
    port: u16,
    dir: &Path,
    mut cond: impl FnMut() -> bool,
) -> Duration {
    let start = Instant::now();
    loop {
        if cond() {
            return start.elapsed();
        }
        if start.elapsed() >= within {
            panic!(
                "timed out after {:?} waiting for {what}{}",
                start.elapsed(),
                diagnostics(port, dir)
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Every AOF file under `dir`, in both layouts: the legacy single
/// `appendonly.aof` (tokio `--shards 1`) and the manifest layout's base and
/// incr files under `appendonlydir/` (per shard included).
pub fn aof_files(dir: &Path) -> Vec<PathBuf> {
    fn walk(p: &Path, out: &mut Vec<PathBuf>) {
        let Ok(rd) = std::fs::read_dir(p) else {
            return;
        };
        for e in rd.flatten() {
            let path = e.path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|x| x == "aof") {
                out.push(path);
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, &mut out);
    out
}

/// Does an AOF file under `dir` hold `parts` as a RESP record? Once the
/// writer has written a record it is in the kernel, and a `SIGKILL` of the
/// server cannot lose it; until then it can (moon#1266).
pub fn aof_holds(dir: &Path, parts: &[&str]) -> bool {
    let needle = encode(parts);
    aof_files(dir).iter().any(|p| {
        std::fs::read(p).is_ok_and(|b| b.windows(needle.len()).any(|w| w == needle.as_slice()))
    })
}

/// Wait until the AOF holds `parts` — what "sleep so the everysec fsync
/// covers the last write before the kill" meant. The writer appends in order
/// per shard, so every earlier write on the same connection and shard is in
/// by then too.
pub fn wait_aof_holds(parts: &[String], port: u16, dir: &Path) {
    let parts: Vec<&str> = parts.iter().map(String::as_str).collect();
    wait_until(
        &format!(
            "the AOF to hold {:?}… before the SIGKILL",
            parts.iter().take(2).collect::<Vec<_>>()
        ),
        CONDITION_DEADLINE,
        port,
        dir,
        || aof_holds(dir, &parts),
    );
}

/// [`wait_aof_holds`] for the last write `stats` saw applied (a no-op when
/// none was).
pub fn wait_aof_holds_last(stats: &WriteStats, port: u16, dir: &Path) {
    if let Some(last) = stats.last_accepted.as_ref() {
        wait_aof_holds(last, port, dir);
    }
}

/// Wait until every spill the server has queued has been written: the
/// spill thread's loop (`spill_last_heartbeat_ms`, stamped every iteration,
/// at most 100 ms apart when idle) has run for [`SPILL_IDLE_FOR`] without
/// `spill_batches_flushed` moving. A thread that ran that long with nothing
/// flushed had an empty queue and an empty buffer; a stalled one does not
/// advance its heartbeat, so a stall cannot pass for idleness. Replaces "sleep
/// a second and let the spills land" (moon#1065).
pub fn wait_spill_idle(what: &str, port: u16, dir: &Path) {
    let read = || {
        (
            info_field(port, "spill_last_heartbeat_ms"),
            info_field(port, "spill_batches_flushed"),
        )
    };
    let (mut since_hb, mut flushed) = read();
    wait_until(
        &format!("{what}: the spill thread to run {SPILL_IDLE_FOR:?} with nothing to write"),
        CONDITION_DEADLINE,
        port,
        dir,
        || {
            let (hb, now_flushed) = read();
            if now_flushed != flushed || since_hb.is_none() {
                (since_hb, flushed) = (hb, now_flushed);
                return false;
            }
            matches!((since_hb, hb), (Some(a), Some(b)) if b >= a + SPILL_IDLE_FOR.as_millis() as u64)
        },
    );
}

/// See [`wait_spill_idle`]: three idle iterations of the spill loop.
pub const SPILL_IDLE_FOR: Duration = Duration::from_millis(300);

/// One numeric `INFO all` field from the server on `port`; `None` when the
/// server cannot be asked or does not report it.
pub fn info_field(port: u16, name: &str) -> Option<u64> {
    let info = fetch_info(port).ok()?;
    let prefix = format!("{name}:");
    info.lines()
        .find_map(|l| l.strip_prefix(prefix.as_str()))
        .and_then(|v| v.trim().parse().ok())
}

/// `INFO` lines that say where a spill, an eviction or an append went:
/// memory against the cap, eviction and spill counters, AOF status and
/// backpressure counters, cold-tier file counts, save state.
const INFO_PREFIXES: &[&str] = &[
    "used_memory:",
    "maxmemory:",
    "maxmemory_policy:",
    "evicted_keys:",
    "loading:",
    "aof_",
    "spill_",
    "cold_",
    "rdb_",
    "current_cow_size:",
    "reclamation_cold_orphans_reclaimed_total:",
];

/// Evidence for a failure message: the [`INFO_PREFIXES`] lines of `INFO all`
/// from the server on `port`, and the last 40 lines of `dir/server.err`.
/// Never panics — it runs inside failure paths, against a server that may be
/// wedged or gone.
pub fn diagnostics(port: u16, dir: &Path) -> String {
    format!(
        "\n--- INFO (port {port}): memory, eviction, spill, AOF, cold tier ---\n{}\n--- last 40 \
         lines of {} ---\n{}",
        info_excerpt(port),
        dir.join("server.err").display(),
        server_err_tail(dir, 40)
    )
}

/// `INFO all` over a fresh connection, bounded by 2 s to connect and 10 s to
/// answer. Never panics.
fn fetch_info(port: u16) -> std::io::Result<String> {
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
    s.set_read_timeout(Some(Duration::from_millis(200)))?;
    s.write_all(&encode(&["INFO", "all"]))?;
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut buf = Vec::new();
    let mut chunk = [0u8; 65536];
    while framed_len(&buf, 1).is_none() {
        if Instant::now() >= deadline {
            return Err(std::io::Error::other(format!(
                "no complete INFO reply within 10 s ({} bytes)",
                buf.len()
            )));
        }
        match s.read(&mut chunk) {
            Ok(0) => return Err(std::io::Error::other("server closed the connection")),
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

fn info_excerpt(port: u16) -> String {
    match fetch_info(port) {
        Ok(info) => info
            .lines()
            .filter(|l| INFO_PREFIXES.iter().any(|p| l.starts_with(p)))
            .collect::<Vec<_>>()
            .join("\n"),
        Err(e) => format!("<INFO unavailable: {e}>"),
    }
}

/// The last `lines` lines of `dir/server.err` (the sink
/// [`super::server_stderr`] opens).
pub fn server_err_tail(dir: &Path, lines: usize) -> String {
    match std::fs::read(dir.join("server.err")) {
        Ok(b) => {
            let text = String::from_utf8_lossy(&b);
            let all: Vec<&str> = text.lines().collect();
            let tail = all[all.len().saturating_sub(lines)..].join("\n");
            if tail.is_empty() {
                "<empty>".to_string()
            } else {
                tail
            }
        }
        Err(e) => format!("<unreadable: {e}>"),
    }
}
