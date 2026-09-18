//! moon#982: a command routed to ANOTHER shard must be counted and timed.
//!
//! Before the fix, only connection-shard-local commands went through the
//! telemetry probe. A command whose key lives on another shard — served on
//! the origin thread by the cross-shard read fast path, or executed on the
//! owner via an SPSC message — was invisible to `total_commands_processed`
//! AND to `moon_command_duration_microseconds`. On `a8eb2efc`, 400 `SMEMBERS`
//! over 16 untagged keys on one connection counted 400 / 150 / 100 / 50 at
//! `--shards 1 / 2 / 4 / 8`: the counted fraction is `1/shards`, and with the
//! fast path off at 8 shards it was **0**.
//!
//! One server per test, ONE persistent connection for the traffic; `INFO`
//! is read over a separate connection so the traffic connection's reply
//! stream stays exactly N replies for N commands.
//!
//! Run against a pre-fix binary with `MOON_BIN=<path>`; the tests spawn
//! whatever `find_moon_binary` resolves.

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Enough untagged keys that, at 4 shards, the connection's own shard owns
/// only a fraction of them (the odds that all 16 land on one shard are
/// `4 * (1/4)^16`).
const KEYS: usize = 16;
/// 400 commands: what moon#982 was measured with.
const COMMANDS: usize = 400;
/// 320 reads = 20 sampled ticks on ONE sampler. The routed reads are now
/// sampled by whichever sampler executes them (the origin connection's for
/// the fast path, the owner shard's for SPSC), so the sum of `floor(n_i/16)`
/// across samplers is 20 minus at most one per extra sampler, plus the
/// phase the population writes left behind. `[15, 25]` accepts every
/// honest split and rejects both the pre-fix `~5` and a double count `~40`.
const HIST_READS: usize = 320;
const HIST_MIN: f64 = 15.0;
const HIST_MAX: f64 = 25.0;

struct TmpDir(std::path::PathBuf);

impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Field order is the drop order: the guard reaps the server first, then
/// the directory goes.
struct Moon {
    _guard: common::ServerGuard,
    _dir: TmpDir,
    port: u16,
    admin_port: u16,
}

fn spawn_moon(shards: &str, fast_path: &str) -> Moon {
    let bin = common::find_moon_binary();
    let admin_port = common::reserve_port();
    let dir = std::env::temp_dir().join(format!("moon-xshard-982-{admin_port}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tmp dir");
    let stderr = dir.join("moon.stderr");
    let (guard, port) = common::spawn_listening_guarded(|port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                shards,
                "--admin-port",
                &admin_port.to_string(),
                "--cross-shard-fast-path",
                fast_path,
                "--appendonly",
                "no",
                "--disk-free-min-pct",
                "0",
                "--dir",
                dir.to_str().expect("utf8 dir"),
            ])
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(&stderr).expect("stderr log"))
            .spawn()
            .expect("spawn moon")
    });
    Moon {
        _guard: guard,
        _dir: TmpDir(dir),
        port,
        admin_port,
    }
}

/// Minimal RESP2 value, enough for every reply these tests read.
#[derive(Debug, Clone, PartialEq)]
enum Resp {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Vec<Resp>),
}

struct Conn(BufReader<TcpStream>);

impl Conn {
    fn open(port: u16) -> Self {
        let s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        s.set_read_timeout(Some(Duration::from_secs(30)))
            .expect("rd timeout");
        s.set_write_timeout(Some(Duration::from_secs(30)))
            .expect("wr timeout");
        Conn(BufReader::new(s))
    }

    fn encode(out: &mut Vec<u8>, argv: &[&[u8]]) {
        out.extend_from_slice(format!("*{}\r\n", argv.len()).as_bytes());
        for a in argv {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a);
            out.extend_from_slice(b"\r\n");
        }
    }

    fn read_line(&mut self) -> String {
        let mut line = String::new();
        let n = self.0.read_line(&mut line).expect("read line");
        assert!(n > 0, "server closed the connection");
        line.trim_end_matches("\r\n").to_string()
    }

    fn read_reply(&mut self) -> Resp {
        let line = self.read_line();
        let (tag, rest) = line.split_at(1);
        match tag {
            "+" => Resp::Simple(rest.to_string()),
            "-" => Resp::Error(rest.to_string()),
            ":" => Resp::Int(rest.parse().expect("int reply")),
            "$" => {
                let len: i64 = rest.parse().expect("bulk len");
                if len < 0 {
                    return Resp::Bulk(None);
                }
                let mut buf = vec![0u8; len as usize + 2];
                self.0.read_exact(&mut buf).expect("bulk payload");
                buf.truncate(len as usize);
                Resp::Bulk(Some(buf))
            }
            "*" => {
                let n: i64 = rest.parse().expect("array len");
                let mut items = Vec::new();
                for _ in 0..n.max(0) {
                    items.push(self.read_reply());
                }
                Resp::Array(items)
            }
            other => panic!("unexpected RESP tag {other:?} in {line:?}"),
        }
    }

    /// Send every argv in `cmds` as one pipeline; read exactly that many
    /// replies back.
    fn pipeline(&mut self, cmds: &[Vec<Vec<u8>>]) -> Vec<Resp> {
        let mut out = Vec::new();
        for argv in cmds {
            let borrowed: Vec<&[u8]> = argv.iter().map(Vec::as_slice).collect();
            Self::encode(&mut out, &borrowed);
        }
        self.0.get_mut().write_all(&out).expect("write pipeline");
        (0..cmds.len()).map(|_| self.read_reply()).collect()
    }

    fn cmd(&mut self, argv: &[&[u8]]) -> Resp {
        let owned = vec![argv.iter().map(|a| a.to_vec()).collect::<Vec<_>>()];
        self.pipeline(&owned).remove(0)
    }
}

/// `total_commands_processed` from `INFO stats`.
fn total_commands_processed(port: u16) -> u64 {
    let mut c = Conn::open(port);
    let Resp::Bulk(Some(body)) = c.cmd(&[b"INFO", b"stats"]) else {
        panic!("INFO stats must answer a bulk string");
    };
    let body = String::from_utf8_lossy(&body);
    body.lines()
        .find_map(|l| l.strip_prefix("total_commands_processed:"))
        .map(|v| v.trim().parse::<u64>().expect("counter value"))
        .unwrap_or_else(|| panic!("no total_commands_processed in INFO stats:\n{body}"))
}

/// `GET /metrics` over raw HTTP/1.1; returns the body.
fn scrape_metrics(admin_port: u16) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(mut s) = TcpStream::connect(("127.0.0.1", admin_port)) {
            s.set_read_timeout(Some(Duration::from_secs(5)))
                .expect("rd timeout");
            s.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .expect("http write");
            let mut buf = String::new();
            s.read_to_string(&mut buf).expect("http read");
            let body_start = buf.find("\r\n\r\n").map(|i| i + 4).unwrap_or(0);
            return buf[body_start..].to_string();
        }
        assert!(
            Instant::now() < deadline,
            "admin port {admin_port} never answered"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// `moon_command_duration_microseconds_count{cmd="<cmd>"} <value>`, or
/// `None` when the series does not exist at all.
fn duration_count(body: &str, cmd: &str) -> Option<f64> {
    let prefix = format!("moon_command_duration_microseconds_count{{cmd=\"{cmd}\"}} ");
    body.lines()
        .find_map(|l| l.strip_prefix(&prefix))
        .map(|v| v.trim().parse::<f64>().expect("metric value"))
}

fn key(i: usize) -> Vec<u8> {
    format!("s{}", i % KEYS).into_bytes()
}

/// `SADD s<i> a b c` for every key, then `n` `SMEMBERS` spread over them.
fn populate_and_read(port: u16, n: usize) {
    let mut c = Conn::open(port);
    let sadds: Vec<Vec<Vec<u8>>> = (0..KEYS)
        .map(|i| {
            vec![
                b"SADD".to_vec(),
                key(i),
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
            ]
        })
        .collect();
    for r in c.pipeline(&sadds) {
        assert!(matches!(r, Resp::Int(_)), "SADD reply: {r:?}");
    }
    let reads: Vec<Vec<Vec<u8>>> = (0..n).map(|i| vec![b"SMEMBERS".to_vec(), key(i)]).collect();
    let replies = c.pipeline(&reads);
    assert_eq!(replies.len(), n);
    for r in &replies {
        assert!(
            matches!(r, Resp::Array(v) if v.len() == 3),
            "SMEMBERS reply: {r:?}"
        );
    }
}

fn assert_counted(shards: &str, fast_path: &str) {
    let moon = spawn_moon(shards, fast_path);
    // Populate on its own connection so the measured window holds exactly
    // the reads.
    {
        let mut c = Conn::open(moon.port);
        let sadds: Vec<Vec<Vec<u8>>> = (0..KEYS)
            .map(|i| {
                vec![
                    b"SADD".to_vec(),
                    key(i),
                    b"a".to_vec(),
                    b"b".to_vec(),
                    b"c".to_vec(),
                ]
            })
            .collect();
        c.pipeline(&sadds);
    }
    let before = total_commands_processed(moon.port);
    {
        let mut c = Conn::open(moon.port);
        let reads: Vec<Vec<Vec<u8>>> = (0..COMMANDS)
            .map(|i| vec![b"SMEMBERS".to_vec(), key(i)])
            .collect();
        let replies = c.pipeline(&reads);
        assert_eq!(replies.len(), COMMANDS);
        assert!(
            replies
                .iter()
                .all(|r| matches!(r, Resp::Array(v) if v.len() == 3))
        );
    }
    let after = total_commands_processed(moon.port);
    let counted = after - before;
    // The two INFO reads bracket the window; whether either lands inside it
    // is an accounting detail of INFO, not of this fix.
    let ok = (COMMANDS as u64..=COMMANDS as u64 + 2).contains(&counted);
    assert!(
        ok,
        "moon#982: --shards {shards} --cross-shard-fast-path {fast_path}: sent {COMMANDS} \
         SMEMBERS over {KEYS} untagged keys on one connection, INFO counted {counted} \
         (a 1/shards fraction means routed commands are not accounted)"
    );
}

/// moon#982: `total_commands_processed` sees a command executed on the
/// shard that OWNS its key — with the fast path on (served on the origin
/// thread) and off (executed by the owner via SPSC).
#[test]
fn routed_commands_are_counted_with_the_fast_path_on() {
    assert_counted("4", "auto");
}

#[test]
fn routed_commands_are_counted_over_spsc_with_the_fast_path_off() {
    assert_counted("4", "off");
}

/// Eight shards, fast path off: the pre-fix count here was literally 0 when
/// the connection's shard owned none of the 16 keys.
#[test]
fn routed_commands_are_counted_at_eight_shards() {
    assert_counted("8", "off");
}

/// Single-shard control: the local path was always counted; this pins the
/// window arithmetic (`COMMANDS..=COMMANDS+2`) against the same binary.
#[test]
fn local_commands_are_still_counted_at_one_shard() {
    assert_counted("1", "auto");
}

/// moon#982: routed reads land in the duration histogram. Pre-fix at four
/// shards only the connection-local quarter was sampled (~5 of 20).
#[test]
fn routed_reads_are_sampled_into_the_histogram() {
    for fast_path in ["auto", "off"] {
        let moon = spawn_moon("4", fast_path);
        populate_and_read(moon.port, HIST_READS);
        let body = scrape_metrics(moon.admin_port);
        let count = duration_count(&body, "smembers");
        assert!(
            count.is_some_and(|c| (HIST_MIN..=HIST_MAX).contains(&c)),
            "moon#982: --cross-shard-fast-path {fast_path}: {HIST_READS} SMEMBERS over \
             {KEYS} keys at 4 shards must sample about {} times, got {count:?}",
            HIST_READS / 16
        );
    }
}
