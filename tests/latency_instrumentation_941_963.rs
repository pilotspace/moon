//! moon#941 / moon#963: the latency telemetry must see writes and the
//! inline `GET`/`SET` path.
//!
//! Before the fix, on the shipped (monoio) runtime:
//!
//! - every write reported **0 µs** — the write path constructed its timer
//!   after the command had already run — so `SLOWLOG` could never fire for a
//!   write (moon#941);
//! - `GET`/`SET` emitted **no** `moon_command_duration_microseconds` series at
//!   all, because `try_inline_dispatch` recorded nothing (moon#963).
//!
//! One server per test, ONE persistent connection: the sampler is `1-in-16`
//! per connection, so a fresh connection per command never samples and would
//! make every assertion here pass or fail for the wrong reason. 320 repeats of
//! each command give exactly 20 samples per command name.
//!
//! Run against a pre-fix binary with `MOON_BIN=<path>`; the tests spawn
//! whatever `find_moon_binary` resolves.

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// A slow write: `SADD` of this many members is strictly more work than the
/// `SMEMBERS` control, and tens of microseconds even on a fast box, so a
/// correctly placed timer cannot truncate it to 0.
const SADD_MEMBERS: usize = 3000;
/// 320 / 16 = 20 sampled ticks per command name.
const REPEATS: usize = 320;
const EXPECTED_SAMPLES: u64 = (REPEATS / 16) as u64;

/// Removes the server's `--dir` on drop.
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

fn spawn_moon() -> Moon {
    let bin = common::find_moon_binary();
    let admin_port = common::reserve_port();
    let dir = std::env::temp_dir().join(format!("moon-lat-941-{admin_port}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tmp dir");
    let stderr = dir.join("moon.stderr");
    let (guard, port) = common::spawn_listening_guarded(|port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--admin-port",
                &admin_port.to_string(),
                // 1 µs: a real duration lands, a truncated-to-zero one does not.
                "--slowlog-log-slower-than",
                "1",
                "--slowlog-max-len",
                "1024",
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

    /// Send the same command `n` times as one pipeline; read exactly `n`
    /// replies back. Returns the replies.
    fn repeat(&mut self, argv: &[&[u8]], n: usize) -> Vec<Resp> {
        let mut out = Vec::new();
        for _ in 0..n {
            Self::encode(&mut out, argv);
        }
        self.0.get_mut().write_all(&out).expect("write pipeline");
        (0..n).map(|_| self.read_reply()).collect()
    }

    fn cmd(&mut self, argv: &[&[u8]]) -> Resp {
        self.repeat(argv, 1).remove(0)
    }
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

/// `moon_command_duration_microseconds_<suffix>{cmd="<cmd>"} <value>`, or
/// `None` when the series does not exist at all.
fn series(body: &str, suffix: &str, cmd: &str) -> Option<f64> {
    let prefix = format!("moon_command_duration_microseconds_{suffix}{{cmd=\"{cmd}\"}} ");
    body.lines()
        .find_map(|l| l.strip_prefix(&prefix))
        .map(|v| v.trim().parse::<f64>().expect("metric value"))
}

/// Only the `count` lines, for a readable failure message.
fn count_lines(body: &str) -> String {
    body.lines()
        .filter(|l| l.starts_with("moon_command_duration_microseconds_count"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// `(command name, duration_us)` for every slowlog entry.
fn slowlog_entries(c: &mut Conn) -> Vec<(String, i64)> {
    let Resp::Array(entries) = c.cmd(&[b"SLOWLOG", b"GET", b"1024"]) else {
        panic!("SLOWLOG GET must answer an array");
    };
    entries
        .iter()
        .map(|e| {
            let Resp::Array(fields) = e else {
                panic!("slowlog entry must be an array: {e:?}");
            };
            let Resp::Int(duration) = fields[2] else {
                panic!("slowlog duration must be an integer: {e:?}");
            };
            let Resp::Array(argv) = &fields[3] else {
                panic!("slowlog argv must be an array: {e:?}");
            };
            let Resp::Bulk(Some(name)) = &argv[0] else {
                panic!("slowlog argv[0] must be a bulk string: {e:?}");
            };
            (String::from_utf8_lossy(name).to_ascii_lowercase(), duration)
        })
        .collect()
}

/// moon#941: a slow WRITE reports its duration on the histogram and reaches
/// SLOWLOG. The read control over the same members proves the sampler, the
/// histogram and the slowlog all work — the write arm is the only variable.
#[test]
fn slow_write_reports_a_real_duration_and_reaches_slowlog() {
    let moon = spawn_moon();
    let mut c = Conn::open(moon.port);

    let members: Vec<String> = (0..SADD_MEMBERS).map(|i| format!("m{i}")).collect();
    let mut sadd: Vec<&[u8]> = vec![b"SADD", b"w"];
    sadd.extend(members.iter().map(|m| m.as_bytes()));
    let replies = c.repeat(&sadd, REPEATS);
    assert!(
        matches!(replies[0], Resp::Int(n) if n == SADD_MEMBERS as i64),
        "{:?}",
        replies[0]
    );
    let replies = c.repeat(&[b"SMEMBERS", b"w"], REPEATS);
    assert!(matches!(&replies[0], Resp::Array(v) if v.len() == SADD_MEMBERS));

    let body = scrape_metrics(moon.admin_port);
    // The sampler fired identically on both arms: this separates "the probe
    // ran" from "what it recorded".
    for cmd in ["sadd", "smembers"] {
        assert_eq!(
            series(&body, "count", cmd),
            Some(EXPECTED_SAMPLES as f64),
            "`{cmd}` must be sampled exactly {EXPECTED_SAMPLES} times on one connection\n{}",
            count_lines(&body)
        );
    }
    let sadd_sum = series(&body, "sum", "sadd").expect("sadd sum series");
    let smembers_sum = series(&body, "sum", "smembers").expect("smembers sum series");
    assert!(
        sadd_sum > 0.0,
        "moon#941: {EXPECTED_SAMPLES} sampled SADDs of {SADD_MEMBERS} members summed to \
         {sadd_sum} us (SMEMBERS control over the same members: {smembers_sum} us)"
    );

    // The user-visible surface: the slow write is in SLOWLOG with a real
    // duration. The 1 us threshold is what a truncated-to-zero sample can
    // never cross.
    let entries = slowlog_entries(&mut c);
    let sadd_entries: Vec<&(String, i64)> = entries.iter().filter(|(n, _)| n == "sadd").collect();
    assert!(
        !sadd_entries.is_empty(),
        "moon#941: no SADD entry in SLOWLOG after {REPEATS} slow SADDs (entries: {entries:?})"
    );
    assert!(
        sadd_entries.iter().all(|(_, d)| *d >= 1),
        "every logged SADD must carry its duration: {sadd_entries:?}"
    );
    assert!(
        entries.iter().any(|(n, _)| n == "smembers"),
        "control: SMEMBERS must appear in SLOWLOG (entries: {entries:?})"
    );
}

/// moon#963: plain `GET`/`SET` — the inline fast path — are sampled like
/// every other command. An absent series is an uninstrumented path, not "no
/// traffic".
#[test]
fn inline_get_and_set_are_sampled_into_the_histogram() {
    let moon = spawn_moon();
    let mut c = Conn::open(moon.port);

    let replies = c.repeat(&[b"SET", b"k", b"v"], REPEATS);
    assert_eq!(replies[0], Resp::Simple("OK".into()));
    let replies = c.repeat(&[b"GET", b"k"], REPEATS);
    assert_eq!(replies[0], Resp::Bulk(Some(b"v".to_vec())));
    // A generic-path control on the same connection, so the scrape is known
    // to contain this connection's samples at all.
    let replies = c.repeat(&[b"HSET", b"h", b"f", b"v"], REPEATS);
    assert!(matches!(replies[0], Resp::Int(_)), "{:?}", replies[0]);

    let body = scrape_metrics(moon.admin_port);
    assert_eq!(
        series(&body, "count", "hset"),
        Some(EXPECTED_SAMPLES as f64),
        "control: the generic path is sampled\n{}",
        count_lines(&body)
    );
    for cmd in ["set", "get"] {
        assert_eq!(
            series(&body, "count", cmd),
            Some(EXPECTED_SAMPLES as f64),
            "moon#963: `{cmd}` must be sampled exactly {EXPECTED_SAMPLES} times on one \
             connection (absent = the inline path is uninstrumented)\n{}",
            count_lines(&body)
        );
    }
}
