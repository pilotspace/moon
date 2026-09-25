//! Shared harness for the WS7 (`perf_ws7_*`) connection-path suites: spawn a
//! `MOON_BIN` server with the Prometheus exporter on, and read counters from
//! `/metrics`. Every suite records its red run by pointing `MOON_BIN` at the
//! pre-fix binary, so nothing here may depend on the fixes themselves.
#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use super::common;

pub struct Server {
    _guard: common::ServerGuard,
    pub port: u16,
    pub admin_port: u16,
    pub dir: std::path::PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Start `MOON_BIN` with the exporter on `admin_port`, persistence off, and
/// `extra` appended. Retries on fresh ports if the child comes up dead (an
/// already-held admin port exits moon at start-up; `spawn_listening` only
/// re-picks the client port).
pub fn spawn(tag: &str, shards: &str, extra: &[&str]) -> Server {
    let bin = common::find_moon_binary();
    for attempt in 1..=3 {
        let dir = common::unique_test_dir(tag);
        std::fs::create_dir_all(&dir).unwrap();
        let admin_port = common::reserve_port();
        let d = dir.clone();
        let (guard, port) = common::spawn_listening_guarded(|port| {
            let mut args: Vec<String> = [
                "--port",
                &port.to_string(),
                "--shards",
                shards,
                "--admin-port",
                &admin_port.to_string(),
                "--appendonly",
                "no",
                "--save",
                "",
                "--disk-free-min-pct",
                "0",
                "--dir",
                d.to_str().unwrap(),
            ]
            .iter()
            .map(|s| s.to_string())
            .collect();
            args.extend(extra.iter().map(|s| s.to_string()));
            Command::new(&bin)
                .args(&args)
                .stdout(Stdio::null())
                .stderr(common::server_stderr(&d))
                .spawn()
                .expect("spawn moon")
        });
        if try_http_get(admin_port, "/metrics").is_some() {
            return Server {
                _guard: guard,
                port,
                admin_port,
                dir,
            };
        }
        eprintln!("spawn attempt {attempt}: admin port {admin_port} never answered, retrying");
        drop(guard);
    }
    panic!("moon never came up with a live admin port");
}

fn try_http_get(port: u16, path: &str) -> Option<String> {
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let start = Instant::now();
    let mut stream = loop {
        match TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
            Ok(s) => break s,
            Err(_) if start.elapsed() < Duration::from_secs(20) => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(_) => return None,
        }
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    stream
        .write_all(format!("GET {path} HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n").as_bytes())
        .ok()?;
    let mut body = Vec::new();
    stream.read_to_end(&mut body).ok()?;
    let text = String::from_utf8_lossy(&body).into_owned();
    text.contains("200").then_some(text)
}

/// One `/metrics` scrape (asserted 200).
pub fn scrape(admin_port: u16) -> String {
    try_http_get(admin_port, "/metrics").expect("admin /metrics answered non-200")
}

/// The value of the first sample of `name` whose label set contains every
/// `label` fragment (e.g. `path="local_inline"`). metrics-rs does not emit an
/// untouched counter, so absent reads as 0.
pub fn sample(body: &str, name: &str, labels: &[&str]) -> u64 {
    for line in body.lines() {
        let Some(rest) = line.strip_prefix(name) else {
            continue;
        };
        if !(rest.starts_with('{') || rest.starts_with(' ')) {
            continue;
        }
        if labels.iter().all(|l| rest.contains(l)) {
            let v = line.rsplit(' ').next().unwrap_or("0");
            return v.trim().parse::<f64>().unwrap_or(0.0) as u64;
        }
    }
    0
}

/// `moon_dispatch_path_total{path="local_inline"}` right now.
pub fn local_inline(admin_port: u16) -> u64 {
    sample(
        &scrape(admin_port),
        "moon_dispatch_path_total",
        &["path=\"local_inline\""],
    )
}

/// A plain RESP connection with the default 5 s socket timeouts.
pub fn conn(port: u16) -> common::Conn {
    common::Conn::open(port)
}
