//! Minimal self-contained RESP2 client (pattern:
//! `tests/crash_recovery_mq_effects.rs::Conn` / `tests/shardslice_live.rs`).
//! Deliberately duplicated rather than shared — every other crash suite in
//! this repo carries its own copy too (each has slightly different framing
//! needs: RESP3 maps, pipelining, etc.), and unifying RESP clients is a
//! separate, larger refactor than the harness-primitive consolidation this
//! stage takes on (see the module doc in `crash_matrix_cross_plane.rs`).

#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq)]
pub enum Resp {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Resp>>),
}

impl Resp {
    pub fn flat(&self) -> String {
        match self {
            Resp::Simple(s) | Resp::Error(s) => s.clone(),
            Resp::Int(i) => i.to_string(),
            Resp::Bulk(Some(b)) => String::from_utf8_lossy(b).into_owned(),
            Resp::Bulk(None) => "<nil>".into(),
            Resp::Array(Some(items)) => items.iter().map(Resp::flat).collect::<Vec<_>>().join(" "),
            Resp::Array(None) => "<nil-array>".into(),
        }
    }

    pub fn is_ok(&self) -> bool {
        matches!(self, Resp::Simple(s) if s == "OK")
    }
}

pub fn connect(port: u16, deadline: Duration) -> TcpStream {
    let addr = format!("127.0.0.1:{port}")
        .to_socket_addrs()
        .expect("parse addr")
        .next()
        .expect("one addr");
    let start = Instant::now();
    loop {
        match TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
            Ok(s) => {
                s.set_read_timeout(Some(Duration::from_secs(15))).ok();
                s.set_write_timeout(Some(Duration::from_secs(15))).ok();
                return s;
            }
            Err(_) if start.elapsed() < deadline => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => panic!("server never accepted on port {port}: {e}"),
        }
    }
}

/// Poll `PING` until the server answers `PONG` — a successful TCP accept
/// only means "listening", not "serving" (recovery/replay can still be
/// running Phase B before the dispatch loop comes up).
pub fn wait_ready(port: u16) -> TcpStream {
    let mut s = connect(port, Duration::from_secs(30));
    let start = Instant::now();
    loop {
        s.write_all(b"PING\r\n").expect("write PING");
        let mut buf = [0u8; 64];
        if let Ok(n) = s.read(&mut buf)
            && n > 0
            && buf[..n].windows(4).any(|w| w == b"PONG")
        {
            return s;
        }
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "server accepted TCP but never answered PING on port {port}"
        );
        std::thread::sleep(Duration::from_millis(100));
        s = connect(port, Duration::from_secs(10));
    }
}

pub struct Conn {
    s: TcpStream,
    buf: Vec<u8>,
    pos: usize,
    /// Last command (or pipeline summary) sent on this connection — panic
    /// context for the rare mid-scenario ConnectionReset (#365): knowing
    /// WHICH phase reset is diagnostic step 2 in that issue.
    last_cmd: String,
}

impl Conn {
    pub fn new(s: TcpStream) -> Self {
        Conn {
            s,
            buf: Vec::with_capacity(16 * 1024),
            pos: 0,
            last_cmd: String::new(),
        }
    }

    /// Record a compact description of an outgoing command for panic context.
    fn note_cmd(&mut self, parts: &[&[u8]]) {
        let mut desc = String::with_capacity(48);
        for p in parts.iter().take(2) {
            if !desc.is_empty() {
                desc.push(' ');
            }
            desc.push_str(&String::from_utf8_lossy(&p[..p.len().min(32)]));
        }
        if parts.len() > 2 {
            desc.push_str(" …");
        }
        self.last_cmd = desc;
    }

    pub fn open(port: u16) -> Self {
        Conn::new(connect(port, Duration::from_secs(15)))
    }

    pub fn cmd(&mut self, parts: &[&[u8]]) -> Resp {
        self.note_cmd(parts);
        let mut req = Vec::with_capacity(128);
        req.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for p in parts {
            req.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            req.extend_from_slice(p);
            req.extend_from_slice(b"\r\n");
        }
        if let Err(e) = self.s.write_all(&req) {
            panic!("write cmd [{}]: {e:?}", self.last_cmd);
        }
        self.frame()
    }

    pub fn cmd_s(&mut self, parts: &[&str]) -> Resp {
        let v: Vec<&[u8]> = parts.iter().map(|p| p.as_bytes()).collect();
        self.cmd(&v)
    }

    /// Send every command without waiting for a reply, then read all replies
    /// in order (pipelining — used to send `MULTI`-body writes as a burst
    /// right before a kill so the harness controls exactly what got queued
    /// on the wire before the process died).
    pub fn pipeline_s(&mut self, cmds: &[Vec<&str>]) -> Vec<Resp> {
        let mut buf = Vec::new();
        for c in cmds {
            let mut req = Vec::with_capacity(64);
            req.extend_from_slice(format!("*{}\r\n", c.len()).as_bytes());
            for p in c {
                let bytes = p.as_bytes();
                req.extend_from_slice(format!("${}\r\n", bytes.len()).as_bytes());
                req.extend_from_slice(bytes);
                req.extend_from_slice(b"\r\n");
            }
            buf.extend_from_slice(&req);
        }
        self.last_cmd = format!(
            "pipeline[{}]: {} .. {}",
            cmds.len(),
            cmds.first().map(|c| c.join(" ")).unwrap_or_default(),
            cmds.last().map(|c| c.join(" ")).unwrap_or_default()
        );
        if let Err(e) = self.s.write_all(&buf) {
            panic!("write pipeline [{}]: {e:?}", self.last_cmd);
        }
        cmds.iter().map(|_| self.frame()).collect()
    }

    fn fill(&mut self) {
        let mut chunk = [0u8; 16 * 1024];
        let n = match self.s.read(&mut chunk) {
            Ok(n) => n,
            // #365 diagnostic: the rare mid-scenario ConnectionReset lands
            // here — the last command sent tells us WHICH phase reset.
            Err(e) => panic!(
                "read from server after [{}] (peer {:?}): {e:?}",
                self.last_cmd,
                self.s.peer_addr()
            ),
        };
        assert!(
            n > 0,
            "connection closed mid-frame after [{}] (peer {:?})",
            self.last_cmd,
            self.s.peer_addr()
        );
        self.buf.extend_from_slice(&chunk[..n]);
    }

    fn line(&mut self) -> String {
        loop {
            if let Some(rel) = self.buf[self.pos..].windows(2).position(|w| w == b"\r\n") {
                let line =
                    String::from_utf8_lossy(&self.buf[self.pos..self.pos + rel]).into_owned();
                self.pos += rel + 2;
                return line;
            }
            self.fill();
        }
    }

    fn exact(&mut self, n: usize) -> Vec<u8> {
        while self.buf.len() - self.pos < n + 2 {
            self.fill();
        }
        let out = self.buf[self.pos..self.pos + n].to_vec();
        self.pos += n + 2;
        out
    }

    /// Read one RESP frame. Used both for command replies and for
    /// unsolicited pub/sub pushes on a SUBSCRIBEd connection.
    pub fn frame(&mut self) -> Resp {
        if self.pos > 0 && self.pos == self.buf.len() {
            self.buf.clear();
            self.pos = 0;
        }
        let line = self.line();
        let (tag, rest) = line.split_at(1);
        match tag {
            "+" => Resp::Simple(rest.to_string()),
            "-" => Resp::Error(rest.to_string()),
            ":" => Resp::Int(rest.parse().unwrap_or(0)),
            "$" => {
                let n: i64 = rest.parse().unwrap_or(-1);
                if n < 0 {
                    Resp::Bulk(None)
                } else {
                    Resp::Bulk(Some(self.exact(n as usize)))
                }
            }
            "*" => {
                let n: i64 = rest.parse().unwrap_or(-1);
                if n < 0 {
                    Resp::Array(None)
                } else {
                    let mut items = Vec::with_capacity(n as usize);
                    for _ in 0..n {
                        items.push(self.frame());
                    }
                    Resp::Array(Some(items))
                }
            }
            other => panic!("unexpected RESP tag {other:?} in line {line:?}"),
        }
    }
}

pub fn as_array(r: &Resp) -> &[Resp] {
    match r {
        Resp::Array(Some(items)) => items,
        other => panic!("expected array, got {other:?}"),
    }
}

pub fn as_int(r: &Resp) -> i64 {
    match r {
        Resp::Int(i) => *i,
        other => panic!("expected integer, got {other:?}"),
    }
}
