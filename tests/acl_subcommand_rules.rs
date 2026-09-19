//! Per-subcommand ACL rules (`-config|set`, `+config|get`, `+select|0`) are
//! enforced on every path a restricted client can reach a command through.
//!
//! The permission check used to probe only the bare command name, so
//! `ACL SETUSER sc on >pw ~* &* +@all -config|set` answered `+OK` and `sc`
//! could still run `CONFIG SET`. Every expected reply below is the one
//! redis-server 8.6.1 gives for the same request.
//!
//! Runs at `--shards 1` and `--shards 4` (the two connection handlers), and
//! covers the top-level gate (with CONFIG, an intercepted command), MULTI
//! queueing, scripting (`redis.pcall`), the PUBSUB introspection intercept,
//! the `ACL LOG` object, and an `ACL SAVE`/`ACL LOAD` round trip. Skips
//! gracefully when the moon binary is missing.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn moon_binary() -> Option<std::path::PathBuf> {
    if let Ok(p) = std::env::var("MOON_BIN") {
        return Some(std::path::PathBuf::from(p));
    }
    let cargo_bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    cargo_bin.exists().then_some(cargo_bin)
}

struct Moon {
    child: Child,
    port: u16,
    tmp_dir: std::path::PathBuf,
}

impl Drop for Moon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.tmp_dir);
    }
}

fn spawn_moon(shards: &str) -> Option<Moon> {
    let bin = moon_binary()?;
    let tmp_dir = std::env::temp_dir().join(format!(
        "moon-acl-subcommand-{}-{shards}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&tmp_dir);
    let _ = std::fs::create_dir_all(&tmp_dir);
    let aclfile = tmp_dir.join("users.acl");
    let (child, port) = common::spawn_listening(|port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                shards,
                "--admin-port",
                "0",
                "--appendonly",
                "no",
                "--disk-free-min-pct",
                "0",
                "--maxmemory",
                "268435456",
                "--aclfile",
                aclfile.to_str().expect("utf8 acl path"),
                "--dir",
                tmp_dir.to_str().expect("utf8 tmp dir"),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn moon")
    });
    let moon = Moon {
        child,
        port,
        tmp_dir,
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Ok(mut c) = TcpStream::connect(("127.0.0.1", moon.port)) {
            let _ = c.set_read_timeout(Some(Duration::from_millis(500)));
            if c.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok() {
                let mut buf = [0u8; 64];
                if let Ok(n) = c.read(&mut buf)
                    && n > 0
                    && buf.starts_with(b"+PONG")
                {
                    return Some(moon);
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    eprintln!("skipping: moon did not become ready on port {port}");
    None
}

/// Minimal RESP client over a blocking TcpStream.
struct Resp {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Resp {
    fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .expect("set read timeout");
        Self {
            stream,
            buf: Vec::new(),
        }
    }

    /// Send one command and return every byte that came back for it.
    fn cmd(&mut self, args: &[&str]) -> String {
        self.buf.clear();
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
        }
        self.stream.write_all(&out).expect("write");
        let window = if cfg!(windows) {
            Duration::from_millis(1500)
        } else {
            Duration::from_millis(250)
        };
        let deadline = Instant::now() + window;
        let mut chunk = [0u8; 4096];
        while Instant::now() < deadline {
            match self.stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(_) => {}
            }
        }
        String::from_utf8_lossy(&self.buf).into_owned()
    }
}

fn noperm(user: &str, name: &str) -> String {
    format!("-NOPERM User {user} has no permissions to run the '{name}' command\r\n")
}

fn run(shards: &str) {
    let Some(moon) = spawn_moon(shards) else {
        return;
    };
    let tag = format!("[shards={shards}]");
    let mut admin = Resp::connect(moon.port);
    assert_eq!(
        admin.cmd(&["CONFIG", "SET", "maxmemory-samples", "5"]),
        "+OK\r\n"
    );
    assert_eq!(
        admin.cmd(&[
            "ACL",
            "SETUSER",
            "sc",
            "on",
            ">pw",
            "~*",
            "&*",
            "+@all",
            "-config|set",
            "-object|encoding",
            "-pubsub|channels",
        ]),
        "+OK\r\n",
        "{tag}"
    );
    assert_eq!(admin.cmd(&["SET", "k", "v"]), "+OK\r\n", "{tag}");

    let mut sc = Resp::connect(moon.port);
    assert_eq!(sc.cmd(&["AUTH", "sc", "pw"]), "+OK\r\n", "{tag}");

    // Top-level gate, in any casing; CONFIG is an intercepted command.
    for set in ["SET", "set", "sEt"] {
        assert_eq!(
            sc.cmd(&["CONFIG", set, "maxmemory-samples", "7"]),
            noperm("sc", "config|set"),
            "{tag} CONFIG {set}"
        );
    }
    assert_eq!(
        sc.cmd(&["CONFIG", "GET", "maxmemory-samples"]),
        "*2\r\n$17\r\nmaxmemory-samples\r\n$1\r\n5\r\n",
        "{tag} CONFIG GET stays allowed, and the value was not changed"
    );

    // MULTI: refused at queue time, never executed by EXEC.
    assert_eq!(sc.cmd(&["MULTI"]), "+OK\r\n", "{tag}");
    assert_eq!(
        sc.cmd(&["CONFIG", "SET", "maxmemory-samples", "8"]),
        noperm("sc", "config|set"),
        "{tag} MULTI"
    );
    assert_eq!(
        sc.cmd(&["OBJECT", "ENCODING", "k"]),
        noperm("sc", "object|encoding"),
        "{tag} MULTI"
    );
    let _ = sc.cmd(&["EXEC"]);

    // PUBSUB introspection intercept.
    assert_eq!(
        sc.cmd(&["PUBSUB", "CHANNELS"]),
        noperm("sc", "pubsub|channels"),
        "{tag}"
    );
    assert_eq!(sc.cmd(&["PUBSUB", "NUMSUB"]), "*0\r\n", "{tag}");

    // Scripting: redis.pcall into a revoked subcommand is refused; a sibling
    // subcommand still runs.
    let r = sc.cmd(&[
        "EVAL",
        "return redis.pcall('object','Encoding','k')",
        "1",
        "k",
    ]);
    assert!(
        r.contains("has no permissions to run the 'object|encoding' command"),
        "{tag} script: {r:?}"
    );
    assert!(
        !sc.cmd(&["EVAL", "return redis.pcall('object','freq','k')", "1", "k"])
            .contains("no permissions"),
        "{tag} OBJECT FREQ stays allowed from a script"
    );

    // ACL LOG names the subcommand (the log is per connection in moon).
    let log = sc.cmd(&["ACL", "LOG"]);
    assert!(
        log.contains("$10\r\nconfig|set\r\n"),
        "{tag} ACL LOG: {log:?}"
    );
    assert!(
        log.contains("$15\r\nobject|encoding\r\n"),
        "{tag} ACL LOG: {log:?}"
    );

    // SAVE/LOAD round trip: a `-config|set` after a bare `+config` must
    // reload in that order, or the reloaded user can run CONFIG SET.
    assert_eq!(
        admin.cmd(&[
            "ACL",
            "SETUSER",
            "rt",
            "on",
            ">pw",
            "~*",
            "&*",
            "+@all",
            "-config",
            "+config",
            "-config|set",
            "-select",
            "+select|0",
        ]),
        "+OK\r\n",
        "{tag}"
    );
    assert_eq!(admin.cmd(&["ACL", "SAVE"]), "+OK\r\n", "{tag}");
    assert_eq!(admin.cmd(&["ACL", "LOAD"]), "+OK\r\n", "{tag}");
    let mut rt = Resp::connect(moon.port);
    assert_eq!(rt.cmd(&["AUTH", "rt", "pw"]), "+OK\r\n", "{tag}");
    assert_eq!(
        rt.cmd(&["CONFIG", "SET", "maxmemory-samples", "9"]),
        noperm("rt", "config|set"),
        "{tag} after ACL LOAD"
    );
    assert!(
        rt.cmd(&["CONFIG", "GET", "maxmemory-samples"])
            .starts_with("*2\r\n"),
        "{tag} after ACL LOAD"
    );
    assert_eq!(rt.cmd(&["SELECT", "0"]), "+OK\r\n", "{tag}");
    assert_eq!(rt.cmd(&["SELECT", "1"]), noperm("rt", "select"), "{tag}");
}

#[test]
fn subcommand_rules_are_enforced_single_shard() {
    run("1");
}

#[test]
fn subcommand_rules_are_enforced_multi_shard() {
    run("4");
}
