//! INFO must answer the question the client actually asked.
//!
//! Measured against redis-server 8.6.1. Moon's `info()` takes `_args` and
//! discards it, so section selection does not exist:
//!
//! ```text
//! INFO replication
//!   redis: # Replication                                   (1 section)
//!   moon : # Server # Clients # Memory # Persistence # Vector # MoonStore
//!          # Reclamation # Stats # CPU # Replication # Commandstats
//!          # Keyspace # Replication                        (13, one twice)
//! ```
//!
//! A monitoring agent that scrapes `INFO replication` every second is paying
//! for the whole payload and parsing a duplicated header. Separately, Moon
//! exposes 61 fields where Redis exposes 213, and the missing ones are the
//! ones dashboards read: keyspace_hits, evicted_keys, maxmemory_policy.
//!
//! Raw sockets: redis-rs parses INFO into a map, which would hide both the
//! section ORDER and the duplicate-header bug under test.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

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

fn spawn_moon(shards: &str) -> Moon {
    spawn_moon_in(shards, None)
}

/// A server with persistence ON. BGSAVE only completes when a durability
/// backstop is configured — with `appendonly no` and no `--save`, Moon logs
/// "BGSAVE triggered" and the snapshot epoch is never advanced, so a test that
/// waits for a save on the default spawner waits forever.
fn spawn_moon_persistent(shards: &str) -> Moon {
    spawn_moon_with(shards, None, &["--appendonly", "yes"])
}

/// `dir` lets a restart test reuse the SAME data dir, which is the only way
/// to prove run_id changes for reasons other than a fresh dataset.
fn spawn_moon_in(shards: &str, dir: Option<std::path::PathBuf>) -> Moon {
    spawn_moon_with(shards, dir, &["--appendonly", "no"])
}

fn spawn_moon_with(shards: &str, dir: Option<std::path::PathBuf>, extra: &[&str]) -> Moon {
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let fixed_dir = dir.clone();
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = fixed_dir
            .clone()
            .unwrap_or_else(|| std::env::temp_dir().join(format!("moon-infoobs-{port}")));
        let _ = std::fs::create_dir_all(&tmp_dir);
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                shards,
                "--admin-port",
                "0",
                "--disk-free-min-pct",
                "0",
                "--dir",
                tmp_dir.to_str().unwrap(),
            ])
            .args(extra)
            .stdout(Stdio::null())
            .stderr(
                std::fs::File::create(tmp_dir.join("moon.stderr")).expect("create moon stderr log"),
            )
            .spawn()
            .expect("spawn moon")
    });
    let tmp_dir = dir.unwrap_or_else(|| std::env::temp_dir().join(format!("moon-infoobs-{port}")));
    let mut moon = Moon {
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
                    return moon;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let status = match moon.child.try_wait() {
        Ok(Some(s)) => format!("exited with {s}"),
        Ok(None) => "still running but never answered PING".to_string(),
        Err(e) => format!("status unavailable: {e}"),
    };
    let log = std::fs::read_to_string(moon.tmp_dir.join("moon.stderr")).unwrap_or_default();
    panic!("moon never became ready on port {port} ({status})\n--- stderr ---\n{log}");
}

struct Conn(TcpStream);

impl Conn {
    fn open(port: u16) -> Self {
        let s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        s.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
        Conn(s)
    }

    fn send(&mut self, parts: &[&str]) -> String {
        let mut out = format!("*{}\r\n", parts.len());
        for p in parts {
            out.push_str(&format!("${}\r\n{p}\r\n", p.len()));
        }
        self.0.write_all(out.as_bytes()).expect("write");
        self.read_reply()
    }

    /// Write a command without waiting for its reply — for commands that are
    /// SUPPOSED not to answer yet (a parked `BLPOP`, a `SUBSCRIBE` whose push
    /// stream we do not consume). Reading here would block the test, not the
    /// server.
    fn write_only(&mut self, parts: &[&str]) {
        let mut out = format!("*{}\r\n", parts.len());
        for p in parts {
            out.push_str(&format!("${}\r\n{p}\r\n", p.len()));
        }
        self.0.write_all(out.as_bytes()).expect("write");
    }

    fn read_reply(&mut self) -> String {
        let mut buf = [0u8; 16384];
        let mut acc = Vec::new();
        loop {
            match self.0.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    acc.extend_from_slice(&buf[..n]);
                    self.0
                        .set_read_timeout(Some(Duration::from_millis(200)))
                        .unwrap();
                }
                Err(_) => break,
            }
        }
        self.0
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        String::from_utf8_lossy(&acc).into_owned()
    }
}

/// Section headers, in reply order. `INFO` is a bulk string whose payload is
/// CRLF-delimited, so the `$<len>` prefix line is skipped by the `#` filter.
fn headers(reply: &str) -> Vec<String> {
    reply
        .lines()
        .map(|l| l.trim_end_matches('\r'))
        .filter(|l| l.starts_with('#'))
        .map(|l| l.to_string())
        .collect()
}

fn field(reply: &str, name: &str) -> Option<String> {
    reply
        .lines()
        .map(|l| l.trim_end_matches('\r'))
        .find_map(|l| l.strip_prefix(&format!("{name}:")).map(|v| v.to_string()))
}

// ---------------------------------------------------------------------------
// io1 — the headline. This is why the task exists.
// ---------------------------------------------------------------------------

#[test]
fn io1_single_section_only() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    let reply = c.send(&["INFO", "replication"]);
    let h = headers(&reply);
    assert_eq!(
        h,
        vec!["# Replication"],
        "INFO <section> must return ONLY that section — a monitoring agent \
         polling INFO replication should not pay for the whole payload. got {h:?}"
    );
}

#[test]
fn io2_section_case_insensitive() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    let lower = headers(&c.send(&["INFO", "replication"]));
    let upper = headers(&c.send(&["INFO", "REPLICATION"]));
    assert_eq!(
        lower, upper,
        "section matching is case-insensitive in Redis; clients send both"
    );
    assert_eq!(upper, vec!["# Replication"]);
}

#[test]
fn io3_multiple_sections() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    let h = headers(&c.send(&["INFO", "server", "clients"]));
    assert_eq!(
        h,
        vec!["# Server", "# Clients"],
        "INFO accepts several section names and returns exactly those, in the \
         server's canonical order — not the caller's. got {h:?}"
    );
}

#[test]
fn io4_unknown_section_is_empty() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    let reply = c.send(&["INFO", "nosuchsection"]);
    assert!(
        !reply.starts_with('-'),
        "an unknown section is NOT an error in Redis — it is an empty bulk \
         string. Erroring here breaks clients that probe optional sections. got {reply:?}"
    );
    assert!(
        headers(&reply).is_empty(),
        "unknown section must yield no sections; got {:?}",
        headers(&reply)
    );
    // The connection must survive — an unknown section is not a protocol fault.
    assert!(c.send(&["PING"]).starts_with("+PONG"));
}

#[test]
fn io5_default_omits_commandstats() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    let default = headers(&c.send(&["INFO"]));
    let all = headers(&c.send(&["INFO", "all"]));
    assert!(
        !default.iter().any(|h| h == "# Commandstats"),
        "Redis's default INFO omits Commandstats — it is per-command data that \
         grows with the command table and is only emitted on request. got {default:?}"
    );
    assert!(
        all.iter().any(|h| h == "# Commandstats"),
        "INFO all must include Commandstats; got {all:?}"
    );
}

#[test]
fn io6_no_duplicate_headers() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    let h = headers(&c.send(&["INFO"]));
    let mut seen = std::collections::HashSet::new();
    let dups: Vec<&String> = h.iter().filter(|x| !seen.insert((*x).clone())).collect();
    assert!(
        dups.is_empty(),
        "every section header must appear at most once — a duplicate makes \
         naive INFO parsers (split on '#', build a map) silently keep only one \
         copy and drop the other's fields. duplicated: {dups:?} in {h:?}"
    );
}

#[test]
fn io7_repeated_section_emitted_once() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    let h = headers(&c.send(&["INFO", "server", "server"]));
    assert_eq!(
        h,
        vec!["# Server"],
        "a repeated section name must not duplicate the section; got {h:?}"
    );
}

#[test]
fn io8_required_fields_present() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    let reply = c.send(&["INFO", "all"]);
    // The fields clients, drivers and monitoring agents actually parse.
    for name in [
        "run_id",
        "redis_mode",
        "cluster_enabled",
        "process_id",
        "os",
        "arch_bits",
        "keyspace_hits",
        "keyspace_misses",
        "expired_keys",
        "evicted_keys",
        "rejected_connections",
        "maxmemory",
        "maxmemory_policy",
        "instantaneous_ops_per_sec",
        "blocked_clients",
        "pubsub_channels",
        "pubsub_patterns",
        "total_net_input_bytes",
        "total_net_output_bytes",
    ] {
        assert!(
            field(&reply, name).is_some(),
            "INFO is missing {name:?} — stock monitoring agents read it and \
             will either KeyError or silently report zero"
        );
    }
}

#[test]
fn io9_run_id_shape_and_restart() {
    let dir = std::env::temp_dir().join(format!("moon-infoobs-runid-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    let first = {
        let m = spawn_moon_in("1", Some(dir.clone()));
        let mut c = Conn::open(m.port);
        let id = field(&c.send(&["INFO", "server"]), "run_id").expect("run_id present");
        assert_eq!(
            id.len(),
            40,
            "run_id is a 40-char hex string in Redis; got {id:?}"
        );
        assert!(
            id.bytes().all(|b| b.is_ascii_hexdigit()),
            "run_id must be hex; got {id:?}"
        );
        id
        // m dropped here -> server killed, and Drop also removes the dir, so
        // the second server below starts clean. That is fine for this test:
        // run_id must be per-PROCESS, and deriving it from dataset state would
        // be wrong regardless of whether the dataset survived.
    };

    // Fresh process: clients use run_id to detect that the server they are
    // talking to is not the one they were talking to.
    let m2 = spawn_moon_in("1", Some(dir.clone()));
    let mut c2 = Conn::open(m2.port);
    let second = field(&c2.send(&["INFO", "server"]), "run_id").expect("run_id present");
    assert_ne!(
        first, second,
        "run_id must differ across a restart — a stable one defeats every \
         client-side restart/failover detection that depends on it"
    );
}

#[test]
fn io10_keyspace_hit_miss_counters() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    c.send(&["SET", "io10key", "v"]);

    let before = c.send(&["INFO", "stats"]);
    let h0: u64 = field(&before, "keyspace_hits")
        .expect("keyspace_hits present")
        .parse()
        .expect("numeric");
    let m0: u64 = field(&before, "keyspace_misses")
        .expect("keyspace_misses present")
        .parse()
        .expect("numeric");

    c.send(&["GET", "io10key"]); // hit
    c.send(&["GET", "io10missing"]); // miss

    let after = c.send(&["INFO", "stats"]);
    let h1: u64 = field(&after, "keyspace_hits").unwrap().parse().unwrap();
    let m1: u64 = field(&after, "keyspace_misses").unwrap().parse().unwrap();

    assert_eq!(
        h1 - h0,
        1,
        "one existing-key GET must move keyspace_hits by exactly 1 \
         (hit-rate dashboards divide by these)"
    );
    assert_eq!(
        m1 - m0,
        1,
        "one missing-key GET must move keyspace_misses by exactly 1"
    );
}

#[test]
fn io11_maxmemory_policy_reflects_config() {
    // A dashboard reads `maxmemory_policy` to decide whether an OOM is an
    // operator choice (`noeviction`) or a bug. Reporting a constant would be
    // worse than omitting the field, so it must track CONFIG SET.
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    // Against CONFIG GET, not a hardcoded name: Moon's memory guardrail
    // auto-caps maxmemory and switches the policy when the operator sets
    // neither, so the startup default is a runtime decision. What must hold is
    // that the two surfaces agree.
    let configured = c.send(&["CONFIG", "GET", "maxmemory-policy"]);
    let configured = configured
        .lines()
        .last()
        .map(|l| l.trim().to_string())
        .expect("CONFIG GET reply");
    assert_eq!(
        field(&c.send(&["INFO", "memory"]), "maxmemory_policy").as_deref(),
        Some(configured.as_str()),
        "INFO and CONFIG GET must name the same policy at startup"
    );

    let flipped = if configured == "noeviction" {
        "allkeys-lru"
    } else {
        "noeviction"
    };
    c.send(&["CONFIG", "SET", "maxmemory-policy", flipped]);
    assert_eq!(
        field(&c.send(&["INFO", "memory"]), "maxmemory_policy").as_deref(),
        Some(flipped),
        "INFO must follow CONFIG SET — a stale policy tells an operator the \
         instance will OOM when it will in fact evict, or vice versa"
    );
}

#[test]
fn io12_blocked_clients_tracks_a_real_block() {
    // `blocked_clients` is how an operator distinguishes "the server is idle"
    // from "every worker is parked on an empty queue". A hardcoded 0 reads as
    // the former while the latter is happening.
    let m = spawn_moon("1");
    let mut observer = Conn::open(m.port);
    assert_eq!(
        field(&observer.send(&["INFO", "clients"]), "blocked_clients").as_deref(),
        Some("0"),
        "no client is blocked yet"
    );

    let mut blocker = Conn::open(m.port);
    blocker.write_only(&["BLPOP", "io12queue", "0"]);
    // The block registers on the shard thread; give it a moment to land.
    let mut seen = None;
    for _ in 0..50 {
        std::thread::sleep(Duration::from_millis(20));
        seen = field(&observer.send(&["INFO", "clients"]), "blocked_clients");
        if seen.as_deref() == Some("1") {
            break;
        }
    }
    assert_eq!(
        seen.as_deref(),
        Some("1"),
        "a client parked in BLPOP must be counted"
    );

    observer.send(&["LPUSH", "io12queue", "v"]);
    let mut after = None;
    for _ in 0..50 {
        std::thread::sleep(Duration::from_millis(20));
        after = field(&observer.send(&["INFO", "clients"]), "blocked_clients");
        if after.as_deref() == Some("0") {
            break;
        }
    }
    assert_eq!(
        after.as_deref(),
        Some("0"),
        "serving the blocked client must decrement the gauge — a counter that \
         only goes up is worse than no counter"
    );
}

#[test]
fn io13_pubsub_counts_are_instance_wide() {
    // Both fields must agree with the PUBSUB command, which scatter-gathers
    // across every shard's registry. A local-only answer under-reports by
    // roughly 1/N and makes a fan-out look broken.
    let m = spawn_moon("4");
    let mut sub = Conn::open(m.port);
    sub.write_only(&["SUBSCRIBE", "io13a", "io13b"]);
    let mut psub = Conn::open(m.port);
    psub.write_only(&["PSUBSCRIBE", "io13.*"]);
    std::thread::sleep(Duration::from_millis(300));

    let mut c = Conn::open(m.port);
    let stats = c.send(&["INFO", "stats"]);
    assert_eq!(
        field(&stats, "pubsub_channels").as_deref(),
        Some("2"),
        "two subscribed channels must be visible instance-wide"
    );
    assert_eq!(
        field(&stats, "pubsub_patterns").as_deref(),
        Some("1"),
        "one subscribed pattern must be visible instance-wide"
    );
}

// ---------------------------------------------------------------------------
// io14..io18 — the ten INFO fields the pinned client manifest reads but Moon
// did not answer. Each is backed by a real source; a field Moon cannot answer
// truthfully is waived in scripts/client-compat/info_fields.txt with a reason,
// not emitted as a constant. See `# Server`/`# Stats` in command/connection.rs.
// ---------------------------------------------------------------------------

/// `tcp_port` is how a client that reached the server via a proxy, a container
/// port map, or a sentinel handoff learns the port to hand to a peer. Reporting
/// the port the connection arrived on would be wrong behind a NAT — this must
/// be the listener's own configured port.
#[test]
fn io14_tcp_port_is_the_configured_listener_port() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    let reply = c.send(&["INFO", "server"]);
    let got = field(&reply, "tcp_port")
        .unwrap_or_else(|| panic!("INFO server has no tcp_port field; got:\n{reply}"));
    assert_eq!(
        got,
        m.port.to_string(),
        "tcp_port must be the port this instance listens on ({}), not {got}",
        m.port
    );
}

/// `uptime_in_seconds` is the field every restart-detector keys on: a drop to
/// near zero is how a dashboard learns the process died. A constant, or a value
/// that never advances, makes a crash-looping server indistinguishable from a
/// healthy one.
#[test]
fn io15_uptime_advances_and_days_agree() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);

    let first = field(&c.send(&["INFO", "server"]), "uptime_in_seconds")
        .unwrap_or_else(|| panic!("INFO server has no uptime_in_seconds"))
        .parse::<u64>()
        .expect("uptime_in_seconds must be an integer");
    assert!(
        first < 60,
        "a just-spawned server reported uptime_in_seconds={first} — the start \
         instant is not being captured at startup"
    );

    std::thread::sleep(Duration::from_millis(1600));
    let reply = c.send(&["INFO", "server"]);
    let second = field(&reply, "uptime_in_seconds")
        .expect("uptime_in_seconds")
        .parse::<u64>()
        .expect("integer");
    assert!(
        second > first,
        "uptime_in_seconds did not advance across 1.6s ({first} -> {second}); \
         a frozen uptime hides a restart from every monitoring agent"
    );

    let days = field(&reply, "uptime_in_days")
        .expect("uptime_in_days")
        .parse::<u64>()
        .expect("integer");
    assert_eq!(
        days,
        second / 86_400,
        "uptime_in_days must be uptime_in_seconds/86400, not an independent counter"
    );
}

/// `used_memory_lua` tells an operator whether a runaway script is holding
/// memory. Moon initialises the Lua VM lazily per shard, so zero before the
/// first EVAL is the truth — but it must become non-zero once a script has
/// actually run, or the field is a constant wearing a counter's name.
#[test]
fn io16_used_memory_lua_reflects_a_real_vm() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);

    let before = field(&c.send(&["INFO", "memory"]), "used_memory_lua")
        .unwrap_or_else(|| panic!("INFO memory has no used_memory_lua"))
        .parse::<u64>()
        .expect("used_memory_lua must be an integer");
    assert_eq!(
        before, 0,
        "no script has run, so the Lua VM does not exist yet; reporting \
         {before} means the field is not reading a real VM"
    );

    let ev = c.send(&["EVAL", "return 1", "0"]);
    assert!(
        ev.starts_with(":1"),
        "EVAL did not run, so this test proves nothing about the VM: {ev}"
    );

    let after = field(&c.send(&["INFO", "memory"]), "used_memory_lua")
        .expect("used_memory_lua")
        .parse::<u64>()
        .expect("integer");
    assert!(
        after > 0,
        "a Lua VM has been created and a script executed, but used_memory_lua \
         is still 0 — the field is hardcoded, not sampled from mlua"
    );
}

/// The two AOF status fields are the ones an operator reads after a disk
/// incident. Redis reports `ok`/`err`; anything else breaks the parse in
/// stock tooling. With AOF off they must still report a defined status.
#[test]
fn io17_aof_status_fields_are_ok_or_err() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    let reply = c.send(&["INFO", "persistence"]);

    for name in ["aof_last_write_status", "aof_last_bgrewrite_status"] {
        let got = field(&reply, name)
            .unwrap_or_else(|| panic!("INFO persistence has no {name}; got:\n{reply}"));
        assert!(
            got == "ok" || got == "err",
            "{name} must be `ok` or `err` (Redis parity — tooling string-matches \
             these), got {got:?}"
        );
    }
}

/// `rdb_changes_since_last_save` is the "is a save worth doing" signal. It must
/// rise with writes and reset when a save completes; a field pinned at 0 tells
/// a backup script there is nothing to persist.
#[test]
fn io18_rdb_changes_tracks_writes_and_resets_on_save() {
    let m = spawn_moon_persistent("1");
    let mut c = Conn::open(m.port);

    let base = field(
        &c.send(&["INFO", "persistence"]),
        "rdb_changes_since_last_save",
    )
    .unwrap_or_else(|| panic!("INFO persistence has no rdb_changes_since_last_save"))
    .parse::<u64>()
    .expect("integer");

    for i in 0..25 {
        c.send(&["SET", &format!("io18:{i}"), "v"]);
    }
    let after_writes = field(
        &c.send(&["INFO", "persistence"]),
        "rdb_changes_since_last_save",
    )
    .expect("field")
    .parse::<u64>()
    .expect("integer");
    assert!(
        after_writes >= base + 25,
        "25 SETs advanced rdb_changes_since_last_save by {} (expected >= 25) — \
         the counter is not fed by the write path",
        after_writes - base
    );

    // BGSAVE, not SAVE: SAVE is refused in sharded mode, and Moon spawns
    // every instance sharded. BGSAVE returns before the save finishes, so the
    // reset must be observed by polling `rdb_bgsave_in_progress` rather than
    // read immediately — reading too early would pass for the wrong reason
    // (the counter simply had not been reset yet).
    let saved = c.send(&["BGSAVE"]);
    assert!(
        saved.starts_with('+'),
        "BGSAVE failed, test proves nothing: {saved}"
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut after_save = after_writes;
    while Instant::now() < deadline {
        let reply = c.send(&["INFO", "persistence"]);
        let in_progress = field(&reply, "rdb_bgsave_in_progress").unwrap_or_default();
        after_save = field(&reply, "rdb_changes_since_last_save")
            .expect("field")
            .parse::<u64>()
            .expect("integer");
        if in_progress == "0" && after_save < after_writes {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        after_save < after_writes,
        "a completed SAVE did not reset rdb_changes_since_last_save \
         ({after_writes} -> {after_save}); the field never returns to a \
         'nothing to persist' state"
    );
}

/// The three `sync_*` counters are how an operator sees replicas thrashing:
/// a climbing `sync_full` means partial resync keeps failing. On a standalone
/// master with no replicas they must be present and zero — present, because a
/// missing field breaks the scrape; zero, because nothing has synced.
#[test]
fn io19_sync_counters_present_and_zero_without_replicas() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    let reply = c.send(&["INFO", "stats"]);
    for name in ["sync_full", "sync_partial_ok", "sync_partial_err"] {
        let got = field(&reply, name)
            .unwrap_or_else(|| panic!("INFO stats has no {name}; got:\n{reply}"))
            .parse::<u64>()
            .unwrap_or_else(|_| panic!("{name} must be an integer"));
        assert_eq!(
            got, 0,
            "{name} is {got} on an instance no replica ever contacted"
        );
    }
}
