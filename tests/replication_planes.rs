//! Wave A plane-replication RED suite (task #34, `.planning/rfcs/plane-replication-design.md`).
//!
//! Today (before Wave A) three classes of master-side key removal reach
//! NEITHER the AOF plane NOR the replication plane:
//!
//! 1. Active-expiry DELs (background TTL sweep).
//! 2. Eviction plain-drops (`--maxmemory` + an evicting policy).
//! 3. Lua `redis.call` write *effects* (EVAL/EVALSHA carry no WRITE
//!    command-metadata flag, so the generic AOF/replication gate never
//!    sees them; the script's own writes vanish on restart AND never
//!    reach an attached replica).
//!
//! `redis.call('SELECT', ...)` inside a script also silently corrupts the
//! wrong db today instead of erroring loudly.
//!
//! Six black-box scenarios pin these gaps down. Run:
//!
//! ```text
//! MOON_BIN=./target/release/moon \
//!   cargo test --test replication_planes -- --ignored --nocapture
//! ```
//!
//! Server dirs use `tempfile::tempdir()` (respects `$TMPDIR`, which on this
//! host resolves to the boot volume with ample free space) PLUS
//! `--disk-free-min-pct 0` on every spawn, matching
//! `tests/replication_multishard.rs` — belt and suspenders against the
//! diskfull guard (`/Volumes/Games` itself hovers near the 5% floor).

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

fn moon_bin() -> String {
    std::env::var("MOON_BIN").unwrap_or_else(|_| "./target/release/moon".to_string())
}

/// Spawn moon with explicit `--shards` plus arbitrary extra CLI args (mirrors
/// `tests/replication_hardening.rs::start_moon`'s extra-args signature).
/// Always includes `--disk-free-min-pct 0` (repo harness rule) unless the
/// caller already passed that flag.
fn start_moon(port: u16, dir: &str, shards: usize, extra: &[&str]) -> Child {
    let port_s = port.to_string();
    let shards_s = shards.to_string();
    let mut full: Vec<&str> = vec![
        "--port",
        &port_s,
        "--shards",
        &shards_s,
        "--dir",
        dir,
        "--disk-free-min-pct",
        "0",
    ];
    full.extend_from_slice(extra);
    Command::new(moon_bin())
        .args(&full)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to start moon (set MOON_BIN to a built binary)")
}

/// Send one inline command and return the raw reply text (bulk bodies
/// inlined; `$-1` nil yields an empty string).
fn send_cmd(addr: &str, cmd: &str) -> String {
    let Ok(mut stream) = TcpStream::connect(addr) else {
        return String::new();
    };
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    stream
        .write_all(format!("{}\r\n", cmd).as_bytes())
        .expect("write");
    stream.flush().ok();
    let mut reader = BufReader::new(&stream);
    read_one_reply(&mut reader)
}

fn read_one_reply<R: BufRead>(reader: &mut R) -> String {
    let mut out = String::new();
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) => {
                let trimmed = line.trim_end_matches("\r\n").trim_end_matches('\n');
                if trimmed.starts_with('+') || trimmed.starts_with('-') || trimmed.starts_with(':')
                {
                    out.push_str(trimmed);
                    break;
                }
                if let Some(rest) = trimmed.strip_prefix('$') {
                    let len: i64 = rest.trim().parse().unwrap_or(-1);
                    if len < 0 {
                        break;
                    }
                    let mut buf = vec![0u8; (len as usize) + 2];
                    if reader.read_exact(&mut buf).is_ok() {
                        out.push_str(&String::from_utf8_lossy(&buf[..len as usize]));
                    }
                    break;
                }
                // Ignore array/other headers — not needed by these helpers.
            }
        }
    }
    out
}

/// Send a RESP-array command (needed for args that contain spaces/binary,
/// e.g. Lua script bodies) and return the raw wire reply as text.
fn send_resp(addr: &str, parts: &[&str]) -> String {
    let Ok(mut stream) = TcpStream::connect(addr) else {
        return String::new();
    };
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .ok();
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    if stream.write_all(&out).is_err() {
        return String::new();
    }
    stream.flush().ok();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let deadline = std::time::Instant::now() + Duration::from_millis(600);
    while std::time::Instant::now() < deadline {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(_) => {
                if !buf.is_empty() {
                    break;
                }
            }
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

/// Run a burst of pipelined inline commands on one fresh connection and wait
/// for all replies (order-preserving, no interleave risk).
fn pipeline(addr: &str, cmds: &[String]) {
    let Ok(mut stream) = TcpStream::connect(addr) else {
        panic!("pipeline: connect failed to {addr}");
    };
    stream.set_read_timeout(Some(Duration::from_secs(20))).ok();
    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
    let mut buf = String::new();
    for c in cmds {
        buf.push_str(c);
        buf.push_str("\r\n");
    }
    stream.write_all(buf.as_bytes()).expect("write");
    stream.flush().ok();
    for _ in cmds {
        read_one_reply(&mut reader);
    }
}

fn dbsize(addr: &str) -> i64 {
    send_cmd(addr, "DBSIZE")
        .strip_prefix(':')
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(-1)
}

/// Pull the body out of a raw `$<len>\r\n<body>\r\n` bulk-string wire reply
/// (as returned by `send_resp`). Falls back to a trimmed copy of the input
/// for non-bulk replies.
fn bulk_body(raw: &str) -> String {
    if let Some(rest) = raw.strip_prefix('$')
        && let Some(nl) = rest.find("\r\n")
        && let Ok(len) = rest[..nl].trim().parse::<usize>()
        && let Some(body) = rest.get(nl + 2..nl + 2 + len)
    {
        return body.to_string();
    }
    raw.trim().to_string()
}

fn master_repl_offset(addr: &str) -> i64 {
    let info = send_cmd(addr, "INFO replication");
    info.lines()
        .find_map(|l| l.strip_prefix("master_repl_offset:"))
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(-1)
}

fn wait_until<F: Fn() -> bool>(timeout: Duration, f: F) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if f() {
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    false
}

fn await_ready(addr: &str) {
    assert!(
        wait_until(Duration::from_secs(15), || send_cmd(addr, "PING")
            .starts_with("+PONG")),
        "server at {} did not become ready",
        addr
    );
}

fn await_link_up(replica_addr: &str) {
    assert!(
        wait_until(Duration::from_secs(15), || send_cmd(
            replica_addr,
            "INFO replication"
        )
        .contains("master_link_status:up")),
        "replica {} link did not come up",
        replica_addr
    );
}

struct Guard(Vec<Child>);
impl Drop for Guard {
    fn drop(&mut self) {
        for c in &mut self.0 {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

/// SAFETY: `pid` is a live child PID we spawned ourselves; SIGKILL is always
/// a valid signal to send. Mirrors `tests/replication_hardening.rs`.
fn sigkill(child: &mut Child) {
    // SAFETY: see doc comment above.
    let ret = unsafe { libc::kill(child.id() as i32, libc::SIGKILL) };
    assert_eq!(ret, 0, "libc::kill failed");
    let _ = child.wait();
}

// ============================================================================
// Scenario 1: eviction_parity — plain-dropped (evicted) keys on the master
// must also disappear on the replica. Today `record_reason_del` doesn't
// exist, so eviction plain-drops never reach the replication plane: the
// replica keeps every key the master ever sent it, even ones the master
// itself has since evicted.
// ============================================================================

fn run_eviction_parity(shards: usize, master_port: u16, replica_port: u16) {
    let mdir = tempfile::tempdir().expect("mdir");
    let rdir = tempfile::tempdir().expect("rdir");
    // Small whole-instance cap + allkeys-lru: writes past the cap force
    // plain-drop eviction of older keys. `--disk-offload disable` per the
    // Wave A eviction-emission scope (plain-drops only, no spill/cold tier).
    const MAXMEMORY: &str = "262144"; // 256KB
    let master = start_moon(
        master_port,
        mdir.path().to_str().unwrap(),
        shards,
        &[
            "--maxmemory",
            MAXMEMORY,
            "--maxmemory-policy",
            "allkeys-lru",
            "--disk-offload",
            "disable",
            "--appendonly",
            "no",
        ],
    );
    let replica = start_moon(
        replica_port,
        rdir.path().to_str().unwrap(),
        1,
        &["--appendonly", "no"],
    );
    let mut guard = Guard(vec![master, replica]);
    let m = format!("127.0.0.1:{}", master_port);
    let r = format!("127.0.0.1:{}", replica_port);
    await_ready(&m);
    await_ready(&r);

    // Attach the replica FIRST (before the eviction-triggering writes), so
    // every write (and every eviction, once implemented) is observed live.
    assert!(send_cmd(&r, &format!("REPLICAOF 127.0.0.1 {}", master_port)).starts_with("+OK"));
    await_link_up(&r);

    // ~4x the cap in raw value bytes (200B values), comfortably past 256KB
    // so eviction is guaranteed to fire repeatedly, not just once.
    const N: usize = 5000;
    let value = "v".repeat(200);
    for burst in 0..(N / 100) {
        let cmds: Vec<String> = (0..100)
            .map(|i| format!("SET ev:{} {}", burst * 100 + i, value))
            .collect();
        pipeline(&m, &cmds);
    }

    // Let the master settle (writes done, no more evictions in flight).
    thread::sleep(Duration::from_millis(500));
    let master_final = dbsize(&m);
    assert!(
        master_final < N as i64,
        "setup invariant broken: master did not evict anything (dbsize={}, wrote {}) — \
         raise write volume or lower --maxmemory",
        master_final,
        N
    );

    // FEATURE ASSERTION: replica must converge to the SAME (evicted) dbsize,
    // not the full write count.
    assert!(
        wait_until(Duration::from_secs(10), || dbsize(&r) == master_final),
        "replica did not converge to master's evicted dbsize: master={} replica={} \
         (wrote {} keys) — eviction plain-drops are not propagated to the replica",
        master_final,
        dbsize(&r),
        N
    );

    // Spot-check: any key missing on the master (evicted) must also be
    // missing on the replica.
    let mut master_missing = Vec::new();
    for i in 0..N {
        if send_cmd(&m, &format!("GET ev:{}", i)).is_empty() {
            master_missing.push(i);
            if master_missing.len() >= 20 {
                break;
            }
        }
    }
    assert!(
        !master_missing.is_empty(),
        "setup invariant broken: could not find any evicted key on the master"
    );
    for i in &master_missing {
        assert!(
            send_cmd(&r, &format!("GET ev:{}", i)).is_empty(),
            "key ev:{} was evicted on master but still present on replica",
            i
        );
    }

    guard.0.clear();
}

#[test]
#[ignore]
fn eviction_parity_shards1() {
    run_eviction_parity(1, 17301, 17302);
}

#[test]
#[ignore]
fn eviction_parity_shards4() {
    run_eviction_parity(4, 17311, 17312);
}

// ============================================================================
// Scenario 2: eval_effects_parity — a Lua script's `redis.call` write
// effects (SET x3 + INCR x5 on distinct keys) must reach an attached
// replica, for both EVAL and EVALSHA. Today EVAL/EVALSHA carry no WRITE
// command-metadata flag, so the generic replication gate never sees them —
// the replica never observes the script's writes at all.
// ============================================================================

/// EVAL/EVALSHA do NOT cross-shard forward (`src/server/conn/handler_monoio/
/// dispatch.rs::try_handle_eval` always runs on `ctx.shard_id`, the shard
/// that accepted the TCP connection): `validate_keys_same_shard` requires
/// every key to hash to THAT shard specifically, not merely to agree with
/// each other. A single connection is pinned to whichever shard SO_REUSEPORT
/// handed it, so a hardcoded hash tag has only a `1/num_shards` chance of
/// matching. Probe on ONE fixed connection (so the shard binding never
/// changes mid-probe) by trying tag suffixes until the real EVAL succeeds —
/// the first non-CROSSSLOT reply IS the real call, so this costs nothing
/// extra on the (common) shards=1 case.
fn eval_find_shard_and_run(
    addr: &str,
    script: &str,
    tag_prefix: &str,
    key_suffixes: &[&str],
    argv: &[&str],
) -> (String, String) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
    for attempt in 0..64 {
        let tag = format!("{}{}", tag_prefix, attempt);
        let keys: Vec<String> = key_suffixes
            .iter()
            .map(|s| format!("{{{}}}:{}", tag, s))
            .collect();
        let mut parts: Vec<&str> = vec!["EVAL", script];
        let numkeys = keys.len().to_string();
        parts.push(&numkeys);
        for k in &keys {
            parts.push(k);
        }
        for a in argv {
            parts.push(a);
        }
        let mut out = format!("*{}\r\n", parts.len()).into_bytes();
        for p in &parts {
            out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            out.extend_from_slice(p.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        stream.write_all(&out).expect("write");
        stream.flush().ok();
        let reply = read_one_reply(&mut reader);
        if !reply.contains("CROSSSLOT") {
            return (tag, reply);
        }
    }
    panic!(
        "eval_find_shard_and_run: no hash tag among 64 tried landed on this connection's shard \
         (tag_prefix={})",
        tag_prefix
    );
}

/// Same shard-matching probe, for EVALSHA (script must already be loaded on
/// every shard — `SCRIPT LOAD` fans out, see `try_handle_script`).
fn evalsha_find_shard_and_run(
    addr: &str,
    sha: &str,
    tag_prefix: &str,
    key_suffixes: &[&str],
    argv: &[&str],
) -> (String, String) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
    for attempt in 0..64 {
        let tag = format!("{}{}", tag_prefix, attempt);
        let keys: Vec<String> = key_suffixes
            .iter()
            .map(|s| format!("{{{}}}:{}", tag, s))
            .collect();
        let mut parts: Vec<&str> = vec!["EVALSHA", sha];
        let numkeys = keys.len().to_string();
        parts.push(&numkeys);
        for k in &keys {
            parts.push(k);
        }
        for a in argv {
            parts.push(a);
        }
        let mut out = format!("*{}\r\n", parts.len()).into_bytes();
        for p in &parts {
            out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            out.extend_from_slice(p.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        stream.write_all(&out).expect("write");
        stream.flush().ok();
        let reply = read_one_reply(&mut reader);
        if !reply.contains("CROSSSLOT") {
            return (tag, reply);
        }
    }
    panic!(
        "evalsha_find_shard_and_run: no hash tag among 64 tried landed on this connection's \
         shard (tag_prefix={})",
        tag_prefix
    );
}

fn run_eval_effects_parity(shards: usize, master_port: u16, replica_port: u16) {
    let mdir = tempfile::tempdir().expect("mdir");
    let rdir = tempfile::tempdir().expect("rdir");
    let master = start_moon(
        master_port,
        mdir.path().to_str().unwrap(),
        shards,
        &["--appendonly", "no"],
    );
    let replica = start_moon(
        replica_port,
        rdir.path().to_str().unwrap(),
        1,
        &["--appendonly", "no"],
    );
    let mut guard = Guard(vec![master, replica]);
    let m = format!("127.0.0.1:{}", master_port);
    let r = format!("127.0.0.1:{}", replica_port);
    await_ready(&m);
    await_ready(&r);
    assert!(send_cmd(&r, &format!("REPLICAOF 127.0.0.1 {}", master_port)).starts_with("+OK"));
    await_link_up(&r);

    // EVAL variant: 3 SETs + 5 INCRs, 8 distinct keys, single db (db0).
    let eval_script = "redis.call('SET', KEYS[1], ARGV[1]) \
                        redis.call('SET', KEYS[2], ARGV[2]) \
                        redis.call('SET', KEYS[3], ARGV[3]) \
                        redis.call('INCR', KEYS[4]) \
                        redis.call('INCR', KEYS[5]) \
                        redis.call('INCR', KEYS[6]) \
                        redis.call('INCR', KEYS[7]) \
                        redis.call('INCR', KEYS[8]) \
                        return 'OK'";
    // Hash-tagged keys: on a multi-shard master a script's KEYS must all
    // route to ONE shard (cross-shard scripts are rejected with CROSSSLOT).
    // `eval_find_shard_and_run` probes tag suffixes on ONE fixed connection
    // until it lands on that connection's own shard.
    let key_suffixes = ["s1", "s2", "s3", "i1", "i2", "i3", "i4", "i5"];
    let (evg, reply) =
        eval_find_shard_and_run(&m, eval_script, "evg", &key_suffixes, &["va", "vb", "vc"]);
    assert!(
        reply.contains("OK"),
        "setup: EVAL should succeed, got: {}",
        reply
    );
    let evg_set_keys: Vec<String> = ["s1", "s2", "s3"]
        .iter()
        .map(|s| format!("{{{}}}:{}", evg, s))
        .collect();
    let evg_incr_keys: Vec<String> = ["i1", "i2", "i3", "i4", "i5"]
        .iter()
        .map(|s| format!("{{{}}}:{}", evg, s))
        .collect();
    for k in &evg_set_keys {
        assert!(
            !send_cmd(&m, &format!("GET {}", k)).is_empty(),
            "setup invariant broken: master missing {} after EVAL",
            k
        );
    }
    for k in &evg_incr_keys {
        assert_eq!(
            send_cmd(&m, &format!("GET {}", k)),
            "1",
            "setup invariant broken: master {} should be 1 after EVAL INCR",
            k
        );
    }

    // EVALSHA variant: SCRIPT LOAD then EVALSHA, distinct hash-tagged key
    // group so the two sub-scenarios can't mask each other. SCRIPT LOAD fans
    // the sha out to every shard (`try_handle_script`), so it can be issued
    // on any connection.
    let sha_reply = send_resp(&m, &["SCRIPT", "LOAD", eval_script]);
    let sha = bulk_body(&sha_reply);
    assert_eq!(
        sha.len(),
        40,
        "setup: SCRIPT LOAD should return a 40-char sha1, got: {}",
        sha_reply
    );
    let (eshg, reply) =
        evalsha_find_shard_and_run(&m, &sha, "eshg", &key_suffixes, &["wa", "wb", "wc"]);
    assert!(
        reply.contains("OK"),
        "setup: EVALSHA should succeed, got: {}",
        reply
    );
    let eshg_set_keys: Vec<String> = ["s1", "s2", "s3"]
        .iter()
        .map(|s| format!("{{{}}}:{}", eshg, s))
        .collect();
    let eshg_incr_keys: Vec<String> = ["i1", "i2", "i3", "i4", "i5"]
        .iter()
        .map(|s| format!("{{{}}}:{}", eshg, s))
        .collect();

    // FEATURE ASSERTION: replica must converge on every key from BOTH
    // sub-scenarios.
    let all_set_keys: Vec<String> = evg_set_keys
        .iter()
        .chain(eshg_set_keys.iter())
        .cloned()
        .collect();
    let all_set_vals = ["va", "vb", "vc", "wa", "wb", "wc"];
    let all_incr_keys: Vec<String> = evg_incr_keys
        .iter()
        .chain(eshg_incr_keys.iter())
        .cloned()
        .collect();

    let converged = wait_until(Duration::from_secs(10), || {
        all_set_keys
            .iter()
            .zip(all_set_vals.iter())
            .all(|(k, v)| send_cmd(&r, &format!("GET {}", k)) == *v)
            && all_incr_keys
                .iter()
                .all(|k| send_cmd(&r, &format!("GET {}", k)) == "1")
    });
    if !converged {
        let missing_sets: Vec<_> = all_set_keys
            .iter()
            .filter(|k| send_cmd(&r, &format!("GET {}", k)).is_empty())
            .collect();
        let missing_incrs: Vec<_> = all_incr_keys
            .iter()
            .filter(|k| send_cmd(&r, &format!("GET {}", k)).is_empty())
            .collect();
        panic!(
            "replica never observed the Lua script's write effects — \
             missing SET keys: {:?}, missing INCR keys: {:?} \
             (EVAL/EVALSHA writes are not propagated to the replica)",
            missing_sets, missing_incrs
        );
    }

    guard.0.clear();
}

#[test]
#[ignore]
fn eval_effects_parity_shards1() {
    run_eval_effects_parity(1, 17321, 17322);
}

#[test]
#[ignore]
fn eval_effects_parity_shards4() {
    run_eval_effects_parity(4, 17331, 17332);
}

// ============================================================================
// Scenario 3: eval_writes_survive_restart — standalone durability bug. No
// replica involved: a Lua script's writes must survive a kill -9 + restart
// against the SAME `--appendonly yes` dir. Today the AOF gate never sees
// EVAL/EVALSHA's inner writes either (same missing-WRITE-flag root cause as
// scenario 2), so they are lost on restart.
// ============================================================================

#[test]
#[ignore]
fn eval_writes_survive_restart() {
    let (port,) = (17341,);
    let dir = tempfile::tempdir().expect("dir");
    let mut master = start_moon(
        port,
        dir.path().to_str().unwrap(),
        1,
        &["--appendonly", "yes"],
    );
    let m = format!("127.0.0.1:{}", port);
    await_ready(&m);

    let script = "redis.call('SET', KEYS[1], ARGV[1]) \
                   redis.call('SET', KEYS[2], ARGV[2]) \
                   redis.call('SET', KEYS[3], ARGV[3]) \
                   return 'OK'";
    let reply = send_resp(
        &m,
        &[
            "EVAL", script, "3", "surv:a", "surv:b", "surv:c", "va", "vb", "vc",
        ],
    );
    assert!(
        reply.contains("OK"),
        "setup: EVAL should succeed, got: {}",
        reply
    );
    for (k, v) in [("surv:a", "va"), ("surv:b", "vb"), ("surv:c", "vc")] {
        assert_eq!(
            send_cmd(&m, &format!("GET {}", k)),
            v,
            "setup invariant broken: master missing {} before kill",
            k
        );
    }

    // Give the AOF writer a moment to flush/fsync before the kill (WAL/AOF
    // group-commit cadence; not testing the fsync timing itself here).
    thread::sleep(Duration::from_millis(500));
    sigkill(&mut master);

    let master2 = start_moon(
        port,
        dir.path().to_str().unwrap(),
        1,
        &["--appendonly", "yes"],
    );
    let _guard = Guard(vec![master2]);
    await_ready(&m);

    // FEATURE ASSERTION: EVAL-written keys must exist after restart.
    for (k, v) in [("surv:a", "va"), ("surv:b", "vb"), ("surv:c", "vc")] {
        let got = send_cmd(&m, &format!("GET {}", k));
        assert_eq!(
            got, v,
            "key {} lost across kill-9 + restart (EVAL write did not reach the AOF plane): \
             got {:?}, want {:?}",
            k, got, v
        );
    }
}

// ============================================================================
// Scenario 4: evicted_keys_stay_dead_after_restart — an evicted key must
// stay evicted after a kill -9 + restart against the SAME `--appendonly yes`
// dir. Today eviction plain-drops never reach the AOF plane either, so the
// AOF replay resurrects every key the eviction sweep ever dropped.
// ============================================================================

#[test]
#[ignore]
fn evicted_keys_stay_dead_after_restart() {
    let (port,) = (17351,);
    let dir = tempfile::tempdir().expect("dir");
    const MAXMEMORY: &str = "262144"; // 256KB
    let mut master = start_moon(
        port,
        dir.path().to_str().unwrap(),
        1,
        &[
            "--appendonly",
            "yes",
            "--maxmemory",
            MAXMEMORY,
            "--maxmemory-policy",
            "allkeys-lru",
            "--disk-offload",
            "disable",
        ],
    );
    let m = format!("127.0.0.1:{}", port);
    await_ready(&m);

    const N: usize = 5000;
    let value = "v".repeat(200);
    for burst in 0..(N / 100) {
        let cmds: Vec<String> = (0..100)
            .map(|i| format!("SET ek:{} {}", burst * 100 + i, value))
            .collect();
        pipeline(&m, &cmds);
    }
    thread::sleep(Duration::from_millis(500));

    // Find a confirmed-evicted key (allkeys-lru evicts oldest-touched first,
    // so the earliest-written keys are the most likely victims — scan for
    // one to avoid hardcoding LRU internals).
    let evicted_key = (0..N).find(|i| send_cmd(&m, &format!("GET ek:{}", i)).is_empty());
    let Some(evicted_key) = evicted_key else {
        panic!(
            "setup invariant broken: master did not evict anything (raise write volume or lower --maxmemory)"
        );
    };
    let pre_kill_dbsize = dbsize(&m);
    assert!(
        pre_kill_dbsize < N as i64,
        "setup invariant broken: dbsize {} should be less than {} written keys",
        pre_kill_dbsize,
        N
    );

    thread::sleep(Duration::from_millis(500));
    sigkill(&mut master);

    let master2 = start_moon(
        port,
        dir.path().to_str().unwrap(),
        1,
        &[
            "--appendonly",
            "yes",
            "--maxmemory",
            MAXMEMORY,
            "--maxmemory-policy",
            "allkeys-lru",
            "--disk-offload",
            "disable",
        ],
    );
    let _guard = Guard(vec![master2]);
    await_ready(&m);

    // FEATURE ASSERTION: the evicted key must still be gone.
    let got = send_cmd(&m, &format!("GET ek:{}", evicted_key));
    assert!(
        got.is_empty(),
        "evicted key ek:{} resurrected after kill-9 + restart (got {:?}) — \
         eviction plain-drops are not propagated to the AOF plane",
        evicted_key,
        got
    );

    // FEATURE ASSERTION: dbsize must not balloon back up past a small slack
    // (a handful of keys written in the final unflushed AOF tail before the
    // kill are tolerable; a full resurrection of all evicted keys is not).
    let post_restart_dbsize = dbsize(&m);
    const SLACK: i64 = 20;
    assert!(
        post_restart_dbsize <= pre_kill_dbsize + SLACK,
        "dbsize ballooned after restart: pre-kill={} post-restart={} (slack={}) — \
         evicted keys resurrected from AOF replay",
        pre_kill_dbsize,
        post_restart_dbsize,
        SLACK
    );
}

// ============================================================================
// Scenario 5: expiry_del_propagates — active-expiry DELs must be visible on
// the replication plane (proven via master_repl_offset advancing after the
// SETs settle), not just via the replica's own independent TTL sweep
// silently masking the gap (both sides end up empty either way — that alone
// does NOT prove the DEL was replicated).
// ============================================================================

#[test]
#[ignore]
fn expiry_del_propagates() {
    let (master_port, replica_port) = (17361, 17362);
    let mdir = tempfile::tempdir().expect("mdir");
    let rdir = tempfile::tempdir().expect("rdir");
    let master = start_moon(
        master_port,
        mdir.path().to_str().unwrap(),
        1,
        &["--appendonly", "no"],
    );
    let replica = start_moon(
        replica_port,
        rdir.path().to_str().unwrap(),
        1,
        &["--appendonly", "no"],
    );
    let mut guard = Guard(vec![master, replica]);
    let m = format!("127.0.0.1:{}", master_port);
    let r = format!("127.0.0.1:{}", replica_port);
    await_ready(&m);
    await_ready(&r);
    assert!(send_cmd(&r, &format!("REPLICAOF 127.0.0.1 {}", master_port)).starts_with("+OK"));
    await_link_up(&r);

    const N: usize = 50;
    let cmds: Vec<String> = (0..N).map(|i| format!("SET xk:{} v PX 200", i)).collect();
    pipeline(&m, &cmds);
    assert!(
        wait_until(Duration::from_secs(5), || dbsize(&r) == N as i64),
        "setup invariant broken: replica did not receive the {} SETs before expiry",
        N
    );
    assert_eq!(dbsize(&m), N as i64, "setup: master dbsize after SETs");

    // Baseline offset AFTER the SETs have settled (excludes SET/PEXPIRE
    // writes themselves — only the subsequent active-expiry DELs, if any,
    // should move the needle from here).
    let baseline_offset = master_repl_offset(&m);

    // Let the 200ms TTLs expire (master active-expiry sweep runs at 100ms
    // cadence, comfortably inside this 2s window either way).
    thread::sleep(Duration::from_secs(2));

    assert!(
        wait_until(Duration::from_secs(5), || dbsize(&m) == 0),
        "master did not actively expire all keys: dbsize={}",
        dbsize(&m)
    );
    assert!(
        wait_until(Duration::from_secs(5), || dbsize(&r) == 0),
        "replica did not converge to 0 keys (either via replicated DELs or its own \
         independent expiry): dbsize={}",
        dbsize(&r)
    );

    // FEATURE ASSERTION: the offset must have moved — proof that the master
    // actually EMITTED replication records for the active-expiry DELs,
    // rather than relying entirely on the replica's own independent sweep
    // (which would leave both sides at dbsize 0 while the offset stayed
    // frozen at `baseline_offset`).
    let after_offset = master_repl_offset(&m);
    assert!(
        after_offset > baseline_offset,
        "master_repl_offset did not advance after active expiry (baseline={}, after={}) — \
         active-expiry DELs are not emitted to the replication plane \
         (both sides reaching dbsize 0 is being masked by the replica's own independent TTL sweep)",
        baseline_offset,
        after_offset
    );

    guard.0.clear();
}

// ============================================================================
// Scenario 6: select_in_script_errors — `redis.call('SELECT', ...)` inside a
// script must return a loud error mentioning SELECT, and no write must land
// anywhere. Today SELECT inside a script silently succeeds and the
// subsequent write completes (landing in the ORIGINAL db regardless of the
// SELECT target — a silent cross-db bug that Wave A converts into a loud
// script error instead).
// ============================================================================

#[test]
#[ignore]
fn select_in_script_errors() {
    let (port,) = (17371,);
    let dir = tempfile::tempdir().expect("dir");
    let master = start_moon(
        port,
        dir.path().to_str().unwrap(),
        1,
        &["--appendonly", "no"],
    );
    let _guard = Guard(vec![master]);
    let m = format!("127.0.0.1:{}", port);
    await_ready(&m);

    let script = "redis.call('SELECT', '1') \
                   redis.call('SET', KEYS[1], ARGV[1]) \
                   return 'done'";
    let reply = send_resp(&m, &["EVAL", script, "1", "sel:key", "sel:val"]);

    // FEATURE ASSERTION: must be a RESP error mentioning SELECT.
    assert!(
        reply.starts_with('-') && reply.to_uppercase().contains("SELECT"),
        "EVAL with redis.call('SELECT', ...) should return a loud error mentioning SELECT, \
         got: {:?}",
        reply
    );

    // FEATURE ASSERTION: the SET must never have landed anywhere — the
    // script should abort AT the SELECT call, before the SET ever runs.
    let in_db0 = send_cmd(&m, "GET sel:key");
    assert!(
        in_db0.is_empty(),
        "SELECT-then-SET script should abort before the SET executes, but sel:key exists \
         in db0: {:?}",
        in_db0
    );
    let mut stream = TcpStream::connect(&m).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
    stream.write_all(b"SELECT 1\r\n").unwrap();
    read_one_reply(&mut reader);
    stream.write_all(b"GET sel:key\r\n").unwrap();
    let db1_val = read_one_reply(&mut reader);
    assert!(
        db1_val.is_empty(),
        "sel:key should not exist in db1 either: {:?}",
        db1_val
    );
}
