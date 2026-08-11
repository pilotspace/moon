//! R2 acceptance: MULTI-SHARD master PSYNC (task #20, RFC 1B).
//!
//! A master running `--shards N` (N > 1) must serve a full resync to a
//! single-shard replica: one merged Redis-format RDB snapshot followed by the
//! merged live command stream from all N shards. Before R2 the master answered
//! `-ERR PSYNC across multiple shards is not yet supported`.
//!
//! Black-box tests over real `moon` processes; `#[ignore]`d like the other
//! replication suites:
//!
//! ```text
//! MOON_BIN=./target/release/moon \
//!   cargo test --test replication_multishard -- --ignored --nocapture
//! ```

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

fn moon_bin() -> std::path::PathBuf {
    common::find_moon_binary()
}

fn start_moon_shards(port: u16, dir: &str, shards: usize) -> Child {
    Command::new(moon_bin())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            &shards.to_string(),
            "--dir",
            dir,
            "--appendonly",
            "no",
            // /Volumes/Games hovers near the 5% diskfull guard; disable it so a
            // low-free-space dev host does not turn writes into MOONERR diskfull.
            "--disk-free-min-pct",
            "0",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to start moon (set MOON_BIN to a built binary)")
}

/// Send one inline command and return the raw reply (one logical RESP reply).
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

/// Read exactly one RESP reply from `reader`, returned as text (bulk bodies
/// inline; `$-1` nil yields an empty string).
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
                // Ignore array/other headers for these simple sequences.
            }
        }
    }
    out
}

/// Run a sequence of commands on ONE connection (so `SELECT` persists) and
/// return the LAST reply as text.
fn send_seq(addr: &str, cmds: &[&str]) -> String {
    let Ok(mut stream) = TcpStream::connect(addr) else {
        return String::new();
    };
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
    let mut last = String::new();
    for cmd in cmds {
        stream
            .write_all(format!("{}\r\n", cmd).as_bytes())
            .expect("write");
        stream.flush().ok();
        last = read_one_reply(&mut reader);
    }
    last
}

fn get_in_db(addr: &str, db: usize, key: &str) -> Option<String> {
    let sel = format!("SELECT {}", db);
    let out = send_seq(addr, &[&sel, &format!("GET {}", key)]);
    if out.is_empty() { None } else { Some(out) }
}

fn dbsize_in_db(addr: &str, db: usize) -> i64 {
    let sel = format!("SELECT {}", db);
    let out = send_seq(addr, &[&sel, "DBSIZE"]);
    out.strip_prefix(':')
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

struct Guard(Vec<Child>);
impl Drop for Guard {
    fn drop(&mut self) {
        for c in &mut self.0 {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

/// Core R2 scenario, parameterized on the master's shard count:
///  1. Load keys into db0 and db2 on an N-shard master (keys spread across
///     all shards by hash).
///  2. Attach a single-shard replica → full resync must deliver EVERYTHING.
///  3. Write more keys (including INCR — non-idempotent, catches any
///     double-delivery between snapshot and live stream) → replica converges.
///  4. WAIT 1 must observe the acked replica.
fn run_multishard_master_scenario(shards: usize, master_port: u16, replica_port: u16) {
    let mdir = tempfile::tempdir().expect("mdir");
    let rdir = tempfile::tempdir().expect("rdir");
    let master = start_moon_shards(master_port, mdir.path().to_str().unwrap(), shards);
    let replica = start_moon_shards(replica_port, rdir.path().to_str().unwrap(), 1);
    let _guard = Guard(vec![master, replica]);
    let m = format!("127.0.0.1:{}", master_port);
    let r = format!("127.0.0.1:{}", replica_port);
    await_ready(&m);
    await_ready(&r);

    // Pre-sync dataset: 200 keys in db0, 60 in db2, plus a counter INCR'd to 7.
    {
        let mut stream = TcpStream::connect(&m).expect("connect master");
        stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
        let mut reader = BufReader::new(stream.try_clone().expect("clone"));
        let mut run = |cmd: String| {
            stream.write_all(format!("{}\r\n", cmd).as_bytes()).unwrap();
            stream.flush().ok();
            read_one_reply(&mut reader)
        };
        for i in 0..200 {
            assert!(
                run(format!("SET pre:{} v{}", i, i)).starts_with("+OK"),
                "SET pre:{}",
                i
            );
        }
        for _ in 0..7 {
            run("INCR pre:counter".to_string());
        }
        assert!(run("SELECT 2".to_string()).starts_with("+OK"));
        for i in 0..60 {
            assert!(
                run(format!("SET d2:{} w{}", i, i)).starts_with("+OK"),
                "SET d2:{}",
                i
            );
        }
    }
    assert_eq!(dbsize_in_db(&m, 0), 201, "master db0 baseline");
    assert_eq!(dbsize_in_db(&m, 2), 60, "master db2 baseline");

    // Attach the replica. Before R2 the master answered
    // `-ERR PSYNC across multiple shards is not yet supported` and the replica
    // stayed empty forever.
    let ro = send_cmd(&r, &format!("REPLICAOF 127.0.0.1 {}", master_port));
    assert!(ro.starts_with("+OK"), "REPLICAOF failed: {}", ro);

    assert!(
        wait_until(Duration::from_secs(30), || {
            dbsize_in_db(&r, 0) == 201 && dbsize_in_db(&r, 2) == 60
        }),
        "replica did not receive the {}-shard master's full snapshot: db0={} (want 201) db2={} (want 60)",
        shards,
        dbsize_in_db(&r, 0),
        dbsize_in_db(&r, 2)
    );
    // Spot-check values in both dbs + the non-idempotent counter.
    assert_eq!(get_in_db(&r, 0, "pre:42").as_deref(), Some("v42"));
    assert_eq!(get_in_db(&r, 0, "pre:counter").as_deref(), Some("7"));
    assert_eq!(get_in_db(&r, 2, "d2:13").as_deref(), Some("w13"));

    // Live stream: more writes across dbs and shards, incl. INCRs.
    {
        let mut stream = TcpStream::connect(&m).expect("connect master");
        stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
        let mut reader = BufReader::new(stream.try_clone().expect("clone"));
        let mut run = |cmd: String| {
            stream.write_all(format!("{}\r\n", cmd).as_bytes()).unwrap();
            stream.flush().ok();
            read_one_reply(&mut reader)
        };
        for i in 0..150 {
            assert!(run(format!("SET live:{} L{}", i, i)).starts_with("+OK"));
        }
        for _ in 0..5 {
            run("INCR pre:counter".to_string());
        }
        assert!(run("SELECT 2".to_string()).starts_with("+OK"));
        for i in 0..40 {
            assert!(run(format!("SET live2:{} M{}", i, i)).starts_with("+OK"));
        }
    }
    assert!(
        wait_until(Duration::from_secs(30), || {
            dbsize_in_db(&r, 0) == 351 && dbsize_in_db(&r, 2) == 100
        }),
        "replica did not converge on the live stream: db0={} (want 351) db2={} (want 100)",
        dbsize_in_db(&r, 0),
        dbsize_in_db(&r, 2)
    );
    assert_eq!(get_in_db(&r, 0, "live:149").as_deref(), Some("L149"));
    assert_eq!(get_in_db(&r, 0, "pre:counter").as_deref(), Some("12"));
    assert_eq!(get_in_db(&r, 2, "live2:39").as_deref(), Some("M39"));

    // WAIT must see the acked replica (R1 plumbing on a multi-shard master).
    let w = send_cmd(&m, "WAIT 1 3000");
    assert_eq!(w.trim(), ":1", "WAIT on multi-shard master: {}", w);
}

#[test]
#[ignore]
fn multishard_master_full_resync_2shards() {
    run_multishard_master_scenario(2, 17021, 17022);
}

#[test]
#[ignore]
fn multishard_master_full_resync_4shards() {
    run_multishard_master_scenario(4, 17031, 17032);
}

#[test]
#[ignore]
fn multishard_master_full_resync_8shards() {
    run_multishard_master_scenario(8, 17041, 17042);
}

/// Interleaved multi-db writers against a 4-shard master: two connections pin
/// different dbs and hammer pipelined SETs concurrently WHILE the replica is
/// attached. On a merged multi-shard wire the per-record `SELECT` framing must
/// keep every write in its own db — any cross-shard interleave that splits a
/// SELECT from its payload lands writes in the wrong db (leak asserts catch
/// it).
#[test]
#[ignore]
fn multishard_master_interleaved_multidb_live_stream() {
    let shards = 4;
    let (master_port, replica_port) = (17051, 17052);
    let mdir = tempfile::tempdir().expect("mdir");
    let rdir = tempfile::tempdir().expect("rdir");
    let master = start_moon_shards(master_port, mdir.path().to_str().unwrap(), shards);
    let replica = start_moon_shards(replica_port, rdir.path().to_str().unwrap(), 1);
    let _guard = Guard(vec![master, replica]);
    let m = format!("127.0.0.1:{}", master_port);
    let r = format!("127.0.0.1:{}", replica_port);
    await_ready(&m);
    await_ready(&r);

    let ro = send_cmd(&r, &format!("REPLICAOF 127.0.0.1 {}", master_port));
    assert!(ro.starts_with("+OK"), "REPLICAOF failed: {}", ro);
    assert!(
        wait_until(Duration::from_secs(15), || send_cmd(&r, "INFO replication")
            .contains("master_link_status:up")),
        "replica link did not come up"
    );

    const PER_DB: usize = 5000;
    let m0 = m.clone();
    let t0 = thread::spawn(move || {
        let mut stream = TcpStream::connect(&m0).expect("connect");
        stream.set_read_timeout(Some(Duration::from_secs(10))).ok();
        let mut reader = BufReader::new(stream.try_clone().expect("clone"));
        // db 0 writer, pipelined bursts of 100.
        for burst in 0..(PER_DB / 100) {
            let mut buf = String::new();
            for i in 0..100 {
                buf.push_str(&format!("SET a:{} x{}\r\n", burst * 100 + i, i));
            }
            stream.write_all(buf.as_bytes()).unwrap();
            stream.flush().ok();
            for _ in 0..100 {
                read_one_reply(&mut reader);
            }
        }
    });
    let m2 = m.clone();
    let t2 = thread::spawn(move || {
        let mut stream = TcpStream::connect(&m2).expect("connect");
        stream.set_read_timeout(Some(Duration::from_secs(10))).ok();
        let mut reader = BufReader::new(stream.try_clone().expect("clone"));
        stream.write_all(b"SELECT 2\r\n").unwrap();
        read_one_reply(&mut reader);
        for burst in 0..(PER_DB / 100) {
            let mut buf = String::new();
            for i in 0..100 {
                buf.push_str(&format!("SET b:{} y{}\r\n", burst * 100 + i, i));
            }
            stream.write_all(buf.as_bytes()).unwrap();
            stream.flush().ok();
            for _ in 0..100 {
                read_one_reply(&mut reader);
            }
        }
    });
    t0.join().expect("db0 writer");
    t2.join().expect("db2 writer");
    assert_eq!(dbsize_in_db(&m, 0), PER_DB as i64, "master db0");
    assert_eq!(dbsize_in_db(&m, 2), PER_DB as i64, "master db2");

    assert!(
        wait_until(Duration::from_secs(60), || {
            dbsize_in_db(&r, 0) == PER_DB as i64 && dbsize_in_db(&r, 2) == PER_DB as i64
        }),
        "replica diverged under interleaved multi-db load: db0={} db2={} (want {} each)",
        dbsize_in_db(&r, 0),
        dbsize_in_db(&r, 2),
        PER_DB
    );
    // Leak checks: a misapplied SELECT would put a:* keys in db2 or b:* in db0.
    assert_eq!(get_in_db(&r, 0, "a:4999").as_deref(), Some("x99"));
    assert_eq!(get_in_db(&r, 2, "b:4999").as_deref(), Some("y99"));
    assert!(get_in_db(&r, 0, "b:0").is_none(), "db2 key leaked into db0");
    assert!(get_in_db(&r, 2, "a:0").is_none(), "db0 key leaked into db2");
}

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

/// Graph content is SHARDED: a merged multi-shard snapshot carries one
/// graph-store aux blob per shard, and the replica must import ALL of them
/// (`install_graph_store_many`) — importing only the first/last blob loses
/// every graph living on the other shards.
#[test]
#[ignore]
fn multishard_master_graph_snapshot_all_shards() {
    let shards = 4;
    let (master_port, replica_port) = (17071, 17072);
    let mdir = tempfile::tempdir().expect("mdir");
    let rdir = tempfile::tempdir().expect("rdir");
    let master = start_moon_shards(master_port, mdir.path().to_str().unwrap(), shards);
    let replica = start_moon_shards(replica_port, rdir.path().to_str().unwrap(), 1);
    let _guard = Guard(vec![master, replica]);
    let m = format!("127.0.0.1:{}", master_port);
    let r = format!("127.0.0.1:{}", replica_port);
    await_ready(&m);
    await_ready(&r);

    // Enough graphs that hashing spreads them across all 4 shards.
    let graphs = ["ga", "gb", "gc", "gd", "ge", "gf", "gg", "gh"];
    for (i, g) in graphs.iter().enumerate() {
        assert!(
            send_cmd(&m, &format!("GRAPH.CREATE {}", g)).contains("OK"),
            "GRAPH.CREATE {}",
            g
        );
        for j in 0..=i {
            let reply = send_resp(
                &m,
                &["GRAPH.ADDNODE", g, "Person", "name", &format!("p{}", j)],
            );
            assert!(reply.starts_with(':'), "ADDNODE {} p{}: {}", g, j, reply);
        }
    }

    // Attach AFTER the writes — everything must arrive via the merged
    // snapshot (per-shard graph aux blobs), not the live stream.
    assert!(send_cmd(&r, &format!("REPLICAOF 127.0.0.1 {}", master_port)).starts_with("+OK"));
    assert!(
        wait_until(Duration::from_secs(30), || {
            let list = send_resp(&r, &["GRAPH.LIST"]);
            graphs.iter().all(|g| list.contains(g))
        }),
        "replica GRAPH.LIST missing graphs after snapshot: {}",
        send_resp(&r, &["GRAPH.LIST"])
    );
    // Node counts survive per graph (graph i has i+1 nodes).
    for (i, g) in graphs.iter().enumerate() {
        let want = format!(":{}", i + 1);
        assert!(
            wait_until(Duration::from_secs(10), || {
                send_resp(&r, &["GRAPH.QUERY", g, "MATCH (n:Person) RETURN count(n)"])
                    .contains(&want)
            }),
            "replica graph {} node count != {}: {}",
            g,
            i + 1,
            send_resp(&r, &["GRAPH.QUERY", g, "MATCH (n:Person) RETURN count(n)"])
        );
    }
}

/// Adversarial-review P0 regression (attach-under-write race): a local write
/// on the ACCEPTING shard that lands between the PSYNC task queueing its
/// self-shard snapshot leg and the event loop draining it is visible to the
/// snapshot body (mutation + offset already applied) while its live fan-out
/// message sits BEHIND the snapshot leg in the same FIFO — so it was
/// delivered twice (in the RDB and again live), double-applying INCR.
///
/// Hammer counters continuously WHILE the replica attaches; every counter
/// must match the master exactly after convergence. Repeated attaches widen
/// the race window.
#[test]
#[ignore]
fn multishard_master_attach_under_write_no_double_apply() {
    let shards = 4;
    let (master_port, replica_port) = (17081, 17082);
    let mdir = tempfile::tempdir().expect("mdir");
    let master = start_moon_shards(master_port, mdir.path().to_str().unwrap(), shards);
    let mut guard = Guard(vec![master]);
    let m = format!("127.0.0.1:{}", master_port);
    await_ready(&m);

    const COUNTERS: usize = 64;
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut writers = Vec::new();
    for w in 0..4 {
        let m = m.clone();
        let stop = stop.clone();
        writers.push(thread::spawn(move || {
            let mut stream = TcpStream::connect(&m).expect("connect");
            stream.set_read_timeout(Some(Duration::from_secs(10))).ok();
            let mut reader = BufReader::new(stream.try_clone().expect("clone"));
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let mut buf = String::new();
                for i in 0..COUNTERS / 4 {
                    buf.push_str(&format!("INCR cnt:{}\r\n", w * (COUNTERS / 4) + i));
                }
                stream.write_all(buf.as_bytes()).unwrap();
                stream.flush().ok();
                for _ in 0..COUNTERS / 4 {
                    read_one_reply(&mut reader);
                }
            }
        }));
    }

    // Attach (and re-attach) replicas mid-load: each fresh attach runs the
    // full multi-shard snapshot fan-out while writes race it.
    let rdir = tempfile::tempdir().expect("rdir");
    let replica = start_moon_shards(replica_port, rdir.path().to_str().unwrap(), 1);
    guard.0.push(replica);
    let r = format!("127.0.0.1:{}", replica_port);
    await_ready(&r);
    for _ in 0..5 {
        assert!(send_cmd(&r, "REPLICAOF NO ONE").starts_with("+OK"));
        thread::sleep(Duration::from_millis(50));
        assert!(send_cmd(&r, &format!("REPLICAOF 127.0.0.1 {}", master_port)).starts_with("+OK"));
        assert!(
            wait_until(Duration::from_secs(15), || send_cmd(&r, "INFO replication")
                .contains("master_link_status:up")),
            "replica link did not come up during attach-under-write"
        );
        thread::sleep(Duration::from_millis(300));
    }

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for w in writers {
        w.join().expect("writer");
    }

    // Convergence, then exact per-counter parity. A double-applied INCR
    // shows as replica > master for that counter.
    let master_vals: Vec<i64> = (0..COUNTERS)
        .map(|i| {
            get_in_db(&m, 0, &format!("cnt:{}", i))
                .and_then(|v| v.parse().ok())
                .unwrap_or(-1)
        })
        .collect();
    assert!(
        wait_until(Duration::from_secs(30), || {
            (0..COUNTERS).all(|i| {
                get_in_db(&r, 0, &format!("cnt:{}", i)).and_then(|v| v.parse().ok())
                    == Some(master_vals[i])
            })
        }),
        "replica counters diverged after attach-under-write: {:?}",
        (0..COUNTERS)
            .filter_map(|i| {
                let rv: i64 = get_in_db(&r, 0, &format!("cnt:{}", i))
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(-2);
                (rv != master_vals[i]).then_some((i, master_vals[i], rv))
            })
            .collect::<Vec<_>>()
    );
}

/// R2 exactly-once redesign regression (D2, same-key wire ordering): before
/// the unified fan-out, a cross-shard (SPSC-dispatched) write was sent to the
/// replica DIRECTLY from the execute arm while a local handler write's
/// delivery sat queued as a self-queue message — so a later-offset write
/// could hit the wire before an earlier-offset write to the SAME key on the
/// same shard. The replica applied them in arrival order and finished with
/// the loser: permanent same-key divergence with byte-exact offsets (WAIT
/// and DBSIZE both look healthy).
///
/// Four writers on distinct connections APPEND distinguishable tokens to the
/// SAME key set while a replica is attached. APPEND is order-sensitive: ONE
/// reordered pair anywhere in the stream leaves the strings permanently
/// different ("..ab.." vs "..ba.."), so this catches even a single mid-stream
/// swap — a SET-based last-write-wins check only sees a race on the very
/// last pair. Replica must byte-equal the master on every key after quiesce.
#[test]
#[ignore]
fn multishard_master_same_key_write_order_parity() {
    let shards = 4;
    let (master_port, replica_port) = (17091, 17092);
    let mdir = tempfile::tempdir().expect("mdir");
    let master = start_moon_shards(master_port, mdir.path().to_str().unwrap(), shards);
    let mut guard = Guard(vec![master]);
    let m = format!("127.0.0.1:{}", master_port);
    await_ready(&m);

    let rdir = tempfile::tempdir().expect("rdir");
    let replica = start_moon_shards(replica_port, rdir.path().to_str().unwrap(), 1);
    guard.0.push(replica);
    let r = format!("127.0.0.1:{}", replica_port);
    await_ready(&r);
    assert!(send_cmd(&r, &format!("REPLICAOF 127.0.0.1 {}", master_port)).starts_with("+OK"));
    assert!(
        wait_until(Duration::from_secs(15), || send_cmd(&r, "INFO replication")
            .contains("master_link_status:up")),
        "replica link did not come up during ordered-write load"
    );

    const KEYS: usize = 32;
    const BURSTS: u64 = 400;
    let mut writers = Vec::new();
    // 12 connections: SO_REUSEPORT placement is kernel-hashed, so a handful
    // of conns can all land on one shard — enough conns makes mixed
    // local + SPSC traffic per key near-certain.
    for w in 0..12 {
        let m = m.clone();
        writers.push(thread::spawn(move || {
            let mut stream = TcpStream::connect(&m).expect("connect");
            stream.set_read_timeout(Some(Duration::from_secs(10))).ok();
            let mut reader = BufReader::new(stream.try_clone().expect("clone"));
            for seq in 0..BURSTS {
                let mut buf = String::new();
                // Every writer APPENDs to every key — same-key races between
                // connections homed on different shards exercise both the
                // local and the SPSC-dispatched write path on each shard.
                for k in 0..KEYS {
                    let tok = format!("w{}:{};", w, seq);
                    buf.push_str(&format!(
                        "*3\r\n$6\r\nAPPEND\r\n${}\r\nokey:{}\r\n${}\r\n{}\r\n",
                        format!("okey:{}", k).len(),
                        k,
                        tok.len(),
                        tok
                    ));
                }
                stream.write_all(buf.as_bytes()).unwrap();
                stream.flush().ok();
                for _ in 0..KEYS {
                    read_one_reply(&mut reader);
                }
            }
        }));
    }

    for w in writers {
        w.join().expect("writer");
    }

    let master_vals: Vec<String> = (0..KEYS)
        .map(|k| get_in_db(&m, 0, &format!("okey:{}", k)).unwrap_or_default())
        .collect();
    assert!(
        master_vals.iter().all(|v| !v.is_empty()),
        "master lost keys?!"
    );
    assert!(
        wait_until(Duration::from_secs(30), || {
            (0..KEYS).all(|k| {
                get_in_db(&r, 0, &format!("okey:{}", k)).as_deref() == Some(&master_vals[k])
            })
        }),
        "replica strings diverged (same-key write reorder): {:?}",
        (0..KEYS)
            .filter_map(|k| {
                let rv = get_in_db(&r, 0, &format!("okey:{}", k)).unwrap_or_default();
                (rv != master_vals[k]).then(|| {
                    // Print the first divergent window, not multi-KB strings.
                    let mv = &master_vals[k];
                    let d = mv
                        .bytes()
                        .zip(rv.bytes())
                        .position(|(a, b)| a != b)
                        .unwrap_or(mv.len().min(rv.len()));
                    let lo = d.saturating_sub(20);
                    (
                        k,
                        mv.get(lo..(d + 20).min(mv.len())).unwrap_or("").to_string(),
                        rv.get(lo..(d + 20).min(rv.len())).unwrap_or("").to_string(),
                        mv.len(),
                        rv.len(),
                    )
                })
            })
            .collect::<Vec<_>>()
    );
}

/// A multi-shard master must answer ANY resumable PSYNC with +FULLRESYNC (a
/// single total offset cannot be mapped back onto N per-shard backlogs), and
/// the payload must be ONE merged RDB bulk.
#[test]
#[ignore]
fn multishard_master_partial_resync_degrades_to_full() {
    let (master_port,) = (17061,);
    let mdir = tempfile::tempdir().expect("mdir");
    let master = start_moon_shards(master_port, mdir.path().to_str().unwrap(), 4);
    let _guard = Guard(vec![master]);
    let m = format!("127.0.0.1:{}", master_port);
    await_ready(&m);
    for i in 0..50 {
        assert!(send_cmd(&m, &format!("SET k:{} v", i)).starts_with("+OK"));
    }

    // Learn the master's replid.
    let info = send_cmd(&m, "INFO replication");
    let replid = info
        .lines()
        .find_map(|l| l.strip_prefix("master_replid:"))
        .map(|s| s.trim().to_string())
        .expect("master_replid in INFO");

    // Speak the handshake by hand and ask to RESUME at offset 10 — the master
    // must refuse to CONTINUE and issue a full resync instead.
    let mut stream = TcpStream::connect(&m).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
    stream.write_all(b"PING\r\n").unwrap();
    read_one_reply(&mut reader);
    stream
        .write_all(b"REPLCONF listening-port 17062\r\n")
        .unwrap();
    read_one_reply(&mut reader);
    stream
        .write_all(format!("PSYNC {} 10\r\n", replid).as_bytes())
        .unwrap();
    stream.flush().ok();
    let mut line = String::new();
    reader.read_line(&mut line).expect("psync reply");
    assert!(
        line.starts_with("+FULLRESYNC"),
        "multi-shard master must degrade partial resync to FULLRESYNC, got: {}",
        line.trim_end()
    );
    // Next line: one merged RDB bulk header `$<len>` with a REDIS magic body.
    line.clear();
    reader.read_line(&mut line).expect("rdb header");
    let len: usize = line
        .trim_start_matches('$')
        .trim()
        .parse()
        .unwrap_or_else(|_| panic!("expected $<len> RDB header, got: {}", line.trim_end()));
    let mut magic = vec![0u8; 5];
    reader.read_exact(&mut magic).expect("rdb magic");
    assert_eq!(&magic, b"REDIS", "merged snapshot must be Redis-format RDB");
    assert!(len > 9, "suspiciously small RDB ({} bytes)", len);
}
/// CONTROL: same scenario, single-shard master (R0/R1 path untouched by R2).
#[test]
#[ignore]
fn singleshard_master_attach_under_write_control() {
    let shards = 1;
    let (master_port, replica_port) = (17085, 17086);
    let mdir = tempfile::tempdir().expect("mdir");
    let master = start_moon_shards(master_port, mdir.path().to_str().unwrap(), shards);
    let mut guard = Guard(vec![master]);
    let m = format!("127.0.0.1:{}", master_port);
    await_ready(&m);

    const COUNTERS: usize = 64;
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut writers = Vec::new();
    for w in 0..4 {
        let m = m.clone();
        let stop = stop.clone();
        writers.push(thread::spawn(move || {
            let mut stream = TcpStream::connect(&m).expect("connect");
            stream.set_read_timeout(Some(Duration::from_secs(10))).ok();
            let mut reader = BufReader::new(stream.try_clone().expect("clone"));
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let mut buf = String::new();
                for i in 0..COUNTERS / 4 {
                    buf.push_str(&format!("INCR cnt:{}\r\n", w * (COUNTERS / 4) + i));
                }
                stream.write_all(buf.as_bytes()).unwrap();
                stream.flush().ok();
                for _ in 0..COUNTERS / 4 {
                    read_one_reply(&mut reader);
                }
            }
        }));
    }

    // Attach (and re-attach) replicas mid-load: each fresh attach runs the
    // full multi-shard snapshot fan-out while writes race it.
    let rdir = tempfile::tempdir().expect("rdir");
    let replica = start_moon_shards(replica_port, rdir.path().to_str().unwrap(), 1);
    guard.0.push(replica);
    let r = format!("127.0.0.1:{}", replica_port);
    await_ready(&r);
    for _ in 0..5 {
        assert!(send_cmd(&r, "REPLICAOF NO ONE").starts_with("+OK"));
        thread::sleep(Duration::from_millis(50));
        assert!(send_cmd(&r, &format!("REPLICAOF 127.0.0.1 {}", master_port)).starts_with("+OK"));
        assert!(
            wait_until(Duration::from_secs(15), || send_cmd(&r, "INFO replication")
                .contains("master_link_status:up")),
            "replica link did not come up during attach-under-write"
        );
        thread::sleep(Duration::from_millis(300));
    }

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for w in writers {
        w.join().expect("writer");
    }

    // Convergence, then exact per-counter parity. A double-applied INCR
    // shows as replica > master for that counter.
    let master_vals: Vec<i64> = (0..COUNTERS)
        .map(|i| {
            get_in_db(&m, 0, &format!("cnt:{}", i))
                .and_then(|v| v.parse().ok())
                .unwrap_or(-1)
        })
        .collect();
    assert!(
        wait_until(Duration::from_secs(30), || {
            (0..COUNTERS).all(|i| {
                get_in_db(&r, 0, &format!("cnt:{}", i)).and_then(|v| v.parse().ok())
                    == Some(master_vals[i])
            })
        }),
        "replica counters diverged after attach-under-write: {:?}",
        (0..COUNTERS)
            .filter_map(|i| {
                let rv: i64 = get_in_db(&r, 0, &format!("cnt:{}", i))
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(-2);
                (rv != master_vals[i]).then_some((i, master_vals[i], rv))
            })
            .collect::<Vec<_>>()
    );
}
