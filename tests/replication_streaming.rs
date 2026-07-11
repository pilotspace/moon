//! R0 acceptance: single-shard streaming replication actually APPLIES the
//! master's snapshot AND its live command stream on the replica.
//!
//! These are black-box tests over two real `moon` processes. They are
//! `#[ignore]`d (like `replication_hardening.rs`) because they need a prebuilt
//! binary; run them explicitly:
//!
//! ```text
//! MOON_BIN=./target/release/moon \
//!   cargo test --test replication_streaming -- --ignored --nocapture
//! ```
//!
//! Before R0 the replica discarded both the FULLRESYNC snapshot and the live
//! stream (`buf.clear()`), so a freshly-attached replica reported `DBSIZE 0`.
//! These tests lock in the fix.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

fn moon_bin() -> String {
    std::env::var("MOON_BIN").unwrap_or_else(|_| "./target/release/moon".to_string())
}

fn start_moon(port: u16, dir: &str) -> Child {
    Command::new(moon_bin())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "1",
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
    let mut out = String::new();
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) => {
                let trimmed = line.trim_end_matches("\r\n").trim_end_matches('\n');
                out.push_str(trimmed);
                out.push('\n');
                if trimmed.starts_with('+') || trimmed.starts_with('-') || trimmed.starts_with(':')
                {
                    break;
                }
                if let Some(rest) = trimmed.strip_prefix('$') {
                    let len: i64 = rest.trim().parse().unwrap_or(-1);
                    if len < 0 {
                        break; // $-1 nil
                    }
                    let mut buf = vec![0u8; (len as usize) + 2];
                    if reader.read_exact(&mut buf).is_ok() {
                        out.push_str(&String::from_utf8_lossy(&buf[..len as usize]));
                        out.push('\n');
                    }
                    break;
                }
            }
        }
    }
    out
}

/// Read exactly one RESP reply from `reader`, returned as text (bulk bodies on
/// their own line; `$-1` nil yields an empty trailing line).
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
    for c in cmds {
        if stream.write_all(format!("{}\r\n", c).as_bytes()).is_err() {
            return String::new();
        }
    }
    stream.flush().ok();
    let mut reader = BufReader::new(&stream);
    let mut last = String::new();
    for _ in 0..cmds.len() {
        last = read_one_reply(&mut reader);
    }
    last
}

/// GET `key` in logical db `db` (SELECT + GET on one connection).
fn get_in_db(addr: &str, db: usize, key: &str) -> Option<String> {
    let v = send_seq(addr, &[&format!("SELECT {}", db), &format!("GET {}", key)]);
    let v = v.trim().to_string();
    if v.is_empty() { None } else { Some(v) }
}

fn dbsize(addr: &str) -> i64 {
    let resp = send_cmd(addr, "DBSIZE");
    resp.trim()
        .trim_start_matches(':')
        .trim()
        .parse()
        .unwrap_or(-1)
}

fn get(addr: &str, key: &str) -> Option<String> {
    let resp = send_cmd(addr, &format!("GET {}", key));
    let v = resp.lines().nth(1).map(|s| s.to_string());
    v.filter(|s| !s.is_empty())
}

fn wait_until<F: Fn() -> bool>(timeout: Duration, f: F) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if f() {
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    f()
}

struct Killer(Child);
impl Drop for Killer {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Send one command as a RESP array (binary-safe — vector blobs contain
/// arbitrary bytes) and return the whole reply, lossy-decoded, read until the
/// server goes quiet. FT.SEARCH replies are nested arrays; tests only need
/// substring assertions, not structured parsing.
fn send_resp(addr: &str, parts: &[&[u8]]) -> String {
    let Ok(mut stream) = TcpStream::connect(addr) else {
        return String::new();
    };
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .ok();
    let mut req = Vec::new();
    req.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for p in parts {
        req.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        req.extend_from_slice(p);
        req.extend_from_slice(b"\r\n");
    }
    if stream.write_all(&req).is_err() {
        return String::new();
    }
    stream.flush().ok();
    let mut out = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match Read::read(&mut stream, &mut buf) {
            Ok(0) | Err(_) => break, // closed or quiet for 500ms — reply done
            Ok(n) => out.extend_from_slice(&buf[..n]),
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Little-endian FP32 blob for a 4-dim vector.
fn vec4(a: f32, b: f32, c: f32, d: f32) -> Vec<u8> {
    let mut v = Vec::with_capacity(16);
    for f in [a, b, c, d] {
        v.extend_from_slice(&f.to_le_bytes());
    }
    v
}

/// FT.SEARCH KNN over `idx` for the query vector, returning the raw reply text.
fn knn_search(addr: &str, idx: &str, query: &[u8]) -> String {
    send_resp(
        addr,
        &[
            b"FT.SEARCH",
            idx.as_bytes(),
            b"*=>[KNN 4 @vec $q]",
            b"PARAMS",
            b"2",
            b"q",
            query,
        ],
    )
}

/// REPL-STREAM-01: replica applies the initial snapshot AND subsequent live
/// writes streamed from the master.
#[test]
#[ignore]
fn replica_applies_snapshot_and_live_stream() {
    let master_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();

    let master_addr = "127.0.0.1:16700";
    let replica_addr = "127.0.0.1:16701";

    let _master = Killer(start_moon(16700, master_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(master_addr, "PING")
            .starts_with("+PONG")),
        "master never became ready"
    );

    // Pre-connect state → must arrive via the FULLRESYNC snapshot.
    send_cmd(master_addr, "SET before1 alpha");
    send_cmd(master_addr, "SET before2 beta");

    let _replica = Killer(start_moon(16701, replica_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(replica_addr, "PING")
            .starts_with("+PONG")),
        "replica never became ready"
    );

    send_cmd(replica_addr, &format!("REPLICAOF 127.0.0.1 {}", 16700));

    // Snapshot must land: both pre-connect keys visible on the replica.
    let synced = wait_until(Duration::from_secs(10), || dbsize(replica_addr) >= 2);
    assert!(
        synced,
        "replica did not load the FULLRESYNC snapshot (dbsize={})",
        dbsize(replica_addr)
    );
    assert_eq!(get(replica_addr, "before1").as_deref(), Some("alpha"));
    assert_eq!(get(replica_addr, "before2").as_deref(), Some("beta"));

    // Post-connect write → must arrive via the LIVE stream.
    send_cmd(master_addr, "SET after1 gamma");
    let streamed = wait_until(Duration::from_secs(10), || {
        get(replica_addr, "after1").as_deref() == Some("gamma")
    });
    assert!(
        streamed,
        "replica did not apply the live-streamed write (after1={:?})",
        get(replica_addr, "after1")
    );

    // A live DEL must also propagate (delete, not just set).
    send_cmd(master_addr, "DEL before1");
    let deleted = wait_until(Duration::from_secs(10), || {
        get(replica_addr, "before1").is_none()
    });
    assert!(deleted, "replica did not apply the live-streamed DEL");

    // Confirm the replica reports replica role while still attached.
    let info = send_cmd(replica_addr, "INFO replication");
    assert!(
        info.contains("role:slave") || info.contains("role:replica"),
        "replica INFO should report replica role, got:\n{}",
        info
    );

    // MOVE and cross-db COPY are intercepted BEFORE generic dispatch on the
    // master (two-db path); the replica must mirror that intercept or they
    // silently diverge (generic dispatch errors on MOVE / mis-targets COPY..DB).
    // Their destination db is a command ARGUMENT (self-describing), so they
    // replicate even in db0-scoped R0.
    send_cmd(master_addr, "SET mvkey moved");
    send_cmd(master_addr, "MOVE mvkey 1"); // db0 -> db1
    send_cmd(master_addr, "SET cpkey orig");
    send_cmd(master_addr, "COPY cpkey cpkey2 DB 1");
    send_cmd(master_addr, "SET twodb_done 1"); // in-order stream sentinel (db0)

    // The stream is applied in order, so once the sentinel is visible on the
    // replica, MOVE + COPY have already been applied. (A bare is_none() check on
    // mvkey would false-pass immediately, since mvkey starts absent.)
    let sentinel = wait_until(Duration::from_secs(10), || {
        get(replica_addr, "twodb_done").as_deref() == Some("1")
    });
    assert!(sentinel, "two-db ordering sentinel never replicated");
    // MOVE's db0 side-effect (key left db0) is observable WITHOUT SELECT —
    // SELECT is (wrongly) rejected on a read-only replica, task #23.
    assert!(
        get(replica_addr, "mvkey").is_none(),
        "MOVE did not remove mvkey from replica db0"
    );
    // COPY leaves the source key in db0.
    assert_eq!(get(replica_addr, "cpkey").as_deref(), Some("orig"));

    // Promote the replica so the read-only guard lifts and SELECT works — then
    // confirm both two-db writes landed in db1.
    assert!(
        send_cmd(replica_addr, "REPLICAOF NO ONE").starts_with("+OK"),
        "REPLICAOF NO ONE should succeed"
    );
    thread::sleep(Duration::from_millis(300));
    assert_eq!(
        get_in_db(replica_addr, 1, "mvkey").as_deref(),
        Some("moved"),
        "MOVE did not land in replica db1"
    );
    assert_eq!(
        get_in_db(replica_addr, 1, "cpkey2").as_deref(),
        Some("orig"),
        "cross-db COPY did not land in replica db1"
    );
}

/// REPL-STREAM-02 (v0.7 R0.5): the vector/text index plane replicates —
/// definitions AND contents.
///
/// Before R0.5 only the KV plane replicated: the replica applied HSET into the
/// hash but never fed `auto_index_hset` (FT.SEARCH on the replica returned
/// nothing), never tombstoned on DEL (ghosts), never cleared on FLUSHALL, and
/// FT.CREATE was not streamed at all (FT._LIST on the replica was empty).
#[test]
#[ignore]
fn replica_syncs_vector_index_defs_and_contents() {
    let master_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();

    let master_addr = "127.0.0.1:16710";
    let replica_addr = "127.0.0.1:16711";

    let _master = Killer(start_moon(16710, master_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(master_addr, "PING")
            .starts_with("+PONG")),
        "master never became ready"
    );

    // Pre-attach state: index definition + one indexed document. Both must
    // reach the replica via the FULLRESYNC snapshot (defs) + backfill (docs).
    let created = send_resp(
        master_addr,
        &[
            b"FT.CREATE",
            b"repidx",
            b"ON",
            b"HASH",
            b"PREFIX",
            b"1",
            b"v:",
            b"SCHEMA",
            b"vec",
            b"VECTOR",
            b"HNSW",
            b"6",
            b"DIM",
            b"4",
            b"TYPE",
            b"FLOAT32",
            b"DISTANCE_METRIC",
            b"L2",
        ],
    );
    assert!(created.contains("OK"), "FT.CREATE failed: {created}");
    let v1 = vec4(1.0, 0.0, 0.0, 0.0);
    send_resp(master_addr, &[b"HSET", b"v:1", b"vec", &v1]);
    // Sanity: master itself must find it.
    let q = vec4(1.0, 0.0, 0.0, 0.0);
    assert!(
        knn_search(master_addr, "repidx", &q).contains("v:1"),
        "master should find v:1 before attach"
    );

    let _replica = Killer(start_moon(16711, replica_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(replica_addr, "PING")
            .starts_with("+PONG")),
        "replica never became ready"
    );
    send_cmd(replica_addr, &format!("REPLICAOF 127.0.0.1 {}", 16710));

    // 1. Index DEFINITION must arrive with the snapshot.
    let def_synced = wait_until(Duration::from_secs(10), || {
        send_resp(replica_addr, &[b"FT._LIST"]).contains("repidx")
    });
    assert!(
        def_synced,
        "replica FT._LIST never listed repidx (index defs not in snapshot): {}",
        send_resp(replica_addr, &[b"FT._LIST"])
    );

    // 2. Pre-attach CONTENT must be searchable (snapshot backfill).
    let content_synced = wait_until(Duration::from_secs(10), || {
        knn_search(replica_addr, "repidx", &q).contains("v:1")
    });
    assert!(
        content_synced,
        "replica never indexed snapshot doc v:1: {}",
        knn_search(replica_addr, "repidx", &q)
    );

    // 3. Live HSET must be indexed on the replica (apply-side auto-index hook).
    let v2 = vec4(0.0, 1.0, 0.0, 0.0);
    send_resp(master_addr, &[b"HSET", b"v:2", b"vec", &v2]);
    let live_indexed = wait_until(Duration::from_secs(10), || {
        knn_search(replica_addr, "repidx", &q).contains("v:2")
    });
    assert!(
        live_indexed,
        "replica never indexed live-streamed v:2: {}",
        knn_search(replica_addr, "repidx", &q)
    );

    // 4. Live DEL must tombstone on the replica (apply-side delete hook).
    send_cmd(master_addr, "DEL v:1");
    let deleted = wait_until(Duration::from_secs(10), || {
        !knn_search(replica_addr, "repidx", &q).contains("v:1")
    });
    assert!(
        deleted,
        "replica still returns deleted v:1 (ghost): {}",
        knn_search(replica_addr, "repidx", &q)
    );

    // 5. Live FT.CREATE must stream (def created AFTER attach).
    let created2 = send_resp(
        master_addr,
        &[
            b"FT.CREATE",
            b"repidx2",
            b"ON",
            b"HASH",
            b"PREFIX",
            b"1",
            b"w:",
            b"SCHEMA",
            b"vec",
            b"VECTOR",
            b"HNSW",
            b"6",
            b"DIM",
            b"4",
            b"TYPE",
            b"FLOAT32",
            b"DISTANCE_METRIC",
            b"L2",
        ],
    );
    assert!(
        created2.contains("OK"),
        "second FT.CREATE failed: {created2}"
    );
    let live_def = wait_until(Duration::from_secs(10), || {
        send_resp(replica_addr, &[b"FT._LIST"]).contains("repidx2")
    });
    assert!(
        live_def,
        "replica FT._LIST never listed live-created repidx2: {}",
        send_resp(replica_addr, &[b"FT._LIST"])
    );

    // 6. FLUSHALL clears index CONTENTS on the replica but keeps definitions.
    send_cmd(master_addr, "FLUSHALL");
    let flushed = wait_until(Duration::from_secs(10), || {
        !knn_search(replica_addr, "repidx", &q).contains("v:2")
    });
    assert!(
        flushed,
        "replica still returns flushed v:2 (ghost): {}",
        knn_search(replica_addr, "repidx", &q)
    );
    let list_after_flush = send_resp(replica_addr, &[b"FT._LIST"]);
    assert!(
        list_after_flush.contains("repidx") && list_after_flush.contains("repidx2"),
        "FLUSHALL must keep index definitions on the replica: {list_after_flush}"
    );
}

/// REPL-STREAM-03 (C1 regression guard): FT.CREATE traffic racing a replica
/// attach must never lose a definition. The master's PSYNC path registers the
/// replica BEFORE the backlog catch-up read and bounds that read to the
/// registration offset; before that fix, a def-mutation drained between the
/// catch-up read and registration reached neither the RDB, the catch-up, nor
/// the live stream — a silent gap. This test hammers FT.CREATE from a side
/// thread while the replica attaches mid-stream, then requires exact
/// index-list parity. Probabilistic per run, deterministic across CI history:
/// any hit is a real ordering regression.
#[test]
#[ignore]
fn replica_attach_races_live_ft_create() {
    let master_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();

    let master_addr = "127.0.0.1:16720";
    let replica_addr = "127.0.0.1:16721";

    let _master = Killer(start_moon(16720, master_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(master_addr, "PING")
            .starts_with("+PONG")),
        "master never became ready"
    );

    // Side thread: create rc_idx_0..rc_idx_29 with tiny gaps so creations
    // interleave with every phase of the attach (handshake, RDB write,
    // catch-up, registration).
    const N: usize = 30;
    let writer = std::thread::spawn(move || {
        for i in 0..N {
            let name = format!("rc_idx_{i}");
            let prefix = format!("rc{i}:");
            let created = send_resp(
                "127.0.0.1:16720",
                &[
                    b"FT.CREATE",
                    name.as_bytes(),
                    b"ON",
                    b"HASH",
                    b"PREFIX",
                    b"1",
                    prefix.as_bytes(),
                    b"SCHEMA",
                    b"vec",
                    b"VECTOR",
                    b"HNSW",
                    b"6",
                    b"DIM",
                    b"4",
                    b"TYPE",
                    b"FLOAT32",
                    b"DISTANCE_METRIC",
                    b"L2",
                ],
            );
            assert!(created.contains("OK"), "FT.CREATE {name} failed: {created}");
            std::thread::sleep(Duration::from_millis(20));
        }
    });

    // Attach the replica mid-stream (~1/4 of the creations done).
    std::thread::sleep(Duration::from_millis(150));
    let _replica = Killer(start_moon(16721, replica_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(replica_addr, "PING")
            .starts_with("+PONG")),
        "replica never became ready"
    );
    let attach = send_cmd(replica_addr, "REPLICAOF 127.0.0.1 16720");
    assert!(attach.starts_with("+OK"), "REPLICAOF failed: {attach}");

    writer.join().expect("FT.CREATE writer thread panicked");

    // Every index the master knows must reach the replica — via RDB aux,
    // catch-up, or live stream; which leg is timing-dependent, parity is not.
    let synced = wait_until(Duration::from_secs(15), || {
        let list = send_resp(replica_addr, &[b"FT._LIST"]);
        (0..N).all(|i| list.contains(&format!("rc_idx_{i}")))
    });
    let master_list = send_resp(master_addr, &[b"FT._LIST"]);
    let replica_list = send_resp(replica_addr, &[b"FT._LIST"]);
    assert!(
        synced,
        "replica lost FT.CREATE(s) during attach race.\nmaster:  {master_list}\nreplica: {replica_list}"
    );
}

/// REPL-STREAM-04 (adversarial-review P0-1 on the self-SPSC fix): MULTI/EXEC
/// bodies must replicate.
///
/// The single-command local-write path records every successful write in the
/// replication plane, but EXEC persisted its body through `persist_txn_aof`'s
/// AOF-only leg: a `MULTI / SET / INCR / EXEC` on a `--shards 1` master with
/// an attached replica committed durably on the master and NEVER reached the
/// replica — no backlog bytes, no offset advance, no live fan-out. Silent,
/// deterministic divergence for every application using transactions.
#[test]
#[ignore]
fn replica_applies_multi_exec_bodies() {
    let master_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();

    let master_addr = "127.0.0.1:16730";
    let replica_addr = "127.0.0.1:16731";

    let _master = Killer(start_moon(16730, master_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(master_addr, "PING")
            .starts_with("+PONG")),
        "master never became ready"
    );

    let _replica = Killer(start_moon(16731, replica_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(replica_addr, "PING")
            .starts_with("+PONG")),
        "replica never became ready"
    );

    send_cmd(replica_addr, &format!("REPLICAOF 127.0.0.1 {}", 16730));

    // Prove the live stream is up with a plain single write first.
    send_cmd(master_addr, "SET plain alive");
    assert!(
        wait_until(Duration::from_secs(10), || {
            get(replica_addr, "plain").as_deref() == Some("alive")
        }),
        "single-command live stream not flowing — txn assertions would be meaningless"
    );

    // The transaction under test: a SET and two INCRs (INCR doubles as a
    // double-apply canary — a re-delivered body would show ctr=4).
    let exec_reply = send_seq(
        master_addr,
        &["MULTI", "SET t1 v1", "INCR ctr", "INCR ctr", "EXEC"],
    );
    assert!(
        !exec_reply.contains("ERR"),
        "EXEC failed on the master: {exec_reply}"
    );
    assert_eq!(
        get(master_addr, "ctr").as_deref(),
        Some("2"),
        "master must see the txn's own effects"
    );

    // In-order stream sentinel: once this single post-txn write is visible,
    // the txn body (streamed before it) must already have been applied.
    send_cmd(master_addr, "SET txn_done 1");
    assert!(
        wait_until(Duration::from_secs(10), || {
            get(replica_addr, "txn_done").as_deref() == Some("1")
        }),
        "post-txn sentinel never replicated"
    );

    assert_eq!(
        get(replica_addr, "t1").as_deref(),
        Some("v1"),
        "MULTI/EXEC SET did not replicate"
    );
    assert_eq!(
        get(replica_addr, "ctr").as_deref(),
        Some("2"),
        "MULTI/EXEC INCRs did not replicate exactly once (None=lost, 4=double-applied)"
    );
}

/// HIGH-2 (task #22 + #23): multi-db writes must land in the SAME logical db
/// on the replica. The master prepends `SELECT <db>` to the stream whenever
/// the writing connection's db differs from the stream's current db context;
/// the replica-side drain already binds subsequent commands to it. Also
/// exercises #23: reading db 2 on the replica requires SELECT on a replica
/// connection (previously rejected with READONLY — SELECT is flagged W).
#[test]
#[ignore]
fn replica_applies_multi_db_stream() {
    let master_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();

    let master_addr = "127.0.0.1:16734";
    let replica_addr = "127.0.0.1:16735";

    let _master = Killer(start_moon(16734, master_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(master_addr, "PING")
            .starts_with("+PONG")),
        "master never became ready"
    );
    let _replica = Killer(start_moon(16735, replica_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(replica_addr, "PING")
            .starts_with("+PONG")),
        "replica never became ready"
    );
    send_cmd(replica_addr, "REPLICAOF 127.0.0.1 16734");

    // Prove the stream is up in db 0 first.
    send_cmd(master_addr, "SET d0key d0val");
    assert!(
        wait_until(Duration::from_secs(10), || {
            get(replica_addr, "d0key").as_deref() == Some("d0val")
        }),
        "db-0 live stream not flowing"
    );

    // #23: SELECT must be served by the read-only replica.
    let sel = send_cmd(replica_addr, "SELECT 2");
    assert!(
        sel.starts_with("+OK"),
        "SELECT on a read-only replica must succeed, got: {sel}"
    );

    // Write in db 2, then hop back to db 0 — the stream must carry both
    // context switches.
    let r = send_seq(master_addr, &["SELECT 2", "SET d2key d2val"]);
    assert!(r.starts_with("+OK"), "master db-2 SET failed: {r}");
    send_cmd(master_addr, "SET d0post after");

    assert!(
        wait_until(Duration::from_secs(10), || {
            get(replica_addr, "d0post").as_deref() == Some("after")
        }),
        "post-hop db-0 sentinel never replicated"
    );
    assert_eq!(
        get_in_db(replica_addr, 2, "d2key").as_deref(),
        Some("d2val"),
        "db-2 write did not land in db 2 on the replica"
    );
    assert_eq!(
        get_in_db(replica_addr, 0, "d2key"),
        None,
        "db-2 write leaked into db 0 on the replica (SELECT not streamed)"
    );
    // The db-0 sentinel must not have leaked into db 2 either.
    assert_eq!(
        get_in_db(replica_addr, 2, "d0post"),
        None,
        "db-0 write leaked into db 2 on the replica"
    );
}

/// R1 (task #19): real WAIT/ACK plumbing. The replica sends
/// `REPLCONF ACK <offset>` on the replication link (after each applied batch
/// + a 1s idle tick); the master reads them off the hijacked PSYNC socket and
/// records them per replica; `WAIT <n> <ms>` blocks until n replicas
/// acknowledge the master's current offset. Previously WAIT returned 0
/// unconditionally on the monoio runtime and no ACK was ever sent or read.
#[test]
#[ignore]
fn wait_returns_acked_replica_count() {
    let master_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();

    let master_addr = "127.0.0.1:16736";
    let replica_addr = "127.0.0.1:16737";

    let _master = Killer(start_moon(16736, master_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(master_addr, "PING")
            .starts_with("+PONG")),
        "master never became ready"
    );

    // No replicas attached: WAIT 1 with a short timeout must return 0 fast.
    let r = send_cmd(master_addr, "WAIT 1 200");
    assert_eq!(r.trim(), ":0", "WAIT with no replicas must return 0: {r}");

    let _replica = Killer(start_moon(16737, replica_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(replica_addr, "PING")
            .starts_with("+PONG")),
        "replica never became ready"
    );
    send_cmd(replica_addr, &format!("REPLICAOF 127.0.0.1 {}", 16736));

    // Prove the stream is live.
    send_cmd(master_addr, "SET w1 v1");
    assert!(
        wait_until(Duration::from_secs(10), || {
            get(replica_addr, "w1").as_deref() == Some("v1")
        }),
        "live stream not flowing — WAIT assertions would be meaningless"
    );

    // Write, then WAIT for 1 replica to acknowledge everything so far. The
    // ACK cadence is drain-driven + a 1s idle tick, so a 3s budget is ample.
    send_cmd(master_addr, "SET w2 v2");
    let r = send_cmd(master_addr, "WAIT 1 3000");
    assert_eq!(
        r.trim(),
        ":1",
        "WAIT must observe the replica's ACK of the current offset: {r}"
    );

    // Asking for MORE replicas than exist must time out and report the real
    // acked count (1), not hang or claim 2.
    let r = send_cmd(master_addr, "WAIT 2 300");
    assert_eq!(r.trim(), ":1", "WAIT 2 with one replica must return 1: {r}");
}

/// Task #35 (v0.7 consistency gate): interleaved multi-connection multi-db
/// load must CONVERGE on the replica. Locks in two pre-existing defects the
/// R1 gates caught:
///
/// 1. Client `SELECT` commands leaked into the replication stream as literal
///    records (SELECT is W-flagged), desyncing the master's `stream_db`
///    tracking — a db-0 write after another connection's SELECT 2 handshake
///    applied into the replica's db 2.
/// 2. `wal_append_and_fanout` / `ReplicaLiveFanout` silently DROPPED records
///    when a replica's bounded fan-out channel filled under pipelined load —
///    the replica stayed "up" but permanently diverged (observed 2k of 40k
///    keys, link_status:up). The fix kicks the lagging replica so it
///    reconnects and resyncs — divergence becomes a loud, self-healing
///    resync, never silence.
#[test]
#[ignore]
fn replica_converges_under_interleaved_multidb_load() {
    let master_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();

    let master_addr = "127.0.0.1:16738";
    let replica_addr = "127.0.0.1:16739";

    let _master = Killer(start_moon(16738, master_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(master_addr, "PING")
            .starts_with("+PONG")),
        "master never became ready"
    );
    let _replica = Killer(start_moon(16739, replica_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(replica_addr, "PING")
            .starts_with("+PONG")),
        "replica never became ready"
    );
    send_cmd(replica_addr, "REPLICAOF 127.0.0.1 16738");

    // Prove the stream is up before loading.
    send_cmd(master_addr, "SET warm 1");
    assert!(
        wait_until(Duration::from_secs(10), || {
            get(replica_addr, "warm").as_deref() == Some("1")
        }),
        "live stream not flowing — load assertions would be meaningless"
    );

    // Two persistent connections: conn0 stays on db 0; conn2 SELECTs db 2 (a
    // literal client SELECT — the record that used to poison the stream).
    let mut conn0 = TcpStream::connect(master_addr).expect("conn0");
    let mut conn2 = TcpStream::connect(master_addr).expect("conn2");
    conn0
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    conn2
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    pipeline_burst(&mut conn2, &["SELECT 2".to_string()]);

    // Interleaved pipelined bursts, large enough to overflow a bounded
    // per-replica fan-out channel repeatedly.
    const BATCH: usize = 2000;
    const BATCHES: usize = 5;
    for b in 0..BATCHES {
        let cmds0: Vec<String> = (0..BATCH)
            .map(|i| format!("SET k0:{} v{}", b * BATCH + i, b * BATCH + i))
            .collect();
        let cmds2: Vec<String> = (0..BATCH)
            .map(|i| format!("SET k2:{} v{}", b * BATCH + i, b * BATCH + i))
            .collect();
        pipeline_burst(&mut conn0, &cmds0);
        pipeline_burst(&mut conn2, &cmds2);
    }
    drop(conn0);
    drop(conn2);

    let total = (BATCH * BATCHES) as i64;
    assert_eq!(dbsize(master_addr), total + 1, "master db0 load incomplete");

    // Convergence: the replica may be kicked + resync mid-load; give it a
    // generous settle window but require exact parity at the end.
    let converged = wait_until(Duration::from_secs(60), || {
        dbsize_in_db(replica_addr, 0) == total + 1 && dbsize_in_db(replica_addr, 2) == total
    });
    let (r0, r2) = (dbsize_in_db(replica_addr, 0), dbsize_in_db(replica_addr, 2));
    assert!(
        converged,
        "replica diverged after interleaved multi-db load: db0={r0}/{} db2={r2}/{}",
        total + 1,
        total
    );
    // Spot checks: correct placement, no cross-db leaks.
    assert_eq!(
        get_in_db(replica_addr, 0, "k0:9999").as_deref(),
        Some("v9999"),
        "db0 tail key wrong"
    );
    assert_eq!(
        get_in_db(replica_addr, 2, "k2:9999").as_deref(),
        Some("v9999"),
        "db2 tail key wrong"
    );
    assert_eq!(
        get_in_db(replica_addr, 2, "k0:42"),
        None,
        "db0 key leaked into db2 (SELECT poisoning)"
    );
    assert_eq!(
        get_in_db(replica_addr, 0, "k2:42"),
        None,
        "db2 key leaked into db0"
    );
}

/// Write `cmds` as one pipelined burst on a persistent connection and read
/// exactly one reply line per command (inline protocol; replies here are all
/// +OK/+PONG-class single lines).
fn pipeline_burst(stream: &mut TcpStream, cmds: &[String]) {
    let mut payload = String::with_capacity(cmds.len() * 24);
    for c in cmds {
        payload.push_str(c);
        payload.push_str("\r\n");
    }
    stream.write_all(payload.as_bytes()).expect("burst write");
    stream.flush().ok();
    let mut reader = BufReader::new(&*stream);
    for i in 0..cmds.len() {
        let mut line = String::new();
        reader.read_line(&mut line).expect("burst reply");
        assert!(
            line.starts_with("+OK"),
            "burst cmd {i} ({}) got: {line}",
            cmds[i]
        );
    }
}

/// DBSIZE in a specific db over one connection (SELECT + DBSIZE).
fn dbsize_in_db(addr: &str, db: usize) -> i64 {
    let reply = send_seq(addr, &[&format!("SELECT {db}"), "DBSIZE"]);
    reply
        .rsplit(':')
        .next()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(-1)
}
