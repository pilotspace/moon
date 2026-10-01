//! moon#1300: a cross-store `TXN` is atomic across a crash and a snapshot.
//!
//! F4 (with an AOF): a transaction's writes reach the AOF as they run. A
//! kill -9 before `TXN COMMIT` / `TXN ABORT` left them in the log with nothing
//! to say they were uncommitted, and recovery brought them back. The writer
//! now brackets them in `MOON.TXN BEGIN|PAUSE|END <id>` records; a block that
//! never ended is rolled back on replay.
//!
//! F3 (without an AOF): a snapshot taken while a transaction was open
//! serialized its uncommitted values, and a later abort had nowhere to log
//! its compensation. Every snapshot now stores a held key's pre-transaction
//! image.
//!
//! Every test runs a real server (`MOON_BIN` pinned) with
//! `--appendfsync always` (an acknowledged write is on disk), SIGKILLs it and
//! restarts it on the same directory, at `--shards 1` (the flat/top-level
//! AOF layout) and `--shards 4` (per-shard). Keys share a hash tag local to
//! the transaction's connection: a `TXN` refuses another shard's key.
//!
//! ```text
//! MOON_BIN=/path/to/moon cargo test --test txn_crash_atomicity_1300 \
//!     -- --include-ignored --test-threads 2
//! ```
//!
//! The replica tests need master-side PSYNC (monoio): set
//! `MOON_TEST_NO_MASTER_PSYNC=1` with a tokio binary to skip them.
//! `MOON_DOWNGRADE_BIN=<pre-#1300 moon>` runs the downgrade-read test.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::Conn;

const OK: &str = "+OK\r\n";
const NIL: &str = "$-1\r\n";

fn bulk(s: &str) -> String {
    format!("${}\r\n{s}\r\n", s.len())
}

fn int(n: i64) -> String {
    format!(":{n}\r\n")
}

/// How a server is started (kept so a restart reuses it).
#[derive(Clone)]
struct Cfg {
    bin: PathBuf,
    dir: PathBuf,
    shards: usize,
    aof: bool,
    save: String,
    env: Vec<(String, String)>,
}

impl Cfg {
    fn new(dir: &Path, shards: usize, aof: bool) -> Self {
        Cfg {
            bin: common::find_moon_binary(),
            dir: dir.to_path_buf(),
            shards,
            aof,
            save: String::new(),
            env: Vec::new(),
        }
    }
}

struct Srv {
    guard: common::ServerGuard,
    port: u16,
    cfg: Cfg,
}

fn start(cfg: &Cfg) -> Srv {
    let dir_s = cfg.dir.to_str().expect("utf8 dir").to_string();
    let (guard, port) = common::spawn_listening_guarded(|port| {
        let mut cmd = Command::new(&cfg.bin);
        cmd.args([
            "--port",
            &port.to_string(),
            "--shards",
            &cfg.shards.to_string(),
            "--dir",
            &dir_s,
            "--appendonly",
            if cfg.aof { "yes" } else { "no" },
            "--appendfsync",
            "always",
            "--save",
            &cfg.save,
            "--disk-free-min-pct",
            "0",
        ])
        .env("RUST_LOG", "moon=info")
        .env("MOON_DISK_FREE_MIN_PCT", "0")
        .stdout(Stdio::null())
        .stderr(common::server_stderr(&cfg.dir));
        for (k, v) in &cfg.env {
            cmd.env(k, v);
        }
        cmd.spawn().expect("spawn moon (set MOON_BIN)")
    });
    wait_ready(port);
    Srv {
        guard,
        port,
        cfg: cfg.clone(),
    }
}

/// PING until `+PONG` — a restarting server answers `-LOADING` first.
fn wait_ready(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if let Ok(mut s) = std::net::TcpStream::connect(("127.0.0.1", port)) {
            use std::io::{Read, Write};
            let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
            if s.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok() {
                let mut buf = [0u8; 64];
                if let Ok(n) = s.read(&mut buf)
                    && buf[..n].starts_with(b"+PONG")
                {
                    return;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("server on {port} never answered PONG");
}

/// kill -9, then start again on the same directory and flags.
fn crash_restart(mut srv: Srv) -> Srv {
    srv.guard.kill_now();
    let cfg = srv.cfg.clone();
    drop(srv);
    start(&cfg)
}

/// A hash tag whose keys this connection's shard owns (a `TXN` refuses a
/// write to another shard's key). Probed with a throwaway transaction.
fn local_tag(c: &mut Conn) -> String {
    for i in 0..512 {
        let tag = format!("t{i}");
        assert_eq!(c.send(&["TXN", "BEGIN"]), OK);
        let r = c.send(&["SET", &format!("{{{tag}}}:probe"), "1"]);
        assert_eq!(c.send(&["TXN", "ABORT"]), OK);
        if r == OK {
            assert_eq!(c.send(&["DEL", &format!("{{{tag}}}:probe")]), int(0));
            return tag;
        }
    }
    panic!("no hash tag is local to this connection's shard");
}

fn info_field(info: &str, field: &str) -> Option<String> {
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .map(|v| v.trim().to_string())
}

fn bgrewriteaof_and_wait(c: &mut Conn) {
    let r = c.send(&["BGREWRITEAOF"]);
    assert!(r.starts_with('+'), "BGREWRITEAOF answered {r:?}");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let info = c.send(&["INFO", "persistence"]);
        if info_field(&info, "aof_rewrite_in_progress").as_deref() == Some("0")
            && info_field(&info, "aof_rewrite_scheduled").as_deref() != Some("1")
        {
            assert_eq!(
                info_field(&info, "aof_last_bgrewrite_status").as_deref(),
                Some("ok"),
                "the rewrite failed: {info}"
            );
            return;
        }
        assert!(Instant::now() < deadline, "BGREWRITEAOF never finished");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn lastsave(c: &mut Conn) -> i64 {
    c.send(&["LASTSAVE"])
        .trim_start_matches(':')
        .trim()
        .parse::<i64>()
        .unwrap_or(0)
}

/// Run `start_save` (BGSAVE, SAVE, …) and wait until a save completed after
/// it: LASTSAVE moves (1 s resolution, hence the sleep first).
fn save_and_wait(c: &mut Conn, start_save: &[&str]) {
    let before = lastsave(c);
    std::thread::sleep(Duration::from_millis(1_100));
    if !start_save.is_empty() {
        let r = c.send(start_save);
        assert!(r.starts_with('+'), "{start_save:?} answered {r:?}");
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let info = c.send(&["INFO", "persistence"]);
        if info.contains("rdb_bgsave_in_progress:0") && lastsave(c) > before {
            assert!(info.contains("rdb_last_bgsave_status:ok"), "{info}");
            return;
        }
        assert!(Instant::now() < deadline, "the save never completed");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Every AOF file of `dir`, concatenated (for byte-level assertions).
fn aof_bytes(dir: &Path) -> Vec<u8> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.to_string_lossy().contains(".aof") {
                out.extend(std::fs::read(&p).unwrap_or_default());
            }
        }
    }
    out
}

/// The most recently written incr file of `dir`'s AOF.
fn newest_incr(dir: &Path) -> PathBuf {
    let mut best: Option<(std::time::SystemTime, PathBuf)> = None;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            let name = p.to_string_lossy().to_string();
            if !(name.contains("incr") || name.ends_with("appendonly.aof")) {
                continue;
            }
            let Ok(m) = std::fs::metadata(&p).and_then(|m| m.modified()) else {
                continue;
            };
            if best.as_ref().is_none_or(|(t, _)| m >= *t) {
                best = Some((m, p));
            }
        }
    }
    best.expect("an AOF incr file").1
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

fn tmpdir(tag: &str) -> PathBuf {
    common::unique_test_dir(&format!("txn-1300-{tag}"))
}

// ---------------------------------------------------------------------------
// The keys every KV case uses: an update, an insert, a delete, a hash field
// write (with a field deadline on the original), a counter, and another
// client's key written while the transaction is open.
// ---------------------------------------------------------------------------

struct Keys {
    upd: String,
    new: String,
    del: String,
    hash: String,
    ctr: String,
    other: String,
}

impl Keys {
    fn new(tag: &str) -> Self {
        let k = |n: &str| format!("{{{tag}}}:{n}");
        Keys {
            upd: k("upd"),
            new: k("new"),
            del: k("del"),
            hash: k("hash"),
            ctr: k("ctr"),
            other: k("other"),
        }
    }
}

fn seed(c: &mut Conn, k: &Keys) {
    assert_eq!(c.send(&["SET", &k.upd, "original"]), OK);
    assert_eq!(c.send(&["RPUSH", &k.del, "a", "b"]), int(2));
    assert_eq!(c.send(&["HSET", &k.hash, "f1", "v1", "f2", "v2"]), int(2));
    assert_eq!(
        c.send(&["HPEXPIRE", &k.hash, "600000", "FIELDS", "1", "f1"]),
        "*1\r\n:1\r\n"
    );
}

/// The transaction's writes, with another client's acknowledged write in
/// the middle of them (same shard: the AOF interleaves the two).
fn txn_writes(t: &mut Conn, c: &mut Conn, k: &Keys, other_value: &str) {
    assert_eq!(t.send(&["TXN", "BEGIN"]), OK);
    assert_eq!(t.send(&["SET", &k.upd, "uncommitted"]), OK);
    assert_eq!(t.send(&["SET", &k.new, "uncommitted"]), OK);
    assert_eq!(c.send(&["SET", &k.other, other_value]), OK);
    assert_eq!(t.send(&["DEL", &k.del]), int(1));
    assert_eq!(t.send(&["HSET", &k.hash, "f1", "X", "f3", "Y"]), int(1));
    assert_eq!(t.send(&["INCR", &k.ctr]), int(1));
}

/// The pre-transaction state (plus the other client's write).
fn assert_pre_txn(c: &mut Conn, k: &Keys, other_value: Option<&str>, when: &str) {
    assert_eq!(
        c.send(&["GET", &k.upd]),
        bulk("original"),
        "{when}: update rolled back"
    );
    assert_eq!(c.send(&["GET", &k.new]), NIL, "{when}: insert rolled back");
    assert_eq!(
        c.send(&["LRANGE", &k.del, "0", "-1"]),
        "*2\r\n$1\r\na\r\n$1\r\nb\r\n",
        "{when}: delete rolled back"
    );
    assert_eq!(c.send(&["HLEN", &k.hash]), int(2), "{when}: hash fields");
    assert_eq!(
        c.send(&["HGET", &k.hash, "f1"]),
        bulk("v1"),
        "{when}: hash value"
    );
    let fttl = c.send(&["HPTTL", &k.hash, "FIELDS", "2", "f1", "f2"]);
    assert!(
        fttl.starts_with("*2\r\n:") && fttl.ends_with(":-1\r\n") && !fttl.contains(":-2"),
        "{when}: f1 keeps its field deadline, f2 has none: {fttl:?}"
    );
    assert_eq!(c.send(&["GET", &k.ctr]), NIL, "{when}: counter rolled back");
    match other_value {
        Some(v) => assert_eq!(
            c.send(&["GET", &k.other]),
            bulk(v),
            "{when}: the other client's write stands"
        ),
        None => assert_eq!(c.send(&["GET", &k.other]), NIL, "{when}: other"),
    }
}

/// The committed state.
fn assert_committed(c: &mut Conn, k: &Keys, other_value: &str, when: &str) {
    assert_eq!(
        c.send(&["GET", &k.upd]),
        bulk("uncommitted"),
        "{when}: update"
    );
    assert_eq!(
        c.send(&["GET", &k.new]),
        bulk("uncommitted"),
        "{when}: insert"
    );
    assert_eq!(c.send(&["EXISTS", &k.del]), int(0), "{when}: delete");
    assert_eq!(c.send(&["HGET", &k.hash, "f1"]), bulk("X"), "{when}: hash");
    assert_eq!(c.send(&["HGET", &k.hash, "f3"]), bulk("Y"), "{when}: hash");
    assert_eq!(c.send(&["GET", &k.ctr]), bulk("1"), "{when}: counter");
    assert_eq!(
        c.send(&["GET", &k.other]),
        bulk(other_value),
        "{when}: other"
    );
}

// ---------------------------------------------------------------------------
// F4 — with an AOF
// ---------------------------------------------------------------------------

/// kill -9 inside an open transaction: none of its records replay, another
/// client's write made inside the block does, and writes made after the
/// restart survive the NEXT restart (the reopened log owes `MOON.TXN RESET`,
/// so the dead block cannot roll back over them at the end of the file).
fn crash_inside_an_open_txn(shards: usize) {
    let dir = tmpdir("crash");
    let srv = start(&Cfg::new(&dir, shards, true));
    let mut t = Conn::open(srv.port);
    let tag = local_tag(&mut t);
    let k = Keys::new(&tag);
    let mut c = Conn::open(srv.port);
    seed(&mut c, &k);
    txn_writes(&mut t, &mut c, &k, "acked");
    // kill -9 with the transaction's connection still open: a disconnect
    // first would roll it back cleanly and hide the crash.
    let srv = crash_restart(srv);
    drop((t, c));
    let mut c = Conn::open(srv.port);
    assert_pre_txn(&mut c, &k, Some("acked"), "after kill -9 inside the TXN");
    // The next session writes a key the dead transaction wrote, and reads one.
    assert_eq!(c.send(&["SET", &k.upd, "next-session"]), OK);
    assert_eq!(c.send(&["APPEND", &k.new, "fresh"]), int(5));
    drop(c);
    let srv = crash_restart(srv);
    let mut c = Conn::open(srv.port);
    assert_eq!(
        c.send(&["GET", &k.upd]),
        bulk("next-session"),
        "a later session's write survives"
    );
    assert_eq!(c.send(&["GET", &k.new]), bulk("fresh"));
    assert_eq!(c.send(&["HGET", &k.hash, "f1"]), bulk("v1"));
    drop(c);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn crash_inside_an_open_txn_replays_none_of_it_1_shard() {
    crash_inside_an_open_txn(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn crash_inside_an_open_txn_replays_none_of_it_4_shards() {
    crash_inside_an_open_txn(4);
}

/// A committed transaction replays fully, interleaved with another client's
/// writes; a write to one of its keys after the commit stands.
fn committed_txn_replays_fully(shards: usize) {
    let dir = tmpdir("commit");
    let srv = start(&Cfg::new(&dir, shards, true));
    let mut t = Conn::open(srv.port);
    let tag = local_tag(&mut t);
    let k = Keys::new(&tag);
    let mut c = Conn::open(srv.port);
    seed(&mut c, &k);
    txn_writes(&mut t, &mut c, &k, "acked");
    assert_eq!(t.send(&["TXN", "COMMIT"]), OK);
    assert_eq!(c.send(&["SET", &k.other, "after-commit"]), OK);
    // kill -9 with the transaction's connection still open: a disconnect
    // first would roll it back cleanly and hide the crash.
    let srv = crash_restart(srv);
    drop((t, c));
    let mut c = Conn::open(srv.port);
    assert_committed(&mut c, &k, "after-commit", "after kill -9 past the COMMIT");
    drop(c);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_committed_txn_replays_fully_1_shard() {
    committed_txn_replays_fully(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_committed_txn_replays_fully_4_shards() {
    committed_txn_replays_fully(4);
}

/// An aborted transaction (its records plus the abort's compensation, one
/// block) replays to the pre-transaction state; the other client's write
/// stands.
fn aborted_txn_replays_to_pre_state(shards: usize) {
    let dir = tmpdir("abort");
    let srv = start(&Cfg::new(&dir, shards, true));
    let mut t = Conn::open(srv.port);
    let tag = local_tag(&mut t);
    let k = Keys::new(&tag);
    let mut c = Conn::open(srv.port);
    seed(&mut c, &k);
    txn_writes(&mut t, &mut c, &k, "acked");
    assert_eq!(t.send(&["TXN", "ABORT"]), OK);
    assert_pre_txn(&mut c, &k, Some("acked"), "live, after the abort");
    // kill -9 with the transaction's connection still open: a disconnect
    // first would roll it back cleanly and hide the crash.
    let srv = crash_restart(srv);
    drop((t, c));
    let mut c = Conn::open(srv.port);
    assert_pre_txn(&mut c, &k, Some("acked"), "after kill -9 past the ABORT");
    drop(c);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn an_aborted_txn_replays_to_the_pre_txn_state_1_shard() {
    aborted_txn_replays_to_pre_state(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn an_aborted_txn_replays_to_the_pre_txn_state_4_shards() {
    aborted_txn_replays_to_pre_state(4);
}

/// The abort's compensation for a hash with field deadlines is `RESTORE …
/// REPLACE ABSTTL` then one `HPEXPIREAT` per deadline. A crash between them
/// (`MOON_TEST_TXN_ABORT_CRASH_AFTER_RECORDS=1`: the server aborts the
/// process once the first compensating record is on disk) must never bring
/// the hash back without its field deadline.
fn crash_between_restore_and_hpexpireat(shards: usize) {
    let dir = tmpdir("restore-crash");
    let mut cfg = Cfg::new(&dir, shards, true);
    cfg.env
        .push(("MOON_TEST_TXN_ABORT_CRASH_AFTER_RECORDS".into(), "1".into()));
    let srv = start(&cfg);
    let mut t = Conn::open(srv.port);
    let tag = local_tag(&mut t);
    let h = format!("{{{tag}}}:h");
    let mut c = Conn::open(srv.port);
    assert_eq!(c.send(&["HSET", &h, "f1", "v1", "f2", "v2"]), int(2));
    assert_eq!(
        c.send(&["HPEXPIRE", &h, "600000", "FIELDS", "1", "f1"]),
        "*1\r\n:1\r\n"
    );
    assert_eq!(t.send(&["TXN", "BEGIN"]), OK);
    assert_eq!(t.send(&["HSET", &h, "f1", "X", "f3", "Y"]), int(1));
    // The server dies mid-reply: a closed socket is the expected outcome.
    let reply = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        t.send_within(&["TXN", "ABORT"], Duration::from_secs(10))
    }))
    .unwrap_or_else(|_| "<connection closed>".to_string());
    assert_ne!(
        reply, OK,
        "precondition: the crash hook must stop the server between the RESTORE and the \
         HPEXPIREAT (this binary does not have it)"
    );
    let mut srv = srv;
    srv.guard.kill_now();
    drop((t, c));
    let mut cfg = srv.cfg.clone();
    cfg.env.clear();
    drop(srv);
    let srv = start(&cfg);
    let mut c = Conn::open(srv.port);
    assert_eq!(c.send(&["HLEN", &h]), int(2), "the pre-transaction fields");
    assert_eq!(c.send(&["HGET", &h, "f1"]), bulk("v1"));
    let fttl = c.send(&["HPTTL", &h, "FIELDS", "2", "f1", "f2"]);
    assert!(
        fttl.starts_with("*2\r\n:") && fttl.ends_with(":-1\r\n") && !fttl.contains(":-2"),
        "f1 came back without its deadline: {fttl:?}"
    );
    drop(c);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_crash_between_restore_and_hpexpireat_keeps_the_field_deadline_1_shard() {
    crash_between_restore_and_hpexpireat(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_crash_between_restore_and_hpexpireat_keeps_the_field_deadline_4_shards() {
    crash_between_restore_and_hpexpireat(4);
}

/// A torn log: BEGIN and the transaction's first record on disk, its last
/// record torn by the crash, no END. Everything the block wrote is rolled
/// back; what was committed before it stands.
fn torn_log(shards: usize) {
    let dir = tmpdir("torn");
    let srv = start(&Cfg::new(&dir, shards, true));
    let mut t = Conn::open(srv.port);
    let tag = local_tag(&mut t);
    let a = format!("{{{tag}}}:a");
    let b = format!("{{{tag}}}:b");
    let mut c = Conn::open(srv.port);
    assert_eq!(c.send(&["SET", &a, "original"]), OK);
    assert_eq!(c.send(&["SET", &b, "original"]), OK);
    assert_eq!(t.send(&["TXN", "BEGIN"]), OK);
    assert_eq!(t.send(&["SET", &a, "uncommitted"]), OK);
    assert_eq!(t.send(&["SET", &b, "uncommitted-and-torn"]), OK);
    let mut srv = srv;
    srv.guard.kill_now();
    drop((t, c));
    let incr = newest_incr(&dir);
    let len = std::fs::metadata(&incr).unwrap().len();
    let f = std::fs::OpenOptions::new().write(true).open(&incr).unwrap();
    f.set_len(len - 5).unwrap();
    drop(f);
    let cfg = srv.cfg.clone();
    drop(srv);
    let srv = start(&cfg);
    let mut c = Conn::open(srv.port);
    assert_eq!(
        c.send(&["GET", &a]),
        bulk("original"),
        "torn block rolled back"
    );
    assert_eq!(
        c.send(&["GET", &b]),
        bulk("original"),
        "torn record not applied"
    );
    drop(c);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_torn_log_with_begin_and_no_end_is_rolled_back_1_shard() {
    torn_log(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_torn_log_with_begin_and_no_end_is_rolled_back_4_shards() {
    torn_log(4);
}

#[derive(Clone, Copy, Debug)]
enum Ending {
    Commit,
    Abort,
    Crash,
}

/// A transaction open across a BGREWRITEAOF fold: the new base holds the
/// pre-transaction image of its keys and the new incr re-opens the block
/// with their live values, so commit, abort and crash each end right — and
/// writes after the fold still bracket.
fn txn_across_a_fold(shards: usize, ending: Ending) {
    let dir = tmpdir("fold");
    let srv = start(&Cfg::new(&dir, shards, true));
    let mut t = Conn::open(srv.port);
    let tag = local_tag(&mut t);
    let k = Keys::new(&tag);
    let late = format!("{{{tag}}}:late");
    let mut c = Conn::open(srv.port);
    seed(&mut c, &k);
    txn_writes(&mut t, &mut c, &k, "acked");
    bgrewriteaof_and_wait(&mut c);
    // After the fold: more of the transaction, and another client.
    assert_eq!(t.send(&["SET", &late, "uncommitted"]), OK);
    assert_eq!(t.send(&["INCR", &k.ctr]), int(2));
    assert_eq!(c.send(&["SET", &k.other, "after-fold"]), OK);
    match ending {
        Ending::Commit => assert_eq!(t.send(&["TXN", "COMMIT"]), OK),
        Ending::Abort => assert_eq!(t.send(&["TXN", "ABORT"]), OK),
        Ending::Crash => {}
    }
    // kill -9 with the transaction's connection still open: a disconnect
    // first would roll it back cleanly and hide the crash.
    let srv = crash_restart(srv);
    drop((t, c));
    let mut c = Conn::open(srv.port);
    let when = format!("{ending:?} across a fold, then kill -9");
    match ending {
        Ending::Commit => {
            assert_eq!(c.send(&["GET", &k.upd]), bulk("uncommitted"), "{when}");
            assert_eq!(c.send(&["GET", &k.new]), bulk("uncommitted"), "{when}");
            assert_eq!(c.send(&["EXISTS", &k.del]), int(0), "{when}");
            assert_eq!(c.send(&["HGET", &k.hash, "f3"]), bulk("Y"), "{when}");
            assert_eq!(c.send(&["GET", &k.ctr]), bulk("2"), "{when}");
            assert_eq!(c.send(&["GET", &late]), bulk("uncommitted"), "{when}");
        }
        Ending::Abort | Ending::Crash => {
            assert_pre_txn(&mut c, &k, Some("after-fold"), &when);
            assert_eq!(c.send(&["GET", &late]), NIL, "{when}");
        }
    }
    assert_eq!(c.send(&["GET", &k.other]), bulk("after-fold"), "{when}");
    // One more session over the folded generation.
    assert_eq!(c.send(&["SET", &late, "next"]), OK);
    drop(c);
    let srv = crash_restart(srv);
    let mut c = Conn::open(srv.port);
    assert_eq!(
        c.send(&["GET", &late]),
        bulk("next"),
        "{when}: the next session"
    );
    drop(c);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_txn_across_a_rewrite_fold_commits_1_shard() {
    txn_across_a_fold(1, Ending::Commit);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_txn_across_a_rewrite_fold_commits_4_shards() {
    txn_across_a_fold(4, Ending::Commit);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_txn_across_a_rewrite_fold_aborts_1_shard() {
    txn_across_a_fold(1, Ending::Abort);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_txn_across_a_rewrite_fold_aborts_4_shards() {
    txn_across_a_fold(4, Ending::Abort);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_txn_across_a_rewrite_fold_crashes_1_shard() {
    txn_across_a_fold(1, Ending::Crash);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_txn_across_a_rewrite_fold_crashes_4_shards() {
    txn_across_a_fold(4, Ending::Crash);
}

/// A clean stop (SIGTERM) while a transaction is open: the stop rolls it
/// back — logged as its compensation and END, or left as a block the
/// clean-close marker ends — and the restart shows the pre-transaction
/// state; the next session's writes survive another restart.
fn clean_stop_with_a_txn_open(shards: usize) {
    let dir = tmpdir("clean-stop");
    let srv = start(&Cfg::new(&dir, shards, true));
    let mut t = Conn::open(srv.port);
    let tag = local_tag(&mut t);
    let k = Keys::new(&tag);
    let mut c = Conn::open(srv.port);
    seed(&mut c, &k);
    txn_writes(&mut t, &mut c, &k, "acked");
    let mut srv = srv;
    let pid = srv.guard.id();
    let status = Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let deadline = Instant::now() + Duration::from_secs(30);
    while srv.guard.as_mut().try_wait().ok().flatten().is_none() {
        assert!(
            Instant::now() < deadline,
            "SIGTERM never stopped the server"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    drop((t, c));
    let log = aof_bytes(&dir);
    assert!(
        contains(&log, b"MOON.TXN"),
        "the transaction's block is in the log"
    );
    assert!(
        contains(&log, b"CLOSE"),
        "a clean stop ends the log with a clean-close marker"
    );
    let cfg = srv.cfg.clone();
    drop(srv);
    let srv = start(&cfg);
    let mut c = Conn::open(srv.port);
    assert_pre_txn(
        &mut c,
        &k,
        Some("acked"),
        "after a clean stop with the TXN open",
    );
    assert_eq!(c.send(&["SET", &k.upd, "next-session"]), OK);
    drop(c);
    let srv = crash_restart(srv);
    let mut c = Conn::open(srv.port);
    assert_eq!(c.send(&["GET", &k.upd]), bulk("next-session"));
    drop(c);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_clean_stop_with_a_txn_open_rolls_it_back_1_shard() {
    clean_stop_with_a_txn_open(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_clean_stop_with_a_txn_open_rolls_it_back_4_shards() {
    clean_stop_with_a_txn_open(4);
}

/// A script's writes inside a transaction are the transaction's records.
fn script_inside_a_txn(shards: usize) {
    let dir = tmpdir("script");
    let srv = start(&Cfg::new(&dir, shards, true));
    let mut t = Conn::open(srv.port);
    let tag = local_tag(&mut t);
    let a = format!("{{{tag}}}:a");
    let h = format!("{{{tag}}}:h");
    let mut c = Conn::open(srv.port);
    assert_eq!(c.send(&["SET", &a, "original"]), OK);
    assert_eq!(t.send(&["TXN", "BEGIN"]), OK);
    let r = t.send(&[
        "EVAL",
        "redis.call('SET', KEYS[1], 'uncommitted'); redis.call('HSET', KEYS[2], 'f', 'v'); return 1",
        "2",
        &a,
        &h,
    ]);
    assert_eq!(r, int(1));
    // kill -9 with the transaction's connection still open: a disconnect
    // first would roll it back cleanly and hide the crash.
    let srv = crash_restart(srv);
    drop((t, c));
    let mut c = Conn::open(srv.port);
    assert_eq!(c.send(&["GET", &a]), bulk("original"));
    assert_eq!(c.send(&["EXISTS", &h]), int(0));
    drop(c);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_script_inside_a_crashed_txn_is_rolled_back_1_shard() {
    script_inside_a_txn(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_script_inside_a_crashed_txn_is_rolled_back_4_shards() {
    script_inside_a_txn(4);
}

/// With no transaction writing, the AOF holds no `MOON.TXN` record: plain
/// writes, a script, MULTI/EXEC, a rewrite and a transaction that wrote
/// nothing add none.
fn no_txn_no_markers(shards: usize) {
    let dir = tmpdir("no-markers");
    let srv = start(&Cfg::new(&dir, shards, true));
    let mut c = Conn::open(srv.port);
    for i in 0..50 {
        assert_eq!(c.send(&["SET", &format!("k{i}"), "v"]), OK);
        assert_eq!(c.send(&["HSET", &format!("h{i}"), "f", "v"]), int(1));
    }
    assert_eq!(
        c.send(&["EVAL", "redis.call('SET','{s}:a','b'); return 1", "0"]),
        int(1)
    );
    assert_eq!(c.send(&["MULTI"]), OK);
    assert_eq!(c.send(&["INCR", "ctr"]), "+QUEUED\r\n");
    assert_eq!(c.send(&["EXEC"]), "*1\r\n:1\r\n");
    assert_eq!(c.send(&["TXN", "BEGIN"]), OK);
    assert_eq!(c.send(&["GET", "k1"]), bulk("v"));
    assert_eq!(c.send(&["TXN", "COMMIT"]), OK);
    bgrewriteaof_and_wait(&mut c);
    assert_eq!(c.send(&["SET", "after", "v"]), OK);
    drop(c);
    let mut srv = srv;
    srv.guard.kill_now();
    let log = aof_bytes(&dir);
    assert!(!log.is_empty());
    assert!(
        !contains(&log, b"MOON.TXN"),
        "no transaction wrote, no transaction record"
    );
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn no_txn_writes_log_no_txn_records_1_shard() {
    no_txn_no_markers(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn no_txn_writes_log_no_txn_records_4_shards() {
    no_txn_no_markers(4);
}

// ---------------------------------------------------------------------------
// F3 — without an AOF, the snapshot is the durability authority
// ---------------------------------------------------------------------------

/// The snapshot kinds of a sharded server (`SAVE` answers "not supported in
/// sharded mode, use BGSAVE"; `SHUTDOWN SAVE` has its own test below).
#[derive(Clone, Copy, Debug)]
enum Snap {
    Bgsave,
    SaveRule,
}

/// A snapshot taken while a transaction is open stores the pre-transaction
/// image of its keys: an abort, or a crash, after it restarts to the
/// pre-transaction state; a later snapshot after a commit stores the commit.
fn snapshot_mid_txn(shards: usize, snap: Snap, ending: Ending) {
    let dir = tmpdir("snap");
    let mut cfg = Cfg::new(&dir, shards, false);
    if matches!(snap, Snap::SaveRule) {
        cfg.save = "1 1".into();
    }
    let srv = start(&cfg);
    let mut t = Conn::open(srv.port);
    let tag = local_tag(&mut t);
    let k = Keys::new(&tag);
    let mut c = Conn::open(srv.port);
    seed(&mut c, &k);
    txn_writes(&mut t, &mut c, &k, "acked");
    match snap {
        Snap::Bgsave => save_and_wait(&mut c, &["BGSAVE"]),
        // `save 1 1`: the transaction's writes trigger it.
        Snap::SaveRule => save_and_wait(&mut c, &[]),
    }
    match ending {
        Ending::Commit => {
            assert_eq!(t.send(&["TXN", "COMMIT"]), OK);
            // The commit is in the NEXT snapshot.
            save_and_wait(&mut c, &["BGSAVE"]);
        }
        Ending::Abort => assert_eq!(t.send(&["TXN", "ABORT"]), OK),
        Ending::Crash => {}
    }
    // kill -9 with the transaction's connection still open: a disconnect
    // first would roll it back cleanly and hide the crash.
    let srv = crash_restart(srv);
    drop((t, c));
    let mut c = Conn::open(srv.port);
    let when = format!("{snap:?} mid-TXN, {ending:?}, kill -9");
    match ending {
        Ending::Commit => assert_committed(&mut c, &k, "acked", &when),
        Ending::Abort | Ending::Crash => assert_pre_txn(&mut c, &k, Some("acked"), &when),
    }
    drop(c);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_bgsave_mid_txn_then_abort_restarts_pre_txn_1_shard() {
    snapshot_mid_txn(1, Snap::Bgsave, Ending::Abort);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_bgsave_mid_txn_then_abort_restarts_pre_txn_4_shards() {
    snapshot_mid_txn(4, Snap::Bgsave, Ending::Abort);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_bgsave_mid_txn_then_crash_restarts_pre_txn_1_shard() {
    snapshot_mid_txn(1, Snap::Bgsave, Ending::Crash);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_bgsave_mid_txn_then_crash_restarts_pre_txn_4_shards() {
    snapshot_mid_txn(4, Snap::Bgsave, Ending::Crash);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_bgsave_mid_txn_then_commit_keeps_the_commit_in_the_next_save_4_shards() {
    snapshot_mid_txn(4, Snap::Bgsave, Ending::Commit);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_save_rule_snapshot_mid_txn_stores_the_pre_txn_image_1_shard() {
    snapshot_mid_txn(1, Snap::SaveRule, Ending::Crash);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_save_rule_snapshot_mid_txn_stores_the_pre_txn_image_4_shards() {
    snapshot_mid_txn(4, Snap::SaveRule, Ending::Abort);
}

/// `SHUTDOWN SAVE` while a transaction is open: the final image holds the
/// pre-transaction state.
fn shutdown_save_mid_txn(shards: usize) {
    let dir = tmpdir("shutdown-save");
    let srv = start(&Cfg::new(&dir, shards, false));
    let mut t = Conn::open(srv.port);
    let tag = local_tag(&mut t);
    let k = Keys::new(&tag);
    let mut c = Conn::open(srv.port);
    seed(&mut c, &k);
    txn_writes(&mut t, &mut c, &k, "acked");
    // SHUTDOWN answers nothing: the server closes the connection.
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        c.send_within(&["SHUTDOWN", "SAVE"], Duration::from_secs(30))
    }));
    let mut srv = srv;
    let deadline = Instant::now() + Duration::from_secs(30);
    while srv.guard.as_mut().try_wait().ok().flatten().is_none() {
        assert!(
            Instant::now() < deadline,
            "SHUTDOWN SAVE never stopped the server"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    drop((t, c));
    let cfg = srv.cfg.clone();
    drop(srv);
    let srv = start(&cfg);
    let mut c = Conn::open(srv.port);
    assert_pre_txn(&mut c, &k, Some("acked"), "SHUTDOWN SAVE with the TXN open");
    drop(c);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_shutdown_save_mid_txn_stores_the_pre_txn_image_1_shard() {
    shutdown_save_mid_txn(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_shutdown_save_mid_txn_stores_the_pre_txn_image_4_shards() {
    shutdown_save_mid_txn(4);
}

// ---------------------------------------------------------------------------
// Replicas (master-side PSYNC: monoio)
// ---------------------------------------------------------------------------

fn no_master_psync() -> bool {
    std::env::var_os("MOON_TEST_NO_MASTER_PSYNC").is_some()
}

fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    let start = Instant::now();
    while !cond() {
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "timed out waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn settle(master: u16, replica: u16) {
    let marker = format!(
        "m{}",
        Instant::now().elapsed().as_nanos() ^ u128::from(std::process::id())
    );
    let marker = format!("{marker}-{}", rand_suffix());
    assert_eq!(Conn::open(master).send(&["SET", "settle", &marker]), OK);
    let want = bulk(&marker);
    wait_for("the replica to apply the stream", || {
        Conn::open(replica).send(&["GET", "settle"]) == want
    });
}

fn rand_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

#[derive(Clone, Copy, Debug)]
enum ReplEnding {
    Commit,
    Abort,
    /// The master dies inside the transaction; the replica is promoted.
    MasterDiesThenPromote,
}

/// `attach_mid_txn`: the replica's full sync happens while the transaction
/// is open (its earlier writes reach the replica through the sync image);
/// otherwise the replica is attached before the transaction begins.
fn replica_txn(attach_mid_txn: bool, ending: ReplEnding) {
    if no_master_psync() {
        eprintln!("MOON_TEST_NO_MASTER_PSYNC set: replica test skipped");
        return;
    }
    let dm = tmpdir("repl-m");
    let ds = tmpdir("repl-s");
    let master = start(&Cfg::new(&dm, 1, false));
    let mut replica = start(&Cfg::new(&ds, 1, false));
    let attach = |replica: &Srv| {
        let r =
            Conn::open(replica.port).send(&["REPLICAOF", "127.0.0.1", &master.port.to_string()]);
        assert!(r.starts_with('+'), "REPLICAOF answered {r:?}");
        wait_for("the replica link", || {
            Conn::open(replica.port)
                .send(&["INFO", "replication"])
                .contains("master_link_status:up")
        });
    };
    let mut t = Conn::open(master.port);
    let tag = local_tag(&mut t);
    let k = Keys::new(&tag);
    let mut c = Conn::open(master.port);
    seed(&mut c, &k);
    if !attach_mid_txn {
        attach(&replica);
    }
    txn_writes(&mut t, &mut c, &k, "acked");
    if attach_mid_txn {
        attach(&replica);
    }
    settle(master.port, replica.port);
    let when = format!(
        "replica (attached {}), {ending:?}",
        if attach_mid_txn { "mid-TXN" } else { "before" }
    );
    match ending {
        ReplEnding::Commit => {
            assert_eq!(t.send(&["TXN", "COMMIT"]), OK);
            settle(master.port, replica.port);
            let mut r = Conn::open(replica.port);
            assert_committed(&mut r, &k, "acked", &when);
        }
        ReplEnding::Abort => {
            assert_eq!(t.send(&["TXN", "ABORT"]), OK);
            settle(master.port, replica.port);
            let mut r = Conn::open(replica.port);
            assert_pre_txn(&mut r, &k, Some("acked"), &when);
        }
        ReplEnding::MasterDiesThenPromote => {
            let mut master = master;
            master.guard.kill_now();
            let r = Conn::open(replica.port).send(&["REPLICAOF", "NO", "ONE"]);
            assert_eq!(r, OK);
            let mut r = Conn::open(replica.port);
            wait_for("the promotion's rollback", || {
                r.send(&["GET", &k.upd]) == bulk("original")
            });
            assert_pre_txn(&mut r, &k, Some("acked"), &when);
            drop(master);
        }
    }
    replica.guard.kill_now();
    let _ = std::fs::remove_dir_all(&dm);
    let _ = std::fs::remove_dir_all(&ds);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned (monoio)"]
fn a_replica_full_sync_mid_txn_then_commit_agrees() {
    replica_txn(true, ReplEnding::Commit);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned (monoio)"]
fn a_replica_full_sync_mid_txn_then_abort_agrees() {
    replica_txn(true, ReplEnding::Abort);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned (monoio)"]
fn a_replica_full_sync_mid_txn_then_master_death_and_promotion_rolls_back() {
    replica_txn(true, ReplEnding::MasterDiesThenPromote);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned (monoio)"]
fn a_replica_promoted_after_its_master_dies_inside_a_txn_rolls_it_back() {
    replica_txn(false, ReplEnding::MasterDiesThenPromote);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned (monoio)"]
fn a_replica_attached_before_the_txn_agrees_on_commit() {
    replica_txn(false, ReplEnding::Commit);
}

// ---------------------------------------------------------------------------
// Downgrade (MOON_DOWNGRADE_BIN: a binary that predates moon#1300)
// ---------------------------------------------------------------------------

/// An older binary skips `MOON.TXN` records as unknown commands: a committed
/// transaction replays fully. It then also replays an UNTERMINATED block's
/// records — the documented downgrade caveat (`docs/STORAGE-FORMAT-V1.md`
/// §3.3): stop the new binary cleanly (or let it boot once) before
/// downgrading.
fn downgrade_read(shards: usize) {
    let Some(old) = std::env::var_os("MOON_DOWNGRADE_BIN").map(PathBuf::from) else {
        eprintln!("MOON_DOWNGRADE_BIN unset: downgrade-read test skipped");
        return;
    };
    let dir = tmpdir("downgrade");
    let srv = start(&Cfg::new(&dir, shards, true));
    let mut t = Conn::open(srv.port);
    let tag = local_tag(&mut t);
    let k = Keys::new(&tag);
    let mut c = Conn::open(srv.port);
    seed(&mut c, &k);
    txn_writes(&mut t, &mut c, &k, "acked");
    assert_eq!(t.send(&["TXN", "COMMIT"]), OK);
    let open = format!("{{{tag}}}:open");
    assert_eq!(t.send(&["TXN", "BEGIN"]), OK);
    assert_eq!(t.send(&["SET", &open, "uncommitted"]), OK);
    let mut srv = srv;
    srv.guard.kill_now();
    drop((t, c));
    assert!(contains(&aof_bytes(&dir), b"MOON.TXN"));
    let mut cfg = srv.cfg.clone();
    drop(srv);
    cfg.bin = old;
    let srv = start(&cfg);
    let mut c = Conn::open(srv.port);
    assert_committed(&mut c, &k, "acked", "an older binary reading the log");
    assert_eq!(
        c.send(&["GET", &open]),
        bulk("uncommitted"),
        "the documented caveat: an older binary replays an unterminated block"
    );
    drop(c);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "real-server: run with --include-ignored, MOON_BIN and MOON_DOWNGRADE_BIN pinned"]
fn an_older_binary_skips_txn_records_1_shard() {
    downgrade_read(1);
}

#[test]
#[ignore = "real-server: run with --include-ignored, MOON_BIN and MOON_DOWNGRADE_BIN pinned"]
fn an_older_binary_skips_txn_records_4_shards() {
    downgrade_read(4);
}
