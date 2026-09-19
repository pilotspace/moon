//! moon#894 — scripts queued inside MULTI must RUN at EXEC.
//!
//! Before the fix, `EVAL`/`EVALSHA`/`EVAL_RO`/`FCALL` inside `MULTI` answered
//! `+QUEUED` and then `-ERR unknown command` at `EXEC`, while the rest of the
//! transaction committed. Exactly one step of an otherwise successful
//! transaction was silently skipped.
//!
//! Every expected reply below was measured against redis-server 8.6.1 over raw
//! RESP, one fresh connection per case. Where moon's error TEXT legitimately
//! differs (the Lua error wording), only the error class is asserted.
//!
//! Covered at `--shards 1` and `--shards 4`. At 4, every case runs over eight
//! `{tag}`s, so the body executes both on the connection's own shard and
//! routed to the owner (`ShardMessage::TxnExecute`). Durability is checked by
//! SIGKILL and restart under `appendfsync always`: a script's effects must
//! replay in body order and exactly once.
//!
//! Pin the binary: `MOON_BIN=/path/to/moon cargo test --test scripts_in_multi_894`.

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use common::Conn;

const SET_SCRIPT: &str = "return redis.call('SET',KEYS[1],ARGV[1])";
const APPEND_X: &str = "return redis.call('APPEND',KEYS[1],'x')";
const INCR_SCRIPT: &str = "return redis.call('INCR',KEYS[1])";
const PARTIAL_THEN_ERROR: &str =
    "redis.call('SET',KEYS[1],'partial'); return redis.error_reply('boom')";
const GET_SCRIPT: &str = "return redis.call('GET',KEYS[1])";
const LIB: &str = "#!lua name=moon894\n\
redis.register_function('setit894', function(keys, args) return redis.call('SET', keys[1], args[1]) end)\n\
redis.register_function('incrit894', function(keys, args) return redis.call('INCR', keys[1]) end)\n\
redis.register_function{function_name='getit894', callback=function(keys, args) return redis.call('GET', keys[1]) end, flags={'no-writes'}}";

const TAGS_4: [&str; 8] = ["a", "b", "c", "d", "e", "f", "g", "h"];

fn spawn_moon(dir: &Path, port: u16, shards: usize, aof: bool) -> Child {
    std::fs::create_dir_all(dir).expect("create --dir");
    let mut cmd = Command::new(common::find_moon_binary());
    cmd.args([
        "--port",
        &port.to_string(),
        "--shards",
        &shards.to_string(),
        "--dir",
        dir.to_str().expect("utf-8 dir"),
        "--disk-free-min-pct",
        "0",
    ]);
    if aof {
        cmd.args(["--appendonly", "yes", "--appendfsync", "always"]);
    } else {
        cmd.args(["--appendonly", "no"]);
    }
    cmd.stdout(std::process::Stdio::null())
        .stderr(common::server_stderr(dir))
        .spawn()
        .expect("spawn moon (set MOON_BIN to a built binary)")
}

fn start(dir: &Path, shards: usize, aof: bool) -> (common::ServerGuard, u16) {
    let (guard, port) = common::spawn_listening_guarded(|p| spawn_moon(dir, p, shards, aof));
    await_serving(port);
    (guard, port)
}

/// Wait until the server answers a keyspace read, not merely a TCP connect —
/// after a restart it refuses reads with `-LOADING` while it replays.
fn await_serving(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let r = Conn::open(port).send(&["EXISTS", "moon894:probe"]);
        if r.starts_with(':') {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "server never served reads: {r:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Run `cmds` on ONE fresh connection and return every raw reply.
fn run(port: u16, cmds: &[&[&str]]) -> Vec<String> {
    let mut c = Conn::open(port);
    cmds.iter().map(|cmd| c.send(cmd)).collect()
}

/// `MULTI`, the body, `EXEC` on one connection; returns the raw EXEC reply
/// after checking every body command was `+QUEUED`.
fn multi_exec(port: u16, body: &[&[&str]]) -> String {
    let mut c = Conn::open(port);
    assert_eq!(c.send(&["MULTI"]), "+OK\r\n");
    for cmd in body {
        assert_eq!(c.send(cmd), "+QUEUED\r\n", "queueing {cmd:?}");
    }
    c.send(&["EXEC"])
}

fn get(port: u16, key: &str) -> String {
    Conn::open(port).send(&["GET", key])
}

fn bulk(v: &str) -> String {
    format!("${}\r\n{v}\r\n", v.len())
}

fn tags(shards: usize) -> Vec<String> {
    if shards == 1 {
        vec!["s1".to_string()]
    } else {
        TAGS_4.iter().map(|t| t.to_string()).collect()
    }
}

fn scratch(prefix: &str) -> PathBuf {
    common::unique_test_dir(prefix)
}

/// The issue's own shape, plus ordering: the script's write lands, at its
/// position, and the commands around it still run.
fn check_script_runs_in_body_order(port: u16, t: &str) {
    let k3 = format!("{{{t}}}k3");
    run(port, &[&["DEL", &k3]]);
    assert_eq!(
        multi_exec(port, &[&["EVAL", SET_SCRIPT, "1", &k3, "v3"]]),
        "*1\r\n+OK\r\n",
        "[{t}] EVAL inside MULTI must run at EXEC"
    );
    assert_eq!(
        get(port, &k3),
        bulk("v3"),
        "[{t}] the script's write must apply"
    );

    let (b, m, a) = (
        format!("{{{t}}}before"),
        format!("{{{t}}}mid"),
        format!("{{{t}}}after"),
    );
    run(port, &[&["DEL", &b, &m, &a]]);
    assert_eq!(
        multi_exec(
            port,
            &[
                &["SET", &b, "1"],
                &["EVAL", SET_SCRIPT, "1", &m, "m"],
                &["SET", &a, "2"],
            ]
        ),
        "*3\r\n+OK\r\n+OK\r\n+OK\r\n",
        "[{t}] mixed transaction"
    );
    assert_eq!(
        run(port, &[&["MGET", &b, &m, &a]])[0],
        "*3\r\n$1\r\n1\r\n$1\r\nm\r\n$1\r\n2\r\n",
        "[{t}] no step of the transaction may be skipped"
    );

    let o = format!("{{{t}}}o");
    run(port, &[&["DEL", &o]]);
    assert_eq!(
        multi_exec(
            port,
            &[
                &["SET", &o, "1"],
                &["EVAL", APPEND_X, "1", &o],
                &["APPEND", &o, "y"],
            ]
        ),
        "*3\r\n+OK\r\n:2\r\n:3\r\n",
        "[{t}] the script must see the write queued before it"
    );
    assert_eq!(get(port, &o), bulk("1xy"), "[{t}] body order");
}

fn check_evalsha_and_ro(port: u16, t: &str) {
    let sha = run(port, &[&["SCRIPT", "LOAD", SET_SCRIPT]])[0]
        .lines()
        .nth(1)
        .expect("SCRIPT LOAD sha")
        .to_string();
    let k = format!("{{{t}}}sha");
    run(port, &[&["DEL", &k]]);
    assert_eq!(
        multi_exec(port, &[&["EVALSHA", &sha, "1", &k, "x"]]),
        "*1\r\n+OK\r\n",
        "[{t}] EVALSHA after SCRIPT LOAD"
    );
    assert_eq!(get(port, &k), bulk("x"));

    let unknown = multi_exec(
        port,
        &[&[
            "EVALSHA",
            "ffffffffffffffffffffffffffffffffffffffff",
            "1",
            &k,
            "y",
        ]],
    );
    assert!(
        unknown.starts_with("*1\r\n-NOSCRIPT"),
        "[{t}] unknown sha: {unknown:?}"
    );

    let ro = format!("{{{t}}}ro");
    run(port, &[&["DEL", &ro]]);
    let refused = multi_exec(port, &[&["EVAL_RO", SET_SCRIPT, "1", &ro, "x"]]);
    assert!(
        refused.starts_with("*1\r\n-ERR"),
        "[{t}] EVAL_RO must refuse a write: {refused:?}"
    );
    assert_eq!(run(port, &[&["EXISTS", &ro]])[0], ":0\r\n");
    run(port, &[&["SET", &ro, "val"]]);
    assert_eq!(
        multi_exec(port, &[&["EVAL_RO", GET_SCRIPT, "1", &ro]]),
        format!("*1\r\n{}", bulk("val")),
        "[{t}] EVAL_RO read"
    );
}

fn check_fcall(port: u16, t: &str) {
    let loaded = run(port, &[&["FUNCTION", "LOAD", "REPLACE", LIB]]);
    assert_eq!(loaded[0], bulk("moon894"), "FUNCTION LOAD: {loaded:?}");
    let f = format!("{{{t}}}f");
    run(port, &[&["DEL", &f]]);
    assert_eq!(
        multi_exec(port, &[&["FCALL", "setit894", "1", &f, "fv"]]),
        "*1\r\n+OK\r\n",
        "[{t}] FCALL after FUNCTION LOAD"
    );
    assert_eq!(get(port, &f), bulk("fv"));
    assert_eq!(
        multi_exec(port, &[&["FCALL_RO", "getit894", "1", &f]]),
        format!("*1\r\n{}", bulk("fv")),
        "[{t}] FCALL_RO"
    );
    let missing = multi_exec(port, &[&["FCALL", "nosuchfn894", "1", &f]]);
    assert!(
        missing.starts_with("*1\r\n-ERR"),
        "[{t}] unknown function: {missing:?}"
    );
}

fn check_script_error_mid_transaction(port: u16, t: &str) {
    let (e, a) = (format!("{{{t}}}e"), format!("{{{t}}}ea"));
    run(port, &[&["DEL", &e, &a]]);
    let reply = multi_exec(
        port,
        &[&["EVAL", PARTIAL_THEN_ERROR, "1", &e], &["SET", &a, "1"]],
    );
    assert!(
        reply.starts_with("*2\r\n-") && reply.contains("boom") && reply.ends_with("+OK\r\n"),
        "[{t}] a failing script is one error element; the rest still runs: {reply:?}"
    );
    // Redis does not roll a script back: the write before the error stays.
    assert_eq!(
        run(port, &[&["MGET", &e, &a]])[0],
        "*2\r\n$7\r\npartial\r\n$1\r\n1\r\n"
    );
    assert_eq!(
        multi_exec(port, &[&["EVAL", "return 42", "0"]]),
        "*1\r\n:42\r\n",
        "[{t}] keyless script"
    );
}

fn check_watch_aborts_before_the_script(port: u16, t: &str) {
    let w = format!("{{{t}}}w");
    run(port, &[&["SET", &w, "orig"]]);
    let mut c = Conn::open(port);
    assert_eq!(c.send(&["WATCH", &w]), "+OK\r\n");
    run(port, &[&["SET", &w, "changed"]]);
    assert_eq!(c.send(&["MULTI"]), "+OK\r\n");
    assert_eq!(
        c.send(&["EVAL", SET_SCRIPT, "1", &w, "script"]),
        "+QUEUED\r\n"
    );
    assert_eq!(c.send(&["EXEC"]), "*-1\r\n", "[{t}] WATCH conflict aborts");
    assert_eq!(
        get(port, &w),
        bulk("changed"),
        "[{t}] the script must not run"
    );
}

fn run_all(shards: usize) {
    let dir = scratch(&format!("moon-894-s{shards}"));
    let (_srv, port) = start(&dir, shards, false);
    for t in tags(shards) {
        check_script_runs_in_body_order(port, &t);
        check_evalsha_and_ro(port, &t);
        check_fcall(port, &t);
        check_script_error_mid_transaction(port, &t);
        check_watch_aborts_before_the_script(port, &t);
    }
}

#[test]
fn scripts_in_multi_run_at_exec_1_shard() {
    run_all(1);
}

#[test]
fn scripts_in_multi_run_at_exec_4_shards() {
    run_all(4);
}

/// A body whose plain commands and script keys live on different shards is
/// refused whole, before anything runs — the moon#247 rule, with no exception
/// for scripts.
#[test]
fn cross_shard_body_with_a_script_is_refused_whole() {
    let dir = scratch("moon-894-xs");
    let (_srv, port) = start(&dir, 4, false);
    // Find two tags the server itself calls cross-shard for a plain body.
    let mut pair = None;
    'outer: for a in TAGS_4 {
        for b in TAGS_4 {
            if a == b {
                continue;
            }
            let (ka, kb) = (format!("{{{a}}}probe"), format!("{{{b}}}probe"));
            if multi_exec(port, &[&["SET", &ka, "1"], &["SET", &kb, "1"]]).starts_with("-CROSSSLOT")
            {
                pair = Some((a, b));
                break 'outer;
            }
        }
    }
    let (a, b) = pair.expect("eight tags over four shards must include a cross-shard pair");
    let (ka, kb) = (format!("{{{a}}}xs"), format!("{{{b}}}xs"));
    run(port, &[&["DEL", &ka], &["DEL", &kb]]);
    let reply = multi_exec(
        port,
        &[&["SET", &ka, "1"], &["EVAL", SET_SCRIPT, "1", &kb, "v"]],
    );
    assert!(reply.starts_with("-CROSSSLOT"), "got {reply:?}");
    assert_eq!(run(port, &[&["EXISTS", &ka]])[0], ":0\r\n");
    assert_eq!(run(port, &[&["EXISTS", &kb]])[0], ":0\r\n");
}

/// The script's effects reach the AOF in body order and exactly once.
///
/// `SET o 1; EVAL APPEND o x; APPEND o y` must replay to `1xy`, which it
/// cannot if the script's record is logged ahead of the `SET` queued before
/// it. `EVAL INCR` then `FCALL INCR` must replay to 2, which they cannot if a
/// raw `FCALL` frame is logged next to its effect.
fn durability(shards: usize) {
    let dir = scratch(&format!("moon-894-aof-s{shards}"));
    let tags = tags(shards);
    {
        let (mut srv, port) = start(&dir, shards, true);
        run(port, &[&["FUNCTION", "LOAD", "REPLACE", LIB]]);
        for t in &tags {
            let (o, n) = (format!("{{{t}}}o"), format!("{{{t}}}n"));
            assert_eq!(
                multi_exec(
                    port,
                    &[
                        &["SET", &o, "1"],
                        &["EVAL", APPEND_X, "1", &o],
                        &["APPEND", &o, "y"],
                        &["EVAL", INCR_SCRIPT, "1", &n],
                        &["FCALL", "incrit894", "1", &n],
                    ]
                ),
                "*5\r\n+OK\r\n:2\r\n:3\r\n:1\r\n:2\r\n",
                "[{t}] body before the crash"
            );
        }
        srv.kill_now();
    }
    let (_srv, port) = start(&dir, shards, true);
    for t in &tags {
        assert_eq!(
            get(port, &format!("{{{t}}}o")),
            bulk("1xy"),
            "[{t}] after SIGKILL + restart the script's write must replay in body order"
        );
        assert_eq!(
            get(port, &format!("{{{t}}}n")),
            bulk("2"),
            "[{t}] after SIGKILL + restart each script effect must replay exactly once"
        );
    }
}

#[test]
fn script_effects_in_exec_survive_restart_in_order_1_shard() {
    durability(1);
}

#[test]
fn script_effects_in_exec_survive_restart_in_order_4_shards() {
    durability(4);
}

/// The script's effects reach a replica, in body order. `#[ignore]`d like
/// the other replication suites; run with `-- --ignored`.
#[test]
#[ignore]
fn script_effects_in_exec_reach_the_replica_in_order() {
    let (mdir, rdir) = (scratch("moon-894-master"), scratch("moon-894-replica"));
    let (_m, mport) = start(&mdir, 1, false);
    let (_r, rport) = start(&rdir, 1, false);
    assert_eq!(
        run(rport, &[&["REPLICAOF", "127.0.0.1", &mport.to_string()]])[0],
        "+OK\r\n"
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while !run(rport, &[&["INFO", "replication"]])[0].contains("master_link_status:up") {
        assert!(Instant::now() < deadline, "replica never linked");
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(
        multi_exec(
            mport,
            &[
                &["SET", "o", "1"],
                &["EVAL", APPEND_X, "1", "o"],
                &["APPEND", "o", "y"],
                &["EVAL", INCR_SCRIPT, "1", "n"],
            ]
        ),
        "*4\r\n+OK\r\n:2\r\n:3\r\n:1\r\n"
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let (o, n) = (get(rport, "o"), get(rport, "n"));
        if o == bulk("1xy") && n == bulk("1") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "replica diverged from master: o={o:?} n={n:?} (want 1xy / 1)"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}
