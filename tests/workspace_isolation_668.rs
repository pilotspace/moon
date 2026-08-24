//! A workspace-bound connection must not reach the global keyspace — moon#668.
//!
//! These drive a real server over RESP, so they fence the property README
//! sells ("multi-tenant isolation that's actually enforced") rather than the
//! shape of one function. `tests/workspace_key_walker_668.rs` is the unit-level
//! half: it proves the workspace walker and the shared walker name the same
//! positions. This half proves what that buys — that a key the old walker
//! missed really did land in the global keyspace, visible to and clobberable by
//! every other tenant.
//!
//! Every case below is written to FAIL against the pre-moon#668 binary. The
//! discriminator is always a THIRD, unbound connection: it sees the global
//! keyspace, so a key that escaped a workspace shows up there and a key that
//! stayed put does not.

mod common;

use common::Conn;

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
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-ws668-{port}"));
        let _ = std::fs::create_dir_all(&tmp_dir);
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
                "--dir",
                tmp_dir.to_str().unwrap_or("/tmp"),
            ])
            .stdout(Stdio::null())
            .stderr(
                std::fs::File::create(tmp_dir.join("moon.stderr")).expect("create moon stderr log"),
            )
            .spawn()
            .expect("spawn moon")
    });
    let tmp_dir = std::env::temp_dir().join(format!("moon-ws668-{port}"));
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
                    return moon;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let log = std::fs::read_to_string(moon.tmp_dir.join("moon.stderr")).unwrap_or_default();
    panic!("moon never became ready on port {port}\n--- stderr ---\n{log}");
}

/// A fresh connection bound to a NEW workspace, plus that workspace's hash tag
/// (`{<32 hex>}:`) — the literal prefix its keys carry in the shared keyspace.
fn bound_with_prefix(port: u16, name: &str) -> (Conn, String) {
    let mut c = Conn::open(port);
    let created = c.send(&["WS", "CREATE", name]);
    let id = created
        .strip_prefix('$')
        .and_then(|r| r.split_once("\r\n"))
        .map(|(_, rest)| rest.trim_end_matches("\r\n").to_string())
        .unwrap_or_else(|| panic!("WS CREATE {name} answered {created:?}"));
    assert_eq!(
        c.send(&["WS", "AUTH", &id]),
        "+OK\r\n",
        "WS AUTH {id} (workspace {name})"
    );
    let prefix = format!("{{{}}}:", id.replace('-', ""));
    (c, prefix)
}

/// A fresh connection bound to a NEW workspace.
fn bound(port: u16, name: &str) -> Conn {
    bound_with_prefix(port, name).0
}

/// Walk `SCAN` to completion and return every page concatenated.
///
/// A single `SCAN 0` may legitimately return an empty page with a non-zero
/// cursor — asserting on one page would be a coin flip, not a test.
fn scan_all(c: &mut Conn) -> String {
    let mut cursor = "0".to_string();
    let mut pages = String::new();
    for _ in 0..64 {
        let reply = c.send(&["SCAN", &cursor]);
        pages.push_str(&reply);
        // `*2\r\n$<n>\r\n<cursor>\r\n<the array of names>`
        let after_header = reply.split_once("\r\n").map_or("", |(_, rest)| rest);
        let next = after_header.split("\r\n").nth(1).unwrap_or("0").to_string();
        if next == "0" {
            return pages;
        }
        cursor = next;
    }
    panic!("SCAN never returned to cursor 0 after 64 pages:\n{pages}")
}

/// Every case is `(label, setup-in-the-workspace, the key that must not escape)`.
fn assert_no_global_key(global: &mut Conn, key: &str, label: &str) {
    assert_eq!(
        global.send(&["EXISTS", key]),
        ":0\r\n",
        "moon#668 — {label} wrote '{key}' into the GLOBAL keyspace from inside a \
         workspace, where every other tenant can read and overwrite it"
    );
}

// ---------------------------------------------------------------------------
// Writes that escaped
// ---------------------------------------------------------------------------

#[test]
fn ws668_1_store_destinations_stay_inside_the_workspace() {
    for shards in ["1", "4"] {
        let m = spawn_moon(shards);
        let mut a = bound(m.port, &format!("store-{shards}"));
        let mut global = Conn::open(m.port);

        // BITOP: `args[0]` is the OPERATION, not a key — the old walker
        // prefixed it and left dest and both sources global.
        a.send(&["SET", "b1", "abc"]);
        a.send(&["SET", "b2", "abd"]);
        let bitop = a.send(&["BITOP", "AND", "bdst", "b1", "b2"]);
        assert!(
            bitop.starts_with(':'),
            "BITOP must run inside a workspace (its operation name is not a key); got {bitop:?}"
        );
        assert_eq!(a.send(&["EXISTS", "bdst"]), ":1\r\n");
        assert_no_global_key(&mut global, "bdst", "BITOP");

        // SORT ... STORE — a keyword-positional destination.
        a.send(&["RPUSH", "slist", "3", "1", "2"]);
        assert_eq!(a.send(&["SORT", "slist", "STORE", "sdst"]), ":3\r\n");
        assert_eq!(a.send(&["EXISTS", "sdst"]), ":1\r\n");
        assert_no_global_key(&mut global, "sdst", "SORT ... STORE");

        // ZRANGESTORE / GEOSEARCHSTORE — `args[0]` was prefixed BY ACCIDENT
        // (it happens to be the destination); the SOURCE was not.
        a.send(&["ZADD", "zsrc", "1", "m1", "2", "m2"]);
        assert_eq!(
            a.send(&["ZRANGESTORE", "zdst", "zsrc", "0", "-1"]),
            ":2\r\n"
        );
        assert_eq!(a.send(&["EXISTS", "zdst"]), ":1\r\n");
        assert_no_global_key(&mut global, "zdst", "ZRANGESTORE");

        a.send(&["GEOADD", "gsrc", "13.361389", "38.115556", "palermo"]);
        let gss = a.send(&[
            "GEOSEARCHSTORE",
            "gdst",
            "gsrc",
            "FROMMEMBER",
            "palermo",
            "BYRADIUS",
            "200",
            "km",
            "ASC",
        ]);
        assert!(
            gss.starts_with(':'),
            "GEOSEARCHSTORE must find its SOURCE inside the workspace; got {gss:?}"
        );
        assert_eq!(a.send(&["EXISTS", "gdst"]), ":1\r\n");
        assert_no_global_key(&mut global, "gdst", "GEOSEARCHSTORE");

        // GEORADIUS ... STORE — the destination the moon#645 comment admitted
        // was not handled.
        let gr = a.send(&[
            "GEORADIUS",
            "gsrc",
            "13.361389",
            "38.115556",
            "200",
            "km",
            "STORE",
            "grdst",
        ]);
        assert!(
            gr.starts_with(':'),
            "GEORADIUS ... STORE must run inside a workspace; got {gr:?}"
        );
        assert_no_global_key(&mut global, "grdst", "GEORADIUS ... STORE");

        // PFMERGE — destination and sources.
        a.send(&["PFADD", "h1", "x"]);
        a.send(&["PFADD", "h2", "y"]);
        assert_eq!(a.send(&["PFMERGE", "hdst", "h1", "h2"]), "+OK\r\n");
        assert_no_global_key(&mut global, "hdst", "PFMERGE");
    }
}

/// A Lua script's `KEYS[]` were not prefixed AT ALL: `EVAL` sat in the walker's
/// no-key list, so every key a script was handed addressed the global keyspace.
#[test]
fn ws668_2_script_keys_stay_inside_the_workspace() {
    let m = spawn_moon("1");
    let mut a = bound(m.port, "scripts");
    let mut global = Conn::open(m.port);

    let reply = a.send(&[
        "EVAL",
        "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])",
        "1",
        "luakey",
        "inside",
    ]);
    assert_eq!(reply, "$6\r\ninside\r\n");
    assert_eq!(a.send(&["GET", "luakey"]), "$6\r\ninside\r\n");
    assert_no_global_key(&mut global, "luakey", "EVAL KEYS[1]");
}

// ---------------------------------------------------------------------------
// Reads that escaped
// ---------------------------------------------------------------------------

/// Keys AFTER the first fell through to the single-key default, so a
/// workspace-bound client could read the global keyspace by naming a global key
/// in any position but the first.
#[test]
fn ws668_3_secondary_key_positions_cannot_read_the_global_keyspace() {
    let m = spawn_moon("1");
    let mut global = Conn::open(m.port);
    assert_eq!(global.send(&["SET", "gsecret", "leaked"]), "+OK\r\n");

    let mut a = bound(m.port, "secondary");
    // Neither key exists INSIDE the workspace, so a correct TOUCH answers 0.
    // Before moon#668 only `args[0]` was prefixed, so `gsecret` resolved to the
    // global key and TOUCH answered 1 — a read oracle across the tenant
    // boundary.
    assert_eq!(
        a.send(&["TOUCH", "mine", "gsecret"]),
        ":0\r\n",
        "moon#668 — a workspace-bound TOUCH reached the GLOBAL 'gsecret'"
    );
    assert_eq!(
        a.send(&["EXISTS", "mine", "gsecret"]),
        ":0\r\n",
        "moon#668 — a workspace-bound EXISTS reached the GLOBAL 'gsecret'"
    );
}

/// `OBJECT`/`XINFO`/`MEMORY` put the key BEHIND a subcommand, and the walker's
/// no-key list swallowed all three — so they answered about the global key.
#[test]
fn ws668_4_subcommand_containers_answer_about_the_workspaces_key() {
    let m = spawn_moon("1");
    let mut global = Conn::open(m.port);
    // A long string encodes as `raw`; a small integer encodes as `int`. The two
    // keys share a NAME and differ in encoding, so the reply says which one was
    // read.
    assert_eq!(global.send(&["SET", "shape", &"x".repeat(64)]), "+OK\r\n");

    let mut a = bound(m.port, "containers");
    assert_eq!(a.send(&["SET", "shape", "1234"]), "+OK\r\n");
    let enc = a.send(&["OBJECT", "ENCODING", "shape"]);
    assert!(
        enc.contains("int"),
        "moon#668 — OBJECT ENCODING answered about the GLOBAL 'shape' (a long \
         string) instead of the workspace's (an integer); got {enc:?}"
    );
    let usage = a.send(&["MEMORY", "USAGE", "shape"]);
    assert!(
        usage.starts_with(':'),
        "MEMORY USAGE must find the workspace's key; got {usage:?}"
    );
}

/// An unscoped `SCAN` walks the whole keyspace. Before moon#668 the single-key
/// default prefixed the CURSOR instead, so SCAN did not leak — it did not work.
/// Making it work must not make it leak.
#[test]
fn ws668_5_scan_sees_only_its_own_workspace() {
    let m = spawn_moon("1");
    let mut global = Conn::open(m.port);
    assert_eq!(global.send(&["SET", "globalkey", "g"]), "+OK\r\n");

    let mut b = bound(m.port, "scan-b");
    assert_eq!(b.send(&["SET", "bkey", "b"]), "+OK\r\n");

    let mut a = bound(m.port, "scan-a");
    assert_eq!(a.send(&["SET", "akey", "a"]), "+OK\r\n");

    let scan = scan_all(&mut a);
    assert!(
        scan.contains("akey"),
        "SCAN must return the workspace's own keys; got {scan:?}"
    );
    assert!(
        !scan.contains("bkey") && !scan.contains("globalkey"),
        "moon#668 — SCAN inside a workspace enumerated another tenant's keys: {scan:?}"
    );
    // And the names come back UNPREFIXED, like KEYS.
    assert!(
        !scan.contains('{'),
        "SCAN must strip the workspace prefix from the names it returns: {scan:?}"
    );
}

// ---------------------------------------------------------------------------
// Commands the invented position simply BROKE
// ---------------------------------------------------------------------------

/// `args[0]` of a numkeys-counted command is a COUNT. Prefixing it made the
/// count unparseable, so these commands could not run inside a workspace at
/// all.
#[test]
fn ws668_6_numkeys_commands_run_inside_a_workspace() {
    let m = spawn_moon("1");
    let mut a = bound(m.port, "numkeys");
    a.send(&["ZADD", "z1", "1", "m1"]);
    a.send(&["ZADD", "z2", "2", "m2"]);
    a.send(&["RPUSH", "l1", "v"]);

    assert_eq!(a.send(&["ZUNIONSTORE", "zdst", "2", "z1", "z2"]), ":2\r\n");
    assert_eq!(a.send(&["ZINTERCARD", "2", "z1", "z2"]), ":0\r\n");
    let zu = a.send(&["ZUNION", "2", "z1", "z2"]);
    assert!(
        zu.starts_with("*2"),
        "ZUNION must run inside a workspace; got {zu:?}"
    );
    let lm = a.send(&["LMPOP", "2", "lmissing", "l1", "LEFT"]);
    assert!(
        lm.starts_with('*'),
        "LMPOP must run inside a workspace; got {lm:?}"
    );
    // XGROUP's `args[0]` is a SUBCOMMAND, not a key.
    a.send(&["XADD", "st", "*", "f", "v"]);
    assert_eq!(a.send(&["XGROUP", "CREATE", "st", "g", "0"]), "+OK\r\n");
}

/// `SORT ... BY w_*` dereferences one key per element, named at run time. The
/// pattern is prefixed so every expansion lands inside the workspace.
#[test]
fn ws668_7_sort_by_pattern_dereferences_inside_the_workspace() {
    let m = spawn_moon("1");
    let mut global = Conn::open(m.port);
    // Global weights that would invert the order if they were the ones read.
    assert_eq!(global.send(&["SET", "w_a", "99"]), "+OK\r\n");
    assert_eq!(global.send(&["SET", "w_b", "1"]), "+OK\r\n");

    let mut a = bound(m.port, "sortby");
    a.send(&["RPUSH", "names", "a", "b"]);
    assert_eq!(a.send(&["SET", "w_a", "1"]), "+OK\r\n");
    assert_eq!(a.send(&["SET", "w_b", "99"]), "+OK\r\n");

    let sorted = a.send(&["SORT", "names", "BY", "w_*"]);
    assert_eq!(
        sorted, "*2\r\n$1\r\na\r\n$1\r\nb\r\n",
        "moon#668 — SORT BY read the GLOBAL weights (which would order b,a) \
         instead of the workspace's"
    );

    // `BY nosort` is a sentinel, not a pattern: prefixing it would turn it into
    // a lookup that misses, silently changing the command's meaning.
    assert_eq!(
        a.send(&["SORT", "names", "BY", "nosort"]),
        "*2\r\n$1\r\na\r\n$1\r\nb\r\n"
    );
}

// ---------------------------------------------------------------------------
// moon#702 — MULTI queued the RAW argv, bypassing the rewrite entirely
// ---------------------------------------------------------------------------
//
// The MULTI queue gate stores FRAMES and `EXEC` replays those, and the gate sat
// ~450 lines ABOVE the workspace rewrite — so every key inside a transaction
// reached the keyspace unprefixed, on BOTH the blocking and the non-blocking
// branch. moon#668 hoists the rewrite to just below the ACL gate; that alone
// fixes the blocking branch (it rebuilds from `cmd_args` already, moon#524) and
// leaves only the raw-`frame` branch needing a reframe.

/// A transaction's writes must land in the workspace, not the global keyspace.
#[test]
fn ws702_1_multi_writes_stay_inside_the_workspace() {
    for shards in ["1", "4"] {
        let m = spawn_moon(shards);
        let mut a = bound(m.port, &format!("txn-{shards}"));
        let mut global = Conn::open(m.port);

        assert_eq!(a.send(&["MULTI"]), "+OK\r\n");
        assert_eq!(a.send(&["SET", "txkey", "inside"]), "+QUEUED\r\n");
        assert_eq!(a.send(&["INCR", "txctr"]), "+QUEUED\r\n");
        let exec = a.send(&["EXEC"]);
        assert!(
            exec.starts_with("*2\r\n"),
            "EXEC must answer both queued replies; got {exec:?} (shards={shards})"
        );

        // The writer can read back what it wrote. Before moon#702 the queued
        // SET was unprefixed and this later, PREFIXED read answered nil — the
        // transaction wrote somewhere its own connection could not see.
        assert_eq!(
            a.send(&["GET", "txkey"]),
            "$6\r\ninside\r\n",
            "moon#702 — a workspace-bound MULTI wrote where its own connection \
             cannot read (shards={shards})"
        );
        assert_no_global_key(&mut global, "txkey", "SET inside MULTI");
        assert_no_global_key(&mut global, "txctr", "INCR inside MULTI");
    }
}

/// The whole point: one tenant must not be able to reach another's keys.
///
/// Because no prefix was applied inside a transaction, a tenant could simply
/// TYPE the other tenant's hash tag and address its keys directly. Measured
/// against the pre-fix binary: tenant B read `alpha-private` and overwrote it.
#[test]
fn ws702_2_a_tenant_cannot_reach_another_tenants_keys_through_multi() {
    let m = spawn_moon("1");
    let (mut a, a_prefix) = bound_with_prefix(m.port, "tenant-a");
    assert_eq!(a.send(&["SET", "secret", "alpha-private"]), "+OK\r\n");

    let a_secret = format!("{a_prefix}secret");
    let mut b = bound(m.port, "tenant-b");
    assert_eq!(b.send(&["MULTI"]), "+OK\r\n");
    assert_eq!(b.send(&["GET", &a_secret]), "+QUEUED\r\n");
    assert_eq!(
        b.send(&["SET", &a_secret, "OVERWRITTEN-BY-B"]),
        "+QUEUED\r\n"
    );
    let exec = b.send(&["EXEC"]);
    assert!(
        !exec.contains("alpha-private"),
        "moon#702 — tenant B READ tenant A's private value through MULTI: {exec:?}"
    );

    assert_eq!(
        a.send(&["GET", "secret"]),
        "$13\r\nalpha-private\r\n",
        "moon#702 — tenant B OVERWROTE tenant A's key through MULTI"
    );
}

/// The blocking half of the gate queues its non-blocking twin (moon#524),
/// rebuilt from `cmd_args` — so it was broken for the same reason and fixed by
/// the same hoist. Measured against the pre-fix binary: a queued `BLPOP` popped
/// `global-item` off the GLOBAL list. Pinned separately because the two
/// branches are one `if`/`else` apart, and a later "simplification" that queued
/// the raw frame on either side would re-open the hole.
#[test]
fn ws702_3_a_queued_blocking_twin_is_workspace_scoped() {
    let m = spawn_moon("1");
    let mut global = Conn::open(m.port);
    assert_eq!(global.send(&["RPUSH", "gqueue", "global-item"]), ":1\r\n");

    let mut a = bound(m.port, "blocking");
    assert_eq!(a.send(&["RPUSH", "gqueue", "mine"]), ":1\r\n");
    assert_eq!(a.send(&["MULTI"]), "+OK\r\n");
    assert_eq!(a.send(&["BLPOP", "gqueue", "0"]), "+QUEUED\r\n");
    let exec = a.send(&["EXEC"]);
    assert!(
        exec.contains("mine") && !exec.contains("global-item"),
        "a queued BLPOP must pop from the WORKSPACE's list; got {exec:?}"
    );
}

/// An argv whose key positions cannot be determined poisons the transaction at
/// queue time, exactly like any other queue-time rejection — EXEC must not run
/// the valid half against the global keyspace.
#[test]
fn ws702_4_an_indeterminate_argv_poisons_the_transaction() {
    let m = spawn_moon("1");
    let mut a = bound(m.port, "indeterminate");
    let mut global = Conn::open(m.port);

    assert_eq!(a.send(&["MULTI"]), "+OK\r\n");
    assert_eq!(a.send(&["SET", "valid_half", "written"]), "+QUEUED\r\n");
    let refused = a.send(&["ZUNIONSTORE", "dst", "notanumber", "x", "y"]);
    assert!(
        refused.starts_with("-ERR workspace: refusing 'ZUNIONSTORE'"),
        "an indeterminate argv must be refused at QUEUE time; got {refused:?}"
    );
    assert_eq!(
        a.send(&["EXEC"]),
        "-EXECABORT Transaction discarded because of previous errors.\r\n"
    );
    assert_eq!(
        a.send(&["EXISTS", "valid_half"]),
        ":0\r\n",
        "a poisoned transaction runs NOTHING"
    );
    assert_no_global_key(&mut global, "valid_half", "a poisoned transaction");
}
