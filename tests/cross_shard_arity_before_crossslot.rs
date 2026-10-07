//! A wrong-arity invocation earns its arity error, never `CROSSSLOT`, whatever
//! the shard count.
//!
//! ## The defect
//!
//! The pre-routing cross-shard guard (`cross_shard_multikey_rejection`, and
//! the Lua bridge's `script_cross_shard_rejection`) decides from the key
//! positions alone. It never looked at the argument COUNT, so a key pair that
//! spans shards turned an arity error into a refusal that sends the client
//! chasing hash tags:
//!
//! ```text
//! RENAME k1 k2 extra   (--shards 12)
//!   redis 8.6.1, standalone AND cluster -> ERR wrong number of arguments for 'rename' command
//!   moon                                -> CROSSSLOT Keys in request don't hash to the same shard; ...
//! ```
//!
//! Redis Cluster checks arity in `processCommand` BEFORE the slot check, so
//! this is not a standalone-vs-cluster difference: both oracles agree.
//! Arguments OTHER than the count (`ZMPOP 2 a MIN`'s missing direction,
//! `ZRANGESTORE`'s non-integer rank) are parsed by the command itself, after
//! the slot check, and Redis Cluster answers `CROSSSLOT` for those — moon does
//! the same, and the controls below keep it that way.
//!
//! ## Why these assertions can fail
//!
//! The destination key is SEARCHED for with the server's own routing hash
//! (`moon::shard::dispatch::key_to_shard`), so at `--shards 12` every row
//! provably spans shards; no row can pass on a lucky placement. Every
//! expected string is the reply redis-server 8.6.1 returned for the same argv
//! (measured 2026-10-07, standalone and one-node cluster).
//!
//! Two controls stop the fix from being a blanket weakening of the guard:
//! a WELL-FORMED spanning `RENAME` must still be refused, and a spanning
//! `ZMPOP` whose count is right but whose direction is missing must still be
//! refused (Redis Cluster refuses it too).

mod common;

use std::process::Command;

use common::{Conn, ServerGuard};
use moon::shard::dispatch::key_to_shard;

const WIDE: usize = 12;

const CROSS_SHARD_PREFIX: &str = "-CROSSSLOT Keys in request don't hash to the same shard";

struct Moon {
    _guard: ServerGuard,
    port: u16,
    dir: std::path::PathBuf,
}

impl Drop for Moon {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn spawn_moon(shards: usize) -> Moon {
    let bin = common::find_moon_binary();
    let dir = common::unique_test_dir("moon-arity-xshard");
    std::fs::create_dir_all(&dir).expect("create test dir");
    let (guard, port) = common::spawn_listening_guarded(|port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                &shards.to_string(),
                "--admin-port",
                "0",
                "--appendonly",
                "no",
                "--disk-free-min-pct",
                "0",
                "--dir",
                dir.to_str().expect("utf-8 temp dir"),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon")
    });
    Moon {
        _guard: guard,
        port,
        dir,
    }
}

/// A source key and a destination the server routes to a DIFFERENT shard at
/// `WIDE`. At one shard the same names trivially co-locate, which is the
/// point: both legs send byte-identical argv.
fn spanning_pair() -> (String, String) {
    let src = "arity:src".to_owned();
    let owner = key_to_shard(src.as_bytes(), WIDE);
    let dst = (0..10_000)
        .map(|i| format!("arity:dst:{i}"))
        .find(|k| key_to_shard(k.as_bytes(), WIDE) != owner)
        .expect("a key on another shard must exist");
    (src, dst)
}

fn fill(shape: &[&str], src: &str, dst: &str) -> Vec<String> {
    shape
        .iter()
        .map(|p| match *p {
            "{s}" => src.to_owned(),
            "{d}" => dst.to_owned(),
            lit => lit.to_owned(),
        })
        .collect()
}

fn send(c: &mut Conn, argv: &[String]) -> String {
    let parts: Vec<&str> = argv.iter().map(String::as_str).collect();
    c.send(&parts)
}

fn wrong_args(name: &str) -> String {
    format!("-ERR wrong number of arguments for '{name}' command\r\n")
}

/// Wrong-arity invocations of the two-key family, each naming a spanning key
/// pair. The command name in the expected text is Redis's.
const WRONG_ARITY: &[(&str, &[&str])] = &[
    // arity 3: one extra
    ("rename", &["RENAME", "{s}", "{d}", "extra"]),
    ("renamenx", &["RENAMENX", "{s}", "{d}", "extra"]),
    // arity 4: one short, one extra
    ("smove", &["SMOVE", "{s}", "{d}"]),
    ("smove", &["SMOVE", "{s}", "{d}", "m", "extra"]),
    // arity -5: one short
    ("zrangestore", &["ZRANGESTORE", "{d}", "{s}", "0"]),
    // arity 5: one short. Owned by the list-MOVE guard, which already
    // declined to refuse a malformed argv; pinned so both guards agree.
    ("lmove", &["LMOVE", "{s}", "{d}", "LEFT"]),
];

fn arity_wins_at(shards: usize) {
    let moon = spawn_moon(shards);
    let mut c = Conn::open(moon.port);
    let (src, dst) = spanning_pair();
    // A real value under the source, so a wrongly-routed command would have
    // something to destroy.
    assert_eq!(c.send(&["SET", &src, "v"]), "+OK\r\n");

    for (name, shape) in WRONG_ARITY {
        let argv = fill(shape, &src, &dst);
        assert_eq!(
            send(&mut c, &argv),
            wrong_args(name),
            "--shards {shards}: {argv:?} must earn its arity error (redis 8.6.1, \
             standalone and cluster), not a cross-shard refusal"
        );
    }

    // Nothing above may have run: the source is untouched, the destination
    // was never created.
    assert_eq!(c.send(&["GET", &src]), "$1\r\nv\r\n", "--shards {shards}");
    assert_eq!(c.send(&["EXISTS", &dst]), ":0\r\n", "--shards {shards}");
}

/// Both legs, the same expected bytes: the shard count must not change the
/// answer to a malformed command.
#[test]
fn wrong_arity_is_reported_at_one_shard() {
    arity_wins_at(1);
}

#[test]
fn wrong_arity_is_reported_not_crossslot_at_twelve_shards() {
    arity_wins_at(WIDE);
}

/// The Lua bridge consults the same key walk. A `redis.call` with the wrong
/// argument count must answer what the one-shard server answers — whatever
/// moon's script-arity wording is, it is not a function of the shard count.
#[test]
fn script_wrong_arity_matches_one_shard() {
    let (src, dst) = spanning_pair();
    let script = "return redis.call('RENAME', KEYS[1], ARGV[1], ARGV[2])";
    let argv = |c: &mut Conn| c.send(&["EVAL", script, "1", &src, &dst, "extra"]);

    let narrow = spawn_moon(1);
    let at_one = argv(&mut Conn::open(narrow.port));
    drop(narrow);

    let wide = spawn_moon(WIDE);
    let at_wide = argv(&mut Conn::open(wide.port));

    assert!(
        !at_one.starts_with("-CROSSSLOT"),
        "control: one shard cannot span: {at_one:?}"
    );
    assert_eq!(
        at_wide, at_one,
        "a script's wrong-arity redis.call must answer the same at --shards \
         {WIDE} as at --shards 1"
    );
}

/// Controls: arity is the ONLY thing that now outranks the refusal.
#[test]
fn well_formed_or_argument_level_errors_are_still_refused() {
    let moon = spawn_moon(WIDE);
    let mut c = Conn::open(moon.port);
    let (src, dst) = spanning_pair();
    assert_eq!(c.send(&["SET", &src, "v"]), "+OK\r\n");

    // Right count, spanning keys: the moon#592 refusal, unchanged.
    let rename = c.send(&["RENAME", &src, &dst]);
    assert!(
        rename.starts_with(CROSS_SHARD_PREFIX),
        "a well-formed spanning RENAME must still be refused: {rename:?}"
    );
    // Right count (ZMPOP is variadic), direction missing: Redis Cluster
    // answers CROSSSLOT here too, because the command's own parser runs
    // after the slot check.
    let zmpop = c.send(&["ZMPOP", "2", &src, &dst]);
    assert!(
        zmpop.starts_with(CROSS_SHARD_PREFIX),
        "an argument-level error on spanning keys is refused, as in Redis \
         Cluster: {zmpop:?}"
    );
    assert_eq!(c.send(&["GET", &src]), "$1\r\nv\r\n");
    assert_eq!(c.send(&["EXISTS", &dst]), ":0\r\n");
}
