//! moon#1296: `ACL GETUSER`, `ACL LIST` and the `ACL SAVE` file render a
//! user's command rules in the order they were applied, as redis 7.2+ does.
//!
//! Moon kept two `HashSet`s and sorted them at render time, so `+set +get`
//! read back as `+get +set` and (before the sort) the rows in
//! `scripts/test-consistency.sh` flipped between runs of the same binary.
//! Every expected string below was read off redis-server 7.2.7
//! (`ACL SETUSER u <rules>` then `ACL LIST`).
//!
//! Each case is checked three ways: GETUSER, the line `ACL SAVE` writes, and
//! GETUSER again after `ACL LOAD` (the reload is where an order that only
//! looked right would drift).
//!
//! `MOON_BIN=<moon> cargo test --test acl_rule_order_1296 -- --include-ignored`

#![cfg(unix)]
#![allow(clippy::unwrap_used)]

mod common;

use common::Conn;

/// `(rules typed, rules as redis 7.2.7 renders them)`.
const CASES: &[(&str, &str)] = &[
    ("+set +get", "-@all +set +get"),
    ("+get +set", "-@all +get +set"),
    ("-@all +get +set +get", "-@all +set +get"),
    ("+@all -set -get", "+@all -set -get"),
    ("+@all -get -set", "+@all -get -set"),
    ("+@all -get -set +get -del", "+@all -set +get -del"),
    ("-@all +get -set +append", "-@all +get -set +append"),
    ("-@all +hset +hget +hdel", "-@all +hset +hget +hdel"),
    ("-@all +config|get +config", "-@all +config"),
    ("-@all +config +config|get", "-@all +config +config|get"),
    ("-@all +config|get +get", "-@all +config|get +get"),
    (
        "-@all +config|set +config|get",
        "-@all +config|set +config|get",
    ),
    (
        "+@all -config|set -config|get",
        "+@all -config|set -config|get",
    ),
    ("+@all -config -config|get", "+@all -config -config|get"),
];

fn spawn(dir: &std::path::Path, shards: &str) -> (common::ServerGuard, u16) {
    let bin = common::find_moon_binary();
    let dir = dir.to_path_buf();
    let shards = shards.to_string();
    common::spawn_listening_guarded(move |port| {
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                dir.to_str().unwrap(),
                "--shards",
                &shards,
                "--appendonly",
                "no",
                "--save",
                "",
                "--disk-offload",
                "disable",
                "--aclfile",
                dir.join("users.acl").to_str().unwrap(),
            ])
            .stdout(common::server_stderr(&dir))
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    })
}

/// The value after `commands` in a GETUSER reply (RESP2 flat array or map).
fn commands_field(reply: &str) -> String {
    let lines: Vec<&str> = reply.split("\r\n").collect();
    let at = lines
        .iter()
        .position(|l| *l == "commands")
        .unwrap_or_else(|| panic!("no commands field in {reply:?}"));
    // `$<len>`, then the value.
    lines[at + 2].to_string()
}

fn getuser_commands(c: &mut Conn) -> String {
    commands_field(&c.send(&["ACL", "GETUSER", "u"]))
}

fn run(shards: &str) {
    let dir = common::unique_test_dir("acl1296");
    std::fs::create_dir_all(&dir).unwrap();
    let (_guard, port) = spawn(&dir, shards);
    let mut c = Conn::open(port);
    for (typed, want) in CASES {
        c.send(&["ACL", "DELUSER", "u"]);
        let mut cmd = vec!["ACL", "SETUSER", "u", "on", "nopass"];
        cmd.extend(typed.split(' '));
        assert_eq!(c.send(&cmd), "+OK\r\n", "SETUSER {typed}");
        assert_eq!(getuser_commands(&mut c), *want, "GETUSER after `{typed}`");
        let list = c.send(&["ACL", "LIST"]);
        assert!(
            list.contains(&format!(" {want}\r\n")) || list.contains(&format!(" {want}\r")),
            "ACL LIST after `{typed}` must end its `user u` line with `{want}`: {list:?}"
        );
        assert_eq!(c.send(&["ACL", "SAVE"]), "+OK\r\n");
        let file = std::fs::read_to_string(dir.join("users.acl")).unwrap();
        let line = file.lines().find(|l| l.starts_with("user u ")).unwrap();
        assert!(
            line.ends_with(&format!(" {want}")),
            "the saved line for `{typed}` must end with `{want}`: {line}"
        );
        assert_eq!(c.send(&["ACL", "LOAD"]), "+OK\r\n");
        assert_eq!(
            getuser_commands(&mut c),
            *want,
            "GETUSER after `{typed}` and an ACL SAVE / ACL LOAD round trip"
        );
    }
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn rules_render_in_application_order_1_shard() {
    run("1");
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn rules_render_in_application_order_4_shards() {
    run("4");
}
