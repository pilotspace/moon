//! CLIENT INFO / CLIENT LIST and RESET for a subscribed connection, checked
//! against redis-server 8.6.1 over raw RESP (moon#1105).
//!
//! Measured on redis 8.6.1:
//!
//! | sequence | redis 8.6.1 |
//! |---|---|
//! | `HELLO 3`, `SUBSCRIBE x`, `CLIENT INFO` | `flags=P sub=1 psub=0 ssub=0 resp=3` |
//! | + `PSUBSCRIBE p*`, `SSUBSCRIBE sx` | `flags=P sub=1 psub=1 ssub=1 resp=3` |
//! | `HELLO 3`, `CLIENT INFO` | `flags=N resp=3` |
//! | RESP2 `SUBSCRIBE x y`, `PSUBSCRIBE p*`, seen by another client's `CLIENT LIST` | `flags=P sub=2 psub=1 resp=2` |
//! | `SELECT 3`, `CLIENT TRACKING on`, `CLIENT SETNAME nm`, `SUBSCRIBE x`, `RESET`, `CLIENT INFO` | `flags=N db=0 sub=0 redir=-1 resp=2 name=` |
//! | `HELLO 3`, `SSUBSCRIBE sx`, `RESET`, then `SPUBLISH sx hi` elsewhere | `:0` |
//!
//! moon answered `flags=S sub=0 psub=0 resp=2` for every subscribed client
//! (`S` is redis's flag for a REPLICA), kept db 3, tracking and the name across
//! a RESET sent from RESP2 subscriber mode, and left a RESP3 client's sharded
//! subscription registered after RESET, so `SPUBLISH` still counted it.
//!
//! Every case runs at `--shards 1` and `--shards 4`.
//!
//! Run alone with: cargo test --test subscriber_client_state

mod common;

use std::collections::HashMap;
use std::process::Command;
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard};

struct Server {
    _guard: ServerGuard,
    _dir: tempfile::TempDir,
    port: u16,
}

fn server(shards: u32) -> Server {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    let (guard, port) = common::spawn_listening_guarded(|port| {
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &path.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "no",
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(common::server_stderr(&path))
            .spawn()
            .expect("spawn moon")
    });
    Server {
        _guard: guard,
        _dir: dir,
        port,
    }
}

/// The `key=value` fields of the one CLIENT INFO / CLIENT LIST line in `raw`
/// whose `id=` is `id` (any line when `id` is `None`).
fn client_fields(raw: &str, id: Option<&str>) -> HashMap<String, String> {
    for line in raw.split(['\r', '\n']) {
        let Some(start) = line.find("id=") else {
            continue;
        };
        let kv: HashMap<String, String> = line[start..]
            .split(' ')
            .filter_map(|p| p.split_once('='))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        if id.is_none_or(|want| kv.get("id").map(String::as_str) == Some(want)) {
            return kv;
        }
    }
    panic!("no client line for id {id:?} in {raw:?}");
}

/// Assert each `(field, value)` pair, naming the whole line on failure.
fn assert_fields(what: &str, kv: &HashMap<String, String>, want: &[(&str, &str)]) {
    for (k, v) in want {
        assert_eq!(
            kv.get(*k).map(String::as_str),
            Some(*v),
            "{what}: field {k} (whole line: {kv:?})"
        );
    }
}

fn client_id(c: &mut Conn) -> String {
    let r = c.send(&["CLIENT", "ID"]);
    r.trim_start_matches(':').trim_end().to_string()
}

#[test]
fn resp3_subscriber_client_info_reports_pubsub_flag_counts_and_protocol() {
    for shards in [1, 4] {
        let srv = server(shards);
        let mut c = Conn::open(srv.port);
        c.send(&["HELLO", "3"]);
        c.send(&["SUBSCRIBE", "x"]);
        let info = client_fields(&c.send(&["CLIENT", "INFO"]), None);
        assert_fields(
            &format!("RESP3 SUBSCRIBE x, shards={shards}"),
            &info,
            &[
                ("flags", "P"),
                ("sub", "1"),
                ("psub", "0"),
                ("ssub", "0"),
                ("resp", "3"),
            ],
        );

        c.send(&["PSUBSCRIBE", "p*"]);
        c.send(&["SSUBSCRIBE", "sx"]);
        let info = client_fields(&c.send(&["CLIENT", "INFO"]), None);
        assert_fields(
            &format!("RESP3 + PSUBSCRIBE p* + SSUBSCRIBE sx, shards={shards}"),
            &info,
            &[
                ("flags", "P"),
                ("sub", "1"),
                ("psub", "1"),
                ("ssub", "1"),
                ("resp", "3"),
            ],
        );

        let mut plain = Conn::open(srv.port);
        plain.send(&["HELLO", "3"]);
        let info = client_fields(&plain.send(&["CLIENT", "INFO"]), None);
        assert_fields(
            &format!("RESP3, not subscribed, shards={shards}"),
            &info,
            &[("flags", "N"), ("sub", "0"), ("resp", "3")],
        );
    }
}

#[test]
fn resp2_subscriber_seen_by_another_clients_client_list() {
    for shards in [1, 4] {
        let srv = server(shards);
        let mut sub = Conn::open(srv.port);
        let id = client_id(&mut sub);
        sub.pipeline(&[&["SUBSCRIBE", "x", "y"], &["PSUBSCRIBE", "p*"]]);
        // Two SUBSCRIBE confirmations and one PSUBSCRIBE: three replies, the
        // pipeline above consumed two.
        sub.read_replies(1);

        // The subscriber's task publishes its counts once it is back in its
        // loop, after writing the confirmations this test just read; poll a
        // bounded while rather than assume the two threads interleave.
        let mut other = Conn::open(srv.port);
        let deadline = Instant::now() + Duration::from_secs(5);
        let want = [("flags", "P"), ("sub", "2"), ("psub", "1"), ("resp", "2")];
        loop {
            let kv = client_fields(&other.send(&["CLIENT", "LIST", "ID", &id]), Some(&id));
            if want
                .iter()
                .all(|(k, v)| kv.get(*k).map(String::as_str) == Some(*v))
                || Instant::now() >= deadline
            {
                assert_fields(
                    &format!("RESP2 subscriber in CLIENT LIST, shards={shards}"),
                    &kv,
                    &want,
                );
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

#[test]
fn reset_from_resp2_subscriber_mode_restores_every_default() {
    for shards in [1, 4] {
        let srv = server(shards);
        let mut c = Conn::open(srv.port);
        c.send(&["SELECT", "3"]);
        c.send(&["CLIENT", "TRACKING", "on"]);
        c.send(&["CLIENT", "SETNAME", "nm"]);
        c.send(&["SUBSCRIBE", "x"]);
        assert_eq!(c.send(&["RESET"]), "+RESET\r\n", "shards={shards}");
        let info = client_fields(&c.send(&["CLIENT", "INFO"]), None);
        assert_fields(
            &format!("RESET from RESP2 subscriber mode, shards={shards}"),
            &info,
            &[
                ("flags", "N"),
                ("db", "0"),
                ("sub", "0"),
                ("psub", "0"),
                ("redir", "-1"),
                ("resp", "2"),
                ("name", ""),
            ],
        );
    }
}

#[test]
fn reset_pipelined_behind_subscribe_restores_db() {
    for shards in [1, 4] {
        let srv = server(shards);
        let mut c = Conn::open(srv.port);
        c.send(&["SELECT", "3"]);
        let replies = c.pipeline(&[&["SUBSCRIBE", "x"], &["RESET"], &["PING"]]);
        assert_eq!(
            replies, "*3\r\n$9\r\nsubscribe\r\n$1\r\nx\r\n:1\r\n+RESET\r\n+PONG\r\n",
            "shards={shards}"
        );
        let info = client_fields(&c.send(&["CLIENT", "INFO"]), None);
        assert_fields(
            &format!("SUBSCRIBE|RESET|PING pipelined, shards={shards}"),
            &info,
            &[("flags", "N"), ("db", "0"), ("sub", "0")],
        );
    }
}

#[test]
fn reset_drops_every_subscription_namespace() {
    for shards in [1, 4] {
        let srv = server(shards);
        for proto in ["2", "3"] {
            for (sub_cmd, pub_cmd, target) in [
                ("SUBSCRIBE", "PUBLISH", "ch"),
                ("PSUBSCRIBE", "PUBLISH", "pat"),
                ("SSUBSCRIBE", "SPUBLISH", "sch"),
            ] {
                let channel = format!("reset-{proto}-{target}");
                let arg = if sub_cmd == "PSUBSCRIBE" {
                    format!("{channel}*")
                } else {
                    channel.clone()
                };
                let mut c = Conn::open(srv.port);
                c.send(&["HELLO", proto]);
                c.send(&[sub_cmd, &arg]);
                assert_eq!(c.send(&["RESET"]), "+RESET\r\n");
                let mut other = Conn::open(srv.port);
                assert_eq!(
                    other.send(&[pub_cmd, &channel, "hi"]),
                    ":0\r\n",
                    "RESP{proto} {sub_cmd} then RESET: {pub_cmd} must find no receiver, \
                     shards={shards}"
                );
                assert_eq!(c.send(&["PING"]), "+PONG\r\n");
            }
        }
    }
}
