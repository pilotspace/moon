//! moon#635 red/green: `COMMAND LIST` must enumerate container subcommands and
//! the four commands Moon answers but never published.
//!
//! Measured 2026-08-22 against redis-server 8.6.1 on the same host:
//!
//! ```text
//!                  COMMAND COUNT    COMMAND LIST entries
//!   redis 8.6.1          274                411
//!   moon (before)        263                263
//! ```
//!
//! Redis's LIST is LARGER than its COUNT because LIST enumerates container
//! subcommands as their own `container|sub` entries and COUNT does not. Moon
//! emitted zero entries containing `|`, so the two matched exactly — which is
//! the tell that the container surface was not published at all. A client that
//! builds its command table from `COMMAND LIST` therefore refused to send
//! `PUBSUB CHANNELS` (works, was unlisted) and never learned that `CONFIG`,
//! `CLIENT`, `XINFO` and the rest take subcommands.
//!
//! `cl3` is the test that keeps this honest in the other direction: it SENDS
//! every name the list publishes and fails if any of them does not dispatch.
//! Publishing a name that is not implemented is the same defect one level up.
//!
//! Run with:
//!   cargo test --release --test command_list_subcommands

mod common;

use std::collections::HashSet;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use common::Conn;

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

fn spawn_moon(cluster: bool) -> Option<Moon> {
    let bin = common::find_moon_binary();
    if !bin.exists() {
        eprintln!(
            "skipping: {} not built. Run `cargo build --release` first.",
            bin.display()
        );
        return None;
    }
    let tag = if cluster { "moon-cl-c" } else { "moon-cl" };
    // A cluster node also binds port+10000 for the bus, so its port has to
    // come from the reserved cluster range: an ephemeral port near 56000
    // makes the bus sibling exceed 65535 and the child exits before it ever
    // accepts (measured — this test's first run died three respawns deep).
    let spawner = if cluster {
        common::spawn_listening_cluster
    } else {
        common::spawn_listening
    };
    let (child, port) = spawner(|port: u16| {
        let tmp_dir = std::env::temp_dir().join(format!("{tag}-{port}"));
        let _ = std::fs::create_dir_all(&tmp_dir);
        let mut c = Command::new(&bin);
        c.args([
            "--port",
            &port.to_string(),
            "--shards",
            "1",
            "--admin-port",
            "0",
            "--appendonly",
            "no",
            "--dir",
            tmp_dir.to_str().unwrap(),
        ]);
        if cluster {
            c.arg("--cluster-enabled");
        }
        c.stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn moon")
    });
    let tmp_dir = std::env::temp_dir().join(format!("{tag}-{port}"));
    let moon = Moon {
        child,
        port,
        tmp_dir,
    };

    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        let ok = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Conn::open(moon.port).send(&["PING"]).contains("PONG")
        }))
        .unwrap_or(false);
        if ok {
            return Some(moon);
        }
        thread::sleep(Duration::from_millis(100));
    }
    eprintln!("skipping: moon did not answer PING within 15s on port {port}");
    None
}

/// The bulk-string payloads of an array reply, in wire order.
fn array_items(reply: &str) -> Vec<String> {
    let mut lines = reply.split("\r\n");
    let header = lines.next().unwrap_or_default();
    assert!(
        header.starts_with('*'),
        "expected an array reply, got {reply:?}"
    );
    let n: usize = header[1..].parse().expect("array length");
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        let l = lines.next().expect("truncated array");
        if l.starts_with('$') {
            out.push(lines.next().expect("truncated bulk").to_string());
        } else if let Some(rest) = l.strip_prefix('+') {
            out.push(rest.to_string());
        }
    }
    out
}

fn command_list(c: &mut Conn) -> Vec<String> {
    array_items(&c.send(&["COMMAND", "LIST"]))
}

// ---------------------------------------------------------------------------

/// The headline: `COMMAND LIST` publishes `container|sub` entries at all, and
/// `COMMAND COUNT` stays a TOP-LEVEL count, so the two legitimately differ the
/// way redis's do.
#[test]
fn cl1_list_enumerates_subcommands_and_count_stays_top_level() {
    let Some(m) = spawn_moon(false) else { return };
    let mut c = Conn::open(m.port);

    let list = command_list(&mut c);
    let subs: Vec<&String> = list.iter().filter(|n| n.contains('|')).collect();
    assert!(
        !subs.is_empty(),
        "COMMAND LIST published no `container|sub` entries at all — the exact \
         state moon#635 describes"
    );

    let count_reply = c.send(&["COMMAND", "COUNT"]);
    let count: usize = count_reply
        .trim_start_matches(':')
        .trim_end()
        .parse()
        .unwrap_or_else(|_| panic!("COMMAND COUNT -> {count_reply:?}"));

    assert_eq!(
        count,
        list.len() - subs.len(),
        "COUNT must equal the number of TOP-LEVEL names; redis counts 274 \
         while listing 411, and the two matching is what proved Moon listed \
         no subcommands"
    );
    assert!(
        count < list.len(),
        "LIST must be strictly larger than COUNT once subcommands are published"
    );
}

/// The four commands Moon answers but never published. `PUBSUB` is the sharp
/// one: a pub/sub client that feature-detects concluded the server had no
/// pub/sub introspection at all.
#[test]
fn cl2_previously_unlisted_but_implemented_commands_are_published() {
    let Some(m) = spawn_moon(false) else { return };
    let mut c = Conn::open(m.port);

    let list: HashSet<String> = command_list(&mut c).into_iter().collect();
    for name in ["pubsub", "asking", "readonly", "readwrite"] {
        assert!(
            list.contains(name),
            "`{name}` is implemented but absent from COMMAND LIST"
        );
    }

    // Implemented means: SENDING it does not come back "unknown command".
    // `COMMAND INFO` is the very surface under repair, so it is not evidence.
    for parts in [
        vec!["PUBSUB", "CHANNELS"],
        vec!["ASKING"],
        vec!["READONLY"],
        vec!["READWRITE"],
    ] {
        let r = c.send(&parts);
        assert!(
            !r.contains("unknown command"),
            "{parts:?} is published but not implemented: {r:?}"
        );
    }
}

/// Every published `container|sub` must dispatch. This is the guard that stops
/// the table from becoming a list of names the server does not answer — the
/// same defect as the unlisted commands above, one level up.
///
/// A per-container BOGUS control is required, not optional: five containers
/// answer an unknown subcommand with something other than "unknown
/// subcommand". `MEMORY` says "not supported", `XGROUP` says "not recognized",
/// and `OBJECT` / `XINFO` answer an ARITY error, which a naive probe reads as
/// "it exists". The control proves the probe can tell the two apart AT ALL for
/// this container before any name is judged by it.
#[test]
fn cl3_every_published_subcommand_actually_dispatches() {
    let Some(m) = spawn_moon(false) else { return };
    let Some(mc) = spawn_moon(true) else { return };
    let mut c = Conn::open(m.port);
    let mut cc = Conn::open(mc.port);

    // Give OBJECT/XINFO a real key so an arity error cannot masquerade as a
    // missing subcommand.
    assert!(c.send(&["SET", "cl3:k", "v"]).contains("OK"));
    assert!(
        c.send(&["XADD", "cl3:st", "1-1", "f", "v"])
            .starts_with('$')
    );

    let list = command_list(&mut c);
    let mut checked = 0usize;
    for name in list.iter().filter(|n| n.contains('|')) {
        let (container, sub) = name.split_once('|').expect("checked above");
        let cont_up = container.to_ascii_uppercase();
        let sub_up = sub.to_ascii_uppercase();

        // CLUSTER answers every subcommand "This instance has cluster support
        // disabled" unless cluster mode is on, which hides the distinction
        // entirely — so it is probed against the cluster-enabled server.
        let conn = if cont_up == "CLUSTER" {
            &mut cc
        } else {
            &mut c
        };

        let extra: &[&str] = match cont_up.as_str() {
            "OBJECT" => &["cl3:k"],
            "XINFO" => &["cl3:st"],
            _ => &[],
        };
        let mut parts = vec![cont_up.as_str(), sub_up.as_str()];
        parts.extend_from_slice(extra);

        let bogus_parts = {
            let mut p = vec![cont_up.as_str(), "ZZBOGUSZZ"];
            p.extend_from_slice(extra);
            p
        };
        let bogus = conn.send(&bogus_parts);
        assert!(
            bogus.starts_with('-'),
            "the {cont_up} control must be refused, else this probe cannot \
             tell a missing subcommand from a present one: {bogus:?}"
        );

        let reply = conn.send(&parts);
        let missing = reply.starts_with('-')
            && ["unknown", "not supported", "not recognized"]
                .iter()
                .any(|needle| reply.to_ascii_lowercase().contains(needle));
        assert!(
            !missing,
            "COMMAND LIST publishes `{name}` but `{cont_up} {sub_up}` does not \
             dispatch: {reply:?} (control for this container: {bogus:?})"
        );
        checked += 1;
    }
    assert!(
        checked >= 80,
        "expected the full container surface to be probed, only saw {checked}"
    );
}

/// A client that reads `config|get` out of `COMMAND LIST` and asks `COMMAND
/// INFO` about it must get a spec, not a null — publishing a name the very
/// next call denies is worse than not publishing it.
#[test]
fn cl4_command_info_resolves_the_names_command_list_publishes() {
    let Some(m) = spawn_moon(false) else { return };
    let mut c = Conn::open(m.port);

    let r = c.send(&["COMMAND", "INFO", "config|get"]);
    assert!(
        !r.contains("*-1") && !r.starts_with("*1\r\n_"),
        "COMMAND INFO config|get must not be a null element: {r:?}"
    );
    assert!(
        r.contains("config|get"),
        "the spec must name itself `config|get`: {r:?}"
    );

    // The container's own spec now carries its subcommands, the way redis's
    // does — and a leaf command still carries none.
    let cfg = c.send(&["COMMAND", "INFO", "config"]);
    assert!(
        cfg.contains("config|get"),
        "CONFIG's spec must list its subcommands: {cfg:?}"
    );

    // A subcommand of nothing is still unknown.
    let bogus = c.send(&["COMMAND", "INFO", "nosuchcontainer|get"]);
    assert!(
        bogus.contains("_\r\n") || bogus.contains("$-1"),
        "an unknown container|sub must stay a null element: {bogus:?}"
    );
}

/// `COMMAND DOCS` has the same publish-then-deny hazard as `COMMAND INFO`, but
/// redis splits the two halves and Moon must split them the same way (measured
/// on redis 8.6.1):
///
/// * an EXPLICITLY named `container|sub` resolves to a full doc map;
/// * the BARE `COMMAND DOCS` keys only the top-level names — redis's answer has
///   274 keys and not one contains a `|`, because subcommand docs are nested
///   under each container instead of flattened.
///
/// Matching only the first half would leave the bare listing disagreeing with
/// every driver that counts it; matching only the second half leaves
/// `COMMAND LIST` publishing names `DOCS` drops on the floor.
#[test]
fn cl5_command_docs_resolves_a_subcommand_but_does_not_flatten_them() {
    let Some(m) = spawn_moon(false) else { return };
    let mut c = Conn::open(m.port);

    let r = c.send(&["COMMAND", "DOCS", "config|get"]);
    assert!(
        r.contains("config|get") && r.contains("summary"),
        "COMMAND DOCS config|get must answer a doc map: {r:?}"
    );

    // Redis omits an unknown name from DOCS entirely rather than nulling it.
    let bogus = c.send(&["COMMAND", "DOCS", "nosuchcontainer|get"]);
    assert!(
        !bogus.contains("nosuchcontainer"),
        "an unknown container|sub must be omitted from DOCS: {bogus:?}"
    );

    // The bare listing stays top-level-only.
    let all = c.send(&["COMMAND", "DOCS"]);
    assert!(
        !all.contains("config|get"),
        "bare COMMAND DOCS must not flatten subcommands the way LIST does"
    );
}
