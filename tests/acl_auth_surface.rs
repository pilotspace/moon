//! moon#640 red/green: three divergences on the ACL/AUTH surface, each
//! measured against redis-server 8.6.1 before the fix was written.
//!
//!   * `AUTH <password>` answered `+OK` on a server with **no password
//!     configured**. Redis errors. `AUTH <pw>` returning OK is how a client or
//!     an operator probes whether a server requires authentication, so a
//!     deployment that MEANT to set `requirepass` and did not looked correctly
//!     secured from the client side.
//!   * `ACL USERS` was unimplemented.
//!   * `ACL HELP` was unimplemented — while Moon's own unknown-subcommand
//!     error said "Try ACL HELP.", pointing the reader at the one subcommand
//!     that did not exist.
//!
//! The configured-password path is asserted too (`au4`), because the fix must
//! not touch it: with `--requirepass` Moon already matched redis byte-for-byte
//! on wrong password, right password, and unknown user.
//!
//! Run with:
//!   cargo test --release --test acl_auth_surface

mod common;

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

/// `requirepass`: `None` leaves the default user `nopass`, which is the whole
/// point of `au1`.
fn spawn_moon(requirepass: Option<&str>) -> Option<Moon> {
    let bin = common::find_moon_binary();
    if !bin.exists() {
        eprintln!(
            "skipping: {} not built. Run `cargo build --release` first.",
            bin.display()
        );
        return None;
    }
    let tag = "moon-acl-auth";
    let (child, port) = common::spawn_listening(|port| {
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
        if let Some(pw) = requirepass {
            c.args(["--requirepass", pw]);
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
        // On a password-protected server PING is refused pre-auth; either
        // answer proves the listener is up and parsing.
        let ok = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let r = Conn::open(moon.port).send(&["PING"]);
            r.contains("PONG") || r.contains("NOAUTH")
        }))
        .unwrap_or(false);
        if ok {
            return Some(moon);
        }
        thread::sleep(Duration::from_millis(100));
    }
    eprintln!("skipping: moon did not answer within 15s on port {port}");
    None
}

// ---------------------------------------------------------------------------
// AUTH
// ---------------------------------------------------------------------------

/// Captured from `redis-server 8.6.1` with no `requirepass`, 2026-08-22.
/// One line on purpose: a wrapped literal is how the stray-whitespace bug got
/// in, and this is the assertion that has to catch it.
const REDIS_NOPASS_AUTH_ERROR: &str = "-ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?\r\n";

/// redis 8.6.1, no `requirepass`:
/// `AUTH wrongpass` -> `-ERR AUTH <password> called without any password
/// configured for the default user. Are you sure your configuration is
/// correct?`
#[test]
fn au1_single_arg_auth_errors_when_no_password_is_configured() {
    let Some(m) = spawn_moon(None) else { return };
    let mut c = Conn::open(m.port);

    let r = c.send(&["AUTH", "wrongpass"]);
    assert!(
        r.starts_with('-'),
        "AUTH with no password configured must be an ERROR, not `{r:?}` — a \
         client probing `AUTH <pw>` to discover whether auth is required is \
         told `yes, and you are in`"
    );
    // The FULL text, byte for byte against redis 8.6.1 — not a substring.
    // A substring check went green while `cargo fmt` had collapsed a line
    // continuation in the source and left 27 stray spaces mid-sentence; a
    // driver matching on the message would not have tolerated that.
    assert_eq!(
        r, REDIS_NOPASS_AUTH_ERROR,
        "must reproduce redis 8.6.1's wording exactly: {r:?}"
    );
}

/// The two-arg form is UNCHANGED: `default` is `nopass`, so any password
/// matches it — that is what redis answers too.
#[test]
fn au2_two_arg_auth_for_the_nopass_default_still_succeeds() {
    let Some(m) = spawn_moon(None) else { return };
    let mut c = Conn::open(m.port);

    let r = c.send(&["AUTH", "default", "anything"]);
    assert!(
        r.contains("OK"),
        "AUTH default <pw> must still succeed against the nopass default: {r:?}"
    );
}

/// A user created WITH a password is unaffected by the nopass carve-out.
#[test]
fn au3_two_arg_auth_still_checks_a_real_password() {
    let Some(m) = spawn_moon(None) else { return };
    let mut c = Conn::open(m.port);

    let r = c.send(&["ACL", "SETUSER", "au3u", "on", ">pw", "~*", "+@all"]);
    assert!(r.contains("OK"), "SETUSER -> {r:?}");

    assert!(
        c.send(&["AUTH", "au3u", "wrong"]).contains("WRONGPASS"),
        "a real password must still be checked"
    );
    assert!(
        c.send(&["AUTH", "au3u", "pw"]).contains("OK"),
        "the right password must still be accepted"
    );
}

/// The control: with `--requirepass` set, all three cases must keep matching
/// redis. This is what stops the fix from becoming a regression on the path
/// that was already correct.
#[test]
fn au4_configured_password_path_is_unchanged() {
    let Some(m) = spawn_moon(Some("secret")) else {
        return;
    };
    let mut c = Conn::open(m.port);

    assert!(
        c.send(&["AUTH", "wrong"]).contains("WRONGPASS"),
        "wrong password must still be WRONGPASS, not the nopass error"
    );
    assert!(c.send(&["AUTH", "secret"]).contains("OK"));

    let mut c2 = Conn::open(m.port);
    assert!(
        c2.send(&["AUTH", "nosuchuser", "secret"])
            .contains("WRONGPASS")
    );
}

// ---------------------------------------------------------------------------
// ACL USERS / ACL HELP
// ---------------------------------------------------------------------------

#[test]
fn au5_acl_users_lists_every_registered_username() {
    let Some(m) = spawn_moon(None) else { return };
    let mut c = Conn::open(m.port);

    let r = c.send(&["ACL", "USERS"]);
    assert!(
        r.starts_with('*'),
        "ACL USERS must answer an array, got {r:?}"
    );
    assert!(
        r.contains("default"),
        "the default user must be listed: {r:?}"
    );

    assert!(
        c.send(&["ACL", "SETUSER", "au5u", "on", ">pw", "~*", "+@all"])
            .contains("OK")
    );
    let r = c.send(&["ACL", "USERS"]);
    assert!(
        r.contains("au5u") && r.contains("default"),
        "a newly created user must appear alongside default: {r:?}"
    );
}

#[test]
fn au6_acl_help_exists_because_the_error_text_points_at_it() {
    let Some(m) = spawn_moon(None) else { return };
    let mut c = Conn::open(m.port);

    // The exact self-contradiction this test exists for: Moon answered
    // "unknown subcommand 'HELP'. Try ACL HELP."
    let r = c.send(&["ACL", "HELP"]);
    assert!(
        !r.starts_with('-'),
        "ACL HELP must not be an error — Moon's own unknown-subcommand text \
         tells the reader to run it: {r:?}"
    );
    assert!(r.starts_with('*'), "ACL HELP answers an array: {r:?}");

    // Every subcommand the help advertises must actually dispatch. A help text
    // listing a subcommand Moon does not implement is the same defect one
    // level up.
    for sub in ["WHOAMI", "LIST", "USERS", "CAT", "GENPASS"] {
        assert!(
            r.contains(sub),
            "help must advertise {sub}, which Moon implements: {r:?}"
        );
        let probe = c.send(&["ACL", sub]);
        assert!(
            !probe.contains("unknown subcommand"),
            "help advertises {sub} but it does not dispatch: {probe:?}"
        );
    }
}

/// An unknown subcommand must still be refused — the fix adds two names, it
/// does not make ACL permissive.
#[test]
fn au7_unknown_acl_subcommand_is_still_refused() {
    let Some(m) = spawn_moon(None) else { return };
    let mut c = Conn::open(m.port);

    let r = c.send(&["ACL", "NOSUCHSUB"]);
    assert!(
        r.contains("unknown subcommand"),
        "ACL NOSUCHSUB must still be refused: {r:?}"
    );
}
