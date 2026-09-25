//! `common::find_moon_binary` must honour a pinned `MOON_BIN` or fail loudly.
//!
//! A `MOON_BIN` that is set, non-empty and names no file used to fall through
//! to `CARGO_BIN_EXE_moon`: a typo in the pin of a red run silently ran the
//! suite against the freshly built binary, and its green verdict was reported
//! as the pinned binary's (FIX3B-CM item S4, the part 3b connection review).
//!
//! The environment is process-global and tests run on parallel threads, so
//! the cases run in CHILD processes of this test binary, each with its own
//! `MOON_BIN` — no `set_var` in the parent.

mod common;

use std::process::Command;

const CHILD: &str = "MOON_BIN_PIN_CHILD";
const TEST: &str = "moon_bin_pin_resolution";

/// Run this test binary's [`TEST`] in child mode with `MOON_BIN` = `pin`
/// (unset when `None`); returns (success, stdout + stderr).
fn child(pin: Option<&str>) -> (bool, String) {
    let exe = std::env::current_exe().expect("current_exe");
    let mut cmd = Command::new(exe);
    cmd.args(["--exact", TEST, "--nocapture", "--test-threads=1"])
        .env(CHILD, "1");
    match pin {
        Some(p) => cmd.env("MOON_BIN", p),
        None => cmd.env_remove("MOON_BIN"),
    };
    let out = cmd.output().expect("spawn the child test");
    let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&out.stderr));
    (out.status.success(), text)
}

#[test]
fn moon_bin_pin_resolution() {
    if std::env::var_os(CHILD).is_some() {
        // Child mode: resolve and report; a panic fails the child.
        let p = common::find_moon_binary();
        println!("RESOLVED<{}>", p.display());
        return;
    }

    // A pin that names no file: the child must fail, naming the pin.
    let missing = "/nonexistent/fix3b-cm/moon-pin-typo";
    let (ok, out) = child(Some(missing));
    assert!(
        !ok && out.contains("does not name a file") && out.contains(missing),
        "MOON_BIN={missing} did not fail loudly — the suite would have run \
         against another binary. Child output:\n{out}"
    );

    // A pin that names a file is used verbatim (this binary stands in for
    // one: resolution never executes it).
    let exe = std::env::current_exe().expect("current_exe");
    let exe = exe.to_string_lossy().into_owned();
    let (ok, out) = child(Some(&exe));
    assert!(
        ok && out.contains(&format!("RESOLVED<{exe}>")),
        "a valid MOON_BIN pin was not used verbatim:\n{out}"
    );

    // Unset and empty both fall back to the binary Cargo built.
    let cargo_bin = env!("CARGO_BIN_EXE_moon");
    for pin in [None, Some(""), Some("   ")] {
        let (ok, out) = child(pin);
        assert!(
            ok && out.contains(&format!("RESOLVED<{cargo_bin}>")),
            "MOON_BIN={pin:?} did not fall back to CARGO_BIN_EXE_moon:\n{out}"
        );
    }
}
