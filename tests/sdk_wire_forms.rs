//! ADD task `sdk-wire-form-fixes` — the SDK-to-server wire-form sweep.
//!
//! One rule: **every command name the first-party SDKs send is one this server
//! answers.** Three did not (`MQ.PUSH`, `MQ.POP`, `FT.UPSERT`), each returning
//! `ERR unknown command` on its first round trip, so no caller could ever have
//! depended on their behaviour — only on the code compiling.
//!
//! They shipped because **no CI workflow references `sdk/` at all**: the SDK
//! crate is not in the workspace, is not built, and is not tested (the SDK's own
//! `tests/integration.rs` is entirely `#[ignore]`d). This suite is the guard,
//! and it lives in the MAIN repo's test tree precisely so it runs on every PR
//! without a new job.
//!
//! Why it SENDS each command rather than consulting the registry: the two
//! disagree in BOTH directions. `FT.AGGREGATE` dispatches fine yet answers
//! `COMMAND INFO` with nothing, so a registry lookup would flag a working
//! command; and the `FT.` dispatcher swallows unknown `FT.*` names before the
//! top-level registry sees them, so a registry lookup would MISS `FT.UPSERT`.
//! Sending is the only oracle that is right both ways.
//!
//! An arity error is a PASS. It proves the name reached a handler, which is the
//! whole question; demanding success would mean calling all ~125 commands
//! correctly and would rot within a milestone.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Scraping the SDK sources
// ---------------------------------------------------------------------------

/// Every command-name literal the Rust SDK sends, with the file it came from.
fn rust_sdk_commands(root: &std::path::Path) -> Vec<(String, String)> {
    let dir = root.join("sdk/rust/src");
    let mut out = Vec::new();
    for entry in std::fs::read_dir(&dir).expect("sdk/rust/src must exist") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let src = std::fs::read_to_string(&path).expect("read sdk source");
        let file = path
            .file_name()
            .and_then(|f| f.to_str())
            .unwrap_or("?")
            .to_string();
        for name in scan_literals(&src, "redis::cmd(\"") {
            out.push((name, format!("sdk/rust/src/{file}")));
        }
    }
    assert!(
        !out.is_empty(),
        "scraped zero commands from sdk/rust/src — the scraper is broken, and a \
         scraper that finds nothing passes this suite vacuously"
    );
    out
}

/// Every command-name literal the Python SDK sends, with the file it came from.
fn python_sdk_commands(root: &std::path::Path) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut dirs = vec![root.join("sdk/python/moondb")];
    while let Some(dir) = dirs.pop() {
        let Ok(rd) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in rd {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                if path.file_name().and_then(|f| f.to_str()) != Some("__pycache__") {
                    dirs.push(path);
                }
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("py") {
                continue;
            }
            let src = std::fs::read_to_string(&path).expect("read sdk source");
            let rel = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .to_string_lossy()
                .into_owned();
            // Python builds arg lists (`["FT.SEARCH", index, …]`) AND calls
            // `execute_command("FT.INFO", …)`; both are a quoted literal that
            // looks like a command name, so scan for the shape rather than for
            // one call form and silently miss the other.
            for name in scan_literals(&src, "\"") {
                if looks_like_a_command(&name) {
                    out.push((name, rel.clone()));
                }
            }
        }
    }
    assert!(
        !out.is_empty(),
        "scraped zero commands from sdk/python/moondb — the scraper is broken"
    );
    out
}

/// Pull every `<prefix>NAME"` occurrence where NAME is `A-Z0-9_.` only.
fn scan_literals(src: &str, prefix: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = src;
    while let Some(at) = rest.find(prefix) {
        rest = &rest[at + prefix.len()..];
        let Some(end) = rest.find('"') else { break };
        let candidate = &rest[..end];
        if !candidate.is_empty()
            && candidate
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_' || b == b'.')
        {
            out.push(candidate.to_string());
        }
    }
    out
}

/// A quoted uppercase literal only counts as a command if it plausibly names
/// one.
///
/// Deliberately narrow for the Python side, where the scan cannot key on a call
/// form: an over-eager filter turns a constant like `"ASC"` into a spurious
/// failure, and this suite must go red only for a real dead wire form. The
/// dotted namespace (`FT.`, `GRAPH.`, `TEMPORAL.`) is what every command the
/// Python SDK sends by literal has in common.
///
/// The leading-letter test is not decoration: without it `__version__ =
/// "0.1.0"` scrapes as a command and the sweep reports a version string as a
/// dead wire form. Caught on the first red run.
fn looks_like_a_command(name: &str) -> bool {
    name.contains('.') && name.len() > 3 && name.starts_with(|c: char| c.is_ascii_uppercase())
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn spawn_moon(dir: &std::path::Path) -> (Child, u16) {
    common::spawn_listening(|port| {
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                "1",
                "--appendonly",
                "no",
                // The shared /Volumes checkout hovers near the 5% diskfull
                // guard; a tripped guard would fail this suite for an unrelated
                // reason.
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
            .spawn()
            .expect("spawn moon")
    })
}

struct ServerGuard(Child);
impl Drop for ServerGuard {
    fn drop(&mut self) {
        common::sigkill(&mut self.0);
    }
}

struct Conn {
    s: TcpStream,
    buf: Vec<u8>,
    pos: usize,
}

impl Conn {
    fn new(port: u16) -> Self {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(s) = TcpStream::connect(format!("127.0.0.1:{port}")) {
                s.set_read_timeout(Some(Duration::from_secs(10))).ok();
                s.set_write_timeout(Some(Duration::from_secs(10))).ok();
                let mut c = Conn {
                    s,
                    buf: Vec::with_capacity(8 * 1024),
                    pos: 0,
                };
                if c.s.write_all(b"PING\r\n").is_ok() && c.line().contains("PONG") {
                    return c;
                }
            }
            assert!(
                Instant::now() < deadline,
                "server on {port} never answered PING"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn line(&mut self) -> String {
        loop {
            if let Some(rel) = self.buf[self.pos..].windows(2).position(|w| w == b"\r\n") {
                let start = self.pos;
                let end = start + rel;
                let out = String::from_utf8_lossy(&self.buf[start..end]).into_owned();
                self.pos = end + 2;
                return out;
            }
            let mut chunk = [0u8; 8 * 1024];
            let n = self.s.read(&mut chunk).expect("read");
            assert!(n > 0, "connection closed mid-frame");
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }

    /// Send `parts` as a RESP array and return the FIRST line of the reply.
    ///
    /// Only the first line is needed: every "unknown command" answer is a
    /// single-line error, so a reply that is not one is a pass regardless of
    /// what follows. A multi-line reply leaves a tail in the buffer, which is
    /// why every probe takes its own connection.
    fn probe(&mut self, parts: &[&str]) -> String {
        let mut req = format!("*{}\r\n", parts.len());
        for p in parts {
            req.push_str(&format!("${}\r\n{p}\r\n", p.len()));
        }
        self.s.write_all(req.as_bytes()).expect("write probe");
        self.line()
    }
}

/// `true` when the server did not recognise the command NAME at all.
///
/// Two distinct replies mean the same thing: the top-level registry gate
/// (`src/server/conn/shared.rs`) answers `unknown command` on a lookup miss,
/// and the `FT.` dispatcher answers `unknown FT.* command` for a name it does
/// not handle. Neither reaches a handler.
fn is_unknown(reply: &str) -> bool {
    let r = reply.to_ascii_lowercase();
    r.starts_with("-err unknown command") || r.starts_with("-err unknown ft.")
}

/// The optional server feature a command name belongs to, if any.
///
/// `graph` and `text-index` are DEFAULT features, but CI's portability leg
/// builds `--no-default-features --features runtime-tokio,jemalloc` and so has
/// neither. `GRAPH.*` and `FT.AGGREGATE` are then genuinely absent, and
/// correctly so — a server that was not built with a feature is not missing a
/// command, and the SDK is not wrong to offer helpers for it.
///
/// Returns `Some(feature_name)` for a gated command, `None` for one that every
/// build must answer.
fn gated_by(cmd: &str) -> Option<&'static str> {
    if cmd.starts_with("GRAPH.") {
        return Some("graph");
    }
    // FT.AGGREGATE lives in `vector_search::ft_aggregate`, behind `text-index`.
    // The rest of FT.* is the vector engine, which is unconditional.
    if cmd == "FT.AGGREGATE" {
        return Some("text-index");
    }
    None
}

/// Whether THIS test binary's server was built with `feature`.
///
/// Integration tests compile against the crate's active feature set, so
/// `cfg!` here reports what the spawned server actually supports.
fn feature_enabled(feature: &str) -> bool {
    match feature {
        "graph" => cfg!(feature = "graph"),
        "text-index" => cfg!(feature = "text-index"),
        // An unknown gate must not silently excuse a command.
        _ => true,
    }
}

// ---------------------------------------------------------------------------
// The sweep
// ---------------------------------------------------------------------------

/// RED before the fixes: names `MQ.PUSH`, `MQ.POP`, `FT.UPSERT` (genuinely
/// dead) and `TXN` (dispatched by an intercept but missing from the registry,
/// so the bare name falls through to the gate).
#[test]
fn swf1_every_sdk_command_is_answered() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut all = rust_sdk_commands(root);
    all.extend(python_sdk_commands(root));
    all.sort();
    all.dedup();

    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path());
    let _guard = ServerGuard(child);

    let mut dead = Vec::new();
    let mut skipped = Vec::new();
    for (cmd, file) in &all {
        if let Some(feature) = gated_by(cmd)
            && !feature_enabled(feature)
        {
            skipped.push(format!("{cmd} (feature `{feature}` off)"));
            continue;
        }
        let reply = Conn::new(port).probe(&[cmd]);
        if is_unknown(&reply) {
            dead.push(format!("  {cmd}  (sent from {file})  ->  {reply}"));
        }
    }

    // Announced, never silent: a sweep that quietly shrinks its own scope
    // reads as "everything passed" when it means "less was checked". The
    // default build skips nothing, so this list is empty in the leg that
    // matters most.
    if !skipped.is_empty() {
        println!(
            "swf1: {} of {} names skipped — server built without their feature: {}",
            skipped.len(),
            all.len(),
            skipped.join(", ")
        );
    }

    assert!(
        dead.is_empty(),
        "{} of {} SDK command names are not answered by this server:\n{}\n\n\
         Either the command does not exist — then REMOVE the helper that sends \
         it, do not add a server feature to satisfy an SDK method — or it is \
         dispatched by an intercept and missing from `src/command/metadata.rs`, \
         which also makes it invisible to COMMAND INFO. Add the registry entry.\n\n\
         (If the command belongs to an optional feature this build lacks, it \
         belongs in `gated_by` — but check first that it is genuinely gated, \
         rather than dead.)",
        dead.len(),
        all.len() - skipped.len(),
        dead.join("\n")
    );
}

/// Pins WHY the sweep sends rather than reads the registry.
///
/// `FT.AGGREGATE` dispatches today but answers `COMMAND INFO` with nothing, so
/// a registry-backed sweep would have failed a working command. This test goes
/// red if someone "simplifies" the sweep that way.
#[test]
#[cfg_attr(
    not(feature = "text-index"),
    ignore = "FT.AGGREGATE is behind `text-index`, which this build lacks"
)]
fn swf2_a_dispatchable_command_is_not_flagged_by_the_sweep() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path());
    let _guard = ServerGuard(child);

    let reply = Conn::new(port).probe(&["FT.AGGREGATE"]);
    assert!(
        !is_unknown(&reply),
        "FT.AGGREGATE must be answered — it is the case that separates \
         'send the command' from 'read the registry': {reply}"
    );
}

/// RED before the registry entries. A command a client cannot discover is,
/// for every driver that introspects before calling, a command that does not
/// exist.
#[test]
fn swf3_intercept_dispatched_commands_are_introspectable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path());
    let _guard = ServerGuard(child);

    for cmd in ["FT.AGGREGATE", "TXN"] {
        let mut c = Conn::new(port);
        // `COMMAND INFO X` always answers a 1-element array, so the OUTER
        // header is `*1` whether or not the command is known — checking only
        // that line is a vacuous assertion, which is exactly what the first
        // draft of this test did and why it passed against an unfixed server.
        // The answer is in the element: a null for an unknown command, a
        // 10-field array for a known one.
        let outer = c.probe(&["COMMAND", "INFO", cmd]);
        assert!(
            outer.starts_with('*'),
            "COMMAND INFO {cmd} did not answer an array: {outer}"
        );
        let element = c.line();
        assert!(
            !element.starts_with("$-1") && element != "_" && !element.starts_with("*-1"),
            "COMMAND INFO {cmd} answered a null element ({element}) — {cmd} \
             dispatches, so a client that introspects before calling wrongly \
             concludes it is unsupported. Add it to src/command/metadata.rs."
        );
    }
}

/// Pins the risk named at the contract freeze: `TXN` is served by an intercept
/// in `src/command/transaction.rs` that runs BEFORE the registry gate, so
/// adding a registry entry must make it DISCOVERABLE without rerouting it.
///
/// Before the entry, bare `TXN` answers `unknown command`; after, it must
/// answer a wrong-arity error — and the three real subcommands must be
/// untouched, because they never reach the gate.
#[test]
fn swf3b_registering_txn_does_not_reroute_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path());
    let _guard = ServerGuard(child);

    let begin = Conn::new(port).probe(&["TXN", "BEGIN"]);
    assert!(
        begin.starts_with("+OK"),
        "TXN BEGIN must still be served by the intercept: {begin}"
    );

    let abort = Conn::new(port).probe(&["TXN", "ABORT"]);
    assert!(
        abort.starts_with("+OK") || abort.to_ascii_lowercase().contains("transaction"),
        "TXN ABORT must still reach the intercept: {abort}"
    );

    let bare = Conn::new(port).probe(&["TXN"]);
    assert!(
        bare.to_ascii_lowercase()
            .contains("wrong number of arguments"),
        "a bare TXN must answer a wrong-arity error, not `unknown command` — \
         the command exists, it is the registry entry that is missing: {bare}"
    );
}
