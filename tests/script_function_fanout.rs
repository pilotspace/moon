//! Scripting state must be visible from EVERY shard, not just the one whose
//! connection created it. (moon#515, moon#514)
//!
//! Two defects with one shape — "the routing key is not every piece of state
//! the command needs", the scripting half of the family that moon#508 and
//! moon#592 opened.
//!
//! **moon#515** — `EVAL` caches the script body only in the executing shard's
//! `ScriptCache`. `SCRIPT LOAD` fans out; `EVAL` did not. So the Redis idiom
//! "`EVAL` once, then `EVALSHA` by sha" answered `NOSCRIPT` for every key that
//! routed to a shard which had never seen the body. Measured on the unfixed
//! build at `--shards 4`: one bare `EVAL`, then `EVALSHA` on 12 other keys →
//! **ok=2, NOSCRIPT=10**. After `SCRIPT LOAD`, 12/12.
//!
//! **moon#514** — `FCALL` was broken three ways at once, each masking the
//! next:
//!
//! 1. it took the old `validate_keys_same_shard` path, so a single key that
//!    simply lived elsewhere was refused `CROSSSLOT` (identical to moon#508);
//! 2. the `FunctionRegistry` was built PER CONNECTION, so `FUNCTION LOAD` was
//!    invisible even to the next connection on the same shard, and `FUNCTION
//!    LOAD` never fanned out to the other shards either;
//! 3. callbacks were invoked with NO arguments, so the documented Redis
//!    signature `function(keys, args)` received `nil`.
//!
//! Measured on the unfixed build at `--shards 4`, one `FUNCTION LOAD` then 8
//! single-key `FCALL`s on fresh connections: **5/8 `CROSSSLOT`, 3/8 `ERR
//! Function not found`, 0/8 succeeded**, and `FUNCTION LIST` on a fresh
//! connection returned an empty array. Fixing any one alone changes only which
//! error you get, which is why they land together.
//!
//! Every assertion is written so the FIXED state is the passing state, and the
//! multi-key ones loop over key placements: at `--shards 4` a single trial
//! passes by luck ~25% of the time.

mod common;

use common::Conn;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

/// Four shards: a key is remote ~75% of the time, so a build that only ever
/// serves scripting state locally cannot pass by luck.
const SHARDS: &str = "4";
/// Distinct keys per assertion. At p(remote)=0.75 the chance that all 12 land
/// on the connection's own shard — and vacuously pass — is under 1e-7.
const TRIALS: usize = 12;

/// A library exercising both calling conventions at once: `kv_set`/`kv_get`
/// take the documented `(keys, args)` PARAMETERS, `legacy_get` reads the
/// `KEYS` global the way an EVAL-style body would. Both must work.
const LIB: &str = "#!lua name=fanoutlib\n\
redis.register_function('kv_set', function(keys, args) \
return redis.call('set', keys[1], args[1]) end)\n\
redis.register_function('kv_get', function(keys, args) \
return redis.call('get', keys[1]) end)\n\
redis.register_function('legacy_get', function() \
return redis.call('get', KEYS[1]) end)\n\
redis.register_function('two_key', function(keys, args) \
return redis.call('set', keys[1], keys[2]) end)\n";

/// `return redis.call('set', KEYS[1], 'v')` — the shape a cache or lock
/// wrapper `EVAL`s once and then re-issues by sha.
const SET_SCRIPT: &str = "return redis.call('set',KEYS[1],'v')";

struct Moon {
    child: Child,
    port: u16,
    /// Every data dir `spawn_listening` created, in attempt order; the LAST is
    /// the live one.
    tmp_dirs: std::rc::Rc<std::cell::RefCell<Vec<std::path::PathBuf>>>,
}

impl Moon {
    fn live_dir(&self) -> std::path::PathBuf {
        self.tmp_dirs.borrow().last().cloned().unwrap_or_default()
    }
}

impl Drop for Moon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        // EVERY attempt, not just the winner: `spawn_listening` retries on a
        // busy port and each try creates its own data dir, so cleaning only
        // the final one leaks a directory per retry.
        for dir in self.tmp_dirs.borrow().iter() {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

fn spawn_moon(shards: &str) -> Moon {
    spawn_moon_env(shards, &[])
}

fn spawn_moon_env(shards: &str, env: &[(&str, &str)]) -> Moon {
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let tmp_dirs: std::rc::Rc<std::cell::RefCell<Vec<std::path::PathBuf>>> =
        std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let seen = std::rc::Rc::clone(&tmp_dirs);
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-fanout-{port}"));
        seen.borrow_mut().push(tmp_dir.clone());
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
                tmp_dir.to_str().unwrap(),
            ])
            .envs(env.iter().copied())
            // BOTH streams into one file. moon's `tracing` output goes to
            // STDOUT; discarding it and keeping only stderr leaves a 69-byte
            // file with an allocator notice in it, and any test that reads the
            // log to observe server behaviour silently sees nothing.
            .stdout(std::fs::File::create(tmp_dir.join("moon.log")).expect("create moon log"))
            .stderr(
                std::fs::File::options()
                    .append(true)
                    .open(tmp_dir.join("moon.log"))
                    .expect("open moon log for stderr"),
            )
            .spawn()
            .expect("spawn moon")
    });
    let moon = Moon {
        child,
        port,
        tmp_dirs,
    };
    // Patience, not a correctness bound: this suite starts a 4-shard server
    // per test, in parallel, and a machine that is also compiling can take
    // well past 30s to get one listening. A too-tight budget here surfaces as
    // "moon never became ready" attributed to a random test, which reads as a
    // scripting failure and is not one.
    let deadline = Instant::now() + Duration::from_secs(90);
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
    let log = std::fs::read_to_string(moon.live_dir().join("moon.log")).unwrap_or_default();
    panic!("moon never became ready on port {port}\n--- stderr ---\n{log}");
}

/// Run `body` once per key placement on a FRESH connection, collecting the
/// trials that came out wrong.
///
/// Fresh connections on purpose: which shard a connection lands on is what
/// decides whether its key — and its scripting state — is "local", so reusing
/// one connection samples a single placement TRIALS times instead of the
/// distribution. That is exactly how a per-connection registry passed review.
fn each_trial(
    port: u16,
    what: &str,
    tag: &str,
    mut body: impl FnMut(&mut Conn, &str) -> Option<String>,
) {
    let mut wrong: Vec<String> = Vec::new();
    for i in 0..TRIALS {
        let mut c = Conn::open(port);
        let key = format!("{tag}{i}");
        if let Some(why) = body(&mut c, &key) {
            wrong.push(format!("  key {key}: {why}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "{}/{} key placements wrong ({what}):\n{}",
        wrong.len(),
        TRIALS,
        wrong.join("\n")
    );
}

/// The sha1 of `script`, obtained from a THROWAWAY server.
///
/// This detour is the whole reason the moon#515 tests are not vacuous.
/// `SCRIPT LOAD` is the obvious way to learn a sha — and it is exactly the
/// command that already fanned out, so calling it on the server under test
/// publishes the body to every shard and makes the assertion pass against the
/// BROKEN build. Verified: with the `EVAL` fan-out deleted, the sff1/sff2
/// pair still passed until the sha came from here instead.
///
/// A one-shard oracle also proves the sha is a pure function of the body, not
/// of anything the server under test did.
/// The SHA1 of [`SET_SCRIPT`], computed OUTSIDE this codebase.
///
/// Non-vacuity depends on this literal. The first draft of these tests asked
/// the server under test for the digest (`SCRIPT LOAD`) — the one scripting
/// command that ALREADY fanned out — so the seeding call fixed the very defect
/// the test then failed to detect, and the suite passed against the broken
/// build. A digest moon cannot influence is the fix for that; a hard-coded
/// literal is strictly stronger than a second moon process, and it also stops
/// this suite spawning three extra servers, which is what made it flake under
/// a loaded machine (a server that needs >30s to bind fails the readiness
/// wait, and the failure reads as a scripting bug).
///
/// Redis defines the EVAL cache key as the SHA1 of the script body, so this is
/// an interoperability constant, not an implementation detail. Recompute with:
///
/// ```text
/// printf %s "return redis.call('set',KEYS[1],'v')" | shasum -a 1
/// ```
const SET_SCRIPT_SHA: &str = "7b6149474e93dc3661a8a719fd6876221f0e5f0c";

fn load_lib(port: u16) -> Conn {
    let mut c = Conn::open(port);
    let r = c.send(&["FUNCTION", "LOAD", LIB]);
    assert!(
        r.contains("fanoutlib"),
        "FUNCTION LOAD did not accept the library: {r:?}"
    );
    c
}

// ===========================================================================
// moon#515 — EVAL must fan its body out
// ===========================================================================

/// The reported defect: one bare `EVAL`, then `EVALSHA` of that sha on keys
/// that live on other shards. Redis caches an `EVAL`'d body server-wide, so
/// all 12 must run. Unfixed build: ok=2, NOSCRIPT=10.
#[test]
fn sff1_bare_eval_makes_its_sha_callable_from_every_shard() {
    let m = spawn_moon(SHARDS);
    let mut loader = Conn::open(m.port);
    // The ONLY thing that publishes the body. Deliberately not SCRIPT LOAD —
    // that path already fanned out, and using it here would make this test
    // pass against the broken build.
    let first = loader.send(&["EVAL", SET_SCRIPT, "1", "sff1seed"]);
    assert!(first.contains("OK"), "seed EVAL failed: {first:?}");
    // A literal, NOT `SCRIPT LOAD` here — see SET_SCRIPT_SHA.
    let sha = SET_SCRIPT_SHA;

    each_trial(
        m.port,
        "moon#515 — a script cached by EVAL must be EVALSHA-able from every \
         shard, not only the one that ran it",
        "sff1",
        |c, k| {
            let r = c.send(&["EVALSHA", sha, "1", k]);
            if r.contains("NOSCRIPT") {
                return Some(format!("EVALSHA replied NOSCRIPT ({r:?})"));
            }
            (!r.contains("OK")).then(|| format!("EVALSHA replied {r:?}, expected +OK"))
        },
    );
}

/// The same fact stated about the CACHE rather than about execution:
/// `SCRIPT EXISTS` must answer 1 on whichever shard a fresh connection lands.
///
/// Kept separate from sff1 because it fails for one reason only — a missing
/// fan-out — where sff1 could in principle also fail on routing.
///
/// # What this test does NOT prove
///
/// `SCRIPT EXISTS` is answered by the CONNECTION's shard, and which shard a
/// fresh connection lands on is up to the kernel's `SO_REUSEPORT` group.
///
/// On macOS that group is NOT balanced — MEASURED, not assumed: with shard 0's
/// fan-out wedged (`MOON_TEST_DROP_FANOUT_TO_SHARD=0`), eight consecutive
/// fresh connections all reported the same fan-out target set, i.e. they all
/// landed on the same shard. So on macOS every `TRIALS` connection here
/// samples ONE shard, and this assertion holds even against a build with no
/// fan-out at all.
///
/// moon exposes no per-connection shard id to assert the spread with, so the
/// non-vacuity guarantee is carried by **sff1**, which addresses shards by KEY
/// (deterministic hashing, no kernel involved) and which was verified to FAIL
/// against the pre-fix binary. Read this one as a cheaper corroboration on
/// Linux, never as the proof.
#[test]
fn sff2_eval_populates_the_script_cache_on_every_shard() {
    let m = spawn_moon(SHARDS);
    let sha = SET_SCRIPT_SHA;
    {
        // The ONLY thing that publishes the body on the server under test.
        let mut loader = Conn::open(m.port);
        let r = loader.send(&["EVAL", SET_SCRIPT, "1", "sff2seed"]);
        assert!(r.contains("OK"), "seed EVAL failed: {r:?}");
    }
    let mut missing = 0usize;
    for _ in 0..TRIALS {
        // SCRIPT EXISTS is answered by the CONNECTION's shard, so fresh
        // connections sample the shards.
        let r = Conn::open(m.port).send(&["SCRIPT", "EXISTS", sha]);
        if !r.contains(":1") {
            missing += 1;
        }
    }
    assert_eq!(
        missing, 0,
        "{missing}/{TRIALS} connections' shards had no cache entry for a sha \
         this server has already executed (moon#515: EVAL must fan out like \
         SCRIPT LOAD)"
    );
}

/// `--shards 1` has no fan-out to do and must be untouched: the whole
/// EVAL→EVALSHA idiom keeps working, and nothing new is refused.
#[test]
fn sff3_single_shard_eval_then_evalsha_unchanged() {
    let m = spawn_moon("1");
    let sha = SET_SCRIPT_SHA;
    let mut c = Conn::open(m.port);
    assert!(c.send(&["EVAL", SET_SCRIPT, "1", "sff3a"]).contains("OK"));
    for i in 0..TRIALS {
        let k = format!("sff3k{i}");
        let r = Conn::open(m.port).send(&["EVALSHA", sha, "1", &k]);
        assert!(
            r.contains("OK"),
            "single-shard EVALSHA regressed: {r:?} for {k}"
        );
    }
}

// ===========================================================================
// moon#514 — FCALL routing + FUNCTION fan-out + the (keys, args) convention
// ===========================================================================

/// Defects 1+2 together: a single-key `FCALL` must run wherever its key lives,
/// from a connection that never issued `FUNCTION LOAD`.
///
/// The write is read back through the NORMAL path — a plain `GET`, which
/// routes correctly — so a function that ran on the wrong shard's database
/// cannot fake success.
#[test]
fn sff4_single_key_fcall_runs_on_the_keys_shard() {
    let m = spawn_moon(SHARDS);
    let _loader = load_lib(m.port);
    each_trial(
        m.port,
        "moon#514 — FCALL must be routed to the shard owning its key, with the \
         library visible there",
        "sff4",
        |c, k| {
            let r = c.send(&["FCALL", "kv_set", "1", k, "w"]);
            if r.contains("CROSSSLOT") {
                return Some(format!("FCALL refused CROSSSLOT for ONE key ({r:?})"));
            }
            if r.contains("Function not found") {
                return Some(format!("library not present on that shard ({r:?})"));
            }
            if !r.contains("OK") {
                return Some(format!("FCALL replied {r:?}, expected +OK"));
            }
            let got = c.send(&["GET", k]);
            (got != "$1\r\nw\r\n")
                .then(|| format!("GET after FCALL replied {got:?} — write landed elsewhere"))
        },
    );
}

/// `FCALL_RO` takes the same routing path and must read the same value.
#[test]
fn sff5_single_key_fcall_ro_reads_the_keys_shard() {
    let m = spawn_moon(SHARDS);
    let _loader = load_lib(m.port);
    each_trial(
        m.port,
        "moon#514 — FCALL_RO must route like FCALL",
        "sff5",
        |c, k| {
            c.send(&["SET", k, "r"]);
            let r = c.send(&["FCALL_RO", "kv_get", "1", k]);
            (r != "$1\r\nr\r\n").then(|| format!("FCALL_RO replied {r:?}, expected the value"))
        },
    );
}

/// The guard must SURVIVE: an `FCALL` whose keys genuinely span shards is
/// still refused, because a function runs against one shard's database and
/// cannot reach another's. This is what stops the fix from being "delete the
/// check" — the mistake that would turn moon#514 into moon#592.
#[test]
fn sff6_genuinely_cross_shard_fcall_keys_are_still_rejected() {
    let m = spawn_moon(SHARDS);
    let _loader = load_lib(m.port);
    let mut rejected = 0usize;
    let mut ran = 0usize;
    for i in 0..24 {
        let mut c = Conn::open(m.port);
        let k1 = format!("{{x{i}}}one");
        let k2 = format!("{{y{i}}}two");
        let r = c.send(&["FCALL", "two_key", "2", &k1, &k2]);
        if r.contains("CROSSSLOT") {
            rejected += 1;
        } else if r.contains("OK") {
            ran += 1;
        } else {
            panic!("2-key FCALL replied neither +OK nor CROSSSLOT: {r:?}");
        }
    }
    assert_eq!(
        rejected + ran,
        24,
        "every pair must be classified as exactly one of ran / CROSSSLOT"
    );
    assert!(
        rejected > 0,
        "no 2-key FCALL was rejected across 24 tag pairs — the cross-shard \
         guard is gone, so a function can now write a key on a shard where no \
         normally-routed access will ever find it (that is moon#592). Fixing \
         moon#514 must ROUTE single-shard calls, not delete the check."
    );

    // Deterministic counterpart: a SHARED tag is co-located by construction,
    // so it must run. (Whether any {x<i>}/{y<i>} pair co-locates is chance.)
    let mut c = Conn::open(m.port);
    let r = c.send(&["FCALL", "two_key", "2", "{same}one", "{same}two"]);
    assert!(
        r.contains("OK") && !r.contains("CROSSSLOT"),
        "keys sharing a hash tag cannot cross shards and must run — got {r:?}"
    );
    let got = c.send(&["GET", "{same}one"]);
    assert!(
        got.contains("{same}two"),
        "co-located 2-key FCALL did not write what it claimed: {got:?}"
    );
}

/// Defect 2 stated about the REGISTRY: the library a connection loads must be
/// visible to every other connection, on every shard.
///
/// Unfixed build: `FUNCTION LIST` on a fresh connection returned `*0` — the
/// registry was per CONNECTION, which also made the bug reproduce at
/// `--shards 1`.
#[test]
fn sff7_loaded_library_is_visible_from_every_connection() {
    let m = spawn_moon(SHARDS);
    let _loader = load_lib(m.port);
    let mut blind = 0usize;
    for _ in 0..TRIALS {
        let r = Conn::open(m.port).send(&["FUNCTION", "LIST"]);
        if !r.contains("fanoutlib") {
            blind += 1;
        }
    }
    assert_eq!(
        blind, 0,
        "{blind}/{TRIALS} fresh connections could not see a library this \
         server accepted (moon#514: the FunctionRegistry must be per-shard and \
         FUNCTION LOAD must fan out)"
    );
}

/// The reverse direction, and the reason `DELETE`/`FLUSH` fan out too: a
/// delete that only reached one shard leaves the library callable elsewhere,
/// which is a worse lie than never having deleted it.
#[test]
fn sff8_function_delete_and_flush_reach_every_shard() {
    let m = spawn_moon(SHARDS);
    let mut loader = load_lib(m.port);
    assert!(
        loader
            .send(&["FUNCTION", "DELETE", "fanoutlib"])
            .contains("OK")
    );
    for i in 0..TRIALS {
        let k = format!("sff8d{i}");
        let r = Conn::open(m.port).send(&["FCALL", "kv_set", "1", &k, "x"]);
        assert!(
            r.contains("Function not found"),
            "library still callable after FUNCTION DELETE ({r:?}) — the delete \
             did not reach that shard"
        );
    }

    // FLUSH must behave the same way.
    let mut loader = load_lib(m.port);
    assert!(loader.send(&["FUNCTION", "FLUSH"]).contains("OK"));
    for i in 0..TRIALS {
        let k = format!("sff8f{i}");
        let r = Conn::open(m.port).send(&["FCALL", "kv_set", "1", &k, "x"]);
        assert!(
            r.contains("Function not found"),
            "library still callable after FUNCTION FLUSH ({r:?})"
        );
    }
}

/// Defect 3: the documented Redis Functions signature. `redis.register_function
/// ('f', function(keys, args) ... end)` is the form in every Redis example;
/// moon invoked callbacks with no arguments, so `keys` was `nil` and any
/// idiomatic library died with "attempt to index a nil value (local 'keys')".
///
/// The EVAL-style `KEYS` global must keep working alongside it.
#[test]
fn sff9_callbacks_receive_keys_and_args_and_the_globals_still_work() {
    let m = spawn_moon(SHARDS);
    let _loader = load_lib(m.port);
    each_trial(
        m.port,
        "moon#514 — a function callback must receive (keys, args), and the \
         KEYS/ARGV globals must keep working",
        "sff9",
        |c, k| {
            // (keys, args) parameters.
            let set = c.send(&["FCALL", "kv_set", "1", k, "p"]);
            if set.contains("nil value") {
                return Some(format!("callback got nil keys/args: {set:?}"));
            }
            if !set.contains("OK") {
                return Some(format!("FCALL kv_set replied {set:?}"));
            }
            // KEYS global, same key, same shard.
            let got = c.send(&["FCALL_RO", "legacy_get", "1", k]);
            (got != "$1\r\np\r\n")
                .then(|| format!("KEYS-global function replied {got:?}, expected the value"))
        },
    );
}

/// The repair leg must not paper over a real mistake: an unknown function name
/// still reports `Function not found` — once — rather than looping, hanging,
/// or being retried into a different error.
#[test]
fn sff10_unknown_function_still_reports_not_found() {
    let m = spawn_moon(SHARDS);
    let _loader = load_lib(m.port);
    for i in 0..TRIALS {
        let k = format!("sff10{i}");
        let r = Conn::open(m.port).send(&["FCALL", "no_such_function", "1", &k]);
        assert!(
            r.contains("Function not found"),
            "a typo'd function name must still be reported, got {r:?}"
        );
    }
}

/// `--shards 1` must be untouched by the FCALL routing change: no fan-out, no
/// hop, and — because the registry moved from per-connection to per-shard —
/// a library loaded on one connection is now visible on the next, which is
/// the SAME defect the multi-shard tests cover, minus the shards.
#[test]
fn sff11_single_shard_functions_unchanged_and_shared() {
    let m = spawn_moon("1");
    let _loader = load_lib(m.port);
    for i in 0..TRIALS {
        let k = format!("sff11{i}");
        let mut c = Conn::open(m.port);
        let r = c.send(&["FCALL", "kv_set", "1", &k, "s"]);
        assert!(
            r.contains("OK"),
            "single-shard FCALL regressed: {r:?} for {k}"
        );
        assert_eq!(
            c.send(&["GET", &k]),
            "$1\r\ns\r\n",
            "single-shard FCALL write not visible"
        );
    }
    assert!(
        Conn::open(m.port)
            .send(&["FUNCTION", "LIST"])
            .contains("fanoutlib"),
        "at --shards 1 a loaded library must still be visible to other \
         connections — the registry is per-shard, and there is one shard"
    );
}

// ===========================================================================
// Partial fan-out — the failure path, reached via MOON_TEST_DROP_FANOUT_TO_SHARD
// ===========================================================================

/// A `FUNCTION` mutation that did not reach every shard must SAY SO.
///
/// This is the branch that replaced the original design's repair leg, and it
/// is the whole reason the acks exist. Without it, a `FUNCTION LOAD` whose
/// replay was lost answers `+OK` over a registry where `FCALL` succeeds or
/// fails depending on which shard the key hashes to — silently, and
/// indefinitely. Adversarial review rejected the alternative (a later routed
/// call re-pushing the state) because the error it would trigger on is
/// indistinguishable from a `FUNCTION DELETE` that reached the target first,
/// so repairing on it resurrects deleted libraries.
///
/// `MOON_TEST_DROP_FANOUT_TO_SHARD` wedges exactly one shard's inbound pushes,
/// which is otherwise unreachable from an integration test.
///
/// EVERY shard is wedged, not one. A connection is never a target of its own
/// fan-out, so wedging a single shard makes the result depend on where the
/// kernel put the client — the first draft of this test named shard 3, the
/// client landed on shard 3, and it "passed" against a build that reports
/// nothing. Wedging all four is deterministic from any placement.
#[test]
fn sff12_partial_function_fanout_is_reported_not_swallowed() {
    let m = spawn_moon_env(SHARDS, &[("MOON_TEST_DROP_FANOUT_TO_SHARD", "0,1,2,3")]);
    let r = Conn::open(m.port).send(&["FUNCTION", "LOAD", LIB]);
    assert!(
        r.contains("partialfanout"),
        "FUNCTION LOAD answered {r:?} with every peer shard's fan-out wedged; a \
         mutation the server could not publish must not be reported as if it \
         had been (moon#514)"
    );
    // ...and it says HOW partial, because "1 of 4" is what tells an operator
    // this is a fan-out problem rather than a rejected body.
    assert!(
        r.contains("1 of 4 shards"),
        "partial-fan-out error does not report the reach: {r:?}"
    );
}

/// A dropped `EVAL` fan-out must be RETRIED by the next `EVAL` of the same
/// body, not written off because the body is already in the local cache.
///
/// The natural implementation — gate the fan-out on the `ScriptCache` insert —
/// has exactly that bug: the failed publish still leaves the entry behind, so
/// every later `EVAL` takes the "already known" early return and the divergence
/// becomes permanent for a self-inflicted reason. `ScriptCache` therefore
/// tracks "published" separately from "cached".
///
/// Observable without metrics: with one shard wedged, `EVAL` keeps working
/// everywhere (it carries its own body) and `EVALSHA` degrades to `NOSCRIPT`
/// only for keys on the wedged shard — never to a wrong answer.
#[test]
fn sff13_dropped_eval_fanout_degrades_to_noscript_never_to_wrong_data() {
    let m = spawn_moon_env(SHARDS, &[("MOON_TEST_DROP_FANOUT_TO_SHARD", "0,1,2,3")]);
    const SEEDS: usize = 8;
    {
        let mut seed = Conn::open(m.port);
        for i in 0..SEEDS {
            let key = format!("sff13seed{i}");
            let r = seed.send(&["EVAL", SET_SCRIPT, "1", &key]);
            assert!(r.contains("OK"), "EVAL itself must not fail: {r:?}");
        }
    }
    // The republish itself, observed directly. Each EVAL of this ONE body must
    // re-attempt the publish while it is still owed, and each failed attempt
    // logs a drop. A gate that keyed off the local cache entry instead would
    // log exactly one and then go quiet forever — the body is cached, so the
    // "already known" early return swallows every retry. Counting the warnings
    // is the only way to tell those two apart from outside the process.
    std::thread::sleep(Duration::from_millis(200));
    let path = m.live_dir().join("moon.log");
    let log = std::fs::read_to_string(&path).expect("moon log must be readable");
    // One publish ROUND logs one line per wedged peer, so the bar has to be
    // stated in rounds: `>= 2 lines` is met by a SINGLE round at --shards 4
    // and proves nothing. (Measured: the "claim the publish regardless"
    // mutation logs exactly PER_ROUND lines and passed a `>= 2` assertion.)
    const PER_ROUND: usize = 3; // SHARDS - 1 peers, all wedged
    let lines = log.matches("script_load fan-out to shard").count();
    assert!(
        lines > PER_ROUND,
        "{SEEDS} EVALs of the same body with every peer shard wedged logged \
         {lines} drop line(s) — at most one publish round. A failed fan-out \
         must be retried by the next EVAL, not written off because the body is \
         already in the local cache"
    );
    // EVALSHA may answer NOSCRIPT for keys on the wedged shard. What it must
    // never do is answer OK without the write, or answer some other error.
    for i in 0..TRIALS {
        let key = format!("sff13:{i}");
        let mut c = Conn::open(m.port);
        let r = c.send(&["EVALSHA", SET_SCRIPT_SHA, "1", &key]);
        if r.contains("NOSCRIPT") {
            continue;
        }
        assert!(
            r.contains("OK"),
            "EVALSHA answered neither OK nor NOSCRIPT for {key}: {r:?}"
        );
        let got = c.send(&["GET", &key]);
        assert!(
            got.contains('v'),
            "EVALSHA said OK for {key} but the write is not readable: {got:?}"
        );
    }
}
