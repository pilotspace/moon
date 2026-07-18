//! jemalloc allocator-config re-spawn: `--memory-arenas-cap` / `--memory-thp`.
//!
//! jemalloc reads `opt.narenas` / `opt.thp` exactly once at process init from
//! the `_rjem_malloc_conf` symbol **or** the `_RJEM_MALLOC_CONF` env var (env
//! wins); `mallctl` after init is a documented no-op for these opts.
//! Re-spawning via `execve` with the env var set is therefore the only
//! correct path for a CLI override — see
//! [`maybe_respawn_with_memory_overrides`], which must run as the FIRST
//! statement of `main()`, before tracing init and clap parse.
//!
//! The baked-in defaults live in the `_rjem_malloc_conf` static exported from
//! `main.rs`; [`build_malloc_conf`] must stay byte-identical to that string
//! (modulo the two overrides).
//!
//! **CLI-only limitation:** the pre-clap argv scan cannot see values sourced
//! from `moon.conf` (the conf file is parsed long after jemalloc init), so
//! conf-file values for these two options can never reach the allocator.
//! [`warn_if_conf_only_overrides`] runs after config parse and turns that
//! previously-silent no-op into a loud startup warning.

// This module is compiled into the `moon` binary only (declared from
// main.rs); the library crate does not include it.

/// Sentinel env var preventing infinite re-spawn (shared between both flags —
/// a re-spawned child must never re-spawn again).
#[cfg(all(feature = "jemalloc", unix))]
const SENTINEL: &str = "MOON_ARENAS_CAP_APPLIED";

/// tikv-jemalloc-sys builds with prefix `_rjem_`, so the env var is prefixed.
#[cfg(all(feature = "jemalloc", unix))]
const MALLOC_CONF_ENV: &str = "_RJEM_MALLOC_CONF";

/// Result of the lightweight pre-clap argv scan for the two allocator
/// override flags. Pure data — see [`scan_argv`].
#[cfg(any(feature = "jemalloc", test))]
#[derive(Debug, PartialEq, Eq)]
struct ScannedOverrides {
    /// `--memory-arenas-cap N` / `=N` value, if present and parseable.
    narenas: Option<u32>,
    /// Whether `--memory-thp` appeared.
    thp: bool,
}

/// Lightweight scan of raw argv for `--memory-arenas-cap N` (or `=N`) and
/// `--memory-thp`. We can't use clap here because `clap::parse()` requires
/// the full config struct, and the caller needs the answer BEFORE jemalloc
/// reads its config. Scans the FULL argv (no early break) so both flags are
/// found regardless of order. Malformed values resolve to `None`/absent —
/// clap rejects those command lines later anyway, before the server starts.
///
/// Operates on `OsString` argv (via `as_encoded_bytes`) so non-UTF-8 argv
/// never panics, and so [`maybe_respawn_with_memory_overrides`] can re-exec
/// with the original untouched argv.
#[cfg(any(feature = "jemalloc", test))]
fn scan_argv(args: &[std::ffi::OsString]) -> ScannedOverrides {
    let mut scanned = ScannedOverrides {
        narenas: None,
        thp: false,
    };
    let mut i = 1;
    while i < args.len() {
        let a = args[i].as_encoded_bytes();
        // Out-of-range values (clap enforces 1..=256) are treated as absent:
        // jemalloc's own abort_conf:true would ABORT the process on e.g.
        // narenas:0 before clap ever prints its friendly range error, so the
        // scan must never let such a value reach the conf string.
        let parse_narenas = |s: &str| s.parse::<u32>().ok().filter(|n| (1..=256).contains(n));
        if let Some(rest) = a.strip_prefix(b"--memory-arenas-cap=") {
            scanned.narenas = std::str::from_utf8(rest).ok().and_then(parse_narenas);
            i += 1;
            continue;
        }
        if a == b"--memory-arenas-cap" {
            if i + 1 < args.len() {
                scanned.narenas = args[i + 1].as_os_str().to_str().and_then(parse_narenas);
            }
            i += 2;
            continue;
        }
        if a == b"--memory-thp" {
            scanned.thp = true;
            i += 1;
            continue;
        }
        i += 1;
    }
    scanned
}

/// Builds the `_RJEM_MALLOC_CONF` value from the baked-in static defaults
/// (the `malloc_conf` static in `main.rs` — keep byte-identical), an optional
/// `narenas` override, and an optional `thp:always` opt-in.
///
/// Pure and side-effect-free — this is the seam
/// [`maybe_respawn_with_memory_overrides`] uses to compose
/// `--memory-arenas-cap` and `--memory-thp` into a single conf string, and
/// the seam the unit tests below exercise directly (no process re-spawn
/// needed to test the string logic).
#[cfg(any(feature = "jemalloc", test))]
fn build_malloc_conf(narenas: u32, thp: bool) -> String {
    let mut conf = format!(
        "narenas:{narenas},background_thread:true,metadata_thp:auto,dirty_decay_ms:1000,muzzy_decay_ms:5000,abort_conf:true"
    );
    if thp {
        // thp:always requests huge-page-backed allocation for jemalloc's
        // large-slab arena allocations (the value heap), on top of the
        // always-on metadata_thp:auto (which only covers jemalloc's own
        // bookkeeping structures, not application allocations). The two
        // opts are independent and jemalloc accepts both together.
        conf.push_str(",thp:always");
    }
    conf
}

/// Whether jemalloc's `thp:always` opt is meaningful on this platform.
///
/// Verified experimentally (2026-07-18, aarch64-apple-darwin): jemalloc has
/// no THP support outside Linux (THP is a Linux kernel feature — sysfs
/// `transparent_hugepage/` + `MADV_HUGEPAGE`). With the baked-in
/// `abort_conf:true`, feeding it `thp:always` on macOS does not silently
/// ignore the opt — jemalloc logs `<jemalloc>: No THP support: thp:always`
/// and **aborts the process** at init, before a single connection is
/// accepted. `--memory-thp` must never reach the conf string outside Linux;
/// `is_linux` is injected (rather than reading `cfg!` inline) so this
/// decision is unit-testable on any host platform.
#[cfg(any(feature = "jemalloc", test))]
const fn thp_supported(is_linux: bool) -> bool {
    is_linux
}

/// Pure decision function: given the requested `--memory-arenas-cap` /
/// `--memory-thp` overrides and whether the operator has already set
/// `_RJEM_MALLOC_CONF` in the environment, decides whether a re-spawn is
/// needed and what conf string it should carry.
///
/// Returns `None` when no re-spawn is needed — either the request matches
/// the baked-in default (`narenas: 8`, no thp), or an operator-set
/// `_RJEM_MALLOC_CONF` takes precedence (the caller is responsible for
/// warning in that case). Returns `Some(conf)` otherwise.
#[cfg(any(feature = "jemalloc", test))]
fn resolve_malloc_conf_override(
    narenas: u32,
    thp: bool,
    operator_env_already_set: bool,
) -> Option<String> {
    if narenas == 8 && !thp {
        return None; // matches baked-in default; nothing to override
    }
    if operator_env_already_set {
        return None; // operator env wins
    }
    Some(build_malloc_conf(narenas, thp))
}

/// Re-spawn the current process with `_RJEM_MALLOC_CONF` when the operator
/// passes `--memory-arenas-cap N` (N != 8) and/or `--memory-thp`, composing
/// both into a single conf string and a single process re-spawn.
///
/// tikv-jemallocator uses prefixed symbols (`_rjem_malloc_conf`), so the env
/// var that overrides the config is `_RJEM_MALLOC_CONF` (with
/// `JEMALLOC_CPREFIX` = `_rjem_`). Must run before tracing init and clap
/// parse — the re-exec'd child must see the env var before jemalloc's
/// one-time init. Sentinel [`SENTINEL`] prevents infinite re-spawn.
#[cfg(all(feature = "jemalloc", unix))]
pub fn maybe_respawn_with_memory_overrides() -> anyhow::Result<()> {
    use std::env;
    use std::ffi::OsString;
    use std::os::unix::process::CommandExt;

    if env::var_os(SENTINEL).is_some() {
        return Ok(());
    }

    // args_os() avoids panicking on non-UTF-8 argv and preserves the
    // original OsString argv for the re-spawn below (CodeRabbit).
    let args: Vec<OsString> = env::args_os().collect();
    let scanned = scan_argv(&args);
    let narenas = scanned.narenas.unwrap_or(8);

    // jemalloc has no THP support outside Linux; with abort_conf:true,
    // feeding it thp:always on e.g. macOS aborts the process at init
    // (verified experimentally) instead of ignoring the opt. Downgrade the
    // request to a warning rather than let that reach the conf string.
    let effective_thp = thp_supported(cfg!(target_os = "linux")) && scanned.thp;
    if scanned.thp && !effective_thp {
        eprintln!(
            "WARN: --memory-thp requires Linux (jemalloc has no THP support on \
             this platform); ignoring the flag rather than risk an allocator \
             abort under abort_conf:true"
        );
    }

    let operator_env_set = env::var_os(MALLOC_CONF_ENV).is_some();
    let Some(conf_val) = resolve_malloc_conf_override(narenas, effective_thp, operator_env_set)
    else {
        // Either nothing was requested (baked-in default already in effect,
        // the common case) or the operator's own env var takes precedence.
        // Only warn for the latter -- a request was made but silently
        // dropped, which the operator should know about.
        if operator_env_set && (narenas != 8 || effective_thp) {
            eprintln!(
                "WARN: --memory-arenas-cap/--memory-thp ignored because {} is already set",
                MALLOC_CONF_ENV
            );
        }
        return Ok(());
    };

    let exe = env::current_exe()?;
    // unix-only: replaces current process image via the platform process-
    // replace primitive (no shell involved — direct execve of our own
    // binary with the original argv); never returns on success.
    let err = std::process::Command::new(&exe)
        .args(args.iter().skip(1))
        .env(MALLOC_CONF_ENV, &conf_val)
        .env(SENTINEL, "1")
        .exec();
    Err(anyhow::anyhow!(
        "re-spawn for --memory-arenas-cap/--memory-thp failed: {}",
        err
    ))
}

#[cfg(not(all(feature = "jemalloc", unix)))]
pub fn maybe_respawn_with_memory_overrides() -> anyhow::Result<()> {
    Ok(())
}

/// Pure core of [`warn_if_conf_only_overrides`]: given the post-clap config
/// values and the raw-argv scan, decides which of the two overrides were
/// sourced from the conf file ONLY (present in the merged config but absent
/// from raw argv — the merged clap token stream includes conf-file tokens,
/// raw process argv does not). Those values never reached jemalloc.
///
/// Returns `(narenas_conf_only, thp_conf_only)`.
#[cfg(any(feature = "jemalloc", test))]
fn conf_only_overrides(
    cfg_narenas: u32,
    cfg_thp: bool,
    scanned: &ScannedOverrides,
) -> (bool, bool) {
    // narenas: a non-default merged value that raw argv did not produce must
    // have come from the conf file. (CLI always wins clap's last-wins merge,
    // so cfg == scanned whenever the CLI flag was present and parseable.)
    let narenas_conf_only = cfg_narenas != 8 && scanned.narenas != Some(cfg_narenas);
    let thp_conf_only = cfg_thp && !scanned.thp;
    (narenas_conf_only, thp_conf_only)
}

/// Warn loudly when `memory-arenas-cap` / `memory-thp` were set via the conf
/// file only: jemalloc read its config (and any re-spawn already happened)
/// long before the conf file was parsed, so such values have NO allocator
/// effect. Call after config parse + tracing init. No-op when the values
/// came from the CLI (the re-spawn honored them) or match the defaults.
#[cfg(all(feature = "jemalloc", unix))]
pub fn warn_if_conf_only_overrides(cfg_narenas: u32, cfg_thp: bool) {
    let args: Vec<std::ffi::OsString> = std::env::args_os().collect();
    let (narenas_conf_only, thp_conf_only) =
        conf_only_overrides(cfg_narenas, cfg_thp, &scan_argv(&args));
    if narenas_conf_only {
        tracing::warn!(
            "memory-arenas-cap {} comes from the conf file and has NO effect: \
             jemalloc reads its config at process start, before the conf file \
             is parsed. Pass --memory-arenas-cap {} on the command line instead.",
            cfg_narenas,
            cfg_narenas
        );
    }
    if thp_conf_only {
        tracing::warn!(
            "memory-thp comes from the conf file and has NO effect: jemalloc \
             reads its config at process start, before the conf file is \
             parsed. Pass --memory-thp on the command line instead."
        );
    }
}

#[cfg(not(all(feature = "jemalloc", unix)))]
pub fn warn_if_conf_only_overrides(_cfg_narenas: u32, _cfg_thp: bool) {
    // Non-jemalloc builds already warn that both flags are no-ops
    // (see main.rs); nothing conf-specific to add.
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;

    fn argv(parts: &[&str]) -> Vec<OsString> {
        std::iter::once(OsString::from("moon"))
            .chain(parts.iter().map(OsString::from))
            .collect()
    }

    // ── argv scan ──────────────────────────────────────────────────────────

    #[test]
    fn scan_finds_both_flags_in_either_order() {
        let s = scan_argv(&argv(&["--memory-arenas-cap", "4", "--memory-thp"]));
        assert_eq!(
            s,
            ScannedOverrides {
                narenas: Some(4),
                thp: true
            }
        );
        let s = scan_argv(&argv(&["--memory-thp", "--memory-arenas-cap=16"]));
        assert_eq!(
            s,
            ScannedOverrides {
                narenas: Some(16),
                thp: true
            }
        );
    }

    #[test]
    fn scan_absent_flags_and_malformed_values() {
        assert_eq!(
            scan_argv(&argv(&["--port", "6399"])),
            ScannedOverrides {
                narenas: None,
                thp: false
            }
        );
        // Malformed value -> None; clap rejects this command line later.
        assert_eq!(
            scan_argv(&argv(&["--memory-arenas-cap=garbage"])).narenas,
            None
        );
        // Flag at end of argv with no value -> None, no panic.
        assert_eq!(scan_argv(&argv(&["--memory-arenas-cap"])).narenas, None);
    }

    #[test]
    fn scan_rejects_out_of_range_narenas() {
        // clap enforces 1..=256; the pre-clap scan must too, or narenas:0
        // reaches jemalloc which ABORTS under abort_conf:true before clap
        // can print its friendly range error (PR #387 review).
        assert_eq!(
            scan_argv(&argv(&["--memory-arenas-cap", "0"])).narenas,
            None
        );
        assert_eq!(scan_argv(&argv(&["--memory-arenas-cap=257"])).narenas, None);
        assert_eq!(
            scan_argv(&argv(&["--memory-arenas-cap", "256"])).narenas,
            Some(256)
        );
        assert_eq!(
            scan_argv(&argv(&["--memory-arenas-cap=1"])).narenas,
            Some(1)
        );
    }

    // ── conf-string builder ───────────────────────────────────────────────

    #[test]
    fn malloc_conf_default_has_no_thp_opt() {
        let conf = build_malloc_conf(8, false);
        assert_eq!(
            conf,
            "narenas:8,background_thread:true,metadata_thp:auto,dirty_decay_ms:1000,\
             muzzy_decay_ms:5000,abort_conf:true"
        );
        assert!(!conf.contains("thp:always"));
    }

    #[test]
    fn malloc_conf_thp_only_appends_thp_always() {
        let conf = build_malloc_conf(8, true);
        assert!(conf.starts_with("narenas:8,"));
        assert!(conf.contains(",thp:always"));
        // metadata_thp:auto must remain -- thp:always is additive, not a
        // replacement (they cover different allocation classes).
        assert!(conf.contains("metadata_thp:auto"));
    }

    #[test]
    fn malloc_conf_composes_arenas_cap_and_thp() {
        // --memory-arenas-cap 4 --memory-thp together must land in ONE
        // conf string carrying both narenas:4 and thp:always.
        let conf = build_malloc_conf(4, true);
        assert!(conf.starts_with("narenas:4,"));
        assert!(conf.contains(",thp:always"));
    }

    #[test]
    fn malloc_conf_thp_present_iff_flag_set() {
        assert!(!build_malloc_conf(16, false).contains("thp:always"));
        assert!(build_malloc_conf(16, true).contains("thp:always"));
    }

    // ── override resolution ───────────────────────────────────────────────

    #[test]
    fn resolve_override_none_when_matches_baked_in_default() {
        // narenas:8 + no thp is exactly the static default -> no re-spawn.
        assert!(resolve_malloc_conf_override(8, false, false).is_none());
    }

    #[test]
    fn resolve_override_some_for_arenas_cap_alone() {
        let conf = resolve_malloc_conf_override(4, false, false)
            .expect("narenas override must trigger a re-spawn");
        assert!(conf.starts_with("narenas:4,"));
        assert!(!conf.contains("thp:always"));
    }

    #[test]
    fn resolve_override_some_for_thp_alone() {
        // narenas stays at the default 8, but thp:always still needs a
        // re-spawn since it's not in the baked-in static conf.
        let conf = resolve_malloc_conf_override(8, true, false)
            .expect("thp override must trigger a re-spawn even with default narenas");
        assert!(conf.starts_with("narenas:8,"));
        assert!(conf.contains("thp:always"));
    }

    #[test]
    fn resolve_override_composes_both_flags_in_one_conf() {
        let conf = resolve_malloc_conf_override(4, true, false)
            .expect("both overrides must compose into one re-spawn");
        assert!(conf.starts_with("narenas:4,"));
        assert!(conf.contains("thp:always"));
    }

    #[test]
    fn resolve_override_operator_env_wins_over_arenas_cap() {
        // Operator already exported _RJEM_MALLOC_CONF -- flag is a no-op,
        // regardless of what was requested.
        assert!(resolve_malloc_conf_override(4, false, true).is_none());
    }

    #[test]
    fn resolve_override_operator_env_wins_over_thp() {
        assert!(resolve_malloc_conf_override(8, true, true).is_none());
    }

    #[test]
    fn resolve_override_operator_env_wins_over_both_composed() {
        assert!(resolve_malloc_conf_override(4, true, true).is_none());
    }

    // ── O4: thp:always is Linux-only (jemalloc aborts under abort_conf:true
    //    if fed an unsupported opt on e.g. macOS -- verified experimentally
    //    2026-07-18, must never reach the conf string off Linux) ───────────

    #[test]
    fn thp_supported_only_on_linux() {
        assert!(thp_supported(true));
        assert!(!thp_supported(false));
    }

    #[test]
    fn non_linux_thp_request_downgrades_to_no_override() {
        // The exact call-site composition in maybe_respawn_with_memory_overrides:
        // effective_thp = thp_supported(is_linux) && thp_requested.
        let thp_requested = true;
        let is_linux = false;
        let effective_thp = thp_supported(is_linux) && thp_requested;
        assert!(!effective_thp);
        // With narenas left at the default and thp downgraded to false,
        // no override -- and critically, thp:always never reaches the
        // conf string that would otherwise abort a non-Linux jemalloc init.
        assert!(resolve_malloc_conf_override(8, effective_thp, false).is_none());
    }

    #[test]
    fn non_linux_thp_request_still_allows_arenas_cap() {
        // --memory-arenas-cap 4 --memory-thp on macOS: arenas override must
        // still land; only thp:always is stripped.
        let thp_requested = true;
        let is_linux = false;
        let effective_thp = thp_supported(is_linux) && thp_requested;
        let conf = resolve_malloc_conf_override(4, effective_thp, false)
            .expect("arenas-cap override must still apply");
        assert!(conf.starts_with("narenas:4,"));
        assert!(!conf.contains("thp:always"));
    }

    #[test]
    fn linux_thp_request_reaches_conf_string() {
        let thp_requested = true;
        let is_linux = true;
        let effective_thp = thp_supported(is_linux) && thp_requested;
        assert!(effective_thp);
        let conf = resolve_malloc_conf_override(8, effective_thp, false)
            .expect("thp override must trigger a re-spawn on Linux");
        assert!(conf.contains("thp:always"));
    }

    // ── conf-file-only detection (silent no-op -> loud warning) ───────────

    #[test]
    fn conf_only_flags_detected_when_absent_from_argv() {
        // moon.conf set memory-arenas-cap 4 + memory-thp; raw argv has
        // neither -> both are conf-only (and thus allocator no-ops).
        let scanned = scan_argv(&argv(&["--port", "6399"]));
        assert_eq!(conf_only_overrides(4, true, &scanned), (true, true));
    }

    #[test]
    fn cli_flags_are_not_conf_only() {
        let scanned = scan_argv(&argv(&["--memory-arenas-cap", "4", "--memory-thp"]));
        assert_eq!(conf_only_overrides(4, true, &scanned), (false, false));
    }

    #[test]
    fn default_values_never_warn() {
        // Nothing requested anywhere -> nothing to warn about.
        let scanned = scan_argv(&argv(&[]));
        assert_eq!(conf_only_overrides(8, false, &scanned), (false, false));
    }

    #[test]
    fn cli_value_winning_over_conf_value_does_not_warn() {
        // conf says 6, CLI says 4 -> clap last-wins gives 4; raw argv also
        // says 4 -> not conf-only. The conf value losing is expected
        // precedence, not a silent allocator no-op.
        let scanned = scan_argv(&argv(&["--memory-arenas-cap", "4"]));
        assert_eq!(conf_only_overrides(4, false, &scanned), (false, false));
    }
}
