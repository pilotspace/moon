//! Redis-style `moon.conf` file parser.
//!
//! # Format
//!
//! Lines are `key value` pairs where `value` is everything after the first
//! whitespace, trimmed.  `#` begins a comment (rest of line ignored).  Blank
//! lines are silently skipped.
//!
//! ```text
//! # this is a comment
//! port 6379
//! bind 127.0.0.1
//! appendonly yes       # inline comment stripped
//! maxmemory 1gb
//! ```
//!
//! Key normalisation: `_` → `-` so `append_only` and `append-only` are
//! identical.
//!
//! Surrounding double-quotes on the value are stripped (redis.conf convention),
//! e.g. `save "900 1"` → value `900 1`.  Single-`#` after non-space is only a
//! comment when preceded by whitespace (matches redis behaviour: `pass#word` is
//! a valid value, `pass #word` is value `pass`).
//!
//! # Bool flags
//!
//! Some `ServerConfig` fields are `bool` and use clap's `ArgAction::SetTrue`
//! (the flag has no value argument).  For these keys a conf-file value of
//! `yes`, `true`, or `1` emits the bare flag `--key` in the synthesised argv.
//! `no`, `false`, or `0` emits nothing (flag absent = false).  Any other value
//! is a [`ConfFileError::InvalidBoolValue`].
//!
//! **Bool-flag list** — kept in sync with `ServerConfig`'s `bool` fields.
//! When a new `bool` field is added to `ServerConfig` its clap long-name MUST
//! be appended to [`BOOL_FLAGS`].  Failure to do so means the conf file
//! silently emits `["--key", "yes"]` which clap will reject at parse time (a
//! runtime error, not silently wrong) but the error message won't be ideal.
//!
//! ```text
//! cluster-enabled yes        → ["--cluster-enabled"]
//! cluster-enabled no         → []          (flag absent)
//! cluster-enabled maybe      → ConfFileError::InvalidBoolValue
//! ```
//!
//! # Vec flags
//!
//! `console-cors-origin` uses `ArgAction::Append`.  When present in both the
//! conf file and the CLI, both sets of values accumulate (redis list semantics):
//! conf-file entries appear first in the merged argv, CLI entries follow.
//! There is no way to *remove* a conf-file value with a CLI flag; use the conf
//! file as the sole source for that key if you need full control.
//!
//! # Maintenance note
//!
//! [`BOOL_FLAGS`] is a `const` slice that must be kept in sync with the `bool`
//! fields in `ServerConfig` (see `src/config.rs`).  Search for `: bool,` in
//! that file whenever a new field is added.

use std::io;
use std::path::Path;

/// Error type for conf-file parsing failures.
#[derive(Debug, thiserror::Error)]
pub enum ConfFileError {
    /// The conf file does not exist.
    #[error("conf file not found: {path}")]
    NotFound { path: String },

    /// An I/O error other than "not found" occurred while reading.
    #[error("I/O error reading conf file: {0}")]
    Io(#[source] io::Error),

    /// A bool flag (one listed in [`BOOL_FLAGS`]) was given an unrecognised
    /// value.  Valid values: `yes`, `true`, `1`, `no`, `false`, `0`.
    #[error(
        "conf file line {line}: key '{key}' is a bool flag; \
         got '{value}' (expected yes/true/1/no/false/0)"
    )]
    InvalidBoolValue {
        line: usize,
        key: String,
        value: String,
    },

    /// A key appeared on a line by itself with no value.
    #[error("conf file line {line}: key '{key}' has no value")]
    EmptyValue { line: usize, key: String },

    /// `--config` was the last argument, with no path following it.
    #[error("missing value for --config (expected a conf file path)")]
    MissingConfigValue,
}

/// Long-form flag names (without leading `--`) for every `bool` field in
/// `ServerConfig`.  These fields use clap `ArgAction::SetTrue`, which means
/// the flag has no value argument.
///
/// **Must be kept in sync with `src/config.rs`.**
/// Search for `: bool,` inside the `ServerConfig` struct when adding fields.
pub const BOOL_FLAGS: &[&str] = &[
    "console-auth-required",
    "check-config",
    "unsafe-multishard-aof",
    "experimental-per-shard-rewrite",
    "cluster-enabled",
];

/// Parse a conf file and return a list of synthesised argv tokens.
///
/// The tokens are in the form `["--key", "value", ...]`.  For bool flags
/// that are set to a truthy value the token is `["--key"]` alone.
///
/// # Errors
///
/// Returns [`ConfFileError::NotFound`] when the file does not exist,
/// [`ConfFileError::Io`] on other I/O failures, [`ConfFileError::InvalidBoolValue`]
/// when a bool flag value is unrecognised, and [`ConfFileError::EmptyValue`]
/// when a key appears with no value.
pub fn load_conf_file(path: &Path) -> Result<Vec<String>, ConfFileError> {
    let contents = std::fs::read_to_string(path).map_err(|e| {
        if e.kind() == io::ErrorKind::NotFound {
            ConfFileError::NotFound {
                path: path.display().to_string(),
            }
        } else {
            ConfFileError::Io(e)
        }
    })?;
    parse_conf_contents(&contents)
}

/// Parse conf file contents (string slice) into synthesised argv tokens.
///
/// Exposed for unit testing without touching the filesystem.
pub fn parse_conf_contents(contents: &str) -> Result<Vec<String>, ConfFileError> {
    let mut tokens: Vec<String> = Vec::new();

    for (raw_lineno, raw_line) in contents.lines().enumerate() {
        let lineno = raw_lineno + 1; // 1-based for error messages

        // Strip inline comments: only strip `#` when preceded by whitespace
        // (so passwords containing `#` are not accidentally truncated).
        let line = strip_comment(raw_line);
        let line = line.trim();

        if line.is_empty() {
            continue;
        }

        // Split on the first whitespace token: key = first token, value = remainder.
        let (raw_key, raw_value) = match line.split_once(|c: char| c.is_whitespace()) {
            Some((k, v)) => (k, v.trim()),
            None => {
                // Line has only a key with no whitespace — missing value.
                let key = normalise_key(line);
                return Err(ConfFileError::EmptyValue { line: lineno, key });
            }
        };

        let key = normalise_key(raw_key);

        // Strip surrounding double-quotes from value (redis.conf convention).
        let value = strip_quotes(raw_value);

        // Handle bool flags.
        if BOOL_FLAGS.contains(&key.as_str()) {
            match value {
                "yes" | "true" | "1" => {
                    tokens.push(format!("--{key}"));
                }
                "no" | "false" | "0" => {
                    // Flag absent → false; emit nothing.
                }
                other => {
                    return Err(ConfFileError::InvalidBoolValue {
                        line: lineno,
                        key,
                        value: other.to_string(),
                    });
                }
            }
            continue;
        }

        // Normal key=value flag.
        tokens.push(format!("--{key}"));
        tokens.push(value.to_string());
    }

    Ok(tokens)
}

/// Merge a conf-file argv vector and a real-process argv (as `OsString`s) into
/// a single merged argv suitable for `ServerConfig::parse_from`.
///
/// The conf path is recognised two ways:
/// - `argv[1]` does not start with `-` (redis convention: positional conf path).
/// - The value following a `--config` flag anywhere in argv.
///
/// The resulting merged argv is:
/// ```text
/// [argv0] ++ conf_tokens ++ real_args_minus(conf_path, --config_pair)
/// ```
///
/// CLI args appear *after* conf tokens so clap's last-wins semantics let the
/// CLI override the conf.  `ArgAction::Append` flags (e.g. `console-cors-origin`)
/// accumulate both sets.
///
/// # Errors
///
/// Propagates [`ConfFileError`] from [`load_conf_file`].  When no conf path is
/// found in argv the function returns `Ok(None)` — the caller should use the
/// raw argv unchanged.
pub fn merge_conf_argv<I>(raw_argv: I) -> Result<Option<Vec<String>>, ConfFileError>
where
    I: IntoIterator<Item = std::ffi::OsString>,
{
    let args: Vec<std::ffi::OsString> = raw_argv.into_iter().collect();

    // We need at least argv[0] (program name).
    let argv0 = args
        .first()
        .and_then(|s| s.to_str())
        .unwrap_or("moon")
        .to_string();

    // Scan for the conf path.
    let mut conf_path: Option<std::path::PathBuf> = None;
    // Indices to remove from the remaining args (conf positional or --config pair).
    let mut skip_indices: Vec<usize> = Vec::new();

    let mut i = 1usize;
    while i < args.len() {
        let arg = args[i].to_string_lossy();

        if arg == "--config" {
            // --config FILE: next arg is the path.
            if let Some(next) = args.get(i + 1) {
                conf_path = Some(std::path::PathBuf::from(next));
                skip_indices.push(i);
                skip_indices.push(i + 1);
                i += 2;
                continue;
            }
            // `--config` as the final argument: fail with a clear message
            // instead of letting clap report a confusing "unexpected argument".
            return Err(ConfFileError::MissingConfigValue);
        } else if let Some(val) = arg.strip_prefix("--config=") {
            // --config=FILE form.
            conf_path = Some(std::path::PathBuf::from(val));
            skip_indices.push(i);
            i += 1;
            continue;
        } else if i == 1 && !arg.starts_with('-') {
            // argv[1] is positional and doesn't look like a flag → conf path.
            conf_path = Some(std::path::PathBuf::from(args[i].as_os_str()));
            skip_indices.push(i);
        }

        i += 1;
    }

    let Some(path) = conf_path else {
        return Ok(None);
    };

    let conf_tokens = load_conf_file(&path)?;

    // Build the remaining real args (everything in 1..args.len() except
    // skipped indices — they are conf path / --config pair).
    let real_remaining: Vec<String> = args
        .iter()
        .enumerate()
        .skip(1) // skip argv0 — we handle it separately
        .filter(|(idx, _)| !skip_indices.contains(idx))
        .filter_map(|(_, s)| s.to_str().map(String::from))
        .collect();

    let mut merged: Vec<String> = Vec::with_capacity(1 + conf_tokens.len() + real_remaining.len());
    merged.push(argv0);
    merged.extend(conf_tokens);
    merged.extend(real_remaining);

    Ok(Some(merged))
}

// ── private helpers ──────────────────────────────────────────────────────────

/// Normalise a key: lower-case and replace `_` with `-`.
fn normalise_key(raw: &str) -> String {
    raw.to_lowercase().replace('_', "-")
}

/// Strip a leading/trailing pair of `"` from a value.
/// Only applies when the *entire* value is surrounded by double-quotes.
fn strip_quotes(s: &str) -> &str {
    if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
        &s[1..s.len() - 1]
    } else {
        s
    }
}

/// Strip a trailing comment from a raw line.
///
/// A `#` is considered a comment start only when it is preceded by whitespace
/// (or is the first character).  This mirrors redis.conf: `requirepass p#w`
/// keeps the full value `p#w`, while `requirepass pw # comment` trims to `pw`.
fn strip_comment(line: &str) -> &str {
    let bytes = line.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'#' {
            if i == 0 || bytes[i - 1].is_ascii_whitespace() {
                return &line[..i];
            }
        }
    }
    line
}

// ── tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Red → Green: these tests drove the initial implementation ────────────

    #[test]
    fn basic_kv() {
        let tokens = parse_conf_contents("port 6379\nbind 127.0.0.1\n").unwrap();
        assert_eq!(tokens, vec!["--port", "6379", "--bind", "127.0.0.1"]);
    }

    #[test]
    fn full_line_comment_stripped() {
        let tokens = parse_conf_contents("# this is a comment\nport 7000\n").unwrap();
        assert_eq!(tokens, vec!["--port", "7000"]);
    }

    #[test]
    fn trailing_inline_comment_stripped() {
        let tokens = parse_conf_contents("port 7000 # inline comment\n").unwrap();
        assert_eq!(tokens, vec!["--port", "7000"]);
    }

    #[test]
    fn hash_inside_value_not_stripped() {
        // A `#` not preceded by whitespace is part of the value.
        let tokens = parse_conf_contents("requirepass p#ssword\n").unwrap();
        assert_eq!(tokens, vec!["--requirepass", "p#ssword"]);
    }

    #[test]
    fn blank_lines_ignored() {
        let tokens = parse_conf_contents("\n\nport 6380\n\n").unwrap();
        assert_eq!(tokens, vec!["--port", "6380"]);
    }

    #[test]
    fn underscore_normalised_to_dash() {
        let tokens = parse_conf_contents("max_memory 512mb\n").unwrap();
        assert_eq!(tokens, vec!["--max-memory", "512mb"]);
    }

    #[test]
    fn bool_yes_emits_bare_flag() {
        let tokens = parse_conf_contents("cluster-enabled yes\n").unwrap();
        assert_eq!(tokens, vec!["--cluster-enabled"]);
    }

    #[test]
    fn bool_true_emits_bare_flag() {
        let tokens = parse_conf_contents("cluster-enabled true\n").unwrap();
        assert_eq!(tokens, vec!["--cluster-enabled"]);
    }

    #[test]
    fn bool_1_emits_bare_flag() {
        let tokens = parse_conf_contents("cluster-enabled 1\n").unwrap();
        assert_eq!(tokens, vec!["--cluster-enabled"]);
    }

    #[test]
    fn bool_no_emits_nothing() {
        let tokens = parse_conf_contents("cluster-enabled no\n").unwrap();
        assert!(tokens.is_empty());
    }

    #[test]
    fn bool_false_emits_nothing() {
        let tokens = parse_conf_contents("cluster-enabled false\n").unwrap();
        assert!(tokens.is_empty());
    }

    #[test]
    fn bool_0_emits_nothing() {
        let tokens = parse_conf_contents("cluster-enabled 0\n").unwrap();
        assert!(tokens.is_empty());
    }

    #[test]
    fn bool_invalid_value_error() {
        let err = parse_conf_contents("cluster-enabled maybe\n").unwrap_err();
        match err {
            ConfFileError::InvalidBoolValue { line, key, value } => {
                assert_eq!(line, 1);
                assert_eq!(key, "cluster-enabled");
                assert_eq!(value, "maybe");
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn missing_file_error() {
        let err = load_conf_file(std::path::Path::new("/no/such/file.conf")).unwrap_err();
        match err {
            ConfFileError::NotFound { path } => {
                assert!(path.contains("no/such/file.conf"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn multi_word_value_rest_of_line() {
        // `save "900 1"` → value is `900 1` (quotes stripped).
        let tokens = parse_conf_contents("save \"900 1\"\n").unwrap();
        assert_eq!(tokens, vec!["--save", "900 1"]);
    }

    #[test]
    fn multi_word_value_unquoted() {
        // Without quotes: `save 900 1` → rest-of-line after first whitespace.
        let tokens = parse_conf_contents("save 900 1\n").unwrap();
        assert_eq!(tokens, vec!["--save", "900 1"]);
    }

    #[test]
    fn empty_value_line_error() {
        let err = parse_conf_contents("port\n").unwrap_err();
        match err {
            ConfFileError::EmptyValue { line, key } => {
                assert_eq!(line, 1);
                assert_eq!(key, "port");
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn bool_underscore_key_normalised() {
        // `cluster_enabled yes` normalises to `cluster-enabled` → bool flag.
        let tokens = parse_conf_contents("cluster_enabled yes\n").unwrap();
        assert_eq!(tokens, vec!["--cluster-enabled"]);
    }

    #[test]
    fn multiple_bool_flags() {
        let contents = "cluster-enabled yes\nconsole-auth-required no\nunsafe-multishard-aof yes\n";
        let tokens = parse_conf_contents(contents).unwrap();
        assert_eq!(tokens, vec!["--cluster-enabled", "--unsafe-multishard-aof"]);
    }

    // ── merge_conf_argv tests ─────────────────────────────────────────────────

    fn os_args(v: &[&str]) -> Vec<std::ffi::OsString> {
        v.iter().map(|s| std::ffi::OsString::from(s)).collect()
    }

    #[test]
    fn merge_no_conf_path_returns_none() {
        let result = merge_conf_argv(os_args(&["moon", "--port", "7000"])).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn merge_positional_conf_path_stripped() {
        // Write a tiny temp conf.
        let dir = std::env::temp_dir();
        let path = dir.join("moon_test_positional.conf");
        std::fs::write(&path, "port 6399\n").unwrap();

        let args = os_args(&["moon", path.to_str().unwrap()]);
        let merged = merge_conf_argv(args).unwrap().unwrap();

        // Should be: ["moon", "--port", "6399"]  (conf path removed)
        assert_eq!(merged[0], "moon");
        assert!(merged.contains(&"--port".to_string()));
        assert!(merged.contains(&"6399".to_string()));
        // conf path must NOT appear in merged (stripped)
        assert!(!merged.contains(&path.to_str().unwrap().to_string()));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn merge_config_flag_stripped() {
        let dir = std::env::temp_dir();
        let path = dir.join("moon_test_config_flag.conf");
        std::fs::write(&path, "port 6400\n").unwrap();

        let args = os_args(&["moon", "--config", path.to_str().unwrap()]);
        let merged = merge_conf_argv(args).unwrap().unwrap();

        assert!(!merged.iter().any(|s| s == "--config"));
        assert!(!merged.iter().any(|s| *s == path.to_str().unwrap()));
        assert!(merged.contains(&"--port".to_string()));
        assert!(merged.contains(&"6400".to_string()));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn merge_config_flag_without_value_is_clear_error() {
        // `--config` as the last argument must yield MissingConfigValue,
        // not fall through to clap's confusing "unexpected argument".
        let args = os_args(&["moon", "--config"]);
        let err = merge_conf_argv(args).unwrap_err();
        assert!(
            matches!(err, ConfFileError::MissingConfigValue),
            "expected MissingConfigValue, got: {err}"
        );
    }

    #[test]
    fn merge_cli_after_conf_so_cli_wins() {
        // Verify that real CLI args come AFTER conf tokens so clap last-wins
        // gives CLI priority.
        let dir = std::env::temp_dir();
        let path = dir.join("moon_test_cli_wins.conf");
        std::fs::write(&path, "port 6401\n").unwrap();

        let args = os_args(&["moon", path.to_str().unwrap(), "--port", "7401"]);
        let merged = merge_conf_argv(args).unwrap().unwrap();

        // Find the last --port in merged.
        let last_port_val = merged
            .windows(2)
            .filter(|w| w[0] == "--port")
            .last()
            .map(|w| w[1].clone());
        assert_eq!(
            last_port_val.as_deref(),
            Some("7401"),
            "CLI --port should be last (wins)"
        );

        std::fs::remove_file(&path).ok();
    }

    // ── clap-level parse tests (require args_override_self = true) ───────────

    /// Verify that `args_override_self` lets clap accept duplicate scalar args
    /// and that the last value wins — this is the mechanism that makes conf-file
    /// tokens overrideable by CLI flags.
    #[test]
    fn clap_args_override_self_last_port_wins() {
        use crate::config::ServerConfig;
        use clap::Parser;
        // Simulate merged argv: conf says port 6379, CLI says port 7401.
        // With args_override_self=true the last value (7401) should win.
        let cfg = ServerConfig::try_parse_from(["moon", "--port", "6379", "--port", "7401"])
            .expect("duplicate --port should be accepted with args_override_self=true");
        assert_eq!(cfg.port, 7401, "CLI --port should override conf port");
    }

    /// Verify that conf-only port (no CLI override) is used.
    #[test]
    fn clap_conf_port_used_when_no_cli_override() {
        use crate::config::ServerConfig;
        use clap::Parser;
        let cfg = ServerConfig::try_parse_from(["moon", "--port", "6399"])
            .expect("single --port should always be accepted");
        assert_eq!(cfg.port, 6399);
    }

    /// Verify that short-flag alias `-p` also works as last-wins when conf emits
    /// `--port`.  This catches the alias-collision edge case.
    #[test]
    fn clap_short_flag_port_alias_override() {
        use crate::config::ServerConfig;
        use clap::Parser;
        // conf emits --port 6379, user passes -p 7401.
        let cfg = ServerConfig::try_parse_from(["moon", "--port", "6379", "-p", "7401"])
            .expect("--port then -p should be accepted with args_override_self=true");
        assert_eq!(cfg.port, 7401, "short -p should override --port from conf");
    }
}
