use bytes::Bytes;
use smallvec::SmallVec;

use crate::command::key::glob_match;
use crate::config::{RuntimeConfig, ServerConfig};
use crate::protocol::Frame;

/// Handle CONFIG GET with glob pattern matching on parameter names.
pub fn config_get(
    runtime_config: &RuntimeConfig,
    server_config: &ServerConfig,
    args: &[Frame],
) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'config|get' command",
        ));
    }

    // Redis accepts MANY parameters, not one: `CONFIG GET maxmemory appendonly`
    // answers both. Reading only `args[0]` silently dropped the rest, which is
    // what `redis-py`'s `config_get(*params)` and every monitoring agent that
    // reads two settings in one call send.
    //
    // Measured on redis-server 8.6.1: the result is the UNION over the
    // patterns, deduplicated (`maxmemory` + `maxmemory*` reports `maxmemory`
    // once), in the server's own table order rather than the caller's argument
    // order, with unknown patterns silently skipped. Filtering the table once
    // per entry — rather than looping the patterns outermost — gives all three
    // properties for free.
    let mut patterns: SmallVec<[Vec<u8>; 4]> = SmallVec::new();
    for arg in args {
        match arg {
            Frame::BulkString(s) | Frame::SimpleString(s) => patterns.push(s.to_ascii_lowercase()),
            _ => {
                return Frame::Error(Bytes::from_static(b"ERR invalid argument"));
            }
        }
    }

    // Build list of all known config parameters
    let params: Vec<(&[u8], String)> = vec![
        (b"maxmemory" as &[u8], runtime_config.maxmemory.to_string()),
        (b"maxmemory-policy", runtime_config.maxmemory_policy.clone()),
        (
            // Canonical form, not the caller's spelling: `CONFIG SET KEA`
            // reads back `AKE`, and clients compare the readback.
            b"notify-keyspace-events",
            crate::notify::flags_to_string(crate::notify::published_flags()),
        ),
        (
            b"maxmemory-samples",
            runtime_config.maxmemory_samples.to_string(),
        ),
        (b"db-maxmemory", format_db_maxmemory(runtime_config)),
        (b"lfu-log-factor", runtime_config.lfu_log_factor.to_string()),
        (b"lfu-decay-time", runtime_config.lfu_decay_time.to_string()),
        (
            b"save",
            runtime_config.save.as_deref().unwrap_or("").to_string(),
        ),
        (b"appendonly", runtime_config.appendonly.clone()),
        (b"appendfsync", runtime_config.appendfsync.clone()),
        (
            b"auto-aof-rewrite-percentage",
            server_config.auto_aof_rewrite_percentage.to_string(),
        ),
        (
            // Redis reports this in bytes; normalize the "64mb"-style input.
            b"auto-aof-rewrite-min-size",
            crate::config::ServerConfig::parse_size(&server_config.auto_aof_rewrite_min_size)
                .unwrap_or(64 * 1024 * 1024)
                .to_string(),
        ),
        (b"databases", server_config.databases.to_string()),
        (b"bind", server_config.bind.clone()),
        (b"port", server_config.port.to_string()),
        (b"dir", server_config.dir.clone()),
        (b"dbfilename", server_config.dbfilename.clone()),
        (b"appendfilename", server_config.appendfilename.clone()),
        (
            b"protected-mode" as &[u8],
            runtime_config.protected_mode.clone(),
        ),
        (b"acllog-max-len", runtime_config.acllog_max_len.to_string()),
        (
            b"lazyfree-threshold" as &[u8],
            runtime_config.lazyfree_threshold.to_string(),
        ),
        (b"maxclients", runtime_config.maxclients.to_string()),
        (b"timeout", runtime_config.timeout.to_string()),
        (b"tcp-keepalive", runtime_config.tcp_keepalive.to_string()),
    ];

    let mut result = Vec::new();
    for (name, value) in params {
        if patterns.iter().any(|p| glob_match(p, name)) {
            result.push(Frame::BulkString(Bytes::copy_from_slice(name)));
            result.push(Frame::BulkString(Bytes::from(value)));
        }
    }

    Frame::Array(result.into())
}

/// Format `db-maxmemory` for CONFIG GET as a comma-joined `<db>:<bytes>` list
/// of only the dbs with a nonzero quota (an all-zero/empty Vec formats as
/// the empty string, matching the "feature unconfigured" fast path).
fn format_db_maxmemory(runtime_config: &RuntimeConfig) -> String {
    runtime_config
        .db_maxmemory
        .iter()
        .enumerate()
        .filter(|&(_, &limit)| limit > 0)
        .map(|(idx, &limit)| format!("{idx}:{limit}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Handle CONFIG SET for runtime-mutable parameters.
pub fn config_set(runtime_config: &mut RuntimeConfig, args: &[Frame]) -> Frame {
    if args.len() < 2 || !args.len().is_multiple_of(2) {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'config|set' command",
        ));
    }

    // Process pairs of param-name param-value
    let mut i = 0;
    while i < args.len() {
        let param_name = match &args[i] {
            Frame::BulkString(s) => String::from_utf8_lossy(s).to_ascii_lowercase(),
            Frame::SimpleString(s) => String::from_utf8_lossy(s).to_ascii_lowercase(),
            _ => return Frame::Error(Bytes::from_static(b"ERR invalid argument")),
        };

        let value_bytes = match &args[i + 1] {
            Frame::BulkString(s) => s.clone(),
            Frame::SimpleString(s) => s.clone(),
            _ => return Frame::Error(Bytes::from_static(b"ERR invalid argument")),
        };

        let value_str = String::from_utf8_lossy(&value_bytes);

        match param_name.as_str() {
            "maxmemory" => match value_str.parse::<usize>() {
                Ok(v) => {
                    runtime_config.maxmemory = v;
                    // Keep the inline write path's lock-free pre-gate in sync.
                    crate::storage::eviction::publish_maxmemory_hints(&*runtime_config);
                    // Keep the SPSC-drain / Lua-bridge "is maxmemory set?" atomic
                    // in sync (Gap C) — a missed publish here silently bypasses
                    // the eviction gate on both paths.
                    crate::storage::eviction::publish_maxmemory(v as u64);
                }
                Err(_) => {
                    return Frame::Error(Bytes::from(format!(
                        "ERR Invalid argument '{}' for CONFIG SET 'maxmemory'",
                        value_str
                    )));
                }
            },
            "maxmemory-policy" => {
                let valid = [
                    "noeviction",
                    "allkeys-lru",
                    "allkeys-lfu",
                    "allkeys-random",
                    "volatile-lru",
                    "volatile-lfu",
                    "volatile-random",
                    "volatile-ttl",
                ];
                let lower = value_str.to_ascii_lowercase();
                if valid.contains(&lower.as_str()) {
                    // Same publish contract as `maxmemory` above: INFO and the
                    // eviction gate must never name different policies.
                    crate::storage::eviction::publish_maxmemory_policy(&lower);
                    runtime_config.maxmemory_policy = lower;
                } else {
                    return Frame::Error(Bytes::from(format!(
                        "ERR Invalid argument '{}' for CONFIG SET 'maxmemory-policy'",
                        value_str
                    )));
                }
            }
            "notify-keyspace-events" => match crate::notify::parse_flags(&value_str) {
                Ok(flags) => crate::notify::publish_flags(flags),
                Err(reason) => {
                    // Redis wraps the class-set message in its generic CONFIG
                    // SET failure envelope; config-management tooling matches
                    // on the tail, so both halves are reproduced verbatim.
                    return Frame::Error(Bytes::from(format!(
                        "ERR CONFIG SET failed (possibly related to argument \
                         'notify-keyspace-events') - {reason}"
                    )));
                }
            },
            "maxmemory-samples" => match value_str.parse::<usize>() {
                Ok(v) if v > 0 => runtime_config.maxmemory_samples = v,
                _ => {
                    return Frame::Error(Bytes::from(format!(
                        "ERR Invalid argument '{}' for CONFIG SET 'maxmemory-samples'",
                        value_str
                    )));
                }
            },
            "db-maxmemory" => match crate::config::parse_one_db_maxmemory_entry(&value_str) {
                Ok((idx, bytes)) if idx < runtime_config.db_maxmemory.len() => {
                    runtime_config.db_maxmemory[idx] = bytes;
                    crate::storage::db_quota::publish_db_maxmemory_any_set(&*runtime_config);
                }
                Ok((idx, _)) => {
                    return Frame::Error(Bytes::from(format!(
                        "ERR CONFIG SET 'db-maxmemory': db index {} is out of range \
                         (0..{}, see --databases)",
                        idx,
                        runtime_config.db_maxmemory.len()
                    )));
                }
                Err(reason) => {
                    return Frame::Error(Bytes::from(format!(
                        "ERR Invalid argument '{}' for CONFIG SET 'db-maxmemory' ({reason}, \
                         expected '<db>:<bytes>')",
                        value_str
                    )));
                }
            },
            "lfu-log-factor" => match value_str.parse::<u8>() {
                Ok(v) => runtime_config.lfu_log_factor = v,
                Err(_) => {
                    return Frame::Error(Bytes::from(format!(
                        "ERR Invalid argument '{}' for CONFIG SET 'lfu-log-factor'",
                        value_str
                    )));
                }
            },
            "lfu-decay-time" => match value_str.parse::<u64>() {
                Ok(v) => runtime_config.lfu_decay_time = v,
                Err(_) => {
                    return Frame::Error(Bytes::from(format!(
                        "ERR Invalid argument '{}' for CONFIG SET 'lfu-decay-time'",
                        value_str
                    )));
                }
            },
            // These are accepted but don't take live effect (documented behavior)
            "save" => runtime_config.save = Some(value_str.to_string()),
            "appendonly" => runtime_config.appendonly = value_str.to_string(),
            "appendfsync" => runtime_config.appendfsync = value_str.to_string(),
            "protected-mode" => {
                let lower = value_str.to_ascii_lowercase();
                if lower == "yes" || lower == "no" {
                    runtime_config.protected_mode = lower;
                } else {
                    return Frame::Error(Bytes::from(format!(
                        "ERR Invalid argument '{}' for CONFIG SET 'protected-mode'",
                        value_str
                    )));
                }
            }
            "acllog-max-len" => match value_str.parse::<usize>() {
                Ok(v) => runtime_config.acllog_max_len = v,
                Err(_) => {
                    return Frame::Error(Bytes::from(format!(
                        "ERR Invalid argument '{}' for CONFIG SET 'acllog-max-len'",
                        value_str
                    )));
                }
            },
            "lazyfree-threshold" => match value_str.parse::<usize>() {
                Ok(v) => runtime_config.lazyfree_threshold = v,
                Err(_) => {
                    return Frame::Error(Bytes::from(format!(
                        "ERR Invalid argument '{}' for CONFIG SET 'lazyfree-threshold'",
                        value_str
                    )));
                }
            },
            "maxclients" => match value_str.parse::<usize>() {
                Ok(v) => runtime_config.maxclients = v,
                Err(_) => {
                    return Frame::Error(Bytes::from(format!(
                        "ERR Invalid argument '{}' for CONFIG SET 'maxclients'",
                        value_str
                    )));
                }
            },
            "timeout" => match value_str.parse::<u64>() {
                Ok(v) => runtime_config.timeout = v,
                Err(_) => {
                    return Frame::Error(Bytes::from(format!(
                        "ERR Invalid argument '{}' for CONFIG SET 'timeout'",
                        value_str
                    )));
                }
            },
            "tcp-keepalive" => match value_str.parse::<u64>() {
                Ok(v) => runtime_config.tcp_keepalive = v,
                Err(_) => {
                    return Frame::Error(Bytes::from(format!(
                        "ERR Invalid argument '{}' for CONFIG SET 'tcp-keepalive'",
                        value_str
                    )));
                }
            },
            _ => {
                return Frame::Error(Bytes::from(format!(
                    "ERR Unsupported CONFIG parameter: {}",
                    param_name
                )));
            }
        }

        i += 2;
    }

    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// CONFIG REWRITE — serialize current runtime config to a Redis-style config file.
///
/// Writes to `<dir>/moon.conf` atomically (tmpfile + rename).
pub fn config_rewrite(runtime_config: &RuntimeConfig, server_config: &ServerConfig) -> Frame {
    let mut lines = Vec::with_capacity(20);
    lines.push("# Moon configuration file — generated by CONFIG REWRITE".to_string());
    lines.push(format!("# {}", chrono_lite_now()));
    lines.push(String::new());

    // Server settings (from ServerConfig — immutable at runtime but persisted)
    lines.push(format!("bind {}", server_config.bind));
    lines.push(format!("port {}", server_config.port));
    lines.push(format!("databases {}", server_config.databases));
    if let Some(ref pass) = runtime_config.requirepass {
        lines.push(format!("requirepass {}", pass));
    }
    lines.push(format!("protected-mode {}", runtime_config.protected_mode));
    lines.push(String::new());

    // Memory settings
    lines.push(format!("maxmemory {}", runtime_config.maxmemory));
    lines.push(format!(
        "maxmemory-policy {}",
        runtime_config.maxmemory_policy
    ));
    lines.push(format!(
        "maxmemory-samples {}",
        runtime_config.maxmemory_samples
    ));
    for (idx, &limit) in runtime_config.db_maxmemory.iter().enumerate() {
        if limit > 0 {
            lines.push(format!("db-maxmemory {idx}:{limit}"));
        }
    }
    lines.push(format!("lfu-log-factor {}", runtime_config.lfu_log_factor));
    lines.push(format!("lfu-decay-time {}", runtime_config.lfu_decay_time));
    lines.push(String::new());

    // Persistence settings
    lines.push(format!("dir {}", runtime_config.dir));
    lines.push(format!("dbfilename {}", server_config.dbfilename));
    lines.push(format!("appendonly {}", runtime_config.appendonly));
    lines.push(format!("appendfsync {}", runtime_config.appendfsync));
    lines.push(format!("appendfilename {}", server_config.appendfilename));
    if let Some(ref save) = runtime_config.save {
        lines.push(format!("save {}", save));
    }
    lines.push(String::new());

    // ACL settings
    lines.push(format!("acllog-max-len {}", runtime_config.acllog_max_len));
    if let Some(ref aclfile) = runtime_config.aclfile {
        lines.push(format!("aclfile {}", aclfile));
    }
    lines.push(format!(
        "lazyfree-threshold {}",
        runtime_config.lazyfree_threshold
    ));

    let content = lines.join("\n") + "\n";

    // Atomic write via `atomic_write_durable` (task #49): temp + fsync +
    // rename + dir-fsync. The prior code wrote+renamed with no fsync at
    // all, so a kill-9 right after `rename()` returned could still revert
    // moon.conf to empty/stale on ext4/xfs (the directory-entry update was
    // never made durable).
    let dir = &runtime_config.dir;
    let conf_path = std::path::Path::new(dir).join("moon.conf");

    if let Err(e) = crate::persistence::atomic::atomic_write_durable(&conf_path, content.as_bytes())
    {
        return Frame::Error(Bytes::from(format!("ERR failed to write config: {e}")));
    }

    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// Lightweight timestamp without chrono dependency.
fn chrono_lite_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("Generated at epoch {secs}")
}

/// CONFIG RESETSTAT — reset server statistics (placeholder).
pub fn config_resetstat() -> Frame {
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn default_server_config() -> ServerConfig {
        ServerConfig::parse_from::<[&str; 0], &str>([])
    }

    fn make_args(parts: &[&[u8]]) -> Vec<Frame> {
        parts
            .iter()
            .map(|p| Frame::BulkString(Bytes::copy_from_slice(p)))
            .collect()
    }

    #[test]
    fn test_config_get_single() {
        let rt = RuntimeConfig::default();
        let sc = default_server_config();
        let args = make_args(&[b"maxmemory"]);
        let result = config_get(&rt, &sc, &args);
        match result {
            Frame::Array(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(
                    items[0],
                    Frame::BulkString(Bytes::from_static(b"maxmemory"))
                );
                assert_eq!(items[1], Frame::BulkString(Bytes::from_static(b"0")));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_config_get_glob() {
        let rt = RuntimeConfig::default();
        let sc = default_server_config();
        let args = make_args(&[b"maxmemory*"]);
        let result = config_get(&rt, &sc, &args);
        match result {
            Frame::Array(items) => {
                // Should match maxmemory, maxmemory-policy, maxmemory-samples = 6 items
                assert_eq!(items.len(), 6);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_config_get_all() {
        let rt = RuntimeConfig::default();
        let sc = default_server_config();
        let args = make_args(&[b"*"]);
        let result = config_get(&rt, &sc, &args);
        match result {
            Frame::Array(items) => {
                // Should have pairs for all parameters
                assert!(items.len() >= 20); // at least 10 params * 2
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_config_set_maxmemory() {
        let mut rt = RuntimeConfig::default();
        let args = make_args(&[b"maxmemory", b"1048576"]);
        let result = config_set(&mut rt, &args);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert_eq!(rt.maxmemory, 1048576);
    }

    #[test]
    fn test_config_set_policy() {
        let mut rt = RuntimeConfig::default();
        let args = make_args(&[b"maxmemory-policy", b"allkeys-lru"]);
        let result = config_set(&mut rt, &args);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert_eq!(rt.maxmemory_policy, "allkeys-lru");
    }

    #[test]
    fn test_config_set_invalid_policy() {
        let mut rt = RuntimeConfig::default();
        let args = make_args(&[b"maxmemory-policy", b"invalid"]);
        let result = config_set(&mut rt, &args);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_config_set_unknown_param() {
        let mut rt = RuntimeConfig::default();
        let args = make_args(&[b"unknownparam", b"value"]);
        let result = config_set(&mut rt, &args);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_config_set_multiple_params() {
        let mut rt = RuntimeConfig::default();
        let args = make_args(&[b"maxmemory", b"2048", b"maxmemory-policy", b"allkeys-lfu"]);
        let result = config_set(&mut rt, &args);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert_eq!(rt.maxmemory, 2048);
        assert_eq!(rt.maxmemory_policy, "allkeys-lfu");
    }

    #[test]
    fn test_config_rewrite() {
        let tmp = std::env::temp_dir().join(format!("moon-test-{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();

        let rt = RuntimeConfig {
            maxmemory: 1_073_741_824, // 1GB
            maxmemory_policy: "allkeys-lru".to_string(),
            dir: tmp.to_string_lossy().to_string(),
            ..Default::default()
        };

        let sc = default_server_config();
        let result = config_rewrite(&rt, &sc);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));

        // Verify file was created
        let conf_path = tmp.join("moon.conf");
        assert!(conf_path.exists());
        let content = std::fs::read_to_string(&conf_path).unwrap();
        assert!(content.contains("maxmemory 1073741824"));
        assert!(content.contains("maxmemory-policy allkeys-lru"));
        assert!(content.contains("port 6379"));

        // Cleanup
        let _ = std::fs::remove_dir_all(tmp);
    }

    // --- db-maxmemory CONFIG SET/GET (WS5b) ---

    fn rt_with_dbs(n: usize) -> RuntimeConfig {
        RuntimeConfig {
            db_maxmemory: vec![0u64; n],
            ..Default::default()
        }
    }

    #[test]
    fn test_config_set_db_maxmemory() {
        let mut rt = rt_with_dbs(16);
        let args = make_args(&[b"db-maxmemory", b"3:1048576"]);
        let result = config_set(&mut rt, &args);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert_eq!(rt.db_maxmemory[3], 1048576);
        // Sibling dbs untouched.
        assert_eq!(rt.db_maxmemory[0], 0);
        assert_eq!(rt.db_maxmemory[2], 0);
    }

    #[test]
    fn test_config_set_db_maxmemory_zero_clears_quota() {
        let mut rt = rt_with_dbs(4);
        rt.db_maxmemory[1] = 500;
        let args = make_args(&[b"db-maxmemory", b"1:0"]);
        let result = config_set(&mut rt, &args);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert_eq!(rt.db_maxmemory[1], 0);
    }

    #[test]
    fn test_config_set_db_maxmemory_out_of_range_errors() {
        let mut rt = rt_with_dbs(4);
        let args = make_args(&[b"db-maxmemory", b"99:1024"]);
        let result = config_set(&mut rt, &args);
        match result {
            Frame::Error(msg) => {
                assert!(String::from_utf8_lossy(&msg).contains("out of range"));
            }
            other => panic!("expected Frame::Error, got {other:?}"),
        }
        // Must not have mutated the Vec (no resize, no partial write).
        assert_eq!(rt.db_maxmemory, vec![0u64; 4]);
    }

    #[test]
    fn test_config_set_db_maxmemory_malformed_errors() {
        let mut rt = rt_with_dbs(4);
        for bad in [b"garbage" as &[u8], b"1", b"a:1024", b"1:notbytes"] {
            let args = make_args(&[b"db-maxmemory", bad]);
            let result = config_set(&mut rt, &args);
            assert!(
                matches!(result, Frame::Error(_)),
                "expected error for malformed entry {:?}",
                std::str::from_utf8(bad)
            );
        }
    }

    #[test]
    fn test_config_get_db_maxmemory_lists_only_nonzero() {
        let mut rt = rt_with_dbs(4);
        rt.db_maxmemory[0] = 1024;
        rt.db_maxmemory[2] = 2048;
        let sc = default_server_config();
        let args = make_args(&[b"db-maxmemory"]);
        let result = config_get(&rt, &sc, &args);
        if let Frame::Array(arr) = result {
            assert_eq!(arr.len(), 2, "name + value pair");
            assert_eq!(
                arr[0],
                Frame::BulkString(Bytes::from_static(b"db-maxmemory"))
            );
            let value = match &arr[1] {
                Frame::BulkString(b) => String::from_utf8_lossy(b).to_string(),
                other => panic!("expected BulkString, got {other:?}"),
            };
            assert!(value.contains("0:1024"));
            assert!(value.contains("2:2048"));
            assert!(!value.contains("1:"), "db 1 has no quota, must not appear");
        } else {
            panic!("expected Array");
        }
    }

    #[test]
    fn test_config_get_db_maxmemory_empty_when_unconfigured() {
        let rt = rt_with_dbs(16);
        let sc = default_server_config();
        let args = make_args(&[b"db-maxmemory"]);
        let result = config_get(&rt, &sc, &args);
        if let Frame::Array(arr) = result {
            assert_eq!(arr[1], Frame::BulkString(Bytes::from_static(b"")));
        } else {
            panic!("expected Array");
        }
    }
}
