use bytes::Bytes;

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

    let pattern = match &args[0] {
        Frame::BulkString(s) => s.to_ascii_lowercase(),
        Frame::SimpleString(s) => s.to_ascii_lowercase(),
        _ => {
            return Frame::Error(Bytes::from_static(b"ERR invalid argument"));
        }
    };

    // Build list of all known config parameters
    let params: Vec<(&[u8], String)> = vec![
        (b"maxmemory" as &[u8], runtime_config.maxmemory.to_string()),
        (b"maxmemory-policy", runtime_config.maxmemory_policy.clone()),
        (
            b"maxmemory-samples",
            runtime_config.maxmemory_samples.to_string(),
        ),
        (
            b"lfu-log-factor",
            runtime_config.lfu_log_factor.to_string(),
        ),
        (
            b"lfu-decay-time",
            runtime_config.lfu_decay_time.to_string(),
        ),
        (
            b"save",
            runtime_config
                .save
                .as_deref()
                .unwrap_or("")
                .to_string(),
        ),
        (b"appendonly", runtime_config.appendonly.clone()),
        (b"appendfsync", runtime_config.appendfsync.clone()),
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
        (
            b"acllog-max-len",
            runtime_config.acllog_max_len.to_string(),
        ),
    ];

    let mut result = Vec::new();
    for (name, value) in params {
        if glob_match(&pattern, name) {
            result.push(Frame::BulkString(Bytes::copy_from_slice(name)));
            result.push(Frame::BulkString(Bytes::from(value)));
        }
    }

    Frame::Array(result.into())
}

/// Handle CONFIG SET for runtime-mutable parameters.
pub fn config_set(runtime_config: &mut RuntimeConfig, args: &[Frame]) -> Frame {
    if args.len() < 2 || args.len() % 2 != 0 {
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
                Ok(v) => runtime_config.maxmemory = v,
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
                    runtime_config.maxmemory_policy = lower;
                } else {
                    return Frame::Error(Bytes::from(format!(
                        "ERR Invalid argument '{}' for CONFIG SET 'maxmemory-policy'",
                        value_str
                    )));
                }
            }
            "maxmemory-samples" => match value_str.parse::<usize>() {
                Ok(v) if v > 0 => runtime_config.maxmemory_samples = v,
                _ => {
                    return Frame::Error(Bytes::from(format!(
                        "ERR Invalid argument '{}' for CONFIG SET 'maxmemory-samples'",
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
                assert_eq!(items[0], Frame::BulkString(Bytes::from_static(b"maxmemory")));
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
}
