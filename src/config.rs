use clap::Parser;

/// Server configuration parsed from command-line arguments.
#[derive(Parser, Debug, Clone)]
#[command(name = "moon", about = "A Redis-compatible server")]
pub struct ServerConfig {
    /// Bind address
    #[arg(long, default_value = "127.0.0.1")]
    pub bind: String,

    /// Port to listen on
    #[arg(long, short, default_value_t = 6379)]
    pub port: u16,

    /// Number of databases
    #[arg(long, default_value_t = 16)]
    pub databases: usize,

    /// Require clients to authenticate with this password
    #[arg(long)]
    pub requirepass: Option<String>,

    /// Enable append-only file persistence (yes/no)
    #[arg(long, default_value = "no")]
    pub appendonly: String,

    /// AOF fsync policy (always/everysec/no)
    #[arg(long, default_value = "everysec")]
    pub appendfsync: String,

    /// RDB auto-save rules (e.g., "3600 1 300 100")
    #[arg(long)]
    pub save: Option<String>,

    /// Directory for persistence files
    #[arg(long, default_value = ".")]
    pub dir: String,

    /// RDB snapshot filename
    #[arg(long, default_value = "dump.rdb")]
    pub dbfilename: String,

    /// AOF filename
    #[arg(long, default_value = "appendonly.aof")]
    pub appendfilename: String,

    /// Maximum memory in bytes (0 = unlimited)
    #[arg(long, default_value_t = 0)]
    pub maxmemory: usize,

    /// Eviction policy when maxmemory is reached
    #[arg(long, default_value = "noeviction")]
    pub maxmemory_policy: String,

    /// Number of random keys to sample for eviction
    #[arg(long, default_value_t = 5)]
    pub maxmemory_samples: usize,

    /// Number of shards (0 = auto-detect from CPU count)
    #[arg(long, default_value_t = 0)]
    pub shards: usize,

    /// Path to ACL file (Redis-compatible format)
    #[arg(long)]
    pub aclfile: Option<String>,

    /// Enable cluster mode
    #[arg(long, default_value_t = false)]
    pub cluster_enabled: bool,

    /// Cluster node timeout in milliseconds (PFAIL detection threshold)
    #[arg(long, default_value_t = 15000)]
    pub cluster_node_timeout: u64,

    /// Enable protected mode (reject non-loopback connections when no password set)
    #[arg(long, default_value = "yes")]
    pub protected_mode: String,

    /// Maximum number of entries in the ACL log
    #[arg(long, default_value_t = 128)]
    pub acllog_max_len: usize,

    /// TLS port (0 = TLS disabled)
    #[arg(long, default_value_t = 0)]
    pub tls_port: u16,

    /// Path to TLS certificate file (PEM format)
    #[arg(long)]
    pub tls_cert_file: Option<String>,

    /// Path to TLS private key file (PEM format)
    #[arg(long)]
    pub tls_key_file: Option<String>,

    /// Path to CA certificate for client authentication (mTLS)
    #[arg(long)]
    pub tls_ca_cert_file: Option<String>,

    /// TLS 1.3 cipher suites (comma-separated, e.g., "TLS_AES_256_GCM_SHA384,TLS_CHACHA20_POLY1305_SHA256")
    #[arg(long)]
    pub tls_ciphersuites: Option<String>,
}

impl ServerConfig {
    /// Create a RuntimeConfig from this server config, copying mutable parameters.
    pub fn to_runtime_config(&self) -> RuntimeConfig {
        RuntimeConfig {
            maxmemory: self.maxmemory,
            maxmemory_policy: self.maxmemory_policy.clone(),
            maxmemory_samples: self.maxmemory_samples,
            lfu_log_factor: 10,
            lfu_decay_time: 1,
            save: self.save.clone(),
            appendonly: self.appendonly.clone(),
            appendfsync: self.appendfsync.clone(),
            aclfile: self.aclfile.clone(),
            dir: self.dir.clone(),
            requirepass: self.requirepass.clone(),
            protected_mode: self.protected_mode.clone(),
            acllog_max_len: self.acllog_max_len,
        }
    }
}

/// Runtime-mutable configuration parameters.
///
/// These can be changed via CONFIG SET without server restart.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Maximum memory in bytes (0 = unlimited).
    pub maxmemory: usize,
    /// Eviction policy name (e.g., "noeviction", "allkeys-lru").
    pub maxmemory_policy: String,
    /// Number of random keys to sample for eviction.
    pub maxmemory_samples: usize,
    /// LFU logarithmic factor for probabilistic counter increment.
    pub lfu_log_factor: u8,
    /// LFU decay time in minutes.
    pub lfu_decay_time: u64,
    /// Save rules (copied from ServerConfig, mutable via CONFIG SET but no live effect).
    pub save: Option<String>,
    /// Appendonly setting (mutable via CONFIG SET but no live effect).
    pub appendonly: String,
    /// Appendfsync setting (mutable via CONFIG SET but no live effect).
    pub appendfsync: String,
    /// ACL file path (mutable via CONFIG SET).
    pub aclfile: Option<String>,
    /// Data directory for persistence files (snapshot, WAL).
    pub dir: String,
    /// Require clients to authenticate with this password (mutable via CONFIG SET).
    pub requirepass: Option<String>,
    /// Protected mode setting (mutable via CONFIG SET).
    pub protected_mode: String,
    /// Maximum number of entries in the ACL log (mutable via CONFIG SET).
    pub acllog_max_len: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        RuntimeConfig {
            maxmemory: 0,
            maxmemory_policy: "noeviction".to_string(),
            maxmemory_samples: 5,
            lfu_log_factor: 10,
            lfu_decay_time: 1,
            save: None,
            appendonly: "no".to_string(),
            appendfsync: "everysec".to_string(),
            aclfile: None,
            dir: ".".to_string(),
            requirepass: None,
            protected_mode: "yes".to_string(),
            acllog_max_len: 128,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let config = ServerConfig::parse_from::<[&str; 0], &str>([]);
        assert_eq!(config.bind, "127.0.0.1");
        assert_eq!(config.port, 6379);
        assert_eq!(config.databases, 16);
    }

    #[test]
    fn test_custom_port() {
        let config = ServerConfig::parse_from(["moon", "--port", "6380"]);
        assert_eq!(config.port, 6380);
    }

    #[test]
    fn test_custom_bind_and_databases() {
        let config = ServerConfig::parse_from(["moon", "--bind", "0.0.0.0", "--databases", "4"]);
        assert_eq!(config.bind, "0.0.0.0");
        assert_eq!(config.databases, 4);
    }

    #[test]
    fn test_requirepass() {
        let config = ServerConfig::parse_from(["moon", "--requirepass", "mysecret"]);
        assert_eq!(config.requirepass, Some("mysecret".to_string()));
    }

    #[test]
    fn test_requirepass_default_none() {
        let config = ServerConfig::parse_from::<[&str; 0], &str>([]);
        assert_eq!(config.requirepass, None);
    }

    #[test]
    fn test_persistence_defaults() {
        let config = ServerConfig::parse_from::<[&str; 0], &str>([]);
        assert_eq!(config.appendonly, "no");
        assert_eq!(config.appendfsync, "everysec");
        assert_eq!(config.save, None);
        assert_eq!(config.dir, ".");
        assert_eq!(config.dbfilename, "dump.rdb");
        assert_eq!(config.appendfilename, "appendonly.aof");
    }

    #[test]
    fn test_persistence_custom_values() {
        let config = ServerConfig::parse_from([
            "moon",
            "--dir",
            "/data",
            "--dbfilename",
            "my.rdb",
            "--appendonly",
            "yes",
            "--appendfsync",
            "always",
            "--save",
            "3600 1 300 100",
            "--appendfilename",
            "my.aof",
        ]);
        assert_eq!(config.dir, "/data");
        assert_eq!(config.dbfilename, "my.rdb");
        assert_eq!(config.appendonly, "yes");
        assert_eq!(config.appendfsync, "always");
        assert_eq!(config.save, Some("3600 1 300 100".to_string()));
        assert_eq!(config.appendfilename, "my.aof");
    }

    #[test]
    fn test_maxmemory_defaults() {
        let config = ServerConfig::parse_from::<[&str; 0], &str>([]);
        assert_eq!(config.maxmemory, 0);
        assert_eq!(config.maxmemory_policy, "noeviction");
        assert_eq!(config.maxmemory_samples, 5);
    }

    #[test]
    fn test_maxmemory_custom() {
        let config = ServerConfig::parse_from([
            "moon",
            "--maxmemory",
            "1048576",
            "--maxmemory-policy",
            "allkeys-lru",
            "--maxmemory-samples",
            "10",
        ]);
        assert_eq!(config.maxmemory, 1048576);
        assert_eq!(config.maxmemory_policy, "allkeys-lru");
        assert_eq!(config.maxmemory_samples, 10);
    }

    #[test]
    fn test_to_runtime_config() {
        let config = ServerConfig::parse_from([
            "moon",
            "--maxmemory",
            "1024",
            "--maxmemory-policy",
            "allkeys-lfu",
        ]);
        let rt = config.to_runtime_config();
        assert_eq!(rt.maxmemory, 1024);
        assert_eq!(rt.maxmemory_policy, "allkeys-lfu");
        assert_eq!(rt.maxmemory_samples, 5);
        assert_eq!(rt.lfu_log_factor, 10);
        assert_eq!(rt.lfu_decay_time, 1);
    }

    #[test]
    fn test_runtime_config_default() {
        let rt = RuntimeConfig::default();
        assert_eq!(rt.maxmemory, 0);
        assert_eq!(rt.maxmemory_policy, "noeviction");
        assert_eq!(rt.maxmemory_samples, 5);
    }

    #[test]
    fn test_shards_default() {
        let config = ServerConfig::parse_from::<[&str; 0], &str>([]);
        assert_eq!(config.shards, 0); // auto-detect
    }

    #[test]
    fn test_shards_custom() {
        let config = ServerConfig::parse_from(["moon", "--shards", "4"]);
        assert_eq!(config.shards, 4);
    }

    #[test]
    fn test_aclfile_default_none() {
        let config = ServerConfig::parse_from::<[&str; 0], &str>([]);
        assert_eq!(config.aclfile, None);
    }

    #[test]
    fn test_aclfile_custom() {
        let config = ServerConfig::parse_from(["moon", "--aclfile", "/tmp/test.acl"]);
        assert_eq!(config.aclfile, Some("/tmp/test.acl".to_string()));
    }

    #[test]
    fn test_to_runtime_config_aclfile() {
        let config = ServerConfig::parse_from(["moon", "--aclfile", "/data/users.acl"]);
        let rt = config.to_runtime_config();
        assert_eq!(rt.aclfile, Some("/data/users.acl".to_string()));
    }
}
