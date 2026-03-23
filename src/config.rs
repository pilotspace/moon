use clap::Parser;

/// Server configuration parsed from command-line arguments.
#[derive(Parser, Debug, Clone)]
#[command(name = "rust-redis", about = "A Redis-compatible server")]
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
        let config = ServerConfig::parse_from(["rust-redis", "--port", "6380"]);
        assert_eq!(config.port, 6380);
    }

    #[test]
    fn test_custom_bind_and_databases() {
        let config =
            ServerConfig::parse_from(["rust-redis", "--bind", "0.0.0.0", "--databases", "4"]);
        assert_eq!(config.bind, "0.0.0.0");
        assert_eq!(config.databases, 4);
    }

    #[test]
    fn test_requirepass() {
        let config =
            ServerConfig::parse_from(["rust-redis", "--requirepass", "mysecret"]);
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
            "rust-redis",
            "--dir", "/data",
            "--dbfilename", "my.rdb",
            "--appendonly", "yes",
            "--appendfsync", "always",
            "--save", "3600 1 300 100",
            "--appendfilename", "my.aof",
        ]);
        assert_eq!(config.dir, "/data");
        assert_eq!(config.dbfilename, "my.rdb");
        assert_eq!(config.appendonly, "yes");
        assert_eq!(config.appendfsync, "always");
        assert_eq!(config.save, Some("3600 1 300 100".to_string()));
        assert_eq!(config.appendfilename, "my.aof");
    }
}
