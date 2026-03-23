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
}
