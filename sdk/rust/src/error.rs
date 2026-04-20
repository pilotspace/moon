use thiserror::Error;

#[derive(Debug, Error)]
pub enum MoonError {
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("parse error: {0}")]
    Parse(String),

    #[error("index not found: {0}")]
    IndexNotFound(String),

    #[error("graph not found: {0}")]
    GraphNotFound(String),

    #[error("workspace error: {0}")]
    Workspace(String),

    #[error("transaction aborted: {0}")]
    TransactionAborted(String),

    #[error("authentication failed")]
    AuthFailed,

    #[error("command not supported: {0}")]
    Unsupported(String),
}

pub type Result<T> = std::result::Result<T, MoonError>;

impl MoonError {
    pub fn invalid_arg(msg: impl Into<String>) -> Self {
        Self::InvalidArgument(msg.into())
    }

    pub fn parse(msg: impl Into<String>) -> Self {
        Self::Parse(msg.into())
    }
}
