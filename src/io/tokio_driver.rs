//! TokioDriver wraps the existing Framed<TcpStream, RespCodec> path.
//!
//! Used on macOS and as fallback when io_uring is unavailable on Linux.
//! This preserves the existing connection handling path while providing
//! a consistent API surface that can be swapped for UringDriver.

use crate::protocol::Frame;
use crate::server::codec::RespCodec;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

/// TokioDriver wraps the existing Framed<TcpStream, RespCodec> path.
/// Used on macOS and as fallback when io_uring is unavailable.
pub struct TokioDriver {
    framed: Framed<TcpStream, RespCodec>,
}

impl TokioDriver {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            framed: Framed::new(stream, RespCodec::default()),
        }
    }

    /// Read next frame from connection. Returns None on disconnect.
    pub async fn read_frame(&mut self) -> Result<Option<Frame>, std::io::Error> {
        self.framed.next().await.transpose()
    }

    /// Write a single frame to connection.
    pub async fn write_frame(&mut self, frame: &Frame) -> Result<(), std::io::Error> {
        self.framed.send(frame.clone()).await
    }

    /// Write raw bytes directly (for static responses).
    pub async fn write_raw(&mut self, data: &[u8]) -> Result<(), std::io::Error> {
        use tokio::io::AsyncWriteExt;
        self.framed.get_mut().write_all(data).await
    }

    /// Write multiple frames (pipeline batch).
    pub async fn write_batch(&mut self, frames: &[Frame]) -> Result<(), std::io::Error> {
        for frame in frames {
            self.framed.feed(frame.clone()).await?;
        }
        self.framed.flush().await
    }

    /// Get reference to underlying stream for TCP options.
    pub fn get_ref(&self) -> &TcpStream {
        self.framed.get_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TokioDriver requires a real TcpStream, so we verify the struct compiles
    // and the API surface is correct. Integration tests cover actual I/O.

    #[test]
    fn test_tokio_driver_type_exists() {
        // Verify the type is constructible (compile-time check)
        fn _assert_send<T: Send>() {}
        // TokioDriver should be Send (TcpStream + Framed are Send)
        _assert_send::<TokioDriver>();
    }
}
