use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

use crate::command::{dispatch, DispatchResult};
use crate::protocol::Frame;
use crate::storage::Database;

use super::codec::RespCodec;

/// Handle a single client connection.
///
/// Reads frames from the TCP stream, dispatches commands, and writes responses.
/// Terminates on client disconnect, protocol error, QUIT command, or server shutdown.
pub async fn handle_connection(
    stream: TcpStream,
    db: Arc<Mutex<Vec<Database>>>,
    shutdown: CancellationToken,
    requirepass: Option<String>,
) {
    let mut framed = Framed::new(stream, RespCodec::default());
    let mut selected_db: usize = 0;

    loop {
        tokio::select! {
            result = framed.next() => {
                match result {
                    Some(Ok(frame)) => {
                        let result = {
                            let mut dbs = db.lock().unwrap();
                            let db_count = dbs.len();
                            dispatch(&mut dbs[selected_db], frame, &mut selected_db, db_count)
                        };
                        let (response, should_quit) = match result {
                            DispatchResult::Response(f) => (f, false),
                            DispatchResult::Quit(f) => (f, true),
                        };
                        if framed.send(response).await.is_err() {
                            break;
                        }
                        if should_quit {
                            break;
                        }
                    }
                    Some(Err(_)) => break,
                    None => break,
                }
            }
            _ = shutdown.cancelled() => {
                let _ = framed.send(Frame::Error(
                    Bytes::from_static(b"ERR server shutting down")
                )).await;
                break;
            }
        }
    }
}
