use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

use crate::command::connection as conn_cmd;
use crate::command::{dispatch, DispatchResult};
use crate::protocol::Frame;
use crate::storage::Database;

use super::codec::RespCodec;

/// Extract command name (uppercased) and args from a Frame::Array.
fn extract_command(frame: &Frame) -> Option<(Vec<u8>, &[Frame])> {
    match frame {
        Frame::Array(args) if !args.is_empty() => {
            let name = match &args[0] {
                Frame::BulkString(s) => s.to_ascii_uppercase(),
                Frame::SimpleString(s) => s.to_ascii_uppercase(),
                _ => return None,
            };
            Some((name, &args[1..]))
        }
        _ => None,
    }
}

/// Handle a single client connection.
///
/// Reads frames from the TCP stream, dispatches commands, and writes responses.
/// Terminates on client disconnect, protocol error, QUIT command, or server shutdown.
///
/// When `requirepass` is set, clients must authenticate via AUTH before any other
/// commands are accepted (except QUIT).
pub async fn handle_connection(
    stream: TcpStream,
    db: Arc<Mutex<Vec<Database>>>,
    shutdown: CancellationToken,
    requirepass: Option<String>,
) {
    let mut framed = Framed::new(stream, RespCodec::default());
    let mut selected_db: usize = 0;
    let mut authenticated = requirepass.is_none();

    loop {
        tokio::select! {
            result = framed.next() => {
                match result {
                    Some(Ok(frame)) => {
                        // Check auth gate before dispatching
                        if !authenticated {
                            match extract_command(&frame) {
                                Some((ref cmd, cmd_args)) if cmd.as_slice() == b"AUTH" => {
                                    let response = conn_cmd::auth(cmd_args, &requirepass);
                                    if response == Frame::SimpleString(Bytes::from_static(b"OK")) {
                                        authenticated = true;
                                    }
                                    if framed.send(response).await.is_err() {
                                        break;
                                    }
                                    continue;
                                }
                                Some((ref cmd, _)) if cmd.as_slice() == b"QUIT" => {
                                    let _ = framed.send(Frame::SimpleString(Bytes::from_static(b"OK"))).await;
                                    break;
                                }
                                _ => {
                                    if framed.send(Frame::Error(
                                        Bytes::from_static(b"NOAUTH Authentication required.")
                                    )).await.is_err() {
                                        break;
                                    }
                                    continue;
                                }
                            }
                        }

                        // Handle AUTH when already authenticated
                        if let Some((ref cmd, cmd_args)) = extract_command(&frame) {
                            if cmd.as_slice() == b"AUTH" {
                                let response = conn_cmd::auth(cmd_args, &requirepass);
                                if framed.send(response).await.is_err() {
                                    break;
                                }
                                continue;
                            }
                        }

                        // Normal dispatch
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
