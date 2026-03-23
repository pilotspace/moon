use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

use crate::command::connection as conn_cmd;
use crate::command::{dispatch, DispatchResult};
use crate::config::ServerConfig;
use crate::persistence::aof::{self, AofMessage};
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
///
/// When `aof_tx` is provided, write commands are logged to the AOF file.
/// When `change_counter` is provided, write commands increment the counter for auto-save.
pub async fn handle_connection(
    stream: TcpStream,
    db: Arc<Mutex<Vec<Database>>>,
    shutdown: CancellationToken,
    requirepass: Option<String>,
    config: Arc<ServerConfig>,
    aof_tx: Option<mpsc::Sender<AofMessage>>,
    change_counter: Option<Arc<AtomicU64>>,
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
                            if cmd.as_slice() == b"BGSAVE" {
                                let response = crate::command::persistence::bgsave_start(
                                    db.clone(),
                                    config.dir.clone(),
                                    config.dbfilename.clone(),
                                );
                                if framed.send(response).await.is_err() {
                                    break;
                                }
                                continue;
                            }
                            if cmd.as_slice() == b"BGREWRITEAOF" {
                                if let Some(ref tx) = aof_tx {
                                    let response = crate::command::persistence::bgrewriteaof_start(
                                        tx,
                                        db.clone(),
                                    );
                                    if framed.send(response).await.is_err() {
                                        break;
                                    }
                                } else {
                                    if framed.send(Frame::Error(
                                        Bytes::from_static(b"ERR AOF is not enabled"),
                                    )).await.is_err() {
                                        break;
                                    }
                                }
                                continue;
                            }
                        }

                        // Check if this is a write command BEFORE dispatch (which consumes the frame)
                        let is_write = extract_command(&frame)
                            .map(|(ref cmd, _)| aof::is_write_command(cmd))
                            .unwrap_or(false);

                        // Serialize for AOF before dispatch consumes the frame
                        let aof_bytes = if is_write && aof_tx.is_some() {
                            let mut buf = BytesMut::new();
                            crate::protocol::serialize::serialize(&frame, &mut buf);
                            Some(buf.freeze())
                        } else {
                            None
                        };

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

                        // Log to AOF after successful dispatch (not error responses)
                        if let Some(bytes) = aof_bytes {
                            if !matches!(&response, Frame::Error(_)) {
                                if let Some(ref tx) = aof_tx {
                                    let _ = tx.send(AofMessage::Append(bytes)).await;
                                }
                                // Increment change counter for auto-save
                                if let Some(ref counter) = change_counter {
                                    counter.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }

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
