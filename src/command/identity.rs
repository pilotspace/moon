//! `ROLE` and the connection-state half of `RESET`.
//!
//! Both exist to stop the server contradicting itself. Before this module,
//! `HELLO` reported `role: master` from a `Bytes::from_static` literal while
//! `INFO replication` reported `role:slave` from `ReplicationState` on the same
//! connection — two sources of truth for one fact, which is the defect class
//! this task closes. Everything here reads `ReplicationState`, the same value
//! `build_info_replication` reads.

use std::sync::atomic::Ordering;

use bytes::Bytes;
use parking_lot::RwLock;

use crate::framevec;
use crate::protocol::Frame;
use crate::replication::state::{ReplicaHandshakeState, ReplicationRole, ReplicationState};

/// `ROLE` — `[master, <offset>, [[ip, port, offset], …]]` on a master,
/// `[slave, <host>, <port>, <state>, <offset>]` on a replica.
///
/// With replication disabled entirely there is no `ReplicationState`; the
/// honest answer is still "master with no replicas", which is what a
/// standalone redis-server reports.
pub fn role(repl_state: Option<&std::sync::Arc<RwLock<ReplicationState>>>) -> Frame {
    let Some(state) = repl_state else {
        return Frame::Array(framevec![
            Frame::BulkString(Bytes::from_static(b"master")),
            Frame::Integer(0),
            Frame::Array(framevec![]),
        ]);
    };
    let s = state.read();
    match &s.role {
        ReplicationRole::Master => {
            let offset = s.master_repl_offset.load(Ordering::Relaxed) as i64;
            let mut replicas = crate::protocol::FrameVec::with_capacity(s.replicas.len());
            for r in &s.replicas {
                let ack: u64 = r
                    .ack_offsets
                    .iter()
                    .map(|a| a.load(Ordering::Relaxed))
                    .sum();
                // Redis emits ip, port and offset as bulk strings here — not as
                // an integer for the port — so a client parsing positionally
                // gets the types it expects.
                replicas.push(Frame::Array(framevec![
                    Frame::BulkString(Bytes::from(r.addr.ip().to_string())),
                    Frame::BulkString(Bytes::from(r.addr.port().to_string())),
                    Frame::BulkString(Bytes::from(ack.to_string())),
                ]));
            }
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"master")),
                Frame::Integer(offset),
                Frame::Array(replicas),
            ])
        }
        ReplicationRole::Replica { host, port, state } => {
            // The link state string is the same vocabulary INFO uses, so the
            // two cannot disagree about whether the link is up.
            let link = match state {
                ReplicaHandshakeState::Streaming => "connected",
                _ => "connect",
            };
            let offset = s.master_repl_offset.load(Ordering::Relaxed) as i64;
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"slave")),
                Frame::BulkString(Bytes::from(host.clone())),
                Frame::Integer(*port as i64),
                Frame::BulkString(Bytes::from_static(link.as_bytes())),
                Frame::Integer(offset),
            ])
        }
    }
}

/// The `role` and `mode` fields HELLO reports, read from real state rather than
/// hardcoded. Returned as `&'static str` so the HELLO builder stays allocation
/// free.
pub fn hello_role_and_mode(
    repl_state: Option<&std::sync::Arc<RwLock<ReplicationState>>>,
    cluster_enabled: bool,
) -> (&'static str, &'static str) {
    let role = match repl_state {
        Some(s) => match s.read().role {
            ReplicationRole::Master => "master",
            ReplicationRole::Replica { .. } => "replica",
        },
        None => "master",
    };
    let mode = if cluster_enabled {
        "cluster"
    } else {
        "standalone"
    };
    (role, mode)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_without_replication_is_a_standalone_master() {
        let Frame::Array(f) = role(None) else {
            panic!("ROLE must be an array");
        };
        assert_eq!(f.len(), 3);
        assert!(matches!(&f[0], Frame::BulkString(b) if b.as_ref() == b"master"));
        assert!(matches!(f[1], Frame::Integer(0)));
        assert!(matches!(&f[2], Frame::Array(r) if r.is_empty()));
    }

    #[test]
    fn hello_reports_standalone_master_by_default() {
        assert_eq!(hello_role_and_mode(None, false), ("master", "standalone"));
    }

    #[test]
    fn hello_reports_cluster_mode_when_clustered() {
        assert_eq!(hello_role_and_mode(None, true).1, "cluster");
    }
}
