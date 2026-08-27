//! `FT.CREATE` returning `+OK` must mean the definition is durable on EVERY
//! shard (moon#743).
//!
//! ## The bug
//!
//! `VectorStore::save_index_meta_sidecar` is documented as a **no-op when
//! `persist_dir` is not set**, and `persist_dir` used to be assigned only
//! inside `recover_indexes_task`, which is *spawned* rather than run inline
//! (moon#476, so the shard starts serving immediately). Between the listener
//! accepting and that task's first poll, a shard would run `create_index`,
//! silently persist nothing, and still ack `+OK`.
//!
//! The `-LOADING` gate cannot cover this: `shard::loading::is_loading()` is a
//! thread-local read on the *connection's* shard, while `FT.CREATE` fans out to
//! every shard via `broadcast_vector_command`. The connection's own shard being
//! ready says nothing about shard 3.
//!
//! On the next restart the shards that lost the race have no sidecar, so
//! `recover_indexes_task` finds no index metadata, skips the HASH rescan
//! entirely, and their documents never re-enter the index — permanently, and
//! (before this fix) with no log line saying so. The user-visible symptom is
//! `FT.SEARCH <index> "*"` returning a fraction of the documents forever.
//!
//! ## Why this test spawns repeatedly instead of asserting once
//!
//! The defect is a startup race, so a single spawn is not a reliable detector.
//! Measured on the pre-fix binary (macOS, 10 cores, no competing load), the
//! per-spawn probability that at least one shard loses the race was:
//!
//! ```text
//!   --shards 4  ->  3/12 spawns incomplete
//!   --shards 8  ->  5/10
//!   --shards 16 ->  7/10
//! ```
//!
//! The outcome is bimodal — a spawn tends to have either every shard ready or a
//! large fraction not — which says the race is won or lost per *startup*, not
//! per shard. So the amplifier is repetition: at `--shards 8` and `SPAWNS`
//! rounds the pre-fix build escapes detection with probability ~0.5^SPAWNS
//! (~1.6% at 6). Post-fix the assertion is deterministic: `persist_dir` is
//! assigned synchronously before the recovery task is spawned, so there is no
//! window in which a shard can serve `FT.CREATE` without a persist dir.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

/// More shards = a wider window for at least one to still be initialising.
const SHARDS: usize = 8;
/// Rounds. See the module docs for the measured per-round detection rate.
const SPAWNS: usize = 6;

fn spawn_on(port: u16, dir: &std::path::Path) -> Child {
    Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            &SHARDS.to_string(),
            // The index definition's durability is the whole subject, so the
            // server must be running with persistence configured.
            "--appendonly",
            "yes",
            "--appendfsync",
            "always",
            "--disk-free-min-pct",
            "0",
            "--dir",
        ])
        .arg(dir)
        .stdout(std::process::Stdio::null())
        .stderr(common::server_stderr(dir))
        .spawn()
        .expect("spawn moon (run `cargo build` first)")
}

/// Connect and send one command the *instant* the port answers `PING`.
///
/// Deliberately no settling delay: the whole point is to reach `FT.CREATE`
/// while the shards may still be initialising.
fn create_index_asap(port: u16) -> String {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut sock = loop {
        assert!(Instant::now() < deadline, "server never answered PING");
        if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
            s.set_read_timeout(Some(Duration::from_secs(5))).ok();
            if s.write_all(b"PING\r\n").is_ok() {
                let mut buf = [0u8; 64];
                if let Ok(n) = s.read(&mut buf)
                    && buf[..n].starts_with(b"+PONG")
                {
                    break s;
                }
            }
        }
    };

    sock.write_all(
        b"FT.CREATE vidx ON HASH PREFIX 1 v: SCHEMA e VECTOR HNSW 6 \
          TYPE FLOAT32 DIM 4 DISTANCE_METRIC L2\r\n",
    )
    .expect("send FT.CREATE");
    let mut buf = [0u8; 256];
    let n = sock.read(&mut buf).expect("read FT.CREATE reply");
    String::from_utf8_lossy(&buf[..n]).trim().to_string()
}

/// Shard dirs that actually carry a persisted vector-index definition.
fn shards_with_sidecar(dir: &std::path::Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut found: Vec<String> = entries
        .flatten()
        .filter(|e| e.path().join("vector-indexes.meta").is_file())
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|n| n.starts_with("shard-"))
        .collect();
    found.sort();
    found
}

#[test]
fn ft_create_ack_means_every_shard_persisted_the_definition() {
    let mut incomplete: Vec<String> = Vec::new();

    for round in 0..SPAWNS {
        let dir = common::unique_test_dir("moon-743-ack-durable");
        std::fs::create_dir_all(&dir).expect("create test dir");
        let port = common::reserve_port();
        let guard = common::ServerGuard::new(spawn_on(port, &dir));

        let reply = create_index_asap(port);
        assert!(
            reply.starts_with("+OK"),
            "round {round}: FT.CREATE did not succeed: {reply:?}"
        );

        // Checked at ACK TIME, with no settling delay: "+OK means durable" is
        // the entire contract, and sleeping first would let a build that
        // persists shortly *after* the ack pass a test written to forbid
        // exactly that.
        let found = shards_with_sidecar(&dir);
        if found.len() != SHARDS {
            // Only now, and only to classify a failure that has already been
            // decided: a shard that persists a second late is a different bug
            // from one that never persists, and the message should say which.
            std::thread::sleep(Duration::from_secs(1));
            let late = shards_with_sidecar(&dir);
            let verdict = if late.len() == SHARDS {
                "persisted late -- durable, but AFTER the ack".to_string()
            } else {
                format!("still only {}/{SHARDS} a second later", late.len())
            };
            incomplete.push(format!(
                "round {round}: {}/{SHARDS} shards persisted at ack ({found:?}); {verdict}",
                found.len()
            ));
        }

        drop(guard);
        common::wait_for_port_down(port);
        let _ = std::fs::remove_dir_all(&dir);
    }

    assert!(
        incomplete.is_empty(),
        "FT.CREATE acked +OK while some shards had not persisted the index \
         definition; those shards lose their documents from the index on the \
         next restart (moon#743):\n  {}",
        incomplete.join("\n  ")
    );
}
