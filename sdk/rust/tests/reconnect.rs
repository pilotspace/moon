//! Reconnect regression test: a long-lived `MoonClient` MUST survive a
//! backend restart.
//!
//! ## Why this exists
//!
//! `redis::aio::MultiplexedConnection` (the type `MoonClient` wrapped before
//! this fix) opens ONE socket at connect time and NEVER rebuilds it. When the
//! server drops the connection — a restart, an idle-timeout reap, a transient
//! network blip — every subsequent command fails with `broken pipe` FOREVER,
//! and the only recovery is to reconstruct the client (i.e. restart the whole
//! process). This bit Lunaris in production: after a live Moon daemon flip,
//! the `lunaris-mcp` server's pooled connection went permanently dead and
//! `memory.recall` returned `broken pipe` on every call until the MCP process
//! was restarted.
//!
//! `redis::aio::ConnectionManager` is the reconnecting variant: on a broken
//! socket it transparently re-establishes the connection (with backoff) so
//! later commands recover on their own.
//!
//! ## The discriminator
//!
//! Spawn a real `redis-server` (Moon speaks the same RESP wire protocol) on an
//! ephemeral port, drive one PING through a long-lived client, KILL the server,
//! restart it on the SAME port, then assert a PING on THE SAME client succeeds
//! within a bounded window.
//!
//! * RED  (MultiplexedConnection): the post-restart PING never succeeds — the
//!        client is wedged on the dead socket. The test times out and fails.
//! * GREEN (ConnectionManager): the client reconnects and PING returns PONG.
//!
//! The test spawns its own server, so it is self-contained. If `redis-server`
//! is not on `PATH` (some minimal dev boxes) it SKIPS with a message rather
//! than failing — it still discriminates everywhere a server is available
//! (developer machines + moon CI, which ships `redis-server`).

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use moondb::MoonClient;

/// Grab an ephemeral TCP port by binding to :0 and immediately releasing it.
/// Small TOCTOU window, acceptable for a spawn-a-throwaway-server test.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = l.local_addr().expect("local_addr").port();
    drop(l);
    port
}

/// Spawn a throwaway `redis-server` on `port` with `dir` as its working dir,
/// persistence disabled. Returns `None` if `redis-server` is not on `PATH`.
fn spawn_server(port: u16, dir: &std::path::Path) -> Option<Child> {
    Command::new("redis-server")
        .args([
            "--port",
            &port.to_string(),
            "--dir",
            dir.to_str().expect("utf8 dir"),
            "--save",
            "",
            "--appendonly",
            "no",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()
}

/// Poll a FRESH client until the server at `url` answers PING, or `budget`
/// elapses. Used only for readiness gating — the actual subject-under-test is a
/// separate long-lived client.
async fn wait_ready(url: &str, budget: Duration) -> bool {
    let deadline = Instant::now() + budget;
    while Instant::now() < deadline {
        if let Ok(mut c) = MoonClient::connect(url).await
            && c.ping().await.is_ok()
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

#[tokio::test]
async fn client_survives_backend_restart() {
    let port = free_port();
    let url = format!("redis://127.0.0.1:{port}");
    let dir =
        std::env::temp_dir().join(format!("moondb-reconnect-{}-{}", port, std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");

    // ── Bring up the backend ────────────────────────────────────────────────
    let Some(mut child) = spawn_server(port, &dir) else {
        eprintln!("SKIP client_survives_backend_restart: `redis-server` not on PATH");
        let _ = std::fs::remove_dir_all(&dir);
        return;
    };
    assert!(
        wait_ready(&url, Duration::from_secs(10)).await,
        "backend never became ready on first boot"
    );

    // ── The subject under test: ONE long-lived client across the restart ─────
    let mut client = MoonClient::connect_with_timeout(url.as_str(), Duration::from_secs(5))
        .await
        .expect("initial connect");
    assert_eq!(
        client.ping().await.expect("first ping").to_uppercase(),
        "PONG",
        "sanity: client talks to the live backend before the restart"
    );

    // ── Drop the backend out from under the live client ──────────────────────
    let _ = child.kill();
    let _ = child.wait();

    // ── Restart on the SAME port ─────────────────────────────────────────────
    let mut child2 = match spawn_server(port, &dir) {
        Some(c) => c,
        None => panic!("restart spawn failed (redis-server vanished mid-test?)"),
    };
    assert!(
        wait_ready(&url, Duration::from_secs(10)).await,
        "backend never came back on restart"
    );

    // ── DISCRIMINATOR: the SAME long-lived client must recover ───────────────
    // MultiplexedConnection: broken-pipe forever → this loop never succeeds → fail.
    // ConnectionManager: transparently reconnects → PONG within the window.
    let mut recovered = false;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Ok(p) = client.ping().await
            && p.to_uppercase() == "PONG"
        {
            recovered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // ── Cleanup (best-effort) ────────────────────────────────────────────────
    let _ = child2.kill();
    let _ = child2.wait();
    let _ = std::fs::remove_dir_all(&dir);

    assert!(
        recovered,
        "long-lived client did NOT recover after a backend restart — the \
         connection never reconnected (MultiplexedConnection wedges on the dead \
         socket; ConnectionManager is required)"
    );
}
