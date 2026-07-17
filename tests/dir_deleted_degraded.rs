//! Issue #366: data-dir deleted under a running server must latch into a
//! degraded state — not spin the persistence tick in a syscall error loop.
//!
//! Observed in the wild (macOS, runtime-tokio, 4 shards, appendonly yes): an
//! orphaned server whose `--dir` was cleaned climbed to ~667% CPU while idle
//! (per-thread sys-time dominating user ~50:1) and wedged PING. The per-tick
//! WAL/checkpoint file operations retried on ENOENT with no failure latch, no
//! backoff, and no escalation.
//!
//! Contract pinned here (design-for-failure, mirrors the diskfull guard):
//!
//! 1. Within one disk-monitor poll (5s) of the dir vanishing, write commands
//!    are refused with `MOONERR dirmissing` — like `MOONERR diskfull`.
//! 2. Reads and PING keep answering; the server must not wedge.
//! 3. The per-tick error loop stops: stderr must go quiet (no 1/s WAL-scan
//!    warnings) and CPU must stay near idle baseline while latched.
//! 4. If the directory reappears, writes resume within one poll (self-heal).
//!
//! Run with:
//!   cargo test --release --test dir_deleted_degraded

#![cfg(unix)]
#![allow(clippy::unwrap_used)]

mod common;

use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::Child;
use std::time::{Duration, Instant};

/// Kill-on-drop guard so a mid-test panic can't orphan a server whose data
/// dir this test deletes — exactly the leak class that produced issue #366.
struct MoonGuard(Option<Child>);

impl Drop for MoonGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            common::sigkill(&mut child);
        }
    }
}

struct Conn(std::net::TcpStream);

impl Conn {
    fn open(port: u16) -> Self {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(s) = std::net::TcpStream::connect(("127.0.0.1", port)) {
                s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                return Self(s);
            }
            assert!(Instant::now() < deadline, "server never accepted");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Send an inline command, return the raw reply up to one read() chunk.
    fn cmd(&mut self, line: &str) -> String {
        self.0
            .write_all(format!("{line}\r\n").as_bytes())
            .expect("write");
        let mut buf = [0u8; 64 * 1024];
        let n = self.0.read(&mut buf).expect("read");
        String::from_utf8_lossy(&buf[..n]).into_owned()
    }
}

/// Total process CPU time (user + system) in milliseconds, via `ps` so the
/// same probe works on macOS and Linux. `cputime` renders as `[DD-]HH:MM:SS`
/// or `MM:SS.cc` depending on platform — parse colon fields generically.
fn process_cpu_ms(pid: u32) -> u64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "cputime=", "-p", &pid.to_string()])
        .output()
        .expect("ps");
    let s = String::from_utf8_lossy(&out.stdout);
    let s = s.trim().replace('-', ":"); // DD-HH:MM:SS → DD:HH:MM:SS
    let mut secs = 0.0f64;
    for field in s.split(':') {
        secs = secs * 60.0 + field.parse::<f64>().unwrap_or(0.0);
    }
    (secs * 1000.0) as u64
}

#[test]
fn dir_deleted_latches_write_refusal_stays_responsive_and_self_heals() {
    let data_dir: PathBuf = std::env::temp_dir().join(format!(
        "moon-dir-deleted-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&data_dir).unwrap();
    let log_path = data_dir.with_extension("stderr.log");

    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        let log = std::fs::File::create(&log_path).expect("create stderr log");
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &data_dir.to_string_lossy(),
                "--shards",
                "4",
                "--appendonly",
                "yes",
                // The dir-lost latch must work even with the disk-FULL guard
                // relaxed (the standard crash-test configuration).
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::from(log))
            .spawn()
            .expect("spawn moon")
    });
    let pid = child.id();
    let _guard = MoonGuard(Some(child));

    let mut c = Conn::open(port);
    assert!(
        c.cmd("SET before-loss v1").starts_with("+OK"),
        "healthy server must accept writes"
    );

    // Healthy-baseline CPU window. The latched-CPU check below is RELATIVE
    // to this: on a loaded CI runner the whole box thrashes and even an idle
    // 4-shard tick legitimately reads 40-80% of a core, so an absolute
    // threshold flakes (observed: 2-4s/5s on the self-hosted runner while
    // the vector benches ran alongside). Both windows see the same box load.
    let cpu_b0 = process_cpu_ms(pid);
    std::thread::sleep(Duration::from_secs(5));
    let baseline_cpu = process_cpu_ms(pid).saturating_sub(cpu_b0);

    // ── The incident trigger: data dir vanishes under the running server ──
    std::fs::remove_dir_all(&data_dir).unwrap();

    // (1) Writes must latch to MOONERR dirmissing within ~one 5s poll.
    let refusal = wait_for(Duration::from_secs(20), || {
        let r = c.cmd("SET after-loss v2");
        r.contains("MOONERR dirmissing").then_some(r)
    });
    assert!(
        refusal.is_some(),
        "writes were still accepted 20s after the data dir was deleted — \
         the dir-lost latch never engaged"
    );

    // (2) Reads and PING keep answering while latched.
    assert!(
        c.cmd("PING").starts_with("+PONG"),
        "PING must stay responsive while persistence is degraded"
    );
    assert!(
        c.cmd("GET before-loss").contains("v1"),
        "reads must keep serving in-memory data while latched"
    );

    // (3a) The per-tick error loop must stop: stderr goes quiet once latched.
    //      Pre-fix every shard warns once per second (WAL overflow scan), so
    //      5s accumulates ≥20 lines; allow a small tail for in-flight lines.
    let len_before = std::fs::metadata(&log_path).map(|m| m.len()).unwrap_or(0);
    std::thread::sleep(Duration::from_secs(5));
    let len_after = std::fs::metadata(&log_path).map(|m| m.len()).unwrap_or(0);
    assert!(
        len_after - len_before < 400,
        "stderr grew {} bytes in 5s while dir-lost latched — the persistence \
         tick is still error-looping",
        len_after - len_before
    );

    // (3b) CPU stays near the healthy baseline (the 667%-spin regression
    //      pin). Fail only when BOTH hold, so shared-runner load can't flake
    //      it while the incident state (≈6+ cores ≈ 30s CPU per 5s wall)
    //      fails both by an order of magnitude:
    //      - absolute: ≥90% of one core (4500ms/5s) — an idle 4-shard
    //        server never legitimately needs that, even descheduled/thrashed;
    //      - relative: >4× the pre-deletion baseline + 2.5s slack (Linux
    //        `ps cputime` is whole-second granular; both windows see the
    //        same box load, so uniform thrash cancels out).
    let cpu_before = process_cpu_ms(pid);
    std::thread::sleep(Duration::from_secs(5));
    let cpu_delta = process_cpu_ms(pid).saturating_sub(cpu_before);
    assert!(
        cpu_delta < 4_500 || cpu_delta <= baseline_cpu * 4 + 2_500,
        "process burned {cpu_delta}ms CPU in 5s wall while idle+latched \
         (healthy baseline was {baseline_cpu}ms/5s) — persistence tick is \
         spinning"
    );

    // (4) Self-heal: restore the directory, writes resume within one poll.
    std::fs::create_dir_all(&data_dir).unwrap();
    let resumed = wait_for(Duration::from_secs(20), || {
        c.cmd("SET after-heal v3").starts_with("+OK").then_some(())
    });
    assert!(
        resumed.is_some(),
        "writes never resumed within 20s of the data dir reappearing"
    );
    assert!(c.cmd("GET after-heal").contains("v3"));

    let _ = std::fs::remove_dir_all(&data_dir);
    let _ = std::fs::remove_file(&log_path);
}

/// Poll `probe` every 500ms until it returns Some or `timeout` elapses.
fn wait_for<T>(timeout: Duration, mut probe: impl FnMut() -> Option<T>) -> Option<T> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(v) = probe() {
            return Some(v);
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}
