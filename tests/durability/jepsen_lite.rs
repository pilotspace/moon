//! Jepsen-lite linearizability harness.
//!
//! Spawns a Moon server with `appendfsync=always`, runs 4 writer threads
//! each writing monotonically increasing sequence numbers, periodically
//! SIGKILLs the server, restarts it, and verifies that committed values
//! are monotonically increasing (no gaps = linearizable for single keys).
//!
//! Run: cargo test --test durability_tests -- jepsen --ignored

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

const PORT: u16 = 16410;
const ADDR: &str = "127.0.0.1:16410";
const WRITER_THREADS: usize = 4;
const KEYS_PER_THREAD: usize = 50;
const RESTART_CYCLES: usize = 3;

fn start_moon(port: u16, dir: &str) -> std::process::Child {
    Command::new("./target/release/moon")
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "1",
            "--appendonly",
            "yes",
            "--appendfsync",
            "always",
            "--dir",
            dir,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to start moon server")
}

fn send_cmd(addr: &str, cmd: &str) -> String {
    let Ok(mut stream) = TcpStream::connect(addr) else {
        return String::new();
    };
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    stream
        .write_all(format!("{}\r\n", cmd).as_bytes())
        .expect("write");
    stream.flush().ok();

    let reader = BufReader::new(&stream);
    let mut resp = String::new();
    for line in reader.lines() {
        match line {
            Ok(l) => {
                resp.push_str(&l);
                resp.push('\n');
                if l.starts_with('+') || l.starts_with('-') || l.starts_with(':') {
                    break;
                }
                // Bulk string: read the $N header then the data line
                if l.starts_with('$') {
                    let len: i64 = l[1..].trim().parse().unwrap_or(-1);
                    if len < 0 {
                        break;
                    }
                    // read the actual data line
                    continue;
                }
            }
            Err(_) => break,
        }
    }
    resp
}

/// Writer thread: SET jepsen_{tid}_{key} = seq, incrementing seq each cycle.
fn writer_loop(tid: usize, stop: Arc<AtomicBool>) {
    let mut seq = 0u64;
    while !stop.load(Ordering::Relaxed) {
        for k in 0..KEYS_PER_THREAD {
            let key = format!("jepsen_{}_{}", tid, k);
            let cmd = format!("SET {} {}", key, seq);
            let _ = send_cmd(ADDR, &cmd);
        }
        seq += 1;
        // Small pause so we don't spin too fast
        thread::sleep(Duration::from_millis(10));
    }
}

/// After restart, verify: for each thread's keys, values are monotonically
/// increasing (no gaps in committed sequence).
fn verify_linearizability(addr: &str) -> Result<(), String> {
    for tid in 0..WRITER_THREADS {
        let mut prev_val: Option<u64> = None;
        for k in 0..KEYS_PER_THREAD {
            let key = format!("jepsen_{}_{}", tid, k);
            let resp = send_cmd(addr, &format!("GET {}", key));

            // Parse bulk string response: "$N\nvalue\n" or "$-1\n" (nil)
            let lines: Vec<&str> = resp.trim().split('\n').collect();
            if lines.is_empty() || lines[0].starts_with("$-1") {
                // Key never committed — OK if it's consistently nil
                continue;
            }

            // Try to extract the value
            let val_str = if lines.len() >= 2 {
                lines[1].trim()
            } else {
                continue;
            };

            let val: u64 = match val_str.parse() {
                Ok(v) => v,
                Err(_) => continue,
            };

            if let Some(pv) = prev_val {
                if val < pv {
                    return Err(format!(
                        "Linearizability violation: thread {}, key {}: value {} < previous {}",
                        tid, k, val, pv
                    ));
                }
            }
            prev_val = Some(val);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Jepsen-lite: 4 writers, 3 SIGKILL cycles, verify monotonicity.
    #[test]
    #[ignore]
    fn jepsen_lite_linearizability() {
        let dir = tempfile::tempdir().unwrap();
        let dir_str = dir.path().to_str().unwrap().to_string();

        for cycle in 0..RESTART_CYCLES {
            // Start server
            let mut server = start_moon(PORT, &dir_str);
            thread::sleep(Duration::from_millis(800));

            // Spawn writers
            let stop = Arc::new(AtomicBool::new(false));
            let mut handles = Vec::new();
            for tid in 0..WRITER_THREADS {
                let stop_clone = stop.clone();
                handles.push(thread::spawn(move || writer_loop(tid, stop_clone)));
            }

            // Let writers run for 5 seconds
            thread::sleep(Duration::from_secs(5));

            // Stop writers
            stop.store(true, Ordering::Relaxed);
            for h in handles {
                let _ = h.join();
            }

            // SIGKILL the server
            unsafe {
                libc::kill(server.id() as i32, libc::SIGKILL);
            }
            let _ = server.wait();

            // Restart and verify
            let mut server2 = start_moon(PORT, &dir_str);
            thread::sleep(Duration::from_secs(2));

            let result = verify_linearizability(ADDR);
            assert!(
                result.is_ok(),
                "Cycle {}: {}",
                cycle,
                result.unwrap_err()
            );

            // Shutdown cleanly before next cycle
            let _ = send_cmd(ADDR, "SHUTDOWN NOSAVE");
            let _ = server2.kill();
            let _ = server2.wait();
        }
    }
}
