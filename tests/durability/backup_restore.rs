//! Backup/restore workflow test.
//!
//! Validates: BGSAVE → copy snapshot → restore on fresh node → data parity.
//! Uses DBSIZE comparison (DEBUG DIGEST not yet implemented in Moon).

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpStream;
    use std::process::{Command, Stdio};
    use std::thread;
    use std::time::Duration;

    fn send_command(addr: &str, cmd: &str) -> String {
        let mut stream = TcpStream::connect(addr).expect("connect");
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
                }
                Err(_) => break,
            }
        }
        resp
    }

    #[test]
    #[ignore] // Requires built moon binary
    fn backup_restore_parity() {
        let dir1 = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();

        // Start primary server
        let mut primary = Command::new("./target/release/moon")
            .args([
                "--port",
                "16500",
                "--shards",
                "1",
                "--dir",
                dir1.path().to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start primary");

        thread::sleep(Duration::from_millis(500));

        // Write data
        for i in 0..100 {
            send_command(
                "127.0.0.1:16500",
                &format!("SET backup_key_{} value_{}", i, i),
            );
        }

        let before = send_command("127.0.0.1:16500", "DBSIZE");

        // Trigger BGSAVE and poll for dump.rdb existence
        send_command("127.0.0.1:16500", "BGSAVE");
        let rdb_src = dir1.path().join("dump.rdb");
        let poll_deadline = std::time::Instant::now() + Duration::from_secs(10);
        while std::time::Instant::now() < poll_deadline {
            if rdb_src.exists() {
                break;
            }
            thread::sleep(Duration::from_millis(100));
        }
        assert!(rdb_src.exists(), "dump.rdb was not created within timeout");

        // Copy RDB to restore dir
        let rdb_dst = dir2.path().join("dump.rdb");
        std::fs::copy(&rdb_src, &rdb_dst).expect("copy RDB");

        // Stop primary
        send_command("127.0.0.1:16500", "SHUTDOWN NOSAVE");
        let _ = primary.wait();

        // Start restore server from copied RDB
        let mut restore = Command::new("./target/release/moon")
            .args([
                "--port",
                "16501",
                "--shards",
                "1",
                "--dir",
                dir2.path().to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start restore");

        // Poll until the restore server is ready (accepts connections) instead of fixed sleep.
        let mut restore_ready = false;
        for _ in 0..40 {
            if TcpStream::connect("127.0.0.1:16501").is_ok() {
                // Server is accepting connections; give it a moment to finish loading.
                thread::sleep(Duration::from_millis(200));
                restore_ready = true;
                break;
            }
            thread::sleep(Duration::from_millis(100));
        }
        assert!(
            restore_ready,
            "restore server did not become ready within timeout"
        );

        let after = send_command("127.0.0.1:16501", "DBSIZE");

        // Cleanup
        send_command("127.0.0.1:16501", "SHUTDOWN NOSAVE");
        let _ = restore.wait();

        // Verify parity
        assert_eq!(
            before.trim(),
            after.trim(),
            "DBSIZE mismatch: primary={} restore={}",
            before.trim(),
            after.trim()
        );
    }
}
