//! moon-bench: Purpose-built benchmark tool for Moon/Redis servers.
//! Uses raw std TCP sockets — no async runtime overhead.

use std::io::{BufWriter, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use clap::Parser;

#[derive(Parser)]
#[command(name = "moon-bench", about = "Moon/Redis Benchmark Tool")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = 6379)]
    port: u16,
    #[arg(long, default_value_t = 50)]
    clients: usize,
    #[arg(long, default_value_t = 100_000)]
    requests: usize,
    #[arg(long, default_value_t = 1)]
    pipeline: usize,
    #[arg(long, default_value = "get")]
    command: String,
    #[arg(long, default_value_t = 3)]
    data_size: usize,
    #[arg(long, default_value_t = false)]
    csv: bool,
    #[arg(long, default_value_t = 1000)]
    warmup: usize,
}

/// Write a RESP bulk string ($len\r\ndata\r\n) to buf.
fn bulk(buf: &mut Vec<u8>, s: &str) {
    write!(buf, "${}\r\n{}\r\n", s.len(), s).unwrap();
}

fn build_command(cmd: &str, key: &str, val: &str, buf: &mut Vec<u8>) {
    match cmd {
        "ping" => buf.extend_from_slice(b"*1\r\n$4\r\nPING\r\n"),
        "get" => { buf.extend_from_slice(b"*2\r\n$3\r\nGET\r\n"); bulk(buf, key); }
        "set" => { buf.extend_from_slice(b"*3\r\n$3\r\nSET\r\n"); bulk(buf, key); bulk(buf, val); }
        "incr" => { buf.extend_from_slice(b"*2\r\n$4\r\nINCR\r\n"); bulk(buf, key); }
        "lpush" => { buf.extend_from_slice(b"*3\r\n$5\r\nLPUSH\r\n"); bulk(buf, key); bulk(buf, val); }
        "rpush" => { buf.extend_from_slice(b"*3\r\n$5\r\nRPUSH\r\n"); bulk(buf, key); bulk(buf, val); }
        "lpop" => { buf.extend_from_slice(b"*2\r\n$4\r\nLPOP\r\n"); bulk(buf, key); }
        "rpop" => { buf.extend_from_slice(b"*2\r\n$4\r\nRPOP\r\n"); bulk(buf, key); }
        "sadd" => { buf.extend_from_slice(b"*3\r\n$4\r\nSADD\r\n"); bulk(buf, key); bulk(buf, val); }
        "spop" => { buf.extend_from_slice(b"*2\r\n$4\r\nSPOP\r\n"); bulk(buf, key); }
        "hset" => {
            buf.extend_from_slice(b"*4\r\n$4\r\nHSET\r\n");
            bulk(buf, key); bulk(buf, "f"); bulk(buf, val);
        }
        "zadd" => {
            buf.extend_from_slice(b"*4\r\n$4\r\nZADD\r\n");
            bulk(buf, key); bulk(buf, "1"); bulk(buf, val);
        }
        _ => panic!("unsupported command: {cmd}"),
    }
}

fn count_resp_replies(buf: &[u8]) -> (usize, usize) {
    let (mut count, mut pos) = (0, 0);
    while let Some(end) = try_parse_reply(buf, pos) {
        count += 1;
        pos = end;
    }
    (count, pos)
}

fn try_parse_reply(buf: &[u8], s: usize) -> Option<usize> {
    if s >= buf.len() { return None; }
    match buf[s] {
        b'+' | b'-' | b':' => find_crlf(buf, s + 1).map(|p| p + 2),
        b'$' => {
            let crlf = find_crlf(buf, s + 1)?;
            let len: i64 = std::str::from_utf8(&buf[s + 1..crlf]).ok()?.parse().ok()?;
            if len < 0 { Some(crlf + 2) }
            else {
                let end = crlf + 2 + len as usize + 2;
                (end <= buf.len()).then_some(end)
            }
        }
        b'*' => {
            let crlf = find_crlf(buf, s + 1)?;
            let len: i64 = std::str::from_utf8(&buf[s + 1..crlf]).ok()?.parse().ok()?;
            if len < 0 { return Some(crlf + 2); }
            let mut pos = crlf + 2;
            for _ in 0..len { pos = try_parse_reply(buf, pos)?; }
            Some(pos)
        }
        _ => None,
    }
}

fn find_crlf(buf: &[u8], from: usize) -> Option<usize> {
    (from < buf.len()).then(|| memchr::memmem::find(&buf[from..], b"\r\n").map(|i| from + i))?
}

fn drain_replies(stream: &mut TcpStream, read_buf: &mut [u8], expected: usize) {
    let (mut got, mut leftover) = (0, Vec::new());
    while got < expected {
        let n = stream.read(read_buf).expect("read failed");
        assert!(n > 0, "server closed connection unexpectedly");
        leftover.extend_from_slice(&read_buf[..n]);
        let (replies, consumed) = count_resp_replies(&leftover);
        got += replies;
        leftover.drain(..consumed);
    }
}

fn pre_populate(addr: &str, total_keys: usize, data_size: usize) {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.set_nodelay(true).unwrap();
    let value = "x".repeat(data_size);
    let (batch, mut cmd_buf, mut read_buf) = (500, Vec::with_capacity(500 * 64), vec![0u8; 64 * 1024]);
    let mut sent = 0;
    while sent < total_keys {
        cmd_buf.clear();
        let count = (sent + batch).min(total_keys) - sent;
        for i in sent..sent + count {
            build_command("set", &format!("key:pre:{i}"), &value, &mut cmd_buf);
        }
        stream.write_all(&cmd_buf).unwrap();
        drain_replies(&mut stream, &mut read_buf, count);
        sent += count;
    }
}

#[allow(clippy::too_many_arguments)]
fn run_client(
    addr: &str, cmd: &str, pipeline: usize, data_size: usize,
    counter: &AtomicUsize, total: usize, tid: usize, barrier: &Barrier, warmup: usize,
) -> Vec<Duration> {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.set_nodelay(true).unwrap();
    // No read timeout — blocking socket waits for server response.
    // Timeout-based error handling causes busy-wait on single-core VMs.
    let value = "x".repeat(data_size);
    let mut cmd_buf = Vec::with_capacity(pipeline * 128);
    let mut read_buf = vec![0u8; 256 * 1024];
    let mut latencies = Vec::with_capacity(total / 4);
    let mut seq = 0u64;

    // Warmup (before barrier, not measured)
    let mut warmed = 0;
    while warmed < warmup {
        cmd_buf.clear();
        let n = pipeline.min(warmup - warmed);
        for _ in 0..n {
            let key = if cmd == "get" { format!("key:pre:{}", seq % total as u64) }
                      else { format!("key:{tid}:{seq}") };
            build_command(cmd, &key, &value, &mut cmd_buf);
            seq += 1;
        }
        stream.write_all(&cmd_buf).unwrap();
        drain_replies(&mut stream, &mut read_buf, n);
        warmed += n;
    }
    barrier.wait();

    // Measured phase
    loop {
        let claimed = counter.fetch_add(pipeline, Ordering::Relaxed);
        if claimed >= total { break; }
        let batch = pipeline.min(total - claimed);
        cmd_buf.clear();
        for i in 0..batch {
            let key = if cmd == "get" { format!("key:pre:{}", (claimed + i) % total) }
                      else { format!("key:{tid}:{seq}") };
            build_command(cmd, &key, &value, &mut cmd_buf);
            seq += 1;
        }
        let t = Instant::now();
        { let mut w = BufWriter::new(&stream); w.write_all(&cmd_buf).unwrap(); w.flush().unwrap(); }
        drain_replies(&mut stream, &mut read_buf, batch);
        latencies.push(t.elapsed());
    }
    let _ = stream.shutdown(Shutdown::Write);
    latencies
}

fn main() {
    let args = Args::parse();
    let addr = format!("{}:{}", args.host, args.port);
    let cmd = args.command.to_lowercase();

    if !args.csv {
        eprintln!("moon-bench: Moon/Redis Benchmark Tool");
        eprintln!("Connecting to {addr}...");
    }
    if cmd == "get" {
        if !args.csv { eprintln!("Pre-populating {} keys...", args.requests); }
        pre_populate(&addr, args.requests, args.data_size);
    }

    let counter = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(args.clients));
    if !args.csv {
        eprintln!("{}: {} clients, {} requests, pipeline {}",
            cmd.to_uppercase(), args.clients, args.requests, args.pipeline);
    }

    let start = Instant::now();
    let handles: Vec<_> = (0..args.clients).map(|tid| {
        let (addr, cmd, counter, barrier) = (addr.clone(), cmd.clone(), Arc::clone(&counter), Arc::clone(&barrier));
        let (pl, ds, total, wu) = (args.pipeline, args.data_size, args.requests, args.warmup / args.clients);
        std::thread::spawn(move || run_client(&addr, &cmd, pl, ds, &counter, total, tid, &barrier, wu))
    }).collect();

    let mut all_lat: Vec<Duration> = Vec::new();
    for h in handles { all_lat.extend(h.join().unwrap()); }
    let wall = start.elapsed();
    all_lat.sort_unstable();

    let total_done = counter.load(Ordering::Relaxed).min(args.requests);
    let rps = total_done as f64 / wall.as_secs_f64();
    let pl = args.pipeline as f64;
    let p50 = pct(&all_lat, 50.0).as_secs_f64() * 1000.0 / pl;
    let p99 = pct(&all_lat, 99.0).as_secs_f64() * 1000.0 / pl;
    let max = all_lat.last().copied().unwrap_or(Duration::ZERO).as_secs_f64() * 1000.0 / pl;

    if args.csv {
        println!("\"test\",\"rps\",\"p50_ms\",\"p99_ms\",\"max_ms\"");
        println!("\"{}\",\"{rps:.2}\",\"{p50:.3}\",\"{p99:.3}\",\"{max:.3}\"", cmd.to_uppercase());
    } else {
        println!("\nThroughput: {:>12} requests/sec", fmt_num(rps as u64));
        println!("Latency:\n  p50: {p50:.3}ms\n  p99: {p99:.3}ms\n  max: {max:.3}ms");
    }
}

fn pct(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() { return Duration::ZERO; }
    sorted[((p / 100.0) * (sorted.len() - 1) as f64).round() as usize]
}

fn fmt_num(n: u64) -> String {
    let s = n.to_string();
    let mut r = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 { r.push(','); }
        r.push(c);
    }
    r.chars().rev().collect()
}
