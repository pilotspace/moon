//! Basic Moon client usage — standard Redis commands + Moon TXN.
//!
//! Run: `cargo run --example basic`

use moondb::{MoonClient, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = MoonClient::connect("redis://127.0.0.1:6399").await?;

    // Ping
    println!("PING → {}", client.ping().await?);

    // String commands
    client.set("example:name", "Moon").await?;
    let name: String = client.get("example:name").await?;
    println!("GET example:name → {name}");

    // Expire and TTL
    client.expire("example:name", 60).await?;
    let ttl = client.ttl("example:name").await?;
    println!("TTL example:name → {ttl}s");

    // Hash commands
    client
        .hset_multiple(
            "example:user:1",
            &[
                ("name", "Alice"),
                ("email", "alice@example.com"),
                ("score", "42"),
            ],
        )
        .await?;
    let all = client.hgetall("example:user:1").await?;
    println!("HGETALL example:user:1 → {all:?}");

    // Counter with INCR
    client.del("example:counter").await?;
    for _ in 0..5 {
        client.incr("example:counter").await?;
    }
    let count: i64 = client.get("example:counter").await?;
    println!("Counter after 5 INCRs → {count}");

    // Sorted set leaderboard
    client.del("example:leaderboard").await?;
    client.zadd("example:leaderboard", 100.0, "alice").await?;
    client.zadd("example:leaderboard", 200.0, "bob").await?;
    client.zadd("example:leaderboard", 150.0, "charlie").await?;
    let top: Vec<String> = client.zrevrange("example:leaderboard", 0, 2).await?;
    println!("Leaderboard top-3 → {top:?}");

    // Moon cross-store ACID transaction
    client.txn_begin().await?;
    client.set("txn:a", "hello").await?;
    client.set("txn:b", "world").await?;
    client.txn_commit().await?;
    let a: String = client.get("txn:a").await?;
    let b: String = client.get("txn:b").await?;
    println!("TXN result → {a} {b}");

    // Server info
    let info = client.info(Some("server")).await?;
    let version_line = info.lines().find(|l| l.starts_with("moon_version:"));
    println!("Server → {}", version_line.unwrap_or("(version not found)"));

    // Cleanup
    for key in &[
        "example:name",
        "example:user:1",
        "example:counter",
        "example:leaderboard",
        "txn:a",
        "txn:b",
    ] {
        client.del(*key).await?;
    }
    println!("Done — all example keys cleaned up.");

    Ok(())
}
