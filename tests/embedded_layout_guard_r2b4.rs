//! R2b round 4 F7: the embedded server applies the binary's AOF layout
//! refusals (`aof::layout_guard`) before recovery. It replays only the
//! single-file `appendonly.aof`, and used to replay it into EVERY shard at
//! `--shards N` (moon#1321) and to write `--appendfilename` while recovering
//! `appendonly.aof`.

// `server::embedded` exists only with the tokio runtime.
#![cfg(feature = "runtime-tokio")]

use clap::Parser;

use moon::config::ServerConfig;
use moon::runtime::cancel::CancellationToken;

fn start(dir: &std::path::Path, extra: &[&str]) -> anyhow::Result<()> {
    let mut args = vec![
        "moon".to_string(),
        "--port".into(),
        "0".into(),
        "--dir".into(),
        dir.display().to_string(),
        "--appendonly".into(),
        "yes".into(),
        "--disk-free-min-pct".into(),
        "0".into(),
    ];
    args.extend(extra.iter().map(|s| s.to_string()));
    let config = ServerConfig::parse_from(args);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    // A refusal returns before anything is spawned or bound.
    rt.block_on(moon::server::embedded::run_embedded(
        config,
        CancellationToken::new(),
    ))
}

#[test]
fn embedded_refuses_a_flat_aof_with_data_at_four_shards() {
    let dir = tempfile::tempdir().unwrap();
    let flat = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";
    std::fs::write(dir.path().join("appendonly.aof"), flat).unwrap();
    let err = start(dir.path(), &["--shards", "4"]).expect_err("refused");
    let msg = format!("{err:#}");
    assert!(msg.contains("moon#1321"), "{msg}");
    assert_eq!(
        std::fs::read(dir.path().join("appendonly.aof")).unwrap(),
        flat,
        "untouched"
    );
}

#[test]
fn embedded_refuses_an_appendfilename_it_does_not_recover() {
    let dir = tempfile::tempdir().unwrap();
    let err = start(
        dir.path(),
        &["--shards", "1", "--appendfilename", "foo.aof"],
    )
    .expect_err("refused");
    let msg = format!("{err:#}");
    assert!(msg.contains("--appendfilename foo.aof"), "{msg}");
    assert!(!dir.path().join("foo.aof").exists());
}
