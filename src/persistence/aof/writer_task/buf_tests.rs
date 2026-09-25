//! moon#1226: the tokio AOF writer's `BufWriter` size.
//!
//! It was a whole group-commit batch (`AOF_GROUP_COMMIT_MAX_BYTES`, 8 MiB)
//! per writer, one writer per shard, never shrunk. `tokio::fs::File` hands at
//! most 2 MiB to one blocking-pool hop whatever it is given, so the extra
//! 6 MiB per shard bought nothing. These tests pin both halves: the capacity,
//! and that the hop count for a large batch did not go up.

use std::pin::Pin;
use std::task::{Context, Poll};

use super::*;

/// A model of `tokio::fs::File`'s write side: every `poll_write` is one
/// blocking-pool hop that takes at most `AOF_TOKIO_BUF_CAPACITY` bytes
/// (tokio's `DEFAULT_MAX_BUF_SIZE`). Records the bytes and the hops.
#[derive(Default)]
struct HopCountingFile {
    bytes: Vec<u8>,
    hops: usize,
}

impl tokio::io::AsyncWrite for HopCountingFile {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let n = buf.len().min(AOF_TOKIO_BUF_CAPACITY);
        self.hops += 1;
        self.bytes.extend_from_slice(&buf[..n]);
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Write `records` through a `BufWriter` of `capacity`, flush, and return
/// the sink.
fn write_batch(capacity: usize, records: &[Vec<u8>]) -> HopCountingFile {
    use tokio::io::AsyncWriteExt;
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("tokio runtime")
        .block_on(async {
            let mut w = tokio::io::BufWriter::with_capacity(capacity, HopCountingFile::default());
            for r in records {
                w.write_all(r).await.expect("write");
            }
            w.flush().await.expect("flush");
            w.into_inner()
        })
}

/// The most bytes `aof_buf_writer` holds before handing any to the file:
/// its capacity, observed (tokio's `BufWriter` does not expose it).
fn held_before_first_hop() -> usize {
    use tokio::io::AsyncWriteExt;
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("tokio runtime")
        .block_on(async {
            let mut w = aof_buf_writer(HopCountingFile::default());
            let chunk = [b'x'; 1024];
            let mut held = 0;
            while w.get_ref().hops == 0 {
                held = held.max(w.buffer().len());
                w.write_all(&chunk).await.expect("write");
                assert!(held <= 64 * 1024 * 1024, "runaway buffer");
            }
            held
        })
}

#[test]
fn the_tokio_aof_buffer_is_one_file_hop_not_a_whole_batch() {
    let held = held_before_first_hop();
    assert!(
        held <= AOF_TOKIO_BUF_CAPACITY,
        "the tokio AOF BufWriter held {held} bytes per writer (one per shard, never \
         shrunk); tokio::fs::File takes at most {AOF_TOKIO_BUF_CAPACITY} per blocking \
         hop, so anything above that is resident memory for nothing"
    );
    assert!(
        held >= AOF_TOKIO_BUF_CAPACITY - 1024,
        "still one file hop per 2 MiB, not per 8 KiB (moon#1187): held {held}"
    );
}

/// A full 8 MiB group-commit batch of 1 KiB records: the same number of
/// file hops through the 2 MiB buffer as through the old 8 MiB one (the file
/// caps each hop at 2 MiB either way), and byte-identical output.
#[test]
fn a_large_batch_takes_no_more_hops_through_the_smaller_buffer() {
    let records: Vec<Vec<u8>> = (0..AOF_GROUP_COMMIT_MAX_BYTES / 1024)
        .map(|i| {
            let mut r = format!("*3\r\n$3\r\nSET\r\n$8\r\nk{i:07}\r\n$990\r\n").into_bytes();
            r.resize(1024 - 2, b'v');
            r.extend_from_slice(b"\r\n");
            r
        })
        .collect();
    let want: Vec<u8> = records.concat();
    let small = write_batch(AOF_TOKIO_BUF_CAPACITY, &records);
    let big = write_batch(AOF_GROUP_COMMIT_MAX_BYTES, &records);
    assert_eq!(small.bytes, want, "2 MiB buffer: bytes intact and in order");
    assert_eq!(big.bytes, want);
    assert_eq!(
        small.hops, big.hops,
        "hops: 2 MiB buffer {} vs 8 MiB buffer {}",
        small.hops, big.hops
    );
    assert_eq!(
        small.hops,
        AOF_GROUP_COMMIT_MAX_BYTES / AOF_TOKIO_BUF_CAPACITY
    );
}
