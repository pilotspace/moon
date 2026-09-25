//! A BOUNDED streaming reader for replaying a RESP log (moon#1160).
//!
//! The RESP replay readers used to `std::fs::read` the whole file and then
//! `BytesMut::from(&data[..])` it — a SECOND full copy — before parsing, so
//! boot held twice the log size on top of the dataset. Every replayed argument
//! was a `Bytes` slice of that one giant buffer, so any stored element kept the
//! entire replayed log alive after boot.
//!
//! This reader holds at most one chunk plus the frame being parsed: it reads
//! [`ReplayChunks::CHUNK`] bytes at a time, hands out each complete frame, and
//! grows geometrically only while a single frame is larger than what is
//! buffered (so a huge frame is re-scanned O(log size) times, and the
//! resumable parser's cursor makes even that a resume, not a restart). Once a
//! frame has been dispatched and dropped, the chunk it came from is reclaimed
//! in place on the next refill. The collection write sites store exact-size
//! copies (`storage::owned_bytes`), so nothing a replay stores pins a chunk
//! either.

use std::io::Read;

use bytes::BytesMut;

use crate::protocol::{Frame, ParseConfig, ParseError, ParseState, parse_resumable};

/// What [`ReplayChunks::next_frame`] found.
#[derive(Debug)]
pub(crate) enum ReplayNext {
    /// One complete frame.
    Frame(Frame),
    /// Clean end of the source: every byte was consumed by a frame.
    End,
    /// The source ended inside a frame: `len` unparseable bytes starting at
    /// stream offset `offset` (a crash-time torn tail).
    Truncated { offset: u64, len: usize },
    /// A protocol violation in the frame starting at stream offset `offset`.
    Corrupt { offset: u64, err: ParseError },
}

/// Streams RESP frames out of `src` through a bounded buffer.
pub(crate) struct ReplayChunks<R> {
    src: R,
    buf: BytesMut,
    state: ParseState,
    config: ParseConfig,
    /// Stream offset of `buf[0]`, including the caller's base offset.
    offset: u64,
    chunk: usize,
    eof: bool,
    /// High-water mark of `buf.len()` — what the bound is tested against.
    peak_buffered: usize,
}

impl<R: Read> ReplayChunks<R> {
    /// Bytes read from the source per refill.
    pub(crate) const CHUNK: usize = 1 << 20;

    /// A reader over `src`, whose first byte sits at `base_offset` in the
    /// file (the offsets it reports are absolute, for operator messages).
    pub(crate) fn new(src: R, base_offset: u64) -> Self {
        Self::with_chunk(src, base_offset, Self::CHUNK)
    }

    /// [`Self::new`] with an explicit refill size (tests drive frames across
    /// chunk boundaries with a tiny one).
    pub(crate) fn with_chunk(src: R, base_offset: u64, chunk: usize) -> Self {
        Self {
            src,
            buf: BytesMut::new(),
            state: ParseState::new(),
            config: ParseConfig::default(),
            offset: base_offset,
            chunk: chunk.max(1),
            eof: false,
            peak_buffered: 0,
        }
    }

    /// Stream offset of the next unconsumed byte (absolute, base included).
    pub(crate) fn offset(&self) -> u64 {
        self.offset
    }

    /// The largest number of bytes this reader has buffered at once.
    #[cfg(test)]
    pub(crate) fn peak_buffered(&self) -> usize {
        self.peak_buffered
    }

    /// The next frame, or why there is none.
    pub(crate) fn next_frame(&mut self) -> std::io::Result<ReplayNext> {
        loop {
            if !self.buf.is_empty() {
                let before = self.buf.len();
                match parse_resumable(&mut self.buf, &self.config, &mut self.state) {
                    Ok(Some(frame)) => {
                        self.offset += (before - self.buf.len()) as u64;
                        return Ok(ReplayNext::Frame(frame));
                    }
                    Ok(None) => {}
                    Err(err) => {
                        return Ok(ReplayNext::Corrupt {
                            offset: self.offset,
                            err,
                        });
                    }
                }
            }
            if self.eof {
                return Ok(if self.buf.is_empty() {
                    ReplayNext::End
                } else {
                    ReplayNext::Truncated {
                        offset: self.offset,
                        len: self.buf.len(),
                    }
                });
            }
            self.fill()?;
        }
    }

    /// The opt-in best-effort resync of the legacy reader: discard one byte
    /// at the corruption point, then everything up to the next `*`. `false`
    /// when the source ends first (nothing recoverable remains).
    pub(crate) fn skip_to_next_array(&mut self) -> std::io::Result<bool> {
        // The front of `buf` is about to be consumed by something other than
        // the parser, so the resumable cursor is void.
        self.state = ParseState::new();
        if self.buf.is_empty() && !self.eof {
            self.fill()?;
        }
        if self.buf.is_empty() {
            return Ok(false);
        }
        let _ = self.buf.split_to(1);
        self.offset += 1;
        loop {
            if let Some(pos) = memchr::memchr(b'*', &self.buf) {
                let _ = self.buf.split_to(pos);
                self.offset += pos as u64;
                return Ok(true);
            }
            self.offset += self.buf.len() as u64;
            self.buf.clear();
            if self.eof {
                return Ok(false);
            }
            self.fill()?;
        }
    }

    /// Append up to `max(chunk, buffered)` bytes from the source. Growing with
    /// what is already buffered keeps a frame larger than one chunk from being
    /// refilled (and re-scanned) once per chunk.
    fn fill(&mut self) -> std::io::Result<()> {
        let want = self.chunk.max(self.buf.len());
        let old = self.buf.len();
        self.buf.resize(old + want, 0);
        let mut got = 0usize;
        while got < want {
            match self.src.read(&mut self.buf[old + got..]) {
                Ok(0) => {
                    self.eof = true;
                    break;
                }
                Ok(n) => got += n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => {
                    self.buf.truncate(old + got);
                    return Err(e);
                }
            }
        }
        self.buf.truncate(old + got);
        self.peak_buffered = self.peak_buffered.max(self.buf.len());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn resp(parts: &[&[u8]]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", parts.len()).into_bytes();
        for p in parts {
            out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            out.extend_from_slice(p);
            out.extend_from_slice(b"\r\n");
        }
        out
    }

    fn args(f: &Frame) -> Vec<Bytes> {
        match f {
            Frame::Array(items) => items
                .iter()
                .map(|i| match i {
                    Frame::BulkString(b) => b.clone(),
                    other => panic!("{other:?}"),
                })
                .collect(),
            other => panic!("{other:?}"),
        }
    }

    /// A reader that hands out at most `step` bytes per `read`, like a pipe.
    struct Trickle<'a> {
        data: &'a [u8],
        step: usize,
    }

    impl Read for Trickle<'_> {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            let n = self.step.min(out.len()).min(self.data.len());
            out[..n].copy_from_slice(&self.data[..n]);
            self.data = &self.data[n..];
            Ok(n)
        }
    }

    #[test]
    fn frames_across_every_chunk_boundary_match_a_whole_buffer_parse() {
        let mut log = Vec::new();
        let mut expected = Vec::new();
        for i in 0..200u32 {
            let v = vec![b'a' + (i % 26) as u8; (i as usize * 7) % 300];
            let key = format!("k{i}");
            log.extend(resp(&[b"SET", key.as_bytes(), &v]));
            expected.push(vec![
                Bytes::from_static(b"SET"),
                Bytes::from(key.into_bytes()),
                Bytes::from(v),
            ]);
        }
        for chunk in [1usize, 3, 17, 64, 4096] {
            for step in [1usize, 5, 1 << 20] {
                let mut r = ReplayChunks::with_chunk(Trickle { data: &log, step }, 0, chunk);
                let mut got = Vec::new();
                loop {
                    match r.next_frame().unwrap() {
                        ReplayNext::Frame(f) => got.push(args(&f)),
                        ReplayNext::End => break,
                        other => panic!("chunk {chunk} step {step}: {other:?}"),
                    }
                }
                assert_eq!(got, expected, "chunk {chunk} step {step}");
            }
        }
    }

    #[test]
    fn a_torn_tail_is_truncated_at_its_absolute_offset() {
        let mut log = resp(&[b"SET", b"a", b"1"]);
        let whole = log.len() as u64;
        log.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$1\r\nb");
        let mut r = ReplayChunks::with_chunk(&log[..], 100, 4);
        assert!(matches!(r.next_frame().unwrap(), ReplayNext::Frame(_)));
        match r.next_frame().unwrap() {
            ReplayNext::Truncated { offset, len } => {
                assert_eq!(offset, 100 + whole);
                assert_eq!(len, log.len() - whole as usize);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn corruption_is_reported_at_its_offset_and_resync_finds_the_next_array() {
        let mut log = resp(&[b"SET", b"a", b"1"]);
        let bad_at = log.len() as u64;
        log.extend_from_slice(b"$zz\r\ngarbage");
        log.extend(resp(&[b"SET", b"b", b"2"]));
        let mut r = ReplayChunks::with_chunk(&log[..], 0, 3);
        assert!(matches!(r.next_frame().unwrap(), ReplayNext::Frame(_)));
        match r.next_frame().unwrap() {
            ReplayNext::Corrupt { offset, .. } => assert_eq!(offset, bad_at),
            other => panic!("{other:?}"),
        }
        assert!(r.skip_to_next_array().unwrap());
        match r.next_frame().unwrap() {
            ReplayNext::Frame(f) => assert_eq!(args(&f)[1], Bytes::from_static(b"b")),
            other => panic!("{other:?}"),
        }
        assert!(matches!(r.next_frame().unwrap(), ReplayNext::End));
        // Nothing recoverable: the skip reports it.
        let mut r = ReplayChunks::with_chunk(&b"$zz\r\nno arrays here"[..], 0, 4);
        assert!(matches!(
            r.next_frame().unwrap(),
            ReplayNext::Corrupt { .. }
        ));
        assert!(!r.skip_to_next_array().unwrap());
    }

    /// The bound itself: an 8 MiB log of small commands never buffers more
    /// than one chunk plus a frame — the whole-file read held all of it, and
    /// its `BytesMut::from(&data[..])` held it twice.
    #[test]
    fn buffered_bytes_stay_bounded_by_the_chunk_not_the_log() {
        let one = resp(&[b"HSET", b"h", b"field", &[b'v'; 100]]);
        let log: Vec<u8> = one
            .iter()
            .copied()
            .cycle()
            .take(one.len() * 60_000)
            .collect();
        assert!(log.len() > 8 * ReplayChunks::<&[u8]>::CHUNK);
        let mut r = ReplayChunks::new(&log[..], 0);
        let mut frames = 0usize;
        while let ReplayNext::Frame(_) = r.next_frame().unwrap() {
            frames += 1;
        }
        assert_eq!(frames, 60_000);
        assert!(
            r.peak_buffered() <= ReplayChunks::<&[u8]>::CHUNK + one.len(),
            "buffered {} B replaying a {} B log",
            r.peak_buffered(),
            log.len()
        );
    }

    /// A frame bigger than a chunk grows the buffer geometrically instead of
    /// being refilled one chunk at a time.
    #[test]
    fn a_frame_larger_than_the_chunk_is_read_whole() {
        let big = vec![b'x'; 10_000];
        let log = resp(&[b"SET", b"big", &big]);
        let mut r = ReplayChunks::with_chunk(&log[..], 0, 64);
        match r.next_frame().unwrap() {
            ReplayNext::Frame(f) => assert_eq!(args(&f)[2].len(), 10_000),
            other => panic!("{other:?}"),
        }
        assert!(matches!(r.next_frame().unwrap(), ReplayNext::End));
        assert!(r.peak_buffered() < 4 * log.len());
    }
}
