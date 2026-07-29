use std::{fmt::Debug, io, mem};

use monoio::{
    buf::{IoBuf, IoBufMut},
    io::{AsyncReadRent, AsyncWriteRent, AsyncWriteRentExt, CancelHandle, CancelableAsyncReadRent},
};

const BUFFER_SIZE: usize = 16 * 1024;

struct Buffer {
    read: usize,
    write: usize,
    // moon patch (c10k P4b): target capacity. `buf` is either exactly this
    // size or released (len 0, only legal while drained); storage is
    // materialized on first use and can be dropped again via
    // `release_if_empty`, so an idle stream holds no backing allocation.
    cap: usize,
    buf: Box<[u8]>,
}

impl Default for Buffer {
    fn default() -> Self {
        Self::new(BUFFER_SIZE)
    }
}

impl Buffer {
    fn new(size: usize) -> Self {
        Self {
            read: 0,
            write: 0,
            cap: size,
            // moon patch (c10k P4b): lazy — allocated on first use.
            buf: Box::default(),
        }
    }

    // moon patch (c10k P4b): materialize backing storage before an I/O or
    // copy-in touches it. Only legal transitions: released <-> allocated,
    // both while drained.
    fn ensure_allocated(&mut self) {
        if self.buf.len() != self.cap {
            debug_assert!(self.is_empty());
            self.buf = vec![0; self.cap].into_boxed_slice();
        }
    }

    // moon patch (c10k P4b): drop the backing storage while drained.
    fn release_if_empty(&mut self) -> bool {
        if self.is_empty() && !self.buf.is_empty() {
            self.read = 0;
            self.write = 0;
            self.buf = Box::default();
            true
        } else {
            false
        }
    }

    fn len(&self) -> usize {
        self.write - self.read
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn available(&self) -> usize {
        self.buf.len() - self.write
    }

    fn is_full(&self) -> bool {
        self.available() == 0
    }

    fn advance(&mut self, n: usize) {
        assert!(self.read + n <= self.write);
        self.read += n;
        if self.read == self.write {
            self.read = 0;
            self.write = 0;
        }
    }
}

unsafe impl monoio::buf::IoBuf for Buffer {
    fn read_ptr(&self) -> *const u8 {
        unsafe { self.buf.as_ptr().add(self.read) }
    }

    fn bytes_init(&self) -> usize {
        self.write - self.read
    }
}

unsafe impl monoio::buf::IoBufMut for Buffer {
    fn write_ptr(&mut self) -> *mut u8 {
        unsafe { self.buf.as_mut_ptr().add(self.write) }
    }

    fn bytes_total(&mut self) -> usize {
        self.buf.len() - self.write
    }

    unsafe fn set_init(&mut self, pos: usize) {
        self.write += pos;
    }
}

pub struct SafeRead {
    // the option is only meant for temporary take, it always should be some
    buffer: Option<Buffer>,
    status: ReadStatus,
}

impl Debug for SafeRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SafeRead")
            .field("status", &self.status)
            .finish()
    }
}

#[derive(Debug)]
enum ReadStatus {
    Eof,
    Err(io::Error),
    Ok,
}

impl Default for SafeRead {
    fn default() -> Self {
        Self {
            buffer: Some(Buffer::default()),
            status: ReadStatus::Ok,
        }
    }
}

impl SafeRead {
    /// Create a new SafeRead with given buffer size.
    pub fn new(buffer_size: usize) -> Self {
        Self {
            buffer: Some(Buffer::new(buffer_size)),
            status: ReadStatus::Ok,
        }
    }

    /// `do_io` do async read from io to inner buffer.
    /// # Handle return value
    /// _: the read result.
    pub async fn do_io<IO: AsyncReadRent>(&mut self, mut io: IO) -> io::Result<usize> {
        // if there are some data inside the buffer, just return.
        let buffer = self.buffer.as_mut().expect("buffer mut expected");
        if !buffer.is_empty() {
            return Ok(buffer.len());
        }
        // moon patch (c10k P4b): lazily (re)materialize released storage.
        buffer.ensure_allocated();

        // read from raw io
        // # Safety
        // We have already checked it is not None.
        let buffer = unsafe { self.buffer.take().unwrap_unchecked() };
        let (result, buf) = io.read(buffer).await;
        self.buffer = Some(buf);
        match result {
            Ok(0) => {
                self.status = ReadStatus::Eof;
                result
            }
            Ok(_) => {
                self.status = ReadStatus::Ok;
                result
            }
            Err(e) => {
                let rerr = e.kind().into();
                self.status = ReadStatus::Err(e);
                Err(rerr)
            }
        }
    }

    /// moon patch (c10k P4b): cancelable twin of `do_io`, used by the
    /// idle-park sweep path. Errors from the cancelable read — including
    /// ECANCELED from a fired cancel — are returned directly and NOT stashed
    /// in `status`: stashing would replay a spurious error into the next
    /// plain read after an idle-park cancel and tear down a healthy
    /// connection. EOF is genuine state and is still recorded.
    pub async fn do_io_cancelable<IO: CancelableAsyncReadRent>(
        &mut self,
        mut io: IO,
        c: CancelHandle,
    ) -> io::Result<usize> {
        let buffer = self.buffer.as_mut().expect("buffer mut expected");
        if !buffer.is_empty() {
            return Ok(buffer.len());
        }
        buffer.ensure_allocated();

        // # Safety
        // We have already checked it is not None.
        let buffer = unsafe { self.buffer.take().unwrap_unchecked() };
        let (result, buf) = io.cancelable_read(buffer, c).await;
        self.buffer = Some(buf);
        match &result {
            Ok(0) => self.status = ReadStatus::Eof,
            Ok(_) => self.status = ReadStatus::Ok,
            Err(_) => {}
        }
        result
    }

    /// moon patch (c10k P4b): drop the backing storage while drained; it
    /// reallocates lazily on the next `do_io`. Returns whether storage was
    /// released.
    pub fn release_if_empty(&mut self) -> bool {
        self.buffer
            .as_mut()
            .expect("buffer mut expected")
            .release_if_empty()
    }

    /// moon patch (c1M P1-TLS): true when the buffer holds no bytes AND no
    /// deferred status (EOF/error) is pending delivery. Task-parking on raw
    /// fd readability is only correct when nothing is waiting here — a
    /// buffered byte or a pending EOF would never make the fd readable.
    pub fn is_drained(&self) -> bool {
        self.buffer
            .as_ref()
            .expect("buffer ref expected")
            .is_empty()
            && matches!(self.status, ReadStatus::Ok)
    }
}

impl io::Read for SafeRead {
    /// `read` from buffer.
    /// # Handle return value
    /// 1. Err(WouldBlock): the buffer is empty, call do_io to fetch more.
    /// 2. _: handle it.
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // if buffer is empty, return WoundBlock.
        let buffer = self.buffer.as_mut().expect("buffer mut expected");
        if buffer.is_empty() {
            return match mem::replace(&mut self.status, ReadStatus::Ok) {
                ReadStatus::Eof => Ok(0),
                ReadStatus::Err(e) => Err(e),
                ReadStatus::Ok => Err(io::ErrorKind::WouldBlock.into()),
            };
        }

        // now buffer is not empty. copy it.
        let to_copy = buffer.len().min(buf.len());
        unsafe { std::ptr::copy_nonoverlapping(buffer.read_ptr(), buf.as_mut_ptr(), to_copy) };
        buffer.advance(to_copy);

        Ok(to_copy)
    }
}

pub struct SafeWrite {
    // the option is only meant for temporary take, it always should be some
    buffer: Option<Buffer>,
    status: WriteStatus,
}

impl Debug for SafeWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SafeWrite")
            .field("status", &self.status)
            .finish()
    }
}

#[derive(Debug)]
enum WriteStatus {
    Err(io::Error),
    Ok,
}

impl Default for SafeWrite {
    fn default() -> Self {
        Self {
            buffer: Some(Buffer::default()),
            status: WriteStatus::Ok,
        }
    }
}

impl SafeWrite {
    /// Create a new SafeWrite with given buffer size.
    pub fn new(buffer_size: usize) -> Self {
        Self {
            buffer: Some(Buffer::new(buffer_size)),
            status: WriteStatus::Ok,
        }
    }

    /// `do_io` do async write from inner buffer to io.
    /// # Handle return value
    /// _: the write_all result(note: the data may have been written even when error).
    pub async fn do_io<IO: AsyncWriteRent>(&mut self, mut io: IO) -> io::Result<usize> {
        // if the buffer is empty, just return.
        let buffer = self.buffer.as_ref().expect("buffer ref expected");
        if buffer.is_empty() {
            return Ok(0);
        }

        // buffer is not empty now. write it.
        // # Safety
        // We have already checked it is not None.
        let buffer = unsafe { self.buffer.take().unwrap_unchecked() };
        let (result, buffer) = io.write_all(buffer).await;
        self.buffer = Some(buffer);
        match result {
            Ok(written_len) => {
                unsafe { self.buffer.as_mut().unwrap_unchecked().advance(written_len) };
                Ok(written_len)
            }
            Err(e) => {
                let rerr = e.kind().into();
                self.status = WriteStatus::Err(e);
                Err(rerr)
            }
        }
    }

    /// moon patch (c10k P4b): drop the backing storage while drained; it
    /// reallocates lazily on the next copy-in. Returns whether storage was
    /// released.
    pub fn release_if_empty(&mut self) -> bool {
        self.buffer
            .as_mut()
            .expect("buffer mut expected")
            .release_if_empty()
    }

    /// moon patch (c1M P1-TLS): true when no unflushed bytes and no stashed
    /// write error are pending. See `SafeRead::is_drained`.
    pub fn is_drained(&self) -> bool {
        self.buffer
            .as_ref()
            .expect("buffer ref expected")
            .is_empty()
            && matches!(self.status, WriteStatus::Ok)
    }
}

impl io::Write for SafeWrite {
    /// `write` to buffer.
    /// # Handle return value
    /// 1. Err(WouldBlock): the buffer is full, call do_io to flush it.
    /// 2. _: handle it.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // if there is too much data inside the buffer, return WoundBlock
        let buffer = self.buffer.as_mut().expect("buffer mut expected");
        // moon patch (c10k P4b): lazily (re)materialize released storage
        // before the fullness check and copy-in.
        buffer.ensure_allocated();
        match mem::replace(&mut self.status, WriteStatus::Ok) {
            WriteStatus::Err(e) => return Err(e),
            WriteStatus::Ok if buffer.is_full() => return Err(io::ErrorKind::WouldBlock.into()),
            _ => (),
        }

        // there is space inside the buffer, copy to it.
        let to_copy = buf.len().min(buffer.available());
        unsafe { std::ptr::copy_nonoverlapping(buf.as_ptr(), buffer.write_ptr(), to_copy) };
        unsafe { buffer.set_init(to_copy) };
        Ok(to_copy)
    }

    /// `flush` to buffer.
    /// # Handle return value
    /// 1. Err(WouldBlock): the buffer is full, call do_io to flush it.
    /// 2. _: handle it.
    fn flush(&mut self) -> io::Result<()> {
        let buffer = self.buffer.as_mut().expect("buffer mut expected");
        match mem::replace(&mut self.status, WriteStatus::Ok) {
            WriteStatus::Err(e) => Err(e),
            WriteStatus::Ok if !buffer.is_empty() => Err(io::ErrorKind::WouldBlock.into()),
            _ => Ok(()),
        }
    }
}

// moon patch (c10k P4b): tests for lazy allocation, release, and the
// no-stash cancel-error contract.
#[cfg(test)]
mod moon_patch_tests {
    use std::future::Future;
    use std::io::{Read, Write};
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    use monoio::buf::{IoBufMut, IoVecBufMut};
    use monoio::io::{AsyncReadRent, CancelHandle, CancelableAsyncReadRent, Canceller};
    use monoio::BufResult;

    use super::*;

    /// Drive an immediately-ready future to completion without a runtime.
    fn now<F: Future>(fut: F) -> F::Output {
        let mut fut = pin!(fut);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => v,
            Poll::Pending => panic!("mock future must be immediately ready"),
        }
    }

    /// Mock io: serves one scripted result per read call.
    struct MockIo {
        results: Vec<Result<Vec<u8>, io::Error>>,
    }

    impl AsyncReadRent for MockIo {
        async fn read<T: IoBufMut>(&mut self, mut buf: T) -> BufResult<usize, T> {
            match self.results.remove(0) {
                Ok(data) => {
                    let n = data.len().min(buf.bytes_total());
                    unsafe {
                        std::ptr::copy_nonoverlapping(data.as_ptr(), buf.write_ptr(), n);
                        buf.set_init(n);
                    }
                    (Ok(n), buf)
                }
                Err(e) => (Err(e), buf),
            }
        }

        async fn readv<T: IoVecBufMut>(&mut self, buf: T) -> BufResult<usize, T> {
            (Ok(0), buf)
        }
    }

    impl CancelableAsyncReadRent for MockIo {
        async fn cancelable_read<T: IoBufMut>(
            &mut self,
            buf: T,
            _c: CancelHandle,
        ) -> BufResult<usize, T> {
            // The scripted results decide the outcome; a fired cancel is
            // modelled by scripting Err(ECANCELED), matching what monoio's
            // real drivers return for a cancelled op.
            self.read(buf).await
        }

        async fn cancelable_readv<T: IoVecBufMut>(
            &mut self,
            buf: T,
            _c: CancelHandle,
        ) -> BufResult<usize, T> {
            (Ok(0), buf)
        }
    }

    fn allocated(read: &SafeRead) -> usize {
        read.buffer.as_ref().unwrap().buf.len()
    }

    #[test]
    fn read_buffer_starts_released_and_allocates_on_do_io() {
        let mut r = SafeRead::new(64);
        assert_eq!(allocated(&r), 0, "must start lazy");
        let mut io = MockIo {
            results: vec![Ok(b"hello".to_vec())],
        };
        assert_eq!(now(r.do_io(&mut io)).unwrap(), 5);
        assert_eq!(allocated(&r), 64, "do_io must materialize storage");
        let mut out = [0u8; 8];
        assert_eq!(r.read(&mut out).unwrap(), 5);
        assert_eq!(&out[..5], b"hello");
        assert!(r.release_if_empty(), "drained buffer must release");
        assert_eq!(allocated(&r), 0);
        // Released buffer reallocates transparently on the next do_io.
        let mut io = MockIo {
            results: vec![Ok(b"again".to_vec())],
        };
        assert_eq!(now(r.do_io(&mut io)).unwrap(), 5);
        assert_eq!(r.read(&mut out).unwrap(), 5);
        assert_eq!(&out[..5], b"again");
    }

    #[test]
    fn release_refuses_undrained_buffer() {
        let mut r = SafeRead::new(64);
        let mut io = MockIo {
            results: vec![Ok(b"pending".to_vec())],
        };
        now(r.do_io(&mut io)).unwrap();
        assert!(!r.release_if_empty(), "undrained data must be kept");
        assert_eq!(allocated(&r), 64);
    }

    #[test]
    fn write_buffer_lazy_alloc_and_release() {
        let mut w = SafeWrite::new(64);
        assert_eq!(w.buffer.as_ref().unwrap().buf.len(), 0, "must start lazy");
        assert_eq!(w.write(b"abc").unwrap(), 3);
        assert_eq!(w.buffer.as_ref().unwrap().buf.len(), 64);
        assert!(!w.release_if_empty(), "unflushed data must be kept");
        // Drain via advance (as write_all's do_io would).
        w.buffer.as_mut().unwrap().advance(3);
        assert!(w.release_if_empty());
        assert_eq!(w.buffer.as_ref().unwrap().buf.len(), 0);
        // And it comes back on the next copy-in.
        assert_eq!(w.write(b"xyz").unwrap(), 3);
        assert_eq!(w.buffer.as_ref().unwrap().buf.len(), 64);
    }

    #[test]
    fn cancel_error_is_not_stashed_for_replay() {
        let mut r = SafeRead::new(64);
        let canceller = Canceller::new();
        let mut io = MockIo {
            results: vec![Err(io::Error::from_raw_os_error(125))],
        };
        let err = now(r.do_io_cancelable(&mut io, canceller.handle())).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(125), "ECANCELED surfaces once");
        // The next sync read must see a clean WouldBlock, not a replayed
        // cancel error (which would tear down a healthy connection).
        let mut out = [0u8; 8];
        let replay = r.read(&mut out).unwrap_err();
        assert_eq!(replay.kind(), io::ErrorKind::WouldBlock);
        // And a later cancelable read with a fresh handle works.
        let mut io = MockIo {
            results: vec![Ok(b"ok".to_vec())],
        };
        assert_eq!(now(r.do_io_cancelable(&mut io, canceller.handle())).unwrap(), 2);
    }

    #[test]
    fn real_error_is_still_stashed_on_plain_do_io() {
        let mut r = SafeRead::new(64);
        let mut io = MockIo {
            results: vec![Err(io::Error::new(io::ErrorKind::ConnectionReset, "boom"))],
        };
        assert!(now(r.do_io(&mut io)).is_err());
        let mut out = [0u8; 8];
        let replay = r.read(&mut out).unwrap_err();
        assert_eq!(replay.kind(), io::ErrorKind::ConnectionReset);
    }

    // moon patch (c1M P1-TLS): is_drained must be false whenever a byte OR a
    // deferred status is pending — task-parking on fd readability while
    // either waits would hang the connection.
    #[test]
    fn read_is_drained_tracks_bytes_and_deferred_status() {
        let mut r = SafeRead::new(64);
        assert!(r.is_drained(), "fresh buffer is drained");
        let mut io = MockIo {
            results: vec![Ok(b"data".to_vec())],
        };
        now(r.do_io(&mut io)).unwrap();
        assert!(!r.is_drained(), "buffered bytes must block parking");
        let mut out = [0u8; 8];
        r.read(&mut out).unwrap();
        assert!(r.is_drained(), "drained after consume");
        // Pending EOF status must also block parking (it would never make
        // the fd readable again).
        let mut io = MockIo {
            results: vec![Ok(Vec::new())],
        };
        now(r.do_io(&mut io)).unwrap();
        assert!(!r.is_drained(), "pending EOF must block parking");
        // Pending stashed error likewise.
        let mut r2 = SafeRead::new(64);
        let mut io = MockIo {
            results: vec![Err(io::Error::new(io::ErrorKind::ConnectionReset, "boom"))],
        };
        let _ = now(r2.do_io(&mut io));
        assert!(!r2.is_drained(), "stashed error must block parking");
    }

    #[test]
    fn write_is_drained_tracks_unflushed_bytes() {
        let mut w = SafeWrite::new(64);
        assert!(w.is_drained(), "fresh buffer is drained");
        w.write(b"abc").unwrap();
        assert!(!w.is_drained(), "unflushed bytes must block parking");
        w.buffer.as_mut().unwrap().advance(3);
        assert!(w.is_drained(), "drained after flush");
    }
}
