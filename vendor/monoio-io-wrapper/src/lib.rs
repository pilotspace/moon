#![allow(clippy::unsafe_removed_from_name)]

use monoio::io::{AsyncReadRent, AsyncWriteRent};

mod safe_io;
#[cfg(feature = "unsafe_io")]
mod unsafe_io;

#[derive(Debug)]
pub enum ReadBuffer {
    Safe(safe_io::SafeRead),
    #[cfg(feature = "unsafe_io")]
    Unsafe(unsafe_io::UnsafeRead),
}

#[derive(Debug)]
pub enum WriteBuffer {
    Safe(safe_io::SafeWrite),
    #[cfg(feature = "unsafe_io")]
    Unsafe(unsafe_io::UnsafeWrite),
}

impl ReadBuffer {
    /// Create a new ReadBuffer with given buffer size.
    #[inline]
    pub fn new(buffer_size: usize) -> Self {
        Self::Safe(safe_io::SafeRead::new(buffer_size))
    }

    /// Create a new ReadBuffer that uses unsafe I/O.
    /// # Safety
    /// Users must make sure the buffer ptr and len is valid until io finished.
    /// So the Future cannot be dropped directly. Consider using CancellableIO.
    #[inline]
    #[cfg(feature = "unsafe_io")]
    pub const unsafe fn new_unsafe() -> Self {
        Self::Unsafe(unsafe_io::UnsafeRead::new())
    }

    #[inline]
    pub async fn do_io<IO: AsyncReadRent>(&mut self, mut io: IO) -> std::io::Result<usize> {
        match self {
            Self::Safe(b) => b.do_io(&mut io).await,
            #[cfg(feature = "unsafe_io")]
            Self::Unsafe(b) => unsafe { b.do_io(&mut io).await },
        }
    }

    /// moon patch (c10k P4b): cancelable variant of `do_io`. Cancel errors
    /// surface directly without being stashed for replay (see
    /// `SafeRead::do_io_cancelable`). The unsafe-io variant has no cancel
    /// support and falls back to a plain read.
    #[inline]
    pub async fn do_io_cancelable<IO: monoio::io::CancelableAsyncReadRent>(
        &mut self,
        mut io: IO,
        c: monoio::io::CancelHandle,
    ) -> std::io::Result<usize> {
        match self {
            Self::Safe(b) => b.do_io_cancelable(&mut io, c).await,
            #[cfg(feature = "unsafe_io")]
            Self::Unsafe(b) => unsafe { b.do_io(&mut io).await },
        }
    }

    /// moon patch (c10k P4b): drop the backing storage while drained; it
    /// reallocates lazily on next use. No-op (false) for unsafe-io buffers.
    #[inline]
    pub fn release_if_empty(&mut self) -> bool {
        match self {
            Self::Safe(b) => b.release_if_empty(),
            #[cfg(feature = "unsafe_io")]
            Self::Unsafe(_) => false,
        }
    }

    /// moon patch (c1M P1-TLS): no bytes buffered and no deferred status
    /// pending. Conservatively false for unsafe-io buffers (never
    /// task-park them).
    #[inline]
    pub fn is_drained(&self) -> bool {
        match self {
            Self::Safe(b) => b.is_drained(),
            #[cfg(feature = "unsafe_io")]
            Self::Unsafe(_) => false,
        }
    }

    #[inline]
    #[cfg(feature = "unsafe_io")]
    pub fn is_safe(&self) -> bool {
        match self {
            Self::Safe(_) => true,
            #[cfg(feature = "unsafe_io")]
            Self::Unsafe(_) => false,
        }
    }

    #[inline]
    #[cfg(not(feature = "unsafe_io"))]
    pub const fn is_safe(&self) -> bool {
        true
    }
}

impl Default for ReadBuffer {
    #[inline]
    fn default() -> Self {
        Self::Safe(Default::default())
    }
}

impl std::io::Read for ReadBuffer {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Safe(b) => b.read(buf),
            #[cfg(feature = "unsafe_io")]
            Self::Unsafe(b) => b.read(buf),
        }
    }
}

impl WriteBuffer {
    /// Create a new WriteBuffer with given buffer size.
    #[inline]
    pub fn new(buffer_size: usize) -> Self {
        Self::Safe(safe_io::SafeWrite::new(buffer_size))
    }

    /// Create a new WriteBuffer that uses unsafe I/O.
    /// # Safety
    /// Users must make sure the buffer ptr and len is valid until io finished.
    /// So the Future cannot be dropped directly. Consider using CancellableIO.
    #[inline]
    #[cfg(feature = "unsafe_io")]
    pub const unsafe fn new_unsafe() -> Self {
        Self::Unsafe(unsafe_io::UnsafeWrite::new())
    }

    #[inline]
    pub async fn do_io<IO: AsyncWriteRent>(&mut self, mut io: IO) -> std::io::Result<usize> {
        match self {
            Self::Safe(buf) => buf.do_io(&mut io).await,
            #[cfg(feature = "unsafe_io")]
            Self::Unsafe(buf) => unsafe { buf.do_io(&mut io).await },
        }
    }

    /// moon patch (c10k P4b): drop the backing storage while drained; it
    /// reallocates lazily on next use. No-op (false) for unsafe-io buffers.
    #[inline]
    pub fn release_if_empty(&mut self) -> bool {
        match self {
            Self::Safe(b) => b.release_if_empty(),
            #[cfg(feature = "unsafe_io")]
            Self::Unsafe(_) => false,
        }
    }

    /// moon patch (c1M P1-TLS): no bytes buffered and no stashed write
    /// error pending. Conservatively false for unsafe-io buffers.
    #[inline]
    pub fn is_drained(&self) -> bool {
        match self {
            Self::Safe(b) => b.is_drained(),
            #[cfg(feature = "unsafe_io")]
            Self::Unsafe(_) => false,
        }
    }

    #[inline]
    #[cfg(feature = "unsafe_io")]
    pub fn is_safe(&self) -> bool {
        match self {
            Self::Safe(_) => true,
            #[cfg(feature = "unsafe_io")]
            Self::Unsafe(_) => false,
        }
    }

    #[inline]
    #[cfg(not(feature = "unsafe_io"))]
    pub const fn is_safe(&self) -> bool {
        true
    }
}

impl Default for WriteBuffer {
    #[inline]
    fn default() -> Self {
        Self::Safe(Default::default())
    }
}

impl std::io::Write for WriteBuffer {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Safe(b) => b.write(buf),
            #[cfg(feature = "unsafe_io")]
            Self::Unsafe(b) => b.write(buf),
        }
    }

    #[inline]
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Safe(b) => b.flush(),
            #[cfg(feature = "unsafe_io")]
            Self::Unsafe(b) => b.flush(),
        }
    }
}
