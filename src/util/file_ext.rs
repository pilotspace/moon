/// Cross-platform positioned I/O helpers.
///
/// These functions provide `write_at` and `read_exact_at` semantics on a
/// non-shared `std::fs::File` handle:
///
/// - **Unix**: delegates to `std::os::unix::fs::FileExt::write_all_at` /
///   `read_exact_at`, which loop internally and never move the file cursor.
///
/// - **Windows**: uses `std::os::windows::fs::FileExt::seek_write` /
///   `seek_read`. The Windows variants *do* move the file cursor — this is
///   acceptable here because **all call sites use fresh, non-shared handles**
///   (opened exclusively for one checkpoint/recovery pass). Never pass a handle
///   that is concurrently read or written.
///
/// # Invariant
/// Each call site MUST open its own `File` handle. Sharing a `File` across
/// concurrent `write_at`/`read_exact_at` calls on Windows is incorrect because
/// the cursor is process-global per handle.

use std::fs::File;
use std::io::{self, ErrorKind};

/// Write `buf` at byte `offset` of `file`.
///
/// The write is guaranteed to be complete (no short writes): on Unix this
/// delegates to `write_all_at`; on Windows the function loops until all bytes
/// are written.
pub fn write_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.write_all_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut written = 0usize;
        while written < buf.len() {
            let n = file.seek_write(&buf[written..], offset + written as u64)?;
            if n == 0 {
                return Err(io::Error::new(
                    ErrorKind::WriteZero,
                    "seek_write returned 0 — disk full or write error",
                ));
            }
            written += n;
        }
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (file, buf, offset);
        Err(io::Error::new(
            ErrorKind::Unsupported,
            "write_at: unsupported platform",
        ))
    }
}

/// Read exactly `buf.len()` bytes from `file` starting at byte `offset`.
///
/// Returns `UnexpectedEof` if the file ends before `buf` is filled.
/// On Unix delegates to `read_exact_at`; on Windows loops with `seek_read`.
pub fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut read = 0usize;
        while read < buf.len() {
            let n = file.seek_read(&mut buf[read..], offset + read as u64)?;
            if n == 0 {
                return Err(io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "read_exact_at: unexpected end of file",
                ));
            }
            read += n;
        }
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (file, buf, offset);
        Err(io::Error::new(
            ErrorKind::Unsupported,
            "read_exact_at: unsupported platform",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempfile;

    /// Roundtrip: write data at various offsets and read it back.
    #[test]
    fn test_roundtrip_at_offset_0() {
        let file = tempfile().expect("tempfile");
        let data = b"hello, world!";
        write_at(&file, data, 0).expect("write_at offset 0");
        let mut buf = vec![0u8; data.len()];
        read_exact_at(&file, &mut buf, 0).expect("read_exact_at offset 0");
        assert_eq!(&buf, data);
    }

    #[test]
    fn test_roundtrip_at_offset_4096() {
        let file = tempfile().expect("tempfile");
        let data = b"page-1 payload";
        write_at(&file, data, 4096).expect("write_at offset 4096");
        let mut buf = vec![0u8; data.len()];
        read_exact_at(&file, &mut buf, 4096).expect("read_exact_at offset 4096");
        assert_eq!(&buf, data);
    }

    #[test]
    fn test_roundtrip_at_offset_8192() {
        let file = tempfile().expect("tempfile");
        let data = b"page-2 payload";
        write_at(&file, data, 8192).expect("write_at offset 8192");
        let mut buf = vec![0u8; data.len()];
        read_exact_at(&file, &mut buf, 8192).expect("read_exact_at offset 8192");
        assert_eq!(&buf, data);
    }

    /// Multi-offset independence: writes at different offsets must not clobber
    /// each other.
    #[test]
    fn test_multi_offset_independence() {
        let file = tempfile().expect("tempfile");
        let data_a = b"AAAA";
        let data_b = b"BBBB";
        write_at(&file, data_a, 0).expect("write A");
        write_at(&file, data_b, 4096).expect("write B");

        let mut buf_a = [0u8; 4];
        let mut buf_b = [0u8; 4];
        read_exact_at(&file, &mut buf_a, 0).expect("read A");
        read_exact_at(&file, &mut buf_b, 4096).expect("read B");
        assert_eq!(&buf_a, data_a);
        assert_eq!(&buf_b, data_b);
    }

    /// Reading past EOF must return UnexpectedEof.
    #[test]
    fn test_eof_returns_unexpected_eof() {
        let file = tempfile().expect("tempfile");
        // File is empty; reading 1 byte must fail with UnexpectedEof.
        let mut buf = [0u8; 1];
        let err = read_exact_at(&file, &mut buf, 0).expect_err("should be EOF");
        assert_eq!(err.kind(), ErrorKind::UnexpectedEof, "got: {:?}", err);
    }

    /// Reading at an offset beyond the last written byte must return UnexpectedEof.
    #[test]
    fn test_eof_beyond_written_data() {
        let file = tempfile().expect("tempfile");
        write_at(&file, b"short", 0).expect("write");
        let mut buf = [0u8; 10];
        let err = read_exact_at(&file, &mut buf, 1024).expect_err("should be EOF");
        assert_eq!(err.kind(), ErrorKind::UnexpectedEof, "got: {:?}", err);
    }
}
