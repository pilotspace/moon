//! Pre-computed RESP response bytes for common responses.
//!
//! These constants avoid repeated serialization of frequently-sent responses
//! like +OK, +PONG, $-1 (null bulk), and small integers. The integer table
//! caches `:0\r\n` through `:999\r\n` for O(1) lookup.

use once_cell::sync::Lazy;

/// +OK\r\n
pub const OK: &[u8] = b"+OK\r\n";

/// +PONG\r\n
pub const PONG: &[u8] = b"+PONG\r\n";

/// $-1\r\n (null bulk string)
pub const NULL_BULK: &[u8] = b"$-1\r\n";

/// *-1\r\n (null array)
pub const NULL_ARRAY: &[u8] = b"*-1\r\n";

/// *0\r\n (empty array)
pub const EMPTY_ARRAY: &[u8] = b"*0\r\n";

/// $0\r\n\r\n (empty bulk string)
pub const EMPTY_BULK: &[u8] = b"$0\r\n\r\n";

/// \r\n (CRLF separator)
pub const CRLF: &[u8] = b"\r\n";

/// +QUEUED\r\n
pub const QUEUED: &[u8] = b"+QUEUED\r\n";

/// :0\r\n
pub const ZERO: &[u8] = b":0\r\n";

/// :1\r\n
pub const ONE: &[u8] = b":1\r\n";

/// Pre-computed integer responses for :0\r\n through :999\r\n.
static INTEGER_TABLE: Lazy<Vec<Vec<u8>>> = Lazy::new(|| {
    (0..1000)
        .map(|i| {
            let mut buf = Vec::with_capacity(8);
            buf.push(b':');
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(i as i64).as_bytes());
            buf.extend_from_slice(b"\r\n");
            buf
        })
        .collect()
});

/// Return a cached RESP integer response for values 0..999.
/// Returns `None` for values outside that range.
#[inline]
pub fn integer_response(n: i64) -> Option<&'static [u8]> {
    if n >= 0 && n < 1000 {
        Some(INTEGER_TABLE[n as usize].as_slice())
    } else {
        None
    }
}

/// Write a bulk string header `$<len>\r\n` into a stack buffer.
/// Returns the number of bytes written.
#[inline]
pub fn bulk_string_header(len: usize, buf: &mut [u8; 32]) -> usize {
    buf[0] = b'$';
    let mut itoa_buf = itoa::Buffer::new();
    let digits = itoa_buf.format(len).as_bytes();
    let end = 1 + digits.len();
    buf[1..end].copy_from_slice(digits);
    buf[end] = b'\r';
    buf[end + 1] = b'\n';
    end + 2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ok_bytes() {
        assert_eq!(OK, b"+OK\r\n");
    }

    #[test]
    fn test_pong_bytes() {
        assert_eq!(PONG, b"+PONG\r\n");
    }

    #[test]
    fn test_null_bulk_bytes() {
        assert_eq!(NULL_BULK, b"$-1\r\n");
    }

    #[test]
    fn test_null_array_bytes() {
        assert_eq!(NULL_ARRAY, b"*-1\r\n");
    }

    #[test]
    fn test_empty_array_bytes() {
        assert_eq!(EMPTY_ARRAY, b"*0\r\n");
    }

    #[test]
    fn test_empty_bulk_bytes() {
        assert_eq!(EMPTY_BULK, b"$0\r\n\r\n");
    }

    #[test]
    fn test_queued_bytes() {
        assert_eq!(QUEUED, b"+QUEUED\r\n");
    }

    #[test]
    fn test_zero_bytes() {
        assert_eq!(ZERO, b":0\r\n");
    }

    #[test]
    fn test_one_bytes() {
        assert_eq!(ONE, b":1\r\n");
    }

    #[test]
    fn test_integer_response_zero() {
        assert_eq!(integer_response(0), Some(b":0\r\n".as_slice()));
    }

    #[test]
    fn test_integer_response_one() {
        assert_eq!(integer_response(1), Some(b":1\r\n".as_slice()));
    }

    #[test]
    fn test_integer_response_999() {
        assert_eq!(integer_response(999), Some(b":999\r\n".as_slice()));
    }

    #[test]
    fn test_integer_response_1000_none() {
        assert_eq!(integer_response(1000), None);
    }

    #[test]
    fn test_integer_response_negative_none() {
        assert_eq!(integer_response(-1), None);
    }

    #[test]
    fn test_integer_response_42() {
        assert_eq!(integer_response(42), Some(b":42\r\n".as_slice()));
    }

    #[test]
    fn test_bulk_string_header_len_5() {
        let mut buf = [0u8; 32];
        let n = bulk_string_header(5, &mut buf);
        assert_eq!(&buf[..n], b"$5\r\n");
    }

    #[test]
    fn test_bulk_string_header_len_0() {
        let mut buf = [0u8; 32];
        let n = bulk_string_header(0, &mut buf);
        assert_eq!(&buf[..n], b"$0\r\n");
    }

    #[test]
    fn test_bulk_string_header_large() {
        let mut buf = [0u8; 32];
        let n = bulk_string_header(12345, &mut buf);
        assert_eq!(&buf[..n], b"$12345\r\n");
    }
}
