//! Async read-line convenience function.
//!
//! # Cancel Safety
//!
//! [`ReadLine`] is cancel-safe. Bytes already appended to the output `String`
//! remain committed. If cancelled and then restarted with a fresh `ReadLine`,
//! the caller can observe the partial line already present in the buffer.

use super::AsyncBufRead;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Read bytes from `reader` until a newline (`\n`) is found, appending them
/// (including the newline) to `buf`.
///
/// Returns the number of bytes read (including the newline). If the reader
/// reaches EOF without a newline, the remaining bytes are still appended and
/// counted. Returns `Ok(0)` only when the reader is at EOF and no bytes remain.
///
/// `\r\n` line endings are normalised: the `\r` before `\n` is stripped from
/// `buf`, but it **is** counted in the returned byte count (matching
/// `std::io::BufRead::read_line` semantics for the return value).
///
/// # Cancel Safety
///
/// This future is cancel-safe. Bytes already appended to `buf` remain
/// committed upon cancellation. The caller should be aware that `buf` may
/// contain a partial line if the future is dropped before completion.
///
/// # Example
///
/// ```ignore
/// use asupersync::io::{BufReader, read_line};
///
/// let mut reader = BufReader::new(&b"hello\nworld\n"[..]);
/// let mut line = String::new();
/// let n = read_line(&mut reader, &mut line).await?;
/// assert_eq!(line, "hello\n");
/// assert_eq!(n, 6);
/// ```
pub fn read_line<'a, R>(reader: &'a mut R, buf: &'a mut String) -> ReadLine<'a, R>
where
    R: AsyncBufRead + Unpin + ?Sized,
{
    ReadLine {
        reader,
        buf,
        bytes_read: 0,
    }
}

/// Future for the [`read_line`] function.
pub struct ReadLine<'a, R: ?Sized> {
    reader: &'a mut R,
    buf: &'a mut String,
    bytes_read: usize,
}

impl<R> Future for ReadLine<'_, R>
where
    R: AsyncBufRead + Unpin + ?Sized,
{
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            let available = match Pin::new(&mut *this.reader).poll_fill_buf(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(buf)) => buf,
            };

            // EOF
            if available.is_empty() {
                return Poll::Ready(Ok(this.bytes_read));
            }

            // Scan for newline
            if let Some(pos) = available.iter().position(|&b| b == b'\n') {
                // Include everything up to and including the newline
                let chunk = &available[..=pos];
                let chunk_len = chunk.len();

                // Validate as UTF-8 and append
                let s = std::str::from_utf8(chunk)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                this.buf.push_str(s);
                this.bytes_read += chunk_len;
                Pin::new(&mut *this.reader).consume(chunk_len);

                // Strip \r before \n if present
                let buf_bytes = this.buf.as_bytes();
                let len = buf_bytes.len();
                if len >= 2 && buf_bytes[len - 2] == b'\r' && buf_bytes[len - 1] == b'\n' {
                    // Remove the \r (keep the \n)
                    let cr_pos = this.buf.len() - 2;
                    this.buf.remove(cr_pos);
                }

                return Poll::Ready(Ok(this.bytes_read));
            }

            // No newline found — consume all available bytes
            let all_len = available.len();
            let s = std::str::from_utf8(available)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            this.buf.push_str(s);
            this.bytes_read += all_len;
            Pin::new(&mut *this.reader).consume(all_len);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::BufReader;
    use std::sync::Arc;
    use std::task::{Wake, Waker};

    struct NoopWaker;

    impl Wake for NoopWaker {
        fn wake(self: Arc<Self>) {}
    }

    fn noop_waker() -> Waker {
        Waker::from(Arc::new(NoopWaker))
    }

    fn poll_ready<F: Future>(fut: &mut Pin<&mut F>) -> Option<F::Output> {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        for _ in 0..1024 {
            if let Poll::Ready(output) = fut.as_mut().poll(&mut cx) {
                return Some(output);
            }
        }
        None
    }

    fn init_test(name: &str) {
        crate::test_utils::init_test_logging();
        crate::test_phase!(name);
    }

    #[test]
    fn read_line_basic() {
        init_test("read_line_basic");
        let mut reader = BufReader::new(&b"hello\nworld\n"[..]);
        let mut line = String::new();
        let mut fut = read_line(&mut reader, &mut line);
        let mut fut = Pin::new(&mut fut);
        let n = poll_ready(&mut fut)
            .expect("future did not resolve")
            .unwrap();
        crate::assert_with_log!(n == 6, "bytes", 6, n);
        crate::assert_with_log!(line == "hello\n", "line", "hello\n", line);
        crate::test_complete!("read_line_basic");
    }

    #[test]
    fn read_line_crlf() {
        init_test("read_line_crlf");
        let mut reader = BufReader::new(&b"hello\r\nworld\r\n"[..]);
        let mut line = String::new();
        let mut fut = read_line(&mut reader, &mut line);
        let mut fut = Pin::new(&mut fut);
        let n = poll_ready(&mut fut)
            .expect("future did not resolve")
            .unwrap();
        // \r\n is 7 bytes read, but \r is stripped from the string
        crate::assert_with_log!(n == 7, "bytes", 7, n);
        crate::assert_with_log!(line == "hello\n", "line", "hello\n", line);
        crate::test_complete!("read_line_crlf");
    }

    #[test]
    fn read_line_eof_no_newline() {
        init_test("read_line_eof_no_newline");
        let mut reader = BufReader::new(&b"no newline"[..]);
        let mut line = String::new();
        let mut fut = read_line(&mut reader, &mut line);
        let mut fut = Pin::new(&mut fut);
        let n = poll_ready(&mut fut)
            .expect("future did not resolve")
            .unwrap();
        crate::assert_with_log!(n == 10, "bytes", 10, n);
        crate::assert_with_log!(line == "no newline", "line", "no newline", line);
        crate::test_complete!("read_line_eof_no_newline");
    }

    #[test]
    fn read_line_empty() {
        init_test("read_line_empty");
        let mut reader = BufReader::new(&b""[..]);
        let mut line = String::new();
        let mut fut = read_line(&mut reader, &mut line);
        let mut fut = Pin::new(&mut fut);
        let n = poll_ready(&mut fut)
            .expect("future did not resolve")
            .unwrap();
        crate::assert_with_log!(n == 0, "bytes", 0, n);
        let empty = line.is_empty();
        crate::assert_with_log!(empty, "line empty", true, empty);
        crate::test_complete!("read_line_empty");
    }

    #[test]
    fn read_line_successive() {
        init_test("read_line_successive");
        let mut reader = BufReader::new(&b"first\nsecond\n"[..]);

        let mut line1 = String::new();
        let mut fut = read_line(&mut reader, &mut line1);
        let mut fut = Pin::new(&mut fut);
        let n1 = poll_ready(&mut fut)
            .expect("future did not resolve")
            .unwrap();
        crate::assert_with_log!(n1 == 6, "bytes1", 6, n1);
        crate::assert_with_log!(line1 == "first\n", "line1", "first\n", line1);

        let mut line2 = String::new();
        let mut fut = read_line(&mut reader, &mut line2);
        let mut fut = Pin::new(&mut fut);
        let n2 = poll_ready(&mut fut)
            .expect("future did not resolve")
            .unwrap();
        crate::assert_with_log!(n2 == 7, "bytes2", 7, n2);
        crate::assert_with_log!(line2 == "second\n", "line2", "second\n", line2);

        // EOF
        let mut line3 = String::new();
        let mut fut = read_line(&mut reader, &mut line3);
        let mut fut = Pin::new(&mut fut);
        let n3 = poll_ready(&mut fut)
            .expect("future did not resolve")
            .unwrap();
        crate::assert_with_log!(n3 == 0, "bytes3", 0, n3);
        crate::test_complete!("read_line_successive");
    }

    #[test]
    fn read_line_only_newline() {
        init_test("read_line_only_newline");
        let mut reader = BufReader::new(&b"\n"[..]);
        let mut line = String::new();
        let mut fut = read_line(&mut reader, &mut line);
        let mut fut = Pin::new(&mut fut);
        let n = poll_ready(&mut fut)
            .expect("future did not resolve")
            .unwrap();
        crate::assert_with_log!(n == 1, "bytes", 1, n);
        crate::assert_with_log!(line == "\n", "line", "\n", line);
        crate::test_complete!("read_line_only_newline");
    }

    #[test]
    fn read_line_invalid_utf8() {
        init_test("read_line_invalid_utf8");
        let mut reader = BufReader::new(&[0xff, 0xfe, b'\n'][..]);
        let mut line = String::new();
        let mut fut = read_line(&mut reader, &mut line);
        let mut fut = Pin::new(&mut fut);
        let err = poll_ready(&mut fut)
            .expect("future did not resolve")
            .unwrap_err();
        let kind = err.kind();
        crate::assert_with_log!(
            kind == io::ErrorKind::InvalidData,
            "error kind",
            io::ErrorKind::InvalidData,
            kind
        );
        crate::test_complete!("read_line_invalid_utf8");
    }
}
