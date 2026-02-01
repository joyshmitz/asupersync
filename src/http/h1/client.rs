//! HTTP/1.1 client for sending requests over a transport.
//!
//! [`Http1Client`] sends a single HTTP/1.1 request and reads the response.

use crate::bytes::BytesMut;
use crate::codec::Framed;
use crate::http::h1::codec::{
    parse_header_line, require_transfer_encoding_chunked, unique_header_value,
    validate_header_field, ChunkedBodyDecoder, HttpError,
};
use crate::http::h1::types::{Request, Response, Version};
use crate::io::{AsyncRead, AsyncWrite};
use crate::stream::Stream;
use std::fmt::Write;
use std::pin::Pin;

/// Maximum allowed header block size (64 KiB).
const DEFAULT_MAX_HEADERS_SIZE: usize = 64 * 1024;

/// Maximum allowed body size (16 MiB).
const DEFAULT_MAX_BODY_SIZE: usize = 16 * 1024 * 1024;

/// Maximum number of headers.
const MAX_HEADERS: usize = 128;

/// HTTP/1.1 client codec that encodes *requests* and decodes *responses*.
///
/// This is the mirror of [`Http1Codec`](super::Http1Codec) which decodes
/// requests and encodes responses. The client codec is used with
/// [`Framed`] for client-side connections.
///
/// # Limits
///
/// - Maximum header block size: 64 KiB (configurable via [`max_headers_size`](Self::max_headers_size))
/// - Maximum body size: 16 MiB (configurable via [`max_body_size`](Self::max_body_size))
/// - Maximum number of headers: 128
pub struct Http1ClientCodec {
    state: ClientDecodeState,
    max_headers_size: usize,
    max_body_size: usize,
}

enum ClientDecodeState {
    Head,
    Body {
        version: Version,
        status: u16,
        reason: String,
        headers: Vec<(String, String)>,
        remaining: usize,
    },
    Chunked {
        version: Version,
        status: u16,
        reason: String,
        headers: Vec<(String, String)>,
        chunked: ChunkedBodyDecoder,
    },
    /// Response body is delimited by EOF (no Content-Length/Transfer-Encoding).
    Eof {
        version: Version,
        status: u16,
        reason: String,
        headers: Vec<(String, String)>,
    },
}

impl Http1ClientCodec {
    /// Create a new client codec with default limits.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: ClientDecodeState::Head,
            max_headers_size: DEFAULT_MAX_HEADERS_SIZE,
            max_body_size: DEFAULT_MAX_BODY_SIZE,
        }
    }

    /// Set the maximum header block size.
    #[must_use]
    pub fn max_headers_size(mut self, size: usize) -> Self {
        self.max_headers_size = size;
        self
    }

    /// Set the maximum body size.
    #[must_use]
    pub fn max_body_size(mut self, size: usize) -> Self {
        self.max_body_size = size;
        self
    }
}

impl Default for Http1ClientCodec {
    fn default() -> Self {
        Self::new()
    }
}

/// Find `\r\n\r\n` delimiter.
fn find_headers_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4)
}

/// Parse status line: `HTTP/1.1 200 OK`.
fn parse_status_line(line: &str) -> Result<(Version, u16, String), HttpError> {
    let mut parts = line.splitn(3, ' ');
    let ver = parts.next().ok_or(HttpError::BadRequestLine)?;
    let code = parts.next().ok_or(HttpError::BadRequestLine)?;
    let reason = parts.next().unwrap_or("").to_owned();

    let version = Version::from_bytes(ver.as_bytes()).ok_or(HttpError::UnsupportedVersion)?;
    let status: u16 = code.parse().map_err(|_| HttpError::BadRequestLine)?;

    Ok((version, status, reason))
}

impl crate::codec::Decoder for Http1ClientCodec {
    type Item = Response;
    type Error = HttpError;

    #[allow(clippy::too_many_lines)]
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Response>, HttpError> {
        loop {
            match &mut self.state {
                state @ ClientDecodeState::Head => {
                    let Some(end) = find_headers_end(src.as_ref()) else {
                        // Check for header overflow while waiting for more data
                        if src.len() > self.max_headers_size {
                            return Err(HttpError::HeadersTooLarge);
                        }
                        return Ok(None);
                    };

                    if end > self.max_headers_size {
                        return Err(HttpError::HeadersTooLarge);
                    }

                    let head_bytes = src.split_to(end);
                    let head_str = std::str::from_utf8(head_bytes.as_ref())
                        .map_err(|_| HttpError::BadRequestLine)?;

                    let mut lines = head_str.split("\r\n");
                    let status_line = lines.next().ok_or(HttpError::BadRequestLine)?;
                    let (version, status, reason) = parse_status_line(status_line)?;

                    let mut headers = Vec::new();
                    for line in lines {
                        if line.is_empty() {
                            break;
                        }
                        headers.push(parse_header_line(line)?);
                        if headers.len() > MAX_HEADERS {
                            return Err(HttpError::TooManyHeaders);
                        }
                    }

                    // RFC 7230/9110: responses with these status codes have no body.
                    if matches!(status, 100..=199 | 204 | 304) {
                        *state = ClientDecodeState::Head;
                        return Ok(Some(Response {
                            version,
                            status,
                            reason,
                            headers,
                            body: Vec::new(),
                            trailers: Vec::new(),
                        }));
                    }

                    // RFC 7230 3.3.3: Reject responses with both Transfer-Encoding
                    // and Content-Length to prevent response smuggling.
                    let te = unique_header_value(&headers, "Transfer-Encoding")?;
                    let cl = unique_header_value(&headers, "Content-Length")?;
                    if te.is_some() && cl.is_some() {
                        return Err(HttpError::AmbiguousBodyLength);
                    }

                    if let Some(te) = te {
                        require_transfer_encoding_chunked(te)?;
                        *state = ClientDecodeState::Chunked {
                            version,
                            status,
                            reason,
                            headers,
                            chunked: ChunkedBodyDecoder::new(
                                self.max_body_size,
                                self.max_headers_size,
                            ),
                        };
                        continue;
                    }

                    if let Some(cl) = cl {
                        let content_length: usize =
                            cl.trim().parse().map_err(|_| HttpError::BadContentLength)?;

                        if content_length == 0 {
                            *state = ClientDecodeState::Head;
                            return Ok(Some(Response {
                                version,
                                status,
                                reason,
                                headers,
                                body: Vec::new(),
                                trailers: Vec::new(),
                            }));
                        }

                        // Check body size limit upfront for Content-Length
                        if content_length > self.max_body_size {
                            return Err(HttpError::BodyTooLarge);
                        }

                        *state = ClientDecodeState::Body {
                            version,
                            status,
                            reason,
                            headers,
                            remaining: content_length,
                        };
                        continue;
                    }

                    // No Content-Length/Transfer-Encoding: body is delimited by EOF.
                    if src.len() > self.max_body_size {
                        return Err(HttpError::BodyTooLarge);
                    }

                    *state = ClientDecodeState::Eof {
                        version,
                        status,
                        reason,
                        headers,
                    };
                    return Ok(None);
                }

                ClientDecodeState::Body { remaining, .. } => {
                    let need = *remaining;
                    if src.len() < need {
                        return Ok(None);
                    }

                    let body_bytes = src.split_to(need);
                    let old = std::mem::replace(&mut self.state, ClientDecodeState::Head);
                    let ClientDecodeState::Body {
                        version,
                        status,
                        reason,
                        headers,
                        ..
                    } = old
                    else {
                        unreachable!()
                    };

                    return Ok(Some(Response {
                        version,
                        status,
                        reason,
                        headers,
                        body: body_bytes.to_vec(),
                        trailers: Vec::new(),
                    }));
                }

                ClientDecodeState::Chunked { chunked, .. } => {
                    let Some((body, trailers)) = chunked.decode(src)? else {
                        return Ok(None);
                    };

                    let old = std::mem::replace(&mut self.state, ClientDecodeState::Head);
                    let ClientDecodeState::Chunked {
                        version,
                        status,
                        reason,
                        headers,
                        ..
                    } = old
                    else {
                        unreachable!()
                    };

                    return Ok(Some(Response {
                        version,
                        status,
                        reason,
                        headers,
                        body,
                        trailers,
                    }));
                }

                ClientDecodeState::Eof { .. } => {
                    if src.len() > self.max_body_size {
                        return Err(HttpError::BodyTooLarge);
                    }
                    return Ok(None);
                }
            }
        }
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Response>, HttpError> {
        if matches!(&self.state, ClientDecodeState::Eof { .. }) {
            let old = std::mem::replace(&mut self.state, ClientDecodeState::Head);
            let ClientDecodeState::Eof {
                version,
                status,
                reason,
                headers,
            } = old
            else {
                unreachable!()
            };

            if src.len() > self.max_body_size {
                return Err(HttpError::BodyTooLarge);
            }

            let body = src.split_to(src.len()).to_vec();
            return Ok(Some(Response {
                version,
                status,
                reason,
                headers,
                body,
                trailers: Vec::new(),
            }));
        }

        match self.decode(src)? {
            Some(frame) => Ok(Some(frame)),
            None if src.is_empty() => Ok(None),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "incomplete frame at EOF",
            )
            .into()),
        }
    }
}

impl crate::codec::Encoder<Request> for Http1ClientCodec {
    type Error = HttpError;

    fn encode(&mut self, req: Request, dst: &mut BytesMut) -> Result<(), HttpError> {
        if req.uri.contains('\r')
            || req.uri.contains('\n')
            || req.uri.contains(' ')
            || req.uri.contains('\t')
        {
            return Err(HttpError::BadRequestLine);
        }
        if req.method.as_str().contains('\r')
            || req.method.as_str().contains('\n')
            || req.method.as_str().contains(' ')
            || req.method.as_str().contains('\t')
        {
            return Err(HttpError::BadMethod);
        }

        let te = unique_header_value(&req.headers, "Transfer-Encoding")?;
        let cl = unique_header_value(&req.headers, "Content-Length")?;

        let chunked = match te {
            Some(value) => {
                require_transfer_encoding_chunked(value)?;
                true
            }
            None => false,
        };

        if chunked && cl.is_some() {
            return Err(HttpError::AmbiguousBodyLength);
        }
        if !chunked && !req.trailers.is_empty() {
            return Err(HttpError::TrailersNotAllowed);
        }
        if !chunked {
            if let Some(cl) = cl {
                let declared: usize = cl.trim().parse().map_err(|_| HttpError::BadContentLength)?;
                if declared != req.body.len() {
                    return Err(HttpError::BadContentLength);
                }
            }
        }

        // Request line
        let mut head = String::with_capacity(256);
        let _ = write!(head, "{} {} {}\r\n", req.method, req.uri, req.version);

        // Headers
        let mut has_content_length = false;
        for (name, value) in &req.headers {
            validate_header_field(name, value)?;
            if name.eq_ignore_ascii_case("content-length") {
                has_content_length = true;
            }
            let _ = write!(head, "{name}: {value}\r\n");
        }

        if chunked {
            head.push_str("\r\n");
            dst.extend_from_slice(head.as_bytes());

            if !req.body.is_empty() {
                let mut chunk_line = String::with_capacity(16);
                let _ = write!(chunk_line, "{:X}\r\n", req.body.len());
                dst.extend_from_slice(chunk_line.as_bytes());
                dst.extend_from_slice(&req.body);
                dst.extend_from_slice(b"\r\n");
            }

            dst.extend_from_slice(b"0\r\n");
            if !req.trailers.is_empty() {
                let mut trailer_block = String::with_capacity(256);
                for (name, value) in &req.trailers {
                    validate_header_field(name, value)?;
                    let _ = write!(trailer_block, "{name}: {value}\r\n");
                }
                dst.extend_from_slice(trailer_block.as_bytes());
            }
            dst.extend_from_slice(b"\r\n");
            return Ok(());
        }

        if !has_content_length && !req.body.is_empty() {
            let _ = write!(head, "Content-Length: {}\r\n", req.body.len());
        }

        head.push_str("\r\n");

        dst.extend_from_slice(head.as_bytes());
        if !req.body.is_empty() {
            dst.extend_from_slice(&req.body);
        }

        Ok(())
    }
}

/// A simple HTTP/1.1 client for sending a single request over a transport.
pub struct Http1Client;

impl Http1Client {
    /// Send a request over the given transport and return the response.
    pub async fn request<T>(io: T, req: Request) -> Result<Response, HttpError>
    where
        T: AsyncRead + AsyncWrite + Unpin,
    {
        let codec = Http1ClientCodec::new();
        let mut framed = Framed::new(io, codec);

        // Encode and buffer the request
        framed.send(req)?;

        // Read response
        match std::future::poll_fn(|cx| Pin::new(&mut framed).poll_next(cx)).await {
            Some(Ok(resp)) => Ok(resp),
            Some(Err(e)) => Err(e),
            None => Err(HttpError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed before response",
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytes::BytesMut;
    use crate::codec::Decoder;

    #[test]
    fn decode_simple_response() {
        let mut codec = Http1ClientCodec::new();
        let mut buf = BytesMut::from(&b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello"[..]);
        let resp = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.reason, "OK");
        assert_eq!(resp.version, Version::Http11);
        assert_eq!(resp.body, b"hello");
        assert!(resp.trailers.is_empty());
    }

    #[test]
    fn decode_response_no_body() {
        let mut codec = Http1ClientCodec::new();
        let mut buf = BytesMut::from(&b"HTTP/1.1 204 No Content\r\n\r\n"[..]);
        let resp = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(resp.status, 204);
        assert!(resp.body.is_empty());
        assert!(resp.trailers.is_empty());
    }

    #[test]
    fn decode_response_incomplete() {
        let mut codec = Http1ClientCodec::new();
        let mut buf = BytesMut::from(&b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nhel"[..]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn encode_request() {
        let mut codec = Http1ClientCodec::new();
        let req = Request {
            method: crate::http::h1::types::Method::Get,
            uri: "/index.html".into(),
            version: Version::Http11,
            headers: vec![("Host".into(), "example.com".into())],
            body: Vec::new(),
            trailers: Vec::new(),
        };
        let mut buf = BytesMut::with_capacity(256);
        crate::codec::Encoder::encode(&mut codec, req, &mut buf).unwrap();
        let s = String::from_utf8(buf.to_vec()).unwrap();
        assert!(s.starts_with("GET /index.html HTTP/1.1\r\n"));
        assert!(s.contains("Host: example.com\r\n"));
    }

    #[test]
    fn encode_request_with_body() {
        let mut codec = Http1ClientCodec::new();
        let req = Request {
            method: crate::http::h1::types::Method::Post,
            uri: "/api".into(),
            version: Version::Http11,
            headers: vec![("Host".into(), "api.example.com".into())],
            body: b"data".to_vec(),
            trailers: Vec::new(),
        };
        let mut buf = BytesMut::with_capacity(256);
        crate::codec::Encoder::encode(&mut codec, req, &mut buf).unwrap();
        let s = String::from_utf8(buf.to_vec()).unwrap();
        assert!(s.contains("Content-Length: 4\r\n"));
        assert!(s.ends_with("\r\n\r\ndata"));
    }

    #[test]
    fn decode_chunked_response() {
        let mut codec = Http1ClientCodec::new();
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\
                    5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        let mut buf = BytesMut::from(&raw[..]);
        let resp = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"hello world");
        assert!(resp.trailers.is_empty());
    }

    #[test]
    fn decode_chunked_response_with_trailers() {
        let mut codec = Http1ClientCodec::new();
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\
                    5\r\nhello\r\n0\r\nX-Trailer: one\r\nY-Trailer: two\r\n\r\n";
        let mut buf = BytesMut::from(&raw[..]);
        let resp = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(resp.body, b"hello");
        assert_eq!(resp.trailers.len(), 2);
        assert_eq!(resp.trailers[0].0, "X-Trailer");
        assert_eq!(resp.trailers[0].1, "one");
    }

    #[test]
    fn decode_response_without_length_is_eof_delimited() {
        let mut codec = Http1ClientCodec::new();
        let raw = b"HTTP/1.1 200 OK\r\n\r\nhello";
        let mut buf = BytesMut::from(&raw[..]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
        let resp = codec.decode_eof(&mut buf).unwrap().unwrap();
        assert_eq!(resp.body, b"hello");
    }

    #[test]
    fn decode_headers_too_large() {
        let mut codec = Http1ClientCodec::new().max_headers_size(32);
        let mut buf = BytesMut::from(&b"HTTP/1.1 200 OK\r\nX-Large: aaaaaaaaaaaaaaa\r\n\r\n"[..]);
        let result = codec.decode(&mut buf);
        assert!(matches!(result, Err(HttpError::HeadersTooLarge)));
    }

    #[test]
    fn decode_body_too_large_content_length() {
        let mut codec = Http1ClientCodec::new().max_body_size(10);
        let mut buf = BytesMut::from(&b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n"[..]);
        let result = codec.decode(&mut buf);
        assert!(matches!(result, Err(HttpError::BodyTooLarge)));
    }

    #[test]
    fn decode_body_too_large_chunked() {
        let mut codec = Http1ClientCodec::new().max_body_size(10);
        // Chunked body with 20 bytes total (exceeds 10 byte limit)
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\
                    14\r\n01234567890123456789\r\n0\r\n\r\n";
        let mut buf = BytesMut::from(&raw[..]);
        let result = codec.decode(&mut buf);
        assert!(matches!(result, Err(HttpError::BodyTooLarge)));
    }

    #[test]
    fn decode_body_at_limit_succeeds() {
        let mut codec = Http1ClientCodec::new().max_body_size(5);
        let mut buf = BytesMut::from(&b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello"[..]);
        let resp = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(resp.body, b"hello");
    }

    #[test]
    fn reject_both_content_length_and_transfer_encoding() {
        let mut codec = Http1ClientCodec::new();
        let mut buf = BytesMut::from(
            &b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n"[..],
        );
        let result = codec.decode(&mut buf);
        assert!(matches!(result, Err(HttpError::AmbiguousBodyLength)));
    }

    #[test]
    fn reject_invalid_crlf_after_chunk() {
        let mut codec = Http1ClientCodec::new();
        // Invalid: missing proper CRLF after chunk data
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\
                    5\r\nhelloXX0\r\n\r\n";
        let mut buf = BytesMut::from(&raw[..]);
        let result = codec.decode(&mut buf);
        assert!(matches!(result, Err(HttpError::BadChunkedEncoding)));
    }
}
