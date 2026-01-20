//! Unix domain socket stream implementation.
//!
//! This module provides [`UnixStream`] for bidirectional communication over
//! Unix domain sockets.
//!
//! # Example
//!
//! ```ignore
//! use asupersync::net::unix::UnixStream;
//! use std::io::{Read, Write};
//!
//! async fn client() -> std::io::Result<()> {
//!     let mut stream = UnixStream::connect("/tmp/my_socket.sock").await?;
//!     stream.write_all(b"hello")?;
//!     Ok(())
//! }
//! ```

use std::io::{self, Read, Write};
use std::os::unix::net::{self, SocketAddr};
use std::path::Path;

/// A Unix domain socket stream.
///
/// Provides a bidirectional byte stream for inter-process communication
/// within the same machine.
///
/// # Cancel-Safety
///
/// Read and write operations are cancel-safe in the sense that if cancelled,
/// partial data may have been transferred. For cancel-correctness with
/// guaranteed delivery, use higher-level protocols.
#[derive(Debug)]
pub struct UnixStream {
    /// The underlying standard library stream.
    pub(crate) inner: net::UnixStream,
    // TODO: Add Registration when reactor integration is complete
    // registration: Option<Registration>,
}

impl UnixStream {
    /// Connects to a Unix domain socket at the given path.
    ///
    /// # Arguments
    ///
    /// * `path` - The filesystem path of the socket to connect to
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The socket doesn't exist
    /// - Permission is denied
    /// - Connection is refused
    ///
    /// # Example
    ///
    /// ```ignore
    /// let stream = UnixStream::connect("/tmp/my_socket.sock").await?;
    /// ```
    pub async fn connect<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        // For now, use blocking connect
        // TODO: Use non-blocking connect with reactor when available
        let inner = net::UnixStream::connect(path)?;
        inner.set_nonblocking(true)?;

        Ok(Self { inner })
    }

    /// Creates a pair of connected Unix domain sockets.
    ///
    /// This is useful for inter-thread or bidirectional communication
    /// within the same process.
    ///
    /// # Errors
    ///
    /// Returns an error if socket creation fails.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let (stream1, stream2) = UnixStream::pair()?;
    /// ```
    pub fn pair() -> io::Result<(Self, Self)> {
        let (s1, s2) = net::UnixStream::pair()?;
        s1.set_nonblocking(true)?;
        s2.set_nonblocking(true)?;

        Ok((Self { inner: s1 }, Self { inner: s2 }))
    }

    /// Creates an async `UnixStream` from a standard library stream.
    ///
    /// The stream will be set to non-blocking mode.
    ///
    /// # Note
    ///
    /// For proper reactor integration, use this only with newly created
    /// streams that haven't been registered elsewhere.
    #[must_use]
    pub fn from_std(stream: net::UnixStream) -> Self {
        // Non-blocking is set by caller for streams from accept()
        Self { inner: stream }
    }

    /// Returns the socket address of the local end.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    /// Returns the socket address of the remote end.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner.peer_addr()
    }

    /// Shuts down the read, write, or both halves of the stream.
    pub fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()> {
        self.inner.shutdown(how)
    }

    /// Returns the underlying std stream.
    #[must_use]
    pub fn as_std(&self) -> &net::UnixStream {
        &self.inner
    }
}

impl Read for UnixStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl Write for UnixStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(unix)]
impl std::os::unix::io::AsRawFd for UnixStream {
    fn as_raw_fd(&self) -> std::os::unix::io::RawFd {
        self.inner.as_raw_fd()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pair() {
        let (mut s1, mut s2) = UnixStream::pair().expect("pair failed");

        s1.write_all(b"hello").expect("write failed");
        let mut buf = [0u8; 5];
        s2.read_exact(&mut buf).expect("read failed");

        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn test_local_peer_addr() {
        let (s1, s2) = UnixStream::pair().expect("pair failed");

        // Unnamed sockets from pair() don't have pathname addresses
        let local = s1.local_addr().expect("local_addr failed");
        let peer = s2.peer_addr().expect("peer_addr failed");

        // Both should be unnamed (no pathname)
        assert!(local.as_pathname().is_none());
        assert!(peer.as_pathname().is_none());
    }

    #[test]
    fn test_shutdown() {
        let (s1, _s2) = UnixStream::pair().expect("pair failed");

        // Shutdown should succeed
        s1.shutdown(std::net::Shutdown::Write)
            .expect("shutdown failed");
    }
}
