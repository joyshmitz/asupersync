//! Async networking primitives.
//!
//! Phase 0 provides synchronous `std::net` wrappers exposed through async APIs.
//! This keeps the public surface stable while the runtime lacks an I/O reactor.
//! Later phases will replace the blocking internals with true async drivers.
//!
//! # Cancel Safety
//!
//! - `bind`, `connect`, `accept`: cancel-safe (no partial state committed).
//! - `poll_read`: cancel-safe (partial data is discarded by caller).
//! - `poll_write`: cancel-safe (partial writes are acceptable).
//! - `WritePermit`: use for cancel-safe write commit.

use crate::io::{AsyncRead, AsyncWrite, ReadBuf, WritePermit};
use crate::stream::Stream;
use std::io;
use std::net::{
    Shutdown, SocketAddr, TcpListener as StdTcpListener, TcpStream as StdTcpStream, ToSocketAddrs,
};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

mod udp;

pub use udp::{RecvStream, SendSink, UdpSocket};

/// A TCP listener.
#[derive(Debug)]
pub struct TcpListener {
    inner: StdTcpListener,
}

impl TcpListener {
    /// Bind a TCP listener to the given address.
    pub async fn bind(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let listener = StdTcpListener::bind(addr)?;
        Ok(Self { inner: listener })
    }

    /// Accept a new incoming connection.
    pub async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        let (stream, addr) = self.inner.accept()?;
        Ok((TcpStream::from(stream), addr))
    }

    /// Returns the local address of this listener.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    /// Sets the TTL for this listener.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.inner.set_ttl(ttl)
    }

    /// Returns a stream of incoming connections.
    #[must_use]
    pub fn incoming(&self) -> Incoming<'_> {
        Incoming { listener: self }
    }

    /// Consumes this wrapper and returns the underlying std listener.
    #[must_use]
    pub fn into_std(self) -> StdTcpListener {
        self.inner
    }
}

impl From<StdTcpListener> for TcpListener {
    fn from(listener: StdTcpListener) -> Self {
        Self { inner: listener }
    }
}

/// A TCP stream.
#[derive(Debug, Clone)]
pub struct TcpStream {
    inner: Arc<StdTcpStream>,
}

impl TcpStream {
    /// Connect to a remote address.
    pub async fn connect(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let stream = StdTcpStream::connect(addr)?;
        Ok(Self::from(stream))
    }

    /// Connect with a timeout.
    pub async fn connect_timeout(addr: SocketAddr, timeout: Duration) -> io::Result<Self> {
        let stream = StdTcpStream::connect_timeout(&addr, timeout)?;
        Ok(Self::from(stream))
    }

    /// Returns the peer address for this connection.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner.peer_addr()
    }

    /// Returns the local address for this connection.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    /// Shut down the read, write, or both halves of this connection.
    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        self.inner.shutdown(how)
    }

    /// Sets the TCP_NODELAY option on this socket.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.inner.set_nodelay(nodelay)
    }

    /// Sets the keepalive option for this socket.
    pub fn set_keepalive(&self, keepalive: Option<Duration>) -> io::Result<()> {
        (&*self.inner).set_keepalive(keepalive)
    }

    /// Returns a cancel-safe write permit for this stream.
    #[must_use]
    pub fn write_permit(&mut self) -> WritePermit<'_, TcpStream> {
        WritePermit::new(self)
    }

    /// Split this stream into borrowed read/write halves.
    #[must_use]
    pub fn split(&self) -> (ReadHalf<'_>, WriteHalf<'_>) {
        (
            ReadHalf {
                inner: self.inner.as_ref(),
            },
            WriteHalf {
                inner: self.inner.as_ref(),
            },
        )
    }

    /// Split this stream into owned read/write halves.
    #[must_use]
    pub fn into_split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
        let shared = self.inner;
        (
            OwnedReadHalf {
                inner: Arc::clone(&shared),
            },
            OwnedWriteHalf { inner: shared },
        )
    }

    /// Consumes this wrapper and returns the underlying std stream if unique.
    ///
    /// If the stream is shared (because it was cloned or split), this returns
    /// a cloned std stream instead.
    pub fn into_std(self) -> io::Result<StdTcpStream> {
        match Arc::try_unwrap(self.inner) {
            Ok(stream) => Ok(stream),
            Err(shared) => shared.try_clone(),
        }
    }
}

impl From<StdTcpStream> for TcpStream {
    fn from(stream: StdTcpStream) -> Self {
        Self {
            inner: Arc::new(stream),
        }
    }
}

impl AsyncRead for TcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        use std::io::Read as _;

        let mut stream = self.inner.as_ref();
        let n = stream.read(buf.unfilled())?;
        buf.advance(n);
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        use std::io::Write as _;

        let mut stream = self.inner.as_ref();
        let n = stream.write(buf)?;
        Poll::Ready(Ok(n))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        use std::io::Write as _;

        let mut stream = self.inner.as_ref();
        let n = stream.write_vectored(bufs)?;
        Poll::Ready(Ok(n))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        use std::io::Write as _;

        let mut stream = self.inner.as_ref();
        stream.flush()?;
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.shutdown(Shutdown::Write)?;
        Poll::Ready(Ok(()))
    }
}

/// A TCP socket used for configuring options before connect/listen.
#[derive(Debug)]
pub struct TcpSocket {
    state: Mutex<TcpSocketState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TcpSocketFamily {
    V4,
    V6,
}

#[derive(Debug)]
struct TcpSocketState {
    family: TcpSocketFamily,
    bound: Option<SocketAddr>,
    reuseaddr: bool,
    reuseport: bool,
}

impl TcpSocket {
    /// Creates a new IPv4 TCP socket.
    pub fn new_v4() -> io::Result<Self> {
        Ok(Self {
            state: Mutex::new(TcpSocketState {
                family: TcpSocketFamily::V4,
                bound: None,
                reuseaddr: false,
                reuseport: false,
            }),
        })
    }

    /// Creates a new IPv6 TCP socket.
    pub fn new_v6() -> io::Result<Self> {
        Ok(Self {
            state: Mutex::new(TcpSocketState {
                family: TcpSocketFamily::V6,
                bound: None,
                reuseaddr: false,
                reuseport: false,
            }),
        })
    }

    /// Sets the SO_REUSEADDR option on this socket.
    pub fn set_reuseaddr(&self, reuseaddr: bool) -> io::Result<()> {
        let mut state = self.state.lock().expect("tcp socket lock poisoned");
        state.reuseaddr = reuseaddr;
        Ok(())
    }

    /// Sets the SO_REUSEPORT option on this socket (Unix only).
    #[cfg(unix)]
    pub fn set_reuseport(&self, reuseport: bool) -> io::Result<()> {
        let mut state = self.state.lock().expect("tcp socket lock poisoned");
        state.reuseport = reuseport;
        Ok(())
    }

    /// Binds this socket to the given local address.
    pub fn bind(&self, addr: SocketAddr) -> io::Result<()> {
        let mut state = self.state.lock().expect("tcp socket lock poisoned");
        if !family_matches(state.family, addr) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "address family does not match socket",
            ));
        }
        state.bound = Some(addr);
        Ok(())
    }

    /// Starts listening, returning a TCP listener.
    pub fn listen(self, _backlog: u32) -> io::Result<TcpListener> {
        let state = self
            .state
            .into_inner()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.reuseaddr || state.reuseport {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "SO_REUSEADDR/SO_REUSEPORT not supported in Phase 0",
            ));
        }
        let addr = state.bound.ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "socket is not bound")
        })?;
        let listener = StdTcpListener::bind(addr)?;
        Ok(TcpListener::from(listener))
    }

    /// Connects this socket, returning a TCP stream.
    pub async fn connect(self, addr: SocketAddr) -> io::Result<TcpStream> {
        let state = self
            .state
            .into_inner()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.bound.is_some() || state.reuseaddr || state.reuseport {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "TcpSocket configuration before connect is not supported in Phase 0",
            ));
        }
        if !family_matches(state.family, addr) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "address family does not match socket",
            ));
        }
        let stream = StdTcpStream::connect(addr)?;
        Ok(TcpStream::from(stream))
    }
}

fn family_matches(family: TcpSocketFamily, addr: SocketAddr) -> bool {
    match family {
        TcpSocketFamily::V4 => addr.is_ipv4(),
        TcpSocketFamily::V6 => addr.is_ipv6(),
    }
}

/// Stream of incoming TCP connections.
#[derive(Debug)]
pub struct Incoming<'a> {
    listener: &'a TcpListener,
}

impl Stream for Incoming<'_> {
    type Item = io::Result<TcpStream>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.listener.inner.accept() {
            Ok((stream, _addr)) => Poll::Ready(Some(Ok(TcpStream::from(stream)))),
            Err(err) => Poll::Ready(Some(Err(err))),
        }
    }
}

/// Borrowed read half of a TCP stream.
#[derive(Debug, Clone, Copy)]
pub struct ReadHalf<'a> {
    inner: &'a StdTcpStream,
}

impl AsyncRead for ReadHalf<'_> {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        use std::io::Read as _;

        let mut stream = self.inner;
        let n = stream.read(buf.unfilled())?;
        buf.advance(n);
        Poll::Ready(Ok(()))
    }
}

/// Borrowed write half of a TCP stream.
#[derive(Debug, Clone, Copy)]
pub struct WriteHalf<'a> {
    inner: &'a StdTcpStream,
}

impl AsyncWrite for WriteHalf<'_> {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        use std::io::Write as _;

        let mut stream = self.inner;
        let n = stream.write(buf)?;
        Poll::Ready(Ok(n))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        use std::io::Write as _;

        let mut stream = self.inner;
        let n = stream.write_vectored(bufs)?;
        Poll::Ready(Ok(n))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        use std::io::Write as _;

        let mut stream = self.inner;
        stream.flush()?;
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.shutdown(Shutdown::Write)?;
        Poll::Ready(Ok(()))
    }
}

/// Owned read half of a TCP stream.
#[derive(Debug, Clone)]
pub struct OwnedReadHalf {
    inner: Arc<StdTcpStream>,
}

impl AsyncRead for OwnedReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        use std::io::Read as _;

        let mut stream = self.inner.as_ref();
        let n = stream.read(buf.unfilled())?;
        buf.advance(n);
        Poll::Ready(Ok(()))
    }
}

/// Owned write half of a TCP stream.
#[derive(Debug, Clone)]
pub struct OwnedWriteHalf {
    inner: Arc<StdTcpStream>,
}

impl AsyncWrite for OwnedWriteHalf {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        use std::io::Write as _;

        let mut stream = self.inner.as_ref();
        let n = stream.write(buf)?;
        Poll::Ready(Ok(n))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        use std::io::Write as _;

        let mut stream = self.inner.as_ref();
        let n = stream.write_vectored(bufs)?;
        Poll::Ready(Ok(n))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        use std::io::Write as _;

        let mut stream = self.inner.as_ref();
        stream.flush()?;
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.shutdown(Shutdown::Write)?;
        Poll::Ready(Ok(()))
    }
}
