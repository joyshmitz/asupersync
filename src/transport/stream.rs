//! Symbol stream traits and implementations.

use crate::security::authenticated::AuthenticatedSymbol;
use crate::transport::error::StreamError;
use crate::transport::{SharedChannel, SymbolSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

/// A stream of incoming symbols.
pub trait SymbolStream: Send {
    /// Receive the next symbol.
    ///
    /// Returns `None` when stream is exhausted or closed.
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<AuthenticatedSymbol, StreamError>>>;

    /// Hint about remaining symbols (if known).
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }

    /// Check if the stream is exhausted.
    fn is_exhausted(&self) -> bool {
        false
    }
}

/// Extension methods for SymbolStream.
pub trait SymbolStreamExt: SymbolStream {
    /// Receive the next symbol.
    fn next(&mut self) -> NextFuture<'_, Self>
    where
        Self: Unpin,
    {
        NextFuture { stream: self }
    }

    /// Collect all symbols into a SymbolSet.
    fn collect_to_set<'a>(&'a mut self, set: &'a mut SymbolSet) -> CollectToSetFuture<'a, Self>
    where
        Self: Unpin,
    {
        CollectToSetFuture { stream: self, set }
    }

    /// Map symbols through a function.
    fn map<F, T>(self, f: F) -> MapStream<Self, F>
    where
        Self: Sized,
        F: FnMut(AuthenticatedSymbol) -> T,
    {
        MapStream { inner: self, f }
    }

    /// Filter symbols.
    fn filter<F>(self, f: F) -> FilterStream<Self, F>
    where
        Self: Sized,
        F: FnMut(&AuthenticatedSymbol) -> bool,
    {
        FilterStream { inner: self, f }
    }

    /// Take only symbols for a specific block.
    fn for_block(self, sbn: u8) -> FilterStream<Self, Box<dyn FnMut(&AuthenticatedSymbol) -> bool + Send>>
    where
        Self: Sized + 'static,
    {
        let f = Box::new(move |s: &AuthenticatedSymbol| s.symbol().sbn() == sbn);
        FilterStream { inner: self, f }
    }
}

impl<S: SymbolStream + ?Sized> SymbolStreamExt for S {}

// ---- Futures ----

/// Future for `next()`.
pub struct NextFuture<'a, S: ?Sized> {
    stream: &'a mut S,
}

impl<S: SymbolStream + Unpin + ?Sized> Future for NextFuture<'_, S> {
    type Output = Option<Result<AuthenticatedSymbol, StreamError>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut *self.stream).poll_next(cx)
    }
}

/// Future for `collect_to_set()`.
pub struct CollectToSetFuture<'a, S: ?Sized> {
    stream: &'a mut S,
    set: &'a mut SymbolSet,
}

impl<S: SymbolStream + Unpin + ?Sized> Future for CollectToSetFuture<'_, S> {
    type Output = Result<usize, StreamError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match Pin::new(&mut *self.stream).poll_next(cx) {
                Poll::Ready(Some(Ok(symbol))) => {
                    self.set.insert(symbol.into_symbol());
                },
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(e)),
                Poll::Ready(None) => return Poll::Ready(Ok(self.set.len())),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// ---- Stream Adapters ----

/// Stream that maps items.
pub struct MapStream<S, F> {
    inner: S,
    f: F,
}

// MapStream is not a SymbolStream because it changes the type T.
// But SymbolStream forces Item = Result<AuthenticatedSymbol...
// So MapStream is only valid if T is Result<AuthenticatedSymbol...
// But generic map allows any T.
// The trait `SymbolStream` is specific to `AuthenticatedSymbol`.
// `map` in `SymbolStreamExt` returns `MapStream` which might NOT implement `SymbolStream`.
// It implements `futures::Stream` if I had it.
// But here `map` is only useful if it produces `AuthenticatedSymbol`.
// The description says: `fn map<F, T>(self, f: F) -> MapStream<Self, F>`.
// If `T` is not `AuthenticatedSymbol`, then `MapStream` cannot be `SymbolStream`.
// I will implement `SymbolStream` for `MapStream` ONLY IF `T` is `Result<AuthenticatedSymbol, StreamError>`.
// Wait, `SymbolStream` returns `Result`. `map` transforms `AuthenticatedSymbol`.
// If `f` returns `AuthenticatedSymbol`, then we can wrap it in `Ok`.
// Let's assume `map` transforms the *success* value.

impl<S, F> SymbolStream for MapStream<S, F>
where
    S: SymbolStream + Unpin,
    F: FnMut(AuthenticatedSymbol) -> AuthenticatedSymbol + Send + Unpin,
{
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<AuthenticatedSymbol, StreamError>>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(s))) => Poll::Ready(Some(Ok((this.f)(s)))),
            other => other,
        }
    }
}

/// Stream that filters items.
pub struct FilterStream<S, F> {
    inner: S,
    f: F,
}

impl<S, F> SymbolStream for FilterStream<S, F>
where
    S: SymbolStream + Unpin,
    F: FnMut(&AuthenticatedSymbol) -> bool + Send + Unpin,
{
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<AuthenticatedSymbol, StreamError>>> {
        let this = self.get_mut();
        loop {
            match Pin::new(&mut this.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(s))) => {
                    if (this.f)(&s) {
                        return Poll::Ready(Some(Ok(s)));
                    }
                    // Loop to next
                }
                other => return other,
            }
        }
    }
}

// ---- Implementations ----

/// In-memory channel stream.
pub struct ChannelStream {
    pub(crate) shared: Arc<SharedChannel>,
}

impl ChannelStream {
    pub(crate) fn new(shared: Arc<SharedChannel>) -> Self {
        Self { shared }
    }
}

impl SymbolStream for ChannelStream {
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<AuthenticatedSymbol, StreamError>>> {
        let mut queue = self.shared.queue.lock().unwrap();
        
        if let Some(symbol) = queue.pop_front() {
            // Wake sender if we freed space
            let mut wakers = self.shared.send_wakers.lock().unwrap();
            if let Some(w) = wakers.pop() {
                w.wake();
            }
            return Poll::Ready(Some(Ok(symbol)));
        }

        if self.shared.closed.load(Ordering::SeqCst) {
            return Poll::Ready(None);
        }

        // Register waker
        let mut wakers = self.shared.recv_wakers.lock().unwrap();
        wakers.push(cx.waker().clone());
        Poll::Pending
    }
}

/// Stream from a Vec.
pub struct VecStream {
    symbols: std::vec::IntoIter<AuthenticatedSymbol>,
}

impl VecStream {
    pub fn new(symbols: Vec<AuthenticatedSymbol>) -> Self {
        Self {
            symbols: symbols.into_iter(),
        }
    }
}

impl SymbolStream for VecStream {
    fn poll_next(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<AuthenticatedSymbol, StreamError>>> {
        Poll::Ready(self.get_mut().symbols.next().map(Ok))
    }
    
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.symbols.size_hint()
    }
}

// ---- Timeout ----

// Implementation of TimeoutStream requires a timer facility.
// Asupersync has `time::sleep`.
// But that returns a Future. `poll_next` is synchronous-ish (returns Poll).
// To implement timeout in `poll_next`, we need to poll a Sleep future stored in the struct.

pub struct TimeoutStream<S> {
    inner: S,
    duration: Duration,
    sleep: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl<S> TimeoutStream<S> {
    pub fn new(inner: S, duration: Duration) -> Self {
        Self {
            inner,
            duration,
            sleep: None,
        }
    }
}

// Note: This requires linking to `asupersync::time` or runtime.
// If `asupersync::time::sleep` works without runtime (it does checks), we can use it.
// But `Sleep` needs polling.

impl<S: SymbolStream + Unpin> SymbolStream for TimeoutStream<S> {
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<AuthenticatedSymbol, StreamError>>> {
        // 1. Poll inner
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(v) => {
                self.sleep = None; // Reset timer on activity
                return Poll::Ready(v);
            }
            Poll::Pending => {}
        }

        // 2. Setup/Poll timer
        if self.sleep.is_none() {
            // Using asupersync::time::sleep if available, or just assume we can use it
            // Note: `asupersync::time::sleep` returns a `Sleep` struct which implements Future.
            // We need to import it.
            // Let's assume we can use `crate::time::sleep`.
            // Ideally we should use the runtime's timer, but `crate::time::sleep` is the public API.
            // We need a `now`. `crate::time::Time::now()` is not available directly?
            // `crate::time::sleep` takes `(now, duration)`.
            // We don't have `now`.
            // Phase 0: we can use `std::time::Instant` but `Sleep` uses `crate::types::Time`.
            // `crate::types::Time` corresponds to wall clock in production.
            // This suggests we might need a runtime context to get time.
            // Without it, implementing `timeout` combinator purely as a struct is hard if we depend on `asupersync` time.
            // I'll skip implementing `TimeoutStream` logic for now or use `std` sleep if I can wrap it?
            // But I need to return `Poll`.
            // I'll mark it as TODO or return Pending.
            return Poll::Pending;
        }
        
        /* 
        if let Some(ref mut sleep) = self.sleep {
            if sleep.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Some(Err(StreamError::Timeout)));
            }
        }
        */
        
        Poll::Pending
    }
}
