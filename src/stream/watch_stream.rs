//! Stream adapter for watch receivers.

use crate::channel::watch;
use crate::cx::Cx;
use crate::stream::Stream;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Stream that yields when watch value changes.
#[derive(Debug)]
pub struct WatchStream<T> {
    inner: watch::Receiver<T>,
    cx: Cx,
    has_seen_initial: bool,
}

impl<T: Clone> WatchStream<T> {
    /// Create from watch receiver.
    #[must_use]
    pub fn new(cx: Cx, recv: watch::Receiver<T>) -> Self {
        Self {
            inner: recv,
            cx,
            has_seen_initial: false,
        }
    }

    /// Create, skipping the initial value.
    #[must_use]
    pub fn from_changes(cx: Cx, recv: watch::Receiver<T>) -> Self {
        let mut stream = Self::new(cx, recv);
        stream.has_seen_initial = true;
        stream
    }
}

impl<T: Clone + Send + Sync> Stream for WatchStream<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // First poll: return current value immediately
        if !this.has_seen_initial {
            this.has_seen_initial = true;
            return Poll::Ready(Some(this.inner.borrow_and_clone()));
        }

        // Poll the changed future (non-blocking, waker-based)
        let runtime_cx = this.cx.clone();
        let result = {
            let mut future = this.inner.changed(&runtime_cx);
            Pin::new(&mut future).poll(context)
        };
        match result {
            Poll::Ready(Ok(())) => Poll::Ready(Some(this.inner.borrow_and_clone())),
            Poll::Ready(Err(_)) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
