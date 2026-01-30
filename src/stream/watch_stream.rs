//! Stream adapter for watch receivers.

use crate::channel::watch;
use crate::cx::Cx;
use crate::stream::Stream;
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

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // First poll: return current value immediately
        if !self.has_seen_initial {
            self.has_seen_initial = true;
            return Poll::Ready(Some(self.inner.borrow_and_clone()));
        }

        // Wait for changes
        // changed() blocks in Phase 0
        let cx = self.cx.clone();
        let inner = &mut self.inner;
        match inner.changed(&cx) {
            Ok(()) => Poll::Ready(Some(inner.borrow_and_clone())),
            Err(watch::RecvError::Closed | watch::RecvError::Cancelled) => Poll::Ready(None),
        }
    }
}
