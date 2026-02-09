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
    terminated: bool,
}

impl<T: Clone> WatchStream<T> {
    /// Create from watch receiver.
    #[must_use]
    pub fn new(cx: Cx, recv: watch::Receiver<T>) -> Self {
        Self {
            inner: recv,
            cx,
            has_seen_initial: false,
            terminated: false,
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
        if this.terminated {
            return Poll::Ready(None);
        }

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
            Poll::Ready(Err(_)) => {
                this.terminated = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::task::{Context, Wake, Waker};

    struct NoopWaker;

    impl Wake for NoopWaker {
        fn wake(self: Arc<Self>) {}
    }

    fn noop_waker() -> Waker {
        Waker::from(Arc::new(NoopWaker))
    }

    fn init_test(name: &str) {
        crate::test_utils::init_test_logging();
        crate::test_phase!(name);
    }

    #[test]
    fn watch_stream_none_is_terminal_after_cancel() {
        init_test("watch_stream_none_is_terminal_after_cancel");
        let cx: Cx = Cx::for_testing();
        cx.set_cancel_requested(true);
        let (tx, rx) = watch::channel(0);
        let mut stream = WatchStream::from_changes(cx.clone(), rx);
        let waker = noop_waker();
        let mut task_cx = Context::from_waker(&waker);

        let poll = Pin::new(&mut stream).poll_next(&mut task_cx);
        let first_none = matches!(poll, Poll::Ready(None));
        crate::assert_with_log!(first_none, "first poll none", true, first_none);

        cx.set_cancel_requested(false);
        tx.send(1).expect("send after cancel clear");

        let poll = Pin::new(&mut stream).poll_next(&mut task_cx);
        let still_none = matches!(poll, Poll::Ready(None));
        crate::assert_with_log!(still_none, "stream remains terminated", true, still_none);
        crate::test_complete!("watch_stream_none_is_terminal_after_cancel");
    }
}
