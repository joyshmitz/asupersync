//! Debounce combinator for streams.
//!
//! The `Debounce` combinator suppresses rapid bursts of items, yielding
//! only the most recent item after a quiet period has elapsed.

use super::Stream;
use crate::time::Sleep;
use crate::types::Time;
use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

fn wall_clock_now() -> Time {
    crate::time::wall_now()
}

/// A stream that debounces items, emitting only after a quiet period.
///
/// Created by [`StreamExt::debounce`](super::StreamExt::debounce).
///
/// When the underlying stream produces an item, it is buffered. If no
/// new item arrives for `period`, the buffered item is yielded. Each
/// new item replaces the buffered value and resets the timer.
///
/// When the underlying stream ends, any buffered item is flushed
/// immediately.
///
/// # Note
///
/// By default this combinator uses the runtime wall clock via
/// [`crate::time::wall_now`], but tests and adapters can override that with
/// [`Debounce::with_time_getter`]. The executor still needs to re-poll after
/// the quiet period expires.
/// For proper async timer integration, consider pairing with a timeout
/// or interval-driven polling loop.
#[pin_project]
#[must_use = "streams do nothing unless polled"]
pub struct Debounce<S: Stream> {
    #[pin]
    stream: S,
    period: Duration,
    /// The most recently received item and when it was received.
    pending: Option<(S::Item, Time)>,
    /// Whether the underlying stream has ended.
    done: bool,
    /// Timer future for delayed wakeup (avoids spin-loop).
    timer: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    time_getter: fn() -> Time,
}

impl<S: Stream + std::fmt::Debug> std::fmt::Debug for Debounce<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Debounce")
            .field("stream", &self.stream)
            .field("period", &self.period)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl<S: Stream> Debounce<S> {
    /// Creates a new `Debounce` stream.
    pub(crate) fn new(stream: S, period: Duration) -> Self {
        Self::with_time_getter(stream, period, wall_clock_now)
    }

    /// Creates a new `Debounce` stream with a custom time source.
    pub fn with_time_getter(stream: S, period: Duration, time_getter: fn() -> Time) -> Self {
        Self {
            stream,
            period,
            pending: None,
            done: false,
            timer: None,
            time_getter,
        }
    }

    /// Returns a reference to the underlying stream.
    pub fn get_ref(&self) -> &S {
        &self.stream
    }

    /// Returns a mutable reference to the underlying stream.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.stream
    }

    /// Consumes the combinator, returning the underlying stream.
    pub fn into_inner(self) -> S {
        self.stream
    }

    /// Returns the configured time source.
    pub const fn time_getter(&self) -> fn() -> Time {
        self.time_getter
    }
}

impl<S: Stream> Stream for Debounce<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<S::Item>> {
        let mut this = self.project();

        // Drain all immediately available items from the underlying stream.
        let had_pending_before = this.pending.is_some();
        if !*this.done {
            loop {
                match this.stream.as_mut().poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        *this.pending = Some((item, (this.time_getter)()));
                        // New item arrived, reset the timer.
                        *this.timer = None;
                    }
                    Poll::Ready(None) => {
                        *this.done = true;
                        break;
                    }
                    Poll::Pending => break,
                }
            }
        }

        // Check if the buffered item's quiet period has elapsed.
        if let Some((_, received_at)) = this.pending.as_ref() {
            let now = (this.time_getter)();
            let elapsed = Duration::from_nanos(now.duration_since(*received_at));
            if *this.done || elapsed >= *this.period {
                *this.timer = None;
                let (item, _) = this.pending.take().unwrap();
                return Poll::Ready(Some(item));
            }
            // Set up a timer for the remaining quiet period.
            let remaining = this.period.saturating_sub(elapsed);
            if this.timer.is_none() || !had_pending_before {
                let remaining_nanos = remaining.as_nanos().min(u128::from(u64::MAX)) as u64;
                let deadline = now.saturating_add_nanos(remaining_nanos);
                *this.timer = Some(Box::pin(Sleep::with_time_getter(
                    deadline,
                    *this.time_getter,
                )));
            }
            // Poll the timer to register the waker for delayed wakeup.
            if let Some(ref mut timer) = *this.timer {
                if Pin::new(timer).poll(cx).is_ready() {
                    *this.timer = None;
                    let (item, _) = this.pending.take().unwrap();
                    return Poll::Ready(Some(item));
                }
            }
            return Poll::Pending;
        }

        if *this.done {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::iter;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::task::{Poll, Wake, Waker};

    static TEST_NOW_NANOS: AtomicU64 = AtomicU64::new(0);

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

    fn set_test_time(nanos: u64) {
        TEST_NOW_NANOS.store(nanos, Ordering::SeqCst);
    }

    fn test_time() -> Time {
        Time::from_nanos(TEST_NOW_NANOS.load(Ordering::SeqCst))
    }

    #[test]
    fn debounce_flushes_on_stream_end() {
        init_test("debounce_flushes_on_stream_end");
        // When the stream ends, the buffered item should be flushed immediately.
        let mut stream = Debounce::new(iter(vec![1, 2, 3]), Duration::from_secs(999));
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // All items arrive synchronously and stream ends.
        // The last item (3) should be flushed.
        let poll = Pin::new(&mut stream).poll_next(&mut cx);
        assert_eq!(poll, Poll::Ready(Some(3)));

        // Stream is done.
        let poll = Pin::new(&mut stream).poll_next(&mut cx);
        assert_eq!(poll, Poll::Ready(None));
        crate::test_complete!("debounce_flushes_on_stream_end");
    }

    #[test]
    fn debounce_zero_duration_passes_last() {
        init_test("debounce_zero_duration_passes_last");
        // With zero period, debounce should emit the last synchronously-available item.
        let mut stream = Debounce::new(iter(vec![10, 20, 30]), Duration::ZERO);
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let poll = Pin::new(&mut stream).poll_next(&mut cx);
        assert_eq!(poll, Poll::Ready(Some(30)));

        let poll = Pin::new(&mut stream).poll_next(&mut cx);
        assert_eq!(poll, Poll::Ready(None));
        crate::test_complete!("debounce_zero_duration_passes_last");
    }

    #[test]
    fn debounce_empty_stream() {
        init_test("debounce_empty_stream");
        let mut stream = Debounce::new(iter(Vec::<i32>::new()), Duration::from_millis(100));
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        assert_eq!(Pin::new(&mut stream).poll_next(&mut cx), Poll::Ready(None));
        crate::test_complete!("debounce_empty_stream");
    }

    #[test]
    fn debounce_single_item_flushes() {
        init_test("debounce_single_item_flushes");
        let mut stream = Debounce::new(iter(vec![42]), Duration::from_secs(10));
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Single item + stream end → immediate flush.
        assert_eq!(
            Pin::new(&mut stream).poll_next(&mut cx),
            Poll::Ready(Some(42))
        );
        assert_eq!(Pin::new(&mut stream).poll_next(&mut cx), Poll::Ready(None));
        crate::test_complete!("debounce_single_item_flushes");
    }

    #[test]
    fn debounce_with_elapsed_quiet_period() {
        init_test("debounce_with_elapsed_quiet_period");
        // Use a very short debounce period.
        let mut stream = Debounce::new(iter(vec![1, 2, 3]), Duration::from_millis(1));
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // All items arrive synchronously. Since the stream ends, the last
        // item is flushed regardless of debounce period.
        let poll = Pin::new(&mut stream).poll_next(&mut cx);
        assert_eq!(poll, Poll::Ready(Some(3)));
        crate::test_complete!("debounce_with_elapsed_quiet_period");
    }

    #[test]
    fn debounce_accessors() {
        init_test("debounce_accessors");
        set_test_time(17);
        let mut stream =
            Debounce::with_time_getter(iter(vec![1, 2]), Duration::from_millis(100), test_time);
        let _ref = stream.get_ref();
        let _mut = stream.get_mut();
        assert_eq!((stream.time_getter())().as_nanos(), 17);
        let inner = stream.into_inner();
        let mut inner = inner;
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(
            Pin::new(&mut inner).poll_next(&mut cx),
            Poll::Ready(Some(1))
        );
        crate::test_complete!("debounce_accessors");
    }

    #[test]
    fn debounce_debug() {
        let stream = Debounce::new(iter(vec![1, 2, 3]), Duration::from_millis(100));
        let dbg = format!("{stream:?}");
        assert!(dbg.contains("Debounce"));
    }

    #[derive(Debug)]
    struct PendingStream;

    impl Stream for PendingStream {
        type Item = i32;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Pending
        }
    }

    #[test]
    fn debounce_emits_immediately_when_timer_future_is_ready() {
        init_test("debounce_emits_immediately_when_timer_future_is_ready");
        set_test_time(0);
        let mut stream =
            Debounce::with_time_getter(PendingStream, Duration::from_mins(1), test_time);
        stream.pending = Some((7, Time::from_nanos(0)));
        stream.timer = Some(Box::pin(std::future::ready(())));

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(
            Pin::new(&mut stream).poll_next(&mut cx),
            Poll::Ready(Some(7))
        );
        assert!(stream.pending.is_none(), "pending item should be drained");
        assert!(stream.timer.is_none(), "timer should be cleared after emit");
        crate::test_complete!("debounce_emits_immediately_when_timer_future_is_ready");
    }

    #[test]
    fn debounce_respects_custom_time_getter_without_sleeping() {
        init_test("debounce_respects_custom_time_getter_without_sleeping");
        set_test_time(0);
        let mut stream =
            Debounce::with_time_getter(PendingStream, Duration::from_secs(5), test_time);
        stream.pending = Some((11, Time::from_nanos(0)));

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        assert_eq!(Pin::new(&mut stream).poll_next(&mut cx), Poll::Pending);
        assert!(stream.pending.is_some(), "item should still be buffered");
        assert!(stream.timer.is_some(), "timer should be armed");

        set_test_time(Duration::from_secs(5).as_nanos() as u64);

        assert_eq!(
            Pin::new(&mut stream).poll_next(&mut cx),
            Poll::Ready(Some(11))
        );
        assert!(stream.pending.is_none(), "pending item should be emitted");
        assert!(stream.timer.is_none(), "timer should be cleared after emit");
        crate::test_complete!("debounce_respects_custom_time_getter_without_sleeping");
    }
}
