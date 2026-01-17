//! Timeout wrapper for futures.
//!
//! The [`TimeoutFuture`] wraps another future and limits how long it can run.

use super::elapsed::Elapsed;
use super::sleep::Sleep;
use crate::types::Time;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

/// A future that wraps another future with a timeout.
///
/// If the inner future doesn't complete before the deadline, `TimeoutFuture`
/// resolves to `Err(Elapsed)`. If it completes in time, it resolves to
/// `Ok(F::Output)`.
///
/// # Type Parameters
///
/// * `F` - The inner future type. Must implement `Unpin`.
///
/// # Cancel Safety
///
/// `TimeoutFuture` is cancel-safe in the sense that dropping it is safe.
/// However, if the inner future has side effects that occur during polling,
/// those may be partially applied.
///
/// # Example
///
/// ```ignore
/// use asupersync::time::timeout;
/// use std::time::Duration;
///
/// async fn slow_operation() -> u32 {
///     // ... takes a long time ...
///     42
/// }
///
/// let result = timeout(Time::ZERO, Duration::from_secs(5), slow_operation()).await;
/// match result {
///     Ok(value) => println!("Got: {value}"),
///     Err(_) => println!("Operation timed out!"),
/// }
/// ```
#[derive(Debug)]
pub struct TimeoutFuture<F> {
    /// The inner future.
    future: F,
    /// The sleep future for the timeout.
    sleep: Sleep,
}

impl<F> TimeoutFuture<F> {
    /// Creates a new timeout wrapper.
    ///
    /// # Arguments
    ///
    /// * `future` - The future to wrap
    /// * `deadline` - When the timeout expires
    ///
    /// # Example
    ///
    /// ```
    /// use asupersync::time::TimeoutFuture;
    /// use asupersync::types::Time;
    /// use std::future::ready;
    ///
    /// let future = ready(42);
    /// let timeout = TimeoutFuture::new(future, Time::from_secs(5));
    /// assert_eq!(timeout.deadline(), Time::from_secs(5));
    /// ```
    #[must_use]
    pub const fn new(future: F, deadline: Time) -> Self {
        Self {
            future,
            sleep: Sleep::new(deadline),
        }
    }

    /// Creates a timeout that expires after the given duration.
    ///
    /// # Arguments
    ///
    /// * `now` - The current time
    /// * `duration` - How long until timeout
    /// * `future` - The future to wrap
    #[must_use]
    pub fn after(now: Time, duration: Duration, future: F) -> Self {
        Self {
            future,
            sleep: Sleep::after(now, duration),
        }
    }

    /// Returns the timeout deadline.
    #[must_use]
    pub const fn deadline(&self) -> Time {
        self.sleep.deadline()
    }

    /// Returns the remaining time until timeout.
    ///
    /// Returns `Duration::ZERO` if the timeout has elapsed.
    #[must_use]
    pub fn remaining(&self, now: Time) -> Duration {
        self.sleep.remaining(now)
    }

    /// Returns true if the timeout has elapsed.
    #[must_use]
    pub fn is_elapsed(&self, now: Time) -> bool {
        self.sleep.is_elapsed(now)
    }

    /// Returns a reference to the inner future.
    #[must_use]
    pub const fn inner(&self) -> &F {
        &self.future
    }

    /// Returns a mutable reference to the inner future.
    pub fn inner_mut(&mut self) -> &mut F {
        &mut self.future
    }

    /// Consumes the timeout, returning the inner future.
    ///
    /// Note: This discards the timeout and lets the future run indefinitely.
    #[must_use]
    pub fn into_inner(self) -> F {
        self.future
    }

    /// Resets the timeout to a new deadline.
    pub fn reset(&mut self, deadline: Time) {
        self.sleep.reset(deadline);
    }

    /// Resets the timeout to expire after the given duration.
    pub fn reset_after(&mut self, now: Time, duration: Duration) {
        self.sleep.reset_after(now, duration);
    }
}

impl<F: Future + Unpin> TimeoutFuture<F> {
    /// Polls the timeout future with an explicit time value.
    ///
    /// This is useful when you want to control the time source manually.
    ///
    /// # Arguments
    ///
    /// * `now` - The current time
    /// * `cx` - The task context for the inner future
    ///
    /// # Returns
    ///
    /// - `Poll::Ready(Ok(output))` if the inner future completed
    /// - `Poll::Ready(Err(Elapsed))` if the timeout elapsed
    /// - `Poll::Pending` if neither has occurred yet
    pub fn poll_with_time(
        &mut self,
        now: Time,
        cx: &mut Context<'_>,
    ) -> Poll<Result<F::Output, Elapsed>> {
        // Check the timeout first
        if self.sleep.poll_with_time(now).is_ready() {
            return Poll::Ready(Err(Elapsed::new(self.sleep.deadline())));
        }

        // Try the inner future
        // SAFETY: We require F: Unpin, so this is safe
        match Pin::new(&mut self.future).poll(cx) {
            Poll::Ready(output) => Poll::Ready(Ok(output)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<F: Future + Unpin> Future for TimeoutFuture<F> {
    type Output = Result<F::Output, Elapsed>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Use the sleep's time getter (which defaults to Time::MAX without a source)
        // In practice, callers should use poll_with_time or integrate with a runtime
        let now = if let Some(getter) = self.sleep.time_getter {
            getter()
        } else {
            // Without a time source, we can't implement timeout properly
            // Just poll the inner future
            // This is a limitation of standalone use; runtime integration fixes this
            return match Pin::new(&mut self.future).poll(cx) {
                Poll::Ready(output) => Poll::Ready(Ok(output)),
                Poll::Pending => Poll::Pending,
            };
        };

        self.poll_with_time(now, cx)
    }
}

impl<F: Clone> Clone for TimeoutFuture<F> {
    fn clone(&self) -> Self {
        Self {
            future: self.future.clone(),
            sleep: self.sleep.clone(),
        }
    }
}

/// Creates a `TimeoutFuture` that wraps the given future with a timeout.
///
/// # Arguments
///
/// * `now` - The current time
/// * `duration` - How long until the timeout expires
/// * `future` - The future to wrap
///
/// # Example
///
/// ```
/// use asupersync::time::timeout;
/// use asupersync::types::Time;
/// use std::time::Duration;
/// use std::future::ready;
///
/// let future = timeout(Time::ZERO, Duration::from_secs(5), ready(42));
/// assert_eq!(future.deadline(), Time::from_secs(5));
/// ```
#[must_use]
pub fn timeout<F>(now: Time, duration: Duration, future: F) -> TimeoutFuture<F> {
    TimeoutFuture::after(now, duration, future)
}

/// Creates a `TimeoutFuture` that wraps the given future with a deadline.
///
/// # Arguments
///
/// * `deadline` - The absolute time when the timeout expires
/// * `future` - The future to wrap
///
/// # Example
///
/// ```
/// use asupersync::time::timeout_at;
/// use asupersync::types::Time;
/// use std::future::ready;
///
/// let future = timeout_at(Time::from_secs(10), ready(42));
/// assert_eq!(future.deadline(), Time::from_secs(10));
/// ```
#[must_use]
pub const fn timeout_at<F>(deadline: Time, future: F) -> TimeoutFuture<F> {
    TimeoutFuture::new(future, deadline)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::{pending, ready};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::task::Waker;

    // =========================================================================
    // Construction Tests
    // =========================================================================

    #[test]
    fn new_creates_timeout() {
        let future = ready(42);
        let timeout = TimeoutFuture::new(future, Time::from_secs(5));
        assert_eq!(timeout.deadline(), Time::from_secs(5));
    }

    #[test]
    fn after_computes_deadline() {
        let future = ready(42);
        let timeout = TimeoutFuture::after(Time::from_secs(10), Duration::from_secs(5), future);
        assert_eq!(timeout.deadline(), Time::from_secs(15));
    }

    #[test]
    fn timeout_function() {
        let t = timeout(Time::from_secs(10), Duration::from_secs(3), ready(42));
        assert_eq!(t.deadline(), Time::from_secs(13));
    }

    #[test]
    fn timeout_at_function() {
        let t = timeout_at(Time::from_secs(42), ready(123));
        assert_eq!(t.deadline(), Time::from_secs(42));
    }

    // =========================================================================
    // Accessor Tests
    // =========================================================================

    #[test]
    fn remaining_before_deadline() {
        let t = TimeoutFuture::new(ready(42), Time::from_secs(10));
        let remaining = t.remaining(Time::from_secs(7));
        assert_eq!(remaining, Duration::from_secs(3));
    }

    #[test]
    fn remaining_after_deadline() {
        let t = TimeoutFuture::new(ready(42), Time::from_secs(10));
        let remaining = t.remaining(Time::from_secs(15));
        assert_eq!(remaining, Duration::ZERO);
    }

    #[test]
    fn is_elapsed() {
        let t = TimeoutFuture::new(ready(42), Time::from_secs(10));
        assert!(!t.is_elapsed(Time::from_secs(5)));
        assert!(t.is_elapsed(Time::from_secs(10)));
        assert!(t.is_elapsed(Time::from_secs(15)));
    }

    #[test]
    fn inner() {
        let future = ready(42);
        let t = TimeoutFuture::new(future, Time::from_secs(5));
        let _ = t.inner(); // Just check it compiles
    }

    #[test]
    fn inner_mut() {
        let future = ready(42);
        let mut t = TimeoutFuture::new(future, Time::from_secs(5));
        let _ = t.inner_mut(); // Just check it compiles
    }

    #[test]
    fn into_inner() {
        let future = ready(42);
        let t = TimeoutFuture::new(future, Time::from_secs(5));
        let _inner = t.into_inner();
    }

    // =========================================================================
    // Reset Tests
    // =========================================================================

    #[test]
    fn reset_changes_deadline() {
        let mut t = TimeoutFuture::new(ready(42), Time::from_secs(5));
        t.reset(Time::from_secs(10));
        assert_eq!(t.deadline(), Time::from_secs(10));
    }

    #[test]
    fn reset_after_changes_deadline() {
        let mut t = TimeoutFuture::new(ready(42), Time::from_secs(5));
        t.reset_after(Time::from_secs(3), Duration::from_secs(7));
        assert_eq!(t.deadline(), Time::from_secs(10));
    }

    // =========================================================================
    // poll_with_time Tests
    // =========================================================================

    #[test]
    fn poll_with_time_future_completes() {
        let mut t = TimeoutFuture::new(ready(42), Time::from_secs(10));
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Time is before deadline, future is ready
        let result = t.poll_with_time(Time::from_secs(5), &mut cx);
        assert!(matches!(result, Poll::Ready(Ok(42))));
    }

    #[test]
    fn poll_with_time_timeout_elapsed() {
        let mut t = TimeoutFuture::new(pending::<i32>(), Time::from_secs(10));
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Time is past deadline
        let result = t.poll_with_time(Time::from_secs(15), &mut cx);
        assert!(matches!(result, Poll::Ready(Err(_))));

        if let Poll::Ready(Err(elapsed)) = result {
            assert_eq!(elapsed.deadline(), Time::from_secs(10));
        }
    }

    #[test]
    fn poll_with_time_pending() {
        let mut t = TimeoutFuture::new(pending::<i32>(), Time::from_secs(10));
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Time is before deadline, future is pending
        let result = t.poll_with_time(Time::from_secs(5), &mut cx);
        assert!(result.is_pending());
    }

    #[test]
    fn poll_with_time_at_exact_deadline() {
        let mut t = TimeoutFuture::new(pending::<i32>(), Time::from_secs(10));
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Time is exactly at deadline
        let result = t.poll_with_time(Time::from_secs(10), &mut cx);
        assert!(matches!(result, Poll::Ready(Err(_))));
    }

    #[test]
    fn poll_with_time_zero_deadline() {
        let mut t = TimeoutFuture::new(pending::<i32>(), Time::ZERO);
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Even at time zero, deadline is reached
        let result = t.poll_with_time(Time::ZERO, &mut cx);
        assert!(matches!(result, Poll::Ready(Err(_))));
    }

    // =========================================================================
    // Clone Tests
    // =========================================================================

    #[test]
    fn clone_copies_deadline_and_future() {
        let t = TimeoutFuture::new(ready(42), Time::from_secs(10));
        let t2 = t.clone();
        assert_eq!(t2.deadline(), Time::from_secs(10));
    }

    // =========================================================================
    // Integration Scenario Tests
    // =========================================================================

    #[test]
    fn simulated_timeout_scenario() {
        // Simulate a scenario where we poll multiple times as time advances
        static CURRENT_TIME: AtomicU64 = AtomicU64::new(0);

        let mut t = TimeoutFuture::new(pending::<i32>(), Time::from_secs(5));
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // t=0: pending
        let now = Time::from_nanos(CURRENT_TIME.load(Ordering::SeqCst));
        assert!(t.poll_with_time(now, &mut cx).is_pending());

        // t=2: still pending
        CURRENT_TIME.store(2_000_000_000, Ordering::SeqCst);
        let now = Time::from_nanos(CURRENT_TIME.load(Ordering::SeqCst));
        assert!(t.poll_with_time(now, &mut cx).is_pending());

        // t=4: still pending
        CURRENT_TIME.store(4_000_000_000, Ordering::SeqCst);
        let now = Time::from_nanos(CURRENT_TIME.load(Ordering::SeqCst));
        assert!(t.poll_with_time(now, &mut cx).is_pending());

        // t=5: timeout!
        CURRENT_TIME.store(5_000_000_000, Ordering::SeqCst);
        let now = Time::from_nanos(CURRENT_TIME.load(Ordering::SeqCst));
        let result = t.poll_with_time(now, &mut cx);
        assert!(matches!(result, Poll::Ready(Err(_))));
    }

    #[test]
    fn simulated_success_scenario() {
        // Future that completes on the 3rd poll
        struct CountingFuture {
            count: u32,
            ready_at: u32,
        }

        impl Future for CountingFuture {
            type Output = &'static str;

            fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
                self.count += 1;
                if self.count >= self.ready_at {
                    Poll::Ready("done")
                } else {
                    Poll::Pending
                }
            }
        }

        impl Unpin for CountingFuture {}

        let future = CountingFuture {
            count: 0,
            ready_at: 3,
        };
        let mut t = TimeoutFuture::new(future, Time::from_secs(10));
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Poll 1: pending
        assert!(t.poll_with_time(Time::from_secs(1), &mut cx).is_pending());

        // Poll 2: pending
        assert!(t.poll_with_time(Time::from_secs(2), &mut cx).is_pending());

        // Poll 3: ready!
        let result = t.poll_with_time(Time::from_secs(3), &mut cx);
        assert!(matches!(result, Poll::Ready(Ok("done"))));
    }

    // =========================================================================
    // Helper Functions
    // =========================================================================

    use std::sync::Arc;
    use std::task::Wake;

    /// A no-op waker implementation for testing.
    struct NoopWaker;

    impl Wake for NoopWaker {
        fn wake(self: Arc<Self>) {
            // No-op
        }

        fn wake_by_ref(self: &Arc<Self>) {
            // No-op
        }
    }

    /// Creates a no-op waker for testing.
    fn noop_waker() -> Waker {
        Arc::new(NoopWaker).into()
    }
}
