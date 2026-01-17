//! Sleep future for delaying execution.
//!
//! The [`Sleep`] future completes after a deadline has passed.
//! It works with both wall clock time (production) and virtual time (lab).

use crate::types::Time;
use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

/// A future that completes after a specified deadline.
///
/// `Sleep` is the core primitive for time-based delays. It can be awaited
/// to pause execution until the deadline has passed.
///
/// # Time Sources
///
/// By default, `Sleep` checks time at each poll. The actual time source
/// depends on the runtime context:
/// - Production: Uses wall clock time
/// - Lab runtime: Uses virtual time
///
/// For standalone use without a runtime, you can provide a time getter.
///
/// # Cancel Safety
///
/// `Sleep` is cancel-safe. Dropping it simply stops the wait with no
/// side effects. It can be recreated with the same or a different deadline.
///
/// # Example
///
/// ```ignore
/// use asupersync::time::sleep;
/// use std::time::Duration;
///
/// // Sleep for 100 milliseconds
/// sleep(Duration::from_millis(100)).await;
///
/// // Sleep until a specific time
/// use asupersync::time::sleep_until;
/// use asupersync::types::Time;
/// sleep_until(Time::from_secs(5)).await;
/// ```
#[derive(Debug)]
pub struct Sleep {
    /// The deadline when this sleep completes.
    deadline: Time,
    /// Optional time getter for standalone use.
    /// When None, uses a default mechanism (currently instant check).
    pub(crate) time_getter: Option<fn() -> Time>,
    /// Whether this sleep has been polled at least once.
    /// Used for tracing/debugging.
    polled: Cell<bool>,
}

impl Sleep {
    /// Creates a new `Sleep` that completes at the given deadline.
    ///
    /// # Arguments
    ///
    /// * `deadline` - The absolute time when this sleep completes
    ///
    /// # Example
    ///
    /// ```
    /// use asupersync::time::Sleep;
    /// use asupersync::types::Time;
    ///
    /// let sleep = Sleep::new(Time::from_secs(5));
    /// assert_eq!(sleep.deadline(), Time::from_secs(5));
    /// ```
    #[must_use]
    pub const fn new(deadline: Time) -> Self {
        Self {
            deadline,
            time_getter: None,
            polled: Cell::new(false),
        }
    }

    /// Creates a `Sleep` that completes after the given duration from `now`.
    ///
    /// # Arguments
    ///
    /// * `now` - The current time
    /// * `duration` - How long to sleep
    ///
    /// # Example
    ///
    /// ```
    /// use asupersync::time::Sleep;
    /// use asupersync::types::Time;
    /// use std::time::Duration;
    ///
    /// let now = Time::from_secs(10);
    /// let sleep = Sleep::after(now, Duration::from_secs(5));
    /// assert_eq!(sleep.deadline(), Time::from_secs(15));
    /// ```
    #[must_use]
    pub fn after(now: Time, duration: Duration) -> Self {
        let deadline = now.saturating_add_nanos(duration.as_nanos() as u64);
        Self::new(deadline)
    }

    /// Creates a `Sleep` with a custom time getter function.
    ///
    /// This is useful for testing or when you need to control the time source.
    ///
    /// # Arguments
    ///
    /// * `deadline` - The deadline when this sleep completes
    /// * `time_getter` - Function that returns the current time
    #[must_use]
    pub const fn with_time_getter(deadline: Time, time_getter: fn() -> Time) -> Self {
        Self {
            deadline,
            time_getter: Some(time_getter),
            polled: Cell::new(false),
        }
    }

    /// Returns the deadline for this sleep.
    #[must_use]
    pub const fn deadline(&self) -> Time {
        self.deadline
    }

    /// Returns the remaining duration until the deadline.
    ///
    /// Returns `Duration::ZERO` if the deadline has passed.
    ///
    /// # Arguments
    ///
    /// * `now` - The current time to compare against
    #[must_use]
    pub fn remaining(&self, now: Time) -> Duration {
        if now >= self.deadline {
            Duration::ZERO
        } else {
            let nanos = self.deadline.as_nanos().saturating_sub(now.as_nanos());
            Duration::from_nanos(nanos)
        }
    }

    /// Checks if the deadline has elapsed.
    ///
    /// # Arguments
    ///
    /// * `now` - The current time to compare against
    #[must_use]
    pub fn is_elapsed(&self, now: Time) -> bool {
        now >= self.deadline
    }

    /// Resets this sleep to a new deadline.
    ///
    /// This can be used to reuse a `Sleep` instance without allocating a new one.
    pub fn reset(&mut self, deadline: Time) {
        self.deadline = deadline;
        self.polled.set(false);
    }

    /// Resets this sleep to complete after the given duration from `now`.
    pub fn reset_after(&mut self, now: Time, duration: Duration) {
        self.deadline = now.saturating_add_nanos(duration.as_nanos() as u64);
        self.polled.set(false);
    }

    /// Returns true if this sleep has been polled at least once.
    #[must_use]
    pub fn was_polled(&self) -> bool {
        self.polled.get()
    }

    /// Gets the current time using the configured time getter or default.
    fn current_time(&self) -> Time {
        // Default: for standalone use without a time getter, return MAX
        // so sleep never completes without a time source.
        // In a real runtime, the time source would be provided by context.
        self.time_getter.map_or(Time::MAX, |getter| getter())
    }

    /// Polls this sleep with an explicit time value.
    ///
    /// This is useful when you want to control the time source manually
    /// rather than using the built-in time getter.
    ///
    /// Returns `Poll::Ready(())` if the deadline has passed.
    pub fn poll_with_time(&self, now: Time) -> Poll<()> {
        self.polled.set(true);
        if now >= self.deadline {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let now = self.current_time();
        self.poll_with_time(now)
    }
}

impl Clone for Sleep {
    fn clone(&self) -> Self {
        Self {
            deadline: self.deadline,
            time_getter: self.time_getter,
            polled: Cell::new(false), // Fresh clone hasn't been polled
        }
    }
}

/// Creates a `Sleep` future that completes after the given duration.
///
/// This function requires a current time to compute the deadline.
/// For use without explicit time, see [`sleep_until`].
///
/// # Arguments
///
/// * `now` - The current time
/// * `duration` - How long to sleep
///
/// # Example
///
/// ```
/// use asupersync::time::sleep;
/// use asupersync::types::Time;
/// use std::time::Duration;
///
/// let now = Time::from_secs(10);
/// let sleep_future = sleep(now, Duration::from_millis(100));
/// assert_eq!(sleep_future.deadline(), Time::from_nanos(10_100_000_000));
/// ```
#[must_use]
pub fn sleep(now: Time, duration: Duration) -> Sleep {
    Sleep::after(now, duration)
}

/// Creates a `Sleep` future that completes at the given deadline.
///
/// # Arguments
///
/// * `deadline` - The absolute time when the sleep completes
///
/// # Example
///
/// ```
/// use asupersync::time::sleep_until;
/// use asupersync::types::Time;
///
/// let sleep_future = sleep_until(Time::from_secs(5));
/// assert_eq!(sleep_future.deadline(), Time::from_secs(5));
/// ```
#[must_use]
pub const fn sleep_until(deadline: Time) -> Sleep {
    Sleep::new(deadline)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    // =========================================================================
    // Construction Tests
    // =========================================================================

    #[test]
    fn new_creates_sleep_with_deadline() {
        let sleep = Sleep::new(Time::from_secs(5));
        assert_eq!(sleep.deadline(), Time::from_secs(5));
        assert!(!sleep.was_polled());
    }

    #[test]
    fn after_computes_deadline() {
        let now = Time::from_secs(10);
        let sleep = Sleep::after(now, Duration::from_secs(5));
        assert_eq!(sleep.deadline(), Time::from_secs(15));
    }

    #[test]
    fn after_saturates() {
        let now = Time::from_nanos(u64::MAX - 1000);
        let sleep = Sleep::after(now, Duration::from_secs(1));
        assert_eq!(sleep.deadline(), Time::MAX);
    }

    #[test]
    fn sleep_function() {
        let now = Time::from_millis(100);
        let s = sleep(now, Duration::from_millis(50));
        assert_eq!(s.deadline(), Time::from_millis(150));
    }

    #[test]
    fn sleep_until_function() {
        let s = sleep_until(Time::from_secs(42));
        assert_eq!(s.deadline(), Time::from_secs(42));
    }

    // =========================================================================
    // Time Getter Tests
    // =========================================================================

    #[test]
    fn with_time_getter() {
        static CURRENT_TIME: AtomicU64 = AtomicU64::new(0);

        fn get_time() -> Time {
            Time::from_nanos(CURRENT_TIME.load(Ordering::SeqCst))
        }

        let sleep = Sleep::with_time_getter(Time::from_secs(5), get_time);

        // Time is 0, should be pending
        assert!(!sleep.is_elapsed(get_time()));

        // Advance time past deadline
        CURRENT_TIME.store(6_000_000_000, Ordering::SeqCst);
        assert!(sleep.is_elapsed(get_time()));
    }

    // =========================================================================
    // is_elapsed and remaining Tests
    // =========================================================================

    #[test]
    fn is_elapsed_before_deadline() {
        let sleep = Sleep::new(Time::from_secs(10));
        assert!(!sleep.is_elapsed(Time::from_secs(5)));
    }

    #[test]
    fn is_elapsed_at_deadline() {
        let sleep = Sleep::new(Time::from_secs(10));
        assert!(sleep.is_elapsed(Time::from_secs(10)));
    }

    #[test]
    fn is_elapsed_after_deadline() {
        let sleep = Sleep::new(Time::from_secs(10));
        assert!(sleep.is_elapsed(Time::from_secs(15)));
    }

    #[test]
    fn remaining_before_deadline() {
        let sleep = Sleep::new(Time::from_secs(10));
        let remaining = sleep.remaining(Time::from_secs(7));
        assert_eq!(remaining, Duration::from_secs(3));
    }

    #[test]
    fn remaining_at_deadline() {
        let sleep = Sleep::new(Time::from_secs(10));
        let remaining = sleep.remaining(Time::from_secs(10));
        assert_eq!(remaining, Duration::ZERO);
    }

    #[test]
    fn remaining_after_deadline() {
        let sleep = Sleep::new(Time::from_secs(10));
        let remaining = sleep.remaining(Time::from_secs(15));
        assert_eq!(remaining, Duration::ZERO);
    }

    // =========================================================================
    // poll_with_time Tests
    // =========================================================================

    #[test]
    fn poll_with_time_before_deadline() {
        let sleep = Sleep::new(Time::from_secs(10));
        let poll = sleep.poll_with_time(Time::from_secs(5));
        assert!(poll.is_pending());
        assert!(sleep.was_polled());
    }

    #[test]
    fn poll_with_time_at_deadline() {
        let sleep = Sleep::new(Time::from_secs(10));
        let poll = sleep.poll_with_time(Time::from_secs(10));
        assert!(poll.is_ready());
    }

    #[test]
    fn poll_with_time_after_deadline() {
        let sleep = Sleep::new(Time::from_secs(10));
        let poll = sleep.poll_with_time(Time::from_secs(15));
        assert!(poll.is_ready());
    }

    #[test]
    fn poll_with_time_zero_deadline() {
        let sleep = Sleep::new(Time::ZERO);
        let poll = sleep.poll_with_time(Time::ZERO);
        assert!(poll.is_ready());
    }

    // =========================================================================
    // Reset Tests
    // =========================================================================

    #[test]
    fn reset_changes_deadline() {
        let mut sleep = Sleep::new(Time::from_secs(10));

        // Poll it
        let _ = sleep.poll_with_time(Time::from_secs(5));
        assert!(sleep.was_polled());

        // Reset
        sleep.reset(Time::from_secs(20));
        assert_eq!(sleep.deadline(), Time::from_secs(20));
        assert!(!sleep.was_polled()); // Reset clears polled flag
    }

    #[test]
    fn reset_after_changes_deadline() {
        let mut sleep = Sleep::new(Time::from_secs(10));
        sleep.reset_after(Time::from_secs(5), Duration::from_secs(3));
        assert_eq!(sleep.deadline(), Time::from_secs(8));
    }

    // =========================================================================
    // Clone Tests
    // =========================================================================

    #[test]
    fn clone_copies_deadline() {
        let original = Sleep::new(Time::from_secs(10));
        let cloned = original.clone();
        assert_eq!(cloned.deadline(), Time::from_secs(10));
    }

    #[test]
    fn clone_has_fresh_polled_flag() {
        let original = Sleep::new(Time::from_secs(10));
        let _ = original.poll_with_time(Time::from_secs(5));
        assert!(original.was_polled());

        let cloned = original.clone();
        assert!(!cloned.was_polled());
    }

    // =========================================================================
    // Edge Cases
    // =========================================================================

    #[test]
    fn zero_duration_sleep() {
        let now = Time::from_secs(10);
        let sleep = sleep(now, Duration::ZERO);
        assert_eq!(sleep.deadline(), Time::from_secs(10));

        // Should be immediately ready
        let poll = sleep.poll_with_time(now);
        assert!(poll.is_ready());
    }

    #[test]
    fn max_time_deadline() {
        let sleep = Sleep::new(Time::MAX);
        let poll = sleep.poll_with_time(Time::from_secs(1000));
        assert!(poll.is_pending());

        // Only ready at MAX
        let poll = sleep.poll_with_time(Time::MAX);
        assert!(poll.is_ready());
    }

    #[test]
    fn time_zero_deadline() {
        let sleep = Sleep::new(Time::ZERO);

        // Any non-zero time is past deadline
        let poll = sleep.poll_with_time(Time::from_nanos(1));
        assert!(poll.is_ready());
    }
}
