//! Elapsed error type for timeout operations.
//!
//! The [`Elapsed`] error is returned when a timeout expires before
//! the wrapped operation completes.

use crate::types::Time;
use core::fmt;

/// Error returned when a timeout elapses.
///
/// This error indicates that a [`TimeoutFuture`](super::TimeoutFuture)
/// did not complete before its deadline. The inner future was dropped
/// without producing a value.
///
/// # Example
///
/// ```
/// use asupersync::time::Elapsed;
/// use asupersync::types::Time;
///
/// let elapsed = Elapsed::new(Time::from_secs(5));
/// assert_eq!(elapsed.deadline(), Time::from_secs(5));
/// println!("{elapsed}"); // "deadline has elapsed at Time(5000000000ns)"
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Elapsed {
    /// The deadline that was exceeded.
    deadline: Time,
}

impl Elapsed {
    /// Creates a new `Elapsed` error with the given deadline.
    #[must_use]
    pub const fn new(deadline: Time) -> Self {
        Self { deadline }
    }

    /// Returns the deadline that was exceeded.
    #[must_use]
    pub const fn deadline(&self) -> Time {
        self.deadline
    }

    /// Returns the deadline as nanoseconds since the epoch.
    #[must_use]
    pub const fn deadline_nanos(&self) -> u64 {
        self.deadline.as_nanos()
    }
}

impl fmt::Display for Elapsed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "deadline has elapsed at {:?}", self.deadline)
    }
}

impl std::error::Error for Elapsed {}

impl Default for Elapsed {
    fn default() -> Self {
        Self::new(Time::ZERO)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_creates_with_deadline() {
        let deadline = Time::from_secs(10);
        let elapsed = Elapsed::new(deadline);
        assert_eq!(elapsed.deadline(), deadline);
    }

    #[test]
    fn deadline_nanos() {
        let elapsed = Elapsed::new(Time::from_millis(500));
        assert_eq!(elapsed.deadline_nanos(), 500_000_000);
    }

    #[test]
    fn display_format() {
        let elapsed = Elapsed::new(Time::from_secs(5));
        let s = elapsed.to_string();
        assert!(s.contains("elapsed"));
        assert!(s.contains("5000000000"));
    }

    #[test]
    fn default_is_zero() {
        let elapsed = Elapsed::default();
        assert_eq!(elapsed.deadline(), Time::ZERO);
    }

    #[test]
    fn clone_and_copy() {
        let e1 = Elapsed::new(Time::from_secs(1));
        let e2 = e1; // Copy
        let e3 = e1; // Also copy
        assert_eq!(e1, e2);
        assert_eq!(e2, e3);
    }

    #[test]
    fn equality() {
        let e1 = Elapsed::new(Time::from_secs(1));
        let e2 = Elapsed::new(Time::from_secs(1));
        let e3 = Elapsed::new(Time::from_secs(2));

        assert_eq!(e1, e2);
        assert_ne!(e1, e3);
    }

    #[test]
    fn is_error() {
        let elapsed = Elapsed::new(Time::from_secs(1));
        // Verify it implements Error
        let _: &dyn std::error::Error = &elapsed;
    }
}
