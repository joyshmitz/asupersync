//! Interest flags for I/O readiness.
//!
//! This module defines the [`Interest`] bitflags for specifying which I/O events
//! to monitor on registered sources.
//!
//! # Platform Mapping
//!
//! | Interest Flag | epoll | kqueue | IOCP |
//! |--------------|-------|--------|------|
//! | READABLE | EPOLLIN | EVFILT_READ | Completion |
//! | WRITABLE | EPOLLOUT | EVFILT_WRITE | Completion |
//! | ERROR | EPOLLERR | EV_ERROR | Completion |
//! | HUP | EPOLLHUP/RDHUP | EV_EOF | Completion |
//! | PRIORITY | EPOLLPRI | N/A | N/A |
//! | ONESHOT | EPOLLONESHOT | EV_ONESHOT | N/A |
//! | EDGE_TRIGGERED | EPOLLET | EV_CLEAR | N/A |
//!
//! # Example
//!
//! ```ignore
//! use asupersync::runtime::reactor::Interest;
//!
//! let interest = Interest::READABLE | Interest::WRITABLE;
//! assert!(interest.contains(Interest::READABLE));
//! assert!(interest.is_readable());
//! assert!(interest.is_writable());
//! ```

use std::ops::{BitAnd, BitAndAssign, BitOr, BitOrAssign, Not};

/// Interest in I/O readiness events.
///
/// Combines multiple interests with the `|` operator.
///
/// # Example
///
/// ```ignore
/// let interest = Interest::READABLE | Interest::WRITABLE;
/// assert!(interest.contains(Interest::READABLE));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(transparent)]
pub struct Interest(u8);

impl Interest {
    /// No interest (empty set).
    pub const NONE: Self = Self(0);

    /// Interested in read readiness.
    pub const READABLE: Self = Self(1 << 0);

    /// Interested in write readiness.
    pub const WRITABLE: Self = Self(1 << 1);

    /// Interested in error conditions.
    pub const ERROR: Self = Self(1 << 2);

    /// Interested in hang-up (peer closed).
    pub const HUP: Self = Self(1 << 3);

    /// Interested in priority/OOB data (EPOLLPRI).
    pub const PRIORITY: Self = Self(1 << 4);

    /// Request one-shot notification (EPOLLONESHOT).
    /// After firing, must re-arm with modify().
    pub const ONESHOT: Self = Self(1 << 5);

    /// Request edge-triggered mode (EPOLLET).
    /// Event fires on state CHANGE, not while condition persists.
    pub const EDGE_TRIGGERED: Self = Self(1 << 6);

    /// Common combination for sockets.
    pub const SOCKET: Self = Self(Self::READABLE.0 | Self::WRITABLE.0 | Self::ERROR.0 | Self::HUP.0);

    /// Returns interest in readable events.
    #[must_use]
    pub const fn readable() -> Self {
        Self::READABLE
    }

    /// Returns interest in writable events.
    #[must_use]
    pub const fn writable() -> Self {
        Self::WRITABLE
    }

    /// Returns interest in both readable and writable events.
    #[must_use]
    pub const fn both() -> Self {
        Self(Self::READABLE.0 | Self::WRITABLE.0)
    }

    /// Create empty interest set.
    #[must_use]
    pub const fn empty() -> Self {
        Self::NONE
    }

    /// Create interest from raw bits.
    #[must_use]
    pub const fn from_bits(bits: u8) -> Self {
        Self(bits)
    }

    /// Get raw bits.
    #[must_use]
    pub const fn bits(&self) -> u8 {
        self.0
    }

    /// Check if interest contains all flags in other.
    #[must_use]
    pub const fn contains(&self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    /// Check if interest is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0 == 0
    }

    /// Check if readable interest is set.
    #[must_use]
    pub const fn is_readable(&self) -> bool {
        (self.0 & Self::READABLE.0) != 0
    }

    /// Check if writable interest is set.
    #[must_use]
    pub const fn is_writable(&self) -> bool {
        (self.0 & Self::WRITABLE.0) != 0
    }

    /// Check if error interest is set.
    #[must_use]
    pub const fn is_error(&self) -> bool {
        (self.0 & Self::ERROR.0) != 0
    }

    /// Check if HUP interest is set.
    #[must_use]
    pub const fn is_hup(&self) -> bool {
        (self.0 & Self::HUP.0) != 0
    }

    /// Check if priority interest is set.
    #[must_use]
    pub const fn is_priority(&self) -> bool {
        (self.0 & Self::PRIORITY.0) != 0
    }

    /// Check if oneshot mode is set.
    #[must_use]
    pub const fn is_oneshot(&self) -> bool {
        (self.0 & Self::ONESHOT.0) != 0
    }

    /// Check if edge-triggered mode is set.
    #[must_use]
    pub const fn is_edge_triggered(&self) -> bool {
        (self.0 & Self::EDGE_TRIGGERED.0) != 0
    }

    /// Combines interests by adding flags.
    #[must_use]
    #[allow(clippy::should_implement_trait)]
    pub const fn add(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Removes interest flags.
    #[must_use]
    pub const fn remove(self, other: Self) -> Self {
        Self(self.0 & !other.0)
    }

    /// Returns a new interest with oneshot mode set.
    #[must_use]
    pub const fn with_oneshot(self) -> Self {
        Self(self.0 | Self::ONESHOT.0)
    }

    /// Returns a new interest with edge-triggered mode set.
    #[must_use]
    pub const fn with_edge_triggered(self) -> Self {
        Self(self.0 | Self::EDGE_TRIGGERED.0)
    }
}

impl BitOr for Interest {
    type Output = Self;

    #[inline]
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl BitOrAssign for Interest {
    #[inline]
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

impl BitAnd for Interest {
    type Output = Self;

    #[inline]
    fn bitand(self, rhs: Self) -> Self {
        Self(self.0 & rhs.0)
    }
}

impl BitAndAssign for Interest {
    #[inline]
    fn bitand_assign(&mut self, rhs: Self) {
        self.0 &= rhs.0;
    }
}

impl Not for Interest {
    type Output = Self;

    #[inline]
    fn not(self) -> Self {
        Self(!self.0)
    }
}

impl std::fmt::Display for Interest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut flags = Vec::new();
        if self.is_readable() {
            flags.push("READABLE");
        }
        if self.is_writable() {
            flags.push("WRITABLE");
        }
        if self.is_error() {
            flags.push("ERROR");
        }
        if self.is_hup() {
            flags.push("HUP");
        }
        if self.is_priority() {
            flags.push("PRIORITY");
        }
        if self.is_oneshot() {
            flags.push("ONESHOT");
        }
        if self.is_edge_triggered() {
            flags.push("EDGE_TRIGGERED");
        }
        if flags.is_empty() {
            write!(f, "NONE")
        } else {
            write!(f, "{}", flags.join(" | "))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interest_constants() {
        assert_eq!(Interest::NONE.bits(), 0);
        assert_eq!(Interest::READABLE.bits(), 1);
        assert_eq!(Interest::WRITABLE.bits(), 2);
        assert_eq!(Interest::ERROR.bits(), 4);
        assert_eq!(Interest::HUP.bits(), 8);
        assert_eq!(Interest::PRIORITY.bits(), 16);
        assert_eq!(Interest::ONESHOT.bits(), 32);
        assert_eq!(Interest::EDGE_TRIGGERED.bits(), 64);
    }

    #[test]
    fn interest_combining() {
        let interest = Interest::READABLE | Interest::WRITABLE;
        assert!(interest.is_readable());
        assert!(interest.is_writable());
        assert!(!interest.is_error());
        assert_eq!(interest, Interest::both());
    }

    #[test]
    fn interest_contains() {
        let interest = Interest::READABLE | Interest::WRITABLE | Interest::ERROR;
        assert!(interest.contains(Interest::READABLE));
        assert!(interest.contains(Interest::WRITABLE));
        assert!(interest.contains(Interest::ERROR));
        assert!(interest.contains(Interest::both()));
        assert!(!interest.contains(Interest::HUP));
    }

    #[test]
    fn interest_add_remove() {
        let mut interest = Interest::READABLE;
        interest = interest.add(Interest::WRITABLE);
        assert!(interest.is_readable());
        assert!(interest.is_writable());

        interest = interest.remove(Interest::READABLE);
        assert!(!interest.is_readable());
        assert!(interest.is_writable());
    }

    #[test]
    fn interest_bit_operators() {
        // BitOr
        let interest = Interest::READABLE | Interest::WRITABLE;
        assert_eq!(interest.bits(), 3);

        // BitAnd
        let masked = interest & Interest::READABLE;
        assert!(masked.is_readable());
        assert!(!masked.is_writable());

        // BitOrAssign
        let mut interest = Interest::READABLE;
        interest |= Interest::WRITABLE;
        assert!(interest.is_writable());

        // BitAndAssign
        interest &= Interest::READABLE;
        assert!(!interest.is_writable());

        // Not
        let not_readable = !Interest::READABLE;
        assert!(!not_readable.is_readable());
    }

    #[test]
    fn interest_socket() {
        let socket = Interest::SOCKET;
        assert!(socket.is_readable());
        assert!(socket.is_writable());
        assert!(socket.is_error());
        assert!(socket.is_hup());
        assert!(!socket.is_priority());
    }

    #[test]
    fn interest_modes() {
        let interest = Interest::READABLE.with_oneshot().with_edge_triggered();
        assert!(interest.is_readable());
        assert!(interest.is_oneshot());
        assert!(interest.is_edge_triggered());
    }

    #[test]
    fn interest_from_bits() {
        let interest = Interest::from_bits(0b011);
        assert!(interest.is_readable());
        assert!(interest.is_writable());
        assert!(!interest.is_error());
    }

    #[test]
    fn interest_empty() {
        assert!(Interest::NONE.is_empty());
        assert!(Interest::empty().is_empty());
        assert!(!Interest::READABLE.is_empty());
    }

    #[test]
    fn interest_default() {
        assert_eq!(Interest::default(), Interest::NONE);
    }

    #[test]
    fn interest_display() {
        assert_eq!(format!("{}", Interest::NONE), "NONE");
        assert_eq!(format!("{}", Interest::READABLE), "READABLE");
        assert_eq!(
            format!("{}", Interest::READABLE | Interest::WRITABLE),
            "READABLE | WRITABLE"
        );
    }

    #[test]
    fn interest_helpers() {
        assert_eq!(Interest::readable(), Interest::READABLE);
        assert_eq!(Interest::writable(), Interest::WRITABLE);
        assert_eq!(Interest::both(), Interest::READABLE | Interest::WRITABLE);
    }
}
