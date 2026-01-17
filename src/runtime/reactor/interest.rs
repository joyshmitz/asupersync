//! Interest flags for I/O readiness.

/// Interest flags indicating what I/O events to monitor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Interest(u8);

impl Interest {
    /// Interest in readable events.
    pub const READABLE: Interest = Interest(0b01);
    /// Interest in writable events.
    pub const WRITABLE: Interest = Interest(0b10);

    /// Returns interest in readable events.
    pub const fn readable() -> Self {
        Self::READABLE
    }

    /// Returns interest in writable events.
    pub const fn writable() -> Self {
        Self::WRITABLE
    }

    /// Returns interest in both readable and writable events.
    pub const fn both() -> Self {
        Interest(0b11)
    }

    /// Returns true if readable interest is set.
    pub const fn is_readable(&self) -> bool {
        self.0 & Self::READABLE.0 != 0
    }

    /// Returns true if writable interest is set.
    pub const fn is_writable(&self) -> bool {
        self.0 & Self::WRITABLE.0 != 0
    }
    
    /// Combines interests.
    pub fn add(self, other: Interest) -> Self {
        Interest(self.0 | other.0)
    }
    
    /// Removes interest.
    pub fn remove(self, other: Interest) -> Self {
        Interest(self.0 & !other.0)
    }
}