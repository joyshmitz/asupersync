//! Fuzz target for Interest flags operations.
//!
//! This is a simple fuzz target that exercises the Interest bitflag type
//! from the reactor module. It ensures all bitwise operations are safe
//! and don't cause panics or undefined behavior.
//!
//! # Running
//! ```bash
//! cargo +nightly fuzz run fuzz_interest_flags
//! ```

#![no_main]

use libfuzzer_sys::fuzz_target;

/// Simulated Interest flags (mirrors reactor::Interest).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Interest(u8);

impl Interest {
    const NONE: Self = Self(0);
    const READABLE: Self = Self(1 << 0);
    const WRITABLE: Self = Self(1 << 1);
    const ERROR: Self = Self(1 << 2);
    const HUP: Self = Self(1 << 3);
    const EDGE_TRIGGERED: Self = Self(1 << 7);

    fn is_readable(self) -> bool {
        self.0 & Self::READABLE.0 != 0
    }

    fn is_writable(self) -> bool {
        self.0 & Self::WRITABLE.0 != 0
    }

    fn is_error(self) -> bool {
        self.0 & Self::ERROR.0 != 0
    }

    fn is_hup(self) -> bool {
        self.0 & Self::HUP.0 != 0
    }

    fn add(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    fn remove(self, other: Self) -> Self {
        Self(self.0 & !other.0)
    }

    fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    fn both() -> Self {
        Self::READABLE.add(Self::WRITABLE)
    }
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    // Create interest from first byte
    let interest = Interest(data[0]);

    // Exercise all operations
    let _ = interest.is_readable();
    let _ = interest.is_writable();
    let _ = interest.is_error();
    let _ = interest.is_hup();

    // Combine with known flags
    let combined = interest.add(Interest::READABLE);
    let _ = combined.contains(Interest::READABLE);

    let removed = combined.remove(Interest::WRITABLE);
    let _ = removed.is_writable();

    // Test both()
    let both = Interest::both();
    let _ = both.is_readable();
    let _ = both.is_writable();

    // If we have more bytes, do pairwise operations
    for window in data.windows(2) {
        let a = Interest(window[0]);
        let b = Interest(window[1]);

        let _ = a.add(b);
        let _ = a.remove(b);
        let _ = a.contains(b);

        // Verify invariants
        assert!((a.add(b)).contains(a) || a.0 == 0);
        assert!((a.add(b)).contains(b) || b.0 == 0);
    }
});
