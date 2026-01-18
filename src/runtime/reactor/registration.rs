//! RAII registration handle for I/O sources.
//!
//! This module provides the [`Registration`] type that represents an active I/O
//! registration with the reactor. When dropped, it automatically deregisters
//! from the reactor, ensuring no leaked registrations and enabling cancel-safety.
//!
//! # Design
//!
//! The registration holds a weak reference to the reactor, allowing graceful
//! handling when the reactor is dropped before all registrations. This design
//! is critical for cancel-correctness:
//!
//! 1. When a task is cancelled, its I/O futures are dropped
//! 2. Dropping futures drops their Registration
//! 3. Registration::drop() deregisters from reactor
//! 4. No dangling registrations, no wakeup of dead tasks
//!
//! # Example
//!
//! ```ignore
//! use asupersync::runtime::reactor::{Registration, Interest, Token};
//!
//! // Registration is created by the reactor when registering a source
//! let registration = reactor.register(source, Interest::READABLE)?;
//!
//! // Change interest later
//! registration.set_interest(Interest::READABLE | Interest::WRITABLE)?;
//!
//! // Automatic deregistration when dropped
//! drop(registration);
//! ```

use super::{Interest, Token};
use std::cell::Cell;
use std::io;
use std::marker::PhantomData;
use std::sync::Weak;

/// Internal trait for reactor operations needed by Registration.
///
/// This trait is implemented by reactors to support RAII deregistration
/// and interest modification. It uses interior mutability since Registration
/// only holds a shared reference.
pub(crate) trait ReactorHandle: Send + Sync {
    /// Deregisters a source by its token.
    ///
    /// This is called from Registration::drop(). Errors are ignored since
    /// the source may already be gone or the reactor may be shutting down.
    fn deregister_by_token(&self, token: Token) -> io::Result<()>;

    /// Modifies the interest set for a registered source.
    ///
    /// # Errors
    ///
    /// Returns an error if the token is invalid or the reactor operation fails.
    fn modify_interest(&self, token: Token, interest: Interest) -> io::Result<()>;
}

/// Handle to an active I/O source registration.
///
/// Dropping a Registration automatically deregisters from the reactor.
/// This ensures no leaked registrations and is cancel-safe.
///
/// # Thread Safety
///
/// Registration is `!Send` and `!Sync` because it's tied to a specific reactor
/// and should be used only on the thread where it was created. This prevents
/// cross-thread deregistration issues.
///
/// # Cancel-Safety
///
/// When a task holding a Registration is cancelled:
/// 1. The task's future is dropped
/// 2. The Registration is dropped as part of the future
/// 3. The Drop impl deregisters from the reactor
/// 4. No stale wakeups can occur for the cancelled task
pub struct Registration {
    /// Token identifying this registration in the reactor's slab.
    token: Token,
    /// Weak reference to reactor (allows safe drop if reactor gone).
    reactor: Weak<dyn ReactorHandle>,
    /// Current interest (for modify operations).
    interest: Cell<Interest>,
    /// Marker to make Registration !Send + !Sync.
    _marker: PhantomData<*const ()>,
}

impl Registration {
    /// Creates a new registration.
    ///
    /// This is called internally by the reactor when registering a source.
    pub(crate) fn new(
        token: Token,
        reactor: Weak<dyn ReactorHandle>,
        interest: Interest,
    ) -> Self {
        Self {
            token,
            reactor,
            interest: Cell::new(interest),
            _marker: PhantomData,
        }
    }

    /// Returns the token identifying this registration.
    #[must_use]
    pub fn token(&self) -> Token {
        self.token
    }

    /// Returns the current interest set.
    #[must_use]
    pub fn interest(&self) -> Interest {
        self.interest.get()
    }

    /// Modifies the interest set for this registration.
    ///
    /// This allows changing which events the source is monitored for
    /// without deregistering and re-registering.
    ///
    /// # Errors
    ///
    /// Returns an error if the reactor is no longer available or if
    /// the modify operation fails.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Start monitoring only for reads
    /// let registration = reactor.register(source, Interest::READABLE)?;
    ///
    /// // Later, also monitor for writes
    /// registration.set_interest(Interest::READABLE | Interest::WRITABLE)?;
    /// ```
    pub fn set_interest(&self, interest: Interest) -> io::Result<()> {
        if let Some(reactor) = self.reactor.upgrade() {
            reactor.modify_interest(self.token, interest)?;
            self.interest.set(interest);
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "reactor has been dropped",
            ))
        }
    }

    /// Returns `true` if the registration is still active.
    ///
    /// A registration becomes inactive when the reactor is dropped.
    /// Inactive registrations will no-op on drop and fail on set_interest.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.reactor.strong_count() > 0
    }

    /// Explicitly deregisters without waiting for drop.
    ///
    /// This is useful when you want to handle deregistration errors
    /// explicitly rather than ignoring them (as drop does).
    ///
    /// # Errors
    ///
    /// Returns an error if the reactor is no longer available or if
    /// the deregister operation fails.
    pub fn deregister(self) -> io::Result<()> {
        if let Some(reactor) = self.reactor.upgrade() {
            let result = reactor.deregister_by_token(self.token);
            // Prevent Drop from running since we've already deregistered
            std::mem::forget(self);
            result
        } else {
            // Reactor already gone, nothing to do
            std::mem::forget(self);
            Ok(())
        }
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        if let Some(reactor) = self.reactor.upgrade() {
            // Deregister, ignoring errors (source may already be gone
            // or reactor may be shutting down)
            let _ = reactor.deregister_by_token(self.token);
        }
    }
}

impl std::fmt::Debug for Registration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registration")
            .field("token", &self.token)
            .field("interest", &self.interest.get())
            .field("active", &self.is_active())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Mock reactor for testing Registration RAII behavior.
    struct MockReactor {
        deregistered: AtomicBool,
        deregister_count: AtomicUsize,
        last_token: std::sync::Mutex<Option<Token>>,
        last_interest: std::sync::Mutex<Option<Interest>>,
    }

    impl MockReactor {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                deregistered: AtomicBool::new(false),
                deregister_count: AtomicUsize::new(0),
                last_token: std::sync::Mutex::new(None),
                last_interest: std::sync::Mutex::new(None),
            })
        }

        fn was_deregistered(&self) -> bool {
            self.deregistered.load(Ordering::SeqCst)
        }

        fn deregister_count(&self) -> usize {
            self.deregister_count.load(Ordering::SeqCst)
        }
    }

    impl ReactorHandle for MockReactor {
        fn deregister_by_token(&self, token: Token) -> io::Result<()> {
            self.deregistered.store(true, Ordering::SeqCst);
            self.deregister_count.fetch_add(1, Ordering::SeqCst);
            *self.last_token.lock().unwrap() = Some(token);
            Ok(())
        }

        fn modify_interest(&self, token: Token, interest: Interest) -> io::Result<()> {
            *self.last_token.lock().unwrap() = Some(token);
            *self.last_interest.lock().unwrap() = Some(interest);
            Ok(())
        }
    }

    #[test]
    fn drop_deregisters() {
        let reactor = MockReactor::new();
        let token = Token::new(42);

        {
            let _reg = Registration::new(
                token,
                Arc::downgrade(&reactor) as Weak<dyn ReactorHandle>,
                Interest::READABLE,
            );
            assert!(!reactor.was_deregistered());
        }

        assert!(reactor.was_deregistered());
        assert_eq!(*reactor.last_token.lock().unwrap(), Some(token));
    }

    #[test]
    fn set_interest_updates_reactor() {
        let reactor = MockReactor::new();
        let token = Token::new(1);

        let reg = Registration::new(
            token,
            Arc::downgrade(&reactor) as Weak<dyn ReactorHandle>,
            Interest::READABLE,
        );

        assert_eq!(reg.interest(), Interest::READABLE);

        reg.set_interest(Interest::WRITABLE).unwrap();

        assert_eq!(reg.interest(), Interest::WRITABLE);
        assert_eq!(
            *reactor.last_interest.lock().unwrap(),
            Some(Interest::WRITABLE)
        );
    }

    #[test]
    fn handles_reactor_dropped() {
        let token = Token::new(1);

        let reg = {
            let reactor = MockReactor::new();
            Registration::new(
                token,
                Arc::downgrade(&reactor) as Weak<dyn ReactorHandle>,
                Interest::READABLE,
            )
            // reactor is dropped here
        };

        // Registration should not panic when reactor is gone
        assert!(!reg.is_active());

        // set_interest should fail gracefully
        let result = reg.set_interest(Interest::WRITABLE);
        assert!(result.is_err());

        // drop should not panic
        drop(reg);
    }

    #[test]
    fn is_active() {
        let reactor = MockReactor::new();
        let token = Token::new(1);

        let reg = Registration::new(
            token,
            Arc::downgrade(&reactor) as Weak<dyn ReactorHandle>,
            Interest::READABLE,
        );

        assert!(reg.is_active());

        drop(reactor);

        assert!(!reg.is_active());
    }

    #[test]
    fn explicit_deregister() {
        let reactor = MockReactor::new();
        let token = Token::new(1);

        let reg = Registration::new(
            token,
            Arc::downgrade(&reactor) as Weak<dyn ReactorHandle>,
            Interest::READABLE,
        );

        let result = reg.deregister();
        assert!(result.is_ok());
        assert!(reactor.was_deregistered());
        assert_eq!(reactor.deregister_count(), 1);
        // Note: reg is consumed, so drop won't run again
    }

    #[test]
    fn explicit_deregister_when_reactor_gone() {
        let token = Token::new(1);

        let reg = {
            let reactor = MockReactor::new();
            Registration::new(
                token,
                Arc::downgrade(&reactor) as Weak<dyn ReactorHandle>,
                Interest::READABLE,
            )
        };

        // Should succeed even though reactor is gone
        let result = reg.deregister();
        assert!(result.is_ok());
    }

    #[test]
    fn token_accessor() {
        let reactor = MockReactor::new();
        let token = Token::new(999);

        let reg = Registration::new(
            token,
            Arc::downgrade(&reactor) as Weak<dyn ReactorHandle>,
            Interest::READABLE,
        );

        assert_eq!(reg.token(), token);
    }

    #[test]
    fn debug_impl() {
        let reactor = MockReactor::new();
        let token = Token::new(42);

        let reg = Registration::new(
            token,
            Arc::downgrade(&reactor) as Weak<dyn ReactorHandle>,
            Interest::READABLE,
        );

        let debug = format!("{:?}", reg);
        assert!(debug.contains("Registration"));
        assert!(debug.contains("42"));
    }

    #[test]
    fn multiple_registrations() {
        let reactor = MockReactor::new();

        {
            let _reg1 = Registration::new(
                Token::new(1),
                Arc::downgrade(&reactor) as Weak<dyn ReactorHandle>,
                Interest::READABLE,
            );
            let _reg2 = Registration::new(
                Token::new(2),
                Arc::downgrade(&reactor) as Weak<dyn ReactorHandle>,
                Interest::WRITABLE,
            );

            assert_eq!(reactor.deregister_count(), 0);
        }

        // Both should have been deregistered
        assert_eq!(reactor.deregister_count(), 2);
    }
}
