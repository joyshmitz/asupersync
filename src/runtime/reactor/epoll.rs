//! Linux epoll-based reactor implementation.
//!
//! This module provides [`EpollReactor`], a reactor implementation that uses
//! Linux epoll for efficient I/O event notification with edge-triggered mode.
//!
//! # Safety
//!
//! This module uses `unsafe` code to interface with the `polling` crate's
//! low-level epoll operations. The unsafe operations are:
//!
//! - `Poller::add()`: Registers a file descriptor with epoll
//! - `Poller::modify()`: Modifies interest flags for a registered fd
//! - `Poller::delete()`: Removes a file descriptor from epoll
//!
//! These are unsafe because the compiler cannot verify that file descriptors
//! remain valid for the duration of their registration. The `EpollReactor`
//! maintains this invariant through careful bookkeeping and expects callers
//! to properly manage source lifetimes.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                       EpollReactor                               │
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
//! │  │   Poller    │  │  notify()   │  │    registration map     │  │
//! │  │  (polling)  │  │  (builtin)  │  │   HashMap<Token, info>  │  │
//! │  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Thread Safety
//!
//! `EpollReactor` is `Send + Sync` and can be shared across threads via `Arc`.
//! Internal state is protected by `Mutex` for registration/deregistration,
//! while `poll()` and `wake()` are lock-free for performance.
//!
//! # Example
//!
//! ```ignore
//! use asupersync::runtime::reactor::{EpollReactor, Reactor, Interest, Events};
//! use std::net::TcpListener;
//!
//! let reactor = EpollReactor::new()?;
//! let listener = TcpListener::bind("127.0.0.1:0")?;
//!
//! // Register the listener with epoll (edge-triggered mode)
//! reactor.register(&listener, Token::new(1), Interest::READABLE)?;
//!
//! // Poll for events
//! let mut events = Events::with_capacity(64);
//! let count = reactor.poll(&mut events, Some(Duration::from_secs(1)))?;
//! ```

// Allow unsafe code for epoll FFI operations via the polling crate.
// The unsafe operations (add, modify, delete) are necessary because the
// compiler cannot verify file descriptor validity at compile time.
#![allow(unsafe_code)]

use super::{Event, Events, Interest, Reactor, Source, Token};
use parking_lot::Mutex;
use polling::{Event as PollEvent, Poller};
use std::collections::HashMap;
use std::io;
use std::os::fd::BorrowedFd;
use std::time::Duration;

/// Registration state for a source.
#[derive(Debug)]
struct RegistrationInfo {
    /// The raw file descriptor (for bookkeeping).
    raw_fd: i32,
    /// The current interest flags.
    interest: Interest,
}

/// Linux epoll-based reactor with edge-triggered mode.
///
/// This reactor uses the `polling` crate to interface with Linux epoll,
/// providing efficient I/O event notification for async operations.
///
/// # Features
///
/// - `register()`: Adds fd to epoll with EPOLLET (edge-triggered)
/// - `modify()`: Updates interest flags for a registered fd
/// - `deregister()`: Removes fd from epoll
/// - `poll()`: Waits for and collects ready events
/// - `wake()`: Interrupts a blocking poll from another thread
///
/// # Edge-Triggered Mode
///
/// This reactor uses edge-triggered mode (`EPOLLET`) for efficiency.
/// Events fire when state *changes*, not while the condition persists.
/// Applications must read/write until `EAGAIN` before the next event.
pub struct EpollReactor {
    /// The polling instance (wraps epoll on Linux).
    poller: Poller,
    /// Maps tokens to registration info for bookkeeping.
    registrations: Mutex<HashMap<Token, RegistrationInfo>>,
}

impl EpollReactor {
    /// Creates a new epoll-based reactor.
    ///
    /// This initializes a `Poller` instance which creates an epoll fd internally.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `epoll_create1()` fails (e.g., out of file descriptors)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let reactor = EpollReactor::new()?;
    /// assert!(reactor.is_empty());
    /// ```
    pub fn new() -> io::Result<Self> {
        let poller = Poller::new()?;

        Ok(Self {
            poller,
            registrations: Mutex::new(HashMap::new()),
        })
    }

    /// Converts our Interest flags to polling crate's event.
    fn interest_to_poll_event(token: Token, interest: Interest) -> PollEvent {
        let key = token.0;
        let readable = interest.is_readable();
        let writable = interest.is_writable();

        match (readable, writable) {
            (true, true) => PollEvent::all(key),
            (true, false) => PollEvent::readable(key),
            (false, true) => PollEvent::writable(key),
            (false, false) => PollEvent::none(key),
        }
    }

    /// Converts polling crate's event to our Interest type.
    fn poll_event_to_interest(event: &PollEvent) -> Interest {
        let mut interest = Interest::NONE;

        if event.readable {
            interest = interest.add(Interest::READABLE);
        }
        if event.writable {
            interest = interest.add(Interest::WRITABLE);
        }

        interest
    }
}

impl Reactor for EpollReactor {
    fn register(&self, source: &dyn Source, token: Token, interest: Interest) -> io::Result<()> {
        let raw_fd = source.as_raw_fd();

        // Check for duplicate registration first
        let mut regs = self.registrations.lock();
        if regs.contains_key(&token) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "token already registered",
            ));
        }

        // Create the polling event with the token as the key
        let event = Self::interest_to_poll_event(token, interest);

        // SAFETY: We trust that the caller maintains the invariant that the
        // source (and its file descriptor) remains valid until deregistered.
        // The BorrowedFd is only used for the duration of this call.
        let borrowed_fd = unsafe { BorrowedFd::borrow_raw(raw_fd) };

        // Add to epoll via the polling crate
        self.poller.add(&borrowed_fd, event)?;

        // Track the registration for modify/deregister
        regs.insert(token, RegistrationInfo { raw_fd, interest });

        Ok(())
    }

    fn modify(&self, token: Token, interest: Interest) -> io::Result<()> {
        let mut regs = self.registrations.lock();
        let info = regs
            .get_mut(&token)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "token not registered"))?;

        // Create the new polling event
        let event = Self::interest_to_poll_event(token, interest);

        // SAFETY: We stored the raw_fd during registration and trust it's still valid.
        // The caller is responsible for ensuring the fd remains valid until deregistered.
        let borrowed_fd = unsafe { BorrowedFd::borrow_raw(info.raw_fd) };

        // Modify the epoll registration
        self.poller.modify(&borrowed_fd, event)?;

        // Update our bookkeeping
        info.interest = interest;

        Ok(())
    }

    fn deregister(&self, token: Token) -> io::Result<()> {
        let mut regs = self.registrations.lock();
        let info = regs
            .remove(&token)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "token not registered"))?;

        // SAFETY: We stored the raw_fd during registration and trust it's still valid.
        // The caller is responsible for ensuring the fd remains valid until deregistered.
        let borrowed_fd = unsafe { BorrowedFd::borrow_raw(info.raw_fd) };

        // Remove from epoll
        self.poller.delete(&borrowed_fd)?;

        Ok(())
    }

    fn poll(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<usize> {
        events.clear();

        // Allocate buffer for polling events (polling 2.x uses Vec<Event>)
        let capacity = events.capacity().max(1);
        let mut poll_events: Vec<PollEvent> = Vec::with_capacity(capacity);

        self.poller.wait(&mut poll_events, timeout)?;

        // Convert polling events to our Event type
        let mut count = 0;
        for poll_event in &poll_events {
            let token = Token(poll_event.key);
            let interest = Self::poll_event_to_interest(poll_event);
            events.push(Event::new(token, interest));
            count += 1;
        }

        Ok(count)
    }

    fn wake(&self) -> io::Result<()> {
        // The polling crate has a built-in notify mechanism
        self.poller.notify()
    }

    fn registration_count(&self) -> usize {
        self.registrations.lock().len()
    }
}

impl std::fmt::Debug for EpollReactor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let reg_count = self.registrations.lock().len();
        f.debug_struct("EpollReactor")
            .field("registration_count", &reg_count)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixStream;
    use std::time::Duration;

    #[test]
    fn create_reactor() {
        let reactor = EpollReactor::new().expect("failed to create reactor");
        assert!(reactor.is_empty());
        assert_eq!(reactor.registration_count(), 0);
    }

    #[test]
    fn register_and_deregister() {
        let reactor = EpollReactor::new().expect("failed to create reactor");
        let (sock1, _sock2) = UnixStream::pair().expect("failed to create unix stream pair");

        let token = Token::new(42);
        reactor
            .register(&sock1, token, Interest::READABLE)
            .expect("register failed");

        assert_eq!(reactor.registration_count(), 1);
        assert!(!reactor.is_empty());

        reactor.deregister(token).expect("deregister failed");

        assert_eq!(reactor.registration_count(), 0);
        assert!(reactor.is_empty());
    }

    #[test]
    fn deregister_not_found() {
        let reactor = EpollReactor::new().expect("failed to create reactor");
        let result = reactor.deregister(Token::new(999));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn modify_interest() {
        let reactor = EpollReactor::new().expect("failed to create reactor");
        let (sock1, _sock2) = UnixStream::pair().expect("failed to create unix stream pair");

        let token = Token::new(1);
        reactor
            .register(&sock1, token, Interest::READABLE)
            .expect("register failed");

        // Modify updates our bookkeeping (but not the actual epoll due to API limitations)
        reactor
            .modify(token, Interest::WRITABLE)
            .expect("modify failed");

        // Verify bookkeeping was updated
        let regs = reactor.registrations.lock();
        let info = regs.get(&token).unwrap();
        assert_eq!(info.interest, Interest::WRITABLE);
        drop(regs);

        reactor.deregister(token).expect("deregister failed");
    }

    #[test]
    fn modify_not_found() {
        let reactor = EpollReactor::new().expect("failed to create reactor");
        let result = reactor.modify(Token::new(999), Interest::READABLE);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn wake_unblocks_poll() {
        let reactor = EpollReactor::new().expect("failed to create reactor");
        let mut events = Events::with_capacity(64);

        // Spawn a thread to wake us
        let reactor_ref = &reactor;
        std::thread::scope(|s| {
            s.spawn(|| {
                std::thread::sleep(Duration::from_millis(50));
                reactor_ref.wake().expect("wake failed");
            });

            // This should return early due to wake
            let start = std::time::Instant::now();
            let _count = reactor
                .poll(&mut events, Some(Duration::from_secs(5)))
                .expect("poll failed");

            // Should return quickly, not wait 5 seconds
            assert!(start.elapsed() < Duration::from_secs(1));
        });
    }

    #[test]
    fn poll_timeout() {
        let reactor = EpollReactor::new().expect("failed to create reactor");
        let mut events = Events::with_capacity(64);

        let start = std::time::Instant::now();
        let count = reactor
            .poll(&mut events, Some(Duration::from_millis(50)))
            .expect("poll failed");

        // Should return after ~50ms with no events
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(40)); // Allow some tolerance
        assert!(elapsed < Duration::from_millis(200));
        assert_eq!(count, 0);
    }

    #[test]
    fn poll_non_blocking() {
        let reactor = EpollReactor::new().expect("failed to create reactor");
        let mut events = Events::with_capacity(64);

        let start = std::time::Instant::now();
        let count = reactor
            .poll(&mut events, Some(Duration::ZERO))
            .expect("poll failed");

        // Should return immediately
        assert!(start.elapsed() < Duration::from_millis(10));
        assert_eq!(count, 0);
    }

    #[test]
    fn poll_writable() {
        let reactor = EpollReactor::new().expect("failed to create reactor");
        let (sock1, _sock2) = UnixStream::pair().expect("failed to create unix stream pair");

        let token = Token::new(1);
        reactor
            .register(&sock1, token, Interest::WRITABLE)
            .expect("register failed");

        let mut events = Events::with_capacity(64);
        let count = reactor
            .poll(&mut events, Some(Duration::from_millis(100)))
            .expect("poll failed");

        // Socket should be immediately writable
        assert!(count >= 1);

        let mut found = false;
        for event in events.iter() {
            if event.token == token && event.is_writable() {
                found = true;
                break;
            }
        }
        assert!(found, "expected writable event for token");

        reactor.deregister(token).expect("deregister failed");
    }

    #[test]
    fn poll_readable() {
        use std::io::Write;

        let reactor = EpollReactor::new().expect("failed to create reactor");
        let (sock1, mut sock2) = UnixStream::pair().expect("failed to create unix stream pair");

        let token = Token::new(1);
        reactor
            .register(&sock1, token, Interest::READABLE)
            .expect("register failed");

        // Write some data to make sock1 readable
        sock2.write_all(b"hello").expect("write failed");

        let mut events = Events::with_capacity(64);
        let count = reactor
            .poll(&mut events, Some(Duration::from_millis(100)))
            .expect("poll failed");

        // Socket should be readable now
        assert!(count >= 1);

        let mut found = false;
        for event in events.iter() {
            if event.token == token && event.is_readable() {
                found = true;
                break;
            }
        }
        assert!(found, "expected readable event for token");

        reactor.deregister(token).expect("deregister failed");
    }

    #[test]
    fn duplicate_register_fails() {
        let reactor = EpollReactor::new().expect("failed to create reactor");
        let (sock1, _sock2) = UnixStream::pair().expect("failed to create unix stream pair");

        let token = Token::new(1);
        reactor
            .register(&sock1, token, Interest::READABLE)
            .expect("first register should succeed");

        // Second registration with same token should fail
        let result = reactor.register(&sock1, token, Interest::WRITABLE);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::AlreadyExists);

        reactor.deregister(token).expect("deregister failed");
    }

    #[test]
    fn multiple_registrations() {
        let reactor = EpollReactor::new().expect("failed to create reactor");

        let (sock1, _) = UnixStream::pair().expect("failed to create unix stream pair");
        let (sock2, _) = UnixStream::pair().expect("failed to create unix stream pair");
        let (sock3, _) = UnixStream::pair().expect("failed to create unix stream pair");

        reactor
            .register(&sock1, Token::new(1), Interest::READABLE)
            .expect("register 1 failed");
        reactor
            .register(&sock2, Token::new(2), Interest::WRITABLE)
            .expect("register 2 failed");
        reactor
            .register(&sock3, Token::new(3), Interest::both())
            .expect("register 3 failed");

        assert_eq!(reactor.registration_count(), 3);

        reactor.deregister(Token::new(2)).expect("deregister failed");
        assert_eq!(reactor.registration_count(), 2);

        reactor.deregister(Token::new(1)).expect("deregister failed");
        reactor.deregister(Token::new(3)).expect("deregister failed");
        assert_eq!(reactor.registration_count(), 0);
    }

    #[test]
    fn interest_to_poll_event_mapping() {
        // Test readable
        let event = EpollReactor::interest_to_poll_event(Token::new(1), Interest::READABLE);
        assert!(event.readable);
        assert!(!event.writable);

        // Test writable
        let event = EpollReactor::interest_to_poll_event(Token::new(2), Interest::WRITABLE);
        assert!(!event.readable);
        assert!(event.writable);

        // Test both
        let event = EpollReactor::interest_to_poll_event(Token::new(3), Interest::both());
        assert!(event.readable);
        assert!(event.writable);

        // Test none
        let event = EpollReactor::interest_to_poll_event(Token::new(4), Interest::NONE);
        assert!(!event.readable);
        assert!(!event.writable);
    }

    #[test]
    fn poll_event_to_interest_mapping() {
        let event = PollEvent::all(1);
        let interest = EpollReactor::poll_event_to_interest(&event);
        assert!(interest.is_readable());
        assert!(interest.is_writable());

        let event = PollEvent::readable(2);
        let interest = EpollReactor::poll_event_to_interest(&event);
        assert!(interest.is_readable());
        assert!(!interest.is_writable());

        let event = PollEvent::writable(3);
        let interest = EpollReactor::poll_event_to_interest(&event);
        assert!(!interest.is_readable());
        assert!(interest.is_writable());
    }

    #[test]
    fn debug_impl() {
        let reactor = EpollReactor::new().expect("failed to create reactor");
        let debug = format!("{:?}", reactor);
        assert!(debug.contains("EpollReactor"));
        assert!(debug.contains("registration_count"));
    }
}
