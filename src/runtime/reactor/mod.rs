//! Reactor abstraction for I/O event multiplexing.

pub mod interest;
pub mod lab;
mod registration;
pub mod source;
pub mod token;

pub use interest::Interest;
pub use lab::LabReactor;
pub use registration::Registration;
// ReactorHandle will be used by reactor implementations
#[allow(unused_imports)]
pub(crate) use registration::ReactorHandle;
pub use source::{next_source_id, Source, SourceId, SourceWrapper};
pub use token::{SlabToken, TokenSlab};

use std::io;
use std::time::Duration;

/// Token identifying a registered source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Token(pub usize);

impl Token {
    /// Creates a new token.
    #[must_use]
    pub const fn new(val: usize) -> Self {
        Self(val)
    }
}

/// I/O event from the reactor.
///
/// Represents a single readiness notification for a registered source.
///
/// # Example
///
/// ```ignore
/// use asupersync::runtime::reactor::{Event, Interest, Token};
///
/// let event = Event::new(Token::new(1), Interest::READABLE | Interest::WRITABLE);
/// assert!(event.is_readable());
/// assert!(event.is_writable());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Event {
    /// Token identifying the registered source.
    pub token: Token,
    /// Readiness flags that triggered.
    pub ready: Interest,
}

impl Event {
    /// Creates a new event with specified token and readiness flags.
    #[must_use]
    pub const fn new(token: Token, ready: Interest) -> Self {
        Self { token, ready }
    }

    /// Creates a readable event.
    #[must_use]
    pub const fn readable(token: Token) -> Self {
        Self {
            token,
            ready: Interest::READABLE,
        }
    }

    /// Creates a writable event.
    #[must_use]
    pub const fn writable(token: Token) -> Self {
        Self {
            token,
            ready: Interest::WRITABLE,
        }
    }

    /// Creates an error event.
    #[must_use]
    pub const fn errored(token: Token) -> Self {
        Self {
            token,
            ready: Interest::ERROR,
        }
    }

    /// Creates a hangup event.
    #[must_use]
    pub const fn hangup(token: Token) -> Self {
        Self {
            token,
            ready: Interest::HUP,
        }
    }

    /// Returns true if the source is readable.
    #[must_use]
    pub const fn is_readable(&self) -> bool {
        self.ready.is_readable()
    }

    /// Returns true if the source is writable.
    #[must_use]
    pub const fn is_writable(&self) -> bool {
        self.ready.is_writable()
    }

    /// Returns true if an error was reported.
    #[must_use]
    pub const fn is_error(&self) -> bool {
        self.ready.is_error()
    }

    /// Returns true if the source reported hangup.
    #[must_use]
    pub const fn is_hangup(&self) -> bool {
        self.ready.is_hup()
    }
}

/// Container for I/O events returned by poll().
///
/// Re-use across poll() calls to avoid allocation.
///
/// # Example
///
/// ```ignore
/// use asupersync::runtime::reactor::Events;
///
/// let mut events = Events::with_capacity(64);
/// // ... poll ...
/// for event in &events {
///     println!("Token {:?} is ready: {:?}", event.token, event.ready);
/// }
/// events.clear();
/// ```
#[derive(Debug)]
pub struct Events {
    inner: Vec<Event>,
    capacity: usize,
}

impl Events {
    /// Creates a new events buffer with the given capacity.
    ///
    /// The capacity limits the maximum number of events that can be stored.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Vec::with_capacity(capacity),
            capacity,
        }
    }

    /// Clears all events, maintaining capacity.
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Pushes an event.
    ///
    /// Events beyond capacity are silently dropped to prevent unbounded growth.
    pub(crate) fn push(&mut self, event: Event) {
        if self.inner.len() < self.capacity {
            self.inner.push(event);
        }
    }

    /// Returns the number of events.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if no events are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns the capacity (maximum number of events).
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Iterates over events.
    pub fn iter(&self) -> std::slice::Iter<'_, Event> {
        self.inner.iter()
    }
}

impl<'a> IntoIterator for &'a Events {
    type Item = &'a Event;
    type IntoIter = std::slice::Iter<'a, Event>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl IntoIterator for Events {
    type Item = Event;
    type IntoIter = std::vec::IntoIter<Event>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}


/// Trait for an I/O reactor.
///
/// Reactors provide platform-specific I/O event notification (epoll, kqueue, IOCP).
/// Sources are registered with interest flags and receive events through polling.
pub trait Reactor: Send + Sync {
    /// Registers interest in I/O events for a source.
    ///
    /// # Arguments
    ///
    /// * `source` - The I/O source to register
    /// * `token` - A unique token to identify this registration in events
    /// * `interest` - The events to monitor (readable, writable)
    ///
    /// # Errors
    ///
    /// Returns an error if registration fails (e.g., invalid fd, too many registrations).
    fn register(&self, source: &dyn Source, token: Token, interest: Interest) -> io::Result<()>;

    /// Deregisters a previously registered source.
    ///
    /// # Arguments
    ///
    /// * `source` - The I/O source to deregister
    /// * `token` - The token used during registration
    ///
    /// # Errors
    ///
    /// Returns an error if deregistration fails (e.g., source not registered).
    fn deregister(&self, source: &dyn Source, token: Token) -> io::Result<()>;

    /// Polls for I/O events, blocking up to `timeout`.
    ///
    /// # Arguments
    ///
    /// * `events` - Buffer to store received events
    /// * `timeout` - Maximum time to wait, or None for indefinite wait
    ///
    /// # Returns
    ///
    /// The number of events received.
    ///
    /// # Errors
    ///
    /// Returns an error if polling fails (e.g., interrupted).
    fn poll(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<usize>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // Event tests
    #[test]
    fn event_new() {
        let event = Event::new(Token::new(42), Interest::READABLE | Interest::WRITABLE);
        assert_eq!(event.token.0, 42);
        assert!(event.is_readable());
        assert!(event.is_writable());
        assert!(!event.is_error());
        assert!(!event.is_hangup());
    }

    #[test]
    fn event_readable() {
        let event = Event::readable(Token::new(1));
        assert!(event.is_readable());
        assert!(!event.is_writable());
        assert!(!event.is_error());
        assert!(!event.is_hangup());
    }

    #[test]
    fn event_writable() {
        let event = Event::writable(Token::new(2));
        assert!(!event.is_readable());
        assert!(event.is_writable());
        assert!(!event.is_error());
        assert!(!event.is_hangup());
    }

    #[test]
    fn event_errored() {
        let event = Event::errored(Token::new(3));
        assert!(!event.is_readable());
        assert!(!event.is_writable());
        assert!(event.is_error());
        assert!(!event.is_hangup());
    }

    #[test]
    fn event_hangup() {
        let event = Event::hangup(Token::new(4));
        assert!(!event.is_readable());
        assert!(!event.is_writable());
        assert!(!event.is_error());
        assert!(event.is_hangup());
    }

    #[test]
    fn event_combined_flags() {
        let event = Event::new(
            Token::new(5),
            Interest::READABLE | Interest::ERROR | Interest::HUP,
        );
        assert!(event.is_readable());
        assert!(!event.is_writable());
        assert!(event.is_error());
        assert!(event.is_hangup());
    }

    // Events container tests
    #[test]
    fn events_with_capacity() {
        let events = Events::with_capacity(64);
        assert_eq!(events.capacity(), 64);
        assert_eq!(events.len(), 0);
        assert!(events.is_empty());
    }

    #[test]
    fn events_push_and_iterate() {
        let mut events = Events::with_capacity(10);
        events.push(Event::readable(Token::new(1)));
        events.push(Event::writable(Token::new(2)));
        events.push(Event::errored(Token::new(3)));

        assert_eq!(events.len(), 3);
        assert!(!events.is_empty());

        let tokens: Vec<usize> = events.iter().map(|e| e.token.0).collect();
        assert_eq!(tokens, vec![1, 2, 3]);
    }

    #[test]
    fn events_clear() {
        let mut events = Events::with_capacity(10);
        events.push(Event::readable(Token::new(1)));
        events.push(Event::readable(Token::new(2)));

        assert_eq!(events.len(), 2);
        events.clear();
        assert_eq!(events.len(), 0);
        assert!(events.is_empty());
        // Capacity is maintained
        assert_eq!(events.capacity(), 10);
    }

    #[test]
    fn events_capacity_limit_respected() {
        let mut events = Events::with_capacity(3);
        events.push(Event::readable(Token::new(1)));
        events.push(Event::readable(Token::new(2)));
        events.push(Event::readable(Token::new(3)));
        // This should be silently dropped
        events.push(Event::readable(Token::new(4)));
        events.push(Event::readable(Token::new(5)));

        assert_eq!(events.len(), 3);
        assert_eq!(events.capacity(), 3);

        let tokens: Vec<usize> = events.iter().map(|e| e.token.0).collect();
        assert_eq!(tokens, vec![1, 2, 3]);
    }

    #[test]
    fn events_into_iter_ref() {
        let mut events = Events::with_capacity(10);
        events.push(Event::readable(Token::new(1)));
        events.push(Event::writable(Token::new(2)));

        let mut count = 0;
        for event in &events {
            assert!(event.is_readable() || event.is_writable());
            count += 1;
        }
        assert_eq!(count, 2);
    }

    #[test]
    fn events_into_iter_owned() {
        let mut events = Events::with_capacity(10);
        events.push(Event::readable(Token::new(1)));
        events.push(Event::writable(Token::new(2)));

        let collected: Vec<Event> = events.into_iter().collect();
        assert_eq!(collected.len(), 2);
        assert!(collected[0].is_readable());
        assert!(collected[1].is_writable());
    }

    #[test]
    fn events_zero_capacity() {
        let mut events = Events::with_capacity(0);
        assert_eq!(events.capacity(), 0);
        assert_eq!(events.len(), 0);

        // Should be silently dropped
        events.push(Event::readable(Token::new(1)));
        assert_eq!(events.len(), 0);
    }

    // Token tests
    #[test]
    fn token_new() {
        let token = Token::new(123);
        assert_eq!(token.0, 123);
    }

    #[test]
    fn token_equality() {
        let t1 = Token::new(1);
        let t2 = Token::new(1);
        let t3 = Token::new(2);

        assert_eq!(t1, t2);
        assert_ne!(t1, t3);
    }

    #[test]
    fn token_ordering() {
        let t1 = Token::new(1);
        let t2 = Token::new(2);

        assert!(t1 < t2);
        assert!(t2 > t1);
    }
}
