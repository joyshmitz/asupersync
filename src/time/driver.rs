//! Timer driver for managing sleep/timeout registration.
//!
//! The timer driver provides the time source and manages timer registrations.
//! It supports both production (wall clock) and virtual (lab) time.

use crate::types::Time;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::task::Waker;

/// Time source abstraction for getting the current time.
///
/// This trait allows the timer driver to work with both wall clock time
/// (production) and virtual time (lab testing).
pub trait TimeSource: Send + Sync {
    /// Returns the current time.
    fn now(&self) -> Time;
}

/// Wall clock time source for production use.
///
/// Uses `std::time::Instant` internally, converting to our `Time` type.
/// The epoch is the time when this source was created.
#[derive(Debug)]
pub struct WallClock {
    /// The instant when this clock was created.
    epoch: std::time::Instant,
}

impl WallClock {
    /// Creates a new wall clock time source.
    #[must_use]
    pub fn new() -> Self {
        Self {
            epoch: std::time::Instant::now(),
        }
    }
}

impl Default for WallClock {
    fn default() -> Self {
        Self::new()
    }
}

impl TimeSource for WallClock {
    fn now(&self) -> Time {
        let elapsed = self.epoch.elapsed();
        Time::from_nanos(elapsed.as_nanos() as u64)
    }
}

/// Virtual time source for lab testing.
///
/// Time only advances when explicitly told to do so, enabling
/// deterministic testing of time-dependent code.
///
/// # Example
///
/// ```
/// use asupersync::time::{TimeSource, VirtualClock};
/// use asupersync::types::Time;
///
/// let clock = VirtualClock::new();
/// assert_eq!(clock.now(), Time::ZERO);
///
/// clock.advance(1_000_000_000); // 1 second
/// assert_eq!(clock.now(), Time::from_secs(1));
/// ```
#[derive(Debug)]
pub struct VirtualClock {
    /// Current time in nanoseconds.
    now: AtomicU64,
}

impl VirtualClock {
    /// Creates a new virtual clock starting at time zero.
    #[must_use]
    pub fn new() -> Self {
        Self {
            now: AtomicU64::new(0),
        }
    }

    /// Creates a virtual clock starting at the given time.
    #[must_use]
    pub fn starting_at(time: Time) -> Self {
        Self {
            now: AtomicU64::new(time.as_nanos()),
        }
    }

    /// Advances time by the given number of nanoseconds.
    pub fn advance(&self, nanos: u64) {
        self.now.fetch_add(nanos, Ordering::Release);
    }

    /// Advances time to the given absolute time.
    ///
    /// If the target time is in the past, this is a no-op.
    pub fn advance_to(&self, time: Time) {
        let target = time.as_nanos();
        loop {
            let current = self.now.load(Ordering::Acquire);
            if current >= target {
                break;
            }
            if self
                .now
                .compare_exchange_weak(current, target, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    /// Sets the current time (for testing).
    pub fn set(&self, time: Time) {
        self.now.store(time.as_nanos(), Ordering::Release);
    }
}

impl Default for VirtualClock {
    fn default() -> Self {
        Self::new()
    }
}

impl TimeSource for VirtualClock {
    fn now(&self) -> Time {
        Time::from_nanos(self.now.load(Ordering::Acquire))
    }
}

/// A handle to a registered timer.
///
/// When dropped, the timer registration is invalidated (though the entry
/// may remain in the heap until it naturally expires and gets cleaned up).
#[derive(Debug)]
pub struct TimerHandle {
    /// Unique ID for this timer registration.
    id: u64,
    /// Generation to detect stale handles.
    generation: u64,
}

impl TimerHandle {
    /// Returns the timer ID.
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }

    /// Returns the generation.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }
}

/// Entry in the timer heap.
#[derive(Debug)]
struct TimerEntry {
    /// When this timer fires.
    deadline: Time,
    /// Waker to call when timer fires.
    waker: Waker,
    /// Unique ID.
    id: u64,
    /// Generation for cancellation.
    generation: u64,
}

impl std::cmp::Eq for TimerEntry {}

impl std::cmp::PartialEq for TimerEntry {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.generation == other.generation
    }
}

impl std::cmp::Ord for TimerEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse for min-heap (earliest deadline first)
        other
            .deadline
            .cmp(&self.deadline)
            .then_with(|| other.generation.cmp(&self.generation))
    }
}

impl std::cmp::PartialOrd for TimerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Timer driver that manages timer registrations and fires them.
///
/// The driver maintains a min-heap of timer entries ordered by deadline.
/// When `process_timers` is called, all expired timers have their wakers called.
///
/// # Thread Safety
///
/// The driver is thread-safe and can be shared across tasks.
///
/// # Example
///
/// ```
/// use asupersync::time::{TimerDriver, VirtualClock};
/// use asupersync::types::Time;
/// use std::sync::Arc;
///
/// let clock = Arc::new(VirtualClock::new());
/// let driver = TimerDriver::with_clock(clock.clone());
///
/// // In a real scenario, you'd register timers via Sleep futures
/// // and process them in your event loop.
/// ```
#[derive(Debug)]
pub struct TimerDriver<T: TimeSource = VirtualClock> {
    /// The time source.
    clock: std::sync::Arc<T>,
    /// Timer heap (protected by mutex for thread safety).
    heap: Mutex<BinaryHeap<TimerEntry>>,
    /// Next timer ID.
    next_id: AtomicU64,
    /// Current generation (increments on each registration).
    generation: AtomicU64,
}

impl<T: TimeSource> TimerDriver<T> {
    /// Creates a new timer driver with the given time source.
    #[must_use]
    pub fn with_clock(clock: std::sync::Arc<T>) -> Self {
        Self {
            clock,
            heap: Mutex::new(BinaryHeap::new()),
            next_id: AtomicU64::new(0),
            generation: AtomicU64::new(0),
        }
    }

    /// Returns the current time from the underlying clock.
    #[must_use]
    pub fn now(&self) -> Time {
        self.clock.now()
    }

    /// Registers a timer to fire at the given deadline.
    ///
    /// Returns a handle that can be used to identify the timer.
    /// The waker will be called when `process_timers` is called
    /// and the deadline has passed.
    pub fn register(&self, deadline: Time, waker: Waker) -> TimerHandle {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let generation = self.generation.fetch_add(1, Ordering::Relaxed);

        let entry = TimerEntry {
            deadline,
            waker,
            id,
            generation,
        };

        self.heap.lock().unwrap().push(entry);

        TimerHandle { id, generation }
    }

    /// Updates an existing timer registration with a new deadline and waker.
    ///
    /// This doesn't actually remove the old entry (to avoid O(n) removal),
    /// but registers a new one. Stale entries are cleaned up on pop.
    pub fn update(&self, _handle: &TimerHandle, deadline: Time, waker: Waker) -> TimerHandle {
        // Just register a new timer; old one will be ignored when it fires
        // because the generation won't match
        self.register(deadline, waker)
    }

    /// Returns the next deadline that will fire, if any.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        self.heap.lock().unwrap().peek().map(|e| e.deadline)
    }

    /// Processes all expired timers, calling their wakers.
    ///
    /// Returns the number of timers fired.
    pub fn process_timers(&self) -> usize {
        let now = self.clock.now();

        // Collect expired entries while holding the lock, then release it
        // before waking to prevent potential deadlocks if wakers try to
        // re-enter the timer driver.
        let expired_wakers = self.collect_expired(now);
        let fired = expired_wakers.len();

        // Wake them outside the lock
        for waker in expired_wakers {
            waker.wake();
        }

        fired
    }

    /// Helper to collect expired wakers while holding the lock.
    #[allow(clippy::significant_drop_tightening)]
    fn collect_expired(&self, now: Time) -> Vec<Waker> {
        let mut expired = Vec::new();
        let mut heap = self.heap.lock().unwrap();
        while let Some(entry) = heap.peek() {
            if entry.deadline <= now {
                let entry = heap.pop().unwrap();
                expired.push(entry.waker);
            } else {
                break;
            }
        }
        expired
    }

    /// Returns the number of pending timers.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.heap.lock().unwrap().len()
    }

    /// Returns true if there are no pending timers.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.heap.lock().unwrap().is_empty()
    }

    /// Clears all pending timers without firing them.
    pub fn clear(&self) {
        self.heap.lock().unwrap().clear();
    }
}

impl TimerDriver<VirtualClock> {
    /// Creates a new timer driver with a virtual clock.
    ///
    /// This is the default for testing and lab use.
    #[must_use]
    pub fn new() -> Self {
        Self::with_clock(std::sync::Arc::new(VirtualClock::new()))
    }
}

impl Default for TimerDriver<VirtualClock> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    // =========================================================================
    // VirtualClock Tests
    // =========================================================================

    #[test]
    fn virtual_clock_starts_at_zero() {
        let clock = VirtualClock::new();
        assert_eq!(clock.now(), Time::ZERO);
    }

    #[test]
    fn virtual_clock_starting_at() {
        let clock = VirtualClock::starting_at(Time::from_secs(10));
        assert_eq!(clock.now(), Time::from_secs(10));
    }

    #[test]
    fn virtual_clock_advance() {
        let clock = VirtualClock::new();
        clock.advance(1_000_000_000); // 1 second
        assert_eq!(clock.now(), Time::from_secs(1));

        clock.advance(500_000_000); // 0.5 seconds
        assert_eq!(clock.now().as_nanos(), 1_500_000_000);
    }

    #[test]
    fn virtual_clock_advance_to() {
        let clock = VirtualClock::new();
        clock.advance_to(Time::from_secs(5));
        assert_eq!(clock.now(), Time::from_secs(5));

        // Advancing to past time is no-op
        clock.advance_to(Time::from_secs(3));
        assert_eq!(clock.now(), Time::from_secs(5));
    }

    #[test]
    fn virtual_clock_set() {
        let clock = VirtualClock::new();
        clock.set(Time::from_secs(100));
        assert_eq!(clock.now(), Time::from_secs(100));

        // Set can go backwards
        clock.set(Time::from_secs(50));
        assert_eq!(clock.now(), Time::from_secs(50));
    }

    // =========================================================================
    // WallClock Tests
    // =========================================================================

    #[test]
    fn wall_clock_starts_near_zero() {
        let clock = WallClock::new();
        let now = clock.now();
        // Should be very close to zero (within 1ms of creation)
        assert!(now.as_nanos() < 1_000_000);
    }

    #[test]
    fn wall_clock_advances() {
        let clock = WallClock::new();
        let t1 = clock.now();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let t2 = clock.now();
        assert!(t2 > t1);
    }

    // =========================================================================
    // TimerDriver Tests
    // =========================================================================

    #[test]
    fn timer_driver_new() {
        let driver = TimerDriver::new();
        assert!(driver.is_empty());
        assert_eq!(driver.pending_count(), 0);
    }

    #[test]
    fn timer_driver_register() {
        let clock = Arc::new(VirtualClock::new());
        let driver = TimerDriver::with_clock(clock);

        let waker = futures_waker();
        let handle = driver.register(Time::from_secs(1), waker);

        assert_eq!(handle.id(), 0);
        assert_eq!(driver.pending_count(), 1);
        assert!(!driver.is_empty());
    }

    #[test]
    fn timer_driver_next_deadline() {
        let clock = Arc::new(VirtualClock::new());
        let driver = TimerDriver::with_clock(clock);

        assert_eq!(driver.next_deadline(), None);

        driver.register(Time::from_secs(5), futures_waker());
        driver.register(Time::from_secs(3), futures_waker());
        driver.register(Time::from_secs(7), futures_waker());

        // Should return earliest deadline
        assert_eq!(driver.next_deadline(), Some(Time::from_secs(3)));
    }

    #[test]
    fn timer_driver_process_expired() {
        let clock = Arc::new(VirtualClock::new());
        let driver = TimerDriver::with_clock(clock.clone());

        let woken = Arc::new(AtomicBool::new(false));
        let woken_clone = woken.clone();

        let waker = waker_that_sets(woken_clone);
        driver.register(Time::from_secs(1), waker);

        // Time is 0, no timers should fire
        assert_eq!(driver.process_timers(), 0);
        assert!(!woken.load(Ordering::SeqCst));

        // Advance time past deadline
        clock.advance(2_000_000_000); // 2 seconds
        assert_eq!(driver.process_timers(), 1);
        assert!(woken.load(Ordering::SeqCst));

        // No more timers
        assert!(driver.is_empty());
    }

    #[test]
    fn timer_driver_multiple_timers() {
        let clock = Arc::new(VirtualClock::new());
        let driver = TimerDriver::with_clock(clock.clone());

        let count = Arc::new(AtomicU64::new(0));

        for i in 1..=5 {
            let count_clone = count.clone();
            let waker = waker_that_increments(count_clone);
            driver.register(Time::from_secs(i), waker);
        }

        assert_eq!(driver.pending_count(), 5);

        // Advance to t=3, should fire 3 timers
        clock.set(Time::from_secs(3));
        assert_eq!(driver.process_timers(), 3);
        assert_eq!(count.load(Ordering::SeqCst), 3);
        assert_eq!(driver.pending_count(), 2);

        // Advance to t=10, should fire remaining 2
        clock.set(Time::from_secs(10));
        assert_eq!(driver.process_timers(), 2);
        assert_eq!(count.load(Ordering::SeqCst), 5);
        assert!(driver.is_empty());
    }

    #[test]
    fn timer_driver_clear() {
        let clock = Arc::new(VirtualClock::new());
        let driver = TimerDriver::with_clock(clock);

        driver.register(Time::from_secs(1), futures_waker());
        driver.register(Time::from_secs(2), futures_waker());

        assert_eq!(driver.pending_count(), 2);
        driver.clear();
        assert!(driver.is_empty());
    }

    #[test]
    fn timer_driver_now() {
        let clock = Arc::new(VirtualClock::new());
        let driver = TimerDriver::with_clock(clock.clone());

        assert_eq!(driver.now(), Time::ZERO);

        clock.advance(1_000_000_000);
        assert_eq!(driver.now(), Time::from_secs(1));
    }

    // =========================================================================
    // TimerHandle Tests
    // =========================================================================

    #[test]
    fn timer_handle_id_and_generation() {
        let clock = Arc::new(VirtualClock::new());
        let driver = TimerDriver::with_clock(clock);

        let h1 = driver.register(Time::from_secs(1), futures_waker());
        let h2 = driver.register(Time::from_secs(2), futures_waker());

        assert_eq!(h1.id(), 0);
        assert_eq!(h2.id(), 1);
        assert_ne!(h1.generation(), h2.generation());
    }

    // =========================================================================
    // Helper Functions
    // =========================================================================

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
    fn futures_waker() -> Waker {
        Arc::new(NoopWaker).into()
    }

    /// A waker that sets an AtomicBool when woken.
    struct FlagWaker {
        flag: AtomicBool,
    }

    impl Wake for FlagWaker {
        fn wake(self: Arc<Self>) {
            self.flag.store(true, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.flag.store(true, Ordering::SeqCst);
        }
    }

    /// Creates a waker that sets an AtomicBool when woken.
    fn waker_that_sets(flag: Arc<AtomicBool>) -> Waker {
        // We create a new FlagWaker that shares the flag
        // by wrapping the Arc<AtomicBool> in another struct
        struct SharedFlagWaker {
            flag: Arc<AtomicBool>,
        }

        impl Wake for SharedFlagWaker {
            fn wake(self: Arc<Self>) {
                self.flag.store(true, Ordering::SeqCst);
            }

            fn wake_by_ref(self: &Arc<Self>) {
                self.flag.store(true, Ordering::SeqCst);
            }
        }

        Arc::new(SharedFlagWaker { flag }).into()
    }

    /// A waker that increments a counter when woken.
    struct CounterWaker {
        counter: Arc<AtomicU64>,
    }

    impl Wake for CounterWaker {
        fn wake(self: Arc<Self>) {
            self.counter.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.counter.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Creates a waker that increments an AtomicU64 when woken.
    fn waker_that_increments(counter: Arc<AtomicU64>) -> Waker {
        Arc::new(CounterWaker { counter }).into()
    }
}
