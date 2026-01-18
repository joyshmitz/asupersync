//! Instrumented future wrapper for await point tracking.
//!
//! The [`InstrumentedFuture`] wrapper tracks await points for cancellation
//! injection testing. It enables deterministic testing of cancel-correctness
//! by recording when futures yield and allowing cancellation to be injected
//! at specific await points.
//!
//! # Design
//!
//! ## Await Point Identification
//!
//! Each await point is identified by a monotonic counter within the future.
//! Combined with a task ID, this gives unique identification:
//! `(task_id, await_counter) → AwaitPoint`
//!
//! This approach is deterministic if the same code path is followed, which
//! is guaranteed in the Lab runtime with deterministic scheduling.
//!
//! ## Recording Mode
//!
//! In recording mode, the future tracks each poll invocation:
//! - Increments the await counter on each poll
//! - Records the await point to the injector
//!
//! ## Injection Mode
//!
//! In injection mode, the future checks if cancellation should be injected:
//! - On each poll, checks the injector for the target await point
//! - If matched, returns a cancellation result instead of polling the inner future
//!
//! # Example
//!
//! ```ignore
//! use asupersync::lab::instrumented_future::{InstrumentedFuture, CancellationInjector};
//!
//! // Create an injector that will cancel at await point 3
//! let injector = CancellationInjector::inject_at(3);
//!
//! // Wrap a future
//! let future = InstrumentedFuture::new(my_future, injector);
//!
//! // When polled, if await_counter reaches 3, it will trigger cancellation
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::types::TaskId;

/// Identifies a specific await point within a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AwaitPoint {
    /// The task this await point belongs to.
    pub task_id: Option<TaskId>,
    /// The sequential number of this await point (1-based).
    pub sequence: u64,
}

impl AwaitPoint {
    /// Creates a new await point identifier.
    #[must_use]
    pub const fn new(task_id: Option<TaskId>, sequence: u64) -> Self {
        Self { task_id, sequence }
    }

    /// Creates an await point without task association (for testing).
    #[must_use]
    pub const fn anonymous(sequence: u64) -> Self {
        Self {
            task_id: None,
            sequence,
        }
    }
}

/// Strategy for when to inject cancellation.
#[derive(Debug, Clone)]
pub enum InjectionStrategy {
    /// Never inject cancellation (recording mode).
    Never,
    /// Inject at a specific await point sequence number.
    AtSequence(u64),
    /// Inject at a specific await point.
    AtPoint(AwaitPoint),
    /// Inject at every Nth await point.
    EveryNth(u64),
}

impl Default for InjectionStrategy {
    fn default() -> Self {
        Self::Never
    }
}

/// Records await points and controls cancellation injection.
///
/// The injector has two modes:
/// - **Recording**: Tracks all await points reached (strategy = `Never`)
/// - **Injection**: Triggers cancellation at specific points
#[derive(Debug)]
pub struct CancellationInjector {
    /// The injection strategy.
    strategy: InjectionStrategy,
    /// Recorded await points (sequence numbers in order).
    recorded: parking_lot::Mutex<Vec<u64>>,
    /// Number of injections performed.
    injection_count: AtomicU64,
    /// Associated task ID (optional).
    task_id: Option<TaskId>,
}

impl CancellationInjector {
    /// Creates a new injector in recording mode.
    #[must_use]
    pub fn recording() -> Arc<Self> {
        Arc::new(Self {
            strategy: InjectionStrategy::Never,
            recorded: parking_lot::Mutex::new(Vec::new()),
            injection_count: AtomicU64::new(0),
            task_id: None,
        })
    }

    /// Creates an injector that injects at a specific sequence number.
    #[must_use]
    pub fn inject_at(sequence: u64) -> Arc<Self> {
        Arc::new(Self {
            strategy: InjectionStrategy::AtSequence(sequence),
            recorded: parking_lot::Mutex::new(Vec::new()),
            injection_count: AtomicU64::new(0),
            task_id: None,
        })
    }

    /// Creates an injector that injects at a specific await point.
    #[must_use]
    pub fn inject_at_point(point: AwaitPoint) -> Arc<Self> {
        Arc::new(Self {
            strategy: InjectionStrategy::AtPoint(point),
            recorded: parking_lot::Mutex::new(Vec::new()),
            injection_count: AtomicU64::new(0),
            task_id: point.task_id,
        })
    }

    /// Creates an injector that injects at every Nth await point.
    #[must_use]
    pub fn inject_every_nth(n: u64) -> Arc<Self> {
        Arc::new(Self {
            strategy: InjectionStrategy::EveryNth(n),
            recorded: parking_lot::Mutex::new(Vec::new()),
            injection_count: AtomicU64::new(0),
            task_id: None,
        })
    }

    /// Creates an injector with a specific strategy.
    #[must_use]
    pub fn with_strategy(strategy: InjectionStrategy) -> Arc<Self> {
        Arc::new(Self {
            strategy,
            recorded: parking_lot::Mutex::new(Vec::new()),
            injection_count: AtomicU64::new(0),
            task_id: None,
        })
    }

    /// Sets the associated task ID.
    pub fn set_task_id(&mut self, task_id: TaskId) {
        self.task_id = Some(task_id);
    }

    /// Records an await point.
    pub fn record_await(&self, sequence: u64) {
        self.recorded.lock().push(sequence);
    }

    /// Checks if cancellation should be injected at this await point.
    #[must_use]
    pub fn should_inject_at(&self, sequence: u64) -> bool {
        match &self.strategy {
            InjectionStrategy::Never => false,
            InjectionStrategy::AtSequence(target) => {
                if sequence == *target {
                    self.injection_count.fetch_add(1, Ordering::SeqCst);
                    true
                } else {
                    false
                }
            }
            InjectionStrategy::AtPoint(point) => {
                // If task_id doesn't match, don't inject
                if point.task_id.is_some() && point.task_id != self.task_id {
                    return false;
                }
                if sequence == point.sequence {
                    self.injection_count.fetch_add(1, Ordering::SeqCst);
                    true
                } else {
                    false
                }
            }
            InjectionStrategy::EveryNth(n) => {
                if *n > 0 && sequence % *n == 0 {
                    self.injection_count.fetch_add(1, Ordering::SeqCst);
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Returns the recorded await points.
    #[must_use]
    pub fn recorded_points(&self) -> Vec<u64> {
        self.recorded.lock().clone()
    }

    /// Returns the number of injections performed.
    #[must_use]
    pub fn injection_count(&self) -> u64 {
        self.injection_count.load(Ordering::SeqCst)
    }

    /// Clears recorded await points.
    pub fn clear_recorded(&self) {
        self.recorded.lock().clear();
    }
}

/// The result of polling an instrumented future when cancellation is injected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstrumentedPollResult<T> {
    /// The inner future returned this result.
    Inner(T),
    /// Cancellation was injected at this await point.
    CancellationInjected(u64),
}

/// A future wrapper that tracks await points for cancellation injection.
///
/// This wrapper instruments a future to:
/// 1. Count poll invocations (await points)
/// 2. Record await points to a [`CancellationInjector`]
/// 3. Optionally inject cancellation at specific await points
///
/// # Type Parameters
///
/// * `F` - The inner future type
#[pin_project::pin_project]
pub struct InstrumentedFuture<F> {
    /// The wrapped future.
    #[pin]
    inner: F,
    /// The cancellation injector.
    injector: Arc<CancellationInjector>,
    /// Current await point counter (1-based, incremented before each poll).
    await_counter: u64,
    /// Whether cancellation was injected.
    cancellation_injected: bool,
    /// The await point where cancellation was injected.
    injection_point: Option<u64>,
}

impl<F> InstrumentedFuture<F> {
    /// Creates a new instrumented future.
    #[must_use]
    pub fn new(inner: F, injector: Arc<CancellationInjector>) -> Self {
        Self {
            inner,
            injector,
            await_counter: 0,
            cancellation_injected: false,
            injection_point: None,
        }
    }

    /// Creates an instrumented future in recording mode.
    #[must_use]
    pub fn recording(inner: F) -> Self {
        Self::new(inner, CancellationInjector::recording())
    }

    /// Returns the current await counter value.
    #[must_use]
    pub fn await_count(&self) -> u64 {
        self.await_counter
    }

    /// Returns whether cancellation was injected.
    #[must_use]
    pub fn was_cancelled(&self) -> bool {
        self.cancellation_injected
    }

    /// Returns the await point where cancellation was injected.
    #[must_use]
    pub fn injection_point(&self) -> Option<u64> {
        self.injection_point
    }

    /// Returns a reference to the injector.
    #[must_use]
    pub fn injector(&self) -> &Arc<CancellationInjector> {
        &self.injector
    }
}

impl<F: Future> Future for InstrumentedFuture<F> {
    type Output = InstrumentedPollResult<F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        // If cancellation was already injected, return immediately
        if *this.cancellation_injected {
            return Poll::Ready(InstrumentedPollResult::CancellationInjected(
                this.injection_point.unwrap_or(0),
            ));
        }

        // Increment await counter before polling
        *this.await_counter += 1;
        let current_point = *this.await_counter;

        // Record this await point
        this.injector.record_await(current_point);

        // Check if we should inject cancellation here
        if this.injector.should_inject_at(current_point) {
            *this.cancellation_injected = true;
            *this.injection_point = Some(current_point);
            return Poll::Ready(InstrumentedPollResult::CancellationInjected(current_point));
        }

        // Poll the inner future
        match this.inner.poll(cx) {
            Poll::Ready(output) => Poll::Ready(InstrumentedPollResult::Inner(output)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::{Poll, Wake, Waker};

    /// A simple noop waker for testing.
    struct NoopWaker;

    impl Wake for NoopWaker {
        fn wake(self: Arc<Self>) {}
        fn wake_by_ref(self: &Arc<Self>) {}
    }

    /// Creates a noop waker for testing.
    fn noop_waker() -> Waker {
        Waker::from(Arc::new(NoopWaker))
    }

    /// A future that yields a specific number of times before completing.
    struct YieldingFuture {
        yields_remaining: u32,
        value: i32,
    }

    impl YieldingFuture {
        fn new(yields: u32, value: i32) -> Self {
            Self {
                yields_remaining: yields,
                value,
            }
        }
    }

    impl Future for YieldingFuture {
        type Output = i32;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.yields_remaining > 0 {
                self.yields_remaining -= 1;
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(self.value)
            }
        }
    }

    /// Helper to poll a pinned future to completion using a safe waker.
    fn poll_to_completion<F: Future>(future: F) -> F::Output {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut pinned = Box::pin(future);

        loop {
            match pinned.as_mut().poll(&mut cx) {
                Poll::Ready(output) => return output,
                Poll::Pending => {}
            }
        }
    }

    #[test]
    fn recording_mode_tracks_await_points() {
        let future = YieldingFuture::new(3, 42);
        let instrumented = InstrumentedFuture::recording(future);

        let result = poll_to_completion(instrumented);

        match result {
            InstrumentedPollResult::Inner(value) => assert_eq!(value, 42),
            InstrumentedPollResult::CancellationInjected(_) => {
                panic!("should not inject in recording mode")
            }
        }
    }

    #[test]
    fn recording_captures_all_await_points() {
        let future = YieldingFuture::new(3, 42);
        let injector = CancellationInjector::recording();
        let instrumented = InstrumentedFuture::new(future, injector.clone());

        let _ = poll_to_completion(instrumented);

        // 3 yields + 1 final completion = 4 polls
        let recorded = injector.recorded_points();
        assert_eq!(recorded, vec![1, 2, 3, 4]);
    }

    #[test]
    fn injection_at_specific_sequence() {
        let future = YieldingFuture::new(5, 42);
        let injector = CancellationInjector::inject_at(3);
        let instrumented = InstrumentedFuture::new(future, injector.clone());

        let result = poll_to_completion(instrumented);

        match result {
            InstrumentedPollResult::CancellationInjected(point) => {
                assert_eq!(point, 3);
            }
            InstrumentedPollResult::Inner(_) => {
                panic!("should have injected cancellation")
            }
        }

        // Should have recorded points 1, 2, 3 before cancellation
        let recorded = injector.recorded_points();
        assert_eq!(recorded, vec![1, 2, 3]);
        assert_eq!(injector.injection_count(), 1);
    }

    #[test]
    fn injection_every_nth() {
        let future = YieldingFuture::new(10, 42);
        let injector = CancellationInjector::inject_every_nth(4);
        let instrumented = InstrumentedFuture::new(future, injector.clone());

        let result = poll_to_completion(instrumented);

        match result {
            InstrumentedPollResult::CancellationInjected(point) => {
                assert_eq!(point, 4); // First multiple of 4
            }
            InstrumentedPollResult::Inner(_) => {
                panic!("should have injected cancellation")
            }
        }

        assert_eq!(injector.recorded_points(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn await_point_identification() {
        let task_id = TaskId::from_arena(crate::util::ArenaIndex::new(1, 0));
        let point = AwaitPoint::new(Some(task_id), 5);

        assert_eq!(point.task_id, Some(task_id));
        assert_eq!(point.sequence, 5);

        let anon = AwaitPoint::anonymous(10);
        assert_eq!(anon.task_id, None);
        assert_eq!(anon.sequence, 10);
    }

    #[test]
    fn instrumented_future_tracks_await_count() {
        let future = YieldingFuture::new(2, 42);
        let instrumented = InstrumentedFuture::recording(future);
        let mut pinned = Box::pin(instrumented);

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // First poll
        assert!(matches!(pinned.as_mut().poll(&mut cx), Poll::Pending));
        assert_eq!(pinned.await_count(), 1);

        // Second poll
        assert!(matches!(pinned.as_mut().poll(&mut cx), Poll::Pending));
        assert_eq!(pinned.await_count(), 2);

        // Third poll (completes)
        assert!(matches!(pinned.as_mut().poll(&mut cx), Poll::Ready(_)));
        assert_eq!(pinned.await_count(), 3);
    }

    #[test]
    fn cancellation_is_idempotent() {
        let future = YieldingFuture::new(5, 42);
        let injector = CancellationInjector::inject_at(2);
        let instrumented = InstrumentedFuture::new(future, injector);
        let mut pinned = Box::pin(instrumented);

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // First poll: pending
        assert!(matches!(pinned.as_mut().poll(&mut cx), Poll::Pending));
        assert!(!pinned.was_cancelled());

        // Second poll: cancellation injected
        let result = pinned.as_mut().poll(&mut cx);
        assert!(matches!(
            result,
            Poll::Ready(InstrumentedPollResult::CancellationInjected(2))
        ));
        assert!(pinned.was_cancelled());
        assert_eq!(pinned.injection_point(), Some(2));

        // Third poll: still cancelled (idempotent)
        let result = pinned.as_mut().poll(&mut cx);
        assert!(matches!(
            result,
            Poll::Ready(InstrumentedPollResult::CancellationInjected(2))
        ));
    }

    #[test]
    fn strategy_never_does_not_inject() {
        let injector = CancellationInjector::with_strategy(InjectionStrategy::Never);

        assert!(!injector.should_inject_at(1));
        assert!(!injector.should_inject_at(100));
        assert!(!injector.should_inject_at(1000));
    }

    #[test]
    fn clear_recorded_works() {
        let injector = CancellationInjector::recording();

        injector.record_await(1);
        injector.record_await(2);
        injector.record_await(3);

        assert_eq!(injector.recorded_points().len(), 3);

        injector.clear_recorded();

        assert!(injector.recorded_points().is_empty());
    }
}
