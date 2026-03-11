//! Hedge middleware layer.
//!
//! The [`HedgeLayer`] wraps a cloneable service to issue a backup (hedge)
//! request when the primary request takes too long. The first response
//! to complete is returned, reducing tail latency.
//!
//! This is a latency-optimisation technique from the paper "The Tail at
//! Scale" (Dean & Barroso, 2013).

use super::{Layer, Service};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

// ─── HedgeLayer ───────────────────────────────────────────────────────────

/// A layer that applies hedging to a service.
#[derive(Debug, Clone)]
pub struct HedgeLayer {
    config: HedgeConfig,
}

impl HedgeLayer {
    /// Create a new hedge layer with the given configuration.
    #[must_use]
    pub fn new(config: HedgeConfig) -> Self {
        Self { config }
    }

    /// Create a hedge layer with a fixed delay threshold.
    #[must_use]
    pub fn with_delay(delay: Duration) -> Self {
        Self::new(HedgeConfig::new(delay))
    }
}

impl<S: Clone> Layer<S> for HedgeLayer {
    type Service = Hedge<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Hedge::new(inner, self.config.clone())
    }
}

// ─── HedgeConfig ──────────────────────────────────────────────────────────

/// Configuration for the hedge middleware.
#[derive(Debug, Clone)]
pub struct HedgeConfig {
    /// Duration to wait before sending the hedge request.
    pub delay: Duration,
    /// Maximum number of outstanding hedge requests.
    pub max_pending: u32,
}

impl HedgeConfig {
    /// Create a new hedge configuration with the given delay.
    #[must_use]
    pub fn new(delay: Duration) -> Self {
        Self {
            delay,
            max_pending: 10,
        }
    }

    /// Set the maximum number of concurrent hedge requests.
    #[must_use]
    pub fn max_pending(mut self, max: u32) -> Self {
        self.max_pending = max;
        self
    }
}

// ─── HedgeError ───────────────────────────────────────────────────────────

/// Error from the hedge middleware.
#[derive(Debug)]
pub enum HedgeError<E> {
    /// The caller attempted `call()` without a preceding successful `poll_ready()`.
    NotReady,
    /// The inner service returned an error.
    Inner(E),
    /// Both primary and hedge requests failed.
    BothFailed {
        /// Error from the primary request.
        primary: E,
        /// Error from the hedge request.
        hedge: E,
    },
}

impl<E: fmt::Display> fmt::Display for HedgeError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotReady => write!(f, "poll_ready required before call"),
            Self::Inner(e) => write!(f, "service error: {e}"),
            Self::BothFailed { primary, .. } => {
                write!(f, "both primary and hedge failed: {primary}")
            }
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for HedgeError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotReady => None,
            Self::Inner(e) | Self::BothFailed { primary: e, .. } => Some(e),
        }
    }
}

// ─── Hedge service ────────────────────────────────────────────────────────

/// A service that hedges requests to reduce tail latency.
///
/// When a request takes longer than the configured delay, a second
/// (hedge) request is sent to the same service. The first response
/// to arrive is returned.
///
/// Each successful `poll_ready` authorizes exactly one subsequent `call`.
pub struct Hedge<S> {
    inner: S,
    config: HedgeConfig,
    stats: HedgeStats,
    /// Tracks whether this clone has observed readiness for one call.
    ready_observed: bool,
}

struct HedgeStats {
    /// Total requests processed.
    total: AtomicU64,
    /// Hedge requests sent.
    hedged: AtomicU64,
    /// Times the hedge response won.
    hedge_wins: AtomicU64,
}

impl<S> Hedge<S> {
    /// Create a new hedge service.
    #[must_use]
    pub fn new(inner: S, config: HedgeConfig) -> Self {
        Self {
            inner,
            config,
            stats: HedgeStats {
                total: AtomicU64::new(0),
                hedged: AtomicU64::new(0),
                hedge_wins: AtomicU64::new(0),
            },
            ready_observed: false,
        }
    }

    /// Get the configured delay threshold.
    #[must_use]
    pub fn delay(&self) -> Duration {
        self.config.delay
    }

    /// Get the maximum pending hedge limit.
    #[must_use]
    pub fn max_pending(&self) -> u32 {
        self.config.max_pending
    }

    /// Total requests processed.
    #[must_use]
    pub fn total_requests(&self) -> u64 {
        self.stats.total.load(Ordering::Relaxed)
    }

    /// Number of hedge requests sent.
    #[must_use]
    pub fn hedged_requests(&self) -> u64 {
        self.stats.hedged.load(Ordering::Relaxed)
    }

    /// Number of times the hedge response arrived first.
    #[must_use]
    pub fn hedge_wins(&self) -> u64 {
        self.stats.hedge_wins.load(Ordering::Relaxed)
    }

    /// Get the hedge rate (hedged / total).
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn hedge_rate(&self) -> f64 {
        let total = self.total_requests();
        if total == 0 {
            return 0.0;
        }
        self.hedged_requests() as f64 / total as f64
    }

    /// Record that a request was processed.
    pub fn record_request(&self) {
        self.stats.total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a hedge request was sent.
    pub fn record_hedge(&self) {
        self.stats.hedged.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that the hedge response won.
    pub fn record_hedge_win(&self) {
        self.stats.hedge_wins.fetch_add(1, Ordering::Relaxed);
    }

    /// Get a reference to the inner service.
    #[must_use]
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Get a mutable reference to the inner service.
    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// Get a reference to the configuration.
    #[must_use]
    pub fn config(&self) -> &HedgeConfig {
        &self.config
    }
}

impl<S: fmt::Debug> fmt::Debug for Hedge<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hedge")
            .field("inner", &self.inner)
            .field("delay", &self.config.delay)
            .field("max_pending", &self.config.max_pending)
            .field("total", &self.total_requests())
            .field("hedged", &self.hedged_requests())
            .field("hedge_wins", &self.hedge_wins())
            .finish_non_exhaustive()
    }
}

impl<S: Clone> Clone for Hedge<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            config: self.config.clone(),
            stats: HedgeStats {
                total: AtomicU64::new(0),
                hedged: AtomicU64::new(0),
                hedge_wins: AtomicU64::new(0),
            },
            // Readiness tickets are handle-local and must not be cloned.
            ready_observed: false,
        }
    }
}

impl<S, Request> Service<Request> for Hedge<S>
where
    S: Service<Request> + Clone,
    S::Future: Unpin,
    Request: Clone,
{
    type Response = S::Response;
    type Error = HedgeError<S::Error>;
    type Future = HedgeFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.inner.poll_ready(cx) {
            Poll::Ready(Ok(())) => {
                self.ready_observed = true;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => {
                self.ready_observed = false;
                Poll::Ready(Err(HedgeError::Inner(e)))
            }
            Poll::Pending => {
                self.ready_observed = false;
                Poll::Pending
            }
        }
    }

    fn call(&mut self, req: Request) -> Self::Future {
        if !std::mem::replace(&mut self.ready_observed, false) {
            return HedgeFuture::not_ready();
        }

        let fut = self.inner.call(req);
        self.record_request();
        HedgeFuture::inner(fut)
    }
}

/// Future returned by the [`Hedge`] service.
///
/// In Phase 0, this simply wraps the primary future. Full hedging
/// with timers requires async runtime support (Phase 1).
pub struct HedgeFuture<F> {
    state: HedgeFutureState<F>,
}

enum HedgeFutureState<F> {
    NotReady,
    Inner(F),
    Done,
}

impl<F> fmt::Debug for HedgeFuture<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HedgeFuture").finish()
    }
}

impl<F> HedgeFuture<F> {
    #[must_use]
    fn not_ready() -> Self {
        Self {
            state: HedgeFutureState::NotReady,
        }
    }

    #[must_use]
    fn inner(inner: F) -> Self {
        Self {
            state: HedgeFutureState::Inner(inner),
        }
    }
}

impl<F, T, E> Future for HedgeFuture<F>
where
    F: Future<Output = Result<T, E>> + Unpin,
{
    type Output = Result<T, HedgeError<E>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.state {
            HedgeFutureState::NotReady => {
                self.state = HedgeFutureState::Done;
                Poll::Ready(Err(HedgeError::NotReady))
            }
            HedgeFutureState::Inner(inner) => {
                let polled = Pin::new(inner).poll(cx);
                if polled.is_ready() {
                    self.state = HedgeFutureState::Done;
                }
                polled.map_err(HedgeError::Inner)
            }
            HedgeFutureState::Done => panic!("HedgeFuture polled after completion"),
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Wake, Waker};

    fn init_test(name: &str) {
        crate::test_utils::init_test_logging();
        crate::test_phase!(name);
    }

    struct NoopWaker;

    impl Wake for NoopWaker {
        fn wake(self: Arc<Self>) {}
        fn wake_by_ref(self: &Arc<Self>) {}
    }

    fn noop_waker() -> Waker {
        Arc::new(NoopWaker).into()
    }

    // ================================================================
    // HedgeConfig
    // ================================================================

    #[test]
    fn config_new() {
        init_test("config_new");
        let config = HedgeConfig::new(Duration::from_millis(100));
        assert_eq!(config.delay, Duration::from_millis(100));
        assert_eq!(config.max_pending, 10);
        crate::test_complete!("config_new");
    }

    #[test]
    fn config_max_pending() {
        let config = HedgeConfig::new(Duration::from_millis(50)).max_pending(5);
        assert_eq!(config.max_pending, 5);
    }

    #[test]
    fn config_debug_clone() {
        let config = HedgeConfig::new(Duration::from_millis(100));
        let dbg = format!("{config:?}");
        assert!(dbg.contains("HedgeConfig"));
        let cloned = config.clone();
        assert_eq!(cloned.delay, Duration::from_millis(100));
        assert_eq!(config.delay, Duration::from_millis(100));
    }

    // ================================================================
    // HedgeLayer
    // ================================================================

    #[test]
    fn layer_new() {
        let layer = HedgeLayer::new(HedgeConfig::new(Duration::from_millis(100)));
        let dbg = format!("{layer:?}");
        assert!(dbg.contains("HedgeLayer"));
    }

    #[test]
    fn layer_with_delay() {
        let layer = HedgeLayer::with_delay(Duration::from_millis(200));
        let dbg = format!("{layer:?}");
        assert!(dbg.contains("HedgeLayer"));
    }

    #[test]
    fn layer_clone() {
        let layer = HedgeLayer::with_delay(Duration::from_millis(100));
        let cloned = layer.clone();
        assert_eq!(cloned.config.delay, Duration::from_millis(100));
        assert_eq!(layer.config.delay, Duration::from_millis(100));
    }

    // ================================================================
    // Hedge service
    // ================================================================

    #[derive(Clone, Debug)]
    struct MockSvc;

    #[derive(Clone, Debug)]
    struct PanicOnCallService;

    #[derive(Clone, Debug)]
    struct RequiresReadyService {
        ready: bool,
        calls: Arc<AtomicUsize>,
    }

    impl Service<u32> for PanicOnCallService {
        type Response = ();
        type Error = ();
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: u32) -> Self::Future {
            panic!("panic during hedge call construction");
        }
    }

    impl Service<u32> for RequiresReadyService {
        type Response = u32;
        type Error = &'static str;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.ready = true;
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: u32) -> Self::Future {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let was_ready = std::mem::replace(&mut self.ready, false);
            if was_ready {
                std::future::ready(Ok(req))
            } else {
                std::future::ready(Err("not ready"))
            }
        }
    }

    #[test]
    fn hedge_new() {
        init_test("hedge_new");
        let hedge = Hedge::new(MockSvc, HedgeConfig::new(Duration::from_millis(100)));
        assert_eq!(hedge.delay(), Duration::from_millis(100));
        assert_eq!(hedge.max_pending(), 10);
        assert_eq!(hedge.total_requests(), 0);
        assert_eq!(hedge.hedged_requests(), 0);
        assert_eq!(hedge.hedge_wins(), 0);
        assert!((hedge.hedge_rate() - 0.0).abs() < f64::EPSILON);
        crate::test_complete!("hedge_new");
    }

    #[test]
    fn hedge_stats() {
        init_test("hedge_stats");
        let hedge = Hedge::new(MockSvc, HedgeConfig::new(Duration::from_millis(100)));
        hedge.record_request();
        hedge.record_request();
        hedge.record_hedge();
        hedge.record_hedge_win();
        assert_eq!(hedge.total_requests(), 2);
        assert_eq!(hedge.hedged_requests(), 1);
        assert_eq!(hedge.hedge_wins(), 1);
        assert!((hedge.hedge_rate() - 0.5).abs() < f64::EPSILON);
        crate::test_complete!("hedge_stats");
    }

    #[test]
    fn hedge_call_panic_does_not_overcount_total_requests() {
        init_test("hedge_call_panic_does_not_overcount_total_requests");
        let mut hedge = Hedge::new(
            PanicOnCallService,
            HedgeConfig::new(Duration::from_millis(100)),
        );
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let ready = hedge.poll_ready(&mut cx);
        assert!(matches!(ready, Poll::Ready(Ok(()))));

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _f = hedge.call(7);
        }));
        let panicked = panic.is_err();
        crate::assert_with_log!(panicked, "inner call panicked", true, panicked);

        let total = hedge.total_requests();
        crate::assert_with_log!(total == 0, "total requests", 0, total);
        crate::test_complete!("hedge_call_panic_does_not_overcount_total_requests");
    }

    #[test]
    fn hedge_call_without_poll_ready_fails_closed() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut hedge = Hedge::new(
            RequiresReadyService {
                ready: false,
                calls: Arc::clone(&calls),
            },
            HedgeConfig::new(Duration::from_millis(100)),
        );
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let mut fut = hedge.call(7);
        let result = Pin::new(&mut fut).poll(&mut cx);
        assert!(matches!(result, Poll::Ready(Err(HedgeError::NotReady))));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(hedge.total_requests(), 0);
    }

    #[test]
    fn hedge_ready_window_is_consumed_by_call() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut hedge = Hedge::new(
            RequiresReadyService {
                ready: false,
                calls: Arc::clone(&calls),
            },
            HedgeConfig::new(Duration::from_millis(100)),
        );
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let first_ready = hedge.poll_ready(&mut cx);
        assert!(matches!(first_ready, Poll::Ready(Ok(()))));

        let mut first = hedge.call(11);
        let first_result = Pin::new(&mut first).poll(&mut cx);
        assert!(matches!(first_result, Poll::Ready(Ok(11))));

        let mut second = hedge.call(22);
        let second_result = Pin::new(&mut second).poll(&mut cx);
        assert!(matches!(
            second_result,
            Poll::Ready(Err(HedgeError::NotReady))
        ));

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(hedge.total_requests(), 1);
    }

    #[test]
    fn hedge_clone_does_not_inherit_ready_window() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut hedge = Hedge::new(
            RequiresReadyService {
                ready: false,
                calls: Arc::clone(&calls),
            },
            HedgeConfig::new(Duration::from_millis(100)),
        );
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let ready = hedge.poll_ready(&mut cx);
        assert!(matches!(ready, Poll::Ready(Ok(()))));

        let mut clone = hedge.clone();
        let mut fut = clone.call(99);
        let result = Pin::new(&mut fut).poll(&mut cx);
        assert!(matches!(result, Poll::Ready(Err(HedgeError::NotReady))));

        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(clone.total_requests(), 0);
    }

    #[test]
    fn hedge_inner() {
        let hedge = Hedge::new(42u32, HedgeConfig::new(Duration::from_millis(100)));
        assert_eq!(*hedge.inner(), 42);
    }

    #[test]
    fn hedge_inner_mut() {
        let mut hedge = Hedge::new(42u32, HedgeConfig::new(Duration::from_millis(100)));
        *hedge.inner_mut() = 99;
        assert_eq!(*hedge.inner(), 99);
    }

    #[test]
    fn hedge_config_ref() {
        let hedge = Hedge::new(MockSvc, HedgeConfig::new(Duration::from_millis(100)));
        assert_eq!(hedge.config().delay, Duration::from_millis(100));
    }

    #[test]
    fn hedge_debug() {
        let hedge = Hedge::new(MockSvc, HedgeConfig::new(Duration::from_millis(100)));
        let dbg = format!("{hedge:?}");
        assert!(dbg.contains("Hedge"));
    }

    #[test]
    fn hedge_clone() {
        let hedge = Hedge::new(MockSvc, HedgeConfig::new(Duration::from_millis(100)));
        hedge.record_request();
        let cloned = hedge.clone();
        // Stats are reset on clone.
        assert_eq!(cloned.total_requests(), 0);
        assert_eq!(cloned.delay(), Duration::from_millis(100));
        assert_eq!(hedge.total_requests(), 1);
    }

    #[test]
    fn hedge_layer_applies() {
        init_test("hedge_layer_applies");
        let layer = HedgeLayer::with_delay(Duration::from_millis(50));
        let svc = layer.layer(MockSvc);
        assert_eq!(svc.delay(), Duration::from_millis(50));
        crate::test_complete!("hedge_layer_applies");
    }

    // ================================================================
    // HedgeError
    // ================================================================

    #[test]
    fn error_inner_display() {
        let err: HedgeError<std::io::Error> = HedgeError::Inner(std::io::Error::other("fail"));
        assert!(format!("{err}").contains("service error"));
    }

    #[test]
    fn error_not_ready_display() {
        let err: HedgeError<std::io::Error> = HedgeError::NotReady;
        assert_eq!(format!("{err}"), "poll_ready required before call");
    }

    #[test]
    fn error_both_failed_display() {
        let err: HedgeError<std::io::Error> = HedgeError::BothFailed {
            primary: std::io::Error::other("p"),
            hedge: std::io::Error::other("h"),
        };
        assert!(format!("{err}").contains("both primary and hedge failed"));
    }

    #[test]
    fn error_source() {
        use std::error::Error;
        let err: HedgeError<std::io::Error> = HedgeError::Inner(std::io::Error::other("fail"));
        assert!(err.source().is_some());

        let not_ready: HedgeError<std::io::Error> = HedgeError::NotReady;
        assert!(not_ready.source().is_none());
    }

    #[test]
    fn error_debug() {
        let err: HedgeError<std::io::Error> = HedgeError::Inner(std::io::Error::other("fail"));
        let dbg = format!("{err:?}");
        assert!(dbg.contains("Inner"));
    }

    // ================================================================
    // HedgeFuture
    // ================================================================

    #[test]
    fn hedge_future_debug() {
        let fut = HedgeFuture::inner(std::future::ready(Ok::<i32, std::io::Error>(42)));
        let dbg = format!("{fut:?}");
        assert!(dbg.contains("HedgeFuture"));
    }
}
