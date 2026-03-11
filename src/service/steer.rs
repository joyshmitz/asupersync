//! Steer combinator: routes requests to one of several services.
//!
//! [`Steer`] dispatches each request to one of N inner services based on
//! a user-supplied routing function. This enables content-based routing,
//! A/B testing, and service selection patterns.

use super::Service;
use parking_lot::{Mutex, MutexGuard};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

// ─── Steer ────────────────────────────────────────────────────────────────

/// A service that routes requests to one of several inner services.
///
/// The `picker` function is called with the request to select which
/// backend receives it (by index into the `services` vec).
pub struct Steer<S, F> {
    services: Vec<Arc<Mutex<S>>>,
    picker: F,
}

impl<S, F> Steer<S, F> {
    /// Create a new steer combinator.
    ///
    /// `picker` is called with a reference to the request and must return
    /// an index into `services`.
    ///
    /// # Panics
    ///
    /// Panics if `services` is empty.
    #[must_use]
    pub fn new(services: Vec<S>, picker: F) -> Self {
        assert!(!services.is_empty(), "steer requires at least one service");
        Self {
            services: services
                .into_iter()
                .map(|service| Arc::new(Mutex::new(service)))
                .collect(),
            picker,
        }
    }

    /// Get the number of inner services.
    #[must_use]
    pub fn len(&self) -> usize {
        self.services.len()
    }

    /// Returns false (at least one service is always present).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.services.is_empty()
    }

    /// Snapshot the current inner services by cloning them.
    ///
    /// This is intended for inspection and tests.
    #[must_use]
    pub fn services(&self) -> Vec<S>
    where
        S: Clone,
    {
        self.services
            .iter()
            .map(|service| service.lock().clone())
            .collect()
    }

    /// Acquire mutable guards for the inner services.
    ///
    /// This is intended for tests and synchronous maintenance operations.
    pub fn services_mut(&self) -> Vec<MutexGuard<'_, S>> {
        self.services.iter().map(|service| service.lock()).collect()
    }
}

impl<S, F: Clone> Clone for Steer<S, F> {
    fn clone(&self) -> Self {
        Self {
            services: self.services.clone(),
            picker: self.picker.clone(),
        }
    }
}

impl<S: fmt::Debug, F> fmt::Debug for Steer<S, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let services: Vec<String> = self
            .services
            .iter()
            .map(|service| format!("{:?}", &*service.lock()))
            .collect();
        f.debug_struct("Steer")
            .field("services", &services)
            .finish_non_exhaustive()
    }
}

/// Future returned by [`Steer`].
pub struct SteerFuture<S, Request>
where
    S: Service<Request>,
{
    state: SteerState<S, Request>,
}

enum SteerState<S, Request>
where
    S: Service<Request>,
{
    PollReady {
        service: Arc<Mutex<S>>,
        request: Option<Request>,
    },
    Calling {
        future: S::Future,
    },
    Done,
}

impl<S, Request> SteerFuture<S, Request>
where
    S: Service<Request>,
{
    fn new(service: Arc<Mutex<S>>, request: Request) -> Self {
        Self {
            state: SteerState::PollReady {
                service,
                request: Some(request),
            },
        }
    }
}

impl<S, Request> fmt::Debug for SteerFuture<S, Request>
where
    S: Service<Request>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SteerFuture").finish_non_exhaustive()
    }
}

impl<S, Request> Future for SteerFuture<S, Request>
where
    S: Service<Request>,
    S::Future: Unpin,
    Request: Unpin,
{
    type Output = Result<S::Response, SteerError<S::Error>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            let state = std::mem::replace(&mut this.state, SteerState::Done);
            match state {
                SteerState::PollReady {
                    service,
                    mut request,
                } => {
                    let mut inner = service.lock();
                    match inner.poll_ready(cx) {
                        Poll::Pending => {
                            drop(inner);
                            this.state = SteerState::PollReady { service, request };
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(err)) => {
                            return Poll::Ready(Err(SteerError::Inner(err)));
                        }
                        Poll::Ready(Ok(())) => {
                            let req = request
                                .take()
                                .expect("SteerFuture polled after request taken");
                            let future = inner.call(req);
                            drop(inner);
                            this.state = SteerState::Calling { future };
                        }
                    }
                }
                SteerState::Calling { mut future } => {
                    let result = Pin::new(&mut future).poll(cx).map_err(SteerError::Inner);
                    if result.is_pending() {
                        this.state = SteerState::Calling { future };
                    } else {
                        this.state = SteerState::Done;
                    }
                    return result;
                }
                SteerState::Done => {
                    panic!("SteerFuture polled after completion");
                }
            }
        }
    }
}

impl<S, F, Request> Service<Request> for Steer<S, F>
where
    S: Service<Request>,
    S::Future: Unpin,
    F: Fn(&Request) -> usize,
    Request: Unpin,
{
    type Response = S::Response;
    type Error = SteerError<S::Error>;
    type Future = SteerFuture<S, Request>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Route selection is request-dependent. Polling every backend here can
        // strand reservations on services that never receive the request.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let idx = (self.picker)(&req) % self.services.len();
        SteerFuture::new(Arc::clone(&self.services[idx]), req)
    }
}

// ─── SteerError ───────────────────────────────────────────────────────────

/// Error wrapping for steer operations.
#[derive(Debug)]
pub enum SteerError<E> {
    /// Inner service error.
    Inner(E),
    /// No services available.
    NoServices,
}

impl<E: fmt::Display> fmt::Display for SteerError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inner(e) => write!(f, "steer service error: {e}"),
            Self::NoServices => write!(f, "no services available"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for SteerError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Inner(e) => Some(e),
            Self::NoServices => None,
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::future::block_on;
    use std::future::{Ready, ready};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake, Waker};

    fn init_test(name: &str) {
        crate::test_utils::init_test_logging();
        crate::test_phase!(name);
    }

    // Mock services.
    #[derive(Debug, Clone)]
    struct IdService {
        id: usize,
    }

    impl Service<usize> for IdService {
        type Response = usize;
        type Error = std::convert::Infallible;
        type Future = Ready<Result<usize, std::convert::Infallible>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: usize) -> Self::Future {
            ready(Ok(self.id))
        }
    }

    #[derive(Debug)]
    struct ReservingService {
        id: usize,
        available: Arc<AtomicUsize>,
        reserved: bool,
    }

    impl ReservingService {
        fn new(id: usize, available: Arc<AtomicUsize>) -> Self {
            Self {
                id,
                available,
                reserved: false,
            }
        }

        fn available(&self) -> usize {
            self.available.load(Ordering::SeqCst)
        }
    }

    impl Service<usize> for ReservingService {
        type Response = usize;
        type Error = &'static str;
        type Future = Ready<Result<usize, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            if self.reserved {
                return Poll::Ready(Ok(()));
            }

            let available = self.available.load(Ordering::SeqCst);
            if available == 0 {
                return Poll::Pending;
            }

            self.available.fetch_sub(1, Ordering::SeqCst);
            self.reserved = true;
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: usize) -> Self::Future {
            if !std::mem::replace(&mut self.reserved, false) {
                return ready(Err("not ready"));
            }

            self.available.fetch_add(1, Ordering::SeqCst);
            ready(Ok(self.id))
        }
    }

    struct NoopWaker;

    impl Wake for NoopWaker {
        fn wake(self: Arc<Self>) {}
        fn wake_by_ref(self: &Arc<Self>) {}
    }

    fn noop_waker() -> Waker {
        Arc::new(NoopWaker).into()
    }

    #[test]
    fn steer_new() {
        init_test("steer_new");
        let svcs = vec![IdService { id: 0 }, IdService { id: 1 }];
        let steer = Steer::new(svcs, |_req: &()| 0);
        assert_eq!(steer.len(), 2);
        assert!(!steer.is_empty());
        crate::test_complete!("steer_new");
    }

    #[test]
    #[should_panic(expected = "steer requires at least one service")]
    fn steer_empty_panics() {
        let svcs: Vec<IdService> = vec![];
        let _ = Steer::new(svcs, |_req: &()| 0);
    }

    #[test]
    fn steer_services_ref() {
        let svcs = vec![IdService { id: 10 }, IdService { id: 20 }];
        let steer = Steer::new(svcs, |_req: &()| 0);
        assert_eq!(steer.services().len(), 2);
        assert_eq!(steer.services()[0].id, 10);
    }

    #[test]
    fn steer_services_mut() {
        let svcs = vec![IdService { id: 10 }];
        let steer = Steer::new(svcs, |_req: &()| 0);
        {
            let mut guards = steer.services_mut();
            guards[0].id = 99;
        }
        assert_eq!(steer.services()[0].id, 99);
    }

    #[test]
    fn steer_debug() {
        let svcs = vec![IdService { id: 1 }];
        let steer = Steer::new(svcs, |_req: &()| 0);
        let dbg = format!("{steer:?}");
        assert!(dbg.contains("Steer"));
    }

    #[test]
    fn steer_picker_routes() {
        init_test("steer_picker_routes");
        let svcs = vec![IdService { id: 0 }, IdService { id: 1 }];
        let steer = Steer::new(svcs, |req: &usize| req % 2);
        let picker = &steer.picker;
        assert_eq!(picker(&0), 0);
        assert_eq!(picker(&1), 1);
        assert_eq!(picker(&2), 0);
        assert_eq!(picker(&3), 1);
        crate::test_complete!("steer_picker_routes");
    }

    #[test]
    fn steer_picker_wraps() {
        let svcs = vec![IdService { id: 0 }, IdService { id: 1 }];
        let steer = Steer::new(svcs, |(): &()| 5);
        let idx = (steer.picker)(&()) % steer.len();
        assert_eq!(idx, 1);
    }

    #[test]
    fn steer_error_inner_display() {
        let err: SteerError<std::io::Error> = SteerError::Inner(std::io::Error::other("fail"));
        assert!(format!("{err}").contains("steer service error"));
    }

    #[test]
    fn steer_error_no_services_display() {
        let err: SteerError<std::io::Error> = SteerError::NoServices;
        assert!(format!("{err}").contains("no services available"));
    }

    #[test]
    fn steer_error_source() {
        use std::error::Error;
        let err: SteerError<std::io::Error> = SteerError::Inner(std::io::Error::other("fail"));
        assert!(err.source().is_some());

        let err2: SteerError<std::io::Error> = SteerError::NoServices;
        assert!(err2.source().is_none());
    }

    #[test]
    fn steer_error_debug() {
        let err: SteerError<std::io::Error> = SteerError::NoServices;
        let dbg = format!("{err:?}");
        assert!(dbg.contains("NoServices"));
    }

    #[test]
    fn steer_call_without_outer_poll_ready_still_routes_selected_backend() {
        init_test("steer_call_without_outer_poll_ready_still_routes_selected_backend");
        let mut steer = Steer::new(vec![IdService { id: 7 }], |_: &usize| 0);
        let result = block_on(steer.call(0)).expect("selected backend should succeed");
        assert_eq!(result, 7);
        crate::test_complete!("steer_call_without_outer_poll_ready_still_routes_selected_backend");
    }

    #[test]
    fn steer_call_only_reserves_selected_backend() {
        init_test("steer_call_only_reserves_selected_backend");
        let even_available = Arc::new(AtomicUsize::new(1));
        let odd_available = Arc::new(AtomicUsize::new(1));
        let mut steer = Steer::new(
            vec![
                ReservingService::new(0, Arc::clone(&even_available)),
                ReservingService::new(1, Arc::clone(&odd_available)),
            ],
            |req: &usize| req % 2,
        );

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let ready = steer.poll_ready(&mut cx);
        assert!(matches!(ready, Poll::Ready(Ok(()))));
        assert_eq!(even_available.load(Ordering::SeqCst), 1);
        assert_eq!(odd_available.load(Ordering::SeqCst), 1);

        let result = block_on(steer.call(0)).expect("selected backend should succeed");
        assert_eq!(result, 0);

        let guards = steer.services_mut();
        assert_eq!(guards[0].available(), 1);
        assert_eq!(guards[1].available(), 1);
        crate::test_complete!("steer_call_only_reserves_selected_backend");
    }

    #[test]
    fn steer_selected_route_is_not_blocked_by_other_backends() {
        init_test("steer_selected_route_is_not_blocked_by_other_backends");
        let blocked_available = Arc::new(AtomicUsize::new(0));
        let ready_available = Arc::new(AtomicUsize::new(1));
        let mut steer = Steer::new(
            vec![
                ReservingService::new(0, Arc::clone(&blocked_available)),
                ReservingService::new(1, Arc::clone(&ready_available)),
            ],
            |req: &usize| req % 2,
        );

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let ready = steer.poll_ready(&mut cx);
        assert!(matches!(ready, Poll::Ready(Ok(()))));

        let result = block_on(steer.call(1)).expect("ready route should succeed");
        assert_eq!(result, 1);
        assert_eq!(blocked_available.load(Ordering::SeqCst), 0);
        assert_eq!(ready_available.load(Ordering::SeqCst), 1);
        crate::test_complete!("steer_selected_route_is_not_blocked_by_other_backends");
    }
}
