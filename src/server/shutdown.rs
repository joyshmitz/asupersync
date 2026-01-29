//! Shutdown coordination for HTTP server lifecycle.
//!
//! This module provides [`ShutdownSignal`] for coordinating graceful server shutdown
//! with drain timeouts and phase tracking. It builds on the lower-level
//! [`ShutdownController`](crate::signal::ShutdownController) by adding drain-phase
//! awareness and timeout semantics.

use crate::signal::{ShutdownController, ShutdownReceiver};
use crate::sync::Notify;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Phases of a graceful server shutdown.
///
/// Shutdown proceeds through these phases in order:
/// 1. [`Running`](ShutdownPhase::Running) — normal operation
/// 2. [`Draining`](ShutdownPhase::Draining) — stopped accepting, waiting for in-flight
/// 3. [`ForceClosing`](ShutdownPhase::ForceClosing) — drain timeout exceeded, force-closing
/// 4. [`Stopped`](ShutdownPhase::Stopped) — all connections closed
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ShutdownPhase {
    /// Normal operation — accepting connections and processing requests.
    Running = 0,
    /// Stopped accepting new connections; waiting for in-flight requests to complete.
    Draining = 1,
    /// Drain timeout exceeded; force-closing remaining connections.
    ForceClosing = 2,
    /// All connections closed; server fully stopped.
    Stopped = 3,
}

impl ShutdownPhase {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Running,
            1 => Self::Draining,
            2 => Self::ForceClosing,
            3 => Self::Stopped,
            _ => Self::Stopped,
        }
    }
}

impl std::fmt::Display for ShutdownPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Running => write!(f, "Running"),
            Self::Draining => write!(f, "Draining"),
            Self::ForceClosing => write!(f, "ForceClosing"),
            Self::Stopped => write!(f, "Stopped"),
        }
    }
}

/// Statistics collected during shutdown.
#[derive(Debug, Clone)]
pub struct ShutdownStats {
    /// Number of connections that completed gracefully during drain.
    pub drained: usize,
    /// Number of connections force-closed after the drain timeout.
    pub force_closed: usize,
    /// Total shutdown duration.
    pub duration: Duration,
}

/// Internal state shared between the signal and its subscribers.
struct SignalState {
    phase: AtomicU8,
    controller: ShutdownController,
    phase_notify: Notify,
    drain_deadline: std::sync::Mutex<Option<Instant>>,
}

/// Broadcast signal for server shutdown coordination.
///
/// `ShutdownSignal` wraps the lower-level [`ShutdownController`] with
/// shutdown-phase tracking and drain timeout awareness. Handlers can check
/// whether the server is draining to add `Connection: close` headers or
/// reject new work.
///
/// # Example
///
/// ```ignore
/// use asupersync::server::ShutdownSignal;
/// use std::time::Duration;
///
/// let signal = ShutdownSignal::new();
///
/// // In the accept loop:
/// if signal.is_draining() {
///     break; // stop accepting
/// }
///
/// // Initiate shutdown with a 30-second drain period:
/// signal.begin_drain(Duration::from_secs(30));
/// ```
#[derive(Clone)]
pub struct ShutdownSignal {
    state: Arc<SignalState>,
}

impl ShutdownSignal {
    /// Creates a new shutdown signal in the [`Running`](ShutdownPhase::Running) phase.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(SignalState {
                phase: AtomicU8::new(ShutdownPhase::Running as u8),
                controller: ShutdownController::new(),
                phase_notify: Notify::new(),
                drain_deadline: std::sync::Mutex::new(None),
            }),
        }
    }

    /// Returns the current shutdown phase.
    #[must_use]
    pub fn phase(&self) -> ShutdownPhase {
        ShutdownPhase::from_u8(self.state.phase.load(Ordering::Acquire))
    }

    /// Returns `true` if the server is in the draining phase.
    #[must_use]
    pub fn is_draining(&self) -> bool {
        self.phase() == ShutdownPhase::Draining
    }

    /// Returns `true` if shutdown has been initiated (draining or later).
    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        self.phase() != ShutdownPhase::Running
    }

    /// Returns `true` if the server has fully stopped.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.phase() == ShutdownPhase::Stopped
    }

    /// Returns the drain deadline, if one has been set.
    #[must_use]
    pub fn drain_deadline(&self) -> Option<Instant> {
        self.state
            .drain_deadline
            .lock()
            .expect("drain_deadline lock poisoned")
            .as_ref()
            .copied()
    }

    /// Subscribes to the underlying shutdown controller for async waiting.
    #[must_use]
    pub fn subscribe(&self) -> ShutdownReceiver {
        self.state.controller.subscribe()
    }

    /// Begins the drain phase with the given timeout.
    ///
    /// Transitions from `Running` to `Draining` and sets a drain deadline.
    /// The caller should stop accepting new connections after this call.
    ///
    /// Returns `false` if shutdown was already initiated.
    pub fn begin_drain(&self, timeout: Duration) -> bool {
        let result = self.state.phase.compare_exchange(
            ShutdownPhase::Running as u8,
            ShutdownPhase::Draining as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        if result.is_ok() {
            {
                let mut deadline = self
                    .state
                    .drain_deadline
                    .lock()
                    .expect("drain_deadline lock poisoned");
                *deadline = Some(Instant::now() + timeout);
            }
            self.state.controller.shutdown();
            self.state.phase_notify.notify_waiters();
            true
        } else {
            false
        }
    }

    /// Transitions to the force-closing phase.
    ///
    /// Called when the drain timeout has expired and remaining connections
    /// must be terminated. Returns `false` if not currently draining.
    pub fn begin_force_close(&self) -> bool {
        let result = self.state.phase.compare_exchange(
            ShutdownPhase::Draining as u8,
            ShutdownPhase::ForceClosing as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        if result.is_ok() {
            self.state.phase_notify.notify_waiters();
            true
        } else {
            false
        }
    }

    /// Marks the server as fully stopped.
    ///
    /// Called when all connections have been closed.
    pub fn mark_stopped(&self) {
        self.state
            .phase
            .store(ShutdownPhase::Stopped as u8, Ordering::Release);
        self.state.phase_notify.notify_waiters();
    }

    /// Waits until the shutdown phase changes.
    ///
    /// Returns the new phase after the change.
    pub async fn phase_changed(&self) -> ShutdownPhase {
        self.state.phase_notify.notified().await;
        self.phase()
    }

    /// Triggers an immediate stop (skips drain phase).
    ///
    /// Useful for hard shutdowns or test scenarios.
    pub fn trigger_immediate(&self) {
        self.state
            .phase
            .store(ShutdownPhase::Stopped as u8, Ordering::Release);
        self.state.controller.shutdown();
        self.state.phase_notify.notify_waiters();
    }
}

impl Default for ShutdownSignal {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ShutdownSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShutdownSignal")
            .field("phase", &self.phase())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::init_test_logging;

    fn init_test(name: &str) {
        init_test_logging();
        crate::test_phase!(name);
    }

    #[test]
    fn initial_state_is_running() {
        init_test("initial_state_is_running");
        let signal = ShutdownSignal::new();
        crate::assert_with_log!(
            signal.phase() == ShutdownPhase::Running,
            "phase",
            ShutdownPhase::Running,
            signal.phase()
        );
        crate::assert_with_log!(
            !signal.is_draining(),
            "not draining",
            false,
            signal.is_draining()
        );
        crate::assert_with_log!(
            !signal.is_shutting_down(),
            "not shutting down",
            false,
            signal.is_shutting_down()
        );
        crate::assert_with_log!(
            !signal.is_stopped(),
            "not stopped",
            false,
            signal.is_stopped()
        );
        crate::test_complete!("initial_state_is_running");
    }

    #[test]
    fn begin_drain_transitions_to_draining() {
        init_test("begin_drain_transitions_to_draining");
        let signal = ShutdownSignal::new();
        let initiated = signal.begin_drain(Duration::from_secs(30));
        crate::assert_with_log!(initiated, "initiated", true, initiated);
        crate::assert_with_log!(
            signal.phase() == ShutdownPhase::Draining,
            "phase",
            ShutdownPhase::Draining,
            signal.phase()
        );
        crate::assert_with_log!(
            signal.is_draining(),
            "is draining",
            true,
            signal.is_draining()
        );
        crate::assert_with_log!(
            signal.is_shutting_down(),
            "is shutting down",
            true,
            signal.is_shutting_down()
        );
        let has_deadline = signal.drain_deadline().is_some();
        crate::assert_with_log!(has_deadline, "has deadline", true, has_deadline);
        crate::test_complete!("begin_drain_transitions_to_draining");
    }

    #[test]
    fn begin_drain_idempotent() {
        init_test("begin_drain_idempotent");
        let signal = ShutdownSignal::new();
        let first = signal.begin_drain(Duration::from_secs(30));
        crate::assert_with_log!(first, "first drain", true, first);

        let second = signal.begin_drain(Duration::from_secs(60));
        crate::assert_with_log!(!second, "second drain rejected", false, second);

        crate::assert_with_log!(
            signal.phase() == ShutdownPhase::Draining,
            "still draining",
            ShutdownPhase::Draining,
            signal.phase()
        );
        crate::test_complete!("begin_drain_idempotent");
    }

    #[test]
    fn force_close_from_draining() {
        init_test("force_close_from_draining");
        let signal = ShutdownSignal::new();
        signal.begin_drain(Duration::from_secs(1));

        let forced = signal.begin_force_close();
        crate::assert_with_log!(forced, "force close", true, forced);
        crate::assert_with_log!(
            signal.phase() == ShutdownPhase::ForceClosing,
            "phase",
            ShutdownPhase::ForceClosing,
            signal.phase()
        );
        crate::test_complete!("force_close_from_draining");
    }

    #[test]
    fn force_close_only_from_draining() {
        init_test("force_close_only_from_draining");
        let signal = ShutdownSignal::new();

        // Can't force close from Running
        let forced = signal.begin_force_close();
        crate::assert_with_log!(!forced, "can't force from running", false, forced);
        crate::assert_with_log!(
            signal.phase() == ShutdownPhase::Running,
            "still running",
            ShutdownPhase::Running,
            signal.phase()
        );
        crate::test_complete!("force_close_only_from_draining");
    }

    #[test]
    fn mark_stopped() {
        init_test("mark_stopped");
        let signal = ShutdownSignal::new();
        signal.begin_drain(Duration::from_secs(1));
        signal.begin_force_close();
        signal.mark_stopped();

        crate::assert_with_log!(
            signal.phase() == ShutdownPhase::Stopped,
            "stopped",
            ShutdownPhase::Stopped,
            signal.phase()
        );
        crate::assert_with_log!(signal.is_stopped(), "is stopped", true, signal.is_stopped());
        crate::test_complete!("mark_stopped");
    }

    #[test]
    fn trigger_immediate_skips_drain() {
        init_test("trigger_immediate_skips_drain");
        let signal = ShutdownSignal::new();
        signal.trigger_immediate();

        crate::assert_with_log!(
            signal.phase() == ShutdownPhase::Stopped,
            "stopped immediately",
            ShutdownPhase::Stopped,
            signal.phase()
        );
        crate::test_complete!("trigger_immediate_skips_drain");
    }

    #[test]
    fn subscribe_receives_shutdown() {
        init_test("subscribe_receives_shutdown");
        let signal = ShutdownSignal::new();
        let receiver = signal.subscribe();

        let not_shutting = receiver.is_shutting_down();
        crate::assert_with_log!(!not_shutting, "not shutting", false, not_shutting);

        signal.begin_drain(Duration::from_secs(30));

        let shutting = receiver.is_shutting_down();
        crate::assert_with_log!(shutting, "shutting down", true, shutting);
        crate::test_complete!("subscribe_receives_shutdown");
    }

    #[test]
    fn display_formatting() {
        init_test("display_formatting");
        let cases = [
            (ShutdownPhase::Running, "Running"),
            (ShutdownPhase::Draining, "Draining"),
            (ShutdownPhase::ForceClosing, "ForceClosing"),
            (ShutdownPhase::Stopped, "Stopped"),
        ];
        for (phase, expected) in cases {
            let actual = format!("{phase}");
            crate::assert_with_log!(actual == expected, "phase display", expected, actual);
        }
        crate::test_complete!("display_formatting");
    }

    #[test]
    fn clone_shares_state() {
        init_test("clone_shares_state");
        let signal = ShutdownSignal::new();
        let cloned = signal.clone();

        signal.begin_drain(Duration::from_secs(30));

        crate::assert_with_log!(
            cloned.is_draining(),
            "clone sees drain",
            true,
            cloned.is_draining()
        );
        crate::test_complete!("clone_shares_state");
    }
}
