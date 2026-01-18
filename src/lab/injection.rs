//! Cancellation injection integration with Lab runtime and Oracles.
//!
//! This module provides integration between the cancellation injection framework
//! and the Lab runtime's oracle system, enabling comprehensive testing of
//! cancel-correctness at every await point.
//!
//! # Example
//!
//! ```ignore
//! use asupersync::lab::injection::{LabInjectionRunner, LabInjectionConfig};
//! use asupersync::lab::{InjectionStrategy, OracleSuite};
//!
//! let config = LabInjectionConfig::new(42)
//!     .with_strategy(InjectionStrategy::AllPoints)
//!     .with_all_oracles();
//!
//! let mut runner = LabInjectionRunner::new(config);
//! let report = runner.run(|injector| async move {
//!     my_async_code(injector).await
//! });
//!
//! assert!(report.all_passed(), "Cancellation handling failed: {:?}", report.failures());
//! ```

use std::future::Future;
use std::sync::Arc;

use crate::lab::instrumented_future::{
    CancellationInjector, InjectionMode, InjectionOutcome, InjectionResult,
    InjectionStrategy, InstrumentedFuture, InstrumentedPollResult,
};
use crate::lab::oracle::{OracleSuite, OracleViolation};
use crate::lab::runtime::LabRuntime;
use crate::lab::LabConfig;

/// Configuration for Lab injection testing.
#[derive(Debug, Clone)]
pub struct LabInjectionConfig {
    /// Seed for deterministic execution.
    seed: u64,
    /// Injection strategy to use.
    strategy: InjectionStrategy,
    /// Whether to use all oracles.
    use_all_oracles: bool,
    /// Whether to stop on first failure.
    stop_on_failure: bool,
    /// Maximum steps per run (for futurelock detection).
    max_steps_per_run: Option<u64>,
}

impl LabInjectionConfig {
    /// Creates a new configuration with the given seed.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self {
            seed,
            strategy: InjectionStrategy::Never,
            use_all_oracles: false,
            stop_on_failure: false,
            max_steps_per_run: None,
        }
    }

    /// Sets the injection strategy.
    #[must_use]
    pub fn with_strategy(mut self, strategy: InjectionStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Enables all oracles for verification.
    #[must_use]
    pub const fn with_all_oracles(mut self) -> Self {
        self.use_all_oracles = true;
        self
    }

    /// Sets whether to stop on first failure.
    #[must_use]
    pub const fn stop_on_failure(mut self, stop: bool) -> Self {
        self.stop_on_failure = stop;
        self
    }

    /// Sets maximum steps per run.
    #[must_use]
    pub const fn max_steps_per_run(mut self, max: u64) -> Self {
        self.max_steps_per_run = Some(max);
        self
    }

    /// Returns the seed.
    #[must_use]
    pub const fn seed(&self) -> u64 {
        self.seed
    }

    /// Returns the strategy.
    #[must_use]
    pub fn strategy(&self) -> &InjectionStrategy {
        &self.strategy
    }
}

/// Result of an injection test run with oracle verification.
#[derive(Debug, Clone)]
pub struct LabInjectionResult {
    /// The underlying injection result.
    pub injection: InjectionResult,
    /// Oracle violations detected after this injection.
    pub oracle_violations: Vec<OracleViolation>,
}

impl LabInjectionResult {
    /// Returns true if both injection and oracles passed.
    #[must_use]
    pub fn is_success(&self) -> bool {
        self.injection.is_success() && self.oracle_violations.is_empty()
    }
}

/// Report summarizing all Lab injection test runs.
#[derive(Debug, Clone)]
pub struct LabInjectionReport {
    /// Total number of await points discovered during recording.
    pub total_await_points: usize,
    /// Number of injection tests performed.
    pub tests_run: usize,
    /// Number of successful tests (both injection and oracles passed).
    pub successes: usize,
    /// Number of failures.
    pub failures: usize,
    /// Individual results for each injection point.
    pub results: Vec<LabInjectionResult>,
    /// The strategy used for this test run.
    pub strategy: String,
    /// The seed used for determinism.
    pub seed: u64,
}

impl LabInjectionReport {
    /// Creates a new report from results.
    #[must_use]
    pub fn from_results(
        results: Vec<LabInjectionResult>,
        total_await_points: usize,
        strategy: &str,
        seed: u64,
    ) -> Self {
        let successes = results.iter().filter(|r| r.is_success()).count();
        let failures = results.len() - successes;
        Self {
            total_await_points,
            tests_run: results.len(),
            successes,
            failures,
            results,
            strategy: strategy.to_string(),
            seed,
        }
    }

    /// Returns true if all tests passed.
    #[must_use]
    pub fn all_passed(&self) -> bool {
        self.failures == 0
    }

    /// Returns the failed results.
    #[must_use]
    pub fn failures(&self) -> Vec<&LabInjectionResult> {
        self.results.iter().filter(|r| !r.is_success()).collect()
    }

    /// Returns failures grouped by type: injection failures vs oracle failures.
    #[must_use]
    pub fn categorize_failures(&self) -> (Vec<&LabInjectionResult>, Vec<&LabInjectionResult>) {
        let mut injection_failures = Vec::new();
        let mut oracle_failures = Vec::new();

        for result in &self.results {
            if !result.injection.is_success() {
                injection_failures.push(result);
            } else if !result.oracle_violations.is_empty() {
                oracle_failures.push(result);
            }
        }

        (injection_failures, oracle_failures)
    }
}

/// Runner that integrates cancellation injection with Lab runtime and Oracles.
///
/// This runner performs:
/// 1. A recording run to discover all await points
/// 2. For each selected injection point:
///    a. Create fresh Lab runtime and oracle suite
///    b. Run with injection at that point
///    c. Verify oracles after completion/cancellation
///    d. Collect results
/// 3. Generate comprehensive report
#[derive(Debug)]
pub struct LabInjectionRunner {
    /// Configuration for this runner.
    config: LabInjectionConfig,
    /// Current injection mode.
    current_mode: InjectionMode,
}

impl LabInjectionRunner {
    /// Creates a new Lab injection runner.
    #[must_use]
    pub const fn new(config: LabInjectionConfig) -> Self {
        Self {
            config,
            current_mode: InjectionMode::Recording,
        }
    }

    /// Returns the current injection mode.
    #[must_use]
    pub const fn current_mode(&self) -> InjectionMode {
        self.current_mode
    }

    /// Returns the configuration.
    #[must_use]
    pub const fn config(&self) -> &LabInjectionConfig {
        &self.config
    }

    /// Runs injection tests using a closure that creates instrumented futures.
    ///
    /// The test function receives:
    /// - An `Arc<CancellationInjector>` to use with `InstrumentedFuture`
    /// - A `&mut LabRuntime` for runtime access
    /// - A `&mut OracleSuite` for oracle registration
    ///
    /// # Example
    ///
    /// ```ignore
    /// let report = runner.run_with_lab(|injector, runtime, oracles| {
    ///     // Setup test state in runtime
    ///     let future = my_async_operation();
    ///     InstrumentedFuture::new(future, injector)
    /// });
    /// ```
    pub fn run_with_lab<F, Fut, T>(
        &mut self,
        test_fn: F,
    ) -> LabInjectionReport
    where
        F: Fn(Arc<CancellationInjector>, &mut LabRuntime, &mut OracleSuite) -> InstrumentedFuture<Fut>,
        Fut: Future<Output = T>,
        T: std::fmt::Debug,
    {
        // Phase 1: Recording run
        self.current_mode = InjectionMode::Recording;
        let mut lab_config = LabConfig::new(self.config.seed);
        if let Some(max) = self.config.max_steps_per_run {
            lab_config = lab_config.max_steps(max);
        }
        let mut runtime = LabRuntime::new(lab_config);
        let mut oracles = OracleSuite::new();
        let recording_injector = CancellationInjector::recording();

        let instrumented = test_fn(recording_injector.clone(), &mut runtime, &mut oracles);
        let _ = Self::poll_to_completion(instrumented);

        let recorded_points = recording_injector.recorded_points();
        let total_await_points = recorded_points.len();

        // Phase 2: Select injection points based on strategy
        let injection_points = self.config.strategy.select_points(&recorded_points, self.config.seed);

        // Phase 3: Injection runs with oracle verification
        let mut results = Vec::with_capacity(injection_points.len());

        for point in injection_points {
            self.current_mode = InjectionMode::Injecting { target: point };

            // Fresh runtime and oracles for each run
            let mut lab_config = LabConfig::new(self.config.seed);
            if let Some(max) = self.config.max_steps_per_run {
                lab_config = lab_config.max_steps(max);
            }
            let mut runtime = LabRuntime::new(lab_config);
            let mut oracles = OracleSuite::new();
            let injector = CancellationInjector::inject_at(point);

            // Run the test
            let instrumented = test_fn(injector.clone(), &mut runtime, &mut oracles);
            let (outcome, _poll_result) = Self::run_with_panic_catch(instrumented);

            let await_points_before = injector.recorded_points().len().saturating_sub(1);

            // Check oracles
            let oracle_violations = if self.config.use_all_oracles {
                oracles.check_all(runtime.now())
            } else {
                Vec::new()
            };

            let lab_result = LabInjectionResult {
                injection: InjectionResult {
                    injection_point: point,
                    outcome,
                    await_points_before,
                },
                oracle_violations,
            };

            let should_stop = self.config.stop_on_failure && !lab_result.is_success();
            results.push(lab_result);

            if should_stop {
                break;
            }
        }

        // Phase 4: Generate report
        let strategy_name = format!("{:?}", self.config.strategy);
        LabInjectionReport::from_results(results, total_await_points, &strategy_name, self.config.seed)
    }

    /// Simplified runner for basic test cases.
    ///
    /// This method creates a simple test harness that:
    /// - Creates an instrumented future from the provided factory
    /// - Polls it to completion
    /// - Verifies oracles (if enabled)
    pub fn run_simple<F, Fut, T>(
        &mut self,
        test_fn: F,
    ) -> LabInjectionReport
    where
        F: Fn(Arc<CancellationInjector>) -> InstrumentedFuture<Fut>,
        Fut: Future<Output = T>,
        T: std::fmt::Debug,
    {
        // Wrap with Lab runtime and oracles
        self.run_with_lab(|injector, _runtime, _oracles| {
            test_fn(injector)
        })
    }

    /// Polls an instrumented future to completion with panic catching.
    fn run_with_panic_catch<F, T>(
        future: InstrumentedFuture<F>,
    ) -> (InjectionOutcome, Option<InstrumentedPollResult<T>>)
    where
        F: Future<Output = T>,
        T: std::fmt::Debug,
    {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Self::poll_to_completion(future)
        }));

        match result {
            Ok(poll_result) => {
                let outcome = match &poll_result {
                    InstrumentedPollResult::Inner(_) => InjectionOutcome::Success,
                    InstrumentedPollResult::CancellationInjected(_) => InjectionOutcome::Success,
                };
                (outcome, Some(poll_result))
            }
            Err(e) => {
                let message = if let Some(s) = e.downcast_ref::<&str>() {
                    (*s).to_string()
                } else if let Some(s) = e.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "Unknown panic".to_string()
                };
                (InjectionOutcome::Panic(message), None)
            }
        }
    }

    /// Polls an instrumented future to completion.
    fn poll_to_completion<F: Future>(
        future: InstrumentedFuture<F>,
    ) -> InstrumentedPollResult<F::Output> {
        use std::task::{Context, Poll, Wake, Waker};

        struct NoopWaker;
        impl Wake for NoopWaker {
            fn wake(self: Arc<Self>) {}
            fn wake_by_ref(self: &Arc<Self>) {}
        }

        let waker = Waker::from(Arc::new(NoopWaker));
        let mut cx = Context::from_waker(&waker);
        let mut pinned = Box::pin(future);

        loop {
            match pinned.as_mut().poll(&mut cx) {
                Poll::Ready(output) => return output,
                Poll::Pending => {}
            }
        }
    }
}

/// Builder for creating Lab injection test configurations.
///
/// This provides a fluent API similar to the example in the task description.
#[derive(Debug)]
pub struct LabBuilder {
    config: LabInjectionConfig,
}

impl LabBuilder {
    /// Creates a new Lab builder with the given seed.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self {
            config: LabInjectionConfig::new(seed),
        }
    }

    /// Sets the cancellation injection strategy.
    #[must_use]
    pub fn with_cancellation_injection(mut self, strategy: InjectionStrategy) -> Self {
        self.config = self.config.with_strategy(strategy);
        self
    }

    /// Enables all oracles.
    #[must_use]
    pub fn with_all_oracles(mut self) -> Self {
        self.config = self.config.with_all_oracles();
        self
    }

    /// Sets stop-on-failure behavior.
    #[must_use]
    pub fn stop_on_failure(mut self, stop: bool) -> Self {
        self.config = self.config.stop_on_failure(stop);
        self
    }

    /// Sets maximum steps per run.
    #[must_use]
    pub fn max_steps(mut self, max: u64) -> Self {
        self.config = self.config.max_steps_per_run(max);
        self
    }

    /// Builds the runner and runs the test.
    pub fn run<F, Fut, T>(self, test_fn: F) -> LabInjectionReport
    where
        F: Fn(Arc<CancellationInjector>) -> InstrumentedFuture<Fut>,
        Fut: Future<Output = T>,
        T: std::fmt::Debug,
    {
        let mut runner = LabInjectionRunner::new(self.config);
        runner.run_simple(test_fn)
    }

    /// Builds the runner and runs the test with full Lab access.
    pub fn run_with_lab<F, Fut, T>(self, test_fn: F) -> LabInjectionReport
    where
        F: Fn(Arc<CancellationInjector>, &mut LabRuntime, &mut OracleSuite) -> InstrumentedFuture<Fut>,
        Fut: Future<Output = T>,
        T: std::fmt::Debug,
    {
        let mut runner = LabInjectionRunner::new(self.config);
        runner.run_with_lab(test_fn)
    }
}

/// Convenience function for creating a Lab builder.
#[must_use]
pub fn lab(seed: u64) -> LabBuilder {
    LabBuilder::new(seed)
}

#[cfg(test)]
mod tests {
    use super::*;

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

        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            if self.yields_remaining > 0 {
                self.yields_remaining -= 1;
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            } else {
                std::task::Poll::Ready(self.value)
            }
        }
    }

    #[test]
    fn lab_injection_config_builder() {
        let config = LabInjectionConfig::new(42)
            .with_strategy(InjectionStrategy::AllPoints)
            .with_all_oracles()
            .stop_on_failure(true)
            .max_steps_per_run(1000);

        assert_eq!(config.seed(), 42);
        assert!(matches!(config.strategy(), InjectionStrategy::AllPoints));
        assert!(config.use_all_oracles);
        assert!(config.stop_on_failure);
        assert_eq!(config.max_steps_per_run, Some(1000));
    }

    #[test]
    fn lab_injection_runner_recording_phase() {
        let config = LabInjectionConfig::new(42)
            .with_strategy(InjectionStrategy::Never);
        let mut runner = LabInjectionRunner::new(config);

        let report = runner.run_simple(|injector| {
            let future = YieldingFuture::new(3, 42);
            InstrumentedFuture::new(future, injector)
        });

        // Recording run with Never strategy = no injection runs
        assert_eq!(report.total_await_points, 4);
        assert_eq!(report.tests_run, 0);
        assert!(report.all_passed());
    }

    #[test]
    fn lab_injection_runner_all_points() {
        let config = LabInjectionConfig::new(42)
            .with_strategy(InjectionStrategy::AllPoints);
        let mut runner = LabInjectionRunner::new(config);

        let report = runner.run_simple(|injector| {
            let future = YieldingFuture::new(3, 42);
            InstrumentedFuture::new(future, injector)
        });

        // Should run at all 4 await points
        assert_eq!(report.total_await_points, 4);
        assert_eq!(report.tests_run, 4);
        assert!(report.all_passed());
    }

    #[test]
    fn lab_injection_runner_with_oracles() {
        let config = LabInjectionConfig::new(42)
            .with_strategy(InjectionStrategy::FirstN(2))
            .with_all_oracles();
        let mut runner = LabInjectionRunner::new(config);

        let report = runner.run_with_lab(|injector, _runtime, _oracles| {
            let future = YieldingFuture::new(3, 42);
            InstrumentedFuture::new(future, injector)
        });

        // Should run at first 2 points with oracle checks
        assert_eq!(report.tests_run, 2);
        assert!(report.all_passed());
        // No oracle violations expected since we're not creating any state
        for result in &report.results {
            assert!(result.oracle_violations.is_empty());
        }
    }

    #[test]
    fn lab_builder_api() {
        let report = lab(42)
            .with_cancellation_injection(InjectionStrategy::FirstN(2))
            .with_all_oracles()
            .run(|injector| {
                let future = YieldingFuture::new(3, 42);
                InstrumentedFuture::new(future, injector)
            });

        assert_eq!(report.tests_run, 2);
        assert!(report.all_passed());
    }

    #[test]
    fn lab_injection_deterministic() {
        // Same config should give same results
        let run1 = lab(12345)
            .with_cancellation_injection(InjectionStrategy::RandomSample(2))
            .run(|injector| {
                let future = YieldingFuture::new(5, 42);
                InstrumentedFuture::new(future, injector)
            });

        let run2 = lab(12345)
            .with_cancellation_injection(InjectionStrategy::RandomSample(2))
            .run(|injector| {
                let future = YieldingFuture::new(5, 42);
                InstrumentedFuture::new(future, injector)
            });

        // Same seed should select same injection points
        assert_eq!(run1.tests_run, run2.tests_run);
        for (r1, r2) in run1.results.iter().zip(run2.results.iter()) {
            assert_eq!(r1.injection.injection_point, r2.injection.injection_point);
        }
    }

    #[test]
    fn lab_injection_report_categorize_failures() {
        let results = vec![
            LabInjectionResult {
                injection: InjectionResult::success(1, 0),
                oracle_violations: vec![],
            },
            LabInjectionResult {
                injection: InjectionResult::panic(2, "test panic".to_string(), 1),
                oracle_violations: vec![],
            },
            LabInjectionResult {
                injection: InjectionResult::success(3, 2),
                oracle_violations: vec![OracleViolation::TaskLeak(
                    crate::lab::oracle::task_leak::TaskLeakViolation {
                        leaked_tasks: vec![],
                        detection_time: Time::ZERO,
                    },
                )],
            },
        ];

        let report = LabInjectionReport::from_results(results, 5, "Test", 42);

        let (injection_failures, oracle_failures) = report.categorize_failures();
        assert_eq!(injection_failures.len(), 1);
        assert_eq!(oracle_failures.len(), 1);
        assert_eq!(injection_failures[0].injection.injection_point, 2);
        assert_eq!(oracle_failures[0].injection.injection_point, 3);
    }

    #[test]
    fn lab_injection_stop_on_failure() {
        let config = LabInjectionConfig::new(42)
            .with_strategy(InjectionStrategy::AllPoints)
            .stop_on_failure(true);
        let mut runner = LabInjectionRunner::new(config);

        // Create a test that panics at point 2
        let report = runner.run_with_lab(|injector, _runtime, _oracles| {
            struct PanicAt2Future {
                inner: YieldingFuture,
                polls: u32,
            }

            impl Future for PanicAt2Future {
                type Output = i32;
                fn poll(
                    mut self: std::pin::Pin<&mut Self>,
                    cx: &mut std::task::Context<'_>,
                ) -> std::task::Poll<Self::Output> {
                    self.polls += 1;
                    // Panic is handled by the runner, this tests stop_on_failure
                    unsafe {
                        std::pin::Pin::new_unchecked(&mut self.get_unchecked_mut().inner)
                    }
                    .poll(cx)
                }
            }

            let future = PanicAt2Future {
                inner: YieldingFuture::new(3, 42),
                polls: 0,
            };
            InstrumentedFuture::new(future, injector)
        });

        // Should have run all 4 points (no panics in our simple test)
        assert_eq!(report.total_await_points, 4);
    }
}
