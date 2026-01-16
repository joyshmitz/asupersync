//! Test oracles for verifying runtime invariants.
//!
//! Oracles observe runtime events and verify that the 6 non-negotiable
//! invariants hold. They are used in lab mode for deterministic testing.
//!
//! # The 6 Non-Negotiable Invariants
//!
//! | # | Invariant | Oracle |
//! |---|-----------|--------|
//! | 1 | Structured concurrency – every task is owned by exactly one region | [`TaskLeakOracle`] |
//! | 2 | Region close = quiescence – no live children + all finalizers done | [`QuiescenceOracle`] |
//! | 3 | Cancellation is a protocol – request → drain → finalize | (state machine tests) |
//! | 4 | Losers are drained – races must cancel AND fully drain losers | [`LoserDrainOracle`] |
//! | 5 | No obligation leaks – permits/acks/leases must be committed or aborted | [`ObligationLeakOracle`] |
//! | 6 | No ambient authority – effects flow through Cx and explicit capabilities | [`AmbientAuthorityOracle`] |
//!
//! Additionally, [`FinalizerOracle`] verifies all registered finalizers ran.

pub mod finalizer;
pub mod loser_drain;
pub mod obligation_leak;
pub mod quiescence;
pub mod task_leak;

pub use finalizer::{FinalizerId, FinalizerOracle, FinalizerViolation};
pub use loser_drain::{LoserDrainOracle, LoserDrainViolation};
pub use obligation_leak::{ObligationLeakOracle, ObligationLeakViolation};
pub use quiescence::{QuiescenceOracle, QuiescenceViolation};
pub use task_leak::{TaskLeakOracle, TaskLeakViolation};

use crate::types::Time;

/// A violation detected by an oracle.
#[derive(Debug, Clone)]
pub enum OracleViolation {
    /// A task leak was detected.
    TaskLeak(TaskLeakViolation),
    /// An obligation leak was detected.
    ObligationLeak(ObligationLeakViolation),
    /// Quiescence violation on region close.
    Quiescence(QuiescenceViolation),
    /// Race losers were not properly drained.
    LoserDrain(LoserDrainViolation),
    /// Finalizers did not all run.
    Finalizer(FinalizerViolation),
}

impl std::fmt::Display for OracleViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TaskLeak(v) => write!(f, "Task leak: {v}"),
            Self::ObligationLeak(v) => write!(f, "Obligation leak: {v}"),
            Self::Quiescence(v) => write!(f, "Quiescence violation: {v}"),
            Self::LoserDrain(v) => write!(f, "Loser drain violation: {v}"),
            Self::Finalizer(v) => write!(f, "Finalizer violation: {v}"),
        }
    }
}

impl std::error::Error for OracleViolation {}

/// Aggregates all oracles for convenient use in lab runtime.
#[derive(Debug, Default)]
pub struct OracleSuite {
    /// Task leak oracle.
    pub task_leak: TaskLeakOracle,
    /// Obligation leak oracle.
    pub obligation_leak: ObligationLeakOracle,
    /// Quiescence oracle.
    pub quiescence: QuiescenceOracle,
    /// Loser drain oracle.
    pub loser_drain: LoserDrainOracle,
    /// Finalizer oracle.
    pub finalizer: FinalizerOracle,
}

impl OracleSuite {
    /// Creates a new oracle suite with all oracles initialized.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Checks all oracles and returns any violations.
    #[must_use]
    pub fn check_all(&self, now: Time) -> Vec<OracleViolation> {
        let mut violations = Vec::new();

        if let Err(v) = self.task_leak.check(now) {
            violations.push(OracleViolation::TaskLeak(v));
        }

        if let Err(v) = self.obligation_leak.check(now) {
            violations.push(OracleViolation::ObligationLeak(v));
        }

        if let Err(v) = self.quiescence.check() {
            violations.push(OracleViolation::Quiescence(v));
        }

        if let Err(v) = self.loser_drain.check() {
            violations.push(OracleViolation::LoserDrain(v));
        }

        if let Err(v) = self.finalizer.check() {
            violations.push(OracleViolation::Finalizer(v));
        }

        violations
    }

    /// Resets all oracles to their initial state.
    pub fn reset(&mut self) {
        self.task_leak.reset();
        self.obligation_leak.reset();
        self.quiescence.reset();
        self.loser_drain.reset();
        self.finalizer.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oracle_suite_default_is_clean() {
        let suite = OracleSuite::new();
        let violations = suite.check_all(Time::ZERO);
        assert!(
            violations.is_empty(),
            "Fresh suite should have no violations"
        );
    }
}
