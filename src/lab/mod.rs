//! Deterministic lab runtime for testing.
//!
//! The lab runtime provides:
//!
//! - Virtual time (no wall-clock dependencies)
//! - Deterministic scheduling (same seed → same execution)
//! - Trace capture and replay
//! - Schedule exploration (DPOR-style)
//! - Test oracles for invariant verification

pub mod config;
pub mod oracle;
pub mod replay;
pub mod runtime;

pub use config::LabConfig;
pub use oracle::{
    FinalizerId, FinalizerOracle, FinalizerViolation, LoserDrainOracle, LoserDrainViolation,
    ObligationLeakOracle, ObligationLeakViolation, OracleSuite, OracleViolation, QuiescenceOracle,
    QuiescenceViolation, TaskLeakOracle, TaskLeakViolation,
};
pub use runtime::LabRuntime;
