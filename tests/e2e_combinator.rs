//! Combinator E2E test suite with cancel-correctness verification.
//!
//! This test suite validates the core combinator invariants:
//! - **Loser drain**: Race losers are fully cancelled and drained (not abandoned)
//! - **Obligation safety**: No obligation leaks across combinator branches
//! - **Outcome aggregation**: Correct severity lattice for join outcomes
//! - **Determinism**: All tests reproducible under lab runtime

mod e2e {
    pub mod combinator;
}
