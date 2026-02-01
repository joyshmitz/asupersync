//! Cancel-correctness tests for combinators.
//!
//! These tests verify the CRITICAL invariant that race losers are fully
//! drained (not just dropped), and that obligations are properly resolved.

pub mod loser_drain;
pub mod obligation_cleanup;
