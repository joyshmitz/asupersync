//! Distribution of symbols to replicas with consistency guarantees.
//!
//! Provides [`SymbolDistributor`] for distributing encoded symbols to
//! replicas and tracking acknowledgements with quorum semantics.

use crate::combinator::quorum::{quorum_outcomes, QuorumResult};
use crate::error::ErrorKind;
use crate::record::distributed_region::{ConsistencyLevel, ReplicaInfo};
use crate::types::symbol::ObjectId;
use crate::types::{Outcome, Time};
use std::time::Duration;

use super::assignment::{AssignmentStrategy, SymbolAssigner};
use super::encoding::EncodedState;

// ---------------------------------------------------------------------------
// DistributionConfig
// ---------------------------------------------------------------------------

/// Configuration for symbol distribution.
#[derive(Debug, Clone)]
pub struct DistributionConfig {
    /// Consistency level for distribution.
    pub consistency: ConsistencyLevel,
    /// Timeout for replica acknowledgement.
    pub ack_timeout: Duration,
    /// Maximum concurrent distributions.
    pub max_concurrent: usize,
    /// Whether to use hedged requests.
    pub hedge_enabled: bool,
    /// Hedge delay (send to backup after this delay).
    pub hedge_delay: Duration,
}

impl Default for DistributionConfig {
    fn default() -> Self {
        Self {
            consistency: ConsistencyLevel::Quorum,
            ack_timeout: Duration::from_secs(5),
            max_concurrent: 10,
            hedge_enabled: false,
            hedge_delay: Duration::from_millis(50),
        }
    }
}

// ---------------------------------------------------------------------------
// DistributionResult
// ---------------------------------------------------------------------------

/// Result of a distribution operation.
#[derive(Debug)]
pub struct DistributionResult {
    /// Object ID that was distributed.
    pub object_id: ObjectId,
    /// Number of symbols distributed.
    pub symbols_distributed: u32,
    /// Successful replica acknowledgements.
    pub acks: Vec<ReplicaAck>,
    /// Failed replicas.
    pub failures: Vec<ReplicaFailure>,
    /// Whether quorum was achieved.
    pub quorum_achieved: bool,
    /// Total distribution duration.
    pub duration: Duration,
}

/// Acknowledgement from a replica.
#[derive(Debug, Clone)]
pub struct ReplicaAck {
    /// Identifier of the acknowledging replica.
    pub replica_id: String,
    /// Number of symbols received.
    pub symbols_received: u32,
    /// Timestamp of acknowledgement.
    pub ack_time: Time,
}

/// Failure information for a replica.
#[derive(Debug, Clone)]
pub struct ReplicaFailure {
    /// Identifier of the failed replica.
    pub replica_id: String,
    /// Error description.
    pub error: String,
    /// Error kind for categorization.
    pub error_kind: ErrorKind,
}

// ---------------------------------------------------------------------------
// DistributionMetrics
// ---------------------------------------------------------------------------

/// Metrics for distribution operations.
#[derive(Debug, Default)]
pub struct DistributionMetrics {
    /// Total distribution attempts.
    pub distributions_total: u64,
    /// Successful distributions (quorum achieved).
    pub distributions_successful: u64,
    /// Failed distributions (quorum not achieved).
    pub distributions_failed: u64,
    /// Total symbols sent across all distributions.
    pub symbols_sent_total: u64,
    /// Total acknowledgements received.
    pub acks_received_total: u64,
    /// Count of distributions where quorum was achieved.
    pub quorum_achieved_count: u64,
    /// Count of distributions where quorum was missed.
    pub quorum_missed_count: u64,
}

// ---------------------------------------------------------------------------
// SymbolDistributor
// ---------------------------------------------------------------------------

/// Distributes encoded symbols to replicas.
///
/// Handles symbol assignment, distribution, and quorum-based acknowledgement
/// tracking. The async `distribute` method is intended for runtime use;
/// [`evaluate_outcomes`](Self::evaluate_outcomes) provides a synchronous
/// test path.
pub struct SymbolDistributor {
    config: DistributionConfig,
    /// Metrics for distribution operations.
    pub metrics: DistributionMetrics,
}

impl SymbolDistributor {
    /// Creates a new distributor with the given configuration.
    #[must_use]
    pub fn new(config: DistributionConfig) -> Self {
        Self {
            config,
            metrics: DistributionMetrics::default(),
        }
    }

    /// Returns a reference to the configuration.
    #[must_use]
    pub fn config(&self) -> &DistributionConfig {
        &self.config
    }

    /// Computes the required acknowledgement count for the given consistency
    /// level and replica count.
    #[must_use]
    pub fn required_acks(consistency: ConsistencyLevel, replica_count: usize) -> usize {
        match consistency {
            ConsistencyLevel::One => 1,
            ConsistencyLevel::Quorum => (replica_count / 2) + 1,
            ConsistencyLevel::All => replica_count,
            ConsistencyLevel::Local => 0,
        }
    }

    /// Computes symbol assignments for the given encoded state and replicas.
    #[must_use]
    pub fn compute_assignments(
        encoded: &EncodedState,
        replicas: &[ReplicaInfo],
    ) -> Vec<super::assignment::ReplicaAssignment> {
        let assigner = SymbolAssigner::new(AssignmentStrategy::Full);
        assigner.assign(&encoded.symbols, replicas, encoded.source_count)
    }

    /// Evaluates pre-computed outcomes with quorum semantics.
    ///
    /// This is the synchronous core of the distribution logic. The async
    /// `distribute` method wraps actual I/O around this function.
    ///
    /// # Arguments
    ///
    /// * `encoded` - The encoded state being distributed
    /// * `replicas` - Target replicas (for computing required acks)
    /// * `outcomes` - Pre-computed outcomes from each replica
    /// * `duration` - Time spent distributing
    pub fn evaluate_outcomes(
        &mut self,
        encoded: &EncodedState,
        replicas: &[ReplicaInfo],
        outcomes: Vec<Outcome<ReplicaAck, ReplicaFailure>>,
        duration: Duration,
    ) -> DistributionResult {
        let required = Self::required_acks(self.config.consistency, replicas.len());

        let quorum_result: QuorumResult<ReplicaAck, ReplicaFailure> =
            quorum_outcomes(required, outcomes);

        self.metrics.distributions_total += 1;
        self.metrics.symbols_sent_total += u64::from(encoded.symbols.len() as u32);

        let acks: Vec<ReplicaAck> = quorum_result
            .successes
            .into_iter()
            .map(|(_, ack)| ack)
            .collect();

        let failures: Vec<ReplicaFailure> = quorum_result
            .failures
            .into_iter()
            .filter_map(|(_, f)| match f {
                crate::combinator::quorum::QuorumFailure::Error(e) => Some(e),
                _ => None,
            })
            .collect();

        self.metrics.acks_received_total += acks.len() as u64;

        if quorum_result.quorum_met {
            self.metrics.distributions_successful += 1;
            self.metrics.quorum_achieved_count += 1;
        } else {
            self.metrics.distributions_failed += 1;
            self.metrics.quorum_missed_count += 1;
        }

        DistributionResult {
            object_id: encoded.params.object_id,
            symbols_distributed: encoded.symbols.len() as u32,
            acks,
            failures,
            quorum_achieved: quorum_result.quorum_met,
            duration,
        }
    }
}

impl std::fmt::Debug for SymbolDistributor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SymbolDistributor")
            .field("config", &self.config)
            .field("metrics", &self.metrics)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::symbol::{ObjectParams, Symbol};

    fn create_test_replicas(count: usize) -> Vec<ReplicaInfo> {
        (0..count)
            .map(|i| ReplicaInfo::new(&format!("r{i}"), &format!("addr{i}")))
            .collect()
    }

    fn create_test_symbols(count: usize) -> Vec<Symbol> {
        (0..count)
            .map(|i| Symbol::new_for_test(1, 0, i as u32, &[0u8; 128]))
            .collect()
    }

    fn create_test_encoded_state() -> EncodedState {
        EncodedState {
            params: ObjectParams::new_for_test(1, 1024),
            symbols: create_test_symbols(10),
            source_count: 8,
            repair_count: 2,
            original_size: 1000,
            encoded_at: Time::ZERO,
        }
    }

    fn make_ack(replica_id: &str, count: u32) -> ReplicaAck {
        ReplicaAck {
            replica_id: replica_id.to_string(),
            symbols_received: count,
            ack_time: Time::ZERO,
        }
    }

    fn make_failure(replica_id: &str) -> ReplicaFailure {
        ReplicaFailure {
            replica_id: replica_id.to_string(),
            error: "connection refused".to_string(),
            error_kind: ErrorKind::NodeUnavailable,
        }
    }

    #[test]
    fn distribute_with_quorum_consistency() {
        let config = DistributionConfig {
            consistency: ConsistencyLevel::Quorum,
            ..Default::default()
        };
        let mut distributor = SymbolDistributor::new(config);

        let replicas = create_test_replicas(3);
        let encoded = create_test_encoded_state();

        // 2 of 3 replicas succeed (quorum = 2).
        let outcomes = vec![
            Outcome::Ok(make_ack("r0", 10)),
            Outcome::Ok(make_ack("r1", 10)),
            Outcome::Err(make_failure("r2")),
        ];

        let result =
            distributor.evaluate_outcomes(&encoded, &replicas, outcomes, Duration::from_millis(50));

        assert!(result.quorum_achieved);
        assert_eq!(result.acks.len(), 2);
        assert_eq!(result.failures.len(), 1);
    }

    #[test]
    fn distribute_with_all_consistency() {
        let config = DistributionConfig {
            consistency: ConsistencyLevel::All,
            ..Default::default()
        };
        let mut distributor = SymbolDistributor::new(config);

        let replicas = create_test_replicas(3);
        let encoded = create_test_encoded_state();

        // Only 2 of 3 respond.
        let outcomes = vec![
            Outcome::Ok(make_ack("r0", 10)),
            Outcome::Ok(make_ack("r1", 10)),
            Outcome::Err(make_failure("r2")),
        ];

        let result =
            distributor.evaluate_outcomes(&encoded, &replicas, outcomes, Duration::from_millis(50));

        assert!(!result.quorum_achieved);
    }

    #[test]
    fn distribute_tracks_failures() {
        let config = DistributionConfig::default();
        let mut distributor = SymbolDistributor::new(config);

        let replicas = create_test_replicas(3);
        let encoded = create_test_encoded_state();

        let outcomes = vec![
            Outcome::Ok(make_ack("r0", 10)),
            Outcome::Ok(make_ack("r1", 10)),
            Outcome::Err(make_failure("r2")),
        ];

        let result =
            distributor.evaluate_outcomes(&encoded, &replicas, outcomes, Duration::from_millis(50));

        assert!(!result.failures.is_empty());
        assert_eq!(result.failures.len(), 1);
        assert_eq!(result.failures[0].replica_id, "r2");
    }

    #[test]
    fn distribution_metrics_updated() {
        let config = DistributionConfig::default();
        let mut distributor = SymbolDistributor::new(config);

        let replicas = create_test_replicas(3);
        let encoded = create_test_encoded_state();

        let outcomes = vec![
            Outcome::Ok(make_ack("r0", 10)),
            Outcome::Ok(make_ack("r1", 10)),
            Outcome::Ok(make_ack("r2", 10)),
        ];

        distributor.evaluate_outcomes(&encoded, &replicas, outcomes, Duration::from_millis(50));

        assert_eq!(distributor.metrics.distributions_total, 1);
        assert_eq!(distributor.metrics.distributions_successful, 1);
        assert!(distributor.metrics.symbols_sent_total > 0);
        assert_eq!(distributor.metrics.acks_received_total, 3);
    }

    #[test]
    fn distribute_to_no_replicas() {
        let config = DistributionConfig::default();
        let mut distributor = SymbolDistributor::new(config);

        let replicas: Vec<ReplicaInfo> = vec![];
        let encoded = create_test_encoded_state();

        // No outcomes.
        let outcomes: Vec<Outcome<ReplicaAck, ReplicaFailure>> = vec![];

        let result =
            distributor.evaluate_outcomes(&encoded, &replicas, outcomes, Duration::from_millis(50));

        // Quorum required = (0/2)+1 = 1 for Quorum level, but with 0 replicas
        // this fails because required(1) > total(0).
        assert!(!result.quorum_achieved);
    }

    #[test]
    fn required_acks_calculation() {
        assert_eq!(
            SymbolDistributor::required_acks(ConsistencyLevel::One, 3),
            1
        );
        assert_eq!(
            SymbolDistributor::required_acks(ConsistencyLevel::Quorum, 3),
            2
        );
        assert_eq!(
            SymbolDistributor::required_acks(ConsistencyLevel::Quorum, 5),
            3
        );
        assert_eq!(
            SymbolDistributor::required_acks(ConsistencyLevel::All, 3),
            3
        );
        assert_eq!(
            SymbolDistributor::required_acks(ConsistencyLevel::Local, 3),
            0
        );
    }

    #[test]
    fn local_consistency_always_succeeds() {
        let config = DistributionConfig {
            consistency: ConsistencyLevel::Local,
            ..Default::default()
        };
        let mut distributor = SymbolDistributor::new(config);

        let replicas = create_test_replicas(3);
        let encoded = create_test_encoded_state();

        // Even with all failures, Local consistency needs 0 acks.
        let outcomes = vec![
            Outcome::Err(make_failure("r0")),
            Outcome::Err(make_failure("r1")),
            Outcome::Err(make_failure("r2")),
        ];

        let result =
            distributor.evaluate_outcomes(&encoded, &replicas, outcomes, Duration::from_millis(50));

        assert!(result.quorum_achieved);
    }
}
