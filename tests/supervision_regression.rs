#![allow(missing_docs)]

use asupersync::supervision::BackoffStrategy;
use std::time::Duration;

#[test]
fn test_backoff_handles_invalid_multiplier() {
    // Negative multiplier should fallback to safe default (2.0) or handle gracefully
    let backoff = BackoffStrategy::Exponential {
        initial: Duration::from_millis(100),
        max: Duration::from_secs(10),
        multiplier: -5.0,
    };
    // Should not panic
    let delay = backoff.delay_for_attempt(1);
    assert!(delay.is_some());

    // NaN multiplier
    let backoff = BackoffStrategy::Exponential {
        initial: Duration::from_millis(100),
        max: Duration::from_secs(10),
        multiplier: f64::NAN,
    };
    // Should not panic
    let delay = backoff.delay_for_attempt(1);
    assert!(delay.is_some());

    // Infinite multiplier
    let backoff = BackoffStrategy::Exponential {
        initial: Duration::from_millis(100),
        max: Duration::from_secs(10),
        multiplier: f64::INFINITY,
    };
    // Should cap at max or fallback
    let delay = backoff.delay_for_attempt(10).unwrap();
    assert!(delay <= Duration::from_secs(10));
}
