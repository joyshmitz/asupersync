//! T6.10 — Deterministic integration and fault-injection suites for database and messaging.
//!
//! Bead: `asupersync-2oh2u.6.10`
//! Track: T6 (Database and messaging ecosystem closure)
//!
//! Validates client behavior under simulated faults, retry semantics,
//! error classification parity, circuit breaker integration, rate limiter
//! integration, and failure escalation chains.
//!
//! These tests do NOT require running database servers. They exercise:
//! - Error type construction and classification methods
//! - Retry policy arithmetic and state machine behavior
//! - Circuit breaker state transitions under fault scenarios
//! - Rate limiter token bucket behavior
//! - Pool configuration and statistics contracts
//! - Cross-backend error classification equivalence

// ─── Retry Policy Integration ────────────────────────────────────────────────

mod retry_integration {
    use asupersync::combinator::retry::{
        calculate_delay, total_delay_budget, AlwaysRetry, NeverRetry, RetryError, RetryIf,
        RetryPolicy, RetryPredicate, RetryResult, RetryState,
    };
    use std::time::Duration;

    #[test]
    fn c_rty_01_exponential_backoff_formula_matches_contract() {
        // Contract C-RTY-01: delay = min(initial_delay * multiplier^(attempt-1), max_delay) * (1+jitter)
        // Test with zero jitter for determinism.
        let policy = RetryPolicy::new()
            .with_initial_delay(Duration::from_millis(100))
            .with_multiplier(2.0)
            .with_max_delay(Duration::from_secs(30))
            .no_jitter();

        // Attempt 1 (first retry): 100ms * 2^0 = 100ms
        assert_eq!(calculate_delay(&policy, 1, None), Duration::from_millis(100));
        // Attempt 2: 100ms * 2^1 = 200ms
        assert_eq!(calculate_delay(&policy, 2, None), Duration::from_millis(200));
        // Attempt 3: 100ms * 2^2 = 400ms
        assert_eq!(calculate_delay(&policy, 3, None), Duration::from_millis(400));
        // Attempt 4: 100ms * 2^3 = 800ms
        assert_eq!(calculate_delay(&policy, 4, None), Duration::from_millis(800));
    }

    #[test]
    fn c_rty_01_delay_capped_at_max_delay() {
        let policy = RetryPolicy::new()
            .with_initial_delay(Duration::from_secs(1))
            .with_multiplier(10.0)
            .with_max_delay(Duration::from_secs(5))
            .no_jitter();

        // Attempt 1: 1s (within cap)
        assert_eq!(calculate_delay(&policy, 1, None), Duration::from_secs(1));
        // Attempt 2: 10s -> capped at 5s
        assert_eq!(calculate_delay(&policy, 2, None), Duration::from_secs(5));
        // Attempt 10: still capped at 5s
        assert_eq!(calculate_delay(&policy, 10, None), Duration::from_secs(5));
    }

    #[test]
    fn c_rty_01_jitter_stays_within_bounds() {
        let policy = RetryPolicy::new()
            .with_initial_delay(Duration::from_millis(100))
            .with_jitter(0.1);

        let mut rng = asupersync::util::det_rng::DetRng::new(42);
        let base = Duration::from_millis(100);
        let max_with_jitter = Duration::from_millis(110); // 100 * 1.1

        for _ in 0..200 {
            let delay = calculate_delay(&policy, 1, Some(&mut rng));
            assert!(delay >= base, "delay {delay:?} < base {base:?}");
            assert!(
                delay <= max_with_jitter,
                "delay {delay:?} > max_with_jitter {max_with_jitter:?}"
            );
        }
    }

    #[test]
    fn c_rty_01_deterministic_jitter_with_same_seed() {
        let policy = RetryPolicy::new()
            .with_initial_delay(Duration::from_millis(100))
            .with_jitter(0.1);

        let mut rng1 = asupersync::util::det_rng::DetRng::new(99);
        let mut rng2 = asupersync::util::det_rng::DetRng::new(99);

        for attempt in 1..=5 {
            let d1 = calculate_delay(&policy, attempt, Some(&mut rng1));
            let d2 = calculate_delay(&policy, attempt, Some(&mut rng2));
            assert_eq!(d1, d2, "attempt {attempt}: same seed must produce same delay");
        }
    }

    #[test]
    fn c_rty_01_total_budget_calculation() {
        let policy = RetryPolicy::new()
            .with_max_attempts(4) // 1 initial + 3 retries
            .with_initial_delay(Duration::from_millis(100))
            .with_multiplier(2.0)
            .with_max_delay(Duration::from_secs(30))
            .no_jitter();

        // Budget = delay(1) + delay(2) + delay(3) = 100 + 200 + 400 = 700ms
        let budget = total_delay_budget(&policy);
        assert_eq!(budget, Duration::from_millis(700));
    }

    #[test]
    fn c_rty_01_fixed_delay_produces_constant_delays() {
        let policy = RetryPolicy::fixed_delay(Duration::from_millis(500), 5);

        for attempt in 1..=4 {
            let delay = calculate_delay(&policy, attempt, None);
            assert_eq!(
                delay,
                Duration::from_millis(500),
                "fixed delay inconsistent at attempt {attempt}"
            );
        }
    }

    #[test]
    fn c_rty_01_immediate_policy_has_zero_delays() {
        let policy = RetryPolicy::immediate(5);

        for attempt in 0..=5 {
            let delay = calculate_delay(&policy, attempt, None);
            assert_eq!(delay, Duration::ZERO, "immediate policy has non-zero delay");
        }
    }

    #[test]
    fn c_rty_01_overflow_safe_at_extreme_attempts() {
        let policy = RetryPolicy::new()
            .with_initial_delay(Duration::from_secs(1))
            .with_multiplier(2.0)
            .with_max_delay(Duration::from_secs(60))
            .no_jitter();

        // Very large exponents must not panic
        let d63 = calculate_delay(&policy, 63, None);
        assert!(d63 <= Duration::from_secs(60));
        let d100 = calculate_delay(&policy, 100, None);
        assert!(d100 <= Duration::from_secs(60));
        let dmax = calculate_delay(&policy, u32::MAX, None);
        assert!(dmax <= Duration::from_secs(60));
    }

    #[test]
    fn c_rty_05_cancel_aware_retry_state() {
        let policy = RetryPolicy::new().with_max_attempts(5);
        let mut state = RetryState::new(policy);

        assert!(state.has_attempts_remaining());
        assert_eq!(state.attempts_remaining(), 5);

        // First attempt: no delay
        let d = state.next_attempt(None);
        assert_eq!(d, Some(Duration::ZERO));
        assert_eq!(state.attempt, 1);

        // Cancel mid-retry
        state.cancel();

        assert!(!state.has_attempts_remaining());
        assert_eq!(state.attempts_remaining(), 0);
        assert!(state.next_attempt(None).is_none());
    }

    #[test]
    fn retry_state_exhaustion_at_max_attempts() {
        let policy = RetryPolicy::new().with_max_attempts(3);
        let mut state = RetryState::new(policy);

        let _ = state.next_attempt(None); // attempt 1
        let _ = state.next_attempt(None); // attempt 2
        let _ = state.next_attempt(None); // attempt 3
        assert!(!state.has_attempts_remaining());
        assert!(state.next_attempt(None).is_none());
    }

    #[test]
    fn retry_state_into_error_preserves_metadata() {
        let policy = RetryPolicy::new()
            .with_max_attempts(3)
            .with_initial_delay(Duration::from_millis(100))
            .no_jitter();
        let mut state = RetryState::new(policy);

        state.next_attempt(None); // attempt 1, delay=0
        state.next_attempt(None); // attempt 2, delay=100ms
        state.next_attempt(None); // attempt 3, delay=200ms

        let error = state.into_error("db_timeout");
        assert_eq!(error.attempts, 3);
        assert_eq!(error.final_error, "db_timeout");
        // Total delay = 100ms + 200ms = 300ms
        assert_eq!(error.total_delay, Duration::from_millis(300));
    }

    #[test]
    fn retry_predicate_always_retry() {
        let pred = AlwaysRetry;
        assert!(pred.should_retry(&"anything", 1));
        assert!(pred.should_retry(&"anything", 100));
    }

    #[test]
    fn retry_predicate_never_retry() {
        let pred = NeverRetry;
        assert!(!pred.should_retry(&"anything", 1));
        assert!(!pred.should_retry(&"anything", 100));
    }

    #[test]
    fn retry_predicate_selective() {
        let pred = RetryIf(|e: &&str, _attempt: u32| e.contains("transient"));
        assert!(pred.should_retry(&"transient timeout", 1));
        assert!(!pred.should_retry(&"permanent auth failure", 1));
    }

    #[test]
    fn retry_error_display_includes_context() {
        let err = RetryError::new("connection refused", 3, Duration::from_millis(300));
        let display = err.to_string();
        assert!(display.contains("3 attempts"), "{display}");
        assert!(display.contains("connection refused"), "{display}");
        assert!(display.contains("300"), "{display}"); // delay info
    }

    #[test]
    fn retry_result_into_outcome_round_trip() {
        let ok: RetryResult<i32, &str> = RetryResult::Ok(42);
        let outcome = ok.into_outcome();
        assert!(outcome.is_ok());

        let failed: RetryResult<i32, &str> =
            RetryResult::Failed(RetryError::new("err", 3, Duration::ZERO));
        let outcome = failed.into_outcome();
        assert!(outcome.is_err());
    }
}

// ─── Error Classification Parity ─────────────────────────────────────────────

mod error_classification {
    use asupersync::database::postgres::PgError;

    #[test]
    fn c_err_04_pg_serialization_failure() {
        let err = PgError::Server {
            code: "40001".to_string(),
            message: "could not serialize".to_string(),
            detail: None,
            hint: None,
        };
        assert!(err.is_serialization_failure());
        assert!(err.is_transient());
        assert!(err.is_retryable());
        assert!(!err.is_deadlock());
        assert!(!err.is_unique_violation());
        assert!(!err.is_constraint_violation());
        assert!(!err.is_connection_error());
        assert_eq!(err.error_code(), Some("40001"));
    }

    #[test]
    fn c_err_04_pg_deadlock() {
        let err = PgError::Server {
            code: "40P01".to_string(),
            message: "deadlock detected".to_string(),
            detail: None,
            hint: None,
        };
        assert!(err.is_deadlock());
        assert!(err.is_transient()); // 40xxx is transient
        assert!(err.is_retryable());
        assert!(!err.is_serialization_failure());
    }

    #[test]
    fn c_err_04_pg_unique_violation() {
        let err = PgError::Server {
            code: "23505".to_string(),
            message: "duplicate key".to_string(),
            detail: None,
            hint: None,
        };
        assert!(err.is_unique_violation());
        assert!(err.is_constraint_violation());
        assert!(!err.is_transient());
        assert!(!err.is_retryable());
    }

    #[test]
    fn c_err_04_pg_constraint_violation_class() {
        // All 23xxx codes are constraint violations
        for code in ["23000", "23001", "23502", "23503", "23505", "23514"] {
            let err = PgError::Server {
                code: code.to_string(),
                message: "constraint".to_string(),
                detail: None,
                hint: None,
            };
            assert!(
                err.is_constraint_violation(),
                "code {code} should be constraint violation"
            );
            assert!(
                !err.is_transient(),
                "code {code} should NOT be transient"
            );
        }
    }

    #[test]
    fn c_err_04_pg_connection_error_class_08() {
        let err = PgError::Server {
            code: "08003".to_string(),
            message: "connection does not exist".to_string(),
            detail: None,
            hint: None,
        };
        assert!(err.is_connection_error());
        assert!(err.is_transient()); // 08xxx is transient
    }

    #[test]
    fn c_err_04_pg_connection_error_variants() {
        // I/O error
        let io_err = PgError::Io(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken"));
        assert!(io_err.is_connection_error());
        assert!(io_err.is_transient());

        // Connection closed
        let closed = PgError::ConnectionClosed;
        assert!(closed.is_connection_error());
        assert!(closed.is_transient());

        // TLS required
        let tls = PgError::TlsRequired;
        assert!(tls.is_connection_error());
    }

    #[test]
    fn c_err_04_pg_resource_exhaustion_transient() {
        // SQLSTATE class 53 = insufficient resources
        let err = PgError::Server {
            code: "53000".to_string(),
            message: "insufficient resources".to_string(),
            detail: None,
            hint: None,
        };
        assert!(err.is_transient());
        assert!(err.is_retryable());
        assert!(!err.is_connection_error());
    }

    #[test]
    fn c_err_04_pg_syntax_error_not_transient() {
        let err = PgError::Server {
            code: "42601".to_string(),
            message: "syntax error".to_string(),
            detail: None,
            hint: None,
        };
        assert!(!err.is_transient());
        assert!(!err.is_retryable());
        assert!(!err.is_connection_error());
    }
}

mod mysql_error_classification {
    use asupersync::database::mysql::MySqlError;

    #[test]
    fn c_err_04_mysql_deadlock_1213() {
        let err = MySqlError::Server {
            code: 1213,
            sql_state: "40001".to_string(),
            message: "Deadlock found".to_string(),
        };
        assert!(err.is_serialization_failure());
        assert!(err.is_deadlock());
        assert!(err.is_transient());
        assert!(err.is_retryable());
        assert!(!err.is_unique_violation());
        assert!(!err.is_constraint_violation());
        assert_eq!(err.error_code(), Some("1213".to_string()));
    }

    #[test]
    fn c_err_04_mysql_lock_wait_timeout_1205() {
        let err = MySqlError::Server {
            code: 1205,
            sql_state: "HY000".to_string(),
            message: "Lock wait timeout exceeded".to_string(),
        };
        assert!(err.is_deadlock()); // 1205 is treated as deadlock for retry
        assert!(err.is_transient());
        assert!(err.is_retryable());
        assert!(!err.is_serialization_failure()); // only 1213
    }

    #[test]
    fn c_err_04_mysql_unique_violation_1062() {
        let err = MySqlError::Server {
            code: 1062,
            sql_state: "23000".to_string(),
            message: "Duplicate entry".to_string(),
        };
        assert!(err.is_unique_violation());
        assert!(err.is_constraint_violation());
        assert!(!err.is_transient());
        assert!(!err.is_retryable());
    }

    #[test]
    fn c_err_04_mysql_foreign_key_violations() {
        for code in [1451u16, 1452] {
            let err = MySqlError::Server {
                code,
                sql_state: "23000".to_string(),
                message: "Cannot add or update a child row".to_string(),
            };
            assert!(
                err.is_constraint_violation(),
                "code {code} should be constraint violation"
            );
            assert!(
                !err.is_unique_violation(),
                "code {code} should NOT be unique violation"
            );
            assert!(!err.is_transient());
        }
    }

    #[test]
    fn c_err_04_mysql_connection_lost() {
        for code in [2006u16, 2013] {
            let err = MySqlError::Server {
                code,
                sql_state: "HY000".to_string(),
                message: "MySQL server has gone away".to_string(),
            };
            assert!(
                err.is_connection_error(),
                "code {code} should be connection error"
            );
            assert!(
                err.is_transient(),
                "code {code} should be transient"
            );
        }
    }

    #[test]
    fn c_err_04_mysql_io_and_closed_are_connection_errors() {
        let io_err = MySqlError::Io(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken"));
        assert!(io_err.is_connection_error());
        assert!(io_err.is_transient());

        let closed = MySqlError::ConnectionClosed;
        assert!(closed.is_connection_error());
        assert!(closed.is_transient());
    }

    #[test]
    fn c_err_04_mysql_sql_state_accessor() {
        let err = MySqlError::Server {
            code: 1213,
            sql_state: "40001".to_string(),
            message: "test".to_string(),
        };
        assert_eq!(err.sql_state(), Some("40001"));

        let non_server = MySqlError::ConnectionClosed;
        assert_eq!(non_server.sql_state(), None);
    }
}

mod sqlite_error_classification {
    use asupersync::database::sqlite::SqliteError;

    #[test]
    fn c_err_04_sqlite_busy() {
        let err = SqliteError::Sqlite("database is locked".to_string());
        assert!(err.is_busy());
        assert!(err.is_transient());
        assert!(err.is_retryable());
        assert!(!err.is_locked()); // locked is a different condition
    }

    #[test]
    fn c_err_04_sqlite_busy_explicit_code() {
        let err = SqliteError::Sqlite("SQLITE_BUSY: database is busy".to_string());
        assert!(err.is_busy());
        assert!(err.is_transient());
    }

    #[test]
    fn c_err_04_sqlite_locked() {
        let err = SqliteError::Sqlite("database table is locked".to_string());
        assert!(err.is_locked());
        assert!(err.is_transient());
        assert!(err.is_retryable());
    }

    #[test]
    fn c_err_04_sqlite_locked_explicit_code() {
        let err = SqliteError::Sqlite("SQLITE_LOCKED: table is locked".to_string());
        assert!(err.is_locked());
        assert!(err.is_transient());
    }

    #[test]
    fn c_err_04_sqlite_unique_constraint() {
        let err = SqliteError::Sqlite("UNIQUE constraint failed: users.email".to_string());
        assert!(err.is_unique_violation());
        assert!(err.is_constraint_violation());
        assert!(!err.is_transient());
        assert!(!err.is_retryable());
    }

    #[test]
    fn c_err_04_sqlite_constraint_variants() {
        let cases = [
            "NOT NULL constraint failed: users.name",
            "FOREIGN KEY constraint failed",
            "CHECK constraint failed: age_positive",
            "SQLITE_CONSTRAINT: not null",
        ];
        for msg in &cases {
            let err = SqliteError::Sqlite(msg.to_string());
            assert!(
                err.is_constraint_violation(),
                "'{msg}' should be constraint violation"
            );
            assert!(
                !err.is_transient(),
                "'{msg}' should NOT be transient"
            );
        }
    }

    #[test]
    fn c_err_04_sqlite_connection_errors() {
        let io_err = SqliteError::Io(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe"));
        assert!(io_err.is_connection_error());
        assert!(io_err.is_transient());

        let closed = SqliteError::ConnectionClosed;
        assert!(closed.is_connection_error());
        assert!(closed.is_transient());

        let poisoned = SqliteError::LockPoisoned;
        assert!(poisoned.is_connection_error());
    }

    #[test]
    fn c_err_04_sqlite_error_code_synthetic() {
        let busy = SqliteError::Sqlite("database is locked".to_string());
        assert_eq!(busy.error_code(), Some("SQLITE_BUSY"));

        let locked = SqliteError::Sqlite("database table is locked".to_string());
        assert_eq!(locked.error_code(), Some("SQLITE_LOCKED"));

        let constraint = SqliteError::Sqlite("UNIQUE constraint failed".to_string());
        assert_eq!(constraint.error_code(), Some("SQLITE_CONSTRAINT"));

        let io = SqliteError::Io(std::io::Error::new(std::io::ErrorKind::Other, "disk"));
        assert_eq!(io.error_code(), Some("SQLITE_IOERR"));

        // Generic messages without a known keyword map to None
        let generic = SqliteError::Sqlite("unknown error".to_string());
        assert_eq!(generic.error_code(), None);

        // But messages containing "SQLITE_ERROR" DO map to Some
        let explicit = SqliteError::Sqlite("SQLITE_ERROR: something".to_string());
        assert_eq!(explicit.error_code(), Some("SQLITE_ERROR"));
    }
}

// ─── Cross-Backend Error Classification Equivalence ──────────────────────────

mod cross_backend_equivalence {
    use asupersync::database::mysql::MySqlError;
    use asupersync::database::postgres::PgError;
    use asupersync::database::sqlite::SqliteError;

    /// C-ERR-04: All backends must classify the same categories consistently.
    #[test]
    fn serialization_failure_classification_consistent() {
        let pg = PgError::Server {
            code: "40001".to_string(),
            message: "serialization failure".to_string(),
            detail: None,
            hint: None,
        };
        let mysql = MySqlError::Server {
            code: 1213,
            sql_state: "40001".to_string(),
            message: "Deadlock found".to_string(),
        };
        // SQLite does not have serialization failure (N/A per contract)

        assert!(pg.is_serialization_failure());
        assert!(mysql.is_serialization_failure());

        // Both should be transient and retryable
        assert!(pg.is_transient());
        assert!(mysql.is_transient());
        assert!(pg.is_retryable());
        assert!(mysql.is_retryable());
    }

    #[test]
    fn unique_violation_classification_consistent() {
        let pg = PgError::Server {
            code: "23505".to_string(),
            message: "duplicate key value violates unique constraint".to_string(),
            detail: None,
            hint: None,
        };
        let mysql = MySqlError::Server {
            code: 1062,
            sql_state: "23000".to_string(),
            message: "Duplicate entry".to_string(),
        };
        let sqlite = SqliteError::Sqlite("UNIQUE constraint failed: users.email".to_string());

        // All must agree: unique violation + constraint violation
        assert!(pg.is_unique_violation());
        assert!(mysql.is_unique_violation());
        assert!(sqlite.is_unique_violation());

        assert!(pg.is_constraint_violation());
        assert!(mysql.is_constraint_violation());
        assert!(sqlite.is_constraint_violation());

        // None should be transient
        assert!(!pg.is_transient());
        assert!(!mysql.is_transient());
        assert!(!sqlite.is_transient());
    }

    #[test]
    fn connection_error_classification_consistent() {
        let pg = PgError::ConnectionClosed;
        let mysql = MySqlError::ConnectionClosed;
        let sqlite = SqliteError::ConnectionClosed;

        assert!(pg.is_connection_error());
        assert!(mysql.is_connection_error());
        assert!(sqlite.is_connection_error());

        // All connection errors should be transient
        assert!(pg.is_transient());
        assert!(mysql.is_transient());
        assert!(sqlite.is_transient());
    }

    #[test]
    fn io_error_classification_consistent() {
        let make_io = || std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken");

        let pg = PgError::Io(make_io());
        let mysql = MySqlError::Io(make_io());
        let sqlite = SqliteError::Io(make_io());

        assert!(pg.is_connection_error());
        assert!(mysql.is_connection_error());
        assert!(sqlite.is_connection_error());

        assert!(pg.is_transient());
        assert!(mysql.is_transient());
        assert!(sqlite.is_transient());
    }

    /// Non-retryable errors must be classified consistently across backends.
    #[test]
    fn non_retryable_categories_consistent() {
        // Constraint violations are never retryable (per C-RTY-02)
        let pg_constraint = PgError::Server {
            code: "23505".to_string(),
            message: "unique".to_string(),
            detail: None,
            hint: None,
        };
        let mysql_constraint = MySqlError::Server {
            code: 1062,
            sql_state: "23000".to_string(),
            message: "duplicate".to_string(),
        };
        let sqlite_constraint =
            SqliteError::Sqlite("UNIQUE constraint failed: t.col".to_string());

        assert!(!pg_constraint.is_retryable());
        assert!(!mysql_constraint.is_retryable());
        assert!(!sqlite_constraint.is_retryable());

        // Syntax errors are never retryable
        let pg_syntax = PgError::Server {
            code: "42601".to_string(),
            message: "syntax error".to_string(),
            detail: None,
            hint: None,
        };
        assert!(!pg_syntax.is_retryable());
    }
}

// ─── C-RTY-02: Transaction Retry Eligibility ─────────────────────────────────

mod transaction_retry_eligibility {
    use asupersync::database::transaction::RetryPolicy;
    use std::time::Duration;

    #[test]
    fn c_rty_02_retry_policy_structure_matches_contract() {
        // Contract C-RTY-02 specifies fields: max_retries, base_delay, max_delay
        let policy = RetryPolicy::default_retry();
        assert_eq!(policy.max_retries, 3);
        assert_eq!(policy.base_delay, Duration::from_millis(50));
        assert_eq!(policy.max_delay, Duration::from_secs(2));
    }

    #[test]
    fn c_rty_02_transaction_retry_delay_formula() {
        // delay = min(base_delay * 2^attempt, max_delay)
        let policy = RetryPolicy {
            max_retries: 5,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
        };

        assert_eq!(policy.delay_for(0), Duration::from_millis(100)); // 100 * 2^0
        assert_eq!(policy.delay_for(1), Duration::from_millis(200)); // 100 * 2^1
        assert_eq!(policy.delay_for(2), Duration::from_millis(400)); // 100 * 2^2
        assert_eq!(policy.delay_for(3), Duration::from_millis(800)); // 100 * 2^3
    }

    #[test]
    fn c_rty_02_transaction_retry_capped() {
        let policy = RetryPolicy {
            max_retries: 10,
            base_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(2),
        };

        // 500 * 8 = 4000ms -> capped at 2000ms
        assert_eq!(policy.delay_for(3), Duration::from_secs(2));
        // Still capped at extreme attempts
        assert_eq!(policy.delay_for(100), Duration::from_secs(2));
    }

    #[test]
    fn c_rty_02_retry_policy_none_disables() {
        let policy = RetryPolicy::none();
        assert_eq!(policy.max_retries, 0);
        assert_eq!(policy.base_delay, Duration::ZERO);
    }

    #[test]
    fn c_rty_02_pg_retry_eligibility() {
        use asupersync::database::postgres::PgError;

        // Retryable: 40001 (serialization failure)
        let retryable = PgError::Server {
            code: "40001".to_string(),
            message: "serialization failure".to_string(),
            detail: None,
            hint: None,
        };
        assert!(retryable.is_serialization_failure());

        // NOT retryable: constraint violations
        let not_retryable = PgError::Server {
            code: "23505".to_string(),
            message: "duplicate".to_string(),
            detail: None,
            hint: None,
        };
        assert!(!not_retryable.is_serialization_failure());
    }

    #[test]
    fn c_rty_02_mysql_retry_eligibility() {
        use asupersync::database::mysql::MySqlError;

        // Retryable: 1213 (deadlock) and 1205 (lock wait timeout)
        let deadlock = MySqlError::Server {
            code: 1213,
            sql_state: "40001".to_string(),
            message: "Deadlock found".to_string(),
        };
        assert!(deadlock.is_deadlock());

        let lock_wait = MySqlError::Server {
            code: 1205,
            sql_state: "HY000".to_string(),
            message: "Lock wait timeout".to_string(),
        };
        assert!(lock_wait.is_deadlock());
    }

    #[test]
    fn c_rty_02_sqlite_retry_eligibility() {
        use asupersync::database::sqlite::SqliteError;

        // Retryable: SQLITE_BUSY and SQLITE_LOCKED
        let busy = SqliteError::Sqlite("database is locked".to_string());
        assert!(busy.is_busy());

        let locked = SqliteError::Sqlite("database table is locked".to_string());
        assert!(locked.is_locked());

        // NOT retryable: constraint violations
        let constraint = SqliteError::Sqlite("UNIQUE constraint failed".to_string());
        assert!(!constraint.is_busy());
        assert!(!constraint.is_locked());
    }
}

// ─── Circuit Breaker Integration ─────────────────────────────────────────────

mod circuit_breaker_integration {
    use asupersync::combinator::circuit_breaker::{
        CircuitBreaker, CircuitBreakerError, CircuitBreakerPolicy, State,
    };
    use asupersync::types::Time;
    use std::time::Duration;

    fn default_policy() -> CircuitBreakerPolicy {
        CircuitBreakerPolicy {
            failure_threshold: 3,
            success_threshold: 2,
            open_duration: Duration::from_secs(30),
            ..Default::default()
        }
    }

    #[test]
    fn c_fpr_01_closed_to_open_on_threshold() {
        let cb = CircuitBreaker::new(default_policy());
        let now = Time::from_millis(0);

        assert!(matches!(cb.state(), State::Closed { .. }));

        // Use call() to record failures through the proper API
        for _ in 0..3 {
            let _: Result<(), CircuitBreakerError<String>> =
                cb.call(now, || Err::<(), String>("fail".into()));
        }

        assert!(matches!(cb.state(), State::Open { .. }));
    }

    #[test]
    fn c_fpr_01_open_circuit_rejects_calls() {
        let cb = CircuitBreaker::new(default_policy());
        let now = Time::from_millis(0);

        // Trip the circuit via call()
        for _ in 0..3 {
            let _: Result<(), CircuitBreakerError<String>> =
                cb.call(now, || Err::<(), String>("fail".into()));
        }

        // Calls should be rejected when open
        let result: Result<i32, CircuitBreakerError<String>> = cb.call(now, || Ok(42));
        assert!(matches!(result, Err(CircuitBreakerError::Open { .. })));
    }

    #[test]
    fn c_fpr_01_open_transitions_to_half_open_after_duration() {
        let cb = CircuitBreaker::new(default_policy());
        let now = Time::from_millis(0);

        // Trip the circuit
        for _ in 0..3 {
            let _: Result<(), CircuitBreakerError<String>> =
                cb.call(now, || Err::<(), String>("fail".into()));
        }
        assert!(matches!(cb.state(), State::Open { .. }));

        // After open_duration, a call attempt transitions to HalfOpen
        let later = Time::from_millis(31_000);
        // should_allow at later time triggers half-open transition
        let result = cb.should_allow(later);
        assert!(result.is_ok(), "should allow probe in half-open");
    }

    #[test]
    fn c_fpr_01_metrics_track_success_and_failure() {
        let cb = CircuitBreaker::new(default_policy());
        let now = Time::from_millis(0);

        let metrics = cb.metrics();
        assert_eq!(metrics.total_success, 0);
        assert_eq!(metrics.total_failure, 0);

        // Record successes and failures via call()
        let _: Result<i32, CircuitBreakerError<String>> = cb.call(now, || Ok(42));
        let _: Result<i32, CircuitBreakerError<String>> = cb.call(now, || Ok(99));
        let _: Result<(), CircuitBreakerError<String>> =
            cb.call(now, || Err::<(), String>("err".into()));

        let metrics = cb.metrics();
        assert_eq!(metrics.total_success, 2);
        assert_eq!(metrics.total_failure, 1);
    }

    #[test]
    fn c_fpr_01_failure_streak_tracked() {
        let cb = CircuitBreaker::new(default_policy());
        let now = Time::from_millis(0);

        for _ in 0..2 {
            let _: Result<(), CircuitBreakerError<String>> =
                cb.call(now, || Err::<(), String>("fail".into()));
        }

        let metrics = cb.metrics();
        assert_eq!(metrics.current_failure_streak, 2);
    }
}

// ─── Rate Limiter Integration ────────────────────────────────────────────────

mod rate_limiter_integration {
    use asupersync::combinator::rate_limit::{RateLimitPolicy, RateLimiter};
    use asupersync::types::Time;
    use std::time::Duration;

    fn default_limiter() -> RateLimiter {
        RateLimiter::new(RateLimitPolicy {
            name: "test".into(),
            rate: 10,
            period: Duration::from_secs(1),
            burst: 5,
            ..Default::default()
        })
    }

    #[test]
    fn c_fpr_02_acquire_within_burst() {
        let limiter = default_limiter();
        let now = Time::from_millis(0);

        // Should be able to acquire up to burst size
        for i in 0..5 {
            assert!(
                limiter.try_acquire(1, now),
                "should acquire token {i} within burst"
            );
        }
    }

    #[test]
    fn c_fpr_02_reject_beyond_burst() {
        let limiter = default_limiter();
        let now = Time::from_millis(0);

        // Exhaust burst
        for _ in 0..5 {
            let _ = limiter.try_acquire(1, now);
        }

        // Next acquire should fail (no tokens)
        assert!(!limiter.try_acquire(1, now), "should reject beyond burst");
    }

    #[test]
    fn c_fpr_02_tokens_replenish_over_time() {
        let limiter = default_limiter();
        let now = Time::from_millis(0);

        // Exhaust all tokens
        for _ in 0..5 {
            let _ = limiter.try_acquire(1, now);
        }
        assert!(!limiter.try_acquire(1, now));

        // After 1 second (10 tokens/sec), tokens should replenish
        let later = Time::from_millis(1000);
        assert!(
            limiter.try_acquire(1, later),
            "tokens should replenish after period"
        );
    }

    #[test]
    fn c_fpr_02_retry_after_reports_wait_time() {
        let limiter = default_limiter();
        let now = Time::from_millis(0);

        // Exhaust tokens
        for _ in 0..5 {
            let _ = limiter.try_acquire(1, now);
        }

        // retry_after should return a positive duration
        let wait = limiter.retry_after(1, now);
        assert!(wait > Duration::ZERO, "retry_after should be positive when tokens exhausted");
    }

    #[test]
    fn c_fpr_02_metrics_track_allowed_and_rejected() {
        let limiter = default_limiter();
        let now = Time::from_millis(0);

        // Two successful acquires
        assert!(limiter.try_acquire(1, now));
        assert!(limiter.try_acquire(1, now));

        let metrics = limiter.metrics();
        assert!(
            metrics.total_allowed >= 2,
            "metrics should track allowed count: got {}",
            metrics.total_allowed
        );
    }
}

// ─── Pool Configuration Contract ─────────────────────────────────────────────

mod pool_contracts {
    use asupersync::database::pool::DbPoolConfig;
    use std::time::Duration;

    #[test]
    fn c_pool_02_config_defaults_match_contract() {
        // Contract C-POOL-02 specifies parameter defaults
        let config = DbPoolConfig::default();

        assert_eq!(config.min_idle, 1);
        assert_eq!(config.max_size, 10);
        assert_eq!(config.connection_timeout, Duration::from_secs(30));
        assert_eq!(config.idle_timeout, Duration::from_secs(600));
        assert_eq!(config.max_lifetime, Duration::from_secs(3600));
        assert!(config.validate_on_checkout);
    }

    #[test]
    fn c_pool_02_config_builder_methods() {
        let config = DbPoolConfig::with_max_size(20)
            .min_idle(2)
            .validate_on_checkout(false)
            .idle_timeout(Duration::from_secs(300))
            .max_lifetime(Duration::from_secs(1800))
            .connection_timeout(Duration::from_secs(10));

        assert_eq!(config.max_size, 20);
        assert_eq!(config.min_idle, 2);
        assert!(!config.validate_on_checkout);
        assert_eq!(config.idle_timeout, Duration::from_secs(300));
        assert_eq!(config.max_lifetime, Duration::from_secs(1800));
        assert_eq!(config.connection_timeout, Duration::from_secs(10));
    }
}

// ─── Failure Escalation Chain ────────────────────────────────────────────────

mod failure_escalation {
    use asupersync::combinator::retry::{RetryError, RetryFailure, RetryResult};
    use asupersync::types::cancel::CancelReason;
    use std::time::Duration;

    #[test]
    fn c_fpr_03_error_chain_preserves_original_error() {
        // C-FPR-03: Each level MUST preserve the original error for diagnostics.
        let original_error = "SQLSTATE 40001: serialization failure";
        let retry_error = RetryError::new(original_error, 3, Duration::from_millis(300));

        // The original error must be accessible through the chain
        assert_eq!(retry_error.final_error, original_error);
        assert_eq!(retry_error.attempts, 3);
        assert_eq!(retry_error.total_delay, Duration::from_millis(300));

        // Display includes context
        let display = retry_error.to_string();
        assert!(
            display.contains(original_error),
            "display must contain original error: {display}"
        );
    }

    #[test]
    fn c_fpr_03_retry_failure_preserves_cancel_reason() {
        let reason = CancelReason::timeout();
        let failure: RetryFailure<&str> = RetryFailure::Cancelled(reason);

        let display = failure.to_string();
        assert!(display.contains("cancelled"), "must mention cancellation: {display}");
    }

    #[test]
    fn c_fpr_03_retry_result_into_result_preserves_info() {
        // Exhausted
        let exhausted: RetryResult<i32, &str> =
            RetryResult::Failed(RetryError::new("db_error", 5, Duration::from_millis(700)));
        let result = exhausted.into_result();
        match result {
            Err(RetryFailure::Exhausted(e)) => {
                assert_eq!(e.final_error, "db_error");
                assert_eq!(e.attempts, 5);
                assert_eq!(e.total_delay, Duration::from_millis(700));
            }
            _ => panic!("expected Exhausted"),
        }

        // Cancelled
        let cancelled: RetryResult<i32, &str> = RetryResult::Cancelled(CancelReason::timeout());
        let result = cancelled.into_result();
        assert!(matches!(result, Err(RetryFailure::Cancelled(_))));
    }

    #[test]
    fn c_fpr_03_error_map_preserves_metadata() {
        let err = RetryError::new("original", 3, Duration::from_millis(500));
        let mapped = err.map(|e| format!("wrapped: {e}"));

        assert_eq!(mapped.final_error, "wrapped: original");
        assert_eq!(mapped.attempts, 3);
        assert_eq!(mapped.total_delay, Duration::from_millis(500));
    }
}

// ─── Contract Artifact Validation ────────────────────────────────────────────

mod contract_artifacts {
    use std::collections::HashSet;

    fn load_t65_json() -> serde_json::Value {
        let raw = include_str!("../docs/tokio_db_pool_transaction_observability_contracts.json");
        serde_json::from_str(raw).expect("T6.5 JSON must be valid")
    }

    fn load_t69_json() -> serde_json::Value {
        let raw = include_str!("../docs/tokio_retry_idempotency_failure_contracts.json");
        serde_json::from_str(raw).expect("T6.9 JSON must be valid")
    }

    #[test]
    fn t65_error_method_parity_fully_implemented() {
        let json = load_t65_json();
        let err_contracts = json["contracts"]["error_normalization"].as_array().unwrap();
        let c_err_02 = err_contracts
            .iter()
            .find(|c| c["id"] == "C-ERR-02")
            .expect("C-ERR-02 must exist");
        assert_eq!(
            c_err_02["status"].as_str().unwrap(),
            "implemented",
            "C-ERR-02 should be implemented"
        );
    }

    #[test]
    fn t65_transaction_retry_fully_implemented() {
        let json = load_t65_json();
        let txn_contracts = json["contracts"]["transaction"].as_array().unwrap();
        let c_txn_04 = txn_contracts
            .iter()
            .find(|c| c["id"] == "C-TXN-04")
            .expect("C-TXN-04 must exist");
        assert_eq!(
            c_txn_04["status"].as_str().unwrap(),
            "implemented",
            "C-TXN-04 should be implemented after MySQL/SQLite retry added"
        );

        // All backends should now have retry
        let backends = c_txn_04["backends_with_retry"].as_array().unwrap();
        let backend_names: HashSet<&str> =
            backends.iter().map(|b| b.as_str().unwrap()).collect();
        assert!(backend_names.contains("postgresql"));
        assert!(backend_names.contains("mysql"));
        assert!(backend_names.contains("sqlite"));

        let missing = c_txn_04["backends_missing_retry"].as_array().unwrap();
        assert!(missing.is_empty(), "no backends should be missing retry");
    }

    #[test]
    fn t69_retry_transaction_eligibility_fully_implemented() {
        let json = load_t69_json();
        let retry_contracts = json["contracts"]["retry"].as_array().unwrap();
        let c_rty_02 = retry_contracts
            .iter()
            .find(|c| c["id"] == "C-RTY-02")
            .expect("C-RTY-02 must exist");
        assert_eq!(
            c_rty_02["status"].as_str().unwrap(),
            "implemented",
            "C-RTY-02 should be implemented"
        );

        let backends = c_rty_02["backends_with_retry"].as_array().unwrap();
        assert_eq!(backends.len(), 3);
    }

    #[test]
    fn t69_error_classification_extended_implemented() {
        let json = load_t69_json();
        let err_contracts = json["contracts"]["error_classification"].as_array().unwrap();
        let c_err_04 = err_contracts
            .iter()
            .find(|c| c["id"] == "C-ERR-04")
            .expect("C-ERR-04 must exist");
        assert_eq!(
            c_err_04["status"].as_str().unwrap(),
            "implemented",
            "C-ERR-04 should be fully implemented"
        );

        // Verify each backend has all methods implemented
        let backend_status = &c_err_04["backend_status"];
        for backend in ["postgresql", "mysql", "sqlite"] {
            let status = &backend_status[backend];
            for (method, val) in status.as_object().unwrap() {
                let s = val.as_str().unwrap();
                assert!(
                    s == "implemented" || s == "not_applicable",
                    "{backend}.{method} should be implemented or N/A, got {s}"
                );
            }
        }
    }

    #[test]
    fn t69_blocking_gaps_retry_and_error_closed() {
        let json = load_t69_json();
        let gaps = json["summary"]["blocking_gaps"].as_array().unwrap();

        let rty_g1 = gaps.iter().find(|g| g["id"] == "RTY-G1").expect("RTY-G1");
        assert_eq!(rty_g1["severity"].as_str().unwrap(), "closed");

        let err_g1 = gaps.iter().find(|g| g["id"] == "ERR-G1").expect("ERR-G1");
        assert_eq!(err_g1["severity"].as_str().unwrap(), "closed");

        let err_g2 = gaps.iter().find(|g| g["id"] == "ERR-G2").expect("ERR-G2");
        assert_eq!(err_g2["severity"].as_str().unwrap(), "closed");
    }

    #[test]
    fn t69_source_modules_reference_real_paths() {
        let json = load_t69_json();
        let modules = json["source_modules"].as_object().unwrap();

        for (key, path_val) in modules {
            let path = path_val.as_str().unwrap();
            assert!(
                std::path::Path::new(path).extension().is_some_and(|ext| ext.eq_ignore_ascii_case("rs")),
                "module {key} path {path} should end in .rs"
            );
            assert!(
                std::path::Path::new(path).exists(),
                "module {key} path {path} should exist on disk"
            );
        }
    }

    #[test]
    fn t65_and_t69_domain_counts_consistent() {
        let t65 = load_t65_json();
        let t69 = load_t69_json();

        // T6.5 summary counts
        let t65_summary = &t65["summary"]["domains"];
        for (domain, info) in t65_summary.as_object().unwrap() {
            let count = info["count"].as_u64().unwrap();
            let impl_count = info["implemented"].as_u64().unwrap()
                + info.get("partial").and_then(|v| v.as_u64()).unwrap_or(0)
                + info.get("defined").and_then(|v| v.as_u64()).unwrap_or(0)
                + info.get("not_implemented").and_then(|v| v.as_u64()).unwrap_or(0);
            assert_eq!(
                count, impl_count,
                "T6.5 domain {domain}: count {count} != sum {impl_count}"
            );
        }

        // T6.9 summary counts
        let t69_summary = &t69["summary"]["domains"];
        for (domain, info) in t69_summary.as_object().unwrap() {
            let count = info["count"].as_u64().unwrap();
            let sum = info["implemented"].as_u64().unwrap()
                + info.get("partial").and_then(|v| v.as_u64()).unwrap_or(0)
                + info.get("defined").and_then(|v| v.as_u64()).unwrap_or(0)
                + info.get("not_implemented").and_then(|v| v.as_u64()).unwrap_or(0);
            assert_eq!(
                count, sum,
                "T6.9 domain {domain}: count {count} != sum {sum}"
            );
        }
    }
}
