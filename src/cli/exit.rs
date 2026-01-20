//! Semantic exit codes for Asupersync CLI tools.
//!
//! Exit codes follow common conventions and are in the valid range (0-125).
//! Codes 126-255 are reserved by shells for special purposes.

/// Semantic exit codes for CLI tools.
///
/// These follow common conventions and provide machine-readable status.
/// All codes are in the valid range (0-125).
pub struct ExitCode;

impl ExitCode {
    /// Success - operation completed without errors.
    pub const SUCCESS: i32 = 0;

    /// User error - bad arguments, missing files, invalid input.
    pub const USER_ERROR: i32 = 1;

    /// Runtime error - test failed, invariant violated.
    pub const RUNTIME_ERROR: i32 = 2;

    /// Internal error - bug in the tool itself.
    pub const INTERNAL_ERROR: i32 = 3;

    /// Operation cancelled - by user signal or timeout.
    pub const CANCELLED: i32 = 4;

    /// Partial success - some items succeeded, some failed.
    pub const PARTIAL_SUCCESS: i32 = 5;

    // Application-specific codes (10-125)

    /// Test failure - one or more tests failed.
    pub const TEST_FAILURE: i32 = 10;

    /// Oracle violation detected during testing.
    pub const ORACLE_VIOLATION: i32 = 11;

    /// Determinism check failed - non-reproducible execution.
    pub const DETERMINISM_FAILURE: i32 = 12;

    /// Trace mismatch during replay.
    pub const TRACE_MISMATCH: i32 = 13;

    /// Get human-readable description of an exit code.
    #[must_use]
    pub const fn description(code: i32) -> &'static str {
        match code {
            0 => "success",
            1 => "user error (invalid input/arguments)",
            2 => "runtime error",
            3 => "internal error (bug)",
            4 => "cancelled",
            5 => "partial success",
            10 => "test failure",
            11 => "oracle violation",
            12 => "determinism failure",
            13 => "trace mismatch",
            _ => "unknown",
        }
    }

    /// Check if an exit code indicates success (code 0).
    #[must_use]
    pub const fn is_success(code: i32) -> bool {
        code == Self::SUCCESS
    }

    /// Check if an exit code indicates any kind of failure (non-zero).
    #[must_use]
    pub const fn is_failure(code: i32) -> bool {
        code != Self::SUCCESS
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn exit_codes_are_distinct() {
        let codes = vec![
            ExitCode::SUCCESS,
            ExitCode::USER_ERROR,
            ExitCode::RUNTIME_ERROR,
            ExitCode::INTERNAL_ERROR,
            ExitCode::CANCELLED,
            ExitCode::PARTIAL_SUCCESS,
            ExitCode::TEST_FAILURE,
            ExitCode::ORACLE_VIOLATION,
            ExitCode::DETERMINISM_FAILURE,
            ExitCode::TRACE_MISMATCH,
        ];

        let unique: HashSet<_> = codes.iter().collect();
        assert_eq!(codes.len(), unique.len(), "Exit codes must be unique");
    }

    #[test]
    fn exit_codes_in_valid_range() {
        let codes = vec![
            ExitCode::SUCCESS,
            ExitCode::USER_ERROR,
            ExitCode::RUNTIME_ERROR,
            ExitCode::INTERNAL_ERROR,
            ExitCode::CANCELLED,
            ExitCode::PARTIAL_SUCCESS,
            ExitCode::TEST_FAILURE,
            ExitCode::ORACLE_VIOLATION,
            ExitCode::DETERMINISM_FAILURE,
            ExitCode::TRACE_MISMATCH,
        ];

        for code in codes {
            assert!(
                (0..=125).contains(&code),
                "Exit code {code} out of valid range (0-125)"
            );
        }
    }

    #[test]
    fn exit_code_descriptions_not_empty() {
        let codes = [0, 1, 2, 3, 4, 5, 10, 11, 12, 13];
        for code in codes {
            let desc = ExitCode::description(code);
            assert!(!desc.is_empty(), "Description for code {code} is empty");
            assert_ne!(desc, "unknown", "Description for code {code} is 'unknown'");
        }
    }

    #[test]
    fn unknown_code_description() {
        assert_eq!(ExitCode::description(99), "unknown");
        assert_eq!(ExitCode::description(-1), "unknown");
    }

    #[test]
    fn is_success_and_failure() {
        assert!(ExitCode::is_success(0));
        assert!(!ExitCode::is_success(1));
        assert!(!ExitCode::is_failure(0));
        assert!(ExitCode::is_failure(1));
    }
}
