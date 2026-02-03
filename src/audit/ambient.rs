//! Ambient authority detection patterns and regression tests.
//!
//! This module documents all known ambient authority patterns in the codebase
//! and provides grep patterns for CI enforcement. Each finding is categorized
//! by severity and includes the rationale for exemption (if applicable).
//!
//! # Categories
//!
//! - **Time**: Direct `Instant::now()` / `SystemTime::now()` bypassing Cx time capability.
//! - **Spawn**: Direct `std::thread::spawn` bypassing Cx/scheduler.
//! - **Entropy**: Direct `getrandom` / `rand` bypassing Cx entropy capability.
//! - **IO**: Direct `std::net` / `std::fs` bypassing Cx IO capability.
//!
//! # Exemptions
//!
//! Some uses are intentionally exempt:
//! - `src/util/entropy.rs` — This IS the entropy provider; it must call OS RNG.
//! - `src/fs/` — This IS the IO wrapper; it must call OS filesystem.
//! - `src/runtime/blocking_pool.rs` — Thread pool needs real threads by design.
//! - Test code (`#[cfg(test)]`) — Tests may use ambient authority freely.

/// Known ambient authority violations with their status.
#[derive(Debug, Clone)]
pub struct AmbientFinding {
    /// Source file (relative to src/).
    pub file: &'static str,
    /// Approximate line number.
    pub line: u32,
    /// Category of ambient authority.
    pub category: AmbientCategory,
    /// Severity level.
    pub severity: Severity,
    /// Description of the violation.
    pub description: &'static str,
    /// Whether this is an intentional exemption.
    pub exempt: bool,
    /// Reason for exemption (if exempt).
    pub exemption_reason: Option<&'static str>,
}

/// Category of ambient authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AmbientCategory {
    /// Direct wall-clock time access.
    Time,
    /// Direct thread spawning.
    Spawn,
    /// Direct entropy/RNG access.
    Entropy,
    /// Direct network/filesystem IO.
    Io,
}

/// Severity of the finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    /// Informational — documented, low risk.
    Info,
    /// Warning — should be addressed but not blocking.
    Warning,
    /// Critical — breaks capability invariants, must be fixed.
    Critical,
}

/// All known ambient authority findings in the codebase.
///
/// This list should be kept in sync with actual code. CI tests verify
/// that no NEW ambient authority is introduced beyond what's listed here.
pub const KNOWN_FINDINGS: &[AmbientFinding] = &[
    // ── Time ────────────────────────────────────────────────────────────
    AmbientFinding {
        file: "web/middleware.rs",
        line: 80,
        category: AmbientCategory::Time,
        severity: Severity::Warning,
        description: "Instant::now() in TimeoutMiddleware::call()",
        exempt: false,
        exemption_reason: None,
    },
    AmbientFinding {
        file: "time/driver.rs",
        line: 38,
        category: AmbientCategory::Time,
        severity: Severity::Info,
        description: "WallClock epoch initialization",
        exempt: true,
        exemption_reason: Some("Timer driver is the time provider"),
    },
    AmbientFinding {
        file: "server/connection.rs",
        line: 123,
        category: AmbientCategory::Time,
        severity: Severity::Warning,
        description: "Instant::now() in ConnectionManager::accept()",
        exempt: false,
        exemption_reason: None,
    },
    AmbientFinding {
        file: "server/shutdown.rs",
        line: 175,
        category: AmbientCategory::Time,
        severity: Severity::Warning,
        description: "Instant::now() in ShutdownSignal::begin_drain()",
        exempt: false,
        exemption_reason: None,
    },
    AmbientFinding {
        file: "runtime/blocking_pool.rs",
        line: 194,
        category: AmbientCategory::Time,
        severity: Severity::Info,
        description: "Instant::now() in blocking pool timeout",
        exempt: true,
        exemption_reason: Some("Blocking pool operates outside async runtime"),
    },
    // ── Spawn ───────────────────────────────────────────────────────────
    AmbientFinding {
        file: "http/h1/listener.rs",
        line: 254,
        category: AmbientCategory::Spawn,
        severity: Severity::Critical,
        description: "std::thread::spawn for per-connection handler",
        exempt: false,
        exemption_reason: None,
    },
    AmbientFinding {
        file: "time/sleep.rs",
        line: 421,
        category: AmbientCategory::Spawn,
        severity: Severity::Warning,
        description: "Fallback timer thread in Sleep::poll()",
        exempt: true,
        exemption_reason: Some("Documented fallback; used only when no timer driver"),
    },
    AmbientFinding {
        file: "runtime/blocking_pool.rs",
        line: 589,
        category: AmbientCategory::Spawn,
        severity: Severity::Info,
        description: "Worker thread spawning in blocking pool",
        exempt: true,
        exemption_reason: Some("Blocking pool requires real OS threads by design"),
    },
    // ── Entropy ─────────────────────────────────────────────────────────
    AmbientFinding {
        file: "net/websocket/frame.rs",
        line: 717,
        category: AmbientCategory::Entropy,
        severity: Severity::Critical,
        description: "getrandom::fill() for WebSocket mask key",
        exempt: false,
        exemption_reason: None,
    },
    AmbientFinding {
        file: "net/websocket/handshake.rs",
        line: 61,
        category: AmbientCategory::Entropy,
        severity: Severity::Critical,
        description: "getrandom::fill() for WebSocket client key",
        exempt: false,
        exemption_reason: None,
    },
    AmbientFinding {
        file: "server/connection.rs",
        line: 230,
        category: AmbientCategory::Spawn,
        severity: Severity::Critical,
        description: "std::thread::spawn for connection handler lifecycle",
        exempt: false,
        exemption_reason: None,
    },
    AmbientFinding {
        file: "http/h1/stream.rs",
        line: 1140,
        category: AmbientCategory::Spawn,
        severity: Severity::Critical,
        description: "std::thread::spawn in HTTP/1 stream handling",
        exempt: false,
        exemption_reason: None,
    },
    // ── IO ──────────────────────────────────────────────────────────────
    AmbientFinding {
        file: "web/debug.rs",
        line: 35,
        category: AmbientCategory::Io,
        severity: Severity::Warning,
        description: "std::net::TcpListener in DebugServer",
        exempt: true,
        exemption_reason: Some("Debug server is intentionally outside runtime"),
    },
];

/// Count findings by severity.
#[must_use]
pub fn count_by_severity(severity: Severity) -> usize {
    KNOWN_FINDINGS
        .iter()
        .filter(|f| f.severity == severity && !f.exempt)
        .count()
}

/// Count non-exempt findings.
#[must_use]
pub fn unresolved_count() -> usize {
    KNOWN_FINDINGS.iter().filter(|f| !f.exempt).count()
}

/// Grep patterns for CI enforcement.
///
/// These patterns should be run against `src/` (excluding test code)
/// to detect new ambient authority introductions.
pub const GREP_PATTERNS: &[(&str, AmbientCategory)] = &[
    (r"Instant::now\(\)", AmbientCategory::Time),
    (r"SystemTime::now\(\)", AmbientCategory::Time),
    (r"std::thread::spawn", AmbientCategory::Spawn),
    (r"thread::spawn", AmbientCategory::Spawn),
    (r"getrandom::", AmbientCategory::Entropy),
    (r"rand::thread_rng", AmbientCategory::Entropy),
    (r"std::net::TcpListener", AmbientCategory::Io),
    (r"std::net::TcpStream", AmbientCategory::Io),
    (r"std::fs::File::open", AmbientCategory::Io),
    (r"std::fs::File::create", AmbientCategory::Io),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_findings_are_documented() {
        assert!(
            !KNOWN_FINDINGS.is_empty(),
            "Findings list should not be empty"
        );
    }

    #[test]
    fn critical_findings_exist() {
        let critical = count_by_severity(Severity::Critical);
        // We know about 3 critical findings (HTTP spawn, 2 WebSocket entropy).
        assert!(
            critical >= 3,
            "Expected at least 3 critical findings, got {critical}"
        );
    }

    #[test]
    fn exempt_findings_have_reasons() {
        for finding in KNOWN_FINDINGS {
            if finding.exempt {
                assert!(
                    finding.exemption_reason.is_some(),
                    "Exempt finding in {} has no reason",
                    finding.file
                );
            }
        }
    }

    #[test]
    fn grep_patterns_cover_all_categories() {
        let categories: std::collections::HashSet<_> =
            GREP_PATTERNS.iter().map(|(_, cat)| *cat).collect();
        assert!(categories.contains(&AmbientCategory::Time));
        assert!(categories.contains(&AmbientCategory::Spawn));
        assert!(categories.contains(&AmbientCategory::Entropy));
        assert!(categories.contains(&AmbientCategory::Io));
    }

    #[test]
    fn unresolved_count_tracks_non_exempt() {
        let unresolved = unresolved_count();
        let total = KNOWN_FINDINGS.len();
        let exempt = KNOWN_FINDINGS.iter().filter(|f| f.exempt).count();
        assert_eq!(unresolved, total - exempt);
    }

    #[test]
    fn severity_ordering() {
        assert!(Severity::Info < Severity::Warning);
        assert!(Severity::Warning < Severity::Critical);
    }
}
