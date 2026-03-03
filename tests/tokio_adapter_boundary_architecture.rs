//! Contract tests for Tokio adapter boundary architecture (2oh2u.7.2).
//!
//! Validates enforceable adapter invariants, outcome contracts, structured
//! replay evidence requirements, and rch-offloaded validation commands.

#![allow(missing_docs)]

use std::path::Path;

fn load_doc() -> String {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("docs/tokio_adapter_boundary_architecture.md");
    std::fs::read_to_string(path).expect("adapter boundary architecture document must exist")
}

#[test]
fn architecture_doc_exists_and_is_substantial() {
    let doc = load_doc();
    assert!(
        doc.len() > 9_000,
        "adapter boundary architecture doc should be substantial, got {} bytes",
        doc.len()
    );
}

#[test]
fn architecture_doc_references_correct_bead_and_metadata() {
    let doc = load_doc();
    for token in [
        "asupersync-2oh2u.7.2",
        "[T7.2]",
        "Maintained by",
        "WhiteDesert",
        "Version",
        "1.1.0",
    ] {
        assert!(doc.contains(token), "missing metadata token: {token}");
    }
}

#[test]
fn architecture_doc_declares_non_negotiable_runtime_invariants() {
    let doc = load_doc();
    for token in [
        "No ambient authority",
        "Structured concurrency",
        "Cancellation is a protocol",
        "No obligation leaks",
        "Outcome severity lattice",
    ] {
        assert!(doc.contains(token), "missing invariant token: {token}");
    }
}

#[test]
fn architecture_doc_enforces_hard_tokio_boundary_rules() {
    let doc = load_doc();
    for token in [
        "RULE 1: No Tokio in core runtime paths.",
        "RULE 2: Adapters are in a separate crate.",
        "RULE 3: Cx must cross the boundary.",
        "RULE 4: Region ownership is non-negotiable.",
        "asupersync-tokio-compat",
    ] {
        assert!(doc.contains(token), "missing boundary-rule token: {token}");
    }
}

#[test]
fn architecture_doc_has_success_failure_cancellation_outcome_matrix() {
    let doc = load_doc();
    assert!(
        doc.contains("Boundary Outcome Contract (Success/Failure/Cancellation)"),
        "must include boundary outcome contract section"
    );
    for token in [
        "Success Contract",
        "Failure Contract",
        "Cancellation Contract",
        "Deterministic Assertion",
        "Runtime bridge (`with_tokio_context`)",
        "Hyper bridge (`hyper_bridge`)",
        "SQLx runtime adapter (`sqlx_runtime`)",
        "Tonic transport bridge (`tonic_transport`)",
        "Outcome::Cancelled",
    ] {
        assert!(
            doc.contains(token),
            "missing outcome-contract token: {token}"
        );
    }
}

#[test]
fn architecture_doc_declares_forbidden_patterns_explicitly() {
    let doc = load_doc();
    for token in [
        "NEVER: Embed a Hidden Tokio Runtime",
        "NEVER: Bypass Cx for Convenience",
        "NEVER: Spawn Untracked Background Tasks",
        "NEVER: Swallow Cancellation",
    ] {
        assert!(
            doc.contains(token),
            "missing forbidden-pattern token: {token}"
        );
    }
}

#[test]
fn architecture_doc_requires_structured_logs_and_replay_artifacts() {
    let doc = load_doc();
    assert!(
        doc.contains("Structured Logs and Replay Artifacts"),
        "must include structured logs and replay artifacts section"
    );
    for token in [
        "`correlation_id`",
        "`adapter_path`",
        "`trace_id`",
        "`decision_id`",
        "`replay_seed`",
        "`artifact_uri`",
        "artifacts/tokio_adapter_boundary/<run-id>/adapter_events.jsonl",
        "artifacts/tokio_adapter_boundary/<run-id>/replay_summary.json",
        "artifacts/tokio_adapter_boundary/<run-id>/failure_triage.md",
        "Hard-fail quality gate policy",
    ] {
        assert!(
            doc.contains(token),
            "missing replay-evidence token: {token}"
        );
    }
}

#[test]
fn architecture_doc_includes_rch_validation_bundle() {
    let doc = load_doc();
    for token in [
        "rch exec -- cargo test --test tokio_adapter_boundary_architecture -- --nocapture",
        "rch exec -- cargo check --all-targets -q",
        "rch exec -- cargo fmt --check",
        "rch exec -- cargo clippy --all-targets -- -D warnings",
    ] {
        assert!(
            doc.contains(token),
            "missing validation command token: {token}"
        );
    }
}

#[test]
fn architecture_doc_links_contract_and_source_evidence() {
    let doc = load_doc();
    for token in [
        "docs/tokio_interop_target_ranking.md",
        "docs/tokio_functional_parity_contract.md",
        "docs/tokio_nonfunctional_closure_criteria.md",
        "docs/tokio_evidence_checklist.md",
        "asupersync-tokio-compat/src/runtime.rs",
        "asupersync-tokio-compat/src/executor.rs",
        "asupersync-tokio-compat/src/timer.rs",
        "asupersync-tokio-compat/src/io.rs",
        "asupersync-tokio-compat/src/cancel.rs",
        "tests/tokio_adapter_boundary_architecture.rs",
    ] {
        assert!(doc.contains(token), "missing evidence-link token: {token}");
    }
}

#[test]
fn architecture_doc_revision_history_tracks_latest_update() {
    let doc = load_doc();
    assert!(
        doc.contains("| 2026-03-03 | WhiteDesert |"),
        "revision history should include WhiteDesert row"
    );
    assert!(
        doc.contains("| 2026-03-03 | SapphireHill | Initial architecture (v1.0) |"),
        "revision history should retain initial baseline row"
    );
}
