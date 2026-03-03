//! Contract tests for cross-track unit-test quality thresholds (2oh2u.10.11).
//!
//! These tests enforce presence of required gate IDs, prerequisite bead bindings,
//! leak-oracle requirements, required artifacts, and deterministic CI command tokens.

#![allow(missing_docs)]

use std::collections::BTreeSet;
use std::path::Path;

fn load_doc() -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("docs/tokio_unit_quality_threshold_contract.md");
    std::fs::read_to_string(path).expect("unit quality threshold contract must exist")
}

fn extract_gate_ids(doc: &str) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    for line in doc.lines() {
        let trimmed = line.trim().trim_start_matches('|').trim();
        if let Some(id) = trimmed.split('|').next() {
            let id = id.trim().trim_matches('`');
            if id.starts_with("UQ-") {
                ids.insert(id.to_string());
            }
        }
    }
    ids
}

#[test]
fn doc_exists_and_is_substantial() {
    let doc = load_doc();
    assert!(
        doc.len() > 3000,
        "document should be substantial, got {} bytes",
        doc.len()
    );
}

#[test]
fn doc_references_correct_bead_and_program() {
    let doc = load_doc();
    assert!(
        doc.contains("asupersync-2oh2u.10.11"),
        "document must reference bead 2oh2u.10.11"
    );
    assert!(doc.contains("[T8.11]"), "document must reference T8.11");
    assert!(
        doc.contains("asupersync-2oh2u"),
        "document must reference the TOKIO-REPLACE program root"
    );
}

#[test]
fn doc_references_all_prerequisite_unit_matrix_beads() {
    let doc = load_doc();
    for token in [
        "asupersync-2oh2u.2.9",
        "asupersync-2oh2u.3.9",
        "asupersync-2oh2u.4.10",
        "asupersync-2oh2u.5.11",
        "asupersync-2oh2u.6.12",
        "asupersync-2oh2u.7.10",
    ] {
        assert!(
            doc.contains(token),
            "missing prerequisite bead token: {token}"
        );
    }
}

#[test]
fn doc_defines_required_test_categories() {
    let doc = load_doc();
    for token in [
        "Happy path",
        "Edge cases",
        "Error paths",
        "Cancellation race invariants",
        "Leak invariants",
    ] {
        assert!(doc.contains(token), "missing category token: {token}");
    }
}

#[test]
fn doc_defines_full_uq_gate_set() {
    let doc = load_doc();
    let gate_ids = extract_gate_ids(&doc);
    for id in ["UQ-01", "UQ-02", "UQ-03", "UQ-04", "UQ-05", "UQ-06"] {
        assert!(gate_ids.contains(id), "missing gate id token: {id}");
    }
}

#[test]
fn doc_requires_leak_oracles_for_concurrency_paths() {
    let doc = load_doc();
    for token in [
        "no_task_leak",
        "no_obligation_leak",
        "region_close_quiescence",
        "loser_drain_complete",
        "oracle_not_applicable",
    ] {
        assert!(
            doc.contains(token),
            "missing leak-oracle contract token: {token}"
        );
    }
}

#[test]
fn doc_defines_required_ci_commands_with_rch() {
    let doc = load_doc();
    assert!(
        doc.contains("rch exec --"),
        "document must require rch exec for heavy checks"
    );
    for token in [
        "rch exec -- cargo check --all-targets",
        "rch exec -- cargo clippy --all-targets -- -D warnings",
        "rch exec -- cargo fmt --check",
        "rch exec -- cargo test --test tokio_unit_quality_threshold_contract -- --nocapture",
    ] {
        assert!(doc.contains(token), "missing CI command token: {token}");
    }
}

#[test]
fn doc_defines_required_artifact_bundle() {
    let doc = load_doc();
    for token in [
        "tokio_unit_quality_manifest.json",
        "tokio_unit_quality_report.md",
        "tokio_unit_quality_failures.json",
        "tokio_unit_quality_triage_pointers.txt",
    ] {
        assert!(doc.contains(token), "missing artifact token: {token}");
    }
}

#[test]
fn doc_defines_manifest_schema_fields() {
    let doc = load_doc();
    for token in [
        "track_id",
        "bead_id",
        "commit_sha",
        "category_counts",
        "threshold_result",
        "oracle_status",
        "repro_commands",
        "artifact_links",
    ] {
        assert!(doc.contains(token), "missing manifest field token: {token}");
    }
}

#[test]
fn doc_defines_failure_routing_contract() {
    let doc = load_doc();
    for token in [
        "gate_id",
        "track_id",
        "bead_id",
        "severity",
        "owner",
        "repro_command",
        "first_failing_commit",
    ] {
        assert!(
            doc.contains(token),
            "missing failure routing token: {token}"
        );
    }
}

#[test]
fn doc_binds_downstream_t8_tasks() {
    let doc = load_doc();
    for token in ["asupersync-2oh2u.10.12", "asupersync-2oh2u.10.9"] {
        assert!(
            doc.contains(token),
            "missing downstream dependency token: {token}"
        );
    }
}
