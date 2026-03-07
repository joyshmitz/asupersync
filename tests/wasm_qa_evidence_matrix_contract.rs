//! WASM QA evidence matrix contract invariants (3qv04.8.1).

#![allow(missing_docs)]

use serde_json::Value;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

const DOC_PATH: &str = "docs/wasm_qa_evidence_matrix_contract.md";
const ARTIFACT_PATH: &str = "artifacts/wasm_qa_evidence_matrix_v1.json";
const RUNNER_SCRIPT_PATH: &str = "scripts/run_wasm_qa_evidence_smoke.sh";

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn load_doc() -> String {
    std::fs::read_to_string(repo_root().join(DOC_PATH))
        .expect("failed to load wasm qa evidence doc")
}

fn load_artifact() -> Value {
    let raw = std::fs::read_to_string(repo_root().join(ARTIFACT_PATH))
        .expect("failed to load wasm qa evidence artifact");
    serde_json::from_str(&raw).expect("failed to parse artifact")
}

// -- Doc existence and structure --

#[test]
fn doc_exists() {
    assert!(Path::new(DOC_PATH).exists(), "doc must exist");
}

#[test]
fn doc_references_bead() {
    let doc = load_doc();
    assert!(
        doc.contains("asupersync-3qv04.8.1"),
        "doc must reference bead id"
    );
}

#[test]
fn doc_has_required_sections() {
    let doc = load_doc();
    let sections = [
        "Purpose",
        "Contract Artifacts",
        "Evidence Layers",
        "Structured Logging Contract",
        "Comparator-Smoke Runner",
        "Validation",
        "Cross-References",
    ];
    let mut missing = Vec::new();
    for section in sections {
        if !doc.contains(section) {
            missing.push(section);
        }
    }
    assert!(
        missing.is_empty(),
        "doc missing sections:\n{}",
        missing
            .iter()
            .map(|s| format!("  - {s}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn doc_references_artifact_runner_and_test() {
    let doc = load_doc();
    for reference in [
        "artifacts/wasm_qa_evidence_matrix_v1.json",
        "scripts/run_wasm_qa_evidence_smoke.sh",
        "tests/wasm_qa_evidence_matrix_contract.rs",
        "src/types/wasm_abi.rs",
    ] {
        assert!(doc.contains(reference), "doc must reference {reference}");
    }
}

#[test]
fn doc_reproduction_command_uses_rch() {
    let doc = load_doc();
    assert!(
        doc.contains(
            "rch exec -- env CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/rch-codex-wasm-qa cargo test --test wasm_qa_evidence_matrix_contract -- --nocapture"
        ),
        "doc must route heavy validation through rch"
    );
}

// -- Artifact schema and version stability --

#[test]
fn artifact_versions_are_stable() {
    let artifact = load_artifact();
    assert_eq!(
        artifact["contract_version"].as_str(),
        Some("wasm-qa-evidence-matrix-v1")
    );
    assert_eq!(
        artifact["runner_bundle_schema_version"].as_str(),
        Some("wasm-qa-evidence-smoke-bundle-v1")
    );
    assert_eq!(
        artifact["runner_report_schema_version"].as_str(),
        Some("wasm-qa-evidence-smoke-run-report-v1")
    );
    assert_eq!(
        artifact["runner_script"].as_str(),
        Some("scripts/run_wasm_qa_evidence_smoke.sh")
    );
}

// -- Evidence layer catalog --

#[test]
fn layer_ids_are_complete() {
    let artifact = load_artifact();
    let actual: BTreeSet<String> = artifact["evidence_layers"]
        .as_array()
        .expect("evidence_layers must be array")
        .iter()
        .map(|l| l["layer_id"].as_str().unwrap().to_string())
        .collect();
    let expected: BTreeSet<String> = ["L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8"]
        .into_iter()
        .map(ToOwned::to_owned)
        .collect();
    assert_eq!(actual, expected, "evidence layers must be L1-L8");
}

#[test]
fn each_layer_has_name_and_evidence_items() {
    let artifact = load_artifact();
    for layer in artifact["evidence_layers"].as_array().unwrap() {
        let lid = layer["layer_id"].as_str().unwrap();
        assert!(layer.get("name").is_some(), "layer {lid} missing name");
        let items = layer["evidence_items"]
            .as_array()
            .expect("evidence_items must be array");
        assert!(
            !items.is_empty(),
            "layer {lid} must have at least one evidence item"
        );
    }
}

#[test]
fn evidence_ids_are_globally_unique() {
    let artifact = load_artifact();
    let mut all_ids = BTreeSet::new();
    for layer in artifact["evidence_layers"].as_array().unwrap() {
        for item in layer["evidence_items"].as_array().unwrap() {
            let eid = item["evidence_id"].as_str().unwrap().to_string();
            assert!(all_ids.insert(eid.clone()), "duplicate evidence id: {eid}");
        }
    }
    // Should have at least 25 evidence items across all layers
    assert!(
        all_ids.len() >= 25,
        "must have at least 25 evidence items, got {}",
        all_ids.len()
    );
}

#[test]
fn evidence_ids_are_prefixed_by_layer() {
    let artifact = load_artifact();
    for layer in artifact["evidence_layers"].as_array().unwrap() {
        let lid = layer["layer_id"].as_str().unwrap();
        for item in layer["evidence_items"].as_array().unwrap() {
            let eid = item["evidence_id"].as_str().unwrap();
            assert!(
                eid.starts_with(lid),
                "evidence {eid} must be prefixed by layer {lid}"
            );
        }
    }
}

#[test]
fn each_evidence_item_has_required_fields() {
    let artifact = load_artifact();
    for layer in artifact["evidence_layers"].as_array().unwrap() {
        let lid = layer["layer_id"].as_str().unwrap();
        for item in layer["evidence_items"].as_array().unwrap() {
            let eid = item["evidence_id"].as_str().unwrap_or("<missing>");
            for field in ["evidence_id", "description", "tool"] {
                assert!(
                    item.get(field).is_some(),
                    "evidence {eid} in {lid} missing field: {field}"
                );
            }
        }
    }
}

// -- WASM profiles --

#[test]
fn profile_catalog_has_expected_profiles() {
    let artifact = load_artifact();
    let actual: BTreeSet<String> = artifact["wasm_profiles"]
        .as_array()
        .expect("wasm_profiles must be array")
        .iter()
        .map(|p| p.as_str().unwrap().to_string())
        .collect();
    let expected: BTreeSet<String> = [
        "wasm-browser-minimal",
        "wasm-browser-dev",
        "wasm-browser-prod",
        "wasm-browser-deterministic",
    ]
    .into_iter()
    .map(ToOwned::to_owned)
    .collect();
    assert_eq!(actual, expected, "wasm profiles must remain stable");
}

// -- Cfg-gated files --

#[test]
fn cfg_gated_files_exist() {
    let artifact = load_artifact();
    let root = repo_root();
    for file in artifact["cfg_gated_files"].as_array().unwrap() {
        let path = file.as_str().unwrap();
        assert!(
            root.join(path).exists(),
            "cfg-gated file must exist: {path}"
        );
    }
}

// -- Structured log fields --

#[test]
fn structured_log_fields_are_unique_and_nonempty() {
    let artifact = load_artifact();
    let fields = artifact["structured_log_fields_required"]
        .as_array()
        .expect("structured_log_fields_required must be array");
    assert!(!fields.is_empty());
    let mut set = BTreeSet::new();
    for field in fields {
        let f = field.as_str().expect("field must be string").to_string();
        assert!(!f.is_empty());
        assert!(set.insert(f.clone()), "duplicate field: {f}");
    }
}

// -- Smoke runner and scenarios --

#[test]
fn smoke_scenarios_are_rch_routed() {
    let artifact = load_artifact();
    let scenarios = artifact["smoke_scenarios"].as_array().expect("array");
    assert!(!scenarios.is_empty());
    for scenario in scenarios {
        let sid = scenario["scenario_id"].as_str().unwrap();
        let cmd = scenario["command"].as_str().unwrap();
        assert!(cmd.contains("rch exec --"), "scenario {sid} must use rch");
    }
}

#[test]
fn runner_script_exists_and_declares_modes() {
    let root = repo_root();
    let script_path = root.join(RUNNER_SCRIPT_PATH);
    assert!(script_path.exists(), "runner script must exist");
    let script = std::fs::read_to_string(&script_path).unwrap();
    for token in [
        "--list",
        "--scenario",
        "--dry-run",
        "--execute",
        "wasm-qa-evidence-smoke-bundle-v1",
        "wasm-qa-evidence-smoke-run-report-v1",
    ] {
        assert!(script.contains(token), "runner missing token: {token}");
    }
}

// -- Downstream beads --

#[test]
fn downstream_beads_are_in_wasm_namespace() {
    let artifact = load_artifact();
    for bead in artifact["downstream_beads"].as_array().unwrap() {
        let bead = bead.as_str().unwrap();
        assert!(
            bead.starts_with("asupersync-3qv04.") || bead.starts_with("asupersync-1508v."),
            "downstream bead must be in project namespace: {bead}"
        );
    }
}
