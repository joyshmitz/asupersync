#![allow(missing_docs)]

use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

fn load_json(path: &Path) -> serde_json::Value {
    let raw = fs::read_to_string(path).expect("failed to read JSON file");
    serde_json::from_str(&raw).expect("failed to parse JSON")
}

fn sha256_hex(path: &Path) -> String {
    let bytes = fs::read(path).expect("failed to read artifact bytes");
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    format!("{digest:x}")
}

fn load_package_policy() -> serde_json::Value {
    load_json(Path::new(".github/wasm_typescript_package_policy.json"))
}

fn required_browser_packages() -> Vec<String> {
    let policy = load_package_policy();
    let packages = policy["required_packages"]
        .as_array()
        .expect("required_packages must be an array");
    packages
        .iter()
        .map(|entry| {
            entry
                .as_str()
                .expect("required package entry must be string")
                .to_string()
        })
        .collect()
}

fn package_manifest_path(package_name: &str) -> PathBuf {
    let package_dir = package_name
        .strip_prefix("@asupersync/")
        .expect("expected @asupersync/ package name");
    Path::new("packages").join(package_dir).join("package.json")
}

fn load_package_manifest(package_name: &str) -> serde_json::Value {
    load_json(&package_manifest_path(package_name))
}

fn discovered_package_manifest_paths() -> BTreeSet<PathBuf> {
    let mut discovered = BTreeSet::new();
    for entry in fs::read_dir("packages").expect("packages directory must exist") {
        let entry = entry.expect("failed to read packages directory entry");
        let path = entry.path().join("package.json");
        if path.exists() {
            discovered.insert(path);
        }
    }
    discovered
}

fn internal_dependency_set(manifest: &serde_json::Value) -> BTreeSet<String> {
    manifest["dependencies"]
        .as_object()
        .map(|deps| {
            deps.keys()
                .filter(|name| name.starts_with("@asupersync/"))
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

#[test]
fn security_release_policy_declares_supply_chain_artifact_gate() {
    let policy = load_json(Path::new(".github/security_release_policy.json"));
    let blocking = policy["release_blocking_criteria"]
        .as_array()
        .expect("release_blocking_criteria must be an array");

    let gate = blocking
        .iter()
        .find(|entry| entry["id"] == "SEC-BLOCK-07")
        .expect("SEC-BLOCK-07 must be declared");

    assert_eq!(gate["category"], "supply_chain_artifact_integrity");
    assert_eq!(gate["blocks_release"], true);

    let required = gate["required_artifacts"]
        .as_array()
        .expect("required_artifacts must be an array");
    assert!(
        required
            .iter()
            .any(|entry| entry == "docs/wasm_browser_sbom_v1.json"),
        "required_artifacts must include SBOM artifact"
    );
    assert!(
        required
            .iter()
            .any(|entry| entry == "docs/wasm_browser_provenance_attestation_v1.json"),
        "required_artifacts must include provenance artifact"
    );
    assert_eq!(
        gate["integrity_manifest"],
        "docs/wasm_browser_artifact_integrity_manifest_v1.json"
    );
}

#[test]
fn artifact_integrity_manifest_matches_committed_artifacts() {
    let manifest_path = Path::new("docs/wasm_browser_artifact_integrity_manifest_v1.json");
    let manifest = load_json(manifest_path);

    assert_eq!(
        manifest["schema_version"],
        "asupersync-wasm-artifact-integrity-v1"
    );
    assert_eq!(manifest["bead"], "asupersync-umelq.14.3");
    assert_eq!(manifest["hash_algorithm"], "sha256");

    let entries = manifest["entries"]
        .as_array()
        .expect("manifest entries must be an array");
    assert!(
        entries.len() >= 2,
        "manifest should include at least two entries"
    );

    let mut seen: BTreeMap<PathBuf, String> = BTreeMap::new();
    for entry in entries {
        let path = PathBuf::from(
            entry["path"]
                .as_str()
                .expect("manifest entry path must be string"),
        );
        let sha256 = entry["sha256"]
            .as_str()
            .expect("manifest entry sha256 must be string")
            .to_string();
        assert_eq!(sha256.len(), 64, "manifest sha256 must be 64 hex chars");
        assert!(
            seen.insert(path.clone(), sha256.clone()).is_none(),
            "manifest should not contain duplicate artifact paths"
        );

        assert!(path.exists(), "manifest artifact path must exist: {path:?}");
        let actual = sha256_hex(&path);
        assert_eq!(
            actual,
            sha256,
            "integrity manifest digest drift for {}",
            path.display()
        );
    }

    assert!(
        seen.contains_key(&PathBuf::from("docs/wasm_browser_sbom_v1.json")),
        "manifest must include SBOM artifact"
    );
    assert!(
        seen.contains_key(&PathBuf::from(
            "docs/wasm_browser_provenance_attestation_v1.json"
        )),
        "manifest must include provenance artifact"
    );
}

#[test]
fn dependency_audit_docs_reference_supply_chain_bundle_and_repro_commands() {
    let policy_doc = fs::read_to_string("docs/wasm_dependency_audit_policy.md")
        .expect("failed to read wasm dependency audit policy doc");
    let audit_doc = fs::read_to_string("docs/wasm_dependency_audit.md")
        .expect("failed to read wasm dependency audit doc");

    for expected in [
        "docs/wasm_browser_sbom_v1.json",
        "docs/wasm_browser_provenance_attestation_v1.json",
        "docs/wasm_browser_artifact_integrity_manifest_v1.json",
        "python3 scripts/check_security_release_gate.py \\\n  --policy .github/security_release_policy.json \\\n  --check-deps \\\n  --dep-policy .github/wasm_dependency_policy.json",
    ] {
        assert!(
            policy_doc.contains(expected) || audit_doc.contains(expected),
            "supply-chain docs missing required token: {expected}"
        );
    }
}

#[test]
fn publish_workflow_declares_release_contract_traceability_controls() {
    let workflow = fs::read_to_string(".github/workflows/publish.yml")
        .expect("failed to read publish workflow");

    for expected in [
        "WASM_RELEASE_CONTRACT_ID: wasm-release-channel-strategy-v1",
        "WASM_RELEASE_BEAD_ID: asupersync-umelq.15.2",
        "security_policy = Path(\".github/security_release_policy.json\")",
        "\"release_blocking_criteria\": criteria",
        "Path(\"artifacts/wasm/release/release_traceability.json\").write_text",
        "Path(\"artifacts/wasm/release/rollback_safety_report.json\").write_text",
        "Path(\"artifacts/wasm/release/incident_response_packet.json\").write_text",
        "\"schema_version\": \"wasm-rollback-safety-v1\"",
        "\"schema_version\": \"wasm-release-incident-response-v1\"",
        "\"rollback_safety_checks\": [",
        "\"artifact_revocation_strategy\": {",
        "\"postmortem_requirements\": [",
        "bash ./scripts/run_all_e2e.sh --suite wasm-incident-forensics",
        "TEST_SEED=4242 bash ./scripts/test_wasm_incident_forensics_e2e.sh",
        "python3 ./scripts/check_incident_forensics_playbook.py",
        "artifacts/wasm/release/release_traceability.json",
        "artifacts/wasm/release/rollback_safety_report.json",
        "artifacts/wasm/release/incident_response_packet.json",
        "if: ${{ always() }}",
    ] {
        assert!(
            workflow.contains(expected),
            "publish workflow missing release traceability control token: {expected}"
        );
    }
}

#[test]
fn publish_workflow_and_strategy_doc_align_on_npm_artifact_contract() {
    let workflow = fs::read_to_string(".github/workflows/publish.yml")
        .expect("failed to read publish workflow");
    let strategy = fs::read_to_string("docs/wasm_release_channel_strategy.md")
        .expect("failed to read wasm release strategy");

    for expected in [
        "artifacts/npm/package_json_paths.txt",
        "artifacts/npm/npm_release_assumptions.json",
        "artifacts/npm/publish_outcome.json",
        "artifacts/npm/rollback_outcome.json",
        "packages/*/package.json",
    ] {
        assert!(
            workflow.contains(expected),
            "publish workflow missing npm artifact contract token: {expected}"
        );
        assert!(
            strategy.contains(expected),
            "strategy doc missing npm artifact contract token: {expected}"
        );
    }

    assert!(
        workflow.contains("rollback_reason is required when rollback_npm_to_version is set."),
        "publish workflow must enforce rollback reason requirement"
    );
    assert!(
        strategy.contains("Rollback mode requires both target version and operator reason"),
        "strategy doc must document rollback reason requirement"
    );
    assert!(
        strategy.contains("Missing package manifests are a hard release-blocking failure"),
        "strategy doc must enforce mandatory package discovery (no controlled skip)"
    );
}

#[test]
fn browser_package_manifests_exist_for_all_required_packages() {
    let required_packages = required_browser_packages();
    let expected_paths: BTreeSet<PathBuf> = required_packages
        .iter()
        .map(|package| package_manifest_path(package))
        .collect();
    let discovered_paths = discovered_package_manifest_paths();

    assert_eq!(
        discovered_paths, expected_paths,
        "discovered package manifests must exactly match policy-required package set"
    );

    for package in &required_packages {
        let manifest = load_package_manifest(package);
        assert_eq!(
            manifest["name"].as_str(),
            Some(package.as_str()),
            "manifest name mismatch for {package}"
        );
        assert_eq!(
            manifest["version"].as_str(),
            Some(env!("CARGO_PKG_VERSION")),
            "manifest version must track root crate version for {package}"
        );
        assert_eq!(
            manifest["sideEffects"].as_bool(),
            Some(false),
            "manifest must declare tree-shake-safe package intent for {package}"
        );
    }
}

#[test]
fn browser_package_manifests_encode_allowed_internal_layer_edges() {
    let browser_core = load_package_manifest("@asupersync/browser-core");
    let browser = load_package_manifest("@asupersync/browser");
    let react = load_package_manifest("@asupersync/react");
    let next = load_package_manifest("@asupersync/next");

    assert!(
        internal_dependency_set(&browser_core).is_empty(),
        "browser-core must not depend on higher-level @asupersync packages"
    );
    assert_eq!(
        internal_dependency_set(&browser),
        BTreeSet::from([String::from("@asupersync/browser-core")]),
        "browser package must depend only on browser-core inside the Browser Edition package graph"
    );
    assert_eq!(
        internal_dependency_set(&react),
        BTreeSet::from([String::from("@asupersync/browser")]),
        "react adapter must depend on browser package inside the Browser Edition package graph"
    );
    assert_eq!(
        internal_dependency_set(&next),
        BTreeSet::from([String::from("@asupersync/browser")]),
        "next adapter must depend on browser package inside the Browser Edition package graph"
    );
}
