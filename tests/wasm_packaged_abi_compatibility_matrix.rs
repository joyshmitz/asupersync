//! Packaged WASM ABI compatibility matrix contract (`asupersync-3qv04.6.5`).
//!
//! Validates the package-level ABI compatibility surfaces that consumers rely
//! on outside the Rust crate graph: published metadata sidecars, manifest
//! exports, package-layer cross references, and the documented packaged
//! upgrade/downgrade matrix.

use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read(path: &str) -> String {
    let full = repo_root().join(path);
    assert!(full.exists(), "missing {}", full.display());
    std::fs::read_to_string(&full).expect("required fixture file should be readable")
}

fn read_json(path: &str) -> serde_json::Value {
    serde_json::from_str(&read(path)).expect("invalid JSON")
}

#[test]
fn packaged_policy_document_exists() {
    assert!(
        repo_root()
            .join(Path::new("docs/wasm_abi_compatibility_policy.md"))
            .exists(),
        "packaged ABI policy document must exist"
    );
}

#[test]
fn packaged_policy_references_extension_bead() {
    let doc = read("docs/wasm_abi_compatibility_policy.md");
    assert!(
        doc.contains("asupersync-3qv04.6.5"),
        "policy must reference packaged ABI compatibility bead"
    );
}

#[test]
fn packaged_policy_covers_observability_surfaces() {
    let doc = read("docs/wasm_abi_compatibility_policy.md");
    for marker in [
        "Packaged Observability Surfaces",
        "`packages/browser-core/abi-metadata.json`",
        "`./abi-metadata.json`",
        "`abi_version()`",
        "`abi_fingerprint()`",
        "`scripts/validate_package_build.sh`",
    ] {
        assert!(
            doc.contains(marker),
            "policy missing packaged observability marker: {marker}"
        );
    }
}

#[test]
fn packaged_policy_covers_upgrade_and_downgrade_decisions() {
    let doc = read("docs/wasm_abi_compatibility_policy.md");
    for marker in [
        "Packaged Browser-Core Upgrade / Downgrade Matrix",
        "`Exact`",
        "`BackwardCompatible`",
        "`ConsumerTooOld`",
        "`MajorMismatch`",
        "`compatibility_rejected`",
        "omitted consumer version",
    ] {
        assert!(
            doc.contains(marker),
            "policy missing packaged matrix marker: {marker}"
        );
    }
}

#[test]
fn packaged_policy_reproduction_includes_contract_test() {
    let doc = read("docs/wasm_abi_compatibility_policy.md");
    assert!(
        doc.contains("cargo test --test wasm_packaged_abi_compatibility_matrix -- --nocapture"),
        "policy must include packaged ABI matrix reproduction command"
    );
}

#[test]
fn browser_core_manifest_publishes_abi_metadata_sidecar() {
    let manifest = read_json("packages/browser-core/package.json");
    let exports = manifest["exports"]
        .as_object()
        .expect("exports map required");
    assert!(
        exports.contains_key("./abi-metadata.json"),
        "browser-core package must export abi-metadata sidecar"
    );

    let has_abi_metadata = manifest["files"]
        .as_array()
        .expect("files array required")
        .iter()
        .filter_map(serde_json::Value::as_str)
        .any(|x| x == "abi-metadata.json");
    assert!(
        has_abi_metadata,
        "browser-core package files must publish abi-metadata.json"
    );
}

#[test]
fn build_artifact_script_emits_and_syncs_abi_metadata() {
    let script = read("scripts/build_browser_core_artifacts.sh");
    for marker in [
        "abi-metadata.json",
        "\"abi_version\": {",
        "\"abi_signature_fingerprint_v1\":",
        "major=\"$(rg -No 'WASM_ABI_MAJOR_VERSION",
        "minor=\"$(rg -No 'WASM_ABI_MINOR_VERSION",
        "fingerprint=\"$(rg -No 'WASM_ABI_SIGNATURE_FINGERPRINT_V1[^=]*= ([0-9_]+);' \"${ABI_FILE}\" -r '$1' -m1 | tr -d '_')\"",
        "cp \"${STAGING_DIR}/${artifact}\" \"${PACKAGE_DIR}/${artifact}\"",
    ] {
        assert!(
            script.contains(marker),
            "artifact build script missing ABI metadata marker: {marker}"
        );
    }
}

#[test]
fn package_validation_script_checks_abi_metadata_keys() {
    let script = read("scripts/validate_package_build.sh");
    for marker in [
        "check_json_key",
        "'abi_version'",
        "'abi_signature_fingerprint_v1'",
        "ABI version key",
        "ABI fingerprint key",
    ] {
        assert!(
            script.contains(marker),
            "package validation script missing ABI metadata check marker: {marker}"
        );
    }
}

#[test]
fn raw_browser_core_export_tests_cover_version_and_fingerprint() {
    let tests = read("asupersync-browser-core/tests/abi_exports.rs");
    for marker in [
        "abi_version().expect(\"abi_version succeeds\")",
        "assert_eq!(version.major, WASM_ABI_MAJOR_VERSION);",
        "assert_eq!(version.minor, WASM_ABI_MINOR_VERSION);",
        "assert_eq!(abi_fingerprint(), WASM_ABI_SIGNATURE_FINGERPRINT_V1);",
    ] {
        assert!(
            tests.contains(marker),
            "browser-core export tests missing ABI marker: {marker}"
        );
    }
}

#[test]
fn browser_package_keeps_browser_core_as_abi_source_of_truth() {
    let manifest = read_json("packages/browser/package.json");
    let dependency = manifest["dependencies"]["@asupersync/browser-core"]
        .as_str()
        .expect("browser package must depend on browser-core");
    assert!(
        dependency.contains("workspace:") || dependency.starts_with("0."),
        "browser package must consume browser-core through workspace or semver dependency"
    );

    let source = read("packages/browser/src/index.ts");
    assert!(
        source.contains("@asupersync/browser-core"),
        "browser entrypoint must source its ABI-facing surface from browser-core"
    );
}
