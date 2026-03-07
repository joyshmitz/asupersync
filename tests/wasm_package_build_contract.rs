//! Contract tests for the JS/TS package build tooling (asupersync-3qv04.4.3).
//!
//! These tests validate that the workspace configuration, package topology,
//! and build infrastructure are correctly wired without requiring wasm-pack
//! or pnpm to be installed.

use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

// ── Workspace Configuration ──────────────────────────────────────────

#[test]
fn pnpm_workspace_yaml_exists_and_lists_packages() {
    let path = repo_root().join("pnpm-workspace.yaml");
    let content = std::fs::read_to_string(&path)
        .unwrap_or_else(|_| panic!("missing {}", path.display()));
    assert!(
        content.contains("packages:"),
        "pnpm-workspace.yaml must declare packages"
    );
    assert!(
        content.contains("packages/*"),
        "pnpm-workspace.yaml must glob packages/*"
    );
}

#[test]
fn root_package_json_exists_with_workspace_scripts() {
    let path = repo_root().join("package.json");
    let content = std::fs::read_to_string(&path)
        .unwrap_or_else(|_| panic!("missing {}", path.display()));
    let v: serde_json::Value = serde_json::from_str(&content).expect("invalid JSON");

    assert_eq!(v["private"], true, "root package.json must be private");

    let scripts = v["scripts"].as_object().expect("scripts must be an object");
    for required in &["build", "build:wasm", "build:packages", "clean", "typecheck", "validate"] {
        assert!(
            scripts.contains_key(*required),
            "root package.json missing script: {required}"
        );
    }
}

#[test]
fn npmrc_exists() {
    let path = repo_root().join(".npmrc");
    assert!(path.exists(), ".npmrc must exist");
    let content = std::fs::read_to_string(&path).unwrap();
    assert!(content.contains("enable-pre-post-scripts=true"));
}

#[test]
fn tsconfig_base_exists() {
    let path = repo_root().join("tsconfig.base.json");
    let content = std::fs::read_to_string(&path)
        .unwrap_or_else(|_| panic!("missing {}", path.display()));
    let v: serde_json::Value = serde_json::from_str(&content).expect("invalid JSON");
    assert!(
        v["compilerOptions"]["strict"] == true,
        "tsconfig.base.json must enable strict mode"
    );
}

// ── Package Topology ─────────────────────────────────────────────────

const PACKAGES: &[&str] = &["browser-core", "browser", "react", "next"];

#[test]
fn all_four_packages_have_package_json() {
    for pkg in PACKAGES {
        let path = repo_root().join("packages").join(pkg).join("package.json");
        assert!(path.exists(), "missing package.json for {pkg}");
    }
}

#[test]
fn all_four_packages_have_correct_name() {
    for pkg in PACKAGES {
        let path = repo_root().join("packages").join(pkg).join("package.json");
        let content = std::fs::read_to_string(&path).unwrap();
        let v: serde_json::Value = serde_json::from_str(&content).unwrap();
        let expected = format!("@asupersync/{pkg}");
        assert_eq!(
            v["name"].as_str().unwrap(),
            expected,
            "package name mismatch for {pkg}"
        );
    }
}

#[test]
fn higher_level_packages_are_esm_modules() {
    for pkg in &["browser", "react", "next"] {
        let path = repo_root().join("packages").join(pkg).join("package.json");
        let content = std::fs::read_to_string(&path).unwrap();
        let v: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(
            v["type"].as_str().unwrap(),
            "module",
            "{pkg} must be ESM"
        );
        assert!(
            v["main"].as_str().is_some(),
            "{pkg} must have main field"
        );
        assert!(
            v["types"].as_str().is_some(),
            "{pkg} must have types field"
        );
        assert!(
            v["exports"].is_object(),
            "{pkg} must have exports map"
        );
    }
}

#[test]
fn higher_level_packages_have_build_scripts() {
    for pkg in &["browser", "react", "next"] {
        let path = repo_root().join("packages").join(pkg).join("package.json");
        let content = std::fs::read_to_string(&path).unwrap();
        let v: serde_json::Value = serde_json::from_str(&content).unwrap();
        let scripts = v["scripts"].as_object().expect("scripts required");
        assert!(scripts.contains_key("build"), "{pkg} missing build script");
        assert!(
            scripts.contains_key("typecheck"),
            "{pkg} missing typecheck script"
        );
        assert!(scripts.contains_key("clean"), "{pkg} missing clean script");
    }
}

#[test]
fn higher_level_packages_have_typescript_source() {
    for pkg in &["browser", "react", "next"] {
        let path = repo_root()
            .join("packages")
            .join(pkg)
            .join("src")
            .join("index.ts");
        assert!(
            path.exists(),
            "missing src/index.ts for {pkg} ({})",
            path.display()
        );
    }
}

#[test]
fn higher_level_packages_have_tsconfig() {
    for pkg in &["browser", "react", "next"] {
        let path = repo_root()
            .join("packages")
            .join(pkg)
            .join("tsconfig.json");
        assert!(path.exists(), "missing tsconfig.json for {pkg}");
        let content = std::fs::read_to_string(&path).unwrap();
        let v: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert!(
            v["extends"].as_str().is_some() || v["compilerOptions"].is_object(),
            "{pkg} tsconfig must extend base or define compilerOptions"
        );
    }
}

// ── Dependency Graph ─────────────────────────────────────────────────

#[test]
fn browser_depends_on_browser_core() {
    let v = read_pkg_json("browser");
    let dep = v["dependencies"]["@asupersync/browser-core"]
        .as_str()
        .expect("browser must depend on browser-core");
    assert!(
        dep.contains("workspace:") || dep.starts_with("0."),
        "browser-core dep should be workspace protocol or version"
    );
}

#[test]
fn react_depends_on_browser() {
    let v = read_pkg_json("react");
    let dep = v["dependencies"]["@asupersync/browser"]
        .as_str()
        .expect("react must depend on browser");
    assert!(
        dep.contains("workspace:") || dep.starts_with("0."),
        "browser dep should be workspace protocol or version"
    );
}

#[test]
fn next_depends_on_browser() {
    let v = read_pkg_json("next");
    let dep = v["dependencies"]["@asupersync/browser"]
        .as_str()
        .expect("next must depend on browser");
    assert!(
        dep.contains("workspace:") || dep.starts_with("0."),
        "browser dep should be workspace protocol or version"
    );
}

#[test]
fn no_circular_dependencies() {
    // browser-core must not depend on any other @asupersync package
    let v = read_pkg_json("browser-core");
    let deps = v["dependencies"].as_object();
    if let Some(deps) = deps {
        for key in deps.keys() {
            assert!(
                !key.starts_with("@asupersync/") || key == "@asupersync/browser-core",
                "browser-core must not depend on {key}"
            );
        }
    }
}

// ── Build Script Infrastructure ──────────────────────────────────────

#[test]
fn build_browser_core_artifacts_script_exists() {
    let path = repo_root().join("scripts/build_browser_core_artifacts.sh");
    assert!(path.exists(), "build_browser_core_artifacts.sh must exist");
    let content = std::fs::read_to_string(&path).unwrap();
    assert!(content.contains("wasm-pack build"), "must invoke wasm-pack");
    assert!(
        content.contains("abi-metadata.json"),
        "must generate abi-metadata.json"
    );
}

#[test]
fn clean_script_exists() {
    let path = repo_root().join("scripts/clean_packages.sh");
    assert!(path.exists(), "clean_packages.sh must exist");
    let content = std::fs::read_to_string(&path).unwrap();
    assert!(content.contains("pkg/browser-core"), "must clean staging dir");
}

#[test]
fn validate_script_exists() {
    let path = repo_root().join("scripts/validate_package_build.sh");
    assert!(path.exists(), "validate_package_build.sh must exist");
    let content = std::fs::read_to_string(&path).unwrap();
    assert!(
        content.contains("VALIDATION PASSED"),
        "must report validation result"
    );
}

// ── browser-core Package Structure ───────────────────────────────────

#[test]
fn browser_core_package_json_lists_wasm_artifacts_in_files() {
    let v = read_pkg_json("browser-core");
    let files: Vec<&str> = v["files"]
        .as_array()
        .expect("files array required")
        .iter()
        .filter_map(|f| f.as_str())
        .collect();
    assert!(files.contains(&"asupersync.js"), "must include JS entry");
    assert!(files.contains(&"asupersync.d.ts"), "must include TS decl");
    assert!(
        files.contains(&"asupersync_bg.wasm"),
        "must include WASM binary"
    );
    assert!(
        files.contains(&"abi-metadata.json"),
        "must include ABI metadata"
    );
}

#[test]
fn browser_core_exports_map_includes_wasm_and_metadata() {
    let v = read_pkg_json("browser-core");
    let exports = v["exports"].as_object().expect("exports map required");
    assert!(exports.contains_key("."), "must export root entry");
    assert!(
        exports.contains_key("./asupersync_bg.wasm"),
        "must export WASM"
    );
    assert!(
        exports.contains_key("./abi-metadata.json"),
        "must export ABI metadata"
    );
}

// ── Version Consistency ──────────────────────────────────────────────

#[test]
fn all_packages_share_same_version() {
    let versions: Vec<(String, String)> = PACKAGES
        .iter()
        .map(|pkg| {
            let v = read_pkg_json(pkg);
            (
                pkg.to_string(),
                v["version"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    let first = &versions[0].1;
    for (pkg, ver) in &versions {
        assert_eq!(
            ver, first,
            "version mismatch: {pkg} has {ver}, expected {first}"
        );
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

fn read_pkg_json(pkg: &str) -> serde_json::Value {
    let path = repo_root().join("packages").join(pkg).join("package.json");
    let content = std::fs::read_to_string(&path)
        .unwrap_or_else(|_| panic!("cannot read {}", path.display()));
    serde_json::from_str(&content).expect("invalid JSON")
}
