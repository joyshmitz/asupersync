# WASM Release Channel Strategy

Contract ID: `wasm-release-channel-strategy-v1`  
Bead: `asupersync-umelq.15.3`  
Depends on: `asupersync-umelq.15.1`

## Purpose

Define a deterministic promotion and demotion workflow for Browser Edition
artifacts across three channels:

1. `nightly` (fast feedback, highest churn),
2. `canary` (limited-risk pre-stable validation),
3. `stable` (production channel with strict release gates).

This policy is intentionally gate-driven: no channel promotion is valid unless
all required checks and artifact contracts pass.

## Channel Contract

| Channel | Optimization profile | Intended use | Promotion source |
|---|---|---|---|
| `nightly` | `wasm-browser-dev` | Daily integration and rapid iteration | n/a |
| `canary` | `wasm-browser-canary` | Limited rollout and early regression detection | `nightly` |
| `stable` | `wasm-browser-release` | Production release lane | `canary` |

Profile definitions are sourced from:

- `.github/wasm_optimization_policy.json`
- `scripts/check_wasm_optimization_policy.py`

## Required Promotion Gates

Promotion requires all gates below to pass in the same decision window.

### Gate 1: Profile and optimization policy validity

```bash
python3 scripts/check_wasm_optimization_policy.py \
  --policy .github/wasm_optimization_policy.json
```

Required artifact:

- `artifacts/wasm_optimization_pipeline_summary.json`

### Gate 2: Dependency provenance and forbidden-crate audit

```bash
python3 scripts/check_wasm_dependency_policy.py \
  --policy .github/wasm_dependency_policy.json
```

Required artifacts:

- `artifacts/wasm_dependency_audit_summary.json`
- `artifacts/wasm_dependency_audit_log.ndjson`

### Gate 3: Security release gate (with dependency checks enabled)

```bash
python3 scripts/check_security_release_gate.py \
  --policy .github/security_release_policy.json \
  --check-deps \
  --dep-policy .github/wasm_dependency_policy.json
```

Required artifacts:

- `artifacts/security_release_gate_report.json`
- `artifacts/security_release_gate_events.ndjson`

### Gate 4: Browser profile build checks (cargo-heavy, offloaded)

```bash
rch exec -- cargo check --target wasm32-unknown-unknown \
  --no-default-features --features wasm-browser-dev

rch exec -- cargo check --target wasm32-unknown-unknown \
  --no-default-features --features wasm-browser-prod

rch exec -- cargo check --target wasm32-unknown-unknown \
  --no-default-features --features wasm-browser-deterministic
```

### Gate 5: Deterministic scenario evidence and replayability

Run deterministic onboarding and scenario validation before promotion:

```bash
python3 scripts/run_browser_onboarding_checks.py --scenario all
```

At minimum, promotion evidence must include:

1. command bundle used for each gate,
2. produced artifacts and paths,
3. replay command pointers for any non-pass diagnostics.

### Gate 6: Packaged release artifact and consumer-build evidence

Pilot and GA promotion for `asupersync-3qv04.7.3` is not satisfied by policy
documents alone. Release confidence must be derived from real packages, real
consumer builds, and real behavioral evidence from the current candidate.

Required package-release artifacts:

- `artifacts/npm/package_release_validation.json`
- `artifacts/npm/package_pack_dry_run_summary.json`
- `artifacts/npm/publish_outcome.json`

The Gate 6 package-validation command bundle must also prove that the candidate
ran the full Browser Edition package gate, not a symbolic placeholder. The
accepted evidence is either `corepack pnpm run validate` from the root workspace
or direct command provenance showing both `bash scripts/validate_package_build.sh`
and `bash scripts/validate_npm_pack_smoke.sh` for the same candidate window.
A lone generic `validate` label without both underlying validators is
insufficient for promotion.

Required consumer-build and smoke artifacts:

- `artifacts/onboarding/vanilla.summary.json`
- `artifacts/onboarding/react.summary.json`
- `artifacts/onboarding/next.summary.json`
- `target/wasm-qa-evidence-smoke/<run>/<scenario>/bundle_manifest.json`
- `target/e2e-results/wasm_qa_evidence_smoke/run_<timestamp>/summary.json`

Missing any Gate 6 artifact is a release-blocking failure even if the policy,
security, and dependency gates are otherwise green.

## Promotion Rules

`nightly -> canary` promotion requires:

1. all required gates pass,
2. no critical/high unresolved findings in security release gate report,
3. dependency policy transitions are active and non-expired,
4. package validation and pack dry-run artifacts exist for the candidate, with
   command provenance tying the candidate to both
   `bash scripts/validate_package_build.sh` and
   `bash scripts/validate_npm_pack_smoke.sh`,
5. onboarding and QA smoke artifacts exist for the same decision window.

`canary -> stable` promotion requires:

1. all required gates pass on canary artifact set,
2. no blocked release criteria in security report (`gate_status != fail`),
3. artifact provenance present and reproducible for optimization, dependency,
   and security summaries,
4. no unresolved deterministic replay failures from the selected promotion run,
5. package validation, pack dry-run, and publish outcome artifacts are
   reproducible for the current candidate, and the package-validation provenance
   must still show both `bash scripts/validate_package_build.sh` and
   `bash scripts/validate_npm_pack_smoke.sh`,
6. consumer-build onboarding summaries and QA smoke bundle/summary artifacts
   are reproducible for the current candidate.

## Demotion and Rollback Policy

Demotion is mandatory when one of these triggers occurs post-publish:

1. any release-blocking security criterion fails,
2. dependency policy check fails due to forbidden crate hit or expired
   conditional transition,
3. deterministic replay checks for mandatory scenarios fail in two consecutive
   release-gate runs,
4. required promotion artifacts are missing or non-reproducible,
5. package validation or pack dry-run artifacts are missing, stale, do not
   match the candidate being promoted, or do not prove both
   `bash scripts/validate_package_build.sh` and
   `bash scripts/validate_npm_pack_smoke.sh`,
6. consumer-build onboarding or QA smoke artifacts are missing, stale, or not
   reproducible for the candidate being promoted.

Demotion actions:

1. `stable -> canary` immediately on trigger detection.
2. If canary also violates a blocking trigger, `canary -> nightly`.
3. Record demotion event with trigger id, artifact pointers, and replay command.
4. Open/attach a remediation bead before any re-promotion attempt.

## Operator Runbook (Deterministic Order)

1. Run optimization policy check.
2. Run dependency policy check.
3. Run security gate with `--check-deps`.
4. Run `rch` offloaded wasm profile checks.
5. Run deterministic onboarding/scenario validation.
6. Confirm package validation, pack dry-run, and publish outcome artifacts for
   the candidate are present and non-empty, and that the command bundle still
   captures `corepack pnpm run validate` or both
   `bash scripts/validate_package_build.sh` and
   `bash scripts/validate_npm_pack_smoke.sh`.
7. Confirm onboarding summaries plus QA smoke bundle/summary artifacts for the
   same candidate are present and non-empty.
8. Publish promotion decision with command + artifact pointers.

## Traceability and Audit Fields

Promotion or demotion decisions must capture:

1. channel transition (`from`, `to`),
2. decision timestamp (UTC),
3. command bundle (exact commands),
4. artifact paths and hashes where available,
5. blocking criterion IDs (if demoted),
6. owning bead ID for remediation.

## Workflow Contract + Package Assumptions

The canonical automation entrypoint for this contract is:

- `.github/workflows/publish.yml`
- `docs/wasm_release_rollback_incident_playbook.md` (operational rollback + incident procedure)

Release automation for this workflow is tied to:

- contract id: `wasm-release-channel-strategy-v1`
- active bead scope: `asupersync-umelq.15.2`

Policy wiring expectations:

1. WASM release gates produce a traceability artifact linking this contract
   to security release-block criteria in `.github/security_release_policy.json`
   (for example `SEC-BLOCK-01`, `SEC-BLOCK-06`, and `SEC-BLOCK-07`).
2. Required gate report artifacts are retained alongside release artifacts:
   - `artifacts/wasm_optimization_pipeline_summary.json`
   - `artifacts/wasm_dependency_audit_summary.json`
   - `artifacts/security_release_gate_report.json`
3. npm release assumptions are explicit and artifactized:
   - package discovery glob: `packages/*/package.json`
   - discovered package path list: `artifacts/npm/package_json_paths.txt`
   - assumptions artifact: `artifacts/npm/npm_release_assumptions.json`
   - package validation artifact: `artifacts/npm/package_release_validation.json`
   - pack dry-run evidence artifact: `artifacts/npm/package_pack_dry_run_summary.json`
   - publish outcome artifact: `artifacts/npm/publish_outcome.json`
   - rollback outcome artifact (when rollback mode is used): `artifacts/npm/rollback_outcome.json`
4. Missing package manifests are a hard release-blocking failure. Missing package manifests or missing built package outputs are hard release-blocking failures. The exact required package set from
   `.github/wasm_typescript_package_policy.json` must be discovered, built via
   `corepack pnpm run build`, validated with
   `bash scripts/validate_package_build.sh`, and pack-smoke checked with
   `bash scripts/validate_npm_pack_smoke.sh`, and evidenced with
   `npm pack --json --dry-run` before any npm publish can proceed.
5. Consumer-build and behavioral evidence are artifactized alongside the
   package-release bundle:
   - `artifacts/onboarding/vanilla.summary.json`
   - `artifacts/onboarding/react.summary.json`
   - `artifacts/onboarding/next.summary.json`
   - `target/wasm-qa-evidence-smoke/<run>/<scenario>/bundle_manifest.json`
   - `target/e2e-results/wasm_qa_evidence_smoke/run_<timestamp>/summary.json`
6. Rollback mode requires both target version and operator reason; the executed
   dist-tag commands must be captured in release artifacts.

Incident response and rollback operational requirements are defined in:

- `docs/wasm_release_rollback_incident_playbook.md`

That playbook is enforced by CI certification artifacts:

- `artifacts/wasm_release_rollback_playbook_summary.json`
- `artifacts/wasm_release_rollback_playbook_test.log`

## Non-Negotiable Constraints

1. No channel promotion can bypass dependency or security gates.
2. No channel promotion can proceed with unresolved critical findings.
3. Cargo-heavy validation in this workflow must be executed through `rch`.
4. This policy does not weaken runtime invariants: structured concurrency,
   cancellation protocol, loser-drain behavior, obligation closure, and explicit
   capability boundaries remain mandatory.
