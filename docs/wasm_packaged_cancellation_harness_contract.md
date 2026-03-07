# WASM Packaged Cancellation Harness Contract

Contract ID: `wasm-packaged-cancellation-harness-v1`  
Bead: `asupersync-3qv04.8.4.2`

## Purpose

Define the standalone Browser Edition E2E harness for the user-visible
invariants that make Asupersync distinct from generic wasm glue:

1. interrupted bootstrap can recover cleanly,
2. render/lifecycle restarts cancel and drain losers,
3. nested cancellation cascades reach quiescence before close,
4. shutdown cancellation still resolves obligations cleanly.

This harness is intentionally scoped to dedicated files so it can progress in
parallel with other packaged-harness lanes without contending on the shared
orchestrator surfaces.

## Contract Artifacts

- Contract artifact: `artifacts/wasm_packaged_cancellation_harness_v1.json`
- Runner script: `scripts/test_wasm_packaged_cancellation_e2e.sh`
- Contract tests: `tests/wasm_packaged_cancellation_harness_contract.rs`
- Shared schema: `artifacts/wasm_e2e_log_schema_v1.json`

## Scenario Flow

Scenario ID: `e2e-wasm-packaged-cancellation-quiescence`  
Suite Scenario ID: `E2E-SUITE-WASM-PACKAGED-CANCELLATION`

Required step sequence:

1. `cancelled_bootstrap_retry_recovery`
2. `render_restart_loser_drain`
3. `nested_cancel_cascade_quiescence`
4. `shutdown_obligation_cleanup`

All step commands MUST be `rch exec -- ...` routed.

## Structured Logging Contract

`log.jsonl` entries MUST conform to `wasm-e2e-log-schema-v1` required fields:

- `ts`
- `level`
- `scenario_id`
- `run_id`
- `event`
- `msg`

The harness also records:

- `abi_version` + `abi_fingerprint`
- `browser` and `build` metadata
- `evidence_ids`
- scenario/step-specific `extra` payload

## Artifact Bundle Layout

Run bundle root:

`target/e2e-results/wasm_packaged_cancellation/e2e-runs/{scenario_id}/{run_id}/`

Required files:

- `run-metadata.json`
- `log.jsonl`
- `summary.json`
- `steps.ndjson`

`run-metadata.json` MUST use schema version `wasm-e2e-run-metadata-v1` and
include package version and wasm artifact identifier extensions:

- `package_versions`
- `wasm_artifact_identifiers`

## Usage

Execute:

```bash
bash ./scripts/test_wasm_packaged_cancellation_e2e.sh
```

Dry-run:

```bash
WASM_PACKAGED_CANCELLATION_DRY_RUN=1 bash ./scripts/test_wasm_packaged_cancellation_e2e.sh
```

## Validation

Contract tests:

```bash
rch exec -- env CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/rch-codex-wasm-packaged-cancellation cargo test --test wasm_packaged_cancellation_harness_contract -- --nocapture
```

Targeted runner contract:

```bash
WASM_PACKAGED_CANCELLATION_DRY_RUN=1 bash ./scripts/test_wasm_packaged_cancellation_e2e.sh
```

## Cross-References

- `docs/wasm_e2e_log_schema.md`
- `artifacts/wasm_qa_evidence_matrix_v1.json`
- `tests/nextjs_bootstrap_harness.rs`
- `tests/react_wasm_strictmode_harness.rs`
- `tests/close_quiescence_regression.rs`
- `tests/cancel_obligation_invariants.rs`
