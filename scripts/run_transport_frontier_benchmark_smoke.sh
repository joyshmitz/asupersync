#!/usr/bin/env bash
# Transport frontier benchmark smoke runner (AA-08.1)
#
# Usage:
#   bash ./scripts/run_transport_frontier_benchmark_smoke.sh --list
#   bash ./scripts/run_transport_frontier_benchmark_smoke.sh --scenario AA08-SMOKE-WORKLOAD-VOCAB --dry-run
#   bash ./scripts/run_transport_frontier_benchmark_smoke.sh --scenario AA08-SMOKE-WORKLOAD-VOCAB --execute
#
# Bundle schema: transport-frontier-benchmark-smoke-bundle-v1
# Report schema: transport-frontier-benchmark-smoke-run-report-v1

set -euo pipefail

RUNNER_SCRIPT="scripts/run_transport_frontier_benchmark_smoke.sh"
ARTIFACT="${AA08_ARTIFACT:-artifacts/transport_frontier_benchmark_v1.json}"
OUTPUT_ROOT="${AA08_OUTPUT_ROOT:-target/transport-frontier-benchmark-smoke}"
MODE=""
SCENARIO=""

usage() {
  echo "Usage: $0 --list | --scenario <ID> (--dry-run | --execute)"
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --list)   MODE="list"; shift ;;
    --scenario) SCENARIO="$2"; shift 2 ;;
    --dry-run)  MODE="dry-run"; shift ;;
    --execute)  MODE="execute"; shift ;;
    *) usage ;;
  esac
done

[[ -z "$MODE" ]] && usage

if [[ "$MODE" == "list" ]]; then
  echo "=== Transport Frontier Benchmark Smoke Scenarios ==="
  jq -r '.smoke_scenarios[] | "  \(.scenario_id) [\(.workload_id)] -> \(.validation_surface): \(.description)"' "$ARTIFACT"
  exit 0
fi

[[ -z "$SCENARIO" ]] && { echo "error: --scenario required with --dry-run/--execute"; exit 1; }

COMMAND=$(jq -r --arg sid "$SCENARIO" '.smoke_scenarios[] | select(.scenario_id == $sid) | .command' "$ARTIFACT")
DESCRIPTION=$(jq -r --arg sid "$SCENARIO" '.smoke_scenarios[] | select(.scenario_id == $sid) | .description' "$ARTIFACT")
WORKLOAD_ID=$(jq -r --arg sid "$SCENARIO" '.smoke_scenarios[] | select(.scenario_id == $sid) | .workload_id' "$ARTIFACT")
VALIDATION_SURFACE=$(jq -r --arg sid "$SCENARIO" '.smoke_scenarios[] | select(.scenario_id == $sid) | .validation_surface' "$ARTIFACT")
FOCUS_DIMENSIONS=$(jq -c --arg sid "$SCENARIO" '.smoke_scenarios[] | select(.scenario_id == $sid) | (.focus_dimension_ids // [])' "$ARTIFACT")

if [[ -z "$COMMAND" || "$COMMAND" == "null" ]]; then
  echo "error: unknown scenario $SCENARIO"
  exit 1
fi

RUN_ID="${AA08_RUN_ID:-run_$(date +%Y%m%d_%H%M%S)}"
STARTED_AT="${AA08_TIMESTAMP:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
OUTDIR="$OUTPUT_ROOT/$RUN_ID/$SCENARIO"
BUNDLE_PATH="$OUTDIR/bundle_manifest.json"
RUN_LOG_PATH="$OUTDIR/run.log"
RUN_REPORT_PATH="$OUTDIR/run_report.json"
RCH_ROUTED=false
if [[ "$COMMAND" == *"rch exec --"* ]]; then
  RCH_ROUTED=true
fi
mkdir -p "$OUTDIR"

jq -n \
  --arg schema "transport-frontier-benchmark-smoke-bundle-v1" \
  --arg scenario_id "$SCENARIO" \
  --arg description "$DESCRIPTION" \
  --arg workload_id "$WORKLOAD_ID" \
  --arg validation_surface "$VALIDATION_SURFACE" \
  --arg run_id "$RUN_ID" \
  --arg mode "$MODE" \
  --arg command "$COMMAND" \
  --arg timestamp "$STARTED_AT" \
  --arg artifact_path "$ARTIFACT" \
  --arg runner_script "$RUNNER_SCRIPT" \
  --arg bundle_manifest_path "$BUNDLE_PATH" \
  --arg planned_run_log_path "$RUN_LOG_PATH" \
  --arg planned_run_report_path "$RUN_REPORT_PATH" \
  --argjson focus_dimension_ids "$FOCUS_DIMENSIONS" \
  --argjson rch_routed "$RCH_ROUTED" \
  '{
    schema: $schema,
    scenario_id: $scenario_id,
    description: $description,
    workload_id: $workload_id,
    validation_surface: $validation_surface,
    focus_dimension_ids: $focus_dimension_ids,
    run_id: $run_id,
    mode: $mode,
    command: $command,
    timestamp: $timestamp,
    artifact_path: $artifact_path,
    runner_script: $runner_script,
    bundle_manifest_path: $bundle_manifest_path,
    planned_run_log_path: $planned_run_log_path,
    planned_run_report_path: $planned_run_report_path,
    rch_routed: $rch_routed
  }' > "$BUNDLE_PATH"

if [[ "$MODE" == "dry-run" ]]; then
  echo "[dry-run] $SCENARIO: $DESCRIPTION"
  echo "[dry-run] command: $COMMAND"
  echo "[dry-run] bundle: $BUNDLE_PATH"
  exit 0
fi

echo "=== Executing $SCENARIO ==="
echo "  $DESCRIPTION"
echo "  command: $COMMAND"

EXITCODE=0
eval "$COMMAND" > "$RUN_LOG_PATH" 2>&1 || EXITCODE=$?
FINISHED_AT="${AA08_FINISHED_AT:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"

jq -n \
  --arg schema "transport-frontier-benchmark-smoke-run-report-v1" \
  --arg scenario_id "$SCENARIO" \
  --arg description "$DESCRIPTION" \
  --arg workload_id "$WORKLOAD_ID" \
  --arg validation_surface "$VALIDATION_SURFACE" \
  --arg run_id "$RUN_ID" \
  --arg mode "$MODE" \
  --arg command "$COMMAND" \
  --arg artifact_path "$ARTIFACT" \
  --arg runner_script "$RUNNER_SCRIPT" \
  --arg bundle_manifest_path "$BUNDLE_PATH" \
  --arg run_log_path "$RUN_LOG_PATH" \
  --arg run_report_path "$RUN_REPORT_PATH" \
  --arg output_dir "$OUTDIR" \
  --arg started_at "$STARTED_AT" \
  --arg finished_at "$FINISHED_AT" \
  --argjson focus_dimension_ids "$FOCUS_DIMENSIONS" \
  --argjson exit_code "$EXITCODE" \
  --argjson rch_routed "$RCH_ROUTED" \
  '{
    schema: $schema,
    scenario_id: $scenario_id,
    description: $description,
    workload_id: $workload_id,
    validation_surface: $validation_surface,
    focus_dimension_ids: $focus_dimension_ids,
    run_id: $run_id,
    mode: $mode,
    command: $command,
    artifact_path: $artifact_path,
    runner_script: $runner_script,
    bundle_manifest_path: $bundle_manifest_path,
    run_log_path: $run_log_path,
    run_report_path: $run_report_path,
    output_dir: $output_dir,
    rch_routed: $rch_routed,
    started_at: $started_at,
    finished_at: $finished_at,
    exit_code: $exit_code
  }' > "$RUN_REPORT_PATH"

if [[ $EXITCODE -eq 0 ]]; then
  echo "  PASS (exit 0)"
else
  echo "  FAIL (exit $EXITCODE)"
  tail -20 "$RUN_LOG_PATH"
fi

exit $EXITCODE
