#!/usr/bin/env bash
# Transport frontier benchmark smoke runner (AA-08.1)
#
# Usage:
#   bash ./scripts/run_transport_frontier_benchmark_smoke.sh --list
#   bash ./scripts/run_transport_frontier_benchmark_smoke.sh --scenario AA08-SMOKE-WORKLOAD-VOCAB --dry-run
#   bash ./scripts/run_transport_frontier_benchmark_smoke.sh --scenario AA08-SMOKE-WORKLOAD-VOCAB --execute
#   bash ./scripts/run_transport_frontier_benchmark_smoke.sh --all --dry-run
#   bash ./scripts/run_transport_frontier_benchmark_smoke.sh --all --execute
#
# Bundle schema: transport-frontier-benchmark-smoke-bundle-v1
# Report schema: transport-frontier-benchmark-smoke-run-report-v1
# Suite summary schema: transport-frontier-benchmark-smoke-suite-summary-v1

set -euo pipefail

RUNNER_SCRIPT="scripts/run_transport_frontier_benchmark_smoke.sh"
ARTIFACT="${AA08_ARTIFACT:-artifacts/transport_frontier_benchmark_v1.json}"
OUTPUT_ROOT="${AA08_OUTPUT_ROOT:-target/transport-frontier-benchmark-smoke}"
MODE=""
SCENARIO=""
RUN_ALL=false

usage() {
  echo "Usage: $0 --list | --all (--dry-run | --execute) | --scenario <ID> (--dry-run | --execute)"
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --list)   MODE="list"; shift ;;
    --all) RUN_ALL=true; shift ;;
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

if [[ "$RUN_ALL" == true && -n "$SCENARIO" ]]; then
  echo "error: --all and --scenario are mutually exclusive"
  exit 1
fi

if [[ "$RUN_ALL" == false && -z "$SCENARIO" ]]; then
  echo "error: --scenario required with --dry-run/--execute unless --all is used"
  exit 1
fi

scenario_field() {
  local sid="$1"
  local field="$2"
  jq -r --arg sid "$sid" ".smoke_scenarios[] | select(.scenario_id == \$sid) | $field" "$ARTIFACT"
}

scenario_focus_dimensions() {
  local sid="$1"
  jq -c --arg sid "$sid" '.smoke_scenarios[] | select(.scenario_id == $sid) | (.focus_dimension_ids // [])' "$ARTIFACT"
}

write_bundle() {
  local sid="$1"
  local description="$2"
  local workload_id="$3"
  local validation_surface="$4"
  local focus_dimensions="$5"
  local command="$6"
  local run_id="$7"
  local mode="$8"
  local timestamp="$9"
  local outdir="${10}"
  local bundle_path="${11}"
  local run_log_path="${12}"
  local run_report_path="${13}"
  local rch_routed="${14}"

  jq -n \
    --arg schema "transport-frontier-benchmark-smoke-bundle-v1" \
    --arg scenario_id "$sid" \
    --arg description "$description" \
    --arg workload_id "$workload_id" \
    --arg validation_surface "$validation_surface" \
    --arg run_id "$run_id" \
    --arg mode "$mode" \
    --arg command "$command" \
    --arg timestamp "$timestamp" \
    --arg artifact_path "$ARTIFACT" \
    --arg runner_script "$RUNNER_SCRIPT" \
    --arg bundle_manifest_path "$bundle_path" \
    --arg planned_run_log_path "$run_log_path" \
    --arg planned_run_report_path "$run_report_path" \
    --argjson focus_dimension_ids "$focus_dimensions" \
    --argjson rch_routed "$rch_routed" \
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
    }' > "$bundle_path"
}

write_report() {
  local sid="$1"
  local description="$2"
  local workload_id="$3"
  local validation_surface="$4"
  local focus_dimensions="$5"
  local command="$6"
  local run_id="$7"
  local mode="$8"
  local outdir="${9}"
  local bundle_path="${10}"
  local run_log_path="${11}"
  local run_report_path="${12}"
  local rch_routed="${13}"
  local started_at="${14}"
  local finished_at="${15}"
  local exit_code="${16}"

  jq -n \
    --arg schema "transport-frontier-benchmark-smoke-run-report-v1" \
    --arg scenario_id "$sid" \
    --arg description "$description" \
    --arg workload_id "$workload_id" \
    --arg validation_surface "$validation_surface" \
    --arg run_id "$run_id" \
    --arg mode "$mode" \
    --arg command "$command" \
    --arg artifact_path "$ARTIFACT" \
    --arg runner_script "$RUNNER_SCRIPT" \
    --arg bundle_manifest_path "$bundle_path" \
    --arg run_log_path "$run_log_path" \
    --arg run_report_path "$run_report_path" \
    --arg output_dir "$outdir" \
    --arg started_at "$started_at" \
    --arg finished_at "$finished_at" \
    --argjson focus_dimension_ids "$focus_dimensions" \
    --argjson exit_code "$exit_code" \
    --argjson rch_routed "$rch_routed" \
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
    }' > "$run_report_path"
}

append_summary_entry() {
  local summary_entries_path="$1"
  local sid="$2"
  local description="$3"
  local workload_id="$4"
  local validation_surface="$5"
  local focus_dimensions="$6"
  local command="$7"
  local outdir="$8"
  local bundle_path="$9"
  local run_log_path="${10}"
  local run_report_path="${11}"
  local status="${12}"
  local exit_code_json="${13}"
  local rch_routed="${14}"

  jq -n \
    --arg scenario_id "$sid" \
    --arg description "$description" \
    --arg workload_id "$workload_id" \
    --arg validation_surface "$validation_surface" \
    --arg command "$command" \
    --arg output_dir "$outdir" \
    --arg bundle_manifest_path "$bundle_path" \
    --arg run_log_path "$run_log_path" \
    --arg run_report_path "$run_report_path" \
    --arg status "$status" \
    --argjson focus_dimension_ids "$focus_dimensions" \
    --argjson exit_code "$exit_code_json" \
    --argjson rch_routed "$rch_routed" \
    '{
      scenario_id: $scenario_id,
      description: $description,
      workload_id: $workload_id,
      validation_surface: $validation_surface,
      focus_dimension_ids: $focus_dimension_ids,
      command: $command,
      output_dir: $output_dir,
      bundle_manifest_path: $bundle_manifest_path,
      run_log_path: $run_log_path,
      run_report_path: $run_report_path,
      status: $status,
      exit_code: $exit_code,
      rch_routed: $rch_routed
    }' >> "$summary_entries_path"
}

run_single_scenario() {
  local sid="$1"
  local run_root="$2"
  local run_id="$3"
  local started_at="$4"
  local summary_entries_path="${5:-}"
  local description workload_id validation_surface focus_dimensions command outdir bundle_path
  local run_log_path run_report_path rch_routed exit_code finished_at status exit_code_json

  command=$(scenario_field "$sid" '.command')
  description=$(scenario_field "$sid" '.description')
  workload_id=$(scenario_field "$sid" '.workload_id')
  validation_surface=$(scenario_field "$sid" '.validation_surface')
  focus_dimensions=$(scenario_focus_dimensions "$sid")

  if [[ -z "$command" || "$command" == "null" ]]; then
    echo "error: unknown scenario $sid"
    exit 1
  fi

  outdir="$run_root/$sid"
  bundle_path="$outdir/bundle_manifest.json"
  run_log_path="$outdir/run.log"
  run_report_path="$outdir/run_report.json"
  rch_routed=false
  if [[ "$command" == *"rch exec --"* ]]; then
    rch_routed=true
  fi
  mkdir -p "$outdir"

  write_bundle \
    "$sid" \
    "$description" \
    "$workload_id" \
    "$validation_surface" \
    "$focus_dimensions" \
    "$command" \
    "$run_id" \
    "$MODE" \
    "$started_at" \
    "$outdir" \
    "$bundle_path" \
    "$run_log_path" \
    "$run_report_path" \
    "$rch_routed"

  if [[ "$MODE" == "dry-run" ]]; then
    finished_at="${AA08_FINISHED_AT:-$started_at}"
    write_report \
      "$sid" \
      "$description" \
      "$workload_id" \
      "$validation_surface" \
      "$focus_dimensions" \
      "$command" \
      "$run_id" \
      "$MODE" \
      "$outdir" \
      "$bundle_path" \
      "$run_log_path" \
      "$run_report_path" \
      "$rch_routed" \
      "$started_at" \
      "$finished_at" \
      "0"
    echo "[dry-run] $sid: $description"
    echo "[dry-run] command: $command"
    echo "[dry-run] bundle: $bundle_path"
    if [[ -n "$summary_entries_path" ]]; then
      append_summary_entry \
        "$summary_entries_path" \
        "$sid" \
        "$description" \
        "$workload_id" \
        "$validation_surface" \
        "$focus_dimensions" \
        "$command" \
        "$outdir" \
        "$bundle_path" \
        "$run_log_path" \
        "$run_report_path" \
        "planned" \
        "null" \
        "$rch_routed"
    fi
    return 0
  fi

  echo "=== Executing $sid ==="
  echo "  $description"
  echo "  command: $command"

  exit_code=0
  eval "$command" > "$run_log_path" 2>&1 || exit_code=$?
  finished_at="${AA08_FINISHED_AT:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"

  write_report \
    "$sid" \
    "$description" \
    "$workload_id" \
    "$validation_surface" \
    "$focus_dimensions" \
    "$command" \
    "$run_id" \
    "$MODE" \
    "$outdir" \
    "$bundle_path" \
    "$run_log_path" \
    "$run_report_path" \
    "$rch_routed" \
    "$started_at" \
    "$finished_at" \
    "$exit_code"

  status="passed"
  if [[ $exit_code -ne 0 ]]; then
    status="failed"
  fi

  if [[ -n "$summary_entries_path" ]]; then
    exit_code_json="$exit_code"
    append_summary_entry \
      "$summary_entries_path" \
      "$sid" \
      "$description" \
      "$workload_id" \
      "$validation_surface" \
      "$focus_dimensions" \
      "$command" \
      "$outdir" \
      "$bundle_path" \
      "$run_log_path" \
      "$run_report_path" \
      "$status" \
      "$exit_code_json" \
      "$rch_routed"
  fi

  if [[ $exit_code -eq 0 ]]; then
    echo "  PASS (exit 0)"
  else
    echo "  FAIL (exit $exit_code)"
    tail -20 "$run_log_path"
  fi

  return "$exit_code"
}

RUN_ID="${AA08_RUN_ID:-run_$(date +%Y%m%d_%H%M%S)}"
STARTED_AT="${AA08_TIMESTAMP:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
RUN_ROOT="$OUTPUT_ROOT/$RUN_ID"
mkdir -p "$RUN_ROOT"

if [[ "$RUN_ALL" == true ]]; then
  SUMMARY_PATH="$RUN_ROOT/summary.json"
  SUMMARY_ENTRIES_PATH="$RUN_ROOT/.summary_entries.jsonl"
  SUITE_EXIT_CODE=0
  SUITE_STATUS="planned"
  ALL_RCH_ROUTED=true

  : > "$SUMMARY_ENTRIES_PATH"

  while IFS= read -r sid; do
    if run_single_scenario "$sid" "$RUN_ROOT" "$RUN_ID" "$STARTED_AT" "$SUMMARY_ENTRIES_PATH"; then
      :
    else
      exit_code=$?
      if [[ $SUITE_EXIT_CODE -eq 0 ]]; then
        SUITE_EXIT_CODE=$exit_code
      fi
    fi
    command=$(scenario_field "$sid" '.command')
    if [[ "$command" != *"rch exec --"* ]]; then
      ALL_RCH_ROUTED=false
    fi
  done < <(jq -r '.smoke_scenarios[].scenario_id' "$ARTIFACT")

  FINISHED_AT="${AA08_FINISHED_AT:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
  SCENARIO_IDS=$(jq -c '[.smoke_scenarios[].scenario_id]' "$ARTIFACT")
  SCENARIO_COUNT=$(jq '.smoke_scenarios | length' "$ARTIFACT")
  SCENARIOS=$(jq -s '.' "$SUMMARY_ENTRIES_PATH")

  if [[ "$MODE" == "execute" ]]; then
    SUITE_STATUS="passed"
    if [[ $SUITE_EXIT_CODE -ne 0 ]]; then
      SUITE_STATUS="failed"
    fi
    SUITE_EXIT_CODE_JSON="$SUITE_EXIT_CODE"
  else
    SUITE_EXIT_CODE_JSON="null"
  fi

  jq -n \
    --arg schema "transport-frontier-benchmark-smoke-suite-summary-v1" \
    --arg run_id "$RUN_ID" \
    --arg mode "$MODE" \
    --arg artifact_path "$ARTIFACT" \
    --arg runner_script "$RUNNER_SCRIPT" \
    --arg output_dir "$RUN_ROOT" \
    --arg summary_path "$SUMMARY_PATH" \
    --arg started_at "$STARTED_AT" \
    --arg finished_at "$FINISHED_AT" \
    --arg status "$SUITE_STATUS" \
    --argjson scenario_count "$SCENARIO_COUNT" \
    --argjson scenario_ids "$SCENARIO_IDS" \
    --argjson all_rch_routed "$ALL_RCH_ROUTED" \
    --argjson suite_exit_code "$SUITE_EXIT_CODE_JSON" \
    --argjson scenarios "$SCENARIOS" \
    '{
      schema: $schema,
      run_id: $run_id,
      mode: $mode,
      artifact_path: $artifact_path,
      runner_script: $runner_script,
      output_dir: $output_dir,
      summary_path: $summary_path,
      started_at: $started_at,
      finished_at: $finished_at,
      status: $status,
      scenario_count: $scenario_count,
      scenario_ids: $scenario_ids,
      all_rch_routed: $all_rch_routed,
      suite_exit_code: $suite_exit_code,
      scenarios: $scenarios
    }' > "$SUMMARY_PATH"

  rm -f "$SUMMARY_ENTRIES_PATH"

  echo "[${MODE}] summary: $SUMMARY_PATH"
  exit "$SUITE_EXIT_CODE"
fi

run_single_scenario "$SCENARIO" "$RUN_ROOT" "$RUN_ID" "$STARTED_AT"
exit $?
