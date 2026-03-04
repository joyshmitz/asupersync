#!/usr/bin/env bash
# Doctor Orchestration State-Machine E2E Runner (asupersync-2b4jj.3.7)
#
# Executes the orchestration state-machine unit-test slice through rch and
# verifies deterministic outcomes across repeated runs.
#
# Usage:
#   ./scripts/test_doctor_orchestration_state_machine_e2e.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
OUTPUT_DIR="${PROJECT_ROOT}/target/e2e-results/doctor_orchestration_state_machine"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RUN_STARTED_TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
ARTIFACT_DIR="${OUTPUT_DIR}/artifacts_${TIMESTAMP}"
SUMMARY_FILE="${ARTIFACT_DIR}/summary.json"
RUN1_JSON="${ARTIFACT_DIR}/suite_run1.json"
RUN2_JSON="${ARTIFACT_DIR}/suite_run2.json"
RUN1_LOG="${ARTIFACT_DIR}/run1.log"
RUN2_LOG="${ARTIFACT_DIR}/run2.log"
SUITE_ID="doctor_orchestration_state_machine_e2e"
SCENARIO_ID="E2E-SUITE-DOCTOR-ORCHESTRATION-STATE-MACHINE"
TEST_FILTER="orchestration_state_machine_"
EXPECTED_MIN_TESTS=5

export TEST_LOG_LEVEL="${TEST_LOG_LEVEL:-info}"
export RUST_LOG="${RUST_LOG:-asupersync=info}"
export TEST_SEED="${TEST_SEED:-4242}"
DOCTOR_FULLSTACK_SINGLE_RUN="${DOCTOR_FULLSTACK_SINGLE_RUN:-0}"
RCH_SCAN_TIMEOUT="${RCH_SCAN_TIMEOUT:-360}"
RCH_RETRY_ATTEMPTS="${RCH_RETRY_ATTEMPTS:-3}"

RCH_BIN="${RCH_BIN:-$HOME/.local/bin/rch}"
if [[ ! -x "${RCH_BIN}" ]]; then
    echo "FATAL: rch is required and was not found/executable at: ${RCH_BIN}" >&2
    exit 1
fi

mkdir -p "${OUTPUT_DIR}" "${ARTIFACT_DIR}"

echo "==================================================================="
echo "   Asupersync Doctor Orchestration State-Machine E2E              "
echo "==================================================================="
echo "Config:"
echo "  RCH_BIN:          ${RCH_BIN}"
echo "  TEST_LOG_LEVEL:   ${TEST_LOG_LEVEL}"
echo "  RUST_LOG:         ${RUST_LOG}"
echo "  TEST_SEED:        ${TEST_SEED}"
echo "  Test filter:      ${TEST_FILTER}"
echo "  Artifact dir:     ${ARTIFACT_DIR}"
echo ""

EXIT_CODE=0
CHECK_FAILURES=0
CHECKS_PASSED=0

run_suite_call() {
    local run_label="$1"
    local run_log="$2"
    local run_json="$3"
    local run_id="$4"
    local rc=0
    local attempt_log=""
    local running_count=""
    local passed_count=""

    for ((attempt = 1; attempt <= RCH_RETRY_ATTEMPTS; attempt++)); do
        local target_dir="/tmp/rch-doctor-orch-sm-${TIMESTAMP}"
        local -a run_cmd=(
            env "CARGO_TARGET_DIR=${target_dir}" \
            cargo test --quiet --features cli --lib "${TEST_FILTER}" -- --nocapture
        )

        attempt_log="${run_log%.log}.attempt${attempt}.log"
        if timeout "${RCH_SCAN_TIMEOUT}s" "${RCH_BIN}" exec -- "${run_cmd[@]}" >"${attempt_log}" 2>&1; then
            rc=0
        else
            rc=$?
        fi

        running_count="$(
            grep -Eo 'running [0-9]+ tests' "${attempt_log}" \
                | tail -n1 \
                | awk '{print $2}' \
                || true
        )"
        passed_count="$(
            sed -nE 's/.*test result: ok\. ([0-9]+) passed.*/\1/p' "${attempt_log}" \
                | tail -n1 \
                || true
        )"

        if [[ ${rc} -eq 0 && -n "${running_count}" && -n "${passed_count}" ]]; then
            cp "${attempt_log}" "${run_log}"
            jq -n \
                --arg schema_version "doctor-orchestration-state-machine-e2e-v1" \
                --arg test_filter "${TEST_FILTER}" \
                --arg status "passed" \
                --argjson running_tests "${running_count}" \
                --argjson passed_tests "${passed_count}" \
                --arg replay_ref "replay:doctor-orchestration-state-machine-v1" \
                '{
                  schema_version: $schema_version,
                  test_filter: $test_filter,
                  status: $status,
                  running_tests: $running_tests,
                  passed_tests: $passed_tests,
                  replay_ref: $replay_ref
                }' > "${run_json}"
            return 0
        fi

        if [[ ${attempt} -lt ${RCH_RETRY_ATTEMPTS} ]]; then
            echo "  WARN: ${run_label} attempt ${attempt}/${RCH_RETRY_ATTEMPTS} failed (exit=${rc}); retrying"
            sleep 1
        fi
    done

    if [[ -n "${attempt_log}" && -f "${attempt_log}" ]]; then
        cp "${attempt_log}" "${run_log}"
    fi
    echo "  ERROR: ${run_label} failed after ${RCH_RETRY_ATTEMPTS} attempts (see ${run_log})"
    return 1
}

echo ">>> [1/4] Running orchestration suite (run 1) via rch..."
if ! run_suite_call "suite run 1" "${RUN1_LOG}" "${RUN1_JSON}" "run1"; then
    EXIT_CODE=1
fi

if [[ "${DOCTOR_FULLSTACK_SINGLE_RUN}" == "1" ]]; then
    cp "${RUN1_LOG}" "${RUN2_LOG}"
    cp "${RUN1_JSON}" "${RUN2_JSON}"
else
    echo ">>> [2/4] Running orchestration suite (run 2) via rch..."
    if ! run_suite_call "suite run 2" "${RUN2_LOG}" "${RUN2_JSON}" "run2"; then
        EXIT_CODE=1
    fi
fi

if [[ ${EXIT_CODE} -eq 0 ]]; then
    echo ">>> [3/4] Verifying deterministic suite summary..."
    if diff -u "${RUN1_JSON}" "${RUN2_JSON}" > "${ARTIFACT_DIR}/determinism.diff"; then
        CHECKS_PASSED=$((CHECKS_PASSED + 1))
        rm -f "${ARTIFACT_DIR}/determinism.diff"
    else
        echo "  ERROR: run summaries diverged (see determinism.diff)"
        CHECK_FAILURES=$((CHECK_FAILURES + 1))
    fi

    echo ">>> [4/4] Validating suite schema + coverage floor..."
    if jq -e \
        --arg filter "${TEST_FILTER}" \
        --argjson min_tests "${EXPECTED_MIN_TESTS}" '
        .schema_version == "doctor-orchestration-state-machine-e2e-v1" and
        .test_filter == $filter and
        .status == "passed" and
        (. as $report | ($report.running_tests | type == "number") and $report.running_tests >= $min_tests) and
        (.passed_tests | type == "number") and
        .passed_tests == .running_tests and
        (.replay_ref | type == "string" and startswith("replay:"))
    ' "${RUN1_JSON}" >/dev/null; then
        CHECKS_PASSED=$((CHECKS_PASSED + 1))
    else
        echo "  ERROR: schema/coverage validation failed"
        CHECK_FAILURES=$((CHECK_FAILURES + 1))
    fi
fi

RUN_ENDED_TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
SUITE_STATUS="failed"
FAILURE_CLASS="test_or_pattern_failure"

if [[ ${EXIT_CODE} -eq 0 && ${CHECK_FAILURES} -eq 0 ]]; then
    SUITE_STATUS="passed"
    FAILURE_CLASS="none"
fi

TESTS_PASSED=0
TESTS_FAILED=1
if [[ "${SUITE_STATUS}" == "passed" ]]; then
    TESTS_PASSED=1
    TESTS_FAILED=0
fi

REPRO_COMMAND="TEST_LOG_LEVEL=${TEST_LOG_LEVEL} RUST_LOG=${RUST_LOG} TEST_SEED=${TEST_SEED} RCH_BIN=${RCH_BIN} bash ${SCRIPT_DIR}/$(basename "$0")"

cat > "${SUMMARY_FILE}" <<ENDJSON
{
  "schema_version": "e2e-suite-summary-v3",
  "suite_id": "${SUITE_ID}",
  "scenario_id": "${SCENARIO_ID}",
  "seed": "${TEST_SEED}",
  "started_ts": "${RUN_STARTED_TS}",
  "ended_ts": "${RUN_ENDED_TS}",
  "status": "${SUITE_STATUS}",
  "failure_class": "${FAILURE_CLASS}",
  "repro_command": "${REPRO_COMMAND}",
  "artifact_path": "${SUMMARY_FILE}",
  "suite": "${SUITE_ID}",
  "timestamp": "${TIMESTAMP}",
  "test_log_level": "${TEST_LOG_LEVEL}",
  "tests_passed": ${TESTS_PASSED},
  "tests_failed": ${TESTS_FAILED},
  "exit_code": ${EXIT_CODE},
  "pattern_failures": ${CHECK_FAILURES},
  "log_file": "${RUN1_LOG}",
  "artifact_dir": "${ARTIFACT_DIR}",
  "checks_passed": ${CHECKS_PASSED}
}
ENDJSON

echo ""
echo "==================================================================="
echo "  Doctor Orchestration State-Machine E2E Summary                  "
echo "==================================================================="
echo "  Status:         ${SUITE_STATUS}"
echo "  Exit code:      ${EXIT_CODE}"
echo "  Check failures: ${CHECK_FAILURES}"
echo "  Checks passed:  ${CHECKS_PASSED}"
echo "  Summary:        ${SUMMARY_FILE}"
echo "==================================================================="

if [[ ${EXIT_CODE} -ne 0 || ${CHECK_FAILURES} -ne 0 ]]; then
    exit 1
fi
