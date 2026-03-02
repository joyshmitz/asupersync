#!/usr/bin/env bash
# Unified Semantic Verification Runner (SEM-12.9)
#
# Single entrypoint for all semantic verification suites.
# Orchestrates docs lint, runtime tests, golden fixtures, Lean proofs,
# and TLA+ model checks with consistent output and CI/local parity.
#
# Usage:
#   scripts/run_semantic_verification.sh [OPTIONS]
#
# Options:
#   --profile PROFILE   Run profile: smoke (fast), full (default), forensics (verbose)
#   --json              Write structured JSON report
#   --ci                CI mode: strict exit codes, artifact publishing
#   --verbose           Verbose output for all suites
#   --suite SUITE       Run only specified suite (docs, runtime, golden, lean, tla)
#
# Exit codes:
#   0 - All suites passed
#   1 - One or more suites failed
#   2 - Configuration error
#
# Bead: asupersync-3cddg.12.9

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPORT_DIR="$PROJECT_ROOT/target/semantic-verification"
REPORT_FILE="$REPORT_DIR/verification_report.json"
PROFILE="full"
JSON_OUTPUT=false
CI_MODE=false
VERBOSE=false
SUITE_FILTER=""

# Parse arguments
while [ $# -gt 0 ]; do
  case "$1" in
    --profile) PROFILE="$2"; shift 2 ;;
    --json) JSON_OUTPUT=true; shift ;;
    --ci) CI_MODE=true; JSON_OUTPUT=true; shift ;;
    --verbose) VERBOSE=true; shift ;;
    --suite) SUITE_FILTER="$2"; shift 2 ;;
    -h|--help)
      head -28 "$0" | tail -25
      exit 0
      ;;
    *) echo "Unknown argument: $1"; exit 2 ;;
  esac
done

mkdir -p "$REPORT_DIR"

# ─── Suite definitions ────────────────────────────────────────────

# Each suite: (name, command, required?)
declare -A SUITE_CMDS
declare -A SUITE_REQUIRED

SUITE_CMDS[docs]="cargo test --test semantic_docs_lint --test semantic_docs_rule_mapping_lint"
SUITE_CMDS[golden]="cargo test --test semantic_golden_fixture_validation"
SUITE_CMDS[lean_validation]="cargo test --test semantic_lean_regression"
SUITE_CMDS[tla_validation]="cargo test --test semantic_tla_scenarios"
SUITE_CMDS[lean_build]="scripts/run_lean_regression.sh --json"
SUITE_CMDS[tla_check]="scripts/run_tla_scenarios.sh --json"

# Required suites must pass; optional suites are reported but don't fail the run
SUITE_REQUIRED[docs]=true
SUITE_REQUIRED[golden]=true
SUITE_REQUIRED[lean_validation]=true
SUITE_REQUIRED[tla_validation]=true
SUITE_REQUIRED[lean_build]=false    # Requires Lean toolchain
SUITE_REQUIRED[tla_check]=false     # Requires TLC

# Profile-based suite selection
case "$PROFILE" in
  smoke)
    SUITES="docs golden"
    ;;
  full)
    SUITES="docs golden lean_validation tla_validation lean_build tla_check"
    ;;
  forensics)
    SUITES="docs golden lean_validation tla_validation lean_build tla_check"
    VERBOSE=true
    ;;
  *)
    echo "ERROR: Unknown profile '$PROFILE'. Use: smoke, full, forensics"
    exit 2
    ;;
esac

# Apply suite filter if specified
if [ -n "$SUITE_FILTER" ]; then
  case "$SUITE_FILTER" in
    docs) SUITES="docs" ;;
    runtime) SUITES="lean_validation tla_validation" ;;
    golden) SUITES="golden" ;;
    lean) SUITES="lean_validation lean_build" ;;
    tla) SUITES="tla_validation tla_check" ;;
    *) echo "ERROR: Unknown suite '$SUITE_FILTER'. Use: docs, runtime, golden, lean, tla"; exit 2 ;;
  esac
fi

# ─── Run suites ──────────────────────────────────────────────────

log() {
  echo "[semantic-verify] $*"
}

TOTAL=0
PASSED=0
FAILED=0
SKIPPED=0
RESULTS=()

RUN_START=$(date +%s)

for suite in $SUITES; do
  ((TOTAL++)) || true
  cmd="${SUITE_CMDS[$suite]}"
  required="${SUITE_REQUIRED[$suite]}"

  log "Running suite: $suite"
  SUITE_START=$(date +%s)

  suite_output=""
  suite_exit=0
  suite_output=$(cd "$PROJECT_ROOT" && eval "$cmd" 2>&1) || suite_exit=$?

  SUITE_END=$(date +%s)
  SUITE_DURATION=$((SUITE_END - SUITE_START))

  if [ "$suite_exit" -eq 0 ]; then
    status="passed"
    ((PASSED++)) || true
    log "  $suite: PASSED (${SUITE_DURATION}s)"
  else
    # Check if it was a graceful skip
    if echo "$suite_output" | grep -q "SKIP:"; then
      status="skipped"
      ((SKIPPED++)) || true
      log "  $suite: SKIPPED (${SUITE_DURATION}s)"
    else
      status="failed"
      ((FAILED++)) || true
      log "  $suite: FAILED (${SUITE_DURATION}s)"
      if [ "$VERBOSE" = true ]; then
        echo "$suite_output" | tail -20
      fi
    fi
  fi

  RESULTS+=("$suite|$status|$SUITE_DURATION|$required")

  # Save suite output
  echo "$suite_output" > "$REPORT_DIR/${suite}_output.txt"
done

RUN_END=$(date +%s)
TOTAL_DURATION=$((RUN_END - RUN_START))

# ─── Summary ─────────────────────────────────────────────────────

echo ""
echo "══════════════════════════════════════════════════"
echo " Semantic Verification Summary ($PROFILE profile)"
echo "══════════════════════════════════════════════════"
echo ""
printf "  %-20s %s\n" "Total suites:" "$TOTAL"
printf "  %-20s %s\n" "Passed:" "$PASSED"
printf "  %-20s %s\n" "Failed:" "$FAILED"
printf "  %-20s %s\n" "Skipped:" "$SKIPPED"
printf "  %-20s %s\n" "Duration:" "${TOTAL_DURATION}s"
echo ""
echo "  Suite Results:"
for result in "${RESULTS[@]}"; do
  IFS='|' read -r name rstatus dur req <<< "$result"
  case "$rstatus" in
    passed)  marker="[PASS]" ;;
    failed)  marker="[FAIL]" ;;
    skipped) marker="[SKIP]" ;;
    *)       marker="[????]" ;;
  esac
  printf "    %-20s %s  (%ss)\n" "$name" "$marker" "$dur"
done
echo ""

# ─── JSON report ─────────────────────────────────────────────────

if [ "$JSON_OUTPUT" = true ]; then
  RESULTS_JSON="["
  FIRST=true
  for result in "${RESULTS[@]}"; do
    IFS='|' read -r name rstatus dur req <<< "$result"
    if [ "$FIRST" = false ]; then RESULTS_JSON+=","; fi
    RESULTS_JSON+="{\"suite\":\"$name\",\"status\":\"$rstatus\",\"duration_s\":$dur,\"required\":$req}"
    FIRST=false
  done
  RESULTS_JSON+="]"

  cat > "$REPORT_FILE" <<EOF
{
  "schema": "semantic-verification-report-v1",
  "profile": "$PROFILE",
  "ci_mode": $CI_MODE,
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "total_duration_s": $TOTAL_DURATION,
  "suites_total": $TOTAL,
  "suites_passed": $PASSED,
  "suites_failed": $FAILED,
  "suites_skipped": $SKIPPED,
  "overall_status": "$([ "$FAILED" -eq 0 ] && echo "passed" || echo "failed")",
  "results": $RESULTS_JSON,
  "report_dir": "$REPORT_DIR"
}
EOF
  log "JSON report: $REPORT_FILE"
fi

# ─── Exit code ───────────────────────────────────────────────────

# In CI mode, only required suite failures cause non-zero exit
if [ "$CI_MODE" = true ]; then
  REQUIRED_FAILURES=0
  for result in "${RESULTS[@]}"; do
    IFS='|' read -r name rstatus dur req <<< "$result"
    if [ "$rstatus" = "failed" ] && [ "$req" = "true" ]; then
      ((REQUIRED_FAILURES++)) || true
    fi
  done
  if [ "$REQUIRED_FAILURES" -gt 0 ]; then
    log "CI FAILED: $REQUIRED_FAILURES required suite(s) failed"
    exit 1
  fi
  exit 0
fi

# In local mode, any failure is an error
if [ "$FAILED" -gt 0 ]; then
  exit 1
fi
exit 0
