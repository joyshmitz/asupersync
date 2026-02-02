#!/usr/bin/env bash
# E2E Test Script for HTTP/2 Security Hardening Verification
#
# Runs the full HTTP/2 security test pyramid:
#   1. HPACK inline unit tests (integer, huffman, dynamic table, RFC compliance)
#   2. H2 frame/settings/connection inline tests
#   3. Dedicated security integration tests (h2_security)
#   4. HPACK stress tests (malformed inputs, random bytes, table churn)
#   5. Fuzz corpus validation (seeds only, no long-running fuzz)
#
# Usage:
#   ./scripts/test_h2_security_e2e.sh
#
# Environment Variables:
#   SKIP_FUZZ      - Set to 1 to skip fuzz seed validation
#   RUST_LOG       - Standard Rust logging level

set -euo pipefail

OUTPUT_DIR="target/e2e-results/h2_security"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_DIR="$OUTPUT_DIR/$TIMESTAMP"

export RUST_LOG="${RUST_LOG:-info}"
export RUST_BACKTRACE="${RUST_BACKTRACE:-1}"

mkdir -p "$LOG_DIR"

TOTAL_SUITES=0
PASSED_SUITES=0
FAILED_SUITES=0

echo "==================================================================="
echo "       HTTP/2 Security E2E Test Suite                              "
echo "==================================================================="
echo ""
echo "  Log directory: $LOG_DIR"
echo "  Start time:    $(date -Iseconds)"
echo ""

run_suite() {
    local name="$1"
    local log_file="$LOG_DIR/${name}.log"
    shift
    TOTAL_SUITES=$((TOTAL_SUITES + 1))

    echo "[$TOTAL_SUITES] Running $name..."
    if "$@" 2>&1 | tee "$log_file"; then
        echo "    PASS"
        PASSED_SUITES=$((PASSED_SUITES + 1))
        return 0
    else
        echo "    FAIL (see $log_file)"
        FAILED_SUITES=$((FAILED_SUITES + 1))
        return 1
    fi
}

# --------------------------------------------------------------------------
# 1. HPACK inline unit tests
# --------------------------------------------------------------------------
run_suite "hpack_unit" \
    cargo test --lib http::h2::hpack -- --nocapture || true

# --------------------------------------------------------------------------
# 2. H2 frame/settings inline tests
# --------------------------------------------------------------------------
run_suite "h2_frame_unit" \
    cargo test --lib http::h2::frame -- --nocapture || true

run_suite "h2_settings_unit" \
    cargo test --lib http::h2::settings -- --nocapture || true

# --------------------------------------------------------------------------
# 3. Dedicated security integration tests
# --------------------------------------------------------------------------
run_suite "h2_security_integration" \
    cargo test --test h2_security -- --nocapture || true

# --------------------------------------------------------------------------
# 4. HTTP verification tests (existing)
# --------------------------------------------------------------------------
run_suite "http_verification" \
    cargo test --test http_verification -- --nocapture || true

# --------------------------------------------------------------------------
# 5. Fuzz seed validation (runs each seed through fuzz target, fast)
# --------------------------------------------------------------------------
if [ "${SKIP_FUZZ:-0}" != "1" ] && [ -d "fuzz/seeds" ]; then
    run_suite "fuzz_seed_hpack" \
        cargo test --lib stress_test_hpack -- --nocapture || true

    run_suite "fuzz_seed_huffman" \
        cargo test --lib stress_test_huffman -- --nocapture || true
else
    echo "[skip] Fuzz seed validation"
fi

# --------------------------------------------------------------------------
# Failure pattern analysis
# --------------------------------------------------------------------------
echo ""
echo ">>> Analyzing logs for security issues..."
ISSUES=0

for pattern in "overflow" "out of bounds" "index out of range" "memory allocation" "stack overflow"; do
    if grep -rqi "$pattern" "$LOG_DIR"/*.log 2>/dev/null; then
        echo "  WARNING: '$pattern' detected"
        ISSUES=$((ISSUES + 1))
    fi
done

if grep -rq "panicked at" "$LOG_DIR"/*.log 2>/dev/null; then
    echo "  WARNING: Panics detected"
    grep -rh "panicked at" "$LOG_DIR"/*.log | head -5
    ISSUES=$((ISSUES + 1))
fi

# --------------------------------------------------------------------------
# Summary
# --------------------------------------------------------------------------
cat > "$LOG_DIR/summary.md" << EOF
# HTTP/2 Security E2E Test Report

**Date:** $(date -Iseconds)

## Results

| Suite | Status |
|-------|--------|
| Total | $TOTAL_SUITES |
| Passed | $PASSED_SUITES |
| Failed | $FAILED_SUITES |
| Issues | $ISSUES |

## Test Counts
$(grep -rh "^test result:" "$LOG_DIR"/*.log 2>/dev/null || echo "N/A")
EOF

echo ""
echo "==================================================================="
echo "                       SUMMARY                                     "
echo "==================================================================="
echo "  Suites:  $PASSED_SUITES/$TOTAL_SUITES passed"
echo "  Issues:  $ISSUES pattern warnings"
echo "  Logs:    $LOG_DIR/"
echo "  End:     $(date -Iseconds)"
echo "==================================================================="

if [ "$FAILED_SUITES" -gt 0 ] || [ "$ISSUES" -gt 0 ]; then
    exit 1
fi

echo ""
echo "All HTTP/2 security tests passed!"
