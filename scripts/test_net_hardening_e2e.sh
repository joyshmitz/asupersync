#!/usr/bin/env bash
# E2E Test Script for Network Primitives Hardening Verification
#
# Runs the full network test pyramid:
#   1. TCP inline unit tests
#   2. UDP inline unit tests
#   3. TCP integration tests
#   4. UDP integration tests
#   5. Unix socket integration tests
#   6. Network hardening tests (keepalive, error handling, concurrency)
#   7. Network verification suite
#
# Usage:
#   ./scripts/test_net_hardening_e2e.sh
#
# Environment Variables:
#   RUST_LOG       - Standard Rust logging level

set -euo pipefail

OUTPUT_DIR="target/e2e-results/net_hardening"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_DIR="$OUTPUT_DIR/$TIMESTAMP"

export RUST_LOG="${RUST_LOG:-info}"
export RUST_BACKTRACE="${RUST_BACKTRACE:-1}"

mkdir -p "$LOG_DIR"

TOTAL_SUITES=0
PASSED_SUITES=0
FAILED_SUITES=0

echo "==================================================================="
echo "       Network Primitives Hardening E2E Test Suite                  "
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
# 1. TCP inline unit tests
# --------------------------------------------------------------------------
run_suite "tcp_unit" \
    cargo test --lib net::tcp -- --nocapture || true

# --------------------------------------------------------------------------
# 2. UDP inline unit tests
# --------------------------------------------------------------------------
run_suite "udp_unit" \
    cargo test --lib net::udp -- --nocapture || true

# --------------------------------------------------------------------------
# 3. TCP integration tests
# --------------------------------------------------------------------------
run_suite "tcp_integration" \
    cargo test --test net_tcp -- --nocapture || true

# --------------------------------------------------------------------------
# 4. UDP integration tests
# --------------------------------------------------------------------------
run_suite "udp_integration" \
    cargo test --test net_udp -- --nocapture || true

# --------------------------------------------------------------------------
# 5. Unix socket integration tests
# --------------------------------------------------------------------------
run_suite "unix_integration" \
    cargo test --test net_unix -- --nocapture || true

# --------------------------------------------------------------------------
# 6. Network hardening tests
# --------------------------------------------------------------------------
run_suite "net_hardening" \
    cargo test --test net_hardening -- --nocapture || true

# --------------------------------------------------------------------------
# 7. Network verification suite
# --------------------------------------------------------------------------
run_suite "net_verification" \
    cargo test --test net_verification -- --nocapture || true

# --------------------------------------------------------------------------
# Failure pattern analysis
# --------------------------------------------------------------------------
echo ""
echo ">>> Analyzing logs for issues..."
ISSUES=0

for pattern in "timed out" "connection refused" "broken pipe" "reset by peer"; do
    count=$(grep -rci "$pattern" "$LOG_DIR"/*.log 2>/dev/null | awk -F: '{s+=$2}END{print s+0}')
    if [ "$count" -gt 0 ]; then
        echo "  NOTE: '$pattern' appeared $count time(s) (may be expected)"
    fi
done

if grep -rq "panicked at" "$LOG_DIR"/*.log 2>/dev/null; then
    echo "  WARNING: Panics detected"
    grep -rh "panicked at" "$LOG_DIR"/*.log | head -5
    ISSUES=$((ISSUES + 1))
fi

if grep -rqi "leak" "$LOG_DIR"/*.log 2>/dev/null; then
    echo "  WARNING: Potential leak detected"
    ISSUES=$((ISSUES + 1))
fi

# --------------------------------------------------------------------------
# Summary
# --------------------------------------------------------------------------
cat > "$LOG_DIR/summary.md" << EOF
# Network Hardening E2E Test Report

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
echo "All network hardening tests passed!"
