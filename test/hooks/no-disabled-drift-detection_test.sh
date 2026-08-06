#!/usr/bin/env bash
# no-disabled-drift-detection_test.sh — guardrail: no example manifest under
# examples/ may set spec.driftDetection.mode to "disabled". A disabled
# example stops E2E from proving convergence, which is the one thing E2E
# exists to do; `warn` still detects drift and is permitted.
#
# This file is both the unit test for disabled_drift_detection_files()
# (synthetic fixtures under a temp dir) and the guardrail itself: the final
# case runs the check against this repo's REAL examples/ directory, so a
# future regression fails `make reviewable` (test.tools), not just a review
# comment.
#
# Run: bash test/hooks/no-disabled-drift-detection_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=./lib/no-disabled-drift.sh
source "${SCRIPT_DIR}/lib/no-disabled-drift.sh"

PASS=0
FAIL=0

assert_true() {
    local desc="$1" cond="$2"
    if [ "$cond" = "0" ]; then
        echo "  ok   ${desc}"
        PASS=$((PASS + 1))
    else
        echo "  FAIL ${desc}: expected success (0), got exit ${cond}"
        FAIL=$((FAIL + 1))
    fi
}

assert_false() {
    local desc="$1" cond="$2"
    if [ "$cond" != "0" ]; then
        echo "  ok   ${desc}"
        PASS=$((PASS + 1))
    else
        echo "  FAIL ${desc}: expected failure (non-zero), got exit 0"
        FAIL=$((FAIL + 1))
    fi
}

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "=== TestDisabledDriftDetectionFilesDetectsDisabled ==="
mkdir -p "$TMP/bad/x"
cat > "$TMP/bad/x/x.yaml" <<'EOF'
spec:
  driftDetection:
    mode: disabled
  forProvider:
    comment: seed
EOF
if OUT="$(disabled_drift_detection_files "$TMP/bad" 2>&1)"; then RC=0; else RC=$?; fi
assert_false "mode: disabled is reported as offending" "$RC"
case "$OUT" in
    *"x.yaml"*)
        echo "  ok   offending file named in output"
        PASS=$((PASS + 1))
        ;;
    *)
        echo "  FAIL offending file not named in output: ${OUT}"
        FAIL=$((FAIL + 1))
        ;;
esac

echo "=== TestDisabledDriftDetectionFilesAllowsWarn ==="
mkdir -p "$TMP/warn/x"
cat > "$TMP/warn/x/x.yaml" <<'EOF'
spec:
  driftDetection:
    mode: warn
  forProvider:
    comment: seed
EOF
if disabled_drift_detection_files "$TMP/warn" >/dev/null 2>&1; then rc=0; else rc=$?; fi
assert_true "mode: warn is permitted" "$rc"

echo "=== TestDisabledDriftDetectionFilesAllowsAbsent ==="
mkdir -p "$TMP/absent/x"
cat > "$TMP/absent/x/x.yaml" <<'EOF'
spec:
  forProvider:
    comment: seed
EOF
if disabled_drift_detection_files "$TMP/absent" >/dev/null 2>&1; then rc=0; else rc=$?; fi
assert_true "no driftDetection block is permitted" "$rc"

echo "=== TestDisabledDriftDetectionFilesIgnoresCommentedOutMode ==="
mkdir -p "$TMP/commented/x"
cat > "$TMP/commented/x/x.yaml" <<'EOF'
spec:
  driftDetection:
    # mode: disabled -- documentation example only, not live config
    mode: warn
  forProvider:
    comment: seed
EOF
if disabled_drift_detection_files "$TMP/commented" >/dev/null 2>&1; then rc=0; else rc=$?; fi
assert_true "a commented-out mode: disabled line is not flagged" "$rc"

echo "=== TestDisabledDriftDetectionFilesIgnoresProviderConfigDir ==="
mkdir -p "$TMP/withprovider/provider"
cat > "$TMP/withprovider/provider/provider.yaml" <<'EOF'
spec:
  driftDetection:
    mode: disabled
EOF
if disabled_drift_detection_files "$TMP/withprovider" >/dev/null 2>&1; then rc=0; else rc=$?; fi
assert_true "examples/provider/* is excluded, mirroring UPTEST_MANIFESTS_ALL's own filter" "$rc"

echo "=== TestDisabledDriftDetectionFilesUnrelatedModeFieldIsIgnored ==="
mkdir -p "$TMP/unrelated/x"
cat > "$TMP/unrelated/x/x.yaml" <<'EOF'
spec:
  forProvider:
    mode: disabled
EOF
if disabled_drift_detection_files "$TMP/unrelated" >/dev/null 2>&1; then rc=0; else rc=$?; fi
assert_true "a same-named 'mode' field outside driftDetection is not flagged" "$rc"

echo "=== TestRepoExamplesHaveNoDisabledDriftDetection ==="
# The actual guardrail: this repo's real examples/ directory, today, must
# carry zero driftDetection.mode: disabled manifests. This is what makes a
# future regression fail `make reviewable` rather than only a code review.
if OUT="$(disabled_drift_detection_files "${REPO_ROOT}/examples" 2>&1)"; then RC=0; else RC=$?; fi
assert_true "repo examples/ carries no driftDetection.mode: disabled (offending: ${OUT:-none})" "$RC"

echo
echo "=== Summary: ${PASS} passed, ${FAIL} failed ==="
if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
exit 0
