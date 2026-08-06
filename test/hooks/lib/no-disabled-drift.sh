#!/usr/bin/env bash
# no-disabled-drift.sh — guardrail: no example manifest under
# examples/ may set spec.driftDetection.mode to "disabled". A disabled
# example stops E2E from proving convergence, which is the one thing E2E
# exists to do. `warn` is permitted — it still detects drift and reports
# it, it just doesn't correct it.
#
# Sourced by no-disabled-drift-detection_test.sh, which both unit-tests the
# function below against synthetic fixtures and runs it against this repo's
# real examples/ directory as the guardrail itself.

set -euo pipefail

# _file_sets_drift_mode_disabled returns success (0) if $1 contains a
# driftDetection block whose mode is "disabled". Full-line comments are
# stripped first so a documentation example mentioning the string does not
# trip the check. The driftDetection block's own indentation is tracked so a
# `mode: disabled` belonging to some unrelated, more deeply nested field
# never matches.
_file_sets_drift_mode_disabled() {
    local file="$1"
    awk '
        /^[[:space:]]*#/ { next }
        {
            line = $0
            n = match(line, /[^ ]/)
            indent = (n > 0) ? n - 1 : length(line)
            if (line ~ /^[[:space:]]*driftDetection:[[:space:]]*$/) {
                indrift = 1
                driftindent = indent
                next
            }
            if (indrift) {
                if (line ~ /^[[:space:]]*$/) next
                if (indent <= driftindent) { indrift = 0; next }
                if (line ~ /^[[:space:]]*mode:[[:space:]]*disabled[[:space:]]*$/) { found = 1 }
            }
        }
        END { exit !found }
    ' "$file"
}

# disabled_drift_detection_files prints the path of every *.yaml file under
# $1 (excluding $1/provider/*, mirroring the Makefile's UPTEST_MANIFESTS_ALL
# exclusion of provider/config manifests) that sets driftDetection.mode to
# "disabled". Returns 0 (clean) if none are found, 1 otherwise.
disabled_drift_detection_files() {
    local dir="$1" f rc=0
    while IFS= read -r -d '' f; do
        if _file_sets_drift_mode_disabled "$f"; then
            echo "$f"
            rc=1
        fi
    done < <(find "$dir" -name '*.yaml' -not -path "${dir}/provider/*" -print0 | sort -z)
    return "$rc"
}
