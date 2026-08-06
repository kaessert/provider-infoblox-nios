#!/usr/bin/env bash
# post-assert-record-a-drift-warn.sh — proves spec.driftDetection's warn
# mode end to end against the live NIOS Grid (scenario 3): tamper any field
# out of band and confirm the provider reports drift (DriftDetected
# condition, reason Drifted) WITHOUT correcting it.
#
# Why a dedicated hook: see post-assert-record-a-drift-ignore.sh — the
# generic update-tester tool proves fields DO converge; warn mode proves the
# opposite. Deliberately no `ignore` list on this manifest — warn suppresses
# correction for ANY detected drift, not just listed paths (see
# internal/driftdetection's Observe, case ModeWarn), so tampering `comment`
# is a sufficient probe.
#
# Symlink naming convention (mirrors run-update-tester.sh's dispatch-by-$0
# style):
#   test/hooks/post-assert-record-a-drift-warn.sh            -> examples/record-a/record-a-drift-warn.yaml (cluster)
#   test/hooks/post-assert-record-a-drift-warn-namespaced.sh -> examples/record-a/record-a-drift-warn-namespaced.yaml
#
# Requires: kubectl, curl, jq. Reads the WAPI credentials from the
# infobloxnios-credentials Secret in crossplane-system (see
# test/hooks/lib/drift-verify.sh).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./lib/drift-verify.sh
source "${SCRIPT_DIR}/lib/drift-verify.sh"

for bin in curl jq; do
    if ! command -v "$bin" >/dev/null 2>&1; then
        echo "ERROR: post-assert-record-a-drift-warn requires '$bin' on PATH" >&2
        exit 1
    fi
done

BASENAME="$(basename "$0" .sh)"
if [[ "$BASENAME" == *-namespaced ]]; then
    SCOPE="namespaced"
    NAME="example-arecord-drift-warn-ns"
    RESOURCE="arecords.recorda.infobloxnios.m.crossplane.io"
    NS_ARGS=(-n default)
else
    SCOPE="cluster"
    NAME="example-arecord-drift-warn"
    RESOURCE="arecords.recorda.infobloxnios.crossplane.io"
    NS_ARGS=()
fi

echo "==> post-assert-record-a-drift-warn (${SCOPE}): reading ARecord/${NAME}"

if ! drift_wait_synced_true "$RESOURCE" "$NAME" "${NS_ARGS[@]}"; then
    echo "ERROR: post-assert-record-a-drift-warn (${SCOPE}): Synced never reached True" >&2
    exit 1
fi

drift_wapi_creds

REF="$(drift_get "$RESOURCE" "$NAME" '.metadata.annotations.crossplane\.io/external-name' "${NS_ARGS[@]}")"
SEED_COMMENT="$(drift_get "$RESOURCE" "$NAME" '.spec.forProvider.comment' "${NS_ARGS[@]}")"
if [ -z "$REF" ]; then
    echo "ERROR: post-assert-record-a-drift-warn (${SCOPE}): missing external-name on the CR" >&2
    exit 1
fi

FAIL=0
TAMPERED_COMMENT="drift-owner-set-$(date +%s)"

API_COMMENT="$(drift_wapi_get_field "$REF" '.comment' 'comment')"
echo "==> post-assert-record-a-drift-warn (${SCOPE}): seed comment spec='${SEED_COMMENT}' wapi='${API_COMMENT}'"
if [ "$API_COMMENT" != "$SEED_COMMENT" ]; then
    echo "ERROR: post-assert-record-a-drift-warn (${SCOPE}): seed comment never reached the Grid (spec='${SEED_COMMENT}', wapi='${API_COMMENT}')" >&2
    FAIL=1
fi

echo "==> post-assert-record-a-drift-warn (${SCOPE}): PUT ${REF} comment='${TAMPERED_COMMENT}' (out of band)"
drift_wapi_put "$REF" "{\"comment\":\"${TAMPERED_COMMENT}\"}"

drift_sleep_reconciles 4

ATPROVIDER_COMMENT="$(drift_get "$RESOURCE" "$NAME" '.status.atProvider.comment' "${NS_ARGS[@]}")"
SYNCED_STATUS="$(drift_condition_status "$RESOURCE" "$NAME" Synced "${NS_ARGS[@]}")"
DRIFT_REASON="$(drift_condition_reason "$RESOURCE" "$NAME" DriftDetected "${NS_ARGS[@]}")"
API_COMMENT_AFTER="$(drift_wapi_get_field "$REF" '.comment' 'comment')"

echo "==> post-assert-record-a-drift-warn (${SCOPE}): after tamper — atProvider.comment='${ATPROVIDER_COMMENT}' Synced='${SYNCED_STATUS}' DriftDetected.reason='${DRIFT_REASON}' wapi.comment='${API_COMMENT_AFTER}'"

# warn mode: no correction (the tampered value stands on both the CR and
# the live Grid), Synced stays True (warn forces ResourceUpToDate=true so
# the reconciler never treats this as needing a write), and the
# DriftDetected condition reports Drifted (detected, not corrected).
if [ "$ATPROVIDER_COMMENT" != "$TAMPERED_COMMENT" ]; then
    echo "ERROR: post-assert-record-a-drift-warn (${SCOPE}): status.atProvider.comment='${ATPROVIDER_COMMENT}', want the tampered value '${TAMPERED_COMMENT}' — warn mode corrected drift it should only report" >&2
    FAIL=1
fi
if [ "$SYNCED_STATUS" != "True" ]; then
    echo "ERROR: post-assert-record-a-drift-warn (${SCOPE}): Synced='${SYNCED_STATUS}', want True" >&2
    FAIL=1
fi
if [ "$DRIFT_REASON" != "Drifted" ]; then
    echo "ERROR: post-assert-record-a-drift-warn (${SCOPE}): DriftDetected.reason='${DRIFT_REASON}', want Drifted" >&2
    FAIL=1
fi
if [ "$API_COMMENT_AFTER" != "$TAMPERED_COMMENT" ]; then
    echo "ERROR: post-assert-record-a-drift-warn (${SCOPE}): live Grid comment reverted to '${API_COMMENT_AFTER}' — warn mode corrected drift it should only report" >&2
    FAIL=1
fi

if [ "$FAIL" -ne 0 ]; then
    echo "==> post-assert-record-a-drift-warn (${SCOPE}): FAILED" >&2
    exit 1
fi
echo "==> post-assert-record-a-drift-warn (${SCOPE}): PASSED — drift reported (reason Drifted) and left uncorrected on both the CR and the live Grid"
