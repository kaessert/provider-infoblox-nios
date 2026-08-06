#!/usr/bin/env bash
# post-assert-record-a-drift-ignore.sh — proves spec.driftDetection's ignore
# semantics (scenario 1, "ignore holds") and the write path (scenario 2,
# "an ignored field survives a sibling field's update") end to end against
# the live NIOS Grid for ARecord
# (examples/record-a/record-a-drift-ignore.yaml). The cluster-scoped
# invocation additionally proves scenario 4 ("rejection"): an MR naming an
# identity field (forProvider.name, forProvider.view) under
# driftDetection.ignore must be refused outright, with no reconcile and no
# WAPI write.
#
# Why a dedicated hook rather than test/hooks/run-update-tester.sh: the
# generic tool proves a field DOES converge to a spec value. Scenario 1
# proves the opposite for an ignored path — that once its real owner sets
# the field on the live Grid, the controller must NOT write it back — which
# needs a step the generic tool has no concept of: tampering the backend
# directly over WAPI, bypassing the controller. Scenario 2 then proves a
# sibling field's whole-object PUT does not revert that tamper — this is the
# assertion the whole ownership design turns on.
#
# Symlink naming convention (mirrors run-update-tester.sh's dispatch-by-$0
# style):
#   test/hooks/post-assert-record-a-drift-ignore.sh            -> examples/record-a/record-a-drift-ignore.yaml (cluster)
#   test/hooks/post-assert-record-a-drift-ignore-namespaced.sh -> examples/record-a/record-a-drift-ignore-namespaced.yaml
#
# Requires: kubectl, curl, jq. Reads the WAPI credentials from the
# infobloxnios-credentials Secret in crossplane-system (see
# test/hooks/lib/drift-verify.sh).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./lib/drift-verify.sh
source "${SCRIPT_DIR}/lib/drift-verify.sh"

KUBECTL="${KUBECTL:-kubectl}"

for bin in curl jq; do
    if ! command -v "$bin" >/dev/null 2>&1; then
        echo "ERROR: post-assert-record-a-drift-ignore requires '$bin' on PATH" >&2
        exit 1
    fi
done

BASENAME="$(basename "$0" .sh)"
if [[ "$BASENAME" == *-namespaced ]]; then
    SCOPE="namespaced"
    NAME="example-arecord-drift-ignore-ns"
    RESOURCE="arecords.recorda.infobloxnios.m.crossplane.io"
    NS_ARGS=(-n default)
else
    SCOPE="cluster"
    NAME="example-arecord-drift-ignore"
    RESOURCE="arecords.recorda.infobloxnios.crossplane.io"
    NS_ARGS=()
fi

echo "==> post-assert-record-a-drift-ignore (${SCOPE}): reading ARecord/${NAME}"

if ! drift_wait_synced_true "$RESOURCE" "$NAME" "${NS_ARGS[@]}"; then
    echo "ERROR: post-assert-record-a-drift-ignore (${SCOPE}): Synced never reached True" >&2
    exit 1
fi

drift_wapi_creds

REF="$(drift_get "$RESOURCE" "$NAME" '.metadata.annotations.crossplane\.io/external-name' "${NS_ARGS[@]}")"
SEED_COMMENT="$(drift_get "$RESOURCE" "$NAME" '.spec.forProvider.comment' "${NS_ARGS[@]}")"
if [ -z "$REF" ]; then
    echo "ERROR: post-assert-record-a-drift-ignore (${SCOPE}): missing external-name on the CR" >&2
    exit 1
fi

FAIL=0
TAMPERED_COMMENT="drift-owner-set-$(date +%s)"

# --- Step 1: confirm the seed reached the Grid. ---
API_COMMENT="$(drift_wapi_get_field "$REF" '.comment')"
echo "==> post-assert-record-a-drift-ignore (${SCOPE}): seed comment spec='${SEED_COMMENT}' wapi='${API_COMMENT}'"
if [ "$API_COMMENT" != "$SEED_COMMENT" ]; then
    echo "ERROR: post-assert-record-a-drift-ignore (${SCOPE}): seed comment never reached the Grid (spec='${SEED_COMMENT}', wapi='${API_COMMENT}')" >&2
    FAIL=1
fi

# --- Step 2: tamper the ignored field out of band, bypassing the controller. ---
echo "==> post-assert-record-a-drift-ignore (${SCOPE}): PUT ${REF} comment='${TAMPERED_COMMENT}' (out of band)"
drift_wapi_put "$REF" "{\"comment\":\"${TAMPERED_COMMENT}\"}"

drift_sleep_reconciles 4

# --- Step 3 (scenario 1, "ignore holds"): confirm no revert. ---
ATPROVIDER_COMMENT="$(drift_get "$RESOURCE" "$NAME" '.status.atProvider.comment' "${NS_ARGS[@]}")"
SYNCED_STATUS="$(drift_condition_status "$RESOURCE" "$NAME" Synced "${NS_ARGS[@]}")"
DRIFT_REASON="$(drift_condition_reason "$RESOURCE" "$NAME" DriftDetected "${NS_ARGS[@]}")"
API_COMMENT_AFTER="$(drift_wapi_get_field "$REF" '.comment')"

echo "==> post-assert-record-a-drift-ignore (${SCOPE}): after tamper — atProvider.comment='${ATPROVIDER_COMMENT}' Synced='${SYNCED_STATUS}' DriftDetected.reason='${DRIFT_REASON}' wapi.comment='${API_COMMENT_AFTER}'"

if [ "$ATPROVIDER_COMMENT" != "$TAMPERED_COMMENT" ]; then
    echo "ERROR: post-assert-record-a-drift-ignore (${SCOPE}): status.atProvider.comment='${ATPROVIDER_COMMENT}', want the tampered value '${TAMPERED_COMMENT}' — the ignore did not hold" >&2
    FAIL=1
fi
if [ "$SYNCED_STATUS" != "True" ]; then
    echo "ERROR: post-assert-record-a-drift-ignore (${SCOPE}): Synced='${SYNCED_STATUS}', want True" >&2
    FAIL=1
fi
if [ "$DRIFT_REASON" != "DriftIgnored" ]; then
    echo "ERROR: post-assert-record-a-drift-ignore (${SCOPE}): DriftDetected.reason='${DRIFT_REASON}', want DriftIgnored" >&2
    FAIL=1
fi
if [ "$API_COMMENT_AFTER" != "$TAMPERED_COMMENT" ]; then
    echo "ERROR: post-assert-record-a-drift-ignore (${SCOPE}): live Grid comment reverted to '${API_COMMENT_AFTER}' — the controller wrote over the external owner's value before the write-path step even ran" >&2
    FAIL=1
fi

# --- Step 4 (scenario 2, "write path", THE load-bearing assertion): patch a
# sibling field via the MR spec and confirm it reconciles WITHOUT reverting
# the ignored one. Every update on this provider is a whole-object PUT
# (connector.go's toMethod maps UPDATE -> PUT unconditionally), so a build
# that skips substitution here sends the STALE spec comment and silently
# reverts the tamper. ---
echo "==> post-assert-record-a-drift-ignore (${SCOPE}): patching spec.forProvider.ttl=3600 (a sibling field, not on the ignore list)"
"${KUBECTL}" patch "$RESOURCE" "$NAME" "${NS_ARGS[@]}" --type=merge -p '{"spec":{"forProvider":{"ttl":3600}}}'

drift_sleep_reconciles 6

ATPROVIDER_TTL="$(drift_get "$RESOURCE" "$NAME" '.status.atProvider.ttl' "${NS_ARGS[@]}")"
ATPROVIDER_COMMENT_FINAL="$(drift_get "$RESOURCE" "$NAME" '.status.atProvider.comment' "${NS_ARGS[@]}")"
API_TTL_FINAL="$(drift_wapi_get_field "$REF" '.ttl')"
API_COMMENT_FINAL="$(drift_wapi_get_field "$REF" '.comment')"

echo "==> post-assert-record-a-drift-ignore (${SCOPE}): after write-path patch — atProvider.ttl='${ATPROVIDER_TTL}' atProvider.comment='${ATPROVIDER_COMMENT_FINAL}' wapi.ttl='${API_TTL_FINAL}' wapi.comment='${API_COMMENT_FINAL}'"

if [ "$ATPROVIDER_TTL" != "3600" ] || [ "$API_TTL_FINAL" != "3600" ]; then
    echo "ERROR: post-assert-record-a-drift-ignore (${SCOPE}): ttl did not reconcile to 3600 (atProvider='${ATPROVIDER_TTL}', wapi='${API_TTL_FINAL}') — the sibling-field write path itself is broken" >&2
    FAIL=1
fi
if [ "$ATPROVIDER_COMMENT_FINAL" != "$TAMPERED_COMMENT" ]; then
    echo "ERROR: post-assert-record-a-drift-ignore (${SCOPE}): status.atProvider.comment='${ATPROVIDER_COMMENT_FINAL}' after the ttl update — the ignored field was reverted by the sibling-field write" >&2
    FAIL=1
fi
if [ "$API_COMMENT_FINAL" != "$TAMPERED_COMMENT" ]; then
    echo "ERROR: post-assert-record-a-drift-ignore (${SCOPE}): live Grid comment='${API_COMMENT_FINAL}' after the ttl update — the whole-object PUT reverted the external owner's value; this is the compare-only-ignore defect the design exists to prevent" >&2
    FAIL=1
fi

if [ "$FAIL" -ne 0 ]; then
    echo "==> post-assert-record-a-drift-ignore (${SCOPE}): FAILED" >&2
    exit 1
fi
echo "==> post-assert-record-a-drift-ignore (${SCOPE}): PASSED — ttl reconciled to 3600 and comment stayed tampered on both the CR and the live Grid"

# --- Scenario 4 ("rejection"), run once from the cluster-scoped invocation
# only — the eligibility gate has no scope-dependent behaviour, so repeating
# it per scope would only create redundant throwaway objects. An MR naming
# an identity field (forProvider.name, forProvider.view) under
# driftDetection.ignore must be refused outright: a hard error surfaced on
# the MR, no reconcile, no WAPI write. ---
if [ "$SCOPE" = "cluster" ]; then
    echo "==> post-assert-record-a-drift-ignore: scenario 4 — confirming ineligible ignore paths are rejected"

    for FIELD in name view; do
        REJECT_NAME="example-arecord-drift-reject-${FIELD}"
        echo "==> post-assert-record-a-drift-ignore: applying an ARecord with forProvider.${FIELD} under ignore (expected to be refused)"
        "${KUBECTL}" apply -f - <<EOF
apiVersion: recorda.infobloxnios.crossplane.io/v1alpha1
kind: ARecord
metadata:
  name: ${REJECT_NAME}
spec:
  driftDetection:
    ignore:
      - paths:
          - forProvider.${FIELD}
  forProvider:
    name: reject-${FIELD}.example.com
    ipv4Addr: 192.0.2.99
    view: default
    comment: should never reconcile
  providerConfigRef:
    name: default
EOF

        REJECTED=0
        SYNCED=""
        MSG=""
        for _ in $(seq 1 12); do
            SYNCED="$(drift_condition_status "$RESOURCE" "$REJECT_NAME" Synced)"
            MSG="$(drift_condition_message "$RESOURCE" "$REJECT_NAME" Synced)"
            if [ "$SYNCED" = "False" ] && [[ "$MSG" == *ineligible* ]]; then
                REJECTED=1
                break
            fi
            sleep 5
        done

        EXTNAME="$(drift_get "$RESOURCE" "$REJECT_NAME" '.metadata.annotations.crossplane\.io/external-name')"

        if [ "$REJECTED" -ne 1 ]; then
            echo "ERROR: post-assert-record-a-drift-ignore: forProvider.${FIELD} under ignore was NOT refused — Synced='${SYNCED}' message='${MSG}'" >&2
            FAIL=1
        else
            echo "==> post-assert-record-a-drift-ignore: forProvider.${FIELD} correctly refused — Synced='${SYNCED}' message='${MSG}'"
        fi
        if [ -n "$EXTNAME" ]; then
            echo "ERROR: post-assert-record-a-drift-ignore: forProvider.${FIELD}: external-name='${EXTNAME}' was set — a WAPI object was created despite the ineligible ignore path" >&2
            FAIL=1
        fi

        "${KUBECTL}" delete "$RESOURCE" "$REJECT_NAME" --wait=true --timeout=60s 2>/dev/null || true
    done

    if [ "$FAIL" -ne 0 ]; then
        echo "==> post-assert-record-a-drift-ignore: scenario 4 FAILED" >&2
        exit 1
    fi
    echo "==> post-assert-record-a-drift-ignore: scenario 4 PASSED — forProvider.name and forProvider.view are both refused, no WAPI write"
fi
