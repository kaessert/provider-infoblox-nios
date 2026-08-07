#!/usr/bin/env bash
# post-assert-zone-auth-drift-ignore.sh — repeats
# post-assert-record-a-drift-ignore.sh's scenarios 1 ("ignore holds") and 2
# ("write path") against ZoneAuth (examples/zone-auth/zone-auth-drift-ignore.yaml),
# a resource of a different shape (a zone addressed by fqdn, not a DNS
# record addressed by (view, zone, name)) — confirming the drift ownership
# mechanism is not record-specific.
#
# `comment` is the ignored (externally-owned) field; `disable` is the
# sibling field patched afterwards to prove the write path. As with every
# resource on this provider, ZoneAuth updates via a whole-object PUT, so
# patching disable alone still sends comment on the same request — a
# compare-only ("skip the field") implementation would visibly revert the
# tampered comment the moment disable changes.
#
# Symlink naming convention (mirrors run-update-tester.sh's dispatch-by-$0
# style):
#   test/hooks/post-assert-zone-auth-drift-ignore.sh            -> examples/zone-auth/zone-auth-drift-ignore.yaml (cluster)
#   test/hooks/post-assert-zone-auth-drift-ignore-namespaced.sh -> examples/zone-auth/zone-auth-drift-ignore-namespaced.yaml
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
        echo "ERROR: post-assert-zone-auth-drift-ignore requires '$bin' on PATH" >&2
        exit 1
    fi
done

BASENAME="$(basename "$0" .sh)"
if [[ "$BASENAME" == *-namespaced ]]; then
    SCOPE="namespaced"
    NAME="example-zoneauth-drift-ignore-ns"
    RESOURCE="zoneauths.zoneauth.infobloxnios.m.crossplane.io"
    NS_ARGS=(-n default)
else
    SCOPE="cluster"
    NAME="example-zoneauth-drift-ignore"
    RESOURCE="zoneauths.zoneauth.infobloxnios.crossplane.io"
    NS_ARGS=()
fi

echo "==> post-assert-zone-auth-drift-ignore (${SCOPE}): reading ZoneAuth/${NAME}"

if ! drift_wait_synced_true "$RESOURCE" "$NAME" "${NS_ARGS[@]}"; then
    echo "ERROR: post-assert-zone-auth-drift-ignore (${SCOPE}): Synced never reached True" >&2
    exit 1
fi

drift_wapi_creds

REF="$(drift_get "$RESOURCE" "$NAME" '.metadata.annotations.crossplane\.io/external-name' "${NS_ARGS[@]}")"
SEED_COMMENT="$(drift_get "$RESOURCE" "$NAME" '.spec.forProvider.comment' "${NS_ARGS[@]}")"
if [ -z "$REF" ]; then
    echo "ERROR: post-assert-zone-auth-drift-ignore (${SCOPE}): missing external-name on the CR" >&2
    exit 1
fi

FAIL=0
TAMPERED_COMMENT="drift-owner-set-$(date +%s)"

# --- Step 1: confirm the seed reached the Grid. ---
API_COMMENT="$(drift_wapi_get_field "$REF" '.comment' 'comment,disable')"
echo "==> post-assert-zone-auth-drift-ignore (${SCOPE}): seed comment spec='${SEED_COMMENT}' wapi='${API_COMMENT}'"
if [ "$API_COMMENT" != "$SEED_COMMENT" ]; then
    echo "ERROR: post-assert-zone-auth-drift-ignore (${SCOPE}): seed comment never reached the Grid (spec='${SEED_COMMENT}', wapi='${API_COMMENT}')" >&2
    FAIL=1
fi

# --- Step 2: tamper the ignored field out of band, bypassing the controller. ---
echo "==> post-assert-zone-auth-drift-ignore (${SCOPE}): PUT ${REF} comment='${TAMPERED_COMMENT}' (out of band)"
drift_wapi_put "$REF" "{\"comment\":\"${TAMPERED_COMMENT}\"}"

# Poll the asserted value itself, not a proxy condition plus a fixed sleep:
# status.atProvider is refreshed by the NEXT Observe after the tamper lands
# on the Grid, and a single sample taken after a fixed sleep can catch the
# CR between the tamper landing and that Observe running (see
# drift_wait_jsonpath_equals doc comment).
IGNORE_HOLD_POLL_TIMEOUT=90
ATPROVIDER_COMMENT="$(drift_wait_jsonpath_equals "$RESOURCE" "$NAME" '.status.atProvider.comment' "$TAMPERED_COMMENT" "$IGNORE_HOLD_POLL_TIMEOUT" "${NS_ARGS[@]}")" || true

# --- Step 3 (scenario 1, "ignore holds"): confirm no revert. ---
SYNCED_STATUS="$(drift_condition_status "$RESOURCE" "$NAME" Synced "${NS_ARGS[@]}")"
DRIFT_REASON="$(drift_condition_reason "$RESOURCE" "$NAME" DriftDetected "${NS_ARGS[@]}")"
API_COMMENT_AFTER="$(drift_wapi_get_field "$REF" '.comment' 'comment,disable')"

echo "==> post-assert-zone-auth-drift-ignore (${SCOPE}): after tamper — atProvider.comment='${ATPROVIDER_COMMENT}' Synced='${SYNCED_STATUS}' DriftDetected.reason='${DRIFT_REASON}' wapi.comment='${API_COMMENT_AFTER}'"

if [ "$ATPROVIDER_COMMENT" != "$TAMPERED_COMMENT" ]; then
    echo "ERROR: post-assert-zone-auth-drift-ignore (${SCOPE}): status.atProvider.comment='${ATPROVIDER_COMMENT}', want the tampered value '${TAMPERED_COMMENT}' — the ignore did not hold" >&2
    FAIL=1
fi
if [ "$SYNCED_STATUS" != "True" ]; then
    echo "ERROR: post-assert-zone-auth-drift-ignore (${SCOPE}): Synced='${SYNCED_STATUS}', want True" >&2
    FAIL=1
fi
if [ "$DRIFT_REASON" != "DriftIgnored" ]; then
    echo "ERROR: post-assert-zone-auth-drift-ignore (${SCOPE}): DriftDetected.reason='${DRIFT_REASON}', want DriftIgnored" >&2
    FAIL=1
fi
if [ "$API_COMMENT_AFTER" != "$TAMPERED_COMMENT" ]; then
    echo "ERROR: post-assert-zone-auth-drift-ignore (${SCOPE}): live Grid comment reverted to '${API_COMMENT_AFTER}' — the controller wrote over the external owner's value before the write-path step even ran" >&2
    FAIL=1
fi

# --- Step 4 (scenario 2, "write path"): patch a sibling field via the MR
# spec and confirm it reconciles WITHOUT reverting the ignored one. ---
NEW_DISABLE=true
WRITE_PATH_POLL_TIMEOUT=120
echo "==> post-assert-zone-auth-drift-ignore (${SCOPE}): patching spec.forProvider.disable=${NEW_DISABLE} (a sibling field, not on the ignore list)"
"${KUBECTL}" patch "$RESOURCE" "$NAME" "${NS_ARGS[@]}" --type=merge -p "{\"spec\":{\"forProvider\":{\"disable\":${NEW_DISABLE}}}}"

# Poll the asserted value itself, not a proxy condition: Synced=True can be
# set right after a successful PUT while status.atProvider still holds the
# PRECEDING Observe's snapshot, one reconcile behind the write that just
# landed. Sampling once after a fixed sleep would read that stale snapshot
# on any run where the write happens to land near the end of the window
# (see drift_wait_jsonpath_equals doc comment).
ATPROVIDER_DISABLE="$(drift_wait_jsonpath_equals "$RESOURCE" "$NAME" '.status.atProvider.disable' "$NEW_DISABLE" "$WRITE_PATH_POLL_TIMEOUT" "${NS_ARGS[@]}")" || true
ATPROVIDER_COMMENT_FINAL="$(drift_get "$RESOURCE" "$NAME" '.status.atProvider.comment' "${NS_ARGS[@]}")"
API_DISABLE_FINAL="$(drift_wapi_get_field "$REF" '.disable' 'comment,disable')"
API_COMMENT_FINAL="$(drift_wapi_get_field "$REF" '.comment' 'comment,disable')"

echo "==> post-assert-zone-auth-drift-ignore (${SCOPE}): after write-path patch — atProvider.disable='${ATPROVIDER_DISABLE}' atProvider.comment='${ATPROVIDER_COMMENT_FINAL}' wapi.disable='${API_DISABLE_FINAL}' wapi.comment='${API_COMMENT_FINAL}'"

# The two failure modes are distinct and must be reported differently: a
# live Grid that never received the write is a real write-path defect; a
# live Grid that DID receive it, with only the status mirror still stale, is
# an observation-timing issue — the poll above already gave it
# WRITE_PATH_POLL_TIMEOUT seconds to catch up, so a mismatch surviving that
# is worth reporting on its own terms, not as "the write path is broken".
if [ "$API_DISABLE_FINAL" != "$NEW_DISABLE" ]; then
    echo "ERROR: post-assert-zone-auth-drift-ignore (${SCOPE}): backend never converged — disable never reached '${NEW_DISABLE}' on the live Grid (wapi='${API_DISABLE_FINAL}', atProvider='${ATPROVIDER_DISABLE}')" >&2
    FAIL=1
elif [ "$ATPROVIDER_DISABLE" != "$NEW_DISABLE" ]; then
    echo "ERROR: post-assert-zone-auth-drift-ignore (${SCOPE}): status mirror lagged — live Grid holds '${NEW_DISABLE}' but status.atProvider.disable still reads '${ATPROVIDER_DISABLE}' after ${WRITE_PATH_POLL_TIMEOUT}s of polling — an observation-timing issue, not a write-path failure" >&2
    FAIL=1
fi
if [ "$ATPROVIDER_COMMENT_FINAL" != "$TAMPERED_COMMENT" ]; then
    echo "ERROR: post-assert-zone-auth-drift-ignore (${SCOPE}): status.atProvider.comment='${ATPROVIDER_COMMENT_FINAL}' after the disable update — the ignored field was reverted by the sibling-field write" >&2
    FAIL=1
fi
if [ "$API_COMMENT_FINAL" != "$TAMPERED_COMMENT" ]; then
    echo "ERROR: post-assert-zone-auth-drift-ignore (${SCOPE}): live Grid comment='${API_COMMENT_FINAL}' after the disable update — the whole-object PUT reverted the external owner's value; this is the compare-only-ignore defect the design exists to prevent" >&2
    FAIL=1
fi

if [ "$FAIL" -ne 0 ]; then
    echo "==> post-assert-zone-auth-drift-ignore (${SCOPE}): FAILED" >&2
    exit 1
fi
echo "==> post-assert-zone-auth-drift-ignore (${SCOPE}): PASSED — disable reconciled to true and comment stayed tampered on both the CR and the live Grid"
