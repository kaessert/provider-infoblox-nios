#!/usr/bin/env bash
# post-assert-range-template-linked.sh — proves status.atProvider.template
# mirrors spec.forProvider.template on a live Range whose template is set
# (examples/range/range-template-linked.yaml,
# examples/range/range-template-linked-namespaced.yaml).
#
# Why a dedicated hook rather than test/hooks/run-update-tester.sh: the
# generic tool proves a field converges to a value supplied through an
# UPDATE. template is create-only (UpdateNetworkRange does not accept it,
# and the WAPI never echoes it back on a GET), so there is nothing to
# update-test; the fact this hook exists to prove is that Observe mirrors
# spec.forProvider.template into status.atProvider.template at CREATE time
# and keeps mirroring it on every subsequent reconcile. No external
# probing is needed for that — the comparison is entirely between two
# fields of the same live Kubernetes object, so this hook only ever calls
# kubectl.
#
# Symlink naming convention (mirrors run-update-tester.sh's dispatch-by-$0
# style):
#   test/hooks/post-assert-range-template-linked.sh            -> examples/range/range-template-linked.yaml (cluster)
#   test/hooks/post-assert-range-template-linked-namespaced.sh -> examples/range/range-template-linked-namespaced.yaml
#
# Requires: kubectl.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./lib/drift-verify.sh
source "${SCRIPT_DIR}/lib/drift-verify.sh"

BASENAME="$(basename "$0" .sh)"
if [[ "$BASENAME" == *-namespaced ]]; then
    SCOPE="namespaced"
    NAME="example-range-template-linked-ns"
    RESOURCE="ranges.range.infobloxnios.m.crossplane.io"
    NS_ARGS=(-n default)
else
    SCOPE="cluster"
    NAME="example-range-template-linked"
    RESOURCE="ranges.range.infobloxnios.crossplane.io"
    NS_ARGS=()
fi

echo "==> post-assert-range-template-linked (${SCOPE}): reading Range/${NAME}"

if ! drift_wait_synced_true "$RESOURCE" "$NAME" "${NS_ARGS[@]}"; then
    echo "ERROR: post-assert-range-template-linked (${SCOPE}): Synced never reached True" >&2
    exit 1
fi

SPEC_TEMPLATE="$(drift_get "$RESOURCE" "$NAME" '.spec.forProvider.template' "${NS_ARGS[@]}")"
if [ -z "$SPEC_TEMPLATE" ]; then
    echo "ERROR: post-assert-range-template-linked (${SCOPE}): spec.forProvider.template is empty — the fixture itself is broken, not the mirror" >&2
    exit 1
fi

# status.atProvider is refreshed by the NEXT Observe after Create, so poll
# rather than sample once: a single read taken too early could still be
# looking at the pre-Create zero value.
ATPROVIDER_TEMPLATE="$(drift_wait_jsonpath_equals "$RESOURCE" "$NAME" '.status.atProvider.template' "$SPEC_TEMPLATE" 90 "${NS_ARGS[@]}")" || true

echo "==> post-assert-range-template-linked (${SCOPE}): spec.forProvider.template='${SPEC_TEMPLATE}' status.atProvider.template='${ATPROVIDER_TEMPLATE}'"

if [ "$ATPROVIDER_TEMPLATE" != "$SPEC_TEMPLATE" ]; then
    echo "ERROR: post-assert-range-template-linked (${SCOPE}): status.atProvider.template='${ATPROVIDER_TEMPLATE}', want '${SPEC_TEMPLATE}' — the create-time mirror did not populate" >&2
    exit 1
fi

echo "==> post-assert-range-template-linked (${SCOPE}): PASSED — status.atProvider.template mirrors spec.forProvider.template"
