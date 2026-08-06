#!/usr/bin/env bash
# drift-verify.sh — shared helpers for the driftDetection post-assert hooks
# (post-assert-record-a-drift-ignore.sh, post-assert-record-a-drift-warn.sh,
# post-assert-zone-auth-drift-ignore.sh).
#
# These hooks exist because the generic update-tester tool (run via
# test/hooks/run-update-tester.sh) only ever proves that a field DOES
# converge to a spec-supplied value. spec.driftDetection's job is the
# opposite for ignored paths: prove a field does NOT converge once its real
# owner sets it out of band. That needs a step no generic tool has — tamper
# the live Grid directly over WAPI, bypassing the controller entirely — so
# each resource gets its own small hook instead of a driven-by-annotation
# runner.
#
# Sourced with `set -euo pipefail` already in effect in the caller.

# drift_get prints a jsonpath value read from a live cluster object, or the
# empty string if the object or path does not resolve. Usage:
#   drift_get <resource.group> <name> <jsonpath> [kubectl-args...]
# kubectl-args is typically `-n default` for a namespaced resource, or
# nothing for cluster-scoped.
drift_get() {
    local resource="$1" name="$2" jsonpath="$3"
    shift 3
    "${KUBECTL:-kubectl}" get "$resource" "$name" "$@" -o jsonpath="{${jsonpath}}" 2>/dev/null || true
}

# drift_condition_reason / drift_condition_status read the named condition's
# .reason / .status off a live object.
drift_condition_reason() {
    local resource="$1" name="$2" type="$3"
    shift 3
    drift_get "$resource" "$name" ".status.conditions[?(@.type==\"${type}\")].reason" "$@"
}

drift_condition_status() {
    local resource="$1" name="$2" type="$3"
    shift 3
    drift_get "$resource" "$name" ".status.conditions[?(@.type==\"${type}\")].status" "$@"
}

drift_condition_message() {
    local resource="$1" name="$2" type="$3"
    shift 3
    drift_get "$resource" "$name" ".status.conditions[?(@.type==\"${type}\")].message" "$@"
}

# drift_wapi_creds reads host/username/password from the
# infobloxnios-credentials Secret that test/setup.sh creates in
# crossplane-system, and exports DRIFT_WAPI_HOST/DRIFT_WAPI_USER/
# DRIFT_WAPI_PASS. Reading from the Secret (rather than trusting
# INFOBLOX_HOST/INFOBLOX_USER/INFOBLOX_PASS to still be exported in the
# shell that invokes uptest) mirrors every other custom post-assert hook
# pattern in this fleet.
drift_wapi_creds() {
    DRIFT_WAPI_HOST="$("${KUBECTL:-kubectl}" -n crossplane-system get secret infobloxnios-credentials -o jsonpath='{.data.host}' | base64 -d)"
    DRIFT_WAPI_USER="$("${KUBECTL:-kubectl}" -n crossplane-system get secret infobloxnios-credentials -o jsonpath='{.data.username}' | base64 -d)"
    DRIFT_WAPI_PASS="$("${KUBECTL:-kubectl}" -n crossplane-system get secret infobloxnios-credentials -o jsonpath='{.data.password}' | base64 -d)"
    DRIFT_WAPI_BASE="https://${DRIFT_WAPI_HOST}/wapi/${INFOBLOX_WAPI_VERSION:-v2.13.1}"
}

# drift_wapi_get_json GETs a WAPI object by its _ref (the same string stored
# in the crossplane.io/external-name annotation) and prints the raw JSON
# body. Retries a bounded number of times on a transient 5xx, mirroring
# test/setup.sh's tolerance of a flaky Grid Manager response.
drift_wapi_get_json() {
    local ref="$1" tries=5 attempt status body
    for ((attempt = 1; attempt <= tries; attempt++)); do
        body="$(curl -sk -u "${DRIFT_WAPI_USER}:${DRIFT_WAPI_PASS}" -w $'\n%{http_code}' "${DRIFT_WAPI_BASE}/${ref}")"
        status="${body##*$'\n'}"
        body="${body%$'\n'*}"
        if [[ "$status" =~ ^2[0-9][0-9]$ ]]; then
            printf '%s' "$body"
            return 0
        fi
        if [[ "$status" =~ ^5[0-9][0-9]$ ]] && [ "$attempt" -lt "$tries" ]; then
            sleep 3
            continue
        fi
        echo "ERROR: drift_wapi_get_json: GET ${ref} returned HTTP ${status}: ${body}" >&2
        return 1
    done
}

# drift_wapi_get_field GETs a WAPI object by ref and extracts one field with
# jq (e.g. '.comment', '.ttl').
drift_wapi_get_field() {
    local ref="$1" field="$2"
    drift_wapi_get_json "$ref" | jq -r "$field"
}

# drift_wapi_put mutates a WAPI object directly, bypassing the controller —
# this is the "external owner" tamper step every scenario needs. Retries a
# bounded number of times on a transient 5xx.
drift_wapi_put() {
    local ref="$1" json_body="$2" tries=5 attempt status
    for ((attempt = 1; attempt <= tries; attempt++)); do
        status="$(curl -sk -o /dev/null -w '%{http_code}' \
            -X PUT -u "${DRIFT_WAPI_USER}:${DRIFT_WAPI_PASS}" -H 'Content-Type: application/json' \
            -d "$json_body" "${DRIFT_WAPI_BASE}/${ref}")"
        if [ "$status" = "200" ]; then
            return 0
        fi
        if [[ "$status" =~ ^5[0-9][0-9]$ ]] && [ "$attempt" -lt "$tries" ]; then
            sleep 3
            continue
        fi
        echo "ERROR: drift_wapi_put: PUT ${ref} returned HTTP ${status}" >&2
        return 1
    done
}

# drift_sleep_reconciles sleeps long enough to guarantee the controller has
# had the opportunity to run several reconciles, scaled off the E2E
# controlplane's own poll interval (Makefile E2E_POLL_INTERVAL, propagated
# as UPDATE_TESTER_POLL_INTERVAL — see Makefile's
# `export override UPDATE_TESTER_POLL_INTERVAL = $(E2E_POLL_INTERVAL)`).
drift_sleep_reconciles() {
    local cycles="${1:-4}"
    local poll="${UPDATE_TESTER_POLL_INTERVAL:-10s}"
    local secs="${poll%s}"
    [[ "$secs" =~ ^[0-9]+$ ]] || secs=10
    sleep $((secs * cycles))
}

# drift_wait_synced_true polls the Synced condition for up to 90s before a
# hook takes its final snapshot, tolerating a transient Grid Manager
# hiccup that flips Synced False for exactly one reconcile before the
# controller's own retry/backoff clears it. A resource that genuinely never
# re-syncs still fails whatever explicit Synced check the caller runs next.
drift_wait_synced_true() {
    local resource="$1" name="$2"
    shift 2
    local deadline=$((SECONDS + 90))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if [ "$(drift_condition_status "$resource" "$name" Synced "$@")" = "True" ]; then
            return 0
        fi
        sleep 5
    done
    return 1
}
