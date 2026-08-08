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

# drift_wapi_creds reads the Grid host from the cluster-scoped
# ProviderConfig/default (spec.host is a non-secret connection parameter,
# not a Secret key), and reads username/password from the
# infobloxnios-credentials Secret that test/setup.sh creates in
# crossplane-system. It exports DRIFT_WAPI_HOST/DRIFT_WAPI_USER/
# DRIFT_WAPI_PASS. Reading credentials from the Secret (rather than
# trusting INFOBLOX_USER/INFOBLOX_PASS to still be exported in the shell
# that invokes uptest) mirrors every other custom post-assert hook pattern
# in this fleet. test/setup.sh sets the same INFOBLOX_HOST on every
# ProviderConfig/ClusterProviderConfig it creates (cluster- and
# namespace-scoped alike), so the cluster-scoped ProviderConfig/default is
# a valid host source regardless of which scope's hook is calling.
drift_wapi_creds() {
    DRIFT_WAPI_HOST="$("${KUBECTL:-kubectl}" get providerconfig.infobloxnios.crossplane.io default -o jsonpath='{.spec.host}')"
    : "${DRIFT_WAPI_HOST:?drift_wapi_creds: ProviderConfig/default has no spec.host}"
    DRIFT_WAPI_USER="$("${KUBECTL:-kubectl}" -n crossplane-system get secret infobloxnios-credentials -o jsonpath='{.data.username}' | base64 -d)"
    DRIFT_WAPI_PASS="$("${KUBECTL:-kubectl}" -n crossplane-system get secret infobloxnios-credentials -o jsonpath='{.data.password}' | base64 -d)"
    DRIFT_WAPI_BASE="https://${DRIFT_WAPI_HOST}/wapi/${INFOBLOX_WAPI_VERSION:-v2.13.1}"
}

# drift_wapi_get_json GETs a WAPI object by its _ref (the same string stored
# in the crossplane.io/external-name annotation) and prints the raw JSON
# body. WAPI omits every field from a GET response unless it is explicitly
# named in _return_fields (confirmed elsewhere in this repo — see
# internal/controller/zonedelegated's controller_test.go and
# test/e2e/reap.sh's build_return_fields) — _ref itself is the only
# exception, always returned by default. $2, if given, is a comma-separated
# list of extra fields to request (e.g. "comment,ttl"). Retries a bounded
# number of times on a transient 5xx, mirroring test/setup.sh's tolerance of
# a flaky Grid Manager response.
drift_wapi_get_json() {
    local ref="$1" fields="${2:-}" tries=5 attempt status body url
    url="${DRIFT_WAPI_BASE}/${ref}"
    [ -n "$fields" ] && url="${url}?_return_fields=${fields}"
    for ((attempt = 1; attempt <= tries; attempt++)); do
        body="$(curl -sk -u "${DRIFT_WAPI_USER}:${DRIFT_WAPI_PASS}" -w $'\n%{http_code}' "$url")"
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

# drift_wapi_get_field GETs a WAPI object by ref, requesting $3 (comma-joined
# field list — pass at least the field(s) $2 reads, since WAPI omits
# anything not explicitly requested) and extracts one field with jq (e.g.
# '.comment', '.ttl').
drift_wapi_get_field() {
    local ref="$1" jq_filter="$2" fields="$3"
    drift_wapi_get_json "$ref" "$fields" | jq -r "$jq_filter"
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

# drift_wait_jsonpath_equals polls a jsonpath field on a live cluster object
# until it equals the wanted value or timeout_secs elapses, printing the
# last-seen value either way (success or timeout) so the caller can inspect
# what was actually observed. Deliberately bounded, mirroring
# drift_wait_synced_true: a field that never converges is a real failure and
# must still surface as one once timeout_secs is spent.
#
# Exists because a proxy condition (Synced=True) does not guarantee the
# SPECIFIC field a caller cares about has been re-observed by the reconcile
# that just landed: crossplane-runtime sets Synced=ReconcileSuccess right
# after a successful Update()/PUT, but the status.atProvider snapshot
# alongside it was captured by the PRECEDING Observe — so for one reconcile
# cycle, Synced can read True while the field of interest is still one write
# behind. Polling the actual value being asserted, rather than sampling it
# once after a fixed sleep, removes that race.
drift_wait_jsonpath_equals() {
    local resource="$1" name="$2" jsonpath="$3" want="$4" timeout="$5"
    shift 5
    local interval=5 waited=0 got
    while :; do
        got="$(drift_get "$resource" "$name" "$jsonpath" "$@")"
        if [ "$got" = "$want" ]; then
            echo "$got"
            return 0
        fi
        if [ "$waited" -ge "$timeout" ]; then
            echo "$got"
            return 1
        fi
        sleep "$interval"
        waited=$((waited + interval))
    done
}
