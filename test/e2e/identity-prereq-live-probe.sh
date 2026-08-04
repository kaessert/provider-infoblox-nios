#!/usr/bin/env bash
# test/e2e/identity-prereq-live-probe.sh — Live verification harness for the
# identity extensible-attribute install prerequisite (see the "Identity
# resolution" doc comment in internal/clients/identity/probe.go for the
# mechanism this exercises).
#
# The provider stamps a "Crossplane Internal ID" extensible attribute onto
# every NIOS object it manages, and needs that attribute's *definition* to
# exist on the Grid before it can do so. This script proves, against a real
# Grid, the four properties that follow from that:
#
#   1. A superuser credential auto-creates the definition on first use, and
#      the created object is stamped and searchable by it.
#   2. Once the definition disappears, a resource that must re-locate its
#      object by searching the identity attribute (a stale/lost reference)
#      is refused with an operator-facing remediation, not a raw WAPI error.
#   3. A non-superuser credential cannot create the definition, so a brand
#      new object is refused before any mutating call reaches the Grid.
#   4. Recreating the definition clears the refusal on the next reconcile,
#      within the provider's cached-verdict TTL, with no pod restart.
#
# ── Isolation: a scratch attribute name, not the real one ─────────────────
#
# internal/clients/identity.EAKey is a single Go constant consumed by every
# identity-aware controller. This script builds the provider with that one
# constant substituted for a per-run scratch name, so the whole scenario
# plays out against an attribute definition nothing else on the Grid
# references — the real "Crossplane Internal ID" definition that every
# other managed object depends on is never touched, deleted, or raced
# against. The substitution is made with `sed` in a scratch worktree, never
# committed, and reverted immediately after use.
#
# ── What this script does NOT do ───────────────────────────────────────────
#
# It does not run through `make e2e.<resource>` — that target's lifecycle
# (build, tear the cluster down, bring a fresh one up, apply/assert/delete,
# leave) does not fit a scenario that needs to delete-and-recreate a Grid
# object out of band midway through a run and then keep asserting against
# the same live cluster. This script drives the same underlying make
# targets (`build`, `controlplane.up`, `local-deploy`, `controlplane.down`)
# directly, in the order the scenario needs.
#
# Usage:
#   INFOBLOX_HOST=... INFOBLOX_USER=... INFOBLOX_PASS=... \
#   INFOBLOX_E2E_RESTRICTED_USER=... INFOBLOX_E2E_RESTRICTED_PASS=... \
#   test/e2e/identity-prereq-live-probe.sh
#
# Requires: the usual E2E toolchain (kind, kubectl, helm — see
# build/makelib/k8s_tools.mk, which downloads them on demand), a superuser
# and a non-superuser WAPI credential (see the credentials section of the
# provider documentation), and a kind cluster name exported as
# KIND_CLUSTER_NAME (falls back to a script-generated one). Run from the
# provider module root.
#
# Prints progress to stdout and leaves a non-zero exit status (with the
# cluster and any scratch Grid objects already cleaned up) if any assertion
# fails. Every scratch object this script creates carries the run token in
# its name, so a failed run's leftovers are trivially distinguishable from
# anything else on the Grid.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

: "${INFOBLOX_HOST:?set INFOBLOX_HOST (superuser Grid credential)}"
: "${INFOBLOX_USER:?set INFOBLOX_USER (superuser Grid credential)}"
: "${INFOBLOX_PASS:?set INFOBLOX_PASS (superuser Grid credential)}"
: "${INFOBLOX_E2E_RESTRICTED_USER:?set INFOBLOX_E2E_RESTRICTED_USER (non-superuser Grid credential)}"
: "${INFOBLOX_E2E_RESTRICTED_PASS:?set INFOBLOX_E2E_RESTRICTED_PASS (non-superuser Grid credential)}"

KUBECTL="${KUBECTL:-kubectl}"
WAPI_VERSION="${INFOBLOX_WAPI_VERSION:-v2.12}"
WAPI_BASE="https://${INFOBLOX_HOST}/wapi/${WAPI_VERSION}"
RUN_TOKEN="$(date -u +%Y%m%d%H%M%S)"
SCRATCH_KEY="Crossplane Internal ID E2E ${RUN_TOKEN}"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-identity-prereq-probe-${RUN_TOKEN}}"
export KUBECONFIG="${KUBECONFIG:-${ROOT_DIR}/identity-prereq-probe.kubeconfig}"

log() { echo "[identity-prereq-probe] $*"; }

curl_wapi() {
  # curl_wapi <user> <pass> <method> <path> [json-body]
  local user="$1" pass="$2" method="$3" path="$4" body="${5:-}"
  if [ -n "${body}" ]; then
    curl -sk -u "${user}:${pass}" -X "${method}" -H "Content-Type: application/json" \
      -d "${body}" "${WAPI_BASE}/${path}"
  else
    curl -sk -u "${user}:${pass}" -X "${method}" "${WAPI_BASE}/${path}"
  fi
}

cleanup() {
  local status=$?
  log "cleanup: removing scratch managed resources (if any remain)..."
  ${KUBECTL} delete arecord.recorda.infobloxnios.crossplane.io \
    -l "identity-prereq-probe/run=${RUN_TOKEN}" --wait=true --timeout=120s 2>/dev/null || true
  ${KUBECTL} delete arecord.recorda.infobloxnios.m.crossplane.io -n default \
    -l "identity-prereq-probe/run=${RUN_TOKEN}" --wait=true --timeout=120s 2>/dev/null || true

  log "cleanup: removing the scratch extensible attribute definition (if it still exists)..."
  local ref
  ref="$(curl_wapi "${INFOBLOX_USER}" "${INFOBLOX_PASS}" GET \
    "extensibleattributedef?name=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "${SCRATCH_KEY}")" \
    | python3 -c 'import json,sys
d=json.load(sys.stdin)
print(d[0]["_ref"] if d else "")' 2>/dev/null || true)"
  if [ -n "${ref}" ]; then
    # Two-call teardown: clear the read-only flag before deleting, since a
    # definition this provider creates is always R-flagged.
    curl_wapi "${INFOBLOX_USER}" "${INFOBLOX_PASS}" PUT "${ref}" '{"flags":"C"}' >/dev/null || true
    curl_wapi "${INFOBLOX_USER}" "${INFOBLOX_PASS}" DELETE "${ref}" >/dev/null || true
    log "cleanup: deleted scratch definition ${SCRATCH_KEY}"
  fi

  log "cleanup: tearing down the kind cluster..."
  make controlplane.down KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME}" >/dev/null 2>&1 || true

  if [ "${status}" -eq 0 ]; then
    log "done — all scenarios passed, Grid and cluster clean"
  else
    log "FAILED (exit ${status}) — Grid and cluster cleaned up regardless"
  fi
  exit "${status}"
}
trap cleanup EXIT

log "run token: ${RUN_TOKEN}"
log "scratch attribute name: ${SCRATCH_KEY}"

# ── 1. Substitute the identity attribute key, build, never commit it ──────
IDENTITY_GO="internal/clients/identity/identity.go"
if [ -f "${IDENTITY_GO}.orig" ]; then
  echo "FATAL: ${IDENTITY_GO}.orig already exists — a previous run of this" >&2
  echo "  script did not clean up. Restore it by hand before re-running:" >&2
  echo "  mv ${IDENTITY_GO}.orig ${IDENTITY_GO}" >&2
  exit 1
fi
cp "${IDENTITY_GO}" "${IDENTITY_GO}.orig"
restore_identity_go() { mv -f "${IDENTITY_GO}.orig" "${IDENTITY_GO}" 2>/dev/null || true; }
trap 'restore_identity_go; cleanup' EXIT

sed -i "s/^const EAKey = \"Crossplane Internal ID\"\$/const EAKey = \"${SCRATCH_KEY}\"/" "${IDENTITY_GO}"
if ! grep -q "const EAKey = \"${SCRATCH_KEY}\"" "${IDENTITY_GO}"; then
  echo "FATAL: EAKey substitution did not match — identity.go may have changed shape" >&2
  exit 1
fi
log "one-line substitution applied (not committed):"
git diff --stat -- "${IDENTITY_GO}"

log "building provider image with the scratch identity key..."
make build KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME}"

# ── 2. Bring up a scratch cluster and deploy the substituted build ────────
log "starting kind cluster ${KIND_CLUSTER_NAME}..."
make controlplane.up KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME}"
log "deploying provider package..."
make local-deploy KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME}"

PROVIDER_POD=""
for _ in $(seq 1 30); do
  PROVIDER_POD="$(${KUBECTL} -n crossplane-system get pods -o name 2>/dev/null | grep provider-infobloxnios | head -1 || true)"
  if [ -n "${PROVIDER_POD}" ] && ${KUBECTL} -n crossplane-system wait "${PROVIDER_POD}" --for=condition=Ready --timeout=10s >/dev/null 2>&1; then
    break
  fi
  sleep 10
done
[ -n "${PROVIDER_POD}" ] || { echo "FATAL: provider pod never became Ready" >&2; exit 1; }
log "provider pod ready: ${PROVIDER_POD}"
POD_START="$(${KUBECTL} -n crossplane-system get "${PROVIDER_POD}" -o jsonpath='{.status.startTime}')"

log "running test/setup.sh (superuser ProviderConfig + Grid prerequisites)..."
INFOBLOX_HOST="${INFOBLOX_HOST}" INFOBLOX_USER="${INFOBLOX_USER}" INFOBLOX_PASS="${INFOBLOX_PASS}" \
  bash test/setup.sh

# Confirm the scratch definition is genuinely absent before scenario 1 —
# this scenario's whole point depends on it never having existed.
existing="$(curl_wapi "${INFOBLOX_USER}" "${INFOBLOX_PASS}" GET \
  "extensibleattributedef?name=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "${SCRATCH_KEY}")")"
[ "${existing}" = "[]" ] || { echo "FATAL: scratch definition already exists — pick a different run token" >&2; exit 1; }

apply_arecord() {
  # apply_arecord <name> <ipv4> <scope: cluster|namespaced>
  local name="$1" ip="$2" scope="$3"
  if [ "${scope}" = "cluster" ]; then
    cat <<YAML | ${KUBECTL} apply -f -
apiVersion: recorda.infobloxnios.crossplane.io/v1alpha1
kind: ARecord
metadata:
  name: ${name}
  labels:
    identity-prereq-probe/run: "${RUN_TOKEN}"
spec:
  forProvider:
    name: ${name}.example.com
    ipv4Addr: ${ip}
    view: default
    comment: "identity-prereq-live-probe scratch object"
  providerConfigRef:
    name: default
YAML
  else
    cat <<YAML | ${KUBECTL} apply -f -
apiVersion: recorda.infobloxnios.m.crossplane.io/v1alpha1
kind: ARecord
metadata:
  name: ${name}
  namespace: default
  labels:
    identity-prereq-probe/run: "${RUN_TOKEN}"
spec:
  forProvider:
    name: ${name}.example.com
    ipv4Addr: ${ip}
    view: default
    comment: "identity-prereq-live-probe scratch object (namespaced)"
  providerConfigRef:
    kind: ClusterProviderConfig
    name: default
YAML
  fi
}

wait_synced() {
  # wait_synced <kind.group> <name> <scope: cluster|namespaced> <want True|False> <timeout-seconds>
  local res="$1" name="$2" scope="$3" want="$4" timeout="${5:-90}" ns_args=()
  [ "${scope}" = "namespaced" ] && ns_args=(-n default)
  for _ in $(seq 1 "$((timeout / 3))"); do
    got="$(${KUBECTL} "${ns_args[@]}" get "${res}/${name}" -o jsonpath='{.status.conditions[?(@.type=="Synced")].status}' 2>/dev/null || true)"
    [ "${got}" = "${want}" ] && return 0
    sleep 3
  done
  echo "FATAL: ${res}/${name} never reached Synced=${want} within ${timeout}s (last seen: ${got:-<none>})" >&2
  return 1
}

# ── Scenario 1: superuser auto-create ──────────────────────────────────────
log "=== scenario 1: superuser auto-creates the scratch definition ==="
apply_arecord "idp-s1-${RUN_TOKEN}" "192.0.2.150" cluster
apply_arecord "idp-s1-ns-${RUN_TOKEN}" "192.0.2.151" namespaced
wait_synced arecord.recorda.infobloxnios.crossplane.io "idp-s1-${RUN_TOKEN}" cluster True
wait_synced arecord.recorda.infobloxnios.m.crossplane.io "idp-s1-ns-${RUN_TOKEN}" namespaced True

def_after_create="$(curl_wapi "${INFOBLOX_USER}" "${INFOBLOX_PASS}" GET \
  "extensibleattributedef?name=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "${SCRATCH_KEY}")&_return_fields=name,type,flags")"
echo "${def_after_create}" | grep -q '"flags": *"CR"' || { echo "FATAL: scratch definition missing or not CR after scenario 1" >&2; exit 1; }
log "scenario 1 PASSED — definition auto-created with flags CR:"
echo "${def_after_create}"

log "=== scoped-refusal property: startup and unrelated controllers are unaffected ==="
log "provider pod restarts so far: $(${KUBECTL} -n crossplane-system get "${PROVIDER_POD}" -o jsonpath='{.status.containerStatuses[0].restartCount}')"

# ── Delete the scratch definition out of band (two-call: clear R, then DELETE) ──
log "deleting the scratch definition out of band..."
ref="$(curl_wapi "${INFOBLOX_USER}" "${INFOBLOX_PASS}" GET \
  "extensibleattributedef?name=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "${SCRATCH_KEY}")" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["_ref"])')"
curl_wapi "${INFOBLOX_USER}" "${INFOBLOX_PASS}" PUT "${ref}" '{"flags":"C"}' >/dev/null
curl_wapi "${INFOBLOX_USER}" "${INFOBLOX_PASS}" DELETE "${ref}" >/dev/null

# Swap the shared credential secret to the restricted (non-superuser) user —
# scenarios 2 and 3 both need a credential that cannot recreate the
# definition, or the refusal self-heals before it can be observed.
log "swapping the ProviderConfig secret to the restricted credential..."
${KUBECTL} create secret generic infobloxnios-credentials -n crossplane-system \
  --from-literal="host=${INFOBLOX_HOST}" \
  --from-literal="username=${INFOBLOX_E2E_RESTRICTED_USER}" \
  --from-literal="password=${INFOBLOX_E2E_RESTRICTED_PASS}" \
  --from-literal="ssl_verify=false" \
  --dry-run=client -o yaml | ${KUBECTL} apply -f -

# The provider caches a positive prerequisite verdict for up to 5 minutes
# (see identity.DefaultProbeTTL) — until that cache entry expires, a
# resource forced down the identity-search path sees the raw WAPI error the
# guard exists to convert, not the remediation. Wait it out so the
# assertions below observe steady state rather than a race with the cache.
log "waiting for the cached prerequisite verdict to expire (up to 5 minutes)..."
sleep 280

# Force scenario 1's object down the identity-search path by staling its
# reference to something that will 404.
cluster_ref="$(${KUBECTL} get arecord.recorda.infobloxnios.crossplane.io/idp-s1-${RUN_TOKEN} -o jsonpath='{.metadata.annotations.crossplane\.io/external-name}')"
ns_ref="$(${KUBECTL} get arecord.recorda.infobloxnios.m.crossplane.io/idp-s1-ns-${RUN_TOKEN} -n default -o jsonpath='{.metadata.annotations.crossplane\.io/external-name}')"
${KUBECTL} annotate arecord.recorda.infobloxnios.crossplane.io/idp-s1-${RUN_TOKEN} \
  crossplane.io/external-name="record:a/nonexistent-${RUN_TOKEN}:stale.example.com/default" --overwrite
${KUBECTL} annotate arecord.recorda.infobloxnios.m.crossplane.io/idp-s1-ns-${RUN_TOKEN} -n default \
  crossplane.io/external-name="record:a/nonexistent-ns-${RUN_TOKEN}:stale-ns.example.com/default" --overwrite
# Force an immediate reconcile rather than waiting for the poll interval.
for r in \
  "arecord.recorda.infobloxnios.crossplane.io/idp-s1-${RUN_TOKEN}" \
  ; do
  ${KUBECTL} annotate "${r}" crossplane.io/paused=true --overwrite
done
${KUBECTL} annotate arecord.recorda.infobloxnios.m.crossplane.io/idp-s1-ns-${RUN_TOKEN} -n default crossplane.io/paused=true --overwrite
sleep 3
${KUBECTL} annotate arecord.recorda.infobloxnios.crossplane.io/idp-s1-${RUN_TOKEN} crossplane.io/paused- 2>/dev/null || true
${KUBECTL} annotate arecord.recorda.infobloxnios.m.crossplane.io/idp-s1-ns-${RUN_TOKEN} -n default crossplane.io/paused- 2>/dev/null || true

# ── Scenario 2: reactive guard converts the raw search failure ────────────
log "=== scenario 2: reactive guard surfaces the remediation, not the raw WAPI error ==="
wait_synced arecord.recorda.infobloxnios.crossplane.io "idp-s1-${RUN_TOKEN}" cluster False 60
msg="$(${KUBECTL} get arecord.recorda.infobloxnios.crossplane.io/idp-s1-${RUN_TOKEN} -o jsonpath='{.status.conditions[?(@.type=="Synced")].message}')"
echo "${msg}" | grep -q "the configured credential cannot create one" || { echo "FATAL: scenario 2 condition did not carry the remediation: ${msg}" >&2; exit 1; }
log "scenario 2 PASSED — Synced=False message:"
echo "${msg}"

raw="$(python3 -c "
import urllib.parse
print(urllib.parse.urlencode({'*' + '${SCRATCH_KEY}': 'probe'}))
")"
log "raw WAPI error for the same search, captured independently:"
curl -sk -u "${INFOBLOX_USER}:${INFOBLOX_PASS}" "${WAPI_BASE}/record:a?${raw}" -w '\nHTTP %{http_code}\n'

# Restore the real reference so scenario 1's object stops trying to recreate
# a Grid object that still exists — deleting the definition also stripped
# the stamped attribute VALUE from every object that carried it (a live
# observation worth carrying forward, not documented ahead of time), so a
# forced re-search finds nothing and the ladder treats the object as
# genuinely absent until the reference is restored.
${KUBECTL} annotate arecord.recorda.infobloxnios.crossplane.io/idp-s1-${RUN_TOKEN} \
  crossplane.io/external-name="${cluster_ref}" --overwrite
${KUBECTL} annotate arecord.recorda.infobloxnios.m.crossplane.io/idp-s1-ns-${RUN_TOKEN} -n default \
  crossplane.io/external-name="${ns_ref}" --overwrite

# ── Scenario 3: restricted credential refuses a brand-new create ──────────
log "=== scenario 3: restricted credential refuses a create before any mutating call ==="
apply_arecord "idp-s2-${RUN_TOKEN}" "192.0.2.160" cluster
apply_arecord "idp-s2-ns-${RUN_TOKEN}" "192.0.2.161" namespaced
wait_synced arecord.recorda.infobloxnios.crossplane.io "idp-s2-${RUN_TOKEN}" cluster False 60
wait_synced arecord.recorda.infobloxnios.m.crossplane.io "idp-s2-ns-${RUN_TOKEN}" namespaced False 60

for n in "idp-s2-${RUN_TOKEN}" "idp-s2-ns-${RUN_TOKEN}"; do
  found="$(curl_wapi "${INFOBLOX_USER}" "${INFOBLOX_PASS}" GET "record:a?name=${n}.example.com")"
  [ "${found}" = "[]" ] || { echo "FATAL: ${n}.example.com was created on the Grid despite the refusal" >&2; exit 1; }
done
log "scenario 3 PASSED — both scopes refused, no record:a created on the Grid"

# ── Scenario 4: recreating the definition clears the refusal ──────────────
log "=== scenario 4: recreating the definition converges within the cached-verdict TTL ==="
pod_restarts_before="$(${KUBECTL} -n crossplane-system get "${PROVIDER_POD}" -o jsonpath='{.status.containerStatuses[0].restartCount}')"
recreate_ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
curl_wapi "${INFOBLOX_USER}" "${INFOBLOX_PASS}" POST "extensibleattributedef" \
  "{\"name\":\"${SCRATCH_KEY}\",\"type\":\"STRING\",\"flags\":\"CR\",\"comment\":\"identity-prereq-live-probe recreation\"}" >/dev/null
log "recreated at ${recreate_ts}"

wait_synced arecord.recorda.infobloxnios.crossplane.io "idp-s2-${RUN_TOKEN}" cluster True 320
converge_ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
wait_synced arecord.recorda.infobloxnios.m.crossplane.io "idp-s2-ns-${RUN_TOKEN}" namespaced True 60
wait_synced arecord.recorda.infobloxnios.crossplane.io "idp-s1-${RUN_TOKEN}" cluster True 60
wait_synced arecord.recorda.infobloxnios.m.crossplane.io "idp-s1-ns-${RUN_TOKEN}" namespaced True 60

pod_restarts_after="$(${KUBECTL} -n crossplane-system get "${PROVIDER_POD}" -o jsonpath='{.status.containerStatuses[0].restartCount}')"
pod_start_after="$(${KUBECTL} -n crossplane-system get "${PROVIDER_POD}" -o jsonpath='{.status.startTime}')"
[ "${pod_start_after}" = "${POD_START}" ] || { echo "FATAL: provider pod restarted (start time changed from ${POD_START} to ${pod_start_after})" >&2; exit 1; }
[ "${pod_restarts_before}" = "${pod_restarts_after}" ] || { echo "FATAL: provider pod restart count changed (${pod_restarts_before} -> ${pod_restarts_after})" >&2; exit 1; }
log "scenario 4 PASSED — converged at ${converge_ts} (recreated at ${recreate_ts}), pod start time unchanged (${POD_START}), restarts unchanged (${pod_restarts_after})"

log "all scenarios passed"
