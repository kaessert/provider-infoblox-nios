#!/usr/bin/env bash
# test/setup.sh — Bootstrap the Infoblox NIOS E2E environment in a kind
# cluster before uptest/chainsaw assertions run.
#
# This script creates:
#   - infobloxnios-credentials Secret (host/username/password/ssl_verify) in
#     crossplane-system, populated from the INFOBLOX_HOST, INFOBLOX_USER,
#     and INFOBLOX_PASS environment variables (Hive nest secrets / CI
#     secrets — see the credentials section of the provider blueprint).
#     ssl_verify is set to "false" because the E2E Grid Manager presents a
#     self-signed certificate whose SAN does not match the reachable host
#     address.
#   - ProviderConfig (cluster-scoped, group infobloxnios.crossplane.io) —
#     used by cluster-scoped managed resources.
#   - ProviderConfig (namespace-scoped, group infobloxnios.m.crossplane.io)
#     in BOTH the crossplane-system and default namespaces — used by
#     namespaced managed resources that reference a same-namespace
#     ProviderConfig (the unified example-manifest convention places
#     namespaced example manifests in the default namespace).
#   - ClusterProviderConfig (cluster-scoped, group
#     infobloxnios.m.crossplane.io) — used by namespaced managed resources
#     from any namespace via a ClusterProviderConfig reference.
#
# Waits for the provider pod to be Ready before applying any ProviderConfig,
# since the ProviderConfig/ClusterProviderConfig CRDs are only served once
# the provider package has finished installing.
#
# There is no NIOS simulator/mock backend available for this SDK
# (github.com/infobloxopen/infoblox-go-client/v2 talks to a real Grid
# Manager over WAPI), so this script always uses the real credentials
# supplied via INFOBLOX_HOST/INFOBLOX_USER/INFOBLOX_PASS. `make
# e2e-preflight` validates those env vars are present before this script
# ever runs.
#
#   - Auth DNS zone (example.com) in the "default" WAPI view — the shared
#     Grid Manager only has example.com pre-provisioned under the
#     "External" and "Internal" views, not "default". Every example
#     manifest that creates a DNS record uses view: default, so record
#     creates fail with IB.Data.Conflict ("A parent was not found") unless
#     this zone exists first. Created directly via a WAPI POST (bypassing
#     the ZoneAuth managed resource so this prerequisite zone is not
#     coupled to any single resource's CRUD lifecycle), guarded by a GET
#     so re-running setup.sh is a no-op once the zone exists. The ZoneAuth
#     example manifests deliberately target a different fqdn
#     (zoneauth-example.com) so their own Create calls never collide with
#     this pre-provisioned example.com/default zone.
#   - Member Network objects (network view "default") for the CIDRs
#     referenced by the IPv4SharedNetwork example manifests
#     (203.0.113.0/25, 203.0.113.128/25) — WAPI validates shared-network
#     membership against real network objects on the Grid Manager. Created
#     directly via a WAPI POST, guarded by a GET so re-running setup.sh
#     is a no-op once each network exists.
#     The two CIDRs are adjacent, non-overlapping /25 halves of the same
#     /24 block — WAPI rejects Network creation when a candidate CIDR
#     overlaps an existing Network object (e.g. a /24 supernet overlaps
#     any /25 subnet already registered within it), so the two member
#     networks used by the cluster-scoped and namespaced examples MUST
#     NOT be a superset/subset pair of each other.
#
# Usage: test/setup.sh
#   Requires a running kind cluster with Crossplane installed and
#   INFOBLOX_HOST/INFOBLOX_USER/INFOBLOX_PASS set in the environment.
#   Uses ${KUBECTL:-kubectl} for all kubectl operations.
#
# Idempotent: safe to re-run (all resources use $KUBECTL apply).
set -euo pipefail

KUBECTL="${KUBECTL:-kubectl}"

if [ -z "${INFOBLOX_HOST:-}" ] || [ -z "${INFOBLOX_USER:-}" ] || [ -z "${INFOBLOX_PASS:-}" ]; then
  echo "ERROR: INFOBLOX_HOST, INFOBLOX_USER, and INFOBLOX_PASS must all be set." >&2
  echo "  Set them as env vars (Hive nest secrets) or in ../.env" >&2
  exit 1
fi

echo "==> Ensuring crossplane-system and default namespaces exist..."
${KUBECTL} create namespace crossplane-system --dry-run=client -o yaml | ${KUBECTL} apply -f -
${KUBECTL} create namespace default --dry-run=client -o yaml | ${KUBECTL} apply -f - 2>/dev/null || true

# ---------------------------------------------------------------------------
# Wait for the provider pod to be Ready before applying ProviderConfigs.
# The provider package is installed asynchronously by Crossplane's package
# manager; the ProviderConfig/ClusterProviderConfig CRDs are only served
# once the provider's controller pod is up and running.
# ---------------------------------------------------------------------------
echo "==> Waiting for provider-infobloxnios pod to be Ready (up to 300s)..."
READY=""
for i in $(seq 1 60); do
  POD_NAME=$(${KUBECTL} -n crossplane-system get pods -o name 2>/dev/null \
    | grep 'provider-infobloxnios' | head -n1 || true)
  if [ -n "${POD_NAME}" ]; then
    if ${KUBECTL} -n crossplane-system wait "${POD_NAME}" \
        --for=condition=Ready --timeout=5s >/dev/null 2>&1; then
      READY="1"
      echo "==> ${POD_NAME} is Ready"
      break
    fi
  fi
  if [ "${i}" -eq 60 ]; then
    break
  fi
  sleep 5
done

if [ -z "${READY}" ]; then
  echo "ERROR: provider-infobloxnios pod not Ready after 300s" >&2
  ${KUBECTL} -n crossplane-system get pods >&2 || true
  exit 1
fi

echo "==> Creating infobloxnios-credentials Secret in crossplane-system..."

${KUBECTL} create secret generic infobloxnios-credentials \
  --namespace=crossplane-system \
  --from-literal="host=${INFOBLOX_HOST}" \
  --from-literal="username=${INFOBLOX_USER}" \
  --from-literal="password=${INFOBLOX_PASS}" \
  --from-literal=ssl_verify="false" \
  --dry-run=client -o yaml | ${KUBECTL} apply -f -

echo "==> Creating cluster-scoped ProviderConfig (default)..."

${KUBECTL} apply -f - <<'EOF'
apiVersion: infobloxnios.crossplane.io/v1alpha1
kind: ProviderConfig
metadata:
  name: default
spec:
  credentials:
    source: Secret
    secretRef:
      namespace: crossplane-system
      name: infobloxnios-credentials
      key: password
EOF

echo "==> Creating namespace-scoped ProviderConfig (default) in crossplane-system..."

${KUBECTL} apply -f - <<'EOF'
apiVersion: infobloxnios.m.crossplane.io/v1alpha1
kind: ProviderConfig
metadata:
  name: default
  namespace: crossplane-system
spec:
  credentials:
    source: Secret
    secretRef:
      namespace: crossplane-system
      name: infobloxnios-credentials
      key: password
EOF

echo "==> Creating namespace-scoped ProviderConfig (default) in default namespace..."

${KUBECTL} apply -f - <<'EOF'
apiVersion: infobloxnios.m.crossplane.io/v1alpha1
kind: ProviderConfig
metadata:
  name: default
  namespace: default
spec:
  credentials:
    source: Secret
    secretRef:
      namespace: crossplane-system
      name: infobloxnios-credentials
      key: password
EOF

echo "==> Creating ClusterProviderConfig (default, cluster-scoped)..."

${KUBECTL} apply -f - <<'EOF'
apiVersion: infobloxnios.m.crossplane.io/v1alpha1
kind: ClusterProviderConfig
metadata:
  name: default
spec:
  credentials:
    source: Secret
    secretRef:
      namespace: crossplane-system
      name: infobloxnios-credentials
      key: password
EOF

echo "==> Ensuring the example.com auth zone exists in the default WAPI view..."

WAPI_VERSION="${INFOBLOX_WAPI_VERSION:-v2.13.1}"
WAPI_BASE="https://${INFOBLOX_HOST}/wapi/${WAPI_VERSION}"
ZONE_LOOKUP=$(curl -sk -u "${INFOBLOX_USER}:${INFOBLOX_PASS}" \
  "${WAPI_BASE}/zone_auth?view=default&fqdn=example.com")

if [ "${ZONE_LOOKUP}" = "[]" ]; then
  echo "    Zone example.com not found in view default — creating it..."
  curl -sk -u "${INFOBLOX_USER}:${INFOBLOX_PASS}" \
    -X POST "${WAPI_BASE}/zone_auth" \
    -H "Content-Type: application/json" \
    -d '{"fqdn": "example.com", "view": "default"}' >/dev/null
  echo "    Created zone_auth example.com/default."
else
  echo "    Zone example.com already exists in view default — skipping."
fi

echo "==> Ensuring member Network objects for IPv4SharedNetwork examples exist..."

for NETWORK_CIDR in "203.0.113.0/25" "203.0.113.128/25"; do
  NETWORK_LOOKUP=$(curl -sk -u "${INFOBLOX_USER}:${INFOBLOX_PASS}" \
    -G --data-urlencode "network=${NETWORK_CIDR}" --data-urlencode "network_view=default" \
    "${WAPI_BASE}/network")
  if [ "${NETWORK_LOOKUP}" = "[]" ]; then
    echo "    Network ${NETWORK_CIDR} not found in view default — creating it..."
    NETWORK_CREATE_RESPONSE=$(curl -sk -u "${INFOBLOX_USER}:${INFOBLOX_PASS}" \
      -w '\n%{http_code}' \
      -X POST "${WAPI_BASE}/network" \
      -H "Content-Type: application/json" \
      -d "{\"network\": \"${NETWORK_CIDR}\", \"network_view\": \"default\"}")
    NETWORK_CREATE_STATUS="${NETWORK_CREATE_RESPONSE##*$'\n'}"
    if [ "${NETWORK_CREATE_STATUS}" != "201" ]; then
      echo "ERROR: failed to create network ${NETWORK_CIDR}/default (HTTP ${NETWORK_CREATE_STATUS}):" >&2
      echo "${NETWORK_CREATE_RESPONSE%$'\n'*}" >&2
      exit 1
    fi
    echo "    Created network ${NETWORK_CIDR}/default."
  else
    echo "    Network ${NETWORK_CIDR} already exists in view default — skipping."
  fi
done

echo "==> E2E setup complete."
echo "    NIOS Grid Manager: ${INFOBLOX_HOST}"
