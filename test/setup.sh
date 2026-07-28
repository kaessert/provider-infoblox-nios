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
# There is no NIOS simulator/mock backend available for this SDK
# (github.com/infobloxopen/infoblox-go-client/v2 talks to a real Grid
# Manager over WAPI), so this script always uses the real credentials
# supplied via INFOBLOX_HOST/INFOBLOX_USER/INFOBLOX_PASS. `make
# e2e-preflight` validates those env vars are present before this script
# ever runs.
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

echo "==> E2E setup complete."
echo "    NIOS Grid Manager: ${INFOBLOX_HOST}"
