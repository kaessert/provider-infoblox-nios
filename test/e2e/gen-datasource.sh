#!/usr/bin/env bash
# test/e2e/gen-datasource.sh — Generate a per-run uptest datasource file.
#
# The shared NIOS Grid Manager is one host, one object namespace. Two E2E
# runs of the same resource that both use the literal identity baked into an
# example manifest (e.g. `www.example.com`) collide on Create with
# `IBDataConflictError`. This script derives per-run backend identities from
# a seed — normally $KIND_CLUSTER_NAME, which AGENTS.md worktree isolation
# already makes unique per run — and writes them to a uptest
# `--data-source` YAML file so `${data.<key>}` placeholders in example
# manifests resolve to values that differ between concurrent runs.
#
# Usage:
#   test/e2e/gen-datasource.sh <seed> <output-file>
#
#   seed:        per-run identity seed. Use $KIND_CLUSTER_NAME — do not pass
#                a raw worktree path or other high-entropy string directly:
#                this script hashes the seed down to a fixed-length token
#                regardless of the seed's own length, but the seed itself
#                should be stable for the whole run and different across
#                concurrent runs, which KIND_CLUSTER_NAME already guarantees.
#   output-file: where to write the generated datasource YAML. Point
#                UPTEST_DATASOURCE_PATH at this same path before invoking
#                `make e2e.<resource>` — see "Wiring" below.
#
# Two values are generated:
#
#   runToken  — a fixed-length (10 char), lowercase hex token. Safe to embed
#               in any DNS label: hex is alphanumeric, so splicing it
#               between two hyphens (e.g. `www-${data.runToken}.example.com`)
#               can never produce a leading/trailing hyphen or an invalid
#               character, and the fixed length bounds the resulting FQDN
#               regardless of how long the seed is. Used to disambiguate
#               NAMED objects between runs — DNS records, zones, DTC
#               objects, EA definitions, and similar resources whose WAPI
#               identity is a name, not an address.
#
#   netPrefix — a /24 block carved out of 100.64.0.0/16 (RFC 6598 Shared
#               Address Space, reserved for exactly this kind of
#               large-scale, non-routed test partitioning — never expected
#               to appear in a customer's real network config). The /16 is
#               partitioned into 256 non-overlapping /24 blocks and one is
#               picked deterministically from a full byte of the seed's
#               hash, so concurrent runs land in different blocks. This is
#               the address-space half of the mechanism for ADDRESSED
#               objects (Network, NetworkContainer, Range, FixedAddress,
#               IPv4SharedNetwork), which have no name field at all and
#               cannot be disambiguated by runToken.
#
#               This range is deliberately NOT 192.0.2.0/24 (TEST-NET-1):
#               that block is where several NAMED examples already put
#               illustrative field values (record-a's ipv4Addr, dtc-server's
#               host, record-ns's address), so a generated /28 inside it can
#               literally equal one of those hardcoded literals. It is also
#               not 198.51.100/101.0/24, 203.0.113.0/24, 10.0.0.0/24 or
#               172.25/26.0.0/16 — every one of those is already the fixed
#               CIDR of an existing addressed-object example (network,
#               ipv4-shared-network, range, fixed-address, host-record,
#               network-container), so a generated block there would need to
#               dodge each literal individually and would still be too
#               narrow: range.yaml's 21-address span alone does not fit a
#               /28 (16 addresses, 14 usable). A /24 (254 usable) comfortably
#               holds every existing addressed-object example's span with
#               room to grow. The concurrency ceiling this implies — at most
#               256 non-colliding runs against this block before a prefix
#               repeats — is a real, documented bound (ADR-IN-0007), not one
#               this script tries to eliminate.
#
#               NOTE: this value is generated and plumbed through the
#               datasource file on every run, but a given resource's example
#               manifest only consumes it once that resource's identity is
#               tokenised (record-a does not — its identity is (view, zone,
#               name), not an address).
#
# Both values are derived deterministically from the seed with sha256, so
# the same seed always reproduces the same identities within a run
# (idempotent — re-running this script mid-run does not rotate the
# identity out from under an in-flight test), and two different seeds are
# astronomically unlikely to collide on runToken (2^40 space).
#
# Wiring: `make e2e.<resource>` generates this file itself, from
# KIND_CLUSTER_NAME, before invoking uptest — this script is not meant to be
# hand-invoked as part of the normal E2E flow. See UPTEST_DATASOURCE_PATH
# and the e2e-datasource target in the Makefile.
#
# Mutation check / disabling the mechanism: pass UPTEST_DATASOURCE_PATH=
# (empty) to `make`, or otherwise ensure this script does not run. The
# `${data.runToken}` placeholders in the example manifests then stay
# literal — identical across every run — which reproduces the
# pre-isolation collision this script exists to prevent.
set -euo pipefail

SEED="${1:?usage: gen-datasource.sh <seed> <output-file>}"
OUT="${2:?usage: gen-datasource.sh <seed> <output-file>}"

HASH="$(printf '%s' "${SEED}" | sha256sum | cut -d' ' -f1)"

RUN_TOKEN="${HASH:0:10}"
if ! [[ "${RUN_TOKEN}" =~ ^[0-9a-f]{10}$ ]]; then
  echo "gen-datasource: derived runToken '${RUN_TOKEN}' is not a safe DNS label fragment" >&2
  exit 1
fi

BLOCK_INDEX=$(( 16#${HASH:10:2} ))
NET_PREFIX="100.64.${BLOCK_INDEX}.0/24"

mkdir -p "$(dirname "${OUT}")"
cat > "${OUT}" <<DATASOURCE
# Generated by test/e2e/gen-datasource.sh — do not edit by hand.
# seed: ${SEED}
runToken: "${RUN_TOKEN}"
netPrefix: "${NET_PREFIX}"
DATASOURCE

echo "==> Wrote per-run uptest datasource to ${OUT}"
echo "    seed=${SEED} runToken=${RUN_TOKEN} netPrefix=${NET_PREFIX}"
