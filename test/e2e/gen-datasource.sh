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
# ── runToken — NAMED objects ──────────────────────────────────────────────
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
# ── netPrefix and its sub-blocks — ADDRESSED objects ────────────────────────
#
#   netPrefix — a /24 block carved out of 100.64.0.0/16 (RFC 6598 Shared
#               Address Space, reserved for exactly this kind of
#               large-scale, non-routed test partitioning — never expected
#               to appear in a customer's real network config). The /16 is
#               partitioned into 256 non-overlapping /24 blocks and one is
#               picked deterministically from a full byte of the seed's
#               hash (BLOCK_INDEX), so concurrent runs usually land in
#               different blocks. This is the address-space half of the
#               mechanism for ADDRESSED objects (Network, NetworkContainer,
#               Range, FixedAddress, IPv4SharedNetwork, HostRecord,
#               PTRRecord), which have no name field — or no name field
#               that is independently sufficient — and cannot be
#               disambiguated by runToken alone.
#
#               This range is deliberately NOT 192.0.2.0/24 (TEST-NET-1):
#               that block is where several NAMED examples already put
#               illustrative field values (record-a's ipv4Addr, dtc-server's
#               host, record-ns's address), so a generated block inside it
#               can literally equal one of those hardcoded literals. It is
#               also not 198.51.100.0/24, 198.51.101.0/24, 203.0.113.0/24,
#               10.0.0.0/24, or 172.25.0.0/16 / 172.26.0.0/16 — every
#               addressed-object example that once hardcoded a CIDR from
#               one of those ranges has since been tokenised onto a
#               ${data.*} sub-block below, so none of them is a live
#               literal any more, but the ranges stay excluded here as a
#               defensive margin against a future example reintroducing
#               one by hand.
#
#               A SINGLE run applies every family in ONE `make e2e` (CORE)
#               invocation with ONE netPrefix, and WAPI rejects overlapping
#               Network objects — so netPrefix alone (one CIDR string)
#               cannot be handed directly to more than one CIDR-shaped
#               consumer (Network, NetworkContainer, IPv4SharedNetwork's member
#               networks) or more than one host-shaped consumer
#               (FixedAddress, HostRecord, Range) without them colliding
#               with each other WITHIN the same run. This script therefore
#               also derives fixed, non-overlapping SUB-BLOCK offsets inside
#               netPrefix — one per (resource, scope) pair — using plain
#               arithmetic on BLOCK_INDEX (no additional hash draws), so the
#               offsets never collide with each other by construction and
#               the only collision risk that remains is netPrefix's own
#               256-block draw (unchanged from before sub-blocking existed).
#
#               Sub-allocation map (all addresses are
#               100.64.<BLOCK_INDEX>.<offset>; recorded here so every
#               resource ticket in the IPAM isolation wave consumes the
#               same layout instead of re-deriving its own):
#
#                 .0/28    netNetworkCluster            (Network, cluster)
#                 .16/28   netNetworkNamespaced          (Network, namespaced)
#                 .32/28   netContainerCluster           (NetworkContainer, cluster)
#                 .48/28   netContainerNamespaced        (NetworkContainer, namespaced)
#                 .64/28   netSharedMemberCluster        (IPv4SharedNetwork member CIDR, cluster)
#                 .80/28   netSharedMemberNamespaced     (IPv4SharedNetwork member CIDR, namespaced)
#                 .96/28   netFixedAddrParentCluster     (prerequisite parent Network for FixedAddress, cluster)
#                 .100     netFixedAddrHostCluster       (FixedAddress ipv4addr, cluster — inside the block above)
#                 .112/28  netFixedAddrParentNamespaced  (prerequisite parent Network for FixedAddress, namespaced)
#                 .120     netFixedAddrHostNamespaced    (FixedAddress ipv4addr, namespaced — inside the block above)
#                 .128     netHostRecordCluster          (HostRecord ipv4Addr, cluster)
#                 .129     netHostRecordNamespaced       (HostRecord ipv4Addr, namespaced)
#                 .160/27  netRangeParentCluster         (prerequisite parent Network for Range, cluster)
#                 .161-181 netRangeStartCluster/netRangeEndCluster          (Range, cluster — 21 addresses, inside the block above)
#                 .192/27  netRangeParentNamespaced      (prerequisite parent Network for Range, namespaced)
#                 .193-213 netRangeStartNamespaced/netRangeEndNamespaced    (Range, namespaced — 21 addresses, inside the block above)
#                 .224/27  netAllocParentCluster         (prerequisite parent NetworkContainer for the EA-based
#                                                          dynamic-allocation Network example, cluster only — no
#                                                          namespaced sibling exists for this pair yet)
#                 .144/28  netContainerRefCluster        (NetworkContainer, networkViewRef reference-path
#                                                          example, cluster only — no namespaced sibling exists
#                                                          for this pair yet)
#                 .130-.143 reserved / unused — room to grow
#
#               netAllocParentCluster arithmetic: the allocate example
#               requests a /28 (16 addresses) via allocatePrefixLen, so the
#               /27 parent (32 addresses, .224-.255) contains exactly two
#               candidate /28 halves — .224-.239 and .240-.255 — and
#               AllocateNetworkByEA may pick either one; its choice is not
#               contractual. Sizing the parent to fit two full /28s gives it
#               room to pick without tripping over the parent's own
#               network/broadcast addresses — the same "child fits with
#               headroom" sizing Range's /27 parent uses above for its
#               21-address span. A per-run parent (nothing else was ever
#               provisioned in it before this run) also makes exhaustion
#               moot either way; the headroom is about allocation room, not
#               about surviving repeat allocations.
#
#               HostRecord does not need its own parent Network object (it
#               bypasses the requirement via configureForDns). FixedAddress
#               and Range both DO need one: FixedAddress's AllocateIP call
#               and Range's CreateNetworkRange call each unconditionally
#               require an existing parent Network covering the address —
#               live-verified on the Grid for Range (WAPI 400
#               IBDataConflictError: "Cannot find the parent network for
#               the DHCP range ..." when no covering Network object
#               exists), correcting an earlier assumption that Range's
#               `network` field being omitted from the example meant no
#               parent was required at all. Each's host/range offset is
#               therefore nested inside its own per-run parent-Network
#               sub-block — provisioning that parent (Makefile
#               prerequisite-bundling ordering, or an equivalent) is a
#               separate concern from this script, which only reserves
#               disjoint address space for it. IPv4SharedNetwork's member
#               CIDR has the same prerequisite shape (WAPI validates
#               shared-network membership against a real Network object) —
#               same resolution.
#
#               The concurrency ceiling is governed ENTIRELY by BLOCK_INDEX
#               (one byte, 256 values) — the sub-block offsets are fixed
#               constants, not additional random draws, so they never add
#               new collision surface. BLOCK_INDEX is HASHED, not allocated
#               from a registry, so two runs CAN draw the same block: the
#               real bound is the birthday bound over 256 values,
#               P(any collision among k runs) ≈ 1 − exp(−k(k−1)/512):
#                 k=2  → ~0.4%      k=5  → ~4%
#                 k=10 → ~16%       k=20 → ~52%
#               This is a real, documented bound — accepted, not one
#               this script tries to eliminate.
#
# ── netV6 — IPv6 ADDRESSED objects (Network's ipv6network variant) ────────
#
#   netV6NetworkCluster / netV6NetworkNamespaced — Network's IPv6 variant
#               (WAPI object type `ipv6network`, selected when the example's
#               `network` field holds an IPv6 CIDR instead of an IPv4 one)
#               has the same no-independent-name-field problem as its IPv4
#               sibling and needs its own per-run sub-block for the same
#               reason. It reuses the SAME BLOCK_INDEX byte netPrefix
#               already draws — no second hash byte — so this does not add
#               a new collision surface on top of the birthday bound
#               documented above; it only spends one more fixed offset
#               inside a draw that already happened.
#
#               Address family: 2001:db8::/32 (RFC 3849 IPv6 documentation
#               prefix, the IPv6 analogue of 100.64.0.0/16 above — reserved
#               for exactly this kind of test partitioning, never expected
#               to appear in a real Grid config). BLOCK_INDEX is rendered as
#               a 2-digit lowercase hex string (netV6Hex) and spliced into
#               the third hextet; the fourth hextet is a fixed 1 (cluster)
#               or 2 (namespaced) — the IPv6 analogue of netPrefix's own
#               sub-block offsets, so the two scopes can never collide with
#               each other within one run regardless of which BLOCK_INDEX is
#               drawn:
#
#                 netV6NetworkCluster:    2001:db8:<netV6Hex>:1::/64
#                 netV6NetworkNamespaced: 2001:db8:<netV6Hex>:2::/64
#
#               This is deliberately NOT 2001:db8::/64 (i.e. third and
#               fourth hextet both zero): record-aaaa's example payload
#               addresses (2001:db8::10, ::11, ::20, ::21) already live
#               there, and a generated block with the fourth hextet fixed
#               at zero could literally reproduce one of those literals. A
#               nonzero fourth hextet (1 or 2) rules that out unconditionally
#               — it never lands in the third-and-fourth-both-zero /64 no
#               matter what BLOCK_INDEX hashes to.
#
#               The fourth-hextet allocation continues past Network's 1/2
#               for every other dual-object-type IPv6 gate that needs its
#               own disjoint /64 within the same run — same reasoning, same
#               reused netV6Hex, no new hash draw:
#
#                 netV6ContainerCluster:    2001:db8:<netV6Hex>:3::/64  (NetworkContainer, cluster)
#                 netV6ContainerNamespaced: 2001:db8:<netV6Hex>:4::/64  (NetworkContainer, namespaced)
#                 netV6FixedAddrParent:     2001:db8:<netV6Hex>:5::/64  (prerequisite parent ipv6network for FixedAddress — ONE block, shared by both scopes)
#                 netV6FixedAddrHostCluster:    2001:db8:<netV6Hex>:5::50  (FixedAddress ipv6addr, cluster — inside the block above)
#                 netV6FixedAddrHostNamespaced: 2001:db8:<netV6Hex>:5::51  (FixedAddress ipv6addr, namespaced — inside the block above)
#
#               FixedAddress's IPv6 parent is deliberately ONE shared /64
#               (hextet 5) rather than two per-scope blocks the way the
#               IPv4 sub-allocation map gives FixedAddress separate
#               netFixedAddrParentCluster/Namespaced /28s. AllocateIP's
#               parent-network requirement is scoped to (network_view,
#               network), not to a Kubernetes API scope — WAPI does not
#               care whether the K8s object that requested the address was
#               cluster- or namespace-scoped — so a single per-run
#               ipv6network can safely cover both the cluster and
#               namespaced FixedAddress examples. The two host addresses
#               (::50, ::51) stay distinct fixed offsets inside that one
#               block so the two scopes never collide with each other
#               within one run. Every one of hextets 3/4/5 is nonzero, so
#               none of them can ever land in the third-and-fourth-both-zero
#               /64 record-aaaa's payload addresses occupy, for the same
#               reason 1/2 cannot.
#
# ── ptrHost — record-ptr's reverse-zone exception ──────────────────────────
#
#   ptrHostCluster / ptrHostNamespaced — PTRRecord needs a reverse-mapping
#               zone (record:ptr's reverse zone_auth) to already exist on
#               the Grid for its address's network, in the given view. The
#               only such zone on this test Grid Manager is 10.1.1.0/24 in
#               DNS view "Internal" — pre-existing Grid state, not created
#               by test/setup.sh, and setup.sh deliberately stays a
#               convergent (idempotent, non-mutating-per-run) prerequisite
#               provisioner rather than creating a per-run reverse zone.
#               A netPrefix (100.64.x.0/24) address has no
#               reverse zone at all, so record-ptr cannot use netPrefix.
#
#               Instead, a per-run HOST offset is derived within the
#               existing shared 10.1.1.0/24 from a second, independent byte
#               of the seed hash (PTR_OCTET, HASH[12:14]) — kept separate
#               from BLOCK_INDEX so the two draws are independent rather
#               than perfectly correlated. The usable pool (10.1.1.2 -
#               10.1.1.253, 252 addresses — .1 and .254 stay reserved
#               headroom, unchanged from before this was a three-way split)
#               is split into THREE fixed, non-overlapping 84-address
#               thirds so the cluster, namespaced, AND reference-path
#               examples of the SAME run never collide with each other by
#               construction:
#                 netPtrHostCluster:    10.1.1.<2 + PTR_OCTET % 84>    (range 2-85)
#                 netPtrHostNamespaced: 10.1.1.<86 + PTR_OCTET % 84>   (range 86-169)
#                 netPtrHostRefCluster: 10.1.1.<170 + PTR_OCTET % 84>  (range 170-253)
#               netPtrHostRefCluster backs record-ptr's ptrdnameRef
#               reference-path example (cluster-scoped only — no namespaced
#               sibling exists for this pair yet, the same shape
#               netContainerRefCluster and netAllocParentCluster already
#               use above).
#               This is a documented, explicitly accepted exception: the
#               collision pool here (84 values per scope) is smaller than
#               netPrefix's 256-block pool, because it is bounded by the
#               size of the one pre-existing reverse zone rather than by a
#               dedicated /16 reserved for this mechanism. Expanding it
#               would require either a second pre-provisioned reverse zone
#               (a setup.sh change, same objection as above) or a per-run
#               reverse zone (also a setup.sh change) — both rejected for
#               the same reason. If record-ptr's E2E concurrency becomes a
#               real constraint, that is the tradeoff to revisit.
#
# Every value is derived deterministically from the seed with sha256, so
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

# netPrefix sub-blocks — fixed offsets inside the run's /24, see the header
# comment's sub-allocation map for the full layout and rationale.
NET_NETWORK_CLUSTER="100.64.${BLOCK_INDEX}.0/28"
NET_NETWORK_NAMESPACED="100.64.${BLOCK_INDEX}.16/28"
NET_CONTAINER_CLUSTER="100.64.${BLOCK_INDEX}.32/28"
NET_CONTAINER_NAMESPACED="100.64.${BLOCK_INDEX}.48/28"
NET_SHARED_MEMBER_CLUSTER="100.64.${BLOCK_INDEX}.64/28"
NET_SHARED_MEMBER_NAMESPACED="100.64.${BLOCK_INDEX}.80/28"
NET_FIXEDADDR_PARENT_CLUSTER="100.64.${BLOCK_INDEX}.96/28"
NET_FIXEDADDR_HOST_CLUSTER="100.64.${BLOCK_INDEX}.100"
NET_FIXEDADDR_PARENT_NAMESPACED="100.64.${BLOCK_INDEX}.112/28"
NET_FIXEDADDR_HOST_NAMESPACED="100.64.${BLOCK_INDEX}.120"
NET_HOSTRECORD_CLUSTER="100.64.${BLOCK_INDEX}.128"
NET_HOSTRECORD_NAMESPACED="100.64.${BLOCK_INDEX}.129"
NET_RANGE_PARENT_CLUSTER="100.64.${BLOCK_INDEX}.160/27"
NET_RANGE_START_CLUSTER="100.64.${BLOCK_INDEX}.161"
NET_RANGE_END_CLUSTER="100.64.${BLOCK_INDEX}.181"
NET_RANGE_PARENT_NAMESPACED="100.64.${BLOCK_INDEX}.192/27"
NET_RANGE_START_NAMESPACED="100.64.${BLOCK_INDEX}.193"
NET_RANGE_END_NAMESPACED="100.64.${BLOCK_INDEX}.213"
NET_ALLOC_PARENT_CLUSTER="100.64.${BLOCK_INDEX}.224/27"
NET_CONTAINER_REF_CLUSTER="100.64.${BLOCK_INDEX}.144/28"

# netV6Network{Cluster,Namespaced} — Network's IPv6 (ipv6network) variant,
# see the header comment's netV6 section. Reuses BLOCK_INDEX (same byte
# netPrefix drew, no second hash byte) rendered as 2-digit lowercase hex and
# spliced into the third hextet; the fourth hextet (1/2) is a fixed,
# non-hashed offset that keeps the two scopes disjoint from each other.
NET_V6_HEX="$(printf '%02x' "${BLOCK_INDEX}")"
NET_V6_NETWORK_CLUSTER="2001:db8:${NET_V6_HEX}:1::/64"
NET_V6_NETWORK_NAMESPACED="2001:db8:${NET_V6_HEX}:2::/64"

# netV6Container{Cluster,Namespaced} and netV6FixedAddr{Parent,HostCluster,
# HostNamespaced} — the same netV6Hex byte, continuing the fourth-hextet
# allocation past Network's 1/2 (see the header comment's netV6 section).
# netV6FixedAddrParent is a SINGLE shared /64 for both FixedAddress scopes
# — WAPI's parent-network requirement is not K8s-scope-aware, so one
# per-run ipv6network safely covers both host offsets below.
NET_V6_CONTAINER_CLUSTER="2001:db8:${NET_V6_HEX}:3::/64"
NET_V6_CONTAINER_NAMESPACED="2001:db8:${NET_V6_HEX}:4::/64"
NET_V6_FIXEDADDR_PARENT="2001:db8:${NET_V6_HEX}:5::/64"
NET_V6_FIXEDADDR_HOST_CLUSTER="2001:db8:${NET_V6_HEX}:5::50"
NET_V6_FIXEDADDR_HOST_NAMESPACED="2001:db8:${NET_V6_HEX}:5::51"

# ptrHost — record-ptr's separate, independently-drawn shared-pool
# exception. PTR_OCTET is a second, independent byte of the hash so this
# draw is not perfectly correlated with BLOCK_INDEX. Three fixed,
# non-overlapping 84-address thirds — see the header comment's ptrHost
# section for the full range table and rationale.
PTR_OCTET=$(( 16#${HASH:12:2} ))
NET_PTRHOST_CLUSTER="10.1.1.$(( 2 + (PTR_OCTET % 84) ))"
NET_PTRHOST_NAMESPACED="10.1.1.$(( 86 + (PTR_OCTET % 84) ))"
NET_PTRHOST_REF_CLUSTER="10.1.1.$(( 170 + (PTR_OCTET % 84) ))"

mkdir -p "$(dirname "${OUT}")"
cat > "${OUT}" <<DATASOURCE
# Generated by test/e2e/gen-datasource.sh — do not edit by hand.
# seed: ${SEED}
runToken: "${RUN_TOKEN}"
netPrefix: "${NET_PREFIX}"
netNetworkCluster: "${NET_NETWORK_CLUSTER}"
netNetworkNamespaced: "${NET_NETWORK_NAMESPACED}"
netContainerCluster: "${NET_CONTAINER_CLUSTER}"
netContainerNamespaced: "${NET_CONTAINER_NAMESPACED}"
netSharedMemberCluster: "${NET_SHARED_MEMBER_CLUSTER}"
netSharedMemberNamespaced: "${NET_SHARED_MEMBER_NAMESPACED}"
netFixedAddrParentCluster: "${NET_FIXEDADDR_PARENT_CLUSTER}"
netFixedAddrHostCluster: "${NET_FIXEDADDR_HOST_CLUSTER}"
netFixedAddrParentNamespaced: "${NET_FIXEDADDR_PARENT_NAMESPACED}"
netFixedAddrHostNamespaced: "${NET_FIXEDADDR_HOST_NAMESPACED}"
netHostRecordCluster: "${NET_HOSTRECORD_CLUSTER}"
netHostRecordNamespaced: "${NET_HOSTRECORD_NAMESPACED}"
netRangeParentCluster: "${NET_RANGE_PARENT_CLUSTER}"
netRangeStartCluster: "${NET_RANGE_START_CLUSTER}"
netRangeEndCluster: "${NET_RANGE_END_CLUSTER}"
netRangeParentNamespaced: "${NET_RANGE_PARENT_NAMESPACED}"
netRangeStartNamespaced: "${NET_RANGE_START_NAMESPACED}"
netRangeEndNamespaced: "${NET_RANGE_END_NAMESPACED}"
netAllocParentCluster: "${NET_ALLOC_PARENT_CLUSTER}"
netContainerRefCluster: "${NET_CONTAINER_REF_CLUSTER}"
netV6NetworkCluster: "${NET_V6_NETWORK_CLUSTER}"
netV6NetworkNamespaced: "${NET_V6_NETWORK_NAMESPACED}"
netV6ContainerCluster: "${NET_V6_CONTAINER_CLUSTER}"
netV6ContainerNamespaced: "${NET_V6_CONTAINER_NAMESPACED}"
netV6FixedAddrParent: "${NET_V6_FIXEDADDR_PARENT}"
netV6FixedAddrHostCluster: "${NET_V6_FIXEDADDR_HOST_CLUSTER}"
netV6FixedAddrHostNamespaced: "${NET_V6_FIXEDADDR_HOST_NAMESPACED}"
netPtrHostCluster: "${NET_PTRHOST_CLUSTER}"
netPtrHostNamespaced: "${NET_PTRHOST_NAMESPACED}"
netPtrHostRefCluster: "${NET_PTRHOST_REF_CLUSTER}"
DATASOURCE

echo "==> Wrote per-run uptest datasource to ${OUT}"
echo "    seed=${SEED} runToken=${RUN_TOKEN} netPrefix=${NET_PREFIX}"
