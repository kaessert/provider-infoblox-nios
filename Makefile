# ====================================================================================
# Setup Project
PROJECT_NAME := provider-infobloxnios
PROJECT_REPO := github.com/crossplane-contrib/provider-infoblox-nios

PLATFORMS ?= linux_amd64 linux_arm64

# Source .env credentials for E2E tests if present (optional fallback).
# Primary credential source: environment variables / CI secrets.
# .env format is KEY=VALUE (no `export` prefix) — valid GNU Make syntax.
# Note: ROOT_DIR is not yet set (common.mk sets it), so we use CURDIR.
ifneq (,$(wildcard $(CURDIR)/../.env))
  include $(CURDIR)/../.env
  export
endif

-include build/makelib/common.mk

# ====================================================================================
# Setup Output

-include build/makelib/output.mk

# ====================================================================================
# Setup Go

NPROCS ?= 1
GO_TEST_PARALLEL := $(shell echo $$(( $(NPROCS) / 2 )))
GO_STATIC_PACKAGES = $(GO_PROJECT)/cmd/provider
GO_LDFLAGS += -X $(GO_PROJECT)/internal/version.Version=$(VERSION)
GO_SUBDIRS += cmd internal apis test/e2e
GO111MODULE = on
GOLANGCILINT_VERSION = 2.12.2
-include build/makelib/golang.mk

# ====================================================================================
# Setup Kubernetes tools

KIND_VERSION = v0.31.0
CROSSPLANE_VERSION = 2.2.1
CROSSPLANE_CLI_VERSION = v2.2.1
-include build/makelib/k8s_tools.mk

# ====================================================================================
# Setup Images

IMAGES = provider-infobloxnios
-include build/makelib/imagelight.mk

# ====================================================================================
# Setup XPKG

UP_ORG ?= upbound
ifeq ($(strip $(UP_ORG)),)
UP_ORG := upbound
endif
XPKG_REG_ORGS ?= xpkg.upbound.io/$(UP_ORG)
# NOTE(hasheddan): skip promoting on xpkg.upbound.io as channel tags are
# inferred.
XPKG_REG_ORGS_NO_PROMOTE ?= xpkg.upbound.io/$(UP_ORG)
XPKGS = provider-infobloxnios
-include build/makelib/xpkg.mk
-include build/makelib/local.xpkg.mk
-include build/makelib/controlplane.mk

# NOTE(hasheddan): we force image building to happen prior to xpkg build so that
# we ensure image is present in daemon.
xpkg.build.provider-infobloxnios: do.build.images

fallthrough: submodules
	@echo Initial setup complete. Running make again . . .
	@make

# ====================================================================================
# E2E Testing (uptest)

UPTEST_LOCAL_DEPLOY_TARGET = local-deploy

# ----------------------------------------------------------------------------------
# E2E poll interval — single source of truth
#
# The provider's post-assert converge/run checks (test/hooks/run-update-tester.sh)
# wait a multiple of the provider's --poll interval to prove the controller has
# actually stopped reconciling, not just that it hasn't ticked yet. At the
# production default (--poll=1m) those waits dominate E2E wall-clock time. This
# variable exists to lower BOTH sides of that wait — the E2E controlplane's
# --poll flag AND the update-tester wait windows — from one place, so neither
# can be changed without the other following. Lowering only the tester side
# would shrink its drift-detection sleep below a real reconcile cycle and
# silently stop catching the reconcile-loop / drift bug class it exists to
# catch — every converge would pass because nothing had a chance to happen.
#
# `?=` keeps this overridable per invocation (e.g. a resource whose backend
# settles slowly can raise it: `make e2e.<resource> E2E_POLL_INTERVAL=30s`).
E2E_POLL_INTERVAL ?= 10s

# Rendered E2E-only DeploymentRuntimeConfig. test/e2e/deployment-runtime-config.yaml
# is the checked-in template (placeholder: __E2E_POLL_INTERVAL__); the e2e-drc
# target below substitutes the current E2E_POLL_INTERVAL and writes the result
# here. build/makelib/local.xpkg.mk's local.xpkg.deploy.provider.% rule reads
# DRC_FILE and applies it instead of its own built-in default — but only on
# the controlplane.up path (local-deploy, below), which is reached only from
# `make e2e`/`make e2e.<resource>`/`make e2e-full`. It never touches production
# packaging, and the provider's own default (cmd/provider/main.go) stays 1m.
DRC_FILE := $(CACHE_DIR)/e2e-deployment-runtime-config.yaml

# Reaches update-tester's `converge` (drift-detection sleep = 1.5x this value)
# and `run` (slow-observe bar = 0.5x this value) subcommands via
# test/hooks/run-update-tester.sh, which execs into the tool reading this from
# the environment. `export` propagates it through uptest -> chainsaw -> the
# post-assert hook subprocess tree.
#
# `E2E_POLL_INTERVAL` (above) is the ONLY supported knob for this pairing.
# `override` here means a command-line or environment attempt to set
# UPDATE_TESTER_POLL_INTERVAL directly (e.g. `make e2e.record-a
# UPDATE_TESTER_POLL_INTERVAL=5s`) is deliberately ignored — GNU Make lets a
# command-line variable outrank a plain assignment, so without `override`
# that invocation would unpair the two knobs: the tester's drift-detection
# sleep would shrink to 1.5x the smaller value while the controlplane still
# polls at the E2E_POLL_INTERVAL-derived rate. That sleep would no longer
# outlast a reconcile cycle, so every converge would pass because nothing had
# a chance to happen — a silent correctness regression, not a tuning choice.
# `override` does not affect E2E_POLL_INTERVAL itself, so the documented
# per-resource escape hatch (`make e2e.<resource> E2E_POLL_INTERVAL=30s`)
# still moves both knobs together.
#
# Per-resource tuning uses the target-specific idiom already established
# fleet-wide (e.g. `e2e.record-a: E2E_POLL_INTERVAL = 30s`), NOT a
# command-line override. This assignment MUST stay recursive (`=`, not
# `:=`): a simple assignment expands $(E2E_POLL_INTERVAL) once at parse
# time, before any target-specific value is set, so it would permanently
# capture the global default and never see a per-target override. Recursive
# expansion is evaluated per-recipe, so a target-specific E2E_POLL_INTERVAL
# reaches both knobs. `override` still blocks setting the derived variable
# directly, so the two compose: `override` forces callers onto
# E2E_POLL_INTERVAL, and `=` makes that knob work per-target.
export override UPDATE_TESTER_POLL_INTERVAL = $(E2E_POLL_INTERVAL)

# Renders test/e2e/deployment-runtime-config.yaml's placeholder into $(DRC_FILE).
#
# NOTE: this can NOT be wired the way e2e-datasource (below) is wired onto
# `uptest` — that trick relies on merging prerequisite lists across two
# rules for the SAME target, which only works for ordinary (non-%) targets.
# local.xpkg.deploy.provider.% is a pattern rule, and GNU Make does not merge
# a second pattern rule's prerequisites onto a stem that already matched a
# pattern rule WITH a recipe (verified empirically: an `local.xpkg.deploy.
# provider.%: e2e-drc` rule here is silently never evaluated). Instead this
# is listed as the FIRST prerequisite, in the SAME rule statement, ahead of
# local.xpkg.deploy.provider.$(PROJECT_NAME) on the `local-deploy:` line
# below — GNU Make's default serial (non -j) mode builds one rule's own
# prerequisite list strictly left to right, so e2e-drc's render is
# guaranteed to finish before local.xpkg.deploy.provider.%'s recipe runs and
# reads $(DRC_FILE). This repo does not invoke E2E with -j.
.PHONY: e2e-drc
e2e-drc:
	@mkdir -p $(CACHE_DIR)
	@sed 's/--poll=__E2E_POLL_INTERVAL__/--poll=$(E2E_POLL_INTERVAL)/' test/e2e/deployment-runtime-config.yaml > $(DRC_FILE)

# Helper variables for comma-separated manifest lists (uptest CLI convention —
# `uptest e2e` takes a single comma-separated string; GNU Make's $(wildcard)
# produces space-separated lists).
comma := ,
empty :=
space := $(empty) $(empty)

# Per-resource manifest variables (comma pair of cluster + namespaced variants,
# so `make e2e.<resource>` gates both scopes).
UPTEST_MANIFESTS_DNS_VIEW := examples/dns-view/dns-view.yaml,examples/dns-view/dns-view-namespaced.yaml
UPTEST_MANIFESTS_DTC_LBDN := examples/dtc-lbdn/dtc-lbdn.yaml,examples/dtc-lbdn/dtc-lbdn-namespaced.yaml
UPTEST_MANIFESTS_DTC_POOL := examples/dtc-pool/dtc-pool.yaml,examples/dtc-pool/dtc-pool-namespaced.yaml
UPTEST_MANIFESTS_DTC_SERVER := examples/dtc-server/dtc-server.yaml,examples/dtc-server/dtc-server-namespaced.yaml
UPTEST_MANIFESTS_EXTENSIBLE_ATTRIBUTE_DEF := examples/extensible-attribute-def/extensible-attribute-def.yaml,examples/extensible-attribute-def/extensible-attribute-def-namespaced.yaml
# FixedAddress's AllocateIP call unconditionally requires an existing
# parent Network object covering the address (unlike HostRecord, which
# bypasses the requirement via configureForDns — see
# test/e2e/gen-datasource.sh's sub-allocation map comment). Prerequisite-
# bundling: the parent Network's manifest is listed BEFORE
# fixed-address.yaml's own, so uptest creates it first (IN-ISO-IPAM-PREREQ;
# address plan: IN-ISO-IPAM-PLAN).
# network-view-ref-prereq.yaml, fixed-address-ref-network-prereq.yaml, and
# fixed-address-ref.yaml are a third, later-added member of this set:
# fixed-address-ref.yaml exercises the networkViewRef reference path
# instead of the literal networkView the first two fixed-address examples
# use, so it needs both its own NetworkView prerequisite AND its own
# parent Network prerequisite (in that NetworkView) — listed in that order
# so uptest creates each dependency before the next needs it. Folded into
# this same variable (not a separate e2e target) so `make e2e.fixed-address`
# proves both the literal-networkView path (still covered by the first two
# manifests) AND the reference-resolver path in one run.
UPTEST_MANIFESTS_FIXED_ADDRESS := examples/fixed-address/network-prereq.yaml,examples/fixed-address/network-prereq-namespaced.yaml,examples/fixed-address/fixed-address.yaml,examples/fixed-address/fixed-address-namespaced.yaml,examples/fixed-address/network-view-ref-prereq.yaml,examples/fixed-address/fixed-address-ref-network-prereq.yaml,examples/fixed-address/fixed-address-ref.yaml
# IPv6 variant of FixedAddress — WAPI resolves this to the
# ipv6fixedaddress object type at runtime instead of fixedaddress. Kept as
# a separate target (not folded into UPTEST_MANIFESTS_FIXED_ADDRESS/
# e2e.fixed-address) so a regression on either address family fails
# independently. AllocateIP's parent-network requirement is scoped to
# (network_view, network), not to a Kubernetes API scope, so — unlike
# IPv4 FixedAddress's two per-scope prereqs — a SINGLE shared
# network-prereq-v6.yaml (a per-run /64) covers both
# fixed-address-v6.yaml and fixed-address-v6-namespaced.yaml. Listed
# first so uptest creates the parent ipv6network before either resource
# manifest (IN-ISO-IPAM-PREREQ; address plan: IN-ISO-IPAM-PLAN).
UPTEST_MANIFESTS_FIXED_ADDRESS_V6 := examples/fixed-address/network-prereq-v6.yaml,examples/fixed-address/fixed-address-v6.yaml,examples/fixed-address/fixed-address-v6-namespaced.yaml
# host-record-ref.yaml is the third member of this set: it exercises the
# networkViewRef reference path instead of the literal networkView
# host-record.yaml/host-record-namespaced.yaml use, so it needs its own
# NetworkView prerequisite (network-view-ref-prereq.yaml, listed first so
# uptest creates it before the reference can resolve). Folded into this
# same variable (not a separate e2e target) so `make e2e.host-record`
# proves both the literal-networkView path (still covered by the first two
# manifests) AND the reference-resolver path in one run.
UPTEST_MANIFESTS_HOST_RECORD := examples/host-record/host-record.yaml,examples/host-record/host-record-namespaced.yaml,examples/host-record/network-view-ref-prereq.yaml,examples/host-record/host-record-ref-network-prereq.yaml,examples/host-record/host-record-ref.yaml
# IPv4SharedNetwork's `networks` field must reference a real, pre-existing
# Network object's CIDR, and a Network can belong to only ONE shared
# network — so its member is a per-run prerequisite, not the general
# examples/network/network.yaml object (which is independently created and
# deleted by its own e2e.network run). Prerequisite-bundling: the member
# Network's manifest is listed BEFORE ipv4-shared-network.yaml's own, so
# uptest creates it first (IN-ISO-IPAM-PREREQ; address plan: IN-ISO-IPAM-PLAN).
UPTEST_MANIFESTS_IPV4_SHARED_NETWORK := examples/ipv4-shared-network/network-prereq.yaml,examples/ipv4-shared-network/network-prereq-namespaced.yaml,examples/ipv4-shared-network/ipv4-shared-network.yaml,examples/ipv4-shared-network/ipv4-shared-network-namespaced.yaml
# network-view-ref-prereq.yaml + network-ref.yaml are the third member of
# this set: network-ref.yaml exercises the networkViewRef reference path
# instead of the literal networkView network.yaml/network-namespaced.yaml
# use, so it needs its own NetworkView prerequisite (listed first so
# uptest creates it before the reference can resolve). Folded into this
# same variable (not a separate e2e target) so `make e2e.network` proves
# both the literal-networkView path (still covered by the first two
# manifests) AND the reference-resolver path in one run.
UPTEST_MANIFESTS_NETWORK := examples/network/network.yaml,examples/network/network-namespaced.yaml,examples/network/network-view-ref-prereq.yaml,examples/network/network-ref.yaml
# Network's EA-based dynamic allocation path (filterParams + allocatePrefixLen
# + object) needs an existing parent NetworkContainer to allocate from, the
# same requirement AllocateNetworkByEA has for FixedAddress/Range's parent
# Network — see UPTEST_MANIFESTS_FIXED_ADDRESS/UPTEST_MANIFESTS_RANGE above.
# Prerequisite-bundling: the parent manifest is listed BEFORE the child, so
# uptest creates it first (IN-ISO-IPAM-PREREQ pattern). No namespaced sibling
# exists for this pair yet.
UPTEST_MANIFESTS_NETWORK_ALLOCATE := examples/network/network-allocate-parent.yaml,examples/network/network-allocate.yaml
# NetworkContainer references NetworkView by name (networkViewRef/Selector
# also available) but both scoped example manifests above use the Grid's
# well-known "default" network view inline — same pattern as
# UPTEST_MANIFESTS_NETWORK above. network-container-ref.yaml is the third
# member of this set: it exercises the networkViewRef reference path
# instead, so it needs its own NetworkView prerequisite
# (network-view-ref-prereq.yaml, listed first so uptest creates it before
# the reference can resolve). Folded into this same variable (not a
# separate e2e target) so `make e2e.network-container` proves both the
# literal-networkView path (still covered by the first two manifests) AND
# the reference-resolver path in one run.
UPTEST_MANIFESTS_NETWORK_CONTAINER := examples/network-container/network-container.yaml,examples/network-container/network-container-namespaced.yaml,examples/network-container/network-view-ref-prereq.yaml,examples/network-container/network-container-ref.yaml
# IPv6 variant of NetworkContainer — WAPI resolves this to the
# ipv6networkcontainer object type at runtime instead of networkcontainer.
# Kept as a separate target (not folded into
# UPTEST_MANIFESTS_NETWORK_CONTAINER/e2e.network-container) so a regression
# on either address family fails independently. Core tier: needs only NIOS
# Grid Manager API credentials, same as its IPv4 sibling.
UPTEST_MANIFESTS_NETWORK_CONTAINER_V6 := examples/network-container/network-container-v6.yaml,examples/network-container/network-container-v6-namespaced.yaml
# IPv6 variant of Network — WAPI resolves this to the ipv6network object
# type at runtime instead of network. Kept as a separate target (not
# folded into UPTEST_MANIFESTS_NETWORK/e2e.network) so a regression on
# either address family fails independently. Core tier: needs only NIOS
# Grid Manager API credentials, same as its IPv4 sibling.
UPTEST_MANIFESTS_NETWORK_V6 := examples/network/network-v6.yaml,examples/network/network-v6-namespaced.yaml
UPTEST_MANIFESTS_NETWORK_VIEW := examples/network-view/network-view.yaml,examples/network-view/network-view-namespaced.yaml
# Range's CreateNetworkRange call, like FixedAddress's AllocateIP,
# unconditionally requires an existing parent Network object covering the
# range's address span, even though the example leaves the Range's own
# `network` field unset — live-verified on the Grid: WAPI 400
# IBDataConflictError ("Cannot find the parent network for the DHCP
# range...") without one. This corrects an earlier assumption that omitting
# `network` from the spec meant no parent Network was required at all.
# Prerequisite-bundling: each scope's parent Network manifest is listed
# BEFORE its own range.yaml/range-namespaced.yaml, so uptest creates it
# first (IN-ISO-IPAM-PREREQ fix; address plan: IN-ISO-IPAM-PLAN).
# network-view-ref-prereq.yaml, range-ref-network-prereq.yaml, and
# range-ref.yaml are a third, later-added member of this set: range-ref.yaml
# exercises the networkViewRef reference path instead of the literal
# networkView the first two range examples use, so it needs both its own
# NetworkView prerequisite AND its own parent Network prerequisite (in
# that NetworkView) — listed in that order so uptest creates each
# dependency before the next needs it. Folded into this same variable (not
# a separate e2e target) so `make e2e.range` proves both the
# literal-networkView path (still covered by the first two manifests) AND
# the reference-resolver path in one run.
UPTEST_MANIFESTS_RANGE := examples/range/network-prereq.yaml,examples/range/network-prereq-namespaced.yaml,examples/range/range.yaml,examples/range/range-namespaced.yaml,examples/range/network-view-ref-prereq.yaml,examples/range/range-ref-network-prereq.yaml,examples/range/range-ref.yaml
UPTEST_MANIFESTS_RANGE_TEMPLATE := examples/range-template/range-template.yaml,examples/range-template/range-template-namespaced.yaml
# The four drift-detection variants (drift-ignore, drift-ignore-namespaced,
# drift-warn, drift-warn-namespaced) prove spec.driftDetection's ignore and
# warn modes end to end against the live NIOS Grid — a dedicated post-assert
# hook per variant tampers the Grid directly over WAPI and asserts the
# controller's convergence behavior, which the generic update-tester tool
# cannot exercise. Folded into this same variable (not a separate e2e
# target) so `make e2e.record-a` proves both the baseline CRUD path (the
# first two manifests) and drift convergence in one run. No prerequisite
# bundling needed: each variant uses the built-in "default" view and a
# per-run isolation token, so it needs nothing test/setup.sh doesn't already
# provision for the baseline pair.
UPTEST_MANIFESTS_RECORD_A := examples/record-a/record-a.yaml,examples/record-a/record-a-namespaced.yaml,examples/record-a/record-a-drift-ignore.yaml,examples/record-a/record-a-drift-ignore-namespaced.yaml,examples/record-a/record-a-drift-warn.yaml,examples/record-a/record-a-drift-warn-namespaced.yaml
UPTEST_MANIFESTS_RECORD_AAAA := examples/record-aaaa/record-aaaa.yaml,examples/record-aaaa/record-aaaa-namespaced.yaml
UPTEST_MANIFESTS_RECORD_ALIAS := examples/record-alias/record-alias.yaml,examples/record-alias/record-alias-namespaced.yaml
# canonicalRef/canonicalSelector let CNAMERecord.canonical resolve against
# a real ARecord instead of a literal FQDN, but both scoped example
# manifests above set the literal value directly — same pattern as
# UPTEST_MANIFESTS_NETWORK above. record-cname-ref.yaml is a third member
# of this set: it exercises the canonicalRef reference path instead, so it
# needs its own ARecord prerequisite (arecord-cname-ref-prereq.yaml, listed
# first so uptest creates it before the reference can resolve). Folded
# into this same variable (not a separate e2e target) so
# `make e2e.record-cname` proves both the literal-canonical path (still
# covered by the first two manifests) AND the reference-resolver path in
# one run.
UPTEST_MANIFESTS_RECORD_CNAME := examples/record-cname/record-cname.yaml,examples/record-cname/record-cname-namespaced.yaml,examples/record-cname/arecord-cname-ref-prereq.yaml,examples/record-cname/record-cname-ref.yaml
UPTEST_MANIFESTS_RECORD_MX := examples/record-mx/record-mx.yaml,examples/record-mx/record-mx-namespaced.yaml
UPTEST_MANIFESTS_RECORD_NS := examples/record-ns/record-ns.yaml,examples/record-ns/record-ns-namespaced.yaml
# record-ptr's two literal-ptrdname examples need no Makefile
# prerequisite-bundling: their reverse zone is the pre-existing, shared
# 10.1.1.0/24 zone_auth (view Internal) that test/setup.sh already
# provisions convergently, not a per-run object. Isolation is per-run HOST
# offsets drawn from a second, independently-hashed pool within that
# shared zone (test/e2e/gen-datasource.sh's ptrHost derivation) — a
# documented exception, not prerequisite-bundling (IN-ISO-IPAM-PREREQ;
# address plan: IN-ISO-IPAM-PLAN). record-ptr-ref.yaml is a third member
# of this set: it exercises the ptrdnameRef reference path instead, so it
# needs its own ARecord prerequisite (arecord-ptr-ref-prereq.yaml, listed
# first so uptest creates it before the reference can resolve) — same
# pattern as UPTEST_MANIFESTS_RECORD_CNAME below. Folded into this same
# variable (not a separate e2e target) so `make e2e.record-ptr` proves
# both the literal-ptrdname path (still covered by the first two
# manifests) AND the reference-resolver path in one run.
UPTEST_MANIFESTS_RECORD_PTR := examples/record-ptr/record-ptr.yaml,examples/record-ptr/record-ptr-namespaced.yaml,examples/record-ptr/arecord-ptr-ref-prereq.yaml,examples/record-ptr/record-ptr-ref.yaml
UPTEST_MANIFESTS_RECORD_SRV := examples/record-srv/record-srv.yaml,examples/record-srv/record-srv-namespaced.yaml
UPTEST_MANIFESTS_RECORD_TXT := examples/record-txt/record-txt.yaml,examples/record-txt/record-txt-namespaced.yaml
# The two drift-ignore variants prove spec.driftDetection's ignore mode end
# to end on a second resource shape (a zone rather than a DNS record) —
# see the UPTEST_MANIFESTS_RECORD_A comment above for the full rationale.
# No zone-auth drift-warn variant exists: warn mode is already proven on
# record-a and repeating it here adds no new coverage of the mode itself.
# No prerequisite bundling needed: each variant targets its own
# per-run-token fqdn in the built-in "default" view, distinct from both the
# baseline zone-auth.yaml zone and the "example.com" parent zone
# test/setup.sh pre-provisions for the DNS record examples.
UPTEST_MANIFESTS_ZONE_AUTH := examples/zone-auth/zone-auth.yaml,examples/zone-auth/zone-auth-namespaced.yaml,examples/zone-auth/zone-auth-drift-ignore.yaml,examples/zone-auth/zone-auth-drift-ignore-namespaced.yaml
UPTEST_MANIFESTS_ZONE_DELEGATED := examples/zone-delegated/zone-delegated.yaml,examples/zone-delegated/zone-delegated-namespaced.yaml
UPTEST_MANIFESTS_ZONE_FORWARD := examples/zone-forward/zone-forward.yaml,examples/zone-forward/zone-forward-namespaced.yaml

# E2E manifest tiers
# CORE: resources that only need NIOS Grid Manager API credentials
# (INFOBLOX_HOST, INFOBLOX_USER, INFOBLOX_PASS) — no additional external
# infrastructure to provision. This is the full core-tier set: all record
# types, HostRecord, Network(View/Container), NetworkV6 (IPv6 sibling of
# Network), RangeTemplate, ZoneAuth, ZoneDelegated, ZoneForward,
# IPv4SharedNetwork, FixedAddress, Range, NetworkAllocate (EA-based dynamic
# CIDR allocation), DTCServer, DTCPool, DTCLBDN, NSRecord, and
# ExtensibleAttributeDef. DTCPool and DTCLBDN need no external
# prerequisites — their `servers`/`pools`/`authZones` fields are optional.
UPTEST_MANIFESTS_CORE = $(UPTEST_MANIFESTS_RECORD_A),$(UPTEST_MANIFESTS_RECORD_AAAA),$(UPTEST_MANIFESTS_RECORD_ALIAS),$(UPTEST_MANIFESTS_RECORD_CNAME),$(UPTEST_MANIFESTS_RECORD_MX),$(UPTEST_MANIFESTS_RECORD_NS),$(UPTEST_MANIFESTS_RECORD_PTR),$(UPTEST_MANIFESTS_RECORD_SRV),$(UPTEST_MANIFESTS_RECORD_TXT),$(UPTEST_MANIFESTS_ZONE_DELEGATED),$(UPTEST_MANIFESTS_NETWORK_VIEW),$(UPTEST_MANIFESTS_HOST_RECORD),$(UPTEST_MANIFESTS_NETWORK),$(UPTEST_MANIFESTS_NETWORK_V6),$(UPTEST_MANIFESTS_RANGE_TEMPLATE),$(UPTEST_MANIFESTS_ZONE_AUTH),$(UPTEST_MANIFESTS_IPV4_SHARED_NETWORK),$(UPTEST_MANIFESTS_NETWORK_CONTAINER),$(UPTEST_MANIFESTS_NETWORK_CONTAINER_V6),$(UPTEST_MANIFESTS_FIXED_ADDRESS),$(UPTEST_MANIFESTS_FIXED_ADDRESS_V6),$(UPTEST_MANIFESTS_ZONE_FORWARD),$(UPTEST_MANIFESTS_RANGE),$(UPTEST_MANIFESTS_NETWORK_ALLOCATE),$(UPTEST_MANIFESTS_DTC_SERVER),$(UPTEST_MANIFESTS_EXTENSIBLE_ATTRIBUTE_DEF),$(UPTEST_MANIFESTS_DNS_VIEW),$(UPTEST_MANIFESTS_DTC_POOL),$(UPTEST_MANIFESTS_DTC_LBDN)

# UPTEST_MANIFESTS_ALL: discover all resource examples, excluding provider/ config.
# Produces a comma-separated list for `uptest e2e` (the unified example-manifest convention).
_UPTEST_MANIFESTS_ALL_RAW := $(filter-out examples/provider/%,$(wildcard examples/*/*.yaml))
UPTEST_MANIFESTS_ALL := $(subst $(space),$(comma),$(_UPTEST_MANIFESTS_ALL_RAW))

# Default e2e input: CORE manifests
UPTEST_INPUT_MANIFESTS ?= $(UPTEST_MANIFESTS_CORE)

# uptest's built-in default process budget is 1200s, which is shorter than a
# single multi-resource apply stage once post-assert update-test hooks run
# (~3-4 min per resource). Without this, uptest SIGKILLs chainsaw AFTER it
# has already reported PASS on the apply stage and never runs the
# import/delete stages — leaking external Grid objects that then collide
# with the next run. `?=` keeps this overridable per invocation. This is
# independent of the per-resource `uptest.upbound.io/timeout` annotations,
# which gate a single chainsaw assertion, not the whole process.
UPTEST_DEFAULT_TIMEOUT ?= 3600s

UPTEST_SETUP_SCRIPT ?= test/setup.sh

# Minimum free space (in GB) required on / before starting an E2E run.
# A full root filesystem doesn't announce itself as a disk error — it
# surfaces as a build or pod failure that never mentions disk, while the
# kind node's DiskPressure condition stays False the whole time (kubelet
# never evicts, nothing self-heals). Without this guard, the run burns its
# full E2E timeout and reports a misleading provider failure. `?=` keeps
# this overridable per invocation (e.g. `make e2e-preflight E2E_MIN_FREE_GB=5`).
E2E_MIN_FREE_GB ?= 15

# Per-run uptest test-directory isolation (each concurrent E2E run gets its own
# staging directory) — without this, concurrent E2E runs share /tmp/uptest-e2e
# and corrupt each other's staged chainsaw test files.
ifdef KIND_CLUSTER_NAME
  UPTEST_ARGS += --test-directory=/tmp/uptest-e2e-$(KIND_CLUSTER_NAME)
endif

-include build/makelib/uptest.mk

# Per-run uptest datasource: generates the name/address
# isolation values that let concurrent E2E runs share the NIOS Grid Manager
# without colliding, and points UPTEST_DATASOURCE_PATH at the result so
# uptest.mk's UPTEST_COMMAND picks it up as --data-source=<path>.
#
# This is hooked as a prerequisite of the `uptest` target itself, not `e2e`.
# `e2e`'s own prerequisite list is assembled from two separate rules (this
# Makefile's and uptest.mk's) and Make only guarantees that ALL of a
# target's prerequisites finish before that target's OWN recipe runs — it
# does NOT guarantee siblings run in declaration order across merged rules
# (e2e-preflight below is proof: it is declared after uptest.mk's `e2e`
# rule and so runs after the `uptest` recipe, not before it). Attaching
# generation to `uptest` instead guarantees it always completes before
# uptest's recipe constructs and runs the `uptest e2e ...` command line,
# regardless of where `e2e`'s prerequisite list puts it.
#
# The `?=` default lets a caller explicitly disable the mechanism for the
# mutation check with `make e2e.<resource> UPTEST_DATASOURCE_PATH=` (empty
# command-line override) — Make gives command-line assignments priority
# over `?=`, so the override sticks, generation is skipped, and
# `--data-source=""` reaches uptest, reproducing the pre-isolation
# collision behavior on purpose.
UPTEST_DATASOURCE_PATH ?= $(CACHE_DIR)/e2e-datasource-$(if $(KIND_CLUSTER_NAME),$(KIND_CLUSTER_NAME),local-dev).yaml

.PHONY: e2e-datasource
e2e-datasource:
	@if [ -n "$(UPTEST_DATASOURCE_PATH)" ]; then \
		test/e2e/gen-datasource.sh "$(if $(KIND_CLUSTER_NAME),$(KIND_CLUSTER_NAME),local-dev)" "$(UPTEST_DATASOURCE_PATH)"; \
	else \
		echo "e2e-datasource: UPTEST_DATASOURCE_PATH explicitly empty — skipping generation (mutation-check / opt-out path)"; \
	fi

uptest: e2e-datasource

# E2E preflight: validate NIOS Grid Manager credentials before running the
# (expensive) uptest pipeline.
e2e-preflight: ## Validate credentials before E2E
	@FREE=$$(df -BG --output=avail / | tail -1 | tr -dc '0-9'); \
	if [ "$$FREE" -lt "$(E2E_MIN_FREE_GB)" ]; then \
	  echo "ERROR: only $${FREE}GB free on / — E2E needs >= $(E2E_MIN_FREE_GB)GB." >&2; \
	  echo "  A full disk fails E2E as a BOGUS PROVIDER error 30+ min from now." >&2; \
	  echo "  Reclaim: docker image prune -f; rm -rf ~/.cache/grype;" >&2; \
	  echo "           find /tmp -maxdepth 1 \( -name 'go-build*' -o -name 'lint-home-*' \) \\" >&2; \
	  echo "                -type d -mmin +720 -exec rm -rf {} +" >&2; \
	  exit 1; \
	fi
	@echo "e2e-preflight: $(E2E_MIN_FREE_GB)GB minimum free space OK"
	@MISSING=""; \
	[ -z "$${INFOBLOX_HOST:-}" ] && MISSING="$$MISSING INFOBLOX_HOST"; \
	[ -z "$${INFOBLOX_USER:-}" ] && MISSING="$$MISSING INFOBLOX_USER"; \
	[ -z "$${INFOBLOX_PASS:-}" ] && MISSING="$$MISSING INFOBLOX_PASS"; \
	if [ -n "$$MISSING" ]; then \
	  echo "ERROR: E2E requires a real NIOS Grid Manager. Missing env var(s):$$MISSING" >&2; \
	  echo "  Set them as env vars or in ../.env" >&2; \
	  exit 1; \
	fi
	@echo "e2e-preflight: credentials OK"

# e2e-preflight must gate the chain, not trail it. build/makelib/uptest.mk
# declares `e2e: build controlplane.down controlplane.up ... uptest`, and a
# second `e2e:` rule here only APPENDS to that prerequisite list — GNU Make
# does not order prerequisites merged from separate rules for the same
# target, and `e2e` itself has no recipe of its own, so a prerequisite
# attached directly to `e2e` runs LAST, not first (proved empirically: the
# preflight lines appeared after the final test summary in a real run).
# `build` is the first prerequisite in uptest.mk's chain and DOES have a
# recipe (see build/makelib/common.mk), so attaching the guard here forces
# it to run — and to be able to abort the whole chain — before any image
# build or kind cluster is created. Do not move this back onto `e2e`.
#
# The prerequisite is conditional on the top-level goal actually being an
# E2E run. `make build` is the documented way to build this provider's
# binary (README "Developing" section, contributor workflow) and must
# succeed with no NIOS Grid Manager credentials present — it is not itself
# an E2E entry point. MAKECMDGOALS is fixed at parse time and `build` runs
# as a prerequisite inside the SAME make invocation as `e2e`/`e2e.<resource>`
# (uptest.mk lists it directly; it is not a recursive sub-make), so the
# filter below correctly sees the real top-level goal the user asked for.
# `e2e.%` covers every per-resource target alongside the bare `e2e`
# aggregate and `e2e-full`.
#
# tools.prefetch (below) rides the same attachment point and for the same
# reason: it too must run before controlplane.up touches $(KIND)/$(KUBECTL)/
# $(HELM), and `build` is the earliest recipe-bearing link in the chain.
build: $(if $(filter e2e e2e.% e2e-full,$(MAKECMDGOALS)),e2e-preflight tools.prefetch,)

# ====================================================================================
# E2E tool download retry
#
# build/makelib/k8s_tools.mk fetches every E2E tool binary (helm, kind,
# kubectl, kustomize, the Crossplane CLI, kuttl, chainsaw, uptest, yq) with
# a single bare `curl` and no retry. helm, kustomize, and chainsaw
# additionally pipe curl straight into tar, which loses curl's exit code to
# the pipeline (tar's own status wins) unless pipefail is set — a failed
# download there can proceed straight to a tar error that hides the real
# cause. A lone transient connection reset anywhere in that tool set aborts
# a 30+ minute E2E run before the kind cluster even exists, and the failure
# looks exactly like a real E2E failure in the log.
#
# build/ is the upstream crossplane/build submodule and must not be patched
# in place — a submodule bump would silently revert any fix made there.
# Instead, this target pre-populates the EXACT same target file paths
# k8s_tools.mk's own rules would produce ($(TOOLS_HOST_DIR)/<tool>-<version>,
# driven by the same version variables declared above), but downloads each
# one to a temp file first and extracts separately — never a bare
# `curl | tar` — so a failed transfer is never silently masked by a
# downstream tar/mv step. curl's own `--retry`/`--retry-all-errors` flags
# provide the retry-with-backoff (curl's default backoff between retries is
# exponential; passing `--retry-delay` would replace that with a fixed
# delay, so it is deliberately omitted here). `--remove-on-error` (curl
# 7.83+) covers the remaining gap: a server that announces a Content-Length
# and then truncates the body mid-transfer causes curl to retry and
# eventually fail loudly, but without this flag it leaves the partial file
# at the final -o path. The next `make` invocation's `test -f $(TOOL)`
# guard (and k8s_tools.mk's own file-target check) then see a file and skip
# the download entirely, handing a truncated binary to controlplane.up —
# turning today's loud failure into a confusing one on the next run.
# `--remove-on-error` deletes the partial output file whenever curl exits
# non-zero, so a truncated transfer leaves nothing behind and the next run
# retries the download instead of trusting corrupt bytes.
#
# Because $(KIND) et al. are ordinary file targets in k8s_tools.mk with no
# listed prerequisites, once the file exists on disk make treats that rule
# as already satisfied and never runs its retry-less recipe — this target
# only needs to win the race to create the file first, which the `build`
# attachment point above guarantees.
TOOL_FETCH_RETRIES ?= 3
CURL_RETRY_FLAGS := --retry $(TOOL_FETCH_RETRIES) --retry-all-errors --connect-timeout 15 --remove-on-error

.PHONY: tools.prefetch
tools.prefetch: ## Pre-fetch e2e tool binaries with retry (helm, kind, kubectl, kustomize, crossplane-cli, kuttl, chainsaw, uptest, yq)
	@mkdir -p $(TOOLS_HOST_DIR)
	@test -f $(KIND) || { $(INFO) "tools.prefetch: kind $(KIND_VERSION)"; \
		curl $(CURL_RETRY_FLAGS) -fsSLo $(KIND) https://github.com/kubernetes-sigs/kind/releases/download/$(KIND_VERSION)/kind-$(SAFEHOSTPLATFORM) \
		&& chmod +x $(KIND) || $(FAIL); }
	@test -f $(KUBECTL) || { $(INFO) "tools.prefetch: kubectl $(KUBECTL_VERSION)"; \
		curl $(CURL_RETRY_FLAGS) -fsSLo $(KUBECTL) https://dl.k8s.io/release/$(KUBECTL_VERSION)/bin/$(HOSTOS)/$(SAFEHOSTARCH)/kubectl \
		&& chmod +x $(KUBECTL) || $(FAIL); }
	@test -f $(CROSSPLANE_CLI) || { $(INFO) "tools.prefetch: crossplane-cli $(CROSSPLANE_CLI_VERSION)"; \
		curl $(CURL_RETRY_FLAGS) -fsSLo $(CROSSPLANE_CLI) "https://releases.crossplane.io/$(CROSSPLANE_CLI_CHANNEL)/$(CROSSPLANE_CLI_VERSION)/bin/$(SAFEHOST_PLATFORM)/crank?source=build" \
		&& chmod +x $(CROSSPLANE_CLI) || $(FAIL); }
	@test -f $(KUTTL) || { $(INFO) "tools.prefetch: kuttl $(KUTTL_VERSION)"; \
		curl $(CURL_RETRY_FLAGS) -fsSLo $(KUTTL) https://github.com/kudobuilder/kuttl/releases/download/v$(KUTTL_VERSION)/kubectl-kuttl_$(KUTTL_VERSION)_$(HOST_PLATFORM) \
		&& chmod +x $(KUTTL) || $(FAIL); }
	@test -f $(UPTEST) || { $(INFO) "tools.prefetch: uptest $(UPTEST_VERSION)"; \
		curl $(CURL_RETRY_FLAGS) -fsSLo $(UPTEST) https://github.com/crossplane/uptest/releases/download/$(UPTEST_VERSION)/uptest_$(SAFEHOSTPLATFORM) \
		&& chmod +x $(UPTEST) || $(FAIL); }
	@test -f $(YQ) || { $(INFO) "tools.prefetch: yq $(YQ_VERSION)"; \
		curl $(CURL_RETRY_FLAGS) -fsSLo $(YQ) https://github.com/mikefarah/yq/releases/download/$(YQ_VERSION)/yq_$(SAFEHOST_PLATFORM) \
		&& chmod +x $(YQ) || $(FAIL); }
	@test -f $(HELM) || { $(INFO) "tools.prefetch: helm $(HELM_VERSION)"; \
		tmp=$$(mktemp -d) && \
		curl $(CURL_RETRY_FLAGS) -fsSL -o $$tmp/helm.tar.gz https://get.helm.sh/helm-$(HELM_VERSION)-$(SAFEHOSTPLATFORM).tar.gz \
		&& tar -xzf $$tmp/helm.tar.gz -C $$tmp \
		&& mv $$tmp/$(SAFEHOSTPLATFORM)/helm $(HELM) \
		&& chmod +x $(HELM) \
		&& rm -rf $$tmp || $(FAIL); }
	@test -f $(KUSTOMIZE) || { $(INFO) "tools.prefetch: kustomize $(KUSTOMIZE_VERSION)"; \
		tmp=$$(mktemp -d) && \
		curl $(CURL_RETRY_FLAGS) -fsSL -o $$tmp/kustomize.tar.gz https://github.com/kubernetes-sigs/kustomize/releases/download/kustomize/$(KUSTOMIZE_VERSION)/kustomize_$(KUSTOMIZE_VERSION)_$(SAFEHOST_PLATFORM).tar.gz \
		&& tar -xzf $$tmp/kustomize.tar.gz -C $$tmp \
		&& mv $$tmp/kustomize $(KUSTOMIZE) \
		&& chmod +x $(KUSTOMIZE) \
		&& rm -rf $$tmp || $(FAIL); }
	@test -f $(CHAINSAW) || { $(INFO) "tools.prefetch: chainsaw $(CHAINSAW_VERSION)"; \
		tmp=$$(mktemp -d) && \
		curl $(CURL_RETRY_FLAGS) -fsSL -o $$tmp/chainsaw.tar.gz https://github.com/kyverno/chainsaw/releases/download/v$(CHAINSAW_VERSION)/chainsaw_$(SAFEHOST_PLATFORM).tar.gz \
		&& tar -xzf $$tmp/chainsaw.tar.gz -C $$tmp chainsaw \
		&& mv $$tmp/chainsaw $(CHAINSAW) \
		&& chmod +x $(CHAINSAW) \
		&& rm -rf $$tmp || $(FAIL); }
	@$(OK) "tools.prefetch: all e2e tool binaries present"

# Per-resource E2E targets
e2e.dns-view: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_DNS_VIEW)
e2e.dns-view: e2e

e2e.dtc-lbdn: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_DTC_LBDN)
e2e.dtc-lbdn: e2e

e2e.dtc-pool: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_DTC_POOL)
e2e.dtc-pool: e2e

e2e.dtc-server: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_DTC_SERVER)
e2e.dtc-server: e2e

e2e.extensible-attribute-def: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_EXTENSIBLE_ATTRIBUTE_DEF)
e2e.extensible-attribute-def: e2e

e2e.fixed-address-v6: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_FIXED_ADDRESS_V6)
e2e.fixed-address-v6: e2e

e2e.fixed-address: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_FIXED_ADDRESS)
e2e.fixed-address: e2e

e2e.host-record: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_HOST_RECORD)
e2e.host-record: e2e

e2e.ipv4-shared-network: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_IPV4_SHARED_NETWORK)
e2e.ipv4-shared-network: e2e

e2e.network-allocate: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_NETWORK_ALLOCATE)
e2e.network-allocate: e2e

e2e.network-container-v6: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_NETWORK_CONTAINER_V6)
e2e.network-container-v6: e2e

e2e.network-container: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_NETWORK_CONTAINER)
e2e.network-container: e2e

e2e.network-v6: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_NETWORK_V6)
e2e.network-v6: e2e

e2e.network-view: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_NETWORK_VIEW)
e2e.network-view: e2e

e2e.network: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_NETWORK)
e2e.network: e2e

e2e.range-template: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_RANGE_TEMPLATE),$(UPTEST_MANIFESTS_ZONE_AUTH)
e2e.range-template: e2e

e2e.range: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_RANGE)
e2e.range: e2e

e2e.record-a: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_RECORD_A)
e2e.record-a: e2e

e2e.record-aaaa: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_RECORD_AAAA)
e2e.record-aaaa: e2e

e2e.record-alias: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_RECORD_ALIAS)
e2e.record-alias: e2e

e2e.record-cname: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_RECORD_CNAME)
e2e.record-cname: e2e

e2e.record-mx: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_RECORD_MX)
e2e.record-mx: e2e

e2e.record-ns: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_RECORD_NS)
e2e.record-ns: e2e

e2e.record-ptr: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_RECORD_PTR)
e2e.record-ptr: e2e

e2e.record-srv: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_RECORD_SRV)
e2e.record-srv: e2e

e2e.record-txt: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_RECORD_TXT)
e2e.record-txt: e2e

e2e.zone-auth: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_ZONE_AUTH)
e2e.zone-auth: e2e

e2e.zone-delegated: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_ZONE_DELEGATED)
e2e.zone-delegated: e2e

e2e.zone-forward: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_ZONE_FORWARD)
e2e.zone-forward: e2e

# Full E2E: all resource examples (core + namespaced variants)
e2e-full: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_ALL)
e2e-full: e2e-preflight e2e

# local-deploy: build + load provider image into kind cluster, then deploy the xpkg.
# Called by `make e2e` via UPTEST_LOCAL_DEPLOY_TARGET=local-deploy.
# e2e-drc MUST stay first in this list — see its own comment above for why
# ordering (not merged prerequisites) is what makes DRC_FILE ready in time.
local-deploy: e2e-drc local.xpkg.deploy.provider.$(PROJECT_NAME)

.PHONY: e2e-full
.PHONY: e2e-preflight
.PHONY: e2e.dns-view
.PHONY: e2e.dtc-lbdn
.PHONY: e2e.dtc-pool
.PHONY: e2e.dtc-server
.PHONY: e2e.extensible-attribute-def
.PHONY: e2e.fixed-address
.PHONY: e2e.fixed-address-v6
.PHONY: e2e.host-record
.PHONY: e2e.ipv4-shared-network
.PHONY: e2e.network
.PHONY: e2e.network-allocate
.PHONY: e2e.network-container
.PHONY: e2e.network-container-v6
.PHONY: e2e.network-v6
.PHONY: e2e.network-view
.PHONY: e2e.range
.PHONY: e2e.range-template
.PHONY: e2e.record-a
.PHONY: e2e.record-aaaa
.PHONY: e2e.record-alias
.PHONY: e2e.record-cname
.PHONY: e2e.record-mx
.PHONY: e2e.record-ns
.PHONY: e2e.record-ptr
.PHONY: e2e.record-srv
.PHONY: e2e.record-txt
.PHONY: e2e.zone-auth
.PHONY: e2e.zone-delegated
.PHONY: e2e.zone-forward
.PHONY: local-deploy

# ====================================================================================
# Update-tester standalone targets (per-field update-tester convention)
#
# These invoke the update-tester tool directly (converge + per-field run)
# against both scope variants of each resource. They are separate from the
# uptest post-assert hook integration (which also runs update-tester as part
# of the full `make e2e.<resource>` Create→Update→Import→Delete cycle) — use
# these for a fast, standalone check against an already-deployed resource.
#
# The tool is consumed as a pinned module from tools/update-tester (a stub
# module holding only go.mod/go.sum — no vendored source), so there is no
# build step: `go -C` runs it directly from the module cache. Every manifest
# argument is wrapped in $(abspath …) because `go -C tools/update-tester`
# changes the child process's working directory, so a path that is correct
# from the repo root would otherwise resolve against tools/update-tester/.
UPDATE_TESTER := go -C tools/update-tester tool crossplane-update-tester

update-test.dns-view:
	$(UPDATE_TESTER) converge $(abspath examples/dns-view/dns-view.yaml)
	$(UPDATE_TESTER) run $(abspath examples/dns-view/dns-view.yaml)
	$(UPDATE_TESTER) converge $(abspath examples/dns-view/dns-view-namespaced.yaml)
	$(UPDATE_TESTER) run $(abspath examples/dns-view/dns-view-namespaced.yaml)

.PHONY: update-test.dns-view

update-test.dtc-lbdn:
	$(UPDATE_TESTER) converge $(abspath examples/dtc-lbdn/dtc-lbdn.yaml)
	$(UPDATE_TESTER) run $(abspath examples/dtc-lbdn/dtc-lbdn.yaml)
	$(UPDATE_TESTER) converge $(abspath examples/dtc-lbdn/dtc-lbdn-namespaced.yaml)
	$(UPDATE_TESTER) run $(abspath examples/dtc-lbdn/dtc-lbdn-namespaced.yaml)

.PHONY: update-test.dtc-lbdn

update-test.dtc-pool:
	$(UPDATE_TESTER) converge $(abspath examples/dtc-pool/dtc-pool.yaml)
	$(UPDATE_TESTER) run $(abspath examples/dtc-pool/dtc-pool.yaml)
	$(UPDATE_TESTER) converge $(abspath examples/dtc-pool/dtc-pool-namespaced.yaml)
	$(UPDATE_TESTER) run $(abspath examples/dtc-pool/dtc-pool-namespaced.yaml)

.PHONY: update-test.dtc-pool

update-test.dtc-server:
	$(UPDATE_TESTER) converge $(abspath examples/dtc-server/dtc-server.yaml)
	$(UPDATE_TESTER) run $(abspath examples/dtc-server/dtc-server.yaml)
	$(UPDATE_TESTER) converge $(abspath examples/dtc-server/dtc-server-namespaced.yaml)
	$(UPDATE_TESTER) run $(abspath examples/dtc-server/dtc-server-namespaced.yaml)

.PHONY: update-test.dtc-server

update-test.extensible-attribute-def:
	$(UPDATE_TESTER) converge $(abspath examples/extensible-attribute-def/extensible-attribute-def.yaml)
	$(UPDATE_TESTER) run $(abspath examples/extensible-attribute-def/extensible-attribute-def.yaml)
	$(UPDATE_TESTER) converge $(abspath examples/extensible-attribute-def/extensible-attribute-def-namespaced.yaml)
	$(UPDATE_TESTER) run $(abspath examples/extensible-attribute-def/extensible-attribute-def-namespaced.yaml)

.PHONY: update-test.extensible-attribute-def

update-test.fixed-address:
	$(UPDATE_TESTER) converge $(abspath examples/fixed-address/fixed-address.yaml)
	$(UPDATE_TESTER) run $(abspath examples/fixed-address/fixed-address.yaml)
	$(UPDATE_TESTER) converge $(abspath examples/fixed-address/fixed-address-namespaced.yaml)
	$(UPDATE_TESTER) run $(abspath examples/fixed-address/fixed-address-namespaced.yaml)

.PHONY: update-test.fixed-address

update-test.host-record:
	$(UPDATE_TESTER) converge $(abspath examples/host-record/host-record.yaml)
	$(UPDATE_TESTER) run $(abspath examples/host-record/host-record.yaml)
	$(UPDATE_TESTER) converge $(abspath examples/host-record/host-record-namespaced.yaml)
	$(UPDATE_TESTER) run $(abspath examples/host-record/host-record-namespaced.yaml)

.PHONY: update-test.host-record

update-test.ipv4-shared-network:
	$(UPDATE_TESTER) converge $(abspath examples/ipv4-shared-network/ipv4-shared-network.yaml)
	$(UPDATE_TESTER) run $(abspath examples/ipv4-shared-network/ipv4-shared-network.yaml)
	$(UPDATE_TESTER) converge $(abspath examples/ipv4-shared-network/ipv4-shared-network-namespaced.yaml)
	$(UPDATE_TESTER) run $(abspath examples/ipv4-shared-network/ipv4-shared-network-namespaced.yaml)

.PHONY: update-test.ipv4-shared-network

update-test.network-container:
	$(UPDATE_TESTER) converge $(abspath examples/network-container/network-container.yaml)
	$(UPDATE_TESTER) run $(abspath examples/network-container/network-container.yaml)
	$(UPDATE_TESTER) converge $(abspath examples/network-container/network-container-namespaced.yaml)
	$(UPDATE_TESTER) run $(abspath examples/network-container/network-container-namespaced.yaml)

.PHONY: update-test.network-container

update-test.network-view:
	$(UPDATE_TESTER) converge $(abspath examples/network-view/network-view.yaml)
	$(UPDATE_TESTER) run $(abspath examples/network-view/network-view.yaml)
	$(UPDATE_TESTER) converge $(abspath examples/network-view/network-view-namespaced.yaml)
	$(UPDATE_TESTER) run $(abspath examples/network-view/network-view-namespaced.yaml)

.PHONY: update-test.network-view

update-test.network:
	$(UPDATE_TESTER) converge $(abspath examples/network/network.yaml)
	$(UPDATE_TESTER) run $(abspath examples/network/network.yaml)
	$(UPDATE_TESTER) converge $(abspath examples/network/network-namespaced.yaml)
	$(UPDATE_TESTER) run $(abspath examples/network/network-namespaced.yaml)

.PHONY: update-test.network

update-test.range-template:
	$(UPDATE_TESTER) converge $(abspath examples/range-template/range-template.yaml)
	$(UPDATE_TESTER) run $(abspath examples/range-template/range-template.yaml)
	$(UPDATE_TESTER) converge $(abspath examples/range-template/range-template-namespaced.yaml)
	$(UPDATE_TESTER) run $(abspath examples/range-template/range-template-namespaced.yaml)

.PHONY: update-test.range-template

update-test.range:
	$(UPDATE_TESTER) converge $(abspath examples/range/range.yaml)
	$(UPDATE_TESTER) run $(abspath examples/range/range.yaml)
	$(UPDATE_TESTER) converge $(abspath examples/range/range-namespaced.yaml)
	$(UPDATE_TESTER) run $(abspath examples/range/range-namespaced.yaml)

.PHONY: update-test.range

update-test.record-aaaa:
	$(UPDATE_TESTER) converge $(abspath examples/record-aaaa/record-aaaa.yaml)
	$(UPDATE_TESTER) run $(abspath examples/record-aaaa/record-aaaa.yaml)
	$(UPDATE_TESTER) converge $(abspath examples/record-aaaa/record-aaaa-namespaced.yaml)
	$(UPDATE_TESTER) run $(abspath examples/record-aaaa/record-aaaa-namespaced.yaml)

.PHONY: update-test.record-aaaa

update-test.record-alias:
	$(UPDATE_TESTER) converge $(abspath examples/record-alias/record-alias.yaml)
	$(UPDATE_TESTER) run $(abspath examples/record-alias/record-alias.yaml)
	$(UPDATE_TESTER) converge $(abspath examples/record-alias/record-alias-namespaced.yaml)
	$(UPDATE_TESTER) run $(abspath examples/record-alias/record-alias-namespaced.yaml)

.PHONY: update-test.record-alias

update-test.record-cname: ## Update test for CNAMERecord (per-field convergence check)
	$(UPDATE_TESTER) converge $(abspath examples/record-cname/record-cname.yaml)
	$(UPDATE_TESTER) run $(abspath examples/record-cname/record-cname.yaml)
	$(UPDATE_TESTER) converge $(abspath examples/record-cname/record-cname-namespaced.yaml)
	$(UPDATE_TESTER) run $(abspath examples/record-cname/record-cname-namespaced.yaml)

.PHONY: update-test.record-cname

update-test.record-mx:
	$(UPDATE_TESTER) converge $(abspath examples/record-mx/record-mx.yaml)
	$(UPDATE_TESTER) run $(abspath examples/record-mx/record-mx.yaml)
	$(UPDATE_TESTER) converge $(abspath examples/record-mx/record-mx-namespaced.yaml)
	$(UPDATE_TESTER) run $(abspath examples/record-mx/record-mx-namespaced.yaml)

.PHONY: update-test.record-mx

update-test.record-ns:
	$(UPDATE_TESTER) converge $(abspath examples/record-ns/record-ns.yaml)
	$(UPDATE_TESTER) run $(abspath examples/record-ns/record-ns.yaml)
	$(UPDATE_TESTER) converge $(abspath examples/record-ns/record-ns-namespaced.yaml)
	$(UPDATE_TESTER) run $(abspath examples/record-ns/record-ns-namespaced.yaml)

.PHONY: update-test.record-ns

update-test.record-ptr:
	$(UPDATE_TESTER) converge $(abspath examples/record-ptr/record-ptr.yaml)
	$(UPDATE_TESTER) run $(abspath examples/record-ptr/record-ptr.yaml)
	$(UPDATE_TESTER) converge $(abspath examples/record-ptr/record-ptr-namespaced.yaml)
	$(UPDATE_TESTER) run $(abspath examples/record-ptr/record-ptr-namespaced.yaml)

.PHONY: update-test.record-ptr

update-test.record-srv:
	$(UPDATE_TESTER) converge $(abspath examples/record-srv/record-srv.yaml)
	$(UPDATE_TESTER) run $(abspath examples/record-srv/record-srv.yaml)
	$(UPDATE_TESTER) converge $(abspath examples/record-srv/record-srv-namespaced.yaml)
	$(UPDATE_TESTER) run $(abspath examples/record-srv/record-srv-namespaced.yaml)

.PHONY: update-test.record-srv

update-test.record-txt:
	$(UPDATE_TESTER) converge $(abspath examples/record-txt/record-txt.yaml)
	$(UPDATE_TESTER) run $(abspath examples/record-txt/record-txt.yaml)
	$(UPDATE_TESTER) converge $(abspath examples/record-txt/record-txt-namespaced.yaml)
	$(UPDATE_TESTER) run $(abspath examples/record-txt/record-txt-namespaced.yaml)

.PHONY: update-test.record-txt

# ====================================================================================
# update-test.validate: annotation-coverage gate (the update-tester validate convention)
#
# Runs `update-tester validate` against every example manifest that carries a
# crossplane.io/update-test annotation, so a field silently dropped from an
# annotation is caught instead of going unnoticed. Unlike update-test.<resource>
# above (which exercises the live update behavior against a deployed resource),
# this is a static, offline check — it only reads the example YAML and the
# generated Go types, so it belongs in the fast local dev loop.
#
# The --types-file for each manifest is resolved from its apiVersion: the
# first dot-separated segment of the API group is both the apis/<scope>/<seg>
# directory name and the <seg>_types.go file prefix. The scope is namespaced
# when the API group ends in ".m.crossplane.io" (the "m" marker is a fixed
# group suffix, not tied to a fixed segment position — e.g.
# recordcname.infobloxnios.m.crossplane.io) and cluster otherwise (e.g.
# recordcname.infobloxnios.crossplane.io). A self-check cross-references the
# manifest filename convention (*-namespaced.yaml) against the resolved scope
# so a future naming drift fails loudly instead of silently validating
# against the wrong types file.
update-test.validate:
	@fail=0; \
	for f in $$(grep -rl 'crossplane.io/update-test' examples --include='*.yaml' | sort); do \
	  av=$$(awk '/^apiVersion:/ {print $$2; exit}' "$$f"); \
	  grp=$$(echo "$$av" | cut -d/ -f1); \
	  seg1=$$(echo "$$grp" | cut -d. -f1); \
	  case "$$grp" in \
	    *.m.crossplane.io) scope=namespaced ;; \
	    *) scope=cluster ;; \
	  esac; \
	  case "$$f" in \
	    *-namespaced.yaml) \
	      if [ "$$scope" != "namespaced" ]; then \
	        echo "FAIL: $$f looks namespaced by filename but resolved scope=$$scope from apiVersion=$$av"; \
	        fail=1; \
	        continue; \
	      fi ;; \
	    *) \
	      if [ "$$scope" != "cluster" ]; then \
	        echo "FAIL: $$f looks cluster-scoped by filename but resolved scope=$$scope from apiVersion=$$av"; \
	        fail=1; \
	        continue; \
	      fi ;; \
	  esac; \
	  types="apis/$$scope/$$seg1/v1alpha1/$${seg1}_types.go"; \
	  if [ ! -f "$$types" ]; then \
	    echo "SKIP: $$f — no types file at $$types (apiVersion=$$av)"; \
	    fail=1; \
	    continue; \
	  fi; \
	  echo "=== $$f ($$types) ==="; \
	  $(UPDATE_TESTER) validate --types-file "$$PWD/$$types" "$$PWD/$$f" || fail=1; \
	done; \
	exit $$fail

.PHONY: update-test.validate

update-test.zone-auth:
	$(UPDATE_TESTER) converge $(abspath examples/zone-auth/zone-auth.yaml)
	$(UPDATE_TESTER) run $(abspath examples/zone-auth/zone-auth.yaml)
	$(UPDATE_TESTER) converge $(abspath examples/zone-auth/zone-auth-namespaced.yaml)
	$(UPDATE_TESTER) run $(abspath examples/zone-auth/zone-auth-namespaced.yaml)

.PHONY: update-test.zone-auth

update-test.zone-delegated:
	$(UPDATE_TESTER) converge $(abspath examples/zone-delegated/zone-delegated.yaml)
	$(UPDATE_TESTER) run $(abspath examples/zone-delegated/zone-delegated.yaml)
	$(UPDATE_TESTER) converge $(abspath examples/zone-delegated/zone-delegated-namespaced.yaml)
	$(UPDATE_TESTER) run $(abspath examples/zone-delegated/zone-delegated-namespaced.yaml)

.PHONY: update-test.zone-delegated

update-test.zone-forward:
	$(UPDATE_TESTER) converge $(abspath examples/zone-forward/zone-forward.yaml)
	$(UPDATE_TESTER) run $(abspath examples/zone-forward/zone-forward.yaml)
	$(UPDATE_TESTER) converge $(abspath examples/zone-forward/zone-forward-namespaced.yaml)
	$(UPDATE_TESTER) run $(abspath examples/zone-forward/zone-forward-namespaced.yaml)

.PHONY: update-test.zone-forward

# ====================================================================================
# test.tools: unit tests for standalone tool modules, root-module tools/
# packages, and hook scripts
#
# Three passes, none of which overlap:
#  1. Root-module tools/ packages via `go test ./tools/...`. `go list ./...`
#     in the root module automatically excludes directories that declare
#     their own go.mod, so this reaches exactly the tools/ code that lives
#     in the root module (e.g. tools/openapi, tools/openapi2crd) without
#     double-running anything the next loop covers.
#  2. Every tools/*/go.mod directory and every test/hooks/*_test.sh,
#     running whichever exist. A tools/*/ module that declares only a `tool`
#     directive (e.g. tools/update-tester, a stub module with no Go files of
#     its own) has nothing to test — `go test ./...` exits non-zero in a
#     package-less module, so it is skipped via a `go list ./...` guard
#     rather than being allowed to fail the recipe.
test.tools: ## Run unit tests for tools/*/ modules, root-module tools/ packages, and test/hooks/*_test.sh
	@$(INFO) go test ./tools/...
	@go test ./tools/... -count=1 || $(FAIL)
	@rc=0; for d in $$(find tools -name go.mod -not -path '*/vendor/*' -exec dirname {} \;); do \
		[ -n "$$(cd $$d && go list ./... 2>/dev/null)" ] || continue; \
		$(INFO) go test $$d; \
		(cd $$d && go test ./... -count=1) || rc=1; \
	done; [ $$rc -eq 0 ] || $(FAIL)
	@for s in test/hooks/*_test.sh; do \
		[ -e "$$s" ] || continue; \
		$(INFO) running $$s; \
		bash "$$s" || exit 1; \
	done
	@$(OK) tool tests passed

.PHONY: test.tools

# Fold test.tools into the documented DoD gate so a broken tool-module test
# blocks review the same way a broken provider test does.
reviewable: test.tools

# Fold check-conventions into the documented DoD gate so a convention
# violation blocks review the same way a failing test does.
reviewable: check-conventions

# ====================================================================================
# Convention Checks (test naming, kubectl env override, and error-wrapping
# style — enforced here so a violation fails the build, not just review)

check-conventions: ## Detect convention violations (test names, error wrapping, kubectl usage)
	@# PascalCase test names: no underscores after "Test"
	@! grep -rn 'func Test.*_' --include='*_test.go' . 2>/dev/null || \
	  (echo "FAIL: test names must use PascalCase (no underscores)" && exit 1)
	@# KUBECTL env var: test scripts must use $${KUBECTL:-kubectl}, never a bare
	@# `kubectl` invocation. Three passes over each test script keep this to
	@# genuine invocations: comment lines are blanked (not deleted, so line
	@# numbers stay accurate), double-quoted string literals are stripped (so
	@# prose inside a quoted message like `assert_true "... kubectl" "$$rc"`
	@# can't match), and a word boundary is required before `kubectl` (so
	@# `_kubectl` inside a longer identifier, e.g. `__stub_kubectl_empty`,
	@# doesn't match). A `KUBECTL:-kubectl` backstop catches the one shape
	@# quote-stripping and the word boundary both miss: an unquoted
	@# `KUBECTL=$${KUBECTL:-kubectl}` assignment.
	@bad=0; \
	for f in $$(find test/ -name '*.sh' 2>/dev/null); do \
	  m=$$(sed -E 's/^[[:space:]]*#.*$$//' "$$f" | sed -E 's/"[^"]*"//g' \
	    | grep -nE '(^|[^[:alnum:]_$$])kubectl' | grep -v 'KUBECTL:-kubectl'); \
	  [ -z "$$m" ] && continue; \
	  for ln in $$(echo "$$m" | cut -d: -f1); do \
	    echo "$$f:$$ln:$$(sed -n "$${ln}p" "$$f")"; \
	    bad=1; \
	  done; \
	done; \
	if [ "$$bad" -eq 1 ]; then echo "FAIL: use \$${KUBECTL:-kubectl} instead of bare kubectl"; exit 1; fi
	@# Error wrapping: no fmt.Errorf in production code. Comment lines are
	@# filtered out so the controllers' own "never fmt.Errorf" doc comments
	@# do not trip the check that documents them.
	@! grep -rn 'fmt\.Errorf' internal/ --include='*.go' 2>/dev/null \
	    | grep -v '_test.go' \
	    | grep -vE '^[^:]+:[0-9]+:[[:space:]]*//' || \
	  (echo "FAIL: use errors.Wrap/Errorf from crossplane-runtime, not fmt.Errorf" && exit 1)
	@# No bare stdlib "errors" import (match import lines only, not JSON struct tags).
	@! grep -rn '^\s*"errors"\s*$$' internal/ --include='*.go' 2>/dev/null | grep -v '_test.go' || \
	  (echo "FAIL: use crossplane-runtime/v2/pkg/errors, not stdlib \"errors\"" && exit 1)
	@echo "check-conventions: all checks passed"

.PHONY: check-conventions

# ====================================================================================
# Publishing
#
# The crossplane/build xpkg machinery publishes via `crossplane xpkg push`,
# which authenticates through the Docker keychain (~/.docker/config.json). We
# publish with the `up` CLI instead so CI can authenticate with an Upbound API
# token -- the same UP_API_TOKEN / UP_ORG wiring the Upbound configuration
# packages use, and because `up xpkg push --create` will create the registry
# repository on first push.
#
# NOTE: the `up login` performed by upbound/action-up is NOT what authenticates
# the push. UP_API_TOKEN is a robot token, and up's RegistryKeychain falls back
# to the Docker keychain for robot tokens ("robot tokens cannot be used for
# registry login 401 Unauthorized"), so CI must additionally `docker login`
# to xpkg.upbound.io with UP_ROBOT_ID as the username. See the "Login to xpkg
# with robot" step in .github/workflows/release.yaml.
UP ?= up

xpkg.push.up: ## Push the built xpkg to xpkg.upbound.io using the up CLI.
	@$(INFO) Pushing package $(XPKG_REG_ORGS)/$(PROJECT_NAME):$(VERSION)
	@$(UP) xpkg push --create \
		$(foreach p,$(XPKG_LINUX_PLATFORMS),-f $(XPKG_OUTPUT_DIR)/$(p)/$(PROJECT_NAME)-$(VERSION).xpkg) \
		$(XPKG_REG_ORGS)/$(PROJECT_NAME):$(VERSION) || $(FAIL)
	@$(OK) Pushed package $(XPKG_REG_ORGS)/$(PROJECT_NAME):$(VERSION)

.PHONY: xpkg.push.up

# Legacy integration tests (disabled — the provider now uses uptest/chainsaw
# for E2E via uptest.mk, above). Removing the e2e.run: test-integration
# override lets common.mk's no-op e2e.run apply, so `make e2e.<resource>`
# exits 0 after uptest passes instead of re-running a second, unrelated kind
# cluster lifecycle.
#
# e2e.run: test-integration
#
# test-integration: $(KIND) $(KUBECTL) $(CROSSPLANE_CLI) $(HELM3)
# 	@$(INFO) running integration tests using kind $(KIND_VERSION)
# 	@KIND_NODE_IMAGE_TAG=${KIND_NODE_IMAGE_TAG} $(ROOT_DIR)/cluster/local/integration_tests.sh || $(FAIL)
# 	@$(OK) integration tests passed

submodules:
	@git submodule sync
	@git submodule update --init --recursive

# NOTE(hasheddan): the build submodule currently overrides XDG_CACHE_HOME in
# order to force the Helm 3 to use the .work/helm directory. This causes Go on
# Linux machines to use that directory as the build cache as well. We should
# adjust this behavior in the build submodule because it is also causing Linux
# users to duplicate their build cache, but for now we just make it easier to
# identify its location in CI so that we cache between builds.
go.cachedir:
	@go env GOCACHE

go.mod.cachedir:
	@go env GOMODCACHE

# NOTE(hasheddan): we must ensure up is installed in tool cache prior to build
# as including the k8s_tools machinery prior to the xpkg machinery sets UP to
# point to tool cache.
build.init: $(CROSSPLANE_CLI)

# This is for running out-of-cluster locally, and is for convenience. Running
# this make target will print out the command which was used. For more control,
# try running the binary directly with different arguments.
run: go.build
	@$(INFO) Running Crossplane locally out-of-cluster . . .
	@# To see other arguments that can be provided, run the command with --help instead
	$(GO_OUT_DIR)/provider --debug

dev: $(KIND) $(KUBECTL)
	@$(INFO) Creating kind cluster
	@$(KIND) create cluster --name=$(PROJECT_NAME)-dev
	@$(KUBECTL) cluster-info --context kind-$(PROJECT_NAME)-dev
	@$(INFO) Installing Provider Infobloxnios CRDs
	@$(KUBECTL) apply -R -f package/crds
	@$(INFO) Starting Provider Infobloxnios controllers
	@$(GO) run cmd/provider/main.go --debug

dev-clean: $(KIND) $(KUBECTL)
	@$(INFO) Deleting kind cluster
	@$(KIND) delete cluster --name=$(PROJECT_NAME)-dev

.PHONY: submodules fallthrough test-integration run dev dev-clean

# ====================================================================================
# Registration generation (the auto-generated registration convention)

# Generate scheme and controller registration files from directory structure.
# Run this after adding a new resource API or controller package.
generate-registration:
	go run hack/generate-registration.go $(PROJECT_REPO)

# Verify that generated registration files match what the generator would produce.
# Fails if hack/generate-registration.go has not been run after a directory change.
check-diff: generate-registration
	@git diff --exit-code apis/zz_generated_register.go internal/controller/zz_generated_register.go || \
	  (echo "ERROR: generated registration files are out of date — run 'make generate-registration'" && exit 1)

# generate.done is the post-generation hook in the common.mk chain:
#   generate.init → generate.run (go.generate → go generate ./...) → generate.done
# By overriding generate.done here (instead of adding a prerequisite to `generate`),
# we guarantee goimports runs AFTER controller-gen deepcopy generation, eliminating
# the empty `import ()` blocks that controller-gen emits for types with no imported deps.
generate.done: generate-registration
	go tool goimports -w .

.PHONY: generate-registration generate.done check-diff

# ====================================================================================
# Special Targets

# Install gomplate
GOMPLATE_VERSION := 3.10.0
GOMPLATE := $(TOOLS_HOST_DIR)/gomplate-$(GOMPLATE_VERSION)

$(GOMPLATE):
	@$(INFO) installing gomplate $(SAFEHOSTPLATFORM)
	@mkdir -p $(TOOLS_HOST_DIR)
	@curl -fsSLo $(GOMPLATE) https://github.com/hairyhenderson/gomplate/releases/download/v$(GOMPLATE_VERSION)/gomplate_$(SAFEHOSTPLATFORM) || $(FAIL)
	@chmod +x $(GOMPLATE)
	@$(OK) installing gomplate $(SAFEHOSTPLATFORM)

export GOMPLATE

# This target prepares repo for your provider by replacing all "infobloxnios"
# occurrences with your provider name.
# This target can only be run once, if you want to rerun for some reason,
# consider stashing/resetting your git state.
# Arguments:
#   provider: Camel case name of your provider, e.g. GitHub, PlanetScale
provider.prepare:
	@[ "${provider}" ] || ( echo "argument \"provider\" is not set"; exit 1 )
	@PROVIDER=$(provider) ./hack/helpers/prepare.sh

# This target adds a new api type and its controller.
# You would still need to register new api in "apis/<provider>.go" and
# controller in "internal/controller/<provider>.go".
# Arguments:
#   provider: Camel case name of your provider, e.g. GitHub, PlanetScale
#   group: API group for the type you want to add.
#   kind: Kind of the type you want to add
#	apiversion: API version of the type you want to add. Optional and defaults to "v1alpha1"
provider.addtype: $(GOMPLATE)
	@[ "${provider}" ] || ( echo "argument \"provider\" is not set"; exit 1 )
	@[ "${group}" ] || ( echo "argument \"group\" is not set"; exit 1 )
	@[ "${kind}" ] || ( echo "argument \"kind\" is not set"; exit 1 )
	@PROVIDER=$(provider) GROUP=$(group) KIND=$(kind) APIVERSION=$(apiversion) PROJECT_REPO=$(PROJECT_REPO) ./hack/helpers/addtype.sh

define CROSSPLANE_MAKE_HELP
Crossplane Targets:
    submodules            Update the submodules, such as the common build scripts.
    run                   Run crossplane locally, out-of-cluster. Useful for development.

endef
# The reason CROSSPLANE_MAKE_HELP is used instead of CROSSPLANE_HELP is because the crossplane
# binary will try to use CROSSPLANE_HELP if it is set, and this is for something different.
export CROSSPLANE_MAKE_HELP

crossplane.help:
	@echo "$$CROSSPLANE_MAKE_HELP"

help-special: crossplane.help

.PHONY: crossplane.help help-special
