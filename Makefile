# ====================================================================================
# Setup Project
PROJECT_NAME := provider-infobloxnios
PROJECT_REPO := github.com/crossplane-contrib/provider-infoblox-nios

PLATFORMS ?= linux_amd64 linux_arm64

# Source .env credentials for E2E tests if present (optional fallback).
# Primary credential source: Hive nest secrets / CI secrets (env vars).
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
GO_SUBDIRS += cmd internal apis
GO111MODULE = on
GOLANGCILINT_VERSION = 2.11.4
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

XPKG_REG_ORGS ?= xpkg.upbound.io/crossplane-contrib
# NOTE(hasheddan): skip promoting on xpkg.upbound.io as channel tags are
# inferred.
XPKG_REG_ORGS_NO_PROMOTE ?= xpkg.upbound.io/crossplane-contrib
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

# Helper variables for comma-separated manifest lists (uptest CLI convention —
# `uptest e2e` takes a single comma-separated string; GNU Make's $(wildcard)
# produces space-separated lists).
comma := ,
empty :=
space := $(empty) $(empty)

# Per-resource manifest variables (comma pair of cluster + namespaced variants,
# so `make e2e.<resource>` gates both scopes).
UPTEST_MANIFESTS_RECORD_A := examples/record-a/record-a.yaml,examples/record-a/record-a-namespaced.yaml
UPTEST_MANIFESTS_RECORD_AAAA := examples/record-aaaa/record-aaaa.yaml,examples/record-aaaa/record-aaaa-namespaced.yaml
UPTEST_MANIFESTS_RECORD_ALIAS := examples/record-alias/record-alias.yaml,examples/record-alias/record-alias-namespaced.yaml
UPTEST_MANIFESTS_RECORD_PTR := examples/record-ptr/record-ptr.yaml,examples/record-ptr/record-ptr-namespaced.yaml
UPTEST_MANIFESTS_RECORD_TXT := examples/record-txt/record-txt.yaml,examples/record-txt/record-txt-namespaced.yaml
UPTEST_MANIFESTS_ZONE_DELEGATED := examples/zone-delegated/zone-delegated.yaml,examples/zone-delegated/zone-delegated-namespaced.yaml
UPTEST_MANIFESTS_RECORD_CNAME := examples/record-cname/record-cname.yaml,examples/record-cname/record-cname-namespaced.yaml
UPTEST_MANIFESTS_RECORD_MX := examples/record-mx/record-mx.yaml,examples/record-mx/record-mx-namespaced.yaml
UPTEST_MANIFESTS_RECORD_NS := examples/record-ns/record-ns.yaml,examples/record-ns/record-ns-namespaced.yaml
UPTEST_MANIFESTS_RECORD_SRV := examples/record-srv/record-srv.yaml,examples/record-srv/record-srv-namespaced.yaml
UPTEST_MANIFESTS_NETWORK_VIEW := examples/network-view/network-view.yaml,examples/network-view/network-view-namespaced.yaml
UPTEST_MANIFESTS_HOST_RECORD := examples/host-record/host-record.yaml,examples/host-record/host-record-namespaced.yaml
UPTEST_MANIFESTS_NETWORK := examples/network/network.yaml,examples/network/network-namespaced.yaml
UPTEST_MANIFESTS_RANGE_TEMPLATE := examples/range-template/range-template.yaml,examples/range-template/range-template-namespaced.yaml
UPTEST_MANIFESTS_ZONE_AUTH := examples/zone-auth/zone-auth.yaml,examples/zone-auth/zone-auth-namespaced.yaml
UPTEST_MANIFESTS_IPV4_SHARED_NETWORK := examples/ipv4-shared-network/ipv4-shared-network.yaml,examples/ipv4-shared-network/ipv4-shared-network-namespaced.yaml
# NetworkContainer references NetworkView by name (networkViewRef/Selector
# also available) but both example manifests use the Grid's well-known
# "default" network view inline, so no NetworkView prerequisite manifest is
# prepended here — same pattern as UPTEST_MANIFESTS_NETWORK above.
UPTEST_MANIFESTS_NETWORK_CONTAINER := examples/network-container/network-container.yaml,examples/network-container/network-container-namespaced.yaml
UPTEST_MANIFESTS_ZONE_FORWARD := examples/zone-forward/zone-forward.yaml,examples/zone-forward/zone-forward-namespaced.yaml
UPTEST_MANIFESTS_FIXED_ADDRESS := examples/fixed-address/fixed-address.yaml,examples/fixed-address/fixed-address-namespaced.yaml
UPTEST_MANIFESTS_RANGE := examples/range/range.yaml,examples/range/range-namespaced.yaml
UPTEST_MANIFESTS_DTC_SERVER := examples/dtc-server/dtc-server.yaml,examples/dtc-server/dtc-server-namespaced.yaml
UPTEST_MANIFESTS_EXTENSIBLE_ATTRIBUTE_DEF := examples/extensible-attribute-def/extensible-attribute-def.yaml,examples/extensible-attribute-def/extensible-attribute-def-namespaced.yaml
UPTEST_MANIFESTS_DNS_VIEW := examples/dns-view/dns-view.yaml,examples/dns-view/dns-view-namespaced.yaml
UPTEST_MANIFESTS_DTC_POOL := examples/dtc-pool/dtc-pool.yaml,examples/dtc-pool/dtc-pool-namespaced.yaml
UPTEST_MANIFESTS_DTC_LBDN := examples/dtc-lbdn/dtc-lbdn.yaml,examples/dtc-lbdn/dtc-lbdn-namespaced.yaml

# E2E manifest tiers
# CORE: resources that only need NIOS Grid Manager API credentials
# (INFOBLOX_HOST, INFOBLOX_USER, INFOBLOX_PASS) — no additional external
# infrastructure to provision. This is the full core-tier set: all record
# types, HostRecord, Network(View/Container), RangeTemplate, ZoneAuth,
# ZoneDelegated, ZoneForward, IPv4SharedNetwork, FixedAddress, Range,
# DTCServer, DTCPool, DTCLBDN, NSRecord, and ExtensibleAttributeDef. DTCPool and
# DTCLBDN need no external prerequisites — their `servers`/`pools`/
# `authZones` fields are optional.
UPTEST_MANIFESTS_CORE = $(UPTEST_MANIFESTS_RECORD_A),$(UPTEST_MANIFESTS_RECORD_AAAA),$(UPTEST_MANIFESTS_RECORD_ALIAS),$(UPTEST_MANIFESTS_RECORD_CNAME),$(UPTEST_MANIFESTS_RECORD_MX),$(UPTEST_MANIFESTS_RECORD_NS),$(UPTEST_MANIFESTS_RECORD_PTR),$(UPTEST_MANIFESTS_RECORD_SRV),$(UPTEST_MANIFESTS_RECORD_TXT),$(UPTEST_MANIFESTS_ZONE_DELEGATED),$(UPTEST_MANIFESTS_NETWORK_VIEW),$(UPTEST_MANIFESTS_HOST_RECORD),$(UPTEST_MANIFESTS_NETWORK),$(UPTEST_MANIFESTS_RANGE_TEMPLATE),$(UPTEST_MANIFESTS_ZONE_AUTH),$(UPTEST_MANIFESTS_IPV4_SHARED_NETWORK),$(UPTEST_MANIFESTS_NETWORK_CONTAINER),$(UPTEST_MANIFESTS_FIXED_ADDRESS),$(UPTEST_MANIFESTS_ZONE_FORWARD),$(UPTEST_MANIFESTS_RANGE),$(UPTEST_MANIFESTS_DTC_SERVER),$(UPTEST_MANIFESTS_EXTENSIBLE_ATTRIBUTE_DEF),$(UPTEST_MANIFESTS_DNS_VIEW),$(UPTEST_MANIFESTS_DTC_POOL),$(UPTEST_MANIFESTS_DTC_LBDN)

# UPTEST_MANIFESTS_ALL: discover all resource examples, excluding provider/ config.
# Produces a comma-separated list for `uptest e2e` (the unified example-manifest convention).
_UPTEST_MANIFESTS_ALL_RAW := $(filter-out examples/provider/%,$(wildcard examples/*/*.yaml))
UPTEST_MANIFESTS_ALL := $(subst $(space),$(comma),$(_UPTEST_MANIFESTS_ALL_RAW))

# Default e2e input: CORE manifests
UPTEST_INPUT_MANIFESTS ?= $(UPTEST_MANIFESTS_CORE)

UPTEST_SETUP_SCRIPT ?= test/setup.sh

# Per-run uptest test-directory isolation (each concurrent E2E run gets its own
# staging directory) — without this, concurrent E2E runs share /tmp/uptest-e2e
# and corrupt each other's staged chainsaw test files.
ifdef KIND_CLUSTER_NAME
  UPTEST_ARGS += --test-directory=/tmp/uptest-e2e-$(KIND_CLUSTER_NAME)
endif

-include build/makelib/uptest.mk

# E2E preflight: validate NIOS Grid Manager credentials before running the
# (expensive) uptest pipeline.
e2e-preflight: ## Validate credentials before E2E
	@MISSING=""; \
	[ -z "$${INFOBLOX_HOST:-}" ] && MISSING="$$MISSING INFOBLOX_HOST"; \
	[ -z "$${INFOBLOX_USER:-}" ] && MISSING="$$MISSING INFOBLOX_USER"; \
	[ -z "$${INFOBLOX_PASS:-}" ] && MISSING="$$MISSING INFOBLOX_PASS"; \
	if [ -n "$$MISSING" ]; then \
	  echo "ERROR: E2E requires a real NIOS Grid Manager. Missing env var(s):$$MISSING" >&2; \
	  echo "  Set them as env vars (Hive nest secrets) or in ../.env" >&2; \
	  exit 1; \
	fi
	@echo "e2e-preflight: credentials OK"

e2e: e2e-preflight

# Per-resource E2E targets
e2e.record-a: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_RECORD_A)
e2e.record-a: e2e

e2e.record-aaaa: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_RECORD_AAAA)
e2e.record-aaaa: e2e

e2e.record-alias: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_RECORD_ALIAS)
e2e.record-alias: e2e

e2e.record-ptr: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_RECORD_PTR)
e2e.record-ptr: e2e

e2e.record-txt: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_RECORD_TXT)
e2e.record-txt: e2e

e2e.zone-delegated: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_ZONE_DELEGATED)
e2e.zone-delegated: e2e

e2e.record-cname: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_RECORD_CNAME)
e2e.record-cname: e2e

e2e.record-mx: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_RECORD_MX)
e2e.record-mx: e2e

e2e.record-ns: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_RECORD_NS)
e2e.record-ns: e2e

e2e.record-srv: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_RECORD_SRV)
e2e.record-srv: e2e

e2e.network-view: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_NETWORK_VIEW)
e2e.network-view: e2e
e2e.host-record: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_HOST_RECORD)
e2e.host-record: e2e

e2e.network: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_NETWORK)
e2e.network: e2e

e2e.range-template: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_RANGE_TEMPLATE),$(UPTEST_MANIFESTS_ZONE_AUTH)
e2e.range-template: e2e

e2e.zone-auth: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_ZONE_AUTH)
e2e.zone-auth: e2e

e2e.ipv4-shared-network: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_IPV4_SHARED_NETWORK)
e2e.ipv4-shared-network: e2e

e2e.network-container: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_NETWORK_CONTAINER)
e2e.network-container: e2e
e2e.zone-forward: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_ZONE_FORWARD)
e2e.zone-forward: e2e

e2e.fixed-address: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_FIXED_ADDRESS)
e2e.fixed-address: e2e
e2e.range: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_RANGE)
e2e.range: e2e
e2e.dtc-server: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_DTC_SERVER)
e2e.dtc-server: e2e
e2e.extensible-attribute-def: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_EXTENSIBLE_ATTRIBUTE_DEF)
e2e.extensible-attribute-def: e2e

e2e.dns-view: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_DNS_VIEW)
e2e.dns-view: e2e

e2e.dtc-pool: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_DTC_POOL)
e2e.dtc-pool: e2e

e2e.dtc-lbdn: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_DTC_LBDN)
e2e.dtc-lbdn: e2e

# Full E2E: all resource examples (core + namespaced variants)
e2e-full: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_ALL)
e2e-full: e2e-preflight e2e

# local-deploy: build + load provider image into kind cluster, then deploy the xpkg.
# Called by `make e2e` via UPTEST_LOCAL_DEPLOY_TARGET=local-deploy.
local-deploy: local.xpkg.deploy.provider.$(PROJECT_NAME)

.PHONY: local-deploy e2e-preflight e2e.record-a e2e.record-aaaa e2e.record-alias e2e.record-cname e2e.record-mx e2e.record-ns e2e.record-ptr e2e.record-srv e2e.record-txt e2e.zone-delegated e2e.network-view e2e.host-record e2e.network e2e.range-template e2e.zone-auth e2e.ipv4-shared-network e2e.network-container e2e.fixed-address e2e.zone-forward e2e.range e2e.dtc-server e2e.extensible-attribute-def e2e.dns-view e2e.dtc-pool e2e.dtc-lbdn e2e-full

# ====================================================================================
# Update-tester standalone targets (per-field update-tester convention)
#
# These invoke the update-tester tool directly (converge + per-field run)
# against both scope variants of each resource. They are separate from the
# uptest post-assert hook integration (which also runs update-tester as part
# of the full `make e2e.<resource>` Create→Update→Import→Delete cycle) — use
# these for a fast, standalone check against an already-deployed resource.
UPDATE_TESTER := tools/update-tester/update-tester
UPDATE_TESTER_SRC := $(wildcard tools/update-tester/*.go)

$(UPDATE_TESTER): $(UPDATE_TESTER_SRC)
	@if [ -z "$(UPDATE_TESTER_SRC)" ]; then \
	  echo "tools/update-tester is not scaffolded yet for this provider" >&2; \
	  exit 1; \
	fi
	cd tools/update-tester && go build -o update-tester .

update-test.record-aaaa: $(UPDATE_TESTER)
	$(UPDATE_TESTER) converge examples/record-aaaa/record-aaaa.yaml
	$(UPDATE_TESTER) run examples/record-aaaa/record-aaaa.yaml
	$(UPDATE_TESTER) converge examples/record-aaaa/record-aaaa-namespaced.yaml
	$(UPDATE_TESTER) run examples/record-aaaa/record-aaaa-namespaced.yaml

update-test.record-alias: $(UPDATE_TESTER)
	$(UPDATE_TESTER) converge examples/record-alias/record-alias.yaml
	$(UPDATE_TESTER) run examples/record-alias/record-alias.yaml
	$(UPDATE_TESTER) converge examples/record-alias/record-alias-namespaced.yaml
	$(UPDATE_TESTER) run examples/record-alias/record-alias-namespaced.yaml

update-test.record-ptr: $(UPDATE_TESTER)
	$(UPDATE_TESTER) converge examples/record-ptr/record-ptr.yaml
	$(UPDATE_TESTER) run examples/record-ptr/record-ptr.yaml
	$(UPDATE_TESTER) converge examples/record-ptr/record-ptr-namespaced.yaml
	$(UPDATE_TESTER) run examples/record-ptr/record-ptr-namespaced.yaml

update-test.record-srv: $(UPDATE_TESTER)
	$(UPDATE_TESTER) converge examples/record-srv/record-srv.yaml
	$(UPDATE_TESTER) run examples/record-srv/record-srv.yaml
	$(UPDATE_TESTER) converge examples/record-srv/record-srv-namespaced.yaml
	$(UPDATE_TESTER) run examples/record-srv/record-srv-namespaced.yaml

update-test.record-txt: $(UPDATE_TESTER)
	$(UPDATE_TESTER) converge examples/record-txt/record-txt.yaml
	$(UPDATE_TESTER) run examples/record-txt/record-txt.yaml
	$(UPDATE_TESTER) converge examples/record-txt/record-txt-namespaced.yaml
	$(UPDATE_TESTER) run examples/record-txt/record-txt-namespaced.yaml

update-test.zone-delegated: $(UPDATE_TESTER)
	$(UPDATE_TESTER) converge examples/zone-delegated/zone-delegated.yaml
	$(UPDATE_TESTER) run examples/zone-delegated/zone-delegated.yaml
	$(UPDATE_TESTER) converge examples/zone-delegated/zone-delegated-namespaced.yaml
	$(UPDATE_TESTER) run examples/zone-delegated/zone-delegated-namespaced.yaml

update-test.zone-auth: $(UPDATE_TESTER)
	$(UPDATE_TESTER) converge examples/zone-auth/zone-auth.yaml
	$(UPDATE_TESTER) run examples/zone-auth/zone-auth.yaml
	$(UPDATE_TESTER) converge examples/zone-auth/zone-auth-namespaced.yaml
	$(UPDATE_TESTER) run examples/zone-auth/zone-auth-namespaced.yaml

update-test.ipv4-shared-network: $(UPDATE_TESTER)
	$(UPDATE_TESTER) converge examples/ipv4-shared-network/ipv4-shared-network.yaml
	$(UPDATE_TESTER) run examples/ipv4-shared-network/ipv4-shared-network.yaml
	$(UPDATE_TESTER) converge examples/ipv4-shared-network/ipv4-shared-network-namespaced.yaml
	$(UPDATE_TESTER) run examples/ipv4-shared-network/ipv4-shared-network-namespaced.yaml

.PHONY: update-test.record-aaaa update-test.record-alias update-test.record-ns update-test.record-ptr update-test.record-srv update-test.record-txt update-test.zone-auth update-test.zone-delegated update-test.ipv4-shared-network update-test.dtc-server update-test.extensible-attribute-def
update-test.record-ns: $(UPDATE_TESTER)
	$(UPDATE_TESTER) converge examples/record-ns/record-ns.yaml
	$(UPDATE_TESTER) run examples/record-ns/record-ns.yaml
	$(UPDATE_TESTER) converge examples/record-ns/record-ns-namespaced.yaml
	$(UPDATE_TESTER) run examples/record-ns/record-ns-namespaced.yaml

update-test.dtc-server: $(UPDATE_TESTER)
	$(UPDATE_TESTER) converge examples/dtc-server/dtc-server.yaml
	$(UPDATE_TESTER) run examples/dtc-server/dtc-server.yaml
	$(UPDATE_TESTER) converge examples/dtc-server/dtc-server-namespaced.yaml
	$(UPDATE_TESTER) run examples/dtc-server/dtc-server-namespaced.yaml

update-test.extensible-attribute-def: $(UPDATE_TESTER)
	$(UPDATE_TESTER) converge examples/extensible-attribute-def/extensible-attribute-def.yaml
	$(UPDATE_TESTER) run examples/extensible-attribute-def/extensible-attribute-def.yaml
	$(UPDATE_TESTER) converge examples/extensible-attribute-def/extensible-attribute-def-namespaced.yaml
	$(UPDATE_TESTER) run examples/extensible-attribute-def/extensible-attribute-def-namespaced.yaml


update-test.network-view: $(UPDATE_TESTER)
	$(UPDATE_TESTER) converge examples/network-view/network-view.yaml
	$(UPDATE_TESTER) run examples/network-view/network-view.yaml
	$(UPDATE_TESTER) converge examples/network-view/network-view-namespaced.yaml
	$(UPDATE_TESTER) run examples/network-view/network-view-namespaced.yaml

.PHONY: update-test.network-view
update-test.host-record: $(UPDATE_TESTER)
	$(UPDATE_TESTER) converge examples/host-record/host-record.yaml
	$(UPDATE_TESTER) run examples/host-record/host-record.yaml
	$(UPDATE_TESTER) converge examples/host-record/host-record-namespaced.yaml
	$(UPDATE_TESTER) run examples/host-record/host-record-namespaced.yaml

update-test.network: $(UPDATE_TESTER)
	$(UPDATE_TESTER) converge examples/network/network.yaml
	$(UPDATE_TESTER) run examples/network/network.yaml
	$(UPDATE_TESTER) converge examples/network/network-namespaced.yaml
	$(UPDATE_TESTER) run examples/network/network-namespaced.yaml

update-test.range-template: $(UPDATE_TESTER)
	$(UPDATE_TESTER) converge examples/range-template/range-template.yaml
	$(UPDATE_TESTER) run examples/range-template/range-template.yaml
	$(UPDATE_TESTER) converge examples/range-template/range-template-namespaced.yaml
	$(UPDATE_TESTER) run examples/range-template/range-template-namespaced.yaml

update-test.network-container: $(UPDATE_TESTER)
	$(UPDATE_TESTER) converge examples/network-container/network-container.yaml
	$(UPDATE_TESTER) run examples/network-container/network-container.yaml
	$(UPDATE_TESTER) converge examples/network-container/network-container-namespaced.yaml
	$(UPDATE_TESTER) run examples/network-container/network-container-namespaced.yaml

update-test.fixed-address: $(UPDATE_TESTER)
	$(UPDATE_TESTER) converge examples/fixed-address/fixed-address.yaml
	$(UPDATE_TESTER) run examples/fixed-address/fixed-address.yaml
	$(UPDATE_TESTER) converge examples/fixed-address/fixed-address-namespaced.yaml
	$(UPDATE_TESTER) run examples/fixed-address/fixed-address-namespaced.yaml

update-test.zone-forward: $(UPDATE_TESTER)
	$(UPDATE_TESTER) converge examples/zone-forward/zone-forward.yaml
	$(UPDATE_TESTER) run examples/zone-forward/zone-forward.yaml
	$(UPDATE_TESTER) converge examples/zone-forward/zone-forward-namespaced.yaml
	$(UPDATE_TESTER) run examples/zone-forward/zone-forward-namespaced.yaml

update-test.range: $(UPDATE_TESTER)
	$(UPDATE_TESTER) converge examples/range/range.yaml
	$(UPDATE_TESTER) run examples/range/range.yaml
	$(UPDATE_TESTER) converge examples/range/range-namespaced.yaml
	$(UPDATE_TESTER) run examples/range/range-namespaced.yaml

.PHONY: update-test.record-aaaa update-test.record-ptr update-test.record-txt update-test.zone-delegated update-test.host-record update-test.network update-test.range-template update-test.network-container update-test.fixed-address update-test.zone-forward update-test.range

update-test.dns-view: $(UPDATE_TESTER)
	$(UPDATE_TESTER) converge examples/dns-view/dns-view.yaml
	$(UPDATE_TESTER) run examples/dns-view/dns-view.yaml
	$(UPDATE_TESTER) converge examples/dns-view/dns-view-namespaced.yaml
	$(UPDATE_TESTER) run examples/dns-view/dns-view-namespaced.yaml

.PHONY: update-test.dns-view

update-test.dtc-pool: $(UPDATE_TESTER)
	$(UPDATE_TESTER) converge examples/dtc-pool/dtc-pool.yaml
	$(UPDATE_TESTER) run examples/dtc-pool/dtc-pool.yaml
	$(UPDATE_TESTER) converge examples/dtc-pool/dtc-pool-namespaced.yaml
	$(UPDATE_TESTER) run examples/dtc-pool/dtc-pool-namespaced.yaml

.PHONY: update-test.dtc-pool

update-test.dtc-lbdn: $(UPDATE_TESTER)
	$(UPDATE_TESTER) converge examples/dtc-lbdn/dtc-lbdn.yaml
	$(UPDATE_TESTER) run examples/dtc-lbdn/dtc-lbdn.yaml
	$(UPDATE_TESTER) converge examples/dtc-lbdn/dtc-lbdn-namespaced.yaml
	$(UPDATE_TESTER) run examples/dtc-lbdn/dtc-lbdn-namespaced.yaml

.PHONY: update-test.dtc-lbdn

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

# ====================================================================================
# Update-tester standalone targets (the update-tester convergence-check convention)
#
# These invoke the update-tester tool directly (converge + per-field run)
# against both scope variants of each resource. They are separate from the
# uptest post-assert hook integration (which also runs update-tester as part
# of the full `make e2e.<resource>` Create→Update→Import→Delete cycle) — use
# these for a fast, standalone check against an already-deployed resource.
#
# Guarded: tools/update-tester has not been scaffolded for this provider yet,
# so these targets no-op with a message rather than failing the build. Once
# tools/update-tester/*.go exists, $(UPDATE_TESTER) builds normally and the
# targets run the real converge/run cycle.

UPDATE_TESTER := tools/update-tester/update-tester

$(UPDATE_TESTER):
	@if [ -d tools/update-tester ] && ls tools/update-tester/*.go >/dev/null 2>&1; then \
	  cd tools/update-tester && $(GO) build -o update-tester . ; \
	else \
	  echo "update-tester: tools/update-tester not yet scaffolded — skipping (no-op)"; \
	fi

update-test.record-cname: $(UPDATE_TESTER) ## Update test for CNAMERecord (per-field convergence check)
	@if [ -x $(UPDATE_TESTER) ]; then \
	  $(UPDATE_TESTER) converge examples/record-cname/record-cname.yaml; \
	  $(UPDATE_TESTER) run examples/record-cname/record-cname.yaml; \
	  $(UPDATE_TESTER) converge examples/record-cname/record-cname-namespaced.yaml; \
	  $(UPDATE_TESTER) run examples/record-cname/record-cname-namespaced.yaml; \
	else \
	  echo "update-test.record-cname: update-tester not available yet — no-op"; \
	fi

.PHONY: update-test.record-cname

update-test.record-mx: $(UPDATE_TESTER)
	$(UPDATE_TESTER) converge examples/record-mx/record-mx.yaml
	$(UPDATE_TESTER) run examples/record-mx/record-mx.yaml
	$(UPDATE_TESTER) converge examples/record-mx/record-mx-namespaced.yaml
	$(UPDATE_TESTER) run examples/record-mx/record-mx-namespaced.yaml

.PHONY: update-test.record-mx

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
# directory name and the <seg>_types.go file prefix. The second group segment
# ("m" for namespace-scoped, absent for cluster-scoped) selects apis/namespaced
# vs apis/cluster.
update-test.validate: $(UPDATE_TESTER)
	@if [ ! -x $(UPDATE_TESTER) ]; then \
	  echo "update-test.validate: update-tester not available yet — no-op"; \
	  exit 0; \
	fi
	@fail=0; \
	for f in $$(grep -rl 'crossplane.io/update-test' examples --include='*.yaml' | sort); do \
	  av=$$(awk '/^apiVersion:/ {print $$2; exit}' "$$f"); \
	  grp=$$(echo "$$av" | cut -d/ -f1); \
	  seg1=$$(echo "$$grp" | cut -d. -f1); \
	  seg2=$$(echo "$$grp" | cut -d. -f2); \
	  if [ "$$seg2" = "m" ]; then scope=namespaced; else scope=cluster; fi; \
	  types="apis/$$scope/$$seg1/v1alpha1/$${seg1}_types.go"; \
	  if [ ! -f "$$types" ]; then \
	    echo "SKIP: $$f — no types file at $$types (apiVersion=$$av)"; \
	    fail=1; \
	    continue; \
	  fi; \
	  echo "=== $$f ($$types) ==="; \
	  $(UPDATE_TESTER) validate --types-file "$$types" "$$f" || fail=1; \
	done; \
	exit $$fail

.PHONY: update-test.validate
