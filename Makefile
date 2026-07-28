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

# Helper variables for comma-separated manifest lists (uptest CLI convention).
comma := ,
empty :=
space := $(empty) $(empty)

# Per-resource manifest variables (comma pair of cluster + namespaced variants,
# so `make e2e.<resource>` gates both scopes).
UPTEST_MANIFESTS_RECORD_TXT := examples/record-txt/record-txt.yaml,examples/record-txt/record-txt-namespaced.yaml
UPTEST_MANIFESTS_ZONE_DELEGATED := examples/zone-delegated/zone-delegated.yaml,examples/zone-delegated/zone-delegated-namespaced.yaml

# E2E manifest tiers
# Core tier: only needs API credentials (INFOBLOX_HOST,
# INFOBLOX_USER, INFOBLOX_PASS), no external infrastructure beyond the NIOS
# Grid Manager itself.
UPTEST_MANIFESTS_CORE = $(UPTEST_MANIFESTS_RECORD_TXT),$(UPTEST_MANIFESTS_ZONE_DELEGATED)

# UPTEST_MANIFESTS_ALL: discover all resource examples, excluding provider/ config.
# Produces a comma-separated list for `uptest e2e` (the unified example-manifest convention).
_UPTEST_MANIFESTS_ALL_RAW := $(filter-out examples/provider/%,$(wildcard examples/*/*.yaml))
UPTEST_MANIFESTS_ALL := $(subst $(space),$(comma),$(_UPTEST_MANIFESTS_ALL_RAW))

# Default e2e input: CORE manifests
UPTEST_INPUT_MANIFESTS ?= $(UPTEST_MANIFESTS_CORE)

UPTEST_SETUP_SCRIPT ?= test/setup.sh

# Per-run uptest test-directory isolation (each concurrent E2E run gets its own
# staging directory).
ifdef KIND_CLUSTER_NAME
  UPTEST_ARGS += --test-directory=/tmp/uptest-e2e-$(KIND_CLUSTER_NAME)
endif

-include build/makelib/uptest.mk

# E2E preflight: validate NIOS Grid Manager credentials before running the
# (expensive) E2E pipeline.
e2e-preflight: ## Validate credentials before E2E
	@MISSING=""; \
	[ -z "$${INFOBLOX_HOST:-}" ] && MISSING="$$MISSING INFOBLOX_HOST"; \
	[ -z "$${INFOBLOX_USER:-}" ] && MISSING="$$MISSING INFOBLOX_USER"; \
	[ -z "$${INFOBLOX_PASS:-}" ] && MISSING="$$MISSING INFOBLOX_PASS"; \
	if [ -n "$$MISSING" ]; then \
	  echo "ERROR: missing required credential env var(s):$$MISSING" >&2; \
	  echo "  Set them as env vars (Hive nest secrets) or in ../.env" >&2; \
	  exit 1; \
	fi
	@echo "e2e-preflight: credentials OK"

e2e: e2e-preflight

# Per-resource E2E targets
e2e.record-txt: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_RECORD_TXT)
e2e.record-txt: e2e

e2e.zone-delegated: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_ZONE_DELEGATED)
e2e.zone-delegated: e2e

# Full E2E: all resource examples (core + namespaced variants)
e2e-full: UPTEST_INPUT_MANIFESTS = $(UPTEST_MANIFESTS_ALL)
e2e-full: e2e-preflight e2e

# local-deploy: build + load provider image into kind cluster, then deploy the xpkg.
# Called by `make e2e` via UPTEST_LOCAL_DEPLOY_TARGET=local-deploy.
local-deploy: local.xpkg.deploy.provider.$(PROJECT_NAME)

.PHONY: local-deploy e2e-preflight e2e.record-txt e2e.zone-delegated e2e-full

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

.PHONY: update-test.record-txt update-test.zone-delegated

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

# Update the submodules, such as the common build scripts.
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
