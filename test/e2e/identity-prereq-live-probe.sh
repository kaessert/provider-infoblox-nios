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
# It additionally exercises the two remaining call sites from
# ADR-IN-0006 §4 that the four properties above never happen to reach:
#
#   5. Create()'s unconditional guard (cluster.go:199 / namespaced.go)
#      refusing a brand-new object. This is NOT reachable by simply
#      repeating property 3 with the definition absent throughout — a
#      brand-new object's Observe() always resolves through the identity
#      search first (external-name == CR name -> ref "" -> search), so
#      Observe's own reactive guard refuses before Create() is ever
#      called; property 3 above proves exactly that path, not Create()'s.
#      Reaching Create() specifically requires the identity search to
#      SUCCEED at Observe time (so Create() is actually invoked) while
#      Create()'s own prerequisite check still refuses — see "Scenario 5"
#      below for how this script engineers that split deterministically,
#      without racing a live network call.
#   6. Delete()'s reactive guard (controller.go:791). Every reconcile
#      resolves the SAME (external-name, uid) pair through Observe()
#      before Delete() is ever considered, and Delete() is only called
#      when Observe() just reported the object exists — which means
#      Observe() already performed the identical identity resolution
#      Delete() would need to repeat, using the same connection, against
#      the same Grid state, moments earlier in the same reconcile. Any
#      Grid state that would make Delete()'s own search fail has therefore
#      already made Observe()'s identical search fail first, and Observe()
#      returning an error aborts the reconcile before Delete() runs. This
#      script proves that chain live (see "Scenario 6") rather than
#      asserting it from the source alone: it drives an in-flight deletion
#      of a resource with a stale reference into exactly this state and
#      confirms the observable, user-facing outcome while it holds — the
#      delete is blocked safely (no orphan, no false success) as long as
#      the definition stays absent — while documenting why the specific
#      refusal at controller.go:791 cannot be triggered from outside the
#      process without racing two calls a few milliseconds apart on a
#      live Grid. Recreating the definition afterward exposed a SEPARATE,
#      real gap this script cannot fix (production code, out of a test
#      script's charter): the Kubernetes object then finalizes cleanly,
#      but the Grid object is never actually deleted — see Scenario 6's
#      own header comment for the live-verified mechanism.
#
# ── Isolation: a scratch attribute name, not the real one ─────────────────
#
# internal/clients/identity.EAKey is a single Go constant consumed by every
# identity-aware controller. This script builds the provider from a
# throwaway git worktree with that one constant substituted for a per-run
# scratch name, so the whole scenario plays out against an attribute
# definition nothing else on the Grid references — the real "Crossplane
# Internal ID" definition that every other managed object depends on is
# never touched, deleted, or raced against. The substitution never touches
# this checkout: it is made with `sed` inside a `git worktree add --detach`
# scratch copy that is removed at the end of the run. A checkout-local `sed`
# plus a restore-on-exit trap was tried first and rejected — a SIGKILL
# between the edit and the restore (which a trap cannot catch) leaves this
# checkout holding a scratch EAKey and unable to build correctly on the next
# run. Building from a worktree removes the failure mode instead of trying
# to catch it: this checkout is never written to, so there is nothing for a
# kill to corrupt here regardless of when it lands.
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
#   ./test/e2e/identity-prereq-live-probe.sh
#
# Requires: the usual E2E toolchain (kind, kubectl, helm — see
# build/makelib/k8s_tools.mk, which downloads them on demand), flock
# (util-linux — used to tell a live scratch build worktree apart from an
# abandoned one, see the cleanup hygiene section below), a superuser
# and a non-superuser WAPI credential (see the credentials section of the
# provider documentation), and a kind cluster name exported as
# KIND_CLUSTER_NAME (falls back to a script-generated one). Run from the
# provider module root.
#
# Prints progress to stdout and leaves a non-zero exit status (with the
# cluster, the scratch build worktree, and any scratch Grid objects already
# cleaned up) if any assertion fails. Every scratch object this script
# creates carries the run token in its name, so a failed run's leftovers
# are trivially distinguishable from anything else on the Grid.
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
# Per-run kubeconfig path, keyed on RUN_TOKEN. Two runs started from the
# same checkout must never share this file: `make controlplane.up` writes
# its own cluster's context into whatever path KUBECONFIG names, so a
# checkout-wide default lets a second run started mid-first-run repoint
# the first run's kubectl at the second run's cluster. An externally
# supplied KUBECONFIG (how an automated caller isolates this script from
# concurrent invocations) is honoured unchanged — KUBECONFIG_WAS_DEFAULTED
# records which case we are in so cleanup() only ever removes a file this
# run created.
KUBECONFIG_WAS_DEFAULTED=0
[ -n "${KUBECONFIG:-}" ] || KUBECONFIG_WAS_DEFAULTED=1
export KUBECONFIG="${KUBECONFIG:-${ROOT_DIR}/identity-prereq-probe-${RUN_TOKEN}.kubeconfig}"
# REMEDIATION_SUBSTR is the operator-facing text every PrerequisiteError
# carries (identity/probe.go's PrerequisiteError.Error()), regardless of
# which of the four call sites returned it — used below together with the
# crossplane-runtime verb prefix to prove WHICH guard fired, not merely
# that some guard fired.
REMEDIATION_SUBSTR="the configured credential cannot create one"

log() { echo "[identity-prereq-probe] $*"; }

url_encode() {
  python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$1"
}

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

# get_ea_def_ref returns the _ref of the named extensible attribute
# definition, or "" if it does not exist.
get_ea_def_ref() {
  local name="$1"
  curl_wapi "${INFOBLOX_USER}" "${INFOBLOX_PASS}" GET \
    "extensibleattributedef?name=$(url_encode "${name}")" \
    | python3 -c 'import json,sys
d=json.load(sys.stdin)
print(d[0]["_ref"] if d else "")' 2>/dev/null || true
}

# delete_ea_def removes the named extensible attribute definition if it
# exists — a two-call teardown (clear the read-only flag, then DELETE)
# since a definition this provider creates is always R-flagged.
delete_ea_def() {
  local name="$1" ref
  ref="$(get_ea_def_ref "${name}")"
  if [ -n "${ref}" ]; then
    curl_wapi "${INFOBLOX_USER}" "${INFOBLOX_PASS}" PUT "${ref}" '{"flags":"C"}' >/dev/null || true
    curl_wapi "${INFOBLOX_USER}" "${INFOBLOX_PASS}" DELETE "${ref}" >/dev/null || true
    log "cleanup: deleted extensible attribute definition ${name}"
  fi
}

# record_a_refs_for_token returns one "<_ref><TAB><name>" line per
# record:a object still on the Grid whose name carries this run's
# RUN_TOKEN, or nothing if none remain. RUN_TOKEN (a timestamp) is
# embedded in every scratch object this script creates (see apply_arecord)
# and in nothing else on a shared Grid, so a substring match on it alone
# is exact scoping — never a bare "idp-" prefix and never an age check,
# both of which would risk matching another concurrent run's live
# fixtures.
record_a_refs_for_token() {
  curl_wapi "${INFOBLOX_USER}" "${INFOBLOX_PASS}" GET \
    "record:a?name~=${RUN_TOKEN}&_return_fields=name" \
    | python3 -c 'import json,sys
d=json.load(sys.stdin)
for o in d:
    print(o.get("_ref", "") + "\t" + o.get("name", ""))' 2>/dev/null || true
}

# sweep_record_a deletes every record:a object still on the Grid whose
# name carries this run's RUN_TOKEN. This is here because of a known,
# separately-filed production gap: once the identity EA definition is
# recreated mid-run (scenario 4), a resource whose stored reference was
# staled (scenario 6) finalizes cleanly on the Kubernetes side but its
# Grid object is never actually deleted — see scenario 6's own header
# comment for the live-verified mechanism. This function's job is only to
# leave the Grid clean regardless; it does not touch, retry, or work
# around the reconciler's delete path.
sweep_record_a() {
  local ref name
  while IFS=$'\t' read -r ref name; do
    [ -z "${ref}" ] && continue
    curl_wapi "${INFOBLOX_USER}" "${INFOBLOX_PASS}" DELETE "${ref}" >/dev/null || true
    log "cleanup: swept orphaned record:a ${name} (${ref})"
  done < <(record_a_refs_for_token)
}

# recreate_scratch_definition (re)creates the scratch identity EA
# definition as a superuser, with the exact type/flags/comment shape the
# provider itself uses. Shared by scenario 1's post-condition check and by
# scenario 5's setup (recreating it out of band to engineer the
# Observe-succeeds/Create-refuses split).
recreate_scratch_definition() {
  curl_wapi "${INFOBLOX_USER}" "${INFOBLOX_PASS}" POST "extensibleattributedef" \
    "{\"name\":\"${SCRATCH_KEY}\",\"type\":\"STRING\",\"flags\":\"CR\",\"comment\":\"identity-prereq-live-probe recreation\"}" >/dev/null
}

# assert_condition_prefix verifies that a Synced=False condition message
# both (a) starts with the exact crossplane-runtime verb prefix identifying
# WHICH external.<Method> call failed, and (b) carries the shared
# PrerequisiteError remediation text. All four guarded call sites return
# textually identical PrerequisiteError values, so the remediation text
# alone cannot distinguish which one fired — only crossplane-runtime's own
# "<verb> failed: " wrapper (added once, in the reconciler, keyed to which
# method returned the error) can. See
# crossplane-runtime/pkg/reconciler/managed/reconciler.go's
# errReconcileObserve/errReconcileCreate/errReconcileUpdate/errReconcileDelete.
assert_condition_prefix() {
  local label="$1" msg="$2" prefix="$3"
  if [[ "${msg}" != "${prefix}"* ]]; then
    echo "FATAL: ${label} — condition did not carry the expected crossplane-runtime verb prefix '${prefix}': ${msg}" >&2
    exit 1
  fi
  if ! grep -q "${REMEDIATION_SUBSTR}" <<<"${msg}"; then
    echo "FATAL: ${label} — condition carried the '${prefix}' prefix but not the shared remediation text: ${msg}" >&2
    exit 1
  fi
  log "${label} PASSED — condition carries '${prefix}':"
  echo "${msg}"
}

# remove_scratch_build_images <worktree> — removes only the docker image(s)
# the crossplane build submodule tagged for THIS scratch worktree
# (build/makelib/imagelight.mk scopes every build to
# build-$(sha256(HOSTNAME-ROOT_DIR))/..., and that registry can never be
# recomputed once the worktree directory is gone — see the cleanup call
# sites below for why this must run before `git worktree remove`).
#
# This intentionally does NOT shell out to `make img.clean`: that target's
# own `do.img.clean` recipe parses the human-readable `docker images` table
# by fixed column position (`awk '{print $1":"$2}'`), and at least one
# supported docker CLI here reports a merged IMAGE column (repo:tag already
# combined) rather than separate REPOSITORY/TAG columns — `img.clean` then
# builds a garbled reference, silently matches nothing, and reports "OK"
# having removed zero images. `make build.vars` is the same upstream
# mechanism computing the same BUILD_REGISTRY value, but as a plain
# `KEY=value` line with no table to misparse; the actual removal then uses
# `docker images --filter reference=... --format` (Go-template fields, not
# column position) exactly like the shared ant-worktree.sh reclaimer.
# Never `-f`: a reference docker still considers in use by a container is
# left alone rather than forced out from under it.
remove_scratch_build_images() {
  local worktree="$1" registry ref
  [ -n "${worktree}" ] && [ -d "${worktree}" ] || return 0
  command -v docker >/dev/null 2>&1 || return 0
  # Guarded, not just `|| true`-wrapped at the call site: under `set -e`,
  # a failing command on the right of a pipe still aborts THIS function
  # (pipefail propagates the failure into the assignment) before the
  # `[ -n ... ] || return 0` guard below ever runs. A worktree whose
  # build/ submodule was never initialized (e.g. a SIGKILL landed between
  # `git worktree add` and `git submodule update`) makes `make build.vars`
  # fail exactly this way, so the empty-registry case above must be
  # reachable even when the underlying command errors, not only when it
  # succeeds with no output.
  registry=""
  registry="$(make -C "${worktree}" build.vars 2>/dev/null \
              | sed -n 's/^BUILD_REGISTRY=//p')" || registry=""
  [ -n "${registry}" ] || return 0
  while IFS= read -r ref; do
    [ -n "${ref}" ] || continue
    docker rmi "${ref}" >/dev/null 2>&1 && log "cleanup: removed build image ${ref}"
  done < <(docker images --filter "reference=${registry}/*" --format '{{.Repository}}:{{.Tag}}' 2>/dev/null)
  return 0
}

cleanup() {
  local status=$?
  log "cleanup: removing scratch managed resources (if any remain)..."
  ${KUBECTL} delete arecord.recorda.infobloxnios.crossplane.io \
    -l "identity-prereq-probe/run=${RUN_TOKEN}" --wait=true --timeout=120s 2>/dev/null || true
  ${KUBECTL} delete arecord.recorda.infobloxnios.m.crossplane.io -n default \
    -l "identity-prereq-probe/run=${RUN_TOKEN}" --wait=true --timeout=120s 2>/dev/null || true

  log "cleanup: removing the scratch extensible attribute definition (if it still exists)..."
  delete_ea_def "${SCRATCH_KEY}"

  # The kubectl delete above cannot reach a record:a object whose
  # Kubernetes resource already finalized without a matching Grid delete
  # (see sweep_record_a's comment) — sweep the Grid directly by RUN_TOKEN,
  # then read back to confirm the sweep actually worked rather than
  # asserting it did. grid_clean only becomes true once the read-back
  # itself comes back empty.
  log "cleanup: sweeping any surviving token-scoped record:a objects from the Grid..."
  sweep_record_a
  local surviving grid_clean=1
  surviving="$(record_a_refs_for_token)"
  if [ -n "${surviving}" ]; then
    grid_clean=0
    status=1
    log "FATAL: the following token-scoped record:a object(s) survived the sweep and are still on the Grid:"
    echo "${surviving}" >&2
  fi

  log "cleanup: tearing down the kind cluster..."
  make controlplane.down KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME}" >/dev/null 2>&1 || true

  if [ -n "${SCRATCH_WORKTREE:-}" ]; then
    # MUST run before the worktree is removed below — once the directory is
    # gone the registry name can no longer be derived from anything and the
    # image becomes unattributable. The helper itself guards its `make
    # build.vars` call so a docker/make hiccup degrades to "nothing to
    # clean" instead of propagating; the `|| true` here is defence in
    # depth for any future change to the helper, not the only thing
    # standing between a docker/make hiccup and a passing probe going red.
    log "cleanup: removing the scratch build worktree's images..."
    remove_scratch_build_images "${SCRATCH_WORKTREE}" || true
    log "cleanup: removing the scratch build worktree..."
    git -C "${ROOT_DIR}" worktree remove --force "${SCRATCH_WORKTREE}" 2>/dev/null || true
    rm -f "${SCRATCH_WORKTREE}.lock"
  fi

  if [ "${KUBECONFIG_WAS_DEFAULTED}" -eq 1 ]; then
    log "cleanup: removing the per-run kubeconfig ${KUBECONFIG}..."
    rm -f "${KUBECONFIG}"
  fi

  if [ "${status}" -eq 0 ] && [ "${grid_clean}" -eq 1 ]; then
    log "done — all scenarios passed, Grid and cluster clean (verified by read-back)"
  elif [ "${grid_clean}" -eq 1 ]; then
    log "FAILED (exit ${status}) — Grid verified clean by read-back, cluster and scratch worktree cleaned up regardless"
  else
    log "FAILED (exit ${status}) — Grid is NOT clean, see the FATAL line(s) above for the surviving record:a object(s); cluster and scratch worktree cleaned up regardless"
  fi
  exit "${status}"
}
trap cleanup EXIT

log "run token: ${RUN_TOKEN}"
log "scratch attribute name: ${SCRATCH_KEY}"

# Defensive hygiene: a prior run killed hard enough to skip its own cleanup
# leaves an orphaned scratch worktree registered against this checkout.
# `git worktree add` below would fail outright if that path is still
# claimed, so prune stale entries first — this never touches a worktree
# still in use, only registrations whose directory no longer exists.
git -C "${ROOT_DIR}" worktree prune 2>/dev/null || true

# `worktree prune` above only drops registrations whose directory is
# already gone — a SIGKILLed run's scratch worktree directory survives
# (only the running process, not the filesystem, was killed), so its
# registration survives every subsequent `prune` forever. A killed run has
# no scenario state worth keeping, so force-remove any leftover
# identity-prereq-probe-build.* worktree before minting a fresh one — but
# ONLY if it is actually abandoned, not merely old: a second probe started
# against this same checkout while a first one is still mid-build must
# leave the first one's worktree alone. Liveness is decided with an flock,
# not a PID file — a PID file only proves a process with that number
# existed once, and PIDs get reused, so a stale file can lie "alive" long
# after its owner is gone. Every run holds an exclusive, non-blocking
# flock on "<worktree>.lock" for its entire lifetime (acquired right below,
# before the worktree is even created) via a file descriptor that is never
# closed explicitly, so the kernel is the one thing that has to agree a
# run is alive: a live run still holds the lock, and a SIGKILLed run's
# lock is released automatically the instant the process dies, with no
# trap required. That makes "can this run acquire the lock" and "is the
# owning run dead" the same question, so the sweep below can ask it
# directly instead of inferring liveness from a timestamp or a PID.
# Then prune again so the registration is gone immediately rather than
# lingering until the run after next.
STALE_SCRATCH_PREFIX="${TMPDIR:-/tmp}/identity-prereq-probe-build."
while IFS= read -r stale_worktree; do
  [ -n "${stale_worktree}" ] || continue
  case "${stale_worktree}" in
    "${STALE_SCRATCH_PREFIX}"*)
      stale_lock="${stale_worktree}.lock"
      if [ -e "${stale_lock}" ] && ! flock -n "${stale_lock}" -c true 2>/dev/null; then
        log "cleanup: leaving scratch worktree in place, still owned by a live run: ${stale_worktree}"
        continue
      fi
      log "cleanup: removing stale scratch worktree from a prior killed run: ${stale_worktree}"
      # A SIGKILLed run never reached its own trap, so its build image (if
      # the kill landed after the `make build` on line ~385) is exactly the
      # leak this sweep exists to reclaim — do it before the worktree goes
      # away, same as the live-run cleanup above, and for the same reason:
      # the registry name is derived from this path and is unrecoverable
      # once it is gone. The helper itself is a no-op if the kill landed
      # before `make build` (or before the submodule was initialized, in
      # which case `make build.vars` itself fails and the helper's own
      # guard degrades that to a no-op) — this is exactly the case a
      # SIGKILL between `git worktree add` and `git submodule update`
      # leaves behind, so it must not abort this startup sweep. `|| true`
      # here is defence in depth on top of that guard: this sweep runs
      # before any scenario and outside any trap, so an unguarded failure
      # here would brick every subsequent run on the same stale worktree.
      remove_scratch_build_images "${stale_worktree}" || true
      git -C "${ROOT_DIR}" worktree remove --force "${stale_worktree}" 2>/dev/null || rm -rf "${stale_worktree}"
      rm -f "${stale_lock}"
      ;;
  esac
done < <(git -C "${ROOT_DIR}" worktree list --porcelain | awk '/^worktree /{print substr($0,10)}')
git -C "${ROOT_DIR}" worktree prune 2>/dev/null || true

# ── 1. Build from a throwaway worktree with the scratch identity key ──────
IDENTITY_GO_REL="internal/clients/identity/identity.go"
SCRATCH_WORKTREE="$(mktemp -u "${TMPDIR:-/tmp}/identity-prereq-probe-build.XXXXXX")"
SCRATCH_LOCK="${SCRATCH_WORKTREE}.lock"
# Take the liveness lock BEFORE the worktree directory exists, so there is
# no window in which a concurrent sweep can see this path without also
# seeing an unlockable lock file for it. The descriptor is intentionally
# left open for the rest of the script — see the comment above the sweep
# for why that is the whole mechanism, not a leak.
exec {SCRATCH_LOCK_FD}>"${SCRATCH_LOCK}"
if ! flock -n "${SCRATCH_LOCK_FD}"; then
  echo "FATAL: could not acquire the scratch worktree liveness lock ${SCRATCH_LOCK} (unexpected — the name was just minted)" >&2
  exit 1
fi
git -C "${ROOT_DIR}" worktree add --detach --quiet "${SCRATCH_WORKTREE}" HEAD
log "scratch build worktree: ${SCRATCH_WORKTREE}"
git -C "${SCRATCH_WORKTREE}" submodule update --init --recursive

sed -i "s/^const EAKey = \"Crossplane Internal ID\"\$/const EAKey = \"${SCRATCH_KEY}\"/" \
  "${SCRATCH_WORKTREE}/${IDENTITY_GO_REL}"
if ! grep -q "const EAKey = \"${SCRATCH_KEY}\"" "${SCRATCH_WORKTREE}/${IDENTITY_GO_REL}"; then
  echo "FATAL: EAKey substitution did not match — identity.go may have changed shape" >&2
  exit 1
fi
log "one-line substitution applied in the scratch worktree only (this checkout is untouched):"
git -C "${SCRATCH_WORKTREE}" diff --stat -- "${IDENTITY_GO_REL}"

log "building provider image with the scratch identity key..."
make -C "${SCRATCH_WORKTREE}" build KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME}"

# ── 2. Bring up a scratch cluster and deploy the substituted build ────────
log "starting kind cluster ${KIND_CLUSTER_NAME}..."
make -C "${SCRATCH_WORKTREE}" controlplane.up KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME}"
log "deploying provider package..."
make -C "${SCRATCH_WORKTREE}" local-deploy KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME}"

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
  "extensibleattributedef?name=$(url_encode "${SCRATCH_KEY}")")"
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
  # On success, also sets the global LAST_SYNCED_MESSAGE to the Synced
  # condition's message from the SAME kubectl snapshot that matched
  # `want` — a separate follow-up `kubectl get` for the message, after
  # this function returns, can race a fast-reconciling object and
  # capture a DIFFERENT (possibly still-empty) condition than the one
  # that just satisfied `want`. Live-verified to actually happen: the
  # status matched "False" one poll, then a second, independent read of
  # the message moments later came back empty.
  #
  # When want="False", a status/message pair is only accepted once the
  # message is non-empty. Live-verified this matters even reading status
  # and message from one snapshot: a Synced=False condition was observed
  # transiently with no message at all before settling, a few polls
  # later, into the real ReconcileError text — status and message can
  # each update within the window this loop polls at, and every
  # Synced=False this provider sets always carries a reason in the
  # message, so an empty one is a transient in-flight write, not the
  # steady state a caller is waiting for.
  local res="$1" name="$2" scope="$3" want="$4" timeout="${5:-90}" ns_args=() json got msg
  [ "${scope}" = "namespaced" ] && ns_args=(-n default)
  LAST_SYNCED_MESSAGE=""
  for _ in $(seq 1 "$((timeout / 3))"); do
    json="$(${KUBECTL} "${ns_args[@]}" get "${res}/${name}" -o json 2>/dev/null || true)"
    read -r got msg < <(printf '%s' "${json}" | python3 -c '
import json,sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
for c in d.get("status", {}).get("conditions", []):
    if c.get("type") == "Synced":
        # NUL-separated on one line so a multi-line message cannot be
        # mistaken for a second record by the caller.
        sys.stdout.write(c.get("status", "") + "\x00" + c.get("message", "").replace("\n", " "))
        break
' 2>/dev/null | tr '\0' '\t') || true
    if [ "${got}" = "${want}" ] && { [ "${want}" != "False" ] || [ -n "${msg}" ]; }; then
      LAST_SYNCED_MESSAGE="$(printf '%s' "${json}" | python3 -c '
import json,sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
for c in d.get("status", {}).get("conditions", []):
    if c.get("type") == "Synced":
        print(c.get("message", ""))
        break
' 2>/dev/null || true)"
      return 0
    fi
    sleep 3
  done
  echo "FATAL: ${res}/${name} never reached Synced=${want} within ${timeout}s (last seen: ${got:-<none>})" >&2
  return 1
}

wait_deleted() {
  # wait_deleted <kind.group> <name> <scope: cluster|namespaced> <timeout-seconds>
  local res="$1" name="$2" scope="$3" timeout="${4:-90}" ns_args=()
  [ "${scope}" = "namespaced" ] && ns_args=(-n default)
  for _ in $(seq 1 "$((timeout / 3))"); do
    ${KUBECTL} "${ns_args[@]}" get "${res}/${name}" >/dev/null 2>&1 || return 0
    sleep 3
  done
  echo "FATAL: ${res}/${name} was not deleted within ${timeout}s" >&2
  return 1
}

# fake_stale_ref <label> — a syntactically well-formed record:a WAPI
# reference that resolves to a genuine 404, for forcing the identity
# ladder's search fallback (identity.Resolve treats a 404 at ref, not a
# malformed one, as "recover by searching"). WAPI's ref parser requires
# the middle segment to decode as base64; an arbitrary placeholder string
# (e.g. "record:a/nonexistent-123:name/default") fails that check outright
# with a 400 "AdmConProtoError: Invalid reference" — resolveByRef treats
# that as a genuine, non-recoverable error and returns immediately without
# ever reaching the identity-EA search, which silently defeats every
# scenario that depends on staling a reference. Live-verified: a
# placeholder string 400s; a base64-encoded (fabricated, never real)
# identity-field tuple 404s with "AdmConDataNotFoundError ... not found",
# exactly the shape a genuinely rotated handle produces.
fake_stale_ref() {
  local label="$1" b64
  b64="$(python3 -c "import base64,sys; print(base64.b64encode(sys.argv[1].encode()).decode())" \
    "dns.bind_a\$._default.com.example,${label},0.0.0.0")"
  echo "record:a/${b64}:${label}.example.com/default"
}

# ── Scenario 1: superuser auto-create ──────────────────────────────────────
log "=== scenario 1: superuser auto-creates the scratch definition ==="
apply_arecord "idp-s1-${RUN_TOKEN}" "192.0.2.150" cluster
apply_arecord "idp-s1-ns-${RUN_TOKEN}" "192.0.2.151" namespaced
wait_synced arecord.recorda.infobloxnios.crossplane.io "idp-s1-${RUN_TOKEN}" cluster True
wait_synced arecord.recorda.infobloxnios.m.crossplane.io "idp-s1-ns-${RUN_TOKEN}" namespaced True

# Pause both objects the moment they converge. Update() calls
# ensureIdentityPrerequisite unconditionally on every reconcile (ADR-
# IN-0006 §4), so an unpaused, still-reconciling object keeps refreshing
# the shared Prober's cached POSITIVE verdict on every poll interval —
# live-verified: a fixed sleep timed from "scenario 1 converged" was not
# long enough to observe the cache actually expire, because these objects
# kept re-arming it right up until the delete-and-swap below. Pausing
# stops all further reconciliation of these two objects, so the TTL wait
# further down counts from their last real refresh, not from whenever the
# script happens to check.
${KUBECTL} annotate arecord.recorda.infobloxnios.crossplane.io/idp-s1-${RUN_TOKEN} crossplane.io/paused=true --overwrite
${KUBECTL} annotate arecord.recorda.infobloxnios.m.crossplane.io/idp-s1-ns-${RUN_TOKEN} -n default crossplane.io/paused=true --overwrite

def_after_create="$(curl_wapi "${INFOBLOX_USER}" "${INFOBLOX_PASS}" GET \
  "extensibleattributedef?name=$(url_encode "${SCRATCH_KEY}")&_return_fields=name,type,flags")"
echo "${def_after_create}" | grep -q '"flags": *"CR"' || { echo "FATAL: scratch definition missing or not CR after scenario 1" >&2; exit 1; }
log "scenario 1 PASSED — definition auto-created with flags CR:"
echo "${def_after_create}"

log "=== scoped-refusal property: startup and unrelated controllers are unaffected ==="
log "provider pod restarts so far: $(${KUBECTL} -n crossplane-system get "${PROVIDER_POD}" -o jsonpath='{.status.containerStatuses[0].restartCount}')"

# ── Delete the scratch definition out of band ──────────────────────────────
log "deleting the scratch definition out of band..."
delete_ea_def "${SCRATCH_KEY}"

# Swap the shared credential secret to the restricted (non-superuser) user —
# every remaining scenario before recreation needs a credential that cannot
# recreate the definition, or the refusal self-heals before it can be
# observed.
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
# idp-s1(+ns) are paused above specifically so this clock starts from
# their last real reconcile, not from whenever the script happens to
# check — a few seconds of margin over the TTL itself is still kept here
# for scheduling/clock jitter.
log "waiting for the cached prerequisite verdict to expire (up to 5 minutes)..."
sleep 310

# Force scenario 1's cluster-scoped object down the identity-search path by
# staling its reference to something that will 404. Its namespaced sibling
# is staled too, so it can be independently restored before scenario 3
# creates unrelated new objects.
# idp-s1's own (cluster-scoped) reference is deliberately never restored —
# it stays staled all the way into scenario 6 below, which deletes it in
# this exact state. Only the namespaced sibling's reference is captured
# here, to be restored once scenario 2 has used it.
ns_ref="$(${KUBECTL} get arecord.recorda.infobloxnios.m.crossplane.io/idp-s1-ns-${RUN_TOKEN} -n default -o jsonpath='{.metadata.annotations.crossplane\.io/external-name}')"
${KUBECTL} annotate arecord.recorda.infobloxnios.crossplane.io/idp-s1-${RUN_TOKEN} \
  crossplane.io/external-name="$(fake_stale_ref "nonexistent-${RUN_TOKEN}")" --overwrite
${KUBECTL} annotate arecord.recorda.infobloxnios.m.crossplane.io/idp-s1-ns-${RUN_TOKEN} -n default \
  crossplane.io/external-name="$(fake_stale_ref "nonexistent-ns-${RUN_TOKEN}")" --overwrite
# Force an immediate reconcile rather than waiting for the poll interval.
${KUBECTL} annotate arecord.recorda.infobloxnios.crossplane.io/idp-s1-${RUN_TOKEN} crossplane.io/paused=true --overwrite
${KUBECTL} annotate arecord.recorda.infobloxnios.m.crossplane.io/idp-s1-ns-${RUN_TOKEN} -n default crossplane.io/paused=true --overwrite
sleep 3
${KUBECTL} annotate arecord.recorda.infobloxnios.crossplane.io/idp-s1-${RUN_TOKEN} crossplane.io/paused- 2>/dev/null || true
${KUBECTL} annotate arecord.recorda.infobloxnios.m.crossplane.io/idp-s1-ns-${RUN_TOKEN} -n default crossplane.io/paused- 2>/dev/null || true

# ── Scenario 2: reactive guard converts the raw search failure ────────────
log "=== scenario 2: Observe()'s reactive guard (controller.go:732) surfaces the remediation, not the raw WAPI error ==="
wait_synced arecord.recorda.infobloxnios.crossplane.io "idp-s1-${RUN_TOKEN}" cluster False 60
assert_condition_prefix "scenario 2 (Observe)" "${LAST_SYNCED_MESSAGE}" "observe failed: "

raw="$(python3 -c "
import urllib.parse
print(urllib.parse.urlencode({'*' + '${SCRATCH_KEY}': 'probe'}))
")"
log "raw WAPI error for the same search, captured independently:"
curl -sk -u "${INFOBLOX_USER}:${INFOBLOX_PASS}" "${WAPI_BASE}/record:a?${raw}" -w '\nHTTP %{http_code}\n'

# ── Scenario 6: Delete()'s lifecycle while the definition is absent, and
#    the recovery gap this scenario exposes ─────────────────────────────
#
# idp-s1 is still staled from scenario 2 above — its stored reference
# 404s, and the scratch definition is still absent. Every reconcile
# resolves this SAME (external-name, uid) pair through Observe() before
# Delete() is ever considered: Observe() has to run first regardless of
# whether a deletion is in flight, and Delete() is only called when
# Observe() just reported the object exists. Since Observe()'s own
# resolution is refusing right now (scenario 2), requesting deletion here
# cannot reach Delete()'s independent reactive guard at controller.go:791
# — Observe() refuses on the very same reconcile and the reconciler never
# gets past it to call Delete() at all. What IS live-verifiable, and is
# asserted below, is that THIS part is safe: no orphan, no
# silently-successful delete while the definition stays absent.
#
# The part that is NOT safe, live-verified in the same run this scenario
# performs: once the definition is recreated (below), the identity-EA
# search starts succeeding again but matches zero objects, because
# deleting the definition also wiped every object's stamp (ADR-IN-0006
# §4) — recreating the definition does not restore old stamp values.
# Observe() reads that zero-match search as "genuinely absent" (no
# error), so the reconciler's WasDeleted branch skips Delete() entirely
# and removes the finalizer — the Kubernetes object disappears cleanly,
# but the Grid object is never actually deleted. This scenario's own
# wait_deleted call below only proves the Kubernetes side converged; it
# does not prove the backend was touched, and a direct Grid read-back
# after this script's own full run confirmed the object survives,
# unstamped, exactly as this comment describes. That is a real gap in
# the identity ladder's delete path, tracked separately — not something
# this test-only script can fix.
log "=== scenario 6: an in-flight delete of a stale-ref object is blocked safely by Observe(), not silently completed ==="
${KUBECTL} delete arecord.recorda.infobloxnios.crossplane.io/idp-s1-${RUN_TOKEN} --wait=false
deletion_ts="$(${KUBECTL} get arecord.recorda.infobloxnios.crossplane.io/idp-s1-${RUN_TOKEN} -o jsonpath='{.metadata.deletionTimestamp}' 2>/dev/null || true)"
[ -n "${deletion_ts}" ] || { echo "FATAL: idp-s1 has no deletionTimestamp after kubectl delete" >&2; exit 1; }
wait_synced arecord.recorda.infobloxnios.crossplane.io "idp-s1-${RUN_TOKEN}" cluster False 60
assert_condition_prefix "scenario 6 (in-flight delete, blocked by Observe)" "${LAST_SYNCED_MESSAGE}" "observe failed: "
${KUBECTL} get arecord.recorda.infobloxnios.crossplane.io/idp-s1-${RUN_TOKEN} >/dev/null 2>&1 || {
  echo "FATAL: idp-s1 disappeared from Kubernetes despite Observe() refusing — this would mean the delete completed (or the finalizer was dropped) without ever proving ownership, exactly the silent-loss failure mode ADR-IN-0006 §5 exists to close" >&2
  exit 1
}
found="$(curl_wapi "${INFOBLOX_USER}" "${INFOBLOX_PASS}" GET "record:a?name=idp-s1-${RUN_TOKEN}.example.com")"
[ "${found}" != "[]" ] || { echo "FATAL: idp-s1's Grid object is gone — it should still exist, untouched, while the delete is blocked" >&2; exit 1; }
log "scenario 6 setup PASSED — deletion is in flight (deletionTimestamp ${deletion_ts}), Observe() refuses, no orphan, Grid object intact. The Kubernetes side is expected to converge once the definition is recreated below, but that convergence does NOT prove the Grid object was deleted — see this scenario's header comment."

# Restore idp-s1-ns's real reference — only the cluster-scoped sibling is
# being carried into the delete scenario above, so the namespaced one goes
# back to normal now.
${KUBECTL} annotate arecord.recorda.infobloxnios.m.crossplane.io/idp-s1-ns-${RUN_TOKEN} -n default \
  crossplane.io/external-name="${ns_ref}" --overwrite

# ── Scenario 3: restricted credential's Observe() refuses a brand-new
#    object before any mutating call ───────────────────────────────────────
#
# This is Observe()'s reactive guard (controller.go:732) again, not
# Create()'s unconditional one (cluster.go:199): a brand-new object's
# external-name still equals its Kubernetes name, so observeRefFor returns
# "" and Resolve goes straight to the identity-EA search — which fails
# first, refusing before Create() is ever reached. See scenario 5 below
# for Create()'s own guard.
log "=== scenario 3: Observe()'s reactive guard refuses a brand-new object under the restricted credential ==="
apply_arecord "idp-s2-${RUN_TOKEN}" "192.0.2.160" cluster
apply_arecord "idp-s2-ns-${RUN_TOKEN}" "192.0.2.161" namespaced
wait_synced arecord.recorda.infobloxnios.crossplane.io "idp-s2-${RUN_TOKEN}" cluster False 60
assert_condition_prefix "scenario 3 (Observe, cluster)" "${LAST_SYNCED_MESSAGE}" "observe failed: "
wait_synced arecord.recorda.infobloxnios.m.crossplane.io "idp-s2-ns-${RUN_TOKEN}" namespaced False 60
assert_condition_prefix "scenario 3 (Observe, namespaced)" "${LAST_SYNCED_MESSAGE}" "observe failed: "

for n in "idp-s2-${RUN_TOKEN}" "idp-s2-ns-${RUN_TOKEN}"; do
  found="$(curl_wapi "${INFOBLOX_USER}" "${INFOBLOX_PASS}" GET "record:a?name=${n}.example.com")"
  [ "${found}" = "[]" ] || { echo "FATAL: ${n}.example.com was created on the Grid despite the refusal" >&2; exit 1; }
done
log "scenario 3 PASSED — both scopes refused via Observe(), no record:a created on the Grid"

# ── Recreate the definition (superuser, out of band) ───────────────────────
#
# This does double duty: it is the setup both scenario 5 (below, while the
# stale-negative verdict this run cached during scenario 3 is still within
# its TTL) and scenario 4 (further below, once that same cache entry
# expires) depend on.
log "recreating the scratch definition as superuser..."
recreate_ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
recreate_scratch_definition
log "recreated at ${recreate_ts}"

# ── Scenario 5: Create()'s unconditional guard (cluster.go:199) ───────────
#
# Reaching this specific guard in the refuse direction needs Observe() to
# conclude "does not exist" WITHOUT an error — which requires the identity
# search to succeed — while Create()'s own prerequisite check still
# refuses. Those two requirements look contradictory (if the search just
# succeeded, doesn't that mean the definition is fine?) unless the
# prerequisite CHECK and the prerequisite's REALITY are allowed to
# disagree for a bounded window, which is exactly what identity.Prober's
# cache does: scenario 3 immediately above populated a NEGATIVE verdict
# for this Grid endpoint (the restricted credential couldn't create the
# definition), cached for up to identity.DefaultProbeTTL (5 minutes)
# regardless of which credential asks next. The definition was just
# recreated (as superuser, out of band, above) — real enough for a
# brand-new object's live identity-EA search to succeed — but the cached
# verdict has not been re-checked yet. A brand-new object's Observe()
# therefore resolves cleanly (its search never even touches the cache —
# only a search FAILURE triggers ensureIdentityPrerequisite), and Create()
# is invoked; Create()'s own ensureIdentityPrerequisite call hits the
# still-cached refusal instantly, without a network round trip. This
# requires no race: only that scenario 5 runs well inside the 5-minute
# window scenario 3 started, which it comfortably does.
log "=== scenario 5: Create()'s unconditional guard refuses a brand-new object via the still-cached refusal ==="
apply_arecord "idp-s4-${RUN_TOKEN}" "192.0.2.170" cluster
apply_arecord "idp-s4-ns-${RUN_TOKEN}" "192.0.2.171" namespaced
wait_synced arecord.recorda.infobloxnios.crossplane.io "idp-s4-${RUN_TOKEN}" cluster False 60
assert_condition_prefix "scenario 5 (Create, cluster)" "${LAST_SYNCED_MESSAGE}" "create failed: "
wait_synced arecord.recorda.infobloxnios.m.crossplane.io "idp-s4-ns-${RUN_TOKEN}" namespaced False 60
assert_condition_prefix "scenario 5 (Create, namespaced)" "${LAST_SYNCED_MESSAGE}" "create failed: "

for n in "idp-s4-${RUN_TOKEN}" "idp-s4-ns-${RUN_TOKEN}"; do
  found="$(curl_wapi "${INFOBLOX_USER}" "${INFOBLOX_PASS}" GET "record:a?name=${n}.example.com")"
  [ "${found}" = "[]" ] || { echo "FATAL: ${n}.example.com was created on the Grid despite Create()'s refusal" >&2; exit 1; }
done
log "scenario 5 PASSED — both scopes refused via Create(), no record:a created on the Grid"

# ── Scenario 4: recreating the definition converges within the cached-
#    verdict TTL ────────────────────────────────────────────────────────
log "=== scenario 4: definition recreation (above) converges every remaining resource within the cached-verdict TTL ==="
pod_restarts_before="$(${KUBECTL} -n crossplane-system get "${PROVIDER_POD}" -o jsonpath='{.status.containerStatuses[0].restartCount}')"

wait_synced arecord.recorda.infobloxnios.crossplane.io "idp-s2-${RUN_TOKEN}" cluster True 320
converge_ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
wait_synced arecord.recorda.infobloxnios.m.crossplane.io "idp-s2-ns-${RUN_TOKEN}" namespaced True 60
# idp-s1 (cluster) is mid-deletion (scenario 6) rather than converging back
# to Ready — its recovery is that the delete itself completes.
wait_deleted arecord.recorda.infobloxnios.crossplane.io "idp-s1-${RUN_TOKEN}" cluster 90
wait_synced arecord.recorda.infobloxnios.m.crossplane.io "idp-s1-ns-${RUN_TOKEN}" namespaced True 60

pod_restarts_after="$(${KUBECTL} -n crossplane-system get "${PROVIDER_POD}" -o jsonpath='{.status.containerStatuses[0].restartCount}')"
pod_start_after="$(${KUBECTL} -n crossplane-system get "${PROVIDER_POD}" -o jsonpath='{.status.startTime}')"
[ "${pod_start_after}" = "${POD_START}" ] || { echo "FATAL: provider pod restarted (start time changed from ${POD_START} to ${pod_start_after})" >&2; exit 1; }
[ "${pod_restarts_before}" = "${pod_restarts_after}" ] || { echo "FATAL: provider pod restart count changed (${pod_restarts_before} -> ${pod_restarts_after})" >&2; exit 1; }
log "scenario 4 PASSED — converged at ${converge_ts} (recreated at ${recreate_ts}), idp-s1's Kubernetes object finished deleting (scenario 6) — this confirms Kubernetes convergence only, not that the Grid object was deleted (see scenario 6's header comment), pod start time unchanged (${POD_START}), restarts unchanged (${pod_restarts_after})"

log "all scenarios passed"
