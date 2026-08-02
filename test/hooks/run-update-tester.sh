#!/usr/bin/env bash
# run-update-tester.sh — generic post-assert hook (per-field update-tester convention)
#
# This is the SINGLE script that all per-resource post-assert symlinks target.
# It derives the manifest path from $0 (the symlink name) so the same script
# serves every resource without modification.
#
# Symlink naming convention (unified example-manifest convention):
#   test/hooks/post-assert-<resource>.sh            → examples/<resource>/<resource>.yaml
#   test/hooks/post-assert-<resource>-namespaced.sh → examples/<resource>/<resource>-namespaced.yaml
#   test/hooks/post-assert-<resource>-ns.sh         → examples/<resource>/<resource>-namespaced.yaml
#
# A resource directory may also hold more than one example pair for the
# same kind — e.g. an alternate address-family variant such as
# examples/network/network-v6.yaml sitting alongside network.yaml. When the
# primary derivation's directory (named after the full slug) doesn't exist,
# a fallback strips the slug's last hyphenated segment and looks for the
# full slug's manifest inside THAT (shorter) resource directory instead —
# so post-assert-network-v6.sh resolves to examples/network/network-v6.yaml.
#
# Usage (via symlink):
#   test/hooks/post-assert-<resource>.sh
#   test/hooks/post-assert-<resource>-namespaced.sh
#
# Direct invocation for debugging:
#   MANIFEST=/path/to/manifest.yaml test/hooks/run-update-tester.sh
#   (When $MANIFEST is set directly it overrides the $0-derived path.)
#
# ── Enable-flag / companion-list trap ──────────────────────────────────
# When authoring a crossplane.io/update-test annotation, watch for boolean
# "enable"-style fields (e.g. enableBlacklist, dns64Enabled — as opposed to
# the paired "use<X>" override flags, which are always safe to flip alone)
# that turn on a feature backed by a list/struct field. Some of these
# enable flags carry a hidden backend business rule requiring the
# companion list to already be non-empty — setting the flag true against
# a minimal example (whose companion list is empty and therefore skipped
# elsewhere in the same annotation) gets rejected by the API, and because
# per-field tests apply as cumulative patches, that single rejection can
# leave the resource in a state where every later field in the same test
# run also fails to reconcile. This does not show up as a fast, readable
# error — it wedges the whole per-field run for anyone who happens to
# trigger that resource's E2E next.
#
# The doc comment on the Go struct field is NOT a reliable signal either
# way: fields with a real hidden requirement and fields without one can
# both read "Determines if X is enabled or not" with no mention of the
# dependency. When a resource has this enable-flag/companion-list shape,
# reason from the target API's own docs first. If the field is still
# ambiguous, run a quick, isolated probe against a scratch object (create
# → PUT the candidate field → observe the response → delete) rather than
# relying on a full E2E cycle to surface the answer — and always clean the
# scratch object up afterward. If the probe (or the API docs) confirms the
# enable flag requires non-empty companion data that the example doesn't
# provide, skip the enable flag with a reason naming the companion field,
# and keep the paired "use" override flag value-tested (flipping the
# override alone is always a valid, harmless state). See
# examples/dns-view/dns-view.yaml for a worked example of this pattern.
# ─────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROVIDER_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Derive resource slug and manifest path from the invocation name ($0).
# $0 is the symlink name when called via a symlink — bash does NOT resolve
# symlinks for $0 or BASH_SOURCE[0] before basename extraction.
BASENAME="$(basename "$0" .sh)"      # e.g. post-assert-<resource>-namespaced
SLUG="${BASENAME#post-assert-}"       # e.g. <resource>-namespaced

if [ -z "${MANIFEST:-}" ]; then
  # Detect namespaced variants: suffix is either "-namespaced" or "-ns".
  if [[ "$SLUG" == *-namespaced ]]; then
    RESOURCE="${SLUG%-namespaced}"    # strip -namespaced suffix
    MANIFEST="$PROVIDER_ROOT/examples/$RESOURCE/$RESOURCE-namespaced.yaml"
  elif [[ "$SLUG" == *-ns ]]; then
    # Check if the full slug is itself a resource directory first. This
    # avoids collisions between resource names that legitimately end in
    # "-ns" (e.g. record-ns for DNS NS records) and the "-ns" shorthand
    # used elsewhere for "-namespaced".
    if [ -d "$PROVIDER_ROOT/examples/$SLUG" ]; then
      RESOURCE="$SLUG"
      MANIFEST="$PROVIDER_ROOT/examples/$RESOURCE/$RESOURCE.yaml"
    else
      RESOURCE="${SLUG%-ns}"            # strip -ns suffix
      MANIFEST="$PROVIDER_ROOT/examples/$RESOURCE/$RESOURCE-namespaced.yaml"
    fi
  else
    RESOURCE="$SLUG"
    MANIFEST="$PROVIDER_ROOT/examples/$RESOURCE/$RESOURCE.yaml"
  fi

  # Sibling-variant fallback (see the header comment above). Only engages
  # when the primary derivation's manifest is missing, so it never changes
  # behavior for any symlink whose slug already resolves directly.
  if [ ! -f "$MANIFEST" ] && [[ "$RESOURCE" == *-* ]]; then
    LEAF="$(basename "$MANIFEST")"
    PARENT="${RESOURCE%-*}"
    ALT="$PROVIDER_ROOT/examples/$PARENT/$LEAF"
    if [ -f "$ALT" ]; then
      MANIFEST="$ALT"
    fi
  fi
fi

if [ ! -f "$MANIFEST" ]; then
  echo "==> run-update-tester: manifest not found: $MANIFEST" >&2
  echo "    Derived from: $0 (basename=$BASENAME slug=$SLUG)" >&2
  exit 1
fi

# Build the update-tester binary if it is missing or stale. A staleness
# check (not just an existence check) is required: the binary may be
# committed to the repo as a convenience snapshot, and a committed binary
# that predates a source fix would silently reintroduce the bug the fix
# addressed (e.g. an observedGeneration read-path fix) until someone
# remembered to rebuild it by hand.
UPDATE_TESTER="${UPDATE_TESTER:-$PROVIDER_ROOT/tools/update-tester/update-tester}"
UPDATE_TESTER_SRC_DIR="$PROVIDER_ROOT/tools/update-tester"
if [ ! -x "$UPDATE_TESTER" ] || [ -n "$(find "$UPDATE_TESTER_SRC_DIR" -maxdepth 1 -name '*.go' -newer "$UPDATE_TESTER" -print -quit 2>/dev/null)" ]; then
  echo "==> run-update-tester: building update-tester (missing or stale)..."
  (cd "$UPDATE_TESTER_SRC_DIR" && go build -o update-tester .)
fi

# A. Convergence check — abort if the resource is looping. `set -euo
# pipefail` (above) means a non-zero exit here aborts the script
# immediately, so the per-field tests below never run against an unstable
# resource.
echo "==> run-update-tester: converge $MANIFEST"
"$UPDATE_TESTER" converge "$MANIFEST"

# A2. External-name object-type check — only when the manifest declares an
# expectation via crossplane.io/expect-external-name-prefix. Guards against
# the dual-object-type silent-duplicate hazard some WAPI resources have
# (e.g. Network models both "network" and "ipv6network"): an identity
# search issued against the wrong object type returns zero matches and the
# reconciler creates a duplicate while still reporting Ready — invisible to
# a plain Ready assertion. Runs right after Create (before the per-field
# update tests below) because object type is fixed at Create and should
# never need re-checking after. See examples/network/network-v6.yaml.
if grep -q 'crossplane.io/expect-external-name-prefix:' "$MANIFEST"; then
  echo "==> run-update-tester: check-external-name-prefix $MANIFEST"
  "$UPDATE_TESTER" check-external-name-prefix "$MANIFEST"

  # A3. Ref-less identity-resolve recovery check — same gate, same manifest
  # annotation. A2 above only proves the object type resolved correctly at
  # CREATE time; it can never fail, because create-time object-type
  # selection is independent of the identity-search hazard entirely (see
  # the file header). This step drives the hazard's actual code path: it
  # pauses reconciliation, strips crossplane.io/external-name, unpauses,
  # and asserts the controller recovers to the SAME backend object
  # (exactly one CreatedExternalResource event across the resource's
  # lifecycle) instead of silently creating a duplicate. Runs before the
  # per-field update tests below so its own patch/pause machinery never
  # interleaves with theirs.
  echo "==> run-update-tester: resolve-recover $MANIFEST"
  "$UPDATE_TESTER" resolve-recover "$MANIFEST"
fi

# B. Per-field update tests.
echo "==> run-update-tester: run $MANIFEST"
"$UPDATE_TESTER" run "$MANIFEST"

# C. Post-update convergence check — the field updates in step B just
# proved a value round-trips correctly, but that alone does not prove the
# controller stopped reconciling. Asserting only Ready here would have
# missed a real field-reported bug: a resource stuck in a perpetual
# Update loop still reports Ready on every cycle, so the loop only shows
# up as a non-zero Update event count over a wait window. Re-running the
# same convergence check used in step A, now after every field
# transition in this manifest's update-test annotation (including any
# use-flag toggles), confirms the resource settles instead of just
# announcing Ready once.
echo "==> run-update-tester: post-update converge $MANIFEST"
"$UPDATE_TESTER" converge "$MANIFEST"
