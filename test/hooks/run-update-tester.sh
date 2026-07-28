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
# Usage (via symlink):
#   test/hooks/post-assert-record-txt.sh
#   test/hooks/post-assert-record-txt-namespaced.sh
#
# Direct invocation for debugging:
#   MANIFEST=/path/to/manifest.yaml test/hooks/run-update-tester.sh
#   (When $MANIFEST is set directly it overrides the $0-derived path.)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROVIDER_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Derive resource slug and manifest path from the invocation name ($0).
# $0 is the symlink name when called via a symlink — bash does NOT resolve
# symlinks for $0 or BASH_SOURCE[0] before basename extraction.
BASENAME="$(basename "$0" .sh)"      # e.g. post-assert-record-txt-namespaced
SLUG="${BASENAME#post-assert-}"       # e.g. record-txt-namespaced

if [ -z "${MANIFEST:-}" ]; then
  # Detect namespaced variants: suffix is either "-namespaced" or "-ns".
  if [[ "$SLUG" == *-namespaced ]]; then
    RESOURCE="${SLUG%-namespaced}"    # strip -namespaced suffix
    MANIFEST="$PROVIDER_ROOT/examples/$RESOURCE/$RESOURCE-namespaced.yaml"
  elif [[ "$SLUG" == *-ns ]]; then
    RESOURCE="${SLUG%-ns}"            # strip -ns suffix
    MANIFEST="$PROVIDER_ROOT/examples/$RESOURCE/$RESOURCE-namespaced.yaml"
  else
    RESOURCE="$SLUG"
    MANIFEST="$PROVIDER_ROOT/examples/$RESOURCE/$RESOURCE.yaml"
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

# B. Per-field update tests.
echo "==> run-update-tester: run $MANIFEST"
"$UPDATE_TESTER" run "$MANIFEST"
