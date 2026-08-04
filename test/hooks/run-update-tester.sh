#!/usr/bin/env bash
# run-update-tester.sh — generic post-assert hook (per-field update-tester convention)
#
# This is the SINGLE script that all per-resource post-assert symlinks target.
# It derives the manifest path from the invocation name ($0, the symlink
# name — bash does not resolve symlinks before `basename`), so the same
# script serves every resource without modification. All derivation,
# validation, and check-running logic lives in the shared
# crossplane-update-tester tool, consumed here as a pinned module in
# tools/update-tester; this wrapper does no work of its own beyond
# resolving the repo root and exec-ing into it.
#
# The step sequence the `hook` subcommand runs for every manifest is:
# converge, then — ONLY when the manifest carries the
# crossplane.io/expect-external-name-prefix annotation — check-external-name-prefix
# followed by resolve-recover (pause the MR, strip crossplane.io/external-name,
# unpause, assert it re-resolves to the SAME backend object rather than
# creating a second one or wedging), then run, then a final converge. The
# prefix annotation alone only proves the create path picked the right
# object-type variant; resolve-recover is what proves the runtime search path
# (Observe with no stored handle) resolves to the same variant instead of
# silently creating a duplicate. See tools/update-tester's own README and
# test/hooks/resolve_recover_wiring_test.go, which drives this exact wiring
# against a fake backend and proves the resolve-recover step disappears when
# the annotation is absent.
#
# Symlink naming convention:
#   test/hooks/post-assert-<resource>.sh            → examples/<resource>/<resource>.yaml
#   test/hooks/post-assert-<resource>-namespaced.sh → examples/<resource>/<resource>-namespaced.yaml
#   test/hooks/post-assert-<resource>-ns.sh         → examples/<resource>/<resource>-namespaced.yaml
#
# See the tool's own README (tools/update-tester) for the full derivation
# rules, including the directory-existence fallback for a resource with more
# than one example variant. Manifest-authoring guidance for this provider —
# including the enable-flag / companion-list trap — lives in this provider's
# AGENTS.md, not in this script.
#
# Direct invocation for debugging:
#   MANIFEST=/path/to/manifest.yaml test/hooks/run-update-tester.sh
#   (When $MANIFEST is set directly it overrides the $0-derived path.)
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
exec go -C "$ROOT/tools/update-tester" tool crossplane-update-tester \
  hook "$(basename "$0")" --root "$ROOT"
