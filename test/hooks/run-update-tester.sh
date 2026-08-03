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
