#!/usr/bin/env bash
# test/e2e/reap.sh — Token/prefix-scoped orphan reaper for the shared NIOS
# Grid Manager.
#
# ADR-IN-0007 gives every E2E run its own backend identities: a runToken
# spliced into NAMED objects (DNS records, zones, DTC objects, templates, EA
# definitions) and a netPrefix /24 spliced into ADDRESSED objects (Network,
# NetworkContainer, Range, FixedAddress). Before that mechanism, an object
# left behind by a killed run was indistinguishable from another run's live
# object, so cleanup had to be a manual, conservative ritual. Now cleanup is
# a token-scoped WAPI query: this script is that query.
#
# ============================================================================
# THE SAFETY PROPERTY THAT MATTERS MOST
# ============================================================================
# A reaper that deletes broadly is worse than no reaper. Concurrent E2E runs
# against this Grid are the normal case, not the exception. A sweep that
# matches "everything that looks like a test object" will delete another
# run's live object mid-test and produce a failure that looks exactly like a
# provider defect.
#
# So this script refuses to run unscoped:
#   - Routine mode requires --token and/or --net-prefix, supplied by the
#     caller (normally the same seed gen-datasource.sh derived for the run
#     being cleaned up — see test/e2e/gen-datasource.sh).
#   - --token must match gen-datasource.sh's own output format exactly
#     (10 lowercase hex chars). A malformed or overly-short token could
#     become a regex that matches far more than one run's objects — this
#     script rejects anything else before issuing a single WAPI query.
#   - --net-prefix must be a /24 inside 100.64.0.0/16 (the block
#     gen-datasource.sh draws from). A prefix outside that block is refused
#     — it could otherwise be pointed at a literal CIDR a live example
#     depends on.
#   - Dry-run is the default. Nothing is deleted unless the caller passes
#     --apply.
#
# ============================================================================
# THE DENY-LIST
# ============================================================================
# test/setup.sh provisions prerequisites that are shared CONTEXT, not any
# run's identity (ADR-IN-0007's central distinction), and must survive every
# sweep this script performs, in every mode:
#   - zone_auth  fqdn=example.com        view=default
#   - network    network=10.0.0.0/24     network_view=default
#   - network    network=203.0.113.0/25  network_view=default
#   - network    network=203.0.113.128/25 network_view=default
# These four are hard-coded in is_denied() below and checked before every
# delete (and every dry-run listing), regardless of --token, --net-prefix, or
# --literal-sweep. If deleted, setup.sh must re-provision them before any
# subsequent run's DNS records or shared-network examples pass again.
#
# Two more zone_auth identities are Grid-baseline shared context that
# test/setup.sh does NOT provision (they pre-exist on the shared Grid
# Manager itself) but that record-ns's examples depend on as the
# pre-existing zone a delegation record lives under (see EXCEPTIONS below):
#   - zone_auth  fqdn=example.com        view=Internal
#   - zone_auth  fqdn=example.com        view=External
#   - zone_auth  fqdn=peatestinglab.com  view=Internal
# is_denied() protects fqdn=example.com in EVERY view (not just default),
# and fqdn=peatestinglab.com in every view, for the same reason: the
# deny-list exists to protect shared context regardless of which script
# provisioned it, and narrowing it to "only what setup.sh itself creates"
# would leave a Grid-baseline zone unprotected against a broad or malformed
# future query.
#
# ============================================================================
# COVERAGE — every WAPI object type this provider's examples create
# ============================================================================
# Twenty-five example families exist under examples/ (excluding
# examples/provider/, which is ProviderConfig sample config, not a resource
# family). Each maps to exactly one WAPI object type below. Classification
# (name-identified / address-identified / both) is derived from the
# controller's *ExistsByNaturalKey natural-key logic, not assumed:
#
#   Family                    WAPI type                Reaped by
#   ------                    ---------                ---------
#   record-a                  record:a                 name  (token)
#   record-aaaa                record:aaaa               name  (token)
#   record-alias                record:alias              name  (token)
#   record-cname                record:cname              name  (token)
#   record-mx                  record:mx                 name  (token)
#   record-srv                 record:srv                name  (token)
#   record-txt                 record:txt                name  (token)
#   record-ns                  record:ns                 nameserver (token) — see EXCEPTIONS
#   host-record                 record:host               name (token) AND ipv4addr (prefix)
#   record-ptr                  record:ptr                ipv4addr (prefix) — address-primary
#   zone-auth                  zone_auth                 fqdn  (token)
#   zone-delegated               zone_delegated            fqdn  (token)
#   zone-forward                zone_forward              fqdn  (token)
#   dns-view                   view                      name  (token)
#   network-view                 networkview               name  (token)
#   dtc-pool                   dtc:pool                  name  (token)
#   dtc-server                  dtc:server                name  (token)
#   dtc-lbdn                   dtc:lbdn                  name  (token)
#   range-template               rangetemplate             name  (token)
#   extensible-attribute-def       extensibleattributedef    name  (token)
#   ipv4-shared-network           sharednetwork             name  (token) — see EXCEPTIONS
#   network                    network / ipv6network     network (prefix / v6 prefix)
#   network-container             networkcontainer / ipv6networkcontainer  network (prefix / v6 prefix)
#   range                      range                     start_addr (prefix)
#   fixed-address                fixedaddress / ipv6fixedaddress  ipv4addr / ipv6addr (prefix / v6 prefix)
#
# ============================================================================
# EXCEPTIONS — resources whose objects cannot be cleanly token-scoped
# ============================================================================
# record-ns: the "name" field is a PRE-EXISTING zone the record lives under
#   (shared context — e.g. peatestinglab.com / example.com in the current
#   examples), not this run's identity, and must never be matched or deleted
#   by this script. "nameserver" is the tokenised component (name/view stay
#   fixed shared context) — every current example already writes
#   nameserver as ns1[-ns]-${data.runToken}.example.com, so routine mode's
#   nameserver-token match is live, not aspirational. record:ns objects
#   predating that change (literal "ns1.example.com") are handled by
#   --literal-sweep instead (see below), which matches the full (name,
#   nameserver, view) triple — it never deletes by name alone.
#
# ipv4-shared-network: the object's own "name" tokenises and reaps normally.
#   Its "networks" member list references Network objects that each belong
#   to exactly one shared network — tokenising the shared network's name is
#   necessary but does not by itself give the member networks a per-run
#   identity. Those member networks are provisioned by test/setup.sh
#   (203.0.113.0/25 and .128/25) and are permanently deny-listed — they are
#   shared context, reaped by nothing, ever, in any mode.
#
# record-ptr / host-record: both have a natural key that mixes a name-ish
#   field and an address. Reaped by the union of a token match (if the name
#   half is ever tokenised) and a prefix match on the address half, so it
#   works whichever half of the mechanism a given resource ends up using.
#
# network / IPv6: gen-datasource.sh gives Network's, NetworkContainer's, and
#   FixedAddress's IPv6 (ipv6network / ipv6networkcontainer / ipv6fixedaddress)
#   variants per-run identities inside the RFC 3849 documentation range
#   (2001:db8::/32) — see its netV6 section. The third hextet is the SAME
#   BLOCK_INDEX byte --net-prefix's IPv4 /24 already draws (no second hash
#   draw, no new flag), so this script derives an IPv6 prefix regex from
#   --net-prefix itself: given --net-prefix=100.64.<N>.0/24, it matches
#   2001:db8:<hex(N)>:*, where hex(N) is the UNPADDED lowercase hex spelling
#   of N with an optional leading zero (e.g. N=14 matches both "e" and "0e").
#   The Grid canonicalizes the stored address per RFC 5952, which strips a
#   lone hextet's leading zero, so for BLOCK_INDEX 0-15 the padded two-digit
#   spelling gen-datasource.sh submits (e.g. "0e") is never what comes back
#   from a query — only the unpadded form ("e") is. BLOCK_INDEX 16-255
#   already render as two non-zero-leading hex digits, where padded and
#   canonical agree. The optional leading zero in the regex matches the
#   canonical stored form across all 256 possible BLOCK_INDEX values without
#   needing to know in advance whether a given index falls in the padded or
#   unpadded case. Routine mode therefore reaps ipv6network /
#   ipv6networkcontainer (network field) and ipv6fixedaddress (ipv6addr
#   field) whenever --net-prefix is supplied, alongside their IPv4
#   counterparts — see routine_reap()'s IPV6_ADDRESSED_TYPES table. No other
#   family has an IPv6 variant in examples/ today (record-aaaa is a NAMED
#   AAAA record, already covered by its own token match, not an ADDRESSED
#   object needing prefix scoping); if one is added, extend
#   IPV6_ADDRESSED_TYPES rather than introducing a second mechanism.
#
# ============================================================================
# THE ONE-SHOT PRE-TOKENISATION SWEEP
# ============================================================================
# Before this mechanism existed, every run used the SAME literal identities
# (www.example.com, test-dtc-pool, ...). Once a resource family's rollout
# wave has actually merged, no run creates that family's literal identities
# any more, and a Grid object still carrying one is definitionally an
# orphan of unknown age from before tokenisation.
#
# The rollout ships in four independent waves that land on main at
# different times, so at any given moment some families are already
# tokenised and others are not. A static literal table would go stale the
# instant it shipped: a family whose wave had not yet merged would still
# create the "pre-tokenisation" literal for real, and
# --literal-sweep --apply would delete a concurrently running, live E2E
# run's object — exactly the failure class this whole mechanism exists to
# remove. So this table is NOT trusted at face value: before ANY entry is
# queried, is_literal_live() re-derives liveness straight from examples/ at
# runtime (see below) and skips the entry if the identity is still shipped
# there. The table below only ever shrinks the amount of work verified —
# it can never cause a live object to be deleted, regardless of which
# waves have merged when this script runs.
#
# This is also a genuinely different operation from the routine reap above:
#   - it matches fixed literal identities, not a caller-supplied token/prefix
#   - it is opt-in (--literal-sweep) and mutually exclusive with --token /
#     --net-prefix — routine and historical cleanup are never combined into
#     one broader query
#   - it is meant to run once, by a human or a one-off ant task, never as
#     part of the automatic per-run cleanup a live E2E run performs
# The literal table (LITERAL_SWEEP below) is the candidate set: what
# examples/ shipped pre-tokenisation, cross-checked directly against every
# example manifest in this repo at the time this table was written (not
# copied from an earlier draft of this ticket — several of that draft's
# names did not match the shipped examples, e.g. "example-ns.com" is
# zone-auth's namespaced fqdn, not record-ns's identity, and record-alias's
# literal is "alias-test.example.com" not "alias.example.com"). record-a's
# literals (www.example.com, www-ns.example.com) are included for the
# historical record even though IN-E2E-ISO-MECH already tokenised that
# family — any Grid object still carrying them predates that change.
#
# ============================================================================
# THE LIVENESS GUARD (is_literal_live)
# ============================================================================
# For each --literal-sweep table entry, only the field(s) that carry this
# object type's RUN IDENTITY are checked for liveness — the same token_field
# / addr_field columns the routine reaper above matches on (see
# get_identity_fields()). Fields in the same entry that are shared Grid
# CONTEXT, not identity, are deliberately excluded from the check: record:ns's
# "name" is a pre-existing zone every run reuses on purpose (see EXCEPTIONS
# above), so checking it would find a match on every run and permanently
# skip record:ns's literal-sweep entries even after they become genuine
# orphans. Checking only "nameserver" (record:ns's actual identity field)
# avoids that false negative.
#
# The check itself greps that object type's OWN example directory
# (examples/<family>/*.yaml, comments stripped) for the literal value as a
# fixed string — scoped to one family's directory, not the whole examples/
# tree, because the same literal string can legitimately appear as a
# reference VALUE in an unrelated family's example (e.g. record-a's retired
# identity "www.example.com" is still record-cname's live `canonical`
# target and record-ptr's live `ptrdname` target — neither is record-a's
# own identity). A whole-tree grep would treat those as false positives and
# refuse to ever reap record-a's pre-tokenisation orphans.
#
# The grep is further scoped to each file's spec.forProvider block, not the
# whole file, because a WAPI identity can only ever live there. Two families
# (range-template, extensible-attribute-def) happen to reuse the
# pre-tokenisation literal as their Crossplane CR's metadata.name — a
# Kubernetes object name with no relationship to the Grid identity. Without
# this restriction that coincidence reads as "still live" forever and those
# families' genuine pre-rollout orphans could never be reaped by this sweep.
#
# ============================================================================
# USAGE
# ============================================================================
#   test/e2e/reap.sh --token=<runToken> [--net-prefix=<CIDR>] [--apply]
#   test/e2e/reap.sh --net-prefix=<CIDR> [--apply]
#   test/e2e/reap.sh --literal-sweep [--apply]
#
#   --token=TOKEN       10-char lowercase hex runToken (gen-datasource.sh
#                        format). Reaps NAMED objects whose identity field
#                        contains this token.
#   --net-prefix=CIDR    A /24 inside 100.64.0.0/16 (gen-datasource.sh
#                        format). Reaps ADDRESSED objects whose address
#                        falls inside this prefix.
#   --literal-sweep      One-shot historical mode. Mutually exclusive with
#                        --token/--net-prefix. Matches the fixed literal
#                        identities examples/ used before tokenisation —
#                        skipping (with a loud warning) any entry whose
#                        identity is still shipped live in examples/ today,
#                        so a not-yet-merged wave's still-literal identity
#                        is never at risk (see THE LIVENESS GUARD above).
#   --apply              Actually delete. Without it, every match is only
#                        listed (dry-run is the default, deliberately —
#                        cleanup runs unattended after E2E and a destructive
#                        default is the wrong failure mode for a scoping
#                        bug).
#   --wapi-version=VER   Defaults to $INFOBLOX_WAPI_VERSION or v2.13.1,
#                        matching test/setup.sh.
#
# Requires INFOBLOX_HOST, INFOBLOX_USER, INFOBLOX_PASS (same as
# test/setup.sh). Idempotent: running with nothing left to reap exits 0 and
# prints a summary, never an error.
set -euo pipefail

# Repo root, derived from this script's own location — not the caller's
# CWD — so the examples/ liveness check (is_literal_live, below) resolves
# correctly regardless of where reap.sh is invoked from.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  test/e2e/reap.sh --token=<runToken> [--net-prefix=<CIDR>] [--apply]
  test/e2e/reap.sh --net-prefix=<CIDR> [--apply]
  test/e2e/reap.sh --literal-sweep [--apply]
  test/e2e/reap.sh --net-prefix=<CIDR> --print-scope

  --token=TOKEN        10-char lowercase hex runToken (gen-datasource.sh
                        format). Reaps NAMED objects whose identity field
                        contains this token.
  --net-prefix=CIDR     A /24 inside 100.64.0.0/16 (gen-datasource.sh
                        format). Reaps ADDRESSED objects whose address
                        falls inside this prefix.
  --literal-sweep       One-shot historical mode. Mutually exclusive with
                        --token/--net-prefix. Matches the fixed literal
                        identities examples/ used before tokenisation,
                        skipping (with a warning) any entry still shipped
                        live in examples/ today.
  --apply               Actually delete. Without it, every match is only
                        listed (dry-run is the default).
  --print-scope         Print the IPv6 object-type/field/regex triples this
                        run's --net-prefix would sweep, one per line, then
                        exit 0 without contacting the Grid. Requires no
                        INFOBLOX_HOST/USER/PASS — this is a pure derivation
                        from --net-prefix, useful for a test asserting the
                        sweep's coverage never drifts from what
                        gen-datasource.sh actually emits.
  --wapi-version=VER    Defaults to $INFOBLOX_WAPI_VERSION or v2.13.1,
                        matching test/setup.sh.

Requires INFOBLOX_HOST, INFOBLOX_USER, INFOBLOX_PASS (same as
test/setup.sh). Idempotent: running with nothing left to reap exits 0 and
prints a summary, never an error. See the header comment in this script for
the full safety rationale, deny-list, and per-object-type coverage table.
USAGE
  exit "${1:-0}"
}

TOKEN=""
NET_PREFIX=""
LITERAL_SWEEP=""
APPLY=""
PRINT_SCOPE=""
WAPI_VERSION="${INFOBLOX_WAPI_VERSION:-v2.13.1}"

for arg in "$@"; do
  case "${arg}" in
    --token=*) TOKEN="${arg#--token=}" ;;
    --net-prefix=*) NET_PREFIX="${arg#--net-prefix=}" ;;
    --literal-sweep) LITERAL_SWEEP="1" ;;
    --apply) APPLY="1" ;;
    --print-scope) PRINT_SCOPE="1" ;;
    --wapi-version=*) WAPI_VERSION="${arg#--wapi-version=}" ;;
    -h|--help) usage 0 ;;
    *)
      echo "reap.sh: unknown argument '${arg}'" >&2
      usage 1
      ;;
  esac
done

if [ -n "${LITERAL_SWEEP}" ] && { [ -n "${TOKEN}" ] || [ -n "${NET_PREFIX}" ]; }; then
  echo "ERROR: --literal-sweep is mutually exclusive with --token/--net-prefix — routine and historical cleanup are never combined into one query." >&2
  exit 1
fi

if [ -z "${LITERAL_SWEEP}" ] && [ -z "${TOKEN}" ] && [ -z "${NET_PREFIX}" ]; then
  echo "ERROR: refusing to run unscoped. Pass --token, --net-prefix, or --literal-sweep — an unscoped sweep could delete a concurrently running run's objects." >&2
  usage 1
fi

if [ -n "${TOKEN}" ] && ! [[ "${TOKEN}" =~ ^[0-9a-f]{10}$ ]]; then
  echo "ERROR: --token '${TOKEN}' is not a 10-char lowercase hex runToken (gen-datasource.sh format). Refusing — a malformed token could become an over-broad regex." >&2
  exit 1
fi

PREFIX_REGEX=""
PREFIX_V6_REGEX=""
if [ -n "${NET_PREFIX}" ]; then
  if ! [[ "${NET_PREFIX}" =~ ^100\.64\.([0-9]{1,3})\.0/24$ ]]; then
    echo "ERROR: --net-prefix '${NET_PREFIX}' is not a /24 inside 100.64.0.0/16 (the block gen-datasource.sh draws from). Refusing — a prefix outside that block could match a literal CIDR a live example depends on." >&2
    exit 1
  fi
  BLOCK_INDEX="${BASH_REMATCH[1]}"
  if [ "${BLOCK_INDEX}" -gt 255 ]; then
    echo "ERROR: --net-prefix '${NET_PREFIX}' block index ${BLOCK_INDEX} is out of range (0-255)." >&2
    exit 1
  fi
  # Escape dots for use inside a WAPI regex (`~=`) match.
  PREFIX_REGEX="^100\\.64\\.${BLOCK_INDEX}\\."
  # IPv6 counterpart — gen-datasource.sh's netV6 sub-block reuses this SAME
  # BLOCK_INDEX byte spliced into the third hextet of the RFC 3849
  # documentation prefix. No second hash draw, no new flag: --net-prefix
  # alone determines both the IPv4 and IPv6 scope for this run. Colons need
  # no escaping in a POSIX ERE.
  #
  # gen-datasource.sh writes that hextet zero-padded to two hex digits
  # (e.g. BLOCK_INDEX=14 -> "0e") into the CR spec it submits, but the Grid
  # canonicalizes the stored address per RFC 5952 before returning it: a
  # single hextet that isn't part of the longest run of all-zero groups
  # keeps its value but drops any leading zero, so "0e" is stored and
  # returned as "e" for every BLOCK_INDEX 0-15. WAPI's `~=` regex operator
  # matches against that canonicalized stored form and does not canonicalize
  # the pattern itself, so a pattern built from the padded, zero-prefixed
  # hex string can never match. BLOCK_INDEX 16-255 render as two hex digits
  # with no leading zero either way (e.g. "1a", "ff"), so padded and
  # canonical already agree there. Matching the unpadded hex string with an
  # optional leading zero covers the stored form for every BLOCK_INDEX in
  # 0-255, whether or not a caller ever submits the padded spelling.
  NET_V6_HEX="$(printf '%x' "${BLOCK_INDEX}")"
  PREFIX_V6_REGEX="^2001:db8:0?${NET_V6_HEX}:"
fi

# IPv6 counterparts of the ADDRESSED rows in NAMED_AND_ADDRESSED_TYPES
# (defined below, in the routine-mode section). Kept in a separate table
# because they are matched against PREFIX_V6_REGEX, not PREFIX_REGEX — the
# two are different derived values, both sourced from the single
# --net-prefix flag above (no separate --net-prefix-v6 flag exists or is
# needed). Declared here, ahead of routine_reap()'s use of it, so
# --print-scope (below) can also read it without depending on anything
# defined later in the script. Each line: object-type|addr-field|addr-return-field.
IPV6_ADDRESSED_TYPES="
ipv6network|network|
ipv6networkcontainer|network|
ipv6fixedaddress|ipv6addr|
"

# --print-scope is a pure derivation from --net-prefix: it prints exactly
# the object-type/field/regex triples routine_reap()'s IPv6 pass (below)
# would query, then exits before this script ever touches the network or
# requires credentials. This is deliberately placed before the
# INFOBLOX_HOST/USER/PASS check below — a caller (e.g. a Go test asserting
# gen-datasource.sh's netV6 keys are all covered) has no reason to hold
# Grid credentials just to inspect what a run's scope would be.
if [ -n "${PRINT_SCOPE}" ]; then
  otype=""
  addr_field=""
  while IFS='|' read -r otype addr_field _; do
    [ -z "${otype}" ] && continue
    echo "${otype} ${addr_field} ${PREFIX_V6_REGEX}"
  done <<<"${IPV6_ADDRESSED_TYPES}"
  exit 0
fi

if [ -z "${INFOBLOX_HOST:-}" ] || [ -z "${INFOBLOX_USER:-}" ] || [ -z "${INFOBLOX_PASS:-}" ]; then
  echo "ERROR: INFOBLOX_HOST, INFOBLOX_USER, and INFOBLOX_PASS must all be set." >&2
  exit 1
fi

WAPI_BASE="https://${INFOBLOX_HOST}/wapi/${WAPI_VERSION}"
CURL_AUTH=(-sk -u "${INFOBLOX_USER}:${INFOBLOX_PASS}")

MODE="dry-run"
[ -n "${APPLY}" ] && MODE="apply"
echo "==> reap.sh mode=${MODE} token='${TOKEN}' net-prefix='${NET_PREFIX}' literal-sweep='${LITERAL_SWEEP}'"
echo "    WAPI base: ${WAPI_BASE}"

REAPED_COUNT=0
SKIPPED_COUNT=0

# wapi_search <object-type> <curl -G --data-urlencode args...>
# Returns the raw JSON array from WAPI. _ref is always included by default —
# it must NOT be requested in _return_fields (WAPI rejects that as an
# unknown argument).
wapi_search() {
  local otype="$1"
  shift
  curl "${CURL_AUTH[@]}" -G "${WAPI_BASE}/${otype}" "$@"
}

# is_denied <object-type> <json-object> — true (0) if this object is shared
# Grid context (test/setup.sh's provisioned prerequisites, or Grid-baseline
# zones examples depend on as pre-existing context) and must never be
# reaped.
is_denied() {
  local otype="$1" obj="$2"
  case "${otype}" in
    zone_auth)
      local fqdn view
      fqdn="$(jq -r '.fqdn // empty' <<<"${obj}")"
      view="$(jq -r '.view // empty' <<<"${obj}")"
      # example.com is protected in EVERY view, not just "default": the
      # default-view zone is test/setup.sh's own prerequisite, but the
      # Internal/External-view zones are Grid-baseline shared context that
      # record-ns's examples depend on (see EXCEPTIONS in the header
      # comment) and that no script in this repo provisions or can
      # re-create. peatestinglab.com/Internal is the same kind of
      # Grid-baseline context. Deny-list scope is "shared context this repo
      # cannot afford to lose", not "only what setup.sh itself creates".
      [ "${fqdn}" = "example.com" ] && return 0
      [ "${fqdn}" = "peatestinglab.com" ] && [ "${view}" = "Internal" ] && return 0
      ;;
    network)
      local net nv
      net="$(jq -r '.network // empty' <<<"${obj}")"
      nv="$(jq -r '.network_view // empty' <<<"${obj}")"
      case "${net}" in
        "10.0.0.0/24"|"203.0.113.0/25"|"203.0.113.128/25")
          [ "${nv}" = "default" ] && return 0
          ;;
      esac
      ;;
  esac
  return 1
}

# safety_fields_for <object-type> — extra fields is_denied() needs to make
# its decision, beyond whatever field the caller was actually searching on.
# Without this, a query that only requested "fqdn" (say) would hand
# is_denied() an object with no "view" key, and the deny check would fail
# OPEN (silently treat it as not-denied) rather than correctly evaluating
# it — the wrong failure mode for a safety check. build_return_fields()
# below folds these into every query unconditionally.
safety_fields_for() {
  case "$1" in
    zone_auth) echo "view" ;;
    network) echo "network_view" ;;
    *) echo "" ;;
  esac
}

# build_return_fields <object-type> <base-fields-csv> — base fields plus
# this type's safety fields, de-duplicated, comma-joined.
build_return_fields() {
  local otype="$1" base="$2" extra all
  extra="$(safety_fields_for "${otype}")"
  all="${base}"
  if [ -n "${extra}" ]; then
    case ",${all}," in
      *",${extra},"*) : ;;
      *) all="${all:+${all},}${extra}" ;;
    esac
  fi
  echo "${all}"
}

# process_matches <object-type> <json-array> <matched-field-description>
# Applies the deny-list, then lists (dry-run) or deletes (--apply) every
# surviving match. Counts feed the final summary.
process_matches() {
  local otype="$1" json_array="$2" reason="$3"
  local n
  n="$(jq 'length' <<<"${json_array}")"
  [ "${n}" -eq 0 ] && return 0
  local i
  for i in $(seq 0 $((n - 1))); do
    local obj ref
    obj="$(jq -c ".[${i}]" <<<"${json_array}")"
    ref="$(jq -r '._ref' <<<"${obj}")"
    if is_denied "${otype}" "${obj}"; then
      echo "    SKIP  (protected, setup.sh prerequisite) ${otype} ${ref}"
      SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
      continue
    fi
    if [ -n "${APPLY}" ]; then
      echo "    DELETE ${otype} ${ref}  [matched: ${reason}]"
      curl "${CURL_AUTH[@]}" -X DELETE "${WAPI_BASE}/${ref}" >/dev/null
    else
      echo "    WOULD DELETE ${otype} ${ref}  [matched: ${reason}]"
    fi
    REAPED_COUNT=$((REAPED_COUNT + 1))
  done
}

# ----------------------------------------------------------------------------
# Routine mode: token-scoped and/or prefix-scoped reap.
# ----------------------------------------------------------------------------
# Each line: object-type|token-field|addr-field|addr-return-field
# token-field: name of the field searched with a regex token match, and also
#   returned for the log line. Empty if this type has no name-carrying
#   identity component.
# addr-field: name of the field SEARCHED with a regex prefix match. This is
#   sometimes a WAPI search-only virtual field (e.g. record:host's
#   "ipv4addr", which flattens the nested ipv4addrs array for querying) that
#   WAPI refuses to also return in _return_fields ("Field is not readable").
# addr-return-field: what to put in _return_fields instead, for the log
#   line. Defaults to addr-field itself when omitted (the common case, where
#   the search field and the readable field are the same name).
NAMED_AND_ADDRESSED_TYPES="
record:a|name||
record:aaaa|name||
record:alias|name||
record:cname|name||
record:mx|name||
record:srv|name||
record:txt|name||
record:ns|nameserver||
record:host|name|ipv4addr|name,ipv4addrs
record:ptr||ipv4addr|
zone_auth|fqdn||
zone_delegated|fqdn||
zone_forward|fqdn||
view|name||
networkview|name||
dtc:pool|name||
dtc:server|name||
dtc:lbdn|name||
rangetemplate|name||
extensibleattributedef|name||
sharednetwork|name||
network||network|
networkcontainer||network|
range||start_addr|
fixedaddress||ipv4addr|
"

# IPV6_ADDRESSED_TYPES (the IPv6 counterparts of the ADDRESSED rows above)
# is declared earlier in this script, alongside PREFIX_V6_REGEX, so
# --print-scope can read it without depending on anything defined below —
# see the comment there for why it is a separate table from
# NAMED_AND_ADDRESSED_TYPES.

routine_reap() {
  local otype token_field addr_field addr_return_field
  while IFS='|' read -r otype token_field addr_field addr_return_field; do
    [ -z "${otype}" ] && continue
    if [ -n "${TOKEN}" ] && [ -n "${token_field}" ]; then
      local resp
      resp="$(wapi_search "${otype}" --data-urlencode "${token_field}~=${TOKEN}" --data-urlencode "_return_fields=$(build_return_fields "${otype}" "${token_field}")")"
      process_matches "${otype}" "${resp}" "${token_field}~=${TOKEN}"
    fi
    if [ -n "${NET_PREFIX}" ] && [ -n "${addr_field}" ]; then
      local resp2
      resp2="$(wapi_search "${otype}" --data-urlencode "${addr_field}~=${PREFIX_REGEX}" --data-urlencode "_return_fields=$(build_return_fields "${otype}" "${addr_return_field:-${addr_field}}")")"
      process_matches "${otype}" "${resp2}" "${addr_field}~=${PREFIX_REGEX}"
    fi
  done <<<"${NAMED_AND_ADDRESSED_TYPES}"

  if [ -n "${NET_PREFIX}" ]; then
    local v6_otype v6_addr_field v6_addr_return_field
    while IFS='|' read -r v6_otype v6_addr_field v6_addr_return_field; do
      [ -z "${v6_otype}" ] && continue
      local resp3
      resp3="$(wapi_search "${v6_otype}" --data-urlencode "${v6_addr_field}~=${PREFIX_V6_REGEX}" --data-urlencode "_return_fields=$(build_return_fields "${v6_otype}" "${v6_addr_return_field:-${v6_addr_field}}")")"
      process_matches "${v6_otype}" "${resp3}" "${v6_addr_field}~=${PREFIX_V6_REGEX}"
    done <<<"${IPV6_ADDRESSED_TYPES}"
  fi
}

# ----------------------------------------------------------------------------
# Liveness guard for --literal-sweep (see "THE LIVENESS GUARD" in the header
# comment). Maps each WAPI object type to the examples/ family directory
# that owns it, so a literal value can be checked against ONLY that family's
# own manifests — never the whole examples/ tree, which would false-positive
# on the same literal string appearing as an unrelated field's VALUE in a
# different family's example (record-a's retired name vs. record-cname's
# live `canonical` target, for instance).
# ----------------------------------------------------------------------------
OTYPE_EXAMPLE_DIR="
record:a|record-a
record:aaaa|record-aaaa
record:alias|record-alias
record:cname|record-cname
record:mx|record-mx
record:srv|record-srv
record:txt|record-txt
record:ns|record-ns
record:host|host-record
record:ptr|record-ptr
zone_auth|zone-auth
zone_delegated|zone-delegated
zone_forward|zone-forward
view|dns-view
networkview|network-view
dtc:pool|dtc-pool
dtc:server|dtc-server
dtc:lbdn|dtc-lbdn
rangetemplate|range-template
extensibleattributedef|extensible-attribute-def
sharednetwork|ipv4-shared-network
network|network
networkcontainer|network-container
range|range
fixedaddress|fixed-address
"

# example_dir_for <object-type> — the examples/ subdirectory that owns this
# WAPI object type, or empty if unknown.
example_dir_for() {
  local otype="$1" ot dir
  while IFS='|' read -r ot dir; do
    [ "${ot}" = "${otype}" ] && { echo "${dir}"; return 0; }
  done <<<"${OTYPE_EXAMPLE_DIR}"
  echo ""
}

# get_identity_fields <object-type> — the field name(s) that carry this
# type's RUN IDENTITY (the same token_field / addr_field columns
# routine_reap matches on), space-separated. A --literal-sweep entry's OTHER
# fields (e.g. record:ns's "name" / "view", which are shared Grid context —
# see EXCEPTIONS in the header comment) are deliberately excluded from the
# liveness check: they are expected to match every run's example on
# purpose, and checking them would make the entry look permanently "live".
get_identity_fields() {
  local otype="$1" ot tf af _arf out=""
  while IFS='|' read -r ot tf af _arf; do
    [ "${ot}" = "${otype}" ] || continue
    [ -n "${tf}" ] && out="${tf}"
    [ -n "${af}" ] && out="${out:+${out} }${af}"
    break
  done <<<"${NAMED_AND_ADDRESSED_TYPES}"
  echo "${out}"
}

# is_literal_live <object-type> <value> — true (0) if this literal identity
# value still appears, as a real field value (comments stripped), somewhere
# in that object type's OWN examples/ family directory.
#
# Two precision requirements, both load-bearing:
#   1. Scoped to ONE family's own directory (not the whole examples/ tree):
#      the same literal string can legitimately appear as a reference VALUE
#      in an unrelated family's example (record-a's retired identity
#      "www.example.com" is still record-cname's live `canonical` target
#      and record-ptr's live `ptrdname` target — neither is record-a's own
#      identity). A whole-tree grep would treat those as false positives
#      and refuse to ever reap record-a's pre-tokenisation orphans.
#   2. End-anchored on the field value, not a bare substring: a family that
#      has already been tokenised writes its identity as
#      "<literal>-${data.runToken}", which contains the OLD literal as a
#      prefix (e.g. dtc-lbdn's current "test-dtc-lbdn-${data.runToken}"
#      contains the pre-tokenisation literal "test-dtc-lbdn"). A plain
#      substring match would flag that family as still-live forever and
#      the literal-sweep entry could never reap its genuine pre-rollout
#      orphans. Anchoring the match to the end of the field's value (the
#      literal must be the WHOLE value, not merely a prefix of it)
#      distinguishes "still creates the bare literal" from "now creates a
#      token-suffixed value that happens to start with it".
#
# Fails safe: an object type with no known examples/ mapping, a family
# directory that does not exist, or a managed-resource file (one with a
# top-level "spec:" key) whose forProvider block this script fails to
# extract, is treated as LIVE (return 0) rather than silently skipping
# verification — a --literal-sweep entry this script cannot verify must
# never be reaped unattended.
is_literal_live() {
  local otype="$1" value="$2" dir examples_dir f unparseable=""
  dir="$(example_dir_for "${otype}")"
  if [ -z "${dir}" ]; then
    return 0
  fi
  examples_dir="${REPO_ROOT}/examples/${dir}"
  if [ ! -d "${examples_dir}" ]; then
    return 0
  fi
  # Escape regex metacharacters in the literal value (fqdns and CIDRs
  # contain "." and "/") before building the anchored pattern.
  local escaped pattern
  # shellcheck disable=SC2016 # sed program is a literal pattern, not meant to expand
  escaped="$(printf '%s' "${value}" | sed -e 's/[.[\*^$()+?{}|\/]/\\&/g')"
  # Match the value as a whole YAML scalar: preceded by start-of-line,
  # whitespace, or a quote; followed only by optional whitespace/quotes and
  # end-of-line. This rejects "<value>-${data.runToken}" (extra characters
  # follow) while still matching "<value>", '"<value>"', and trailing
  # whitespace variants.
  pattern="(^|[[:space:]\"'])${escaped}([[:space:]\"']*)\$"
  for f in "${examples_dir}"/*.yaml; do
    [ -f "${f}" ] || continue
    # Restrict the search to the spec.forProvider block — the only place a
    # WAPI identity field can legitimately live. Every example in this repo
    # follows the same shape: a top-level "spec:" key, a "  forProvider:"
    # child at 2-space indent, and the provider fields nested under it at
    # 4+ spaces, terminated by the next 2-space sibling key (providerConfigRef,
    # writeConnectionSecretToRef, ...). Searching the whole file instead
    # would also catch metadata.name, which is a Kubernetes object name —
    # unrelated to the Grid identity — and two families (range-template,
    # extensible-attribute-def) reuse the pre-tokenisation literal there on
    # purpose (metadata.name: example-range-template / example-ea-def),
    # producing a false "still live" match that can never be reaped.
    #
    # A file with no top-level "spec:" key at all (a Secret, a Namespace, or
    # any other non-managed-resource YAML a family directory might ship
    # alongside its MR example) is not expected to have a forProvider block
    # in the first place — skip it without comment, exactly like the
    # not-a-file check above. But a file that DOES have "spec:" and still
    # produces an empty forProvider block failed to parse (different
    # indentation, an unexpected shape this script doesn't understand) —
    # that is a verification failure, not evidence of absence, and must
    # fail safe (treat the literal as LIVE) rather than silently being
    # skipped, which would let the surrounding loop's final `return 1`
    # (not live) fire on a file this script never actually checked.
    grep -Eq '^spec:' "${f}" || continue
    local for_provider_block
    for_provider_block="$(awk '
      /^  forProvider:/ { in_block=1; next }
      in_block && /^  [^ ]/ { in_block=0 }
      in_block { print }
    ' "${f}")"
    if [ -z "${for_provider_block}" ]; then
      unparseable="1"
      continue
    fi
    if printf '%s\n' "${for_provider_block}" | grep -v '^[[:space:]]*#' | grep -Eq -- "${pattern}"; then
      return 0
    fi
  done
  if [ -n "${unparseable}" ]; then
    return 0
  fi
  return 1
}

# ----------------------------------------------------------------------------
# Literal-sweep mode: exact-match pre-tokenisation identities.
# ----------------------------------------------------------------------------
# Each line: object-type|field=value[,field=value...]
# Candidate set cross-checked directly against examples/ as it existed in
# this repo when this table was written — see the header comment above for
# corrections to any earlier draft, and THE LIVENESS GUARD for why this
# table alone is never trusted to decide what gets deleted.
LITERAL_SWEEP_TABLE="
record:a|name=www.example.com
record:a|name=www-ns.example.com
record:aaaa|name=www6.example.com
record:aaaa|name=www6-ns.example.com
record:alias|name=alias-test.example.com
record:alias|name=alias-test-ns.example.com
record:cname|name=alias.example.com
record:cname|name=alias-ns.example.com
record:mx|name=test-mx.example.com
record:mx|name=test-mx-ns.example.com
record:srv|name=_sip._tcp.example.com
record:srv|name=_sip._tcp.ns.example.com
record:txt|name=txt.example.com
record:txt|name=txt-ns.example.com
record:ns|name=peatestinglab.com,nameserver=ns1.example.com,view=Internal
record:ns|name=example.com,nameserver=ns1.example.com,view=Internal
record:host|name=test-host.example.com
record:host|name=test-host-ns.example.com
record:ptr|ipv4addr=10.1.1.10
record:ptr|ipv4addr=10.1.1.20
zone_auth|fqdn=zoneauth-example.com
zone_auth|fqdn=example-ns.com
zone_delegated|fqdn=delegated.example.com
zone_delegated|fqdn=delegated-ns.example.com
zone_forward|fqdn=forward.example.com
zone_forward|fqdn=forward-ns.example.com
view|name=crossplane-test-view
view|name=crossplane-test-view-ns
networkview|name=test-network-view
networkview|name=test-network-view-ns
dtc:pool|name=test-dtc-pool
dtc:pool|name=test-dtc-pool-ns
dtc:server|name=example-server.example.com
dtc:server|name=example-server-ns.example.com
dtc:lbdn|name=test-dtc-lbdn
dtc:lbdn|name=test-dtc-lbdn-ns
rangetemplate|name=example-range-template
rangetemplate|name=example-range-template-ns
extensibleattributedef|name=example-ea-def
extensibleattributedef|name=example-ea-def-ns
sharednetwork|name=example-shared-network
sharednetwork|name=example-shared-network-ns
network|network=198.51.100.0/24
network|network=198.51.101.0/24
networkcontainer|network=172.25.0.0/16
networkcontainer|network=172.26.0.0/16
range|start_addr=203.0.113.100,end_addr=203.0.113.120
range|start_addr=203.0.113.160,end_addr=203.0.113.180
fixedaddress|ipv4addr=10.0.0.50
fixedaddress|ipv4addr=10.0.0.51
"

literal_sweep() {
  local otype pairs
  while IFS='|' read -r otype pairs; do
    [ -z "${otype}" ] && continue
    local -a curlargs=()
    local reason="" fields=""
    local -A field_values=()
    local IFS_SAVE="${IFS}"
    IFS=','
    local pair
    for pair in ${pairs}; do
      local field="${pair%%=*}" value="${pair#*=}"
      curlargs+=(--data-urlencode "${field}=${value}")
      fields="${fields}${fields:+,}${field}"
      reason="${reason}${reason:+,}${field}=${value}"
      field_values["${field}"]="${value}"
    done
    IFS="${IFS_SAVE}"

    # THE LIVENESS GUARD: verify this entry's RUN-IDENTITY field(s) — not
    # every field in the entry — are not still shipped live in examples/
    # before ever issuing the WAPI query. This is what makes the table safe
    # to ship even while some rollout waves have not merged yet: a family
    # whose wave hasn't landed still creates this literal for real, and
    # this check catches that and skips instead of reaping a live object.
    local id_fields id_field live_hit=""
    id_fields="$(get_identity_fields "${otype}")"
    for id_field in ${id_fields}; do
      local id_value="${field_values[${id_field}]:-}"
      [ -z "${id_value}" ] && continue
      if is_literal_live "${otype}" "${id_value}"; then
        live_hit="${id_field}=${id_value}"
        break
      fi
    done

    if [ -n "${live_hit}" ]; then
      echo "    SKIP  (still LIVE in examples/ — ${live_hit}) ${otype} [literal:${reason}]"
      SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
      continue
    fi

    # A single comma-joined _return_fields=... — WAPI rejects the additive
    # `_return_fields+=` form when the value is URL-encoded (the encoded
    # `+` is parsed as part of the option name, not "add to defaults"), and
    # a plain `_return_fields=` already returns exactly the fields asked
    # for plus `_ref` (always included), which is all process_matches needs.
    # build_return_fields also folds in this type's is_denied() safety
    # fields (e.g. network_view for `network`), even though every literal
    # entry below already includes any field the deny-list cares about —
    # belt and suspenders against a future entry that doesn't.
    curlargs+=(--data-urlencode "_return_fields=$(build_return_fields "${otype}" "${fields}")")
    local resp
    resp="$(wapi_search "${otype}" "${curlargs[@]}")"
    process_matches "${otype}" "${resp}" "literal:${reason}"
  done <<<"${LITERAL_SWEEP_TABLE}"
}

if [ -n "${LITERAL_SWEEP}" ]; then
  literal_sweep
else
  routine_reap
fi

echo "==> reap.sh done: ${REAPED_COUNT} $([ -n "${APPLY}" ] && echo deleted || echo "would delete"), ${SKIPPED_COUNT} protected/skipped."
if [ "${REAPED_COUNT}" -eq 0 ]; then
  echo "    Nothing to reap."
fi
exit 0
