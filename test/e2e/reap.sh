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
#   network                    network                   network (prefix)
#   network-container             networkcontainer          network (prefix)
#   range                      range                     start_addr (prefix)
#   fixed-address                fixedaddress              ipv4addr (prefix)
#
# ============================================================================
# EXCEPTIONS — resources whose objects cannot be cleanly token-scoped
# ============================================================================
# record-ns: the "name" field is a PRE-EXISTING zone the record lives under
#   (shared context — e.g. peatestinglab.com / example.com in the current
#   examples), not this run's identity, and must never be matched or deleted
#   by this script. Only "nameserver" is a candidate for tokenisation, and
#   that decision has not shipped yet (open A/B question, tracked separately
#   from this ticket). This script wires nameserver-token matching now so no
#   further reaper change is needed once it lands; until then the query
#   simply returns nothing, because no run writes a token into nameserver
#   yet. record:ns objects predating any such change are handled by
#   --literal-sweep instead (see below), which matches the full (name,
#   nameserver) pair — it never deletes by name alone.
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
# ============================================================================
# THE ONE-SHOT PRE-TOKENISATION SWEEP
# ============================================================================
# Before this mechanism existed, every run used the SAME literal identities
# (www.example.com, test-dtc-pool, ...). After the rollout, no run creates
# those literal identities any more — a Grid object still carrying one is
# definitionally an orphan, of unknown age, from before tokenisation. That
# makes a one-shot sweep possible, and it is a genuinely different operation
# from the routine reap above:
#   - it matches fixed literal identities, not a caller-supplied token/prefix
#   - it is opt-in (--literal-sweep) and mutually exclusive with --token /
#     --net-prefix — routine and historical cleanup are never combined into
#     one broader query
#   - it is meant to run once, by a human or a one-off ant task, never as
#     part of the automatic per-run cleanup a live E2E run performs
# The literal table (LITERAL_SWEEP below) is the ground truth for what
# examples/ currently ships pre-tokenisation, cross-checked directly against
# every example manifest in this repo (not copied from an earlier draft of
# this ticket — several of that draft's names do not match the shipped
# examples, e.g. "example-ns.com" is zone-auth's namespaced fqdn, not
# record-ns's identity, and record-alias's literal is "alias-test.example.com"
# not "alias.example.com"). record-a's literals (www.example.com,
# www-ns.example.com) are included for the historical record even though
# IN-E2E-ISO-MECH already tokenised that family — any Grid object still
# carrying them predates that change.
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
#                        identities examples/ used before tokenisation.
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

usage() {
  cat <<'USAGE'
Usage:
  test/e2e/reap.sh --token=<runToken> [--net-prefix=<CIDR>] [--apply]
  test/e2e/reap.sh --net-prefix=<CIDR> [--apply]
  test/e2e/reap.sh --literal-sweep [--apply]

  --token=TOKEN        10-char lowercase hex runToken (gen-datasource.sh
                        format). Reaps NAMED objects whose identity field
                        contains this token.
  --net-prefix=CIDR     A /24 inside 100.64.0.0/16 (gen-datasource.sh
                        format). Reaps ADDRESSED objects whose address
                        falls inside this prefix.
  --literal-sweep       One-shot historical mode. Mutually exclusive with
                        --token/--net-prefix. Matches the fixed literal
                        identities examples/ used before tokenisation.
  --apply               Actually delete. Without it, every match is only
                        listed (dry-run is the default).
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
WAPI_VERSION="${INFOBLOX_WAPI_VERSION:-v2.13.1}"

for arg in "$@"; do
  case "${arg}" in
    --token=*) TOKEN="${arg#--token=}" ;;
    --net-prefix=*) NET_PREFIX="${arg#--net-prefix=}" ;;
    --literal-sweep) LITERAL_SWEEP="1" ;;
    --apply) APPLY="1" ;;
    --wapi-version=*) WAPI_VERSION="${arg#--wapi-version=}" ;;
    -h|--help) usage 0 ;;
    *)
      echo "reap.sh: unknown argument '${arg}'" >&2
      usage 1
      ;;
  esac
done

if [ -z "${INFOBLOX_HOST:-}" ] || [ -z "${INFOBLOX_USER:-}" ] || [ -z "${INFOBLOX_PASS:-}" ]; then
  echo "ERROR: INFOBLOX_HOST, INFOBLOX_USER, and INFOBLOX_PASS must all be set." >&2
  exit 1
fi

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

# is_denied <object-type> <json-object> — true (0) if this object is one of
# test/setup.sh's shared prerequisites and must never be reaped.
is_denied() {
  local otype="$1" obj="$2"
  case "${otype}" in
    zone_auth)
      local fqdn view
      fqdn="$(jq -r '.fqdn // empty' <<<"${obj}")"
      view="$(jq -r '.view // empty' <<<"${obj}")"
      [ "${fqdn}" = "example.com" ] && [ "${view}" = "default" ] && return 0
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
}

# ----------------------------------------------------------------------------
# Literal-sweep mode: exact-match pre-tokenisation identities.
# ----------------------------------------------------------------------------
# Each line: object-type|field=value[,field=value...]
# Ground truth cross-checked directly against examples/ as it exists in this
# repo — see the header comment above for corrections to any earlier draft.
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
    local IFS_SAVE="${IFS}"
    IFS=','
    local pair
    for pair in ${pairs}; do
      local field="${pair%%=*}" value="${pair#*=}"
      curlargs+=(--data-urlencode "${field}=${value}")
      fields="${fields}${fields:+,}${field}"
      reason="${reason}${reason:+,}${field}=${value}"
    done
    IFS="${IFS_SAVE}"
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
