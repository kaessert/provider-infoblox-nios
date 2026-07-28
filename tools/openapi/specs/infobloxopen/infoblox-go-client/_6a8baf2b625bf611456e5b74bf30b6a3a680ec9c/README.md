# Pinned SDK Source — infoblox-go-client/v2

Infoblox NIOS does not publish a formal OpenAPI/Swagger specification for its
WAPI. This directory pins the three Go source files that define the resource
schema and CRUD surface used by the investigation and (later) generator
tooling, taken from the official Go SDK:

- Repository: `github.com/infobloxopen/infoblox-go-client/v2`
- Commit: `6a8baf2b625bf611456e5b74bf30b6a3a680ec9c`
- Commit date: 2026-06-09

Files pinned (see `checksums.txt` for SHA-256 sums):

- `object_manager.go` — the `IBObjectManager` interface: the typed
  Create/Get/Update/Delete method surface the provider wraps. This is the
  authoritative list of which WAPI object types are reachable through the
  SDK's high-level API, and which fields each Create/Update call accepts.
- `objects.go` — hand-written struct definitions for a handful of object
  types (`Network`, `NetworkContainer`, `FixedAddress`, `EA`, etc.) with
  runtime-selected object types (IPv4 vs IPv6 variants).
- `objects_generated.go` — the bulk of the WAPI object model, generated from
  the NIOS WAPI schema documentation. Struct field doc comments are the
  source for field descriptions, enum values ("Valid values are ..."), and
  read-only/response-only fields.

## Why source files instead of an OpenAPI document

There is no machine-readable specification to fetch and pin as JSON/YAML.
Pinning the exact SDK source (rather than depending on a floating `go.mod`
version) keeps the investigation reproducible: field names, types, and the
Create/Update parameter surface are fixed at this commit, and any future SDK
update is a deliberate re-pin (re-copy files + update checksums + note the
diff) — the same discipline applied to any pinned API specification, checked
into version control with a verifiable checksum rather than resolved at
build time.

## Re-pinning

```bash
git clone https://github.com/infobloxopen/infoblox-go-client /tmp/infoblox-go-client
cd /tmp/infoblox-go-client && git checkout <new-commit>
cp objects.go objects_generated.go object_manager.go \
  <repo>/tools/openapi/specs/infobloxopen/infoblox-go-client/<new-commit>/
cd <repo>/tools/openapi/specs/infobloxopen/infoblox-go-client/<new-commit>
sha256sum objects.go objects_generated.go object_manager.go > checksums.txt
```
