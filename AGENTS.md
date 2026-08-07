# Provider Infoblox NIOS

`provider-infobloxnios` is a native Crossplane v2 provider for managing
Infoblox NIOS DDI (DNS, DHCP, IPAM) resources.

## Provider Identity

| Property | Value |
|----------|-------|
| Name | `provider-infobloxnios` |
| Module | `github.com/crossplane-contrib/provider-infoblox-nios` |
| API Group (cluster) | `<resource>.infobloxnios.crossplane.io` |
| API Group (namespaced) | `<resource>.infobloxnios.m.crossplane.io` |

## Scope

Pilot only — A-Record (to be selected during Phase 1 investigation).

## Repository Layout

```
provider-infobloxnios/
├── AGENTS.md                              ← this file
├── Makefile                               ← build, test, lint, e2e
├── apis/
│   ├── v1alpha1/                          ← ProviderConfig types
│   ├── cluster/<resource>/v1alpha1/       ← cluster-scoped MR types
│   ├── namespaced/<resource>/v1alpha1/    ← namespaced MR types
│   └── generate.go                        ← CRD + deepcopy generation
├── cmd/provider/main.go                   ← provider entry point
├── internal/
│   ├── clients/                           ← WAPI client (wraps infoblox-go-client)
│   └── controller/
│       ├── config/                        ← ProviderConfig reconciler
│       └── <resource>/                    ← MR controllers
├── tools/openapi/                         ← API investigation tool + inventory
├── package/
│   ├── crossplane.yaml                    ← provider manifest
│   └── crds/                              ← generated CRD YAMLs
├── examples/                              ← example manifests
├── test/                                  ← E2E test infrastructure
└── build/                                 ← crossplane build submodule
```

## Development

### Prerequisites

- Go 1.24+
- Docker (for image builds and E2E)
- make

### Build

```bash
make build         # compile provider binary
make generate      # regenerate CRDs and deepcopy
make lint          # run linters
make reviewable    # full build + lint + test gate
```

### Local Development

```bash
make dev           # create kind cluster, install CRDs, run controller
make dev-clean     # teardown kind cluster
```

### Testing

```bash
go test ./...      # unit tests
make e2e           # E2E tests against live NIOS appliance
```

### E2E Credentials

The E2E tests require a NIOS appliance. Set the following environment variables:

```bash
export INFOBLOX_HOST=<nios-host-ip>
export INFOBLOX_USER=<username>
export INFOBLOX_PASS=<password>
```

### Authoring `crossplane.io/update-test` annotations: the enable-flag / companion-list trap

When adding a `crossplane.io/update-test` annotation to an example manifest,
watch for boolean "enable"-style fields (e.g. `enableBlacklist`,
`dns64Enabled` — as opposed to the paired `use<X>` override flags, which are
always safe to flip alone) that turn on a feature backed by a list/struct
field. Some of these enable flags carry a hidden backend business rule
requiring the companion list to already be non-empty — setting the flag true
against a minimal example (whose companion list is empty and therefore
skipped elsewhere in the same annotation) gets rejected by the API, and
because per-field tests apply as cumulative patches, that single rejection
can leave the resource in a state where every later field in the same test
run also fails to reconcile. This does not show up as a fast, readable
error — it wedges the whole per-field run for anyone who happens to trigger
that resource's E2E next.

The doc comment on the Go struct field is NOT a reliable signal either way:
fields with a real hidden requirement and fields without one can both read
"Determines if X is enabled or not" with no mention of the dependency. When a
resource has this enable-flag/companion-list shape, reason from the target
API's own docs first. If the field is still ambiguous, run a quick, isolated
probe against a scratch object (create → PUT the candidate field → observe
the response → delete) rather than relying on a full E2E cycle to surface the
answer — and always clean the scratch object up afterward. If the probe (or
the API docs) confirms the enable flag requires non-empty companion data that
the example doesn't provide, skip the enable flag with a reason naming the
companion field, and keep the paired `use` override flag value-tested
(flipping the override alone is always a valid, harmless state). See
`examples/dns-view/dns-view.yaml` for a worked example of this pattern.

### Quick Start

1. Install the provider:
```yaml
apiVersion: pkg.crossplane.io/v1
kind: Provider
metadata:
  name: provider-infobloxnios
spec:
  package: xpkg.upbound.io/crossplane-contrib/provider-infobloxnios:v0.1.0
```

2. Create a Secret with NIOS credentials:
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: infobloxnios-api-key
  namespace: crossplane-system
type: Opaque
stringData:
  host: "<NIOS_HOST>"
  username: "<username>"
  password: "<password>"
```

3. Create a ProviderConfig:
```yaml
apiVersion: infobloxnios.crossplane.io/v1alpha1
kind: ProviderConfig
metadata:
  name: default
spec:
  credentials:
    source: Secret
    secretRef:
      namespace: crossplane-system
      name: infobloxnios-api-key
      key: credentials
```

## Credential extraction

The Grid Manager host (`spec.host` on ProviderConfig/ClusterProviderConfig, and
`spec.readEndpoint.host` for the optional read endpoint) is a non-secret
connection parameter, not a Secret key. The credentials Secret carries only
`username`/`password`. `internal/clients/dualclient.ExtractCredentials` is the
single shared helper every controller package's `Connect()` calls to combine
the two into a `dualclient.Credentials` value — it takes the host as an
explicit parameter and returns an error if it is empty, so a `Credentials`
value is always either fully populated or absent; no call site can construct
one with a blank host.

When a controller package still carries its own private `nioCredentials`
type and `extractCredentials` function instead of calling
`dualclient.ExtractCredentials`, migrate it with this recipe (each edit is
in-place, mechanical):

**1. `internal/controller/<pkg>/controller.go`**
- Delete `type nioCredentials struct { Host, Username, Password string }`.
- Delete `func extractCredentials(...)`.
- Delete the error consts that become unused: `errGetSecret`, `errNoSecretRef`,
  `errUnsupportedCreds`, `errMissingCredKey`. Unused consts do not fail `go
  build` in Go — grep each name in the package after deleting the function and
  remove the ones with no remaining reference.
- `func newObjectManager(creds *nioCredentials, sslVerify bool)` →
  `func newObjectManager(creds dualclient.Credentials, sslVerify bool)`. Same
  for `newObjectManagerWithScheme`. Value, not pointer — there is no nil case
  now.
- Fix any doc comment claiming the endpoint is "validated non-empty by
  extractCredentials" — it is now resolved from `pc.Spec.Host`, which the CRD
  requires with `MinLength=1`.

**2. `cluster.go` — `Connect()`**
```go
creds, err := dualclient.ExtractCredentials(ctx, c.kube, pc.Spec.Host,
    pc.Spec.Credentials.Source, pc.Spec.Credentials.SecretRef, "")
```
`endpoint: creds.Host` at the call site is unchanged and still compiles.

**3. `namespaced.go` — `Connect()`**
Two arms of the existing Kind switch, both edited in place:
- `case "ProviderConfig"`: `pc.Spec.Host`, fallback namespace `pc.GetNamespace()`.
- `case "ClusterProviderConfig"`: `cpc.Spec.Host`, fallback namespace `""`.

Change `var creds *nioCredentials` to `var creds dualclient.Credentials` and
drop the `&` / pointer handling. Do not restructure the switch, the usage
trackers, or the `errUnsupportedKind` default — the dual-scope shape here is
unrelated to this migration.

**4. `*_test.go`**
- Secret fixtures: drop the `host` key from `Data`/`StringData`.
- Every ProviderConfig / ClusterProviderConfig fixture: set `Spec.Host`.
- `&nioCredentials{…}` literals → `dualclient.Credentials{…}` (value).
- A test asserting `errMissingCredKey` when the Secret's `host` key is absent
  must be deleted, not rewritten. A missing host is now rejected by CRD schema
  validation, so there is no runtime path left to assert. Keep the
  missing-username / missing-password cases.
- A test documenting that the `ssl_verify` Secret key is ignored should be
  retargeted at `dualclient.ExtractCredentials` — it is still the right
  assertion, just in a new home.

## Contributing

### Code Conventions

- **Go**: `gofmt` + `goimports`, imports ordered stdlib → external → internal
- **Tests**: PascalCase names, no underscores. Unit tests use `net/http/httptest` mocks.
- **Commits**: `feat(<scope>): <description>` — conventional commits
- **CRD categories**: `{crossplane, managed, infobloxnios}`

### API Design

Every managed resource (MR) exists in both scopes:
- **Cluster-scoped**: `<resource>.infobloxnios.crossplane.io`
- **Namespaced**: `<resource>.infobloxnios.m.crossplane.io`

External names use the NIOS `_ref` (server-assigned opaque reference).
