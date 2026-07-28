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
