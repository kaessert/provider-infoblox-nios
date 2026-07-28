# provider-infobloxnios

`provider-infobloxnios` is a [Crossplane](https://crossplane.io/) Provider for
[Infoblox NIOS](https://www.infoblox.com/products/nios/), built on the
Crossplane v2 runtime. It manages Infoblox NIOS DDI (DNS, DHCP, IPAM)
resources declaratively using Kubernetes custom resources.

## Features

- **ARecord** — create and manage Infoblox NIOS DNS "A" records (cluster-scoped
  and namespace-scoped)
- Dual-scope managed resources: cluster-scoped (`infobloxnios.crossplane.io`)
  and namespace-scoped (`infobloxnios.m.crossplane.io`)
- Standard Crossplane management policies, usage tracking, and connection
  detail publishing

## Prerequisites

- Kubernetes cluster with [Crossplane v2](https://docs.crossplane.io/) installed
- An Infoblox NIOS Grid Manager appliance reachable from the cluster, with a
  WAPI-capable user account (host, username, password)

## Installation

### Using kubectl (recommended)

Install Crossplane if not already installed:

```bash
helm repo add crossplane-stable https://charts.crossplane.io/stable
helm repo update
helm install crossplane crossplane-stable/crossplane \
  --namespace crossplane-system \
  --create-namespace
```

Install the provider:

```yaml
apiVersion: pkg.crossplane.io/v1
kind: Provider
metadata:
  name: provider-infobloxnios
spec:
  package: ghcr.io/kaessert/provider-infobloxnios:latest
```

### From Source

```bash
git clone https://github.com/kaessert/provider-infoblox-nios
cd provider-infoblox-nios
make build docker-build
```

## Configuration

### 1. Create a Kubernetes Secret with your NIOS Grid Manager credentials

The provider reads three keys directly from the Secret's data: `host`,
`username`, and `password`.

```bash
kubectl create secret generic infobloxnios-credentials \
  --namespace crossplane-system \
  --from-literal=host=<nios-host-or-ip> \
  --from-literal=username=<wapi-username> \
  --from-literal=password=<wapi-password>
```

### 2. Create a ProviderConfig

Apply the example provider config (Secret + cluster ProviderConfig +
namespaced ProviderConfig + ClusterProviderConfig):

```bash
kubectl apply -f examples/provider/config.yaml
```

Or create one manually. Cluster-scoped managed resources use the
cluster-scoped `ProviderConfig`:

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
      name: infobloxnios-credentials
      key: password # required by the schema; unused by the credential bridge
```

Namespaced managed resources use either a namespace-scoped `ProviderConfig`
or a cluster-scoped `ClusterProviderConfig`, both under the
`infobloxnios.m.crossplane.io` API group:

```yaml
apiVersion: infobloxnios.m.crossplane.io/v1alpha1
kind: ClusterProviderConfig
metadata:
  name: default
spec:
  credentials:
    source: Secret
    secretRef:
      namespace: crossplane-system
      name: infobloxnios-credentials
      key: password
```

## Resources

### ARecord

Manage Infoblox NIOS DNS "A" records (WAPI object type `record:a`).

**Cluster-scoped** (`recorda.infobloxnios.crossplane.io/v1alpha1`):

```yaml
apiVersion: recorda.infobloxnios.crossplane.io/v1alpha1
kind: ARecord
metadata:
  name: example-arecord
spec:
  forProvider:
    name: www.example.com
    ipv4Addr: 192.0.2.10
    view: default
    comment: Managed by Crossplane
  providerConfigRef:
    name: default
```

**Namespace-scoped** (`recorda.infobloxnios.m.crossplane.io/v1alpha1`):

```yaml
apiVersion: recorda.infobloxnios.m.crossplane.io/v1alpha1
kind: ARecord
metadata:
  name: example-arecord-ns
  namespace: default
spec:
  forProvider:
    name: www-ns.example.com
    ipv4Addr: 192.0.2.20
    view: default
  providerConfigRef:
    kind: ClusterProviderConfig
    name: default
```

External name: WAPI assigns an opaque `_ref` reference to every object
(e.g. `record:a/ZG5zLmJpbmRfYQ:www.example.com/default`). Crossplane stores
this in the `crossplane.io/external-name` annotation — do not set it
manually.

The `view` field is immutable after creation: WAPI ties an A record's `_ref`
to `(view, zone, name)`, and the underlying SDK's update call has no `view`
parameter.

Apply the full set of example manifests:

```bash
kubectl apply -f examples/record-a/record-a.yaml
kubectl apply -f examples/record-a/record-a-namespaced.yaml
```

## Development

```bash
make build         # compile provider binary
make generate      # regenerate CRDs and deepcopy
make lint          # run linters
make reviewable    # full build + lint + test gate
go test ./...      # unit tests
```

## Contributing

Refer to Crossplane's [CONTRIBUTING.md] file for more information on how the
Crossplane community prefers to work. The [Provider Development][provider-dev]
guide may also be of use.

[CONTRIBUTING.md]: https://github.com/crossplane/crossplane/blob/master/CONTRIBUTING.md
[provider-dev]: https://github.com/crossplane/crossplane/blob/master/contributing/guide-provider-development.md
