# provider-infobloxnios

`provider-infobloxnios` is a [Crossplane](https://crossplane.io/) Provider for
[Infoblox NIOS](https://www.infoblox.com/products/nios/), built on the
Crossplane v2 runtime. It manages Infoblox NIOS DDI (DNS, DHCP, IPAM)
resources declaratively using Kubernetes custom resources.

## Features

- **ARecord** — create and manage Infoblox NIOS DNS "A" records (cluster-scoped
  and namespace-scoped)
- **AAAARecord** — create and manage Infoblox NIOS DNS "AAAA" records
  (cluster-scoped and namespace-scoped)
- **PTRRecord** — create and manage Infoblox NIOS DNS "PTR" records
  (cluster-scoped and namespace-scoped)
- **TXTRecord** — create and manage Infoblox NIOS DNS "TXT" records
  (cluster-scoped and namespace-scoped)
- **ZoneDelegated** — create and manage Infoblox NIOS delegated DNS zones
  (cluster-scoped and namespace-scoped)
- **CNAMERecord** — create and manage Infoblox NIOS DNS "CNAME" records
- **MXRecord** — create and manage Infoblox NIOS DNS "MX" records
  (cluster-scoped and namespace-scoped)
  (cluster-scoped and namespace-scoped)
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

### AAAARecord

Manage Infoblox NIOS DNS "AAAA" records (WAPI object type `record:aaaa`).

**Cluster-scoped** (`recordaaaa.infobloxnios.crossplane.io/v1alpha1`):

```yaml
apiVersion: recordaaaa.infobloxnios.crossplane.io/v1alpha1
kind: AAAARecord
metadata:
  name: example-aaaarecord
spec:
  forProvider:
    name: www6.example.com
    ipv6Addr: "2001:db8::10"
    view: default
    comment: Managed by Crossplane
  providerConfigRef:
    name: default
```

**Namespace-scoped** (`recordaaaa.infobloxnios.m.crossplane.io/v1alpha1`):

```yaml
apiVersion: recordaaaa.infobloxnios.m.crossplane.io/v1alpha1
kind: AAAARecord
metadata:
  name: example-aaaarecord-ns
  namespace: default
spec:
  forProvider:
    name: www6-ns.example.com
    ipv6Addr: "2001:db8::20"
    view: default
  providerConfigRef:
    kind: ClusterProviderConfig
    name: default
```

External name: WAPI assigns an opaque `_ref` reference to every object
(e.g. `record:aaaa/ZG5zLmJpbmRfYWFhYQ:www6.example.com/default`). Crossplane
stores this in the `crossplane.io/external-name` annotation — do not set it
manually.

The `view` field is immutable after creation: WAPI ties an AAAA record's
`_ref` to `(view, zone, name)`, and the underlying SDK's update call has no
`view` parameter.

Apply the full set of example manifests:

```bash
kubectl apply -f examples/record-aaaa/record-aaaa.yaml
kubectl apply -f examples/record-aaaa/record-aaaa-namespaced.yaml
```

### PTRRecord

Manage Infoblox NIOS DNS "PTR" records (WAPI object type `record:ptr`).

**Cluster-scoped** (`recordptr.infobloxnios.crossplane.io/v1alpha1`):

```yaml
apiVersion: recordptr.infobloxnios.crossplane.io/v1alpha1
kind: PTRRecord
metadata:
  name: example-ptrrecord
spec:
  forProvider:
    ptrdname: www.example.com
    ipv4Addr: 192.0.2.10
    view: default
    comment: Managed by Crossplane
  providerConfigRef:
    name: default
```

**Namespace-scoped** (`recordptr.infobloxnios.m.crossplane.io/v1alpha1`):

```yaml
apiVersion: recordptr.infobloxnios.m.crossplane.io/v1alpha1
kind: PTRRecord
metadata:
  name: example-ptrrecord-ns
  namespace: default
spec:
  forProvider:
    ptrdname: www-ns.example.com
    ipv4Addr: 192.0.2.20
    view: default
  providerConfigRef:
    kind: ClusterProviderConfig
    name: default
```

External name: WAPI assigns an opaque `_ref` reference to every object
(e.g. `record:ptr/ZG5zLmJpbmRfcHRy:10.2.0.192.in-addr.arpa/default`).
Crossplane stores this in the `crossplane.io/external-name` annotation — do
not set it manually.

`ptrdname` is required and identifies the FQDN the record points to. `name`
(the in-addr.arpa/ip6.arpa owner name) is auto-derived from `ipv4Addr` or
`ipv6Addr` when omitted. `ptrdname` also carries an optional reference to an
ARecord managed resource (`ptrdnameRef`/`ptrdnameSelector`) so it can resolve
the target FQDN from another managed A record instead of being hand-typed;
WAPI itself does not require the referenced record to exist.

The `view` field is immutable after creation: WAPI ties a PTR record's
`_ref` to `(view, name)`, and the underlying SDK's update call rejects any
attempt to change it ("Field is not allowed for update: view").

Apply the full set of example manifests:

```bash
kubectl apply -f examples/record-ptr/record-ptr.yaml
kubectl apply -f examples/record-ptr/record-ptr-namespaced.yaml
```

### TXTRecord

Manage Infoblox NIOS DNS "TXT" records.

**Cluster-scoped** (`recordtxt.infobloxnios.crossplane.io/v1alpha1`):

```yaml
apiVersion: recordtxt.infobloxnios.crossplane.io/v1alpha1
kind: TXTRecord
metadata:
  name: example-txtrecord
spec:
  forProvider:
    name: txt.example.com
    text: "v=spf1 -all"
    view: default
    comment: Managed by Crossplane
  providerConfigRef:
    name: default
```

**Namespace-scoped** (`recordtxt.infobloxnios.m.crossplane.io/v1alpha1`):

```yaml
apiVersion: recordtxt.infobloxnios.m.crossplane.io/v1alpha1
kind: TXTRecord
metadata:
  name: example-txtrecord-ns
  namespace: default
spec:
  forProvider:
    name: txt-ns.example.com
    text: "v=spf1 -all"
    view: default
  providerConfigRef:
    kind: ClusterProviderConfig
    name: default
```

External name: WAPI assigns an opaque `_ref` reference to every object
(e.g. `record:txt/ZG5zLmJpbmRfdHh0:example.com/default`). Crossplane stores
this in the `crossplane.io/external-name` annotation — do not set it
manually.

The `view` field is soft-immutable after creation: the WAPI schema reports
it as updatable, but a PUT that changes view is rejected at runtime, and a
CEL rule enforces immutability at the CRD level.

Apply the full set of example manifests:

```bash
kubectl apply -f examples/record-txt/record-txt.yaml
kubectl apply -f examples/record-txt/record-txt-namespaced.yaml
```

### ZoneDelegated

Manage Infoblox NIOS delegated DNS zones (WAPI object type `zone_delegated`).
A delegated zone redirects queries for a subdomain to a set of remote name
servers rather than resolving them locally.

**Cluster-scoped** (`zonedelegated.infobloxnios.crossplane.io/v1alpha1`):

```yaml
apiVersion: zonedelegated.infobloxnios.crossplane.io/v1alpha1
kind: ZoneDelegated
metadata:
  name: example-zonedelegated
spec:
  forProvider:
    fqdn: delegated.example.com
    delegateTo:
      - name: ns1.delegate.com
        address: 10.0.0.1
      - name: ns2.delegate.com
        address: 10.0.0.2
    view: default
    comment: Managed by Crossplane
  providerConfigRef:
    name: default
```

### CNAMERecord

Manage Infoblox NIOS DNS "CNAME" records (WAPI object type `record:cname`).

**Cluster-scoped** (`recordcname.infobloxnios.crossplane.io/v1alpha1`):

```yaml
apiVersion: recordcname.infobloxnios.crossplane.io/v1alpha1
kind: CNAMERecord
metadata:
  name: example-cnamerecord
spec:
  forProvider:
    name: alias.example.com
    canonical: www.example.com
    view: default
    comment: Managed by Crossplane
  providerConfigRef:
    name: default
```

**Namespace-scoped** (`zonedelegated.infobloxnios.m.crossplane.io/v1alpha1`):

```yaml
apiVersion: zonedelegated.infobloxnios.m.crossplane.io/v1alpha1
kind: ZoneDelegated
metadata:
  name: example-zonedelegated-ns
  namespace: default
spec:
  forProvider:
    fqdn: delegated-ns.example.com
    delegateTo:
      - name: ns1.delegate.com
        address: 10.0.1.1
      - name: ns2.delegate.com
        address: 10.0.1.2
    view: default
  providerConfigRef:
    kind: ClusterProviderConfig
    name: default
```

**Namespace-scoped** (`recordcname.infobloxnios.m.crossplane.io/v1alpha1`):

```yaml
apiVersion: recordcname.infobloxnios.m.crossplane.io/v1alpha1
kind: CNAMERecord
metadata:
  name: example-cnamerecord-ns
  namespace: default
spec:
  forProvider:
    name: alias-ns.example.com
    canonical: www-ns.example.com
    view: default
  providerConfigRef:
    kind: ClusterProviderConfig
    name: default
```

External name: WAPI assigns an opaque `_ref` reference to every object
(e.g. `zone_delegated/ZG5zLnpvbmUk...:delegated.example.com/default`).
Crossplane stores this in the `crossplane.io/external-name` annotation — do
not set it manually.

The `fqdn`, `view`, and `zoneFormat` fields are immutable after creation: the
underlying SDK's update call has no parameters for them, and WAPI
additionally rejects moving an existing zone between views at the data
level.

External name: WAPI assigns an opaque `_ref` reference to every object
(e.g. `record:cname/ZG5zLmJpbmRfY25hbWUk:alias.example.com/default`).
Crossplane stores this in the `crossplane.io/external-name` annotation — do
not set it manually.

The `view` field is immutable after creation: WAPI ties a CNAME record's
`_ref` to `(view, zone, name)`, and the underlying SDK's update call has no
`view` parameter.

Apply the full set of example manifests:

```bash
kubectl apply -f examples/zone-delegated/zone-delegated.yaml
kubectl apply -f examples/zone-delegated/zone-delegated-namespaced.yaml
kubectl apply -f examples/record-cname/record-cname.yaml
kubectl apply -f examples/record-cname/record-cname-namespaced.yaml
kubectl apply -f examples/record-mx/record-mx.yaml
kubectl apply -f examples/record-mx/record-mx-namespaced.yaml
```

### MXRecord

Manage Infoblox NIOS DNS "MX" records (WAPI object type `record:mx`).

**Cluster-scoped** (`recordmx.infobloxnios.crossplane.io/v1alpha1`):

```yaml
apiVersion: recordmx.infobloxnios.crossplane.io/v1alpha1
kind: MXRecord
metadata:
  name: example-mxrecord
spec:
  forProvider:
    name: test-mx.example.com
    mailExchanger: mail.example.com
    preference: 10
    view: default
    comment: Managed by Crossplane
  providerConfigRef:
    name: default
```

**Namespace-scoped** (`recordmx.infobloxnios.m.crossplane.io/v1alpha1`):

```yaml
apiVersion: recordmx.infobloxnios.m.crossplane.io/v1alpha1
kind: MXRecord
metadata:
  name: example-mxrecord-ns
  namespace: default
spec:
  forProvider:
    name: test-mx-ns.example.com
    mailExchanger: mail-ns.example.com
    preference: 20
    view: default
  providerConfigRef:
    kind: ClusterProviderConfig
    name: default
```

External name: WAPI assigns an opaque `_ref` reference to every object
(e.g. `record:mx/ZG5zLmJpbmRfbXg:test-mx.example.com/default`). Crossplane
stores this in the `crossplane.io/external-name` annotation — do not set it
manually.

The `view` field is immutable after creation: WAPI ties an MX record's
`_ref` to `(view, zone, name)`, and the underlying SDK's update call has no
`view` parameter.

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
