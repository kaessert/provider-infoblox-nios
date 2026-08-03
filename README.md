# provider-infobloxnios

> **Migrating from the Upjet provider?** This provider replaces
> [`crossplane-contrib/provider-infoblox-nios`](https://github.com/crossplane-contrib/provider-infoblox-nios)
> (v0.3.0). The credential format, API groups, and resource kinds have all
> changed. See **[MIGRATION.md](MIGRATION.md)** for a step-by-step guide.

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
- **ZoneAuth** — create and manage Infoblox NIOS authoritative DNS zones
  (cluster-scoped and namespace-scoped)
- **CNAMERecord** — create and manage Infoblox NIOS DNS "CNAME" records
  (cluster-scoped and namespace-scoped)
- **MXRecord** — create and manage Infoblox NIOS DNS "MX" records
  (cluster-scoped and namespace-scoped)
- **NetworkView** — create and manage Infoblox NIOS network views
  (cluster-scoped and namespace-scoped)
- **Network** — create and manage Infoblox NIOS networks and IPv6 networks
  (cluster-scoped and namespace-scoped)
- **NetworkContainer** — create and manage Infoblox NIOS network containers,
  reserving a CIDR block for later child networks and ranges
- **ZoneForward** — create and manage Infoblox NIOS forward DNS zones
  (cluster-scoped and namespace-scoped)
- **FixedAddress** — create and manage Infoblox NIOS DHCP fixed addresses
- **RangeTemplate** — create and manage Infoblox NIOS DHCP range templates
  (cluster-scoped and namespace-scoped)
- **Range** — create and manage Infoblox NIOS DHCP address ranges
  (cluster-scoped and namespace-scoped)
- **DTCServer** — create and manage Infoblox NIOS DTC (DNS Traffic Control)
  servers (cluster-scoped and namespace-scoped)
- **DNSView** — create and manage Infoblox NIOS DNS views (cluster-scoped
  and namespace-scoped)
- Dual-scope managed resources: cluster-scoped (`infobloxnios.crossplane.io`)
  and namespace-scoped (`infobloxnios.m.crossplane.io`)
- Standard Crossplane management policies, usage tracking, and connection
  detail publishing

## Prerequisites

- Kubernetes cluster with [Crossplane v2](https://docs.crossplane.io/) installed
- An Infoblox NIOS Grid Manager appliance reachable from the cluster, with a
  WAPI-capable user account (host, username, password)
- A `Crossplane Internal ID` extensible attribute definition on the Grid — see
  [Identity extensible attribute definition](#identity-extensible-attribute-definition)
  below

### Identity extensible attribute definition

The NIOS WAPI reference (`_ref`) that identifies a Grid object is derived from
that object's own identity fields, not a stable backend-assigned ID — changing
a field like a record's priority computes a different `_ref` for the same
object. To keep track of an object reliably across such changes, this
provider stamps its own `metadata.uid` onto every object it creates, in a
`Crossplane Internal ID` extensible attribute, and resolves objects through
that attribute rather than trusting the stored `_ref` alone.

This applies to every managed resource in this provider's catalog, with two
exceptions that don't need it:

- **`ExtensibleAttributeDef`** — its `name` is unique Grid-wide, so a name
  lookup resolves it unambiguously without a stamp.
- **`NSRecord`** — the WAPI `record:ns` object type has no `extattrs` field at
  all, so it cannot carry one.

**Before installing the provider (or immediately after), make sure the
attribute definition exists on your Grid.** Create it once with a superuser
credential:

```
POST /wapi/v2.12/extensibleattributedef
{"name":"Crossplane Internal ID","type":"STRING","flags":"CR"}
```

Managing extensible attribute definitions is a superuser-only Grid operation —
NIOS has no `permission.resource_type` value that covers it, so it cannot be
delegated to a non-superuser admin group. You have two options:

- **Run the `POST` above once**, with any superuser credential, independently
  of the credential the provider itself uses.
- **Give the provider's own credential (the one in its Secret) superuser
  access.** The provider probes for the definition automatically and creates
  it itself the first time it's needed.

If neither is done, the provider still starts, and controllers that don't
need the guarantee — including the `ExtensibleAttributeDef` controller that
manages this very definition — keep reconciling normally. Only the resources
that need the guarantee are affected: their `Synced` condition turns `False`,
carrying the exact `POST` command above as remediation text, so the problem is
discoverable from `kubectl describe` without reading this document. This
applies to reads as well as writes: with the definition absent, NIOS answers
an identity lookup with `HTTP 400 AdmConProtoError: Unknown extensible
attribute: Crossplane Internal ID`, not an empty result, so even `Observe`
fails closed rather than reporting the resource as gone. Once the definition
exists, the refusal clears on its own — the provider caches the check per
Grid endpoint for about five minutes — with no pod restart or resource
re-creation required.

> This failure mode is implemented and unit-tested as described above. It has
> not yet been exercised end-to-end against a live Grid, so treat this as the
> designed behavior rather than a field-proven one.

## Installation

Complete the [Prerequisites](#prerequisites) above — in particular, the
identity extensible attribute definition — before or immediately after
installing the provider.

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

### 3. Read/Write Endpoint Split (Optional)

Production NIOS grids typically run a Grid Master (GM) as the primary and one
or more Grid Master Candidates (GMC) as read replicas. You can offload
Crossplane's reconcile-loop read traffic onto a candidate by configuring a
`readEndpoint` in your ProviderConfig.

When `readEndpoint` is configured:

- **Writes** (Create / Update / Delete) always go to the **primary** Grid Master
- **Reads** (Observe) go to the **candidate** — but only after the candidate has
  caught up to the primary's latest write (SOA serial convergence gate)
- **IPAM resources** always read from the primary (no SOA serial signal exists
  for IPAM objects)

When `readEndpoint` is omitted, all traffic goes to the primary — identical to
the provider's default single-endpoint behavior.

#### ProviderConfig with readEndpoint

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
      name: infoblox-primary
      key: password
  readEndpoint:
    credentialsRef:
      name: infoblox-candidate
      namespace: crossplane-system
    convergence:
      mode: soaSerial       # default for DNS; compare zone SOA serials
      pollInterval: 2s      # how often to check the candidate's serial
      timeout: 60s          # fall back to primary if not converged in time
```

The `readEndpoint.credentialsRef` secret uses the same format as the primary
(keys: `host`, `username`, `password`), allowing a least-privilege read-only
NIOS account for the candidate:

```bash
# Primary (read-write)
kubectl create secret generic infoblox-primary \
  --namespace crossplane-system \
  --from-literal=host=gm.example.com \
  --from-literal=username=admin \
  --from-literal=password=<password>

# Candidate (read-only)
kubectl create secret generic infoblox-candidate \
  --namespace crossplane-system \
  --from-literal=host=gmc.example.com \
  --from-literal=username=readonly-user \
  --from-literal=password=<password>
```

#### How convergence works

After a DNS write, the controller:

1. Reads the zone's `soa_serial_number` from the primary
2. Sets an annotation (`infobloxnios.crossplane.io/pending-zone-serial`) on the
   managed resource with the expected serial
3. On subsequent Observe calls, checks the candidate's `member_soa_serials` for
   that zone — if the candidate's serial ≥ expected, reads from the candidate;
   otherwise reads from the primary
4. Reports routing state via a `ReadRouting` status condition on each resource

#### Failure handling

- If the candidate is unreachable or returns errors, that Observe falls back to
  the primary and emits a Kubernetes Warning event
- After 5 consecutive candidate failures, the circuit breaker opens and all
  reads are pinned to the primary for 60 seconds (configurable)
- If convergence does not complete within `timeout`, the controller falls back
  to the primary and emits a Warning condition with the lag delta
- A misconfigured `readEndpoint` (missing Secret, missing keys) fails
  `Connect()` loudly — it never silently degrades to primary-only

#### Convergence modes

| Mode | Behavior | Default for |
|------|----------|-------------|
| `soaSerial` | Compare zone SOA serials between endpoints | DNS resources |
| `primaryOnly` | Always read from primary | IPAM resources (automatic) |

IPAM resources (`Network`, `FixedAddress`, etc.) are hardcoded to `primaryOnly`
regardless of the configured mode — no SOA serial signal exists for IPAM
objects.

> **Note:** This feature requires a GM + GMC grid pair. The convergence gate
> design has been validated against a single Grid Master. Replication behavior
> across a real GM/GMC pair is the remaining validation gap before production
> use.

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

### AliasRecord

Manage Infoblox NIOS DNS "Alias" records (WAPI object type `record:alias`).
An alias record resolves queries for its own name to another target name of
a specified record type (A, AAAA, MX, NAPTR, PTR, SPF, SRV, or TXT).

**Cluster-scoped** (`recordalias.infobloxnios.crossplane.io/v1alpha1`):

```yaml
apiVersion: recordalias.infobloxnios.crossplane.io/v1alpha1
kind: AliasRecord
metadata:
  name: example-aliasrecord
spec:
  forProvider:
    name: alias-test.example.com
    targetName: target.example.com
    targetType: A
    view: default
    comment: Managed by Crossplane
  providerConfigRef:
    name: default
```

**Namespace-scoped** (`recordalias.infobloxnios.m.crossplane.io/v1alpha1`):

```yaml
apiVersion: recordalias.infobloxnios.m.crossplane.io/v1alpha1
kind: AliasRecord
metadata:
  name: example-aliasrecord-ns
  namespace: default
spec:
  forProvider:
    name: alias-test-ns.example.com
    targetName: target-ns.example.com
    targetType: A
    view: default
  providerConfigRef:
    kind: ClusterProviderConfig
    name: default
```

External name: WAPI assigns an opaque `_ref` reference to every object
(e.g. `record:alias/ZG5zLmJpbmRfYWxpYXMk:alias-test.example.com/default`).
Crossplane stores this in the `crossplane.io/external-name` annotation — do
not set it manually.

The `view` field is soft-immutable: the WAPI schema advertises it as
updatable, but a live update attempt is rejected at the data level, so it is
treated as fixed at creation (CEL rule enforced). `targetName` and
`targetType` are mutable — updating them does not change the record's
`_ref`.

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

### HostRecord

Manage Infoblox NIOS host records (WAPI object type `record:host`). A host
record combines one or more IPv4/IPv6 addresses with a DNS name under a
single object, optionally with DHCP association.

**Cluster-scoped** (`hostrecord.infobloxnios.crossplane.io/v1alpha1`):

```yaml
apiVersion: hostrecord.infobloxnios.crossplane.io/v1alpha1
kind: HostRecord
metadata:
  name: example-hostrecord
spec:
  forProvider:
    name: test-host.example.com
    ipv4Addrs:
      - ipv4Addr: 10.0.0.100
    view: default
    comment: Managed by Crossplane
  providerConfigRef:
    name: default
```

**Namespace-scoped** (`hostrecord.infobloxnios.m.crossplane.io/v1alpha1`):

```yaml
apiVersion: hostrecord.infobloxnios.m.crossplane.io/v1alpha1
kind: HostRecord
metadata:
  name: example-hostrecord-ns
  namespace: default
spec:
  forProvider:
    name: test-host-ns.example.com
    ipv4Addrs:
      - ipv4Addr: 10.0.0.101
    view: default
  providerConfigRef:
    kind: ClusterProviderConfig
    name: default
```

External name: WAPI assigns an opaque `_ref` reference to every object
(e.g. `record:host/ZG5zLmhvc3Rfcm9v:test-host.example.com/default`).
Crossplane stores this in the `crossplane.io/external-name` annotation — do
not set it manually.

The `networkView` field is immutable after creation (the underlying SDK's
update call has no `networkView` parameter). The `view` field is mutable,
but changing it moves the record between DNS views and changes the `_ref`.

Apply the full set of example manifests:

```bash
kubectl apply -f examples/host-record/host-record.yaml
kubectl apply -f examples/host-record/host-record-namespaced.yaml
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

External name: WAPI assigns an opaque `_ref` reference to every object
(e.g. `zone_delegated/ZG5zLnpvbmUk...:delegated.example.com/default`).
Crossplane stores this in the `crossplane.io/external-name` annotation — do
not set it manually.

The `fqdn`, `view`, and `zoneFormat` fields are immutable after creation: the
underlying SDK's update call has no parameters for them, and WAPI
additionally rejects moving an existing zone between views at the data
level.

Apply the full set of example manifests:

```bash
kubectl apply -f examples/zone-delegated/zone-delegated.yaml
kubectl apply -f examples/zone-delegated/zone-delegated-namespaced.yaml
```

### ZoneAuth

Manage Infoblox NIOS authoritative DNS zones (WAPI object type `zone_auth`).

**Cluster-scoped** (`zoneauth.infobloxnios.crossplane.io/v1alpha1`):

```yaml
apiVersion: zoneauth.infobloxnios.crossplane.io/v1alpha1
kind: ZoneAuth
metadata:
  name: example-zoneauth
spec:
  forProvider:
    fqdn: example.com
  providerConfigRef:
    name: default
```

**Namespace-scoped** (`zoneauth.infobloxnios.m.crossplane.io/v1alpha1`):

```yaml
apiVersion: zoneauth.infobloxnios.m.crossplane.io/v1alpha1
kind: ZoneAuth
metadata:
  name: example-zoneauth-ns
  namespace: default
spec:
  forProvider:
    fqdn: example-ns.com
    comment: Managed by Crossplane (namespaced)
  providerConfigRef:
    kind: ClusterProviderConfig
    name: default
```

External name: WAPI assigns an opaque `_ref` reference to every object
(e.g. `zone_auth/ZG5zLnpvbmUk...:example.com/default`). Crossplane stores
this in the `crossplane.io/external-name` annotation — do not set it
manually.

The `fqdn`, `view`, and `zoneFormat` fields are immutable after creation:
WAPI rejects a PUT that changes `fqdn` or `zoneFormat` with "Field is not
allowed for update", and rejects a `view` change with "Cannot move zones
between views". All other fields (`comment`, `disable`, the SOA timers,
`nsGroup`, `extAttrs`, and the primary/secondary server lists) are mutable
in place via WAPI PUT.

Apply the full set of example manifests:

```bash
kubectl apply -f examples/zone-auth/zone-auth.yaml
kubectl apply -f examples/zone-auth/zone-auth-namespaced.yaml
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

External name: WAPI assigns an opaque `_ref` reference to every object
(e.g. `zone_delegated/ZG5zLnpvbmUk...:delegated.example.com/default`).
Crossplane stores this in the `crossplane.io/external-name` annotation — do
not set it manually.

The `fqdn`, `view`, and `zoneFormat` fields are immutable after creation: the
underlying SDK's update call has no parameters for them, and WAPI
additionally rejects moving an existing zone between views at the data
level.

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
(e.g. `record:cname/ZG5zLmJpbmRfY25hbWUk:alias.example.com/default`).
Crossplane stores this in the `crossplane.io/external-name` annotation — do
not set it manually.

The `view` field is immutable after creation: WAPI ties a CNAME record's
`_ref` to `(view, zone, name)`, and the underlying SDK's update call has no
`view` parameter.

Apply the full set of example manifests:

```bash
kubectl apply -f examples/record-cname/record-cname.yaml
kubectl apply -f examples/record-cname/record-cname-namespaced.yaml
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

Apply the full set of example manifests:

```bash
kubectl apply -f examples/record-mx/record-mx.yaml
kubectl apply -f examples/record-mx/record-mx-namespaced.yaml

### NSRecord

Manage Infoblox NIOS DNS delegation "NS" records (WAPI object type
`record:ns`).

**Cluster-scoped** (`recordns.infobloxnios.crossplane.io/v1alpha1`):

```yaml
apiVersion: recordns.infobloxnios.crossplane.io/v1alpha1
kind: NSRecord
metadata:
  name: example-nsrecord
spec:
  forProvider:
    name: subdomain.zone.com
    nameserver: ns1.example.com
    addresses:
      - address: 192.0.2.53
        autoCreatePtr: false
    view: default
  providerConfigRef:
    name: default
```

**Namespace-scoped** (`recordns.infobloxnios.m.crossplane.io/v1alpha1`):

```yaml
apiVersion: recordns.infobloxnios.m.crossplane.io/v1alpha1
kind: NSRecord
metadata:
  name: example-nsrecord-ns
  namespace: default
spec:
  forProvider:
    name: subdomain-ns.zone.com
    nameserver: ns1.example.com
    addresses:
      - address: 192.0.2.54
        autoCreatePtr: false
    view: default
  providerConfigRef:
    kind: ClusterProviderConfig
    name: default
```

External name: WAPI assigns an opaque `_ref` reference to every object
(e.g. `record:ns/ZG5zLmJpbmRfbnM:subdomain.zone.com/default`). Crossplane
stores this in the `crossplane.io/external-name` annotation — do not set it
manually.

The `name` and `view` fields are immutable after creation: WAPI ties an NS
record's `_ref` to `(view, zone, name)`, and the underlying SDK's update
call drops both parameters from the request body. `addresses` is required
on create — WAPI rejects a create request that omits it.

Grid policy may block manual NS record creation for zones that are not
delegated. Point `name`/`nameserver` at a zone/nameserver pair your Grid
Manager permits before applying.

Apply the full set of example manifests:

```bash
kubectl apply -f examples/record-ns/record-ns.yaml
kubectl apply -f examples/record-ns/record-ns-namespaced.yaml
```

### SRVRecord

Manage Infoblox NIOS DNS "SRV" records (WAPI object type `record:srv`).

**Cluster-scoped** (`recordsrv.infobloxnios.crossplane.io/v1alpha1`):

```yaml
apiVersion: recordsrv.infobloxnios.crossplane.io/v1alpha1
kind: SRVRecord
metadata:
  name: example-srvrecord
spec:
  forProvider:
    name: _sip._tcp.example.com
    target: sipserver.example.com
    priority: 10
    weight: 20
    port: 5060
    view: default
    comment: Managed by Crossplane
  providerConfigRef:
    name: default
```

**Namespace-scoped** (`recordsrv.infobloxnios.m.crossplane.io/v1alpha1`):

```yaml
apiVersion: recordsrv.infobloxnios.m.crossplane.io/v1alpha1
kind: SRVRecord
metadata:
  name: example-srvrecord-ns
  namespace: default
spec:
  forProvider:
    name: _sip._tcp.ns.example.com
    target: sipserver-ns.example.com
    priority: 10
    weight: 20
    port: 5060
    view: default
    comment: Managed by Crossplane (namespaced)
  providerConfigRef:
    kind: ClusterProviderConfig
    name: default
```

External name: WAPI assigns an opaque `_ref` reference to every object
(e.g. `record:srv/ZG5zLmJpbmRfc3J2:_sip._tcp.example.com/default`). Crossplane
stores this in the `crossplane.io/external-name` annotation — do not set it
manually.

The `view` field is immutable after creation: WAPI ties an SRV record's
`_ref` to `(view, zone, name)`, and the underlying SDK's update call has no
`view` parameter.

Apply the full set of example manifests:

```bash
kubectl apply -f examples/record-srv/record-srv.yaml
kubectl apply -f examples/record-srv/record-srv-namespaced.yaml
```

### ZoneForward

Manage Infoblox NIOS forward DNS zones (WAPI object type `zone_forward`). A
forward zone directs queries for a subdomain to a set of remote name servers
rather than resolving them locally.

**Cluster-scoped** (`zoneforward.infobloxnios.crossplane.io/v1alpha1`):

```yaml
apiVersion: zoneforward.infobloxnios.crossplane.io/v1alpha1
kind: ZoneForward
metadata:
  name: example-zoneforward
spec:
  forProvider:
    fqdn: forward.example.com
    forwardTo:
      - name: ns1.forwarder.com
        address: 10.0.0.1
      - name: ns2.forwarder.com
        address: 10.0.0.2
    view: default
    comment: Managed by Crossplane
  providerConfigRef:
    name: default
```

**Namespace-scoped** (`zoneforward.infobloxnios.m.crossplane.io/v1alpha1`):

```yaml
apiVersion: zoneforward.infobloxnios.m.crossplane.io/v1alpha1
kind: ZoneForward
metadata:
  name: example-zoneforward-ns
  namespace: default
spec:
  forProvider:
    fqdn: forward-ns.example.com
    forwardTo:
      - name: ns1.forwarder.com
        address: 10.0.1.1
      - name: ns2.forwarder.com
        address: 10.0.1.2
    view: default
  providerConfigRef:
    kind: ClusterProviderConfig
    name: default
```

External name: WAPI assigns an opaque `_ref` reference to every object
(e.g. `zone_forward/ZG5zLnpvbmUk...:forward.example.com/default`). Crossplane
stores this in the `crossplane.io/external-name` annotation — do not set it
manually.

The `fqdn`, `view`, and `zoneFormat` fields are immutable after creation: the
underlying SDK's update call has no parameters for them, and WAPI
additionally rejects moving an existing zone between views at the data
level.

Apply the full set of example manifests:

```bash
kubectl apply -f examples/zone-forward/zone-forward.yaml
kubectl apply -f examples/zone-forward/zone-forward-namespaced.yaml
```

### RangeTemplate

Manage Infoblox NIOS DHCP range templates (WAPI object type `rangetemplate`).
A range template captures the address count and offset used to instantiate
new DHCP `Range` objects consistently across networks.

**Cluster-scoped** (`rangetemplate.infobloxnios.crossplane.io/v1alpha1`):

```yaml
apiVersion: rangetemplate.infobloxnios.crossplane.io/v1alpha1
kind: RangeTemplate
metadata:
  name: example-range-template
spec:
  forProvider:
    name: example-range-template
    numberOfAddresses: 10
    offset: 10
    comment: Managed by Crossplane
  providerConfigRef:
    name: default
```

**Namespace-scoped** (`rangetemplate.infobloxnios.m.crossplane.io/v1alpha1`):

```yaml
apiVersion: rangetemplate.infobloxnios.m.crossplane.io/v1alpha1
kind: RangeTemplate
metadata:
  name: example-range-template-ns
  namespace: default
spec:
  forProvider:
    name: example-range-template-ns
    numberOfAddresses: 20
    offset: 50
  providerConfigRef:
    kind: ClusterProviderConfig
    name: default
```

External name: WAPI assigns an opaque `_ref` reference to every object
(e.g. `rangetemplate/ZG5zLnJhbmdl...:my-range-template`). Crossplane stores
this in the `crossplane.io/external-name` annotation — do not set it
manually.

Unlike the DNS record types above, RangeTemplate has no known immutable
fields: every parameter accepted by the WAPI create call is also accepted by
the update call, including `name`.

Apply the full set of example manifests:

```bash
kubectl apply -f examples/range-template/range-template.yaml
kubectl apply -f examples/range-template/range-template-namespaced.yaml
```

### NetworkView

Manage Infoblox NIOS network views (WAPI object type `networkview`).

**Cluster-scoped** (`networkview.infobloxnios.crossplane.io/v1alpha1`):

```yaml
apiVersion: networkview.infobloxnios.crossplane.io/v1alpha1
kind: NetworkView
metadata:
  name: example-network-view
spec:
  forProvider:
    name: "test-network-view"
    comment: "Test network view created by Crossplane"
  providerConfigRef:
    name: default
```

**Namespace-scoped** (`networkview.infobloxnios.m.crossplane.io/v1alpha1`):

```yaml
apiVersion: networkview.infobloxnios.m.crossplane.io/v1alpha1
kind: NetworkView
metadata:
  name: example-network-view-ns
  namespace: default
spec:
  forProvider:
    name: "test-network-view-ns"
    comment: "Test namespaced network view created by Crossplane"
  providerConfigRef:
    kind: ClusterProviderConfig
    name: default
```

External name: WAPI assigns an opaque `_ref` reference to every object
(e.g. `networkview/ZG5z...:test-network-view/false`). Crossplane stores this
in the `crossplane.io/external-name` annotation — do not set it manually.

The Grid always has a well-known "default" network view (`is_default=true`)
that cannot be deleted or un-defaulted; `isDefault` is a server-controlled,
read-only field. Standard CRUD applies to any additional network view you
create, such as the example above.

Apply the full set of example manifests:

```bash
kubectl apply -f examples/network-view/network-view.yaml
kubectl apply -f examples/network-view/network-view-namespaced.yaml
```

### Network

Manage Infoblox NIOS networks (WAPI object type `network`, or `ipv6network`
for an IPv6 CIDR — the type is selected automatically from the CIDR format).

**Cluster-scoped** (`network.infobloxnios.crossplane.io/v1alpha1`):

```yaml
apiVersion: network.infobloxnios.crossplane.io/v1alpha1
kind: Network
metadata:
  name: example-network
spec:
  forProvider:
    networkView: default
    network: 198.51.100.0/24
  providerConfigRef:
    name: default
```

**Namespace-scoped** (`network.infobloxnios.m.crossplane.io/v1alpha1`):

```yaml
apiVersion: network.infobloxnios.m.crossplane.io/v1alpha1
kind: Network
metadata:
  name: example-network-ns
  namespace: default
spec:
  forProvider:
    networkView: default
    network: 198.51.101.0/24
  providerConfigRef:
    kind: ClusterProviderConfig
    name: default
```

External name: WAPI assigns an opaque `_ref` reference to every object.
Crossplane stores this in the `crossplane.io/external-name` annotation — do
not set it manually.

Both `networkView` and `network` are immutable after creation: the
underlying SDK's update call has no parameters for either field. `networkView`
references a NetworkView by name; this example uses the Grid's well-known
"default" network view so it runs standalone without creating a NetworkView
resource first.

**Dynamic CIDR allocation.** Instead of a static `network` CIDR, a Network
can be created with `filterParams` (an extensible-attribute search) plus
`allocatePrefixLen` and `object`, and the controller allocates a free
subnet from a matching parent object (e.g. a NetworkContainer) instead:

```yaml
apiVersion: network.infobloxnios.crossplane.io/v1alpha1
kind: Network
metadata:
  name: example-network-allocate
spec:
  forProvider:
    networkView: default
    filterParams:
      "*Site": network-allocate-demo
    allocatePrefixLen: 28
    object: networkcontainer
  providerConfigRef:
    name: default
```

The `network-allocate-demo` value above is illustrative — it shows the
`filterParams` API shape. The shipped example pair derives its own value
per run (see below).

`filterParams` keys must carry the WAPI extensible-attribute search prefix
`*` — `"*Site"`, not `"Site"`. A bare key is rejected by WAPI with
`AdmConProtoError: Unknown argument/field: 'Site'`. The parent object
(here, a NetworkContainer) must already exist and carry a matching
extensible attribute for the allocation to succeed — see
`examples/network/network-allocate-parent.yaml` and
`examples/network/network-allocate.yaml` for the version wired into the
E2E suite. The controller late-initializes `spec.forProvider.network` with
the allocated CIDR once creation succeeds. A `parentCidr` field is also
available for allocating from a fixed parent CIDR instead of an
extensible-attribute search; `network`, `parentCidr`, and `filterParams`
are mutually exclusive — exactly one must be set.

Apply the full set of example manifests:

```bash
kubectl apply -f examples/network/network.yaml
kubectl apply -f examples/network/network-namespaced.yaml
```

The dynamic-allocation pair (`network-allocate-parent.yaml` +
`network-allocate.yaml`) uses per-run `${data.*}` placeholders for the
parent CIDR and the shared `Site` extensible-attribute search value, so a
literal `kubectl apply` does not produce a valid manifest. Run it through
the provider's end-to-end suite instead, which resolves the placeholders
and applies the parent before the child:

```bash
make e2e.network-allocate
```

### DNSView

Manage Infoblox NIOS DNS views (WAPI object type `view`).

**Cluster-scoped** (`dnsview.infobloxnios.crossplane.io/v1alpha1`):

```yaml
apiVersion: dnsview.infobloxnios.crossplane.io/v1alpha1
kind: DNSView
metadata:
  name: example-dnsview
spec:
  forProvider:
    name: crossplane-test-view
    comment: Managed by Crossplane
  providerConfigRef:
    name: default
```

**Namespace-scoped** (`dnsview.infobloxnios.m.crossplane.io/v1alpha1`):

```yaml
apiVersion: dnsview.infobloxnios.m.crossplane.io/v1alpha1
kind: DNSView
metadata:
  name: example-dnsview-ns
  namespace: default
spec:
  forProvider:
    name: crossplane-test-view-ns
    comment: Managed by Crossplane (namespaced)
  providerConfigRef:
    kind: ClusterProviderConfig
    name: default
```

External name: WAPI assigns an opaque `_ref` reference to every object
(e.g. `view/ZG5zLnZpZXckY3Jvc3NwbGFuZS10ZXN0LXZpZXcvZmFsc2U:crossplane-test-view/false`).
Crossplane stores this in the `crossplane.io/external-name` annotation — do
not set it manually.

The NIOS Grid Manager always ships three pre-existing views (`default`,
`External`, `Internal`); these examples create a distinct custom view
instead of trying to re-create one of the built-in views, so they never
collide with a fresh Grid Manager. The `isDefault` field is response-only
(WAPI `supports=sr`) and immutable once set — it has no `forProvider`
representation.

Apply the full set of example manifests:

```bash
kubectl apply -f examples/dns-view/dns-view.yaml
kubectl apply -f examples/dns-view/dns-view-namespaced.yaml
```

### IPv4SharedNetwork
### ExtensibleAttributeDef

Manage Infoblox NIOS extensible attribute definitions (WAPI object type
`extensibleattributedef`) — the custom metadata fields defined in the NIOS
Grid Manager that other objects reference via their `extAttrs` map.

**Cluster-scoped** (`extensibleattributedef.infobloxnios.crossplane.io/v1alpha1`):

```yaml
apiVersion: extensibleattributedef.infobloxnios.crossplane.io/v1alpha1
kind: ExtensibleAttributeDef
metadata:
  name: example-ea-def
spec:
  forProvider:
    name: example-ea-def
    type: STRING
    comment: Managed by Crossplane
  providerConfigRef:
    name: default
```

**Namespace-scoped** (`extensibleattributedef.infobloxnios.m.crossplane.io/v1alpha1`):

```yaml
apiVersion: extensibleattributedef.infobloxnios.m.crossplane.io/v1alpha1
kind: ExtensibleAttributeDef
metadata:
  name: example-ea-def-ns
  namespace: default
spec:
  forProvider:
    name: example-ea-def-ns
    type: STRING
    comment: Managed by Crossplane (namespaced)
  providerConfigRef:
    kind: ClusterProviderConfig
    name: default
```

External name: WAPI assigns an opaque `_ref` reference to every object.
Crossplane stores this in the `crossplane.io/external-name` annotation — do
not set it manually.

The `type`, `min`, and `max` fields are immutable after creation: WAPI
rejects updates to them ("cannot be modified"). `name`, `comment`,
`defaultValue`, `flags`, `listValues`, and `allowedObjectTypes` are mutable.

Apply the full set of example manifests:

```bash
kubectl apply -f examples/extensible-attribute-def/extensible-attribute-def.yaml
kubectl apply -f examples/extensible-attribute-def/extensible-attribute-def-namespaced.yaml
```

Manage Infoblox NIOS IPv4 shared networks (WAPI object type `sharednetwork`)
— a group of member networks that share a single DHCP address pool.

**Cluster-scoped** (`ipv4sharednetwork.infobloxnios.crossplane.io/v1alpha1`):

```yaml
apiVersion: ipv4sharednetwork.infobloxnios.crossplane.io/v1alpha1
kind: IPv4SharedNetwork
metadata:
  name: example-ipv4-shared-network
spec:
  forProvider:
    name: example-shared-network
    networks:
      - 203.0.113.0/25
    networkView: default
### DTCServer

Manage Infoblox NIOS DTC (DNS Traffic Control) servers (WAPI object type
`dtc:server`). A DTC Server represents a backend server (identified by
address or FQDN) that DTC pools and LBDNs can distribute traffic to.

**Cluster-scoped** (`dtcserver.infobloxnios.crossplane.io/v1alpha1`):

```yaml
apiVersion: dtcserver.infobloxnios.crossplane.io/v1alpha1
kind: DTCServer
metadata:
  name: example-dtcserver
spec:
  forProvider:
    name: example-server.example.com
    host: 192.0.2.30
    comment: Managed by Crossplane
  providerConfigRef:
    name: default
```

### Range

Manage Infoblox NIOS DHCP address ranges (WAPI object type `range`) — a
contiguous block of addresses within a network that DHCP can lease out.

**Cluster-scoped** (`range.infobloxnios.crossplane.io/v1alpha1`):

```yaml
apiVersion: range.infobloxnios.crossplane.io/v1alpha1
kind: Range
metadata:
  name: example-range
spec:
  forProvider:
    startAddr: 203.0.113.100
    endAddr: 203.0.113.120
    networkView: default
    comment: Managed by Crossplane
  providerConfigRef:
    name: default
```

**Namespace-scoped** (`ipv4sharednetwork.infobloxnios.m.crossplane.io/v1alpha1`):

```yaml
apiVersion: ipv4sharednetwork.infobloxnios.m.crossplane.io/v1alpha1
kind: IPv4SharedNetwork
metadata:
  name: example-ipv4-shared-network-ns
  namespace: default
spec:
  forProvider:
    name: example-shared-network-ns
    networks:
      - 203.0.113.128/25
    networkView: default
    comment: Managed by Crossplane (namespaced)
**Namespace-scoped** (`dtcserver.infobloxnios.m.crossplane.io/v1alpha1`):

```yaml
apiVersion: dtcserver.infobloxnios.m.crossplane.io/v1alpha1
kind: DTCServer
metadata:
  name: example-dtcserver-ns
  namespace: default
spec:
  forProvider:
    name: example-server-ns.example.com
    host: 192.0.2.31
  providerConfigRef:
    kind: ClusterProviderConfig
    name: default
```

**Namespace-scoped** (`range.infobloxnios.m.crossplane.io/v1alpha1`):

```yaml
apiVersion: range.infobloxnios.m.crossplane.io/v1alpha1
kind: Range
metadata:
  name: example-range-ns
  namespace: default
spec:
  forProvider:
    startAddr: 203.0.113.160
    endAddr: 203.0.113.180
    networkView: default
    comment: Managed by Crossplane (namespaced)
  providerConfigRef:
    kind: ClusterProviderConfig
    name: default
```

External name: WAPI assigns an opaque `_ref` reference to every object.
Crossplane stores this in the `crossplane.io/external-name` annotation —
do not set it manually.

Each entry in `networks` must match the CIDR of an existing Network object
on the Grid Manager — WAPI validates shared-network membership against real
network objects, not arbitrary strings. This provider ships a Network
managed resource; create the referenced Network objects first (or ensure
they already exist on the target Grid Manager) before applying an
IPv4SharedNetwork that references their CIDRs.

The `networkView` field is immutable after creation: although the
underlying SDK's update call accepts a `networkView` parameter, live WAPI
schema probing found the Grid Manager rejects changing it once the shared
network is created. All other fields (`name`, `comment`, `extAttrs`,
`disable`, `useOptions`, `options`) are mutable in place via WAPI PUT.

`template` (the optional `RangeTemplate` to pre-populate settings from) is a
create-only parameter: `UpdateNetworkRange` does not accept it, so it is
excluded from drift comparison and has no `atProvider` mirror. `startAddr`,
`endAddr`, `networkView`, `network`, `comment`, and `extAttrs` are all
mutable in place via WAPI PUT. This example references the Grid's
well-known "default" network view and omits the optional `network` field so
it runs standalone without requiring a pre-existing `Network` object.

Apply the full set of example manifests:

```bash
kubectl apply -f examples/ipv4-shared-network/ipv4-shared-network.yaml
kubectl apply -f examples/ipv4-shared-network/ipv4-shared-network-namespaced.yaml
kubectl apply -f examples/range/range.yaml
kubectl apply -f examples/range/range-namespaced.yaml
```

### NetworkContainer

Manage Infoblox NIOS network containers (WAPI object type
`networkcontainer`). A network container reserves a CIDR block from which
child networks and DHCP ranges can later be carved.

**Cluster-scoped** (`networkcontainer.infobloxnios.crossplane.io/v1alpha1`):

```yaml
apiVersion: networkcontainer.infobloxnios.crossplane.io/v1alpha1
kind: NetworkContainer
metadata:
  name: example-network-container
spec:
  forProvider:
    networkView: default
    network: 172.25.0.0/16
    comment: Managed by Crossplane
  providerConfigRef:
    name: default
```

**Namespace-scoped** (`networkcontainer.infobloxnios.m.crossplane.io/v1alpha1`):

```yaml
apiVersion: networkcontainer.infobloxnios.m.crossplane.io/v1alpha1
kind: NetworkContainer
metadata:
  name: example-network-container-ns
  namespace: default
spec:
  forProvider:
    networkView: default
    network: 172.26.0.0/16
  providerConfigRef:
    kind: ClusterProviderConfig
    name: default
```

External name: WAPI assigns an opaque `_ref` reference to every object.
Crossplane stores this in the `crossplane.io/external-name` annotation — do
not set it manually.

Both `networkView` and `network` are immutable after creation: the
underlying SDK's update call has no parameters for either field.
`networkView` is a cross-resource reference to a NetworkView (by name, with
`networkViewRef`/`networkViewSelector` also available); this example uses
the Grid's well-known "default" network view so it runs standalone without
creating a NetworkView resource first.
External name: WAPI assigns an opaque `_ref` reference to every object
(e.g. `dtc:server/ZG5zLmRfoi5zZXJ2ZXIkX2V4YW1wbGU:example-server`).
Crossplane stores this in the `crossplane.io/external-name` annotation — do
not set it manually.

Create is synchronous: WAPI's POST returns the `_ref` immediately, so no
special create-pending handling is required.

Apply the full set of example manifests:

```bash
kubectl apply -f examples/network-container/network-container.yaml
kubectl apply -f examples/network-container/network-container-namespaced.yaml
kubectl apply -f examples/zone-forward/zone-forward.yaml
kubectl apply -f examples/zone-forward/zone-forward-namespaced.yaml
```

### FixedAddress

Manage Infoblox NIOS DHCP fixed addresses (WAPI object type `fixedaddress`,
or `ipv6fixedaddress` for an IPv6 address). A fixed address reserves a
specific IP for a specific client, identified by MAC address, DHCP client
identifier, or circuit/remote ID, depending on `matchClient`.

**Cluster-scoped** (`fixedaddress.infobloxnios.crossplane.io/v1alpha1`):

```yaml
apiVersion: fixedaddress.infobloxnios.crossplane.io/v1alpha1
kind: FixedAddress
metadata:
  name: example-fixed-address
spec:
  forProvider:
    ipv4addr: 10.0.0.50
    mac: "00:00:00:00:00:00"
    networkView: default
    comment: Managed by Crossplane
  providerConfigRef:
    name: default
```

**Namespace-scoped** (`fixedaddress.infobloxnios.m.crossplane.io/v1alpha1`):

```yaml
apiVersion: fixedaddress.infobloxnios.m.crossplane.io/v1alpha1
kind: FixedAddress
metadata:
  name: example-fixed-address-ns
  namespace: default
spec:
  forProvider:
    ipv4addr: 10.0.0.51
    mac: "00:00:00:00:00:00"
    networkView: default
    comment: Managed by Crossplane (namespaced)
  providerConfigRef:
    kind: ClusterProviderConfig
    name: default
```

External name: WAPI's `AllocateIP` call (the SDK's non-standard name for
Create on this object type) returns an opaque `_ref` reference
(e.g. `fixedaddress/ZG5zLmZpeGVkX2FkZHJlc3Mk...:10.0.0.50/default`).
Crossplane stores this in the `crossplane.io/external-name` annotation — do
not set it manually. The `_ref` is unstable: changing `ipv4addr`/`ipv6addr`
changes it.

Exactly one of `ipv4addr` or `ipv6addr` must be set (mutually exclusive
address families, enforced by a CRD validation rule). No immutable fields
are known for this resource — every parameter accepted by the WAPI create
call is also accepted by the update call.

Apply the full set of example manifests:

```bash
kubectl apply -f examples/fixed-address/fixed-address.yaml
kubectl apply -f examples/fixed-address/fixed-address-namespaced.yaml
kubectl apply -f examples/dtc-server/dtc-server.yaml
kubectl apply -f examples/dtc-server/dtc-server-namespaced.yaml
```

### DTCPool

Manage Infoblox NIOS DTC (DNS Traffic Control) pools (WAPI object type
`dtc:pool`). A DTC Pool groups DTCServer members behind a load-balancing
method (round robin, ratio, topology, dynamic ratio, global availability,
or source-IP hash) and is in turn referenced by a DTCLBDN.

**Cluster-scoped** (`dtcpool.infobloxnios.crossplane.io/v1alpha1`):

```yaml
apiVersion: dtcpool.infobloxnios.crossplane.io/v1alpha1
kind: DTCPool
metadata:
  name: example-dtcpool
spec:
  forProvider:
    name: test-dtc-pool
    lbPreferredMethod: ROUND_ROBIN
    comment: Managed by Crossplane
  providerConfigRef:
    name: default
```

**Namespace-scoped** (`dtcpool.infobloxnios.m.crossplane.io/v1alpha1`):

```yaml
apiVersion: dtcpool.infobloxnios.m.crossplane.io/v1alpha1
kind: DTCPool
metadata:
  name: example-dtcpool-ns
  namespace: default
spec:
  forProvider:
    name: test-dtc-pool-ns
    lbPreferredMethod: ROUND_ROBIN
    comment: Managed by Crossplane (namespaced)
  providerConfigRef:
    kind: ClusterProviderConfig
    name: default
```

External name: WAPI assigns an opaque `_ref` reference to every object.
Crossplane stores this in the `crossplane.io/external-name` annotation — do
not set it manually. Create is synchronous: WAPI's POST returns the `_ref`
immediately, so no special create-pending handling is required.

`servers` (the DTCServer members of the pool, each with a per-server
`ratio` weight) is optional and references DTCServer resources by external
name via `serverRef`/`serverSelector`; these examples omit it so they run
standalone without requiring a pre-existing DTCServer. `name` is mutable,
but changes are not reference-resolved on the fly by DTCLBDN resources that
reference this pool. All other fields (`lbAlternateMethod`, `servers`,
`availability`, `quorum`, `comment`, `disable`, `ttl`, `useTtl`,
`extAttrs`, and the topology/dynamic-ratio settings) are mutable in place
via WAPI PUT.

Apply the full set of example manifests:

```bash
kubectl apply -f examples/dtc-pool/dtc-pool.yaml
kubectl apply -f examples/dtc-pool/dtc-pool-namespaced.yaml
```

### DTCLBDN

Manage Infoblox NIOS DTC (DNS Traffic Control) Load Balanced Domain Names
(WAPI object type `dtc:lbdn`). A DTCLBDN matches incoming DNS queries
against a set of FQDN wildcard patterns and answers them by distributing
traffic across one or more DTCPool members.

**Cluster-scoped** (`dtclbdn.infobloxnios.crossplane.io/v1alpha1`):

```yaml
apiVersion: dtclbdn.infobloxnios.crossplane.io/v1alpha1
kind: DTCLBDN
metadata:
  name: example-dtclbdn
spec:
  forProvider:
    name: test-dtc-lbdn
    lbMethod: ROUND_ROBIN
    patterns:
      - "*.example.com"
    comment: Managed by Crossplane
  providerConfigRef:
    name: default
```

**Namespace-scoped** (`dtclbdn.infobloxnios.m.crossplane.io/v1alpha1`):

```yaml
apiVersion: dtclbdn.infobloxnios.m.crossplane.io/v1alpha1
kind: DTCLBDN
metadata:
  name: example-dtclbdn-ns
  namespace: default
spec:
  forProvider:
    name: test-dtc-lbdn-ns
    lbMethod: ROUND_ROBIN
    patterns:
      - "*.namespaced.example.com"
    comment: Managed by Crossplane (namespaced)
  providerConfigRef:
    kind: ClusterProviderConfig
    name: default
```

External name: WAPI assigns an opaque `_ref` reference to every object.
Crossplane stores this in the `crossplane.io/external-name` annotation — do
not set it manually. Create is synchronous: WAPI's POST returns the `_ref`
immediately, so no special create-pending handling is required.

`pools` (the DTCPool members of the LBDN, each with a per-pool match
priority `ratio`) and `authZones` (the ZoneAuth zones the LBDN's patterns
are matched against, identified by each zone's WAPI `_ref`) are optional
and reference DTCPool/ZoneAuth resources by external name; these examples
omit both so they run standalone without requiring pre-existing DTCPool or
ZoneAuth resources. `name` is mutable. All other fields (`types`,
`priority`, `persistence`, `topology`, `ttl`, `useTtl`, `comment`,
`disable`, `extattrs`, `pools`, `authZones`) are mutable in place via WAPI
PUT — no immutable fields are known for this resource.

Apply the full set of example manifests:

```bash
kubectl apply -f examples/dtc-lbdn/dtc-lbdn.yaml
kubectl apply -f examples/dtc-lbdn/dtc-lbdn-namespaced.yaml
```

## Upgrade Notes

### Identity extensible attribute adoption

Every managed resource covered by the [identity extensible attribute
definition](#identity-extensible-attribute-definition) stamps a
`Crossplane Internal ID` value onto its Grid object at create time. Objects
created by a provider build that predates this mechanism — or any object the
provider has adopted via `crossplane.io/external-name` without having created
it itself — carry no such stamp yet.

No action is required: the next time the provider reconciles such an object
and can still resolve it through its stored reference, it treats the missing
attribute as an adoption case and writes the stamp, bringing the object under
the same protection as one created by this build. The visible side effect is
that a `Crossplane Internal ID` extensible attribute will start appearing
against your existing managed objects in Grid Manager as this happens.

### TTL field type unification (ARecord, AAAARecord, CNAMERecord, MXRecord, SRVRecord)

The `ttl` field on `ARecord`, `AAAARecord`, `CNAMERecord`, `MXRecord`, and
`SRVRecord` (both cluster-scoped and namespace-scoped, `spec.forProvider.ttl`
and `status.atProvider.ttl`) changed its CRD schema format from `int64` to
`int32`. This aligns `ttl` with every other DNS record type in the provider,
all of which already used the 32-bit form.

The `minimum: 0` / `maximum: 2147483647` bounds on `ttl` were introduced in
the same change as this type conversion. If you are upgrading from a build
published before 2026-07-30, your existing `ttl` values were never
range-checked and the field had no lower bound at all — a negative or
otherwise out-of-range value could have been persisted, and such a resource
will fail CRD validation once you apply the new schema. Check your existing
resources before upgrading:

```bash
for res in arecords.recorda aaaarecords.recordaaaa cnamerecords.recordcname mxrecords.recordmx srvrecords.recordsrv; do
  for grp in infobloxnios.crossplane.io infobloxnios.m.crossplane.io; do
    kubectl get "${res}.${grp}" -A -o json 2>/dev/null | jq -r \
      '.items[] | select(.spec.forProvider.ttl != null and (.spec.forProvider.ttl < 0 or .spec.forProvider.ttl > 2147483647)) |
       "\(.kind) \(.metadata.namespace // "-")/\(.metadata.name): ttl=\(.spec.forProvider.ttl)"'
  done
done
```

Any resource this reports must have its `ttl` corrected to the `0`-`2147483647`
range before you reapply the CRDs, or the update will be rejected by
admission.

**Action required:** reapply the CRDs for these five kinds after upgrading
the provider (`kubectl apply -f package/crds/`, or let your package manager
do it as part of the provider upgrade). Existing managed resources do not
need to be re-created.

### DNSView integer field type unification

`DNSView` (both cluster-scoped and namespace-scoped) modeled seventeen
numeric fields as 64-bit integers even though the backing API field is
32-bit unsigned. Those fields now use the same `int32` CRD schema format
already used everywhere else in the provider:

- `spec.forProvider`/`status.atProvider`: `blacklistRedirectTtl`, `lameTtl`,
  `maxCacheTtl`, `maxNcacheTtl`, `nxdomainRedirectTtl`, `notifyDelay`,
  `rpzDropIpRuleMinPrefixLengthIpv4`, `rpzDropIpRuleMinPrefixLengthIpv6`
- `responseRateLimiting`: `responsesPerSecond`, `window`, `slip`
- `scavengingSettings.scavengingSchedule`: `every`, `minutesPastHour`,
  `hourOfDay`, `year`, `month`, `dayOfMonth`

The five `*Ttl` fields keep their existing `minimum: 0` / `maximum:
2147483647` bounds. `scavengingSchedule.recurringTime` is unchanged — it
carries a Unix epoch timestamp, not a plain counter, and stays `int64`.

No existing value is out of range for any of the converted fields — the
backing API has always rejected out-of-range input for these fields, this
is a schema-shape change only, not a behavior change.

**Action required:** reapply the `DNSView` CRDs after upgrading the
provider (`kubectl apply -f package/crds/`, or let your package manager do
it as part of the provider upgrade). Existing managed resources do not
need to be re-created.

### SRVRecord integer field type unification

`SRVRecord` (both cluster-scoped and namespace-scoped) modeled five
numeric fields as 64-bit integers even though the backing API field is
32-bit unsigned. Those fields now use the same `int32` CRD schema format
already used everywhere else in the provider:

- `spec.forProvider`/`status.atProvider`: `priority`, `weight`, `port`
- `status.atProvider.awsRte53RecordInfo`: `weight`
- `status.atProvider.msAdUserData`: `activeUsersCount`

`status.atProvider.creationTime` and `status.atProvider.lastQueried` are
unchanged — they carry Unix epoch timestamps, not plain counters, and stay
`int64`.

No existing value is out of range for any of the converted fields — the
backing API has always rejected out-of-range input for these fields, this
is a schema-shape change only, not a behavior change.

**Action required:** reapply the `SRVRecord` CRDs after upgrading the
provider (`kubectl apply -f package/crds/`, or let your package manager do
it as part of the provider upgrade). Existing managed resources do not
need to be re-created.

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
