# Migration Guide: Upjet provider-infoblox-nios → provider-infobloxnios (v2)

This guide covers migrating from the Upjet-based
[`crossplane-contrib/provider-infoblox-nios`](https://github.com/crossplane-contrib/provider-infoblox-nios)
(v0.3.0 and earlier) to the native Crossplane v2
[`provider-infobloxnios`](https://github.com/kaessert/provider-infoblox-nios).

> **Important:** The two providers manage the same Infoblox NIOS objects
> through the same WAPI, but they use completely different CRDs. This is
> **not** an in-place upgrade — you must re-create your managed resources
> against the new API types. Plan for a maintenance window or a
> blue-green cutover.

## 1. Provider Package

| | Old (Upjet) | New (v2) |
|---|---|---|
| **Package** | `xpkg.upbound.io/crossplane-contrib/provider-infoblox-nios:v0.3.0` | `ghcr.io/kaessert/provider-infobloxnios:latest` |
| **ProviderConfig API** | `infoblox-nios.crossplane.io/v1beta1` | `infobloxnios.crossplane.io/v1alpha1` |
| **Runtime** | Upjet / Terraform | Native Crossplane v2 (no Terraform dependency) |

```yaml
# Old
apiVersion: pkg.crossplane.io/v1
kind: Provider
metadata:
  name: crossplane-contrib-provider-infoblox-nios
spec:
  package: xpkg.upbound.io/crossplane-contrib/provider-infoblox-nios:v0.3.0

# New
apiVersion: pkg.crossplane.io/v1
kind: Provider
metadata:
  name: provider-infobloxnios
spec:
  package: ghcr.io/kaessert/provider-infobloxnios:latest
```

## 2. Credentials Secret

This is the most common migration issue. The secret format has changed
completely.

### Old format (Upjet)

A **single key** (`credentials`) containing a **JSON blob**. The
`secretRef.key` field selected which key to read, and the provider parsed it
as JSON:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: infoblox-creds
  namespace: crossplane-system
type: Opaque
stringData:
  credentials: |
    {
      "server": "10.0.0.1",
      "username": "admin",
      "password": "secret",
      "port": "443",
      "sslmode": "true",
      "connection_timeout": "60",
      "pool_connections": "10",
      "wapi_version": "2.12"
    }
```

### New format (v2)

**Three separate top-level keys** — `host`, `username`, `password`. No JSON
parsing. The `secretRef.key` field is required by the CRD schema but is
**ignored** by the credential bridge (it reads the three keys directly from
`Secret.data`).

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: infoblox-creds
  namespace: crossplane-system
type: Opaque
stringData:
  host: "10.0.0.1"
  username: "admin"
  password: "secret"
```

### Key name changes

| Old JSON key | New Secret key | Notes |
|---|---|---|
| `server` | `host` | **Renamed** |
| `username` | `username` | Same |
| `password` | `password` | Same |
| `sslmode` | *(removed)* | Moved to `spec.sslVerify` on ProviderConfig |
| `port` | *(removed)* | No longer configurable (WAPI default) |
| `connection_timeout` | *(removed)* | No longer configurable |
| `pool_connections` | *(removed)* | No longer configurable |
| `wapi_version` | *(removed)* | Configured per-controller, not per-credential |

### If you use External Secrets Operator

If your Secret is synced from an external store (e.g. AWS SSM, Vault),
update your `ExternalSecret` / `SecretStore` mapping to emit three discrete
keys instead of a single JSON blob. For example, with AWS SSM:

```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
spec:
  target:
    name: infoblox-creds
  data:
    - secretKey: host
      remoteRef:
        key: /infoblox/host
    - secretKey: username
      remoteRef:
        key: /infoblox/username
    - secretKey: password
      remoteRef:
        key: /infoblox/password
```

## 3. ProviderConfig

### Old (Upjet)

```yaml
apiVersion: infoblox-nios.crossplane.io/v1beta1
kind: ProviderConfig
metadata:
  name: default
spec:
  credentials:
    source: Secret
    secretRef:
      name: infoblox-creds
      namespace: crossplane-system
      key: credentials    # ← actually used to select the JSON key
```

### New (v2)

```yaml
apiVersion: infobloxnios.crossplane.io/v1alpha1
kind: ProviderConfig
metadata:
  name: default
spec:
  sslVerify: true          # ← was "sslmode" inside the JSON secret
  credentials:
    source: Secret
    secretRef:
      name: infoblox-creds
      namespace: crossplane-system
      key: password        # ← required by schema but ignored by the provider
```

**Key differences:**
- API group changed from `infoblox-nios.crossplane.io` to `infobloxnios.crossplane.io` (no hyphen)
- API version changed from `v1beta1` to `v1alpha1`
- `sslVerify` is now a first-class ProviderConfig field (default: `true`),
  no longer buried in the credentials JSON
- New optional `readEndpoint` field for read/write endpoint splitting (see
  `examples/provider/config-read-write-split.yaml`)

## 4. API Groups

All API groups changed from `*.infoblox-nios.crossplane.io` (with hyphen) to
`*.infobloxnios.crossplane.io` (no hyphen). The old provider grouped
resources by domain (dns, ip, ipv4, ipv6, network); the new provider uses
per-resource API groups.

| Old API Group | New API Group (cluster-scoped) | New API Group (namespace-scoped) |
|---|---|---|
| `dns.infoblox-nios.crossplane.io` | `recorda.infobloxnios.crossplane.io` | `recorda.infobloxnios.m.crossplane.io` |
| | `recordaaaa.infobloxnios.crossplane.io` | `recordaaaa.infobloxnios.m.crossplane.io` |
| | `recordcname.infobloxnios.crossplane.io` | `recordcname.infobloxnios.m.crossplane.io` |
| | `recordmx.infobloxnios.crossplane.io` | `recordmx.infobloxnios.m.crossplane.io` |
| | `recordptr.infobloxnios.crossplane.io` | `recordptr.infobloxnios.m.crossplane.io` |
| | `recordsrv.infobloxnios.crossplane.io` | `recordsrv.infobloxnios.m.crossplane.io` |
| | `recordtxt.infobloxnios.crossplane.io` | `recordtxt.infobloxnios.m.crossplane.io` |
| `network.infoblox-nios.crossplane.io` | `networkview.infobloxnios.crossplane.io` | `networkview.infobloxnios.m.crossplane.io` |
| `ipv4.infoblox-nios.crossplane.io` | `network.infobloxnios.crossplane.io` | `network.infobloxnios.m.crossplane.io` |
| | `networkcontainer.infobloxnios.crossplane.io` | `networkcontainer.infobloxnios.m.crossplane.io` |
| `ip.infoblox-nios.crossplane.io` | *(see note below)* | |
| `ipv6.infoblox-nios.crossplane.io` | *(unified into Network)* | |

> **Note:** The old `ip.infoblox-nios.crossplane.io` group contained
> `Allocation` and `Association` resources for IPAM next-available-IP
> workflows. The new provider does not have direct equivalents — IP
> allocation is handled through the Network and FixedAddress resources.
> IPv4 and IPv6 networks are unified under a single `Network` kind.

## 5. Managed Resource Kinds

The old Upjet provider used Terraform resource names as Kind names. The new
provider uses cleaner, NIOS-native names. All resources are available in
**both cluster-scoped and namespace-scoped** variants.

| Old Kind | Old API Group | New Kind (cluster) | New Kind (namespaced) |
|---|---|---|---|
| `ARecord` | `dns.infoblox-nios` | `ARecord` | `ARecord` |
| `AAAARecord` | `dns.infoblox-nios` | `AAAARecord` | `AAAARecord` |
| `CNameRecord` | `dns.infoblox-nios` | `CNAMERecord` | `CNAMERecord` |
| `PTRRecord` | `dns.infoblox-nios` | `PTRRecord` | `PTRRecord` |
| `MXRecord` | `dns.infoblox-nios` | `MXRecord` | `MXRecord` |
| `SRVRecord` | `dns.infoblox-nios` | `SRVRecord` | `SRVRecord` |
| `TXTRecord` | `dns.infoblox-nios` | `TXTRecord` | `TXTRecord` |
| `View` | `network.infoblox-nios` | `NetworkView` | `NetworkView` |
| `Network` | `ipv4.infoblox-nios` | `Network` | `Network` |
| `NetworkContainer` | `ipv4.infoblox-nios` | `NetworkContainer` | `NetworkContainer` |
| `Network` | `ipv6.infoblox-nios` | `Network` *(IPv6 via spec)* | `Network` |
| `NetworkContainer` | `ipv6.infoblox-nios` | `NetworkContainer` *(IPv6 via spec)* | `NetworkContainer` |
| `Allocation` | `ip.infoblox-nios` | *(no direct equivalent)* | |
| `Association` | `ip.infoblox-nios` | *(no direct equivalent)* | |
| *(not available)* | | `ZoneAuth` | `ZoneAuth` |
| *(not available)* | | `ZoneDelegated` | `ZoneDelegated` |
| *(not available)* | | `ZoneForward` | `ZoneForward` |
| *(not available)* | | `HostRecord` | `HostRecord` |
| *(not available)* | | `FixedAddress` | `FixedAddress` |
| *(not available)* | | `Range` | `Range` |
| *(not available)* | | `RangeTemplate` | `RangeTemplate` |
| *(not available)* | | `AliasRecord` | `AliasRecord` |
| *(not available)* | | `NSRecord` | `NSRecord` |
| *(not available)* | | `DNSView` | `DNSView` |
| *(not available)* | | `DTCServer` | `DTCServer` |
| *(not available)* | | `DTCPool` | `DTCPool` |
| *(not available)* | | `DTCLBDN` | `DTCLBDN` |
| *(not available)* | | `IPv4SharedNetwork` | `IPv4SharedNetwork` |
| *(not available)* | | `ExtensibleAttributeDef` | `ExtensibleAttributeDef` |

## 6. Dual-Scope Resources (New Feature)

The new provider introduces **namespace-scoped** variants of every managed
resource, alongside the traditional cluster-scoped versions:

- **Cluster-scoped**: `*.infobloxnios.crossplane.io` — references a
  cluster-scoped `ProviderConfig`
- **Namespace-scoped**: `*.infobloxnios.m.crossplane.io` — references either
  a namespace-scoped `ProviderConfig` or a cluster-scoped
  `ClusterProviderConfig`

The old Upjet provider only had cluster-scoped resources.

## 7. Spec Field Changes

Resource specs have been redesigned to match the NIOS WAPI object model more
closely instead of mirroring Terraform schema fields. Common patterns:

- Fields are nested under `spec.forProvider` (same as before)
- `spec.forProvider.dnsView` replaces `spec.forProvider.view` on DNS records
- CIDR notation is used directly (e.g. `cidr: "10.0.0.0/24"`) instead of
  separate `cidr`/`network_view` Terraform-style fields
- Extensible attributes use `spec.forProvider.extattrs` (a map) instead of
  `spec.forProvider.ext_attrs` (JSON string)

Consult the README and the `examples/` directory for the exact spec shape of
each resource.

## 8. Migration Procedure

### Option A: Blue-Green (Recommended)

1. **Install the new provider** alongside the old one — they use different
   CRD groups so they do not conflict.
2. **Create the new Secret** with the three-key format.
3. **Create a new ProviderConfig** for the v2 provider.
4. **Re-create managed resources** using the new API types, pointing at the
   same Infoblox objects. The new provider will adopt existing WAPI objects
   by matching on the `crossplane.io/external-name` annotation (the WAPI
   `_ref`).
5. **Delete old managed resources** with `deletionPolicy: Orphan` to avoid
   deleting the backing NIOS objects.
6. **Uninstall the old provider.**

### Option B: Cut-Over

1. **Set `deletionPolicy: Orphan`** on all old managed resources.
2. **Delete old managed resources** (NIOS objects are preserved).
3. **Uninstall the old provider** and its CRDs.
4. **Install the new provider**, create the new Secret and ProviderConfig.
5. **Create new managed resources** — the provider adopts existing WAPI
   objects via external-name.

### Adopting Existing NIOS Objects

To adopt an object that already exists in NIOS, set the
`crossplane.io/external-name` annotation to the object's WAPI `_ref`:

```yaml
apiVersion: recorda.infobloxnios.crossplane.io/v1alpha1
kind: ARecord
metadata:
  name: my-record
  annotations:
    crossplane.io/external-name: "record:a/ZG5zLmJpbmRfYSQ:10.0.0.1/my-record.example.com/default"
spec:
  forProvider:
    fqdn: "my-record.example.com"
    ipv4addr: "10.0.0.1"
  providerConfigRef:
    name: default
```

## 9. Cleanup

After migration is complete:

```bash
# Remove old provider
kubectl delete provider crossplane-contrib-provider-infoblox-nios

# Remove old CRDs (if not automatically cleaned up)
kubectl get crds -o name | grep 'infoblox-nios\.crossplane\.io' | xargs kubectl delete

# Remove old secret (optional — only if no longer needed)
kubectl delete secret -n crossplane-system <old-secret-name>
```
