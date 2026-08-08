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
  host: "<NIOS_HOST>"
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
`username`/`password`.

`internal/controller/config` is the single place a controller resolves a
ProviderConfig into an authenticated WAPI connection. It exports:

- `type Conn struct { Connector *ibclient.Connector; Endpoint string }` — a
  struct, not a bare connector, so a later field addition (read-endpoint
  routing, a convergence gate) never changes a call-site signature.
- `func GetLegacy(ctx, kube, pc *clusterv1alpha1.ProviderConfig) (*Conn, error)`
  — legacy cluster-scoped ProviderConfig, referenced directly by name.
- `func Get(ctx, kube, pc *namespacedv1alpha1.ProviderConfig) (*Conn, error)`
  — namespaced ProviderConfig; a `secretRef` with no namespace falls back to
  the ProviderConfig's own namespace.
- `func GetCluster(ctx, kube, cpc *namespacedv1alpha1.ClusterProviderConfig) (*Conn, error)`
  — namespaced ClusterProviderConfig; no namespace fallback, since a
  cluster-scoped config has none of its own.
- `func BuildConnector(creds dualclient.Credentials, sslVerify bool, scheme, port string) (*ibclient.Connector, error)`
  — the test seam. Unit tests call this directly with `scheme: "http"` and an
  `httptest.Server`'s port instead of a real HTTPS Grid Manager.

Each resolver extracts credentials via `internal/clients/dualclient.ExtractCredentials`,
applies the `SSLVerify` nil-default (unset → `true`, secure) exactly once, and
returns a fully authenticated `*Conn`. Both live inside `internal/controller/config`
and appear nowhere in `internal/controller/<pkg>`.

A controller's whole credential path is three statements:

```go
pc := &apisv1alpha1.ProviderConfig{}
if err := c.kube.Get(ctx, types.NamespacedName{Name: ref.Name}, pc); err != nil {
    return nil, errors.Wrap(err, errGetPC)
}
conn, err := config.GetLegacy(ctx, c.kube, pc)
if err != nil {
    return nil, err
}
```

(`config.Get`/`config.GetCluster` for the namespaced Kind-switch arms.)

What a package does with `conn.Connector` depends on which of four
constructor-family shapes it had before the bridge:

| Family | Old private constructor | Returns | Transformation |
|---|---|---|---|
| A/B | `newObjectManager`/`newClient` + `…WithScheme` | `identity.ManagerAndConnector` | delete both; call `identity.NewManagerAndConnector(conn.Connector)` at the call site |
| C | `newConnector` + `…WithScheme` | `ibclient.IBConnector` | delete both; use `conn.Connector` directly |
| D | `newClients`/`newHostRecordClient` + `…WithScheme` | a package-specific wrapper struct | keep the wrapper constructor, but build it from `conn.Connector` — delete only the credential-resolution half |

Migrating a package off its own private `nioCredentials`/`extractCredentials`/
`newObjectManager*` plumbing onto this bridge is mechanical:

**1. `internal/controller/<pkg>/controller.go`**
- Delete `type nioCredentials struct { Host, Username, Password string }`,
  `func extractCredentials(...)`, and the matching `new*`/`new*WithScheme`
  constructor pair for whichever family (A–D) the package used.
- Delete the error consts that become unused: `errGetSecret`, `errNoSecretRef`,
  `errUnsupportedCreds`, `errMissingCredKey`, `errNewObjectManager` (or the
  package's equivalent). Unused consts do not fail `go build` in Go — grep
  each name in the package after deleting the function and remove the ones
  with no remaining reference.
- A `wapiVersion` const used only to build test request paths (e.g.
  `"/wapi/v"+wapiVersion+"/..."`) may stay — `config.BuildConnector` pins the
  same version internally, so the two never drift.
- Fix any doc comment claiming the endpoint is "validated non-empty by
  extractCredentials" — it is now resolved from `pc.Spec.Host`, which the CRD
  requires with `MinLength=1`.

**2. `cluster.go` — `Connect()`**
Replace the credential-extraction + `SSLVerify` nil-default + `new*(creds,
sslVerify)` block with `config.GetLegacy(ctx, c.kube, pc)`, then build
whatever this package's family needs from `conn.Connector` (see the table
above). `endpoint: conn.Endpoint` replaces `endpoint: creds.Host`.

**3. `namespaced.go` — `Connect()`**
Same replacement in both arms of the existing Kind switch — `config.Get(ctx,
c.kube, pc)` for `"ProviderConfig"`, `config.GetCluster(ctx, c.kube, cpc)` for
`"ClusterProviderConfig"`. Do not restructure the switch, the usage trackers,
or the `errUnsupportedKind` default — the dual-scope shape here is unrelated
to this migration.

**4. `*_test.go`**
- Secret fixtures: drop the `host` key from `Data`/`StringData`; carry only
  `username`/`password`.
- Every ProviderConfig / ClusterProviderConfig fixture: set `Spec.Host`.
- Any call to the package's own `new*WithScheme(creds, sslVerify, scheme,
  port)` test helper becomes `config.BuildConnector(creds, sslVerify, scheme,
  port)`, then `identity.NewManagerAndConnector(conn)` (family A/B) or the
  package's own wrapper (family D) built from the returned `*ibclient.Connector`.
- A test asserting `errMissingCredKey` when the Secret's `host` key is absent
  must be deleted, not rewritten. A missing host is now rejected by CRD schema
  validation, so there is no runtime path left to assert. Keep the
  missing-username / missing-password cases.
- A test documenting that the `ssl_verify` Secret key is ignored should be
  retargeted at the shared credential-extraction helper — it is still the
  right assertion, just in a new home.

## Read/write endpoint split wiring

`ProviderConfigSpec.ReadEndpoint`, `internal/clients/dualclient` and
`internal/clients/convergence` implement an opt-in split: Observe may read from
a read-only candidate endpoint (typically a Grid Master Candidate) instead of
the primary, gated by a SOA-serial convergence check so a just-written record
is never read back stale. Create/Update/Delete always go to the primary. See
the `readEndpoint` field on ProviderConfig for the user-facing shape.

The decision logic itself — candidate-vs-primary routing, the ReadRouting
condition, the Warning event on fallback, and the convergence write-recording
a write path must persist — lives in `internal/controller/readrouting`, a
shared package every resource package consumes. It is generic: nothing in it
is specific to ARecord or to any other resource kind.

**Shape:** extending `config.Conn` (over extending `dualclient.Client`) is
where the read-side connector and convergence gate get built: `config.Conn`
grows a `Router` field (`readrouting.Router`), built once in `config.resolve`
via `dualclient.Connect` + `convergence.NewGate`. `config.Conn` was already a
struct instead of a bare connector for exactly this reason: adding the
read-endpoint routing and a convergence gate costs zero call-site signature
changes. A resource package that ignores `conn.Router` (leaves it at its zero
value) gets byte-for-byte its pre-split behavior — `Router{}` is exactly the
"no readEndpoint configured" case, since `Router.Gate == nil` makes every
`Router` method a no-op.

**Signal-less variant (verbatim):** the routing predicate is named for the
*signal*, not for IPAM — `router.BeginObserve`'s last parameter is
`hasZoneSerial bool`, and IPAM is only one instance of a resource with no zone
SOA-serial signal to gate on. DTC pools/LBDNs/servers, DNS views, and
extensible-attribute definitions are equally signal-less. Any signal-less
resource's Observe must always call `router.BeginObserve(..., hasZoneSerial:
false)` — unconditionally, regardless of the configured mode. This forces
`primaryOnly` inside `Router` (via `convergence.EffectiveMode(mode,
!hasZoneSerial)`) and never issues a candidate call. A DNS record or zone
resource with a real SOA serial passes `hasZoneSerial: true`.

**Classification table** (filled in as each rollout ticket lands; a resource
absent from this table is unreviewed, not merely undocumented):

| Resource class | Packages | Connector field | `hasZoneSerial` | Gate key (fqdn, view) |
|---|---|---|---|---|
| DNS record (base case) | `recorda` | `e.conn` | `true` | `convergence.ZoneFQDNFromRecordName(name)`, `view` |
| DNS record (7 more) | owned by IN-RWS-WIRE-A | — | `true` | derived from the record's own name field, same pattern as `recorda` |
| DNS zone (zoneauth) | done — wired | `e.conn` | `true` — a ZoneAuth zone carries its own soa_serial_number/member_soa_serials for its own fqdn | the zone's OWN fqdn/view field, never the record-derivation helper (that strips a label, which is correct for a record and wrong for a zone) |
| DNS zone (zonedelegated, zoneforward) | done — wired | `e.conn` | `false` — no `zone_auth` match for a zone the Grid does not serve authoritatively (ADR §7) | n/a |
| Non-standard client structs (dtcpool, dtclbdn, dtcserver, hostrecord) | owned by IN-RWS-WIRE-C | `e.clients.conn` / `e.client.conn` | `false` for dtc×3 (no serial signal); `true` for hostrecord (has one) | n/a for dtc×3; record-name-derived for hostrecord |
| IPAM (7 packages) | owned by IN-RWS-WIRE-D | — | `false` | n/a — IPAM has no zone_auth object at all |
| extensibleattributedef, dnsview | owned by IN-RWS-WIRE-E | — | `false` — neither is an object inside a DNS zone | n/a |
| recordns | owned by IN-RWS-WIRE-E | — | open question — an NS record's name IS the delegated zone, so the base-case derivation may name the wrong zone; IN-RWS-WIRE-E must answer explicitly rather than assume | record-name-derived only if the derivation is proven correct; otherwise primary-only |

**Hazard — Update() must persist explicitly:** crossplane-runtime's managed
reconciler persists metadata (including annotations) written inside `Create()`
automatically, via `UpdateCriticalAnnotations`. It does **not** do the same for
`Update()` — the only metadata flush after `Update()` succeeds is triggered by
`ResourceLateInitialized` from a *later* `Observe`, not by anything `Update()`
itself returns. A convergence-gate annotation write made only in memory inside
`Update()` is therefore silently discarded at the end of that reconcile,
wedging the gate for that write until an unrelated change happens to trigger a
late-init persist. Consequences for every rollout package:

- `router.RecordWrite(ctx, kube, cr, fqdn, view)` always persists immediately,
  via a JSON merge patch scoped to `metadata.annotations` wrapped in
  `retry.RetryOnConflict` (`convergence.Gate.RecordAndPersistWrite` /
  `PersistPendingSerial`) — the same call is correct from both `Create()` and
  `Update()`. `Update()` MUST use it, for the reason above; `Create()`'s own
  annotation write would ride along with `UpdateCriticalAnnotations` on its
  own, but calling the one persisting method from both places keeps a rollout
  package down to a single write-path call site instead of two, at the cost of
  one extra (idempotent) read+patch in `Create()`. This mirrors
  `internal/controller/externalname.Refresh`'s pattern for the same reason: a
  merge patch carries no whole-object version precondition, so a concurrent
  writer touching an unrelated field never turns this into a lost update.
- Inside `Observe()`, `router.BeginObserve` may **clear** the annotation (once
  the candidate catches up, or on a convergence timeout) by mutating `cr` in
  place, and reports this as its second return value. Fold that into
  `ExternalObservation.ResourceLateInitialized` — OR it with whatever
  `lateInit` value the package already computes. Without this, the clear is
  silently discarded exactly like the Update() case, and the resource stays
  gated on a write that already converged.
- `Delete()` never calls `router.RecordWrite` — the object is being removed,
  so there is no future Observe left to gate on the resulting zone-serial
  bump.

**(i) One Observe read path — before/after:**

```go
// before
func (e *external) Observe(ctx context.Context, cr *v1alpha1.ARecord) (managed.ExternalObservation, error) {
	p := &cr.Spec.ForProvider
	res, err := observeARecord(ctx, e.conn, e.prober, e.endpoint, cr.GetName(), meta.GetExternalName(cr), string(cr.GetUID()), &p.Comment, &p.TTL, &p.UseTTL, &p.ExtAttrs)
	...
}
```

```go
// after
func (e *external) Observe(ctx context.Context, cr *v1alpha1.ARecord) (managed.ExternalObservation, error) {
	p := &cr.Spec.ForProvider

	fqdn := convergence.ZoneFQDNFromRecordName(strOrEmpty(p.Name))
	readFrom, annotationChanged := e.router.BeginObserve(ctx, cr, e.conn, fqdn, strOrEmpty(p.View), false)

	res, err := observeARecord(ctx, readFrom, e.prober, e.endpoint, cr.GetName(), meta.GetExternalName(cr), string(cr.GetUID()), &p.Comment, &p.TTL, &p.UseTTL, &p.ExtAttrs)
	...
	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        upToDate,
		ResourceLateInitialized: res.lateInit || annotationChanged,
	}, nil
}
```

`router.BeginObserve` takes the caller's own primary connector (`e.conn`) as a
parameter rather than storing a copy of it — every resource package already
tracks its own primary for CRUD (and, in this provider, identity resolution),
so `Router` only needs to own what the read/write split actually introduces:
the candidate connector, the gate, and its settings. It sets the ReadRouting
condition and emits the Warning event on fallback internally; the caller never
touches `convergence.ReadRoutingCondition` or an event.Recorder directly. Zone
derivation (`convergence.ZoneFQDNFromRecordName` for a record whose zone is
implicit in its own name) stays in the resource package, since only it knows
its own resource shape — this is the one line of judgment a rollout package
writes.

**(ii) One write path (Update) — before/after:**

```go
// before
func (e *external) Update(ctx context.Context, cr *v1alpha1.ARecord) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	rec, err := updateARecord(e.objMgr, meta.GetExternalName(cr), p.Name, p.IPv4Addr, p.Comment, p.TTL, p.UseTTL, p.ExtAttrs, string(cr.GetUID()))
	...
	return managed.ExternalUpdate{}, nil
}
```

```go
// after
func (e *external) Update(ctx context.Context, cr *v1alpha1.ARecord) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	rec, err := updateARecord(e.objMgr, meta.GetExternalName(cr), p.Name, p.IPv4Addr, p.Comment, p.TTL, p.UseTTL, p.ExtAttrs, string(cr.GetUID()))
	...
	fqdn := convergence.ZoneFQDNFromRecordName(strOrEmpty(p.Name))
	if err := e.router.RecordWrite(ctx, e.kube, cr, fqdn, strOrEmpty(p.View)); err != nil {
		return managed.ExternalUpdate{}, err
	}
	return managed.ExternalUpdate{}, nil
}
```

`e.objMgr`/`e.conn` are unconditionally the primary — Create/Update/Delete
never consult `e.router` to decide which connector to use, only Observe does.
`Create()` calls `e.router.RecordWrite` the same way, right after its own
write succeeds.

**(iii) External-struct field addition — ONE field, not five:**

```go
type external struct {
	kube     k8sclient.Client
	objMgr   ibclient.IBObjectManager // primary — Writer(), never the candidate
	conn     ibclient.IBConnector     // primary
	prober   *identity.Prober
	endpoint string

	// router routes Observe reads between the primary and an (optional)
	// candidate read endpoint, gated by SOA-serial convergence, and wraps
	// the convergence gate's write-recording for Create/Update. Its zero
	// value (Gate == nil) is "always read from the primary".
	router readrouting.Router
}
```

**(iv) Connect() additions, for all three ProviderConfig kinds:**

The credential bridge already unifies cluster/namespaced/cluster-namespaced
resolution into `config.GetLegacy`/`config.Get`/`config.GetCluster`, each
returning a `*config.Conn`. Every package's `Connect()` — regardless of which
of the three it calls — adds one field to its external struct's constructor,
unconditionally (nil-safety lives in `config.Conn`/`readrouting.Router`, not
in the connector):

```go
conn, err := config.GetLegacy(ctx, c.kube, pc) // or config.Get / config.GetCluster
if err != nil {
	return nil, err
}
return &external{
	kube:     c.kube,
	objMgr:   conn.DualClient.Writer(), // always primary, even without a readEndpoint
	conn:     conn.Connector,
	endpoint: conn.Endpoint,

	// router is conn.Router with this controller's own recorder injected
	// — conn.Router carries none of its own, since Connect() has no
	// event.Recorder to give it (Setup builds one, shared with
	// managed.WithRecorder).
	router: readrouting.WithRecorder(conn.Router, c.recorder),
}, nil
```

`conn.DualClient` is never nil (a passthrough `dualclient.Client` wraps just
the primary when no readEndpoint is configured — see `dualclient.New`), so
`conn.DualClient.Writer()` is always safe to call and always resolves to the
primary object manager.

**What a rollout package writes:** one import (`readrouting`), one struct
field (`router readrouting.Router`), one `Connect()` line
(`router: readrouting.WithRecorder(conn.Router, c.recorder)`), a two-line
Observe call (zone derivation + `BeginObserve`), and a matching
three-to-four-line `RecordWrite` call in each of `Create()`/`Update()` —
roughly a dozen lines of code per scope, none of it read-routing judgment: the
only package-specific line is deriving `fqdn`/`view` from the resource's own
spec fields.

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
