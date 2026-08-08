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

**Shape:** the credential bridge (`internal/controller/config`) is the single
place this is wired — `config.Conn` grows `DualClient`, `ReadConnector`,
`Gate`, `ConvergenceMode` and `ConvergenceTimeout` fields, alongside the
existing `Connector`/`Endpoint`. A resource package that ignores these new
fields (leaves them at their zero value) gets byte-for-byte its pre-split
behavior — every one of them is nil/empty exactly when the ProviderConfig has
no `readEndpoint` configured. `config.Conn` was already a struct instead of a
bare connector for exactly this reason: adding the read-endpoint routing and a
convergence gate costs zero call-site signature changes.

**IPAM variant (verbatim):** IPAM resources have no serial signal, so their
Observe must always call `convergence.EffectiveMode(mode, true)` — passing
`isIPAM: true` unconditionally, regardless of the configured mode — before
calling `Gate.Evaluate`. This forces `primaryOnly` and never issues a
candidate call. DNS resources pass `isIPAM: false`.

**Hazard — Update() must persist explicitly:** crossplane-runtime's managed
reconciler persists metadata (including annotations) written inside `Create()`
automatically, via `UpdateCriticalAnnotations`. It does **not** do the same for
`Update()` — the only metadata flush after `Update()` succeeds is triggered by
`ResourceLateInitialized` from a *later* `Observe`, not by anything `Update()`
itself returns. A convergence-gate annotation write made only in memory inside
`Update()` is therefore silently discarded at the end of that reconcile,
wedging the gate for that write until an unrelated change happens to trigger a
late-init persist. Consequences for every rollout package:

- Inside `Create()`, call `gate.RecordWrite(ctx, cr, fqdn, view)` — the
  in-memory-only form. Its annotation write rides along with
  `UpdateCriticalAnnotations`, which the reconciler always calls after
  `Create()`.
- Inside `Update()`, call `gate.RecordAndPersistWrite(ctx, kube, cr, fqdn,
  view)` instead — it does the same read-and-set, then immediately persists
  via a JSON merge patch scoped to `metadata.annotations`, wrapped in
  `retry.RetryOnConflict` (`convergence.PersistPendingSerial`). This mirrors
  `internal/controller/externalname.Refresh`'s pattern for the same reason:
  a merge patch carries no whole-object version precondition, so a concurrent
  writer touching an unrelated field never turns this into a lost update.
- Inside `Observe()`, `Gate.Evaluate` may **clear** the annotation (once the
  candidate catches up, or on a convergence timeout) by mutating `cr` in
  place. Fold that into `ExternalObservation.ResourceLateInitialized` — set it
  `true` whenever the pending-zone-serial annotation's value changed across
  the `Evaluate` call, OR'd with whatever `lateInit` value the package already
  computes. Without this, the clear is silently discarded exactly like the
  Update() case, and the resource stays gated on a write that already
  converged.
- `Delete()` never calls either RecordWrite variant — the object is being
  removed, so there is no future Observe left to gate on the resulting
  zone-serial bump.

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

	decision, annotationChanged := evaluateReadRouting(ctx, e.gate, e.convergenceMode, e.convergenceTimeout, cr, p.Name, p.View)
	readFrom := e.conn
	if e.gate != nil {
		cr.SetConditions(convergence.ReadRoutingCondition(decision))
		emitReadRoutingWarning(e.recorder, cr, decision) // no-op unless decision.Warning
		if decision.UseCandidate {
			readFrom = e.readConn
		}
	}

	res, err := observeARecord(ctx, readFrom, e.prober, e.endpoint, cr.GetName(), meta.GetExternalName(cr), string(cr.GetUID()), &p.Comment, &p.TTL, &p.UseTTL, &p.ExtAttrs)
	...
	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        upToDate,
		ResourceLateInitialized: res.lateInit || annotationChanged,
	}, nil
}
```

`evaluateReadRouting` is a small per-package helper: nil-safe on `gate`
(returns the zero `RouteDecision` and `annotationChanged: false` immediately
when no readEndpoint is configured — no candidate call, no annotation touch),
derives the zone from the record's own name field
(`convergence.ZoneFQDNFromRecordName`), and diffs the pending-zone-serial
annotation's value before/after `Gate.Evaluate` to compute `annotationChanged`.

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
	if err := recordConvergenceWrite(ctx, e.kube, e.gate, cr, p.Name, p.View); err != nil {
		return managed.ExternalUpdate{}, err
	}
	return managed.ExternalUpdate{}, nil
}
```

`e.objMgr`/`e.conn` are unconditionally the primary — Create/Update/Delete
never read `e.gate` to decide which connector to use, only Observe does.
`recordConvergenceWrite` is nil-safe on `gate` and calls
`gate.RecordAndPersistWrite` (Update) — see the hazard above for why Create
uses the plain `RecordWrite` instead.

**(iii) External-struct field additions:**

```go
type external struct {
	kube     k8sclient.Client
	objMgr   ibclient.IBObjectManager // primary — Writer(), never the candidate
	conn     ibclient.IBConnector     // primary
	prober   *identity.Prober
	endpoint string

	// Added for the read/write split — all zero values when no
	// readEndpoint is configured, which is byte-for-byte this package's
	// pre-split behavior.
	readConn           ibclient.IBConnector // candidate; nil ⇒ never read
	gate               *convergence.Gate    // nil ⇒ evaluateReadRouting/recordConvergenceWrite are no-ops
	convergenceMode    string
	convergenceTimeout time.Duration
	recorder           event.Recorder // Warning events on fallback transitions; nil-safe
}
```

**(iv) Connect() additions, for all three ProviderConfig kinds:**

The credential bridge already unifies cluster/namespaced/cluster-namespaced
resolution into `config.GetLegacy`/`config.Get`/`config.GetCluster`, each
returning a `*config.Conn`. Every package's `Connect()` — regardless of which
of the three it calls — adds the same handful of fields to its external
struct's constructor, unconditionally (nil-safety lives in `config.Conn`, not
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

	readConn:           conn.ReadConnector,
	gate:               conn.Gate,
	convergenceMode:    conn.ConvergenceMode,
	convergenceTimeout: conn.ConvergenceTimeout,
	recorder:           c.recorder, // wired into setup*() via event.NewAPIRecorder, shared with managed.WithRecorder
}, nil
```

`conn.DualClient` is never nil (a passthrough `dualclient.Client` wraps just
the primary when no readEndpoint is configured — see `dualclient.New`), so
`conn.DualClient.Writer()` is always safe to call and always resolves to the
primary object manager.

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
