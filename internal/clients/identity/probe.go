// Probe implements the install-prerequisite check for the identity EA
// definition: the "Crossplane Internal ID" extensible attribute
// definition must exist on a Grid before any resource that relies on
// the UID-in-EA identity ladder (see Resolve) can be managed safely.
//
// The check is deliberately scoped and self-clearing rather than a
// provider-wide hard failure:
//
//   - Scoped: a missing definition refuses only the resources that need
//     the guarantee. Provider startup, Connect for unaffected resources,
//     and the ExtensibleAttributeDef controller itself are unaffected.
//   - Self-clearing: once an administrator (or the provider itself, if
//     the credential permits) creates the definition, the next probe
//     after the cache TTL expires converges without a restart. A
//     negative verdict is never cached permanently.
//   - Cheap: Prober caches its verdict per endpoint for DefaultProbeTTL,
//     so a burst of reconciles against the same Grid costs at most one
//     WAPI round-trip per TTL window, not one per reconcile.
//   - Idempotent under races: the provider's own attempt to create the
//     definition may lose a race against a concurrent creator (another
//     replica, or an admin). WAPI reporting the resulting conflict is
//     itself proof the definition now exists, so it is treated as
//     success, never as a refusal.
package identity

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

// DefaultProbeTTL bounds how long a Prober verdict — positive or
// negative — is trusted before the next Ensure call re-checks the Grid.
// A negative verdict must never be cached permanently, and this is the
// documented upper bound on how stale one can be.
const DefaultProbeTTL = 5 * time.Minute

// eaDefType and eaDefFlags are the exact type/flags the provider uses
// when it creates the identity EA definition itself. "R" makes the
// attribute read-only to the Grid Manager GUI and the standard API, so
// an admin cannot casually break it, while "C" keeps it writable through
// the cloud API path the provider uses.
const (
	eaDefType  = "STRING"
	eaDefFlags = "CR"
)

// wapiVersionForRemediation is the WAPI version quoted in
// PrerequisiteError's remediation text — the exact command an operator
// with superuser access should run. It intentionally matches the
// documented remediation regardless of which WAPI version this build
// targets, since the definition object itself is version-independent.
const wapiVersionForRemediation = "2.12"

const (
	errEmptyEndpoint         = "identity: probe endpoint key is empty or blank — Ensure requires a non-empty, per-Grid/credential cache key"
	errCheckDefinitionExists = "identity: cannot check whether the identity extensible attribute definition exists"
	errCreateDefinition      = "identity: cannot create the identity extensible attribute definition"
)

// PrerequisiteError is returned by Prober.Ensure when the identity
// extensible attribute definition is absent from a Grid and the
// configured credential cannot create one. Its Error() text is the
// operator-facing remediation verbatim, so a controller can surface it
// directly in a Synced=False condition. Match with errors.As, e.g.:
//
//	var prereq *identity.PrerequisiteError
//	if errors.As(err, &prereq) { ... }
type PrerequisiteError struct {
	// Endpoint identifies the Grid this refusal applies to (typically
	// its host), for the remediation message.
	Endpoint string
}

func (e *PrerequisiteError) Error() string {
	return fmt.Sprintf(
		"Grid %s has no %q extensible attribute\n"+
			"definition, and the configured credential cannot create one. Resources whose\n"+
			"_ref rotates on update cannot be managed safely without it. A Grid superuser\n"+
			"should run:\n"+
			"  POST /wapi/v%s/extensibleattributedef\n"+
			"  {\"name\":%q,\"type\":\"STRING\",\"flags\":\"CR\"}",
		e.Endpoint, EAKey, wapiVersionForRemediation, EAKey,
	)
}

// probeCacheEntry is one Prober cache row: the verdict from the last
// probe of a given endpoint, and when it expires.
type probeCacheEntry struct {
	err       error
	expiresAt time.Time
}

// Prober caches the identity EA-definition prerequisite verdict per
// Grid endpoint/credential, so repeated Observe/Create calls across many
// resources and many reconciles cost at most one WAPI round-trip per
// TTL window per endpoint. Two ProviderConfigs pointing at different
// Grids never share a verdict — each caller-supplied endpoint key gets
// its own cache row.
//
// The zero value is not usable; construct with NewProber. Safe for
// concurrent use — a burst of concurrent Ensure calls for the same
// endpoint collapses to at most one probe (and, if needed, at most one
// create attempt).
type Prober struct {
	ttl   time.Duration
	clock func() time.Time

	mu       sync.Mutex
	cache    map[string]probeCacheEntry
	keyLocks map[string]*sync.Mutex
}

// NewProber constructs a Prober with the default TTL and wall-clock
// time source. Production code should generally share one Prober across
// every controller — see DefaultProber — rather than constructing its
// own, so the TTL window is shared across resource types reconciling
// against the same Grid.
func NewProber() *Prober {
	return &Prober{
		ttl:      DefaultProbeTTL,
		clock:    time.Now,
		cache:    make(map[string]probeCacheEntry),
		keyLocks: make(map[string]*sync.Mutex),
	}
}

// DefaultProber is the process-wide Prober every controller should use.
// Sharing one instance is what makes the per-TTL-window round-trip
// budget apply across the whole provider rather than per resource type.
var DefaultProber = NewProber()

// Ensure reports whether the identity extensible attribute definition is
// usable on the Grid reachable through conn, probing and — when
// permitted — creating it if necessary.
//
// endpoint is the caller's cache key for "this Grid, reached with this
// credential" (for example, the ProviderConfig's host plus a stable
// fingerprint of its credential). It must be non-empty and must not be
// shared between two ProviderConfigs that point at different Grids or
// use different credentials — Ensure trusts the caller's key completely
// and does not attempt to derive or validate it itself.
//
// A nil return means the definition is present (or was just created);
// callers may proceed to stamp/resolve identity as normal. A non-nil
// return is either a *PrerequisiteError (the definition is absent and
// cannot be created — refuse to manage the affected resource, make no
// mutating call, and surface Error() verbatim) or a wrapped transient
// error (the probe itself failed — treat as retriable, the same as any
// other WAPI error).
func (p *Prober) Ensure(ctx context.Context, conn ibclient.IBConnector, endpoint string) error {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return errors.New(errEmptyEndpoint)
	}

	if err, ok := p.lookup(endpoint); ok {
		return err
	}

	// Serialize probes per endpoint so a burst of concurrent Ensure
	// calls for the same Grid collapses to one probe (and, on the
	// absent-and-creatable path, one create attempt) instead of a
	// thundering herd that would each try to create the definition.
	lock := p.lockFor(endpoint)
	lock.Lock()
	defer lock.Unlock()

	// Re-check: another goroutine may have completed the probe and
	// populated the cache while this one waited for the lock.
	if err, ok := p.lookup(endpoint); ok {
		return err
	}

	err := p.probe(ctx, conn, endpoint)
	p.store(endpoint, err)
	return err
}

// probe issues the actual WAPI round-trip(s): check for the definition,
// and if absent, attempt to create it. Never called with the cache
// already holding a live verdict for endpoint.
func (p *Prober) probe(ctx context.Context, conn ibclient.IBConnector, endpoint string) error {
	log := ctrllog.FromContext(ctx)

	exists, err := definitionExists(conn)
	if err != nil {
		return errors.Wrap(err, errCheckDefinitionExists)
	}
	if exists {
		return nil
	}

	createErr := createDefinition(conn)
	switch {
	case createErr == nil:
		log.Info("identity: created the identity extensible attribute definition",
			"endpoint", endpoint, "name", EAKey)
		recordProbeCreated(endpoint)
		return nil
	case isAlreadyExists(createErr):
		// Another creator (a concurrent replica, or an admin) won the
		// race between our existence check and our create call. WAPI
		// reporting the resulting conflict is itself proof the
		// definition now exists.
		log.V(1).Info("identity: lost a race to create the identity extensible attribute definition; treating as success",
			"endpoint", endpoint, "name", EAKey)
		recordProbeRaceWon(endpoint)
		return nil
	case isForbidden(createErr):
		log.Info("identity: refusing to manage resources that require the identity extensible attribute definition — it is absent and the configured credential cannot create it",
			"endpoint", endpoint, "name", EAKey)
		recordProbeRefused(endpoint)
		return &PrerequisiteError{Endpoint: endpoint}
	default:
		return errors.Wrap(createErr, errCreateDefinition)
	}
}

// lookup returns the cached verdict for endpoint if one exists and has
// not expired.
func (p *Prober) lookup(endpoint string) (error, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	entry, ok := p.cache[endpoint]
	if !ok {
		return nil, false
	}
	if !p.now().Before(entry.expiresAt) {
		return nil, false
	}
	return entry.err, true
}

// store caches verdict for endpoint, valid until the Prober's TTL
// elapses. Caching applies uniformly to positive and negative verdicts —
// a negative one simply expires and is re-probed, which is what makes it
// bounded rather than permanent.
func (p *Prober) store(endpoint string, verdict error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.cache[endpoint] = probeCacheEntry{err: verdict, expiresAt: p.now().Add(p.ttl)}
}

// lockFor returns the per-endpoint mutex used to serialize probes,
// creating it on first use.
func (p *Prober) lockFor(endpoint string) *sync.Mutex {
	p.mu.Lock()
	defer p.mu.Unlock()

	l, ok := p.keyLocks[endpoint]
	if !ok {
		l = &sync.Mutex{}
		p.keyLocks[endpoint] = l
	}
	return l
}

func (p *Prober) now() time.Time {
	return p.clock()
}

// definitionExists reports whether the identity extensible attribute
// definition is present on the Grid conn talks to. A definition name is
// unique Grid-wide, so a name search resolves it unambiguously — unlike
// every other identity operation in this package, no UID or ambiguity
// handling is needed here.
func definitionExists(conn ibclient.IBConnector) (bool, error) {
	name := EAKey
	var res []ibclient.EADefinition
	qp := ibclient.NewQueryParams(false, map[string]string{"name": name})
	err := conn.GetObject(ibclient.NewEADefinition(ibclient.EADefinition{Name: &name}), "", qp, &res)
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return len(res) > 0, nil
}

// createDefinition issues the WAPI create call for the identity
// extensible attribute definition with the exact type/flags the
// provider's own remediation instructs an operator to use.
func createDefinition(conn ibclient.IBConnector) error {
	name := EAKey
	typ := eaDefType
	flags := eaDefFlags
	obj := ibclient.NewEADefinition(ibclient.EADefinition{
		Name:  &name,
		Type:  typ,
		Flags: &flags,
	})
	_, err := conn.CreateObject(obj)
	return err
}

// isForbidden reports whether err indicates the WAPI credential lacks
// permission to create the definition (HTTP 401 or 403).
func isForbidden(err error) bool {
	if err == nil {
		return false
	}
	m := errStatusRe.FindStringSubmatch(err.Error())
	if len(m) != 2 {
		return false
	}
	code, convErr := strconv.Atoi(m[1])
	if convErr != nil {
		return false
	}
	return code == http.StatusUnauthorized || code == http.StatusForbidden
}

// isAlreadyExists reports whether err indicates a concurrent creator won
// the race to create the definition. WAPI does not expose a typed
// conflict error through this SDK — every non-not-found HTTP error comes
// back as an opaquely formatted string — so this matches on the
// message text WAPI uses for a duplicate-name conflict, the same
// best-effort approach the rest of this package's error classification
// already takes for status codes.
func isAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "already exist")
}
