package identity

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
)

// ── fake connector ──────────────────────────────────────────────────────
//
// Per the mocks-only testing convention this factory uses, this fake
// implements ibclient.IBConnector against *ibclient.EADefinition — the
// only object type the probe ever touches. It is safe for concurrent
// use so the race tests can drive it from multiple goroutines.

type fakeProbeConnector struct {
	mu sync.Mutex

	// existing, when non-empty, is returned by GetObject's name search —
	// simulating the definition already being present on the Grid.
	existing []ibclient.EADefinition
	// searchErr, when set, is returned by GetObject instead of a normal
	// search result (a transient/network failure probing existence).
	searchErr error
	// createErr, when set, is returned by CreateObject.
	createErr error

	searchCalls int32
	createCalls int32
}

func (f *fakeProbeConnector) CreateObject(_ ibclient.IBObject) (string, error) {
	atomic.AddInt32(&f.createCalls, 1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createErr != nil {
		return "", f.createErr
	}
	return "extensibleattributedef/abc123:Crossplane%20Internal%20ID", nil
}

func (f *fakeProbeConnector) DeleteObject(_ string) (string, error) { return "", nil }

func (f *fakeProbeConnector) UpdateObject(_ ibclient.IBObject, _ string) (string, error) {
	return "", nil
}

func (f *fakeProbeConnector) GetObject(_ ibclient.IBObject, _ string, _ *ibclient.QueryParams, res interface{}) error {
	atomic.AddInt32(&f.searchCalls, 1)
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.searchErr != nil {
		return f.searchErr
	}
	ptr, ok := res.(*[]ibclient.EADefinition)
	if !ok {
		return fmt.Errorf("fakeProbeConnector: unexpected res type %T", res)
	}
	*ptr = f.existing
	if len(f.existing) == 0 {
		return ibclient.NewNotFoundError("not found")
	}
	return nil
}

func (f *fakeProbeConnector) setExisting(defs []ibclient.EADefinition) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.existing = defs
}

func (f *fakeProbeConnector) numSearchCalls() int { return int(atomic.LoadInt32(&f.searchCalls)) }
func (f *fakeProbeConnector) numCreateCalls() int { return int(atomic.LoadInt32(&f.createCalls)) }

// wapiStatusErr builds an error in the same generically-wrapped shape
// the SDK's Connector returns for any non-not-found HTTP error, so the
// probe's status-code classification exercises the real parsing path.
func wapiStatusErr(status int, text string) error {
	return fmt.Errorf("WAPI request error: %d('%s')\n%s", status, text, text)
}

// wapiSuperuserRefusalErr builds the error in the exact shape a live NIOS
// Grid returns for a non-superuser attempt to create the identity
// extensible attribute definition — HTTP 400 (not 401/403) with an
// IBDataConflictError body. Captured verbatim from a real Grid using a
// restricted, non-superuser credential:
//
//	HTTP 400
//	{ "Error": "AdmConDataError: None (IBDataConflictError: IB.Data.Conflict:Cannot
//	   create extensible attribute definition 'Crossplane Prereq Probe'. Only
//	   superusers can manage extensible attribute definition)",
//	  "code": "Client.Ibap.Data.Conflict",
//	  "text": "Cannot create extensible attribute definition 'Crossplane Prereq
//	   Probe'. Only superusers can manage extensible attribute definition" }
func wapiSuperuserRefusalErr() error {
	body := `{ "Error": "AdmConDataError: None (IBDataConflictError: IB.Data.Conflict:Cannot create extensible attribute definition 'Crossplane Prereq Probe'. Only superusers can manage extensible attribute definition)", "code": "Client.Ibap.Data.Conflict", "text": "Cannot create extensible attribute definition 'Crossplane Prereq Probe'. Only superusers can manage extensible attribute definition" }`
	return fmt.Errorf("WAPI request error: 400('400 Bad Request')\nContents:\n%s\n", body)
}

// wapiGenericBadRequestErr builds an error in the real NIOS shape for an
// ordinary WAPI validation failure unrelated to the superuser-only
// EA-definition privilege — an AdmConProtoError on an unrelated field.
// This must never be classified as a *PrerequisiteError; it stays a
// retriable wrapped error like any other 400.
func wapiGenericBadRequestErr() error {
	body := `{ "Error": "AdmConProtoError: None (IBDataValueError: IB.Data.Value: 'flags' has an invalid value)", "code": "Client.Ibap.Proto", "text": "'flags' has an invalid value" }`
	return fmt.Errorf("WAPI request error: 400('400 Bad Request')\nContents:\n%s\n", body)
}

// newTestProber builds a Prober with a controllable clock, so tests can
// advance time deterministically instead of sleeping.
func newTestProber(ttl time.Duration) (*Prober, *fakeClock) {
	fc := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	p := NewProber()
	p.ttl = ttl
	p.clock = fc.now
	return p, fc
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// ── present ──────────────────────────────────────────────────────────────

func TestEnsureDefinitionPresent(t *testing.T) {
	p, _ := newTestProber(DefaultProbeTTL)
	conn := &fakeProbeConnector{existing: []ibclient.EADefinition{{}}}

	if err := p.Ensure(context.Background(), conn, "grid-a"); err != nil {
		t.Fatalf("Ensure returned error: %v", err)
	}
	if conn.numCreateCalls() != 0 {
		t.Fatalf("createCalls = %d, want 0 — a present definition must never be (re)created", conn.numCreateCalls())
	}
}

// ── absent + creatable ─────────────────────────────────────────────────────

func TestEnsureDefinitionAbsentAndCreatable(t *testing.T) {
	p, _ := newTestProber(DefaultProbeTTL)
	conn := &fakeProbeConnector{}

	if err := p.Ensure(context.Background(), conn, "grid-a"); err != nil {
		t.Fatalf("Ensure returned error: %v", err)
	}
	if conn.numCreateCalls() != 1 {
		t.Fatalf("createCalls = %d, want exactly 1", conn.numCreateCalls())
	}
}

// ── absent + forbidden ───────────────────────────────────────────────────
//
// NIOS answers a non-superuser attempt to create the identity EA
// definition with HTTP 400, not 401/403 — this is the shape a real Grid
// actually returns, and the primary fixture below reproduces it
// verbatim rather than a synthetic 403.

func TestEnsureDefinitionAbsentAndSuperuserOnly400Refuses(t *testing.T) {
	p, _ := newTestProber(DefaultProbeTTL)
	conn := &fakeProbeConnector{createErr: wapiSuperuserRefusalErr()}

	err := p.Ensure(context.Background(), conn, "nios.example.com")
	if err == nil {
		t.Fatal("Ensure returned nil error, want a refusal")
	}
	var prereq *PrerequisiteError
	if !errors.As(err, &prereq) {
		t.Fatalf("err = %v (%T), want *PrerequisiteError", err, err)
	}
	if prereq.Endpoint != "nios.example.com" {
		t.Fatalf("Endpoint = %q, want %q", prereq.Endpoint, "nios.example.com")
	}

	msg := err.Error()
	for _, want := range []string{
		`Grid nios.example.com has no "Crossplane Internal ID" extensible attribute`,
		"definition, and the configured credential cannot create one.",
		"POST /wapi/v2.12/extensibleattributedef",
		`{"name":"Crossplane Internal ID","type":"STRING","flags":"CR"}`,
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error message missing remediation fragment %q; got:\n%s", want, msg)
		}
	}
}

// A Grid that does answer this refusal with an ordinary 401/403 must
// still be classified as a refusal — the 400 handling above is
// additive, not a replacement for the pre-existing 401/403 path.

func TestEnsureDefinitionAbsentAndForbidden403AlsoRefuses(t *testing.T) {
	p, _ := newTestProber(DefaultProbeTTL)
	conn := &fakeProbeConnector{createErr: wapiStatusErr(403, "Forbidden")}

	err := p.Ensure(context.Background(), conn, "grid-a")
	var prereq *PrerequisiteError
	if !errors.As(err, &prereq) {
		t.Fatalf("err = %v (%T), want *PrerequisiteError", err, err)
	}
}

func TestEnsureDefinitionAbsentAndUnauthorizedAlsoRefuses(t *testing.T) {
	p, _ := newTestProber(DefaultProbeTTL)
	conn := &fakeProbeConnector{createErr: wapiStatusErr(401, "Unauthorized")}

	err := p.Ensure(context.Background(), conn, "grid-a")
	var prereq *PrerequisiteError
	if !errors.As(err, &prereq) {
		t.Fatalf("err = %v (%T), want *PrerequisiteError", err, err)
	}
}

// A generic HTTP 400 unrelated to the superuser-only privilege — e.g. an
// AdmConProtoError on an unrelated field — must not be swept up by the
// new 400 classification; it stays a retriable wrapped error.

func TestEnsureDefinitionAbsentAndGenericBadRequestStaysRetriable(t *testing.T) {
	p, _ := newTestProber(DefaultProbeTTL)
	conn := &fakeProbeConnector{createErr: wapiGenericBadRequestErr()}

	err := p.Ensure(context.Background(), conn, "grid-a")
	if err == nil {
		t.Fatal("Ensure returned nil, want the wrapped transient/validation error")
	}
	var prereq *PrerequisiteError
	if errors.As(err, &prereq) {
		t.Fatalf("a generic 400 unrelated to the superuser-only privilege must not be classified as a *PrerequisiteError: %v", err)
	}
}

// An "already exists" 400 (a lost create race) must still be treated as
// success, not swept up by the new 400 classification.

func TestEnsureDefinitionAbsentAndAlreadyExists400StaysSuccess(t *testing.T) {
	p, _ := newTestProber(DefaultProbeTTL)
	conn := &fakeProbeConnector{createErr: wapiStatusErr(400, "IB.Data.Conflict: Extensible attribute definition with this name already exists.")}

	err := p.Ensure(context.Background(), conn, "grid-a")
	if err != nil {
		t.Fatalf("Ensure returned error for an already-exists race: %v", err)
	}
}

// ── no mutating call once refused (cached) ────────────────────────────────

func TestEnsureRefusalMakesNoFurtherMutatingCallWhileCached(t *testing.T) {
	p, _ := newTestProber(DefaultProbeTTL)
	conn := &fakeProbeConnector{createErr: wapiStatusErr(403, "Forbidden")}

	var lastErr error
	for i := 0; i < 5; i++ {
		lastErr = p.Ensure(context.Background(), conn, "grid-a")
	}

	var prereq *PrerequisiteError
	if !errors.As(lastErr, &prereq) {
		t.Fatalf("err = %v, want *PrerequisiteError", lastErr)
	}
	if conn.numSearchCalls() != 1 || conn.numCreateCalls() != 1 {
		t.Fatalf("after 5 Ensure calls while cached: searchCalls=%d createCalls=%d, want 1/1 — a cached refusal must never re-issue the existence check or a create",
			conn.numSearchCalls(), conn.numCreateCalls())
	}
}

// ── racing create ──────────────────────────────────────────────────────

func TestEnsureConcurrentEnsureCallsIssueAtMostOneCreate(t *testing.T) {
	p, _ := newTestProber(DefaultProbeTTL)
	conn := &fakeProbeConnector{}

	const n = 20
	var wg sync.WaitGroup
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			errs[i] = p.Ensure(context.Background(), conn, "grid-a")
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("Ensure[%d] returned error: %v", i, err)
		}
	}
	if got := conn.numCreateCalls(); got != 1 {
		t.Fatalf("createCalls = %d, want exactly 1 — concurrent Ensure calls for the same endpoint must collapse to one create", got)
	}
}

func TestEnsureConcurrentRaceAlreadyExistsTreatedAsSuccess(t *testing.T) {
	p, _ := newTestProber(DefaultProbeTTL)
	conn := &fakeProbeConnector{createErr: wapiStatusErr(400, "IB.Data.Conflict: Extensible attribute definition with this name already exists.")}

	err := p.Ensure(context.Background(), conn, "grid-a")
	if err != nil {
		t.Fatalf("Ensure returned error for an already-exists race: %v", err)
	}
	if conn.numCreateCalls() != 1 {
		t.Fatalf("createCalls = %d, want exactly 1", conn.numCreateCalls())
	}
}

// ── self-clearing: refused → definition appears → converges without restart ──

func TestEnsureSelfClearsAfterTTLWithoutRestart(t *testing.T) {
	ttl := time.Minute
	p, clock := newTestProber(ttl)
	conn := &fakeProbeConnector{createErr: wapiStatusErr(403, "Forbidden")}

	err := p.Ensure(context.Background(), conn, "grid-a")
	var prereq *PrerequisiteError
	if !errors.As(err, &prereq) {
		t.Fatalf("first Ensure: err = %v, want *PrerequisiteError", err)
	}
	if conn.numSearchCalls() != 1 || conn.numCreateCalls() != 1 {
		t.Fatalf("after first Ensure: searchCalls=%d createCalls=%d, want 1/1", conn.numSearchCalls(), conn.numCreateCalls())
	}

	// An administrator creates the definition out-of-band. Before the
	// TTL elapses, Ensure must still return the cached (now stale)
	// refusal — the bounded staleness the ADR accepts.
	conn.setExisting([]ibclient.EADefinition{{}})
	clock.advance(ttl / 2)
	err = p.Ensure(context.Background(), conn, "grid-a")
	if !errors.As(err, &prereq) {
		t.Fatalf("Ensure before TTL elapsed: err = %v, want the cached *PrerequisiteError", err)
	}
	if conn.numSearchCalls() != 1 || conn.numCreateCalls() != 1 {
		t.Fatalf("Ensure before TTL elapsed issued extra WAPI calls: searchCalls=%d createCalls=%d, want 1/1", conn.numSearchCalls(), conn.numCreateCalls())
	}

	// Once the TTL elapses, the next Ensure call re-probes and converges
	// — no restart, no additional process state.
	clock.advance(ttl)
	if err := p.Ensure(context.Background(), conn, "grid-a"); err != nil {
		t.Fatalf("Ensure after TTL elapsed: err = %v, want nil (self-cleared)", err)
	}
	if conn.numSearchCalls() != 2 {
		t.Fatalf("searchCalls = %d, want exactly 2 (one before TTL, one after)", conn.numSearchCalls())
	}
	if conn.numCreateCalls() != 1 {
		t.Fatalf("createCalls = %d, want still exactly 1 — the definition is now present, so no create should be attempted", conn.numCreateCalls())
	}
}

// ── cost: at most one round-trip per TTL window per endpoint ─────────────

func TestEnsureCostsAtMostOneRoundTripPerTTLWindow(t *testing.T) {
	p, clock := newTestProber(time.Minute)
	conn := &fakeProbeConnector{existing: []ibclient.EADefinition{{}}}

	for i := 0; i < 10; i++ {
		if err := p.Ensure(context.Background(), conn, "grid-a"); err != nil {
			t.Fatalf("Ensure[%d] returned error: %v", i, err)
		}
	}
	if conn.numSearchCalls() != 1 {
		t.Fatalf("searchCalls = %d after 10 reconciles inside one TTL window, want exactly 1", conn.numSearchCalls())
	}

	clock.advance(time.Minute)
	if err := p.Ensure(context.Background(), conn, "grid-a"); err != nil {
		t.Fatalf("Ensure after TTL elapsed returned error: %v", err)
	}
	if conn.numSearchCalls() != 2 {
		t.Fatalf("searchCalls = %d after a second TTL window, want exactly 2", conn.numSearchCalls())
	}
}

// ── per-endpoint keying: two Grids never share a verdict ─────────────────

func TestEnsurePerEndpointVerdictsDoNotCrossContaminate(t *testing.T) {
	p, _ := newTestProber(DefaultProbeTTL)
	connA := &fakeProbeConnector{existing: []ibclient.EADefinition{{}}}
	connB := &fakeProbeConnector{createErr: wapiStatusErr(403, "Forbidden")}

	if err := p.Ensure(context.Background(), connA, "grid-a"); err != nil {
		t.Fatalf("grid-a: Ensure returned error: %v", err)
	}
	errB := p.Ensure(context.Background(), connB, "grid-b")
	var prereq *PrerequisiteError
	if !errors.As(errB, &prereq) {
		t.Fatalf("grid-b: err = %v, want *PrerequisiteError", errB)
	}
	if prereq.Endpoint != "grid-b" {
		t.Fatalf("Endpoint = %q, want %q", prereq.Endpoint, "grid-b")
	}

	// Re-querying grid-a must still succeed from cache, unaffected by
	// grid-b's refusal.
	if err := p.Ensure(context.Background(), connA, "grid-a"); err != nil {
		t.Fatalf("grid-a (second call): err = %v, want nil", err)
	}
	if connA.numSearchCalls() != 1 {
		t.Fatalf("grid-a searchCalls = %d, want 1 (cached)", connA.numSearchCalls())
	}
}

// ── transient / non-classified errors are never mistaken for a refusal ───

func TestEnsureExistenceCheckTransientErrorPropagates(t *testing.T) {
	p, _ := newTestProber(DefaultProbeTTL)
	conn := &fakeProbeConnector{searchErr: wapiStatusErr(500, "internal error")}

	err := p.Ensure(context.Background(), conn, "grid-a")
	if err == nil {
		t.Fatal("Ensure returned nil, want the wrapped transient error")
	}
	var prereq *PrerequisiteError
	if errors.As(err, &prereq) {
		t.Fatalf("a 500 on the existence check must not be classified as a *PrerequisiteError: %v", err)
	}
	if conn.numCreateCalls() != 0 {
		t.Fatalf("createCalls = %d, want 0 — a failed existence check must never fall through to create", conn.numCreateCalls())
	}
}

func TestEnsureCreateServerErrorPropagatesNotRefusal(t *testing.T) {
	p, _ := newTestProber(DefaultProbeTTL)
	conn := &fakeProbeConnector{createErr: wapiStatusErr(500, "internal error")}

	err := p.Ensure(context.Background(), conn, "grid-a")
	if err == nil {
		t.Fatal("Ensure returned nil, want the wrapped transient error")
	}
	var prereq *PrerequisiteError
	if errors.As(err, &prereq) {
		t.Fatalf("a 500 on create must not be classified as a *PrerequisiteError: %v", err)
	}
}

// ── input validation ───────────────────────────────────────────────────

func TestEnsureEmptyEndpointIsRejected(t *testing.T) {
	p, _ := newTestProber(DefaultProbeTTL)
	conn := &fakeProbeConnector{existing: []ibclient.EADefinition{{}}}

	if err := p.Ensure(context.Background(), conn, "   "); err == nil {
		t.Fatal("Ensure with a blank endpoint returned nil, want an error")
	}
	if conn.numSearchCalls() != 0 {
		t.Fatalf("searchCalls = %d, want 0 — an invalid endpoint must never reach the connector", conn.numSearchCalls())
	}
}

// ── DefaultProber is a distinct, usable instance ──────────────────────────

func TestDefaultProberIsUsable(t *testing.T) {
	conn := &fakeProbeConnector{existing: []ibclient.EADefinition{{}}}
	if err := DefaultProber.Ensure(context.Background(), conn, "default-prober-test-endpoint"); err != nil {
		t.Fatalf("DefaultProber.Ensure returned error: %v", err)
	}
}
