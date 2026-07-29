package convergence

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/dualclient"
)

// poisonServer fails the test if it receives any request — used to prove
// a code path returns without ever calling the candidate.
func poisonServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected HTTP request to candidate: %s %s", r.Method, r.URL.String())
	}))
}

func newObj(name string) *metav1.ObjectMeta {
	return &metav1.ObjectMeta{Name: name}
}

// ── EffectiveMode ────────────────────────────────────────────────────────

func TestEffectiveModeNonIPAMUnaffected(t *testing.T) {
	mode, overridden := EffectiveMode(ModeSOASerial, false)
	if mode != ModeSOASerial || overridden {
		t.Fatalf("EffectiveMode(soaSerial, false) = (%s, %v), want (soaSerial, false)", mode, overridden)
	}
}

func TestEffectiveModeIPAMOverridesSOASerial(t *testing.T) {
	mode, overridden := EffectiveMode(ModeSOASerial, true)
	if mode != ModePrimaryOnly || !overridden {
		t.Fatalf("EffectiveMode(soaSerial, true) = (%s, %v), want (primaryOnly, true)", mode, overridden)
	}
}

func TestEffectiveModeIPAMAlreadyPrimaryOnly(t *testing.T) {
	mode, overridden := EffectiveMode(ModePrimaryOnly, true)
	if mode != ModePrimaryOnly || overridden {
		t.Fatalf("EffectiveMode(primaryOnly, true) = (%s, %v), want (primaryOnly, false) — no override needed, already primaryOnly", mode, overridden)
	}
}

// ── ReadRoutingCondition ─────────────────────────────────────────────────

func TestReadRoutingConditionCandidate(t *testing.T) {
	cond := ReadRoutingCondition(RouteDecision{UseCandidate: true, Reason: ReasonCandidateReady, Message: "ok"})
	if cond.Type != ReadRoutingConditionType || cond.Status != corev1.ConditionTrue || string(cond.Reason) != ReasonCandidateReady {
		t.Fatalf("unexpected condition: %+v", cond)
	}
}

func TestReadRoutingConditionPrimary(t *testing.T) {
	cond := ReadRoutingCondition(RouteDecision{UseCandidate: false, Reason: ReasonWaitingForReplication, Message: "lagging"})
	if cond.Status != corev1.ConditionFalse || string(cond.Reason) != ReasonWaitingForReplication {
		t.Fatalf("unexpected condition: %+v", cond)
	}
}

// ── Gate.Evaluate: structural / configuration paths (no candidate call) ──

func TestGateEvaluatePrimaryOnlyModeNoCandidateCall(t *testing.T) {
	srv := poisonServer(t)
	defer srv.Close()
	g := NewGate(nil, newTestClient(t, srv), nil, "gmc.example.com")

	d := g.Evaluate(context.Background(), newObj("r1"), "example.com", "Internal", ModePrimaryOnly, DefaultTimeout)
	if d.UseCandidate || d.Reason != ReasonPrimaryOnly {
		t.Fatalf("unexpected decision: %+v", d)
	}
}

func TestGateEvaluateNoCandidateConfigured(t *testing.T) {
	g := NewGate(nil, nil, nil, "")
	d := g.Evaluate(context.Background(), newObj("r1"), "example.com", "Internal", ModeSOASerial, DefaultTimeout)
	if d.UseCandidate || d.Reason != ReasonPrimaryOnly {
		t.Fatalf("unexpected decision: %+v", d)
	}
}

func TestGateEvaluateCircuitBreakerOpenNoCandidateCall(t *testing.T) {
	srv := poisonServer(t)
	defer srv.Close()
	breaker := dualclient.NewCircuitBreaker(1, time.Minute)
	breaker.RecordFailure() // opens at threshold 1

	g := NewGate(nil, newTestClient(t, srv), breaker, "gmc.example.com")
	d := g.Evaluate(context.Background(), newObj("r1"), "example.com", "Internal", ModeSOASerial, DefaultTimeout)
	if d.UseCandidate || d.Reason != ReasonCandidateDegraded || !d.Warning {
		t.Fatalf("unexpected decision: %+v", d)
	}
}

// ── Gate.Evaluate: steady state (no pending write) ────────────────────────

func TestGateEvaluateNoPendingAnnotationRoutesToCandidate(t *testing.T) {
	srv := poisonServer(t) // steady state must not need a candidate call
	defer srv.Close()
	g := NewGate(nil, newTestClient(t, srv), nil, "gmc.example.com")

	d := g.Evaluate(context.Background(), newObj("r1"), "example.com", "Internal", ModeSOASerial, DefaultTimeout)
	if !d.UseCandidate || d.Reason != ReasonCandidateReady {
		t.Fatalf("unexpected decision: %+v", d)
	}
}

func TestGateEvaluateCorruptAnnotationFallsBackToPrimary(t *testing.T) {
	srv := poisonServer(t)
	defer srv.Close()
	g := NewGate(nil, newTestClient(t, srv), nil, "gmc.example.com")

	obj := newObj("r1")
	obj.SetAnnotations(map[string]string{PendingZoneSerialAnnotation: "{not json"})

	d := g.Evaluate(context.Background(), obj, "example.com", "Internal", ModeSOASerial, DefaultTimeout)
	if d.UseCandidate || d.Reason != ReasonWaitingForReplication || !d.Warning {
		t.Fatalf("unexpected decision: %+v", d)
	}
}

// ── Gate.RecordWrite ───────────────────────────────────────────────────────

func TestGateRecordWriteSetsAnnotation(t *testing.T) {
	m := &mockZoneAuthServer{respond: func(r *http.Request) (interface{}, int) {
		return []zoneAuthObject{{Ref: "zone_auth/xyz", SOASerialNumber: uint32Ptr(5)}}, http.StatusOK
	}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	g := NewGate(newTestClient(t, srv), nil, nil, "")
	obj := newObj("r1")

	if err := g.RecordWrite(context.Background(), obj, "example.com", "Internal"); err != nil {
		t.Fatalf("RecordWrite: unexpected error: %v", err)
	}

	pending, ok, err := GetPendingSerial(obj)
	if err != nil || !ok {
		t.Fatalf("expected a pending annotation after RecordWrite, ok=%v err=%v", ok, err)
	}
	if pending.Serial != 5 || pending.Zone != "example.com/Internal" {
		t.Fatalf("unexpected pending serial: %+v", pending)
	}
}

func TestGateRecordWriteNoSerialSignalDoesNothing(t *testing.T) {
	m := &mockZoneAuthServer{respond: func(r *http.Request) (interface{}, int) {
		return []zoneAuthObject{}, http.StatusOK // zone not found
	}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	g := NewGate(newTestClient(t, srv), nil, nil, "")
	obj := newObj("r1")

	if err := g.RecordWrite(context.Background(), obj, "example.com", "Internal"); err != nil {
		t.Fatalf("RecordWrite: unexpected error: %v", err)
	}
	if _, ok, _ := GetPendingSerial(obj); ok {
		t.Fatal("RecordWrite must not set an annotation when there is no serial signal")
	}
}

// ── Gate.Evaluate: pending-write convergence lifecycle ─────────────────────

func gateWithPending(t *testing.T, candidateServer *httptest.Server, breaker *dualclient.CircuitBreaker, hostname string) (*Gate, *metav1.ObjectMeta) {
	t.Helper()
	g := NewGate(nil, newTestClient(t, candidateServer), breaker, hostname)
	obj := newObj("r1")
	if err := SetPendingSerial(obj, "example.com", "Internal", 5, time.Now()); err != nil {
		t.Fatalf("SetPendingSerial: unexpected error: %v", err)
	}
	return g, obj
}

func TestGateEvaluateCaughtUpClearsAnnotationAndRoutesToCandidate(t *testing.T) {
	m := &mockZoneAuthServer{respond: func(r *http.Request) (interface{}, int) {
		return []zoneAuthObject{{MemberSOASerials: []MemberSerial{{GridPrimary: "gmc.example.com", Serial: 5}}}}, http.StatusOK
	}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	breaker := dualclient.NewCircuitBreaker(5, time.Minute)
	g, obj := gateWithPending(t, srv, breaker, "gmc.example.com")

	d := g.Evaluate(context.Background(), obj, "example.com", "Internal", ModeSOASerial, DefaultTimeout)
	if !d.UseCandidate || d.Reason != ReasonCandidateReady {
		t.Fatalf("unexpected decision: %+v", d)
	}
	if _, ok, _ := GetPendingSerial(obj); ok {
		t.Fatal("expected the pending annotation to be cleared once caught up")
	}
	if breaker.FailureCount() != 0 {
		t.Fatal("a successful candidate read must not leave a nonzero failure count")
	}
}

func TestGateEvaluateNotCaughtUpRoutesToPrimaryAndKeepsAnnotation(t *testing.T) {
	m := &mockZoneAuthServer{respond: func(r *http.Request) (interface{}, int) {
		return []zoneAuthObject{{MemberSOASerials: []MemberSerial{{GridPrimary: "gmc.example.com", Serial: 3}}}}, http.StatusOK
	}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	g, obj := gateWithPending(t, srv, nil, "gmc.example.com")

	d := g.Evaluate(context.Background(), obj, "example.com", "Internal", ModeSOASerial, DefaultTimeout)
	if d.UseCandidate || d.Reason != ReasonWaitingForReplication {
		t.Fatalf("unexpected decision: %+v", d)
	}
	if _, ok, _ := GetPendingSerial(obj); !ok {
		t.Fatal("the pending annotation must remain while still waiting for convergence")
	}
}

func TestGateEvaluateCandidateHostnameNotInMemberSerials(t *testing.T) {
	m := &mockZoneAuthServer{respond: func(r *http.Request) (interface{}, int) {
		return []zoneAuthObject{{MemberSOASerials: []MemberSerial{{GridPrimary: "some-other-member.example.com", Serial: 9}}}}, http.StatusOK
	}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	g, obj := gateWithPending(t, srv, nil, "gmc.example.com")

	d := g.Evaluate(context.Background(), obj, "example.com", "Internal", ModeSOASerial, DefaultTimeout)
	if d.UseCandidate || d.Reason != ReasonWaitingForReplication {
		t.Fatalf("unexpected decision: %+v", d)
	}
	if _, ok, _ := GetPendingSerial(obj); !ok {
		t.Fatal("the pending annotation must remain when the candidate hostname is absent from member_soa_serials")
	}
}

func TestGateEvaluateZoneNotFoundGuard(t *testing.T) {
	m := &mockZoneAuthServer{respond: func(r *http.Request) (interface{}, int) {
		return []zoneAuthObject{}, http.StatusOK
	}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	g, obj := gateWithPending(t, srv, nil, "gmc.example.com")

	d := g.Evaluate(context.Background(), obj, "example.com", "Internal", ModeSOASerial, DefaultTimeout)
	if d.UseCandidate || d.Reason != ReasonPrimaryOnly || !d.Warning {
		t.Fatalf("unexpected decision: %+v", d)
	}
}

func TestGateEvaluateNoGridPrimaryGuard(t *testing.T) {
	// Zone exists but member_soa_serials is an empty array (no
	// grid_primary assigned).
	m := &mockZoneAuthServer{respond: func(r *http.Request) (interface{}, int) {
		return []zoneAuthObject{{MemberSOASerials: []MemberSerial{}}}, http.StatusOK
	}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	g, obj := gateWithPending(t, srv, nil, "gmc.example.com")

	d := g.Evaluate(context.Background(), obj, "example.com", "Internal", ModeSOASerial, DefaultTimeout)
	if d.UseCandidate || d.Reason != ReasonPrimaryOnly || !d.Warning {
		t.Fatalf("unexpected decision: %+v", d)
	}
}

func TestGateEvaluateCandidateHTTPErrorTripsBreaker(t *testing.T) {
	m := &mockZoneAuthServer{respond: func(r *http.Request) (interface{}, int) {
		return map[string]string{"Error": "boom"}, http.StatusInternalServerError
	}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	breaker := dualclient.NewCircuitBreaker(5, time.Minute)
	g, obj := gateWithPending(t, srv, breaker, "gmc.example.com")

	d := g.Evaluate(context.Background(), obj, "example.com", "Internal", ModeSOASerial, DefaultTimeout)
	if d.UseCandidate || d.Reason != ReasonCandidateDegraded || !d.Warning {
		t.Fatalf("unexpected decision: %+v", d)
	}
	if breaker.FailureCount() != 1 {
		t.Fatalf("expected the circuit breaker failure count to increment, got %d", breaker.FailureCount())
	}
}

func TestGateEvaluateConvergenceTimeoutClearsAnnotation(t *testing.T) {
	srv := poisonServer(t) // timeout must short-circuit before calling the candidate
	defer srv.Close()
	g := NewGate(nil, newTestClient(t, srv), nil, "gmc.example.com")

	obj := newObj("r1")
	staleSince := time.Now().Add(-2 * time.Minute)
	if err := SetPendingSerial(obj, "example.com", "Internal", 5, staleSince); err != nil {
		t.Fatalf("SetPendingSerial: unexpected error: %v", err)
	}

	d := g.Evaluate(context.Background(), obj, "example.com", "Internal", ModeSOASerial, time.Minute)
	if d.UseCandidate || d.Reason != ReasonConvergenceTimeout || !d.Warning {
		t.Fatalf("unexpected decision: %+v", d)
	}
	if _, ok, _ := GetPendingSerial(obj); ok {
		t.Fatal("a convergence timeout must clear the pending annotation so the gate can resume on the next write")
	}
}

// ── Controller restart resumption ───────────────────────────────────────

func TestGateEvaluateResumesAfterSimulatedRestart(t *testing.T) {
	m := &mockZoneAuthServer{respond: func(r *http.Request) (interface{}, int) {
		return []zoneAuthObject{{MemberSOASerials: []MemberSerial{{GridPrimary: "gmc.example.com", Serial: 5}}}}, http.StatusOK
	}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	// Pre-restart: a write sets the pending annotation directly — the
	// Gate that recorded it is gone by the time we check, so only the
	// annotation it left behind matters for this test.
	obj := newObj("r1")
	if err := SetPendingSerial(obj, "example.com", "Internal", 5, time.Now()); err != nil {
		t.Fatalf("SetPendingSerial: unexpected error: %v", err)
	}

	// A brand-new Gate (simulating a controller restart — no in-memory
	// state survives) picks the pending annotation straight back up.
	g2 := NewGate(nil, newTestClient(t, srv), nil, "gmc.example.com")
	d := g2.Evaluate(context.Background(), obj, "example.com", "Internal", ModeSOASerial, DefaultTimeout)
	if !d.UseCandidate || d.Reason != ReasonCandidateReady {
		t.Fatalf("unexpected decision after simulated restart: %+v", d)
	}
}

// ── Multiple records sharing one zone ───────────────────────────────────

func TestGateEvaluateMultipleRecordsSameZoneBothClearOnConverge(t *testing.T) {
	m := &mockZoneAuthServer{respond: func(r *http.Request) (interface{}, int) {
		return []zoneAuthObject{{MemberSOASerials: []MemberSerial{{GridPrimary: "gmc.example.com", Serial: 9}}}}, http.StatusOK
	}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	g := NewGate(nil, newTestClient(t, srv), nil, "gmc.example.com")

	obj1 := newObj("r1")
	obj2 := newObj("r2")
	if err := SetPendingSerial(obj1, "example.com", "Internal", 5, time.Now()); err != nil {
		t.Fatalf("SetPendingSerial obj1: unexpected error: %v", err)
	}
	if err := SetPendingSerial(obj2, "example.com", "Internal", 5, time.Now()); err != nil {
		t.Fatalf("SetPendingSerial obj2: unexpected error: %v", err)
	}

	d1 := g.Evaluate(context.Background(), obj1, "example.com", "Internal", ModeSOASerial, DefaultTimeout)
	d2 := g.Evaluate(context.Background(), obj2, "example.com", "Internal", ModeSOASerial, DefaultTimeout)

	if !d1.UseCandidate || !d2.UseCandidate {
		t.Fatalf("expected both records in the converged zone to route to candidate: d1=%+v d2=%+v", d1, d2)
	}
	if _, ok, _ := GetPendingSerial(obj1); ok {
		t.Fatal("obj1 annotation must be cleared")
	}
	if _, ok, _ := GetPendingSerial(obj2); ok {
		t.Fatal("obj2 annotation must be cleared")
	}
}

// ── IPAM override, end to end (EffectiveMode feeding into Evaluate) ──────

func TestGateEvaluateIPAMResourceAlwaysPrimary(t *testing.T) {
	srv := poisonServer(t)
	defer srv.Close()
	g := NewGate(nil, newTestClient(t, srv), nil, "gmc.example.com")

	mode, overridden := EffectiveMode(ModeSOASerial, true) // IPAM resource, configured soaSerial
	if !overridden {
		t.Fatal("expected EffectiveMode to report an override for an IPAM resource configured with soaSerial")
	}

	d := g.Evaluate(context.Background(), newObj("net1"), "", "", mode, DefaultTimeout)
	if d.UseCandidate || d.Reason != ReasonPrimaryOnly {
		t.Fatalf("IPAM resource must always route to primary: %+v", d)
	}
}
