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

// TestGateNowFallsBackToRealClockWhenZeroValue covers a Gate built via a
// bare struct literal (clock left nil) rather than NewGate, which always
// wires clock to time.Now. now() must still return a sane, current-ish
// time rather than panicking or returning the zero time.Time.
func TestGateNowFallsBackToRealClockWhenZeroValue(t *testing.T) {
	g := &Gate{}
	before := time.Now()
	got := g.now()
	after := time.Now()
	if got.Before(before) || got.After(after) {
		t.Fatalf("Gate{}.now() = %v, want a value between %v and %v", got, before, after)
	}
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
	g := NewGate(nil, newTestClient(t, srv), nil, testCandidateFQN)

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

	g := NewGate(nil, newTestClient(t, srv), breaker, testCandidateFQN)
	d := g.Evaluate(context.Background(), newObj("r1"), "example.com", "Internal", ModeSOASerial, DefaultTimeout)
	if d.UseCandidate || d.Reason != ReasonCandidateDegraded || !d.Warning {
		t.Fatalf("unexpected decision: %+v", d)
	}
}

// ── Gate.Evaluate: steady state (no pending write) ────────────────────────

func TestGateEvaluateNoPendingAnnotationRoutesToCandidate(t *testing.T) {
	srv := poisonServer(t) // steady state must not need a candidate call
	defer srv.Close()
	g := NewGate(nil, newTestClient(t, srv), nil, testCandidateFQN)

	d := g.Evaluate(context.Background(), newObj("r1"), "example.com", "Internal", ModeSOASerial, DefaultTimeout)
	if !d.UseCandidate || d.Reason != ReasonCandidateReady {
		t.Fatalf("unexpected decision: %+v", d)
	}
}

func TestGateEvaluateCorruptAnnotationFallsBackToPrimary(t *testing.T) {
	srv := poisonServer(t)
	defer srv.Close()
	g := NewGate(nil, newTestClient(t, srv), nil, testCandidateFQN)

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
		return []zoneAuthObject{{Ref: testZoneAuthRef, SOASerialNumber: uint32Ptr(5)}}, http.StatusOK
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

func TestGateRecordWriteNoPrimaryConfiguredIsNoop(t *testing.T) {
	g := NewGate(nil, nil, nil, "")
	obj := newObj("r1")
	if err := g.RecordWrite(context.Background(), obj, "example.com", "Internal"); err != nil {
		t.Fatalf("RecordWrite with no primary configured: unexpected error: %v", err)
	}
	if _, ok, _ := GetPendingSerial(obj); ok {
		t.Fatal("RecordWrite must not set an annotation when no primary client is configured")
	}
}

func TestGateRecordWritePropagatesPrimaryError(t *testing.T) {
	m := &mockZoneAuthServer{respond: func(r *http.Request) (interface{}, int) {
		return map[string]string{"Error": "boom"}, http.StatusInternalServerError
	}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	g := NewGate(newTestClient(t, srv), nil, nil, "")
	obj := newObj("r1")

	if err := g.RecordWrite(context.Background(), obj, "example.com", "Internal"); err == nil {
		t.Fatal("expected RecordWrite to surface a primary zone_auth read error")
	}
	if _, ok, _ := GetPendingSerial(obj); ok {
		t.Fatal("RecordWrite must not set an annotation when the primary read fails")
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
		return []zoneAuthObject{{MemberSOASerials: []MemberSerial{{GridPrimary: testCandidateFQN, Serial: 5}}}}, http.StatusOK
	}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	breaker := dualclient.NewCircuitBreaker(5, time.Minute)
	g, obj := gateWithPending(t, srv, breaker, testCandidateFQN)

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
		return []zoneAuthObject{{MemberSOASerials: []MemberSerial{{GridPrimary: testCandidateFQN, Serial: 3}}}}, http.StatusOK
	}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	g, obj := gateWithPending(t, srv, nil, testCandidateFQN)

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
	g, obj := gateWithPending(t, srv, nil, testCandidateFQN)

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
	g, obj := gateWithPending(t, srv, nil, testCandidateFQN)

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
	g, obj := gateWithPending(t, srv, nil, testCandidateFQN)

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
	g, obj := gateWithPending(t, srv, breaker, testCandidateFQN)

	d := g.Evaluate(context.Background(), obj, "example.com", "Internal", ModeSOASerial, DefaultTimeout)
	if d.UseCandidate || d.Reason != ReasonCandidateDegraded || !d.Warning {
		t.Fatalf("unexpected decision: %+v", d)
	}
	if breaker.FailureCount() != 1 {
		t.Fatalf("expected the circuit breaker failure count to increment, got %d", breaker.FailureCount())
	}
}

// TestGateEvaluateConsecutiveCandidateFailuresOpenBreakerEndToEnd drives N
// consecutive candidate HTTP errors through Gate.Evaluate itself (not the
// CircuitBreaker unit tested in isolation) and confirms the (N+1)th
// Evaluate call trips the breaker and stops calling the candidate WAPI
// entirely — proving the wiring between Evaluate and the breaker, not just
// each piece independently.
func TestGateEvaluateConsecutiveCandidateFailuresOpenBreakerEndToEnd(t *testing.T) {
	const threshold = 3
	var calls int
	m := &mockZoneAuthServer{respond: func(r *http.Request) (interface{}, int) {
		calls++
		return map[string]string{"Error": "boom"}, http.StatusInternalServerError
	}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	breaker := dualclient.NewCircuitBreaker(threshold, time.Minute)

	for i := 0; i < threshold; i++ {
		g, obj := gateWithPending(t, srv, breaker, testCandidateFQN)
		d := g.Evaluate(context.Background(), obj, "example.com", "Internal", ModeSOASerial, DefaultTimeout)
		if d.UseCandidate || d.Reason != ReasonCandidateDegraded {
			t.Fatalf("failure #%d: unexpected decision: %+v", i+1, d)
		}
	}
	if calls != threshold {
		t.Fatalf("expected %d candidate WAPI calls before the breaker opened, got %d", threshold, calls)
	}
	if !breaker.IsOpen() {
		t.Fatal("breaker must be open after reaching the failure threshold via Evaluate")
	}

	// The next Evaluate call must trip the structural (pre-call) guard and
	// never touch the candidate WAPI again.
	poison := poisonServer(t)
	defer poison.Close()
	g := NewGate(nil, newTestClient(t, poison), breaker, testCandidateFQN)
	obj := newObj("r-final")
	if err := SetPendingSerial(obj, "example.com", "Internal", 5, time.Now()); err != nil {
		t.Fatalf("SetPendingSerial: unexpected error: %v", err)
	}
	d := g.Evaluate(context.Background(), obj, "example.com", "Internal", ModeSOASerial, DefaultTimeout)
	if d.UseCandidate || d.Reason != ReasonCandidateDegraded || !d.Warning {
		t.Fatalf("unexpected decision once the breaker is open: %+v", d)
	}
}

func TestGateEvaluateConvergenceTimeoutClearsAnnotation(t *testing.T) {
	srv := poisonServer(t) // timeout must short-circuit before calling the candidate
	defer srv.Close()
	g := NewGate(nil, newTestClient(t, srv), nil, testCandidateFQN)

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

func TestGateEvaluateZeroTimeoutUsesDefault(t *testing.T) {
	srv := poisonServer(t) // a zero/negative timeout must still short-circuit via DefaultTimeout, not skip straight to the candidate
	defer srv.Close()
	g := NewGate(nil, newTestClient(t, srv), nil, testCandidateFQN)

	obj := newObj("r1")
	// Since is far enough in the past to exceed DefaultTimeout (60s) but the
	// test asserts the zero-timeout path substitutes DefaultTimeout rather
	// than treating <=0 as "never times out".
	staleSince := time.Now().Add(-2 * time.Minute)
	if err := SetPendingSerial(obj, "example.com", "Internal", 5, staleSince); err != nil {
		t.Fatalf("SetPendingSerial: unexpected error: %v", err)
	}

	d := g.Evaluate(context.Background(), obj, "example.com", "Internal", ModeSOASerial, 0)
	if d.UseCandidate || d.Reason != ReasonConvergenceTimeout {
		t.Fatalf("unexpected decision with timeout=0 (must fall back to DefaultTimeout): %+v", d)
	}
}

func TestGateEvaluateUsesInjectedClock(t *testing.T) {
	m := &mockZoneAuthServer{respond: func(r *http.Request) (interface{}, int) {
		return []zoneAuthObject{{MemberSOASerials: []MemberSerial{{GridPrimary: testCandidateFQN, Serial: 3}}}}, http.StatusOK
	}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	g := NewGate(nil, newTestClient(t, srv), nil, testCandidateFQN)
	fixed := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	g.clock = func() time.Time { return fixed }

	obj := newObj("r1")
	if err := SetPendingSerial(obj, "example.com", "Internal", 5, fixed.Add(-30*time.Second)); err != nil {
		t.Fatalf("SetPendingSerial: unexpected error: %v", err)
	}

	// 30s elapsed per the injected clock, well under DefaultTimeout, so the
	// gate must still be waiting (not have timed out) — proving g.now()
	// consulted the injected clock rather than the real wall clock.
	d := g.Evaluate(context.Background(), obj, "example.com", "Internal", ModeSOASerial, DefaultTimeout)
	if d.Reason != ReasonWaitingForReplication {
		t.Fatalf("unexpected decision with injected clock: %+v", d)
	}
}

func TestGateEvaluateCandidateHostnameFoundAfterOtherMembers(t *testing.T) {
	// member_soa_serials lists another grid member before the candidate's
	// own hostname — the lookup loop must not stop at the first entry.
	m := &mockZoneAuthServer{respond: func(r *http.Request) (interface{}, int) {
		return []zoneAuthObject{{MemberSOASerials: []MemberSerial{
			{GridPrimary: "other-member.example.com", Serial: 1},
			{GridPrimary: testCandidateFQN, Serial: 5},
		}}}, http.StatusOK
	}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	g, obj := gateWithPending(t, srv, nil, testCandidateFQN)

	d := g.Evaluate(context.Background(), obj, "example.com", "Internal", ModeSOASerial, DefaultTimeout)
	if !d.UseCandidate || d.Reason != ReasonCandidateReady {
		t.Fatalf("unexpected decision: %+v", d)
	}
}

// ── Controller restart resumption ───────────────────────────────────────

func TestGateEvaluateResumesAfterSimulatedRestart(t *testing.T) {
	m := &mockZoneAuthServer{respond: func(r *http.Request) (interface{}, int) {
		return []zoneAuthObject{{MemberSOASerials: []MemberSerial{{GridPrimary: testCandidateFQN, Serial: 5}}}}, http.StatusOK
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
	g2 := NewGate(nil, newTestClient(t, srv), nil, testCandidateFQN)
	d := g2.Evaluate(context.Background(), obj, "example.com", "Internal", ModeSOASerial, DefaultTimeout)
	if !d.UseCandidate || d.Reason != ReasonCandidateReady {
		t.Fatalf("unexpected decision after simulated restart: %+v", d)
	}
}

// ── Multiple records sharing one zone ───────────────────────────────────

func TestGateEvaluateMultipleRecordsSameZoneBothClearOnConverge(t *testing.T) {
	m := &mockZoneAuthServer{respond: func(r *http.Request) (interface{}, int) {
		return []zoneAuthObject{{MemberSOASerials: []MemberSerial{{GridPrimary: testCandidateFQN, Serial: 9}}}}, http.StatusOK
	}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	g := NewGate(nil, newTestClient(t, srv), nil, testCandidateFQN)

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
	g := NewGate(nil, newTestClient(t, srv), nil, testCandidateFQN)

	mode, overridden := EffectiveMode(ModeSOASerial, true) // IPAM resource, configured soaSerial
	if !overridden {
		t.Fatal("expected EffectiveMode to report an override for an IPAM resource configured with soaSerial")
	}

	d := g.Evaluate(context.Background(), newObj("net1"), "", "", mode, DefaultTimeout)
	if d.UseCandidate || d.Reason != ReasonPrimaryOnly {
		t.Fatalf("IPAM resource must always route to primary: %+v", d)
	}
}
