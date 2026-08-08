// Package zoneauth read/write endpoint split (read-routing) tests.
//
// ZoneAuth is the wave's one gated zone package: it carries its own SOA
// serial (soa_serial_number/member_soa_serials on the zone_auth object),
// so unlike ZoneDelegated/ZoneForward it is wired with hasZoneSerial=true
// and its own fqdn/view as the gate key — never the record-shaped
// strip-a-label derivation the base-case DNS record packages use, which
// would name the wrong (parent) zone for a zone object. Observe has two
// read sites (resolveZoneAuthIdentity and, on a search failure, the
// identity-prerequisite check) and both must route through readFrom —
// see TestClusterObserveBothReadSitesRouteToCandidate.
package zoneauth

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/convergence"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/readrouting"
)

// recordingKubeClient is a minimal client.Client stub used to verify that
// Create()/Update() persist the convergence watermark annotation via a
// real kube client call (RecordWrite's conflict-safe merge patch), not
// merely an in-memory annotation mutation crossplane-runtime's managed
// reconciler would silently discard after Update(). Only Patch/Update are
// exercised by these tests; every other client.Client method is left to
// the embedded nil interface.
type recordingKubeClient struct {
	client.Client
	updated client.Object
}

func (k *recordingKubeClient) Update(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
	k.updated = obj
	return nil
}

func (k *recordingKubeClient) Patch(_ context.Context, obj client.Object, _ client.Patch, _ ...client.PatchOption) error {
	k.updated = obj
	return nil
}

// poisonIBConnector fails the test if any of its methods is ever called —
// used to prove a code path never touches the connector it wraps when
// routing decided against it.
type poisonIBConnector struct{ t *testing.T }

func (p poisonIBConnector) CreateObject(ibclient.IBObject) (string, error) {
	p.t.Helper()
	p.t.Fatal("unexpected poisoned-connector CreateObject call")
	return "", nil
}

func (p poisonIBConnector) GetObject(ibclient.IBObject, string, *ibclient.QueryParams, interface{}) error {
	p.t.Helper()
	p.t.Fatal("unexpected poisoned-connector GetObject call")
	return nil
}

func (p poisonIBConnector) DeleteObject(string) (string, error) {
	p.t.Helper()
	p.t.Fatal("unexpected poisoned-connector DeleteObject call")
	return "", nil
}

func (p poisonIBConnector) UpdateObject(ibclient.IBObject, string) (string, error) {
	p.t.Helper()
	p.t.Fatal("unexpected poisoned-connector UpdateObject call")
	return "", nil
}

// always500Server answers every HTTP request with 500 — used to force a
// search-by-uid failure (identity.IsSearchFailure) so Observe's
// search-failure fallback branch (the second read site) is exercised.
func always500Server(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
}

// zoneAuthSerialServer answers every zone_auth query with a fixed
// soa_serial_number — used to prove RecordWrite actually stores a serial
// for a ZoneAuth zone (this ticket's central open question).
func zoneAuthSerialServer(t *testing.T, serial uint32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `[{"soa_serial_number":%d}]`, serial)
	}))
}

// newZoneClient builds a convergence.Client pointed at an httptest.Server
// via plain HTTP, mirroring newTestConnector's pattern for the SDK
// connector.
func newZoneClient(t *testing.T, srv *httptest.Server) *convergence.Client {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("cannot parse test server URL: %v", err)
	}
	return convergence.NewClientWithScheme("http", u.Hostname(), u.Port(), wapiVersion, "test-user", "test-pass", true)
}

// TestClusterObserveReadEndpointAbsentNoCandidateCall proves that with no
// readEndpoint configured (router.Gate == nil), Observe never touches a
// candidate connector — readFrom is always e.conn, and no ReadRouting
// condition is set.
func TestClusterObserveReadEndpointAbsentNoCandidateCall(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneAuth{
		Fqdn: "example.com",
		View: stringPtr("default"),
		Ea:   identity.Stamp(nil, "test-uid-cluster"),
	})

	e := &clusterExternal{
		kube: &recordingKubeClient{}, conn: newTestConnector(t, srv),
		router: readrouting.Router{Candidate: poisonIBConnector{t: t}}, // gate is nil, so this must never be touched
		prober: identity.NewProber(), endpoint: t.Name(),
	}
	cr := newClusterZoneAuth("my-zoneauth", ref)

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Fatal("Observe: want ResourceExists=true, got false")
	}
	if cond := cr.GetCondition(convergence.ReadRoutingConditionType); cond.Status != corev1.ConditionUnknown {
		t.Fatalf("Observe: expected no ReadRouting condition when no readEndpoint is configured, got %+v", cond)
	}
}

// TestNamespacedObserveReadEndpointAbsentNoCandidateCall is the
// namespaced-scope variant.
func TestNamespacedObserveReadEndpointAbsentNoCandidateCall(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneAuth{
		Fqdn: "example.com",
		View: stringPtr("default"),
		Ea:   identity.Stamp(nil, "test-uid-namespaced"),
	})

	e := &namespacedExternal{
		kube: &recordingKubeClient{}, conn: newTestConnector(t, srv),
		router: readrouting.Router{Candidate: poisonIBConnector{t: t}},
		prober: identity.NewProber(), endpoint: t.Name(),
	}
	cr := newNamespacedZoneAuth("team-a", "my-zoneauth", ref, "ProviderConfig")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Fatal("Observe: want ResourceExists=true, got false")
	}
	if cond := cr.GetCondition(convergence.ReadRoutingConditionType); cond.Status != corev1.ConditionUnknown {
		t.Fatalf("Observe: expected no ReadRouting condition when no readEndpoint is configured, got %+v", cond)
	}
}

// TestClusterObserveBothReadSitesRouteToCandidate proves the ticket's
// central variation-3 requirement: ZoneAuth's Observe has TWO e.conn read
// sites (resolveZoneAuthIdentity, and — on a search failure — the
// identity-prerequisite fallback), and both must route through readFrom.
// The primary connector is poisoned; the candidate is a real server that
// answers every request with 500, forcing a search failure and driving
// Observe into the prerequisite fallback branch. If either read site
// still used the primary, the poison connector would fail the test
// immediately instead of the assertions below ever running.
func TestClusterObserveBothReadSitesRouteToCandidate(t *testing.T) {
	candidateSrv := always500Server(t)
	defer candidateSrv.Close()

	// Gate.Candidate (a convergence.Client for the zone_auth
	// member_soa_serials query) is never actually dialed here — the
	// steady-state "no pending write" branch in evaluatePending decides
	// UseCandidate before evaluateCandidateSerial would touch it. It
	// must still be non-nil, though: evaluateStructural treats a nil
	// Gate.Candidate as "no read endpoint configured" and forces
	// primary-only, which is a different code path than this test
	// means to exercise.
	gate := convergence.NewGate(nil, convergence.NewClientWithScheme("http", "unused", "0", wapiVersion, "u", "p", true), nil, "candidate-host")

	e := &clusterExternal{
		kube: &recordingKubeClient{},
		conn: poisonIBConnector{t: t}, // primary must never be touched
		router: readrouting.Router{
			Candidate: newTestConnector(t, candidateSrv),
			Gate:      gate,
			Mode:      convergence.ModeSOASerial,
			Timeout:   convergence.DefaultTimeout,
		},
		prober: identity.NewProber(), endpoint: t.Name(),
	}
	// No prior external-name — ref is "" so Resolve calls searchByUID
	// directly (skipping resolveByRef), and the always-500 candidate
	// server fails that search.
	cr := newClusterZoneAuth("my-zoneauth", "")

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected an error (the always-500 candidate breaks both the search and the prerequisite fallback), got nil")
	}
	cond := cr.GetCondition(convergence.ReadRoutingConditionType)
	if cond.Status != corev1.ConditionTrue || string(cond.Reason) != convergence.ReasonCandidateReady {
		t.Fatalf("Observe: expected steady-state candidate routing before the read failed, got %+v", cond)
	}
}

// TestCreatePersistsConvergenceWriteViaPatch is this ticket's answer to
// the open question: it proves RecordWrite actually stores a SOA serial
// for a ZoneAuth zone, keyed on the zone's OWN fqdn/view (never the
// record-shaped strip-a-label derivation).
func TestCreatePersistsConvergenceWriteViaPatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	zoneSrv := zoneAuthSerialServer(t, 42)
	defer zoneSrv.Close()

	gate := convergence.NewGate(newZoneClient(t, zoneSrv), nil, nil, "")
	kube := &recordingKubeClient{}

	e := &clusterExternal{
		kube: kube, conn: newTestConnector(t, srv),
		router: readrouting.Router{Gate: gate, Mode: convergence.ModeSOASerial, Timeout: convergence.DefaultTimeout},
		prober: identity.NewProber(), endpoint: t.Name(),
	}
	cr := newClusterZoneAuth("my-zoneauth", "")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	ann, ok := cr.GetAnnotations()[convergence.PendingZoneSerialAnnotation]
	if !ok || ann == "" {
		t.Fatal("Create: expected the pending-zone-serial annotation to be set — RecordWrite must actually store a serial for a ZoneAuth zone")
	}
	if kube.updated == nil {
		t.Fatal("Create: expected the annotation to be persisted via kube.Patch")
	}
}

// TestCreateReadEndpointAbsentSkipsConvergenceWrite proves Create() is a
// byte-for-byte no-op on the convergence path when no readEndpoint is
// configured.
func TestCreateReadEndpointAbsentSkipsConvergenceWrite(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	kube := &recordingKubeClient{}
	e := &clusterExternal{kube: kube, conn: newTestConnector(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneAuth("my-zoneauth", "")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}
	if _, ok := cr.GetAnnotations()[convergence.PendingZoneSerialAnnotation]; ok {
		t.Fatal("Create: expected no pending-zone-serial annotation when no readEndpoint is configured")
	}
	if kube.updated != nil {
		t.Fatal("Create: expected no Patch call when no readEndpoint is configured")
	}
}

// TestUpdatePersistsConvergenceWriteViaPatch is the hazard-2 pin for
// ZoneAuth: Update() must explicitly persist the convergence watermark
// via kube.Patch, because crossplane-runtime's managed reconciler does
// not flush metadata mutations made inside Update() on its own.
func TestUpdatePersistsConvergenceWriteViaPatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneAuth{
		Fqdn: "example.com",
		View: stringPtr("default"),
		Ea:   identity.Stamp(nil, "test-uid-cluster"),
	})

	zoneSrv := zoneAuthSerialServer(t, 7)
	defer zoneSrv.Close()

	gate := convergence.NewGate(newZoneClient(t, zoneSrv), nil, nil, "")
	kube := &recordingKubeClient{}

	e := &clusterExternal{
		kube: kube, conn: newTestConnector(t, srv),
		router: readrouting.Router{Gate: gate, Mode: convergence.ModeSOASerial, Timeout: convergence.DefaultTimeout},
		prober: identity.NewProber(), endpoint: t.Name(),
	}
	cr := newClusterZoneAuth("my-zoneauth", ref)
	cr.Spec.ForProvider.Comment = stringPtr("updated")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	ann, ok := cr.GetAnnotations()[convergence.PendingZoneSerialAnnotation]
	if !ok || ann == "" {
		t.Fatal("Update: expected the pending-zone-serial annotation to be set")
	}
	if kube.updated == nil {
		t.Fatal("Update: expected the annotation to be persisted via kube.Patch — an in-memory-only write is silently discarded by crossplane-runtime")
	}
}

// TestDeleteNeverRecordsConvergenceWrite proves Delete() never calls
// router.RecordWrite: the managed resource is being removed, so there is
// no future Observe left to gate, and no Patch call should occur.
func TestDeleteNeverRecordsConvergenceWrite(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneAuth{
		Fqdn: "example.com",
		View: stringPtr("default"),
		Ea:   identity.Stamp(nil, "test-uid-cluster"),
	})

	gate := convergence.NewGate(newZoneClient(t, always500Server(t)), nil, nil, "")
	kube := &recordingKubeClient{}

	e := &clusterExternal{
		kube: kube, conn: newTestConnector(t, srv),
		router: readrouting.Router{Gate: gate, Mode: convergence.ModeSOASerial, Timeout: convergence.DefaultTimeout},
		prober: identity.NewProber(), endpoint: t.Name(),
	}
	cr := newClusterZoneAuth("my-zoneauth", ref)

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
	if kube.updated != nil {
		t.Fatal("Delete: expected no Patch call — Delete never records a convergence write")
	}
}

// TestNamespacedUpdatePersistsConvergenceWriteViaPatch is the namespaced
// counterpart of TestUpdatePersistsConvergenceWriteViaPatch.
func TestNamespacedUpdatePersistsConvergenceWriteViaPatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneAuth{
		Fqdn: "example.com",
		View: stringPtr("default"),
		Ea:   identity.Stamp(nil, "test-uid-namespaced"),
	})

	zoneSrv := zoneAuthSerialServer(t, 7)
	defer zoneSrv.Close()

	gate := convergence.NewGate(newZoneClient(t, zoneSrv), nil, nil, "")
	kube := &recordingKubeClient{}

	e := &namespacedExternal{
		kube: kube, conn: newTestConnector(t, srv),
		router: readrouting.Router{Gate: gate, Mode: convergence.ModeSOASerial, Timeout: convergence.DefaultTimeout},
		prober: identity.NewProber(), endpoint: t.Name(),
	}
	cr := newNamespacedZoneAuth("team-a", "my-zoneauth", ref, "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("updated")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	if _, ok := cr.GetAnnotations()[convergence.PendingZoneSerialAnnotation]; !ok {
		t.Fatal("Update: expected the pending-zone-serial annotation to be set")
	}
	if kube.updated == nil {
		t.Fatal("Update: expected the annotation to be persisted via kube.Patch")
	}
}
