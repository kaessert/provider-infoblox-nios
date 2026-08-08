package readrouting

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource/fake"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/convergence"
)

const wapiVersion = "2.9.7"

// poisonIBConnector fails the test if any of its methods is ever called
// — used to prove BeginObserve never touches a connector it decided
// against.
type poisonIBConnector struct{ t *testing.T }

func (p poisonIBConnector) CreateObject(ibclient.IBObject) (string, error) {
	p.t.Helper()
	p.t.Fatal("unexpected CreateObject call")
	return "", nil
}

func (p poisonIBConnector) GetObject(ibclient.IBObject, string, *ibclient.QueryParams, interface{}) error {
	p.t.Helper()
	p.t.Fatal("unexpected GetObject call")
	return nil
}

func (p poisonIBConnector) DeleteObject(string) (string, error) {
	p.t.Helper()
	p.t.Fatal("unexpected DeleteObject call")
	return "", nil
}

func (p poisonIBConnector) UpdateObject(ibclient.IBObject, string) (string, error) {
	p.t.Helper()
	p.t.Fatal("unexpected UpdateObject call")
	return "", nil
}

// poisonServer fails the test if it receives any HTTP request.
func poisonServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
	}))
}

func zoneAuthSerialServer(t *testing.T, serial uint32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `[{"soa_serial_number":%d}]`, serial)
	}))
}

func newZoneClient(t *testing.T, srv *httptest.Server) *convergence.Client {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("cannot parse test server URL: %v", err)
	}
	return convergence.NewClientWithScheme("http", u.Hostname(), u.Port(), wapiVersion, "test-user", "test-pass", true)
}

// fakeEventRecorder captures the events passed to Event, for tests
// asserting a Kubernetes Warning event was (or was not) emitted.
type fakeEventRecorder struct{ events []event.Event }

func (r *fakeEventRecorder) Event(_ runtime.Object, e event.Event)    { r.events = append(r.events, e) }
func (r *fakeEventRecorder) WithAnnotations(...string) event.Recorder { return r }

// recordingKubeClient is a minimal client.Client stub that records
// whether Patch was called, mirroring the pattern used by every resource
// package's own tests for the same purpose.
type recordingKubeClient struct {
	client.Client
	patched client.Object
}

func (k *recordingKubeClient) Patch(_ context.Context, obj client.Object, _ client.Patch, _ ...client.PatchOption) error {
	k.patched = obj
	return nil
}

func newObj(name string) *fake.Managed {
	o := &fake.Managed{}
	o.SetName(name)
	o.SetUID("test-uid")
	return o
}

// TestBeginObserveNoGateReturnsPrimary proves the zero-value/no-gate case
// is a byte-for-byte no-op: the given primary is returned unchanged,
// Candidate is never touched, and no ReadRouting condition is set.
func TestBeginObserveNoGateReturnsPrimary(t *testing.T) {
	primary := poisonIBConnector{} // not called, just needs to be non-nil to compare against
	var r Router
	cr := newObj("test")

	readFrom, changed := r.BeginObserve(context.Background(), cr, primary, "example.com", "default", false)
	if changed {
		t.Fatal("BeginObserve: expected annotationChanged=false when Gate is nil")
	}
	if _, ok := readFrom.(poisonIBConnector); !ok {
		t.Fatalf("BeginObserve: expected the given primary to be returned, got %#v", readFrom)
	}
	if cond := cr.GetCondition(convergence.ReadRoutingConditionType); cond.Status != corev1.ConditionUnknown {
		t.Fatalf("BeginObserve: expected no ReadRouting condition when Gate is nil, got %+v", cond)
	}
}

// TestBeginObserveRoutesToCandidateWhenGateReady proves the steady-state
// case: no pending-zone-serial annotation means the gate reports
// UseCandidate immediately, and BeginObserve returns Candidate.
func TestBeginObserveRoutesToCandidateWhenGateReady(t *testing.T) {
	gate := convergence.NewGate(nil, newZoneClient(t, poisonServer(t)), nil, "candidate-host")
	candidate := poisonIBConnector{t: t} // never touched here — evaluatePending short-circuits before any zone_auth call
	r := Router{
		Candidate: candidate,
		Gate:      gate,
		Mode:      convergence.ModeSOASerial,
		Timeout:   convergence.DefaultTimeout,
	}
	cr := newObj("test")
	primary := poisonIBConnector{t: t} // must not be touched or returned once the gate says candidate

	readFrom, _ := r.BeginObserve(context.Background(), cr, primary, "example.com", "default", false)
	if readFrom != ibclient.IBConnector(candidate) {
		t.Fatalf("BeginObserve: expected Candidate to be returned, got %#v", readFrom)
	}
	cond := cr.GetCondition(convergence.ReadRoutingConditionType)
	if cond.Status != corev1.ConditionTrue || string(cond.Reason) != convergence.ReasonCandidateReady {
		t.Fatalf("BeginObserve: unexpected ReadRouting condition: %+v", cond)
	}
}

// TestBeginObserveIPAMOverrideForcesPrimaryOnly proves isIPAM=true
// forces primaryOnly regardless of the configured mode, and never issues
// a candidate call.
func TestBeginObserveIPAMOverrideForcesPrimaryOnly(t *testing.T) {
	gate := convergence.NewGate(nil, newZoneClient(t, poisonServer(t)), nil, "candidate-host")
	r := Router{
		Candidate: poisonIBConnector{t: t},
		Gate:      gate,
		Mode:      convergence.ModeSOASerial,
		Timeout:   convergence.DefaultTimeout,
	}
	cr := newObj("test")
	primary := poisonIBConnector{t: t}

	readFrom, _ := r.BeginObserve(context.Background(), cr, primary, "example.com", "default", true)
	if readFrom != ibclient.IBConnector(primary) {
		t.Fatalf("BeginObserve: expected the primary to be returned for an IPAM resource, got %#v", readFrom)
	}
	cond := cr.GetCondition(convergence.ReadRoutingConditionType)
	if cond.Status != corev1.ConditionFalse || string(cond.Reason) != convergence.ReasonPrimaryOnly {
		t.Fatalf("BeginObserve: unexpected ReadRouting condition for an IPAM resource: %+v", cond)
	}
}

// TestBeginObserveEmitsWarningOnFallback proves a fallback decision
// (candidate unreachable) emits exactly one Kubernetes Warning event.
func TestBeginObserveEmitsWarningOnFallback(t *testing.T) {
	failingSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failingSrv.Close()

	gate := convergence.NewGate(nil, newZoneClient(t, failingSrv), nil, "candidate-host")
	cr := newObj("test")
	if err := convergence.SetPendingSerial(cr, "example.com", "default", 5, time.Now()); err != nil {
		t.Fatalf("SetPendingSerial: unexpected error: %v", err)
	}
	rec := &fakeEventRecorder{}
	r := Router{
		Candidate: poisonIBConnector{t: t},
		Gate:      gate,
		Mode:      convergence.ModeSOASerial,
		Timeout:   convergence.DefaultTimeout,
		Recorder:  rec,
	}
	primary := poisonIBConnector{t: t}

	readFrom, _ := r.BeginObserve(context.Background(), cr, primary, "example.com", "default", false)
	if readFrom != ibclient.IBConnector(primary) {
		t.Fatalf("BeginObserve: expected the primary on candidate failure, got %#v", readFrom)
	}
	if len(rec.events) != 1 || rec.events[0].Type != event.TypeWarning {
		t.Fatalf("BeginObserve: expected exactly one Warning event, got %+v", rec.events)
	}
}

// TestRecordWriteNilGateIsNoOp proves RecordWrite is a byte-for-byte
// no-op when no readEndpoint is configured.
func TestRecordWriteNilGateIsNoOp(t *testing.T) {
	var r Router
	kube := &recordingKubeClient{}
	cr := newObj("test")

	if err := r.RecordWrite(context.Background(), kube, cr, "example.com", "default"); err != nil {
		t.Fatalf("RecordWrite: unexpected error: %v", err)
	}
	if kube.patched != nil {
		t.Fatal("RecordWrite: expected no Patch call when Gate is nil")
	}
	if _, ok := cr.GetAnnotations()[convergence.PendingZoneSerialAnnotation]; ok {
		t.Fatal("RecordWrite: expected no pending-zone-serial annotation when Gate is nil")
	}
}

// TestRecordWritePersistsViaPatch proves RecordWrite reads the primary's
// serial, sets the pending-zone-serial annotation, and persists it via
// kube.Patch rather than relying on an in-memory-only write.
func TestRecordWritePersistsViaPatch(t *testing.T) {
	zoneSrv := zoneAuthSerialServer(t, 42)
	defer zoneSrv.Close()

	gate := convergence.NewGate(newZoneClient(t, zoneSrv), nil, nil, "")
	r := Router{Gate: gate}
	kube := &recordingKubeClient{}
	cr := newObj("test")

	if err := r.RecordWrite(context.Background(), kube, cr, "example.com", "default"); err != nil {
		t.Fatalf("RecordWrite: unexpected error: %v", err)
	}
	if ann, ok := cr.GetAnnotations()[convergence.PendingZoneSerialAnnotation]; !ok || ann == "" {
		t.Fatal("RecordWrite: expected the pending-zone-serial annotation to be set")
	}
	if kube.patched == nil {
		t.Fatal("RecordWrite: expected the annotation to be persisted via kube.Patch")
	}
}
