/*
Copyright 2021 Upbound Inc.
*/

package split

import (
	"context"
	"testing"

	"github.com/crossplane/crossplane-runtime/pkg/logging"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	xpresource "github.com/crossplane/crossplane-runtime/pkg/resource"
	ujconfig "github.com/crossplane/upjet/pkg/config"
	tjcontroller "github.com/crossplane/upjet/pkg/controller"
	"github.com/crossplane/upjet/pkg/resource/fake"
	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// recordingClient is a managed.ExternalClient that records, into a shared trace
// slice, which underlying endpoint ("read"/"write") served each call. Its
// Observe reports the resource exists and is up-to-date.
func recordingClient(label string, trace *[]string) managed.ExternalClient {
	return observingClient(label, trace, managed.ExternalObservation{ResourceExists: true, ResourceUpToDate: true})
}

// observingClient is like recordingClient but returns a caller-supplied Observe
// result, so tests can simulate a candidate that has not yet replicated a write
// (ResourceExists=false or ResourceUpToDate=false).
func observingClient(label string, trace *[]string, obs managed.ExternalObservation) managed.ExternalClient {
	return managed.ExternalClientFns{
		ObserveFn: func(_ context.Context, _ xpresource.Managed) (managed.ExternalObservation, error) {
			*trace = append(*trace, label+":Observe")
			return obs, nil
		},
		CreateFn: func(_ context.Context, _ xpresource.Managed) (managed.ExternalCreation, error) {
			*trace = append(*trace, label+":Create")
			return managed.ExternalCreation{}, nil
		},
		UpdateFn: func(_ context.Context, _ xpresource.Managed) (managed.ExternalUpdate, error) {
			*trace = append(*trace, label+":Update")
			return managed.ExternalUpdate{}, nil
		},
		DeleteFn: func(_ context.Context, _ xpresource.Managed) (managed.ExternalDelete, error) {
			*trace = append(*trace, label+":Delete")
			return managed.ExternalDelete{}, nil
		},
		DisconnectFn: func(_ context.Context) error {
			*trace = append(*trace, label+":Disconnect")
			return nil
		},
	}
}

// fakeConnecter is a hand-rolled managed.ExternalConnecter (crossplane-runtime
// ships ExternalClientFns but no connecter fns fake).
type fakeConnecter struct {
	client managed.ExternalClient
	err    error
}

func (f *fakeConnecter) Connect(_ context.Context, _ xpresource.Managed) (managed.ExternalClient, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.client, nil
}

// trackedMR wraps an upjet fake.Terraformed with a settable GroupVersionKind so
// tests can exercise the DNS-only scope of the split (isDNS). The embedded
// fake.Managed returns schema.EmptyObjectKind, so we shadow GetObjectKind.
type trackedMR struct {
	*fake.Terraformed
	tm metav1.TypeMeta
}

func (t *trackedMR) GetObjectKind() schema.ObjectKind { return &t.tm }

// newTracked builds a tracked MR in the DNS group (eligible for the split).
func newTracked(uid string) *trackedMR { return newTrackedGroup(uid, dnsGroup, "ARecord") }

// newTrackedGroup builds a tracked MR in an arbitrary API group so tests can
// cover both DNS (offloaded) and IPAM (primary-only) routing.
func newTrackedGroup(uid, group, kind string) *trackedMR {
	inner := &fake.Terraformed{}
	inner.SetUID(types.UID(uid))
	inner.SetName("mr-" + uid)
	t := &trackedMR{Terraformed: inner}
	t.tm.SetGroupVersionKind(schema.GroupVersionKind{Group: group, Version: "v1alpha1", Kind: kind})
	return t
}

func newOTS() *tjcontroller.OperationTrackerStore {
	return tjcontroller.NewOperationStore(logging.NewNopLogger())
}

// TestExternalRouting asserts each managed.ExternalClient verb is routed to the
// correct endpoint for a DNS resource in steady state: Observe->read (no write
// in flight, not post-write), the mutating verbs->write, and Disconnect->both.
// Create and Update first prime the write client with its own Observe (against
// the primary), because upjet computes the Terraform instance diff during
// Observe and consumes it during Create/Update; since the reconciler's Observe
// was routed to the read client, the write client must observe before it can
// apply a mutation.
func TestExternalRouting(t *testing.T) {
	resetForTest()

	var trace []string
	e := &external{
		write:      recordingClient("write", &trace),
		read:       recordingClient("read", &trace),
		ots:        newOTS(),
		sameClient: false,
		logger:     logging.NewNopLogger(),
	}
	mg := newTracked("routing")
	ctx := context.Background()

	if _, err := e.Observe(ctx, mg); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if _, err := e.Create(ctx, mg); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := e.Update(ctx, mg); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if _, err := e.Delete(ctx, mg); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := e.Disconnect(ctx); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	want := []string{
		"read:Observe",  // reconciler Observe -> read (candidate), steady state
		"write:Observe", // Create primes the write client's diff -> write (primary)
		"write:Create",
		"write:Observe", // Update primes the write client's diff -> write (primary)
		"write:Update",
		"write:Delete", // Delete needs no priming (upjet guards a nil diff)
		"write:Disconnect",
		"read:Disconnect",
	}
	if !equal(trace, want) {
		t.Fatalf("routing trace mismatch:\n got=%v\nwant=%v", trace, want)
	}
}

// TestObserveSteadyStateReadsCandidate asserts that a DNS resource with no
// post-write marker and no in-flight write Observes from the candidate.
func TestObserveSteadyStateReadsCandidate(t *testing.T) {
	resetForTest()

	var trace []string
	e := &external{
		write:      recordingClient("write", &trace),
		read:       recordingClient("read", &trace),
		ots:        newOTS(),
		sameClient: false,
		logger:     logging.NewNopLogger(),
	}
	if _, err := e.Observe(context.Background(), newTracked("steady")); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if !equal(trace, []string{"read:Observe"}) {
		t.Fatalf("steady-state Observe should hit the candidate; got=%v", trace)
	}
}

// TestCreateMarksPostWrite asserts Create records the MR as post-write so the
// next Observe enters the convergence gate.
func TestCreateMarksPostWrite(t *testing.T) {
	resetForTest()

	var trace []string
	e := &external{
		write:      recordingClient("write", &trace),
		read:       recordingClient("read", &trace),
		ots:        newOTS(),
		sameClient: false,
		logger:     logging.NewNopLogger(),
	}
	mg := newTracked("marks")
	if _, err := e.Create(context.Background(), mg); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !isPostWrite(mg.GetUID()) {
		t.Fatalf("Create should mark the MR post-write")
	}
}

// TestConvergenceGate drives the post-write convergence gate through its phases
// using a candidate whose Observe result flips from not-caught-up to caught-up:
//
//	(a) post-write, candidate not-exists     -> Observe served from primary, marker stays
//	(b) post-write, candidate not-up-to-date -> Observe served from primary, marker stays
//	(c) candidate exists & up-to-date        -> Observe served from candidate, marker cleared
//	(d) steady state                         -> Observe served from candidate
func TestConvergenceGate(t *testing.T) {
	resetForTest()

	var trace []string
	// A mutable candidate observation the read client returns on each Observe.
	candObs := managed.ExternalObservation{ResourceExists: false}
	read := managed.ExternalClientFns{
		ObserveFn: func(_ context.Context, _ xpresource.Managed) (managed.ExternalObservation, error) {
			trace = append(trace, "read:Observe")
			return candObs, nil
		},
	}
	e := &external{
		write:      recordingClient("write", &trace),
		read:       read,
		ots:        newOTS(),
		sameClient: false,
		logger:     logging.NewNopLogger(),
	}
	mg := newTracked("converge")
	ctx := context.Background()

	// Enter the gate as a Create would.
	seedPostWrite(mg.GetUID())

	// (a) candidate reports not-exists -> probe candidate, fall back to primary.
	if _, err := e.Observe(ctx, mg); err != nil {
		t.Fatalf("Observe (a): %v", err)
	}
	if !isPostWrite(mg.GetUID()) {
		t.Fatalf("(a) marker must remain while candidate is stale")
	}

	// (b) candidate exists but drifted -> still primary, marker remains.
	candObs = managed.ExternalObservation{ResourceExists: true, ResourceUpToDate: false}
	if _, err := e.Observe(ctx, mg); err != nil {
		t.Fatalf("Observe (b): %v", err)
	}
	if !isPostWrite(mg.GetUID()) {
		t.Fatalf("(b) marker must remain while candidate is not up-to-date")
	}

	// (c) candidate caught up -> serve from candidate, clear the marker.
	candObs = managed.ExternalObservation{ResourceExists: true, ResourceUpToDate: true}
	if _, err := e.Observe(ctx, mg); err != nil {
		t.Fatalf("Observe (c): %v", err)
	}
	if isPostWrite(mg.GetUID()) {
		t.Fatalf("(c) marker must clear once the candidate has caught up")
	}

	// (d) steady state -> candidate.
	if _, err := e.Observe(ctx, mg); err != nil {
		t.Fatalf("Observe (d): %v", err)
	}

	want := []string{
		"read:Observe",  // (a) probe candidate (not-exists)
		"write:Observe", // (a) serve truth from primary
		"read:Observe",  // (b) probe candidate (drifted)
		"write:Observe", // (b) serve truth from primary
		"read:Observe",  // (c) probe candidate (caught up) -> served
		"read:Observe",  // (d) steady state -> candidate
	}
	if !equal(trace, want) {
		t.Fatalf("convergence-gate routing mismatch:\n got=%v\nwant=%v", trace, want)
	}
}

// TestIPAMAlwaysPrimary asserts a non-DNS (IPAM) resource always Observes from
// the primary — in steady state and even if it were marked post-write — because
// only DNS records are offloaded to the candidate.
func TestIPAMAlwaysPrimary(t *testing.T) {
	resetForTest()

	var trace []string
	e := &external{
		write:      recordingClient("write", &trace),
		read:       recordingClient("read", &trace),
		ots:        newOTS(),
		sameClient: false,
		logger:     logging.NewNopLogger(),
	}
	mg := newTrackedGroup("ipam", "ipam.infoblox-nios.crossplane.io", "Network")
	ctx := context.Background()

	// Steady state: still primary because it is not a DNS group.
	if _, err := e.Observe(ctx, mg); err != nil {
		t.Fatalf("Observe (steady): %v", err)
	}
	// Even seeded post-write, IPAM never probes the candidate.
	seedPostWrite(mg.GetUID())
	if _, err := e.Observe(ctx, mg); err != nil {
		t.Fatalf("Observe (post-write): %v", err)
	}

	if !equal(trace, []string{"write:Observe", "write:Observe"}) {
		t.Fatalf("IPAM Observe must always hit the primary; got=%v", trace)
	}
}

// TestAsyncInFlightServesPrimary asserts that while an async write is in flight
// on the shared tracker, Observe is served from the primary and the candidate is
// NOT probed — even for a DNS resource marked post-write.
func TestAsyncInFlightServesPrimary(t *testing.T) {
	resetForTest()

	var trace []string
	ots := newOTS()
	mg := newTracked("inflight")
	e := &external{
		write:      recordingClient("write", &trace),
		read:       recordingClient("read", &trace),
		ots:        ots,
		sameClient: false,
		logger:     logging.NewNopLogger(),
	}
	seedPostWrite(mg.GetUID())
	if ok := ots.Tracker(mg).LastOperation.MarkStart("update"); !ok {
		t.Fatalf("MarkStart should succeed on a fresh operation")
	}

	if _, err := e.Observe(context.Background(), mg); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if !equal(trace, []string{"write:Observe"}) {
		t.Fatalf("in-flight Observe must hit primary only (candidate not probed); got=%v", trace)
	}
}

// TestSameClientObserveDelegatesToWrite asserts the read-connect fallback path:
// when read==write, Observe delegates straight to the write client without
// probing or panicking, regardless of DNS scope or post-write state.
func TestSameClientObserveDelegatesToWrite(t *testing.T) {
	resetForTest()

	var trace []string
	shared := recordingClient("write", &trace)
	e := &external{
		write:      shared,
		read:       shared,
		ots:        newOTS(),
		sameClient: true,
		logger:     logging.NewNopLogger(),
	}
	mg := newTracked("same")
	seedPostWrite(mg.GetUID())
	if _, err := e.Observe(context.Background(), mg); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if !equal(trace, []string{"write:Observe"}) {
		t.Fatalf("sameClient Observe should delegate to write; got=%v", trace)
	}
}

// TestDeleteClearsPostWrite asserts Delete drops the per-UID post-write marker so
// a recreated MR does not inherit a stale convergence window.
func TestDeleteClearsPostWrite(t *testing.T) {
	resetForTest()

	var trace []string
	e := &external{
		write:      recordingClient("write", &trace),
		read:       recordingClient("read", &trace),
		ots:        newOTS(),
		sameClient: false,
		logger:     logging.NewNopLogger(),
	}
	mg := newTracked("del")
	ctx := context.Background()

	seedPostWrite(mg.GetUID())
	if _, err := e.Delete(ctx, mg); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if isPostWrite(mg.GetUID()) {
		t.Fatalf("Delete should clear the post-write marker")
	}

	// After Delete the MR reads from the candidate again (steady state).
	trace = nil
	if _, err := e.Observe(ctx, mg); err != nil {
		t.Fatalf("Observe post-delete: %v", err)
	}
	if !equal(trace, []string{"read:Observe"}) {
		t.Fatalf("post-delete Observe should hit the candidate; got=%v", trace)
	}
}

// TestWrapConnectorNoOpIdentity asserts that when the split is not Configured,
// WrapConnector returns the write connecter unchanged (same value), so behavior
// is byte-for-byte identical to the single-endpoint provider.
func TestWrapConnectorNoOpIdentity(t *testing.T) {
	resetForTest() // configured == false

	var write managed.ExternalConnecter = &fakeConnecter{}
	got := WrapConnector(nil, &ujconfig.Resource{}, write, logging.NewNopLogger())
	if got != write {
		t.Fatalf("expected WrapConnector to return the write connecter identity when not configured; got %T (%p) want %p", got, got, write)
	}
}

// TestConnectFallbackOnReadError asserts that when the read endpoint fails to
// connect, the reconcile degrades to single-endpoint behavior: reads for that
// cycle are served from the write (primary) client rather than failing.
func TestConnectFallbackOnReadError(t *testing.T) {
	resetForTest()

	var trace []string
	c := &connector{
		write:  &fakeConnecter{client: recordingClient("write", &trace)},
		read:   &fakeConnecter{err: errors.New("candidate unreachable")},
		ots:    newOTS(),
		logger: logging.NewNopLogger(),
	}

	ec, err := c.Connect(context.Background(), newTracked("fallback"))
	if err != nil {
		t.Fatalf("Connect should not fail when only the read endpoint is down: %v", err)
	}
	ext, ok := ec.(*external)
	if !ok {
		t.Fatalf("expected *external, got %T", ec)
	}
	if !ext.sameClient {
		t.Fatalf("expected sameClient=true after read connect failure")
	}

	// Observe must be served from the write client in the fallback state.
	if _, err := ext.Observe(context.Background(), newTracked("fallback")); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	want := []string{"write:Observe"}
	if !equal(trace, want) {
		t.Fatalf("fallback Observe should hit write client:\n got=%v\nwant=%v", trace, want)
	}
}

// TestConnectHardFailsOnWriteError asserts a write-endpoint connect failure is
// propagated (not swallowed) — only the read endpoint degrades gracefully.
func TestConnectHardFailsOnWriteError(t *testing.T) {
	resetForTest()

	c := &connector{
		write:  &fakeConnecter{err: errors.New("primary unreachable")},
		read:   &fakeConnecter{client: recordingClient("read", new([]string))},
		ots:    newOTS(),
		logger: logging.NewNopLogger(),
	}
	if _, err := c.Connect(context.Background(), newTracked("writefail")); err == nil {
		t.Fatalf("expected Connect to fail when the write endpoint is unreachable")
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
