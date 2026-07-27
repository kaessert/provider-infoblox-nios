/*
Copyright 2021 Upbound Inc.
*/

package split

import (
	"context"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/pkg/logging"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	xpresource "github.com/crossplane/crossplane-runtime/pkg/resource"
	ujconfig "github.com/crossplane/upjet/pkg/config"
	tjcontroller "github.com/crossplane/upjet/pkg/controller"
	"github.com/crossplane/upjet/pkg/resource/fake"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/types"
)

// recordingClient is a managed.ExternalClient that records, into a shared trace
// slice, which underlying endpoint ("read"/"write") served each call.
func recordingClient(label string, trace *[]string) managed.ExternalClient {
	return managed.ExternalClientFns{
		ObserveFn: func(_ context.Context, _ xpresource.Managed) (managed.ExternalObservation, error) {
			*trace = append(*trace, label+":Observe")
			return managed.ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
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

func newTracked(uid string) *fake.Terraformed {
	tr := &fake.Terraformed{}
	tr.SetUID(types.UID(uid))
	tr.SetName("mr-" + uid)
	return tr
}

func newOTS() *tjcontroller.OperationTrackerStore {
	return tjcontroller.NewOperationStore(logging.NewNopLogger())
}

// TestExternalRouting asserts each managed.ExternalClient verb is routed to the
// correct endpoint: Observe->read (no write in flight, no grace), the mutating
// verbs->write, and Disconnect->both. Create and Update first prime the write
// client with its own Observe (against the primary), because upjet computes the
// Terraform instance diff during Observe and consumes it during Create/Update;
// since the reconciler's Observe was routed to the read client, the write client
// must observe before it can apply a mutation.
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
		"read:Observe",  // reconciler Observe -> read (candidate)
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

// TestGraceWindowStateMachine drives the read-after-write grace state machine
// through its three phases using the injectable clock and a real shared
// OperationTrackerStore:
//
//	(a) write in flight            -> Observe routed to primary (write)
//	(b) just completed, in window  -> Observe routed to primary (write)
//	(c) window elapsed             -> Observe routed back to candidate (read)
func TestGraceWindowStateMachine(t *testing.T) {
	resetForTest()

	// Freeze the clock; advance it explicitly per phase.
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	current := base
	now = func() time.Time { return current }
	t.Cleanup(func() { now = time.Now })

	GraceWindow = 30 * time.Second

	var trace []string
	ots := newOTS()
	mg := newTracked("grace")
	e := &external{
		write:      recordingClient("write", &trace),
		read:       recordingClient("read", &trace),
		ots:        ots,
		sameClient: false,
		logger:     logging.NewNopLogger(),
	}
	ctx := context.Background()

	// Phase (a): mark an async write in flight via the shared tracker.
	if ok := ots.Tracker(mg).LastOperation.MarkStart("update"); !ok {
		t.Fatalf("MarkStart should succeed on a fresh operation")
	}
	if _, err := e.Observe(ctx, mg); err != nil {
		t.Fatalf("Observe (a): %v", err)
	}

	// Phase (b): the async write completes; first Observe after completion opens
	// the grace window and still routes to primary.
	ots.Tracker(mg).LastOperation.MarkEnd()
	if _, err := e.Observe(ctx, mg); err != nil {
		t.Fatalf("Observe (b): %v", err)
	}
	// Still within the window (clock not advanced): another Observe -> primary.
	if _, err := e.Observe(ctx, mg); err != nil {
		t.Fatalf("Observe (b'): %v", err)
	}

	// Phase (c): advance the clock past the grace window; reads return to the
	// candidate.
	current = base.Add(GraceWindow + time.Second)
	if _, err := e.Observe(ctx, mg); err != nil {
		t.Fatalf("Observe (c): %v", err)
	}

	want := []string{
		"write:Observe", // (a) in flight
		"write:Observe", // (b) just completed -> open window
		"write:Observe", // (b') still in window
		"read:Observe",  // (c) window elapsed -> back to candidate
	}
	if !equal(trace, want) {
		t.Fatalf("grace-window routing mismatch:\n got=%v\nwant=%v", trace, want)
	}
}

// TestDeleteClearsGraceState asserts Delete drops per-UID grace state so a
// recreated MR does not inherit a stale grace window.
func TestDeleteClearsGraceState(t *testing.T) {
	resetForTest()

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now = func() time.Time { return base }
	t.Cleanup(func() { now = time.Now })

	var trace []string
	ots := newOTS()
	mg := newTracked("del")
	e := &external{
		write:      recordingClient("write", &trace),
		read:       recordingClient("read", &trace),
		ots:        ots,
		sameClient: false,
		logger:     logging.NewNopLogger(),
	}
	ctx := context.Background()

	// Open a grace window (write ran, then ended).
	ots.Tracker(mg).LastOperation.MarkStart("update")
	if _, err := e.Observe(ctx, mg); err != nil { // in flight
		t.Fatal(err)
	}
	ots.Tracker(mg).LastOperation.MarkEnd()
	if _, err := e.Observe(ctx, mg); err != nil { // opens window
		t.Fatal(err)
	}

	if _, err := e.Delete(ctx, mg); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// After Delete the per-UID state is gone. Clear the tracker too so nothing
	// is running, then Observe must route to the candidate (fresh state).
	ots.Tracker(mg).LastOperation.Clear(false)
	trace = nil
	if _, err := e.Observe(ctx, mg); err != nil {
		t.Fatalf("Observe post-delete: %v", err)
	}
	if !equal(trace, []string{"read:Observe"}) {
		t.Fatalf("post-delete Observe should hit read (grace state cleared); got=%v", trace)
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
