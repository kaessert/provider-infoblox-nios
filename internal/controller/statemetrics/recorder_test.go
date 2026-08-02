package statemetrics

import (
	"context"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	"github.com/crossplane/crossplane-runtime/v2/pkg/statemetrics"
)

// fakeManagedList is a minimal resource.ManagedList implementation used to
// drive statemetrics.MRStateRecorder in tests without depending on any
// generated resource type.
type fakeManagedList struct {
	metav1.ListMeta
}

func (l *fakeManagedList) GetObjectKind() schema.ObjectKind { return &fakeObjectKind{} }

func (l *fakeManagedList) DeepCopyObject() runtime.Object {
	return &fakeManagedList{ListMeta: l.ListMeta}
}

func (l *fakeManagedList) GetItems() []resource.Managed { return nil }

// fakeObjectKind is a no-op schema.ObjectKind — the recorder never inspects
// it directly (GVK resolution goes through the scheme), it only needs to
// satisfy runtime.Object.
type fakeObjectKind struct{}

func (fakeObjectKind) SetGroupVersionKind(_ schema.GroupVersionKind) {}
func (fakeObjectKind) GroupVersionKind() schema.GroupVersionKind     { return schema.GroupVersionKind{} }

// fakeClient implements just enough of client.Client to drive
// statemetrics.MRStateRecorder.Record: List and Scheme. Every other method
// is unused by the recorder and left unimplemented (embedding a nil
// client.Client would panic if called, which is the desired failure mode
// for an unexpected dependency on it).
type fakeClient struct {
	client.Client

	results []error
	calls   int
}

func (f *fakeClient) List(_ context.Context, _ client.ObjectList, _ ...client.ListOption) error {
	i := f.calls
	if i >= len(f.results) {
		i = len(f.results) - 1
	}
	if f.calls < len(f.results)-1 {
		f.calls++
	}
	return f.results[i]
}

func (f *fakeClient) Scheme() *runtime.Scheme {
	s := runtime.NewScheme()
	s.AddKnownTypeWithName(schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "FooList"}, &fakeManagedList{})

	return s
}

// TestResilientRecorderRetriesOnNoKindMatch proves that a "no matches for
// kind" error — the RESTMapper not yet having caught up with a
// just-established CRD — is retried on the next tick instead of aborting
// Start, and that Start returns once the same List call starts succeeding.
func TestResilientRecorderRetriesOnNoKindMatch(t *testing.T) {
	fc := &fakeClient{
		results: []error{
			&apimeta.NoKindMatchError{GroupKind: schema.GroupKind{Group: "recordcname.infobloxnios.crossplane.io", Kind: "CNAMERecord"}},
			nil,
		},
	}

	metrics := statemetrics.NewMRStateMetrics()
	rec := NewResilientRecorder(fc, logging.NewNopLogger(), metrics, &fakeManagedList{}, 5*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- rec.Start(ctx) }()

	// Wait for the retry to happen (first tick fails, second succeeds),
	// then cancel so Start returns cleanly.
	deadline := time.Now().Add(150 * time.Millisecond)
	for fc.calls < 1 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	cancel()

	if err := <-errCh; err != nil {
		t.Fatalf("Start(...): unexpected error, should have retried past the no-match error: %v", err)
	}

	if fc.calls < 1 {
		t.Fatalf("List was not retried after a no-match error: calls=%d", fc.calls)
	}
}

// TestResilientRecorderPropagatesOtherErrors proves that errors other than
// a RESTMapper no-match are not swallowed — Start must still surface real
// failures rather than retry forever.
func TestResilientRecorderPropagatesOtherErrors(t *testing.T) {
	wantErr := errNewForTest("boom")

	fc := &fakeClient{results: []error{wantErr}}

	metrics := statemetrics.NewMRStateMetrics()
	rec := NewResilientRecorder(fc, logging.NewNopLogger(), metrics, &fakeManagedList{}, 5*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := rec.Start(ctx)
	if err == nil {
		t.Fatalf("Start(...): expected a non-nil error to propagate, got nil")
	}
}

type testError string

func (e testError) Error() string { return string(e) }

func errNewForTest(msg string) error { return testError(msg) }
