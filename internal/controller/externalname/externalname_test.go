package externalname

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// newTestScheme returns a scheme with corev1 registered — Secret stands
// in for a managed resource here since Refresh only depends on the
// generic client.Object/metav1.Object interfaces, not on any
// provider-specific type.
func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("cannot build test scheme: %v", err)
	}
	return s
}

// TestRefreshPersistsAnnotationViaKubeClient is the regression guard for
// the external-name-refresh-after-update bug: Refresh must not just
// mutate the in-memory object, it must call through to a real client to
// persist the annotation. A test that only asserted
// meta.GetExternalName(obj) after calling Refresh would pass even if
// Refresh never touched the API server at all — exactly the false sense
// of coverage the original Update()-only fix produced.
func TestRefreshPersistsAnnotationViaKubeClient(t *testing.T) {
	obj := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "my-object", Namespace: "default"},
	}
	meta.SetExternalName(obj, "old-ref")

	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(obj).Build()

	if err := Refresh(context.Background(), kube, obj, "new-ref"); err != nil {
		t.Fatalf("Refresh: unexpected error: %v", err)
	}

	// The in-memory object must reflect the new name...
	if got := meta.GetExternalName(obj); got != "new-ref" {
		t.Errorf("Refresh: in-memory external-name = %q, want %q", got, "new-ref")
	}

	// ...but the real assertion is that a fresh Get from the API server
	// (a distinct object instance, not the one Refresh mutated in place)
	// also sees it. This proves persistence went through the kube
	// client, not just a local field assignment.
	fetched := &corev1.Secret{}
	if err := kube.Get(context.Background(), types.NamespacedName{Name: "my-object", Namespace: "default"}, fetched); err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	if got := meta.GetExternalName(fetched); got != "new-ref" {
		t.Errorf("Refresh: persisted external-name = %q, want %q", got, "new-ref")
	}
}

// TestRefreshPreservesUnrelatedFields proves the persist is scoped to the
// external-name annotation: a merge Update (the original, buggy
// implementation) would round-trip the entire cached object, including
// any spec/annotation drift already present on the in-memory copy versus
// the server. A merge patch must leave every field it doesn't mention —
// other annotations, spec — exactly as the server already has them.
func TestRefreshPreservesUnrelatedFields(t *testing.T) {
	stored := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-object",
			Namespace: "default",
			Annotations: map[string]string{
				"crossplane.io/external-name": "old-ref",
				"keep-me":                     "server-value",
			},
		},
		StringData: map[string]string{"keep-me-too": "server-value"},
	}

	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(stored).Build()

	// cr is a stale, separately-mutated in-memory copy — simulating a
	// concurrent writer having changed the server-side object after cr
	// was read, but before Refresh runs.
	cr := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-object",
			Namespace: "default",
			Annotations: map[string]string{
				"crossplane.io/external-name": "old-ref",
			},
		},
	}

	if err := Refresh(context.Background(), kube, cr, "new-ref"); err != nil {
		t.Fatalf("Refresh: unexpected error: %v", err)
	}

	fetched := &corev1.Secret{}
	if err := kube.Get(context.Background(), types.NamespacedName{Name: "my-object", Namespace: "default"}, fetched); err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}

	if got := meta.GetExternalName(fetched); got != "new-ref" {
		t.Errorf("Refresh: persisted external-name = %q, want %q", got, "new-ref")
	}
	if got := fetched.Annotations["keep-me"]; got != "server-value" {
		t.Errorf("Refresh: clobbered unrelated annotation, got %q, want %q", got, "server-value")
	}
	if got := fetched.StringData["keep-me-too"]; got != "server-value" {
		t.Errorf("Refresh: clobbered unrelated field, got %q, want %q", got, "server-value")
	}
}

// conflictOnceClient wraps a client.Client and forces the FIRST call to
// Patch to fail with an optimistic-concurrency conflict (HTTP 409),
// regardless of what the underlying fake client would otherwise do. It
// exists to reproduce the exact failure this ticket is about: a write
// that loses a concurrency race must not be allowed to simply surface
// the error and give up, because by the time that race is lost the
// external system has already committed the corresponding change and
// the in-flight write is the only remaining copy of the new identity.
type conflictOnceClient struct {
	client.Client
	patchAttempts int
}

func (c *conflictOnceClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	c.patchAttempts++
	if c.patchAttempts == 1 {
		gr := schema.GroupResource{Group: "", Resource: "secrets"}
		return apierrors.NewConflict(gr, obj.GetName(), stderrors.New("optimistic lock error: stale resourceVersion"))
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

// TestRefreshSurvivesLostConflict is the primary regression test for this
// ticket: a lost optimistic-concurrency race on the FIRST persist attempt
// must not cause the refreshed external name to be dropped. It must
// still land — verified via a re-GET into a distinct object instance,
// not by inspecting the object Refresh mutated in place — because that
// second check is the only one that would have failed against the
// original bare kube.Update() implementation.
func TestRefreshSurvivesLostConflict(t *testing.T) {
	obj := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "my-object", Namespace: "default"},
	}
	meta.SetExternalName(obj, "old-ref")

	base := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(obj).Build()
	kube := &conflictOnceClient{Client: base}

	if err := Refresh(context.Background(), kube, obj, "new-ref"); err != nil {
		t.Fatalf("Refresh: unexpected error after a single lost conflict: %v", err)
	}

	if kube.patchAttempts < 2 {
		t.Fatalf("Refresh: want a retry after the first conflict, got only %d patch attempt(s)", kube.patchAttempts)
	}

	fetched := &corev1.Secret{}
	if err := base.Get(context.Background(), types.NamespacedName{Name: "my-object", Namespace: "default"}, fetched); err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	if got := meta.GetExternalName(fetched); got != "new-ref" {
		t.Errorf("Refresh: persisted external-name after a lost conflict = %q, want %q", got, "new-ref")
	}
}

// TestRefreshPropagatesPersistError ensures a non-conflict failure (e.g.
// the object no longer exists) is surfaced to the caller rather than
// silently swallowed — callers wrap this in an errors.Wrap and fail the
// reconcile so the stale annotation doesn't linger unnoticed. Unlike a
// conflict, a NotFound is not retried: there is nothing to retry against.
func TestRefreshPropagatesPersistError(t *testing.T) {
	obj := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "does-not-exist", Namespace: "default"},
	}

	// No WithObjects — the fake client's tracker has nothing to patch,
	// so Patch returns a NotFound error, mirroring a real API server
	// rejecting a patch to an object that no longer exists.
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build()

	if err := Refresh(context.Background(), kube, obj, "new-ref"); err == nil {
		t.Fatal("Refresh: want error when the underlying persist call fails, got nil")
	}
}

var _ client.Object = &corev1.Secret{}
var _ client.Client = &conflictOnceClient{}
