package externalname

import (
	"context"
	"testing"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
	// also sees it. This proves persistence went through kube.Update,
	// not just a local field assignment.
	fetched := &corev1.Secret{}
	if err := kube.Get(context.Background(), types.NamespacedName{Name: "my-object", Namespace: "default"}, fetched); err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	if got := meta.GetExternalName(fetched); got != "new-ref" {
		t.Errorf("Refresh: persisted external-name = %q, want %q", got, "new-ref")
	}
}

// TestRefreshPropagatesUpdateError ensures a kube.Update failure (e.g. a
// resource-version conflict) is surfaced to the caller rather than
// silently swallowed — callers wrap this in an errors.Wrap and fail the
// reconcile so the stale annotation doesn't linger unnoticed.
func TestRefreshPropagatesUpdateError(t *testing.T) {
	obj := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "does-not-exist", Namespace: "default"},
	}

	// No WithObjects — the fake client's tracker has nothing to update,
	// so Update returns a NotFound error, mirroring a real API server
	// rejecting an update to an object that no longer exists.
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build()

	if err := Refresh(context.Background(), kube, obj, "new-ref"); err == nil {
		t.Fatal("Refresh: want error when the underlying kube.Update fails, got nil")
	}
}

var _ client.Object = &corev1.Secret{}
