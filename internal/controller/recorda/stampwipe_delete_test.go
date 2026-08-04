// Regression coverage for the "stamp wiped mid-delete" gap: a managed
// resource whose stored reference has rotated or gone stale, deleted
// while the identity extensible attribute definition is (or was
// recently) absent, must never have its Kubernetes finalizer cleared
// without the backend object either being genuinely confirmed gone or
// actually deleted.
//
// crossplane-runtime's managed reconciler only calls external.Delete()
// when the immediately preceding Observe() reports ResourceExists: true
// (see pkg/reconciler/managed/reconciler.go, the meta.WasDeleted branch).
// Before this fix, observeARecord treated a 0-match identity-EA search —
// reached whenever the stored reference 404s — identically whether or
// not the caller had ever previously resolved a real object: it always
// reported ResourceExists: false, so a delete in flight against a
// resource whose stamp was wiped (and whose reference is therefore also
// stale) would never reach Delete()'s ownership-verification logic at
// all. crossplane-runtime would then clear the finalizer as if the
// delete had completed successfully, permanently orphaning the live Grid
// object with no error, no event and no trace.
//
// Mutation check (recorded manually, not committed as an automated test
// per the guard that a probe must fail when the behavior it proves is
// disabled): observeARecord's new guard —
//
//	if wasDeleted && ref != "" {
//	    return observeResult{}, errors.New(errAmbiguousDeleteState)
//	}
//
// — was temporarily commented out (falling through to the pre-fix
// `return observeResult{exists: false}, nil`) before running this file's
// two "ambiguous" tests. Both failed — "Observe: want an error when a
// delete is in flight and the identity ladder cannot find a
// previously-known object, got nil (ResourceExists=false)" — while every
// other test in this package continued to pass. The mutation was
// reverted immediately after confirming the failure; it was never
// committed.
package recorda

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
)

// withDeletionTimestamp stamps o with a non-zero deletionTimestamp so
// meta.WasDeleted(o) reports true, exactly as crossplane-runtime leaves a
// managed resource once a delete has been requested against it.
func withDeletionTimestamp(t metav1.ObjectMeta) metav1.ObjectMeta {
	now := metav1.NewTime(time.Now())
	t.DeletionTimestamp = &now
	return t
}

// ── cluster ──────────────────────────────────────────────────────────────

// TestClusterObserveDuringDeleteIsAmbiguousAfterStampWipe reproduces the
// live-verified defect directly: a delete is in flight, the stored
// reference is stale (as a real rotation or an admin-forced annotation
// would leave it), and the Grid carries no object stamped with this
// managed resource's uid (as a stamp wipe — the identity extensible
// attribute definition deleted and recreated — would leave it). Observe
// must refuse to report a clean ResourceExists: false here; crossplane-
// runtime would otherwise clear the finalizer without ever calling
// Delete().
func TestClusterObserveDuringDeleteIsAmbiguousAfterStampWipe(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	// Deliberately nothing seeded: the identity-EA search for this uid
	// legitimately matches zero objects, exactly what a stamp wipe (or a
	// genuine prior delete) looks like.
	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	staleRef := "record:a/stale-ref:host.example.com/default"
	cr := newClusterARecord("my-arecord", staleRef)
	cr.ObjectMeta = withDeletionTimestamp(cr.ObjectMeta)

	got, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatalf("Observe: want an error when a delete is in flight and the identity ladder cannot find a previously-known object, got nil (ResourceExists=%v)", got.ResourceExists)
	}
	if !strings.Contains(err.Error(), "cannot verify deletion") {
		t.Errorf("Observe: error = %v, want it to explain that deletion cannot be verified", err)
	}
	if got.ResourceExists {
		t.Error("Observe: ResourceExists=true on the error path, want the zero value")
	}
}

// TestClusterObserveDuringDeleteWithNoPriorRefIsCleanNotFound proves the
// guard above is scoped correctly: a managed resource that never
// recorded a real reference (the framework's NameAsExternalName default,
// i.e. it was never actually created on the Grid) must still report a
// clean ResourceExists: false during delete — there is nothing to have
// silently abandoned, and finalizer removal must proceed normally.
func TestClusterObserveDuringDeleteWithNoPriorRefIsCleanNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	cr := newClusterARecord("my-arecord", "") // external-name unset
	meta.SetExternalName(cr, cr.GetName())    // simulate NameAsExternalName initializer — pre-create state
	cr.ObjectMeta = withDeletionTimestamp(cr.ObjectMeta)

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error for a resource that was never created: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false for a pre-create resource being deleted, got true")
	}
}

// TestClusterObserveNotFoundDuringDeleteWithoutStampWipeStillDeletes
// proves the fix does not regress the ordinary, already-covered delete
// path: when the reference resolves directly (no stamp-wipe ambiguity at
// all), Observe still reports the object as existing and Delete still
// succeeds normally during a real deletion.
func TestClusterObserveNotFoundDuringDeleteWithoutStampWipeStillDeletes(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		Ea:       identity.Stamp(nil, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	cr := newClusterARecord("my-arecord", ref)
	cr.ObjectMeta = withDeletionTimestamp(cr.ObjectMeta)

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error for a reference that resolves directly: %v", err)
	}
	if !got.ResourceExists {
		t.Fatal("Observe: want ResourceExists=true for a reference that resolves directly, got false")
	}

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
	if got := searchGridByIdentity(t, srv, testUIDCluster); len(got) != 0 {
		t.Fatalf("post-delete WAPI identity search: got %d matches, want 0", len(got))
	}
}

// ── namespaced ───────────────────────────────────────────────────────────

func TestNamespacedObserveDuringDeleteIsAmbiguousAfterStampWipe(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	staleRef := "record:a/stale-ref:host.example.com/default"
	cr := newNamespacedARecord("default", "my-arecord", staleRef, "ProviderConfig")
	cr.ObjectMeta = withDeletionTimestamp(cr.ObjectMeta)

	got, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatalf("Observe: want an error when a delete is in flight and the identity ladder cannot find a previously-known object, got nil (ResourceExists=%v)", got.ResourceExists)
	}
	if !strings.Contains(err.Error(), "cannot verify deletion") {
		t.Errorf("Observe: error = %v, want it to explain that deletion cannot be verified", err)
	}
	if got.ResourceExists {
		t.Error("Observe: ResourceExists=true on the error path, want the zero value")
	}
}

func TestNamespacedObserveDuringDeleteWithNoPriorRefIsCleanNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	cr := newNamespacedARecord("default", "my-arecord", "", "ProviderConfig")
	meta.SetExternalName(cr, cr.GetName())
	cr.ObjectMeta = withDeletionTimestamp(cr.ObjectMeta)

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error for a resource that was never created: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false for a pre-create resource being deleted, got true")
	}
}
