// Identity ladder matrix tests for the NetworkView MR controllers,
// covering the rows and refusal paths convention 0004/0107 require but
// controller_test.go's original suite predates: namespaced-scope
// coverage for every ladder row, the two typed refusals surfaced via
// errors.As, the reactive identity-prerequisite probe, the full
// convention-0004 category set (server error / minimal response), and a
// genuine round-trip persistence proof for the Update()-triggered
// rename-rotates-_ref path (NetworkView's _ref embeds its name, so a
// rename really does mint a new _ref — unlike the immutable-identity
// resources in this catalog, this is not a defensive no-op).
package networkview

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/networkview/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/networkview/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
)

// fixedStatusHandler always responds with the given HTTP status — used
// to simulate a server error / forbidden response for an entire request
// path (e.g. a ref-GET that returns 500 before any identity search is
// ever attempted).
func fixedStatusHandler(status int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	})
}

// ── Namespaced: core ladder rows ──────────────────────────────────────────

func TestNamespacedObserveResolvedUpToDate(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nv := &ibclient.NetworkView{Name: stringPtr("test-view"), Comment: stringPtr("c")}
	nv.Ea = identity.Stamp(nil, testUIDNamespaced)
	ref := m.seed(nv)

	cr := newNamespacedNetworkView("ns", "my-view", ref, "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("c")
	e := &namespacedExternal{objMgr: mc.Manager, conn: mc.Connector}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !obs.ResourceExists || !obs.ResourceUpToDate {
		t.Fatalf("Observe: expected exists+up-to-date, got %+v", obs)
	}
}

func TestNamespacedObserveNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newNamespacedNetworkView("ns", "my-view", "networkview/doesnotexist:my-view", "ProviderConfig")
	e := &namespacedExternal{objMgr: mc.Manager, conn: mc.Connector}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if obs.ResourceExists {
		t.Fatalf("Observe: expected ResourceExists=false, got %+v", obs)
	}
}

func TestNamespacedObservePreCreateState(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newNamespacedNetworkView("ns", "my-view", "my-view", "ProviderConfig")
	e := &namespacedExternal{objMgr: mc.Manager, conn: mc.Connector}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if obs.ResourceExists {
		t.Fatalf("Observe: expected ResourceExists=false, got %+v", obs)
	}
}

func TestNamespacedObserveAdoptsUnstampedObjectAndForcesUpdate(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nv := &ibclient.NetworkView{Name: stringPtr("test-view"), Comment: stringPtr("c")}
	ref := m.seed(nv)

	cr := newNamespacedNetworkView("ns", "my-view", ref, "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("c")
	e := &namespacedExternal{objMgr: mc.Manager, conn: mc.Connector}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !obs.ResourceExists {
		t.Fatalf("Observe: expected exists, got %+v", obs)
	}
	if obs.ResourceUpToDate {
		t.Fatal("Observe: adopted object must never report up to date, even though every user-facing field matches")
	}
}

func TestNamespacedObserveRecoversRotatedRefAndPersistsAnnotation(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nv := &ibclient.NetworkView{Name: stringPtr("test-view")}
	nv.Ea = identity.Stamp(nil, testUIDNamespaced)
	realRef := m.seed(nv)

	cr := newNamespacedNetworkView("ns", "my-view", "networkview/stale:my-view", "ProviderConfig")
	e := &namespacedExternal{objMgr: mc.Manager, conn: mc.Connector}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !obs.ResourceExists || !obs.ResourceLateInitialized {
		t.Fatalf("Observe: expected exists+late-init so the refreshed reference is persisted, got %+v", obs)
	}
	if got := meta.GetExternalName(cr); got != realRef {
		t.Fatalf("Observe: external-name = %q, want the recovered reference %q", got, realRef)
	}
}

func TestNamespacedObserveRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nv := &ibclient.NetworkView{Name: stringPtr("test-view")}
	nv.Ea = identity.Stamp(nil, "someone-elses-uid")
	ref := m.seed(nv)

	cr := newNamespacedNetworkView("ns", "my-view", ref, "ProviderConfig")
	e := &namespacedExternal{objMgr: mc.Manager, conn: mc.Connector}

	_, err := e.Observe(context.Background(), cr)
	var reuse *identity.HandleReuseError
	if !errors.As(err, &reuse) {
		t.Fatalf("Observe: error = %v (%T), want a *identity.HandleReuseError", err, err)
	}
}

// ── Cluster/Namespaced: typed refusals via errors.As ──────────────────────

// TestClusterObserveRefusesOnForeignIdentityTyped strengthens the
// original TestClusterObserveRefusesOnForeignIdentity: convention 0107
// requires callers to be able to classify this refusal structurally
// (errors.As), not merely observe that Observe returned some error.
func TestClusterObserveRefusesOnForeignIdentityTyped(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nv := &ibclient.NetworkView{Name: stringPtr("test-view")}
	nv.Ea = identity.Stamp(nil, "someone-elses-uid")
	ref := m.seed(nv)

	cr := newClusterNetworkView("my-view", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	_, err := e.Observe(context.Background(), cr)
	var reuse *identity.HandleReuseError
	if !errors.As(err, &reuse) {
		t.Fatalf("Observe: error = %v (%T), want a *identity.HandleReuseError", err, err)
	}
}

func TestClusterObserveRefusesOnAmbiguousMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	// Two objects stamped with the same uid but a sibling name sorts
	// first for at least some map iterations — the fail-closed refusal
	// must hold regardless of iteration order (see convention 0107's
	// note on Go map iteration order flushing out first-match bugs).
	m.seed(&ibclient.NetworkView{Name: stringPtr("view-a"), Ea: identity.Stamp(nil, testUIDCluster)})
	m.seed(&ibclient.NetworkView{Name: stringPtr("view-b"), Ea: identity.Stamp(nil, testUIDCluster)})

	cr := newClusterNetworkView("my-view", "networkview/stale:my-view")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	_, err := e.Observe(context.Background(), cr)
	var ambiguous *identity.AmbiguousMatchError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("Observe: error = %v (%T), want a *identity.AmbiguousMatchError — ambiguity must fail closed, never resolve to an arbitrary first match", err, err)
	}
}

func TestNamespacedObserveRefusesOnAmbiguousMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	m.seed(&ibclient.NetworkView{Name: stringPtr("view-a"), Ea: identity.Stamp(nil, testUIDNamespaced)})
	m.seed(&ibclient.NetworkView{Name: stringPtr("view-b"), Ea: identity.Stamp(nil, testUIDNamespaced)})

	cr := newNamespacedNetworkView("ns", "my-view", "networkview/stale:my-view", "ProviderConfig")
	e := &namespacedExternal{objMgr: mc.Manager, conn: mc.Connector}

	_, err := e.Observe(context.Background(), cr)
	var ambiguous *identity.AmbiguousMatchError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("Observe: error = %v (%T), want a *identity.AmbiguousMatchError", err, err)
	}
}

// ── Convention 0004 categories: server error / minimal response ─────────

func TestClusterObserveServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterNetworkView("my-view", "networkview/test1:my-view")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected an error for a 500 response, got nil")
	}
}

func TestClusterObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	// Only the identifier and the identity stamp are populated — every
	// other field is at its zero value.
	nv := &ibclient.NetworkView{}
	nv.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(nv)

	cr := newClusterNetworkView("my-view", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error on a minimal response: %v", err)
	}
	if !obs.ResourceExists {
		t.Fatalf("Observe: expected ResourceExists=true on a minimal response, got %+v", obs)
	}
	if cr.Status.AtProvider.Ref == nil || *cr.Status.AtProvider.Ref != ref {
		t.Errorf("Observe: AtProvider.Ref = %v, want %q", cr.Status.AtProvider.Ref, ref)
	}
}

func TestClusterDeleteServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterNetworkView("my-view", "networkview/test1:my-view")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Delete(context.Background(), cr); err == nil {
		t.Fatal("Delete: expected an error for a 500 response, got nil")
	}
}

// ── Delete matrix: foreign identity, ambiguity, rotated-and-recovered ────

func TestClusterDeleteRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nv := &ibclient.NetworkView{Name: stringPtr("test-view")}
	nv.Ea = identity.Stamp(nil, "someone-elses-uid")
	ref := m.seed(nv)

	cr := newClusterNetworkView("my-view", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	_, err := e.Delete(context.Background(), cr)
	var reuse *identity.HandleReuseError
	if !errors.As(err, &reuse) {
		t.Fatalf("Delete: error = %v (%T), want a *identity.HandleReuseError", err, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.views[ref]; !ok {
		t.Fatal("Delete: object with a foreign identity must never be deleted")
	}
}

func TestClusterDeleteRefusesOnAmbiguousMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	refA := m.seed(&ibclient.NetworkView{Name: stringPtr("view-a"), Ea: identity.Stamp(nil, testUIDCluster)})
	refB := m.seed(&ibclient.NetworkView{Name: stringPtr("view-b"), Ea: identity.Stamp(nil, testUIDCluster)})

	cr := newClusterNetworkView("my-view", "networkview/stale:my-view")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	_, err := e.Delete(context.Background(), cr)
	var ambiguous *identity.AmbiguousMatchError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("Delete: error = %v (%T), want a *identity.AmbiguousMatchError", err, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.views[refA]; !ok {
		t.Fatal("Delete: ambiguous match must delete neither candidate (A missing)")
	}
	if _, ok := m.views[refB]; !ok {
		t.Fatal("Delete: ambiguous match must delete neither candidate (B missing)")
	}
}

// TestClusterDeleteRecoversRotatedRefAndDeletes proves the last Delete
// matrix row: a bare 404 on the stored reference is not proof the
// backend object is gone — when the object is still recoverable by its
// stamped identity attribute, Delete must resolve it and issue the
// DELETE against the recovered ref, not report success against a merely
// stale handle (convention 0107's "a 404 is proof the handle is stale,
// not that the object is gone").
func TestClusterDeleteRecoversRotatedRefAndDeletes(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nv := &ibclient.NetworkView{Name: stringPtr("test-view")}
	nv.Ea = identity.Stamp(nil, testUIDCluster)
	realRef := m.seed(nv)

	cr := newClusterNetworkView("my-view", "networkview/stale:my-view")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.views[realRef]; ok {
		t.Fatal("Delete: expected the recovered object to be deleted, but it still exists")
	}
}

// ── Create: identity EA in the request body, single request, empty uid ──

func TestClusterCreateSendsIdentityEAInSingleRequest(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterNetworkView("my-view", "my-view")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.views) != 1 {
		t.Fatalf("Create: expected exactly one object created, got %d", len(m.views))
	}
	for _, nv := range m.views {
		if got := nv.Ea[identity.EAKey]; got != testUIDCluster {
			t.Fatalf("Create: identity EA in the created object = %v, want %q", got, testUIDCluster)
		}
	}
}

func TestClusterCreateRefusesBlankUID(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterNetworkView("my-view", "my-view")
	cr.UID = ""
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: expected an error for a blank uid")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.views) != 0 {
		t.Fatalf("Create: expected zero mutating requests for a blank uid, got %d objects created", len(m.views))
	}
}

// TestCreateNetworkViewGuardRejectsBlankUID documents the shape of
// createNetworkView's own uid guard: it trims whitespace before
// comparing against "", matching identity.Resolve's ladder and the
// pilot ARecord resource's validateARecordCreateInputs. This test calls
// createNetworkView directly, bypassing clusterExternal.Create's
// equivalent guard, to prove the helper independently rejects a blank
// uid before issuing any WAPI mutation.
func TestCreateNetworkViewGuardRejectsBlankUID(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	if _, err := createNetworkView(mc.Manager, stringPtr("x"), nil, nil, ""); err == nil {
		t.Fatal("createNetworkView: expected an error for a blank uid")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.views) != 0 {
		t.Fatalf("createNetworkView: expected zero mutating requests for a blank uid, got %d objects created", len(m.views))
	}
}

func TestNamespacedCreateSendsIdentityEAInSingleRequest(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newNamespacedNetworkView("ns", "my-view", "my-view", "ProviderConfig")
	e := &namespacedExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.views) != 1 {
		t.Fatalf("Create: expected exactly one object created, got %d", len(m.views))
	}
	for _, nv := range m.views {
		if got := nv.Ea[identity.EAKey]; got != testUIDNamespaced {
			t.Fatalf("Create: identity EA in the created object = %v, want %q", got, testUIDNamespaced)
		}
	}
}

// ── Update: genuine round-trip persistence for a rename-triggered rotation ─

// TestClusterUpdateRefreshedExternalNamePersistsAcrossReGet proves the
// convention 0107 requirement directly: the rotated external-name must
// be visible on a re-GET into a DISTINCT object instance, not merely on
// the in-memory cr the test already holds a pointer to.
func TestClusterUpdateRefreshedExternalNamePersistsAcrossReGet(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nv := &ibclient.NetworkView{Name: stringPtr("test-view")}
	nv.Ea = identity.Stamp(nil, testUIDCluster)
	oldRef := m.seed(nv)

	cr := newClusterNetworkView("my-view", oldRef)
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()
	e := &clusterExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector}
	cr.Spec.ForProvider.Name = stringPtr("renamed-view")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	newRef := meta.GetExternalName(cr)
	if newRef == oldRef {
		t.Fatal("Update: external-name unchanged after a _ref-mutating rename, want a refreshed _ref")
	}

	fetched := &clusterv1alpha1.NetworkView{}
	if err := kube.Get(context.Background(), types.NamespacedName{Name: cr.GetName()}, fetched); err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	if got := meta.GetExternalName(fetched); got != newRef {
		t.Errorf("Update: persisted external-name (re-GET into a distinct object) = %q, want %q", got, newRef)
	}
}

func TestNamespacedUpdateRefreshedExternalNamePersistsAcrossReGet(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nv := &ibclient.NetworkView{Name: stringPtr("test-view")}
	nv.Ea = identity.Stamp(nil, testUIDNamespaced)
	oldRef := m.seed(nv)

	cr := newNamespacedNetworkView("ns", "my-view", oldRef, "ProviderConfig")
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()
	e := &namespacedExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector}
	cr.Spec.ForProvider.Name = stringPtr("renamed-view")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	newRef := meta.GetExternalName(cr)
	if newRef == oldRef {
		t.Fatal("Update: external-name unchanged after a _ref-mutating rename, want a refreshed _ref")
	}

	fetched := &namespacedv1alpha1.NetworkView{}
	if err := kube.Get(context.Background(), types.NamespacedName{Name: cr.GetName(), Namespace: cr.GetNamespace()}, fetched); err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	if got := meta.GetExternalName(fetched); got != newRef {
		t.Errorf("Update: persisted external-name (re-GET into a distinct object) = %q, want %q", got, newRef)
	}
}

// ── AtProvider identity mirror (convention 0032) ─────────────────────────

// TestClusterObserveAtProviderExtAttrsIncludesIdentityKey proves that
// while isUpToDate/lateInitialize strip the reserved identity key out of
// spec.forProvider.extAttrs, the AtProvider status mirror is a full,
// unfiltered mirror of the Grid object's extattrs — including the
// identity stamp — per the full-mirror AtProvider convention.
func TestClusterObserveAtProviderExtAttrsIncludesIdentityKey(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nv := &ibclient.NetworkView{Name: stringPtr("test-view")}
	nv.Ea = identity.Stamp(ibclient.EA{"Site": "dc1"}, testUIDCluster)
	ref := m.seed(nv)

	cr := newClusterNetworkView("my-view", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got := cr.Status.AtProvider.ExtAttrs[identity.EAKey]; got != testUIDCluster {
		t.Errorf("Observe: AtProvider.ExtAttrs[%q] = %q, want %q — AtProvider must mirror the Grid's full extattrs map", identity.EAKey, got, testUIDCluster)
	}
}

// ── Identity-prerequisite probe: reactive, not unconditional ────────────

func TestClusterObserveSurfacesPrerequisiteErrorFromIdentitySearch(t *testing.T) {
	m := newMockWapiServer()
	m.eaDefExists = false
	m.eaDefCreateStatus = http.StatusForbidden
	m.eaDefCreateBody = `{"Error":"AdmConProtoError: Not authorized"}`
	m.undefinedEASearch = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-observe-undefined-ea"}
	// No external-name ever assigned: observeRefFor reports "" here,
	// sending the ladder straight to the identity-EA search.
	cr := newClusterNetworkView("my-view", "")

	_, err := e.Observe(context.Background(), cr)
	var prereq *identity.PrerequisiteError
	if !errors.As(err, &prereq) {
		t.Fatalf("Observe: error = %v (%T), want it to wrap a *identity.PrerequisiteError", err, err)
	}
}

func TestClusterObserveSteadyStateNeverProbesPrerequisite(t *testing.T) {
	m := newMockWapiServer()
	m.undefinedEASearch = true // would break the ladder if ever reached
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nv := &ibclient.NetworkView{Name: stringPtr("test-view")}
	nv.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(nv)

	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-steady-state"}
	cr := newClusterNetworkView("my-view", ref)

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error on a reference that resolves directly: %v", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.eaDefSearchCalls != 0 {
		t.Errorf("eaDefSearchCalls = %d, want 0 — the steady-state (reference resolves) path must never probe", m.eaDefSearchCalls)
	}
}

func TestClusterObserveForeignIdentityNeverProbesPrerequisite(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nv := &ibclient.NetworkView{Name: stringPtr("test-view")}
	nv.Ea = identity.Stamp(nil, "someone-elses-uid")
	ref := m.seed(nv)

	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-foreign-identity"}
	cr := newClusterNetworkView("my-view", ref)

	_, err := e.Observe(context.Background(), cr)
	var reuse *identity.HandleReuseError
	if !errors.As(err, &reuse) {
		t.Fatalf("Observe: error = %v, want a *identity.HandleReuseError, not intercepted by the prerequisite guard", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.eaDefSearchCalls != 0 {
		t.Errorf("eaDefSearchCalls = %d, want 0 — a HandleReuseError is unrelated to the identity-EA search and must not probe", m.eaDefSearchCalls)
	}
}

func TestClusterObserveRefGetFailureNeverProbesPrerequisite(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()
	mc := newTestClient(t, srv)

	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-ref-get-failure"}
	cr := newClusterNetworkView("my-view", "networkview/test1:my-view")

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected an error for a 500 on the ref-GET, got nil")
	}
	if identity.IsSearchFailure(err) {
		t.Fatalf("Observe: error = %v, want it NOT classified as identity.IsSearchFailure — a ref-GET failure never reaches the search step", err)
	}
	var prereq *identity.PrerequisiteError
	if errors.As(err, &prereq) {
		t.Fatalf("Observe: error = %v, want it NOT to be a *identity.PrerequisiteError — the guard must not fire on a ref-GET failure", err)
	}
}

func TestClusterObserveAmbiguousMatchNeverProbesPrerequisite(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	m.seed(&ibclient.NetworkView{Name: stringPtr("view-a"), Ea: identity.Stamp(nil, testUIDCluster)})
	m.seed(&ibclient.NetworkView{Name: stringPtr("view-b"), Ea: identity.Stamp(nil, testUIDCluster)})

	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-ambiguous"}
	cr := newClusterNetworkView("my-view", "networkview/stale:my-view")

	_, err := e.Observe(context.Background(), cr)
	var ambiguous *identity.AmbiguousMatchError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("Observe: error = %v, want a *identity.AmbiguousMatchError, not intercepted by the prerequisite guard", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.eaDefSearchCalls != 0 {
		t.Errorf("eaDefSearchCalls = %d, want 0 — an AmbiguousMatchError is unrelated to whether the search itself failed and must not probe", m.eaDefSearchCalls)
	}
}

func TestClusterDeleteSurfacesPrerequisiteErrorFromIdentitySearch(t *testing.T) {
	m := newMockWapiServer()
	m.eaDefExists = false
	m.eaDefCreateStatus = http.StatusForbidden
	m.eaDefCreateBody = `{"Error":"AdmConProtoError: Not authorized"}`
	m.undefinedEASearch = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-delete-undefined-ea"}
	cr := newClusterNetworkView("my-view", "networkview/stale:my-view")

	_, err := e.Delete(context.Background(), cr)
	var prereq *identity.PrerequisiteError
	if !errors.As(err, &prereq) {
		t.Fatalf("Delete: error = %v (%T), want it to wrap a *identity.PrerequisiteError", err, err)
	}
}

func TestClusterDeleteSteadyStateNeverProbesPrerequisite(t *testing.T) {
	m := newMockWapiServer()
	m.undefinedEASearch = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nv := &ibclient.NetworkView{Name: stringPtr("test-view")}
	nv.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(nv)

	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-delete-steady-state"}
	cr := newClusterNetworkView("my-view", ref)

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error on a reference that resolves directly: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.eaDefSearchCalls != 0 {
		t.Errorf("eaDefSearchCalls = %d, want 0 — the steady-state path must never probe", m.eaDefSearchCalls)
	}
}
