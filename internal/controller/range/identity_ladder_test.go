// Identity ladder matrix tests for the Range MR controllers. See
// networkview's identity_ladder_test.go for the rationale shared across
// this wave. Range's _ref embeds its start/end address bounds, both of
// which are mutable via Update() — so, like NetworkView and
// IPv4SharedNetwork, a genuine Update()-triggered rotation is reachable
// (not merely a defensive no-op).
package rangepkg

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/range/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/range/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
)

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

	rng := &ibclient.Range{StartAddr: stringPtr("10.0.0.10"), EndAddr: stringPtr("10.0.0.20")}
	rng.Ea = identity.Stamp(nil, testUIDNamespaced)
	ref := m.seed(rng)

	cr := newNamespacedRange("ns", "my-range", ref, "ProviderConfig")
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

	cr := newNamespacedRange("ns", "my-range", "range/doesnotexist:10.0.0.10/10.0.0.20/default", "ProviderConfig")
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

	rng := &ibclient.Range{StartAddr: stringPtr("10.0.0.10"), EndAddr: stringPtr("10.0.0.20")}
	ref := m.seed(rng)

	cr := newNamespacedRange("ns", "my-range", ref, "ProviderConfig")
	e := &namespacedExternal{objMgr: mc.Manager, conn: mc.Connector}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !obs.ResourceExists {
		t.Fatalf("Observe: expected exists, got %+v", obs)
	}
	if obs.ResourceUpToDate {
		t.Fatal("Observe: adopted object must never report up to date, even when every user-facing field matches")
	}
}

func TestNamespacedObserveRecoversRotatedRefAndPersistsAnnotation(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	rng := &ibclient.Range{StartAddr: stringPtr("10.0.0.10"), EndAddr: stringPtr("10.0.0.20")}
	rng.Ea = identity.Stamp(nil, testUIDNamespaced)
	realRef := m.seed(rng)

	cr := newNamespacedRange("ns", "my-range", "range/stale:10.0.0.10/10.0.0.20/default", "ProviderConfig")
	e := &namespacedExternal{objMgr: mc.Manager, conn: mc.Connector}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !obs.ResourceExists || !obs.ResourceLateInitialized {
		t.Fatalf("Observe: expected exists+late-init, got %+v", obs)
	}
	if got := meta.GetExternalName(cr); got != realRef {
		t.Fatalf("Observe: external-name = %q, want recovered reference %q", got, realRef)
	}
}

func TestNamespacedObserveRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	rng := &ibclient.Range{StartAddr: stringPtr("10.0.0.10"), EndAddr: stringPtr("10.0.0.20")}
	rng.Ea = identity.Stamp(nil, "someone-elses-uid")
	ref := m.seed(rng)

	cr := newNamespacedRange("ns", "my-range", ref, "ProviderConfig")
	e := &namespacedExternal{objMgr: mc.Manager, conn: mc.Connector}

	_, err := e.Observe(context.Background(), cr)
	var reuse *identity.HandleReuseError
	if !errors.As(err, &reuse) {
		t.Fatalf("Observe: error = %v (%T), want a *identity.HandleReuseError", err, err)
	}
}

// ── Typed refusals via errors.As ──────────────────────────────────────────

func TestClusterObserveRefusesOnForeignIdentityTyped(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	rng := &ibclient.Range{StartAddr: stringPtr("10.0.0.10"), EndAddr: stringPtr("10.0.0.20")}
	rng.Ea = identity.Stamp(nil, "someone-elses-uid")
	ref := m.seed(rng)

	cr := newClusterRange("my-range", ref)
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

	m.seed(&ibclient.Range{StartAddr: stringPtr("10.0.0.10"), EndAddr: stringPtr("10.0.0.20"), Ea: identity.Stamp(nil, testUIDCluster)})
	m.seed(&ibclient.Range{StartAddr: stringPtr("10.0.1.10"), EndAddr: stringPtr("10.0.1.20"), Ea: identity.Stamp(nil, testUIDCluster)})

	cr := newClusterRange("my-range", "range/stale:10.0.0.10/10.0.0.20/default")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	_, err := e.Observe(context.Background(), cr)
	var ambiguous *identity.AmbiguousMatchError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("Observe: error = %v (%T), want a *identity.AmbiguousMatchError — ambiguity must fail closed", err, err)
	}
}

func TestNamespacedObserveRefusesOnAmbiguousMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	m.seed(&ibclient.Range{StartAddr: stringPtr("10.0.0.10"), EndAddr: stringPtr("10.0.0.20"), Ea: identity.Stamp(nil, testUIDNamespaced)})
	m.seed(&ibclient.Range{StartAddr: stringPtr("10.0.1.10"), EndAddr: stringPtr("10.0.1.20"), Ea: identity.Stamp(nil, testUIDNamespaced)})

	cr := newNamespacedRange("ns", "my-range", "range/stale:10.0.0.10/10.0.0.20/default", "ProviderConfig")
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

	cr := newClusterRange("my-range", "range/test1:10.0.0.10/10.0.0.20/default")
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

	rng := &ibclient.Range{}
	rng.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(rng)

	cr := newClusterRange("my-range", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error on a minimal response: %v", err)
	}
	if !obs.ResourceExists {
		t.Fatalf("Observe: expected ResourceExists=true on a minimal response, got %+v", obs)
	}
}

func TestClusterDeleteServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterRange("my-range", "range/test1:10.0.0.10/10.0.0.20/default")
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

	rng := &ibclient.Range{StartAddr: stringPtr("10.0.0.10"), EndAddr: stringPtr("10.0.0.20")}
	rng.Ea = identity.Stamp(nil, "someone-elses-uid")
	ref := m.seed(rng)

	cr := newClusterRange("my-range", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	_, err := e.Delete(context.Background(), cr)
	var reuse *identity.HandleReuseError
	if !errors.As(err, &reuse) {
		t.Fatalf("Delete: error = %v (%T), want a *identity.HandleReuseError", err, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.ranges[ref]; !ok {
		t.Fatal("Delete: object with a foreign identity must never be deleted")
	}
}

func TestClusterDeleteRefusesOnAmbiguousMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	refA := m.seed(&ibclient.Range{StartAddr: stringPtr("10.0.0.10"), EndAddr: stringPtr("10.0.0.20"), Ea: identity.Stamp(nil, testUIDCluster)})
	refB := m.seed(&ibclient.Range{StartAddr: stringPtr("10.0.1.10"), EndAddr: stringPtr("10.0.1.20"), Ea: identity.Stamp(nil, testUIDCluster)})

	cr := newClusterRange("my-range", "range/stale:10.0.0.10/10.0.0.20/default")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	_, err := e.Delete(context.Background(), cr)
	var ambiguous *identity.AmbiguousMatchError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("Delete: error = %v (%T), want a *identity.AmbiguousMatchError", err, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.ranges[refA]; !ok {
		t.Fatal("Delete: ambiguous match must delete neither candidate (A missing)")
	}
	if _, ok := m.ranges[refB]; !ok {
		t.Fatal("Delete: ambiguous match must delete neither candidate (B missing)")
	}
}

func TestClusterDeleteRecoversRotatedRefAndDeletes(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	rng := &ibclient.Range{StartAddr: stringPtr("10.0.0.10"), EndAddr: stringPtr("10.0.0.20")}
	rng.Ea = identity.Stamp(nil, testUIDCluster)
	realRef := m.seed(rng)

	cr := newClusterRange("my-range", "range/stale:10.0.0.10/10.0.0.20/default")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.ranges[realRef]; ok {
		t.Fatal("Delete: expected the recovered object to be deleted, but it still exists")
	}
}

// ── Create: identity EA in the request body, single request, blank uid ──

func TestClusterCreateSendsIdentityEAInSingleRequest(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterRange("my-range", "my-range")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.ranges) != 1 {
		t.Fatalf("Create: expected exactly one object created, got %d", len(m.ranges))
	}
	for _, rng := range m.ranges {
		if got := rng.Ea[identity.EAKey]; got != testUIDCluster {
			t.Fatalf("Create: identity EA = %v, want %q", got, testUIDCluster)
		}
	}
}

func TestClusterCreateRefusesBlankUID(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterRange("my-range", "my-range")
	cr.UID = ""
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: expected an error for a blank uid")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.ranges) != 0 {
		t.Fatalf("Create: expected zero mutating requests for a blank uid, got %d objects created", len(m.ranges))
	}
}

func TestNamespacedCreateSendsIdentityEAInSingleRequest(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newNamespacedRange("ns", "my-range", "my-range", "ProviderConfig")
	e := &namespacedExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.ranges) != 1 {
		t.Fatalf("Create: expected exactly one object created, got %d", len(m.ranges))
	}
	for _, rng := range m.ranges {
		if got := rng.Ea[identity.EAKey]; got != testUIDNamespaced {
			t.Fatalf("Create: identity EA = %v, want %q", got, testUIDNamespaced)
		}
	}
}

// ── Update: genuine round-trip persistence for a bounds-mutating rotation ─

func TestClusterUpdateRefreshedExternalNamePersistsAcrossReGet(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	rng := &ibclient.Range{StartAddr: stringPtr("10.0.0.10"), EndAddr: stringPtr("10.0.0.20")}
	rng.Ea = identity.Stamp(nil, testUIDCluster)
	oldRef := m.seed(rng)

	cr := newClusterRange("my-range", oldRef)
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()
	e := &clusterExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector}
	cr.Spec.ForProvider.StartAddr = stringPtr("10.0.0.30")
	cr.Spec.ForProvider.EndAddr = stringPtr("10.0.0.40")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	newRef := meta.GetExternalName(cr)
	if newRef == oldRef {
		t.Fatal("Update: external-name unchanged after a _ref-mutating bounds change, want a refreshed _ref")
	}

	fetched := &clusterv1alpha1.Range{}
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

	rng := &ibclient.Range{StartAddr: stringPtr("10.0.0.10"), EndAddr: stringPtr("10.0.0.20")}
	rng.Ea = identity.Stamp(nil, testUIDNamespaced)
	oldRef := m.seed(rng)

	cr := newNamespacedRange("ns", "my-range", oldRef, "ProviderConfig")
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()
	e := &namespacedExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector}
	cr.Spec.ForProvider.StartAddr = stringPtr("10.0.0.30")
	cr.Spec.ForProvider.EndAddr = stringPtr("10.0.0.40")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	newRef := meta.GetExternalName(cr)
	if newRef == oldRef {
		t.Fatal("Update: external-name unchanged after a _ref-mutating bounds change, want a refreshed _ref")
	}

	fetched := &namespacedv1alpha1.Range{}
	if err := kube.Get(context.Background(), types.NamespacedName{Name: cr.GetName(), Namespace: cr.GetNamespace()}, fetched); err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	if got := meta.GetExternalName(fetched); got != newRef {
		t.Errorf("Update: persisted external-name (re-GET into a distinct object) = %q, want %q", got, newRef)
	}
}

// ── AtProvider identity mirror (convention 0032) ─────────────────────────

func TestClusterObserveAtProviderExtAttrsIncludesIdentityKey(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	rng := &ibclient.Range{StartAddr: stringPtr("10.0.0.10"), EndAddr: stringPtr("10.0.0.20")}
	rng.Ea = identity.Stamp(ibclient.EA{"Site": "dc1"}, testUIDCluster)
	ref := m.seed(rng)

	cr := newClusterRange("my-range", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got := cr.Status.AtProvider.ExtAttrs[identity.EAKey]; got != testUIDCluster {
		t.Errorf("Observe: AtProvider.ExtAttrs[%q] = %q, want %q", identity.EAKey, got, testUIDCluster)
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
	cr := newClusterRange("my-range", "")

	_, err := e.Observe(context.Background(), cr)
	var prereq *identity.PrerequisiteError
	if !errors.As(err, &prereq) {
		t.Fatalf("Observe: error = %v (%T), want it to wrap a *identity.PrerequisiteError", err, err)
	}
}

func TestClusterObserveSteadyStateNeverProbesPrerequisite(t *testing.T) {
	m := newMockWapiServer()
	m.undefinedEASearch = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	rng := &ibclient.Range{StartAddr: stringPtr("10.0.0.10"), EndAddr: stringPtr("10.0.0.20")}
	rng.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(rng)

	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-steady-state"}
	cr := newClusterRange("my-range", ref)

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error on a reference that resolves directly: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.eaDefSearchCalls != 0 {
		t.Errorf("eaDefSearchCalls = %d, want 0", m.eaDefSearchCalls)
	}
}

func TestClusterObserveForeignIdentityNeverProbesPrerequisite(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	rng := &ibclient.Range{StartAddr: stringPtr("10.0.0.10"), EndAddr: stringPtr("10.0.0.20")}
	rng.Ea = identity.Stamp(nil, "someone-elses-uid")
	ref := m.seed(rng)

	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-foreign-identity"}
	cr := newClusterRange("my-range", ref)

	_, err := e.Observe(context.Background(), cr)
	var reuse *identity.HandleReuseError
	if !errors.As(err, &reuse) {
		t.Fatalf("Observe: error = %v, want a *identity.HandleReuseError, not intercepted by the prerequisite guard", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.eaDefSearchCalls != 0 {
		t.Errorf("eaDefSearchCalls = %d, want 0", m.eaDefSearchCalls)
	}
}

func TestClusterObserveRefGetFailureNeverProbesPrerequisite(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()
	mc := newTestClient(t, srv)

	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-ref-get-failure"}
	cr := newClusterRange("my-range", "range/test1:10.0.0.10/10.0.0.20/default")

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected an error for a 500 on the ref-GET, got nil")
	}
	if identity.IsSearchFailure(err) {
		t.Fatalf("Observe: error = %v, want it NOT classified as identity.IsSearchFailure", err)
	}
	var prereq *identity.PrerequisiteError
	if errors.As(err, &prereq) {
		t.Fatalf("Observe: error = %v, want it NOT to be a *identity.PrerequisiteError", err)
	}
}

func TestClusterObserveAmbiguousMatchNeverProbesPrerequisite(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	m.seed(&ibclient.Range{StartAddr: stringPtr("10.0.0.10"), EndAddr: stringPtr("10.0.0.20"), Ea: identity.Stamp(nil, testUIDCluster)})
	m.seed(&ibclient.Range{StartAddr: stringPtr("10.0.1.10"), EndAddr: stringPtr("10.0.1.20"), Ea: identity.Stamp(nil, testUIDCluster)})

	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-ambiguous"}
	cr := newClusterRange("my-range", "range/stale:10.0.0.10/10.0.0.20/default")

	_, err := e.Observe(context.Background(), cr)
	var ambiguous *identity.AmbiguousMatchError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("Observe: error = %v, want a *identity.AmbiguousMatchError, not intercepted by the prerequisite guard", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.eaDefSearchCalls != 0 {
		t.Errorf("eaDefSearchCalls = %d, want 0", m.eaDefSearchCalls)
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
	cr := newClusterRange("my-range", "range/stale:10.0.0.10/10.0.0.20/default")

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

	rng := &ibclient.Range{StartAddr: stringPtr("10.0.0.10"), EndAddr: stringPtr("10.0.0.20")}
	rng.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(rng)

	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-delete-steady-state"}
	cr := newClusterRange("my-range", ref)

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error on a reference that resolves directly: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.eaDefSearchCalls != 0 {
		t.Errorf("eaDefSearchCalls = %d, want 0", m.eaDefSearchCalls)
	}
}
