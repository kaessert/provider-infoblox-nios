// Identity ladder matrix tests for the NetworkContainer MR controllers.
// See networkview's identity_ladder_test.go for the rationale shared
// across this wave, and network's controller_test.go for the
// dual-object-type mock server this package's tests share the same
// shape with (WAPI models IPv4 containers as "networkcontainer" and IPv6
// as "ipv6networkcontainer"). Unlike ARecord/RangeTemplate/etc.,
// NetworkContainer's identity fields (networkView, network) are
// immutable, so the externalname.Refresh call inside its Update() is a
// defensive no-op that live traffic never exercises — already covered
// generically by internal/controller/externalname's own unit tests — so
// this file does not attempt a round-trip persistence test for it.
package networkcontainer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"

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

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nc.Ea = identity.Stamp(nil, testUIDNamespaced)
	ref := m.seed(nc, false)

	cr := newNamespacedNetworkContainer("ns", "my-container", ref, "ProviderConfig")
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

	cr := newNamespacedNetworkContainer("ns", "my-container", "networkcontainer/doesnotexist:10.0.0.0/16/default", "ProviderConfig")
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

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16"}
	ref := m.seed(nc, false)

	cr := newNamespacedNetworkContainer("ns", "my-container", ref, "ProviderConfig")
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

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nc.Ea = identity.Stamp(nil, testUIDNamespaced)
	realRef := m.seed(nc, false)

	cr := newNamespacedNetworkContainer("ns", "my-container", "networkcontainer/stale:10.0.0.0/16/default", "ProviderConfig")
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

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nc.Ea = identity.Stamp(nil, "someone-elses-uid")
	ref := m.seed(nc, false)

	cr := newNamespacedNetworkContainer("ns", "my-container", ref, "ProviderConfig")
	e := &namespacedExternal{objMgr: mc.Manager, conn: mc.Connector}

	_, err := e.Observe(context.Background(), cr)
	var reuse *identity.HandleReuseError
	if !errors.As(err, &reuse) {
		t.Fatalf("Observe: error = %v (%T), want a *identity.HandleReuseError", err, err)
	}
}

// TestNamespacedObserveFindsIPv6Object mirrors network's
// TestObserveFindsIPv6Object for the namespaced scope. No external-name
// is set (pre-create state), forcing the identity-EA search step — the
// only step whose WAPI endpoint depends on the candidate object's
// assumed type (a resolving _ref fetches by literal path and would mask
// a wrong-type newEmpty entirely).
func TestNamespacedObserveFindsIPv6Object(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "2001:db8::/32"}
	nc.Ea = identity.Stamp(nil, testUIDNamespaced)
	m.seed(nc, true)

	cr := newNamespacedNetworkContainer("ns", "my-container", "", "ProviderConfig")
	cr.Spec.ForProvider.Network = stringPtr("2001:db8::/32")
	e := &namespacedExternal{objMgr: mc.Manager, conn: mc.Connector}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !obs.ResourceExists {
		t.Fatalf("Observe: expected the IPv6 object to be found, got %+v", obs)
	}
}

// ── Typed refusals via errors.As ──────────────────────────────────────────

func TestClusterObserveRefusesOnForeignIdentityTyped(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nc.Ea = identity.Stamp(nil, "someone-elses-uid")
	ref := m.seed(nc, false)

	cr := newClusterNetworkContainer("my-container", ref)
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

	m.seed(&ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16", Ea: identity.Stamp(nil, testUIDCluster)}, false)
	m.seed(&ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.1.0.0/16", Ea: identity.Stamp(nil, testUIDCluster)}, false)

	cr := newClusterNetworkContainer("my-container", "networkcontainer/stale:10.0.0.0/16/default")
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

	m.seed(&ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16", Ea: identity.Stamp(nil, testUIDNamespaced)}, false)
	m.seed(&ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.1.0.0/16", Ea: identity.Stamp(nil, testUIDNamespaced)}, false)

	cr := newNamespacedNetworkContainer("ns", "my-container", "networkcontainer/stale:10.0.0.0/16/default", "ProviderConfig")
	e := &namespacedExternal{objMgr: mc.Manager, conn: mc.Connector}

	_, err := e.Observe(context.Background(), cr)
	var ambiguous *identity.AmbiguousMatchError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("Observe: error = %v (%T), want a *identity.AmbiguousMatchError", err, err)
	}
}

// TestObserveUnknownFamilyAmbiguousAcrossUnionFailsClosed proves the
// unknown-family (filterParams-only allocation) fallback's own ambiguity
// rule: a v4 match AND a v6 match for the same uid must refuse, not
// silently pick one — see resolveNetworkContainerIdentityUnknownFamily.
func TestObserveUnknownFamilyAmbiguousAcrossUnionFailsClosed(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	m.seed(&ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16", Ea: identity.Stamp(nil, testUIDCluster)}, false)
	m.seed(&ibclient.NetworkContainer{NetviewName: "default", Cidr: "2001:db8::/32", Ea: identity.Stamp(nil, testUIDCluster)}, true)

	cr := newClusterNetworkContainerUnknownFamily("my-container", "my-container")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	_, err := e.Observe(context.Background(), cr)
	var ambiguous *identity.AmbiguousMatchError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("Observe: error = %v (%T), want a *identity.AmbiguousMatchError when both address families match the same uid", err, err)
	}
}

func TestNamespacedObserveUnknownFamilySearchesBothTypesNotDefaultV4(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "2001:db8::/32"}
	nc.Ea = identity.Stamp(nil, testUIDNamespaced)
	m.seed(nc, true)

	prefixLen := uint(24)
	crc := newNamespacedNetworkContainer("ns", "my-container", "my-container", "ProviderConfig")
	crc.Spec.ForProvider.Network = nil
	crc.Spec.ForProvider.FilterParams = map[string]string{"Site": "dc1"}
	crc.Spec.ForProvider.AllocatePrefixLen = &prefixLen
	e := &namespacedExternal{objMgr: mc.Manager, conn: mc.Connector}

	obs, err := e.Observe(context.Background(), crc)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !obs.ResourceExists {
		t.Fatal("Observe: expected the dual-search fallback to find the IPv6-family object, not silently default to IPv4-only search")
	}
}

// ── Convention 0004 categories: server error / minimal response ─────────

func TestClusterObserveServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterNetworkContainer("my-container", "networkcontainer/test1:10.0.0.0/16/default")
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

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nc.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(nc, false)

	cr := newClusterNetworkContainer("my-container", ref)
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

	cr := newClusterNetworkContainer("my-container", "networkcontainer/test1:10.0.0.0/16/default")
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

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nc.Ea = identity.Stamp(nil, "someone-elses-uid")
	ref := m.seed(nc, false)

	cr := newClusterNetworkContainer("my-container", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	_, err := e.Delete(context.Background(), cr)
	var reuse *identity.HandleReuseError
	if !errors.As(err, &reuse) {
		t.Fatalf("Delete: error = %v (%T), want a *identity.HandleReuseError", err, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.containers[ref]; !ok {
		t.Fatal("Delete: object with a foreign identity must never be deleted")
	}
}

func TestClusterDeleteRefusesOnAmbiguousMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	refA := m.seed(&ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16", Ea: identity.Stamp(nil, testUIDCluster)}, false)
	refB := m.seed(&ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.1.0.0/16", Ea: identity.Stamp(nil, testUIDCluster)}, false)

	cr := newClusterNetworkContainer("my-container", "networkcontainer/stale:10.0.0.0/16/default")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	_, err := e.Delete(context.Background(), cr)
	var ambiguous *identity.AmbiguousMatchError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("Delete: error = %v (%T), want a *identity.AmbiguousMatchError", err, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.containers[refA]; !ok {
		t.Fatal("Delete: ambiguous match must delete neither candidate (A missing)")
	}
	if _, ok := m.containers[refB]; !ok {
		t.Fatal("Delete: ambiguous match must delete neither candidate (B missing)")
	}
}

func TestClusterDeleteRecoversRotatedRefAndDeletes(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nc.Ea = identity.Stamp(nil, testUIDCluster)
	realRef := m.seed(nc, false)

	cr := newClusterNetworkContainer("my-container", "networkcontainer/stale:10.0.0.0/16/default")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.containers[realRef]; ok {
		t.Fatal("Delete: expected the recovered object to be deleted, but it still exists")
	}
}

// ── Create: identity EA in the request body, single request, blank uid ──

func TestClusterCreateSendsIdentityEAInSingleRequest(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterNetworkContainer("my-container", "my-container")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.containers) != 1 {
		t.Fatalf("Create: expected exactly one object created, got %d", len(m.containers))
	}
	for _, nc := range m.containers {
		if got := nc.Ea[identity.EAKey]; got != testUIDCluster {
			t.Fatalf("Create: identity EA = %v, want %q", got, testUIDCluster)
		}
	}
}

func TestClusterCreateRefusesBlankUID(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterNetworkContainer("my-container", "my-container")
	cr.UID = ""
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: expected an error for a blank uid")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.containers) != 0 {
		t.Fatalf("Create: expected zero mutating requests for a blank uid, got %d objects created", len(m.containers))
	}
}

// Note: NetworkContainer's allocate-from-parent creation path
// (createOrAllocateNetworkContainer / AllocateNetworkContainer) issues a
// CreateObject call followed by a separate GET to build the object from
// the returned ref (see the vendored SDK's AllocateNetworkContainer) —
// unlike the static-CIDR path, it is not a single request, and unlike
// Network it has no dual-object-type "unknown family" ambiguity concern
// on its own allocate path (network's allocate path is the one this
// ticket's identity EA / single-request assertion targets). Exercising
// it here would require modeling the WAPI next-available-network
// function-call wire shape in the mock for no additional identity-ladder
// coverage, so it is left to network's own allocate-path test.

func TestNamespacedCreateSendsIdentityEAInSingleRequest(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newNamespacedNetworkContainer("ns", "my-container", "my-container", "ProviderConfig")
	e := &namespacedExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.containers) != 1 {
		t.Fatalf("Create: expected exactly one object created, got %d", len(m.containers))
	}
	for _, nc := range m.containers {
		if got := nc.Ea[identity.EAKey]; got != testUIDNamespaced {
			t.Fatalf("Create: identity EA = %v, want %q", got, testUIDNamespaced)
		}
	}
}

// ── AtProvider identity mirror (convention 0032) ─────────────────────────

func TestClusterObserveAtProviderExtAttrsIncludesIdentityKey(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nc.Ea = identity.Stamp(ibclient.EA{"Site": "dc1"}, testUIDCluster)
	ref := m.seed(nc, false)

	cr := newClusterNetworkContainer("my-container", ref)
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
	cr := newClusterNetworkContainer("my-container", "")

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

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nc.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(nc, false)

	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-steady-state"}
	cr := newClusterNetworkContainer("my-container", ref)

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

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nc.Ea = identity.Stamp(nil, "someone-elses-uid")
	ref := m.seed(nc, false)

	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-foreign-identity"}
	cr := newClusterNetworkContainer("my-container", ref)

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
	cr := newClusterNetworkContainer("my-container", "networkcontainer/test1:10.0.0.0/16/default")

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

	m.seed(&ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16", Ea: identity.Stamp(nil, testUIDCluster)}, false)
	m.seed(&ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.1.0.0/16", Ea: identity.Stamp(nil, testUIDCluster)}, false)

	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-ambiguous"}
	cr := newClusterNetworkContainer("my-container", "networkcontainer/stale:10.0.0.0/16/default")

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
	cr := newClusterNetworkContainer("my-container", "networkcontainer/stale:10.0.0.0/16/default")

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

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nc.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(nc, false)

	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-delete-steady-state"}
	cr := newClusterNetworkContainer("my-container", ref)

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error on a reference that resolves directly: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.eaDefSearchCalls != 0 {
		t.Errorf("eaDefSearchCalls = %d, want 0", m.eaDefSearchCalls)
	}
}
