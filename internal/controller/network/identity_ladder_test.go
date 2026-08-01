// Identity ladder matrix tests for the Network MR controllers. See
// networkview's identity_ladder_test.go for the rationale shared across
// this wave, and controller_test.go's own doc comment for the
// dual-object-type mock server this file builds on. Network's identity
// fields (networkView, network/cidr) are immutable, so — like
// NetworkContainer — the Update() path never rotates its _ref in
// practice (Network's Update() doesn't even attempt an
// externalname.Refresh call; see cluster.go's Update doc). This file
// focuses on the rows unique to Network's dual-object-type /
// unknown-family design plus the matrix every resource in this wave
// needs: namespaced-scope coverage, typed refusals via errors.As, the
// reactive identity-prerequisite probe, and the convention-0004 category
// set.
package network

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

	nw := &ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nw.Ea = identity.Stamp(nil, testUIDNamespaced)
	ref := m.seed(nw, false)

	cr := newNamespacedNetwork("ns", "my-network", ref, "ProviderConfig")
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

	cr := newNamespacedNetwork("ns", "my-network", "network/doesnotexist:10.0.0.0/16/default", "ProviderConfig")
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

	nw := &ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16"}
	ref := m.seed(nw, false)

	cr := newNamespacedNetwork("ns", "my-network", ref, "ProviderConfig")
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

	nw := &ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nw.Ea = identity.Stamp(nil, testUIDNamespaced)
	realRef := m.seed(nw, false)

	cr := newNamespacedNetwork("ns", "my-network", "network/stale:10.0.0.0/16/default", "ProviderConfig")
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

	nw := &ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nw.Ea = identity.Stamp(nil, "someone-elses-uid")
	ref := m.seed(nw, false)

	cr := newNamespacedNetwork("ns", "my-network", ref, "ProviderConfig")
	e := &namespacedExternal{objMgr: mc.Manager, conn: mc.Connector}

	_, err := e.Observe(context.Background(), cr)
	var reuse *identity.HandleReuseError
	if !errors.As(err, &reuse) {
		t.Fatalf("Observe: error = %v (%T), want a *identity.HandleReuseError", err, err)
	}
}

func TestNamespacedObserveFindsIPv6Object(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nw := &ibclient.Network{NetviewName: "default", Cidr: "2001:db8::/32"}
	nw.Ea = identity.Stamp(nil, testUIDNamespaced)
	m.seed(nw, true)

	// No external-name set (pre-create state): forces the identity-EA
	// search step, the only step whose WAPI endpoint depends on the
	// candidate object's assumed type. A resolving _ref would fetch by
	// literal path and mask a wrong-type newEmpty entirely.
	cr := newNamespacedNetwork("ns", "my-network", "", "ProviderConfig")
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

func TestNamespacedObserveUnknownFamilySearchesBothTypesNotDefaultV4(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nw := &ibclient.Network{NetviewName: "default", Cidr: "2001:db8::/32"}
	nw.Ea = identity.Stamp(nil, testUIDNamespaced)
	m.seed(nw, true)

	prefixLen := uint(24)
	cr := newNamespacedNetwork("ns", "my-network", "", "ProviderConfig")
	cr.Spec.ForProvider.Network = nil
	cr.Spec.ForProvider.FilterParams = map[string]string{"Site": "dc1"}
	cr.Spec.ForProvider.AllocatePrefixLen = &prefixLen
	e := &namespacedExternal{objMgr: mc.Manager, conn: mc.Connector}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !obs.ResourceExists {
		t.Fatal("Observe: expected the dual-search fallback to find the IPv6-family object, not silently default to IPv4-only search")
	}
}

// ── Typed refusals via errors.As ──────────────────────────────────────────

func TestClusterObserveRefusesOnForeignIdentityTyped(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nw := &ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nw.Ea = identity.Stamp(nil, "someone-elses-uid")
	ref := m.seed(nw, false)

	cr := newClusterNetwork("my-network", ref)
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

	m.seed(&ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16", Ea: identity.Stamp(nil, testUIDCluster)}, false)
	m.seed(&ibclient.Network{NetviewName: "default", Cidr: "10.1.0.0/16", Ea: identity.Stamp(nil, testUIDCluster)}, false)

	cr := newClusterNetwork("my-network", "network/stale:10.0.0.0/16/default")
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

	m.seed(&ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16", Ea: identity.Stamp(nil, testUIDNamespaced)}, false)
	m.seed(&ibclient.Network{NetviewName: "default", Cidr: "10.1.0.0/16", Ea: identity.Stamp(nil, testUIDNamespaced)}, false)

	cr := newNamespacedNetwork("ns", "my-network", "network/stale:10.0.0.0/16/default", "ProviderConfig")
	e := &namespacedExternal{objMgr: mc.Manager, conn: mc.Connector}

	_, err := e.Observe(context.Background(), cr)
	var ambiguous *identity.AmbiguousMatchError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("Observe: error = %v (%T), want a *identity.AmbiguousMatchError", err, err)
	}
}

// TestObserveUnknownFamilyAmbiguousAcrossUnionFailsClosed proves the
// unknown-family fallback's own ambiguity rule: a v4 match AND a v6
// match for the same uid must refuse, not silently pick one — see
// resolveNetworkIdentityUnknownFamily.
func TestObserveUnknownFamilyAmbiguousAcrossUnionFailsClosed(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	m.seed(&ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16", Ea: identity.Stamp(nil, testUIDCluster)}, false)
	m.seed(&ibclient.Network{NetviewName: "default", Cidr: "2001:db8::/32", Ea: identity.Stamp(nil, testUIDCluster)}, true)

	cr := newClusterNetworkUnknownFamily("my-network", "my-network")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	_, err := e.Observe(context.Background(), cr)
	var ambiguous *identity.AmbiguousMatchError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("Observe: error = %v (%T), want a *identity.AmbiguousMatchError when both address families match the same uid", err, err)
	}
}

// TestObserveUnknownFamilyRefusesZeroMutatingCalls documents that the
// unknown-family path is read-only during Observe (it only ever issues
// identity-EA searches, never a mutating call) regardless of which
// branch (single match, ambiguous, or not-found) it takes.
func TestObserveUnknownFamilyRefusesZeroMutatingCalls(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	m.seed(&ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16", Ea: identity.Stamp(nil, testUIDCluster)}, false)
	m.seed(&ibclient.Network{NetviewName: "default", Cidr: "2001:db8::/32", Ea: identity.Stamp(nil, testUIDCluster)}, true)

	before := len(m.networks)
	cr := newClusterNetworkUnknownFamily("my-network", "my-network")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected an ambiguity refusal")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.networks) != before {
		t.Fatalf("Observe: expected zero mutating calls, object count changed from %d to %d", before, len(m.networks))
	}
}

// ── Convention 0004 categories: server error / minimal response ─────────

func TestClusterObserveServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterNetwork("my-network", "network/test1:10.0.0.0/16/default")
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

	nw := &ibclient.Network{}
	nw.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(nw, false)

	cr := newClusterNetwork("my-network", ref)
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

	cr := newClusterNetwork("my-network", "network/test1:10.0.0.0/16/default")
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

	nw := &ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nw.Ea = identity.Stamp(nil, "someone-elses-uid")
	ref := m.seed(nw, false)

	cr := newClusterNetwork("my-network", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	_, err := e.Delete(context.Background(), cr)
	var reuse *identity.HandleReuseError
	if !errors.As(err, &reuse) {
		t.Fatalf("Delete: error = %v (%T), want a *identity.HandleReuseError", err, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.networks[ref]; !ok {
		t.Fatal("Delete: object with a foreign identity must never be deleted")
	}
}

func TestClusterDeleteRefusesOnAmbiguousMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	refA := m.seed(&ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16", Ea: identity.Stamp(nil, testUIDCluster)}, false)
	refB := m.seed(&ibclient.Network{NetviewName: "default", Cidr: "10.1.0.0/16", Ea: identity.Stamp(nil, testUIDCluster)}, false)

	cr := newClusterNetwork("my-network", "network/stale:10.0.0.0/16/default")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	_, err := e.Delete(context.Background(), cr)
	var ambiguous *identity.AmbiguousMatchError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("Delete: error = %v (%T), want a *identity.AmbiguousMatchError", err, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.networks[refA]; !ok {
		t.Fatal("Delete: ambiguous match must delete neither candidate (A missing)")
	}
	if _, ok := m.networks[refB]; !ok {
		t.Fatal("Delete: ambiguous match must delete neither candidate (B missing)")
	}
}

func TestClusterDeleteRecoversRotatedRefAndDeletes(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nw := &ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nw.Ea = identity.Stamp(nil, testUIDCluster)
	realRef := m.seed(nw, false)

	cr := newClusterNetwork("my-network", "network/stale:10.0.0.0/16/default")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.networks[realRef]; ok {
		t.Fatal("Delete: expected the recovered object to be deleted, but it still exists")
	}
}

// ── Create: identity EA in the request body, single request, blank uid ──

func TestClusterCreateSendsIdentityEAInSingleRequest(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterNetwork("my-network", "my-network")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.networks) != 1 {
		t.Fatalf("Create: expected exactly one object created, got %d", len(m.networks))
	}
	for _, nw := range m.networks {
		if got := nw.Ea[identity.EAKey]; got != testUIDCluster {
			t.Fatalf("Create: identity EA = %v, want %q", got, testUIDCluster)
		}
	}
}

func TestClusterCreateRefusesBlankUID(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterNetwork("my-network", "my-network")
	cr.UID = ""
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: expected an error for a blank uid")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.networks) != 0 {
		t.Fatalf("Create: expected zero mutating requests for a blank uid, got %d objects created", len(m.networks))
	}
}

// TestClusterCreateAllocatePathStampsIdentity proves the parentCidr
// allocate-from-parent creation path (createOrAllocateNetwork ->
// objMgr.AllocateNetwork) also stamps identity in the request it issues
// — not just the static-CIDR path. The SDK's AllocateNetwork
// reconstructs its return value purely from the parsed _ref text (see
// ibclient.BuildNetworkFromRef), which never carries extattrs, so the
// identity stamp is asserted against the server-side stored object (the
// actual request body), not the returned struct.
func TestClusterCreateAllocatePathStampsIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	prefixLen := uint(24)
	if _, err := createOrAllocateNetwork(mc.Manager, stringPtr("default"), nil, stringPtr("10.0.0.0/8"), nil, nil, &prefixLen, nil, nil, testUIDCluster); err != nil {
		t.Fatalf("createOrAllocateNetwork: unexpected error: %v", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.networks) != 1 {
		t.Fatalf("expected exactly one object created via the allocate path, got %d", len(m.networks))
	}
	for _, nw := range m.networks {
		if got := nw.Ea[identity.EAKey]; got != testUIDCluster {
			t.Fatalf("allocate-path create: identity EA = %v, want %q", got, testUIDCluster)
		}
	}
}

func TestNamespacedCreateSendsIdentityEAInSingleRequest(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newNamespacedNetwork("ns", "my-network", "my-network", "ProviderConfig")
	e := &namespacedExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.networks) != 1 {
		t.Fatalf("Create: expected exactly one object created, got %d", len(m.networks))
	}
	for _, nw := range m.networks {
		if got := nw.Ea[identity.EAKey]; got != testUIDNamespaced {
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

	nw := &ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nw.Ea = identity.Stamp(ibclient.EA{"Site": "dc1"}, testUIDCluster)
	ref := m.seed(nw, false)

	cr := newClusterNetwork("my-network", ref)
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
	cr := newClusterNetwork("my-network", "")

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

	nw := &ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nw.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(nw, false)

	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-steady-state"}
	cr := newClusterNetwork("my-network", ref)

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

	nw := &ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nw.Ea = identity.Stamp(nil, "someone-elses-uid")
	ref := m.seed(nw, false)

	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-foreign-identity"}
	cr := newClusterNetwork("my-network", ref)

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
	cr := newClusterNetwork("my-network", "network/test1:10.0.0.0/16/default")

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

	m.seed(&ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16", Ea: identity.Stamp(nil, testUIDCluster)}, false)
	m.seed(&ibclient.Network{NetviewName: "default", Cidr: "10.1.0.0/16", Ea: identity.Stamp(nil, testUIDCluster)}, false)

	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-ambiguous"}
	cr := newClusterNetwork("my-network", "network/stale:10.0.0.0/16/default")

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
	cr := newClusterNetwork("my-network", "network/stale:10.0.0.0/16/default")

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

	nw := &ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nw.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(nw, false)

	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-delete-steady-state"}
	cr := newClusterNetwork("my-network", ref)

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error on a reference that resolves directly: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.eaDefSearchCalls != 0 {
		t.Errorf("eaDefSearchCalls = %d, want 0", m.eaDefSearchCalls)
	}
}
