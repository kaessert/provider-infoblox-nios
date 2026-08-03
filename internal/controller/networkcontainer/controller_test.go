// Package networkcontainer unit tests for the NetworkContainer MR
// controllers. Tests use inline httptest.NewServer mocks that emulate the
// WAPI networkcontainer/ipv6networkcontainer endpoints, PascalCase test
// names (no underscores), and white-box access to the unexported
// connectors/clients so both scopes can be exercised without going
// through the full Connect() credential bridge on every test.
package networkcontainer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	cperrors "github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/networkcontainer/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/networkcontainer/v1alpha1"
	namespacedpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
)

const (
	testUIDCluster    = "test-uid-cluster"
	testUIDNamespaced = "test-uid-namespaced"
)

func stringPtr(s string) *string { return &s }

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1.AddToScheme,
		clusterpcv1alpha1.SchemeBuilder.AddToScheme,
		namespacedpcv1alpha1.SchemeBuilder.AddToScheme,
		clusterv1alpha1.SchemeBuilder.AddToScheme,
		namespacedv1alpha1.SchemeBuilder.AddToScheme,
	} {
		if err := add(s); err != nil {
			t.Fatalf("cannot build test scheme: %v", err)
		}
	}
	return s
}

func credentialsSecret(ns, name, host, username, password string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Data: map[string][]byte{
			"host":     []byte(host),
			"username": []byte(username),
			"password": []byte(password),
		},
	}
}

func newClusterNetworkContainer(crName, externalName string) *clusterv1alpha1.NetworkContainer {
	cr := &clusterv1alpha1.NetworkContainer{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: testUIDCluster},
		Spec: clusterv1alpha1.NetworkContainerSpec{
			ClusterManagedResourceSpec: xpv2.ClusterManagedResourceSpec{
				ProviderConfigReference: &xpv2.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.NetworkContainerParameters{
				NetworkView: stringPtr("default"),
				Network:     stringPtr("10.0.0.0/16"),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

func newClusterNetworkContainerIPv6(crName, externalName string) *clusterv1alpha1.NetworkContainer {
	cr := &clusterv1alpha1.NetworkContainer{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: testUIDCluster},
		Spec: clusterv1alpha1.NetworkContainerSpec{
			ClusterManagedResourceSpec: xpv2.ClusterManagedResourceSpec{
				ProviderConfigReference: &xpv2.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.NetworkContainerParameters{
				NetworkView: stringPtr("default"),
				Network:     stringPtr("2001:db8::/32"),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newClusterNetworkContainerUnknownFamily models the filterParams-only
// allocation path: no CIDR anywhere in spec, so the address family
// cannot be derived locally — see networkContainerFamily.
func newClusterNetworkContainerUnknownFamily(crName, externalName string) *clusterv1alpha1.NetworkContainer {
	prefixLen := uint(24)
	cr := &clusterv1alpha1.NetworkContainer{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: testUIDCluster},
		Spec: clusterv1alpha1.NetworkContainerSpec{
			ClusterManagedResourceSpec: xpv2.ClusterManagedResourceSpec{
				ProviderConfigReference: &xpv2.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.NetworkContainerParameters{
				NetworkView:       stringPtr("default"),
				FilterParams:      map[string]string{"Site": "dc1"},
				AllocatePrefixLen: &prefixLen,
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

func newNamespacedNetworkContainer(ns, crName, externalName, pcKind string) *namespacedv1alpha1.NetworkContainer {
	cr := &namespacedv1alpha1.NetworkContainer{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: testUIDNamespaced},
		Spec: namespacedv1alpha1.NetworkContainerSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv2.ProviderConfigReference{Kind: pcKind, Name: "default"},
			},
			ForProvider: namespacedv1alpha1.NetworkContainerParameters{
				NetworkView: stringPtr("default"),
				Network:     stringPtr("10.0.0.0/16"),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// ── mock WAPI server ─────────────────────────────────────────────────────
//
// NetworkContainer is dual-object-type: WAPI models IPv4 containers as
// "networkcontainer" and IPv6 as "ipv6networkcontainer". Both share a
// single backing map here (keyed by ref) since the mock never needs to
// distinguish family for storage — only for routing search/create
// requests to the right URL path.
type mockWapiServer struct {
	mu          sync.Mutex
	containers  map[string]*ibclient.NetworkContainer
	nextRef     int
	searchCalls int

	eaDefExists       bool
	eaDefCreateStatus int
	eaDefCreateBody   string
	eaDefSearchCalls  int
	undefinedEASearch bool
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{
		containers:  map[string]*ibclient.NetworkContainer{},
		eaDefExists: true,
	}
}

func (m *mockWapiServer) seed(nc *ibclient.NetworkContainer, isIPv6 bool) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRef++
	if nc.Ref == "" {
		objType := "networkcontainer"
		if isIPv6 {
			objType = "ipv6networkcontainer"
		}
		nc.Ref = objType + "/test" + itoa(m.nextRef) + ":" + nc.Cidr + "/" + nc.NetviewName
	}
	m.containers[nc.Ref] = nc
	return nc.Ref
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	body, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func (m *mockWapiServer) handler() http.Handler {
	mux := http.NewServeMux()

	createHandler := func(isIPv6 bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			var nc ibclient.NetworkContainer
			if err := json.NewDecoder(r.Body).Decode(&nc); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			ref := m.seed(&nc, isIPv6)
			writeJSON(w, http.StatusOK, ref)
		}
	}
	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/networkcontainer", createHandler(false))
	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/ipv6networkcontainer", createHandler(true))

	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/extensibleattributedef", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.eaDefSearchCalls++
		exists := m.eaDefExists
		m.mu.Unlock()
		if !exists {
			writeJSON(w, http.StatusOK, []ibclient.EADefinition{})
			return
		}
		name := identity.EAKey
		writeJSON(w, http.StatusOK, []ibclient.EADefinition{{Name: &name}})
	})

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/extensibleattributedef", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		status := m.eaDefCreateStatus
		body := m.eaDefCreateBody
		m.mu.Unlock()
		if status != 0 {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
			return
		}
		m.mu.Lock()
		m.eaDefExists = true
		m.mu.Unlock()
		writeJSON(w, http.StatusOK, "extensibleattributedef/test:"+url.QueryEscape(identity.EAKey))
	})

	searchHandler := func(objType string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			m.mu.Lock()
			m.searchCalls++
			m.mu.Unlock()

			q := r.URL.Query()
			eaFilters := map[string]string{}
			for k, vals := range q {
				if strings.HasPrefix(k, "*") && len(vals) > 0 {
					eaFilters[strings.TrimPrefix(k, "*")] = vals[0]
				}
			}

			m.mu.Lock()
			undefinedEA := m.undefinedEASearch
			m.mu.Unlock()
			if len(eaFilters) > 0 && undefinedEA {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"Error":"AdmConProtoError: Unknown extensible attribute: ` + identity.EAKey + `","code":"Client.Ibap.Proto","text":"Unknown extensible attribute: ` + identity.EAKey + `"}`))
				return
			}

			m.mu.Lock()
			var matches []ibclient.NetworkContainer
			for _, nc := range m.containers {
				if !strings.HasPrefix(nc.Ref, objType+"/") {
					continue
				}
				mismatch := false
				for k, v := range eaFilters {
					got, ok := nc.Ea[k]
					if !ok {
						mismatch = true
						break
					}
					if s, ok := got.(string); !ok || s != v {
						mismatch = true
						break
					}
				}
				if mismatch {
					continue
				}
				matches = append(matches, *nc)
			}
			m.mu.Unlock()
			writeJSON(w, http.StatusOK, matches)
		}
	}
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/networkcontainer", searchHandler("networkcontainer"))
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/ipv6networkcontainer", searchHandler("ipv6networkcontainer"))

	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		nc, ok := m.containers[ref]
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, nc)
	})

	mux.HandleFunc("PUT /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		existing, ok := m.containers[ref]
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var incoming ibclient.NetworkContainer
		if err := json.NewDecoder(r.Body).Decode(&incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		m.mu.Lock()
		existing.Comment = incoming.Comment
		existing.Ea = incoming.Ea
		m.mu.Unlock()
		writeJSON(w, http.StatusOK, ref)
	})

	mux.HandleFunc("DELETE /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		_, ok := m.containers[ref]
		delete(m.containers, ref)
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, ref)
	})

	return mux
}

func newTestClient(t *testing.T, srv *httptest.Server) identity.ManagerAndConnector {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("cannot parse test server URL: %v", err)
	}
	mc, err := newObjectManagerWithScheme(&nioCredentials{
		Host:     u.Hostname(),
		Username: "test-user",
		Password: "test-pass",
	}, true, "http", u.Port())
	if err != nil {
		t.Fatalf("cannot build test client: %v", err)
	}
	return mc
}

// ── Observe ──────────────────────────────────────────────────────────────

func TestClusterObserveResolvedUpToDate(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nc.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(nc, false)

	cr := newClusterNetworkContainer("my-container", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !obs.ResourceExists || !obs.ResourceUpToDate {
		t.Fatalf("expected exists+up-to-date, got %+v", obs)
	}
}

func TestClusterObserveNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterNetworkContainer("my-container", "networkcontainer/doesnotexist:10.0.0.0/16/default")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if obs.ResourceExists {
		t.Fatalf("expected ResourceExists=false, got %+v", obs)
	}
}

func TestObservePreCreateState(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterNetworkContainer("my-container", "my-container")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if obs.ResourceExists {
		t.Fatalf("expected ResourceExists=false, got %+v", obs)
	}

	m.mu.Lock()
	searchCalls := m.searchCalls
	m.mu.Unlock()
	if searchCalls == 0 {
		t.Fatal("expected the identity ladder to search by uid even in the pre-create state, got zero search calls")
	}
}

func TestClusterObserveAdoptsUnstampedObjectAndForcesUpdate(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16"}
	ref := m.seed(nc, false)

	cr := newClusterNetworkContainer("my-container", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !obs.ResourceExists {
		t.Fatalf("expected exists, got %+v", obs)
	}
	if obs.ResourceUpToDate {
		t.Fatal("adopted object must never report up to date")
	}
}

func TestClusterObserveRecoversRotatedRefAndPersistsAnnotation(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nc.Ea = identity.Stamp(nil, testUIDCluster)
	realRef := m.seed(nc, false)

	cr := newClusterNetworkContainer("my-container", "networkcontainer/stale:10.0.0.0/16/default")
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()
	e := &clusterExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !obs.ResourceExists {
		t.Fatalf("expected exists, got %+v", obs)
	}
	if meta.GetExternalName(cr) != realRef {
		t.Fatalf("expected external-name refreshed to %q, got %q", realRef, meta.GetExternalName(cr))
	}
}

func TestClusterObserveRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nc.Ea = identity.Stamp(nil, "someone-elses-uid")
	ref := m.seed(nc, false)

	cr := newClusterNetworkContainer("my-container", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("expected an error for foreign identity")
	}
}

// TestObserveFindsIPv6Object proves the identity ladder searches under
// the correct WAPI object type ("ipv6networkcontainer") when the CR's
// network CIDR is IPv6 — the dual-object-type hazard this ladder must not
// fall into (see the package doc comment).
func TestObserveFindsIPv6Object(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "2001:db8::/32"}
	nc.Ea = identity.Stamp(nil, testUIDCluster)
	m.seed(nc, true)

	// No external-name set (pre-create state): forces the identity-EA
	// search step, the only step whose WAPI endpoint depends on the
	// candidate object's assumed type. A resolving _ref would fetch by
	// literal path and mask a wrong-type newEmpty entirely.
	cr := newClusterNetworkContainerIPv6("my-container", "")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !obs.ResourceExists {
		t.Fatalf("expected the IPv6 object to be found, got %+v", obs)
	}
}

// TestObserveUnknownFamilySearchesBothTypesNotDefaultV4 proves that when
// the address family cannot be derived from spec (filterParams-only
// allocation, no CIDR anywhere in spec), the identity ladder searches
// BOTH object types rather than silently assuming IPv4 — an IPv6-family
// object stamped with this managed resource's uid must still be found.
// No external-name is set (pre-create state) so the ladder cannot skip
// straight to a ref-based fetch, which would mask the search-routing
// hazard entirely.
func TestObserveUnknownFamilySearchesBothTypesNotDefaultV4(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "2001:db8::/32"}
	nc.Ea = identity.Stamp(nil, testUIDCluster)
	m.seed(nc, true)

	cr := newClusterNetworkContainerUnknownFamily("my-container", "")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !obs.ResourceExists {
		t.Fatal("expected the dual-search fallback to find the IPv6-family object, not silently default to IPv4-only search")
	}
}

// ── Create ───────────────────────────────────────────────────────────────

func TestClusterCreateStampsIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterNetworkContainer("my-container", "my-container")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if meta.GetExternalName(cr) == "my-container" {
		t.Fatal("expected external-name to be set to the server _ref")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, nc := range m.containers {
		got, ok := nc.Ea[identity.EAKey]
		if !ok || got != testUIDCluster {
			t.Fatalf("expected identity stamp %q, got %v", testUIDCluster, nc.Ea)
		}
	}
}

func TestCreateNetworkContainerRefusesEmptyUID(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	if _, err := createNetworkContainer(mc.Manager, stringPtr("default"), stringPtr("10.0.0.0/16"), nil, nil, ""); err == nil {
		t.Fatal("expected an error for empty uid")
	}
}

// TestClusterCreateWhitespaceUIDFailsWithZeroMutatingRequests proves the
// Create path rejects a whitespace-only uid before issuing any WAPI
// call — createOrAllocateNetworkContainer's guard trims before
// comparing, matching identity.Resolve's ladder (see
// internal/clients/identity). Without the trim, a whitespace-only uid
// would pass Create's guard and get stamped verbatim into the object's
// extensible attributes, while Observe/Delete (which route through
// identity.Resolve) would treat that same object as unowned.
func TestClusterCreateWhitespaceUIDFailsWithZeroMutatingRequests(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterNetworkContainer("my-container", "my-container")
	cr.UID = "   "
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: want a hard error for a whitespace-only uid, got nil")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.containers) != 0 {
		t.Errorf("Create: len(m.containers) = %d, want 0 — a whitespace-only uid must not create anything", len(m.containers))
	}
}

// ── Update ───────────────────────────────────────────────────────────────

func TestClusterUpdateReassertsIdentityStamp(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nc.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(nc, false)

	cr := newClusterNetworkContainer("my-container", ref)
	cr.Spec.ForProvider.Comment = stringPtr("updated")
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()
	e := &clusterExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	updated := m.containers[ref]
	if updated.Ea[identity.EAKey] != testUIDCluster {
		t.Fatalf("expected identity stamp to survive update, got %v", updated.Ea)
	}
}

// TestClusterUpdatePrerequisiteAutoCreates verifies ADR-IN-0006 §6's
// unconditional Update guard: when the identity extensible attribute
// definition is absent but the configured credential can create one, the
// probe auto-creates it before the mutating PUT, and the update proceeds
// normally — this is the exact path a pre-existing, unstamped object hits
// on every reconcile (Observe resolves it as OutcomeAdopted, forcing
// Update), so the auto-create must be reachable from here, not just from
// Create.
func TestClusterUpdatePrerequisiteAutoCreates(t *testing.T) {
	m := newMockWapiServer()
	m.eaDefExists = false
	m.eaDefCreateStatus = 0
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nc.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(nc, false)

	cr := newClusterNetworkContainer("my-container", ref)
	cr.Spec.ForProvider.Comment = stringPtr("updated")
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()
	e := &clusterExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	defExists := m.eaDefExists
	m.mu.Unlock()
	if !defExists {
		t.Error("Update: eaDefExists = false, want true — the prerequisite probe must auto-create the identity definition before the mutating call")
	}
}

// TestClusterUpdatePrerequisiteRefusesUncreatable verifies ADR-IN-0006
// §6's unconditional Update guard on the refusal side: when the identity
// extensible attribute definition is absent and the configured credential
// cannot create one, Update returns the typed PrerequisiteError (not a raw
// wrapped WAPI 400) and issues no mutating call — the object is left
// exactly as it was.
func TestClusterUpdatePrerequisiteRefusesUncreatable(t *testing.T) {
	m := newMockWapiServer()
	m.eaDefExists = false
	m.eaDefCreateStatus = http.StatusForbidden
	m.eaDefCreateBody = `{"Error":"AdmConAuthError: Not authorized"}`
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nc.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(nc, false)

	cr := newClusterNetworkContainer("my-container", ref)
	cr.Spec.ForProvider.Comment = stringPtr("updated")
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()
	e := &clusterExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected an error when the identity extensible attribute definition is absent and uncreatable, got nil")
	}
	var prereq *identity.PrerequisiteError
	if !cperrors.As(err, &prereq) {
		t.Fatalf("Update: error = %v (%T), want it to wrap a *identity.PrerequisiteError", err, err)
	}

	m.mu.Lock()
	defExists := m.eaDefExists
	m.mu.Unlock()
	if defExists {
		t.Error("Update: eaDefExists = true, want false — a refused create must not be treated as success")
	}
	if got := meta.GetExternalName(cr); got != ref {
		t.Errorf("Update: external-name = %q, want unchanged %q — a refused prerequisite must issue no mutating call", got, ref)
	}
}

// TestNamespacedUpdatePrerequisiteAutoCreates verifies ADR-IN-0006 §6's
// unconditional Update guard: when the identity extensible attribute
// definition is absent but the configured credential can create one, the
// probe auto-creates it before the mutating PUT, and the update proceeds
// normally — this is the exact path a pre-existing, unstamped object hits
// on every reconcile (Observe resolves it as OutcomeAdopted, forcing
// Update), so the auto-create must be reachable from here, not just from
// Create.
func TestNamespacedUpdatePrerequisiteAutoCreates(t *testing.T) {
	m := newMockWapiServer()
	m.eaDefExists = false
	m.eaDefCreateStatus = 0
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nc.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(nc, false)

	cr := newNamespacedNetworkContainer("default", "my-container", ref, "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("updated")
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()
	e := &namespacedExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	defExists := m.eaDefExists
	m.mu.Unlock()
	if !defExists {
		t.Error("Update: eaDefExists = false, want true — the prerequisite probe must auto-create the identity definition before the mutating call")
	}
}

// TestNamespacedUpdatePrerequisiteRefusesUncreatable verifies ADR-IN-0006
// §6's unconditional Update guard on the refusal side: when the identity
// extensible attribute definition is absent and the configured credential
// cannot create one, Update returns the typed PrerequisiteError (not a raw
// wrapped WAPI 400) and issues no mutating call — the object is left
// exactly as it was.
func TestNamespacedUpdatePrerequisiteRefusesUncreatable(t *testing.T) {
	m := newMockWapiServer()
	m.eaDefExists = false
	m.eaDefCreateStatus = http.StatusForbidden
	m.eaDefCreateBody = `{"Error":"AdmConAuthError: Not authorized"}`
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nc.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(nc, false)

	cr := newNamespacedNetworkContainer("default", "my-container", ref, "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("updated")
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()
	e := &namespacedExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected an error when the identity extensible attribute definition is absent and uncreatable, got nil")
	}
	var prereq *identity.PrerequisiteError
	if !cperrors.As(err, &prereq) {
		t.Fatalf("Update: error = %v (%T), want it to wrap a *identity.PrerequisiteError", err, err)
	}

	m.mu.Lock()
	defExists := m.eaDefExists
	m.mu.Unlock()
	if defExists {
		t.Error("Update: eaDefExists = true, want false — a refused create must not be treated as success")
	}
	if got := meta.GetExternalName(cr); got != ref {
		t.Errorf("Update: external-name = %q, want unchanged %q — a refused prerequisite must issue no mutating call", got, ref)
	}
}

// TestClusterUpdateWhitespaceUIDFailsWithZeroMutatingRequests proves the
// Update path rejects a whitespace-only uid before issuing any WAPI
// call — updateNetworkContainer's guard trims before comparing,
// matching identity.Resolve's ladder (see internal/clients/identity).
// Without the trim, a whitespace-only uid would pass Update's guard and
// get re-stamped verbatim into the object's extensible attributes,
// while Observe/Delete (which route through identity.Resolve) would
// treat that same object as unowned.
func TestClusterUpdateWhitespaceUIDFailsWithZeroMutatingRequests(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16", Comment: "old comment"}
	nc.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(nc, false)

	cr := newClusterNetworkContainer("my-container", ref)
	cr.UID = "   "
	cr.Spec.ForProvider.Comment = stringPtr("new comment")
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()
	e := &clusterExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	if _, err := e.Update(context.Background(), cr); err == nil {
		t.Fatal("Update: want a hard error for a whitespace-only uid, got nil")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	comment := m.containers[ref].Comment
	if comment != "old comment" {
		t.Errorf("Update: Comment = %q, want unchanged 'old comment' — a whitespace-only uid must not mutate the object", comment)
	}
}

// ── Delete ───────────────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nc.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(nc, false)

	cr := newClusterNetworkContainer("my-container", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.containers[ref]; ok {
		t.Fatal("expected the object to be deleted")
	}
}

func TestClusterDeleteNotFoundIsSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterNetworkContainer("my-container", "networkcontainer/gone:10.0.0.0/16/default")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("expected nil error for already-gone object, got %v", err)
	}
}

func TestClusterDeleteRefusesUnverifiedOwnership(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16"}
	ref := m.seed(nc, false)

	cr := newClusterNetworkContainer("my-container", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	if _, err := e.Delete(context.Background(), cr); err == nil {
		t.Fatal("expected delete to be refused for an unstamped object")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.containers[ref]; !ok {
		t.Fatal("object must not be deleted when ownership cannot be verified")
	}
}

// ── Connect ──────────────────────────────────────────────────────────────

func TestClusterConnectProviderConfigNotFound(t *testing.T) {
	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()
	c := &clusterConnector{kube: kube, usage: resource.NewLegacyProviderConfigUsageTracker(kube, &clusterpcv1alpha1.ProviderConfigUsage{})}
	cr := newClusterNetworkContainer("my-container", "")

	if _, err := c.Connect(context.Background(), cr); err == nil {
		t.Fatal("expected an error when ProviderConfig is missing")
	}
}

func TestClusterConnectSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	u, _ := url.Parse(srv.URL)

	scheme := newTestScheme(t)
	pc := &clusterpcv1alpha1.ProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "default"},
		Spec: clusterpcv1alpha1.ProviderConfigSpec{
			Credentials: clusterpcv1alpha1.ProviderCredentials{
				Source: xpv2.CredentialsSourceSecret,
				CommonCredentialSelectors: xpv2.CommonCredentialSelectors{
					SecretRef: &xpv2.SecretKeySelector{Key: "creds", SecretReference: xpv2.SecretReference{Name: "creds", Namespace: "ns"}},
				},
			},
		},
	}
	secret := credentialsSecret("ns", "creds", u.Hostname(), "user", "pass")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pc, secret).Build()
	c := &clusterConnector{kube: kube, usage: resource.NewLegacyProviderConfigUsageTracker(kube, &clusterpcv1alpha1.ProviderConfigUsage{})}
	cr := newClusterNetworkContainer("my-container", "")

	if _, err := c.Connect(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNamespacedConnectUnsupportedKind(t *testing.T) {
	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()
	c := &namespacedConnector{kube: kube, usage: resource.NewProviderConfigUsageTracker(kube, &namespacedpcv1alpha1.ProviderConfigUsage{})}
	cr := newNamespacedNetworkContainer("ns", "my-container", "", "SomethingElse")

	if _, err := c.Connect(context.Background(), cr); err == nil {
		t.Fatal("expected an error for an unsupported providerConfigRef Kind")
	}
}

func TestNamespacedConnectWithClusterProviderConfig(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	u, _ := url.Parse(srv.URL)

	scheme := newTestScheme(t)
	cpc := &namespacedpcv1alpha1.ClusterProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "default"},
		Spec: namespacedpcv1alpha1.ProviderConfigSpec{
			Credentials: namespacedpcv1alpha1.ProviderCredentials{
				Source: xpv2.CredentialsSourceSecret,
				CommonCredentialSelectors: xpv2.CommonCredentialSelectors{
					SecretRef: &xpv2.SecretKeySelector{Key: "creds", SecretReference: xpv2.SecretReference{Name: "creds", Namespace: "ns"}},
				},
			},
		},
	}
	secret := credentialsSecret("ns", "creds", u.Hostname(), "user", "pass")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cpc, secret).Build()
	c := &namespacedConnector{kube: kube, usage: resource.NewProviderConfigUsageTracker(kube, &namespacedpcv1alpha1.ProviderConfigUsage{})}
	cr := newNamespacedNetworkContainer("ns", "my-container", "", "ClusterProviderConfig")

	if _, err := c.Connect(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ── newEmpty correctness (dual-object-type gate) ──────────────────────────

func TestNewEmptyNetworkContainerCorrectness(t *testing.T) {
	for name, isIPv6 := range map[string]bool{"IPv4": false, "IPv6": true} {
		t.Run(name, func(t *testing.T) {
			nc := newEmptyNetworkContainer(isIPv6)()
			wantType := "networkcontainer"
			if isIPv6 {
				wantType = "ipv6networkcontainer"
			}
			if nc.ObjectType() != wantType {
				t.Fatalf("expected ObjectType %q, got %q", wantType, nc.ObjectType())
			}
			found := false
			for _, f := range nc.ReturnFields() {
				if f == "extattrs" {
					found = true
				}
			}
			if !found {
				t.Fatalf("expected ReturnFields to include extattrs, got %v", nc.ReturnFields())
			}
		})
	}
}

// TestNewEmptyNetworkContainerHazard documents the dual-object-type
// hazard directly: a bare struct literal (bypassing NewNetworkContainer)
// leaves the unexported objectType field at its zero value, which would
// silently route identity searches to a WAPI endpoint that matches
// nothing.
func TestNewEmptyNetworkContainerHazard(t *testing.T) {
	bare := &ibclient.NetworkContainer{}
	if bare.ObjectType() != "" {
		t.Fatalf("expected a bare struct literal to have an empty ObjectType (documenting the hazard), got %q", bare.ObjectType())
	}
}

// ── Identity EA must never late-init into spec.forProvider ───────────────

func TestLateInitializeDoesNotLeakIdentityKeyIntoExtAttrs(t *testing.T) {
	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nc.Ea = identity.Stamp(ibclient.EA{"Site": "dc1"}, "some-uid")

	var network, comment *string
	var extAttrs map[string]string
	lateInitialize(&network, &comment, &extAttrs, nc)

	if _, ok := extAttrs[identity.EAKey]; ok {
		t.Fatalf("identity key must never late-init into spec.forProvider.extAttrs, got %v", extAttrs)
	}
	if extAttrs["Site"] != "dc1" {
		t.Fatalf("expected non-reserved EA to still be back-filled, got %v", extAttrs)
	}
}

func TestIsUpToDateIgnoresIdentityEA(t *testing.T) {
	nc := &ibclient.NetworkContainer{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nc.Ea = identity.Stamp(nil, "some-uid")

	if !isUpToDate(nil, nil, nc) {
		t.Fatal("expected isUpToDate to ignore the identity EA when spec.extAttrs is empty")
	}
}

func TestClusterDisconnectIsNoop(t *testing.T) {
	e := &clusterExternal{prober: identity.NewProber(), endpoint: t.Name()}
	if err := e.Disconnect(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNamespacedDisconnectIsNoop(t *testing.T) {
	e := &namespacedExternal{prober: identity.NewProber(), endpoint: t.Name()}
	if err := e.Disconnect(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
