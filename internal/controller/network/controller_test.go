// Package network unit tests for the Network MR controllers. Tests use
// inline httptest.NewServer mocks that emulate the WAPI
// network/ipv6network endpoints, PascalCase test names (no underscores),
// and white-box access to the unexported connectors/clients so both
// scopes can be exercised without going through the full Connect()
// credential bridge on every test.
package network

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	xpv2 "github.com/crossplane/crossplane-runtime/v2/apis/common/v2"
	cperrors "github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/network/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/network/v1alpha1"
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

func newClusterNetwork(crName, externalName string) *clusterv1alpha1.Network {
	cr := &clusterv1alpha1.Network{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: testUIDCluster},
		Spec: clusterv1alpha1.NetworkSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.NetworkParameters{
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

func newClusterNetworkIPv6(crName, externalName string) *clusterv1alpha1.Network {
	cr := &clusterv1alpha1.Network{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: testUIDCluster},
		Spec: clusterv1alpha1.NetworkSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.NetworkParameters{
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

// newClusterNetworkUnknownFamily models the filterParams-only allocation
// path: no CIDR anywhere in spec, so the address family cannot be
// derived locally — see networkFamily.
func newClusterNetworkUnknownFamily(crName, externalName string) *clusterv1alpha1.Network {
	prefixLen := uint(24)
	cr := &clusterv1alpha1.Network{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: testUIDCluster},
		Spec: clusterv1alpha1.NetworkSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.NetworkParameters{
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

func newNamespacedNetwork(ns, crName, externalName, pcKind string) *namespacedv1alpha1.Network {
	cr := &namespacedv1alpha1.Network{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: testUIDNamespaced},
		Spec: namespacedv1alpha1.NetworkSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: "default"},
			},
			ForProvider: namespacedv1alpha1.NetworkParameters{
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
// Network is dual-object-type: WAPI models IPv4 networks as "network" and
// IPv6 as "ipv6network". Both share a single backing map here (keyed by
// ref) since the mock never needs to distinguish family for storage —
// only for routing search/create requests to the right URL path.
type mockWapiServer struct {
	mu          sync.Mutex
	networks    map[string]*ibclient.Network
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
		networks:    map[string]*ibclient.Network{},
		eaDefExists: true,
	}
}

func (m *mockWapiServer) seed(nw *ibclient.Network, isIPv6 bool) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRef++
	if nw.Ref == "" {
		objType := "network"
		if isIPv6 {
			objType = "ipv6network"
		}
		nw.Ref = objType + "/test" + itoa(m.nextRef) + ":" + nw.Cidr + "/" + nw.NetviewName
	}
	m.networks[nw.Ref] = nw
	return nw.Ref
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

// nextAvailableNetworkFuncRe matches the WAPI call-string AllocateNetwork
// submits as the cidr field: "func:nextavailablenetwork:<parentCidr>,
// <netview>,<prefixLen>".
var nextAvailableNetworkFuncRe = regexp.MustCompile(`^func:nextavailablenetwork:([^,]+),[^,]*,(\d+)$`)

// resolveNextAvailableNetworkFunc reports the concrete subnet this mock
// resolves a "func:nextavailablenetwork:..." call-string to: the
// parent CIDR's own network address, re-prefixed to the requested
// length. Good enough to exercise the identity ladder end-to-end
// (create -> mint a parseable _ref -> resolve); this mock performs no
// real IPAM allocation bookkeeping (never checks for exhaustion or
// overlapping allocations).
func resolveNextAvailableNetworkFunc(cidr string) (string, bool) {
	m := nextAvailableNetworkFuncRe.FindStringSubmatch(cidr)
	if m == nil {
		return "", false
	}
	parts := strings.SplitN(m[1], "/", 2)
	if len(parts) != 2 {
		return "", false
	}
	return parts[0] + "/" + m[2], true
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
			var nw ibclient.Network
			if err := json.NewDecoder(r.Body).Decode(&nw); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			// AllocateNetwork (the parentCidr allocation path) submits a
			// WAPI "func:nextavailablenetwork:<parentCidr>,<netview>,
			// <prefixLen>" call-string as the cidr field instead of a
			// literal CIDR. The real Grid resolves this to a concrete
			// subnet before minting the object's _ref; this mock does the
			// same (picking the parent's network address with the
			// requested prefix length — good enough to exercise the
			// identity ladder, not real IPAM) so the SDK's own
			// BuildNetworkFromRef/BuildIPv6NetworkFromRef ref-parsing
			// (which requires a literal dotted-decimal or IPv6 CIDR in
			// the ref) succeeds exactly as it would against a live Grid.
			if resolved, ok := resolveNextAvailableNetworkFunc(nw.Cidr); ok {
				nw.Cidr = resolved
			}
			ref := m.seed(&nw, isIPv6)
			writeJSON(w, http.StatusOK, ref)
		}
	}
	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/network", createHandler(false))
	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/ipv6network", createHandler(true))

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
			var matches []ibclient.Network
			for _, nw := range m.networks {
				if !strings.HasPrefix(nw.Ref, objType+"/") {
					continue
				}
				mismatch := false
				for k, v := range eaFilters {
					got, ok := nw.Ea[k]
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
				matches = append(matches, *nw)
			}
			m.mu.Unlock()
			writeJSON(w, http.StatusOK, matches)
		}
	}
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/network", searchHandler("network"))
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/ipv6network", searchHandler("ipv6network"))

	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		nw, ok := m.networks[ref]
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, nw)
	})

	mux.HandleFunc("PUT /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		existing, ok := m.networks[ref]
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var incoming ibclient.Network
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
		_, ok := m.networks[ref]
		delete(m.networks, ref)
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

	nw := &ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nw.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(nw, false)

	cr := newClusterNetwork("my-network", ref)
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

	cr := newClusterNetwork("my-network", "network/doesnotexist:10.0.0.0/16/default")
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

	cr := newClusterNetwork("my-network", "my-network")
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

	nw := &ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16"}
	ref := m.seed(nw, false)

	cr := newClusterNetwork("my-network", ref)
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

	nw := &ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nw.Ea = identity.Stamp(nil, testUIDCluster)
	realRef := m.seed(nw, false)

	cr := newClusterNetwork("my-network", "network/stale:10.0.0.0/16/default")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

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

	nw := &ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nw.Ea = identity.Stamp(nil, "someone-elses-uid")
	ref := m.seed(nw, false)

	cr := newClusterNetwork("my-network", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("expected an error for foreign identity")
	}
}

// TestObserveFindsIPv6Object proves the identity ladder searches under
// the correct WAPI object type ("ipv6network") when the CR's network
// CIDR is IPv6 — the dual-object-type hazard this ladder must not fall
// into (see the package doc comment). No external-name is set yet (the
// pre-create state), so observeRefFor reports "" and the ladder is
// forced onto the identity-EA search step — the one step where the
// candidate object's assumed WAPI type actually determines which
// endpoint is queried (a resolving _ref, by contrast, addresses the
// object directly and would mask a wrong-type newEmpty entirely).
func TestObserveFindsIPv6Object(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nw := &ibclient.Network{NetviewName: "default", Cidr: "2001:db8::/32"}
	nw.Ea = identity.Stamp(nil, testUIDCluster)
	m.seed(nw, true)

	cr := newClusterNetworkIPv6("my-network", "")
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

	nw := &ibclient.Network{NetviewName: "default", Cidr: "2001:db8::/32"}
	nw.Ea = identity.Stamp(nil, testUIDCluster)
	m.seed(nw, true)

	cr := newClusterNetworkUnknownFamily("my-network", "")
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

	cr := newClusterNetwork("my-network", "my-network")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if meta.GetExternalName(cr) == "my-network" {
		t.Fatal("expected external-name to be set to the server _ref")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, nw := range m.networks {
		got, ok := nw.Ea[identity.EAKey]
		if !ok || got != testUIDCluster {
			t.Fatalf("expected identity stamp %q, got %v", testUIDCluster, nw.Ea)
		}
	}
}

func TestCreateNetworkRefusesEmptyUID(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	if _, err := createNetwork(mc.Manager, stringPtr("default"), stringPtr("10.0.0.0/16"), nil, nil, ""); err == nil {
		t.Fatal("expected an error for empty uid")
	}
}

// TestClusterCreateWhitespaceUIDFailsWithZeroMutatingRequests proves the
// Create path rejects a whitespace-only uid before issuing any WAPI
// call — createOrAllocateNetwork's guard trims before comparing,
// matching identity.Resolve's ladder (see internal/clients/identity).
// Without the trim, a whitespace-only uid would pass Create's guard and
// get stamped verbatim into the object's extensible attributes, while
// Observe/Delete (which route through identity.Resolve) would treat
// that same object as unowned.
func TestClusterCreateWhitespaceUIDFailsWithZeroMutatingRequests(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterNetwork("my-network", "my-network")
	cr.UID = "   "
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: want a hard error for a whitespace-only uid, got nil")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.networks) != 0 {
		t.Errorf("Create: len(m.networks) = %d, want 0 — a whitespace-only uid must not create anything", len(m.networks))
	}
}

// ── Update ───────────────────────────────────────────────────────────────

func TestClusterUpdateReassertsIdentityStamp(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nw := &ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nw.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(nw, false)

	cr := newClusterNetwork("my-network", ref)
	cr.Spec.ForProvider.Comment = stringPtr("updated")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	updated := m.networks[ref]
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

	nw := &ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nw.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(nw, false)

	cr := newClusterNetwork("my-network", ref)
	cr.Spec.ForProvider.Comment = stringPtr("updated")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}

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

	nw := &ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nw.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(nw, false)

	cr := newClusterNetwork("my-network", ref)
	cr.Spec.ForProvider.Comment = stringPtr("updated")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}

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

	nw := &ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nw.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(nw, false)

	cr := newNamespacedNetwork("default", "my-network", ref, "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("updated")
	e := &namespacedExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}

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

	nw := &ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nw.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(nw, false)

	cr := newNamespacedNetwork("default", "my-network", ref, "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("updated")
	e := &namespacedExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}

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
// call — updateNetwork's guard trims before comparing, matching
// identity.Resolve's ladder (see internal/clients/identity). Without the
// trim, a whitespace-only uid would pass Update's guard and get
// re-stamped verbatim into the object's extensible attributes, while
// Observe/Delete (which route through identity.Resolve) would treat
// that same object as unowned.
func TestClusterUpdateWhitespaceUIDFailsWithZeroMutatingRequests(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	nw := &ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16", Comment: "old comment"}
	nw.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(nw, false)

	cr := newClusterNetwork("my-network", ref)
	cr.UID = "   "
	cr.Spec.ForProvider.Comment = stringPtr("new comment")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	if _, err := e.Update(context.Background(), cr); err == nil {
		t.Fatal("Update: want a hard error for a whitespace-only uid, got nil")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	comment := m.networks[ref].Comment
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

	nw := &ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nw.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(nw, false)

	cr := newClusterNetwork("my-network", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.networks[ref]; ok {
		t.Fatal("expected the object to be deleted")
	}
}

func TestClusterDeleteNotFoundIsSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterNetwork("my-network", "network/gone:10.0.0.0/16/default")
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

	nw := &ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16"}
	ref := m.seed(nw, false)

	cr := newClusterNetwork("my-network", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

	if _, err := e.Delete(context.Background(), cr); err == nil {
		t.Fatal("expected delete to be refused for an unstamped object")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.networks[ref]; !ok {
		t.Fatal("object must not be deleted when ownership cannot be verified")
	}
}

// ── Connect ──────────────────────────────────────────────────────────────

func TestClusterConnectProviderConfigNotFound(t *testing.T) {
	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()
	c := &clusterConnector{kube: kube, usage: resource.NewLegacyProviderConfigUsageTracker(kube, &clusterpcv1alpha1.ProviderConfigUsage{})}
	cr := newClusterNetwork("my-network", "")

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
				Source: xpv1.CredentialsSourceSecret,
				CommonCredentialSelectors: xpv1.CommonCredentialSelectors{
					SecretRef: &xpv1.SecretKeySelector{Key: "creds", SecretReference: xpv1.SecretReference{Name: "creds", Namespace: "ns"}},
				},
			},
		},
	}
	secret := credentialsSecret("ns", "creds", u.Hostname(), "user", "pass")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pc, secret).Build()
	c := &clusterConnector{kube: kube, usage: resource.NewLegacyProviderConfigUsageTracker(kube, &clusterpcv1alpha1.ProviderConfigUsage{})}
	cr := newClusterNetwork("my-network", "")

	if _, err := c.Connect(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNamespacedConnectUnsupportedKind(t *testing.T) {
	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()
	c := &namespacedConnector{kube: kube, usage: resource.NewProviderConfigUsageTracker(kube, &namespacedpcv1alpha1.ProviderConfigUsage{})}
	cr := newNamespacedNetwork("ns", "my-network", "", "SomethingElse")

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
				Source: xpv1.CredentialsSourceSecret,
				CommonCredentialSelectors: xpv1.CommonCredentialSelectors{
					SecretRef: &xpv1.SecretKeySelector{Key: "creds", SecretReference: xpv1.SecretReference{Name: "creds", Namespace: "ns"}},
				},
			},
		},
	}
	secret := credentialsSecret("ns", "creds", u.Hostname(), "user", "pass")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cpc, secret).Build()
	c := &namespacedConnector{kube: kube, usage: resource.NewProviderConfigUsageTracker(kube, &namespacedpcv1alpha1.ProviderConfigUsage{})}
	cr := newNamespacedNetwork("ns", "my-network", "", "ClusterProviderConfig")

	if _, err := c.Connect(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ── newEmpty correctness (dual-object-type gate) ──────────────────────────

func TestNewEmptyNetworkCorrectness(t *testing.T) {
	for name, isIPv6 := range map[string]bool{"IPv4": false, "IPv6": true} {
		t.Run(name, func(t *testing.T) {
			nw := newEmptyNetwork(isIPv6)()
			wantType := "network"
			if isIPv6 {
				wantType = "ipv6network"
			}
			if nw.ObjectType() != wantType {
				t.Fatalf("expected ObjectType %q, got %q", wantType, nw.ObjectType())
			}
			found := false
			for _, f := range nw.ReturnFields() {
				if f == "extattrs" {
					found = true
				}
			}
			if !found {
				t.Fatalf("expected ReturnFields to include extattrs, got %v", nw.ReturnFields())
			}
		})
	}
}

// TestNewEmptyNetworkHazard documents the dual-object-type hazard
// directly: a bare struct literal (bypassing NewNetwork) leaves the
// unexported objectType field at its zero value, which would silently
// route identity searches to a WAPI endpoint that matches nothing.
func TestNewEmptyNetworkHazard(t *testing.T) {
	bare := &ibclient.Network{}
	if bare.ObjectType() != "" {
		t.Fatalf("expected a bare struct literal to have an empty ObjectType (documenting the hazard), got %q", bare.ObjectType())
	}
}

// ── Identity EA must never late-init into spec.forProvider ───────────────

func TestLateInitializeDoesNotLeakIdentityKeyIntoExtAttrs(t *testing.T) {
	nw := &ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nw.Ea = identity.Stamp(ibclient.EA{"Site": "dc1"}, "some-uid")

	var network, comment *string
	var extAttrs map[string]string
	lateInitialize(&network, &comment, &extAttrs, nw)

	if _, ok := extAttrs[identity.EAKey]; ok {
		t.Fatalf("identity key must never late-init into spec.forProvider.extAttrs, got %v", extAttrs)
	}
	if extAttrs["Site"] != "dc1" {
		t.Fatalf("expected non-reserved EA to still be back-filled, got %v", extAttrs)
	}
}

func TestIsUpToDateIgnoresIdentityEA(t *testing.T) {
	nw := &ibclient.Network{NetviewName: "default", Cidr: "10.0.0.0/16"}
	nw.Ea = identity.Stamp(nil, "some-uid")

	if !isUpToDate(nil, nil, nw) {
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
