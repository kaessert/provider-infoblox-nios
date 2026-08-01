// Package fixedaddress unit tests for the FixedAddress MR controllers.
// Tests use inline httptest.NewServer mocks that emulate the WAPI
// fixedaddress/ipv6fixedaddress endpoints, PascalCase test names (no
// underscores), and white-box access to the unexported connectors/clients
// so both scopes can be exercised without going through the full
// Connect() credential bridge on every test.
package fixedaddress

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	xpv2 "github.com/crossplane/crossplane-runtime/v2/apis/common/v2"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/fixedaddress/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/fixedaddress/v1alpha1"
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

func newClusterFixedAddress(crName, externalName string) *clusterv1alpha1.FixedAddress {
	cr := &clusterv1alpha1.FixedAddress{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: testUIDCluster},
		Spec: clusterv1alpha1.FixedAddressSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.FixedAddressParameters{
				IPv4Addr: stringPtr("10.0.0.10"),
				MAC:      stringPtr("00:11:22:33:44:55"),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

func newClusterFixedAddressIPv6(crName, externalName string) *clusterv1alpha1.FixedAddress {
	cr := &clusterv1alpha1.FixedAddress{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: testUIDCluster},
		Spec: clusterv1alpha1.FixedAddressSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.FixedAddressParameters{
				IPv6Addr: stringPtr("2001:db8::10"),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

func newNamespacedFixedAddress(ns, crName, externalName, pcKind string) *namespacedv1alpha1.FixedAddress {
	cr := &namespacedv1alpha1.FixedAddress{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: testUIDNamespaced},
		Spec: namespacedv1alpha1.FixedAddressSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: "default"},
			},
			ForProvider: namespacedv1alpha1.FixedAddressParameters{
				IPv4Addr: stringPtr("10.0.0.10"),
				MAC:      stringPtr("00:11:22:33:44:55"),
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
// FixedAddress is dual-object-type: WAPI models IPv4 fixed addresses as
// "fixedaddress" and IPv6 as "ipv6fixedaddress". Both share a single
// backing map here (keyed by ref) since the mock never needs to
// distinguish family for storage — only for routing search/create
// requests to the right URL path.
type mockWapiServer struct {
	mu          sync.Mutex
	addrs       map[string]*ibclient.FixedAddress
	nextRef     int
	searchCalls int

	eaDefExists bool
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{
		addrs:       map[string]*ibclient.FixedAddress{},
		eaDefExists: true,
	}
}

func (m *mockWapiServer) seed(fa *ibclient.FixedAddress, isIPv6 bool) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRef++
	if fa.Ref == "" {
		objType := "fixedaddress"
		if isIPv6 {
			objType = "ipv6fixedaddress"
		}
		fa.Ref = objType + "/test" + itoa(m.nextRef) + ":x"
	}
	m.addrs[fa.Ref] = fa
	return fa.Ref
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
			var fa ibclient.FixedAddress
			if err := json.NewDecoder(r.Body).Decode(&fa); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			ref := m.seed(&fa, isIPv6)
			writeJSON(w, http.StatusOK, ref)
		}
	}
	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/fixedaddress", createHandler(false))
	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/ipv6fixedaddress", createHandler(true))

	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/extensibleattributedef", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
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
			var matches []ibclient.FixedAddress
			for _, fa := range m.addrs {
				if !strings.HasPrefix(fa.Ref, objType+"/") {
					continue
				}
				mismatch := false
				for k, v := range eaFilters {
					got, ok := fa.Ea[k]
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
				matches = append(matches, *fa)
			}
			m.mu.Unlock()
			writeJSON(w, http.StatusOK, matches)
		}
	}
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/fixedaddress", searchHandler("fixedaddress"))
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/ipv6fixedaddress", searchHandler("ipv6fixedaddress"))

	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		fa, ok := m.addrs[ref]
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, fa)
	})

	mux.HandleFunc("PUT /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		existing, ok := m.addrs[ref]
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var incoming ibclient.FixedAddress
		if err := json.NewDecoder(r.Body).Decode(&incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		m.mu.Lock()
		existing.Comment = incoming.Comment
		existing.Ea = incoming.Ea
		existing.MatchClient = incoming.MatchClient
		existing.Mac = incoming.Mac
		m.mu.Unlock()
		writeJSON(w, http.StatusOK, ref)
	})

	mux.HandleFunc("DELETE /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		_, ok := m.addrs[ref]
		delete(m.addrs, ref)
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

	fa := &ibclient.FixedAddress{IPv4Address: "10.0.0.10", Mac: stringPtr("00:11:22:33:44:55")}
	fa.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(fa, false)

	cr := newClusterFixedAddress("my-addr", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

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

	cr := newClusterFixedAddress("my-addr", "fixedaddress/doesnotexist:x")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

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

	cr := newClusterFixedAddress("my-addr", "my-addr")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

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

	fa := &ibclient.FixedAddress{IPv4Address: "10.0.0.10", Mac: stringPtr("00:11:22:33:44:55")}
	ref := m.seed(fa, false)

	cr := newClusterFixedAddress("my-addr", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

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

	fa := &ibclient.FixedAddress{IPv4Address: "10.0.0.10", Mac: stringPtr("00:11:22:33:44:55")}
	fa.Ea = identity.Stamp(nil, testUIDCluster)
	realRef := m.seed(fa, false)

	cr := newClusterFixedAddress("my-addr", "fixedaddress/stale:x")
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()
	e := &clusterExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector}

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

	fa := &ibclient.FixedAddress{IPv4Address: "10.0.0.10", Mac: stringPtr("00:11:22:33:44:55")}
	fa.Ea = identity.Stamp(nil, "someone-elses-uid")
	ref := m.seed(fa, false)

	cr := newClusterFixedAddress("my-addr", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("expected an error for foreign identity")
	}
}

// TestObserveFindsIPv6Object proves the identity ladder searches under
// the correct WAPI object type ("ipv6fixedaddress") when the managed
// resource's family is IPv6 — the dual-object-type hazard this ladder
// must not fall into (see the package doc comment).
func TestObserveFindsIPv6Object(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	fa := &ibclient.FixedAddress{IPv6Address: "2001:db8::10"}
	fa.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(fa, true)

	cr := newClusterFixedAddressIPv6("my-addr", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !obs.ResourceExists {
		t.Fatalf("expected the IPv6 object to be found, got %+v", obs)
	}
}

// ── Create ───────────────────────────────────────────────────────────────

func TestClusterCreateStampsIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterFixedAddress("my-addr", "my-addr")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if meta.GetExternalName(cr) == "my-addr" {
		t.Fatal("expected external-name to be set to the server _ref")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, fa := range m.addrs {
		got, ok := fa.Ea[identity.EAKey]
		if !ok || got != testUIDCluster {
			t.Fatalf("expected identity stamp %q, got %v", testUIDCluster, fa.Ea)
		}
	}
}

func TestCreateFixedAddressRefusesEmptyUID(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	f := fixedAddressFields{IPv4Addr: stringPtr("10.0.0.10"), MAC: stringPtr("00:11:22:33:44:55")}
	if _, err := createFixedAddress(mc.Manager, f, ""); err == nil {
		t.Fatal("expected an error for empty uid")
	}
}

// ── Update ───────────────────────────────────────────────────────────────

func TestClusterUpdateReassertsIdentityStamp(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	fa := &ibclient.FixedAddress{IPv4Address: "10.0.0.10", Mac: stringPtr("00:11:22:33:44:55")}
	fa.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(fa, false)

	cr := newClusterFixedAddress("my-addr", ref)
	cr.Spec.ForProvider.Comment = stringPtr("updated")
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()
	e := &clusterExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	updated := m.addrs[ref]
	if updated.Ea[identity.EAKey] != testUIDCluster {
		t.Fatalf("expected identity stamp to survive update, got %v", updated.Ea)
	}
}

// ── Delete ───────────────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	fa := &ibclient.FixedAddress{IPv4Address: "10.0.0.10", Mac: stringPtr("00:11:22:33:44:55")}
	fa.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(fa, false)

	cr := newClusterFixedAddress("my-addr", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.addrs[ref]; ok {
		t.Fatal("expected the object to be deleted")
	}
}

func TestClusterDeleteNotFoundIsSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterFixedAddress("my-addr", "fixedaddress/gone:x")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("expected nil error for already-gone object, got %v", err)
	}
}

func TestClusterDeleteRefusesUnverifiedOwnership(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	fa := &ibclient.FixedAddress{IPv4Address: "10.0.0.10", Mac: stringPtr("00:11:22:33:44:55")}
	ref := m.seed(fa, false)

	cr := newClusterFixedAddress("my-addr", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Delete(context.Background(), cr); err == nil {
		t.Fatal("expected delete to be refused for an unstamped object")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.addrs[ref]; !ok {
		t.Fatal("object must not be deleted when ownership cannot be verified")
	}
}

// ── Connect ──────────────────────────────────────────────────────────────

func TestClusterConnectProviderConfigNotFound(t *testing.T) {
	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()
	c := &clusterConnector{kube: kube, usage: resource.NewLegacyProviderConfigUsageTracker(kube, &clusterpcv1alpha1.ProviderConfigUsage{})}
	cr := newClusterFixedAddress("my-addr", "")

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
	cr := newClusterFixedAddress("my-addr", "")

	if _, err := c.Connect(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNamespacedConnectUnsupportedKind(t *testing.T) {
	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()
	c := &namespacedConnector{kube: kube, usage: resource.NewProviderConfigUsageTracker(kube, &namespacedpcv1alpha1.ProviderConfigUsage{})}
	cr := newNamespacedFixedAddress("ns", "my-addr", "", "SomethingElse")

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
	cr := newNamespacedFixedAddress("ns", "my-addr", "", "ClusterProviderConfig")

	if _, err := c.Connect(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ── newEmpty correctness (dual-object-type gate) ──────────────────────────

func TestNewEmptyFixedAddressCorrectness(t *testing.T) {
	for name, isIPv6 := range map[string]bool{"IPv4": false, "IPv6": true} {
		t.Run(name, func(t *testing.T) {
			fa := newEmptyFixedAddress(isIPv6)()
			wantType := "fixedaddress"
			if isIPv6 {
				wantType = "ipv6fixedaddress"
			}
			if fa.ObjectType() != wantType {
				t.Fatalf("expected ObjectType %q, got %q", wantType, fa.ObjectType())
			}
			found := false
			for _, f := range fa.ReturnFields() {
				if f == "extattrs" {
					found = true
				}
			}
			if !found {
				t.Fatalf("expected ReturnFields to include extattrs, got %v", fa.ReturnFields())
			}
		})
	}
}

// ── Identity EA must never late-init into spec.forProvider ───────────────

func TestLateInitializeDoesNotLeakIdentityKeyIntoExtAttrs(t *testing.T) {
	fa := &ibclient.FixedAddress{IPv4Address: "10.0.0.10", Mac: stringPtr("00:11:22:33:44:55")}
	fa.Ea = identity.Stamp(ibclient.EA{"Site": "dc1"}, "some-uid")

	f := &fixedAddressFields{IPv4Addr: stringPtr(""), MAC: stringPtr("00:11:22:33:44:55")}
	lateInitialize(f, fa)

	if _, ok := f.ExtAttrs[identity.EAKey]; ok {
		t.Fatalf("identity key must never late-init into spec.forProvider.extAttrs, got %v", f.ExtAttrs)
	}
	if f.ExtAttrs["Site"] != "dc1" {
		t.Fatalf("expected non-reserved EA to still be back-filled, got %v", f.ExtAttrs)
	}
}

func TestIsUpToDateIgnoresIdentityEA(t *testing.T) {
	fa := &ibclient.FixedAddress{IPv4Address: "10.0.0.10", Mac: stringPtr("00:11:22:33:44:55")}
	fa.Ea = identity.Stamp(nil, "some-uid")

	f := fixedAddressFields{IPv4Addr: stringPtr("10.0.0.10"), MAC: stringPtr("00:11:22:33:44:55")}
	if !isUpToDate(f, fa) {
		t.Fatal("expected isUpToDate to ignore the identity EA when spec.extAttrs is empty")
	}
}

func TestClusterDisconnectIsNoop(t *testing.T) {
	e := &clusterExternal{}
	if err := e.Disconnect(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNamespacedDisconnectIsNoop(t *testing.T) {
	e := &namespacedExternal{}
	if err := e.Disconnect(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
