// Package network unit tests for the Network MR controllers. Tests use
// inline httptest.NewServer mocks that emulate the WAPI network/ipv6network
// endpoints, PascalCase test names (no underscores), and white-box access
// to the unexported connectors/clients so both scopes can be exercised
// without going through the full Connect() credential bridge on every
// test.
package network

import (
	"context"
	"encoding/json"
	"net"
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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/network/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/network/v1alpha1"
	namespacedpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
)

// ── generic helpers ─────────────────────────────────────────────────────────

// Shared literals reused across the many table-driven and seed fixtures in
// this file, hoisted into constants to avoid goconst duplication warnings.
const (
	testCIDR      = "10.0.0.0/24"
	testNamespace = "default"
	testEAKey     = "env"
	testEAVal     = "prod"
)

func stringPtr(s string) *string { return &s }

func uintPtr(u uint) *uint { return &u }

// newTestScheme returns a scheme with corev1 (for Secrets) and the
// provider's API types registered.
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

// credentialsSecret returns a Secret carrying the host/username/password
// keys the credential bridge expects.
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

// newClusterNetwork builds a minimal cluster-scoped Network CR. When
// externalName is empty, the external-name annotation is left unset. When
// it equals crName it simulates the framework's NameAsExternalName
// initializer (the pre-create state); any other value simulates a
// Create()-assigned server ref.
func newClusterNetwork(crName, externalName string) *clusterv1alpha1.Network {
	cr := &clusterv1alpha1.Network{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: "test-uid-cluster"},
		Spec: clusterv1alpha1.NetworkSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: testNamespace},
			},
			ForProvider: clusterv1alpha1.NetworkParameters{
				NetworkView: stringPtr(testNamespace),
				Network:     stringPtr(testCIDR),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newNamespacedNetwork is the namespaced variant of newClusterNetwork.
func newNamespacedNetwork(ns, crName, externalName, pcKind string) *namespacedv1alpha1.Network {
	cr := &namespacedv1alpha1.Network{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: "test-uid-namespaced"},
		Spec: namespacedv1alpha1.NetworkSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: testNamespace},
			},
			ForProvider: namespacedv1alpha1.NetworkParameters{
				NetworkView: stringPtr(testNamespace),
				Network:     stringPtr(testCIDR),
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
// mockWapiServer emulates the subset of NIOS WAPI network/ipv6network
// endpoints exercised by the Network controller (POST create for both
// object types, GET/PUT/DELETE by _ref). Records are marshaled/unmarshaled
// using the real ibclient.Network type so the wire format (including the
// EA {"value": ...} envelope) exactly matches what the SDK sends and
// expects.

type mockWapiServer struct {
	mu       sync.Mutex
	networks map[string]*ibclient.Network
	nextRef  int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert immutable fields are omitted.
	lastUpdateBody []byte

	// lastCreateBody captures the raw JSON body of the most recent POST
	// (create/allocate) request, for tests that assert allocation-path
	// request shape — e.g. that the "object" field reaches the
	// AllocateNetworkByEA request's "_object" filter.
	lastCreateBody []byte
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{networks: map[string]*ibclient.Network{}}
}

// seed pre-populates the mock server with a network. isIPv6 controls the
// generated ref's object-type prefix ("network/" vs "ipv6network/") when
// the caller has not already set Ref.
func (m *mockWapiServer) seed(nw *ibclient.Network, isIPv6 bool) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRef++
	if nw.Ref == "" {
		nw.Ref = m.newRefLocked(nw, isIPv6)
	}
	m.networks[nw.Ref] = nw
	return nw.Ref
}

func (m *mockWapiServer) newRefLocked(nw *ibclient.Network, isIPv6 bool) string {
	objType := "network"
	if isIPv6 {
		objType = "ipv6network"
	}
	return objType + "/test" + itoa(m.nextRef) + ":" + nw.Cidr + "/" + nw.NetviewName
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
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

// nextAvailableNetworkRequest mirrors the wire shape of the SDK's
// NetworkContainerNextAvailable object (the request body AllocateNetworkByEA
// sends) — a nested "network" object carrying the EA search filter and the
// requested prefix length, rather than a plain CIDR string. Probing the raw
// "network" field's JSON type (object vs string) is what distinguishes an
// AllocateNetworkByEA request from a static-CIDR/AllocateNetwork request in
// createHandler below.
type nextAvailableNetworkRequest struct {
	Network *struct {
		Object       string            `json:"_object"`
		ObjectParams map[string]string `json:"_object_parameters"`
		Params       map[string]uint   `json:"_parameters"`
	} `json:"network"`
	NetviewName string      `json:"network_view"`
	Comment     string      `json:"comment"`
	Ea          ibclient.EA `json:"extattrs"`
}

// resolveNextAvailableCIDR fabricates a deterministic "allocated" CIDR for
// the mock server's AllocateNetwork path, given the SDK's
// "func:nextavailablenetwork:<parentCidr>,<netview>,<prefixLen>" encoding.
// Real WAPI carves an actual free subnet out of the parent; the mock only
// needs a syntactically valid CIDR at the requested prefix length so the
// SDK's ref-parsing regex (BuildNetworkFromRef) succeeds.
func resolveNextAvailableCIDR(funcCall string) string {
	parts := strings.SplitN(funcCall, ":", 3)
	if len(parts) != 3 {
		return funcCall
	}
	fields := strings.Split(parts[2], ",")
	if len(fields) < 3 {
		return funcCall
	}
	parentCidr := fields[0]
	prefixLen := fields[len(fields)-1]
	ip, _, err := net.ParseCIDR(parentCidr)
	if err != nil {
		return funcCall
	}
	return ip.String() + "/" + prefixLen
}

// handler returns an http.Handler implementing the network/ipv6network
// WAPI surface.
func (m *mockWapiServer) handler() http.Handler {
	mux := http.NewServeMux()

	createHandler := func(isIPv6 bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			body, err := readAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			m.mu.Lock()
			m.lastCreateBody = body
			m.mu.Unlock()

			// Probe the raw "network" field's JSON type: a nested object
			// means this is an AllocateNetworkByEA request; a string (or
			// absent) means static-CIDR create or AllocateNetwork.
			var probe struct {
				Network json.RawMessage `json:"network"`
			}
			if err := json.Unmarshal(body, &probe); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			var nw ibclient.Network
			switch {
			case len(probe.Network) > 0 && probe.Network[0] == '{':
				var req nextAvailableNetworkRequest
				if err := json.Unmarshal(body, &req); err != nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				var prefixLen uint
				if req.Network != nil {
					prefixLen = req.Network.Params["cidr"]
				}
				// The mock allocates from a fixed pool for EA-matched
				// containers — real WAPI would resolve _object_parameters
				// against actual container EAs server-side.
				nw = ibclient.Network{
					NetviewName: req.NetviewName,
					Cidr:        "192.168.100.0/" + itoa(int(prefixLen)),
					Comment:     req.Comment,
					Ea:          req.Ea,
				}
			default:
				if err := json.Unmarshal(body, &nw); err != nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if strings.HasPrefix(nw.Cidr, "func:nextavailablenetwork:") {
					nw.Cidr = resolveNextAvailableCIDR(nw.Cidr)
				}
			}

			ref := m.seed(&nw, isIPv6)
			writeJSON(w, http.StatusOK, ref)
		}
	}
	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/network", createHandler(false))
	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/ipv6network", createHandler(true))

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

		body, err := readAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var incoming ibclient.Network
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body
		existing.Comment = incoming.Comment
		existing.Ea = incoming.Ea
		// network_view/network (cidr) are immutable — the mock never
		// applies incoming values for them, mirroring live WAPI
		// behavior (UpdateNetwork has no parameter for either).
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

func readAll(rc interface{ Read([]byte) (int, error) }) ([]byte, error) {
	buf := make([]byte, 0, 512)
	tmp := make([]byte, 512)
	for {
		n, err := rc.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil {
			if err.Error() == "EOF" {
				return buf, nil
			}
			return buf, err
		}
	}
}

// fixedStatusHandler always responds with the given HTTP status — used to
// exercise the generic (non-404) error classification paths.
func fixedStatusHandler(status int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"Error":"boom"}`))
	})
}

// newTestObjectManager builds an ibclient.IBObjectManager pointed at the
// given httptest.Server via plain HTTP (no TLS needed — the WapiRequestBuilder
// only switches to HTTPS when hostCfg.Scheme != "http").
func newTestObjectManager(t *testing.T, srv *httptest.Server) ibclient.IBObjectManager {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("cannot parse test server URL: %v", err)
	}
	objMgr, err := newObjectManagerWithScheme(&nioCredentials{
		Host:     u.Hostname(),
		Username: "test-user",
		Password: "test-pass",
	}, "http", u.Port())
	if err != nil {
		t.Fatalf("cannot build test object manager: %v", err)
	}
	return objMgr
}

// ── cluster: Observe ────────────────────────────────────────────────────

func TestClusterObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Network{
		NetviewName: testNamespace,
		Cidr:        testCIDR,
		Comment:     "hello",
		Ea:          ibclient.EA{testEAKey: testEAVal},
	}, false)

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetwork("my-network", ref)
	cr.Spec.ForProvider.Comment = stringPtr("hello")
	cr.Spec.ForProvider.ExtAttrs = map[string]string{testEAKey: testEAVal}

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Error("Observe: want ResourceExists=true, got false")
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true, got false")
	}
	if cr.Status.AtProvider.ID != ref {
		t.Errorf("AtProvider.ID = %q, want %q", cr.Status.AtProvider.ID, ref)
	}
	if cr.Status.AtProvider.Network == nil || *cr.Status.AtProvider.Network != testCIDR {
		t.Errorf("AtProvider.Network = %v, want 10.0.0.0/24", cr.Status.AtProvider.Network)
	}
	if cond := cr.GetCondition(xpv1.TypeReady); cond.Status != corev1.ConditionTrue {
		t.Errorf("condition Ready = %v, want True", cond.Status)
	}
}

func TestClusterObserveLateInitializesNetworkAfterAllocation(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	// Simulates a resource created via the parentCidr/filterParams
	// allocation path: spec.forProvider.network was never set by the
	// user, only the server knows the allocated CIDR.
	ref := m.seed(&ibclient.Network{
		NetviewName: testNamespace,
		Cidr:        "10.0.5.0/24",
	}, false)

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetwork("my-network", ref)
	cr.Spec.ForProvider.Network = nil
	cr.Spec.ForProvider.ParentCidr = stringPtr("10.0.0.0/8")
	cr.Spec.ForProvider.AllocatePrefixLen = uintPtr(24)

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceLateInitialized {
		t.Error("Observe: want ResourceLateInitialized=true after backfilling network from the allocated CIDR, got false")
	}
	if cr.Spec.ForProvider.Network == nil || *cr.Spec.ForProvider.Network != "10.0.5.0/24" {
		t.Errorf("Spec.ForProvider.Network = %v, want 10.0.5.0/24", cr.Spec.ForProvider.Network)
	}
}

func TestClusterObserveNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetwork("my-network", "network/does-not-exist:10.0.0.0/24/default")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false for 404, got true")
	}
}

// TestObservePreCreateState verifies that Observe short-circuits (no HTTP
// call) when the external-name still equals the CR's Kubernetes name — the
// pre-create state for a server-assigned external-name strategy.
func TestObservePreCreateState(t *testing.T) {
	// Zero-route server: any request is an error, proving Observe never
	// calls it during the pre-create guard.
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetwork("my-network", "") // external-name unset
	meta.SetExternalName(cr, cr.GetName())    // simulate NameAsExternalName initializer

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false in pre-create state, got true")
	}
}

func TestClusterObserveServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetwork("my-network", "network/test1:10.0.0.0/24/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetwork("my-network", "network/test1:10.0.0.0/24/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestClusterObserveMinimalResponse verifies that Observe() does not panic
// and correctly leaves every optional AtProvider field nil/empty when the
// WAPI response carries only the server-assigned _ref and none of
// network_view, network, comment, extattrs, or members — the minimal
// shape a Network response can take.
func TestClusterObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Network{Ref: "network/minimal:10.0.0.0/24/default"}, false)

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetwork("my-network", ref)

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error on minimal response: %v", err)
	}
	if !got.ResourceExists {
		t.Error("Observe: want ResourceExists=true for minimal response, got false")
	}

	ap := cr.Status.AtProvider
	if ap.ID != ref {
		t.Errorf("AtProvider.ID = %q, want %q", ap.ID, ref)
	}
	if ap.NetworkView != nil {
		t.Errorf("AtProvider.NetworkView = %v, want nil", ap.NetworkView)
	}
	if ap.Network != nil {
		t.Errorf("AtProvider.Network = %v, want nil", ap.Network)
	}
	if ap.Comment != nil {
		t.Errorf("AtProvider.Comment = %v, want nil", ap.Comment)
	}
	if ap.ExtAttrs != nil {
		t.Errorf("AtProvider.ExtAttrs = %v, want nil", ap.ExtAttrs)
	}
	if len(ap.Members) != 0 {
		t.Errorf("AtProvider.Members = %v, want empty", ap.Members)
	}
}

// TestClusterObserveIsUpToDateIgnoresImmutableField verifies that drift on
// networkView/network (both immutable — absent from UpdateNetwork's SDK
// signature) does not flip ResourceUpToDate to false.
func TestClusterObserveIsUpToDateIgnoresImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Network{
		NetviewName: "original-view",
		Cidr:        testCIDR,
	}, false)

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetwork("my-network", ref)
	// Mutate the immutable fields in spec — this must NOT affect
	// ResourceUpToDate.
	cr.Spec.ForProvider.NetworkView = stringPtr("changed-view")
	cr.Spec.ForProvider.Network = stringPtr("10.0.1.0/24")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true despite networkView/network drift (immutable fields), got false")
	}
}

// TestClusterObserveIsUpToDateIgnoresAllocationFields verifies that
// parentCidr, allocatePrefixLen, and filterParams — create-time-only
// inputs to the allocation call, never echoed back by the WAPI response —
// never trigger a spurious Update. isUpToDate's signature does not even
// accept these fields, but this test guards that invariant explicitly so
// a future refactor cannot accidentally wire them in.
func TestClusterObserveIsUpToDateIgnoresAllocationFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Network{
		NetviewName: testNamespace,
		Cidr:        testCIDR,
	}, false)

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetwork("my-network", ref)
	// Simulate drift on the allocation-only fields after creation — none
	// of these are ever part of the WAPI response, so none should affect
	// ResourceUpToDate.
	cr.Spec.ForProvider.ParentCidr = stringPtr("10.0.0.0/8")
	cr.Spec.ForProvider.AllocatePrefixLen = uintPtr(24)
	cr.Spec.ForProvider.FilterParams = map[string]string{"region": "us-east"}

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true despite parentCidr/allocatePrefixLen/filterParams drift (allocation-only fields), got false")
	}
}

func TestClusterObserveDetectsCommentDrift(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Network{
		NetviewName: testNamespace,
		Cidr:        testCIDR,
		Comment:     "old comment",
	}, false)

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetwork("my-network", ref)
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=false for comment drift, got true")
	}
}

// ── cluster: Create ─────────────────────────────────────────────────────

func TestClusterCreateSuccessIPv4(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetwork("my-network", "") // no external-name yet

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
	if !strings.HasPrefix(got, "network/") {
		t.Errorf("Create: external-name = %q, want network/ prefix for IPv4 CIDR", got)
	}
}

func TestClusterCreateSuccessIPv6(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetwork("my-network", "")
	cr.Spec.ForProvider.Network = stringPtr("2001:db8::/64")

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if !strings.HasPrefix(got, "ipv6network/") {
		t.Errorf("Create: external-name = %q, want ipv6network/ prefix for IPv6 CIDR", got)
	}
}

// ── cluster: Create — allocation paths ───────────────────────────────────

func TestClusterCreateAllocateFromParentCidr(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetwork("my-network", "")
	cr.Spec.ForProvider.Network = nil
	cr.Spec.ForProvider.ParentCidr = stringPtr("10.0.0.0/8")
	cr.Spec.ForProvider.AllocatePrefixLen = uintPtr(24)

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
	if !strings.HasPrefix(got, "network/") {
		t.Errorf("Create: external-name = %q, want network/ prefix", got)
	}
	if !strings.Contains(got, "/24/") {
		t.Errorf("Create: external-name = %q, want the allocated /24 subnet in the ref", got)
	}
}

func TestClusterCreateAllocateByFilterParams(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetwork("my-network", "")
	cr.Spec.ForProvider.Network = nil
	cr.Spec.ForProvider.FilterParams = map[string]string{"region": "us-east"}
	cr.Spec.ForProvider.AllocatePrefixLen = uintPtr(28)
	cr.Spec.ForProvider.Object = stringPtr("networkcontainer")

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
	if !strings.Contains(got, "/28/") {
		t.Errorf("Create: external-name = %q, want the allocated /28 subnet in the ref", got)
	}
}

// TestClusterCreateAllocateByEAWithObjectField verifies that Network's
// object field reaches the AllocateNetworkByEA call and selects the
// EA-search container type in the WAPI request: object="network" targets
// the "network"/"ipv6network" object type directly (rather than the
// default "networkcontainer"/"ipv6networkcontainer").
func TestClusterCreateAllocateByEAWithObjectField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetwork("my-network", "")
	cr.Spec.ForProvider.Network = nil
	cr.Spec.ForProvider.FilterParams = map[string]string{"region": "us-east"}
	cr.Spec.ForProvider.AllocatePrefixLen = uintPtr(28)
	cr.Spec.ForProvider.Object = stringPtr("network")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	var req nextAvailableNetworkRequest
	if err := json.Unmarshal(m.lastCreateBody, &req); err != nil {
		t.Fatalf("cannot unmarshal captured create request: %v", err)
	}
	if req.Network == nil {
		t.Fatal("Create: want a nested next-available-network request body, got none")
	}
	if req.Network.Object != "network" {
		t.Errorf("Create: request _object = %q, want %q (object field not passed through to AllocateNetworkByEA)", req.Network.Object, "network")
	}
}

// TestClusterCreateAllocateByEADefaultsObjectToNetworkContainer verifies
// the counterpart of TestClusterCreateAllocateByEAWithObjectField — when
// object is left unset, AllocateNetworkByEA searches "networkcontainer"
// (the SDK's non-"network" fallback), not "network".
func TestClusterCreateAllocateByEADefaultsObjectToNetworkContainer(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetwork("my-network", "")
	cr.Spec.ForProvider.Network = nil
	cr.Spec.ForProvider.FilterParams = map[string]string{"region": "us-east"}
	cr.Spec.ForProvider.AllocatePrefixLen = uintPtr(28)
	// Object intentionally left nil.

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	var req nextAvailableNetworkRequest
	if err := json.Unmarshal(m.lastCreateBody, &req); err != nil {
		t.Fatalf("cannot unmarshal captured create request: %v", err)
	}
	if req.Network == nil {
		t.Fatal("Create: want a nested next-available-network request body, got none")
	}
	if req.Network.Object != "networkcontainer" {
		t.Errorf("Create: request _object = %q, want %q when object is unset", req.Network.Object, "networkcontainer")
	}
}

func TestCreateValidationRejectsParentCidrAndFilterParams(t *testing.T) {
	_, err := createOrAllocateNetwork(nil, stringPtr(testNamespace), nil, stringPtr("10.0.0.0/8"), nil, nil, uintPtr(24), map[string]string{"region": "us-east"}, nil)
	if err == nil {
		t.Fatal("createOrAllocateNetwork: want error when parentCidr and filterParams are both set, got nil")
	}
}

func TestCreateValidationRequiresAllocatePrefixLenForParentCidr(t *testing.T) {
	_, err := createOrAllocateNetwork(nil, stringPtr(testNamespace), nil, stringPtr("10.0.0.0/8"), nil, nil, nil, nil, nil)
	if err == nil {
		t.Fatal("createOrAllocateNetwork: want error when parentCidr is set without allocatePrefixLen, got nil")
	}
}

func TestCreateValidationRequiresAllocatePrefixLenForFilterParams(t *testing.T) {
	_, err := createOrAllocateNetwork(nil, stringPtr(testNamespace), nil, nil, nil, nil, nil, map[string]string{"region": "us-east"}, nil)
	if err == nil {
		t.Fatal("createOrAllocateNetwork: want error when filterParams is set without allocatePrefixLen, got nil")
	}
}

// ── cluster: Update ─────────────────────────────────────────────────────

func TestClusterUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Network{
		NetviewName: testNamespace,
		Cidr:        testCIDR,
		Comment:     "old comment",
	}, false)

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetwork("my-network", ref)
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.networks[ref]
	m.mu.Unlock()
	if stored.Comment != "new comment" {
		t.Errorf("Update: stored comment = %q, want %q", stored.Comment, "new comment")
	}
}

// TestClusterUpdateRefStable verifies that the external-name annotation
// (the WAPI _ref) is left untouched by Update — networkView/cidr are
// immutable, so the _ref returned at Create time never needs to be
// refreshed after a PUT.
func TestClusterUpdateRefStable(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Network{
		NetviewName: testNamespace,
		Cidr:        testCIDR,
		Comment:     "old comment",
	}, false)

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetwork("my-network", ref)
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	before := meta.GetExternalName(cr)
	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	after := meta.GetExternalName(cr)
	if after != before {
		t.Errorf("Update: external-name changed from %q to %q, want unchanged (ref is stable)", before, after)
	}
	if after != ref {
		t.Errorf("Update: external-name = %q, want original ref %q", after, ref)
	}
}

// TestClusterUpdateDoesNotSendImmutableField verifies that network_view is
// never present in the PUT request body — the SDK's UpdateNetwork method
// clears NetviewName to "" before issuing the request (which the
// omitempty tag then drops from the wire payload), and the method's
// signature has no parameter accepting it from spec at all. The CIDR
// (network) field is separately fetched by UpdateNetwork's internal GET
// and echoed back unchanged as part of the SDK's partial-merge PUT — this
// is a fixed property of the sanctioned SDK call, not something the
// controller ever populates from spec.ForProvider.Network.
func TestClusterUpdateDoesNotSendImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Network{
		NetviewName: testNamespace,
		Cidr:        testCIDR,
	}, false)

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetwork("my-network", ref)

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	body := m.lastUpdateBody
	m.mu.Unlock()

	var raw map[string]interface{}
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("cannot decode captured PUT body: %v", err)
	}
	if _, present := raw["network_view"]; present {
		t.Errorf("Update: request body contains immutable field 'network_view': %v", raw["network_view"])
	}
}

// ── cluster: Delete ─────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Network{NetviewName: testNamespace, Cidr: testCIDR}, false)

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetwork("my-network", ref)

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}

	m.mu.Lock()
	_, stillExists := m.networks[ref]
	m.mu.Unlock()
	if stillExists {
		t.Error("Delete: network still present after Delete")
	}
}

func TestClusterDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetwork("my-network", "network/does-not-exist:10.0.0.0/24/default")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error for already-gone resource, got: %v", err)
	}
}

// TestClusterDeleteServerError verifies that a 5xx response from the WAPI
// delete endpoint is propagated (wrapped, not swallowed) rather than being
// treated as a not-found/already-deleted success.
func TestClusterDeleteServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetwork("my-network", "network/test1:10.0.0.0/24/default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteNetwork) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteNetwork)
	}
}

// ── cluster: Disconnect ──────────────────────────────────────────────────

func TestClusterDisconnectIsNoop(t *testing.T) {
	e := &clusterExternal{}
	if err := e.Disconnect(context.Background()); err != nil {
		t.Errorf("Disconnect: unexpected error: %v", err)
	}
}

// ── cluster: Connect ─────────────────────────────────────────────────────

func TestClusterConnectSuccess(t *testing.T) {
	const (
		ns     = "crossplane-system"
		secret = "infobloxnios-api-key"
	)

	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			credentialsSecret(ns, secret, "grid.example.com", "admin", "s3cr3t"),
			&clusterpcv1alpha1.ProviderConfig{
				ObjectMeta: metav1.ObjectMeta{Name: testNamespace},
				Spec: clusterpcv1alpha1.ProviderConfigSpec{
					Credentials: clusterpcv1alpha1.ProviderCredentials{
						Source: xpv1.CredentialsSourceSecret,
						CommonCredentialSelectors: xpv1.CommonCredentialSelectors{
							SecretRef: &xpv1.SecretKeySelector{
								SecretReference: xpv1.SecretReference{Name: secret, Namespace: ns},
								Key:             "unused",
							},
						},
					},
				},
			},
		).Build()

	conn := &clusterConnector{
		kube:  kube,
		usage: resource.NewLegacyProviderConfigUsageTracker(kube, &clusterpcv1alpha1.ProviderConfigUsage{}),
	}

	cr := newClusterNetwork("my-network", "")
	got, err := conn.Connect(context.Background(), cr)
	if err != nil {
		t.Fatalf("Connect: unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("Connect: expected non-nil ExternalClient, got nil")
	}
}

func TestClusterConnectProviderConfigNotFound(t *testing.T) {
	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()

	conn := &clusterConnector{
		kube:  kube,
		usage: resource.NewLegacyProviderConfigUsageTracker(kube, &clusterpcv1alpha1.ProviderConfigUsage{}),
	}

	cr := newClusterNetwork("my-network", "")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for missing ProviderConfig, got nil")
	}
}

// ── namespaced: Observe ──────────────────────────────────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Network{
		NetviewName: testNamespace,
		Cidr:        testCIDR,
	}, false)

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetwork(testNamespace, "my-network", ref, "ProviderConfig")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Error("Observe: want ResourceExists=true, got false")
	}
	if cr.Status.AtProvider.ID != ref {
		t.Errorf("AtProvider.ID = %q, want %q", cr.Status.AtProvider.ID, ref)
	}
}

func TestNamespacedObserveNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetwork(testNamespace, "my-network", "network/does-not-exist:10.0.0.0/24/default", "ProviderConfig")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false for 404, got true")
	}
}

func TestNamespacedObservePreCreateState(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetwork(testNamespace, "my-network", "", "ProviderConfig")
	meta.SetExternalName(cr, cr.GetName())

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false in pre-create state, got true")
	}
}

func TestNamespacedObserveServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetwork(testNamespace, "my-network", "network/test1:10.0.0.0/24/default", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestNamespacedObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetwork(testNamespace, "my-network", "network/test1:10.0.0.0/24/default", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestNamespacedObserveMinimalResponse verifies that Observe() does not
// panic and correctly leaves every optional AtProvider field nil/empty
// when the WAPI response carries only the server-assigned _ref and none
// of network_view, network, comment, extattrs, or members — the minimal
// shape a Network response can take.
func TestNamespacedObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Network{Ref: "network/minimal:10.0.0.0/24/default"}, false)

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetwork(testNamespace, "my-network", ref, "ProviderConfig")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error on minimal response: %v", err)
	}
	if !got.ResourceExists {
		t.Error("Observe: want ResourceExists=true for minimal response, got false")
	}

	ap := cr.Status.AtProvider
	if ap.ID != ref {
		t.Errorf("AtProvider.ID = %q, want %q", ap.ID, ref)
	}
	if ap.NetworkView != nil {
		t.Errorf("AtProvider.NetworkView = %v, want nil", ap.NetworkView)
	}
	if ap.Network != nil {
		t.Errorf("AtProvider.Network = %v, want nil", ap.Network)
	}
	if ap.Comment != nil {
		t.Errorf("AtProvider.Comment = %v, want nil", ap.Comment)
	}
	if ap.ExtAttrs != nil {
		t.Errorf("AtProvider.ExtAttrs = %v, want nil", ap.ExtAttrs)
	}
	if len(ap.Members) != 0 {
		t.Errorf("AtProvider.Members = %v, want empty", ap.Members)
	}
}

// ── namespaced: Create/Update/Delete ─────────────────────────────────────

func TestNamespacedCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetwork(testNamespace, "my-network", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

func TestNamespacedUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Network{
		NetviewName: testNamespace,
		Cidr:        testCIDR,
	}, false)

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetwork(testNamespace, "my-network", ref, "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("updated")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.networks[ref]
	m.mu.Unlock()
	if stored.Comment != "updated" {
		t.Errorf("Update: stored comment = %q, want %q", stored.Comment, "updated")
	}
}

// TestNamespacedUpdateRefStable mirrors TestClusterUpdateRefStable for the
// namespaced scope: the external-name (_ref) must not change after Update.
func TestNamespacedUpdateRefStable(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Network{
		NetviewName: testNamespace,
		Cidr:        testCIDR,
	}, false)

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetwork(testNamespace, "my-network", ref, "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("updated")

	before := meta.GetExternalName(cr)
	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	after := meta.GetExternalName(cr)
	if after != before || after != ref {
		t.Errorf("Update: external-name = %q, want unchanged original ref %q", after, ref)
	}
}

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Network{NetviewName: testNamespace, Cidr: testCIDR}, false)

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetwork(testNamespace, "my-network", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}

	m.mu.Lock()
	_, stillExists := m.networks[ref]
	m.mu.Unlock()
	if stillExists {
		t.Error("Delete: network still present after Delete")
	}
}

func TestNamespacedDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetwork(testNamespace, "my-network", "network/does-not-exist:10.0.0.0/24/default", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error for already-gone resource, got: %v", err)
	}
}

func TestNamespacedDeleteServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetwork(testNamespace, "my-network", "network/test1:10.0.0.0/24/default", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteNetwork) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteNetwork)
	}
}

// ── namespaced: Connect ────────────────────────────────────────────────

func TestNamespacedConnectWithProviderConfig(t *testing.T) {
	const (
		ns     = testNamespace
		secret = "infobloxnios-api-key"
	)

	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			credentialsSecret(ns, secret, "grid.example.com", "admin", "s3cr3t"),
			&namespacedpcv1alpha1.ProviderConfig{
				ObjectMeta: metav1.ObjectMeta{Name: testNamespace, Namespace: ns},
				Spec: namespacedpcv1alpha1.ProviderConfigSpec{
					Credentials: namespacedpcv1alpha1.ProviderCredentials{
						Source: xpv1.CredentialsSourceSecret,
						CommonCredentialSelectors: xpv1.CommonCredentialSelectors{
							SecretRef: &xpv1.SecretKeySelector{
								SecretReference: xpv1.SecretReference{Name: secret, Namespace: ns},
								Key:             "unused",
							},
						},
					},
				},
			},
		).Build()

	conn := &namespacedConnector{
		kube:  kube,
		usage: resource.NewProviderConfigUsageTracker(kube, &namespacedpcv1alpha1.ProviderConfigUsage{}),
	}

	cr := newNamespacedNetwork(ns, "my-network", "", "ProviderConfig")
	got, err := conn.Connect(context.Background(), cr)
	if err != nil {
		t.Fatalf("Connect: unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("Connect: expected non-nil ExternalClient, got nil")
	}
}

func TestNamespacedConnectWithClusterProviderConfig(t *testing.T) {
	const secret = "infobloxnios-api-key"
	ns := "crossplane-system"

	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			credentialsSecret(ns, secret, "grid.example.com", "admin", "s3cr3t"),
			&namespacedpcv1alpha1.ClusterProviderConfig{
				ObjectMeta: metav1.ObjectMeta{Name: testNamespace},
				Spec: namespacedpcv1alpha1.ProviderConfigSpec{
					Credentials: namespacedpcv1alpha1.ProviderCredentials{
						Source: xpv1.CredentialsSourceSecret,
						CommonCredentialSelectors: xpv1.CommonCredentialSelectors{
							SecretRef: &xpv1.SecretKeySelector{
								SecretReference: xpv1.SecretReference{Name: secret, Namespace: ns},
								Key:             "unused",
							},
						},
					},
				},
			},
		).Build()

	conn := &namespacedConnector{
		kube:  kube,
		usage: resource.NewProviderConfigUsageTracker(kube, &namespacedpcv1alpha1.ProviderConfigUsage{}),
	}

	cr := newNamespacedNetwork("app-ns", "my-network", "", "ClusterProviderConfig")
	got, err := conn.Connect(context.Background(), cr)
	if err != nil {
		t.Fatalf("Connect: unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("Connect: expected non-nil ExternalClient, got nil")
	}
}

func TestNamespacedConnectUnsupportedKind(t *testing.T) {
	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()

	conn := &namespacedConnector{
		kube:  kube,
		usage: resource.NewProviderConfigUsageTracker(kube, &namespacedpcv1alpha1.ProviderConfigUsage{}),
	}

	cr := newNamespacedNetwork(testNamespace, "my-network", "", "SomeOtherKind")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for unsupported provider config kind, got nil")
	}
}

func TestNamespacedDisconnectIsNoop(t *testing.T) {
	e := &namespacedExternal{}
	if err := e.Disconnect(context.Background()); err != nil {
		t.Errorf("Disconnect: unexpected error: %v", err)
	}
}

// ── shared helpers ─────────────────────────────────────────────────────

func TestExtAttrsRoundTrip(t *testing.T) {
	in := map[string]string{testEAKey: testEAVal, "team": "net"}
	ea := buildEA(in)
	out := extAttrsFromEA(ea)
	if !extAttrsEqual(in, out) {
		t.Errorf("ExtAttrs round-trip: got %v, want %v", out, in)
	}
}

func TestExtAttrsEqualTreatsNilAndEmptyAsEqual(t *testing.T) {
	if !extAttrsEqual(nil, map[string]string{}) {
		t.Error("extAttrsEqual: want nil and empty map to be equal")
	}
}

func TestIsNotFoundClassifiesTypedError(t *testing.T) {
	err := ibclient.NewNotFoundError("boom")
	if !isNotFound(err) {
		t.Error("isNotFound: want true for *ibclient.NotFoundError")
	}
}

func TestIsNotFoundClassifiesGenericStatusError(t *testing.T) {
	err := errWithStatus(404)
	if !isNotFound(err) {
		t.Error("isNotFound: want true for generic 404 status error")
	}
	if isNotFound(errWithStatus(500)) {
		t.Error("isNotFound: want false for a 500 status error")
	}
	if isNotFound(nil) {
		t.Error("isNotFound: want false for nil error")
	}
}

// errWithStatus synthesizes an error matching the SDK's generic
// "WAPI request error: <status>(...)" format for classifier tests.
type wapiStatusErr struct{ msg string }

func (e wapiStatusErr) Error() string { return e.msg }

func errWithStatus(code int) error {
	return wapiStatusErr{msg: "WAPI request error: " + itoa(code) + "('boom')"}
}

func TestLateInitializeBackfillsOptionalFields(t *testing.T) {
	var network *string
	var comment *string
	extAttrs := map[string]string{}

	nw := &ibclient.Network{
		Cidr:    "10.0.0.0/24",
		Comment: "server comment",
		Ea:      ibclient.EA{testEAKey: testEAVal},
	}

	changed := lateInitialize(&network, &comment, &extAttrs, nw)
	if !changed {
		t.Error("lateInitialize: want changed=true, got false")
	}
	if network == nil || *network != "10.0.0.0/24" {
		t.Errorf("lateInitialize: network = %v, want %q", network, "10.0.0.0/24")
	}
	if comment == nil || *comment != "server comment" {
		t.Errorf("lateInitialize: comment = %v, want %q", comment, "server comment")
	}
	if extAttrs[testEAKey] != testEAVal {
		t.Errorf("lateInitialize: extAttrs = %v, want env=prod", extAttrs)
	}
}

func TestLateInitializeDoesNotOverwriteSetFields(t *testing.T) {
	network := stringPtr("10.0.0.0/24")
	comment := stringPtr("user comment")
	extAttrs := map[string]string{testEAKey: "user-set"}

	nw := &ibclient.Network{
		Cidr:    "10.0.1.0/24",
		Comment: "server comment",
		Ea:      ibclient.EA{testEAKey: "server-set"},
	}

	changed := lateInitialize(&network, &comment, &extAttrs, nw)
	if changed {
		t.Error("lateInitialize: want changed=false when fields already set, got true")
	}
	if *network != "10.0.0.0/24" {
		t.Errorf("lateInitialize: network overwritten, got %q, want %q", *network, "10.0.0.0/24")
	}
	if *comment != "user comment" {
		t.Errorf("lateInitialize: comment overwritten, got %q, want %q", *comment, "user comment")
	}
	if extAttrs[testEAKey] != "user-set" {
		t.Errorf("lateInitialize: extAttrs overwritten, got %v", extAttrs)
	}
}

func TestIsUpToDate(t *testing.T) {
	cases := []struct {
		name     string
		comment  *string
		extAttrs map[string]string
		nw       *ibclient.Network
		want     bool
	}{
		{
			name:    "matching comment and no extattrs",
			comment: stringPtr("hello"),
			nw:      &ibclient.Network{Comment: "hello"},
			want:    true,
		},
		{
			name:    "comment drift",
			comment: stringPtr("hello"),
			nw:      &ibclient.Network{Comment: "goodbye"},
			want:    false,
		},
		{
			name:     "matching extattrs",
			comment:  stringPtr("hello"),
			extAttrs: map[string]string{testEAKey: testEAVal},
			nw:       &ibclient.Network{Comment: "hello", Ea: ibclient.EA{testEAKey: testEAVal}},
			want:     true,
		},
		{
			name:     "extattrs drift",
			comment:  stringPtr("hello"),
			extAttrs: map[string]string{testEAKey: testEAVal},
			nw:       &ibclient.Network{Comment: "hello", Ea: ibclient.EA{testEAKey: "staging"}},
			want:     false,
		},
		{
			name: "nil comment vs empty string comment",
			nw:   &ibclient.Network{Comment: ""},
			want: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isUpToDate(tc.comment, tc.extAttrs, tc.nw)
			if got != tc.want {
				t.Errorf("isUpToDate() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsIPv6CIDR(t *testing.T) {
	cases := []struct {
		cidr string
		want bool
	}{
		{testCIDR, false},
		{"192.168.1.0/24", false},
		{"2001:db8::/64", true},
		{"fd00::/8", true},
		{"not-a-cidr", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := isIPv6CIDR(tc.cidr); got != tc.want {
			t.Errorf("isIPv6CIDR(%q) = %v, want %v", tc.cidr, got, tc.want)
		}
	}
}

func TestCreateNetworkMissingCIDR(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	objMgr := newTestObjectManager(t, srv)
	if _, err := createNetwork(objMgr, stringPtr(testNamespace), nil, nil, nil); err == nil {
		t.Fatal("createNetwork: expected error for missing CIDR, got nil")
	}
}

func TestConvertMembers(t *testing.T) {
	members := []ibclient.NetworkMember{
		{DhcpMember: &ibclient.Dhcpmember{Name: "member1", Ipv4Addr: "10.0.0.5"}},
		{MsDhcpServer: &ibclient.Msdhcpserver{Ipv4Addr: "10.0.0.6"}},
	}
	got := convertMembers(members)
	if len(got) != 2 {
		t.Fatalf("convertMembers: got %d members, want 2", len(got))
	}
	if got[0].DhcpMemberName == nil || *got[0].DhcpMemberName != "member1" {
		t.Errorf("convertMembers[0].DhcpMemberName = %v, want member1", got[0].DhcpMemberName)
	}
	if got[0].DhcpMemberIPv4Addr == nil || *got[0].DhcpMemberIPv4Addr != "10.0.0.5" {
		t.Errorf("convertMembers[0].DhcpMemberIPv4Addr = %v, want 10.0.0.5", got[0].DhcpMemberIPv4Addr)
	}
	if got[1].MsDhcpServerIPv4Addr == nil || *got[1].MsDhcpServerIPv4Addr != "10.0.0.6" {
		t.Errorf("convertMembers[1].MsDhcpServerIPv4Addr = %v, want 10.0.0.6", got[1].MsDhcpServerIPv4Addr)
	}
}

// ── extractCredentials: ssl_verify ──────────────────────────────────────

func TestExtractCredentialsSslVerifyDefaultsTrue(t *testing.T) {
	scheme := newTestScheme(t)
	secret := credentialsSecret("crossplane-system", "infobloxnios-credentials", "grid.example.com", "admin", "s3cr3t")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	creds, err := extractCredentials(context.Background(), kube, xpv1.CredentialsSourceSecret, &xpv1.SecretKeySelector{
		SecretReference: xpv1.SecretReference{Name: "infobloxnios-credentials", Namespace: "crossplane-system"},
		Key:             "unused",
	}, "")
	if err != nil {
		t.Fatalf("extractCredentials: unexpected error: %v", err)
	}
	if !creds.SslVerify {
		t.Error("extractCredentials: expected SslVerify to default to true when ssl_verify key is absent")
	}
}

func TestExtractCredentialsSslVerifyFalse(t *testing.T) {
	scheme := newTestScheme(t)
	secret := credentialsSecret("crossplane-system", "infobloxnios-credentials", "grid.example.com", "admin", "s3cr3t")
	secret.Data["ssl_verify"] = []byte("false")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	creds, err := extractCredentials(context.Background(), kube, xpv1.CredentialsSourceSecret, &xpv1.SecretKeySelector{
		SecretReference: xpv1.SecretReference{Name: "infobloxnios-credentials", Namespace: "crossplane-system"},
		Key:             "unused",
	}, "")
	if err != nil {
		t.Fatalf("extractCredentials: unexpected error: %v", err)
	}
	if creds.SslVerify {
		t.Error("extractCredentials: expected SslVerify to be false when ssl_verify key is \"false\"")
	}
}

func TestExtractCredentialsSslVerifyUnrecognizedValueDefaultsTrue(t *testing.T) {
	scheme := newTestScheme(t)
	secret := credentialsSecret("crossplane-system", "infobloxnios-credentials", "grid.example.com", "admin", "s3cr3t")
	secret.Data["ssl_verify"] = []byte("nope")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	creds, err := extractCredentials(context.Background(), kube, xpv1.CredentialsSourceSecret, &xpv1.SecretKeySelector{
		SecretReference: xpv1.SecretReference{Name: "infobloxnios-credentials", Namespace: "crossplane-system"},
		Key:             "unused",
	}, "")
	if err != nil {
		t.Fatalf("extractCredentials: unexpected error: %v", err)
	}
	if !creds.SslVerify {
		t.Error("extractCredentials: expected SslVerify to default to true for any value other than exactly \"false\"")
	}
}

func TestNewObjectManagerWithSchemeUsesConfiguredSslVerify(t *testing.T) {
	// Regression guard: newObjectManagerWithScheme must not hardcode
	// SslVerify to "true" — it must honor creds.SslVerify. Both branches
	// must construct successfully (transport config validation happens
	// locally; no network round-trip occurs here).
	for name, sslVerify := range map[string]bool{"Enabled": true, "Disabled": false} {
		t.Run(name, func(t *testing.T) {
			creds := &nioCredentials{Host: "127.0.0.1", Username: "admin", Password: "s3cr3t", SslVerify: sslVerify}
			objMgr, err := newObjectManagerWithScheme(creds, "http", "80")
			if err != nil {
				t.Fatalf("newObjectManagerWithScheme: unexpected error: %v", err)
			}
			if objMgr == nil {
				t.Fatal("newObjectManagerWithScheme: expected non-nil object manager")
			}
		})
	}
}

func TestConvertMembersEmpty(t *testing.T) {
	if got := convertMembers(nil); got != nil {
		t.Errorf("convertMembers(nil) = %v, want nil", got)
	}
}
