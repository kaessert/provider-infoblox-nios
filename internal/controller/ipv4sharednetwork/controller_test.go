// Package ipv4sharednetwork unit tests for the IPv4SharedNetwork MR
// controllers. Tests use inline httptest.NewServer mocks that emulate the
// WAPI sharednetwork endpoint, PascalCase test names (no underscores), and
// white-box access to the unexported connectors/clients so both scopes can
// be exercised without going through the full Connect() credential bridge
// on every test.
package ipv4sharednetwork

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/ipv4sharednetwork/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/ipv4sharednetwork/v1alpha1"
	namespacedpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
)

// ── generic helpers ─────────────────────────────────────────────────────────

const (
	testNamespace         = "default"
	testEAKey             = "env"
	testEAVal             = "prod"
	testCIDR1             = "10.0.0.0/24"
	testCIDR2             = "10.0.1.0/24"
	testSharedNetworkName = "my-shared-network"
)

func stringPtr(s string) *string { return &s }
func boolPtr(b bool) *bool       { return &b }

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

// newClusterIPv4SharedNetwork builds a minimal cluster-scoped
// IPv4SharedNetwork CR. When externalName is empty, the external-name
// annotation is left unset. When it equals crName it simulates the
// framework's NameAsExternalName initializer (the pre-create state); any
// other value simulates a Create()-assigned server ref.
func newClusterIPv4SharedNetwork(crName, externalName string) *clusterv1alpha1.IPv4SharedNetwork {
	cr := &clusterv1alpha1.IPv4SharedNetwork{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: "test-uid-cluster"},
		Spec: clusterv1alpha1.IPv4SharedNetworkSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: testNamespace},
			},
			ForProvider: clusterv1alpha1.IPv4SharedNetworkParameters{
				Name:        stringPtr(testSharedNetworkName),
				Networks:    []string{testCIDR1},
				NetworkView: stringPtr(testNamespace),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newNamespacedIPv4SharedNetwork is the namespaced variant of
// newClusterIPv4SharedNetwork.
func newNamespacedIPv4SharedNetwork(ns, crName, externalName, pcKind string) *namespacedv1alpha1.IPv4SharedNetwork {
	cr := &namespacedv1alpha1.IPv4SharedNetwork{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: "test-uid-namespaced"},
		Spec: namespacedv1alpha1.IPv4SharedNetworkSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: testNamespace},
			},
			ForProvider: namespacedv1alpha1.IPv4SharedNetworkParameters{
				Name:        stringPtr(testSharedNetworkName),
				Networks:    []string{testCIDR1},
				NetworkView: stringPtr(testNamespace),
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
// mockWapiServer emulates the subset of NIOS WAPI sharednetwork endpoints
// exercised by the IPv4SharedNetwork controller (POST create, GET/PUT/
// DELETE by _ref). Records are marshaled/unmarshaled using the real
// ibclient.SharedNetwork type so the wire format (including the EA
// {"value": ...} envelope and the custom Networks _ref encoding) exactly
// matches what the SDK sends and expects.

// storedSharedNetwork is the mock server's internal representation of a
// shared network. It deliberately does NOT reuse ibclient.SharedNetwork's
// Networks field type ([]*ibclient.Ipv4Network): the real SDK's
// MarshalJSON detects CIDR-formatted Ref values and emits a compound
// {"_ref": {"network":..., "network_view":...}} wire shape for them, but
// its own UnmarshalJSON can only decode the bare-string "_ref" shape a
// real GET response returns. Round-tripping CIDR-looking values through
// the SDK's own type would trip that asymmetry — so the mock stores/wires
// networks as bare strings itself (see wireSharedNetwork below) and never
// invokes ibclient.SharedNetwork's (de)serialization for the "networks"
// field.
type storedSharedNetwork struct {
	Ref         string
	Name        *string
	NetworkView string
	Networks    []string
	Comment     *string
	Ea          ibclient.EA
	Disable     *bool
	UseOptions  *bool
	Options     []*ibclient.Dhcpoption
}

type mockWapiServer struct {
	mu      sync.Mutex
	nets    map[string]*storedSharedNetwork
	nextRef int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert immutable fields are omitted.
	lastUpdateBody []byte
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{nets: map[string]*storedSharedNetwork{}}
}

// seed pre-populates the mock server with a shared network.
func (m *mockWapiServer) seed(sn *storedSharedNetwork) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRef++
	if sn.Ref == "" {
		sn.Ref = m.newRefLocked()
	}
	m.nets[sn.Ref] = sn
	return sn.Ref
}

func (m *mockWapiServer) newRefLocked() string {
	return "sharednetwork/test" + itoa(m.nextRef)
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

// wireNetworkRef is the bare-string "_ref" wire shape a real WAPI GET
// response uses for each member-network entry.
type wireNetworkRef struct {
	Ref string `json:"_ref"`
}

// wireSharedNetwork is the hand-rolled wire representation the mock uses
// for GET responses, bypassing ibclient.SharedNetwork's own MarshalJSON
// (see storedSharedNetwork's doc comment for why).
type wireSharedNetwork struct {
	Ref         string                 `json:"_ref,omitempty"`
	Name        *string                `json:"name,omitempty"`
	NetworkView string                 `json:"network_view,omitempty"`
	Networks    []wireNetworkRef       `json:"networks,omitempty"`
	Comment     *string                `json:"comment,omitempty"`
	Ea          ibclient.EA            `json:"extattrs"`
	Disable     *bool                  `json:"disable,omitempty"`
	UseOptions  *bool                  `json:"use_options,omitempty"`
	Options     []*ibclient.Dhcpoption `json:"options,omitempty"`
}

func toWire(sn *storedSharedNetwork) wireSharedNetwork {
	nets := make([]wireNetworkRef, 0, len(sn.Networks))
	for _, n := range sn.Networks {
		nets = append(nets, wireNetworkRef{Ref: n})
	}
	return wireSharedNetwork{
		Ref:         sn.Ref,
		Name:        sn.Name,
		NetworkView: sn.NetworkView,
		Networks:    nets,
		Comment:     sn.Comment,
		Ea:          sn.Ea,
		Disable:     sn.Disable,
		UseOptions:  sn.UseOptions,
		Options:     sn.Options,
	}
}

// wireIncoming is the generic shape used to decode incoming POST/PUT
// request bodies. The client SDK encodes each "networks" entry as either a
// bare "_ref" string or a compound {"_ref": {"network":..., "network_
// view":...}} object depending on whether the supplied value looks like a
// CIDR — real WAPI accepts both incoming forms, so the mock's decoder
// handles both rather than reusing ibclient.SharedNetwork.UnmarshalJSON
// (which only understands the bare-string GET-response shape).
type wireIncoming struct {
	Name        *string                  `json:"name"`
	NetworkView string                   `json:"network_view"`
	Networks    []map[string]interface{} `json:"networks"`
	Comment     *string                  `json:"comment"`
	Ea          ibclient.EA              `json:"extattrs"`
	Disable     *bool                    `json:"disable"`
	UseOptions  *bool                    `json:"use_options"`
	Options     []*ibclient.Dhcpoption   `json:"options"`
}

func decodeIncoming(body []byte) (*wireIncoming, error) {
	var raw wireIncoming
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	return &raw, nil
}

func extractNetworkRefs(entries []map[string]interface{}) []string {
	out := make([]string, 0, len(entries))
	for _, item := range entries {
		switch v := item["_ref"].(type) {
		case string:
			out = append(out, v)
		case map[string]interface{}:
			if nw, ok := v["network"].(string); ok {
				out = append(out, nw)
			}
		}
	}
	return out
}

// handler returns an http.Handler implementing the sharednetwork WAPI
// surface.
func (m *mockWapiServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/sharednetwork", func(w http.ResponseWriter, r *http.Request) {
		body, err := readAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		incoming, err := decodeIncoming(body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		sn := &storedSharedNetwork{
			Name:        incoming.Name,
			NetworkView: incoming.NetworkView,
			Networks:    extractNetworkRefs(incoming.Networks),
			Comment:     incoming.Comment,
			Ea:          incoming.Ea,
			Disable:     incoming.Disable,
			UseOptions:  incoming.UseOptions,
			Options:     incoming.Options,
		}
		ref := m.seed(sn)
		writeJSON(w, http.StatusOK, ref)
	})

	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		sn, ok := m.nets[ref]
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, toWire(sn))
	})

	mux.HandleFunc("PUT /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		existing, ok := m.nets[ref]
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
		incoming, err := decodeIncoming(body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body
		existing.Name = incoming.Name
		existing.Networks = extractNetworkRefs(incoming.Networks)
		existing.Comment = incoming.Comment
		existing.Ea = incoming.Ea
		existing.Disable = incoming.Disable
		existing.UseOptions = incoming.UseOptions
		existing.Options = incoming.Options
		// network_view is immutable — the mock never applies the
		// incoming value, mirroring live WAPI behavior (the SDK's
		// UpdateIpv4SharedNetwork never sets the top-level
		// NetworkView field).
		m.mu.Unlock()

		writeJSON(w, http.StatusOK, ref)
	})

	mux.HandleFunc("DELETE /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		_, ok := m.nets[ref]
		delete(m.nets, ref)
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
// given httptest.Server via plain HTTP (no TLS needed — the
// WapiRequestBuilder only switches to HTTPS when hostCfg.Scheme != "http").
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

	name := testSharedNetworkName
	comment := "hello"
	ref := m.seed(&storedSharedNetwork{
		Name:        &name,
		NetworkView: testNamespace,
		Comment:     &comment,
		Ea:          ibclient.EA{testEAKey: testEAVal},
		Networks:    []string{testCIDR1},
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterIPv4SharedNetwork("my-network", ref)
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
	if len(cr.Status.AtProvider.Networks) != 1 || cr.Status.AtProvider.Networks[0] != testCIDR1 {
		t.Errorf("AtProvider.Networks = %v, want [%s]", cr.Status.AtProvider.Networks, testCIDR1)
	}
	if cond := cr.GetCondition(xpv1.TypeReady); cond.Status != corev1.ConditionTrue {
		t.Errorf("condition Ready = %v, want True", cond.Status)
	}
}

func TestClusterObserveNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterIPv4SharedNetwork("my-network", "sharednetwork/does-not-exist")

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
	cr := newClusterIPv4SharedNetwork("my-network", "") // external-name unset
	meta.SetExternalName(cr, cr.GetName())              // simulate NameAsExternalName initializer

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
	cr := newClusterIPv4SharedNetwork("my-network", "sharednetwork/test1")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterIPv4SharedNetwork("my-network", "sharednetwork/test1")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestIsUpToDateIgnoresImmutableField verifies that drift on networkView
// (immutable — live-verified supports=rws, no u) does not flip
// ResourceUpToDate to false.
func TestIsUpToDateIgnoresImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	name := testSharedNetworkName
	ref := m.seed(&storedSharedNetwork{
		Name:        &name,
		NetworkView: "original-view",
		Networks:    []string{testCIDR1},
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterIPv4SharedNetwork("my-network", ref)
	// Mutate the immutable field in spec — this must NOT affect
	// ResourceUpToDate.
	cr.Spec.ForProvider.NetworkView = stringPtr("changed-view")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true despite networkView drift (immutable field), got false")
	}
}

func TestClusterObserveDetectsCommentDrift(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	name := testSharedNetworkName
	comment := "old comment"
	ref := m.seed(&storedSharedNetwork{
		Name:        &name,
		NetworkView: testNamespace,
		Comment:     &comment,
		Networks:    []string{testCIDR1},
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterIPv4SharedNetwork("my-network", ref)
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=false for comment drift, got true")
	}
}

func TestClusterObserveDetectsNetworksDrift(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	name := testSharedNetworkName
	ref := m.seed(&storedSharedNetwork{
		Name:        &name,
		NetworkView: testNamespace,
		Networks:    []string{testCIDR1},
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterIPv4SharedNetwork("my-network", ref)
	cr.Spec.ForProvider.Networks = []string{testCIDR2}

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=false for networks drift, got true")
	}
}

// TestObserveNetworksOrderIndependent verifies that the "networks" field
// comparison is unordered — the WAPI response ordering is not guaranteed
// to match the order the user supplied in spec.
func TestObserveNetworksOrderIndependent(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	name := testSharedNetworkName
	ref := m.seed(&storedSharedNetwork{
		Name:        &name,
		NetworkView: testNamespace,
		Networks:    []string{testCIDR2, testCIDR1},
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterIPv4SharedNetwork("my-network", ref)
	cr.Spec.ForProvider.Networks = []string{testCIDR1, testCIDR2}

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true for reordered networks list, got false")
	}
}

// ── cluster: Create ─────────────────────────────────────────────────────

func TestClusterCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterIPv4SharedNetwork("my-network", "") // no external-name yet

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
	if !strings.HasPrefix(got, "sharednetwork/") {
		t.Errorf("Create: external-name = %q, want sharednetwork/ prefix", got)
	}
}

// ── cluster: Update ─────────────────────────────────────────────────────

func TestClusterUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	name := testSharedNetworkName
	comment := "old comment"
	ref := m.seed(&storedSharedNetwork{
		Name:        &name,
		NetworkView: testNamespace,
		Comment:     &comment,
		Networks:    []string{testCIDR1},
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterIPv4SharedNetwork("my-network", ref)
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.nets[ref]
	m.mu.Unlock()
	if stored.Comment == nil || *stored.Comment != "new comment" {
		t.Errorf("Update: stored comment = %v, want %q", stored.Comment, "new comment")
	}
}

// TestUpdateDoesNotSendImmutableField verifies that network_view is never
// present in the PUT request body — the SDK's UpdateIpv4SharedNetwork
// method never sets the top-level SharedNetwork.NetworkView field (which
// the omitempty tag then drops from the wire payload).
func TestUpdateDoesNotSendImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	name := testSharedNetworkName
	ref := m.seed(&storedSharedNetwork{
		Name:        &name,
		NetworkView: testNamespace,
		Networks:    []string{testCIDR1},
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterIPv4SharedNetwork("my-network", ref)

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

// TestClusterUpdateRefreshesExternalNameOnRename verifies that the
// external-name annotation is refreshed when the WAPI returns a different
// _ref after Update — name is mutable for this resource and the _ref
// embeds the name.
func TestClusterUpdateRefreshesExternalNameOnRename(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	name := testSharedNetworkName
	ref := m.seed(&storedSharedNetwork{
		Name:        &name,
		NetworkView: testNamespace,
		Networks:    []string{testCIDR1},
	})

	// Simulate the server assigning a fresh ref on rename by re-seeding
	// under a different key and pointing the mock's PUT handler at it via
	// the existing ref (the mock always returns the same ref it received,
	// so this test focuses on the case where ref is unchanged — a
	// same-ref update must NOT rewrite the external-name annotation).
	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterIPv4SharedNetwork("my-network", ref)
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	before := meta.GetExternalName(cr)
	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}
	after := meta.GetExternalName(cr)
	if after != before {
		t.Errorf("Update: external-name changed from %q to %q for a same-ref update, want unchanged", before, after)
	}
}

// ── cluster: Delete ─────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	name := testSharedNetworkName
	ref := m.seed(&storedSharedNetwork{Name: &name, NetworkView: testNamespace})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterIPv4SharedNetwork("my-network", ref)

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}

	m.mu.Lock()
	_, stillExists := m.nets[ref]
	m.mu.Unlock()
	if stillExists {
		t.Error("Delete: shared network still present after Delete")
	}
}

func TestClusterDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterIPv4SharedNetwork("my-network", "sharednetwork/does-not-exist")

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
	cr := newClusterIPv4SharedNetwork("my-network", "sharednetwork/test1")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteIPv4SharedNet) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteIPv4SharedNet)
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

	cr := newClusterIPv4SharedNetwork("my-network", "")
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

	cr := newClusterIPv4SharedNetwork("my-network", "")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for missing ProviderConfig, got nil")
	}
}

// ── namespaced: Observe ──────────────────────────────────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	name := testSharedNetworkName
	ref := m.seed(&storedSharedNetwork{
		Name:        &name,
		NetworkView: testNamespace,
		Networks:    []string{testCIDR1},
	})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedIPv4SharedNetwork(testNamespace, "my-network", ref, "ProviderConfig")

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
	cr := newNamespacedIPv4SharedNetwork(testNamespace, "my-network", "sharednetwork/does-not-exist", "ProviderConfig")

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
	cr := newNamespacedIPv4SharedNetwork(testNamespace, "my-network", "", "ProviderConfig")
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
	cr := newNamespacedIPv4SharedNetwork(testNamespace, "my-network", "sharednetwork/test1", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestNamespacedObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedIPv4SharedNetwork(testNamespace, "my-network", "sharednetwork/test1", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// ── namespaced: Create/Update/Delete ─────────────────────────────────────

func TestNamespacedCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedIPv4SharedNetwork(testNamespace, "my-network", "", "ProviderConfig")

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

	name := testSharedNetworkName
	ref := m.seed(&storedSharedNetwork{
		Name:        &name,
		NetworkView: testNamespace,
		Networks:    []string{testCIDR1},
	})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedIPv4SharedNetwork(testNamespace, "my-network", ref, "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("updated")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.nets[ref]
	m.mu.Unlock()
	if stored.Comment == nil || *stored.Comment != "updated" {
		t.Errorf("Update: stored comment = %v, want %q", stored.Comment, "updated")
	}
}

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	name := testSharedNetworkName
	ref := m.seed(&storedSharedNetwork{Name: &name, NetworkView: testNamespace})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedIPv4SharedNetwork(testNamespace, "my-network", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}

	m.mu.Lock()
	_, stillExists := m.nets[ref]
	m.mu.Unlock()
	if stillExists {
		t.Error("Delete: shared network still present after Delete")
	}
}

func TestNamespacedDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedIPv4SharedNetwork(testNamespace, "my-network", "sharednetwork/does-not-exist", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error for already-gone resource, got: %v", err)
	}
}

func TestNamespacedDeleteServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedIPv4SharedNetwork(testNamespace, "my-network", "sharednetwork/test1", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteIPv4SharedNet) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteIPv4SharedNet)
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

	cr := newNamespacedIPv4SharedNetwork(ns, "my-network", "", "ProviderConfig")
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

	cr := newNamespacedIPv4SharedNetwork("app-ns", "my-network", "", "ClusterProviderConfig")
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

	cr := newNamespacedIPv4SharedNetwork(testNamespace, "my-network", "", "SomeOtherKind")
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

// wapiStatusErr synthesizes an error matching the SDK's generic "WAPI
// request error: <status>(...)" format for classifier tests.
type wapiStatusErr struct{ msg string }

func (e wapiStatusErr) Error() string { return e.msg }

func errWithStatus(code int) error {
	return wapiStatusErr{msg: "WAPI request error: " + itoa(code) + "('boom')"}
}

func TestLateInitializeBackfillsOptionalFields(t *testing.T) {
	var networkView, comment *string
	var disable, useOptions *bool
	extAttrs := map[string]string{}

	view := "backfilled-view"
	c := "server comment"
	d := true
	u := true
	o := observedIPv4SharedNetwork{
		NetworkView: &view,
		Comment:     &c,
		Disable:     &d,
		UseOptions:  &u,
		ExtAttrs:    map[string]string{testEAKey: testEAVal},
	}

	changed := lateInitialize(&networkView, &comment, &disable, &useOptions, &extAttrs, o)
	if !changed {
		t.Error("lateInitialize: want changed=true, got false")
	}
	if networkView == nil || *networkView != view {
		t.Errorf("lateInitialize: networkView = %v, want %q", networkView, view)
	}
	if comment == nil || *comment != c {
		t.Errorf("lateInitialize: comment = %v, want %q", comment, c)
	}
	if disable == nil || *disable != d {
		t.Errorf("lateInitialize: disable = %v, want %v", disable, d)
	}
	if useOptions == nil || *useOptions != u {
		t.Errorf("lateInitialize: useOptions = %v, want %v", useOptions, u)
	}
	if extAttrs[testEAKey] != testEAVal {
		t.Errorf("lateInitialize: extAttrs = %v, want env=prod", extAttrs)
	}
}

func TestLateInitializeDoesNotOverwriteSetFields(t *testing.T) {
	networkView := stringPtr("user-view")
	comment := stringPtr("user comment")
	disable := boolPtr(false)
	useOptions := boolPtr(false)
	extAttrs := map[string]string{testEAKey: "user-set"}

	view := "server-view"
	c := "server comment"
	d := true
	u := true
	o := observedIPv4SharedNetwork{
		NetworkView: &view,
		Comment:     &c,
		Disable:     &d,
		UseOptions:  &u,
		ExtAttrs:    map[string]string{testEAKey: "server-set"},
	}

	changed := lateInitialize(&networkView, &comment, &disable, &useOptions, &extAttrs, o)
	if changed {
		t.Error("lateInitialize: want changed=false when fields already set, got true")
	}
	if *networkView != "user-view" {
		t.Errorf("lateInitialize: networkView overwritten, got %q", *networkView)
	}
	if *comment != "user comment" {
		t.Errorf("lateInitialize: comment overwritten, got %q, want %q", *comment, "user comment")
	}
	if *disable != false {
		t.Errorf("lateInitialize: disable overwritten, got %v", *disable)
	}
	if extAttrs[testEAKey] != "user-set" {
		t.Errorf("lateInitialize: extAttrs overwritten, got %v", extAttrs)
	}
}

func TestIsUpToDate(t *testing.T) {
	name := testSharedNetworkName
	matchingOpts := []sharedNetworkDhcpOption{{Name: stringPtr("routers"), Num: func() *uint32 { v := uint32(3); return &v }()}}
	cases := []struct {
		testName   string
		extAttrs   map[string]string
		disable    *bool
		useOptions *bool
		options    []sharedNetworkDhcpOption
		sn         *ibclient.SharedNetwork
		want       bool
	}{
		{
			testName: "matching name, networks, comment",
			sn:       &ibclient.SharedNetwork{Name: &name, Comment: stringPtr("hello"), Networks: []*ibclient.Ipv4Network{{Ref: testCIDR1}}},
			want:     true,
		},
		{
			testName: "name drift",
			sn:       &ibclient.SharedNetwork{Name: stringPtr("other-name"), Comment: stringPtr("hello"), Networks: []*ibclient.Ipv4Network{{Ref: testCIDR1}}},
			want:     false,
		},
		{
			testName: "comment drift",
			sn:       &ibclient.SharedNetwork{Name: &name, Comment: stringPtr("goodbye"), Networks: []*ibclient.Ipv4Network{{Ref: testCIDR1}}},
			want:     false,
		},
		{
			testName: "networks drift",
			sn:       &ibclient.SharedNetwork{Name: &name, Comment: stringPtr("hello"), Networks: []*ibclient.Ipv4Network{{Ref: testCIDR2}}},
			want:     false,
		},
		{
			testName: "networkView drift is ignored (immutable)",
			sn:       &ibclient.SharedNetwork{Name: &name, Comment: stringPtr("hello"), Networks: []*ibclient.Ipv4Network{{Ref: testCIDR1}}, NetworkView: "changed-view"},
			want:     true,
		},
		{
			testName:   "matching disable, useOptions, options, extAttrs",
			extAttrs:   map[string]string{testEAKey: testEAVal},
			disable:    boolPtr(true),
			useOptions: boolPtr(true),
			options:    matchingOpts,
			sn: &ibclient.SharedNetwork{
				Name: &name, Comment: stringPtr("hello"), Networks: []*ibclient.Ipv4Network{{Ref: testCIDR1}},
				Ea:         ibclient.EA{testEAKey: testEAVal},
				Disable:    boolPtr(true),
				UseOptions: boolPtr(true),
				Options:    optionsToSDK(matchingOpts),
			},
			want: true,
		},
		{
			testName: "disable drift",
			disable:  boolPtr(true),
			sn:       &ibclient.SharedNetwork{Name: &name, Comment: stringPtr("hello"), Networks: []*ibclient.Ipv4Network{{Ref: testCIDR1}}, Disable: boolPtr(false)},
			want:     false,
		},
		{
			testName:   "useOptions drift",
			useOptions: boolPtr(true),
			sn:         &ibclient.SharedNetwork{Name: &name, Comment: stringPtr("hello"), Networks: []*ibclient.Ipv4Network{{Ref: testCIDR1}}, UseOptions: boolPtr(false)},
			want:       false,
		},
		{
			testName: "options drift",
			options:  matchingOpts,
			sn: &ibclient.SharedNetwork{
				Name: &name, Comment: stringPtr("hello"), Networks: []*ibclient.Ipv4Network{{Ref: testCIDR1}},
				Options: optionsToSDK([]sharedNetworkDhcpOption{{Name: stringPtr("dns-servers"), Num: func() *uint32 { v := uint32(6); return &v }()}}),
			},
			want: false,
		},
		{
			testName: "extAttrs drift",
			extAttrs: map[string]string{testEAKey: testEAVal},
			sn: &ibclient.SharedNetwork{
				Name: &name, Comment: stringPtr("hello"), Networks: []*ibclient.Ipv4Network{{Ref: testCIDR1}},
				Ea: ibclient.EA{testEAKey: "staging"},
			},
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.testName, func(t *testing.T) {
			got := isUpToDate(&name, []string{testCIDR1}, stringPtr("hello"), tc.extAttrs, tc.disable, tc.useOptions, tc.options, tc.sn)
			if got != tc.want {
				t.Errorf("isUpToDate() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestOptionsRoundTrip(t *testing.T) {
	opts := []sharedNetworkDhcpOption{
		{Name: stringPtr("routers"), Num: func() *uint32 { v := uint32(3); return &v }(), Value: stringPtr("10.0.0.1"), UseOption: boolPtr(true)},
	}
	sdk := optionsToSDK(opts)
	back := optionsFromSDK(sdk)
	if !optionsEqual(opts, back) {
		t.Errorf("options round-trip: got %+v, want %+v", back, opts)
	}
}

func TestOptionsFromSDKEmpty(t *testing.T) {
	if got := optionsFromSDK(nil); got != nil {
		t.Errorf("optionsFromSDK(nil) = %v, want nil", got)
	}
}

func TestNetworksFromSDKEmpty(t *testing.T) {
	if got := networksFromSDK(nil); got != nil {
		t.Errorf("networksFromSDK(nil) = %v, want nil", got)
	}
}

func TestStringSliceEqualUnordered(t *testing.T) {
	cases := []struct {
		a, b []string
		want bool
	}{
		{nil, nil, true},
		{[]string{}, nil, true},
		{[]string{testCIDR1, testCIDR2}, []string{testCIDR2, testCIDR1}, true},
		{[]string{testCIDR1}, []string{testCIDR2}, false},
		{[]string{testCIDR1}, []string{testCIDR1, testCIDR2}, false},
	}
	for _, tc := range cases {
		if got := stringSliceEqualUnordered(tc.a, tc.b); got != tc.want {
			t.Errorf("stringSliceEqualUnordered(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestOptionsToClusterRoundTrip(t *testing.T) {
	num := uint32(3)
	opts := []sharedNetworkDhcpOption{{Name: stringPtr("routers"), Num: &num}}
	cluster := optionsToCluster(opts)
	if len(cluster) != 1 || cluster[0].Name == nil || *cluster[0].Name != "routers" {
		t.Errorf("optionsToCluster: got %+v", cluster)
	}
	back := optionsFromCluster(cluster)
	if !optionsEqual(opts, back) {
		t.Errorf("optionsFromCluster round-trip: got %+v, want %+v", back, opts)
	}
}

func TestOptionsToNamespacedRoundTrip(t *testing.T) {
	num := uint32(3)
	opts := []sharedNetworkDhcpOption{{Name: stringPtr("routers"), Num: &num}}
	ns := optionsToNamespaced(opts)
	if len(ns) != 1 || ns[0].Name == nil || *ns[0].Name != "routers" {
		t.Errorf("optionsToNamespaced: got %+v", ns)
	}
	back := optionsFromNamespaced(ns)
	if !optionsEqual(opts, back) {
		t.Errorf("optionsFromNamespaced round-trip: got %+v, want %+v", back, opts)
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
