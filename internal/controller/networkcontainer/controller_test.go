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

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	xpv2 "github.com/crossplane/crossplane-runtime/v2/apis/common/v2"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/networkcontainer/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/networkcontainer/v1alpha1"
	namespacedpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
)

// ── generic helpers ─────────────────────────────────────────────────────────

// Shared literal test fixtures, declared as named constants (rather than
// repeated inline string literals) to stay under the linter's
// minimum-occurrence threshold for repeated literals across this file.
const (
	testDefaultName      = "default" // provider config name / network view name / namespace / object name fixture
	testCIDR             = "10.0.0.0/16"
	testExtAttrKey       = "env"
	testExtAttrValue     = "prod"
	testUnusedKey        = "unused"
	testClusterNamespace = "crossplane-system"
)

func stringPtr(s string) *string { return &s }

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

// newClusterNetworkContainer builds a minimal cluster-scoped
// NetworkContainer CR. When externalName is empty, the external-name
// annotation is left unset. When it equals crName it simulates the
// framework's NameAsExternalName initializer (the pre-create state); any
// other value simulates a Create()-assigned server ref.
func newClusterNetworkContainer(crName, externalName string) *clusterv1alpha1.NetworkContainer {
	cr := &clusterv1alpha1.NetworkContainer{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: "test-uid-cluster"},
		Spec: clusterv1alpha1.NetworkContainerSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: testDefaultName},
			},
			ForProvider: clusterv1alpha1.NetworkContainerParameters{
				NetworkView: stringPtr(testDefaultName),
				Network:     stringPtr(testCIDR),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newNamespacedNetworkContainer is the namespaced variant of
// newClusterNetworkContainer.
func newNamespacedNetworkContainer(ns, crName, externalName, pcKind string) *namespacedv1alpha1.NetworkContainer {
	cr := &namespacedv1alpha1.NetworkContainer{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: "test-uid-namespaced"},
		Spec: namespacedv1alpha1.NetworkContainerSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: testDefaultName},
			},
			ForProvider: namespacedv1alpha1.NetworkContainerParameters{
				NetworkView: stringPtr(testDefaultName),
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
// mockWapiServer emulates the subset of NIOS WAPI networkcontainer /
// ipv6networkcontainer endpoints exercised by the NetworkContainer
// controller (POST create — routed to one of the two object-type paths,
// GET/PUT/DELETE by _ref). Records are marshaled/unmarshaled using the
// real ibclient.NetworkContainer type so the wire format (including the
// EA {"value": ...} envelope) exactly matches what the SDK sends and
// expects.

type mockWapiServer struct {
	mu         sync.Mutex
	containers map[string]*ibclient.NetworkContainer
	nextRef    int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert immutable fields are omitted.
	lastUpdateBody []byte
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{containers: map[string]*ibclient.NetworkContainer{}}
}

// seed inserts nc directly (bypassing HTTP) and returns its _ref,
// assigning one if not already set. The object-type prefix (networkcontainer
// vs ipv6networkcontainer) is derived from whether Cidr looks like an IPv6
// network, mirroring isIPv6CIDR's colon heuristic.
func (m *mockWapiServer) seed(nc *ibclient.NetworkContainer) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRef++
	if nc.Ref == "" {
		nc.Ref = m.newRefLocked(nc)
	}
	m.containers[nc.Ref] = nc
	return nc.Ref
}

func (m *mockWapiServer) newRefLocked(nc *ibclient.NetworkContainer) string {
	prefix := "networkcontainer"
	if strings.Contains(nc.Cidr, ":") {
		prefix = "ipv6networkcontainer"
	}
	return prefix + "/test" + itoa(m.nextRef) + ":" + nc.Cidr + "/" + nc.NetviewName
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

// filterReturnFields emulates WAPI's _return_fields behavior: when the
// query parameter is present, the response includes only _ref plus the
// explicitly requested field names. When absent, the full object is
// returned unfiltered. This matters for
// TestClusterUpdateDoesNotSendImmutableField — UpdateNetworkContainer's
// internal merge GET requests only extattrs/comment (never
// network/network_view), so a mock that ignored _return_fields would let
// the immutable identity fields leak back into the PUT body via the
// merge, masking a real bug.
func filterReturnFields(nc *ibclient.NetworkContainer, returnFields string) interface{} {
	if returnFields == "" {
		return nc
	}
	raw, err := json.Marshal(nc)
	if err != nil {
		return nc
	}
	var full map[string]json.RawMessage
	if err := json.Unmarshal(raw, &full); err != nil {
		return nc
	}
	keep := map[string]bool{"_ref": true}
	for _, f := range strings.Split(returnFields, ",") {
		keep[f] = true
	}
	filtered := map[string]json.RawMessage{}
	for k, v := range full {
		if keep[k] {
			filtered[k] = v
		}
	}
	return filtered
}

// handler returns an http.Handler implementing the networkcontainer /
// ipv6networkcontainer WAPI surface.
func (m *mockWapiServer) handler() http.Handler {
	mux := http.NewServeMux()

	createHandler := func(w http.ResponseWriter, r *http.Request) {
		var nc ibclient.NetworkContainer
		if err := json.NewDecoder(r.Body).Decode(&nc); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ref := m.seed(&nc)
		writeJSON(w, http.StatusOK, ref)
	}
	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/networkcontainer", createHandler)
	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/ipv6networkcontainer", createHandler)

	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		nc, ok := m.containers[ref]
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, filterReturnFields(nc, r.URL.Query().Get("_return_fields")))
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

		body, err := readAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var incoming ibclient.NetworkContainer
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body
		// Only the mutable fields (comment, extattrs) are ever applied —
		// networkView/network stay untouched on the stored record,
		// mirroring WAPI's rejection of identity-field changes.
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

	ref := m.seed(&ibclient.NetworkContainer{
		NetviewName: testDefaultName,
		Cidr:        testCIDR,
		Comment:     "hello",
		Ea:          ibclient.EA{testExtAttrKey: testExtAttrValue},
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", ref)
	cr.Spec.ForProvider.Comment = stringPtr("hello")
	cr.Spec.ForProvider.ExtAttrs = map[string]string{testExtAttrKey: testExtAttrValue}

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
	if cr.Status.AtProvider.NetworkView == nil || *cr.Status.AtProvider.NetworkView != testDefaultName {
		t.Errorf("AtProvider.NetworkView = %v, want %q", cr.Status.AtProvider.NetworkView, testDefaultName)
	}
	if cr.Status.AtProvider.Network == nil || *cr.Status.AtProvider.Network != testCIDR {
		t.Errorf("AtProvider.Network = %v, want %q", cr.Status.AtProvider.Network, testCIDR)
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
	cr := newClusterNetworkContainer("my-container", "networkcontainer/does-not-exist:10.0.0.0/16/default")

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
	cr := newClusterNetworkContainer("my-container", "") // external-name unset
	meta.SetExternalName(cr, cr.GetName())               // simulate NameAsExternalName initializer

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
	cr := newClusterNetworkContainer("my-container", "networkcontainer/test1:10.0.0.0/16/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", "networkcontainer/test1:10.0.0.0/16/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestClusterObserveMinimalResponse pins nil-safety in Observe: a WAPI
// response carrying only the object's _ref (the resource identifier) and
// every other field at its Go zero value (empty strings, a nil Ea map)
// must not panic and must produce a valid observation with nil-safe
// AtProvider fields.
func TestClusterObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkContainer{})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", ref)

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
}

func TestClusterObserveIsUpToDateIgnoresImmutableFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkContainer{
		NetviewName: testDefaultName,
		Cidr:        testCIDR,
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", ref)
	// Mutate the immutable networkView/network fields in spec — this must
	// NOT affect ResourceUpToDate, since they are excluded from
	// isUpToDate (WAPI has no UpdateNetworkContainer parameter for them).
	cr.Spec.ForProvider.NetworkView = stringPtr("changed-view")
	cr.Spec.ForProvider.Network = stringPtr("192.168.0.0/16")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true despite networkView/network drift (immutable fields), got false")
	}
}

// ── cluster: Create ─────────────────────────────────────────────────────

func TestClusterCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", "") // no external-name yet

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
	if !strings.HasPrefix(got, "networkcontainer/") {
		t.Errorf("Create: external-name = %q, want networkcontainer/ prefix for IPv4 CIDR", got)
	}
}

// TestClusterCreateSelectsIPv6ObjectType verifies that an IPv6 CIDR routes
// the create request to the ipv6networkcontainer object type, not
// networkcontainer.
func TestClusterCreateSelectsIPv6ObjectType(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", "")
	cr.Spec.ForProvider.Network = stringPtr("2001:db8::/32")

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if !strings.HasPrefix(got, "ipv6networkcontainer/") {
		t.Errorf("Create: external-name = %q, want ipv6networkcontainer/ prefix for IPv6 CIDR", got)
	}
}

// TestClusterCreateError verifies that a WAPI 5xx response during Create
// is propagated (wrapped, not swallowed) and the external-name is left
// unset — a failed Create must not falsely mark the resource as
// provisioned.
func TestClusterCreateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", "")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errCreateNetworkContainer) {
		t.Errorf("Create: error = %q, want it to contain %q (wrapped, not swallowed)", got, errCreateNetworkContainer)
	}
	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name = %q, want empty after failed create", got)
	}
}

// ── cluster: Update ─────────────────────────────────────────────────────

func TestClusterUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkContainer{
		NetviewName: testDefaultName,
		Cidr:        testCIDR,
		Comment:     "old comment",
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", ref)
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.containers[ref]
	m.mu.Unlock()
	if stored.Comment != "new comment" {
		t.Errorf("Update: stored comment = %q, want %q", stored.Comment, "new comment")
	}
}

func TestUpdateDoesNotSendImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkContainer{
		NetviewName: testDefaultName,
		Cidr:        testCIDR,
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", ref)

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
	for _, immutable := range []string{"network", "network_view"} {
		if _, present := raw[immutable]; present {
			t.Errorf("Update: request body contains immutable field %q: %v", immutable, raw[immutable])
		}
	}
}

// TestClusterUpdateError verifies that a WAPI 5xx response during Update
// is propagated (wrapped, not swallowed) rather than being silently
// treated as a successful reconcile.
func TestClusterUpdateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", "networkcontainer/test1:10.0.0.0/16/default")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errUpdateNetworkContainer) {
		t.Errorf("Update: error = %q, want it to contain %q (wrapped, not swallowed)", got, errUpdateNetworkContainer)
	}
}

// ── cluster: Delete ─────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkContainer{NetviewName: testDefaultName, Cidr: testCIDR})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", ref)

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}

	m.mu.Lock()
	_, stillExists := m.containers[ref]
	m.mu.Unlock()
	if stillExists {
		t.Error("Delete: record still present after Delete")
	}
}

func TestClusterDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", "networkcontainer/does-not-exist:10.0.0.0/16/default")

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
	cr := newClusterNetworkContainer("my-container", "networkcontainer/test1:10.0.0.0/16/default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteNetworkContainer) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteNetworkContainer)
	}
}

func TestClusterDeleteForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", "networkcontainer/test1:10.0.0.0/16/default")

	if _, err := e.Delete(context.Background(), cr); err == nil {
		t.Fatal("Delete: expected error for 403, got nil")
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
		ns     = testClusterNamespace
		secret = "infobloxnios-api-key"
	)

	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			credentialsSecret(ns, secret, "grid.example.com", "admin", "s3cr3t"),
			&clusterpcv1alpha1.ProviderConfig{
				ObjectMeta: metav1.ObjectMeta{Name: testDefaultName},
				Spec: clusterpcv1alpha1.ProviderConfigSpec{
					Credentials: clusterpcv1alpha1.ProviderCredentials{
						Source: xpv1.CredentialsSourceSecret,
						CommonCredentialSelectors: xpv1.CommonCredentialSelectors{
							SecretRef: &xpv1.SecretKeySelector{
								SecretReference: xpv1.SecretReference{Name: secret, Namespace: ns},
								Key:             testUnusedKey,
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

	cr := newClusterNetworkContainer("my-container", "")
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

	cr := newClusterNetworkContainer("my-container", "")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for missing ProviderConfig, got nil")
	}
}

// ── namespaced: Observe ──────────────────────────────────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkContainer{
		NetviewName: testDefaultName,
		Cidr:        testCIDR,
	})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", ref, "ProviderConfig")

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
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", "networkcontainer/does-not-exist:10.0.0.0/16/default", "ProviderConfig")

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
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", "", "ProviderConfig")
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
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", "networkcontainer/test1:10.0.0.0/16/default", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

// ── namespaced: Create/Update/Delete ─────────────────────────────────────

func TestNamespacedCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

// TestNamespacedCreateError verifies that a WAPI 5xx response during
// Create is propagated (wrapped, not swallowed) and the external-name is
// left unset.
func TestNamespacedCreateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", "", "ProviderConfig")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errCreateNetworkContainer) {
		t.Errorf("Create: error = %q, want it to contain %q (wrapped, not swallowed)", got, errCreateNetworkContainer)
	}
	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name = %q, want empty after failed create", got)
	}
}

func TestNamespacedUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkContainer{
		NetviewName: testDefaultName,
		Cidr:        testCIDR,
		Comment:     "old comment",
	})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", ref, "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.containers[ref]
	m.mu.Unlock()
	if stored.Comment != "new comment" {
		t.Errorf("Update: stored comment = %q, want %q", stored.Comment, "new comment")
	}
}

// TestNamespacedUpdateError verifies that a WAPI 5xx response during
// Update is propagated (wrapped, not swallowed).
func TestNamespacedUpdateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", "networkcontainer/test1:10.0.0.0/16/default", "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errUpdateNetworkContainer) {
		t.Errorf("Update: error = %q, want it to contain %q (wrapped, not swallowed)", got, errUpdateNetworkContainer)
	}
}

// TestNamespacedUpdateDoesNotSendImmutableFields mirrors the cluster-scope
// assertion: network and network_view must never appear in the
// namespaced-scope Update request body either.
func TestNamespacedUpdateDoesNotSendImmutableFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkContainer{
		NetviewName: testDefaultName,
		Cidr:        testCIDR,
	})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", ref, "ProviderConfig")

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
	for _, immutable := range []string{"network", "network_view"} {
		if _, present := raw[immutable]; present {
			t.Errorf("Update: request body contains immutable field %q: %v", immutable, raw[immutable])
		}
	}
}

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkContainer{NetviewName: testDefaultName, Cidr: testCIDR})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}

	m.mu.Lock()
	_, stillExists := m.containers[ref]
	m.mu.Unlock()
	if stillExists {
		t.Error("Delete: record still present after Delete")
	}
}

func TestNamespacedDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", "networkcontainer/does-not-exist:10.0.0.0/16/default", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error for already-gone resource, got: %v", err)
	}
}

// TestNamespacedDeleteServerError verifies that a 5xx response from the
// WAPI delete endpoint is propagated (wrapped, not swallowed) for the
// namespaced scope too.
func TestNamespacedDeleteServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", "networkcontainer/test1:10.0.0.0/16/default", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteNetworkContainer) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteNetworkContainer)
	}
}

// ── namespaced: Disconnect ──────────────────────────────────────────────

func TestNamespacedDisconnectIsNoop(t *testing.T) {
	e := &namespacedExternal{}
	if err := e.Disconnect(context.Background()); err != nil {
		t.Errorf("Disconnect: unexpected error: %v", err)
	}
}

// ── namespaced: Connect ──────────────────────────────────────────────────

func TestNamespacedConnectWithProviderConfig(t *testing.T) {
	const (
		ns     = testDefaultName
		secret = "infobloxnios-api-key"
	)

	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			credentialsSecret(ns, secret, "grid.example.com", "admin", "s3cr3t"),
			&namespacedpcv1alpha1.ProviderConfig{
				ObjectMeta: metav1.ObjectMeta{Name: testDefaultName, Namespace: ns},
				Spec: namespacedpcv1alpha1.ProviderConfigSpec{
					Credentials: namespacedpcv1alpha1.ProviderCredentials{
						Source: xpv1.CredentialsSourceSecret,
						CommonCredentialSelectors: xpv1.CommonCredentialSelectors{
							SecretRef: &xpv1.SecretKeySelector{
								SecretReference: xpv1.SecretReference{Name: secret, Namespace: ns},
								Key:             testUnusedKey,
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

	cr := newNamespacedNetworkContainer(ns, "my-container", "", "ProviderConfig")
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
	ns := testClusterNamespace

	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			credentialsSecret(ns, secret, "grid.example.com", "admin", "s3cr3t"),
			&namespacedpcv1alpha1.ClusterProviderConfig{
				ObjectMeta: metav1.ObjectMeta{Name: testDefaultName},
				Spec: namespacedpcv1alpha1.ProviderConfigSpec{
					Credentials: namespacedpcv1alpha1.ProviderCredentials{
						Source: xpv1.CredentialsSourceSecret,
						CommonCredentialSelectors: xpv1.CommonCredentialSelectors{
							SecretRef: &xpv1.SecretKeySelector{
								SecretReference: xpv1.SecretReference{Name: secret, Namespace: ns},
								Key:             testUnusedKey,
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

	cr := newNamespacedNetworkContainer("app-ns", "my-container", "", "ClusterProviderConfig")
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

	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", "", "SomeOtherKind")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for unsupported provider config kind, got nil")
	}
}

// ── shared helper unit tests ─────────────────────────────────────────────

// TestStringifyEAValue exercises every branch of the extensible-attribute
// value renderer: the plain string fast path exercised elsewhere via
// TestExtAttrsRoundTrip only covers the string case, so this test pins
// down the nil, ibclient.Bool (both true and false), []string, and
// default (numeric) cases too.
func TestStringifyEAValue(t *testing.T) {
	cases := map[string]struct {
		reason string
		in     interface{}
		want   string
	}{
		"Nil": {
			reason: "a nil EA value renders as an empty string",
			in:     nil,
			want:   "",
		},
		"String": {
			reason: "a string value passes through unchanged",
			in:     "prod",
			want:   "prod",
		},
		"BoolTrue": {
			reason: "ibclient.Bool(true) renders as the CRD's \"True\" literal",
			in:     ibclient.Bool(true),
			want:   "True",
		},
		"BoolFalse": {
			reason: "ibclient.Bool(false) renders as the CRD's \"False\" literal",
			in:     ibclient.Bool(false),
			want:   "False",
		},
		"StringSlice": {
			reason: "a []string value (as produced by EA.UnmarshalJSON for multi-value EAs) joins on commas",
			in:     []string{"a", "b", "c"},
			want:   "a,b,c",
		},
		"StringSliceEmpty": {
			reason: "an empty []string renders as an empty string",
			in:     []string{},
			want:   "",
		},
		"DefaultInt": {
			reason: "any other type (e.g. int) falls through to the default fmt.Sprintf branch",
			in:     42,
			want:   "42",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := stringifyEAValue(tc.in); got != tc.want {
				t.Errorf("%s: stringifyEAValue(%#v) = %q, want %q", tc.reason, tc.in, got, tc.want)
			}
		})
	}
}

func TestExtAttrsRoundTrip(t *testing.T) {
	in := map[string]string{testExtAttrKey: testExtAttrValue, "owner": "platform-team"}
	ea := buildEA(in)
	out := extAttrsFromEA(ea)
	if !extAttrsEqual(in, out) {
		t.Errorf("ExtAttrs round-trip: got %v, want %v", out, in)
	}
}

func TestExtAttrsEqualTreatsNilAndEmptyAsEqual(t *testing.T) {
	if !extAttrsEqual(nil, map[string]string{}) {
		t.Error("extAttrsEqual(nil, {}) = false, want true")
	}
}

func TestIsNotFoundClassifiesTypedError(t *testing.T) {
	err := ibclient.NewNotFoundError("boom")
	if !isNotFound(err) {
		t.Error("isNotFound(*ibclient.NotFoundError) = false, want true")
	}
}

func TestIsNotFoundClassifiesGenericStatusError(t *testing.T) {
	err := errGenericStatus(404)
	if !isNotFound(err) {
		t.Error("isNotFound(generic 404 error) = false, want true")
	}
	if isNotFound(errGenericStatus(500)) {
		t.Error("isNotFound(generic 500 error) = true, want false")
	}
}

func errGenericStatus(code int) error {
	return &genericStatusError{code: code}
}

type genericStatusError struct{ code int }

func (e *genericStatusError) Error() string {
	return "WAPI request error: " + itoa(e.code) + "('boom')\nContents:\n{}\n"
}

func TestIsIPv6CIDR(t *testing.T) {
	cases := map[string]struct {
		cidr string
		want bool
	}{
		"IPv4Cidr":       {cidr: testCIDR, want: false},
		"IPv4Slash32":    {cidr: "192.168.1.1/32", want: false},
		"IPv6Cidr":       {cidr: "2001:db8::/32", want: true},
		"IPv6Slash128":   {cidr: "::1/128", want: true},
		"MalformedColon": {cidr: "not-a-cidr:still-has-colon", want: true},
		"MalformedPlain": {cidr: "not-a-cidr", want: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := isIPv6CIDR(tc.cidr); got != tc.want {
				t.Errorf("isIPv6CIDR(%q) = %v, want %v", tc.cidr, got, tc.want)
			}
		})
	}
}

func TestLateInitializeBackfillsOptionalFields(t *testing.T) {
	var comment *string
	extAttrs := map[string]string(nil)

	nc := &ibclient.NetworkContainer{
		Comment: "server default",
		Ea:      ibclient.EA{testExtAttrKey: testExtAttrValue},
	}

	changed := lateInitialize(&comment, &extAttrs, nc)
	if !changed {
		t.Fatal("lateInitialize: want changed=true, got false")
	}
	if comment == nil || *comment != "server default" {
		t.Errorf("lateInitialize: comment = %v, want %q", comment, "server default")
	}
	if !extAttrsEqual(extAttrs, map[string]string{testExtAttrKey: testExtAttrValue}) {
		t.Errorf("lateInitialize: extAttrs = %v, want {env: prod}", extAttrs)
	}
}

func TestLateInitializeDoesNotOverwriteSetFields(t *testing.T) {
	comment := stringPtr("user comment")
	extAttrs := map[string]string{testExtAttrKey: "staging"}

	nc := &ibclient.NetworkContainer{
		Comment: "server default",
		Ea:      ibclient.EA{testExtAttrKey: testExtAttrValue},
	}

	changed := lateInitialize(&comment, &extAttrs, nc)
	if changed {
		t.Error("lateInitialize: want changed=false when all fields already set, got true")
	}
	if *comment != "user comment" {
		t.Errorf("lateInitialize: comment = %q, want unchanged %q", *comment, "user comment")
	}
	if !extAttrsEqual(extAttrs, map[string]string{testExtAttrKey: "staging"}) {
		t.Errorf("lateInitialize: extAttrs = %v, want unchanged {env: staging}", extAttrs)
	}
}

func TestIsUpToDateDetectsCommentDrift(t *testing.T) {
	nc := &ibclient.NetworkContainer{Comment: "current"}
	if isUpToDate(stringPtr("different"), nil, nc) {
		t.Error("isUpToDate: want false when comment differs, got true")
	}
	if !isUpToDate(stringPtr("current"), nil, nc) {
		t.Error("isUpToDate: want true when comment matches, got false")
	}
}

func TestIsUpToDateDetectsExtAttrsDrift(t *testing.T) {
	nc := &ibclient.NetworkContainer{Ea: ibclient.EA{testExtAttrKey: testExtAttrValue}}
	if isUpToDate(nil, map[string]string{testExtAttrKey: "staging"}, nc) {
		t.Error("isUpToDate: want false when extAttrs differ, got true")
	}
	if !isUpToDate(nil, map[string]string{testExtAttrKey: testExtAttrValue}, nc) {
		t.Error("isUpToDate: want true when extAttrs match, got false")
	}
}

// ── extractCredentials ────────────────────────────────────────────────────

func TestExtractCredentialsMissingSecretRef(t *testing.T) {
	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()

	_, err := extractCredentials(context.Background(), kube, xpv1.CredentialsSourceSecret, nil, "")
	if err == nil {
		t.Fatal("extractCredentials: expected error for nil secretRef, got nil")
	}
}

func TestExtractCredentialsUnsupportedSource(t *testing.T) {
	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()

	_, err := extractCredentials(context.Background(), kube, xpv1.CredentialsSourceNone, nil, "")
	if err == nil {
		t.Fatal("extractCredentials: expected error for unsupported credentials source, got nil")
	}
}

func TestExtractCredentialsMissingKeys(t *testing.T) {
	scheme := newTestScheme(t)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "infobloxnios-credentials", Namespace: testClusterNamespace},
		Data:       map[string][]byte{"host": []byte("grid.example.com")},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	_, err := extractCredentials(context.Background(), kube, xpv1.CredentialsSourceSecret, &xpv1.SecretKeySelector{
		SecretReference: xpv1.SecretReference{Name: "infobloxnios-credentials", Namespace: testClusterNamespace},
		Key:             testUnusedKey,
	}, "")
	if err == nil {
		t.Fatal("extractCredentials: expected error for missing username/password keys, got nil")
	}
}

func TestExtractCredentialsSslVerifyDefaultsTrue(t *testing.T) {
	scheme := newTestScheme(t)
	secret := credentialsSecret(testClusterNamespace, "infobloxnios-credentials", "grid.example.com", "admin", "s3cr3t")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	creds, err := extractCredentials(context.Background(), kube, xpv1.CredentialsSourceSecret, &xpv1.SecretKeySelector{
		SecretReference: xpv1.SecretReference{Name: "infobloxnios-credentials", Namespace: testClusterNamespace},
		Key:             testUnusedKey,
	}, "")
	if err != nil {
		t.Fatalf("extractCredentials: unexpected error: %v", err)
	}
	if !creds.SslVerify {
		t.Error("extractCredentials: expected SslVerify to default to true")
	}
}

func TestExtractCredentialsSslVerifyFalse(t *testing.T) {
	scheme := newTestScheme(t)
	secret := credentialsSecret(testClusterNamespace, "infobloxnios-credentials", "grid.example.com", "admin", "s3cr3t")
	secret.Data["ssl_verify"] = []byte("false")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	creds, err := extractCredentials(context.Background(), kube, xpv1.CredentialsSourceSecret, &xpv1.SecretKeySelector{
		SecretReference: xpv1.SecretReference{Name: "infobloxnios-credentials", Namespace: testClusterNamespace},
		Key:             testUnusedKey,
	}, "")
	if err != nil {
		t.Fatalf("extractCredentials: unexpected error: %v", err)
	}
	if creds.SslVerify {
		t.Error("extractCredentials: expected SslVerify=false when secret key is \"false\"")
	}
}
