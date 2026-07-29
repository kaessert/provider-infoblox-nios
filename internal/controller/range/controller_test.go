// Package rangepkg unit tests for the Range MR controllers. Tests use
// inline httptest.NewServer mocks that emulate the WAPI range endpoints,
// PascalCase test names (no underscores), and white-box access to the
// unexported connectors/clients so both scopes can be exercised without
// going through the full Connect() credential bridge on every test.
package rangepkg

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/range/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/range/v1alpha1"
	namespacedpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
)

// ── generic helpers ─────────────────────────────────────────────────────────

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

// newClusterRange builds a minimal cluster-scoped Range CR. When
// externalName is empty, the external-name annotation is left unset. When
// it equals crName it simulates the framework's NameAsExternalName
// initializer (the pre-create state); any other value simulates a
// Create()-assigned server ref.
func newClusterRange(crName, externalName string) *clusterv1alpha1.Range {
	cr := &clusterv1alpha1.Range{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: "test-uid-cluster"},
		Spec: clusterv1alpha1.RangeSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.RangeParameters{
				StartAddr: stringPtr("10.0.0.10"),
				EndAddr:   stringPtr("10.0.0.20"),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newNamespacedRange is the namespaced variant of newClusterRange.
func newNamespacedRange(ns, crName, externalName, pcKind string) *namespacedv1alpha1.Range {
	cr := &namespacedv1alpha1.Range{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: "test-uid-namespaced"},
		Spec: namespacedv1alpha1.RangeSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: "default"},
			},
			ForProvider: namespacedv1alpha1.RangeParameters{
				StartAddr: stringPtr("10.0.0.10"),
				EndAddr:   stringPtr("10.0.0.20"),
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
// mockWapiServer emulates the subset of NIOS WAPI range endpoints
// exercised by the Range controller (POST create, GET/PUT/DELETE by
// _ref). Records are marshaled/unmarshaled using the real ibclient.Range
// type so the wire format (including the EA {"value": ...} envelope)
// exactly matches what the SDK sends and expects.

type mockWapiServer struct {
	mu      sync.Mutex
	ranges  map[string]*ibclient.Range
	nextRef int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert immutable fields are omitted.
	lastUpdateBody []byte
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{ranges: map[string]*ibclient.Range{}}
}

func (m *mockWapiServer) seed(rng *ibclient.Range) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRef++
	if rng.Ref == "" {
		rng.Ref = m.newRefLocked(rng)
	}
	m.ranges[rng.Ref] = rng
	return rng.Ref
}

func (m *mockWapiServer) newRefLocked(rng *ibclient.Range) string {
	start := ""
	if rng.StartAddr != nil {
		start = *rng.StartAddr
	}
	nv := "default"
	if rng.NetworkView != nil && *rng.NetworkView != "" {
		nv = *rng.NetworkView
	}
	return "range/test" + itoa(m.nextRef) + ":" + start + "/" + nv
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

// handler returns an http.Handler implementing the range WAPI surface.
func (m *mockWapiServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/range", func(w http.ResponseWriter, r *http.Request) {
		var rng ibclient.Range
		if err := json.NewDecoder(r.Body).Decode(&rng); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		// Mirror live NIOS behavior: network_view defaults to "default"
		// server-side when the create request omits it.
		if rng.NetworkView == nil || *rng.NetworkView == "" {
			nv := "default"
			rng.NetworkView = &nv
		}
		ref := m.seed(&rng)
		writeJSON(w, http.StatusOK, ref)
	})

	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		rng, ok := m.ranges[ref]
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, rng)
	})

	mux.HandleFunc("PUT /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		existing, ok := m.ranges[ref]
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
		var incoming ibclient.Range
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body
		existing.StartAddr = incoming.StartAddr
		existing.EndAddr = incoming.EndAddr
		existing.NetworkView = incoming.NetworkView
		existing.Network = incoming.Network
		existing.Comment = incoming.Comment
		existing.Ea = incoming.Ea
		// template is intentionally NOT applied from the incoming body —
		// UpdateNetworkRange never sends it (immutable, create-only),
		// and this mock deliberately does not simulate a WAPI-side
		// mutation for a field the controller never transmits.
		m.mu.Unlock()

		writeJSON(w, http.StatusOK, ref)
	})

	mux.HandleFunc("DELETE /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		_, ok := m.ranges[ref]
		delete(m.ranges, ref)
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

	ref := m.seed(&ibclient.Range{
		StartAddr:   stringPtr("10.0.0.10"),
		EndAddr:     stringPtr("10.0.0.20"),
		NetworkView: stringPtr("default"),
		Network:     stringPtr("10.0.0.0/24"),
		Comment:     stringPtr("hello"),
		Ea:          ibclient.EA{"env": "prod"},
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterRange("my-range", ref)
	cr.Spec.ForProvider.NetworkView = stringPtr("default")
	cr.Spec.ForProvider.Network = stringPtr("10.0.0.0/24")
	cr.Spec.ForProvider.Comment = stringPtr("hello")
	cr.Spec.ForProvider.ExtAttrs = map[string]string{"env": "prod"}

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
	if cr.Status.AtProvider.Ref == nil || *cr.Status.AtProvider.Ref != ref {
		t.Errorf("AtProvider.Ref = %v, want %q", cr.Status.AtProvider.Ref, ref)
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
	cr := newClusterRange("my-range", "range/does-not-exist:10.0.0.10/default")

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
	cr := newClusterRange("my-range", "")  // external-name unset
	meta.SetExternalName(cr, cr.GetName()) // simulate NameAsExternalName initializer

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
	cr := newClusterRange("my-range", "range/test1:10.0.0.10/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterRange("my-range", "range/test1:10.0.0.10/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestClusterObserveMinimalResponse pins nil-safety in Observe: a WAPI
// response carrying only the object's _ref (the resource identifier) and
// every other field at its Go zero value (nil pointers, empty strings, a
// nil Ea map) must not panic and must produce a valid observation with
// nil-safe AtProvider fields. observeFromRange copies optional pointer
// fields directly (never dereferences without a nil guard), so this test
// also pins that contract for future edits.
func TestClusterObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Range{})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterRange("my-range", ref)

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
	if ap.StartAddr != nil {
		t.Errorf("AtProvider.StartAddr = %v, want nil", ap.StartAddr)
	}
	if ap.EndAddr != nil {
		t.Errorf("AtProvider.EndAddr = %v, want nil", ap.EndAddr)
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

// TestClusterObserveLateInitializesNetworkView proves that an
// empty/omitted spec.forProvider.networkView is back-filled from the
// observed Range's server-defaulted value ("default"), per the
// blueprint's documented server-default behavior.
func TestClusterObserveLateInitializesNetworkView(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Range{
		StartAddr:   stringPtr("10.0.0.10"),
		EndAddr:     stringPtr("10.0.0.20"),
		NetworkView: stringPtr("default"),
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterRange("my-range", ref) // NetworkView left nil in spec

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceLateInitialized {
		t.Error("Observe: want ResourceLateInitialized=true, got false")
	}
	if cr.Spec.ForProvider.NetworkView == nil || *cr.Spec.ForProvider.NetworkView != "default" {
		t.Errorf("Observe: spec.forProvider.networkView = %v, want back-filled to %q", cr.Spec.ForProvider.NetworkView, "default")
	}
}

// ── cluster: Create ─────────────────────────────────────────────────────

func TestClusterCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterRange("my-range", "") // no external-name yet

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

func TestClusterObserveIsUpToDateIgnoresImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Range{
		StartAddr:   stringPtr("10.0.0.10"),
		EndAddr:     stringPtr("10.0.0.20"),
		NetworkView: stringPtr("default"),
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterRange("my-range", ref)
	cr.Spec.ForProvider.NetworkView = stringPtr("default")
	// Mutate the immutable template field in spec — this must NOT affect
	// ResourceUpToDate, since template is excluded from isUpToDate
	// (create-only parameter, never echoed back by GetNetworkRange).
	cr.Spec.ForProvider.Template = stringPtr("some-template")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true despite template drift (immutable field), got false")
	}
}

// ── cluster: Update ─────────────────────────────────────────────────────

func TestClusterUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Range{
		StartAddr:   stringPtr("10.0.0.10"),
		EndAddr:     stringPtr("10.0.0.20"),
		NetworkView: stringPtr("default"),
		Comment:     stringPtr("old comment"),
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterRange("my-range", ref)
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.ranges[ref]
	m.mu.Unlock()
	if stored.Comment == nil || *stored.Comment != "new comment" {
		t.Errorf("Update: stored comment = %v, want %q", stored.Comment, "new comment")
	}
}

func TestClusterUpdateDoesNotSendImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Range{
		StartAddr:   stringPtr("10.0.0.10"),
		EndAddr:     stringPtr("10.0.0.20"),
		NetworkView: stringPtr("default"),
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterRange("my-range", ref)
	cr.Spec.ForProvider.Template = stringPtr("some-template")

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
	if _, present := raw["template"]; present {
		t.Errorf("Update: request body contains immutable field 'template': %v", raw["template"])
	}
}

func TestClusterCreateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterRange("my-range", "")

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
}

func TestClusterUpdateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterRange("my-range", "range/test1:10.0.0.10/default")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	if _, err := e.Update(context.Background(), cr); err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
}

// ── cluster: Delete ─────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Range{StartAddr: stringPtr("10.0.0.10"), EndAddr: stringPtr("10.0.0.20")})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterRange("my-range", ref)

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}

	m.mu.Lock()
	_, stillExists := m.ranges[ref]
	m.mu.Unlock()
	if stillExists {
		t.Error("Delete: range still present after Delete")
	}
}

func TestClusterDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterRange("my-range", "range/does-not-exist:10.0.0.10/default")

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
	cr := newClusterRange("my-range", "range/test1:10.0.0.10/default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteRange) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteRange)
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
				ObjectMeta: metav1.ObjectMeta{Name: "default"},
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

	cr := newClusterRange("my-range", "")
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

	cr := newClusterRange("my-range", "")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for missing ProviderConfig, got nil")
	}
}

// ── namespaced: Observe ──────────────────────────────────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Range{
		StartAddr:   stringPtr("10.0.0.10"),
		EndAddr:     stringPtr("10.0.0.20"),
		NetworkView: stringPtr("default"),
	})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedRange("default", "my-range", ref, "ProviderConfig")

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
	cr := newNamespacedRange("default", "my-range", "range/does-not-exist:10.0.0.10/default", "ProviderConfig")

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
	cr := newNamespacedRange("default", "my-range", "", "ProviderConfig")
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
	cr := newNamespacedRange("default", "my-range", "range/test1:10.0.0.10/default", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestNamespacedObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedRange("default", "my-range", "range/test1:10.0.0.10/default", "ProviderConfig")

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
	cr := newNamespacedRange("default", "my-range", "", "ProviderConfig")

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

	ref := m.seed(&ibclient.Range{
		StartAddr:   stringPtr("10.0.0.10"),
		EndAddr:     stringPtr("10.0.0.20"),
		NetworkView: stringPtr("default"),
	})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedRange("default", "my-range", ref, "ProviderConfig")
	cr.Spec.ForProvider.EndAddr = stringPtr("10.0.0.50")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.ranges[ref]
	m.mu.Unlock()
	if stored.EndAddr == nil || *stored.EndAddr != "10.0.0.50" {
		t.Errorf("Update: stored endAddr = %v, want 10.0.0.50", stored.EndAddr)
	}
}

func TestNamespacedUpdateDoesNotSendImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Range{
		StartAddr:   stringPtr("10.0.0.10"),
		EndAddr:     stringPtr("10.0.0.20"),
		NetworkView: stringPtr("default"),
	})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedRange("default", "my-range", ref, "ProviderConfig")
	cr.Spec.ForProvider.Template = stringPtr("some-template")

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
	if _, present := raw["template"]; present {
		t.Errorf("Update: request body contains immutable field 'template': %v", raw["template"])
	}
}

func TestNamespacedCreateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedRange("default", "my-range", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
}

func TestNamespacedUpdateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedRange("default", "my-range", "range/test1:10.0.0.10/default", "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	if _, err := e.Update(context.Background(), cr); err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
}

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Range{StartAddr: stringPtr("10.0.0.10"), EndAddr: stringPtr("10.0.0.20")})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedRange("default", "my-range", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
}

func TestNamespacedDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedRange("default", "my-range", "range/does-not-exist:10.0.0.10/default", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error for already-gone resource, got: %v", err)
	}
}

// TestNamespacedDeleteServerError verifies that a 5xx response from the
// WAPI delete endpoint is propagated (wrapped, not swallowed) rather than
// being treated as a not-found/already-deleted success.
func TestNamespacedDeleteServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedRange("default", "my-range", "range/test1:10.0.0.10/default", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteRange) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteRange)
	}
}

// ── namespaced: Connect ───────────────────────────────────────────────────

func TestNamespacedConnectWithProviderConfig(t *testing.T) {
	const (
		ns     = "default"
		secret = "infobloxnios-api-key"
	)

	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			credentialsSecret(ns, secret, "grid.example.com", "admin", "s3cr3t"),
			&namespacedpcv1alpha1.ProviderConfig{
				ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: ns},
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

	cr := newNamespacedRange(ns, "my-range", "", "ProviderConfig")
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
				ObjectMeta: metav1.ObjectMeta{Name: "default"},
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

	cr := newNamespacedRange("app-ns", "my-range", "", "ClusterProviderConfig")
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

	cr := newNamespacedRange("default", "my-range", "", "SomeOtherKind")
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

// ── shared helper unit tests ─────────────────────────────────────────────

func TestExtAttrsRoundTrip(t *testing.T) {
	in := map[string]string{"env": "prod", "owner": "platform-team"}
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

func TestLateInitializeBackfillsOptionalFields(t *testing.T) {
	var networkView *string
	var network *string
	var comment *string
	extAttrs := map[string]string(nil)

	rng := &ibclient.Range{
		NetworkView: stringPtr("default"),
		Network:     stringPtr("10.0.0.0/24"),
		Comment:     stringPtr("server default"),
		Ea:          ibclient.EA{"env": "prod"},
	}

	changed := lateInitialize(&networkView, &network, &comment, &extAttrs, rng)
	if !changed {
		t.Fatal("lateInitialize: want changed=true, got false")
	}
	if networkView == nil || *networkView != "default" {
		t.Errorf("lateInitialize: networkView = %v, want %q", networkView, "default")
	}
	if network == nil || *network != "10.0.0.0/24" {
		t.Errorf("lateInitialize: network = %v, want %q", network, "10.0.0.0/24")
	}
	if comment == nil || *comment != "server default" {
		t.Errorf("lateInitialize: comment = %v, want %q", comment, "server default")
	}
	if !extAttrsEqual(extAttrs, map[string]string{"env": "prod"}) {
		t.Errorf("lateInitialize: extAttrs = %v, want {env: prod}", extAttrs)
	}
}

func TestLateInitializeDoesNotOverwriteSetFields(t *testing.T) {
	networkView := stringPtr("custom-view")
	network := stringPtr("192.168.0.0/24")
	comment := stringPtr("user comment")
	extAttrs := map[string]string{"env": "staging"}

	rng := &ibclient.Range{
		NetworkView: stringPtr("default"),
		Network:     stringPtr("10.0.0.0/24"),
		Comment:     stringPtr("server default"),
		Ea:          ibclient.EA{"env": "prod"},
	}

	changed := lateInitialize(&networkView, &network, &comment, &extAttrs, rng)
	if changed {
		t.Error("lateInitialize: want changed=false when all fields already set, got true")
	}
	if *networkView != "custom-view" || *network != "192.168.0.0/24" || *comment != "user comment" || extAttrs["env"] != "staging" {
		t.Error("lateInitialize: overwrote already-set ForProvider fields")
	}
}

// TestObserveDoesNotLateInitializeRequiredFields proves that startAddr and
// endAddr — the CRD's required RangeParameters fields — are never
// overwritten by Observe()'s late-init step. lateInitialize only accepts
// pointers to the optional fields (networkView, network, comment,
// extAttrs).
func TestObserveDoesNotLateInitializeRequiredFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Range{
		StartAddr: stringPtr("10.0.0.100"),
		EndAddr:   stringPtr("10.0.0.200"),
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterRange("my-range", ref)
	cr.Spec.ForProvider.StartAddr = stringPtr("10.0.0.10")
	cr.Spec.ForProvider.EndAddr = stringPtr("10.0.0.20")

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}

	if got := *cr.Spec.ForProvider.StartAddr; got != "10.0.0.10" {
		t.Errorf("Observe: required field StartAddr late-initialized to %q, want unchanged %q", got, "10.0.0.10")
	}
	if got := *cr.Spec.ForProvider.EndAddr; got != "10.0.0.20" {
		t.Errorf("Observe: required field EndAddr late-initialized to %q, want unchanged %q", got, "10.0.0.20")
	}
}

// ── isUpToDate: table-driven field comparison ───────────────────────────

func TestIsUpToDate(t *testing.T) {
	observedRng := func() *ibclient.Range {
		return &ibclient.Range{
			StartAddr:   stringPtr("10.0.0.10"),
			EndAddr:     stringPtr("10.0.0.20"),
			NetworkView: stringPtr("default"),
			Network:     stringPtr("10.0.0.0/24"),
			Comment:     stringPtr("hello"),
			Ea:          ibclient.EA{"env": "prod"},
		}
	}

	cases := map[string]struct {
		mutate func(*clusterv1alpha1.RangeParameters)
		want   bool
	}{
		"MatchesExactly": {
			mutate: func(p *clusterv1alpha1.RangeParameters) {},
			want:   true,
		},
		"StartAddrDiffers": {
			mutate: func(p *clusterv1alpha1.RangeParameters) { p.StartAddr = stringPtr("10.0.0.99") },
			want:   false,
		},
		"EndAddrDiffers": {
			mutate: func(p *clusterv1alpha1.RangeParameters) { p.EndAddr = stringPtr("10.0.0.99") },
			want:   false,
		},
		"NetworkViewDiffers": {
			mutate: func(p *clusterv1alpha1.RangeParameters) { p.NetworkView = stringPtr("other-view") },
			want:   false,
		},
		"NetworkDiffers": {
			mutate: func(p *clusterv1alpha1.RangeParameters) { p.Network = stringPtr("192.168.0.0/24") },
			want:   false,
		},
		"CommentDiffers": {
			mutate: func(p *clusterv1alpha1.RangeParameters) { p.Comment = stringPtr("different") },
			want:   false,
		},
		"ExtAttrsDiffer": {
			mutate: func(p *clusterv1alpha1.RangeParameters) { p.ExtAttrs = map[string]string{"env": "staging"} },
			want:   false,
		},
		"TemplateDiffersButIgnored": {
			mutate: func(p *clusterv1alpha1.RangeParameters) { p.Template = stringPtr("ignored-template") },
			want:   true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			p := clusterv1alpha1.RangeParameters{
				StartAddr:   stringPtr("10.0.0.10"),
				EndAddr:     stringPtr("10.0.0.20"),
				NetworkView: stringPtr("default"),
				Network:     stringPtr("10.0.0.0/24"),
				Comment:     stringPtr("hello"),
				ExtAttrs:    map[string]string{"env": "prod"},
			}
			tc.mutate(&p)

			got := isUpToDate(p.StartAddr, p.EndAddr, p.NetworkView, p.Network, p.Comment, p.ExtAttrs, observedRng())
			if got != tc.want {
				t.Errorf("isUpToDate() = %v, want %v", got, tc.want)
			}
		})
	}
}
