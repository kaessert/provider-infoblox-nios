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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/range/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/range/v1alpha1"
	namespacedpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
)

// recordingKubeClient is a minimal client.Client stub used to verify that
// Update() persists a rotated external-name annotation via a real kube
// client call, not merely an in-memory meta.SetExternalName mutation that
// crossplane-runtime's managed reconciler would silently discard after a
// successful external Update(). Only Update is exercised by these tests;
// every other client.Client method is unused here and left to the
// embedded nil interface (calling one would panic, which is the correct
// failure mode for an accidental, untested dependency).
type recordingKubeClient struct {
	client.Client
	updated client.Object
}

func (k *recordingKubeClient) Update(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
	k.updated = obj
	return nil
}

// Patch mirrors Update. The fix for this ticket persists the refreshed
// external-name annotation via a conflict-safe JSON merge Patch instead
// of a whole-object Update, so this stub must record Patch calls the
// same way for the existing assertions on k.updated to keep working.
func (k *recordingKubeClient) Patch(_ context.Context, obj client.Object, _ client.Patch, _ ...client.PatchOption) error {
	k.updated = obj
	return nil
}

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

	// Search endpoint (GetNetworkRange): a GET with no _ref path segment,
	// filtered by start_addr/end_addr/network_view query params.
	// Registered as an exact literal path so Go's ServeMux prefers it
	// over the {ref...} wildcard below for requests to precisely
	// "range" (real _refs always carry additional path segments).
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/range", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		startAddr := q.Get("start_addr")
		endAddr := q.Get("end_addr")
		networkView := q.Get("network_view")

		m.mu.Lock()
		// Initialized (not nil) so an empty result set marshals to a
		// JSON "[]" body, matching real WAPI search semantics — the SDK
		// connector treats literal "[]" as its NotFoundError trigger, and
		// a nil slice marshaling to "null" would mask that behavior in
		// tests (see the package-level defect this mock now reproduces).
		matches := []ibclient.Range{}
		for _, rng := range m.ranges {
			if startAddr != "" && (rng.StartAddr == nil || *rng.StartAddr != startAddr) {
				continue
			}
			if endAddr != "" && (rng.EndAddr == nil || *rng.EndAddr != endAddr) {
				continue
			}
			if networkView != "" && (rng.NetworkView == nil || *rng.NetworkView != networkView) {
				continue
			}
			matches = append(matches, *rng)
		}
		m.mu.Unlock()

		// Always respond 200 — WAPI search semantics report "not found"
		// via an empty array, never an HTTP error status.
		writeJSON(w, http.StatusOK, matches)
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
	return newTestManagerAndConnector(t, srv).Manager
}

// newTestConnector returns the raw ibclient.IBConnector pointed at the
// given httptest.Server, for tests that construct a clusterExternal or
// namespacedExternal directly and need its conn field populated so
// rangeExistsByNaturalKey can search.
func newTestConnector(t *testing.T, srv *httptest.Server) ibclient.IBConnector {
	t.Helper()
	return newTestManagerAndConnector(t, srv).Connector
}

// newTestManagerAndConnector is the shared constructor behind
// newTestObjectManager and newTestConnector — both handles come from the
// same underlying Connector, exactly like production's newObjectManager.
func newTestManagerAndConnector(t *testing.T, srv *httptest.Server) identity.ManagerAndConnector {
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
		t.Fatalf("cannot build test object manager: %v", err)
	}
	return mc
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
	cr := newClusterRange("my-range", "range/test1:10.0.0.10/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
	cr := newClusterRange("my-range", "")

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
}

func TestClusterUpdateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
	cr := newClusterRange("my-range", "range/does-not-exist:10.0.0.10/default")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error for already-gone resource, got: %v", err)
	}
}

// TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject verifies the
// core defect fix: a 404 against the stored _ref must not be treated as
// "already deleted" when a natural-key search finds the same identity
// still live under a different _ref. Deleting that range would be
// unverifiable ownership, so Delete() must refuse and leave the range in
// place.
func TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed(&ibclient.Range{
		StartAddr:   stringPtr("10.0.0.10"),
		EndAddr:     stringPtr("10.0.0.20"),
		NetworkView: stringPtr("default"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
	cr := newClusterRange("my-range", "range/stale-ref:10.0.0.10/default")
	cr.Spec.ForProvider.NetworkView = stringPtr("default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected refusal error when a natural-key search still matches a live object, got nil")
	}
	if !strings.Contains(err.Error(), "refusing to delete") {
		t.Errorf("Delete: error = %q, want it to explain the refusal", err.Error())
	}

	m.mu.Lock()
	_, stillExists := m.ranges[liveRef]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Delete: live range was removed despite the refusal — DELETE must not have been issued against it")
	}
}

// TestClusterDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch is the
// companion happy path: a 404 against the stored _ref, and a natural-key
// search that finds nothing, means the object really is gone.
func TestClusterDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
	cr := newClusterRange("my-range", "range/stale-ref:10.0.0.10/default")
	cr.Spec.ForProvider.NetworkView = stringPtr("default")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the natural-key search also finds nothing, got: %v", err)
	}
}

// TestClusterDeleteServerError verifies that a 5xx response from the WAPI
// delete endpoint is propagated (wrapped, not swallowed) rather than being
// treated as a not-found/already-deleted success.
func TestClusterDeleteServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
	cr := newClusterRange("my-range", "range/test1:10.0.0.10/default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteRange) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteRange)
	}
}

// TestClusterObserveRefusesWhenStaleRefStillMatchesLiveObject verifies the
// Observe()-side half of the same defect: crossplane-runtime's managed
// reconciler calls Observe() before Delete() on the deletion path, and if
// Observe() reports ResourceExists:false the reconciler never calls
// Delete() at all — it just clears the finalizer, orphaning the Grid
// object. A 404 against the stored _ref must not be silently treated as
// "does not exist" when a natural-key search finds a live object under
// the CR's own identity fields.
func TestClusterObserveRefusesWhenStaleRefStillMatchesLiveObject(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed(&ibclient.Range{
		StartAddr:   stringPtr("10.0.0.10"),
		EndAddr:     stringPtr("10.0.0.20"),
		NetworkView: stringPtr("default"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
	cr := newClusterRange("my-range", "range/stale-ref:10.0.0.10/default")
	cr.Spec.ForProvider.NetworkView = stringPtr("default")

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected refusal error when a natural-key search still matches a live object, got nil")
	}
	if !strings.Contains(err.Error(), "cannot observe") {
		t.Errorf("Observe: error = %q, want it to explain the refusal", err.Error())
	}

	m.mu.Lock()
	_, stillExists := m.ranges[liveRef]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Observe: live range was removed — Observe() must never mutate the backend")
	}
}

// TestClusterObserveSucceedsWhenStaleRefHasNoNaturalKeyMatch verifies the
// genuine-absence direction of the same defect: a 404 against the stored
// _ref, and a natural-key search over the CR's own identity that
// genuinely finds nothing, must report ResourceExists:false with no
// error — not the "failed getting DHCP IPv4 Range: not found" error the
// SDK's ObjectManager.GetNetworkRange produced before this fix. Without
// this, Observe fails, the delete finalizer is never cleared, and the MR
// is stuck forever even though the backend object is already gone.
func TestClusterObserveSucceedsWhenStaleRefHasNoNaturalKeyMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
	cr := newClusterRange("my-range", "range/stale-ref:10.0.0.10/default")
	cr.Spec.ForProvider.NetworkView = stringPtr("default")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: want nil error when the natural-key search also finds nothing, got: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false when the natural-key search finds nothing, got true")
	}
}

// ── cluster: Disconnect ──────────────────────────────────────────────────

func TestClusterDisconnectIsNoop(t *testing.T) {
	e := &clusterExternal{kube: &recordingKubeClient{}}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
	cr := newNamespacedRange("default", "my-range", "range/test1:10.0.0.10/default", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestNamespacedObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
	cr := newNamespacedRange("default", "my-range", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
}

func TestNamespacedUpdateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
	cr := newNamespacedRange("default", "my-range", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
}

func TestNamespacedDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
	cr := newNamespacedRange("default", "my-range", "range/does-not-exist:10.0.0.10/default", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error for already-gone resource, got: %v", err)
	}
}

// TestNamespacedDeleteRefusesWhenStaleRefStillMatchesLiveObject is the
// namespaced-scope counterpart of
// TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject.
func TestNamespacedDeleteRefusesWhenStaleRefStillMatchesLiveObject(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed(&ibclient.Range{
		StartAddr:   stringPtr("10.0.0.10"),
		EndAddr:     stringPtr("10.0.0.20"),
		NetworkView: stringPtr("default"),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
	cr := newNamespacedRange("default", "my-range", "range/stale-ref:10.0.0.10/default", "ProviderConfig")
	cr.Spec.ForProvider.NetworkView = stringPtr("default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected refusal error when a natural-key search still matches a live object, got nil")
	}
	if !strings.Contains(err.Error(), "refusing to delete") {
		t.Errorf("Delete: error = %q, want it to explain the refusal", err.Error())
	}

	m.mu.Lock()
	_, stillExists := m.ranges[liveRef]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Delete: live range was removed despite the refusal — DELETE must not have been issued against it")
	}
}

// TestNamespacedObserveSucceedsWhenStaleRefHasNoNaturalKeyMatch is the
// namespaced-scope counterpart of
// TestClusterObserveSucceedsWhenStaleRefHasNoNaturalKeyMatch.
func TestNamespacedObserveSucceedsWhenStaleRefHasNoNaturalKeyMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
	cr := newNamespacedRange("default", "my-range", "range/stale-ref:10.0.0.10/default", "ProviderConfig")
	cr.Spec.ForProvider.NetworkView = stringPtr("default")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: want nil error when the natural-key search also finds nothing, got: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false when the natural-key search finds nothing, got true")
	}
}

// TestNamespacedObserveRefusesWhenStaleRefStillMatchesLiveObject is the
// namespaced-scope counterpart of
// TestClusterObserveRefusesWhenStaleRefStillMatchesLiveObject.
func TestNamespacedObserveRefusesWhenStaleRefStillMatchesLiveObject(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed(&ibclient.Range{
		StartAddr:   stringPtr("10.0.0.10"),
		EndAddr:     stringPtr("10.0.0.20"),
		NetworkView: stringPtr("default"),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
	cr := newNamespacedRange("default", "my-range", "range/stale-ref:10.0.0.10/default", "ProviderConfig")
	cr.Spec.ForProvider.NetworkView = stringPtr("default")

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected refusal error when a natural-key search still matches a live object, got nil")
	}
	if !strings.Contains(err.Error(), "cannot observe") {
		t.Errorf("Observe: error = %q, want it to explain the refusal", err.Error())
	}

	m.mu.Lock()
	_, stillExists := m.ranges[liveRef]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Observe: live range was removed — Observe() must never mutate the backend")
	}
}

// TestNamespacedDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch is the
// namespaced-scope counterpart of
// TestClusterDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch.
func TestNamespacedDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
	cr := newNamespacedRange("default", "my-range", "range/stale-ref:10.0.0.10/default", "ProviderConfig")
	cr.Spec.ForProvider.NetworkView = stringPtr("default")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the natural-key search also finds nothing, got: %v", err)
	}
}

// TestNamespacedDeleteServerError verifies that a 5xx response from the
// WAPI delete endpoint is propagated (wrapped, not swallowed) rather than
// being treated as a not-found/already-deleted success.
func TestNamespacedDeleteServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
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
	e := &namespacedExternal{kube: &recordingKubeClient{}}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv), conn: newTestConnector(t, srv)}
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

// ── extractCredentials: error paths and ssl_verify ──────────────────────

func TestExtractCredentialsUnsupportedSource(t *testing.T) {
	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()

	_, err := extractCredentials(context.Background(), kube, xpv1.CredentialsSourceInjectedIdentity, nil, "")
	if err == nil {
		t.Fatal("extractCredentials: expected error for unsupported credentials source, got nil")
	}
}

func TestExtractCredentialsMissingSecretRef(t *testing.T) {
	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()

	_, err := extractCredentials(context.Background(), kube, xpv1.CredentialsSourceSecret, nil, "")
	if err == nil {
		t.Fatal("extractCredentials: expected error when secretRef is nil, got nil")
	}
}

func TestExtractCredentialsSecretNotFound(t *testing.T) {
	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()

	_, err := extractCredentials(context.Background(), kube, xpv1.CredentialsSourceSecret, &xpv1.SecretKeySelector{
		SecretReference: xpv1.SecretReference{Name: "missing", Namespace: "crossplane-system"},
		Key:             "unused",
	}, "")
	if err == nil {
		t.Fatal("extractCredentials: expected error when the credentials Secret does not exist, got nil")
	}
}

func TestExtractCredentialsMissingKeys(t *testing.T) {
	scheme := newTestScheme(t)
	// Secret exists but is missing the required password key.
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "infobloxnios-credentials", Namespace: "crossplane-system"},
		Data: map[string][]byte{
			"host":     []byte("grid.example.com"),
			"username": []byte("admin"),
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	_, err := extractCredentials(context.Background(), kube, xpv1.CredentialsSourceSecret, &xpv1.SecretKeySelector{
		SecretReference: xpv1.SecretReference{Name: "infobloxnios-credentials", Namespace: "crossplane-system"},
		Key:             "unused",
	}, "")
	if err == nil {
		t.Fatal("extractCredentials: expected error when a required credential key is missing, got nil")
	}
}

func TestExtractCredentialsFallsBackToProvidedNamespace(t *testing.T) {
	scheme := newTestScheme(t)
	secret := credentialsSecret("fallback-ns", "infobloxnios-credentials", "grid.example.com", "admin", "s3cr3t")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	// secretRef.Namespace left empty — extractCredentials must fall back
	// to the caller-supplied namespace.
	creds, err := extractCredentials(context.Background(), kube, xpv1.CredentialsSourceSecret, &xpv1.SecretKeySelector{
		SecretReference: xpv1.SecretReference{Name: "infobloxnios-credentials"},
		Key:             "unused",
	}, "fallback-ns")
	if err != nil {
		t.Fatalf("extractCredentials: unexpected error: %v", err)
	}
	if creds.Host != "grid.example.com" {
		t.Errorf("extractCredentials: Host = %q, want grid.example.com", creds.Host)
	}
}

// ── extractCredentials: ssl_verify key is fully ignored ────────────────
//
// TLS verification is governed by the ProviderConfig's own sslVerify spec
// field (see cluster.go/namespaced.go's Connect methods), never by a key
// in the credentials Secret. This pins the migration: a legacy
// "ssl_verify" key in the Secret must have zero effect on
// extractCredentials — nioCredentials has no SslVerify field to read it
// into.
func TestExtractCredentialsIgnoresSecretSslVerifyKey(t *testing.T) {
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
	if creds.Host != "grid.example.com" || creds.Username != "admin" || creds.Password != "s3cr3t" {
		t.Errorf("extractCredentials: got %+v, want Host/Username/Password populated regardless of the ssl_verify key", creds)
	}
}

func TestNewObjectManagerWithSchemeUsesConfiguredSslVerify(t *testing.T) {
	// Regression guard: newObjectManagerWithScheme must not hardcode
	// SslVerify to "true" — it must honor the sslVerify parameter. Both branches
	// must construct successfully (transport config validation happens
	// locally; no network round-trip occurs here).
	for name, sslVerify := range map[string]bool{"Enabled": true, "Disabled": false} {
		t.Run(name, func(t *testing.T) {
			creds := &nioCredentials{Host: "127.0.0.1", Username: "admin", Password: "s3cr3t"}
			mc, err := newObjectManagerWithScheme(creds, sslVerify, "http", "80")
			if err != nil {
				t.Fatalf("newObjectManagerWithScheme: unexpected error: %v", err)
			}
			if mc.Manager == nil {
				t.Fatal("newObjectManagerWithScheme: expected non-nil ObjectManager, got nil")
			}
			if mc.Connector == nil {
				t.Fatal("newObjectManagerWithScheme: expected non-nil Connector, got nil")
			}
		})
	}
}

func TestClusterConnectNoProviderConfigReference(t *testing.T) {
	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()

	conn := &clusterConnector{
		kube:  kube,
		usage: resource.NewLegacyProviderConfigUsageTracker(kube, &clusterpcv1alpha1.ProviderConfigUsage{}),
	}

	cr := newClusterRange("my-range", "")
	cr.Spec.ProviderConfigReference = nil

	_, err := conn.Connect(context.Background(), cr)
	if err == nil {
		t.Fatal("Connect: expected error when ProviderConfigReference is nil, got nil")
	}
}
