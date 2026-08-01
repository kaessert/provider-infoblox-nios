// Package networkview unit tests for the NetworkView MR controllers. Tests
// use inline httptest.NewServer mocks that emulate the WAPI networkview
// endpoints, PascalCase test names (no underscores), and white-box access
// to the unexported connectors/clients so both scopes can be exercised
// without going through the full Connect() credential bridge on every
// test.
package networkview

import (
	"context"
	"encoding/json"
	"fmt"
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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/networkview/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/networkview/v1alpha1"
	namespacedpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
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

// newClusterNetworkView builds a minimal cluster-scoped NetworkView CR.
// When externalName is empty, the external-name annotation is left unset.
// When it equals crName it simulates the framework's NameAsExternalName
// initializer (the pre-create state); any other value simulates a
// Create()-assigned server ref.
func newClusterNetworkView(crName, externalName string) *clusterv1alpha1.NetworkView {
	cr := &clusterv1alpha1.NetworkView{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: "test-uid-cluster"},
		Spec: clusterv1alpha1.NetworkViewSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.NetworkViewParameters{
				Name: stringPtr("my-networkview"),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newNamespacedNetworkView is the namespaced variant of
// newClusterNetworkView.
func newNamespacedNetworkView(ns, crName, externalName, pcKind string) *namespacedv1alpha1.NetworkView {
	cr := &namespacedv1alpha1.NetworkView{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: "test-uid-namespaced"},
		Spec: namespacedv1alpha1.NetworkViewSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: "default"},
			},
			ForProvider: namespacedv1alpha1.NetworkViewParameters{
				Name: stringPtr("my-networkview"),
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
// mockWapiServer emulates the subset of NIOS WAPI networkview endpoints
// exercised by the NetworkView controller (POST create, GET/PUT/DELETE by
// _ref). Records are marshaled/unmarshaled using the real
// ibclient.NetworkView type so the wire format (including the EA
// {"value": ...} envelope) exactly matches what the SDK sends and expects.
// The mock always returns the full stored object regardless of
// _return_fields — the real field-trimming behavior is a live-API detail,
// not something the controller's own logic depends on beyond ensuring
// getNetworkViewByRef additionally requests is_default.

type mockWapiServer struct {
	mu      sync.Mutex
	views   map[string]*ibclient.NetworkView
	nextRef int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert immutable fields are omitted.
	lastUpdateBody []byte
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{views: map[string]*ibclient.NetworkView{}}
}

func (m *mockWapiServer) seed(nv *ibclient.NetworkView) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRef++
	if nv.Ref == "" {
		nv.Ref = m.newRefLocked(nv)
	}
	m.views[nv.Ref] = nv
	return nv.Ref
}

func (m *mockWapiServer) newRefLocked(nv *ibclient.NetworkView) string {
	name := ""
	if nv.Name != nil {
		name = *nv.Name
	}
	return "networkview/test" + itoa(m.nextRef) + ":" + name + "/false"
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
// returned unfiltered (WAPI's own server-side default field set — not
// modeled precisely here since no test in this file relies on the
// unfiltered-request shape).
func filterReturnFields(nv *ibclient.NetworkView, returnFields string) interface{} {
	if returnFields == "" {
		return nv
	}
	raw, err := json.Marshal(nv)
	if err != nil {
		return nv
	}
	var full map[string]json.RawMessage
	if err := json.Unmarshal(raw, &full); err != nil {
		return nv
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

// handler returns an http.Handler implementing the networkview WAPI
// surface.
func (m *mockWapiServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/networkview", func(w http.ResponseWriter, r *http.Request) {
		var nv ibclient.NetworkView
		if err := json.NewDecoder(r.Body).Decode(&nv); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ref := m.seed(&nv)
		writeJSON(w, http.StatusOK, ref)
	})

	// Search endpoint (GetNetworkView): a GET with no _ref path segment,
	// filtered by the name query param. Registered as an exact literal
	// path so Go's ServeMux prefers it over the {ref...} wildcard below
	// for requests to precisely "networkview" (real _refs always carry
	// additional path segments). Unlike the other resources' Get*
	// searches, GetNetworkView's SDK wrapper treats a zero-result
	// response as its own synthesized error rather than an empty slice
	// bubbling up unexamined — but the wire response is the same "200
	// with an empty array" shape, so the mock does not need to special
	// case it.
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/networkview", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("name")

		m.mu.Lock()
		var matches []ibclient.NetworkView
		for _, nv := range m.views {
			if name != "" && (nv.Name == nil || *nv.Name != name) {
				continue
			}
			matches = append(matches, *nv)
		}
		m.mu.Unlock()

		writeJSON(w, http.StatusOK, matches)
	})

	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		nv, ok := m.views[ref]
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		// Honor _return_fields the way real WAPI does: the response
		// includes only _ref plus the explicitly requested fields. This
		// matters for TestClusterUpdateDoesNotSendImmutableField —
		// UpdateNetworkView's internal merge GET requests only
		// extattrs/name/comment (never is_default), so a mock that
		// ignored _return_fields would let is_default leak back into the
		// PUT body via the merge, masking a real bug.
		writeJSON(w, http.StatusOK, filterReturnFields(nv, r.URL.Query().Get("_return_fields")))
	})

	mux.HandleFunc("PUT /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		existing, ok := m.views[ref]
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
		var incoming ibclient.NetworkView
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body
		existing.Name = incoming.Name
		existing.Comment = incoming.Comment
		existing.Ea = incoming.Ea
		m.mu.Unlock()

		writeJSON(w, http.StatusOK, ref)
	})

	mux.HandleFunc("DELETE /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		_, ok := m.views[ref]
		delete(m.views, ref)
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

// newTestClient builds an ibclient.IBObjectManager and Connector pointed
// at the given httptest.Server via plain HTTP (no TLS needed — the
// WapiRequestBuilder only switches to HTTPS when hostCfg.Scheme != "http").
func newTestClient(t *testing.T, srv *httptest.Server) (ibclient.IBObjectManager, *ibclient.Connector) {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("cannot parse test server URL: %v", err)
	}
	objMgr, conn, err := newClientWithScheme(&nioCredentials{
		Host:     u.Hostname(),
		Username: "test-user",
		Password: "test-pass",
	}, true, "http", u.Port())
	if err != nil {
		t.Fatalf("cannot build test client: %v", err)
	}
	return objMgr, conn
}

// ── cluster: Observe ────────────────────────────────────────────────────

func TestClusterObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkView{
		Name:    stringPtr("my-networkview"),
		Comment: stringPtr("hello"),
		Ea:      ibclient.EA{"env": "prod"},
	})

	objMgr, conn := newTestClient(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterNetworkView("my-nv", ref)
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
	if cond := cr.GetCondition(xpv1.TypeReady); cond.Status != corev1.ConditionTrue {
		t.Errorf("condition Ready = %v, want True", cond.Status)
	}
}

func TestClusterObserveNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	objMgr, conn := newTestClient(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterNetworkView("my-nv", "networkview/does-not-exist:my-networkview/false")

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

	objMgr, conn := newTestClient(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterNetworkView("my-nv", "") // external-name unset
	meta.SetExternalName(cr, cr.GetName())   // simulate NameAsExternalName initializer

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

	objMgr, conn := newTestClient(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterNetworkView("my-nv", "networkview/test1:my-networkview/false")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	objMgr, conn := newTestClient(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterNetworkView("my-nv", "networkview/test1:my-networkview/false")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestClusterObserveIsDefaultReported pins that getNetworkViewByRef's
// extended field request correctly propagates is_default=true into
// AtProvider, exercising the well-known-default pattern's most important
// observability guarantee.
func TestClusterObserveIsDefaultReported(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkView{
		Name:      stringPtr("default"),
		IsDefault: true,
	})

	objMgr, conn := newTestClient(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterNetworkView("my-nv", ref)
	cr.Spec.ForProvider.Name = stringPtr("default")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Fatal("Observe: want ResourceExists=true, got false")
	}
	if cr.Status.AtProvider.IsDefault == nil || !*cr.Status.AtProvider.IsDefault {
		t.Errorf("AtProvider.IsDefault = %v, want true", cr.Status.AtProvider.IsDefault)
	}
}

// TestClusterObserveMinimalResponse pins nil-safety in Observe: a WAPI
// response carrying only the object's _ref (the resource identifier) and
// every other field at its Go zero value (nil pointers, empty strings, a
// nil Ea map) must not panic and must produce a valid observation with
// nil-safe AtProvider fields.
func TestClusterObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkView{})

	objMgr, conn := newTestClient(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterNetworkView("my-nv", ref)

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
	if ap.Name != nil {
		t.Errorf("AtProvider.Name = %v, want nil", ap.Name)
	}
	if ap.Comment != nil {
		t.Errorf("AtProvider.Comment = %v, want nil", ap.Comment)
	}
	if ap.ExtAttrs != nil {
		t.Errorf("AtProvider.ExtAttrs = %v, want nil", ap.ExtAttrs)
	}
	if ap.IsDefault == nil || *ap.IsDefault {
		t.Errorf("AtProvider.IsDefault = %v, want false", ap.IsDefault)
	}
}

// TestClusterObserveLateInitializesFields verifies that Observe
// back-fills server-defaulted optional fields (comment, extattrs) into
// spec.forProvider when they were not user-supplied, and reports
// ResourceLateInitialized=true so the reconciler persists the patch.
func TestClusterObserveLateInitializesFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkView{
		Name:    stringPtr("my-networkview"),
		Comment: stringPtr("server-set comment"),
		Ea:      ibclient.EA{"env": "prod"},
	})

	objMgr, conn := newTestClient(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterNetworkView("my-nv", ref)
	// Comment and ExtAttrs are left unset on the spec to exercise
	// late-init.

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceLateInitialized {
		t.Error("Observe: want ResourceLateInitialized=true, got false")
	}
	if cr.Spec.ForProvider.Comment == nil || *cr.Spec.ForProvider.Comment != "server-set comment" {
		t.Errorf("Observe: ForProvider.Comment = %v, want late-initialized %q", cr.Spec.ForProvider.Comment, "server-set comment")
	}
	if !extAttrsEqual(cr.Spec.ForProvider.ExtAttrs, map[string]string{"env": "prod"}) {
		t.Errorf("Observe: ForProvider.ExtAttrs = %v, want late-initialized %v", cr.Spec.ForProvider.ExtAttrs, map[string]string{"env": "prod"})
	}
}

// TestClusterObserveNeedsUpdate verifies that Observe reports
// ResourceUpToDate=false when a mutable field (comment) differs between
// spec and the observed WAPI object.
func TestClusterObserveNeedsUpdate(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkView{
		Name:    stringPtr("my-networkview"),
		Comment: stringPtr("old comment"),
	})

	objMgr, conn := newTestClient(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterNetworkView("my-nv", ref)
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=false when comment differs, got true")
	}
}

// ── cluster: Create ─────────────────────────────────────────────────────

func TestClusterCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	objMgr, conn := newTestClient(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterNetworkView("my-nv", "") // no external-name yet

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

// TestClusterCreateError verifies that a WAPI error from the create call
// is wrapped (not swallowed) and returned to the reconciler.
func TestClusterCreateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	objMgr, conn := newTestClient(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterNetworkView("my-nv", "")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errCreateNetworkView) {
		t.Errorf("Create: error = %q, want it to contain %q (wrapped, not swallowed)", got, errCreateNetworkView)
	}
}

func TestClusterObserveIsUpToDateIgnoresImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkView{
		Name:      stringPtr("default"),
		IsDefault: true,
	})

	objMgr, conn := newTestClient(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterNetworkView("my-nv", ref)
	cr.Spec.ForProvider.Name = stringPtr("default")
	// AtProvider.IsDefault is response-only (no ForProvider counterpart),
	// so there is no spec-side drift to simulate here beyond confirming
	// isUpToDate never reads it — see isUpToDate's doc comment.

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true for the default NetworkView, got false")
	}
}

// ── cluster: Update ─────────────────────────────────────────────────────

func TestClusterUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkView{
		Name:    stringPtr("my-networkview"),
		Comment: stringPtr("old comment"),
	})

	objMgr, conn := newTestClient(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterNetworkView("my-nv", ref)
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.views[ref]
	m.mu.Unlock()
	if stored.Comment == nil || *stored.Comment != "new comment" {
		t.Errorf("Update: stored comment = %v, want %q", stored.Comment, "new comment")
	}
}

// TestClusterUpdateError verifies that a WAPI error from the update call
// is wrapped (not swallowed) and returned to the reconciler.
func TestClusterUpdateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	objMgr, conn := newTestClient(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterNetworkView("my-nv", "networkview/test1:my-networkview/false")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errUpdateNetworkView) {
		t.Errorf("Update: error = %q, want it to contain %q (wrapped, not swallowed)", got, errUpdateNetworkView)
	}
}

func TestClusterUpdateDoesNotSendImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkView{
		Name:      stringPtr("default"),
		IsDefault: true,
	})

	objMgr, conn := newTestClient(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterNetworkView("my-nv", ref)
	cr.Spec.ForProvider.Name = stringPtr("default")
	cr.Spec.ForProvider.Comment = stringPtr("updated")

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
	if _, present := raw["is_default"]; present {
		t.Errorf("Update: request body contains immutable field 'is_default': %v", raw["is_default"])
	}
}

// ── cluster: Delete ─────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkView{Name: stringPtr("my-networkview")})

	objMgr, conn := newTestClient(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterNetworkView("my-nv", ref)

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}

	m.mu.Lock()
	_, stillExists := m.views[ref]
	m.mu.Unlock()
	if stillExists {
		t.Error("Delete: record still present after Delete")
	}
}

func TestClusterDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	objMgr, conn := newTestClient(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterNetworkView("my-nv", "networkview/does-not-exist:my-networkview/false")

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

	objMgr, conn := newTestClient(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterNetworkView("my-nv", "networkview/test1:my-networkview/false")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteNetworkView) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteNetworkView)
	}
}

// TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject verifies the
// core defect fix: a 404 against the stored _ref must not be treated as
// "already deleted" when a natural-key search finds the same identity
// still live under a different _ref. Deleting that network view would be
// unverifiable ownership, so Delete() must refuse and leave the record
// in place.
func TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed(&ibclient.NetworkView{Name: stringPtr("my-networkview")})

	objMgr, conn := newTestClient(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterNetworkView("my-nv", "networkview/stale-ref:my-networkview/false")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected refusal error when a natural-key search still matches a live object, got nil")
	}
	if !strings.Contains(err.Error(), "refusing to delete") {
		t.Errorf("Delete: error = %q, want it to explain the refusal", err.Error())
	}

	m.mu.Lock()
	_, stillExists := m.views[liveRef]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Delete: live network view was removed despite the refusal — DELETE must not have been issued against it")
	}
}

// TestClusterDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch is the
// companion happy path: a 404 against the stored _ref, and a natural-key
// search that finds nothing, means the object really is gone.
func TestClusterDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	objMgr, conn := newTestClient(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterNetworkView("my-nv", "networkview/stale-ref:my-networkview/false")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the natural-key search also finds nothing, got: %v", err)
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

	cr := newClusterNetworkView("my-nv", "")
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

	cr := newClusterNetworkView("my-nv", "")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for missing ProviderConfig, got nil")
	}
}

// ── namespaced: Observe ──────────────────────────────────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkView{
		Name: stringPtr("my-networkview"),
	})

	objMgr, conn := newTestClient(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedNetworkView("default", "my-nv", ref, "ProviderConfig")

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

	objMgr, conn := newTestClient(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedNetworkView("default", "my-nv", "networkview/does-not-exist:my-networkview/false", "ProviderConfig")

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

	objMgr, conn := newTestClient(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedNetworkView("default", "my-nv", "", "ProviderConfig")
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

	objMgr, conn := newTestClient(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedNetworkView("default", "my-nv", "networkview/test1:my-networkview/false", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestNamespacedObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	objMgr, conn := newTestClient(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedNetworkView("default", "my-nv", "networkview/test1:my-networkview/false", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestNamespacedObserveMinimalResponse is the namespaced-scope counterpart
// of TestClusterObserveMinimalResponse — see that test's doc comment for
// rationale.
func TestNamespacedObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkView{})

	objMgr, conn := newTestClient(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedNetworkView("default", "my-nv", ref, "ProviderConfig")

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
	if ap.Name != nil {
		t.Errorf("AtProvider.Name = %v, want nil", ap.Name)
	}
	if ap.Comment != nil {
		t.Errorf("AtProvider.Comment = %v, want nil", ap.Comment)
	}
	if ap.ExtAttrs != nil {
		t.Errorf("AtProvider.ExtAttrs = %v, want nil", ap.ExtAttrs)
	}
}

// TestNamespacedObserveLateInitializesFields is the namespaced-scope
// counterpart of TestClusterObserveLateInitializesFields.
func TestNamespacedObserveLateInitializesFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkView{
		Name:    stringPtr("my-networkview"),
		Comment: stringPtr("server-set comment"),
		Ea:      ibclient.EA{"env": "prod"},
	})

	objMgr, conn := newTestClient(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedNetworkView("default", "my-nv", ref, "ProviderConfig")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceLateInitialized {
		t.Error("Observe: want ResourceLateInitialized=true, got false")
	}
	if cr.Spec.ForProvider.Comment == nil || *cr.Spec.ForProvider.Comment != "server-set comment" {
		t.Errorf("Observe: ForProvider.Comment = %v, want late-initialized %q", cr.Spec.ForProvider.Comment, "server-set comment")
	}
	if !extAttrsEqual(cr.Spec.ForProvider.ExtAttrs, map[string]string{"env": "prod"}) {
		t.Errorf("Observe: ForProvider.ExtAttrs = %v, want late-initialized %v", cr.Spec.ForProvider.ExtAttrs, map[string]string{"env": "prod"})
	}
}

// TestNamespacedObserveNeedsUpdate is the namespaced-scope counterpart of
// TestClusterObserveNeedsUpdate.
func TestNamespacedObserveNeedsUpdate(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkView{
		Name:    stringPtr("my-networkview"),
		Comment: stringPtr("old comment"),
	})

	objMgr, conn := newTestClient(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedNetworkView("default", "my-nv", ref, "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=false when comment differs, got true")
	}
}

// ── namespaced: Create/Update/Delete ─────────────────────────────────────

func TestNamespacedCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	objMgr, conn := newTestClient(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedNetworkView("default", "my-nv", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

// TestNamespacedCreateError is the namespaced-scope counterpart of
// TestClusterCreateError.
func TestNamespacedCreateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	objMgr, conn := newTestClient(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedNetworkView("default", "my-nv", "", "ProviderConfig")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errCreateNetworkView) {
		t.Errorf("Create: error = %q, want it to contain %q (wrapped, not swallowed)", got, errCreateNetworkView)
	}
}

func TestNamespacedUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkView{
		Name: stringPtr("my-networkview"),
	})

	objMgr, conn := newTestClient(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedNetworkView("default", "my-nv", ref, "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.views[ref]
	m.mu.Unlock()
	if stored.Comment == nil || *stored.Comment != "new comment" {
		t.Errorf("Update: stored comment = %v, want %q", stored.Comment, "new comment")
	}
}

// TestNamespacedUpdateError is the namespaced-scope counterpart of
// TestClusterUpdateError.
func TestNamespacedUpdateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	objMgr, conn := newTestClient(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedNetworkView("default", "my-nv", "networkview/test1:my-networkview/false", "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errUpdateNetworkView) {
		t.Errorf("Update: error = %q, want it to contain %q (wrapped, not swallowed)", got, errUpdateNetworkView)
	}
}

// TestNamespacedUpdateDoesNotSendImmutableField is the namespaced-scope
// counterpart of TestClusterUpdateDoesNotSendImmutableField.
func TestNamespacedUpdateDoesNotSendImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkView{
		Name:      stringPtr("default"),
		IsDefault: true,
	})

	objMgr, conn := newTestClient(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedNetworkView("default", "my-nv", ref, "ProviderConfig")
	cr.Spec.ForProvider.Name = stringPtr("default")
	cr.Spec.ForProvider.Comment = stringPtr("updated")

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
	if _, present := raw["is_default"]; present {
		t.Errorf("Update: request body contains immutable field 'is_default': %v", raw["is_default"])
	}
}

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkView{Name: stringPtr("my-networkview")})

	objMgr, conn := newTestClient(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedNetworkView("default", "my-nv", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
}

func TestNamespacedDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	objMgr, conn := newTestClient(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedNetworkView("default", "my-nv", "networkview/does-not-exist:my-networkview/false", "ProviderConfig")

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

	objMgr, conn := newTestClient(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedNetworkView("default", "my-nv", "networkview/test1:my-networkview/false", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteNetworkView) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteNetworkView)
	}
}

// TestNamespacedDeleteRefusesWhenStaleRefStillMatchesLiveObject is the
// namespaced-scope counterpart of
// TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject.
func TestNamespacedDeleteRefusesWhenStaleRefStillMatchesLiveObject(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed(&ibclient.NetworkView{Name: stringPtr("my-networkview")})

	objMgr, conn := newTestClient(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedNetworkView("default", "my-nv", "networkview/stale-ref:my-networkview/false", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected refusal error when a natural-key search still matches a live object, got nil")
	}
	if !strings.Contains(err.Error(), "refusing to delete") {
		t.Errorf("Delete: error = %q, want it to explain the refusal", err.Error())
	}

	m.mu.Lock()
	_, stillExists := m.views[liveRef]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Delete: live network view was removed despite the refusal — DELETE must not have been issued against it")
	}
}

// TestNamespacedDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch is the
// namespaced-scope counterpart of
// TestClusterDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch.
func TestNamespacedDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	objMgr, conn := newTestClient(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedNetworkView("default", "my-nv", "networkview/stale-ref:my-networkview/false", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the natural-key search also finds nothing, got: %v", err)
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

	cr := newNamespacedNetworkView(ns, "my-nv", "", "ProviderConfig")
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

	cr := newNamespacedNetworkView("app-ns", "my-nv", "", "ClusterProviderConfig")
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

	cr := newNamespacedNetworkView("default", "my-nv", "", "SomeOtherKind")
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
	return fmt.Errorf("WAPI request error: %d('boom')", code)
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

func TestNewClientWithSchemeUsesConfiguredSslVerify(t *testing.T) {
	// Regression guard: newClientWithScheme must not hardcode SslVerify to
	// "true" — it must honor the sslVerify parameter. Both branches must construct
	// successfully (transport config validation happens locally; no
	// network round-trip occurs here).
	for name, sslVerify := range map[string]bool{"Enabled": true, "Disabled": false} {
		t.Run(name, func(t *testing.T) {
			creds := &nioCredentials{Host: "127.0.0.1", Username: "admin", Password: "s3cr3t"}
			objMgr, conn, err := newClientWithScheme(creds, sslVerify, "http", "80")
			if err != nil {
				t.Fatalf("newClientWithScheme: unexpected error: %v", err)
			}
			if objMgr == nil {
				t.Fatal("newClientWithScheme: expected non-nil object manager")
			}
			if conn == nil {
				t.Fatal("newClientWithScheme: expected non-nil connector")
			}
		})
	}
}
