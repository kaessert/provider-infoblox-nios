// Package dnsview unit tests for the DNSView MR controllers. Tests use
// inline httptest.NewServer mocks that emulate the WAPI view endpoints,
// PascalCase test names (no underscores), and white-box access to the
// unexported connectors/clients so both scopes can be exercised without
// going through the full Connect() credential bridge on every test.
package dnsview

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	xpv2 "github.com/crossplane/crossplane-runtime/v2/apis/common/v2"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/dnsview/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/dnsview/v1alpha1"
	namespacedpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
)

// ── generic helpers ─────────────────────────────────────────────────────────

func stringPtr(s string) *string { return &s }
func boolPtr(b bool) *bool       { return &b }
func int64Ptr(i int64) *int64    { return &i }

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

// newClusterDNSView builds a minimal cluster-scoped DNSView CR. When
// externalName is empty, the external-name annotation is left unset. When
// it equals crName it simulates the framework's NameAsExternalName
// initializer (the pre-create state); any other value simulates a
// Create()-assigned server ref.
func newClusterDNSView(crName, externalName string) *clusterv1alpha1.DNSView {
	cr := &clusterv1alpha1.DNSView{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: "test-uid-cluster"},
		Spec: clusterv1alpha1.DNSViewSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.DNSViewParameters{
				Name: stringPtr("my-view"),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newNamespacedDNSView is the namespaced variant of newClusterDNSView.
func newNamespacedDNSView(ns, crName, externalName, pcKind string) *namespacedv1alpha1.DNSView {
	cr := &namespacedv1alpha1.DNSView{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: "test-uid-namespaced"},
		Spec: namespacedv1alpha1.DNSViewSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: "default"},
			},
			ForProvider: namespacedv1alpha1.DNSViewParameters{
				Name: stringPtr("my-view"),
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
// mockWapiServer emulates the subset of NIOS WAPI view endpoints exercised
// by the DNSView controller (POST create, GET/PUT/DELETE by _ref). Records
// are marshaled/unmarshaled using the real ibclient.View type so the wire
// format (including the EA {"value": ...} envelope) exactly matches what
// the SDK sends and expects.

type mockWapiServer struct {
	mu      sync.Mutex
	records map[string]*ibclient.View
	nextRef int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert immutable fields are omitted.
	lastUpdateBody []byte
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{records: map[string]*ibclient.View{}}
}

func (m *mockWapiServer) seed(rec *ibclient.View) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRef++
	if rec.Ref == "" {
		rec.Ref = m.newRefLocked(rec)
	}
	m.records[rec.Ref] = rec
	return rec.Ref
}

func (m *mockWapiServer) newRefLocked(rec *ibclient.View) string {
	name := ""
	if rec.Name != nil {
		name = *rec.Name
	}
	return "view/test" + itoa(m.nextRef) + ":" + name + "/" + boolStr(rec.IsDefault)
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
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

// handler returns an http.Handler implementing the view WAPI surface.
func (m *mockWapiServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/view", func(w http.ResponseWriter, r *http.Request) {
		var rec ibclient.View
		if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ref := m.seed(&rec)
		writeJSON(w, http.StatusOK, ref)
	})

	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		rec, ok := m.records[ref]
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, rec)
	})

	mux.HandleFunc("PUT /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		existing, ok := m.records[ref]
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
		var incoming ibclient.View
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body
		// Only mutable fields are ever applied. is_default is read-only
		// (supports=sr) and is never accepted on a PUT — mirroring WAPI's
		// rejection of changes to that field.
		newRef := existing.Ref
		if incoming.Name != nil && (existing.Name == nil || *existing.Name != *incoming.Name) {
			existing.Name = incoming.Name
			newRef = "view/test" + itoa(m.nextRef) + ":" + *incoming.Name + "/" + boolStr(existing.IsDefault)
		}
		existing.Comment = incoming.Comment
		existing.NetworkView = incoming.NetworkView
		existing.Disable = incoming.Disable
		existing.Ea = incoming.Ea
		existing.CustomRootNameServers = incoming.CustomRootNameServers
		if newRef != existing.Ref {
			delete(m.records, existing.Ref)
			existing.Ref = newRef
			m.records[newRef] = existing
		}
		m.mu.Unlock()

		writeJSON(w, http.StatusOK, existing.Ref)
	})

	mux.HandleFunc("DELETE /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		_, ok := m.records[ref]
		delete(m.records, ref)
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

// newTestConnector builds an ibclient.IBConnector pointed at the given
// httptest.Server via plain HTTP (no TLS needed — the WapiRequestBuilder
// only switches to HTTPS when hostCfg.Scheme != "http").
func newTestConnector(t *testing.T, srv *httptest.Server) ibclient.IBConnector {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("cannot parse test server URL: %v", err)
	}
	conn, err := newConnectorWithScheme(&nioCredentials{
		Host:     u.Hostname(),
		Username: "test-user",
		Password: "test-pass",
	}, "http", u.Port())
	if err != nil {
		t.Fatalf("cannot build test connector: %v", err)
	}
	return conn
}

// ── cluster: Observe ────────────────────────────────────────────────────

func TestClusterObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{
		Name:    stringPtr("my-view"),
		Comment: stringPtr("hello"),
		Disable: boolPtr(false),
		Ea:      ibclient.EA{"env": "prod"},
	})

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)
	cr.Spec.ForProvider.Comment = stringPtr("hello")
	cr.Spec.ForProvider.Disable = boolPtr(false)
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
	if cr.Status.AtProvider.Name == nil || *cr.Status.AtProvider.Name != "my-view" {
		t.Errorf("AtProvider.Name = %v, want my-view", cr.Status.AtProvider.Name)
	}
	if cond := cr.GetCondition(xpv1.TypeReady); cond.Status != corev1.ConditionTrue {
		t.Errorf("condition Ready = %v, want True", cond.Status)
	}
}

func TestClusterObserveNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", "view/does-not-exist:my-view/false")

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

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", "") // external-name unset
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

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", "view/test1:my-view/false")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", "view/test1:my-view/false")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestClusterObserveMinimalResponse pins nil-safety in Observe: a WAPI
// response carrying only the object's _ref/name and every other field at
// its Go zero value (nil pointers, empty strings, a nil Ea map, nil
// slices) must not panic and must produce a valid observation with
// nil-safe AtProvider fields.
func TestClusterObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{Name: stringPtr("my-view")})

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)

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
	if ap.Comment != nil {
		t.Errorf("AtProvider.Comment = %v, want nil", ap.Comment)
	}
	if ap.ExtAttrs != nil {
		t.Errorf("AtProvider.ExtAttrs = %v, want nil", ap.ExtAttrs)
	}
	if ap.CustomRootNameServers != nil {
		t.Errorf("AtProvider.CustomRootNameServers = %v, want nil", ap.CustomRootNameServers)
	}
	if ap.ScavengingSettings != nil {
		t.Errorf("AtProvider.ScavengingSettings = %v, want nil", ap.ScavengingSettings)
	}
}

// ── cluster: is_default (immutable field) ────────────────────────────────

// TestIsUpToDateIgnoresIsDefault verifies is_default (read-only, no
// ForProvider representation) is excluded from the mutable-field
// comparison — a view whose spec matches every mutable field must report
// ResourceUpToDate=true regardless of its observed is_default value.
func TestIsUpToDateIgnoresIsDefault(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{
		Name:      stringPtr("default"),
		IsDefault: true,
	})

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)
	cr.Spec.ForProvider.Name = stringPtr("default")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true (is_default excluded from comparison), got false")
	}
	if cr.Status.AtProvider.IsDefault == nil || !*cr.Status.AtProvider.IsDefault {
		t.Errorf("AtProvider.IsDefault = %v, want true (still mirrored in status)", cr.Status.AtProvider.IsDefault)
	}
}

// TestUpdateDoesNotSendImmutableField asserts the PUT body never carries
// is_default — it has no ForProvider field at all, so buildView can never
// emit it, but this pins that guarantee against regression.
func TestUpdateDoesNotSendImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{Name: stringPtr("default"), IsDefault: true})

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)
	cr.Spec.ForProvider.Name = stringPtr("default")
	cr.Spec.ForProvider.Comment = stringPtr("updated")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	body := m.lastUpdateBody
	m.mu.Unlock()

	var sent map[string]interface{}
	if err := json.Unmarshal(body, &sent); err != nil {
		t.Fatalf("cannot unmarshal PUT body: %v", err)
	}
	if _, present := sent["is_default"]; present {
		t.Errorf("Update: PUT body contains immutable field is_default, want absent. body=%s", body)
	}
}

// ── cluster: Create ─────────────────────────────────────────────────────

func TestClusterCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", "") // no external-name yet

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

func TestClusterCreateCapturesServerAssignedRef(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", "")
	cr.Spec.ForProvider.Comment = stringPtr("created by test")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	ref := meta.GetExternalName(cr)
	m.mu.Lock()
	rec, ok := m.records[ref]
	m.mu.Unlock()
	if !ok {
		t.Fatalf("Create: seeded record not found for ref %q", ref)
	}
	if rec.Name == nil || *rec.Name != "my-view" {
		t.Errorf("Create: Name = %v, want my-view", rec.Name)
	}
	if rec.Comment == nil || *rec.Comment != "created by test" {
		t.Errorf("Create: Comment = %v, want 'created by test'", rec.Comment)
	}
}

// ── cluster: Update ─────────────────────────────────────────────────────

func TestClusterUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{
		Name:    stringPtr("my-view"),
		Comment: stringPtr("old comment"),
	})

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	rec := m.records[ref]
	m.mu.Unlock()
	if rec.Comment == nil || *rec.Comment != "new comment" {
		t.Errorf("Update: Comment = %v, want 'new comment'", rec.Comment)
	}
}

// TestClusterUpdateRefChangesOnRename pins the _ref-unstable behavior: PUT
// a name change and confirm the controller re-reads the new _ref from the
// response and refreshes the external-name annotation.
func TestClusterUpdateRefChangesOnRename(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{Name: stringPtr("old-name")})

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)
	cr.Spec.ForProvider.Name = stringPtr("new-name")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == ref {
		t.Error("Update: external-name unchanged after rename, want the new _ref")
	}
	m.mu.Lock()
	_, oldStillExists := m.records[ref]
	newRec, newExists := m.records[got]
	m.mu.Unlock()
	if oldStillExists {
		t.Error("Update: old _ref still present in the WAPI after rename")
	}
	if !newExists || newRec.Name == nil || *newRec.Name != "new-name" {
		t.Errorf("Update: new ref record = %v, want Name=new-name", newRec)
	}
}

// ── cluster: Delete ─────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{Name: stringPtr("my-view")})

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}

	m.mu.Lock()
	_, stillExists := m.records[ref]
	m.mu.Unlock()
	if stillExists {
		t.Error("Delete: record still present after Delete")
	}
}

func TestClusterDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", "view/does-not-exist:my-view/false")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Errorf("Delete: expected nil error for already-deleted resource (404), got %v", err)
	}
}

// TestClusterDeleteProtectsWellKnownDefaultView verifies that a DNSView CR
// whose observed/desired name is one of the three well-known views
// (default/External/Internal) never issues a WAPI DELETE — protecting the
// live Grid Manager from a well-known view being wiped out by an
// accidental `kubectl delete`.
func TestClusterDeleteProtectsWellKnownDefaultView(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{Name: stringPtr("default"), IsDefault: true})

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)
	cr.Spec.ForProvider.Name = stringPtr("default")
	cr.Status.AtProvider.Name = stringPtr("default")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}

	m.mu.Lock()
	_, stillExists := m.records[ref]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Delete: well-known default view was deleted from the WAPI, want protected (still present)")
	}
}

func TestClusterDeleteProtectsExternalAndInternal(t *testing.T) {
	for _, name := range []string{"External", "Internal"} {
		name := name
		t.Run(name, func(t *testing.T) {
			m := newMockWapiServer()
			srv := httptest.NewServer(m.handler())
			defer srv.Close()

			ref := m.seed(&ibclient.View{Name: stringPtr(name)})

			e := &clusterExternal{conn: newTestConnector(t, srv)}
			cr := newClusterDNSView("my-dnsview", ref)
			cr.Spec.ForProvider.Name = stringPtr(name)
			cr.Status.AtProvider.Name = stringPtr(name)

			if _, err := e.Delete(context.Background(), cr); err != nil {
				t.Fatalf("Delete: unexpected error: %v", err)
			}

			m.mu.Lock()
			_, stillExists := m.records[ref]
			m.mu.Unlock()
			if !stillExists {
				t.Errorf("Delete: well-known view %q was deleted from the WAPI, want protected", name)
			}
		})
	}
}

// ── cluster: Connect ──────────────────────────────────────────────────────

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

	cr := newClusterDNSView("my-dnsview", "")
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

	cr := newClusterDNSView("my-dnsview", "")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for missing ProviderConfig, got nil")
	}
}

func TestClusterDisconnectIsNoop(t *testing.T) {
	e := &clusterExternal{}
	if err := e.Disconnect(context.Background()); err != nil {
		t.Errorf("Disconnect: unexpected error: %v", err)
	}
}

// ── namespaced: Observe / Create / Update / Delete ───────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{Name: stringPtr("my-view")})

	e := &namespacedExternal{conn: newTestConnector(t, srv)}
	cr := newNamespacedDNSView("default", "my-dnsview", ref, "ProviderConfig")

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

	e := &namespacedExternal{conn: newTestConnector(t, srv)}
	cr := newNamespacedDNSView("default", "my-dnsview", "view/does-not-exist:my-view/false", "ProviderConfig")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false for 404, got true")
	}
}

func TestNamespacedCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{conn: newTestConnector(t, srv)}
	cr := newNamespacedDNSView("default", "my-dnsview", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}
	if got := meta.GetExternalName(cr); got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

func TestNamespacedUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{Name: stringPtr("my-view"), Comment: stringPtr("old")})

	e := &namespacedExternal{conn: newTestConnector(t, srv)}
	cr := newNamespacedDNSView("default", "my-dnsview", ref, "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("new")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	rec := m.records[ref]
	m.mu.Unlock()
	if rec.Comment == nil || *rec.Comment != "new" {
		t.Errorf("Update: Comment = %v, want 'new'", rec.Comment)
	}
}

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{Name: stringPtr("my-view")})

	e := &namespacedExternal{conn: newTestConnector(t, srv)}
	cr := newNamespacedDNSView("default", "my-dnsview", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
	m.mu.Lock()
	_, stillExists := m.records[ref]
	m.mu.Unlock()
	if stillExists {
		t.Error("Delete: record still present after Delete")
	}
}

func TestNamespacedDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{conn: newTestConnector(t, srv)}
	cr := newNamespacedDNSView("default", "my-dnsview", "view/does-not-exist:my-view/false", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Errorf("Delete: expected nil error for already-deleted resource (404), got %v", err)
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

	cr := newNamespacedDNSView(ns, "my-dnsview", "", "ProviderConfig")
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

	cr := newNamespacedDNSView("app-ns", "my-dnsview", "", "ClusterProviderConfig")
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

	cr := newNamespacedDNSView("default", "my-dnsview", "", "SomeOtherKind")
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

func TestIsNotFoundClassifiesFormattedStatus(t *testing.T) {
	err := &formattedWapiError{msg: "WAPI request error: 404('Not Found')\nContents:\n{}\n"}
	if !isNotFound(err) {
		t.Error("isNotFound(formatted 404) = false, want true")
	}
}

func TestIsNotFoundFalseForNil(t *testing.T) {
	if isNotFound(nil) {
		t.Error("isNotFound(nil) = true, want false")
	}
}

// formattedWapiError mimics the plain-string error the SDK's
// getHTTPResponseError constructs for non-404 statuses.
type formattedWapiError struct{ msg string }

func (e *formattedWapiError) Error() string { return e.msg }

func TestIsWellKnownDNSViewName(t *testing.T) {
	cases := []struct {
		name *string
		want bool
	}{
		{stringPtr("default"), true},
		{stringPtr("External"), true},
		{stringPtr("Internal"), true},
		{stringPtr("custom-view"), false},
		{nil, false},
	}
	for _, c := range cases {
		if got := isWellKnownDNSViewName(c.name); got != c.want {
			t.Errorf("isWellKnownDNSViewName(%v) = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestNameServerValuesRoundTrip(t *testing.T) {
	in := []nameServerValue{{
		Address:                      stringPtr("10.0.0.1"),
		Name:                         stringPtr("ns1.example.com"),
		SharedWithMsParentDelegation: boolPtr(true),
		Stealth:                      boolPtr(false),
		TsigKey:                      stringPtr("key"),
		TsigKeyAlg:                   stringPtr("hmac-sha256"),
		TsigKeyName:                  stringPtr("keyname"),
		UseTsigKeyName:               boolPtr(true),
	}}
	sdk := nameServerValuesToSDK(in)
	back := nameServerValuesFromSDK(sdk)
	if len(back) != 1 || *back[0].Address != *in[0].Address || *back[0].Name != *in[0].Name {
		t.Errorf("NameServer round-trip mismatch: got %+v, want %+v", back, in)
	}
}

func TestIsUpToDateExtAttrsMismatch(t *testing.T) {
	desired := dnsViewFields{Name: stringPtr("v"), ExtAttrs: map[string]string{"env": "prod"}}
	observed := dnsViewFields{Name: stringPtr("v"), ExtAttrs: map[string]string{"env": "dev"}}
	if isUpToDate(desired, observed) {
		t.Error("isUpToDate: want false on ExtAttrs mismatch, got true")
	}
}

func TestIsUpToDateNestedListMismatch(t *testing.T) {
	desired := dnsViewFields{Name: stringPtr("v"), Forwarders: []string{"8.8.8.8"}}
	observed := dnsViewFields{Name: stringPtr("v"), Forwarders: []string{"1.1.1.1"}}
	if isUpToDate(desired, observed) {
		t.Error("isUpToDate: want false on Forwarders mismatch, got true")
	}
}

func TestLateInitializeBackfillsServerDefaults(t *testing.T) {
	desired := dnsViewFields{Name: stringPtr("v")}
	observed := dnsViewFields{
		Name:        stringPtr("v"),
		Comment:     stringPtr("server default"),
		NetworkView: stringPtr("default"),
		LameTTL:     int64Ptr(600),
	}

	got, changed := lateInitializeFields(desired, observed)
	if !changed {
		t.Error("lateInitializeFields: want changed=true, got false")
	}
	if got.Comment == nil || *got.Comment != "server default" {
		t.Errorf("Comment = %v, want 'server default'", got.Comment)
	}
	if got.NetworkView == nil || *got.NetworkView != "default" {
		t.Errorf("NetworkView = %v, want 'default'", got.NetworkView)
	}
	if got.LameTTL == nil || *got.LameTTL != 600 {
		t.Errorf("LameTTL = %v, want 600", got.LameTTL)
	}
}

func TestLateInitializeDoesNotOverwriteSetFields(t *testing.T) {
	desired := dnsViewFields{Name: stringPtr("v"), Comment: stringPtr("user set")}
	observed := dnsViewFields{Name: stringPtr("v"), Comment: stringPtr("server value")}

	got, changed := lateInitializeFields(desired, observed)
	if changed {
		t.Error("lateInitializeFields: want changed=false when nothing to back-fill for Comment, got true")
	}
	if got.Comment == nil || *got.Comment != "user set" {
		t.Errorf("Comment = %v, want 'user set' (must not be overwritten)", got.Comment)
	}
}

// ── full-field-mirror coverage (nested lists/singles) ────────────────────

// TestClusterObserveFullFieldMirror seeds a WAPI response exercising every
// nested list/single field (custom root name servers, DNSSEC trusted
// keys, address-ACL lists, fixed-RRset-order FQDNs, response rate
// limiting, scavenging settings with a schedule and both expression
// lists, and a sortlist) and confirms Observe mirrors every one of them
// into AtProvider without panicking — the full-mirror AtProvider
// convention applied to DNSView's deepest nesting.
func TestClusterObserveFullFieldMirror(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{
		Name: stringPtr("full-view"),
		CustomRootNameServers: []ibclient.NameServer{
			{Address: "10.0.0.1", Name: "ns1.example.com"},
		},
		DnssecTrustedKeys: []*ibclient.Dnssectrustedkey{
			{Fqdn: "example.com", Algorithm: "RSASHA256", Key: "abc123"},
		},
		FilterAaaaList: []*ibclient.Addressac{
			{Address: "192.0.2.0/24", Permission: "ALLOW"},
		},
		LastQueriedAcl:    []*ibclient.Addressac{{Address: "192.0.2.1", Permission: "ALLOW"}},
		MatchClients:      []*ibclient.Addressac{{Address: "198.51.100.0/24", Permission: "ALLOW"}},
		MatchDestinations: []*ibclient.Addressac{{Address: "203.0.113.0/24", Permission: "DENY"}},
		FixedRrsetOrderFqdns: []*ibclient.GridDnsFixedrrsetorderfqdn{
			{Fqdn: "svc.example.com", RecordType: "A"},
		},
		ResponseRateLimiting: &ibclient.GridResponseratelimiting{
			EnableRrl: true, ResponsesPerSecond: 5, Window: 1, Slip: 2,
		},
		ScavengingSettings: &ibclient.SettingScavenging{
			EnableScavenging: true,
			ScavengingSchedule: &ibclient.SettingSchedule{
				Weekdays: []string{"Monday"}, TimeZone: "UTC", Frequency: "weekly", Every: 1,
			},
			ExpressionList:   []*ibclient.Expressionop{{Op: "AND", Op1: "a", Op1Type: "STRING"}},
			EaExpressionList: []*ibclient.Eaexpressionop{{Op: "AND", Op1: "b", Op1Type: "STRING"}},
		},
		Sortlist: []*ibclient.Sortlist{
			{Address: "10.0.0.0/8", MatchList: []string{"10.0.0.1"}},
		},
	})

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Fatal("Observe: want ResourceExists=true")
	}

	ap := cr.Status.AtProvider
	if len(ap.CustomRootNameServers) != 1 || ap.CustomRootNameServers[0].Address == nil || *ap.CustomRootNameServers[0].Address != "10.0.0.1" {
		t.Errorf("AtProvider.CustomRootNameServers = %+v, want one entry with Address=10.0.0.1", ap.CustomRootNameServers)
	}
	if len(ap.DnssecTrustedKeys) != 1 || ap.DnssecTrustedKeys[0].Fqdn == nil || *ap.DnssecTrustedKeys[0].Fqdn != "example.com" {
		t.Errorf("AtProvider.DnssecTrustedKeys = %+v, want one entry with Fqdn=example.com", ap.DnssecTrustedKeys)
	}
	if len(ap.FilterAaaaList) != 1 || len(ap.LastQueriedAcl) != 1 || len(ap.MatchClients) != 1 || len(ap.MatchDestinations) != 1 {
		t.Errorf("AtProvider address-ACL lists not fully mirrored: %+v", ap)
	}
	if len(ap.FixedRrsetOrderFqdns) != 1 || ap.FixedRrsetOrderFqdns[0].Fqdn == nil || *ap.FixedRrsetOrderFqdns[0].Fqdn != "svc.example.com" {
		t.Errorf("AtProvider.FixedRrsetOrderFqdns = %+v, want one entry with Fqdn=svc.example.com", ap.FixedRrsetOrderFqdns)
	}
	if ap.ResponseRateLimiting == nil || ap.ResponseRateLimiting.ResponsesPerSecond == nil || *ap.ResponseRateLimiting.ResponsesPerSecond != 5 {
		t.Errorf("AtProvider.ResponseRateLimiting = %+v, want ResponsesPerSecond=5", ap.ResponseRateLimiting)
	}
	if ap.ScavengingSettings == nil || ap.ScavengingSettings.EnableScavenging == nil || !*ap.ScavengingSettings.EnableScavenging {
		t.Fatalf("AtProvider.ScavengingSettings = %+v, want EnableScavenging=true", ap.ScavengingSettings)
	}
	if ap.ScavengingSettings.ScavengingSchedule == nil || ap.ScavengingSettings.ScavengingSchedule.TimeZone == nil || *ap.ScavengingSettings.ScavengingSchedule.TimeZone != "UTC" {
		t.Errorf("AtProvider.ScavengingSettings.ScavengingSchedule = %+v, want TimeZone=UTC", ap.ScavengingSettings.ScavengingSchedule)
	}
	if len(ap.ScavengingSettings.ExpressionList) != 1 || len(ap.ScavengingSettings.EaExpressionList) != 1 {
		t.Errorf("AtProvider.ScavengingSettings expression lists not mirrored: %+v", ap.ScavengingSettings)
	}
	if len(ap.Sortlist) != 1 || ap.Sortlist[0].Address == nil || *ap.Sortlist[0].Address != "10.0.0.0/8" {
		t.Errorf("AtProvider.Sortlist = %+v, want one entry with Address=10.0.0.0/8", ap.Sortlist)
	}

	// Round-trip: Update() must be able to re-send everything Observe just
	// populated into spec.ForProvider (via late-init) without panicking —
	// proves the Cluster<->bag<->SDK conversions compose in both
	// directions for every nested type, not just the read path above.
	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error round-tripping full field set: %v", err)
	}
}

// TestNamespacedObserveFullFieldMirror is the namespaced-scope counterpart
// of TestClusterObserveFullFieldMirror — same nested-field coverage, using
// the namespaced CRD conversion path (fieldsFromNamespacedParams /
// namespacedObservationFromFields) instead of the cluster one. Also
// exercises the SDK's epoch-seconds RecurringTime field, which the
// cluster-side test above leaves unset.
func TestNamespacedObserveFullFieldMirror(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{
		Name: stringPtr("full-view-ns"),
		CustomRootNameServers: []ibclient.NameServer{
			{Address: "10.0.0.2", Name: "ns2.example.com"},
		},
		DnssecTrustedKeys: []*ibclient.Dnssectrustedkey{
			{Fqdn: "example.org", Algorithm: "RSASHA256", Key: "def456"},
		},
		FilterAaaaList:    []*ibclient.Addressac{{Address: "192.0.2.0/24", Permission: "ALLOW"}},
		LastQueriedAcl:    []*ibclient.Addressac{{Address: "192.0.2.1", Permission: "ALLOW"}},
		MatchClients:      []*ibclient.Addressac{{Address: "198.51.100.0/24", Permission: "ALLOW"}},
		MatchDestinations: []*ibclient.Addressac{{Address: "203.0.113.0/24", Permission: "DENY"}},
		FixedRrsetOrderFqdns: []*ibclient.GridDnsFixedrrsetorderfqdn{
			{Fqdn: "svc.example.org", RecordType: "A"},
		},
		ResponseRateLimiting: &ibclient.GridResponseratelimiting{
			EnableRrl: true, ResponsesPerSecond: 10, Window: 2, Slip: 1,
		},
		ScavengingSettings: &ibclient.SettingScavenging{
			EnableScavenging: true,
			ScavengingSchedule: &ibclient.SettingSchedule{
				Weekdays:      []string{"Tuesday"},
				TimeZone:      "UTC",
				Frequency:     "weekly",
				Every:         1,
				RecurringTime: &ibclient.UnixTime{Time: time.Unix(1700000000, 0)},
			},
			ExpressionList:   []*ibclient.Expressionop{{Op: "AND", Op1: "a", Op1Type: "STRING"}},
			EaExpressionList: []*ibclient.Eaexpressionop{{Op: "AND", Op1: "b", Op1Type: "STRING"}},
		},
		Sortlist: []*ibclient.Sortlist{
			{Address: "10.0.0.0/8", MatchList: []string{"10.0.0.2"}},
		},
	})

	e := &namespacedExternal{conn: newTestConnector(t, srv)}
	cr := newNamespacedDNSView("default", "my-dnsview", ref, "ProviderConfig")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Fatal("Observe: want ResourceExists=true")
	}

	ap := cr.Status.AtProvider
	if len(ap.CustomRootNameServers) != 1 || ap.CustomRootNameServers[0].Name == nil || *ap.CustomRootNameServers[0].Name != "ns2.example.com" {
		t.Errorf("AtProvider.CustomRootNameServers = %+v, want one entry with Name=ns2.example.com", ap.CustomRootNameServers)
	}
	if len(ap.DnssecTrustedKeys) != 1 {
		t.Errorf("AtProvider.DnssecTrustedKeys = %+v, want one entry", ap.DnssecTrustedKeys)
	}
	if len(ap.FilterAaaaList) != 1 || len(ap.LastQueriedAcl) != 1 || len(ap.MatchClients) != 1 || len(ap.MatchDestinations) != 1 {
		t.Errorf("AtProvider address-ACL lists not fully mirrored: %+v", ap)
	}
	if len(ap.FixedRrsetOrderFqdns) != 1 {
		t.Errorf("AtProvider.FixedRrsetOrderFqdns = %+v, want one entry", ap.FixedRrsetOrderFqdns)
	}
	if ap.ResponseRateLimiting == nil || ap.ResponseRateLimiting.ResponsesPerSecond == nil || *ap.ResponseRateLimiting.ResponsesPerSecond != 10 {
		t.Errorf("AtProvider.ResponseRateLimiting = %+v, want ResponsesPerSecond=10", ap.ResponseRateLimiting)
	}
	if ap.ScavengingSettings == nil || ap.ScavengingSettings.ScavengingSchedule == nil || ap.ScavengingSettings.ScavengingSchedule.RecurringTime == nil || *ap.ScavengingSettings.ScavengingSchedule.RecurringTime != 1700000000 {
		t.Errorf("AtProvider.ScavengingSettings.ScavengingSchedule.RecurringTime = %+v, want 1700000000", ap.ScavengingSettings)
	}
	if len(ap.Sortlist) != 1 {
		t.Errorf("AtProvider.Sortlist = %+v, want one entry", ap.Sortlist)
	}

	// Round-trip through Update() — same rationale as the cluster-scope test.
	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error round-tripping full field set: %v", err)
	}
}
