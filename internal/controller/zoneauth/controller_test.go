// Package zoneauth unit tests for the ZoneAuth MR controllers. Tests use
// inline httptest.NewServer mocks that emulate the WAPI zone_auth
// endpoints, PascalCase test names (no underscores), and white-box access
// to the unexported connectors/clients so both scopes can be exercised
// without going through the full Connect() credential bridge on every
// test.
package zoneauth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
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

	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/zoneauth/v1alpha1"
	namespacedpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/zoneauth/v1alpha1"
)

// ── generic helpers ─────────────────────────────────────────────────────────

func stringPtr(s string) *string { return &s }
func boolPtr(b bool) *bool       { return &b }
func uint32Ptr(u uint32) *uint32 { return &u }

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

// newClusterZoneAuth builds a minimal cluster-scoped ZoneAuth CR. When
// externalName is empty, the external-name annotation is left unset. When
// it equals crName it simulates the framework's NameAsExternalName
// initializer (the pre-create state); any other value simulates a
// Create()-assigned server ref.
func newClusterZoneAuth(crName, externalName string) *clusterv1alpha1.ZoneAuth {
	cr := &clusterv1alpha1.ZoneAuth{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: "test-uid-cluster"},
		Spec: clusterv1alpha1.ZoneAuthSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.ZoneAuthParameters{
				FQDN: stringPtr("example.com"),
				View: stringPtr("default"),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newNamespacedZoneAuth is the namespaced variant of newClusterZoneAuth.
func newNamespacedZoneAuth(ns, crName, externalName, pcKind string) *namespacedv1alpha1.ZoneAuth {
	cr := &namespacedv1alpha1.ZoneAuth{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: "test-uid-namespaced"},
		Spec: namespacedv1alpha1.ZoneAuthSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: "default"},
			},
			ForProvider: namespacedv1alpha1.ZoneAuthParameters{
				FQDN: stringPtr("example.com"),
				View: stringPtr("default"),
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
// mockWapiServer emulates the subset of NIOS WAPI zone_auth endpoints
// exercised by the ZoneAuth controller (POST create, GET/PUT/DELETE by
// _ref). Records are marshaled/unmarshaled using the real
// ibclient.ZoneAuth type so the wire format (including the EA
// {"value": ...} envelope) exactly matches what the SDK sends and expects.

type mockWapiServer struct {
	mu      sync.Mutex
	records map[string]*ibclient.ZoneAuth
	nextRef int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert immutable fields are omitted.
	lastUpdateBody []byte
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{records: map[string]*ibclient.ZoneAuth{}}
}

func (m *mockWapiServer) seed(rec *ibclient.ZoneAuth) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRef++
	if rec.Ref == "" {
		rec.Ref = m.newRefLocked(rec)
	}
	m.records[rec.Ref] = rec
	return rec.Ref
}

func (m *mockWapiServer) newRefLocked(rec *ibclient.ZoneAuth) string {
	view := ""
	if rec.View != nil {
		view = *rec.View
	}
	return "zone_auth/test" + itoa(m.nextRef) + ":" + rec.Fqdn + "/" + view
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

// handler returns an http.Handler implementing the zone_auth WAPI surface.
func (m *mockWapiServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/zone_auth", func(w http.ResponseWriter, r *http.Request) {
		var rec ibclient.ZoneAuth
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
		var incoming ibclient.ZoneAuth
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body
		// Only mutable fields are ever applied — fqdn/view/zone_format
		// are left untouched regardless of what (if anything) the
		// request body carries for them, mirroring WAPI's rejection of
		// changes to those fields via PUT.
		existing.Comment = incoming.Comment
		existing.Disable = incoming.Disable
		existing.SoaDefaultTtl = incoming.SoaDefaultTtl
		existing.SoaExpire = incoming.SoaExpire
		existing.SoaNegativeTtl = incoming.SoaNegativeTtl
		existing.SoaRefresh = incoming.SoaRefresh
		existing.SoaRetry = incoming.SoaRetry
		existing.NsGroup = incoming.NsGroup
		existing.Ea = incoming.Ea
		existing.GridPrimary = incoming.GridPrimary
		existing.GridSecondaries = incoming.GridSecondaries
		existing.ExternalPrimaries = incoming.ExternalPrimaries
		existing.ExternalSecondaries = incoming.ExternalSecondaries
		m.mu.Unlock()

		writeJSON(w, http.StatusOK, ref)
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

	ref := m.seed(&ibclient.ZoneAuth{
		Fqdn:    "example.com",
		View:    stringPtr("default"),
		Comment: stringPtr("hello"),
		Disable: boolPtr(false),
		Ea:      ibclient.EA{"env": "prod"},
	})

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", ref)
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
	if cr.Status.AtProvider.FQDN == nil || *cr.Status.AtProvider.FQDN != "example.com" {
		t.Errorf("AtProvider.FQDN = %v, want example.com", cr.Status.AtProvider.FQDN)
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
	cr := newClusterZoneAuth("my-zoneauth", "zone_auth/does-not-exist:example.com/default")

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
	cr := newClusterZoneAuth("my-zoneauth", "") // external-name unset
	meta.SetExternalName(cr, cr.GetName())      // simulate NameAsExternalName initializer

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
	cr := newClusterZoneAuth("my-zoneauth", "zone_auth/test1:example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", "zone_auth/test1:example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestClusterObserveMinimalResponse pins nil-safety in Observe: a WAPI
// response carrying only the object's _ref (the resource identifier) and
// every other field at its Go zero value (nil pointers, empty strings, a
// nil Ea map, nil slices) must not panic and must produce a valid
// observation with nil-safe AtProvider fields.
func TestClusterObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneAuth{Fqdn: "example.com"})

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", ref)

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
	if ap.Disable != nil {
		t.Errorf("AtProvider.Disable = %v, want nil", ap.Disable)
	}
	if ap.ExtAttrs != nil {
		t.Errorf("AtProvider.ExtAttrs = %v, want nil", ap.ExtAttrs)
	}
	if ap.GridPrimary != nil {
		t.Errorf("AtProvider.GridPrimary = %v, want nil", ap.GridPrimary)
	}
	if ap.ExternalPrimaries != nil {
		t.Errorf("AtProvider.ExternalPrimaries = %v, want nil", ap.ExternalPrimaries)
	}
}

// ── cluster: Create ─────────────────────────────────────────────────────

func TestClusterCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", "") // no external-name yet

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

func TestClusterCreateSendsFullPayload(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", "")
	cr.Spec.ForProvider.ZoneFormat = stringPtr("FORWARD")
	cr.Spec.ForProvider.Comment = stringPtr("created by test")
	cr.Spec.ForProvider.SoaDefaultTTL = uint32Ptr(3600)

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
	if rec.Fqdn != "example.com" {
		t.Errorf("Create: Fqdn = %q, want example.com", rec.Fqdn)
	}
	if rec.ZoneFormat != "FORWARD" {
		t.Errorf("Create: ZoneFormat = %q, want FORWARD", rec.ZoneFormat)
	}
	if rec.Comment == nil || *rec.Comment != "created by test" {
		t.Errorf("Create: Comment = %v, want 'created by test'", rec.Comment)
	}
	if rec.SoaDefaultTtl == nil || *rec.SoaDefaultTtl != 3600 {
		t.Errorf("Create: SoaDefaultTtl = %v, want 3600", rec.SoaDefaultTtl)
	}
}

// ── cluster: immutable fields ────────────────────────────────────────────

func TestClusterIsUpToDateIgnoresImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneAuth{
		Fqdn: "example.com",
		View: stringPtr("original-view"),
	})

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", ref)
	// Mutate the immutable view/zoneFormat fields in spec — this must
	// NOT affect ResourceUpToDate, since WAPI rejects PUT changes to
	// fqdn/view/zone_format.
	cr.Spec.ForProvider.View = stringPtr("changed-view")
	cr.Spec.ForProvider.ZoneFormat = stringPtr("IPV4")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true despite view/zoneFormat drift (immutable fields), got false")
	}
}

func TestUpdateDoesNotSendImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneAuth{
		Fqdn: "example.com",
		View: stringPtr("default"),
	})

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", ref)
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
	for _, immutable := range []string{"fqdn", "view", "zone_format"} {
		if _, present := sent[immutable]; present {
			t.Errorf("Update: PUT body contains immutable field %q, want absent. body=%s", immutable, body)
		}
	}
}

// ── cluster: Update ─────────────────────────────────────────────────────

func TestClusterUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneAuth{
		Fqdn:    "example.com",
		View:    stringPtr("default"),
		Comment: stringPtr("old comment"),
	})

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", ref)
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

func TestClusterUpdateRefStaysStable(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneAuth{Fqdn: "example.com", View: stringPtr("default")})

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", ref)
	cr.Spec.ForProvider.Comment = stringPtr("bump")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}
	if got := meta.GetExternalName(cr); got != ref {
		t.Errorf("Update: external-name changed to %q, want stable %q", got, ref)
	}
}

func TestClusterUpdateNestedFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneAuth{Fqdn: "example.com", View: stringPtr("default")})

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", ref)
	cr.Spec.ForProvider.GridPrimary = []clusterv1alpha1.MemberServer{
		{Name: stringPtr("member1.example.com"), Stealth: boolPtr(true)},
	}
	cr.Spec.ForProvider.ExternalSecondaries = []clusterv1alpha1.ExternalServer{
		{Address: stringPtr("10.0.0.5"), Name: stringPtr("ext-ns.example.com")},
	}

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	rec := m.records[ref]
	m.mu.Unlock()
	if len(rec.GridPrimary) != 1 || rec.GridPrimary[0].Name != "member1.example.com" {
		t.Errorf("Update: GridPrimary = %+v, want one member1.example.com entry", rec.GridPrimary)
	}
	if len(rec.ExternalSecondaries) != 1 || rec.ExternalSecondaries[0].Address != "10.0.0.5" {
		t.Errorf("Update: ExternalSecondaries = %+v, want one 10.0.0.5 entry", rec.ExternalSecondaries)
	}
}

// ── cluster: Delete ─────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneAuth{Fqdn: "example.com", View: stringPtr("default")})

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", ref)

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
	cr := newClusterZoneAuth("my-zoneauth", "zone_auth/does-not-exist:example.com/default")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Errorf("Delete: expected nil error for already-deleted resource (404), got %v", err)
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

	cr := newClusterZoneAuth("my-zoneauth", "")
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

	cr := newClusterZoneAuth("my-zoneauth", "")
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

// ── namespaced: Observe ──────────────────────────────────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneAuth{
		Fqdn: "example.com",
		View: stringPtr("default"),
	})

	e := &namespacedExternal{conn: newTestConnector(t, srv)}
	cr := newNamespacedZoneAuth("default", "my-zoneauth", ref, "ProviderConfig")

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
	cr := newNamespacedZoneAuth("default", "my-zoneauth", "zone_auth/does-not-exist:example.com/default", "ProviderConfig")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false for 404, got true")
	}
}

// ── namespaced: Create/Update/Delete ─────────────────────────────────────

func TestNamespacedCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{conn: newTestConnector(t, srv)}
	cr := newNamespacedZoneAuth("default", "my-zoneauth", "", "ProviderConfig")

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

	ref := m.seed(&ibclient.ZoneAuth{Fqdn: "example.com", View: stringPtr("default")})

	e := &namespacedExternal{conn: newTestConnector(t, srv)}
	cr := newNamespacedZoneAuth("default", "my-zoneauth", ref, "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("namespaced update")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	rec := m.records[ref]
	m.mu.Unlock()
	if rec.Comment == nil || *rec.Comment != "namespaced update" {
		t.Errorf("Update: Comment = %v, want 'namespaced update'", rec.Comment)
	}
}

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneAuth{Fqdn: "example.com", View: stringPtr("default")})

	e := &namespacedExternal{conn: newTestConnector(t, srv)}
	cr := newNamespacedZoneAuth("default", "my-zoneauth", ref, "ProviderConfig")

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
	cr := newNamespacedZoneAuth("default", "my-zoneauth", "zone_auth/does-not-exist:example.com/default", "ProviderConfig")

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

	cr := newNamespacedZoneAuth(ns, "my-zoneauth", "", "ProviderConfig")
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

	cr := newNamespacedZoneAuth("app-ns", "my-zoneauth", "", "ClusterProviderConfig")
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

	cr := newNamespacedZoneAuth("default", "my-zoneauth", "", "SomeOtherKind")
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

// formattedWapiError mimics the plain-string error the SDK's
// getHTTPResponseError constructs for non-404 statuses.
type formattedWapiError struct{ msg string }

func (e *formattedWapiError) Error() string { return e.msg }

func TestIsNotFoundFalseForNil(t *testing.T) {
	if isNotFound(nil) {
		t.Error("isNotFound(nil) = true, want false")
	}
}

func TestMemberServerValuesRoundTrip(t *testing.T) {
	in := []clusterv1alpha1.MemberServer{
		{
			Name:          stringPtr("member1.example.com"),
			Stealth:       boolPtr(true),
			GridReplicate: boolPtr(false),
			Lead:          boolPtr(true),
			PreferredPrimaries: []clusterv1alpha1.ExternalServer{
				{Address: stringPtr("10.0.0.1"), Name: stringPtr("ns1.example.com")},
			},
			EnablePreferredPrimaries: boolPtr(true),
		},
	}
	vals := clusterMemberServerValues(in)
	sdk := memberServerValuesToSDK(vals)
	roundTripped := memberServerValuesFromSDK(sdk)

	if !memberServerValuesEqual(vals, roundTripped) {
		t.Errorf("MemberServer round-trip mismatch: got %+v, want %+v", roundTripped, vals)
	}
}

func TestIsUpToDateExtAttrsMismatch(t *testing.T) {
	desired := zoneAuthFields{ExtAttrs: map[string]string{"env": "prod"}}
	observed := zoneAuthFields{ExtAttrs: map[string]string{"env": "staging"}}
	if isUpToDate(desired, observed) {
		t.Error("isUpToDate: want false for differing ExtAttrs, got true")
	}
}

func TestIsUpToDateGridPrimaryMismatch(t *testing.T) {
	desired := zoneAuthFields{GridPrimary: []memberServerValue{{Name: "a.example.com"}}}
	observed := zoneAuthFields{GridPrimary: []memberServerValue{{Name: "b.example.com"}}}
	if isUpToDate(desired, observed) {
		t.Error("isUpToDate: want false for differing GridPrimary, got true")
	}
}

func TestLateInitializeBackfillsServerDefaults(t *testing.T) {
	desired := zoneAuthFields{}
	observed := zoneAuthFields{
		Comment:       stringPtr("server comment"),
		Disable:       boolPtr(false),
		SoaDefaultTTL: uint32Ptr(28800),
		NsGroup:       stringPtr("default-nsgroup"),
	}

	updated, changed := lateInitializeFields(desired, observed)
	if !changed {
		t.Fatal("lateInitializeFields: want changed=true, got false")
	}
	if updated.Comment == nil || *updated.Comment != "server comment" {
		t.Errorf("lateInitializeFields: Comment = %v, want 'server comment'", updated.Comment)
	}
	if updated.SoaDefaultTTL == nil || *updated.SoaDefaultTTL != 28800 {
		t.Errorf("lateInitializeFields: SoaDefaultTTL = %v, want 28800", updated.SoaDefaultTTL)
	}
	if updated.NsGroup == nil || *updated.NsGroup != "default-nsgroup" {
		t.Errorf("lateInitializeFields: NsGroup = %v, want 'default-nsgroup'", updated.NsGroup)
	}
}

func TestLateInitializeDoesNotOverwriteSetFields(t *testing.T) {
	desired := zoneAuthFields{Comment: stringPtr("user comment")}
	observed := zoneAuthFields{Comment: stringPtr("server comment")}

	updated, changed := lateInitializeFields(desired, observed)
	if changed {
		t.Error("lateInitializeFields: want changed=false when spec already set, got true")
	}
	if updated.Comment == nil || *updated.Comment != "user comment" {
		t.Errorf("lateInitializeFields: Comment = %v, want 'user comment' preserved", updated.Comment)
	}
}

func TestLateInitializeNeverTouchesImmutableFields(t *testing.T) {
	desired := zoneAuthFields{FQDN: "example.com"}
	observed := zoneAuthFields{FQDN: "example.com", View: stringPtr("default"), ZoneFormat: "FORWARD"}

	updated, _ := lateInitializeFields(desired, observed)
	if updated.View != nil {
		t.Errorf("lateInitializeFields: View = %v, want nil (immutable fields never late-initialized)", updated.View)
	}
	if updated.ZoneFormat != "" {
		t.Errorf("lateInitializeFields: ZoneFormat = %q, want empty (immutable fields never late-initialized)", updated.ZoneFormat)
	}
}
