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
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/zoneauth/v1alpha1"
	namespacedpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/zoneauth/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
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

	// eaDefExists controls the identity extensible-attribute-definition
	// prerequisite endpoint. Defaults to true via newMockWapiServer.
	eaDefExists bool
	// eaDefCreatable controls whether a POST to create the missing
	// definition succeeds, when eaDefExists is false.
	eaDefCreatable bool
	// searchCalls counts identity-EA search requests.
	searchCalls int
	// eaDefSearchCalls counts prerequisite-probe GET requests.
	eaDefSearchCalls int
	// createCalls counts POST (create) requests — tests assert this to
	// prove a refusal or a validation failure issued zero mutating
	// requests, not just that the in-memory record set looks unchanged.
	createCalls int
	// deleteCalls counts DELETE requests, for the same reason.
	deleteCalls int
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{records: map[string]*ibclient.ZoneAuth{}, eaDefExists: true}
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
		m.mu.Lock()
		m.createCalls++
		m.mu.Unlock()
		ref := m.seed(&rec)
		writeJSON(w, http.StatusOK, ref)
	})

	// Search endpoint: a GET with no _ref path segment. The identity
	// ladder (identity.Resolve's searchByUID) filters by the stamped
	// "*Crossplane Internal ID" extensible attribute; legacy tests may
	// still filter by fqdn/view query params. Registered as an exact
	// literal path so Go's ServeMux prefers it over the {ref...}
	// wildcard below for requests to precisely "zone_auth" (real _refs
	// always carry additional path segments).
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/zone_auth", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		uid := q.Get("*" + identity.EAKey)
		fqdn := q.Get("fqdn")
		view := q.Get("view")

		m.mu.Lock()
		m.searchCalls++
		var matches []ibclient.ZoneAuth
		for _, rec := range m.records {
			if uid != "" {
				got, ok := rec.Ea[identity.EAKey]
				if !ok || got != uid {
					continue
				}
				matches = append(matches, *rec)
				continue
			}
			if fqdn != "" && rec.Fqdn != fqdn {
				continue
			}
			if view != "" && (rec.View == nil || *rec.View != view) {
				continue
			}
			matches = append(matches, *rec)
		}
		m.mu.Unlock()

		// Always respond 200 — WAPI search semantics report "not found"
		// via an empty array, never an HTTP error status.
		writeJSON(w, http.StatusOK, matches)
	})

	// Identity extensible-attribute-definition prerequisite endpoint
	// (see internal/clients/identity's Prober).
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/extensibleattributedef", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.eaDefSearchCalls++
		exists := m.eaDefExists
		m.mu.Unlock()
		if exists {
			writeJSON(w, http.StatusOK, []ibclient.EADefinition{{Name: stringPtr(identity.EAKey)}})
			return
		}
		writeJSON(w, http.StatusOK, []ibclient.EADefinition{})
	})
	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/extensibleattributedef", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		creatable := m.eaDefCreatable
		m.mu.Unlock()
		if creatable {
			m.mu.Lock()
			m.eaDefExists = true
			m.mu.Unlock()
			writeJSON(w, http.StatusOK, "extensibleattributedef/identity-def:"+identity.EAKey)
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"Error":"IBDataConflictError: Cannot create extensible attribute definition. Only superusers can manage extensible attribute definition"}`))
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
		existing.UseGridZoneTimer = incoming.UseGridZoneTimer
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
		m.deleteCalls++
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
	}, true, "http", u.Port())
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
		Ea:      ibclient.EA{"env": "prod", identity.EAKey: "test-uid-cluster"},
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

// TestClusterObserveStripsIdentityEAFromExtAttrs proves the reserved
// identity key never late-inits into spec.forProvider.extAttrs. The CRD
// schema never includes it, and a CEL rule rejects a user-supplied value
// — back-filling it here would produce a permanent validation failure on
// the very next apply.
func TestClusterObserveStripsIdentityEAFromExtAttrs(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneAuth{
		Fqdn: "example.com",
		View: stringPtr("default"),
		Ea:   ibclient.EA{"env": "prod", identity.EAKey: "test-uid-cluster"},
	})

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", ref)
	cr.Spec.ForProvider.ExtAttrs = nil // force late-init from the observed EA

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}

	if _, present := cr.Spec.ForProvider.ExtAttrs[identity.EAKey]; present {
		t.Errorf("Observe: spec.forProvider.extAttrs contains the reserved identity key %q, want it stripped", identity.EAKey)
	}
	if !extAttrsEqual(cr.Spec.ForProvider.ExtAttrs, map[string]string{"env": "prod"}) {
		t.Errorf("Observe: spec.forProvider.extAttrs = %v, want {env: prod} (identity key stripped)", cr.Spec.ForProvider.ExtAttrs)
	}
	// The full-mirror AtProvider copy, by contrast, keeps the unstripped
	// map (convention 0032) — this is intentional, not a bug.
	if _, present := cr.Status.AtProvider.ExtAttrs[identity.EAKey]; !present {
		t.Error("Observe: status.atProvider.extAttrs must keep the identity key (full-mirror convention), but it was stripped")
	}
}

// TestObservePreCreateState verifies that Observe runs one identity
// search (not a hard-coded no-op) when the external-name still equals
// the CR's Kubernetes name — the pre-create state for a server-assigned
// external-name strategy. The pre-create guard does not short-circuit:
// it maps the annotation to "" and lets the identity ladder search by
// uid before concluding ResourceExists:false.
func TestObservePreCreateState(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
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

	m.mu.Lock()
	searchCalls := m.searchCalls
	m.mu.Unlock()
	if searchCalls == 0 {
		t.Error("Observe: want the identity ladder to search by uid even in the pre-create state, got zero search calls")
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
		Ea:   ibclient.EA{identity.EAKey: "test-uid-cluster"},
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

// ── cluster: use_grid_zone_timer forced on when any soa_* field is set ──
//
// WAPI silently ignores soa_default_ttl/soa_expire/soa_negative_ttl/
// soa_refresh/soa_retry while use_grid_zone_timer is off — a zone with
// the flag off inherits the Grid's timer values and never reflects what
// was submitted, with no error and no drift signal. buildZoneAuthForCreate/
// buildZoneAuthForUpdate force the flag on whenever any of the five is
// set (see effectiveUseGridZoneTimer), regardless of what — if anything —
// the spec itself says for useGridZoneTimer.

func TestClusterCreateForcesUseGridZoneTimerWhenSoaFieldSet(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", "")
	cr.Spec.ForProvider.SoaRefresh = uint32Ptr(21600)
	// useGridZoneTimer intentionally left unset.

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	ref := meta.GetExternalName(cr)
	m.mu.Lock()
	rec := m.records[ref]
	m.mu.Unlock()
	if rec.UseGridZoneTimer == nil || !*rec.UseGridZoneTimer {
		t.Errorf("Create: UseGridZoneTimer = %v, want true (forced on because soaRefresh is set)", rec.UseGridZoneTimer)
	}
	if rec.SoaRefresh == nil || *rec.SoaRefresh != 21600 {
		t.Errorf("Create: SoaRefresh = %v, want 21600", rec.SoaRefresh)
	}
}

func TestClusterUpdateForcesUseGridZoneTimerWhenSoaFieldSet(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneAuth{
		Fqdn:             "example.com",
		View:             stringPtr("default"),
		UseGridZoneTimer: boolPtr(false),
	})

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", ref)
	cr.Spec.ForProvider.SoaRefresh = uint32Ptr(21600)
	// useGridZoneTimer intentionally left unset in spec.

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	body := m.lastUpdateBody
	rec := m.records[ref]
	m.mu.Unlock()

	var sent map[string]interface{}
	if err := json.Unmarshal(body, &sent); err != nil {
		t.Fatalf("cannot unmarshal PUT body: %v", err)
	}
	if v, ok := sent["use_grid_zone_timer"]; !ok || v != true {
		t.Errorf("Update: PUT body use_grid_zone_timer = %v (present=%v), want true", v, ok)
	}
	if rec.UseGridZoneTimer == nil || !*rec.UseGridZoneTimer {
		t.Errorf("Update: stored UseGridZoneTimer = %v, want true (forced on)", rec.UseGridZoneTimer)
	}
}

// TestObserveConvergesAfterSoaSetWithoutExplicitFlag proves the fix does
// not introduce a new infinite reconcile loop: Create forces
// use_grid_zone_timer on (because soaRefresh is set), late-init then
// back-fills the flag into spec on the next Observe (spec never set it),
// and every subsequent Observe reports ResourceUpToDate=true — the effective
// flag semantics used by isUpToDate/lateInitializeScalars stay consistent
// with what the wire builders actually sent.
func TestObserveConvergesAfterSoaSetWithoutExplicitFlag(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", "")
	cr.Spec.ForProvider.SoaRefresh = uint32Ptr(21600)

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !obs.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true after Create forces use_grid_zone_timer on, got false")
	}
	if !obs.ResourceLateInitialized {
		t.Error("Observe: want ResourceLateInitialized=true (useGridZoneTimer back-filled from observed), got false")
	}
	if cr.Spec.ForProvider.UseGridZoneTimer == nil || !*cr.Spec.ForProvider.UseGridZoneTimer {
		t.Errorf("Observe: spec.forProvider.useGridZoneTimer = %v, want true (back-filled)", cr.Spec.ForProvider.UseGridZoneTimer)
	}

	// A second Observe (the following reconcile) must stay stable.
	obs2, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe (2nd): unexpected error: %v", err)
	}
	if !obs2.ResourceUpToDate {
		t.Error("Observe (2nd): want ResourceUpToDate=true (stable, no loop), got false")
	}
}

// TestObserveNoInfiniteLoopWhenUseGridZoneTimerExplicitlyFalseWithSoaSet
// is the risk scenario called out during the fix's design: the user
// explicitly sets useGridZoneTimer: false in spec (so late-init's
// unconditional back-fill never touches it) while also setting a soa_*
// field. Without gating isUpToDate/lateInitializeScalars on the same
// effective flag the wire builders use, this would loop forever (desired
// stuck reading false, observed always reporting true because Create/
// Update always force it on the wire).
func TestObserveNoInfiniteLoopWhenUseGridZoneTimerExplicitlyFalseWithSoaSet(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", "")
	cr.Spec.ForProvider.SoaRefresh = uint32Ptr(21600)
	cr.Spec.ForProvider.UseGridZoneTimer = boolPtr(false)

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	for i := 0; i < 3; i++ {
		obs, err := e.Observe(context.Background(), cr)
		if err != nil {
			t.Fatalf("Observe iteration %d: unexpected error: %v", i, err)
		}
		if !obs.ResourceUpToDate {
			t.Fatalf("Observe iteration %d: want ResourceUpToDate=true even though spec explicitly sets useGridZoneTimer=false (soaRefresh being set forces the effective flag on), got false — this would be an infinite reconcile loop", i)
		}
	}
}

// ── cluster: Delete ─────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneAuth{Fqdn: "example.com", View: stringPtr("default"), Ea: ibclient.EA{identity.EAKey: "test-uid-cluster"}})

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

// TestClusterDeleteServerError verifies that a 5xx response from the WAPI
// delete endpoint is propagated (wrapped, not swallowed) rather than being
// treated as a not-found/already-deleted success.
func TestClusterDeleteServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", "zone_auth/test1:example.com/default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteZoneAuth) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteZoneAuth)
	}
}

// TestClusterDeleteRefusesOnForeignIdentity verifies the identity ladder's
// handle-reuse refusal: the stored _ref still resolves, but its stamped
// identity attribute belongs to a different managed resource. Deleting it
// would destroy someone else's object, so Delete() must refuse and leave
// the record in place.
func TestClusterDeleteRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneAuth{Fqdn: "example.com", View: stringPtr("default"), Ea: ibclient.EA{identity.EAKey: "someone-elses-uid"}})

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", ref)

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected refusal error when the resolved object's identity attribute belongs to a different owner, got nil")
	}
	var reuse *identity.HandleReuseError
	if !cperrors.As(err, &reuse) {
		t.Errorf("Delete: error = %v, want a *identity.HandleReuseError", err)
	}

	m.mu.Lock()
	_, stillExists := m.records[ref]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Delete: live record was removed despite the refusal — DELETE must not have been issued against it")
	}
}

// TestClusterDeleteRefusesOnUnstampedObject verifies the identity ladder's
// adopt-vs-delete asymmetry: the stored _ref resolves but the object
// carries no identity stamp at all. Observe() adopts and re-stamps such
// objects leniently, but Delete() must refuse — destroying an object is
// irreversible and ownership cannot be proven.
func TestClusterDeleteRefusesOnUnstampedObject(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneAuth{Fqdn: "example.com", View: stringPtr("default")})

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", ref)

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected refusal error when the resolved object carries no identity stamp, got nil")
	}
	if !strings.Contains(err.Error(), "refusing to delete") {
		t.Errorf("Delete: error = %q, want it to explain the refusal", err.Error())
	}

	m.mu.Lock()
	_, stillExists := m.records[ref]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Delete: live record was removed despite the refusal — DELETE must not have been issued against it")
	}
}

// TestClusterDeleteRecoversRotatedRefAndDeletes verifies rotation
// recovery: the stored _ref 404s, but exactly one live object carries
// this managed resource's identity stamp. Delete() must recover it via
// the identity-EA search and delete the recovered object, not report a
// false already-gone success.
func TestClusterDeleteRecoversRotatedRefAndDeletes(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	cr := newClusterZoneAuth("my-zoneauth", "zone_auth/stale-ref:example.com/default")
	liveRef := m.seed(&ibclient.ZoneAuth{Fqdn: "example.com", View: stringPtr("default"), Ea: ibclient.EA{identity.EAKey: string(cr.GetUID())}})

	e := &clusterExternal{conn: newTestConnector(t, srv)}

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error recovering a rotated reference: %v", err)
	}

	m.mu.Lock()
	_, stillExists := m.records[liveRef]
	m.mu.Unlock()
	if stillExists {
		t.Error("Delete: recovered record was not removed")
	}
}

// TestClusterDeleteSucceedsWhenTrulyAbsent is the companion happy path: a
// 404 against the stored _ref, and an identity-EA search that finds
// nothing, means the object really is gone.
func TestClusterDeleteSucceedsWhenTrulyAbsent(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", "zone_auth/stale-ref:example.com/default")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the identity search also finds nothing, got: %v", err)
	}
}

// TestClusterObserveRefusesOnForeignIdentity verifies the Observe()-side
// half of handle-reuse refusal: crossplane-runtime's managed reconciler
// calls Observe() before Delete() on the deletion path, and if Observe()
// silently adopted a foreign object it would let the next Update/Delete
// mutate or destroy someone else's record.
func TestClusterObserveRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneAuth{Fqdn: "example.com", View: stringPtr("default"), Ea: ibclient.EA{identity.EAKey: "someone-elses-uid"}})

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", ref)

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected refusal error when the resolved object's identity attribute belongs to a different owner, got nil")
	}
	var reuse *identity.HandleReuseError
	if !cperrors.As(err, &reuse) {
		t.Errorf("Observe: error = %v, want a *identity.HandleReuseError", err)
	}

	m.mu.Lock()
	_, stillExists := m.records[ref]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Observe: live record was removed — Observe() must never mutate the backend")
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

	ref := m.seed(&ibclient.ZoneAuth{Fqdn: "example.com", View: stringPtr("default"), Ea: ibclient.EA{identity.EAKey: "test-uid-namespaced"}})

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

// TestNamespacedDeleteServerError verifies that a 5xx response from the
// WAPI delete endpoint is propagated (wrapped, not swallowed) rather than
// being treated as a not-found/already-deleted success.
func TestNamespacedDeleteServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{conn: newTestConnector(t, srv)}
	cr := newNamespacedZoneAuth("default", "my-zoneauth", "zone_auth/test1:example.com/default", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteZoneAuth) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteZoneAuth)
	}
}

// TestNamespacedDeleteRefusesOnForeignIdentity is the namespaced-scope
// counterpart of TestClusterDeleteRefusesOnForeignIdentity.
func TestNamespacedDeleteRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneAuth{Fqdn: "example.com", View: stringPtr("default"), Ea: ibclient.EA{identity.EAKey: "someone-elses-uid"}})

	e := &namespacedExternal{conn: newTestConnector(t, srv)}
	cr := newNamespacedZoneAuth("default", "my-zoneauth", ref, "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected refusal error when the resolved object's identity attribute belongs to a different owner, got nil")
	}
	var reuse *identity.HandleReuseError
	if !cperrors.As(err, &reuse) {
		t.Errorf("Delete: error = %v, want a *identity.HandleReuseError", err)
	}

	m.mu.Lock()
	_, stillExists := m.records[ref]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Delete: live record was removed despite the refusal — DELETE must not have been issued against it")
	}
}

// TestNamespacedObserveRefusesOnForeignIdentity is the namespaced-scope
// counterpart of TestClusterObserveRefusesOnForeignIdentity.
func TestNamespacedObserveRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneAuth{Fqdn: "example.com", View: stringPtr("default"), Ea: ibclient.EA{identity.EAKey: "someone-elses-uid"}})

	e := &namespacedExternal{conn: newTestConnector(t, srv)}
	cr := newNamespacedZoneAuth("default", "my-zoneauth", ref, "ProviderConfig")

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected refusal error when the resolved object's identity attribute belongs to a different owner, got nil")
	}
	var reuse *identity.HandleReuseError
	if !cperrors.As(err, &reuse) {
		t.Errorf("Observe: error = %v, want a *identity.HandleReuseError", err)
	}

	m.mu.Lock()
	_, stillExists := m.records[ref]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Observe: live record was removed — Observe() must never mutate the backend")
	}
}

// TestNamespacedDeleteSucceedsWhenTrulyAbsent is the namespaced-scope
// counterpart of TestClusterDeleteSucceedsWhenTrulyAbsent.
func TestNamespacedDeleteSucceedsWhenTrulyAbsent(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{conn: newTestConnector(t, srv)}
	cr := newNamespacedZoneAuth("default", "my-zoneauth", "zone_auth/stale-ref:example.com/default", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the identity search also finds nothing, got: %v", err)
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
		Comment:          stringPtr("server comment"),
		Disable:          boolPtr(false),
		SoaDefaultTTL:    uint32Ptr(28800),
		UseGridZoneTimer: boolPtr(true),
		NsGroup:          stringPtr("default-nsgroup"),
	}

	updated, changed := lateInitializeFields(desired, observed)
	if !changed {
		t.Fatal("lateInitializeFields: want changed=true, got false")
	}
	if updated.Comment == nil || *updated.Comment != "server comment" {
		t.Errorf("lateInitializeFields: Comment = %v, want 'server comment'", updated.Comment)
	}
	if updated.UseGridZoneTimer == nil || !*updated.UseGridZoneTimer {
		t.Errorf("lateInitializeFields: UseGridZoneTimer = %v, want true", updated.UseGridZoneTimer)
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

func TestNewConnectorWithSchemeUsesConfiguredSslVerify(t *testing.T) {
	// Regression guard: newConnectorWithScheme must not hardcode
	// SslVerify to "true" — it must honor the sslVerify parameter. Both branches
	// must construct successfully (transport config validation happens
	// locally; no network round-trip occurs here).
	for name, sslVerify := range map[string]bool{"Enabled": true, "Disabled": false} {
		t.Run(name, func(t *testing.T) {
			creds := &nioCredentials{Host: "127.0.0.1", Username: "admin", Password: "s3cr3t"}
			conn, err := newConnectorWithScheme(creds, sslVerify, "http", "80")
			if err != nil {
				t.Fatalf("newConnectorWithScheme: unexpected error: %v", err)
			}
			if conn == nil {
				t.Fatal("newConnectorWithScheme: expected non-nil connector")
			}
		})
	}
}

// ── identity ladder: Ambiguous match refusal ────────────────────────────
//
// AmbiguousMatchError is the second typed refusal the identity ladder can
// return (alongside HandleReuseError) — when more than one Grid object
// carries this managed resource's identity stamp, the ladder must refuse
// rather than silently pick the first match. Taking an
// arbitrary match risks mutating or deleting an object this resource does
// not actually own.

func TestClusterObserveAmbiguousMatchRefusesAndDoesNotMutate(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	uid := "test-uid-cluster"
	ref1 := m.seed(&ibclient.ZoneAuth{Fqdn: "one.example.com", View: stringPtr("default"), Ea: ibclient.EA{identity.EAKey: uid}})
	ref2 := m.seed(&ibclient.ZoneAuth{Fqdn: "two.example.com", View: stringPtr("default"), Ea: ibclient.EA{identity.EAKey: uid}})

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", "")
	meta.SetExternalName(cr, cr.GetName()) // simulate NameAsExternalName initializer — forces the UID-only search path

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected refusal error when the identity search matches more than one Grid object, got nil")
	}
	var ambiguous *identity.AmbiguousMatchError
	if !cperrors.As(err, &ambiguous) {
		t.Errorf("Observe: error = %v, want a *identity.AmbiguousMatchError", err)
	}

	m.mu.Lock()
	_, ref1Exists := m.records[ref1]
	_, ref2Exists := m.records[ref2]
	deleteCalls := m.deleteCalls
	createCalls := m.createCalls
	eaDefSearchCalls := m.eaDefSearchCalls
	m.mu.Unlock()
	if !ref1Exists || !ref2Exists {
		t.Error("Observe: a live record was removed — Observe() must never mutate the backend, ambiguous or not")
	}
	if deleteCalls != 0 || createCalls != 0 {
		t.Errorf("Observe: deleteCalls=%d createCalls=%d, want 0/0 — an ambiguous match must never trigger a mutating request", deleteCalls, createCalls)
	}
	if eaDefSearchCalls != 0 {
		t.Errorf("Observe: eaDefSearchCalls = %d, want 0 — an AmbiguousMatchError is unrelated to whether the search itself failed and must not probe", eaDefSearchCalls)
	}
}

func TestNamespacedObserveAmbiguousMatchRefusesAndDoesNotMutate(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	uid := "test-uid-namespaced"
	ref1 := m.seed(&ibclient.ZoneAuth{Fqdn: "one.example.com", View: stringPtr("default"), Ea: ibclient.EA{identity.EAKey: uid}})
	ref2 := m.seed(&ibclient.ZoneAuth{Fqdn: "two.example.com", View: stringPtr("default"), Ea: ibclient.EA{identity.EAKey: uid}})

	e := &namespacedExternal{conn: newTestConnector(t, srv)}
	cr := newNamespacedZoneAuth("default", "my-zoneauth", "", "ProviderConfig")
	meta.SetExternalName(cr, cr.GetName())

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected refusal error when the identity search matches more than one Grid object, got nil")
	}
	var ambiguous *identity.AmbiguousMatchError
	if !cperrors.As(err, &ambiguous) {
		t.Errorf("Observe: error = %v, want a *identity.AmbiguousMatchError", err)
	}

	m.mu.Lock()
	_, ref1Exists := m.records[ref1]
	_, ref2Exists := m.records[ref2]
	deleteCalls := m.deleteCalls
	createCalls := m.createCalls
	eaDefSearchCalls := m.eaDefSearchCalls
	m.mu.Unlock()
	if !ref1Exists || !ref2Exists {
		t.Error("Observe: a live record was removed — Observe() must never mutate the backend, ambiguous or not")
	}
	if deleteCalls != 0 || createCalls != 0 {
		t.Errorf("Observe: deleteCalls=%d createCalls=%d, want 0/0 — an ambiguous match must never trigger a mutating request", deleteCalls, createCalls)
	}
	if eaDefSearchCalls != 0 {
		t.Errorf("Observe: eaDefSearchCalls = %d, want 0 — an AmbiguousMatchError is unrelated to whether the search itself failed and must not probe", eaDefSearchCalls)
	}
}

// ── identity ladder: Adopted never reports up to date ───────────────────
//
// OutcomeAdopted means the stored _ref resolved but the live object
// carries no identity stamp at all — Observe() adopts it leniently (so
// the very next reconcile can re-stamp it via Update), but it must never
// report ResourceUpToDate:true in the same pass, even when every
// user-facing field already matches the desired spec. Reporting up to
// date here would mean the object never gets stamped, since Update() is
// only invoked when ResourceUpToDate is false.

func TestClusterObserveAdoptedNeverReportsUpToDate(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneAuth{
		Fqdn:    "example.com",
		View:    stringPtr("default"),
		Comment: stringPtr("hello"),
		Disable: boolPtr(false),
		// No identity.EAKey stamped — this object is unowned.
	})

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", ref)
	cr.Spec.ForProvider.Comment = stringPtr("hello")
	cr.Spec.ForProvider.Disable = boolPtr(false)

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Error("Observe: want ResourceExists=true for an adoptable object, got false")
	}
	if got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=false for an adopted (unstamped) object even though every user-facing field matches — otherwise it is never re-stamped")
	}
}

func TestNamespacedObserveAdoptedNeverReportsUpToDate(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneAuth{
		Fqdn:    "example.com",
		View:    stringPtr("default"),
		Comment: stringPtr("hello"),
		Disable: boolPtr(false),
	})

	e := &namespacedExternal{conn: newTestConnector(t, srv)}
	cr := newNamespacedZoneAuth("default", "my-zoneauth", ref, "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("hello")
	cr.Spec.ForProvider.Disable = boolPtr(false)

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Error("Observe: want ResourceExists=true for an adoptable object, got false")
	}
	if got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=false for an adopted (unstamped) object even though every user-facing field matches — otherwise it is never re-stamped")
	}
}

// ── Create: identity stamp in the wire body, exactly once ───────────────

// TestClusterCreateStampsIdentityEAExactlyOnce asserts the identity value
// in the request the mock server actually decoded off the wire — not the
// in-memory cr or the local zoneAuthFields struct — and that Create()
// issued exactly one POST.
func TestClusterCreateStampsIdentityEAExactlyOnce(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", "")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	ref := meta.GetExternalName(cr)
	m.mu.Lock()
	rec, ok := m.records[ref]
	createCalls := m.createCalls
	m.mu.Unlock()
	if !ok {
		t.Fatalf("Create: no record captured for ref %q", ref)
	}
	if createCalls != 1 {
		t.Errorf("Create: createCalls = %d, want exactly 1", createCalls)
	}
	if got, present := rec.Ea[identity.EAKey]; !present || got != string(cr.GetUID()) {
		t.Errorf("Create: wire-captured Ea[%q] = %q (present=%v), want %q", identity.EAKey, got, present, cr.GetUID())
	}
}

// TestClusterCreateEmptyUIDFailsWithZeroMutatingRequests asserts that a
// managed resource with a blank uid (should never happen in a real
// cluster — the API server always assigns a well-formed UUID — but
// crossplane-runtime's type system does not guarantee it) fails hard
// before issuing any WAPI call. Stamping an empty identity value would
// make every future ambiguity search match every unstamped object.
//
// A whitespace-only uid is rejected the same way — createZoneAuth's
// guard trims before the emptiness check, matching identity.Resolve's
// ladder (see TestClusterCreateWhitespaceUIDFailsWithZeroMutatingRequests).
func TestClusterCreateEmptyUIDFailsWithZeroMutatingRequests(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", "")
	cr.UID = types.UID("")

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: want a hard error for a blank uid, got nil")
	}

	m.mu.Lock()
	createCalls := m.createCalls
	recordCount := len(m.records)
	m.mu.Unlock()
	if createCalls != 0 || recordCount != 0 {
		t.Errorf("Create: createCalls=%d recordCount=%d, want 0/0 for a blank uid", createCalls, recordCount)
	}
}

// TestClusterCreateWhitespaceUIDFailsWithZeroMutatingRequests proves a
// whitespace-only uid is rejected the same way a blank one is —
// createZoneAuth's guard trims before comparing, matching
// identity.Resolve's ladder (see internal/clients/identity). Without the
// trim, a whitespace-only uid would pass Create's guard and get stamped
// verbatim into the object's extensible attributes, while Observe/Delete
// (which route through identity.Resolve) would treat that same object as
// unowned.
func TestClusterCreateWhitespaceUIDFailsWithZeroMutatingRequests(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", "")
	cr.UID = types.UID("   ")

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: want a hard error for a whitespace-only uid, got nil")
	}

	m.mu.Lock()
	createCalls := m.createCalls
	recordCount := len(m.records)
	m.mu.Unlock()
	if createCalls != 0 || recordCount != 0 {
		t.Errorf("Create: createCalls=%d recordCount=%d, want 0/0 for a whitespace-only uid", createCalls, recordCount)
	}
}

// TestClusterUpdateWhitespaceUIDFailsWithZeroMutatingRequests proves the
// Update path rejects a whitespace-only uid the same way Create does —
// updateZoneAuth's guard trims before comparing, matching
// identity.Resolve's ladder. Without the trim, a whitespace-only uid
// would pass Update's guard and get re-stamped verbatim into the
// object's extensible attributes, while Observe/Delete (which route
// through identity.Resolve) would treat that same object as unowned.
func TestClusterUpdateWhitespaceUIDFailsWithZeroMutatingRequests(t *testing.T) {
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
	cr.UID = types.UID("   ")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	if _, err := e.Update(context.Background(), cr); err == nil {
		t.Fatal("Update: want a hard error for a whitespace-only uid, got nil")
	}

	m.mu.Lock()
	lastUpdateBody := m.lastUpdateBody
	comment := m.records[ref].Comment
	m.mu.Unlock()
	if lastUpdateBody != nil {
		t.Errorf("Update: PUT body = %s, want no PUT request issued for a whitespace-only uid", lastUpdateBody)
	}
	if comment == nil || *comment != "old comment" {
		t.Errorf("Update: Comment = %v, want unchanged 'old comment' — a whitespace-only uid must not mutate the object", comment)
	}
}

// ── rotation: persistence round-trips through a client ──────────────────
//
// Convention: a test asserting meta.GetExternalName(cr) on the very same
// in-memory object Observe() just mutated would pass even if the
// annotation mutation never actually reaches the object crossplane-
// runtime persists — for example if a future refactor read from a stale
// copy. This test performs the same write+re-GET-on-a-distinct-instance
// round trip the real managed reconciler performs after
// ResourceLateInitialized:true.
func TestClusterObserveRecoversRotatedRefPersistsAcrossReGet(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	cr := newClusterZoneAuth("my-zoneauth", "zone_auth/stale-ref:example.com/default")
	newRef := m.seed(&ibclient.ZoneAuth{Fqdn: "example.com", View: stringPtr("default"), Ea: ibclient.EA{identity.EAKey: string(cr.GetUID())}})

	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()
	e := &clusterExternal{conn: newTestConnector(t, srv)}

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceLateInitialized {
		t.Fatal("Observe: want ResourceLateInitialized=true so the recovered reference is persisted, got false")
	}
	if meta.GetExternalName(cr) != newRef {
		t.Fatalf("Observe: in-memory external-name = %q, want %q", meta.GetExternalName(cr), newRef)
	}

	// Simulate the managed reconciler's post-Observe persistence.
	if err := kube.Update(context.Background(), cr); err != nil {
		t.Fatalf("kube.Update: unexpected error: %v", err)
	}

	fetched := &clusterv1alpha1.ZoneAuth{}
	if err := kube.Get(context.Background(), types.NamespacedName{Name: cr.GetName()}, fetched); err != nil {
		t.Fatalf("kube.Get: unexpected error: %v", err)
	}
	if got := meta.GetExternalName(fetched); got != newRef {
		t.Errorf("Observe: persisted external-name (re-GET into a distinct object) = %q, want %q", got, newRef)
	}
}

// ── newZoneAuthForGet: the constructor actually passed to Resolve ───────
//
// internal/clients/identity's own TestNewEmptyCorrectness exercises
// ibclient.NewZoneAuth(ibclient.ZoneAuth{}) directly, documenting the SDK
// baseline — but resolveZoneAuthIdentity passes this package's own
// newZoneAuthForGet, not that raw constructor, to identity.Resolve. This
// test closes that gap: it must fail if newZoneAuthForGet is ever
// rewritten to build a bare &ibclient.ZoneAuth{} (losing every field the
// append-based construction currently adds on top of the SDK default,
// though extattrs would still survive since it is one of the SDK's own
// baseline return fields — the real risk this guards against is a
// rewrite that constructs the return-fields list from scratch instead of
// appending to the SDK default).
func TestNewZoneAuthForGetObjectTypeAndReturnFields(t *testing.T) {
	z := newZoneAuthForGet()
	if got := z.ObjectType(); got != "zone_auth" {
		t.Errorf("newZoneAuthForGet().ObjectType() = %q, want %q", got, "zone_auth")
	}
	fields := z.ReturnFields()
	found := false
	for _, f := range fields {
		if f == "extattrs" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("newZoneAuthForGet().ReturnFields() = %v, want it to contain %q — identity.Resolve reads the identity stamp from the Ea field, which this field populates on GET", fields, "extattrs")
	}
}
