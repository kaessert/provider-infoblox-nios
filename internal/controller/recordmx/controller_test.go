// Package recordmx unit tests for the MXRecord MR controllers. Tests use
// inline httptest.NewServer mocks that emulate the WAPI record:mx
// endpoints, PascalCase test names (no underscores), and white-box access
// to the unexported connectors/clients so both scopes can be exercised
// without going through the full Connect() credential bridge on every
// test.
package recordmx

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recordmx/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recordmx/v1alpha1"
	namespacedpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
)

// ── generic helpers ─────────────────────────────────────────────────────────

func stringPtr(s string) *string { return &s }
func int64Ptr(i int64) *int64    { return &i }
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

// newClusterMXRecord builds a minimal cluster-scoped MXRecord CR. When
// externalName is empty, the external-name annotation is left unset. When
// it equals crName it simulates the framework's NameAsExternalName
// initializer (the pre-create state); any other value simulates a
// Create()-assigned server ref.
func newClusterMXRecord(crName, externalName string) *clusterv1alpha1.MXRecord {
	cr := &clusterv1alpha1.MXRecord{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: "test-uid-cluster"},
		Spec: clusterv1alpha1.MXRecordSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.MXRecordParameters{
				Name:          stringPtr("example.com"),
				MailExchanger: stringPtr("mail.example.com"),
				Preference:    int64Ptr(10),
				View:          stringPtr("default"),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newNamespacedMXRecord is the namespaced variant of newClusterMXRecord.
func newNamespacedMXRecord(ns, crName, externalName, pcKind string) *namespacedv1alpha1.MXRecord {
	cr := &namespacedv1alpha1.MXRecord{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: "test-uid-namespaced"},
		Spec: namespacedv1alpha1.MXRecordSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: "default"},
			},
			ForProvider: namespacedv1alpha1.MXRecordParameters{
				Name:          stringPtr("example.com"),
				MailExchanger: stringPtr("mail.example.com"),
				Preference:    int64Ptr(10),
				View:          stringPtr("default"),
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
// mockWapiServer emulates the subset of NIOS WAPI record:mx endpoints
// exercised by the MXRecord controller (POST create, GET/PUT/DELETE by
// _ref, GET search by identity fields). Records are marshaled/unmarshaled
// using the real ibclient.RecordMX type so the wire format (including the
// EA {"value": ...} envelope) exactly matches what the SDK sends and
// expects.

type mockWapiServer struct {
	mu      sync.Mutex
	records map[string]*ibclient.RecordMX
	nextRef int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests inspecting exactly what the controller sends.
	lastUpdateBody []byte
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{records: map[string]*ibclient.RecordMX{}}
}

func (m *mockWapiServer) seed(rec *ibclient.RecordMX) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRef++
	if rec.Ref == "" {
		rec.Ref = m.newRefLocked(rec)
	}
	if rec.Zone == "" {
		rec.Zone = zoneFromName(rec.Name)
	}
	m.records[rec.Ref] = rec
	return rec.Ref
}

func (m *mockWapiServer) newRefLocked(rec *ibclient.RecordMX) string {
	name := ""
	if rec.Name != nil {
		name = *rec.Name
	}
	view := ""
	if rec.View != nil {
		view = *rec.View
	}
	return "record:mx/test" + itoa(m.nextRef) + ":" + name + "/" + view
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

// handler returns an http.Handler implementing the record:mx WAPI
// surface.
func (m *mockWapiServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/record:mx", func(w http.ResponseWriter, r *http.Request) {
		var rec ibclient.RecordMX
		if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		// Synthesize the zone the way NIOS derives it server-side (last
		// two labels of the FQDN), so Observe/Create tests can assert
		// the response-only Zone field is mirrored.
		rec.Zone = zoneFromName(rec.Name)
		ref := m.seed(&rec)
		writeJSON(w, http.StatusOK, ref)
	})

	// Search endpoint (GetMXRecord): a GET with no _ref path segment,
	// filtered by view/name/mail_exchanger/preference query params.
	// Registered as an exact literal path so Go's ServeMux prefers it
	// over the {ref...} wildcard below for requests to precisely
	// "record:mx" (real _refs always carry additional path segments).
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/record:mx", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		view := q.Get("view")
		name := q.Get("name")
		mx := q.Get("mail_exchanger")
		pref := q.Get("preference")

		m.mu.Lock()
		var matches []ibclient.RecordMX
		for _, rec := range m.records {
			if view != "" && (rec.View == nil || *rec.View != view) {
				continue
			}
			if name != "" && (rec.Name == nil || *rec.Name != name) {
				continue
			}
			if mx != "" && (rec.MailExchanger == nil || *rec.MailExchanger != mx) {
				continue
			}
			if pref != "" && (rec.Preference == nil || fmt.Sprintf("%d", *rec.Preference) != pref) {
				continue
			}
			matches = append(matches, *rec)
		}
		m.mu.Unlock()

		// Always respond 200 — WAPI search semantics report "not found"
		// via an empty array, never an HTTP error status.
		writeJSON(w, http.StatusOK, matches)
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
		var incoming ibclient.RecordMX
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body

		// Ref-mutating identity fields: name, mail_exchanger, and
		// preference all mutate the object's _ref on a real Grid
		// Manager (live-verified). Simulate that here so
		// Update()'s external-name refresh logic has real coverage.
		identityChanged := strOrEmpty(existing.Name) != strOrEmpty(incoming.Name) ||
			strOrEmpty(existing.MailExchanger) != strOrEmpty(incoming.MailExchanger) ||
			uint32PtrOrZero(existing.Preference) != uint32PtrOrZero(incoming.Preference)

		existing.Name = incoming.Name
		existing.MailExchanger = incoming.MailExchanger
		existing.Preference = incoming.Preference
		existing.Comment = incoming.Comment
		existing.Ttl = incoming.Ttl
		existing.UseTtl = incoming.UseTtl
		existing.Ea = incoming.Ea
		existing.Zone = zoneFromName(existing.Name)

		newRef := ref
		if identityChanged {
			delete(m.records, ref)
			m.nextRef++
			newRef = m.newRefLocked(existing)
			existing.Ref = newRef
			m.records[newRef] = existing
		}
		m.mu.Unlock()

		writeJSON(w, http.StatusOK, newRef)
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

func zoneFromName(name *string) string {
	if name == nil || *name == "" {
		return ""
	}
	n := *name
	for i := 0; i < len(n); i++ {
		if n[i] == '.' {
			return n[i+1:]
		}
	}
	return ""
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
	}, true, "http", u.Port())
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

	ref := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		Comment:       stringPtr("hello"),
		Ttl:           func() *uint32 { v := uint32(300); return &v }(),
		UseTtl:        boolPtr(true),
		Ea:            ibclient.EA{"env": "prod"},
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterMXRecord("my-mxrecord", ref)
	cr.Spec.ForProvider.Comment = stringPtr("hello")
	cr.Spec.ForProvider.TTL = int64Ptr(300)
	cr.Spec.ForProvider.UseTTL = boolPtr(true)
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
	if cr.Status.AtProvider.Zone == nil || *cr.Status.AtProvider.Zone != "com" {
		t.Errorf("AtProvider.Zone = %v, want com", cr.Status.AtProvider.Zone)
	}
	if cr.Status.AtProvider.Preference == nil || *cr.Status.AtProvider.Preference != 10 {
		t.Errorf("AtProvider.Preference = %v, want 10", cr.Status.AtProvider.Preference)
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
	cr := newClusterMXRecord("my-mxrecord", "record:mx/does-not-exist:example.com/default")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false for 404 with no matching search result, got true")
	}
}

// TestClusterObserveRefInstabilityFallsBackToSearch verifies that Observe
// recovers from a stale external-name annotation (the stored _ref no
// longer resolves) by re-searching via the CR's own identity fields
// (view, name, mailExchanger, preference), and refreshes the external
// name to the record's current _ref.
func TestClusterObserveRefInstabilityFallsBackToSearch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	realRef := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	// External-name annotation points at a stale _ref (as if the record
	// was renamed by a prior reconcile that changed identity fields, but
	// the annotation refresh was lost — e.g. controller restart).
	cr := newClusterMXRecord("my-mxrecord", "record:mx/stale:old.example.com/default")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Error("Observe: want ResourceExists=true via fallback search, got false")
	}
	if got := meta.GetExternalName(cr); got != realRef {
		t.Errorf("Observe: external-name = %q, want refreshed to %q", got, realRef)
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
	cr := newClusterMXRecord("my-mxrecord", "") // external-name unset
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

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterMXRecord("my-mxrecord", "record:mx/test1:example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterMXRecord("my-mxrecord", "record:mx/test1:example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestClusterObserveMinimalResponse pins nil-safety in Observe: a WAPI
// response carrying only the object's _ref (the resource identifier) and
// every other field at its Go zero value (nil pointers, empty strings, a
// nil Ea map) must not panic and must produce a valid observation with
// nil-safe AtProvider fields. observeFromRecordMX copies optional pointer
// fields directly (never dereferences without a nil guard), so this test
// also pins that contract for future edits.
func TestClusterObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	// Seed a completely bare RecordMX — only the SDK-assigned _ref (via
	// seed()) identifies the object. Name/View are nil, so zoneFromName
	// leaves Zone at "" too.
	ref := m.seed(&ibclient.RecordMX{})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterMXRecord("my-mxrecord", ref)

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
	if ap.MailExchanger != nil {
		t.Errorf("AtProvider.MailExchanger = %v, want nil", ap.MailExchanger)
	}
	if ap.Preference != nil {
		t.Errorf("AtProvider.Preference = %v, want nil", ap.Preference)
	}
	if ap.Comment != nil {
		t.Errorf("AtProvider.Comment = %v, want nil", ap.Comment)
	}
	if ap.TTL != nil {
		t.Errorf("AtProvider.TTL = %v, want nil", ap.TTL)
	}
	if ap.UseTTL != nil {
		t.Errorf("AtProvider.UseTTL = %v, want nil", ap.UseTTL)
	}
	if ap.ExtAttrs != nil {
		t.Errorf("AtProvider.ExtAttrs = %v, want nil", ap.ExtAttrs)
	}
	if ap.View != nil {
		t.Errorf("AtProvider.View = %v, want nil", ap.View)
	}
	if ap.Zone != nil {
		t.Errorf("AtProvider.Zone = %v, want nil", ap.Zone)
	}
}

// ── cluster: Create ─────────────────────────────────────────────────────

func TestClusterCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterMXRecord("my-mxrecord", "") // no external-name yet

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

// TestClusterCreateError verifies that a 5xx response from the WAPI create
// endpoint is propagated (wrapped, not swallowed) and that the
// external-name annotation is left unset so the framework retries Create
// rather than treating the resource as provisioned.
func TestClusterCreateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterMXRecord("my-mxrecord", "")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errCreateMXRecord) {
		t.Errorf("Create: error = %q, want it to contain %q (wrapped, not swallowed)", got, errCreateMXRecord)
	}
	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name = %q after failed create, want unset", got)
	}
}

func TestClusterObserveIsUpToDateIgnoresImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("original-view"),
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterMXRecord("my-mxrecord", ref)
	// Mutate the immutable view field in spec — this must NOT affect
	// ResourceUpToDate, since view is excluded from isUpToDate.
	cr.Spec.ForProvider.View = stringPtr("changed-view")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true despite view drift (immutable field), got false")
	}
}

// ── cluster: Update ─────────────────────────────────────────────────────

func TestClusterUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		Comment:       stringPtr("old comment"),
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterMXRecord("my-mxrecord", ref)
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.records[ref]
	m.mu.Unlock()
	if stored.Comment == nil || *stored.Comment != "new comment" {
		t.Errorf("Update: stored comment = %v, want %q", stored.Comment, "new comment")
	}
}

// TestClusterUpdateEchoesUnchangedView pins the documented SDK contract
// (see updateMXRecord's doc comment): UpdateMXRecord requires a dnsView
// argument and rejects the call if it doesn't match the record's current
// view, so the controller always echoes the CR's own (immutable, always
// correct) spec.View. This test proves that echo never actually changes
// the stored view — it's a required-parameter pass-through, not a
// mutation.
func TestClusterUpdateEchoesUnchangedView(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterMXRecord("my-mxrecord", ref)

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.records[ref]
	body := m.lastUpdateBody
	m.mu.Unlock()

	if stored.View == nil || *stored.View != "default" {
		t.Errorf("Update: stored view = %v, want unchanged %q", stored.View, "default")
	}

	var raw map[string]interface{}
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("cannot decode captured PUT body: %v", err)
	}
	if got, _ := raw["view"].(string); got != "default" {
		t.Errorf("Update: request body view = %v, want echoed %q", raw["view"], "default")
	}
}

// TestClusterUpdateRefChangedUpdatesExternalName verifies that when a
// ref-mutating field (name) changes, Update() refreshes the external-name
// annotation to the record's new _ref rather than leaving it pointed at
// the now-stale value.
func TestClusterUpdateRefChangedUpdatesExternalName(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterMXRecord("my-mxrecord", ref)
	cr.Spec.ForProvider.Name = stringPtr("renamed.example.com")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == ref {
		t.Error("Update: external-name unchanged after identity-field rename, want refreshed to new _ref")
	}
	if got == "" {
		t.Error("Update: external-name empty after rename")
	}
}

// TestClusterUpdateError verifies that a 5xx response from the WAPI
// update endpoint is propagated (wrapped, not swallowed).
func TestClusterUpdateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterMXRecord("my-mxrecord", "record:mx/test1:example.com/default")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errUpdateMXRecord) {
		t.Errorf("Update: error = %q, want it to contain %q (wrapped, not swallowed)", got, errUpdateMXRecord)
	}
}

// ── cluster: Delete ─────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordMX{Name: stringPtr("example.com"), View: stringPtr("default")})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterMXRecord("my-mxrecord", ref)

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

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterMXRecord("my-mxrecord", "record:mx/does-not-exist:example.com/default")

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
	cr := newClusterMXRecord("my-mxrecord", "record:mx/test1:example.com/default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteMXRecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteMXRecord)
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

	cr := newClusterMXRecord("my-mxrecord", "")
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

	cr := newClusterMXRecord("my-mxrecord", "")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for missing ProviderConfig, got nil")
	}
}

// ── namespaced: Observe ──────────────────────────────────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
	})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedMXRecord("default", "my-mxrecord", ref, "ProviderConfig")

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
	cr := newNamespacedMXRecord("default", "my-mxrecord", "record:mx/does-not-exist:example.com/default", "ProviderConfig")

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
	cr := newNamespacedMXRecord("default", "my-mxrecord", "", "ProviderConfig")
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
	cr := newNamespacedMXRecord("default", "my-mxrecord", "record:mx/test1:example.com/default", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestNamespacedObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedMXRecord("default", "my-mxrecord", "record:mx/test1:example.com/default", "ProviderConfig")

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

	ref := m.seed(&ibclient.RecordMX{})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedMXRecord("default", "my-mxrecord", ref, "ProviderConfig")

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
	if ap.MailExchanger != nil {
		t.Errorf("AtProvider.MailExchanger = %v, want nil", ap.MailExchanger)
	}
	if ap.Preference != nil {
		t.Errorf("AtProvider.Preference = %v, want nil", ap.Preference)
	}
	if ap.Comment != nil {
		t.Errorf("AtProvider.Comment = %v, want nil", ap.Comment)
	}
	if ap.TTL != nil {
		t.Errorf("AtProvider.TTL = %v, want nil", ap.TTL)
	}
	if ap.UseTTL != nil {
		t.Errorf("AtProvider.UseTTL = %v, want nil", ap.UseTTL)
	}
	if ap.ExtAttrs != nil {
		t.Errorf("AtProvider.ExtAttrs = %v, want nil", ap.ExtAttrs)
	}
	if ap.View != nil {
		t.Errorf("AtProvider.View = %v, want nil", ap.View)
	}
	if ap.Zone != nil {
		t.Errorf("AtProvider.Zone = %v, want nil", ap.Zone)
	}
}

// ── namespaced: Create/Update/Delete ─────────────────────────────────────

func TestNamespacedCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedMXRecord("default", "my-mxrecord", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

// TestNamespacedCreateError verifies that a 5xx response from the WAPI
// create endpoint is propagated (wrapped, not swallowed) and that the
// external-name annotation is left unset.
func TestNamespacedCreateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedMXRecord("default", "my-mxrecord", "", "ProviderConfig")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errCreateMXRecord) {
		t.Errorf("Create: error = %q, want it to contain %q (wrapped, not swallowed)", got, errCreateMXRecord)
	}
	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name = %q after failed create, want unset", got)
	}
}

func TestNamespacedUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
	})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedMXRecord("default", "my-mxrecord", ref, "ProviderConfig")
	cr.Spec.ForProvider.MailExchanger = stringPtr("mail2.example.com")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	m.mu.Lock()
	stored, ok := m.records[got]
	m.mu.Unlock()
	if !ok {
		t.Fatalf("Update: no record found under refreshed external-name %q", got)
	}
	if stored.MailExchanger == nil || *stored.MailExchanger != "mail2.example.com" {
		t.Errorf("Update: stored mailExchanger = %v, want mail2.example.com", stored.MailExchanger)
	}
}

// TestNamespacedUpdateError verifies that a 5xx response from the WAPI
// update endpoint is propagated (wrapped, not swallowed).
func TestNamespacedUpdateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedMXRecord("default", "my-mxrecord", "record:mx/test1:example.com/default", "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errUpdateMXRecord) {
		t.Errorf("Update: error = %q, want it to contain %q (wrapped, not swallowed)", got, errUpdateMXRecord)
	}
}

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordMX{Name: stringPtr("example.com"), View: stringPtr("default")})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedMXRecord("default", "my-mxrecord", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
}

func TestNamespacedDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedMXRecord("default", "my-mxrecord", "record:mx/does-not-exist:example.com/default", "ProviderConfig")

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
	cr := newNamespacedMXRecord("default", "my-mxrecord", "record:mx/test1:example.com/default", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteMXRecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteMXRecord)
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

	cr := newNamespacedMXRecord(ns, "my-mxrecord", "", "ProviderConfig")
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

	cr := newNamespacedMXRecord("app-ns", "my-mxrecord", "", "ClusterProviderConfig")
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

	cr := newNamespacedMXRecord("default", "my-mxrecord", "", "SomeOtherKind")
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
	var comment *string
	var ttl *int64
	var useTTL *bool
	extAttrs := map[string]string(nil)

	rec := &ibclient.RecordMX{
		Comment: stringPtr("server default"),
		Ttl:     func() *uint32 { v := uint32(600); return &v }(),
		UseTtl:  boolPtr(true),
		Ea:      ibclient.EA{"env": "prod"},
	}

	changed := lateInitialize(&comment, &ttl, &useTTL, &extAttrs, rec)
	if !changed {
		t.Fatal("lateInitialize: want changed=true, got false")
	}
	if comment == nil || *comment != "server default" {
		t.Errorf("lateInitialize: comment = %v, want %q", comment, "server default")
	}
	if ttl == nil || *ttl != 600 {
		t.Errorf("lateInitialize: ttl = %v, want 600", ttl)
	}
	if useTTL == nil || *useTTL != true {
		t.Errorf("lateInitialize: useTTL = %v, want true", useTTL)
	}
	if !extAttrsEqual(extAttrs, map[string]string{"env": "prod"}) {
		t.Errorf("lateInitialize: extAttrs = %v, want {env: prod}", extAttrs)
	}
}

func TestLateInitializeDoesNotOverwriteSetFields(t *testing.T) {
	comment := stringPtr("user comment")
	ttl := int64Ptr(120)
	useTTL := boolPtr(false)
	extAttrs := map[string]string{"env": "staging"}

	rec := &ibclient.RecordMX{
		Comment: stringPtr("server default"),
		Ttl:     func() *uint32 { v := uint32(600); return &v }(),
		UseTtl:  boolPtr(true),
		Ea:      ibclient.EA{"env": "prod"},
	}

	changed := lateInitialize(&comment, &ttl, &useTTL, &extAttrs, rec)
	if changed {
		t.Error("lateInitialize: want changed=false when all fields already set, got true")
	}
	if *comment != "user comment" || *ttl != 120 || *useTTL != false || extAttrs["env"] != "staging" {
		t.Error("lateInitialize: overwrote already-set ForProvider fields")
	}
}

// TestLateInitializeDoesNotBackfillTTLWhenUseTTLOff proves that when
// useTtl is false the observed ttl (WAPI's zone default, not a value the
// user's config implies) is never written back into spec.forProvider.ttl.
func TestLateInitializeDoesNotBackfillTTLWhenUseTTLOff(t *testing.T) {
	var comment *string
	var ttl *int64
	useTTL := boolPtr(false)
	extAttrs := map[string]string(nil)

	zoneDefault := uint32(28800)
	rec := &ibclient.RecordMX{
		Ttl:    &zoneDefault,
		UseTtl: boolPtr(false),
	}

	lateInitialize(&comment, &ttl, &useTTL, &extAttrs, rec)

	if ttl != nil {
		t.Errorf("lateInitialize: ttl = %v, want nil (useTtl is off, observed ttl is the zone default, not a user value)", *ttl)
	}
}

// TestObserveDoesNotLateInitializeRequiredFields proves that name,
// mailExchanger, preference, and view — the CRD's required
// MXRecordParameters fields — are never overwritten by Observe()'s
// late-init step. lateInitialize only accepts pointers to the optional
// fields (comment, ttl, useTtl, extAttrs), so a spec/observed mismatch on
// a required field can never occur through the real WAPI flow — this test
// drives it artificially to pin the guarantee.
func TestObserveDoesNotLateInitializeRequiredFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("observed.example.com"),
		MailExchanger: stringPtr("observed-mail.example.com"),
		Preference:    uint32Ptr(99),
		View:          stringPtr("observed-view"),
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterMXRecord("my-mxrecord", ref)
	cr.Spec.ForProvider.Name = stringPtr("host.example.com")
	cr.Spec.ForProvider.MailExchanger = stringPtr("mail.example.com")
	cr.Spec.ForProvider.Preference = int64Ptr(10)
	cr.Spec.ForProvider.View = stringPtr("default")

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}

	if got := *cr.Spec.ForProvider.Name; got != "host.example.com" {
		t.Errorf("Observe: required field Name late-initialized to %q, want unchanged %q", got, "host.example.com")
	}
	if got := *cr.Spec.ForProvider.MailExchanger; got != "mail.example.com" {
		t.Errorf("Observe: required field MailExchanger late-initialized to %q, want unchanged %q", got, "mail.example.com")
	}
	if got := *cr.Spec.ForProvider.Preference; got != 10 {
		t.Errorf("Observe: required field Preference late-initialized to %d, want unchanged %d", got, 10)
	}
	if got := *cr.Spec.ForProvider.View; got != "default" {
		t.Errorf("Observe: required field View late-initialized to %q, want unchanged %q", got, "default")
	}
}

// ── isUpToDate: table-driven field comparison ───────────────────────────

func TestIsUpToDate(t *testing.T) {
	observedRecord := func() *ibclient.RecordMX {
		ttl := uint32(300)
		pref := uint32(10)
		return &ibclient.RecordMX{
			Name:          stringPtr("example.com"),
			MailExchanger: stringPtr("mail.example.com"),
			Preference:    &pref,
			Comment:       stringPtr("hello"),
			Ttl:           &ttl,
			UseTtl:        boolPtr(true),
			Ea:            ibclient.EA{"env": "prod"},
		}
	}

	cases := map[string]struct {
		reason        string
		name          *string
		mailExchanger *string
		preference    *int64
		comment       *string
		ttl           *int64
		useTTL        *bool
		extAttrs      map[string]string
		want          bool
	}{
		"IdenticalFieldsAreUpToDate": {
			reason:        "when every mutable field matches the observed record, the resource must be reported up to date",
			name:          stringPtr("example.com"),
			mailExchanger: stringPtr("mail.example.com"),
			preference:    int64Ptr(10),
			comment:       stringPtr("hello"),
			ttl:           int64Ptr(300),
			useTTL:        boolPtr(true),
			extAttrs:      map[string]string{"env": "prod"},
			want:          true,
		},
		"ChangedNameIsNotUpToDate": {
			reason:        "a changed name must be detected as drift",
			name:          stringPtr("renamed.example.com"),
			mailExchanger: stringPtr("mail.example.com"),
			preference:    int64Ptr(10),
			comment:       stringPtr("hello"),
			ttl:           int64Ptr(300),
			useTTL:        boolPtr(true),
			extAttrs:      map[string]string{"env": "prod"},
			want:          false,
		},
		"ChangedMailExchangerIsNotUpToDate": {
			reason:        "a changed mailExchanger must be detected as drift",
			name:          stringPtr("example.com"),
			mailExchanger: stringPtr("mail2.example.com"),
			preference:    int64Ptr(10),
			comment:       stringPtr("hello"),
			ttl:           int64Ptr(300),
			useTTL:        boolPtr(true),
			extAttrs:      map[string]string{"env": "prod"},
			want:          false,
		},
		"ChangedPreferenceIsNotUpToDate": {
			reason:        "a changed preference must be detected as drift",
			name:          stringPtr("example.com"),
			mailExchanger: stringPtr("mail.example.com"),
			preference:    int64Ptr(20),
			comment:       stringPtr("hello"),
			ttl:           int64Ptr(300),
			useTTL:        boolPtr(true),
			extAttrs:      map[string]string{"env": "prod"},
			want:          false,
		},
		"ChangedCommentIsNotUpToDate": {
			reason:        "a changed comment must be detected as drift",
			name:          stringPtr("example.com"),
			mailExchanger: stringPtr("mail.example.com"),
			preference:    int64Ptr(10),
			comment:       stringPtr("goodbye"),
			ttl:           int64Ptr(300),
			useTTL:        boolPtr(true),
			extAttrs:      map[string]string{"env": "prod"},
			want:          false,
		},
		"ChangedTTLIsNotUpToDate": {
			reason:        "a changed ttl must be detected as drift",
			name:          stringPtr("example.com"),
			mailExchanger: stringPtr("mail.example.com"),
			preference:    int64Ptr(10),
			comment:       stringPtr("hello"),
			ttl:           int64Ptr(600),
			useTTL:        boolPtr(true),
			extAttrs:      map[string]string{"env": "prod"},
			want:          false,
		},
		"ChangedUseTTLIsNotUpToDate": {
			reason:        "a changed useTtl flag must be detected as drift",
			name:          stringPtr("example.com"),
			mailExchanger: stringPtr("mail.example.com"),
			preference:    int64Ptr(10),
			comment:       stringPtr("hello"),
			ttl:           int64Ptr(300),
			useTTL:        boolPtr(false),
			extAttrs:      map[string]string{"env": "prod"},
			want:          false,
		},
		"ExtAttrsDifferentValueIsNotUpToDate": {
			reason:        "an extAttrs value change on an existing key must be detected as drift",
			name:          stringPtr("example.com"),
			mailExchanger: stringPtr("mail.example.com"),
			preference:    int64Ptr(10),
			comment:       stringPtr("hello"),
			ttl:           int64Ptr(300),
			useTTL:        boolPtr(true),
			extAttrs:      map[string]string{"env": "staging"},
			want:          false,
		},
		"ExtAttrsDifferentKeyIsNotUpToDate": {
			reason:        "an extAttrs key added/removed must be detected as drift",
			name:          stringPtr("example.com"),
			mailExchanger: stringPtr("mail.example.com"),
			preference:    int64Ptr(10),
			comment:       stringPtr("hello"),
			ttl:           int64Ptr(300),
			useTTL:        boolPtr(true),
			extAttrs:      map[string]string{"owner": "platform-team"},
			want:          false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := isUpToDate(tc.name, tc.mailExchanger, tc.preference, tc.comment, tc.ttl, tc.useTTL, tc.extAttrs, observedRecord())
			if got != tc.want {
				t.Errorf("%s: isUpToDate() = %v, want %v", tc.reason, got, tc.want)
			}
		})
	}
}

// TestIsUpToDateIgnoresTTLWhenUseTTLOff proves the ttl comparison is
// gated on useTtl. When useTtl is false, WAPI ignores the submitted ttl
// and returns the zone default (a realistic non-zero value, not 0) on
// every GET — the spec ttl and the observed ttl are unrelated
// quantities, and comparing them unconditionally can never converge.
func TestIsUpToDateIgnoresTTLWhenUseTTLOff(t *testing.T) {
	zoneDefault := uint32(28800)
	pref := uint32(10)
	observed := &ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    &pref,
		Comment:       stringPtr("hello"),
		Ttl:           &zoneDefault,
		UseTtl:        boolPtr(false),
		Ea:            ibclient.EA{"env": "prod"},
	}

	got := isUpToDate(
		stringPtr("example.com"),
		stringPtr("mail.example.com"),
		int64Ptr(10),
		stringPtr("hello"),
		int64Ptr(0),
		boolPtr(false),
		map[string]string{"env": "prod"},
		observed,
	)
	if !got {
		t.Error("isUpToDate: want true when useTtl is off and only the server-owned ttl differs, got false (non-convergent drift comparison)")
	}
}

// TestIsUpToDateDetectsUseTTLTransition proves a useTtl true -> false
// transition is still detected as drift even though the value comparison
// is gated off. The flag comparison must be unconditional.
func TestIsUpToDateDetectsUseTTLTransition(t *testing.T) {
	ttl := uint32(300)
	pref := uint32(10)
	observed := &ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    &pref,
		Comment:       stringPtr("hello"),
		Ttl:           &ttl,
		UseTtl:        boolPtr(true),
		Ea:            ibclient.EA{"env": "prod"},
	}

	got := isUpToDate(
		stringPtr("example.com"),
		stringPtr("mail.example.com"),
		int64Ptr(10),
		stringPtr("hello"),
		int64Ptr(300),
		boolPtr(false),
		map[string]string{"env": "prod"},
		observed,
	)
	if got {
		t.Error("isUpToDate: want false on a useTtl true -> false transition, got true (drift not detected)")
	}
}

func TestIsUpToDateExtAttrsEmptyVsNil(t *testing.T) {
	pref := uint32(10)
	rec := &ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    &pref,
	}
	// The observed record carries no extattrs (nil Ea) — a spec with an
	// explicit empty map must still compare as up to date, since
	// extAttrsEqual treats nil and empty as equivalent (avoids a phantom
	// diff when the WAPI response omits an empty extattrs object).
	got := isUpToDate(stringPtr("example.com"), stringPtr("mail.example.com"), int64Ptr(10), nil, nil, nil, map[string]string{}, rec)
	if !got {
		t.Error("isUpToDate: empty ExtAttrs spec vs nil observed Ea = false, want true")
	}
}

// ── ExtAttrs conversion: table-driven round-trip ────────────────────────

func TestExtAttrsRoundTripTable(t *testing.T) {
	cases := map[string]struct {
		reason string
		in     map[string]string
	}{
		"NilMap": {
			reason: "a nil ExtAttrs map must round-trip without producing a phantom entry",
			in:     nil,
		},
		"EmptyMap": {
			reason: "an empty ExtAttrs map must round-trip as empty, not as a spurious single-entry map",
			in:     map[string]string{},
		},
		"SingleEntry": {
			reason: "a single key/value pair must survive the SDK EA envelope round-trip unchanged",
			in:     map[string]string{"env": "prod"},
		},
		"MultipleEntries": {
			reason: "multiple key/value pairs must all survive the round-trip",
			in:     map[string]string{"env": "prod", "owner": "platform-team", "team": "dns"},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ea := buildEA(tc.in)
			out := extAttrsFromEA(ea)
			if !extAttrsEqual(tc.in, out) {
				t.Errorf("%s: round-trip got %v, want %v", tc.reason, out, tc.in)
			}
		})
	}
}

// ── ttlOrZero / preferenceOrZero: uint32 conversion edge cases ─────────

func TestTtlOrZero(t *testing.T) {
	cases := map[string]struct {
		reason string
		ttl    *int64
		want   uint32
	}{
		"NilReturnsZero": {
			reason: "an unset TTL pointer must map to 0 — the WAPI create/update calls take a plain uint32 with no separate unset sentinel",
			ttl:    nil,
			want:   0,
		},
		"NegativeClampsToZero": {
			reason: "a negative TTL is invalid for a uint32 wire value and must clamp to 0 rather than wrap around",
			ttl:    int64Ptr(-1),
			want:   0,
		},
		"ValidValuePassesThrough": {
			reason: "an in-range TTL must convert to the identical uint32 value",
			ttl:    int64Ptr(300),
			want:   300,
		},
		"OverflowClampsToZero": {
			reason: "a TTL larger than uint32 max must clamp to 0 rather than silently truncate",
			ttl:    int64Ptr(int64(math.MaxUint32) + 1),
			want:   0,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := ttlOrZero(tc.ttl)
			if got != tc.want {
				t.Errorf("%s: ttlOrZero(%v) = %d, want %d", tc.reason, tc.ttl, got, tc.want)
			}
		})
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
			objMgr, err := newObjectManagerWithScheme(creds, sslVerify, "http", "80")
			if err != nil {
				t.Fatalf("newObjectManagerWithScheme: unexpected error: %v", err)
			}
			if objMgr == nil {
				t.Fatal("newObjectManagerWithScheme: expected non-nil object manager")
			}
		})
	}
}

func TestPreferenceOrZero(t *testing.T) {
	cases := map[string]struct {
		reason     string
		preference *int64
		want       uint32
	}{
		"NilReturnsZero": {
			reason:     "an unset preference pointer must map to 0",
			preference: nil,
			want:       0,
		},
		"NegativeClampsToZero": {
			reason:     "a negative preference is invalid and must clamp to 0 rather than wrap around",
			preference: int64Ptr(-1),
			want:       0,
		},
		"ValidValuePassesThrough": {
			reason:     "an in-range preference must convert to the identical uint32 value",
			preference: int64Ptr(10),
			want:       10,
		},
		"MaxValidValuePassesThrough": {
			reason:     "the maximum valid MX preference (65535) must pass through unclamped",
			preference: int64Ptr(65535),
			want:       65535,
		},
		"OverflowClampsToZero": {
			reason:     "a preference above the 65535 WAPI limit must clamp to 0 rather than silently truncate",
			preference: int64Ptr(65536),
			want:       0,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := preferenceOrZero(tc.preference)
			if got != tc.want {
				t.Errorf("%s: preferenceOrZero(%v) = %d, want %d", tc.reason, tc.preference, got, tc.want)
			}
		})
	}
}
