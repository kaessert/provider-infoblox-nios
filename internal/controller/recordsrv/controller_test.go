// Package recordsrv unit tests for the SRVRecord MR controllers. Tests use
// inline httptest.NewServer mocks that emulate the WAPI record:srv
// endpoints, PascalCase test names (no underscores), and white-box access
// to the unexported connectors/clients so both scopes can be exercised
// without going through the full Connect() credential bridge on every
// test.
package recordsrv

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recordsrv/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recordsrv/v1alpha1"
	namespacedpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
)

// ── generic helpers ─────────────────────────────────────────────────────────

func stringPtr(s string) *string { return &s }
func boolPtr(b bool) *bool       { return &b }
func uint32Ptr(i uint32) *uint32 { return &i }

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

// newClusterSRVRecord builds a minimal cluster-scoped SRVRecord CR. When
// externalName is empty, the external-name annotation is left unset. When
// it equals crName it simulates the framework's NameAsExternalName
// initializer (the pre-create state); any other value simulates a
// Create()-assigned server ref.
func newClusterSRVRecord(crName, externalName string) *clusterv1alpha1.SRVRecord {
	cr := &clusterv1alpha1.SRVRecord{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: "test-uid-cluster"},
		Spec: clusterv1alpha1.SRVRecordSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.SRVRecordParameters{
				Name:     stringPtr("_sip._tcp.example.com"),
				Target:   stringPtr("sipserver.example.com"),
				Priority: uint32Ptr(10),
				Weight:   uint32Ptr(20),
				Port:     uint32Ptr(5060),
				View:     stringPtr("default"),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newNamespacedSRVRecord is the namespaced variant of newClusterSRVRecord.
func newNamespacedSRVRecord(ns, crName, externalName, pcKind string) *namespacedv1alpha1.SRVRecord {
	cr := &namespacedv1alpha1.SRVRecord{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: "test-uid-namespaced"},
		Spec: namespacedv1alpha1.SRVRecordSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: "default"},
			},
			ForProvider: namespacedv1alpha1.SRVRecordParameters{
				Name:     stringPtr("_sip._tcp.example.com"),
				Target:   stringPtr("sipserver.example.com"),
				Priority: uint32Ptr(10),
				Weight:   uint32Ptr(20),
				Port:     uint32Ptr(5060),
				View:     stringPtr("default"),
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
// mockWapiServer emulates the subset of NIOS WAPI record:srv endpoints
// exercised by the SRVRecord controller (POST create, GET/PUT/DELETE by
// _ref). Records are marshaled/unmarshaled using the real
// ibclient.RecordSRV type so the wire format (including the EA
// {"value": ...} envelope) exactly matches what the SDK sends and
// expects. PUT additionally mints a new _ref whenever a _ref-mutating
// field (name/target/priority/weight/port) changes, mirroring the live
// NIOS behavior documented for this resource.

type mockWapiServer struct {
	mu      sync.Mutex
	records map[string]*ibclient.RecordSRV
	nextRef int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert immutable fields are omitted.
	lastUpdateBody []byte
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{records: map[string]*ibclient.RecordSRV{}}
}

func (m *mockWapiServer) seed(rec *ibclient.RecordSRV) string {
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

func (m *mockWapiServer) newRefLocked(rec *ibclient.RecordSRV) string {
	name := ""
	if rec.Name != nil {
		name = *rec.Name
	}
	return "record:srv/test" + itoa(m.nextRef) + ":" + name + "/" + rec.View
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

// handler returns an http.Handler implementing the record:srv WAPI
// surface.
func (m *mockWapiServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/record:srv", func(w http.ResponseWriter, r *http.Request) {
		var rec ibclient.RecordSRV
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
		var incoming ibclient.RecordSRV
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body

		// UNSTABLE _ref: renaming any of name/target/priority/weight/port
		// mints a new _ref — mirrors live NIOS Grid Manager behavior.
		refMutated := strOrEmpty(existing.Name) != strOrEmpty(incoming.Name) ||
			strOrEmpty(existing.Target) != strOrEmpty(incoming.Target) ||
			uint32PtrOrZero(existing.Priority) != uint32PtrOrZero(incoming.Priority) ||
			uint32PtrOrZero(existing.Weight) != uint32PtrOrZero(incoming.Weight) ||
			uint32PtrOrZero(existing.Port) != uint32PtrOrZero(incoming.Port)

		existing.Name = incoming.Name
		existing.Target = incoming.Target
		existing.Priority = incoming.Priority
		existing.Weight = incoming.Weight
		existing.Port = incoming.Port
		existing.Comment = incoming.Comment
		existing.Ttl = incoming.Ttl
		existing.UseTtl = incoming.UseTtl
		existing.Ea = incoming.Ea
		existing.Zone = zoneFromName(existing.Name)

		newRef := ref
		if refMutated {
			m.nextRef++
			newRef = m.newRefLocked(existing)
			delete(m.records, ref)
			existing.Ref = newRef
		}
		m.records[newRef] = existing
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
// given httptest.Server via plain HTTP (no TLS needed — the
// WapiRequestBuilder only switches to HTTPS when hostCfg.Scheme !=
// "http").
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

	ref := m.seed(&ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		View:     "default",
		Comment:  stringPtr("hello"),
		Ttl:      uint32Ptr(300),
		UseTtl:   boolPtr(true),
		Ea:       ibclient.EA{"env": "prod"},
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterSRVRecord("my-srvrecord", ref)
	cr.Spec.ForProvider.Comment = stringPtr("hello")
	cr.Spec.ForProvider.TTL = uint32Ptr(300)
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
	if cr.Status.AtProvider.Zone == nil || *cr.Status.AtProvider.Zone != "_tcp.example.com" {
		t.Errorf("AtProvider.Zone = %v, want _tcp.example.com", cr.Status.AtProvider.Zone)
	}
	if cr.Status.AtProvider.Priority == nil || *cr.Status.AtProvider.Priority != 10 {
		t.Errorf("AtProvider.Priority = %v, want 10", cr.Status.AtProvider.Priority)
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
	cr := newClusterSRVRecord("my-srvrecord", "record:srv/does-not-exist:_sip._tcp.example.com/default")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false for 404, got true")
	}
}

// TestObservePreCreateState verifies that Observe short-circuits (no HTTP
// call) when the external-name still equals the CR's Kubernetes name —
// the pre-create state for a server-assigned external-name strategy.
func TestObservePreCreateState(t *testing.T) {
	// Zero-route server: any request is an error, proving Observe never
	// calls it during the pre-create guard.
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterSRVRecord("my-srvrecord", "") // external-name unset
	meta.SetExternalName(cr, cr.GetName())        // simulate NameAsExternalName initializer

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
	cr := newClusterSRVRecord("my-srvrecord", "record:srv/test1:_sip._tcp.example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterSRVRecord("my-srvrecord", "record:srv/test1:_sip._tcp.example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestClusterObserveMinimalResponse pins nil-safety in Observe: a WAPI
// response carrying only the object's _ref (the resource identifier) and
// every other field at its Go zero value (nil pointers, empty strings, a
// nil Ea map) must not panic and must produce a valid observation with
// nil-safe AtProvider fields. observeFromRecordSRV copies optional
// pointer fields directly (never dereferences without a nil guard), so
// this test also pins that contract for future edits.
func TestClusterObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	// Seed a completely bare RecordSRV — only the SDK-assigned _ref (via
	// seed()) identifies the object. Name/View are the Go zero value
	// (nil/empty string), so zoneFromName leaves Zone at "" too.
	ref := m.seed(&ibclient.RecordSRV{})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterSRVRecord("my-srvrecord", ref)

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
	if ap.Target != nil {
		t.Errorf("AtProvider.Target = %v, want nil", ap.Target)
	}
	if ap.Priority != nil {
		t.Errorf("AtProvider.Priority = %v, want nil", ap.Priority)
	}
	if ap.Weight != nil {
		t.Errorf("AtProvider.Weight = %v, want nil", ap.Weight)
	}
	if ap.Port != nil {
		t.Errorf("AtProvider.Port = %v, want nil", ap.Port)
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
	cr := newClusterSRVRecord("my-srvrecord", "") // no external-name yet

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

// TestClusterCreateCapturesServerAssignedRef asserts the external-name
// annotation is set exactly to the _ref returned by the WAPI create
// response (server-assigned external-name strategy).
func TestClusterCreateCapturesServerAssignedRef(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterSRVRecord("my-srvrecord", "")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	m.mu.Lock()
	_, exists := m.records[got]
	m.mu.Unlock()
	if !exists {
		t.Errorf("Create: external-name %q does not match any server-side record", got)
	}
}

// TestClusterCreateServerError verifies Create() surfaces a wrapped error
// (rather than a panic or silent success) when the WAPI backend rejects
// the POST /record:srv request.
func TestClusterCreateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterSRVRecord("my-srvrecord", "")

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: want error for a 500 WAPI response, got nil")
	}

	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name = %q after a failed create, want unset", got)
	}
}

func TestClusterObserveIsUpToDateIgnoresImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		View:     "original-view",
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterSRVRecord("my-srvrecord", ref)
	// Mutate the immutable view field in spec — this must NOT affect
	// ResourceUpToDate, since view is excluded from isUpToDate (WAPI has
	// no UpdateSRVRecord parameter for it).
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

	ref := m.seed(&ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		View:     "default",
		Comment:  stringPtr("old comment"),
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterSRVRecord("my-srvrecord", ref)
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
	if got := meta.GetExternalName(cr); got != ref {
		t.Errorf("Update: external-name changed to %q for a comment-only update, want unchanged %q", got, ref)
	}
}

// TestClusterUpdateRefreshesUnstableRef verifies the UNSTABLE _ref
// contract: when a _ref-mutating field (here, name) changes, Update()
// picks up the new _ref from the PUT response and refreshes the
// external-name annotation so subsequent reconciles use the correct
// reference.
func TestClusterUpdateRefreshesUnstableRef(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	oldRef := m.seed(&ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		View:     "default",
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterSRVRecord("my-srvrecord", oldRef)
	cr.Spec.ForProvider.Name = stringPtr("_sip._tcp.renamed.example.com")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	newRef := meta.GetExternalName(cr)
	if newRef == oldRef {
		t.Fatal("Update: external-name unchanged after a _ref-mutating field update, want a refreshed _ref")
	}

	m.mu.Lock()
	_, oldStillExists := m.records[oldRef]
	newRec, newExists := m.records[newRef]
	m.mu.Unlock()
	if oldStillExists {
		t.Error("Update: old _ref still present in server state, want it replaced by the new _ref")
	}
	if !newExists {
		t.Fatal("Update: new _ref not present in server state")
	}
	if newRec.Name == nil || *newRec.Name != "_sip._tcp.renamed.example.com" {
		t.Errorf("Update: stored name = %v, want %q", newRec.Name, "_sip._tcp.renamed.example.com")
	}
}

func TestClusterUpdateDoesNotSendImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		View:     "default",
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterSRVRecord("my-srvrecord", ref)

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
	if _, present := raw["view"]; present {
		t.Errorf("Update: request body contains immutable field 'view': %v", raw["view"])
	}
}

// ── cluster: Delete ─────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordSRV{Name: stringPtr("_sip._tcp.example.com"), View: "default"})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterSRVRecord("my-srvrecord", ref)

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
	cr := newClusterSRVRecord("my-srvrecord", "record:srv/does-not-exist:_sip._tcp.example.com/default")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error for already-gone resource, got: %v", err)
	}
}

// TestClusterDeleteServerError verifies that a 5xx response from the WAPI
// delete endpoint is propagated (wrapped, not swallowed) rather than
// being treated as a not-found/already-deleted success.
func TestClusterDeleteServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterSRVRecord("my-srvrecord", "record:srv/test1:_sip._tcp.example.com/default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteSRVRecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteSRVRecord)
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

	cr := newClusterSRVRecord("my-srvrecord", "")
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

	cr := newClusterSRVRecord("my-srvrecord", "")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for missing ProviderConfig, got nil")
	}
}

// ── namespaced: Observe ──────────────────────────────────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		View:     "default",
	})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", ref, "ProviderConfig")

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
	cr := newNamespacedSRVRecord("default", "my-srvrecord", "record:srv/does-not-exist:_sip._tcp.example.com/default", "ProviderConfig")

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
	cr := newNamespacedSRVRecord("default", "my-srvrecord", "", "ProviderConfig")
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
	cr := newNamespacedSRVRecord("default", "my-srvrecord", "record:srv/test1:_sip._tcp.example.com/default", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestNamespacedObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", "record:srv/test1:_sip._tcp.example.com/default", "ProviderConfig")

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

	ref := m.seed(&ibclient.RecordSRV{})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", ref, "ProviderConfig")

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
	if ap.Target != nil {
		t.Errorf("AtProvider.Target = %v, want nil", ap.Target)
	}
	if ap.Priority != nil {
		t.Errorf("AtProvider.Priority = %v, want nil", ap.Priority)
	}
	if ap.Weight != nil {
		t.Errorf("AtProvider.Weight = %v, want nil", ap.Weight)
	}
	if ap.Port != nil {
		t.Errorf("AtProvider.Port = %v, want nil", ap.Port)
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
	cr := newNamespacedSRVRecord("default", "my-srvrecord", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

// TestNamespacedCreateServerError is the namespaced-scope counterpart of
// TestClusterCreateServerError.
func TestNamespacedCreateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: want error for a 500 WAPI response, got nil")
	}

	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name = %q after a failed create, want unset", got)
	}
}

func TestNamespacedUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		View:     "default",
	})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", ref, "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("updated comment")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.records[ref]
	m.mu.Unlock()
	if stored.Comment == nil || *stored.Comment != "updated comment" {
		t.Errorf("Update: stored comment = %v, want %q", stored.Comment, "updated comment")
	}
}

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordSRV{Name: stringPtr("_sip._tcp.example.com"), View: "default"})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
}

func TestNamespacedDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", "record:srv/does-not-exist:_sip._tcp.example.com/default", "ProviderConfig")

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
	cr := newNamespacedSRVRecord("default", "my-srvrecord", "record:srv/test1:_sip._tcp.example.com/default", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteSRVRecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteSRVRecord)
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

	cr := newNamespacedSRVRecord(ns, "my-srvrecord", "", "ProviderConfig")
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

	cr := newNamespacedSRVRecord("app-ns", "my-srvrecord", "", "ClusterProviderConfig")
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

	cr := newNamespacedSRVRecord("default", "my-srvrecord", "", "SomeOtherKind")
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
	var ttl *uint32
	var useTTL *bool
	extAttrs := map[string]string(nil)

	rec := &ibclient.RecordSRV{
		Comment: stringPtr("server default"),
		Ttl:     uint32Ptr(600),
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
	ttl := uint32Ptr(120)
	useTTL := boolPtr(false)
	extAttrs := map[string]string{"env": "staging"}

	rec := &ibclient.RecordSRV{
		Comment: stringPtr("server default"),
		Ttl:     uint32Ptr(600),
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
	var ttl *uint32
	useTTL := boolPtr(false)
	extAttrs := map[string]string(nil)

	rec := &ibclient.RecordSRV{
		Ttl:    uint32Ptr(28800),
		UseTtl: boolPtr(false),
	}

	lateInitialize(&comment, &ttl, &useTTL, &extAttrs, rec)

	if ttl != nil {
		t.Errorf("lateInitialize: ttl = %v, want nil (useTtl is off, observed ttl is the zone default, not a user value)", *ttl)
	}
}

// TestObserveDoesNotLateInitializeRequiredFields proves that name,
// target, priority, weight, port, and view — the CRD's required
// SRVRecordParameters fields — are never overwritten by Observe()'s
// late-init step. lateInitialize only accepts pointers to the optional
// fields (comment, ttl, useTtl, extAttrs), so a spec/observed mismatch on
// a required field can never occur through the real WAPI flow (any of
// those fields changing mints a new _ref) — this test drives it
// artificially to pin the guarantee.
func TestObserveDoesNotLateInitializeRequiredFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordSRV{
		Name:     stringPtr("_observed._tcp.example.com"),
		Target:   stringPtr("observed-target.example.com"),
		Priority: uint32Ptr(1),
		Weight:   uint32Ptr(2),
		Port:     uint32Ptr(3),
		View:     "observed-view",
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterSRVRecord("my-srvrecord", ref)
	cr.Spec.ForProvider.Name = stringPtr("_sip._tcp.example.com")
	cr.Spec.ForProvider.Target = stringPtr("sipserver.example.com")
	cr.Spec.ForProvider.Priority = uint32Ptr(10)
	cr.Spec.ForProvider.Weight = uint32Ptr(20)
	cr.Spec.ForProvider.Port = uint32Ptr(5060)
	cr.Spec.ForProvider.View = stringPtr("default")

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}

	if got := *cr.Spec.ForProvider.Name; got != "_sip._tcp.example.com" {
		t.Errorf("Observe: required field Name late-initialized to %q, want unchanged %q", got, "_sip._tcp.example.com")
	}
	if got := *cr.Spec.ForProvider.Target; got != "sipserver.example.com" {
		t.Errorf("Observe: required field Target late-initialized to %q, want unchanged %q", got, "sipserver.example.com")
	}
	if got := *cr.Spec.ForProvider.Priority; got != 10 {
		t.Errorf("Observe: required field Priority late-initialized to %d, want unchanged 10", got)
	}
	if got := *cr.Spec.ForProvider.Weight; got != 20 {
		t.Errorf("Observe: required field Weight late-initialized to %d, want unchanged 20", got)
	}
	if got := *cr.Spec.ForProvider.Port; got != 5060 {
		t.Errorf("Observe: required field Port late-initialized to %d, want unchanged 5060", got)
	}
	if got := *cr.Spec.ForProvider.View; got != "default" {
		t.Errorf("Observe: required field View late-initialized to %q, want unchanged %q", got, "default")
	}
}

// ── isUpToDate: table-driven field comparison ───────────────────────────

func TestIsUpToDate(t *testing.T) {
	observedRecord := func() *ibclient.RecordSRV {
		return &ibclient.RecordSRV{
			Name:     stringPtr("_sip._tcp.example.com"),
			Target:   stringPtr("sipserver.example.com"),
			Priority: uint32Ptr(10),
			Weight:   uint32Ptr(20),
			Port:     uint32Ptr(5060),
			Comment:  stringPtr("hello"),
			Ttl:      uint32Ptr(300),
			UseTtl:   boolPtr(true),
			Ea:       ibclient.EA{"env": "prod"},
		}
	}

	cases := map[string]struct {
		mutate                 func(rec *ibclient.RecordSRV)
		name, target, comment  *string
		priority, weight, port *uint32
		ttl                    *uint32
		useTTL                 *bool
		extAttrs               map[string]string
		want                   bool
	}{
		"AllFieldsMatch": {
			name: stringPtr("_sip._tcp.example.com"), target: stringPtr("sipserver.example.com"),
			priority: uint32Ptr(10), weight: uint32Ptr(20), port: uint32Ptr(5060),
			comment: stringPtr("hello"), ttl: uint32Ptr(300), useTTL: boolPtr(true),
			extAttrs: map[string]string{"env": "prod"},
			want:     true,
		},
		"NameDiffers": {
			name: stringPtr("_other._tcp.example.com"), target: stringPtr("sipserver.example.com"),
			priority: uint32Ptr(10), weight: uint32Ptr(20), port: uint32Ptr(5060),
			comment: stringPtr("hello"), ttl: uint32Ptr(300), useTTL: boolPtr(true),
			extAttrs: map[string]string{"env": "prod"},
			want:     false,
		},
		"TargetDiffers": {
			name: stringPtr("_sip._tcp.example.com"), target: stringPtr("other.example.com"),
			priority: uint32Ptr(10), weight: uint32Ptr(20), port: uint32Ptr(5060),
			comment: stringPtr("hello"), ttl: uint32Ptr(300), useTTL: boolPtr(true),
			extAttrs: map[string]string{"env": "prod"},
			want:     false,
		},
		"PriorityDiffers": {
			name: stringPtr("_sip._tcp.example.com"), target: stringPtr("sipserver.example.com"),
			priority: uint32Ptr(99), weight: uint32Ptr(20), port: uint32Ptr(5060),
			comment: stringPtr("hello"), ttl: uint32Ptr(300), useTTL: boolPtr(true),
			extAttrs: map[string]string{"env": "prod"},
			want:     false,
		},
		"WeightDiffers": {
			name: stringPtr("_sip._tcp.example.com"), target: stringPtr("sipserver.example.com"),
			priority: uint32Ptr(10), weight: uint32Ptr(99), port: uint32Ptr(5060),
			comment: stringPtr("hello"), ttl: uint32Ptr(300), useTTL: boolPtr(true),
			extAttrs: map[string]string{"env": "prod"},
			want:     false,
		},
		"PortDiffers": {
			name: stringPtr("_sip._tcp.example.com"), target: stringPtr("sipserver.example.com"),
			priority: uint32Ptr(10), weight: uint32Ptr(20), port: uint32Ptr(9999),
			comment: stringPtr("hello"), ttl: uint32Ptr(300), useTTL: boolPtr(true),
			extAttrs: map[string]string{"env": "prod"},
			want:     false,
		},
		"CommentDiffers": {
			name: stringPtr("_sip._tcp.example.com"), target: stringPtr("sipserver.example.com"),
			priority: uint32Ptr(10), weight: uint32Ptr(20), port: uint32Ptr(5060),
			comment: stringPtr("goodbye"), ttl: uint32Ptr(300), useTTL: boolPtr(true),
			extAttrs: map[string]string{"env": "prod"},
			want:     false,
		},
		"TTLDiffers": {
			name: stringPtr("_sip._tcp.example.com"), target: stringPtr("sipserver.example.com"),
			priority: uint32Ptr(10), weight: uint32Ptr(20), port: uint32Ptr(5060),
			comment: stringPtr("hello"), ttl: uint32Ptr(60), useTTL: boolPtr(true),
			extAttrs: map[string]string{"env": "prod"},
			want:     false,
		},
		"UseTTLDiffers": {
			name: stringPtr("_sip._tcp.example.com"), target: stringPtr("sipserver.example.com"),
			priority: uint32Ptr(10), weight: uint32Ptr(20), port: uint32Ptr(5060),
			comment: stringPtr("hello"), ttl: uint32Ptr(300), useTTL: boolPtr(false),
			extAttrs: map[string]string{"env": "prod"},
			want:     false,
		},
		"ExtAttrsDiffer": {
			name: stringPtr("_sip._tcp.example.com"), target: stringPtr("sipserver.example.com"),
			priority: uint32Ptr(10), weight: uint32Ptr(20), port: uint32Ptr(5060),
			comment: stringPtr("hello"), ttl: uint32Ptr(300), useTTL: boolPtr(true),
			extAttrs: map[string]string{"env": "staging"},
			want:     false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rec := observedRecord()
			if tc.mutate != nil {
				tc.mutate(rec)
			}
			got := isUpToDate(tc.name, tc.target, tc.comment, tc.priority, tc.weight, tc.port, tc.ttl, tc.useTTL, tc.extAttrs, rec)
			if got != tc.want {
				t.Errorf("isUpToDate() = %v, want %v", got, tc.want)
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
	observed := &ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		Comment:  stringPtr("hello"),
		Ttl:      uint32Ptr(28800),
		UseTtl:   boolPtr(false),
		Ea:       ibclient.EA{"env": "prod"},
	}

	got := isUpToDate(
		stringPtr("_sip._tcp.example.com"), stringPtr("sipserver.example.com"), stringPtr("hello"),
		uint32Ptr(10), uint32Ptr(20), uint32Ptr(5060), uint32Ptr(0), boolPtr(false),
		map[string]string{"env": "prod"}, observed,
	)
	if !got {
		t.Error("isUpToDate: want true when useTtl is off and only the server-owned ttl differs, got false (non-convergent drift comparison)")
	}
}

// TestIsUpToDateDetectsUseTTLTransition proves a useTtl true -> false
// transition is still detected as drift even though the value comparison
// is gated off. The flag comparison must be unconditional.
func TestIsUpToDateDetectsUseTTLTransition(t *testing.T) {
	observed := &ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		Comment:  stringPtr("hello"),
		Ttl:      uint32Ptr(300),
		UseTtl:   boolPtr(true),
		Ea:       ibclient.EA{"env": "prod"},
	}

	got := isUpToDate(
		stringPtr("_sip._tcp.example.com"), stringPtr("sipserver.example.com"), stringPtr("hello"),
		uint32Ptr(10), uint32Ptr(20), uint32Ptr(5060), uint32Ptr(300), boolPtr(false),
		map[string]string{"env": "prod"}, observed,
	)
	if got {
		t.Error("isUpToDate: want false on a useTtl true -> false transition, got true (drift not detected)")
	}
}

func TestIsUpToDateExtAttrsEmptyVsNil(t *testing.T) {
	rec := &ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		Ea:       nil,
	}
	got := isUpToDate(stringPtr("_sip._tcp.example.com"), stringPtr("sipserver.example.com"), nil,
		uint32Ptr(10), uint32Ptr(20), uint32Ptr(5060), nil, nil, map[string]string{}, rec)
	if !got {
		t.Error("isUpToDate: want true when spec ExtAttrs is empty map and observed Ea is nil")
	}
}

func TestUint32PtrOrZero(t *testing.T) {
	cases := map[string]struct {
		v      *uint32
		want   uint32
		reason string
	}{
		"Nil":      {v: nil, want: 0, reason: "nil pointer becomes 0"},
		"Zero":     {v: uint32Ptr(0), want: 0, reason: "zero passes through"},
		"Positive": {v: uint32Ptr(5060), want: 5060, reason: "in-range value passes through"},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := uint32PtrOrZero(tc.v)
			if got != tc.want {
				t.Errorf("%s: uint32PtrOrZero(%v) = %d, want %d", tc.reason, tc.v, got, tc.want)
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
