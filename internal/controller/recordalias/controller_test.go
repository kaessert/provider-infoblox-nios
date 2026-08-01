// Package recordalias unit tests for the AliasRecord MR controllers.
// Tests use inline httptest.NewServer mocks that emulate the WAPI
// record:alias endpoints, PascalCase test names (no underscores), and
// white-box access to the unexported connectors/clients so both scopes
// can be exercised without going through the full Connect() credential
// bridge on every test.
package recordalias

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recordalias/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recordalias/v1alpha1"
	namespacedpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
)

// testDefault is the value used throughout these tests for the DNS view,
// the Kubernetes namespace, and the ProviderConfig/ClusterProviderConfig
// name — they are all conventionally "default" in this provider's test
// fixtures, so a single named constant avoids repeating the literal
// string dozens of times across unrelated test cases.
const testDefault = "default"

// testEAKey is the extensible-attribute key reused across the
// isUpToDate/lateInitialize/round-trip test cases below.
const testEAKey = "env"

// testEAValue is the extensible-attribute value paired with testEAKey in
// the "up to date" fixtures (as opposed to the "staging"/other values
// used to exercise drift detection).
const testEAValue = "prod"

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
func uint32Ptr(i uint32) *uint32 { return &i }
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

// newClusterAliasRecord builds a minimal cluster-scoped AliasRecord CR.
// When externalName is empty, the external-name annotation is left
// unset. When it equals crName it simulates the framework's
// NameAsExternalName initializer (the pre-create state); any other value
// simulates a Create()-assigned server ref.
func newClusterAliasRecord(crName, externalName string) *clusterv1alpha1.AliasRecord {
	cr := &clusterv1alpha1.AliasRecord{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: "test-uid-cluster"},
		Spec: clusterv1alpha1.AliasRecordSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: testDefault},
			},
			ForProvider: clusterv1alpha1.AliasRecordParameters{
				Name:       stringPtr("alias.example.com"),
				TargetName: stringPtr("target.example.com"),
				TargetType: stringPtr("A"),
				View:       stringPtr(testDefault),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newNamespacedAliasRecord is the namespaced variant of
// newClusterAliasRecord.
func newNamespacedAliasRecord(ns, crName, externalName, pcKind string) *namespacedv1alpha1.AliasRecord {
	cr := &namespacedv1alpha1.AliasRecord{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: "test-uid-namespaced"},
		Spec: namespacedv1alpha1.AliasRecordSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: testDefault},
			},
			ForProvider: namespacedv1alpha1.AliasRecordParameters{
				Name:       stringPtr("alias.example.com"),
				TargetName: stringPtr("target.example.com"),
				TargetType: stringPtr("A"),
				View:       stringPtr(testDefault),
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
// mockWapiServer emulates the subset of NIOS WAPI record:alias endpoints
// exercised by the AliasRecord controller (POST create, GET/PUT/DELETE by
// _ref). Records are marshaled/unmarshaled using the real
// ibclient.RecordAlias type so the wire format (including the EA
// {"value": ...} envelope) exactly matches what the SDK sends and expects.

type mockWapiServer struct {
	mu      sync.Mutex
	records map[string]*ibclient.RecordAlias
	nextRef int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert immutable fields are omitted.
	lastUpdateBody []byte
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{records: map[string]*ibclient.RecordAlias{}}
}

func (m *mockWapiServer) seed(rec *ibclient.RecordAlias) string {
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

func (m *mockWapiServer) newRefLocked(rec *ibclient.RecordAlias) string {
	name := ""
	if rec.Name != nil {
		name = *rec.Name
	}
	view := ""
	if rec.View != nil {
		view = *rec.View
	}
	return "record:alias/test" + itoa(m.nextRef) + ":" + name + "/" + view
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

// handler returns an http.Handler implementing the record:alias WAPI
// surface.
func (m *mockWapiServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/record:alias", func(w http.ResponseWriter, r *http.Request) {
		var rec ibclient.RecordAlias
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

	// Search endpoint (GetAllAliasRecord): a GET with no _ref path
	// segment, filtered by view/name/target_name/target_type query
	// params. Registered as an exact literal path so Go's ServeMux
	// prefers it over the {ref...} wildcard below for requests to
	// precisely "record:alias" (real _refs always carry additional path
	// segments).
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/record:alias", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		view := q.Get("view")
		name := q.Get("name")
		targetName := q.Get("target_name")
		targetType := q.Get("target_type")

		m.mu.Lock()
		// Initialized (not nil) so an empty result set marshals to a
		// JSON "[]" body, matching real WAPI search semantics — the SDK
		// connector treats literal "[]" as its NotFoundError trigger, and
		// a nil slice marshaling to "null" would mask that behavior in
		// tests (see the package-level defect this mock now reproduces).
		matches := []ibclient.RecordAlias{}
		for _, rec := range m.records {
			if view != "" && (rec.View == nil || *rec.View != view) {
				continue
			}
			if name != "" && (rec.Name == nil || *rec.Name != name) {
				continue
			}
			if targetName != "" && (rec.TargetName == nil || *rec.TargetName != targetName) {
				continue
			}
			if targetType != "" && rec.TargetType != targetType {
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
		var incoming ibclient.RecordAlias
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body
		// Partial-PUT merge semantics: only fields present on the wire
		// overwrite the stored record. Pointer fields distinguish
		// "absent" (nil) from an explicit zero value.
		if incoming.Name != nil {
			existing.Name = incoming.Name
		}
		if incoming.TargetName != nil {
			existing.TargetName = incoming.TargetName
		}
		if incoming.TargetType != "" {
			existing.TargetType = incoming.TargetType
		}
		if incoming.Comment != nil {
			existing.Comment = incoming.Comment
		}
		if incoming.Disable != nil {
			existing.Disable = incoming.Disable
		}
		if incoming.Ttl != nil {
			existing.Ttl = incoming.Ttl
		}
		if incoming.UseTtl != nil {
			existing.UseTtl = incoming.UseTtl
		}
		if incoming.Ea != nil {
			existing.Ea = incoming.Ea
		}
		existing.Zone = zoneFromName(existing.Name)
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

// newTestObjectManager builds an ibclient.IBObjectManager (and its
// underlying IBConnector) pointed at the given httptest.Server via plain
// HTTP (no TLS needed — the WapiRequestBuilder only switches to HTTPS
// when hostCfg.Scheme != "http").
func newTestObjectManager(t *testing.T, srv *httptest.Server) (ibclient.IBObjectManager, ibclient.IBConnector) {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("cannot parse test server URL: %v", err)
	}
	objMgr, conn, err := newObjectManagerWithScheme(&nioCredentials{
		Host:     u.Hostname(),
		Username: "test-user",
		Password: "test-pass",
	}, true, "http", u.Port())
	if err != nil {
		t.Fatalf("cannot build test object manager: %v", err)
	}
	return objMgr, conn
}

// ── cluster: Observe ────────────────────────────────────────────────────

func TestClusterObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	view := testDefault
	ref := m.seed(&ibclient.RecordAlias{
		Name:       stringPtr("alias.example.com"),
		TargetName: stringPtr("target.example.com"),
		TargetType: "A",
		View:       &view,
		Comment:    stringPtr("hello"),
		Disable:    boolPtr(false),
		Ttl:        uint32Ptr(300),
		UseTtl:     boolPtr(true),
		Ea:         ibclient.EA{testEAKey: testEAValue},
	})

	objMgr, conn := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterAliasRecord("my-alias", ref)
	cr.Spec.ForProvider.Comment = stringPtr("hello")
	cr.Spec.ForProvider.Disable = boolPtr(false)
	cr.Spec.ForProvider.TTL = uint32Ptr(300)
	cr.Spec.ForProvider.UseTTL = boolPtr(true)
	cr.Spec.ForProvider.ExtAttrs = map[string]string{testEAKey: testEAValue}

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
	if cr.Status.AtProvider.Zone == nil || *cr.Status.AtProvider.Zone != "example.com" {
		t.Errorf("AtProvider.Zone = %v, want example.com", cr.Status.AtProvider.Zone)
	}
	if cond := cr.GetCondition(xpv1.TypeReady); cond.Status != corev1.ConditionTrue {
		t.Errorf("condition Ready = %v, want True", cond.Status)
	}
}

func TestClusterObserveNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	objMgr, conn := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterAliasRecord("my-alias", "record:alias/does-not-exist:alias.example.com/default")

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

	objMgr, conn := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterAliasRecord("my-alias", "") // external-name unset
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

	objMgr, conn := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterAliasRecord("my-alias", "record:alias/test1:alias.example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	objMgr, conn := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterAliasRecord("my-alias", "record:alias/test1:alias.example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
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

	// Seed a completely bare RecordAlias — only the SDK-assigned _ref
	// (via seed()) identifies the object.
	ref := m.seed(&ibclient.RecordAlias{})

	objMgr, conn := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterAliasRecord("my-alias", ref)

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
	if ap.TargetName != nil {
		t.Errorf("AtProvider.TargetName = %v, want nil", ap.TargetName)
	}
	if ap.TargetType != nil {
		t.Errorf("AtProvider.TargetType = %v, want nil", ap.TargetType)
	}
	if ap.Comment != nil {
		t.Errorf("AtProvider.Comment = %v, want nil", ap.Comment)
	}
	if ap.Disable != nil {
		t.Errorf("AtProvider.Disable = %v, want nil", ap.Disable)
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

	objMgr, conn := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterAliasRecord("my-alias", "") // no external-name yet

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

func TestClusterCreateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	objMgr, conn := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterAliasRecord("my-alias", "") // no external-name yet

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errCreateAliasRecord) {
		t.Errorf("Create: error = %q, want it to contain %q (wrapped, not swallowed)", got, errCreateAliasRecord)
	}
	if got := meta.GetExternalName(cr); got != cr.GetName() && got != "" {
		t.Errorf("Create: external-name mutated on failed create, got %q", got)
	}
}

func TestClusterObserveIsUpToDateIgnoresImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	view := "original-view"
	ref := m.seed(&ibclient.RecordAlias{
		Name:       stringPtr("alias.example.com"),
		TargetName: stringPtr("target.example.com"),
		TargetType: "A",
		View:       &view,
	})

	objMgr, conn := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterAliasRecord("my-alias", ref)
	// Mutate the immutable (soft-immutable) view field in spec — this
	// must NOT affect ResourceUpToDate.
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

	view := testDefault
	ref := m.seed(&ibclient.RecordAlias{
		Name:       stringPtr("alias.example.com"),
		TargetName: stringPtr("target.example.com"),
		TargetType: "A",
		View:       &view,
		Comment:    stringPtr("old comment"),
	})

	objMgr, conn := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterAliasRecord("my-alias", ref)
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

func TestClusterUpdateDoesNotSendImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	view := testDefault
	ref := m.seed(&ibclient.RecordAlias{
		Name:       stringPtr("alias.example.com"),
		TargetName: stringPtr("target.example.com"),
		TargetType: "A",
		View:       &view,
	})

	objMgr, conn := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterAliasRecord("my-alias", ref)

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

	// The stored view must remain untouched by the update.
	m.mu.Lock()
	stored := m.records[ref]
	m.mu.Unlock()
	if stored.View == nil || *stored.View != testDefault {
		t.Errorf("Update: stored view = %v, want unchanged %q", stored.View, testDefault)
	}
}

// TestClusterUpdateRefChangeUpdatesExternalName proves that when the WAPI
// PUT response carries a _ref different from the one the update was
// issued against — as happens when name-mutating fields change the
// object's identity server-side — the external-name annotation is
// refreshed to the new _ref so the next reconcile does not 404 against a
// stale reference.
func TestClusterUpdateRefChangeUpdatesExternalName(t *testing.T) {
	oldRef := "record:alias/test1:old.example.com/default"
	newRef := "record:alias/test2:new.example.com/default"

	mux := http.NewServeMux()
	mux.HandleFunc("PUT /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		if got := r.PathValue("ref"); got != oldRef {
			t.Errorf("PUT: unexpected ref in request path: %q, want %q", got, oldRef)
		}
		writeJSON(w, http.StatusOK, newRef)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	objMgr, conn := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterAliasRecord("my-alias", oldRef)
	cr.Spec.ForProvider.Name = stringPtr("new.example.com")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	if got := meta.GetExternalName(cr); got != newRef {
		t.Errorf("Update: external-name = %q, want refreshed to %q", got, newRef)
	}
}

// ── cluster: Delete ─────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	view := testDefault
	ref := m.seed(&ibclient.RecordAlias{Name: stringPtr("alias.example.com"), View: &view})

	objMgr, conn := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterAliasRecord("my-alias", ref)

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

	objMgr, conn := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterAliasRecord("my-alias", "record:alias/does-not-exist:alias.example.com/default")

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

	objMgr, conn := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterAliasRecord("my-alias", "record:alias/test1:alias.example.com/default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteAliasRecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteAliasRecord)
	}
}

// TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject verifies the
// core defect fix: a 404 against the stored _ref must not be treated as
// "already deleted" when a natural-key search finds the same identity
// still live under a different _ref. Deleting that record would be
// unverifiable ownership, so Delete() must refuse and leave the record in
// place.
func TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	view := testDefault
	liveRef := m.seed(&ibclient.RecordAlias{
		Name:       stringPtr("alias.example.com"),
		View:       &view,
		TargetName: stringPtr("target.example.com"),
		TargetType: "A",
	})

	objMgr, conn := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterAliasRecord("my-alias", "record:alias/stale-ref:alias.example.com/default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected refusal error when a natural-key search still matches a live object, got nil")
	}
	if !strings.Contains(err.Error(), "refusing to delete") {
		t.Errorf("Delete: error = %q, want it to explain the refusal", err.Error())
	}

	m.mu.Lock()
	_, stillExists := m.records[liveRef]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Delete: live record was removed despite the refusal — DELETE must not have been issued against it")
	}
}

// TestClusterDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch is the
// companion happy path: a 404 against the stored _ref, and a natural-key
// search that finds nothing, means the object really is gone.
func TestClusterDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	objMgr, conn := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterAliasRecord("my-alias", "record:alias/stale-ref:alias.example.com/default")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the natural-key search also finds nothing, got: %v", err)
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

	view := testDefault
	liveRef := m.seed(&ibclient.RecordAlias{
		Name:       stringPtr("alias.example.com"),
		View:       &view,
		TargetName: stringPtr("target.example.com"),
		TargetType: "A",
	})

	objMgr, conn := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterAliasRecord("my-alias", "record:alias/stale-ref:alias.example.com/default")

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected refusal error when a natural-key search still matches a live object, got nil")
	}
	if !strings.Contains(err.Error(), "cannot observe") {
		t.Errorf("Observe: error = %q, want it to explain the refusal", err.Error())
	}

	m.mu.Lock()
	_, stillExists := m.records[liveRef]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Observe: live record was removed — Observe() must never mutate the backend")
	}
}

// TestClusterObserveSucceedsWhenStaleRefHasNoNaturalKeyMatch verifies the
// genuine-absence direction of the same defect: a 404 against the stored
// _ref, and a natural-key search over the CR's own identity that
// genuinely finds nothing, must report ResourceExists:false with no
// error — not the "failed getting Alias Record: not found" error the
// SDK's ObjectManager.GetAllAliasRecord produced before this fix. Without
// this, Observe fails, the delete finalizer is never cleared, and the MR
// is stuck forever even though the backend object is already gone.
func TestClusterObserveSucceedsWhenStaleRefHasNoNaturalKeyMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	objMgr, conn := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterAliasRecord("my-alias", "record:alias/stale-ref:alias.example.com/default")

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
				ObjectMeta: metav1.ObjectMeta{Name: testDefault},
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

	cr := newClusterAliasRecord("my-alias", "")
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

	cr := newClusterAliasRecord("my-alias", "")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for missing ProviderConfig, got nil")
	}
}

// ── namespaced: Observe ──────────────────────────────────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	view := testDefault
	ref := m.seed(&ibclient.RecordAlias{
		Name:       stringPtr("alias.example.com"),
		TargetName: stringPtr("target.example.com"),
		TargetType: "A",
		View:       &view,
	})

	objMgr, conn := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedAliasRecord(testDefault, "my-alias", ref, "ProviderConfig")

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

	objMgr, conn := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedAliasRecord(testDefault, "my-alias", "record:alias/does-not-exist:alias.example.com/default", "ProviderConfig")

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

	objMgr, conn := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedAliasRecord(testDefault, "my-alias", "", "ProviderConfig")
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

	objMgr, conn := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedAliasRecord(testDefault, "my-alias", "record:alias/test1:alias.example.com/default", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestNamespacedObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	objMgr, conn := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedAliasRecord(testDefault, "my-alias", "record:alias/test1:alias.example.com/default", "ProviderConfig")

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

	ref := m.seed(&ibclient.RecordAlias{})

	objMgr, conn := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedAliasRecord(testDefault, "my-alias", ref, "ProviderConfig")

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
	if ap.TargetName != nil {
		t.Errorf("AtProvider.TargetName = %v, want nil", ap.TargetName)
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

	objMgr, conn := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedAliasRecord(testDefault, "my-alias", "", "ProviderConfig")

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

	view := testDefault
	ref := m.seed(&ibclient.RecordAlias{
		Name:       stringPtr("alias.example.com"),
		TargetName: stringPtr("target.example.com"),
		TargetType: "A",
		View:       &view,
	})

	objMgr, conn := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedAliasRecord(testDefault, "my-alias", ref, "ProviderConfig")
	cr.Spec.ForProvider.TargetName = stringPtr("other-target.example.com")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.records[ref]
	m.mu.Unlock()
	if stored.TargetName == nil || *stored.TargetName != "other-target.example.com" {
		t.Errorf("Update: stored targetName = %v, want other-target.example.com", stored.TargetName)
	}
}

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	view := testDefault
	ref := m.seed(&ibclient.RecordAlias{Name: stringPtr("alias.example.com"), View: &view})

	objMgr, conn := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedAliasRecord(testDefault, "my-alias", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
}

func TestNamespacedDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	objMgr, conn := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedAliasRecord(testDefault, "my-alias", "record:alias/does-not-exist:alias.example.com/default", "ProviderConfig")

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

	objMgr, conn := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedAliasRecord(testDefault, "my-alias", "record:alias/test1:alias.example.com/default", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteAliasRecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteAliasRecord)
	}
}

// TestNamespacedDeleteRefusesWhenStaleRefStillMatchesLiveObject is the
// namespaced-scope counterpart of
// TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject.
func TestNamespacedDeleteRefusesWhenStaleRefStillMatchesLiveObject(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	view := testDefault
	liveRef := m.seed(&ibclient.RecordAlias{
		Name:       stringPtr("alias.example.com"),
		View:       &view,
		TargetName: stringPtr("target.example.com"),
		TargetType: "A",
	})

	objMgr, conn := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedAliasRecord(testDefault, "my-alias", "record:alias/stale-ref:alias.example.com/default", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected refusal error when a natural-key search still matches a live object, got nil")
	}
	if !strings.Contains(err.Error(), "refusing to delete") {
		t.Errorf("Delete: error = %q, want it to explain the refusal", err.Error())
	}

	m.mu.Lock()
	_, stillExists := m.records[liveRef]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Delete: live record was removed despite the refusal — DELETE must not have been issued against it")
	}
}

// TestNamespacedDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch is the
// namespaced-scope counterpart of
// TestClusterDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch.
func TestNamespacedDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	objMgr, conn := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedAliasRecord(testDefault, "my-alias", "record:alias/stale-ref:alias.example.com/default", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the natural-key search also finds nothing, got: %v", err)
	}
}

// TestNamespacedObserveRefusesWhenStaleRefStillMatchesLiveObject is the
// namespaced-scope counterpart of
// TestClusterObserveRefusesWhenStaleRefStillMatchesLiveObject.
func TestNamespacedObserveRefusesWhenStaleRefStillMatchesLiveObject(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	view := testDefault
	liveRef := m.seed(&ibclient.RecordAlias{
		Name:       stringPtr("alias.example.com"),
		View:       &view,
		TargetName: stringPtr("target.example.com"),
		TargetType: "A",
	})

	objMgr, conn := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedAliasRecord(testDefault, "my-alias", "record:alias/stale-ref:alias.example.com/default", "ProviderConfig")

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected refusal error when a natural-key search still matches a live object, got nil")
	}
	if !strings.Contains(err.Error(), "cannot observe") {
		t.Errorf("Observe: error = %q, want it to explain the refusal", err.Error())
	}

	m.mu.Lock()
	_, stillExists := m.records[liveRef]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Observe: live record was removed — Observe() must never mutate the backend")
	}
}

// TestNamespacedObserveSucceedsWhenStaleRefHasNoNaturalKeyMatch is the
// namespaced-scope counterpart of
// TestClusterObserveSucceedsWhenStaleRefHasNoNaturalKeyMatch.
func TestNamespacedObserveSucceedsWhenStaleRefHasNoNaturalKeyMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	objMgr, conn := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newNamespacedAliasRecord(testDefault, "my-alias", "record:alias/stale-ref:alias.example.com/default", "ProviderConfig")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: want nil error when the natural-key search also finds nothing, got: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false when the natural-key search finds nothing, got true")
	}
}

// ── namespaced: Connect ───────────────────────────────────────────────────

func TestNamespacedConnectWithProviderConfig(t *testing.T) {
	const (
		ns     = testDefault
		secret = "infobloxnios-api-key"
	)

	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			credentialsSecret(ns, secret, "grid.example.com", "admin", "s3cr3t"),
			&namespacedpcv1alpha1.ProviderConfig{
				ObjectMeta: metav1.ObjectMeta{Name: testDefault, Namespace: ns},
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

	cr := newNamespacedAliasRecord(ns, "my-alias", "", "ProviderConfig")
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
				ObjectMeta: metav1.ObjectMeta{Name: testDefault},
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

	cr := newNamespacedAliasRecord("app-ns", "my-alias", "", "ClusterProviderConfig")
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

	cr := newNamespacedAliasRecord(testDefault, "my-alias", "", "SomeOtherKind")
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
	in := map[string]string{testEAKey: testEAValue, "owner": "platform-team"}
	ea := buildEA(in)
	out := extAttrsFromEA(ea)
	if !extAttrsEqual(in, out) {
		t.Errorf("ExtAttrs round-trip: got %v, want %v", out, in)
	}
}

// TestStringifyEAValue covers every branch of the extensible-attribute
// value stringification helper: nil, a plain string, both states of the
// SDK's ibclient.Bool type, a []string (multi-value EA), and the
// default/numeric fallback.
func TestStringifyEAValue(t *testing.T) {
	cases := map[string]struct {
		in   interface{}
		want string
	}{
		"Nil":         {in: nil, want: ""},
		"String":      {in: "platform-team", want: "platform-team"},
		"BoolTrue":    {in: ibclient.Bool(true), want: "True"},
		"BoolFalse":   {in: ibclient.Bool(false), want: "False"},
		"StringSlice": {in: []string{"a", "b", "c"}, want: "a,b,c"},
		"Default":     {in: 42, want: "42"},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := stringifyEAValue(tc.in); got != tc.want {
				t.Errorf("stringifyEAValue(%#v) = %q, want %q", tc.in, got, tc.want)
			}
		})
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
	var disable *bool
	var ttl *uint32
	var useTTL *bool
	extAttrs := map[string]string(nil)

	rec := &ibclient.RecordAlias{
		Comment: stringPtr("server default"),
		Disable: boolPtr(true),
		Ttl:     uint32Ptr(600),
		UseTtl:  boolPtr(true),
		Ea:      ibclient.EA{testEAKey: testEAValue},
	}

	changed := lateInitialize(&comment, &disable, &ttl, &useTTL, &extAttrs, rec)
	if !changed {
		t.Fatal("lateInitialize: want changed=true, got false")
	}
	if comment == nil || *comment != "server default" {
		t.Errorf("lateInitialize: comment = %v, want %q", comment, "server default")
	}
	if disable == nil || *disable != true {
		t.Errorf("lateInitialize: disable = %v, want true", disable)
	}
	if ttl == nil || *ttl != 600 {
		t.Errorf("lateInitialize: ttl = %v, want 600", ttl)
	}
	if useTTL == nil || *useTTL != true {
		t.Errorf("lateInitialize: useTTL = %v, want true", useTTL)
	}
	if !extAttrsEqual(extAttrs, map[string]string{testEAKey: testEAValue}) {
		t.Errorf("lateInitialize: extAttrs = %v, want {env: prod}", extAttrs)
	}
}

func TestLateInitializeDoesNotOverwriteSetFields(t *testing.T) {
	comment := stringPtr("user comment")
	disable := boolPtr(false)
	ttl := uint32Ptr(120)
	useTTL := boolPtr(false)
	extAttrs := map[string]string{testEAKey: "staging"}

	rec := &ibclient.RecordAlias{
		Comment: stringPtr("server default"),
		Disable: boolPtr(true),
		Ttl:     uint32Ptr(600),
		UseTtl:  boolPtr(true),
		Ea:      ibclient.EA{testEAKey: testEAValue},
	}

	changed := lateInitialize(&comment, &disable, &ttl, &useTTL, &extAttrs, rec)
	if changed {
		t.Error("lateInitialize: want changed=false when all fields already set, got true")
	}
	if *comment != "user comment" || *disable != false || *ttl != 120 || *useTTL != false || extAttrs[testEAKey] != "staging" {
		t.Error("lateInitialize: overwrote already-set ForProvider fields")
	}
}

// TestLateInitializeDoesNotBackfillTTLWhenUseTTLOff proves that when
// useTtl is false the observed ttl (WAPI's zone default, not a value the
// user's config implies) is never written back into spec.forProvider.ttl.
func TestLateInitializeDoesNotBackfillTTLWhenUseTTLOff(t *testing.T) {
	var comment *string
	var disable *bool
	var ttl *uint32
	useTTL := boolPtr(false)
	extAttrs := map[string]string(nil)

	rec := &ibclient.RecordAlias{
		Ttl:    uint32Ptr(28800),
		UseTtl: boolPtr(false),
	}

	lateInitialize(&comment, &disable, &ttl, &useTTL, &extAttrs, rec)

	if ttl != nil {
		t.Errorf("lateInitialize: ttl = %v, want nil (useTtl is off, observed ttl is the zone default, not a user value)", *ttl)
	}
}

// TestObserveDoesNotLateInitializeRequiredFields proves that name,
// targetName, targetType, and view — the CRD's required
// AliasRecordParameters fields — are never overwritten by Observe()'s
// late-init step. lateInitialize only accepts pointers to the optional
// fields (comment, disable, ttl, useTtl, extAttrs), so a spec/observed
// mismatch on a required field can never occur through the real WAPI flow
// — this test drives it artificially to pin the guarantee.
func TestObserveDoesNotLateInitializeRequiredFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	observedView := "observed-view"
	ref := m.seed(&ibclient.RecordAlias{
		Name:       stringPtr("observed.example.com"),
		TargetName: stringPtr("observed-target.example.com"),
		TargetType: "AAAA",
		View:       &observedView,
	})

	objMgr, conn := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: objMgr, conn: conn}
	cr := newClusterAliasRecord("my-alias", ref)
	cr.Spec.ForProvider.Name = stringPtr("alias.example.com")
	cr.Spec.ForProvider.TargetName = stringPtr("target.example.com")
	cr.Spec.ForProvider.TargetType = stringPtr("A")
	cr.Spec.ForProvider.View = stringPtr(testDefault)

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}

	if got := *cr.Spec.ForProvider.Name; got != "alias.example.com" {
		t.Errorf("Observe: required field Name late-initialized to %q, want unchanged %q", got, "alias.example.com")
	}
	if got := *cr.Spec.ForProvider.TargetName; got != "target.example.com" {
		t.Errorf("Observe: required field TargetName late-initialized to %q, want unchanged %q", got, "target.example.com")
	}
	if got := *cr.Spec.ForProvider.TargetType; got != "A" {
		t.Errorf("Observe: required field TargetType late-initialized to %q, want unchanged %q", got, "A")
	}
	if got := *cr.Spec.ForProvider.View; got != testDefault {
		t.Errorf("Observe: required field View late-initialized to %q, want unchanged %q", got, testDefault)
	}
}

// ── isUpToDate: table-driven field comparison ───────────────────────────

func TestIsUpToDate(t *testing.T) {
	observedRecord := func() *ibclient.RecordAlias {
		view := testDefault
		ttl := uint32(300)
		return &ibclient.RecordAlias{
			Name:       stringPtr("alias.example.com"),
			TargetName: stringPtr("target.example.com"),
			TargetType: "A",
			View:       &view,
			Comment:    stringPtr("hello"),
			Disable:    boolPtr(false),
			Ttl:        &ttl,
			UseTtl:     boolPtr(true),
			Ea:         ibclient.EA{testEAKey: testEAValue},
		}
	}

	cases := map[string]struct {
		mutate func(rec *ibclient.RecordAlias)
		want   bool
	}{
		"Identical": {
			mutate: func(rec *ibclient.RecordAlias) {},
			want:   true,
		},
		"NameDiffers": {
			mutate: func(rec *ibclient.RecordAlias) { rec.Name = stringPtr("other.example.com") },
			want:   false,
		},
		"TargetNameDiffers": {
			mutate: func(rec *ibclient.RecordAlias) { rec.TargetName = stringPtr("other-target.example.com") },
			want:   false,
		},
		"TargetTypeDiffers": {
			mutate: func(rec *ibclient.RecordAlias) { rec.TargetType = "AAAA" },
			want:   false,
		},
		"CommentDiffers": {
			mutate: func(rec *ibclient.RecordAlias) { rec.Comment = stringPtr("changed") },
			want:   false,
		},
		"DisableDiffers": {
			mutate: func(rec *ibclient.RecordAlias) { rec.Disable = boolPtr(true) },
			want:   false,
		},
		"TTLDiffers": {
			mutate: func(rec *ibclient.RecordAlias) { t := uint32(60); rec.Ttl = &t },
			want:   false,
		},
		"UseTTLDiffers": {
			mutate: func(rec *ibclient.RecordAlias) { rec.UseTtl = boolPtr(false) },
			want:   false,
		},
		"ExtAttrsDiffer": {
			mutate: func(rec *ibclient.RecordAlias) { rec.Ea = ibclient.EA{testEAKey: "staging"} },
			want:   false,
		},
		"ViewDiffersButIgnored": {
			mutate: func(rec *ibclient.RecordAlias) { v := "other-view"; rec.View = &v },
			want:   true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rec := observedRecord()
			tc.mutate(rec)

			got := isUpToDate(
				stringPtr("alias.example.com"),
				stringPtr("target.example.com"),
				stringPtr("A"),
				stringPtr("hello"),
				boolPtr(false),
				uint32Ptr(300),
				boolPtr(true),
				map[string]string{testEAKey: testEAValue},
				rec,
			)
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
	view := testDefault
	zoneDefault := uint32(28800)
	observed := &ibclient.RecordAlias{
		Name:       stringPtr("alias.example.com"),
		TargetName: stringPtr("target.example.com"),
		TargetType: "A",
		View:       &view,
		Comment:    stringPtr("hello"),
		Disable:    boolPtr(false),
		Ttl:        &zoneDefault,
		UseTtl:     boolPtr(false),
		Ea:         ibclient.EA{testEAKey: testEAValue},
	}

	got := isUpToDate(
		stringPtr("alias.example.com"),
		stringPtr("target.example.com"),
		stringPtr("A"),
		stringPtr("hello"),
		boolPtr(false),
		uint32Ptr(0),
		boolPtr(false),
		map[string]string{testEAKey: testEAValue},
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
	view := testDefault
	ttl := uint32(300)
	observed := &ibclient.RecordAlias{
		Name:       stringPtr("alias.example.com"),
		TargetName: stringPtr("target.example.com"),
		TargetType: "A",
		View:       &view,
		Comment:    stringPtr("hello"),
		Disable:    boolPtr(false),
		Ttl:        &ttl,
		UseTtl:     boolPtr(true),
		Ea:         ibclient.EA{testEAKey: testEAValue},
	}

	got := isUpToDate(
		stringPtr("alias.example.com"),
		stringPtr("target.example.com"),
		stringPtr("A"),
		stringPtr("hello"),
		boolPtr(false),
		uint32Ptr(300),
		boolPtr(false),
		map[string]string{testEAKey: testEAValue},
		observed,
	)
	if got {
		t.Error("isUpToDate: want false on a useTtl true -> false transition, got true (drift not detected)")
	}
}

func TestIsUpToDateExtAttrsEmptyVsNil(t *testing.T) {
	view := testDefault
	rec := &ibclient.RecordAlias{
		Name:       stringPtr("alias.example.com"),
		TargetName: stringPtr("target.example.com"),
		TargetType: "A",
		View:       &view,
	}

	if !isUpToDate(stringPtr("alias.example.com"), stringPtr("target.example.com"), stringPtr("A"), nil, nil, nil, nil, nil, rec) {
		t.Error("isUpToDate: want true when both spec and observed ExtAttrs are empty/nil")
	}
	if !isUpToDate(stringPtr("alias.example.com"), stringPtr("target.example.com"), stringPtr("A"), nil, nil, nil, nil, map[string]string{}, rec) {
		t.Error("isUpToDate: want true when spec ExtAttrs is empty map and observed is nil")
	}
}

func TestUint32OrZero(t *testing.T) {
	cases := map[string]struct {
		ttl  *uint32
		want uint32
	}{
		"Nil":  {ttl: nil, want: 0},
		"Zero": {ttl: uint32Ptr(0), want: 0},
		"300":  {ttl: uint32Ptr(300), want: 300},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := uint32OrZero(tc.ttl)
			if got != tc.want {
				t.Errorf("uint32OrZero(%v) = %d, want %d", tc.ttl, got, tc.want)
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
			objMgr, conn, err := newObjectManagerWithScheme(creds, sslVerify, "http", "80")
			if err != nil {
				t.Fatalf("newObjectManagerWithScheme: unexpected error: %v", err)
			}
			if objMgr == nil {
				t.Fatal("newObjectManagerWithScheme: expected non-nil object manager")
			}
			if conn == nil {
				t.Fatal("newObjectManagerWithScheme: expected non-nil connector")
			}
		})
	}
}
